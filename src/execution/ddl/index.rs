use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::Expr;

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_create_index(
        &self,
        index_name: &Option<sqlparser::ast::ObjectName>,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::IndexColumn],
        _unique: bool,
        index_options: &[sqlparser::ast::IndexOption],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let index_name_str = index_name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| format!("idx_{}_{}", table_name_str, uuid::Uuid::new_v4()));

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let mut target_col_indices = Vec::new();
        for index_col in columns {
            let col_expr = &index_col.column;
            if let Expr::Identifier(ident) = &col_expr.expr {
                if let Some(idx) = schema.get_column_index(&ident.value) {
                    target_col_indices.push(idx);
                } else {
                    return Err(FusionError::Execution(format!(
                        "Column {} not found",
                        ident.value
                    )));
                }
            } else {
                return Err(FusionError::Execution(
                    "Index only supports simple column references".to_string(),
                ));
            }
        }

        if target_col_indices.len() != 1 {
            return Err(FusionError::Execution(
                "Currently only single-column index is supported".to_string(),
            ));
        }
        let col_idx = target_col_indices[0];
        let col_name = schema.columns[col_idx].name.clone();

        let mut index_type = IndexType::BTree;
        for opt in index_options {
            if let sqlparser::ast::IndexOption::Using(sqlparser::ast::IndexType::Custom(ident)) =
                opt
            {
                if ident.value.eq_ignore_ascii_case("FTS") {
                    index_type = IndexType::FTS;
                } else if ident.value.eq_ignore_ascii_case("HNSW") {
                    index_type = IndexType::HNSW;
                }
            }
        }

        schema.columns[col_idx].is_indexed = true;
        schema.columns[col_idx].index_type = index_type.clone();

        // If HNSW, initialize the vector index
        if index_type == IndexType::HNSW {
            let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
            self.vector_index.create_index(&idx_name);
        }
        let new_schema_value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &new_schema_value).await?;

        let prefix = format!("data:{}:", table_name_str);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut count = 0;
        for (k, v) in kv_pairs {
            let key_str = std::str::from_utf8(&k)
                .map_err(|e| FusionError::Execution(format!("Data key decode error: {}", e)))?;
            let row_id = key_str
                .rsplit(':')
                .next()
                .ok_or_else(|| FusionError::Execution("Invalid data key".to_string()))?;

            let val = if let Some(row) = self.row_cache.get(key_str) {
                monitor::inc_row_cache_hit();
                row.get(col_idx).cloned()
            } else {
                crate::common::encoding::RowDecoder::decode_column(&v, col_idx).map_err(|e| {
                    FusionError::Execution(format!("Data deserialization error: {}", e))
                })?
            };
            let Some(val) = val else {
                continue;
            };

            if index_type == IndexType::FTS {
                if let Value::String(text) = &val {
                    for token in Self::tokenize_unique(text) {
                        let index_key =
                            format!("fts:{}:{}:{}:{}", table_name_str, col_name, token, row_id);
                        txn.put(index_key.as_bytes(), &[]).await?;
                    }
                }
            } else if index_type == IndexType::HNSW {
                if let Value::Vector(vec) = &val {
                    let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
                    self.vector_index
                        .insert(&idx_name, row_id.to_string(), vec.clone())?;
                }
            } else {
                let val_str = match &val {
                    Value::Integer(i) => i.to_string(),
                    Value::String(s) => s.clone(),
                    Value::Boolean(b) => b.to_string(),
                    _ => continue,
                };
                let index_key = format!(
                    "index:{}:{}:{}:{}",
                    table_name_str, col_name, val_str, row_id
                );
                txn.put(index_key.as_bytes(), &[]).await?;
            }
            count += 1;
        }

        // Store index metadata for DROP INDEX support
        let meta_key = format!("index_meta:{}", index_name_str);
        let meta_val = format!("{}:{}", table_name_str, col_name);
        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;

        Ok(QueryResult::Success {
            message: format!(
                "Index {} ({:?}) created on {}({}), indexed {} rows",
                index_name_str, index_type, table_name_str, col_name, count
            ),
        })
    }

    pub(crate) async fn handle_drop_index(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let mut dropped = 0;
        for name in names {
            let index_name = name.to_string();
            // Index metadata key: index_meta:<index_name>
            let meta_key = format!("index_meta:{}", index_name);
            if let Some(meta_bytes) = txn.get(meta_key.as_bytes()).await? {
                // Meta stores "table_name:column_name"
                let meta_str = String::from_utf8(meta_bytes).unwrap_or_default();
                let parts: Vec<&str> = meta_str.split(':').collect();
                if parts.len() >= 2 {
                    let table_name = parts[0];
                    let col_name = parts[1];

                    // Delete index entries
                    let index_prefix = format!("index:{}:{}:", table_name, col_name);
                    let entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
                    for (k, _) in entries {
                        txn.delete(&k).await?;
                    }

                    // Update schema: mark column as not indexed
                    let schema_key = format!("schema:{}", table_name);
                    if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                        if let Ok(mut schema) = bincode::deserialize::<TableSchema>(&schema_bytes) {
                            for col in &mut schema.columns {
                                if col.name == col_name {
                                    col.is_indexed = false;
                                    col.index_type = IndexType::None;
                                }
                            }
                            let new_bytes = bincode::serialize(&schema).map_err(|e| {
                                FusionError::Execution(format!("Serialize error: {}", e))
                            })?;
                            txn.put(schema_key.as_bytes(), &new_bytes).await?;
                        }
                    }
                }
                txn.delete(meta_key.as_bytes()).await?;
                dropped += 1;
            } else if !if_exists {
                return Err(FusionError::Execution(format!(
                    "Index {} does not exist",
                    index_name
                )));
            }
        }

        Ok(QueryResult::Success {
            message: format!("Dropped {} index(es)", dropped),
        })
    }
}
