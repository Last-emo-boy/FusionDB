use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::Expr;

use super::super::{Executor, QueryResult};

fn create_index_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn index_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

fn drop_index_prefix_for_column(table_name: &str, column_name: &str) -> String {
    let mut prefix =
        String::with_capacity("index:".len() + table_name.len() + 1 + column_name.len() + 1);
    prefix.push_str("index:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix.push_str(column_name);
    prefix.push(':');
    prefix
}

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
        let meta_key = format!("index_meta:{}", index_name_str);
        if txn.get(meta_key.as_bytes()).await?.is_some() {
            return Err(FusionError::Execution(format!(
                "Index {} already exists",
                index_name_str
            )));
        }

        let schema_key = index_schema_key_for_table(&table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let mut target_col_indices = Vec::with_capacity(columns.len());
        let mut target_col_names = Vec::with_capacity(columns.len());
        for index_col in columns {
            let col_expr = &index_col.column;
            if let Expr::Identifier(ident) = &col_expr.expr {
                if let Some(idx) = schema.get_column_index(&ident.value) {
                    target_col_indices.push(idx);
                    target_col_names.push(schema.columns[idx].name.clone());
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

        if target_col_indices.is_empty() {
            return Err(FusionError::Execution(
                "CREATE INDEX requires at least one column".to_string(),
            ));
        }

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

        if target_col_indices.len() != 1 && index_type != IndexType::BTree {
            return Err(FusionError::Execution(
                "Composite indexes only support BTree".to_string(),
            ));
        }

        if target_col_indices.len() == 1 {
            let col_idx = target_col_indices[0];
            let col_name = schema.columns[col_idx].name.clone();

            schema.columns[col_idx].is_indexed = true;
            schema.columns[col_idx].index_type = index_type.clone();

            // If HNSW, initialize the vector index
            if index_type == IndexType::HNSW {
                let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
                self.vector_index.create_index(&idx_name);
            }
        }
        let new_schema_value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &new_schema_value).await?;

        let prefix = create_index_data_prefix_for_table(&table_name_str);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut count = 0;
        for (k, v) in kv_pairs {
            let key_str = std::str::from_utf8(&k)
                .map_err(|e| FusionError::Execution(format!("Data key decode error: {}", e)))?;
            let row_id = key_str
                .rsplit(':')
                .next()
                .ok_or_else(|| FusionError::Execution("Invalid data key".to_string()))?;

            let row = if target_col_indices.len() > 1 {
                crate::common::encoding::RowDecoder::decode_partial(&v, &target_col_indices)
                    .map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
            } else {
                Vec::new()
            };

            if target_col_indices.len() > 1 {
                if let Some(index_key) = self.composite_index_key(
                    &table_name_str,
                    &target_col_names,
                    &row,
                    &schema,
                    row_id,
                ) {
                    txn.put(index_key.as_bytes(), &[]).await?;
                    count += 1;
                }
                continue;
            }

            let col_idx = target_col_indices[0];
            let col_name = &target_col_names[0];
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
                self.update_trigram_index_for_value(&table_name_str, col_name, &val, row_id, txn);
            } else if index_type == IndexType::HNSW {
                if let Value::Vector(vec) = &val {
                    let idx_name = format!("hnsw_{}_{}", table_name_str, col_name);
                    self.vector_index
                        .insert(&idx_name, row_id.to_string(), vec.clone())?;
                }
            } else {
                if let Some(val_str) = self.value_to_index_string(&val) {
                    let index_key = format!(
                        "index:{}:{}:{}:{}",
                        table_name_str, col_name, val_str, row_id
                    );
                    txn.put(index_key.as_bytes(), &[]).await?;
                } else {
                    continue;
                }
                self.update_trigram_index_for_value(&table_name_str, col_name, &val, row_id, txn);
            }
            count += 1;
        }

        // Store index metadata for DROP INDEX support
        let meta_val = if target_col_names.len() == 1 {
            format!("{}:{}", table_name_str, target_col_names[0])
        } else {
            Self::composite_index_meta_value(&table_name_str, &target_col_names)
        };
        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;
        if target_col_names.len() > 1 {
            self.rebuild_composite_index_directory_for_table(&table_name_str, txn)
                .await?;
        }

        Ok(QueryResult::Success {
            message: format!(
                "Index {} ({:?}) created on {}({}), indexed {} rows",
                index_name_str,
                index_type,
                table_name_str,
                target_col_names.join(", "),
                count
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
                let meta_str = String::from_utf8(meta_bytes).unwrap_or_default();
                if let Some(meta) = Self::parse_index_meta(&index_name, &meta_str) {
                    let table_name = meta.table;

                    // Delete index entries
                    let index_prefix = if meta.columns.len() == 1 {
                        drop_index_prefix_for_column(&table_name, &meta.columns[0])
                    } else {
                        Self::composite_index_prefix(&table_name, &meta.columns)
                    };
                    let entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
                    for (k, _) in entries {
                        txn.delete(&k).await?;
                    }

                    // Update schema: mark column as not indexed
                    if meta.columns.len() == 1 {
                        let schema_key = index_schema_key_for_table(&table_name);
                        if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                            if let Ok(mut schema) =
                                bincode::deserialize::<TableSchema>(&schema_bytes)
                            {
                                for col in &mut schema.columns {
                                    if col.name == meta.columns[0] {
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
                    } else {
                        let table_meta_key =
                            Self::composite_index_table_meta_key(&table_name, &index_name);
                        txn.delete(table_meta_key.as_bytes()).await?;
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

#[cfg(test)]
mod tests {
    use super::{
        create_index_data_prefix_for_table, drop_index_prefix_for_column,
        index_schema_key_for_table,
    };

    #[test]
    fn index_schema_key_for_table_preallocates_exact_key() {
        let key = index_schema_key_for_table("orders");

        assert_eq!(key, "schema:orders");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn create_index_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = create_index_data_prefix_for_table("orders");

        assert_eq!(prefix, "data:orders:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn drop_index_prefix_for_column_preallocates_exact_prefix() {
        let prefix = drop_index_prefix_for_column("orders", "status");

        assert_eq!(prefix, "index:orders:status:");
        assert!(prefix.capacity() >= prefix.len());
    }
}
