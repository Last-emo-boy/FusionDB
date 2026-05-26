use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::TableFactor;

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_update(
        &self,
        update: &sqlparser::ast::Update,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        let sqlparser::ast::TableWithJoins { relation, .. } = &update.table;
        let table_name_str = if let TableFactor::Table { name, .. } = relation {
            name.to_string()
        } else {
            return Err(FusionError::Execution(
                "Unsupported UPDATE format".to_string(),
            ));
        };

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let prefix = format!("data:{}:", table_name_str);

        // Optimization: Check for Primary Key (Clustered Index) Update
        let allowed_qualifiers = Self::primary_key_qualifiers(relation);
        let target_row_id = self.primary_key_row_id_from_eq_selection(
            update.selection.as_ref(),
            &schema,
            params,
            &allowed_qualifiers,
        );
        let composite_indexes = self
            .load_composite_indexes_for_table(&table_name_str, txn)
            .await?;
        let child_foreign_keys = self.load_child_foreign_keys(&table_name_str, txn).await?;
        let parent_foreign_keys = self.load_parent_foreign_keys(&table_name_str, txn).await?;

        let kv_pairs = if let Some(row_id) = target_row_id {
            // Point Lookup
            let key = format!("{}{}", prefix, row_id);
            if let Some(v) = txn.get(key.as_bytes()).await? {
                vec![(key.into_bytes(), v)]
            } else {
                vec![]
            }
        } else {
            // Full Scan Fallback
            txn.scan_prefix(prefix.as_bytes(), None).await?
        };

        let mut updated_count = 0;
        let mut updated_rows: Vec<Vec<Value>> = if update.returning.is_some() {
            Vec::with_capacity(kv_pairs.len())
        } else {
            Vec::new()
        };
        for (k, v) in kv_pairs {
            let mut row: Vec<Value> = if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(row) = self.row_cache.get(key_str) {
                    monitor::inc_row_cache_hit();
                    row
                } else {
                    crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                }
            } else {
                crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                    FusionError::Execution(format!("Data deserialization error: {}", e))
                })?
            };
            let old_row = row.clone();

            let mut update_flag = true;
            if let Some(selection_expr) = &update.selection {
                update_flag = self.evaluate_expr(selection_expr, &row, &schema, params)?;
            }

            if update_flag {
                for assignment in &update.assignments {
                    let col_name = match &assignment.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                        _ => {
                            return Err(FusionError::Execution(
                                "Unsupported assignment target".to_string(),
                            ))
                        }
                    };

                    if let Some(col_idx) = schema.get_column_index(&col_name) {
                        let new_val =
                            self.evaluate_value(&assignment.value, &old_row, &schema, params)?;
                        row[col_idx] = Self::coerce_value_to_column_type(
                            new_val,
                            &schema.columns[col_idx].data_type,
                        )?;
                    } else {
                        return Err(FusionError::Execution(format!(
                            "Column {} not found in assignment",
                            col_name
                        )));
                    }
                }

                // Enforce NOT NULL constraints after UPDATE
                for (idx, col) in schema.columns.iter().enumerate() {
                    if !col.is_nullable && row[idx] == Value::Null {
                        return Err(FusionError::Execution(format!(
                            "NOT NULL constraint violated for column '{}' during UPDATE",
                            col.name
                        )));
                    }
                }

                // Enforce CHECK constraints after UPDATE
                for (idx, col) in schema.columns.iter().enumerate() {
                    if let Some(ref check_sql) = col.check_expr {
                        if !self.evaluate_check_constraint(check_sql, &col.name, &row[idx]) {
                            return Err(FusionError::Execution(format!(
                                "CHECK constraint violated for column '{}': {}",
                                col.name, check_sql
                            )));
                        }
                    }
                }
                self.validate_child_foreign_keys(
                    &table_name_str,
                    &schema,
                    &row,
                    &child_foreign_keys,
                    txn,
                )
                .await?;
                self.validate_parent_foreign_key_references(
                    &table_name_str,
                    &schema,
                    &old_row,
                    Some(&row),
                    &parent_foreign_keys,
                    txn,
                )
                .await?;

                let new_value_bytes = crate::common::encoding::RowEncoder::encode(&row);
                txn.put(&k, &new_value_bytes).await?;
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.invalidate(key_str);
                }

                let row_id = Self::row_id_from_data_key(&k)?;

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let old_val = &old_row[idx];
                        let new_val = &row[idx];

                        if old_val != new_val {
                            if col.index_type == IndexType::FTS {
                                if let Value::String(text) = old_val {
                                    for token in Self::tokenize_unique(text) {
                                        let index_key = format!(
                                            "fts:{}:{}:{}:{}",
                                            table_name_str, col.name, token, row_id
                                        );
                                        txn.delete(index_key.as_bytes()).await?;
                                    }
                                }
                                if let Value::String(text) = new_val {
                                    for token in Self::tokenize_unique(text) {
                                        let index_key = format!(
                                            "fts:{}:{}:{}:{}",
                                            table_name_str, col.name, token, row_id
                                        );
                                        txn.put(index_key.as_bytes(), &[]).await?;
                                    }
                                }
                            } else if col.index_type == IndexType::HNSW {
                                let idx_name = format!("hnsw_{}_{}", table_name_str, col.name);
                                if matches!(old_val, Value::Vector(_)) {
                                    self.vector_index.delete(&idx_name, row_id)?;
                                }
                                if let Value::Vector(vec) = new_val {
                                    self.vector_index.insert(
                                        &idx_name,
                                        row_id.to_string(),
                                        vec.clone(),
                                    )?;
                                }
                            } else {
                                if let Some(old_val_str) = self.value_to_index_string(old_val) {
                                    let old_index_key = format!(
                                        "index:{}:{}:{}:{}",
                                        table_name_str, col.name, old_val_str, row_id
                                    );
                                    txn.delete(old_index_key.as_bytes()).await?;
                                }

                                if let Some(new_val_str) = self.value_to_index_string(new_val) {
                                    let new_index_key = format!(
                                        "index:{}:{}:{}:{}",
                                        table_name_str, col.name, new_val_str, row_id
                                    );
                                    txn.put(new_index_key.as_bytes(), &[]).await?;
                                }
                            }
                        }
                    }
                }
                self.update_loaded_composite_indexes_for_row(
                    &composite_indexes,
                    &table_name_str,
                    &schema,
                    &old_row,
                    &row,
                    row_id,
                    txn,
                )
                .await?;

                if update.returning.is_some() {
                    updated_rows.push(row.clone());
                }
                updated_count += 1;
            }
        }

        // Handle RETURNING clause for UPDATE
        if let Some(ref ret_items) = update.returning {
            return self.build_returning_result(ret_items, &updated_rows, &schema);
        }

        Ok(QueryResult::Success {
            message: format!("Updated {} rows", updated_count),
        })
    }
}
