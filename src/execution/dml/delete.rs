use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::TableFactor;

use super::super::{Executor, QueryResult};

fn delete_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

impl Executor {
    pub(crate) async fn handle_delete(
        &self,
        delete: &sqlparser::ast::Delete,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        let table_name_str = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                if let Some(table) = tables.first() {
                    if let TableFactor::Table { name, .. } = &table.relation {
                        name.to_string()
                    } else {
                        return Err(FusionError::Execution(
                            "Unsupported DELETE format".to_string(),
                        ));
                    }
                } else {
                    return Err(FusionError::Execution(
                        "Missing table in DELETE".to_string(),
                    ));
                }
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                if let Some(table) = tables.first() {
                    if let TableFactor::Table { name, .. } = &table.relation {
                        name.to_string()
                    } else {
                        return Err(FusionError::Execution(
                            "Unsupported DELETE format".to_string(),
                        ));
                    }
                } else {
                    return Err(FusionError::Execution(
                        "Missing table in DELETE".to_string(),
                    ));
                }
            }
        };

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let prefix = delete_data_prefix_for_table(&table_name_str);
        let allowed_qualifiers = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => tables
                .first()
                .map(|table| Self::primary_key_qualifiers(&table.relation))
                .unwrap_or_default(),
            sqlparser::ast::FromTable::WithoutKeyword(tables) => tables
                .first()
                .map(|table| Self::primary_key_qualifiers(&table.relation))
                .unwrap_or_default(),
        };
        let target_row_id = self.primary_key_row_id_from_eq_selection(
            delete.selection.as_ref(),
            &schema,
            params,
            &allowed_qualifiers,
        );
        let composite_indexes = self
            .load_composite_indexes_for_table(&table_name_str, txn)
            .await?;
        let parent_foreign_keys = self.load_parent_foreign_keys(&table_name_str, txn).await?;
        let trigram_column_indices = Self::indexed_trigram_text_columns(&schema);

        if delete.returning.is_none() {
            let no_secondary_indexes = schema
                .columns
                .iter()
                .all(|col| !col.is_indexed || col.is_primary)
                && composite_indexes.is_empty()
                && parent_foreign_keys.is_empty();
            if no_secondary_indexes {
                if let Some(row_id) = &target_row_id {
                    let key = format!("{}{}", prefix, row_id);
                    if txn.get(key.as_bytes()).await?.is_some() {
                        txn.delete(key.as_bytes()).await?;
                        self.row_cache.invalidate(&key);
                        return Ok(QueryResult::Success {
                            message: "Deleted 1 rows".to_string(),
                        });
                    }

                    return Ok(QueryResult::Success {
                        message: "Deleted 0 rows".to_string(),
                    });
                }

                if delete.selection.is_none() {
                    let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
                    let deleted_count = kv_pairs.len();
                    for (k, _) in kv_pairs {
                        txn.delete(&k).await?;
                        if let Ok(key_str) = std::str::from_utf8(&k) {
                            self.row_cache.invalidate(key_str);
                        }
                    }

                    return Ok(QueryResult::Success {
                        message: format!("Deleted {} rows", deleted_count),
                    });
                }
            }
        }

        let kv_pairs = if let Some(row_id) = target_row_id {
            let key = format!("{}{}", prefix, row_id);
            if let Some(v) = txn.get(key.as_bytes()).await? {
                vec![(key.into_bytes(), v)]
            } else {
                vec![]
            }
        } else {
            txn.scan_prefix(prefix.as_bytes(), None).await?
        };

        let mut deleted_count = 0;
        let mut deleted_rows: Vec<Vec<Value>> = if delete.returning.is_some() {
            Vec::with_capacity(kv_pairs.len())
        } else {
            Vec::new()
        };
        for (k, v) in kv_pairs {
            let row: Vec<Value> = if let Ok(key_str) = std::str::from_utf8(&k) {
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

            let mut delete_flag = true;
            if let Some(selection) = &delete.selection {
                delete_flag = self.evaluate_expr(selection, &row, &schema, params)?;
            }

            if delete_flag {
                self.validate_parent_foreign_key_references(
                    &table_name_str,
                    &schema,
                    &row,
                    None,
                    &parent_foreign_keys,
                    txn,
                )
                .await?;

                txn.delete(&k).await?;

                // Invalidate Cache
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.invalidate(key_str);
                }

                let row_id = Self::row_id_from_data_key(&k)?;
                self.update_trigram_index_for_delete(
                    &table_name_str,
                    &schema,
                    &row,
                    row_id,
                    &trigram_column_indices,
                    txn,
                );

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let val = &row[idx];

                        if col.index_type == IndexType::FTS {
                            if let Value::String(text) = val {
                                for token in Self::tokenize_unique(text) {
                                    let index_key = format!(
                                        "fts:{}:{}:{}:{}",
                                        table_name_str, col.name, token, row_id
                                    );
                                    txn.delete(index_key.as_bytes()).await?;
                                }
                            }
                        } else if col.index_type == IndexType::HNSW {
                            let idx_name = format!("hnsw_{}_{}", table_name_str, col.name);
                            self.vector_index.delete(&idx_name, row_id)?;
                        } else if let Some(val_str) = self.value_to_index_string(val) {
                            let index_key = format!(
                                "index:{}:{}:{}:{}",
                                table_name_str, col.name, val_str, row_id
                            );
                            txn.delete(index_key.as_bytes()).await?;
                        }
                    }
                }
                self.delete_loaded_composite_indexes_for_row(
                    &composite_indexes,
                    &table_name_str,
                    &schema,
                    &row,
                    row_id,
                    txn,
                )
                .await?;

                if delete.returning.is_some() {
                    deleted_rows.push(row.clone());
                }
                deleted_count += 1;
            }
        }

        // Handle RETURNING clause for DELETE
        if let Some(ref ret_items) = delete.returning {
            return self.build_returning_result(ret_items, &deleted_rows, &schema);
        }

        Ok(QueryResult::Success {
            message: format!("Deleted {} rows", deleted_count),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::delete_data_prefix_for_table;

    #[test]
    fn delete_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = delete_data_prefix_for_table("accounts");

        assert_eq!(prefix, "data:accounts:");
        assert!(prefix.capacity() >= prefix.len());
    }
}
