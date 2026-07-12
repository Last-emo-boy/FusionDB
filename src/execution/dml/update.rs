use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::TableFactor;

use super::super::{Executor, QueryResult};
use super::UniqueColumnValueSets;

#[cfg(test)]
fn update_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

#[cfg(test)]
fn update_data_key_for_prefix_row(prefix: &str, row_id: &str) -> String {
    let mut key = String::with_capacity(prefix.len() + row_id.len());
    key.push_str(prefix);
    key.push_str(row_id);
    key
}

#[cfg(test)]
fn update_index_key_for_value(
    table_name: &str,
    column_name: &str,
    value: &str,
    row_id: &str,
) -> String {
    let mut key = String::with_capacity(
        "index:".len()
            + table_name.len()
            + 1
            + column_name.len()
            + 1
            + value.len()
            + 1
            + row_id.len(),
    );
    key.push_str("index:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(column_name);
    key.push(':');
    key.push_str(value);
    key.push(':');
    key.push_str(row_id);
    key
}

fn update_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

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

        let schema_key = update_schema_key_for_table(&table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        // Optimization: Check for Primary Key (Clustered Index) Update
        let allowed_qualifiers = Self::primary_key_qualifiers(relation);
        let target_row_id = self.primary_key_row_id_from_eq_selection(
            update.selection.as_ref(),
            &schema,
            params,
            &allowed_qualifiers,
        );
        if let Some(row_id) = target_row_id.as_deref() {
            if let Some(result) = self
                .try_handle_simple_primary_key_update(
                    update,
                    &table_name_str,
                    &schema,
                    row_id,
                    txn,
                    params,
                )
                .await?
            {
                return Ok(result);
            }
        }
        let composite_indexes = self
            .load_composite_indexes_for_table(&table_name_str, txn)
            .await?;
        let composite_unique_indexes = self
            .load_composite_unique_indexes_for_table(&table_name_str, txn)
            .await?;
        let single_column_index_includes = self
            .load_single_column_index_includes_for_table(&table_name_str, &schema, txn)
            .await?;
        let child_foreign_keys = self.load_child_foreign_keys(&table_name_str, txn).await?;
        let parent_foreign_keys = self.load_parent_foreign_keys(&table_name_str, txn).await?;
        let trigram_column_indices = Self::indexed_trigram_text_columns(&schema);

        let kv_pairs = if let Some(row_id) = target_row_id {
            // Point Lookup
            let key = self.routed_data_key_for_row_id(&table_name_str, &row_id);
            if let Some(v) = txn.get(key.as_bytes()).await? {
                vec![(key.into_bytes(), v)]
            } else {
                vec![]
            }
        } else {
            // Full Scan Fallback
            self.scan_routed_data_prefixes_for_table(&table_name_str, txn, None)
                .await?
        };

        let mut updated_count = 0;
        let mut updated_rows: Vec<Vec<Value>> = if update.returning.is_some() {
            Vec::with_capacity(kv_pairs.len())
        } else {
            Vec::new()
        };
        let mut unique_sets = UniqueColumnValueSets::new();
        for (k, v) in kv_pairs {
            let mut row: Vec<Value> = if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(row) = self.row_cache_lookup(key_str, &v) {
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

                super::reject_primary_key_change(
                    &schema,
                    &composite_unique_indexes,
                    &old_row,
                    &row,
                    "UPDATE",
                )?;

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
                for col in schema.columns.iter() {
                    if let Some(ref check_sql) = col.check_expr {
                        if !self.evaluate_check_constraint(check_sql, &schema, &row) {
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
                let row_id = self.legacy_row_id_from_routed_data_key(&table_name_str, &k)?;
                self.validate_composite_unique_constraints(
                    &composite_unique_indexes,
                    &table_name_str,
                    &schema,
                    &row,
                    Some(row_id),
                    txn,
                )
                .await?;

                self.check_unique_columns_for_update(
                    &table_name_str,
                    &schema,
                    &old_row,
                    &row,
                    row_id,
                    &mut unique_sets,
                    txn,
                )
                .await?;

                let new_value_bytes = crate::common::encoding::RowEncoder::encode(&row);
                self.write_routed_data_row(&table_name_str, row_id, &new_value_bytes, txn)
                    .await?;
                self.migrate_unique_sentinels_for_update(
                    &table_name_str,
                    &schema,
                    &old_row,
                    &row,
                    row_id,
                    txn,
                )
                .await?;
                unique_sets.track_update(&schema, &old_row, &row, row_id);

                self.update_trigram_index_for_update(
                    &table_name_str,
                    &schema,
                    &old_row,
                    &row,
                    row_id,
                    &trigram_column_indices,
                    txn,
                );

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let old_val = &old_row[idx];
                        let new_val = &row[idx];
                        let include_indices = single_column_index_includes
                            .get(&idx)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        let key_changed = old_val != new_val;
                        let include_changed = Self::single_column_index_payload_touched(
                            &old_row,
                            &row,
                            include_indices,
                        );

                        if key_changed || include_changed {
                            if col.index_type == IndexType::FTS {
                                if !key_changed {
                                    continue;
                                }
                                if let Value::String(text) = old_val {
                                    for token in Self::tokenize_unique(text) {
                                        let index_key = self.routed_fts_index_key_for_row(
                                            &table_name_str,
                                            &col.name,
                                            &token,
                                            row_id,
                                        );
                                        txn.delete(index_key.as_bytes()).await?;
                                    }
                                }
                                if let Value::String(text) = new_val {
                                    for token in Self::tokenize_unique(text) {
                                        let index_key = self.routed_fts_index_key_for_row(
                                            &table_name_str,
                                            &col.name,
                                            &token,
                                            row_id,
                                        );
                                        txn.put(index_key.as_bytes(), &[]).await?;
                                    }
                                }
                            } else if col.index_type == IndexType::HNSW {
                                if !key_changed {
                                    continue;
                                }
                                let idx_name =
                                    Self::hnsw_index_name_for_column(&table_name_str, &col.name)?;
                                if matches!(old_val, Value::Vector(_)) {
                                    self.defer_or_apply_vector_delete(&idx_name, row_id, txn)?;
                                }
                                if let Value::Vector(vec) = new_val {
                                    self.defer_or_apply_vector_insert(
                                        &idx_name,
                                        row_id.to_string(),
                                        vec.clone(),
                                        txn,
                                    )?;
                                }
                            } else {
                                let old_val_str = self.value_to_index_string(old_val);
                                if let Some(old_val_str) = old_val_str.as_deref() {
                                    let old_index_key = self.routed_index_key_for_value(
                                        &table_name_str,
                                        &col.name,
                                        old_val_str,
                                        row_id,
                                    );
                                    txn.delete(old_index_key.as_bytes()).await?;
                                }

                                let new_val_str = self.value_to_index_string(new_val);
                                if let Some(new_val_str) = new_val_str.as_deref() {
                                    let new_index_key = self.routed_index_key_for_value(
                                        &table_name_str,
                                        &col.name,
                                        new_val_str,
                                        row_id,
                                    );
                                    let payload = Self::secondary_index_payload_for_row(
                                        &row,
                                        include_indices,
                                    );
                                    txn.put(new_index_key.as_bytes(), &payload).await?;
                                }
                                if old_val_str != new_val_str {
                                    if let Some(old_val_str) = old_val_str.as_deref() {
                                        self.adjust_index_count_summary(
                                            &table_name_str,
                                            &col.name,
                                            old_val_str,
                                            -1,
                                            txn,
                                        )
                                        .await?;
                                    }
                                    if let Some(new_val_str) = new_val_str.as_deref() {
                                        self.adjust_index_count_summary(
                                            &table_name_str,
                                            &col.name,
                                            new_val_str,
                                            1,
                                            txn,
                                        )
                                        .await?;
                                    }
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

    async fn try_handle_simple_primary_key_update(
        &self,
        update: &sqlparser::ast::Update,
        table_name: &str,
        schema: &TableSchema,
        row_id: &str,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if update.returning.is_some() {
            return Ok(None);
        }
        if schema
            .columns
            .iter()
            .any(|column| column.check_expr.is_some())
        {
            return Ok(None);
        }
        let mut assignment_indices = Vec::with_capacity(update.assignments.len());
        for assignment in &update.assignments {
            let col_name = match &assignment.target {
                sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                _ => return Ok(None),
            };
            let Some(col_idx) = schema.get_column_index(&col_name) else {
                return Err(FusionError::Execution(format!(
                    "Column {} not found in assignment",
                    col_name
                )));
            };
            if schema.columns[col_idx].is_indexed
                && !(schema.columns[col_idx].is_primary
                    && schema.columns[col_idx].index_type == IndexType::BTree)
            {
                return Ok(None);
            }
            assignment_indices.push(col_idx);
        }
        let single_column_index_includes = self
            .load_single_column_index_includes_for_table(table_name, schema, txn)
            .await?;
        if single_column_index_includes
            .values()
            .any(|include_indices| {
                include_indices
                    .iter()
                    .any(|include_idx| assignment_indices.contains(include_idx))
            })
        {
            return Ok(None);
        }
        let metadata_cache_key = table_name.to_string();
        let metadata_allows_fast_path = if let Some(cached) = self
            .simple_pk_update_fast_path_cache
            .get(&metadata_cache_key)
        {
            cached
        } else {
            let has_composite_index = !self
                .load_composite_indexes_for_table(table_name, txn)
                .await?
                .is_empty();
            let foreign_key_child_prefix = Self::foreign_key_child_prefix_for_table(table_name);
            let foreign_key_parent_prefix = Self::foreign_key_parent_prefix_for_table(table_name);
            let has_foreign_key = txn
                .scan_prefix(foreign_key_child_prefix.as_bytes(), Some(1))
                .await?
                .len()
                > 0
                || txn
                    .scan_prefix(foreign_key_parent_prefix.as_bytes(), Some(1))
                    .await?
                    .len()
                    > 0;
            let allowed = !has_composite_index && !has_foreign_key;
            // 肯定结果只对当前快照成立；并发 DDL 后重试时必须重新读取元数据。
            if !allowed {
                self.simple_pk_update_fast_path_cache
                    .insert(metadata_cache_key, false);
            }
            allowed
        };
        if !metadata_allows_fast_path {
            return Ok(None);
        }

        let key = self.routed_data_key_for_row_id(table_name, row_id);
        let Some(value) = txn.get(key.as_bytes()).await? else {
            return Ok(Some(QueryResult::Success {
                message: "Updated 0 rows".to_string(),
            }));
        };

        let mut row: Vec<Value> = if let Some(cached) = self.row_cache_lookup(&key, &value) {
            cached
        } else {
            crate::common::encoding::RowDecoder::decode(&value)
                .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?
        };
        let old_row = row.clone();

        for (assignment, &col_idx) in update.assignments.iter().zip(&assignment_indices) {
            let new_val = self.evaluate_value(&assignment.value, &old_row, schema, params)?;
            let new_val =
                Self::coerce_value_to_column_type(new_val, &schema.columns[col_idx].data_type)?;
            if schema.columns[col_idx].is_primary && old_row.get(col_idx) != Some(&new_val) {
                return Ok(None);
            }
            row[col_idx] = new_val;
        }

        for (idx, col) in schema.columns.iter().enumerate() {
            if !col.is_nullable && row[idx] == Value::Null {
                return Err(FusionError::Execution(format!(
                    "NOT NULL constraint violated for column '{}' during UPDATE",
                    col.name
                )));
            }
        }

        let mut unique_sets = UniqueColumnValueSets::new();
        self.check_unique_columns_for_update(
            table_name,
            schema,
            &old_row,
            &row,
            row_id,
            &mut unique_sets,
            txn,
        )
        .await?;

        self.touch_index_build_barrier_for_row(table_name, row_id, txn)
            .await?;
        let new_value_bytes = crate::common::encoding::RowEncoder::encode(&row);
        self.write_routed_data_row(table_name, row_id, &new_value_bytes, txn)
            .await?;
        self.migrate_unique_sentinels_for_update(table_name, schema, &old_row, &row, row_id, txn)
            .await?;
        monitor::inc_row_write();

        Ok(Some(QueryResult::Success {
            message: "Updated 1 rows".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        update_data_key_for_prefix_row, update_data_prefix_for_table, update_index_key_for_value,
        update_schema_key_for_table,
    };

    #[test]
    fn update_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = update_data_prefix_for_table("accounts");

        assert_eq!(prefix, "data:accounts:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn update_data_key_for_prefix_row_preallocates_exact_key() {
        let prefix = update_data_prefix_for_table("accounts");
        let key = update_data_key_for_prefix_row(&prefix, "00042");

        assert_eq!(key, "data:accounts:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn update_index_key_for_value_preallocates_exact_key() {
        let key = update_index_key_for_value("accounts", "status", "active", "00042");

        assert_eq!(key, "index:accounts:status:active:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn update_schema_key_for_table_preallocates_exact_key() {
        let key = update_schema_key_for_table("accounts");

        assert_eq!(key, "schema:accounts");
        assert!(key.capacity() >= key.len());
    }
}
