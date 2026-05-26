use crate::catalog::{IndexType, TableSchema};
use crate::common::encoding::encode_key;
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::SetExpr;

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_insert(
        &self,
        table_name: String,
        columns: &[sqlparser::ast::Ident],
        source: &Option<Box<sqlparser::ast::Query>>,
        returning: &Option<Vec<sqlparser::ast::SelectItem>>,
        on_conflict: &Option<sqlparser::ast::OnInsert>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        let table_name_str = table_name;

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
        let composite_indexes = self
            .load_composite_indexes_for_table(&table_name_str, txn)
            .await?;
        let foreign_keys = self.load_child_foreign_keys(&table_name_str, txn).await?;

        // Build column index mapping if explicit column list provided
        let col_mapping: Option<Vec<usize>> = if !columns.is_empty() {
            let mut mapping = Vec::with_capacity(columns.len());
            for col_ident in columns {
                let col_name = col_ident.value.clone();
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&col_name))
                    .ok_or_else(|| {
                        FusionError::Execution(format!(
                            "Column {} not found in table {}",
                            col_name, table_name_str
                        ))
                    })?;
                mapping.push(idx);
            }
            Some(mapping)
        } else {
            None
        };

        let mut inserted_rows: Vec<Vec<Value>> = Vec::new();

        if let Some(query) = source {
            if let SetExpr::Values(values) = &query.body.as_ref() {
                if returning.is_some() {
                    inserted_rows.reserve(values.rows.len());
                }
                let mut count = 0;
                for row in &values.rows {
                    let mut raw_values = Vec::with_capacity(row.len());
                    for expr in row.iter() {
                        let val = self
                            .evaluate_value(expr, &[], &schema, params)
                            .unwrap_or(Value::Null);
                        raw_values.push(val);
                    }

                    // If column list specified, map values to full row with defaults for missing columns
                    let row_values = if let Some(ref mapping) = col_mapping {
                        if raw_values.len() != mapping.len() {
                            return Err(FusionError::Execution(
                                "Column count mismatch".to_string(),
                            ));
                        }
                        let mut full_row: Vec<Value> = schema
                            .columns
                            .iter()
                            .map(|col| {
                                // Use DEFAULT value if specified, otherwise NULL
                                if let Some(ref def_str) = col.default_value {
                                    self.parse_default_value(def_str)
                                        .and_then(|value| {
                                            Self::coerce_value_to_column_type(value, &col.data_type)
                                        })
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            })
                            .collect();
                        for (i, &schema_idx) in mapping.iter().enumerate() {
                            full_row[schema_idx] = raw_values[i].clone();
                        }
                        full_row
                    } else {
                        raw_values
                    };

                    if row_values.len() != schema.columns.len() {
                        return Err(FusionError::Execution("Column count mismatch".to_string()));
                    }
                    let row_values = self.coerce_row_to_schema(row_values, &schema)?;

                    // Enforce NOT NULL constraints
                    for (idx, col) in schema.columns.iter().enumerate() {
                        if !col.is_nullable && row_values[idx] == Value::Null {
                            return Err(FusionError::Execution(format!(
                                "NOT NULL constraint violated for column '{}'",
                                col.name
                            )));
                        }
                    }

                    // Enforce CHECK constraints
                    for (idx, col) in schema.columns.iter().enumerate() {
                        if let Some(ref check_sql) = col.check_expr {
                            let check_result = self.evaluate_check_constraint(
                                check_sql,
                                &col.name,
                                &row_values[idx],
                            );
                            if !check_result {
                                return Err(FusionError::Execution(format!(
                                    "CHECK constraint violated for column '{}': {}",
                                    col.name, check_sql
                                )));
                            }
                        }
                    }

                    // Enforce UNIQUE constraints (non-PK unique columns)
                    for (idx, col) in schema.columns.iter().enumerate() {
                        if col.is_unique && !col.is_primary && row_values[idx] != Value::Null {
                            // Scan existing rows for duplicate value
                            let prefix = format!("data:{}:", table_name_str);
                            let existing = txn.scan_prefix(prefix.as_bytes(), None).await?;
                            for (k, v) in &existing {
                                let existing_value = if let Ok(key_str) = std::str::from_utf8(k) {
                                    if let Some(row) = self.row_cache.get(key_str) {
                                        monitor::inc_row_cache_hit();
                                        row.get(idx).cloned().unwrap_or(Value::Null)
                                    } else {
                                        crate::common::encoding::RowDecoder::decode_column(v, idx)
                                            .map_err(|e| {
                                                FusionError::Execution(format!(
                                                    "Decode error: {}",
                                                    e
                                                ))
                                            })?
                                            .unwrap_or(Value::Null)
                                    }
                                } else {
                                    crate::common::encoding::RowDecoder::decode_column(v, idx)
                                        .map_err(|e| {
                                            FusionError::Execution(format!("Decode error: {}", e))
                                        })?
                                        .unwrap_or(Value::Null)
                                };
                                if existing_value == row_values[idx] {
                                    return Err(FusionError::Execution(format!(
                                        "UNIQUE constraint violated for column '{}': duplicate value '{}'",
                                        col.name, crate::common::encoding::encode_key(&row_values[idx])
                                    )));
                                }
                            }
                        }
                    }

                    let row_id = if let Some(first) = row_values.first() {
                        encode_key(first)
                    } else {
                        uuid::Uuid::new_v4().to_string()
                    };

                    let key = format!("data:{}:{}", table_name_str, row_id);

                    // Handle ON CONFLICT (UPSERT)
                    if let Some(sqlparser::ast::OnInsert::OnConflict(oc)) = on_conflict {
                        if let Some(existing_bytes) = txn.get(key.as_bytes()).await? {
                            match &oc.action {
                                sqlparser::ast::OnConflictAction::DoNothing => {
                                    // Skip this row
                                    count += 1;
                                    continue;
                                }
                                sqlparser::ast::OnConflictAction::DoUpdate(do_update) => {
                                    // Load existing row, apply assignments using EXCLUDED references
                                    let mut existing_row: Vec<Value> = if let Some(row) =
                                        self.row_cache.get(&key)
                                    {
                                        monitor::inc_row_cache_hit();
                                        row
                                    } else {
                                        crate::common::encoding::RowDecoder::decode(&existing_bytes)
                                            .map_err(|e| {
                                                FusionError::Execution(format!(
                                                    "Decode error: {}",
                                                    e
                                                ))
                                            })?
                                    };
                                    let old_existing_row = existing_row.clone();
                                    for assignment in &do_update.assignments {
                                        let col_name = match &assignment.target {
                                            sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                                name.to_string()
                                            }
                                            _ => continue,
                                        };
                                        if let Some(col_idx) = schema.get_column_index(&col_name) {
                                            // Evaluate the value expression; EXCLUDED.col references map to the new row_values
                                            let new_val = self.evaluate_upsert_value(
                                                &assignment.value,
                                                &existing_row,
                                                &row_values,
                                                &schema,
                                            )?;
                                            existing_row[col_idx] =
                                                Self::coerce_value_to_column_type(
                                                    new_val,
                                                    &schema.columns[col_idx].data_type,
                                                )?;
                                        }
                                    }
                                    self.validate_child_foreign_keys(
                                        &table_name_str,
                                        &schema,
                                        &existing_row,
                                        &foreign_keys,
                                        txn,
                                    )
                                    .await?;
                                    let value =
                                        crate::common::encoding::RowEncoder::encode(&existing_row);
                                    txn.put(key.as_bytes(), &value).await?;
                                    self.row_cache.invalidate(&key);
                                    for (idx, col) in schema.columns.iter().enumerate() {
                                        if col.is_indexed && col.index_type == IndexType::HNSW {
                                            let idx_name =
                                                format!("hnsw_{}_{}", table_name_str, col.name);
                                            if idx < old_existing_row.len()
                                                && old_existing_row[idx] != existing_row[idx]
                                            {
                                                if matches!(old_existing_row[idx], Value::Vector(_))
                                                {
                                                    self.vector_index.delete(&idx_name, &row_id)?;
                                                }
                                                if let Value::Vector(vec) = &existing_row[idx] {
                                                    self.vector_index.insert(
                                                        &idx_name,
                                                        row_id.clone(),
                                                        vec.clone(),
                                                    )?;
                                                }
                                            }
                                        }
                                    }
                                    self.update_loaded_composite_indexes_for_row(
                                        &composite_indexes,
                                        &table_name_str,
                                        &schema,
                                        &old_existing_row,
                                        &existing_row,
                                        &row_id,
                                        txn,
                                    )
                                    .await?;
                                    monitor::inc_row_write();
                                    if returning.is_some() {
                                        inserted_rows.push(existing_row);
                                    }
                                    count += 1;
                                    continue;
                                }
                            }
                        }
                    }

                    self.validate_child_foreign_keys(
                        &table_name_str,
                        &schema,
                        &row_values,
                        &foreign_keys,
                        txn,
                    )
                    .await?;

                    let value = crate::common::encoding::RowEncoder::encode(&row_values);
                    txn.put(key.as_bytes(), &value).await?;
                    self.row_cache.invalidate(&key);
                    monitor::inc_row_write();

                    // Update Cache
                    // self.row_cache.insert(key.clone(), row_values.clone());

                    // Update Trigram Index
                    if let Some(ftxn) = txn
                        .as_any()
                        .downcast_ref::<crate::storage::fusion::FusionTransaction>()
                    {
                        let storage = &ftxn.storage;
                        let mut idx_lock = storage.trigram_index.write().unwrap();

                        let numeric_id = if let Some(n) =
                            crate::common::encoding::decode_i64_comparable(&row_id)
                        {
                            Some(n as u64)
                        } else if let Ok(n) = row_id.parse::<u64>() {
                            Some(n)
                        } else {
                            // Fallback: Hash the ID
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            row_id.hash(&mut hasher);
                            Some(hasher.finish())
                        };

                        if let Some(rid) = numeric_id {
                            for (i, val) in row_values.iter().enumerate() {
                                if let Value::String(s) = val {
                                    idx_lock.add_with_id_str(
                                        &table_name_str,
                                        &schema.columns[i].name,
                                        rid,
                                        &row_id,
                                        s,
                                    );
                                }
                            }
                        }
                    }

                    for (idx, col) in schema.columns.iter().enumerate() {
                        if col.is_indexed {
                            let val = &row_values[idx];

                            if col.index_type == IndexType::FTS {
                                if let Value::String(text) = val {
                                    for token in Self::tokenize_unique(text) {
                                        let index_key = format!(
                                            "fts:{}:{}:{}:{}",
                                            table_name_str, col.name, token, row_id
                                        );
                                        txn.put(index_key.as_bytes(), &[]).await?;
                                    }
                                }
                            } else if col.index_type == IndexType::HNSW {
                                if let Value::Vector(vec) = val {
                                    let idx_name = format!("hnsw_{}_{}", table_name_str, col.name);
                                    self.vector_index.insert(
                                        &idx_name,
                                        row_id.clone(),
                                        vec.clone(),
                                    )?;
                                }
                            } else if let Some(val_str) = self.value_to_index_string(val) {
                                let index_key = format!(
                                    "index:{}:{}:{}:{}",
                                    table_name_str, col.name, val_str, row_id
                                );
                                txn.put(index_key.as_bytes(), &[]).await?;
                            }
                        }
                    }

                    self.put_loaded_composite_indexes_for_row(
                        &composite_indexes,
                        &table_name_str,
                        &schema,
                        &row_values,
                        &row_id,
                        txn,
                    )
                    .await?;

                    if returning.is_some() {
                        inserted_rows.push(row_values.clone());
                    }
                    count += 1;
                }

                // Handle RETURNING clause
                if let Some(ret_items) = returning {
                    return self.build_returning_result(ret_items, &inserted_rows, &schema);
                }

                return Ok(QueryResult::Success {
                    message: format!("Inserted {} rows", count),
                });
            }

            // INSERT ... SELECT: execute query then insert results
            let query_result = self.handle_query(query, txn, params).await?;
            if let QueryResult::Select {
                rows: select_rows, ..
            } = query_result
            {
                let mut count = 0;
                for row_values in select_rows {
                    if row_values.len() != schema.columns.len() {
                        return Err(FusionError::Execution(
                            "Column count mismatch in INSERT ... SELECT".to_string(),
                        ));
                    }
                    let row_values = self.coerce_row_to_schema(row_values, &schema)?;
                    let row_id = if let Some(first) = row_values.first() {
                        encode_key(first)
                    } else {
                        uuid::Uuid::new_v4().to_string()
                    };
                    let key = format!("data:{}:{}", table_name_str, row_id);
                    self.validate_child_foreign_keys(
                        &table_name_str,
                        &schema,
                        &row_values,
                        &foreign_keys,
                        txn,
                    )
                    .await?;
                    let value = crate::common::encoding::RowEncoder::encode(&row_values);
                    txn.put(key.as_bytes(), &value).await?;
                    monitor::inc_row_write();

                    for (idx, col) in schema.columns.iter().enumerate() {
                        if col.is_indexed && col.index_type == IndexType::HNSW {
                            if let Value::Vector(vec) = &row_values[idx] {
                                let idx_name = format!("hnsw_{}_{}", table_name_str, col.name);
                                self.vector_index
                                    .insert(&idx_name, row_id.clone(), vec.clone())?;
                            }
                        }
                    }
                    self.put_loaded_composite_indexes_for_row(
                        &composite_indexes,
                        &table_name_str,
                        &schema,
                        &row_values,
                        &row_id,
                        txn,
                    )
                    .await?;
                    count += 1;
                }
                return Ok(QueryResult::Success {
                    message: format!("Inserted {} rows", count),
                });
            }
        }

        Err(FusionError::Execution(
            "Unsupported INSERT format".to_string(),
        ))
    }

    pub(super) fn evaluate_upsert_value(
        &self,
        expr: &sqlparser::ast::Expr,
        existing_row: &[Value],
        new_row: &[Value],
        schema: &TableSchema,
    ) -> Result<Value> {
        // Handle EXCLUDED.column references — map to the new (conflicting) row values
        if let sqlparser::ast::Expr::CompoundIdentifier(idents) = expr {
            if idents.len() == 2 && idents[0].value.eq_ignore_ascii_case("EXCLUDED") {
                let col_name = &idents[1].value;
                if let Some(idx) = schema.get_column_index(col_name) {
                    if idx < new_row.len() {
                        return Ok(new_row[idx].clone());
                    }
                }
                // Case-insensitive fallback
                for (i, col) in schema.columns.iter().enumerate() {
                    if col.name.eq_ignore_ascii_case(col_name) && i < new_row.len() {
                        return Ok(new_row[i].clone());
                    }
                }
                return Ok(Value::Null);
            }
        }
        // For non-EXCLUDED expressions, evaluate against the existing row
        self.evaluate_value(expr, existing_row, schema, &[])
    }
}
