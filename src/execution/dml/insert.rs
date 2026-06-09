use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::{Ident, OnInsert, SelectItem, SetExpr};

use super::super::composite_index::CompositeIndexMeta;
use super::super::{Executor, ForeignKeyMeta, QueryResult};

struct InsertRowsContext {
    table_name: String,
    schema: TableSchema,
    composite_indexes: Vec<CompositeIndexMeta>,
    composite_unique_indexes: Vec<CompositeIndexMeta>,
    foreign_keys: Vec<ForeignKeyMeta>,
    trigram_column_indices: Vec<usize>,
    col_mapping: Option<Vec<usize>>,
}

fn insert_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
    let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
    key.push_str("data:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(row_id);
    key
}

fn insert_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn insert_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

impl Executor {
    fn insert_trace_enabled() -> bool {
        std::env::var("FUSIONDB_INSERT_TRACE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
    }

    fn insert_trace(message: impl AsRef<str>) {
        if Self::insert_trace_enabled() {
            eprintln!("[insert-trace] {}", message.as_ref());
        }
    }

    fn serial_default_candidate_column_indexes(schema: &TableSchema) -> Vec<usize> {
        let mut indices = Vec::with_capacity(schema.columns.len());
        for (idx, column) in schema.columns.iter().enumerate() {
            let ty = column.data_type.trim().to_ascii_uppercase();
            if matches!(
                ty.as_str(),
                "SERIAL" | "SERIAL2" | "SERIAL4" | "SERIAL8" | "SMALLSERIAL" | "BIGSERIAL"
            ) {
                indices.push(idx);
            }
        }
        indices
    }

    async fn next_serial_default_value(
        &self,
        table_name: &str,
        schema: &TableSchema,
        column_idx: usize,
        txn: &mut dyn Transaction,
    ) -> Result<Value> {
        let prefix = insert_data_prefix_for_table(table_name);
        let existing = txn.scan_prefix(prefix.as_bytes(), None).await?;
        let mut max_seen = 0_i64;
        for (key, value) in existing {
            let candidate = if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(row) = self.row_cache.get(key_str) {
                    row.get(column_idx).cloned()
                } else {
                    crate::common::encoding::RowDecoder::decode_column(&value, column_idx)
                        .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
                }
            } else {
                crate::common::encoding::RowDecoder::decode_column(&value, column_idx)
                    .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
            };
            if let Some(Value::Integer(value)) = candidate {
                max_seen = max_seen.max(value);
            }
        }

        let column = &schema.columns[column_idx];
        let next = max_seen.checked_add(1).ok_or_else(|| {
            FusionError::Execution(format!("SERIAL column {} exhausted", column.name))
        })?;
        Ok(Value::Integer(next))
    }

    async fn apply_missing_serial_defaults(
        &self,
        table_name: &str,
        schema: &TableSchema,
        row_values: &mut [Value],
        missing_serial_indexes: &[usize],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for &idx in missing_serial_indexes {
            if row_values[idx] == Value::Null {
                row_values[idx] = self
                    .next_serial_default_value(table_name, schema, idx, txn)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn insert_direct_rows(
        &self,
        table_name: &str,
        columns: &[Ident],
        rows: &[Vec<Value>],
        txn: &mut dyn Transaction,
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let context = self
            .build_insert_rows_context(table_name, columns, txn)
            .await?;
        let result = self
            .insert_raw_values_rows(&context, rows.to_vec(), None, None, txn)
            .await?;
        match result {
            QueryResult::Success { .. } => Ok(rows.len()),
            other => Err(FusionError::Execution(format!(
                "Direct insert expected success, got {:?}",
                other
            ))),
        }
    }

    async fn build_insert_rows_context(
        &self,
        table_name: &str,
        columns: &[Ident],
        txn: &mut dyn Transaction,
    ) -> Result<InsertRowsContext> {
        let schema_key = insert_schema_key_for_table(table_name);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
        let composite_indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        let composite_unique_indexes = self
            .load_composite_unique_indexes_for_table(table_name, txn)
            .await?;
        let foreign_keys = self.load_child_foreign_keys(table_name, txn).await?;
        let trigram_column_indices = Self::indexed_trigram_text_columns(&schema);
        let col_mapping = Self::insert_column_mapping(table_name, columns, &schema)?;

        Ok(InsertRowsContext {
            table_name: table_name.to_string(),
            schema,
            composite_indexes,
            composite_unique_indexes,
            foreign_keys,
            trigram_column_indices,
            col_mapping,
        })
    }

    fn insert_column_mapping(
        table_name: &str,
        columns: &[Ident],
        schema: &TableSchema,
    ) -> Result<Option<Vec<usize>>> {
        if columns.is_empty() {
            return Ok(None);
        }

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
                        col_name, table_name
                    ))
                })?;
            mapping.push(idx);
        }
        Ok(Some(mapping))
    }

    fn map_raw_insert_values(
        &self,
        context: &InsertRowsContext,
        raw_values: Vec<Value>,
    ) -> Result<(Vec<Value>, Vec<usize>)> {
        if let Some(ref mapping) = context.col_mapping {
            if raw_values.len() != mapping.len() {
                return Err(FusionError::Execution("Column count mismatch".to_string()));
            }
            let mut missing_serial_indexes =
                Self::serial_default_candidate_column_indexes(&context.schema);
            let mut full_row = Vec::with_capacity(context.schema.columns.len());
            for col in &context.schema.columns {
                let value = if let Some(ref def_str) = col.default_value {
                    self.parse_default_value(def_str)
                        .and_then(|value| Self::coerce_value_to_column_type(value, &col.data_type))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                full_row.push(value);
            }
            for (i, &schema_idx) in mapping.iter().enumerate() {
                full_row[schema_idx] = raw_values[i].clone();
            }
            missing_serial_indexes.retain(|idx| !mapping.contains(idx));
            Ok((full_row, missing_serial_indexes))
        } else {
            Ok((raw_values, Vec::new()))
        }
    }

    async fn insert_raw_values_rows(
        &self,
        context: &InsertRowsContext,
        raw_rows: Vec<Vec<Value>>,
        returning: Option<&Vec<SelectItem>>,
        on_conflict: Option<&OnInsert>,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let mut inserted_rows: Vec<Vec<Value>> = if returning.is_some() {
            Vec::with_capacity(raw_rows.len())
        } else {
            Vec::new()
        };

        let mut count = 0usize;
        for raw_values in raw_rows {
            if let Some(row) = self
                .insert_prepared_values_row(context, raw_values, on_conflict, txn, count)
                .await?
            {
                if returning.is_some() {
                    inserted_rows.push(row);
                }
            }
            count += 1;
        }

        if let Some(ret_items) = returning {
            Self::insert_trace(format!(
                "values insert returning table={} rows={}",
                context.table_name,
                inserted_rows.len()
            ));
            return self.build_returning_result(ret_items, &inserted_rows, &context.schema);
        }

        Self::insert_trace(format!(
            "values insert done table={} count={}",
            context.table_name, count
        ));
        Ok(QueryResult::Success {
            message: format!("Inserted {} rows", count),
        })
    }

    async fn insert_prepared_values_row(
        &self,
        context: &InsertRowsContext,
        raw_values: Vec<Value>,
        on_conflict: Option<&OnInsert>,
        txn: &mut dyn Transaction,
        row_index: usize,
    ) -> Result<Option<Vec<Value>>> {
        let (mut row_values, missing_serial_indexes) =
            self.map_raw_insert_values(context, raw_values)?;

        if row_values.len() != context.schema.columns.len() {
            return Err(FusionError::Execution("Column count mismatch".to_string()));
        }
        Self::insert_trace(format!(
            "row defaults start table={} row_index={} missing_serial={:?}",
            context.table_name, row_index, missing_serial_indexes
        ));
        self.apply_missing_serial_defaults(
            &context.table_name,
            &context.schema,
            &mut row_values,
            &missing_serial_indexes,
            txn,
        )
        .await?;
        Self::insert_trace(format!(
            "row coerce start table={} row_index={}",
            context.table_name, row_index
        ));
        let row_values = self.coerce_row_to_schema(row_values, &context.schema)?;
        Self::insert_trace(format!(
            "row constraints start table={} row_index={}",
            context.table_name, row_index
        ));

        for (idx, col) in context.schema.columns.iter().enumerate() {
            if !col.is_nullable && row_values[idx] == Value::Null {
                return Err(FusionError::Execution(format!(
                    "NOT NULL constraint violated for column '{}'",
                    col.name
                )));
            }
        }

        for col in context.schema.columns.iter() {
            if let Some(ref check_sql) = col.check_expr {
                let check_result =
                    self.evaluate_check_constraint(check_sql, &context.schema, &row_values);
                if !check_result {
                    return Err(FusionError::Execution(format!(
                        "CHECK constraint violated for column '{}': {}",
                        col.name, check_sql
                    )));
                }
            }
        }

        for (idx, col) in context.schema.columns.iter().enumerate() {
            if col.is_unique && !col.is_primary && row_values[idx] != Value::Null {
                let prefix = insert_data_prefix_for_table(&context.table_name);
                let existing = txn.scan_prefix(prefix.as_bytes(), None).await?;
                for (k, v) in &existing {
                    let existing_value = if let Ok(key_str) = std::str::from_utf8(k) {
                        if let Some(row) = self.row_cache.get(key_str) {
                            monitor::inc_row_cache_hit();
                            row.get(idx).cloned().unwrap_or(Value::Null)
                        } else {
                            crate::common::encoding::RowDecoder::decode_column(v, idx)
                                .map_err(|e| {
                                    FusionError::Execution(format!("Decode error: {}", e))
                                })?
                                .unwrap_or(Value::Null)
                        }
                    } else {
                        crate::common::encoding::RowDecoder::decode_column(v, idx)
                            .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
                            .unwrap_or(Value::Null)
                    };
                    if existing_value == row_values[idx] {
                        return Err(FusionError::Execution(format!(
                            "UNIQUE constraint violated for column '{}': duplicate value '{}'",
                            col.name,
                            crate::common::encoding::encode_key(&row_values[idx])
                        )));
                    }
                }
            }
        }

        let row_id = self.row_id_for_insert(
            &context.schema,
            &row_values,
            &context.composite_unique_indexes,
        );
        Self::insert_trace(format!(
            "row row_id table={} row_index={} row_id={}",
            context.table_name, row_index, row_id
        ));

        let key = insert_data_key_for_row_id(&context.table_name, &row_id);

        if let Some(OnInsert::OnConflict(oc)) = on_conflict {
            if let Some(existing_bytes) = txn.get(key.as_bytes()).await? {
                match &oc.action {
                    sqlparser::ast::OnConflictAction::DoNothing => {
                        return Ok(None);
                    }
                    sqlparser::ast::OnConflictAction::DoUpdate(do_update) => {
                        let mut existing_row: Vec<Value> = if let Some(row) =
                            self.row_cache.get(&key)
                        {
                            monitor::inc_row_cache_hit();
                            row
                        } else {
                            crate::common::encoding::RowDecoder::decode(&existing_bytes).map_err(
                                |e| FusionError::Execution(format!("Decode error: {}", e)),
                            )?
                        };
                        let old_existing_row = existing_row.clone();
                        for assignment in &do_update.assignments {
                            let col_name = match &assignment.target {
                                sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                    name.to_string()
                                }
                                _ => continue,
                            };
                            if let Some(col_idx) = context.schema.get_column_index(&col_name) {
                                let new_val = self.evaluate_upsert_value(
                                    &assignment.value,
                                    &existing_row,
                                    &row_values,
                                    &context.schema,
                                )?;
                                existing_row[col_idx] = Self::coerce_value_to_column_type(
                                    new_val,
                                    &context.schema.columns[col_idx].data_type,
                                )?;
                            }
                        }
                        self.validate_child_foreign_keys(
                            &context.table_name,
                            &context.schema,
                            &existing_row,
                            &context.foreign_keys,
                            txn,
                        )
                        .await?;
                        self.validate_composite_unique_constraints(
                            &context.composite_unique_indexes,
                            &context.table_name,
                            &context.schema,
                            &existing_row,
                            Some(&row_id),
                            txn,
                        )
                        .await?;
                        let value = crate::common::encoding::RowEncoder::encode(&existing_row);
                        txn.put(key.as_bytes(), &value).await?;
                        self.row_cache.invalidate(&key);
                        self.update_trigram_index_for_update(
                            &context.table_name,
                            &context.schema,
                            &old_existing_row,
                            &existing_row,
                            &row_id,
                            &context.trigram_column_indices,
                            txn,
                        );
                        for (idx, col) in context.schema.columns.iter().enumerate() {
                            if col.is_indexed && col.index_type == IndexType::HNSW {
                                let idx_name = format!("hnsw_{}_{}", context.table_name, col.name);
                                if idx < old_existing_row.len()
                                    && old_existing_row[idx] != existing_row[idx]
                                {
                                    if matches!(old_existing_row[idx], Value::Vector(_)) {
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
                            &context.composite_indexes,
                            &context.table_name,
                            &context.schema,
                            &old_existing_row,
                            &existing_row,
                            &row_id,
                            txn,
                        )
                        .await?;
                        monitor::inc_row_write();
                        return Ok(Some(existing_row));
                    }
                }
            }
        }
        if txn.get(key.as_bytes()).await?.is_some() {
            return Err(FusionError::Execution(
                "PRIMARY KEY constraint violated: duplicate row key".to_string(),
            ));
        }

        self.validate_child_foreign_keys(
            &context.table_name,
            &context.schema,
            &row_values,
            &context.foreign_keys,
            txn,
        )
        .await?;
        Self::insert_trace(format!(
            "row composite unique start table={} row_index={}",
            context.table_name, row_index
        ));
        self.validate_composite_unique_constraints(
            &context.composite_unique_indexes,
            &context.table_name,
            &context.schema,
            &row_values,
            Some(&row_id),
            txn,
        )
        .await?;
        Self::insert_trace(format!(
            "row put start table={} row_index={}",
            context.table_name, row_index
        ));

        let value = crate::common::encoding::RowEncoder::encode(&row_values);
        txn.put(key.as_bytes(), &value).await?;
        self.row_cache.invalidate(&key);
        monitor::inc_row_write();

        self.update_trigram_index_for_insert(
            &context.table_name,
            &context.schema,
            &row_values,
            &row_id,
            &context.trigram_column_indices,
            txn,
        );

        for (idx, col) in context.schema.columns.iter().enumerate() {
            if col.is_indexed {
                let val = &row_values[idx];

                if col.index_type == IndexType::FTS {
                    if let Value::String(text) = val {
                        for token in Self::tokenize_unique(text) {
                            let index_key = Self::fts_index_key_for_row(
                                &context.table_name,
                                &col.name,
                                &token,
                                &row_id,
                            );
                            txn.put(index_key.as_bytes(), &[]).await?;
                        }
                    }
                } else if col.index_type == IndexType::HNSW {
                    if let Value::Vector(vec) = val {
                        let idx_name = format!("hnsw_{}_{}", context.table_name, col.name);
                        self.vector_index
                            .insert(&idx_name, row_id.clone(), vec.clone())?;
                    }
                } else if let Some(val_str) = self.value_to_index_string(val) {
                    let index_key = format!(
                        "index:{}:{}:{}:{}",
                        context.table_name, col.name, val_str, row_id
                    );
                    txn.put(index_key.as_bytes(), &[]).await?;
                }
            }
        }

        self.put_loaded_composite_indexes_for_row(
            &context.composite_indexes,
            &context.table_name,
            &context.schema,
            &row_values,
            &row_id,
            txn,
        )
        .await?;
        Self::insert_trace(format!(
            "row done table={} row_index={}",
            context.table_name, row_index
        ));

        Ok(Some(row_values))
    }

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
        Self::insert_trace(format!(
            "handle_insert start table={} columns={} params={}",
            table_name_str,
            columns.len(),
            params.len()
        ));

        let schema_key = insert_schema_key_for_table(&table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
        let composite_indexes = self
            .load_composite_indexes_for_table(&table_name_str, txn)
            .await?;
        let composite_unique_indexes = self
            .load_composite_unique_indexes_for_table(&table_name_str, txn)
            .await?;
        let foreign_keys = self.load_child_foreign_keys(&table_name_str, txn).await?;
        let trigram_column_indices = Self::indexed_trigram_text_columns(&schema);

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

        if let Some(query) = source {
            if let SetExpr::Values(values) = &query.body.as_ref() {
                Self::insert_trace(format!(
                    "values insert start table={} rows={} returning={} on_conflict={}",
                    table_name_str,
                    values.rows.len(),
                    returning.is_some(),
                    on_conflict.is_some()
                ));
                let mut inserted_rows: Vec<Vec<Value>> = if returning.is_some() {
                    Vec::with_capacity(values.rows.len())
                } else {
                    Vec::new()
                };
                let mut count = 0;
                for row in &values.rows {
                    Self::insert_trace(format!(
                        "row start table={} row_index={} exprs={}",
                        table_name_str,
                        count,
                        row.len()
                    ));
                    let mut raw_values = Vec::with_capacity(row.len());
                    for expr in row.iter() {
                        let val = self
                            .evaluate_value(expr, &[], &schema, params)
                            .unwrap_or(Value::Null);
                        raw_values.push(val);
                    }

                    // If column list specified, map values to full row with defaults for missing columns
                    let (mut row_values, missing_serial_indexes) =
                        if let Some(ref mapping) = col_mapping {
                            if raw_values.len() != mapping.len() {
                                return Err(FusionError::Execution(
                                    "Column count mismatch".to_string(),
                                ));
                            }
                            let mut missing_serial_indexes =
                                Self::serial_default_candidate_column_indexes(&schema);
                            let mut full_row = Vec::with_capacity(schema.columns.len());
                            for col in &schema.columns {
                                let value = if let Some(ref def_str) = col.default_value {
                                    self.parse_default_value(def_str)
                                        .and_then(|value| {
                                            Self::coerce_value_to_column_type(value, &col.data_type)
                                        })
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                full_row.push(value);
                            }
                            for (i, &schema_idx) in mapping.iter().enumerate() {
                                full_row[schema_idx] = raw_values[i].clone();
                            }
                            missing_serial_indexes.retain(|idx| !mapping.contains(idx));
                            (full_row, missing_serial_indexes)
                        } else {
                            (raw_values, Vec::new())
                        };

                    if row_values.len() != schema.columns.len() {
                        return Err(FusionError::Execution("Column count mismatch".to_string()));
                    }
                    Self::insert_trace(format!(
                        "row defaults start table={} row_index={} missing_serial={:?}",
                        table_name_str, count, missing_serial_indexes
                    ));
                    self.apply_missing_serial_defaults(
                        &table_name_str,
                        &schema,
                        &mut row_values,
                        &missing_serial_indexes,
                        txn,
                    )
                    .await?;
                    Self::insert_trace(format!(
                        "row coerce start table={} row_index={}",
                        table_name_str, count
                    ));
                    let row_values = self.coerce_row_to_schema(row_values, &schema)?;
                    Self::insert_trace(format!(
                        "row constraints start table={} row_index={}",
                        table_name_str, count
                    ));

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
                    for col in schema.columns.iter() {
                        if let Some(ref check_sql) = col.check_expr {
                            let check_result =
                                self.evaluate_check_constraint(check_sql, &schema, &row_values);
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
                            let prefix = insert_data_prefix_for_table(&table_name_str);
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

                    let row_id =
                        self.row_id_for_insert(&schema, &row_values, &composite_unique_indexes);
                    Self::insert_trace(format!(
                        "row row_id table={} row_index={} row_id={}",
                        table_name_str, count, row_id
                    ));

                    let key = insert_data_key_for_row_id(&table_name_str, &row_id);

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
                                    self.validate_composite_unique_constraints(
                                        &composite_unique_indexes,
                                        &table_name_str,
                                        &schema,
                                        &existing_row,
                                        Some(&row_id),
                                        txn,
                                    )
                                    .await?;
                                    let value =
                                        crate::common::encoding::RowEncoder::encode(&existing_row);
                                    txn.put(key.as_bytes(), &value).await?;
                                    self.row_cache.invalidate(&key);
                                    self.update_trigram_index_for_update(
                                        &table_name_str,
                                        &schema,
                                        &old_existing_row,
                                        &existing_row,
                                        &row_id,
                                        &trigram_column_indices,
                                        txn,
                                    );
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
                    if txn.get(key.as_bytes()).await?.is_some() {
                        return Err(FusionError::Execution(
                            "PRIMARY KEY constraint violated: duplicate row key".to_string(),
                        ));
                    }

                    self.validate_child_foreign_keys(
                        &table_name_str,
                        &schema,
                        &row_values,
                        &foreign_keys,
                        txn,
                    )
                    .await?;
                    Self::insert_trace(format!(
                        "row composite unique start table={} row_index={}",
                        table_name_str, count
                    ));
                    self.validate_composite_unique_constraints(
                        &composite_unique_indexes,
                        &table_name_str,
                        &schema,
                        &row_values,
                        Some(&row_id),
                        txn,
                    )
                    .await?;
                    Self::insert_trace(format!(
                        "row put start table={} row_index={}",
                        table_name_str, count
                    ));

                    let value = crate::common::encoding::RowEncoder::encode(&row_values);
                    txn.put(key.as_bytes(), &value).await?;
                    self.row_cache.invalidate(&key);
                    monitor::inc_row_write();

                    // Update Cache
                    // self.row_cache.insert(key.clone(), row_values.clone());

                    self.update_trigram_index_for_insert(
                        &table_name_str,
                        &schema,
                        &row_values,
                        &row_id,
                        &trigram_column_indices,
                        txn,
                    );

                    for (idx, col) in schema.columns.iter().enumerate() {
                        if col.is_indexed {
                            let val = &row_values[idx];

                            if col.index_type == IndexType::FTS {
                                if let Value::String(text) = val {
                                    for token in Self::tokenize_unique(text) {
                                        let index_key = Self::fts_index_key_for_row(
                                            &table_name_str,
                                            &col.name,
                                            &token,
                                            &row_id,
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
                    Self::insert_trace(format!(
                        "row done table={} row_index={}",
                        table_name_str, count
                    ));

                    if returning.is_some() {
                        inserted_rows.push(row_values.clone());
                    }
                    count += 1;
                }

                // Handle RETURNING clause
                if let Some(ret_items) = returning {
                    Self::insert_trace(format!(
                        "values insert returning table={} rows={}",
                        table_name_str,
                        inserted_rows.len()
                    ));
                    return self.build_returning_result(ret_items, &inserted_rows, &schema);
                }

                Self::insert_trace(format!(
                    "values insert done table={} count={}",
                    table_name_str, count
                ));
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
                    let row_id =
                        self.row_id_for_insert(&schema, &row_values, &composite_unique_indexes);
                    let key = insert_data_key_for_row_id(&table_name_str, &row_id);
                    if txn.get(key.as_bytes()).await?.is_some() {
                        return Err(FusionError::Execution(
                            "PRIMARY KEY constraint violated: duplicate row key".to_string(),
                        ));
                    }
                    self.validate_child_foreign_keys(
                        &table_name_str,
                        &schema,
                        &row_values,
                        &foreign_keys,
                        txn,
                    )
                    .await?;
                    self.validate_composite_unique_constraints(
                        &composite_unique_indexes,
                        &table_name_str,
                        &schema,
                        &row_values,
                        Some(&row_id),
                        txn,
                    )
                    .await?;
                    let value = crate::common::encoding::RowEncoder::encode(&row_values);
                    txn.put(key.as_bytes(), &value).await?;
                    monitor::inc_row_write();
                    self.update_trigram_index_for_insert(
                        &table_name_str,
                        &schema,
                        &row_values,
                        &row_id,
                        &trigram_column_indices,
                        txn,
                    );

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

#[cfg(test)]
mod tests {
    use super::{
        insert_data_key_for_row_id, insert_data_prefix_for_table, insert_schema_key_for_table,
    };

    #[test]
    fn insert_data_key_for_row_id_preallocates_exact_key() {
        let key = insert_data_key_for_row_id("orders", "00042");

        assert_eq!(key, "data:orders:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn insert_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = insert_data_prefix_for_table("orders");

        assert_eq!(prefix, "data:orders:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn insert_schema_key_for_table_preallocates_exact_key() {
        let key = insert_schema_key_for_table("orders");

        assert_eq!(key, "schema:orders");
        assert!(key.capacity() >= key.len());
    }
}
