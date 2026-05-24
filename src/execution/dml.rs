use crate::catalog::{IndexType, TableSchema};
use crate::common::encoding::encode_key;
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, TableFactor};
use std::collections::HashSet;

use super::{Executor, QueryResult};

impl Executor {
    fn primary_key_row_id_from_eq_selection(
        &self,
        selection: Option<&Expr>,
        schema: &TableSchema,
        params: &[Value],
        allowed_qualifiers: &[String],
    ) -> Option<String> {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = selection?
        else {
            return None;
        };

        let col_name = match left.as_ref() {
            Expr::Identifier(ident) => &ident.value,
            Expr::CompoundIdentifier(idents) => {
                if idents.len() < 2 {
                    return None;
                }

                let qualifier = idents[..idents.len() - 1]
                    .iter()
                    .map(|ident| ident.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");

                if !allowed_qualifiers
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(&qualifier))
                {
                    return None;
                }

                &idents.last()?.value
            }
            _ => return None,
        };

        let pk_idx = schema.get_primary_key_index()?;
        if pk_idx != 0 {
            return None;
        }

        let col_idx = schema
            .columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(col_name))?;
        if col_idx != pk_idx {
            return None;
        }

        match self
            .evaluate_value(right, &[], schema, params)
            .unwrap_or(Value::Null)
        {
            Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(i)),
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    fn primary_key_qualifiers(relation: &TableFactor) -> Vec<String> {
        let mut qualifiers = Vec::new();
        if let TableFactor::Table { name, alias, .. } = relation {
            let table_name = name.to_string();
            qualifiers.push(table_name);
            if let Some(alias) = alias {
                qualifiers.push(alias.name.value.clone());
            }
        }
        qualifiers
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

        let schema_key = format!("schema:{}", table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        // Build column index mapping if explicit column list provided
        let col_mapping: Option<Vec<usize>> = if !columns.is_empty() {
            let mut mapping = Vec::new();
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
                let mut count = 0;
                for row in &values.rows {
                    let mut raw_values = Vec::new();
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
                            for (_, v) in &existing {
                                let existing_value =
                                    crate::common::encoding::RowDecoder::decode_column(v, idx)
                                        .map_err(|e| {
                                            FusionError::Execution(format!("Decode error: {}", e))
                                        })?
                                        .unwrap_or(Value::Null);
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
                                    let mut existing_row: Vec<Value> =
                                        crate::common::encoding::RowDecoder::decode(
                                            &existing_bytes,
                                        )
                                        .map_err(|e| {
                                            FusionError::Execution(format!("Decode error: {}", e))
                                        })?;
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
                                            existing_row[col_idx] = new_val;
                                        }
                                    }
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
                                    let tokens = Self::tokenize(text);
                                    let unique_tokens: HashSet<String> =
                                        tokens.into_iter().collect();
                                    for token in unique_tokens {
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
            if let super::QueryResult::Select {
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
                    let row_id = if let Some(first) = row_values.first() {
                        encode_key(first)
                    } else {
                        uuid::Uuid::new_v4().to_string()
                    };
                    let key = format!("data:{}:{}", table_name_str, row_id);
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

        let prefix = format!("data:{}:", table_name_str);
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

        if delete.returning.is_none() {
            let no_secondary_indexes = schema
                .columns
                .iter()
                .all(|col| !col.is_indexed || col.is_primary);
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
        let mut deleted_rows: Vec<Vec<Value>> = Vec::new();
        for (k, v) in kv_pairs {
            let row: Vec<Value> = crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                FusionError::Execution(format!("Data deserialization error: {}", e))
            })?;

            let mut delete_flag = true;
            if let Some(selection) = &delete.selection {
                delete_flag = self.evaluate_expr(selection, &row, &schema, params)?;
            }

            if delete_flag {
                txn.delete(&k).await?;

                // Invalidate Cache
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.invalidate(key_str);
                }

                let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                let row_id = parts.last().unwrap();

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let val = &row[idx];

                        if col.index_type == IndexType::FTS {
                            if let Value::String(text) = val {
                                let tokens = Self::tokenize(text);
                                let unique_tokens: HashSet<String> = tokens.into_iter().collect();
                                for token in unique_tokens {
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
        let mut updated_rows: Vec<Vec<Value>> = Vec::new();
        for (k, v) in kv_pairs {
            let mut row: Vec<Value> =
                crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                    FusionError::Execution(format!("Data deserialization error: {}", e))
                })?;
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
                        row[col_idx] = new_val;
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

                let new_value_bytes = crate::common::encoding::RowEncoder::encode(&row);
                txn.put(&k, &new_value_bytes).await?;
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.invalidate(key_str);
                }

                let parts: Vec<&str> = std::str::from_utf8(&k).unwrap().split(':').collect();
                let row_id = parts.last().unwrap();

                for (idx, col) in schema.columns.iter().enumerate() {
                    if col.is_indexed {
                        let old_val = &old_row[idx];
                        let new_val = &row[idx];

                        if old_val != new_val {
                            if col.index_type == IndexType::FTS {
                                if let Value::String(text) = old_val {
                                    let tokens = Self::tokenize(text);
                                    for token in tokens {
                                        let index_key = format!(
                                            "fts:{}:{}:{}:{}",
                                            table_name_str, col.name, token, row_id
                                        );
                                        txn.delete(index_key.as_bytes()).await?;
                                    }
                                }
                                if let Value::String(text) = new_val {
                                    let tokens = Self::tokenize(text);
                                    let unique_tokens: HashSet<String> =
                                        tokens.into_iter().collect();
                                    for token in unique_tokens {
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

    fn evaluate_upsert_value(
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

    fn build_returning_result(
        &self,
        ret_items: &[sqlparser::ast::SelectItem],
        rows: &[Vec<Value>],
        schema: &TableSchema,
    ) -> Result<QueryResult> {
        use sqlparser::ast::SelectItem;
        let mut col_names = Vec::new();
        let mut result_rows = Vec::new();

        // Build column names from RETURNING items
        let is_wildcard = ret_items
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_)));
        if is_wildcard {
            col_names = schema.columns.iter().map(|c| c.name.clone()).collect();
            result_rows = rows.to_vec();
        } else {
            for item in ret_items {
                match item {
                    SelectItem::UnnamedExpr(expr) => col_names.push(format!("{}", expr)),
                    SelectItem::ExprWithAlias { alias, .. } => col_names.push(alias.value.clone()),
                    _ => {}
                }
            }
            for row in rows {
                let mut result_row = Vec::new();
                for item in ret_items {
                    let expr = match item {
                        SelectItem::UnnamedExpr(e) => e,
                        SelectItem::ExprWithAlias { expr, .. } => expr,
                        _ => continue,
                    };
                    let val = self
                        .evaluate_value(expr, row, schema, &[])
                        .unwrap_or(Value::Null);
                    result_row.push(val);
                }
                result_rows.push(result_row);
            }
        }

        Ok(QueryResult::Select {
            columns: col_names,
            rows: result_rows,
        })
    }

    fn evaluate_check_constraint(&self, check_sql: &str, col_name: &str, value: &Value) -> bool {
        // Parse the CHECK expression and evaluate it against the column value
        // CHECK expressions reference the column by name, e.g. CHECK(age > 0)
        // We build a minimal SELECT with a WHERE clause to reuse existing expression evaluation
        if *value == Value::Null {
            return true; // NULL passes CHECK constraints (SQL standard)
        }

        // Strip "CHECK" prefix if present (sqlparser Display may include it)
        let expr_str = check_sql.trim();
        let expr_str = if expr_str.to_uppercase().starts_with("CHECK") {
            let rest = expr_str[5..].trim();
            if rest.starts_with('(') && rest.ends_with(')') {
                &rest[1..rest.len() - 1]
            } else {
                rest
            }
        } else {
            expr_str
        };

        // Try to parse the check expression
        let parse_result = crate::parser::parse_sql(&format!("SELECT 1 WHERE {}", expr_str));
        if let Ok(stmts) = parse_result {
            if let Some(sqlparser::ast::Statement::Query(query)) = stmts.first() {
                if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(ref where_expr) = select.selection {
                        // Build a single-column schema and row for evaluation
                        use crate::catalog::{Column, IndexType, TableSchema};
                        let schema = TableSchema::new(
                            "_check".to_string(),
                            vec![Column {
                                name: col_name.to_string(),
                                data_type: "TEXT".to_string(),
                                is_primary: false,
                                is_indexed: false,
                                index_type: IndexType::None,
                                default_value: None,
                                is_nullable: true,
                                is_unique: false,
                                check_expr: None,
                            }],
                        );
                        let row = vec![value.clone()];
                        return self
                            .evaluate_expr(where_expr, &row, &schema, &[])
                            .unwrap_or(false);
                    }
                }
            }
        }
        true // If we can't parse, pass the check (don't break existing data)
    }

    fn parse_default_value(&self, def_str: &str) -> Value {
        // Try parsing as integer
        if let Ok(n) = def_str.parse::<i64>() {
            return Value::Integer(n);
        }
        // Try parsing as float
        if let Ok(f) = def_str.parse::<f64>() {
            return Value::Float(f);
        }
        // Boolean
        match def_str.to_lowercase().as_str() {
            "true" => return Value::Boolean(true),
            "false" => return Value::Boolean(false),
            "null" => return Value::Null,
            _ => {}
        }
        // Strip quotes for string literals
        let trimmed = def_str.trim();
        if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        {
            return Value::String(trimmed[1..trimmed.len() - 1].to_string());
        }
        Value::String(def_str.to_string())
    }
}
