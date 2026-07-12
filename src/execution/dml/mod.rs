use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use sqlparser::ast::{BinaryOperator, Expr, TableFactor};
use std::collections::HashMap;

use super::Executor;

mod constraints;
mod delete;
mod insert;
mod returning;
mod update;

#[cfg(test)]
fn starts_with_ascii_case_insensitive(value: &str, prefix: &str) -> bool {
    match value.as_bytes().get(..prefix.len()) {
        Some(candidate) => candidate.eq_ignore_ascii_case(prefix.as_bytes()),
        None => false,
    }
}

#[cfg(test)]
fn is_trigram_text_data_type(data_type: &str) -> bool {
    let data_type = data_type.trim();
    data_type.eq_ignore_ascii_case("TEXT")
        || data_type.eq_ignore_ascii_case("STRING")
        || data_type.eq_ignore_ascii_case("VARCHAR")
        || data_type.eq_ignore_ascii_case("CHAR")
        || starts_with_ascii_case_insensitive(data_type, "VARCHAR(")
        || starts_with_ascii_case_insensitive(data_type, "CHAR(")
        || starts_with_ascii_case_insensitive(data_type, "CHARACTER")
}

/// Stable FNV-1a 64 over the value's bincode encoding. Used as the sentinel
/// key component for value types `value_to_index_string` cannot render
/// (FLOAT/BLOB/VECTOR/...). Equal values always hash equal, so the OCC
/// collision is guaranteed; a hash collision between different values only
/// produces a conservative false conflict, never a missed one.
fn unique_sentinel_hash_string(value: &Value) -> Option<String> {
    // -0.0 == 0.0 under SQL/f64 equality but their bit patterns differ;
    // normalize so equal floats always hash to the same sentinel.
    let normalized;
    let value = match value {
        Value::Float(f) if *f == 0.0 => {
            normalized = Value::Float(0.0);
            &normalized
        }
        other => other,
    };
    let bytes = bincode::serialize(value).ok()?;
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    Some(format!("h64:{hash:016x}"))
}

/// Unique values are compared with SQL equality but hashed by bit pattern,
/// so -0.0 == 0.0 would land in different hash buckets; normalize float
/// zeros (also inside VECTOR/ARRAY/OBJECT) so set membership agrees with
/// value equality.
fn normalized_unique_set_value(value: &Value) -> Value {
    match value {
        Value::Float(f) if *f == 0.0 => Value::Float(0.0),
        Value::Vector(items) => Value::Vector(
            items
                .iter()
                .map(|f| if *f == 0.0 { 0.0 } else { *f })
                .collect(),
        ),
        Value::Array(items) => {
            Value::Array(items.iter().map(normalized_unique_set_value).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), normalized_unique_set_value(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Row keys are derived from PRIMARY KEY values (a single `is_primary`
/// column or the columns of the `is_primary` composite index), so changing a
/// PK column in place strands the row under its old key (full scans lose the
/// row; point lookups on old and new key both misbehave). Reject loudly
/// until row-id migration exists.
pub(crate) fn reject_primary_key_change(
    schema: &TableSchema,
    composite_unique_indexes: &[super::composite_index::CompositeIndexMeta],
    old_row: &[Value],
    new_row: &[Value],
    statement_kind: &str,
) -> Result<()> {
    for (idx, col) in schema.columns.iter().enumerate() {
        if col.is_primary && old_row.get(idx) != new_row.get(idx) {
            return Err(FusionError::Execution(format!(
                "{statement_kind} cannot change PRIMARY KEY column '{}'",
                col.name
            )));
        }
    }
    if let Some(primary) = composite_unique_indexes
        .iter()
        .find(|index| index.is_primary)
    {
        for col_name in &primary.columns {
            if let Some(idx) = schema.get_column_index(col_name) {
                if old_row.get(idx) != new_row.get(idx) {
                    return Err(FusionError::Execution(format!(
                        "{statement_kind} cannot change PRIMARY KEY column '{col_name}'"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn unique_column_indexes(schema: &TableSchema) -> impl Iterator<Item = usize> + '_ {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.is_unique && !col.is_primary)
        .map(|(idx, _)| idx)
}

/// Per-statement cache mapping every non-PK UNIQUE column's values to their
/// owner row_id: one table scan on first use instead of one scan per row and
/// column, kept in sync as the statement writes rows. The owner row_id serves
/// ON CONFLICT (unique_col) resolution and UPDATE self-exclusion. The scan
/// check covers already-committed duplicates; concurrent same-value writers
/// still collide through the unique sentinels (BENCHPROD-464).
pub(crate) struct UniqueColumnValueSets {
    sets: Option<HashMap<usize, HashMap<Value, String>>>,
}

impl UniqueColumnValueSets {
    pub(crate) fn new() -> Self {
        Self { sets: None }
    }

    /// True when the row carries a non-NULL value in some non-PK UNIQUE
    /// column, i.e. a duplicate check is required at all.
    fn row_needs_check(schema: &TableSchema, row_values: &[Value]) -> bool {
        unique_column_indexes(schema).any(|idx| {
            row_values
                .get(idx)
                .is_some_and(|value| *value != Value::Null)
        })
    }

    fn loaded_sets(&self) -> Result<&HashMap<usize, HashMap<Value, String>>> {
        self.sets.as_ref().ok_or_else(|| {
            FusionError::Execution("unique column value sets consulted before load".to_string())
        })
    }

    /// Error if the row duplicates a tracked value in any non-PK UNIQUE
    /// column. Fails closed when the sets were never loaded.
    fn assert_row_absent(&self, schema: &TableSchema, row_values: &[Value]) -> Result<()> {
        let sets = self.loaded_sets()?;
        for idx in unique_column_indexes(schema) {
            let Some(value) = row_values.get(idx) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            let is_duplicate = sets
                .get(&idx)
                .is_some_and(|values| values.contains_key(&normalized_unique_set_value(value)));
            if is_duplicate {
                return Err(FusionError::Execution(format!(
                    "UNIQUE constraint violated for column '{}': duplicate value '{}'",
                    schema.columns[idx].name,
                    crate::common::encoding::encode_key(value)
                )));
            }
        }
        Ok(())
    }

    /// UPDATE-side check: error when a changed unique column's new value is
    /// already owned by a different row. Fails closed when never loaded.
    fn assert_update_values_available(
        &self,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        row_id: &str,
    ) -> Result<()> {
        let sets = self.loaded_sets()?;
        for idx in unique_column_indexes(schema) {
            let Some(new_value) = new_row.get(idx) else {
                continue;
            };
            if *new_value == Value::Null || old_row.get(idx) == Some(new_value) {
                continue;
            }
            let owned_by_other = sets
                .get(&idx)
                .and_then(|values| values.get(&normalized_unique_set_value(new_value)))
                .is_some_and(|owner| owner != row_id);
            if owned_by_other {
                return Err(FusionError::Execution(format!(
                    "UNIQUE constraint violated for column '{}': duplicate value '{}'",
                    schema.columns[idx].name,
                    crate::common::encoding::encode_key(new_value)
                )));
            }
        }
        Ok(())
    }

    /// The row_id owning `value` in unique column `col_idx`, if any. Fails
    /// closed when the sets were never loaded.
    pub(crate) fn conflict_owner_row_id(
        &self,
        col_idx: usize,
        value: &Value,
    ) -> Result<Option<&str>> {
        let sets = self.loaded_sets()?;
        Ok(sets
            .get(&col_idx)
            .and_then(|values| values.get(&normalized_unique_set_value(value)))
            .map(String::as_str))
    }

    /// Track a freshly inserted row. No-op when the sets were never loaded:
    /// a later load scans the post-write state through the transaction.
    pub(crate) fn track_insert(
        &mut self,
        schema: &TableSchema,
        row_values: &[Value],
        row_id: &str,
    ) {
        let Some(sets) = self.sets.as_mut() else {
            return;
        };
        for idx in unique_column_indexes(schema) {
            let Some(value) = row_values.get(idx) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            if let Some(values) = sets.get_mut(&idx) {
                values.insert(normalized_unique_set_value(value), row_id.to_string());
            }
        }
    }

    /// Track an UPSERT DO UPDATE that rewrote an existing row in place.
    pub(crate) fn track_update(
        &mut self,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        row_id: &str,
    ) {
        let Some(sets) = self.sets.as_mut() else {
            return;
        };
        for idx in unique_column_indexes(schema) {
            if old_row.get(idx) == new_row.get(idx) {
                continue;
            }
            let Some(values) = sets.get_mut(&idx) else {
                continue;
            };
            if let Some(old_value) = old_row.get(idx) {
                if *old_value != Value::Null {
                    values.remove(&normalized_unique_set_value(old_value));
                }
            }
            if let Some(new_value) = new_row.get(idx) {
                if *new_value != Value::Null {
                    values.insert(normalized_unique_set_value(new_value), row_id.to_string());
                }
            }
        }
    }
}

impl Executor {
    /// Sentinel key component for a unique value: the normal index string
    /// when available, else a stable hash so every value type is covered.
    fn unique_sentinel_value_string(&self, value: &Value) -> Option<String> {
        self.value_to_index_string(value)
            .or_else(|| unique_sentinel_hash_string(value))
    }

    /// INSERT-side duplicate check for the row's non-PK UNIQUE columns
    /// against the per-statement value sets, loading them with a single
    /// table scan on first use.
    pub(crate) async fn check_unique_columns_for_insert(
        &self,
        table_name: &str,
        schema: &TableSchema,
        row_values: &[Value],
        unique_sets: &mut UniqueColumnValueSets,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        if !UniqueColumnValueSets::row_needs_check(schema, row_values) {
            return Ok(());
        }
        self.ensure_unique_column_value_sets(table_name, schema, unique_sets, txn)
            .await?;
        unique_sets.assert_row_absent(schema, row_values)
    }

    /// Build the per-statement unique-value sets with one table scan that
    /// serves every non-PK UNIQUE column at once. No-op when already loaded.
    async fn ensure_unique_column_value_sets(
        &self,
        table_name: &str,
        schema: &TableSchema,
        unique_sets: &mut UniqueColumnValueSets,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        if unique_sets.sets.is_some() {
            return Ok(());
        }
        let mut sets: HashMap<usize, HashMap<Value, String>> = unique_column_indexes(schema)
            .map(|idx| (idx, HashMap::new()))
            .collect();
        if !sets.is_empty() {
            let existing = self
                .scan_routed_data_prefixes_for_table(table_name, txn, None)
                .await?;
            for (k, v) in &existing {
                let row_id = self.legacy_row_id_from_routed_data_key(table_name, k)?;
                let cached_row = std::str::from_utf8(k)
                    .ok()
                    .and_then(|key_str| self.row_cache_lookup(key_str, v));
                for (idx, values) in sets.iter_mut() {
                    let existing_value = if let Some(row) = cached_row.as_ref() {
                        row.get(*idx).cloned().unwrap_or(Value::Null)
                    } else {
                        crate::common::encoding::RowDecoder::decode_column(v, *idx)
                            .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
                            .unwrap_or(Value::Null)
                    };
                    if existing_value != Value::Null {
                        values.insert(
                            normalized_unique_set_value(&existing_value),
                            row_id.to_string(),
                        );
                    }
                }
            }
        }
        unique_sets.sets = Some(sets);
        Ok(())
    }

    /// UPDATE-side duplicate check for unique columns whose value changed,
    /// against the per-statement value sets (loaded with one table scan on
    /// first use), excluding values owned by the row being updated.
    pub(crate) async fn check_unique_columns_for_update(
        &self,
        table_name: &str,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        row_id: &str,
        unique_sets: &mut UniqueColumnValueSets,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        let has_changed_unique_value = unique_column_indexes(schema).any(|idx| {
            new_row
                .get(idx)
                .is_some_and(|value| *value != Value::Null && old_row.get(idx) != Some(value))
        });
        if !has_changed_unique_value {
            return Ok(());
        }
        self.ensure_unique_column_value_sets(table_name, schema, unique_sets, txn)
            .await?;
        unique_sets.assert_update_values_available(schema, old_row, new_row, row_id)
    }

    /// Stage unique-constraint sentinel keys for every non-PK UNIQUE column
    /// of a freshly written row. Sentinels carry no row-id suffix, so they
    /// enter the OCC write set and concurrent same-value writers collide at
    /// commit — closing the scan-then-write phantom (BENCHPROD-464). The
    /// scan-based duplicate check remains responsible for already-committed
    /// duplicates (legacy rows have no sentinels).
    pub(crate) async fn put_unique_sentinels_for_row(
        &self,
        table_name: &str,
        schema: &TableSchema,
        row_values: &[Value],
        row_id: &str,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        for (idx, col) in schema.columns.iter().enumerate() {
            if !col.is_unique || col.is_primary {
                continue;
            }
            let Some(value) = row_values.get(idx) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            let Some(value_str) = self.unique_sentinel_value_string(value) else {
                continue;
            };
            let key = self.routed_unique_sentinel_key_for_value(table_name, &col.name, &value_str);
            txn.put(key.as_bytes(), row_id.as_bytes()).await?;
        }
        Ok(())
    }

    /// Remove the unique sentinels owned by a row that is being deleted.
    pub(crate) async fn delete_unique_sentinels_for_row(
        &self,
        table_name: &str,
        schema: &TableSchema,
        row_values: &[Value],
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        for (idx, col) in schema.columns.iter().enumerate() {
            if !col.is_unique || col.is_primary {
                continue;
            }
            let Some(value) = row_values.get(idx) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            let Some(value_str) = self.unique_sentinel_value_string(value) else {
                continue;
            };
            let key = self.routed_unique_sentinel_key_for_value(table_name, &col.name, &value_str);
            txn.delete(key.as_bytes()).await?;
        }
        Ok(())
    }

    /// Migrate unique sentinels when an UPDATE (or UPSERT DO UPDATE) changes
    /// a unique column's value: drop the old value's sentinel, stage the new
    /// value's sentinel so concurrent writers of the new value collide.
    pub(crate) async fn migrate_unique_sentinels_for_update(
        &self,
        table_name: &str,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        row_id: &str,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        for (idx, col) in schema.columns.iter().enumerate() {
            if !col.is_unique || col.is_primary {
                continue;
            }
            let old_value = old_row.get(idx);
            let new_value = new_row.get(idx);
            if old_value == new_value {
                continue;
            }
            if let Some(old_value) = old_value {
                if *old_value != Value::Null {
                    if let Some(value_str) = self.unique_sentinel_value_string(old_value) {
                        let key = self.routed_unique_sentinel_key_for_value(
                            table_name, &col.name, &value_str,
                        );
                        txn.delete(key.as_bytes()).await?;
                    }
                }
            }
            if let Some(new_value) = new_value {
                if *new_value != Value::Null {
                    if let Some(value_str) = self.unique_sentinel_value_string(new_value) {
                        let key = self.routed_unique_sentinel_key_for_value(
                            table_name, &col.name, &value_str,
                        );
                        txn.put(key.as_bytes(), row_id.as_bytes()).await?;
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fts_index_key_for_row(
        table_name: &str,
        column_name: &str,
        token: &str,
        row_id: &str,
    ) -> String {
        let mut key = String::with_capacity(
            "fts:".len()
                + table_name.len()
                + 1
                + column_name.len()
                + 1
                + token.len()
                + 1
                + row_id.len(),
        );
        key.push_str("fts:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(column_name);
        key.push(':');
        key.push_str(token);
        key.push(':');
        key.push_str(row_id);
        key
    }

    #[cfg(test)]
    pub(crate) fn fts_column_prefix_for_column(table_name: &str, column_name: &str) -> String {
        let mut prefix =
            String::with_capacity("fts:".len() + table_name.len() + 1 + column_name.len() + 1);
        prefix.push_str("fts:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix
    }

    #[cfg(test)]
    pub(crate) fn fts_token_prefix_for_token(
        table_name: &str,
        column_name: &str,
        token: &str,
    ) -> String {
        let mut prefix = String::with_capacity(
            "fts:".len() + table_name.len() + 1 + column_name.len() + 1 + token.len() + 1,
        );
        prefix.push_str("fts:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix.push_str(column_name);
        prefix.push(':');
        prefix.push_str(token);
        prefix.push(':');
        prefix
    }

    pub(crate) fn hnsw_index_name_for_column(
        table_name: &str,
        column_name: &str,
    ) -> Result<String> {
        crate::storage::hnsw_index_name_for_column(table_name, column_name)
    }

    pub(crate) fn indexed_trigram_text_columns(schema: &TableSchema) -> Vec<usize> {
        let mut indices = Vec::with_capacity(schema.columns.len());
        for (idx, col) in schema.columns.iter().enumerate() {
            if col.is_trigram_text_column() {
                indices.push(idx);
            }
        }
        indices
    }

    pub(crate) fn update_trigram_index_for_insert(
        &self,
        table_name: &str,
        schema: &TableSchema,
        row_values: &[Value],
        row_id: &str,
        trigram_column_indices: &[usize],
        txn: &mut dyn crate::storage::Transaction,
    ) {
        if trigram_column_indices.is_empty() {
            return;
        }

        let Some(ftxn) = txn
            .as_any()
            .downcast_ref::<crate::storage::fusion::FusionTransaction>()
        else {
            return;
        };

        let numeric_id = Self::trigram_numeric_row_id(row_id);
        for &idx in trigram_column_indices {
            if let Some(Value::String(text)) = row_values.get(idx) {
                ftxn.defer_side_index_delta(crate::storage::fusion::SideIndexDelta::TrigramAdd {
                    table: table_name.to_string(),
                    column: schema.columns[idx].name.clone(),
                    numeric_id,
                    row_id: row_id.to_string(),
                    text: text.clone(),
                });
            }
        }
    }

    pub(crate) fn update_trigram_index_for_value(
        &self,
        table_name: &str,
        column_name: &str,
        value: &Value,
        row_id: &str,
        txn: &mut dyn crate::storage::Transaction,
    ) {
        let Value::String(text) = value else {
            return;
        };

        let Some(ftxn) = txn
            .as_any()
            .downcast_ref::<crate::storage::fusion::FusionTransaction>()
        else {
            return;
        };

        ftxn.defer_side_index_delta(crate::storage::fusion::SideIndexDelta::TrigramAdd {
            table: table_name.to_string(),
            column: column_name.to_string(),
            numeric_id: Self::trigram_numeric_row_id(row_id),
            row_id: row_id.to_string(),
            text: text.clone(),
        });
    }

    pub(crate) fn update_trigram_index_for_update(
        &self,
        table_name: &str,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        row_id: &str,
        trigram_column_indices: &[usize],
        txn: &mut dyn crate::storage::Transaction,
    ) {
        if trigram_column_indices.is_empty() {
            return;
        }

        let Some(ftxn) = txn
            .as_any()
            .downcast_ref::<crate::storage::fusion::FusionTransaction>()
        else {
            return;
        };

        let numeric_id = Self::trigram_numeric_row_id(row_id);
        for &idx in trigram_column_indices {
            let old_val = old_row.get(idx);
            let new_val = new_row.get(idx);
            if old_val == new_val {
                continue;
            }

            if let Some(Value::String(text)) = old_val {
                ftxn.defer_side_index_delta(
                    crate::storage::fusion::SideIndexDelta::TrigramRemove {
                        table: table_name.to_string(),
                        column: schema.columns[idx].name.clone(),
                        numeric_id,
                        text: text.clone(),
                    },
                );
            }
            if let Some(Value::String(text)) = new_val {
                ftxn.defer_side_index_delta(crate::storage::fusion::SideIndexDelta::TrigramAdd {
                    table: table_name.to_string(),
                    column: schema.columns[idx].name.clone(),
                    numeric_id,
                    row_id: row_id.to_string(),
                    text: text.clone(),
                });
            }
        }
    }

    pub(crate) fn update_trigram_index_for_delete(
        &self,
        table_name: &str,
        schema: &TableSchema,
        row_values: &[Value],
        row_id: &str,
        trigram_column_indices: &[usize],
        txn: &mut dyn crate::storage::Transaction,
    ) {
        if trigram_column_indices.is_empty() {
            return;
        }

        let Some(ftxn) = txn
            .as_any()
            .downcast_ref::<crate::storage::fusion::FusionTransaction>()
        else {
            return;
        };

        let numeric_id = Self::trigram_numeric_row_id(row_id);
        for &idx in trigram_column_indices {
            if let Some(Value::String(text)) = row_values.get(idx) {
                ftxn.defer_side_index_delta(
                    crate::storage::fusion::SideIndexDelta::TrigramRemove {
                        table: table_name.to_string(),
                        column: schema.columns[idx].name.clone(),
                        numeric_id,
                        text: text.clone(),
                    },
                );
            }
        }
    }

    /// Defer an HNSW insert to commit time on the Fusion backend (aborted
    /// transactions must not touch the shared vector index); apply directly
    /// on other backends (test-only MemoryStorage keeps its old semantics).
    pub(crate) fn defer_or_apply_vector_insert(
        &self,
        index_name: &str,
        id: String,
        vector: Vec<f32>,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        if let Some(ftxn) = txn
            .as_any()
            .downcast_ref::<crate::storage::fusion::FusionTransaction>()
        {
            // Fail the statement now (pre-commit) on a dimension conflict;
            // the deferred apply must not be the first place this surfaces.
            self.vector_index
                .validate_insert_dimensions(index_name, vector.len())?;
            ftxn.defer_side_index_delta(crate::storage::fusion::SideIndexDelta::VectorInsert {
                index: index_name.to_string(),
                id,
                vector,
            });
            return Ok(());
        }
        self.vector_index.insert(index_name, id, vector)
    }

    /// Commit-deferred counterpart of `VectorIndex::delete`.
    pub(crate) fn defer_or_apply_vector_delete(
        &self,
        index_name: &str,
        id: &str,
        txn: &mut dyn crate::storage::Transaction,
    ) -> Result<()> {
        if let Some(ftxn) = txn
            .as_any()
            .downcast_ref::<crate::storage::fusion::FusionTransaction>()
        {
            ftxn.defer_side_index_delta(crate::storage::fusion::SideIndexDelta::VectorDelete {
                index: index_name.to_string(),
                id: id.to_string(),
            });
            return Ok(());
        }
        self.vector_index.delete(index_name, id)?;
        Ok(())
    }

    pub(crate) fn trigram_numeric_row_id(row_id: &str) -> u64 {
        crate::storage::trigram::numeric_row_id_for_str(row_id)
    }

    pub(super) fn primary_key_row_id_from_eq_selection(
        &self,
        selection: Option<&Expr>,
        schema: &TableSchema,
        params: &[Value],
        allowed_qualifiers: &[String],
    ) -> Option<String> {
        let Expr::BinaryOp { left, op, right } = selection? else {
            return None;
        };
        if *op != BinaryOperator::Eq {
            return None;
        }

        let (col_name, value_expr) = if let Some(col_name) =
            self.primary_key_column_name(left.as_ref(), schema, allowed_qualifiers)
        {
            (col_name, right.as_ref())
        } else if let Some(col_name) =
            self.primary_key_column_name(right.as_ref(), schema, allowed_qualifiers)
        {
            (col_name, left.as_ref())
        } else {
            return None;
        };

        if self.expr_has_column_reference(value_expr) {
            return None;
        }

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
            .evaluate_value(value_expr, &[], schema, params)
            .unwrap_or(Value::Null)
        {
            Value::Integer(i) => Some(crate::common::encoding::encode_i64_comparable(i)),
            Value::String(s) => Some(s),
            Value::Date(days) => Some(crate::common::encoding::encode_i64_comparable(days as i64)),
            Value::Timestamp(micros) => {
                Some(crate::common::encoding::encode_i64_comparable(micros))
            }
            _ => None,
        }
    }

    fn dml_compound_identifier_prefix(idents: &[sqlparser::ast::Ident]) -> String {
        let prefix_len = idents.len().saturating_sub(1);
        let capacity = idents
            .iter()
            .take(prefix_len)
            .map(|ident| ident.value.len())
            .sum::<usize>()
            + prefix_len.saturating_sub(1);
        let mut qualifier = String::with_capacity(capacity);

        for (index, ident) in idents.iter().take(prefix_len).enumerate() {
            if index > 0 {
                qualifier.push('.');
            }
            qualifier.push_str(&ident.value);
        }

        qualifier
    }

    fn primary_key_column_name<'a>(
        &self,
        expr: &'a Expr,
        schema: &TableSchema,
        allowed_qualifiers: &[String],
    ) -> Option<&'a str> {
        let col_name = match expr {
            Expr::Identifier(ident) => &ident.value,
            Expr::CompoundIdentifier(idents) => {
                if idents.len() < 2 {
                    return None;
                }

                let qualifier = Self::dml_compound_identifier_prefix(idents);

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
        let col_idx = schema
            .columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(col_name))?;
        if col_idx == pk_idx {
            Some(col_name)
        } else {
            None
        }
    }

    pub(super) fn primary_key_qualifiers(relation: &TableFactor) -> Vec<String> {
        let mut qualifiers = Vec::with_capacity(2);
        if let TableFactor::Table { name, alias, .. } = relation {
            let table_name = name.to_string();
            qualifiers.push(table_name);
            if let Some(alias) = alias {
                qualifiers.push(alias.name.value.clone());
            }
        }
        qualifiers
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{Column, IndexType, TableSchema};

    use super::Executor;

    fn column(name: &str, data_type: &str, is_indexed: bool, index_type: IndexType) -> Column {
        Column {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_primary: false,
            is_indexed,
            index_type,
            default_value: None,
            is_nullable: true,
            is_unique: false,
            check_expr: None,
        }
    }

    #[test]
    fn trigram_text_data_type_matching_is_ascii_case_insensitive() {
        assert!(super::is_trigram_text_data_type("TEXT"));
        assert!(super::is_trigram_text_data_type(" string "));
        assert!(super::is_trigram_text_data_type("varchar(32)"));
        assert!(super::is_trigram_text_data_type("Char(8)"));
        assert!(super::is_trigram_text_data_type("character varying"));
        assert!(!super::is_trigram_text_data_type("INTEGER"));
    }

    #[test]
    fn indexed_trigram_text_columns_filters_text_indexes_without_uppercase_allocation() {
        let schema = TableSchema::new(
            "docs".to_string(),
            vec![
                column("id", "INTEGER", false, IndexType::None),
                column("body", " varchar(255) ", true, IndexType::BTree),
                column("title", "TEXT", true, IndexType::FTS),
                column("score", "TEXT", false, IndexType::None),
                column("embedding", "TEXT", true, IndexType::HNSW),
            ],
        );

        assert_eq!(Executor::indexed_trigram_text_columns(&schema), vec![1, 2]);
    }

    #[test]
    fn fts_index_key_for_row_preallocates_exact_key() {
        let key = Executor::fts_index_key_for_row("docs", "body", "search", "0007");

        assert_eq!(key, "fts:docs:body:search:0007");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn fts_column_prefix_for_column_preallocates_exact_prefix() {
        let prefix = Executor::fts_column_prefix_for_column("docs", "body");

        assert_eq!(prefix, "fts:docs:body:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn fts_token_prefix_for_token_preallocates_exact_prefix() {
        let prefix = Executor::fts_token_prefix_for_token("docs", "body", "search");

        assert_eq!(prefix, "fts:docs:body:search:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn hnsw_index_name_for_column_uses_structured_identity() {
        let name = Executor::hnsw_index_name_for_column("docs", "embedding").unwrap();

        assert_eq!(name, "hnsw_v2_AEZEQksCBwAAAARkb2NzAAAACWVtYmVkZGluZw");
    }
}
