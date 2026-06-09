use crate::catalog::TableSchema;
use crate::common::{Result, Value};
use crate::storage::Transaction;
use base64::Engine;
use sqlparser::ast::{BinaryOperator, Expr, OrderByKind};
use std::cmp::Ordering;
use std::collections::HashMap;

use super::Executor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositeIndexMeta {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub ordered_encoding: bool,
}

#[derive(Clone)]
struct CompositeRangeBound {
    component: String,
    inclusive: bool,
}

struct CompositeRangeBounds {
    lower: Option<CompositeRangeBound>,
    upper: Option<CompositeRangeBound>,
}

impl CompositeIndexMeta {
    fn encoded_columns(&self) -> String {
        self.columns.join(",")
    }
}

impl Executor {
    fn composite_index_table_marker_key(table_name: &str) -> String {
        let mut key =
            String::with_capacity("index_meta_table:".len() + table_name.len() + ":__marker".len());
        key.push_str("index_meta_table:");
        key.push_str(table_name);
        key.push_str(":__marker");
        key
    }

    pub(crate) fn composite_index_table_prefix(table_name: &str) -> String {
        let mut prefix = String::with_capacity("index_meta_table:".len() + table_name.len() + 1);
        prefix.push_str("index_meta_table:");
        prefix.push_str(table_name);
        prefix.push(':');
        prefix
    }

    pub(crate) fn composite_index_table_meta_key(table_name: &str, index_name: &str) -> String {
        let mut key = String::with_capacity(
            "index_meta_table:".len() + table_name.len() + 1 + index_name.len(),
        );
        key.push_str("index_meta_table:");
        key.push_str(table_name);
        key.push(':');
        key.push_str(index_name);
        key
    }

    fn composite_index_component_separator() -> &'static str {
        "|"
    }

    fn composite_index_meta_value_for_prefix(
        prefix: &str,
        table: &str,
        columns: &[String],
    ) -> String {
        let columns_len: usize = columns.iter().map(|column| column.len()).sum();
        let mut value = String::with_capacity(
            prefix.len() + 1 + table.len() + 1 + columns_len + columns.len().saturating_sub(1),
        );
        value.push_str(prefix);
        value.push(':');
        value.push_str(table);
        value.push(':');
        for (idx, column) in columns.iter().enumerate() {
            if idx > 0 {
                value.push(',');
            }
            value.push_str(column);
        }
        value
    }

    pub(crate) fn composite_index_meta_value(table: &str, columns: &[String]) -> String {
        Self::composite_index_meta_value_for_prefix("v3", table, columns)
    }

    pub(crate) fn composite_unique_meta_value(table: &str, columns: &[String]) -> String {
        Self::composite_index_meta_value_for_prefix("u3", table, columns)
    }

    pub(crate) fn single_column_index_meta_value(table: &str, column: &str) -> String {
        let mut value = String::with_capacity(table.len() + 1 + column.len());
        value.push_str(table);
        value.push(':');
        value.push_str(column);
        value
    }

    fn prefixed_index_component(prefix: char, encoded: &str) -> String {
        let mut component = String::with_capacity(prefix.len_utf8() + encoded.len());
        component.push(prefix);
        component.push_str(encoded);
        component
    }

    pub(crate) fn parse_index_meta(index_name: &str, meta_str: &str) -> Option<CompositeIndexMeta> {
        let rest = meta_str
            .strip_prefix("v3:")
            .or_else(|| meta_str.strip_prefix("u3:"));
        if let Some(rest) = rest {
            let (table, columns) = rest.split_once(':')?;
            let mut parsed_columns = Vec::with_capacity(columns.matches(',').count() + 1);
            for column in columns.split(',') {
                let column = column.trim();
                if !column.is_empty() {
                    parsed_columns.push(column.to_owned());
                }
            }

            if table.is_empty() || parsed_columns.is_empty() {
                return None;
            }

            Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table: table.to_string(),
                columns: parsed_columns,
                ordered_encoding: true,
            })
        } else if let Some(rest) = meta_str.strip_prefix("v2:") {
            let (table, columns) = rest.split_once(':')?;
            let mut parsed_columns = Vec::with_capacity(columns.matches(',').count() + 1);
            for column in columns.split(',') {
                let column = column.trim();
                if !column.is_empty() {
                    parsed_columns.push(column.to_owned());
                }
            }

            if table.is_empty() || parsed_columns.is_empty() {
                return None;
            }

            Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table: table.to_string(),
                columns: parsed_columns,
                ordered_encoding: false,
            })
        } else {
            let (table, column) = meta_str.split_once(':')?;
            if table.is_empty() || column.is_empty() {
                return None;
            }

            Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table: table.to_string(),
                columns: vec![column.to_string()],
                ordered_encoding: false,
            })
        }
    }

    pub(crate) async fn load_composite_indexes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<CompositeIndexMeta>> {
        let marker_key = Self::composite_index_table_marker_key(table_name);
        if txn.get(marker_key.as_bytes()).await?.is_some() {
            return self
                .load_composite_indexes_for_table_directory(table_name, txn)
                .await;
        }

        self.load_composite_indexes_for_table_legacy_scan(table_name, txn)
            .await
    }

    pub(crate) async fn load_composite_unique_indexes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<CompositeIndexMeta>> {
        let indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        let mut unique_indexes = Vec::with_capacity(indexes.len());
        for index in indexes {
            if index.name.ends_with("_pkey") {
                unique_indexes.push(index);
            }
        }

        Ok(unique_indexes)
    }

    async fn load_composite_indexes_for_table_directory(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<CompositeIndexMeta>> {
        let prefix = Self::composite_index_table_prefix(table_name);
        let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
        let mut indexes = Vec::with_capacity(entries.len());

        for (key, value) in entries {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix(&prefix) else {
                continue;
            };
            if index_name == "__marker" {
                continue;
            }

            let meta_str = String::from_utf8(value).unwrap_or_default();
            let Some(meta) = Self::parse_index_meta(index_name, &meta_str) else {
                continue;
            };

            if meta.table == table_name && meta.columns.len() > 1 {
                indexes.push(meta);
            }
        }

        Ok(indexes)
    }

    async fn load_composite_indexes_for_table_legacy_scan(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<CompositeIndexMeta>> {
        let entries = txn.scan_prefix(b"index_meta:", None).await?;
        let mut indexes = Vec::with_capacity(entries.len());

        for (key, value) in entries {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix("index_meta:") else {
                continue;
            };

            let meta_str = String::from_utf8(value).unwrap_or_default();
            let Some(meta) = Self::parse_index_meta(index_name, &meta_str) else {
                continue;
            };

            if meta.table == table_name && meta.columns.len() > 1 {
                indexes.push(meta);
            }
        }

        Ok(indexes)
    }

    pub(crate) async fn ensure_composite_index_directory_marker(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let marker_key = Self::composite_index_table_marker_key(table_name);
        txn.put(marker_key.as_bytes(), b"v1").await
    }

    pub(crate) async fn rebuild_composite_index_directory_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let prefix = Self::composite_index_table_prefix(table_name);
        let existing_entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
        for (key, _) in existing_entries {
            txn.delete(&key).await?;
        }

        self.ensure_composite_index_directory_marker(table_name, txn)
            .await?;

        let global_entries = txn.scan_prefix(b"index_meta:", None).await?;
        for (key, value) in global_entries {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix("index_meta:") else {
                continue;
            };

            let meta_str = String::from_utf8(value.clone()).unwrap_or_default();
            let Some(meta) = Self::parse_index_meta(index_name, &meta_str) else {
                continue;
            };
            if meta.table == table_name && meta.columns.len() > 1 {
                let table_meta_key = Self::composite_index_table_meta_key(table_name, index_name);
                txn.put(table_meta_key.as_bytes(), &value).await?;
            }
        }

        Ok(())
    }

    pub(crate) fn composite_index_prefix(table_name: &str, columns: &[String]) -> String {
        let columns_len = columns.iter().map(String::len).sum::<usize>();
        let mut prefix = String::with_capacity(
            "index:".len()
                + table_name.len()
                + 1
                + columns_len
                + columns.len().saturating_sub(1)
                + 1,
        );
        prefix.push_str("index:");
        prefix.push_str(table_name);
        prefix.push(':');
        for (idx, column) in columns.iter().enumerate() {
            if idx > 0 {
                prefix.push(',');
            }
            prefix.push_str(column);
        }
        prefix.push(':');
        prefix
    }

    fn composite_index_entry_key(prefix: &str, value_key: &str, row_id: &str) -> String {
        let mut key = String::with_capacity(prefix.len() + value_key.len() + 1 + row_id.len());
        key.push_str(prefix);
        key.push_str(value_key);
        key.push(':');
        key.push_str(row_id);
        key
    }

    fn composite_index_value_prefix(prefix: &str, value_key: &str) -> String {
        let mut value_prefix = String::with_capacity(prefix.len() + value_key.len() + 1);
        value_prefix.push_str(prefix);
        value_prefix.push_str(value_key);
        value_prefix.push(':');
        value_prefix
    }

    pub(crate) fn composite_index_key(
        &self,
        table_name: &str,
        columns: &[String],
        row: &[Value],
        schema: &TableSchema,
        row_id: &str,
    ) -> Option<String> {
        let value_key = self.composite_index_value_key(columns, row, schema, true)?;
        let prefix = Self::composite_index_prefix(table_name, columns);
        Some(Self::composite_index_entry_key(&prefix, &value_key, row_id))
    }

    fn composite_index_key_for_meta(
        &self,
        meta: &CompositeIndexMeta,
        table_name: &str,
        row: &[Value],
        schema: &TableSchema,
        row_id: &str,
    ) -> Option<String> {
        let value_key =
            self.composite_index_value_key(&meta.columns, row, schema, meta.ordered_encoding)?;
        let prefix = Self::composite_index_prefix(table_name, &meta.columns);
        Some(Self::composite_index_entry_key(&prefix, &value_key, row_id))
    }

    pub(crate) fn composite_index_value_key_for_columns(
        &self,
        columns: &[String],
        row: &[Value],
        schema: &TableSchema,
    ) -> Option<String> {
        self.composite_index_value_key(columns, row, schema, true)
    }

    pub(crate) fn composite_index_value_key_for_meta_values(
        &self,
        meta: &CompositeIndexMeta,
        values: &[Value],
    ) -> Option<String> {
        if meta.columns.len() != values.len() {
            return None;
        }
        let mut parts = Vec::with_capacity(values.len());
        for value in values {
            parts.push(self.index_component_for_meta(value, meta)?);
        }
        Some(parts.join(Self::composite_index_component_separator()))
    }

    fn composite_index_value_key(
        &self,
        columns: &[String],
        row: &[Value],
        schema: &TableSchema,
        ordered_encoding: bool,
    ) -> Option<String> {
        let mut parts = Vec::with_capacity(columns.len());

        for column in columns {
            let idx = schema.get_column_index(column)?;
            let value = row.get(idx)?;
            let part = if ordered_encoding {
                self.ordered_index_component(value)?
            } else {
                self.legacy_encoded_index_component(value)?
            };
            parts.push(part);
        }

        Some(parts.join(Self::composite_index_component_separator()))
    }

    fn legacy_encoded_index_component(&self, value: &Value) -> Option<String> {
        let raw = self.value_to_index_string(value)?;
        Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes()))
    }

    fn ordered_index_component(&self, value: &Value) -> Option<String> {
        Some(match value {
            Value::Integer(value) => {
                let encoded = crate::common::encoding::encode_i64_comparable(*value);
                Self::prefixed_index_component('i', &encoded)
            }
            Value::Date(days) => {
                let encoded = crate::common::encoding::encode_i64_comparable(*days as i64);
                Self::prefixed_index_component('d', &encoded)
            }
            Value::Timestamp(micros) => {
                let encoded = crate::common::encoding::encode_i64_comparable(*micros);
                Self::prefixed_index_component('t', &encoded)
            }
            Value::Interval(micros) => {
                let encoded = crate::common::encoding::encode_i64_comparable(*micros);
                Self::prefixed_index_component('v', &encoded)
            }
            Value::Boolean(value) => {
                if *value {
                    "b1".to_string()
                } else {
                    "b0".to_string()
                }
            }
            Value::String(value) => {
                let encoded =
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.as_bytes());
                Self::prefixed_index_component('s', &encoded)
            }
            Value::Decimal(value) => {
                let encoded =
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.as_bytes());
                Self::prefixed_index_component('n', &encoded)
            }
            _ => return None,
        })
    }

    fn index_component_for_meta(&self, value: &Value, meta: &CompositeIndexMeta) -> Option<String> {
        if meta.ordered_encoding {
            self.ordered_index_component(value)
        } else {
            self.legacy_encoded_index_component(value)
        }
    }

    pub(crate) async fn put_loaded_composite_indexes_for_row(
        &self,
        indexes: &[CompositeIndexMeta],
        table_name: &str,
        schema: &TableSchema,
        row: &[Value],
        row_id: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for index in indexes {
            if let Some(index_key) =
                self.composite_index_key_for_meta(index, table_name, row, schema, row_id)
            {
                txn.put(index_key.as_bytes(), &[]).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn validate_composite_unique_constraints(
        &self,
        indexes: &[CompositeIndexMeta],
        table_name: &str,
        schema: &TableSchema,
        row: &[Value],
        current_row_id: Option<&str>,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for index in indexes {
            let Some(value_key) =
                self.composite_index_value_key_for_columns(&index.columns, row, schema)
            else {
                continue;
            };
            let index_prefix = Self::composite_index_prefix(table_name, &index.columns);
            let prefix = Self::composite_index_value_prefix(&index_prefix, &value_key);
            let entries = txn.scan_prefix(prefix.as_bytes(), Some(1)).await?;
            for (key, _) in entries {
                let Some(row_id) = Self::row_id_from_key(&key) else {
                    continue;
                };
                if current_row_id.is_some_and(|current| current == row_id) {
                    continue;
                }
                return Err(crate::common::FusionError::Execution(format!(
                    "UNIQUE constraint violated for columns '{}'",
                    index.columns.join(", ")
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn row_id_for_insert(
        &self,
        schema: &TableSchema,
        row: &[Value],
        composite_unique_indexes: &[CompositeIndexMeta],
    ) -> String {
        if let Some(primary_key) = composite_unique_indexes
            .iter()
            .find(|index| index.name.ends_with("_pkey"))
        {
            if let Some(value_key) =
                self.composite_index_value_key_for_columns(&primary_key.columns, row, schema)
            {
                return value_key;
            }
        }

        if let Some(pk_idx) = schema.get_primary_key_index() {
            if let Some(pk_value) = row.get(pk_idx) {
                if let Some(row_id) = Self::value_to_primary_row_id(pk_value) {
                    return row_id;
                }
            }
        }

        uuid::Uuid::new_v4().to_string()
    }

    pub(crate) async fn delete_loaded_composite_indexes_for_row(
        &self,
        indexes: &[CompositeIndexMeta],
        table_name: &str,
        schema: &TableSchema,
        row: &[Value],
        row_id: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for index in indexes {
            if let Some(index_key) =
                self.composite_index_key_for_meta(index, table_name, row, schema, row_id)
            {
                txn.delete(index_key.as_bytes()).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn update_loaded_composite_indexes_for_row(
        &self,
        indexes: &[CompositeIndexMeta],
        table_name: &str,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: &[Value],
        row_id: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for index in indexes {
            let touches_index = index.columns.iter().any(|column| {
                schema
                    .get_column_index(column)
                    .is_some_and(|idx| old_row.get(idx) != new_row.get(idx))
            });

            if !touches_index {
                continue;
            }

            if let Some(old_key) =
                self.composite_index_key_for_meta(index, table_name, old_row, schema, row_id)
            {
                txn.delete(old_key.as_bytes()).await?;
            }

            if let Some(new_key) =
                self.composite_index_key_for_meta(index, table_name, new_row, schema, row_id)
            {
                txn.put(new_key.as_bytes(), &[]).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn try_composite_index_scan(
        &self,
        expr: &Expr,
        table_name: &str,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
        params: &[Value],
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
        ordered_limit: Option<usize>,
    ) -> Result<Option<super::scan::IndexScanPlan>> {
        let indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        if indexes.is_empty() {
            return Ok(None);
        }

        let predicates = Self::collect_conjunctive_predicates(expr);
        let equality_values = self.composite_index_equality_values(&predicates, schema, params)?;

        let mut best: Option<(CompositeIndexMeta, Vec<String>, bool)> = None;
        for index in indexes {
            let mut components = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                let Some(value) = equality_values.get(&column.to_ascii_lowercase()) else {
                    break;
                };
                let Some(component) = self.index_component_for_meta(value, &index) else {
                    break;
                };
                components.push(component);
            }

            if components.is_empty() {
                continue;
            }

            let exact = components.len() == index.columns.len();
            if best.as_ref().is_none_or(|(_, current_components, _)| {
                components.len() > current_components.len()
            }) {
                best = Some((index, components, exact));
            }
        }

        let Some((index, components, all_index_columns_matched)) = best else {
            return Ok(None);
        };

        let range_column = index.columns.get(components.len());
        let range_column_orderable = range_column
            .and_then(|column| schema.get_column_index(column))
            .is_some_and(|idx| {
                Self::composite_column_type_is_orderable(&schema.columns[idx].data_type)
            });

        let range = if index.ordered_encoding
            && components.len() < index.columns.len()
            && range_column_orderable
        {
            self.composite_index_range_bounds(&predicates, schema, params, range_column.unwrap())?
        } else {
            None
        };
        let range_predicate_count = range
            .as_ref()
            .map(|range| usize::from(range.lower.is_some()) + usize::from(range.upper.is_some()))
            .unwrap_or(0);

        let order_direction = if index.ordered_encoding && range_column_orderable {
            Self::composite_order_next_column_direction(order_by, range_column.map(String::as_str))
        } else {
            None
        };
        let order_matches = order_direction.is_some();

        let index_prefix = format!(
            "{}{}",
            Self::composite_index_prefix(table_name, &index.columns),
            components.join(Self::composite_index_component_separator())
        );

        let can_cover_predicates = all_index_columns_matched
            && predicates.len() == index.columns.len()
            || (range.is_some()
                && predicates.len() == components.len().saturating_add(range_predicate_count));
        let scan_limit = if order_matches && can_cover_predicates {
            ordered_limit.or(limit)
        } else if all_index_columns_matched {
            limit
        } else {
            None
        };

        let index_entries = if let Some(range) = range {
            let range_prefix = format!(
                "{}{}",
                index_prefix,
                Self::composite_index_component_separator()
            );
            let start = if let Some(lower) = range.lower {
                format!(
                    "{}{}:{}",
                    range_prefix,
                    lower.component,
                    if lower.inclusive { "" } else { "\u{0}" }
                )
            } else {
                range_prefix.clone()
            };
            if let Some(upper) = range.upper {
                let end = format!(
                    "{}{}:{}",
                    range_prefix,
                    upper.component,
                    if upper.inclusive { "\u{0}" } else { "" }
                );
                txn.scan_range(start.as_bytes(), end.as_bytes(), scan_limit)
                    .await?
            } else {
                let mut end = range_prefix.into_bytes();
                end.push(0xFF);
                txn.scan_range(start.as_bytes(), &end, scan_limit).await?
            }
        } else {
            let mut prefix = index_prefix;
            if all_index_columns_matched {
                prefix.push(':');
            } else {
                prefix.push_str(Self::composite_index_component_separator());
            }
            txn.scan_prefix(prefix.as_bytes(), scan_limit).await?
        };

        let mut row_ids = std::collections::HashSet::with_capacity(index_entries.len());
        let mut ordered_row_ids = if order_matches {
            Some(Vec::with_capacity(index_entries.len()))
        } else {
            None
        };
        for (key, _) in index_entries {
            if let Some(row_id) = Self::row_id_from_key(&key) {
                let row_id = row_id.to_string();
                if row_ids.insert(row_id.clone()) {
                    if let Some(ordered) = &mut ordered_row_ids {
                        ordered.push(row_id);
                    }
                }
            }
        }
        if let (Some(ordered), Some(asc)) = (&mut ordered_row_ids, order_direction) {
            if !asc {
                ordered.reverse();
            }
            if can_cover_predicates {
                if let Some(limit) = ordered_limit.or(limit) {
                    ordered.truncate(limit);
                }
            }
        }

        Ok(Some(super::scan::IndexScanPlan {
            row_ids,
            ordered_row_ids,
            exact: can_cover_predicates,
        }))
    }

    fn composite_index_equality_values(
        &self,
        predicates: &[Expr],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<HashMap<String, Value>> {
        let mut values = HashMap::with_capacity(predicates.len());

        for predicate in predicates {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = predicate
            else {
                continue;
            };

            let Some((_, column, value_expr)) =
                self.equality_schema_column_value_expr(left, right, schema)
            else {
                continue;
            };

            let value = self.evaluate_value(value_expr, &[], schema, params)?;
            if self.value_to_index_string(&value).is_some() {
                values.insert(column.to_ascii_lowercase(), value);
            }
        }

        Ok(values)
    }

    fn composite_index_range_bounds(
        &self,
        predicates: &[Expr],
        schema: &TableSchema,
        params: &[Value],
        range_column: &str,
    ) -> Result<Option<CompositeRangeBounds>> {
        let mut lower: Option<CompositeRangeBound> = None;
        let mut upper: Option<CompositeRangeBound> = None;

        for predicate in predicates {
            let Expr::BinaryOp { left, op, right } = predicate else {
                continue;
            };
            let Some((range_op, value_expr)) =
                self.composite_index_range_value_expr(left, op, right, schema, range_column)
            else {
                continue;
            };

            let value = self.evaluate_value(value_expr, &[], schema, params)?;
            let Some(component) = self.ordered_index_component(&value) else {
                continue;
            };
            let bound = CompositeRangeBound {
                component,
                inclusive: matches!(range_op, BinaryOperator::GtEq | BinaryOperator::LtEq),
            };

            match range_op {
                BinaryOperator::Gt | BinaryOperator::GtEq => {
                    if Self::range_lower_is_better(lower.as_ref(), &bound) {
                        lower = Some(bound);
                    }
                }
                BinaryOperator::Lt | BinaryOperator::LtEq => {
                    if Self::range_upper_is_better(upper.as_ref(), &bound) {
                        upper = Some(bound);
                    }
                }
                _ => {}
            }
        }

        if lower.is_none() && upper.is_none() {
            Ok(None)
        } else {
            Ok(Some(CompositeRangeBounds { lower, upper }))
        }
    }

    fn composite_index_range_value_expr<'a>(
        &self,
        left: &'a Expr,
        op: &BinaryOperator,
        right: &'a Expr,
        schema: &TableSchema,
        range_column: &str,
    ) -> Option<(BinaryOperator, &'a Expr)> {
        let normalized_op = match op {
            BinaryOperator::Gt => BinaryOperator::Gt,
            BinaryOperator::GtEq => BinaryOperator::GtEq,
            BinaryOperator::Lt => BinaryOperator::Lt,
            BinaryOperator::LtEq => BinaryOperator::LtEq,
            _ => return None,
        };

        let left_matches = self
            .resolve_schema_column_name(left, schema)
            .is_some_and(|(_, column)| column.eq_ignore_ascii_case(range_column));
        if left_matches {
            if self.expr_has_column_reference(right) {
                None
            } else {
                Some((normalized_op, right))
            }
        } else {
            let right_matches = self
                .resolve_schema_column_name(right, schema)
                .is_some_and(|(_, column)| column.eq_ignore_ascii_case(range_column));
            if !right_matches || self.expr_has_column_reference(left) {
                return None;
            }
            let flipped_op = match op {
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                _ => return None,
            };
            Some((flipped_op, left))
        }
    }

    fn range_lower_is_better(
        current: Option<&CompositeRangeBound>,
        candidate: &CompositeRangeBound,
    ) -> bool {
        let Some(current) = current else {
            return true;
        };
        match candidate.component.cmp(&current.component) {
            Ordering::Greater => true,
            Ordering::Equal => !candidate.inclusive && current.inclusive,
            Ordering::Less => false,
        }
    }

    fn range_upper_is_better(
        current: Option<&CompositeRangeBound>,
        candidate: &CompositeRangeBound,
    ) -> bool {
        let Some(current) = current else {
            return true;
        };
        match candidate.component.cmp(&current.component) {
            Ordering::Less => true,
            Ordering::Equal => !candidate.inclusive && current.inclusive,
            Ordering::Greater => false,
        }
    }

    fn composite_order_next_column_direction(
        order_by: Option<&sqlparser::ast::OrderBy>,
        next_column: Option<&str>,
    ) -> Option<bool> {
        let (Some(order_by), Some(next_column)) = (order_by, next_column) else {
            return None;
        };
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return None;
        };
        let [order_expr] = exprs.as_slice() else {
            return None;
        };
        if Self::order_limit_column_name(&order_expr.expr)
            .is_some_and(|column| column.eq_ignore_ascii_case(next_column))
        {
            Some(order_expr.options.asc.unwrap_or(true))
        } else {
            None
        }
    }

    fn composite_column_type_is_orderable(data_type: &str) -> bool {
        let upper = data_type.to_ascii_uppercase();
        Self::is_integer_type_name(&upper)
            || matches!(upper.as_str(), "BOOL" | "BOOLEAN" | "DATE" | "DATE32")
            || upper == "TIMESTAMP"
            || upper == "TIMESTAMP WITHOUT TIME ZONE"
            || upper == "TIMESTAMP WITH TIME ZONE"
            || upper == "TIMESTAMPTZ"
            || upper == "DATETIME"
            || upper.starts_with("TIMESTAMP(")
            || upper.starts_with("DATETIME(")
            || upper == "INTERVAL"
            || upper.starts_with("INTERVAL ")
    }

    pub(crate) async fn delete_index_meta_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let entries = txn.scan_prefix(b"index_meta:", None).await?;
        for (key, value) in entries {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix("index_meta:") else {
                continue;
            };
            let meta_str = String::from_utf8(value).unwrap_or_default();
            if Self::parse_index_meta(index_name, &meta_str)
                .is_some_and(|meta| meta.table == table_name)
            {
                txn.delete(&key).await?;
            }
        }

        let table_prefix = Self::composite_index_table_prefix(table_name);
        let table_entries = txn.scan_prefix(table_prefix.as_bytes(), None).await?;
        for (key, _) in table_entries {
            txn.delete(&key).await?;
        }

        Ok(())
    }

    pub(crate) fn describe_index_columns(meta_str: &str) -> Option<(String, String, Vec<String>)> {
        let meta = Self::parse_index_meta("", meta_str)?;
        let table = meta.table.clone();
        let encoded_columns = meta.encoded_columns();
        Some((table, encoded_columns, meta.columns))
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;

    #[test]
    fn composite_index_table_marker_key_preallocates_exact_key() {
        let key = Executor::composite_index_table_marker_key("stock");

        assert_eq!(key, "index_meta_table:stock:__marker");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn composite_index_table_prefix_preallocates_exact_prefix() {
        let prefix = Executor::composite_index_table_prefix("stock");

        assert_eq!(prefix, "index_meta_table:stock:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn composite_index_table_meta_key_preallocates_exact_key() {
        let key = Executor::composite_index_table_meta_key("stock", "idx_stock_warehouse_item");

        assert_eq!(key, "index_meta_table:stock:idx_stock_warehouse_item");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn composite_index_prefix_preallocates_exact_prefix() {
        let columns = vec![
            "warehouse_id".to_string(),
            "district_id".to_string(),
            "customer_id".to_string(),
        ];
        let prefix = Executor::composite_index_prefix("orders", &columns);

        assert_eq!(prefix, "index:orders:warehouse_id,district_id,customer_id:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn composite_index_entry_key_preallocates_exact_key() {
        let key = Executor::composite_index_entry_key(
            "index:orders:warehouse_id,district_id:",
            "i1|i2",
            "0007",
        );

        assert_eq!(key, "index:orders:warehouse_id,district_id:i1|i2:0007");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn composite_index_value_prefix_preallocates_exact_prefix() {
        let prefix = Executor::composite_index_value_prefix(
            "index:orders:warehouse_id,district_id:",
            "i1|i2",
        );

        assert_eq!(prefix, "index:orders:warehouse_id,district_id:i1|i2:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn composite_index_meta_value_preallocates_exact_value() {
        let columns = vec!["warehouse_id".to_string(), "district_id".to_string()];
        let value = Executor::composite_index_meta_value("stock", &columns);

        assert_eq!(value, "v3:stock:warehouse_id,district_id");
        assert!(value.capacity() >= value.len());
    }

    #[test]
    fn composite_unique_meta_value_preallocates_exact_value() {
        let columns = vec!["warehouse_id".to_string(), "district_id".to_string()];
        let value = Executor::composite_unique_meta_value("stock", &columns);

        assert_eq!(value, "u3:stock:warehouse_id,district_id");
        assert!(value.capacity() >= value.len());
    }

    #[test]
    fn single_column_index_meta_value_preallocates_exact_value() {
        let value = Executor::single_column_index_meta_value("orders", "status");

        assert_eq!(value, "orders:status");
        assert!(value.capacity() >= value.len());
    }

    #[test]
    fn prefixed_index_component_preallocates_exact_component() {
        let component = Executor::prefixed_index_component('i', "800000000000002a");

        assert_eq!(component, "i800000000000002a");
        assert!(component.capacity() >= component.len());
    }
}
