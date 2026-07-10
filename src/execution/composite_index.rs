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
    pub include_columns: Vec<String>,
    pub ordered_encoding: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositeOrderedIndexAccess {
    pub index_name: String,
    pub order_column: String,
    pub ascending: bool,
    pub row_limit: usize,
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
        join_composite_index_parts(&self.columns, ",")
    }
}

fn join_composite_index_parts(parts: &[String], separator: &str) -> String {
    let parts_len = parts.iter().map(String::len).sum::<usize>();
    let mut joined =
        String::with_capacity(parts_len + separator.len() * parts.len().saturating_sub(1));
    if let Some((first, rest)) = parts.split_first() {
        joined.push_str(first);
        for part in rest {
            joined.push_str(separator);
            joined.push_str(part);
        }
    }
    joined
}

impl Executor {
    const MAX_C5_COMPOSITE_META_PARTS: usize = 1024;

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

    fn append_c5_meta_count(value: &mut String, count: usize) {
        value.push_str(&count.to_string());
        value.push(':');
    }

    fn append_c5_meta_part(value: &mut String, part: &str) {
        Self::append_c5_meta_count(value, part.len());
        value.push_str(part);
    }

    fn read_c5_meta_count(meta: &str, cursor: &mut usize) -> Option<usize> {
        let rest = meta.get(*cursor..)?;
        let colon = rest.find(':')?;
        if colon == 0 {
            return None;
        }
        let count = rest.get(..colon)?.parse::<usize>().ok()?;
        *cursor = cursor.checked_add(colon + 1)?;
        Some(count)
    }

    fn read_c5_meta_part(meta: &str, cursor: &mut usize) -> Option<String> {
        let len = Self::read_c5_meta_count(meta, cursor)?;
        let end = cursor.checked_add(len)?;
        let part = meta.get(*cursor..end)?;
        *cursor = end;
        Some(part.to_string())
    }

    fn parse_c5_index_meta_payload(meta: &str) -> Option<(String, Vec<String>, Vec<String>)> {
        let mut cursor = 0;
        let table = Self::read_c5_meta_part(meta, &mut cursor)?;
        let column_count = Self::read_c5_meta_count(meta, &mut cursor)?;
        if table.is_empty()
            || column_count == 0
            || column_count > Self::MAX_C5_COMPOSITE_META_PARTS
            || column_count > meta.len()
        {
            return None;
        }

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let column = Self::read_c5_meta_part(meta, &mut cursor)?;
            if column.is_empty() {
                return None;
            }
            columns.push(column);
        }

        let include_count = Self::read_c5_meta_count(meta, &mut cursor)?;
        if include_count == 0
            || include_count > Self::MAX_C5_COMPOSITE_META_PARTS
            || include_count > meta.len()
        {
            return None;
        }
        let mut include_columns = Vec::with_capacity(include_count);
        for _ in 0..include_count {
            let include_column = Self::read_c5_meta_part(meta, &mut cursor)?;
            if include_column.is_empty() {
                return None;
            }
            include_columns.push(include_column);
        }

        (cursor == meta.len()).then_some((table, columns, include_columns))
    }

    pub(crate) fn composite_index_meta_value_with_include(
        table: &str,
        columns: &[String],
        include_columns: &[String],
    ) -> String {
        if include_columns.is_empty() {
            return Self::composite_index_meta_value(table, columns);
        }

        let columns_len: usize = columns.iter().map(String::len).sum();
        let include_len: usize = include_columns.iter().map(String::len).sum();
        let mut value = String::with_capacity(
            "c5:".len()
                + table.len()
                + columns_len
                + include_len
                + (columns.len() + include_columns.len() + 3) * 8,
        );
        value.push_str("c5:");
        Self::append_c5_meta_part(&mut value, table);
        Self::append_c5_meta_count(&mut value, columns.len());
        for column in columns {
            Self::append_c5_meta_part(&mut value, column);
        }
        Self::append_c5_meta_count(&mut value, include_columns.len());
        for include_column in include_columns {
            Self::append_c5_meta_part(&mut value, include_column);
        }
        value
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

    pub(crate) fn single_column_index_meta_value_with_include(
        table: &str,
        column: &str,
        include_columns: &[String],
    ) -> String {
        if include_columns.is_empty() {
            return Self::single_column_index_meta_value(table, column);
        }

        let include_len = include_columns.iter().map(String::len).sum::<usize>();
        let mut value = String::with_capacity(
            "s3:".len()
                + table.len()
                + column.len()
                + include_len
                + (include_columns.len() + 3) * 8,
        );
        value.push_str("s3:");
        Self::append_c5_meta_part(&mut value, table);
        Self::append_c5_meta_count(&mut value, 1);
        Self::append_c5_meta_part(&mut value, column);
        Self::append_c5_meta_count(&mut value, include_columns.len());
        for include_column in include_columns {
            Self::append_c5_meta_part(&mut value, include_column);
        }
        value
    }

    fn prefixed_index_component(prefix: char, encoded: &str) -> String {
        let mut component = String::with_capacity(prefix.len_utf8() + encoded.len());
        component.push(prefix);
        component.push_str(encoded);
        component
    }

    pub(crate) fn parse_index_meta(index_name: &str, meta_str: &str) -> Option<CompositeIndexMeta> {
        if let Some(rest) = meta_str.strip_prefix("s3:") {
            let (table, columns, include_columns) = Self::parse_c5_index_meta_payload(rest)?;
            if columns.len() != 1 {
                return None;
            }

            return Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table,
                columns,
                include_columns,
                ordered_encoding: false,
            });
        }

        if let Some(rest) = meta_str.strip_prefix("s2:") {
            let (table_and_column, includes) = rest.rsplit_once(':')?;
            let (table, column) = table_and_column.split_once(':')?;
            let mut include_columns = Vec::with_capacity(includes.matches(',').count() + 1);
            for include_column in includes.split(',') {
                let include_column = include_column.trim();
                if !include_column.is_empty() {
                    include_columns.push(include_column.to_owned());
                }
            }

            if table.is_empty() || column.is_empty() || include_columns.is_empty() {
                return None;
            }

            return Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table: table.to_string(),
                columns: vec![column.to_string()],
                include_columns,
                ordered_encoding: false,
            });
        }

        if let Some(rest) = meta_str.strip_prefix("c5:") {
            let (table, columns, include_columns) = Self::parse_c5_index_meta_payload(rest)?;
            return Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table,
                columns,
                include_columns,
                ordered_encoding: true,
            });
        }

        if let Some(rest) = meta_str.strip_prefix("c4:") {
            let mut parts = rest.splitn(3, ':');
            let table = parts.next()?;
            let columns = parts.next()?;
            let includes = parts.next()?;
            let mut parsed_columns = Vec::with_capacity(columns.matches(',').count() + 1);
            for column in columns.split(',') {
                let column = column.trim();
                if !column.is_empty() {
                    parsed_columns.push(column.to_owned());
                }
            }
            let mut include_columns = Vec::with_capacity(includes.matches(',').count() + 1);
            for include_column in includes.split(',') {
                let include_column = include_column.trim();
                if !include_column.is_empty() {
                    include_columns.push(include_column.to_owned());
                }
            }

            if table.is_empty() || parsed_columns.is_empty() || include_columns.is_empty() {
                return None;
            }

            return Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table: table.to_string(),
                columns: parsed_columns,
                include_columns,
                ordered_encoding: true,
            });
        }

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
                include_columns: Vec::new(),
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
                include_columns: Vec::new(),
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
                include_columns: Vec::new(),
                ordered_encoding: false,
            })
        }
    }

    pub(crate) async fn load_single_column_index_includes_for_table(
        &self,
        table_name: &str,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<HashMap<usize, Vec<usize>>> {
        let entries = txn.scan_prefix(b"index_meta:", None).await?;
        let mut includes_by_column = HashMap::new();

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

            if meta.table != table_name
                || meta.columns.len() != 1
                || meta.include_columns.is_empty()
            {
                continue;
            }

            let Some(key_idx) = schema.get_column_index(&meta.columns[0]) else {
                continue;
            };
            let mut include_indices = Vec::with_capacity(meta.include_columns.len());
            for include_column in &meta.include_columns {
                if let Some(include_idx) = schema.get_column_index(include_column) {
                    include_indices.push(include_idx);
                }
            }
            if include_indices.len() == meta.include_columns.len() {
                includes_by_column.insert(key_idx, include_indices);
            }
        }

        Ok(includes_by_column)
    }

    pub(crate) fn secondary_index_payload_for_row(
        row: &[Value],
        include_indices: &[usize],
    ) -> Vec<u8> {
        if include_indices.is_empty() {
            return Vec::new();
        }

        let mut values = Vec::with_capacity(include_indices.len());
        for &idx in include_indices {
            values.push(row.get(idx).cloned().unwrap_or(Value::Null));
        }
        crate::common::encoding::RowEncoder::encode(&values)
    }

    pub(crate) fn secondary_index_payload_values(
        payload: &[u8],
        include_indices: &[usize],
    ) -> Option<Vec<Value>> {
        if include_indices.is_empty() {
            return Some(Vec::new());
        }
        if payload.is_empty() {
            return None;
        }

        let values = crate::common::encoding::RowDecoder::decode(payload).ok()?;
        (values.len() == include_indices.len()).then_some(values)
    }

    pub(crate) fn single_column_index_payload_touched(
        old_row: &[Value],
        new_row: &[Value],
        include_indices: &[usize],
    ) -> bool {
        include_indices
            .iter()
            .any(|&idx| old_row.get(idx) != new_row.get(idx))
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
        for (key, value) in existing_entries {
            if Self::composite_index_table_directory_entry_belongs_to_table(
                table_name, &key, &value,
            ) {
                txn.delete(&key).await?;
            }
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

    fn composite_index_table_directory_entry_belongs_to_table(
        table_name: &str,
        key: &[u8],
        value: &[u8],
    ) -> bool {
        let marker_key = Self::composite_index_table_marker_key(table_name);
        if key == marker_key.as_bytes() {
            return true;
        }

        let prefix = Self::composite_index_table_prefix(table_name);
        let Ok(key_str) = std::str::from_utf8(key) else {
            return false;
        };
        let Some(index_name) = key_str.strip_prefix(&prefix) else {
            return false;
        };
        let meta_str = String::from_utf8_lossy(value);
        Self::parse_index_meta(index_name, &meta_str).is_some_and(|meta| meta.table == table_name)
    }

    #[cfg(test)]
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

    pub(crate) fn routed_composite_index_prefixes(
        &self,
        table_name: &str,
        columns: &[String],
    ) -> Vec<String> {
        let column_key = join_composite_index_parts(columns, ",");
        self.routed_index_prefixes_for_column(table_name, &column_key)
    }

    fn routed_composite_index_entry_key(
        &self,
        table_name: &str,
        columns: &[String],
        value_key: &str,
        row_id: &str,
    ) -> String {
        let column_key = join_composite_index_parts(columns, ",");
        self.routed_index_key_for_value(table_name, &column_key, value_key, row_id)
    }

    #[cfg(test)]
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

    fn composite_index_components_prefix(prefix: &str, components: &str) -> String {
        let mut component_prefix = String::with_capacity(prefix.len() + components.len());
        component_prefix.push_str(prefix);
        component_prefix.push_str(components);
        component_prefix
    }

    fn composite_index_range_prefix(index_prefix: &str) -> String {
        let separator = Self::composite_index_component_separator();
        let mut range_prefix = String::with_capacity(index_prefix.len() + separator.len());
        range_prefix.push_str(index_prefix);
        range_prefix.push_str(separator);
        range_prefix
    }

    fn composite_index_range_bound(prefix: &str, component: &str, suffix: &str) -> String {
        let mut bound = String::with_capacity(prefix.len() + component.len() + 1 + suffix.len());
        bound.push_str(prefix);
        bound.push_str(component);
        bound.push(':');
        bound.push_str(suffix);
        bound
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
        Some(self.routed_composite_index_entry_key(table_name, columns, &value_key, row_id))
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
        Some(self.routed_composite_index_entry_key(table_name, &meta.columns, &value_key, row_id))
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
        Some(join_composite_index_parts(
            &parts,
            Self::composite_index_component_separator(),
        ))
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

        Some(join_composite_index_parts(
            &parts,
            Self::composite_index_component_separator(),
        ))
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

    fn ordered_index_component_value(component: &str, data_type: &str) -> Option<Value> {
        let (prefix, encoded) = component.split_at(1);
        match prefix {
            "i" if Self::is_integer_type_name(data_type) => {
                crate::common::encoding::decode_i64_comparable(encoded).map(Value::Integer)
            }
            "d" if Self::is_date_type_name(data_type) => {
                crate::common::encoding::decode_i64_comparable(encoded)
                    .map(|days| Value::Date(days as i32))
            }
            "t" if Self::is_timestamp_type_name(data_type) => {
                crate::common::encoding::decode_i64_comparable(encoded).map(Value::Timestamp)
            }
            "v" if Self::is_interval_type_name(data_type) => {
                crate::common::encoding::decode_i64_comparable(encoded).map(Value::Interval)
            }
            "b" if Self::is_boolean_type_name(data_type) => match encoded {
                "0" => Some(Value::Boolean(false)),
                "1" => Some(Value::Boolean(true)),
                _ => None,
            },
            "s" if matches!(
                data_type.to_ascii_lowercase().as_str(),
                "text" | "string" | "varchar" | "char"
            ) =>
            {
                let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded.as_bytes())
                    .ok()?;
                String::from_utf8(bytes).ok().map(Value::String)
            }
            "n" if Self::is_decimal_type_name(data_type) => {
                let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(encoded.as_bytes())
                    .ok()?;
                String::from_utf8(bytes).ok().map(Value::Decimal)
            }
            _ => None,
        }
    }

    fn ordered_composite_component_values(
        schema: &TableSchema,
        index: &CompositeIndexMeta,
        components: &[String],
    ) -> Option<Vec<Value>> {
        if !index.ordered_encoding || components.len() > index.columns.len() {
            return None;
        }

        let mut values = Vec::with_capacity(components.len());
        for (component, column_name) in components.iter().zip(&index.columns) {
            let column_idx = schema.get_column_index(column_name)?;
            let data_type = schema.columns.get(column_idx)?.data_type.as_str();
            values.push(Self::ordered_index_component_value(component, data_type)?);
        }
        Some(values)
    }

    fn ordered_composite_component_from_key<'a>(key: &'a [u8], prefix: &str) -> Option<&'a str> {
        let suffix = key.strip_prefix(prefix.as_bytes())?;
        let component_end = suffix.iter().position(|byte| *byte == b':')?;
        std::str::from_utf8(&suffix[..component_end]).ok()
    }

    fn ordered_composite_component_values_for_key(
        schema: &TableSchema,
        index: &CompositeIndexMeta,
        leading_component_values: &[Value],
        range_component_index: Option<usize>,
        key: &[u8],
        range_prefix: &str,
    ) -> Option<Vec<Value>> {
        let mut values = leading_component_values.to_vec();
        let Some(component_index) = range_component_index else {
            return Some(values);
        };
        let component = Self::ordered_composite_component_from_key(key, range_prefix)?;
        let column_name = index.columns.get(component_index)?;
        let column_idx = schema.get_column_index(column_name)?;
        let data_type = schema.columns.get(column_idx)?.data_type.as_str();
        values.push(Self::ordered_index_component_value(component, data_type)?);
        Some(values)
    }

    fn composite_index_covered_column_indices(
        schema: &TableSchema,
        pk_index: Option<usize>,
        index: &CompositeIndexMeta,
        component_count: usize,
        include_indices: &[usize],
    ) -> Vec<usize> {
        let mut indices = Vec::with_capacity(1 + component_count + include_indices.len());
        if let Some(pk_idx) = pk_index {
            indices.push(pk_idx);
        }
        for column_name in index.columns.iter().take(component_count) {
            if let Some(column_idx) = schema.get_column_index(column_name) {
                if !indices.contains(&column_idx) {
                    indices.push(column_idx);
                }
            }
        }
        for &include_idx in include_indices {
            if !indices.contains(&include_idx) {
                indices.push(include_idx);
            }
        }
        indices
    }

    fn composite_index_covered_row(
        schema: &TableSchema,
        pk_index: Option<usize>,
        row_id: &str,
        index: &CompositeIndexMeta,
        component_values: &[Value],
        include_indices: &[usize],
        include_values: Option<Vec<Value>>,
    ) -> Vec<Value> {
        let mut row = Self::primary_key_row_from_id(schema, pk_index, row_id);
        for (column_name, component_value) in index.columns.iter().zip(component_values) {
            if let Some(column_idx) = schema.get_column_index(column_name) {
                if let Some(value) = row.get_mut(column_idx) {
                    *value = component_value.clone();
                }
            }
        }
        if let Some(include_values) = include_values {
            for (&include_idx, include_value) in include_indices.iter().zip(include_values) {
                if let Some(value) = row.get_mut(include_idx) {
                    *value = include_value;
                }
            }
        }
        row
    }

    fn composite_index_include_indices(
        schema: &TableSchema,
        index: &CompositeIndexMeta,
    ) -> Option<Vec<usize>> {
        let mut include_indices = Vec::with_capacity(index.include_columns.len());
        for include_column in &index.include_columns {
            let include_idx = schema.get_column_index(include_column)?;
            if include_indices.contains(&include_idx) {
                return None;
            }
            include_indices.push(include_idx);
        }
        Some(include_indices)
    }

    fn composite_index_payload_for_row(
        schema: &TableSchema,
        index: &CompositeIndexMeta,
        row: &[Value],
    ) -> Option<Vec<u8>> {
        let include_indices = Self::composite_index_include_indices(schema, index)?;
        Some(Self::secondary_index_payload_for_row(row, &include_indices))
    }

    fn composite_index_payload_values(
        payload: &[u8],
        include_indices: &[usize],
        include_payloads_complete: &mut bool,
    ) -> Option<Vec<Value>> {
        let include_values = Self::secondary_index_payload_values(payload, include_indices);
        if !include_indices.is_empty() && include_values.is_none() {
            *include_payloads_complete = false;
        }
        include_values
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
                let payload =
                    Self::composite_index_payload_for_row(schema, index, row).unwrap_or_default();
                txn.put(index_key.as_bytes(), &payload).await?;
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
            for index_prefix in self.routed_composite_index_prefixes(table_name, &index.columns) {
                let prefix = Self::composite_index_value_prefix(&index_prefix, &value_key);
                let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
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
            let touches_index_key = index.columns.iter().any(|column| {
                schema
                    .get_column_index(column)
                    .is_some_and(|idx| old_row.get(idx) != new_row.get(idx))
            });
            let include_indices =
                Self::composite_index_include_indices(schema, index).unwrap_or_default();
            let touches_payload = !include_indices.is_empty()
                && Self::single_column_index_payload_touched(old_row, new_row, &include_indices);

            if !touches_index_key && !touches_payload {
                continue;
            }

            if touches_index_key {
                if let Some(old_key) =
                    self.composite_index_key_for_meta(index, table_name, old_row, schema, row_id)
                {
                    txn.delete(old_key.as_bytes()).await?;
                }
            }

            if let Some(new_key) =
                self.composite_index_key_for_meta(index, table_name, new_row, schema, row_id)
            {
                let payload = Self::composite_index_payload_for_row(schema, index, new_row)
                    .unwrap_or_default();
                txn.put(new_key.as_bytes(), &payload).await?;
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
        let equality_values =
            self.composite_index_equality_values_by_column_index(&predicates, schema, params)?;

        let mut best: Option<(CompositeIndexMeta, Vec<String>, bool)> = None;
        for index in indexes {
            let mut components = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                let Some(column_idx) = schema.get_column_index(column) else {
                    break;
                };
                let Some(value) = equality_values.get(&column_idx) else {
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

        let order_direction = if self.shard_router.is_none()
            && index.ordered_encoding
            && range_column_orderable
        {
            Self::composite_order_next_column_direction(order_by, range_column.map(String::as_str))
                .filter(|direction| *direction || txn.supports_bounded_scan_range_reverse())
        } else {
            None
        };
        let order_matches = order_direction.is_some();
        let scan_descending = matches!(order_direction, Some(false));

        let component_key =
            join_composite_index_parts(&components, Self::composite_index_component_separator());
        let index_prefixes = self
            .routed_composite_index_prefixes(table_name, &index.columns)
            .into_iter()
            .map(|base_prefix| {
                Self::composite_index_components_prefix(&base_prefix, &component_key)
            })
            .collect::<Vec<_>>();

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

        let mut row_ids = std::collections::HashSet::new();
        let mut ordered_row_ids = if order_matches {
            Some(Vec::new())
        } else {
            None
        };
        let pk_index = schema.get_primary_key_index();
        let range_component_index =
            (range.is_some() && components.len() < index.columns.len()).then_some(components.len());
        let covered_component_count =
            components.len() + usize::from(range_component_index.is_some());
        let leading_component_values =
            Self::ordered_composite_component_values(schema, &index, &components);
        let include_indices = Self::composite_index_include_indices(schema, &index);
        let include_indices_for_coverage = include_indices.as_deref().unwrap_or(&[]);
        let covered_column_indices = leading_component_values
            .as_ref()
            .map(|_| {
                Self::composite_index_covered_column_indices(
                    schema,
                    pk_index,
                    &index,
                    covered_component_count,
                    include_indices_for_coverage,
                )
            })
            .filter(|indices| !indices.is_empty());
        let mut covered_rows = covered_column_indices
            .as_ref()
            .map(|_| HashMap::with_capacity(scan_limit.unwrap_or(0).max(16)));
        let mut include_payloads_complete = true;
        let mut entry_visit_count = 0usize;

        if let Some(range) = range {
            for index_prefix in index_prefixes {
                let remaining = scan_limit.map(|limit| limit.saturating_sub(entry_visit_count));
                if remaining == Some(0) {
                    break;
                }
                let range_prefix = Self::composite_index_range_prefix(&index_prefix);
                let start = if let Some(lower) = range.lower.as_ref() {
                    let suffix = if lower.inclusive { "" } else { "\u{0}" };
                    Self::composite_index_range_bound(&range_prefix, &lower.component, suffix)
                } else {
                    range_prefix.clone()
                };

                let mut visitor = |key: &[u8], payload: &[u8]| {
                    if let Some(row_id) = Self::row_id_from_key(key) {
                        let row_id = row_id.to_string();
                        if row_ids.insert(row_id.clone()) {
                            if let Some(ordered) = &mut ordered_row_ids {
                                ordered.push(row_id.clone());
                            }
                            if let (Some(rows), Some(leading_values)) =
                                (&mut covered_rows, leading_component_values.as_ref())
                            {
                                if let Some(component_values) =
                                    Self::ordered_composite_component_values_for_key(
                                        schema,
                                        &index,
                                        leading_values,
                                        range_component_index,
                                        key,
                                        &range_prefix,
                                    )
                                    .filter(|values| values.len() == covered_component_count)
                                {
                                    let include_values = Self::composite_index_payload_values(
                                        payload,
                                        include_indices_for_coverage,
                                        &mut include_payloads_complete,
                                    );
                                    rows.insert(
                                        row_id.clone(),
                                        Self::composite_index_covered_row(
                                            schema,
                                            pk_index,
                                            &row_id,
                                            &index,
                                            &component_values,
                                            include_indices_for_coverage,
                                            include_values,
                                        ),
                                    );
                                }
                            }
                        }
                    }
                    true
                };
                let visited = if let Some(upper) = range.upper.as_ref() {
                    let suffix = if upper.inclusive { "\u{0}" } else { "" };
                    let end =
                        Self::composite_index_range_bound(&range_prefix, &upper.component, suffix);
                    if scan_descending {
                        txn.scan_range_reverse_for_each(
                            start.as_bytes(),
                            end.as_bytes(),
                            remaining,
                            &mut visitor,
                        )
                        .await?
                    } else {
                        txn.scan_range_for_each(
                            start.as_bytes(),
                            end.as_bytes(),
                            remaining,
                            &mut visitor,
                        )
                        .await?
                    }
                } else {
                    let mut end = range_prefix.clone().into_bytes();
                    end.push(0xFF);
                    if scan_descending {
                        txn.scan_range_reverse_for_each(
                            start.as_bytes(),
                            &end,
                            remaining,
                            &mut visitor,
                        )
                        .await?
                    } else {
                        txn.scan_range_for_each(start.as_bytes(), &end, remaining, &mut visitor)
                            .await?
                    }
                };
                entry_visit_count += visited;
                if scan_limit.is_some_and(|limit| entry_visit_count >= limit) {
                    break;
                }
            }
        } else {
            let mut prefixes = Vec::with_capacity(index_prefixes.len());
            for mut prefix in index_prefixes {
                if all_index_columns_matched {
                    prefix.push(':');
                } else {
                    prefix.push_str(Self::composite_index_component_separator());
                }
                prefixes.push(prefix);
            }
            if scan_descending {
                for prefix in prefixes {
                    let remaining = scan_limit.map(|limit| limit.saturating_sub(entry_visit_count));
                    if remaining == Some(0) {
                        break;
                    }
                    let mut end = prefix.as_bytes().to_vec();
                    end.push(0xFF);
                    let mut visitor = |key: &[u8], payload: &[u8]| {
                        if let Some(row_id) = Self::row_id_from_key(key) {
                            let row_id = row_id.to_string();
                            if row_ids.insert(row_id.clone()) {
                                if let Some(ordered) = &mut ordered_row_ids {
                                    ordered.push(row_id.clone());
                                }
                                if let (Some(rows), Some(leading_values)) =
                                    (&mut covered_rows, leading_component_values.as_ref())
                                {
                                    if let Some(component_values) =
                                        Self::ordered_composite_component_values_for_key(
                                            schema,
                                            &index,
                                            leading_values,
                                            range_component_index,
                                            key,
                                            &prefix,
                                        )
                                        .filter(|values| values.len() == covered_component_count)
                                    {
                                        let include_values = Self::composite_index_payload_values(
                                            payload,
                                            include_indices_for_coverage,
                                            &mut include_payloads_complete,
                                        );
                                        rows.insert(
                                            row_id.clone(),
                                            Self::composite_index_covered_row(
                                                schema,
                                                pk_index,
                                                &row_id,
                                                &index,
                                                &component_values,
                                                include_indices_for_coverage,
                                                include_values,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        true
                    };
                    let visited = txn
                        .scan_range_reverse_for_each(
                            prefix.as_bytes(),
                            &end,
                            remaining,
                            &mut visitor,
                        )
                        .await?;
                    entry_visit_count += visited;
                    if scan_limit.is_some_and(|limit| entry_visit_count >= limit) {
                        break;
                    }
                }
            } else {
                let mut visitor = |key: &[u8], payload: &[u8]| {
                    if let Some(row_id) = Self::row_id_from_key(key) {
                        let row_id = row_id.to_string();
                        if row_ids.insert(row_id.clone()) {
                            if let Some(ordered) = &mut ordered_row_ids {
                                ordered.push(row_id.clone());
                            }
                            if let (Some(rows), Some(leading_values)) =
                                (&mut covered_rows, leading_component_values.as_ref())
                            {
                                if let Some(component_values) =
                                    Self::ordered_composite_component_values_for_key(
                                        schema,
                                        &index,
                                        leading_values,
                                        range_component_index,
                                        key,
                                        "",
                                    )
                                    .filter(|values| values.len() == covered_component_count)
                                {
                                    let include_values = Self::composite_index_payload_values(
                                        payload,
                                        include_indices_for_coverage,
                                        &mut include_payloads_complete,
                                    );
                                    rows.insert(
                                        row_id.clone(),
                                        Self::composite_index_covered_row(
                                            schema,
                                            pk_index,
                                            &row_id,
                                            &index,
                                            &component_values,
                                            include_indices_for_coverage,
                                            include_values,
                                        ),
                                    );
                                }
                            }
                        }
                    }
                    true
                };
                entry_visit_count = self
                    .scan_routed_prefixes_for_each(prefixes, txn, scan_limit, &mut visitor)
                    .await?;
            }
        }
        let ordered_topk_counted = order_direction.filter(|_| can_cover_predicates).is_some()
            && ordered_limit.or(limit).is_some();
        if let (Some(asc), true) = (order_direction, ordered_topk_counted) {
            crate::monitor::inc_index_ordered_topk_scan();
            if !asc {
                crate::monitor::inc_index_ordered_topk_reverse_scan();
            }
            crate::monitor::add_index_ordered_topk_entry_visits(entry_visit_count as u64);
        }
        if let (Some(ordered), Some(asc)) = (&mut ordered_row_ids, order_direction) {
            if !asc && !scan_descending {
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
            ordered_topk_counted,
            covered: covered_rows.and_then(|rows| {
                (!rows.is_empty()).then(|| super::scan::CoveredIndexRows {
                    column_indices: if include_payloads_complete {
                        covered_column_indices.unwrap_or_default()
                    } else {
                        Self::composite_index_covered_column_indices(
                            schema,
                            pk_index,
                            &index,
                            covered_component_count,
                            &[],
                        )
                    },
                    rows,
                })
            }),
        }))
    }

    pub(crate) async fn composite_ordered_index_for_explain(
        &self,
        expr: &Expr,
        table_name: &str,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
        order_by: Option<&sqlparser::ast::OrderBy>,
        ordered_limit: usize,
    ) -> Result<Option<CompositeOrderedIndexAccess>> {
        if self.shard_router.is_some() {
            return Ok(None);
        }

        let indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        if indexes.is_empty() {
            return Ok(None);
        }

        let predicates = Self::collect_conjunctive_predicates(expr);
        let equality_values =
            match self.composite_index_equality_values_by_column_index(&predicates, schema, &[]) {
                Ok(values) => values,
                Err(_) => return Ok(None),
            };

        let mut best: Option<(CompositeIndexMeta, Vec<String>, bool)> = None;
        for index in indexes {
            let mut components = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                let Some(column_idx) = schema.get_column_index(column) else {
                    break;
                };
                let Some(value) = equality_values.get(&column_idx) else {
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
        if !index.ordered_encoding {
            return Ok(None);
        }

        let Some(range_column) = index.columns.get(components.len()) else {
            return Ok(None);
        };
        let range_column_orderable = schema.get_column_index(range_column).is_some_and(|idx| {
            Self::composite_column_type_is_orderable(&schema.columns[idx].data_type)
        });
        if !range_column_orderable {
            return Ok(None);
        }

        let Some(range) =
            (match self.composite_index_range_bounds(&predicates, schema, &[], range_column) {
                Ok(range) => range,
                Err(_) => return Ok(None),
            })
        else {
            return Ok(None);
        };
        let range_predicate_count =
            usize::from(range.lower.is_some()) + usize::from(range.upper.is_some());
        let can_cover_predicates = (all_index_columns_matched
            && predicates.len() == index.columns.len())
            || predicates.len() == components.len().saturating_add(range_predicate_count);
        if !can_cover_predicates {
            return Ok(None);
        }

        let Some(ascending) =
            Self::composite_order_next_column_direction(order_by, Some(range_column.as_str()))
        else {
            return Ok(None);
        };
        if !ascending && !txn.supports_bounded_scan_range_reverse() {
            return Ok(None);
        }

        Ok(Some(CompositeOrderedIndexAccess {
            index_name: index.name,
            order_column: range_column.clone(),
            ascending,
            row_limit: ordered_limit,
        }))
    }

    fn composite_index_equality_values_by_column_index(
        &self,
        predicates: &[Expr],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<HashMap<usize, Value>> {
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

            let Some((column_idx, _, value_expr)) =
                self.equality_schema_column_value_expr(left, right, schema)
            else {
                continue;
            };

            let value = self.evaluate_value(value_expr, &[], schema, params)?;
            if self.value_to_index_string(&value).is_some() {
                values.insert(column_idx, value);
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

    fn starts_with_ascii_case_insensitive(value: &str, prefix: &str) -> bool {
        value
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
    }

    fn composite_column_type_matches_any(data_type: &str, candidates: &[&str]) -> bool {
        candidates
            .iter()
            .any(|candidate| data_type.eq_ignore_ascii_case(candidate))
    }

    fn composite_column_type_is_integer(data_type: &str) -> bool {
        Self::composite_column_type_matches_any(
            data_type,
            &[
                "INT",
                "INT2",
                "INT4",
                "INT8",
                "INTEGER",
                "SMALLINT",
                "BIGINT",
                "TINYINT",
                "MEDIUMINT",
                "SERIAL",
                "SERIAL2",
                "SERIAL4",
                "SERIAL8",
                "SMALLSERIAL",
                "BIGSERIAL",
            ],
        )
    }

    fn composite_column_type_is_orderable(data_type: &str) -> bool {
        Self::composite_column_type_is_integer(data_type)
            || Self::composite_column_type_matches_any(
                data_type,
                &[
                    "BOOL",
                    "BOOLEAN",
                    "DATE",
                    "DATE32",
                    "TIMESTAMP",
                    "TIMESTAMP WITHOUT TIME ZONE",
                    "TIMESTAMP WITH TIME ZONE",
                    "TIMESTAMPTZ",
                    "DATETIME",
                    "INTERVAL",
                ],
            )
            || Self::starts_with_ascii_case_insensitive(data_type, "TIMESTAMP(")
            || Self::starts_with_ascii_case_insensitive(data_type, "DATETIME(")
            || Self::starts_with_ascii_case_insensitive(data_type, "INTERVAL ")
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
        for (key, value) in table_entries {
            if Self::composite_index_table_directory_entry_belongs_to_table(
                table_name, &key, &value,
            ) {
                txn.delete(&key).await?;
            }
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
    use super::{join_composite_index_parts, CompositeIndexMeta, Executor};
    use crate::catalog::{Column, IndexType, TableSchema};
    use crate::common::{FusionError, Result, Value};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::{ScanVisitor, Transaction};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::Mutex;

    fn test_column(name: &str, data_type: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_primary: false,
            is_indexed: false,
            index_type: IndexType::None,
            default_value: None,
            is_nullable: true,
            is_unique: false,
            check_expr: None,
        }
    }

    fn test_executor() -> Executor {
        let wal_path = format!("test_composite_index_{}.wal", uuid::Uuid::new_v4());
        let storage = Arc::new(MemoryStorage::new(&wal_path).unwrap());
        Executor::new(storage)
    }

    struct RecordingCompositeTxn {
        marker_key: Vec<u8>,
        table_meta_prefix: Vec<u8>,
        table_meta_entry: (Vec<u8>, Vec<u8>),
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        supports_reverse: bool,
        range_calls: Arc<Mutex<usize>>,
        reverse_calls: Arc<Mutex<Vec<(Vec<u8>, Vec<u8>, Option<usize>)>>>,
        range_for_each_calls: Arc<Mutex<usize>>,
        reverse_for_each_calls: Arc<Mutex<Vec<(Vec<u8>, Vec<u8>, Option<usize>)>>>,
    }

    impl RecordingCompositeTxn {
        fn new(
            marker_key: Vec<u8>,
            table_meta_prefix: Vec<u8>,
            table_meta_entry: (Vec<u8>, Vec<u8>),
            mut entries: Vec<(Vec<u8>, Vec<u8>)>,
        ) -> Self {
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Self {
                marker_key,
                table_meta_prefix,
                table_meta_entry,
                entries,
                supports_reverse: true,
                range_calls: Arc::new(Mutex::new(0)),
                reverse_calls: Arc::new(Mutex::new(Vec::new())),
                range_for_each_calls: Arc::new(Mutex::new(0)),
                reverse_for_each_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn without_bounded_reverse(mut self) -> Self {
            self.supports_reverse = false;
            self
        }
    }

    #[async_trait]
    impl Transaction for RecordingCompositeTxn {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            if key == self.marker_key.as_slice() {
                Ok(Some(Vec::new()))
            } else {
                Ok(None)
            }
        }

        async fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
            Err(FusionError::Execution(
                "unused recording transaction put".into(),
            ))
        }

        async fn delete(&mut self, _key: &[u8]) -> Result<()> {
            Err(FusionError::Execution(
                "unused recording transaction delete".into(),
            ))
        }

        async fn scan_prefix(
            &self,
            prefix: &[u8],
            _limit: Option<usize>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            if prefix == self.table_meta_prefix.as_slice() {
                Ok(vec![self.table_meta_entry.clone()])
            } else {
                Ok(Vec::new())
            }
        }

        async fn scan_prefix_for_each(
            &self,
            _prefix: &[u8],
            _limit: Option<usize>,
            _visitor: &mut dyn ScanVisitor,
        ) -> Result<usize> {
            Err(FusionError::Execution(
                "unused recording transaction prefix visitor".into(),
            ))
        }

        async fn scan_range(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            *self.range_calls.lock().unwrap() += 1;
            let mut rows = self
                .entries
                .iter()
                .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
                .cloned()
                .collect::<Vec<_>>();
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            Ok(rows)
        }

        async fn scan_range_for_each(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
        ) -> Result<usize> {
            *self.range_for_each_calls.lock().unwrap() += 1;
            let mut visited = 0usize;
            for (key, value) in self
                .entries
                .iter()
                .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
            {
                if limit.is_some_and(|limit| visited >= limit) {
                    break;
                }
                visited += 1;
                if !visitor.visit(key, value) {
                    break;
                }
            }
            Ok(visited)
        }

        fn supports_bounded_scan_range_reverse(&self) -> bool {
            self.supports_reverse
        }

        async fn scan_range_reverse(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.reverse_calls
                .lock()
                .unwrap()
                .push((start.to_vec(), end.to_vec(), limit));
            let mut rows = self
                .entries
                .iter()
                .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
                .cloned()
                .collect::<Vec<_>>();
            rows.reverse();
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            Ok(rows)
        }

        async fn scan_range_reverse_for_each(
            &self,
            start: &[u8],
            end: &[u8],
            limit: Option<usize>,
            visitor: &mut dyn ScanVisitor,
        ) -> Result<usize> {
            self.reverse_for_each_calls
                .lock()
                .unwrap()
                .push((start.to_vec(), end.to_vec(), limit));
            let mut visited = 0usize;
            for (key, value) in self
                .entries
                .iter()
                .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
                .rev()
            {
                if limit.is_some_and(|limit| visited >= limit) {
                    break;
                }
                visited += 1;
                if !visitor.visit(key, value) {
                    break;
                }
            }
            Ok(visited)
        }

        async fn count_prefix(&self, _prefix: &[u8]) -> Result<usize> {
            Err(FusionError::Execution(
                "unused recording transaction count_prefix".into(),
            ))
        }

        async fn first(&self, _start: &[u8], _end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
            Err(FusionError::Execution(
                "unused recording transaction first".into(),
            ))
        }

        async fn last(&self, _start: &[u8], _end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
            Err(FusionError::Execution(
                "unused recording transaction last".into(),
            ))
        }

        async fn commit(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

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
    fn composite_index_components_prefix_preallocates_exact_prefix() {
        let prefix = Executor::composite_index_components_prefix(
            "index:orders:warehouse_id,district_id:",
            "i1|i2",
        );

        assert_eq!(prefix, "index:orders:warehouse_id,district_id:i1|i2");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn composite_index_range_prefix_preallocates_exact_prefix() {
        let prefix =
            Executor::composite_index_range_prefix("index:orders:warehouse_id,district_id:i1|i2");

        assert_eq!(prefix, "index:orders:warehouse_id,district_id:i1|i2|");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn composite_index_range_bound_preallocates_exact_bound() {
        let bound = Executor::composite_index_range_bound(
            "index:orders:warehouse_id,district_id:i1|i2|",
            "i3",
            "\u{0}",
        );

        assert_eq!(
            bound,
            "index:orders:warehouse_id,district_id:i1|i2|i3:\u{0}"
        );
        assert!(bound.capacity() >= bound.len());
    }

    #[test]
    fn composite_index_meta_value_preallocates_exact_value() {
        let columns = vec!["warehouse_id".to_string(), "district_id".to_string()];
        let value = Executor::composite_index_meta_value("stock", &columns);

        assert_eq!(value, "v3:stock:warehouse_id,district_id");
        assert!(value.capacity() >= value.len());
    }

    #[test]
    fn single_column_index_meta_value_with_include_roundtrips() {
        let include_columns = vec!["payload".to_string(), "metric".to_string()];
        let value = Executor::single_column_index_meta_value_with_include(
            "stock",
            "score",
            &include_columns,
        );

        assert_eq!(value, "s3:5:stock1:5:score2:7:payload6:metric");
        let meta = Executor::parse_index_meta("idx_stock_score_cover", &value).unwrap();
        assert_eq!(meta.table, "stock");
        assert_eq!(meta.columns, vec!["score".to_string()]);
        assert_eq!(meta.include_columns, include_columns);
        assert!(!meta.ordered_encoding);
    }

    #[test]
    fn single_column_index_meta_value_with_include_preserves_delimiter_identifiers() {
        let include_columns = vec!["payload:text".to_string(), "metric,value".to_string()];
        let value = Executor::single_column_index_meta_value_with_include(
            "stock:west,1",
            "score:rank",
            &include_columns,
        );

        assert!(value.starts_with("s3:"));
        let meta = Executor::parse_index_meta("idx_stock_score_cover", &value).unwrap();
        assert_eq!(meta.table, "stock:west,1");
        assert_eq!(meta.columns, vec!["score:rank".to_string()]);
        assert_eq!(meta.include_columns, include_columns);
        assert!(!meta.ordered_encoding);
    }

    #[test]
    fn single_column_index_meta_value_with_include_reads_legacy_s2() {
        let meta =
            Executor::parse_index_meta("idx_stock_score_cover", "s2:stock:score:payload,metric")
                .unwrap();

        assert_eq!(meta.table, "stock");
        assert_eq!(meta.columns, vec!["score".to_string()]);
        assert_eq!(
            meta.include_columns,
            vec!["payload".to_string(), "metric".to_string()]
        );
        assert!(!meta.ordered_encoding);
    }

    #[test]
    fn single_column_index_meta_value_with_include_rejects_malformed_s3() {
        for meta in [
            "s3:",
            "s3:5:stock0:1:7:payload",
            "s3:5:stock2:5:score5:other1:7:payload",
            "s3:5:stock1:5:score0:",
            "s3:5:stock1:5:score1:7:payloadjunk",
            "s3:10:stock",
            "s3:5:stock999999999999:5:score1:7:payload",
        ] {
            assert!(
                Executor::parse_index_meta("idx_bad", meta).is_none(),
                "{meta} should be rejected"
            );
        }
    }

    #[test]
    fn composite_index_meta_value_with_include_roundtrips() {
        let columns = vec!["warehouse_id".to_string(), "district_id".to_string()];
        let include_columns = vec!["payload".to_string(), "metric".to_string()];
        let value =
            Executor::composite_index_meta_value_with_include("stock", &columns, &include_columns);

        assert_eq!(
            value,
            "c5:5:stock2:12:warehouse_id11:district_id2:7:payload6:metric"
        );
        let meta = Executor::parse_index_meta("idx_stock_cover", &value).unwrap();
        assert_eq!(meta.table, "stock");
        assert_eq!(meta.columns, columns);
        assert_eq!(meta.include_columns, include_columns);
        assert!(meta.ordered_encoding);
    }

    #[test]
    fn composite_index_meta_value_with_include_preserves_delimiter_identifiers() {
        let columns = vec!["warehouse,id".to_string(), "district:id".to_string()];
        let include_columns = vec!["payload:text".to_string(), "metric,value".to_string()];
        let value = Executor::composite_index_meta_value_with_include(
            "stock:west,1",
            &columns,
            &include_columns,
        );

        assert!(value.starts_with("c5:"));
        let meta = Executor::parse_index_meta("idx_stock_cover", &value).unwrap();
        assert_eq!(meta.table, "stock:west,1");
        assert_eq!(meta.columns, columns);
        assert_eq!(meta.include_columns, include_columns);
        assert!(meta.ordered_encoding);
    }

    #[test]
    fn composite_index_meta_value_with_include_reads_legacy_c4() {
        let meta = Executor::parse_index_meta(
            "idx_stock_cover",
            "c4:stock:warehouse_id,district_id:payload,metric",
        )
        .unwrap();

        assert_eq!(meta.table, "stock");
        assert_eq!(
            meta.columns,
            vec!["warehouse_id".to_string(), "district_id".to_string()]
        );
        assert_eq!(
            meta.include_columns,
            vec!["payload".to_string(), "metric".to_string()]
        );
        assert!(meta.ordered_encoding);
    }

    #[test]
    fn composite_index_meta_value_with_include_rejects_malformed_c5() {
        for meta in [
            "c5:",
            "c5:5:stock0:1:7:payload",
            "c5:5:stock1:12:warehouse_id0:",
            "c5:5:stock1:12:warehouse_id1:7:payloadjunk",
            "c5:10:stock",
            "c5:5:stock999999999999:12:warehouse_id1:7:payload",
        ] {
            assert!(
                Executor::parse_index_meta("idx_bad", meta).is_none(),
                "{meta} should be rejected"
            );
        }
    }

    #[test]
    fn composite_index_table_directory_filter_avoids_colon_prefix_collisions() {
        let table_a_key = Executor::composite_index_table_meta_key("a", "idx_a");
        let table_ab_key = Executor::composite_index_table_meta_key("a:b", "idx_ab");
        let table_a_value = Executor::composite_index_meta_value_with_include(
            "a",
            &["host_id".to_string(), "ts".to_string()],
            &["payload".to_string()],
        );
        let table_ab_value = Executor::composite_index_meta_value_with_include(
            "a:b",
            &["host_id".to_string(), "ts".to_string()],
            &["payload".to_string()],
        );

        assert!(
            Executor::composite_index_table_directory_entry_belongs_to_table(
                "a",
                table_a_key.as_bytes(),
                table_a_value.as_bytes()
            )
        );
        assert!(
            !Executor::composite_index_table_directory_entry_belongs_to_table(
                "a",
                table_ab_key.as_bytes(),
                table_ab_value.as_bytes()
            )
        );
        assert!(
            Executor::composite_index_table_directory_entry_belongs_to_table(
                "a",
                Executor::composite_index_table_marker_key("a").as_bytes(),
                b"v1"
            )
        );
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

    #[test]
    fn join_composite_index_parts_preallocates_exact_parts() {
        let parts = vec!["i1".to_string(), "sYWJj".to_string(), "n".to_string()];
        let joined =
            join_composite_index_parts(&parts, Executor::composite_index_component_separator());

        assert_eq!(joined, "i1|sYWJj|n");
        assert!(joined.capacity() >= joined.len());
    }

    #[test]
    fn composite_index_meta_encoded_columns_preallocates_exact_columns() {
        let meta = CompositeIndexMeta {
            name: "idx_stock_warehouse_district".to_string(),
            table: "stock".to_string(),
            columns: vec!["warehouse_id".to_string(), "district_id".to_string()],
            include_columns: Vec::new(),
            ordered_encoding: true,
        };
        let encoded = meta.encoded_columns();

        assert_eq!(encoded, "warehouse_id,district_id");
        assert!(encoded.capacity() >= encoded.len());
    }

    #[test]
    fn composite_index_equality_values_use_column_indices_without_lowercase_keys() {
        let executor = test_executor();
        let schema = TableSchema::new(
            "orders".to_string(),
            vec![
                test_column("Warehouse_ID", "INTEGER"),
                test_column("Status", "TEXT"),
                test_column("Total", "INTEGER"),
            ],
        );
        let statements = crate::parser::parse_sql(
            "SELECT * FROM orders WHERE warehouse_id = 7 AND STATUS = 'open' AND total > 10",
        )
        .unwrap();
        let statement = statements.into_iter().next().unwrap();
        let selection = match statement {
            sqlparser::ast::Statement::Query(query) => match *query.body {
                sqlparser::ast::SetExpr::Select(select) => select.selection.unwrap(),
                _ => panic!("expected select query"),
            },
            _ => panic!("expected query statement"),
        };
        let predicates = Executor::collect_conjunctive_predicates(&selection);

        let values = executor
            .composite_index_equality_values_by_column_index(&predicates, &schema, &[])
            .unwrap();

        assert_eq!(values.get(&0), Some(&Value::Integer(7)));
        assert_eq!(values.get(&1), Some(&Value::String("open".to_string())));
        assert!(!values.contains_key(&2));
    }

    #[tokio::test]
    async fn composite_desc_ordered_scan_uses_bounded_reverse_range() {
        let executor = test_executor();
        let schema = TableSchema::new(
            "tsbs".to_string(),
            vec![
                test_column("id", "INTEGER"),
                test_column("host_id", "INTEGER"),
                test_column("ts", "INTEGER"),
            ],
        );
        let meta = CompositeIndexMeta {
            name: "idx_tsbs_host_ts".to_string(),
            table: "tsbs".to_string(),
            columns: vec!["host_id".to_string(), "ts".to_string()],
            include_columns: Vec::new(),
            ordered_encoding: true,
        };
        let entries = [1000_i64, 2000, 3000, 4000]
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                let value_key = executor
                    .composite_index_value_key_for_meta_values(
                        &meta,
                        &[Value::Integer(1), Value::Integer(ts)],
                    )
                    .unwrap();
                let row_id = (idx + 1).to_string();
                (
                    executor
                        .routed_composite_index_entry_key(
                            "tsbs",
                            &meta.columns,
                            &value_key,
                            &row_id,
                        )
                        .into_bytes(),
                    Vec::new(),
                )
            })
            .collect::<Vec<_>>();
        let table_meta_key = Executor::composite_index_table_meta_key("tsbs", &meta.name);
        let table_meta_value = Executor::composite_index_meta_value("tsbs", &meta.columns);
        let mut txn = RecordingCompositeTxn::new(
            Executor::composite_index_table_marker_key("tsbs").into_bytes(),
            Executor::composite_index_table_prefix("tsbs").into_bytes(),
            (table_meta_key.into_bytes(), table_meta_value.into_bytes()),
            entries,
        );
        let reverse_calls = txn.reverse_calls.clone();
        let reverse_for_each_calls = txn.reverse_for_each_calls.clone();
        let range_calls = txn.range_calls.clone();

        let statement = crate::parser::parse_sql(
            "SELECT id FROM tsbs WHERE host_id = 1 AND ts >= 0 ORDER BY ts DESC LIMIT 2",
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let (selection, order_by) = match statement {
            sqlparser::ast::Statement::Query(query) => {
                let order_by = query.order_by.clone().unwrap();
                let selection = match *query.body {
                    sqlparser::ast::SetExpr::Select(select) => select.selection.unwrap(),
                    _ => panic!("expected SELECT"),
                };
                (selection, order_by)
            }
            _ => panic!("expected query"),
        };

        let plan = executor
            .try_composite_index_scan(
                &selection,
                "tsbs",
                &schema,
                &mut txn,
                &[],
                Some(2),
                Some(&order_by),
                Some(2),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(*range_calls.lock().unwrap(), 0);
        assert!(reverse_calls.lock().unwrap().is_empty());
        let calls = reverse_for_each_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].2, Some(2));
        assert_eq!(
            plan.ordered_row_ids.unwrap(),
            vec!["4".to_string(), "3".to_string()]
        );
        assert!(plan.exact);
    }

    #[tokio::test]
    async fn composite_ordered_scan_covers_primary_key_and_index_columns() {
        let executor = test_executor();
        let mut id_column = test_column("id", "INTEGER");
        id_column.is_primary = true;
        let schema = TableSchema::new(
            "tsbs".to_string(),
            vec![
                id_column,
                test_column("host_id", "INTEGER"),
                test_column("ts", "INTEGER"),
            ],
        );
        let meta = CompositeIndexMeta {
            name: "idx_tsbs_host_ts".to_string(),
            table: "tsbs".to_string(),
            columns: vec!["host_id".to_string(), "ts".to_string()],
            include_columns: Vec::new(),
            ordered_encoding: true,
        };
        let row_ids = [1_i64, 2, 3, 4]
            .into_iter()
            .zip([1000_i64, 2000, 3000, 4000])
            .map(|(id, ts)| {
                (
                    Executor::value_to_primary_row_id(&Value::Integer(id)).unwrap(),
                    ts,
                )
            })
            .collect::<Vec<_>>();
        let entries = row_ids
            .iter()
            .map(|(row_id, ts)| {
                let value_key = executor
                    .composite_index_value_key_for_meta_values(
                        &meta,
                        &[Value::Integer(1), Value::Integer(*ts)],
                    )
                    .unwrap();
                (
                    executor
                        .routed_composite_index_entry_key(
                            "tsbs",
                            &meta.columns,
                            &value_key,
                            &row_id,
                        )
                        .into_bytes(),
                    Vec::new(),
                )
            })
            .collect::<Vec<_>>();
        let table_meta_key = Executor::composite_index_table_meta_key("tsbs", &meta.name);
        let table_meta_value = Executor::composite_index_meta_value("tsbs", &meta.columns);
        let mut txn = RecordingCompositeTxn::new(
            Executor::composite_index_table_marker_key("tsbs").into_bytes(),
            Executor::composite_index_table_prefix("tsbs").into_bytes(),
            (table_meta_key.into_bytes(), table_meta_value.into_bytes()),
            entries,
        );

        let statement = crate::parser::parse_sql(
            "SELECT id, host_id, ts FROM tsbs WHERE host_id = 1 AND ts >= 0 ORDER BY ts ASC LIMIT 2",
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let (selection, order_by) = match statement {
            sqlparser::ast::Statement::Query(query) => {
                let order_by = query.order_by.clone().unwrap();
                let selection = match *query.body {
                    sqlparser::ast::SetExpr::Select(select) => select.selection.unwrap(),
                    _ => panic!("expected SELECT"),
                };
                (selection, order_by)
            }
            _ => panic!("expected query"),
        };

        let plan = executor
            .try_composite_index_scan(
                &selection,
                "tsbs",
                &schema,
                &mut txn,
                &[],
                Some(2),
                Some(&order_by),
                Some(2),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            plan.ordered_row_ids.as_ref().unwrap(),
            &vec![row_ids[0].0.clone(), row_ids[1].0.clone()]
        );
        assert!(plan.exact);
        let covered = plan.covered.unwrap();
        assert_eq!(covered.column_indices, vec![0, 1, 2]);
        assert_eq!(
            covered.rows.get(&row_ids[0].0).unwrap(),
            &vec![Value::Integer(1), Value::Integer(1), Value::Integer(1000)]
        );
        assert_eq!(
            covered.rows.get(&row_ids[1].0).unwrap(),
            &vec![Value::Integer(2), Value::Integer(1), Value::Integer(2000)]
        );
    }

    #[tokio::test]
    async fn composite_include_ordered_scan_covers_payload_columns() {
        let executor = test_executor();
        let mut id_column = test_column("id", "INTEGER");
        id_column.is_primary = true;
        let schema = TableSchema::new(
            "tsbs".to_string(),
            vec![
                id_column,
                test_column("host_id", "INTEGER"),
                test_column("ts", "INTEGER"),
                test_column("payload", "TEXT"),
                test_column("metric", "INTEGER"),
            ],
        );
        let meta = CompositeIndexMeta {
            name: "idx_tsbs_host_ts_cover".to_string(),
            table: "tsbs".to_string(),
            columns: vec!["host_id".to_string(), "ts".to_string()],
            include_columns: vec!["payload".to_string(), "metric".to_string()],
            ordered_encoding: true,
        };
        let include_indices = vec![3, 4];
        let rows = [
            (
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(1000),
                Value::String("alpha".to_string()),
                Value::Integer(11),
            ),
            (
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2000),
                Value::String("beta".to_string()),
                Value::Integer(22),
            ),
        ];
        let row_ids = rows
            .iter()
            .map(|row| Executor::value_to_primary_row_id(&row.0).unwrap())
            .collect::<Vec<_>>();
        let entries = rows
            .iter()
            .zip(&row_ids)
            .map(|(row, row_id)| {
                let full_row = vec![
                    row.0.clone(),
                    row.1.clone(),
                    row.2.clone(),
                    row.3.clone(),
                    row.4.clone(),
                ];
                let value_key = executor
                    .composite_index_value_key_for_meta_values(
                        &meta,
                        &[row.1.clone(), row.2.clone()],
                    )
                    .unwrap();
                let payload =
                    Executor::secondary_index_payload_for_row(&full_row, &include_indices);
                (
                    executor
                        .routed_composite_index_entry_key("tsbs", &meta.columns, &value_key, row_id)
                        .into_bytes(),
                    payload,
                )
            })
            .collect::<Vec<_>>();
        let table_meta_key = Executor::composite_index_table_meta_key("tsbs", &meta.name);
        let table_meta_value = Executor::composite_index_meta_value_with_include(
            "tsbs",
            &meta.columns,
            &meta.include_columns,
        );
        let mut txn = RecordingCompositeTxn::new(
            Executor::composite_index_table_marker_key("tsbs").into_bytes(),
            Executor::composite_index_table_prefix("tsbs").into_bytes(),
            (table_meta_key.into_bytes(), table_meta_value.into_bytes()),
            entries,
        );

        let statement = crate::parser::parse_sql(
            "SELECT id, host_id, ts, payload, metric FROM tsbs WHERE host_id = 1 AND ts >= 0 ORDER BY ts ASC LIMIT 2",
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let (selection, order_by) = match statement {
            sqlparser::ast::Statement::Query(query) => {
                let order_by = query.order_by.clone().unwrap();
                let selection = match *query.body {
                    sqlparser::ast::SetExpr::Select(select) => select.selection.unwrap(),
                    _ => panic!("expected SELECT"),
                };
                (selection, order_by)
            }
            _ => panic!("expected query"),
        };

        let plan = executor
            .try_composite_index_scan(
                &selection,
                "tsbs",
                &schema,
                &mut txn,
                &[],
                Some(2),
                Some(&order_by),
                Some(2),
            )
            .await
            .unwrap()
            .unwrap();

        assert!(plan.exact);
        let covered = plan.covered.unwrap();
        assert_eq!(covered.column_indices, vec![0, 1, 2, 3, 4]);
        assert_eq!(
            covered.rows.get(&row_ids[0]).unwrap(),
            &vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Integer(1000),
                Value::String("alpha".to_string()),
                Value::Integer(11),
            ]
        );
        assert_eq!(
            covered.rows.get(&row_ids[1]).unwrap(),
            &vec![
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2000),
                Value::String("beta".to_string()),
                Value::Integer(22),
            ]
        );
    }

    #[tokio::test]
    async fn composite_desc_ordered_scan_requires_bounded_reverse_capability() {
        let executor = test_executor();
        let schema = TableSchema::new(
            "tsbs".to_string(),
            vec![
                test_column("id", "INTEGER"),
                test_column("host_id", "INTEGER"),
                test_column("ts", "INTEGER"),
            ],
        );
        let meta = CompositeIndexMeta {
            name: "idx_tsbs_host_ts".to_string(),
            table: "tsbs".to_string(),
            columns: vec!["host_id".to_string(), "ts".to_string()],
            include_columns: Vec::new(),
            ordered_encoding: true,
        };
        let entries = [1000_i64, 2000, 3000, 4000]
            .into_iter()
            .enumerate()
            .map(|(idx, ts)| {
                let value_key = executor
                    .composite_index_value_key_for_meta_values(
                        &meta,
                        &[Value::Integer(1), Value::Integer(ts)],
                    )
                    .unwrap();
                let row_id = (idx + 1).to_string();
                (
                    executor
                        .routed_composite_index_entry_key(
                            "tsbs",
                            &meta.columns,
                            &value_key,
                            &row_id,
                        )
                        .into_bytes(),
                    Vec::new(),
                )
            })
            .collect::<Vec<_>>();
        let table_meta_key = Executor::composite_index_table_meta_key("tsbs", &meta.name);
        let table_meta_value = Executor::composite_index_meta_value("tsbs", &meta.columns);
        let mut txn = RecordingCompositeTxn::new(
            Executor::composite_index_table_marker_key("tsbs").into_bytes(),
            Executor::composite_index_table_prefix("tsbs").into_bytes(),
            (table_meta_key.into_bytes(), table_meta_value.into_bytes()),
            entries,
        )
        .without_bounded_reverse();
        let reverse_calls = txn.reverse_calls.clone();
        let range_calls = txn.range_calls.clone();
        let range_for_each_calls = txn.range_for_each_calls.clone();
        let reverse_for_each_calls = txn.reverse_for_each_calls.clone();

        let statement = crate::parser::parse_sql(
            "SELECT id FROM tsbs WHERE host_id = 1 AND ts >= 0 ORDER BY ts DESC LIMIT 2",
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let (selection, order_by) = match statement {
            sqlparser::ast::Statement::Query(query) => {
                let order_by = query.order_by.clone().unwrap();
                let selection = match *query.body {
                    sqlparser::ast::SetExpr::Select(select) => select.selection.unwrap(),
                    _ => panic!("expected SELECT"),
                };
                (selection, order_by)
            }
            _ => panic!("expected query"),
        };

        let plan = executor
            .try_composite_index_scan(
                &selection,
                "tsbs",
                &schema,
                &mut txn,
                &[],
                Some(2),
                Some(&order_by),
                Some(2),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(*range_calls.lock().unwrap(), 0);
        assert_eq!(*range_for_each_calls.lock().unwrap(), 1);
        assert!(reverse_calls.lock().unwrap().is_empty());
        assert!(reverse_for_each_calls.lock().unwrap().is_empty());
        assert!(plan.ordered_row_ids.is_none());
        assert!(plan.exact);
    }

    #[test]
    fn composite_column_type_orderable_matches_case_without_uppercase_allocation() {
        for data_type in [
            "iNt4",
            "bigSerial",
            "BoOlEaN",
            "date32",
            "timeStamp",
            "timestamp without time zone",
            "TIMESTAMP WITH TIME ZONE",
            "timestamp(6)",
            "dateTime(3)",
            "interval day",
        ] {
            assert!(
                Executor::composite_column_type_is_orderable(data_type),
                "{data_type} should be orderable"
            );
        }
    }

    #[test]
    fn composite_column_type_orderable_rejects_non_orderable_names() {
        for data_type in [
            "TEXT",
            "varchar(20)",
            "timestampz",
            "datetimeoffset",
            "intervals",
            " TIMESTAMP",
        ] {
            assert!(
                !Executor::composite_column_type_is_orderable(data_type),
                "{data_type} should not be orderable"
            );
        }
    }
}
