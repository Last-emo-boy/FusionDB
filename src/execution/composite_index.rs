use crate::catalog::TableSchema;
use crate::common::{Result, Value};
use crate::storage::Transaction;
use base64::Engine;
use sqlparser::ast::{BinaryOperator, Expr};
use std::collections::HashMap;

use super::Executor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositeIndexMeta {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

impl CompositeIndexMeta {
    fn encoded_columns(&self) -> String {
        self.columns.join(",")
    }
}

impl Executor {
    pub(crate) fn composite_index_meta_value(table: &str, columns: &[String]) -> String {
        format!("v2:{}:{}", table, columns.join(","))
    }

    pub(crate) fn parse_index_meta(index_name: &str, meta_str: &str) -> Option<CompositeIndexMeta> {
        if let Some(rest) = meta_str.strip_prefix("v2:") {
            let (table, columns) = rest.split_once(':')?;
            let columns = columns
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();

            if table.is_empty() || columns.is_empty() {
                return None;
            }

            Some(CompositeIndexMeta {
                name: index_name.to_string(),
                table: table.to_string(),
                columns,
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
            })
        }
    }

    pub(crate) async fn load_composite_indexes_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<CompositeIndexMeta>> {
        let entries = txn.scan_prefix(b"index_meta:", None).await?;
        let mut indexes = Vec::new();

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

    pub(crate) fn composite_index_prefix(table_name: &str, columns: &[String]) -> String {
        format!("index:{}:{}:", table_name, columns.join(","))
    }

    pub(crate) fn composite_index_key(
        &self,
        table_name: &str,
        columns: &[String],
        row: &[Value],
        schema: &TableSchema,
        row_id: &str,
    ) -> Option<String> {
        let value_key = self.composite_index_value_key(columns, row, schema)?;
        Some(format!(
            "{}{}:{}",
            Self::composite_index_prefix(table_name, columns),
            value_key,
            row_id
        ))
    }

    fn composite_index_value_key(
        &self,
        columns: &[String],
        row: &[Value],
        schema: &TableSchema,
    ) -> Option<String> {
        let mut parts = Vec::with_capacity(columns.len());

        for column in columns {
            let idx = schema.get_column_index(column)?;
            let value = row.get(idx)?;
            parts.push(self.encoded_index_component(value)?);
        }

        Some(parts.join("|"))
    }

    fn encoded_index_component(&self, value: &Value) -> Option<String> {
        let raw = self.value_to_index_string(value)?;
        Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes()))
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
                self.composite_index_key(table_name, &index.columns, row, schema, row_id)
            {
                txn.put(index_key.as_bytes(), &[]).await?;
            }
        }
        Ok(())
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
                self.composite_index_key(table_name, &index.columns, row, schema, row_id)
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
                self.composite_index_key(table_name, &index.columns, old_row, schema, row_id)
            {
                txn.delete(old_key.as_bytes()).await?;
            }

            if let Some(new_key) =
                self.composite_index_key(table_name, &index.columns, new_row, schema, row_id)
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
    ) -> Result<Option<super::scan::IndexScanPlan>> {
        let indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        if indexes.is_empty() {
            return Ok(None);
        }

        let predicates = Self::collect_conjunctive_predicates(expr);
        let equality_values = self.composite_index_equality_values(&predicates, schema, params)?;

        let mut best: Option<(CompositeIndexMeta, String)> = None;
        for index in indexes {
            if !index
                .columns
                .iter()
                .all(|column| equality_values.contains_key(&column.to_ascii_lowercase()))
            {
                continue;
            }

            let mut components = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                let Some(value) = equality_values.get(&column.to_ascii_lowercase()) else {
                    continue;
                };
                let Some(component) = self.encoded_index_component(value) else {
                    continue;
                };
                components.push(component);
            }

            if components.len() != index.columns.len() {
                continue;
            }

            let value_key = components.join("|");
            if best
                .as_ref()
                .is_none_or(|(current, _)| index.columns.len() > current.columns.len())
            {
                best = Some((index, value_key));
            }
        }

        let Some((index, value_key)) = best else {
            return Ok(None);
        };

        let index_prefix = format!(
            "{}{}:",
            Self::composite_index_prefix(table_name, &index.columns),
            value_key
        );
        let index_entries = txn.scan_prefix(index_prefix.as_bytes(), limit).await?;

        let mut row_ids = std::collections::HashSet::with_capacity(index_entries.len());
        for (key, _) in index_entries {
            if let Some(row_id) = Self::row_id_from_key(&key) {
                row_ids.insert(row_id.to_string());
            }
        }

        Ok(Some(super::scan::IndexScanPlan {
            row_ids,
            exact: predicates.len() == index.columns.len(),
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
        Ok(())
    }

    pub(crate) fn describe_index_columns(meta_str: &str) -> Option<(String, String, Vec<String>)> {
        let meta = Self::parse_index_meta("", meta_str)?;
        let table = meta.table.clone();
        let encoded_columns = meta.encoded_columns();
        Some((table, encoded_columns, meta.columns))
    }
}
