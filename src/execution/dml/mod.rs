use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use sqlparser::ast::{BinaryOperator, Expr, TableFactor};
use std::collections::HashSet;

use super::Executor;

mod constraints;
mod delete;
mod insert;
mod returning;
mod update;

impl Executor {
    pub(crate) fn indexed_trigram_text_columns(schema: &TableSchema) -> Vec<usize> {
        let mut indices = Vec::with_capacity(schema.columns.len());
        for (idx, col) in schema.columns.iter().enumerate() {
            let upper = col.data_type.trim().to_ascii_uppercase();
            let text_type = upper == "TEXT"
                || upper == "STRING"
                || upper == "VARCHAR"
                || upper == "CHAR"
                || upper.starts_with("VARCHAR(")
                || upper.starts_with("CHAR(")
                || upper.starts_with("CHARACTER");
            if text_type
                && col.is_indexed
                && matches!(col.index_type, IndexType::BTree | IndexType::FTS)
            {
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

        let storage = &ftxn.storage;
        let mut idx_lock = storage.trigram_index.write().unwrap();
        let numeric_id = Self::trigram_numeric_row_id(row_id);

        for &idx in trigram_column_indices {
            if let Some(Value::String(text)) = row_values.get(idx) {
                idx_lock.add_with_id_str(
                    table_name,
                    &schema.columns[idx].name,
                    numeric_id,
                    row_id,
                    text,
                );
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

        let storage = &ftxn.storage;
        let mut idx_lock = storage.trigram_index.write().unwrap();
        idx_lock.add_with_id_str(
            table_name,
            column_name,
            Self::trigram_numeric_row_id(row_id),
            row_id,
            text,
        );
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

        let storage = &ftxn.storage;
        let mut idx_lock = storage.trigram_index.write().unwrap();
        let numeric_id = Self::trigram_numeric_row_id(row_id);

        for &idx in trigram_column_indices {
            let old_val = old_row.get(idx);
            let new_val = new_row.get(idx);
            if old_val == new_val {
                continue;
            }

            if let Some(Value::String(text)) = old_val {
                idx_lock.remove_with_id(table_name, &schema.columns[idx].name, numeric_id, text);
            }
            if let Some(Value::String(text)) = new_val {
                idx_lock.add_with_id_str(
                    table_name,
                    &schema.columns[idx].name,
                    numeric_id,
                    row_id,
                    text,
                );
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

        let storage = &ftxn.storage;
        let mut idx_lock = storage.trigram_index.write().unwrap();
        let numeric_id = Self::trigram_numeric_row_id(row_id);

        for &idx in trigram_column_indices {
            if let Some(Value::String(text)) = row_values.get(idx) {
                idx_lock.remove_with_id(table_name, &schema.columns[idx].name, numeric_id, text);
            }
        }
    }

    pub(crate) fn trigram_numeric_row_id(row_id: &str) -> u64 {
        if let Some(n) = crate::common::encoding::decode_i64_comparable(row_id) {
            n as u64
        } else if let Ok(n) = row_id.parse::<u64>() {
            n
        } else {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            row_id.hash(&mut hasher);
            hasher.finish()
        }
    }

    pub(super) fn row_id_from_data_key(key: &[u8]) -> Result<&str> {
        std::str::from_utf8(key)
            .ok()
            .and_then(|key| key.rsplit(':').next())
            .filter(|row_id| !row_id.is_empty())
            .ok_or_else(|| FusionError::Execution("Invalid data key".to_string()))
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

        let mut value_columns = HashSet::new();
        self.extract_columns_from_expr(value_expr, &mut value_columns);
        if !value_columns.is_empty() {
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
