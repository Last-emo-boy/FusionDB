use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnOption, ColumnOptionDef, ForeignKeyConstraint, Ident, TableConstraint};

use super::Executor;

fn foreign_key_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
    let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
    key.push_str("data:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(row_id);
    key
}

fn foreign_key_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ForeignKeyMeta {
    pub name: String,
    pub child_table: String,
    #[serde(default)]
    pub child_column: String,
    #[serde(default)]
    pub child_columns: Vec<String>,
    pub parent_table: String,
    #[serde(default)]
    pub parent_column: String,
    #[serde(default)]
    pub parent_columns: Vec<String>,
}

impl ForeignKeyMeta {
    fn new(
        name: String,
        child_table: String,
        child_columns: Vec<String>,
        parent_table: String,
        parent_columns: Vec<String>,
    ) -> Self {
        let child_column = child_columns.first().cloned().unwrap_or_default();
        let parent_column = parent_columns.first().cloned().unwrap_or_default();
        Self {
            name,
            child_table,
            child_column,
            child_columns,
            parent_table,
            parent_column,
            parent_columns,
        }
    }

    pub(crate) fn child_columns(&self) -> Vec<String> {
        if self.child_columns.is_empty() {
            vec![self.child_column.clone()]
        } else {
            self.child_columns.clone()
        }
    }

    pub(crate) fn parent_columns(&self) -> Vec<String> {
        if self.parent_columns.is_empty() {
            vec![self.parent_column.clone()]
        } else {
            self.parent_columns.clone()
        }
    }
}

impl Executor {
    pub(crate) fn collect_foreign_keys(
        table_name: &str,
        columns: &[sqlparser::ast::ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Vec<ForeignKeyMeta>> {
        let mut metas = Vec::with_capacity(columns.len().saturating_add(constraints.len()));

        for column in columns {
            for option in &column.options {
                if let ColumnOption::ForeignKey(fk) = &option.option {
                    metas.push(Self::foreign_key_meta_from_column_option(
                        table_name,
                        &column.name,
                        option,
                        fk,
                    )?);
                }
            }
        }

        for constraint in constraints {
            if let TableConstraint::ForeignKey(fk) = constraint {
                metas.push(Self::foreign_key_meta_from_table_constraint(
                    table_name, fk,
                )?);
            }
        }

        Ok(metas)
    }

    fn foreign_key_meta_from_column_option(
        table_name: &str,
        child_column: &Ident,
        option: &ColumnOptionDef,
        fk: &ForeignKeyConstraint,
    ) -> Result<ForeignKeyMeta> {
        if fk.referred_columns.len() > 1 {
            return Err(FusionError::Execution(
                "Composite foreign keys are not supported yet".to_string(),
            ));
        }
        let parent_columns = fk
            .referred_columns
            .first()
            .map(|ident| vec![ident.value.clone()])
            .unwrap_or_else(|| vec!["id".to_string()]);

        Ok(ForeignKeyMeta::new(
            Self::foreign_key_name(
                option.name.as_ref(),
                table_name,
                &[child_column.value.clone()],
                &fk.foreign_table.to_string(),
            ),
            table_name.to_string(),
            vec![child_column.value.clone()],
            fk.foreign_table.to_string(),
            parent_columns,
        ))
    }

    fn foreign_key_meta_from_table_constraint(
        table_name: &str,
        fk: &ForeignKeyConstraint,
    ) -> Result<ForeignKeyMeta> {
        if fk.columns.is_empty() {
            return Err(FusionError::Execution(
                "FOREIGN KEY requires at least one child column".to_string(),
            ));
        }
        let mut child_columns = Vec::with_capacity(fk.columns.len());
        for ident in &fk.columns {
            child_columns.push(ident.value.clone());
        }
        let parent_columns = if fk.referred_columns.is_empty() {
            vec!["id".to_string()]
        } else {
            let mut parent_columns = Vec::with_capacity(fk.referred_columns.len());
            for ident in &fk.referred_columns {
                parent_columns.push(ident.value.clone());
            }
            parent_columns
        };
        if child_columns.len() != parent_columns.len() {
            return Err(FusionError::Execution(format!(
                "FOREIGN KEY column count mismatch: {} child column(s), {} parent column(s)",
                child_columns.len(),
                parent_columns.len()
            )));
        }

        Ok(ForeignKeyMeta::new(
            Self::foreign_key_name(
                fk.name.as_ref(),
                table_name,
                &child_columns,
                &fk.foreign_table.to_string(),
            ),
            table_name.to_string(),
            child_columns,
            fk.foreign_table.to_string(),
            parent_columns,
        ))
    }

    fn foreign_key_name(
        explicit: Option<&Ident>,
        child_table: &str,
        child_columns: &[String],
        parent_table: &str,
    ) -> String {
        explicit
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| {
                format!(
                    "fk_{}_{}_{}",
                    child_table,
                    child_columns.join("_"),
                    parent_table
                )
            })
    }

    pub(crate) async fn store_foreign_keys(
        &self,
        table_name: &str,
        foreign_keys: &[ForeignKeyMeta],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for fk in foreign_keys {
            let child_key = format!("fk_meta:child:{}:{}", table_name, fk.name);
            let parent_key = format!("fk_meta:parent:{}:{}", fk.parent_table, fk.name);
            let bytes = bincode::serialize(fk)
                .map_err(|e| FusionError::Execution(format!("FK serialization error: {}", e)))?;
            txn.put(child_key.as_bytes(), &bytes).await?;
            txn.put(parent_key.as_bytes(), &bytes).await?;
        }
        Ok(())
    }

    pub(crate) async fn load_child_foreign_keys(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<ForeignKeyMeta>> {
        self.load_foreign_keys_by_prefix(&format!("fk_meta:child:{}:", table_name), txn)
            .await
    }

    pub(crate) async fn load_parent_foreign_keys(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<ForeignKeyMeta>> {
        self.load_foreign_keys_by_prefix(&format!("fk_meta:parent:{}:", table_name), txn)
            .await
    }

    async fn load_foreign_keys_by_prefix(
        &self,
        prefix: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<ForeignKeyMeta>> {
        let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
        let mut foreign_keys = Vec::with_capacity(entries.len());
        for (_, value) in entries {
            let fk = bincode::deserialize::<ForeignKeyMeta>(&value)
                .map_err(|e| FusionError::Execution(format!("FK deserialization error: {}", e)))?;
            foreign_keys.push(fk);
        }
        Ok(foreign_keys)
    }

    pub(crate) async fn delete_foreign_keys_for_table(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for prefix in [
            format!("fk_meta:child:{}:", table_name),
            format!("fk_meta:parent:{}:", table_name),
        ] {
            let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
            for (key, _) in entries {
                txn.delete(&key).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn validate_child_foreign_keys(
        &self,
        _table_name: &str,
        schema: &TableSchema,
        row: &[Value],
        foreign_keys: &[ForeignKeyMeta],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for fk in foreign_keys {
            let child_columns = fk.child_columns();
            let mut child_values = Vec::with_capacity(child_columns.len());
            for child_column in &child_columns {
                let Some(child_idx) = schema.get_column_index(child_column) else {
                    continue;
                };
                let Some(value) = row.get(child_idx) else {
                    continue;
                };
                child_values.push(value.clone());
            }
            if child_values.iter().any(|value| *value == Value::Null) {
                continue;
            }

            if !self
                .foreign_key_parent_exists(
                    &fk.parent_table,
                    &fk.parent_columns(),
                    &child_values,
                    txn,
                )
                .await?
            {
                return Err(FusionError::Execution(format!(
                    "FOREIGN KEY constraint '{}' violated: {}.{} references missing {}.{}",
                    fk.name,
                    fk.child_table,
                    child_columns.join(","),
                    fk.parent_table,
                    fk.parent_columns().join(",")
                )));
            }
        }

        Ok(())
    }

    pub(crate) async fn validate_parent_foreign_key_references(
        &self,
        table_name: &str,
        schema: &TableSchema,
        old_row: &[Value],
        new_row: Option<&[Value]>,
        foreign_keys: &[ForeignKeyMeta],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for fk in foreign_keys {
            let parent_columns = fk.parent_columns();
            let mut old_values = Vec::with_capacity(parent_columns.len());
            let mut new_values = Vec::with_capacity(parent_columns.len());
            for parent_column in &parent_columns {
                let Some(parent_idx) = schema.get_column_index(parent_column) else {
                    continue;
                };
                let Some(old_value) = old_row.get(parent_idx) else {
                    continue;
                };
                old_values.push(old_value.clone());
                if let Some(new_row) = new_row {
                    if let Some(new_value) = new_row.get(parent_idx) {
                        new_values.push(new_value.clone());
                    }
                }
            }
            if old_values.iter().any(|value| *value == Value::Null) {
                continue;
            }

            if new_row.is_some() {
                if new_values == old_values {
                    continue;
                }
            }

            if self
                .foreign_key_child_exists(&fk.child_table, &fk.child_columns(), &old_values, txn)
                .await?
            {
                return Err(FusionError::Execution(format!(
                    "FOREIGN KEY constraint '{}' violated: {}.{} is still referenced by {}.{}",
                    fk.name,
                    table_name,
                    parent_columns.join(","),
                    fk.child_table,
                    fk.child_columns().join(",")
                )));
            }
        }

        Ok(())
    }

    async fn foreign_key_parent_exists(
        &self,
        parent_table: &str,
        parent_columns: &[String],
        values: &[Value],
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        if parent_columns.len() != values.len() {
            return Ok(false);
        }
        let schema = self.load_table_schema(parent_table, txn).await?;
        for parent_column in parent_columns {
            if schema.get_column_index(parent_column).is_none() {
                return Err(FusionError::Execution(format!(
                    "Referenced column {}.{} not found",
                    parent_table, parent_column
                )));
            }
        }

        if parent_columns.len() == 1 {
            let parent_idx = schema.get_column_index(&parent_columns[0]).unwrap();
            if schema.get_primary_key_index() == Some(parent_idx) {
                if let Some(row_id) = Self::value_to_primary_row_id(&values[0]) {
                    let key = foreign_key_data_key_for_row_id(parent_table, &row_id);
                    return Ok(txn.get(key.as_bytes()).await?.is_some());
                }
                return Ok(false);
            }
            return self
                .table_has_column_values(parent_table, &schema, parent_columns, values, txn)
                .await;
        }

        for index in self
            .load_composite_unique_indexes_for_table(parent_table, txn)
            .await?
        {
            if !index.name.ends_with("_pkey")
                || !Self::columns_match_ignore_case(&index.columns, parent_columns)
            {
                continue;
            }
            if let Some(row_id) = self.composite_index_value_key_for_meta_values(&index, values) {
                let key = foreign_key_data_key_for_row_id(parent_table, &row_id);
                if txn.get(key.as_bytes()).await?.is_some() {
                    return Ok(true);
                }
            }
        }

        for index in self
            .load_composite_unique_indexes_for_table(parent_table, txn)
            .await?
        {
            if !Self::columns_match_ignore_case(&index.columns, parent_columns) {
                continue;
            }
            if let Some(value_key) = self.composite_index_value_key_for_meta_values(&index, values)
            {
                let prefix = format!(
                    "{}{}:",
                    Self::composite_index_prefix(parent_table, &index.columns),
                    value_key
                );
                if !txn
                    .scan_prefix(prefix.as_bytes(), Some(1))
                    .await?
                    .is_empty()
                {
                    return Ok(true);
                }
            }
        }

        self.table_has_column_values(parent_table, &schema, parent_columns, values, txn)
            .await
    }

    async fn foreign_key_child_exists(
        &self,
        child_table: &str,
        child_columns: &[String],
        values: &[Value],
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        let schema = self.load_table_schema(child_table, txn).await?;

        self.table_has_column_values(child_table, &schema, child_columns, values, txn)
            .await
    }

    async fn table_has_column_values(
        &self,
        table_name: &str,
        schema: &TableSchema,
        columns: &[String],
        values: &[Value],
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        if columns.len() != values.len() {
            return Ok(false);
        }
        let mut column_indexes = Vec::with_capacity(columns.len());
        for column in columns {
            let Some(column_idx) = schema.get_column_index(column) else {
                return Ok(false);
            };
            column_indexes.push(column_idx);
        }
        let prefix = foreign_key_data_prefix_for_table(table_name);
        let rows = txn.scan_prefix(prefix.as_bytes(), None).await?;
        for (key, bytes) in rows {
            let row = if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(row) = self.row_cache.get(key_str) {
                    row
                } else {
                    crate::common::encoding::RowDecoder::decode(&bytes)
                        .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
                }
            } else {
                crate::common::encoding::RowDecoder::decode(&bytes)
                    .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
            };

            if column_indexes
                .iter()
                .zip(values)
                .all(|(idx, expected)| row.get(*idx) == Some(expected))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn columns_match_ignore_case(left: &[String], right: &[String]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    }

    async fn load_table_schema(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<TableSchema> {
        let schema_key = format!("schema:{}", table_name);
        let schema_bytes = txn.get(schema_key.as_bytes()).await?.ok_or_else(|| {
            FusionError::Execution(format!("Referenced table {} not found", table_name))
        })?;
        bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::{foreign_key_data_key_for_row_id, foreign_key_data_prefix_for_table};

    #[test]
    fn foreign_key_data_key_for_row_id_preallocates_exact_key() {
        let key = foreign_key_data_key_for_row_id("warehouse", "0007");

        assert_eq!(key, "data:warehouse:0007");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn foreign_key_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = foreign_key_data_prefix_for_table("warehouse");

        assert_eq!(prefix, "data:warehouse:");
        assert!(prefix.capacity() >= prefix.len());
    }
}
