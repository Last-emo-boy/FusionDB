use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnOption, ColumnOptionDef, ForeignKeyConstraint, Ident, TableConstraint};

use super::Executor;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ForeignKeyMeta {
    pub name: String,
    pub child_table: String,
    pub child_column: String,
    pub parent_table: String,
    pub parent_column: String,
}

impl Executor {
    pub(crate) fn collect_foreign_keys(
        table_name: &str,
        columns: &[sqlparser::ast::ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Vec<ForeignKeyMeta>> {
        let mut metas = Vec::new();

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

        Ok(ForeignKeyMeta {
            name: Self::foreign_key_name(
                option.name.as_ref(),
                table_name,
                &[child_column.value.clone()],
                &fk.foreign_table.to_string(),
            ),
            child_table: table_name.to_string(),
            child_column: child_column.value.clone(),
            parent_table: fk.foreign_table.to_string(),
            parent_column: fk
                .referred_columns
                .first()
                .map(|ident| ident.value.clone())
                .unwrap_or_else(|| "id".to_string()),
        })
    }

    fn foreign_key_meta_from_table_constraint(
        table_name: &str,
        fk: &ForeignKeyConstraint,
    ) -> Result<ForeignKeyMeta> {
        if fk.columns.len() != 1 || fk.referred_columns.len() > 1 {
            return Err(FusionError::Execution(
                "Only single-column foreign keys are supported yet".to_string(),
            ));
        }

        Ok(ForeignKeyMeta {
            name: Self::foreign_key_name(
                fk.name.as_ref(),
                table_name,
                &[fk.columns[0].value.clone()],
                &fk.foreign_table.to_string(),
            ),
            child_table: table_name.to_string(),
            child_column: fk.columns[0].value.clone(),
            parent_table: fk.foreign_table.to_string(),
            parent_column: fk
                .referred_columns
                .first()
                .map(|ident| ident.value.clone())
                .unwrap_or_else(|| "id".to_string()),
        })
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
            let Some(child_idx) = schema.get_column_index(&fk.child_column) else {
                continue;
            };
            let Some(value) = row.get(child_idx) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }

            if !self
                .foreign_key_parent_exists(&fk.parent_table, &fk.parent_column, value, txn)
                .await?
            {
                return Err(FusionError::Execution(format!(
                    "FOREIGN KEY constraint '{}' violated: {}.{} references missing {}.{}",
                    fk.name, fk.child_table, fk.child_column, fk.parent_table, fk.parent_column
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
            let Some(parent_idx) = schema.get_column_index(&fk.parent_column) else {
                continue;
            };
            let Some(old_value) = old_row.get(parent_idx) else {
                continue;
            };
            if *old_value == Value::Null {
                continue;
            }

            if let Some(new_row) = new_row {
                if new_row.get(parent_idx) == Some(old_value) {
                    continue;
                }
            }

            if self
                .foreign_key_child_exists(&fk.child_table, &fk.child_column, old_value, txn)
                .await?
            {
                return Err(FusionError::Execution(format!(
                    "FOREIGN KEY constraint '{}' violated: {}.{} is still referenced by {}.{}",
                    fk.name, table_name, fk.parent_column, fk.child_table, fk.child_column
                )));
            }
        }

        Ok(())
    }

    async fn foreign_key_parent_exists(
        &self,
        parent_table: &str,
        parent_column: &str,
        value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        let schema = self.load_table_schema(parent_table, txn).await?;
        let Some(parent_idx) = schema.get_column_index(parent_column) else {
            return Err(FusionError::Execution(format!(
                "Referenced column {}.{} not found",
                parent_table, parent_column
            )));
        };

        if schema.get_primary_key_index() == Some(parent_idx) {
            if let Some(row_id) = Self::value_to_primary_row_id(value) {
                let key = format!("data:{}:{}", parent_table, row_id);
                return Ok(txn.get(key.as_bytes()).await?.is_some());
            }
            return Ok(false);
        }

        self.table_has_column_value(parent_table, parent_idx, value, txn)
            .await
    }

    async fn foreign_key_child_exists(
        &self,
        child_table: &str,
        child_column: &str,
        value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        let schema = self.load_table_schema(child_table, txn).await?;
        let Some(child_idx) = schema.get_column_index(child_column) else {
            return Ok(false);
        };

        self.table_has_column_value(child_table, child_idx, value, txn)
            .await
    }

    async fn table_has_column_value(
        &self,
        table_name: &str,
        column_idx: usize,
        value: &Value,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        let prefix = format!("data:{}:", table_name);
        let rows = txn.scan_prefix(prefix.as_bytes(), None).await?;
        for (key, bytes) in rows {
            let current = if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(row) = self.row_cache.get(key_str) {
                    row.get(column_idx).cloned().unwrap_or(Value::Null)
                } else {
                    crate::common::encoding::RowDecoder::decode_column(&bytes, column_idx)
                        .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
                        .unwrap_or(Value::Null)
                }
            } else {
                crate::common::encoding::RowDecoder::decode_column(&bytes, column_idx)
                    .map_err(|e| FusionError::Execution(format!("Decode error: {}", e)))?
                    .unwrap_or(Value::Null)
            };

            if current == *value {
                return Ok(true);
            }
        }
        Ok(false)
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
