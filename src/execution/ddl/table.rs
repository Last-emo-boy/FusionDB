use crate::catalog::{Column, IndexType, TableSchema};
use crate::common::{FusionError, Result};
use crate::monitor;
use crate::storage::Transaction;
use sqlparser::ast::ColumnOption;

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_create_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::ColumnDef],
        if_not_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = name.to_string();

        // IF NOT EXISTS check
        let schema_key_check = format!("schema:{}", table_name);
        if txn.get(schema_key_check.as_bytes()).await?.is_some() {
            if if_not_exists {
                return Ok(QueryResult::Success {
                    message: format!("Table {} already exists (skipped)", table_name),
                });
            } else {
                return Err(FusionError::Execution(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
        }
        let cols: Vec<Column> = columns
            .iter()
            .map(|c| {
                let is_primary = c.options.iter().any(|opt| match &opt.option {
                    ColumnOption::Unique(_) => false,
                    ColumnOption::PrimaryKey(_) => true,
                    _ => false,
                });
                let default_value = c.options.iter().find_map(|opt| {
                    if let ColumnOption::Default(expr) = &opt.option {
                        Some(format!("{}", expr))
                    } else {
                        None
                    }
                });
                Column {
                    name: c.name.to_string(),
                    data_type: format!("{}", c.data_type),
                    is_primary,
                    is_indexed: is_primary,
                    index_type: if is_primary {
                        IndexType::BTree
                    } else {
                        IndexType::None
                    },
                    default_value,
                    is_nullable: !is_primary
                        && !c
                            .options
                            .iter()
                            .any(|opt| matches!(&opt.option, ColumnOption::NotNull)),
                    is_unique: is_primary
                        || c.options
                            .iter()
                            .any(|opt| matches!(&opt.option, ColumnOption::Unique(_))),
                    check_expr: c.options.iter().find_map(|opt| {
                        if let ColumnOption::Check(expr) = &opt.option {
                            Some(format!("{}", expr))
                        } else {
                            None
                        }
                    }),
                }
            })
            .collect();

        let schema = TableSchema::new(table_name.clone(), cols);
        let key = format!("schema:{}", table_name);
        let value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;

        txn.put(key.as_bytes(), &value).await?;

        Ok(QueryResult::Success {
            message: format!("Table {} created", table_name),
        })
    }

    pub(crate) async fn handle_drop_table(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        object_type: sqlparser::ast::ObjectType,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        if object_type != sqlparser::ast::ObjectType::Table {
            return Err(FusionError::Execution(
                "Only DROP TABLE is supported".to_string(),
            ));
        }

        let mut dropped_count = 0;
        for name in names {
            let table_name = name.to_string();

            let schema_key = format!("schema:{}", table_name);
            if txn.get(schema_key.as_bytes()).await?.is_none() {
                if if_exists {
                    continue;
                } else {
                    return Err(FusionError::Execution(format!(
                        "Table {} does not exist",
                        table_name
                    )));
                }
            }

            txn.delete(schema_key.as_bytes()).await?;

            let prefix = format!("data:{}:", table_name);
            let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
            for (k, _) in kv_pairs {
                txn.delete(&k).await?;
                if let Ok(key_str) = std::str::from_utf8(&k) {
                    self.row_cache.invalidate(key_str);
                }
            }

            let index_prefix = format!("index:{}:", table_name);
            let index_entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }
            self.delete_index_meta_for_table(&table_name, txn).await?;

            dropped_count += 1;
        }

        Ok(QueryResult::Success {
            message: format!("Dropped {} tables", dropped_count),
        })
    }

    pub(crate) async fn handle_truncate(
        &self,
        table_names: &[sqlparser::ast::TruncateTableTarget],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let mut count = 0;
        for target in table_names {
            let table_name = target.name.to_string();
            let schema_key = format!("schema:{}", table_name);
            if txn.get(schema_key.as_bytes()).await?.is_none() {
                return Err(FusionError::Execution(format!(
                    "Table {} does not exist",
                    table_name
                )));
            }

            let prefix = format!("data:{}:", table_name);
            let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
            for (k, _) in &kv_pairs {
                txn.delete(k).await?;
                if let Ok(key_str) = std::str::from_utf8(k) {
                    self.row_cache.invalidate(key_str);
                }
            }
            count += kv_pairs.len();

            let index_prefix = format!("index:{}:", table_name);
            let index_entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }
        }

        Ok(QueryResult::Success {
            message: format!("Truncated {} rows", count),
        })
    }

    pub(crate) async fn handle_alter_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        operations: &[sqlparser::ast::AlterTableOperation],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = name.to_string();
        let schema_key = format!("schema:{}", table_name);

        let schema_bytes = txn.get(schema_key.as_bytes()).await?.ok_or_else(|| {
            FusionError::Execution(format!("Table {} does not exist", table_name))
        })?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

        let mut messages = Vec::new();

        for op in operations {
            match op {
                sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = column_def.name.to_string();
                    if schema.columns.iter().any(|c| c.name == col_name) {
                        return Err(FusionError::Execution(format!(
                            "Column {} already exists in table {}",
                            col_name, table_name
                        )));
                    }
                    let is_primary = column_def
                        .options
                        .iter()
                        .any(|opt| matches!(&opt.option, ColumnOption::PrimaryKey(_)));
                    schema.columns.push(Column {
                        name: col_name.clone(),
                        data_type: format!("{}", column_def.data_type),
                        is_primary,
                        is_indexed: is_primary,
                        index_type: if is_primary {
                            IndexType::BTree
                        } else {
                            IndexType::None
                        },
                        default_value: None,
                        is_nullable: true,
                        is_unique: false,
                        check_expr: None,
                    });
                    messages.push(format!("Added column {}", col_name));
                }
                sqlparser::ast::AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    for column_ident in column_names {
                        let col_name = column_ident.to_string();
                        let col_idx = schema.columns.iter().position(|c| c.name == col_name);
                        match col_idx {
                            Some(idx) => {
                                if schema.columns[idx].is_primary {
                                    return Err(FusionError::Execution(
                                        "Cannot drop PRIMARY KEY column".to_string(),
                                    ));
                                }
                                let affected_indexes = self
                                    .load_composite_indexes_for_table(&table_name, txn)
                                    .await?;
                                if affected_indexes.iter().any(|index| {
                                    index
                                        .columns
                                        .iter()
                                        .any(|column| column.eq_ignore_ascii_case(&col_name))
                                }) {
                                    return Err(FusionError::Execution(format!(
                                        "Cannot drop column {} because a composite index depends on it",
                                        col_name
                                    )));
                                }
                                schema.columns.remove(idx);

                                // Rewrite existing rows: remove the column at idx
                                let data_prefix = format!("data:{}:", table_name);
                                let rows = txn.scan_prefix(data_prefix.as_bytes(), None).await?;
                                for (k, v) in rows {
                                    let key_str = std::str::from_utf8(&k).ok();
                                    let row = if let Some(key_str) = key_str {
                                        if let Some(row) = self.row_cache.get(key_str) {
                                            monitor::inc_row_cache_hit();
                                            Some(row)
                                        } else {
                                            crate::common::encoding::RowDecoder::decode(&v).ok()
                                        }
                                    } else {
                                        crate::common::encoding::RowDecoder::decode(&v).ok()
                                    };

                                    if let Some(mut row) = row {
                                        if idx < row.len() {
                                            row.remove(idx);
                                            let new_v =
                                                crate::common::encoding::RowEncoder::encode(&row);
                                            txn.put(&k, &new_v).await?;
                                            if let Some(key_str) = key_str {
                                                self.row_cache.invalidate(key_str);
                                            }
                                        }
                                    }
                                }
                                messages.push(format!("Dropped column {}", col_name));
                            }
                            None => {
                                if !*if_exists {
                                    return Err(FusionError::Execution(format!(
                                        "Column {} does not exist",
                                        col_name
                                    )));
                                }
                            }
                        }
                    }
                }
                sqlparser::ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                    ..
                } => {
                    let old_name = old_column_name.to_string();
                    let new_name = new_column_name.to_string();
                    let col = schema.columns.iter_mut().find(|c| c.name == old_name);
                    match col {
                        Some(c) => {
                            let affected_indexes = self
                                .load_composite_indexes_for_table(&table_name, txn)
                                .await?;
                            if affected_indexes.iter().any(|index| {
                                index
                                    .columns
                                    .iter()
                                    .any(|column| column.eq_ignore_ascii_case(&old_name))
                            }) {
                                return Err(FusionError::Execution(format!(
                                    "Cannot rename column {} because a composite index depends on it",
                                    old_name
                                )));
                            }
                            c.name = new_name.clone();
                            messages.push(format!("Renamed {} to {}", old_name, new_name));
                        }
                        None => {
                            return Err(FusionError::Execution(format!(
                                "Column {} does not exist",
                                old_name
                            )));
                        }
                    }
                }
                other => {
                    return Err(FusionError::Execution(format!(
                        "Unsupported ALTER TABLE operation: {:?}",
                        other
                    )));
                }
            }
        }

        // Save updated schema
        let new_bytes = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &new_bytes).await?;

        Ok(QueryResult::Success {
            message: messages.join("; "),
        })
    }
}
