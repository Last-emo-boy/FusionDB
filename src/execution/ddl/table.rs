use crate::catalog::{Column, IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{ColumnOption, Expr, TableConstraint};
use std::collections::HashSet;

use super::super::{Executor, QueryResult};

struct TablePrimaryKeySpec {
    name: Option<String>,
    columns: Vec<String>,
}

struct TableUniqueSpec {
    name: Option<String>,
    columns: Vec<String>,
}

impl TablePrimaryKeySpec {
    fn single_primary_column(&self) -> Option<&String> {
        (self.columns.len() == 1).then_some(&self.columns[0])
    }

    fn has_column(&self, name: &str) -> bool {
        self.columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case(name))
    }
}

#[cfg(test)]
fn table_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
    let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
    key.push_str("data:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(row_id);
    key
}

#[cfg(test)]
fn table_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn table_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

#[cfg(test)]
fn table_index_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("index:".len() + table_name.len() + 1);
    prefix.push_str("index:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

#[cfg(test)]
fn table_index_prefix_for_column(table_name: &str, column_name: &str) -> String {
    let mut prefix =
        String::with_capacity("index:".len() + table_name.len() + 1 + column_name.len() + 1);
    prefix.push_str("index:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix.push_str(column_name);
    prefix.push(':');
    prefix
}

#[cfg(test)]
fn table_index_key_for_value(
    table_name: &str,
    column_name: &str,
    value: &str,
    row_id: &str,
) -> String {
    let mut key = String::with_capacity(
        "index:".len()
            + table_name.len()
            + 1
            + column_name.len()
            + 1
            + value.len()
            + 1
            + row_id.len(),
    );
    key.push_str("index:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(column_name);
    key.push(':');
    key.push_str(value);
    key.push(':');
    key.push_str(row_id);
    key
}

#[cfg(test)]
fn table_index_key_for_prefix_value_row(prefix: &str, value: &str, row_id: &str) -> String {
    let mut key = String::with_capacity(prefix.len() + value.len() + 1 + row_id.len());
    key.push_str(prefix);
    key.push_str(value);
    key.push(':');
    key.push_str(row_id);
    key
}

fn table_index_meta_key_for_index(index_name: &str) -> String {
    let mut key = String::with_capacity("index_meta:".len() + index_name.len());
    key.push_str("index_meta:");
    key.push_str(index_name);
    key
}

fn table_primary_key_index_name(table_name: &str) -> String {
    let mut name = String::with_capacity(table_name.len() + "_pkey".len());
    name.push_str(table_name);
    name.push_str("_pkey");
    name
}

fn table_column_primary_key_index_name(table_name: &str, column_name: &str) -> String {
    let mut name = String::with_capacity(table_name.len() + 1 + column_name.len() + "_pkey".len());
    name.push_str(table_name);
    name.push('_');
    name.push_str(column_name);
    name.push_str("_pkey");
    name
}

fn table_row_id_suffix(row_id: &str) -> String {
    let mut suffix = String::with_capacity(1 + row_id.len());
    suffix.push(':');
    suffix.push_str(row_id);
    suffix
}

fn sql_identifier_name(ident: &sqlparser::ast::Ident) -> String {
    ident.value.clone()
}

impl Executor {
    async fn column_has_btree_index_dependency(
        &self,
        table_name: &str,
        column_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        let entries = txn.scan_prefix(b"index_meta:", None).await?;
        for (key, value) in entries {
            let Ok(key_str) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix("index_meta:") else {
                continue;
            };
            let meta_str = String::from_utf8(value).unwrap_or_default();
            let Some(index) = Self::parse_index_meta(index_name, &meta_str) else {
                continue;
            };
            if index.table != table_name {
                continue;
            }
            if index
                .columns
                .iter()
                .chain(index.include_columns.iter())
                .any(|column| column.eq_ignore_ascii_case(column_name))
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(crate) async fn handle_create_table(
        &self,
        name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::ColumnDef],
        constraints: &[TableConstraint],
        if_not_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = name.to_string();

        // IF NOT EXISTS check
        let schema_key_check = table_schema_key_for_table(&table_name);
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
        for column in columns {
            for option in &column.options {
                if let ColumnOption::Unique(unique) = &option.option {
                    if matches!(
                        unique.nulls_distinct,
                        sqlparser::ast::NullsDistinctOption::NotDistinct
                    ) {
                        return Err(FusionError::Execution(
                            "UNIQUE NULLS NOT DISTINCT is not supported".to_string(),
                        ));
                    }
                }
            }
        }
        let table_primary_key = Self::extract_table_primary_key(columns, constraints)?;
        let table_unique_constraints =
            Self::extract_table_unique_constraints(columns, constraints)?;
        let mut cols = Vec::with_capacity(columns.len());
        for c in columns {
            let col_name = sql_identifier_name(&c.name);
            let column_primary = c
                .options
                .iter()
                .any(|opt| matches!(&opt.option, ColumnOption::PrimaryKey(_)));
            let is_primary = column_primary
                || table_primary_key
                    .as_ref()
                    .and_then(|pk| pk.single_primary_column())
                    .is_some_and(|pk_name| pk_name.eq_ignore_ascii_case(&col_name));
            let is_composite_primary = table_primary_key
                .as_ref()
                .is_some_and(|pk| pk.columns.len() > 1 && pk.has_column(&col_name));
            let default_value = c.options.iter().find_map(|opt| {
                if let ColumnOption::Default(expr) = &opt.option {
                    Some(format!("{}", expr))
                } else {
                    None
                }
            });
            cols.push(Column {
                name: col_name.clone(),
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
                    && !is_composite_primary
                    && !c
                        .options
                        .iter()
                        .any(|opt| matches!(&opt.option, ColumnOption::NotNull)),
                is_unique: is_primary
                    || table_unique_constraints.iter().any(|constraint| {
                        constraint.columns.len() == 1
                            && constraint.columns[0].eq_ignore_ascii_case(&col_name)
                    })
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
            });
        }

        let foreign_keys = Self::collect_foreign_keys(&table_name, columns, constraints)?;
        for fk in &foreign_keys {
            for child_column in fk.child_columns() {
                if cols
                    .iter()
                    .all(|column| !column.name.eq_ignore_ascii_case(&child_column))
                {
                    return Err(FusionError::Execution(format!(
                        "FOREIGN KEY child column {} not found",
                        child_column
                    )));
                }
            }

            let parent_key = table_schema_key_for_table(&fk.parent_table);
            let parent_schema_bytes = txn.get(parent_key.as_bytes()).await?.ok_or_else(|| {
                FusionError::Execution(format!("Referenced table {} not found", fk.parent_table))
            })?;
            let parent_schema: TableSchema = bincode::deserialize(&parent_schema_bytes)
                .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;
            for parent_column in fk.parent_columns() {
                if parent_schema
                    .columns
                    .iter()
                    .all(|column| !column.name.eq_ignore_ascii_case(&parent_column))
                {
                    return Err(FusionError::Execution(format!(
                        "Referenced column {}.{} not found",
                        fk.parent_table, parent_column
                    )));
                }
            }
        }

        let schema = TableSchema::new(table_name.clone(), cols);
        let key = table_schema_key_for_table(&table_name);
        let value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;

        txn.put(key.as_bytes(), &value).await?;
        self.ensure_composite_index_directory_marker(&table_name, txn)
            .await?;
        if let Some(primary_key) = table_primary_key.as_ref() {
            if primary_key.columns.len() > 1 {
                self.store_composite_primary_key_index(&table_name, primary_key, txn)
                    .await?;
            }
        }
        for unique in &table_unique_constraints {
            if unique.columns.len() > 1 {
                self.store_composite_unique_index(&table_name, unique, txn)
                    .await?;
            }
        }
        self.store_foreign_keys(&table_name, &foreign_keys, txn)
            .await?;

        Ok(QueryResult::Success {
            message: format!("Table {} created", table_name),
        })
    }

    fn extract_table_primary_key(
        columns: &[sqlparser::ast::ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Option<TablePrimaryKeySpec>> {
        let column_primary_count = columns
            .iter()
            .filter(|column| {
                column
                    .options
                    .iter()
                    .any(|opt| matches!(&opt.option, ColumnOption::PrimaryKey(_)))
            })
            .count();
        let mut primary_constraints = Vec::with_capacity(constraints.len());
        for constraint in constraints {
            if let TableConstraint::PrimaryKey(pk) = constraint {
                primary_constraints.push(pk);
            }
        }

        if column_primary_count > 0 && !primary_constraints.is_empty() {
            return Err(FusionError::Execution(
                "Multiple PRIMARY KEY constraints are not supported".to_string(),
            ));
        }
        if primary_constraints.len() > 1 {
            return Err(FusionError::Execution(
                "Multiple PRIMARY KEY constraints are not supported".to_string(),
            ));
        }

        let Some(pk) = primary_constraints.first() else {
            return Ok(None);
        };
        let mut pk_columns = Vec::with_capacity(pk.columns.len());
        for column in &pk.columns {
            let column_name = match &column.column.expr {
                Expr::Identifier(ident) => ident.value.clone(),
                _ => {
                    return Err(FusionError::Execution(
                        "PRIMARY KEY only supports simple column references".to_string(),
                    ))
                }
            };
            if columns
                .iter()
                .all(|column| !column.name.value.eq_ignore_ascii_case(&column_name))
            {
                return Err(FusionError::Execution(format!(
                    "Column {} not found",
                    column_name
                )));
            }
            pk_columns.push(column_name);
        }

        if pk_columns.is_empty() {
            return Err(FusionError::Execution(
                "PRIMARY KEY requires at least one column".to_string(),
            ));
        }
        if pk_columns.len() == 1 {
            let column_name = &pk_columns[0];
            let column_idx = columns
                .iter()
                .position(|column| column.name.value.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| {
                    FusionError::Execution(format!("Column {} not found", column_name))
                })?;
            if column_idx != 0 {
                return Err(FusionError::Execution(format!(
                    "Table-level PRIMARY KEY currently requires the primary key column to be the first column; {} is at position {}",
                    column_name,
                    column_idx + 1
                )));
            }
        } else if !columns
            .first()
            .is_some_and(|column| pk_columns[0].eq_ignore_ascii_case(&column.name.value))
        {
            return Err(FusionError::Execution(format!(
                "Composite PRIMARY KEY currently requires the first key column to be the first table column; {} is first in the key",
                pk_columns[0]
            )));
        }

        Ok(Some(TablePrimaryKeySpec {
            name: pk
                .name
                .as_ref()
                .or(pk.index_name.as_ref())
                .map(|ident| ident.value.clone()),
            columns: pk_columns,
        }))
    }

    fn extract_table_unique_constraints(
        columns: &[sqlparser::ast::ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Vec<TableUniqueSpec>> {
        let mut unique_constraints = Vec::new();
        for constraint in constraints {
            let TableConstraint::Unique(unique) = constraint else {
                continue;
            };
            if matches!(
                unique.nulls_distinct,
                sqlparser::ast::NullsDistinctOption::NotDistinct
            ) {
                return Err(FusionError::Execution(
                    "UNIQUE NULLS NOT DISTINCT is not supported".to_string(),
                ));
            }
            if unique
                .index_type
                .as_ref()
                .is_some_and(|index_type| !matches!(index_type, sqlparser::ast::IndexType::BTree))
            {
                return Err(FusionError::Execution(
                    "UNIQUE constraints only support BTree indexes".to_string(),
                ));
            }

            let mut unique_columns = Vec::with_capacity(unique.columns.len());
            let mut seen_columns = HashSet::with_capacity(unique.columns.len());
            for column in &unique.columns {
                let column_name = match &column.column.expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    _ => {
                        return Err(FusionError::Execution(
                            "UNIQUE only supports simple column references".to_string(),
                        ))
                    }
                };
                let schema_column = columns
                    .iter()
                    .find(|column| column.name.value.eq_ignore_ascii_case(&column_name))
                    .ok_or_else(|| {
                        FusionError::Execution(format!("Column {} not found", column_name))
                    })?;
                let canonical_name = sql_identifier_name(&schema_column.name);
                if !seen_columns.insert(canonical_name.to_ascii_lowercase()) {
                    return Err(FusionError::Execution(format!(
                        "Duplicate UNIQUE column {}",
                        canonical_name
                    )));
                }
                unique_columns.push(canonical_name);
            }
            if unique_columns.is_empty() {
                return Err(FusionError::Execution(
                    "UNIQUE requires at least one column".to_string(),
                ));
            }

            unique_constraints.push(TableUniqueSpec {
                name: unique
                    .name
                    .as_ref()
                    .or(unique.index_name.as_ref())
                    .map(|ident| ident.value.clone()),
                columns: unique_columns,
            });
        }

        Ok(unique_constraints)
    }

    async fn store_composite_primary_key_index(
        &self,
        table_name: &str,
        primary_key: &TablePrimaryKeySpec,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let index_name = primary_key
            .name
            .clone()
            .unwrap_or_else(|| table_primary_key_index_name(table_name));
        let meta_key = table_index_meta_key_for_index(&index_name);
        if txn.get(meta_key.as_bytes()).await?.is_some() {
            return Err(FusionError::Execution(format!(
                "Index {} already exists",
                index_name
            )));
        }
        let meta_val = Self::composite_primary_meta_value(table_name, &primary_key.columns);
        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;
        self.rebuild_composite_index_directory_for_table(table_name, txn)
            .await
    }

    async fn store_composite_unique_index(
        &self,
        table_name: &str,
        unique: &TableUniqueSpec,
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let index_name = unique
            .name
            .clone()
            .unwrap_or_else(|| format!("{}_{}_key", table_name, unique.columns.join("_")));
        let meta_key = table_index_meta_key_for_index(&index_name);
        if txn.get(meta_key.as_bytes()).await?.is_some() {
            return Err(FusionError::Execution(format!(
                "Index {} already exists",
                index_name
            )));
        }
        let meta_val = Self::composite_unique_meta_value(table_name, &unique.columns);
        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;
        self.rebuild_composite_index_directory_for_table(table_name, txn)
            .await
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

            let schema_key = table_schema_key_for_table(&table_name);
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
            if !self
                .load_parent_foreign_keys(&table_name, txn)
                .await?
                .is_empty()
            {
                return Err(FusionError::Execution(format!(
                    "Cannot drop table {} because it is referenced by a FOREIGN KEY",
                    table_name
                )));
            }

            self.touch_all_index_build_barriers(&table_name, txn)
                .await?;

            let kv_pairs = self
                .scan_routed_data_prefixes_for_table(&table_name, txn, None)
                .await?;
            for (k, _) in kv_pairs {
                let row_id = self.legacy_row_id_from_routed_data_key(&table_name, &k)?;
                self.delete_routed_data_row(&table_name, row_id, txn)
                    .await?;
            }
            self.delete_structured_data_shadows_for_table(&table_name, txn)
                .await?;
            // Force a write-write conflict with any in-flight backfill chunk:
            // the cleanup above only tombstones v2 keys its own snapshot saw,
            // so a concurrent chunk's fresh puts would otherwise commit
            // alongside this DDL and strand orphan shadow rows.
            self.touch_backfill_state_for_ddl(txn).await?;

            let mut index_entries = self
                .scan_routed_prefixes(self.routed_index_prefixes_for_table(&table_name), txn, None)
                .await?;
            let mut fts_entries = self
                .scan_routed_prefixes(self.routed_fts_prefixes_for_table(&table_name), txn, None)
                .await?;
            index_entries.append(&mut fts_entries);
            let mut unique_sentinels = self
                .scan_routed_prefixes(
                    self.routed_unique_sentinel_prefixes_for_table(&table_name),
                    txn,
                    None,
                )
                .await?;
            index_entries.append(&mut unique_sentinels);
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }
            self.delete_index_count_summaries_for_table(&table_name, txn)
                .await?;
            self.delete_index_meta_for_table(&table_name, txn).await?;
            self.delete_foreign_keys_for_table(&table_name, txn).await?;
            txn.delete(schema_key.as_bytes()).await?;

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
        let truncated_tables: HashSet<String> = table_names
            .iter()
            .map(|target| target.name.to_string())
            .collect();
        for table_name in &truncated_tables {
            if let Some(reference) = self
                .load_parent_foreign_keys(table_name, txn)
                .await?
                .into_iter()
                .find(|foreign_key| !truncated_tables.contains(&foreign_key.child_table))
            {
                return Err(FusionError::Execution(format!(
                    "Cannot truncate table {} because it is referenced by FOREIGN KEY '{}' on table {}",
                    table_name, reference.name, reference.child_table
                )));
            }
        }

        let mut count = 0;
        for target in table_names {
            let table_name = target.name.to_string();
            let schema_key = table_schema_key_for_table(&table_name);
            let schema_bytes = txn.get(schema_key.as_bytes()).await?.ok_or_else(|| {
                FusionError::Execution(format!("Table {} does not exist", table_name))
            })?;
            let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                FusionError::Execution(format!("Schema deserialization error: {}", e))
            })?;
            self.touch_all_index_build_barriers(&table_name, txn)
                .await?;

            let kv_pairs = self
                .scan_routed_data_prefixes_for_table(&table_name, txn, None)
                .await?;
            for (k, _) in &kv_pairs {
                let row_id = self.legacy_row_id_from_routed_data_key(&table_name, k)?;
                self.delete_routed_data_row(&table_name, row_id, txn)
                    .await?;
            }
            self.delete_structured_data_shadows_for_table(&table_name, txn)
                .await?;
            // Force a write-write conflict with any in-flight backfill chunk:
            // the cleanup above only tombstones v2 keys its own snapshot saw,
            // so a concurrent chunk's fresh puts would otherwise commit
            // alongside this DDL and strand orphan shadow rows.
            self.touch_backfill_state_for_ddl(txn).await?;
            count += kv_pairs.len();

            let mut index_entries = self
                .scan_routed_prefixes(self.routed_index_prefixes_for_table(&table_name), txn, None)
                .await?;
            let mut fts_entries = self
                .scan_routed_prefixes(self.routed_fts_prefixes_for_table(&table_name), txn, None)
                .await?;
            index_entries.append(&mut fts_entries);
            let mut unique_sentinels = self
                .scan_routed_prefixes(
                    self.routed_unique_sentinel_prefixes_for_table(&table_name),
                    txn,
                    None,
                )
                .await?;
            index_entries.append(&mut unique_sentinels);
            for (k, _) in index_entries {
                txn.delete(&k).await?;
            }
            self.delete_index_count_summaries_for_table(&table_name, txn)
                .await?;
            let empty_counts = std::collections::HashMap::new();
            for column in &schema.columns {
                if Self::index_count_summary_maintained_for_column(column) {
                    self.replace_index_count_summary_for_column(
                        &table_name,
                        &column.name,
                        &empty_counts,
                        txn,
                    )
                    .await?;
                }
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
        let schema_key = table_schema_key_for_table(&table_name);

        let schema_bytes = txn.get(schema_key.as_bytes()).await?.ok_or_else(|| {
            FusionError::Execution(format!("Table {} does not exist", table_name))
        })?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

        let mut messages = Vec::with_capacity(operations.len());

        for op in operations {
            match op {
                sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = sql_identifier_name(&column_def.name);
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
                        let col_name = sql_identifier_name(column_ident);
                        let col_idx = schema.columns.iter().position(|c| c.name == col_name);
                        match col_idx {
                            Some(idx) => {
                                if schema.columns[idx].is_primary {
                                    return Err(FusionError::Execution(
                                        "Cannot drop PRIMARY KEY column".to_string(),
                                    ));
                                }
                                if self
                                    .column_has_btree_index_dependency(&table_name, &col_name, txn)
                                    .await?
                                {
                                    return Err(FusionError::Execution(format!(
                                        "Cannot drop column {} because a BTree index depends on it",
                                        col_name
                                    )));
                                }
                                let child_foreign_keys =
                                    self.load_child_foreign_keys(&table_name, txn).await?;
                                if child_foreign_keys.iter().any(|fk| {
                                    fk.child_columns()
                                        .iter()
                                        .any(|column| column.eq_ignore_ascii_case(&col_name))
                                }) {
                                    return Err(FusionError::Execution(format!(
                                        "Cannot drop column {} because a FOREIGN KEY depends on it",
                                        col_name
                                    )));
                                }
                                let parent_foreign_keys =
                                    self.load_parent_foreign_keys(&table_name, txn).await?;
                                if parent_foreign_keys.iter().any(|fk| {
                                    fk.parent_columns()
                                        .iter()
                                        .any(|column| column.eq_ignore_ascii_case(&col_name))
                                }) {
                                    return Err(FusionError::Execution(format!(
                                        "Cannot drop column {} because it is referenced by a FOREIGN KEY",
                                        col_name
                                    )));
                                }
                                schema.columns.remove(idx);

                                // Rewrite existing rows: remove the column at idx
                                let rows = self
                                    .scan_routed_data_prefixes_for_table(&table_name, txn, None)
                                    .await?;
                                for (k, v) in rows {
                                    let key_str = std::str::from_utf8(&k).ok();
                                    let row = if let Some(key_str) = key_str {
                                        if let Some(row) = self.row_cache_lookup(key_str, &v) {
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
                                            let row_id = self.legacy_row_id_from_routed_data_key(
                                                &table_name,
                                                &k,
                                            )?;
                                            self.write_routed_data_row(
                                                &table_name,
                                                row_id,
                                                &new_v,
                                                txn,
                                            )
                                            .await?;
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
                    let old_name = sql_identifier_name(old_column_name);
                    let new_name = sql_identifier_name(new_column_name);
                    let col = schema.columns.iter_mut().find(|c| c.name == old_name);
                    match col {
                        Some(c) => {
                            if self
                                .column_has_btree_index_dependency(&table_name, &old_name, txn)
                                .await?
                            {
                                return Err(FusionError::Execution(format!(
                                    "Cannot rename column {} because a BTree index depends on it",
                                    old_name
                                )));
                            }
                            let child_foreign_keys =
                                self.load_child_foreign_keys(&table_name, txn).await?;
                            if child_foreign_keys.iter().any(|fk| {
                                fk.child_columns()
                                    .iter()
                                    .any(|column| column.eq_ignore_ascii_case(&old_name))
                            }) {
                                return Err(FusionError::Execution(format!(
                                    "Cannot rename column {} because a FOREIGN KEY depends on it",
                                    old_name
                                )));
                            }
                            let parent_foreign_keys =
                                self.load_parent_foreign_keys(&table_name, txn).await?;
                            if parent_foreign_keys.iter().any(|fk| {
                                fk.parent_columns()
                                    .iter()
                                    .any(|column| column.eq_ignore_ascii_case(&old_name))
                            }) {
                                return Err(FusionError::Execution(format!(
                                    "Cannot rename column {} because it is referenced by a FOREIGN KEY",
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
                sqlparser::ast::AlterTableOperation::AddConstraint {
                    constraint:
                        TableConstraint::PrimaryKey(sqlparser::ast::PrimaryKeyConstraint {
                            name,
                            index_name,
                            columns,
                            ..
                        }),
                    ..
                } => {
                    let constraint_name = name
                        .as_ref()
                        .or(index_name.as_ref())
                        .map(|ident| ident.value.clone());
                    let message = self
                        .handle_alter_table_add_primary_key(
                            &table_name,
                            &mut schema,
                            constraint_name,
                            columns,
                            txn,
                        )
                        .await?;
                    messages.push(message);
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

    async fn handle_alter_table_add_primary_key(
        &self,
        table_name: &str,
        schema: &mut TableSchema,
        constraint_name: Option<String>,
        columns: &[sqlparser::ast::IndexColumn],
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        if columns.len() != 1 {
            return Err(FusionError::Execution(
                "ALTER TABLE ADD PRIMARY KEY currently supports exactly one column".to_string(),
            ));
        }

        if schema.get_primary_key_index().is_some() {
            return Err(FusionError::Execution(format!(
                "Table {} already has a PRIMARY KEY",
                table_name
            )));
        }

        let column_name = match &columns[0].column.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            _ => {
                return Err(FusionError::Execution(
                    "PRIMARY KEY only supports simple column references".to_string(),
                ))
            }
        };
        let column_idx = schema
            .get_column_index(&column_name)
            .ok_or_else(|| FusionError::Execution(format!("Column {} not found", column_name)))?;

        if column_idx != 0 {
            return Err(FusionError::Execution(format!(
                "ALTER TABLE ADD PRIMARY KEY currently requires the primary key column to be the first column; {} is at position {}",
                column_name,
                column_idx + 1
            )));
        }

        let rows = self
            .scan_routed_data_prefixes_for_table(table_name, txn, None)
            .await?;
        let mut seen = HashSet::with_capacity(rows.len());
        let mut rewrites = Vec::with_capacity(rows.len());

        for (key, value) in rows {
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| FusionError::Execution(format!("Data key decode error: {}", e)))?;
            let old_row_id = self
                .legacy_row_id_from_routed_data_key(table_name, &key)?
                .to_string();

            let pk_value = if let Some(row) = self.row_cache_lookup(key_str, &value) {
                row.get(column_idx).cloned().unwrap_or(Value::Null)
            } else {
                crate::common::encoding::RowDecoder::decode_column(&value, column_idx)
                    .map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                    .unwrap_or(Value::Null)
            };

            if pk_value == Value::Null {
                return Err(FusionError::Execution(format!(
                    "PRIMARY KEY column '{}' contains NULL values",
                    column_name
                )));
            }

            let pk_key = Self::value_to_primary_row_id(&pk_value).ok_or_else(|| {
                FusionError::Execution(format!(
                    "PRIMARY KEY column '{}' has unsupported value '{}'",
                    column_name, pk_value
                ))
            })?;
            if !seen.insert(pk_key.clone()) {
                return Err(FusionError::Execution(format!(
                    "PRIMARY KEY constraint violated for column '{}': duplicate value '{}'",
                    column_name, pk_value
                )));
            }

            rewrites.push((key, old_row_id, pk_key, value));
        }

        let index_name = constraint_name
            .unwrap_or_else(|| table_column_primary_key_index_name(table_name, &column_name));
        let meta_key = table_index_meta_key_for_index(&index_name);
        if txn.get(meta_key.as_bytes()).await?.is_some() {
            return Err(FusionError::Execution(format!(
                "Index {} already exists",
                index_name
            )));
        }
        let meta_val = Self::single_column_index_meta_value(table_name, &column_name);

        let column = &mut schema.columns[column_idx];
        column.is_primary = true;
        column.is_unique = true;
        column.is_nullable = false;
        column.is_indexed = true;
        column.index_type = IndexType::BTree;

        for (old_key, old_row_id, new_row_id, value) in rewrites {
            let new_key = self.routed_data_key_for_row_id(table_name, &new_row_id);
            let old_key_str = std::str::from_utf8(&old_key)
                .map_err(|e| FusionError::Execution(format!("Data key decode error: {}", e)))?
                .to_string();
            if old_key_str != new_key {
                if txn.get(new_key.as_bytes()).await?.is_some() {
                    return Err(FusionError::Execution(format!(
                        "PRIMARY KEY constraint violated for column '{}': duplicate storage key",
                        column_name
                    )));
                }
                self.delete_routed_data_row(table_name, &old_row_id, txn)
                    .await?;
                self.write_routed_data_row(table_name, &new_row_id, &value, txn)
                    .await?;
                self.rewrite_row_id_references_for_primary_key(
                    table_name,
                    schema,
                    &old_row_id,
                    &new_row_id,
                    &value,
                    txn,
                )
                .await?;
            }
        }

        let rows_after_rewrite = self
            .scan_routed_data_prefixes_for_table(table_name, txn, None)
            .await?;
        for (key, value) in rows_after_rewrite {
            let row_id = self.legacy_row_id_from_routed_data_key(table_name, &key)?;
            let pk_value = crate::common::encoding::RowDecoder::decode_column(&value, column_idx)
                .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?
                .unwrap_or(Value::Null);
            if let Some(index_value) = self.value_to_index_string(&pk_value) {
                let index_key =
                    self.routed_index_key_for_value(table_name, &column_name, &index_value, row_id);
                txn.put(index_key.as_bytes(), &[]).await?;
            }
        }

        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;

        Ok(format!(
            "Added PRIMARY KEY {} on {}({})",
            index_name, table_name, column_name
        ))
    }

    async fn rewrite_row_id_references_for_primary_key(
        &self,
        table_name: &str,
        schema: &TableSchema,
        old_row_id: &str,
        new_row_id: &str,
        row_bytes: &[u8],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        for column in &schema.columns {
            if !column.is_indexed || column.is_primary {
                continue;
            }
            match column.index_type {
                IndexType::BTree => {
                    let old_suffix = table_row_id_suffix(old_row_id);
                    for prefix in self.routed_index_prefixes_for_column(table_name, &column.name) {
                        let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
                        for (index_key, index_value) in entries {
                            let Ok(index_key_str) = std::str::from_utf8(&index_key) else {
                                continue;
                            };
                            if !index_key_str.ends_with(&old_suffix) {
                                continue;
                            }
                            let Some(value_part) = index_key_str
                                .strip_prefix(&prefix)
                                .and_then(|suffix| suffix.rsplit_once(':').map(|(value, _)| value))
                            else {
                                continue;
                            };
                            let new_index_key = self.routed_index_key_for_value(
                                table_name,
                                &column.name,
                                value_part,
                                new_row_id,
                            );
                            txn.delete(&index_key).await?;
                            txn.put(new_index_key.as_bytes(), &index_value).await?;
                        }
                    }
                }
                IndexType::FTS => {
                    let old_suffix = table_row_id_suffix(old_row_id);
                    for prefix in self.routed_fts_prefixes_for_column(table_name, &column.name) {
                        let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
                        for (index_key, index_value) in entries {
                            let Ok(index_key_str) = std::str::from_utf8(&index_key) else {
                                continue;
                            };
                            if !index_key_str.ends_with(&old_suffix) {
                                continue;
                            }
                            let Some(token) = index_key_str
                                .strip_prefix(&prefix)
                                .and_then(|suffix| suffix.rsplit_once(':').map(|(token, _)| token))
                            else {
                                continue;
                            };
                            let new_index_key = self.routed_fts_index_key_for_row(
                                table_name,
                                &column.name,
                                token,
                                new_row_id,
                            );
                            txn.delete(&index_key).await?;
                            txn.put(new_index_key.as_bytes(), &index_value).await?;
                        }
                    }
                }
                IndexType::HNSW => {
                    if let Some(col_idx) = schema.get_column_index(&column.name) {
                        if let Some(Value::Vector(vector)) =
                            crate::common::encoding::RowDecoder::decode_column(row_bytes, col_idx)
                                .map_err(|e| {
                                FusionError::Execution(format!("Data deserialization error: {}", e))
                            })?
                        {
                            let index_name =
                                Self::hnsw_index_name_for_column(table_name, &column.name)?;
                            self.defer_or_apply_vector_delete(&index_name, old_row_id, txn)?;
                            self.defer_or_apply_vector_insert(
                                &index_name,
                                new_row_id.to_string(),
                                vector,
                                txn,
                            )?;
                        }
                    }
                }
                IndexType::None => {}
            }
        }

        let composite_indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        if !composite_indexes.is_empty() {
            let row = crate::common::encoding::RowDecoder::decode(row_bytes).map_err(|e| {
                FusionError::Execution(format!("Data deserialization error: {}", e))
            })?;
            self.delete_loaded_composite_indexes_for_row(
                &composite_indexes,
                table_name,
                schema,
                &row,
                old_row_id,
                txn,
            )
            .await?;
            self.put_loaded_composite_indexes_for_row(
                &composite_indexes,
                table_name,
                schema,
                &row,
                new_row_id,
                txn,
            )
            .await?;
        }

        let trigram_column_indices = Self::indexed_trigram_text_columns(schema);
        if !trigram_column_indices.is_empty() {
            let row = crate::common::encoding::RowDecoder::decode(row_bytes).map_err(|e| {
                FusionError::Execution(format!("Data deserialization error: {}", e))
            })?;
            self.update_trigram_index_for_delete(
                table_name,
                schema,
                &row,
                old_row_id,
                &trigram_column_indices,
                txn,
            );
            self.update_trigram_index_for_insert(
                table_name,
                schema,
                &row,
                new_row_id,
                &trigram_column_indices,
                txn,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        table_column_primary_key_index_name, table_data_key_for_row_id,
        table_data_prefix_for_table, table_index_key_for_prefix_value_row,
        table_index_key_for_value, table_index_meta_key_for_index, table_index_prefix_for_column,
        table_index_prefix_for_table, table_primary_key_index_name, table_row_id_suffix,
        table_schema_key_for_table,
    };

    #[test]
    fn table_schema_key_for_table_preallocates_exact_key() {
        let key = table_schema_key_for_table("accounts");

        assert_eq!(key, "schema:accounts");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = table_data_prefix_for_table("accounts");

        assert_eq!(prefix, "data:accounts:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn table_data_key_for_row_id_preallocates_exact_key() {
        let key = table_data_key_for_row_id("accounts", "00042");

        assert_eq!(key, "data:accounts:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_index_prefix_for_table_preallocates_exact_prefix() {
        let prefix = table_index_prefix_for_table("accounts");

        assert_eq!(prefix, "index:accounts:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn table_index_prefix_for_column_preallocates_exact_prefix() {
        let prefix = table_index_prefix_for_column("accounts", "name");

        assert_eq!(prefix, "index:accounts:name:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn table_index_key_for_value_preallocates_exact_key() {
        let key = table_index_key_for_value("accounts", "name", "alice", "00042");

        assert_eq!(key, "index:accounts:name:alice:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_index_key_for_prefix_value_row_preallocates_exact_key() {
        let prefix = table_index_prefix_for_column("accounts", "name");
        let key = table_index_key_for_prefix_value_row(&prefix, "alice", "00042");

        assert_eq!(key, "index:accounts:name:alice:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_index_meta_key_for_index_preallocates_exact_key() {
        let key = table_index_meta_key_for_index("accounts_pkey");

        assert_eq!(key, "index_meta:accounts_pkey");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn table_primary_key_index_name_preallocates_exact_name() {
        let name = table_primary_key_index_name("accounts");

        assert_eq!(name, "accounts_pkey");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn table_column_primary_key_index_name_preallocates_exact_name() {
        let name = table_column_primary_key_index_name("accounts", "id");

        assert_eq!(name, "accounts_id_pkey");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn table_row_id_suffix_preallocates_exact_suffix() {
        let suffix = table_row_id_suffix("00042");

        assert_eq!(suffix, ":00042");
        assert!(suffix.capacity() >= suffix.len());
    }
}
