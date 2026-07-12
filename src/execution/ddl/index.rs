use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::storage::{hnsw_index_name_for_column, Transaction};
use sqlparser::ast::Expr;
use std::collections::{HashMap, HashSet};

use super::super::composite_index::CompositeIndexMeta;
use super::super::{Executor, QueryResult};

#[cfg(test)]
fn create_index_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn index_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

fn index_meta_key_for_index(index_name: &str) -> String {
    let mut key = String::with_capacity("index_meta:".len() + index_name.len());
    key.push_str("index_meta:");
    key.push_str(index_name);
    key
}

#[cfg(test)]
fn create_index_key_for_value(
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
fn drop_index_prefix_for_column(table_name: &str, column_name: &str) -> String {
    let mut prefix =
        String::with_capacity("index:".len() + table_name.len() + 1 + column_name.len() + 1);
    prefix.push_str("index:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix.push_str(column_name);
    prefix.push(':');
    prefix
}

impl Executor {
    fn index_key_columns_match(left: &CompositeIndexMeta, right: &CompositeIndexMeta) -> bool {
        left.table.eq_ignore_ascii_case(&right.table)
            && left.columns.len() == right.columns.len()
            && left
                .columns
                .iter()
                .zip(&right.columns)
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
    }

    fn index_include_layout_matches(left: &[String], right: &[String]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
    }

    async fn reject_incompatible_shared_index_payload(
        &self,
        index_name: &str,
        table_name: &str,
        columns: &[String],
        include_columns: &[String],
        txn: &mut dyn Transaction,
    ) -> Result<()> {
        let proposed = CompositeIndexMeta {
            name: index_name.to_string(),
            table: table_name.to_string(),
            columns: columns.to_vec(),
            include_columns: include_columns.to_vec(),
            ordered_encoding: true,
            is_unique: false,
            unique_sentinel_authoritative: false,
            is_primary: false,
        };
        for (key, value) in txn.scan_prefix(b"index_meta:", None).await? {
            let Ok(key) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(other_name) = key.strip_prefix("index_meta:") else {
                continue;
            };
            if other_name == index_name {
                continue;
            }
            let Ok(value) = std::str::from_utf8(&value) else {
                continue;
            };
            let Some(other) = Self::parse_index_meta(other_name, value) else {
                continue;
            };
            if Self::index_key_columns_match(&proposed, &other)
                && !Self::index_include_layout_matches(include_columns, &other.include_columns)
            {
                return Err(FusionError::Execution(format!(
                    "Index {} cannot share {}({}) with index {} because their INCLUDE column layouts differ",
                    index_name,
                    table_name,
                    columns.join(", "),
                    other_name
                )));
            }
        }
        Ok(())
    }

    async fn other_index_references_physical_entries(
        &self,
        index_name: &str,
        meta: &CompositeIndexMeta,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        for (key, value) in txn.scan_prefix(b"index_meta:", None).await? {
            let Ok(key) = std::str::from_utf8(&key) else {
                continue;
            };
            let Some(other_name) = key.strip_prefix("index_meta:") else {
                continue;
            };
            if other_name == index_name {
                continue;
            }
            let Ok(value) = std::str::from_utf8(&value) else {
                continue;
            };
            let Some(other) = Self::parse_index_meta(other_name, value) else {
                continue;
            };
            if Self::index_key_columns_match(meta, &other) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) async fn handle_create_index(
        &self,
        index_name: &Option<sqlparser::ast::ObjectName>,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::IndexColumn],
        include: &[sqlparser::ast::Ident],
        unique: bool,
        nulls_distinct: Option<bool>,
        index_options: &[sqlparser::ast::IndexOption],
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let index_name_str = index_name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| format!("idx_{}_{}", table_name_str, uuid::Uuid::new_v4()));
        let meta_key = index_meta_key_for_index(&index_name_str);
        if txn.get(meta_key.as_bytes()).await?.is_some() {
            return Err(FusionError::Execution(format!(
                "Index {} already exists",
                index_name_str
            )));
        }

        let schema_key = index_schema_key_for_table(&table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let mut schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let mut target_col_indices = Vec::with_capacity(columns.len());
        let mut target_col_names = Vec::with_capacity(columns.len());
        for index_col in columns {
            let col_expr = &index_col.column;
            if let Expr::Identifier(ident) = &col_expr.expr {
                if let Some(idx) = schema.get_column_index(&ident.value) {
                    target_col_indices.push(idx);
                    target_col_names.push(schema.columns[idx].name.clone());
                } else {
                    return Err(FusionError::Execution(format!(
                        "Column {} not found",
                        ident.value
                    )));
                }
            } else {
                return Err(FusionError::Execution(
                    "Index only supports simple column references".to_string(),
                ));
            }
        }

        if target_col_indices.is_empty() {
            return Err(FusionError::Execution(
                "CREATE INDEX requires at least one column".to_string(),
            ));
        }
        if unique && nulls_distinct == Some(false) {
            return Err(FusionError::Execution(
                "CREATE UNIQUE INDEX NULLS NOT DISTINCT is not supported".to_string(),
            ));
        }
        if unique && target_col_indices.len() == 1 {
            return Err(FusionError::Execution(
                "CREATE UNIQUE INDEX currently requires at least two columns; use a column or table UNIQUE constraint for one column"
                    .to_string(),
            ));
        }
        let mut seen_target_columns = HashSet::with_capacity(target_col_indices.len());
        for &column_idx in &target_col_indices {
            if !seen_target_columns.insert(column_idx) {
                return Err(FusionError::Execution(format!(
                    "Duplicate index column {}",
                    schema.columns[column_idx].name
                )));
            }
        }

        let mut index_type = IndexType::BTree;
        for opt in index_options {
            if let sqlparser::ast::IndexOption::Using(sqlparser::ast::IndexType::Custom(ident)) =
                opt
            {
                if ident.value.eq_ignore_ascii_case("FTS") {
                    index_type = IndexType::FTS;
                } else if ident.value.eq_ignore_ascii_case("HNSW") {
                    index_type = IndexType::HNSW;
                }
            }
        }

        if target_col_indices.len() != 1 && index_type != IndexType::BTree {
            return Err(FusionError::Execution(
                "Composite indexes only support BTree".to_string(),
            ));
        }

        if !include.is_empty() && index_type != IndexType::BTree {
            return Err(FusionError::Execution(
                "INCLUDE columns currently support BTree indexes only".to_string(),
            ));
        }
        if unique && index_type != IndexType::BTree {
            return Err(FusionError::Execution(
                "UNIQUE indexes only support BTree".to_string(),
            ));
        }
        let mut include_col_indices = Vec::with_capacity(include.len());
        let mut include_col_names = Vec::with_capacity(include.len());
        for ident in include {
            let idx = schema.get_column_index(&ident.value).ok_or_else(|| {
                FusionError::Execution(format!("Column {} not found", ident.value))
            })?;
            if target_col_indices.contains(&idx) || include_col_indices.contains(&idx) {
                return Err(FusionError::Execution(format!(
                    "Duplicate INCLUDE column {}",
                    ident.value
                )));
            }
            include_col_indices.push(idx);
            include_col_names.push(schema.columns[idx].name.clone());
        }
        self.reject_incompatible_shared_index_payload(
            &index_name_str,
            &table_name_str,
            &target_col_names,
            &include_col_names,
            txn,
        )
        .await?;

        if target_col_indices.len() == 1 {
            let col_idx = target_col_indices[0];
            let col_name = schema.columns[col_idx].name.clone();

            schema.columns[col_idx].is_indexed = true;
            schema.columns[col_idx].index_type = index_type.clone();

            // If HNSW, initialize the vector index
            if index_type == IndexType::HNSW {
                let idx_name = hnsw_index_name_for_column(&table_name_str, &col_name)?;
                self.vector_index.create_index(&idx_name);
            }
        }
        let new_schema_value = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("Schema serialization error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &new_schema_value).await?;

        let maintain_count_summary = target_col_indices.len() == 1
            && Self::index_count_summary_maintained_for_column(
                &schema.columns[target_col_indices[0]],
            );
        let mut index_count_summary = HashMap::new();
        let composite_meta = (target_col_indices.len() > 1).then(|| CompositeIndexMeta {
            name: index_name_str.clone(),
            table: table_name_str.clone(),
            columns: target_col_names.clone(),
            include_columns: include_col_names.clone(),
            ordered_encoding: true,
            is_unique: unique,
            unique_sentinel_authoritative: unique,
            is_primary: false,
        });
        let mut seen_unique_values = HashSet::new();
        self.touch_all_index_build_barriers(&table_name_str, txn)
            .await?;
        let kv_pairs = self
            .scan_routed_data_prefixes_for_table_with_options(
                &table_name_str,
                txn,
                None,
                self.bulk_scan_options(),
            )
            .await?;

        let mut count = 0;
        for (k, v) in kv_pairs {
            let key_str = std::str::from_utf8(&k)
                .map_err(|e| FusionError::Execution(format!("Data key decode error: {}", e)))?;
            let row_id = self.legacy_row_id_from_routed_data_key(&table_name_str, &k)?;

            let row = if target_col_indices.len() > 1 {
                let mut decode_indices = target_col_indices.clone();
                for &include_idx in &include_col_indices {
                    if !decode_indices.contains(&include_idx) {
                        decode_indices.push(include_idx);
                    }
                }
                crate::common::encoding::RowDecoder::decode_partial(&v, &decode_indices).map_err(
                    |e| FusionError::Execution(format!("Data deserialization error: {}", e)),
                )?
            } else {
                Vec::new()
            };

            if target_col_indices.len() > 1 {
                let unique_value_key = if unique {
                    let index = composite_meta.as_ref().expect("composite metadata");
                    self.composite_unique_value_key(index, &row, &schema)?
                } else {
                    None
                };
                if let Some(value_key) = unique_value_key.as_ref() {
                    if !seen_unique_values.insert(value_key.clone()) {
                        return Err(FusionError::Execution(format!(
                            "UNIQUE constraint violated for columns '{}'",
                            target_col_names.join(", ")
                        )));
                    }
                }
                if let Some(index_key) = self.composite_index_key(
                    &table_name_str,
                    &target_col_names,
                    &row,
                    &schema,
                    row_id,
                ) {
                    let payload = Self::secondary_index_payload_for_row(&row, &include_col_indices);
                    txn.put(index_key.as_bytes(), &payload).await?;
                    count += 1;
                }
                if let Some(value_key) = unique_value_key {
                    let index = composite_meta.as_ref().expect("composite metadata");
                    let sentinel_key = self.composite_unique_sentinel_key_for_value(
                        &table_name_str,
                        index,
                        &value_key,
                    );
                    txn.put(sentinel_key.as_bytes(), row_id.as_bytes()).await?;
                }
                continue;
            }

            let col_idx = target_col_indices[0];
            let col_name = &target_col_names[0];
            let (val, payload) = if include_col_indices.is_empty() {
                let val = if let Some(row) = self.row_cache_lookup(key_str, &v) {
                    row.get(col_idx).cloned()
                } else {
                    crate::common::encoding::RowDecoder::decode_column(&v, col_idx).map_err(
                        |e| FusionError::Execution(format!("Data deserialization error: {}", e)),
                    )?
                };
                (val, Vec::new())
            } else {
                let row = if let Some(row) = self.row_cache_lookup(key_str, &v) {
                    row
                } else {
                    crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                };
                let val = row.get(col_idx).cloned();
                let payload = Self::secondary_index_payload_for_row(&row, &include_col_indices);
                (val, payload)
            };
            let Some(val) = val else {
                continue;
            };

            if index_type == IndexType::FTS {
                if let Value::String(text) = &val {
                    for token in Self::tokenize_unique(text) {
                        let index_key = self.routed_fts_index_key_for_row(
                            &table_name_str,
                            col_name,
                            &token,
                            row_id,
                        );
                        txn.put(index_key.as_bytes(), &[]).await?;
                    }
                }
                self.update_trigram_index_for_value(&table_name_str, col_name, &val, row_id, txn);
            } else if index_type == IndexType::HNSW {
                if let Value::Vector(vec) = &val {
                    let idx_name = hnsw_index_name_for_column(&table_name_str, col_name)?;
                    self.defer_or_apply_vector_insert(
                        &idx_name,
                        row_id.to_string(),
                        vec.clone(),
                        txn,
                    )?;
                }
            } else {
                if let Some(val_str) = self.value_to_index_string(&val) {
                    let index_key = self.routed_index_key_for_value(
                        &table_name_str,
                        col_name,
                        &val_str,
                        row_id,
                    );
                    txn.put(index_key.as_bytes(), &payload).await?;
                    if maintain_count_summary {
                        *index_count_summary.entry(val_str).or_insert(0) += 1;
                    }
                } else {
                    continue;
                }
                self.update_trigram_index_for_value(&table_name_str, col_name, &val, row_id, txn);
            }
            count += 1;
        }

        if maintain_count_summary {
            self.replace_index_count_summary_for_column(
                &table_name_str,
                &target_col_names[0],
                &index_count_summary,
                txn,
            )
            .await?;
        }

        // Store index metadata for DROP INDEX support
        let meta_val = if target_col_names.len() == 1 {
            Self::single_column_index_meta_value_with_include(
                &table_name_str,
                &target_col_names[0],
                &include_col_names,
            )
        } else if unique {
            Self::composite_index_meta_value_with_include_and_unique(
                &table_name_str,
                &target_col_names,
                &include_col_names,
                true,
            )
        } else {
            Self::composite_index_meta_value_with_include(
                &table_name_str,
                &target_col_names,
                &include_col_names,
            )
        };
        txn.put(meta_key.as_bytes(), meta_val.as_bytes()).await?;
        if target_col_names.len() > 1 {
            self.rebuild_composite_index_directory_for_table(&table_name_str, txn)
                .await?;
        }

        Ok(QueryResult::Success {
            message: format!(
                "Index {} ({:?}) created on {}({}), indexed {} rows",
                index_name_str,
                index_type,
                table_name_str,
                target_col_names.join(", "),
                count
            ),
        })
    }

    pub(crate) async fn handle_drop_index(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let mut dropped = 0;
        for name in names {
            let index_name = name.to_string();
            // Index metadata key: index_meta:<index_name>
            let meta_key = index_meta_key_for_index(&index_name);
            if let Some(meta_bytes) = txn.get(meta_key.as_bytes()).await? {
                let meta_str = String::from_utf8(meta_bytes).unwrap_or_default();
                if let Some(meta) = Self::parse_index_meta(&index_name, &meta_str) {
                    let table_name = meta.table.clone();
                    self.touch_all_index_build_barriers(&table_name, txn)
                        .await?;
                    let entries_are_shared = self
                        .other_index_references_physical_entries(&index_name, &meta, txn)
                        .await?;

                    // Delete index entries
                    let index_entries = if entries_are_shared {
                        Vec::new()
                    } else if meta.columns.len() == 1 {
                        let mut entries = self
                            .scan_routed_prefixes(
                                self.routed_index_prefixes_for_column(
                                    &table_name,
                                    &meta.columns[0],
                                ),
                                txn,
                                None,
                            )
                            .await?;
                        let mut fts_entries = self
                            .scan_routed_prefixes(
                                self.routed_fts_prefixes_for_column(&table_name, &meta.columns[0]),
                                txn,
                                None,
                            )
                            .await?;
                        entries.append(&mut fts_entries);
                        entries
                    } else {
                        self.scan_routed_prefixes(
                            self.routed_composite_index_prefixes(&table_name, &meta.columns),
                            txn,
                            None,
                        )
                        .await?
                    };
                    for (k, _) in index_entries {
                        txn.delete(&k).await?;
                    }
                    if meta.is_unique && meta.columns.len() > 1 {
                        let sentinels = self
                            .scan_routed_prefixes(
                                self.composite_unique_sentinel_prefixes(&table_name, &meta),
                                txn,
                                None,
                            )
                            .await?;
                        for (key, _) in sentinels {
                            txn.delete(&key).await?;
                        }
                    }
                    if meta.columns.len() == 1 && !entries_are_shared {
                        self.delete_index_count_summary_for_column(
                            &table_name,
                            &meta.columns[0],
                            txn,
                        )
                        .await?;
                    }

                    // Update schema: mark column as not indexed
                    if meta.columns.len() == 1 && !entries_are_shared {
                        let schema_key = index_schema_key_for_table(&table_name);
                        if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                            if let Ok(mut schema) =
                                bincode::deserialize::<TableSchema>(&schema_bytes)
                            {
                                for col in &mut schema.columns {
                                    if col.name == meta.columns[0] {
                                        col.is_indexed = false;
                                        col.index_type = IndexType::None;
                                    }
                                }
                                let new_bytes = bincode::serialize(&schema).map_err(|e| {
                                    FusionError::Execution(format!("Serialize error: {}", e))
                                })?;
                                txn.put(schema_key.as_bytes(), &new_bytes).await?;
                            }
                        }
                    } else {
                        let table_meta_key =
                            Self::composite_index_table_meta_key(&table_name, &index_name);
                        txn.delete(table_meta_key.as_bytes()).await?;
                    }
                }
                txn.delete(meta_key.as_bytes()).await?;
                dropped += 1;
            } else if !if_exists {
                return Err(FusionError::Execution(format!(
                    "Index {} does not exist",
                    index_name
                )));
            }
        }

        Ok(QueryResult::Success {
            message: format!("Dropped {} index(es)", dropped),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        create_index_data_prefix_for_table, create_index_key_for_value,
        drop_index_prefix_for_column, hnsw_index_name_for_column, index_meta_key_for_index,
        index_schema_key_for_table,
    };

    #[test]
    fn index_schema_key_for_table_preallocates_exact_key() {
        let key = index_schema_key_for_table("orders");

        assert_eq!(key, "schema:orders");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn create_index_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = create_index_data_prefix_for_table("orders");

        assert_eq!(prefix, "data:orders:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn index_meta_key_for_index_preallocates_exact_key() {
        let key = index_meta_key_for_index("idx_orders_status");

        assert_eq!(key, "index_meta:idx_orders_status");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn create_index_key_for_value_preallocates_exact_key() {
        let key = create_index_key_for_value("orders", "status", "open", "00042");

        assert_eq!(key, "index:orders:status:open:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn drop_index_prefix_for_column_preallocates_exact_prefix() {
        let prefix = drop_index_prefix_for_column("orders", "status");

        assert_eq!(prefix, "index:orders:status:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn hnsw_index_name_for_column_uses_structured_identity() {
        let name = hnsw_index_name_for_column("docs", "embedding").unwrap();

        assert_eq!(name, "hnsw_v2_AEZEQksCBwAAAARkb2NzAAAACWVtYmVkZGluZw");
    }
}
