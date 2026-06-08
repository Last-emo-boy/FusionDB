use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::{Executor, QueryResult};

const ANALYZE_DISTINCT_PREALLOC_LIMIT: usize = 1024;

fn analyze_distinct_capacity(row_count: usize) -> usize {
    row_count.min(ANALYZE_DISTINCT_PREALLOC_LIMIT)
}

fn analyze_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TableStats {
    pub table_name: String,
    pub row_count: usize,
    pub columns: Vec<ColumnStats>,
    pub updated_at_epoch_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ColumnStats {
    pub name: String,
    pub null_count: usize,
    pub distinct_count: usize,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

impl Executor {
    pub(crate) async fn handle_analyze(
        &self,
        analyze: &sqlparser::ast::Analyze,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name = analyze.table_name.to_string();
        let schema = self.load_analyze_schema(&table_name, txn).await?;
        let stats = self.collect_table_stats(&table_name, &schema, txn).await?;
        self.store_table_stats(&stats, txn).await?;

        Ok(QueryResult::Success {
            message: format!(
                "Analyzed table {}, {} rows, {} columns",
                table_name,
                stats.row_count,
                stats.columns.len()
            ),
        })
    }

    pub(crate) async fn load_table_stats(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<TableStats>> {
        let key = format!("stats:table:{}", table_name);
        let Some(bytes) = txn.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        let stats = bincode::deserialize::<TableStats>(&bytes)
            .map_err(|e| FusionError::Execution(format!("Stats deserialization error: {}", e)))?;
        Ok(Some(stats))
    }

    async fn store_table_stats(&self, stats: &TableStats, txn: &mut dyn Transaction) -> Result<()> {
        let key = format!("stats:table:{}", stats.table_name);
        let bytes = bincode::serialize(stats)
            .map_err(|e| FusionError::Execution(format!("Stats serialization error: {}", e)))?;
        txn.put(key.as_bytes(), &bytes).await
    }

    async fn collect_table_stats(
        &self,
        table_name: &str,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<TableStats> {
        let prefix = analyze_data_prefix_for_table(table_name);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
        let distinct_capacity = analyze_distinct_capacity(kv_pairs.len());
        let mut collectors = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            collectors.push(ColumnStatsCollector::new(
                column.name.clone(),
                distinct_capacity,
            ));
        }

        let mut row_count = 0usize;
        for (key, bytes) in kv_pairs {
            row_count += 1;
            let key_str = std::str::from_utf8(&key).ok();
            let row = if let Some(key_str) = key_str {
                if let Some(row) = self.row_cache.get(key_str) {
                    row
                } else {
                    crate::common::encoding::RowDecoder::decode(&bytes).map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                }
            } else {
                crate::common::encoding::RowDecoder::decode(&bytes).map_err(|e| {
                    FusionError::Execution(format!("Data deserialization error: {}", e))
                })?
            };

            for (idx, collector) in collectors.iter_mut().enumerate() {
                let value = row.get(idx).cloned().unwrap_or(Value::Null);
                collector.observe(value);
            }
        }

        let mut columns = Vec::with_capacity(collectors.len());
        for collector in collectors {
            columns.push(collector.finish());
        }

        Ok(TableStats {
            table_name: table_name.to_string(),
            row_count,
            columns,
            updated_at_epoch_ms: Self::current_epoch_ms(),
        })
    }

    async fn load_analyze_schema(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<TableSchema> {
        let schema_key = format!("schema:{}", table_name);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name)))?;
        bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))
    }
}

struct ColumnStatsCollector {
    name: String,
    null_count: usize,
    distinct: HashSet<Value>,
    min: Option<Value>,
    max: Option<Value>,
}

impl ColumnStatsCollector {
    fn new(name: String, distinct_capacity: usize) -> Self {
        Self {
            name,
            null_count: 0,
            distinct: HashSet::with_capacity(distinct_capacity),
            min: None,
            max: None,
        }
    }

    fn observe(&mut self, value: Value) {
        if value == Value::Null {
            self.null_count += 1;
            return;
        }

        if matches!(
            value,
            Value::Boolean(_) | Value::Integer(_) | Value::Float(_) | Value::String(_)
        ) {
            self.distinct.insert(value.clone());
            if self
                .min
                .as_ref()
                .is_none_or(|current| value.compare(current).is_lt())
            {
                self.min = Some(value.clone());
            }
            if self
                .max
                .as_ref()
                .is_none_or(|current| value.compare(current).is_gt())
            {
                self.max = Some(value);
            }
        }
    }

    fn finish(self) -> ColumnStats {
        ColumnStats {
            name: self.name,
            null_count: self.null_count,
            distinct_count: self.distinct.len(),
            min: self.min,
            max: self.max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyze_distinct_capacity_is_bounded() {
        assert_eq!(analyze_distinct_capacity(0), 0);
        assert_eq!(analyze_distinct_capacity(3), 3);
        assert_eq!(
            analyze_distinct_capacity(ANALYZE_DISTINCT_PREALLOC_LIMIT + 1),
            ANALYZE_DISTINCT_PREALLOC_LIMIT
        );
    }

    #[test]
    fn analyze_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = analyze_data_prefix_for_table("items");

        assert_eq!(prefix, "data:items:");
        assert!(prefix.capacity() >= prefix.len());
    }
}
