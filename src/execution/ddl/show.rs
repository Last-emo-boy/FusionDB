use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;

use super::super::{Executor, QueryResult};

fn show_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

impl Executor {
    pub(crate) fn show_all_settings_result() -> QueryResult {
        let settings = [
            (
                "application_name",
                "",
                "Sets the application name to be reported in statistics and logs.",
            ),
            (
                "client_encoding",
                "UTF8",
                "Sets the client's character set encoding.",
            ),
            (
                "DateStyle",
                "ISO, MDY",
                "Sets the display format for date and time values.",
            ),
            (
                "default_transaction_isolation",
                "read committed",
                "Sets the transaction isolation level of each new transaction.",
            ),
            (
                "integer_datetimes",
                "on",
                "Reports whether datetimes use integer storage.",
            ),
            (
                "max_index_keys",
                "32",
                "Shows the maximum number of index columns.",
            ),
            ("search_path", "public", "Sets the schema search order."),
            (
                "server_encoding",
                "UTF8",
                "Shows the server character set encoding.",
            ),
            ("server_version", "15.0", "Shows the server version."),
            (
                "TimeZone",
                "UTC",
                "Sets the time zone for displaying and interpreting time stamps.",
            ),
        ];
        let mut rows = Vec::with_capacity(settings.len());
        for (name, setting, description) in settings {
            rows.push(vec![
                Value::String(name.to_string()),
                Value::String(setting.to_string()),
                Value::String(description.to_string()),
            ]);
        }

        QueryResult::Select {
            columns: vec![
                "name".to_string(),
                "setting".to_string(),
                "description".to_string(),
            ],
            rows,
        }
    }

    pub(crate) async fn handle_describe_table(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let schema_key = show_schema_key_for_table(&table_name_str);
        if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
            let schema: TableSchema = bincode::deserialize(&schema_bytes)
                .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

            let mut rows = Vec::with_capacity(schema.columns.len());
            for col in schema.columns {
                rows.push(vec![
                    Value::String(col.name),
                    Value::String(col.data_type),
                    Value::String(if col.is_primary {
                        "PRI".to_string()
                    } else {
                        "".to_string()
                    }),
                    Value::String(if col.is_indexed {
                        format!("{:?}", col.index_type)
                    } else {
                        "".to_string()
                    }),
                ]);
            }

            Ok(QueryResult::Select {
                columns: vec![
                    "Field".to_string(),
                    "Type".to_string(),
                    "Key".to_string(),
                    "Index".to_string(),
                ],
                rows,
            })
        } else {
            Err(FusionError::Execution(format!(
                "Table {} not found",
                table_name_str
            )))
        }
    }

    pub(crate) async fn handle_show_create_table(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let table_name_str = table_name.to_string();
        let schema_key = show_schema_key_for_table(&table_name_str);
        let schema_bytes = txn
            .get(schema_key.as_bytes())
            .await?
            .ok_or_else(|| FusionError::Execution(format!("Table {} not found", table_name_str)))?;
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

        let mut ddl = format!("CREATE TABLE {} (\n", table_name_str);
        for (i, col) in schema.columns.iter().enumerate() {
            ddl.push_str(&format!("  {} {}", col.name, col.data_type));
            if col.is_primary {
                ddl.push_str(" PRIMARY KEY");
            }
            if i < schema.columns.len() - 1 {
                ddl.push(',');
            }
            ddl.push('\n');
        }
        ddl.push_str(");");

        Ok(QueryResult::Select {
            columns: vec!["Table".to_string(), "Create Table".to_string()],
            rows: vec![vec![Value::String(table_name_str), Value::String(ddl)]],
        })
    }

    pub(crate) async fn handle_show_tables(
        &self,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let prefix = "schema:";
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut tables = Vec::with_capacity(kv_pairs.len());
        for (k, _) in kv_pairs {
            if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(table_name) = key_str.strip_prefix(prefix) {
                    tables.push(vec![Value::String(table_name.to_string())]);
                }
            }
        }

        Ok(QueryResult::Select {
            columns: vec!["Table".to_string()],
            rows: tables,
        })
    }

    pub(crate) async fn handle_show_views(&self, txn: &mut dyn Transaction) -> Result<QueryResult> {
        let prefix = "view:";
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut views = Vec::with_capacity(kv_pairs.len());
        for (k, v) in kv_pairs {
            if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(view_name) = key_str.strip_prefix(prefix) {
                    let definition = String::from_utf8(v).unwrap_or_default();
                    views.push(vec![
                        Value::String(view_name.to_string()),
                        Value::String(definition),
                    ]);
                }
            }
        }

        Ok(QueryResult::Select {
            columns: vec!["View".to_string(), "Definition".to_string()],
            rows: views,
        })
    }

    pub(crate) async fn handle_show_indexes(
        &self,
        table_filter: Option<&str>,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let prefix = "index_meta:";
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;

        let mut indexes = Vec::with_capacity(kv_pairs.len());
        for (k, v) in kv_pairs {
            let Ok(key_str) = std::str::from_utf8(&k) else {
                continue;
            };
            let Some(index_name) = key_str.strip_prefix(prefix) else {
                continue;
            };

            let meta_str = String::from_utf8(v).unwrap_or_default();
            let Some((table_name, column_name, _columns)) = Self::describe_index_columns(&meta_str)
            else {
                continue;
            };
            if table_filter.is_some_and(|filter| filter != table_name) {
                continue;
            }

            indexes.push(vec![
                Value::String(index_name.to_string()),
                Value::String(table_name),
                Value::String(column_name),
            ]);
        }

        indexes.sort_by(|left, right| {
            let Value::String(left_name) = &left[0] else {
                return std::cmp::Ordering::Equal;
            };
            let Value::String(right_name) = &right[0] else {
                return std::cmp::Ordering::Equal;
            };
            left_name.cmp(right_name)
        });

        Ok(QueryResult::Select {
            columns: vec![
                "Index".to_string(),
                "Table".to_string(),
                "Column".to_string(),
            ],
            rows: indexes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_schema_key_for_table_preallocates_exact_key() {
        let key = show_schema_key_for_table("items");

        assert_eq!(key, "schema:items");
        assert!(key.capacity() >= key.len());
    }
}
