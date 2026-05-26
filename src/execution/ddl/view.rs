use crate::common::{FusionError, Result};
use crate::storage::Transaction;

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_create_view(
        &self,
        name: &sqlparser::ast::ObjectName,
        query: &sqlparser::ast::Query,
        or_replace: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        let view_name = name.to_string();
        let view_key = format!("view:{}", view_name);

        // Check if view already exists
        if txn.get(view_key.as_bytes()).await?.is_some() {
            if !or_replace {
                return Err(FusionError::Execution(format!(
                    "View {} already exists",
                    view_name
                )));
            }
        }

        // Store the query SQL as the view definition
        let view_sql = format!("{}", query);
        txn.put(view_key.as_bytes(), view_sql.as_bytes()).await?;

        Ok(QueryResult::Success {
            message: format!("View {} created", view_name),
        })
    }

    pub(crate) async fn handle_drop_view(
        &self,
        names: &[sqlparser::ast::ObjectName],
        if_exists: bool,
        txn: &mut dyn Transaction,
    ) -> Result<QueryResult> {
        for name in names {
            let view_name = name.to_string();
            let view_key = format!("view:{}", view_name);

            if txn.get(view_key.as_bytes()).await?.is_none() {
                if !if_exists {
                    return Err(FusionError::Execution(format!(
                        "View {} not found",
                        view_name
                    )));
                }
                continue;
            }

            txn.delete(view_key.as_bytes()).await?;
        }

        Ok(QueryResult::Success {
            message: "View(s) dropped".to_string(),
        })
    }
}
