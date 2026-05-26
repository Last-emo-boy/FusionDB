use crate::catalog::TableSchema;
use crate::common::{Result, Value};

use super::super::{Executor, QueryResult};

impl Executor {
    pub(super) fn build_returning_result(
        &self,
        ret_items: &[sqlparser::ast::SelectItem],
        rows: &[Vec<Value>],
        schema: &TableSchema,
    ) -> Result<QueryResult> {
        use sqlparser::ast::SelectItem;
        let mut col_names = Vec::with_capacity(ret_items.len());
        let mut result_rows = Vec::with_capacity(rows.len());

        // Build column names from RETURNING items
        let is_wildcard = ret_items
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_)));
        if is_wildcard {
            col_names = schema.columns.iter().map(|c| c.name.clone()).collect();
            result_rows = rows.to_vec();
        } else {
            for item in ret_items {
                match item {
                    SelectItem::UnnamedExpr(expr) => col_names.push(format!("{}", expr)),
                    SelectItem::ExprWithAlias { alias, .. } => col_names.push(alias.value.clone()),
                    _ => {}
                }
            }
            for row in rows {
                let mut result_row = Vec::with_capacity(ret_items.len());
                for item in ret_items {
                    let expr = match item {
                        SelectItem::UnnamedExpr(e) => e,
                        SelectItem::ExprWithAlias { expr, .. } => expr,
                        _ => continue,
                    };
                    let val = self
                        .evaluate_value(expr, row, schema, &[])
                        .unwrap_or(Value::Null);
                    result_row.push(val);
                }
                result_rows.push(result_row);
            }
        }

        Ok(QueryResult::Select {
            columns: col_names,
            rows: result_rows,
        })
    }
}
