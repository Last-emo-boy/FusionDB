use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    BinaryOperator, Expr, LimitClause, OrderByKind, SetExpr, Statement, TableFactor, TableWithJoins,
};
use std::collections::HashSet;
use std::time::Duration;

use super::super::analyze::TableStats;
use super::super::stats::StatsEstimator;
use super::super::{Executor, QueryResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExplainAccessKind {
    FullTableScan,
    PrimaryKeyLookup,
    PrimaryKeyRangeScan,
    IndexScan,
    Unknown,
}

#[derive(Debug, Clone)]
struct ExplainAccessPath {
    description: String,
    kind: ExplainAccessKind,
}

impl ExplainAccessPath {
    fn new(description: impl Into<String>, kind: ExplainAccessKind) -> Self {
        Self {
            description: description.into(),
            kind,
        }
    }
}

#[cfg(test)]
fn explain_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn explain_schema_key_for_table(table_name: &str) -> String {
    let mut key = String::with_capacity("schema:".len() + table_name.len());
    key.push_str("schema:");
    key.push_str(table_name);
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, IndexType};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::Storage;
    use sqlparser::ast::Ident;
    use std::sync::Arc;

    fn test_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: "INTEGER".to_string(),
            is_primary: false,
            is_indexed: false,
            index_type: IndexType::None,
            default_value: None,
            is_nullable: true,
            is_unique: false,
            check_expr: None,
        }
    }

    fn test_executor() -> (Executor, String) {
        let wal_path = format!("test_explain_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
        (Executor::new(storage), wal_path)
    }

    fn compound(name: &str, column: &str) -> Expr {
        Expr::CompoundIdentifier(vec![Ident::new(name), Ident::new(column)])
    }

    #[test]
    fn explain_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = explain_data_prefix_for_table("lineitem");

        assert_eq!(prefix, "data:lineitem:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn explain_schema_key_for_table_preallocates_exact_key() {
        let key = explain_schema_key_for_table("lineitem");

        assert_eq!(key, "schema:lineitem");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn predicate_schema_members_for_explain_tracks_local_and_join_predicates() {
        let (executor, wal_path) = test_executor();
        let schemas = vec![
            TableSchema::new(
                "a".to_string(),
                vec![test_column("a.id"), test_column("a.name")],
            ),
            TableSchema::new(
                "b".to_string(),
                vec![test_column("b.id"), test_column("b.a_id")],
            ),
        ];

        assert_eq!(
            executor.predicate_schema_members_for_explain(&compound("a", "id"), &schemas),
            Some(vec![0])
        );

        let join_predicate = Expr::BinaryOp {
            left: Box::new(compound("a", "id")),
            op: BinaryOperator::Eq,
            right: Box::new(compound("b", "a_id")),
        };
        assert_eq!(
            executor.predicate_schema_members_for_explain(&join_predicate, &schemas),
            Some(vec![0, 1])
        );
        drop(executor);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn explain_q_error_label_handles_zero_and_missing_estimates() {
        assert_eq!(Executor::explain_q_error_label(None, 3), "n/a");
        assert_eq!(Executor::explain_q_error_label(Some(0), 0), "1.00");
        assert_eq!(Executor::explain_q_error_label(Some(0), 5), "inf");
        assert_eq!(Executor::explain_q_error_label(Some(2), 3), "1.50");
        assert_eq!(Executor::explain_q_error_label(Some(9), 3), "3.00");
    }

    #[test]
    fn explain_apply_limit_clause_to_estimate_handles_offset_and_limit() {
        let query = crate::parser::parse_sql("SELECT * FROM users LIMIT 2 OFFSET 3")
            .expect("parse")
            .remove(0);
        let Statement::Query(query) = query else {
            panic!("expected query");
        };

        assert_eq!(
            Executor::explain_apply_limit_clause_to_estimate(Some(10), &query.limit_clause),
            Some(2)
        );
        assert_eq!(
            Executor::explain_apply_limit_clause_to_estimate(Some(4), &query.limit_clause),
            Some(1)
        );
        assert_eq!(
            Executor::explain_apply_limit_clause_to_estimate(Some(3), &query.limit_clause),
            Some(0)
        );
    }
}

#[derive(Debug, Clone, Copy)]
struct ExplainEstimate {
    rows: usize,
    cost: f64,
}

#[derive(Debug, Clone)]
struct ExplainJoinRelation {
    table: TableWithJoins,
    schema: TableSchema,
    stats: Option<TableStats>,
    base_rows: usize,
    estimated_rows: usize,
}

#[derive(Debug, Clone, Copy)]
struct ExplainJoinEdge {
    left_column: usize,
    right_column: usize,
}

#[derive(Debug, Clone)]
struct ExplainJoinOrder {
    order: Vec<String>,
    rows: usize,
    cost: f64,
}

impl Executor {
    pub(crate) async fn handle_explain(
        &self,
        stmt: &Statement,
        analyze: bool,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        if analyze {
            let planning_start = std::time::Instant::now();
            let plan = self.explain_statement_plan(stmt, txn).await?;
            let estimated_rows = self.explain_statement_estimate_rows(stmt, txn).await?;
            let planning_duration = planning_start.elapsed();

            let execution_start = std::time::Instant::now();
            let result =
                Box::pin(self.execute_in_transaction_with_params(stmt, txn, params)).await?;
            let execution_duration = execution_start.elapsed();
            let actual_rows = Self::explain_actual_rows(&result);

            let output = format!(
                "Planning Time: {:.3} ms\nExecution Time: {:.3} ms\nActual Rows: {}\nEstimate Rows: {}\nQ-Error: {}\nPlan:\n{}",
                Self::explain_duration_ms(planning_duration),
                Self::explain_duration_ms(execution_duration),
                actual_rows,
                Self::explain_estimate_rows_label(estimated_rows),
                Self::explain_q_error_label(estimated_rows, actual_rows),
                plan
            );

            Ok(QueryResult::Select {
                columns: vec!["EXPLAIN ANALYZE".to_string()],
                rows: vec![vec![Value::String(output)]],
            })
        } else {
            let plan = self.explain_statement_plan(stmt, txn).await?;
            Ok(QueryResult::Select {
                columns: vec!["EXPLAIN".to_string()],
                rows: vec![vec![Value::String(plan)]],
            })
        }
    }

    fn explain_duration_ms(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1000.0
    }

    fn explain_actual_rows(result: &QueryResult) -> usize {
        match result {
            QueryResult::Select { rows, .. } => rows.len(),
            QueryResult::Success { .. } => 0,
        }
    }

    fn explain_estimate_rows_label(estimated_rows: Option<usize>) -> String {
        estimated_rows
            .map(|rows| rows.to_string())
            .unwrap_or_else(|| "n/a".to_string())
    }

    fn explain_q_error_label(estimated_rows: Option<usize>, actual_rows: usize) -> String {
        let Some(estimated_rows) = estimated_rows else {
            return "n/a".to_string();
        };
        match (estimated_rows, actual_rows) {
            (0, 0) => "1.00".to_string(),
            (0, _) | (_, 0) => "inf".to_string(),
            (estimate, actual) => {
                let estimate = estimate as f64;
                let actual = actual as f64;
                format!("{:.2}", (estimate / actual).max(actual / estimate))
            }
        }
    }

    async fn explain_statement_estimate_rows(
        &self,
        stmt: &Statement,
        txn: &mut dyn Transaction,
    ) -> Result<Option<usize>> {
        match stmt {
            Statement::Query(query) => self.explain_query_estimate_rows(query, txn).await,
            _ => Ok(None),
        }
    }

    async fn explain_query_estimate_rows(
        &self,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<Option<usize>> {
        let SetExpr::Select(select) = &query.body.as_ref() else {
            return Ok(None);
        };

        let inner_join_order_input =
            Self::inner_join_chain_as_comma_join(&select.from, &select.selection);
        let (join_order_from, join_order_selection) =
            if let Some((flattened, combined_selection)) = &inner_join_order_input {
                (flattened.as_slice(), combined_selection)
            } else {
                (select.from.as_slice(), &select.selection)
            };

        if let Some(join_order) = self
            .explain_comma_join_order(join_order_from, join_order_selection, txn)
            .await?
        {
            return Ok(Self::explain_apply_limit_clause_to_estimate(
                Some(join_order.rows),
                &query.limit_clause,
            ));
        }

        let Some(table) = select.from.first() else {
            return Ok(None);
        };
        let access_path = self
            .explain_select_table_access(&table.relation, select, query, txn)
            .await?;
        let estimated_rows = self
            .explain_table_estimate(&table.relation, &select.selection, &access_path, txn)
            .await?
            .map(|estimate| estimate.rows);
        Ok(Self::explain_apply_limit_clause_to_estimate(
            estimated_rows,
            &query.limit_clause,
        ))
    }

    fn explain_apply_limit_clause_to_estimate(
        estimated_rows: Option<usize>,
        limit_clause: &Option<LimitClause>,
    ) -> Option<usize> {
        let mut rows = estimated_rows?;
        let Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) = limit_clause
        else {
            return Some(rows);
        };
        if !limit_by.is_empty() {
            return Some(rows);
        }

        let offset = offset
            .as_ref()
            .and_then(|offset| Self::explain_usize_literal(&offset.value))
            .unwrap_or(0);
        rows = rows.saturating_sub(offset);

        if let Some(limit) = limit.as_ref().and_then(Self::explain_usize_literal) {
            rows = rows.min(limit);
        }

        Some(rows)
    }

    fn explain_usize_literal(expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Number(value, _),
                ..
            }) => value.parse::<usize>().ok(),
            Expr::Nested(inner) => Self::explain_usize_literal(inner),
            _ => None,
        }
    }

    async fn explain_statement_plan(
        &self,
        stmt: &Statement,
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        match stmt {
            Statement::Query(query) => self.explain_query(query, txn).await,
            _ => Ok(format!(
                "Statement type not supported for detailed explanation: {}",
                stmt
            )),
        }
    }

    async fn explain_query(
        &self,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        if let SetExpr::Select(select) = &query.body.as_ref() {
            let mut plan = String::new();
            plan.push_str("SELECT\n");

            if let Some(table) = select.from.first() {
                plan.push_str(&format!("  FROM: {}\n", table.relation));
                let access_path = self
                    .explain_select_table_access(&table.relation, select, query, txn)
                    .await?;
                plan.push_str(&format!("  Access Path: {}\n", access_path.description));
                if let Some(estimate) = self
                    .explain_table_estimate(&table.relation, &select.selection, &access_path, txn)
                    .await?
                {
                    plan.push_str(&format!(
                        "  Estimate: rows={}, cost={:.2}\n",
                        estimate.rows, estimate.cost
                    ));
                }
                if let Some(stats) = self.explain_table_stats(&table.relation, txn).await? {
                    plan.push_str(&format!("  Stats: {}\n", stats));
                }

                for join in &table.joins {
                    plan.push_str(&format!("  JOIN: {}\n", join.relation));
                    let join_access = self
                        .explain_table_access(&join.relation, &None, txn)
                        .await?;
                    plan.push_str(&format!("    Access Path: {}\n", join_access.description));
                    if let Some(estimate) = self
                        .explain_table_estimate(&join.relation, &None, &join_access, txn)
                        .await?
                    {
                        plan.push_str(&format!(
                            "    Estimate: rows={}, cost={:.2}\n",
                            estimate.rows, estimate.cost
                        ));
                    }
                    if let Some(stats) = self.explain_table_stats(&join.relation, txn).await? {
                        plan.push_str(&format!("    Stats: {}\n", stats));
                    }
                    plan.push_str(&format!("    Operator: {:?}\n", join.join_operator));
                }
            }

            let inner_join_order_input =
                Self::inner_join_chain_as_comma_join(&select.from, &select.selection);
            let (join_order_from, join_order_selection) =
                if let Some((flattened, combined_selection)) = &inner_join_order_input {
                    (flattened.as_slice(), combined_selection)
                } else {
                    (select.from.as_slice(), &select.selection)
                };

            if let Some(join_order) = self
                .explain_comma_join_order(join_order_from, join_order_selection, txn)
                .await?
            {
                plan.push_str(&format!(
                    "  Join Order: {}\n",
                    join_order.order.join(" -> ")
                ));
                plan.push_str(&format!(
                    "  Join Estimate: rows={}, cost={:.2}\n",
                    join_order.rows, join_order.cost
                ));
            }

            if let Some(selection) = &select.selection {
                plan.push_str(&format!("  Filter: {}\n", selection));
            }

            if matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if !exprs.is_empty())
            {
                plan.push_str(&format!("  Group By: {}\n", select.group_by));
            }

            if let Some(order_by) = &query.order_by {
                plan.push_str(&format!("  Order By: {}\n", order_by));
            }

            if let Some(limit) = &query.limit_clause {
                plan.push_str(&format!("  Limit: {}\n", limit));
            }

            Ok(plan)
        } else {
            Ok("Complex query (Set Operations?)".to_string())
        }
    }

    async fn explain_select_table_access(
        &self,
        table: &TableFactor,
        select: &sqlparser::ast::Select,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<ExplainAccessPath> {
        let access_path = self
            .explain_table_access(table, &select.selection, txn)
            .await?;
        if select.selection.is_some() {
            if let Some(ordered_composite_access) = self
                .explain_ordered_composite_index_access(table, select, query, txn)
                .await?
            {
                return Ok(ordered_composite_access);
            }
        }
        if access_path.kind != ExplainAccessKind::FullTableScan || select.selection.is_some() {
            return Ok(access_path);
        }

        if let Some(group_access) = self
            .explain_group_by_secondary_index_access(table, select, query, txn)
            .await?
        {
            return Ok(group_access);
        }

        if let Some(distinct_access) = self
            .explain_distinct_secondary_index_access(table, select, query, txn)
            .await?
        {
            return Ok(distinct_access);
        }

        if let Some(ordered_access) = self
            .explain_ordered_secondary_index_access(table, select, query, txn)
            .await?
        {
            return Ok(ordered_access);
        }

        Ok(access_path)
    }

    async fn explain_group_by_secondary_index_access(
        &self,
        table: &TableFactor,
        select: &sqlparser::ast::Select,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExplainAccessPath>> {
        if self.shard_router.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.selection.is_some()
            || select.having.is_some()
            || select.distinct.is_some()
        {
            return Ok(None);
        }

        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return Ok(None);
        };
        if group_exprs.is_empty() {
            return Ok(None);
        }

        let TableFactor::Table { name, .. } = table else {
            return Ok(None);
        };
        let table_name = name.to_string();
        let Some(schema) = self.load_explain_schema(&table_name, txn).await? else {
            return Ok(None);
        };

        let Some((column_index, group_name, count_name)) =
            Self::simple_group_by_count_projection(&select.projection, group_exprs, &schema)
        else {
            return Ok(None);
        };
        let columns = vec![group_name, count_name];
        if !Self::simple_order_limit_supported(&columns, query.order_by.as_ref()) {
            return Ok(None);
        }

        let Some(column) = schema.columns.get(column_index) else {
            return Ok(None);
        };
        if column.is_primary
            || !column.is_indexed
            || column.index_type != IndexType::BTree
            || column.is_nullable
            || !Self::secondary_index_group_key_type_supported(&column.data_type)
        {
            return Ok(None);
        }
        if self
            .index_count_summary_available(&table_name, &column.name, txn)
            .await?
        {
            return Ok(Some(ExplainAccessPath::new(
                format!(
                    "Index Scan using group secondary BTree on {} (GROUP BY COUNT summary index)",
                    column.name
                ),
                ExplainAccessKind::IndexScan,
            )));
        }
        if !self
            .group_by_count_index_key_scan_cost_allowed(&table_name, column_index, &schema, txn)
            .await?
        {
            return Ok(None);
        }

        Ok(Some(ExplainAccessPath::new(
            format!(
                "Index Scan using group secondary BTree on {} (GROUP BY COUNT key stream)",
                column.name
            ),
            ExplainAccessKind::IndexScan,
        )))
    }

    async fn explain_distinct_secondary_index_access(
        &self,
        table: &TableFactor,
        select: &sqlparser::ast::Select,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExplainAccessPath>> {
        if self.shard_router.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.selection.is_some()
            || select.having.is_some()
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
        {
            return Ok(None);
        }

        let TableFactor::Table { name, alias, .. } = table else {
            return Ok(None);
        };
        let table_name = name.to_string();
        let Some(schema) = self.load_explain_schema(&table_name, txn).await? else {
            return Ok(None);
        };

        let mut qualifiers = Vec::with_capacity(2);
        qualifiers.push(table_name);
        if let Some(alias) = alias {
            qualifiers.push(alias.name.value.clone());
        }

        if select.distinct.is_some() {
            let Some((column_index, output_name)) =
                Self::single_column_distinct_projection(&select.projection, &schema)
            else {
                return Ok(None);
            };
            let columns = vec![output_name];
            if !Self::simple_order_limit_supported(&columns, query.order_by.as_ref()) {
                return Ok(None);
            }
            let Some(column) = schema.columns.get(column_index) else {
                return Ok(None);
            };
            if column.is_primary
                || !column.is_indexed
                || column.index_type != IndexType::BTree
                || column.is_nullable
                || !Self::secondary_index_order_type_supported(&column.data_type)
            {
                return Ok(None);
            }

            let access_mode = if Self::secondary_index_loose_key_type_supported(&column.data_type) {
                "DISTINCT loose key seek"
            } else {
                "DISTINCT key stream"
            };
            return Ok(Some(ExplainAccessPath::new(
                format!(
                    "Index Scan using distinct secondary BTree on {} ({})",
                    column.name, access_mode
                ),
                ExplainAccessKind::IndexScan,
            )));
        }

        if query.order_by.is_some() || query.limit_clause.is_some() {
            return Ok(None);
        }

        let Some((column_index, _output_name)) =
            Self::count_distinct_projection(&select.projection, &schema, Some(&qualifiers))
        else {
            return Ok(None);
        };
        let Some(column) = schema.columns.get(column_index) else {
            return Ok(None);
        };
        if column.is_primary
            || !column.is_indexed
            || column.index_type != IndexType::BTree
            || !Self::secondary_index_distinct_key_type_supported(&column.data_type)
        {
            return Ok(None);
        }

        let access_mode = if Self::secondary_index_loose_key_type_supported(&column.data_type) {
            "COUNT DISTINCT loose key seek"
        } else {
            "COUNT DISTINCT key stream"
        };
        Ok(Some(ExplainAccessPath::new(
            format!(
                "Index Scan using distinct secondary BTree on {} ({})",
                column.name, access_mode
            ),
            ExplainAccessKind::IndexScan,
        )))
    }

    async fn explain_ordered_secondary_index_access(
        &self,
        table: &TableFactor,
        select: &sqlparser::ast::Select,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExplainAccessPath>> {
        if self.shard_router.is_some()
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.having.is_some()
            || select.distinct.is_some()
            || self.projection_contains_aggregate_or_window(&select.projection)
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
            || !self.order_by_allows_streaming_topk(query.order_by.as_ref(), &select.projection)
        {
            return Ok(None);
        }

        let Some(ordered_limit) = Self::explain_streaming_order_limit(&query.limit_clause) else {
            return Ok(None);
        };
        let Some(order_by) = &query.order_by else {
            return Ok(None);
        };
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Ok(None);
        };
        let [order_expr] = exprs.as_slice() else {
            return Ok(None);
        };
        let order_asc = order_expr.options.asc.unwrap_or(true);
        if !order_asc && !txn.supports_bounded_scan_range_reverse() {
            return Ok(None);
        }
        let TableFactor::Table { name, .. } = table else {
            return Ok(None);
        };
        let table_name = name.to_string();
        let Some(schema) = self.load_explain_schema(&table_name, txn).await? else {
            return Ok(None);
        };
        let Some(order_col) = Self::order_limit_column_name(&order_expr.expr) else {
            return Ok(None);
        };
        let Ok(col_idx) = self.resolve_column_index(&order_col, &schema) else {
            return Ok(None);
        };
        let Some(col) = schema.columns.get(col_idx) else {
            return Ok(None);
        };
        if col.is_primary
            || !col.is_indexed
            || col.index_type != IndexType::BTree
            || col.is_nullable
            || !Self::secondary_index_order_type_supported(&col.data_type)
        {
            return Ok(None);
        }

        Ok(Some(ExplainAccessPath::new(
            format!(
                "Index Scan using ordered secondary BTree on {} {} (ORDER BY/LIMIT, rows <= {})",
                col.name,
                if order_asc { "ASC" } else { "DESC" },
                ordered_limit
            ),
            ExplainAccessKind::IndexScan,
        )))
    }

    async fn explain_ordered_composite_index_access(
        &self,
        table: &TableFactor,
        select: &sqlparser::ast::Select,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExplainAccessPath>> {
        if select.from.len() != 1
            || !select.from[0].joins.is_empty()
            || select.having.is_some()
            || select.distinct.is_some()
            || self.projection_contains_aggregate_or_window(&select.projection)
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
            || !self.order_by_allows_streaming_topk(query.order_by.as_ref(), &select.projection)
        {
            return Ok(None);
        }

        let Some(selection) = &select.selection else {
            return Ok(None);
        };
        let Some(ordered_limit) = Self::explain_streaming_order_limit(&query.limit_clause) else {
            return Ok(None);
        };
        let Some(order_by) = &query.order_by else {
            return Ok(None);
        };
        let TableFactor::Table { name, .. } = table else {
            return Ok(None);
        };
        let table_name = name.to_string();
        let Some(schema) = self.load_explain_schema(&table_name, txn).await? else {
            return Ok(None);
        };

        let Some(access) = self
            .composite_ordered_index_for_explain(
                selection,
                &table_name,
                &schema,
                txn,
                Some(order_by),
                ordered_limit,
            )
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(ExplainAccessPath::new(
            format!(
                "Index Scan using ordered composite BTree {} on {} {} (ORDER BY/LIMIT, rows <= {})",
                access.index_name,
                access.order_column,
                if access.ascending { "ASC" } else { "DESC" },
                access.row_limit
            ),
            ExplainAccessKind::IndexScan,
        )))
    }

    fn explain_streaming_order_limit(limit_clause: &Option<LimitClause>) -> Option<usize> {
        let LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } = limit_clause.as_ref()?
        else {
            return None;
        };
        if !limit_by.is_empty() {
            return None;
        }
        let limit = limit.as_ref().and_then(Self::explain_usize_literal)?;
        let offset = offset
            .as_ref()
            .and_then(|offset| Self::explain_usize_literal(&offset.value))
            .unwrap_or(0);
        Some(limit.saturating_add(offset))
    }

    async fn explain_table_access(
        &self,
        table: &TableFactor,
        selection: &Option<Expr>,
        txn: &mut dyn Transaction,
    ) -> Result<ExplainAccessPath> {
        if let TableFactor::Table { name, .. } = table {
            let table_name = name.to_string();
            let schema_key = explain_schema_key_for_table(&table_name);
            if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                let schema: TableSchema = bincode::deserialize(&schema_bytes)
                    .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

                if let Some(sel) = selection {
                    if let Some(index_name) = self
                        .composite_index_for_explain(sel, &table_name, txn)
                        .await?
                    {
                        return Ok(ExplainAccessPath::new(
                            format!("Index Scan using {}", index_name),
                            ExplainAccessKind::IndexScan,
                        ));
                    }

                    if self.explain_primary_key_range_selection(sel, &schema) {
                        return Ok(ExplainAccessPath::new(
                            "Primary Key Range Scan (Clustered Index)",
                            ExplainAccessKind::PrimaryKeyRangeScan,
                        ));
                    }

                    if let Expr::BinaryOp { left, op, right } = sel {
                        if *op == BinaryOperator::Eq {
                            if let Some(pk_idx) = schema.get_primary_key_index() {
                                if (self.explain_column_index(left, &schema) == Some(pk_idx)
                                    && !self.explain_expr_has_column_reference(right))
                                    || (self.explain_column_index(right, &schema) == Some(pk_idx)
                                        && !self.explain_expr_has_column_reference(left))
                                {
                                    return Ok(ExplainAccessPath::new(
                                        "Primary Key Lookup (Clustered Index)",
                                        ExplainAccessKind::PrimaryKeyLookup,
                                    ));
                                }
                            }
                        }
                    }

                    let mut used_index = None;
                    self.check_index_usage(sel, &schema, &mut used_index);

                    if let Some(idx_info) = used_index {
                        return Ok(ExplainAccessPath::new(
                            format!("Index Scan using {}", idx_info),
                            ExplainAccessKind::IndexScan,
                        ));
                    }
                }

                Ok(ExplainAccessPath::new(
                    "Full Table Scan",
                    ExplainAccessKind::FullTableScan,
                ))
            } else {
                Ok(ExplainAccessPath::new(
                    "Table not found",
                    ExplainAccessKind::Unknown,
                ))
            }
        } else {
            Ok(ExplainAccessPath::new(
                "Unknown Table Factor",
                ExplainAccessKind::Unknown,
            ))
        }
    }

    async fn explain_table_estimate(
        &self,
        table: &TableFactor,
        selection: &Option<Expr>,
        access_path: &ExplainAccessPath,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExplainEstimate>> {
        let TableFactor::Table { name, .. } = table else {
            return Ok(None);
        };

        let table_name = name.to_string();
        let Some(stats) = self.load_table_stats(&table_name, txn).await? else {
            return Ok(None);
        };
        let Some(schema) = self.load_explain_schema(&table_name, txn).await? else {
            return Ok(None);
        };

        let rows = selection
            .as_ref()
            .and_then(|expr| self.estimate_predicate_rows(expr, &schema, &stats))
            .unwrap_or(stats.row_count);
        let cost = Self::estimate_access_cost(access_path.kind, stats.row_count, rows);

        Ok(Some(ExplainEstimate { rows, cost }))
    }

    async fn load_explain_schema(
        &self,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<TableSchema>> {
        let schema_key = explain_schema_key_for_table(table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return Ok(None);
        };
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;
        Ok(Some(schema))
    }

    fn estimate_access_cost(
        kind: ExplainAccessKind,
        table_rows: usize,
        estimated_rows: usize,
    ) -> f64 {
        let total = table_rows.max(1) as f64;
        let startup = total.log2().max(1.0);
        let rows = estimated_rows as f64;

        match kind {
            ExplainAccessKind::PrimaryKeyLookup => 1.0,
            ExplainAccessKind::PrimaryKeyRangeScan | ExplainAccessKind::IndexScan => startup + rows,
            ExplainAccessKind::FullTableScan | ExplainAccessKind::Unknown => total,
        }
    }

    fn estimate_predicate_rows(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        stats: &TableStats,
    ) -> Option<usize> {
        let predicates = Self::collect_conjunctive_predicates(expr);
        let mut selectivity = 1.0f64;
        let mut estimated = false;

        for predicate in predicates {
            if let Some(predicate_selectivity) =
                self.estimate_predicate_selectivity(&predicate, schema, stats)
            {
                selectivity *= predicate_selectivity;
                estimated = true;
            }
        }

        estimated.then(|| StatsEstimator::selectivity_to_rows(stats.row_count, selectivity))
    }

    fn estimate_predicate_selectivity(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        stats: &TableStats,
    ) -> Option<f64> {
        let estimator = StatsEstimator::new(schema, stats);
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                let column_idx = self.explain_column_constant_index(left, right, schema)?;
                estimator.equality_selectivity(column_idx)
            }
            Expr::BinaryOp { left, op, right }
                if matches!(
                    op,
                    BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                ) =>
            {
                self.explain_column_constant_index(left, right, schema)
                    .and_then(|column_idx| estimator.range_selectivity(column_idx))
            }
            Expr::IsNull(inner) => {
                let column_idx = self.explain_column_index(inner, schema)?;
                estimator.null_selectivity(column_idx)
            }
            Expr::IsNotNull(inner) => {
                let column_idx = self.explain_column_index(inner, schema)?;
                estimator.not_null_selectivity(column_idx)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } if !*negated => {
                let column_idx = self.explain_column_index(expr, schema)?;
                estimator.in_list_selectivity(column_idx, list.len())
            }
            Expr::Like { expr, negated, .. } if !*negated => self
                .explain_column_index(expr, schema)
                .and_then(|column_idx| estimator.like_selectivity(column_idx)),
            Expr::Nested(inner) => self.estimate_predicate_selectivity(inner, schema, stats),
            _ => None,
        }
    }

    fn explain_column_constant_index(
        &self,
        left: &Expr,
        right: &Expr,
        schema: &TableSchema,
    ) -> Option<usize> {
        let left_idx = self.explain_column_index(left, schema);
        let right_idx = self.explain_column_index(right, schema);

        if left_idx.is_some()
            && right_idx.is_none()
            && !self.explain_expr_has_column_reference(right)
        {
            left_idx
        } else if right_idx.is_some()
            && left_idx.is_none()
            && !self.explain_expr_has_column_reference(left)
        {
            right_idx
        } else {
            None
        }
    }

    async fn explain_table_stats(
        &self,
        table: &TableFactor,
        txn: &mut dyn Transaction,
    ) -> Result<Option<String>> {
        let TableFactor::Table { name, .. } = table else {
            return Ok(None);
        };

        let table_name = name.to_string();
        let Some(stats) = self.load_table_stats(&table_name, txn).await? else {
            return Ok(None);
        };

        let column_part_count = stats.columns.len().min(4);
        let mut column_parts = Vec::with_capacity(column_part_count);
        for column in stats.columns.iter().take(4) {
            let mut part = format!(
                "{}(distinct={}, nulls={})",
                column.name, column.distinct_count, column.null_count
            );
            if let (Some(min), Some(max)) = (&column.min, &column.max) {
                part.push_str(&format!(", min={}, max={}", min, max));
            }
            column_parts.push(part);
        }

        let suffix = if stats.columns.len() > column_parts.len() {
            format!(", +{} more", stats.columns.len() - column_parts.len())
        } else {
            String::new()
        };

        Ok(Some(format!(
            "rows={}, columns=[{}{}]",
            stats.row_count,
            column_parts.join("; "),
            suffix
        )))
    }

    async fn explain_comma_join_order(
        &self,
        from: &[TableWithJoins],
        selection: &Option<Expr>,
        txn: &mut dyn Transaction,
    ) -> Result<Option<ExplainJoinOrder>> {
        let Some(selection) = selection else {
            return Ok(None);
        };
        if from.len() < 3 || from.iter().any(|table| !table.joins.is_empty()) {
            return Ok(None);
        }

        let mut relations = Vec::with_capacity(from.len());
        for table in from {
            let TableFactor::Table { name, args, .. } = &table.relation else {
                return Ok(None);
            };
            if args.is_some() {
                return Ok(None);
            }

            let table_name = name.to_string();
            let Some(mut schema) = self.load_explain_schema(&table_name, txn).await? else {
                return Ok(None);
            };
            self.prefix_schema_columns(&mut schema, &table.relation)?;

            let stats = self.load_table_stats(&table_name, txn).await?;
            let base_rows = if let Some(stats) = &stats {
                stats.row_count
            } else {
                self.count_routed_data_prefixes_for_table(&table_name, txn)
                    .await
                    .unwrap_or(usize::MAX)
            };

            relations.push(ExplainJoinRelation {
                table: table.clone(),
                schema,
                stats,
                base_rows,
                estimated_rows: base_rows,
            });
        }

        let predicates = Self::collect_conjunctive_predicates(selection);
        let relation_count = relations.len();
        let mut local_counts = vec![0usize; relation_count];
        let mut edge_counts = vec![vec![0usize; relation_count]; relation_count];
        let mut join_edges =
            vec![vec![Vec::<ExplainJoinEdge>::new(); relation_count]; relation_count];
        let mut schemas = Vec::with_capacity(relations.len());
        for relation in &relations {
            schemas.push(relation.schema.clone());
        }

        for predicate in &predicates {
            let Some(members) = self.predicate_schema_members_for_explain(predicate, &schemas)
            else {
                continue;
            };

            if members.len() == 1 {
                let relation_index = members[0];
                local_counts[relation_index] += 1;
                if let Some(stats) = &relations[relation_index].stats {
                    if let Some(rows) = self.estimate_predicate_rows(
                        predicate,
                        &relations[relation_index].schema,
                        stats,
                    ) {
                        relations[relation_index].estimated_rows =
                            relations[relation_index].estimated_rows.min(rows);
                    }
                }
            } else if members.len() == 2 {
                edge_counts[members[0]][members[1]] += 1;
                edge_counts[members[1]][members[0]] += 1;
                if let Some(edge) =
                    self.explain_equality_join_edge(predicate, members[0], members[1], &schemas)
                {
                    join_edges[members[0]][members[1]].push(edge);
                    join_edges[members[1]][members[0]].push(ExplainJoinEdge {
                        left_column: edge.right_column,
                        right_column: edge.left_column,
                    });
                }
            }
        }

        let has_join_edge = edge_counts
            .iter()
            .any(|row| row.iter().any(|count| *count > 0));
        if !has_join_edge {
            return Ok(None);
        }

        let order = Self::choose_explain_join_order(&relations, &local_counts, &edge_counts);
        let (rows, cost) =
            Self::estimate_join_order_cost(&relations, &order, &edge_counts, &join_edges);
        let mut labels = Vec::with_capacity(order.len());
        for index in &order {
            let relation = &relations[*index];
            let label = if relation.estimated_rows == relation.base_rows {
                format!(
                    "{}(rows={})",
                    relation.table.relation, relation.estimated_rows
                )
            } else {
                format!(
                    "{}(rows={} of {})",
                    relation.table.relation, relation.estimated_rows, relation.base_rows
                )
            };
            labels.push(label);
        }

        Ok(Some(ExplainJoinOrder {
            order: labels,
            rows,
            cost,
        }))
    }

    fn choose_explain_join_order(
        relations: &[ExplainJoinRelation],
        local_counts: &[usize],
        edge_counts: &[Vec<usize>],
    ) -> Vec<usize> {
        let relation_count = relations.len();
        let degree =
            |index: usize, edge_counts: &[Vec<usize>]| -> usize { edge_counts[index].iter().sum() };

        let mut remaining = vec![true; relation_count];
        let mut order = Vec::with_capacity(relation_count);
        let mut first = 0usize;
        let mut first_score = 0usize;
        let mut first_rows = usize::MAX;

        for index in 0..relation_count {
            let score = local_counts[index]
                .saturating_mul(100)
                .saturating_add(degree(index, edge_counts));
            let rows = relations[index].estimated_rows;
            if score > first_score
                || (score == first_score && rows < first_rows)
                || (score == first_score && rows == first_rows && index > first)
            {
                first = index;
                first_score = score;
                first_rows = rows;
            }
        }
        remaining[first] = false;
        order.push(first);

        while remaining.iter().any(|value| *value) {
            let mut next = None;
            let mut next_score = 0usize;
            let mut next_rows = usize::MAX;
            for candidate in 0..relation_count {
                if !remaining[candidate] {
                    continue;
                }
                let connected_edges = order
                    .iter()
                    .map(|placed| edge_counts[candidate][*placed])
                    .sum::<usize>();
                let score = connected_edges
                    .saturating_mul(1000)
                    .saturating_add(local_counts[candidate].saturating_mul(100))
                    .saturating_add(degree(candidate, edge_counts));
                let rows = relations[candidate].estimated_rows;
                if next.is_none()
                    || score > next_score
                    || (score == next_score && rows < next_rows)
                    || (score == next_score
                        && rows == next_rows
                        && candidate > next.unwrap_or(candidate))
                {
                    next = Some(candidate);
                    next_score = score;
                    next_rows = rows;
                }
            }
            let next = next.unwrap();
            remaining[next] = false;
            order.push(next);
        }

        order
    }

    fn estimate_join_order_cost(
        relations: &[ExplainJoinRelation],
        order: &[usize],
        edge_counts: &[Vec<usize>],
        join_edges: &[Vec<Vec<ExplainJoinEdge>>],
    ) -> (usize, f64) {
        let Some((&first, rest)) = order.split_first() else {
            return (0, 0.0);
        };

        let mut placed = vec![first];
        let mut rows = relations[first].estimated_rows;
        let mut cost = rows as f64;

        for index in rest {
            let right_rows = relations[*index].estimated_rows;
            let connected_edges = placed
                .iter()
                .map(|placed_index| edge_counts[*index][*placed_index])
                .sum::<usize>();

            rows = if connected_edges > 0 {
                Self::estimate_connected_join_rows(
                    relations, &placed, *index, rows, right_rows, join_edges,
                )
                .unwrap_or_else(|| rows.max(right_rows).max(1))
            } else {
                rows.saturating_mul(right_rows).max(1)
            };
            cost += right_rows as f64 + rows as f64;
            placed.push(*index);
        }

        (rows, cost)
    }

    fn estimate_connected_join_rows(
        relations: &[ExplainJoinRelation],
        placed: &[usize],
        candidate: usize,
        placed_rows: usize,
        candidate_rows: usize,
        join_edges: &[Vec<Vec<ExplainJoinEdge>>],
    ) -> Option<usize> {
        let candidate_relation = relations.get(candidate)?;
        let candidate_stats = candidate_relation.stats.as_ref()?;
        let mut best_rows = None;

        for placed_index in placed {
            let placed_relation = relations.get(*placed_index)?;
            let placed_stats = placed_relation.stats.as_ref()?;
            for edge in join_edges.get(*placed_index)?.get(candidate)? {
                let Some(estimate) = StatsEstimator::equality_join_estimate(
                    &placed_relation.schema,
                    placed_stats,
                    edge.left_column,
                    placed_rows,
                    &candidate_relation.schema,
                    candidate_stats,
                    edge.right_column,
                    candidate_rows,
                ) else {
                    continue;
                };
                best_rows =
                    Some(best_rows.map_or(estimate.rows, |rows: usize| rows.min(estimate.rows)));
            }
        }

        best_rows
    }

    fn predicate_schema_members_for_explain(
        &self,
        expr: &Expr,
        schemas: &[TableSchema],
    ) -> Option<Vec<usize>> {
        let column_capacity = schemas.iter().fold(0usize, |capacity, schema| {
            capacity.saturating_add(schema.columns.len())
        });
        let mut columns = HashSet::with_capacity(column_capacity);
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return None;
        }

        let mut members = HashSet::with_capacity(schemas.len());
        for column in columns {
            let mut matched = false;
            for (index, schema) in schemas.iter().enumerate() {
                if self.schema_contains_column_name_for_explain(&column, schema) {
                    members.insert(index);
                    matched = true;
                }
            }
            if !matched {
                return None;
            }
        }

        let mut member_list = Vec::with_capacity(members.len());
        for member in members {
            member_list.push(member);
        }
        member_list.sort_unstable();
        Some(member_list)
    }

    fn explain_equality_join_edge(
        &self,
        expr: &Expr,
        left_relation: usize,
        right_relation: usize,
        schemas: &[TableSchema],
    ) -> Option<ExplainJoinEdge> {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = expr
        else {
            if let Expr::Nested(inner) = expr {
                return self.explain_equality_join_edge(
                    inner,
                    left_relation,
                    right_relation,
                    schemas,
                );
            }
            return None;
        };

        let left_schema = schemas.get(left_relation)?;
        let right_schema = schemas.get(right_relation)?;
        if let (Some(left_column), Some(right_column)) = (
            self.explain_column_index(left, left_schema),
            self.explain_column_index(right, right_schema),
        ) {
            return Some(ExplainJoinEdge {
                left_column,
                right_column,
            });
        }

        if let (Some(left_column), Some(right_column)) = (
            self.explain_column_index(right, left_schema),
            self.explain_column_index(left, right_schema),
        ) {
            return Some(ExplainJoinEdge {
                left_column,
                right_column,
            });
        }

        None
    }

    fn schema_contains_column_name_for_explain(
        &self,
        col_name: &str,
        schema: &TableSchema,
    ) -> bool {
        if col_name.contains('.') {
            schema
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(col_name))
        } else {
            self.resolve_column_index(col_name, schema).is_ok()
        }
    }

    async fn composite_index_for_explain(
        &self,
        selection: &Expr,
        table_name: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<String>> {
        let indexes = self
            .load_composite_indexes_for_table(table_name, txn)
            .await?;
        if indexes.is_empty() {
            return Ok(None);
        }

        let predicates = Self::collect_conjunctive_predicates(selection);
        let mut equality_columns = HashSet::with_capacity(predicates.len());

        for predicate in predicates {
            let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = predicate
            else {
                continue;
            };

            let left_columns = !self.explain_expr_has_column_reference(&left);
            let right_columns = !self.explain_expr_has_column_reference(&right);

            if !left_columns {
                if let Some(column) = Self::column_name_from_expr(&left) {
                    if right_columns {
                        equality_columns.insert(column);
                    }
                }
            }

            if !right_columns {
                if let Some(column) = Self::column_name_from_expr(&right) {
                    if left_columns {
                        equality_columns.insert(column);
                    }
                }
            }
        }

        Ok(indexes
            .into_iter()
            .filter(|index| {
                index.columns.iter().all(|column| {
                    equality_columns
                        .iter()
                        .any(|candidate| candidate.eq_ignore_ascii_case(column))
                })
            })
            .max_by_key(|index| index.columns.len())
            .map(|index| format!("{} (BTree composite)", index.name)))
    }

    fn explain_column_index(&self, expr: &Expr, schema: &TableSchema) -> Option<usize> {
        match expr {
            Expr::Identifier(ident) => self.resolve_column_index(&ident.value, schema).ok(),
            Expr::CompoundIdentifier(idents) => {
                let capacity = idents.iter().map(|ident| ident.value.len()).sum::<usize>()
                    + idents.len().saturating_sub(1);
                let mut col_name = String::with_capacity(capacity);
                for (index, ident) in idents.iter().enumerate() {
                    if index > 0 {
                        col_name.push('.');
                    }
                    col_name.push_str(&ident.value);
                }
                self.resolve_column_index(&col_name, schema).ok()
            }
            _ => None,
        }
    }

    fn explain_expr_has_column_reference(&self, expr: &Expr) -> bool {
        self.expr_has_column_reference(expr)
    }

    fn explain_primary_key_range_selection(&self, selection: &Expr, schema: &TableSchema) -> bool {
        let predicates = Self::collect_conjunctive_predicates(selection);
        !predicates.is_empty()
            && predicates.into_iter().all(|predicate| {
                let Expr::BinaryOp { left, op, right } = predicate else {
                    return false;
                };
                self.explain_primary_key_range(&left, &op, &right, schema)
            })
    }

    fn explain_primary_key_range(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        schema: &TableSchema,
    ) -> bool {
        if !matches!(
            op,
            BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq
        ) {
            return false;
        }

        let Some(pk_idx) = schema.get_primary_key_index() else {
            return false;
        };
        (self.explain_column_index(left, schema) == Some(pk_idx)
            && !self.explain_expr_has_column_reference(right))
            || (self.explain_column_index(right, schema) == Some(pk_idx)
                && !self.explain_expr_has_column_reference(left))
    }

    fn check_index_usage(&self, expr: &Expr, schema: &TableSchema, result: &mut Option<String>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let indexed_column = [left.as_ref(), right.as_ref()]
                    .into_iter()
                    .filter_map(|expr| self.explain_column_index(expr, schema))
                    .find(|idx| {
                        schema.columns[*idx].is_indexed
                            && ((self.explain_column_index(left, schema) == Some(*idx)
                                && !self.explain_expr_has_column_reference(right))
                                || (self.explain_column_index(right, schema) == Some(*idx)
                                    && !self.explain_expr_has_column_reference(left)))
                    });

                if let Some(idx) = indexed_column {
                    *result = Some(format!(
                        "{} ({:?})",
                        schema.columns[idx].name, schema.columns[idx].index_type
                    ));
                }
            }
            Expr::MatchAgainst { columns, .. } => {
                if !columns.is_empty() {
                    let col = &columns[0];
                    let col_name = col.to_string();
                    if let Some(idx) = schema.get_column_index(&col_name) {
                        if schema.columns[idx].is_indexed {
                            *result = Some(format!("{} (FTS)", col_name));
                        }
                    }
                }
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                self.check_index_usage(left, schema, result);
                if result.is_none() {
                    self.check_index_usage(right, schema, result);
                }
            }
            _ => {}
        }
    }
}
