use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, TableFactor};
use std::collections::HashSet;

use super::super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn handle_explain(
        &self,
        stmt: &Statement,
        analyze: bool,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        if analyze {
            let start = std::time::Instant::now();
            let _ = Box::pin(self.execute_in_transaction_with_params(stmt, txn, params)).await?;
            let duration = start.elapsed();

            let plan = self.explain_statement_plan(stmt, txn).await?;
            let output = format!("Execution Time: {:?}\nPlan:\n{}", duration, plan);

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
                    .explain_table_access(&table.relation, &select.selection, txn)
                    .await?;
                plan.push_str(&format!("  Access Path: {}\n", access_path));
                if let Some(stats) = self.explain_table_stats(&table.relation, txn).await? {
                    plan.push_str(&format!("  Stats: {}\n", stats));
                }

                for join in &table.joins {
                    plan.push_str(&format!("  JOIN: {}\n", join.relation));
                    let join_access = self
                        .explain_table_access(&join.relation, &None, txn)
                        .await?;
                    plan.push_str(&format!("    Access Path: {}\n", join_access));
                    if let Some(stats) = self.explain_table_stats(&join.relation, txn).await? {
                        plan.push_str(&format!("    Stats: {}\n", stats));
                    }
                    plan.push_str(&format!("    Operator: {:?}\n", join.join_operator));
                }
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

    async fn explain_table_access(
        &self,
        table: &TableFactor,
        selection: &Option<Expr>,
        txn: &mut dyn Transaction,
    ) -> Result<String> {
        if let TableFactor::Table { name, .. } = table {
            let table_name = name.to_string();
            let schema_key = format!("schema:{}", table_name);
            if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                let schema: TableSchema = bincode::deserialize(&schema_bytes)
                    .map_err(|e| FusionError::Execution(format!("Schema error: {}", e)))?;

                if let Some(sel) = selection {
                    if let Some(index_name) = self
                        .composite_index_for_explain(sel, &table_name, txn)
                        .await?
                    {
                        return Ok(format!("Index Scan using {}", index_name));
                    }

                    if let Expr::BinaryOp { left, op, right } = sel {
                        if *op == BinaryOperator::Eq {
                            if (self.explain_column_index(left, &schema) == Some(0)
                                && !self.explain_expr_has_column_reference(right))
                                || (self.explain_column_index(right, &schema) == Some(0)
                                    && !self.explain_expr_has_column_reference(left))
                            {
                                return Ok("Primary Key Lookup (Clustered Index)".to_string());
                            }
                        } else if self.explain_primary_key_range(left, op, right, &schema) {
                            return Ok("Primary Key Range Scan (Clustered Index)".to_string());
                        }
                    }

                    let mut used_index = None;
                    self.check_index_usage(sel, &schema, &mut used_index);

                    if let Some(idx_info) = used_index {
                        return Ok(format!("Index Scan using {}", idx_info));
                    }
                }

                Ok("Full Table Scan".to_string())
            } else {
                Ok("Table not found".to_string())
            }
        } else {
            Ok("Unknown Table Factor".to_string())
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

        let column_parts = stats
            .columns
            .iter()
            .take(4)
            .map(|column| {
                let mut part = format!(
                    "{}(distinct={}, nulls={})",
                    column.name, column.distinct_count, column.null_count
                );
                if let (Some(min), Some(max)) = (&column.min, &column.max) {
                    part.push_str(&format!(", min={}, max={}", min, max));
                }
                part
            })
            .collect::<Vec<_>>();

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
                let col_name = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                self.resolve_column_index(&col_name, schema).ok()
            }
            _ => None,
        }
    }

    fn explain_expr_has_column_reference(&self, expr: &Expr) -> bool {
        let mut cols = HashSet::new();
        self.extract_columns_from_expr(expr, &mut cols);
        !cols.is_empty()
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

        (self.explain_column_index(left, schema) == Some(0)
            && !self.explain_expr_has_column_reference(right))
            || (self.explain_column_index(right, schema) == Some(0)
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
