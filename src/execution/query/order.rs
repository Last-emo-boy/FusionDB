use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{Expr, OrderByKind, SelectItem, TableFactor};
use std::cmp::Ordering;

use super::Executor;

pub(super) enum ProjectionOrderValueSource<'a> {
    RowIndex(usize),
    Expr {
        expr: &'a Expr,
        fallback_index: Option<usize>,
    },
}

pub(super) enum SortOrderValueSource<'a> {
    RowIndex(usize),
    Projection {
        source: ProjectionOrderValueSource<'a>,
        fallback_expr: &'a Expr,
    },
    Expr(&'a Expr),
}

pub(super) struct SortOrderKey<'a> {
    pub(super) source: SortOrderValueSource<'a>,
    pub(super) asc: bool,
}

impl Executor {
    pub(super) fn order_limit_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => {
                let capacity = idents.iter().map(|ident| ident.value.len()).sum::<usize>()
                    + idents.len().saturating_sub(1);
                let mut name = String::with_capacity(capacity);
                for (index, ident) in idents.iter().enumerate() {
                    if index > 0 {
                        name.push('.');
                    }
                    name.push_str(&ident.value);
                }
                Some(name)
            }
            _ => None,
        }
    }

    fn projection_allows_order_limit_pushdown(projection: &[SelectItem]) -> bool {
        projection.iter().all(|item| match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
            SelectItem::UnnamedExpr(expr) => Self::expr_allows_order_limit_pushdown(expr),
            SelectItem::ExprWithAlias { expr, .. } => Self::expr_allows_order_limit_pushdown(expr),
        })
    }

    fn expr_allows_order_limit_pushdown(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
            Expr::Nested(inner) => Self::expr_allows_order_limit_pushdown(inner),
            _ => false,
        }
    }

    pub(super) async fn primary_key_order_scan_limit(
        &self,
        select: &sqlparser::ast::Select,
        order_by: Option<&sqlparser::ast::OrderBy>,
        limit: Option<usize>,
        offset: usize,
        txn: &mut dyn Transaction,
    ) -> Result<Option<usize>> {
        let Some(limit) = limit else {
            return Ok(None);
        };
        let Some(order_by) = order_by else {
            return Ok(None);
        };
        if select.selection.is_some()
            || select.having.is_some()
            || select.distinct.is_some()
            || !Self::projection_allows_order_limit_pushdown(&select.projection)
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
            || select.from.len() != 1
            || !select.from[0].joins.is_empty()
        {
            return Ok(None);
        }

        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return Ok(None);
        };

        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Ok(None);
        };
        let [order_expr] = exprs.as_slice() else {
            return Ok(None);
        };
        if !order_expr.options.asc.unwrap_or(true) {
            return Ok(None);
        }

        let table_name = name.to_string();
        let schema_key = format!("schema:{}", table_name);
        let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? else {
            return Ok(None);
        };
        let schema: TableSchema = bincode::deserialize(&schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let Some(order_col) = Self::order_limit_column_name(&order_expr.expr) else {
            return Ok(None);
        };
        let Ok(order_idx) = self.resolve_column_index(&order_col, &schema) else {
            return Ok(None);
        };
        if schema
            .columns
            .get(order_idx)
            .is_some_and(|column| column.is_primary)
        {
            Ok(Some(if limit == 0 {
                0
            } else {
                offset.saturating_add(limit)
            }))
        } else {
            Ok(None)
        }
    }

    fn resolve_order_by_projection_index(
        &self,
        expr: &Expr,
        projection: &[SelectItem],
        columns: &[String],
    ) -> Option<usize> {
        if let Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Number(n, _),
            ..
        }) = expr
        {
            if let Ok(position) = n.parse::<usize>() {
                let index = position.checked_sub(1)?;
                if index < columns.len() {
                    return Some(index);
                }
            }
        }

        if let Expr::Identifier(ident) = expr {
            if let Some(index) = columns
                .iter()
                .position(|column| column.eq_ignore_ascii_case(&ident.value))
            {
                return Some(index);
            }
        }

        projection
            .iter()
            .enumerate()
            .find_map(|(index, item)| match item {
                SelectItem::UnnamedExpr(proj_expr) if proj_expr == expr => Some(index),
                SelectItem::ExprWithAlias { expr: proj_expr, alias }
                    if proj_expr == expr
                        || matches!(expr, Expr::Identifier(ident) if alias.value.eq_ignore_ascii_case(&ident.value)) =>
                {
                    Some(index)
                }
                _ => None,
            })
    }

    fn resolve_projection_order_value_source<'a>(
        &self,
        expr: &Expr,
        projection: &'a [SelectItem],
        columns: &[String],
    ) -> Option<ProjectionOrderValueSource<'a>> {
        if let Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Number(n, _),
            ..
        }) = expr
        {
            let index = n.parse::<usize>().ok()?.checked_sub(1)?;
            if index >= columns.len() {
                return None;
            }

            return projection.get(index).and_then(|item| match item {
                SelectItem::UnnamedExpr(proj_expr)
                | SelectItem::ExprWithAlias {
                    expr: proj_expr, ..
                } => Some(ProjectionOrderValueSource::Expr {
                    expr: proj_expr,
                    fallback_index: Some(index),
                }),
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    Some(ProjectionOrderValueSource::RowIndex(index))
                }
            });
        }

        projection
            .iter()
            .find_map(|item| match item {
                SelectItem::UnnamedExpr(proj_expr) if proj_expr == expr => {
                    Some(ProjectionOrderValueSource::Expr {
                        expr: proj_expr,
                        fallback_index: None,
                    })
                }
                SelectItem::ExprWithAlias {
                    expr: proj_expr,
                    alias,
                } if proj_expr == expr
                    || matches!(expr, Expr::Identifier(ident) if alias.value.eq_ignore_ascii_case(&ident.value)) =>
                {
                    Some(ProjectionOrderValueSource::Expr {
                        expr: proj_expr,
                        fallback_index: None,
                    })
                }
                _ => None,
            })
    }

    fn resolve_schema_order_value_index(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        columns: &[String],
    ) -> Option<usize> {
        if let Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Number(n, _),
            ..
        }) = expr
        {
            let index = n.parse::<usize>().ok()?.checked_sub(1)?;
            if index < columns.len() && index < schema.columns.len() {
                return Some(index);
            }
            return None;
        }

        let col_name = Self::order_limit_column_name(expr)?;
        self.resolve_column_index(&col_name, schema).ok()
    }

    fn evaluate_projection_order_value_source(
        &self,
        source: &ProjectionOrderValueSource<'_>,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<Value> {
        match source {
            ProjectionOrderValueSource::RowIndex(index) => row.get(*index).cloned(),
            ProjectionOrderValueSource::Expr {
                expr,
                fallback_index,
            } => self
                .evaluate_value(expr, row, schema, params)
                .ok()
                .or_else(|| fallback_index.and_then(|index| row.get(index).cloned())),
        }
    }

    pub(super) fn resolve_order_value_source<'a>(
        &self,
        expr: &'a Expr,
        projection: &'a [SelectItem],
        columns: &[String],
        schema: &TableSchema,
        rows_are_projected: bool,
        rows_are_full_schema: bool,
    ) -> SortOrderValueSource<'a> {
        if rows_are_projected {
            if let Some(index) = self.resolve_order_by_projection_index(expr, projection, columns) {
                return SortOrderValueSource::RowIndex(index);
            }
        }

        if rows_are_full_schema {
            if let Some(index) = self.resolve_schema_order_value_index(expr, schema, columns) {
                return SortOrderValueSource::RowIndex(index);
            }
        }

        if let Some(source) = self.resolve_projection_order_value_source(expr, projection, columns)
        {
            SortOrderValueSource::Projection {
                source,
                fallback_expr: expr,
            }
        } else {
            SortOrderValueSource::Expr(expr)
        }
    }

    fn compare_order_value_source(
        &self,
        source: &SortOrderValueSource<'_>,
        left: &[Value],
        right: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Ordering {
        match source {
            SortOrderValueSource::RowIndex(index) => match (left.get(*index), right.get(*index)) {
                (Some(val_a), Some(val_b)) => self.compare_for_sort(val_a, val_b),
                (Some(val_a), None) => self.compare_for_sort(val_a, &Value::Null),
                (None, Some(val_b)) => self.compare_for_sort(&Value::Null, val_b),
                (None, None) => Ordering::Equal,
            },
            SortOrderValueSource::Projection {
                source,
                fallback_expr,
            } => {
                if let Some((val_a, val_b)) = self
                    .evaluate_projection_order_value_source(source, left, schema, params)
                    .zip(self.evaluate_projection_order_value_source(source, right, schema, params))
                {
                    self.compare_for_sort(&val_a, &val_b)
                } else {
                    let val_a = self
                        .evaluate_value(fallback_expr, left, schema, params)
                        .unwrap_or(Value::Null);
                    let val_b = self
                        .evaluate_value(fallback_expr, right, schema, params)
                        .unwrap_or(Value::Null);
                    self.compare_for_sort(&val_a, &val_b)
                }
            }
            SortOrderValueSource::Expr(expr) => {
                let val_a = self
                    .evaluate_value(expr, left, schema, params)
                    .unwrap_or(Value::Null);
                let val_b = self
                    .evaluate_value(expr, right, schema, params)
                    .unwrap_or(Value::Null);
                self.compare_for_sort(&val_a, &val_b)
            }
        }
    }

    fn compare_sort_order_keys(
        &self,
        sort_keys: &[SortOrderKey<'_>],
        left: &[Value],
        right: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Ordering {
        for sort_key in sort_keys {
            let ordering =
                self.compare_order_value_source(&sort_key.source, left, right, schema, params);
            if ordering != Ordering::Equal {
                return if sort_key.asc {
                    ordering
                } else {
                    ordering.reverse()
                };
            }
        }

        Ordering::Equal
    }

    pub(super) fn sort_rows_by_order_keys(
        &self,
        rows: &mut Vec<Vec<Value>>,
        sort_keys: &[SortOrderKey<'_>],
        schema: &TableSchema,
        params: &[Value],
        limit_window: Option<usize>,
    ) {
        if let Some(window) = limit_window {
            if window == 0 {
                rows.clear();
                return;
            }

            if window < rows.len() {
                let mut indexed_rows: Vec<(usize, Vec<Value>)> =
                    std::mem::take(rows).into_iter().enumerate().collect();

                let compare_indexed = |left: &(usize, Vec<Value>), right: &(usize, Vec<Value>)| {
                    let ordering =
                        self.compare_sort_order_keys(sort_keys, &left.1, &right.1, schema, params);
                    if ordering == Ordering::Equal {
                        left.0.cmp(&right.0)
                    } else {
                        ordering
                    }
                };

                let _ = indexed_rows.select_nth_unstable_by(window, compare_indexed);
                indexed_rows.truncate(window);
                indexed_rows.sort_by(compare_indexed);
                *rows = indexed_rows.into_iter().map(|(_, row)| row).collect();
                return;
            }
        }

        rows.sort_by(|left, right| {
            self.compare_sort_order_keys(sort_keys, left, right, schema, params)
        });
    }
}
