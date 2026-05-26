use crate::catalog::{Column, IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, LimitClause,
    OrderByKind, SelectItem, SetExpr, TableFactor,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::{AggregateAccumulator, Executor, QueryResult};

enum ProjectionOrderValueSource<'a> {
    RowIndex(usize),
    Expr {
        expr: &'a Expr,
        fallback_index: Option<usize>,
    },
}

enum SortOrderValueSource<'a> {
    RowIndex(usize),
    Projection {
        source: ProjectionOrderValueSource<'a>,
        fallback_expr: &'a Expr,
    },
    Expr(&'a Expr),
}

struct SortOrderKey<'a> {
    source: SortOrderValueSource<'a>,
    asc: bool,
}

enum RowValueSource<'a> {
    One,
    Column(usize),
    Literal(Value),
    MultiplyColumns(usize, usize),
    Expr(&'a Expr),
}

impl<'a> RowValueSource<'a> {
    fn evaluate(
        &self,
        executor: &Executor,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Value {
        match self {
            RowValueSource::One => Value::Integer(1),
            RowValueSource::Column(index) => row.get(*index).cloned().unwrap_or(Value::Null),
            RowValueSource::Literal(value) => value.clone(),
            RowValueSource::MultiplyColumns(left_idx, right_idx) => {
                let Some(left) = row.get(*left_idx) else {
                    return Value::Null;
                };
                let Some(right) = row.get(*right_idx) else {
                    return Value::Null;
                };

                match (left, right) {
                    (Value::Integer(left), Value::Integer(right)) => Value::Integer(*left * *right),
                    (Value::Integer(left), Value::Float(right)) => {
                        Value::Float(*left as f64 * *right)
                    }
                    (Value::Float(left), Value::Integer(right)) => {
                        Value::Float(*left * *right as f64)
                    }
                    (Value::Float(left), Value::Float(right)) => Value::Float(*left * *right),
                    _ => Value::Null,
                }
            }
            RowValueSource::Expr(expr) => executor
                .evaluate_value(expr, row, schema, params)
                .unwrap_or(Value::Null),
        }
    }
}

struct GroupAggregatePlan<'a> {
    expr: Expr,
    func_name: String,
    arg_source: RowValueSource<'a>,
}

impl Executor {
    fn count_distinct_projection<'a>(
        projection: &'a [SelectItem],
        schema: &TableSchema,
        allowed_qualifiers: Option<&[String]>,
    ) -> Option<(usize, String)> {
        let [item] = projection else {
            return None;
        };

        let (expr, column_name) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, format!("{}", expr)),
            SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
            _ => return None,
        };

        let Expr::Function(func) = expr else {
            return None;
        };
        if !func.name.to_string().eq_ignore_ascii_case("COUNT") {
            return None;
        }

        let FunctionArguments::List(args) = &func.args else {
            return None;
        };
        if args.duplicate_treatment != Some(DuplicateTreatment::Distinct) || args.args.len() != 1 {
            return None;
        }

        Self::column_arg_index(&args.args[0], schema, allowed_qualifiers)
            .map(|index| (index, column_name))
    }

    async fn count_distinct_column_scan(
        &self,
        table_name: &str,
        column_index: usize,
        txn: &mut dyn Transaction,
    ) -> Result<i64> {
        let prefix = format!("data:{}:", table_name);
        let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
        let mut seen = HashSet::with_capacity(kv_pairs.len().min(4096));

        for (_, data) in kv_pairs {
            let value = crate::common::encoding::RowDecoder::decode_column(&data, column_index)
                .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?
                .unwrap_or(Value::Null);
            if value != Value::Null {
                seen.insert(value);
            }
        }

        Ok(seen.len() as i64)
    }

    fn order_limit_column_name(expr: &Expr) -> Option<String> {
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

    async fn primary_key_order_scan_limit(
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

    fn deduplicate_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        let mut seen = HashSet::with_capacity(rows.len());
        let mut unique_rows = Vec::with_capacity(rows.len());

        for row in rows {
            if seen.insert(row.clone()) {
                unique_rows.push(row);
            }
        }

        unique_rows
    }

    fn compound_identifier_prefix(idents: &[sqlparser::ast::Ident]) -> String {
        let prefix_len = idents.len().saturating_sub(1);
        let capacity = idents
            .iter()
            .take(prefix_len)
            .map(|ident| ident.value.len())
            .sum::<usize>()
            + prefix_len.saturating_sub(1);
        let mut prefix = String::with_capacity(capacity);

        for (index, ident) in idents.iter().take(prefix_len).enumerate() {
            if index > 0 {
                prefix.push('.');
            }
            prefix.push_str(&ident.value);
        }

        prefix
    }

    fn count_prefix_eligible_arg(
        arg: &FunctionArg,
        schema: &TableSchema,
        allowed_qualifiers: Option<&[String]>,
    ) -> bool {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => true,
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => {
                !matches!(value.value, sqlparser::ast::Value::Null)
            }
            _ => Self::column_arg_index(arg, schema, allowed_qualifiers)
                .is_some_and(|idx| !schema.columns[idx].is_nullable),
        }
    }

    fn column_arg_index(
        arg: &FunctionArg,
        schema: &TableSchema,
        allowed_qualifiers: Option<&[String]>,
    ) -> Option<usize> {
        let col_name = match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                ident.value.as_str()
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(idents))) => {
                let qualifier = Self::compound_identifier_prefix(idents);

                if allowed_qualifiers
                    .map(|qualifiers| {
                        !qualifiers
                            .iter()
                            .any(|allowed| allowed.eq_ignore_ascii_case(&qualifier))
                    })
                    .unwrap_or(false)
                {
                    return None;
                }

                idents.last()?.value.as_str()
            }
            _ => return None,
        };

        schema
            .columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(col_name))
    }

    fn row_value_source_for_expr<'a>(
        &'a self,
        expr: &'a Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> RowValueSource<'a> {
        match expr {
            Expr::Identifier(ident) => self
                .resolve_column_index(&ident.value, schema)
                .map(RowValueSource::Column)
                .unwrap_or(RowValueSource::Expr(expr)),
            Expr::CompoundIdentifier(_) => Self::order_limit_column_name(expr)
                .and_then(|name| self.resolve_column_index(&name, schema).ok())
                .map(RowValueSource::Column)
                .unwrap_or(RowValueSource::Expr(expr)),
            Expr::Nested(inner) => self.row_value_source_for_expr(inner, schema, params),
            Expr::Value(value) => {
                if let sqlparser::ast::Value::Placeholder(p) = &value.value {
                    let idx = Self::placeholder_index(p);
                    if idx > 0 && idx <= params.len() {
                        RowValueSource::Literal(params[idx - 1].clone())
                    } else {
                        RowValueSource::Expr(expr)
                    }
                } else {
                    RowValueSource::Literal(self.sql_value_to_fusion_value(&value.value))
                }
            }
            Expr::BinaryOp {
                left,
                op: sqlparser::ast::BinaryOperator::Multiply,
                right,
            } => {
                match (
                    self.row_value_source_for_expr(left, schema, params),
                    self.row_value_source_for_expr(right, schema, params),
                ) {
                    (RowValueSource::Column(left_idx), RowValueSource::Column(right_idx)) => {
                        RowValueSource::MultiplyColumns(left_idx, right_idx)
                    }
                    _ => RowValueSource::Expr(expr),
                }
            }
            _ => RowValueSource::Expr(expr),
        }
    }

    fn aggregate_arg_source<'a>(
        &'a self,
        func: &'a sqlparser::ast::Function,
        schema: &TableSchema,
        params: &[Value],
    ) -> RowValueSource<'a> {
        let FunctionArguments::List(args) = &func.args else {
            return RowValueSource::Literal(Value::Null);
        };

        match args.args.first() {
            None | Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => RowValueSource::One,
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) => {
                self.row_value_source_for_expr(expr, schema, params)
            }
            _ => RowValueSource::Literal(Value::Null),
        }
    }

    fn compile_group_key_sources<'a>(
        &'a self,
        group_exprs: &'a [Expr],
        schema: &TableSchema,
        params: &[Value],
    ) -> Vec<RowValueSource<'a>> {
        group_exprs
            .iter()
            .map(|expr| self.row_value_source_for_expr(expr, schema, params))
            .collect()
    }

    fn compile_group_aggregate_plans<'a>(
        &'a self,
        aggregates: &'a [(Expr, String)],
        schema: &TableSchema,
        params: &[Value],
    ) -> Vec<GroupAggregatePlan<'a>> {
        aggregates
            .iter()
            .map(|(expr, func_name)| GroupAggregatePlan {
                expr: expr.clone(),
                func_name: func_name.clone(),
                arg_source: if let Expr::Function(func) = expr {
                    self.aggregate_arg_source(func, schema, params)
                } else {
                    RowValueSource::Expr(expr)
                },
            })
            .collect()
    }

    fn primary_key_arg_index(
        arg: &FunctionArg,
        schema: &TableSchema,
        allowed_qualifiers: Option<&[String]>,
    ) -> Option<usize> {
        Self::column_arg_index(arg, schema, allowed_qualifiers)
            .filter(|idx| schema.columns[*idx].is_primary)
    }

    fn primary_key_value_from_data_key(
        data_key: &[u8],
        prefix: &str,
        column: &Column,
    ) -> Option<Value> {
        let key = std::str::from_utf8(data_key).ok()?;
        let row_id = key.strip_prefix(prefix)?;

        if matches!(column.data_type.as_str(), "INTEGER" | "BIGINT") {
            crate::common::encoding::decode_i64_comparable(row_id)
                .map(Value::Integer)
                .or_else(|| Some(Value::String(row_id.to_string())))
        } else {
            Some(Value::String(row_id.to_string()))
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

    fn resolve_order_value_source<'a>(
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

    fn sort_rows_by_order_keys(
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

    pub(crate) async fn handle_query(
        &self,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        // Materialize CTEs (WITH ... AS) as temporary tables in the transaction
        let mut cte_names: Vec<String> =
            Vec::with_capacity(query.with.as_ref().map_or(0, |with| with.cte_tables.len()));
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let cte_name = cte.alias.name.value.clone();
                let result = Box::pin(self.handle_query(&cte.query, txn, params)).await?;
                if let QueryResult::Select { columns, rows } = result {
                    use crate::catalog::{Column, IndexType, TableSchema};

                    // Build schema: synthetic _rowid PK + CTE columns
                    let mut cols = Vec::with_capacity(columns.len() + 1);
                    cols.push(Column {
                        name: "_rowid".to_string(),
                        data_type: "INTEGER".to_string(),
                        is_primary: true,
                        is_indexed: true,
                        index_type: IndexType::BTree,
                        default_value: None,
                        is_nullable: false,
                        is_unique: true,
                        check_expr: None,
                    });
                    cols.extend(columns.iter().map(|c| Column {
                        name: c.clone(),
                        data_type: "TEXT".to_string(),
                        is_primary: false,
                        is_indexed: false,
                        index_type: IndexType::None,
                        default_value: None,
                        is_nullable: true,
                        is_unique: false,
                        check_expr: None,
                    }));
                    let schema = TableSchema::new(cte_name.clone(), cols);
                    let schema_key = format!("schema:{}", cte_name);
                    let schema_bytes = bincode::serialize(&schema)
                        .map_err(|e| FusionError::Execution(format!("CTE schema error: {}", e)))?;
                    txn.put(schema_key.as_bytes(), &schema_bytes).await?;

                    for (i, row) in rows.iter().enumerate() {
                        // Prepend synthetic _rowid to each row
                        let mut full_row = Vec::with_capacity(row.len() + 1);
                        full_row.push(Value::Integer(i as i64));
                        full_row.extend(row.iter().cloned());
                        let pk_str = crate::common::encoding::encode_i64_comparable(i as i64);
                        let key = format!("data:{}:{}", cte_name, pk_str);
                        let val = crate::common::encoding::RowEncoder::encode(&full_row);
                        txn.put(key.as_bytes(), &val).await?;
                    }
                    cte_names.push(cte_name);
                }
            }
        }

        let result = self.handle_query_inner(query, txn, params).await;

        // Cleanup CTE temporary tables
        for name in &cte_names {
            let _ = txn.delete(format!("schema:{}", name).as_bytes()).await;
            let prefix = format!("data:{}:", name);
            if let Ok(entries) = txn.scan_prefix(prefix.as_bytes(), None).await {
                for (k, _) in entries {
                    let _ = txn.delete(&k).await;
                }
            }
        }

        result
    }

    async fn handle_query_inner(
        &self,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let SetExpr::Select(select) = &query.body.as_ref() {
            let is_join = select.from.len() > 1
                || (!select.from.is_empty() && !select.from[0].joins.is_empty());

            // Extract Limit
            let (limit, offset) = match &query.limit_clause {
                Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                    let limit = if let Some(limit_expr) = limit {
                        match limit_expr {
                            Expr::Value(sqlparser::ast::ValueWithSpan {
                                value: sqlparser::ast::Value::Number(n, _),
                                ..
                            }) => Some(n.parse::<usize>().unwrap_or(usize::MAX)),
                            _ => None,
                        }
                    } else {
                        None
                    };

                    let offset = if let Some(offset_struct) = offset {
                        match &offset_struct.value {
                            Expr::Value(sqlparser::ast::ValueWithSpan {
                                value: sqlparser::ast::Value::Number(n, _),
                                ..
                            }) => n.parse::<usize>().unwrap_or(0),
                            _ => 0,
                        }
                    } else {
                        0
                    };

                    (limit, offset)
                }
                _ => (None, 0),
            };

            // Push down limit only if no ORDER BY and no GROUP BY (simplified)
            let is_group_by_none = matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty());

            // Optimization: Aggregates on PK (COUNT(*), MIN(id), MAX(id)) and
            // single-column COUNT(DISTINCT col) without materializing full rows.
            if !is_join && select.selection.is_none() && is_group_by_none {
                let mut supported = true;
                let mut result_row = Vec::with_capacity(select.projection.len());
                let mut col_names = Vec::with_capacity(select.projection.len());

                if let Some(table) = select.from.first() {
                    if let TableFactor::Table { name, alias, .. } = &table.relation {
                        let table_name_str = name.to_string();
                        let mut aggregate_qualifiers = Vec::with_capacity(2);
                        aggregate_qualifiers.push(table_name_str.clone());
                        if let Some(alias) = alias {
                            aggregate_qualifiers.push(alias.name.value.clone());
                        }
                        let schema_key = format!("schema:{}", table_name_str);
                        if let Ok(Some(schema_bytes)) = txn.get(schema_key.as_bytes()).await {
                            if let Ok(schema) = bincode::deserialize::<TableSchema>(&schema_bytes) {
                                if let Some((column_index, column_name)) =
                                    Self::count_distinct_projection(
                                        &select.projection,
                                        &schema,
                                        Some(&aggregate_qualifiers),
                                    )
                                {
                                    let count = self
                                        .count_distinct_column_scan(
                                            &table_name_str,
                                            column_index,
                                            txn,
                                        )
                                        .await?;
                                    return Ok(QueryResult::Select {
                                        columns: vec![column_name],
                                        rows: vec![vec![Value::Integer(count)]],
                                    });
                                }

                                for proj_item in &select.projection {
                                    let expr = match proj_item {
                                        SelectItem::UnnamedExpr(expr) => Some(expr),
                                        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                                        _ => None,
                                    };

                                    let mut item_handled = false;
                                    if let Some(Expr::Function(func)) = expr {
                                        let func_name = func.name.to_string().to_uppercase();
                                        if func_name == "COUNT" {
                                            if let FunctionArguments::List(args) = &func.args {
                                                let duplicate_treatment = args.duplicate_treatment;
                                                if args.args.len() == 1
                                                    && duplicate_treatment
                                                        != Some(DuplicateTreatment::Distinct)
                                                    && Self::count_prefix_eligible_arg(
                                                        &args.args[0],
                                                        &schema,
                                                        Some(&aggregate_qualifiers),
                                                    )
                                                {
                                                    let prefix =
                                                        format!("data:{}:", table_name_str);
                                                    let count =
                                                        txn.count_prefix(prefix.as_bytes()).await?;
                                                    result_row.push(Value::Integer(count as i64));
                                                    col_names.push(format!("{}", func));
                                                    item_handled = true;
                                                }
                                            }
                                        } else if func_name == "MAX" || func_name == "MIN" {
                                            if let FunctionArguments::List(args) = &func.args {
                                                if args.args.len() == 1 {
                                                    if let Some(idx) = Self::primary_key_arg_index(
                                                        &args.args[0],
                                                        &schema,
                                                        Some(&aggregate_qualifiers),
                                                    ) {
                                                        let prefix =
                                                            format!("data:{}:", table_name_str);
                                                        let min_key = prefix.as_bytes().to_vec();
                                                        let mut max_key =
                                                            prefix.as_bytes().to_vec();
                                                        max_key.push(0xFF);

                                                        let res = if func_name == "MIN" {
                                                            txn.first(&min_key, &max_key).await?
                                                        } else {
                                                            txn.last(&min_key, &max_key).await?
                                                        };

                                                        let val = if let Some((key, _)) = res {
                                                            let column = &schema.columns[idx];
                                                            Self::primary_key_value_from_data_key(
                                                                &key, &prefix, column,
                                                            )
                                                            .unwrap_or(Value::Null)
                                                        } else {
                                                            Value::Null
                                                        };
                                                        result_row.push(val);
                                                        col_names.push(format!("{}", func));
                                                        item_handled = true;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if !item_handled {
                                        supported = false;
                                        break;
                                    }
                                }

                                if supported && !result_row.is_empty() {
                                    return Ok(QueryResult::Select {
                                        columns: col_names,
                                        rows: vec![result_row],
                                    });
                                }
                            }
                        }
                    }
                }
            }

            let primary_key_order_limit = self
                .primary_key_order_scan_limit(select, query.order_by.as_ref(), limit, offset, txn)
                .await?;
            let push_down_limit = if is_group_by_none && query.order_by.is_none() {
                limit.map(|l| l + offset)
            } else {
                primary_key_order_limit
            };

            let is_wildcard = select
                .projection
                .iter()
                .any(|item| matches!(item, SelectItem::Wildcard(_)));

            // Calculate projection hint for scan pushdown
            let projection_hint: Option<Vec<String>> = if is_wildcard {
                None
            } else {
                let mut cols = HashSet::with_capacity(select.projection.len());
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            self.extract_columns_from_expr(expr, &mut cols)
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            self.extract_columns_from_expr(expr, &mut cols)
                        }
                        _ => {}
                    }
                }
                if let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by {
                    for expr in group_exprs {
                        self.extract_columns_from_expr(expr, &mut cols);
                    }
                }
                if let Some(having) = &select.having {
                    self.extract_columns_from_expr(having, &mut cols);
                }
                if let Some(selection) = &select.selection {
                    self.extract_columns_from_expr(selection, &mut cols);
                }
                if let Some(order_by) = &query.order_by {
                    if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                        for expr in exprs {
                            self.extract_columns_from_expr(&expr.expr, &mut cols);
                        }
                    }
                }
                if cols.is_empty() {
                    Some(vec![])
                } else {
                    Some(cols.into_iter().collect())
                }
            };

            // Pre-materialize subqueries in the WHERE clause
            let materialized_selection = if let Some(sel) = &select.selection {
                if Self::contains_subquery(sel) {
                    Some(self.materialize_subqueries(sel, txn, params).await?)
                } else {
                    None
                }
            } else {
                None
            };
            let effective_selection = materialized_selection
                .as_ref()
                .or(select.selection.as_ref());

            // Recompute projection hint if subqueries were materialized
            let projection_hint = if materialized_selection.is_some() && !is_wildcard {
                let mut cols = HashSet::with_capacity(select.projection.len());
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            self.extract_columns_from_expr(expr, &mut cols)
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            self.extract_columns_from_expr(expr, &mut cols)
                        }
                        _ => {}
                    }
                }
                if let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by {
                    for expr in group_exprs {
                        self.extract_columns_from_expr(expr, &mut cols);
                    }
                }
                if let Some(having) = &select.having {
                    self.extract_columns_from_expr(having, &mut cols);
                }
                if let Some(sel) = effective_selection {
                    self.extract_columns_from_expr(sel, &mut cols);
                }
                if let Some(order_by) = &query.order_by {
                    if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                        for expr in exprs {
                            self.extract_columns_from_expr(&expr.expr, &mut cols);
                        }
                    }
                }
                if cols.is_empty() {
                    Some(vec![])
                } else {
                    Some(cols.into_iter().collect())
                }
            } else {
                projection_hint
            };

            let sel_option = effective_selection.cloned();

            let join_limit = if is_join && effective_selection.is_none() && query.order_by.is_none()
            {
                push_down_limit
            } else {
                None
            };

            let (mut schema, mut rows) = if is_join {
                self.execute_join(
                    &select.from,
                    &sel_option,
                    &projection_hint,
                    txn,
                    params,
                    join_limit,
                )
                .await?
            } else if let Some(table) = select.from.first() {
                self.scan_single_table(
                    &table.relation,
                    &sel_option,
                    &projection_hint,
                    txn,
                    params,
                    push_down_limit,
                    query.order_by.as_ref(),
                )
                .await?
            } else {
                // No FROM clause: evaluate expressions directly (e.g., SELECT 1, SELECT 'hello')
                let empty_schema = TableSchema::new("".to_string(), vec![]);
                let empty_row: Vec<Value> = vec![];
                let mut col_names = Vec::with_capacity(select.projection.len());
                let mut result_row = Vec::with_capacity(select.projection.len());
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            col_names.push(format!("{}", expr));
                            result_row.push(
                                self.evaluate_value(expr, &empty_row, &empty_schema, params)
                                    .unwrap_or(Value::Null),
                            );
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            col_names.push(alias.value.clone());
                            result_row.push(
                                self.evaluate_value(expr, &empty_row, &empty_schema, params)
                                    .unwrap_or(Value::Null),
                            );
                        }
                        _ => {}
                    }
                }
                return Ok(QueryResult::Select {
                    columns: col_names,
                    rows: vec![result_row],
                });
            };

            let mut columns = Vec::with_capacity(select.projection.len());
            let mut is_count_star = false;

            if is_wildcard {
                columns = schema.columns.iter().map(|c| c.name.clone()).collect();
            } else {
                if select.projection.len() == 1 {
                    let expr = match &select.projection[0] {
                        SelectItem::UnnamedExpr(expr) => Some(expr),
                        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                        _ => None,
                    };

                    if let Some(Expr::Function(func)) = expr {
                        if func.name.to_string().to_uppercase() == "COUNT" {
                            if let FunctionArguments::List(args) = &func.args {
                                if args.args.len() == 1 {
                                    if let FunctionArg::Unnamed(FunctionArgExpr::Wildcard) =
                                        &args.args[0]
                                    {
                                        is_count_star = true;
                                        columns.push("COUNT(*)".to_string());
                                    }
                                }
                            }
                        }
                    }
                }

                if !is_count_star {
                    for item in &select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => {
                                if let Expr::Identifier(ident) = expr {
                                    columns.push(ident.value.clone());
                                } else {
                                    columns.push(format!("{}", expr));
                                }
                            }
                            SelectItem::ExprWithAlias { alias, .. } => {
                                columns.push(alias.value.clone());
                            }
                            SelectItem::QualifiedWildcard(_, _) => {
                                columns = schema.columns.iter().map(|c| c.name.clone()).collect();
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }

            if let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by {
                if !group_exprs.is_empty() {
                    let mut aggregates: Vec<(Expr, String)> = Vec::with_capacity(
                        select.projection.len() + usize::from(select.having.is_some()),
                    );

                    for item in &select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => {
                                self.extract_aggregates_from_expr(expr, &mut aggregates)
                            }
                            SelectItem::ExprWithAlias { expr, .. } => {
                                self.extract_aggregates_from_expr(expr, &mut aggregates)
                            }
                            _ => {}
                        }
                    }
                    if let Some(having) = &select.having {
                        self.extract_aggregates_from_expr(having, &mut aggregates);
                    }

                    let group_key_sources =
                        self.compile_group_key_sources(group_exprs, &schema, params);
                    let aggregate_plans =
                        self.compile_group_aggregate_plans(&aggregates, &schema, params);

                    let mut groups: std::collections::HashMap<
                        Vec<Value>,
                        Vec<AggregateAccumulator>,
                    > = std::collections::HashMap::with_capacity(rows.len());

                    for row in rows {
                        let mut group_key = Vec::with_capacity(group_exprs.len());
                        for source in &group_key_sources {
                            group_key.push(source.evaluate(self, &row, &schema, params));
                        }

                        let accs = groups.entry(group_key).or_insert_with(|| {
                            aggregate_plans
                                .iter()
                                .map(|plan| AggregateAccumulator::new(&plan.func_name))
                                .collect()
                        });

                        for (i, plan) in aggregate_plans.iter().enumerate() {
                            let arg_val = plan.arg_source.evaluate(self, &row, &schema, params);
                            accs[i].update(&arg_val);
                        }
                    }

                    let mut grouped_rows = Vec::with_capacity(groups.len());

                    for (group_key, accs) in groups {
                        let mut agg_map =
                            std::collections::HashMap::with_capacity(aggregates.len());
                        for (i, plan) in aggregate_plans.iter().enumerate() {
                            agg_map.insert(plan.expr.clone(), accs[i].finalize());
                        }

                        if let Some(having) = &select.having {
                            let val = self.evaluate_final_group_expr(
                                having,
                                &group_key,
                                group_exprs,
                                &agg_map,
                                &schema,
                                params,
                            )?;
                            let keep = match val {
                                Value::Boolean(b) => b,
                                Value::Null => false,
                                _ => {
                                    return Err(FusionError::Execution(
                                        "HAVING clause must return boolean".to_string(),
                                    ))
                                }
                            };
                            if !keep {
                                continue;
                            }
                        }

                        let mut new_row = Vec::with_capacity(select.projection.len());

                        for item in &select.projection {
                            let val = match item {
                                SelectItem::UnnamedExpr(expr) => self.evaluate_final_group_expr(
                                    expr,
                                    &group_key,
                                    group_exprs,
                                    &agg_map,
                                    &schema,
                                    params,
                                )?,
                                SelectItem::ExprWithAlias { expr, .. } => self
                                    .evaluate_final_group_expr(
                                        expr,
                                        &group_key,
                                        group_exprs,
                                        &agg_map,
                                        &schema,
                                        params,
                                    )?,
                                _ => Value::Null,
                            };
                            new_row.push(val);
                        }
                        grouped_rows.push(new_row);
                    }
                    rows = grouped_rows;

                    let new_cols: Vec<Column> = columns
                        .iter()
                        .map(|name| Column {
                            name: name.clone(),
                            data_type: "UNKNOWN".to_string(),
                            is_primary: false,
                            is_indexed: false,
                            index_type: IndexType::None,
                            default_value: None,
                            is_nullable: true,
                            is_unique: false,
                            check_expr: None,
                        })
                        .collect();
                    schema = TableSchema::new("temp_group_by_result".to_string(), new_cols);
                }
            }

            if let Some(order_by) = &query.order_by {
                if let OrderByKind::Expressions(exprs) = &order_by.kind {
                    let projection = &select.projection;
                    let rows_are_projected = matches!(
                        select.group_by,
                        sqlparser::ast::GroupByExpr::Expressions(ref group_exprs, _)
                            if !group_exprs.is_empty()
                    );
                    let rows_are_full_schema = is_wildcard && !rows_are_projected;
                    let sort_keys: Vec<SortOrderKey<'_>> = exprs
                        .iter()
                        .map(|order_expr| SortOrderKey {
                            source: self.resolve_order_value_source(
                                &order_expr.expr,
                                projection,
                                &columns,
                                &schema,
                                rows_are_projected,
                                rows_are_full_schema,
                            ),
                            asc: order_expr.options.asc.unwrap_or(true),
                        })
                        .collect();

                    let limit_window = limit.map(|value| {
                        if value == 0 {
                            0
                        } else {
                            offset.saturating_add(value)
                        }
                    });
                    self.sort_rows_by_order_keys(
                        &mut rows,
                        &sort_keys,
                        &schema,
                        params,
                        limit_window,
                    );
                }
            }

            let rows = rows.into_iter().skip(offset);
            let rows: Vec<Vec<Value>> = if let Some(limit) = limit {
                rows.take(limit).collect()
            } else {
                rows.collect()
            };

            if is_count_star {
                let count = rows.len();
                return Ok(QueryResult::Select {
                    columns,
                    rows: vec![vec![Value::Integer(count as i64)]],
                });
            }

            // Detect bare aggregates (e.g. SELECT COUNT(DISTINCT x), SUM(y) FROM t — no GROUP BY)
            let is_group_by_empty = matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty());
            if is_group_by_empty && !is_wildcard {
                let mut bare_aggs: Vec<(Expr, String)> =
                    Vec::with_capacity(select.projection.len());
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            self.extract_aggregates_from_expr(expr, &mut bare_aggs)
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            self.extract_aggregates_from_expr(expr, &mut bare_aggs)
                        }
                        _ => {}
                    }
                }
                if !bare_aggs.is_empty() {
                    let aggregate_plans =
                        self.compile_group_aggregate_plans(&bare_aggs, &schema, params);
                    let mut accs: Vec<AggregateAccumulator> = bare_aggs
                        .iter()
                        .map(|(_, name)| AggregateAccumulator::new(name))
                        .collect();
                    for row in &rows {
                        for (i, plan) in aggregate_plans.iter().enumerate() {
                            let arg_val = plan.arg_source.evaluate(self, row, &schema, params);
                            accs[i].update(&arg_val);
                        }
                    }
                    let result_row: Vec<Value> = accs.iter().map(|a| a.finalize()).collect();
                    return Ok(QueryResult::Select {
                        columns,
                        rows: vec![result_row],
                    });
                }
            }

            // Pre-compute window functions for each projection column
            // window_results[col_idx] = Some(vec_of_values_per_row) if that column is a window fn
            let window_results: Vec<Option<Vec<Value>>> = if !is_wildcard
                && matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty())
            {
                select
                    .projection
                    .iter()
                    .map(|item| {
                        let expr = match item {
                            SelectItem::UnnamedExpr(e) => Some(e),
                            SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
                            _ => None,
                        };
                        if let Some(Expr::Function(func)) = expr {
                            let fname = func.name.to_string().to_uppercase();
                            if matches!(
                                fname.as_str(),
                                "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "LAG" | "LEAD"
                            ) {
                                if let Some(ref over) = func.over {
                                    if let sqlparser::ast::WindowType::WindowSpec(spec) = over {
                                        return Some(self.compute_window_function(
                                            &fname, spec, func, &rows, &schema, params,
                                        ));
                                    }
                                }
                            }
                        }
                        None
                    })
                    .collect()
            } else {
                vec![]
            };

            let final_rows = if is_wildcard
                || matches!(select.group_by, sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if !exprs.is_empty())
            {
                rows
            } else {
                let mut projected_rows = Vec::with_capacity(rows.len());
                for (row_idx, row) in rows.iter().enumerate() {
                    let mut new_row = Vec::with_capacity(select.projection.len());
                    for (col_idx, item) in select.projection.iter().enumerate() {
                        // Check if this column has pre-computed window function results
                        if let Some(Some(ref wvals)) = window_results.get(col_idx) {
                            new_row.push(wvals.get(row_idx).cloned().unwrap_or(Value::Null));
                            continue;
                        }
                        let val = match item {
                            SelectItem::UnnamedExpr(expr) => self
                                .evaluate_value(expr, row, &schema, params)
                                .unwrap_or(Value::Null),
                            SelectItem::ExprWithAlias { expr, .. } => self
                                .evaluate_value(expr, row, &schema, params)
                                .unwrap_or(Value::Null),
                            _ => Value::Null,
                        };
                        new_row.push(val);
                    }
                    projected_rows.push(new_row);
                }
                projected_rows
            };

            // Apply DISTINCT
            let final_rows = if select.distinct.is_some() {
                Self::deduplicate_rows(final_rows)
            } else {
                final_rows
            };

            return Ok(QueryResult::Select {
                columns,
                rows: final_rows,
            });
        }
        // Handle UNION / UNION ALL / INTERSECT / EXCEPT
        if let SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } = query.body.as_ref()
        {
            use sqlparser::ast::{SetOperator, SetQuantifier};

            let make_query = |body: Box<SetExpr>| sqlparser::ast::Query {
                body,
                order_by: None,
                limit_clause: None,
                with: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: vec![],
            };

            let left_result =
                Box::pin(self.handle_query(&make_query(left.clone()), txn, params)).await?;
            let right_result =
                Box::pin(self.handle_query(&make_query(right.clone()), txn, params)).await?;

            let (left_cols, left_rows) = match left_result {
                QueryResult::Select { columns, rows } => (columns, rows),
                _ => {
                    return Err(FusionError::Execution(
                        "UNION left side must be SELECT".to_string(),
                    ))
                }
            };
            let (_, right_rows) = match right_result {
                QueryResult::Select { columns, rows } => (columns, rows),
                _ => {
                    return Err(FusionError::Execution(
                        "UNION right side must be SELECT".to_string(),
                    ))
                }
            };

            let mut combined = match op {
                SetOperator::Union => {
                    let mut all_rows = left_rows;
                    all_rows.extend(right_rows);
                    all_rows
                }
                SetOperator::Intersect => {
                    let right_set: HashSet<Vec<Value>> = right_rows.into_iter().collect();
                    left_rows
                        .into_iter()
                        .filter(|row| right_set.contains(row))
                        .collect()
                }
                SetOperator::Except => {
                    let right_set: HashSet<Vec<Value>> = right_rows.into_iter().collect();
                    left_rows
                        .into_iter()
                        .filter(|row| !right_set.contains(row))
                        .collect()
                }
                _ => {
                    return Err(FusionError::Execution(format!(
                        "Unsupported set operator: {:?}",
                        op
                    )))
                }
            };

            // Deduplicate unless ALL
            let is_all = matches!(
                set_quantifier,
                SetQuantifier::All | SetQuantifier::AllByName
            );
            if !is_all {
                combined = Self::deduplicate_rows(combined);
            }

            let (set_offset, set_limit) =
                if let Some(LimitClause::LimitOffset { limit, offset, .. }) = &query.limit_clause {
                    let off = offset
                        .as_ref()
                        .and_then(|os| match &os.value {
                            Expr::Value(sqlparser::ast::ValueWithSpan {
                                value: sqlparser::ast::Value::Number(n, _),
                                ..
                            }) => n.parse::<usize>().ok(),
                            _ => None,
                        })
                        .unwrap_or(0);
                    let lim = limit.as_ref().and_then(|e| match e {
                        Expr::Value(sqlparser::ast::ValueWithSpan {
                            value: sqlparser::ast::Value::Number(n, _),
                            ..
                        }) => n.parse::<usize>().ok(),
                        _ => None,
                    });
                    (off, lim)
                } else {
                    (0, None)
                };

            // Apply ORDER BY from the outer query
            if let Some(order_by) = &query.order_by {
                if let OrderByKind::Expressions(order_exprs) = &order_by.kind {
                    let compare_combined = |a: &(usize, Vec<Value>), b: &(usize, Vec<Value>)| {
                        for oe in order_exprs {
                            let idx = match &oe.expr {
                                Expr::Value(sqlparser::ast::ValueWithSpan {
                                    value: sqlparser::ast::Value::Number(n, _),
                                    ..
                                }) => n.parse::<usize>().unwrap_or(1) - 1,
                                Expr::Identifier(ident) => left_cols
                                    .iter()
                                    .position(|c| c == &ident.value)
                                    .unwrap_or(0),
                                _ => 0,
                            };
                            let va = a.1.get(idx).unwrap_or(&Value::Null);
                            let vb = b.1.get(idx).unwrap_or(&Value::Null);
                            let cmp = self.compare_for_sort(va, vb);
                            if cmp != Ordering::Equal {
                                return if oe.options.asc.unwrap_or(true) {
                                    cmp
                                } else {
                                    cmp.reverse()
                                };
                            }
                        }
                        a.0.cmp(&b.0)
                    };

                    if let Some(limit) = set_limit {
                        let window = if limit == 0 {
                            0
                        } else {
                            set_offset.saturating_add(limit)
                        };
                        if window == 0 {
                            combined.clear();
                        } else if window < combined.len() {
                            let mut indexed_rows: Vec<(usize, Vec<Value>)> =
                                combined.into_iter().enumerate().collect();
                            let _ = indexed_rows.select_nth_unstable_by(window, compare_combined);
                            indexed_rows.truncate(window);
                            indexed_rows.sort_by(compare_combined);
                            combined = indexed_rows.into_iter().map(|(_, row)| row).collect();
                        } else {
                            let mut indexed_rows: Vec<(usize, Vec<Value>)> =
                                combined.into_iter().enumerate().collect();
                            indexed_rows.sort_by(compare_combined);
                            combined = indexed_rows.into_iter().map(|(_, row)| row).collect();
                        }
                    } else {
                        let mut indexed_rows: Vec<(usize, Vec<Value>)> =
                            combined.into_iter().enumerate().collect();
                        indexed_rows.sort_by(compare_combined);
                        combined = indexed_rows.into_iter().map(|(_, row)| row).collect();
                    }
                }
            }

            // Apply LIMIT/OFFSET from the outer query
            if query.limit_clause.is_some() {
                combined = if let Some(limit) = set_limit {
                    combined.into_iter().skip(set_offset).take(limit).collect()
                } else {
                    combined.into_iter().skip(set_offset).collect()
                };
            }

            return Ok(QueryResult::Select {
                columns: left_cols,
                rows: combined,
            });
        }

        Err(FusionError::Execution(
            "Unsupported SELECT format".to_string(),
        ))
    }

    /// Compute window function values for all rows.
    /// Returns a Vec<Value> with one value per input row.
    fn compute_window_function(
        &self,
        func_name: &str,
        spec: &sqlparser::ast::WindowSpec,
        func: &sqlparser::ast::Function,
        rows: &[Vec<Value>],
        schema: &TableSchema,
        params: &[Value],
    ) -> Vec<Value> {
        if rows.is_empty() {
            return vec![];
        }

        let partitions: Vec<Vec<usize>> = if spec.partition_by.is_empty() {
            vec![(0..rows.len()).collect()]
        } else {
            let mut partitions: HashMap<Vec<Value>, Vec<usize>> =
                HashMap::with_capacity(rows.len());
            for (i, row) in rows.iter().enumerate() {
                let mut partition_key = Vec::with_capacity(spec.partition_by.len());
                for expr in &spec.partition_by {
                    partition_key.push(
                        self.evaluate_value(expr, row, schema, params)
                            .unwrap_or(Value::Null),
                    );
                }
                partitions.entry(partition_key).or_default().push(i);
            }
            partitions.into_values().collect()
        };

        let mut result = vec![Value::Null; rows.len()];

        for indices in &partitions {
            // Sort indices within partition by ORDER BY
            let mut sorted_indices: Vec<usize> = indices.clone();
            if !spec.order_by.is_empty() {
                sorted_indices.sort_by(|&a, &b| {
                    for oe in &spec.order_by {
                        let va = self
                            .evaluate_value(&oe.expr, &rows[a], schema, params)
                            .unwrap_or(Value::Null);
                        let vb = self
                            .evaluate_value(&oe.expr, &rows[b], schema, params)
                            .unwrap_or(Value::Null);
                        let cmp = self.compare_for_sort(&va, &vb);
                        if cmp != Ordering::Equal {
                            return if oe.options.asc.unwrap_or(true) {
                                cmp
                            } else {
                                cmp.reverse()
                            };
                        }
                    }
                    Ordering::Equal
                });
            }

            match func_name {
                "ROW_NUMBER" => {
                    for (rank, &row_idx) in sorted_indices.iter().enumerate() {
                        result[row_idx] = Value::Integer((rank + 1) as i64);
                    }
                }
                "RANK" => {
                    let mut rank = 1i64;
                    let mut i = 0;
                    while i < sorted_indices.len() {
                        let current_idx = sorted_indices[i];
                        // Count ties
                        let mut tie_count = 1;
                        while i + tie_count < sorted_indices.len() {
                            let next_idx = sorted_indices[i + tie_count];
                            let same = spec.order_by.iter().all(|oe| {
                                let va = self
                                    .evaluate_value(&oe.expr, &rows[current_idx], schema, params)
                                    .unwrap_or(Value::Null);
                                let vb = self
                                    .evaluate_value(&oe.expr, &rows[next_idx], schema, params)
                                    .unwrap_or(Value::Null);
                                va == vb
                            });
                            if same {
                                tie_count += 1;
                            } else {
                                break;
                            }
                        }
                        for j in 0..tie_count {
                            result[sorted_indices[i + j]] = Value::Integer(rank);
                        }
                        rank += tie_count as i64;
                        i += tie_count;
                    }
                }
                "DENSE_RANK" => {
                    let mut rank = 1i64;
                    let mut i = 0;
                    while i < sorted_indices.len() {
                        let current_idx = sorted_indices[i];
                        let mut tie_count = 1;
                        while i + tie_count < sorted_indices.len() {
                            let next_idx = sorted_indices[i + tie_count];
                            let same = spec.order_by.iter().all(|oe| {
                                let va = self
                                    .evaluate_value(&oe.expr, &rows[current_idx], schema, params)
                                    .unwrap_or(Value::Null);
                                let vb = self
                                    .evaluate_value(&oe.expr, &rows[next_idx], schema, params)
                                    .unwrap_or(Value::Null);
                                va == vb
                            });
                            if same {
                                tie_count += 1;
                            } else {
                                break;
                            }
                        }
                        for j in 0..tie_count {
                            result[sorted_indices[i + j]] = Value::Integer(rank);
                        }
                        rank += 1; // Dense: always increment by 1
                        i += tie_count;
                    }
                }
                "LAG" | "LEAD" => {
                    // Extract offset (default 1) and default value (default NULL)
                    let (offset, default_val) = if let FunctionArguments::List(args) = &func.args {
                        let off = if args.args.len() >= 2 {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = &args.args[1] {
                                if let Ok(Value::Integer(n)) =
                                    self.evaluate_value(e, &rows[0], schema, params)
                                {
                                    n as usize
                                } else {
                                    1
                                }
                            } else {
                                1
                            }
                        } else {
                            1
                        };
                        let def = if args.args.len() >= 3 {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = &args.args[2] {
                                self.evaluate_value(e, &rows[0], schema, params)
                                    .unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            }
                        } else {
                            Value::Null
                        };
                        (off, def)
                    } else {
                        (1, Value::Null)
                    };

                    // Get the expression argument (first arg)
                    let arg_expr = if let FunctionArguments::List(args) = &func.args {
                        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) =
                            args.args.first()
                        {
                            Some(e.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    if let Some(ref arg_e) = arg_expr {
                        for (pos, &row_idx) in sorted_indices.iter().enumerate() {
                            let target_pos = if func_name == "LAG" {
                                if pos >= offset {
                                    Some(pos - offset)
                                } else {
                                    None
                                }
                            } else {
                                // LEAD
                                if pos + offset < sorted_indices.len() {
                                    Some(pos + offset)
                                } else {
                                    None
                                }
                            };
                            let val = if let Some(tp) = target_pos {
                                let target_row_idx = sorted_indices[tp];
                                self.evaluate_value(arg_e, &rows[target_row_idx], schema, params)
                                    .unwrap_or(default_val.clone())
                            } else {
                                default_val.clone()
                            };
                            result[row_idx] = val;
                        }
                    }
                }
                _ => {}
            }
        }

        result
    }
}
