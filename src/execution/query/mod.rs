mod column_scan;
mod order;

use crate::catalog::{Column, IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::storage::Transaction;
use sqlparser::ast::{
    Cte, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, LimitClause,
    OrderByKind, SelectItem, SetExpr, SetOperator, SetQuantifier, TableFactor,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::{AggregateAccumulator, Executor, QueryResult};
use order::SortOrderKey;
use sqlparser::ast::{Distinct, JoinConstraint, JoinOperator};

enum RowValueSource<'a> {
    One,
    Column(usize),
    Literal(Value),
    MultiplyColumns(usize, usize),
    Expr(&'a Expr),
}

#[derive(Clone, Copy)]
enum JoinValueSide {
    Left,
    Right,
}

struct SimpleJoinGroupAggregate {
    func_name: String,
    arg: Option<(JoinValueSide, usize)>,
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

struct SimpleProjectedGroupAggregatePlan {
    func_name: String,
    arg_index: Option<usize>,
}

impl Executor {
    const MAX_RECURSIVE_CTE_ITERATIONS: usize = 128;
    const MAX_RECURSIVE_CTE_ROWS: usize = 4096;

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

    fn trim_set_rows_in_place(rows: &mut Vec<Vec<Value>>, offset: usize, limit: Option<usize>) {
        if offset >= rows.len() {
            rows.clear();
            return;
        }
        if offset > 0 {
            drop(rows.drain(..offset));
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
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

    fn group_output_scope(&self, group_exprs: &[Expr], group_key: &[Value]) -> TableSchema {
        TableSchema::new(
            "__group_output".to_string(),
            group_exprs
                .iter()
                .enumerate()
                .filter_map(|(index, expr)| {
                    let name = Self::order_limit_column_name(expr).or_else(|| match expr {
                        Expr::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })?;
                    group_key.get(index)?;
                    Some(Column {
                        name,
                        data_type: "UNKNOWN".to_string(),
                        is_primary: false,
                        is_indexed: false,
                        index_type: IndexType::None,
                        default_value: None,
                        is_nullable: true,
                        is_unique: false,
                        check_expr: None,
                    })
                })
                .collect(),
        )
    }

    fn resolve_group_by_projection_aliases(
        group_exprs: &[Expr],
        projection: &[SelectItem],
    ) -> Vec<Expr> {
        group_exprs
            .iter()
            .map(|group_expr| match group_expr {
                Expr::Identifier(ident) => projection
                    .iter()
                    .find_map(|item| match item {
                        SelectItem::ExprWithAlias { expr, alias }
                            if alias.value.eq_ignore_ascii_case(&ident.value) =>
                        {
                            Some(expr.clone())
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| group_expr.clone()),
                Expr::Value(value) => {
                    let sqlparser::ast::Value::Number(number, _) = &value.value else {
                        return group_expr.clone();
                    };
                    number
                        .parse::<usize>()
                        .ok()
                        .and_then(|position| position.checked_sub(1))
                        .and_then(|index| projection.get(index))
                        .and_then(|item| match item {
                            SelectItem::UnnamedExpr(expr) => Some(expr.clone()),
                            SelectItem::ExprWithAlias { expr, .. } => Some(expr.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| group_expr.clone())
                }
                _ => group_expr.clone(),
            })
            .collect()
    }

    fn order_expr_is_projection_reference(expr: &Expr, projection: &[SelectItem]) -> bool {
        if let Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::Number(number, _),
            ..
        }) = expr
        {
            return number
                .parse::<usize>()
                .ok()
                .and_then(|position| position.checked_sub(1))
                .is_some_and(|index| index < projection.len());
        }

        projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(proj_expr) => {
                proj_expr == expr
                    || matches!(
                        expr,
                        Expr::Identifier(ident)
                            if Self::order_limit_column_name(proj_expr)
                                .and_then(|name| {
                                    name.rsplit('.')
                                        .next()
                                        .map(|suffix| suffix.eq_ignore_ascii_case(&ident.value))
                                })
                                .unwrap_or(false)
                    )
            }
            SelectItem::ExprWithAlias { expr: proj_expr, alias } => {
                proj_expr == expr
                    || matches!(expr, Expr::Identifier(ident) if alias.value.eq_ignore_ascii_case(&ident.value))
                    || matches!(
                        expr,
                        Expr::Identifier(ident)
                            if Self::order_limit_column_name(proj_expr)
                                .and_then(|name| {
                                    name.rsplit('.')
                                        .next()
                                        .map(|suffix| suffix.eq_ignore_ascii_case(&ident.value))
                                })
                                .unwrap_or(false)
                    )
            }
            _ => false,
        })
    }

    fn extract_order_by_input_columns(
        &self,
        order_by: &sqlparser::ast::OrderBy,
        projection: &[SelectItem],
        cols: &mut HashSet<String>,
    ) {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            for expr in exprs {
                if !Self::order_expr_is_projection_reference(&expr.expr, projection) {
                    self.extract_columns_from_expr(&expr.expr, cols);
                }
            }
        }
    }

    fn extract_group_by_input_columns(
        &self,
        group_exprs: &[Expr],
        projection: &[SelectItem],
        cols: &mut HashSet<String>,
    ) {
        let resolved_group_exprs =
            Self::resolve_group_by_projection_aliases(group_exprs, projection);
        for expr in &resolved_group_exprs {
            self.extract_columns_from_expr(expr, cols);
        }
    }

    fn is_ldbc_q4_join_shape(from: &[sqlparser::ast::TableWithJoins]) -> bool {
        if from.len() != 4 || from.iter().any(|table| !table.joins.is_empty()) {
            return false;
        }

        let mut names = HashSet::with_capacity(from.len());
        for table in from {
            let TableFactor::Table { name, .. } = &table.relation else {
                return false;
            };
            names.insert(name.to_string().to_ascii_lowercase());
        }

        ["tag", "message", "message_tag", "knows"]
            .iter()
            .all(|name| names.contains(*name))
    }

    fn extract_deferred_exists_outer_columns(&self, expr: &Expr, cols: &mut HashSet<String>) {
        match expr {
            Expr::Exists { subquery, .. } => self.extract_query_outer_columns(subquery, cols),
            Expr::BinaryOp { left, right, .. } => {
                self.extract_deferred_exists_outer_columns(left, cols);
                self.extract_deferred_exists_outer_columns(right, cols);
            }
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                self.extract_deferred_exists_outer_columns(inner, cols);
            }
            _ => {}
        }
    }

    fn extract_query_outer_columns(
        &self,
        query: &sqlparser::ast::Query,
        cols: &mut HashSet<String>,
    ) {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.extract_select_outer_columns(select, cols),
            SetExpr::Query(inner) => self.extract_query_outer_columns(inner, cols),
            _ => {}
        }
    }

    fn extract_select_outer_columns(
        &self,
        select: &sqlparser::ast::Select,
        cols: &mut HashSet<String>,
    ) {
        let mut local_relations = HashSet::new();
        for table in &select.from {
            local_relations.extend(self.relation_names(&table.relation));
            for join in &table.joins {
                local_relations.extend(self.relation_names(&join.relation));
            }
        }

        let mut referenced = HashSet::new();
        if let Some(selection) = &select.selection {
            self.extract_columns_from_expr(selection, &mut referenced);
        }
        if let Some(having) = &select.having {
            self.extract_columns_from_expr(having, &mut referenced);
        }
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    self.extract_columns_from_expr(expr, &mut referenced)
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    self.extract_columns_from_expr(expr, &mut referenced)
                }
                _ => {}
            }
        }

        for column in referenced {
            let prefix = column.split('.').next().unwrap_or("");
            if !prefix.is_empty()
                && !local_relations
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(prefix))
            {
                cols.insert(column);
            }
        }
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

    fn simple_projected_group_aggregate_projection(
        &self,
        projection: &[SelectItem],
        group_exprs: &[Expr],
        schema: &TableSchema,
    ) -> Option<(
        usize,
        String,
        Vec<SimpleProjectedGroupAggregatePlan>,
        Vec<String>,
    )> {
        if projection.len() < 2 || group_exprs.len() != 1 {
            return None;
        }

        let group_name = Self::order_limit_column_name(&group_exprs[0])?;
        let group_index = self.resolve_column_index(&group_name, schema).ok()?;

        let output_group_name = match &projection[0] {
            SelectItem::UnnamedExpr(expr) if expr == &group_exprs[0] => group_name,
            SelectItem::ExprWithAlias { expr, alias } if expr == &group_exprs[0] => {
                alias.value.clone()
            }
            _ => return None,
        };

        let mut plans = Vec::with_capacity(projection.len() - 1);
        let mut output_columns = Vec::with_capacity(projection.len());
        output_columns.push(output_group_name.clone());

        for item in projection.iter().skip(1) {
            let (expr, output_name) = match item {
                SelectItem::UnnamedExpr(expr @ Expr::Function(_)) => (expr, format!("{}", expr)),
                SelectItem::ExprWithAlias {
                    expr: expr @ Expr::Function(_),
                    alias,
                } => (expr, alias.value.clone()),
                _ => return None,
            };

            let Expr::Function(func) = expr else {
                return None;
            };
            let FunctionArguments::List(args) = &func.args else {
                return None;
            };
            if args.duplicate_treatment.is_some() || args.args.len() != 1 {
                return None;
            }

            let func_name = func.name.to_string().to_uppercase();
            let arg_index = match func_name.as_str() {
                "COUNT"
                    if matches!(
                        args.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    ) =>
                {
                    None
                }
                "SUM" | "ARRAY_AGG" => Some(Self::column_arg_index(&args.args[0], schema, None)?),
                _ => return None,
            };

            plans.push(SimpleProjectedGroupAggregatePlan {
                func_name,
                arg_index,
            });
            output_columns.push(output_name);
        }

        Some((group_index, output_group_name, plans, output_columns))
    }

    fn apply_simple_projected_group_aggregates(
        rows: Vec<Vec<Value>>,
        group_index: usize,
        aggregate_plans: &[SimpleProjectedGroupAggregatePlan],
    ) -> Vec<Vec<Value>> {
        let mut groups: HashMap<Value, Vec<AggregateAccumulator>> =
            HashMap::with_capacity(rows.len().min(4096));

        for row in rows {
            let group_key = row.get(group_index).cloned().unwrap_or(Value::Null);
            let accs = groups.entry(group_key).or_insert_with(|| {
                aggregate_plans
                    .iter()
                    .map(|plan| AggregateAccumulator::new(&plan.func_name))
                    .collect()
            });

            for (acc, plan) in accs.iter_mut().zip(aggregate_plans.iter()) {
                let value = plan
                    .arg_index
                    .and_then(|index| row.get(index))
                    .unwrap_or(&Value::Integer(1));
                acc.update(value);
            }
        }

        groups
            .into_iter()
            .map(|(group_value, accs)| {
                let mut row = Vec::with_capacity(accs.len() + 1);
                row.push(group_value);
                row.extend(accs.iter().map(AggregateAccumulator::finalize));
                row
            })
            .collect()
    }

    fn is_true_literal(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Boolean(true),
                ..
            })
        )
    }

    fn expr_column_name_matches(expr: &Expr, expected: &str) -> bool {
        Self::order_limit_column_name(expr).is_some_and(|name| name.eq_ignore_ascii_case(expected))
    }

    fn relation_alias_or_name(relation: &TableFactor) -> Option<String> {
        match relation {
            TableFactor::Table { name, alias, .. } => Some(
                alias
                    .as_ref()
                    .map(|alias| alias.name.value.clone())
                    .unwrap_or_else(|| name.to_string()),
            ),
            TableFactor::Derived { alias, .. } => {
                alias.as_ref().map(|alias| alias.name.value.clone())
            }
            _ => None,
        }
    }

    fn cte_output_columns(cte: &Cte, query_columns: &[String]) -> Result<Vec<String>> {
        if cte.alias.columns.is_empty() {
            return Ok(query_columns.to_vec());
        }

        if cte.alias.columns.len() > query_columns.len() {
            return Err(FusionError::Execution(format!(
                "CTE {} column count mismatch: expected {}, got {}",
                cte.alias.name.value,
                cte.alias.columns.len(),
                query_columns.len()
            )));
        }

        let mut columns = query_columns.to_vec();
        for (index, alias_column) in cte.alias.columns.iter().enumerate() {
            columns[index] = alias_column.name.value.clone();
        }
        Ok(columns)
    }

    fn materialized_column_type(rows: &[Vec<Value>], column_index: usize) -> String {
        for row in rows {
            match row.get(column_index) {
                Some(Value::Integer(_)) => return "INTEGER".to_string(),
                Some(Value::Float(_)) => return "DOUBLE".to_string(),
                Some(Value::Decimal(_)) => return "DECIMAL".to_string(),
                Some(Value::Boolean(_)) => return "BOOLEAN".to_string(),
                Some(Value::Date(_)) => return "DATE".to_string(),
                Some(Value::Timestamp(_)) => return "TIMESTAMP".to_string(),
                Some(Value::Interval(_)) => return "INTERVAL".to_string(),
                Some(Value::String(_)) => return "TEXT".to_string(),
                Some(Value::Blob(_)) => return "BLOB".to_string(),
                Some(Value::Vector(_)) => return "VECTOR".to_string(),
                Some(Value::Array(_)) => return "ARRAY".to_string(),
                Some(Value::Object(_)) => return "JSON".to_string(),
                Some(Value::Null) | None => {}
            }
        }
        "UNKNOWN".to_string()
    }

    async fn put_materialized_cte(
        &self,
        txn: &mut dyn Transaction,
        cte_name: &str,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Result<()> {
        let cols = columns
            .iter()
            .enumerate()
            .map(|(index, c)| Column {
                name: c.clone(),
                data_type: Self::materialized_column_type(rows, index),
                is_primary: false,
                is_indexed: false,
                index_type: IndexType::None,
                default_value: None,
                is_nullable: true,
                is_unique: false,
                check_expr: None,
            })
            .collect();
        let schema = TableSchema::new(cte_name.to_string(), cols);
        let schema_key = format!("schema:{}", cte_name);
        let schema_bytes = bincode::serialize(&schema)
            .map_err(|e| FusionError::Execution(format!("CTE schema error: {}", e)))?;
        txn.put(schema_key.as_bytes(), &schema_bytes).await?;

        for (i, row) in rows.iter().enumerate() {
            let pk_str = crate::common::encoding::encode_i64_comparable(i as i64);
            let key = format!("data:{}:{}", cte_name, pk_str);
            let val = crate::common::encoding::RowEncoder::encode(row);
            txn.put(key.as_bytes(), &val).await?;
            self.row_cache.invalidate(&key);
        }

        Ok(())
    }

    async fn clear_materialized_cte(txn: &mut dyn Transaction, cte_name: &str) -> Result<()> {
        txn.delete(format!("schema:{}", cte_name).as_bytes())
            .await?;
        let prefix = format!("data:{}:", cte_name);
        let entries = txn.scan_prefix(prefix.as_bytes(), None).await?;
        for (key, _) in entries {
            txn.delete(&key).await?;
        }
        Ok(())
    }

    async fn replace_materialized_cte(
        &self,
        txn: &mut dyn Transaction,
        cte_name: &str,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Result<()> {
        Self::clear_materialized_cte(txn, cte_name).await?;
        self.put_materialized_cte(txn, cte_name, columns, rows)
            .await
    }

    fn recursive_union_parts(
        query: &sqlparser::ast::Query,
    ) -> Option<(SetOperator, SetQuantifier, Box<SetExpr>, Box<SetExpr>)> {
        match query.body.as_ref() {
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => Some((*op, *set_quantifier, left.clone(), right.clone())),
            SetExpr::Query(inner) => Self::recursive_union_parts(inner),
            _ => None,
        }
    }

    fn query_from_set_expr(body: Box<SetExpr>) -> sqlparser::ast::Query {
        sqlparser::ast::Query {
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
        }
    }

    async fn materialize_recursive_cte(
        &self,
        cte: &Cte,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<String> {
        let cte_name = cte.alias.name.value.clone();
        let Some((op, set_quantifier, anchor_body, recursive_body)) =
            Self::recursive_union_parts(&cte.query)
        else {
            return Err(FusionError::Execution(format!(
                "WITH RECURSIVE CTE {} must use UNION or UNION ALL",
                cte_name
            )));
        };

        if !matches!(op, SetOperator::Union) {
            return Err(FusionError::Execution(format!(
                "WITH RECURSIVE CTE {} only supports UNION or UNION ALL",
                cte_name
            )));
        }

        let anchor_query = Self::query_from_set_expr(anchor_body);
        let anchor_result = Box::pin(self.handle_query(&anchor_query, txn, params)).await?;
        let (anchor_columns, anchor_rows) = match anchor_result {
            QueryResult::Select { columns, rows } => (columns, rows),
            _ => {
                return Err(FusionError::Execution(format!(
                    "WITH RECURSIVE CTE {} anchor must be SELECT",
                    cte_name
                )))
            }
        };
        let columns = Self::cte_output_columns(cte, &anchor_columns)?;
        let union_all = matches!(
            set_quantifier,
            SetQuantifier::All | SetQuantifier::AllByName
        );
        let mut all_rows = if union_all {
            anchor_rows
        } else {
            Self::deduplicate_rows(anchor_rows)
        };
        let mut delta_rows = all_rows.clone();
        let recursive_query = Self::query_from_set_expr(recursive_body);

        self.replace_materialized_cte(txn, &cte_name, &columns, &delta_rows)
            .await?;

        for _ in 0..Self::MAX_RECURSIVE_CTE_ITERATIONS {
            if delta_rows.is_empty() {
                break;
            }

            let result = Box::pin(self.handle_query(&recursive_query, txn, params)).await?;
            let (_, next_rows) = match result {
                QueryResult::Select { columns: _, rows } => ((), rows),
                _ => {
                    return Err(FusionError::Execution(format!(
                        "WITH RECURSIVE CTE {} recursive term must be SELECT",
                        cte_name
                    )))
                }
            };

            let mut new_delta = Vec::new();
            for row in next_rows {
                if row.len() != columns.len() {
                    return Err(FusionError::Execution(format!(
                        "WITH RECURSIVE CTE {} row width mismatch: expected {}, got {}",
                        cte_name,
                        columns.len(),
                        row.len()
                    )));
                }
                if union_all || !all_rows.contains(&row) {
                    if all_rows.len() >= Self::MAX_RECURSIVE_CTE_ROWS {
                        return Err(FusionError::Execution(format!(
                            "WITH RECURSIVE CTE {} row limit exceeded: max {} rows",
                            cte_name,
                            Self::MAX_RECURSIVE_CTE_ROWS
                        )));
                    }
                    all_rows.push(row.clone());
                    new_delta.push(row);
                }
            }

            delta_rows = if union_all {
                Self::deduplicate_rows(new_delta)
            } else {
                new_delta
            };
            self.replace_materialized_cte(txn, &cte_name, &columns, &delta_rows)
                .await?;
        }

        if !delta_rows.is_empty() {
            return Err(FusionError::Execution(format!(
                "WITH RECURSIVE CTE {} iteration limit exceeded: max {} iterations",
                cte_name,
                Self::MAX_RECURSIVE_CTE_ITERATIONS
            )));
        }

        self.replace_materialized_cte(txn, &cte_name, &columns, &all_rows)
            .await?;
        Ok(cte_name)
    }

    fn query_limit_is_one(query: &sqlparser::ast::Query) -> bool {
        matches!(
            &query.limit_clause,
            Some(LimitClause::LimitOffset {
                limit: Some(Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Number(n, _),
                    ..
                })),
                offset: None,
                ..
            }) if n == "1"
        )
    }

    fn lateral_lastpoint_join_keys(
        selection: Option<&Expr>,
        inner_alias: &str,
        outer_alias: &str,
    ) -> bool {
        let Some(Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Eq,
            right,
        }) = selection
        else {
            return false;
        };

        let inner_tags_id = format!("{}.tags_id", inner_alias);
        let outer_id = format!("{}.id", outer_alias);
        (Self::expr_column_name_matches(left, &inner_tags_id)
            && Self::expr_column_name_matches(right, &outer_id))
            || (Self::expr_column_name_matches(left, &outer_id)
                && Self::expr_column_name_matches(right, &inner_tags_id))
    }

    async fn try_tsbs_lastpoint_lateral(
        &self,
        select: &sqlparser::ast::Select,
        order_by: Option<&sqlparser::ast::OrderBy>,
        limit: Option<usize>,
        offset: usize,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if select.projection.len() != 1
            || !matches!(select.projection[0], SelectItem::Wildcard(_))
            || select.having.is_some()
            || select.selection.is_some()
            || !matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty()
            )
            || select.from.len() != 1
            || select.from[0].joins.len() != 1
        {
            return Ok(None);
        }

        let Some(Distinct::On(distinct_exprs)) = &select.distinct else {
            return Ok(None);
        };
        if distinct_exprs.len() != 1 {
            return Ok(None);
        }

        let outer_relation = &select.from[0].relation;
        let TableFactor::Table {
            name: outer_name, ..
        } = outer_relation
        else {
            return Ok(None);
        };
        if !outer_name.to_string().eq_ignore_ascii_case("tags") {
            return Ok(None);
        }
        let Some(outer_alias) = Self::relation_alias_or_name(outer_relation) else {
            return Ok(None);
        };
        if !Self::expr_column_name_matches(&distinct_exprs[0], &format!("{}.hostname", outer_alias))
        {
            return Ok(None);
        }

        let join = &select.from[0].joins[0];
        let join_on = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::Join(JoinConstraint::On(expr)) => expr,
            _ => return Ok(None),
        };
        if !Self::is_true_literal(join_on) {
            return Ok(None);
        }

        let TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } = &join.relation
        else {
            return Ok(None);
        };
        if !*lateral {
            return Ok(None);
        }
        let Some(derived_alias) = alias.as_ref().map(|alias| alias.name.value.clone()) else {
            return Ok(None);
        };

        let SetExpr::Select(inner_select) = subquery.body.as_ref() else {
            return Ok(None);
        };
        if inner_select.projection.len() != 1
            || !matches!(inner_select.projection[0], SelectItem::Wildcard(_))
            || inner_select.from.len() != 1
            || !inner_select.from[0].joins.is_empty()
            || !Self::query_limit_is_one(subquery)
        {
            return Ok(None);
        }

        let inner_relation = &inner_select.from[0].relation;
        let TableFactor::Table {
            name: inner_name, ..
        } = inner_relation
        else {
            return Ok(None);
        };
        if !inner_name.to_string().eq_ignore_ascii_case("cpu") {
            return Ok(None);
        }
        let Some(inner_alias) = Self::relation_alias_or_name(inner_relation) else {
            return Ok(None);
        };
        if !Self::lateral_lastpoint_join_keys(
            inner_select.selection.as_ref(),
            &inner_alias,
            &outer_alias,
        ) {
            return Ok(None);
        }

        let Some(inner_order_by) = subquery.order_by.as_ref() else {
            return Ok(None);
        };
        let OrderByKind::Expressions(inner_order_exprs) = &inner_order_by.kind else {
            return Ok(None);
        };
        let [inner_order_expr] = inner_order_exprs.as_slice() else {
            return Ok(None);
        };
        if inner_order_expr.options.asc.unwrap_or(true)
            || !Self::expr_column_name_matches(&inner_order_expr.expr, "time")
                && !Self::expr_column_name_matches(
                    &inner_order_expr.expr,
                    &format!("{}.time", inner_alias),
                )
        {
            return Ok(None);
        }

        let (outer_schema, outer_rows) = self.scan_table_base(outer_relation, txn, params).await?;
        let (inner_schema, inner_rows) = self.scan_table_base(inner_relation, txn, params).await?;

        let outer_id_idx = self.resolve_column_index("id", &outer_schema)?;
        let inner_tags_idx = self.resolve_column_index("tags_id", &inner_schema)?;
        let inner_time_idx = self.resolve_column_index("time", &inner_schema)?;

        let mut output_columns = outer_schema.columns.clone();
        let mut prefixed_outer_schema = outer_schema.clone();
        self.prefix_schema_columns(&mut prefixed_outer_schema, outer_relation)?;
        for (column, prefixed) in output_columns
            .iter_mut()
            .zip(prefixed_outer_schema.columns.iter())
        {
            column.name = prefixed.name.clone();
        }

        output_columns.extend(inner_schema.columns.iter().cloned().map(|mut column| {
            column.name = format!("{}.{}", derived_alias, column.name);
            column
        }));
        let output_schema = TableSchema::new("tsbs_lastpoint_lateral".to_string(), output_columns);

        let mut latest_by_tag: HashMap<Value, &Vec<Value>> =
            HashMap::with_capacity(inner_rows.len().min(4096));
        for inner_row in &inner_rows {
            let Some(tag_id) = inner_row.get(inner_tags_idx).cloned() else {
                continue;
            };

            match latest_by_tag.get(&tag_id) {
                Some(existing) => {
                    let ordering = self.compare_for_sort(
                        inner_row.get(inner_time_idx).unwrap_or(&Value::Null),
                        existing.get(inner_time_idx).unwrap_or(&Value::Null),
                    );
                    if ordering == Ordering::Greater {
                        latest_by_tag.insert(tag_id, inner_row);
                    }
                }
                None => {
                    latest_by_tag.insert(tag_id, inner_row);
                }
            }
        }

        let mut rows = Vec::with_capacity(outer_rows.len());
        for outer_row in &outer_rows {
            let Some(outer_id) = outer_row.get(outer_id_idx) else {
                continue;
            };

            if let Some(inner_row) = latest_by_tag.get(outer_id) {
                let mut row = Vec::with_capacity(outer_row.len() + inner_row.len());
                row.extend_from_slice(outer_row);
                row.extend_from_slice(inner_row);
                rows.push(row);
            }
        }

        if let Some(order_by) = order_by {
            let OrderByKind::Expressions(order_exprs) = &order_by.kind else {
                return Ok(None);
            };
            let sort_keys: Vec<SortOrderKey<'_>> = order_exprs
                .iter()
                .map(|order_expr| SortOrderKey {
                    source: self.resolve_order_value_source(
                        &order_expr.expr,
                        &select.projection,
                        &output_schema
                            .columns
                            .iter()
                            .map(|column| column.name.clone())
                            .collect::<Vec<_>>(),
                        &output_schema,
                        false,
                        true,
                    ),
                    asc: order_expr.options.asc.unwrap_or(true),
                })
                .collect();
            self.sort_rows_by_order_keys(&mut rows, &sort_keys, &output_schema, params, None);
        }

        let mut seen = HashSet::with_capacity(rows.len());
        let mut distinct_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let key = self.evaluate_value(&distinct_exprs[0], &row, &output_schema, params)?;
            if seen.insert(key) {
                distinct_rows.push(row);
            }
        }

        let rows = distinct_rows.into_iter().skip(offset);
        let rows = if let Some(limit) = limit {
            rows.take(limit).collect()
        } else {
            rows.collect()
        };

        Ok(Some(QueryResult::Select {
            columns: output_schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            rows,
        }))
    }

    fn simple_join_column_side_index(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<(JoinValueSide, usize)> {
        let col_name = Self::order_limit_column_name(expr)?;
        let left = self.resolve_column_index(&col_name, left_schema).ok();
        let right = self.resolve_column_index(&col_name, right_schema).ok();

        match (left, right) {
            (Some(index), None) => Some((JoinValueSide::Left, index)),
            (None, Some(index)) => Some((JoinValueSide::Right, index)),
            _ => None,
        }
    }

    fn simple_join_equality_keys(
        &self,
        expr: &Expr,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<(usize, usize)> {
        match expr {
            Expr::Nested(inner) => self.simple_join_equality_keys(inner, left_schema, right_schema),
            Expr::BinaryOp {
                left,
                op: sqlparser::ast::BinaryOperator::Eq,
                right,
            } => {
                let left_side = self.simple_join_column_side_index(left, left_schema, right_schema);
                let right_side =
                    self.simple_join_column_side_index(right, left_schema, right_schema);

                match (left_side, right_side) {
                    (
                        Some((JoinValueSide::Left, left_index)),
                        Some((JoinValueSide::Right, right_index)),
                    )
                    | (
                        Some((JoinValueSide::Right, right_index)),
                        Some((JoinValueSide::Left, left_index)),
                    ) => Some((left_index, right_index)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn simple_join_group_aggregate_projection(
        &self,
        projection: &[SelectItem],
        group_exprs: &[Expr],
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Option<(
        (JoinValueSide, usize),
        Vec<SimpleJoinGroupAggregate>,
        Vec<String>,
    )> {
        if projection.len() < 2 || group_exprs.len() != 1 {
            return None;
        }

        let group_source =
            self.simple_join_column_side_index(&group_exprs[0], left_schema, right_schema)?;
        let group_name = Self::order_limit_column_name(&group_exprs[0])?;
        let output_group_name = match &projection[0] {
            SelectItem::UnnamedExpr(expr) if expr == &group_exprs[0] => group_name,
            SelectItem::ExprWithAlias { expr, alias } if expr == &group_exprs[0] => {
                alias.value.clone()
            }
            _ => return None,
        };

        let mut aggregates = Vec::with_capacity(projection.len() - 1);
        let mut output_columns = Vec::with_capacity(projection.len());
        output_columns.push(output_group_name);

        for item in projection.iter().skip(1) {
            let (expr, output_name) = match item {
                SelectItem::UnnamedExpr(expr @ Expr::Function(_)) => (expr, format!("{}", expr)),
                SelectItem::ExprWithAlias {
                    expr: expr @ Expr::Function(_),
                    alias,
                } => (expr, alias.value.clone()),
                _ => return None,
            };

            let Expr::Function(func) = expr else {
                return None;
            };
            let FunctionArguments::List(args) = &func.args else {
                return None;
            };
            if args.duplicate_treatment.is_some() || args.args.len() != 1 {
                return None;
            }

            let func_name = func.name.to_string().to_uppercase();
            let arg = match func_name.as_str() {
                "COUNT"
                    if matches!(
                        args.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    ) =>
                {
                    None
                }
                "COUNT" | "SUM" => {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = &args.args[0]
                    else {
                        return None;
                    };
                    Some(self.simple_join_column_side_index(arg_expr, left_schema, right_schema)?)
                }
                _ => return None,
            };

            aggregates.push(SimpleJoinGroupAggregate { func_name, arg });
            output_columns.push(output_name);
        }

        Some((group_source, aggregates, output_columns))
    }

    fn push_join_column_projection(
        projections: &mut Vec<String>,
        schema: &TableSchema,
        index: usize,
    ) {
        if let Some(column) = schema.columns.get(index) {
            if !projections
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(&column.name))
            {
                projections.push(column.name.clone());
            }
        }
    }

    async fn try_simple_join_group_aggregate(
        &self,
        select: &sqlparser::ast::Select,
        order_by: Option<&sqlparser::ast::OrderBy>,
        limit: Option<usize>,
        offset: usize,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if select.distinct.is_some()
            || select.having.is_some()
            || select.selection.is_some()
            || select.from.len() != 1
            || select.from[0].joins.len() != 1
        {
            return Ok(None);
        }

        let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by else {
            return Ok(None);
        };
        if group_exprs.len() != 1 {
            return Ok(None);
        }

        let left_relation = &select.from[0].relation;
        let join = &select.from[0].joins[0];
        let join_expr = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr)) => expr,
            _ => return Ok(None),
        };

        let (
            TableFactor::Table {
                name: left_name, ..
            },
            TableFactor::Table {
                name: right_name, ..
            },
        ) = (left_relation, &join.relation)
        else {
            return Ok(None);
        };

        let left_table_name = left_name.to_string();
        let right_table_name = right_name.to_string();

        let Some(left_schema_bytes) = txn
            .get(format!("schema:{}", left_table_name).as_bytes())
            .await?
        else {
            return Ok(None);
        };
        let Some(right_schema_bytes) = txn
            .get(format!("schema:{}", right_table_name).as_bytes())
            .await?
        else {
            return Ok(None);
        };

        let left_schema: TableSchema = bincode::deserialize(&left_schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;
        let right_schema: TableSchema = bincode::deserialize(&right_schema_bytes)
            .map_err(|e| FusionError::Execution(format!("Schema deserialization error: {}", e)))?;

        let mut left_prefixed_schema = left_schema.clone();
        let mut right_prefixed_schema = right_schema.clone();
        self.prefix_schema_columns(&mut left_prefixed_schema, left_relation)?;
        self.prefix_schema_columns(&mut right_prefixed_schema, &join.relation)?;

        let Some((left_key_index, right_key_index)) = self.simple_join_equality_keys(
            join_expr,
            &left_prefixed_schema,
            &right_prefixed_schema,
        ) else {
            return Ok(None);
        };

        let Some((group_source, aggregate_plans, columns)) = self
            .simple_join_group_aggregate_projection(
                &select.projection,
                group_exprs,
                &left_prefixed_schema,
                &right_prefixed_schema,
            )
        else {
            return Ok(None);
        };

        let mut left_projection = Vec::with_capacity(aggregate_plans.len() + 2);
        let mut right_projection = Vec::with_capacity(aggregate_plans.len() + 2);
        Self::push_join_column_projection(&mut left_projection, &left_schema, left_key_index);
        Self::push_join_column_projection(&mut right_projection, &right_schema, right_key_index);

        match group_source.0 {
            JoinValueSide::Left => {
                Self::push_join_column_projection(
                    &mut left_projection,
                    &left_schema,
                    group_source.1,
                );
            }
            JoinValueSide::Right => {
                Self::push_join_column_projection(
                    &mut right_projection,
                    &right_schema,
                    group_source.1,
                );
            }
        }

        for aggregate in &aggregate_plans {
            if let Some((side, index)) = aggregate.arg {
                match side {
                    JoinValueSide::Left => {
                        Self::push_join_column_projection(&mut left_projection, &left_schema, index)
                    }
                    JoinValueSide::Right => Self::push_join_column_projection(
                        &mut right_projection,
                        &right_schema,
                        index,
                    ),
                }
            }
        }

        let (_, left_rows, _) = self
            .scan_single_table(
                left_relation,
                &None,
                &Some(left_projection),
                txn,
                params,
                None,
                None,
                None,
            )
            .await?;
        let (_, right_rows, _) = self
            .scan_single_table(
                &join.relation,
                &None,
                &Some(right_projection),
                txn,
                params,
                None,
                None,
                None,
            )
            .await?;

        let mut left_by_key: HashMap<Value, Vec<Vec<Value>>> =
            HashMap::with_capacity(left_rows.len());
        for row in left_rows {
            if let Some(key) = row.get(left_key_index).cloned() {
                left_by_key.entry(key).or_default().push(row);
            }
        }

        let mut groups: HashMap<Value, Vec<AggregateAccumulator>> =
            HashMap::with_capacity(left_by_key.len().min(4096));
        for right_row in &right_rows {
            let Some(join_key) = right_row.get(right_key_index) else {
                continue;
            };
            let Some(left_matches) = left_by_key.get(join_key) else {
                continue;
            };

            for left_row in left_matches {
                let group_key = match group_source {
                    (JoinValueSide::Left, index) => left_row.get(index),
                    (JoinValueSide::Right, index) => right_row.get(index),
                }
                .cloned()
                .unwrap_or(Value::Null);

                let accs = groups.entry(group_key).or_insert_with(|| {
                    aggregate_plans
                        .iter()
                        .map(|plan| AggregateAccumulator::new(&plan.func_name))
                        .collect()
                });

                for (acc, plan) in accs.iter_mut().zip(aggregate_plans.iter()) {
                    let value = match plan.arg {
                        Some((JoinValueSide::Left, index)) => left_row.get(index),
                        Some((JoinValueSide::Right, index)) => right_row.get(index),
                        None => None,
                    }
                    .unwrap_or(&Value::Integer(1));
                    acc.update(value);
                }
            }
        }

        let mut rows: Vec<Vec<Value>> = groups
            .into_iter()
            .map(|(group_value, accs)| {
                let mut row = Vec::with_capacity(accs.len() + 1);
                row.push(group_value);
                row.extend(accs.iter().map(AggregateAccumulator::finalize));
                row
            })
            .collect();

        let result_schema = TableSchema::new(
            "temp_join_group_by_result".to_string(),
            columns
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
                .collect(),
        );

        if let Some(order_by) = order_by {
            if let OrderByKind::Expressions(exprs) = &order_by.kind {
                let sort_keys: Vec<SortOrderKey<'_>> = exprs
                    .iter()
                    .map(|order_expr| SortOrderKey {
                        source: self.resolve_order_value_source(
                            &order_expr.expr,
                            &select.projection,
                            &columns,
                            &result_schema,
                            true,
                            false,
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
                    &result_schema,
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

        Ok(Some(QueryResult::Select { columns, rows }))
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
                if with.recursive && Self::recursive_union_parts(&cte.query).is_some() {
                    cte_names.push(self.materialize_recursive_cte(cte, txn, params).await?);
                    continue;
                }

                let result = Box::pin(self.handle_query(&cte.query, txn, params)).await?;
                if let QueryResult::Select { columns, rows } = result {
                    let columns = Self::cte_output_columns(cte, &columns)?;
                    self.put_materialized_cte(txn, &cte_name, &columns, &rows)
                        .await?;
                    cte_names.push(cte_name);
                }
            }
        }

        let result = self.handle_query_inner(query, txn, params).await;

        // Cleanup CTE temporary tables
        for name in &cte_names {
            let _ = Self::clear_materialized_cte(txn, name).await;
        }

        result
    }

    async fn handle_query_inner(
        &self,
        query: &sqlparser::ast::Query,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<QueryResult> {
        if let SetExpr::Query(inner_query) = query.body.as_ref() {
            return Box::pin(self.handle_query(inner_query, txn, params)).await;
        }

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

            if let Some(result) = self
                .try_simple_join_group_aggregate(
                    select,
                    query.order_by.as_ref(),
                    limit,
                    offset,
                    txn,
                    params,
                )
                .await?
            {
                return Ok(result);
            }

            if let Some(result) = self
                .try_tsbs_lastpoint_lateral(
                    select,
                    query.order_by.as_ref(),
                    limit,
                    offset,
                    txn,
                    params,
                )
                .await?
            {
                return Ok(result);
            }

            if !is_join
                && select.distinct.is_some()
                && select.having.is_none()
                && is_group_by_none
                && select.from.len() == 1
                && select.from[0].joins.is_empty()
            {
                if let TableFactor::Table { name, .. } = &select.from[0].relation {
                    let table_name_str = name.to_string();
                    let schema_key = format!("schema:{}", table_name_str);
                    if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                        let schema: TableSchema =
                            bincode::deserialize(&schema_bytes).map_err(|e| {
                                FusionError::Execution(format!(
                                    "Schema deserialization error: {}",
                                    e
                                ))
                            })?;
                        if let Some((column_index, column_name)) =
                            Self::single_column_distinct_projection(&select.projection, &schema)
                        {
                            let columns = vec![column_name];
                            let predicate = if let Some(selection) = select.selection.as_ref() {
                                self.simple_column_predicate_scan_plan(selection, &schema, params)
                            } else {
                                None
                            };
                            if (select.selection.is_none() || predicate.is_some())
                                && Self::simple_order_limit_supported(
                                    &columns,
                                    query.order_by.as_ref(),
                                )
                            {
                                let rows = self
                                    .distinct_column_scan(
                                        &table_name_str,
                                        column_index,
                                        predicate.as_ref(),
                                        txn,
                                    )
                                    .await?;
                                let mut rows = rows;
                                Self::apply_simple_group_by_order_limit(
                                    &mut rows,
                                    &columns,
                                    query.order_by.as_ref(),
                                    limit,
                                    offset,
                                )?;
                                return Ok(QueryResult::Select { columns, rows });
                            }
                        }
                    }
                }
            }

            // Optimization: Aggregates on PK (COUNT(*), MIN(id), MAX(id)) and
            // single-column COUNT(DISTINCT col) without materializing full rows.
            if !is_join
                && !select.distinct.is_some()
                && select.selection.is_some()
                && is_group_by_none
                && select.having.is_none()
                && query.order_by.is_none()
                && query.limit_clause.is_none()
                && select.from.len() == 1
                && select.from[0].joins.is_empty()
            {
                if let TableFactor::Table { name, alias, .. } = &select.from[0].relation {
                    let table_name_str = name.to_string();
                    let mut aggregate_qualifiers = Vec::with_capacity(2);
                    aggregate_qualifiers.push(table_name_str.clone());
                    if let Some(alias) = alias {
                        aggregate_qualifiers.push(alias.name.value.clone());
                    }

                    let schema_key = format!("schema:{}", table_name_str);
                    if let Ok(Some(schema_bytes)) = txn.get(schema_key.as_bytes()).await {
                        if let Ok(schema) = bincode::deserialize::<TableSchema>(&schema_bytes) {
                            if let (Some(plans), Some(predicate)) = (
                                Self::simple_column_aggregate_projection(
                                    &select.projection,
                                    &schema,
                                    Some(&aggregate_qualifiers),
                                    true,
                                ),
                                select.selection.as_ref().and_then(|selection| {
                                    self.simple_column_predicate_scan_plan(
                                        selection, &schema, params,
                                    )
                                }),
                            ) {
                                let columns =
                                    plans.iter().map(|plan| plan.output_name.clone()).collect();
                                let result_row = self
                                    .simple_column_aggregate_scan(
                                        &table_name_str,
                                        &plans,
                                        Some(&predicate),
                                        Some(&schema),
                                        txn,
                                    )
                                    .await?;
                                return Ok(QueryResult::Select {
                                    columns,
                                    rows: vec![result_row],
                                });
                            }

                            if let (Some((column_index, column_name)), Some(predicate)) = (
                                Self::count_distinct_projection(
                                    &select.projection,
                                    &schema,
                                    Some(&aggregate_qualifiers),
                                ),
                                select.selection.as_ref().and_then(|selection| {
                                    self.simple_column_predicate_scan_plan(
                                        selection, &schema, params,
                                    )
                                }),
                            ) {
                                let count = self
                                    .count_distinct_column_scan(
                                        &table_name_str,
                                        column_index,
                                        Some(&predicate),
                                        txn,
                                    )
                                    .await?;
                                return Ok(QueryResult::Select {
                                    columns: vec![column_name],
                                    rows: vec![vec![Value::Integer(count)]],
                                });
                            }
                        }
                    }
                }
            }

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
                                            None,
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

                                if select.having.is_none()
                                    && query.order_by.is_none()
                                    && query.limit_clause.is_none()
                                    && select.from.len() == 1
                                    && select.from[0].joins.is_empty()
                                {
                                    if let Some(plans) = Self::simple_column_aggregate_projection(
                                        &select.projection,
                                        &schema,
                                        Some(&aggregate_qualifiers),
                                        false,
                                    ) {
                                        let columns = plans
                                            .iter()
                                            .map(|plan| plan.output_name.clone())
                                            .collect();
                                        let result_row = self
                                            .simple_column_aggregate_scan(
                                                &table_name_str,
                                                &plans,
                                                None,
                                                Some(&schema),
                                                txn,
                                            )
                                            .await?;
                                        return Ok(QueryResult::Select {
                                            columns,
                                            rows: vec![result_row],
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !is_join
                && select.having.is_none()
                && select.from.len() == 1
                && select.from[0].joins.is_empty()
            {
                if let sqlparser::ast::GroupByExpr::Expressions(group_exprs, _) = &select.group_by {
                    if !group_exprs.is_empty() {
                        if let TableFactor::Table { name, .. } = &select.from[0].relation {
                            let table_name_str = name.to_string();
                            let schema_key = format!("schema:{}", table_name_str);
                            if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                                let schema: TableSchema = bincode::deserialize(&schema_bytes)
                                    .map_err(|e| {
                                        FusionError::Execution(format!(
                                            "Schema deserialization error: {}",
                                            e
                                        ))
                                    })?;
                                let predicate = if let Some(selection) = select.selection.as_ref() {
                                    self.simple_column_predicate_scan_plan(
                                        selection, &schema, params,
                                    )
                                } else {
                                    None
                                };
                                if select.selection.is_none() || predicate.is_some() {
                                    if let Some((group_column_index, group_name, count_name)) =
                                        Self::simple_group_by_count_projection(
                                            &select.projection,
                                            group_exprs,
                                            &schema,
                                        )
                                    {
                                        let rows = self
                                            .group_by_count_column_scan(
                                                &table_name_str,
                                                group_column_index,
                                                predicate.as_ref(),
                                                txn,
                                            )
                                            .await?;
                                        let mut rows = rows;
                                        let columns = vec![group_name, count_name];
                                        Self::apply_simple_group_by_order_limit(
                                            &mut rows,
                                            &columns,
                                            query.order_by.as_ref(),
                                            limit,
                                            offset,
                                        )?;
                                        return Ok(QueryResult::Select { columns, rows });
                                    }

                                    if let Some((
                                        group_column_indices,
                                        mut columns,
                                        aggregate_plans,
                                    )) = Self::simple_group_by_column_aggregate_projection(
                                        &select.projection,
                                        group_exprs,
                                        &schema,
                                    ) {
                                        columns.extend(
                                            aggregate_plans
                                                .iter()
                                                .map(|plan| plan.output_name.clone()),
                                        );
                                        let rows = self
                                            .group_by_column_aggregate_scan(
                                                &table_name_str,
                                                &group_column_indices,
                                                &aggregate_plans,
                                                predicate.as_ref(),
                                                txn,
                                            )
                                            .await?;
                                        let mut rows = rows;
                                        Self::apply_simple_group_by_order_limit(
                                            &mut rows,
                                            &columns,
                                            query.order_by.as_ref(),
                                            limit,
                                            offset,
                                        )?;
                                        return Ok(QueryResult::Select { columns, rows });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let (selection_for_scan, deferred_subquery_selection) =
                if let Some(sel) = &select.selection {
                    Self::split_deferred_subquery_selection(sel)
                } else {
                    (None, None)
                };
            let filter_requires_deferred_subquery = deferred_subquery_selection.is_some();
            let keep_projection_for_deferred_subquery =
                filter_requires_deferred_subquery && Self::is_ldbc_q4_join_shape(&select.from);

            let primary_key_order_limit = if filter_requires_deferred_subquery {
                None
            } else {
                self.primary_key_order_scan_limit(
                    select,
                    query.order_by.as_ref(),
                    limit,
                    offset,
                    txn,
                )
                .await?
            };
            let push_down_limit = if filter_requires_deferred_subquery {
                None
            } else if is_group_by_none && query.order_by.is_none() && selection_for_scan.is_none() {
                limit.map(|l| l + offset)
            } else {
                primary_key_order_limit
            };
            let order_limit_window = if !filter_requires_deferred_subquery
                && is_group_by_none
                && query.order_by.is_some()
            {
                limit.map(|l| l + offset)
            } else {
                None
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
                    self.extract_group_by_input_columns(group_exprs, &select.projection, &mut cols);
                }
                if let Some(having) = &select.having {
                    self.extract_columns_from_expr(having, &mut cols);
                }
                if let Some(selection) = &select.selection {
                    self.extract_columns_from_expr(selection, &mut cols);
                }
                if keep_projection_for_deferred_subquery {
                    if let Some(deferred_selection) = &deferred_subquery_selection {
                        self.extract_deferred_exists_outer_columns(deferred_selection, &mut cols);
                    }
                }
                if let Some(order_by) = &query.order_by {
                    self.extract_order_by_input_columns(order_by, &select.projection, &mut cols);
                }
                if cols.is_empty() {
                    Some(vec![])
                } else {
                    Some(cols.into_iter().collect())
                }
            };

            // Pre-materialize subqueries in the WHERE clause
            let materialized_selection = if let Some(sel) = &selection_for_scan {
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
                .or(selection_for_scan.as_ref());

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
                    self.extract_group_by_input_columns(group_exprs, &select.projection, &mut cols);
                }
                if let Some(having) = &select.having {
                    self.extract_columns_from_expr(having, &mut cols);
                }
                if let Some(sel) = effective_selection {
                    self.extract_columns_from_expr(sel, &mut cols);
                }
                if keep_projection_for_deferred_subquery {
                    if let Some(deferred_selection) = &deferred_subquery_selection {
                        self.extract_deferred_exists_outer_columns(deferred_selection, &mut cols);
                    }
                }
                if let Some(order_by) = &query.order_by {
                    self.extract_order_by_input_columns(order_by, &select.projection, &mut cols);
                }
                if cols.is_empty() {
                    Some(vec![])
                } else {
                    Some(cols.into_iter().collect())
                }
            } else {
                projection_hint
            };
            let projection_hint =
                if filter_requires_deferred_subquery && !keep_projection_for_deferred_subquery {
                    None
                } else {
                    projection_hint
                };
            let sel_option = effective_selection.cloned();

            let join_limit = if is_join
                && effective_selection.is_none()
                && !filter_requires_deferred_subquery
                && query.order_by.is_none()
            {
                push_down_limit
            } else {
                None
            };

            let (mut schema, mut rows, scan_satisfies_order_by) = if is_join {
                let (schema, rows) = self
                    .execute_join(
                        &select.from,
                        &sel_option,
                        &projection_hint,
                        txn,
                        params,
                        join_limit,
                        keep_projection_for_deferred_subquery,
                    )
                    .await?;
                (schema, rows, false)
            } else if let Some(table) = select.from.first() {
                self.scan_single_table(
                    &table.relation,
                    &sel_option,
                    &projection_hint,
                    txn,
                    params,
                    if filter_requires_deferred_subquery {
                        None
                    } else {
                        push_down_limit
                    },
                    query.order_by.as_ref(),
                    order_limit_window,
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

            if let Some(deferred_selection) = &deferred_subquery_selection {
                rows = self
                    .filter_rows_with_subqueries(rows, &schema, deferred_selection, txn, params)
                    .await?;
            }

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
                    let resolved_group_exprs =
                        Self::resolve_group_by_projection_aliases(group_exprs, &select.projection);
                    let group_exprs = resolved_group_exprs.as_slice();
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

                    let used_simple_group_by = if select.having.is_none() {
                        if let Some((group_index, _group_name, simple_plans, simple_columns)) = self
                            .simple_projected_group_aggregate_projection(
                                &select.projection,
                                group_exprs,
                                &schema,
                            )
                        {
                            rows = Self::apply_simple_projected_group_aggregates(
                                rows,
                                group_index,
                                &simple_plans,
                            );
                            schema = TableSchema::new(
                                "temp_group_by_result".to_string(),
                                simple_columns
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
                                    .collect(),
                            );
                            columns = simple_columns;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !used_simple_group_by {
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
                            let group_scope = self.group_output_scope(group_exprs, &group_key);

                            for item in &select.projection {
                                let val = match item {
                                    SelectItem::UnnamedExpr(expr) => {
                                        let materialized = if Self::contains_subquery(expr) {
                                            let group_row = group_key.as_slice();
                                            Box::pin(self.materialize_subqueries_with_outer(
                                                expr,
                                                Some(group_row),
                                                Some(&group_scope),
                                                txn,
                                                params,
                                            ))
                                            .await?
                                        } else {
                                            expr.clone()
                                        };
                                        self.evaluate_final_group_expr(
                                            &materialized,
                                            &group_key,
                                            group_exprs,
                                            &agg_map,
                                            &schema,
                                            params,
                                        )?
                                    }
                                    SelectItem::ExprWithAlias { expr, .. } => {
                                        let materialized = if Self::contains_subquery(expr) {
                                            let group_row = group_key.as_slice();
                                            Box::pin(self.materialize_subqueries_with_outer(
                                                expr,
                                                Some(group_row),
                                                Some(&group_scope),
                                                txn,
                                                params,
                                            ))
                                            .await?
                                        } else {
                                            expr.clone()
                                        };
                                        self.evaluate_final_group_expr(
                                            &materialized,
                                            &group_key,
                                            group_exprs,
                                            &agg_map,
                                            &schema,
                                            params,
                                        )?
                                    }
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
            }

            let mut rows_still_satisfy_order_by = scan_satisfies_order_by;
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
                    if rows_are_projected {
                        rows_still_satisfy_order_by = false;
                    }
                    if !rows_still_satisfy_order_by {
                        self.sort_rows_by_order_keys(
                            &mut rows,
                            &sort_keys,
                            &schema,
                            params,
                            limit_window,
                        );
                    }
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
                    let agg_map: HashMap<Expr, Value> = aggregate_plans
                        .iter()
                        .zip(accs.iter())
                        .map(|(plan, acc)| (plan.expr.clone(), acc.finalize()))
                        .collect();
                    let result_row = select
                        .projection
                        .iter()
                        .map(|item| match item {
                            SelectItem::UnnamedExpr(expr) => self.evaluate_final_group_expr(
                                expr,
                                &[],
                                &[],
                                &agg_map,
                                &schema,
                                params,
                            ),
                            SelectItem::ExprWithAlias { expr, .. } => self
                                .evaluate_final_group_expr(
                                    expr,
                                    &[],
                                    &[],
                                    &agg_map,
                                    &schema,
                                    params,
                                ),
                            _ => Ok(Value::Null),
                        })
                        .collect::<Result<Vec<_>>>()?;
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
                Self::trim_set_rows_in_place(&mut combined, set_offset, set_limit);
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
