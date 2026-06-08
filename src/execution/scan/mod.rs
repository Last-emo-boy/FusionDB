mod index_plan;
mod join;
mod predicate;

pub(crate) use index_plan::IndexScanPlan;
use index_plan::SMALL_INDEX_FETCH_THRESHOLD;

use crate::catalog::Column;
use crate::catalog::{IndexType, TableSchema};
use crate::common::{FusionError, Result, Value};
use crate::monitor;
use crate::storage::Transaction;
use futures::stream::StreamExt;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, TableFactor,
    TableFunctionArgs,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::analyze::{ColumnStats, TableStats};
use super::Executor;

const STATS_INDEX_PROBE_LIMIT_MAX: usize = 65_536;

impl Executor {
    pub(crate) fn generate_subscripts_schema(relation: &TableFactor) -> Option<TableSchema> {
        let TableFactor::Table {
            name, alias, args, ..
        } = relation
        else {
            return None;
        };
        if !name.to_string().eq_ignore_ascii_case("generate_subscripts") || args.is_none() {
            return None;
        }

        let column_name = alias
            .as_ref()
            .and_then(|alias| alias.columns.first())
            .map(|column| column.name.value.clone())
            .or_else(|| alias.as_ref().map(|alias| alias.name.value.clone()))
            .unwrap_or_else(|| "generate_subscripts".to_string());
        Some(TableSchema::new(
            alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| "generate_subscripts".to_string()),
            vec![Column {
                name: column_name,
                data_type: "INTEGER".to_string(),
                is_primary: false,
                is_indexed: false,
                index_type: IndexType::None,
                default_value: None,
                is_nullable: false,
                is_unique: false,
                check_expr: None,
            }],
        ))
    }

    pub(crate) fn generate_subscripts_rows_for_value(value: Value, dim: i64) -> Vec<Vec<Value>> {
        let Value::Array(values) = value else {
            return Vec::new();
        };
        if dim <= 1 {
            return (1..=values.len())
                .map(|index| vec![Value::Integer(index as i64)])
                .collect();
        }

        let max_len = values
            .iter()
            .filter_map(|value| match value {
                Value::Array(values) => Some(values.len()),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        (1..=max_len)
            .map(|index| vec![Value::Integer(index as i64)])
            .collect()
    }

    pub(crate) fn table_function_args_exprs(args: &TableFunctionArgs) -> Option<Vec<&Expr>> {
        args.args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn evaluate_generate_subscripts(
        &self,
        relation: &TableFactor,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<Result<(TableSchema, Vec<Vec<Value>>)>> {
        let TableFactor::Table { args, .. } = relation else {
            return None;
        };
        let output_schema = Self::generate_subscripts_schema(relation)?;
        let args = args.as_ref()?;
        let exprs = match Self::table_function_args_exprs(args) {
            Some(exprs) if exprs.len() == 2 => exprs,
            _ => {
                return Some(Err(FusionError::Execution(
                    "generate_subscripts expects two arguments".to_string(),
                )))
            }
        };
        let value = match self.evaluate_value(exprs[0], row, schema, params) {
            Ok(value) => value,
            Err(err) => return Some(Err(err)),
        };
        let dim = match self.evaluate_value(exprs[1], row, schema, params) {
            Ok(Value::Integer(value)) => value,
            Ok(_) => {
                return Some(Err(FusionError::Execution(
                    "generate_subscripts dimension must be integer".to_string(),
                )))
            }
            Err(err) => return Some(Err(err)),
        };

        Some(Ok((
            output_schema,
            Self::generate_subscripts_rows_for_value(value, dim),
        )))
    }

    fn materialized_query_schema(table_name: String, columns: Vec<String>) -> TableSchema {
        TableSchema::new(
            table_name,
            columns
                .into_iter()
                .map(|name| Column {
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
                .collect(),
        )
    }

    fn projection_indices_for_scan(
        projection: &Option<Vec<String>>,
        schema: &TableSchema,
    ) -> Option<Vec<usize>> {
        let cols = projection.as_ref()?;
        if cols.is_empty() {
            return Some(Vec::new());
        }

        if cols.len().saturating_mul(schema.columns.len()) <= 32 {
            let mut indices = Vec::with_capacity(cols.len());
            for name in cols {
                if let Some((idx, _)) = schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.name.eq_ignore_ascii_case(name))
                {
                    indices.push(idx);
                }
            }
            return if indices.is_empty() {
                None
            } else {
                Some(indices)
            };
        }

        let mut column_indices = HashMap::with_capacity(schema.columns.len());
        for (idx, column) in schema.columns.iter().enumerate() {
            column_indices
                .entry(column.name.to_ascii_lowercase())
                .or_insert(idx);
        }

        let mut indices = Vec::with_capacity(cols.len());
        for name in cols {
            if let Some(idx) = column_indices.get(&name.to_ascii_lowercase()) {
                indices.push(*idx);
            }
        }

        if indices.is_empty() {
            None
        } else {
            Some(indices)
        }
    }

    async fn scan_derived_table(
        &self,
        subquery: &sqlparser::ast::Query,
        alias: &Option<sqlparser::ast::TableAlias>,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        let result = Box::pin(self.handle_query(subquery, txn, params)).await?;
        if let super::QueryResult::Select { columns, rows } = result {
            let table_name = alias
                .as_ref()
                .map(|alias| alias.name.value.clone())
                .unwrap_or_else(|| "derived".to_string());
            return Ok((Self::materialized_query_schema(table_name, columns), rows));
        }

        Err(FusionError::Execution(
            "Derived table subquery must return rows".to_string(),
        ))
    }

    pub(crate) async fn scan_table_base(
        &self,
        relation: &TableFactor,
        txn: &mut dyn Transaction,
        params: &[Value],
    ) -> Result<(TableSchema, Vec<Vec<Value>>)> {
        match relation {
            TableFactor::Table { name, .. } => {
                if let Some(result) = self.evaluate_generate_subscripts(
                    relation,
                    &[],
                    &TableSchema::new("generate_subscripts_input".to_string(), vec![]),
                    params,
                ) {
                    return result;
                }

                let table_name = name.to_string();
                let schema_key = format!("schema:{}", table_name);

                // Try table first
                if let Some(schema_bytes) = txn.get(schema_key.as_bytes()).await? {
                    let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                        FusionError::Execution(format!("Schema deserialization error: {}", e))
                    })?;

                    let prefix = format!("data:{}:", table_name);
                    let kv_pairs = txn.scan_prefix(prefix.as_bytes(), None).await?;
                    let mut rows = Vec::with_capacity(kv_pairs.len());
                    for (k, v) in kv_pairs {
                        let row: Vec<Value> = if let Ok(key_str) = std::str::from_utf8(&k) {
                            if let Some(row) = self.row_cache.get(key_str) {
                                monitor::inc_row_cache_hit();
                                row
                            } else {
                                let row = crate::common::encoding::RowDecoder::decode(&v).map_err(
                                    |e| {
                                        FusionError::Execution(format!(
                                            "Data deserialization error: {}",
                                            e
                                        ))
                                    },
                                )?;
                                self.row_cache.insert(key_str.to_string(), row.clone());
                                row
                            }
                        } else {
                            crate::common::encoding::RowDecoder::decode(&v).map_err(|e| {
                                FusionError::Execution(format!("Data deserialization error: {}", e))
                            })?
                        };
                        rows.push(row);
                    }
                    return Ok((schema, rows));
                }

                // No table schema found — check for a view definition
                let view_key = format!("view:{}", table_name);
                if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                    let view_sql = String::from_utf8(view_bytes)
                        .map_err(|e| FusionError::Execution(format!("View decode error: {}", e)))?;
                    let stmts =
                        crate::parser::parse_sql(&format!("SELECT * FROM ({}) AS _v", view_sql))?;
                    if let Some(sqlparser::ast::Statement::Query(query)) = stmts.into_iter().next()
                    {
                        let result = Box::pin(self.handle_query(&query, txn, params)).await?;
                        if let super::QueryResult::Select { columns, rows } = result {
                            let schema = Self::materialized_query_schema(table_name, columns);
                            return Ok((schema, rows));
                        }
                    }
                }

                Err(FusionError::Execution(format!(
                    "Table {} not found",
                    table_name
                )))
            }
            TableFactor::Derived {
                subquery,
                alias,
                lateral,
            } => {
                if *lateral {
                    return Err(FusionError::Execution(
                        "Unsupported lateral derived table".to_string(),
                    ));
                }
                self.scan_derived_table(subquery, alias, txn, params).await
            }
            _ => Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            )),
        }
    }

    async fn stats_guided_index_probe_limit(
        &self,
        table_name: &str,
        selection: &Expr,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> Result<Option<usize>> {
        let baseline_cap = Self::index_candidate_cap(limit, order_by);
        let Some(stats) = self.load_table_stats(table_name, txn).await? else {
            return Ok(None);
        };
        let Some(estimated_rows) =
            self.scan_estimate_indexable_predicate_rows(selection, schema, &stats)
        else {
            return Ok(None);
        };

        if estimated_rows <= baseline_cap {
            return Ok(None);
        }

        let table_rows = stats.row_count.max(1);
        let index_cost = (table_rows as f64).log2().max(1.0) + estimated_rows as f64;
        if index_cost < table_rows as f64 {
            Ok(Some(
                estimated_rows
                    .min(STATS_INDEX_PROBE_LIMIT_MAX)
                    .max(baseline_cap),
            ))
        } else {
            Ok(None)
        }
    }

    fn scan_estimate_indexable_predicate_rows(
        &self,
        selection: &Expr,
        schema: &TableSchema,
        stats: &TableStats,
    ) -> Option<usize> {
        Self::collect_conjunctive_predicates(selection)
            .iter()
            .filter_map(|predicate| self.scan_estimate_single_index_rows(predicate, schema, stats))
            .min()
    }

    fn scan_estimate_single_index_rows(
        &self,
        expr: &Expr,
        schema: &TableSchema,
        stats: &TableStats,
    ) -> Option<usize> {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                let column_idx = self.scan_column_constant_index(left, right, schema)?;
                let column = schema.columns.get(column_idx)?;
                if !Self::scan_column_can_use_btree_index(column) {
                    return None;
                }
                if column.is_primary || column.is_unique {
                    return Some(usize::from(stats.row_count > 0));
                }
                let column_stats =
                    Self::scan_column_stats_for_schema_index(stats, schema, column_idx)?;
                if column_stats.distinct_count == 0 {
                    return None;
                }
                Some(Self::scan_selectivity_to_rows(
                    stats.row_count,
                    1.0 / column_stats.distinct_count as f64,
                ))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } if !*negated => {
                let column_idx = self.resolve_schema_column_index(expr, schema)?;
                let column = schema.columns.get(column_idx)?;
                if !Self::scan_column_can_use_btree_index(column) {
                    return None;
                }
                if column.is_primary || column.is_unique {
                    return Some(list.len().min(stats.row_count));
                }
                let column_stats =
                    Self::scan_column_stats_for_schema_index(stats, schema, column_idx)?;
                if column_stats.distinct_count == 0 {
                    return None;
                }
                Some(Self::scan_selectivity_to_rows(
                    stats.row_count,
                    (list.len() as f64 / column_stats.distinct_count as f64).clamp(0.0, 1.0),
                ))
            }
            Expr::Nested(inner) => self.scan_estimate_single_index_rows(inner, schema, stats),
            _ => None,
        }
    }

    fn scan_column_constant_index(
        &self,
        left: &Expr,
        right: &Expr,
        schema: &TableSchema,
    ) -> Option<usize> {
        let left_idx = self.resolve_schema_column_index(left, schema);
        let right_idx = self.resolve_schema_column_index(right, schema);

        if left_idx.is_some() && right_idx.is_none() && !self.expr_has_column_reference(right) {
            left_idx
        } else if right_idx.is_some() && left_idx.is_none() && !self.expr_has_column_reference(left)
        {
            right_idx
        } else {
            None
        }
    }

    fn scan_column_can_use_btree_index(column: &Column) -> bool {
        column.is_primary || (column.is_indexed && column.index_type == IndexType::BTree)
    }

    fn scan_column_stats_for_schema_index<'a>(
        stats: &'a TableStats,
        schema: &TableSchema,
        index: usize,
    ) -> Option<&'a ColumnStats> {
        let column_name = schema.columns.get(index)?.name.as_str();
        let unqualified = column_name.rsplit('.').next().unwrap_or(column_name);
        stats.columns.iter().find(|column| {
            column.name.eq_ignore_ascii_case(column_name)
                || column.name.eq_ignore_ascii_case(unqualified)
        })
    }

    fn scan_selectivity_to_rows(row_count: usize, selectivity: f64) -> usize {
        if row_count == 0 {
            return 0;
        }
        let rows = (row_count as f64 * selectivity.clamp(0.0, 1.0)).ceil() as usize;
        rows.clamp(1, row_count)
    }

    pub(crate) async fn scan_single_table(
        &self,
        table: &TableFactor,
        selection: &Option<Expr>,
        projection: &Option<Vec<String>>,
        txn: &mut dyn Transaction,
        params: &[Value],
        limit: Option<usize>,
        order_by: Option<&sqlparser::ast::OrderBy>,
        ordered_limit: Option<usize>,
    ) -> Result<(TableSchema, Vec<Vec<Value>>, bool)> {
        match table {
            TableFactor::Derived {
                subquery,
                alias,
                lateral,
            } => {
                if *lateral {
                    return Err(FusionError::Execution(
                        "Unsupported lateral derived table".to_string(),
                    ));
                }
                let (schema, rows) = self
                    .scan_derived_table(subquery, alias, txn, params)
                    .await?;
                let rows = if let Some(selection) = selection {
                    self.filter_rows_with_expr(rows, &schema, selection, params)?
                } else {
                    rows
                };
                Ok((schema, rows, false))
            }
            TableFactor::Table { name, .. } => {
                if let Some(result) = self.evaluate_generate_subscripts(
                    table,
                    &[],
                    &TableSchema::new("generate_subscripts_input".to_string(), vec![]),
                    params,
                ) {
                    let (schema, rows) = result?;
                    let rows = if let Some(selection) = selection {
                        self.filter_rows_with_expr(rows, &schema, selection, params)?
                    } else {
                        rows
                    };
                    return Ok((schema, rows, false));
                }

                let table_name = name.to_string();
                let schema_key = format!("schema:{}", table_name);

                // Check for view — if no table schema exists, try view expansion
                let schema_bytes_opt = txn.get(schema_key.as_bytes()).await?;
                if schema_bytes_opt.is_none() {
                    let view_key = format!("view:{}", table_name);
                    if let Some(view_bytes) = txn.get(view_key.as_bytes()).await? {
                        let view_sql = String::from_utf8(view_bytes).map_err(|e| {
                            FusionError::Execution(format!("View decode error: {}", e))
                        })?;
                        let stmts = crate::parser::parse_sql(&view_sql)?;
                        if let Some(sqlparser::ast::Statement::Query(query)) =
                            stmts.into_iter().next()
                        {
                            let result = Box::pin(self.handle_query(&query, txn, params)).await?;
                            if let super::QueryResult::Select { columns, rows } = result {
                                let schema = Self::materialized_query_schema(table_name, columns);
                                return Ok((schema, rows, false));
                            }
                        }
                    }
                    return Err(FusionError::Execution(format!(
                        "Table {} not found",
                        table_name
                    )));
                }

                let schema_bytes = schema_bytes_opt.unwrap();
                let schema: TableSchema = bincode::deserialize(&schema_bytes).map_err(|e| {
                    FusionError::Execution(format!("Schema deserialization error: {}", e))
                })?;

                let projection_indices = Self::projection_indices_for_scan(projection, &schema);
                let zero_column_projection = projection_indices
                    .as_ref()
                    .is_some_and(|indices| indices.is_empty());

                let mut pk_index = None;
                for (i, col) in schema.columns.iter().enumerate() {
                    if col.is_primary {
                        pk_index = Some(i);
                        break;
                    }
                }
                // Determine if we can do Key-Only Scan (Projection Pushdown Optimization)
                let mut key_only_scan = false;

                if let Some(pk_idx) = pk_index {
                    let projection_is_pk_only = if let Some(proj) = projection {
                        !proj.is_empty()
                            && proj.iter().all(|name| {
                                self.resolve_column_index(name, &schema)
                                    .ok()
                                    .map(|idx| idx == pk_idx)
                                    .unwrap_or(false)
                            })
                    } else {
                        false
                    };

                    if projection_is_pk_only {
                        let selection_is_pk_only = if let Some(sel) = selection {
                            let mut cols = HashSet::new();
                            self.extract_columns_from_expr(sel, &mut cols);
                            cols.iter().all(|name| {
                                self.resolve_column_index(name, &schema)
                                    .ok()
                                    .map(|idx| idx == pk_idx)
                                    .unwrap_or(false)
                            })
                        } else {
                            true
                        };

                        if selection_is_pk_only {
                            let order_by_is_pk_only = if let Some(ob) = order_by {
                                let mut cols = HashSet::new();
                                if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
                                    for expr in exprs {
                                        self.extract_columns_from_expr(&expr.expr, &mut cols);
                                    }
                                }
                                cols.iter().all(|name| {
                                    self.resolve_column_index(name, &schema)
                                        .ok()
                                        .map(|idx| idx == pk_idx)
                                        .unwrap_or(false)
                                })
                            } else {
                                true
                            };

                            if order_by_is_pk_only {
                                key_only_scan = true;
                            }
                        }
                    }
                }

                // Determine if we can safely push down the limit to storage scans
                let effective_limit = if let Some(ob) = order_by {
                    let mut is_pk_asc = false;
                    if let sqlparser::ast::OrderByKind::Expressions(exprs) = &ob.kind {
                        if exprs.len() == 1 {
                            let expr = &exprs[0];
                            if expr.options.asc.unwrap_or(true) {
                                if self
                                    .resolve_schema_column_index(&expr.expr, &schema)
                                    .is_some_and(|idx| schema.columns[idx].is_primary)
                                {
                                    is_pk_asc = true;
                                }
                            }
                        }
                    }
                    if is_pk_asc {
                        limit
                    } else {
                        None
                    }
                } else {
                    limit
                };

                let mut rows = Vec::new();
                let mut index_used = false;
                let mut selection_fully_applied = selection.is_none();
                let mut rows_satisfy_order_by = false;

                // Optimization: Vector Search (HNSW)
                if let Some(order_by) = order_by {
                    if let Some(l) = limit {
                        if l > 0 {
                            if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind
                            {
                                if exprs.len() == 1 {
                                    let sort_expr = &exprs[0].expr;
                                    let mut vector_search_args = None;

                                    // Case 1: <-> operator
                                    if let Expr::BinaryOp {
                                        left,
                                        op: BinaryOperator::Custom(op_str),
                                        right,
                                    } = sort_expr
                                    {
                                        if op_str == "<->" {
                                            if let Some(col_name) =
                                                Self::column_name_from_expr(left.as_ref())
                                            {
                                                vector_search_args =
                                                    Some((col_name, right.as_ref().clone()));
                                            }
                                        }
                                    }
                                    // Case 2: VECTOR_DISTANCE function
                                    else if let Expr::Function(func) = sort_expr {
                                        if func.name.to_string().to_uppercase() == "VECTOR_DISTANCE"
                                        {
                                            if let FunctionArguments::List(args) = &func.args {
                                                if args.args.len() == 2 {
                                                    if let FunctionArg::Unnamed(
                                                        FunctionArgExpr::Expr(col_expr),
                                                    ) = &args.args[0]
                                                    {
                                                        if let FunctionArg::Unnamed(
                                                            FunctionArgExpr::Expr(val_expr),
                                                        ) = &args.args[1]
                                                        {
                                                            if let Some(col_name) =
                                                                Self::column_name_from_expr(
                                                                    col_expr,
                                                                )
                                                            {
                                                                vector_search_args = Some((
                                                                    col_name,
                                                                    val_expr.clone(),
                                                                ));
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if let Some((col_name, query_expr)) = vector_search_args {
                                        if let Ok(idx) =
                                            self.resolve_column_index(&col_name, &schema)
                                        {
                                            if schema.columns[idx].index_type == IndexType::HNSW {
                                                let storage_col_name =
                                                    schema.columns[idx].name.clone();
                                                let query_val = self.evaluate_value(
                                                    &query_expr,
                                                    &[],
                                                    &schema,
                                                    params,
                                                )?;
                                                if let Value::Vector(query_vec) = query_val {
                                                    let idx_name = format!(
                                                        "hnsw_{}_{}",
                                                        table_name, storage_col_name
                                                    );
                                                    let search_results = self
                                                        .vector_index
                                                        .search(&idx_name, &query_vec, l)?;

                                                    for (id, _dist) in search_results {
                                                        let key =
                                                            format!("data:{}:{}", table_name, id);
                                                        if let Some(row) = self.row_cache.get(&key)
                                                        {
                                                            rows.push(row);
                                                        } else if let Some(data) =
                                                            txn.get(key.as_bytes()).await?
                                                        {
                                                            if let Ok(row) =
                                                                Self::decode_row_for_projection(
                                                                    &data,
                                                                    projection_indices.as_deref(),
                                                                )
                                                            {
                                                                if projection_indices.is_none() {
                                                                    self.row_cache
                                                                        .insert(key, row.clone());
                                                                }
                                                                rows.push(row);
                                                            }
                                                        }
                                                    }
                                                    index_used = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Optimization: Check for Primary Key (Clustered Index) Lookup
                if let Some(sel) = selection {
                    if let Expr::BinaryOp {
                        left,
                        op: BinaryOperator::Eq,
                        right,
                    } = sel
                    {
                        if let Some(value_expr) = pk_index.and_then(|pk_idx| {
                            self.equality_primary_key_value_expr(left, right, &schema, pk_idx)
                        }) {
                            let val = self
                                .evaluate_value(value_expr, &[], &schema, params)
                                .unwrap_or(Value::Null);
                            let val = if let Some(pk_idx) = pk_index {
                                Self::coerce_value_to_column_type(
                                    val,
                                    &schema.columns[pk_idx].data_type,
                                )
                                .unwrap_or(Value::Null)
                            } else {
                                val
                            };
                            let row_id = match val {
                                Value::Integer(i) => {
                                    Some(crate::common::encoding::encode_i64_comparable(i))
                                }
                                Value::String(s) => Some(s),
                                Value::Date(days) => Some(
                                    crate::common::encoding::encode_i64_comparable(days as i64),
                                ),
                                Value::Timestamp(micros) => {
                                    Some(crate::common::encoding::encode_i64_comparable(micros))
                                }
                                _ => None,
                            };

                            if let Some(id) = row_id {
                                let key = format!("data:{}:{}", table_name, id);

                                if key_only_scan {
                                    if txn.get(key.as_bytes()).await?.is_some() {
                                        let row =
                                            Self::primary_key_row_from_id(&schema, pk_index, &id);
                                        return Ok((schema, vec![row], false));
                                    }
                                    return Ok((schema, vec![], false));
                                }

                                if let Some(row) = self.row_cache.get(&key) {
                                    monitor::inc_row_cache_hit();
                                    return Ok((schema, vec![row], false));
                                }

                                if let Some(v) = txn.get(key.as_bytes()).await? {
                                    monitor::inc_row_read();
                                    let lookup_projection_indices =
                                        projection_indices.as_ref().map(|indices| {
                                            if let Some(pk_idx) = pk_index {
                                                indices
                                                    .iter()
                                                    .copied()
                                                    .filter(|idx| *idx != pk_idx)
                                                    .collect::<Vec<_>>()
                                            } else {
                                                indices.clone()
                                            }
                                        });
                                    let row = Self::decode_row_for_projection(
                                        &v,
                                        lookup_projection_indices.as_deref(),
                                    )
                                    .map_err(|e| {
                                        FusionError::Execution(format!(
                                            "Data deserialization error: {}",
                                            e
                                        ))
                                    })?;
                                    let mut row = row;
                                    if let Some(pk_idx) = pk_index {
                                        if row
                                            .get(pk_idx)
                                            .is_some_and(|value| value == &Value::Null)
                                        {
                                            if let Some(value) = Self::primary_key_row_from_id(
                                                &schema, pk_index, &id,
                                            )
                                            .get(pk_idx)
                                            {
                                                row[pk_idx] = value.clone();
                                            }
                                        }
                                    }

                                    if projection_indices.is_none() {
                                        self.row_cache.insert(key, row.clone());
                                    }

                                    return Ok((schema, vec![row], false));
                                }
                                return Ok((schema, vec![], false));
                            }
                        }
                    }
                }

                // Optimization: Primary Key Range Scan
                if let Some(sel) = selection {
                    if !index_used {
                        if let Expr::BinaryOp { left, op, right } = sel {
                            if let Some((range_op, value_expr)) = pk_index.and_then(|pk_idx| {
                                self.primary_key_range_value_expr(left, op, right, &schema, pk_idx)
                            }) {
                                let val = self
                                    .evaluate_value(value_expr, &[], &schema, params)
                                    .unwrap_or(Value::Null);
                                let val = if let Some(pk_idx) = pk_index {
                                    Self::coerce_value_to_column_type(
                                        val,
                                        &schema.columns[pk_idx].data_type,
                                    )
                                    .unwrap_or(Value::Null)
                                } else {
                                    val
                                };

                                if let Some(limit_val) = match val {
                                    Value::Integer(i) => Some(i),
                                    Value::Date(days) => Some(days as i64),
                                    Value::Timestamp(micros) => Some(micros),
                                    _ => None,
                                } {
                                    let table_prefix = format!("data:{}:", table_name);
                                    let min_key = table_prefix.as_bytes().to_vec();
                                    let mut max_key = table_prefix.as_bytes().to_vec();
                                    max_key.push(0xFF);

                                    let (start_key, end_key) = match range_op {
                                        BinaryOperator::Gt => {
                                            let encoded =
                                                crate::common::encoding::encode_i64_comparable(
                                                    limit_val,
                                                );
                                            let mut key = table_prefix.into_bytes();
                                            key.extend_from_slice(encoded.as_bytes());
                                            key.push(0x00);
                                            (Some(key), Some(max_key))
                                        }
                                        BinaryOperator::GtEq => {
                                            let encoded =
                                                crate::common::encoding::encode_i64_comparable(
                                                    limit_val,
                                                );
                                            let mut key = table_prefix.into_bytes();
                                            key.extend_from_slice(encoded.as_bytes());
                                            (Some(key), Some(max_key))
                                        }
                                        BinaryOperator::Lt => {
                                            let encoded =
                                                crate::common::encoding::encode_i64_comparable(
                                                    limit_val,
                                                );
                                            let mut key = table_prefix.into_bytes();
                                            key.extend_from_slice(encoded.as_bytes());
                                            (Some(min_key), Some(key))
                                        }
                                        BinaryOperator::LtEq => {
                                            let encoded =
                                                crate::common::encoding::encode_i64_comparable(
                                                    limit_val,
                                                );
                                            let mut key = table_prefix.into_bytes();
                                            key.extend_from_slice(encoded.as_bytes());
                                            key.push(0x00);
                                            (Some(min_key), Some(key))
                                        }
                                        _ => (None, None),
                                    };

                                    if let (Some(start), Some(end)) = (start_key, end_key) {
                                        index_used = true;
                                        selection_fully_applied = true;
                                        let kv_pairs = txn.scan_range(&start, &end, limit).await?;
                                        for (k, v) in kv_pairs {
                                            let row = if key_only_scan {
                                                let k_str = String::from_utf8_lossy(&k);
                                                let prefix = format!("data:{}:", table_name);
                                                if let Some(pk_str) = k_str.strip_prefix(&prefix) {
                                                    Self::primary_key_row_from_id(
                                                        &schema, pk_index, pk_str,
                                                    )
                                                } else {
                                                    continue;
                                                }
                                            } else {
                                                let cache_key = std::str::from_utf8(&k).ok();
                                                if let Some(key_str) = cache_key {
                                                    if let Some(row) = self.row_cache.get(key_str) {
                                                        monitor::inc_row_cache_hit();
                                                        rows.push(row);
                                                        if let Some(l) = limit {
                                                            if rows.len() >= l {
                                                                break;
                                                            }
                                                        }
                                                        continue;
                                                    }
                                                }

                                                let row = Self::decode_row_for_projection(
                                                    &v,
                                                    projection_indices.as_deref(),
                                                )
                                                .map_err(|e| {
                                                    FusionError::Execution(format!(
                                                        "Data deserialization error: {}",
                                                        e
                                                    ))
                                                })?;
                                                if projection_indices.is_none() {
                                                    if let Some(key_str) = cache_key {
                                                        self.row_cache.insert(
                                                            key_str.to_string(),
                                                            row.clone(),
                                                        );
                                                    }
                                                }
                                                row
                                            };
                                            rows.push(row);
                                            if let Some(l) = limit {
                                                if rows.len() >= l {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Try Index Scan
                if !index_used {
                    if let Some(sel) = selection {
                        let index_candidate_cap = self
                            .stats_guided_index_probe_limit(
                                &table_name,
                                sel,
                                &schema,
                                txn,
                                limit,
                                order_by,
                            )
                            .await?
                            .unwrap_or_else(|| Self::index_candidate_cap(limit, order_by));
                        let index_probe_limit = index_candidate_cap.saturating_add(1);
                        if let Some(index_plan) = self
                            .try_index_scan(
                                sel,
                                &table_name,
                                &schema,
                                txn,
                                params,
                                Some(index_probe_limit),
                                order_by,
                                ordered_limit,
                            )
                            .await?
                        {
                            let ordered_index_rows = index_plan.ordered_row_ids.is_some();
                            let mut row_ids_vec = if let Some(ordered) = index_plan.ordered_row_ids
                            {
                                ordered
                            } else {
                                let mut row_ids_vec: Vec<String> =
                                    index_plan.row_ids.into_iter().collect();
                                row_ids_vec.sort_unstable();
                                row_ids_vec
                            };

                            if order_by.is_none() {
                                if let Some(l) = limit {
                                    if row_ids_vec.len() > l {
                                        row_ids_vec.truncate(l);
                                    }
                                }
                            } else if ordered_index_rows && index_plan.exact {
                                if let Some(l) = ordered_limit.or(limit) {
                                    if row_ids_vec.len() > l {
                                        row_ids_vec.truncate(l);
                                    }
                                }
                            }

                            if row_ids_vec.is_empty() && index_plan.exact {
                                index_used = true;
                                selection_fully_applied = true;
                            } else if Self::should_use_index_plan(
                                row_ids_vec.len(),
                                index_candidate_cap,
                            ) {
                                index_used = true;
                                selection_fully_applied = index_plan.exact;
                                rows_satisfy_order_by = ordered_index_rows;
                                let row_fetch_limit = if ordered_index_rows {
                                    ordered_limit.or(limit)
                                } else {
                                    limit
                                };

                                if ordered_index_rows
                                    || row_ids_vec.len() <= SMALL_INDEX_FETCH_THRESHOLD
                                {
                                    for row_id in row_ids_vec {
                                        let data_key = format!("data:{}:{}", table_name, row_id);

                                        let row = if let Some(row) = self.row_cache.get(&data_key) {
                                            monitor::inc_row_cache_hit();
                                            row
                                        } else if key_only_scan {
                                            Self::primary_key_row_from_id(
                                                &schema, pk_index, &row_id,
                                            )
                                        } else if let Some(data_bytes) =
                                            txn.get(data_key.as_bytes()).await?
                                        {
                                            monitor::inc_row_read();
                                            let row: Vec<Value> = Self::decode_row_for_projection(
                                                &data_bytes,
                                                projection_indices.as_deref(),
                                            )
                                            .map_err(|e| {
                                                FusionError::Execution(format!(
                                                    "Data deserialization error: {}",
                                                    e
                                                ))
                                            })?;
                                            if projection_indices.is_none() {
                                                self.row_cache.insert(data_key, row.clone());
                                            }
                                            row
                                        } else {
                                            continue;
                                        };

                                        if !index_plan.exact
                                            && !self.evaluate_expr(sel, &row, &schema, params)?
                                        {
                                            continue;
                                        }

                                        rows.push(row);
                                        if let Some(l) = row_fetch_limit {
                                            if rows.len() >= l {
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    let txn_ref = &*txn;

                                    let table_name_for_stream =
                                        Arc::<str>::from(table_name.as_str());
                                    let projection_indices_for_stream =
                                        projection_indices.clone().map(Arc::<[usize]>::from);
                                    let schema_width = schema.columns.len();
                                    let pk_type_upper_for_stream = if key_only_scan {
                                        pk_index.and_then(|idx| {
                                            schema.columns.get(idx).map(|column| {
                                                Arc::<str>::from(
                                                    column
                                                        .data_type
                                                        .to_ascii_uppercase()
                                                        .into_boxed_str(),
                                                )
                                            })
                                        })
                                    } else {
                                        None
                                    };
                                    let executor_for_stream = self;

                                    let fetch_stream = futures::stream::iter(row_ids_vec)
                                        .map(|row_id| {
                                            let table_name = table_name_for_stream.clone();
                                            let projection_indices =
                                                projection_indices_for_stream.clone();
                                            let pk_type_upper = pk_type_upper_for_stream.clone();
                                            let executor = executor_for_stream;

                                            async move {
                                                let data_key = format!(
                                                    "data:{}:{}",
                                                    table_name.as_ref(),
                                                    row_id
                                                );

                                                if let Some(row) = executor.row_cache.get(&data_key)
                                                {
                                                    return Ok::<_, FusionError>(Some((
                                                        row_id, row, true, false, false,
                                                    )));
                                                }

                                                if key_only_scan {
                                                    let r = Self::primary_key_row_from_parts(
                                                        schema_width,
                                                        pk_index,
                                                        pk_type_upper.as_deref(),
                                                        &row_id,
                                                    );
                                                    return Ok(Some((
                                                        row_id, r, false, false, false,
                                                    )));
                                                }

                                                if let Some(data_bytes) =
                                                    txn_ref.get(data_key.as_bytes()).await?
                                                {
                                                    let cacheable = projection_indices.is_none();
                                                    let row = Self::decode_row_for_projection(
                                                        &data_bytes,
                                                        projection_indices.as_deref(),
                                                    )
                                                    .map_err(|e| {
                                                        FusionError::Execution(format!(
                                                            "Data deserialization error: {}",
                                                            e
                                                        ))
                                                    })?;
                                                    Ok(Some((row_id, row, false, cacheable, true)))
                                                } else {
                                                    Ok(None)
                                                }
                                            }
                                        })
                                        .buffer_unordered(128);

                                    let mut stream = fetch_stream;
                                    while let Some(res) = stream.next().await {
                                        let res = res?;
                                        if let Some((
                                            row_id,
                                            row,
                                            from_cache,
                                            cacheable,
                                            read_storage,
                                        )) = res
                                        {
                                            if read_storage {
                                                monitor::inc_row_read();
                                                if cacheable {
                                                    let data_key =
                                                        format!("data:{}:{}", table_name, row_id);
                                                    self.row_cache.insert(data_key, row.clone());
                                                }
                                            } else if from_cache {
                                                monitor::inc_row_cache_hit();
                                            }

                                            if !index_plan.exact
                                                && !self
                                                    .evaluate_expr(sel, &row, &schema, params)?
                                            {
                                                continue;
                                            }

                                            rows.push(row);
                                            if let Some(l) = row_fetch_limit {
                                                if rows.len() >= l {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Full Table Scan
                if !index_used {
                    let prefix_str = format!("data:{}:", table_name);
                    let prefix = prefix_str.as_bytes().to_vec();

                    let scan_limit = if selection.is_none() {
                        effective_limit
                    } else {
                        None
                    };

                    let kv_pairs = txn.scan_prefix(&prefix, scan_limit).await?;

                    for (k, v) in kv_pairs {
                        let row_res = if key_only_scan {
                            let k_str = String::from_utf8_lossy(&k);
                            let prefix_str = String::from_utf8_lossy(&prefix);
                            if let Some(pk_str) = k_str.strip_prefix(prefix_str.as_ref()) {
                                Some(Self::primary_key_row_from_id(&schema, pk_index, pk_str))
                            } else {
                                None
                            }
                        } else if zero_column_projection {
                            Some(Vec::new())
                        } else {
                            match std::str::from_utf8(&k) {
                                Ok(key_str) => {
                                    if let Some(row) = self.row_cache.get(key_str) {
                                        monitor::inc_row_cache_hit();
                                        Some(row)
                                    } else if projection_indices.is_none() {
                                        Self::decode_row_for_projection(&v, None).ok().map(|row| {
                                            self.row_cache.insert(key_str.to_string(), row.clone());
                                            row
                                        })
                                    } else {
                                        Self::decode_row_for_projection(
                                            &v,
                                            projection_indices.as_deref(),
                                        )
                                        .ok()
                                    }
                                }
                                Err(_) => Self::decode_row_for_projection(
                                    &v,
                                    projection_indices.as_deref(),
                                )
                                .ok(),
                            }
                        };

                        if let Some(row) = row_res {
                            if let Some(sel) = &selection {
                                if self.evaluate_expr(sel, &row, &schema, params)? {
                                    rows.push(row);
                                    if let Some(l) = limit {
                                        if rows.len() >= l {
                                            break;
                                        }
                                    }
                                }
                            } else {
                                rows.push(row);
                                if let Some(l) = limit {
                                    if rows.len() >= l {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if let Some(sel) = selection {
                    if !selection_fully_applied && rows.len() > 1000 {
                        let filtered_rows = self.parallel_filter_rows(rows, sel, &schema, params);
                        rows = filtered_rows;
                        if let Some(l) = limit {
                            if rows.len() > l {
                                rows.truncate(l);
                            }
                        }
                    } else if !selection_fully_applied {
                        let capacity = limit.map_or(rows.len(), |value| rows.len().min(value));
                        let mut filtered_rows = Vec::with_capacity(capacity);
                        for row in rows {
                            if self.evaluate_expr(sel, &row, &schema, params)? {
                                filtered_rows.push(row);
                                if let Some(l) = limit {
                                    if filtered_rows.len() >= l {
                                        break;
                                    }
                                }
                            }
                        }
                        rows = filtered_rows;
                    }
                }

                Ok((schema, rows, rows_satisfy_order_by))
            }
            _ => Err(FusionError::Execution(
                "Unsupported table factor".to_string(),
            )),
        }
    }
}
