mod function;
mod pattern;
mod subquery;
mod value;

use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value as SqlValue,
};
use std::collections::HashSet;
use std::fmt::Write as FmtWrite;

use super::Executor;

fn group_function_arg_name(index: usize) -> String {
    let mut name = String::with_capacity("__group_fn_arg_".len() + usize_decimal_len(index));
    name.push_str("__group_fn_arg_");
    write!(&mut name, "{index}").expect("writing to String cannot fail");
    name
}

fn usize_decimal_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 10 {
        value /= 10;
        len += 1;
    }
    len
}

impl Executor {
    pub(crate) fn placeholder_index(placeholder: &str) -> usize {
        placeholder
            .strip_prefix('$')
            .unwrap_or(placeholder)
            .parse::<usize>()
            .unwrap_or(0)
    }

    fn compound_identifier_name(idents: &[sqlparser::ast::Ident]) -> String {
        let capacity = idents.iter().map(|ident| ident.value.len()).sum::<usize>()
            + idents.len().saturating_sub(1);
        let mut name = String::with_capacity(capacity);

        for (index, ident) in idents.iter().enumerate() {
            if index > 0 {
                name.push('.');
            }
            name.push_str(&ident.value);
        }

        name
    }

    pub(crate) fn evaluate_expr(
        &self,
        expr: &Expr,
        row: &[Value],
        schema: &TableSchema,
        params: &[Value],
    ) -> Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Handle logical operators with short-circuit evaluation
                match op {
                    BinaryOperator::And => {
                        let l = self.evaluate_expr(left, row, schema, params)?;
                        if !l {
                            return Ok(false);
                        }
                        return self.evaluate_expr(right, row, schema, params);
                    }
                    BinaryOperator::Or => {
                        let l = self.evaluate_expr(left, row, schema, params)?;
                        if l {
                            return Ok(true);
                        }
                        return self.evaluate_expr(right, row, schema, params);
                    }
                    _ => {}
                }

                let left_val = self.evaluate_value(left, row, schema, params)?;
                let right_val = self.evaluate_value(right, row, schema, params)?;
                let (left_val, right_val) =
                    self.align_comparison_values(left, left_val, right, right_val, schema)?;

                match op {
                    BinaryOperator::Eq => {
                        if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                            return Ok(false);
                        }
                        Ok(left_val.compare(&right_val) == std::cmp::Ordering::Equal)
                    }
                    BinaryOperator::NotEq => {
                        if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                            return Ok(false);
                        }
                        Ok(left_val.compare(&right_val) != std::cmp::Ordering::Equal)
                    }
                    BinaryOperator::Gt => self.compare_values(&left_val, &right_val, |l, r| l > r),
                    BinaryOperator::Lt => self.compare_values(&left_val, &right_val, |l, r| l < r),
                    BinaryOperator::GtEq => {
                        self.compare_values(&left_val, &right_val, |l, r| l >= r)
                    }
                    BinaryOperator::LtEq => {
                        self.compare_values(&left_val, &right_val, |l, r| l <= r)
                    }
                    _ => Err(FusionError::Execution(format!(
                        "Unsupported operator: {}",
                        op
                    ))),
                }
            }
            Expr::MatchAgainst {
                columns,
                match_value,
                ..
            } => {
                let search_terms = if let SqlValue::SingleQuotedString(s) = match_value {
                    Self::tokenize(s)
                } else if let SqlValue::Placeholder(p) = match_value {
                    let idx = Self::placeholder_index(p);
                    if idx > 0 && idx <= params.len() {
                        if let Value::String(s) = &params[idx - 1] {
                            Self::tokenize(s)
                        } else {
                            return Err(FusionError::Execution(
                                "MATCH AGAINST parameter must be a string".to_string(),
                            ));
                        }
                    } else {
                        return Err(FusionError::Execution(
                            "Invalid parameter index".to_string(),
                        ));
                    }
                } else {
                    return Err(FusionError::Execution(
                        "MATCH AGAINST requires a string literal or placeholder".to_string(),
                    ));
                };

                if search_terms.is_empty() {
                    return Ok(false);
                }

                if columns.len() != 1 {
                    return Err(FusionError::Execution(
                        "MATCH currently supports only single column".to_string(),
                    ));
                }
                let col_ident = &columns[0];
                let col_name = col_ident.to_string();

                let col_idx = self.resolve_column_index(&col_name, schema)?;
                let val = &row[col_idx];

                if let Value::String(text) = val {
                    let text_tokens = Self::tokenize_unique(text);
                    for term in search_terms {
                        if !text_tokens.contains(&term) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let mut found = false;
                for item in list {
                    let item_val = self.evaluate_value(item, row, schema, params)?;
                    let (_, item_val) =
                        self.align_comparison_values(expr, val.clone(), item, item_val, schema)?;
                    if val.compare(&item_val) == std::cmp::Ordering::Equal {
                        found = true;
                        break;
                    }
                }
                if *negated {
                    Ok(!found)
                } else {
                    Ok(found)
                }
            }
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => self.evaluate_quantified_array_comparison(
                left, compare_op, right, false, row, schema, params,
            ),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => self.evaluate_quantified_array_comparison(
                left, compare_op, right, true, row, schema, params,
            ),
            Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                let s = self.evaluate_value(expr, row, schema, params)?;
                let p = self.evaluate_value(pattern, row, schema, params)?;
                if let (Value::String(s_str), Value::String(p_str)) = (s, p) {
                    let matched = Self::like_match(&s_str, &p_str);
                    if *negated {
                        Ok(!matched)
                    } else {
                        Ok(matched)
                    }
                } else {
                    Ok(false)
                }
            }
            Expr::ILike {
                expr,
                pattern,
                negated,
                ..
            } => {
                let s = self.evaluate_value(expr, row, schema, params)?;
                let p = self.evaluate_value(pattern, row, schema, params)?;
                if let (Value::String(s_str), Value::String(p_str)) = (s, p) {
                    let matched = Self::like_match(&s_str.to_lowercase(), &p_str.to_lowercase());
                    Ok(if *negated { !matched } else { matched })
                } else {
                    Ok(false)
                }
            }
            Expr::IsNull(expr) => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                Ok(val == Value::Null)
            }
            Expr::IsNotNull(expr) => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                Ok(val != Value::Null)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let val = self.evaluate_value(expr, row, schema, params)?;
                let low_val = self.evaluate_value(low, row, schema, params)?;
                let high_val = self.evaluate_value(high, row, schema, params)?;
                let (val, low_val) =
                    self.align_comparison_values(expr, val, low, low_val, schema)?;
                let (val, high_val) =
                    self.align_comparison_values(expr, val, high, high_val, schema)?;
                let ge = self.compare_values(&val, &low_val, |l, r| l >= r)?;
                let le = self.compare_values(&val, &high_val, |l, r| l <= r)?;
                let result = ge && le;
                Ok(if *negated { !result } else { result })
            }
            Expr::Nested(inner) => self.evaluate_expr(inner, row, schema, params),
            Expr::UnaryOp { op, expr } => match op {
                sqlparser::ast::UnaryOperator::Not => {
                    let res = self.evaluate_expr(expr, row, schema, params)?;
                    Ok(!res)
                }
                _ => Err(FusionError::Execution(
                    "Unsupported unary operator in boolean expression".to_string(),
                )),
            },
            Expr::IsFalse(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(val == Value::Boolean(false))
            }
            Expr::IsTrue(inner) => {
                let val = self.evaluate_value(inner, row, schema, params)?;
                Ok(val == Value::Boolean(true))
            }
            Expr::Value(v) => match &v.value {
                SqlValue::Boolean(b) => Ok(*b),
                _ => Err(FusionError::Execution(format!(
                    "Cannot use {:?} as boolean",
                    v.value
                ))),
            },
            _ => {
                // Fallback: try evaluate_value and check if it's a boolean
                match self.evaluate_value(expr, row, schema, params) {
                    Ok(Value::Boolean(b)) => Ok(b),
                    Ok(_) => Err(FusionError::Execution(format!(
                        "Unsupported expression type: {}",
                        expr
                    ))),
                    Err(e) => Err(e),
                }
            }
        }
    }

    pub(crate) fn extract_aggregates_from_expr(
        &self,
        expr: &Expr,
        aggregates: &mut Vec<(Expr, String)>,
    ) {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if matches!(
                    name.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "ARRAY_AGG"
                        | "STRING_AGG"
                        | "GROUP_CONCAT"
                ) {
                    // Check for DISTINCT modifier (e.g., COUNT(DISTINCT col))
                    let is_distinct = if let FunctionArguments::List(args) = &func.args {
                        args.duplicate_treatment
                            == Some(sqlparser::ast::DuplicateTreatment::Distinct)
                    } else {
                        false
                    };
                    let effective_name = if is_distinct && name == "COUNT" {
                        "COUNT_DISTINCT".to_string()
                    } else {
                        name
                    };
                    if !aggregates.iter().any(|(e, _)| e == expr) {
                        aggregates.push((expr.clone(), effective_name));
                    }
                } else if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.extract_aggregates_from_expr(e, aggregates);
                        }
                    }
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_aggregates_from_expr(left, aggregates);
                self.extract_aggregates_from_expr(right, aggregates);
            }
            Expr::Nested(expr) => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::UnaryOp { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::Cast { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            Expr::Extract { expr, .. } => self.extract_aggregates_from_expr(expr, aggregates),
            _ => {}
        }
    }

    pub(crate) fn extract_columns_from_expr(&self, expr: &Expr, cols: &mut HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => {
                cols.insert(ident.value.clone());
            }
            Expr::CompoundIdentifier(idents) => {
                cols.insert(Self::compound_identifier_name(idents));
            }
            Expr::BinaryOp { left, right, .. } => {
                self.extract_columns_from_expr(left, cols);
                self.extract_columns_from_expr(right, cols);
            }
            Expr::Nested(expr) => self.extract_columns_from_expr(expr, cols),
            Expr::UnaryOp { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Cast { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Extract { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Array(array) => {
                for elem in &array.elem {
                    self.extract_columns_from_expr(elem, cols);
                }
            }
            Expr::CompoundFieldAccess { root, access_chain } => {
                self.extract_columns_from_expr(root, cols);
                for access in access_chain {
                    if let sqlparser::ast::AccessExpr::Subscript(
                        sqlparser::ast::Subscript::Index { index },
                    ) = access
                    {
                        self.extract_columns_from_expr(index, cols);
                    }
                }
            }
            Expr::Function(func) => {
                if let FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.extract_columns_from_expr(e, cols);
                        }
                    }
                }
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                self.extract_columns_from_expr(expr, cols);
                if let Some(from) = substring_from {
                    self.extract_columns_from_expr(from, cols);
                }
                if let Some(for_expr) = substring_for {
                    self.extract_columns_from_expr(for_expr, cols);
                }
            }
            Expr::InList { expr, list, .. } => {
                self.extract_columns_from_expr(expr, cols);
                for e in list {
                    self.extract_columns_from_expr(e, cols);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_columns_from_expr(expr, cols);
                self.extract_columns_from_expr(low, cols);
                self.extract_columns_from_expr(high, cols);
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                self.extract_columns_from_expr(left, cols);
                self.extract_columns_from_expr(right, cols);
            }
            Expr::IsNull(expr) => self.extract_columns_from_expr(expr, cols),
            Expr::IsNotNull(expr) => self.extract_columns_from_expr(expr, cols),
            Expr::InSubquery { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Like { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::ILike { expr, .. } => self.extract_columns_from_expr(expr, cols),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    self.extract_columns_from_expr(op, cols);
                }
                for cw in conditions {
                    self.extract_columns_from_expr(&cw.condition, cols);
                    self.extract_columns_from_expr(&cw.result, cols);
                }
                if let Some(el) = else_result {
                    self.extract_columns_from_expr(el, cols);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn evaluate_final_group_expr(
        &self,
        expr: &Expr,
        group_key: &[Value],
        group_exprs: &[Expr],
        agg_map: &std::collections::HashMap<Expr, Value>,
        _schema: &TableSchema,
        _params: &[Value],
    ) -> Result<Value> {
        // 1. Check if it is a pre-calculated aggregate
        if let Some(val) = agg_map.get(expr) {
            return Ok(val.clone());
        }

        // 2. Check if it matches a group expression
        if let Some(idx) = group_exprs.iter().position(|e| e == expr) {
            return Ok(group_key[idx].clone());
        }

        // 3. Recurse / Evaluate
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let l = self.evaluate_final_group_expr(
                    left,
                    group_key,
                    group_exprs,
                    agg_map,
                    _schema,
                    _params,
                )?;
                let r = self.evaluate_final_group_expr(
                    right,
                    group_key,
                    group_exprs,
                    agg_map,
                    _schema,
                    _params,
                )?;
                self.evaluate_binary_op(l, op, r)
            }
            Expr::Nested(e) => {
                self.evaluate_final_group_expr(e, group_key, group_exprs, agg_map, _schema, _params)
            }
            Expr::Value(v) => {
                if let SqlValue::Placeholder(p) = &v.value {
                    let idx = Self::placeholder_index(p);
                    if idx > 0 && idx <= _params.len() {
                        return Ok(_params[idx - 1].clone());
                    }
                }
                Ok(self.sql_value_to_fusion_value(&v.value))
            }
            Expr::Array(_) | Expr::TypedString(_) => {
                self.evaluate_value(expr, &[], _schema, _params)
            }
            Expr::Function(func) if func.name.to_string().eq_ignore_ascii_case("COALESCE") => {
                let FunctionArguments::List(args) = &func.args else {
                    return Ok(Value::Null);
                };
                for arg in &args.args {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
                        continue;
                    };
                    let value = self.evaluate_final_group_expr(
                        expr,
                        group_key,
                        group_exprs,
                        agg_map,
                        _schema,
                        _params,
                    )?;
                    if value != Value::Null {
                        return Ok(value);
                    }
                }
                Ok(Value::Null)
            }
            Expr::Function(func) => {
                let FunctionArguments::List(args) = &func.args else {
                    return Err(FusionError::Execution(
                        "Unsupported function argument format".to_string(),
                    ));
                };
                let mut evaluated_row = Vec::with_capacity(args.args.len());
                let mut evaluated_args = Vec::with_capacity(args.args.len());
                for (index, arg) in args.args.iter().enumerate() {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
                        return Err(FusionError::Execution(
                            "Unsupported function argument type".to_string(),
                        ));
                    };
                    let value = self.evaluate_final_group_expr(
                        expr,
                        group_key,
                        group_exprs,
                        agg_map,
                        _schema,
                        _params,
                    )?;
                    let arg_name = group_function_arg_name(index);
                    evaluated_row.push(value);
                    evaluated_args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        Expr::Identifier(sqlparser::ast::Ident::new(arg_name)),
                    )));
                }

                let mut evaluated_func = func.clone();
                evaluated_func.args = FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: evaluated_args,
                    clauses: vec![],
                });
                let function_schema = TableSchema::new(
                    "__group_function_args".to_string(),
                    (0..evaluated_row.len())
                        .map(|index| crate::catalog::Column {
                            name: group_function_arg_name(index),
                            data_type: "UNKNOWN".to_string(),
                            is_primary: false,
                            is_indexed: false,
                            index_type: crate::catalog::IndexType::None,
                            default_value: None,
                            is_nullable: true,
                            is_unique: false,
                            check_expr: None,
                        })
                        .collect(),
                );

                self.evaluate_function(&evaluated_func, &evaluated_row, &function_schema, _params)
            }
            Expr::Substring { .. } => {
                if let Some(idx) = group_exprs.iter().position(|e| e == expr) {
                    Ok(group_key[idx].clone())
                } else {
                    Err(FusionError::Execution(
                        "SUBSTRING expression must appear in the GROUP BY clause or be used in an aggregate function".to_string(),
                    ))
                }
            }
            Expr::Identifier(ident) => Err(FusionError::Execution(format!(
                "Column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                ident.value
            ))),
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_final_group_expr(
                    expr,
                    group_key,
                    group_exprs,
                    agg_map,
                    _schema,
                    _params,
                )?;
                match op {
                    sqlparser::ast::UnaryOperator::Minus => match val {
                        Value::Integer(i) => Ok(Value::Integer(-i)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Err(FusionError::Execution(
                            "Unary minus on non-number".to_string(),
                        )),
                    },
                    _ => Err(FusionError::Execution(
                        "Unsupported unary operator in GROUP BY".to_string(),
                    )),
                }
            }
            _ => Err(FusionError::Execution(
                "Unsupported expression in GROUP BY projection".to_string(),
            )),
        }
    }

    // Optimize: Parallel Scan for Wildcard LIKE using Rayon
    pub(crate) fn parallel_filter_rows(
        &self,
        rows: Vec<Vec<Value>>,
        filter_expr: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Vec<Vec<Value>> {
        use rayon::iter::IntoParallelIterator;
        use rayon::iter::ParallelIterator;

        rows.into_par_iter()
            .filter(|row| {
                self.evaluate_expr(filter_expr, row, schema, params)
                    .unwrap_or(false)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{group_function_arg_name, usize_decimal_len, Executor};

    #[test]
    fn placeholder_index_parses_dollar_parameters() {
        assert_eq!(Executor::placeholder_index("$1"), 1);
        assert_eq!(Executor::placeholder_index("2"), 2);
        assert_eq!(Executor::placeholder_index("$bad"), 0);
    }

    #[test]
    fn group_function_arg_name_preallocates_exact_name() {
        let name = group_function_arg_name(42);

        assert_eq!(name, "__group_fn_arg_42");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn expr_usize_decimal_len_counts_digits() {
        assert_eq!(usize_decimal_len(0), 1);
        assert_eq!(usize_decimal_len(9), 1);
        assert_eq!(usize_decimal_len(10), 2);
    }

    #[test]
    fn tokenize_unique_deduplicates_tokens() {
        let tokens = Executor::tokenize_unique("Quick quick, brown fox!");
        assert_eq!(tokens.len(), 3);
        assert!(tokens.contains("quick"));
        assert!(tokens.contains("brown"));
        assert!(tokens.contains("fox"));
    }

    #[test]
    fn like_match_fast_percent_patterns() {
        assert!(Executor::like_match("Alice", "Ali%"));
        assert!(Executor::like_match("Alice", "%ce"));
        assert!(Executor::like_match("Charlie", "%li%"));
        assert!(Executor::like_match("brand-a", "%a"));
        assert!(Executor::like_match("alphabet soup", "a%bet%soup"));
        assert!(!Executor::like_match("alphabet soup", "a%bet%soap"));
    }

    #[test]
    fn like_match_wildcard_patterns_still_match() {
        assert!(Executor::like_match("Bob", "Bo_"));
        assert!(Executor::like_match("Bob", "B?b"));
        assert!(!Executor::like_match("Bobby", "Bo_"));
    }
}
