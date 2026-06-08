use crate::catalog::TableSchema;
use sqlparser::ast::{BinaryOperator, Expr};
use std::collections::HashSet;

use super::Executor;

impl Executor {
    fn split_conjunctive_predicates(expr: &Expr, out: &mut Vec<Expr>) {
        if let Expr::Nested(inner) = expr {
            Self::split_conjunctive_predicates(inner, out);
        } else if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = expr
        {
            Self::split_conjunctive_predicates(left, out);
            Self::split_conjunctive_predicates(right, out);
        } else {
            out.push(expr.clone());
        }
    }

    fn split_disjunctive_predicates(expr: &Expr, out: &mut Vec<Expr>) {
        if let Expr::Nested(inner) = expr {
            Self::split_disjunctive_predicates(inner, out);
        } else if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = expr
        {
            Self::split_disjunctive_predicates(left, out);
            Self::split_disjunctive_predicates(right, out);
        } else {
            out.push(expr.clone());
        }
    }

    fn conjunctive_predicate_count(expr: &Expr) -> usize {
        if let Expr::Nested(inner) = expr {
            Self::conjunctive_predicate_count(inner)
        } else if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = expr
        {
            Self::conjunctive_predicate_count(left)
                .saturating_add(Self::conjunctive_predicate_count(right))
        } else {
            1
        }
    }

    fn disjunctive_predicate_count(expr: &Expr) -> usize {
        if let Expr::Nested(inner) = expr {
            Self::disjunctive_predicate_count(inner)
        } else if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = expr
        {
            Self::disjunctive_predicate_count(left)
                .saturating_add(Self::disjunctive_predicate_count(right))
        } else {
            1
        }
    }

    fn combine_disjunctive_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        let mut iter = predicates.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::Or,
            right: Box::new(expr),
        }))
    }

    fn remove_common_predicates(mut predicates: Vec<Expr>, common: &[Expr]) -> Vec<Expr> {
        for common_predicate in common {
            if let Some(index) = predicates
                .iter()
                .position(|predicate| predicate == common_predicate)
            {
                predicates.remove(index);
            }
        }
        predicates
    }

    fn extract_common_or_conjunctive_predicates(expr: &Expr) -> Option<Vec<Expr>> {
        let mut branches = Vec::with_capacity(Self::disjunctive_predicate_count(expr));
        Self::split_disjunctive_predicates(expr, &mut branches);
        if branches.len() < 2 {
            return None;
        }

        let branch_predicates = branches
            .iter()
            .map(|branch| {
                let mut predicates = Vec::with_capacity(Self::conjunctive_predicate_count(branch));
                Self::split_conjunctive_predicates(branch, &mut predicates);
                predicates
            })
            .collect::<Vec<_>>();

        let first_branch = branch_predicates.first()?;
        let mut common = Vec::with_capacity(first_branch.len());
        for predicate in first_branch {
            if common.contains(predicate) {
                continue;
            }
            if branch_predicates
                .iter()
                .all(|branch| branch.iter().any(|candidate| candidate == predicate))
            {
                common.push(predicate.clone());
            }
        }

        if common.is_empty() {
            return None;
        }

        let mut lifted = common.clone();
        let mut residual_branches = Vec::with_capacity(branch_predicates.len());
        for branch in branch_predicates {
            let residual = Self::remove_common_predicates(branch, &common);
            if residual.is_empty() {
                return Some(lifted);
            }
            if let Some(residual_expr) = Self::combine_predicates(residual) {
                residual_branches.push(residual_expr);
            }
        }

        if let Some(residual_expr) = Self::combine_disjunctive_predicates(residual_branches) {
            lifted.push(residual_expr);
        }
        Some(lifted)
    }

    pub(crate) fn collect_conjunctive_predicates(expr: &Expr) -> Vec<Expr> {
        if let Some(predicates) = Self::extract_common_or_conjunctive_predicates(expr) {
            return predicates;
        }
        let mut predicates = Vec::with_capacity(Self::conjunctive_predicate_count(expr));
        Self::split_conjunctive_predicates(expr, &mut predicates);
        predicates
    }

    pub(super) fn combine_predicates(predicates: Vec<Expr>) -> Option<Expr> {
        let mut iter = predicates.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(expr),
        }))
    }

    fn predicate_uses_only_relations(&self, expr: &Expr, relation_names: &HashSet<String>) -> bool {
        let mut columns = HashSet::new();
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }

        columns.into_iter().all(|column| {
            column
                .split('.')
                .next()
                .map(|prefix| relation_names.contains(prefix))
                .unwrap_or(false)
        })
    }

    pub(super) fn take_relation_predicate(
        &self,
        predicates: &mut Vec<Expr>,
        relation_names: &HashSet<String>,
    ) -> Option<Expr> {
        let predicate_count = predicates.len();
        let mut local = Vec::with_capacity(predicate_count);
        let mut remaining = Vec::with_capacity(predicate_count);

        for predicate in predicates.drain(..) {
            if self.predicate_uses_only_relations(&predicate, relation_names) {
                local.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }

        *predicates = remaining;
        Self::combine_predicates(local)
    }

    fn predicate_uses_only_schema(&self, expr: &Expr, schema: &TableSchema) -> bool {
        let mut columns = HashSet::new();
        self.extract_columns_from_expr(expr, &mut columns);
        if columns.is_empty() {
            return false;
        }

        columns
            .into_iter()
            .all(|column| self.schema_contains_column_reference(&column, schema))
    }

    pub(super) fn take_schema_predicate(
        &self,
        predicates: &mut Vec<Expr>,
        schema: &TableSchema,
    ) -> Option<Expr> {
        let predicate_count = predicates.len();
        let mut local = Vec::with_capacity(predicate_count);
        let mut remaining = Vec::with_capacity(predicate_count);

        for predicate in predicates.drain(..) {
            if self.predicate_uses_only_schema(&predicate, schema) {
                local.push(predicate);
            } else {
                remaining.push(predicate);
            }
        }

        *predicates = remaining;
        Self::combine_predicates(local)
    }

    pub(crate) fn column_name_from_expr(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => Some(Self::scan_compound_identifier_name(idents)),
            _ => None,
        }
    }

    fn scan_compound_identifier_name(idents: &[sqlparser::ast::Ident]) -> String {
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

    pub(super) fn resolve_schema_column_index(
        &self,
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<usize> {
        let col_name = Self::column_name_from_expr(expr)?;
        self.resolve_column_index(&col_name, schema).ok()
    }

    pub(super) fn resolve_schema_column_index_strict(
        &self,
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<usize> {
        let col_name = Self::column_name_from_expr(expr)?;
        if col_name.contains('.') {
            schema
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(&col_name))
        } else {
            self.resolve_column_index(&col_name, schema).ok()
        }
    }

    fn schema_contains_column_reference(&self, col_name: &str, schema: &TableSchema) -> bool {
        if col_name.contains('.') {
            schema
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(col_name))
        } else {
            self.resolve_column_index(col_name, schema).is_ok()
        }
    }

    pub(crate) fn resolve_schema_column_name(
        &self,
        expr: &Expr,
        schema: &TableSchema,
    ) -> Option<(usize, String)> {
        let idx = self.resolve_schema_column_index(expr, schema)?;
        Some((idx, schema.columns[idx].name.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;
    use sqlparser::ast::{
        Expr, JoinConstraint, JoinOperator, SelectItem, SetExpr, Statement, TableFactor,
    };

    #[test]
    fn collect_conjunctive_predicates_flattens_nested_on_clause() {
        let statements = crate::parser::parse_sql(
            "SELECT * FROM customer LEFT OUTER JOIN oorder ON \
             (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id AND o_carrier_id > 8)",
        )
        .unwrap();

        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let TableFactor::Table { .. } = &select.from[0].relation else {
            panic!("expected table");
        };
        assert!(matches!(select.projection[0], SelectItem::Wildcard(_)));

        let JoinOperator::LeftOuter(JoinConstraint::On(join_expr)) =
            &select.from[0].joins[0].join_operator
        else {
            panic!("expected left outer join ON expression");
        };
        assert!(matches!(join_expr, Expr::Nested(_)));

        let predicates = Executor::collect_conjunctive_predicates(join_expr);
        assert_eq!(predicates.len(), 4);
        assert_eq!(predicates[0].to_string(), "c_w_id = o_w_id");
        assert_eq!(predicates[3].to_string(), "o_carrier_id > 8");
    }

    #[test]
    fn collect_conjunctive_predicates_lifts_common_or_join_key() {
        let statements = crate::parser::parse_sql(
            "SELECT sum(ol_amount) FROM order_line, item \
             WHERE (ol_i_id = i_id AND i_data LIKE '%a' AND ol_w_id IN (1, 2, 3)) \
                OR (ol_i_id = i_id AND i_data LIKE '%b' AND ol_w_id IN (1, 2, 4)) \
                OR (ol_i_id = i_id AND i_data LIKE '%c' AND ol_w_id IN (1, 5, 3))",
        )
        .unwrap();

        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let selection = select.selection.as_ref().expect("expected selection");

        let predicates = Executor::collect_conjunctive_predicates(selection);
        assert_eq!(predicates.len(), 2);
        assert_eq!(predicates[0].to_string(), "ol_i_id = i_id");
        assert!(predicates[1].to_string().contains(" OR "));
        assert!(!predicates[1].to_string().contains("ol_i_id = i_id"));
    }
}
