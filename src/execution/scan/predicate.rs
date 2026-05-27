use crate::catalog::TableSchema;
use sqlparser::ast::{BinaryOperator, Expr};
use std::collections::HashSet;

use super::Executor;

impl Executor {
    fn split_conjunctive_predicates(expr: &Expr, out: &mut Vec<Expr>) {
        if let Expr::BinaryOp {
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

    fn conjunctive_predicate_count(expr: &Expr) -> usize {
        if let Expr::BinaryOp {
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

    pub(crate) fn collect_conjunctive_predicates(expr: &Expr) -> Vec<Expr> {
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
