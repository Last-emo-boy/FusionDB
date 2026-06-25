use crate::catalog::IndexType;
use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::storage::{ScanVisitor, Transaction};
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectName, ObjectNamePart, OrderByKind, SelectItem,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::Executor;

#[derive(Clone, Copy)]
enum ColumnAggregateKind {
    CountStar,
    CountColumn,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupColumnAggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg,
}

pub(super) struct ColumnAggregateScanPlan {
    kind: ColumnAggregateKind,
    column_index: Option<usize>,
    pub(super) output_name: String,
}

struct ColumnPredicateTerm {
    value_slot: usize,
    op: BinaryOperator,
    value: Value,
}

pub(super) struct ColumnPredicateScanPlan {
    terms: Vec<ColumnPredicateTerm>,
    column_indices: Vec<usize>,
}

#[cfg(test)]
fn column_scan_data_key_for_row_id(table_name: &str, row_id: &str) -> String {
    let mut key = String::with_capacity("data:".len() + table_name.len() + 1 + row_id.len());
    key.push_str("data:");
    key.push_str(table_name);
    key.push(':');
    key.push_str(row_id);
    key
}

#[cfg(test)]
fn column_scan_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn column_scan_index_prefix_for_value(
    table_name: &str,
    column_name: &str,
    value_key: &str,
) -> String {
    let mut prefix = String::with_capacity(
        "index:".len() + table_name.len() + 1 + column_name.len() + 1 + value_key.len() + 1,
    );
    prefix.push_str("index:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix.push_str(column_name);
    prefix.push(':');
    prefix.push_str(value_key);
    prefix.push(':');
    prefix
}

fn column_scan_function_name_eq_ascii(name: &ObjectName, expected: &str) -> bool {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => ident.value.eq_ignore_ascii_case(expected),
        [ObjectNamePart::Function(function)] => function.name.value.eq_ignore_ascii_case(expected),
        _ => false,
    }
}

fn group_column_aggregate_function_kind(name: &ObjectName) -> Option<GroupColumnAggregateFunction> {
    if column_scan_function_name_eq_ascii(name, "COUNT") {
        Some(GroupColumnAggregateFunction::Count)
    } else if column_scan_function_name_eq_ascii(name, "SUM") {
        Some(GroupColumnAggregateFunction::Sum)
    } else if column_scan_function_name_eq_ascii(name, "AVG") {
        Some(GroupColumnAggregateFunction::Avg)
    } else if column_scan_function_name_eq_ascii(name, "MIN") {
        Some(GroupColumnAggregateFunction::Min)
    } else if column_scan_function_name_eq_ascii(name, "MAX") {
        Some(GroupColumnAggregateFunction::Max)
    } else if column_scan_function_name_eq_ascii(name, "STRING_AGG")
        || column_scan_function_name_eq_ascii(name, "GROUP_CONCAT")
    {
        Some(GroupColumnAggregateFunction::StringAgg)
    } else {
        None
    }
}

impl ColumnPredicateTerm {
    fn matches(&self, value: &Value) -> bool {
        if matches!(value, Value::Null) || matches!(self.value, Value::Null) {
            return false;
        }

        match self.op {
            BinaryOperator::Eq => value == &self.value,
            BinaryOperator::NotEq => value != &self.value,
            BinaryOperator::Gt => value.compare(&self.value) == Ordering::Greater,
            BinaryOperator::Lt => value.compare(&self.value) == Ordering::Less,
            BinaryOperator::GtEq => value.compare(&self.value) != Ordering::Less,
            BinaryOperator::LtEq => value.compare(&self.value) != Ordering::Greater,
            _ => false,
        }
    }
}

impl ColumnPredicateScanPlan {
    fn scratch_values(predicate: Option<&Self>) -> Vec<Value> {
        Vec::with_capacity(predicate.map_or(0, |predicate| predicate.column_indices.len()))
    }

    fn decode_values(&self, data: &[u8], values: &mut Vec<Value>) -> Result<()> {
        values.clear();
        values.reserve(self.column_indices.len());
        for &column_index in &self.column_indices {
            values.push(
                crate::common::encoding::RowDecoder::decode_column(data, column_index)
                    .map_err(|e| {
                        FusionError::Execution(format!("Data deserialization error: {}", e))
                    })?
                    .unwrap_or(Value::Null),
            );
        }
        Ok(())
    }

    fn value_for_column<'a>(&self, column_index: usize, values: &'a [Value]) -> Option<&'a Value> {
        self.column_indices
            .iter()
            .position(|&idx| idx == column_index)
            .and_then(|slot| values.get(slot))
    }

    fn matches_values(&self, values: &[Value]) -> bool {
        for term in &self.terms {
            let Some(value) = values.get(term.value_slot) else {
                return false;
            };
            if !term.matches(value) {
                return false;
            }
        }

        true
    }
}

struct ColumnAggregateState {
    kind: ColumnAggregateKind,
    sum: f64,
    count: i64,
    is_int: bool,
    min: Option<Value>,
    max: Option<Value>,
    strings: Vec<String>,
}

fn join_string_aggregate_values(values: &[String]) -> String {
    let values_len = values.iter().map(String::len).sum::<usize>();
    let mut joined = String::with_capacity(values_len + values.len().saturating_sub(1));
    if let Some((first, rest)) = values.split_first() {
        joined.push_str(first);
        for value in rest {
            joined.push(',');
            joined.push_str(value);
        }
    }
    joined
}

impl ColumnAggregateState {
    fn new(kind: ColumnAggregateKind) -> Self {
        Self {
            kind,
            sum: 0.0,
            count: 0,
            is_int: true,
            min: None,
            max: None,
            strings: match kind {
                ColumnAggregateKind::StringAgg => Vec::with_capacity(1),
                _ => Vec::new(),
            },
        }
    }

    fn update(&mut self, value: Value) {
        match self.kind {
            ColumnAggregateKind::CountStar => {
                self.count += 1;
            }
            ColumnAggregateKind::CountColumn => {
                if value != Value::Null {
                    self.count += 1;
                }
            }
            ColumnAggregateKind::Sum | ColumnAggregateKind::Avg => match value {
                Value::Integer(value) => {
                    self.sum += value as f64;
                    self.count += 1;
                }
                Value::Float(value) => {
                    self.sum += value;
                    self.count += 1;
                    self.is_int = false;
                }
                Value::Decimal(value) => {
                    if let Ok(value) = value.parse::<f64>() {
                        self.sum += value;
                        self.count += 1;
                        self.is_int = false;
                    }
                }
                _ => {}
            },
            ColumnAggregateKind::Min => {
                if value == Value::Null {
                    return;
                }
                if self
                    .min
                    .as_ref()
                    .is_none_or(|current| value.compare(current) == Ordering::Less)
                {
                    self.min = Some(value);
                }
            }
            ColumnAggregateKind::Max => {
                if value == Value::Null {
                    return;
                }
                if self
                    .max
                    .as_ref()
                    .is_none_or(|current| value.compare(current) == Ordering::Greater)
                {
                    self.max = Some(value);
                }
            }
            ColumnAggregateKind::StringAgg => match value {
                Value::String(value) => self.strings.push(value),
                Value::Integer(value) => self.strings.push(value.to_string()),
                Value::Float(value) => self.strings.push(value.to_string()),
                Value::Decimal(value) => self.strings.push(value),
                Value::Boolean(value) => self.strings.push(value.to_string()),
                Value::Null => {}
                _ => {}
            },
        }
    }

    fn finalize(&self) -> Value {
        match self.kind {
            ColumnAggregateKind::CountStar => Value::Integer(self.count),
            ColumnAggregateKind::CountColumn => Value::Integer(self.count),
            ColumnAggregateKind::Sum => {
                if self.is_int {
                    Value::Integer(self.sum as i64)
                } else {
                    Value::Float(self.sum)
                }
            }
            ColumnAggregateKind::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float(self.sum / self.count as f64)
                }
            }
            ColumnAggregateKind::Min => self.min.clone().unwrap_or(Value::Null),
            ColumnAggregateKind::Max => self.max.clone().unwrap_or(Value::Null),
            ColumnAggregateKind::StringAgg => {
                if self.strings.is_empty() {
                    Value::Null
                } else {
                    Value::String(join_string_aggregate_values(&self.strings))
                }
            }
        }
    }
}

fn column_aggregate_states(plans: &[ColumnAggregateScanPlan]) -> Vec<ColumnAggregateState> {
    let mut states = Vec::with_capacity(plans.len());
    for plan in plans {
        states.push(ColumnAggregateState::new(plan.kind));
    }
    states
}

fn finalize_column_aggregate_states(states: &[ColumnAggregateState]) -> Vec<Value> {
    let mut values = Vec::with_capacity(states.len());
    for state in states {
        values.push(state.finalize());
    }
    values
}

struct ColumnAggregateScanVisitor<'a> {
    plans: &'a [ColumnAggregateScanPlan],
    predicate: Option<&'a ColumnPredicateScanPlan>,
    states: &'a mut [ColumnAggregateState],
    predicate_values: Vec<Value>,
    error: Option<FusionError>,
}

impl ColumnAggregateScanVisitor<'_> {
    fn visit_row(&mut self, data: &[u8]) -> Result<()> {
        Executor::decode_predicate_values(data, self.predicate, &mut self.predicate_values)?;
        if let Some(predicate) = self.predicate {
            if !predicate.matches_values(&self.predicate_values) {
                return Ok(());
            }
        }

        for (state, plan) in self.states.iter_mut().zip(self.plans.iter()) {
            if let Some(column_index) = plan.column_index {
                let value = Executor::decode_column_or_reuse_predicate(
                    data,
                    column_index,
                    self.predicate,
                    &self.predicate_values,
                )?;
                state.update(value);
            } else {
                state.update(Value::Integer(1));
            }
        }

        Ok(())
    }
}

impl ScanVisitor for ColumnAggregateScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        if let Err(error) = self.visit_row(value) {
            self.error = Some(error);
            return false;
        }
        true
    }
}

#[derive(Clone, Copy)]
enum GroupColumnAggregateKind {
    CountStar,
    CountColumn,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg,
}

pub(super) struct GroupColumnAggregateScanPlan {
    kind: GroupColumnAggregateKind,
    column_index: Option<usize>,
    pub(super) output_name: String,
}

struct GroupColumnAggregateState {
    kind: GroupColumnAggregateKind,
    count: i64,
    sum: f64,
    is_int: bool,
    min: Option<Value>,
    max: Option<Value>,
    distinct: HashSet<Value>,
    strings: Vec<String>,
}

fn group_column_aggregate_states(
    plans: &[GroupColumnAggregateScanPlan],
) -> Vec<GroupColumnAggregateState> {
    let mut states = Vec::with_capacity(plans.len());
    for plan in plans {
        states.push(GroupColumnAggregateState::new(plan.kind));
    }
    states
}

struct GroupAggregateScanVisitor<'a> {
    group_column_indices: &'a [usize],
    aggregate_plans: &'a [GroupColumnAggregateScanPlan],
    predicate: Option<&'a ColumnPredicateScanPlan>,
    groups: &'a mut HashMap<Vec<Value>, Vec<GroupColumnAggregateState>>,
    predicate_values: Vec<Value>,
    error: Option<FusionError>,
}

impl GroupAggregateScanVisitor<'_> {
    fn visit_row(&mut self, data: &[u8]) -> Result<()> {
        Executor::decode_predicate_values(data, self.predicate, &mut self.predicate_values)?;
        if let Some(predicate) = self.predicate {
            if !predicate.matches_values(&self.predicate_values) {
                return Ok(());
            }
        }

        let mut group_values = Vec::with_capacity(self.group_column_indices.len());
        for &group_column_index in self.group_column_indices {
            group_values.push(Executor::decode_column_or_reuse_predicate(
                data,
                group_column_index,
                self.predicate,
                &self.predicate_values,
            )?);
        }

        let states = self
            .groups
            .entry(group_values)
            .or_insert_with(|| group_column_aggregate_states(self.aggregate_plans));

        for (state, plan) in states.iter_mut().zip(self.aggregate_plans.iter()) {
            if let Some(column_index) = plan.column_index {
                let value = Executor::decode_column_or_reuse_predicate(
                    data,
                    column_index,
                    self.predicate,
                    &self.predicate_values,
                )?;
                state.update_value(value);
            } else {
                state.update_count_star();
            }
        }

        Ok(())
    }
}

impl ScanVisitor for GroupAggregateScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        match self.visit_row(value) {
            Ok(()) => true,
            Err(err) => {
                self.error = Some(err);
                false
            }
        }
    }
}

struct SingleGroupAggregateScanVisitor<'a> {
    group_column_index: usize,
    aggregate_plans: &'a [GroupColumnAggregateScanPlan],
    predicate: Option<&'a ColumnPredicateScanPlan>,
    groups: &'a mut HashMap<Value, Vec<GroupColumnAggregateState>>,
    predicate_values: Vec<Value>,
    error: Option<FusionError>,
}

impl SingleGroupAggregateScanVisitor<'_> {
    fn visit_row(&mut self, data: &[u8]) -> Result<()> {
        Executor::decode_predicate_values(data, self.predicate, &mut self.predicate_values)?;
        if let Some(predicate) = self.predicate {
            if !predicate.matches_values(&self.predicate_values) {
                return Ok(());
            }
        }

        let group_value = Executor::decode_column_or_reuse_predicate(
            data,
            self.group_column_index,
            self.predicate,
            &self.predicate_values,
        )?;

        let states = self
            .groups
            .entry(group_value)
            .or_insert_with(|| group_column_aggregate_states(self.aggregate_plans));

        for (state, plan) in states.iter_mut().zip(self.aggregate_plans.iter()) {
            if let Some(column_index) = plan.column_index {
                let value = Executor::decode_column_or_reuse_predicate(
                    data,
                    column_index,
                    self.predicate,
                    &self.predicate_values,
                )?;
                state.update_value(value);
            } else {
                state.update_count_star();
            }
        }

        Ok(())
    }
}

impl ScanVisitor for SingleGroupAggregateScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        match self.visit_row(value) {
            Ok(()) => true,
            Err(err) => {
                self.error = Some(err);
                false
            }
        }
    }
}

struct GroupCountScanVisitor<'a> {
    group_column_index: usize,
    predicate: Option<&'a ColumnPredicateScanPlan>,
    counts: &'a mut HashMap<Value, i64>,
    predicate_values: Vec<Value>,
    error: Option<FusionError>,
}

impl GroupCountScanVisitor<'_> {
    fn visit_row(&mut self, data: &[u8]) -> Result<()> {
        Executor::decode_predicate_values(data, self.predicate, &mut self.predicate_values)?;
        if let Some(predicate) = self.predicate {
            if !predicate.matches_values(&self.predicate_values) {
                return Ok(());
            }
        }

        let value = Executor::decode_column_or_reuse_predicate(
            data,
            self.group_column_index,
            self.predicate,
            &self.predicate_values,
        )?;
        *self.counts.entry(value).or_insert(0) += 1;
        Ok(())
    }
}

impl ScanVisitor for GroupCountScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        match self.visit_row(value) {
            Ok(()) => true,
            Err(err) => {
                self.error = Some(err);
                false
            }
        }
    }
}

impl GroupColumnAggregateState {
    fn new(kind: GroupColumnAggregateKind) -> Self {
        Self {
            kind,
            count: 0,
            sum: 0.0,
            is_int: true,
            min: None,
            max: None,
            distinct: match kind {
                GroupColumnAggregateKind::CountDistinct => HashSet::with_capacity(1),
                _ => HashSet::new(),
            },
            strings: match kind {
                GroupColumnAggregateKind::StringAgg => Vec::with_capacity(1),
                _ => Vec::new(),
            },
        }
    }

    fn update_count_star(&mut self) {
        self.count += 1;
    }

    fn update_value(&mut self, value: Value) {
        match self.kind {
            GroupColumnAggregateKind::CountStar => self.update_count_star(),
            GroupColumnAggregateKind::CountColumn => {
                if value != Value::Null {
                    self.count += 1;
                }
            }
            GroupColumnAggregateKind::CountDistinct => {
                if value != Value::Null {
                    self.distinct.insert(value);
                }
            }
            GroupColumnAggregateKind::Sum => match value {
                Value::Integer(value) => {
                    self.sum += value as f64;
                    self.count += 1;
                }
                Value::Float(value) => {
                    self.sum += value;
                    self.count += 1;
                    self.is_int = false;
                }
                Value::Decimal(value) => {
                    if let Ok(value) = value.parse::<f64>() {
                        self.sum += value;
                        self.count += 1;
                        self.is_int = false;
                    }
                }
                _ => {}
            },
            GroupColumnAggregateKind::Avg => match value {
                Value::Integer(value) => {
                    self.sum += value as f64;
                    self.count += 1;
                }
                Value::Float(value) => {
                    self.sum += value;
                    self.count += 1;
                    self.is_int = false;
                }
                Value::Decimal(value) => {
                    if let Ok(value) = value.parse::<f64>() {
                        self.sum += value;
                        self.count += 1;
                        self.is_int = false;
                    }
                }
                _ => {}
            },
            GroupColumnAggregateKind::Min => {
                if value == Value::Null {
                    return;
                }
                if self
                    .min
                    .as_ref()
                    .is_none_or(|current| value.compare(current) == Ordering::Less)
                {
                    self.min = Some(value);
                }
            }
            GroupColumnAggregateKind::Max => {
                if value == Value::Null {
                    return;
                }
                if self
                    .max
                    .as_ref()
                    .is_none_or(|current| value.compare(current) == Ordering::Greater)
                {
                    self.max = Some(value);
                }
            }
            GroupColumnAggregateKind::StringAgg => match value {
                Value::String(value) => self.strings.push(value),
                Value::Integer(value) => self.strings.push(value.to_string()),
                Value::Float(value) => self.strings.push(value.to_string()),
                Value::Decimal(value) => self.strings.push(value),
                Value::Boolean(value) => self.strings.push(value.to_string()),
                Value::Null => {}
                _ => {}
            },
        }
    }

    fn finalize(&self) -> Value {
        match self.kind {
            GroupColumnAggregateKind::CountStar | GroupColumnAggregateKind::CountColumn => {
                Value::Integer(self.count)
            }
            GroupColumnAggregateKind::CountDistinct => Value::Integer(self.distinct.len() as i64),
            GroupColumnAggregateKind::Sum => {
                if self.is_int {
                    Value::Integer(self.sum as i64)
                } else {
                    Value::Float(self.sum)
                }
            }
            GroupColumnAggregateKind::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Float(self.sum / self.count as f64)
                }
            }
            GroupColumnAggregateKind::Min => self.min.clone().unwrap_or(Value::Null),
            GroupColumnAggregateKind::Max => self.max.clone().unwrap_or(Value::Null),
            GroupColumnAggregateKind::StringAgg => {
                if self.strings.is_empty() {
                    Value::Null
                } else {
                    Value::String(join_string_aggregate_values(&self.strings))
                }
            }
        }
    }
}

impl Executor {
    fn decode_predicate_values(
        data: &[u8],
        predicate: Option<&ColumnPredicateScanPlan>,
        values: &mut Vec<Value>,
    ) -> Result<()> {
        if let Some(predicate) = predicate {
            predicate.decode_values(data, values)
        } else {
            values.clear();
            Ok(())
        }
    }

    fn decode_column_or_reuse_predicate(
        data: &[u8],
        column_index: usize,
        predicate: Option<&ColumnPredicateScanPlan>,
        predicate_values: &[Value],
    ) -> Result<Value> {
        if let Some(value) = predicate.and_then(|predicate| {
            predicate
                .value_for_column(column_index, predicate_values)
                .cloned()
        }) {
            return Ok(value);
        }

        crate::common::encoding::RowDecoder::decode_column(data, column_index)
            .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?
            .map_or(Ok(Value::Null), Ok)
    }

    pub(super) fn simple_column_aggregate_projection(
        projection: &[SelectItem],
        schema: &TableSchema,
        allowed_qualifiers: Option<&[String]>,
        allow_non_nullable_count: bool,
    ) -> Option<Vec<ColumnAggregateScanPlan>> {
        let mut plans = Vec::with_capacity(projection.len());

        for item in projection {
            let (expr, output_name) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, format!("{}", expr)),
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
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

            let (kind, column_index) = if column_scan_function_name_eq_ascii(&func.name, "COUNT")
                && matches!(
                    args.args[0],
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                ) {
                (ColumnAggregateKind::CountStar, None)
            } else if column_scan_function_name_eq_ascii(&func.name, "COUNT") {
                let column_index =
                    Self::column_arg_index(&args.args[0], schema, allowed_qualifiers)?;
                if !allow_non_nullable_count && !schema.columns[column_index].is_nullable {
                    return None;
                }
                (ColumnAggregateKind::CountColumn, Some(column_index))
            } else if column_scan_function_name_eq_ascii(&func.name, "SUM") {
                (
                    ColumnAggregateKind::Sum,
                    Some(Self::column_arg_index(
                        &args.args[0],
                        schema,
                        allowed_qualifiers,
                    )?),
                )
            } else if column_scan_function_name_eq_ascii(&func.name, "AVG") {
                (
                    ColumnAggregateKind::Avg,
                    Some(Self::column_arg_index(
                        &args.args[0],
                        schema,
                        allowed_qualifiers,
                    )?),
                )
            } else if column_scan_function_name_eq_ascii(&func.name, "MIN") {
                (
                    ColumnAggregateKind::Min,
                    Some(Self::column_arg_index(
                        &args.args[0],
                        schema,
                        allowed_qualifiers,
                    )?),
                )
            } else if column_scan_function_name_eq_ascii(&func.name, "MAX") {
                (
                    ColumnAggregateKind::Max,
                    Some(Self::column_arg_index(
                        &args.args[0],
                        schema,
                        allowed_qualifiers,
                    )?),
                )
            } else if column_scan_function_name_eq_ascii(&func.name, "STRING_AGG")
                || column_scan_function_name_eq_ascii(&func.name, "GROUP_CONCAT")
            {
                (
                    ColumnAggregateKind::StringAgg,
                    Some(Self::column_arg_index(
                        &args.args[0],
                        schema,
                        allowed_qualifiers,
                    )?),
                )
            } else {
                return None;
            };
            plans.push(ColumnAggregateScanPlan {
                kind,
                column_index,
                output_name,
            });
        }

        if plans.is_empty() {
            None
        } else {
            Some(plans)
        }
    }

    pub(super) async fn simple_column_aggregate_scan(
        &self,
        table_name: &str,
        plans: &[ColumnAggregateScanPlan],
        predicate: Option<&ColumnPredicateScanPlan>,
        schema: Option<&TableSchema>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Value>> {
        if let (Some(predicate), Some(schema)) = (predicate, schema) {
            if let Some(values) = self
                .simple_column_aggregate_index_scan(table_name, plans, predicate, schema, txn)
                .await?
            {
                return Ok(values);
            }
        }

        let mut states = column_aggregate_states(plans);

        let scan_error = {
            let mut visitor = ColumnAggregateScanVisitor {
                plans,
                predicate,
                states: &mut states,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            visitor.error
        };

        if let Some(err) = scan_error {
            return Err(err);
        }

        Ok(finalize_column_aggregate_states(&states))
    }

    async fn simple_column_aggregate_index_scan(
        &self,
        table_name: &str,
        plans: &[ColumnAggregateScanPlan],
        predicate: &ColumnPredicateScanPlan,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Value>>> {
        let mut index_probe: Option<(usize, String, Value)> = None;
        for term in &predicate.terms {
            if term.op != BinaryOperator::Eq {
                continue;
            }
            let Some(&column_index) = predicate.column_indices.get(term.value_slot) else {
                continue;
            };
            let Some(column) = schema.columns.get(column_index) else {
                continue;
            };
            if column.is_primary {
                let value =
                    Self::coerce_value_to_column_type(term.value.clone(), &column.data_type)
                        .unwrap_or(Value::Null);
                index_probe = Some((column_index, column.name.clone(), value));
                break;
            }
            if column.is_indexed && column.index_type == IndexType::BTree {
                let value =
                    Self::coerce_value_to_column_type(term.value.clone(), &column.data_type)
                        .unwrap_or(Value::Null);
                index_probe = Some((column_index, column.name.clone(), value));
                break;
            }
        }

        let Some((column_index, column_name, value)) = index_probe else {
            return Ok(None);
        };

        let mut states = column_aggregate_states(plans);
        let mut visitor = ColumnAggregateScanVisitor {
            plans,
            predicate: Some(predicate),
            states: &mut states,
            predicate_values: ColumnPredicateScanPlan::scratch_values(Some(predicate)),
            error: None,
        };

        let column = &schema.columns[column_index];
        if column.is_primary {
            let Some(row_id) = Self::value_to_primary_row_id(&value) else {
                return Ok(None);
            };
            let data_key = self.routed_data_key_for_row_id(table_name, &row_id);
            if let Some(data) = txn.get(data_key.as_bytes()).await? {
                visitor.visit_row(&data)?;
            }
        } else if let Some(value_key) = self.value_to_index_string(&value) {
            let index_prefix =
                column_scan_index_prefix_for_value(table_name, &column_name, &value_key);
            let entries = txn.scan_prefix(index_prefix.as_bytes(), None).await?;
            for (key, _) in entries {
                let Some(row_id) = Self::row_id_from_key(&key) else {
                    continue;
                };
                let data_key = self.routed_data_key_for_row_id(table_name, row_id);
                if let Some(data) = txn.get(data_key.as_bytes()).await? {
                    visitor.visit_row(&data)?;
                }
            }
        } else {
            return Ok(None);
        }

        Ok(Some(finalize_column_aggregate_states(&states)))
    }

    pub(super) fn simple_column_predicate_scan_plan(
        &self,
        selection: &Expr,
        schema: &TableSchema,
        params: &[Value],
    ) -> Option<ColumnPredicateScanPlan> {
        let predicates = Self::collect_conjunctive_predicates(selection);
        let mut terms = Vec::with_capacity(predicates.len());
        let mut column_indices = Vec::with_capacity(predicates.len());

        for predicate in predicates {
            let Expr::BinaryOp { left, op, right } = predicate else {
                return None;
            };

            let supported_op = matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Gt
                    | BinaryOperator::Lt
                    | BinaryOperator::GtEq
                    | BinaryOperator::LtEq
            );
            if !supported_op {
                return None;
            }

            if let Some(column_index) = Self::order_limit_column_name(&left)
                .and_then(|name| self.resolve_column_index(&name, schema).ok())
            {
                if !Self::simple_column_predicate_value_expr(&right) {
                    return None;
                }
                let value = self.evaluate_value(&right, &[], schema, params).ok()?;
                let value = Self::coerce_value_to_column_type(
                    value,
                    &schema.columns[column_index].data_type,
                )
                .ok()?;
                let value_slot = match column_indices.iter().position(|&idx| idx == column_index) {
                    Some(slot) => slot,
                    None => {
                        column_indices.push(column_index);
                        column_indices.len() - 1
                    }
                };
                terms.push(ColumnPredicateTerm {
                    value_slot,
                    op,
                    value,
                });
                continue;
            }

            if let Some(column_index) = Self::order_limit_column_name(&right)
                .and_then(|name| self.resolve_column_index(&name, schema).ok())
            {
                if !Self::simple_column_predicate_value_expr(&left) {
                    return None;
                }
                let value = self.evaluate_value(&left, &[], schema, params).ok()?;
                let value = Self::coerce_value_to_column_type(
                    value,
                    &schema.columns[column_index].data_type,
                )
                .ok()?;
                let op = match op {
                    BinaryOperator::Eq => BinaryOperator::Eq,
                    BinaryOperator::NotEq => BinaryOperator::NotEq,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    _ => return None,
                };
                let value_slot = match column_indices.iter().position(|&idx| idx == column_index) {
                    Some(slot) => slot,
                    None => {
                        column_indices.push(column_index);
                        column_indices.len() - 1
                    }
                };
                terms.push(ColumnPredicateTerm {
                    value_slot,
                    op,
                    value,
                });
                continue;
            }

            return None;
        }

        if terms.is_empty() {
            None
        } else {
            Some(ColumnPredicateScanPlan {
                terms,
                column_indices,
            })
        }
    }

    fn simple_column_predicate_value_expr(expr: &Expr) -> bool {
        match expr {
            Expr::Value(_) => true,
            Expr::Nested(inner) => Self::simple_column_predicate_value_expr(inner),
            Expr::UnaryOp { expr, .. } => Self::simple_column_predicate_value_expr(expr),
            Expr::BinaryOp { left, right, .. } => {
                Self::simple_column_predicate_value_expr(left)
                    && Self::simple_column_predicate_value_expr(right)
            }
            _ => false,
        }
    }

    pub(super) fn count_distinct_projection<'a>(
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
        if !column_scan_function_name_eq_ascii(&func.name, "COUNT") {
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

    pub(super) async fn count_distinct_column_scan(
        &self,
        table_name: &str,
        column_index: usize,
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<i64> {
        let kv_pairs = self
            .scan_routed_data_prefixes_for_table(table_name, txn, None)
            .await?;
        let mut seen = HashSet::with_capacity(kv_pairs.len().min(4096));
        let mut predicate_values = ColumnPredicateScanPlan::scratch_values(predicate);

        for (_, data) in kv_pairs {
            Self::decode_predicate_values(&data, predicate, &mut predicate_values)?;
            if let Some(predicate) = predicate {
                if !predicate.matches_values(&predicate_values) {
                    continue;
                }
            }

            let value = Self::decode_column_or_reuse_predicate(
                &data,
                column_index,
                predicate,
                &predicate_values,
            )?;
            if value != Value::Null {
                seen.insert(value);
            }
        }

        Ok(seen.len() as i64)
    }

    pub(super) fn single_column_distinct_projection<'a>(
        projection: &'a [SelectItem],
        schema: &TableSchema,
    ) -> Option<(usize, String)> {
        let [item] = projection else {
            return None;
        };

        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&ident.value))?;
                Some((idx, ident.value.clone()))
            }
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                let name =
                    Self::order_limit_column_name(&Expr::CompoundIdentifier(idents.clone()))?;
                let idx = schema
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&name))?;
                Some((idx, schema.columns[idx].name.clone()))
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(ident),
                alias,
            } => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&ident.value))?;
                Some((idx, alias.value.clone()))
            }
            SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(idents),
                alias,
            } => {
                let name =
                    Self::order_limit_column_name(&Expr::CompoundIdentifier(idents.clone()))?;
                let idx = schema
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&name))?;
                Some((idx, alias.value.clone()))
            }
            _ => None,
        }
    }

    pub(super) async fn distinct_column_scan(
        &self,
        table_name: &str,
        column_index: usize,
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        let kv_pairs = self
            .scan_routed_data_prefixes_for_table(table_name, txn, None)
            .await?;
        let distinct_capacity = kv_pairs.len().min(4096);
        let mut seen = HashSet::with_capacity(distinct_capacity);
        let mut rows = Vec::with_capacity(distinct_capacity);
        let mut predicate_values = ColumnPredicateScanPlan::scratch_values(predicate);

        for (_, data) in kv_pairs {
            Self::decode_predicate_values(&data, predicate, &mut predicate_values)?;
            if let Some(predicate) = predicate {
                if !predicate.matches_values(&predicate_values) {
                    continue;
                }
            }

            let value = Self::decode_column_or_reuse_predicate(
                &data,
                column_index,
                predicate,
                &predicate_values,
            )?;
            if seen.insert(value.clone()) {
                rows.push(vec![value]);
            }
        }

        Ok(rows)
    }

    pub(super) fn simple_group_by_count_projection(
        projection: &[SelectItem],
        group_exprs: &[Expr],
        schema: &TableSchema,
    ) -> Option<(usize, String, String)> {
        if projection.len() != 2 || group_exprs.len() != 1 {
            return None;
        }

        let group_column_name = Self::order_limit_column_name(&group_exprs[0])?;
        let group_column_index = schema
            .columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(&group_column_name))?;

        let output_group_name = match &projection[0] {
            SelectItem::UnnamedExpr(expr) if expr == &group_exprs[0] => match expr {
                Expr::Identifier(ident) => ident.value.clone(),
                Expr::CompoundIdentifier(_) => schema.columns[group_column_index].name.clone(),
                _ => return None,
            },
            SelectItem::ExprWithAlias { expr, alias } if expr == &group_exprs[0] => {
                alias.value.clone()
            }
            _ => return None,
        };

        let count_name = match &projection[1] {
            SelectItem::UnnamedExpr(Expr::Function(func)) => {
                if !Self::is_simple_count_star(func) {
                    return None;
                }
                format!("{}", func)
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(func),
                alias,
            } => {
                if !Self::is_simple_count_star(func) {
                    return None;
                }
                alias.value.clone()
            }
            _ => return None,
        };

        Some((group_column_index, output_group_name, count_name))
    }

    fn is_simple_count_star(func: &sqlparser::ast::Function) -> bool {
        if !column_scan_function_name_eq_ascii(&func.name, "COUNT") {
            return false;
        }

        let FunctionArguments::List(args) = &func.args else {
            return false;
        };

        args.duplicate_treatment != Some(DuplicateTreatment::Distinct)
            && args.args.len() == 1
            && matches!(
                args.args[0],
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            )
    }

    pub(super) async fn group_by_count_column_scan(
        &self,
        table_name: &str,
        column_index: usize,
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        let mut counts: HashMap<Value, i64> = HashMap::with_capacity(4096);

        let scan_error = {
            let mut visitor = GroupCountScanVisitor {
                group_column_index: column_index,
                predicate,
                counts: &mut counts,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            visitor.error
        };

        if let Some(err) = scan_error {
            return Err(err);
        }

        let mut rows = Vec::with_capacity(counts.len());
        for (value, count) in counts {
            rows.push(vec![value, Value::Integer(count)]);
        }
        Ok(rows)
    }

    pub(super) fn simple_group_by_column_aggregate_projection(
        projection: &[SelectItem],
        group_exprs: &[Expr],
        schema: &TableSchema,
    ) -> Option<(Vec<usize>, Vec<String>, Vec<GroupColumnAggregateScanPlan>)> {
        if projection.len() <= group_exprs.len() || group_exprs.is_empty() {
            return None;
        }

        let mut group_column_indices = Vec::with_capacity(group_exprs.len());
        let mut output_group_names = Vec::with_capacity(group_exprs.len());

        for (item, group_expr) in projection.iter().take(group_exprs.len()).zip(group_exprs) {
            let group_column_name = Self::order_limit_column_name(group_expr)?;
            let group_column_index = schema
                .columns
                .iter()
                .position(|col| col.name.eq_ignore_ascii_case(&group_column_name))?;

            let output_group_name = match item {
                SelectItem::UnnamedExpr(expr) if expr == group_expr => match expr {
                    Expr::Identifier(ident) => ident.value.clone(),
                    Expr::CompoundIdentifier(_) => schema.columns[group_column_index].name.clone(),
                    _ => return None,
                },
                SelectItem::ExprWithAlias { expr, alias } if expr == group_expr => {
                    alias.value.clone()
                }
                _ => return None,
            };

            group_column_indices.push(group_column_index);
            output_group_names.push(output_group_name);
        }

        let mut aggregate_plans = Vec::with_capacity(projection.len() - group_exprs.len());

        for item in projection.iter().skip(group_exprs.len()) {
            let (func, output_name) = match item {
                SelectItem::UnnamedExpr(Expr::Function(func)) => (func, format!("{}", func)),
                SelectItem::ExprWithAlias {
                    expr: Expr::Function(func),
                    alias,
                } => (func, alias.value.clone()),
                _ => return None,
            };

            let FunctionArguments::List(args) = &func.args else {
                return None;
            };
            if args.args.len() != 1 {
                return None;
            }

            let Some(function) = group_column_aggregate_function_kind(&func.name) else {
                return None;
            };
            let (kind, column_index) = match function {
                GroupColumnAggregateFunction::Count
                    if args.duplicate_treatment == Some(DuplicateTreatment::Distinct)
                        && !matches!(
                            args.args[0],
                            FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                        ) =>
                {
                    (
                        GroupColumnAggregateKind::CountDistinct,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
                GroupColumnAggregateFunction::Count
                    if matches!(
                        args.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    ) =>
                {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (GroupColumnAggregateKind::CountStar, None)
                }
                GroupColumnAggregateFunction::Count => {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (
                        GroupColumnAggregateKind::CountColumn,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
                GroupColumnAggregateFunction::Sum => {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (
                        GroupColumnAggregateKind::Sum,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
                GroupColumnAggregateFunction::Avg => {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (
                        GroupColumnAggregateKind::Avg,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
                GroupColumnAggregateFunction::Min => {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (
                        GroupColumnAggregateKind::Min,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
                GroupColumnAggregateFunction::Max => {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (
                        GroupColumnAggregateKind::Max,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
                GroupColumnAggregateFunction::StringAgg => {
                    if args.duplicate_treatment.is_some() {
                        return None;
                    }
                    (
                        GroupColumnAggregateKind::StringAgg,
                        Some(Self::column_arg_index(&args.args[0], schema, None)?),
                    )
                }
            };

            aggregate_plans.push(GroupColumnAggregateScanPlan {
                kind,
                column_index,
                output_name,
            });
        }

        Some((group_column_indices, output_group_names, aggregate_plans))
    }

    pub(super) async fn group_by_column_aggregate_scan(
        &self,
        table_name: &str,
        group_column_indices: &[usize],
        aggregate_plans: &[GroupColumnAggregateScanPlan],
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        if let [group_column_index] = group_column_indices {
            return self
                .group_by_single_column_aggregate_scan(
                    table_name,
                    *group_column_index,
                    aggregate_plans,
                    predicate,
                    txn,
                )
                .await;
        }

        let mut groups: HashMap<Vec<Value>, Vec<GroupColumnAggregateState>> =
            HashMap::with_capacity(4096);

        let scan_error = {
            let mut visitor = GroupAggregateScanVisitor {
                group_column_indices,
                aggregate_plans,
                predicate,
                groups: &mut groups,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            visitor.error
        };

        if let Some(err) = scan_error {
            return Err(err);
        }

        let mut rows = Vec::with_capacity(groups.len());
        for (group_values, states) in groups {
            let mut row = Vec::with_capacity(group_values.len() + states.len());
            row.extend(group_values);
            row.extend(states.iter().map(GroupColumnAggregateState::finalize));
            rows.push(row);
        }
        Ok(rows)
    }

    async fn group_by_single_column_aggregate_scan(
        &self,
        table_name: &str,
        group_column_index: usize,
        aggregate_plans: &[GroupColumnAggregateScanPlan],
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        let mut groups: HashMap<Value, Vec<GroupColumnAggregateState>> =
            HashMap::with_capacity(4096);

        let scan_error = {
            let mut visitor = SingleGroupAggregateScanVisitor {
                group_column_index,
                aggregate_plans,
                predicate,
                groups: &mut groups,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            visitor.error
        };

        if let Some(err) = scan_error {
            return Err(err);
        }

        let mut rows = Vec::with_capacity(groups.len());
        for (group_value, states) in groups {
            let mut row = Vec::with_capacity(1 + states.len());
            row.push(group_value);
            row.extend(states.iter().map(GroupColumnAggregateState::finalize));
            rows.push(row);
        }
        Ok(rows)
    }

    pub(super) fn apply_simple_group_by_order_limit(
        rows: &mut Vec<Vec<Value>>,
        columns: &[String],
        order_by: Option<&sqlparser::ast::OrderBy>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<()> {
        if let Some(order_by) = order_by {
            let OrderByKind::Expressions(exprs) = &order_by.kind else {
                return Ok(());
            };

            let order_keys = Self::simple_group_by_order_keys(exprs, columns)?;
            if let Some(window) = limit.and_then(|limit| limit.checked_add(offset)) {
                if window > 0 && window < rows.len() {
                    rows.select_nth_unstable_by(window, |left, right| {
                        Self::compare_simple_group_by_rows(left, right, &order_keys)
                    });
                    rows.truncate(window);
                }
            }

            rows.sort_by(|left, right| {
                Self::compare_simple_group_by_rows(left, right, &order_keys)
            });
        }

        if offset > 0 || limit.is_some() {
            Self::trim_rows_in_place(rows, offset, limit);
        }

        Ok(())
    }

    fn trim_rows_in_place(rows: &mut Vec<Vec<Value>>, offset: usize, limit: Option<usize>) {
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

    pub(super) fn simple_order_limit_supported(
        columns: &[String],
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> bool {
        let Some(order_by) = order_by else {
            return true;
        };
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return false;
        };

        Self::simple_group_by_order_keys(exprs, columns).is_ok()
    }

    fn simple_group_by_order_keys(
        exprs: &[sqlparser::ast::OrderByExpr],
        columns: &[String],
    ) -> Result<Vec<(usize, bool)>> {
        let mut order_keys = Vec::with_capacity(exprs.len());
        for order_expr in exprs {
            let index = match &order_expr.expr {
                Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Number(n, _),
                    ..
                }) => n
                    .parse::<usize>()
                    .ok()
                    .and_then(|position| position.checked_sub(1)),
                Expr::Identifier(ident) => columns
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(&ident.value)),
                Expr::Function(func) => {
                    let function_name = func.to_string();
                    Self::simple_group_by_function_order_index(columns, &function_name)
                }
                _ => None,
            }
            .ok_or_else(|| {
                FusionError::Execution(format!(
                    "Unsupported GROUP BY fast-path ORDER BY expression: {}",
                    order_expr.expr
                ))
            })?;

            if index >= columns.len() {
                return Err(FusionError::Execution(format!(
                    "ORDER BY position {} is out of range",
                    index + 1
                )));
            }

            order_keys.push((index, order_expr.options.asc.unwrap_or(true)));
        }
        Ok(order_keys)
    }

    fn simple_group_by_function_order_index(
        columns: &[String],
        function_name: &str,
    ) -> Option<usize> {
        columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case(function_name))
    }

    fn compare_simple_group_by_rows(
        left: &[Value],
        right: &[Value],
        order_keys: &[(usize, bool)],
    ) -> Ordering {
        for (index, asc) in order_keys {
            let ordering = left[*index].compare(&right[*index]);
            if ordering != Ordering::Equal {
                return if *asc { ordering } else { ordering.reverse() };
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    #[test]
    fn column_aggregate_state_preallocates_string_agg_first_value() {
        let state = ColumnAggregateState::new(ColumnAggregateKind::StringAgg);
        assert!(state.strings.capacity() >= 1);
    }

    #[test]
    fn group_column_aggregate_state_preallocates_collecting_first_value() {
        let distinct = GroupColumnAggregateState::new(GroupColumnAggregateKind::CountDistinct);
        assert!(distinct.distinct.capacity() >= 1);

        let strings = GroupColumnAggregateState::new(GroupColumnAggregateKind::StringAgg);
        assert!(strings.strings.capacity() >= 1);
    }

    #[test]
    fn join_string_aggregate_values_preallocates_exact_value() {
        let values = vec!["alice".to_string(), "42".to_string(), "true".to_string()];
        let joined = join_string_aggregate_values(&values);

        assert_eq!(joined, "alice,42,true");
        assert!(joined.capacity() >= joined.len());
    }

    #[test]
    fn column_scan_data_key_for_row_id_preallocates_exact_key() {
        let key = column_scan_data_key_for_row_id("metrics", "00042");

        assert_eq!(key, "data:metrics:00042");
        assert!(key.capacity() >= key.len());
    }

    #[test]
    fn column_scan_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = column_scan_data_prefix_for_table("metrics");

        assert_eq!(prefix, "data:metrics:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn column_scan_index_prefix_for_value_preallocates_exact_prefix() {
        let prefix = column_scan_index_prefix_for_value("metrics", "host_id", "00042");

        assert_eq!(prefix, "index:metrics:host_id:00042:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn column_scan_function_name_eq_ascii_matches_without_display_string() {
        let count = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("Count"))]);
        let name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("String_Agg"))]);
        let qualified = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("pg_catalog")),
            ObjectNamePart::Identifier(Ident::new("string_agg")),
        ]);

        assert!(column_scan_function_name_eq_ascii(&count, "COUNT"));
        assert!(column_scan_function_name_eq_ascii(&name, "STRING_AGG"));
        assert!(!column_scan_function_name_eq_ascii(&name, "COUNT"));
        assert!(!column_scan_function_name_eq_ascii(
            &qualified,
            "STRING_AGG"
        ));
    }

    #[test]
    fn group_column_aggregate_function_kind_matches_without_display_string() {
        let count = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("count"))]);
        let group_concat = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("Group_Concat"))]);
        let qualified = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("pg_catalog")),
            ObjectNamePart::Identifier(Ident::new("count")),
        ]);

        assert_eq!(
            group_column_aggregate_function_kind(&count),
            Some(GroupColumnAggregateFunction::Count)
        );
        assert_eq!(
            group_column_aggregate_function_kind(&group_concat),
            Some(GroupColumnAggregateFunction::StringAgg)
        );
        assert_eq!(group_column_aggregate_function_kind(&qualified), None);
    }

    #[test]
    fn is_simple_count_star_matches_without_display_string() {
        let statement = crate::parser::parse_sql("SELECT Count(*) FROM metrics")
            .expect("COUNT query parses")
            .pop()
            .expect("statement exists");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] else {
            panic!("expected function projection");
        };

        assert!(Executor::is_simple_count_star(func));
    }

    #[test]
    fn simple_group_by_function_order_index_matches_preformatted_name_once() {
        let columns = vec![
            "city".to_string(),
            "COUNT(*)".to_string(),
            "SUM(amount)".to_string(),
        ];

        assert_eq!(
            Executor::simple_group_by_function_order_index(&columns, "count(*)"),
            Some(1)
        );
        assert_eq!(
            Executor::simple_group_by_function_order_index(&columns, "SUM(amount)"),
            Some(2)
        );
        assert_eq!(
            Executor::simple_group_by_function_order_index(&columns, "AVG(amount)"),
            None
        );
    }
}
