use crate::catalog::IndexType;
use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::execution::analyze::TableStats;
use crate::storage::{ScanVisitor, Transaction};
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectName, ObjectNamePart, OrderByKind, SelectItem,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::Executor;

const COLUMN_SCAN_BATCH_SIZE: usize = 1024;
const GROUP_BY_COUNT_INDEX_STATS_MIN_ENTRIES: usize = 65_536;

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

#[cfg(test)]
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

struct ColumnScanBatch {
    rows: Vec<Vec<u8>>,
    selected: Vec<usize>,
    predicate_values: Vec<Vec<Value>>,
    predicate_scratch: Vec<Value>,
}

impl ColumnScanBatch {
    fn new(predicate: Option<&ColumnPredicateScanPlan>) -> Self {
        Self {
            rows: Vec::with_capacity(COLUMN_SCAN_BATCH_SIZE),
            selected: Vec::with_capacity(COLUMN_SCAN_BATCH_SIZE),
            predicate_values: Vec::with_capacity(predicate.map_or(0, |_| COLUMN_SCAN_BATCH_SIZE)),
            predicate_scratch: ColumnPredicateScanPlan::scratch_values(predicate),
        }
    }

    fn push(&mut self, data: &[u8]) -> bool {
        self.rows.push(data.to_vec());
        self.rows.len() >= COLUMN_SCAN_BATCH_SIZE
    }

    fn flush_with<F>(
        &mut self,
        predicate: Option<&ColumnPredicateScanPlan>,
        mut apply_matched_row: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[Value]) -> Result<()>,
    {
        if self.rows.is_empty() {
            return Ok(());
        }

        self.selected.clear();
        self.predicate_values.clear();
        if let Some(predicate) = predicate {
            for (row_index, row) in self.rows.iter().enumerate() {
                predicate.decode_values(row, &mut self.predicate_scratch)?;
                if predicate.matches_values(&self.predicate_scratch) {
                    self.selected.push(row_index);
                    self.predicate_values.push(self.predicate_scratch.clone());
                }
            }
        } else {
            self.selected.extend(0..self.rows.len());
        }

        for (selection_slot, &row_index) in self.selected.iter().enumerate() {
            let predicate_values = if predicate.is_some() {
                self.predicate_values[selection_slot].as_slice()
            } else {
                &[]
            };
            apply_matched_row(&self.rows[row_index], predicate_values)?;
        }

        self.rows.clear();
        Ok(())
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

fn apply_column_aggregate_matched_row(
    plans: &[ColumnAggregateScanPlan],
    predicate: Option<&ColumnPredicateScanPlan>,
    states: &mut [ColumnAggregateState],
    predicate_values: &[Value],
    data: &[u8],
) -> Result<()> {
    for (state, plan) in states.iter_mut().zip(plans.iter()) {
        if let Some(column_index) = plan.column_index {
            let value = Executor::decode_column_or_reuse_predicate(
                data,
                column_index,
                predicate,
                predicate_values,
            )?;
            state.update(value);
        } else {
            state.update(Value::Integer(1));
        }
    }

    Ok(())
}

struct ColumnAggregateScanVisitor<'a> {
    plans: &'a [ColumnAggregateScanPlan],
    predicate: Option<&'a ColumnPredicateScanPlan>,
    states: &'a mut [ColumnAggregateState],
    predicate_values: Vec<Value>,
    batch: Option<ColumnScanBatch>,
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

        apply_column_aggregate_matched_row(
            self.plans,
            self.predicate,
            self.states,
            &self.predicate_values,
            data,
        )
    }

    fn flush_batch(&mut self) -> Result<()> {
        if let Some(batch) = self.batch.as_mut() {
            let plans = self.plans;
            let predicate = self.predicate;
            let states = &mut *self.states;
            batch.flush_with(predicate, |data, predicate_values| {
                apply_column_aggregate_matched_row(plans, predicate, states, predicate_values, data)
            })?;
        }

        Ok(())
    }
}

impl ScanVisitor for ColumnAggregateScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        let should_flush = match self.batch.as_mut() {
            Some(batch) => batch.push(value),
            None => false,
        };
        if self.batch.is_some() {
            if should_flush {
                if let Err(error) = self.flush_batch() {
                    self.error = Some(error);
                    return false;
                }
            }
            return true;
        }

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

fn apply_group_aggregate_matched_row(
    group_column_indices: &[usize],
    aggregate_plans: &[GroupColumnAggregateScanPlan],
    predicate: Option<&ColumnPredicateScanPlan>,
    groups: &mut HashMap<Vec<Value>, Vec<GroupColumnAggregateState>>,
    predicate_values: &[Value],
    data: &[u8],
) -> Result<()> {
    let mut group_values = Vec::with_capacity(group_column_indices.len());
    for &group_column_index in group_column_indices {
        group_values.push(Executor::decode_column_or_reuse_predicate(
            data,
            group_column_index,
            predicate,
            predicate_values,
        )?);
    }

    let states = groups
        .entry(group_values)
        .or_insert_with(|| group_column_aggregate_states(aggregate_plans));

    for (state, plan) in states.iter_mut().zip(aggregate_plans.iter()) {
        if let Some(column_index) = plan.column_index {
            let value = Executor::decode_column_or_reuse_predicate(
                data,
                column_index,
                predicate,
                predicate_values,
            )?;
            state.update_value(value);
        } else {
            state.update_count_star();
        }
    }

    Ok(())
}

fn apply_single_group_aggregate_matched_row(
    group_column_index: usize,
    aggregate_plans: &[GroupColumnAggregateScanPlan],
    predicate: Option<&ColumnPredicateScanPlan>,
    groups: &mut HashMap<Value, Vec<GroupColumnAggregateState>>,
    predicate_values: &[Value],
    data: &[u8],
) -> Result<()> {
    let group_value = Executor::decode_column_or_reuse_predicate(
        data,
        group_column_index,
        predicate,
        predicate_values,
    )?;

    let states = groups
        .entry(group_value)
        .or_insert_with(|| group_column_aggregate_states(aggregate_plans));

    for (state, plan) in states.iter_mut().zip(aggregate_plans.iter()) {
        if let Some(column_index) = plan.column_index {
            let value = Executor::decode_column_or_reuse_predicate(
                data,
                column_index,
                predicate,
                predicate_values,
            )?;
            state.update_value(value);
        } else {
            state.update_count_star();
        }
    }

    Ok(())
}

fn apply_group_count_matched_row(
    group_column_index: usize,
    predicate: Option<&ColumnPredicateScanPlan>,
    counts: &mut HashMap<Value, i64>,
    predicate_values: &[Value],
    data: &[u8],
) -> Result<()> {
    let value = Executor::decode_column_or_reuse_predicate(
        data,
        group_column_index,
        predicate,
        predicate_values,
    )?;
    *counts.entry(value).or_insert(0) += 1;
    Ok(())
}

struct GroupAggregateScanVisitor<'a> {
    group_column_indices: &'a [usize],
    aggregate_plans: &'a [GroupColumnAggregateScanPlan],
    predicate: Option<&'a ColumnPredicateScanPlan>,
    groups: &'a mut HashMap<Vec<Value>, Vec<GroupColumnAggregateState>>,
    predicate_values: Vec<Value>,
    batch: Option<ColumnScanBatch>,
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

        apply_group_aggregate_matched_row(
            self.group_column_indices,
            self.aggregate_plans,
            self.predicate,
            self.groups,
            &self.predicate_values,
            data,
        )
    }

    fn flush_batch(&mut self) -> Result<()> {
        if let Some(batch) = self.batch.as_mut() {
            let group_column_indices = self.group_column_indices;
            let aggregate_plans = self.aggregate_plans;
            let predicate = self.predicate;
            let groups = &mut *self.groups;
            batch.flush_with(predicate, |data, predicate_values| {
                apply_group_aggregate_matched_row(
                    group_column_indices,
                    aggregate_plans,
                    predicate,
                    groups,
                    predicate_values,
                    data,
                )
            })?;
        }

        Ok(())
    }
}

impl ScanVisitor for GroupAggregateScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        let should_flush = match self.batch.as_mut() {
            Some(batch) => batch.push(value),
            None => false,
        };
        if self.batch.is_some() {
            if should_flush {
                if let Err(err) = self.flush_batch() {
                    self.error = Some(err);
                    return false;
                }
            }
            return true;
        }

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
    batch: Option<ColumnScanBatch>,
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

        apply_single_group_aggregate_matched_row(
            self.group_column_index,
            self.aggregate_plans,
            self.predicate,
            self.groups,
            &self.predicate_values,
            data,
        )
    }

    fn flush_batch(&mut self) -> Result<()> {
        if let Some(batch) = self.batch.as_mut() {
            let group_column_index = self.group_column_index;
            let aggregate_plans = self.aggregate_plans;
            let predicate = self.predicate;
            let groups = &mut *self.groups;
            batch.flush_with(predicate, |data, predicate_values| {
                apply_single_group_aggregate_matched_row(
                    group_column_index,
                    aggregate_plans,
                    predicate,
                    groups,
                    predicate_values,
                    data,
                )
            })?;
        }

        Ok(())
    }
}

impl ScanVisitor for SingleGroupAggregateScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        let should_flush = match self.batch.as_mut() {
            Some(batch) => batch.push(value),
            None => false,
        };
        if self.batch.is_some() {
            if should_flush {
                if let Err(err) = self.flush_batch() {
                    self.error = Some(err);
                    return false;
                }
            }
            return true;
        }

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
    batch: Option<ColumnScanBatch>,
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

        apply_group_count_matched_row(
            self.group_column_index,
            self.predicate,
            self.counts,
            &self.predicate_values,
            data,
        )
    }

    fn flush_batch(&mut self) -> Result<()> {
        if let Some(batch) = self.batch.as_mut() {
            let group_column_index = self.group_column_index;
            let predicate = self.predicate;
            let counts = &mut *self.counts;
            batch.flush_with(predicate, |data, predicate_values| {
                apply_group_count_matched_row(
                    group_column_index,
                    predicate,
                    counts,
                    predicate_values,
                    data,
                )
            })?;
        }

        Ok(())
    }
}

impl ScanVisitor for GroupCountScanVisitor<'_> {
    fn visit(&mut self, _key: &[u8], value: &[u8]) -> bool {
        let should_flush = match self.batch.as_mut() {
            Some(batch) => batch.push(value),
            None => false,
        };
        if self.batch.is_some() {
            if should_flush {
                if let Err(err) = self.flush_batch() {
                    self.error = Some(err);
                    return false;
                }
            }
            return true;
        }

        match self.visit_row(value) {
            Ok(()) => true,
            Err(err) => {
                self.error = Some(err);
                false
            }
        }
    }
}

struct CountDistinctScanVisitor<'a> {
    column_index: usize,
    predicate: Option<&'a ColumnPredicateScanPlan>,
    seen: &'a mut HashSet<Value>,
    predicate_values: Vec<Value>,
    error: Option<FusionError>,
}

impl CountDistinctScanVisitor<'_> {
    fn visit_row(&mut self, data: &[u8]) -> Result<()> {
        Executor::decode_predicate_values(data, self.predicate, &mut self.predicate_values)?;
        if let Some(predicate) = self.predicate {
            if !predicate.matches_values(&self.predicate_values) {
                return Ok(());
            }
        }

        let value = Executor::decode_column_or_reuse_predicate(
            data,
            self.column_index,
            self.predicate,
            &self.predicate_values,
        )?;
        if value != Value::Null {
            self.seen.insert(value);
        }
        Ok(())
    }
}

impl ScanVisitor for CountDistinctScanVisitor<'_> {
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

struct DistinctColumnScanVisitor<'a> {
    column_index: usize,
    predicate: Option<&'a ColumnPredicateScanPlan>,
    seen: &'a mut HashSet<Value>,
    rows: &'a mut Vec<Vec<Value>>,
    predicate_values: Vec<Value>,
    error: Option<FusionError>,
}

impl DistinctColumnScanVisitor<'_> {
    fn visit_row(&mut self, data: &[u8]) -> Result<()> {
        Executor::decode_predicate_values(data, self.predicate, &mut self.predicate_values)?;
        if let Some(predicate) = self.predicate {
            if !predicate.matches_values(&self.predicate_values) {
                return Ok(());
            }
        }

        let value = Executor::decode_column_or_reuse_predicate(
            data,
            self.column_index,
            self.predicate,
            &self.predicate_values,
        )?;
        if self.seen.insert(value.clone()) {
            self.rows.push(vec![value]);
        }
        Ok(())
    }
}

impl ScanVisitor for DistinctColumnScanVisitor<'_> {
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
    fn column_scan_numeric_data_type(data_type: &str) -> bool {
        Self::is_integer_type_name(data_type)
            || Self::is_float_type_name(data_type)
            || Self::is_decimal_type_name(data_type)
    }

    fn column_aggregate_batch_supported(
        plans: &[ColumnAggregateScanPlan],
        schema: &TableSchema,
    ) -> bool {
        plans.iter().all(|plan| match plan.kind {
            ColumnAggregateKind::CountStar | ColumnAggregateKind::CountColumn => true,
            ColumnAggregateKind::Sum
            | ColumnAggregateKind::Avg
            | ColumnAggregateKind::Min
            | ColumnAggregateKind::Max => plan
                .column_index
                .and_then(|index| schema.columns.get(index))
                .is_some_and(|column| Self::column_scan_numeric_data_type(&column.data_type)),
            ColumnAggregateKind::StringAgg => false,
        })
    }

    fn group_column_aggregate_batch_supported(
        aggregate_plans: &[GroupColumnAggregateScanPlan],
        schema: &TableSchema,
    ) -> bool {
        aggregate_plans.iter().all(|plan| match plan.kind {
            GroupColumnAggregateKind::CountStar | GroupColumnAggregateKind::CountColumn => true,
            GroupColumnAggregateKind::Sum
            | GroupColumnAggregateKind::Avg
            | GroupColumnAggregateKind::Min
            | GroupColumnAggregateKind::Max => plan
                .column_index
                .and_then(|index| schema.columns.get(index))
                .is_some_and(|column| Self::column_scan_numeric_data_type(&column.data_type)),
            GroupColumnAggregateKind::CountDistinct | GroupColumnAggregateKind::StringAgg => false,
        })
    }

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
                batch: schema
                    .filter(|schema| Self::column_aggregate_batch_supported(plans, schema))
                    .map(|_| ColumnScanBatch::new(predicate)),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            if visitor.error.is_none() {
                if let Err(err) = visitor.flush_batch() {
                    visitor.error = Some(err);
                }
            }
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
        if !schema.columns[column_index].is_primary
            && !Self::legacy_delimited_index_row_ids_are_unambiguous(schema)
        {
            return Ok(None);
        }

        let mut states = column_aggregate_states(plans);
        let mut visitor = ColumnAggregateScanVisitor {
            plans,
            predicate: Some(predicate),
            states: &mut states,
            predicate_values: ColumnPredicateScanPlan::scratch_values(Some(predicate)),
            batch: None,
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
            // Cap the candidate set: each candidate costs a random point get
            // (~100-400us), so beyond a few thousand matches the batch
            // full-scan aggregate is strictly faster. A 50%-selectivity
            // predicate on a 200k-row table paid 100k point gets (44s)
            // before this gate (BENCHPROD-471). Declining falls through to
            // simple_column_aggregate_scan's batched full scan.
            const COLUMN_AGGREGATE_INDEX_PROBE_CAP: usize = 4096;
            let entries = self
                .scan_routed_prefixes(
                    self.routed_index_prefixes_for_value(table_name, &column_name, &value_key),
                    txn,
                    Some(COLUMN_AGGREGATE_INDEX_PROBE_CAP + 1),
                )
                .await?;
            if entries.len() > COLUMN_AGGREGATE_INDEX_PROBE_CAP {
                return Ok(None);
            }
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

    pub(crate) fn count_distinct_projection<'a>(
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
        schema: &TableSchema,
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<i64> {
        if predicate.is_none() {
            if let Some(count) = self
                .count_distinct_index_key_scan(table_name, column_index, schema, txn)
                .await?
            {
                return Ok(count);
            }
        }

        let mut seen = HashSet::with_capacity(4096);

        let scan_error = {
            let mut visitor = CountDistinctScanVisitor {
                column_index,
                predicate,
                seen: &mut seen,
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

        Ok(seen.len() as i64)
    }

    fn column_scan_index_value_key_from_prefixed_key<'a>(
        key: &'a [u8],
        prefix: &str,
    ) -> Option<&'a str> {
        let key = std::str::from_utf8(key).ok()?;
        let suffix = key.strip_prefix(prefix)?;
        let (value_key, row_id) = suffix.rsplit_once(':')?;
        if row_id.is_empty() {
            return None;
        }
        Some(value_key)
    }

    fn column_scan_index_count_summary_value_key_from_prefixed_key<'a>(
        key: &'a [u8],
        prefix: &str,
    ) -> Option<&'a str> {
        std::str::from_utf8(key).ok()?.strip_prefix(prefix)
    }

    pub(crate) fn secondary_index_distinct_key_type_supported(data_type: &str) -> bool {
        Self::secondary_index_order_type_supported(data_type)
            || Self::is_text_type_name(data_type)
            || Self::is_decimal_type_name(data_type)
    }

    pub(crate) fn secondary_index_group_key_type_supported(data_type: &str) -> bool {
        Self::secondary_index_distinct_key_type_supported(data_type)
    }

    pub(crate) fn secondary_index_loose_key_type_supported(data_type: &str) -> bool {
        Self::secondary_index_order_type_supported(data_type)
    }

    fn secondary_index_group_value_from_key(value_key: &str, column_type: &str) -> Option<Value> {
        if let Some(value) = Self::secondary_index_value_from_key(value_key, column_type) {
            return Some(value);
        }
        if Self::is_text_type_name(column_type) {
            Some(Value::String(value_key.to_string()))
        } else if Self::is_decimal_type_name(column_type) {
            value_key
                .strip_prefix("dec:")
                .map(|value| Value::Decimal(value.to_string()))
        } else {
            None
        }
    }

    pub(crate) async fn group_by_count_index_key_scan_cost_allowed(
        &self,
        table_name: &str,
        column_index: usize,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<bool> {
        let Some(stats) = self.load_table_stats(table_name, txn).await? else {
            return Ok(true);
        };
        Ok(Self::group_by_count_index_key_scan_stats_allowed(
            &stats,
            column_index,
            schema,
        ))
    }

    fn group_by_count_index_key_scan_stats_allowed(
        stats: &TableStats,
        column_index: usize,
        schema: &TableSchema,
    ) -> bool {
        let Some(column) = schema.columns.get(column_index) else {
            return true;
        };
        let column_name = column.name.as_str();
        let unqualified = column_name.rsplit('.').next().unwrap_or(column_name);
        let Some(column_stats) = stats.columns.iter().find(|stats| {
            stats.name.eq_ignore_ascii_case(column_name)
                || stats.name.eq_ignore_ascii_case(unqualified)
        }) else {
            return true;
        };

        let index_entries = stats.row_count.saturating_sub(column_stats.null_count);
        index_entries >= GROUP_BY_COUNT_INDEX_STATS_MIN_ENTRIES
    }

    fn column_scan_index_prefix_end(prefix: &str) -> Vec<u8> {
        let mut key = prefix.as_bytes().to_vec();
        key.push(0xFF);
        key
    }

    fn column_scan_index_next_value_seek(prefix: &str, value_key: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + value_key.len() + 2);
        key.extend_from_slice(prefix.as_bytes());
        key.extend_from_slice(value_key.as_bytes());
        key.push(b':');
        key.push(0xFF);
        key
    }

    async fn scan_distinct_index_value_keys_loose<F>(
        &self,
        table_name: &str,
        column_name: &str,
        txn: &mut dyn Transaction,
        mut visit_value_key: F,
    ) -> Result<bool>
    where
        F: FnMut(&str) -> bool,
    {
        for prefix in self.routed_index_prefixes_for_column(table_name, column_name) {
            let end = Self::column_scan_index_prefix_end(&prefix);
            let mut seek = prefix.as_bytes().to_vec();
            while seek < end {
                crate::monitor::inc_index_loose_seek();
                let Some((key, _)) = txn.first(&seek, &end).await? else {
                    break;
                };
                let Some(value_key) =
                    Self::column_scan_index_value_key_from_prefixed_key(&key, &prefix)
                else {
                    return Ok(false);
                };
                if !visit_value_key(value_key) {
                    return Ok(false);
                }
                crate::monitor::inc_index_loose_value();

                let next_seek = Self::column_scan_index_next_value_seek(&prefix, value_key);
                if next_seek <= key {
                    return Ok(false);
                }
                crate::monitor::inc_index_loose_run_skip();
                seek = next_seek;
            }
        }
        Ok(true)
    }

    async fn count_distinct_index_key_scan(
        &self,
        table_name: &str,
        column_index: usize,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<Option<i64>> {
        if self.shard_router.is_some()
            || !Self::legacy_delimited_index_row_ids_are_unambiguous(schema)
        {
            return Ok(None);
        }

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

        if Self::secondary_index_loose_key_type_supported(&column.data_type) {
            let mut count = 0i64;
            let success = self
                .scan_distinct_index_value_keys_loose(table_name, &column.name, txn, |_| {
                    count += 1;
                    true
                })
                .await?;
            return if success { Ok(Some(count)) } else { Ok(None) };
        }

        let mut previous_value_key: Option<String> = None;
        let mut count = 0i64;

        for prefix in self.routed_index_prefixes_for_column(table_name, &column.name) {
            let mut malformed_key = false;
            let mut visitor = |key: &[u8], _: &[u8]| -> bool {
                crate::monitor::add_index_key_stream_entry_visits(1);
                let Some(value_key) =
                    Self::column_scan_index_value_key_from_prefixed_key(&key, &prefix)
                else {
                    malformed_key = true;
                    return false;
                };

                if previous_value_key.as_deref() != Some(value_key) {
                    count += 1;
                    previous_value_key = Some(value_key.to_string());
                }
                true
            };
            self.scan_routed_prefixes_for_each(vec![prefix.clone()], txn, None, &mut visitor)
                .await?;
            if malformed_key {
                return Ok(None);
            }
        }

        Ok(Some(count))
    }

    pub(crate) fn single_column_distinct_projection<'a>(
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
        schema: &TableSchema,
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        if predicate.is_none() {
            if let Some(rows) = self
                .distinct_index_key_scan(table_name, column_index, schema, txn)
                .await?
            {
                return Ok(rows);
            }
        }

        let mut seen = HashSet::with_capacity(4096);
        let mut rows = Vec::with_capacity(4096);

        let scan_error = {
            let mut visitor = DistinctColumnScanVisitor {
                column_index,
                predicate,
                seen: &mut seen,
                rows: &mut rows,
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

        Ok(rows)
    }

    async fn distinct_index_key_scan(
        &self,
        table_name: &str,
        column_index: usize,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Vec<Value>>>> {
        if self.shard_router.is_some()
            || !Self::legacy_delimited_index_row_ids_are_unambiguous(schema)
        {
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

        let mut rows = Vec::with_capacity(4096);
        let success = self
            .scan_distinct_index_value_keys_loose(table_name, &column.name, txn, |value_key| {
                let Some(value) =
                    Self::secondary_index_value_from_key(value_key, &column.data_type)
                else {
                    return false;
                };
                rows.push(vec![value]);
                true
            })
            .await?;
        if !success {
            return Ok(None);
        }

        Ok(Some(rows))
    }

    pub(crate) fn simple_group_by_count_projection(
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
        schema: &TableSchema,
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        if predicate.is_none() {
            if let Some(rows) = self
                .group_by_count_index_key_scan(table_name, column_index, schema, txn)
                .await?
            {
                return Ok(rows);
            }
        }

        let mut counts: HashMap<Value, i64> = HashMap::with_capacity(4096);

        let scan_error = {
            let mut visitor = GroupCountScanVisitor {
                group_column_index: column_index,
                predicate,
                counts: &mut counts,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                batch: Some(ColumnScanBatch::new(predicate)),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            if visitor.error.is_none() {
                if let Err(err) = visitor.flush_batch() {
                    visitor.error = Some(err);
                }
            }
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

    async fn group_by_count_summary_index_scan(
        &self,
        table_name: &str,
        column_name: &str,
        column_type: &str,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Vec<Value>>>> {
        let Some((expected_total_entries, expected_group_count)) = self
            .load_index_count_summary_meta(table_name, column_name, txn)
            .await?
        else {
            return Ok(None);
        };

        let prefix = Self::index_count_summary_prefix_for_column(table_name, column_name);
        let mut rows = Vec::with_capacity(4096);
        let mut total_entries = 0i64;
        let mut group_count = 0usize;
        let mut malformed_summary = false;
        let mut visitor = |key: &[u8], value: &[u8]| -> bool {
            crate::monitor::add_index_group_count_summary_entry_visits(1);
            let Some(value_key) =
                Self::column_scan_index_count_summary_value_key_from_prefixed_key(key, &prefix)
            else {
                malformed_summary = true;
                return false;
            };
            let Some(count) = Self::decode_index_count_summary_count(value) else {
                malformed_summary = true;
                return false;
            };
            if count <= 0 {
                malformed_summary = true;
                return false;
            }
            let Some(group_value) =
                Self::secondary_index_group_value_from_key(value_key, column_type)
            else {
                malformed_summary = true;
                return false;
            };
            let Some(new_total_entries) = total_entries.checked_add(count) else {
                malformed_summary = true;
                return false;
            };
            let Some(new_group_count) = group_count.checked_add(1) else {
                malformed_summary = true;
                return false;
            };
            total_entries = new_total_entries;
            group_count = new_group_count;
            rows.push(vec![group_value, Value::Integer(count)]);
            true
        };
        txn.scan_prefix_for_each(prefix.as_bytes(), None, &mut visitor)
            .await?;
        if malformed_summary
            || total_entries != expected_total_entries
            || group_count != expected_group_count
        {
            return Ok(None);
        }
        Ok(Some(rows))
    }

    async fn group_by_count_index_key_scan(
        &self,
        table_name: &str,
        column_index: usize,
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Vec<Value>>>> {
        if self.shard_router.is_some()
            || !Self::legacy_delimited_index_row_ids_are_unambiguous(schema)
        {
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
        if let Some(rows) = self
            .group_by_count_summary_index_scan(table_name, &column.name, &column.data_type, txn)
            .await?
        {
            return Ok(Some(rows));
        }
        if !self
            .group_by_count_index_key_scan_cost_allowed(table_name, column_index, schema, txn)
            .await?
        {
            return Ok(None);
        }

        let mut rows = Vec::with_capacity(4096);
        let mut current_value_key: Option<String> = None;
        let mut current_value: Option<Value> = None;
        let mut current_count = 0i64;

        for prefix in self.routed_index_prefixes_for_column(table_name, &column.name) {
            let mut malformed_key = false;
            let mut visitor = |key: &[u8], _: &[u8]| -> bool {
                crate::monitor::add_index_key_stream_entry_visits(1);
                let Some(value_key) =
                    Self::column_scan_index_value_key_from_prefixed_key(&key, &prefix)
                else {
                    malformed_key = true;
                    return false;
                };

                if current_value_key.as_deref() == Some(value_key) {
                    current_count += 1;
                    return true;
                }

                if let Some(value) = current_value.take() {
                    rows.push(vec![value, Value::Integer(current_count)]);
                }

                let Some(value) =
                    Self::secondary_index_group_value_from_key(value_key, &column.data_type)
                else {
                    malformed_key = true;
                    return false;
                };
                current_value_key = Some(value_key.to_string());
                current_value = Some(value);
                current_count = 1;
                true
            };
            self.scan_routed_prefixes_for_each(vec![prefix.clone()], txn, None, &mut visitor)
                .await?;
            if malformed_key {
                return Ok(None);
            }
        }

        if let Some(value) = current_value {
            rows.push(vec![value, Value::Integer(current_count)]);
        }

        Ok(Some(rows))
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
        schema: &TableSchema,
        txn: &mut dyn Transaction,
    ) -> Result<Vec<Vec<Value>>> {
        if let [group_column_index] = group_column_indices {
            return self
                .group_by_single_column_aggregate_scan(
                    table_name,
                    *group_column_index,
                    aggregate_plans,
                    predicate,
                    schema,
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
                batch: Self::group_column_aggregate_batch_supported(aggregate_plans, schema)
                    .then(|| ColumnScanBatch::new(predicate)),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            if visitor.error.is_none() {
                if let Err(err) = visitor.flush_batch() {
                    visitor.error = Some(err);
                }
            }
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
        schema: &TableSchema,
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
                batch: Self::group_column_aggregate_batch_supported(aggregate_plans, schema)
                    .then(|| ColumnScanBatch::new(predicate)),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each(table_name, txn, None, &mut visitor)
                .await?;
            if visitor.error.is_none() {
                if let Err(err) = visitor.flush_batch() {
                    visitor.error = Some(err);
                }
            }
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

    pub(crate) fn simple_order_limit_supported(
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
    use crate::catalog::Column;
    use crate::execution::analyze::{ColumnStats, DistinctCountKind, DistinctCountMethod};
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

    fn group_count_stats(row_count: usize, null_count: usize) -> TableStats {
        TableStats {
            table_name: "metrics".to_string(),
            row_count,
            analyzed_rows: row_count,
            sampled: false,
            columns: vec![ColumnStats {
                name: "bucket".to_string(),
                null_count,
                distinct_count: 10,
                distinct_kind: DistinctCountKind::Exact,
                distinct_method: DistinctCountMethod::ExactSet,
                min: Some(Value::Integer(0)),
                max: Some(Value::Integer(9)),
                most_common_values: Vec::new(),
                histogram: Vec::new(),
            }],
            updated_at_epoch_ms: 0,
        }
    }

    fn group_count_schema() -> TableSchema {
        TableSchema::new(
            "metrics".to_string(),
            vec![
                Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                Column {
                    name: "bucket".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: false,
                    is_indexed: true,
                    index_type: IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        )
    }

    #[test]
    fn group_by_count_index_key_scan_stats_gate_uses_index_entries() {
        let schema = group_count_schema();

        assert!(!Executor::group_by_count_index_key_scan_stats_allowed(
            &group_count_stats(GROUP_BY_COUNT_INDEX_STATS_MIN_ENTRIES - 1, 0),
            1,
            &schema
        ));
        assert!(Executor::group_by_count_index_key_scan_stats_allowed(
            &group_count_stats(GROUP_BY_COUNT_INDEX_STATS_MIN_ENTRIES, 0),
            1,
            &schema
        ));
        assert!(!Executor::group_by_count_index_key_scan_stats_allowed(
            &group_count_stats(GROUP_BY_COUNT_INDEX_STATS_MIN_ENTRIES, 1),
            1,
            &schema
        ));
    }

    #[test]
    fn group_by_count_index_key_scan_stats_gate_fails_open_on_missing_stats() {
        let schema = group_count_schema();
        let mut stats = group_count_stats(1, 0);
        stats.columns.clear();

        assert!(Executor::group_by_count_index_key_scan_stats_allowed(
            &stats, 1, &schema
        ));
        assert!(Executor::group_by_count_index_key_scan_stats_allowed(
            &group_count_stats(1, 0),
            99,
            &schema
        ));
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
