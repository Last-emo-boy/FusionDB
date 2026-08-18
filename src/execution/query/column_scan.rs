use crate::catalog::IndexType;
use crate::catalog::TableSchema;
use crate::common::{FusionError, Result, Value};
use crate::execution::analyze::TableStats;
use crate::storage::fusion::{FusionTransaction, TS_SIZE};
use crate::storage::sstable::{key_user_part, BlockEntrySpan};
use crate::storage::{
    sql_block_zone_map_schema_fingerprint, sql_block_zone_map_scalar, sql_block_zone_map_type_tag,
    SqlBlockZoneMapComparisonOp, SqlBlockZoneMapPredicateKind, SqlBlockZoneMapPredicateTerm,
    SqlBlockZoneMapPruningPlan, ScanVisitor, StorageScanOptions, Transaction,
};
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectName, ObjectNamePart, OrderByKind, SelectItem,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::Executor;

const COLUMN_SCAN_BATCH_SIZE: usize = 1024;
const GROUP_BY_COUNT_INDEX_STATS_MIN_ENTRIES: usize = 65_536;

// Per-thread tally of columnar fast-path fires, used only by tests. The global
// monitor counter is shared across cargo's parallel test threads, so it cannot
// prove a *specific* query fired or declined; `#[tokio::test]` runs each test on
// its own current-thread runtime, so a thread-local delta is race-free.
#[cfg(test)]
thread_local! {
    static COLUMNAR_FAST_PATH_FIRE_LOCAL: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static COLUMN_AGGREGATE_COLUMN_DECODE_LOCAL: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_columnar_fast_path_fire_for_test() {
    COLUMNAR_FAST_PATH_FIRE_LOCAL.with(|cell| cell.set(cell.get() + 1));
}

#[cfg(test)]
fn note_column_aggregate_column_decode_for_test() {
    COLUMN_AGGREGATE_COLUMN_DECODE_LOCAL.with(|cell| cell.set(cell.get() + 1));
}

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

impl ColumnPredicateScanPlan {
    /// Build a storage-level zone-map pruning plan for the clean-block fast
    /// path. Only integer-class columns have zone-map scalars; any term whose
    /// column lacks a type tag is dropped (the per-row predicate still filters
    /// it). Returns `None` when no term can prune a block — the caller then
    /// skips zone-map evaluation entirely.
    fn zone_map_pruning_plan(
        &self,
        table_name: &str,
        schema: &TableSchema,
    ) -> Option<SqlBlockZoneMapPruningPlan> {
        let mut terms = Vec::with_capacity(self.terms.len());
        for term in &self.terms {
            let &column_index = self.column_indices.get(term.value_slot)?;
            let column = schema.columns.get(column_index)?;
            let type_tag = sql_block_zone_map_type_tag(&column.data_type)?;
            // `sql_block_zone_map_scalar` returns `None` for unsupported values
            // (e.g. NULL against an integer column) — skip those terms; the
            // row-level predicate still handles them.
            let scalar = sql_block_zone_map_scalar(&term.value, type_tag)??;
            let op = match term.op {
                BinaryOperator::Eq => SqlBlockZoneMapComparisonOp::Eq,
                BinaryOperator::Lt => SqlBlockZoneMapComparisonOp::Lt,
                BinaryOperator::LtEq => SqlBlockZoneMapComparisonOp::LtEq,
                BinaryOperator::Gt => SqlBlockZoneMapComparisonOp::Gt,
                BinaryOperator::GtEq => SqlBlockZoneMapComparisonOp::GtEq,
                // NotEq cannot prune a min/max block (any block may contain the
                // non-matching value), and zone maps have no NotEq kind.
                BinaryOperator::NotEq => continue,
                _ => return None,
            };
            terms.push(SqlBlockZoneMapPredicateTerm {
                column_index: u32::try_from(column_index).ok()?,
                column_name: column.name.clone(),
                type_tag,
                value_encoding_version: crate::storage::SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION,
                kind: SqlBlockZoneMapPredicateKind::Compare { op, scalar },
            });
        }
        if terms.is_empty() {
            return None;
        }
        Some(SqlBlockZoneMapPruningPlan {
            table_name: table_name.to_string(),
            schema_fingerprint: sql_block_zone_map_schema_fingerprint(schema),
            terms,
        })
    }
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
    predicate_scratch: Vec<Value>,
}

impl ColumnScanBatch {
    fn new(predicate: Option<&ColumnPredicateScanPlan>) -> Self {
        Self {
            rows: Vec::with_capacity(COLUMN_SCAN_BATCH_SIZE),
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

        // Single pass: decode the predicate columns, check the predicate, and
        // fold the row in the same iteration. The decoded predicate values
        // live in `predicate_scratch` for the duration of the synchronous
        // `apply_matched_row` call, so no per-row clone or selection buffer is
        // needed. Late materialization: only predicate columns are decoded
        // here; the aggregate/group columns decode inside `apply_matched_row`.
        if let Some(predicate) = predicate {
            for row in self.rows.iter() {
                predicate.decode_values(row, &mut self.predicate_scratch)?;
                if predicate.matches_values(&self.predicate_scratch) {
                    apply_matched_row(row, &self.predicate_scratch)?;
                }
            }
        } else {
            for row in self.rows.iter() {
                apply_matched_row(row, &[])?;
            }
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

#[derive(Clone, Copy)]
enum ColumnAggregateValueSource {
    Predicate(usize),
    Decoded(usize),
}

struct ColumnAggregateDecodePlan {
    sources: Vec<Option<ColumnAggregateValueSource>>,
    decoded_columns: Vec<usize>,
    decoded_last_consumers: Vec<usize>,
    has_decoded_reuse: bool,
}

impl ColumnAggregateDecodePlan {
    fn new(plans: &[ColumnAggregateScanPlan], predicate: Option<&ColumnPredicateScanPlan>) -> Self {
        let mut sources = Vec::with_capacity(plans.len());
        let mut decoded_columns = Vec::new();

        for plan in plans {
            let Some(column_index) = plan.column_index else {
                sources.push(None);
                continue;
            };

            if let Some(predicate_slot) = predicate.and_then(|predicate| {
                predicate
                    .column_indices
                    .iter()
                    .position(|&index| index == column_index)
            }) {
                sources.push(Some(ColumnAggregateValueSource::Predicate(predicate_slot)));
                continue;
            }

            let decoded_slot = if let Some(slot) = decoded_columns
                .iter()
                .position(|&index| index == column_index)
            {
                slot
            } else {
                let slot = decoded_columns.len();
                decoded_columns.push(column_index);
                slot
            };
            sources.push(Some(ColumnAggregateValueSource::Decoded(decoded_slot)));
        }

        let mut decoded_last_consumers = vec![0; decoded_columns.len()];
        for (plan_index, source) in sources.iter().enumerate() {
            if let Some(ColumnAggregateValueSource::Decoded(slot)) = source {
                decoded_last_consumers[*slot] = plan_index;
            }
        }
        let decoded_source_count = sources
            .iter()
            .filter(|source| matches!(source, Some(ColumnAggregateValueSource::Decoded(_))))
            .count();
        let has_decoded_reuse = decoded_source_count > decoded_last_consumers.len();

        Self {
            sources,
            decoded_columns,
            decoded_last_consumers,
            has_decoded_reuse,
        }
    }

    fn decode(&self, data: &[u8], decoded_values: &mut Vec<Value>) -> Result<()> {
        decoded_values.clear();
        decoded_values.reserve(self.decoded_columns.len());

        for &column_index in &self.decoded_columns {
            decoded_values.push(self.decode_one(data, column_index)?);
        }

        Ok(())
    }

    fn decode_one(&self, data: &[u8], column_index: usize) -> Result<Value> {
        let value = crate::common::encoding::RowDecoder::decode_column(data, column_index)
            .map_err(|e| FusionError::Execution(format!("Data deserialization error: {}", e)))?
            .unwrap_or(Value::Null);
        #[cfg(test)]
        note_column_aggregate_column_decode_for_test();
        Ok(value)
    }

    fn scratch_capacity(&self) -> usize {
        if self.has_decoded_reuse && self.decoded_columns.len() > 1 {
            self.decoded_columns.len()
        } else {
            0
        }
    }

    fn value<'a>(
        source: ColumnAggregateValueSource,
        predicate_values: &'a [Value],
        decoded_values: &'a [Value],
    ) -> &'a Value {
        match source {
            ColumnAggregateValueSource::Predicate(slot) => &predicate_values[slot],
            ColumnAggregateValueSource::Decoded(slot) => &decoded_values[slot],
        }
    }
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

    fn update_count_star(&mut self) {
        debug_assert!(matches!(self.kind, ColumnAggregateKind::CountStar));
        self.count += 1;
    }

    fn update_ref(&mut self, value: &Value) {
        match self.kind {
            ColumnAggregateKind::CountStar => self.update_count_star(),
            ColumnAggregateKind::CountColumn => {
                if !matches!(value, Value::Null) {
                    self.count += 1;
                }
            }
            ColumnAggregateKind::Sum | ColumnAggregateKind::Avg => match value {
                Value::Integer(value) => {
                    self.sum += *value as f64;
                    self.count += 1;
                }
                Value::Float(value) => {
                    self.sum += *value;
                    self.count += 1;
                    self.is_int = false;
                }
                Value::Decimal(value) => {
                    if let Ok(parsed) = value.parse::<f64>() {
                        self.sum += parsed;
                        self.count += 1;
                        self.is_int = false;
                    }
                }
                _ => {}
            },
            ColumnAggregateKind::Min => {
                if matches!(value, Value::Null) {
                    return;
                }
                if self
                    .min
                    .as_ref()
                    .is_none_or(|current| value.compare(current) == Ordering::Less)
                {
                    self.min = Some(value.clone());
                }
            }
            ColumnAggregateKind::Max => {
                if matches!(value, Value::Null) {
                    return;
                }
                if self
                    .max
                    .as_ref()
                    .is_none_or(|current| value.compare(current) == Ordering::Greater)
                {
                    self.max = Some(value.clone());
                }
            }
            ColumnAggregateKind::StringAgg => match value {
                Value::String(value) => self.strings.push(value.clone()),
                Value::Integer(value) => self.strings.push(value.to_string()),
                Value::Float(value) => self.strings.push(value.to_string()),
                Value::Decimal(value) => self.strings.push(value.clone()),
                Value::Boolean(value) => self.strings.push(value.to_string()),
                Value::Null => {}
                _ => {}
            },
        }
    }

    fn update_owned(&mut self, value: Value) {
        match self.kind {
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
            _ => self.update_ref(&value),
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
    decode_plan: &ColumnAggregateDecodePlan,
    states: &mut [ColumnAggregateState],
    predicate_values: &[Value],
    decoded_values: &mut Vec<Value>,
    data: &[u8],
) -> Result<()> {
    // If no decoded column is shared by multiple plans, preserve the original
    // owned per-plan fold. This keeps singleton and multi-column aggregates
    // free of scratch-container bookkeeping; only actual reuse pays for the
    // row-local decode cache below.
    if !decode_plan.has_decoded_reuse {
        for (state, source) in states.iter_mut().zip(decode_plan.sources.iter()) {
            match source {
                Some(ColumnAggregateValueSource::Decoded(slot)) => {
                    let value = decode_plan.decode_one(data, decode_plan.decoded_columns[*slot])?;
                    state.update_owned(value);
                }
                Some(ColumnAggregateValueSource::Predicate(slot)) => {
                    state.update_ref(&predicate_values[*slot]);
                }
                None => state.update_count_star(),
            }
        }
        return Ok(());
    }

    // A single decoded column is common for COUNT/SUM/AVG/MIN/MAX. Keep the
    // value in a local so this path avoids a per-row Vec clear/push and lets
    // the final consumer take ownership (notably for STRING_AGG and MIN/MAX).
    if decode_plan.decoded_columns.len() == 1 {
        let mut decoded_value = decode_plan.decode_one(data, decode_plan.decoded_columns[0])?;
        for (plan_index, (state, source)) in states
            .iter_mut()
            .zip(decode_plan.sources.iter())
            .enumerate()
        {
            match source {
                Some(ColumnAggregateValueSource::Decoded(slot)) => {
                    debug_assert_eq!(*slot, 0);
                    if decode_plan.decoded_last_consumers[*slot] == plan_index {
                        state.update_owned(std::mem::replace(&mut decoded_value, Value::Null));
                    } else {
                        state.update_ref(&decoded_value);
                    }
                }
                Some(ColumnAggregateValueSource::Predicate(slot)) => {
                    state.update_ref(&predicate_values[*slot]);
                }
                None => state.update_count_star(),
            }
        }
        return Ok(());
    }

    decode_plan.decode(data, decoded_values)?;

    for (plan_index, (state, source)) in states
        .iter_mut()
        .zip(decode_plan.sources.iter())
        .enumerate()
    {
        if let Some(source) = source {
            match source {
                ColumnAggregateValueSource::Decoded(slot)
                    if decode_plan.decoded_last_consumers[*slot] == plan_index =>
                {
                    let value = std::mem::replace(&mut decoded_values[*slot], Value::Null);
                    state.update_owned(value);
                }
                _ => {
                    let value =
                        ColumnAggregateDecodePlan::value(*source, predicate_values, decoded_values);
                    state.update_ref(value);
                }
            }
        } else {
            state.update_count_star();
        }
    }

    Ok(())
}

struct ColumnAggregateScanVisitor<'a> {
    decode_plan: ColumnAggregateDecodePlan,
    states: &'a mut [ColumnAggregateState],
    predicate: Option<&'a ColumnPredicateScanPlan>,
    predicate_values: Vec<Value>,
    decoded_values: Vec<Value>,
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
            &self.decode_plan,
            self.states,
            &self.predicate_values,
            &mut self.decoded_values,
            data,
        )
    }
}

impl ScanVisitor for ColumnAggregateScanVisitor<'_> {
    // Bare aggregates decode the referenced columns straight off the borrowed
    // entry and fold into the accumulator: the scan hands out zero-copy views,
    // so a copy-and-defer batch (`ColumnScanBatch`) only added a full-row
    // `to_vec` plus batch bookkeeping per row on top of the same per-row
    // decode work at flush time.
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

        // 472 T1: block-level columnar fast path for single-source aggregates.
        // A pure pre-check that either folds the whole range and returns, or
        // returns `None` so the untouched merge path below runs verbatim (zero
        // risk). Supports bare (predicate-free) and simple value-predicate
        // aggregates: the sink decodes the predicate column(s) and skips
        // non-matching rows before folding.
        if let Some(values) = self
            .try_columnar_single_source_aggregate(table_name, plans, predicate, txn)
            .await?
        {
            return Ok(values);
        }

        let mut states = column_aggregate_states(plans);

        // Build a storage-level zone-map pruning plan so the merge scan skips
        // blocks whose [min,max] cannot satisfy the predicate — block-level
        // late materialization on the fallback (multi-SSTable / memtable
        // overlap) path, complementing the clean-block fast path above.
        let scan_options =
            if let (Some(predicate), Some(schema)) = (predicate, schema) {
                match predicate.zone_map_pruning_plan(table_name, schema) {
                    Some(plan) => {
                        StorageScanOptions::fill_cache().with_sql_block_zone_map_pruning_plan(Arc::new(plan))
                    }
                    None => StorageScanOptions::fill_cache(),
                }
            } else {
                StorageScanOptions::fill_cache()
            };

        let scan_error = {
            let decode_plan = ColumnAggregateDecodePlan::new(plans, predicate);
            let decoded_capacity = decode_plan.scratch_capacity();
            let mut visitor = ColumnAggregateScanVisitor {
                decode_plan,
                predicate,
                states: &mut states,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                decoded_values: Vec::with_capacity(decoded_capacity),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each_with_options(
                table_name, txn, None, &mut visitor, scan_options,
            )
            .await?;
            visitor.error
        };

        if let Some(err) = scan_error {
            return Err(err);
        }

        Ok(finalize_column_aggregate_states(&states))
    }

    /// 472 T1 fast path: fold a bare single-source aggregate straight off the
    /// one clean SSTable window that owns the table's routed range, bypassing
    /// the N-way MVCC merge.
    ///
    /// Storage (`scan_single_source_clean_blocks`) discharges every MVCC
    /// obligation (single source, no write-buffer/memtable overlap, visible,
    /// PUT-only, single-version) and hands back only clean ascending windows;
    /// execution owns exactly two things here — the membership guard and the
    /// fold — both bit-identical to the merge path:
    /// - membership: `routed_data_entry_belongs_to_table` with the schema loaded
    ///   the SAME way the fallback `ExactTableDataScanVisitor` loads it
    ///   (`load_schema_for_data_prefix_filter`), so a colon-bearing non-PK
    ///   row-id or a PK key that is not its own routed data key is excluded
    ///   identically. Dropping this guard is the Design-A over-count bug.
    /// - fold: the SAME `apply_column_aggregate_matched_row` /
    ///   `ColumnAggregateState` accumulator, folding the identical value sequence
    ///   in the identical (ascending user-key) order — so float sums accumulate
    ///   bit-for-bit as the merge would (no per-block partial sums that would
    ///   reorder), and NULL/tombstone/DECIMAL/membership all come out the same.
    ///
    /// Returns `Ok(None)` on any decline (fast path disabled, multi-prefix
    /// table, non-Fusion transaction, or any storage guard failure) so the
    /// caller runs the untouched merge path with fresh accumulators.
    async fn try_columnar_single_source_aggregate(
        &self,
        table_name: &str,
        plans: &[ColumnAggregateScanPlan],
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
    ) -> Result<Option<Vec<Value>>> {
        if !self.columnar_single_source_aggregate_enabled() {
            return Ok(None);
        }

        // T1 fires only for a single routed prefix (sharded single-table
        // aggregates route to multiple prefixes and are deferred to T7).
        let prefixes = self.routed_data_prefixes_for_table(table_name);
        if prefixes.len() != 1 {
            return Ok(None);
        }

        // Membership schema comes from the same source as the fallback path so
        // the two are byte-identical (mut borrow of `txn` ends before downcast).
        let schema = self
            .load_schema_for_data_prefix_filter(table_name, txn)
            .await?;

        let Some(fusion) = txn.as_any().downcast_ref::<FusionTransaction>() else {
            return Ok(None);
        };

        let mut states = column_aggregate_states(plans);
        let decode_plan = ColumnAggregateDecodePlan::new(plans, predicate);
        let mut decoded_values = Vec::with_capacity(decode_plan.scratch_capacity());
        let mut predicate_values = ColumnPredicateScanPlan::scratch_values(predicate);
        let zone_map_plan = predicate.and_then(|p| schema.as_ref().and_then(|s| p.zone_map_pruning_plan(table_name, s)));
        let folded = {
            let schema_ref = schema.as_ref();
            let prefixes_ref = &prefixes;
            let states_ref = &mut states;
            let mut sink = |block: &[u8], spans: &[BlockEntrySpan]| -> Result<()> {
                for span in spans {
                    let key = &block[span.key_start()..span.key_end()];
                    let user_key = key_user_part(key, TS_SIZE);
                    let payload = &block[span.value_start() + 1..span.value_end()];
                    if self.routed_data_entry_belongs_to_table(
                        table_name,
                        schema_ref,
                        prefixes_ref,
                        user_key,
                        payload,
                    ) {
                        // Decode predicate column(s) and skip non-matching rows
                        // before folding — late materialization at the block level.
                        Executor::decode_predicate_values(
                            payload,
                            predicate,
                            &mut predicate_values,
                        )?;
                        if let Some(predicate) = predicate {
                            if !predicate.matches_values(&predicate_values) {
                                continue;
                            }
                        }
                        apply_column_aggregate_matched_row(
                            &decode_plan,
                            states_ref,
                            &predicate_values,
                            &mut decoded_values,
                            payload,
                        )?;
                    }
                }
                Ok(())
            };
            fusion
                .scan_single_source_clean_blocks(prefixes[0].as_bytes(), zone_map_plan.as_ref(), &mut sink)
                .await?
        };

        match folded {
            Some(()) => {
                crate::monitor::inc_columnar_single_source_aggregate_fast_path();
                #[cfg(test)]
                note_columnar_fast_path_fire_for_test();
                Ok(Some(finalize_column_aggregate_states(&states)))
            }
            None => Ok(None),
        }
    }

    /// 472 T2: columnar single-source fast path for single-column GROUP BY.
    /// Same contract as `try_columnar_single_source_aggregate` but folds into
    /// a `HashMap<Value, Vec<GroupColumnAggregateState>>` instead of flat
    /// accumulators. Returns `Ok(Some(true))` if the fast path fired (groups
    /// are populated), `Ok(Some(false))` if it declined (groups untouched), or
    /// `Ok(None)` if the fast path is disabled entirely.
    async fn try_group_by_single_column_columnar_fast_path(
        &self,
        table_name: &str,
        group_column_index: usize,
        aggregate_plans: &[GroupColumnAggregateScanPlan],
        predicate: Option<&ColumnPredicateScanPlan>,
        txn: &mut dyn Transaction,
        groups: &mut HashMap<Value, Vec<GroupColumnAggregateState>>,
    ) -> Result<Option<bool>> {
        if !self.columnar_single_source_aggregate_enabled() {
            return Ok(None);
        }

        let prefixes = self.routed_data_prefixes_for_table(table_name);
        if prefixes.len() != 1 {
            return Ok(Some(false));
        }

        let schema = self
            .load_schema_for_data_prefix_filter(table_name, txn)
            .await?;

        let Some(fusion) = txn.as_any().downcast_ref::<FusionTransaction>() else {
            return Ok(Some(false));
        };

        let mut predicate_values = ColumnPredicateScanPlan::scratch_values(predicate);
        let zone_map_plan = predicate.and_then(|p| schema.as_ref().and_then(|s| p.zone_map_pruning_plan(table_name, s)));
        let folded = {
            let schema_ref = schema.as_ref();
            let prefixes_ref = &prefixes;
            let groups_ref = groups;
            let mut sink = |block: &[u8], spans: &[BlockEntrySpan]| -> Result<()> {
                for span in spans {
                    let key = &block[span.key_start()..span.key_end()];
                    let user_key = key_user_part(key, TS_SIZE);
                    let payload = &block[span.value_start() + 1..span.value_end()];
                    if self.routed_data_entry_belongs_to_table(
                        table_name,
                        schema_ref,
                        prefixes_ref,
                        user_key,
                        payload,
                    ) {
                        Executor::decode_predicate_values(
                            payload,
                            predicate,
                            &mut predicate_values,
                        )?;
                        if let Some(predicate) = predicate {
                            if !predicate.matches_values(&predicate_values) {
                                continue;
                            }
                        }
                        apply_single_group_aggregate_matched_row(
                            group_column_index,
                            aggregate_plans,
                            predicate,
                            groups_ref,
                            &predicate_values,
                            payload,
                        )?;
                    }
                }
                Ok(())
            };
            fusion
                .scan_single_source_clean_blocks(prefixes[0].as_bytes(), zone_map_plan.as_ref(), &mut sink)
                .await?
        };

        Ok(Some(folded.is_some()))
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
        let decode_plan = ColumnAggregateDecodePlan::new(plans, Some(predicate));
        let decoded_capacity = decode_plan.scratch_capacity();
        let mut visitor = ColumnAggregateScanVisitor {
            decode_plan,
            predicate: Some(predicate),
            states: &mut states,
            predicate_values: ColumnPredicateScanPlan::scratch_values(Some(predicate)),
            decoded_values: Vec::with_capacity(decoded_capacity),
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

        let scan_options = match predicate.and_then(|p| p.zone_map_pruning_plan(table_name, schema)) {
            Some(plan) => StorageScanOptions::fill_cache()
                .with_sql_block_zone_map_pruning_plan(Arc::new(plan)),
            None => StorageScanOptions::fill_cache(),
        };

        let scan_error = {
            let mut visitor = GroupCountScanVisitor {
                group_column_index: column_index,
                predicate,
                counts: &mut counts,
                predicate_values: ColumnPredicateScanPlan::scratch_values(predicate),
                batch: Some(ColumnScanBatch::new(predicate)),
                error: None,
            };
            self.scan_routed_data_prefixes_for_each_with_options(
                table_name, txn, None, &mut visitor, scan_options,
            )
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

        let scan_options = match predicate.and_then(|p| p.zone_map_pruning_plan(table_name, schema)) {
            Some(plan) => StorageScanOptions::fill_cache()
                .with_sql_block_zone_map_pruning_plan(Arc::new(plan)),
            None => StorageScanOptions::fill_cache(),
        };

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
            self.scan_routed_data_prefixes_for_each_with_options(
                table_name, txn, None, &mut visitor, scan_options,
            )
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

        // 472 T2: columnar single-source fast path for single-column GROUP BY
        // aggregates. Same clean-window contract as the bare-aggregate path:
        // storage proves the merge would yield the identical ascending-user-key
        // sequence of visible-sole-version PUTs, and the sink applies the
        // identical membership + predicate + group-fold. Returns `None` on any
        // decline so the untouched merge path below runs verbatim.
        if let Some(folded) = self
            .try_group_by_single_column_columnar_fast_path(
                table_name,
                group_column_index,
                aggregate_plans,
                predicate,
                txn,
                &mut groups,
            )
            .await?
        {
            if folded {
                crate::monitor::inc_columnar_single_source_aggregate_fast_path();
                #[cfg(test)]
                note_columnar_fast_path_fire_for_test();
                let mut rows = Vec::with_capacity(groups.len());
                for (group_value, states) in groups {
                    let mut row = Vec::with_capacity(1 + states.len());
                    row.push(group_value);
                    row.extend(states.iter().map(GroupColumnAggregateState::finalize));
                    rows.push(row);
                }
                return Ok(rows);
            }
        }

        let scan_options = match predicate.and_then(|p| p.zone_map_pruning_plan(table_name, schema)) {
            Some(plan) => StorageScanOptions::fill_cache()
                .with_sql_block_zone_map_pruning_plan(Arc::new(plan)),
            None => StorageScanOptions::fill_cache(),
        };

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
            self.scan_routed_data_prefixes_for_each_with_options(
                table_name, txn, None, &mut visitor, scan_options,
            )
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
    use crate::common::encoding::RowEncoder;
    use crate::config::StorageConfig;
    use crate::execution::analyze::{ColumnStats, DistinctCountKind, DistinctCountMethod};
    use crate::execution::QueryResult;
    use crate::storage::fusion::FusionStorage;
    use crate::storage::Storage;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
    use std::sync::Arc;

    // ---- 472 T1 columnar single-source aggregate fast-path tests ----

    async fn fusion_executor(name: &str) -> (Executor, FusionStorage, std::path::PathBuf) {
        let data_dir =
            std::env::temp_dir().join(format!("fusiondb_colagg_{}_{}", name, uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
        let executor = Executor::new(storage);
        (executor, fusion, data_dir)
    }

    fn cleanup_dir(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn fast_path_fire_count() -> u64 {
        // Thread-local so it is race-free under cargo's parallel test threads
        // (each `#[tokio::test]` drives its own current-thread runtime).
        super::COLUMNAR_FAST_PATH_FIRE_LOCAL.with(|cell| cell.get())
    }

    fn aggregate_column_decode_count() -> u64 {
        super::COLUMN_AGGREGATE_COLUMN_DECODE_LOCAL.with(|cell| cell.get())
    }

    async fn exec_ok_sql(executor: &Executor, sql: &str) {
        executor.execute_sql(sql).await.unwrap();
    }

    async fn agg_row_fast(executor: &Executor, sql: &str) -> Vec<Value> {
        executor.invalidate_query_result_cache();
        let results = executor.execute_sql(sql).await.unwrap();
        single_agg_row(results)
    }

    async fn agg_row_fallback(executor: &Executor, sql: &str) -> Vec<Value> {
        executor.invalidate_query_result_cache();
        let results = executor
            .execute_sql_with_columnar_single_source_aggregate(sql, false)
            .await
            .unwrap();
        single_agg_row(results)
    }

    fn single_agg_row(results: Vec<QueryResult>) -> Vec<Value> {
        match results.into_iter().next().unwrap() {
            QueryResult::Select { rows, .. } => rows.into_iter().next().unwrap(),
            other => panic!("expected Select, got {other:?}"),
        }
    }

    fn values_bit_equal(a: &[Value], b: &[Value]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| match (x, y) {
                // Lock float accumulation order: identical sums must have
                // identical bit patterns, not merely compare `==`.
                (Value::Float(fx), Value::Float(fy)) => fx.to_bits() == fy.to_bits(),
                _ => x == y,
            })
    }

    // Deterministic splitmix64 so the randomized differential harness is
    // reproducible across runs.
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    const DIFFERENTIAL_AGGREGATES: &[&str] = &[
        "COUNT(*)",
        "COUNT(vn)",
        "SUM(vi)",
        "SUM(vf)",
        "SUM(vd)",
        "AVG(vi)",
        "AVG(vf)",
        "AVG(vd)",
        "MIN(vi)",
        "MAX(vi)",
        "MIN(vs)",
        "MAX(vs)",
        "STRING_AGG(vs)",
    ];

    #[tokio::test]
    async fn columnar_fast_path_differential_matches_merge_across_aggregates() {
        let mut rng = SplitMix64(0x00C0_FFEE_1234_5678);
        for iteration in 0..6u32 {
            let (executor, fusion, data_dir) =
                fusion_executor(&format!("differential_{iteration}")).await;
            let table = format!("diff_{iteration}");
            exec_ok_sql(
                &executor,
                &format!(
                    "CREATE TABLE {table} (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL, \
                     vf FLOAT NOT NULL, vs TEXT, vn INTEGER, vd DECIMAL(20, 4))"
                ),
            )
            .await;

            let row_count = 40 + rng.below(220) as u32;
            let mut insert = format!("INSERT INTO {table} VALUES ");
            for id in 1..=row_count {
                if id > 1 {
                    insert.push(',');
                }
                let vi = rng.below(2_000_000) as i64 - 1_000_000;
                let vf = (rng.next_u64() as f64 / u64::MAX as f64) * 4000.0 - 2000.0;
                let vd_cents = rng.below(50_000_000) as i64 - 25_000_000;
                let vs = format!("s{:08}", rng.below(100_000_000));
                // Sprinkle NULLs so COUNT(col)/aggregate NULL handling is exercised.
                let vn = if rng.below(4) == 0 {
                    "NULL".to_string()
                } else {
                    (rng.below(1_000_000) as i64).to_string()
                };
                let vs_lit = if rng.below(10) == 0 {
                    "NULL".to_string()
                } else {
                    format!("'{vs}'")
                };
                let vd = format!(
                    "CAST('{}.{:04}' AS DECIMAL(20,4))",
                    vd_cents / 10_000,
                    (vd_cents % 10_000).abs()
                );
                insert.push_str(&format!("({id}, {vi}, {vf:.6}, {vs_lit}, {vn}, {vd})"));
            }
            exec_ok_sql(&executor, &insert).await;
            fusion.create_snapshot_now().await.unwrap();

            for aggregate in DIFFERENTIAL_AGGREGATES {
                let sql = format!("SELECT {aggregate} FROM {table}");
                let before = fast_path_fire_count();
                let fast = agg_row_fast(&executor, &sql).await;
                let fired = fast_path_fire_count() > before;
                let fallback = agg_row_fallback(&executor, &sql).await;
                assert!(
                    values_bit_equal(&fast, &fallback),
                    "iteration {iteration} {aggregate}: fast {fast:?} != fallback {fallback:?}"
                );
                // `COUNT(*)` is answered by the earlier routed-prefix count path
                // and never reaches the columnar fast path; every other aggregate
                // in the matrix must fire it on a clean single SSTable.
                if *aggregate != "COUNT(*)" {
                    assert!(
                        fired,
                        "iteration {iteration} {aggregate}: fast path must fire on a clean single SSTable"
                    );
                }
            }

            cleanup_dir(&data_dir);
        }
    }

    /// Predicate-bearing aggregates must also fire the columnar single-source
    /// fast path on a clean single SSTable, and produce bit-identical results to
    /// the merge fallback (including float accumulation order).
    #[tokio::test]
    async fn columnar_fast_path_predicate_matches_merge_across_aggregates() {
        let mut rng = SplitMix64(0x0BAD_CAFE_DEAD_BEEF);
        for iteration in 0..6u32 {
            let (executor, fusion, data_dir) =
                fusion_executor(&format!("pred_diff_{iteration}")).await;
            let table = format!("pd_{iteration}");
            exec_ok_sql(
                &executor,
                &format!(
                    "CREATE TABLE {table} (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL, \
                     vf FLOAT NOT NULL, vs TEXT, vn INTEGER, vd DECIMAL(20, 4))"
                ),
            )
            .await;

            let row_count = 40 + rng.below(220) as u32;
            let mut insert = format!("INSERT INTO {table} VALUES ");
            for id in 1..=row_count {
                if id > 1 {
                    insert.push(',');
                }
                let vi = rng.below(2_000_000) as i64 - 1_000_000;
                let vf = (rng.next_u64() as f64 / u64::MAX as f64) * 4000.0 - 2000.0;
                let vd_cents = rng.below(50_000_000) as i64 - 25_000_000;
                let vs = format!("s{:08}", rng.below(100_000_000));
                let vn = if rng.below(4) == 0 {
                    "NULL".to_string()
                } else {
                    (rng.below(1_000_000) as i64).to_string()
                };
                let vs_lit = if rng.below(10) == 0 {
                    "NULL".to_string()
                } else {
                    format!("'{vs}'")
                };
                let vd = format!(
                    "CAST('{}.{:04}' AS DECIMAL(20,4))",
                    vd_cents / 10_000,
                    (vd_cents % 10_000).abs()
                );
                insert.push_str(&format!("({id}, {vi}, {vf:.6}, {vs_lit}, {vn}, {vd})"));
            }
            exec_ok_sql(&executor, &insert).await;
            fusion.create_snapshot_now().await.unwrap();

            // Predicates that exercise comparison operators on different column types.
            // Only BinaryOp comparisons are supported by ColumnPredicateScanPlan;
            // IS NULL / IS NOT NULL take a different execution path.
            let predicates = [
                "WHERE vi >= 0",
                "WHERE vi < 500000",
                "WHERE vf > 0.0",
                "WHERE vn >= 0",
            ];
            for predicate in predicates {
                for aggregate in DIFFERENTIAL_AGGREGATES {
                    let sql = format!("SELECT {aggregate} FROM {table} {predicate}");
                    let before = fast_path_fire_count();
                    let fast = agg_row_fast(&executor, &sql).await;
                    let fired = fast_path_fire_count() > before;
                    let fallback = agg_row_fallback(&executor, &sql).await;
                    assert!(
                        values_bit_equal(&fast, &fallback),
                        "iteration {iteration} {aggregate} {predicate}: fast {fast:?} != fallback {fallback:?}"
                    );
                    if *aggregate != "COUNT(*)" {
                        assert!(
                            fired,
                            "iteration {iteration} {aggregate} {predicate}: fast path must fire on a clean single SSTable"
                        );
                    }
                }
            }

            cleanup_dir(&data_dir);
        }
    }

    /// Zone-map pruning on the clean-block fast path: when the predicate
    /// column has integer zone maps, blocks whose [min,max] cannot satisfy
    /// the predicate are skipped without being read. This verifies (1) the
    /// result still matches the merge fallback and (2) the skip counter
    /// advances, proving blocks were pruned at the zone-map level.
    #[tokio::test]
    async fn columnar_fast_path_zone_map_skips_non_matching_blocks() {
        use std::sync::atomic::Ordering::Relaxed;

        let (executor, fusion, data_dir) =
            fusion_executor("zone_map_prune").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE zm (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL)",
        )
        .await;

        // Insert rows where `vi` is a monotonic function of `id` so blocks
        // have non-overlapping [min,max] intervals: block N covers
        // roughly [N*band, (N+1)*band). A range predicate high above the
        // earliest blocks lets zone maps skip them.
        let row_count = 2000u32;
        let band = 100i64;
        let mut insert = String::from("INSERT INTO zm VALUES ");
        for id in 1..=row_count {
            if id > 1 {
                insert.push(',');
            }
            // vi grows with id so consecutive blocks have distinct ranges.
            let vi = (id as i64) * band;
            insert.push_str(&format!("({id}, {vi})"));
        }
        exec_ok_sql(&executor, &insert).await;
        fusion.create_snapshot_now().await.unwrap();

        // Predicate selecting only the top band: zone maps for the lower
        // blocks (whose max < 195000) must be skipped.
        let sql = "SELECT SUM(vi) FROM zm WHERE vi >= 195000";

        executor.invalidate_query_result_cache();
        let fast = agg_row_fast(&executor, sql).await;
        let fallback = agg_row_fallback(&executor, sql).await;
        assert!(
            values_bit_equal(&fast, &fallback),
            "zone-map prune: fast {fast:?} != fallback {fallback:?}"
        );

        let skip_after =
            crate::monitor::GLOBAL_METRICS
                .sstable_block_zone_map_filter_skip_count
                .load(Relaxed);
        // Reset and run a predicate that selects a disjoint high band to
        // measure the skip delta from this query alone.
        crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_skip_count
            .store(0, Relaxed);
        executor.invalidate_query_result_cache();
        let _ = executor.execute_sql(sql).await.unwrap();
        let skip_delta =
            crate::monitor::GLOBAL_METRICS
                .sstable_block_zone_map_filter_skip_count
                .load(Relaxed);
        assert!(
            skip_delta > 0,
            "zone-map pruning must skip at least one block on a clean single \
             SSTable with a high-band predicate (skip_after={skip_after}, \
             skip_delta={skip_delta})"
        );

        cleanup_dir(&data_dir);
    }


    /// single-source fast path on a clean single SSTable, and produce
    /// bit-identical rows to the merge fallback.
    #[tokio::test]
    async fn group_by_single_column_columnar_fast_path_matches_merge() {
        let (executor, fusion, data_dir) =
            fusion_executor("group_by_fast_path").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE g (id INTEGER PRIMARY KEY, cat TEXT NOT NULL, \
             vi INTEGER NOT NULL, vf FLOAT NOT NULL, vn INTEGER)",
        )
        .await;

        let mut rng = SplitMix64(0xFEED_FACE_C0DE_1234);
        let row_count = 300u32;
        let mut insert = String::from("INSERT INTO g VALUES ");
        for id in 1..=row_count {
            if id > 1 {
                insert.push(',');
            }
            let cat = format!("cat{}", rng.below(8));
            let vi = rng.below(1_000_000) as i64;
            let vf = (rng.next_u64() as f64 / u64::MAX as f64) * 1000.0;
            let vn = if rng.below(3) == 0 {
                "NULL".to_string()
            } else {
                (rng.below(500) as i64).to_string()
            };
            insert.push_str(&format!("({id}, '{cat}', {vi}, {vf:.6}, {vn})"));
        }
        exec_ok_sql(&executor, &insert).await;
        fusion.create_snapshot_now().await.unwrap();

        // COUNT(*) routes to the dedicated `group_by_count_column_scan` path
        // (which has its own index-key fast path) and is not exercised by the
        // columnar single-source GROUP BY path. The remaining queries cover
        // SUM/AVG/MIN/MAX/COUNT(col) with and without predicates.
        let queries = [
            "SELECT cat, SUM(vi) FROM g GROUP BY cat",
            "SELECT cat, AVG(vf) FROM g GROUP BY cat",
            "SELECT cat, MIN(vi), MAX(vi) FROM g GROUP BY cat",
            "SELECT cat, COUNT(vn), SUM(vn) FROM g GROUP BY cat",
            "SELECT cat, SUM(vi) FROM g WHERE vi >= 500000 GROUP BY cat",
            "SELECT cat, COUNT(vn) FROM g WHERE vf > 500.0 GROUP BY cat",
        ];

        for sql in queries {
            executor.invalidate_query_result_cache();
            let before = fast_path_fire_count();
            let fast = executor.execute_sql(sql).await.unwrap();
            let fired = fast_path_fire_count() > before;
            let fallback = executor
                .execute_sql_with_columnar_single_source_aggregate(sql, false)
                .await
                .unwrap();
            // Sort both result sets for order-independent comparison.
            let mut fast_rows = group_result_rows(fast);
            let mut fallback_rows = group_result_rows(fallback);
            fast_rows.sort_by(|a, b| a.cmp(b));
            fallback_rows.sort_by(|a, b| a.cmp(b));
            assert_eq!(
                fast_rows, fallback_rows,
                "{sql}: fast {fast_rows:?} != fallback {fallback_rows:?}"
            );
            assert!(
                fired,
                "{sql}: fast path must fire on a clean single SSTable"
            );
        }

        cleanup_dir(&data_dir);
    }

    fn group_result_rows(results: Vec<QueryResult>) -> Vec<Vec<String>> {
        match results.into_iter().next().unwrap() {
            QueryResult::Select { rows, .. } => rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|v| match v {
                            Value::Float(f) => format!("{}", f.to_bits()),
                            other => format!("{other}"),
                        })
                        .collect()
                })
                .collect(),
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn repeated_column_aggregates_decode_once_per_row_and_match_fallback() {
        let (executor, fusion, data_dir) = fusion_executor("decode_once").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE d (id INTEGER PRIMARY KEY, v INTEGER)",
        )
        .await;
        exec_ok_sql(
            &executor,
            "INSERT INTO d VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)",
        )
        .await;
        fusion.create_snapshot_now().await.unwrap();

        let sql = "SELECT SUM(v), AVG(v), MIN(v), MAX(v), COUNT(v), COUNT(*) FROM d";
        let fast_path_before = fast_path_fire_count();
        let decode_before = aggregate_column_decode_count();
        let fast = agg_row_fast(&executor, sql).await;

        assert!(
            fast_path_fire_count() > fast_path_before,
            "the clean single-SSTable path must fire"
        );
        assert_eq!(
            aggregate_column_decode_count() - decode_before,
            4,
            "five aggregates over v must decode that column once for each of four rows"
        );
        assert_eq!(
            fast,
            vec![
                Value::Integer(40),
                Value::Float(20.0),
                Value::Integer(10),
                Value::Integer(30),
                Value::Integer(2),
                Value::Integer(4),
            ]
        );

        let fallback_decode_before = aggregate_column_decode_count();
        let fallback = agg_row_fallback(&executor, sql).await;
        assert!(values_bit_equal(&fast, &fallback));
        assert_eq!(
            aggregate_column_decode_count() - fallback_decode_before,
            4,
            "the merge fallback must share the same decode-once fold"
        );

        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn multi_column_aggregate_reuse_matches_fallback_bitwise() {
        let (executor, fusion, data_dir) = fusion_executor("decode_once_multi").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE dm (id INTEGER PRIMARY KEY, vi INTEGER, vf FLOAT)",
        )
        .await;
        exec_ok_sql(
            &executor,
            "INSERT INTO dm VALUES (1, 10, 1.5), (2, NULL, -2.0), (3, 30, 3.25)",
        )
        .await;
        fusion.create_snapshot_now().await.unwrap();

        let sql = "SELECT SUM(vi), AVG(vi), MIN(vf), MAX(vf), COUNT(*) FROM dm";
        let decode_before = aggregate_column_decode_count();
        let fast = agg_row_fast(&executor, sql).await;
        assert_eq!(
            aggregate_column_decode_count() - decode_before,
            6,
            "two unique columns must be decoded once per row in the shared generic path"
        );
        assert_eq!(
            fast,
            vec![
                Value::Integer(40),
                Value::Float(20.0),
                Value::Float(-2.0),
                Value::Float(3.25),
                Value::Integer(3),
            ]
        );

        let fallback = agg_row_fallback(&executor, sql).await;
        assert!(values_bit_equal(&fast, &fallback));
        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn columnar_fast_path_declines_on_multiple_sstables_but_stays_correct() {
        let (executor, fusion, data_dir) = fusion_executor("multi_sstable_e2e").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE m (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL)",
        )
        .await;
        exec_ok_sql(&executor, "INSERT INTO m VALUES (1, 10), (100, 20)").await;
        fusion.create_snapshot_now().await.unwrap();
        // Second overlapping SSTable -> G1 declines, but the merge answer is
        // still correct.
        exec_ok_sql(&executor, "INSERT INTO m VALUES (50, 30)").await;
        fusion.create_snapshot_now().await.unwrap();

        let before = fast_path_fire_count();
        let fast = agg_row_fast(&executor, "SELECT SUM(vi), COUNT(*) FROM m").await;
        assert!(
            fast_path_fire_count() == before,
            "two overlapping SSTables must decline the fast path"
        );
        assert_eq!(fast, vec![Value::Integer(60), Value::Integer(3)]);

        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn columnar_fast_path_declines_on_memtable_overlap_but_stays_correct() {
        let (executor, fusion, data_dir) = fusion_executor("memtable_e2e").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE mt (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL)",
        )
        .await;
        exec_ok_sql(&executor, "INSERT INTO mt VALUES (1, 10), (2, 20), (3, 30)").await;
        fusion.create_snapshot_now().await.unwrap();

        // Clean: the fast path fires.
        let before_clean = fast_path_fire_count();
        let clean = agg_row_fast(&executor, "SELECT SUM(vi) FROM mt").await;
        assert!(fast_path_fire_count() > before_clean);
        assert_eq!(clean, vec![Value::Integer(60)]);

        // A live memtable row -> G3 declines, answer still includes it.
        exec_ok_sql(&executor, "INSERT INTO mt VALUES (4, 40)").await;
        let before_dirty = fast_path_fire_count();
        let dirty = agg_row_fast(&executor, "SELECT SUM(vi) FROM mt").await;
        assert!(
            fast_path_fire_count() == before_dirty,
            "a live memtable row must decline the fast path"
        );
        assert_eq!(dirty, vec![Value::Integer(100)]);

        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn columnar_fast_path_excludes_non_member_rows() {
        let (executor, fusion, data_dir) = fusion_executor("membership").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE mem (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL)",
        )
        .await;
        // Legit rows: SUM(vi) = 10*(1..10) = 550, COUNT(*) = 10.
        let mut insert = String::from("INSERT INTO mem VALUES ");
        for id in 1..=10i64 {
            if id > 1 {
                insert.push(',');
            }
            insert.push_str(&format!("({id}, {})", id * 10));
        }
        exec_ok_sql(&executor, &insert).await;

        // Plant non-member rows straight into the data keyspace:
        //  (a) a colon-bearing key with no decodable PK -> excluded by the
        //      "suffix contains ':'" rule;
        //  (b) a key that is not its own routed data key (PK 777 != key)
        //      -> excluded by the PK-identity rule.
        // Both carry huge `vi` so any leak is obvious.
        {
            let mut txn = fusion.begin_transaction().await.unwrap();
            let colon_key = b"data:mem:5:secondary";
            let colon_val = RowEncoder::encode(&[Value::Null, Value::Integer(100_000)]);
            txn.put(colon_key, &colon_val).await.unwrap();
            let bogus_key = b"data:mem:zzz_bogus";
            let bogus_val = RowEncoder::encode(&[Value::Integer(777), Value::Integer(200_000)]);
            txn.put(bogus_key, &bogus_val).await.unwrap();
            txn.commit().await.unwrap();
        }
        fusion.create_snapshot_now().await.unwrap();

        let before = fast_path_fire_count();
        let fast = agg_row_fast(&executor, "SELECT SUM(vi), COUNT(*) FROM mem").await;
        assert!(
            fast_path_fire_count() > before,
            "membership planting must not stop the fast path from firing"
        );
        assert_eq!(
            fast,
            vec![Value::Integer(550), Value::Integer(10)],
            "planted non-member rows must be excluded (removing the membership guard makes this fail)"
        );
        let fallback = agg_row_fallback(&executor, "SELECT SUM(vi), COUNT(*) FROM mem").await;
        assert_eq!(fast, fallback);

        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn columnar_fast_path_skips_non_fusion_storage() {
        let wal_path = format!("test_colagg_memory_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> =
            Arc::new(crate::storage::memory::MemoryStorage::new(&wal_path).unwrap());
        let executor = Executor::new(storage);
        exec_ok_sql(
            &executor,
            "CREATE TABLE nf (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL)",
        )
        .await;
        exec_ok_sql(&executor, "INSERT INTO nf VALUES (1, 10), (2, 20), (3, 30)").await;

        let before = fast_path_fire_count();
        let row = agg_row_fast(&executor, "SELECT SUM(vi) FROM nf").await;
        assert!(
            fast_path_fire_count() == before,
            "a non-Fusion transaction cannot downcast, so the fast path must not fire"
        );
        assert_eq!(row, vec![Value::Integer(60)]);

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn columnar_fast_path_decode_error_parity_with_fallback() {
        let (executor, fusion, data_dir) = fusion_executor("decode_parity").await;
        exec_ok_sql(
            &executor,
            "CREATE TABLE de (id INTEGER PRIMARY KEY, vi INTEGER NOT NULL)",
        )
        .await;
        exec_ok_sql(&executor, "INSERT INTO de VALUES (1, 10), (2, 20)").await;

        // Plant a member row (routed key == its own PK-derived key) whose `vi`
        // column span is corrupt: membership passes, so both paths reach the
        // same failing decode.
        {
            let mut row = RowEncoder::encode(&[Value::Integer(3), Value::Integer(30)]);
            corrupt_encoded_column(&mut row, 1, 2);
            let key = format!(
                "data:de:{}",
                crate::common::encoding::encode_i64_comparable(3)
            );
            let mut txn = fusion.begin_transaction().await.unwrap();
            txn.put(key.as_bytes(), &row).await.unwrap();
            txn.commit().await.unwrap();
        }
        fusion.create_snapshot_now().await.unwrap();

        executor.invalidate_query_result_cache();
        let fast = executor.execute_sql("SELECT SUM(vi) FROM de").await;
        executor.invalidate_query_result_cache();
        let fallback = executor
            .execute_sql_with_columnar_single_source_aggregate("SELECT SUM(vi) FROM de", false)
            .await;
        assert!(fast.is_err(), "fast path must surface the decode error");
        assert!(fallback.is_err(), "fallback must surface the decode error");
        assert_eq!(
            fast.unwrap_err().to_string(),
            fallback.unwrap_err().to_string(),
            "decode error must be identical across paths"
        );

        cleanup_dir(&data_dir);
    }

    fn corrupt_encoded_column(row: &mut [u8], column_index: usize, column_count: usize) {
        let off_pos = 2 + column_index * 4;
        let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let end = if column_index + 1 < column_count {
            let next_off_pos = off_pos + 4;
            u32::from_le_bytes(row[next_off_pos..next_off_pos + 4].try_into().unwrap()) as usize
        } else {
            row.len()
        };
        for byte in &mut row[start..end] {
            *byte = 0xff;
        }
    }

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
