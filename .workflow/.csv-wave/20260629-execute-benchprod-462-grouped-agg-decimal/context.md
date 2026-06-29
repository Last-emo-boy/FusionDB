# BENCHPROD-462 Execution Context

DECIMAL/NUMERIC support in grouped (and scalar) aggregate fan-out. Builds on 461 (which made a
non-numeric aggregate column fail loudly instead of silently local-only): 462 makes DECIMAL/NUMERIC
columns actually FAN OUT correctly instead of loud-erroring.

## Key discovery (scoped the change down from the initial plan)
FusionDB's DECIMAL is **f64-backed**, stored as a normalized string (`Value::decimal_from_f64` →
`{:.12}` → `normalize_decimal`), and serializes to query-result JSON as a **STRING**
(`Value::Decimal => serde_json String`). Empirically (and confirmed in `aggregation.rs`
`AggregateAccumulator::Sum`/`Avg`):
- `SUM`/`AVG` over a DECIMAL column accumulate into f64 and return a JSON **NUMBER** (float).
- `MIN`/`MAX` return the value itself → a JSON **STRING**.
- `COUNT` returns an integer.

So the sum-accumulator path NEVER receives a decimal string (SUM/AVG give numbers; COUNT gives ints).
The initial plan to add a `FanoutSum::Decimal` variant + decimal sum parse/add/to_json + decimal AVG
output was therefore **dead code for this scope and was removed** (extreme-simplicity / no-dead-code).
The ONLY real DECIMAL needs are:
1. **Numeric gate**: let DECIMAL columns pass owner-eligibility — added `is_decimal_type_name` to the
   gate in group_aggregate / group_avg / group_multi_aggregate owners checks (and, for consistency, the
   scalar avg / avg_distinct / min_max ones). (`is_decimal_type_name` matches NUMERIC/DECIMAL/DEC + `(`.)
2. **MIN/MAX numeric comparison**: a decimal value arrives at the extremum path as a JSON string;
   `Value::from_json` would map it to `Value::String` → LEXICAL compare (wrong: "9.50" > "30.25"). New
   `fanout_extremum_value` / `forward_extremum_value` map a finite-decimal JSON string to
   `Value::Decimal` (numeric `compare_decimal_strings`) before comparison; the merge functions and the
   extremum candidate-acceptance sites (so a decimal string isn't rejected as "non-numeric") use it.
   `SUM`/`AVG` are untouched (they get numbers).

## Edits
- src/execution/mod.rs: `|| Self::is_decimal_type_name(...)` in 6 owner-eligibility numeric gates.
- src/server/http_server.rs: `fanout_decimal_f64`, `fanout_extremum_value`; `merge_fanout_extremum`
  compares via `fanout_extremum_value`; 2 candidate-acceptance sites (GroupAggAcc::update used by
  single+multi-agg; scalar `extremum_from_select_results`) accept a decimal string. FanoutSum unchanged.
- src/server/pg_server.rs: exact mirror — `forward_decimal_f64`, `forward_extremum_value`,
  `merge_forward_extremum`, and 3 candidate-acceptance sites (single-agg + multi-agg accumulators +
  scalar `extremum_from_forward_select_results`). ForwardSum unchanged.

## Safety
A genuine TEXT column is still rejected by the numeric gate (461 makes it loud-error), so a non-decimal
string never reaches the extremum path — treating a numeric-looking string as a decimal there is safe.
Int/float MIN/MAX are unchanged (JSON numbers → Value::from_json → Integer/Float → numeric compare).

## Verified
- lib 362 (unit `fanout_extremum_compares_decimal_strings_numerically` — the lexical-vs-numeric trap;
  integration `http_query_fanouts_group_decimal_aggregates_across_shard_owners` — 2-owner DECIMAL(10,2):
  multi-agg SUM/MIN/MAX with group 'a' spanning owners (local 9.50, remote 30.25 → MIN 9.50, MAX 30.25
  by NUMERIC compare; SUM 39.75) + AVG 19.875), pg_integration 38, sql_group_aggregate 50. fmt clean;
  no new clippy. (The pgwire DECIMAL path is a line-by-line mirror of the http path covered by the unit
  test + the existing pgwire 2-node fan-out integration test + the adversarial review.)

## Out of scope / pre-existing
Scalar `SUM(DISTINCT)` over DECIMAL still loud-errors (its gate wasn't part of this change and its
distinct-value sum path would see decimal strings; a separate follow-up could enable it). DECIMAL is
f64-backed (not arbitrary precision) DB-wide — fan-out matches single-node exactly.
