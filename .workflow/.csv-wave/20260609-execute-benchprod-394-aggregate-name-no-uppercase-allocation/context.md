# BENCHPROD-394 Aggregate Function Names Without Uppercase Allocation

## Objective

Avoid allocating uppercase function-name strings while constructing aggregate accumulators.

## Scope

- `src/execution/aggregation.rs`

## Change

- Added `aggregate_function_name_eq_ascii`.
- Replaced `func_name.to_uppercase().as_str()` matching in `AggregateAccumulator::new`.
- Replaced `func_name.to_uppercase().as_str()` matching in `AggregateAccumulator::with_input_capacity`.
- Added focused unit coverage for ASCII case-insensitive aggregate name matching.

Accumulator variants, collecting-accumulator capacity behavior, and fallback-to-count behavior are unchanged.

## Verification

| Command | Result |
| --- | --- |
| `cargo test aggregate_function_name_matching_is_ascii_case_insensitive -- --nocapture` | passed: 1/1 |
| `cargo test test_collecting_accumulators_preallocate_from_input_len -- --nocapture` | passed: 1/1 |
| `cargo test test_collecting_accumulators_preallocate_first_input -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_string_agg -- --nocapture` | passed: 2/2 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'to_uppercase\(\)\|aggregate_function_name_eq_ascii' src/execution/aggregation.rs -n` | aggregate function matching uses `aggregate_function_name_eq_ascii`; `to_uppercase` is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
