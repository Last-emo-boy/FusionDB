# BENCHPROD-391 CONCAT Result Buffer Preallocation

## Objective

Avoid starting `CONCAT` evaluation from an empty `String` allocation while preserving argument evaluation and output bytes.

## Scope

- `src/execution/expr/function.rs`

## Change

- Added `concat_result_buffer`.
- Replaced the `CONCAT` branch's `String::new()` with `concat_result_buffer(args.len())`.
- Added a focused helper test for the conservative initial capacity policy.

The change keeps argument evaluation single-pass and in order. `append_concat_value` still controls all visible string, numeric, boolean, null, and fallback text bytes.

## Verification

| Command | Result |
| --- | --- |
| `cargo test concat_result_buffer_preallocates_by_argument_count -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions test_string_functions -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'concat_result_buffer\|let mut result = String::new\(\)' src/execution/expr/function.rs -n` | `CONCAT` uses `concat_result_buffer`; old `String::new` pattern is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
