# BENCHPROD-392 SUBSTRING Character Slice Without Intermediate Vec

## Objective

Avoid allocating an intermediate `Vec<char>` while evaluating `SUBSTRING` by character positions.

## Scope

- `src/execution/expr/function.rs`
- `tests/sql_expr_functions.rs`

## Change

- Added `char_boundary_byte_index`.
- Added `substring_by_chars`.
- Replaced `SUBSTRING` evaluation's `s.chars().collect::<Vec<char>>()` path with UTF-8 byte slicing at computed character boundaries.
- Added focused helper coverage for multibyte character boundaries.
- Added SQL regression coverage for ASCII and multibyte `SUBSTRING` calls.

The change keeps argument evaluation single-pass and preserves one-based start, optional length, and empty-string behavior for out-of-range starts.

## Verification

| Command | Result |
| --- | --- |
| `cargo test substring_by_chars_preserves_character_boundaries -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions test_string_functions -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_expr_functions -- --nocapture` | passed: 22/22 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'substring_by_chars\|Vec<char>\|chars\(\)\.collect' src/execution/expr/function.rs -n` | `SUBSTRING` uses `substring_by_chars`; old `Vec<char>` collection path is absent |

Cargo verification uses `.tmp` under the workspace for `TEMP`/`TMP` and sets `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust and test files while exiting successfully.
