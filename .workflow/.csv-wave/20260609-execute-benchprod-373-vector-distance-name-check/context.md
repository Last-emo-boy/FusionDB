# BENCHPROD-373 Vector Distance Name Check Without Uppercase Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` when detecting `VECTOR_DISTANCE` for HNSW scan optimization.

## Scope

- `src/execution/scan/mod.rs`

## Change

- Reused `scan_object_name_eq_ascii` for the `VECTOR_DISTANCE` sort-expression guard.
- Removed `func.name.to_string().to_uppercase() == "VECTOR_DISTANCE"` from the HNSW scan path.
- Extended the focused helper test to cover `ObjectNamePart::Function`.

The `<->` operator path and the `VECTOR_DISTANCE(embedding, value)` argument extraction path keep the same behavior.

## Verification

| Command | Result |
| --- | --- |
| `cargo test scan_object_name_eq_ascii -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_returning_upsert_vector_rbac test_hnsw_order_by_projection -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_returning_upsert_vector_rbac -- --nocapture` | passed: 14/14 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'func\.name\.to_string\(\)\.to_uppercase\(\) == "VECTOR_DISTANCE"' src/execution/scan/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`cargo fmt` applied the project-standard line wrap before the final `cargo fmt --check`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
