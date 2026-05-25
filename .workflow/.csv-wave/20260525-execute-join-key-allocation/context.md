# TASK-060 Join Key Allocation

Scope: `src/execution/scan.rs`

Implemented:
- Replaced iterator `collect` in `row_key` with explicit `Vec::with_capacity`.
- Added direct row key comparison for indexed probe join candidate filtering.
- Avoided allocating two temporary `Vec<Value>` keys per indexed probe candidate comparison.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-join-key-allocation/verification.json`.
