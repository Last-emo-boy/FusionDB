# Zero-column scan plan

Goal: continue database-core performance iteration by avoiding row decoding when a query only needs row existence.

Scope:
- Include: `src/execution/scan.rs`, focused SQL integration tests.
- Exclude: `dashboard/`.

Findings:
- `projection_hint` can be `Some(vec![])` for projections that do not reference table columns, such as `SELECT 1 FROM t` and `COUNT(1)`.
- `scan_single_table` collapsed empty projection indices into `None`, which made the scan decode full rows even when no column value was required.
- Several scan paths repeated full-row vs partial-row decode branching.

Plan:
- TASK-009: preserve empty projection indices as a zero-column scan signal.
- TASK-010: centralize projection-aware row decoding and return empty rows for zero-column scans.
