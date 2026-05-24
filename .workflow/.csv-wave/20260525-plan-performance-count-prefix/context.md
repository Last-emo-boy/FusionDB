# Plan Context

Intent: database-core performance iteration for FusionDB using Maestro artifacts.

Selected tasks: optimize storage-level aggregate/range fast paths. `src/execution/query.rs` already routes simple `COUNT(*)` queries to `Transaction::count_prefix` and primary-key `MIN` to `Transaction::first`; the Fusion implementation materialized `scan_range` rows for count/first. MemoryTransaction also materialized complete merged ranges for count, first, last and limited scans.

Boundary: database core only. `dashboard/` is explicitly excluded.

Plan: extract shared visitor-style merge helpers. `scan_range` still returns rows, while `count_prefix`, `first`, and Memory fast paths can short-circuit without building intermediate vectors.
