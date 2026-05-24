# Zero-column scan execution

Executed TASK-009 and TASK-010.

Changes:
- `src/execution/scan.rs`: added `decode_row_for_projection`.
- `src/execution/scan.rs`: preserved `Some(Vec::new())` projection indices as a zero-column scan signal.
- `src/execution/scan.rs`: prevented empty projections from activating primary-key-only row construction.
- `src/execution/scan.rs`: routed range scan, index scan, streamed index scan, and full scan projection decoding through the shared helper.
- `tests/sql_integration.rs`: added `SELECT 1 FROM nums` and `SELECT COUNT(1) FROM nums` regressions.

Dashboard files were not modified.
