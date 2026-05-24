# TASK-040 Verification

All targeted checks passed with the low-memory Cargo settings used for the ongoing database-core performance iteration.

The regression test fills `row_cache` with a full row, corrupts the stored projected column bytes, then runs a projected primary-key lookup. The query returns the projected value from the cached full row while the existing partial-decode optimization remains covered by `test_primary_key_equality_projection_skips_unused_column_decode`.
