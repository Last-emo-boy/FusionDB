# TASK-038 Execution

Changed `src/execution/dml.rs` so `ON CONFLICT DO UPDATE` checks `row_cache` for the existing data key before decoding `existing_bytes`.

Added `test_upsert_do_update_reuses_row_cache_for_existing_row` to prove the update path survives deliberately corrupted backing bytes when the cached row is available, while still invalidating cache after the write.
