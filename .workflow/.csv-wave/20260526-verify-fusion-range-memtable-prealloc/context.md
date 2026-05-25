# TASK-115 Verification Context

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --lib fusion_count_prefix_matches_scan_prefix_after_overwrite_delete_and_write_buffer -- --nocapture`: passed.
- `cargo test --lib fusion_first_uses_visible_range_with_write_buffer_shadowing -- --nocapture`: passed.
- Coverage includes Fusion visible range merge behavior for `scan_prefix`, `count_prefix`, `first`, and write-buffer shadowing.
