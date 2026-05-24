# Execute Context

Execution status: completed.

Changes:
- Primary-key range scan checks `row_cache` for full-row projections before decoding storage bytes.
- Full-row range scan cache misses populate `row_cache`.
- Key-only scans and projection-aware partial decodes remain unchanged.
