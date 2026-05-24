# Execute Context

Execution status: completed.

Changes:
- Primary-key point lookup checks `row_cache` before reading and decoding storage bytes for full-row projections.
- Cache misses populate `row_cache` after full-row decode.
- Partial projections continue using projection-aware decode and do not populate the full-row cache.
