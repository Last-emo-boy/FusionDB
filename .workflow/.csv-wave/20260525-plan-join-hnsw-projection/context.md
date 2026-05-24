# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- `execute_join` already trims joined rows after each join step, but the first base relation still used full row decode before stage projection.
- HNSW `ORDER BY VECTOR_DISTANCE(...) LIMIT` fetches top-k rows from storage and decoded complete rows even when query projection plus order-by only required a subset.
- `row_cache` stores full rows, so projected HNSW rows must not be inserted into it.

Decision:
- Keep the join optimization limited to the first base relation to avoid changing indexed join probe row-width assumptions.
- Use existing `decode_row_for_projection` for HNSW row fetches and cache only full-row decodes.
