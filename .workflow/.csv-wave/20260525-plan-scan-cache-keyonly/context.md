# Scan cache and key-only plan

Goal: continue database-core performance iteration by tightening scan-path decoding and cache behavior.

Scope:
- Include: `src/execution/scan.rs`, focused SQL integration tests.
- Exclude: `dashboard/`.

Findings:
- `scan_single_table` already supports projection-aware partial row decoding.
- Small and streamed BTree index scans cached rows after decoding, even when the decoded row was only a sparse projection row with `Null` placeholders.
- Primary-key equality lookup decoded the full row even when projection, selection, and order context only required the primary key.

Plan:
- TASK-007: only cache full rows from index scan fetches; keep partial/key-only rows out of `row_cache`.
- TASK-008: reuse key-derived sparse primary-key rows for PK-only equality queries and deduplicate key-only row construction.
