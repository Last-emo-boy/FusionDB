# Single-column aggregate decoding plan

Goal: continue database-core performance iteration after TASK-001..004 by reducing row allocation on the primary-key MIN/MAX aggregate fast path.

Scope:
- Include: `src/common/encoding.rs`, `src/execution/query.rs`, focused SQL integration tests.
- Exclude: `dashboard/`.

Findings:
- `Executor::handle_query_inner` already has a no-filter aggregate fast path for `COUNT(*)`, `MIN(pk)`, and `MAX(pk)`.
- `COUNT(*)` now uses `Transaction::count_prefix`.
- `MIN(pk)` and `MAX(pk)` still decoded a sparse row through `RowDecoder::decode_partial`, allocating a full row-shaped `Vec<Value>` with `Null` placeholders.

Plan:
- TASK-005: expose a single-column decoder that reuses row-format offsets and falls back to legacy bincode rows.
- TASK-006: switch the aggregate primary-key MIN/MAX fast path to that single-column decoder and cover it with an integration test.
