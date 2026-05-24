# Plan Context

Goal: continue database core performance iteration without touching `dashboard/`.

Findings:
- `COUNT(primary_key)` already uses the prefix-count aggregate fast path.
- Any schema column marked `NOT NULL` has the same no-NULL counting semantics for unfiltered single-table aggregates.
- `MIN/MAX` must stay limited to primary keys because key-bound extrema are only valid for primary-key data-key order.

Decision:
- Split aggregate argument resolution into general column and primary-key helpers.
- Allow `COUNT(non_nullable_column)` and qualified/alias-qualified variants to use `count_prefix`.
- Keep nullable columns, filtered queries, grouped queries, joins, and `COUNT(DISTINCT ...)` on existing row-evaluation paths.
