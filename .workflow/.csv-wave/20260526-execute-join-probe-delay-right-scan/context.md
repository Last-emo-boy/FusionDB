# TASK-126 execution

- Target: `src/execution/scan.rs`
- Change: `apply_join_step` now attempts indexed left-driven JOIN probes before scanning all right-table rows.
- Probe path still builds the prefixed right schema so join-key extraction, residual predicates, projection, and LEFT JOIN null extension use the same shape.
- Fallback hash join and nested-loop paths still scan the right table exactly as before.
- Constraint: database core only; no `dashboard/` changes.
