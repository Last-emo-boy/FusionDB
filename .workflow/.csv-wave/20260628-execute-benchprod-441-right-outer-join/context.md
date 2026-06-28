# BENCHPROD-441 Execution Context

## Outcome
Correctness fix (workflow-implemented in an isolated worktree, then integrated). RIGHT OUTER JOIN
parsed but executed with INNER semantics — unmatched right rows were dropped instead of NULL-padded.
Scope: RIGHT JOIN only (FULL OUTER left for a later ticket; not regressed).

## Implementation (src/execution/scan/join.rs)
- Previously `is_left_outer` / `supports_left_driven_probe` only handled LEFT/INNER/CROSS; RIGHT/FULL
  ran inner. Added RIGHT-outer handling so unmatched right rows are emitted with the left side
  NULL-padded, preserving output column order. Guarded the join reorder/flatten path so a RIGHT join is
  not silently turned into a comma/inner join.
- Result-preserving for non-RIGHT joins: `is_right_outer` is false for LEFT/INNER/CROSS/None, so all new
  branches are inert for existing join types.

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --test sql_join` (incl. new RIGHT-join NULL-pad tests, equi + non-equi) passed;
  `cargo test --lib` passed.

## Remaining
- FULL OUTER JOIN still executes with inner semantics — separate follow-up ticket.
