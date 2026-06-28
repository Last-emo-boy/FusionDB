# BENCHPROD-443 Execution Context

## Outcome
Correctness fix completing the outer-join family (after BENCHPROD-441 RIGHT). FULL OUTER JOIN parsed
but executed with INNER semantics — unmatched rows from BOTH sides were dropped.

## Process note
Spun a worktree workflow to draft this, but `isolation: 'worktree'` bases worktrees on the
session-start commit (032e052 / BENCHPROD-435), so the agent never saw the 436-442 commits — its
diff was against a pre-441 join.rs (no RIGHT support) and would not apply on main. Re-implemented
directly on main, reusing 441's RIGHT machinery (the agent's correctness reasoning was a useful
reference). Recorded the worktree-base limitation in project memory.

## Implementation (src/execution/scan/join.rs)
441 already emits unmatched-left (gated `is_left_outer`) and unmatched-right (gated `is_right_outer`)
in separate branches. FULL OUTER = make BOTH fire:
- Added `is_full_outer` flag (sqlparser `JoinOperator::FullOuter`).
- Equi hash-join: force the build-right branch for full; switched its hash buckets to right-row
  indices and added a `right_matched` bitmap; mark a right row matched only after the ON residual
  passes; extended the unmatched-left NULL-pad gate to `is_left_outer || is_full_outer`; added a
  post-pass emitting never-matched right rows NULL-padded on the left.
- Disabled the expr-hash join path (`a.id = f(b)`) for full (it can't emit unmatched-left), so full
  falls through to the nested-loop fallback.
- Nested-loop fallback (covers non-equi e.g. `a.v > b.w`): added a full branch with the same
  right-match tracking + unmatched-right post-pass.
- `supports_left_driven_probe` excludes full, so the indexed-probe path is not taken for full.
- LEFT/RIGHT/INNER/CROSS unchanged (all new behavior gated on `is_full_outer`).

## Correctness argument
- Matched once: each (left,right) pair emitted once in the left-driven loop; a right row is re-emitted
  only if `right_matched` is still false. `right_matched` is set only after the residual passes, so a
  hash-key collision failing the ON condition does not falsely suppress a right-only row.
- Both unmatched sides emitted; output column order (left cols, right cols) asserted by tests.
- Multi-row matches covered (one left key -> 2 right rows). Limit handled (post-pass guarded; execute_join truncates).

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --test sql_join` passed incl. 3 new FULL tests + all LEFT/RIGHT/INNER regression.
- `cargo test --lib` passed.

## Remaining
- Pre-existing (shared with LEFT/RIGHT, not introduced here): single-side predicates inside an ON
  clause or in WHERE can be pushed to per-relation scans, which for outer joins could filter rows
  strict semantics would keep NULL-padded. Out of scope; broader outer-join predicate-pushdown is a
  separate concern.
