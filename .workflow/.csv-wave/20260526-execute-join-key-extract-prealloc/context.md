# TASK-110 Execution Context

- Target: `src/execution/scan.rs`.
- Change: JOIN key extraction now preallocates left key indices, right key indices, and residual predicates from the split ON predicate count.
- Rationale: each split ON predicate can contribute at most one key pair or one residual predicate, so predicate count is the conservative bound.
