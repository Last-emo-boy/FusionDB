# TASK-109 Execution Context

- Target: `src/execution/scan.rs`.
- Change: relation and schema predicate split helpers now preallocate `local` and `remaining` vectors from the pending predicate count.
- Rationale: these helpers drain the current predicate list into two output lists, so the input length is the conservative capacity bound.
