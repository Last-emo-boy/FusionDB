# TASK-095 Execution Context

- Scope: `src/storage/trigram.rs`
- Change: `TrigramIndex::search` now borrows the original pattern when it contains no `%` or `_`.
- Change: wildcard patterns clean into a `String` preallocated from `pattern.len()`.
- Semantics preserved: trigram generation, empty-pattern behavior, bitmap lookup, and intersection logic are unchanged.
