# TASK-083 Execution Context

## Scope

- `src/storage/trigram.rs`
- Database core only; `dashboard/` untouched.

## Change

- Trigram search now gathers all postings first, sorts by `RoaringTreemap::len`, then intersects from the smallest bitmap.
- Search short-circuits when a trigram is missing or maps to an empty bitmap.
- Row key mapping now preallocates capacity from candidate cardinality and table id-map size.
- Added unit tests for trigram deduplication and search/intersection mapping behavior.

## Expected Impact

- Multi-trigram wildcard LIKE candidates perform less bitmap intersection work.
- Mapping candidate row ids into row keys does fewer Vec reallocations.
