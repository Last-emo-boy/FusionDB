# TASK-089 Execution Context

## Scope

- `src/storage/fusion.rs`
- Database core only; `dashboard/` untouched.

## Change

- `FusionStorage::hybrid_search` now preallocates the RRF score map from the combined text and vector candidate counts.

## Expected Impact

- Lower allocation and rehash cost while fusing hybrid search candidates.
- RRF scoring and final top-k ordering remain unchanged.
