# TASK-103 Execution Context

- Target: `src/execution/dml.rs`.
- Change: `primary_key_qualifiers` now initializes its result vector with capacity for table name and alias.
- Rationale: qualified UPDATE / DELETE primary key lookup only needs up to two qualifier strings, so fixed preallocation avoids a small growth path.
