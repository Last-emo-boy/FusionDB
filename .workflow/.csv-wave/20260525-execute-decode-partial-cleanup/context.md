# TASK-048 Execution

Cleaned `RowDecoder::decode_partial` without changing its public contract. The function still returns a full-length row with unselected columns as `Null`, while avoiding debug-output error wrapping on the projection decode path.

Files changed: `src/common/encoding.rs`.
