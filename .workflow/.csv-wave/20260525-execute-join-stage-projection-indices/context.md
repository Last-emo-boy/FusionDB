# TASK-061 Join Stage Projection Indices

Scope: `src/execution/scan.rs`

Implemented:
- Replaced lowercased required column name storage with required schema indices in `build_stage_join_projection`.
- Preserved schema-order output by filtering enumerated columns with the index set.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-join-stage-projection-indices/verification.json`.
