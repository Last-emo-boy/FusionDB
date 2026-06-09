# BENCHPROD-358 WAL Segment Path Preallocation

## Objective

Preallocate WAL segment file paths and scan prefixes without changing filenames.

## Scope

- `src/storage/wal.rs`

## Change

- Replaced segment path `format!` with exact-capacity construction.
- Added `wal_segment_file_prefix` for segment discovery prefixes.
- Added `u64_decimal_len` to reserve the segment id width before writing it into the path.
- Added focused helper tests for exact output bytes and capacity.

Segment filenames remain unchanged: segment 0 uses the base path, and segment N still uses `<base>.seg.<N>`. Segment discovery still matches `<base_name>.seg.`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test segment_path_preallocates_segment_path -- --nocapture` | passed: 1/1 |
| `cargo test storage::wal::tests -- --nocapture` | passed: 11/11 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\(".*\.seg\.' src/storage/wal.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
