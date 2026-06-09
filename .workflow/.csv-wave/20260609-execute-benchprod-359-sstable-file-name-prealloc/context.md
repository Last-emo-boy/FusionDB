# BENCHPROD-359 SSTable File Name Preallocation

## Objective

Preallocate SSTable file names without changing storage paths.

## Scope

- `src/storage/fusion.rs`

## Change

- Added `sstable_file_name_for_id`.
- Added a local `u64_decimal_len` helper for exact file-name capacity.
- Replaced `format!("{}.sst", id)` in `FusionStorage::sstable_path_for`.
- Added focused helper tests for exact output bytes and capacity.

SSTable file names remain `<id>.sst`, and `FusionStorage::sstable_path_for` still joins that name under the configured SSTable directory.

## Verification

| Command | Result |
| --- | --- |
| `cargo test sstable_file_name_for_id -- --nocapture` | passed: 1/1 |
| `cargo test storage::fusion::tests -- --nocapture` | passed: 24/24 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\{\}\.sst", id\)' src/storage/fusion.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
