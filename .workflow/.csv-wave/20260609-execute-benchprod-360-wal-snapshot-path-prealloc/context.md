# BENCHPROD-360 WAL Snapshot Path Preallocation

## Objective

Preallocate WAL checkpoint snapshot paths without changing checkpoint filenames.

## Scope

- `src/storage/wal.rs`

## Change

- Added `wal_snapshot_path`.
- Replaced `format!("{}.snap", self.path)` in `WalManager::create_checkpoint`.
- Added a focused helper test for exact output bytes and capacity.
- Added a checkpoint replay test covering snapshot creation and replay.

Snapshot paths remain `<base>.snap`. For example, `data/fusion.wal` still produces `data/fusion.wal.snap`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test wal_snapshot_path -- --nocapture` | passed: 1/1 |
| `cargo test test_wal_create_checkpoint_replays_snapshot_entries -- --nocapture` | passed: 1/1 |
| `cargo test storage::wal::tests -- --nocapture` | passed: 13/13 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'format!\("\{\}\.snap", self\.path\)' src/storage/wal.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
