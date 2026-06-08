# BENCHPROD-256 COPY STDIN Row Preallocation

## Goal

Avoid initial `Vec` growth while parsing COPY STDIN payloads whose bytes are already buffered in memory.

## Implementation

- `src/execution/copy.rs`
  - `execute_copy_stdin_payload` now calls the associated `read_copy_bytes` helper.
  - `read_copy_bytes` estimates row capacity from payload newline counts and the `HEADER` option.
  - Added `read_copy_reader_with_capacity` so in-memory payload parsing can pass a capacity hint.
  - `read_copy_reader` still uses zero initial capacity, so file-based COPY does not perform an extra scan.
  - Added private unit coverage for row-capacity estimation and CSV row parsing.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test execution::copy::tests -- --nocapture`
  - Passed: 2/2.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test pg_integration test_pg_protocol_copy_from_stdin_text_and_csv -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

`cargo fmt` was applied once, then the COPY unit tests and PostgreSQL COPY STDIN integration test were rerun against the final formatted state.

## Result

`BENCHPROD-256` is complete. COPY STDIN parsing now preallocates the row buffer from the already-buffered payload while preserving file COPY behavior.
