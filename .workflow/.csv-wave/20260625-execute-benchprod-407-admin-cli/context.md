# BENCHPROD-407 Execution Context

## Summary

Added `fusiondb-cli`, a scriptable admin CLI for FusionDB HTTP operations.

## Files

- `Cargo.toml`
- `src/bin/fusiondb-cli.rs`
- `README.md`
- `ROADMAP.md`

## Behavior

- `fusiondb-cli health` calls `GET /health`.
- `fusiondb-cli query <SQL...>` calls `POST /query`.
- `fusiondb-cli tables`, `metrics`, `prometheus`, `slow-queries`, `capabilities`, and `auth-context` call their corresponding HTTP endpoints.
- `fusiondb-cli checkpoint` calls `POST /checkpoint`.
- `fusiondb-cli compact` and `fusiondb-cli vacuum` call `POST /compact`.
- `--url` selects a custom server endpoint.
- `--user` sends `x-fusiondb-user` for RBAC-aware admin workflows.
- JSON responses are pretty-printed; health/prometheus remain plain text.

## Verification

- `cargo fmt -- src/bin/fusiondb-cli.rs` passed.
- `cargo test --bin fusiondb-cli` passed: 6 passed.
- `cargo check --bins` passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.

