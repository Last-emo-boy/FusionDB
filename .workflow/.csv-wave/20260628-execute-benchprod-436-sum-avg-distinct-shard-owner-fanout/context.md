# BENCHPROD-436 Execution Context

## Outcome

Completed `BENCHPROD-436` as the twenty-second `P5-3` automatic sharding iteration, finishing the
DISTINCT aggregate fan-out family started by `BENCHPROD-435` (`COUNT(DISTINCT)`).

## Implementation

- Added executor-level eligibility and planning for conservative single-table `SUM(DISTINCT column)`
  and `AVG(DISTINCT column)` fan-out via a shared `distinct_aggregate_select_fanout_target` helper
  and a `SqlShardDistinctAggregateFanoutPlan` rewrite descriptor.
- Rewrote eligible distinct aggregates to a shard-local `SELECT DISTINCT column` query, merged the
  deduplicated value union across shard owners, and reduced it: `SUM(DISTINCT)` sums the union,
  `AVG(DISTINCT)` divides the union sum by the union count (NULLs excluded, matching SQL semantics).
- Restricted distinct-aggregate fan-out planning to integer and floating-point table columns after
  schema lookup, mirroring the existing `AVG`/`MIN`/`MAX` numeric guard; non-numeric columns remain
  local/conservative.
- Reused the existing shard-owner SELECT fan-out owner discovery after verifying query shape and
  numeric column type. Distinct-aggregate eligibility is mutually exclusive with the plain
  `SUM`/`AVG` paths (those require no `DISTINCT` duplicate treatment), so dispatch ordering is safe.
- Added HTTP `/query` SUM/AVG distinct fan-out that gathers the cross-owner distinct value map and
  emits the reduced scalar, preserving NULL behavior for empty inputs.
- Added HTTP prepared `/execute` SUM/AVG distinct fan-out using rewritten prepared SQL with the
  original parameter values.
- Added pgwire simple-query SUM/AVG distinct fan-out using owner HTTP `/query` responses and pgwire
  response conversion.
- Added pgwire extended-query SUM/AVG distinct fan-out using owner prepared forwarding and extended
  result encoding.
- Updated README and ROADMAP to document distributed `SUM(DISTINCT column)` and `AVG(DISTINCT column)`
  aggregation alongside the existing distributed aggregate set.

## Verification

- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo check --bins` passed.
- `cargo test --lib http_query_fanouts_simple_select_across_shard_owners -- --nocapture` passed
  (extended with `SUM(DISTINCT)`/`AVG(DISTINCT)` assertions).
- `cargo test --lib http_query_fanouts_distinct_numeric_aggregates_across_shard_owners -- --nocapture`
  passed (new test; crafts a cross-owner duplicate value so `SUM(DISTINCT)=30` differs from plain
  `SUM=70`, proving the union deduplicates across owners).
- `cargo test --lib distinct_aggregate_select_fanout_plans_match_sum_and_avg -- --nocapture` passed
  (new planner unit test).
- `cargo test --lib` passed.

## Environment Note

The sandbox shipped with rustc 1.85, but the pinned dependency set (`pgwire 0.37`, `smol_str`)
declares a minimum supported rust-version of 1.89, so the rust-version-aware resolver refused to
build the test target. Installed a current stable toolchain (rustc/cargo 1.96) via rustup and the
system `libssl-dev` package required by `reqwest`'s `native-tls` build; no repository files,
`Cargo.toml`, or `Cargo.lock` were modified to work around the toolchain.

## Remaining Production Gaps

- `P5-3`: Aggregate forms beyond `COUNT(*)`, `COUNT(DISTINCT)`, `SUM`, `SUM(DISTINCT)`, numeric
  `MIN`/`MAX`, `AVG`, and `AVG(DISTINCT)`, including grouped (`GROUP BY`) aggregation, aggregate
  expressions, `MIN(DISTINCT)`/`MAX(DISTINCT)` (semantically equal to the plain forms),
  DECIMAL/NUMERIC precision handling, `ORDER BY`/`LIMIT`, join, subquery, and set-operation
  distributed planning remain open. The next logical iteration is distributed single-column
  `GROUP BY` aggregate fan-out (starting with `COUNT(*)`).
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite
  primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists
  that omit the primary key.
