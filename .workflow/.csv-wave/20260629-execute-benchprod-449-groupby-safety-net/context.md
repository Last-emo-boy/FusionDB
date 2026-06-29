# BENCHPROD-449 Execution Context
Safety net closing the silent-incomplete-result gap for distributed GROUP BY. A single-table grouped
SELECT that genuinely scatters across more than one shard owner but matches none of the supported
grouped fan-out plans (446 COUNT(*), 447 SUM/MIN/MAX, 448 AVG) used to fall through to local-only
execution and silently return just the local owner's groups. Now `shard_unsupported_group_by_fanout_
error_for_*` detects that case and returns an explicit error (HTTP 400 / pgwire error) instead. Wired
into all four entry points (HTTP /query + /execute, pgwire simple + extended), dispatched right after
the grouped-aggregate handlers.

Trigger conditions (all required): single-table grouped SELECT; no supported grouped plan matches; the
table exists locally (else defer to the normal loud "table not found"); and `shard_select_fanout_
owners_for_prechecked_statements` returns a non-empty owner set. That owner set is empty whenever the
query is a recognized point read pinned to one owner, or there is no shard router / single node — so
single-owner and non-distributed queries are forwarded/executed as before and never error.

DESIGN DECISION (validated by two adversarial review rounds, ~950k tokens). Round 1 flagged that the
net "wrongly errors" on a single-shard query whose PK pin the point-read recognizer is too narrow to
see (`pk = x AND ...`, single-element `pk IN (x)`, or a non-leading primary key). A PK-pin guard was
added to suppress the error for those shapes. Round 2 REFUTED that guard: for exactly those shapes the
point-read path also declines to forward, so suppressing the error falls through to LOCAL-ONLY
execution — which returns ZERO rows when the pinned row lives on a remote shard, i.e. a silent wrong
answer (the very failure 449 exists to prevent). The guard was REVERTED. The net deliberately prefers
a loud error over silent incompleteness: when a pin is real but not forwardable, erroring is the safe
choice. The only residual cost is a benign loud error on a query that *might* have been locally
complete (pinned row happens to be local) — strictly better than silent corruption, and the root
cause (the narrow point-read recognizer / its `pk_idx != 0` restriction) is a separate pre-existing
limitation, not 449's to fix.

Verified: lib 347 (integration http_query_rejects_unsupported_multiowner_group_by: two-aggregate
multi-owner grouped -> BAD_REQUEST; bare PK-eq point-read pin -> OK; supported SUM grouped -> OK),
sql_group_aggregate 50, pg_integration 36, fmt/diff-check clean. No single-node bench delta.
