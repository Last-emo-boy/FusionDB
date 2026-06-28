# BENCHPROD-446 Execution Context
Generalized BENCHPROD-445's single-column `GROUP BY col, COUNT(*)` shard-owner fan-out to N group
columns. Plan struct `group_index: usize` -> `group_indices: Vec<usize>`; eligibility now accepts any
non-empty GROUP BY whose columns exactly match the non-COUNT projection items (any order, no dups);
merge keys on the canonical JSON array of the group-value tuple (NULL components preserved); rows
scattered back into projection order. Covered all 4 entry points (HTTP query/execute, pgwire
simple/extended). Designed by the batch-design workflow. Fixes the multi-node multi-column GROUP BY
silent-incomplete-result bug. Verified: lib 342, planner unit test (incl. multi-col + reordered),
2-node group-count integration, sql_group_aggregate 50, fmt/diff-check clean. No single-node bench delta.
