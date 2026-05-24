# Execute Context

Execution status: completed.

Changes:
- Added shared scan helpers to identify `column = value` and `value = column` equality lookup candidates.
- Guarded the value side so column-to-column expressions do not enter constant lookup fast paths.
- Extended DML primary-key point lookup and EXPLAIN access-path reporting to match the same commuted equality behavior.
