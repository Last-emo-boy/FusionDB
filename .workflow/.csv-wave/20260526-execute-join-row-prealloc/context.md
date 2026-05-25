# TASK-113 Execution Context

- Target: `src/execution/scan.rs`.
- Change: hash JOIN and unmatched LEFT JOIN row construction now preallocates by final joined row width.
- Rationale: joined rows always contain all left columns followed by all right columns, so schema widths give the exact row capacity.
