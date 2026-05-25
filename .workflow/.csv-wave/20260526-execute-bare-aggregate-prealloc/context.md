# TASK-105 Execution Context

- Target: `src/execution/query.rs`.
- Change: bare aggregate expression collection now preallocates from `select.projection.len()`.
- Rationale: a no-GROUP-BY aggregate query can only discover aggregate expressions from projection items on this path, so projection count is a conservative capacity hint.
