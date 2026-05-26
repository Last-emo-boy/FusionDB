# TASK-122 Execution

Target: `src/execution/ddl.rs`

Change:
- `DESCRIBE` rows now reserve from `schema.columns.len()`.
- `SHOW TABLES` rows now reserve from scanned schema key count.
- `SHOW VIEWS` rows now reserve from scanned view key count.

Behavior:
- Metadata query outputs are unchanged.
- The optimization only avoids repeated result vector growth when materializing metadata rows.
