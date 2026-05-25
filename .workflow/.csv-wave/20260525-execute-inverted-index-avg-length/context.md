# TASK-067 Inverted Index Average Length

Scope: `src/storage/inverted_index.rs`

Implemented:
- Pre-sized the term-frequency map with the token count.
- Replaced the per-insert full `doc_lengths.values().sum()` with an incremental average length update.
- Added unit tests for ordinary inserts and the existing duplicate document-id average semantics.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-inverted-index-avg-length/verification.json`.
