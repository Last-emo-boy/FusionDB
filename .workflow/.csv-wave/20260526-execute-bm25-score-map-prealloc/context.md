# TASK-094 Execution Context

- Scope: `src/storage/inverted_index.rs`
- Change: `search_bm25_limited` now collects matching posting lists before scoring.
- Change: the BM25 score map is preallocated from the summed matched posting length, capped by known document count.
- Semantics preserved: tokenization, IDF/TF scoring, limiting, and sort order remain unchanged.
