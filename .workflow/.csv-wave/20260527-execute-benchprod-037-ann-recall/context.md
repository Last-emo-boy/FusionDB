# BENCHPROD-037 ANN Recall and HNSW Metric Expansion

Date: 2026-05-27
Scope: benchmark harness and workflow evidence only; FusionDB database core and dashboard/ui unchanged.

## Objective

Upgrade the ANN benchmark from latency-only probes to production-useful correctness and setup metrics.

## Implementation

- Added select row extraction in `E:/Playground/FusionDB-bench/fusiondb_bench.py` so benchmark cases can inspect returned IDs and vectors.
- Added per-case `metrics` output to JSON reports plus `Additional Metrics` Markdown sections in both `fusiondb_bench.py` and `fusiondb_matrix.py`.
- Added ANN exact ground-truth evaluation using the actual vectors returned from FusionDB.
- Added recall metrics for ANN nearest and filtered nearest cases:
  - `recall_at_1_avg`
  - `recall_at_10_avg`
  - `recall_at_10_min`
  - `recall_at_10_p05`
  - sample expected/actual ID lists
- Split ANN setup timings:
  - `ann_schema_ms`
  - `ann_insert_load_ms`
  - `ann_hnsw_register_ms`
  - `ann_bucket_index_build_ms`
  - `ann_query_embedding_ms`
  - `ann_hnsw_first_search_build_ms`
  - `ann_ground_truth_fetch_ms`
- Recorded vector payload estimate as `vector_payload_bytes_estimate`; true persistent HNSW index-size metrics still require a future FusionDB core metric/API.

## Verification

Commands:

```powershell
cd E:\Playground\FusionDB-bench
python -m py_compile fusiondb_bench.py fusiondb_matrix.py
python fusiondb_matrix.py --scale tiny --suite ann --load-mode insert --allow-failures --run-name matrix_ann_tiny_after_benchprod037_20260527
python fusiondb_matrix.py --scale medium --suite ann --load-mode insert --allow-failures --run-name matrix_ann_medium_after_benchprod037_20260527
```

Results:

- Tiny ANN matrix: `passed`, 3/3 cases, 0 errors.
- Medium ANN matrix: `passed`, 3/3 cases, 0 errors.

Medium artifacts:

- Matrix Markdown: `E:/Playground/FusionDB-bench/runs/matrix_ann_medium_after_benchprod037_20260527/matrix_summary.md`
- Matrix JSON: `E:/Playground/FusionDB-bench/runs/matrix_ann_medium_after_benchprod037_20260527/matrix_summary.json`

## Medium Metrics

| Case | Avg ms | P95 ms | Recall@1 avg | Recall@10 avg | Candidate count |
|---|---:|---:|---:|---:|---:|
| HNSW nearest neighbor | 44.316 | 44.911 | 1.000 | 1.000 | 5000 |
| Filtered nearest neighbor | 5.383 | 5.620 | 1.000 | 1.000 | 500 |
| Insert vector | 0.808 | 0.961 | n/a | n/a | n/a |

Medium setup timings:

| Phase | ms |
|---|---:|
| ann_schema_ms | 2.284 |
| ann_insert_load_ms | 100.454 |
| ann_hnsw_register_ms | 9.418 |
| ann_bucket_index_build_ms | 10.483 |
| ann_query_embedding_ms | 1.619 |
| ann_hnsw_first_search_build_ms | 43.311 |
| ann_ground_truth_fetch_ms | 51.069 |

## Production Gap After This Task

Completed for local ANN credibility:

- recall@k against exact ground truth
- HNSW lazy build timing visibility
- vector payload size estimate
- JSON/Markdown metric propagation through matrix reports

Still open for production-grade ANN benchmark parity:

- external ANN dataset adapters
- external ground-truth file ingestion
- HNSW parameter sweep and configuration recording
- true persistent index memory/disk size metrics from FusionDB core
