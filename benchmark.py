#!/usr/bin/env python3
"""
FusionDB Unified Benchmark Suite
==================================
All-in-one performance benchmark for FusionDB covering:

  Part 1 — Base Benchmarks
    Point queries, range scans, aggregations, sorting, writes,
    index effectiveness, complex filters.

  Part 2 — E-commerce Simulation
    Users, products, orders, order items.
    Customer lookup, product browsing, order placement, inventory updates.

  Part 3 — Financial Ledger
    Accounts, transfers, balance checks, audit trail aggregations.

  Part 4 — Analytics / OLAP
    Revenue reports, customer segmentation, product rankings,
    time-series event analysis, subqueries, multi-table JOINs.

  Part 5 — Concurrent Mixed Workload
    Multi-threaded read/write simulation at different ratios
    (80:20, 50:50, 20:80) measuring throughput under contention.

  Part 6 — Stress & Edge Cases
    Wide IN lists, multi-column ORDER BY, high-cardinality GROUP BY,
    3-table JOINs, bulk UPDATE/DELETE, UNION queries.

  Part 7 — Inventory & Fulfillment
    Stock availability, replenishment candidates, shipment queues,
    cart reservation checks, restock writes.

  Part 8 — Risk & Audit
    Large transfer review, exposure rollups, failed-transfer audits,
    suspicious spend and activity patterns.

  Part 9 — Column-Scan Fast Paths
    Narrow aggregate and DISTINCT workloads that should avoid full-row
    materialization: COUNT(column), COUNT(DISTINCT ... WHERE ...),
    DISTINCT ... WHERE ..., MIN/MAX, STRING_AGG, GROUP_CONCAT.

  Part 10 — Stats-Aware Join Reorder
    Skewed 3-table join workload that compares the same query before and
    after ANALYZE so NDV-based join reorder changes are measurable.

Usage:
    1. Start FusionDB:  cargo run
    2. Run benchmark:   python benchmark.py
    3. Quick mode:      BENCH_SCALE=small python benchmark.py
    4. Large mode:      BENCH_SCALE=large python benchmark.py
    5. pgwire path:     BENCH_PROTO=pg python benchmark.py   (needs psycopg2)

Options (env vars):
    FUSIONDB_URL   - HTTP endpoint (default: http://127.0.0.1:8091/query)
    FUSIONDB_HTTP_USER/HTTP_PASSWORD - HTTP Basic credentials (defaults: postgres/fusiondb)
    BENCH_SCALE    - small / medium / large  (default: medium)
    BENCH_PROTO    - http / pg  (default: http; pg = PostgreSQL wire protocol on :8092)
    BENCH_PARTS    - optional comma list/range of parts or keys, e.g. 1,4,10 or base,join_ndv
    BENCH_MATRIX   - optional preset slice: full / join_ndv / selectivity / topk / columnar_single_source / index_topk / index_topk_restart / index_topk_rseek_ab / index_topk_prefix_prune / sql_block_index_prefix_prune / sql_block_zone_map_prune / index_topk_frontier / sstable_reverse_frontier / fusion_reverse_frontier / index_distinct / groupby / analyze / planner / or_in_scan / between_scan / like_prefix_scan / sql_no_fill_cache / sstable_range_bound / sstable_prefix_bloom / sstable_block_prefix / sstable_block_index_prefix / sstable_user_key_bloom / sstable_no_fill_cache / sstable_startup_index
    BENCH_CLAIM_MODE - set to 1 to turn supported benchmark observations into pass/fail gates
    BENCH_OS_CACHE_CONTROL - none / drop_caches for benchmark-owned restart matrices (default: none)
    BENCH_INDEX_TOPK_RESTART_TRIALS - process restart trials for index_topk_restart (default: 1)
    BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR - allow deleting a non-empty explicit restart workdir
    BENCH_SQL_BLOCK_ZONE_MAP_OWNED_SERVER - run Part 31 claim on a benchmark-owned server (default: claim mode only)
    BENCH_SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB - owned Part 31 server memtable flush threshold (default: 256)
    FUSIONDB_PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DBNAME - pgwire connection overrides
"""

import requests
import time
import random
import os
import sys
import json
import base64
import csv
import hashlib
import io
import platform
import struct
import statistics
import threading
import subprocess
import shutil
import signal
import tempfile
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple, Dict, Set
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ═══════════════════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════════════════
BASE_URL = os.environ.get("FUSIONDB_URL", "http://127.0.0.1:8091/query")
HEALTH_URL = BASE_URL.replace("/query", "/health")
METRICS_URL = os.environ.get("FUSIONDB_METRICS_URL", BASE_URL.replace("/query", "/metrics"))
CHECKPOINT_URL = os.environ.get("FUSIONDB_CHECKPOINT_URL", BASE_URL.replace("/query", "/checkpoint"))
COPY_STDIN_URL = os.environ.get("FUSIONDB_COPY_STDIN_URL", BASE_URL.replace("/query", "/copy_stdin"))
SCALE = os.environ.get("BENCH_SCALE", "medium").lower()
BENCH_PARTS = os.environ.get("BENCH_PARTS", "").strip()
BENCH_MATRIX = os.environ.get("BENCH_MATRIX", "full").strip().lower()
BENCH_CLAIM_MODE = os.environ.get("BENCH_CLAIM_MODE", "0").strip().lower() not in (
    "",
    "0",
    "false",
    "no",
)
HTTP_SESSIONS = threading.local()
HTTP_USER = os.environ.get("FUSIONDB_HTTP_USER", "postgres")
HTTP_PASSWORD = os.environ.get("FUSIONDB_HTTP_PASSWORD", "fusiondb")

# Transport: "http" (HTTP+JSON, default) or "pg" (PostgreSQL wire protocol, binary over TCP).
# Same suite, same SQL — only the client/server protocol differs, so the two reports isolate the
# protocol cost from the engine cost.
PROTO = os.environ.get("BENCH_PROTO", "http").lower()
PG_HOST = os.environ.get("FUSIONDB_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("FUSIONDB_PG_PORT", "8092"))
PG_USER = os.environ.get("FUSIONDB_PG_USER", "postgres")
PG_PASSWORD = os.environ.get("FUSIONDB_PG_PASSWORD", "fusiondb")
PG_DBNAME = os.environ.get("FUSIONDB_PG_DBNAME", "postgres")
PG_LOCAL = threading.local()

SCALES = {
    #                 base_rows  users  products  orders  accounts  transfers  events  wide_rows  iters  warmup  batch  threads
    "small":  dict(base_rows=2000,  users=100,  products=50,   orders=500,   accounts=50,  transfers=200,  events=1000,  wide_rows=20000,  iters=5,  warmup=1, batch=500,  threads=4),
    "medium": dict(base_rows=10000, users=500,  products=200,  orders=5000,  accounts=200, transfers=2000, events=10000, wide_rows=100000, iters=10, warmup=2, batch=500,  threads=8),
    "large":  dict(base_rows=50000, users=2000, products=1000, orders=20000, accounts=500, transfers=5000, events=50000, wide_rows=250000, iters=20, warmup=3, batch=500,  threads=16),
    "xlarge": dict(base_rows=500000,users=20000,products=10000,orders=200000,accounts=5000,transfers=50000,events=500000,wide_rows=500000,iters=20, warmup=3, batch=1000, threads=32),
}

if SCALE not in SCALES:
    print(f"Invalid BENCH_SCALE={SCALE}. Choose: small, medium, large, xlarge")
    sys.exit(1)

C = SCALES[SCALE]
SEED = 42
random.seed(SEED)
RESULT_CHECKSUM_ALGORITHM = "sha256-json-v1"
JOIN_REORDER_NDV_BUCKETS = 10
WIDE_SCAN_PAYLOAD_COLUMNS = 4
WIDE_SCAN_PAYLOAD_BYTES = int(os.environ.get("BENCH_WIDE_PAYLOAD_BYTES", "512"))
WIDE_SCAN_ROWS_OVERRIDE = int(os.environ.get("BENCH_WIDE_ROWS", "0"))
WIDE_SCAN_STABILIZE = os.environ.get("BENCH_WIDE_STABILIZE", "1").strip().lower() not in ("0", "false", "no")
WIDE_SCAN_STABILIZE_MAX_PROBES = int(os.environ.get("BENCH_WIDE_STABILIZE_MAX_PROBES", "12"))
WIDE_SCAN_STABILIZE_WINDOW = int(os.environ.get("BENCH_WIDE_STABILIZE_WINDOW", "3"))
WIDE_SCAN_STABILIZE_CV_PCT = float(os.environ.get("BENCH_WIDE_STABILIZE_CV_PCT", "12.5"))
SST_BOUND_ROWS_OVERRIDE = int(os.environ.get("BENCH_BOUND_ROWS", os.environ.get("BENCH_SST_BOUND_ROWS", "0")))
SST_BOUND_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_BOUND_PAYLOAD_BYTES", "8192"))
SST_BOUND_SNAPSHOT_ROUNDS = int(os.environ.get("BENCH_SST_BOUND_SNAPSHOT_ROUNDS", "2"))
SST_PREFIX_BLOOM_ROWS_OVERRIDE = int(os.environ.get("BENCH_PREFIX_BLOOM_ROWS", os.environ.get("BENCH_SST_PREFIX_BLOOM_ROWS", "0")))
SST_PREFIX_BLOOM_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_PREFIX_BLOOM_PAYLOAD_BYTES", "1024"))
SST_BLOCK_PREFIX_SSTABLES = int(os.environ.get("BENCH_SST_BLOCK_PREFIX_SSTABLES", "512"))
SST_BLOCK_PREFIX_ITERS = int(os.environ.get("BENCH_SST_BLOCK_PREFIX_ITERS", "5"))
SST_BLOCK_PREFIX_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_BLOCK_PREFIX_PAYLOAD_BYTES", "1024"))
SST_BLOCK_PREFIX_RELEASE = os.environ.get("BENCH_SST_BLOCK_PREFIX_RELEASE", "1").strip().lower() not in ("0", "false", "no")
SST_BLOCK_PREFIX_TIMEOUT_SEC = int(os.environ.get("BENCH_SST_BLOCK_PREFIX_TIMEOUT_SEC", "300"))
SST_BLOCK_INDEX_PREFIX_SSTABLES = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_SSTABLES", "512"))
SST_BLOCK_INDEX_PREFIX_ITERS = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_ITERS", "5"))
SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES", "1024"))
SST_BLOCK_INDEX_PREFIX_RELEASE = os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_RELEASE", "1").strip().lower() not in ("0", "false", "no")
SST_BLOCK_INDEX_PREFIX_TIMEOUT_SEC = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_TIMEOUT_SEC", "300"))
SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES", "32768"))
SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS", "3"))
SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES", "16"))
SST_BLOCK_INDEX_PREFIX_NATURAL_CANDIDATES = int(os.environ.get("BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_CANDIDATES", "200000"))
SST_USER_KEY_BLOOM_SSTABLES = int(os.environ.get("BENCH_SST_USER_KEY_BLOOM_SSTABLES", "512"))
SST_USER_KEY_BLOOM_ITERS = int(os.environ.get("BENCH_SST_USER_KEY_BLOOM_ITERS", "5"))
SST_USER_KEY_BLOOM_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_USER_KEY_BLOOM_PAYLOAD_BYTES", "1024"))
SST_USER_KEY_BLOOM_RELEASE = os.environ.get("BENCH_SST_USER_KEY_BLOOM_RELEASE", "1").strip().lower() not in ("0", "false", "no")
SST_USER_KEY_BLOOM_TIMEOUT_SEC = int(os.environ.get("BENCH_SST_USER_KEY_BLOOM_TIMEOUT_SEC", "300"))
SST_NO_FILL_SCAN_BLOCKS = int(os.environ.get("BENCH_SST_NO_FILL_SCAN_BLOCKS", "512"))
SST_NO_FILL_ITERS = int(os.environ.get("BENCH_SST_NO_FILL_ITERS", "5"))
SST_NO_FILL_PAYLOAD_BYTES = int(os.environ.get("BENCH_SST_NO_FILL_PAYLOAD_BYTES", "1024"))
SST_NO_FILL_CACHE_BLOCKS = int(os.environ.get("BENCH_SST_NO_FILL_CACHE_BLOCKS", "1"))
SST_NO_FILL_RELEASE = os.environ.get("BENCH_SST_NO_FILL_RELEASE", "1").strip().lower() not in ("0", "false", "no")
SST_NO_FILL_TIMEOUT_SEC = int(os.environ.get("BENCH_SST_NO_FILL_TIMEOUT_SEC", "300"))
SST_REVERSE_FRONTIER_DECOYS = int(os.environ.get("BENCH_SST_REVERSE_FRONTIER_DECOYS", "64"))
SST_REVERSE_FRONTIER_ITERS = int(os.environ.get("BENCH_SST_REVERSE_FRONTIER_ITERS", "5"))
SST_REVERSE_FRONTIER_PAYLOAD_BYTES = int(
    os.environ.get("BENCH_SST_REVERSE_FRONTIER_PAYLOAD_BYTES", "256")
)
SST_REVERSE_FRONTIER_CACHE_BLOCKS = int(
    os.environ.get("BENCH_SST_REVERSE_FRONTIER_CACHE_BLOCKS", "1000000")
)
SST_REVERSE_FRONTIER_RELEASE = os.environ.get(
    "BENCH_SST_REVERSE_FRONTIER_RELEASE", "1"
).strip().lower() not in ("0", "false", "no")
SST_REVERSE_FRONTIER_TIMEOUT_SEC = int(
    os.environ.get("BENCH_SST_REVERSE_FRONTIER_TIMEOUT_SEC", "300")
)
FUSION_REVERSE_FRONTIER_DECOYS = int(
    os.environ.get("BENCH_FUSION_REVERSE_FRONTIER_DECOYS", "2")
)
FUSION_REVERSE_FRONTIER_ITERS = int(
    os.environ.get("BENCH_FUSION_REVERSE_FRONTIER_ITERS", "3")
)
FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES = int(
    os.environ.get("BENCH_FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES", "8192")
)
FUSION_REVERSE_FRONTIER_CACHE_BLOCKS = int(
    os.environ.get("BENCH_FUSION_REVERSE_FRONTIER_CACHE_BLOCKS", "1000000")
)
FUSION_REVERSE_FRONTIER_RELEASE = os.environ.get(
    "BENCH_FUSION_REVERSE_FRONTIER_RELEASE", "1"
).strip().lower() not in ("0", "false", "no")
FUSION_REVERSE_FRONTIER_TIMEOUT_SEC = int(
    os.environ.get("BENCH_FUSION_REVERSE_FRONTIER_TIMEOUT_SEC", "300")
)
SQL_NO_FILL_ROWS_OVERRIDE = int(os.environ.get("BENCH_SQL_NO_FILL_ROWS", "0"))
SQL_NO_FILL_PAYLOAD_BYTES = int(os.environ.get("BENCH_SQL_NO_FILL_PAYLOAD_BYTES", "512"))
SQL_NO_FILL_CACHE_BLOCKS = int(os.environ.get("BENCH_SQL_NO_FILL_CACHE_BLOCKS", "1"))
SQL_NO_FILL_BINARY = os.environ.get(
    "BENCH_SQL_NO_FILL_BINARY", os.path.join("target", "debug", "fusiondb")
)
SQL_NO_FILL_PORT = int(os.environ.get("BENCH_SQL_NO_FILL_PORT", "18211"))
SQL_NO_FILL_WORKDIR = os.environ.get("BENCH_SQL_NO_FILL_WORKDIR", "")
SQL_NO_FILL_KEEP_WORKDIR = os.environ.get("BENCH_SQL_NO_FILL_KEEP_WORKDIR", "0").strip().lower() not in (
    "0",
    "false",
    "no",
)
SQL_NO_FILL_RESET_WORKDIR = os.environ.get("BENCH_SQL_NO_FILL_RESET_WORKDIR", "0").strip().lower() not in (
    "0",
    "false",
    "no",
)
SQL_NO_FILL_TIMEOUT_SEC = int(os.environ.get("BENCH_SQL_NO_FILL_TIMEOUT_SEC", "60"))
SST_STARTUP_DATA_DIR = os.environ.get("BENCH_SST_STARTUP_DATA_DIR", "data")
SST_STARTUP_WORKDIR = os.environ.get("BENCH_SST_STARTUP_WORKDIR", "")
SST_STARTUP_BINARY = os.environ.get(
    "BENCH_SST_STARTUP_BINARY", os.path.join("target", "debug", "fusiondb")
)
SST_STARTUP_PORT = int(os.environ.get("BENCH_SST_STARTUP_PORT", "18091"))
SST_STARTUP_COPY_DATA = os.environ.get("BENCH_SST_STARTUP_COPY_DATA", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
SST_STARTUP_KEEP_WORKDIR = os.environ.get("BENCH_SST_STARTUP_KEEP_WORKDIR", "0").strip().lower() not in (
    "0",
    "false",
    "no",
)
SST_STARTUP_TIMEOUT_SEC = int(os.environ.get("BENCH_SST_STARTUP_TIMEOUT_SEC", "60"))
SST_STARTUP_ORPHAN_COUNT = int(os.environ.get("BENCH_SST_STARTUP_ORPHAN_COUNT", "64"))
SST_STARTUP_DIRTY_WAL_ENTRIES = int(os.environ.get("BENCH_SST_STARTUP_DIRTY_WAL_ENTRIES", "1000"))
SST_STARTUP_SCENARIOS = tuple(
    item.strip().lower()
    for item in os.environ.get(
        "BENCH_SST_STARTUP_SCENARIOS",
        "warm_sidecar,warm_manifest,orphan_manifest,v2_manifest,v2_orphan_manifest,v2_many_edits,dirty_wal_manifest,no_sidecar,stale_sidecar,corrupt_sidecar",
    ).split(",")
    if item.strip()
)
SST_STARTUP_FIRST_POINT_SQL = os.environ.get(
    "BENCH_SST_STARTUP_FIRST_POINT_SQL",
    "SELECT * FROM bench WHERE id = 0",
)
SST_STARTUP_FIRST_RANGE_SQL = os.environ.get(
    "BENCH_SST_STARTUP_FIRST_RANGE_SQL",
    "SELECT * FROM bench WHERE id >= 0 AND id < 10",
)
BENCH_DISCLOSURE_DATA_DIR = os.environ.get("BENCH_DISCLOSURE_DATA_DIR", "").strip()
LEGACY_MANIFEST_SCENARIOS = ("warm_manifest", "orphan_manifest", "dirty_wal_manifest")
V2_MANIFEST_SCENARIOS = (
    "v2_manifest",
    "v2_orphan_manifest",
    "v2_many_edits",
    "v2_torn_tail_rollover",
)
INDEX_TOPK_ROWS_OVERRIDE = int(os.environ.get("BENCH_INDEX_TOPK_ROWS", "0"))
INDEX_TOPK_LIMIT = int(os.environ.get("BENCH_INDEX_TOPK_LIMIT", "50"))
INDEX_TOPK_SSTABLE_CLAIM = BENCH_CLAIM_MODE and os.environ.get(
    "BENCH_INDEX_TOPK_SSTABLE_CLAIM", "1"
).strip().lower() not in ("0", "false", "no")
INDEX_TOPK_FIRST_PERSISTED_PASS = INDEX_TOPK_SSTABLE_CLAIM and os.environ.get(
    "BENCH_INDEX_TOPK_FIRST_PERSISTED_PASS", "1"
).strip().lower() not in ("0", "false", "no")
INDEX_TOPK_RESTART_BINARY = os.environ.get("BENCH_INDEX_TOPK_RESTART_BINARY", SST_STARTUP_BINARY)
INDEX_TOPK_RESTART_PORT = int(os.environ.get("BENCH_INDEX_TOPK_RESTART_PORT", "18101"))
INDEX_TOPK_RESTART_WORKDIR = os.environ.get("BENCH_INDEX_TOPK_RESTART_WORKDIR", "")
INDEX_TOPK_RESTART_KEEP_WORKDIR = os.environ.get(
    "BENCH_INDEX_TOPK_RESTART_KEEP_WORKDIR", "0"
).strip().lower() not in ("0", "false", "no")
INDEX_TOPK_RESTART_RESET_WORKDIR = os.environ.get(
    "BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR", "0"
).strip().lower() not in ("0", "false", "no")
INDEX_TOPK_RESTART_TIMEOUT_SEC = int(os.environ.get("BENCH_INDEX_TOPK_RESTART_TIMEOUT_SEC", "60"))
INDEX_TOPK_RESTART_TRIALS = max(1, int(os.environ.get("BENCH_INDEX_TOPK_RESTART_TRIALS", "1")))
INDEX_TOPK_RSEEK_AB = (
    BENCH_MATRIX == "index_topk_rseek_ab"
    or os.environ.get("BENCH_INDEX_TOPK_RSEEK_AB", "0").strip().lower()
    not in ("", "0", "false", "no")
)
OS_CACHE_CONTROL = os.environ.get("BENCH_OS_CACHE_CONTROL", "none").strip().lower()
OS_DROP_CACHES_VALUE = os.environ.get("BENCH_OS_DROP_CACHES_VALUE", "3").strip()
INDEX_TOPK_RESTART_OS_CACHE_CONTROL = os.environ.get(
    "BENCH_INDEX_TOPK_RESTART_OS_CACHE_CONTROL", OS_CACHE_CONTROL
).strip().lower()
INDEX_TOPK_PREFIX_PRUNE_DECOY_SSTABLES_OVERRIDE = int(
    os.environ.get("BENCH_INDEX_TOPK_PREFIX_PRUNE_DECOY_SSTABLES", "0")
)
INDEX_TOPK_PREFIX_PRUNE_ROWS_PER_HOST_OVERRIDE = int(
    os.environ.get("BENCH_INDEX_TOPK_PREFIX_PRUNE_ROWS_PER_HOST", "0")
)
SQL_BLOCK_INDEX_PREFIX_DECOY_SSTABLES_OVERRIDE = int(
    os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_DECOY_SSTABLES", "0")
)
SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE_OVERRIDE = int(
    os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE", "0")
)
SQL_BLOCK_INDEX_PREFIX_TARGET_ROWS_OVERRIDE = int(
    os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_TARGET_ROWS", "0")
)
SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES = int(
    os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES", "20000")
)
SQL_BLOCK_INDEX_PREFIX_GAP_HOST = int(
    os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_GAP_HOST", "1000000")
)
SQL_BLOCK_INDEX_PREFIX_FORCE_HOST = int(
    os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_FORCE_HOST", "0")
)
SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX = os.environ.get(
    "BENCH_SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX", "1"
).strip().lower() not in ("", "0", "false", "no")
SQL_BLOCK_INDEX_PREFIX_COPY_STDIN = os.environ.get(
    "BENCH_SQL_BLOCK_INDEX_PREFIX_COPY_STDIN", "1"
).strip().lower() not in ("", "0", "false", "no")
SQL_BLOCK_INDEX_PREFIX_COPY_CHUNK_ROWS = max(
    1, int(os.environ.get("BENCH_SQL_BLOCK_INDEX_PREFIX_COPY_CHUNK_ROWS", "1000"))
)
SQL_BLOCK_ZONE_MAP_ROWS_OVERRIDE = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_ROWS", "0")
)
SQL_BLOCK_ZONE_MAP_RANDOM_ROWS_OVERRIDE = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_RANDOM_ROWS", "0")
)
SQL_BLOCK_ZONE_MAP_MVCC_ROWS_OVERRIDE = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_MVCC_ROWS", "0")
)
SQL_BLOCK_ZONE_MAP_BUCKETS = max(
    2, int(os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_BUCKETS", "16"))
)
SQL_BLOCK_ZONE_MAP_TARGET_BUCKET = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_TARGET_BUCKET", "7")
)
SQL_BLOCK_ZONE_MAP_ABSENT_BUCKET = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_ABSENT_BUCKET", "-1")
)
SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET", "3")
)
SQL_BLOCK_ZONE_MAP_MVCC_NEW_BUCKET = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_MVCC_NEW_BUCKET", "11")
)
SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES", "512")
)
SQL_BLOCK_ZONE_MAP_COPY_STDIN = os.environ.get(
    "BENCH_SQL_BLOCK_ZONE_MAP_COPY_STDIN", "1"
).strip().lower() not in ("", "0", "false", "no")
SQL_BLOCK_ZONE_MAP_COPY_CHUNK_ROWS = max(
    1, int(os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_COPY_CHUNK_ROWS", "1000"))
)
SQL_BLOCK_ZONE_MAP_DISABLED_CONTROL = os.environ.get(
    "BENCH_SQL_BLOCK_ZONE_MAP_DISABLED_CONTROL", "1"
).strip().lower() not in ("0", "false", "no")
SQL_BLOCK_ZONE_MAP_OWNED_SERVER_RAW = os.environ.get(
    "BENCH_SQL_BLOCK_ZONE_MAP_OWNED_SERVER", ""
).strip().lower()
SQL_BLOCK_ZONE_MAP_BINARY = os.environ.get(
    "BENCH_SQL_BLOCK_ZONE_MAP_BINARY", os.path.join("target", "debug", "fusiondb")
)
SQL_BLOCK_ZONE_MAP_PORT = int(os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_PORT", "19191"))
SQL_BLOCK_ZONE_MAP_WORKDIR = os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_WORKDIR", "")
SQL_BLOCK_ZONE_MAP_KEEP_WORKDIR = os.environ.get(
    "BENCH_SQL_BLOCK_ZONE_MAP_KEEP_WORKDIR", "0"
).strip().lower() not in ("0", "false", "no")
SQL_BLOCK_ZONE_MAP_RESET_WORKDIR = os.environ.get(
    "BENCH_SQL_BLOCK_ZONE_MAP_RESET_WORKDIR", "0"
).strip().lower() not in ("0", "false", "no")
SQL_BLOCK_ZONE_MAP_TIMEOUT_SEC = int(
    os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_TIMEOUT_SEC", "60")
)
SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB = max(
    1, int(os.environ.get("BENCH_SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB", "256"))
)
SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT: Dict[str, object] = {}
SQL_BLOCK_ZONE_MAP_DISABLE_HINT = "/*+ FUSIONDB_DISABLE_SQL_BLOCK_ZONE_MAP_PRUNE */"
COPY_STDIN_TIMEOUT_SEC = int(os.environ.get("BENCH_COPY_STDIN_TIMEOUT_SEC", "300"))
SQL_BLOCK_INDEX_PREFIX_DISCOVERY: Dict[str, object] = {}
SQL_BLOCK_INDEX_PREFIX_TARGET_HOST: Optional[int] = None
INDEX_TOPK_FRONTIER_DECOY_SSTABLES_OVERRIDE = int(
    os.environ.get("BENCH_INDEX_TOPK_FRONTIER_DECOY_SSTABLES", "0")
)
INDEX_TOPK_FRONTIER_ROWS_PER_SSTABLE_OVERRIDE = int(
    os.environ.get("BENCH_INDEX_TOPK_FRONTIER_ROWS_PER_SSTABLE", "0")
)
INDEX_DISTINCT_ROWS_OVERRIDE = int(os.environ.get("BENCH_INDEX_DISTINCT_ROWS", "0"))
INDEX_DISTINCT_NDV = int(os.environ.get("BENCH_INDEX_DISTINCT_NDV", "100"))
INDEX_DISTINCT_LIMIT = int(os.environ.get("BENCH_INDEX_DISTINCT_LIMIT", "10"))
METRIC_COUNTER_KEYS = (
    "query_count",
    "query_total_us",
    "slow_query_count",
    "row_read_count",
    "row_cache_hit_count",
    "row_write_count",
    "query_result_cache_eligible_count",
    "query_result_cache_hit_count",
    "query_result_cache_miss_count",
    "query_result_cache_stale_count",
    "query_result_cache_insert_count",
    "query_result_cache_invalidation_count",
    "block_cache_hit_count",
    "block_cache_miss_count",
    "block_cache_insert_count",
    "block_cache_insert_bytes",
    "block_cache_fill_skip_count",
    "block_cache_eviction_count",
    "block_cache_eviction_bytes",
    "sstable_block_file_open_count",
    "sstable_block_read_bytes",
    "sstable_open_count",
    "sstable_open_total_us",
    "sstable_open_index_bytes",
    "sstable_open_index_read_us",
    "sstable_open_index_decode_us",
    "sstable_open_filter_bytes",
    "sstable_open_filter_read_us",
    "sstable_open_filter_decode_us",
    "sstable_open_meta_bytes",
    "sstable_open_meta_read_us",
    "sstable_open_meta_decode_us",
    "sstable_open_index_entries",
    "sstable_open_block_property_count",
    "live_sstable_count",
    "sstable_index_cache_hit_count",
    "sstable_index_cache_miss_count",
    "sstable_index_cache_stale_count",
    "sstable_index_cache_invalid_count",
    "sstable_index_cache_write_count",
    "sstable_index_cache_write_error_count",
    "sstable_prefix_filter_check_count",
    "sstable_prefix_filter_positive_count",
    "sstable_prefix_filter_skip_count",
    "sstable_prefix_filter_fail_open_count",
    "sstable_index_prefix_filter_check_count",
    "sstable_index_prefix_filter_positive_count",
    "sstable_index_prefix_filter_skip_count",
    "sstable_index_prefix_filter_fail_open_count",
    "sstable_user_key_filter_check_count",
    "sstable_user_key_filter_positive_count",
    "sstable_user_key_filter_skip_count",
    "sstable_user_key_filter_fail_open_count",
    "sstable_block_prefix_filter_check_count",
    "sstable_block_prefix_filter_positive_count",
    "sstable_block_prefix_filter_skip_count",
    "sstable_block_prefix_filter_fail_open_count",
    "sstable_block_index_prefix_filter_check_count",
    "sstable_block_index_prefix_filter_positive_count",
    "sstable_block_index_prefix_filter_skip_count",
    "sstable_block_index_prefix_filter_fail_open_count",
    "sstable_block_zone_map_filter_check_count",
    "sstable_block_zone_map_filter_positive_count",
    "sstable_block_zone_map_filter_skip_count",
    "sstable_block_zone_map_filter_fail_open_count",
    "sstable_block_zone_map_metadata_bytes",
    "sstable_block_zone_map_mvcc_overlap_fail_open_count",
    "sstable_block_zone_map_mvcc_boundary_split_fail_open_count",
    "sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count",
    "sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count",
    "sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count",
    "sstable_block_zone_map_schema_fail_open_count",
    "sstable_point_probe_count",
    "sstable_point_overlap_skip_count",
    "sstable_range_probe_count",
    "sstable_range_overlap_skip_count",
    "sstable_iterator_open_count",
    "columnar_single_source_aggregate_fast_path_count",
    "sstable_reverse_iterator_open_count",
    "sstable_reverse_block_read_count",
    "sstable_reverse_block_entry_decode_count",
    "sstable_reverse_block_entry_yield_count",
    "sstable_reverse_block_span_scan_count",
    "sstable_reverse_block_span_scan_entry_count",
    "sstable_reverse_block_span_materialize_entry_count",
    "sstable_reverse_seek_sidecar_hit_count",
    "sstable_reverse_seek_sidecar_miss_count",
    "sstable_reverse_seek_sidecar_stale_count",
    "sstable_reverse_seek_sidecar_invalid_count",
    "sstable_reverse_seek_sidecar_write_count",
    "sstable_reverse_seek_sidecar_write_error_count",
    "sstable_reverse_seek_sidecar_use_count",
    "sstable_reverse_seek_sidecar_fail_open_count",
    "sstable_reverse_seek_sidecar_index_entry_count",
    "sstable_reverse_seek_sidecar_entry_materialize_count",
    "sstable_reverse_seek_sidecar_offset_probe_count",
    "fusion_reverse_scan_count",
    "fusion_reverse_source_open_count",
    "fusion_reverse_sstable_frontier_probe_count",
    "fusion_reverse_sstable_frontier_in_range_count",
    "fusion_reverse_sstable_frontier_file_count",
    "fusion_reverse_sstable_frontier_tighten_count",
    "fusion_reverse_sstable_frontier_empty_skip_count",
    "fusion_reverse_sstable_frontier_fail_open_count",
    "fusion_reverse_sstable_pending_count",
    "fusion_reverse_sstable_activation_count",
    "fusion_reverse_sstable_deferred_unopened_count",
    "fusion_reverse_sstable_activation_equal_frontier_count",
    "fusion_reverse_raw_entry_read_count",
    "fusion_reverse_visible_candidate_count",
    "fusion_reverse_visible_put_count",
    "index_key_stream_entry_visit_count",
    "index_ordered_topk_scan_count",
    "index_ordered_topk_entry_visit_count",
    "index_ordered_topk_reverse_scan_count",
    "index_ordered_topk_index_only_row_count",
    "index_ordered_topk_base_row_fetch_count",
    "index_group_count_summary_entry_visit_count",
    "index_loose_seek_count",
    "index_loose_value_count",
    "index_loose_run_skip_count",
    "compaction_run_count",
    "compaction_input_bytes",
    "compaction_output_bytes",
    "compaction_dropped_version_count",
    "sstable_manifest_load_count",
    "sstable_manifest_load_total_us",
    "sstable_manifest_load_error_count",
    "sstable_manifest_live_file_count",
    "sstable_manifest_legacy_scan_count",
    "sstable_manifest_legacy_scan_candidate_count",
    "sstable_manifest_open_error_count",
    "wal_write_count",
    "wal_write_bytes",
    "wal_replay_count",
    "wal_replay_total_us",
    "wal_replay_segment_count",
    "wal_replay_bytes",
    "wal_replay_valid_bytes",
    "wal_replay_last_segment_id",
    "wal_replay_last_valid_offset",
    "wal_replay_entry_count",
    "wal_replay_put_count",
    "wal_replay_delete_count",
    "wal_replay_partial_tail_count",
    "wal_replay_truncate_count",
    "wal_replay_error_count",
    "wal_replay_apply_count",
    "wal_replay_apply_total_us",
    "wal_replay_max_ts",
    "query_sort_fallback_count",
)

# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════
def http_session():
    session = getattr(HTTP_SESSIONS, "session", None)
    if session is None:
        session = requests.Session()
        session.auth = (HTTP_USER, HTTP_PASSWORD)
        HTTP_SESSIONS.session = session
    return session


def metrics_snapshot() -> Optional[Dict[str, int]]:
    try:
        response = http_session().get(METRICS_URL, timeout=3)
        if not (200 <= response.status_code < 300):
            return None
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None
        snapshot: Dict[str, int] = {}
        for key in METRIC_COUNTER_KEYS:
            value = data.get(key)
            if isinstance(value, (int, float)):
                snapshot[key] = int(value)
        return snapshot
    except Exception:
        return None


def metric_delta(before: Optional[Dict[str, int]], after: Optional[Dict[str, int]]) -> Dict[str, int]:
    if not before or not after:
        return {}
    delta: Dict[str, int] = {}
    for key in METRIC_COUNTER_KEYS:
        if key in before and key in after:
            delta[key] = after[key] - before[key]
    return delta


def metric_subset(snapshot: Optional[Dict[str, int]]) -> Dict[str, int]:
    return {key: int(value) for key, value in (snapshot or {}).items()}


def rss_kb(pid: int) -> Optional[int]:
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
    except OSError:
        return None
    return None


def dir_size_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                total += os.path.getsize(file_path)
            except OSError:
                pass
    return total


def list_index_sidecars(data_dir: str) -> List[str]:
    sstable_dir = os.path.join(data_dir, "sstables")
    if not os.path.isdir(sstable_dir):
        return []
    return sorted(
        os.path.join(sstable_dir, name)
        for name in os.listdir(sstable_dir)
        if name.endswith(".idxcache")
    )


def list_reverse_seek_sidecars(data_dir: str) -> List[str]:
    sstable_dir = os.path.join(data_dir, "sstables")
    if not os.path.isdir(sstable_dir):
        return []
    return sorted(
        os.path.join(sstable_dir, name)
        for name in os.listdir(sstable_dir)
        if name.endswith(".rseek")
    )


def remove_reverse_seek_sidecars(data_dir: str) -> Dict[str, int]:
    sidecars = list_reverse_seek_sidecars(data_dir)
    removed = 0
    bytes_removed = 0
    for path in sidecars:
        size = safe_file_size(path)
        try:
            os.remove(path)
            removed += 1
            bytes_removed += size
        except OSError:
            pass
    return {
        "reverse_seek_sidecar_files_before_remove": len(sidecars),
        "reverse_seek_sidecar_files_removed": removed,
        "reverse_seek_sidecar_bytes_removed": bytes_removed,
        "reverse_seek_sidecar_files_after_remove": len(list_reverse_seek_sidecars(data_dir)),
    }


def list_sstable_files(data_dir: str) -> List[str]:
    sstable_dir = os.path.join(data_dir, "sstables")
    if not os.path.isdir(sstable_dir):
        return []

    def sstable_id(path: str) -> int:
        try:
            return int(os.path.splitext(os.path.basename(path))[0])
        except ValueError:
            return -1

    files = [
        os.path.join(sstable_dir, name)
        for name in os.listdir(sstable_dir)
        if name.endswith(".sst")
    ]
    return sorted((path for path in files if sstable_id(path) >= 0), key=sstable_id)


def startup_descriptor_cache_ids(data_dir: str) -> Set[int]:
    cache_path = os.path.join(data_dir, "sstables", "_fusiondb_sstable_descriptor_cache.json")
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
        entries = cache.get("entries") if isinstance(cache, dict) else None
        if not isinstance(entries, dict):
            return set()
        ids: Set[int] = set()
        for key in entries.keys():
            try:
                ids.add(int(key))
            except (TypeError, ValueError):
                continue
        return ids
    except (OSError, json.JSONDecodeError):
        return set()


def run_command_text(args: List[str], cwd: Optional[str] = None, timeout: float = 2.0) -> Optional[str]:
    try:
        completed = subprocess.run(
            args,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def is_sensitive_env_key(key: str) -> bool:
    upper = key.upper()
    sensitive_markers = (
        "PASSWORD",
        "PASSWD",
        "TOKEN",
        "SECRET",
        "CREDENTIAL",
        "COOKIE",
        "PRIVATE",
        "AUTH",
    )
    if any(marker in upper for marker in sensitive_markers):
        return True
    return upper.endswith("_API_KEY") or upper.endswith("_ACCESS_KEY")


def redact_url(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    if not parts.scheme or not parts.netloc:
        return value

    redacted_netloc = parts.netloc
    if parts.username or parts.password:
        try:
            port = f":{parts.port}" if parts.port is not None else ""
        except ValueError:
            port = ""
        host = parts.hostname or ""
        if parts.username:
            redacted_netloc = f"{parts.username}:<redacted>@{host}{port}"
        else:
            redacted_netloc = f"<redacted>@{host}{port}"
    return urlunsplit((parts.scheme, redacted_netloc, parts.path, parts.query, parts.fragment))


def redact_env_value(key: str, value: str) -> str:
    if is_sensitive_env_key(key):
        return "<redacted>"
    if "://" in value:
        return redact_url(value)
    return value


def selected_env_snapshot() -> Dict[str, str]:
    prefixes = ("BENCH_", "FUSIONDB_", "RUST_", "RAYON_", "CARGO_PROFILE_")
    exact = {
        "LD_PRELOAD",
        "MALLOC_CONF",
        "MIMALLOC_VERBOSE",
        "JEMALLOC_SYS_WITH_MALLOC_CONF",
    }
    snapshot: Dict[str, str] = {}
    for key in sorted(os.environ):
        if key.startswith(prefixes) or key in exact:
            snapshot[key] = redact_env_value(key, os.environ.get(key, ""))
    return snapshot


def read_first_cpu_model() -> Optional[str]:
    try:
        with open("/proc/cpuinfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("model name"):
                    _, value = line.split(":", 1)
                    return value.strip()
                if line.startswith("Hardware"):
                    _, value = line.split(":", 1)
                    return value.strip()
    except OSError:
        return None
    return None


def read_mem_total_kib() -> Optional[int]:
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1])
    except (OSError, ValueError):
            return None
    return None


def read_meminfo_snapshot() -> Dict[str, int]:
    fields = {
        "MemTotal",
        "MemFree",
        "MemAvailable",
        "Buffers",
        "Cached",
        "SwapCached",
        "Active(file)",
        "Inactive(file)",
        "SReclaimable",
        "SUnreclaim",
        "Shmem",
    }
    snapshot: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if ":" not in line:
                    continue
                key, rest = line.split(":", 1)
                if key not in fields:
                    continue
                parts = rest.split()
                if parts:
                    snapshot[key] = int(parts[0])
    except (OSError, ValueError):
        return snapshot
    return snapshot


def benchmark_os_cache_control(mode: str, drop_caches_value: str) -> Dict[str, object]:
    normalized = (mode or "none").strip().lower()
    if normalized in ("", "0", "false", "no", "none", "disabled"):
        return {
            "mode": "none",
            "requested": False,
            "executed": False,
            "success": True,
            "os_page_cache_controlled": False,
        }

    result: Dict[str, object] = {
        "mode": normalized,
        "requested": True,
        "executed": False,
        "success": False,
        "os_page_cache_controlled": False,
        "meminfo_before_kib": read_meminfo_snapshot(),
        "kernel_reference": "https://www.kernel.org/doc/Documentation/sysctl/vm.txt",
        "warning": (
            "drop_caches is intended for testing/debugging and affects the host OS page cache; "
            "use only on an isolated benchmark machine"
        ),
    }
    if normalized != "drop_caches":
        result["error"] = f"unsupported BENCH_OS_CACHE_CONTROL={mode!r}; expected none or drop_caches"
        return result
    if platform.system().lower() != "linux":
        result["error"] = "drop_caches is only supported on Linux"
        return result
    if drop_caches_value not in ("1", "2", "3"):
        result["error"] = (
            f"unsupported BENCH_OS_DROP_CACHES_VALUE={drop_caches_value!r}; expected 1, 2, or 3"
        )
        return result
    drop_caches_path = "/proc/sys/vm/drop_caches"
    if not os.path.exists(drop_caches_path):
        result["error"] = f"{drop_caches_path} does not exist"
        return result
    result["effective_uid"] = os.geteuid() if hasattr(os, "geteuid") else None

    try:
        sync_completed = subprocess.run(
            ["sync"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
            check=False,
        )
        result["sync_returncode"] = sync_completed.returncode
        if sync_completed.stderr:
            result["sync_stderr"] = sync_completed.stderr.strip()[:500]
    except (OSError, subprocess.TimeoutExpired) as exc:
        result["error"] = f"sync failed before drop_caches: {exc}"
        return result

    try:
        with open(drop_caches_path, "w", encoding="utf-8") as f:
            f.write(f"{drop_caches_value}\n")
    except OSError as exc:
        result["error"] = f"failed to write {drop_caches_path}: {exc}"
        result["meminfo_after_kib"] = read_meminfo_snapshot()
        return result

    result.update({
        "executed": True,
        "success": True,
        "drop_caches_value": int(drop_caches_value),
        "os_page_cache_controlled": True,
        "meminfo_after_kib": read_meminfo_snapshot(),
    })
    return result


def count_files_in_dir(path: str) -> int:
    total = 0
    for _, _, files in os.walk(path):
        total += len(files)
    return total


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def repo_root_disclosure() -> str:
    return run_command_text(["git", "rev-parse", "--show-toplevel"], timeout=2.0) or os.getcwd()


def cargo_package_metadata(repo_root: str) -> Dict[str, Optional[str]]:
    cargo_toml = os.path.join(repo_root, "Cargo.toml")
    metadata: Dict[str, Optional[str]] = {"name": None, "version": None}
    in_package = False
    try:
        with open(cargo_toml, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if line == "[package]":
                    in_package = True
                    continue
                if in_package and line.startswith("["):
                    break
                if not in_package or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if key in metadata:
                    metadata[key] = value.strip().strip('"')
    except OSError:
        pass
    return metadata


def git_disclosure(repo_root: str) -> Dict[str, object]:
    commit = run_command_text(["git", "rev-parse", "HEAD"], cwd=repo_root, timeout=2.0)
    branch = run_command_text(["git", "branch", "--show-current"], cwd=repo_root, timeout=2.0)
    describe = run_command_text(["git", "describe", "--always", "--dirty"], cwd=repo_root, timeout=2.0)
    status = run_command_text(["git", "status", "--porcelain"], cwd=repo_root, timeout=5.0)
    diff_shortstat = run_command_text(["git", "diff", "--shortstat"], cwd=repo_root, timeout=5.0)
    cached_shortstat = run_command_text(["git", "diff", "--cached", "--shortstat"], cwd=repo_root, timeout=5.0)

    status_lines = status.splitlines() if status else []
    return {
        "available": commit is not None,
        "repo_root": repo_root,
        "commit": commit,
        "short_commit": commit[:12] if commit else None,
        "branch": branch or None,
        "describe": describe or None,
        "dirty": bool(status_lines),
        "dirty_file_count": len(status_lines),
        "untracked_file_count": sum(1 for line in status_lines if line.startswith("??")),
        "status_sample": status_lines[:25],
        "diff_shortstat": diff_shortstat or "",
        "cached_diff_shortstat": cached_shortstat or "",
    }


def infer_rust_profile(binary_hint: str) -> Optional[str]:
    normalized = binary_hint.replace("\\", "/")
    if "/release/" in normalized:
        return "release"
    if "/debug/" in normalized:
        return "debug"
    return os.environ.get("BENCH_BUILD_PROFILE")


def rust_disclosure() -> Dict[str, object]:
    binary_hint = os.environ.get("BENCH_FUSIONDB_BINARY", "").strip() or SST_STARTUP_BINARY
    binary_mtime_utc = None
    if binary_hint and os.path.exists(binary_hint):
        try:
            binary_mtime_utc = (
                datetime.fromtimestamp(os.path.getmtime(binary_hint), timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except OSError:
            binary_mtime_utc = None
    return {
        "rustc": run_command_text(["rustc", "--version"], timeout=2.0),
        "cargo": run_command_text(["cargo", "--version"], timeout=2.0),
        "build_profile": infer_rust_profile(binary_hint),
        "fusiondb_binary_hint": binary_hint,
        "fusiondb_binary_exists": os.path.exists(binary_hint) if binary_hint else False,
        "fusiondb_binary_mtime_utc": binary_mtime_utc,
    }


def system_disclosure() -> Dict[str, object]:
    mem_total_kib = read_mem_total_kib()
    load_average = None
    if hasattr(os, "getloadavg"):
        try:
            load_average = list(os.getloadavg())
        except OSError:
            load_average = None
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or read_first_cpu_model(),
        "cpu_count": os.cpu_count(),
        "mem_total_kib": mem_total_kib,
        "mem_total_bytes": mem_total_kib * 1024 if mem_total_kib is not None else None,
        "load_average": load_average,
    }


def configured_data_dir_hint() -> Tuple[Optional[str], Optional[str]]:
    if BENCH_DISCLOSURE_DATA_DIR:
        return BENCH_DISCLOSURE_DATA_DIR, "BENCH_DISCLOSURE_DATA_DIR"
    for key in ("BENCH_DISCLOSURE_DATA_DIR", "FUSIONDB_DATA_DIR"):
        value = os.environ.get(key, "").strip()
        if value:
            return value, key
    return None, None


def safe_file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def data_dir_disclosure() -> Dict[str, object]:
    data_dir, source = configured_data_dir_hint()
    if not data_dir:
        return {
            "provided": False,
            "path": None,
            "source": None,
            "note": "Set BENCH_DISCLOSURE_DATA_DIR to record the benchmark server data-dir size.",
        }

    abs_path = os.path.abspath(data_dir)
    exists = os.path.isdir(abs_path)
    sstable_files = list_sstable_files(abs_path) if exists else []
    sidecars = list_index_sidecars(abs_path) if exists else []
    reverse_seek_sidecars = list_reverse_seek_sidecars(abs_path) if exists else []
    disk_usage = shutil.disk_usage(abs_path) if exists else None
    return {
        "provided": True,
        "source": source,
        "path": abs_path,
        "exists": exists,
        "size_bytes": dir_size_bytes(abs_path) if exists else None,
        "size_collected_at_utc": utc_now_iso() if exists else None,
        "file_count": count_files_in_dir(abs_path) if exists else None,
        "sstable_file_count": len(sstable_files),
        "sstable_bytes": sum(safe_file_size(path) for path in sstable_files),
        "index_sidecar_count": len(sidecars),
        "index_sidecar_bytes": sum(safe_file_size(path) for path in sidecars),
        "reverse_seek_sidecar_count": len(reverse_seek_sidecars),
        "reverse_seek_sidecar_bytes": sum(
            safe_file_size(path) for path in reverse_seek_sidecars
        ),
        "descriptor_cache_entry_count": len(startup_descriptor_cache_ids(abs_path)) if exists else None,
        "disk_total_bytes": disk_usage.total if disk_usage else None,
        "disk_free_bytes": disk_usage.free if disk_usage else None,
    }


def cache_disclosure(env_snapshot: Dict[str, str]) -> Dict[str, object]:
    cache_env = {key: value for key, value in env_snapshot.items() if "CACHE" in key}
    cache_metric_keys = [key for key in METRIC_COUNTER_KEYS if "cache" in key]
    return {
        "env": cache_env,
        "metric_counter_keys": cache_metric_keys,
        "part20_claim_policy": "BENCH_CLAIM_MODE requires Part 20 ordered Top-K observations to have zero query-result cache activity.",
    }


def selected_part_keys_from_selection(selection: Dict[str, object]) -> Set[str]:
    selected_parts = selection.get("selected_parts")
    if not isinstance(selected_parts, list):
        return set()
    keys: Set[str] = set()
    for part in selected_parts:
        if isinstance(part, dict):
            key = part.get("key")
            if isinstance(key, str) and key:
                keys.add(key)
    return keys


def first_result_metadata(all_results: Optional[List[object]], matrix: str) -> Dict[str, object]:
    if not all_results:
        return {}
    for result in all_results:
        metadata = getattr(result, "metadata", None)
        if isinstance(metadata, dict) and metadata.get("matrix") == matrix:
            return metadata
    return {}


def optional_str(value: object) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def benchmark_owned_server_disclosure(
    selection: Dict[str, object],
    timings: Optional[Dict[str, object]],
    all_results: Optional[List[object]],
) -> Dict[str, object]:
    timings = timings or {}
    selected_keys = selected_part_keys_from_selection(selection)
    restart_active = "index_topk_restart" in selected_keys or bool(
        timings.get("index_topk_restart_owned_server")
    )
    zone_map_metadata = first_result_metadata(all_results, "sql_block_zone_map_prune")
    zone_map_active = "sql_block_zone_map_prune" in selected_keys and bool(
        timings.get("sql_block_zone_map_owned_server")
        or zone_map_metadata.get("benchmark_owned_server")
    )
    if zone_map_active and not restart_active:
        port = (
            zone_map_metadata.get("http_port")
            or timings.get("sql_block_zone_map_port")
            or SQL_BLOCK_ZONE_MAP_PORT
        )
        query_url = (
            optional_str(zone_map_metadata.get("query_url"))
            or f"http://127.0.0.1:{port}/query"
        )
        metrics_url = (
            optional_str(zone_map_metadata.get("metrics_url"))
            or f"http://127.0.0.1:{port}/metrics"
        )
        checkpoint_url = (
            optional_str(zone_map_metadata.get("checkpoint_url"))
            or f"http://127.0.0.1:{port}/checkpoint"
        )
        scenario_data_dir = optional_str(zone_map_metadata.get("scenario_data_dir")) or optional_str(
            timings.get("sql_block_zone_map_data_dir")
        )
        scenario_workdir = optional_str(zone_map_metadata.get("scenario_workdir")) or optional_str(
            timings.get("sql_block_zone_map_workdir")
        )
        return {
            "active": True,
            "matrix": "sql_block_zone_map_prune",
            "scope": "Part 31 SQL Block Zone-Map SSTable Pruning",
            "protocol": "http",
            "query_url": redact_url(query_url),
            "metrics_url": redact_url(metrics_url),
            "checkpoint_url": redact_url(checkpoint_url),
            "http_port": port,
            "binary": optional_str(zone_map_metadata.get("binary"))
            or optional_str(timings.get("sql_block_zone_map_binary"))
            or SQL_BLOCK_ZONE_MAP_BINARY,
            "workdir": scenario_workdir,
            "data_dir": {
                "path": scenario_data_dir,
                "exists_at_report_time": os.path.isdir(scenario_data_dir)
                if scenario_data_dir
                else None,
                "bytes_after_load": zone_map_metadata.get("data_dir_bytes_after_load")
                or timings.get("sql_block_zone_map_data_dir_bytes_after_load"),
                "sstable_files_after_load": zone_map_metadata.get("sstable_files_after_load")
                or timings.get("sql_block_zone_map_sstable_files_after_load"),
            },
            "memtable_flush_mb": zone_map_metadata.get("memtable_flush_mb")
            or timings.get("sql_block_zone_map_memtable_flush_mb"),
            "timeout_sec": zone_map_metadata.get("timeout_sec")
            or timings.get("sql_block_zone_map_timeout_sec"),
            "lifecycle": (
                "benchmark starts an owned high-memtable FusionDB process before Part 31 setup, "
                "loads zone-map tables, checkpoints during setup, measures claim rows, then stops "
                "the owned process"
            ),
            "disclosure_note": (
                "Part 31 owned-server mode wraps setup and measurement, so the top-level "
                "server.base_url can also identify this owned server in the saved report"
            ),
        }
    if not restart_active:
        return {"active": False}

    metadata = first_result_metadata(all_results, "index_topk_restart")
    port = metadata.get("http_port") or timings.get("index_topk_restart_port") or INDEX_TOPK_RESTART_PORT
    query_url = optional_str(metadata.get("query_url")) or f"http://127.0.0.1:{port}/query"
    metrics_url = optional_str(metadata.get("metrics_url")) or f"http://127.0.0.1:{port}/metrics"
    checkpoint_url = optional_str(metadata.get("checkpoint_url")) or f"http://127.0.0.1:{port}/checkpoint"
    scenario_data_dir = optional_str(metadata.get("scenario_data_dir"))
    scenario_workdir = optional_str(metadata.get("scenario_workdir"))
    return {
        "active": True,
        "matrix": "index_topk_restart",
        "scope": "Part 23 Indexed Top-K Restart Phase",
        "protocol": "http",
        "query_url": redact_url(query_url),
        "metrics_url": redact_url(metrics_url),
        "checkpoint_url": redact_url(checkpoint_url),
        "http_port": port,
        "restart_trials_requested": metadata.get("restart_trials_requested")
        or timings.get("index_topk_restart_trials"),
        "trial_number_sample": metadata.get("trial_number"),
        "rseek_ab_enabled": metadata.get("rseek_ab_enabled"),
        "rseek_ab_variants": metadata.get("rseek_ab_variants"),
        "rseek_ab_fallback": metadata.get("rseek_ab_fallback"),
        "binary": optional_str(metadata.get("binary"))
        or optional_str(timings.get("index_topk_restart_binary"))
        or INDEX_TOPK_RESTART_BINARY,
        "workdir": scenario_workdir,
        "data_dir": {
            "path": scenario_data_dir,
            "exists_at_report_time": os.path.isdir(scenario_data_dir) if scenario_data_dir else None,
            "bytes_after_load": metadata.get("data_dir_bytes_after_load"),
            "bytes_after_restart": metadata.get("data_dir_bytes_after_restart"),
            "sstable_files_after_load": metadata.get("sstable_files_after_load"),
            "sstable_files_after_restart": metadata.get("sstable_files_after_restart"),
            "index_sidecar_files_after_load": metadata.get("index_sidecar_files_after_load"),
            "reverse_seek_sidecar_files_after_load": metadata.get(
                "reverse_seek_sidecar_files_after_load"
            ),
            "reverse_seek_sidecar_bytes_after_load": metadata.get(
                "reverse_seek_sidecar_bytes_after_load"
            ),
            "reverse_seek_sidecar_files_after_restart": metadata.get(
                "reverse_seek_sidecar_files_after_restart"
            ),
            "reverse_seek_sidecar_bytes_after_restart": metadata.get(
                "reverse_seek_sidecar_bytes_after_restart"
            ),
        },
        "lifecycle": (
            "benchmark starts an owned FusionDB process, loads Part 20 data, checkpoints, "
            "stops the load process, restarts a fresh process before each measured case, "
            "records restart-first-pass and restart-warm rows, then stops each restart process"
        ),
        "restart_case_policy": metadata.get("restart_case_policy"),
        "shared_data_dir_reused_across_trials": metadata.get("shared_data_dir_reused_across_trials"),
        "process_cache_state": metadata.get("process_cache_state"),
        "os_page_cache_state": metadata.get("os_page_cache_state"),
        "os_cache_control": metadata.get("os_cache_control"),
        "disclosure_note": (
            "top-level server.base_url remains the restored external benchmark configuration; "
            "this object records the server that actually served benchmark-owned restart rows"
        ),
    }


def benchmark_environment_disclosure(
    selection: Dict[str, object],
    timings: Optional[Dict[str, object]] = None,
    all_results: Optional[List[object]] = None,
) -> Dict[str, object]:
    repo_root = repo_root_disclosure()
    env_snapshot = selected_env_snapshot()
    cargo_metadata = cargo_package_metadata(repo_root)
    owned_server = benchmark_owned_server_disclosure(selection, timings, all_results)
    server = {
        "protocol": PROTO,
        "base_url": redact_url(BASE_URL),
        "health_url": redact_url(HEALTH_URL),
        "metrics_url": redact_url(METRICS_URL),
        "checkpoint_url": redact_url(CHECKPOINT_URL),
        "copy_stdin_url": redact_url(COPY_STDIN_URL),
        "pg_host": PG_HOST,
        "pg_port": PG_PORT,
        "pg_user": PG_USER,
        "pg_dbname": PG_DBNAME,
        "benchmark_owned": owned_server,
    }
    if owned_server.get("active"):
        if owned_server.get("matrix") == "sql_block_zone_map_prune":
            server["base_url_role"] = "benchmark_owned_configuration_at_report_time"
        else:
            server["base_url_role"] = "restored_external_configuration_at_report_time"
    return {
        "schema_version": 1,
        "status": "unaudited_non_official",
        "generated_at_utc": utc_now_iso(),
        "source": {
            "cargo_package_name": cargo_metadata.get("name"),
            "cargo_package_version": cargo_metadata.get("version"),
        },
        "benchmark_client": {
            "cwd": os.getcwd(),
            "python_version": sys.version,
            "python_executable": sys.executable,
            "argv": sys.argv,
            "pid": os.getpid(),
            "timestamp_local": datetime.now().isoformat(),
        },
        "server": server,
        "selection": {
            "source": selection.get("source"),
            "matrix": selection.get("matrix"),
            "parts_env": selection.get("parts_env"),
            "slug": selection.get("slug"),
        },
        "git": git_disclosure(repo_root),
        "rust": rust_disclosure(),
        "system": system_disclosure(),
        "data_dir": data_dir_disclosure(),
        "cache": cache_disclosure(env_snapshot),
        "selected_env": env_snapshot,
        "privacy": {
            "secret_policy": "selected env vars and URLs are redacted when names or URL userinfo look sensitive",
            "path_policy": "absolute local paths are included for reproducibility",
            "status": "review report before publishing outside the project",
        },
    }


def startup_cache_entries(data_dir: str, file_name: str) -> Dict[str, object]:
    cache_path = os.path.join(data_dir, "sstables", file_name)
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
        entries = cache.get("entries") if isinstance(cache, dict) else None
        if isinstance(entries, dict):
            return entries
    except (OSError, json.JSONDecodeError):
        pass
    return {}


def manifest_fingerprint_from_stat(path: str) -> Dict[str, int]:
    stat = os.stat(path)
    mtime_ns = stat.st_mtime_ns
    return {
        "file_len": stat.st_size,
        "modified_unix_secs": mtime_ns // 1_000_000_000,
        "modified_subsec_nanos": mtime_ns % 1_000_000_000,
    }


def manifest_fingerprint_matches(left: object, right: Dict[str, int]) -> bool:
    if not isinstance(left, dict):
        return False
    return (
        int(left.get("file_len", -1)) == right["file_len"]
        and int(left.get("modified_unix_secs", -1)) == right["modified_unix_secs"]
        and int(left.get("modified_subsec_nanos", -1)) == right["modified_subsec_nanos"]
    )


def rotl64(value: int, bits: int) -> int:
    value &= 0xFFFF_FFFF_FFFF_FFFF
    return ((value << bits) | (value >> (64 - bits))) & 0xFFFF_FFFF_FFFF_FFFF


def manifest_content_fingerprint(fingerprint: Dict[str, int], max_ts: int) -> int:
    return (
        int(fingerprint["file_len"])
        ^ rotl64(int(fingerprint["modified_unix_secs"]), 13)
        ^ rotl64(int(fingerprint["modified_subsec_nanos"]), 29)
        ^ rotl64(int(max_ts), 41)
    ) & 0xFFFF_FFFF_FFFF_FFFF


def startup_v2_manifest_entries(data_dir: str) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    descriptors = startup_cache_entries(data_dir, "_fusiondb_sstable_descriptor_cache.json")
    timestamps = startup_cache_entries(data_dir, "_fusiondb_sstable_ts_cache.json")
    entries: List[Dict[str, object]] = []
    skipped_missing_descriptor = 0
    skipped_stale_descriptor = 0
    skipped_stale_timestamp = 0

    for path in list_sstable_files(data_dir):
        file_name = os.path.basename(path)
        sstable_id = int(os.path.splitext(file_name)[0])
        fingerprint = manifest_fingerprint_from_stat(path)
        descriptor = descriptors.get(str(sstable_id))
        if not isinstance(descriptor, dict):
            skipped_missing_descriptor += 1
            continue
        if not manifest_fingerprint_matches(descriptor.get("fingerprint"), fingerprint):
            skipped_stale_descriptor += 1
            continue

        ts_entry = timestamps.get(str(sstable_id))
        max_ts = 0
        if isinstance(ts_entry, dict):
            if manifest_fingerprint_matches(ts_entry.get("fingerprint"), fingerprint):
                max_ts = int(ts_entry.get("max_ts", 0) or 0)
            else:
                skipped_stale_timestamp += 1

        entries.append({
            "id": sstable_id,
            "file_name": file_name,
            "fingerprint": fingerprint,
            "first_key": bytes(int(byte) & 0xFF for byte in descriptor.get("first_key", [])),
            "last_key": bytes(int(byte) & 0xFF for byte in descriptor.get("last_key", [])),
            "format_version": int(descriptor.get("format_version", 0) or 0),
            "max_ts": max_ts,
            "content_fingerprint": manifest_content_fingerprint(fingerprint, max_ts),
        })

    entries.sort(key=lambda entry: int(entry["id"]))
    return entries, {
        "manifest_v2_descriptor_cache_entries": len(descriptors),
        "manifest_v2_timestamp_cache_entries": len(timestamps),
        "manifest_v2_skipped_missing_descriptor": skipped_missing_descriptor,
        "manifest_v2_skipped_stale_descriptor": skipped_stale_descriptor,
        "manifest_v2_skipped_stale_timestamp": skipped_stale_timestamp,
    }


CRC32C_POLY_REVERSED = 0x82F6_3B78
CRC32C_MASK_DELTA = 0xA282_EAD8
MANIFEST_RECORD_BLOCK_SIZE = 32 * 1024
MANIFEST_RECORD_HEADER_SIZE = 7


def crc32c_extend(crc: int, payload: bytes) -> int:
    crc &= 0xFFFF_FFFF
    for byte in payload:
        crc ^= byte
        for _ in range(8):
            mask = -(crc & 1) & 0xFFFF_FFFF
            crc = ((crc >> 1) ^ (CRC32C_POLY_REVERSED & mask)) & 0xFFFF_FFFF
    return crc


def manifest_record_crc32c(record_type: int, fragment: bytes) -> int:
    crc = crc32c_extend(0xFFFF_FFFF, bytes([record_type]))
    return (~crc32c_extend(crc, fragment)) & 0xFFFF_FFFF


def mask_crc32c(crc: int) -> int:
    rotated = ((crc >> 15) | ((crc << 17) & 0xFFFF_FFFF)) & 0xFFFF_FFFF
    return (rotated + CRC32C_MASK_DELTA) & 0xFFFF_FFFF


def manifest_physical_record(record_type: int, fragment: bytes) -> bytes:
    checksum = mask_crc32c(manifest_record_crc32c(record_type, fragment))
    return struct.pack("<IHB", checksum, len(fragment), record_type) + fragment


def manifest_record_file_bytes(payloads: List[bytes]) -> bytes:
    out = bytearray()
    block_offset = 0
    for payload in payloads:
        remaining = payload
        begin = True
        if not remaining:
            out.extend(manifest_physical_record(1, b""))
            block_offset += MANIFEST_RECORD_HEADER_SIZE
            if block_offset == MANIFEST_RECORD_BLOCK_SIZE:
                block_offset = 0
            continue
        while remaining:
            block_remaining = MANIFEST_RECORD_BLOCK_SIZE - block_offset
            if block_remaining < MANIFEST_RECORD_HEADER_SIZE:
                out.extend(b"\x00" * block_remaining)
                block_offset = 0
                continue
            if block_remaining == MANIFEST_RECORD_HEADER_SIZE:
                out.extend(manifest_physical_record(2, b""))
                block_offset = 0
                begin = False
                continue
            available = MANIFEST_RECORD_BLOCK_SIZE - block_offset - MANIFEST_RECORD_HEADER_SIZE
            fragment = remaining[:available]
            remaining = remaining[available:]
            end = not remaining
            if begin and end:
                record_type = 1
            elif begin:
                record_type = 2
            elif end:
                record_type = 4
            else:
                record_type = 3
            out.extend(manifest_physical_record(record_type, fragment))
            block_offset += MANIFEST_RECORD_HEADER_SIZE + len(fragment)
            if block_offset == MANIFEST_RECORD_BLOCK_SIZE:
                block_offset = 0
            begin = False
    return bytes(out)


def put_u8(out: bytearray, value: int) -> None:
    out.extend(struct.pack("<B", value))


def put_u16(out: bytearray, value: int) -> None:
    out.extend(struct.pack("<H", value))


def put_u32(out: bytearray, value: int) -> None:
    out.extend(struct.pack("<I", value))


def put_u64(out: bytearray, value: int) -> None:
    out.extend(struct.pack("<Q", value & 0xFFFF_FFFF_FFFF_FFFF))


def put_bytes(out: bytearray, value: bytes) -> None:
    put_u32(out, len(value))
    out.extend(value)


def put_manifest_entry(out: bytearray, entry: Dict[str, object]) -> None:
    put_u64(out, int(entry["id"]))
    put_bytes(out, str(entry["file_name"]).encode("utf-8"))
    fingerprint = entry["fingerprint"]
    assert isinstance(fingerprint, dict)
    put_u64(out, int(fingerprint["file_len"]))
    put_u64(out, int(fingerprint["modified_unix_secs"]))
    put_u32(out, int(fingerprint["modified_subsec_nanos"]))
    put_bytes(out, bytes(entry["first_key"]))
    put_bytes(out, bytes(entry["last_key"]))
    put_u32(out, int(entry["format_version"]))
    put_u64(out, int(entry["max_ts"]))
    put_u64(out, int(entry["content_fingerprint"]))


def put_manifest_entries(out: bytearray, entries: List[Dict[str, object]]) -> None:
    put_u32(out, len(entries))
    for entry in entries:
        put_manifest_entry(out, entry)


def manifest_snapshot_payload(entries: List[Dict[str, object]]) -> bytes:
    out = bytearray(b"FMED")
    put_u16(out, 1)
    put_u8(out, 1)
    next_file_number = max((int(entry["id"]) + 1 for entry in entries), default=1)
    high_watermark = max((int(entry["max_ts"]) for entry in entries), default=0)
    put_u64(out, next_file_number)
    put_u64(out, high_watermark)
    put_u8(out, 0)
    put_manifest_entries(out, entries)
    return bytes(out)


def manifest_version_edit_payload(
    delete_ids: List[int],
    add_files: List[Dict[str, object]],
    next_file_number: int,
    high_watermark: int,
) -> bytes:
    out = bytearray(b"FMED")
    put_u16(out, 1)
    put_u8(out, 8)
    put_u32(out, len(delete_ids))
    for sstable_id in delete_ids:
        put_u64(out, sstable_id)
    put_manifest_entries(out, add_files)
    put_u8(out, 1)
    put_u64(out, next_file_number)
    put_u8(out, 1)
    put_u64(out, high_watermark)
    put_u8(out, 0)
    return bytes(out)


def write_v2_startup_manifest(data_dir: str, mode: str) -> Dict[str, object]:
    sstable_dir = os.path.join(data_dir, "sstables")
    os.makedirs(sstable_dir, exist_ok=True)
    entries, details = startup_v2_manifest_entries(data_dir)
    payloads: List[bytes]
    edit_count = 1
    if mode == "many_edits":
        payloads = [manifest_snapshot_payload([])]
        next_file_number = 1
        high_watermark = 0
        for entry in entries:
            next_file_number = max(next_file_number, int(entry["id"]) + 1)
            high_watermark = max(high_watermark, int(entry["max_ts"]))
            payloads.append(
                manifest_version_edit_payload([], [entry], next_file_number, high_watermark)
            )
        edit_count = len(payloads)
    else:
        payloads = [manifest_snapshot_payload(entries)]

    manifest_bytes = manifest_record_file_bytes(payloads)
    manifest_path = os.path.join(sstable_dir, "MANIFEST-000001")
    current_path = os.path.join(sstable_dir, "CURRENT")
    with open(manifest_path, "wb") as f:
        f.write(manifest_bytes)
    if mode == "torn_tail_rollover":
        with open(manifest_path, "ab") as f:
            f.write(b"\x99\x88\x77")
    with open(current_path, "w", encoding="utf-8") as f:
        f.write("MANIFEST-000001\n")

    details.update({
        "manifest_written": True,
        "manifest_format": "v2",
        "manifest_live_files_written": len(entries),
        "manifest_v2_edit_mode": mode,
        "manifest_v2_record_count_written": edit_count,
        "manifest_v2_bytes_written": len(manifest_bytes),
        "manifest_v2_torn_tail_bytes": 3 if mode == "torn_tail_rollover" else 0,
        "manifest_path": manifest_path,
        "current_path": current_path,
    })
    return details


def write_startup_manifest(data_dir: str) -> Dict[str, object]:
    sstable_dir = os.path.join(data_dir, "sstables")
    os.makedirs(sstable_dir, exist_ok=True)
    descriptor_ids = startup_descriptor_cache_ids(data_dir)
    sstable_files = list_sstable_files(data_dir)
    if descriptor_ids:
        sstable_files = [
            path
            for path in sstable_files
            if int(os.path.splitext(os.path.basename(path))[0]) in descriptor_ids
        ]
    files = []
    for path in sstable_files:
        stat = os.stat(path)
        file_name = os.path.basename(path)
        sstable_id = int(os.path.splitext(file_name)[0])
        mtime_ns = stat.st_mtime_ns
        files.append({
            "id": sstable_id,
            "file_name": file_name,
            "fingerprint": {
                "file_len": stat.st_size,
                "modified_unix_secs": mtime_ns // 1_000_000_000,
                "modified_subsec_nanos": mtime_ns % 1_000_000_000,
            },
        })

    manifest_path = os.path.join(sstable_dir, "MANIFEST-000001")
    current_path = os.path.join(sstable_dir, "CURRENT")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({"version": 1, "files": files}, f, indent=2)
        f.write("\n")
    with open(current_path, "w", encoding="utf-8") as f:
        f.write("MANIFEST-000001\n")
    return {
        "manifest_written": True,
        "manifest_format": "legacy_json",
        "manifest_live_files_written": len(files),
        "manifest_seeded_from_descriptor_cache": bool(descriptor_ids),
        "manifest_descriptor_cache_ids": len(descriptor_ids),
        "manifest_path": manifest_path,
        "current_path": current_path,
    }


def create_startup_orphan_sstables(data_dir: str, count: int) -> Dict[str, object]:
    sstables = list_sstable_files(data_dir)
    details: Dict[str, object] = {
        "orphan_sstable_files_requested": count,
        "orphan_sstable_files_created": 0,
    }
    if not sstables or count <= 0:
        return details

    sstable_dir = os.path.dirname(sstables[0])
    existing_ids = {
        int(os.path.splitext(os.path.basename(path))[0])
        for path in sstables
    }
    next_id = max(existing_ids) + 10_000
    created = 0
    source = sstables[0]
    for _ in range(count):
        while next_id in existing_ids:
            next_id += 1
        dst = os.path.join(sstable_dir, f"{next_id}.sst")
        try:
            try:
                os.link(source, dst)
            except OSError:
                shutil.copy2(source, dst)
            existing_ids.add(next_id)
            created += 1
            next_id += 1
        except OSError:
            break
    details["orphan_sstable_files_created"] = created
    details["sstable_files_after_orphans"] = len(list_sstable_files(data_dir))
    return details


def encode_fusion_internal_key(user_key: bytes, ts: int) -> bytes:
    return user_key + ((1 << 64) - 1 - ts).to_bytes(8, "big")


def encode_fusion_value(is_put: bool, value: bytes) -> bytes:
    return (b"\x01" if is_put else b"\x00") + value


def append_fusion_wal_put(path: str, key: bytes, value: bytes) -> int:
    record = bytearray()
    record.append(1)
    record.extend(len(key).to_bytes(4, "little"))
    record.extend(key)
    record.extend(len(value).to_bytes(4, "little"))
    record.extend(value)
    with open(path, "ab") as f:
        f.write(record)
    return len(record)


def prepare_startup_dirty_wal(data_dir: str, scenario: str) -> Dict[str, object]:
    details: Dict[str, object] = {
        "dirty_wal_scenario": scenario == "dirty_wal_manifest",
        "dirty_wal_entries_requested": 0,
        "dirty_wal_entries_written": 0,
        "dirty_wal_bytes_written": 0,
    }
    if scenario != "dirty_wal_manifest":
        return details

    wal_path = os.path.join(data_dir, "fusion.wal")
    base_ts = 9_000_000_000
    bytes_written = 0
    for index in range(max(0, SST_STARTUP_DIRTY_WAL_ENTRIES)):
        key = encode_fusion_internal_key(
            f"data:dirty_wal:{index:08}".encode("utf-8"),
            base_ts + index,
        )
        value = encode_fusion_value(True, f"dirty-value-{index}".encode("utf-8"))
        bytes_written += append_fusion_wal_put(wal_path, key, value)

    details.update({
        "dirty_wal_entries_requested": SST_STARTUP_DIRTY_WAL_ENTRIES,
        "dirty_wal_entries_written": max(0, SST_STARTUP_DIRTY_WAL_ENTRIES),
        "dirty_wal_bytes_written": bytes_written,
        "dirty_wal_path": wal_path,
        "dirty_wal_base_ts": base_ts,
    })
    return details


def copy_startup_data(src_dir: str, dst_dir: str) -> None:
    def copy_file(src: str, dst: str) -> str:
        if src.endswith(".sst"):
            try:
                os.link(src, dst)
                return dst
            except OSError:
                pass
        return shutil.copy2(src, dst)

    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)
    if os.path.exists(src_dir):
        shutil.copytree(src_dir, dst_dir, copy_function=copy_file)
    else:
        os.makedirs(os.path.join(dst_dir, "sstables"), exist_ok=True)


def prepare_startup_sidecars(data_dir: str, scenario: str) -> Dict[str, object]:
    sidecars = list_index_sidecars(data_dir)
    details: Dict[str, object] = {
        "scenario": scenario,
        "sidecar_files_before": len(sidecars),
        "sidecar_bytes_before": sum(os.path.getsize(path) for path in sidecars if os.path.exists(path)),
    }

    if scenario in ("warm_sidecar", "warm_manifest", "orphan_manifest", "dirty_wal_manifest"):
        return details
    if scenario == "no_sidecar":
        removed = 0
        for path in sidecars:
            try:
                os.remove(path)
                removed += 1
            except OSError:
                pass
        details["sidecar_files_removed"] = removed
        return details
    if scenario == "stale_sidecar":
        stale = 0
        for path in sidecars:
            try:
                with open(path, "r+b") as f:
                    f.seek(4)
                    f.write((0).to_bytes(4, "little"))
                stale += 1
            except OSError:
                pass
        details["sidecar_files_marked_stale"] = stale
        return details
    if scenario == "corrupt_sidecar":
        corrupted = 0
        for path in sidecars:
            try:
                size = os.path.getsize(path)
                if size <= 64:
                    continue
                with open(path, "r+b") as f:
                    f.seek(size - 1)
                    current = f.read(1)
                    if not current:
                        continue
                    f.seek(size - 1)
                    f.write(bytes([current[0] ^ 0x01]))
                corrupted += 1
            except OSError:
                pass
        details["sidecar_files_corrupted"] = corrupted
        return details

    details["unknown_scenario"] = True
    return details


def prepare_startup_manifest(data_dir: str, scenario: str) -> Dict[str, object]:
    manifest_scenario = scenario in LEGACY_MANIFEST_SCENARIOS or scenario in V2_MANIFEST_SCENARIOS
    details: Dict[str, object] = {
        "manifest_scenario": manifest_scenario,
        "manifest_v2_scenario": scenario in V2_MANIFEST_SCENARIOS,
        "sstable_files_before_manifest_prepare": len(list_sstable_files(data_dir)),
    }
    if not manifest_scenario:
        details["current_file_before_startup"] = os.path.exists(
            os.path.join(data_dir, "sstables", "CURRENT")
        )
        return details

    if scenario in V2_MANIFEST_SCENARIOS:
        v2_mode = {
            "v2_many_edits": "many_edits",
            "v2_torn_tail_rollover": "torn_tail_rollover",
        }.get(scenario, "snapshot")
        details.update(write_v2_startup_manifest(data_dir, v2_mode))
    else:
        details.update(write_startup_manifest(data_dir))

    if scenario in ("orphan_manifest", "v2_orphan_manifest"):
        details.update(create_startup_orphan_sstables(data_dir, SST_STARTUP_ORPHAN_COUNT))
    details["sstable_files_after_manifest_prepare"] = len(list_sstable_files(data_dir))
    return details


def write_startup_config(
    workdir: str,
    data_dir: str,
    http_port: int,
    *,
    row_cache_capacity: int = 10000,
    statement_cache_capacity: int = 1000,
    block_cache_capacity: int = 25000,
    memtable_flush_mb: int = 32,
    sql_bulk_scan_no_fill: bool = True,
) -> None:
    pg_port = http_port + 1
    config = f"""[server]
http_port = {http_port}
pg_port = {pg_port}
redis_enabled = false
redis_port = {pg_port + 1}
bind = "127.0.0.1"
max_connections = 100

[storage]
data_dir = {json.dumps(data_dir)}
wal_file = "fusion.wal"
sstable_dir = "sstables"
memtable_flush_mb = {memtable_flush_mb}
row_cache_capacity = {row_cache_capacity}
statement_cache_capacity = {statement_cache_capacity}
block_cache_capacity = {block_cache_capacity}
sql_bulk_scan_no_fill = {str(sql_bulk_scan_no_fill).lower()}
slow_query_threshold_ms = 100

[auth]
password = "fusiondb"
scram_sha256 = false
http_legacy_unsafe = false

[distributed]
enabled = false
node_id = 1
advertise_addr = ""
bootstrap = false
cluster_name = "fusiondb_bench"
forwarding_secret = ""
initial_members = []

[distributed.sharding]
enabled = false
strategy = "hash"
shard_count = 16
range_boundaries = []
"""
    with open(os.path.join(workdir, "fusiondb.toml"), "w", encoding="utf-8") as f:
        f.write(config)


def local_metrics_snapshot(metrics_url: str) -> Optional[Dict[str, int]]:
    try:
        response = requests.get(
            metrics_url,
            timeout=3,
            auth=(HTTP_USER, HTTP_PASSWORD),
        )
        if not (200 <= response.status_code < 300):
            return None
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None
        return {
            key: int(value)
            for key, value in data.items()
            if key in METRIC_COUNTER_KEYS and isinstance(value, (int, float))
        }
    except Exception:
        return None


def local_query(query_url: str, query: str) -> Tuple[Dict[str, object], float]:
    try:
        t0 = time.perf_counter()
        response = requests.post(
            query_url,
            json={"sql": query},
            timeout=60,
            auth=(HTTP_USER, HTTP_PASSWORD),
        )
        ms = (time.perf_counter() - t0) * 1000
        payload = response.json()
        if not (200 <= response.status_code < 300):
            return {"status": "error", "data": payload, "error": http_error_message(response, payload)}, ms
        if isinstance(payload, dict) and payload.get("status") == "error":
            return {
                "status": "error",
                "data": payload.get("data"),
                "error": payload_error_message(payload) or "unknown error",
            }, ms
        return payload if isinstance(payload, dict) else {"status": "ok", "data": payload, "error": None}, ms
    except Exception as exc:
        return {"status": "error", "data": None, "error": str(exc)}, 0.0


def stop_fusiondb_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass


def checkpoint_storage(label: str) -> bool:
    try:
        response = http_session().post(CHECKPOINT_URL, timeout=60)
        if 200 <= response.status_code < 300:
            print(f"      checkpoint {label}: ok")
            return True
        print(f"      checkpoint {label}: HTTP {response.status_code}")
        return False
    except Exception as e:
        print(f"      checkpoint {label}: {e}")
        return False


def switch_benchmark_urls(query_url: str, metrics_url: str, checkpoint_url: str) -> Tuple[str, str, str, str, str]:
    global BASE_URL, HEALTH_URL, METRICS_URL, CHECKPOINT_URL, COPY_STDIN_URL
    previous = (BASE_URL, HEALTH_URL, METRICS_URL, CHECKPOINT_URL, COPY_STDIN_URL)
    BASE_URL = query_url
    HEALTH_URL = query_url.replace("/query", "/health")
    METRICS_URL = metrics_url
    CHECKPOINT_URL = checkpoint_url
    COPY_STDIN_URL = query_url.replace("/query", "/copy_stdin")
    HTTP_SESSIONS.session = None
    return previous


def restore_benchmark_urls(previous: Tuple[str, str, str, str, str]) -> None:
    global BASE_URL, HEALTH_URL, METRICS_URL, CHECKPOINT_URL, COPY_STDIN_URL
    BASE_URL, HEALTH_URL, METRICS_URL, CHECKPOINT_URL, COPY_STDIN_URL = previous
    HTTP_SESSIONS.session = None


def annotate_block_cache_metrics(result: "BenchResult") -> None:
    hit_count = result.metrics_delta.get("block_cache_hit_count", 0)
    miss_count = result.metrics_delta.get("block_cache_miss_count", 0)
    block_read_requests = hit_count + miss_count
    query_count = result.metrics_delta.get("query_count") or result.success_count
    query_count = max(query_count, 1)
    returned_rows = max(result.row_count, 1)
    query_result_cache_eligible = result.metrics_delta.get(
        "query_result_cache_eligible_count", 0
    )
    query_result_cache_hits = result.metrics_delta.get("query_result_cache_hit_count", 0)
    query_result_cache_misses = result.metrics_delta.get("query_result_cache_miss_count", 0)
    query_result_cache_stale = result.metrics_delta.get("query_result_cache_stale_count", 0)
    query_result_cache_inserts = result.metrics_delta.get(
        "query_result_cache_insert_count", 0
    )
    query_result_cache_invalidations = result.metrics_delta.get(
        "query_result_cache_invalidation_count", 0
    )
    point_probes = result.metrics_delta.get("sstable_point_probe_count", 0)
    range_probes = result.metrics_delta.get("sstable_range_probe_count", 0)
    overlap_skips = result.metrics_delta.get("sstable_range_overlap_skip_count", 0)
    iterator_opens = result.metrics_delta.get("sstable_iterator_open_count", 0)
    reverse_iterator_opens = result.metrics_delta.get(
        "sstable_reverse_iterator_open_count", 0
    )
    reverse_block_reads = result.metrics_delta.get("sstable_reverse_block_read_count", 0)
    reverse_block_decodes = result.metrics_delta.get(
        "sstable_reverse_block_entry_decode_count", 0
    )
    reverse_block_yields = result.metrics_delta.get(
        "sstable_reverse_block_entry_yield_count", 0
    )
    reverse_block_span_scans = result.metrics_delta.get(
        "sstable_reverse_block_span_scan_count", 0
    )
    reverse_block_span_scan_entries = result.metrics_delta.get(
        "sstable_reverse_block_span_scan_entry_count", 0
    )
    reverse_block_span_materialize_entries = result.metrics_delta.get(
        "sstable_reverse_block_span_materialize_entry_count", 0
    )
    reverse_seek_sidecar_hits = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_hit_count", 0
    )
    reverse_seek_sidecar_misses = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_miss_count", 0
    )
    reverse_seek_sidecar_stale = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_stale_count", 0
    )
    reverse_seek_sidecar_invalid = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_invalid_count", 0
    )
    reverse_seek_sidecar_writes = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_write_count", 0
    )
    reverse_seek_sidecar_write_errors = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_write_error_count", 0
    )
    reverse_seek_sidecar_uses = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_use_count", 0
    )
    reverse_seek_sidecar_fail_opens = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_fail_open_count", 0
    )
    reverse_seek_sidecar_index_entries = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_index_entry_count", 0
    )
    reverse_seek_sidecar_materializes = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_entry_materialize_count", 0
    )
    reverse_seek_sidecar_offset_probes = result.metrics_delta.get(
        "sstable_reverse_seek_sidecar_offset_probe_count", 0
    )
    reverse_seek_sidecar_probe_plus_materialize = (
        reverse_seek_sidecar_offset_probes + reverse_seek_sidecar_materializes
    )
    reverse_seek_sidecar_load_events = (
        reverse_seek_sidecar_hits
        + reverse_seek_sidecar_misses
        + reverse_seek_sidecar_stale
        + reverse_seek_sidecar_invalid
    )
    reverse_seek_sidecar_path_failures = (
        reverse_seek_sidecar_misses
        + reverse_seek_sidecar_stale
        + reverse_seek_sidecar_invalid
        + reverse_seek_sidecar_write_errors
        + reverse_seek_sidecar_fail_opens
    )
    if reverse_seek_sidecar_path_failures:
        reverse_seek_sidecar_status = "degraded"
    elif reverse_seek_sidecar_uses > 0:
        reverse_seek_sidecar_status = "observed"
    elif reverse_block_reads > 0:
        reverse_seek_sidecar_status = "fallback"
    else:
        reverse_seek_sidecar_status = "unobservable"
    fusion_reverse_scans = result.metrics_delta.get("fusion_reverse_scan_count", 0)
    fusion_reverse_sources = result.metrics_delta.get("fusion_reverse_source_open_count", 0)
    fusion_reverse_raw_reads = result.metrics_delta.get(
        "fusion_reverse_raw_entry_read_count", 0
    )
    fusion_reverse_candidates = result.metrics_delta.get(
        "fusion_reverse_visible_candidate_count", 0
    )
    fusion_reverse_puts = result.metrics_delta.get("fusion_reverse_visible_put_count", 0)
    user_key_checks = result.metrics_delta.get("sstable_user_key_filter_check_count", 0)
    user_key_positives = result.metrics_delta.get("sstable_user_key_filter_positive_count", 0)
    user_key_skips = result.metrics_delta.get("sstable_user_key_filter_skip_count", 0)
    user_key_fail_opens = result.metrics_delta.get("sstable_user_key_filter_fail_open_count", 0)
    file_opens = result.metrics_delta.get("sstable_block_file_open_count", 0)
    file_read_bytes = result.metrics_delta.get("sstable_block_read_bytes", 0)
    index_key_visits = result.metrics_delta.get("index_key_stream_entry_visit_count", 0)
    ordered_topk_scans = result.metrics_delta.get("index_ordered_topk_scan_count", 0)
    ordered_topk_visits = result.metrics_delta.get("index_ordered_topk_entry_visit_count", 0)
    ordered_topk_reverse_scans = result.metrics_delta.get(
        "index_ordered_topk_reverse_scan_count", 0
    )
    ordered_topk_index_only_rows = result.metrics_delta.get(
        "index_ordered_topk_index_only_row_count", 0
    )
    ordered_topk_base_row_fetches = result.metrics_delta.get(
        "index_ordered_topk_base_row_fetch_count", 0
    )
    index_summary_visits = result.metrics_delta.get("index_group_count_summary_entry_visit_count", 0)
    index_loose_seeks = result.metrics_delta.get("index_loose_seek_count", 0)
    index_loose_values = result.metrics_delta.get("index_loose_value_count", 0)
    index_loose_run_skips = result.metrics_delta.get("index_loose_run_skip_count", 0)
    point_overlap_skips = result.metrics_delta.get("sstable_point_overlap_skip_count", 0)
    query_sort_fallbacks = result.metrics_delta.get("query_sort_fallback_count", 0)
    columnar_single_source_fast_paths = result.metrics_delta.get(
        "columnar_single_source_aggregate_fast_path_count", 0
    )
    result.metadata.update({
        "block_read_requests": block_read_requests,
        "block_read_requests_per_query": round(block_read_requests / query_count, 3),
        "block_read_requests_per_iter": round(block_read_requests / query_count, 3),
        "block_cache_hit_ratio": round(hit_count / block_read_requests, 6) if block_read_requests else None,
        "cold_miss_ratio": round(miss_count / block_read_requests, 6) if block_read_requests else None,
        "query_result_cache_eligible": query_result_cache_eligible,
        "query_result_cache_eligible_per_query": round(
            query_result_cache_eligible / query_count, 3
        ),
        "query_result_cache_hits": query_result_cache_hits,
        "query_result_cache_hits_per_query": round(
            query_result_cache_hits / query_count, 3
        ),
        "query_result_cache_hit_ratio": (
            round(query_result_cache_hits / query_result_cache_eligible, 6)
            if query_result_cache_eligible
            else None
        ),
        "query_result_cache_misses": query_result_cache_misses,
        "query_result_cache_misses_per_query": round(
            query_result_cache_misses / query_count, 3
        ),
        "query_result_cache_stale": query_result_cache_stale,
        "query_result_cache_stale_per_query": round(
            query_result_cache_stale / query_count, 3
        ),
        "query_result_cache_inserts": query_result_cache_inserts,
        "query_result_cache_inserts_per_query": round(
            query_result_cache_inserts / query_count, 3
        ),
        "query_result_cache_invalidations": query_result_cache_invalidations,
        "query_result_cache_invalidations_per_query": round(
            query_result_cache_invalidations / query_count, 3
        ),
        "cold_block_loads": miss_count,
        "cold_block_loads_per_query": round(miss_count / query_count, 3),
        "cold_block_loads_per_iter": round(miss_count / query_count, 3),
        "block_cache_fill_skips": result.metrics_delta.get("block_cache_fill_skip_count", 0),
        "block_cache_fill_skips_per_query": round(
            result.metrics_delta.get("block_cache_fill_skip_count", 0) / query_count, 3
        ),
        "sstable_block_file_opens": file_opens,
        "sstable_block_file_opens_per_query": round(file_opens / query_count, 3),
        "sstable_block_read_bytes": file_read_bytes,
        "sstable_block_read_bytes_per_query": round(file_read_bytes / query_count, 3),
        "blocks_per_returned_row": round(block_read_requests / returned_rows, 6) if result.row_count else None,
        "sstable_point_probes": point_probes,
        "sstable_point_probes_per_query": round(point_probes / query_count, 3),
        "sstable_point_overlap_skips": point_overlap_skips,
        "sstable_point_overlap_skips_per_query": round(point_overlap_skips / query_count, 3),
        "sstable_point_overlap_skip_ratio": round(point_overlap_skips / (point_overlap_skips + point_probes), 6) if point_overlap_skips + point_probes else None,
        "sstable_range_probes": range_probes,
        "sstable_range_probes_per_query": round(range_probes / query_count, 3),
        "sstable_range_overlap_skips": overlap_skips,
        "sstable_range_overlap_skips_per_query": round(overlap_skips / query_count, 3),
        "sstable_range_overlap_skip_ratio": round(overlap_skips / range_probes, 6) if range_probes else None,
        "sstable_iterator_opens": iterator_opens,
        "sstable_iterator_opens_per_query": round(iterator_opens / query_count, 3),
        "columnar_single_source_fast_paths": columnar_single_source_fast_paths,
        "columnar_single_source_fast_paths_per_query": round(
            columnar_single_source_fast_paths / query_count, 3
        ),
        "sstable_reverse_iterator_opens": reverse_iterator_opens,
        "sstable_reverse_iterator_opens_per_query": round(
            reverse_iterator_opens / query_count, 3
        ),
        "sstable_reverse_block_reads": reverse_block_reads,
        "sstable_reverse_block_reads_per_query": round(reverse_block_reads / query_count, 3),
        "sstable_reverse_block_entry_decodes": reverse_block_decodes,
        "sstable_reverse_block_entry_decodes_per_query": round(
            reverse_block_decodes / query_count, 3
        ),
        "sstable_reverse_block_entry_yields": reverse_block_yields,
        "sstable_reverse_block_entry_yields_per_query": round(
            reverse_block_yields / query_count, 3
        ),
        "sstable_reverse_block_span_scans": reverse_block_span_scans,
        "sstable_reverse_block_span_scans_per_query": round(
            reverse_block_span_scans / query_count, 3
        ),
        "sstable_reverse_block_span_scan_entries": reverse_block_span_scan_entries,
        "sstable_reverse_block_span_scan_entries_per_query": round(
            reverse_block_span_scan_entries / query_count, 3
        ),
        "sstable_reverse_block_span_materialize_entries": reverse_block_span_materialize_entries,
        "sstable_reverse_block_span_materialize_entries_per_query": round(
            reverse_block_span_materialize_entries / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_hits": reverse_seek_sidecar_hits,
        "sstable_reverse_seek_sidecar_hits_per_query": round(
            reverse_seek_sidecar_hits / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_misses": reverse_seek_sidecar_misses,
        "sstable_reverse_seek_sidecar_misses_per_query": round(
            reverse_seek_sidecar_misses / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_stale": reverse_seek_sidecar_stale,
        "sstable_reverse_seek_sidecar_invalid": reverse_seek_sidecar_invalid,
        "sstable_reverse_seek_sidecar_writes": reverse_seek_sidecar_writes,
        "sstable_reverse_seek_sidecar_write_errors": reverse_seek_sidecar_write_errors,
        "sstable_reverse_seek_sidecar_uses": reverse_seek_sidecar_uses,
        "sstable_reverse_seek_sidecar_uses_per_query": round(
            reverse_seek_sidecar_uses / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_fail_opens": reverse_seek_sidecar_fail_opens,
        "sstable_reverse_seek_sidecar_fail_opens_per_query": round(
            reverse_seek_sidecar_fail_opens / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_index_entries": reverse_seek_sidecar_index_entries,
        "sstable_reverse_seek_sidecar_index_entries_per_query": round(
            reverse_seek_sidecar_index_entries / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_materializes": reverse_seek_sidecar_materializes,
        "sstable_reverse_seek_sidecar_materializes_per_query": round(
            reverse_seek_sidecar_materializes / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_offset_probes": reverse_seek_sidecar_offset_probes,
        "sstable_reverse_seek_sidecar_offset_probes_per_query": round(
            reverse_seek_sidecar_offset_probes / query_count, 3
        ),
        "sstable_reverse_seek_sidecar_probe_plus_materialize": reverse_seek_sidecar_probe_plus_materialize,
        "sstable_reverse_seek_sidecar_probe_plus_materialize_ratio": (
            round(
                reverse_seek_sidecar_probe_plus_materialize
                / reverse_seek_sidecar_index_entries,
                6,
            )
            if reverse_seek_sidecar_index_entries
            else None
        ),
        "sstable_reverse_seek_sidecar_load_events": reverse_seek_sidecar_load_events,
        "sstable_reverse_seek_sidecar_hit_ratio": (
            round(reverse_seek_sidecar_hits / reverse_seek_sidecar_load_events, 6)
            if reverse_seek_sidecar_load_events
            else None
        ),
        "sstable_reverse_seek_sidecar_path_failures": reverse_seek_sidecar_path_failures,
        "sstable_reverse_seek_sidecar_status": reverse_seek_sidecar_status,
        "sstable_reverse_seek_sidecar_clean": reverse_seek_sidecar_path_failures == 0,
        "sstable_reverse_seek_sidecar_uses_per_reverse_block_read": (
            round(reverse_seek_sidecar_uses / reverse_block_reads, 6)
            if reverse_block_reads
            else None
        ),
        "sstable_reverse_block_span_scan_entries_per_reverse_block_read": (
            round(reverse_block_span_scan_entries / reverse_block_reads, 6)
            if reverse_block_reads
            else None
        ),
        "sstable_reverse_seek_sidecar_offset_probes_per_reverse_block_read": (
            round(reverse_seek_sidecar_offset_probes / reverse_block_reads, 6)
            if reverse_block_reads
            else None
        ),
        "sstable_reverse_seek_sidecar_index_entries_per_reverse_block_read": (
            round(reverse_seek_sidecar_index_entries / reverse_block_reads, 6)
            if reverse_block_reads
            else None
        ),
        "sstable_reverse_seek_sidecar_materializes_per_reverse_block_read": (
            round(reverse_seek_sidecar_materializes / reverse_block_reads, 6)
            if reverse_block_reads
            else None
        ),
        "sstable_reverse_block_entry_decodes_per_yield": (
            round(reverse_block_decodes / reverse_block_yields, 6)
            if reverse_block_yields
            else None
        ),
        "fusion_reverse_scans": fusion_reverse_scans,
        "fusion_reverse_scans_per_query": round(fusion_reverse_scans / query_count, 3),
        "fusion_reverse_source_opens": fusion_reverse_sources,
        "fusion_reverse_source_opens_per_query": round(
            fusion_reverse_sources / query_count, 3
        ),
        "fusion_reverse_raw_entry_reads": fusion_reverse_raw_reads,
        "fusion_reverse_raw_entry_reads_per_query": round(
            fusion_reverse_raw_reads / query_count, 3
        ),
        "fusion_reverse_visible_candidates": fusion_reverse_candidates,
        "fusion_reverse_visible_candidates_per_query": round(
            fusion_reverse_candidates / query_count, 3
        ),
        "fusion_reverse_visible_puts": fusion_reverse_puts,
        "fusion_reverse_visible_puts_per_query": round(fusion_reverse_puts / query_count, 3),
        "user_key_filter_checks": user_key_checks,
        "user_key_filter_positives": user_key_positives,
        "user_key_filter_skips": user_key_skips,
        "user_key_filter_fail_opens": user_key_fail_opens,
        "user_key_filter_skip_ratio": round(user_key_skips / user_key_checks, 6) if user_key_checks else None,
        "user_key_filter_positive_ratio": round(user_key_positives / user_key_checks, 6) if user_key_checks else None,
        "user_key_filter_fail_open_ratio": round(user_key_fail_opens / user_key_checks, 6) if user_key_checks else None,
        "user_key_filter_checks_per_query": round(user_key_checks / query_count, 3),
        "user_key_filter_skips_per_query": round(user_key_skips / query_count, 3),
        "block_insert_bytes": result.metrics_delta.get("block_cache_insert_bytes", 0),
        "row_reads_per_query": round(result.metrics_delta.get("row_read_count", 0) / query_count, 3),
        "row_reads_per_iter": round(result.metrics_delta.get("row_read_count", 0) / query_count, 3),
        "index_key_stream_entry_visits": index_key_visits,
        "index_key_stream_entry_visits_per_query": round(index_key_visits / query_count, 3),
        "index_ordered_topk_scans": ordered_topk_scans,
        "index_ordered_topk_scans_per_query": round(ordered_topk_scans / query_count, 3),
        "index_ordered_topk_entry_visits": ordered_topk_visits,
        "index_ordered_topk_entry_visits_per_query": round(ordered_topk_visits / query_count, 3),
        "index_ordered_topk_reverse_scans": ordered_topk_reverse_scans,
        "index_ordered_topk_reverse_scans_per_query": round(
            ordered_topk_reverse_scans / query_count, 3
        ),
        "index_ordered_topk_index_only_rows": ordered_topk_index_only_rows,
        "index_ordered_topk_index_only_rows_per_query": round(
            ordered_topk_index_only_rows / query_count, 3
        ),
        "index_ordered_topk_base_row_fetches": ordered_topk_base_row_fetches,
        "index_ordered_topk_base_row_fetches_per_query": round(
            ordered_topk_base_row_fetches / query_count, 3
        ),
        "index_group_count_summary_entry_visits": index_summary_visits,
        "index_group_count_summary_entry_visits_per_query": round(index_summary_visits / query_count, 3),
        "index_loose_seeks": index_loose_seeks,
        "index_loose_seeks_per_query": round(index_loose_seeks / query_count, 3),
        "index_loose_values": index_loose_values,
        "index_loose_values_per_query": round(index_loose_values / query_count, 3),
        "index_loose_run_skips": index_loose_run_skips,
        "index_loose_run_skips_per_query": round(index_loose_run_skips / query_count, 3),
        "index_loose_seek_to_value_ratio": (
            round(index_loose_seeks / index_loose_values, 6) if index_loose_values else None
        ),
        "query_sort_fallbacks": query_sort_fallbacks,
        "query_sort_fallbacks_per_query": round(query_sort_fallbacks / query_count, 3),
    })


def annotate_prefix_filter_metrics(result: "BenchResult") -> None:
    query_count = result.metrics_delta.get("query_count") or result.success_count
    query_count = max(query_count, 1)
    checks = result.metrics_delta.get("sstable_prefix_filter_check_count", 0)
    positives = result.metrics_delta.get("sstable_prefix_filter_positive_count", 0)
    skips = result.metrics_delta.get("sstable_prefix_filter_skip_count", 0)
    fail_opens = result.metrics_delta.get("sstable_prefix_filter_fail_open_count", 0)
    index_checks = result.metrics_delta.get("sstable_index_prefix_filter_check_count", 0)
    index_positives = result.metrics_delta.get("sstable_index_prefix_filter_positive_count", 0)
    index_skips = result.metrics_delta.get("sstable_index_prefix_filter_skip_count", 0)
    index_fail_opens = result.metrics_delta.get("sstable_index_prefix_filter_fail_open_count", 0)
    block_checks = result.metrics_delta.get("sstable_block_prefix_filter_check_count", 0)
    block_positives = result.metrics_delta.get("sstable_block_prefix_filter_positive_count", 0)
    block_skips = result.metrics_delta.get("sstable_block_prefix_filter_skip_count", 0)
    block_fail_opens = result.metrics_delta.get("sstable_block_prefix_filter_fail_open_count", 0)
    block_index_checks = result.metrics_delta.get("sstable_block_index_prefix_filter_check_count", 0)
    block_index_positives = result.metrics_delta.get("sstable_block_index_prefix_filter_positive_count", 0)
    block_index_skips = result.metrics_delta.get("sstable_block_index_prefix_filter_skip_count", 0)
    block_index_fail_opens = result.metrics_delta.get("sstable_block_index_prefix_filter_fail_open_count", 0)
    block_zone_checks = result.metrics_delta.get("sstable_block_zone_map_filter_check_count", 0)
    block_zone_positives = result.metrics_delta.get("sstable_block_zone_map_filter_positive_count", 0)
    block_zone_skips = result.metrics_delta.get("sstable_block_zone_map_filter_skip_count", 0)
    block_zone_fail_opens = result.metrics_delta.get("sstable_block_zone_map_filter_fail_open_count", 0)
    block_zone_metadata_bytes = result.metrics_delta.get("sstable_block_zone_map_metadata_bytes", 0)
    block_zone_mvcc_fail_opens = result.metrics_delta.get(
        "sstable_block_zone_map_mvcc_overlap_fail_open_count", 0
    )
    block_zone_mvcc_boundary_fail_opens = result.metrics_delta.get(
        "sstable_block_zone_map_mvcc_boundary_split_fail_open_count", 0
    )
    block_zone_mvcc_write_buffer_fail_opens = result.metrics_delta.get(
        "sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count", 0
    )
    block_zone_mvcc_memtable_fail_opens = result.metrics_delta.get(
        "sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count", 0
    )
    block_zone_mvcc_sstable_fail_opens = result.metrics_delta.get(
        "sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count", 0
    )
    block_zone_schema_fail_opens = result.metrics_delta.get(
        "sstable_block_zone_map_schema_fail_open_count", 0
    )
    result.metadata.update({
        "prefix_filter_checks": checks,
        "prefix_filter_positives": positives,
        "prefix_filter_skips": skips,
        "prefix_filter_fail_opens": fail_opens,
        "prefix_filter_skip_ratio": round(skips / checks, 6) if checks else None,
        "prefix_filter_positive_ratio": round(positives / checks, 6) if checks else None,
        "prefix_filter_fail_open_ratio": round(fail_opens / checks, 6) if checks else None,
        "prefix_filter_checks_per_query": round(checks / query_count, 3),
        "prefix_filter_positives_per_query": round(positives / query_count, 3),
        "prefix_filter_skips_per_query": round(skips / query_count, 3),
        "prefix_filter_fail_opens_per_query": round(fail_opens / query_count, 3),
        "index_prefix_filter_checks": index_checks,
        "index_prefix_filter_positives": index_positives,
        "index_prefix_filter_skips": index_skips,
        "index_prefix_filter_fail_opens": index_fail_opens,
        "index_prefix_filter_skip_ratio": round(index_skips / index_checks, 6) if index_checks else None,
        "index_prefix_filter_positive_ratio": round(index_positives / index_checks, 6) if index_checks else None,
        "index_prefix_filter_fail_open_ratio": round(index_fail_opens / index_checks, 6) if index_checks else None,
        "index_prefix_filter_checks_per_query": round(index_checks / query_count, 3),
        "index_prefix_filter_positives_per_query": round(index_positives / query_count, 3),
        "index_prefix_filter_skips_per_query": round(index_skips / query_count, 3),
        "index_prefix_filter_fail_opens_per_query": round(index_fail_opens / query_count, 3),
        "block_prefix_filter_checks": block_checks,
        "block_prefix_filter_positives": block_positives,
        "block_prefix_filter_skips": block_skips,
        "block_prefix_filter_fail_opens": block_fail_opens,
        "block_prefix_filter_skip_ratio": round(block_skips / block_checks, 6) if block_checks else None,
        "block_prefix_filter_positive_ratio": round(block_positives / block_checks, 6) if block_checks else None,
        "block_prefix_filter_fail_open_ratio": round(block_fail_opens / block_checks, 6) if block_checks else None,
        "block_prefix_filter_checks_per_query": round(block_checks / query_count, 3),
        "block_prefix_filter_positives_per_query": round(block_positives / query_count, 3),
        "block_prefix_filter_skips_per_query": round(block_skips / query_count, 3),
        "block_prefix_filter_fail_opens_per_query": round(block_fail_opens / query_count, 3),
        "block_index_prefix_filter_checks": block_index_checks,
        "block_index_prefix_filter_positives": block_index_positives,
        "block_index_prefix_filter_skips": block_index_skips,
        "block_index_prefix_filter_fail_opens": block_index_fail_opens,
        "block_index_prefix_filter_skip_ratio": round(block_index_skips / block_index_checks, 6) if block_index_checks else None,
        "block_index_prefix_filter_positive_ratio": round(block_index_positives / block_index_checks, 6) if block_index_checks else None,
        "block_index_prefix_filter_fail_open_ratio": round(block_index_fail_opens / block_index_checks, 6) if block_index_checks else None,
        "block_index_prefix_filter_checks_per_query": round(block_index_checks / query_count, 3),
        "block_index_prefix_filter_positives_per_query": round(block_index_positives / query_count, 3),
        "block_index_prefix_filter_skips_per_query": round(block_index_skips / query_count, 3),
        "block_index_prefix_filter_fail_opens_per_query": round(block_index_fail_opens / query_count, 3),
        "block_zone_map_filter_checks": block_zone_checks,
        "block_zone_map_filter_positives": block_zone_positives,
        "block_zone_map_filter_skips": block_zone_skips,
        "block_zone_map_filter_fail_opens": block_zone_fail_opens,
        "block_zone_map_filter_skip_ratio": round(block_zone_skips / block_zone_checks, 6) if block_zone_checks else None,
        "block_zone_map_filter_positive_ratio": round(block_zone_positives / block_zone_checks, 6) if block_zone_checks else None,
        "block_zone_map_filter_fail_open_ratio": round(block_zone_fail_opens / block_zone_checks, 6) if block_zone_checks else None,
        "block_zone_map_filter_checks_per_query": round(block_zone_checks / query_count, 3),
        "block_zone_map_filter_positives_per_query": round(block_zone_positives / query_count, 3),
        "block_zone_map_filter_skips_per_query": round(block_zone_skips / query_count, 3),
        "block_zone_map_filter_fail_opens_per_query": round(block_zone_fail_opens / query_count, 3),
        "block_zone_map_metadata_bytes": block_zone_metadata_bytes,
        "block_zone_map_mvcc_overlap_fail_opens": block_zone_mvcc_fail_opens,
        "block_zone_map_mvcc_boundary_split_fail_opens": block_zone_mvcc_boundary_fail_opens,
        "block_zone_map_mvcc_write_buffer_overlap_fail_opens": block_zone_mvcc_write_buffer_fail_opens,
        "block_zone_map_mvcc_memtable_overlap_fail_opens": block_zone_mvcc_memtable_fail_opens,
        "block_zone_map_mvcc_sstable_overlap_fail_opens": block_zone_mvcc_sstable_fail_opens,
        "block_zone_map_mvcc_reason_accounted_count": (
            block_zone_mvcc_boundary_fail_opens
            + block_zone_mvcc_write_buffer_fail_opens
            + block_zone_mvcc_memtable_fail_opens
            + block_zone_mvcc_sstable_fail_opens
        ),
        "block_zone_map_schema_fail_opens": block_zone_schema_fail_opens,
    })


def payload_error_message(payload) -> str:
    if isinstance(payload, dict):
        for key in ("error", "message", "detail"):
            value = payload.get(key)
            if value:
                return str(value)
        if payload.get("status") == "error":
            return json.dumps(payload, ensure_ascii=False, sort_keys=True)
    elif payload:
        return str(payload)
    return ""


def http_error_message(response, payload) -> str:
    detail = payload_error_message(payload)
    if not detail:
        detail = response.text.strip()
    status = f"HTTP {response.status_code}"
    if getattr(response, "reason", ""):
        status = f"{status} {response.reason}"
    return f"{status}: {detail}" if detail else status


def classify_error(message: str) -> str:
    text = str(message or "unknown error").strip()
    lower = text.lower()
    if "duplicate" in lower or "primary key" in lower or "unique constraint" in lower:
        return "duplicate_key"
    if "timeout" in lower or "timed out" in lower:
        return "timeout"
    if "connection" in lower or "connect" in lower:
        return "connection"
    if "syntax" in lower or "parse" in lower:
        return "syntax"
    if lower.startswith("http "):
        parts = lower.split()
        if len(parts) > 1 and parts[1].isdigit():
            return f"http_{parts[1]}"
        return "http_error"
    if "not found" in lower:
        return "not_found"
    if "unauthorized" in lower or "forbidden" in lower or "permission" in lower:
        return "authorization"
    return "other"


def aggregate_error_classes(errors: List[str]) -> Dict[str, int]:
    classes: Dict[str, int] = {}
    for error in errors:
        cls = classify_error(error)
        classes[cls] = classes.get(cls, 0) + 1
    return classes


@dataclass
class PartSpec:
    id: int
    key: str
    title: str
    fn: Callable[[], List["BenchResult"]]
    tags: Tuple[str, ...] = ()
    default: bool = True


@dataclass
class BenchResult:
    name: str
    category: str = ""
    part_id: int = 0
    part_key: str = ""
    part_title: str = ""
    times_ms: List[float] = field(default_factory=list)
    row_count: int = 0
    error: Optional[str] = None
    note: str = ""
    planned_iters: int = 0
    warmup_iters: int = 0
    errors: List[str] = field(default_factory=list)
    total_ops: int = 0
    wall_ms: float = 0
    throughput_ops_sec: float = 0
    attempted_ops_sec: float = 0
    successful_ops_sec: float = 0
    error_classes: Dict[str, int] = field(default_factory=dict)
    metadata: Dict[str, object] = field(default_factory=dict)
    metrics_delta: Dict[str, int] = field(default_factory=dict)
    result_checksum: Optional[str] = None
    result_checksums: List[str] = field(default_factory=list)

    @property
    def avg(self):  return statistics.mean(self.times_ms) if self.times_ms else 0
    @property
    def p50(self):  return statistics.median(self.times_ms) if self.times_ms else 0
    @property
    def p90(self):
        if len(self.times_ms) < 2: return self.avg
        s = sorted(self.times_ms); return s[min(int(len(s)*0.90), len(s)-1)]
    @property
    def p95(self):
        if len(self.times_ms) < 2: return self.avg
        s = sorted(self.times_ms); return s[min(int(len(s)*0.95), len(s)-1)]
    @property
    def p99(self):
        if len(self.times_ms) < 2: return self.avg
        s = sorted(self.times_ms); return s[min(int(len(s)*0.99), len(s)-1)]
    @property
    def min_ms(self): return min(self.times_ms) if self.times_ms else 0
    @property
    def max_ms(self): return max(self.times_ms) if self.times_ms else 0
    @property
    def ops_sec(self): return 1000.0 / self.avg if self.avg > 0 else 0
    @property
    def total_ms(self): return sum(self.times_ms)
    @property
    def stddev_ms(self): return statistics.pstdev(self.times_ms) if len(self.times_ms) > 1 else 0
    @property
    def cv_pct(self): return (self.stddev_ms / self.avg * 100) if self.avg > 0 else 0
    @property
    def mad_ms(self):
        if not self.times_ms: return 0
        med = self.p50
        return statistics.median([abs(x - med) for x in self.times_ms])
    @property
    def rows_sec(self): return self.row_count / max(self.avg / 1000, 0.001) if self.row_count else 0
    @property
    def success_count(self): return len(self.times_ms)
    @property
    def error_count(self): return len(self.errors) if self.errors else (1 if self.error else 0)
    def grouped_error_classes(self):
        if self.error_classes:
            return self.error_classes
        if self.errors:
            return aggregate_error_classes(self.errors)
        if self.error:
            return aggregate_error_classes([self.error])
        return {}

    def record(self, res, ms, capture_rows=True):
        if not res or res.get("status") == "error":
            msg = str((res or {}).get("error") or "unknown error")
            self.error = self.error or msg
            self.errors.append(msg)
            cls = classify_error(msg)
            self.error_classes[cls] = self.error_classes.get(cls, 0) + 1
            return False
        self.times_ms.append(ms)
        if capture_rows:
            self.row_count = rows(res)
            checksum = result_checksum(res)
            if checksum is not None:
                self.result_checksum = checksum
                self.result_checksums.append(checksum)
                distinct_checksums = sorted(set(self.result_checksums))
                self.metadata.update({
                    "result_checksum": checksum,
                    "result_checksum_algorithm": RESULT_CHECKSUM_ALGORITHM,
                    "result_checksum_scope": (
                        "ordered rows and columns from the first SELECT payload for each "
                        "captured successful execution"
                    ),
                    "result_checksum_count": len(self.result_checksums),
                    "result_checksum_distinct_count": len(distinct_checksums),
                    "result_checksum_consistent": len(distinct_checksums) == 1,
                })
        return True


def pg_conn():
    """Thread-local autocommit psycopg2 connection to the pgwire server."""
    conn = getattr(PG_LOCAL, "conn", None)
    if conn is None:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER,
            password=PG_PASSWORD, dbname=PG_DBNAME, connect_timeout=10,
        )
        conn.autocommit = True
        PG_LOCAL.conn = conn
    return conn


def _http_sql(query: str, silent: bool) -> Tuple[Optional[dict], float]:
    try:
        t0 = time.perf_counter()
        r = http_session().post(BASE_URL, json={"sql": query}, timeout=60)
        ms = (time.perf_counter() - t0) * 1000
        payload = None
        json_error = None
        try:
            payload = r.json()
        except ValueError as e:
            json_error = e

        if not (200 <= r.status_code < 300):
            msg = http_error_message(r, payload)
            if not silent:
                print(f"  [ERR] {query[:80]}… → {msg}")
            return {"status": "error", "data": payload, "error": msg}, ms

        if json_error:
            raise json_error

        if isinstance(payload, dict) and "status" in payload and "data" in payload:
            if payload.get("status") == "error" and not payload.get("error"):
                payload = dict(payload)
                payload["error"] = payload_error_message(payload) or "unknown error"
            return payload, ms
        if isinstance(payload, dict) and payload.get("status") == "error":
            return {"status": "error", "data": payload.get("data"), "error": payload_error_message(payload) or "unknown error"}, ms
        return {"status": "ok", "data": payload, "error": None}, ms
    except Exception as e:
        if not silent:
            print(f"  [ERR] {query[:80]}… → {e}")
        return {"status": "error", "data": None, "error": str(e)}, 0


def _pg_sql(query: str, silent: bool) -> Tuple[Optional[dict], float]:
    # Normalize to the same {status, data:[{type:select, columns, rows}]} shape the HTTP path returns,
    # so rows()/record() and every caller work unchanged.
    try:
        t0 = time.perf_counter()
        cur = pg_conn().cursor()
        cur.execute(query)
        if cur.description is not None:
            cols = [d[0] for d in cur.description]
            fetched = cur.fetchall()
            ms = (time.perf_counter() - t0) * 1000
            data = [{"type": "select", "columns": cols, "rows": [list(r) for r in fetched]}]
        else:
            ms = (time.perf_counter() - t0) * 1000
            data = [{"type": "ok"}]
        cur.close()
        return {"status": "ok", "data": data, "error": None}, ms
    except Exception as e:
        # Drop a possibly-broken connection so the next call reconnects (autocommit SQL errors are
        # recoverable, but connection-level failures are not).
        PG_LOCAL.conn = None
        if not silent:
            print(f"  [ERR] {query[:80]}… → {e}")
        return {"status": "error", "data": None, "error": str(e)}, 0


def sql(query: str, silent=True) -> Tuple[Optional[dict], float]:
    """Execute SQL over the selected transport, return (normalized_response, latency_ms)."""
    if PROTO == "pg":
        return _pg_sql(query, silent)
    return _http_sql(query, silent)

def sql_ok(q):
    res, _ = sql(q)
    return res


def http_copy_stdin_csv(
    table: str,
    columns: List[str],
    value_rows: List[Tuple[Any, ...]],
    silent: bool = True,
) -> Tuple[Optional[dict], float, int]:
    if not value_rows:
        return {"status": "ok", "data": [], "error": None}, 0.0, 0

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerows(value_rows)
    payload = buffer.getvalue().encode("utf-8")
    copy_sql = (
        f"COPY {table} ({', '.join(columns)}) "
        "FROM STDIN WITH (FORMAT csv)"
    )
    body = {
        "sql": copy_sql,
        "payload_base64": base64.b64encode(payload).decode("ascii"),
    }

    try:
        t0 = time.perf_counter()
        response = http_session().post(
            COPY_STDIN_URL,
            json=body,
            timeout=COPY_STDIN_TIMEOUT_SEC,
        )
        ms = (time.perf_counter() - t0) * 1000
        payload_json = None
        json_error = None
        try:
            payload_json = response.json()
        except ValueError as e:
            json_error = e

        if not (200 <= response.status_code < 300):
            msg = http_error_message(response, payload_json)
            if not silent:
                print(f"  [ERR] {copy_sql[:80]}... -> {msg}")
            return {"status": "error", "data": payload_json, "error": msg}, ms, len(payload)

        if json_error:
            raise json_error

        if isinstance(payload_json, dict) and "status" in payload_json and "data" in payload_json:
            if payload_json.get("status") == "error" and not payload_json.get("error"):
                payload_json = dict(payload_json)
                payload_json["error"] = payload_error_message(payload_json) or "unknown error"
            return payload_json, ms, len(payload)
        if isinstance(payload_json, dict) and payload_json.get("status") == "error":
            return {
                "status": "error",
                "data": payload_json.get("data"),
                "error": payload_error_message(payload_json) or "unknown error",
            }, ms, len(payload)
        return {"status": "ok", "data": payload_json, "error": None}, ms, len(payload)
    except Exception as e:
        if not silent:
            print(f"  [ERR] {copy_sql[:80]}... -> {e}")
        return {"status": "error", "data": None, "error": str(e)}, 0.0, len(payload)


def sql_block_index_prefix_copy_enabled() -> bool:
    return PROTO == "http" and SQL_BLOCK_INDEX_PREFIX_COPY_STDIN


def sql_block_index_prefix_load_method() -> str:
    if sql_block_index_prefix_copy_enabled():
        return "copy_stdin_csv"
    return "insert_values"


def sql_block_index_prefix_load_chunk_rows() -> int:
    if sql_block_index_prefix_copy_enabled():
        return SQL_BLOCK_INDEX_PREFIX_COPY_CHUNK_ROWS
    return C["batch"]


def sql_block_index_prefix_value_tuple(row_id: int, host_id: int, ts: int, label: str) -> Tuple[int, int, int, str]:
    return (row_id, host_id, ts, f"{label}_{ts}")


def sql_block_index_prefix_sql_tuple(row: Tuple[Any, ...]) -> str:
    return f"({row[0]},{row[1]},{row[2]},'{row[3]}')"


def load_sql_block_index_prefix_rows(
    rows_to_load: List[Tuple[Any, ...]],
    stats: Dict[str, int],
) -> None:
    if not rows_to_load:
        return

    stats["rows"] = stats.get("rows", 0) + len(rows_to_load)
    if sql_block_index_prefix_copy_enabled():
        res, _ms, payload_bytes = http_copy_stdin_csv(
            "bench_topk_block_index_prefix_idx",
            ["id", "host_id", "ts", "payload"],
            rows_to_load,
        )
        stats["copy_stdin_batches"] = stats.get("copy_stdin_batches", 0) + 1
        stats["copy_stdin_rows"] = stats.get("copy_stdin_rows", 0) + len(rows_to_load)
        stats["copy_stdin_bytes"] = stats.get("copy_stdin_bytes", 0) + payload_bytes
        stats["copy_stdin_total_ms"] = stats.get("copy_stdin_total_ms", 0) + int(round(_ms))
        stats["copy_stdin_max_payload_bytes"] = max(
            stats.get("copy_stdin_max_payload_bytes", 0),
            payload_bytes,
        )
        if not res or res.get("status") == "error":
            raise SystemExit(
                "COPY STDIN load failed for Part 30 SQL block index-prefix setup: "
                f"{(res or {}).get('error') or 'unknown error'}"
            )
        return

    insert_batch(
        "bench_topk_block_index_prefix_idx",
        [sql_block_index_prefix_sql_tuple(row) for row in rows_to_load],
    )
    stats["insert_value_batches"] = stats.get("insert_value_batches", 0) + 1


def sql_block_zone_map_copy_enabled() -> bool:
    return PROTO == "http" and SQL_BLOCK_ZONE_MAP_COPY_STDIN


def sql_block_zone_map_load_method() -> str:
    if sql_block_zone_map_copy_enabled():
        return "copy_stdin_csv"
    return "insert_values"


def sql_block_zone_map_rows() -> int:
    if SQL_BLOCK_ZONE_MAP_ROWS_OVERRIDE > 0:
        return SQL_BLOCK_ZONE_MAP_ROWS_OVERRIDE
    return {
        "small": 4_096,
        "medium": 16_384,
        "large": 65_536,
        "xlarge": 262_144,
    }[SCALE]


def sql_block_zone_map_random_rows() -> int:
    if SQL_BLOCK_ZONE_MAP_RANDOM_ROWS_OVERRIDE > 0:
        return SQL_BLOCK_ZONE_MAP_RANDOM_ROWS_OVERRIDE
    return max(sql_block_zone_map_rows() // 2, SQL_BLOCK_ZONE_MAP_BUCKETS * 128)


def sql_block_zone_map_mvcc_rows() -> int:
    if SQL_BLOCK_ZONE_MAP_MVCC_ROWS_OVERRIDE > 0:
        return SQL_BLOCK_ZONE_MAP_MVCC_ROWS_OVERRIDE
    return max(512, SQL_BLOCK_ZONE_MAP_BUCKETS * 64)


def sql_block_zone_map_payload(label: str, row_id: int) -> str:
    prefix = f"{label}_{row_id}_"
    if len(prefix) >= SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES:
        return prefix[:SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES]
    pattern = f"{row_id:08x}{label}"
    filler_len = SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES - len(prefix)
    repeats = (filler_len // len(pattern)) + 1
    return prefix + (pattern * repeats)[:filler_len]


def sql_block_zone_map_row_tuple(
    row_id: int,
    bucket: int,
    marker: int,
    label: str,
) -> Tuple[int, int, int, str]:
    return (row_id, bucket, marker, sql_block_zone_map_payload(label, row_id))


def sql_block_zone_map_sql_tuple(row: Tuple[Any, ...]) -> str:
    payload = str(row[3]).replace("'", "''")
    return f"({row[0]},{row[1]},{row[2]},'{payload}')"


def load_sql_block_zone_map_rows(
    table: str,
    rows_to_load: List[Tuple[Any, ...]],
    stats: Dict[str, int],
) -> None:
    if not rows_to_load:
        return

    stats["rows"] = stats.get("rows", 0) + len(rows_to_load)
    if sql_block_zone_map_copy_enabled():
        res, _ms, payload_bytes = http_copy_stdin_csv(
            table,
            ["id", "bucket", "marker", "payload"],
            rows_to_load,
        )
        stats["copy_stdin_batches"] = stats.get("copy_stdin_batches", 0) + 1
        stats["copy_stdin_rows"] = stats.get("copy_stdin_rows", 0) + len(rows_to_load)
        stats["copy_stdin_bytes"] = stats.get("copy_stdin_bytes", 0) + payload_bytes
        stats["copy_stdin_total_ms"] = stats.get("copy_stdin_total_ms", 0) + int(round(_ms))
        stats["copy_stdin_max_payload_bytes"] = max(
            stats.get("copy_stdin_max_payload_bytes", 0),
            payload_bytes,
        )
        if not res or res.get("status") == "error":
            raise SystemExit(
                f"COPY STDIN load failed for Part 31 SQL zone-map setup table={table}: "
                f"{(res or {}).get('error') or 'unknown error'}"
            )
        return

    insert_batch(table, [sql_block_zone_map_sql_tuple(row) for row in rows_to_load])
    stats["insert_value_batches"] = stats.get("insert_value_batches", 0) + 1


def load_sql_block_zone_map_rows_chunked(
    table: str,
    rows_to_load: List[Tuple[Any, ...]],
    stats: Dict[str, int],
) -> None:
    chunk_rows = SQL_BLOCK_ZONE_MAP_COPY_CHUNK_ROWS if sql_block_zone_map_copy_enabled() else C["batch"]
    for start in range(0, len(rows_to_load), chunk_rows):
        load_sql_block_zone_map_rows(table, rows_to_load[start:start + chunk_rows], stats)


def first_select_result(res) -> Optional[Tuple[List[Any], List[Any]]]:
    if not res or res.get("status") != "ok":
        return None
    data = res.get("data")
    if data is None:
        data = res.get("result") or []
    first = data[0] if data else {}
    if not isinstance(first, dict):
        return None
    if "Select" in first and isinstance(first["Select"], dict):
        first = first["Select"]
    if first.get("type") == "select" or "rows" in first:
        return (list(first.get("columns") or []), list(first.get("rows") or []))
    return None


def result_checksum(res) -> Optional[str]:
    selected = first_select_result(res)
    if selected is None:
        return None
    columns, result_rows = selected
    payload = {"columns": columns, "rows": result_rows}
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def rows(res):
    selected = first_select_result(res)
    if selected is None:
        return 0
    return len(selected[1])
    return 0

def bench_query_text(query, phase: str, index: int) -> str:
    if callable(query):
        return query(phase, index)
    return query

def explain_plan_text(query) -> str:
    sample_query = bench_query_text(query, "explain", 0)
    res, _ = sql(f"EXPLAIN {sample_query}", silent=True)
    if not res or res.get("status") != "ok":
        return ""
    data = res.get("data") or res.get("result") or []
    if not data:
        return ""
    first = data[0]
    if isinstance(first, dict) and "Select" in first and isinstance(first["Select"], dict):
        first = first["Select"]
    rows_data = first.get("rows") if isinstance(first, dict) else None
    if not rows_data:
        return ""
    first_row = rows_data[0]
    if isinstance(first_row, list) and first_row:
        return str(first_row[0])
    if isinstance(first_row, tuple) and first_row:
        return str(first_row[0])
    return ""

def annotate_explain_metadata(result: "BenchResult", query) -> None:
    plan = explain_plan_text(query)
    if not plan:
        result.metadata["explain_plan_available"] = False
        return
    access_path = ""
    for line in plan.splitlines():
        stripped = line.strip()
        if stripped.startswith("Access Path:"):
            access_path = stripped[len("Access Path:"):].strip()
            break
    result.metadata.update({
        "explain_plan_available": True,
        "explain_access_path": access_path,
        "explain_ordered_composite_btree": "ordered composite BTree" in plan,
        "explain_order_by_limit": "ORDER BY/LIMIT" in plan,
    })

def bench(name, query, iters=None, warmup=None, cat=""):
    iters  = C["iters"] if iters is None else iters
    warmup = C["warmup"] if warmup is None else warmup
    r = BenchResult(name=name, category=cat, planned_iters=iters, warmup_iters=warmup)
    for i in range(warmup):
        sql(bench_query_text(query, "warmup", i))
    metrics_before = metrics_snapshot()
    for i in range(iters):
        res, ms = sql(bench_query_text(query, "measure", i))
        if not r.record(res, ms):
            break
    r.metrics_delta = metric_delta(metrics_before, metrics_snapshot())
    return r

def bench_with_phase(name, query, phase, iters=None, warmup=None, cat=""):
    if phase.endswith("first-pass"):
        r = BenchResult(name=f"{name} [{phase}]", category=cat, planned_iters=1, warmup_iters=0)
        metrics_before = metrics_snapshot()
        res, ms = sql(bench_query_text(query, phase, 0))
        r.record(res, ms)
        r.metrics_delta = metric_delta(metrics_before, metrics_snapshot())
    else:
        r = bench(f"{name} [warm]", query, iters=iters, warmup=warmup, cat=cat)
    r.metadata["phase"] = phase
    return r

def stabilization_summary(times_ms: List[float], window: int) -> Dict[str, object]:
    recent = times_ms[-window:] if window > 0 else times_ms
    avg = statistics.mean(recent) if recent else 0
    stddev = statistics.pstdev(recent) if len(recent) > 1 else 0
    cv_pct = (stddev / avg * 100) if avg > 0 else 0
    return {
        "probe_count": len(times_ms),
        "recent_avg_ms": round(avg, 3),
        "recent_cv_pct": round(cv_pct, 3),
        "recent_times_ms": [round(t, 3) for t in recent],
    }

def stabilize_wide_scan() -> Dict[str, object]:
    probe_query = "SELECT id FROM bench_wide WHERE bucket = -1"
    if not WIDE_SCAN_STABILIZE:
        return {
            "enabled": False,
            "stabilized": False,
            "probe_query": probe_query,
            "probe_count": 0,
        }

    window = max(2, WIDE_SCAN_STABILIZE_WINDOW)
    max_probes = max(window, WIDE_SCAN_STABILIZE_MAX_PROBES)
    times_ms: List[float] = []
    stabilized = False
    print(
        f"  [wide_scan] Stabilizing storage with up to {max_probes} probes "
        f"(window={window}, cv<={WIDE_SCAN_STABILIZE_CV_PCT:.1f}%) …"
    )
    for _ in range(max_probes):
        res, ms = sql(probe_query)
        if not res or res.get("status") == "error":
            return {
                "enabled": True,
                "stabilized": False,
                "probe_query": probe_query,
                "probe_count": len(times_ms),
                "error": str((res or {}).get("error") or "unknown error"),
            }
        times_ms.append(ms)
        if len(times_ms) >= window:
            summary = stabilization_summary(times_ms, window)
            if summary["recent_cv_pct"] <= WIDE_SCAN_STABILIZE_CV_PCT:
                stabilized = True
                break

    summary = stabilization_summary(times_ms, window)
    summary.update({
        "enabled": True,
        "stabilized": stabilized,
        "probe_query": probe_query,
        "max_probes": max_probes,
        "window": window,
        "target_cv_pct": WIDE_SCAN_STABILIZE_CV_PCT,
    })
    print(
        f"  [wide_scan] Stabilization probes={summary['probe_count']} "
        f"recent_avg={summary['recent_avg_ms']:.1f}ms "
        f"recent_cv={summary['recent_cv_pct']:.1f}% "
        f"stabilized={str(stabilized).lower()}"
    )
    return summary

def insert_batch(table, values_list):
    for i in range(0, len(values_list), C["batch"]):
        chunk = values_list[i:i+C["batch"]]
        sql_ok(f"INSERT INTO {table} VALUES {', '.join(chunk)}")

def insert_generated_batches(table, count, row_fn):
    chunk = []
    for i in range(count):
        chunk.append(row_fn(i))
        if len(chunk) >= C["batch"]:
            sql_ok(f"INSERT INTO {table} VALUES {', '.join(chunk)}")
            chunk.clear()
    if chunk:
        sql_ok(f"INSERT INTO {table} VALUES {', '.join(chunk)}")

class Timer:
    def __init__(self, label): self.label = label; self.ms = 0
    def __enter__(self):  self.t = time.perf_counter(); return self
    def __exit__(self,*_): self.ms = (time.perf_counter()-self.t)*1000; print(f"      {self.label}: {self.ms:,.0f} ms")


# ═══════════════════════════════════════════════════════════════════════════════
#  Data Generators
# ═══════════════════════════════════════════════════════════════════════════════
FIRST = ["Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Henry","Iris",
         "Jack","Kate","Leo","Mia","Nick","Olivia","Paul","Quinn","Rose","Sam","Tina"]
LAST  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
         "Martinez","Wilson","Anderson","Taylor","Thomas","Moore","Jackson","Lee"]
CITIES = ["Beijing","Shanghai","Shenzhen","Hangzhou","Chengdu","Guangzhou",
          "Nanjing","Wuhan","New York","London","Tokyo","Paris","Sydney","Berlin"]
CATEGORIES = ["Electronics","Books","Clothing","Food","Sports","Toys","Health",
              "Garden","Auto","Music","Home","Office","Beauty","Pets","Games"]
STATUSES = ["pending","confirmed","shipped","delivered","cancelled","returned"]
ACCT_TYPES = ["checking","savings","business","investment"]
EVENT_TYPES = ["page_view","click","purchase","signup","logout","search","add_cart"]

def gen_base(n):
    cats = ["electronics","books","clothing","food","sports","toys","health","garden","auto","music"]
    return [f"({i},{random.randint(0,999)},'{cats[i%len(cats)]}',{round(random.uniform(1,1000),2)})" for i in range(n)]

def gen_users(n):
    v = []
    for i in range(n):
        nm = f"{FIRST[i%len(FIRST)]} {LAST[i%len(LAST)]}"
        ct = CITIES[i%len(CITIES)].replace("'","''")
        v.append(f"({i},'{nm}','{FIRST[i%len(FIRST)].lower()}{i}@mail.com','{ct}',{18+(i*7+3)%52},{round(random.uniform(0,100),1)})")
    return v

def gen_products(n):
    return [f"({i},'{CATEGORIES[i%len(CATEGORIES)]} Item {i}','{CATEGORIES[i%len(CATEGORIES)]}',{round(random.uniform(5,2000),2)},{random.randint(0,500)},{round(random.uniform(1,5),1)})" for i in range(n)]

def gen_orders(n, nu, np_):
    orders, items = [], []; iid = 0
    for i in range(n):
        uid = random.randint(0, nu-1); st = STATUSES[i%len(STATUSES)]; day = 1000+random.randint(0,365)
        total = 0; ni = random.randint(1,4)
        for _ in range(ni):
            pid = random.randint(0, np_-1); qty = random.randint(1,3); up = round(random.uniform(10,500),2)
            lt = round(qty*up,2); total += lt
            items.append(f"({iid},{i},{pid},{qty},{up})"); iid += 1
        orders.append(f"({i},{uid},'{st}',{round(total,2)},{day})")
    return orders, items

def gen_accounts(n):
    return [f"({i},'owner_{i}','{ACCT_TYPES[i%len(ACCT_TYPES)]}',{round(random.uniform(100,100000),2)})" for i in range(n)]

def gen_transfers(n, na):
    v = []
    for i in range(n):
        s = random.randint(0,na-1); d = random.randint(0,na-1)
        while d == s: d = random.randint(0,na-1)
        st = random.choice(["completed","completed","completed","pending","failed"])
        v.append(f"({i},{s},{d},{round(random.uniform(1,5000),2)},'{st}',{1000+random.randint(0,365)})")
    return v

def gen_events(n, nu):
    return [f"({i},{random.randint(0,nu-1)},'{EVENT_TYPES[i%len(EVENT_TYPES)]}',{1700000000+random.randint(0,86400*30)})" for i in range(n)]

def join_reorder_sizes():
    hub_rows = C["orders"]
    low_rows = max(100, min(500, max(1, hub_rows // 100)))
    return hub_rows, hub_rows, low_rows

def gen_join_reorder_hub(n):
    return [f"({i},{i % JOIN_REORDER_NDV_BUCKETS},{i})" for i in range(n)]

def gen_join_reorder_high(n):
    return [f"({i},{i})" for i in range(n)]

def gen_join_reorder_low(n):
    return [f"({i},{i % JOIN_REORDER_NDV_BUCKETS})" for i in range(n)]

def wide_scan_rows():
    return WIDE_SCAN_ROWS_OVERRIDE if WIDE_SCAN_ROWS_OVERRIDE > 0 else C["wide_rows"]

def index_topk_rows():
    return INDEX_TOPK_ROWS_OVERRIDE if INDEX_TOPK_ROWS_OVERRIDE > 0 else C["wide_rows"]

def sql_no_fill_rows():
    if SQL_NO_FILL_ROWS_OVERRIDE > 0:
        return SQL_NO_FILL_ROWS_OVERRIDE
    return {
        "small": 512,
        "medium": 2_000,
        "large": 8_000,
        "xlarge": 20_000,
    }[SCALE]

def index_topk_composite_hosts(rows):
    target_per_host = max(INDEX_TOPK_LIMIT * 4, 64)
    return max(1, min(128, max(1, rows // target_per_host)))

def index_topk_composite_host(index, hosts):
    return (7 + index * 17) % max(hosts, 1)

def index_topk_composite_rows_for_host(rows, hosts, host_id):
    if rows <= 0 or host_id >= rows:
        return 0
    return ((rows - 1 - host_id) // max(hosts, 1)) + 1

def index_topk_prefix_prune_decoy_sstables():
    if INDEX_TOPK_PREFIX_PRUNE_DECOY_SSTABLES_OVERRIDE > 0:
        return INDEX_TOPK_PREFIX_PRUNE_DECOY_SSTABLES_OVERRIDE
    return {
        "small": 4,
        "medium": 8,
        "large": 16,
        "xlarge": 32,
    }[SCALE]

def index_topk_prefix_prune_rows_per_host():
    if INDEX_TOPK_PREFIX_PRUNE_ROWS_PER_HOST_OVERRIDE > 0:
        return INDEX_TOPK_PREFIX_PRUNE_ROWS_PER_HOST_OVERRIDE
    return max(INDEX_TOPK_LIMIT * 4, 64)

def sql_block_index_prefix_decoy_sstables():
    if SQL_BLOCK_INDEX_PREFIX_DECOY_SSTABLES_OVERRIDE > 0:
        return SQL_BLOCK_INDEX_PREFIX_DECOY_SSTABLES_OVERRIDE
    return {
        "small": 1,
        "medium": 2,
        "large": 4,
        "xlarge": 4,
    }[SCALE]

def sql_block_index_prefix_prefixes_per_sstable():
    if SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE_OVERRIDE > 0:
        return SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE_OVERRIDE
    return {
        "small": 100_000,
        "medium": 100_000,
        "large": 100_000,
        "xlarge": 100_000,
    }[SCALE]

def sql_block_index_prefix_target_rows():
    if SQL_BLOCK_INDEX_PREFIX_TARGET_ROWS_OVERRIDE > 0:
        return SQL_BLOCK_INDEX_PREFIX_TARGET_ROWS_OVERRIDE
    return max(INDEX_TOPK_LIMIT * 4, 64)

def index_topk_frontier_decoy_sstables():
    if INDEX_TOPK_FRONTIER_DECOY_SSTABLES_OVERRIDE > 0:
        return INDEX_TOPK_FRONTIER_DECOY_SSTABLES_OVERRIDE
    return {
        "small": 4,
        "medium": 8,
        "large": 16,
        "xlarge": 32,
    }[SCALE]

def index_topk_frontier_rows_per_sstable():
    if INDEX_TOPK_FRONTIER_ROWS_PER_SSTABLE_OVERRIDE > 0:
        return INDEX_TOPK_FRONTIER_ROWS_PER_SSTABLE_OVERRIDE
    return max(INDEX_TOPK_LIMIT * 4, 1024)

def index_distinct_rows():
    return INDEX_DISTINCT_ROWS_OVERRIDE if INDEX_DISTINCT_ROWS_OVERRIDE > 0 else C["wide_rows"]

def index_distinct_ndv(rows):
    return max(1, min(INDEX_DISTINCT_NDV, max(rows, 1)))

def sst_bound_rows():
    if SST_BOUND_ROWS_OVERRIDE > 0:
        return SST_BOUND_ROWS_OVERRIDE
    return {
        "small": 4096,
        "medium": 8192,
        "large": 16384,
        "xlarge": 32768,
    }[SCALE]

def sst_prefix_bloom_rows():
    if SST_PREFIX_BLOOM_ROWS_OVERRIDE > 0:
        return SST_PREFIX_BLOOM_ROWS_OVERRIDE
    return {
        "small": 2048,
        "medium": 4096,
        "large": 8192,
        "xlarge": 16384,
    }[SCALE]

def wide_payload(i, salt):
    prefix = f"p{salt}_{i}_"
    if len(prefix) >= WIDE_SCAN_PAYLOAD_BYTES:
        return prefix[:WIDE_SCAN_PAYLOAD_BYTES]
    pattern = f"{salt}{i:08x}"
    filler_len = WIDE_SCAN_PAYLOAD_BYTES - len(prefix)
    repeats = (filler_len // len(pattern)) + 1
    return prefix + (pattern * repeats)[:filler_len]

def gen_wide_scan_row(i):
    flag = 1 if i % 100 == 0 else 0
    bucket = i % 1000
    label = f"grp{bucket:03d}"
    measure = round((i % 10000) / 10.0, 1)
    return (
        f"({i},{flag},{bucket},{measure},"
        f"'{label}',"
        f"'{wide_payload(i, 'a')}','{wide_payload(i, 'b')}',"
        f"'{wide_payload(i, 'c')}','{wide_payload(i, 'd')}')"
    )

def sql_no_fill_payload(i):
    prefix = f"sql_nofill_{i}_"
    if len(prefix) >= SQL_NO_FILL_PAYLOAD_BYTES:
        return prefix[:SQL_NO_FILL_PAYLOAD_BYTES]
    pattern = f"{i:08x}"
    filler_len = SQL_NO_FILL_PAYLOAD_BYTES - len(prefix)
    repeats = (filler_len // len(pattern)) + 1
    return prefix + (pattern * repeats)[:filler_len]

def gen_sql_no_fill_row(i):
    return f"({i},{i % 16},'{sql_no_fill_payload(i)}')"

def gen_index_topk_row(i, rows):
    score = (i * 7919) % max(rows, 1)
    payload = f"topk_{i % 1000:04d}_{score % 997:03d}"
    return f"({i},{score},'{payload}')"

def gen_index_topk_type_row(i, rows):
    rank = (i * 7919) % max(rows, 1)
    flag = "true" if rank % 2 else "false"
    day = datetime(2024, 1, 1) + timedelta(days=rank % 365)
    ts = datetime(2024, 1, 1) + timedelta(seconds=rank * 17)
    span_days = 1 + (rank % 31)
    payload = f"topk_type_{i % 1000:04d}_{rank % 997:03d}"
    return (
        f"({i},{flag},'{day:%Y-%m-%d}',"
        f"'{ts:%Y-%m-%d %H:%M:%S}+00','{span_days} days','{payload}')"
    )

def gen_index_topk_composite_row(i, rows):
    hosts = index_topk_composite_hosts(rows)
    host_id = i % hosts
    ts = i // hosts
    payload = "hot" if ts % 10 == 0 else "cold"
    metric = (i * 37) % 1000
    return f"({i},{host_id},{ts},{metric},'{payload}')"

def gen_index_topk_prefix_prune_row(row_id, host_id, ts, label):
    return f"({row_id},{host_id},{ts},'{label}_{ts}')"

def gen_sql_block_index_prefix_row(row_id, host_id, ts, label):
    return sql_block_index_prefix_sql_tuple(
        sql_block_index_prefix_value_tuple(row_id, host_id, ts, label)
    )

def gen_index_topk_frontier_row(row_id, host_id, ts, label):
    prefix = f"{label}_{ts}_"
    filler = "x" * max(0, 512 - len(prefix))
    return f"({row_id},{host_id},{ts},'{prefix}{filler}')"

def gen_index_distinct_row(i, ndv, nullable=False):
    k = i % ndv
    nullable_k = "NULL" if nullable and i % max(ndv * 3, 1) == 0 else str(k)
    payload = f"distinct_{i % 1000:04d}_{k % 997:03d}"
    return f"({i},{k},{nullable_k},'{payload}')"

def sst_bound_payload(i):
    prefix = f"sst_bound_{i}_"
    if len(prefix) >= SST_BOUND_PAYLOAD_BYTES:
        return prefix[:SST_BOUND_PAYLOAD_BYTES]
    pattern = f"{i:08x}"
    filler_len = SST_BOUND_PAYLOAD_BYTES - len(prefix)
    repeats = (filler_len // len(pattern)) + 1
    return prefix + (pattern * repeats)[:filler_len]

def gen_sst_bound_row(i):
    return f"({i},0,'{sst_bound_payload(i)}')"

def sst_prefix_bloom_payload(table_suffix, i):
    prefix = f"sst_prefix_{table_suffix}_{i}_"
    if len(prefix) >= SST_PREFIX_BLOOM_PAYLOAD_BYTES:
        return prefix[:SST_PREFIX_BLOOM_PAYLOAD_BYTES]
    pattern = f"{table_suffix}{i:08x}"
    filler_len = SST_PREFIX_BLOOM_PAYLOAD_BYTES - len(prefix)
    repeats = (filler_len // len(pattern)) + 1
    return prefix + (pattern * repeats)[:filler_len]

def gen_sst_prefix_bloom_row(table_suffix):
    return lambda i: f"({i},'{sst_prefix_bloom_payload(table_suffix, i)}')"

def expected_bucket_eq(rows, bucket):
    if rows <= 0:
        return 0
    full_cycles, rem = divmod(rows, 1000)
    return full_cycles + (1 if bucket < rem else 0)

def expected_bucket_lt(rows, bucket_limit):
    if rows <= 0 or bucket_limit <= 0:
        return 0
    limit = min(bucket_limit, 1000)
    full_cycles, rem = divmod(rows, 1000)
    return full_cycles * limit + min(rem, limit)


def expected_bucket_between(rows, low, high):
    if rows <= 0 or high < low:
        return 0
    low = max(low, 0)
    high = min(high, 999)
    if high < low:
        return 0
    return expected_bucket_lt(rows, high + 1) - expected_bucket_lt(rows, low)


def expected_base_category_count(rows, category_offset):
    if rows <= 0:
        return 0
    full_cycles, rem = divmod(rows, 10)
    return full_cycles + (1 if category_offset < rem else 0)


def print_load_summary(T: Dict[str, float], total_rows: int) -> Dict[str, float]:
    load_ms = sum(v for k,v in T.items() if k.startswith("load_"))
    rate = total_rows / max(load_ms/1000, 0.001)
    T["total_load_ms"] = load_ms; T["total_rows"] = total_rows
    print(f"\n  ✓ Loaded {total_rows:,} rows in {load_ms:,.0f} ms ({rate:,.0f} rows/sec)\n")
    return T

def setup_wide_scan_table(T: Dict[str, float], total_rows: int) -> int:
    wide_rows = wide_scan_rows()
    print("  [setup] Creating wide-row scan schema …")
    sql_ok(
        "CREATE TABLE bench_wide ("
        "id INTEGER PRIMARY KEY, flag INTEGER, bucket INTEGER, measure FLOAT, "
        "label TEXT, payload_a TEXT, payload_b TEXT, payload_c TEXT, payload_d TEXT)"
    )
    print(
        f"  [setup] Loading wide rows ({wide_rows}, payload={WIDE_SCAN_PAYLOAD_BYTES} bytes/column x{WIDE_SCAN_PAYLOAD_COLUMNS}) …"
    )
    with Timer("bench_wide") as t:
        insert_generated_batches("bench_wide", wide_rows, gen_wide_scan_row)
    T["load_wide_scan"] = t.ms
    return total_rows + wide_rows

def setup_index_topk_tables(T: Dict[str, float], total_rows: int) -> int:
    rows = index_topk_rows()
    composite_hosts = index_topk_composite_hosts(rows)
    print("  [setup] Creating indexed Top-K schema …")
    for table in ("bench_topk_scan", "bench_topk_idx", "bench_topk_cover"):
        sql_ok(
            f"CREATE TABLE {table} ("
            "id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)"
        )
    sql_ok(
        "CREATE TABLE bench_topk_types ("
        "id INTEGER PRIMARY KEY, "
        "flag BOOLEAN NOT NULL, "
        "d DATE32 NOT NULL, "
        "ts TIMESTAMPTZ NOT NULL, "
        "span INTERVAL DAY NOT NULL, "
        "payload TEXT)"
    )
    for table in ("bench_topk_comp_scan", "bench_topk_comp_idx", "bench_topk_comp_cover"):
        sql_ok(
            f"CREATE TABLE {table} ("
            "id INTEGER PRIMARY KEY, "
            "host_id INTEGER NOT NULL, "
            "ts INTEGER NOT NULL, "
            "metric INTEGER NOT NULL, "
            "payload TEXT)"
        )

    print(f"  [setup] Loading indexed Top-K rows ({rows}) …")
    with Timer("bench_topk_scan") as t:
        insert_generated_batches(
            "bench_topk_scan", rows, lambda i: gen_index_topk_row(i, rows)
        )
    T["load_index_topk_scan"] = t.ms
    with Timer("bench_topk_idx") as t:
        insert_generated_batches(
            "bench_topk_idx", rows, lambda i: gen_index_topk_row(i, rows)
        )
    T["load_index_topk_idx"] = t.ms
    with Timer("bench_topk_cover") as t:
        insert_generated_batches(
            "bench_topk_cover", rows, lambda i: gen_index_topk_row(i, rows)
        )
    T["load_index_topk_cover"] = t.ms
    with Timer("bench_topk_types") as t:
        insert_generated_batches(
            "bench_topk_types", rows, lambda i: gen_index_topk_type_row(i, rows)
        )
    T["load_index_topk_types"] = t.ms
    with Timer("bench_topk_comp_scan") as t:
        insert_generated_batches(
            "bench_topk_comp_scan", rows, lambda i: gen_index_topk_composite_row(i, rows)
        )
    T["load_index_topk_comp_scan"] = t.ms
    with Timer("bench_topk_comp_idx") as t:
        insert_generated_batches(
            "bench_topk_comp_idx", rows, lambda i: gen_index_topk_composite_row(i, rows)
        )
    T["load_index_topk_comp_idx"] = t.ms
    with Timer("bench_topk_comp_cover") as t:
        insert_generated_batches(
            "bench_topk_comp_cover", rows, lambda i: gen_index_topk_composite_row(i, rows)
        )
    T["load_index_topk_comp_cover"] = t.ms
    total_rows += rows * 7

    print("  [setup] Building indexed Top-K indexes …")
    with Timer("bench_topk_indexes") as t:
        sql_ok("CREATE INDEX idx_bench_topk_idx_score ON bench_topk_idx (score)")
        sql_ok(
            "CREATE INDEX idx_bench_topk_cover_score ON bench_topk_cover (score) INCLUDE (payload)"
        )
        sql_ok("CREATE INDEX idx_bench_topk_types_flag ON bench_topk_types (flag)")
        sql_ok("CREATE INDEX idx_bench_topk_types_d ON bench_topk_types (d)")
        sql_ok(
            "CREATE INDEX idx_bench_topk_types_ts ON bench_topk_types (ts) INCLUDE (payload)"
        )
        sql_ok("CREATE INDEX idx_bench_topk_types_span ON bench_topk_types (span)")
        sql_ok(
            "CREATE INDEX idx_bench_topk_comp_idx_host_ts ON bench_topk_comp_idx (host_id, ts)"
        )
        sql_ok(
            "CREATE INDEX idx_bench_topk_comp_cover_host_ts "
            "ON bench_topk_comp_cover (host_id, ts) INCLUDE (payload, metric)"
        )
    T["load_index_topk_indexes"] = t.ms
    T["index_topk_composite_hosts"] = composite_hosts
    T["index_topk_composite_rows_per_host_avg"] = round(
        rows / max(composite_hosts, 1), 3
    )
    if INDEX_TOPK_SSTABLE_CLAIM:
        print("  [setup] Checkpointing indexed Top-K data for SSTable-heavy claim …")
        with Timer("bench_topk_sstable_claim_checkpoint") as t:
            checkpoint_ok = checkpoint_storage("index_topk_sstable_claim")
        T["index_topk_sstable_claim_checkpoint_ms"] = t.ms
        T["index_topk_sstable_claim_checkpoint_ok"] = int(checkpoint_ok)
        if not checkpoint_ok:
            raise SystemExit(
                "BENCH_CLAIM_MODE requires a successful Part 20 checkpoint "
                "for SSTable-heavy Top-K evidence"
            )
    return total_rows

def index_topk_composite_query(
    table: str,
    direction: str,
    limit: int,
    rows: int,
    range_mode: str = "all",
    residual: bool = False,
    projection: str = "id, host_id, ts",
):
    hosts = index_topk_composite_hosts(rows)

    def query(_phase: str, index: int) -> str:
        host_id = index_topk_composite_host(index, hosts)
        rows_for_host = index_topk_composite_rows_for_host(rows, hosts, host_id)
        if range_mode == "upper_half":
            floor = rows_for_host // 2
            predicate = f"host_id = {host_id} AND ts >= {floor}"
        elif range_mode == "middle_window":
            floor = rows_for_host // 3
            window = max(limit * 2, rows_for_host // 4, 1)
            ceiling = min(rows_for_host, floor + window)
            predicate = f"host_id = {host_id} AND ts >= {floor} AND ts < {ceiling}"
        else:
            floor = 0
            predicate = f"host_id = {host_id} AND ts >= {floor}"
        if residual:
            predicate += " AND payload = 'hot'"
        return (
            f"SELECT {projection} FROM {table} "
            f"WHERE {predicate} ORDER BY ts {direction} LIMIT {limit}"
        )

    return query

def setup_index_topk_prefix_prune_table(T: Dict[str, float], total_rows: int) -> int:
    decoy_sstables = index_topk_prefix_prune_decoy_sstables()
    rows_per_host = index_topk_prefix_prune_rows_per_host()
    low_host = 1
    target_host = 50
    absent_host = 51
    high_host = 99
    row_id = 0

    print("  [setup] Creating SQL index-prefix prune schema …")
    sql_ok(
        "CREATE TABLE bench_topk_prefix_prune_idx ("
        "id INTEGER PRIMARY KEY, "
        "host_id INTEGER NOT NULL, "
        "ts INTEGER NOT NULL, "
        "payload TEXT)"
    )
    sql_ok(
        "CREATE INDEX idx_bench_topk_prefix_prune_host_ts "
        "ON bench_topk_prefix_prune_idx (host_id, ts)"
    )

    print(
        "  [setup] Loading SQL index-prefix prune decoy SSTables "
        f"(decoys={decoy_sstables}, rows/host={rows_per_host}) …"
    )
    decoy_checkpoint_successes = 0
    with Timer("bench_topk_prefix_prune_decoys") as t:
        for sstable_idx in range(decoy_sstables):
            values = []
            for host_id in (low_host, high_host):
                for ts in range(rows_per_host):
                    values.append(
                        gen_index_topk_prefix_prune_row(
                            row_id,
                            host_id,
                            sstable_idx * rows_per_host + ts,
                            f"decoy{sstable_idx}_{host_id}",
                        )
                    )
                    row_id += 1
            insert_batch("bench_topk_prefix_prune_idx", values)
            if checkpoint_storage(f"index_topk_prefix_prune_decoy_{sstable_idx}"):
                decoy_checkpoint_successes += 1
    T["load_index_topk_prefix_prune_decoys"] = t.ms

    target_rows = max(rows_per_host, INDEX_TOPK_LIMIT * 4)
    print(
        "  [setup] Loading SQL index-prefix prune matching SSTable "
        f"(target_host={target_host}, rows={target_rows}) …"
    )
    with Timer("bench_topk_prefix_prune_target") as t:
        values = []
        for ts in range(target_rows):
            values.append(
                gen_index_topk_prefix_prune_row(row_id, target_host, ts, "target")
            )
            row_id += 1
        insert_batch("bench_topk_prefix_prune_idx", values)
        target_checkpoint_ok = checkpoint_storage("index_topk_prefix_prune_target")
    T["load_index_topk_prefix_prune_target"] = t.ms

    T["index_topk_prefix_prune_decoy_sstables"] = decoy_sstables
    T["index_topk_prefix_prune_rows_per_host"] = rows_per_host
    T["index_topk_prefix_prune_target_rows"] = target_rows
    T["index_topk_prefix_prune_low_host"] = low_host
    T["index_topk_prefix_prune_target_host"] = target_host
    T["index_topk_prefix_prune_absent_host"] = absent_host
    T["index_topk_prefix_prune_high_host"] = high_host
    T["index_topk_prefix_prune_decoy_checkpoint_successes"] = decoy_checkpoint_successes
    T["index_topk_prefix_prune_target_checkpoint_ok"] = int(target_checkpoint_ok)
    if BENCH_CLAIM_MODE and (
        decoy_checkpoint_successes != decoy_sstables or not target_checkpoint_ok
    ):
        raise SystemExit(
            "BENCH_CLAIM_MODE requires all SQL index-prefix prune checkpoints to succeed"
        )
    return total_rows + row_id

def index_topk_prefix_prune_query(host_id: int, direction: str, limit: int, lower_ts: int = 0):
    return (
        "SELECT id, host_id, ts FROM bench_topk_prefix_prune_idx "
        f"WHERE host_id = {host_id} AND ts >= {lower_ts} "
        f"ORDER BY ts {direction} LIMIT {limit}"
    )

def sql_block_index_prefix_query(host_id: int, direction: str, limit: int, lower_ts: int = 0):
    return (
        "SELECT id, host_id, ts FROM bench_topk_block_index_prefix_idx "
        f"WHERE host_id = {host_id} AND ts >= {lower_ts} "
        f"ORDER BY ts {direction} LIMIT {limit}"
    )

def sql_block_index_prefix_probe_host(host_id: int) -> Dict[str, object]:
    before = metrics_snapshot()
    res, ms = sql(sql_block_index_prefix_query(host_id, "ASC", 1), silent=True)
    delta = metric_delta(before, metrics_snapshot())
    return {
        "host_id": host_id,
        "ok": bool(res and res.get("status") == "ok"),
        "rows": rows(res),
        "latency_ms": round(ms, 3),
        "metrics_delta_available": bool(delta),
        "index_prefix_checks": delta.get("sstable_index_prefix_filter_check_count", 0),
        "index_prefix_positives": delta.get("sstable_index_prefix_filter_positive_count", 0),
        "index_prefix_skips": delta.get("sstable_index_prefix_filter_skip_count", 0),
        "index_prefix_fail_opens": delta.get("sstable_index_prefix_filter_fail_open_count", 0),
        "block_index_prefix_checks": delta.get("sstable_block_index_prefix_filter_check_count", 0),
        "block_index_prefix_positives": delta.get("sstable_block_index_prefix_filter_positive_count", 0),
        "block_index_prefix_skips": delta.get("sstable_block_index_prefix_filter_skip_count", 0),
        "block_index_prefix_fail_opens": delta.get("sstable_block_index_prefix_filter_fail_open_count", 0),
    }

def discover_sql_block_index_prefix_target_host() -> Tuple[Optional[int], Dict[str, object]]:
    probes = max(0, SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES)
    if SQL_BLOCK_INDEX_PREFIX_FORCE_HOST > 0:
        candidates = [SQL_BLOCK_INDEX_PREFIX_FORCE_HOST]
    else:
        candidates = [
            SQL_BLOCK_INDEX_PREFIX_GAP_HOST + offset
            for offset in range(probes)
        ]
    last_probe: Dict[str, object] = {}
    for probe_index, host_id in enumerate(candidates):
        probe = sql_block_index_prefix_probe_host(host_id)
        probe["probe_index"] = probe_index
        last_probe = probe
        if (
            probe.get("ok")
            and probe.get("rows") == 0
            and int(probe.get("index_prefix_positives", 0) or 0) > 0
            and int(probe.get("block_index_prefix_skips", 0) or 0) > 0
            and int(probe.get("block_index_prefix_fail_opens", 0) or 0) == 0
        ):
            probe["found"] = True
            return host_id, probe
    return None, {
        "found": False,
        "candidate_probes": len(candidates),
        "last_probe": last_probe,
    }

def setup_sql_block_index_prefix_prune_table(T: Dict[str, float], total_rows: int) -> int:
    global SQL_BLOCK_INDEX_PREFIX_DISCOVERY
    global SQL_BLOCK_INDEX_PREFIX_TARGET_HOST

    decoy_sstables = sql_block_index_prefix_decoy_sstables()
    prefixes_per_sstable = max(2, sql_block_index_prefix_prefixes_per_sstable())
    target_rows = sql_block_index_prefix_target_rows()
    row_id = 0
    load_stats: Dict[str, int] = {}
    load_chunk_rows = sql_block_index_prefix_load_chunk_rows()
    setup_metrics_before = metrics_snapshot()

    print("  [setup] Creating SQL block index-prefix prune schema ...")
    sql_ok(
        "CREATE TABLE bench_topk_block_index_prefix_idx ("
        "id INTEGER PRIMARY KEY, "
        "host_id INTEGER NOT NULL, "
        "ts INTEGER NOT NULL, "
        "payload TEXT)"
    )
    index_created = False
    if not SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX:
        with Timer("bench_topk_block_index_prefix_create_index") as t:
            sql_ok(
                "CREATE INDEX idx_bench_topk_block_index_prefix_host_ts "
                "ON bench_topk_block_index_prefix_idx (host_id, ts)"
            )
        T["load_sql_block_index_prefix_create_index"] = t.ms
        index_created = True

    print(
        "  [setup] Loading SQL block index-prefix decoy SSTables "
        f"(decoys={decoy_sstables}, prefixes/sstable={prefixes_per_sstable}) ..."
    )
    decoy_checkpoint_successes = 0
    with Timer("bench_topk_block_index_prefix_decoys") as t:
        for sstable_idx in range(decoy_sstables):
            low_base = 1 + sstable_idx * prefixes_per_sstable
            high_base = (
                SQL_BLOCK_INDEX_PREFIX_GAP_HOST
                + SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES
                + 10_000
                + sstable_idx * prefixes_per_sstable
            )
            split = prefixes_per_sstable // 2
            values = []
            for ordinal in range(prefixes_per_sstable):
                if ordinal < split:
                    host_id = low_base + ordinal
                else:
                    host_id = high_base + (ordinal - split)
                values.append(
                    sql_block_index_prefix_value_tuple(
                        row_id,
                        host_id,
                        sstable_idx * prefixes_per_sstable + ordinal,
                        f"decoy{sstable_idx}_{host_id}",
                    )
                )
                row_id += 1
                if len(values) >= load_chunk_rows:
                    load_sql_block_index_prefix_rows(values, load_stats)
                    values.clear()
            if values:
                load_sql_block_index_prefix_rows(values, load_stats)
            if not index_created:
                print("  [setup] Building SQL block index-prefix composite index after decoy load ...")
                with Timer("bench_topk_block_index_prefix_create_index") as idx_timer:
                    sql_ok(
                        "CREATE INDEX idx_bench_topk_block_index_prefix_host_ts "
                        "ON bench_topk_block_index_prefix_idx (host_id, ts)"
                    )
                T["load_sql_block_index_prefix_create_index"] = idx_timer.ms
                index_created = True
            if checkpoint_storage(f"sql_block_index_prefix_decoy_{sstable_idx}"):
                decoy_checkpoint_successes += 1
    T["load_sql_block_index_prefix_decoys"] = t.ms

    print("  [setup] Discovering natural SQL block index-prefix false positive ...")
    with Timer("bench_topk_block_index_prefix_discovery") as t:
        discovered_host, discovery = discover_sql_block_index_prefix_target_host()
    T["load_sql_block_index_prefix_discovery"] = t.ms
    SQL_BLOCK_INDEX_PREFIX_DISCOVERY = dict(discovery)
    false_positive_found = discovered_host is not None
    target_host = discovered_host if discovered_host is not None else SQL_BLOCK_INDEX_PREFIX_GAP_HOST
    SQL_BLOCK_INDEX_PREFIX_TARGET_HOST = target_host

    if false_positive_found:
        print(
            "      found natural false-positive host="
            f"{target_host} "
            f"file_positives={discovery.get('index_prefix_positives', 0)} "
            f"block_skips={discovery.get('block_index_prefix_skips', 0)}"
        )
    elif BENCH_CLAIM_MODE:
        detail = json.dumps(discovery.get("last_probe", discovery), sort_keys=True)[:500]
        raise SystemExit(
            "BENCH_CLAIM_MODE requires a natural SQL block index-prefix Bloom false positive; "
            "increase BENCH_SQL_BLOCK_INDEX_PREFIX_PREFIXES_PER_SSTABLE or "
            "BENCH_SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES; "
            f"last_probe={detail}"
        )

    print(
        "  [setup] Loading SQL block index-prefix matching SSTable "
        f"(target_host={target_host}, rows={target_rows}) ..."
    )
    with Timer("bench_topk_block_index_prefix_target") as t:
        values = []
        for ts in range(target_rows):
            values.append(
                sql_block_index_prefix_value_tuple(row_id, target_host, ts, "target")
            )
            row_id += 1
        load_sql_block_index_prefix_rows(values, load_stats)
        target_checkpoint_ok = checkpoint_storage("sql_block_index_prefix_target")
    T["load_sql_block_index_prefix_target"] = t.ms

    setup_metrics_delta = metric_delta(setup_metrics_before, metrics_snapshot())
    decoy_rows = decoy_sstables * prefixes_per_sstable
    expected_loaded_rows = decoy_rows + target_rows
    expected_copy_batches = (
        decoy_sstables * ((prefixes_per_sstable + load_chunk_rows - 1) // load_chunk_rows)
        + ((target_rows + load_chunk_rows - 1) // load_chunk_rows)
        if sql_block_index_prefix_copy_enabled()
        else 0
    )

    T["sql_block_index_prefix_load_method"] = sql_block_index_prefix_load_method()
    T["sql_block_index_prefix_copy_format"] = "csv" if sql_block_index_prefix_copy_enabled() else ""
    T["sql_block_index_prefix_copy_stdin_enabled"] = int(sql_block_index_prefix_copy_enabled())
    T["sql_block_index_prefix_copy_chunk_rows"] = SQL_BLOCK_INDEX_PREFIX_COPY_CHUNK_ROWS
    T["sql_block_index_prefix_actual_load_chunk_rows"] = load_chunk_rows
    T["sql_block_index_prefix_expected_loaded_rows"] = expected_loaded_rows
    T["sql_block_index_prefix_expected_copy_stdin_batches"] = expected_copy_batches
    T["sql_block_index_prefix_loaded_rows"] = load_stats.get("rows", 0)
    T["sql_block_index_prefix_copy_stdin_batches"] = load_stats.get("copy_stdin_batches", 0)
    T["sql_block_index_prefix_copy_stdin_rows"] = load_stats.get("copy_stdin_rows", 0)
    T["sql_block_index_prefix_copy_stdin_bytes"] = load_stats.get("copy_stdin_bytes", 0)
    T["sql_block_index_prefix_copy_stdin_total_ms"] = load_stats.get("copy_stdin_total_ms", 0)
    T["sql_block_index_prefix_copy_stdin_max_payload_bytes"] = load_stats.get(
        "copy_stdin_max_payload_bytes", 0
    )
    T["sql_block_index_prefix_insert_value_batches"] = load_stats.get("insert_value_batches", 0)
    T["sql_block_index_prefix_setup_compaction_run_count"] = setup_metrics_delta.get(
        "compaction_run_count", 0
    )
    T["sql_block_index_prefix_setup_row_write_count"] = setup_metrics_delta.get(
        "row_write_count", 0
    )
    T["sql_block_index_prefix_setup_wal_write_count"] = setup_metrics_delta.get(
        "wal_write_count", 0
    )
    T["sql_block_index_prefix_setup_wal_write_bytes"] = setup_metrics_delta.get(
        "wal_write_bytes", 0
    )
    T["sql_block_index_prefix_decoy_sstables"] = decoy_sstables
    T["sql_block_index_prefix_prefixes_per_sstable"] = prefixes_per_sstable
    T["sql_block_index_prefix_delay_index"] = int(SQL_BLOCK_INDEX_PREFIX_DELAY_INDEX)
    T["sql_block_index_prefix_target_rows"] = target_rows
    T["sql_block_index_prefix_target_host"] = target_host
    T["sql_block_index_prefix_candidate_probes"] = SQL_BLOCK_INDEX_PREFIX_CANDIDATE_PROBES
    T["sql_block_index_prefix_false_positive_found"] = int(false_positive_found)
    T["sql_block_index_prefix_discovery_index_prefix_positives"] = int(
        discovery.get("index_prefix_positives", 0) or 0
    )
    T["sql_block_index_prefix_discovery_block_skips"] = int(
        discovery.get("block_index_prefix_skips", 0) or 0
    )
    T["sql_block_index_prefix_decoy_checkpoint_successes"] = decoy_checkpoint_successes
    T["sql_block_index_prefix_target_checkpoint_ok"] = int(target_checkpoint_ok)
    if BENCH_CLAIM_MODE and (
        decoy_checkpoint_successes != decoy_sstables or not target_checkpoint_ok
    ):
        raise SystemExit(
            "BENCH_CLAIM_MODE requires all SQL block index-prefix checkpoints to succeed"
        )
    if BENCH_CLAIM_MODE and setup_metrics_delta.get("compaction_run_count", 0) != 0:
        raise SystemExit(
            "BENCH_CLAIM_MODE requires Part 30 setup compaction_run_count == 0; "
            f"got {setup_metrics_delta.get('compaction_run_count', 0)}"
        )
    if BENCH_CLAIM_MODE and sql_block_index_prefix_copy_enabled():
        copy_failures = []
        if load_stats.get("rows", 0) != expected_loaded_rows:
            copy_failures.append(
                f"loaded_rows={load_stats.get('rows', 0)} expected={expected_loaded_rows}"
            )
        if load_stats.get("copy_stdin_rows", 0) != expected_loaded_rows:
            copy_failures.append(
                "copy_stdin_rows="
                f"{load_stats.get('copy_stdin_rows', 0)} expected={expected_loaded_rows}"
            )
        if load_stats.get("copy_stdin_batches", 0) != expected_copy_batches:
            copy_failures.append(
                "copy_stdin_batches="
                f"{load_stats.get('copy_stdin_batches', 0)} expected={expected_copy_batches}"
            )
        if load_stats.get("insert_value_batches", 0) != 0:
            copy_failures.append(
                f"insert_value_batches={load_stats.get('insert_value_batches', 0)}"
            )
        if copy_failures:
            raise SystemExit(
                "BENCH_CLAIM_MODE Part 30 COPY load metadata mismatch: "
                + "; ".join(copy_failures)
            )
    return total_rows + row_id


def setup_sql_block_zone_map_prune_tables(T: Dict[str, float], total_rows: int) -> int:
    rows_count = sql_block_zone_map_rows()
    random_rows = sql_block_zone_map_random_rows()
    mvcc_rows = sql_block_zone_map_mvcc_rows()
    bucket_count = SQL_BLOCK_ZONE_MAP_BUCKETS
    target_bucket = SQL_BLOCK_ZONE_MAP_TARGET_BUCKET % bucket_count
    load_stats: Dict[str, int] = {}
    setup_metrics_before = metrics_snapshot()

    print("  [setup] Creating SQL block zone-map prune schemas ...")
    for table in (
        "bench_zone_map_clustered",
        "bench_zone_map_random",
        "bench_zone_map_mvcc",
    ):
        sql_ok(
            f"CREATE TABLE {table} ("
            "id INTEGER PRIMARY KEY, "
            "bucket INTEGER NOT NULL, "
            "marker INTEGER NOT NULL, "
            "payload TEXT)"
        )

    print(
        "  [setup] Loading clustered SQL zone-map table "
        f"(rows={rows_count}, buckets={bucket_count}, payload={SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES}) ..."
    )
    rows_per_bucket = max(1, rows_count // bucket_count)
    clustered_rows = []
    row_id = 0
    with Timer("bench_sql_block_zone_map_clustered") as t:
        for bucket in range(bucket_count):
            for local in range(rows_per_bucket):
                clustered_rows.append(
                    sql_block_zone_map_row_tuple(
                        row_id,
                        bucket,
                        local,
                        f"cluster_b{bucket}",
                    )
                )
                row_id += 1
                if len(clustered_rows) >= SQL_BLOCK_ZONE_MAP_COPY_CHUNK_ROWS:
                    load_sql_block_zone_map_rows(
                        "bench_zone_map_clustered",
                        clustered_rows,
                        load_stats,
                    )
                    clustered_rows.clear()
        while row_id < rows_count:
            clustered_rows.append(
                sql_block_zone_map_row_tuple(
                    row_id,
                    target_bucket,
                    row_id,
                    "cluster_tail",
                )
            )
            row_id += 1
        load_sql_block_zone_map_rows_chunked(
            "bench_zone_map_clustered",
            clustered_rows,
            load_stats,
        )
    T["load_sql_block_zone_map_clustered"] = t.ms

    print(
        "  [setup] Loading random-control SQL zone-map table "
        f"(rows={random_rows}, buckets={bucket_count}) ..."
    )
    random_values = []
    random.seed(SEED + 31)
    with Timer("bench_sql_block_zone_map_random") as t:
        for i in range(random_rows):
            bucket = i % bucket_count
            random_values.append(
                sql_block_zone_map_row_tuple(i, bucket, i, f"random_b{bucket}")
            )
        random.shuffle(random_values)
        load_sql_block_zone_map_rows_chunked(
            "bench_zone_map_random",
            random_values,
            load_stats,
        )
        clustered_random_checkpoint_ok = checkpoint_storage(
            "sql_block_zone_map_clustered_random"
        )
    T["load_sql_block_zone_map_random"] = t.ms
    clustered_checkpoint_ok = clustered_random_checkpoint_ok
    random_checkpoint_ok = clustered_random_checkpoint_ok

    print(
        "  [setup] Loading MVCC-overlap SQL zone-map table "
        f"(rows={mvcc_rows}, old={SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET}, new={SQL_BLOCK_ZONE_MAP_MVCC_NEW_BUCKET}) ..."
    )
    mvcc_old_values = [
        sql_block_zone_map_row_tuple(
            i,
            SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET,
            i,
            "mvcc_old",
        )
        for i in range(mvcc_rows)
    ]
    with Timer("bench_sql_block_zone_map_mvcc_old") as t:
        load_sql_block_zone_map_rows_chunked(
            "bench_zone_map_mvcc",
            mvcc_old_values,
            load_stats,
        )
        mvcc_old_checkpoint_ok = checkpoint_storage("sql_block_zone_map_mvcc_old")
    T["load_sql_block_zone_map_mvcc_old"] = t.ms

    update_payload = sql_block_zone_map_payload("mvcc_new", 0).replace("'", "''")
    with Timer("bench_sql_block_zone_map_mvcc_update") as t:
        update_res, _ = sql(
            "UPDATE bench_zone_map_mvcc "
            f"SET bucket = {SQL_BLOCK_ZONE_MAP_MVCC_NEW_BUCKET}, "
            f"marker = marker + 1, payload = '{update_payload}'",
            silent=True,
        )
        if not update_res or update_res.get("status") == "error":
            raise SystemExit(
                "Part 31 MVCC update setup failed: "
                f"{(update_res or {}).get('error') or 'unknown error'}"
            )
        mvcc_new_checkpoint_ok = checkpoint_storage("sql_block_zone_map_mvcc_new")
    T["load_sql_block_zone_map_mvcc_update"] = t.ms

    setup_metrics_delta = metric_delta(setup_metrics_before, metrics_snapshot())
    expected_loaded_rows = rows_count + random_rows + mvcc_rows
    T["sql_block_zone_map_load_method"] = sql_block_zone_map_load_method()
    T["sql_block_zone_map_copy_format"] = "csv" if sql_block_zone_map_copy_enabled() else ""
    T["sql_block_zone_map_copy_stdin_enabled"] = int(sql_block_zone_map_copy_enabled())
    T["sql_block_zone_map_copy_chunk_rows"] = SQL_BLOCK_ZONE_MAP_COPY_CHUNK_ROWS
    T["sql_block_zone_map_rows"] = rows_count
    T["sql_block_zone_map_random_rows"] = random_rows
    T["sql_block_zone_map_mvcc_rows"] = mvcc_rows
    T["sql_block_zone_map_buckets"] = bucket_count
    T["sql_block_zone_map_target_bucket"] = target_bucket
    T["sql_block_zone_map_absent_bucket"] = SQL_BLOCK_ZONE_MAP_ABSENT_BUCKET
    T["sql_block_zone_map_mvcc_old_bucket"] = SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET
    T["sql_block_zone_map_mvcc_new_bucket"] = SQL_BLOCK_ZONE_MAP_MVCC_NEW_BUCKET
    T["sql_block_zone_map_payload_bytes"] = SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES
    T["sql_block_zone_map_rows_per_bucket"] = rows_per_bucket
    T["sql_block_zone_map_expected_loaded_rows"] = expected_loaded_rows
    T["sql_block_zone_map_loaded_rows"] = load_stats.get("rows", 0)
    T["sql_block_zone_map_copy_stdin_batches"] = load_stats.get("copy_stdin_batches", 0)
    T["sql_block_zone_map_copy_stdin_rows"] = load_stats.get("copy_stdin_rows", 0)
    T["sql_block_zone_map_copy_stdin_bytes"] = load_stats.get("copy_stdin_bytes", 0)
    T["sql_block_zone_map_copy_stdin_total_ms"] = load_stats.get("copy_stdin_total_ms", 0)
    T["sql_block_zone_map_copy_stdin_max_payload_bytes"] = load_stats.get(
        "copy_stdin_max_payload_bytes", 0
    )
    T["sql_block_zone_map_insert_value_batches"] = load_stats.get("insert_value_batches", 0)
    T["sql_block_zone_map_setup_compaction_run_count"] = setup_metrics_delta.get(
        "compaction_run_count", 0
    )
    T["sql_block_zone_map_metadata_bytes"] = setup_metrics_delta.get(
        "sstable_block_zone_map_metadata_bytes", 0
    )
    T["sql_block_zone_map_clustered_random_checkpoint_ok"] = int(
        clustered_random_checkpoint_ok
    )
    T["sql_block_zone_map_clustered_checkpoint_ok"] = int(clustered_checkpoint_ok)
    T["sql_block_zone_map_random_checkpoint_ok"] = int(random_checkpoint_ok)
    T["sql_block_zone_map_mvcc_old_checkpoint_ok"] = int(mvcc_old_checkpoint_ok)
    T["sql_block_zone_map_mvcc_new_checkpoint_ok"] = int(mvcc_new_checkpoint_ok)
    if BENCH_CLAIM_MODE:
        checkpoint_failures = []
        if not clustered_checkpoint_ok:
            checkpoint_failures.append("clustered")
        if not random_checkpoint_ok:
            checkpoint_failures.append("random")
        if not mvcc_old_checkpoint_ok:
            checkpoint_failures.append("mvcc_old")
        if not mvcc_new_checkpoint_ok:
            checkpoint_failures.append("mvcc_new")
        if checkpoint_failures:
            raise SystemExit(
                "BENCH_CLAIM_MODE requires all SQL zone-map checkpoints to succeed: "
                + ", ".join(checkpoint_failures)
            )
        if setup_metrics_delta.get("compaction_run_count", 0) != 0:
            raise SystemExit(
                "BENCH_CLAIM_MODE requires Part 31 setup compaction_run_count == 0; "
                f"got {setup_metrics_delta.get('compaction_run_count', 0)}"
            )
        if load_stats.get("rows", 0) != expected_loaded_rows:
            raise SystemExit(
                "BENCH_CLAIM_MODE Part 31 loaded row mismatch: "
                f"loaded_rows={load_stats.get('rows', 0)} expected={expected_loaded_rows}"
            )
        if sql_block_zone_map_copy_enabled() and load_stats.get("insert_value_batches", 0) != 0:
            raise SystemExit("BENCH_CLAIM_MODE Part 31 expected COPY STDIN without INSERT fallback")
    return total_rows + expected_loaded_rows


def setup_index_topk_frontier_table(T: Dict[str, float], total_rows: int) -> int:
    decoy_sstables = index_topk_frontier_decoy_sstables()
    rows_per_sstable = index_topk_frontier_rows_per_sstable()
    target_rows = max(rows_per_sstable, INDEX_TOPK_LIMIT * 4)
    target_host = 50
    high_host = 99
    low_base_ts = 1_000
    target_base_ts = 1_000_000
    row_id = 0

    print("  [setup] Creating reverse frontier Top-K schema …")
    sql_ok(
        "CREATE TABLE bench_topk_frontier_idx ("
        "id INTEGER PRIMARY KEY, "
        "host_id INTEGER NOT NULL, "
        "ts INTEGER NOT NULL, "
        "payload TEXT)"
    )
    sql_ok(
        "CREATE INDEX idx_bench_topk_frontier_host_ts "
        "ON bench_topk_frontier_idx (host_id, ts, payload)"
    )

    print(
        "  [setup] Loading reverse frontier decoy SSTables "
        f"(decoys={decoy_sstables}, rows/sstable={rows_per_sstable}) …"
    )
    decoy_checkpoint_successes = 0
    with Timer("bench_topk_frontier_decoys") as t:
        for sstable_idx in range(decoy_sstables):
            values = []
            ts_base = low_base_ts + sstable_idx * rows_per_sstable
            for offset in range(rows_per_sstable):
                values.append(
                    gen_index_topk_frontier_row(
                        row_id,
                        target_host,
                        ts_base + offset,
                        f"frontier_decoy{sstable_idx}",
                    )
                )
                row_id += 1
            values.append(
                gen_index_topk_frontier_row(
                    row_id,
                    high_host,
                    target_base_ts + sstable_idx,
                    f"frontier_sentinel{sstable_idx}",
                )
            )
            row_id += 1
            insert_batch("bench_topk_frontier_idx", values)
            if checkpoint_storage(f"index_topk_frontier_decoy_{sstable_idx}"):
                decoy_checkpoint_successes += 1
    T["load_index_topk_frontier_decoys"] = t.ms

    print(
        "  [setup] Loading reverse frontier target SSTable "
        f"(target_host={target_host}, rows={target_rows}) …"
    )
    with Timer("bench_topk_frontier_target") as t:
        values = []
        for offset in range(target_rows):
            values.append(
                gen_index_topk_frontier_row(
                    row_id,
                    target_host,
                    target_base_ts + offset,
                    "frontier_target",
                )
            )
            row_id += 1
        insert_batch("bench_topk_frontier_idx", values)
        target_checkpoint_ok = checkpoint_storage("index_topk_frontier_target")
    T["load_index_topk_frontier_target"] = t.ms

    T["index_topk_frontier_decoy_sstables"] = decoy_sstables
    T["index_topk_frontier_rows_per_sstable"] = rows_per_sstable
    T["index_topk_frontier_target_rows"] = target_rows
    T["index_topk_frontier_target_host"] = target_host
    T["index_topk_frontier_high_host"] = high_host
    T["index_topk_frontier_low_base_ts"] = low_base_ts
    T["index_topk_frontier_target_base_ts"] = target_base_ts
    T["index_topk_frontier_decoy_checkpoint_successes"] = decoy_checkpoint_successes
    T["index_topk_frontier_target_checkpoint_ok"] = int(target_checkpoint_ok)
    if BENCH_CLAIM_MODE and (
        decoy_checkpoint_successes != decoy_sstables or not target_checkpoint_ok
    ):
        raise SystemExit(
            "BENCH_CLAIM_MODE requires all reverse frontier checkpoints to succeed"
        )
    return total_rows + row_id

def index_topk_frontier_query(host_id: int, direction: str, limit: int, lower_ts: int = 0):
    return (
        "SELECT id, host_id, ts FROM bench_topk_frontier_idx "
        f"WHERE host_id = {host_id} AND ts >= {lower_ts} "
        f"ORDER BY ts {direction} LIMIT {limit}"
    )

def setup_index_distinct_tables(T: Dict[str, float], total_rows: int) -> int:
    rows = index_distinct_rows()
    ndv = index_distinct_ndv(rows)
    print("  [setup] Creating indexed DISTINCT schema …")
    for table in ("bench_distinct_scan", "bench_distinct_idx", "bench_distinct_nullable"):
        sql_ok(
            f"CREATE TABLE {table} ("
            "id INTEGER PRIMARY KEY, k INTEGER NOT NULL, nullable_k INTEGER, payload TEXT)"
        )

    print(f"  [setup] Loading indexed DISTINCT rows ({rows}, ndv={ndv}) …")
    with Timer("bench_distinct_scan") as t:
        insert_generated_batches(
            "bench_distinct_scan",
            rows,
            lambda i: gen_index_distinct_row(i, ndv),
        )
    T["load_index_distinct_scan"] = t.ms
    with Timer("bench_distinct_idx") as t:
        insert_generated_batches(
            "bench_distinct_idx",
            rows,
            lambda i: gen_index_distinct_row(i, ndv),
        )
    T["load_index_distinct_idx"] = t.ms
    with Timer("bench_distinct_nullable") as t:
        insert_generated_batches(
            "bench_distinct_nullable",
            rows,
            lambda i: gen_index_distinct_row(i, ndv, nullable=True),
        )
    T["load_index_distinct_nullable"] = t.ms
    total_rows += rows * 3

    print("  [setup] Building indexed DISTINCT indexes …")
    with Timer("bench_distinct_indexes") as t:
        sql_ok("CREATE INDEX idx_bench_distinct_idx_k ON bench_distinct_idx (k)")
        sql_ok(
            "CREATE INDEX idx_bench_distinct_nullable_nullable_k ON bench_distinct_nullable (nullable_k)"
        )
    T["load_index_distinct_indexes"] = t.ms
    T["index_distinct_rows"] = rows
    T["index_distinct_ndv"] = ndv
    return total_rows

def setup_sql_no_fill_cache_table(T: Dict[str, float], total_rows: int) -> int:
    rows = sql_no_fill_rows()
    print("  [setup] Creating SQL no-fill cache schema …")
    sql_ok("CREATE TABLE bench_sql_no_fill_hot (id INTEGER PRIMARY KEY, payload TEXT)")
    sql_ok(
        "CREATE TABLE bench_sql_no_fill_bulk "
        "(id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)"
    )
    sql_ok("INSERT INTO bench_sql_no_fill_hot VALUES (1, 'sql_no_fill_hot_payload')")
    print(
        f"  [setup] Loading SQL no-fill cache rows "
        f"({rows}, payload={SQL_NO_FILL_PAYLOAD_BYTES} bytes) …"
    )
    with Timer("bench_sql_no_fill_bulk") as t:
        insert_generated_batches("bench_sql_no_fill_bulk", rows, gen_sql_no_fill_row)
    T["load_sql_no_fill_cache"] = t.ms
    T["sql_no_fill_rows"] = rows
    T["sql_no_fill_payload_bytes"] = SQL_NO_FILL_PAYLOAD_BYTES
    T["sql_no_fill_cache_blocks"] = SQL_NO_FILL_CACHE_BLOCKS
    checkpoint_ok = checkpoint_storage("sql_no_fill_cache")
    T["sql_no_fill_checkpoint_ok"] = int(checkpoint_ok)
    if BENCH_CLAIM_MODE and not checkpoint_ok:
        raise SystemExit("BENCH_CLAIM_MODE requires SQL no-fill checkpoint to succeed")
    return total_rows + rows + 1

def setup_sstable_range_bound_table(T: Dict[str, float], total_rows: int) -> int:
    bound_rows = sst_bound_rows()
    snapshot_rounds = max(0, SST_BOUND_SNAPSHOT_ROUNDS)
    print("  [setup] Creating SSTable range-bound schema …")
    sql_ok("CREATE TABLE bench_sst_bound (id INTEGER PRIMARY KEY, version INTEGER, payload TEXT)")
    print(
        f"  [setup] Loading SSTable range-bound rows "
        f"({bound_rows}, payload={SST_BOUND_PAYLOAD_BYTES} bytes, snapshots={snapshot_rounds + 1}) …"
    )
    with Timer("bench_sst_bound") as t:
        insert_generated_batches("bench_sst_bound", bound_rows, gen_sst_bound_row)
    T["load_sstable_range_bound"] = t.ms
    checkpoint_successes = 1 if checkpoint_storage("sst_bound_load") else 0

    for version in range(1, snapshot_rounds + 1):
        with Timer(f"bench_sst_bound update v{version}") as t:
            sql_ok(f"UPDATE bench_sst_bound SET version = {version}")
        T[f"load_sstable_range_bound_update_v{version}"] = t.ms
        if checkpoint_storage(f"sst_bound_v{version}"):
            checkpoint_successes += 1

    T["sstable_range_bound_rows"] = bound_rows
    T["sstable_range_bound_payload_bytes"] = SST_BOUND_PAYLOAD_BYTES
    T["sstable_range_bound_snapshot_rounds"] = snapshot_rounds
    T["sstable_range_bound_checkpoint_successes"] = checkpoint_successes
    return total_rows + bound_rows


def setup_sstable_prefix_bloom_tables(T: Dict[str, float], total_rows: int) -> int:
    prefix_rows = sst_prefix_bloom_rows()
    print("  [setup] Creating SSTable prefix-Bloom schemas …")
    for suffix in ("a", "m", "z"):
        sql_ok(
            f"CREATE TABLE bench_sst_prefix_{suffix} "
            "(id INTEGER PRIMARY KEY, payload TEXT)"
        )
    print(
        f"  [setup] Loading SSTable prefix-Bloom rows "
        f"({prefix_rows} each into a/z, payload={SST_PREFIX_BLOOM_PAYLOAD_BYTES} bytes) …"
    )
    with Timer("bench_sst_prefix_a") as t:
        insert_generated_batches(
            "bench_sst_prefix_a",
            prefix_rows,
            gen_sst_prefix_bloom_row("a"),
        )
    T["load_sstable_prefix_bloom_a"] = t.ms
    with Timer("bench_sst_prefix_z") as t:
        insert_generated_batches(
            "bench_sst_prefix_z",
            prefix_rows,
            gen_sst_prefix_bloom_row("z"),
        )
    T["load_sstable_prefix_bloom_z"] = t.ms
    T["sstable_prefix_bloom_checkpoint_successes"] = (
        1 if checkpoint_storage("sst_prefix_bloom_load") else 0
    )
    T["sstable_prefix_bloom_rows_per_populated_table"] = prefix_rows
    T["sstable_prefix_bloom_payload_bytes"] = SST_PREFIX_BLOOM_PAYLOAD_BYTES
    return total_rows + (prefix_rows * 2)


# ═══════════════════════════════════════════════════════════════════════════════
#  Setup — Schema & Data Loading
# ═══════════════════════════════════════════════════════════════════════════════
def setup(selected_part_keys: Optional[Set[str]] = None) -> Dict[str, float]:
    T = {}
    total_rows = 0
    selected_part_keys = selected_part_keys or set()
    wide_scan_only = selected_part_keys == {"wide_scan"}
    sst_bound_only = selected_part_keys == {"sstable_range_bound"}
    sst_prefix_bloom_only = selected_part_keys == {"sstable_prefix_bloom"}
    sst_block_prefix_only = selected_part_keys == {"sstable_block_prefix"}
    sst_block_index_prefix_only = selected_part_keys == {"sstable_block_index_prefix"}
    sst_user_key_bloom_only = selected_part_keys == {"sstable_user_key_bloom"}
    sst_no_fill_only = selected_part_keys == {"sstable_no_fill_cache"}
    sst_reverse_frontier_only = selected_part_keys == {"sstable_reverse_frontier"}
    fusion_reverse_frontier_only = selected_part_keys == {"fusion_reverse_frontier"}
    sst_startup_index_only = selected_part_keys == {"sstable_startup_index"}
    index_topk_only = selected_part_keys == {"index_topk"}
    index_topk_restart_only = selected_part_keys == {"index_topk_restart"}
    index_topk_prefix_prune_only = selected_part_keys == {"index_topk_prefix_prune"}
    sql_block_index_prefix_only = selected_part_keys == {"sql_block_index_prefix_prune"}
    sql_block_zone_map_only = selected_part_keys == {"sql_block_zone_map_prune"}
    index_topk_frontier_only = selected_part_keys == {"index_topk_frontier"}
    index_distinct_only = selected_part_keys == {"index_distinct"}
    sql_no_fill_only = selected_part_keys == {"sql_no_fill_cache"}
    column_scan_only = selected_part_keys == {"column_scan"}
    scan_micro_only = (
        bool(selected_part_keys)
        and selected_part_keys.issubset({"or_in_scan", "between_scan", "like_prefix_scan"})
    )

    print(f"\n{'═'*100}")
    print(f"  FusionDB Unified Benchmark  │  Scale: {SCALE.upper()}")
    print(f"  base_rows={C['base_rows']}  users={C['users']}  products={C['products']}  "
          f"orders={C['orders']}  accounts={C['accounts']}  events={C['events']}  threads={C['threads']}")
    if PROTO == "pg":
        print(f"  Transport: PGWIRE  │  {PG_HOST}:{PG_PORT} (user={PG_USER})")
    else:
        print(f"  Transport: HTTP+JSON  │  {BASE_URL}")
    print(f"{'═'*100}\n")

    if sst_block_prefix_only:
        T["sstable_block_prefix_sstables"] = SST_BLOCK_PREFIX_SSTABLES
        T["sstable_block_prefix_iters"] = SST_BLOCK_PREFIX_ITERS
        T["sstable_block_prefix_payload_bytes"] = SST_BLOCK_PREFIX_PAYLOAD_BYTES
        T["sstable_block_prefix_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if sst_block_index_prefix_only:
        T["sstable_block_index_prefix_sstables"] = SST_BLOCK_INDEX_PREFIX_SSTABLES
        T["sstable_block_index_prefix_iters"] = SST_BLOCK_INDEX_PREFIX_ITERS
        T["sstable_block_index_prefix_payload_bytes"] = SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES
        T["sstable_block_index_prefix_natural_prefixes"] = SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES
        T["sstable_block_index_prefix_natural_iters"] = SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS
        T["sstable_block_index_prefix_natural_payload_bytes"] = SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES
        T["sstable_block_index_prefix_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if sst_user_key_bloom_only:
        T["sstable_user_key_bloom_sstables"] = SST_USER_KEY_BLOOM_SSTABLES
        T["sstable_user_key_bloom_iters"] = SST_USER_KEY_BLOOM_ITERS
        T["sstable_user_key_bloom_payload_bytes"] = SST_USER_KEY_BLOOM_PAYLOAD_BYTES
        T["sstable_user_key_bloom_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if sst_no_fill_only:
        T["sstable_no_fill_scan_blocks"] = SST_NO_FILL_SCAN_BLOCKS
        T["sstable_no_fill_iters"] = SST_NO_FILL_ITERS
        T["sstable_no_fill_payload_bytes"] = SST_NO_FILL_PAYLOAD_BYTES
        T["sstable_no_fill_cache_blocks"] = SST_NO_FILL_CACHE_BLOCKS
        T["sstable_no_fill_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if sst_reverse_frontier_only:
        T["sstable_reverse_frontier_decoys"] = SST_REVERSE_FRONTIER_DECOYS
        T["sstable_reverse_frontier_iters"] = SST_REVERSE_FRONTIER_ITERS
        T["sstable_reverse_frontier_payload_bytes"] = SST_REVERSE_FRONTIER_PAYLOAD_BYTES
        T["sstable_reverse_frontier_cache_blocks"] = SST_REVERSE_FRONTIER_CACHE_BLOCKS
        T["sstable_reverse_frontier_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if fusion_reverse_frontier_only:
        T["fusion_reverse_frontier_decoys"] = FUSION_REVERSE_FRONTIER_DECOYS
        T["fusion_reverse_frontier_iters"] = FUSION_REVERSE_FRONTIER_ITERS
        T["fusion_reverse_frontier_payload_bytes"] = FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES
        T["fusion_reverse_frontier_cache_blocks"] = FUSION_REVERSE_FRONTIER_CACHE_BLOCKS
        T["fusion_reverse_frontier_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if sst_startup_index_only:
        T["sstable_startup_data_dir"] = SST_STARTUP_DATA_DIR
        T["sstable_startup_copy_data"] = int(SST_STARTUP_COPY_DATA)
        T["sstable_startup_scenarios"] = list(SST_STARTUP_SCENARIOS)
        T["sstable_startup_low_level_only"] = 1
        return print_load_summary(T, total_rows)

    if index_topk_restart_only:
        T["index_topk_restart_binary"] = INDEX_TOPK_RESTART_BINARY
        T["index_topk_restart_port"] = INDEX_TOPK_RESTART_PORT
        T["index_topk_restart_timeout_sec"] = INDEX_TOPK_RESTART_TIMEOUT_SEC
        T["index_topk_restart_trials"] = INDEX_TOPK_RESTART_TRIALS
        T["index_topk_restart_reset_workdir"] = int(INDEX_TOPK_RESTART_RESET_WORKDIR)
        T["index_topk_restart_os_cache_control"] = INDEX_TOPK_RESTART_OS_CACHE_CONTROL
        T["index_topk_restart_owned_server"] = 1
        return print_load_summary(T, total_rows)

    if sql_no_fill_only:
        T["sql_no_fill_rows"] = sql_no_fill_rows()
        T["sql_no_fill_payload_bytes"] = SQL_NO_FILL_PAYLOAD_BYTES
        T["sql_no_fill_cache_blocks"] = SQL_NO_FILL_CACHE_BLOCKS
        T["sql_no_fill_binary"] = SQL_NO_FILL_BINARY
        T["sql_no_fill_port"] = SQL_NO_FILL_PORT
        T["sql_no_fill_timeout_sec"] = SQL_NO_FILL_TIMEOUT_SEC
        T["sql_no_fill_reset_workdir"] = int(SQL_NO_FILL_RESET_WORKDIR)
        T["sql_no_fill_owned_server"] = 1
        T["sql_no_fill_control_phase"] = 1
        return print_load_summary(T, total_rows)

    try:
        if PROTO == "pg":
            res, _ = sql("SELECT 1")
            if res.get("status") != "ok":
                raise RuntimeError(res.get("error"))
        else:
            http_session().get(HEALTH_URL, timeout=3)
    except Exception as e:
        target = f"{PG_HOST}:{PG_PORT} (pgwire)" if PROTO == "pg" else HEALTH_URL
        print(f"  ERROR: FusionDB not reachable at {target}: {e}\n  Start with: cargo run"); sys.exit(1)

    # ── Drop ──
    print("  [setup] Dropping old tables …")
    drop_tables = (
        ["bench_wide"]
        if wide_scan_only
        else ["bench_sst_bound"]
        if sst_bound_only
        else ["bench_sst_prefix_a", "bench_sst_prefix_m", "bench_sst_prefix_z"]
        if sst_prefix_bloom_only
        else ["bench_topk_prefix_prune_idx"]
        if index_topk_prefix_prune_only
        else ["bench_topk_block_index_prefix_idx"]
        if sql_block_index_prefix_only
        else ["bench_zone_map_clustered", "bench_zone_map_random", "bench_zone_map_mvcc"]
        if sql_block_zone_map_only
        else ["bench_topk_frontier_idx"]
        if index_topk_frontier_only
        else [
            "bench_topk_scan",
            "bench_topk_idx",
            "bench_topk_cover",
            "bench_topk_types",
            "bench_topk_comp_scan",
            "bench_topk_comp_idx",
            "bench_topk_comp_cover",
        ]
        if index_topk_only
        else ["bench_distinct_scan", "bench_distinct_idx", "bench_distinct_nullable"]
        if index_distinct_only
        else ["bench_sql_no_fill_hot", "bench_sql_no_fill_bulk"]
        if sql_no_fill_only
        else [
            "bench_sst_prefix_a",
            "bench_sst_prefix_m",
            "bench_sst_prefix_z",
            "bench_sst_bound",
            "bench_wide",
            "bench_topk_scan",
            "bench_topk_idx",
            "bench_topk_cover",
            "bench_topk_types",
            "bench_topk_comp_scan",
            "bench_topk_comp_idx",
            "bench_topk_comp_cover",
            "bench_topk_prefix_prune_idx",
            "bench_topk_block_index_prefix_idx",
            "bench_zone_map_clustered",
            "bench_zone_map_random",
            "bench_zone_map_mvcc",
            "bench_topk_frontier_idx",
            "bench_distinct_scan",
            "bench_distinct_idx",
            "bench_distinct_nullable",
            "bench_sql_no_fill_hot",
            "bench_sql_no_fill_bulk",
            "jr_low","jr_high","jr_hub",
            "order_items","orders","products","users","accounts","transfers","events",
            "bench","bench_idx",
        ]
    )
    for t in drop_tables:
        sql_ok(f"DROP TABLE IF EXISTS {t}")

    if wide_scan_only:
        total_rows = setup_wide_scan_table(T, total_rows)
        return print_load_summary(T, total_rows)

    if sst_bound_only:
        total_rows = setup_sstable_range_bound_table(T, total_rows)
        return print_load_summary(T, total_rows)

    if sst_prefix_bloom_only:
        total_rows = setup_sstable_prefix_bloom_tables(T, total_rows)
        return print_load_summary(T, total_rows)

    if index_topk_only:
        total_rows = setup_index_topk_tables(T, total_rows)
        return print_load_summary(T, total_rows)

    if index_topk_prefix_prune_only:
        total_rows = setup_index_topk_prefix_prune_table(T, total_rows)
        return print_load_summary(T, total_rows)

    if sql_block_index_prefix_only:
        total_rows = setup_sql_block_index_prefix_prune_table(T, total_rows)
        return print_load_summary(T, total_rows)

    if sql_block_zone_map_only:
        total_rows = setup_sql_block_zone_map_prune_tables(T, total_rows)
        return print_load_summary(T, total_rows)

    if index_topk_frontier_only:
        total_rows = setup_index_topk_frontier_table(T, total_rows)
        return print_load_summary(T, total_rows)

    if index_distinct_only:
        total_rows = setup_index_distinct_tables(T, total_rows)
        return print_load_summary(T, total_rows)

    if sql_no_fill_only:
        total_rows = setup_sql_no_fill_cache_table(T, total_rows)
        return print_load_summary(T, total_rows)

    # ── Base benchmark tables ──
    print("  [setup] Creating base benchmark tables …")
    sql_ok("CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, category TEXT, amount FLOAT)")
    sql_ok("CREATE TABLE bench_idx (id INTEGER PRIMARY KEY, val INTEGER, category TEXT, amount FLOAT)")
    sql_ok("CREATE INDEX idx_val ON bench_idx (val)")
    sql_ok("CREATE INDEX idx_cat ON bench_idx (category)")

    print(f"  [setup] Loading base rows ({C['base_rows']}) …")
    with Timer("bench+bench_idx") as t:
        vals = gen_base(C["base_rows"])
        insert_batch("bench", vals)
        insert_batch("bench_idx", vals)
    T["load_base"] = t.ms; total_rows += C["base_rows"] * 2

    if scan_micro_only:
        total_rows = setup_wide_scan_table(T, total_rows)
        return print_load_summary(T, total_rows)

    # ── E-commerce ──
    print("  [setup] Creating e-commerce schema …")
    sql_ok("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, city TEXT, age INTEGER, score FLOAT)")
    sql_ok("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, category TEXT, price FLOAT, stock INTEGER, rating FLOAT)")
    sql_ok("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT, total FLOAT, order_day INTEGER)")
    sql_ok("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price FLOAT)")
    sql_ok("CREATE INDEX idx_orders_user ON orders (user_id)")
    sql_ok("CREATE INDEX idx_orders_status ON orders (status)")
    sql_ok("CREATE INDEX idx_oi_order ON order_items (order_id)")
    sql_ok("CREATE INDEX idx_products_cat ON products (category)")

    print(f"  [setup] Loading users ({C['users']}) …")
    with Timer("users") as t: insert_batch("users", gen_users(C["users"]))
    T["load_users"] = t.ms; total_rows += C["users"]

    print(f"  [setup] Loading products ({C['products']}) …")
    with Timer("products") as t: insert_batch("products", gen_products(C["products"]))
    T["load_products"] = t.ms; total_rows += C["products"]

    print(f"  [setup] Loading orders + items ({C['orders']}) …")
    with Timer("orders+items") as t:
        ords, itms = gen_orders(C["orders"], C["users"], C["products"])
        insert_batch("orders", ords); insert_batch("order_items", itms)
    T["load_orders"] = t.ms; total_rows += C["orders"] + len(itms)
    T["order_items_count"] = len(itms)

    # ── Financial ──
    print("  [setup] Creating financial schema …")
    sql_ok("CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, acct_type TEXT, balance FLOAT)")
    sql_ok("CREATE TABLE transfers (id INTEGER PRIMARY KEY, from_acct INTEGER, to_acct INTEGER, amount FLOAT, status TEXT, transfer_day INTEGER)")
    sql_ok("CREATE INDEX idx_transfers_status ON transfers (status)")

    print(f"  [setup] Loading accounts ({C['accounts']}) …")
    with Timer("accounts") as t: insert_batch("accounts", gen_accounts(C["accounts"]))
    T["load_accounts"] = t.ms; total_rows += C["accounts"]

    print(f"  [setup] Loading transfers ({C['transfers']}) …")
    with Timer("transfers") as t: insert_batch("transfers", gen_transfers(C["transfers"], C["accounts"]))
    T["load_transfers"] = t.ms; total_rows += C["transfers"]

    # ── Events ──
    print("  [setup] Creating events table …")
    sql_ok("CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, event_type TEXT, ts INTEGER)")
    sql_ok("CREATE INDEX idx_events_type ON events (event_type)")

    print(f"  [setup] Loading events ({C['events']}) …")
    with Timer("events") as t: insert_batch("events", gen_events(C["events"], C["users"]))
    T["load_events"] = t.ms; total_rows += C["events"]

    # ── Stats-aware join reorder micro workload ──
    hub_rows, high_rows, low_rows = join_reorder_sizes()
    print("  [setup] Creating stats-aware join reorder schema …")
    sql_ok("CREATE TABLE jr_hub (id INTEGER PRIMARY KEY, low_key INTEGER, high_key INTEGER)")
    sql_ok("CREATE TABLE jr_high (id INTEGER PRIMARY KEY, high_key INTEGER)")
    sql_ok("CREATE TABLE jr_low (id INTEGER PRIMARY KEY, low_key INTEGER)")

    print(f"  [setup] Loading join-reorder rows (hub={hub_rows}, high={high_rows}, low={low_rows}) …")
    with Timer("join-reorder") as t:
        insert_batch("jr_hub", gen_join_reorder_hub(hub_rows))
        insert_batch("jr_high", gen_join_reorder_high(high_rows))
        insert_batch("jr_low", gen_join_reorder_low(low_rows))
    T["load_join_reorder"] = t.ms; total_rows += hub_rows + high_rows + low_rows

    if "wide_scan" in selected_part_keys:
        total_rows = setup_wide_scan_table(T, total_rows)

    if "sstable_range_bound" in selected_part_keys:
        total_rows = setup_sstable_range_bound_table(T, total_rows)

    if "sstable_prefix_bloom" in selected_part_keys:
        total_rows = setup_sstable_prefix_bloom_tables(T, total_rows)

    if "index_topk" in selected_part_keys:
        total_rows = setup_index_topk_tables(T, total_rows)

    if "index_topk_prefix_prune" in selected_part_keys:
        total_rows = setup_index_topk_prefix_prune_table(T, total_rows)

    if "index_topk_frontier" in selected_part_keys:
        total_rows = setup_index_topk_frontier_table(T, total_rows)

    if "index_distinct" in selected_part_keys:
        total_rows = setup_index_distinct_tables(T, total_rows)

    if "sql_no_fill_cache" in selected_part_keys:
        total_rows = setup_sql_no_fill_cache_table(T, total_rows)

    if column_scan_only:
        print("  [setup] Checkpointing column-scan data for a clean single-SSTable window ...")
        T["columnar_single_source_checkpoint_ok"] = int(
            checkpoint_storage("columnar_single_source")
        )

    return print_load_summary(T, total_rows)


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 1 — Base Benchmarks
# ═══════════════════════════════════════════════════════════════════════════════
def part1_base() -> List[BenchResult]:
    R, cat, N = [], "Base", C["base_rows"]
    tid = N // 2; tv = random.randint(0,999)

    # Point queries
    R.append(bench("PK point lookup",       f"SELECT * FROM bench WHERE id = {tid}", cat=cat))
    R.append(bench("Full scan (val=X)",     f"SELECT * FROM bench WHERE val = {tv}", cat=cat))
    R.append(bench("Full scan narrow val=X", f"SELECT id FROM bench WHERE val = {tv}", cat=cat))
    R.append(bench("Index scan (val=X)",    f"SELECT * FROM bench_idx WHERE val = {tv}", cat=cat))

    # Range
    R.append(bench("Range id>N LIMIT 100",  f"SELECT * FROM bench WHERE id > {N//2} LIMIT 100", cat=cat))
    R.append(bench("BETWEEN range",         f"SELECT * FROM bench WHERE val BETWEEN 100 AND 200", cat=cat))
    R.append(bench("LIKE prefix",           "SELECT * FROM bench WHERE category LIKE 'elec%'", cat=cat))

    # Aggregations
    R.append(bench("COUNT(*)",              "SELECT COUNT(*) FROM bench", cat=cat))
    R.append(bench("SUM(amount)",           "SELECT SUM(amount) FROM bench", cat=cat))
    R.append(bench("GROUP BY category",     "SELECT category, COUNT(*) FROM bench GROUP BY category", cat=cat))
    R.append(bench("GROUP BY + HAVING",     "SELECT category, SUM(amount) FROM bench GROUP BY category HAVING SUM(amount) > 10000", cat=cat))

    # Sorting
    R.append(bench("ORDER BY id LIMIT 50",  "SELECT * FROM bench ORDER BY id LIMIT 50", cat=cat))
    R.append(bench("ORDER BY val DESC L50", "SELECT * FROM bench ORDER BY val DESC LIMIT 50", cat=cat))

    # Writes
    for label, tpl in [("Single INSERT", "INSERT INTO bench VALUES ({rid},999,'bw',0)"),
                        ("Single UPDATE", "UPDATE bench SET val=0 WHERE id={rid}"),
                        ("Single DELETE", "DELETE FROM bench WHERE id={rid}")]:
        r = BenchResult(name=label, category=cat, planned_iters=C["iters"])
        for i in range(C["iters"]):
            rid = N + 200000 + i
            if label == "Single INSERT":
                res, ms = sql(tpl.format(rid=rid))
            elif label == "Single UPDATE":
                res, ms = sql(tpl.format(rid=N + 200000 + i))
            else:
                res, ms = sql(tpl.format(rid=N + 200000 + i))
            if not r.record(res, ms, capture_rows=False):
                break
        R.append(r)

    # Complex filters
    R.append(bench("AND filter",    "SELECT * FROM bench WHERE val > 500 AND category = 'books' LIMIT 50", cat=cat))
    R.append(bench("OR filter",     "SELECT * FROM bench WHERE val < 10 OR val > 990 LIMIT 50", cat=cat))
    R.append(bench("IN list",       "SELECT * FROM bench WHERE val IN (100,200,300,400,500)", cat=cat))
    R.append(bench("DISTINCT",      "SELECT DISTINCT category FROM bench", cat=cat))

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 2 — E-commerce Simulation
# ═══════════════════════════════════════════════════════════════════════════════
def part2_ecommerce() -> List[BenchResult]:
    R, cat = [], "E-commerce"
    nu, np_, no = C["users"], C["products"], C["orders"]
    uid = nu // 2

    # Customer operations
    R.append(bench("Cust PK lookup",      f"SELECT * FROM users WHERE id = {uid}", cat=cat))
    R.append(bench("Cust by email",       f"SELECT * FROM users WHERE email = 'alice{uid}@mail.com'", cat=cat))
    R.append(bench("Cust by city",        "SELECT * FROM users WHERE city = 'Shanghai'", cat=cat))
    R.append(bench("Cust search LIKE",    "SELECT * FROM users WHERE name LIKE 'Alice%' LIMIT 20", cat=cat))

    # Product browsing
    R.append(bench("Products by category","SELECT * FROM products WHERE category = 'Electronics' ORDER BY rating DESC LIMIT 20", cat=cat))
    R.append(bench("Price range filter",  "SELECT * FROM products WHERE price BETWEEN 50 AND 200 ORDER BY price LIMIT 20", cat=cat))
    R.append(bench("Top rated products",  "SELECT * FROM products ORDER BY rating DESC LIMIT 10", cat=cat))
    R.append(bench("Low stock alert",     "SELECT * FROM products WHERE stock < 10", cat=cat))

    # Order history & details
    R.append(bench("User order history",  f"SELECT id, status, total FROM orders WHERE user_id = {uid} ORDER BY order_day DESC LIMIT 10", cat=cat))
    R.append(bench("Order detail (JOIN)", f"SELECT o.id, oi.product_id, oi.quantity, oi.unit_price FROM orders o INNER JOIN order_items oi ON o.id = oi.order_id WHERE o.id = {no//2} LIMIT 20", cat=cat))
    R.append(bench("Orders by status",    "SELECT * FROM orders WHERE status = 'shipped' LIMIT 50", cat=cat))

    # Write: place order
    r = BenchResult(name="Place order (INSERT)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        oid = no + 100000 + i
        res, ms = sql(f"INSERT INTO orders VALUES ({oid},{random.randint(0,nu-1)},'pending',{round(random.uniform(20,1000),2)},1300)")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    # Write: update order status
    r = BenchResult(name="Confirm order (UPDATE)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        res, ms = sql(f"UPDATE orders SET status = 'confirmed' WHERE id = {no+100000+i}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    # Write: cancel order
    r = BenchResult(name="Cancel order (UPDATE)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        res, ms = sql(f"UPDATE orders SET status = 'cancelled' WHERE id = {no+100000+i}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    # Write: deduct stock
    r = BenchResult(name="Deduct stock (UPDATE)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        res, ms = sql(f"UPDATE products SET stock = stock - 1 WHERE id = {random.randint(0,np_-1)}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 3 — Financial Ledger
# ═══════════════════════════════════════════════════════════════════════════════
def part3_financial() -> List[BenchResult]:
    R, cat = [], "Financial"
    na = C["accounts"]; aid = na // 2

    R.append(bench("Balance lookup",      f"SELECT balance FROM accounts WHERE id = {aid}", cat=cat))
    R.append(bench("Accts by type",       "SELECT * FROM accounts WHERE acct_type = 'checking'", cat=cat))
    R.append(bench("High-balance accts",  "SELECT * FROM accounts WHERE balance > 50000 ORDER BY balance DESC LIMIT 20", cat=cat))

    R.append(bench("Transfer history",    f"SELECT * FROM transfers WHERE from_acct = {aid} OR to_acct = {aid} LIMIT 20", cat=cat))
    R.append(bench("Failed transfers",    "SELECT * FROM transfers WHERE status = 'failed'", cat=cat))
    R.append(bench("Daily volume",        "SELECT transfer_day, COUNT(*), SUM(amount) FROM transfers GROUP BY transfer_day ORDER BY transfer_day DESC LIMIT 30", cat=cat))

    # Write: execute transfer
    r = BenchResult(name="Record transfer (INS)", category=cat, planned_iters=C["iters"])
    base_tid = C["transfers"] + 100000
    for i in range(C["iters"]):
        s = random.randint(0,na-1); d = random.randint(0,na-1)
        res, ms = sql(f"INSERT INTO transfers VALUES ({base_tid+i},{s},{d},{round(random.uniform(10,1000),2)},'completed',1400)")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    # Write: debit account
    r = BenchResult(name="Debit account (UPD)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        res, ms = sql(f"UPDATE accounts SET balance = balance - 10 WHERE id = {random.randint(0,na-1)}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    R.append(bench("Total bank balance",  "SELECT SUM(balance) FROM accounts", cat=cat))
    R.append(bench("Avg balance by type", "SELECT acct_type, AVG(balance), COUNT(*) FROM accounts GROUP BY acct_type", cat=cat))

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 4 — Analytics / OLAP
# ═══════════════════════════════════════════════════════════════════════════════
def part4_analytics() -> List[BenchResult]:
    R, cat = [], "Analytics"

    # Revenue
    R.append(bench("Total revenue",       "SELECT SUM(total) FROM orders WHERE status != 'cancelled'", cat=cat))
    R.append(bench("Revenue by status",   "SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status", cat=cat))
    R.append(bench("Daily revenue top30", "SELECT order_day, SUM(total), COUNT(*) FROM orders GROUP BY order_day ORDER BY order_day DESC LIMIT 30", cat=cat))
    R.append(bench("Revenue by category", "SELECT p.category, SUM(oi.quantity * oi.unit_price) FROM order_items oi INNER JOIN products p ON oi.product_id = p.id GROUP BY p.category", cat=cat))

    # Customer analytics
    R.append(bench("Top 10 spenders",     "SELECT user_id, SUM(total), COUNT(*) FROM orders GROUP BY user_id ORDER BY SUM(total) DESC LIMIT 10", cat=cat))
    R.append(bench("City distribution",   "SELECT city, COUNT(*) FROM users GROUP BY city ORDER BY COUNT(*) DESC", cat=cat))
    R.append(bench("Avg order value",     "SELECT AVG(total) FROM orders WHERE status = 'delivered'", cat=cat))

    # Product analytics
    R.append(bench("Best sellers (qty)",  "SELECT product_id, SUM(quantity) FROM order_items GROUP BY product_id ORDER BY SUM(quantity) DESC LIMIT 10", cat=cat))
    R.append(bench("Category avg price",  "SELECT category, AVG(price), MIN(price), MAX(price) FROM products GROUP BY category", cat=cat))
    R.append(bench("Never-ordered items", "SELECT COUNT(*) FROM products WHERE id NOT IN (SELECT DISTINCT product_id FROM order_items)", cat=cat))

    # Time-series events
    R.append(bench("Event counts by type","SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY COUNT(*) DESC", cat=cat))
    R.append(bench("Unique active users", "SELECT COUNT(DISTINCT user_id) FROM events", cat=cat))
    R.append(bench("Events last 7d (sim)","SELECT event_type, COUNT(*) FROM events WHERE ts > 1700000000 + 86400*23 GROUP BY event_type", cat=cat))

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 5 — Concurrent Mixed Workload
# ═══════════════════════════════════════════════════════════════════════════════
def part5_concurrent() -> List[BenchResult]:
    R, cat = [], "Concurrent"
    nu, np_, no = C["users"], C["products"], C["orders"]
    nw = C["threads"]; ops_per = C["iters"] * 5

    def read_op():
        c = random.random()
        if   c < 0.25: res, _ = sql(f"SELECT * FROM users WHERE id = {random.randint(0,nu-1)}")
        elif c < 0.45: res, _ = sql(f"SELECT * FROM products WHERE category = '{CATEGORIES[random.randint(0,len(CATEGORIES)-1)]}' LIMIT 10")
        elif c < 0.65: res, _ = sql(f"SELECT * FROM orders WHERE user_id = {random.randint(0,nu-1)} LIMIT 5")
        elif c < 0.80: res, _ = sql("SELECT status, COUNT(*) FROM orders GROUP BY status")
        elif c < 0.90: res, _ = sql("SELECT SUM(total) FROM orders WHERE status = 'delivered'")
        else:          res, _ = sql(f"SELECT * FROM events WHERE event_type = '{EVENT_TYPES[random.randint(0,len(EVENT_TYPES)-1)]}' LIMIT 20")
        return res

    def write_op(phase_id, wid, idx):
        c = random.random()
        unique_id = phase_id * nw * ops_per + wid * ops_per + idx
        oid = 800000 + unique_id
        if   c < 0.40: res, _ = sql(f"INSERT INTO orders VALUES ({oid},{random.randint(0,nu-1)},'pending',{round(random.uniform(10,500),2)},1400)")
        elif c < 0.65: res, _ = sql(f"UPDATE products SET stock = stock - 1 WHERE id = {random.randint(0,np_-1)}")
        elif c < 0.85: res, _ = sql(f"UPDATE orders SET status = 'confirmed' WHERE id = {random.randint(0,no-1)}")
        else:          res, _ = sql(f"INSERT INTO events VALUES ({700000+unique_id},{random.randint(0,nu-1)},'click',{1700000000+random.randint(0,86400*30)})")
        return res

    def run_mixed(name, read_pct, phase_id):
        lats = []; errors = []; lock = threading.Lock()
        def worker(wid):
            local_lats = []; local_errors = []
            for i in range(ops_per):
                t0 = time.perf_counter()
                if random.random() < read_pct: res = read_op()
                else: res = write_op(phase_id, wid, i)
                ms = (time.perf_counter()-t0)*1000
                if not res or res.get("status") == "error":
                    local_errors.append(str((res or {}).get("error") or "unknown error"))
                else:
                    local_lats.append(ms)
            with lock:
                lats.extend(local_lats)
                errors.extend(local_errors)

        t0 = time.perf_counter()
        threads = [threading.Thread(target=worker, args=(w,)) for w in range(nw)]
        for t in threads: t.start()
        for t in threads: t.join()
        wall = (time.perf_counter()-t0)*1000

        r = BenchResult(name=name, category=cat); r.times_ms = lats
        total_ops = nw * ops_per
        attempted_throughput = total_ops / max(wall/1000, 0.001)
        successful_throughput = len(lats) / max(wall/1000, 0.001)
        r.planned_iters = total_ops
        r.total_ops = total_ops
        r.wall_ms = wall
        r.throughput_ops_sec = attempted_throughput
        r.attempted_ops_sec = attempted_throughput
        r.successful_ops_sec = successful_throughput
        r.errors = errors
        r.error_classes = aggregate_error_classes(errors)
        if errors:
            r.error = errors[0]
        classes = ", ".join(f"{cls}:{count}" for cls, count in sorted(r.error_classes.items()))
        class_note = f" | classes {classes}" if classes else ""
        r.note = (
            f"{total_ops} ops | {len(lats)} ok | {nw} threads | wall {wall:.0f}ms | "
            f"attempted {attempted_throughput:.0f} ops/s | successful {successful_throughput:.0f} ops/s | "
            f"errors {len(errors)}{class_note}"
        )
        return r

    R.append(run_mixed("Read-heavy  (80:20)", 0.80, 0))
    R.append(run_mixed("Balanced    (50:50)", 0.50, 1))
    R.append(run_mixed("Write-heavy (20:80)", 0.20, 2))

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 6 — Stress & Edge Cases
# ═══════════════════════════════════════════════════════════════════════════════
def part6_stress() -> List[BenchResult]:
    R, cat = [], "Stress"
    no = C["orders"]

    ids = ",".join(str(random.randint(0,no-1)) for _ in range(50))
    R.append(bench("Wide IN (50 vals)",   f"SELECT * FROM orders WHERE id IN ({ids})", cat=cat))
    R.append(bench("ORDER BY 3 cols",     "SELECT * FROM users ORDER BY city, age DESC, score LIMIT 50", cat=cat))
    R.append(bench("High-card GROUP BY",  "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id", cat=cat))

    R.append(bench("3-table JOIN",
        "SELECT u.name, o.id, oi.product_id FROM users u "
        "INNER JOIN orders o ON u.id = o.user_id "
        "INNER JOIN order_items oi ON o.id = oi.order_id LIMIT 100", cat=cat))

    R.append(bench("Subquery IN",
        "SELECT * FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders WHERE total > 500) LIMIT 20", cat=cat))

    R.append(bench("COUNT all events",    "SELECT COUNT(*) FROM events", cat=cat))

    # Bulk UPDATE + restore
    r = BenchResult(name="Bulk UPDATE status", category=cat, planned_iters=1)
    res, ms = sql("UPDATE orders SET status = 'archived' WHERE status = 'cancelled'")
    r.record(res, ms, capture_rows=False)
    R.append(r)
    sql_ok("UPDATE orders SET status = 'cancelled' WHERE status = 'archived'")

    R.append(bench("DISTINCT + ORDER",    "SELECT DISTINCT city FROM users ORDER BY city", cat=cat))
    R.append(bench("UNION two SELECTs",   "SELECT id, name FROM users WHERE age < 25 UNION SELECT id, name FROM users WHERE score > 90", cat=cat))

    # CROSS JOIN small
    R.append(bench("CROSS JOIN (small)",  "SELECT a.acct_type, b.acct_type FROM accounts a CROSS JOIN accounts b LIMIT 100", cat=cat))

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 7 — Inventory & Fulfillment
# ═══════════════════════════════════════════════════════════════════════════════
def part7_inventory_fulfillment() -> List[BenchResult]:
    R, cat = [], "Inventory"
    np_, no = C["products"], C["orders"]

    R.append(bench("Stock by category",   "SELECT category, COUNT(*), SUM(stock), AVG(price) FROM products GROUP BY category ORDER BY SUM(stock) DESC", cat=cat))
    R.append(bench("Reorder candidates",  "SELECT id, name, category, stock FROM products WHERE stock < 25 ORDER BY stock LIMIT 50", cat=cat))
    R.append(bench("Available catalog",   "SELECT id, name, price, stock FROM products WHERE stock > 0 AND rating >= 4 ORDER BY rating DESC LIMIT 50", cat=cat))
    R.append(bench("Shipment queue",      "SELECT id, user_id, total, order_day FROM orders WHERE status = 'confirmed' ORDER BY order_day LIMIT 100", cat=cat))
    R.append(bench("Order reservation JOIN",
        "SELECT oi.order_id, oi.product_id, oi.quantity, p.stock FROM order_items oi "
        "INNER JOIN products p ON oi.product_id = p.id WHERE p.stock > 0 LIMIT 100", cat=cat))
    R.append(bench("Fulfillment backlog", "SELECT status, COUNT(*), SUM(total) FROM orders WHERE status IN ('pending','confirmed','shipped') GROUP BY status", cat=cat))

    r = BenchResult(name="Restock products (UPD)", category=cat, planned_iters=C["iters"])
    for _ in range(C["iters"]):
        pid = random.randint(0, np_ - 1)
        res, ms = sql(f"UPDATE products SET stock = stock + 10 WHERE id = {pid}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    r = BenchResult(name="Mark shipped (UPD)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        oid = (no // 2 + i) % no
        res, ms = sql(f"UPDATE orders SET status = 'shipped' WHERE id = {oid}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 8 — Risk & Audit
# ═══════════════════════════════════════════════════════════════════════════════
def part8_risk_audit() -> List[BenchResult]:
    R, cat = [], "Risk"
    na, nt, nu = C["accounts"], C["transfers"], C["users"]

    R.append(bench("Large transfers",     "SELECT * FROM transfers WHERE amount > 4500 ORDER BY amount DESC LIMIT 50", cat=cat))
    R.append(bench("Failed audit",        "SELECT transfer_day, COUNT(*), SUM(amount) FROM transfers WHERE status = 'failed' GROUP BY transfer_day ORDER BY transfer_day DESC LIMIT 30", cat=cat))
    R.append(bench("Account outflow",     "SELECT from_acct, COUNT(*), SUM(amount) FROM transfers WHERE status = 'completed' GROUP BY from_acct ORDER BY SUM(amount) DESC LIMIT 20", cat=cat))
    R.append(bench("Account inflow",      "SELECT to_acct, COUNT(*), SUM(amount) FROM transfers WHERE status = 'completed' GROUP BY to_acct ORDER BY SUM(amount) DESC LIMIT 20", cat=cat))
    R.append(bench("Negative balance scan","SELECT id, owner, balance FROM accounts WHERE balance < 0 ORDER BY balance LIMIT 20", cat=cat))
    R.append(bench("High spender users",  "SELECT user_id, COUNT(*), SUM(total) FROM orders WHERE total > 1000 GROUP BY user_id ORDER BY SUM(total) DESC LIMIT 20", cat=cat))
    R.append(bench("Signup audit",        "SELECT user_id, COUNT(*) FROM events WHERE event_type = 'signup' GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 20", cat=cat))
    R.append(bench("Risk review JOIN",
        "SELECT t.id, a.acct_type, t.amount FROM transfers t "
        "INNER JOIN accounts a ON t.from_acct = a.id WHERE t.amount > 4000 LIMIT 100", cat=cat))

    r = BenchResult(name="Flag transfer (UPD)", category=cat, planned_iters=C["iters"])
    for i in range(C["iters"]):
        tid = (nt // 2 + i) % nt
        res, ms = sql(f"UPDATE transfers SET status = 'review' WHERE id = {tid}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    r = BenchResult(name="Risk event insert", category=cat, planned_iters=C["iters"])
    base_eid = C["events"] + 900000
    for i in range(C["iters"]):
        uid = random.randint(0, nu - 1)
        res, ms = sql(f"INSERT INTO events VALUES ({base_eid+i},{uid},'risk_review',{1700000000+random.randint(0,86400*30)})")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    r = BenchResult(name="Balance correction (UPD)", category=cat, planned_iters=C["iters"])
    for _ in range(C["iters"]):
        aid = random.randint(0, na - 1)
        res, ms = sql(f"UPDATE accounts SET balance = balance + 1 WHERE id = {aid}")
        if not r.record(res, ms, capture_rows=False):
            break
    R.append(r)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 9 — Column-Scan Fast Paths
# ═══════════════════════════════════════════════════════════════════════════════
def apply_columnar_single_source_claim_gate(result: BenchResult) -> None:
    query_count = result.metrics_delta.get("query_count", 0)
    fast_paths = result.metrics_delta.get(
        "columnar_single_source_aggregate_fast_path_count", 0
    )
    cold_misses = result.metrics_delta.get("block_cache_miss_count", 0)
    file_opens = result.metrics_delta.get("sstable_block_file_open_count", 0)
    failures = []

    if query_count <= 0:
        failures.append("expected at least one measured query")
    if fast_paths != query_count:
        failures.append(
            f"expected one columnar single-source fast path per query, got "
            f"fast_paths={fast_paths}, queries={query_count}"
        )
    if cold_misses <= 1:
        failures.append(
            f"expected a cold multi-block scan with more than one miss, got {cold_misses}"
        )
    if file_opens <= 0:
        failures.append("expected at least one SSTable block file open")
    elif cold_misses > 1 and file_opens >= cold_misses:
        failures.append(
            f"expected query-local file reuse to keep opens below cold misses, got "
            f"opens={file_opens}, misses={cold_misses}"
        )
    if not result.result_checksums or len(set(result.result_checksums)) != 1:
        failures.append("expected a stable non-empty result checksum sequence")

    result.metadata.update({
        "claim_gate": "columnar_single_source_file_reuse",
        "claim_scope": (
            "cold multi-block query must fire the clean single-SSTable aggregate path on "
            "every measured execution, return stable checksums, and open fewer files than "
            "cold blocks"
        ),
        "claim_status": "failed" if failures else "passed",
        "claim_failures": failures,
    })
    if BENCH_CLAIM_MODE and failures:
        result.error = "columnar single-source claim failed: " + "; ".join(failures)


def part9_column_scan_fast_paths() -> List[BenchResult]:
    R, cat = [], "ColumnScan"
    focused_claim = BENCH_MATRIX == "columnar_single_source"

    cold_result = bench(
        "Bare COUNT nullable",
        "SELECT COUNT(category) FROM bench",
        warmup=0 if focused_claim else None,
        cat=cat,
    )
    R.append(cold_result)
    R.append(bench("Bare COUNT with WHERE", "SELECT COUNT(category) FROM bench WHERE val >= 500", cat=cat))
    R.append(bench("COUNT DISTINCT WHERE", "SELECT COUNT(DISTINCT user_id) FROM events WHERE event_type = 'click'", cat=cat))
    R.append(bench("DISTINCT with WHERE", "SELECT DISTINCT category FROM bench WHERE val >= 500", cat=cat))
    R.append(bench("DISTINCT ORDER LIMIT", "SELECT DISTINCT category FROM bench ORDER BY category LIMIT 5", cat=cat))
    R.append(bench("Bare MIN/MAX numeric", "SELECT MIN(amount), MAX(amount) FROM bench", cat=cat))
    R.append(bench("Bare STRING_AGG", "SELECT STRING_AGG(category) FROM bench WHERE val < 5", cat=cat))
    R.append(bench("Bare GROUP_CONCAT", "SELECT GROUP_CONCAT(category) FROM bench WHERE val < 5", cat=cat))
    R.append(bench("GROUP BY COUNT WHERE", "SELECT event_type, COUNT(*) FROM events WHERE ts > 1700000000 + 86400*23 GROUP BY event_type", cat=cat))
    R.append(bench("GROUP BY SUM WHERE", "SELECT status, SUM(total), COUNT(*) FROM orders WHERE status != 'cancelled' GROUP BY status", cat=cat))

    for result in R:
        result.metadata["matrix"] = (
            "columnar_single_source" if focused_claim else "column_scan"
        )
        annotate_block_cache_metrics(result)
    if focused_claim:
        apply_columnar_single_source_claim_gate(cold_result)
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 10 — Stats-Aware Join Reorder
# ═══════════════════════════════════════════════════════════════════════════════
JOIN_REORDER_QUERY = (
    "SELECT h.id, hi.id, lo.id FROM jr_hub h, jr_low lo, jr_high hi "
    "WHERE h.low_key = lo.low_key AND h.high_key = hi.high_key LIMIT 100"
)

def analyze_join_reorder_tables(cat: str) -> BenchResult:
    r = BenchResult(name="ANALYZE join reorder stats", category=cat, planned_iters=1)
    t0 = time.perf_counter()
    for table in ("jr_hub", "jr_high", "jr_low"):
        res, _ = sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS")
        if not res or res.get("status") == "error":
            ms = (time.perf_counter() - t0) * 1000
            r.record(res, ms, capture_rows=False)
            return r
    ms = (time.perf_counter() - t0) * 1000
    r.record({"status": "ok", "data": []}, ms, capture_rows=False)
    hub_rows, high_rows, low_rows = join_reorder_sizes()
    r.note = f"3 tables | hub {hub_rows:,} | high {high_rows:,} | low {low_rows:,}"
    return r

def part10_stats_aware_join_reorder() -> List[BenchResult]:
    R, cat = [], "Stats-Aware Join Reorder"
    hub_rows, _, low_rows = join_reorder_sizes()
    expected_old_first_rows = hub_rows * low_rows // JOIN_REORDER_NDV_BUCKETS

    no_stats = bench("NDV join reorder no stats", JOIN_REORDER_QUERY, cat=cat)
    no_stats.note = (
        f"fallback should prefer smaller jr_low first; first join approx {expected_old_first_rows:,} rows"
    )
    R.append(no_stats)

    R.append(analyze_join_reorder_tables(cat))

    with_stats = bench("NDV join reorder with stats", JOIN_REORDER_QUERY, cat=cat)
    with_stats.note = f"stats should prefer unique jr_high first; first join approx {hub_rows:,} rows"
    R.append(with_stats)
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 11 — Wide-Row Predicate-First Scan
# ═══════════════════════════════════════════════════════════════════════════════
def part11_wide_row_scan() -> List[BenchResult]:
    R, cat = [], "WideRowScan"
    rows = wide_scan_rows()
    payload_note = f"payload {WIDE_SCAN_PAYLOAD_BYTES} bytes x{WIDE_SCAN_PAYLOAD_COLUMNS}"
    wide_warmup = max(C["warmup"], 3)

    cases = [
        (
            "Wide id bucket=0",
            "SELECT id FROM bench_wide WHERE bucket = 0",
            "0.1% selectivity",
            expected_bucket_eq(rows, 0),
            {"selectivity_pct": 0.1, "projected_payload_columns": 0},
        ),
        (
            "Wide id+measure bucket<10",
            "SELECT id, measure FROM bench_wide WHERE bucket < 10",
            "1% selectivity",
            expected_bucket_lt(rows, 10),
            {"selectivity_pct": 1.0, "projected_payload_columns": 0},
        ),
        (
            "Wide one payload bucket<10",
            "SELECT id, payload_a FROM bench_wide WHERE bucket < 10",
            "1% selectivity",
            expected_bucket_lt(rows, 10),
            {"selectivity_pct": 1.0, "projected_payload_columns": 1},
        ),
        (
            "Wide id bucket<100",
            "SELECT id FROM bench_wide WHERE bucket < 100",
            "10% selectivity",
            expected_bucket_lt(rows, 100),
            {"selectivity_pct": 10.0, "projected_payload_columns": 0},
        ),
        (
            "Wide id bucket<500",
            "SELECT id FROM bench_wide WHERE bucket < 500",
            "50% selectivity",
            expected_bucket_lt(rows, 500),
            {"selectivity_pct": 50.0, "projected_payload_columns": 0},
        ),
        (
            "Wide id fallback OR",
            "SELECT id FROM bench_wide WHERE bucket = 0 OR bucket = -1",
            "0.1% selectivity | unsupported OR fallback",
            expected_bucket_eq(rows, 0),
            {"selectivity_pct": 0.1, "projected_payload_columns": 0, "fallback_control": True},
        ),
        (
            "Wide full bucket=0",
            "SELECT * FROM bench_wide WHERE bucket = 0",
            "0.1% selectivity | full-row materialization control",
            expected_bucket_eq(rows, 0),
            {"selectivity_pct": 0.1, "projected_payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS},
        ),
    ]

    for name, query, selectivity_note, expected, metadata in cases:
        result = bench_with_phase(name, query, "first-pass", cat=cat)
        result.note = f"first-pass | {selectivity_note} | expected rows {expected:,} | rows {rows:,} | {payload_note}"
        result.metadata.update({
            "wide_rows": rows,
            "expected_rows": expected,
            "warm_storage_passes": 0,
            "phase_definition": "single measured execution before benchmark stabilization and query-specific warmup",
            "payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS,
            "payload_bytes_per_column": WIDE_SCAN_PAYLOAD_BYTES,
        })
        result.metadata.update(metadata)
        R.append(result)

    stabilization = stabilize_wide_scan()

    for name, query, selectivity_note, expected, metadata in cases:
        result = bench_with_phase(name, query, "warm", warmup=wide_warmup, cat=cat)
        result.note = f"warm | {selectivity_note} | expected rows {expected:,} | rows {rows:,} | {payload_note}"
        result.metadata.update({
            "wide_rows": rows,
            "expected_rows": expected,
            "warm_storage_passes": wide_warmup,
            "phase_definition": "measured after benchmark stabilization and query-specific warmup executions",
            "stabilization": stabilization,
            "payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS,
            "payload_bytes_per_column": WIDE_SCAN_PAYLOAD_BYTES,
        })
        result.metadata.update(metadata)
        R.append(result)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 12 — OR-to-IN Predicate-First Scan
# ═══════════════════════════════════════════════════════════════════════════════
def part12_or_in_scan() -> List[BenchResult]:
    R, cat = [], "OR=IN Scan"
    wide_rows = wide_scan_rows()

    cases = [
        (
            "OR=IN narrow 2 vals [OR]",
            "SELECT id FROM bench WHERE val = 100 OR val = 200",
            {"pair": "narrow_2", "shape": "or", "projection": "id", "table": "bench"},
        ),
        (
            "OR=IN narrow 2 vals [IN]",
            "SELECT id FROM bench WHERE val IN (100,200)",
            {"pair": "narrow_2", "shape": "in", "projection": "id", "table": "bench"},
        ),
        (
            "OR=IN narrow 5 vals [OR]",
            "SELECT id FROM bench WHERE val = 100 OR val = 200 OR val = 300 OR val = 400 OR val = 500",
            {"pair": "narrow_5", "shape": "or", "projection": "id", "table": "bench"},
        ),
        (
            "OR=IN narrow 5 vals [IN]",
            "SELECT id FROM bench WHERE val IN (100,200,300,400,500)",
            {"pair": "narrow_5", "shape": "in", "projection": "id", "table": "bench"},
        ),
        (
            "OR=IN full 5 vals [OR]",
            "SELECT * FROM bench WHERE val = 100 OR val = 200 OR val = 300 OR val = 400 OR val = 500",
            {"pair": "full_5", "shape": "or", "projection": "full", "table": "bench"},
        ),
        (
            "OR=IN full 5 vals [IN]",
            "SELECT * FROM bench WHERE val IN (100,200,300,400,500)",
            {"pair": "full_5", "shape": "in", "projection": "full", "table": "bench"},
        ),
        (
            "OR=IN dup NULL [OR]",
            "SELECT id FROM bench WHERE val = 100 OR val = NULL OR val = 200 OR val = 100",
            {
                "pair": "dup_null",
                "shape": "or",
                "projection": "id",
                "table": "bench",
                "semantic_edge": "duplicate_values_and_rhs_null",
            },
        ),
        (
            "OR=IN dup NULL [IN]",
            "SELECT id FROM bench WHERE val IN (100,NULL,200,100)",
            {
                "pair": "dup_null",
                "shape": "in",
                "projection": "id",
                "table": "bench",
                "semantic_edge": "duplicate_values_and_rhs_null",
            },
        ),
        (
            "OR mixed-column fallback",
            "SELECT id FROM bench WHERE val = 100 OR category = 'books'",
            {"shape": "fallback", "fallback_reason": "mixed_columns", "table": "bench"},
        ),
        (
            "OR mixed-op fallback",
            "SELECT id FROM bench WHERE val = 100 OR val > 990",
            {"shape": "fallback", "fallback_reason": "mixed_operators", "table": "bench"},
        ),
        (
            "Wide OR=IN id 0.1% [OR]",
            "SELECT id FROM bench_wide WHERE bucket = 0 OR bucket = -1",
            {
                "pair": "wide_id_0_1pct",
                "shape": "or",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_eq(wide_rows, 0),
                "selectivity_pct": 0.1,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide OR=IN id 0.1% [IN]",
            "SELECT id FROM bench_wide WHERE bucket IN (0,-1)",
            {
                "pair": "wide_id_0_1pct",
                "shape": "in",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_eq(wide_rows, 0),
                "selectivity_pct": 0.1,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide OR=IN payload 1% [OR]",
            "SELECT id, payload_a FROM bench_wide WHERE bucket = 0 OR bucket = 1 OR bucket = 2 OR bucket = 3 OR bucket = 4 OR bucket = 5 OR bucket = 6 OR bucket = 7 OR bucket = 8 OR bucket = 9",
            {
                "pair": "wide_payload_1pct",
                "shape": "or",
                "projection": "payload_a",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 1,
            },
        ),
        (
            "Wide OR=IN payload 1% [IN]",
            "SELECT id, payload_a FROM bench_wide WHERE bucket IN (0,1,2,3,4,5,6,7,8,9)",
            {
                "pair": "wide_payload_1pct",
                "shape": "in",
                "projection": "payload_a",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 1,
            },
        ),
        (
            "Wide OR mixed fallback",
            "SELECT id FROM bench_wide WHERE bucket = 0 OR flag = 1",
            {
                "shape": "fallback",
                "fallback_reason": "mixed_columns",
                "table": "bench_wide",
                "selectivity_pct": None,
                "projected_payload_columns": 0,
            },
        ),
    ]

    stabilization = None
    for name, query, metadata in cases:
        if metadata.get("table") == "bench_wide" and stabilization is None:
            stabilization = stabilize_wide_scan()
        result = bench(name, query, cat=cat)
        result.metadata.update(metadata)
        result.metadata["matrix"] = "or_in_scan"
        if metadata.get("table") == "bench_wide" and stabilization is not None:
            result.metadata["stabilization"] = stabilization
        R.append(result)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 13 — BETWEEN Predicate-First Scan
# ═══════════════════════════════════════════════════════════════════════════════
def part13_between_scan() -> List[BenchResult]:
    R, cat = [], "BETWEEN Scan"
    wide_rows = wide_scan_rows()

    cases = [
        (
            "BETWEEN narrow point [BETWEEN]",
            "SELECT id FROM bench WHERE val BETWEEN 100 AND 100",
            {
                "pair": "narrow_point_100",
                "shape": "between",
                "projection": "id",
                "table": "bench",
            },
        ),
        (
            "BETWEEN narrow point [AND]",
            "SELECT id FROM bench WHERE val >= 100 AND val <= 100",
            {
                "pair": "narrow_point_100",
                "shape": "and_range",
                "projection": "id",
                "table": "bench",
            },
        ),
        (
            "BETWEEN narrow id [BETWEEN]",
            "SELECT id FROM bench WHERE val BETWEEN 100 AND 200",
            {
                "pair": "narrow_id_100_200",
                "shape": "between",
                "projection": "id",
                "table": "bench",
            },
        ),
        (
            "BETWEEN narrow id [AND]",
            "SELECT id FROM bench WHERE val >= 100 AND val <= 200",
            {
                "pair": "narrow_id_100_200",
                "shape": "and_range",
                "projection": "id",
                "table": "bench",
            },
        ),
        (
            "BETWEEN full row [BETWEEN]",
            "SELECT * FROM bench WHERE val BETWEEN 100 AND 200",
            {
                "pair": "full_100_200",
                "shape": "between",
                "projection": "full",
                "table": "bench",
            },
        ),
        (
            "BETWEEN full row [AND]",
            "SELECT * FROM bench WHERE val >= 100 AND val <= 200",
            {
                "pair": "full_100_200",
                "shape": "and_range",
                "projection": "full",
                "table": "bench",
            },
        ),
        (
            "BETWEEN NULL low",
            "SELECT id FROM bench WHERE val BETWEEN NULL AND 200",
            {
                "shape": "between",
                "semantic_edge": "null_low",
                "expected_rows": 0,
                "table": "bench",
            },
        ),
        (
            "BETWEEN inverted bounds",
            "SELECT id FROM bench WHERE val BETWEEN 200 AND 100",
            {
                "shape": "between",
                "semantic_edge": "inverted_bounds",
                "expected_rows": 0,
                "table": "bench",
            },
        ),
        (
            "Wide BETWEEN id 0.1% [BETWEEN]",
            "SELECT id FROM bench_wide WHERE bucket BETWEEN 0 AND 0",
            {
                "pair": "wide_id_0_1pct",
                "shape": "between",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 0),
                "selectivity_pct": 0.1,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide BETWEEN id 0.1% [AND]",
            "SELECT id FROM bench_wide WHERE bucket >= 0 AND bucket <= 0",
            {
                "pair": "wide_id_0_1pct",
                "shape": "and_range",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 0),
                "selectivity_pct": 0.1,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide BETWEEN id 1% [BETWEEN]",
            "SELECT id FROM bench_wide WHERE bucket BETWEEN 0 AND 9",
            {
                "pair": "wide_id_1pct",
                "shape": "between",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 9),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide BETWEEN id 1% [AND]",
            "SELECT id FROM bench_wide WHERE bucket >= 0 AND bucket <= 9",
            {
                "pair": "wide_id_1pct",
                "shape": "and_range",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 9),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide BETWEEN payload 1% [BETWEEN]",
            "SELECT id, payload_a FROM bench_wide WHERE bucket BETWEEN 0 AND 9",
            {
                "pair": "wide_payload_1pct",
                "shape": "between",
                "projection": "payload_a",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 9),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 1,
            },
        ),
        (
            "Wide BETWEEN payload 1% [AND]",
            "SELECT id, payload_a FROM bench_wide WHERE bucket >= 0 AND bucket <= 9",
            {
                "pair": "wide_payload_1pct",
                "shape": "and_range",
                "projection": "payload_a",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 9),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 1,
            },
        ),
        (
            "Wide BETWEEN full 1% [BETWEEN]",
            "SELECT * FROM bench_wide WHERE bucket BETWEEN 0 AND 9",
            {
                "pair": "wide_full_1pct",
                "shape": "between",
                "projection": "full",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 9),
                "selectivity_pct": 1.0,
                "projected_payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS,
            },
        ),
        (
            "Wide BETWEEN full 1% [AND]",
            "SELECT * FROM bench_wide WHERE bucket >= 0 AND bucket <= 9",
            {
                "pair": "wide_full_1pct",
                "shape": "and_range",
                "projection": "full",
                "table": "bench_wide",
                "expected_rows": expected_bucket_between(wide_rows, 0, 9),
                "selectivity_pct": 1.0,
                "projected_payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS,
            },
        ),
        (
            "Wide BETWEEN column-bound fallback",
            "SELECT id FROM bench_wide WHERE bucket BETWEEN 0 AND flag",
            {
                "shape": "fallback",
                "fallback_reason": "column_bound",
                "table": "bench_wide",
                "projected_payload_columns": 0,
            },
        ),
    ]

    stabilization = None
    for name, query, metadata in cases:
        if metadata.get("table") == "bench_wide" and stabilization is None:
            stabilization = stabilize_wide_scan()
        result = bench(name, query, cat=cat)
        result.metadata.update(metadata)
        result.metadata["matrix"] = "between_scan"
        if metadata.get("table") == "bench_wide" and stabilization is not None:
            result.metadata["stabilization"] = stabilization
        R.append(result)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 14 — LIKE Pattern Predicate-First Scan
# ═══════════════════════════════════════════════════════════════════════════════
def part14_like_prefix_scan() -> List[BenchResult]:
    R, cat = [], "LIKE Pattern Scan"
    wide_rows = wide_scan_rows()
    base_electronics_rows = expected_base_category_count(C["base_rows"], 0)

    cases = [
        (
            "LIKE narrow id 10% [prefix]",
            "SELECT id FROM bench WHERE category LIKE 'elec%'",
            {
                "pair": "narrow_id_10pct",
                "shape": "prefix",
                "projection": "id",
                "table": "bench",
                "expected_rows": base_electronics_rows,
                "selectivity_pct": 10.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "LIKE narrow id 10% [leading-wildcard]",
            "SELECT id FROM bench WHERE category LIKE '%onics'",
            {
                "pair": "narrow_id_10pct",
                "shape": "leading_wildcard",
                "projection": "id",
                "table": "bench",
                "expected_rows": base_electronics_rows,
                "selectivity_pct": 10.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "LIKE narrow id 10% [underscore]",
            "SELECT id FROM bench WHERE category LIKE 'electronic_'",
            {
                "pair": "narrow_id_10pct",
                "shape": "underscore_wildcard",
                "projection": "id",
                "table": "bench",
                "expected_rows": base_electronics_rows,
                "selectivity_pct": 10.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "LIKE narrow full 10% [prefix]",
            "SELECT * FROM bench WHERE category LIKE 'elec%'",
            {
                "pair": "narrow_full_10pct",
                "shape": "prefix",
                "projection": "full",
                "table": "bench",
                "expected_rows": base_electronics_rows,
                "selectivity_pct": 10.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "LIKE narrow zero [prefix]",
            "SELECT id FROM bench WHERE category LIKE 'zzz%'",
            {
                "shape": "prefix",
                "semantic_edge": "zero_match",
                "projection": "id",
                "table": "bench",
                "expected_rows": 0,
                "selectivity_pct": 0.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "LIKE NULL pattern fallback",
            "SELECT id FROM bench WHERE category LIKE NULL",
            {
                "shape": "fallback",
                "fallback_reason": "null_pattern",
                "semantic_edge": "unknown_filtered",
                "projection": "id",
                "table": "bench",
                "expected_rows": 0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide LIKE id 0.1% [prefix]",
            "SELECT id FROM bench_wide WHERE label LIKE 'grp000%'",
            {
                "pair": "wide_id_0_1pct",
                "shape": "prefix",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_eq(wide_rows, 0),
                "selectivity_pct": 0.1,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide LIKE id 0.1% [leading-wildcard]",
            "SELECT id FROM bench_wide WHERE label LIKE '%000'",
            {
                "pair": "wide_id_0_1pct",
                "shape": "leading_wildcard",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_eq(wide_rows, 0),
                "selectivity_pct": 0.1,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide LIKE id 1% [prefix]",
            "SELECT id FROM bench_wide WHERE label LIKE 'grp00%'",
            {
                "pair": "wide_id_1pct",
                "shape": "prefix",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide LIKE id 1% [underscore]",
            "SELECT id FROM bench_wide WHERE label LIKE 'grp00_'",
            {
                "pair": "wide_id_1pct",
                "shape": "underscore_wildcard",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide LIKE payload 1% [prefix]",
            "SELECT id, payload_a FROM bench_wide WHERE label LIKE 'grp00%'",
            {
                "pair": "wide_payload_1pct",
                "shape": "prefix",
                "projection": "payload_a",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 1,
            },
        ),
        (
            "Wide LIKE payload 1% [underscore]",
            "SELECT id, payload_a FROM bench_wide WHERE label LIKE 'grp00_'",
            {
                "pair": "wide_payload_1pct",
                "shape": "underscore_wildcard",
                "projection": "payload_a",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": 1,
            },
        ),
        (
            "Wide LIKE full 1% [prefix]",
            "SELECT * FROM bench_wide WHERE label LIKE 'grp00%'",
            {
                "pair": "wide_full_1pct",
                "shape": "prefix",
                "projection": "full",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS,
            },
        ),
        (
            "Wide LIKE full 1% [underscore]",
            "SELECT * FROM bench_wide WHERE label LIKE 'grp00_'",
            {
                "pair": "wide_full_1pct",
                "shape": "underscore_wildcard",
                "projection": "full",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 10),
                "selectivity_pct": 1.0,
                "projected_payload_columns": WIDE_SCAN_PAYLOAD_COLUMNS,
            },
        ),
        (
            "Wide LIKE id 10% [prefix]",
            "SELECT id FROM bench_wide WHERE label LIKE 'grp0%'",
            {
                "pair": "wide_id_10pct",
                "shape": "prefix",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 100),
                "selectivity_pct": 10.0,
                "projected_payload_columns": 0,
            },
        ),
        (
            "Wide LIKE id 10% [underscore]",
            "SELECT id FROM bench_wide WHERE label LIKE 'grp0__'",
            {
                "pair": "wide_id_10pct",
                "shape": "underscore_wildcard",
                "projection": "id",
                "table": "bench_wide",
                "expected_rows": expected_bucket_lt(wide_rows, 100),
                "selectivity_pct": 10.0,
                "projected_payload_columns": 0,
            },
        ),
    ]

    stabilization = None
    for name, query, metadata in cases:
        is_wide = metadata.get("table") == "bench_wide"
        if is_wide and stabilization is None:
            stabilization = stabilize_wide_scan()
        result = bench(
            name,
            query,
            warmup=max(C["warmup"], 3) if is_wide else None,
            cat=cat,
        )
        result.metadata.update(metadata)
        result.metadata["matrix"] = "like_prefix_scan"
        if is_wide and stabilization is not None:
            result.metadata["stabilization"] = stabilization
        R.append(result)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 15 — SSTable Range Upper Bound
# ═══════════════════════════════════════════════════════════════════════════════
def part15_sstable_range_bound() -> List[BenchResult]:
    R, cat = [], "SSTable Range Bound"
    rows = sst_bound_rows()
    snapshot_rounds = max(0, SST_BOUND_SNAPSHOT_ROUNDS)
    warmup = max(C["warmup"], 3)

    def range_start(fraction: float, width: int) -> int:
        if rows <= width:
            return 0
        return min(max(int(rows * fraction), 0), rows - width)

    one_start = range_start(0.25, 1)
    empty_start = range_start(0.33, 1)
    hundred_width = min(100, rows)
    hundred_start = range_start(0.50, hundred_width)
    thousand_width = min(1000, rows)
    thousand_start = range_start(0.75, thousand_width)

    cases = [
        (
            "SST bound one row",
            f"SELECT id, version FROM bench_sst_bound WHERE id >= {one_start} AND id < {one_start + 1}",
            {
                "shape": "pk_range",
                "range_width": 1,
                "range_start": one_start,
                "range_end_exclusive": one_start + 1,
                "expected_rows": 1 if rows > 0 else 0,
            },
        ),
        (
            "SST bound empty range",
            f"SELECT id, version FROM bench_sst_bound WHERE id >= {empty_start} AND id < {empty_start}",
            {
                "shape": "pk_empty_range",
                "range_width": 0,
                "range_start": empty_start,
                "range_end_exclusive": empty_start,
                "expected_rows": 0,
                "semantic_edge": "empty_exclusive_range",
            },
        ),
        (
            "SST bound 100 rows",
            f"SELECT id, version FROM bench_sst_bound WHERE id >= {hundred_start} AND id < {hundred_start + hundred_width}",
            {
                "shape": "pk_range",
                "range_width": hundred_width,
                "range_start": hundred_start,
                "range_end_exclusive": hundred_start + hundred_width,
                "expected_rows": hundred_width,
            },
        ),
        (
            "SST bound 1000 rows",
            f"SELECT id, version FROM bench_sst_bound WHERE id >= {thousand_start} AND id < {thousand_start + thousand_width}",
            {
                "shape": "pk_range",
                "range_width": thousand_width,
                "range_start": thousand_start,
                "range_end_exclusive": thousand_start + thousand_width,
                "expected_rows": thousand_width,
            },
        ),
        (
            "SST bound full scan control",
            "SELECT id, version FROM bench_sst_bound",
            {
                "shape": "full_scan_control",
                "range_width": rows,
                "range_start": 0,
                "range_end_exclusive": rows,
                "expected_rows": rows,
                "control": True,
            },
        ),
    ]

    common_metadata = {
        "matrix": "sstable_range_bound",
        "table": "bench_sst_bound",
        "payload_bytes": SST_BOUND_PAYLOAD_BYTES,
        "rows": rows,
        "snapshot_rounds": snapshot_rounds,
        "sstable_versions": snapshot_rounds + 1,
        "measurement_guidance": "Use block_read_requests_per_iter and cold_block_loads_per_iter as primary evidence; latency is secondary.",
    }

    for name, query, metadata in cases:
        for phase in ("first-pass", "warm"):
            result = bench_with_phase(name, query, phase, warmup=warmup, cat=cat)
            result.metadata.update(common_metadata)
            result.metadata.update(metadata)
            annotate_block_cache_metrics(result)
            expected = metadata["expected_rows"]
            result.note = (
                f"{phase} | expected rows {expected:,} | rows {rows:,} | "
                f"payload {SST_BOUND_PAYLOAD_BYTES} bytes | "
                f"block requests/query {result.metadata['block_read_requests_per_query']}"
            )
            if result.row_count != expected and not result.error:
                result.error = f"expected {expected} rows, got {result.row_count}"
            R.append(result)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 16 — SSTable Prefix Bloom Filter
# ═══════════════════════════════════════════════════════════════════════════════
def part16_sstable_prefix_bloom() -> List[BenchResult]:
    R, cat = [], "SSTable Prefix Bloom"
    prefix_rows = sst_prefix_bloom_rows()
    warmup = max(C["warmup"], 3)
    pk_range_width = 2 if prefix_rows >= 2 else 1
    mid = min(max(prefix_rows // 2, 0), max(prefix_rows - pk_range_width, 0))

    cases = [
        (
            "SST prefix absent table",
            "SELECT id FROM bench_sst_prefix_m",
            {
                "shape": "prefix_absent_negative",
                "table": "bench_sst_prefix_m",
                "expected_rows": 0,
                "expected_prefix_skips": "positive",
                "expected_prefix_checks": "positive",
            },
        ),
        (
            "SST prefix positive table",
            "SELECT id FROM bench_sst_prefix_a",
            {
                "shape": "prefix_positive_control",
                "table": "bench_sst_prefix_a",
                "expected_rows": prefix_rows,
                "expected_prefix_positives": "positive",
                "expected_prefix_checks": "positive",
            },
        ),
        (
            "SST prefix pk-range control",
            f"SELECT id FROM bench_sst_prefix_a WHERE id >= {mid} AND id < {mid + pk_range_width}",
            {
                "shape": "non_prefix_safe_pk_range_control",
                "table": "bench_sst_prefix_a",
                "range_width": pk_range_width,
                "expected_rows": pk_range_width if prefix_rows > 0 else 0,
                "expected_prefix_checks": "zero",
            },
        ),
    ]

    common_metadata = {
        "matrix": "sstable_prefix_bloom",
        "payload_bytes": SST_PREFIX_BLOOM_PAYLOAD_BYTES,
        "rows_per_populated_table": prefix_rows,
        "populated_tables": ["bench_sst_prefix_a", "bench_sst_prefix_z"],
        "absent_table": "bench_sst_prefix_m",
        "measurement_guidance": "Absent-table prefix scans should show prefix_filter_skips_per_query > 0 with low block_read_requests_per_query.",
    }

    prefix_metric_keys = (
        "sstable_prefix_filter_check_count",
        "sstable_prefix_filter_positive_count",
        "sstable_prefix_filter_skip_count",
        "sstable_prefix_filter_fail_open_count",
        "sstable_index_prefix_filter_check_count",
        "sstable_index_prefix_filter_positive_count",
        "sstable_index_prefix_filter_skip_count",
        "sstable_index_prefix_filter_fail_open_count",
        "sstable_block_prefix_filter_check_count",
        "sstable_block_prefix_filter_positive_count",
        "sstable_block_prefix_filter_skip_count",
        "sstable_block_prefix_filter_fail_open_count",
    )

    for name, query, metadata in cases:
        for phase in ("first-pass", "warm"):
            result = bench_with_phase(name, query, phase, warmup=warmup, cat=cat)
            result.metadata.update(common_metadata)
            result.metadata.update(metadata)
            annotate_block_cache_metrics(result)
            annotate_prefix_filter_metrics(result)

            expected = metadata["expected_rows"]
            checks = result.metadata["prefix_filter_checks"]
            positives = result.metadata["prefix_filter_positives"]
            skips = result.metadata["prefix_filter_skips"]
            result.note = (
                f"{phase} | expected rows {expected:,} | rows/table {prefix_rows:,} | "
                f"prefix checks/query {result.metadata['prefix_filter_checks_per_query']} | "
                f"skips/query {result.metadata['prefix_filter_skips_per_query']} | "
                f"block skips/query {result.metadata['block_prefix_filter_skips_per_query']} | "
                f"fail-open/query {result.metadata['prefix_filter_fail_opens_per_query']} | "
                f"block requests/query {result.metadata['block_read_requests_per_query']}"
            )

            if result.row_count != expected and not result.error:
                result.error = f"expected {expected} rows, got {result.row_count}"
            elif not any(key in result.metrics_delta for key in prefix_metric_keys):
                result.error = "prefix Bloom metrics unavailable"
            elif metadata.get("expected_prefix_skips") == "positive" and skips <= 0:
                result.error = "expected prefix Bloom negative skip count to increase"
            elif metadata.get("expected_prefix_positives") == "positive" and positives <= 0:
                result.error = "expected prefix Bloom positive count to increase"
            elif metadata.get("expected_prefix_checks") == "positive" and checks <= 0:
                result.error = "expected prefix Bloom check count to increase"
            elif metadata.get("expected_prefix_checks") == "zero" and checks != 0:
                result.error = f"expected no prefix Bloom checks for non-prefix-safe range, got {checks}"

            R.append(result)

    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 17 — SSTable Block Prefix Property Filter
# ═══════════════════════════════════════════════════════════════════════════════
def run_sstable_block_prefix_microbench() -> Dict[str, object]:
    cmd = ["cargo", "run", "--quiet"]
    if SST_BLOCK_PREFIX_RELEASE:
        cmd.append("--release")
    cmd.extend(["--bin", "sstable-block-prefix-bench"])

    env = os.environ.copy()
    env["BENCH_SST_BLOCK_PREFIX_SSTABLES"] = str(SST_BLOCK_PREFIX_SSTABLES)
    env["BENCH_SST_BLOCK_PREFIX_ITERS"] = str(SST_BLOCK_PREFIX_ITERS)
    env["BENCH_SST_BLOCK_PREFIX_PAYLOAD_BYTES"] = str(SST_BLOCK_PREFIX_PAYLOAD_BYTES)

    completed = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        text=True,
        capture_output=True,
        timeout=SST_BLOCK_PREFIX_TIMEOUT_SEC,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {detail[:1000]}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"sstable block-prefix microbench returned non-JSON output: {completed.stdout[:1000]}"
        ) from exc


def sstable_block_prefix_result(
    report: Dict[str, object],
    phase_key: str,
    display_name: str,
    speedup: float,
) -> BenchResult:
    phase = report.get(phase_key) or {}
    config = report.get("config") or {}
    metrics_delta = phase.get("metrics_delta") if isinstance(phase, dict) else {}
    if not isinstance(metrics_delta, dict):
        metrics_delta = {}

    result = BenchResult(
        name=display_name,
        category="SSTable Block Prefix",
        planned_iters=int(config.get("iters", SST_BLOCK_PREFIX_ITERS)),
        warmup_iters=0,
    )
    result.times_ms = [float(value) for value in phase.get("times_ms", [])]
    result.row_count = int(phase.get("row_count", 0))
    result.metrics_delta = {
        key: int(value) for key, value in metrics_delta.items() if isinstance(value, (int, float))
    }
    result.metadata.update({
        "matrix": "sstable_block_prefix",
        "phase": phase_key,
        "sstables": int(config.get("sstable_count", SST_BLOCK_PREFIX_SSTABLES)),
        "payload_bytes": int(config.get("payload_bytes", SST_BLOCK_PREFIX_PAYLOAD_BYTES)),
        "iters": int(config.get("iters", SST_BLOCK_PREFIX_ITERS)),
        "speedup_vs_fail_open": round(speedup, 3),
        "measurement_guidance": "Optimized should show block_prefix_filter_skips == sstables*iters and zero block cache misses; fail-open should show one miss per SSTable per iteration.",
    })

    sstables = int(result.metadata["sstables"])
    iters = int(result.metadata["iters"])
    expected_checks = sstables * iters
    block_misses = result.metrics_delta.get("block_cache_miss_count", 0)
    block_skips = result.metrics_delta.get("sstable_block_prefix_filter_skip_count", 0)
    fail_opens = result.metrics_delta.get("sstable_block_prefix_filter_fail_open_count", 0)
    result.note = (
        f"sstables {sstables:,} | iters {iters} | "
        f"avg {result.avg:.3f}ms | misses {block_misses:,} | "
        f"block skips {block_skips:,} | fail-opens {fail_opens:,} | "
        f"speedup {speedup:.2f}x"
    )

    if result.row_count != 0:
        result.error = f"expected 0 rows for absent-prefix scan, got {result.row_count}"
    elif phase_key == "optimized" and block_skips != expected_checks:
        result.error = f"expected {expected_checks} block-prefix skips, got {block_skips}"
    elif phase_key == "optimized" and block_misses != 0:
        result.error = f"expected optimized scan to read zero blocks, got {block_misses} misses"
    elif phase_key == "fail_open" and fail_opens != expected_checks:
        result.error = f"expected {expected_checks} fail-open probes, got {fail_opens}"
    elif phase_key == "fail_open" and block_misses != expected_checks:
        result.error = f"expected {expected_checks} cold block misses, got {block_misses}"

    return result


def part17_sstable_block_prefix() -> List[BenchResult]:
    try:
        report = run_sstable_block_prefix_microbench()
    except Exception as exc:
        result = BenchResult(
            name="SST block-prefix microbench",
            category="SSTable Block Prefix",
            planned_iters=SST_BLOCK_PREFIX_ITERS,
            warmup_iters=0,
        )
        result.error = str(exc)
        result.metadata.update({
            "matrix": "sstable_block_prefix",
            "sstables": SST_BLOCK_PREFIX_SSTABLES,
            "payload_bytes": SST_BLOCK_PREFIX_PAYLOAD_BYTES,
            "iters": SST_BLOCK_PREFIX_ITERS,
        })
        return [result]

    speedup = float(report.get("speedup_vs_fail_open") or 0.0)
    return [
        sstable_block_prefix_result(
            report,
            "optimized",
            "SST block-prefix optimized",
            speedup,
        ),
        sstable_block_prefix_result(
            report,
            "fail_open",
            "SST block-prefix fail-open control",
            speedup,
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 29 — SSTable Block SQL Index-Prefix Property Filter
# ═══════════════════════════════════════════════════════════════════════════════
def run_sstable_block_index_prefix_microbench() -> Dict[str, object]:
    cmd = ["cargo", "run", "--quiet"]
    if SST_BLOCK_INDEX_PREFIX_RELEASE:
        cmd.append("--release")
    cmd.extend(["--bin", "sstable-block-index-prefix-bench"])

    env = os.environ.copy()
    env["BENCH_SST_BLOCK_INDEX_PREFIX_SSTABLES"] = str(SST_BLOCK_INDEX_PREFIX_SSTABLES)
    env["BENCH_SST_BLOCK_INDEX_PREFIX_ITERS"] = str(SST_BLOCK_INDEX_PREFIX_ITERS)
    env["BENCH_SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES"] = str(SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES)
    env["BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES"] = str(SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES)
    env["BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS"] = str(SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS)
    env["BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES"] = str(SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES)
    env["BENCH_SST_BLOCK_INDEX_PREFIX_NATURAL_CANDIDATES"] = str(SST_BLOCK_INDEX_PREFIX_NATURAL_CANDIDATES)

    completed = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        text=True,
        capture_output=True,
        timeout=SST_BLOCK_INDEX_PREFIX_TIMEOUT_SEC,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {detail[:1000]}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"sstable block index-prefix microbench returned non-JSON output: {completed.stdout[:1000]}"
        ) from exc


def sstable_block_index_prefix_result(
    report: Dict[str, object],
    phase_key: str,
    display_name: str,
    speedup: float,
) -> BenchResult:
    phase = report.get(phase_key) or {}
    config = report.get("config") or {}
    metrics_delta = phase.get("metrics_delta") if isinstance(phase, dict) else {}
    if not isinstance(metrics_delta, dict):
        metrics_delta = {}

    result = BenchResult(
        name=display_name,
        category="SSTable Block Index-Prefix",
        planned_iters=int(
            config.get("natural_iters", SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS)
            if phase_key == "natural_false_positive"
            else config.get("iters", SST_BLOCK_INDEX_PREFIX_ITERS)
        ),
        warmup_iters=0,
    )
    result.times_ms = [float(value) for value in phase.get("times_ms", [])]
    result.row_count = int(phase.get("row_count", 0))
    result.metrics_delta = {
        key: int(value) for key, value in metrics_delta.items() if isinstance(value, (int, float))
    }
    result.metadata.update({
        "matrix": "sstable_block_index_prefix",
        "phase": phase_key,
        "sstables": int(config.get("sstable_count", SST_BLOCK_INDEX_PREFIX_SSTABLES)),
        "payload_bytes": int(config.get("payload_bytes", SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES)),
        "iters": int(config.get("iters", SST_BLOCK_INDEX_PREFIX_ITERS)),
        "natural_prefixes": int(config.get("natural_prefixes", SST_BLOCK_INDEX_PREFIX_NATURAL_PREFIXES)),
        "natural_iters": int(config.get("natural_iters", SST_BLOCK_INDEX_PREFIX_NATURAL_ITERS)),
        "natural_payload_bytes": int(config.get("natural_payload_bytes", SST_BLOCK_INDEX_PREFIX_NATURAL_PAYLOAD_BYTES)),
        "target_prefix": report.get("target_prefix"),
        "natural_target_prefix": report.get("natural_target_prefix"),
        "speedup_vs_fail_open": round(speedup, 3),
        "measurement_guidance": (
            "Optimized should show block_index_prefix_filter_skips == sstables*iters and zero "
            "block cache misses; fail-open and incomplete controls should show one fail-open "
            "and one cold block miss per SSTable per iteration. Natural false-positive should "
            "prove file-level Bloom MayMatch from real inserted prefixes without synthetic "
            "filter-only keys."
        ),
    })

    if phase_key == "natural_false_positive":
        sstables = 1
        iters = int(result.metadata["natural_iters"])
    else:
        sstables = int(result.metadata["sstables"])
        iters = int(result.metadata["iters"])
    expected_checks = sstables * iters
    block_misses = result.metrics_delta.get("block_cache_miss_count", 0)
    table_checks = result.metrics_delta.get("sstable_index_prefix_filter_check_count", 0)
    table_positives = result.metrics_delta.get("sstable_index_prefix_filter_positive_count", 0)
    table_skips = result.metrics_delta.get("sstable_index_prefix_filter_skip_count", 0)
    table_fail_opens = result.metrics_delta.get("sstable_index_prefix_filter_fail_open_count", 0)
    block_skips = result.metrics_delta.get("sstable_block_index_prefix_filter_skip_count", 0)
    positives = result.metrics_delta.get("sstable_block_index_prefix_filter_positive_count", 0)
    fail_opens = result.metrics_delta.get("sstable_block_index_prefix_filter_fail_open_count", 0)
    checks = result.metrics_delta.get("sstable_block_index_prefix_filter_check_count", 0)
    result.note = (
        f"sstables {sstables:,} | iters {iters} | "
        f"avg {result.avg:.3f}ms | misses {block_misses:,} | "
        f"table positives {table_positives:,} | block index skips {block_skips:,} | "
        f"block positives {positives:,} | block fail-opens {fail_opens:,} | "
        f"speedup {speedup:.2f}x"
    )

    if result.row_count != 0:
        result.error = f"expected 0 rows for absent index-prefix scan, got {result.row_count}"
    elif table_checks != expected_checks:
        result.error = f"expected {expected_checks} SSTable index-prefix checks, got {table_checks}"
    elif table_skips != 0:
        result.error = f"expected zero SSTable index-prefix skips in block-level gate, got {table_skips}"
    elif phase_key in {"optimized", "incomplete", "natural_false_positive"} and table_positives != expected_checks:
        result.error = f"expected {expected_checks} SSTable index-prefix positives, got {table_positives}"
    elif phase_key in {"optimized", "incomplete", "natural_false_positive"} and table_fail_opens != 0:
        result.error = f"expected zero SSTable index-prefix fail-opens, got {table_fail_opens}"
    elif phase_key == "fail_open" and table_fail_opens != expected_checks:
        result.error = f"expected {expected_checks} SSTable index-prefix fail-open probes, got {table_fail_opens}"
    elif checks != expected_checks:
        result.error = f"expected {expected_checks} block-index-prefix checks, got {checks}"
    elif phase_key in {"optimized", "natural_false_positive"} and block_skips != expected_checks:
        result.error = f"expected {expected_checks} block-index-prefix skips, got {block_skips}"
    elif phase_key in {"optimized", "natural_false_positive"} and fail_opens != 0:
        result.error = f"expected block-index scan to have zero fail-open probes, got {fail_opens}"
    elif phase_key in {"optimized", "natural_false_positive"} and block_misses != 0:
        result.error = f"expected block-index scan to read zero blocks, got {block_misses} misses"
    elif phase_key in {"fail_open", "incomplete"} and fail_opens != expected_checks:
        result.error = f"expected {expected_checks} fail-open probes, got {fail_opens}"
    elif phase_key in {"fail_open", "incomplete"} and block_misses != expected_checks:
        result.error = f"expected {expected_checks} cold block misses, got {block_misses}"

    return result


def part29_sstable_block_index_prefix() -> List[BenchResult]:
    try:
        report = run_sstable_block_index_prefix_microbench()
    except Exception as exc:
        result = BenchResult(
            name="SST block index-prefix microbench",
            category="SSTable Block Index-Prefix",
            planned_iters=SST_BLOCK_INDEX_PREFIX_ITERS,
            warmup_iters=0,
        )
        result.error = str(exc)
        result.metadata.update({
            "matrix": "sstable_block_index_prefix",
            "sstables": SST_BLOCK_INDEX_PREFIX_SSTABLES,
            "payload_bytes": SST_BLOCK_INDEX_PREFIX_PAYLOAD_BYTES,
            "iters": SST_BLOCK_INDEX_PREFIX_ITERS,
        })
        return [result]

    speedup = float(report.get("speedup_vs_fail_open") or 0.0)
    return [
        sstable_block_index_prefix_result(
            report,
            "optimized",
            "SST block index-prefix optimized",
            speedup,
        ),
        sstable_block_index_prefix_result(
            report,
            "fail_open",
            "SST block index-prefix fail-open control",
            speedup,
        ),
        sstable_block_index_prefix_result(
            report,
            "incomplete",
            "SST block index-prefix incomplete control",
            speedup,
        ),
        sstable_block_index_prefix_result(
            report,
            "natural_false_positive",
            "SST block index-prefix natural false-positive",
            speedup,
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 18 — SSTable MVCC User-Key Bloom Filter
# ═══════════════════════════════════════════════════════════════════════════════
def run_sstable_user_key_bloom_microbench() -> Dict[str, object]:
    cmd = ["cargo", "run", "--quiet"]
    if SST_USER_KEY_BLOOM_RELEASE:
        cmd.append("--release")
    cmd.extend(["--bin", "sstable-user-key-bloom-bench"])

    env = os.environ.copy()
    env["BENCH_SST_USER_KEY_BLOOM_SSTABLES"] = str(SST_USER_KEY_BLOOM_SSTABLES)
    env["BENCH_SST_USER_KEY_BLOOM_ITERS"] = str(SST_USER_KEY_BLOOM_ITERS)
    env["BENCH_SST_USER_KEY_BLOOM_PAYLOAD_BYTES"] = str(SST_USER_KEY_BLOOM_PAYLOAD_BYTES)

    completed = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        text=True,
        capture_output=True,
        timeout=SST_USER_KEY_BLOOM_TIMEOUT_SEC,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {detail[:1000]}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"sstable user-key Bloom microbench returned non-JSON output: {completed.stdout[:1000]}"
        ) from exc


def sstable_user_key_bloom_result(
    report: Dict[str, object],
    phase_key: str,
    display_name: str,
    speedup: float,
) -> BenchResult:
    phase = report.get(phase_key) or {}
    config = report.get("config") or {}
    metrics_delta = phase.get("metrics_delta") if isinstance(phase, dict) else {}
    if not isinstance(metrics_delta, dict):
        metrics_delta = {}

    result = BenchResult(
        name=display_name,
        category="SSTable User-Key Bloom",
        planned_iters=int(config.get("iters", SST_USER_KEY_BLOOM_ITERS)),
        warmup_iters=0,
    )
    result.times_ms = [float(value) for value in phase.get("times_ms", [])]
    result.row_count = int(phase.get("row_count", 0))
    result.metrics_delta = {
        key: int(value) for key, value in metrics_delta.items() if isinstance(value, (int, float))
    }
    result.metadata.update({
        "matrix": "sstable_user_key_bloom",
        "phase": phase_key,
        "sstables": int(config.get("sstable_count", SST_USER_KEY_BLOOM_SSTABLES)),
        "payload_bytes": int(config.get("payload_bytes", SST_USER_KEY_BLOOM_PAYLOAD_BYTES)),
        "iters": int(config.get("iters", SST_USER_KEY_BLOOM_ITERS)),
        "speedup_vs_fail_open": round(speedup, 3),
        "measurement_guidance": "Optimized should show user_key_filter_skips == sstables*iters and zero block cache misses; fail-open should show one miss per SSTable per iteration.",
    })

    sstables = int(result.metadata["sstables"])
    iters = int(result.metadata["iters"])
    expected_checks = sstables * iters
    block_misses = result.metrics_delta.get("block_cache_miss_count", 0)
    user_key_skips = result.metrics_delta.get("sstable_user_key_filter_skip_count", 0)
    fail_opens = result.metrics_delta.get("sstable_user_key_filter_fail_open_count", 0)
    result.note = (
        f"sstables {sstables:,} | iters {iters} | "
        f"avg {result.avg:.3f}ms | misses {block_misses:,} | "
        f"user-key skips {user_key_skips:,} | fail-opens {fail_opens:,} | "
        f"speedup {speedup:.2f}x"
    )

    if result.row_count != 0:
        result.error = f"expected 0 rows for absent point get, got {result.row_count}"
    elif phase_key == "optimized" and user_key_skips != expected_checks:
        result.error = f"expected {expected_checks} user-key Bloom skips, got {user_key_skips}"
    elif phase_key == "optimized" and block_misses != 0:
        result.error = f"expected optimized point get to read zero blocks, got {block_misses} misses"
    elif phase_key == "fail_open" and fail_opens != expected_checks:
        result.error = f"expected {expected_checks} fail-open probes, got {fail_opens}"
    elif phase_key == "fail_open" and block_misses != expected_checks:
        result.error = f"expected {expected_checks} cold block misses, got {block_misses}"

    return result


def part18_sstable_user_key_bloom() -> List[BenchResult]:
    try:
        report = run_sstable_user_key_bloom_microbench()
    except Exception as exc:
        result = BenchResult(
            name="SST user-key Bloom microbench",
            category="SSTable User-Key Bloom",
            planned_iters=SST_USER_KEY_BLOOM_ITERS,
            warmup_iters=0,
        )
        result.error = str(exc)
        result.metadata.update({
            "matrix": "sstable_user_key_bloom",
            "sstables": SST_USER_KEY_BLOOM_SSTABLES,
            "payload_bytes": SST_USER_KEY_BLOOM_PAYLOAD_BYTES,
            "iters": SST_USER_KEY_BLOOM_ITERS,
        })
        return [result]

    speedup = float(report.get("speedup_vs_fail_open") or 0.0)
    return [
        sstable_user_key_bloom_result(
            report,
            "optimized",
            "SST user-key Bloom optimized",
            speedup,
        ),
        sstable_user_key_bloom_result(
            report,
            "fail_open",
            "SST user-key Bloom fail-open control",
            speedup,
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 19 — SSTable No-Fill Block Cache Policy
# ═══════════════════════════════════════════════════════════════════════════════
def run_sstable_no_fill_cache_microbench() -> Dict[str, object]:
    cmd = ["cargo", "run"]
    if SST_NO_FILL_RELEASE:
        cmd.append("--release")
    cmd.extend(["-q", "--bin", "sstable-no-fill-cache-bench"])

    env = os.environ.copy()
    env["BENCH_SST_NO_FILL_SCAN_BLOCKS"] = str(SST_NO_FILL_SCAN_BLOCKS)
    env["BENCH_SST_NO_FILL_ITERS"] = str(SST_NO_FILL_ITERS)
    env["BENCH_SST_NO_FILL_PAYLOAD_BYTES"] = str(SST_NO_FILL_PAYLOAD_BYTES)
    env["BENCH_SST_NO_FILL_CACHE_BLOCKS"] = str(SST_NO_FILL_CACHE_BLOCKS)

    completed = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        text=True,
        capture_output=True,
        timeout=SST_NO_FILL_TIMEOUT_SEC,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {detail[:1000]}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"sstable no-fill cache microbench returned non-JSON output: {completed.stdout[:1000]}"
        ) from exc


def sstable_no_fill_cache_result(
    report: Dict[str, object],
    phase_key: str,
    display_name: str,
    speedup: float,
) -> BenchResult:
    phase = report.get(phase_key) or {}
    config = report.get("config") or {}
    metrics_delta = phase.get("metrics_delta") if isinstance(phase, dict) else {}
    if not isinstance(metrics_delta, dict):
        metrics_delta = {}

    result = BenchResult(
        name=display_name,
        category="SSTable No-Fill Cache",
        planned_iters=int(config.get("iters", SST_NO_FILL_ITERS)),
        warmup_iters=0,
    )
    result.times_ms = [float(value) for value in phase.get("times_ms", [])]
    result.row_count = int(phase.get("row_count", 0))
    result.metrics_delta = {
        key: int(value) for key, value in metrics_delta.items() if isinstance(value, (int, float))
    }
    scan_blocks = int(config.get("scan_blocks", SST_NO_FILL_SCAN_BLOCKS))
    iters = int(config.get("iters", SST_NO_FILL_ITERS))
    expected_scan_reads = scan_blocks * iters
    inserts = result.metrics_delta.get("block_cache_insert_count", 0)
    fill_skips = result.metrics_delta.get("block_cache_fill_skip_count", 0)
    misses = result.metrics_delta.get("block_cache_miss_count", 0)
    file_opens = result.metrics_delta.get("sstable_block_file_open_count", 0)
    hot_hits = int(phase.get("hot_after_scan_hits", 0))
    hot_misses = int(phase.get("hot_after_scan_misses", 0))
    result.metadata.update({
        "matrix": "sstable_no_fill_cache",
        "phase": phase_key,
        "scan_blocks": scan_blocks,
        "payload_bytes": int(config.get("payload_bytes", SST_NO_FILL_PAYLOAD_BYTES)),
        "cache_blocks": int(config.get("block_cache_capacity", SST_NO_FILL_CACHE_BLOCKS)),
        "iters": iters,
        "hot_after_scan_hits": hot_hits,
        "hot_after_scan_misses": hot_misses,
        "speedup_vs_fill_cache": round(speedup, 3),
        "measurement_guidance": "fill-cache should insert scan blocks and usually miss the hot reread with a tiny cache; no-fill should skip scan fills and hit the hot reread.",
    })
    annotate_block_cache_metrics(result)
    result.note = (
        f"scan blocks {scan_blocks:,} | iters {iters} | avg {result.avg:.3f}ms | "
        f"misses {misses:,} | inserts {inserts:,} | fill-skips {fill_skips:,} | "
        f"file-opens {file_opens:,} | hot hits/misses {hot_hits}/{hot_misses} | "
        f"speedup {speedup:.2f}x"
    )

    if result.row_count != expected_scan_reads:
        result.error = f"expected {expected_scan_reads} scanned rows, got {result.row_count}"
    elif phase_key == "fill_cache" and inserts < expected_scan_reads:
        result.error = f"expected fill-cache scan to insert at least {expected_scan_reads} blocks, got {inserts}"
    elif phase_key == "fill_cache" and file_opens > iters + hot_misses:
        result.error = f"expected iterator file reuse to keep fill-cache file opens <= {iters + hot_misses}, got {file_opens}"
    elif phase_key == "no_fill_cache" and fill_skips != expected_scan_reads:
        result.error = f"expected {expected_scan_reads} no-fill skips, got {fill_skips}"
    elif phase_key == "no_fill_cache" and inserts != 0:
        result.error = f"expected no-fill scan to insert zero scan blocks, got {inserts}"
    elif phase_key == "no_fill_cache" and hot_hits != iters:
        result.error = f"expected no-fill scan to preserve hot block for {iters} hits, got {hot_hits}"
    elif phase_key == "no_fill_cache" and file_opens > iters:
        result.error = f"expected iterator file reuse to keep no-fill file opens <= {iters}, got {file_opens}"

    return result


def part19_sstable_no_fill_cache() -> List[BenchResult]:
    try:
        report = run_sstable_no_fill_cache_microbench()
    except Exception as exc:
        result = BenchResult(
            name="SST no-fill cache microbench",
            category="SSTable No-Fill Cache",
            planned_iters=SST_NO_FILL_ITERS,
            warmup_iters=0,
        )
        result.error = str(exc)
        result.metadata.update({
            "matrix": "sstable_no_fill_cache",
            "scan_blocks": SST_NO_FILL_SCAN_BLOCKS,
            "payload_bytes": SST_NO_FILL_PAYLOAD_BYTES,
            "cache_blocks": SST_NO_FILL_CACHE_BLOCKS,
            "iters": SST_NO_FILL_ITERS,
        })
        return [result]

    speedup = float(report.get("speedup_vs_fill_cache") or 0.0)
    return [
        sstable_no_fill_cache_result(
            report,
            "fill_cache",
            "SST fill-cache scan",
            speedup,
        ),
        sstable_no_fill_cache_result(
            report,
            "no_fill_cache",
            "SST no-fill scan",
            speedup,
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 27 — SSTable Reverse Frontier Activation
# ═══════════════════════════════════════════════════════════════════════════════
def run_sstable_reverse_frontier_microbench() -> Dict[str, object]:
    cmd = ["cargo", "run", "--quiet"]
    if SST_REVERSE_FRONTIER_RELEASE:
        cmd.append("--release")
    cmd.extend(["--bin", "sstable-reverse-frontier-bench"])

    env = os.environ.copy()
    env["BENCH_SST_REVERSE_FRONTIER_DECOYS"] = str(SST_REVERSE_FRONTIER_DECOYS)
    env["BENCH_SST_REVERSE_FRONTIER_ITERS"] = str(SST_REVERSE_FRONTIER_ITERS)
    env["BENCH_SST_REVERSE_FRONTIER_PAYLOAD_BYTES"] = str(SST_REVERSE_FRONTIER_PAYLOAD_BYTES)
    env["BENCH_SST_REVERSE_FRONTIER_CACHE_BLOCKS"] = str(SST_REVERSE_FRONTIER_CACHE_BLOCKS)

    completed = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        text=True,
        capture_output=True,
        timeout=SST_REVERSE_FRONTIER_TIMEOUT_SEC,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {detail[:1000]}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"sstable reverse-frontier microbench returned non-JSON output: {completed.stdout[:1000]}"
        ) from exc


def sstable_reverse_frontier_result(
    report: Dict[str, object],
    phase_key: str,
    display_name: str,
) -> BenchResult:
    phase = report.get(phase_key) or {}
    config = report.get("config") or {}
    metrics_delta = phase.get("metrics_delta") if isinstance(phase, dict) else {}
    if not isinstance(metrics_delta, dict):
        metrics_delta = {}

    decoys = int(config.get("decoy_sstables", SST_REVERSE_FRONTIER_DECOYS))
    iters = int(config.get("iters", SST_REVERSE_FRONTIER_ITERS))
    payload_bytes = int(config.get("payload_bytes", SST_REVERSE_FRONTIER_PAYLOAD_BYTES))
    cache_blocks = int(config.get("block_cache_capacity", SST_REVERSE_FRONTIER_CACHE_BLOCKS))
    candidates = decoys + 1
    expected_rows = iters
    expected_probe_count = candidates * iters
    expected_reduction = decoys * iters
    activation_reduction = int(report.get("activation_reduction") or 0)
    activation_ratio = float(report.get("activation_reduction_ratio") or 0.0)
    same_results = bool(report.get("same_results"))

    result = BenchResult(
        name=display_name,
        category="SSTable Reverse Frontier",
        planned_iters=iters,
        warmup_iters=0,
    )
    result.times_ms = [float(value) for value in phase.get("times_ms", [])]
    result.row_count = int(phase.get("row_count", 0))
    result.result_checksum = str(phase.get("result_checksum") or "")
    result.metrics_delta = {
        key: int(value) for key, value in metrics_delta.items() if isinstance(value, (int, float))
    }
    result.metadata.update({
        "matrix": "sstable_reverse_frontier",
        "phase": phase_key,
        "decoy_sstables": decoys,
        "candidate_sstables": candidates,
        "payload_bytes": payload_bytes,
        "cache_blocks": cache_blocks,
        "iters": iters,
        "expected_rows": expected_rows,
        "same_results": same_results,
        "activation_reduction": activation_reduction,
        "activation_reduction_expected": expected_reduction,
        "activation_reduction_ratio": round(activation_ratio, 3),
        "measurement_guidance": (
            "Hand-built SSTables force every decoy file range to overlap the query upper bound. "
            "The optimized phase uses range-local block-property frontiers; the control phase uses "
            "file-level frontier sentinels."
        ),
    })

    probes = result.metrics_delta.get("fusion_reverse_sstable_frontier_probe_count", 0)
    in_range = result.metrics_delta.get("fusion_reverse_sstable_frontier_in_range_count", 0)
    file_frontiers = result.metrics_delta.get("fusion_reverse_sstable_frontier_file_count", 0)
    tightens = result.metrics_delta.get("fusion_reverse_sstable_frontier_tighten_count", 0)
    empty_skips = result.metrics_delta.get("fusion_reverse_sstable_frontier_empty_skip_count", 0)
    fail_opens = result.metrics_delta.get("fusion_reverse_sstable_frontier_fail_open_count", 0)
    pending = result.metrics_delta.get("fusion_reverse_sstable_pending_count", 0)
    activations = result.metrics_delta.get("fusion_reverse_sstable_activation_count", 0)
    deferred = result.metrics_delta.get("fusion_reverse_sstable_deferred_unopened_count", 0)
    reverse_iterator_opens = result.metrics_delta.get("sstable_reverse_iterator_open_count", 0)

    result.note = (
        f"decoys {decoys:,} | iters {iters} | avg {result.avg:.3f}ms | "
        f"activations {activations:,} | deferred {deferred:,} | "
        f"pending {pending:,} | iterator opens {reverse_iterator_opens:,} | "
        f"reduction {activation_reduction:,} ({activation_ratio:.2f}x control/optimized)"
    )

    if iters <= 0:
        result.error = f"expected positive iters, got {iters}"
    elif result.success_count != iters:
        result.error = f"expected {iters} timing samples, got {result.success_count}"
    elif result.row_count != expected_rows:
        result.error = f"expected {expected_rows} result rows, got {result.row_count}"
    elif not same_results:
        result.error = "optimized and file-level control returned different result checksums"
    elif activation_reduction != expected_reduction:
        result.error = f"expected activation reduction {expected_reduction}, got {activation_reduction}"
    elif probes != expected_probe_count:
        result.error = f"expected {expected_probe_count} frontier probes, got {probes}"
    elif pending != expected_probe_count:
        result.error = f"expected {expected_probe_count} pending SSTables, got {pending}"
    elif empty_skips != 0:
        result.error = f"expected zero empty frontier skips, got {empty_skips}"
    elif fail_opens != 0:
        result.error = f"expected zero frontier fail-opens, got {fail_opens}"
    elif phase_key == "optimized" and in_range != expected_probe_count:
        result.error = f"expected {expected_probe_count} in-range block frontiers, got {in_range}"
    elif phase_key == "optimized" and file_frontiers != 0:
        result.error = f"expected optimized phase to use zero file-level frontiers, got {file_frontiers}"
    elif phase_key == "optimized" and tightens != decoys * iters:
        result.error = f"expected {decoys * iters} optimized frontier tightens, got {tightens}"
    elif phase_key == "optimized" and activations != iters:
        result.error = f"expected optimized phase to activate {iters} SSTables, got {activations}"
    elif phase_key == "optimized" and deferred != decoys * iters:
        result.error = f"expected optimized phase to defer {decoys * iters} SSTables, got {deferred}"
    elif phase_key == "optimized" and reverse_iterator_opens != iters:
        result.error = f"expected optimized phase to open {iters} reverse iterators, got {reverse_iterator_opens}"
    elif phase_key == "file_level_control" and file_frontiers != expected_probe_count:
        result.error = f"expected {expected_probe_count} file-level frontiers, got {file_frontiers}"
    elif phase_key == "file_level_control" and in_range != 0:
        result.error = f"expected control phase to use zero in-range block frontiers, got {in_range}"
    elif phase_key == "file_level_control" and activations != expected_probe_count:
        result.error = f"expected control phase to activate {expected_probe_count} SSTables, got {activations}"
    elif phase_key == "file_level_control" and deferred != 0:
        result.error = f"expected control phase to defer zero SSTables, got {deferred}"
    elif phase_key == "file_level_control" and reverse_iterator_opens != expected_probe_count:
        result.error = (
            f"expected control phase to open {expected_probe_count} reverse iterators, "
            f"got {reverse_iterator_opens}"
        )

    return result


def part27_sstable_reverse_frontier() -> List[BenchResult]:
    try:
        report = run_sstable_reverse_frontier_microbench()
    except Exception as exc:
        result = BenchResult(
            name="SST reverse-frontier microbench",
            category="SSTable Reverse Frontier",
            planned_iters=SST_REVERSE_FRONTIER_ITERS,
            warmup_iters=0,
        )
        result.error = str(exc)
        result.metadata.update({
            "matrix": "sstable_reverse_frontier",
            "decoy_sstables": SST_REVERSE_FRONTIER_DECOYS,
            "payload_bytes": SST_REVERSE_FRONTIER_PAYLOAD_BYTES,
            "cache_blocks": SST_REVERSE_FRONTIER_CACHE_BLOCKS,
            "iters": SST_REVERSE_FRONTIER_ITERS,
        })
        return [result]

    return [
        sstable_reverse_frontier_result(
            report,
            "optimized",
            "SST reverse-frontier optimized",
        ),
        sstable_reverse_frontier_result(
            report,
            "file_level_control",
            "SST reverse-frontier file-level control",
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 28 — Fusion Reverse Frontier Public-API Activation
# ═══════════════════════════════════════════════════════════════════════════════
def run_fusion_reverse_frontier_bench() -> Dict[str, object]:
    cmd = ["cargo", "run", "--quiet"]
    if FUSION_REVERSE_FRONTIER_RELEASE:
        cmd.append("--release")
    cmd.extend(["--bin", "fusion-reverse-frontier-bench"])

    env = os.environ.copy()
    env["BENCH_FUSION_REVERSE_FRONTIER_DECOYS"] = str(FUSION_REVERSE_FRONTIER_DECOYS)
    env["BENCH_FUSION_REVERSE_FRONTIER_ITERS"] = str(FUSION_REVERSE_FRONTIER_ITERS)
    env["BENCH_FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES"] = str(
        FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES
    )
    env["BENCH_FUSION_REVERSE_FRONTIER_CACHE_BLOCKS"] = str(
        FUSION_REVERSE_FRONTIER_CACHE_BLOCKS
    )

    completed = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        text=True,
        capture_output=True,
        timeout=FUSION_REVERSE_FRONTIER_TIMEOUT_SEC,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"{' '.join(cmd)} failed: {detail[:1000]}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"fusion reverse-frontier benchmark returned non-JSON output: {completed.stdout[:1000]}"
        ) from exc


def fusion_reverse_frontier_result(
    report: Dict[str, object],
    phase_key: str,
    display_name: str,
) -> BenchResult:
    phase = report.get(phase_key) or {}
    config = report.get("config") or {}
    metrics_delta = phase.get("metrics_delta") if isinstance(phase, dict) else {}
    if not isinstance(metrics_delta, dict):
        metrics_delta = {}

    decoys = int(config.get("decoy_sstables", FUSION_REVERSE_FRONTIER_DECOYS))
    iters = int(config.get("iters", FUSION_REVERSE_FRONTIER_ITERS))
    payload_bytes = int(config.get("payload_bytes", FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES))
    cache_blocks = int(
        config.get("block_cache_capacity", FUSION_REVERSE_FRONTIER_CACHE_BLOCKS)
    )
    candidate_sstables = decoys + 1
    tombstone_sstables = 2

    if phase_key == "limit1_deferred":
        expected_rows = iters
        expected_frontiers = candidate_sstables * iters
        expected_tightens = decoys * iters
        expected_activations = iters
        expected_deferred = decoys * iters
        expected_equal_frontier = 0
        expected_visible_puts = iters
    elif phase_key == "full_drain":
        expected_rows = (decoys + 2) * iters
        expected_frontiers = candidate_sstables * iters
        expected_tightens = decoys * iters
        expected_activations = candidate_sstables * iters
        expected_deferred = 0
        expected_equal_frontier = 0
        expected_visible_puts = expected_rows
    elif phase_key == "equal_frontier_tombstone":
        expected_rows = 0
        expected_frontiers = tombstone_sstables * iters
        expected_tightens = 0
        expected_activations = tombstone_sstables * iters
        expected_deferred = 0
        expected_equal_frontier = iters
        expected_visible_puts = 0
    else:
        expected_rows = 0
        expected_frontiers = 0
        expected_tightens = 0
        expected_activations = 0
        expected_deferred = 0
        expected_equal_frontier = 0
        expected_visible_puts = 0

    result = BenchResult(
        name=display_name,
        category="Fusion Reverse Frontier",
        planned_iters=iters,
        warmup_iters=0,
    )
    result.times_ms = [float(value) for value in phase.get("times_ms", [])]
    result.row_count = int(phase.get("row_count", 0))
    result.result_checksum = str(phase.get("result_checksum") or "")
    result.metrics_delta = {
        key: int(value) for key, value in metrics_delta.items() if isinstance(value, (int, float))
    }
    result.metadata.update({
        "matrix": "fusion_reverse_frontier",
        "phase": phase_key,
        "decoy_sstables": decoys,
        "candidate_sstables": candidate_sstables,
        "payload_bytes": payload_bytes,
        "cache_blocks": cache_blocks,
        "iters": iters,
        "expected_rows": expected_rows,
        "expected_frontiers": expected_frontiers,
        "expected_activations": expected_activations,
        "expected_deferred_unopened": expected_deferred,
        "expected_equal_frontier_activations_min": expected_equal_frontier,
        "measurement_guidance": (
            "Uses only public FusionStorage/Storage/Transaction APIs to write, snapshot, "
            "and scan real SSTables through the production reverse merge heap. Latency is "
            "smoke evidence; the hard claim is exact counter and checksum behavior."
        ),
    })

    scans = result.metrics_delta.get("fusion_reverse_scan_count", 0)
    probes = result.metrics_delta.get("fusion_reverse_sstable_frontier_probe_count", 0)
    in_range = result.metrics_delta.get("fusion_reverse_sstable_frontier_in_range_count", 0)
    file_frontiers = result.metrics_delta.get("fusion_reverse_sstable_frontier_file_count", 0)
    tightens = result.metrics_delta.get("fusion_reverse_sstable_frontier_tighten_count", 0)
    empty_skips = result.metrics_delta.get("fusion_reverse_sstable_frontier_empty_skip_count", 0)
    fail_opens = result.metrics_delta.get("fusion_reverse_sstable_frontier_fail_open_count", 0)
    pending = result.metrics_delta.get("fusion_reverse_sstable_pending_count", 0)
    activations = result.metrics_delta.get("fusion_reverse_sstable_activation_count", 0)
    deferred = result.metrics_delta.get("fusion_reverse_sstable_deferred_unopened_count", 0)
    equal_frontier = result.metrics_delta.get(
        "fusion_reverse_sstable_activation_equal_frontier_count", 0
    )
    visible_puts = result.metrics_delta.get("fusion_reverse_visible_put_count", 0)
    reverse_iterator_opens = result.metrics_delta.get("sstable_reverse_iterator_open_count", 0)
    compactions = result.metrics_delta.get("compaction_run_count", 0)

    result.note = (
        f"phase {phase_key} | decoys {decoys} | iters {iters} | avg {result.avg:.3f}ms | "
        f"scans {scans} | activations {activations} | deferred {deferred} | "
        f"equal-frontier {equal_frontier} | compactions {compactions}"
    )

    if iters <= 0:
        result.error = f"expected positive iters, got {iters}"
    elif phase_key not in ("limit1_deferred", "full_drain", "equal_frontier_tombstone"):
        result.error = f"unknown fusion reverse frontier phase {phase_key}"
    elif result.success_count != iters:
        result.error = f"expected {iters} timing samples, got {result.success_count}"
    elif result.row_count != expected_rows:
        result.error = f"expected {expected_rows} rows, got {result.row_count}"
    elif compactions != 0:
        result.error = f"expected no background compaction during benchmark, got {compactions}"
    elif scans != iters:
        result.error = f"expected {iters} reverse scans, got {scans}"
    elif probes != expected_frontiers:
        result.error = f"expected {expected_frontiers} frontier probes, got {probes}"
    elif pending != expected_frontiers:
        result.error = f"expected {expected_frontiers} pending SSTables, got {pending}"
    elif in_range != expected_frontiers:
        result.error = f"expected {expected_frontiers} in-range frontiers, got {in_range}"
    elif file_frontiers != 0:
        result.error = f"expected zero file-level frontier fallbacks, got {file_frontiers}"
    elif tightens != expected_tightens:
        result.error = f"expected {expected_tightens} frontier tightens, got {tightens}"
    elif empty_skips != 0:
        result.error = f"expected zero empty frontier skips, got {empty_skips}"
    elif fail_opens != 0:
        result.error = f"expected zero frontier fail-opens, got {fail_opens}"
    elif activations != expected_activations:
        result.error = f"expected {expected_activations} SSTable activations, got {activations}"
    elif deferred != expected_deferred:
        result.error = f"expected {expected_deferred} deferred SSTables, got {deferred}"
    elif reverse_iterator_opens != expected_activations:
        result.error = (
            f"expected {expected_activations} reverse iterator opens, got {reverse_iterator_opens}"
        )
    elif equal_frontier < expected_equal_frontier:
        result.error = (
            f"expected at least {expected_equal_frontier} equal-frontier activations, "
            f"got {equal_frontier}"
        )
    elif phase_key != "equal_frontier_tombstone" and equal_frontier != 0:
        result.error = f"expected zero equal-frontier activations, got {equal_frontier}"
    elif visible_puts != expected_visible_puts:
        result.error = f"expected {expected_visible_puts} visible puts, got {visible_puts}"

    return result


def part28_fusion_reverse_frontier() -> List[BenchResult]:
    try:
        report = run_fusion_reverse_frontier_bench()
    except Exception as exc:
        result = BenchResult(
            name="Fusion reverse-frontier public API",
            category="Fusion Reverse Frontier",
            planned_iters=FUSION_REVERSE_FRONTIER_ITERS,
            warmup_iters=0,
        )
        result.error = str(exc)
        result.metadata.update({
            "matrix": "fusion_reverse_frontier",
            "decoy_sstables": FUSION_REVERSE_FRONTIER_DECOYS,
            "payload_bytes": FUSION_REVERSE_FRONTIER_PAYLOAD_BYTES,
            "cache_blocks": FUSION_REVERSE_FRONTIER_CACHE_BLOCKS,
            "iters": FUSION_REVERSE_FRONTIER_ITERS,
        })
        return [result]

    return [
        fusion_reverse_frontier_result(
            report,
            "limit1_deferred",
            "Fusion frontier LIMIT 1",
        ),
        fusion_reverse_frontier_result(
            report,
            "full_drain",
            "Fusion frontier full drain",
        ),
        fusion_reverse_frontier_result(
            report,
            "equal_frontier_tombstone",
            "Fusion frontier tombstone",
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 25 — SQL No-Fill Block Cache Policy
# ═══════════════════════════════════════════════════════════════════════════════
def sql_no_fill_cache_error(name: str, error: str, metadata: Dict[str, object]) -> BenchResult:
    result = BenchResult(name=name, category="SQL No-Fill Cache", planned_iters=1, warmup_iters=0)
    result.error = error
    result.metadata.update(metadata)
    return result


def apply_sql_no_fill_claim_gate(result: BenchResult) -> None:
    if not BENCH_CLAIM_MODE:
        return

    failures: List[str] = []
    warnings: List[str] = []
    metadata = result.metadata
    expected_rows = int(metadata.get("expected_rows", 0) or 0)
    query_count = _metric_count(result, "query_count")
    misses = _metric_count(result, "block_cache_miss_count")
    inserts = _metric_count(result, "block_cache_insert_count")
    insert_bytes = _metric_count(result, "block_cache_insert_bytes")
    fill_skips = _metric_count(result, "block_cache_fill_skip_count")
    read_bytes = _metric_count(result, "sstable_block_read_bytes")
    iterator_opens = _metric_count(result, "sstable_iterator_open_count")
    hits = _metric_count(result, "block_cache_hit_count")
    evictions = _metric_count(result, "block_cache_eviction_count")
    phase = str(metadata.get("phase") or "")
    required_metric_keys = (
        "query_count",
        "block_cache_hit_count",
        "block_cache_miss_count",
        "block_cache_insert_count",
        "block_cache_insert_bytes",
        "block_cache_fill_skip_count",
        "block_cache_eviction_count",
        "sstable_block_read_bytes",
        "sstable_iterator_open_count",
    )
    cache_events = sum(
        _metric_count(result, key)
        for key in (
            "query_result_cache_eligible_count",
            "query_result_cache_hit_count",
            "query_result_cache_miss_count",
            "query_result_cache_stale_count",
            "query_result_cache_insert_count",
            "query_result_cache_invalidation_count",
        )
    )

    metadata["claim_gate"] = "sql_no_fill_cache"
    metadata["claim_expected_rows_per_query"] = expected_rows
    metadata["claim_expected_query_count"] = result.success_count
    metadata["claim_phase"] = phase

    if not result.metrics_delta:
        failures.append("metrics_delta unavailable")
    else:
        missing_metric_keys = [key for key in required_metric_keys if key not in result.metrics_delta]
        if missing_metric_keys:
            failures.append(f"metrics_delta missing required keys: {', '.join(missing_metric_keys)}")
    if result.error:
        failures.append(f"query failed before claim checks: {result.error}")
    if result.success_count != result.planned_iters:
        failures.append(
            f"expected {result.planned_iters} successful measured iterations, got {result.success_count}"
        )
    if query_count != result.success_count:
        failures.append(f"expected query_count {result.success_count}, got {query_count}")
    if result.row_count != expected_rows:
        failures.append(f"expected {expected_rows} result rows per query, got {result.row_count}")
    if cache_events != 0:
        failures.append(f"expected zero query-result cache events, got {cache_events}")

    if phase == "no_fill_bulk":
        if misses <= 0:
            failures.append("expected SQL no-fill scan to miss at least one SSTable block")
        if fill_skips <= 0:
            failures.append("expected SQL no-fill scan to skip block cache fills")
        if fill_skips < misses:
            if fill_skips + inserts < misses:
                failures.append(
                    "expected no-fill skips plus bounded helper-read inserts to cover block misses, "
                    f"got skips={fill_skips}, inserts={inserts}, misses={misses}"
                )
        helper_insert_limit = max(16, query_count * 3)
        metadata["claim_helper_insert_limit"] = helper_insert_limit
        if inserts > helper_insert_limit:
            failures.append(
                f"expected only bounded non-bulk helper inserts during no-fill scan, "
                f"got inserts={inserts}, limit={helper_insert_limit}"
            )
        if fill_skips <= inserts:
            failures.append(
                f"expected no-fill scan skips to dominate helper inserts, got skips={fill_skips}, inserts={inserts}"
            )
        if insert_bytes >= read_bytes:
            failures.append(
                f"expected no-fill inserted bytes to stay below SSTable read bytes, "
                f"got inserted={insert_bytes}, read={read_bytes}"
            )
        if read_bytes <= 0:
            failures.append("expected SSTable block read bytes > 0")
        if iterator_opens < query_count:
            failures.append(
                f"expected at least one SSTable iterator open per query, got opens={iterator_opens}, "
                f"queries={query_count}"
            )
    elif phase == "no_fill_hot_after":
        if hits <= 0:
            failures.append("expected hot reread after no-fill bulk scan to hit block cache")
        if fill_skips != 0:
            failures.append(f"expected zero no-fill skips for hot point reread, got {fill_skips}")
        if misses or inserts or read_bytes:
            warnings.append(
                "hot reread observed auxiliary block misses/inserts; SQL point probes touch multiple blocks, "
                f"hits={hits}, misses={misses}, inserts={inserts}, read_bytes={read_bytes}"
            )
    elif phase == "fill_cache_bulk":
        if misses <= 0:
            failures.append("expected fill-cache control scan to miss SSTable blocks")
        if inserts <= 0 or insert_bytes <= 0:
            failures.append(
                f"expected fill-cache control scan to insert blocks, got inserts={inserts}, bytes={insert_bytes}"
            )
        if fill_skips != 0:
            failures.append(f"expected zero fill-skip events with sql_bulk_scan_no_fill=false, got {fill_skips}")
        if evictions <= 0:
            failures.append("expected fill-cache control scan to evict cache entries under tiny cache")
        if read_bytes <= 0:
            failures.append("expected fill-cache control scan to read SSTable bytes")
    elif phase == "fill_cache_hot_after":
        if hits <= 0:
            failures.append("expected fill-cache hot reread to complete through block-cache-visible probes")
        if fill_skips != 0:
            failures.append(f"expected zero fill-skip events for fill-cache hot reread, got {fill_skips}")
        if misses == 0:
            warnings.append(
                "hot reread remained cached after fill-cache bulk scan; cache-pollution proof should use "
                "bulk insert/eviction counters rather than this diagnostic point probe"
            )
    else:
        failures.append(f"unknown SQL no-fill claim phase: {phase or '<missing>'}")

    if result.success_count < 20:
        warnings.append(
            f"sample count {result.success_count} is below 20; latency percentiles are smoke-only"
        )

    _set_claim_result(result, failures, warnings)


def part25_sql_no_fill_cache() -> List[BenchResult]:
    cat = "SQL No-Fill Cache"
    repo_root = os.path.dirname(os.path.abspath(__file__))
    binary = SQL_NO_FILL_BINARY
    if not os.path.isabs(binary):
        binary = os.path.join(repo_root, binary)

    rows_count = sql_no_fill_rows()
    bulk_query = "SELECT id FROM bench_sql_no_fill_bulk"
    hot_prewarm_query = "SELECT payload AS warm_payload FROM bench_sql_no_fill_hot WHERE id = 1"
    hot_measure_query = "SELECT payload AS measured_payload FROM bench_sql_no_fill_hot WHERE id = 1"
    metadata: Dict[str, object] = {
        "matrix": "sql_no_fill_cache",
        "binary": binary,
        "http_port": SQL_NO_FILL_PORT,
        "timeout_sec": SQL_NO_FILL_TIMEOUT_SEC,
        "rows": rows_count,
        "payload_bytes": SQL_NO_FILL_PAYLOAD_BYTES,
        "cache_blocks": SQL_NO_FILL_CACHE_BLOCKS,
        "row_cache_capacity": 0,
        "statement_cache_capacity": 1,
        "owned_server": True,
        "control_phase": True,
        "measurement_guidance": (
            "This benchmark owns two fresh FusionDB processes with a one-block cache. The no-fill "
            "phase must scan without cache inserts and preserve a prewarmed hot block. The control "
            "phase disables SQL bulk no-fill, then the same bulk scan must admit blocks and evict "
            "the hot block before reread."
        ),
    }

    if not os.path.exists(binary):
        return [
            sql_no_fill_cache_error(
                "SQL no-fill setup",
                f"fusiondb binary not found: {binary}",
                metadata,
            )
        ]

    cleanup_root = None
    if SQL_NO_FILL_WORKDIR:
        scenario_root = os.path.abspath(SQL_NO_FILL_WORKDIR)
        if os.path.exists(scenario_root):
            if not os.path.isdir(scenario_root):
                return [
                    sql_no_fill_cache_error(
                        "SQL no-fill setup",
                        f"BENCH_SQL_NO_FILL_WORKDIR exists and is not a directory: {scenario_root}",
                        metadata,
                    )
                ]
            if os.listdir(scenario_root):
                if not SQL_NO_FILL_RESET_WORKDIR:
                    return [
                        sql_no_fill_cache_error(
                            "SQL no-fill setup",
                            "BENCH_SQL_NO_FILL_WORKDIR is non-empty; set "
                            "BENCH_SQL_NO_FILL_RESET_WORKDIR=1 to allow deletion: "
                            f"{scenario_root}",
                            metadata,
                        )
                    ]
                shutil.rmtree(scenario_root)
        os.makedirs(scenario_root, exist_ok=True)
    else:
        scenario_root = tempfile.mkdtemp(prefix="fusiondb_sql_no_fill_")
        cleanup_root = scenario_root

    results: List[BenchResult] = []
    phases = (
        ("no_fill", True, SQL_NO_FILL_PORT),
        ("fill_cache_control", False, SQL_NO_FILL_PORT + 10),
    )

    try:
        for phase_order, (phase_key, sql_bulk_scan_no_fill, http_port) in enumerate(phases, start=1):
            phase_root = os.path.join(scenario_root, phase_key)
            data_dir = os.path.join(phase_root, "data")
            log_path = os.path.join(phase_root, "fusiondb.log")
            os.makedirs(phase_root, exist_ok=True)
            write_startup_config(
                phase_root,
                data_dir,
                http_port,
                row_cache_capacity=0,
                statement_cache_capacity=1,
                block_cache_capacity=SQL_NO_FILL_CACHE_BLOCKS,
                sql_bulk_scan_no_fill=sql_bulk_scan_no_fill,
            )
            query_url = f"http://127.0.0.1:{http_port}/query"
            metrics_url = f"http://127.0.0.1:{http_port}/metrics"
            checkpoint_url = f"http://127.0.0.1:{http_port}/checkpoint"
            previous_urls = switch_benchmark_urls(query_url, metrics_url, checkpoint_url)
            proc: Optional[subprocess.Popen] = None

            try:
                proc, ready_metrics, ready_ms, start_error = start_restart_phase_server(
                    binary, phase_root, log_path, metrics_url, SQL_NO_FILL_TIMEOUT_SEC
                )
                phase_metadata: Dict[str, object] = {
                    **metadata,
                    "phase_key": phase_key,
                    "phase_order": phase_order,
                    "sql_bulk_scan_no_fill": sql_bulk_scan_no_fill,
                    "scenario_workdir": scenario_root,
                    "scenario_phase_workdir": phase_root,
                    "scenario_data_dir": data_dir,
                    "query_url": query_url,
                    "metrics_url": metrics_url,
                    "checkpoint_url": checkpoint_url,
                    "ready_ms": round(ready_ms, 3),
                    "rss_ready_kb": rss_kb(proc.pid) if proc else None,
                    "initial_metrics": metric_subset(ready_metrics),
                    "log_path": log_path if SQL_NO_FILL_KEEP_WORKDIR or SQL_NO_FILL_WORKDIR else None,
                }
                if start_error:
                    results.append(
                        sql_no_fill_cache_error(
                            f"SQL no-fill setup [{phase_key}]",
                            start_error,
                            phase_metadata,
                        )
                    )
                    return results

                load_timings: Dict[str, float] = {}
                setup_sql_no_fill_cache_table(load_timings, 0)
                phase_metadata["load_timings"] = load_timings
                phase_metadata["data_dir_bytes_after_load"] = dir_size_bytes(data_dir)
                phase_metadata["sstable_files_after_load"] = len(list_sstable_files(data_dir))

                prewarm_before = metrics_snapshot()
                prewarm_res, prewarm_ms = sql(hot_prewarm_query)
                prewarm_delta = metric_delta(prewarm_before, metrics_snapshot())
                prewarm_rows = rows(prewarm_res)
                phase_metadata.update({
                    "hot_prewarm_ms": round(prewarm_ms, 3),
                    "hot_prewarm_rows": prewarm_rows,
                    "hot_prewarm_metrics_delta": prewarm_delta,
                })
                if not prewarm_res or prewarm_res.get("status") != "ok" or prewarm_rows != 1:
                    results.append(
                        sql_no_fill_cache_error(
                            f"SQL no-fill setup [{phase_key}]",
                            f"hot prewarm failed or returned {prewarm_rows} rows",
                            phase_metadata,
                        )
                    )
                    return results

                if sql_bulk_scan_no_fill:
                    bulk_name = "SQL no-fill bulk scan"
                    hot_name = "SQL no-fill hot reread"
                    bulk_phase = "no_fill_bulk"
                    hot_phase = "no_fill_hot_after"
                else:
                    bulk_name = "SQL fill-cache control bulk scan"
                    hot_name = "SQL fill-cache control hot reread"
                    bulk_phase = "fill_cache_bulk"
                    hot_phase = "fill_cache_hot_after"

                bulk = bench(bulk_name, bulk_query, warmup=0, cat=cat)
                bulk.metadata.update({
                    **phase_metadata,
                    "phase": bulk_phase,
                    "path": "unbounded_full_scan",
                    "expected_rows": rows_count,
                    "cache_phase": "measured_bulk_scan_after_hot_prewarm",
                })
                annotate_block_cache_metrics(bulk)
                apply_sql_no_fill_claim_gate(bulk)
                results.append(bulk)

                hot = bench(hot_name, hot_measure_query, iters=1, warmup=0, cat=cat)
                hot.metadata.update({
                    **phase_metadata,
                    "phase": hot_phase,
                    "path": "hot_point_reread_after_bulk_scan",
                    "expected_rows": 1,
                    "cache_phase": "measured_hot_reread_after_bulk_scan",
                })
                annotate_block_cache_metrics(hot)
                apply_sql_no_fill_claim_gate(hot)
                results.append(hot)
            finally:
                if proc is not None:
                    stop_fusiondb_process(proc)
                restore_benchmark_urls(previous_urls)

        return results
    finally:
        if cleanup_root and not SQL_NO_FILL_KEEP_WORKDIR:
            shutil.rmtree(cleanup_root, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 22 — SSTable Startup Index Cache
# ═══════════════════════════════════════════════════════════════════════════════
def startup_result_from_error(name: str, error: str, metadata: Dict[str, object]) -> BenchResult:
    result = BenchResult(name=name, category="SSTable Startup Index", planned_iters=1, warmup_iters=0)
    result.error = error
    result.metadata.update(metadata)
    return result


def tail_file(path: str, max_bytes: int = 4000) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes), os.SEEK_SET)
            return f.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def run_startup_index_scenario(scenario: str, scenario_idx: int) -> BenchResult:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    binary = SST_STARTUP_BINARY
    if not os.path.isabs(binary):
        binary = os.path.join(repo_root, binary)
    source_data = os.path.abspath(SST_STARTUP_DATA_DIR)

    metadata: Dict[str, object] = {
        "matrix": "sstable_startup_index",
        "scenario": scenario,
        "source_data_dir": source_data,
        "copy_data": SST_STARTUP_COPY_DATA,
        "binary": binary,
        "http_port": SST_STARTUP_PORT,
        "timeout_sec": SST_STARTUP_TIMEOUT_SEC,
        "first_point_sql": SST_STARTUP_FIRST_POINT_SQL,
        "first_range_sql": SST_STARTUP_FIRST_RANGE_SQL,
        "measurement_guidance": (
            "Use ready_ms, rss_ready_kb, first-query latency, live_sstable_count, "
            "manifest load/legacy scan counters, and sidecar hit/miss/stale/invalid/write "
            "counters as startup/index gates. For v2 manifest scenarios, also inspect "
            "manifest_v2_record_count_written, manifest_v2_bytes_written, "
            "current_file_after_startup, and manifest_files_after_startup."
        ),
    }

    if not os.path.exists(binary):
        return startup_result_from_error(
            f"SST startup {scenario}",
            f"fusiondb binary not found: {binary}",
            metadata,
        )
    if not os.path.exists(source_data):
        return startup_result_from_error(
            f"SST startup {scenario}",
            f"startup source data dir not found: {source_data}",
            metadata,
        )
    if not SST_STARTUP_COPY_DATA and scenario != "warm_sidecar":
        return startup_result_from_error(
            f"SST startup {scenario}",
            "destructive startup scenarios require BENCH_SST_STARTUP_COPY_DATA=1",
            metadata,
        )

    cleanup_root = None
    if SST_STARTUP_WORKDIR:
        scenario_root = os.path.abspath(os.path.join(SST_STARTUP_WORKDIR, scenario))
        if os.path.exists(scenario_root):
            shutil.rmtree(scenario_root)
        os.makedirs(scenario_root, exist_ok=True)
    else:
        scenario_root = tempfile.mkdtemp(prefix=f"fusiondb_startup_{scenario_idx}_{scenario}_")
        cleanup_root = scenario_root
    data_dir = os.path.join(scenario_root, "data")
    log_path = os.path.join(scenario_root, "fusiondb.log")

    proc: Optional[subprocess.Popen] = None
    try:
        if SST_STARTUP_COPY_DATA:
            copy_startup_data(source_data, data_dir)
        else:
            data_dir = source_data
        metadata["scenario_workdir"] = scenario_root
        metadata["scenario_data_dir"] = data_dir
        metadata["data_dir_bytes"] = dir_size_bytes(data_dir)
        metadata.update(prepare_startup_sidecars(data_dir, scenario))
        metadata.update(prepare_startup_manifest(data_dir, scenario))
        metadata.update(prepare_startup_dirty_wal(data_dir, scenario))
        metadata["sidecar_files_after_prepare"] = len(list_index_sidecars(data_dir))
        metadata["data_dir_bytes_after_prepare"] = dir_size_bytes(data_dir)
        write_startup_config(scenario_root, data_dir, SST_STARTUP_PORT)

        query_url = f"http://127.0.0.1:{SST_STARTUP_PORT}/query"
        metrics_url = f"http://127.0.0.1:{SST_STARTUP_PORT}/metrics"
        start = time.perf_counter()
        with open(log_path, "w", encoding="utf-8") as log:
            proc = subprocess.Popen(
                [binary],
                cwd=scenario_root,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )

            metrics = None
            ready_ms = 0.0
            deadline = start + SST_STARTUP_TIMEOUT_SEC
            while time.perf_counter() < deadline:
                if proc.poll() is not None:
                    return startup_result_from_error(
                        f"SST startup {scenario}",
                        f"fusiondb exited before readiness with code {proc.returncode}: {tail_file(log_path)}",
                        metadata,
                    )
                metrics = local_metrics_snapshot(metrics_url)
                if metrics is not None:
                    ready_ms = (time.perf_counter() - start) * 1000
                    break
                time.sleep(0.05)

            if metrics is None:
                return startup_result_from_error(
                    f"SST startup {scenario}",
                    f"fusiondb did not become ready within {SST_STARTUP_TIMEOUT_SEC}s: {tail_file(log_path)}",
                    metadata,
                )

            rss_ready = rss_kb(proc.pid)
            point_res, point_ms = local_query(query_url, SST_STARTUP_FIRST_POINT_SQL)
            range_res, range_ms = local_query(query_url, SST_STARTUP_FIRST_RANGE_SQL)
            rss_after_queries = rss_kb(proc.pid)
            current_after = ""
            current_path = os.path.join(data_dir, "sstables", "CURRENT")
            try:
                with open(current_path, "r", encoding="utf-8") as f:
                    current_after = f.read().strip()
            except OSError:
                pass
            manifest_files_after = sorted(
                name
                for name in os.listdir(os.path.join(data_dir, "sstables"))
                if name.startswith("MANIFEST-")
            )

        result = BenchResult(
            name=f"SST startup {scenario}",
            category="SSTable Startup Index",
            planned_iters=1,
            warmup_iters=0,
        )
        result.times_ms = [ready_ms]
        result.row_count = int(metrics.get("live_sstable_count", 0))
        result.metrics_delta = metric_subset(metrics)
        result.metadata.update(metadata)
        result.metadata.update({
            "ready_ms": round(ready_ms, 3),
            "rss_ready_kb": rss_ready,
            "rss_after_first_queries_kb": rss_after_queries,
            "first_point_ms": round(point_ms, 3),
            "first_point_rows": rows(point_res),
            "first_point_error": point_res.get("error") if point_res.get("status") == "error" else None,
            "first_range_ms": round(range_ms, 3),
            "first_range_rows": rows(range_res),
            "first_range_error": range_res.get("error") if range_res.get("status") == "error" else None,
            "current_file_after_startup": current_after,
            "manifest_files_after_startup": manifest_files_after,
            "log_path": log_path if SST_STARTUP_KEEP_WORKDIR or SST_STARTUP_WORKDIR else None,
        })
        hits = result.metrics_delta.get("sstable_index_cache_hit_count", 0)
        misses = result.metrics_delta.get("sstable_index_cache_miss_count", 0)
        stale = result.metrics_delta.get("sstable_index_cache_stale_count", 0)
        invalid = result.metrics_delta.get("sstable_index_cache_invalid_count", 0)
        writes = result.metrics_delta.get("sstable_index_cache_write_count", 0)
        live = result.metrics_delta.get("live_sstable_count", 0)
        manifest_loads = result.metrics_delta.get("sstable_manifest_load_count", 0)
        manifest_live = result.metrics_delta.get("sstable_manifest_live_file_count", 0)
        manifest_load_errors = result.metrics_delta.get("sstable_manifest_load_error_count", 0)
        legacy_scans = result.metrics_delta.get("sstable_manifest_legacy_scan_count", 0)
        legacy_candidates = result.metrics_delta.get(
            "sstable_manifest_legacy_scan_candidate_count", 0
        )
        manifest_open_errors = result.metrics_delta.get("sstable_manifest_open_error_count", 0)
        sstable_opens = result.metrics_delta.get("sstable_open_count", 0)
        compaction_runs = result.metrics_delta.get("compaction_run_count", 0)
        wal_replay_entries = result.metrics_delta.get("wal_replay_entry_count", 0)
        wal_replay_bytes = result.metrics_delta.get("wal_replay_bytes", 0)
        wal_replay_valid_bytes = result.metrics_delta.get("wal_replay_valid_bytes", 0)
        wal_replay_last_segment_id = result.metrics_delta.get("wal_replay_last_segment_id", 0)
        wal_replay_last_valid_offset = result.metrics_delta.get("wal_replay_last_valid_offset", 0)
        wal_replay_apply = result.metrics_delta.get("wal_replay_apply_count", 0)
        wal_replay_max_ts = result.metrics_delta.get("wal_replay_max_ts", 0)
        result.note = (
            f"ready {ready_ms:.1f}ms | RSS {rss_ready or 0:,} KiB | live SSTables {live} | "
            f"manifest {result.metadata.get('manifest_format', 'none')} load/live/errors "
            f"legacy/candidates {manifest_loads}/{manifest_live}/{manifest_load_errors} "
            f"{legacy_scans}/{legacy_candidates} | "
            f"WAL replay entries/bytes/valid/cursor/apply/max_ts {wal_replay_entries}/"
            f"{wal_replay_bytes}/{wal_replay_valid_bytes}/"
            f"{wal_replay_last_segment_id}:{wal_replay_last_valid_offset}/"
            f"{wal_replay_apply}/{wal_replay_max_ts} | "
            f"hit/miss/stale/invalid/write {hits}/{misses}/{stale}/{invalid}/{writes} | "
            f"first point {point_ms:.1f}ms | first range {range_ms:.1f}ms"
        )

        sidecar_inputs = int(result.metadata.get("sidecar_files_before", 0) or 0)
        manifest_scenario = bool(result.metadata.get("manifest_scenario"))
        v2_manifest_scenario = bool(result.metadata.get("manifest_v2_scenario"))
        if live > 0 and scenario == "warm_sidecar" and sidecar_inputs > 0 and hits <= 0:
            result.error = "expected warm sidecar scenario to hit at least one index cache"
        elif live > 0 and scenario == "no_sidecar" and misses <= 0:
            result.error = "expected no-sidecar scenario to record index cache misses"
        elif live > 0 and scenario == "stale_sidecar" and sidecar_inputs > 0 and stale <= 0:
            result.error = "expected stale-sidecar scenario to record stale index caches"
        elif live > 0 and scenario == "corrupt_sidecar" and sidecar_inputs > 0 and invalid <= 0:
            result.error = "expected corrupt-sidecar scenario to record invalid index caches"
        elif manifest_scenario and manifest_loads != 1:
            result.error = "expected manifest startup scenario to load CURRENT/MANIFEST"
        elif manifest_scenario and manifest_load_errors != 0:
            result.error = "manifest startup scenario recorded manifest load errors"
        elif manifest_scenario and manifest_open_errors != 0:
            result.error = "manifest startup scenario recorded SSTable open errors"
        elif manifest_scenario and legacy_scans != 0:
            result.error = "manifest startup scenario unexpectedly used legacy directory scan"
        elif manifest_scenario and manifest_live != live:
            result.error = (
                f"manifest live file count {manifest_live} should match live_sstable_count {live}"
            )
        elif manifest_scenario and sstable_opens != manifest_live:
            result.error = (
                f"manifest startup opened {sstable_opens} SSTables; expected {manifest_live}"
            )
        elif manifest_scenario and compaction_runs != 0:
            result.error = "manifest startup scenario unexpectedly ran compaction"
        elif v2_manifest_scenario and result.metadata.get("manifest_format") != "v2":
            result.error = "v2 manifest scenario did not prepare a v2 MANIFEST fixture"
        elif v2_manifest_scenario and int(result.metadata.get("manifest_live_files_written", 0) or 0) <= 0:
            result.error = "v2 manifest scenario did not write any live SSTables"
        elif scenario == "v2_many_edits" and int(result.metadata.get("manifest_v2_record_count_written", 0) or 0) <= 1:
            result.error = "v2_many_edits did not write append-only VersionEdit records"
        elif scenario == "v2_torn_tail_rollover" and result.metadata.get("current_file_after_startup") == "MANIFEST-000001":
            result.error = "v2_torn_tail_rollover did not install a rolled-over CURRENT"
        elif scenario == "v2_torn_tail_rollover" and "MANIFEST-000002" not in result.metadata.get("manifest_files_after_startup", []):
            result.error = "v2_torn_tail_rollover did not create MANIFEST-000002"
        elif (
            scenario in ("orphan_manifest", "v2_orphan_manifest")
            and int(result.metadata.get("orphan_sstable_files_created", 0) or 0) <= 0
        ):
            result.error = "orphan_manifest scenario failed to create orphan SSTables"
        elif (
            scenario in ("orphan_manifest", "v2_orphan_manifest")
            and int(result.metadata.get("sstable_files_after_manifest_prepare", 0) or 0)
            <= int(result.metadata.get("manifest_live_files_written", 0) or 0)
        ):
            result.error = "orphan_manifest scenario did not leave extra numeric SSTables outside manifest"
        elif (
            scenario == "dirty_wal_manifest"
            and wal_replay_entries < int(result.metadata.get("dirty_wal_entries_written", 0) or 0)
        ):
            result.error = "dirty_wal_manifest did not replay the expected WAL entries"
        elif scenario == "dirty_wal_manifest" and wal_replay_apply <= 0:
            result.error = "dirty_wal_manifest did not record WAL replay apply"
        elif (
            scenario == "dirty_wal_manifest"
            and wal_replay_valid_bytes < int(result.metadata.get("dirty_wal_bytes_written", 0) or 0)
        ):
            result.error = "dirty_wal_manifest did not record complete WAL replay valid bytes"
        return result
    finally:
        if proc is not None:
            stop_fusiondb_process(proc)
        if cleanup_root and not SST_STARTUP_KEEP_WORKDIR:
            shutil.rmtree(cleanup_root, ignore_errors=True)


def part22_sstable_startup_index() -> List[BenchResult]:
    scenarios = SST_STARTUP_SCENARIOS or ("warm_sidecar",)
    return [run_startup_index_scenario(scenario, idx) for idx, scenario in enumerate(scenarios)]


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 20 — Indexed ORDER BY Top-K
# ═══════════════════════════════════════════════════════════════════════════════
TOPK_ORDERED_ASC_PATHS = {
    "existing_range_index_path",
    "pure_secondary_index_order",
    "secondary_index_order_heap_fetch",
    "secondary_index_order_covering",
    "secondary_index_order_boolean",
    "secondary_index_order_date32",
    "secondary_index_order_timestamptz",
    "secondary_index_order_interval_prefix",
    "secondary_index_order_timestamptz_covering",
    "composite_ordered_index_asc",
    "composite_ordered_index_covering_asc",
    "composite_block_index_prefix_prune_asc",
}
TOPK_ORDERED_DESC_PATHS = {
    "secondary_index_order_desc",
    "secondary_index_order_desc_covering",
    "composite_ordered_index_desc",
    "composite_ordered_index_range_desc",
    "composite_ordered_index_window_desc",
    "composite_prefix_prune_desc",
    "composite_prefix_prune_range_desc",
    "composite_prefix_prune_absent_desc",
    "composite_frontier_deferred_desc",
    "composite_frontier_upper_window_desc",
}
TOPK_FALLBACK_PATHS = {
    "full_scan_baseline",
    "expression_order_fallback",
    "composite_full_scan_baseline",
    "composite_residual_unbounded_order",
    "composite_no_leading_equality_fallback",
    "composite_mixed_order_fallback",
}
TOPK_SORT_FALLBACK_PATHS = {
    "expression_order_fallback",
    "composite_mixed_order_fallback",
}
TOPK_COMPOSITE_ORDERED_PATHS = {
    "composite_ordered_index_asc",
    "composite_ordered_index_covering_asc",
    "composite_block_index_prefix_prune_asc",
    "composite_ordered_index_desc",
    "composite_ordered_index_range_desc",
    "composite_ordered_index_window_desc",
    "composite_prefix_prune_desc",
    "composite_prefix_prune_range_desc",
    "composite_prefix_prune_absent_desc",
    "composite_frontier_deferred_desc",
    "composite_frontier_upper_window_desc",
}
TOPK_INDEX_ONLY_COVERING_PATHS = {
    "secondary_index_order_covering",
    "secondary_index_order_desc_covering",
    "secondary_index_order_timestamptz_covering",
    "composite_ordered_index_covering_asc",
}
TOPK_BASE_ROW_FETCH_PATHS = {
    "secondary_index_order_heap_fetch",
}
TOPK_COMPOSITE_FALLBACK_PATHS = {
    "composite_full_scan_baseline",
    "composite_residual_unbounded_order",
    "composite_no_leading_equality_fallback",
    "composite_mixed_order_fallback",
}


def _metric_count(result: BenchResult, key: str) -> int:
    value = result.metrics_delta.get(key, 0)
    return int(value) if isinstance(value, (int, float)) else 0


def _set_claim_result(result: BenchResult, failures: List[str], warnings: List[str]) -> None:
    result.metadata["claim_mode"] = True
    if warnings:
        result.metadata["claim_warnings"] = warnings
    if not failures:
        result.metadata["claim_status"] = "passed"
        return
    result.metadata["claim_status"] = "failed"
    result.metadata["claim_failures"] = failures
    shown = "; ".join(failures[:3])
    if len(failures) > 3:
        shown += f"; +{len(failures) - 3} more"
    result.error = f"claim gate failed: {shown}"


def apply_part20_claim_gate(result: BenchResult) -> None:
    if not BENCH_CLAIM_MODE:
        return

    failures: List[str] = []
    warnings: List[str] = []
    metadata = result.metadata
    path = str(metadata.get("path", ""))
    limit = int(metadata.get("limit", 0) or 0)
    expected_rows = int(metadata.get("expected_rows", 0) or 0)
    query_count = _metric_count(result, "query_count")
    query_result_cache_eligible = _metric_count(result, "query_result_cache_eligible_count")
    query_result_cache_hits = _metric_count(result, "query_result_cache_hit_count")
    query_result_cache_misses = _metric_count(result, "query_result_cache_miss_count")
    query_result_cache_stale = _metric_count(result, "query_result_cache_stale_count")
    query_result_cache_inserts = _metric_count(result, "query_result_cache_insert_count")
    query_result_cache_invalidations = _metric_count(
        result, "query_result_cache_invalidation_count"
    )
    ordered_scans = _metric_count(result, "index_ordered_topk_scan_count")
    ordered_visits = _metric_count(result, "index_ordered_topk_entry_visit_count")
    reverse_scans = _metric_count(result, "index_ordered_topk_reverse_scan_count")
    ordered_index_only_rows = _metric_count(
        result, "index_ordered_topk_index_only_row_count"
    )
    ordered_base_row_fetches = _metric_count(
        result, "index_ordered_topk_base_row_fetch_count"
    )
    sort_fallbacks = _metric_count(result, "query_sort_fallback_count")
    row_reads = _metric_count(result, "row_read_count")
    fusion_reverse_scans = _metric_count(result, "fusion_reverse_scan_count")
    fusion_reverse_frontier_probes = _metric_count(
        result, "fusion_reverse_sstable_frontier_probe_count"
    )
    fusion_reverse_frontier_in_range = _metric_count(
        result, "fusion_reverse_sstable_frontier_in_range_count"
    )
    fusion_reverse_frontier_file = _metric_count(
        result, "fusion_reverse_sstable_frontier_file_count"
    )
    fusion_reverse_frontier_tightens = _metric_count(
        result, "fusion_reverse_sstable_frontier_tighten_count"
    )
    fusion_reverse_frontier_empty_skips = _metric_count(
        result, "fusion_reverse_sstable_frontier_empty_skip_count"
    )
    fusion_reverse_frontier_fail_opens = _metric_count(
        result, "fusion_reverse_sstable_frontier_fail_open_count"
    )
    fusion_reverse_sstable_pending = _metric_count(
        result, "fusion_reverse_sstable_pending_count"
    )
    fusion_reverse_sstable_activations = _metric_count(
        result, "fusion_reverse_sstable_activation_count"
    )
    fusion_reverse_sstable_deferred_unopened = _metric_count(
        result, "fusion_reverse_sstable_deferred_unopened_count"
    )
    fusion_reverse_sstable_equal_frontier_activations = _metric_count(
        result, "fusion_reverse_sstable_activation_equal_frontier_count"
    )
    fusion_reverse_raw_reads = _metric_count(result, "fusion_reverse_raw_entry_read_count")
    fusion_reverse_candidates = _metric_count(result, "fusion_reverse_visible_candidate_count")
    fusion_reverse_puts = _metric_count(result, "fusion_reverse_visible_put_count")
    sstable_reverse_iterators = _metric_count(result, "sstable_reverse_iterator_open_count")
    sstable_reverse_block_reads = _metric_count(result, "sstable_reverse_block_read_count")
    sstable_reverse_decodes = _metric_count(result, "sstable_reverse_block_entry_decode_count")
    sstable_reverse_yields = _metric_count(result, "sstable_reverse_block_entry_yield_count")
    sstable_reverse_span_scans = _metric_count(result, "sstable_reverse_block_span_scan_count")
    sstable_reverse_span_scan_entries = _metric_count(
        result, "sstable_reverse_block_span_scan_entry_count"
    )
    sstable_reverse_span_materializes = _metric_count(
        result, "sstable_reverse_block_span_materialize_entry_count"
    )
    reverse_seek_sidecar_hits = _metric_count(result, "sstable_reverse_seek_sidecar_hit_count")
    reverse_seek_sidecar_misses = _metric_count(result, "sstable_reverse_seek_sidecar_miss_count")
    reverse_seek_sidecar_stale = _metric_count(result, "sstable_reverse_seek_sidecar_stale_count")
    reverse_seek_sidecar_invalid = _metric_count(result, "sstable_reverse_seek_sidecar_invalid_count")
    reverse_seek_sidecar_writes = _metric_count(result, "sstable_reverse_seek_sidecar_write_count")
    reverse_seek_sidecar_write_errors = _metric_count(
        result, "sstable_reverse_seek_sidecar_write_error_count"
    )
    reverse_seek_sidecar_uses = _metric_count(result, "sstable_reverse_seek_sidecar_use_count")
    reverse_seek_sidecar_fail_opens = _metric_count(
        result, "sstable_reverse_seek_sidecar_fail_open_count"
    )
    reverse_seek_sidecar_index_entries = _metric_count(
        result, "sstable_reverse_seek_sidecar_index_entry_count"
    )
    reverse_seek_sidecar_materializes = _metric_count(
        result, "sstable_reverse_seek_sidecar_entry_materialize_count"
    )
    reverse_seek_sidecar_offset_probes = _metric_count(
        result, "sstable_reverse_seek_sidecar_offset_probe_count"
    )
    reverse_seek_sidecar_load_events = (
        reverse_seek_sidecar_hits
        + reverse_seek_sidecar_misses
        + reverse_seek_sidecar_stale
        + reverse_seek_sidecar_invalid
    )
    reverse_seek_sidecar_path_failures = (
        reverse_seek_sidecar_misses
        + reverse_seek_sidecar_stale
        + reverse_seek_sidecar_invalid
        + reverse_seek_sidecar_write_errors
        + reverse_seek_sidecar_fail_opens
    )
    if reverse_seek_sidecar_path_failures:
        reverse_seek_sidecar_status = "degraded"
    elif reverse_seek_sidecar_uses > 0:
        reverse_seek_sidecar_status = "observed"
    elif sstable_reverse_block_reads > 0:
        reverse_seek_sidecar_status = "fallback"
    else:
        reverse_seek_sidecar_status = "unobservable"
    index_prefix_checks = _metric_count(result, "sstable_index_prefix_filter_check_count")
    index_prefix_positives = _metric_count(result, "sstable_index_prefix_filter_positive_count")
    index_prefix_skips = _metric_count(result, "sstable_index_prefix_filter_skip_count")
    index_prefix_fail_opens = _metric_count(result, "sstable_index_prefix_filter_fail_open_count")
    block_index_prefix_checks = _metric_count(
        result, "sstable_block_index_prefix_filter_check_count"
    )
    block_index_prefix_positives = _metric_count(
        result, "sstable_block_index_prefix_filter_positive_count"
    )
    block_index_prefix_skips = _metric_count(
        result, "sstable_block_index_prefix_filter_skip_count"
    )
    block_index_prefix_fail_opens = _metric_count(
        result, "sstable_block_index_prefix_filter_fail_open_count"
    )
    sstable_heavy_required = bool(metadata.get("sstable_heavy_required"))
    rseek_sidecar_expectation = str(metadata.get("rseek_sidecar_expectation", "present"))
    rseek_sidecar_required = sstable_heavy_required and rseek_sidecar_expectation != "removed"

    metadata["claim_gate"] = "part20_index_topk"
    metadata["claim_expected_query_count"] = query_count
    metadata["claim_sstable_reverse_required"] = sstable_heavy_required
    metadata.update({
        "claim_reverse_frontier_probe_count": fusion_reverse_frontier_probes,
        "claim_reverse_frontier_in_range_count": fusion_reverse_frontier_in_range,
        "claim_reverse_frontier_file_count": fusion_reverse_frontier_file,
        "claim_reverse_frontier_tighten_count": fusion_reverse_frontier_tightens,
        "claim_reverse_frontier_empty_skip_count": fusion_reverse_frontier_empty_skips,
        "claim_reverse_frontier_fail_open_count": fusion_reverse_frontier_fail_opens,
        "claim_reverse_sstable_pending_count": fusion_reverse_sstable_pending,
        "claim_reverse_sstable_activation_count": fusion_reverse_sstable_activations,
        "claim_reverse_sstable_deferred_unopened_count": (
            fusion_reverse_sstable_deferred_unopened
        ),
        "claim_reverse_sstable_activation_equal_frontier_count": (
            fusion_reverse_sstable_equal_frontier_activations
        ),
        "claim_reverse_seek_sidecar_expectation": rseek_sidecar_expectation,
        "claim_reverse_seek_sidecar_required": rseek_sidecar_required,
        "claim_reverse_seek_sidecar_use_count": reverse_seek_sidecar_uses,
        "claim_reverse_seek_sidecar_hit_count": reverse_seek_sidecar_hits,
        "claim_reverse_seek_sidecar_load_events": reverse_seek_sidecar_load_events,
        "claim_reverse_seek_sidecar_path_failures": reverse_seek_sidecar_path_failures,
        "claim_reverse_seek_sidecar_status": reverse_seek_sidecar_status,
        "claim_reverse_block_span_scan_count": sstable_reverse_span_scans,
        "claim_reverse_block_span_scan_entry_count": sstable_reverse_span_scan_entries,
        "claim_reverse_block_span_materialize_entry_count": sstable_reverse_span_materializes,
        "claim_reverse_seek_sidecar_index_entry_count": reverse_seek_sidecar_index_entries,
        "claim_reverse_seek_sidecar_materialize_count": reverse_seek_sidecar_materializes,
        "claim_reverse_seek_sidecar_offset_probe_count": reverse_seek_sidecar_offset_probes,
        "claim_reverse_seek_sidecar_observed": reverse_seek_sidecar_uses > 0,
        "claim_reverse_seek_sidecar_clean": reverse_seek_sidecar_path_failures == 0,
        "claim_reverse_seek_sidecar_evidence_scope": (
            "counter/path evidence only; latency is not a hard gate. Span-scan "
            "counters prove whether runtime fallback parsed whole block entry spans; "
            "sidecar offset-probe counters expose the remaining bound-search work."
        ),
        "claim_block_index_prefix_check_count": block_index_prefix_checks,
        "claim_block_index_prefix_positive_count": block_index_prefix_positives,
        "claim_block_index_prefix_skip_count": block_index_prefix_skips,
        "claim_block_index_prefix_fail_open_count": block_index_prefix_fail_opens,
        "claim_ordered_topk_index_only_row_count": ordered_index_only_rows,
        "claim_ordered_topk_base_row_fetch_count": ordered_base_row_fetches,
    })

    if limit <= 0:
        failures.append("BENCH_CLAIM_MODE requires BENCH_INDEX_TOPK_LIMIT > 0")
    if not result.metrics_delta:
        failures.append("metrics_delta unavailable")
    required_claim_metric_keys = (
        "query_count",
        "index_ordered_topk_scan_count",
        "index_ordered_topk_entry_visit_count",
        "index_ordered_topk_reverse_scan_count",
        "index_ordered_topk_index_only_row_count",
        "index_ordered_topk_base_row_fetch_count",
        "query_sort_fallback_count",
        "fusion_reverse_scan_count",
        "fusion_reverse_sstable_frontier_probe_count",
        "fusion_reverse_sstable_frontier_in_range_count",
        "fusion_reverse_sstable_frontier_file_count",
        "fusion_reverse_sstable_frontier_tighten_count",
        "fusion_reverse_sstable_frontier_empty_skip_count",
        "fusion_reverse_sstable_frontier_fail_open_count",
        "fusion_reverse_sstable_pending_count",
        "fusion_reverse_sstable_activation_count",
        "fusion_reverse_sstable_deferred_unopened_count",
        "fusion_reverse_sstable_activation_equal_frontier_count",
        "fusion_reverse_raw_entry_read_count",
        "fusion_reverse_visible_candidate_count",
        "fusion_reverse_visible_put_count",
    )
    missing_claim_metric_keys = [
        key for key in required_claim_metric_keys if key not in result.metrics_delta
    ]
    if missing_claim_metric_keys:
        failures.append(
            "claim metrics unavailable: " + ", ".join(missing_claim_metric_keys)
        )
    required_cache_metric_keys = (
        "query_result_cache_eligible_count",
        "query_result_cache_hit_count",
        "query_result_cache_miss_count",
        "query_result_cache_stale_count",
        "query_result_cache_insert_count",
        "query_result_cache_invalidation_count",
    )
    missing_cache_metric_keys = [
        key for key in required_cache_metric_keys if key not in result.metrics_delta
    ]
    if missing_cache_metric_keys:
        failures.append(
            "query-result cache metrics unavailable: "
            + ", ".join(missing_cache_metric_keys)
        )
    if result.error:
        failures.append(f"query failed before claim checks: {result.error}")
    if result.success_count != result.planned_iters:
        failures.append(
            f"expected {result.planned_iters} successful measured iterations, got {result.success_count}"
        )
    if query_count != result.success_count:
        failures.append(f"expected query_count {result.success_count}, got {query_count}")
    if result.row_count != expected_rows:
        failures.append(f"expected {expected_rows} result rows, got {result.row_count}")
    cache_events = (
        query_result_cache_eligible
        + query_result_cache_hits
        + query_result_cache_misses
        + query_result_cache_stale
        + query_result_cache_inserts
        + query_result_cache_invalidations
    )
    if cache_events != 0:
        failures.append(
            "expected zero query-result cache events, got "
            f"eligible={query_result_cache_eligible}, hits={query_result_cache_hits}, "
            f"misses={query_result_cache_misses}, stale={query_result_cache_stale}, "
            f"inserts={query_result_cache_inserts}, invalidations={query_result_cache_invalidations}"
        )
    if result.success_count < 20:
        warnings.append(
            f"sample count {result.success_count} is below 20; latency percentiles are smoke-only"
        )
    if result.cv_pct > 25:
        warnings.append(f"latency CV {result.cv_pct:.1f}% is high; treat latency claims as noisy")

    frontier_accounted = (
        fusion_reverse_frontier_in_range
        + fusion_reverse_frontier_file
        + fusion_reverse_frontier_empty_skips
    )
    metadata["claim_reverse_frontier_accounted_count"] = frontier_accounted
    if frontier_accounted != fusion_reverse_frontier_probes:
        failures.append(
            "expected reverse frontier probes to equal in-range + file fallback + empty skips, got "
            f"probes={fusion_reverse_frontier_probes}, accounted={frontier_accounted}"
        )
    if fusion_reverse_frontier_fail_opens != fusion_reverse_frontier_file:
        failures.append(
            "expected reverse frontier fail-opens to match file fallback count, got "
            f"fail_open={fusion_reverse_frontier_fail_opens}, file={fusion_reverse_frontier_file}"
        )
    if fusion_reverse_sstable_activations > fusion_reverse_sstable_pending:
        failures.append(
            "expected reverse SSTable activations <= pending SSTables, got "
            f"activations={fusion_reverse_sstable_activations}, pending={fusion_reverse_sstable_pending}"
        )
    if fusion_reverse_sstable_deferred_unopened > fusion_reverse_sstable_pending:
        failures.append(
            "expected deferred unopened SSTables <= pending SSTables, got "
            f"deferred={fusion_reverse_sstable_deferred_unopened}, pending={fusion_reverse_sstable_pending}"
        )
    if sstable_reverse_iterators != fusion_reverse_sstable_activations:
        failures.append(
            "expected SSTable reverse iterator opens to match Fusion reverse SSTable activations, got "
            f"iterators={sstable_reverse_iterators}, activations={fusion_reverse_sstable_activations}"
        )

    is_ordered_asc = path in TOPK_ORDERED_ASC_PATHS
    is_ordered_desc = path in TOPK_ORDERED_DESC_PATHS
    is_ordered = is_ordered_asc or is_ordered_desc
    allow_empty_order_sort_fallback = (
        expected_rows == 0 and bool(metadata.get("allow_empty_order_sort_fallback"))
    )

    if is_ordered:
        if ordered_scans != query_count:
            failures.append(f"expected ordered Top-K scans={query_count}, got {ordered_scans}")
        min_visits = expected_rows * query_count
        max_visits = limit * query_count
        if ordered_visits < min_visits or ordered_visits > max_visits:
            failures.append(
                f"expected ordered Top-K visits between {min_visits} and {max_visits}, got {ordered_visits}"
            )
        if sort_fallbacks != 0 and not allow_empty_order_sort_fallback:
            failures.append(f"expected zero query sort fallbacks, got {sort_fallbacks}")
        if allow_empty_order_sort_fallback and sort_fallbacks != 0:
            warnings.append(
                f"empty ordered prefix-prune path reported {sort_fallbacks} sort fallbacks; "
                "index-prefix skip proof relies on ordered scan and storage counters"
            )
        expected_reverse_scans = query_count if is_ordered_desc else 0
        if reverse_scans != expected_reverse_scans:
            failures.append(
                f"expected ordered reverse scans={expected_reverse_scans}, got {reverse_scans}"
            )
        expected_ordered_rows = expected_rows * query_count
        ordered_source_rows = ordered_index_only_rows + ordered_base_row_fetches
        if ordered_source_rows != expected_ordered_rows:
            failures.append(
                "expected ordered Top-K index-only + base-row fetch rows "
                f"{expected_ordered_rows}, got {ordered_source_rows} "
                f"(index_only={ordered_index_only_rows}, base_fetch={ordered_base_row_fetches})"
            )
        if is_ordered_asc and fusion_reverse_scans != 0:
            failures.append(f"expected zero Fusion reverse scans for ASC, got {fusion_reverse_scans}")
        if path in TOPK_COMPOSITE_ORDERED_PATHS:
            if metadata.get("explain_ordered_composite_btree") is not True:
                failures.append("EXPLAIN did not report ordered composite BTree")
            if metadata.get("explain_order_by_limit") is not True:
                failures.append("EXPLAIN did not report ORDER BY/LIMIT")
        if path in TOPK_INDEX_ONLY_COVERING_PATHS and row_reads != 0:
            failures.append(f"expected zero base-row reads for covering Top-K, got {row_reads}")
        if path in TOPK_INDEX_ONLY_COVERING_PATHS:
            expected_index_only_rows = expected_rows * query_count
            if ordered_base_row_fetches != 0:
                failures.append(
                    "expected zero ordered Top-K base-row fetches for covering path, got "
                    f"{ordered_base_row_fetches}"
                )
            if ordered_index_only_rows != expected_index_only_rows:
                failures.append(
                    "expected ordered Top-K index-only rows "
                    f"{expected_index_only_rows}, got {ordered_index_only_rows}"
                )
        if path in TOPK_BASE_ROW_FETCH_PATHS:
            expected_base_fetches = expected_rows * query_count
            if ordered_index_only_rows != 0:
                failures.append(
                    "expected zero ordered Top-K index-only rows for heap-fetch control, got "
                    f"{ordered_index_only_rows}"
                )
            if ordered_base_row_fetches != expected_base_fetches:
                failures.append(
                    "expected ordered Top-K base-row fetches "
                    f"{expected_base_fetches}, got {ordered_base_row_fetches}"
                )
        if is_ordered_desc:
            raw_limit_per_query = int(
                metadata.get(
                    "fusion_reverse_raw_read_limit_per_query",
                    max(4 * limit, 96),
                )
            )
            raw_limit = raw_limit_per_query * query_count
            metadata["claim_fusion_reverse_raw_read_limit_per_query"] = raw_limit_per_query
            metadata["claim_fusion_reverse_raw_read_limit"] = raw_limit
            min_puts = expected_rows * query_count
            max_puts = limit * query_count
            if fusion_reverse_scans != query_count:
                failures.append(
                    f"expected Fusion reverse scans={query_count}, got {fusion_reverse_scans}"
                )
            if fusion_reverse_puts < min_puts or fusion_reverse_puts > max_puts:
                failures.append(
                    f"expected Fusion visible PUTs between {min_puts} and {max_puts}, got {fusion_reverse_puts}"
                )
            if fusion_reverse_raw_reads < fusion_reverse_puts:
                failures.append(
                    f"expected raw reverse reads >= visible PUTs, got raw={fusion_reverse_raw_reads}, puts={fusion_reverse_puts}"
                )
            if fusion_reverse_raw_reads > raw_limit:
                failures.append(
                    f"expected raw reverse reads <= {raw_limit}, got {fusion_reverse_raw_reads}"
                )
            if fusion_reverse_candidates > fusion_reverse_raw_reads:
                failures.append(
                    f"expected visible candidates <= raw reads, got candidates={fusion_reverse_candidates}, raw={fusion_reverse_raw_reads}"
                )
    elif path in TOPK_FALLBACK_PATHS:
        if ordered_scans != 0:
            failures.append(f"fallback path should not run ordered Top-K scans, got {ordered_scans}")
        if ordered_visits != 0:
            failures.append(f"fallback path should not visit ordered Top-K entries, got {ordered_visits}")
        if reverse_scans != 0:
            failures.append(f"fallback path should not report ordered reverse scans, got {reverse_scans}")
        if ordered_index_only_rows != 0:
            failures.append(
                f"fallback path should not report ordered Top-K index-only rows, got {ordered_index_only_rows}"
            )
        if ordered_base_row_fetches != 0:
            failures.append(
                f"fallback path should not report ordered Top-K base-row fetches, got {ordered_base_row_fetches}"
            )
        if path in TOPK_SORT_FALLBACK_PATHS and sort_fallbacks < query_count:
            failures.append(
                f"expected at least {query_count} query sort fallbacks, got {sort_fallbacks}"
            )
        if path in TOPK_COMPOSITE_FALLBACK_PATHS:
            if metadata.get("explain_ordered_composite_btree") is True:
                failures.append("fallback EXPLAIN unexpectedly reported ordered composite BTree")
            if metadata.get("explain_order_by_limit") is True:
                failures.append("fallback EXPLAIN unexpectedly reported ORDER BY/LIMIT")
    else:
        failures.append(f"unknown Top-K claim path {path!r}")

    if sstable_reverse_iterators > 0:
        metadata["claim_sstable_reverse_observed"] = True
        if sstable_reverse_decodes < sstable_reverse_yields:
            failures.append(
                f"expected SSTable reverse decodes >= yields, got decodes={sstable_reverse_decodes}, yields={sstable_reverse_yields}"
            )
        if sstable_reverse_yields > 0 and sstable_reverse_block_reads <= 0:
            failures.append(
                "expected SSTable reverse block reads when reverse iterator yielded entries"
            )
    else:
        metadata["claim_sstable_reverse_observed"] = False

    if sstable_heavy_required:
        sstable_decode_limit = max(16 * max(limit, 1), 512) * query_count
        sstable_yield_limit = max(8 * max(limit, 1), 256) * query_count
        metadata["claim_sstable_reverse_decode_limit"] = sstable_decode_limit
        metadata["claim_sstable_reverse_yield_limit"] = sstable_yield_limit
        metadata["claim_reverse_seek_sidecar_use_equals_block_reads"] = (
            reverse_seek_sidecar_uses == sstable_reverse_block_reads
        )
        required_reverse_seek_metric_keys = (
            "sstable_reverse_block_span_scan_count",
            "sstable_reverse_block_span_scan_entry_count",
            "sstable_reverse_block_span_materialize_entry_count",
            "sstable_reverse_seek_sidecar_hit_count",
            "sstable_reverse_seek_sidecar_miss_count",
            "sstable_reverse_seek_sidecar_stale_count",
            "sstable_reverse_seek_sidecar_invalid_count",
            "sstable_reverse_seek_sidecar_write_error_count",
            "sstable_reverse_seek_sidecar_use_count",
            "sstable_reverse_seek_sidecar_fail_open_count",
            "sstable_reverse_seek_sidecar_index_entry_count",
            "sstable_reverse_seek_sidecar_entry_materialize_count",
            "sstable_reverse_seek_sidecar_offset_probe_count",
        )
        missing_reverse_seek_metric_keys = [
            key for key in required_reverse_seek_metric_keys if key not in result.metrics_delta
        ]
        if missing_reverse_seek_metric_keys:
            failures.append(
                "reverse seek sidecar metrics unavailable: "
                + ", ".join(missing_reverse_seek_metric_keys)
            )
        if not is_ordered_desc:
            failures.append("SSTable-heavy claim is only valid for ordered DESC Top-K paths")
        if sstable_reverse_iterators <= 0:
            failures.append("expected SSTable reverse iterator opens for persisted DESC Top-K")
        if sstable_reverse_block_reads <= 0:
            failures.append("expected SSTable reverse block reads for persisted DESC Top-K")
        if sstable_reverse_decodes <= 0:
            failures.append("expected SSTable reverse block entry decodes for persisted DESC Top-K")
        if sstable_reverse_yields < expected_rows * query_count:
            failures.append(
                f"expected SSTable reverse yields >= {expected_rows * query_count}, got {sstable_reverse_yields}"
            )
        if sstable_reverse_decodes > sstable_decode_limit:
            failures.append(
                f"expected SSTable reverse decodes <= {sstable_decode_limit}, got {sstable_reverse_decodes}"
            )
        if sstable_reverse_yields > sstable_yield_limit:
            failures.append(
                f"expected SSTable reverse yields <= {sstable_yield_limit}, got {sstable_reverse_yields}"
            )
        phase = str(metadata.get("phase", ""))
        if rseek_sidecar_expectation == "removed":
            if reverse_seek_sidecar_uses != 0:
                failures.append(
                    "expected zero .rseek sidecar uses for removed-sidecar fallback, got "
                    f"{reverse_seek_sidecar_uses}"
                )
            if phase == "restart-first-pass" and reverse_seek_sidecar_misses <= 0:
                failures.append("expected .rseek sidecar miss during removed-sidecar restart-first-pass")
            if sstable_reverse_span_scans <= 0:
                failures.append("expected runtime reverse block span scans for removed-sidecar fallback")
            if sstable_reverse_span_scan_entries <= 0:
                failures.append("expected runtime reverse block span-scan entries for removed-sidecar fallback")
            if sstable_reverse_span_materializes != sstable_reverse_decodes:
                failures.append(
                    "expected runtime span materializations to account for all SSTable reverse decodes "
                    f"without .rseek, got span_materializes={sstable_reverse_span_materializes}, "
                    f"reverse_decodes={sstable_reverse_decodes}"
                )
            if reverse_seek_sidecar_index_entries != 0:
                failures.append(
                    "expected zero sidecar indexed entries for removed-sidecar fallback, got "
                    f"{reverse_seek_sidecar_index_entries}"
                )
            if reverse_seek_sidecar_materializes != 0:
                failures.append(
                    "expected zero sidecar materializations for removed-sidecar fallback, got "
                    f"{reverse_seek_sidecar_materializes}"
                )
            if reverse_seek_sidecar_offset_probes != 0:
                failures.append(
                    "expected zero sidecar offset probes for removed-sidecar fallback, got "
                    f"{reverse_seek_sidecar_offset_probes}"
                )
            if reverse_seek_sidecar_stale != 0:
                failures.append(f"expected zero stale .rseek sidecars, got {reverse_seek_sidecar_stale}")
            if reverse_seek_sidecar_invalid != 0:
                failures.append(f"expected zero invalid .rseek sidecars, got {reverse_seek_sidecar_invalid}")
            if reverse_seek_sidecar_write_errors != 0:
                failures.append(
                    f"expected zero .rseek sidecar write errors, got {reverse_seek_sidecar_write_errors}"
                )
            if reverse_seek_sidecar_fail_opens != 0:
                failures.append(
                    f"expected zero block-level .rseek fail-opens for missing-sidecar fallback, got {reverse_seek_sidecar_fail_opens}"
                )
        else:
            if reverse_seek_sidecar_uses <= 0:
                failures.append("expected persisted .rseek sidecar uses for SSTable-heavy DESC Top-K")
            if sstable_reverse_block_reads > 0 and reverse_seek_sidecar_uses != sstable_reverse_block_reads:
                failures.append(
                    "expected .rseek sidecar uses to match SSTable reverse block reads, got "
                    f"uses={reverse_seek_sidecar_uses}, block_reads={sstable_reverse_block_reads}"
                )
            if sstable_reverse_span_scans != 0:
                failures.append(
                    "expected zero runtime reverse block span scans when .rseek covers all "
                    f"reverse blocks, got {sstable_reverse_span_scans}"
                )
            if sstable_reverse_span_scan_entries != 0:
                failures.append(
                    "expected zero runtime reverse block span-scan entries when .rseek covers all "
                    f"reverse blocks, got {sstable_reverse_span_scan_entries}"
                )
            if sstable_reverse_span_materializes != 0:
                failures.append(
                    "expected zero runtime reverse block span materializations when .rseek covers all "
                    f"reverse blocks, got {sstable_reverse_span_materializes}"
                )
            if reverse_seek_sidecar_index_entries <= 0:
                failures.append("expected .rseek sidecar indexed entries for SSTable-heavy DESC Top-K")
            if reverse_seek_sidecar_materializes != sstable_reverse_decodes:
                failures.append(
                    "expected sidecar materializations to account for all SSTable reverse decodes "
                    f"when .rseek covers all reverse blocks, got sidecar_materializes={reverse_seek_sidecar_materializes}, "
                    f"reverse_decodes={sstable_reverse_decodes}"
                )
            if reverse_seek_sidecar_materializes > reverse_seek_sidecar_index_entries:
                failures.append(
                    "expected sidecar materializations <= sidecar indexed entries, got "
                    f"materializes={reverse_seek_sidecar_materializes}, indexed={reverse_seek_sidecar_index_entries}"
                )
            if reverse_seek_sidecar_misses != 0:
                failures.append(f"expected zero .rseek sidecar misses, got {reverse_seek_sidecar_misses}")
            if reverse_seek_sidecar_stale != 0:
                failures.append(f"expected zero stale .rseek sidecars, got {reverse_seek_sidecar_stale}")
            if reverse_seek_sidecar_invalid != 0:
                failures.append(f"expected zero invalid .rseek sidecars, got {reverse_seek_sidecar_invalid}")
            if reverse_seek_sidecar_write_errors != 0:
                failures.append(
                    f"expected zero .rseek sidecar write errors, got {reverse_seek_sidecar_write_errors}"
                )
            if reverse_seek_sidecar_fail_opens != 0:
                failures.append(
                    f"expected zero .rseek sidecar fail-opens, got {reverse_seek_sidecar_fail_opens}"
                )
            if phase == "restart-first-pass" and reverse_seek_sidecar_hits <= 0:
                failures.append(
                    "expected .rseek sidecar lazy-load hit during restart-first-pass"
                )
            elif phase == "first-pass" and reverse_seek_sidecar_hits <= 0:
                warnings.append(
                    "first-pass observed .rseek uses without a sidecar load hit in the "
                    "measured window; the sidecar may already have been cached by an earlier case"
                )

    if metadata.get("index_prefix_prune_required"):
        required_index_prefix_metric_keys = (
            "sstable_index_prefix_filter_check_count",
            "sstable_index_prefix_filter_positive_count",
            "sstable_index_prefix_filter_skip_count",
            "sstable_index_prefix_filter_fail_open_count",
        )
        missing_index_prefix_metric_keys = [
            key for key in required_index_prefix_metric_keys if key not in result.metrics_delta
        ]
        if missing_index_prefix_metric_keys:
            failures.append(
                "index-prefix filter metrics unavailable: "
                + ", ".join(missing_index_prefix_metric_keys)
            )
        expected_check_min = int(metadata.get("index_prefix_expected_checks_per_query", 0) or 0) * query_count
        expected_skip_min = int(metadata.get("index_prefix_expected_skips_per_query", 0) or 0) * query_count
        expected_positive_min = int(metadata.get("index_prefix_expected_positives_per_query", 0) or 0) * query_count
        iterator_open_max_per_query = metadata.get("index_prefix_sstable_reverse_iterator_open_max_per_query")
        if index_prefix_fail_opens != 0:
            failures.append(f"expected zero index-prefix Bloom fail-opens, got {index_prefix_fail_opens}")
        if index_prefix_checks < expected_check_min:
            failures.append(
                f"expected index-prefix Bloom checks >= {expected_check_min}, got {index_prefix_checks}"
            )
        if index_prefix_skips < expected_skip_min:
            failures.append(
                f"expected index-prefix Bloom skips >= {expected_skip_min}, got {index_prefix_skips}"
            )
        if index_prefix_positives < expected_positive_min:
            failures.append(
                f"expected index-prefix Bloom positives >= {expected_positive_min}, got {index_prefix_positives}"
            )
        if iterator_open_max_per_query is not None:
            iterator_open_limit = int(iterator_open_max_per_query) * query_count
            if sstable_reverse_iterators > iterator_open_limit:
                failures.append(
                    f"expected SSTable reverse iterator opens <= {iterator_open_limit} after index-prefix pruning, got {sstable_reverse_iterators}"
                )
        if metadata.get("index_prefix_reverse_iterator_open_must_not_exceed_positive_count"):
            metadata["claim_index_prefix_reverse_iterator_positive_bound"] = index_prefix_positives
            if sstable_reverse_iterators > index_prefix_positives:
                failures.append(
                    "expected SSTable reverse iterator opens <= index-prefix Bloom positives "
                    f"after pruning, got opens={sstable_reverse_iterators}, positives={index_prefix_positives}"
                )
        metadata["claim_index_prefix_filter_observed"] = index_prefix_checks > 0
        metadata["claim_index_prefix_filter_skip_count"] = index_prefix_skips

    if metadata.get("sql_block_index_prefix_prune_required"):
        required_block_index_prefix_metric_keys = (
            "sstable_index_prefix_filter_check_count",
            "sstable_index_prefix_filter_positive_count",
            "sstable_index_prefix_filter_skip_count",
            "sstable_index_prefix_filter_fail_open_count",
            "sstable_block_index_prefix_filter_check_count",
            "sstable_block_index_prefix_filter_positive_count",
            "sstable_block_index_prefix_filter_skip_count",
            "sstable_block_index_prefix_filter_fail_open_count",
        )
        missing_block_index_prefix_metric_keys = [
            key for key in required_block_index_prefix_metric_keys if key not in result.metrics_delta
        ]
        if missing_block_index_prefix_metric_keys:
            failures.append(
                "SQL block index-prefix metrics unavailable: "
                + ", ".join(missing_block_index_prefix_metric_keys)
            )
        expected_file_positive_min = int(
            metadata.get("sql_block_index_prefix_expected_file_positives_per_query", 0) or 0
        ) * query_count
        expected_block_skip_min = int(
            metadata.get("sql_block_index_prefix_expected_block_skips_per_query", 0) or 0
        ) * query_count
        compaction_runs = _metric_count(result, "compaction_run_count")
        if metadata.get("sql_block_index_prefix_false_positive_found") is not True:
            failures.append("expected setup to discover a natural SQL index-prefix Bloom false positive")
        if compaction_runs != 0:
            failures.append(f"expected zero compactions during SQL block index-prefix gate, got {compaction_runs}")
        if index_prefix_fail_opens != 0:
            failures.append(f"expected zero SQL index-prefix Bloom fail-opens, got {index_prefix_fail_opens}")
        if block_index_prefix_fail_opens != 0:
            failures.append(
                f"expected zero block SQL index-prefix fail-opens, got {block_index_prefix_fail_opens}"
            )
        if index_prefix_positives < expected_file_positive_min:
            failures.append(
                f"expected SQL index-prefix Bloom positives >= {expected_file_positive_min}, got {index_prefix_positives}"
            )
        if block_index_prefix_checks < expected_block_skip_min:
            failures.append(
                f"expected block SQL index-prefix checks >= {expected_block_skip_min}, got {block_index_prefix_checks}"
            )
        if block_index_prefix_skips < expected_block_skip_min:
            failures.append(
                f"expected block SQL index-prefix skips >= {expected_block_skip_min}, got {block_index_prefix_skips}"
            )
        metadata["claim_sql_block_index_prefix_file_positive_count"] = index_prefix_positives
        metadata["claim_sql_block_index_prefix_block_skip_count"] = block_index_prefix_skips
        metadata["claim_sql_block_index_prefix_compaction_run_count"] = compaction_runs

    if metadata.get("reverse_frontier_required"):
        required_reverse_frontier_metric_keys = (
            "fusion_reverse_sstable_frontier_probe_count",
            "fusion_reverse_sstable_frontier_in_range_count",
            "fusion_reverse_sstable_frontier_file_count",
            "fusion_reverse_sstable_frontier_tighten_count",
            "fusion_reverse_sstable_frontier_empty_skip_count",
            "fusion_reverse_sstable_frontier_fail_open_count",
            "fusion_reverse_sstable_pending_count",
            "fusion_reverse_sstable_activation_count",
            "fusion_reverse_sstable_deferred_unopened_count",
            "fusion_reverse_sstable_activation_equal_frontier_count",
            "sstable_index_prefix_filter_check_count",
            "sstable_index_prefix_filter_positive_count",
            "sstable_index_prefix_filter_skip_count",
            "sstable_index_prefix_filter_fail_open_count",
        )
        missing_reverse_frontier_metric_keys = [
            key for key in required_reverse_frontier_metric_keys if key not in result.metrics_delta
        ]
        if missing_reverse_frontier_metric_keys:
            failures.append(
                "reverse frontier metrics unavailable: "
                + ", ".join(missing_reverse_frontier_metric_keys)
            )
        expected_probe_min = int(
            metadata.get("reverse_frontier_expected_probes_per_query", 0) or 0
        ) * query_count
        expected_in_range_min = int(
            metadata.get("reverse_frontier_expected_in_range_per_query", 0) or 0
        ) * query_count
        expected_tighten_min = int(
            metadata.get("reverse_frontier_expected_tightens_per_query", 0) or 0
        ) * query_count
        expected_empty_skip_min = int(
            metadata.get("reverse_frontier_expected_empty_skips_per_query", 0) or 0
        ) * query_count
        expected_pending_min = int(
            metadata.get("reverse_frontier_expected_pending_per_query", 0) or 0
        ) * query_count
        activation_max = metadata.get("reverse_frontier_activation_max_per_query")
        deferred_min = int(
            metadata.get("reverse_frontier_expected_deferred_unopened_per_query", 0) or 0
        ) * query_count
        if not is_ordered_desc:
            failures.append("reverse frontier claim is only valid for ordered DESC Top-K paths")
        if index_prefix_fail_opens != 0:
            failures.append(f"expected zero index-prefix fail-opens in frontier gate, got {index_prefix_fail_opens}")
        if index_prefix_positives < fusion_reverse_frontier_probes:
            failures.append(
                "expected index-prefix positives to cover all frontier-probed SSTables, got "
                f"positives={index_prefix_positives}, frontier_probes={fusion_reverse_frontier_probes}"
            )
        if fusion_reverse_frontier_fail_opens != 0:
            failures.append(f"expected zero reverse frontier fail-open fallbacks, got {fusion_reverse_frontier_fail_opens}")
        if fusion_reverse_frontier_probes < expected_probe_min:
            failures.append(
                f"expected reverse frontier probes >= {expected_probe_min}, got {fusion_reverse_frontier_probes}"
            )
        if fusion_reverse_frontier_in_range < expected_in_range_min:
            failures.append(
                f"expected in-range reverse frontiers >= {expected_in_range_min}, got {fusion_reverse_frontier_in_range}"
            )
        if fusion_reverse_frontier_tightens < expected_tighten_min:
            failures.append(
                f"expected reverse frontier tighten count >= {expected_tighten_min}, got {fusion_reverse_frontier_tightens}"
            )
        if fusion_reverse_frontier_empty_skips < expected_empty_skip_min:
            failures.append(
                f"expected reverse frontier empty skips >= {expected_empty_skip_min}, got {fusion_reverse_frontier_empty_skips}"
            )
        if fusion_reverse_sstable_pending < expected_pending_min:
            failures.append(
                f"expected reverse pending SSTables >= {expected_pending_min}, got {fusion_reverse_sstable_pending}"
            )
        if activation_max is not None:
            activation_limit = int(activation_max) * query_count
            metadata["claim_reverse_frontier_activation_limit"] = activation_limit
            if fusion_reverse_sstable_activations > activation_limit:
                failures.append(
                    f"expected reverse SSTable activations <= {activation_limit}, got {fusion_reverse_sstable_activations}"
                )
        if metadata.get("reverse_frontier_activation_must_be_less_than_pending"):
            if fusion_reverse_sstable_activations >= fusion_reverse_sstable_pending:
                failures.append(
                    "expected reverse SSTable activations < pending SSTables, got "
                    f"activations={fusion_reverse_sstable_activations}, pending={fusion_reverse_sstable_pending}"
                )
        if fusion_reverse_sstable_deferred_unopened < deferred_min:
            failures.append(
                f"expected deferred unopened SSTables >= {deferred_min}, got {fusion_reverse_sstable_deferred_unopened}"
            )
        metadata["claim_reverse_frontier_observed"] = fusion_reverse_frontier_probes > 0
        metadata["claim_reverse_frontier_bloom_isolated"] = (
            index_prefix_fail_opens == 0
            and index_prefix_positives >= fusion_reverse_frontier_probes
        )

    _set_claim_result(result, failures, warnings)


def apply_part31_zone_map_claim_gate(result: BenchResult) -> None:
    if not BENCH_CLAIM_MODE or not result.metadata.get("sql_block_zone_map_prune_required"):
        return

    failures: List[str] = []
    warnings: List[str] = []
    metadata = result.metadata
    path = str(metadata.get("path", ""))
    control_role = str(metadata.get("zone_map_control_role", "enabled"))
    expected_rows = int(metadata.get("expected_rows", 0) or 0)
    query_count = _metric_count(result, "query_count")
    cache_events = sum(
        _metric_count(result, key)
        for key in (
            "query_result_cache_eligible_count",
            "query_result_cache_hit_count",
            "query_result_cache_miss_count",
            "query_result_cache_stale_count",
            "query_result_cache_insert_count",
            "query_result_cache_invalidation_count",
        )
    )
    checks = _metric_count(result, "sstable_block_zone_map_filter_check_count")
    positives = _metric_count(result, "sstable_block_zone_map_filter_positive_count")
    skips = _metric_count(result, "sstable_block_zone_map_filter_skip_count")
    fail_opens = _metric_count(result, "sstable_block_zone_map_filter_fail_open_count")
    mvcc_fail_opens = _metric_count(
        result, "sstable_block_zone_map_mvcc_overlap_fail_open_count"
    )
    mvcc_boundary_fail_opens = _metric_count(
        result, "sstable_block_zone_map_mvcc_boundary_split_fail_open_count"
    )
    mvcc_write_buffer_fail_opens = _metric_count(
        result, "sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count"
    )
    mvcc_memtable_fail_opens = _metric_count(
        result, "sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count"
    )
    mvcc_sstable_fail_opens = _metric_count(
        result, "sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count"
    )
    mvcc_reason_fail_opens = (
        mvcc_boundary_fail_opens
        + mvcc_write_buffer_fail_opens
        + mvcc_memtable_fail_opens
        + mvcc_sstable_fail_opens
    )
    schema_fail_opens = _metric_count(result, "sstable_block_zone_map_schema_fail_open_count")
    compactions = _metric_count(result, "compaction_run_count")

    metadata.update({
        "claim_gate": "part31_sql_block_zone_map_prune",
        "claim_zone_map_control_role": control_role,
        "claim_zone_map_check_count": checks,
        "claim_zone_map_positive_count": positives,
        "claim_zone_map_skip_count": skips,
        "claim_zone_map_fail_open_count": fail_opens,
        "claim_zone_map_mvcc_fail_open_count": mvcc_fail_opens,
        "claim_zone_map_mvcc_boundary_split_fail_open_count": mvcc_boundary_fail_opens,
        "claim_zone_map_mvcc_write_buffer_overlap_fail_open_count": mvcc_write_buffer_fail_opens,
        "claim_zone_map_mvcc_memtable_overlap_fail_open_count": mvcc_memtable_fail_opens,
        "claim_zone_map_mvcc_sstable_overlap_fail_open_count": mvcc_sstable_fail_opens,
        "claim_zone_map_mvcc_reason_accounted_count": mvcc_reason_fail_opens,
        "claim_zone_map_schema_fail_open_count": schema_fail_opens,
        "claim_zone_map_outcome_accounted_count": positives + skips + fail_opens,
        "claim_zone_map_compaction_run_count": compactions,
    })

    if not result.metrics_delta:
        failures.append("metrics_delta unavailable")
    required_metric_keys = (
        "query_count",
        "sstable_block_zone_map_filter_check_count",
        "sstable_block_zone_map_filter_positive_count",
        "sstable_block_zone_map_filter_skip_count",
        "sstable_block_zone_map_filter_fail_open_count",
        "sstable_block_zone_map_mvcc_overlap_fail_open_count",
        "sstable_block_zone_map_mvcc_boundary_split_fail_open_count",
        "sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count",
        "sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count",
        "sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count",
        "sstable_block_zone_map_schema_fail_open_count",
        "compaction_run_count",
    )
    missing_metric_keys = [key for key in required_metric_keys if key not in result.metrics_delta]
    if missing_metric_keys:
        failures.append("zone-map metrics unavailable: " + ", ".join(missing_metric_keys))
    if result.error:
        failures.append(f"query failed before claim checks: {result.error}")
    if result.success_count != result.planned_iters:
        failures.append(
            f"expected {result.planned_iters} successful measured iterations, got {result.success_count}"
        )
    if query_count != result.success_count:
        failures.append(f"expected query_count {result.success_count}, got {query_count}")
    if result.row_count != expected_rows:
        failures.append(f"expected {expected_rows} result rows, got {result.row_count}")
    if cache_events != 0:
        failures.append(f"expected zero query-result cache events, got {cache_events}")
    if compactions != 0:
        failures.append(f"expected zero compactions during Part 31 query gate, got {compactions}")
    if checks != positives + skips + fail_opens:
        failures.append(
            "expected zone-map checks to equal positive + skip + fail-open, got "
            f"checks={checks}, positives={positives}, skips={skips}, fail_opens={fail_opens}"
        )
    if mvcc_fail_opens != mvcc_reason_fail_opens:
        failures.append(
            "expected MVCC fail-opens to equal boundary + write-buffer + memtable + SSTable "
            f"reasons, got mvcc={mvcc_fail_opens}, reasons={mvcc_reason_fail_opens}"
        )
    if schema_fail_opens != 0:
        failures.append(f"expected zero schema fail-opens in Part 31 benchmark, got {schema_fail_opens}")
    if metadata.get("result_checksum_consistent") is False:
        failures.append("result checksum changed across measured iterations")

    valid_part31_paths = (
        "zone_map_clustered_absent",
        "zone_map_clustered_hit",
        "zone_map_random_control",
        "zone_map_mvcc_fail_open",
    )
    if control_role == "disabled":
        if path not in valid_part31_paths:
            failures.append(f"unknown Part 31 zone-map path {path!r}")
        if checks != 0 or positives != 0 or skips != 0 or fail_opens != 0:
            failures.append(
                "expected disabled-control query to avoid zone-map checks entirely, got "
                f"checks={checks}, positives={positives}, skips={skips}, fail_opens={fail_opens}"
            )
        if mvcc_fail_opens != 0 or mvcc_reason_fail_opens != 0:
            failures.append(
                "expected disabled-control query to avoid MVCC zone-map fail-opens, got "
                f"mvcc={mvcc_fail_opens}, reasons={mvcc_reason_fail_opens}"
            )
        if schema_fail_opens != 0:
            failures.append(
                f"expected disabled-control query to avoid schema fail-opens, got {schema_fail_opens}"
            )
        if result.success_count < 5:
            warnings.append(
                f"sample count {result.success_count} is below 5; latency is smoke-only"
            )
        _set_claim_result(result, failures, warnings)
        return
    if control_role != "enabled":
        failures.append(f"unknown Part 31 zone-map control role {control_role!r}")

    if checks <= 0:
        failures.append("expected zone-map checks to be observed")
    if path in ("zone_map_clustered_absent", "zone_map_clustered_hit"):
        if skips <= 0:
            failures.append("expected clustered zone-map case to skip at least one block")
        if path == "zone_map_clustered_hit" and positives <= 0:
            failures.append("expected clustered hit case to read at least one matching block")
    elif path == "zone_map_random_control":
        if positives <= 0:
            failures.append("expected random-control case to produce positive zone-map reads")
        if skips > 0:
            warnings.append(
                "random-control case still skipped some blocks; data layout may not fully defeat min/max pruning"
            )
    elif path == "zone_map_mvcc_fail_open":
        if mvcc_fail_opens <= 0:
            failures.append("expected MVCC-overlap case to report mvcc fail-open")
        if mvcc_sstable_fail_opens <= 0:
            failures.append("expected MVCC-overlap case to report SSTable-overlap reason")
        if result.row_count != 0:
            failures.append("expected MVCC old-bucket query to return zero visible rows")
    else:
        failures.append(f"unknown Part 31 zone-map path {path!r}")

    if result.success_count < 5:
        warnings.append(
            f"sample count {result.success_count} is below 5; latency is smoke-only"
        )
    _set_claim_result(result, failures, warnings)


def finalize_part20_case_result(
    result: BenchResult,
    query,
    path: str,
    extra_metadata: Dict[str, object],
    phase: str,
    rows_count: int,
    limit: int,
) -> BenchResult:
    result.metadata.update({
        "index_topk_rows": rows_count,
        "limit": limit,
        "path": path,
        "expected_rows": min(limit, rows_count),
        "phase": phase,
    })
    if phase == "first-pass":
        result.metadata.update({
            "cache_phase": "first_persisted_pass_same_process",
            "claim_cache_scope": "same_process_persisted_first_pass_device_cold_unproven",
            "phase_definition": (
                "single measured execution after Part 20 checkpoint and before "
                "case-specific warmup; proves first persisted SSTable path but not cold OS cache"
            ),
            "latency_claim_scope": "path/counter evidence only; single-sample latency is smoke-only",
        })
    elif phase == "restart-first-pass":
        result.metadata.update({
            "cache_phase": "restart_first_pass_process_cold_os_cache_uncontrolled",
            "claim_cache_scope": "process_cold_os_cache_uncontrolled_device_cold_unproven",
            "phase_definition": (
                "single measured execution after stopping and restarting the benchmark-owned "
                "FusionDB process on the checkpointed data dir"
            ),
            "latency_claim_scope": (
                "process caches are cold after restart; OS page cache may still be warm"
            ),
        })
    elif phase == "restart-warm":
        result.metadata.update({
            "cache_phase": "restart_warm",
            "claim_cache_scope": "restart_warm_after_process_cold_first_pass",
            "phase_definition": (
                "measured after case-specific warmup executions on the restarted benchmark-owned "
                "FusionDB process"
            ),
        })
    elif path in TOPK_ORDERED_DESC_PATHS and INDEX_TOPK_SSTABLE_CLAIM:
        result.metadata.update({
            "phase_definition": (
                "measured after case-specific warmup executions over checkpointed persisted SSTables"
            ),
        })
    if path in TOPK_ORDERED_DESC_PATHS:
        result.metadata.update({
            "sstable_heavy_required": INDEX_TOPK_SSTABLE_CLAIM,
            "sstable_heavy_setup": (
                "checkpoint_after_part20_load"
                if INDEX_TOPK_SSTABLE_CLAIM
                else "disabled"
            ),
        })
    result.metadata.update(extra_metadata)
    os_cache_control = result.metadata.get("os_cache_control")
    if (
        phase == "restart-first-pass"
        and isinstance(os_cache_control, dict)
        and os_cache_control.get("os_page_cache_controlled")
    ):
        result.metadata.update({
            "cache_phase": "os_cache_dropped_first_pass",
            "claim_cache_scope": "process_cold_os_cache_dropped_device_cold_unproven",
            "phase_definition": (
                "single measured execution after stopping the benchmark-owned FusionDB process, "
                "executing host drop_caches, and restarting on the checkpointed data dir"
            ),
            "latency_claim_scope": (
                "process-local caches are cold and host drop_caches succeeded; this is still "
                "an unaudited benchmark-owned cold-cache method, not an official TPC-style claim"
            ),
        })
    elif phase == "restart-warm":
        result.metadata["cache_phase"] = "restart_warm_after_first_pass"
        result.metadata.setdefault(
            "claim_cache_scope", "restart_warm_after_process_cold_first_pass"
        )
    elif phase == "warm":
        result.metadata.setdefault("cache_phase", "warm_same_process")
        result.metadata.setdefault("claim_cache_scope", "warm_same_process")
    if str(path).startswith("composite_"):
        annotate_explain_metadata(result, query)
    annotate_block_cache_metrics(result)
    annotate_prefix_filter_metrics(result)
    apply_part20_claim_gate(result)
    return result


def index_topk_desc_restart_cases(rows_count: int, limit: int) -> List[Tuple[str, object, str, Dict[str, object]]]:
    composite_hosts = index_topk_composite_hosts(rows_count)
    composite_rows_per_host_avg = rows_count / max(composite_hosts, 1)
    composite_expected_rows = min(limit, int(composite_rows_per_host_avg + 0.999))
    composite_upper_half_expected_rows = min(
        limit, int((composite_rows_per_host_avg / 2) + 0.999)
    )
    composite_window_expected_rows = min(
        limit, max(limit * 2, int((composite_rows_per_host_avg / 4) + 0.999), 1)
    )
    composite_metadata = {
        "composite_hosts": composite_hosts,
        "composite_rows_per_host_avg": round(composite_rows_per_host_avg, 3),
        "cache_busting": "host_id rotates by benchmark iteration",
    }
    return [
        (
            "TopK DESC index order",
            f"SELECT id, score FROM bench_topk_idx ORDER BY score DESC LIMIT {limit}",
            "secondary_index_order_desc",
            {},
        ),
        (
            "TopK DESC covering payload",
            f"SELECT id, score, payload FROM bench_topk_cover ORDER BY score DESC LIMIT {limit}",
            "secondary_index_order_desc_covering",
            {},
        ),
        (
            "TopK composite index DESC",
            index_topk_composite_query("bench_topk_comp_idx", "DESC", limit, rows_count),
            "composite_ordered_index_desc",
            {**composite_metadata, "expected_rows": composite_expected_rows},
        ),
        (
            "TopK composite range DESC",
            index_topk_composite_query(
                "bench_topk_comp_idx", "DESC", limit, rows_count, range_mode="upper_half"
            ),
            "composite_ordered_index_range_desc",
            {
                **composite_metadata,
                "range_mode": "host-local upper half",
                "expected_rows": composite_upper_half_expected_rows,
            },
        ),
        (
            "TopK composite window DESC",
            index_topk_composite_query(
                "bench_topk_comp_idx", "DESC", limit, rows_count, range_mode="middle_window"
            ),
            "composite_ordered_index_window_desc",
            {
                **composite_metadata,
                "range_mode": "host-local middle window",
                "expected_rows": composite_window_expected_rows,
            },
        ),
    ]


def part20_index_topk() -> List[BenchResult]:
    R, cat = [], "Indexed Top-K"
    rows = index_topk_rows()
    limit = max(0, INDEX_TOPK_LIMIT)
    composite_hosts = index_topk_composite_hosts(rows)
    composite_rows_per_host_avg = rows / max(composite_hosts, 1)
    composite_expected_rows = min(limit, int(composite_rows_per_host_avg + 0.999))
    composite_upper_half_expected_rows = min(
        limit, int((composite_rows_per_host_avg / 2) + 0.999)
    )
    composite_window_expected_rows = min(
        limit, max(limit * 2, int((composite_rows_per_host_avg / 4) + 0.999), 1)
    )
    composite_residual_expected_rows = min(
        limit, int((composite_rows_per_host_avg / 10) + 0.999)
    )
    composite_metadata = {
        "composite_hosts": composite_hosts,
        "composite_rows_per_host_avg": round(composite_rows_per_host_avg, 3),
        "cache_busting": "host_id rotates by benchmark iteration",
    }
    cases = [
        (
            "TopK full scan ASC",
            f"SELECT id, score FROM bench_topk_scan ORDER BY score ASC LIMIT {limit}",
            "full_scan_baseline",
        ),
        (
            "TopK range ceiling",
            f"SELECT id, score FROM bench_topk_idx WHERE score >= 0 ORDER BY score ASC LIMIT {limit}",
            "existing_range_index_path",
        ),
        (
            "TopK index order ASC",
            f"SELECT id, score FROM bench_topk_idx ORDER BY score ASC LIMIT {limit}",
            "pure_secondary_index_order",
        ),
        (
            "TopK index fetch payload",
            f"SELECT id, score, payload FROM bench_topk_idx ORDER BY score ASC LIMIT {limit}",
            "secondary_index_order_heap_fetch",
        ),
        (
            "TopK covering payload",
            f"SELECT id, score, payload FROM bench_topk_cover ORDER BY score ASC LIMIT {limit}",
            "secondary_index_order_covering",
        ),
        (
            "TopK expression fallback",
            f"SELECT id, score FROM bench_topk_idx ORDER BY score + 0 ASC LIMIT {limit}",
            "expression_order_fallback",
        ),
        (
            "TopK DESC index order",
            f"SELECT id, score FROM bench_topk_idx ORDER BY score DESC LIMIT {limit}",
            "secondary_index_order_desc",
        ),
        (
            "TopK DESC covering payload",
            f"SELECT id, score, payload FROM bench_topk_cover ORDER BY score DESC LIMIT {limit}",
            "secondary_index_order_desc_covering",
        ),
        (
            "TopK BOOLEAN index order",
            f"SELECT id, flag FROM bench_topk_types ORDER BY flag ASC LIMIT {limit}",
            "secondary_index_order_boolean",
        ),
        (
            "TopK DATE32 index order",
            f"SELECT id, d FROM bench_topk_types ORDER BY d ASC LIMIT {limit}",
            "secondary_index_order_date32",
        ),
        (
            "TopK TIMESTAMPTZ index order",
            f"SELECT id, ts FROM bench_topk_types ORDER BY ts ASC LIMIT {limit}",
            "secondary_index_order_timestamptz",
        ),
        (
            "TopK INTERVAL index order",
            f"SELECT id, span FROM bench_topk_types ORDER BY span ASC LIMIT {limit}",
            "secondary_index_order_interval_prefix",
        ),
        (
            "TopK TIMESTAMPTZ covering payload",
            f"SELECT id, ts, payload FROM bench_topk_types ORDER BY ts ASC LIMIT {limit}",
            "secondary_index_order_timestamptz_covering",
        ),
        (
            "TopK composite full scan ASC",
            index_topk_composite_query("bench_topk_comp_scan", "ASC", limit, rows),
            "composite_full_scan_baseline",
            {**composite_metadata, "expected_rows": composite_expected_rows},
        ),
        (
            "TopK composite index ASC",
            index_topk_composite_query("bench_topk_comp_idx", "ASC", limit, rows),
            "composite_ordered_index_asc",
            {**composite_metadata, "expected_rows": composite_expected_rows},
        ),
        (
            "TopK composite covering payload ASC",
            index_topk_composite_query(
                "bench_topk_comp_cover",
                "ASC",
                limit,
                rows,
                projection="id, host_id, ts, payload, metric",
            ),
            "composite_ordered_index_covering_asc",
            {
                **composite_metadata,
                "expected_rows": composite_expected_rows,
                "covering_projection": "id, host_id, ts, payload, metric",
                "include_columns": "payload, metric",
            },
        ),
        (
            "TopK composite index DESC",
            index_topk_composite_query("bench_topk_comp_idx", "DESC", limit, rows),
            "composite_ordered_index_desc",
            {**composite_metadata, "expected_rows": composite_expected_rows},
        ),
        (
            "TopK composite range DESC",
            index_topk_composite_query(
                "bench_topk_comp_idx", "DESC", limit, rows, range_mode="upper_half"
            ),
            "composite_ordered_index_range_desc",
            {
                **composite_metadata,
                "range_mode": "host-local upper half",
                "expected_rows": composite_upper_half_expected_rows,
            },
        ),
        (
            "TopK composite window DESC",
            index_topk_composite_query(
                "bench_topk_comp_idx", "DESC", limit, rows, range_mode="middle_window"
            ),
            "composite_ordered_index_window_desc",
            {
                **composite_metadata,
                "range_mode": "host-local middle window",
                "expected_rows": composite_window_expected_rows,
            },
        ),
        (
            "TopK composite residual fallback",
            index_topk_composite_query(
                "bench_topk_comp_idx", "DESC", limit, rows, residual=True
            ),
            "composite_residual_unbounded_order",
            {
                **composite_metadata,
                "residual_predicate": "payload = 'hot'",
                "expected_rows": composite_residual_expected_rows,
            },
        ),
        (
            "TopK composite no-prefix fallback",
            f"SELECT id, host_id, ts FROM bench_topk_comp_idx WHERE ts >= 0 ORDER BY ts DESC LIMIT {limit}",
            "composite_no_leading_equality_fallback",
            {
                **composite_metadata,
                "cache_busting": "none; fixed missing-prefix fallback query",
                "fallback_reason": "missing leading equality prefix",
            },
        ),
        (
            "TopK composite mixed-order fallback",
            index_topk_composite_query("bench_topk_comp_idx", "DESC, id ASC", limit, rows),
            "composite_mixed_order_fallback",
            {
                **composite_metadata,
                "fallback_reason": "multiple ORDER BY expressions",
                "expected_rows": composite_expected_rows,
            },
        ),
    ]

    def finalize_case_result(result: BenchResult, query, path: str, extra_metadata: Dict[str, object], phase: str) -> BenchResult:
        result.metadata.update({
            "index_topk_rows": rows,
            "limit": limit,
            "path": path,
            "expected_rows": min(limit, rows),
            "phase": phase,
        })
        if phase == "first-pass":
            result.metadata.update({
                "cache_phase": "first_persisted_pass_same_process",
                "claim_cache_scope": "same_process_persisted_first_pass_device_cold_unproven",
                "phase_definition": (
                    "single measured execution after Part 20 checkpoint and before "
                    "case-specific warmup; proves first persisted SSTable path but not cold OS cache"
                ),
                "latency_claim_scope": "path/counter evidence only; single-sample latency is smoke-only",
            })
        elif path in TOPK_ORDERED_DESC_PATHS and INDEX_TOPK_SSTABLE_CLAIM:
            result.metadata.update({
                "cache_phase": "warm_same_process",
                "claim_cache_scope": "warm_same_process",
                "phase_definition": (
                    "measured after case-specific warmup executions over checkpointed persisted SSTables"
                ),
            })
        if path in TOPK_ORDERED_DESC_PATHS:
            result.metadata.update({
                "sstable_heavy_required": INDEX_TOPK_SSTABLE_CLAIM,
                "sstable_heavy_setup": (
                    "checkpoint_after_part20_load"
                    if INDEX_TOPK_SSTABLE_CLAIM
                    else "disabled"
                ),
            })
        result.metadata.update(extra_metadata)
        if str(path).startswith("composite_"):
            annotate_explain_metadata(result, query)
        annotate_block_cache_metrics(result)
        annotate_prefix_filter_metrics(result)
        apply_part20_claim_gate(result)
        return result

    for case in cases:
        name, query, path = case[:3]
        extra_metadata = case[3] if len(case) > 3 else {}
        if INDEX_TOPK_FIRST_PERSISTED_PASS and path in TOPK_ORDERED_DESC_PATHS:
            first = bench_with_phase(name, query, "first-pass", cat=cat)
            R.append(finalize_case_result(first, query, path, extra_metadata, "first-pass"))

        result = bench(name, query, cat=cat)
        finalize_case_result(result, query, path, extra_metadata, "warm")
        R.append(result)
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 24 — SQL Index-Prefix SSTable Pruning
# ═══════════════════════════════════════════════════════════════════════════════
def part24_index_topk_prefix_prune() -> List[BenchResult]:
    R, cat = [], "Index-Prefix Top-K Prune"
    limit = max(0, INDEX_TOPK_LIMIT)
    decoy_sstables = index_topk_prefix_prune_decoy_sstables()
    rows_per_host = index_topk_prefix_prune_rows_per_host()
    target_rows = max(rows_per_host, INDEX_TOPK_LIMIT * 4)
    low_host = 1
    target_host = 50
    absent_host = 51
    high_host = 99
    false_positive_budget = max(1, decoy_sstables // 20)
    expected_decoy_skips = max(1, decoy_sstables - false_positive_budget)
    common_metadata = {
        "prefix_prune_matrix": "index_topk_prefix_prune",
        "index_prefix_prune_required": True,
        "index_prefix_decoy_sstables": decoy_sstables,
        "index_prefix_rows_per_host": rows_per_host,
        "index_prefix_target_rows": target_rows,
        "index_prefix_low_host": low_host,
        "index_prefix_target_host": target_host,
        "index_prefix_absent_host": absent_host,
        "index_prefix_high_host": high_host,
        "index_prefix_false_positive_budget_per_query": false_positive_budget,
        "index_prefix_expected_skips_per_query": expected_decoy_skips,
        "measurement_guidance": (
            "Decoy SSTables contain low/high hosts so their file key ranges overlap "
            "the target host, but their SQL index-prefix Bloom filters do not contain "
            "the target prefix. The primary proof is index-prefix skip counters and "
            "bounded SSTable reverse iterator opens, not latency alone."
        ),
    }
    cases = [
        (
            "TopK prefix prune composite DESC",
            index_topk_prefix_prune_query(target_host, "DESC", limit),
            "composite_prefix_prune_desc",
            {
                **common_metadata,
                "expected_rows": min(limit, target_rows),
                "index_prefix_expected_checks_per_query": decoy_sstables + 1,
                "index_prefix_expected_positives_per_query": 1,
                "index_prefix_reverse_iterator_open_must_not_exceed_positive_count": True,
                "fusion_reverse_raw_read_limit_per_query": max(4 * limit, 96),
                "sstable_heavy_required": INDEX_TOPK_SSTABLE_CLAIM,
            },
        ),
        (
            "TopK prefix prune range DESC",
            index_topk_prefix_prune_query(target_host, "DESC", limit, target_rows // 2),
            "composite_prefix_prune_range_desc",
            {
                **common_metadata,
                "range_mode": "target-host upper half",
                "expected_rows": min(limit, target_rows - (target_rows // 2)),
                "index_prefix_expected_checks_per_query": decoy_sstables + 1,
                "index_prefix_expected_positives_per_query": 1,
                "index_prefix_reverse_iterator_open_must_not_exceed_positive_count": True,
                "fusion_reverse_raw_read_limit_per_query": max(4 * limit, 96),
                "sstable_heavy_required": INDEX_TOPK_SSTABLE_CLAIM,
            },
        ),
        (
            "TopK prefix prune absent DESC",
            index_topk_prefix_prune_query(absent_host, "DESC", limit),
            "composite_prefix_prune_absent_desc",
            {
                **common_metadata,
                "expected_rows": 0,
                "index_prefix_expected_checks_per_query": decoy_sstables,
                "index_prefix_expected_positives_per_query": 0,
                "index_prefix_sstable_reverse_iterator_open_max_per_query": false_positive_budget,
                "sstable_heavy_required": False,
                "allow_empty_order_sort_fallback": True,
            },
        ),
    ]

    for name, query, path, extra_metadata in cases:
        if BENCH_CLAIM_MODE:
            first = bench_with_phase(name, query, "first-pass", cat=cat)
            R.append(
                finalize_part20_case_result(
                    first, query, path, extra_metadata, "first-pass", target_rows, limit
                )
            )

        result = bench(name, query, cat=cat)
        R.append(
            finalize_part20_case_result(
                result, query, path, extra_metadata, "warm", target_rows, limit
            )
        )
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 30 — SQL Block Index-Prefix SSTable Pruning
# ═══════════════════════════════════════════════════════════════════════════════
def part30_sql_block_index_prefix_prune() -> List[BenchResult]:
    R, cat = [], "SQL Block Index-Prefix Top-K Prune"
    limit = max(0, INDEX_TOPK_LIMIT)
    decoy_sstables = sql_block_index_prefix_decoy_sstables()
    prefixes_per_sstable = max(2, sql_block_index_prefix_prefixes_per_sstable())
    target_rows = sql_block_index_prefix_target_rows()
    target_host = SQL_BLOCK_INDEX_PREFIX_TARGET_HOST or SQL_BLOCK_INDEX_PREFIX_GAP_HOST
    discovery = dict(SQL_BLOCK_INDEX_PREFIX_DISCOVERY)
    discovery_file_positives = int(discovery.get("index_prefix_positives", 0) or 0)
    discovery_block_skips = int(discovery.get("block_index_prefix_skips", 0) or 0)
    false_positive_found = bool(discovery.get("found"))
    common_metadata = {
        "matrix": "sql_block_index_prefix_prune",
        "sql_block_index_prefix_prune_required": True,
        "sql_block_index_prefix_decoy_sstables": decoy_sstables,
        "sql_block_index_prefix_prefixes_per_sstable": prefixes_per_sstable,
        "sql_block_index_prefix_target_rows": target_rows,
        "sql_block_index_prefix_target_host": target_host,
        "sql_block_index_prefix_false_positive_found": false_positive_found,
        "sql_block_index_prefix_discovery": discovery,
        "sql_block_index_prefix_expected_file_positives_per_query": max(
            1, discovery_file_positives + 1
        ),
        "sql_block_index_prefix_expected_block_skips_per_query": max(
            1, discovery_block_skips
        ),
        "sql_block_index_prefix_expected_block_fail_opens_per_query": 0,
        "measurement_guidance": (
            "Decoy SSTables contain only real neighboring SQL index prefixes below and above "
            "the selected target host, so their file key ranges overlap the target range. "
            "Setup first discovers a natural file-level SQL index-prefix Bloom false positive "
            "for that host, then inserts matching target rows into a separate SSTable. The "
            "primary proof is file-level positives plus block-level SQL index-prefix skips "
            "with stable result checksums."
        ),
    }
    cases = [
        (
            "TopK SQL block index-prefix ASC",
            sql_block_index_prefix_query(target_host, "ASC", limit),
            "composite_block_index_prefix_prune_asc",
            {
                **common_metadata,
                "expected_rows": min(limit, target_rows),
                "allow_empty_order_sort_fallback": False,
                "sstable_heavy_required": False,
            },
        ),
    ]

    for name, query, path, extra_metadata in cases:
        if BENCH_CLAIM_MODE:
            first = bench_with_phase(name, query, "first-pass", cat=cat)
            R.append(
                finalize_part20_case_result(
                    first, query, path, extra_metadata, "first-pass", target_rows, limit
                )
            )

        result = bench(name, query, cat=cat)
        R.append(
            finalize_part20_case_result(
                result, query, path, extra_metadata, "warm", target_rows, limit
            )
        )
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 31 — SQL Block Zone-Map SSTable Pruning
# ═══════════════════════════════════════════════════════════════════════════════
def sql_block_zone_map_query(table: str, bucket: int, disabled_control: bool = False):
    def query(_phase: str, index: int) -> str:
        # Vary a semantic no-op predicate so result-cache activity cannot mimic storage pruning.
        marker_floor = -1 - index
        predicate = f"bucket = {bucket} AND marker >= {marker_floor}"
        sql_text = (
            f"SELECT id, bucket FROM {table} "
            f"WHERE {predicate}"
        )
        if disabled_control:
            return f"{SQL_BLOCK_ZONE_MAP_DISABLE_HINT} {sql_text}"
        return sql_text

    return query


def sql_block_zone_map_rows_for_bucket(rows_count: int, bucket: int, bucket_count: int) -> int:
    if bucket < 0 or bucket >= bucket_count:
        return 0
    rows_per_bucket = rows_count // bucket_count
    tail = rows_count - (rows_per_bucket * bucket_count)
    target_bucket = SQL_BLOCK_ZONE_MAP_TARGET_BUCKET % bucket_count
    return rows_per_bucket + (tail if bucket == target_bucket else 0)


def finalize_part31_zone_map_result(
    result: BenchResult,
    path: str,
    expected_rows: int,
    extra_metadata: Dict[str, object],
) -> BenchResult:
    result.metadata.update({
        "matrix": "sql_block_zone_map_prune",
        "path": path,
        "expected_rows": expected_rows,
        "sql_block_zone_map_prune_required": True,
    })
    result.metadata.update(extra_metadata)
    result.metadata.setdefault("phase", "warm")
    result.metadata.setdefault("zone_map_control_role", "enabled")
    result.metadata.setdefault("zone_map_control_pair_key", path)
    annotate_block_cache_metrics(result)
    annotate_prefix_filter_metrics(result)
    apply_part31_zone_map_claim_gate(result)
    return result


def part31_sql_block_zone_map_prune() -> List[BenchResult]:
    R, cat = [], "SQL Block Zone-Map Prune"
    rows_count = sql_block_zone_map_rows()
    random_rows = sql_block_zone_map_random_rows()
    mvcc_rows = sql_block_zone_map_mvcc_rows()
    bucket_count = SQL_BLOCK_ZONE_MAP_BUCKETS
    target_bucket = SQL_BLOCK_ZONE_MAP_TARGET_BUCKET % bucket_count
    absent_bucket = SQL_BLOCK_ZONE_MAP_ABSENT_BUCKET
    clustered_target_rows = sql_block_zone_map_rows_for_bucket(
        rows_count, target_bucket, bucket_count
    )
    random_target_rows = sql_block_zone_map_rows_for_bucket(
        random_rows, target_bucket, bucket_count
    )
    common_metadata = {
        "sql_block_zone_map_rows": rows_count,
        "sql_block_zone_map_random_rows": random_rows,
        "sql_block_zone_map_mvcc_rows": mvcc_rows,
        "sql_block_zone_map_buckets": bucket_count,
        "sql_block_zone_map_target_bucket": target_bucket,
        "sql_block_zone_map_absent_bucket": absent_bucket,
        "sql_block_zone_map_payload_bytes": SQL_BLOCK_ZONE_MAP_PAYLOAD_BYTES,
        "sql_block_zone_map_disabled_control_enabled": SQL_BLOCK_ZONE_MAP_DISABLED_CONTROL,
        "measurement_guidance": (
            "Clustered table groups bucket values by primary-key order, so trusted min/max "
            "zone maps should skip nonmatching data blocks. Random-control table interleaves "
            "bucket values by primary-key order, so most blocks should remain positive. MVCC "
            "case updates the same keys into a newer SSTable and should fail open instead of "
            "skipping newer no-match blocks."
        ),
    }
    if SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT:
        common_metadata.update(SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT)
    cases = [
        (
            "ZoneMap MVCC fail-open",
            sql_block_zone_map_query(
                "bench_zone_map_mvcc",
                SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET,
            ),
            sql_block_zone_map_query(
                "bench_zone_map_mvcc",
                SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET,
                disabled_control=True,
            ),
            "zone_map_mvcc_fail_open",
            0,
            {
                **common_metadata,
                "sql_block_zone_map_mvcc_old_bucket": SQL_BLOCK_ZONE_MAP_MVCC_OLD_BUCKET,
                "sql_block_zone_map_mvcc_new_bucket": SQL_BLOCK_ZONE_MAP_MVCC_NEW_BUCKET,
            },
        ),
        (
            "ZoneMap clustered absent",
            sql_block_zone_map_query("bench_zone_map_clustered", absent_bucket),
            sql_block_zone_map_query(
                "bench_zone_map_clustered",
                absent_bucket,
                disabled_control=True,
            ),
            "zone_map_clustered_absent",
            0,
            common_metadata,
        ),
        (
            "ZoneMap clustered hit",
            sql_block_zone_map_query("bench_zone_map_clustered", target_bucket),
            sql_block_zone_map_query(
                "bench_zone_map_clustered",
                target_bucket,
                disabled_control=True,
            ),
            "zone_map_clustered_hit",
            clustered_target_rows,
            common_metadata,
        ),
        (
            "ZoneMap random control",
            sql_block_zone_map_query("bench_zone_map_random", target_bucket),
            sql_block_zone_map_query(
                "bench_zone_map_random",
                target_bucket,
                disabled_control=True,
            ),
            "zone_map_random_control",
            random_target_rows,
            common_metadata,
        ),
    ]

    for name, query, disabled_query, path, expected_rows, metadata in cases:
        if BENCH_CLAIM_MODE:
            first = bench_with_phase(name, query, "first-pass", cat=cat)
            R.append(finalize_part31_zone_map_result(first, path, expected_rows, metadata))

        result = bench(name, query, cat=cat)
        R.append(finalize_part31_zone_map_result(result, path, expected_rows, metadata))
        if BENCH_CLAIM_MODE and SQL_BLOCK_ZONE_MAP_DISABLED_CONTROL:
            disabled = bench(f"{name} [disabled-control]", disabled_query, cat=cat)
            R.append(
                finalize_part31_zone_map_result(
                    disabled,
                    path,
                    expected_rows,
                    {
                        **metadata,
                        "phase": "disabled-control",
                        "zone_map_control_role": "disabled",
                        "zone_map_control_query_shape": "executor_hint_same_predicate",
                        "zone_map_control_scope": (
                            "same table data and SQL predicate; a leading executor hint disables "
                            "SQL block zone-map pruning without changing planner predicate shape"
                        ),
                    },
                )
            )
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 26 — Reverse Frontier SSTable Activation
# ═══════════════════════════════════════════════════════════════════════════════
def part26_index_topk_frontier() -> List[BenchResult]:
    R, cat = [], "Reverse Frontier Top-K"
    limit = max(0, INDEX_TOPK_LIMIT)
    decoy_sstables = index_topk_frontier_decoy_sstables()
    rows_per_sstable = index_topk_frontier_rows_per_sstable()
    target_rows = max(rows_per_sstable, INDEX_TOPK_LIMIT * 4)
    target_host = 50
    target_base_ts = 1_000_000
    candidates_per_query = decoy_sstables + 1
    common_metadata = {
        "frontier_matrix": "index_topk_frontier",
        "reverse_frontier_required": True,
        "index_prefix_prune_required": False,
        "frontier_decoy_sstables": decoy_sstables,
        "frontier_rows_per_sstable": rows_per_sstable,
        "frontier_target_rows": target_rows,
        "frontier_target_host": target_host,
        "frontier_target_base_ts": target_base_ts,
        "index_prefix_expected_checks_per_query": candidates_per_query,
        "index_prefix_expected_positives_per_query": candidates_per_query,
        "reverse_frontier_expected_probes_per_query": candidates_per_query,
        "fusion_reverse_raw_read_limit_per_query": max(4 * limit, 96),
        "sstable_heavy_required": INDEX_TOPK_SSTABLE_CLAIM,
        "measurement_guidance": (
            "Every decoy SSTable contains the target SQL index prefix, so Bloom pruning should "
            "not hide frontier-positive candidates. SQL-generated SSTable block boundaries can "
            "straddle the query upper bound, so this matrix hard-gates frontier observability "
            "and tightens; exact activation reduction is covered by focused storage tests."
        ),
    }
    cases = [
        (
            "TopK frontier deferred DESC",
            index_topk_frontier_query(target_host, "DESC", limit, 0),
            "composite_frontier_deferred_desc",
            {
                **common_metadata,
                "expected_rows": min(limit, target_rows + decoy_sstables * rows_per_sstable),
                "reverse_frontier_expected_in_range_per_query": candidates_per_query,
                "reverse_frontier_expected_tightens_per_query": decoy_sstables,
                "reverse_frontier_expected_pending_per_query": candidates_per_query,
                "reverse_frontier_expected_deferred_unopened_per_query": 0,
            },
        ),
        (
            "TopK frontier upper-window DESC",
            index_topk_frontier_query(target_host, "DESC", limit, target_base_ts),
            "composite_frontier_upper_window_desc",
            {
                **common_metadata,
                "expected_rows": min(limit, target_rows),
                "reverse_frontier_expected_in_range_per_query": 1,
                "reverse_frontier_expected_tightens_per_query": 1,
                "reverse_frontier_expected_empty_skips_per_query": 0,
                "reverse_frontier_expected_pending_per_query": 1,
                "reverse_frontier_expected_deferred_unopened_per_query": 0,
            },
        ),
    ]

    for name, query, path, extra_metadata in cases:
        if BENCH_CLAIM_MODE:
            first = bench_with_phase(name, query, "first-pass", cat=cat)
            R.append(
                finalize_part20_case_result(
                    first, query, path, extra_metadata, "first-pass", target_rows, limit
                )
            )

        result = bench(name, query, cat=cat)
        R.append(
            finalize_part20_case_result(
                result, query, path, extra_metadata, "warm", target_rows, limit
            )
        )
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 23 — Indexed Top-K Restart Phase
# ═══════════════════════════════════════════════════════════════════════════════
def index_topk_restart_error(name: str, error: str, metadata: Dict[str, object]) -> BenchResult:
    result = BenchResult(name=name, category="Indexed Top-K Restart", planned_iters=1, warmup_iters=0)
    result.error = error
    result.metadata.update(metadata)
    return result


def start_restart_phase_server(
    binary: str,
    workdir: str,
    log_path: str,
    metrics_url: str,
    timeout_sec: int,
) -> Tuple[Optional[subprocess.Popen], Optional[Dict[str, int]], float, Optional[str]]:
    start = time.perf_counter()
    log = open(log_path, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [binary],
        cwd=workdir,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log.close()

    deadline = start + timeout_sec
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            return proc, None, 0.0, (
                f"fusiondb exited before readiness with code {proc.returncode}: "
                f"{tail_file(log_path)}"
            )
        metrics = local_metrics_snapshot(metrics_url)
        if metrics is not None:
            ready_ms = (time.perf_counter() - start) * 1000
            return proc, metrics, ready_ms, None
        time.sleep(0.05)
    return proc, None, 0.0, (
        f"fusiondb did not become ready within {timeout_sec}s: {tail_file(log_path)}"
    )


def part23_index_topk_restart_phase() -> List[BenchResult]:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    binary = INDEX_TOPK_RESTART_BINARY
    if not os.path.isabs(binary):
        binary = os.path.join(repo_root, binary)

    rows_count = index_topk_rows()
    limit = max(0, INDEX_TOPK_LIMIT)
    metadata: Dict[str, object] = {
        "matrix": "index_topk_restart",
        "binary": binary,
        "http_port": INDEX_TOPK_RESTART_PORT,
        "timeout_sec": INDEX_TOPK_RESTART_TIMEOUT_SEC,
        "index_topk_rows": rows_count,
        "limit": limit,
        "os_cache_control_mode": INDEX_TOPK_RESTART_OS_CACHE_CONTROL,
        "restart_trials_requested": INDEX_TOPK_RESTART_TRIALS,
        "restart_reset_workdir": INDEX_TOPK_RESTART_RESET_WORKDIR,
        "restart_case_policy": "restart_before_each_case_per_trial",
        "shared_data_dir_reused_across_trials": True,
        "measurement_guidance": (
            "This matrix owns a temporary FusionDB process, loads Part 20 data, checkpoints, "
            "stops the load process, then restarts a fresh process before each DESC ordered "
            "Top-K case in each trial. It clears process-local caches before every first-pass "
            "row and can optionally run an explicitly requested host drop_caches step."
        ),
    }
    cat = "Indexed Top-K Restart"

    if not os.path.exists(binary):
        return [index_topk_restart_error("TopK restart setup", f"fusiondb binary not found: {binary}", metadata)]

    cleanup_root = None
    if INDEX_TOPK_RESTART_WORKDIR:
        scenario_root = os.path.abspath(INDEX_TOPK_RESTART_WORKDIR)
        if os.path.exists(scenario_root):
            if not os.path.isdir(scenario_root):
                return [
                    index_topk_restart_error(
                        "TopK restart setup",
                        f"BENCH_INDEX_TOPK_RESTART_WORKDIR exists and is not a directory: {scenario_root}",
                        metadata,
                    )
                ]
            if os.listdir(scenario_root):
                if not INDEX_TOPK_RESTART_RESET_WORKDIR:
                    return [
                        index_topk_restart_error(
                            "TopK restart setup",
                            "BENCH_INDEX_TOPK_RESTART_WORKDIR is non-empty; set "
                            "BENCH_INDEX_TOPK_RESTART_RESET_WORKDIR=1 to allow deletion: "
                            f"{scenario_root}",
                            metadata,
                        )
                    ]
                shutil.rmtree(scenario_root)
        os.makedirs(scenario_root, exist_ok=True)
    else:
        scenario_root = tempfile.mkdtemp(prefix="fusiondb_topk_restart_")
        cleanup_root = scenario_root

    data_dir = os.path.join(scenario_root, "data")
    log_path = os.path.join(scenario_root, "fusiondb.log")
    write_startup_config(scenario_root, data_dir, INDEX_TOPK_RESTART_PORT)
    query_url = f"http://127.0.0.1:{INDEX_TOPK_RESTART_PORT}/query"
    metrics_url = f"http://127.0.0.1:{INDEX_TOPK_RESTART_PORT}/metrics"
    checkpoint_url = f"http://127.0.0.1:{INDEX_TOPK_RESTART_PORT}/checkpoint"
    previous_urls = switch_benchmark_urls(query_url, metrics_url, checkpoint_url)
    proc: Optional[subprocess.Popen] = None
    restart_proc: Optional[subprocess.Popen] = None

    try:
        metadata.update({
            "scenario_workdir": scenario_root,
            "scenario_data_dir": data_dir,
            "query_url": query_url,
            "metrics_url": metrics_url,
            "checkpoint_url": checkpoint_url,
        })
        proc, load_metrics, load_ready_ms, load_error = start_restart_phase_server(
            binary, scenario_root, log_path, metrics_url, INDEX_TOPK_RESTART_TIMEOUT_SEC
        )
        if load_error:
            return [index_topk_restart_error("TopK restart setup", load_error, metadata)]
        metadata["load_ready_ms"] = round(load_ready_ms, 3)
        metadata["load_rss_ready_kb"] = rss_kb(proc.pid) if proc else None
        metadata["load_initial_metrics"] = metric_subset(load_metrics)

        load_timings: Dict[str, float] = {}
        setup_index_topk_tables(load_timings, 0)
        if not int(load_timings.get("index_topk_sstable_claim_checkpoint_ok", 0) or 0):
            with Timer("index_topk_restart_checkpoint") as t:
                checkpoint_ok = checkpoint_storage("index_topk_restart")
            load_timings["index_topk_restart_checkpoint_ms"] = t.ms
            load_timings["index_topk_restart_checkpoint_ok"] = int(checkpoint_ok)
            if not checkpoint_ok:
                return [
                    index_topk_restart_error(
                        "TopK restart setup",
                        "checkpoint failed before restart phase",
                        {**metadata, "load_timings": load_timings},
                    )
                ]

        metadata["load_timings"] = load_timings
        metadata["data_dir_bytes_after_load"] = dir_size_bytes(data_dir)
        metadata["sstable_files_after_load"] = len(list_sstable_files(data_dir))
        metadata["index_sidecar_files_after_load"] = len(list_index_sidecars(data_dir))
        reverse_seek_sidecars_after_load = list_reverse_seek_sidecars(data_dir)
        metadata["reverse_seek_sidecar_files_after_load"] = len(reverse_seek_sidecars_after_load)
        metadata["reverse_seek_sidecar_bytes_after_load"] = sum(
            safe_file_size(path) for path in reverse_seek_sidecars_after_load
        )
        if proc is not None:
            stop_fusiondb_process(proc)
            proc = None

        rseek_ab_enabled = bool(INDEX_TOPK_RSEEK_AB and INDEX_TOPK_SSTABLE_CLAIM)
        restart_variants = [
            {
                "name": "rseek-kept",
                "label": "rseek kept",
                "workdir": scenario_root,
                "data_dir": data_dir,
                "log_path": log_path,
                "sidecar_expectation": "present",
            }
        ]
        metadata["rseek_ab_enabled"] = rseek_ab_enabled
        if rseek_ab_enabled:
            fallback_workdir = os.path.join(scenario_root, "rseek_removed")
            fallback_data_dir = os.path.join(fallback_workdir, "data")
            fallback_log_path = os.path.join(fallback_workdir, "fusiondb.log")
            if os.path.exists(fallback_workdir):
                shutil.rmtree(fallback_workdir)
            os.makedirs(fallback_workdir, exist_ok=True)
            shutil.copytree(data_dir, fallback_data_dir)
            removed_sidecars = remove_reverse_seek_sidecars(fallback_data_dir)
            write_startup_config(fallback_workdir, fallback_data_dir, INDEX_TOPK_RESTART_PORT)
            metadata["rseek_ab_fallback"] = {
                "workdir": fallback_workdir,
                "data_dir": fallback_data_dir,
                "data_dir_bytes_after_copy": dir_size_bytes(fallback_data_dir),
                "sstable_files_after_copy": len(list_sstable_files(fallback_data_dir)),
                **removed_sidecars,
            }
            restart_variants.append({
                "name": "rseek-removed",
                "label": "rseek removed",
                "workdir": fallback_workdir,
                "data_dir": fallback_data_dir,
                "log_path": fallback_log_path,
                "sidecar_expectation": "removed",
            })
        metadata["rseek_ab_variants"] = [variant["name"] for variant in restart_variants]

        results: List[BenchResult] = []
        cases = index_topk_desc_restart_cases(rows_count, limit)
        for trial_number in range(1, INDEX_TOPK_RESTART_TRIALS + 1):
            for case_order, (name, query, path, extra_metadata) in enumerate(cases, start=1):
                for variant_order, variant in enumerate(restart_variants, start=1):
                    trial_base_metadata = {
                        **metadata,
                        "trial_number": trial_number,
                        "case_order_in_trial": case_order,
                        "cases_per_trial": len(cases),
                        "rseek_ab_variant": variant["name"],
                        "rseek_ab_variant_order": variant_order,
                        "rseek_ab_variants_per_case": len(restart_variants),
                        "rseek_sidecar_expectation": variant["sidecar_expectation"],
                        "restart_trials_requested": INDEX_TOPK_RESTART_TRIALS,
                        "restart_trial_scope": (
                            "one process restart per case per trial and rseek variant; first-pass "
                            "row is measured before case-specific warmup on that restarted process"
                        ),
                        "restart_case_policy": "restart_before_each_case_per_trial",
                    }
                    os_cache_control = benchmark_os_cache_control(
                        INDEX_TOPK_RESTART_OS_CACHE_CONTROL, OS_DROP_CACHES_VALUE
                    )
                    trial_base_metadata["os_cache_control"] = os_cache_control
                    if os_cache_control.get("requested") and not os_cache_control.get("success"):
                        results.append(
                            index_topk_restart_error(
                                f"TopK restart setup [trial {trial_number} case {case_order} {variant['name']}]",
                                f"OS cache control failed: {os_cache_control.get('error', 'unknown error')}",
                                trial_base_metadata,
                            )
                        )
                        return results

                    restart_proc, restart_metrics, restart_ready_ms, restart_error = start_restart_phase_server(
                        binary,
                        str(variant["workdir"]),
                        str(variant["log_path"]),
                        metrics_url,
                        INDEX_TOPK_RESTART_TIMEOUT_SEC,
                    )
                    if restart_error:
                        results.append(
                            index_topk_restart_error(
                                f"TopK restart setup [trial {trial_number} case {case_order} {variant['name']}]",
                                restart_error,
                                trial_base_metadata,
                            )
                        )
                        return results

                    trial_name = name if INDEX_TOPK_RESTART_TRIALS == 1 else f"{name} [trial {trial_number}]"
                    if rseek_ab_enabled:
                        trial_name = f"{trial_name} [{variant['name']}]"
                    variant_data_dir = str(variant["data_dir"])
                    try:
                        reverse_seek_sidecars_after_restart = list_reverse_seek_sidecars(variant_data_dir)
                        restart_common = {
                            **trial_base_metadata,
                            "scenario_data_dir": variant_data_dir,
                            "active_data_dir": variant_data_dir,
                            "restart_ready_ms": round(restart_ready_ms, 3),
                            "restart_rss_ready_kb": rss_kb(restart_proc.pid) if restart_proc else None,
                            "restart_initial_metrics": metric_subset(restart_metrics),
                            "data_dir_bytes_after_restart": dir_size_bytes(variant_data_dir),
                            "sstable_files_after_restart": len(list_sstable_files(variant_data_dir)),
                            "reverse_seek_sidecar_files_after_restart": len(
                                reverse_seek_sidecars_after_restart
                            ),
                            "reverse_seek_sidecar_bytes_after_restart": sum(
                                safe_file_size(path)
                                for path in reverse_seek_sidecars_after_restart
                            ),
                            "log_path": (
                                str(variant["log_path"])
                                if INDEX_TOPK_RESTART_KEEP_WORKDIR or INDEX_TOPK_RESTART_WORKDIR
                                else None
                            ),
                            "sstable_heavy_setup": (
                                "checkpoint_then_process_restart_rseek_removed"
                                if variant["sidecar_expectation"] == "removed"
                                else "checkpoint_then_process_restart"
                            ),
                            "process_cache_state": "FusionDB process-local caches are new after restart",
                            "os_page_cache_state": (
                                "drop_caches executed before restart; host OS page cache control was requested"
                                if os_cache_control.get("os_page_cache_controlled")
                                else "not controlled; may be warm from load/checkpoint phase"
                            ),
                        }
                        combined_metadata = {**extra_metadata, **restart_common}
                        first = bench_with_phase(trial_name, query, "restart-first-pass", cat=cat)
                        results.append(
                            finalize_part20_case_result(
                                first, query, path, combined_metadata, "restart-first-pass", rows_count, limit
                            )
                        )
                        warm = bench(f"{trial_name} [restart-warm]", query, cat=cat)
                        results.append(
                            finalize_part20_case_result(
                                warm, query, path, combined_metadata, "restart-warm", rows_count, limit
                            )
                        )
                    finally:
                        if restart_proc is not None:
                            stop_fusiondb_process(restart_proc)
                            restart_proc = None
        return results
    finally:
        if proc is not None:
            stop_fusiondb_process(proc)
        if restart_proc is not None:
            stop_fusiondb_process(restart_proc)
        restore_benchmark_urls(previous_urls)
        if cleanup_root and not INDEX_TOPK_RESTART_KEEP_WORKDIR:
            shutil.rmtree(cleanup_root, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 21 — Indexed DISTINCT Loose Seek / GROUP BY Summary
# ═══════════════════════════════════════════════════════════════════════════════
def part21_index_distinct() -> List[BenchResult]:
    R, cat = [], "Indexed DISTINCT"
    rows = index_distinct_rows()
    ndv = index_distinct_ndv(rows)
    limit = max(0, min(INDEX_DISTINCT_LIMIT, ndv))

    def tag(phase: str, index: int) -> str:
        return f"{phase}_{index}".replace("-", "_")

    cases = [
        (
            "Distinct full scan baseline",
            "SELECT DISTINCT k FROM bench_distinct_scan",
            "full_scan_baseline",
            ndv,
        ),
        (
            "Distinct loose key seek",
            "SELECT DISTINCT k FROM bench_distinct_idx",
            "secondary_index_distinct_loose_key_seek",
            ndv,
        ),
        (
            "Distinct order limit loose key seek",
            f"SELECT DISTINCT k FROM bench_distinct_idx ORDER BY k LIMIT {limit}",
            "secondary_index_distinct_order_limit_loose_key_seek",
            limit,
        ),
        (
            "Count distinct full scan baseline",
            "SELECT COUNT(DISTINCT k) FROM bench_distinct_scan",
            "full_scan_count_distinct_baseline",
            1,
        ),
        (
            "Count distinct loose key seek",
            "SELECT COUNT(DISTINCT k) FROM bench_distinct_idx",
            "secondary_index_count_distinct_loose_key_seek",
            1,
        ),
        (
            "Distinct nullable fallback",
            "SELECT DISTINCT nullable_k FROM bench_distinct_nullable",
            "nullable_select_distinct_fallback",
            ndv + 1,
        ),
        (
            "Count distinct nullable loose key seek",
            "SELECT COUNT(DISTINCT nullable_k) FROM bench_distinct_nullable",
            "nullable_count_distinct_loose_key_seek",
            1,
        ),
        (
            "Group by count full scan baseline",
            lambda phase, i: (
                f"SELECT k AS k_{tag(phase, i)}, COUNT(*) AS c_{tag(phase, i)} "
                "FROM bench_distinct_scan GROUP BY k"
            ),
            "full_scan_group_by_count_baseline",
            ndv,
        ),
        (
            "Group by count summary index",
            lambda phase, i: (
                f"SELECT k AS k_{tag(phase, i)}, COUNT(*) AS c_{tag(phase, i)} "
                "FROM bench_distinct_idx GROUP BY k"
            ),
            "secondary_index_group_by_count_summary_index",
            ndv,
        ),
        (
            "Group by count nullable fallback",
            lambda phase, i: (
                f"SELECT nullable_k AS nullable_k_{tag(phase, i)}, "
                f"COUNT(*) AS c_{tag(phase, i)} "
                "FROM bench_distinct_nullable GROUP BY nullable_k"
            ),
            "nullable_group_by_count_fallback",
            ndv + 1,
        ),
    ]

    for name, query, path, expected_rows in cases:
        result = bench(name, query, cat=cat)
        result.metadata.update({
            "index_distinct_rows": rows,
            "distinct_ndv": ndv,
            "limit": limit,
            "path": path,
            "expected_rows": expected_rows,
            "query_text_varies_per_iteration": callable(query),
        })
        if path == "secondary_index_distinct_order_limit_loose_key_seek":
            result.metadata["loose_scan_bounded"] = False
        annotate_block_cache_metrics(result)
        R.append(result)
    return R


# ═══════════════════════════════════════════════════════════════════════════════
#  Report Rendering
# ═══════════════════════════════════════════════════════════════════════════════
COL_W = 110

def section(title, results):
    print(f"\n  ┌─ {title}")
    print(f"  │ {'Query':<28} {'Avg':>8} {'P50':>8} {'P90':>8} {'P95':>8} {'P99':>8} {'Std':>8} {'CV%':>7} {'Ops/s':>9} {'Rows':>6}")
    print(f"  │ {'─'*108}")
    for r in results:
        if r.error and not r.times_ms:
            print(f"  │ {r.name:<28} {'ERR':>8}  {r.error[:60]}  ({r.success_count}/{r.planned_iters or r.success_count} ok)")
        elif r.note:
            print(f"  │ {r.name:<28} {r.avg:>7.1f}ms {r.p50:>7.1f}ms {r.p90:>7.1f}ms {r.p95:>7.1f}ms {r.p99:>7.1f}ms {r.stddev_ms:>7.1f}ms {r.cv_pct:>6.1f}%")
            print(f"  │   └─ {r.note}")
            if r.error:
                print(f"  │      sample error: {r.error[:80]}")
        elif r.error:
            print(f"  │ {r.name:<28} {r.avg:>7.1f}ms {r.p50:>7.1f}ms {r.p90:>7.1f}ms {r.p95:>7.1f}ms {r.p99:>7.1f}ms {r.stddev_ms:>7.1f}ms {r.cv_pct:>6.1f}% {r.ops_sec:>8.0f} {r.row_count:>6}")
            print(f"  │   └─ partial: {r.success_count}/{r.planned_iters or r.success_count} ok, error: {r.error[:80]}")
        else:
            print(f"  │ {r.name:<28} {r.avg:>7.1f}ms {r.p50:>7.1f}ms {r.p90:>7.1f}ms {r.p95:>7.1f}ms {r.p99:>7.1f}ms {r.stddev_ms:>7.1f}ms {r.cv_pct:>6.1f}% {r.ops_sec:>8.0f} {r.row_count:>6}")
    print(f"  └{'─'*COL_W}")


MATRIX_PARTS = {
    "full": ("all",),
    "all": ("all",),
    "join_ndv": ("join_ndv",),
    "selectivity": ("base",),
    "topk": ("base", "inventory"),
    "columnar_single_source": ("column_scan",),
    "index_topk": ("index_topk",),
    "index_topk_prefix_prune": ("index_topk_prefix_prune",),
    "sql_block_index_prefix_prune": ("sql_block_index_prefix_prune",),
    "sql_block_zone_map_prune": ("sql_block_zone_map_prune",),
    "index_topk_frontier": ("index_topk_frontier",),
    "sstable_reverse_frontier": ("sstable_reverse_frontier",),
    "fusion_reverse_frontier": ("fusion_reverse_frontier",),
    "index_topk_rseek_ab": ("index_topk_restart",),
    "index_distinct": ("index_distinct",),
    "groupby": ("base", "analytics", "column_scan"),
    "analyze": ("join_ndv",),
    "planner": ("stress", "join_ndv"),
    "wide_scan": ("wide_scan",),
    "scan": ("base", "wide_scan"),
    "or_in_scan": ("or_in_scan",),
    "between_scan": ("between_scan",),
    "like_prefix_scan": ("like_prefix_scan",),
    "sstable_range_bound": ("sstable_range_bound",),
    "sstable_prefix_bloom": ("sstable_prefix_bloom",),
    "sstable_block_prefix": ("sstable_block_prefix",),
    "sstable_block_index_prefix": ("sstable_block_index_prefix",),
    "sstable_user_key_bloom": ("sstable_user_key_bloom",),
    "sstable_no_fill_cache": ("sstable_no_fill_cache",),
    "sql_no_fill_cache": ("sql_no_fill_cache",),
    "sstable_startup_index": ("sstable_startup_index",),
    "index_topk_restart": ("index_topk_restart",),
}


def slugify_selection(value: str) -> str:
    clean = []
    for ch in value.lower():
        if ch.isalnum():
            clean.append(ch)
        elif clean and clean[-1] != "_":
            clean.append("_")
    slug = "".join(clean).strip("_")
    return slug or "selection"


def part_specs() -> List[PartSpec]:
    return [
        PartSpec(1, "base", "Part 1 — Base Benchmarks", part1_base, ("selectivity", "topk")),
        PartSpec(2, "ecommerce", "Part 2 — E-commerce Simulation", part2_ecommerce),
        PartSpec(3, "financial", "Part 3 — Financial Ledger", part3_financial),
        PartSpec(4, "analytics", "Part 4 — Analytics / OLAP", part4_analytics, ("groupby",)),
        PartSpec(5, "concurrent", "Part 5 — Concurrent Mixed Workload", part5_concurrent),
        PartSpec(6, "stress", "Part 6 — Stress & Edge Cases", part6_stress, ("planner",)),
        PartSpec(7, "inventory", "Part 7 — Inventory & Fulfillment", part7_inventory_fulfillment, ("topk",)),
        PartSpec(8, "risk", "Part 8 — Risk & Audit", part8_risk_audit),
        PartSpec(9, "column_scan", "Part 9 — Column-Scan Fast Paths", part9_column_scan_fast_paths, ("groupby",)),
        PartSpec(10, "join_ndv", "Part 10 — Stats-Aware Join Reorder", part10_stats_aware_join_reorder, ("analyze", "planner")),
        PartSpec(11, "wide_scan", "Part 11 — Wide-Row Predicate-First Scan", part11_wide_row_scan, ("scan", "late_materialization"), default=False),
        PartSpec(12, "or_in_scan", "Part 12 — OR-to-IN Predicate-First Scan", part12_or_in_scan, ("scan", "predicate_first"), default=False),
        PartSpec(13, "between_scan", "Part 13 — BETWEEN Predicate-First Scan", part13_between_scan, ("scan", "predicate_first"), default=False),
        PartSpec(14, "like_prefix_scan", "Part 14 — LIKE Pattern Predicate-First Scan", part14_like_prefix_scan, ("scan", "predicate_first"), default=False),
        PartSpec(15, "sstable_range_bound", "Part 15 — SSTable Range Upper Bound", part15_sstable_range_bound, ("scan", "storage", "sstable"), default=False),
        PartSpec(16, "sstable_prefix_bloom", "Part 16 — SSTable Prefix Bloom Filter", part16_sstable_prefix_bloom, ("scan", "storage", "sstable"), default=False),
        PartSpec(17, "sstable_block_prefix", "Part 17 — SSTable Block Prefix Property Filter", part17_sstable_block_prefix, ("scan", "storage", "sstable"), default=False),
        PartSpec(18, "sstable_user_key_bloom", "Part 18 — SSTable MVCC User-Key Bloom Filter", part18_sstable_user_key_bloom, ("point", "storage", "sstable"), default=False),
        PartSpec(19, "sstable_no_fill_cache", "Part 19 — SSTable No-Fill Block Cache Policy", part19_sstable_no_fill_cache, ("scan", "storage", "sstable", "cache"), default=False),
        PartSpec(20, "index_topk", "Part 20 — Indexed ORDER BY Top-K", part20_index_topk, ("topk", "index", "planner"), default=False),
        PartSpec(21, "index_distinct", "Part 21 — Indexed DISTINCT Loose Seek / GROUP BY Summary", part21_index_distinct, ("distinct", "index", "planner"), default=False),
        PartSpec(22, "sstable_startup_index", "Part 22 — SSTable Startup Index Cache", part22_sstable_startup_index, ("startup", "storage", "sstable", "index"), default=False),
        PartSpec(23, "index_topk_restart", "Part 23 — Indexed Top-K Restart Phase", part23_index_topk_restart_phase, ("topk", "index", "restart", "sstable"), default=False),
        PartSpec(24, "index_topk_prefix_prune", "Part 24 — SQL Index-Prefix SSTable Pruning", part24_index_topk_prefix_prune, ("topk", "index", "sstable", "prefix"), default=False),
        PartSpec(25, "sql_no_fill_cache", "Part 25 — SQL No-Fill Block Cache Policy", part25_sql_no_fill_cache, ("scan", "sql", "cache"), default=False),
        PartSpec(26, "index_topk_frontier", "Part 26 — Reverse Frontier SSTable Activation", part26_index_topk_frontier, ("topk", "index", "sstable", "frontier"), default=False),
        PartSpec(27, "sstable_reverse_frontier", "Part 27 — SSTable Reverse Frontier Activation", part27_sstable_reverse_frontier, ("topk", "storage", "sstable", "frontier"), default=False),
        PartSpec(28, "fusion_reverse_frontier", "Part 28 — Fusion Reverse Frontier Public API", part28_fusion_reverse_frontier, ("topk", "storage", "fusion", "frontier"), default=False),
        PartSpec(29, "sstable_block_index_prefix", "Part 29 — SSTable Block SQL Index-Prefix Property Filter", part29_sstable_block_index_prefix, ("scan", "storage", "sstable", "prefix"), default=False),
        PartSpec(30, "sql_block_index_prefix_prune", "Part 30 — SQL Block Index-Prefix SSTable Pruning", part30_sql_block_index_prefix_prune, ("topk", "index", "sstable", "prefix"), default=False),
        PartSpec(31, "sql_block_zone_map_prune", "Part 31 — SQL Block Zone-Map SSTable Pruning", part31_sql_block_zone_map_prune, ("scan", "storage", "sstable", "zone-map"), default=False),
    ]


def select_part_specs(parts: List[PartSpec]) -> Tuple[List[PartSpec], Dict[str, object]]:
    by_id = {part.id: part for part in parts}
    by_key = {part.key: part for part in parts}

    def add_token(token: str, selected: Dict[int, PartSpec]):
        token = token.strip().lower()
        if not token:
            return
        if token in ("all", "full"):
            for part in parts:
                if part.default:
                    selected[part.id] = part
            return
        if "-" in token:
            left, right = token.split("-", 1)
            if left.isdigit() and right.isdigit():
                start, end = sorted((int(left), int(right)))
                for part_id in range(start, end + 1):
                    if part_id in by_id:
                        selected[part_id] = by_id[part_id]
                return
        if token.isdigit():
            part = by_id.get(int(token))
            if part:
                selected[part.id] = part
                return
            raise SystemExit(f"Unknown BENCH_PARTS id: {token}")
        if token in by_key:
            part = by_key[token]
            selected[part.id] = part
            return
        matched = [part for part in parts if token in part.tags]
        if matched:
            for part in matched:
                selected[part.id] = part
            return
        raise SystemExit(f"Unknown BENCH_PARTS key/tag: {token}")

    selected_by_id: Dict[int, PartSpec] = {}
    raw_parts = BENCH_PARTS
    raw_matrix = BENCH_MATRIX or "full"
    if raw_parts:
        for token in raw_parts.split(","):
            add_token(token, selected_by_id)
        source = "parts"
        slug = f"parts_{slugify_selection(raw_parts)}"
    else:
        matrix_tokens = MATRIX_PARTS.get(raw_matrix)
        if matrix_tokens is None:
            raise SystemExit(
                f"Unknown BENCH_MATRIX={raw_matrix}. Choose: {', '.join(sorted(MATRIX_PARTS))}"
            )
        for token in matrix_tokens:
            add_token(token, selected_by_id)
        source = "matrix"
        slug = "full" if raw_matrix in ("all", "full") else f"matrix_{slugify_selection(raw_matrix)}"

    selected = [part for part in parts if part.id in selected_by_id]
    if not selected:
        raise SystemExit("No benchmark parts selected")

    default_part_count = sum(1 for part in parts if part.default)
    is_default_full = (
        not raw_parts
        and raw_matrix in ("all", "full")
        and len(selected) == default_part_count
    )
    selection = {
        "source": source,
        "matrix": raw_matrix,
        "parts_env": raw_parts,
        "selected_parts": [
            {"id": part.id, "key": part.key, "title": part.title} for part in selected
        ],
        "is_default_full": is_default_full,
        "slug": slug,
    }
    return selected, selection


def percentile_value(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    return ordered[min(int(len(ordered) * percentile), len(ordered) - 1)]


def restart_trial_base_name(name: str) -> str:
    base = name
    for suffix in (" [restart-first-pass]", " [restart-warm]"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
    marker = " [trial "
    if marker in base:
        start = base.find(marker)
        end = base.find("]", start)
        if end >= 0:
            base = base[:start] + base[end + 1:]
        else:
            base = base[:start]
    return base


def rseek_ab_pair_case_name(name: str) -> str:
    base = restart_trial_base_name(name)
    for variant in (" [rseek-kept]", " [rseek-removed]"):
        if base.endswith(variant):
            base = base[: -len(variant)]
            break
    return base


def rseek_ab_pair_groups(
    all_results: List[BenchResult],
) -> Dict[Tuple[int, int, str, str], Dict[str, BenchResult]]:
    groups: Dict[Tuple[int, int, str, str], Dict[str, BenchResult]] = {}
    for result in all_results:
        metadata = result.metadata
        if metadata.get("matrix") != "index_topk_restart":
            continue
        if not metadata.get("rseek_ab_enabled"):
            continue
        variant = metadata.get("rseek_ab_variant")
        if variant not in ("rseek-kept", "rseek-removed"):
            continue
        trial_number = int(metadata.get("trial_number", 0) or 0)
        case_order = int(metadata.get("case_order_in_trial", 0) or 0)
        key = (
            trial_number,
            case_order,
            str(metadata.get("path", "")),
            str(metadata.get("phase", "")),
        )
        groups.setdefault(key, {})[str(variant)] = result
    return groups


def rseek_ab_metric_diff(
    left: BenchResult,
    right: BenchResult,
    metric: str,
) -> int:
    return int(left.metrics_delta.get(metric, 0) or 0) - int(right.metrics_delta.get(metric, 0) or 0)


def rseek_ab_result_equivalent(kept: BenchResult, removed: BenchResult) -> Tuple[bool, List[str]]:
    failures: List[str] = []
    if kept.row_count != removed.row_count:
        failures.append(
            f"row_count mismatch kept={kept.row_count} removed={removed.row_count}"
        )
    if not kept.result_checksums:
        failures.append("kept result checksum sequence missing")
    if not removed.result_checksums:
        failures.append("removed result checksum sequence missing")
    if (
        kept.result_checksums
        and removed.result_checksums
        and kept.result_checksums != removed.result_checksums
    ):
        failures.append(
            "result checksum sequence mismatch "
            f"kept={kept.result_checksums} removed={removed.result_checksums}"
        )
    return (not failures, failures)


def apply_rseek_ab_pair_claims(all_results: List[BenchResult]) -> None:
    if not BENCH_CLAIM_MODE:
        return
    for key, variants in rseek_ab_pair_groups(all_results).items():
        kept = variants.get("rseek-kept")
        removed = variants.get("rseek-removed")
        missing = [
            variant for variant in ("rseek-kept", "rseek-removed") if variant not in variants
        ]
        if missing:
            failures = [f"missing rseek A/B variant(s): {', '.join(missing)}"]
            for result in variants.values():
                result.metadata["rseek_ab_pair_claim_status"] = "failed"
                result.metadata["rseek_ab_pair_failures"] = failures
                if not result.error:
                    result.error = "rseek A/B pair equivalence failed: " + "; ".join(failures)
            continue
        if kept is None or removed is None:
            continue
        equivalent, failures = rseek_ab_result_equivalent(kept, removed)
        pair_key = {
            "trial_number": key[0],
            "case_order_in_trial": key[1],
            "path": key[2],
            "phase": key[3],
        }
        for result in (kept, removed):
            result.metadata["rseek_ab_pair_key"] = pair_key
            result.metadata["rseek_ab_pair_claim_status"] = (
                "passed" if equivalent else "failed"
            )
            if failures:
                result.metadata["rseek_ab_pair_failures"] = failures
                if not result.error:
                    result.error = (
                        "rseek A/B pair equivalence failed: " + "; ".join(failures)
                    )


def part31_zone_map_control_groups(
    all_results: List[BenchResult],
) -> Dict[str, Dict[str, BenchResult]]:
    groups: Dict[str, Dict[str, BenchResult]] = {}
    for result in all_results:
        metadata = result.metadata
        if metadata.get("matrix") != "sql_block_zone_map_prune":
            continue
        if not metadata.get("sql_block_zone_map_disabled_control_enabled"):
            continue
        role = metadata.get("zone_map_control_role")
        if role not in ("enabled", "disabled"):
            continue
        phase = str(metadata.get("phase", "warm"))
        if role == "enabled" and phase != "warm":
            continue
        if role == "disabled" and phase != "disabled-control":
            continue
        key = str(metadata.get("zone_map_control_pair_key") or metadata.get("path", ""))
        if not key:
            continue
        groups.setdefault(key, {})[str(role)] = result
    return groups


def part31_zone_map_block_read_requests(result: BenchResult) -> int:
    return _metric_count(result, "block_cache_hit_count") + _metric_count(
        result, "block_cache_miss_count"
    )


def part31_zone_map_control_pair_metrics(
    enabled: BenchResult,
    disabled: BenchResult,
) -> Dict[str, int]:
    return {
        "zone_map_checks_enabled_minus_disabled": rseek_ab_metric_diff(
            enabled, disabled, "sstable_block_zone_map_filter_check_count"
        ),
        "zone_map_skips_enabled_minus_disabled": rseek_ab_metric_diff(
            enabled, disabled, "sstable_block_zone_map_filter_skip_count"
        ),
        "zone_map_fail_opens_enabled_minus_disabled": rseek_ab_metric_diff(
            enabled, disabled, "sstable_block_zone_map_filter_fail_open_count"
        ),
        "block_read_requests_disabled_minus_enabled": (
            part31_zone_map_block_read_requests(disabled)
            - part31_zone_map_block_read_requests(enabled)
        ),
        "sstable_block_file_opens_disabled_minus_enabled": rseek_ab_metric_diff(
            disabled, enabled, "sstable_block_file_open_count"
        ),
        "sstable_block_read_bytes_disabled_minus_enabled": rseek_ab_metric_diff(
            disabled, enabled, "sstable_block_read_bytes"
        ),
        "row_reads_disabled_minus_enabled": rseek_ab_metric_diff(
            disabled, enabled, "row_read_count"
        ),
    }


def part31_zone_map_control_pair_failures(
    path: str,
    enabled: BenchResult,
    disabled: BenchResult,
) -> Tuple[bool, List[str]]:
    failures: List[str] = []
    if enabled.row_count != disabled.row_count:
        failures.append(
            f"row_count mismatch enabled={enabled.row_count} disabled={disabled.row_count}"
        )
    if not enabled.result_checksums:
        failures.append("enabled result checksum sequence missing")
    if not disabled.result_checksums:
        failures.append("disabled result checksum sequence missing")
    if (
        enabled.result_checksums
        and disabled.result_checksums
        and enabled.result_checksums != disabled.result_checksums
    ):
        failures.append(
            "result checksum sequence mismatch "
            f"enabled={enabled.result_checksums} disabled={disabled.result_checksums}"
        )
    enabled_checks = _metric_count(enabled, "sstable_block_zone_map_filter_check_count")
    disabled_checks = _metric_count(disabled, "sstable_block_zone_map_filter_check_count")
    disabled_skips = _metric_count(disabled, "sstable_block_zone_map_filter_skip_count")
    disabled_fail_opens = _metric_count(
        disabled, "sstable_block_zone_map_filter_fail_open_count"
    )
    if enabled_checks <= 0:
        failures.append("expected enabled query to observe zone-map checks")
    if disabled_checks != 0 or disabled_skips != 0 or disabled_fail_opens != 0:
        failures.append(
            "expected disabled-control query to avoid zone-map work, got "
            f"checks={disabled_checks}, skips={disabled_skips}, fail_opens={disabled_fail_opens}"
        )
    if path in ("zone_map_clustered_absent", "zone_map_clustered_hit"):
        enabled_requests = part31_zone_map_block_read_requests(enabled)
        disabled_requests = part31_zone_map_block_read_requests(disabled)
        if disabled_requests <= enabled_requests:
            failures.append(
                "expected clustered enabled query to issue fewer block read requests than "
                f"disabled-control, got enabled={enabled_requests}, disabled={disabled_requests}"
            )
    return (not failures, failures)


def apply_part31_zone_map_control_pair_claims(all_results: List[BenchResult]) -> None:
    if not BENCH_CLAIM_MODE:
        return
    for path, variants in part31_zone_map_control_groups(all_results).items():
        enabled = variants.get("enabled")
        disabled = variants.get("disabled")
        missing = [
            variant for variant in ("enabled", "disabled") if variant not in variants
        ]
        if missing:
            failures = [f"missing Part 31 zone-map control variant(s): {', '.join(missing)}"]
            for result in variants.values():
                result.metadata["zone_map_control_pair_claim_status"] = "failed"
                result.metadata["zone_map_control_pair_failures"] = failures
                if not result.error:
                    result.error = (
                        "Part 31 zone-map control pair failed: " + "; ".join(failures)
                    )
            continue
        if enabled is None or disabled is None:
            continue
        equivalent, failures = part31_zone_map_control_pair_failures(
            path, enabled, disabled
        )
        pair_metrics = part31_zone_map_control_pair_metrics(enabled, disabled)
        for result in (enabled, disabled):
            result.metadata["zone_map_control_pair_claim_status"] = (
                "passed" if equivalent else "failed"
            )
            result.metadata["zone_map_control_pair_metrics"] = pair_metrics
            if failures:
                result.metadata["zone_map_control_pair_failures"] = failures
                if not result.error:
                    result.error = (
                        "Part 31 zone-map control pair failed: " + "; ".join(failures)
                    )


def benchmark_rseek_ab_paired_summary(
    all_results: List[BenchResult],
) -> Dict[str, object]:
    groups = rseek_ab_pair_groups(all_results)
    if not groups:
        return {}

    pairs: List[Dict[str, object]] = []
    incomplete_pairs: List[Dict[str, object]] = []
    latency_ratios: List[float] = []
    checksum_matches = 0
    checksum_mismatches = 0
    row_count_matches = 0
    row_count_mismatches = 0
    metric_delta_totals: Dict[str, int] = {}

    for key, variants in sorted(groups.items()):
        kept = variants.get("rseek-kept")
        removed = variants.get("rseek-removed")
        if kept is None or removed is None:
            incomplete_pairs.append({
                "trial_number": key[0],
                "case_order_in_trial": key[1],
                "path": key[2],
                "phase": key[3],
                "present_variants": sorted(variants),
            })
            continue

        equivalent, failures = rseek_ab_result_equivalent(kept, removed)
        checksum_match = (
            bool(kept.result_checksums)
            and kept.result_checksums == removed.result_checksums
        )
        row_count_match = kept.row_count == removed.row_count
        if checksum_match:
            checksum_matches += 1
        else:
            checksum_mismatches += 1
        if row_count_match:
            row_count_matches += 1
        else:
            row_count_mismatches += 1

        latency_ratio = None
        if kept.avg > 0 and removed.avg > 0:
            latency_ratio = round(removed.avg / kept.avg, 6)
            latency_ratios.append(removed.avg / kept.avg)

        metric_deltas = {
            "runtime_span_scans_removed_minus_kept": rseek_ab_metric_diff(
                removed, kept, "sstable_reverse_block_span_scan_count"
            ),
            "runtime_span_scan_entries_removed_minus_kept": rseek_ab_metric_diff(
                removed, kept, "sstable_reverse_block_span_scan_entry_count"
            ),
            "runtime_span_materializes_removed_minus_kept": rseek_ab_metric_diff(
                removed, kept, "sstable_reverse_block_span_materialize_entry_count"
            ),
            "sidecar_uses_kept_minus_removed": rseek_ab_metric_diff(
                kept, removed, "sstable_reverse_seek_sidecar_use_count"
            ),
            "sidecar_hits_kept_minus_removed": rseek_ab_metric_diff(
                kept, removed, "sstable_reverse_seek_sidecar_hit_count"
            ),
            "sidecar_misses_removed_minus_kept": rseek_ab_metric_diff(
                removed, kept, "sstable_reverse_seek_sidecar_miss_count"
            ),
            "sidecar_index_entries_kept_minus_removed": rseek_ab_metric_diff(
                kept, removed, "sstable_reverse_seek_sidecar_index_entry_count"
            ),
            "sidecar_materializes_kept_minus_removed": rseek_ab_metric_diff(
                kept, removed, "sstable_reverse_seek_sidecar_entry_materialize_count"
            ),
            "sidecar_offset_probes_kept_minus_removed": rseek_ab_metric_diff(
                kept, removed, "sstable_reverse_seek_sidecar_offset_probe_count"
            ),
        }
        for name, value in metric_deltas.items():
            metric_delta_totals[name] = metric_delta_totals.get(name, 0) + value

        pairs.append({
            "trial_number": key[0],
            "case_order_in_trial": key[1],
            "case_name": rseek_ab_pair_case_name(kept.name),
            "path": key[2],
            "phase": key[3],
            "kept_ms": round(kept.avg, 3),
            "removed_ms": round(removed.avg, 3),
            "latency_delta_ms_removed_minus_kept": round(removed.avg - kept.avg, 3),
            "latency_ratio_removed_over_kept": latency_ratio,
            "row_count_match": row_count_match,
            "kept_row_count": kept.row_count,
            "removed_row_count": removed.row_count,
            "result_checksum_match": checksum_match,
            "kept_result_checksum": kept.result_checksum,
            "removed_result_checksum": removed.result_checksum,
            "kept_result_checksums": kept.result_checksums,
            "removed_result_checksums": removed.result_checksums,
            "result_equivalent": equivalent,
            "result_equivalence_failures": failures,
            "kept_claim_status": kept.metadata.get("claim_status"),
            "removed_claim_status": removed.metadata.get("claim_status"),
            "metric_deltas": metric_deltas,
        })

    return {
        "schema_version": 1,
        "summary_scope": (
            "pairs rseek-kept and rseek-removed rows by trial, case, path, and phase; "
            "latency ratios are benchmark-smoke evidence, while checksum and counter "
            "deltas are the primary A/B correctness and path evidence"
        ),
        "complete_pair_count": len(pairs),
        "incomplete_pair_count": len(incomplete_pairs),
        "checksum_match_count": checksum_matches,
        "checksum_mismatch_count": checksum_mismatches,
        "row_count_match_count": row_count_matches,
        "row_count_mismatch_count": row_count_mismatches,
        "all_result_checksums_match": bool(pairs) and checksum_mismatches == 0,
        "all_row_counts_match": bool(pairs) and row_count_mismatches == 0,
        "latency_ratio_removed_over_kept_avg": (
            round(statistics.mean(latency_ratios), 6) if latency_ratios else None
        ),
        "latency_ratio_removed_over_kept_p50": (
            round(statistics.median(latency_ratios), 6) if latency_ratios else None
        ),
        "metric_delta_totals": metric_delta_totals,
        "incomplete_pairs": incomplete_pairs,
        "pairs": pairs,
    }


def benchmark_part31_zone_map_control_summary(
    all_results: List[BenchResult],
) -> Dict[str, object]:
    groups = part31_zone_map_control_groups(all_results)
    if not groups:
        return {}

    pairs: List[Dict[str, object]] = []
    incomplete_pairs: List[Dict[str, object]] = []
    checksum_matches = 0
    checksum_mismatches = 0
    row_count_matches = 0
    row_count_mismatches = 0
    clustered_block_request_delta_positive = 0
    metric_delta_totals: Dict[str, int] = {}

    for path, variants in sorted(groups.items()):
        enabled = variants.get("enabled")
        disabled = variants.get("disabled")
        if enabled is None or disabled is None:
            incomplete_pairs.append({
                "path": path,
                "present_variants": sorted(variants),
            })
            continue

        equivalent, failures = part31_zone_map_control_pair_failures(
            path, enabled, disabled
        )
        pair_metrics = part31_zone_map_control_pair_metrics(enabled, disabled)
        for name, value in pair_metrics.items():
            metric_delta_totals[name] = metric_delta_totals.get(name, 0) + value

        checksum_match = (
            bool(enabled.result_checksums)
            and enabled.result_checksums == disabled.result_checksums
        )
        row_count_match = enabled.row_count == disabled.row_count
        if checksum_match:
            checksum_matches += 1
        else:
            checksum_mismatches += 1
        if row_count_match:
            row_count_matches += 1
        else:
            row_count_mismatches += 1
        if (
            path in ("zone_map_clustered_absent", "zone_map_clustered_hit")
            and pair_metrics["block_read_requests_disabled_minus_enabled"] > 0
        ):
            clustered_block_request_delta_positive += 1

        latency_ratio = None
        if enabled.avg > 0 and disabled.avg > 0:
            latency_ratio = round(disabled.avg / enabled.avg, 6)

        pairs.append({
            "path": path,
            "enabled_ms": round(enabled.avg, 3),
            "disabled_ms": round(disabled.avg, 3),
            "latency_delta_ms_disabled_minus_enabled": round(
                disabled.avg - enabled.avg, 3
            ),
            "latency_ratio_disabled_over_enabled": latency_ratio,
            "row_count_match": row_count_match,
            "enabled_row_count": enabled.row_count,
            "disabled_row_count": disabled.row_count,
            "result_checksum_match": checksum_match,
            "enabled_result_checksum": enabled.result_checksum,
            "disabled_result_checksum": disabled.result_checksum,
            "enabled_result_checksums": enabled.result_checksums,
            "disabled_result_checksums": disabled.result_checksums,
            "result_equivalent": equivalent,
            "result_equivalence_failures": failures,
            "enabled_claim_status": enabled.metadata.get("claim_status"),
            "disabled_claim_status": disabled.metadata.get("claim_status"),
            "pair_claim_status": enabled.metadata.get(
                "zone_map_control_pair_claim_status"
            ),
            "metric_deltas": pair_metrics,
        })

    return {
        "schema_version": 1,
        "summary_scope": (
            "pairs Part 31 warm enabled rows with disabled-control rows by path; "
            "checksum and row-count equivalence plus zone-map/block-read counters are "
            "the primary evidence, while latency and read-byte deltas are smoke evidence"
        ),
        "read_byte_delta_scope": (
            "sstable_block_read_bytes is reported as a delta but is not a hard gate because "
            "application and OS cache state can turn extra block work into cache hits"
        ),
        "complete_pair_count": len(pairs),
        "incomplete_pair_count": len(incomplete_pairs),
        "checksum_match_count": checksum_matches,
        "checksum_mismatch_count": checksum_mismatches,
        "row_count_match_count": row_count_matches,
        "row_count_mismatch_count": row_count_mismatches,
        "clustered_block_request_delta_positive_count": (
            clustered_block_request_delta_positive
        ),
        "all_result_checksums_match": bool(pairs) and checksum_mismatches == 0,
        "all_row_counts_match": bool(pairs) and row_count_mismatches == 0,
        "metric_delta_totals": metric_delta_totals,
        "incomplete_pairs": incomplete_pairs,
        "pairs": pairs,
    }


def benchmark_trial_summaries(all_results: List[BenchResult]) -> Dict[str, object]:
    groups: Dict[Tuple[str, str, str, str], List[BenchResult]] = {}
    for result in all_results:
        metadata = result.metadata
        if metadata.get("matrix") != "index_topk_restart":
            continue
        if "trial_number" not in metadata:
            continue
        key = (
            restart_trial_base_name(result.name),
            str(metadata.get("path", "")),
            str(metadata.get("phase", "")),
            str(metadata.get("cache_phase", "")),
        )
        groups.setdefault(key, []).append(result)

    if not groups:
        summaries_by_matrix: Dict[str, object] = {}
        rseek_ab_summary = benchmark_rseek_ab_paired_summary(all_results)
        if rseek_ab_summary:
            summaries_by_matrix["index_topk_rseek_ab"] = rseek_ab_summary
        part31_summary = benchmark_part31_zone_map_control_summary(all_results)
        if part31_summary:
            summaries_by_matrix["sql_block_zone_map_prune"] = part31_summary
        return summaries_by_matrix

    summaries = []
    for (case_name, path, phase, cache_phase), results in sorted(groups.items()):
        samples: List[float] = []
        metric_totals: Dict[str, int] = {}
        trial_numbers = []
        claim_statuses = set()
        errors = 0
        row_counts = []
        for result in results:
            samples.extend(result.times_ms)
            trial_number = result.metadata.get("trial_number")
            if isinstance(trial_number, int):
                trial_numbers.append(trial_number)
            claim_status = result.metadata.get("claim_status")
            if isinstance(claim_status, str):
                claim_statuses.add(claim_status)
            errors += result.error_count
            row_counts.append(result.row_count)
            for metric, value in result.metrics_delta.items():
                metric_totals[metric] = metric_totals.get(metric, 0) + int(value)

        query_count = metric_totals.get("query_count") or len(samples) or 1
        metric_per_query = {
            metric: round(value / query_count, 6)
            for metric, value in sorted(metric_totals.items())
            if metric in METRIC_COUNTER_KEYS
        }
        sample_quality = {
            "sample_count": len(samples),
            "p95_tail_claim_supported": len(samples) >= 20,
            "p99_tail_claim_supported": len(samples) >= 100,
            "latency_claim_scope": (
                "aggregate latency summary only; p95/p99 are smoke indicators until sample count "
                "is high enough for tail-latency claims"
            ),
        }
        if len(samples) < 20:
            sample_quality["warning"] = (
                "fewer than 20 samples; p95 and p99 values should not be used as tail-latency claims"
            )
        elif len(samples) < 100:
            sample_quality["warning"] = (
                "fewer than 100 samples; p99 value should not be used as a tail-latency claim"
            )
        summaries.append({
            "case_name": case_name,
            "path": path,
            "phase": phase,
            "cache_phase": cache_phase,
            "trial_count": len(set(trial_numbers)),
            "trial_numbers": sorted(set(trial_numbers)),
            "sample_count": len(samples),
            "error_count": errors,
            "claim_statuses": sorted(claim_statuses),
            "sample_quality": sample_quality,
            "avg_ms": round(statistics.mean(samples), 3) if samples else 0,
            "p50_ms": round(statistics.median(samples), 3) if samples else 0,
            "p90_ms": round(percentile_value(samples, 0.90), 3),
            "p95_ms": round(percentile_value(samples, 0.95), 3),
            "p99_ms": round(percentile_value(samples, 0.99), 3),
            "min_ms": round(min(samples), 3) if samples else 0,
            "max_ms": round(max(samples), 3) if samples else 0,
            "stddev_ms": round(statistics.pstdev(samples), 3) if len(samples) > 1 else 0,
            "cv_pct": (
                round(statistics.pstdev(samples) / statistics.mean(samples) * 100, 3)
                if len(samples) > 1 and statistics.mean(samples) > 0
                else 0
            ),
            "row_count_values": sorted(set(row_counts)),
            "metric_totals": metric_totals,
            "metric_per_query": metric_per_query,
        })

    summaries_by_matrix = {
        "index_topk_restart": {
            "schema_version": 1,
            "summary_scope": (
                "aggregates benchmark rows across process restart trials without changing "
                "the raw per-trial benchmark rows"
            ),
            "group_count": len(summaries),
            "groups": summaries,
        }
    }
    rseek_ab_summary = benchmark_rseek_ab_paired_summary(all_results)
    if rseek_ab_summary:
        summaries_by_matrix["index_topk_rseek_ab"] = rseek_ab_summary
    part31_summary = benchmark_part31_zone_map_control_summary(all_results)
    if part31_summary:
        summaries_by_matrix["sql_block_zone_map_prune"] = part31_summary
    return summaries_by_matrix


def save_report(timings, all_results, selection):
    report = {
        "timestamp": datetime.now().isoformat(),
        "scale": SCALE,
        "protocol": PROTO,
        "seed": SEED,
        "claim_mode": BENCH_CLAIM_MODE,
        "config": C,
        "selection": selection,
        "disclosure": benchmark_environment_disclosure(selection, timings, all_results),
        "load": timings,
        "trial_summaries": benchmark_trial_summaries(all_results),
        "benchmarks": []
    }
    for r in all_results:
        report["benchmarks"].append({
            "name": r.name, "category": r.category,
            "part_id": r.part_id, "part_key": r.part_key, "part_title": r.part_title,
            "avg_ms": round(r.avg,3), "p50_ms": round(r.p50,3),
            "p90_ms": round(r.p90,3),
            "p95_ms": round(r.p95,3), "p99_ms": round(r.p99,3),
            "min_ms": round(r.min_ms,3), "max_ms": round(r.max_ms,3),
            "stddev_ms": round(r.stddev_ms,3),
            "cv_pct": round(r.cv_pct,3),
            "mad_ms": round(r.mad_ms,3),
            "ops_per_sec": round(r.ops_sec,1),
            "rows_per_sec": round(r.rows_sec,1),
            "row_count": r.row_count,
            "iters": len(r.times_ms),
            "planned_iters": r.planned_iters,
            "warmup_iters": r.warmup_iters,
            "success_count": r.success_count,
            "error_count": r.error_count,
            "result_checksum": r.result_checksum,
            "result_checksum_algorithm": (
                RESULT_CHECKSUM_ALGORITHM if r.result_checksum else None
            ),
            "result_checksum_count": len(r.result_checksums),
            "result_checksum_distinct_count": len(set(r.result_checksums)),
            "result_checksum_consistent": (
                len(set(r.result_checksums)) <= 1 if r.result_checksums else None
            ),
            "total_ops": r.total_ops,
            "wall_ms": round(r.wall_ms,3),
            "throughput_ops_sec": round(r.throughput_ops_sec,1),
            "attempted_ops_sec": round(r.attempted_ops_sec,1),
            "successful_ops_sec": round(r.successful_ops_sec,1),
            "error_classes": r.grouped_error_classes(),
            "metadata": r.metadata,
            "metrics_delta": r.metrics_delta,
            "error": r.error, "errors": r.errors, "note": r.note,
        })
    suffix = "" if selection.get("is_default_full") else f"_{selection.get('slug', 'selection')}"
    fname = f"benchmark_report_{SCALE}_{PROTO}{suffix}.json"
    with open(fname, "w") as f: json.dump(report, f, indent=2)
    print(f"  Report saved → {fname}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════
def run_benchmark_body(
    parts: List[PartSpec],
    selection: Dict[str, object],
    selected_part_keys: Set[str],
    timings_extra: Optional[Dict[str, object]] = None,
) -> None:
    timings = setup(selected_part_keys)
    if timings_extra:
        timings.update(timings_extra)
    if timings.get("sql_block_zone_map_owned_server"):
        data_dir = optional_str(timings.get("sql_block_zone_map_data_dir"))
        if data_dir:
            timings["sql_block_zone_map_data_dir_bytes_after_load"] = dir_size_bytes(data_dir)
            timings["sql_block_zone_map_sstable_files_after_load"] = len(list_sstable_files(data_dir))
            SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT.update({
                "data_dir_bytes_after_load": timings[
                    "sql_block_zone_map_data_dir_bytes_after_load"
                ],
                "sstable_files_after_load": timings[
                    "sql_block_zone_map_sstable_files_after_load"
                ],
            })
    all_results = []

    print(f"{'═'*COL_W}")
    print(f"  BENCHMARK RESULTS  (latency in ms · lower = better)")
    if not selection["is_default_full"]:
        selected = ", ".join(part["key"] for part in selection["selected_parts"])
        print(f"  Selection: {selection['slug']}  │  Parts: {selected}")
    print(f"{'═'*COL_W}")

    for part in parts:
        res = part.fn()
        for item in res:
            item.part_id = part.id
            item.part_key = part.key
            item.part_title = part.title
        all_results.extend(res)
        section(part.title, res)

    apply_rseek_ab_pair_claims(all_results)
    apply_part31_zone_map_control_pair_claims(all_results)

    # ── Summary ──
    print(f"\n{'═'*COL_W}")
    print(f"  SUMMARY — {SCALE.upper()} scale")
    print(f"{'═'*COL_W}")
    print(f"  Total rows loaded:  {timings.get('total_rows',0):>12,}")
    load_ms = timings.get('total_load_ms',0)
    rate = timings.get('total_rows',0) / max(load_ms/1000, 0.001)
    print(f"  Load time:          {load_ms:>12,.0f} ms  ({rate:,.0f} rows/sec)")

    valid = [r for r in all_results if not r.error and r.times_ms and not r.note]
    if valid:
        by_cat = {}
        for r in valid: by_cat.setdefault(r.category, []).append(r)
        print()
        for cn, rs in by_cat.items():
            avg_all = statistics.mean([r.avg for r in rs])
            fast = min(rs, key=lambda x: x.avg)
            slow = max(rs, key=lambda x: x.avg)
            print(f"  [{cn}]  avg latency: {avg_all:.2f} ms")
            print(f"    Fastest: {fast.name:<28} {fast.avg:>7.2f} ms")
            print(f"    Slowest: {slow.name:<28} {slow.avg:>7.2f} ms")

    # Index speedup
    scan = next((r for r in all_results if r.name == "Full scan (val=X)"), None)
    idx  = next((r for r in all_results if r.name == "Index scan (val=X)"), None)
    if scan and idx and idx.avg > 0:
        print(f"\n  Index speedup: {scan.avg/idx.avg:.1f}x  (scan {scan.avg:.2f}ms → index {idx.avg:.2f}ms)")

    # Concurrent throughput
    conc = [r for r in all_results if r.note and r.category == "Concurrent"]
    if conc:
        print(f"\n  [Concurrent Throughput]")
        for r in conc:
            print(f"    {r.name:<28} {r.note}")

    errors = [r for r in all_results if r.error]
    if errors:
        print(f"\n  ⚠ {len(errors)} benchmark(s) had errors:")
        for r in errors:
            print(f"    • {r.name}: {r.error[:80]}")

    print(f"\n{'═'*COL_W}")

    save_report(timings, all_results, selection)
    if BENCH_CLAIM_MODE:
        if errors:
            print(f"  BENCH_CLAIM_MODE failed: {len(errors)} benchmark(s) violated gates")
            sys.exit(2)
        print("  BENCH_CLAIM_MODE passed")
    print()


def sql_block_zone_map_owned_server_enabled(selected_part_keys: Set[str]) -> bool:
    if selected_part_keys != {"sql_block_zone_map_prune"}:
        return False
    if SQL_BLOCK_ZONE_MAP_OWNED_SERVER_RAW:
        enabled = SQL_BLOCK_ZONE_MAP_OWNED_SERVER_RAW not in ("0", "false", "no")
    else:
        enabled = BENCH_CLAIM_MODE
    if enabled and PROTO != "http":
        raise SystemExit("BENCH_SQL_BLOCK_ZONE_MAP_OWNED_SERVER requires BENCH_PROTO=http")
    return enabled


def prepare_sql_block_zone_map_owned_workdir() -> Tuple[str, Optional[str]]:
    cleanup_root = None
    if SQL_BLOCK_ZONE_MAP_WORKDIR:
        scenario_root = os.path.abspath(SQL_BLOCK_ZONE_MAP_WORKDIR)
        if os.path.exists(scenario_root):
            if not os.path.isdir(scenario_root):
                raise SystemExit(
                    "BENCH_SQL_BLOCK_ZONE_MAP_WORKDIR exists and is not a directory: "
                    f"{scenario_root}"
                )
            if os.listdir(scenario_root):
                if not SQL_BLOCK_ZONE_MAP_RESET_WORKDIR:
                    raise SystemExit(
                        "BENCH_SQL_BLOCK_ZONE_MAP_WORKDIR is non-empty; set "
                        "BENCH_SQL_BLOCK_ZONE_MAP_RESET_WORKDIR=1 to allow deletion: "
                        f"{scenario_root}"
                    )
                shutil.rmtree(scenario_root)
        os.makedirs(scenario_root, exist_ok=True)
    else:
        scenario_root = tempfile.mkdtemp(prefix="fusiondb_zone_map_")
        if not SQL_BLOCK_ZONE_MAP_KEEP_WORKDIR:
            cleanup_root = scenario_root
    return scenario_root, cleanup_root


def run_with_sql_block_zone_map_owned_server(
    parts: List[PartSpec],
    selection: Dict[str, object],
    selected_part_keys: Set[str],
) -> None:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    binary = SQL_BLOCK_ZONE_MAP_BINARY
    if not os.path.isabs(binary):
        binary = os.path.join(repo_root, binary)
    if not os.path.exists(binary):
        raise SystemExit(f"fusiondb binary not found for Part 31 owned server: {binary}")

    scenario_root, cleanup_root = prepare_sql_block_zone_map_owned_workdir()
    data_dir = os.path.join(scenario_root, "data")
    log_path = os.path.join(scenario_root, "fusiondb.log")
    write_startup_config(
        scenario_root,
        data_dir,
        SQL_BLOCK_ZONE_MAP_PORT,
        memtable_flush_mb=SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB,
    )
    query_url = f"http://127.0.0.1:{SQL_BLOCK_ZONE_MAP_PORT}/query"
    metrics_url = f"http://127.0.0.1:{SQL_BLOCK_ZONE_MAP_PORT}/metrics"
    checkpoint_url = f"http://127.0.0.1:{SQL_BLOCK_ZONE_MAP_PORT}/checkpoint"
    previous_urls = switch_benchmark_urls(query_url, metrics_url, checkpoint_url)
    proc: Optional[subprocess.Popen] = None
    global SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT

    try:
        proc, ready_metrics, ready_ms, start_error = start_restart_phase_server(
            binary,
            scenario_root,
            log_path,
            metrics_url,
            SQL_BLOCK_ZONE_MAP_TIMEOUT_SEC,
        )
        base_metadata: Dict[str, object] = {
            "benchmark_owned_server": True,
            "binary": binary,
            "http_port": SQL_BLOCK_ZONE_MAP_PORT,
            "query_url": query_url,
            "metrics_url": metrics_url,
            "checkpoint_url": checkpoint_url,
            "scenario_workdir": scenario_root,
            "scenario_data_dir": data_dir,
            "memtable_flush_mb": SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB,
            "timeout_sec": SQL_BLOCK_ZONE_MAP_TIMEOUT_SEC,
            "ready_ms": round(ready_ms, 3),
            "rss_ready_kb": rss_kb(proc.pid) if proc else None,
            "initial_metrics": metric_subset(ready_metrics),
            "log_path": log_path
            if SQL_BLOCK_ZONE_MAP_KEEP_WORKDIR or SQL_BLOCK_ZONE_MAP_WORKDIR
            else None,
        }
        SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT = dict(base_metadata)
        if start_error:
            raise SystemExit(f"Part 31 owned FusionDB server failed to start: {start_error}")

        timings_extra = {
            "sql_block_zone_map_owned_server": 1,
            "sql_block_zone_map_binary": binary,
            "sql_block_zone_map_port": SQL_BLOCK_ZONE_MAP_PORT,
            "sql_block_zone_map_workdir": scenario_root,
            "sql_block_zone_map_data_dir": data_dir,
            "sql_block_zone_map_memtable_flush_mb": SQL_BLOCK_ZONE_MAP_MEMTABLE_FLUSH_MB,
            "sql_block_zone_map_timeout_sec": SQL_BLOCK_ZONE_MAP_TIMEOUT_SEC,
            "sql_block_zone_map_ready_ms": round(ready_ms, 3),
            "sql_block_zone_map_initial_metrics": metric_subset(ready_metrics),
        }
        run_benchmark_body(parts, selection, selected_part_keys, timings_extra)
    finally:
        if proc is not None:
            stop_fusiondb_process(proc)
        restore_benchmark_urls(previous_urls)
        SQL_BLOCK_ZONE_MAP_OWNED_SERVER_CONTEXT = {}
        if cleanup_root:
            shutil.rmtree(cleanup_root, ignore_errors=True)


def main():
    parts, selection = select_part_specs(part_specs())
    selected_part_keys = {part.key for part in parts}
    if sql_block_zone_map_owned_server_enabled(selected_part_keys):
        run_with_sql_block_zone_map_owned_server(parts, selection, selected_part_keys)
    else:
        run_benchmark_body(parts, selection, selected_part_keys)


if __name__ == "__main__":
    main()
