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

Usage:
    1. Start FusionDB:  cargo run
    2. Run benchmark:   python benchmark.py
    3. Quick mode:      BENCH_SCALE=small python benchmark.py
    4. Large mode:      BENCH_SCALE=large python benchmark.py

Options (env vars):
    FUSIONDB_URL   - HTTP endpoint (default: http://127.0.0.1:8091/query)
    BENCH_SCALE    - small / medium / large  (default: medium)
"""

import requests
import time
import random
import os
import sys
import json
import statistics
import threading
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ═══════════════════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════════════════
BASE_URL = os.environ.get("FUSIONDB_URL", "http://127.0.0.1:8091/query")
HEALTH_URL = BASE_URL.replace("/query", "/health")
SCALE = os.environ.get("BENCH_SCALE", "medium").lower()
HTTP_SESSIONS = threading.local()

SCALES = {
    #                 base_rows  users  products  orders  accounts  transfers  events  iters  warmup  batch  threads
    "small":  dict(base_rows=2000,  users=100,  products=50,   orders=500,   accounts=50,  transfers=200,  events=1000,  iters=5,  warmup=1, batch=500,  threads=4),
    "medium": dict(base_rows=10000, users=500,  products=200,  orders=5000,  accounts=200, transfers=2000, events=10000, iters=10, warmup=2, batch=500,  threads=8),
    "large":  dict(base_rows=50000, users=2000, products=1000, orders=20000, accounts=500, transfers=5000, events=50000, iters=20, warmup=3, batch=500,  threads=16),
    "xlarge": dict(base_rows=500000,users=20000,products=10000,orders=200000,accounts=5000,transfers=50000,events=500000,iters=20, warmup=3, batch=1000, threads=32),
}

if SCALE not in SCALES:
    print(f"Invalid BENCH_SCALE={SCALE}. Choose: small, medium, large, xlarge")
    sys.exit(1)

C = SCALES[SCALE]
SEED = 42
random.seed(SEED)

# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════
def http_session():
    session = getattr(HTTP_SESSIONS, "session", None)
    if session is None:
        session = requests.Session()
        HTTP_SESSIONS.session = session
    return session


@dataclass
class BenchResult:
    name: str
    category: str = ""
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

    def record(self, res, ms, capture_rows=True):
        if not res or res.get("status") == "error":
            msg = str((res or {}).get("error") or "unknown error")
            self.error = self.error or msg
            self.errors.append(msg)
            return False
        self.times_ms.append(ms)
        if capture_rows:
            self.row_count = rows(res)
        return True


def sql(query: str, silent=True) -> Tuple[Optional[dict], float]:
    """Execute SQL, return (json_response, latency_ms)."""
    try:
        t0 = time.perf_counter()
        r = http_session().post(BASE_URL, json={"sql": query}, timeout=60)
        ms = (time.perf_counter() - t0) * 1000
        r.raise_for_status()
        payload = r.json()
        if isinstance(payload, dict) and "status" in payload and "data" in payload:
            return payload, ms
        return {"status": "ok", "data": payload, "error": None}, ms
    except Exception as e:
        if not silent:
            print(f"  [ERR] {query[:80]}… → {e}")
        return {"status": "error", "data": None, "error": str(e)}, 0

def sql_ok(q):
    res, _ = sql(q)
    return res

def rows(res):
    if not res or res.get("status") != "ok":
        return 0
    data = res.get("data")
    if data is None:
        data = res.get("result") or []
    first = data[0] if data else {}
    if not isinstance(first, dict):
        return 0
    if "Select" in first and isinstance(first["Select"], dict):
        first = first["Select"]
    if first.get("type") == "select" or "rows" in first:
        return len(first.get("rows") or [])
    return 0

def bench(name, query, iters=None, warmup=None, cat=""):
    iters  = iters  or C["iters"]
    warmup = warmup or C["warmup"]
    r = BenchResult(name=name, category=cat, planned_iters=iters, warmup_iters=warmup)
    for _ in range(warmup): sql(query)
    for _ in range(iters):
        res, ms = sql(query)
        if not r.record(res, ms):
            break
    return r

def insert_batch(table, values_list):
    for i in range(0, len(values_list), C["batch"]):
        chunk = values_list[i:i+C["batch"]]
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


# ═══════════════════════════════════════════════════════════════════════════════
#  Setup — Schema & Data Loading
# ═══════════════════════════════════════════════════════════════════════════════
def setup() -> Dict[str, float]:
    T = {}
    total_rows = 0

    print(f"\n{'═'*100}")
    print(f"  FusionDB Unified Benchmark  │  Scale: {SCALE.upper()}")
    print(f"  base_rows={C['base_rows']}  users={C['users']}  products={C['products']}  "
          f"orders={C['orders']}  accounts={C['accounts']}  events={C['events']}  threads={C['threads']}")
    print(f"  Endpoint: {BASE_URL}")
    print(f"{'═'*100}\n")

    try: http_session().get(HEALTH_URL, timeout=3)
    except Exception:
        print(f"  ERROR: FusionDB not running at {HEALTH_URL}\n  Start with: cargo run"); sys.exit(1)

    # ── Drop ──
    print("  [setup] Dropping old tables …")
    for t in ["order_items","orders","products","users","accounts","transfers","events",
              "bench","bench_idx"]:
        sql_ok(f"DROP TABLE IF EXISTS {t}")

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

    load_ms = sum(v for k,v in T.items() if k.startswith("load_"))
    rate = total_rows / max(load_ms/1000, 0.001)
    T["total_load_ms"] = load_ms; T["total_rows"] = total_rows
    print(f"\n  ✓ Loaded {total_rows:,} rows in {load_ms:,.0f} ms ({rate:,.0f} rows/sec)\n")
    return T


# ═══════════════════════════════════════════════════════════════════════════════
#  Part 1 — Base Benchmarks
# ═══════════════════════════════════════════════════════════════════════════════
def part1_base() -> List[BenchResult]:
    R, cat, N = [], "Base", C["base_rows"]
    tid = N // 2; tv = random.randint(0,999)

    # Point queries
    R.append(bench("PK point lookup",       f"SELECT * FROM bench WHERE id = {tid}", cat=cat))
    R.append(bench("Full scan (val=X)",     f"SELECT * FROM bench WHERE val = {tv}", cat=cat))
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

    def write_op(wid, idx):
        c = random.random()
        oid = 800000 + wid * 10000 + idx
        if   c < 0.40: res, _ = sql(f"INSERT INTO orders VALUES ({oid},{random.randint(0,nu-1)},'pending',{round(random.uniform(10,500),2)},1400)")
        elif c < 0.65: res, _ = sql(f"UPDATE products SET stock = stock - 1 WHERE id = {random.randint(0,np_-1)}")
        elif c < 0.85: res, _ = sql(f"UPDATE orders SET status = 'confirmed' WHERE id = {random.randint(0,no-1)}")
        else:          res, _ = sql(f"INSERT INTO events VALUES ({700000+wid*10000+idx},{random.randint(0,nu-1)},'click',{1700000000+random.randint(0,86400*30)})")
        return res

    def run_mixed(name, read_pct):
        lats = []; errors = []; lock = threading.Lock()
        def worker(wid):
            local_lats = []; local_errors = []
            for i in range(ops_per):
                t0 = time.perf_counter()
                if random.random() < read_pct: res = read_op()
                else: res = write_op(wid, i)
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
        throughput = total_ops / max(wall/1000, 0.001)
        r.planned_iters = total_ops
        r.total_ops = total_ops
        r.wall_ms = wall
        r.throughput_ops_sec = throughput
        r.errors = errors
        if errors:
            r.error = errors[0]
        r.note = f"{total_ops} ops | {nw} threads | wall {wall:.0f}ms | {throughput:.0f} ops/s | errors {len(errors)}"
        return r

    R.append(run_mixed("Read-heavy  (80:20)", 0.80))
    R.append(run_mixed("Balanced    (50:50)", 0.50))
    R.append(run_mixed("Write-heavy (20:80)", 0.20))

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
def part9_column_scan_fast_paths() -> List[BenchResult]:
    R, cat = [], "ColumnScan"

    R.append(bench("Bare COUNT nullable", "SELECT COUNT(category) FROM bench", cat=cat))
    R.append(bench("Bare COUNT with WHERE", "SELECT COUNT(category) FROM bench WHERE val >= 500", cat=cat))
    R.append(bench("COUNT DISTINCT WHERE", "SELECT COUNT(DISTINCT user_id) FROM events WHERE event_type = 'click'", cat=cat))
    R.append(bench("DISTINCT with WHERE", "SELECT DISTINCT category FROM bench WHERE val >= 500", cat=cat))
    R.append(bench("Bare MIN/MAX numeric", "SELECT MIN(amount), MAX(amount) FROM bench", cat=cat))
    R.append(bench("Bare STRING_AGG", "SELECT STRING_AGG(category) FROM bench WHERE val < 5", cat=cat))
    R.append(bench("Bare GROUP_CONCAT", "SELECT GROUP_CONCAT(category) FROM bench WHERE val < 5", cat=cat))

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


def save_report(timings, all_results):
    report = {
        "timestamp": datetime.now().isoformat(),
        "scale": SCALE,
        "config": C,
        "load": timings,
        "benchmarks": []
    }
    for r in all_results:
        report["benchmarks"].append({
            "name": r.name, "category": r.category,
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
            "total_ops": r.total_ops,
            "wall_ms": round(r.wall_ms,3),
            "throughput_ops_sec": round(r.throughput_ops_sec,1),
            "error": r.error, "errors": r.errors, "note": r.note,
        })
    fname = f"benchmark_report_{SCALE}.json"
    with open(fname, "w") as f: json.dump(report, f, indent=2)
    print(f"  Report saved → {fname}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    timings = setup()
    all_results = []

    parts = [
        ("Part 1 — Base Benchmarks",             part1_base),
        ("Part 2 — E-commerce Simulation",        part2_ecommerce),
        ("Part 3 — Financial Ledger",             part3_financial),
        ("Part 4 — Analytics / OLAP",             part4_analytics),
        ("Part 5 — Concurrent Mixed Workload",    part5_concurrent),
        ("Part 6 — Stress & Edge Cases",          part6_stress),
        ("Part 7 — Inventory & Fulfillment",      part7_inventory_fulfillment),
        ("Part 8 — Risk & Audit",                 part8_risk_audit),
        ("Part 9 — Column-Scan Fast Paths",        part9_column_scan_fast_paths),
    ]

    print(f"{'═'*COL_W}")
    print(f"  BENCHMARK RESULTS  (latency in ms · lower = better)")
    print(f"{'═'*COL_W}")

    for title, fn in parts:
        res = fn()
        all_results.extend(res)
        section(title, res)

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
    conc = [r for r in all_results if r.note]
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

    save_report(timings, all_results)
    print()


if __name__ == "__main__":
    main()
