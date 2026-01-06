import requests
import time
import random
import json

BASE_URL = "http://localhost:8000/query"

def execute_sql(sql):
    try:
        start = time.time()
        response = requests.post(BASE_URL, json={"sql": sql})
        duration = (time.time() - start) * 1000 # ms
        response.raise_for_status()
        return response.json(), duration
    except Exception as e:
        print(f"Error executing SQL: {sql[:100]}...")
        print(e)
        return None, 0

def setup_data(rows=10000, batch_size=1000):
    print(f"Preparing data: {rows} rows...")
    
    # 1. Cleanup
    execute_sql("DROP TABLE IF EXISTS bench_no_index") # Not implemented DROP yet, but okay
    execute_sql("DROP TABLE IF EXISTS bench_index")

    # 2. Create Tables
    print("Creating tables...")
    execute_sql("CREATE TABLE bench_no_index (id INT, val INT, data TEXT)")
    execute_sql("CREATE TABLE bench_index (id INT, val INT, data TEXT)")
    
    # 3. Create Index
    print("Creating index on bench_index(val)...")
    execute_sql("CREATE INDEX idx_val ON bench_index (val)")

    # 4. Insert Data
    print(f"Inserting {rows} rows...")
    
    # Generate data
    # We want 'val' to be random between 0 and 1000
    for i in range(0, rows, batch_size):
        batch_values = []
        for j in range(batch_size):
            id = i + j
            val = random.randint(0, 1000)
            data = f"row_{id}_data"
            batch_values.append(f"({id}, {val}, '{data}')")
        
        values_str = ", ".join(batch_values)
        sql_no_index = f"INSERT INTO bench_no_index VALUES {values_str}"
        sql_index = f"INSERT INTO bench_index VALUES {values_str}"
        
        execute_sql(sql_no_index)
        execute_sql(sql_index)
        
        if (i + batch_size) % 5000 == 0:
            print(f"  Inserted {i + batch_size} rows...")

def run_benchmark(target_val=500):
    print("\nStarting Benchmark...")
    print("-" * 50)
    print(f"{'Query Type':<20} | {'Avg Time (ms)':<15} | {'Result Count':<10}")
    print("-" * 50)

    # 1. Scan (No Index)
    scan_times = []
    scan_count = 0
    for _ in range(5):
        res, ms = execute_sql(f"SELECT * FROM bench_no_index WHERE val = {target_val}")
        if res and 'result' in res and res['result']:
             # Extract row count from result
             # Result format: {"result": [{"columns": [...], "rows": [[...], [...]]}]}
             if isinstance(res['result'], list) and len(res['result']) > 0:
                 rows = res['result'][0].get('rows', [])
                 scan_count = len(rows)
        scan_times.append(ms)
    
    avg_scan = sum(scan_times) / len(scan_times)
    print(f"{'Full Table Scan':<20} | {avg_scan:<15.2f} | {scan_count:<10}")

    # 2. Index Seek
    index_times = []
    index_count = 0
    for _ in range(5):
        res, ms = execute_sql(f"SELECT * FROM bench_index WHERE val = {target_val}")
        if res and 'result' in res and res['result']:
             if isinstance(res['result'], list) and len(res['result']) > 0:
                 rows = res['result'][0].get('rows', [])
                 index_count = len(rows)
        index_times.append(ms)

    avg_index = sum(index_times) / len(index_times)
    print(f"{'Index Lookup':<20} | {avg_index:<15.2f} | {index_count:<10}")
    print("-" * 50)
    
    speedup = avg_scan / avg_index if avg_index > 0 else 0
    print(f"Speedup: {speedup:.2f}x")

if __name__ == "__main__":
    # Ensure server is healthy
    try:
        requests.get("http://localhost:8000/health")
    except:
        print("Server is not running! Please start the server first.")
        exit(1)

    setup_data(rows=5000, batch_size=500) # 5000 rows for quick test
    run_benchmark(target_val=123)
