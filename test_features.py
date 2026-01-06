import requests
import json
import time
import os

def get_base_url():
    port_file = "server_port.txt"
    print(f"Looking for {port_file}...")
    for _ in range(10):
        if os.path.exists(port_file):
            try:
                with open(port_file, "r") as f:
                    port = f.read().strip()
                    if port:
                        print(f"Found server port: {port}")
                        return f"http://127.0.0.1:{port}/query"
            except:
                pass
        time.sleep(0.5)
    print("WARNING: Could not read server_port.txt, defaulting to 8000")
    return "http://127.0.0.1:8000/query"

BASE_URL = get_base_url()

def execute_sql(sql):
    try:
        response = requests.post(BASE_URL, json={"sql": sql})
        if response.status_code != 200:
             print(f"HTTP Error {response.status_code}: {response.text}")
        
        response.raise_for_status()
        res = response.json()
        if 'error' in res and res['error']:
            print(f"ERROR: {res['error']}")
        return res
    except Exception as e:
        print(f"Exception: {e}")
        return None

def test_features():
    print("Testing new features...")
    
    # 1. Cleanup & Setup
    execute_sql("DROP TABLE IF EXISTS products")
    execute_sql("CREATE TABLE products (id INT, name TEXT, price INT)")
    execute_sql("CREATE INDEX idx_price ON products (price)")
    
    # Insert Data
    print("Inserting data...")
    execute_sql("INSERT INTO products VALUES (1, 'Apple', 10)")
    execute_sql("INSERT INTO products VALUES (2, 'Banana', 20)")
    execute_sql("INSERT INTO products VALUES (3, 'Cherry', 30)")
    execute_sql("INSERT INTO products VALUES (4, 'Date', 20)")
    execute_sql("INSERT INTO products VALUES (5, 'Elderberry', 10)")

    # 2. Test COUNT
    print("\nTest COUNT(*)...")
    res = execute_sql("SELECT COUNT(*) FROM products")
    print(json.dumps(res, indent=2))
    assert res['result'][0]['rows'][0][0] == 5

    # 3. Test LIMIT / OFFSET
    print("\nTest LIMIT 2 OFFSET 1...")
    # Note: Previously this returned all columns. Now it should only return 'name'.
    res = execute_sql("SELECT name FROM products ORDER BY id LIMIT 2 OFFSET 1")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    assert len(rows) == 2
    # Name is now at index 0 because only 'name' was selected
    assert rows[0][0] == 'Banana'
    assert rows[1][0] == 'Cherry'

    # 4. Test UPDATE
    print("\nTest UPDATE price WHERE name='Apple'...")
    execute_sql("UPDATE products SET price = 15 WHERE name = 'Apple'")
    res = execute_sql("SELECT price FROM products WHERE name = 'Apple'")
    print(json.dumps(res, indent=2))
    # Only 'price' selected, so index 0
    assert res['result'][0]['rows'][0][0] == 15

    # 4.1 Verify Index Maintenance after Update
    # Apple price changed from 10 to 15. Searching for 10 should not find Apple.
    print("\nVerify Index Maintenance (Old Value)...")
    res = execute_sql("SELECT name FROM products WHERE price = 10")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    # Should only find Elderberry (5)
    assert len(rows) == 1 
    # Elderberry name is at index 0
    assert rows[0][0] == 'Elderberry'

    # 5. Test DELETE
    print("\nTest DELETE WHERE price = 20...")
    execute_sql("DELETE FROM products WHERE price = 20")
    res = execute_sql("SELECT COUNT(*) FROM products")
    print(json.dumps(res, indent=2))
    # Original 5, Deleted 2 (Banana, Date), Remaining 3
    assert res['result'][0]['rows'][0][0] == 3

    # 6. Test Projection (New)
    print("\nTest Projection SELECT name, price FROM products...")
    res = execute_sql("SELECT name, price FROM products WHERE price < 30")
    print(json.dumps(res, indent=2))
    # Should contain columns ["name", "price"] only
    cols = res['result'][0]['columns']
    assert len(cols) == 2
    assert cols[0] == "name"
    assert cols[1] == "price"
    rows = res['result'][0]['rows']
    # Check row content
    assert len(rows[0]) == 2
    
    # 7. Test Projection with Alias
    print("\nTest Projection SELECT name AS n, price * 2 AS double_price ...")
    # Note: Expression evaluation in projection is not yet implemented, but Alias is part of projection parsing.
    # If I implement expression evaluation in projection, this should work.
    # Let's test simple alias on column first.
    res = execute_sql("SELECT name AS n FROM products")
    print(json.dumps(res, indent=2))
    cols = res['result'][0]['columns']
    assert cols[0] == "n"

    # 8. Test GROUP BY and Aggregation
    print("\nTest GROUP BY and Aggregation (SUM, COUNT)...")
    # Insert more data to have meaningful groups
    execute_sql("INSERT INTO products VALUES (6, 'Apple', 20)")
    execute_sql("INSERT INTO products VALUES (7, 'Banana', 30)")
    # Current Data:
    # 1: Apple, 15 (Updated)
    # 3: Cherry, 30
    # 5: Elderberry, 10
    # 6: Apple, 20
    # 7: Banana, 30
    # Note: Banana(2) and Date(4) were deleted.
    
    res = execute_sql("SELECT name, COUNT(*) AS count, SUM(price) AS total_price FROM products GROUP BY name ORDER BY name")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    # Expected groups:
    # Apple: 2 rows (15+20=35)
    # Banana: 1 row (30)
    # Cherry: 1 row (30)
    # Elderberry: 1 row (10)
    
    # Sort order: Apple, Banana, Cherry, Elderberry
    assert len(rows) == 4
    
    # Check Apple
    assert rows[0][0] == 'Apple'
    assert rows[0][1] == 2
    assert rows[0][2] == 35
    
    # Check Banana
    assert rows[1][0] == 'Banana'
    assert rows[1][1] == 1
    assert rows[1][2] == 30

    # 9. Test HAVING
    print("\nTest HAVING COUNT(*) > 1...")
    res = execute_sql("SELECT name, COUNT(*) as c FROM products GROUP BY name HAVING COUNT(*) > 1")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    # Only Apple should remain (count=2)
    assert len(rows) == 1
    assert rows[0][0] == 'Apple'
    assert rows[0][1] == 2

    # 10. Test JOIN
    print("\nTest JOIN...")
    execute_sql("DROP TABLE IF EXISTS t1")
    execute_sql("DROP TABLE IF EXISTS t2")
    execute_sql("CREATE TABLE t1 (id INT, v1 TEXT)")
    execute_sql("CREATE TABLE t2 (id INT, t1_id INT, v2 TEXT)")
    
    execute_sql("INSERT INTO t1 VALUES (1, 'A')")
    execute_sql("INSERT INTO t1 VALUES (2, 'B')")
    
    execute_sql("INSERT INTO t2 VALUES (100, 1, 'X')")
    execute_sql("INSERT INTO t2 VALUES (200, 1, 'Y')")
    execute_sql("INSERT INTO t2 VALUES (300, 3, 'Z')") # No match in t1
    
    # Join t1 and t2 on t1.id = t2.t1_id
    res = execute_sql("SELECT t1.id, t1.v1, t2.v2 FROM t1 JOIN t2 ON t1.id = t2.t1_id")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    
    # Expected results:
    # t1(1, 'A') matches t2(100, 1, 'X') -> (1, 'A', 'X')
    # t1(1, 'A') matches t2(200, 1, 'Y') -> (1, 'A', 'Y')
    # t1(2, 'B') has no match in t2
    # t2(300, 3, 'Z') has no match in t1
    
    assert len(rows) == 2
    # We don't guarantee order unless ORDER BY is used, but nested loop usually preserves insertion order of outer table (t1) if inner (t2) is scanned
    # Check for existence
    
    found_x = False
    found_y = False
    for r in rows:
        # t1.id is at 0, t1.v1 at 1, t2.v2 at 2
        if r[0] == 1 and r[1] == 'A' and r[2] == 'X':
            found_x = True
        if r[0] == 1 and r[1] == 'A' and r[2] == 'Y':
            found_y = True
            
    assert found_x
    assert found_y

    # 11. Test Arithmetic Expressions
    print("\nTest Arithmetic Expressions...")
    # Apple has price 15 (after update), Elderberry has price 10.
    # Select Apple (id=1): price(15) * 2 = 30, price(15) + 5 = 20
    res = execute_sql("SELECT name, price * 2 AS double_price, price + 5 AS price_plus_5 FROM products WHERE id = 1")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    assert len(rows) == 1
    # Check double_price (index 1)
    assert rows[0][1] == 30
    # Check price_plus_5 (index 2)
    assert rows[0][2] == 20

    # 12. Test LEFT JOIN
    print("\nTest LEFT JOIN...")
    # t1: (1, A), (2, B)
    # t2: (100, 1, X), (200, 1, Y), (300, 3, Z)
    # Left Join t1 ON t2:
    # 1 -> Matches X, Y
    # 2 -> No Match -> (2, B, NULL)
    res = execute_sql("SELECT t1.id, t1.v1, t2.v2 FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    assert len(rows) == 3 
    
    # Check for (2, B, NULL)
    found_null = False
    for r in rows:
        # Value::Null serializes to "Null" string in Rust Serde default for unit variant
        # BUT to_json returns serde_json::Value::Null, which becomes None in Python
        if r[0] == 2 and r[1] == 'B' and r[2] is None:
            found_null = True
    assert found_null

    print("\nALL TESTS PASSED!")

if __name__ == "__main__":
    test_features()
