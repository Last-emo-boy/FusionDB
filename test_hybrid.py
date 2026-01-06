import requests
import json
import math

BASE_URL = "http://localhost:8091/query"

def get_base_url():
    try:
        with open("server_port.txt", "r") as f:
            port = f.read().strip()
            return f"http://localhost:{port}/query"
    except:
        return BASE_URL

URL = get_base_url()

def execute_sql(sql):
    print(f"Executing: {sql}")
    try:
        response = requests.post(URL, json={"sql": sql})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return {"error": str(e)}

def test_hybrid():
    print("--- Testing Hybrid Search (FTS + Vector) ---")

    # 1. Setup
    execute_sql("DROP TABLE IF EXISTS papers")
    execute_sql("CREATE TABLE papers (id INT, content TEXT, embedding VECTOR(3))")
    
    # 2. Insert data
    # 1: "Database internals" -> [1, 0, 0]
    # 2: "Rust programming" -> [0, 1, 0]
    # 3: "Database optimization with Rust" -> [0.5, 0.5, 0] (Mix)
    # 4: "Cooking recipes" -> [0, 0, 1]
    
    execute_sql("INSERT INTO papers VALUES (1, 'Database internals', [1.0, 0.0, 0.0])")
    execute_sql("INSERT INTO papers VALUES (2, 'Rust programming', [0.0, 1.0, 0.0])")
    execute_sql("INSERT INTO papers VALUES (3, 'Database optimization with Rust', [0.7, 0.7, 0.0])")
    execute_sql("INSERT INTO papers VALUES (4, 'Cooking recipes', [0.0, 0.0, 1.0])")

    # 3. Create FTS Index
    res = execute_sql("CREATE INDEX idx_content ON papers (content) USING FTS")
    print(json.dumps(res, indent=2))
    assert res.get('error') is None

    # 4. Hybrid Query
    # Goal: Find papers about "Database" that are close to "Rust" vector [0, 1, 0].
    # "Database internals" (1) matches "Database", dist([1,0,0], [0,1,0]) = sqrt(2) = 1.414
    # "Database optimization with Rust" (3) matches "Database", dist([0.7,0.7,0], [0,1,0]) = sqrt(0.7^2 + 0.3^2) = sqrt(0.49 + 0.09) = sqrt(0.58) = 0.76
    
    # Threshold < 1.0 should exclude (1) and include (3).
    
    print("\nQuery: MATCH 'Database' AND Vector Distance < 1.0")
    # Note: SQL syntax depends on what parser supports. 
    # VECTOR_DISTANCE(embedding, [0,1,0]) < 1.0
    sql = "SELECT id, content FROM papers WHERE MATCH(content) AGAINST('Database') AND VECTOR_DISTANCE(embedding, [0.0, 1.0, 0.0]) < 1.0"
    res = execute_sql(sql)
    print(json.dumps(res, indent=2))
    
    rows = res['result'][0]['rows']
    # Should only return id 3
    # id 1 matches FTS but fails vector check (dist 1.414 > 1.0)
    # id 2 matches vector (dist 0) but fails FTS (no "Database")
    # id 3 matches FTS and vector (dist 0.76 < 1.0)
    # id 4 fails both
    
    assert len(rows) == 1
    assert rows[0][0]['Integer'] == 3
    
    print("\nALL HYBRID TESTS PASSED!")

if __name__ == "__main__":
    test_hybrid()
