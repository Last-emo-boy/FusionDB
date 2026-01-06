import requests
import json
import time

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
        if e.response:
            print(f"Response: {e.response.text}")
        return {"error": str(e)}

def test_fts():
    print("--- Testing Full Text Search ---")

    # 1. Setup
    execute_sql("DROP TABLE IF EXISTS articles")
    execute_sql("CREATE TABLE articles (id INT, title TEXT, content TEXT)")
    
    # 2. Insert data before index
    execute_sql("INSERT INTO articles VALUES (1, 'Rust DB', 'Rust is a systems programming language and great for databases')")
    execute_sql("INSERT INTO articles VALUES (2, 'Python', 'Python is great for scripting and AI')")
    
    # 3. Create FTS Index
    # Note: Ensure parser supports this syntax. We verified it does.
    res = execute_sql("CREATE INDEX idx_content ON articles (content) USING FTS")
    print(json.dumps(res, indent=2))
    assert res['error'] is None

    # 4. Insert data after index
    execute_sql("INSERT INTO articles VALUES (3, 'Go', 'Go is good for microservices')")
    execute_sql("INSERT INTO articles VALUES (4, 'Mixed', 'Rust and Python are both popular')")

    # 5. Search: "Rust"
    print("\nSearch: 'Rust'")
    res = execute_sql("SELECT id, title FROM articles WHERE MATCH(content) AGAINST('Rust')")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    # Should match id 1 and 4
    ids = sorted([r[0]['Integer'] for r in rows])
    assert ids == [1, 4]

    # 6. Search: "databases" (case insensitive check, tokenizer lowercases)
    print("\nSearch: 'databases'")
    res = execute_sql("SELECT id, title FROM articles WHERE MATCH(content) AGAINST('Databases')")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    # Should match id 1
    assert len(rows) == 1
    assert rows[0][0]['Integer'] == 1

    # 7. Search: "Python"
    print("\nSearch: 'Python'")
    res = execute_sql("SELECT id, title FROM articles WHERE MATCH(content) AGAINST('Python')")
    rows = res['result'][0]['rows']
    ids = sorted([r[0]['Integer'] for r in rows])
    assert ids == [2, 4]

    # 8. Search: "Rust Python" (AND logic)
    print("\nSearch: 'Rust Python'")
    res = execute_sql("SELECT id, title FROM articles WHERE MATCH(content) AGAINST('Rust Python')")
    print(json.dumps(res, indent=2))
    rows = res['result'][0]['rows']
    # Should match id 4 only
    assert len(rows) == 1
    assert rows[0][0]['Integer'] == 4

    # 9. Update
    print("\nUpdate id 2 to mention Rust")
    execute_sql("UPDATE articles SET content = 'Python is great but Rust is faster' WHERE id = 2")
    
    print("\nSearch: 'Rust' after update")
    res = execute_sql("SELECT id, title FROM articles WHERE MATCH(content) AGAINST('Rust')")
    rows = res['result'][0]['rows']
    ids = sorted([r[0]['Integer'] for r in rows])
    # Should match 1, 2, 4
    assert ids == [1, 2, 4]

    # 10. Delete
    print("\nDelete id 4")
    execute_sql("DELETE FROM articles WHERE id = 4")
    
    print("\nSearch: 'Rust' after delete")
    res = execute_sql("SELECT id, title FROM articles WHERE MATCH(content) AGAINST('Rust')")
    rows = res['result'][0]['rows']
    ids = sorted([r[0]['Integer'] for r in rows])
    # Should match 1, 2
    assert ids == [1, 2]

    print("\nALL FTS TESTS PASSED!")

if __name__ == "__main__":
    test_fts()
