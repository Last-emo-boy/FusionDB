import requests
import json
import time
import subprocess
import os
import sys

BASE_URL = "http://localhost:8091/query"

def execute_sql(sql):
    try:
        response = requests.post(BASE_URL, json={"sql": sql})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

def test_persistence():
    print("--- Testing Persistence ---")
    
    # 1. Start Server (if not running, but we assume we control it via tools)
    # Actually, the agent environment manages the server.
    # But to test persistence, I need to RESTART it.
    
    # Step 1: Check if server is running, if so, use it to insert data.
    print("Step 1: Insert Data")
    execute_sql("DROP TABLE IF EXISTS persistent_table")
    execute_sql("CREATE TABLE persistent_table (id INT, name TEXT)")
    execute_sql("INSERT INTO persistent_table VALUES (1, 'Data 1')")
    res = execute_sql("SELECT * FROM persistent_table")
    print("Data before restart:", json.dumps(res))
    assert len(res['result'][0]['rows']) == 1

    # Step 2: Stop Server
    print("Step 2: Stopping Server...")
    # I can't stop the server from python script easily if it was started by the agent tool.
    # But I can use the tool to stop it.
    
if __name__ == "__main__":
    # This script only does the insertion part. 
    # The verification part needs to be run after restart.
    
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        print("Step 3: Verify Data after restart")
        res = execute_sql("SELECT * FROM persistent_table")
        print("Data after restart:", json.dumps(res))
        if 'error' in res and res['error']:
             print("Error:", res['error'])
             sys.exit(1)
             
        rows = res['result'][0]['rows']
        if len(rows) == 1 and rows[0][1]['String'] == 'Data 1':
            print("Persistence Verified!")
        else:
            print("Persistence Failed!")
            sys.exit(1)
    else:
        test_persistence()
