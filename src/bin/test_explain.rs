use fusiondb::execution::Executor;
use fusiondb::parser::parse_sql;
use fusiondb::server::http_server;
use fusiondb::storage::memory::MemoryStorage;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let storage = Arc::new(MemoryStorage::new("test_explain_wal").unwrap());
        let executor = Arc::new(Executor::new(storage.clone()));

        // 1. Create Table
        let sql = "CREATE TABLE users (id INT, name TEXT, age INT)";
        let stmt = &parse_sql(sql).unwrap()[0];
        executor.execute(stmt).await.unwrap();
        println!("Table created.");

        // 2. Insert Data
        let sql = "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)";
        let stmt = &parse_sql(sql).unwrap()[0];
        executor.execute(stmt).await.unwrap();
        println!("Data inserted.");

        // 3. Create Index
        let sql = "CREATE INDEX idx_age ON users (age)";
        let stmt = &parse_sql(sql).unwrap()[0];
        executor.execute(stmt).await.unwrap();
        println!("Index created.");

        // 4. EXPLAIN (Should use Index)
        println!("\n--- EXPLAIN (Index Scan) ---");
        let sql = "EXPLAIN SELECT * FROM users WHERE age = 30";
        let stmt = &parse_sql(sql).unwrap()[0];
        let res = executor.execute(stmt).await.unwrap();
        if let fusiondb::execution::QueryResult::Select { rows, .. } = res {
            println!("{}", rows[0][0]);
        }

        // 5. EXPLAIN (Full Scan)
        println!("\n--- EXPLAIN (Full Scan) ---");
        let sql = "EXPLAIN SELECT * FROM users WHERE name = 'Alice'";
        let stmt = &parse_sql(sql).unwrap()[0];
        let res = executor.execute(stmt).await.unwrap();
        if let fusiondb::execution::QueryResult::Select { rows, .. } = res {
            println!("{}", rows[0][0]);
        }

        // 6. EXPLAIN ANALYZE
        println!("\n--- EXPLAIN ANALYZE ---");
        let sql = "EXPLAIN ANALYZE SELECT * FROM users WHERE age = 30";
        let stmt = &parse_sql(sql).unwrap()[0];
        let res = executor.execute(stmt).await.unwrap();
        if let fusiondb::execution::QueryResult::Select { rows, .. } = res {
            println!("{}", rows[0][0]);
        }
        // 7. SHOW TABLES
        println!("\n--- SHOW TABLES ---");
        let sql = "SHOW TABLES";
        let stmt = &parse_sql(sql).unwrap()[0];
        let res = executor.execute(stmt).await.unwrap();
        if let fusiondb::execution::QueryResult::Select { rows, .. } = res {
            println!("Tables found: {}", rows.len());
            for row in rows {
                println!("- {:?}", row[0]);
            }
        }
        // 8. DESCRIBE
        println!("\n--- DESCRIBE users ---");
        let sql = "DESCRIBE users";
        let stmt = &parse_sql(sql).unwrap()[0];
        let res = executor.execute(stmt).await.unwrap();
        if let fusiondb::execution::QueryResult::Select { columns, rows } = res {
            println!("{:?}", columns);
            for row in rows {
                println!("{:?}", row);
            }
        }
    });
}
