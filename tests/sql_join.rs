use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

#[tokio::test]
async fn test_inner_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget'), (3, 1, 'Doohickey')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    )
    .await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_base_scan_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE join_cache_users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE join_cache_orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_cache_users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_cache_orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM join_cache_users").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice"), (2_i64, "Bob")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:join_cache_users:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT * FROM join_cache_users JOIN join_cache_orders ON join_cache_users.id = join_cache_orders.user_id",
    )
    .await;
    assert_eq!(
        cols,
        vec![
            "join_cache_users.id",
            "join_cache_users.name",
            "join_cache_orders.id",
            "join_cache_orders.user_id",
            "join_cache_orders.product"
        ]
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::String("Alice".to_string()));
    assert_eq!(rows[1][1], Value::String("Bob".to_string()));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_join_base_scan_populates_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE join_warm_users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE join_warm_orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_warm_users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_warm_orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT * FROM join_warm_users JOIN join_warm_orders ON join_warm_users.id = join_warm_orders.user_id",
    )
    .await;
    assert_eq!(rows.len(), 2);

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice"), (2_i64, "Bob")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:join_warm_users:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT * FROM join_warm_users").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_three_table_join_with_alias_projection() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_order_items_order_id ON order_items (order_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (10, 1, 'confirmed'), (11, 1, 'shipped'), (20, 2, 'pending')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_items VALUES (100, 10, 9001), (101, 10, 9002), (102, 11, 9003), (103, 20, 9004)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT u.name, o.id, oi.product_id FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items oi ON o.id = oi.order_id LIMIT 100",
    )
    .await;

    assert_eq!(cols, vec!["u.name", "o.id", "oi.product_id"]);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(rows[0][1], Value::Integer(10));
    assert_eq!(rows[0][2], Value::Integer(9001));
    assert!(rows
        .iter()
        .any(|row| row[1] == Value::Integer(11) && row[2] == Value::Integer(9003)));
    assert!(rows
        .iter()
        .any(|row| row[0] == Value::String("Bob".to_string()) && row[1] == Value::Integer(20)));

    let (cols, rows) = query(
        &executor,
        "SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items oi ON o.id = oi.order_id WHERE oi.product_id = 9003",
    )
    .await;

    assert_eq!(cols, vec!["u.name"]);
    assert_eq!(rows, vec![vec![Value::String("Alice".to_string())]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_inner_join_with_left_filter_and_indexed_right_probe() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget'), (3, 1, 'Cable'), (4, 3, 'Mouse')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT users.id, users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1 ORDER BY orders.id",
    )
    .await;

    assert_eq!(cols, vec!["users.id", "users.name", "orders.product"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::String("Alice".to_string()));
    assert_eq!(rows[0][2], Value::String("Widget".to_string()));
    assert_eq!(rows[1][2], Value::String("Cable".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_left_filter_projection_skips_unused_left_column_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE social_knows (id INTEGER PRIMARY KEY, person1_id INTEGER, person2_id INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE social_person (id INTEGER PRIMARY KEY, first_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_social_knows_person1 ON social_knows (person1_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO social_knows VALUES (1, 1, 2, 'unused-a'), (2, 1, 3, 'unused-b')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO social_person VALUES (2, 'Bob'), (3, 'Charlie')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, person1_id, person2_id, payload) in
            [(1_i64, 1_i64, 2_i64, "unused-a"), (2, 1, 3, "unused-b")]
        {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(person1_id),
                Value::Integer(person2_id),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:social_knows:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT p.id, p.first_name FROM social_knows k INNER JOIN social_person p ON k.person2_id = p.id WHERE k.person1_id = 1 LIMIT 10",
    )
    .await;

    assert_eq!(cols, vec!["p.id", "p.first_name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::String("Bob".to_string())],
            vec![Value::Integer(3), Value::String("Charlie".to_string())],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_join_index_probe_projection_skips_unused_right_column_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE graph_knows (id INTEGER PRIMARY KEY, person1_id INTEGER, person2_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE graph_post (id INTEGER PRIMARY KEY, creator_id INTEGER, creation_day INTEGER, content TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_graph_knows_person1 ON graph_knows (person1_id)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_graph_post_creator ON graph_post (creator_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO graph_knows VALUES (1, 1, 2), (2, 1, 3)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO graph_post VALUES (10, 2, 30, 'post-a', 'unused-a'), (11, 3, 40, 'post-b', 'unused-b')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, creator_id, creation_day, content, payload) in [
            (10_i64, 2_i64, 30_i64, "post-a", "unused-a"),
            (11, 3, 40, "post-b", "unused-b"),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(creator_id),
                Value::Integer(creation_day),
                Value::String(content.to_string()),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 4usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:graph_post:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT p.id, p.content, p.creation_day FROM graph_knows k INNER JOIN graph_post p ON k.person2_id = p.creator_id WHERE k.person1_id = 1 LIMIT 10",
    )
    .await;

    assert_eq!(cols, vec!["p.id", "p.content", "p.creation_day"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(10),
                Value::String("post-a".to_string()),
                Value::Integer(30),
            ],
            vec![
                Value::Integer(11),
                Value::String("post-b".to_string()),
                Value::Integer(40),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_inner_join_multi_key_uses_indexed_probe_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, city TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, city TEXT, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Paris', 'Alice'), (2, 'Berlin', 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 'Paris', 'Keyboard'), (2, 1, 'London', 'Mouse'), (3, 2, 'Berlin', 'Cable')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT users.id, users.city, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id AND users.city = orders.city WHERE users.id = 1 ORDER BY orders.id",
    )
    .await;

    assert_eq!(cols, vec!["users.id", "users.city", "orders.product"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::String("Paris".to_string()));
    assert_eq!(rows[0][2], Value::String("Keyboard".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_projection_pushdown_with_group_by() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER, unit_price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_order_items_product_id ON order_items (product_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO products VALUES (1, 'Hardware', 'Mouse'), (2, 'Hardware', 'Keyboard'), (3, 'Accessories', 'Cable')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_items VALUES (1, 1, 2, 50), (2, 1, 1, 50), (3, 2, 3, 80), (4, 3, 4, 10)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT products.category, SUM(order_items.quantity * order_items.unit_price) AS revenue FROM order_items INNER JOIN products ON order_items.product_id = products.id GROUP BY products.category ORDER BY SUM(order_items.quantity * order_items.unit_price) DESC",
    )
    .await;

    assert_eq!(cols, vec!["products.category", "revenue"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::String("Hardware".to_string()));
    assert_eq!(rows[0][1], Value::Integer(390));
    assert_eq!(rows[1][0], Value::String("Accessories".to_string()));
    assert_eq!(rows[1][1], Value::Integer(40));
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_group_by_count_sum_fast_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, city TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total FLOAT, status TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_customer_id ON orders (customer_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO customers VALUES (1, 'Paris', 'Alice'), (2, 'Berlin', 'Bob'), (3, 'Paris', 'Cara')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 10.5, 'new'), (2, 1, 5.0, 'paid'), (3, 2, 7.5, 'new'), (4, 3, 2.0, 'new')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT customers.city, COUNT(*), SUM(orders.total) FROM customers INNER JOIN orders ON customers.id = orders.customer_id GROUP BY customers.city ORDER BY SUM(orders.total) DESC",
    )
    .await;

    assert_eq!(
        cols,
        vec!["customers.city", "COUNT(*)", "SUM(orders.total)"]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("Paris".to_string()),
                Value::Integer(3),
                Value::Float(17.5),
            ],
            vec![
                Value::String("Berlin".to_string()),
                Value::Integer(1),
                Value::Float(7.5),
            ],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_left_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO orders VALUES (1, 1, 'Widget')").await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    )
    .await;
    // Alice has 1 order, Bob/Charlie have 0 => 3 rows (with NULLs for Bob/Charlie)
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

// ==================== Expression Tests ====================

#[tokio::test]
async fn test_cross_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE cj1 (id INTEGER PRIMARY KEY, a TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE cj2 (id INTEGER PRIMARY KEY, b TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO cj1 VALUES (1, 'x'), (2, 'y')").await;
    exec_ok(&executor, "INSERT INTO cj2 VALUES (10, 'p'), (20, 'q')").await;
    let (_, rows) = query(&executor, "SELECT cj1.a, cj2.b FROM cj1 CROSS JOIN cj2").await;
    assert_eq!(rows.len(), 4); // 2 x 2 = 4
    cleanup(&wal);
}
