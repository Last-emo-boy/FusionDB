use fusiondb::common::Value;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

#[tokio::test]
async fn test_union_all() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").await;
    exec_ok(&executor, "INSERT INTO t2 VALUES (2, 'b'), (3, 'c')").await;
    let (_, rows) = query(
        &executor,
        "SELECT name FROM t1 UNION ALL SELECT name FROM t2",
    )
    .await;
    assert_eq!(rows.len(), 4); // duplicates kept
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_all_order_by_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_top_a (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_top_b (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_top_a VALUES (1, 50), (2, 10), (3, 40)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_top_b VALUES (4, 20), (5, 60), (6, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT score FROM union_top_a UNION ALL SELECT score FROM union_top_b ORDER BY score ASC LIMIT 3 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(20)],
            vec![Value::Integer(30)],
            vec![Value::Integer(40)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_distinct() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").await;
    exec_ok(&executor, "INSERT INTO t2 VALUES (2, 'b'), (3, 'c')").await;
    let (_, rows) = query(&executor, "SELECT name FROM t1 UNION SELECT name FROM t2").await;
    assert_eq!(rows.len(), 3); // duplicates removed
    cleanup(&wal);
}

#[tokio::test]
async fn test_except() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO t2 VALUES (2, 'b')").await;
    let (_, rows) = query(&executor, "SELECT name FROM t1 EXCEPT SELECT name FROM t2").await;
    assert_eq!(rows.len(), 2); // 'a' and 'c'
    cleanup(&wal);
}

#[tokio::test]
async fn test_intersect() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO t2 VALUES (2, 'b'), (3, 'c'), (4, 'd')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT name FROM t1 INTERSECT SELECT name FROM t2",
    )
    .await;
    assert_eq!(rows.len(), 2); // 'b' and 'c'
    cleanup(&wal);
}

#[tokio::test]
async fn test_subquery_in() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (100, 1), (101, 2), (102, 1)",
    )
    .await;
    // Find customers who have orders
    let (_, rows) = query(
        &executor,
        "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)",
    )
    .await;
    assert_eq!(rows.len(), 2); // Alice and Bob
    cleanup(&wal);
}

#[tokio::test]
async fn test_subquery_not_in() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO orders VALUES (100, 1), (101, 2)").await;
    // Find customers who have NO orders
    let (_, rows) = query(
        &executor,
        "SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)",
    )
    .await;
    assert_eq!(rows.len(), 1); // Carol
    cleanup(&wal);
}

#[tokio::test]
async fn test_scalar_subquery() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE scores (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO scores VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    // Scalar subquery: find rows above average (average = 20)
    let (_, rows) = query(&executor, "SELECT val FROM scores WHERE val > (SELECT 20)").await;
    assert_eq!(rows.len(), 1); // only 30
    cleanup(&wal);
}

#[tokio::test]
async fn test_cte_basic() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO employees VALUES (1, 'Alice', 'eng', 100), (2, 'Bob', 'eng', 120), (3, 'Carol', 'sales', 80)").await;
    let (_, rows) = query(&executor, "WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'eng') SELECT name FROM eng ORDER BY name").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("Alice".to_string())
    );
    assert_eq!(
        rows[1][0],
        fusiondb::common::Value::String("Bob".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_cte_multiple() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "WITH cat_a AS (SELECT price FROM items WHERE category = 'A'), \
              cat_b AS (SELECT price FROM items WHERE category = 'B') \
         SELECT price FROM cat_a UNION ALL SELECT price FROM cat_b",
    )
    .await;
    assert_eq!(rows.len(), 4);
    cleanup(&wal);
}

#[tokio::test]
async fn test_exists_subquery() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ex_items (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO ex_items VALUES (1, 'a'), (2, 'b')").await;
    exec_ok(
        &executor,
        "CREATE TABLE ex_orders (id INTEGER PRIMARY KEY, item_id INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO ex_orders VALUES (1, 1)").await;
    // EXISTS: items that have orders
    let (_, rows) = query(
        &executor,
        "SELECT name FROM ex_items WHERE EXISTS (SELECT 1 FROM ex_orders WHERE item_id = 1)",
    )
    .await;
    assert_eq!(rows.len(), 2); // EXISTS is not correlated, so all rows match
                               // NOT EXISTS with empty result
    let (_, rows) = query(
        &executor,
        "SELECT name FROM ex_items WHERE NOT EXISTS (SELECT 1 FROM ex_orders WHERE item_id = 999)",
    )
    .await;
    assert_eq!(rows.len(), 2); // NOT EXISTS on empty = true, all rows match
                               // NOT EXISTS with non-empty result
    let (_, rows) = query(
        &executor,
        "SELECT name FROM ex_items WHERE NOT EXISTS (SELECT 1 FROM ex_orders WHERE item_id = 1)",
    )
    .await;
    assert_eq!(rows.len(), 0); // NOT EXISTS on non-empty = false
    cleanup(&wal);
}
