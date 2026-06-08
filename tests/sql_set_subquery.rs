use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::execution::QueryResult;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::path::Path;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

fn write_copy_fixture(name: &str, content: &str) -> String {
    let path = std::env::temp_dir().join(format!("fusiondb_{}_{}.csv", name, uuid::Uuid::new_v4()));
    std::fs::write(&path, content).unwrap();
    path.to_string_lossy().replace('\\', "\\\\")
}

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
async fn test_union_all_with_empty_left_side() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_empty_left (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_empty_left_right (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_empty_left_right VALUES (1, 'a'), (2, 'b')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT name FROM union_empty_left UNION ALL SELECT name FROM union_empty_left_right",
    )
    .await;

    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("a".to_string())],
            vec![Value::String("b".to_string())],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_all_limit_offset_without_order_by() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_window_a (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_window_b (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_window_a VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_window_b VALUES (4, 'd'), (5, 'e'), (6, 'f')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT name FROM union_window_a UNION ALL SELECT name FROM union_window_b LIMIT 3 OFFSET 2",
    )
    .await;

    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("c".to_string())],
            vec![Value::String("d".to_string())],
            vec![Value::String("e".to_string())],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_parenthesized_union_all_query_body() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE paren_union (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO paren_union VALUES (1, 'a'), (2, 'b')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT name FROM ((SELECT name FROM paren_union WHERE id = 1)
         UNION ALL
         (SELECT name FROM paren_union WHERE id = 2)) u
         ORDER BY name",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::String("a".to_string()));
    assert_eq!(rows[1][0], Value::String("b".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_with_recursive_union_all_counter() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE r(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM r WHERE n < 3
         )
         SELECT n FROM r ORDER BY n",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_with_recursive_union_deduplicates_fixpoint() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE r(n) AS (
            SELECT 1
            UNION
            SELECT n + 1 FROM r WHERE n < 3
         )
         SELECT n FROM r WHERE n > 1 ORDER BY n",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_with_recursive_union_skips_seen_recursive_rows() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE r(n) AS (
            SELECT 1
            UNION
            SELECT CASE WHEN n < 3 THEN n + 1 ELSE n END FROM r WHERE n <= 3
         )
         SELECT n FROM r ORDER BY n",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_with_recursive_union_deduplicates_anchor_seen_rows() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE r(n) AS (
            SELECT 1
            UNION ALL
            SELECT 1
            UNION
            SELECT n + 1 FROM r WHERE n < 3
         )
         SELECT n FROM r ORDER BY n",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_recursive_cte_alias_can_rename_prefix_columns() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE r(link, depth, path) AS (
            SELECT 1, 0, 'root'
            UNION ALL
            SELECT link + 1, depth + 1, path FROM r WHERE link < 2
         ),
         sg(node, hops) AS (SELECT * FROM r)
         SELECT node, hops, path FROM sg ORDER BY node",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(0),
                Value::String("root".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::Integer(1),
                Value::String("root".to_string())
            ]
        ]
    );
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
async fn test_union_all_order_by_limit_offset_beyond_rows() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_empty_a (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_empty_b (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_empty_a VALUES (1, 50), (2, 10)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO union_empty_b VALUES (3, 40)").await;

    let (cols, rows) = query(
        &executor,
        "SELECT score FROM union_empty_a UNION ALL SELECT score FROM union_empty_b ORDER BY score ASC LIMIT 2 OFFSET 10",
    )
    .await;

    assert_eq!(cols, vec!["score"]);
    assert!(rows.is_empty());
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
async fn test_union_distinct_single_row() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_single_left (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_single_right (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_single_left VALUES (1, 'solo')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT name FROM union_single_left UNION SELECT name FROM union_single_right",
    )
    .await;

    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows, vec![vec![Value::String("solo".to_string())]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_distinct_with_duplicate_inputs() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_dup_left (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_dup_right (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_dup_left VALUES (1, 'a'), (2, 'b'), (3, 'a')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_dup_right VALUES (1, 'b'), (2, 'c'), (3, 'c')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT name FROM union_dup_left UNION SELECT name FROM union_dup_right ORDER BY name",
    )
    .await;

    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("a".to_string())],
            vec![Value::String("b".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );
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
async fn test_intersect_except_with_duplicate_right_rows() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE set_left_dup (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE set_right_dup (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO set_left_dup VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO set_right_dup VALUES (1, 'b'), (2, 'b'), (3, 'd')",
    )
    .await;

    let (_, intersect_rows) = query(
        &executor,
        "SELECT name FROM set_left_dup INTERSECT SELECT name FROM set_right_dup ORDER BY name",
    )
    .await;
    assert_eq!(
        intersect_rows,
        vec![
            vec![Value::String("b".to_string())],
            vec![Value::String("d".to_string())],
        ]
    );

    let (_, except_rows) = query(
        &executor,
        "SELECT name FROM set_left_dup EXCEPT SELECT name FROM set_right_dup ORDER BY name",
    )
    .await;
    assert_eq!(
        except_rows,
        vec![
            vec![Value::String("a".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_intersect_except_with_all_left_rows_matched() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE set_left_all_match (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE set_right_all_match (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO set_left_all_match VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO set_right_all_match VALUES (1, 'c'), (2, 'b'), (3, 'a')",
    )
    .await;

    let (_, intersect_rows) = query(
        &executor,
        "SELECT name FROM set_left_all_match INTERSECT SELECT name FROM set_right_all_match ORDER BY name",
    )
    .await;
    assert_eq!(
        intersect_rows,
        vec![
            vec![Value::String("a".to_string())],
            vec![Value::String("b".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );

    let (_, except_rows) = query(
        &executor,
        "SELECT name FROM set_left_all_match EXCEPT SELECT name FROM set_right_all_match",
    )
    .await;
    assert!(except_rows.is_empty());
    cleanup(&wal);
}

#[tokio::test]
async fn test_intersect_except_with_empty_right_side() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE set_left_empty_right (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE set_right_empty (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO set_left_empty_right VALUES (1, 'a'), (2, 'b'), (3, 'a')",
    )
    .await;

    let (_, intersect_rows) = query(
        &executor,
        "SELECT name FROM set_left_empty_right INTERSECT SELECT name FROM set_right_empty",
    )
    .await;
    assert!(intersect_rows.is_empty());

    let (_, except_rows) = query(
        &executor,
        "SELECT name FROM set_left_empty_right EXCEPT SELECT name FROM set_right_empty ORDER BY name",
    )
    .await;
    assert_eq!(
        except_rows,
        vec![
            vec![Value::String("a".to_string())],
            vec![Value::String("b".to_string())],
        ]
    );
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

#[tokio::test]
async fn test_correlated_not_exists_against_cte() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE outer_items (id INTEGER PRIMARY KEY, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO outer_items VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "WITH vals(v) AS (SELECT 1 UNION ALL SELECT 2)
         SELECT id FROM outer_items
         WHERE NOT EXISTS (SELECT * FROM vals y WHERE y.v = outer_items.id)
         ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(3)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_correlated_not_exists_with_join_alias_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE knows (id INTEGER PRIMARY KEY, person1 INTEGER, person2 INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO knows VALUES (1, 10, 20), (2, 10, 30), (3, 20, 40)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "WITH sg(link) AS (SELECT 10 UNION ALL SELECT 20)
         SELECT r.person2
         FROM knows r, sg x
         WHERE x.link = r.person1
           AND NOT EXISTS (SELECT * FROM sg y WHERE y.link = r.person2)
         ORDER BY r.person2",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![Value::Integer(30)], vec![Value::Integer(40)]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_correlated_not_exists_filters_before_limit() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE limited_outer (id INTEGER PRIMARY KEY, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO limited_outer VALUES (1, 'filtered'), (2, 'kept'), (3, 'extra')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "WITH vals(v) AS (SELECT 1)
         SELECT id FROM limited_outer
         WHERE NOT EXISTS (SELECT * FROM vals y WHERE y.v = limited_outer.id)
         ORDER BY id
         LIMIT 1",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_recursive_cte_row_budget_fails_fast() {
    let (executor, wal) = setup().await;
    let stmts = executor
        .prepare(
            "WITH RECURSIVE r(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM r
             )
             SELECT n FROM r",
        )
        .unwrap();
    let err = executor.execute(&stmts[0]).await.unwrap_err();
    assert!(
        err.to_string().contains("iteration limit exceeded")
            || err.to_string().contains("row limit exceeded"),
        "unexpected error: {err}"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_recursive_cte_preserves_array_concat_values() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE path_cte(path, depth) AS (
            SELECT ARRAY[]::bigint[][], 0
            UNION ALL
            SELECT path || ARRAY[[depth, depth + 1]], depth + 1 FROM path_cte WHERE depth < 2
         )
         SELECT path FROM path_cte WHERE depth = 2",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![Value::Array(vec![
            Value::Array(vec![Value::Integer(0), Value::Integer(1)]),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
        ])]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_quantified_array_comparison_predicates() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT
             1 <> ALL (ARRAY[2, 3]),
             1 = ANY (ARRAY[2, 1]),
             1 = ANY (ARRAY[2, 3]),
             1 <> ALL (ARRAY[1, 2])",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false)
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_recursive_cte_q13_shape_preserves_path_for_all_predicate() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE q13_knows (p1 BIGINT, p2 BIGINT)").await;
    exec_ok(
        &executor,
        "INSERT INTO q13_knows VALUES (1, 2), (2, 3), (2, 1), (3, 4)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE search_graph(link, depth, path) AS (
            SELECT 1::bigint, 0, ARRAY[1::bigint]::bigint[]
            UNION ALL
            (
                WITH sg(link, depth) AS (SELECT * FROM search_graph)
                SELECT DISTINCT p2, x.depth + 1, array_append(path, p2)
                FROM q13_knows, sg x
                WHERE x.link = p1
                  AND p2 <> ALL (path)
                  AND NOT EXISTS (SELECT * FROM sg y WHERE y.link = 4::bigint)
                  AND NOT EXISTS (SELECT * FROM sg y WHERE y.link = p2)
            )
         )
         SELECT max(depth) FROM (
            SELECT depth FROM search_graph WHERE link = 4::bigint
            UNION SELECT -1
         ) tmp",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(3)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_correlated_not_exists_membership_filter_with_alias() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE sg_membership (link BIGINT)").await;
    exec_ok(&executor, "CREATE TABLE candidate_membership (link BIGINT)").await;
    exec_ok(&executor, "INSERT INTO sg_membership VALUES (2), (4)").await;
    exec_ok(
        &executor,
        "INSERT INTO candidate_membership VALUES (1), (2), (3), (4)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT c.link
         FROM candidate_membership c
         WHERE NOT EXISTS (
            SELECT * FROM sg_membership y WHERE y.link = c.link
         )
         ORDER BY c.link",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_correlated_exists_two_table_membership_matches_ldbc_q6_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE q6_tag (t_tagid BIGINT, t_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE q6_message_tag (mt_messageid BIGINT, mt_tagid BIGINT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE q6_candidate (m_messageid BIGINT, candidate_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q6_tag VALUES (1, 'target'), (2, 'other')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q6_message_tag VALUES (10, 1), (11, 2), (12, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q6_candidate VALUES (10, 'hit-a'), (11, 'miss'), (12, 'hit-b')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT candidate_name
         FROM q6_candidate c
         WHERE EXISTS (
            SELECT *
            FROM q6_tag, q6_message_tag
            WHERE mt_messageid = c.m_messageid
              AND mt_tagid = t_tagid
              AND t_name = 'target'
         )
         ORDER BY candidate_name",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::String("hit-a".to_string())],
            vec![Value::String("hit-b".to_string())],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_generate_subscripts_from_array_literal() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT d1 FROM generate_subscripts(ARRAY[[10, 11], [20, 21]], 1) d1 ORDER BY d1",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_generate_subscripts_depends_on_left_row_array() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH paths(path) AS (SELECT ARRAY[[10, 11], [20, 21]])
         SELECT d1, d2, path[d1][d2]
         FROM paths, generate_subscripts(path, 1) d1, generate_subscripts(path, 2) d2
         ORDER BY d1, d2",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(11)],
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(20)],
            vec![Value::Integer(2), Value::Integer(2), Value::Integer(21)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_array_agg_over_generated_subscripts() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH paths(pid, path) AS (SELECT 1, ARRAY[[10, 11], [20, 21]])
         SELECT pid, d1, array_agg(path[d1][d2])
         FROM paths, generate_subscripts(path, 1) d1, generate_subscripts(path, 2) d2
         GROUP BY pid, d1
         ORDER BY pid, d1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::Array(vec![Value::Integer(10), Value::Integer(11)])
            ],
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Array(vec![Value::Integer(20), Value::Integer(21)])
            ],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_array_agg_over_array_expression_preserves_nested_values() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH orgs(name, since_year, place) AS (
             SELECT 'University A', 2001, 'City A'
             UNION ALL
             SELECT 'University B', 2002, 'City B'
         )
         SELECT array_agg(ARRAY[name, since_year::text, place])
         FROM orgs",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![Value::Array(vec![
            Value::Array(vec![
                Value::String("University A".to_string()),
                Value::String("2001".to_string()),
                Value::String("City A".to_string()),
            ]),
            Value::Array(vec![
                Value::String("University B".to_string()),
                Value::String("2002".to_string()),
                Value::String("City B".to_string()),
            ]),
        ])]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_can_project_array_path_expression() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH paths(pid, path) AS (SELECT 1, ARRAY[[10, 11], [20, 21]])
         SELECT path, count(*)
         FROM paths
         GROUP BY pid, path",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Array(vec![
                Value::Array(vec![Value::Integer(10), Value::Integer(11)]),
                Value::Array(vec![Value::Integer(20), Value::Integer(21)]),
            ]),
            Value::Integer(1)
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_projection_can_coalesce_aggregate() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "WITH scores(path, score) AS (
            SELECT ARRAY[[10, 11]], 1
            UNION ALL
            SELECT ARRAY[[10, 11]], 2
         )
         SELECT path, coalesce(sum(score), 0)
         FROM scores
         GROUP BY path",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Array(vec![Value::Array(vec![
                Value::Integer(10),
                Value::Integer(11)
            ])]),
            Value::Integer(3)
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_projection_can_materialize_correlated_scalar_array_subquery() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE q1_person (id INTEGER PRIMARY KEY, last_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE q1_email (person_id INTEGER, email TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q1_person VALUES (1, 'Smith'), (2, 'Jones')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q1_email VALUES
            (1, 'a@example.test'),
            (1, 'b@example.test'),
            (2, 'c@example.test')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT
             id,
             last_name,
             (SELECT array_agg(email) FROM q1_email WHERE person_id = id GROUP BY person_id) AS emails
         FROM q1_person
         GROUP BY id, last_name
         ORDER BY id",
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::String("Smith".to_string()));
    let Value::Array(emails) = &rows[0][2] else {
        panic!("expected emails array for first grouped row");
    };
    assert_eq!(emails.len(), 2);
    assert!(emails.contains(&Value::String("a@example.test".to_string())));
    assert!(emails.contains(&Value::String("b@example.test".to_string())));

    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[1][1], Value::String("Jones".to_string()));
    assert_eq!(
        rows[1][2],
        Value::Array(vec![Value::String("c@example.test".to_string())])
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_ldbc_short_query6_scalar_min_coalesce_for_post_message() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE message (
            m_messageid BIGINT PRIMARY KEY,
            m_c_replyof BIGINT,
            m_ps_forumid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE forum (
            f_forumid BIGINT PRIMARY KEY,
            f_title TEXT,
            f_moderatorid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE person (
            p_personid BIGINT PRIMARY KEY,
            p_firstname TEXT,
            p_lastname TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message VALUES (343597386489, NULL, 343597384000)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO forum VALUES (343597384000, 'Album 5 of Jose Pereira', 4398046511183)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO person VALUES (4398046511183, 'Jose', 'Pereira')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE chain(parent, child) AS (
            SELECT m_c_replyof, m_messageid FROM message WHERE m_messageid = 343597386489
            UNION ALL
            SELECT p.m_c_replyof, p.m_messageid FROM message p, chain c WHERE p.m_messageid = c.parent
         )
         SELECT f_forumid, f_title, p_personid, p_firstname, p_lastname
         FROM message, person, forum
         WHERE m_messageid = (SELECT coalesce(min(parent), 343597386489) FROM chain)
           AND m_ps_forumid = f_forumid
           AND f_moderatorid = p_personid",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(343597384000),
            Value::String("Album 5 of Jose Pereira".to_string()),
            Value::Integer(4398046511183),
            Value::String("Jose".to_string()),
            Value::String("Pereira".to_string())
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_ldbc_short_query6_parameterized_scalar_min_coalesce() {
    let wal = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal).unwrap());
    let executor = Executor::new(storage.clone());
    exec_ok(
        &executor,
        "CREATE TABLE message (
            m_messageid BIGINT PRIMARY KEY,
            m_c_replyof BIGINT,
            m_ps_forumid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE forum (
            f_forumid BIGINT PRIMARY KEY,
            f_title TEXT,
            f_moderatorid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE person (
            p_personid BIGINT PRIMARY KEY,
            p_firstname TEXT,
            p_lastname TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message VALUES (343597386489, NULL, 343597384000)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO forum VALUES (343597384000, 'Album 5 of Jose Pereira', 4398046511183)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO person VALUES (4398046511183, 'Jose', 'Pereira')",
    )
    .await;

    let stmts = executor
        .prepare(
            "WITH RECURSIVE chain(parent, child) AS (
                SELECT m_c_replyof, m_messageid FROM message WHERE m_messageid = $1
                UNION ALL
                SELECT p.m_c_replyof, p.m_messageid FROM message p, chain c WHERE p.m_messageid = c.parent
             )
             SELECT f_forumid, f_title, p_personid, p_firstname, p_lastname
             FROM message, person, forum
             WHERE m_messageid = (SELECT coalesce(min(parent), $2) FROM chain)
               AND m_ps_forumid = f_forumid
               AND f_moderatorid = p_personid",
        )
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(
            &stmts[0],
            &mut *txn,
            &[Value::Integer(343597386489), Value::Integer(343597386489)],
        )
        .await
        .unwrap();

    let QueryResult::Select { rows, .. } = result else {
        panic!("expected SELECT result");
    };
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(343597384000),
            Value::String("Album 5 of Jose Pereira".to_string()),
            Value::Integer(4398046511183),
            Value::String("Jose".to_string()),
            Value::String("Pereira".to_string())
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_ldbc_short_query6_after_post_to_message_derivation() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE post (
            m_messageid BIGINT,
            m_ps_imagefile TEXT,
            m_creationdate TIMESTAMP,
            m_locationip TEXT,
            m_browserused TEXT,
            m_ps_language TEXT,
            m_content TEXT,
            m_length INT,
            m_creatorid BIGINT,
            m_ps_forumid BIGINT,
            m_locationid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE message (
            m_messageid BIGINT,
            m_ps_imagefile TEXT,
            m_creationdate TIMESTAMP,
            m_locationip TEXT,
            m_browserused TEXT,
            m_ps_language TEXT,
            m_content TEXT,
            m_length INT,
            m_creatorid BIGINT,
            m_locationid BIGINT,
            m_ps_forumid BIGINT,
            m_c_replyof BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE forum (
            f_forumid BIGINT,
            f_title TEXT,
            f_creationdate TIMESTAMP,
            f_moderatorid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE person (
            p_personid BIGINT,
            p_firstname TEXT,
            p_lastname TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO post VALUES (
            343597386489,
            'photo343597386489.jpg',
            TIMESTAMP '2010-11-02 07:46:20.379',
            '193.136.95.244',
            'Firefox',
            NULL,
            NULL,
            0,
            4398046511183,
            343597384000,
            93
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO forum VALUES (
            343597384000,
            'Album 5 of Jose Pereira',
            TIMESTAMP '2010-11-02 07:45:50.379',
            4398046511183
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO person VALUES (4398046511183, 'Jose', 'Pereira')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message
         SELECT
            m_messageid,
            m_ps_imagefile,
            m_creationdate,
            m_locationip,
            m_browserused,
            m_ps_language,
            m_content,
            m_length,
            m_creatorid,
            m_locationid,
            m_ps_forumid,
            NULL AS m_c_replyof
         FROM post",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE chain(parent, child) AS (
            SELECT m_c_replyof, m_messageid FROM message WHERE m_messageid = 343597386489
            UNION ALL
            SELECT p.m_c_replyof, p.m_messageid FROM message p, chain c WHERE p.m_messageid = c.parent
         )
         SELECT f_forumid, f_title, p_personid, p_firstname, p_lastname
         FROM message, person, forum
         WHERE m_messageid = (SELECT coalesce(min(parent), 343597386489) FROM chain)
           AND m_ps_forumid = f_forumid
           AND f_moderatorid = p_personid",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(343597384000),
            Value::String("Album 5 of Jose Pereira".to_string()),
            Value::Integer(4398046511183),
            Value::String("Jose".to_string()),
            Value::String("Pereira".to_string())
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_ldbc_short_query6_after_post_copy_with_empty_null_fields() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE post (
            m_messageid BIGINT,
            m_ps_imagefile TEXT,
            m_creationdate TIMESTAMPTZ,
            m_locationip TEXT,
            m_browserused TEXT,
            m_ps_language TEXT,
            m_content TEXT,
            m_length INT,
            m_creatorid BIGINT,
            m_ps_forumid BIGINT,
            m_locationid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE message (
            m_messageid BIGINT,
            m_ps_imagefile TEXT,
            m_creationdate TIMESTAMPTZ,
            m_locationip TEXT,
            m_browserused TEXT,
            m_ps_language TEXT,
            m_content TEXT,
            m_length INT,
            m_creatorid BIGINT,
            m_locationid BIGINT,
            m_ps_forumid BIGINT,
            m_c_replyof BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE forum (
            f_forumid BIGINT,
            f_title TEXT,
            f_creationdate TIMESTAMPTZ,
            f_moderatorid BIGINT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE person (
            p_personid BIGINT,
            p_firstname TEXT,
            p_lastname TEXT
        )",
    )
    .await;

    let post_csv = write_copy_fixture(
        "ldbc_post",
        "id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|forum|place
343597386489|photo343597386489.jpg|2010-11-02T07:46:20.379+0000|193.136.95.244|Firefox|||0|4398046511183|343597384000|93
",
    );
    let copy_sql = format!(
        "COPY post FROM '{}' WITH (FORMAT CSV, HEADER true, DELIMITER '|', NULL '')",
        post_csv
    );
    exec_ok(&executor, &copy_sql).await;
    exec_ok(
        &executor,
        "INSERT INTO forum VALUES (
            343597384000,
            'Album 5 of Jose Pereira',
            TIMESTAMPTZ '2010-11-02 07:45:50.379+00',
            4398046511183
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO person VALUES (4398046511183, 'Jose', 'Pereira')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message
         SELECT
            m_messageid,
            m_ps_imagefile,
            m_creationdate,
            m_locationip,
            m_browserused,
            m_ps_language,
            m_content,
            m_length,
            m_creatorid,
            m_locationid,
            m_ps_forumid,
            NULL AS m_c_replyof
         FROM post",
    )
    .await;

    let (_, copied) = query(
        &executor,
        "SELECT m_messageid, m_ps_language, m_content, m_length, m_creatorid, m_ps_forumid, m_locationid
         FROM post",
    )
    .await;
    assert_eq!(
        copied,
        vec![vec![
            Value::Integer(343597386489),
            Value::Null,
            Value::Null,
            Value::Integer(0),
            Value::Integer(4398046511183),
            Value::Integer(343597384000),
            Value::Integer(93)
        ]]
    );

    let (_, rows) = query(
        &executor,
        "WITH RECURSIVE chain(parent, child) AS (
            SELECT m_c_replyof, m_messageid FROM message WHERE m_messageid = 343597386489
            UNION ALL
            SELECT p.m_c_replyof, p.m_messageid FROM message p, chain c WHERE p.m_messageid = c.parent
         )
         SELECT f_forumid, f_title, p_personid, p_firstname, p_lastname
         FROM message, person, forum
         WHERE m_messageid = (SELECT coalesce(min(parent), 343597386489) FROM chain)
           AND m_ps_forumid = f_forumid
           AND f_moderatorid = p_personid",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(343597384000),
            Value::String("Album 5 of Jose Pereira".to_string()),
            Value::Integer(4398046511183),
            Value::String("Jose".to_string()),
            Value::String("Pereira".to_string())
        ]]
    );
    let _ = std::fs::remove_file(Path::new(&post_csv));
    cleanup(&wal);
}
