use fusiondb::common::Value;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

#[tokio::test]
async fn test_arithmetic_expression() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER, qty INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO items VALUES (1, 100, 5)").await;
    let (_, rows) = query(&executor, "SELECT price * qty FROM items WHERE id = 1").await;
    assert_eq!(rows[0][0], Value::Integer(500));
    cleanup(&wal);
}

#[tokio::test]
async fn test_json_literal_preserves_nested_arrays_and_objects() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT '{\"items\":[1,2,3],\"meta\":{\"ok\":true}}'",
    )
    .await;

    let mut meta = std::collections::HashMap::new();
    meta.insert("ok".to_string(), Value::Boolean(true));
    let mut expected = std::collections::HashMap::new();
    expected.insert(
        "items".to_string(),
        Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]),
    );
    expected.insert("meta".to_string(), Value::Object(meta));

    assert_eq!(rows, vec![vec![Value::Object(expected)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_timestamp_interval_arithmetic_matches_ldbc_q4_shape() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT
            TIMESTAMP '2010-06-01 00:00:00' + INTERVAL '1 days' * 29,
            TIMESTAMP '2010-06-01 00:00:00' - INTERVAL '1 days',
            INTERVAL '2 days' / 2",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::timestamp_from_str("2010-06-30 00:00:00").unwrap(),
            Value::timestamp_from_str("2010-05-31 00:00:00").unwrap(),
            Value::Interval(86_400_000_000),
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_parameterized_timestamp_interval_arithmetic() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));
    let stmts = executor
        .prepare("SELECT $1 + INTERVAL '1 days' * $2")
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(
            &stmts[0],
            txn.as_mut(),
            &[
                Value::timestamp_from_str("2010-06-01 00:00:00").unwrap(),
                Value::Integer(29),
            ],
        )
        .await
        .unwrap();

    if let QueryResult::Select { rows, .. } = result {
        assert_eq!(
            rows,
            vec![vec![
                Value::timestamp_from_str("2010-06-30 00:00:00").unwrap()
            ]]
        );
    } else {
        panic!("Expected Select result from parameterized timestamp interval query");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_like_pattern() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE name LIKE 'Ali%'").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_not_equal() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 10)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums WHERE val != 10").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(20));
    cleanup(&wal);
}

// ==================== Index Tests ====================

#[tokio::test]
async fn test_parameter_placeholder_select_filter() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE placeholder_filter (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO placeholder_filter VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;

    let stmts = executor
        .prepare("SELECT name FROM placeholder_filter WHERE id = $1")
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(&stmts[0], txn.as_mut(), &[Value::Integer(2)])
        .await
        .unwrap();

    if let QueryResult::Select { columns, rows } = result {
        assert_eq!(columns, vec!["name"]);
        assert_eq!(rows, vec![vec![Value::String("Bob".to_string())]]);
    } else {
        panic!("Expected Select result from parameterized query");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_parameter_placeholder_match_against() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE placeholder_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO placeholder_docs VALUES (1, 'quick brown fox'), (2, 'quick blue hare'), (3, 'slow brown fox')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_placeholder_docs_body ON placeholder_docs (body) USING FTS",
    )
    .await;

    let stmts = executor
        .prepare("SELECT id FROM placeholder_docs WHERE MATCH(body) AGAINST($1)")
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(
            &stmts[0],
            txn.as_mut(),
            &[Value::String("quick fox".to_string())],
        )
        .await
        .unwrap();

    if let QueryResult::Select { columns, rows } = result {
        assert_eq!(columns, vec!["id"]);
        assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    } else {
        panic!("Expected Select result from parameterized MATCH query");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_case_when_searched() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE grades (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO grades VALUES (1, 95), (2, 72), (3, 45)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'F' END FROM grades ORDER BY id").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::String("A".to_string()));
    assert_eq!(rows[1][0], fusiondb::common::Value::String("B".to_string()));
    assert_eq!(rows[2][0], fusiondb::common::Value::String("F".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_string_functions() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT UPPER('hello')").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("HELLO".to_string())
    );
    let (_, rows) = query(&executor, "SELECT LOWER('WORLD')").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("world".to_string())
    );
    let (_, rows) = query(&executor, "SELECT LENGTH('test')").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(4));
    let (_, rows) = query(&executor, "SELECT CONCAT('a', 'b', 'c')").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("abc".to_string())
    );
    let (_, rows) = query(&executor, "SELECT SUBSTRING('abcdef', 2, 3)").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("bcd".to_string())
    );
    let (_, rows) = query(&executor, "SELECT SUBSTRING('a\u{e9}bc', 2, 2)").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("\u{e9}b".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_ascii_function_matches_chbenchmark_q8_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ascii_inputs (id INTEGER PRIMARY KEY, c_state TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO ascii_inputs VALUES (1, 'Germany'), (2, ''), (3, NULL)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT ASCII(substring(c_state from 1 for 1)) FROM ascii_inputs ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(71)],
            vec![Value::Null],
            vec![Value::Null]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_coalesce_nullif() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT COALESCE(NULL, NULL, 42)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(42));
    let (_, rows) = query(&executor, "SELECT NULLIF(1, 1)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Null);
    let (_, rows) = query(&executor, "SELECT NULLIF(1, 2)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
    cleanup(&wal);
}

#[tokio::test]
async fn test_abs_round() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT ABS(-42)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(42));
    cleanup(&wal);
}

#[tokio::test]
async fn test_cast_expressions() {
    let (executor, wal) = setup().await;
    // CAST string to integer
    let (_, rows) = query(&executor, "SELECT CAST('42' AS INTEGER)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(42));
    // CAST integer to text
    let (_, rows) = query(&executor, "SELECT CAST(123 AS TEXT)").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("123".to_string())
    );
    // CAST float to integer
    let (_, rows) = query(&executor, "SELECT CAST(3.7 AS INTEGER)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(3));
    // CAST integer to float
    let (_, rows) = query(&executor, "SELECT CAST(5 AS FLOAT)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Float(5.0));
    // CAST integer to boolean
    let (_, rows) = query(&executor, "SELECT CAST(1 AS BOOLEAN)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Boolean(true));
    let (_, rows) = query(&executor, "SELECT CAST('2024-01-31' AS DATE)").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::date_from_str("2024-01-31").unwrap()
    );
    let (_, rows) = query(&executor, "SELECT CAST('2024-01-31 12:30:45' AS TIMESTAMP)").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::timestamp_from_str("2024-01-31 12:30:45").unwrap()
    );
    let (_, rows) = query(
        &executor,
        "SELECT CAST('2016-01-01 03:33:11.947779 +0000' AS TIMESTAMP)",
    )
    .await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::timestamp_from_str("2016-01-01 03:33:11.947779").unwrap()
    );
    let (_, rows) = query(&executor, "SELECT CAST('123.4500' AS DECIMAL(10, 4))").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::Decimal("123.45".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_postgres_timestamp_functions_for_tsbs_queries() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT EXTRACT(EPOCH FROM TIMESTAMP '2016-01-01 03:33:11.947779 +0000')",
    )
    .await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::Float(1451619191.947779)
    );

    let (_, rows) = query(
        &executor,
        "SELECT to_timestamp(((EXTRACT(EPOCH FROM TIMESTAMP '2016-01-01 03:33:11.947779 +0000')::int)/60)*60)",
    )
    .await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::timestamp_from_str("2016-01-01 03:33:00").unwrap()
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_production_scalar_types_insert_compare_and_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE scalar_types (
            id INTEGER PRIMARY KEY,
            d DATE,
            ts TIMESTAMP,
            amount DECIMAL(12, 2),
            ratio NUMERIC,
            span INTERVAL
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO scalar_types VALUES
            (1, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', 10.50, CAST('1.2500' AS NUMERIC), INTERVAL '2 days'),
            (2, '2024-01-03', '2024-01-03 09:30:00', '9.75', '2.5', '3600')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id, d, ts, amount, ratio, span FROM scalar_types WHERE d >= DATE '2024-01-02' ORDER BY amount",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[0][1], Value::date_from_str("2024-01-03").unwrap());
    assert_eq!(
        rows[0][2],
        Value::timestamp_from_str("2024-01-03 09:30:00").unwrap()
    );
    assert_eq!(rows[0][3], Value::Decimal("9.75".to_string()));
    assert_eq!(rows[0][4], Value::Decimal("2.5".to_string()));
    assert_eq!(rows[0][5], Value::Interval(3_600_000_000));

    exec_ok(
        &executor,
        "UPDATE scalar_types SET amount = amount + CAST('1.25' AS DECIMAL), d = '2024-01-04' WHERE id = 2",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT d, amount FROM scalar_types WHERE amount = CAST('11.00' AS NUMERIC)",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::date_from_str("2024-01-04").unwrap());
    assert_eq!(rows[0][1], Value::Decimal("11".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_string_concat_operator() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT 'hello' || ' ' || 'world'").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("hello world".to_string())
    );
    // Concat with integer
    let (_, rows) = query(&executor, "SELECT 'id=' || 42").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("id=42".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_ilike() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE il (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO il VALUES (1, 'Alice'), (2, 'ALICE'), (3, 'Bob')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT name FROM il WHERE name ILIKE 'alice'").await;
    assert_eq!(rows.len(), 2); // Alice and ALICE
    let (_, rows) = query(
        &executor,
        "SELECT name FROM il WHERE name NOT ILIKE 'alice'",
    )
    .await;
    assert_eq!(rows.len(), 1); // Bob
    cleanup(&wal);
}

#[tokio::test]
async fn test_like_full_patterns() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE lp (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO lp VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Alicia')",
    )
    .await;
    // Prefix pattern
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE 'Ali%'").await;
    assert_eq!(rows.len(), 2); // Alice, Alicia
                               // Suffix pattern
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE '%ce'").await;
    assert_eq!(rows.len(), 1); // Alice
                               // Contains pattern
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE '%li%'").await;
    assert_eq!(rows.len(), 3); // Alice, Charlie, Alicia
                               // Single char wildcard
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE 'Bo_'").await;
    assert_eq!(rows.len(), 1); // Bob
    cleanup(&wal);
}

#[tokio::test]
async fn test_indexed_like_complex_prefix_keeps_residual_filter() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE indexed_like_prefix (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_indexed_like_prefix_name ON indexed_like_prefix (name)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO indexed_like_prefix VALUES (1, 'abc'), (2, 'abzz'), (3, 'abdc'), (4, 'xbc')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id FROM indexed_like_prefix WHERE name LIKE 'ab%c'",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_coalesce_multi_arg() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE co (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO co VALUES (1, NULL, NULL, 'third')").await;
    exec_ok(
        &executor,
        "INSERT INTO co VALUES (2, NULL, 'second', 'third')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO co VALUES (3, 'first', 'second', 'third')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT COALESCE(a, b, c) FROM co ORDER BY id").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("third".to_string())
    );
    assert_eq!(
        rows[1][0],
        fusiondb::common::Value::String("second".to_string())
    );
    assert_eq!(
        rows[2][0],
        fusiondb::common::Value::String("first".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_math_functions() {
    let (executor, wal) = setup().await;
    // CEIL / FLOOR
    let (_, rows) = query(&executor, "SELECT CEIL(3.2), FLOOR(3.8)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(4));
    assert_eq!(rows[0][1], fusiondb::common::Value::Integer(3));
    // MOD
    let (_, rows) = query(&executor, "SELECT MOD(10, 3)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
    // POWER / SQRT
    let (_, rows) = query(&executor, "SELECT POWER(2, 3), SQRT(16)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Float(8.0));
    assert_eq!(rows[0][1], fusiondb::common::Value::Float(4.0));
    cleanup(&wal);
}

#[tokio::test]
async fn test_trim_variants() {
    let (executor, wal) = setup().await;
    // Default TRIM removes leading and trailing whitespace.
    let (_, rows) = query(&executor, "SELECT TRIM('  hello  ')").await;
    assert_eq!(rows[0][0], Value::String("hello".to_string()));
    // BOTH / LEADING / TRAILING with an explicit character set.
    let (_, rows) = query(&executor, "SELECT TRIM(BOTH 'x' FROM 'xxhixx')").await;
    assert_eq!(rows[0][0], Value::String("hi".to_string()));
    let (_, rows) = query(&executor, "SELECT TRIM(LEADING 'x' FROM 'xxhixx')").await;
    assert_eq!(rows[0][0], Value::String("hixx".to_string()));
    let (_, rows) = query(&executor, "SELECT TRIM(TRAILING 'x' FROM 'xxhixx')").await;
    assert_eq!(rows[0][0], Value::String("xxhi".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_position_function() {
    let (executor, wal) = setup().await;
    // Hit: 1-based index of the first match.
    let (_, rows) = query(&executor, "SELECT POSITION('cd' IN 'abcdef')").await;
    assert_eq!(rows[0][0], Value::Integer(3));
    // Miss: returns 0.
    let (_, rows) = query(&executor, "SELECT POSITION('zz' IN 'abcdef')").await;
    assert_eq!(rows[0][0], Value::Integer(0));
    cleanup(&wal);
}

#[tokio::test]
async fn test_greatest_least() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT GREATEST(3, 7, 1, 5)").await;
    assert_eq!(rows[0][0], Value::Integer(7));
    let (_, rows) = query(&executor, "SELECT LEAST(3, 7, 1, 5)").await;
    assert_eq!(rows[0][0], Value::Integer(1));
    // NULL arguments are ignored.
    let (_, rows) = query(&executor, "SELECT GREATEST(3, NULL, 9, NULL)").await;
    assert_eq!(rows[0][0], Value::Integer(9));
    let (_, rows) = query(&executor, "SELECT LEAST(NULL, 4, 2)").await;
    assert_eq!(rows[0][0], Value::Integer(2));
    // All-NULL arguments yield NULL.
    let (_, rows) = query(&executor, "SELECT GREATEST(NULL, NULL)").await;
    assert_eq!(rows[0][0], Value::Null);
    cleanup(&wal);
}

#[tokio::test]
async fn test_extract_quarter_and_week() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2024-08-15 00:00:00')",
    )
    .await;
    assert_eq!(rows[0][0], Value::Integer(3));
    // 2024-01-01 is a Monday, so it falls in ISO week 1.
    let (_, rows) = query(
        &executor,
        "SELECT EXTRACT(WEEK FROM TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;
    assert_eq!(rows[0][0], Value::Integer(1));
    cleanup(&wal);
}

#[tokio::test]
async fn test_now_function() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT NOW()").await;
    // NOW() returns unix epoch seconds as integer
    if let fusiondb::common::Value::Integer(ts) = &rows[0][0] {
        assert!(*ts > 1700000000); // After ~2023
    } else {
        panic!("NOW() should return Integer");
    }
    cleanup(&wal);
}
