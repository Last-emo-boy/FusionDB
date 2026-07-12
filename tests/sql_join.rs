use fusiondb::common::Value;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

fn corrupt_encoded_column_from(row: &mut [u8], column_index: usize) {
    let off_pos = 2 + column_index * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut row[start..] {
        *byte = 0xff;
    }
}

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
async fn test_inner_join_stats_reorder_preserves_wildcard_column_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE inner_reorder_big (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE inner_reorder_mid (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE inner_reorder_small (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO inner_reorder_big VALUES (1, 1), (2, 1), (3, 1), (4, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO inner_reorder_mid VALUES (1, 1), (2, 1)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO inner_reorder_small VALUES (1, 1)").await;
    exec_ok(
        &executor,
        "ANALYZE TABLE inner_reorder_big COMPUTE STATISTICS",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE inner_reorder_mid COMPUTE STATISTICS",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE inner_reorder_small COMPUTE STATISTICS",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT *
           FROM inner_reorder_big
           INNER JOIN inner_reorder_mid
             ON inner_reorder_big.join_key = inner_reorder_mid.join_key
           INNER JOIN inner_reorder_small
             ON inner_reorder_mid.join_key = inner_reorder_small.join_key
          WHERE inner_reorder_small.join_key = 1
          ORDER BY inner_reorder_big.id, inner_reorder_mid.id, inner_reorder_small.id",
    )
    .await;

    assert_eq!(
        cols,
        vec![
            "inner_reorder_big.id",
            "inner_reorder_big.join_key",
            "inner_reorder_mid.id",
            "inner_reorder_mid.join_key",
            "inner_reorder_small.id",
            "inner_reorder_small.join_key"
        ]
    );
    assert_eq!(rows.len(), 8);
    assert_eq!(
        rows[0],
        vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1)
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_order_by_prefers_projected_column_over_ambiguous_join_input() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE q7_tmp (person_id INTEGER, l_creationdate TIMESTAMP)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE q7_likes (
            l_personid INTEGER,
            l_messageid INTEGER,
            l_creationdate TIMESTAMP
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q7_tmp VALUES
            (1, TIMESTAMP '2024-01-03 00:00:00'),
            (2, TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO q7_likes VALUES
            (1, 10, TIMESTAMP '2024-01-03 00:00:00'),
            (2, 20, TIMESTAMP '2024-01-01 00:00:00')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT q7_likes.l_personid, q7_likes.l_creationdate
         FROM q7_tmp, q7_likes
         WHERE q7_tmp.person_id = q7_likes.l_personid
           AND q7_tmp.l_creationdate = q7_likes.l_creationdate
         ORDER BY l_creationdate DESC, l_personid ASC",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::timestamp_from_str("2024-01-03 00:00:00").unwrap()
            ],
            vec![
                Value::Integer(2),
                Value::timestamp_from_str("2024-01-01 00:00:00").unwrap()
            ],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE tag (t_tagid BIGINT PRIMARY KEY, t_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE message (
            m_messageid BIGINT PRIMARY KEY,
            m_creatorid BIGINT,
            m_c_replyof BIGINT,
            m_creationdate TIMESTAMP
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE message_tag (mt_messageid BIGINT, mt_tagid BIGINT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE knows (k_person1id BIGINT, k_person2id BIGINT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_q4_mt_message ON message_tag (mt_messageid)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_q4_mt_tag ON message_tag (mt_tagid)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_knows_person1 ON knows (k_person1id)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_knows_person2 ON knows (k_person2id)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_message_creator ON message (m_creatorid)",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO tag VALUES (1, 'old-topic'), (2, 'new-topic'), (3, 'other-new')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO knows VALUES (100, 200), (100, 201), (999, 200)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message VALUES
            (10, 200, NULL, TIMESTAMP '2010-09-15 00:00:00'),
            (11, 200, NULL, TIMESTAMP '2010-10-05 00:00:00'),
            (12, 201, NULL, TIMESTAMP '2010-10-06 00:00:00'),
            (13, 201, NULL, TIMESTAMP '2010-10-07 00:00:00'),
            (14, 201, 11, TIMESTAMP '2010-10-08 00:00:00'),
            (15, 999, NULL, TIMESTAMP '2010-10-09 00:00:00')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message_tag VALUES
            (10, 1),
            (11, 1),
            (12, 2),
            (13, 2),
            (13, 3),
            (14, 2),
            (15, 2)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT t_name, COUNT(*) AS postCount
         FROM tag, message, message_tag recent, knows
         WHERE
             m_messageid = mt_messageid AND
             mt_tagid = t_tagid AND
             m_creatorid = k_person2id AND
             m_c_replyof IS NULL AND
             k_person1id = 100 AND
             m_creationdate >= TIMESTAMP '2010-10-01 00:00:00' AND
             m_creationdate < (TIMESTAMP '2010-10-01 00:00:00' + INTERVAL '1 days' * 31) AND
             NOT EXISTS (
                 SELECT * FROM (
                     SELECT DISTINCT mt_tagid
                     FROM message, message_tag, knows
                     WHERE
                         k_person1id = 100 AND
                         k_person2id = m_creatorid AND
                         m_c_replyof IS NULL AND
                         mt_messageid = m_messageid AND
                         m_creationdate < TIMESTAMP '2010-10-01 00:00:00'
                 ) tags
                 WHERE tags.mt_tagid = recent.mt_tagid
             )
         GROUP BY t_name
         ORDER BY postCount DESC, t_name ASC
         LIMIT 10",
    )
    .await;

    assert_eq!(cols, vec!["t_name", "postCount"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("new-topic".to_string()), Value::Integer(2)],
            vec![Value::String("other-new".to_string()), Value::Integer(1)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_base_scan_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the join base scan must decode
    // the new bytes instead of serving the stale cached rows.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice-rewritten"), (2_i64, "Bob-rewritten")] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);

            let key = format!(
                "data:join_cache_users:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
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
    assert_eq!(rows[0][1], Value::String("Alice-rewritten".to_string()));
    assert_eq!(rows[1][1], Value::String("Bob-rewritten".to_string()));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_join_base_scan_populated_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the follow-up scan must decode
    // the new bytes instead of serving the rows cached by the join.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice-rewritten"), (2_i64, "Bob-rewritten")] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);

            let key = format!(
                "data:join_warm_users:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT * FROM join_warm_users").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice-rewritten".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob-rewritten".to_string())
            ]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_join_probe_aligns_projected_right_column_after_key() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE probe_left_align (id INTEGER PRIMARY KEY, right_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE probe_right_align (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO probe_left_align VALUES (1, 10), (2, 20)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO probe_right_align VALUES (10, 'Alice', 'unused-a'), (20, 'Bob', 'unused-b')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT l.id, r.name
         FROM probe_left_align l JOIN probe_right_align r ON l.right_id = r.id
         ORDER BY l.id",
    )
    .await;

    assert_eq!(cols, vec!["l.id", "r.name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_primary_key_join_probe_projection_right_row_cache_tracks_storage_bytes() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE probe_left_cache (id INTEGER PRIMARY KEY, right_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE probe_right_cache (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO probe_left_cache VALUES (1, 10)").await;
    exec_ok(
        &executor,
        "INSERT INTO probe_right_cache VALUES (10, 'Cached', 'unused')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM probe_right_cache WHERE id = 10").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(10),
            Value::String("Cached".to_string()),
            Value::String("unused".to_string())
        ]]
    );

    // Rewrite the stored bytes out of band: the primary-key join probe must
    // decode the new bytes instead of serving the stale cached right row.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(10),
            Value::String("Cached-rewritten".to_string()),
            Value::String("unused".to_string()),
        ]);

        let key = format!(
            "data:probe_right_cache:{}",
            fusiondb::common::encoding::encode_i64_comparable(10)
        );
        txn.put(key.as_bytes(), &updated_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT l.id, r.name
         FROM probe_left_cache l JOIN probe_right_cache r ON l.right_id = r.id",
    )
    .await;

    assert_eq!(cols, vec!["l.id", "r.name"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Cached-rewritten".to_string())
        ]]
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
async fn test_three_table_join_limit_only_applies_to_final_join_step() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE jl3_users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE jl3_orders (id INTEGER PRIMARY KEY, user_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE jl3_items (id INTEGER PRIMARY KEY, order_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO jl3_users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO jl3_orders VALUES (10, 99), (20, 2)").await;
    exec_ok(
        &executor,
        "INSERT INTO jl3_items VALUES (100, 10), (200, 20)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT u.name, o.id, i.id
           FROM jl3_users u
           INNER JOIN jl3_orders o ON u.id = o.user_id
           INNER JOIN jl3_items i ON o.id = i.order_id
          LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["u.name", "o.id", "i.id"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::String("Bob".to_string()),
            Value::Integer(20),
            Value::Integer(200),
        ]]
    );
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
async fn test_inner_join_limit_pushdown_with_local_left_predicate_stops_after_limit() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE jl_left_pred_l (id INTEGER PRIMARY KEY, keep INTEGER, right_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE jl_left_pred_r (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO jl_left_pred_l VALUES (1, 1, 1), (2, 1, 2)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO jl_left_pred_r VALUES (1, 'ok-first'), (2, 'bad-second')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::String("bad-second".to_string()),
        ]);
        corrupt_encoded_column_from(&mut row, 1);
        let key = format!(
            "data:jl_left_pred_r:{}",
            fusiondb::common::encoding::encode_i64_comparable(2)
        );
        txn.put(key.as_bytes(), &row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT l.id, r.payload
           FROM jl_left_pred_l l
           INNER JOIN jl_left_pred_r r ON l.right_id = r.id
          WHERE l.keep = 1
          LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["l.id", "r.payload"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("ok-first".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_inner_join_limit_pushdown_with_local_right_predicate_stops_after_limit() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE jl_right_pred_l (id INTEGER PRIMARY KEY, right_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE jl_right_pred_r (id INTEGER PRIMARY KEY, keep INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO jl_right_pred_l VALUES (1, 1), (2, 2)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO jl_right_pred_r VALUES (1, 1, 'ok-first'), (2, 1, 'bad-second')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(1),
            Value::String("bad-second".to_string()),
        ]);
        corrupt_encoded_column_from(&mut row, 2);
        let key = format!(
            "data:jl_right_pred_r:{}",
            fusiondb::common::encoding::encode_i64_comparable(2)
        );
        txn.put(key.as_bytes(), &row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT l.id, r.payload
           FROM jl_right_pred_l l
           INNER JOIN jl_right_pred_r r ON l.right_id = r.id
          WHERE r.keep = 1
          LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["l.id", "r.payload"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("ok-first".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_inner_join_limit_does_not_push_down_cross_table_where_predicate() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE jl_cross_pred_l (id INTEGER PRIMARY KEY, group_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE jl_cross_pred_r (id INTEGER PRIMARY KEY, group_id INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO jl_cross_pred_l VALUES (1, 1)").await;
    exec_ok(
        &executor,
        "INSERT INTO jl_cross_pred_r VALUES (1, 1), (2, 1)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT l.id, r.id
           FROM jl_cross_pred_l l
           INNER JOIN jl_cross_pred_r r ON l.group_id = r.group_id
          WHERE l.id < r.id
          LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["l.id", "r.id"]);
    assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(2)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_inner_join_limit_does_not_truncate_count_aggregate() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE jl_agg_l (id INTEGER PRIMARY KEY, right_id INTEGER)",
    )
    .await;
    exec_ok(&executor, "CREATE TABLE jl_agg_r (id INTEGER PRIMARY KEY)").await;
    exec_ok(
        &executor,
        "INSERT INTO jl_agg_l VALUES (1, 1), (2, 2), (3, 3)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO jl_agg_r VALUES (1), (2), (3)").await;

    let (cols, rows) = query(
        &executor,
        "SELECT COUNT(*)
           FROM jl_agg_l l
           INNER JOIN jl_agg_r r ON l.right_id = r.id
          LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["COUNT(*)"]);
    assert_eq!(rows, vec![vec![Value::Integer(3)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_comma_join_projection_keeps_right_columns_without_join_pushdown() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE join_keep_left (id INTEGER PRIMARY KEY, link INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE join_keep_right (id INTEGER PRIMARY KEY, person2 INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO join_keep_left VALUES (1, 10)").await;
    exec_ok(
        &executor,
        "INSERT INTO join_keep_right VALUES (1, 20), (2, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT person2
         FROM join_keep_right, join_keep_left x
         WHERE x.link = join_keep_right.id
            OR join_keep_right.id = 1",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(20)]]);
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
async fn test_join_primary_key_probe_restores_projected_key_without_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE social_knows_pk_probe (id INTEGER PRIMARY KEY, person1_id INTEGER, person2_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE social_person_pk_probe (id INTEGER PRIMARY KEY, first_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_social_knows_pk_probe_person1 ON social_knows_pk_probe (person1_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO social_knows_pk_probe VALUES (1, 1, 2), (2, 1, 3)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(2_i64, "Bob"), (3_i64, "Charlie")] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);
            let corrupt_col_idx = 0usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            let end =
                u32::from_le_bytes(row[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
            for byte in &mut row[start..end] {
                *byte = 0xff;
            }
            let key = format!(
                "data:social_person_pk_probe:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT p.id, p.first_name FROM social_knows_pk_probe k INNER JOIN social_person_pk_probe p ON k.person2_id = p.id WHERE k.person1_id = 1 LIMIT 10",
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
async fn test_two_hop_join_probe_skips_guaranteed_right_key_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE graph_two_hop (id INTEGER PRIMARY KEY, person1_id INTEGER, person2_id INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_graph_two_hop_person1 ON graph_two_hop (person1_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO graph_two_hop VALUES (1, 1, 2, 'left-a'), (2, 1, 3, 'left-b'), (3, 2, 20, 'right-a'), (4, 3, 30, 'right-b')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, person1_id, person2_id, payload) in
            [(3_i64, 2_i64, 20_i64, "right-a"), (4, 3, 30, "right-b")]
        {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(person1_id),
                Value::Integer(person2_id),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            let end =
                u32::from_le_bytes(row[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
            for byte in &mut row[start..end] {
                *byte = 0xff;
            }
            let key = format!(
                "data:graph_two_hop:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT k2.person2_id FROM graph_two_hop k1 INNER JOIN graph_two_hop k2 ON k1.person2_id = k2.person1_id WHERE k1.person1_id = 1 LIMIT 10",
    )
    .await;

    assert_eq!(cols, vec!["k2.person2_id"]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(20)], vec![Value::Integer(30)]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_join_expr_probe_uses_indexed_subscript_key_projection() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE message_expr_probe (id INTEGER PRIMARY KEY, creator_id INTEGER, content TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_message_expr_probe_creator ON message_expr_probe (creator_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO message_expr_probe VALUES (10, 2, 'hit', 'unused-a'), (11, 3, 'miss', 'unused-b')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, creator_id, content, payload) in [
            (10_i64, 2_i64, "hit", "unused-a"),
            (11, 3, "miss", "unused-b"),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(creator_id),
                Value::String(content.to_string()),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:message_expr_probe:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "WITH paths(e) AS (SELECT ARRAY[2, 3])
         SELECT p.content
           FROM paths
           INNER JOIN message_expr_probe p ON p.creator_id = e[1]",
    )
    .await;

    assert_eq!(cols, vec!["p.content"]);
    assert_eq!(rows, vec![vec![Value::String("hit".to_string())]]);
    cleanup(&wal_path);
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
async fn test_join_group_by_aggregate_fast_path_order_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE trim_customers (id INTEGER PRIMARY KEY, city TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE trim_orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO trim_customers VALUES (1, 'Paris'), (2, 'Berlin'), (3, 'Tokyo'), (4, 'Paris')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO trim_orders VALUES (1, 1, 10), (2, 2, 20), (3, 3, 5), (4, 4, 25)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT c.city, COUNT(*), SUM(o.total) FROM trim_customers c INNER JOIN trim_orders o ON c.id = o.customer_id GROUP BY c.city ORDER BY SUM(o.total) DESC LIMIT 1 OFFSET 1",
    )
    .await;

    assert_eq!(cols, vec!["c.city", "COUNT(*)", "SUM(o.total)"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::String("Berlin".to_string()),
            Value::Integer(1),
            Value::Integer(20),
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_group_by_aggregate_fast_path_matches_chbench_shape() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE ch_customer_fast (c_id INTEGER PRIMARY KEY, city TEXT, note TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE ch_orders_fast (o_id INTEGER PRIMARY KEY, c_id INTEGER, total FLOAT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO ch_customer_fast VALUES (1, 'Shanghai', 'x'), (2, 'Beijing', 'y'), (3, 'Shanghai', 'z')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO ch_orders_fast VALUES (10, 1, 11.5, 'a'), (11, 1, 2.5, 'b'), (12, 2, 7.0, 'c'), (13, 3, 4.0, 'd')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (table, ids, corrupt_idx) in [
            ("ch_customer_fast", vec![1_i64, 2, 3], 2_usize),
            ("ch_orders_fast", vec![10_i64, 11, 12, 13], 3_usize),
        ] {
            for id in ids {
                let key = format!(
                    "data:{}:{}",
                    table,
                    fusiondb::common::encoding::encode_i64_comparable(id)
                );
                let mut row = txn.get(key.as_bytes()).await.unwrap().unwrap();
                let off_pos = 2 + corrupt_idx * 4;
                let start =
                    u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
                for byte in &mut row[start..] {
                    *byte = 0xff;
                }
                txn.put(key.as_bytes(), &row).await.unwrap();
            }
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT c.city, COUNT(*), SUM(o.total) FROM ch_customer_fast c INNER JOIN ch_orders_fast o ON c.c_id = o.c_id GROUP BY c.city ORDER BY SUM(o.total) DESC",
    )
    .await;

    assert_eq!(cols, vec!["c.city", "COUNT(*)", "SUM(o.total)"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("Shanghai".to_string()),
                Value::Integer(3),
                Value::Float(18.0),
            ],
            vec![
                Value::String("Beijing".to_string()),
                Value::Integer(1),
                Value::Float(7.0),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_implicit_join_where_equi_predicate_matches_chbenchmark_q16_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE stock (id INTEGER PRIMARY KEY, s_i_id INTEGER, s_w_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_name TEXT, i_data TEXT, i_price FLOAT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE supplier (su_suppkey INTEGER PRIMARY KEY, su_comment TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO item VALUES
            (1, 'Item A', 'abc-good', 10.0),
            (2, 'Item B', 'zz-bad-prefix', 20.0),
            (3, 'Item C', 'bbb-good', 5.0)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO stock VALUES
            (1, 1, 1),
            (2, 1, 2),
            (3, 2, 1),
            (4, 3, 1),
            (5, 3, 2)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO supplier VALUES
            (1, 'good supplier'),
            (2, 'good supplier'),
            (3, 'bad supplier'),
            (6, 'good supplier')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT i_name,
                substring(i_data from 1 for 3) AS brand,
                i_price,
                count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt
           FROM stock, item
          WHERE i_id = s_i_id
            AND i_data NOT LIKE 'zz%'
            AND (mod((s_w_id * s_i_id),10000) NOT IN (
                SELECT su_suppkey FROM supplier WHERE su_comment LIKE '%bad%'
            ))
          GROUP BY i_name, brand, i_price
          ORDER BY supplier_cnt DESC",
    )
    .await;

    assert_eq!(cols, vec!["i_name", "brand", "i_price", "supplier_cnt"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("Item A".to_string()),
                Value::String("abc".to_string()),
                Value::Float(10.0),
                Value::Integer(2),
            ],
            vec![
                Value::String("Item C".to_string()),
                Value::String("bbb".to_string()),
                Value::Float(5.0),
                Value::Integer(1),
            ],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_or_branch_common_join_key_matches_chbenchmark_q19_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE order_line (
            ol_id INTEGER PRIMARY KEY,
            ol_i_id INTEGER,
            ol_w_id INTEGER,
            ol_quantity INTEGER,
            ol_amount FLOAT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE item (
            i_id INTEGER PRIMARY KEY,
            i_data TEXT,
            i_price FLOAT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO item VALUES
            (1, 'brand-a', 10.0),
            (2, 'brand-b', 20.0),
            (3, 'brand-c', 30.0),
            (4, 'brand-d', 40.0)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_line VALUES
            (1, 1, 1, 5, 10.0),
            (2, 2, 4, 6, 20.0),
            (3, 3, 5, 7, 30.0),
            (4, 4, 1, 5, 40.0),
            (5, 1, 4, 5, 50.0),
            (6, 2, 2, 11, 60.0)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT sum(ol_amount) AS revenue
           FROM order_line, item
          WHERE (ol_i_id = i_id
             AND i_data LIKE '%a'
             AND ol_quantity >= 1
             AND ol_quantity <= 10
             AND i_price BETWEEN 1 AND 400000
             AND ol_w_id IN (1, 2, 3))
             OR (ol_i_id = i_id
             AND i_data LIKE '%b'
             AND ol_quantity >= 1
             AND ol_quantity <= 10
             AND i_price BETWEEN 1 AND 400000
             AND ol_w_id IN (1, 2, 4))
             OR (ol_i_id = i_id
             AND i_data LIKE '%c'
             AND ol_quantity >= 1
             AND ol_quantity <= 10
             AND i_price BETWEEN 1 AND 400000
             AND ol_w_id IN (1, 5, 3))",
    )
    .await;

    assert_eq!(cols, vec!["revenue"]);
    assert_eq!(rows, vec![vec![Value::Float(60.0)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_implicit_join_common_or_equi_predicate_matches_chbenchmark_q19_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_data TEXT, i_price FLOAT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE order_line (ol_id INTEGER PRIMARY KEY, ol_i_id INTEGER, ol_quantity INTEGER, ol_amount FLOAT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO item VALUES
            (1, 'alpha-brand', 4.0),
            (2, 'beta-brand', 8.0),
            (3, 'gamma-brand', 12.0),
            (4, 'other-brand', 4.0)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_line VALUES
            (1, 1, 3, 100.0),
            (2, 1, 8, 40.0),
            (3, 2, 11, 200.0),
            (4, 3, 22, 300.0),
            (5, 4, 3, 400.0),
            (6, 99, 3, 500.0)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT SUM(ol_amount) AS revenue
           FROM order_line, item
          WHERE (ol_i_id = i_id
                 AND i_data LIKE 'alpha%'
                 AND i_price BETWEEN 1.0 AND 5.0
                 AND ol_quantity >= 1
                 AND ol_quantity <= 5)
             OR (ol_i_id = i_id
                 AND i_data LIKE 'beta%'
                 AND i_price BETWEEN 6.0 AND 10.0
                 AND ol_quantity >= 10
                 AND ol_quantity <= 15)
             OR (ol_i_id = i_id
                 AND i_data LIKE 'gamma%'
                 AND i_price BETWEEN 10.0 AND 15.0
                 AND ol_quantity >= 20
                 AND ol_quantity <= 30)",
    )
    .await;

    assert_eq!(cols, vec!["revenue"]);
    assert_eq!(rows, vec![vec![Value::Float(600.0)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_view_timestamp_predicate_matches_chbenchmark_q15_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE order_line (ol_id INTEGER PRIMARY KEY, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_amount FLOAT, ol_delivery_d TIMESTAMP)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE stock (s_pk INTEGER PRIMARY KEY, s_i_id INTEGER, s_w_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE supplier (su_suppkey INTEGER PRIMARY KEY, su_name TEXT, su_address TEXT, su_phone TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_line VALUES
            (1, 10, 2, 5.0, TIMESTAMP '2007-01-01 00:00:00.000000'),
            (2, 10, 2, 7.0, TIMESTAMP '2007-01-02 00:00:00.000000'),
            (3, 20, 3, 11.0, TIMESTAMP '2007-01-03 00:00:00.000000')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO stock VALUES (1, 10, 2), (2, 20, 3)").await;
    exec_ok(
        &executor,
        "INSERT INTO supplier VALUES
            (20, 'Supplier 20', 'Addr 20', 'Phone 20'),
            (60, 'Supplier 60', 'Addr 60', 'Phone 60')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE VIEW revenue0 (supplier_no, total_revenue) AS
            SELECT mod((s_w_id * s_i_id),10000) as supplier_no,
                   sum(ol_amount) as total_revenue
              FROM order_line, stock
             WHERE ol_i_id = s_i_id
               AND ol_supply_w_id = s_w_id
               AND ol_delivery_d >= '2007-01-02 00:00:00.000000'
             GROUP BY supplier_no",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT su_suppkey, su_name, su_address, su_phone, total_revenue
           FROM supplier, revenue0
          WHERE su_suppkey = supplier_no
            AND total_revenue = (select max(total_revenue) from revenue0)
          ORDER BY su_suppkey",
    )
    .await;

    assert_eq!(
        cols,
        vec![
            "su_suppkey",
            "su_name",
            "su_address",
            "su_phone",
            "total_revenue"
        ]
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(60),
            Value::String("Supplier 60".to_string()),
            Value::String("Addr 60".to_string()),
            Value::String("Phone 60".to_string()),
            Value::Float(11.0),
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_derived_table_join_matches_chbenchmark_q17_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE item (i_id INTEGER PRIMARY KEY, i_data TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE order_line (ol_id INTEGER PRIMARY KEY, ol_i_id INTEGER, ol_quantity INTEGER, ol_amount FLOAT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO item VALUES (1, 'aaab'), (2, 'skip')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_line VALUES
            (1, 1, 10, 100.0),
            (2, 1, 2, 30.0),
            (3, 1, 4, 50.0),
            (4, 2, 1, 200.0)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT SUM(ol_amount) / 2.0 AS avg_yearly
           FROM order_line,
                (SELECT i_id, AVG (ol_quantity) AS a
                   FROM item, order_line
                  WHERE i_data LIKE '%b'
                    AND ol_i_id = i_id
                  GROUP BY i_id) t
          WHERE ol_i_id = t.i_id
            AND ol_quantity < t.a",
    )
    .await;

    assert_eq!(cols, vec!["avg_yearly"]);
    assert_eq!(rows, vec![vec![Value::Float(40.0)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_derived_table_join_matches_chbenchmark_q2_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE item (
            i_id INTEGER PRIMARY KEY,
            i_name TEXT,
            i_data TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE supplier (
            su_suppkey INTEGER PRIMARY KEY,
            su_name TEXT,
            su_address TEXT,
            su_nationkey INTEGER,
            su_phone TEXT,
            su_comment TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE stock (
            s_w_id INTEGER,
            s_i_id INTEGER,
            s_quantity INTEGER,
            PRIMARY KEY (s_w_id, s_i_id)
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name TEXT,
            n_regionkey INTEGER
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name TEXT
        )",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO item VALUES
            (10, 'Item 10', 'aaab'),
            (20, 'Item 20', 'skip')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO region VALUES (1, 'Europe'), (2, 'Asia')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nation VALUES
            (11, 'France', 1),
            (12, 'Germany', 1),
            (21, 'Japan', 2)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO supplier VALUES
            (10, 'Supplier B', 'Addr B', 12, 'Phone B', 'Comment B'),
            (20, 'Supplier A', 'Addr A', 11, 'Phone A', 'Comment A'),
            (30, 'Supplier C', 'Addr C', 21, 'Phone C', 'Comment C')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO stock VALUES
            (1, 10, 5),
            (2, 10, 7),
            (1, 20, 3),
            (3, 10, 9)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
           FROM item, supplier, stock, nation, region,
                (SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity
                   FROM stock, supplier, nation, region
                  WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey
                    AND su_nationkey=n_nationkey
                    AND n_regionkey=r_regionkey
                    AND r_name LIKE 'Europ%'
                  GROUP BY s_i_id) m
          WHERE i_id = s_i_id
            AND MOD((s_w_id * s_i_id), 10000) = su_suppkey
            AND su_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND i_data LIKE '%b'
            AND r_name LIKE 'Europ%'
            AND i_id=m_i_id
            AND s_quantity = m_s_quantity
          ORDER BY n_name, su_name, i_id",
    )
    .await;

    assert_eq!(
        cols,
        vec![
            "su_suppkey",
            "su_name",
            "n_name",
            "i_id",
            "i_name",
            "su_address",
            "su_phone",
            "su_comment"
        ]
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(10),
            Value::String("Supplier B".to_string()),
            Value::String("Germany".to_string()),
            Value::Integer(10),
            Value::String("Item 10".to_string()),
            Value::String("Addr B".to_string()),
            Value::String("Phone B".to_string()),
            Value::String("Comment B".to_string()),
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_group_by_aggregate_result_cache_invalidates_after_insert() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE cache_customer_join (c_id INTEGER PRIMARY KEY, city TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE cache_orders_join (o_id INTEGER PRIMARY KEY, c_id INTEGER, total FLOAT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO cache_customer_join VALUES (1, 'Shanghai'), (2, 'Beijing')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO cache_orders_join VALUES (10, 1, 4.0), (11, 2, 3.0)",
    )
    .await;

    let sql = "SELECT c.city, COUNT(*), SUM(o.total) FROM cache_customer_join c INNER JOIN cache_orders_join o ON c.c_id = o.c_id GROUP BY c.city ORDER BY SUM(o.total) DESC";
    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let eligible_before = metrics
        .query_result_cache_eligible_count
        .load(Ordering::Relaxed);
    let hit_before = metrics.query_result_cache_hit_count.load(Ordering::Relaxed);
    let miss_before = metrics
        .query_result_cache_miss_count
        .load(Ordering::Relaxed);
    let insert_before = metrics
        .query_result_cache_insert_count
        .load(Ordering::Relaxed);
    let invalidation_before = metrics
        .query_result_cache_invalidation_count
        .load(Ordering::Relaxed);
    async fn query_sql_rows(executor: &Executor, sql: &str) -> Vec<Vec<Value>> {
        let mut results = executor.execute_sql(sql).await.unwrap();
        match results.remove(0) {
            QueryResult::Select { rows, .. } => rows,
            other => panic!("Expected Select, got {:?}", other),
        }
    }

    let first_rows = query_sql_rows(&executor, sql).await;
    let cached_rows = query_sql_rows(&executor, sql).await;
    assert_eq!(first_rows, cached_rows);

    exec_ok(
        &executor,
        "INSERT INTO cache_orders_join VALUES (12, 1, 6.0)",
    )
    .await;
    let updated_rows = query_sql_rows(&executor, sql).await;

    assert_eq!(
        updated_rows,
        vec![
            vec![
                Value::String("Shanghai".to_string()),
                Value::Integer(2),
                Value::Float(10.0),
            ],
            vec![
                Value::String("Beijing".to_string()),
                Value::Integer(1),
                Value::Float(3.0),
            ],
        ]
    );
    let eligible_delta = metrics
        .query_result_cache_eligible_count
        .load(Ordering::Relaxed)
        .saturating_sub(eligible_before);
    let hit_delta = metrics
        .query_result_cache_hit_count
        .load(Ordering::Relaxed)
        .saturating_sub(hit_before);
    let miss_delta = metrics
        .query_result_cache_miss_count
        .load(Ordering::Relaxed)
        .saturating_sub(miss_before);
    let insert_delta = metrics
        .query_result_cache_insert_count
        .load(Ordering::Relaxed)
        .saturating_sub(insert_before);
    let invalidation_delta = metrics
        .query_result_cache_invalidation_count
        .load(Ordering::Relaxed)
        .saturating_sub(invalidation_before);
    assert!(
        eligible_delta >= 3,
        "expected at least three cache-eligible grouped aggregate reads, got {eligible_delta}"
    );
    assert!(
        hit_delta >= 1,
        "second grouped aggregate read should hit the query-result cache, got {hit_delta}"
    );
    assert!(
        miss_delta >= 2,
        "initial and post-invalidation reads should miss the query-result cache, got {miss_delta}"
    );
    assert!(
        insert_delta >= 2,
        "initial and post-invalidation reads should insert query-result cache entries, got {insert_delta}"
    );
    assert!(
        invalidation_delta >= 1,
        "write should invalidate the query-result cache, got {invalidation_delta}"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_tsbs_lastpoint_distinct_on_lateral_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, hostname TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE cpu (id INTEGER PRIMARY KEY, tags_id INTEGER, time TIMESTAMP, usage_user FLOAT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO tags VALUES (1, 'host_0'), (2, 'host_1'), (3, 'host_0')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO cpu VALUES
            (1, 1, TIMESTAMP '2016-01-01 00:00:00 +0000', 10.0),
            (2, 1, TIMESTAMP '2016-01-01 00:02:00 +0000', 12.0),
            (3, 2, TIMESTAMP '2016-01-01 00:01:00 +0000', 20.0),
            (4, 3, TIMESTAMP '2016-01-01 00:03:00 +0000', 30.0)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC",
    )
    .await;

    assert_eq!(
        cols,
        vec![
            "t.id",
            "t.hostname",
            "b.id",
            "b.tags_id",
            "b.time",
            "b.usage_user"
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(3),
                Value::String("host_0".to_string()),
                Value::Integer(4),
                Value::Integer(3),
                Value::Timestamp(1451606580000000),
                Value::Float(30.0),
            ],
            vec![
                Value::Integer(2),
                Value::String("host_1".to_string()),
                Value::Integer(3),
                Value::Integer(2),
                Value::Timestamp(1451606460000000),
                Value::Float(20.0),
            ],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC LIMIT 1 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("host_1".to_string()),
            Value::Integer(3),
            Value::Integer(2),
            Value::Timestamp(1451606460000000),
            Value::Float(20.0),
        ]]
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

#[tokio::test]
async fn test_right_join_null_pads_unmatched_right_rows() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE rj_a (id INTEGER PRIMARY KEY, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE rj_b (id INTEGER PRIMARY KEY, aid INTEGER, tag TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO rj_a VALUES (1, 'A1'), (2, 'A2')").await;
    exec_ok(
        &executor,
        "INSERT INTO rj_b VALUES (10, 1, 'B10'), (11, 2, 'B11'), (12, 99, 'B12'), (13, NULL, 'B13')",
    )
    .await;

    // RIGHT JOIN must preserve every rj_b row; unmatched rows get NULL rj_a columns.
    // Output column order is (rj_a.*, rj_b.*).
    let (cols, rows) = query(
        &executor,
        "SELECT * FROM rj_a RIGHT JOIN rj_b ON rj_a.id = rj_b.aid ORDER BY rj_b.id",
    )
    .await;

    assert_eq!(
        cols,
        vec!["rj_a.id", "rj_a.label", "rj_b.id", "rj_b.aid", "rj_b.tag"]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("A1".to_string()),
                Value::Integer(10),
                Value::Integer(1),
                Value::String("B10".to_string()),
            ],
            vec![
                Value::Integer(2),
                Value::String("A2".to_string()),
                Value::Integer(11),
                Value::Integer(2),
                Value::String("B11".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Integer(12),
                Value::Integer(99),
                Value::String("B12".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Integer(13),
                Value::Null,
                Value::String("B13".to_string()),
            ],
        ]
    );

    // Result preservation: the matched portion of the RIGHT JOIN equals the INNER JOIN.
    let (_, inner_rows) = query(
        &executor,
        "SELECT * FROM rj_a INNER JOIN rj_b ON rj_a.id = rj_b.aid ORDER BY rj_b.id",
    )
    .await;
    let right_matched: Vec<Vec<Value>> = rows
        .into_iter()
        .filter(|row| row[0] != Value::Null)
        .collect();
    assert_eq!(right_matched, inner_rows);

    cleanup(&wal);
}

#[tokio::test]
async fn test_right_join_non_equi_null_pads_unmatched_right_rows() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE rj2_a (id INTEGER PRIMARY KEY, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE rj2_b (id INTEGER PRIMARY KEY, aid INTEGER, tag TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO rj2_a VALUES (5, 'A5'), (6, 'A6')").await;
    exec_ok(
        &executor,
        "INSERT INTO rj2_b VALUES (10, 1, 'B10'), (11, 7, 'B11')",
    )
    .await;

    // Non-equi ON exercises the nested-loop fallback; b row 11 (aid=7) matches no rj2_a row.
    let (cols, rows) = query(
        &executor,
        "SELECT * FROM rj2_a RIGHT JOIN rj2_b ON rj2_a.id > rj2_b.aid ORDER BY rj2_b.id, rj2_a.id",
    )
    .await;

    assert_eq!(
        cols,
        vec![
            "rj2_a.id",
            "rj2_a.label",
            "rj2_b.id",
            "rj2_b.aid",
            "rj2_b.tag"
        ]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(5),
                Value::String("A5".to_string()),
                Value::Integer(10),
                Value::Integer(1),
                Value::String("B10".to_string()),
            ],
            vec![
                Value::Integer(6),
                Value::String("A6".to_string()),
                Value::Integer(10),
                Value::Integer(1),
                Value::String("B10".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Integer(11),
                Value::Integer(7),
                Value::String("B11".to_string()),
            ],
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_left_join_nested_on_group_by_matches_chbenchmark_q13_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE customer (
            c_w_id INTEGER,
            c_d_id INTEGER,
            c_id INTEGER,
            c_name TEXT,
            PRIMARY KEY (c_w_id, c_d_id, c_id)
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE oorder (
            o_w_id INTEGER,
            o_d_id INTEGER,
            o_id INTEGER,
            o_c_id INTEGER,
            o_carrier_id INTEGER,
            PRIMARY KEY (o_w_id, o_d_id, o_id)
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO customer VALUES
            (1, 1, 1, 'c111'),
            (1, 1, 2, 'c112'),
            (1, 1, 3, 'c113'),
            (1, 2, 1, 'c121'),
            (2, 1, 1, 'c211'),
            (2, 1, 2, 'c212'),
            (2, 1, 3, 'c213')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO oorder VALUES
            (1, 1, 10, 1, 9),
            (1, 1, 11, 1, 7),
            (1, 1, 12, 2, 10),
            (1, 2, 20, 1, 9),
            (2, 1, 30, 2, 10)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT c_count, count(*) AS custdist
           FROM (
             SELECT c_id, count(o_id) AS c_count
               FROM customer
               LEFT OUTER JOIN oorder ON
                 (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id AND o_carrier_id > 8)
              GROUP BY c_id
           ) AS c_orders
          GROUP BY c_count
          ORDER BY custdist DESC, c_count DESC",
    )
    .await;

    assert_eq!(cols, vec!["c_count", "custdist"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::Integer(2)],
            vec![Value::Integer(0), Value::Integer(1)],
        ]
    );
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

// BENCHPROD-445: CROSS JOIN LIMIT/OFFSET must push limit+offset into join execution so
// large Cartesian joins do not materialize the full product before the final LIMIT.
#[tokio::test]
async fn test_cross_join_limit_offset_window() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE cj_limit_l (id INTEGER PRIMARY KEY, a TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE cj_limit_r (id INTEGER PRIMARY KEY, b TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO cj_limit_l VALUES (1, 'l1'), (2, 'l2'), (3, 'l3')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO cj_limit_r VALUES (10, 'r10'), (20, 'r20'), (30, 'r30')",
    )
    .await;

    let (_, all_rows) = query(
        &executor,
        "SELECT l.a, r.b FROM cj_limit_l l CROSS JOIN cj_limit_r r",
    )
    .await;
    let (_, window_rows) = query(
        &executor,
        "SELECT l.a, r.b FROM cj_limit_l l CROSS JOIN cj_limit_r r LIMIT 3 OFFSET 2",
    )
    .await;
    let expected: Vec<Vec<Value>> = all_rows.into_iter().skip(2).take(3).collect();
    assert_eq!(window_rows, expected);
    cleanup(&wal);
}

// BENCHPROD-443: FULL OUTER JOIN — emit matched pairs once, NULL-pad right for unmatched-left,
// NULL-pad left for unmatched-right. Order is unspecified, so rows are sorted before comparison.

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|r| format!("{:?}", r));
    rows
}

#[tokio::test]
async fn test_full_outer_join_mixed_equi() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE fo_a (id INTEGER PRIMARY KEY, lval TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE fo_b (id INTEGER PRIMARY KEY, akey INTEGER, bval TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fo_a VALUES (1,'a1'),(2,'a2'),(3,'a3')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fo_b VALUES (10,1,'b10'),(11,1,'b11'),(12,99,'b12')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT * FROM fo_a FULL OUTER JOIN fo_b ON fo_a.id = fo_b.akey",
    )
    .await;
    assert_eq!(
        cols,
        vec!["fo_a.id", "fo_a.lval", "fo_b.id", "fo_b.akey", "fo_b.bval"]
    );
    let s = |v: &str| Value::String(v.to_string());
    let expected = sorted(vec![
        vec![
            Value::Integer(1),
            s("a1"),
            Value::Integer(10),
            Value::Integer(1),
            s("b10"),
        ],
        vec![
            Value::Integer(1),
            s("a1"),
            Value::Integer(11),
            Value::Integer(1),
            s("b11"),
        ],
        vec![
            Value::Integer(2),
            s("a2"),
            Value::Null,
            Value::Null,
            Value::Null,
        ], // left-only
        vec![
            Value::Integer(3),
            s("a3"),
            Value::Null,
            Value::Null,
            Value::Null,
        ], // left-only
        vec![
            Value::Null,
            Value::Null,
            Value::Integer(12),
            Value::Integer(99),
            s("b12"),
        ], // right-only
    ]);
    assert_eq!(sorted(rows), expected);
    cleanup(&wal);
}

#[tokio::test]
async fn test_full_outer_join_equals_inner_when_all_match() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE fm_a (id INTEGER PRIMARY KEY, lval TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE fm_b (id INTEGER PRIMARY KEY, akey INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO fm_a VALUES (1,'a1'),(2,'a2')").await;
    exec_ok(&executor, "INSERT INTO fm_b VALUES (10,1),(11,2)").await;
    let (_, full) = query(
        &executor,
        "SELECT * FROM fm_a FULL OUTER JOIN fm_b ON fm_a.id = fm_b.akey",
    )
    .await;
    let (_, inner) = query(
        &executor,
        "SELECT * FROM fm_a INNER JOIN fm_b ON fm_a.id = fm_b.akey",
    )
    .await;
    // Every row matches, so FULL OUTER must equal INNER (no NULL-padded rows).
    let inner_sorted = sorted(inner);
    assert_eq!(inner_sorted.len(), 2);
    assert_eq!(sorted(full), inner_sorted);
    cleanup(&wal);
}

#[tokio::test]
async fn test_full_outer_join_non_equi() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE na (id INTEGER PRIMARY KEY, v INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE nb (id INTEGER PRIMARY KEY, w INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO na VALUES (1,10),(2,50)").await;
    exec_ok(&executor, "INSERT INTO nb VALUES (10,5),(11,50),(12,99)").await;
    // Non-equi condition forces the nested-loop FULL path.
    let (_, rows) = query(
        &executor,
        "SELECT * FROM na FULL OUTER JOIN nb ON na.v > nb.w",
    )
    .await;
    let i = Value::Integer;
    let expected = sorted(vec![
        vec![i(1), i(10), i(10), i(5)],               // 10 > 5
        vec![i(2), i(50), i(10), i(5)],               // 50 > 5
        vec![Value::Null, Value::Null, i(11), i(50)], // right-only (w=50)
        vec![Value::Null, Value::Null, i(12), i(99)], // right-only (w=99)
    ]);
    assert_eq!(sorted(rows), expected);
    cleanup(&wal);
}

async fn setup_outer_join_pushdown_tables(executor: &Executor) {
    exec_ok(
        executor,
        "CREATE TABLE oj_left (id INTEGER PRIMARY KEY, x INTEGER)",
    )
    .await;
    exec_ok(
        executor,
        "CREATE TABLE oj_right (aid INTEGER PRIMARY KEY, y INTEGER)",
    )
    .await;
    exec_ok(executor, "INSERT INTO oj_left VALUES (1, 1), (2, 0)").await;
    exec_ok(executor, "INSERT INTO oj_right VALUES (1, 100), (2, 200)").await;
}

#[tokio::test]
async fn test_left_join_on_left_side_predicate_preserves_rows() {
    let (executor, wal) = setup().await;
    setup_outer_join_pushdown_tables(&executor).await;

    // The ON predicate on the PRESERVED (left) side only gates matching:
    // row (2,0) fails it but must still appear NULL-padded.
    let (_, rows) = query(
        &executor,
        "SELECT oj_left.id, oj_left.x, oj_right.y FROM oj_left \
         LEFT JOIN oj_right ON oj_left.id = oj_right.aid AND oj_left.x = 1 \
         ORDER BY oj_left.id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::Integer(0), Value::Null],
        ]
    );

    // An ON predicate on the null (right) side stays pushdown-safe: rows it
    // rejects simply never match, and the left side still NULL-pads.
    let (_, rows) = query(
        &executor,
        "SELECT oj_left.id, oj_right.y FROM oj_left \
         LEFT JOIN oj_right ON oj_left.id = oj_right.aid AND oj_right.y = 100 \
         ORDER BY oj_left.id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(100)],
            vec![Value::Integer(2), Value::Null],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_right_join_on_right_side_predicate_preserves_rows() {
    let (executor, wal) = setup().await;
    setup_outer_join_pushdown_tables(&executor).await;

    let (_, rows) = query(
        &executor,
        "SELECT oj_left.id, oj_right.aid, oj_right.y FROM oj_left \
         RIGHT JOIN oj_right ON oj_left.id = oj_right.aid AND oj_right.y = 100 \
         ORDER BY oj_right.aid",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(100)],
            vec![Value::Null, Value::Integer(2), Value::Integer(200)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_full_join_single_side_on_predicates_preserve_rows() {
    let (executor, wal) = setup().await;
    setup_outer_join_pushdown_tables(&executor).await;

    let (_, rows) = query(
        &executor,
        "SELECT oj_left.id, oj_right.aid FROM oj_left \
         FULL OUTER JOIN oj_right ON oj_left.id = oj_right.aid AND oj_left.x = 1 \
         ORDER BY oj_left.id, oj_right.aid",
    )
    .await;
    // Row 1 matches; row 2 fails the ON predicate so both sides NULL-pad
    // (ascending sort places NULLs first).
    assert_eq!(
        rows,
        vec![
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(2), Value::Null],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_left_join_where_on_null_side_applies_after_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wj_left (id INTEGER PRIMARY KEY, x INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE wj_right (aid INTEGER PRIMARY KEY, y INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO wj_left VALUES (1, 1), (2, 0)").await;
    exec_ok(&executor, "INSERT INTO wj_right VALUES (1, 100)").await;

    // WHERE on the null side filters AFTER the join: the NULL-padded row
    // (2, NULL) fails y = 100 and must be dropped.
    let (_, rows) = query(
        &executor,
        "SELECT wj_left.id, wj_right.y FROM wj_left \
         LEFT JOIN wj_right ON wj_left.id = wj_right.aid WHERE wj_right.y = 100",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(100)]]);

    // The IS NULL anti-join idiom keeps ONLY the NULL-padded row.
    let (_, rows) = query(
        &executor,
        "SELECT wj_left.id, wj_right.y FROM wj_left \
         LEFT JOIN wj_right ON wj_left.id = wj_right.aid WHERE wj_right.y IS NULL",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Null]]);

    // WHERE on the preserved side is unaffected by the outer join.
    let (_, rows) = query(
        &executor,
        "SELECT wj_left.id, wj_right.y FROM wj_left \
         LEFT JOIN wj_right ON wj_left.id = wj_right.aid WHERE wj_left.x = 0",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Null]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_right_join_where_on_null_side_applies_after_join() {
    let (executor, wal) = setup().await;
    setup_outer_join_pushdown_tables(&executor).await;

    // Left is the null side of a RIGHT JOIN: its WHERE conjunct must apply
    // post-join, so the padded row (NULL, 2) fails x = 1 and is dropped.
    let (_, rows) = query(
        &executor,
        "SELECT oj_left.id, oj_right.aid FROM oj_left \
         RIGHT JOIN oj_right ON oj_left.id = oj_right.aid AND oj_left.x = 1 \
         WHERE oj_left.x = 1",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(1)]]);

    // And the IS NULL anti-join over the left side keeps only padded rows.
    let (_, rows) = query(
        &executor,
        "SELECT oj_left.id, oj_right.aid FROM oj_left \
         RIGHT JOIN oj_right ON oj_left.id = oj_right.aid AND oj_left.x = 1 \
         WHERE oj_left.id IS NULL",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Null, Value::Integer(2)]]);
    cleanup(&wal);
}
