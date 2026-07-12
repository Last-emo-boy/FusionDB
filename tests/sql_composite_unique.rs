use fusiondb::common::Value;
use fusiondb::config::StorageConfig;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::parser::parse_sql;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::{FusionStorage, Storage};
use std::sync::Arc;

async fn setup_memory() -> (Arc<Executor>, Arc<dyn Storage>, String) {
    let wal_path = format!("test_composite_unique_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));
    (executor, storage, wal_path)
}

async fn setup_fusion() -> (Arc<Executor>, Arc<dyn Storage>, std::path::PathBuf) {
    let data_dir = std::env::temp_dir().join(format!(
        "fusiondb_composite_unique_{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut config = StorageConfig::default();
    config.data_dir = data_dir.to_string_lossy().to_string();
    let wal_path = config.wal_path();
    let storage: Arc<dyn Storage> = Arc::new(
        FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap(),
    );
    let executor = Arc::new(Executor::new(storage.clone()));
    (executor, storage, data_dir)
}

fn assert_single_count(results: &[QueryResult], expected: i64) {
    match results {
        [QueryResult::Select { rows, .. }] => {
            assert_eq!(rows, &vec![vec![Value::Integer(expected)]])
        }
        other => panic!("expected one count result, got {other:?}"),
    }
}

#[tokio::test]
async fn table_composite_unique_is_enforced_and_nulls_are_distinct() {
    let (executor, _, wal_path) = setup_memory().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_table (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                external_id TEXT,
                CONSTRAINT cu_tenant_external UNIQUE (tenant, external_id)
            )",
        )
        .await
        .unwrap();

    let indexes = executor
        .execute_sql("SHOW INDEXES FROM cu_table")
        .await
        .unwrap();
    match indexes.as_slice() {
        [QueryResult::Select { rows, .. }] => assert!(rows.iter().any(|row| {
            row[0] == Value::String("cu_tenant_external".to_string())
                && row[2] == Value::String("tenant,external_id".to_string())
        })),
        other => panic!("expected SHOW INDEXES result, got {other:?}"),
    }

    executor
        .execute_sql("INSERT INTO cu_table VALUES (1, 7, 'acct')")
        .await
        .unwrap();
    let err = executor
        .execute_sql("INSERT INTO cu_table VALUES (2, 7, 'acct')")
        .await
        .expect_err("duplicate composite value must fail");
    assert!(err.to_string().contains("UNIQUE"));

    executor
        .execute_sql(
            "INSERT INTO cu_table VALUES
                (3, NULL, 'acct'),
                (4, NULL, 'acct'),
                (5, 7, NULL),
                (6, 7, NULL)",
        )
        .await
        .unwrap();
    let count = executor
        .execute_sql("SELECT COUNT(*) FROM cu_table")
        .await
        .unwrap();
    assert_single_count(&count, 5);

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn create_unique_index_rejects_existing_duplicates_and_enforces_future_writes() {
    let (executor, _, wal_path) = setup_memory().await;
    executor
        .execute_sql("CREATE TABLE cu_existing (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)")
        .await
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cu_existing VALUES (1, 4, 'x'), (2, 4, 'x'), (3, NULL, 'x'), (4, NULL, 'x')",
        )
        .await
        .unwrap();

    let err = executor
        .execute_sql("CREATE UNIQUE INDEX cu_existing_key ON cu_existing (tenant, code)")
        .await
        .expect_err("backfill must reject existing duplicates");
    assert!(err.to_string().contains("UNIQUE"));

    executor
        .execute_sql("DELETE FROM cu_existing WHERE id = 2")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX cu_existing_key ON cu_existing (tenant, code)")
        .await
        .unwrap();
    let err = executor
        .execute_sql("INSERT INTO cu_existing VALUES (5, 4, 'x')")
        .await
        .expect_err("new duplicate must fail");
    assert!(err.to_string().contains("UNIQUE"));

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn composite_unique_index_named_marker_is_loaded_updated_and_dropped() {
    let (executor, _, wal_path) = setup_memory().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_marker_name (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                code TEXT
            )",
        )
        .await
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX __marker ON cu_marker_name (tenant, code)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_marker_name VALUES (1, 7, 'first')")
        .await
        .unwrap();

    let error = executor
        .execute_sql("INSERT INTO cu_marker_name VALUES (2, 7, 'first')")
        .await
        .expect_err("a __marker index must reject duplicate INSERT values");
    assert!(error.to_string().contains("UNIQUE"));

    executor
        .execute_sql("UPDATE cu_marker_name SET tenant = 8, code = 'second' WHERE id = 1")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_marker_name VALUES (2, 7, 'first')")
        .await
        .expect("the old composite value must be reusable after UPDATE");
    let error = executor
        .execute_sql("INSERT INTO cu_marker_name VALUES (3, 8, 'second')")
        .await
        .expect_err("a __marker index must maintain the new UPDATE value");
    assert!(error.to_string().contains("UNIQUE"));

    executor.execute_sql("DROP INDEX __marker").await.unwrap();
    executor
        .execute_sql("INSERT INTO cu_marker_name VALUES (3, 8, 'second')")
        .await
        .expect("dropping __marker must remove composite uniqueness");

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn composite_unique_update_delete_reuse_and_upsert_stay_consistent() {
    let (executor, _, wal_path) = setup_memory().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_mutation (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                code TEXT,
                UNIQUE (tenant, code)
            )",
        )
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_mutation VALUES (1, 10, 'a'), (2, 20, 'b')")
        .await
        .unwrap();

    let err = executor
        .execute_sql("UPDATE cu_mutation SET tenant = 10, code = 'a' WHERE id = 2")
        .await
        .expect_err("UPDATE must not create a duplicate");
    assert!(err.to_string().contains("UNIQUE"));

    executor
        .execute_sql("UPDATE cu_mutation SET tenant = 11 WHERE id = 1")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_mutation VALUES (3, 10, 'a')")
        .await
        .expect("old UPDATE value must be reusable");
    executor
        .execute_sql("DELETE FROM cu_mutation WHERE id = 3")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_mutation VALUES (4, 10, 'a')")
        .await
        .expect("deleted value must be reusable");

    let err = executor
        .execute_sql(
            "INSERT INTO cu_mutation VALUES (2, 30, 'c')
             ON CONFLICT (id) DO UPDATE SET tenant = 10, code = 'a'",
        )
        .await
        .expect_err("UPSERT DO UPDATE must enforce composite UNIQUE");
    assert!(err.to_string().contains("UNIQUE"));

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn concurrent_insert_and_copy_collide_on_composite_sentinel() {
    let (executor, storage, data_dir) = setup_fusion().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_race (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                code TEXT,
                UNIQUE (tenant, code)
            )",
        )
        .await
        .unwrap();

    let csv_path = data_dir.join("copy.csv");
    std::fs::write(&csv_path, "id,tenant,code\n1,42,race\n").unwrap();
    let copy_sql = format!(
        "COPY cu_race FROM '{}' WITH (FORMAT CSV, HEADER true)",
        csv_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''")
    );
    let copy = parse_sql(&copy_sql).unwrap();
    let insert = parse_sql("INSERT INTO cu_race VALUES (2, 42, 'race')").unwrap();

    let mut copy_txn = storage.begin_transaction().await.unwrap();
    let mut insert_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&copy[0], copy_txn.as_mut())
        .await
        .expect("stage COPY");
    executor
        .execute_in_transaction(&insert[0], insert_txn.as_mut())
        .await
        .expect("stage concurrent INSERT");

    copy_txn.commit().await.unwrap();
    let err = insert_txn
        .commit()
        .await
        .expect_err("same composite sentinel must make one writer abort");
    assert!(err.to_string().to_lowercase().contains("conflict"));

    let count = executor
        .execute_sql("SELECT COUNT(*) FROM cu_race")
        .await
        .unwrap();
    assert_single_count(&count, 1);

    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn create_unique_index_and_concurrent_writes_conflict_through_ddl_barriers() {
    let (executor, storage, data_dir) = setup_fusion().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_build_race (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)",
        )
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_build_race VALUES (1, 1, 'before')")
        .await
        .unwrap();

    let ddl =
        parse_sql("CREATE UNIQUE INDEX cu_build_race_key ON cu_build_race (tenant, code)").unwrap();
    let insert = parse_sql("INSERT INTO cu_build_race VALUES (2, 2, 'during')").unwrap();
    let mut ddl_txn = storage.begin_transaction().await.unwrap();
    let mut insert_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&ddl[0], ddl_txn.as_mut())
        .await
        .expect("stage index backfill");
    executor
        .execute_in_transaction(&insert[0], insert_txn.as_mut())
        .await
        .expect("stage concurrent insert");
    insert_txn.commit().await.unwrap();
    let error = ddl_txn
        .commit()
        .await
        .expect_err("backfill snapshot must conflict when a row writer commits first");
    assert!(error.to_string().contains("Write conflict"));

    executor
        .execute_sql("CREATE UNIQUE INDEX cu_build_race_key ON cu_build_race (tenant, code)")
        .await
        .expect("retry must backfill the concurrent row");
    let duplicate = executor
        .execute_sql("INSERT INTO cu_build_race VALUES (3, 2, 'during')")
        .await
        .expect_err("retried index must enforce the concurrent row");
    assert!(duplicate.to_string().contains("UNIQUE"));

    executor
        .execute_sql("CREATE TABLE cu_build_race_reverse (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)")
        .await
        .unwrap();
    let insert =
        parse_sql("INSERT INTO cu_build_race_reverse VALUES (1, 9, 'unpublished')").unwrap();
    let ddl = parse_sql(
        "CREATE UNIQUE INDEX cu_build_race_reverse_key ON cu_build_race_reverse (tenant, code)",
    )
    .unwrap();
    let mut insert_txn = storage.begin_transaction().await.unwrap();
    let mut ddl_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&insert[0], insert_txn.as_mut())
        .await
        .expect("stage pre-index insert");
    executor
        .execute_in_transaction(&ddl[0], ddl_txn.as_mut())
        .await
        .expect("stage concurrent index backfill");
    ddl_txn.commit().await.unwrap();
    let error = insert_txn
        .commit()
        .await
        .expect_err("old-snapshot writer must conflict when index DDL commits first");
    assert!(error.to_string().contains("Write conflict"));

    executor
        .execute_sql(
            "CREATE TABLE cu_build_update (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)",
        )
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_build_update VALUES (1, 1, 'before')")
        .await
        .unwrap();
    executor
        .execute_sql("UPDATE cu_build_update SET code = 'primed' WHERE id = 1")
        .await
        .expect("prime the simple primary-key UPDATE cache");
    let ddl =
        parse_sql("CREATE UNIQUE INDEX cu_build_update_key ON cu_build_update (tenant, code)")
            .unwrap();
    let update =
        parse_sql("UPDATE cu_build_update SET tenant = 2, code = 'during' WHERE id = 1").unwrap();
    let mut ddl_txn = storage.begin_transaction().await.unwrap();
    let mut update_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&ddl[0], ddl_txn.as_mut())
        .await
        .expect("stage unique-index backfill");
    executor
        .execute_in_transaction(&update[0], update_txn.as_mut())
        .await
        .expect("stage fast primary-key update");
    update_txn.commit().await.unwrap();
    let error = ddl_txn
        .commit()
        .await
        .expect_err("fast UPDATE must conflict with the backfill snapshot");
    assert!(error.to_string().contains("Write conflict"));
    executor
        .execute_sql("CREATE UNIQUE INDEX cu_build_update_key ON cu_build_update (tenant, code)")
        .await
        .expect("retry must index the updated row");
    let duplicate = executor
        .execute_sql("INSERT INTO cu_build_update VALUES (2, 2, 'during')")
        .await
        .expect_err("retried index must enforce the fast UPDATE value");
    assert!(duplicate.to_string().contains("UNIQUE"));

    executor
        .execute_sql(
            "CREATE TABLE cu_build_nonunique (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)",
        )
        .await
        .unwrap();
    let ddl = parse_sql("CREATE INDEX cu_build_nonunique_key ON cu_build_nonunique (tenant, code)")
        .unwrap();
    let insert = parse_sql("INSERT INTO cu_build_nonunique VALUES (1, 5, 'during')").unwrap();
    let mut ddl_txn = storage.begin_transaction().await.unwrap();
    let mut insert_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&ddl[0], ddl_txn.as_mut())
        .await
        .expect("stage ordinary index backfill");
    executor
        .execute_in_transaction(&insert[0], insert_txn.as_mut())
        .await
        .expect("stage concurrent row");
    insert_txn.commit().await.unwrap();
    let error = ddl_txn
        .commit()
        .await
        .expect_err("ordinary index backfill must conflict with concurrent writes");
    assert!(error.to_string().contains("Write conflict"));

    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn ddl_commit_cannot_leave_a_stale_positive_fast_update_cache_entry() {
    let (executor, storage, data_dir) = setup_fusion().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_update_cache_race (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                code TEXT
            )",
        )
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_update_cache_race VALUES (1, 1, 'before')")
        .await
        .unwrap();

    let ddl = parse_sql(
        "CREATE UNIQUE INDEX cu_update_cache_race_key
         ON cu_update_cache_race (tenant, code)",
    )
    .unwrap();
    let update = parse_sql(
        "UPDATE cu_update_cache_race
         SET tenant = 2, code = 'during'
         WHERE id = 1",
    )
    .unwrap();
    let mut ddl_txn = storage.begin_transaction().await.unwrap();
    let mut stale_update_txn = storage.begin_transaction().await.unwrap();

    executor
        .execute_in_transaction(&ddl[0], ddl_txn.as_mut())
        .await
        .expect("stage unique-index backfill");
    executor
        .execute_in_transaction(&update[0], stale_update_txn.as_mut())
        .await
        .expect("stage old-snapshot fast UPDATE");
    ddl_txn.commit().await.expect("commit index DDL first");
    let error = stale_update_txn
        .commit()
        .await
        .expect_err("old-snapshot UPDATE must conflict with committed index DDL");
    assert!(error.to_string().contains("Write conflict"));

    executor
        .execute_sql(
            "UPDATE cu_update_cache_race
             SET tenant = 2, code = 'during'
             WHERE id = 1",
        )
        .await
        .expect("retry must discover and maintain the committed index");
    let duplicate = executor
        .execute_sql("INSERT INTO cu_update_cache_race VALUES (2, 2, 'during')")
        .await
        .expect_err("retry must publish the new composite UNIQUE sentinel");
    assert!(duplicate.to_string().contains("UNIQUE"));

    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn shared_index_keyspace_rejects_incompatible_include_layouts() {
    let (executor, _, wal_path) = setup_memory().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_include_layout (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                code TEXT,
                payload_a TEXT,
                payload_b TEXT
            )",
        )
        .await
        .unwrap();
    executor
        .execute_sql(
            "CREATE INDEX cu_include_a ON cu_include_layout (tenant, code) INCLUDE (payload_a)",
        )
        .await
        .unwrap();

    let error = executor
        .execute_sql(
            "CREATE INDEX cu_include_b ON cu_include_layout (tenant, code) INCLUDE (payload_b)",
        )
        .await
        .expect_err("shared physical keys cannot encode two payload layouts");
    assert!(error.to_string().contains("INCLUDE column layouts differ"));

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn drop_index_and_drop_table_conflict_with_concurrent_row_writers() {
    let (executor, storage, data_dir) = setup_fusion().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_drop_race (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)",
        )
        .await
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX cu_drop_key ON cu_drop_race (tenant, code)")
        .await
        .unwrap();

    let drop_index = parse_sql("DROP INDEX cu_drop_key").unwrap();
    let insert = parse_sql("INSERT INTO cu_drop_race VALUES (1, 7, 'during')").unwrap();
    let mut drop_txn = storage.begin_transaction().await.unwrap();
    let mut insert_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&drop_index[0], drop_txn.as_mut())
        .await
        .expect("stage index drop");
    executor
        .execute_in_transaction(&insert[0], insert_txn.as_mut())
        .await
        .expect("stage old-snapshot insert");
    drop_txn.commit().await.unwrap();
    let error = insert_txn
        .commit()
        .await
        .expect_err("writer cannot recreate index state after DROP INDEX commits");
    assert!(error.to_string().contains("Write conflict"));

    let insert = parse_sql("INSERT INTO cu_drop_race VALUES (2, 8, 'orphan')").unwrap();
    let drop_table = parse_sql("DROP TABLE cu_drop_race").unwrap();
    let mut insert_txn = storage.begin_transaction().await.unwrap();
    let mut drop_txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&insert[0], insert_txn.as_mut())
        .await
        .expect("stage pre-drop insert");
    executor
        .execute_in_transaction(&drop_table[0], drop_txn.as_mut())
        .await
        .expect("stage table drop");
    drop_txn.commit().await.unwrap();
    let error = insert_txn
        .commit()
        .await
        .expect_err("writer cannot leave rows after DROP TABLE commits");
    assert!(error.to_string().contains("Write conflict"));

    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn primary_index_identity_is_explicit_and_not_inferred_from_name() {
    let (executor, storage, wal_path) = setup_memory().await;
    executor
        .execute_sql(
            "CREATE TABLE cu_named_primary (
                tenant INTEGER,
                code TEXT,
                payload TEXT,
                CONSTRAINT custom_primary_identity PRIMARY KEY (tenant, code)
            )",
        )
        .await
        .unwrap();
    let txn = storage.begin_transaction().await.unwrap();
    let primary_meta = txn
        .get(b"index_meta:custom_primary_identity")
        .await
        .unwrap()
        .expect("primary metadata");
    assert!(primary_meta.starts_with(b"p6:"));
    drop(txn);
    executor
        .execute_sql("INSERT INTO cu_named_primary VALUES (1, 'a', 'first')")
        .await
        .expect("explicitly named composite primary metadata must be usable");
    let duplicate = executor
        .execute_sql("INSERT INTO cu_named_primary VALUES (1, 'a', 'duplicate')")
        .await
        .expect_err("composite primary key must remain unique");
    let duplicate = duplicate.to_string();
    assert!(duplicate.contains("PRIMARY KEY") || duplicate.contains("UNIQUE"));

    executor
        .execute_sql(
            "CREATE TABLE cu_secondary_pkey (
                id INTEGER PRIMARY KEY,
                tenant INTEGER,
                code TEXT
            )",
        )
        .await
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX alias_pkey ON cu_secondary_pkey (tenant, code)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_secondary_pkey VALUES (42, 7, 'stable')")
        .await
        .unwrap();
    let expected_key = format!(
        "data:cu_secondary_pkey:{}",
        fusiondb::common::encoding::encode_i64_comparable(42)
    );
    let txn = storage.begin_transaction().await.unwrap();
    assert!(txn.get(expected_key.as_bytes()).await.unwrap().is_some());

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn dropping_one_of_shared_composite_indexes_preserves_referenced_entries() {
    let (executor, storage, wal_path) = setup_memory().await;
    executor
        .execute_sql("CREATE TABLE cu_shared (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_shared VALUES (1, 1, 'a'), (2, 2, 'b')")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE INDEX cu_shared_a ON cu_shared (tenant, code)")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE INDEX cu_shared_b ON cu_shared (tenant, code)")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX cu_shared_unique ON cu_shared (tenant, code)")
        .await
        .unwrap();

    let txn = storage.begin_transaction().await.unwrap();
    assert_eq!(
        txn.scan_prefix(b"index:cu_shared:tenant,code:", None)
            .await
            .unwrap()
            .len(),
        2
    );
    drop(txn);

    executor
        .execute_sql("DROP INDEX cu_shared_a")
        .await
        .unwrap();
    executor
        .execute_sql("DROP INDEX cu_shared_unique")
        .await
        .unwrap();
    let txn = storage.begin_transaction().await.unwrap();
    assert_eq!(
        txn.scan_prefix(b"index:cu_shared:tenant,code:", None)
            .await
            .unwrap()
            .len(),
        2,
        "the remaining index still owns the shared physical entries"
    );
    drop(txn);

    executor
        .execute_sql("INSERT INTO cu_shared VALUES (3, 1, 'a')")
        .await
        .expect("dropping the unique index must remove only its sentinel ownership");
    executor
        .execute_sql("DROP INDEX cu_shared_b")
        .await
        .unwrap();
    let txn = storage.begin_transaction().await.unwrap();
    assert!(txn
        .scan_prefix(b"index:cu_shared:tenant,code:", None)
        .await
        .unwrap()
        .is_empty());

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn authoritative_unique_sentinel_ignores_orphaned_physical_entry() {
    let (executor, storage, wal_path) = setup_memory().await;
    executor
        .execute_sql("CREATE TABLE cu_orphan (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)")
        .await
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX cu_orphan_key ON cu_orphan (tenant, code)")
        .await
        .unwrap();
    executor
        .execute_sql("INSERT INTO cu_orphan VALUES (1, 7, 'orphan')")
        .await
        .unwrap();

    let txn = storage.begin_transaction().await.unwrap();
    let stale_entry = txn
        .scan_prefix(b"index:cu_orphan:tenant,code:", None)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    drop(txn);
    executor
        .execute_sql("DELETE FROM cu_orphan WHERE id = 1")
        .await
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    txn.put(&stale_entry.0, &stale_entry.1).await.unwrap();
    txn.commit().await.unwrap();

    executor
        .execute_sql("INSERT INTO cu_orphan VALUES (2, 7, 'orphan')")
        .await
        .expect("u6 metadata must use the exact sentinel instead of stale index entries");

    let _ = std::fs::remove_file(wal_path);
}

#[tokio::test]
async fn nulls_not_distinct_unique_forms_are_rejected_explicitly() {
    let (executor, _, wal_path) = setup_memory().await;
    let error = executor
        .execute_sql(
            "CREATE TABLE cu_nulls_table (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT, UNIQUE NULLS NOT DISTINCT (tenant, code))",
        )
        .await
        .expect_err("table constraint must be rejected");
    assert!(error
        .to_string()
        .contains("NULLS NOT DISTINCT is not supported"));

    executor
        .execute_sql(
            "CREATE TABLE cu_nulls_index (id INTEGER PRIMARY KEY, tenant INTEGER, code TEXT)",
        )
        .await
        .unwrap();
    let error = executor
        .execute_sql(
            "CREATE UNIQUE INDEX cu_nulls_index_key ON cu_nulls_index (tenant, code) NULLS NOT DISTINCT",
        )
        .await
        .expect_err("unique index null semantics must be rejected");
    assert!(error
        .to_string()
        .contains("NULLS NOT DISTINCT is not supported"));

    let _ = std::fs::remove_file(wal_path);
}
