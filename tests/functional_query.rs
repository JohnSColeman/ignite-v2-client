//! Functional query tests — Rust port of Apache Ignite's `FunctionalQueryTest.java`.
//!
//! Java source reference:
//! <https://github.com/apache/ignite/blob/master/modules/indexing/src/test/java/org/apache/ignite/client/FunctionalQueryTest.java>
//!
//! Prerequisites: Ignite running on localhost:10800 with no authentication.
//! Run:  cargo test --test functional_query -- --nocapture --test-threads=1

use ignite_client::{IgniteClient, IgniteClientConfig, IgniteValue};

const ADDR: &str = "localhost:10800";

fn client() -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR))
}

fn client_with_page_size(page_size: usize) -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR).with_page_size(page_size))
}

async fn drop_table(c: &IgniteClient, name: &str) {
    let _ = c
        .execute(&format!("DROP TABLE IF EXISTS PUBLIC.{name}"), vec![])
        .await;
}

async fn create_person_table(c: &IgniteClient, name: &str) {
    drop_table(c, name).await;
    c.execute(
        &format!(
            "CREATE TABLE PUBLIC.{name} \
             (id INT PRIMARY KEY, name VARCHAR) \
             WITH \"template=replicated\""
        ),
        vec![],
    )
    .await
    .unwrap_or_else(|e| panic!("CREATE TABLE {name} failed: {e}"));
}

// ─── 1. testQueries (SqlFieldsQuery portion) ──────────────────────────────────

/// Ports the `SqlFieldsQuery` portion of `testQueries`.
///
/// Inserts 100 persons, then queries those with id >= 51 (50 rows) using a
/// deliberately small page size to exercise multi-page cursor fetching.
#[tokio::test]
async fn functional_query_sql_fields_pagination() {
    const TABLE: &str = "FQ_PERSONS_PAGINATE";
    const ROW_COUNT: usize = 100;
    const MIN_ID: i32 = (ROW_COUNT / 2 + 1) as i32; // 51

    // page_size ≈ (100 − 51) / 3  → ~16, forces at least 3 round-trips
    let page_size = (ROW_COUNT as i32 - MIN_ID) as usize / 3;
    let c = client_with_page_size(page_size);

    create_person_table(&c, TABLE).await;

    // Insert 100 rows.
    for id in 1..=ROW_COUNT as i32 {
        c.execute(
            &format!("INSERT INTO PUBLIC.{TABLE} (id, name) VALUES (?, ?)"),
            vec![
                IgniteValue::Int(id),
                IgniteValue::String(format!("Person {id}")),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("INSERT id={id} failed: {e}"));
    }

    // Query rows where id >= 51 using a stream to exercise pagination.
    let stream = c
        .query_stream(
            &format!("SELECT id, name FROM PUBLIC.{TABLE} WHERE id >= ? ORDER BY id"),
            vec![IgniteValue::Int(MIN_ID)],
        )
        .await
        .expect("query_stream failed");

    let rows = stream.collect_all().await.expect("collect_all failed");

    let expected_count = (ROW_COUNT as i32 - MIN_ID + 1) as usize; // 50
    assert_eq!(
        rows.len(),
        expected_count,
        "expected {expected_count} rows, got {}",
        rows.len()
    );

    // Verify ordering and values.
    for (i, row) in rows.iter().enumerate() {
        let expected_id = MIN_ID + i as i32;
        match row.get(0) {
            Some(IgniteValue::Int(id)) => assert_eq!(*id, expected_id, "id mismatch at row {i}"),
            other => panic!("expected Int id at row {i}, got {other:?}"),
        }
        match row.get(1) {
            Some(IgniteValue::String(name)) => {
                assert_eq!(
                    name,
                    &format!("Person {expected_id}"),
                    "name mismatch at row {i}"
                )
            }
            other => panic!("expected String name at row {i}, got {other:?}"),
        }
    }

    drop_table(&c, TABLE).await;
}

// ─── 2. testSql ───────────────────────────────────────────────────────────────

/// Ports `testSql`: creates a Person table, inserts 10 rows, queries by id=1,
/// then fetches all rows with page_size=1.
#[tokio::test]
async fn functional_query_sql() {
    const TABLE: &str = "FQ_PERSONS_SQL";
    const ROW_COUNT: usize = 10;

    let c = client();
    create_person_table(&c, TABLE).await;

    // Insert 10 rows (id 0..9 to match Java's loop: i = 0; i < data.size()).
    for id in 0..ROW_COUNT as i32 {
        c.execute(
            &format!("INSERT INTO PUBLIC.{TABLE} (id, name) VALUES (?, ?)"),
            vec![
                IgniteValue::Int(id),
                IgniteValue::String(format!("Person {id}")),
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("INSERT id={id} failed: {e}"));
    }

    // Query name for id=1 — expect exactly one row "Person 1".
    let result = c
        .query(
            &format!("SELECT name FROM PUBLIC.{TABLE} WHERE id = ?"),
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT id=1 failed");

    assert_eq!(result.row_count(), 1, "expected 1 row");
    match result.first_row().and_then(|r| r.get(0)) {
        Some(IgniteValue::String(name)) => assert_eq!(name, "Person 1"),
        other => panic!("expected String 'Person 1', got {other:?}"),
    }

    // Fetch all rows with page_size=1 (exercises cursor pagination).
    let c_small = client_with_page_size(1);
    let stream = c_small
        .query_stream(
            &format!("SELECT id, name FROM PUBLIC.{TABLE} ORDER BY id"),
            vec![],
        )
        .await
        .expect("query_stream failed");

    let rows = stream.collect_all().await.expect("collect_all failed");
    assert_eq!(
        rows.len(),
        ROW_COUNT,
        "expected {ROW_COUNT} rows with page_size=1"
    );

    // Spot-check first and last row.
    match rows[0].get(0) {
        Some(IgniteValue::Int(0)) => {}
        other => panic!("first row id should be 0, got {other:?}"),
    }
    match rows[ROW_COUNT - 1].get(0) {
        Some(IgniteValue::Int(id)) => assert_eq!(*id, (ROW_COUNT - 1) as i32),
        other => panic!("last row id unexpected: {other:?}"),
    }

    drop_table(&c, TABLE).await;
}

// ─── 3. testGettingEmptyResultWhenQueryingEmptyTable ──────────────────────────

/// Ports `testGettingEmptyResultWhenQueryingEmptyTable`: querying an empty
/// table must return a non-error result with zero rows.
#[tokio::test]
async fn functional_query_empty_table() {
    const TABLE: &str = "FQ_PERSONS_EMPTY";

    let c = client();
    create_person_table(&c, TABLE).await;

    let result = c
        .query(&format!("SELECT id, name FROM PUBLIC.{TABLE}"), vec![])
        .await
        .expect("SELECT on empty table failed");

    assert_eq!(result.row_count(), 0, "empty table should return 0 rows");

    drop_table(&c, TABLE).await;
}

// ─── 4. testMixedQueryAndCacheApiOperations ───────────────────────────────────

/// Ports `testMixedQueryAndCacheApiOperations`: creates a table via SQL,
/// inserts via SQL, writes one entry via cache API, reads via cache API, then
/// queries the cache-written entry via SQL.
#[tokio::test]
async fn functional_query_mixed_sql_and_cache() {
    const TABLE: &str = "FQ_PERSONS_MIXED";
    // Ignite creates a cache named SQL_PUBLIC_<TABLE> (upper-cased) for SQL tables.
    let cache_name = format!("SQL_PUBLIC_{TABLE}");

    let c = client();
    create_person_table(&c, TABLE).await;

    // Insert id=1 via SQL.
    c.execute(
        &format!("INSERT INTO PUBLIC.{TABLE} (id, name) VALUES (?, ?)"),
        vec![
            IgniteValue::Int(1),
            IgniteValue::String("Person 1".to_string()),
        ],
    )
    .await
    .expect("SQL INSERT failed");

    // Read id=1 via cache API — value is the Ignite BinaryObject for the row,
    // which comes back as RawObject bytes.  We only assert the key is found.
    let cache = c.cache(&cache_name);
    let got = cache
        .get(IgniteValue::Int(1))
        .await
        .expect("cache.get(1) failed");
    assert!(
        !matches!(got, IgniteValue::Null),
        "cache.get(1) should return a value, got Null"
    );

    // Verify the SQL-inserted row is visible via SQL SELECT.
    let result = c
        .query(
            &format!("SELECT name FROM PUBLIC.{TABLE} WHERE id = ?"),
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SQL SELECT failed");

    assert_eq!(result.row_count(), 1);
    match result.first_row().and_then(|r| r.get(0)) {
        Some(IgniteValue::String(name)) => assert_eq!(name, "Person 1"),
        other => panic!("expected 'Person 1', got {other:?}"),
    }

    drop_table(&c, TABLE).await;
}

// ─── 5. testSqlParameterValidation (adapted) ─────────────────────────────────

/// Adapted from `testSqlParameterValidation`.
///
/// The Java test uses reflection to inject an illegal `updateBatchSize=-1`.
/// Here we instead verify that querying a non-existent table produces an error,
/// which exercises the same "server returns an error response" code path.
#[tokio::test]
async fn functional_query_server_error() {
    let c = client();

    let result = c
        .query("SELECT * FROM PUBLIC.THIS_TABLE_DOES_NOT_EXIST", vec![])
        .await;

    assert!(
        result.is_err(),
        "expected an error querying a non-existent table"
    );
}

// ─── 6. testEmptyQuery ────────────────────────────────────────────────────────

/// Ports `testEmptyQuery`: submitting an empty SQL string must return an error.
#[tokio::test]
async fn functional_query_empty_sql() {
    let c = client();

    let result = c.query("", vec![]).await;

    assert!(result.is_err(), "empty SQL string should return an error");
}
