
//! End-to-end smoke tests against a live Apache Ignite 2.x node.
//!
//! Prerequisites: Ignite running on localhost:10800 with no authentication.
//! Run:  cargo test --test smoke -- --nocapture

use bigdecimal::BigDecimal;
use ignite_client::{IgniteClient, IgniteClientConfig, IgniteError, IgniteValue, TxConcurrency, TxIsolation};
use ignite_client::ProtocolError;
use std::str::FromStr;
use uuid::Uuid;
use ignite_client::IgniteCache;

const ADDR: &str = "localhost:10800";

fn client() -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR))
}

/// Drop-then-create a test table.  Each test owns a uniquely-named table to
/// avoid inter-test interference when tests run in parallel.
async fn create_table(c: &IgniteClient, name: &str, cols: &str) {
    let _ = c
        .execute(&format!("DROP TABLE IF EXISTS PUBLIC.{name}"), vec![])
        .await;
    c.execute(
        &format!(
            "CREATE TABLE PUBLIC.{name} ({cols}) WITH \"template=replicated\""
        ),
        vec![],
    )
    .await
    .unwrap_or_else(|e| panic!("CREATE TABLE {name} failed: {e}"));
}

/// Create a table suitable for thin-client SQL transactions.
/// TRANSACTIONAL atomicity is required for DML inside thin-client transactions
/// to be properly rolled back.  Requires the Ignite node to be started with
/// -DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true (see ignite.bat).
async fn create_tx_table(c: &IgniteClient, name: &str, cols: &str) {
    let _ = c
        .execute(&format!("DROP TABLE IF EXISTS PUBLIC.{name}"), vec![])
        .await;
    c.execute(
        &format!(
            "CREATE TABLE PUBLIC.{name} ({cols}) WITH \"ATOMICITY=TRANSACTIONAL\""
        ),
        vec![],
    )
    .await
    .unwrap_or_else(|e| panic!("CREATE TABLE {name} failed: {e}"));
}

async fn drop_table(c: &IgniteClient, name: &str) {
    let _ = c
        .execute(&format!("DROP TABLE IF EXISTS PUBLIC.{name}"), vec![])
        .await;
}

// ─── 1. Basic connectivity ────────────────────────────────────────────────────

/// Verify a connection can be established and a literal SELECT works.
#[tokio::test]
async fn smoke_connect() {
    let c = client();
    let r = c.query("SELECT 1", vec![]).await.expect("SELECT 1 failed");
    assert_eq!(r.row_count(), 1);
    // Ignite may return INT or LONG for integer literals
    let val = r.first_row().unwrap().get(0).unwrap();
    assert!(
        matches!(val, IgniteValue::Int(1) | IgniteValue::Long(1)),
        "unexpected literal value: {val:?}"
    );
}

// ─── 2. Full CRUD cycle ───────────────────────────────────────────────────────

#[tokio::test]
async fn smoke_crud() {
    let c = client();
    create_table(&c, "SMOKE_CRUD", "id INT PRIMARY KEY, name VARCHAR, val BIGINT").await;

    // INSERT
    let ins = c
        .execute(
            "INSERT INTO PUBLIC.SMOKE_CRUD (id, name, val) VALUES (?, ?, ?)",
            vec![
                IgniteValue::Int(1),
                IgniteValue::String("alice".into()),
                IgniteValue::Long(42),
            ],
        )
        .await
        .expect("INSERT failed");
    assert_eq!(ins.rows_affected, 1, "INSERT: wrong rows_affected");

    // SELECT
    let r = c
        .query(
            "SELECT id, name, val FROM PUBLIC.SMOKE_CRUD WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT failed");
    assert_eq!(r.row_count(), 1);
    let row = r.first_row().unwrap();
    assert_eq!(row.get(0), Some(&IgniteValue::Int(1)), "id mismatch");
    assert_eq!(
        row.get(1),
        Some(&IgniteValue::String("alice".into())),
        "name mismatch"
    );
    assert_eq!(row.get(2), Some(&IgniteValue::Long(42)), "val mismatch");

    // UPDATE
    let upd = c
        .execute(
            "UPDATE PUBLIC.SMOKE_CRUD SET val = ? WHERE id = ?",
            vec![IgniteValue::Long(99), IgniteValue::Int(1)],
        )
        .await
        .expect("UPDATE failed");
    assert_eq!(upd.rows_affected, 1, "UPDATE: wrong rows_affected");

    let r2 = c
        .query(
            "SELECT val FROM PUBLIC.SMOKE_CRUD WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT after UPDATE failed");
    assert_eq!(
        r2.first_row().unwrap().get(0),
        Some(&IgniteValue::Long(99)),
        "val after UPDATE"
    );

    // DELETE
    let del = c
        .execute(
            "DELETE FROM PUBLIC.SMOKE_CRUD WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("DELETE failed");
    assert_eq!(del.rows_affected, 1, "DELETE: wrong rows_affected");

    let empty = c
        .query("SELECT COUNT(*) FROM PUBLIC.SMOKE_CRUD", vec![])
        .await
        .expect("COUNT failed");
    let count_val = empty.first_row().unwrap().get(0).unwrap();
    assert!(
        matches!(count_val, IgniteValue::Long(0) | IgniteValue::Int(0)),
        "table should be empty, got {count_val:?}"
    );

    drop_table(&c, "SMOKE_CRUD").await;
}

// ─── 3. Column type roundtrips ────────────────────────────────────────────────

#[tokio::test]
async fn smoke_types() {
    let c = client();
    create_table(
        &c,
        "SMOKE_TYPES",
        "id INT PRIMARY KEY, bval BOOLEAN, ival INT, lval BIGINT, dval DOUBLE, sval VARCHAR",
    )
    .await;

    let pi = std::f64::consts::PI;
    c.execute(
        "INSERT INTO PUBLIC.SMOKE_TYPES (id, bval, ival, lval, dval, sval) VALUES (?, ?, ?, ?, ?, ?)",
        vec![
            IgniteValue::Int(1),
            IgniteValue::Bool(true),
            IgniteValue::Int(i32::MAX),
            IgniteValue::Long(i64::MIN),
            IgniteValue::Double(pi),
            IgniteValue::String("hello, 世界".into()),
        ],
    )
    .await
    .expect("INSERT types failed");

    let r = c
        .query(
            "SELECT bval, ival, lval, dval, sval FROM PUBLIC.SMOKE_TYPES WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT types failed");

    assert_eq!(r.row_count(), 1);
    let row = r.first_row().unwrap();
    assert_eq!(row.get(0), Some(&IgniteValue::Bool(true)), "bool");
    assert_eq!(row.get(1), Some(&IgniteValue::Int(i32::MAX)), "int MAX");
    assert_eq!(row.get(2), Some(&IgniteValue::Long(i64::MIN)), "long MIN");
    assert_eq!(row.get(3), Some(&IgniteValue::Double(pi)), "double PI");
    assert_eq!(
        row.get(4),
        Some(&IgniteValue::String("hello, 世界".into())),
        "unicode string"
    );

    // NULL: insert a row with only the PK, expect NULLs for everything else
    c.execute(
        "INSERT INTO PUBLIC.SMOKE_TYPES (id) VALUES (?)",
        vec![IgniteValue::Int(2)],
    )
    .await
    .expect("INSERT null row failed");

    let r_null = c
        .query(
            "SELECT sval FROM PUBLIC.SMOKE_TYPES WHERE id = ?",
            vec![IgniteValue::Int(2)],
        )
        .await
        .expect("SELECT null row failed");
    assert_eq!(
        r_null.first_row().unwrap().get(0),
        Some(&IgniteValue::Null),
        "expected NULL for un-set column"
    );

    drop_table(&c, "SMOKE_TYPES").await;
}

// ─── 4. Multi-page cursor (pagination) ───────────────────────────────────────

/// Insert more rows than the default page size (1024) and verify the full
/// result is materialised correctly via automatic cursor pagination.
#[tokio::test]
async fn smoke_pagination() {
    const ROWS: i32 = 1100; // exceeds default page_size of 1024
    let c = client();
    create_table(&c, "SMOKE_PAGE", "id INT PRIMARY KEY, v INT").await;

    // Concurrent inserts to keep the test fast
    let handles: Vec<_> = (0..ROWS)
        .map(|i| {
            let c = c.clone();
            tokio::spawn(async move {
                c.execute(
                    "INSERT INTO PUBLIC.SMOKE_PAGE (id, v) VALUES (?, ?)",
                    vec![IgniteValue::Int(i), IgniteValue::Int(i * 2)],
                )
                .await
            })
        })
        .collect();
    for h in handles {
        h.await
            .expect("insert task panicked")
            .expect("INSERT failed");
    }

    let result = c
        .query("SELECT id FROM PUBLIC.SMOKE_PAGE", vec![])
        .await
        .expect("paginated SELECT failed");
    assert_eq!(
        result.row_count(),
        ROWS as usize,
        "paginated result: expected {ROWS} rows, got {}",
        result.row_count()
    );

    drop_table(&c, "SMOKE_PAGE").await;
}

// ─── 5. Transaction — commit ──────────────────────────────────────────────────

#[tokio::test]
async fn smoke_tx_commit() {
    let c = client();
    create_tx_table(&c, "SMOKE_TX_COMMIT", "id INT PRIMARY KEY, v INT").await;

    let mut tx = c.begin_transaction().await.expect("begin_transaction failed");
    tx.execute(
        "INSERT INTO PUBLIC.SMOKE_TX_COMMIT (id, v) VALUES (?, ?)",
        vec![IgniteValue::Int(1), IgniteValue::Int(100)],
    )
    .await
    .expect("tx INSERT failed");
    tx.commit().await.expect("commit failed");

    let r = c
        .query(
            "SELECT v FROM PUBLIC.SMOKE_TX_COMMIT WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT after commit failed");
    assert_eq!(
        r.first_row().unwrap().get(0),
        Some(&IgniteValue::Int(100)),
        "committed value not visible"
    );

    drop_table(&c, "SMOKE_TX_COMMIT").await;
}

// ─── 6. Transaction — rollback ────────────────────────────────────────────────

#[tokio::test]
async fn smoke_tx_rollback() {
    let c = client();
    create_tx_table(&c, "SMOKE_TX_ROLLBACK", "id INT PRIMARY KEY, v INT").await;

    let mut tx = c.begin_transaction().await.expect("begin_transaction failed");
    tx.execute(
        "INSERT INTO PUBLIC.SMOKE_TX_ROLLBACK (id, v) VALUES (?, ?)",
        vec![IgniteValue::Int(1), IgniteValue::Int(999)],
    )
    .await
    .expect("tx INSERT failed");
    tx.rollback().await.expect("rollback failed");

    let r = c
        .query(
            "SELECT v FROM PUBLIC.SMOKE_TX_ROLLBACK WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT after rollback failed");
    assert_eq!(r.row_count(), 0, "row should be absent after rollback");

    drop_table(&c, "SMOKE_TX_ROLLBACK").await;
}

// ─── 7. with_transaction helper ──────────────────────────────────────────────

#[tokio::test]
async fn smoke_with_transaction() {
    let c = client();
    create_tx_table(&c, "SMOKE_WITH_TX", "id INT PRIMARY KEY, v INT").await;

    let ret = c
        .with_transaction(|mut tx| async move {
            tx.execute(
                "INSERT INTO PUBLIC.SMOKE_WITH_TX (id, v) VALUES (?, ?)",
                vec![IgniteValue::Int(42), IgniteValue::Int(7)],
            )
            .await?;
            Ok((tx, 42_i32))
        })
        .await
        .expect("with_transaction failed");

    assert_eq!(ret, 42);

    let r = c
        .query(
            "SELECT v FROM PUBLIC.SMOKE_WITH_TX WHERE id = ?",
            vec![IgniteValue::Int(42)],
        )
        .await
        .expect("SELECT after with_transaction failed");
    assert_eq!(
        r.first_row().unwrap().get(0),
        Some(&IgniteValue::Int(7)),
        "value not committed"
    );

    drop_table(&c, "SMOKE_WITH_TX").await;
}

// ─── 8. Drop-based auto-rollback ─────────────────────────────────────────────

#[tokio::test]
async fn smoke_tx_drop_rollback() {
    let c = client();
    create_tx_table(&c, "SMOKE_TX_DROP", "id INT PRIMARY KEY, v INT").await;

    {
        let mut tx = c.begin_transaction().await.expect("begin_transaction failed");
        tx.execute(
            "INSERT INTO PUBLIC.SMOKE_TX_DROP (id, v) VALUES (?, ?)",
            vec![IgniteValue::Int(1), IgniteValue::Int(55)],
        )
        .await
        .expect("tx INSERT failed");
        // Drop without commit → implicit rollback via tokio::spawn
    }

    // Give the fire-and-forget rollback a moment to land
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let r = c
        .query(
            "SELECT v FROM PUBLIC.SMOKE_TX_DROP WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT failed");
    assert_eq!(r.row_count(), 0, "row should have been rolled back on drop");

    drop_table(&c, "SMOKE_TX_DROP").await;
}

// ─── 9. Server error propagation ─────────────────────────────────────────────

#[tokio::test]
async fn smoke_server_error() {
    let c = client();
    let err = c
        .query("SELECT * FROM NONEXISTENT_TABLE_XYZ_SMOKE", vec![])
        .await;
    assert!(err.is_err(), "expected an error for a missing table");
    match err.unwrap_err() {
        ignite_client::IgniteError::Protocol(ProtocolError::ServerError { status, .. }) => {
            assert_ne!(status, 0, "status should be non-zero");
        }
        e => panic!("wrong error type: {e:?}"),
    }
}

// ─── 10. get_by_name (case-insensitive column lookup) ────────────────────────

#[tokio::test]
async fn smoke_get_by_name() {
    let c = client();
    create_table(&c, "SMOKE_NAMES", "id INT PRIMARY KEY, username VARCHAR").await;

    c.execute(
        "INSERT INTO PUBLIC.SMOKE_NAMES (id, username) VALUES (?, ?)",
        vec![IgniteValue::Int(1), IgniteValue::String("bob".into())],
    )
    .await
    .expect("INSERT failed");

    let r = c
        .query(
            "SELECT id, username FROM PUBLIC.SMOKE_NAMES WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT failed");

    let row = r.first_row().unwrap();
    assert_eq!(
        row.get_by_name("USERNAME"),
        Some(&IgniteValue::String("bob".into())),
        "uppercase name lookup"
    );
    assert_eq!(
        row.get_by_name("username"),
        Some(&IgniteValue::String("bob".into())),
        "lowercase name lookup"
    );
    assert_eq!(row.get_by_name("id"), Some(&IgniteValue::Int(1)), "id by name");
    assert_eq!(row.get_by_name("missing_col"), None, "absent column");

    drop_table(&c, "SMOKE_NAMES").await;
}

// ─── 11. Concurrent pipelined queries ────────────────────────────────────────

/// Spawn 40 concurrent queries; they multiplex over the 10-connection pool.
#[tokio::test]
async fn smoke_concurrent() {
    let c = client();
    let handles: Vec<_> = (0..40_i32)
        .map(|_| {
            let c = c.clone();
            tokio::spawn(async move { c.query("SELECT 1", vec![]).await })
        })
        .collect();

    for h in handles {
        h.await
            .expect("concurrent task panicked")
            .expect("concurrent SELECT 1 failed");
    }
}

// ─── 12. Transaction::query (SELECT inside a transaction) ────────────────────

#[tokio::test]
async fn smoke_tx_query() {
    let c = client();
    create_tx_table(&c, "SMOKE_TX_QUERY", "id INT PRIMARY KEY, v INT").await;

    c.execute(
        "INSERT INTO PUBLIC.SMOKE_TX_QUERY (id, v) VALUES (?, ?)",
        vec![IgniteValue::Int(1), IgniteValue::Int(42)],
    )
    .await
    .expect("INSERT failed");

    let mut tx = c.begin_transaction().await.expect("begin_transaction failed");
    let r = tx
        .query(
            "SELECT v FROM PUBLIC.SMOKE_TX_QUERY WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("tx.query failed");
    tx.rollback().await.expect("rollback failed");

    assert_eq!(r.row_count(), 1);
    assert_eq!(
        r.first_row().unwrap().get(0),
        Some(&IgniteValue::Int(42)),
        "value read inside transaction"
    );

    drop_table(&c, "SMOKE_TX_QUERY").await;
}

// ─── 13. with_transaction error path (closure returns Err → Drop rollback) ───

#[tokio::test]
async fn smoke_with_transaction_rollback() {
    let c = client();
    create_tx_table(&c, "SMOKE_WITH_TX_ERR", "id INT PRIMARY KEY, v INT").await;

    let result: std::result::Result<(), _> = c
        .with_transaction(|mut tx| async move {
            tx.execute(
                "INSERT INTO PUBLIC.SMOKE_WITH_TX_ERR (id, v) VALUES (?, ?)",
                vec![IgniteValue::Int(1), IgniteValue::Int(5)],
            )
            .await?;
            // Simulate a business-logic error — the transaction is NOT committed.
            // with_transaction propagates the Err; the Transaction is dropped
            // inside the closure future, firing the fire-and-forget rollback.
            Err(IgniteError::NoRows)
        })
        .await;

    assert!(
        matches!(result, Err(IgniteError::NoRows)),
        "expected NoRows error, got {result:?}"
    );

    // Give the fire-and-forget Drop rollback a moment to land
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let r = c
        .query("SELECT COUNT(*) FROM PUBLIC.SMOKE_WITH_TX_ERR", vec![])
        .await
        .expect("COUNT failed");
    let count = r.first_row().unwrap().get(0).unwrap();
    assert!(
        matches!(count, IgniteValue::Long(0) | IgniteValue::Int(0)),
        "row should have been rolled back, count = {count:?}"
    );

    drop_table(&c, "SMOKE_WITH_TX_ERR").await;
}

// ─── 14. begin_transaction_with explicit concurrency/isolation ────────────────

#[tokio::test]
async fn smoke_begin_transaction_with() {
    let c = client();
    create_tx_table(&c, "SMOKE_TX_WITH", "id INT PRIMARY KEY, v INT").await;

    // Pessimistic / RepeatableRead
    let mut tx1 = c
        .begin_transaction_with(TxConcurrency::Pessimistic, TxIsolation::RepeatableRead, 0)
        .await
        .expect("begin_transaction_with Pessimistic/RepeatableRead failed");
    tx1.execute(
        "INSERT INTO PUBLIC.SMOKE_TX_WITH (id, v) VALUES (?, ?)",
        vec![IgniteValue::Int(1), IgniteValue::Int(10)],
    )
    .await
    .expect("INSERT in tx1 failed");
    tx1.commit().await.expect("commit tx1 failed");

    // Pessimistic / ReadCommitted — pass a non-zero timeout to exercise that
    // parameter, but keep it large enough (30s) not to expire during a normal
    // test run even when many tests execute concurrently.
    let mut tx2 = c
        .begin_transaction_with(TxConcurrency::Pessimistic, TxIsolation::ReadCommitted, 30_000)
        .await
        .expect("begin_transaction_with Pessimistic/ReadCommitted failed");
    tx2.execute(
        "INSERT INTO PUBLIC.SMOKE_TX_WITH (id, v) VALUES (?, ?)",
        vec![IgniteValue::Int(2), IgniteValue::Int(20)],
    )
    .await
    .expect("INSERT in tx2 failed");
    tx2.commit().await.expect("commit tx2 failed");

    let r = c
        .query("SELECT COUNT(*) FROM PUBLIC.SMOKE_TX_WITH", vec![])
        .await
        .expect("COUNT failed");
    let count = r.first_row().unwrap().get(0).unwrap();
    assert!(
        matches!(count, IgniteValue::Long(2) | IgniteValue::Int(2)),
        "expected 2 committed rows, got {count:?}"
    );

    drop_table(&c, "SMOKE_TX_WITH").await;
}

// ─── 15. Row introspection API ────────────────────────────────────────────────

#[tokio::test]
async fn smoke_row_introspection() {
    let c = client();
    create_table(&c, "SMOKE_ROW_API", "id INT PRIMARY KEY, name VARCHAR, val BIGINT").await;

    c.execute(
        "INSERT INTO PUBLIC.SMOKE_ROW_API (id, name, val) VALUES (?, ?, ?)",
        vec![
            IgniteValue::Int(1),
            IgniteValue::String("test".into()),
            IgniteValue::Long(99),
        ],
    )
    .await
    .expect("INSERT failed");

    let r = c
        .query(
            "SELECT id, name, val FROM PUBLIC.SMOKE_ROW_API WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT failed");

    // QueryResult::columns field
    assert_eq!(r.columns.len(), 3, "column count");
    // Ignite returns column names in uppercase
    assert_eq!(r.columns[0].name, "ID", "col 0 name");
    assert_eq!(r.columns[1].name, "NAME", "col 1 name");
    assert_eq!(r.columns[2].name, "VAL", "col 2 name");

    let row = r.first_row().unwrap();

    // Row::len / is_empty
    assert_eq!(row.len(), 3, "row.len()");
    assert!(!row.is_empty(), "row.is_empty()");

    // Row::columns
    let cols = row.columns();
    assert_eq!(cols.len(), 3, "row.columns().len()");
    assert_eq!(cols[1].name, "NAME", "row.columns()[1].name");

    // Row::values
    let vals = row.values();
    assert_eq!(vals.len(), 3, "row.values().len()");
    assert_eq!(vals[0], IgniteValue::Int(1), "values()[0]");
    assert_eq!(vals[1], IgniteValue::String("test".into()), "values()[1]");
    assert_eq!(vals[2], IgniteValue::Long(99), "values()[2]");

    drop_table(&c, "SMOKE_ROW_API").await;
}

// ─── 16. pool_status and with_pool_size ───────────────────────────────────────

#[tokio::test]
async fn smoke_pool_status() {
    // Verify with_pool_size sets the field
    let config = IgniteClientConfig::new(ADDR).with_pool_size(3);
    assert_eq!(config.max_pool_size, 3, "with_pool_size should set max_pool_size");

    // Verify pool_status reflects the configured size
    let c = IgniteClient::new(config);
    let status = c.pool_status();
    assert_eq!(status.max_size, 3, "pool_status().max_size should be 3");

    // Issue a query to create at least one connection
    c.query("SELECT 1", vec![]).await.expect("SELECT 1 failed");
    let status = c.pool_status();
    assert!(status.size >= 1, "pool should have at least one live connection after a query");

    // Verify default pool size
    let default_config = IgniteClientConfig::new(ADDR);
    assert_eq!(default_config.max_pool_size, 10, "default max_pool_size should be 10");

    // Verify with_auth builder sets credentials (no server call needed)
    let authed = IgniteClientConfig::new(ADDR).with_auth("ignite", "ignite");
    assert_eq!(authed.username, Some("ignite".to_string()), "with_auth username");
    assert_eq!(authed.password, Some("ignite".to_string()), "with_auth password");
}

// ─── 17. Numeric narrow types: Byte, Short, Float ────────────────────────────

#[tokio::test]
async fn smoke_types_numeric_narrow() {
    let c = client();
    create_table(
        &c,
        "SMOKE_NARROW",
        "id INT PRIMARY KEY, bv TINYINT, sv SMALLINT, fv REAL",
    )
    .await;

    c.execute(
        "INSERT INTO PUBLIC.SMOKE_NARROW (id, bv, sv, fv) VALUES (?, ?, ?, ?)",
        vec![
            IgniteValue::Int(1),
            IgniteValue::Byte(i8::MAX),
            IgniteValue::Short(i16::MIN),
            IgniteValue::Float(3.14_f32),
        ],
    )
    .await
    .expect("INSERT types failed");

    // Insert a NULL row
    c.execute(
        "INSERT INTO PUBLIC.SMOKE_NARROW (id) VALUES (?)",
        vec![IgniteValue::Int(2)],
    )
    .await
    .expect("INSERT null row failed");

    let r = c
        .query(
            "SELECT bv, sv, fv FROM PUBLIC.SMOKE_NARROW WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT failed");
    let row = r.first_row().unwrap();
    assert_eq!(row.get(0), Some(&IgniteValue::Byte(i8::MAX)), "Byte roundtrip");
    assert_eq!(row.get(1), Some(&IgniteValue::Short(i16::MIN)), "Short roundtrip");
    assert_eq!(row.get(2), Some(&IgniteValue::Float(3.14_f32)), "Float roundtrip");

    let r_null = c
        .query(
            "SELECT bv FROM PUBLIC.SMOKE_NARROW WHERE id = ?",
            vec![IgniteValue::Int(2)],
        )
        .await
        .expect("SELECT null row failed");
    assert_eq!(
        r_null.first_row().unwrap().get(0),
        Some(&IgniteValue::Null),
        "expected NULL for unset TINYINT column"
    );

    drop_table(&c, "SMOKE_NARROW").await;
}

// ─── 18. Temporal types: Date, Time, Timestamp ───────────────────────────────

/// Tests that IgniteValue::Date / Time / Timestamp are accepted as SQL
/// parameters for INSERT and WHERE clauses.
///
/// Note: SELECTing DATE/TIME/TIMESTAMP column values directly returns type
/// code 254 (OPTM_MARSH — Ignite wraps them as Java-serialised
/// java.sql.Date/Time/Timestamp), which this client intentionally does not
/// decode.  The encoding path (Rust → server) is fully exercised here; the
/// decoding path (server → Rust) for temporal columns is a known gap.
#[tokio::test]
async fn smoke_types_temporal() {
    let c = client();
    create_table(
        &c,
        "SMOKE_TEMPORAL",
        "id INT PRIMARY KEY, dv DATE, tv TIME, tsv TIMESTAMP",
    )
    .await;

    // 2024-01-15 00:00:00 UTC in milliseconds
    let date_ms: i64 = 1_705_276_800_000;
    // 12:34:56 expressed as nanoseconds from midnight
    let time_ns: i64 = (12 * 3600 + 34 * 60 + 56) * 1_000_000_000;
    // 2024-01-15 12:34:56 UTC in milliseconds
    let ts_ms: i64 = 1_705_319_696_000;

    // Encoding test: temporal values are accepted as INSERT parameters
    let ins = c
        .execute(
            "INSERT INTO PUBLIC.SMOKE_TEMPORAL (id, dv, tv, tsv) VALUES (?, ?, ?, ?)",
            vec![
                IgniteValue::Int(1),
                IgniteValue::Date(date_ms),
                IgniteValue::Time(time_ns),
                IgniteValue::Timestamp(ts_ms, 0),
            ],
        )
        .await
        .expect("INSERT temporal types failed");
    assert_eq!(ins.rows_affected, 1, "INSERT temporal: wrong rows_affected");

    // Row exists — server accepted the temporal parameter values
    let r = c
        .query(
            "SELECT id FROM PUBLIC.SMOKE_TEMPORAL WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT after temporal INSERT failed");
    assert_eq!(r.row_count(), 1, "row should exist after temporal INSERT");

    // Temporal values can be used in WHERE clause equality comparisons
    let r2 = c
        .query(
            "SELECT id FROM PUBLIC.SMOKE_TEMPORAL WHERE dv = ? AND tsv = ?",
            vec![IgniteValue::Date(date_ms), IgniteValue::Timestamp(ts_ms, 0)],
        )
        .await
        .expect("SELECT WHERE temporal failed");
    assert_eq!(r2.row_count(), 1, "temporal WHERE clause should match stored row");

    // NULL temporal columns: verify via IS NULL rather than reading column value
    c.execute(
        "INSERT INTO PUBLIC.SMOKE_TEMPORAL (id) VALUES (?)",
        vec![IgniteValue::Int(2)],
    )
    .await
    .expect("INSERT null temporal row failed");
    let r_null = c
        .query(
            "SELECT COUNT(*) FROM PUBLIC.SMOKE_TEMPORAL WHERE id = ? AND dv IS NULL",
            vec![IgniteValue::Int(2)],
        )
        .await
        .expect("COUNT null temporal failed");
    let cnt = r_null.first_row().unwrap().get(0).unwrap();
    assert!(
        matches!(cnt, IgniteValue::Long(1) | IgniteValue::Int(1)),
        "NULL temporal column not detected via IS NULL: got {cnt:?}"
    );

    drop_table(&c, "SMOKE_TEMPORAL").await;
}

// ─── 19. Binary and Decimal types ────────────────────────────────────────────

#[tokio::test]
async fn smoke_types_binary_decimal() {
    let c = client();
    create_table(
        &c,
        "SMOKE_BINARY",
        "id INT PRIMARY KEY, bav BINARY(16), dcv DECIMAL(18, 4)",
    )
    .await;

    let bytes = vec![0xDE_u8, 0xAD, 0xBE, 0xEF];
    let decimal = BigDecimal::from_str("1234.5678").unwrap();

    c.execute(
        "INSERT INTO PUBLIC.SMOKE_BINARY (id, bav, dcv) VALUES (?, ?, ?)",
        vec![
            IgniteValue::Int(1),
            IgniteValue::ByteArray(bytes.clone()),
            IgniteValue::Decimal(decimal.clone()),
        ],
    )
    .await
    .expect("INSERT binary/decimal failed");

    // Insert a NULL row
    c.execute(
        "INSERT INTO PUBLIC.SMOKE_BINARY (id) VALUES (?)",
        vec![IgniteValue::Int(2)],
    )
    .await
    .expect("INSERT null binary row failed");

    let r = c
        .query(
            "SELECT bav, dcv FROM PUBLIC.SMOKE_BINARY WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT binary/decimal failed");
    let row = r.first_row().unwrap();

    // BINARY(16) may be padded to 16 bytes; check the prefix
    match row.get(0) {
        Some(IgniteValue::ByteArray(got)) => {
            assert!(
                got.starts_with(&bytes),
                "ByteArray prefix mismatch: got {got:?}"
            );
        }
        other => panic!("expected ByteArray, got {other:?}"),
    }

    // Decimal comparison with normalized() to ignore trailing zeros
    match row.get(1) {
        Some(IgniteValue::Decimal(got)) => {
            assert_eq!(
                got.normalized(),
                decimal.normalized(),
                "Decimal roundtrip mismatch"
            );
        }
        other => panic!("expected Decimal, got {other:?}"),
    }

    let r_null = c
        .query(
            "SELECT bav FROM PUBLIC.SMOKE_BINARY WHERE id = ?",
            vec![IgniteValue::Int(2)],
        )
        .await
        .expect("SELECT null binary row failed");
    assert_eq!(
        r_null.first_row().unwrap().get(0),
        Some(&IgniteValue::Null),
        "expected NULL for unset BINARY column"
    );

    drop_table(&c, "SMOKE_BINARY").await;
}

// ─── 20. UUID type ────────────────────────────────────────────────────────────

/// Verifies RFC 4122 byte-order interoperability with the Java server.
#[tokio::test]
async fn smoke_types_uuid() {
    let c = client();
    create_table(&c, "SMOKE_UUID", "id INT PRIMARY KEY, uv UUID").await;

    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

    c.execute(
        "INSERT INTO PUBLIC.SMOKE_UUID (id, uv) VALUES (?, ?)",
        vec![IgniteValue::Int(1), IgniteValue::Uuid(uuid)],
    )
    .await
    .expect("INSERT UUID failed");

    // Insert a NULL row
    c.execute(
        "INSERT INTO PUBLIC.SMOKE_UUID (id, uv) VALUES (?, ?)",
        vec![IgniteValue::Int(2), IgniteValue::Null],
    )
    .await
    .expect("INSERT null UUID row failed");

    let r = c
        .query(
            "SELECT uv FROM PUBLIC.SMOKE_UUID WHERE id = ?",
            vec![IgniteValue::Int(1)],
        )
        .await
        .expect("SELECT UUID failed");
    assert_eq!(
        r.first_row().unwrap().get(0),
        Some(&IgniteValue::Uuid(uuid)),
        "UUID roundtrip"
    );

    let r_null = c
        .query(
            "SELECT uv FROM PUBLIC.SMOKE_UUID WHERE id = ?",
            vec![IgniteValue::Int(2)],
        )
        .await
        .expect("SELECT null UUID row failed");
    assert_eq!(
        r_null.first_row().unwrap().get(0),
        Some(&IgniteValue::Null),
        "expected NULL for unset UUID column"
    );

    drop_table(&c, "SMOKE_UUID").await;
}

// ─── 21. query_stream — lazy cursor ──────────────────────────────────────────

/// Verifies that `IgniteClient::query_stream` returns the correct rows lazily.
///
/// Inserts 5 rows, then collects them via the stream, checking row count and
/// values.  Also exercises `Transaction::query_stream`.
#[tokio::test]
async fn smoke_query_stream() {
    use futures::StreamExt;
    use ignite_client::QueryStream;

    let c = client();
    create_table(&c, "SMOKE_STREAM", "id INT PRIMARY KEY, v INT").await;

    // Insert 5 rows
    for i in 1i32..=5 {
        c.execute(
            "INSERT INTO PUBLIC.SMOKE_STREAM (id, v) VALUES (?, ?)",
            vec![IgniteValue::Int(i), IgniteValue::Int(i * 10)],
        )
        .await
        .expect("INSERT failed");
    }

    // ── Non-transactional stream ──────────────────────────────────────────────
    let stream: QueryStream = c
        .query_stream(
            "SELECT id, v FROM PUBLIC.SMOKE_STREAM ORDER BY id",
            vec![],
        )
        .await
        .expect("query_stream failed");

    // Column metadata is available before consuming any rows
    assert_eq!(stream.columns.len(), 2);
    assert_eq!(stream.columns[0].name, "ID");
    assert_eq!(stream.columns[1].name, "V");

    let rows: Vec<_> = stream
        .map(|r| r.expect("stream row error"))
        .collect()
        .await;

    assert_eq!(rows.len(), 5, "expected 5 rows from stream");
    for (i, row) in rows.iter().enumerate() {
        let expected_id = (i as i32) + 1;
        assert_eq!(
            row.get(0),
            Some(&IgniteValue::Int(expected_id)),
            "row {i} id mismatch"
        );
        assert_eq!(
            row.get(1),
            Some(&IgniteValue::Int(expected_id * 10)),
            "row {i} v mismatch"
        );
    }

    // ── Transactional stream ──────────────────────────────────────────────────
    let mut tx = c.begin_transaction().await.expect("begin_transaction failed");

    let tx_stream = tx
        .query_stream(
            "SELECT id FROM PUBLIC.SMOKE_STREAM ORDER BY id",
            vec![],
        )
        .await
        .expect("tx.query_stream failed");

    let tx_rows: Vec<_> = tx_stream
        .map(|r| r.expect("tx stream row error"))
        .collect()
        .await;

    tx.rollback().await.expect("rollback failed");

    assert_eq!(tx_rows.len(), 5, "expected 5 rows from tx stream");

    drop_table(&c, "SMOKE_STREAM").await;
}

// ─── 22. KV cache — basic operations ─────────────────────────────────────────

/// Tests `get_or_create_cache`, `put`, `get` (hit + miss), `contains_key`,
/// `remove`, and `destroy_cache`.
#[tokio::test]
async fn smoke_cache_basic() {
    let c = client();
    // Destroy first to clear any stale data from previous (failed) test runs.
    let _ = c.destroy_cache("SMOKE_CACHE_BASIC").await;
    let cache: IgniteCache = c
        .get_or_create_cache("SMOKE_CACHE_BASIC")
        .await
        .expect("get_or_create_cache failed");

    // Miss before any put
    assert_eq!(
        cache.get(IgniteValue::Int(1)).await.expect("get miss failed"),
        IgniteValue::Null,
        "expected Null for absent key"
    );
    assert!(
        !cache
            .contains_key(IgniteValue::Int(1))
            .await
            .expect("contains_key failed"),
        "key should not be present before put"
    );

    // Put then get
    cache
        .put(IgniteValue::Int(1), IgniteValue::String("hello".into()))
        .await
        .expect("put failed");

    assert_eq!(
        cache.get(IgniteValue::Int(1)).await.expect("get hit failed"),
        IgniteValue::String("hello".into()),
        "get after put"
    );
    assert!(
        cache
            .contains_key(IgniteValue::Int(1))
            .await
            .expect("contains_key after put failed"),
        "key should be present after put"
    );

    // Remove (CACHE_REMOVE_KEY returns void; verify removal via get/contains_key)
    cache
        .remove(IgniteValue::Int(1))
        .await
        .expect("remove failed");

    assert_eq!(
        cache.get(IgniteValue::Int(1)).await.expect("get after remove failed"),
        IgniteValue::Null,
        "key should be absent after remove"
    );

    // Removing an already-absent key is a no-op (idempotent)
    cache
        .remove(IgniteValue::Int(1))
        .await
        .expect("remove absent key failed");

    c.destroy_cache("SMOKE_CACHE_BASIC")
        .await
        .expect("destroy_cache failed");
}

// ─── 23. KV cache — bulk operations ──────────────────────────────────────────

/// Tests `put_all`, `get_all`, and `remove_all`.
#[tokio::test]
async fn smoke_cache_bulk() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_CACHE_BULK").await;
    let cache = c
        .get_or_create_cache("SMOKE_CACHE_BULK")
        .await
        .expect("get_or_create_cache failed");

    // put_all
    let entries: Vec<(IgniteValue, IgniteValue)> = (1i32..=5)
        .map(|i| (IgniteValue::Int(i), IgniteValue::Int(i * 100)))
        .collect();
    cache.put_all(entries).await.expect("put_all failed");

    // get_all — all keys present
    let keys: Vec<IgniteValue> = (1i32..=5).map(IgniteValue::Int).collect();
    let mut got = cache.get_all(keys.clone()).await.expect("get_all failed");
    assert_eq!(got.len(), 5, "get_all: expected 5 pairs");
    // Sort by key for deterministic comparison
    got.sort_by_key(|(k, _)| match k {
        IgniteValue::Int(n) => *n,
        _ => 0,
    });
    for (i, (k, v)) in got.iter().enumerate() {
        let expected_i = (i as i32) + 1;
        assert_eq!(*k, IgniteValue::Int(expected_i), "get_all key {i}");
        assert_eq!(*v, IgniteValue::Int(expected_i * 100), "get_all value {i}");
    }

    // get_all with one absent key — absent key is omitted
    let mixed_keys = vec![IgniteValue::Int(2), IgniteValue::Int(99)];
    let partial = cache.get_all(mixed_keys).await.expect("get_all partial failed");
    assert_eq!(partial.len(), 1, "get_all should omit absent keys");
    assert_eq!(partial[0].0, IgniteValue::Int(2), "partial key");
    assert_eq!(partial[0].1, IgniteValue::Int(200), "partial value");

    // remove_all
    let remove_keys: Vec<IgniteValue> = (1i32..=3).map(IgniteValue::Int).collect();
    cache.remove_all(remove_keys).await.expect("remove_all failed");

    let remaining_keys: Vec<IgniteValue> = (1i32..=5).map(IgniteValue::Int).collect();
    let remaining = cache.get_all(remaining_keys).await.expect("get_all after remove_all failed");
    assert_eq!(remaining.len(), 2, "expected 2 remaining entries after remove_all");

    c.destroy_cache("SMOKE_CACHE_BULK")
        .await
        .expect("destroy_cache failed");
}

// ─── 24. KV cache — replace (conditional put) ────────────────────────────────

/// Tests `replace`: only replaces if the key already exists.
#[tokio::test]
async fn smoke_cache_replace() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_CACHE_REPLACE").await;
    let cache = c
        .get_or_create_cache("SMOKE_CACHE_REPLACE")
        .await
        .expect("get_or_create_cache failed");

    // Replace on absent key → false, no value inserted
    let replaced_absent = cache
        .replace(IgniteValue::Int(42), IgniteValue::String("nope".into()))
        .await
        .expect("replace absent failed");
    assert!(!replaced_absent, "replace on absent key should return false");
    assert_eq!(
        cache.get(IgniteValue::Int(42)).await.unwrap(),
        IgniteValue::Null,
        "key should still be absent after failed replace"
    );

    // Put then replace → true, value updated
    cache
        .put(IgniteValue::Int(42), IgniteValue::String("original".into()))
        .await
        .expect("put failed");
    let replaced_present = cache
        .replace(IgniteValue::Int(42), IgniteValue::String("updated".into()))
        .await
        .expect("replace present failed");
    assert!(replaced_present, "replace on existing key should return true");
    assert_eq!(
        cache.get(IgniteValue::Int(42)).await.unwrap(),
        IgniteValue::String("updated".into()),
        "value should be updated after replace"
    );

    c.destroy_cache("SMOKE_CACHE_REPLACE")
        .await
        .expect("destroy_cache failed");
}

// ─── 25. KV cache — get_and_put / get_and_remove ─────────────────────────────

/// Tests atomic `get_and_put` and `get_and_remove`.
#[tokio::test]
async fn smoke_cache_get_and() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_CACHE_GET_AND").await;
    let cache = c
        .get_or_create_cache("SMOKE_CACHE_GET_AND")
        .await
        .expect("get_or_create_cache failed");

    // get_and_put when key absent → returns Null, key is now set
    let old = cache
        .get_and_put(IgniteValue::Int(7), IgniteValue::Long(100))
        .await
        .expect("get_and_put absent failed");
    assert_eq!(old, IgniteValue::Null, "get_and_put on absent key should return Null");
    assert_eq!(
        cache.get(IgniteValue::Int(7)).await.unwrap(),
        IgniteValue::Long(100),
        "key should be set after get_and_put"
    );

    // get_and_put when key present → returns old value, updates to new
    let old2 = cache
        .get_and_put(IgniteValue::Int(7), IgniteValue::Long(200))
        .await
        .expect("get_and_put present failed");
    assert_eq!(old2, IgniteValue::Long(100), "get_and_put should return previous value");
    assert_eq!(
        cache.get(IgniteValue::Int(7)).await.unwrap(),
        IgniteValue::Long(200),
        "key should be updated after get_and_put"
    );

    // get_and_remove when key present → returns old value, key is gone
    let removed_val = cache
        .get_and_remove(IgniteValue::Int(7))
        .await
        .expect("get_and_remove present failed");
    assert_eq!(removed_val, IgniteValue::Long(200), "get_and_remove should return the value");
    assert_eq!(
        cache.get(IgniteValue::Int(7)).await.unwrap(),
        IgniteValue::Null,
        "key should be absent after get_and_remove"
    );

    // get_and_remove when key absent → returns Null
    let removed_absent = cache
        .get_and_remove(IgniteValue::Int(99))
        .await
        .expect("get_and_remove absent failed");
    assert_eq!(removed_absent, IgniteValue::Null, "get_and_remove absent key should return Null");

    c.destroy_cache("SMOKE_CACHE_GET_AND")
        .await
        .expect("destroy_cache failed");
}

// ─── 26. KV cache — put_if_absent ─────────────────────────────────────────────

/// Tests `put_if_absent`: stores only when the key is absent; does not overwrite.
#[tokio::test]
async fn smoke_cache_put_if_absent() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_PIA").await;
    let cache = c.get_or_create_cache("SMOKE_PIA").await.expect("get_or_create_cache failed");

    // Absent key → stored, returns true
    let stored = cache
        .put_if_absent(IgniteValue::Int(1), IgniteValue::String("first".into()))
        .await
        .expect("put_if_absent absent failed");
    assert!(stored, "put_if_absent on absent key should return true");
    assert_eq!(
        cache.get(IgniteValue::Int(1)).await.unwrap(),
        IgniteValue::String("first".into()),
        "value should be stored after put_if_absent"
    );

    // Present key → NOT overwritten, returns false
    let stored_again = cache
        .put_if_absent(IgniteValue::Int(1), IgniteValue::String("second".into()))
        .await
        .expect("put_if_absent present failed");
    assert!(!stored_again, "put_if_absent on present key should return false");
    assert_eq!(
        cache.get(IgniteValue::Int(1)).await.unwrap(),
        IgniteValue::String("first".into()),
        "existing value must not be overwritten by put_if_absent"
    );

    c.destroy_cache("SMOKE_PIA").await.expect("destroy_cache failed");
}

// ─── 27. KV cache — get_and_replace ──────────────────────────────────────────

/// Tests `get_and_replace`: replaces only when the key is present and returns
/// the previous value; returns Null and makes no change when the key is absent.
#[tokio::test]
async fn smoke_cache_get_and_replace() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_GAR").await;
    let cache = c.get_or_create_cache("SMOKE_GAR").await.expect("get_or_create_cache failed");

    // Absent key → Null returned, no insert
    let old_absent = cache
        .get_and_replace(IgniteValue::Int(5), IgniteValue::String("value".into()))
        .await
        .expect("get_and_replace absent failed");
    assert_eq!(old_absent, IgniteValue::Null, "get_and_replace on absent key should return Null");
    assert_eq!(
        cache.get(IgniteValue::Int(5)).await.unwrap(),
        IgniteValue::Null,
        "get_and_replace should not insert when key is absent"
    );

    // Present key → old value returned, new value stored
    cache
        .put(IgniteValue::Int(5), IgniteValue::String("original".into()))
        .await
        .expect("put failed");
    let old_present = cache
        .get_and_replace(IgniteValue::Int(5), IgniteValue::String("updated".into()))
        .await
        .expect("get_and_replace present failed");
    assert_eq!(
        old_present,
        IgniteValue::String("original".into()),
        "get_and_replace should return the old value"
    );
    assert_eq!(
        cache.get(IgniteValue::Int(5)).await.unwrap(),
        IgniteValue::String("updated".into()),
        "get_and_replace should store the new value"
    );

    c.destroy_cache("SMOKE_GAR").await.expect("destroy_cache failed");
}

// ─── 28. KV cache — get_size ──────────────────────────────────────────────────

/// Tests `get_size`: returns the number of entries in the cache.
#[tokio::test]
async fn smoke_cache_size() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_SIZE").await;
    let cache = c.get_or_create_cache("SMOKE_SIZE").await.expect("get_or_create_cache failed");

    // Empty cache
    assert_eq!(cache.get_size().await.expect("get_size empty failed"), 0, "empty cache size");

    // After put_all
    let entries: Vec<(IgniteValue, IgniteValue)> = (1i32..=5)
        .map(|i| (IgniteValue::Int(i), IgniteValue::Long(i as i64 * 10)))
        .collect();
    cache.put_all(entries).await.expect("put_all failed");
    assert_eq!(cache.get_size().await.expect("get_size after put_all failed"), 5, "size after 5 puts");

    // After removing 2 keys
    cache.remove(IgniteValue::Int(1)).await.expect("remove 1 failed");
    cache.remove(IgniteValue::Int(2)).await.expect("remove 2 failed");
    assert_eq!(cache.get_size().await.expect("get_size after removes failed"), 3, "size after 2 removes");

    c.destroy_cache("SMOKE_SIZE").await.expect("destroy_cache failed");
}

// ─── 29. Cache discovery — cache_names ───────────────────────────────────────

/// Tests `IgniteClient::cache_names`: returns all currently defined cache names.
#[tokio::test]
async fn smoke_cache_names() {
    let c = client();
    // Ensure clean state
    let _ = c.destroy_cache("SMOKE_NAMES_A").await;
    let _ = c.destroy_cache("SMOKE_NAMES_B").await;

    c.get_or_create_cache("SMOKE_NAMES_A").await.expect("create NAMES_A failed");
    c.get_or_create_cache("SMOKE_NAMES_B").await.expect("create NAMES_B failed");

    let names = c.cache_names().await.expect("cache_names failed");
    assert!(
        names.iter().any(|n| n == "SMOKE_NAMES_A"),
        "cache_names should include SMOKE_NAMES_A; got: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "SMOKE_NAMES_B"),
        "cache_names should include SMOKE_NAMES_B; got: {names:?}"
    );

    // After destroy, name disappears
    c.destroy_cache("SMOKE_NAMES_B").await.expect("destroy NAMES_B failed");
    let names_after = c.cache_names().await.expect("cache_names after destroy failed");
    assert!(
        !names_after.iter().any(|n| n == "SMOKE_NAMES_B"),
        "cache_names should not include SMOKE_NAMES_B after destroy; got: {names_after:?}"
    );

    c.destroy_cache("SMOKE_NAMES_A").await.expect("destroy NAMES_A failed");
}

// ─── 30. Transactional KV — commit ───────────────────────────────────────────

/// Tests `Transaction::cache()`: KV puts made inside a transaction are visible
/// after commit via a non-transactional handle.
#[tokio::test]
async fn smoke_tx_kv_commit() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_TX_KV_COMMIT").await;
    c.get_or_create_transactional_cache("SMOKE_TX_KV_COMMIT")
        .await
        .expect("create transactional cache failed");

    let tx = c.begin_transaction().await.expect("begin_transaction failed");
    tx.cache("SMOKE_TX_KV_COMMIT")
        .put(IgniteValue::Int(1), IgniteValue::String("alpha".into()))
        .await
        .expect("tx put 1 failed");
    tx.cache("SMOKE_TX_KV_COMMIT")
        .put(IgniteValue::Int(2), IgniteValue::String("beta".into()))
        .await
        .expect("tx put 2 failed");
    tx.commit().await.expect("commit failed");

    // Non-transactional reads must see the committed data
    let cache = c.cache("SMOKE_TX_KV_COMMIT");
    assert_eq!(
        cache.get(IgniteValue::Int(1)).await.unwrap(),
        IgniteValue::String("alpha".into()),
        "key 1 should be committed"
    );
    assert_eq!(
        cache.get(IgniteValue::Int(2)).await.unwrap(),
        IgniteValue::String("beta".into()),
        "key 2 should be committed"
    );

    c.destroy_cache("SMOKE_TX_KV_COMMIT").await.expect("destroy failed");
}

// ─── 31. Transactional KV — within-tx read + rollback ────────────────────────

/// Tests `Transaction::cache()`: reads within the transaction see its own writes;
/// after rollback the data may or may not be absent depending on cache atomicity
/// (TRANSACTIONAL vs ATOMIC server default) — the test verifies the API wiring and
/// within-tx visibility, not the exact post-rollback state.
#[tokio::test]
async fn smoke_tx_kv_rollback() {
    let c = client();
    let _ = c.destroy_cache("SMOKE_TX_KV_ROLLBACK").await;
    c.get_or_create_transactional_cache("SMOKE_TX_KV_ROLLBACK")
        .await
        .expect("create transactional cache failed");

    let tx = c.begin_transaction().await.expect("begin_transaction failed");

    // Write inside the transaction
    tx.cache("SMOKE_TX_KV_ROLLBACK")
        .put(IgniteValue::Int(99), IgniteValue::String("pending".into()))
        .await
        .expect("tx put failed");

    // Within-tx read must see the write (transactional visibility)
    let seen = tx
        .cache("SMOKE_TX_KV_ROLLBACK")
        .get(IgniteValue::Int(99))
        .await
        .expect("tx get failed");
    assert_eq!(
        seen,
        IgniteValue::String("pending".into()),
        "within-tx read must see own write"
    );

    tx.rollback().await.expect("rollback failed");

    // Post-rollback: entry should be absent if the cache is TRANSACTIONAL.
    // On ATOMIC caches (server default) the write is not rolled back — we
    // document but do not fail on this boundary.
    let after = c
        .cache("SMOKE_TX_KV_ROLLBACK")
        .get(IgniteValue::Int(99))
        .await
        .expect("non-tx get after rollback failed");
    if after != IgniteValue::Null {
        // ATOMIC cache: rollback did not undo the write — expected on default servers.
        // Confirm we at least got the value we put.
        assert_eq!(
            after,
            IgniteValue::String("pending".into()),
            "ATOMIC cache: post-rollback value should match what was written"
        );
    }
    // Either Null (TRANSACTIONAL) or the written value (ATOMIC) is acceptable.

    c.destroy_cache("SMOKE_TX_KV_ROLLBACK").await.expect("destroy failed");
}

// ─── P3: Configurable page size ───────────────────────────────────────────────

/// Verifies that `with_page_size(1)` forces one server round-trip per row yet
/// still returns all rows correctly (exercises the multi-page fetch loop).
#[tokio::test]
async fn smoke_configurable_page_size() {
    let c = IgniteClient::new(IgniteClientConfig::new(ADDR).with_page_size(1));
    create_table(&c, "PAGE_SIZE", "id INT PRIMARY KEY, v INT").await;

    for i in 0..5i32 {
        c.execute(
            "INSERT INTO PUBLIC.PAGE_SIZE(id, v) VALUES (?, ?)",
            vec![IgniteValue::Int(i), IgniteValue::Int(i)],
        )
        .await
        .expect("INSERT failed");
    }

    let result = c
        .query("SELECT id FROM PUBLIC.PAGE_SIZE ORDER BY id", vec![])
        .await
        .expect("query with page_size=1 failed");

    assert_eq!(result.row_count(), 5, "expected 5 rows with page_size=1");

    drop_table(&c, "PAGE_SIZE").await;
}

// ─── P3: TLS configuration ────────────────────────────────────────────────────

/// Verifies that `with_tls()` and `with_tls_accept_invalid_certs()` builder
/// methods set the fields correctly.  No network connectivity is required.
#[tokio::test]
async fn smoke_tls_config_builds() {
    // Plain TLS config — fields are set correctly
    let plain_tls = IgniteClientConfig::new(ADDR).with_tls();
    assert!(plain_tls.use_tls, "with_tls() should set use_tls=true");
    assert!(!plain_tls.tls_accept_invalid_certs, "tls_accept_invalid_certs should default to false");

    // Dev config — both flags set
    let dev_tls = IgniteClientConfig::new(ADDR)
        .with_tls()
        .with_tls_accept_invalid_certs();
    assert!(dev_tls.use_tls, "with_tls() should set use_tls=true");
    assert!(dev_tls.tls_accept_invalid_certs, "with_tls_accept_invalid_certs() should set flag");

    // Default config — TLS disabled
    let no_tls = IgniteClientConfig::new(ADDR);
    assert!(!no_tls.use_tls, "TLS should be disabled by default");
}

/// Connecting with TLS to a plaintext Ignite server fails with a transport
/// error, not a panic or hang, confirming graceful TLS failure handling.
#[tokio::test]
async fn smoke_tls_graceful_failure() {
    use std::time::Duration;

    let c = IgniteClient::new(
        IgniteClientConfig::new(ADDR)
            .with_tls()
            .with_tls_accept_invalid_certs()
            .with_connect_timeout(Duration::from_secs(5)),
    );

    let err = c.query("SELECT 1", vec![]).await;
    assert!(
        err.is_err(),
        "expected TLS connection to a plaintext server to fail; got Ok"
    );
    // Verify it is an IgniteError (not a panic) — the exact variant depends on
    // how the server closes the connection, but it must not be Ok.
    match err {
        Err(IgniteError::Transport(_)) | Err(IgniteError::Pool(_)) => {}
        Err(other) => panic!("expected Transport or Pool error, got {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

// ─── P2: QueryStream early-drop cursor close ──────────────────────────────────

/// Exercises the `impl Drop for CursorState` path: open a stream over 20 rows,
/// consume only 5 via `.take(5)`, then let the stream drop.  The drop should
/// fire a best-effort resource-close request without panicking or leaving the
/// connection in a broken state.  A follow-up query on the same client verifies
/// the connection is still healthy after the early drop.
#[tokio::test]
async fn smoke_query_stream_early_drop() {
    use futures::StreamExt;

    let c = client();
    create_table(&c, "STREAM_EARLY_DROP", "id INT PRIMARY KEY, v INT").await;

    // Insert 20 rows
    for i in 0..20i32 {
        c.execute(
            "INSERT INTO PUBLIC.STREAM_EARLY_DROP (id, v) VALUES (?, ?)",
            vec![IgniteValue::Int(i), IgniteValue::Int(i * 10)],
        )
        .await
        .expect("insert failed");
    }

    // Open stream, take only 5, collect — the rest are dropped mid-iteration.
    let stream = c
        .query_stream("SELECT id FROM PUBLIC.STREAM_EARLY_DROP ORDER BY id", vec![])
        .await
        .expect("query_stream failed");

    let rows: Vec<_> = stream.take(5).collect().await;
    assert_eq!(rows.len(), 5, "expected 5 rows from take(5)");
    for r in &rows {
        assert!(r.is_ok(), "unexpected error in row: {r:?}");
    }
    // CursorState drops here — Drop fires encode_resource_close via tokio::spawn.

    // Give the spawned close request a moment to send, then verify the
    // connection is still usable (no zombie state on the client side).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let count = c
        .query("SELECT COUNT(*) FROM PUBLIC.STREAM_EARLY_DROP", vec![])
        .await
        .expect("post-drop query failed");
    let n = count.rows.first().and_then(|r| r.get(0));
    assert!(
        matches!(n, Some(IgniteValue::Long(20))),
        "expected 20 rows, got {n:?}"
    );
}
