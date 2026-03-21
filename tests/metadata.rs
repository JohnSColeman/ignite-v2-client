//! Metadata tests — Rust port of the relevant portions of Apache Ignite's
//! `JdbcThinMetadataSelfTest.java`.
//!
//! Java source reference:
//! <https://raw.githubusercontent.com/apache/ignite/refs/heads/master/modules/clients/src/test/java/org/apache/ignite/jdbc/thin/JdbcThinMetadataSelfTest.java>
//!
//! **Scope:** the Java test class relies on JDBC `DatabaseMetaData`
//! (`getTables`, `getColumns`, `getIndexInfo`, …) which has no equivalent in
//! the thin-client protocol.  Tests that cannot be adapted are omitted; the
//! relevant metadata is instead retrieved by querying the built-in `SYS.*`
//! system views directly via `OP_QUERY_SQL_FIELDS`.
//!
//! **Java → Rust mapping**
//!
//! | Java test | Rust test |
//! |-----------|-----------|
//! | `testResultSetMetaData` | `metadata_result_set_column_names_and_types` |
//! | `testDecimalAndDateTypeMetaData` | `metadata_decimal_and_date_column_types` |
//! | `testGetTables` | `metadata_tables_visible_in_sys_tables_view` |
//! | `testGetAllTables` | `metadata_public_tables_present_in_sys_tables_view` |
//! | `testSchemasMetadata` | `metadata_schemas_visible_in_sys_schemas_view` |
//! | `testEmptySchemasMetadata` | `metadata_sys_schemas_filter_returns_no_rows_for_unknown_pattern` |
//! | `testGetColumns` | `metadata_columns_visible_in_sys_table_columns_view` |
//! | `testIndexMetadata` | `metadata_pk_index_visible_in_sys_indexes_view` |
//! | `testGetAllIndexes` | `metadata_all_table_indexes_present_in_sys_indexes_view` |
//! | `testParametersMetadataNegative` | `metadata_query_unknown_table_returns_error` |
//!
//! Prerequisites: Ignite running on localhost:10800 with no authentication.
//! Run:  cargo test --test metadata -- --nocapture --test-threads=1

use ignite_client::{ColumnType, IgniteClient, IgniteClientConfig, IgniteValue};

const ADDR: &str = "localhost:10800";

fn client() -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR))
}

async fn drop_table(c: &IgniteClient, name: &str) {
    let _ = c
        .execute(&format!("DROP TABLE IF EXISTS PUBLIC.{name}"), vec![])
        .await;
}

async fn create_table(c: &IgniteClient, name: &str, cols: &str) {
    drop_table(c, name).await;
    c.execute(
        &format!("CREATE TABLE PUBLIC.{name} ({cols}) WITH \"template=replicated\""),
        vec![],
    )
    .await
    .unwrap_or_else(|e| panic!("CREATE TABLE {name} failed: {e}"));
}

// ─── 1. testResultSetMetaData ─────────────────────────────────────────────────

/// Ports `testResultSetMetaData`.
///
/// Creates two tables (Person and Org), inserts rows, executes a JOIN query,
/// and verifies that:
/// - the result set has the correct number of columns,
/// - each column has the correct name (case-insensitive),
/// - each value's `column_type()` is the expected [`ColumnType`].
#[tokio::test]
async fn metadata_result_set_column_names_and_types() {
    const PERSON: &str = "META_PERSON_JOIN";
    const ORG: &str = "META_ORG_JOIN";

    let c = client();
    create_table(&c, PERSON, "id INT PRIMARY KEY, name VARCHAR, org_id INT").await;
    create_table(&c, ORG, "id INT PRIMARY KEY, org_name VARCHAR").await;

    c.execute(
        &format!("INSERT INTO PUBLIC.{ORG} (id, org_name) VALUES (10, 'Acme')"),
        vec![],
    )
    .await
    .expect("INSERT org failed");

    c.execute(
        &format!("INSERT INTO PUBLIC.{PERSON} (id, name, org_id) VALUES (1, 'Alice', 10)"),
        vec![],
    )
    .await
    .expect("INSERT person failed");

    let result = c
        .query(
            &format!(
                "SELECT p.name, o.id AS orgid \
                 FROM PUBLIC.{PERSON} p \
                 JOIN PUBLIC.{ORG} o ON p.org_id = o.id"
            ),
            vec![],
        )
        .await
        .expect("JOIN query failed");

    assert_eq!(result.columns.len(), 2, "expected 2 columns");
    assert_eq!(result.row_count(), 1, "expected 1 row");

    // Column 0: name (VARCHAR → String)
    assert_eq!(
        result.columns[0].name.to_uppercase(),
        "NAME",
        "column[0] should be NAME"
    );

    // Column 1: orgid (INT → Int)
    assert_eq!(
        result.columns[1].name.to_uppercase(),
        "ORGID",
        "column[1] should be ORGID"
    );

    // Verify per-value types
    let row = result.first_row().unwrap();
    assert_eq!(row.get(0).unwrap().column_type(), ColumnType::String);
    assert_eq!(row.get(1).unwrap().column_type(), ColumnType::Int);

    drop_table(&c, PERSON).await;
    drop_table(&c, ORG).await;
}

// ─── 2. testDecimalAndDateTypeMetaData ───────────────────────────────────────

/// Ports `testDecimalAndDateTypeMetaData`.
///
/// Creates a table with a DECIMAL and a DATE column, inserts one row using SQL
/// literals, and verifies:
/// - the column names are correct,
/// - `IgniteValue::column_type()` on the returned values is [`ColumnType::Decimal`] / [`ColumnType::Date`].
///
/// **Note on DATE wire encoding:** Ignite 2.x returns DATE column values with
/// type code `0xFE` (OPTM_MARSH) rather than the native date type code.  The
/// codec transparently decodes these to `IgniteValue::Date(ms)`, so
/// `column_type()` still returns [`ColumnType::Date`].
#[tokio::test]
async fn metadata_decimal_and_date_column_types() {
    const TABLE: &str = "META_DEC_DATE";

    let c = client();
    create_table(
        &c,
        TABLE,
        "id INT PRIMARY KEY, dec_col DECIMAL(8, 3), date_col DATE",
    )
    .await;

    // Use SQL literals to avoid needing to construct temporal parameters.
    c.execute(
        &format!(
            "INSERT INTO PUBLIC.{TABLE} (id, dec_col, date_col) \
             VALUES (1, 123.456, '2024-01-15')"
        ),
        vec![],
    )
    .await
    .expect("INSERT failed");

    let result = c
        .query(
            &format!("SELECT dec_col, date_col FROM PUBLIC.{TABLE} WHERE id = 1"),
            vec![],
        )
        .await
        .expect("SELECT failed");

    assert_eq!(result.row_count(), 1, "expected 1 row");
    assert_eq!(result.columns.len(), 2, "expected 2 columns");

    // ── Column 0: DEC_COL (DECIMAL → Decimal) ────────────────────────────────
    assert_eq!(
        result.columns[0].name.to_uppercase(),
        "DEC_COL",
        "column[0] should be DEC_COL"
    );

    // ── Column 1: DATE_COL (DATE → Date) ─────────────────────────────────────
    assert_eq!(
        result.columns[1].name.to_uppercase(),
        "DATE_COL",
        "column[1] should be DATE_COL"
    );

    // ── Per-value column_type() ───────────────────────────────────────────────
    let row = result.first_row().unwrap();
    assert_eq!(
        row.get(0).unwrap().column_type(),
        ColumnType::Decimal,
        "dec_col value.column_type() should be Decimal"
    );
    assert_eq!(
        row.get(1).unwrap().column_type(),
        ColumnType::Date,
        "date_col value.column_type() should be Date"
    );

    // ── The decimal value must round-trip correctly ───────────────────────────
    match row.get(0).unwrap() {
        IgniteValue::Decimal(d) => {
            let s = d.to_string();
            assert!(
                s.starts_with("123.456") || s == "123.456",
                "decimal value should be 123.456, got {s}"
            );
        }
        other => panic!("expected Decimal, got {other:?}"),
    }

    // ── The date value must be a valid IgniteValue::Date(ms) ─────────────────
    assert!(
        matches!(row.get(1).unwrap(), IgniteValue::Date(_)),
        "DATE column should decode to IgniteValue::Date, got {:?}",
        row.get(1)
    );

    drop_table(&c, TABLE).await;
}

// ─── 3. testGetTables (via SYS.TABLES) ───────────────────────────────────────

/// Ports `testGetTables` — adapted to use `SYS.TABLES` instead of JDBC
/// `DatabaseMetaData.getTables()`.
///
/// Creates a test table and verifies it appears in the `SYS.TABLES` system
/// view with the correct schema and table name.
#[tokio::test]
async fn metadata_tables_visible_in_sys_tables_view() {
    const TABLE: &str = "META_TABLES_SYSVW";

    let c = client();
    create_table(&c, TABLE, "id INT PRIMARY KEY, val VARCHAR").await;

    let result = c
        .query(
            "SELECT TABLE_NAME, SCHEMA_NAME FROM SYS.TABLES \
             WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = ?",
            vec![IgniteValue::String(TABLE.to_string())],
        )
        .await
        .expect("SYS.TABLES query failed");

    assert_eq!(
        result.row_count(),
        1,
        "expected exactly 1 row in SYS.TABLES for {TABLE}"
    );

    let row = result.first_row().unwrap();
    let table_name = match row.get(0).unwrap() {
        IgniteValue::String(s) => s.clone(),
        other => panic!("TABLE_NAME should be String, got {other:?}"),
    };
    assert_eq!(table_name, TABLE, "TABLE_NAME mismatch");

    let schema_name = match row.get(1).unwrap() {
        IgniteValue::String(s) => s.clone(),
        other => panic!("SCHEMA_NAME should be String, got {other:?}"),
    };
    assert_eq!(schema_name, "PUBLIC", "SCHEMA_NAME should be PUBLIC");

    drop_table(&c, TABLE).await;
}

// ─── 4. testGetAllTables (PUBLIC tables via SYS.TABLES) ──────────────────────

/// Ports the PUBLIC-schema portion of `testGetAllTables` — verifies that
/// several test tables appear together in `SYS.TABLES`.
///
/// Creates three distinct tables and confirms all three appear in the view.
#[tokio::test]
async fn metadata_public_tables_present_in_sys_tables_view() {
    const TABLES: &[&str] = &["META_ALL_T1", "META_ALL_T2", "META_ALL_T3"];

    let c = client();
    for t in TABLES {
        create_table(&c, t, "id INT PRIMARY KEY, val VARCHAR").await;
    }

    let result = c
        .query(
            "SELECT TABLE_NAME FROM SYS.TABLES \
             WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME LIKE 'META_ALL_%' \
             ORDER BY TABLE_NAME",
            vec![],
        )
        .await
        .expect("SYS.TABLES query failed");

    assert_eq!(
        result.row_count(),
        TABLES.len(),
        "expected {} tables matching META_ALL_%, got {}",
        TABLES.len(),
        result.row_count()
    );

    let returned_names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| match r.get(0)? {
            IgniteValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    for expected in TABLES {
        assert!(
            returned_names.contains(&expected.to_string()),
            "SYS.TABLES should contain {expected}, found: {returned_names:?}"
        );
    }

    for t in TABLES {
        drop_table(&c, t).await;
    }
}

// ─── 5. testSchemasMetadata (via SYS.SCHEMAS) ────────────────────────────────

/// Ports `testSchemasMetadata` — adapted to use `SYS.SCHEMAS` directly.
///
/// Verifies that the mandatory `PUBLIC` and `SYS` schemas are visible, and
/// that each row has a non-null `SCHEMA_NAME`.
#[tokio::test]
async fn metadata_schemas_visible_in_sys_schemas_view() {
    let c = client();

    let result = c
        .query("SELECT SCHEMA_NAME FROM SYS.SCHEMAS", vec![])
        .await
        .expect("SYS.SCHEMAS query failed");

    assert!(
        result.row_count() > 0,
        "SYS.SCHEMAS should return at least one row"
    );

    let schemas: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| match r.get(0)? {
            IgniteValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        schemas.contains(&"PUBLIC".to_string()),
        "SYS.SCHEMAS must include PUBLIC, found: {schemas:?}"
    );
    assert!(
        schemas.contains(&"SYS".to_string()),
        "SYS.SCHEMAS must include SYS, found: {schemas:?}"
    );
}

// ─── 6. testEmptySchemasMetadata ─────────────────────────────────────────────

/// Ports `testEmptySchemasMetadata` — querying `SYS.SCHEMAS` with a filter
/// pattern that matches nothing must return an empty result set.
#[tokio::test]
async fn metadata_sys_schemas_filter_returns_no_rows_for_unknown_pattern() {
    let c = client();

    let result = c
        .query(
            "SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE SCHEMA_NAME = 'DOES_NOT_EXIST_QQQ'",
            vec![],
        )
        .await
        .expect("SYS.SCHEMAS query failed");

    assert_eq!(
        result.row_count(),
        0,
        "non-existent schema pattern should return 0 rows"
    );
}

// ─── 7. testGetColumns (via SYS.TABLE_COLUMNS) ───────────────────────────────

/// Ports `testGetColumns` — adapted to use `SYS.TABLE_COLUMNS` instead of
/// JDBC `DatabaseMetaData.getColumns()`.
///
/// Creates a table with three non-key columns and verifies:
/// - all three column names appear in the view,
/// - the primary-key column is marked `PK = true`,
/// - non-PK columns are marked `PK = false`.
#[tokio::test]
async fn metadata_columns_visible_in_sys_table_columns_view() {
    const TABLE: &str = "META_COL_META";

    let c = client();
    create_table(&c, TABLE, "id INT PRIMARY KEY, name VARCHAR, age INT").await;

    let result = c
        .query(
            "SELECT COLUMN_NAME, PK, NULLABLE \
             FROM SYS.TABLE_COLUMNS \
             WHERE TABLE_NAME = ? AND SCHEMA_NAME = 'PUBLIC' \
             ORDER BY COLUMN_NAME",
            vec![IgniteValue::String(TABLE.to_string())],
        )
        .await
        .expect("SYS.TABLE_COLUMNS query failed");

    // Ignite may add hidden _KEY/_VAL columns — assert our 3 explicit
    // columns are present rather than requiring an exact count.
    assert!(
        result.row_count() >= 3,
        "expected at least 3 rows in SYS.TABLE_COLUMNS for {TABLE}, got {}",
        result.row_count()
    );

    let col_names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| match r.get(0)? {
            IgniteValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        col_names.contains(&"AGE".to_string()),
        "AGE should be in SYS.TABLE_COLUMNS"
    );
    assert!(
        col_names.contains(&"ID".to_string()),
        "ID should be in SYS.TABLE_COLUMNS"
    );
    assert!(
        col_names.contains(&"NAME".to_string()),
        "NAME should be in SYS.TABLE_COLUMNS"
    );

    // Verify PK flag for our three explicitly-declared columns.
    // Ignite may also return hidden _KEY/_VAL columns; those are skipped.
    for row in &result.rows {
        let col_name = match row.get(0).unwrap() {
            IgniteValue::String(s) => s.clone(),
            other => panic!("COLUMN_NAME should be String, got {other:?}"),
        };
        let is_pk = match row.get(1).unwrap() {
            IgniteValue::Bool(b) => *b,
            other => panic!("PK should be Bool for {col_name}, got {other:?}"),
        };
        match col_name.as_str() {
            "ID" => assert!(is_pk, "ID should be marked PK=true"),
            "NAME" => assert!(!is_pk, "NAME should be marked PK=false"),
            "AGE" => assert!(!is_pk, "AGE should be marked PK=false"),
            _ => { /* tolerated: Ignite hidden columns (_KEY, _VAL) */ }
        }
    }

    drop_table(&c, TABLE).await;
}

// ─── 8. testIndexMetadata (via SYS.INDEXES) ──────────────────────────────────

/// Ports `testIndexMetadata` — adapted to use `SYS.INDEXES` instead of JDBC
/// `DatabaseMetaData.getIndexInfo()`.
///
/// Creates a table with a primary key and a secondary index, then verifies:
/// - the primary-key index exists with `IS_PK = true`,
/// - the secondary index exists with `IS_PK = false`.
#[tokio::test]
async fn metadata_pk_index_visible_in_sys_indexes_view() {
    const TABLE: &str = "META_IDX_TEST";

    let c = client();
    create_table(&c, TABLE, "id INT PRIMARY KEY, val VARCHAR").await;

    // Add a secondary index.
    c.execute(
        &format!("CREATE INDEX META_IDX_VAL_IDX ON PUBLIC.{TABLE} (val ASC)"),
        vec![],
    )
    .await
    .expect("CREATE INDEX failed");

    let result = c
        .query(
            "SELECT INDEX_NAME, IS_PK \
             FROM SYS.INDEXES \
             WHERE TABLE_NAME = ? AND SCHEMA_NAME = 'PUBLIC' \
             ORDER BY IS_PK DESC, INDEX_NAME",
            vec![IgniteValue::String(TABLE.to_string())],
        )
        .await
        .expect("SYS.INDEXES query failed");

    assert!(
        result.row_count() >= 2,
        "expected at least 2 indexes (PK + secondary), got {}",
        result.row_count()
    );

    let pk_rows: Vec<_> = result
        .rows
        .iter()
        .filter(|r| matches!(r.get(1), Some(IgniteValue::Bool(true))))
        .collect();

    let secondary_rows: Vec<_> = result
        .rows
        .iter()
        .filter(|r| matches!(r.get(1), Some(IgniteValue::Bool(false))))
        .collect();

    assert!(
        !pk_rows.is_empty(),
        "at least one index should have IS_PK=true"
    );
    assert!(
        !secondary_rows.is_empty(),
        "at least one index should have IS_PK=false"
    );

    // Verify the secondary index name appears
    let secondary_names: Vec<String> = secondary_rows
        .iter()
        .filter_map(|r| match r.get(0)? {
            IgniteValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        secondary_names
            .iter()
            .any(|n| n.to_uppercase().contains("VAL")),
        "secondary index META_IDX_VAL_IDX should appear in SYS.INDEXES, found: {secondary_names:?}"
    );

    drop_table(&c, TABLE).await;
}

// ─── 9. testGetAllIndexes (via SYS.INDEXES) ──────────────────────────────────

/// Ports `testGetAllIndexes` — queries `SYS.INDEXES` globally (no table
/// filter) and verifies that indexes from multiple tables appear together.
///
/// Creates two tables each with a secondary index and confirms all entries
/// appear in `SYS.INDEXES`.
#[tokio::test]
async fn metadata_all_table_indexes_present_in_sys_indexes_view() {
    const TABLE_A: &str = "META_IDX_ALL_A";
    const TABLE_B: &str = "META_IDX_ALL_B";

    let c = client();
    create_table(&c, TABLE_A, "id INT PRIMARY KEY, val VARCHAR").await;
    create_table(&c, TABLE_B, "id INT PRIMARY KEY, score INT").await;

    c.execute(
        &format!("CREATE INDEX META_IDX_ALL_A_VAL ON PUBLIC.{TABLE_A} (val)"),
        vec![],
    )
    .await
    .expect("CREATE INDEX A failed");

    c.execute(
        &format!("CREATE INDEX META_IDX_ALL_B_SCORE ON PUBLIC.{TABLE_B} (score)"),
        vec![],
    )
    .await
    .expect("CREATE INDEX B failed");

    let result = c
        .query(
            "SELECT INDEX_NAME, TABLE_NAME, SCHEMA_NAME, IS_PK \
             FROM SYS.INDEXES \
             WHERE SCHEMA_NAME = 'PUBLIC' \
               AND (TABLE_NAME = ? OR TABLE_NAME = ?) \
             ORDER BY TABLE_NAME, IS_PK DESC, INDEX_NAME",
            vec![
                IgniteValue::String(TABLE_A.to_string()),
                IgniteValue::String(TABLE_B.to_string()),
            ],
        )
        .await
        .expect("SYS.INDEXES query failed");

    assert!(
        result.row_count() >= 4,
        "expected at least 4 index rows (2 PKs + 2 secondary), got {}",
        result.row_count()
    );

    let index_names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| match r.get(0)? {
            IgniteValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(
        index_names
            .iter()
            .any(|n| n.to_uppercase().contains("A_VAL")),
        "META_IDX_ALL_A_VAL should appear in SYS.INDEXES, found: {index_names:?}"
    );
    assert!(
        index_names
            .iter()
            .any(|n| n.to_uppercase().contains("B_SCORE")),
        "META_IDX_ALL_B_SCORE should appear in SYS.INDEXES, found: {index_names:?}"
    );

    drop_table(&c, TABLE_A).await;
    drop_table(&c, TABLE_B).await;
}

// ─── 10. testParametersMetadataNegative (adapted) ────────────────────────────

/// Ports `testParametersMetadataNegative`.
///
/// The Java test verifies that preparing a statement referencing a non-existent
/// table throws `SQLException`.  In the Rust thin client, executing a query
/// against a non-existent table also produces an error.
#[tokio::test]
async fn metadata_query_unknown_table_returns_error() {
    let c = client();

    let result = c
        .query("SELECT * FROM PUBLIC.META_NONEXISTENT_TABLE_XYZ", vec![])
        .await;

    assert!(
        result.is_err(),
        "querying a non-existent table should return an error"
    );
}
