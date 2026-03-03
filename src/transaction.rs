use std::sync::Arc;

use tracing;

use crate::protocol::messages::{
    encode_tx_end, SqlFieldsRequest,
};
use crate::protocol::op_code;
use crate::protocol::IgniteValue;
use crate::transport::{next_request_id, IgniteConnection, TransportError};

use crate::cache::IgniteCache;
use crate::error::{IgniteError, Result};
use crate::query::{Column, QueryResult, Row, UpdateResult};
use crate::stream::{self, QueryStream};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxState {
    Active,
    Finished,
}

/// An active Ignite transaction.
///
/// Drop without committing will roll back the transaction (best-effort, fire-and-forget).
/// Call `.commit()` to commit or `.rollback()` for explicit rollback.
pub struct Transaction {
    pub(crate) tx_id: i32,
    conn: Arc<IgniteConnection>,
    state: TxState,
    /// Rows-per-page for SQL queries inside this transaction.
    /// Inherited from [`crate::pool::IgniteClientConfig::page_size`] when the
    /// transaction is created via [`crate::IgniteClient::begin_transaction`].
    page_size: i32,
}

impl Transaction {
    pub(crate) fn new(tx_id: i32, conn: Arc<IgniteConnection>, page_size: i32) -> Self {
        Self { tx_id, conn, state: TxState::Active, page_size }
    }

    /// Return a [`IgniteCache`] handle that participates in this transaction.
    ///
    /// Every cache operation on the returned handle embeds the transaction's
    /// `tx_id` in the request flags, so reads and writes are part of the
    /// current transaction and subject to its commit/rollback.
    pub fn cache(&self, name: &str) -> IgniteCache {
        IgniteCache::new_tx(
            crate::protocol::cache_id(name),
            self.tx_id,
            Arc::clone(&self.conn),
        )
    }

    fn check_active(&self) -> Result<()> {
        if self.state == TxState::Finished {
            Err(IgniteError::TransactionFinished)
        } else {
            Ok(())
        }
    }

    /// Execute a SELECT inside this transaction.  All rows fetched eagerly.
    pub async fn query(
        &mut self,
        sql: &str,
        params: Vec<IgniteValue>,
    ) -> Result<QueryResult> {
        self.check_active()?;
        let mut req = SqlFieldsRequest::new(sql, params).with_tx(self.tx_id);
        req.page_size = self.page_size;
        execute_sql_fields(&self.conn, req).await
    }

    /// Execute a SELECT inside this transaction and return rows lazily as a [`QueryStream`].
    ///
    /// The first page is fetched immediately; subsequent pages are fetched on
    /// demand as the stream is polled.  The stream shares the transaction's
    /// dedicated connection, so concurrent queries on the same transaction are
    /// multiplexed safely.
    pub async fn query_stream(
        &mut self,
        sql: &str,
        params: Vec<IgniteValue>,
    ) -> Result<QueryStream> {
        use crate::protocol::messages::SqlFieldsFirstPage;
        use crate::protocol::op_code;

        self.check_active()?;
        let mut req = SqlFieldsRequest::new(sql, params).with_tx(self.tx_id);
        req.page_size = self.page_size;
        let req_id = next_request_id();
        let payload = req.encode(op_code::QUERY_SQL_FIELDS, req_id);

        let mut resp = self
            .conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;

        let first = SqlFieldsFirstPage::decode(&mut resp, req.include_field_names)
            .map_err(IgniteError::Protocol)?;

        Ok(stream::build_stream(Arc::clone(&self.conn), first))
    }

    /// Execute a DML statement (INSERT/UPDATE/DELETE) inside this transaction.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn execute(
        &mut self,
        sql: &str,
        params: Vec<IgniteValue>,
    ) -> Result<UpdateResult> {
        self.check_active()?;
        let req = SqlFieldsRequest {
            statement_type: crate::protocol::StatementType::Update,
            ..SqlFieldsRequest::new(sql, params).with_tx(self.tx_id)
        };
        let result = execute_sql_fields(&self.conn, req).await?;
        Ok(UpdateResult { rows_affected: extract_rows_affected(&result) })
    }

    /// Commit the transaction.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn commit(mut self) -> Result<()> {
        self.check_active()?;
        self.end(true).await?;
        self.state = TxState::Finished;
        Ok(())
    }

    /// Roll back the transaction.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn rollback(mut self) -> Result<()> {
        self.check_active()?;
        self.end(false).await?;
        self.state = TxState::Finished;
        Ok(())
    }

    async fn end(&self, committed: bool) -> Result<()> {
        let req_id = next_request_id();
        let payload = encode_tx_end(op_code::TX_END, req_id, self.tx_id, committed);
        self.conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.state == TxState::Active {
            // Best-effort rollback on drop.  Errors are logged so operators
            // know when a rollback fails (e.g. connection died).
            let req_id = next_request_id();
            let payload = encode_tx_end(op_code::TX_END, req_id, self.tx_id, false);
            let conn = Arc::clone(&self.conn);
            let tx_id = self.tx_id;
            tokio::spawn(async move {
                if let Err(e) = conn.request(req_id, payload).await {
                    tracing::error!("drop-rollback failed for tx_id={tx_id}: {e}");
                }
            });
        }
    }
}

// ─── DML helpers ──────────────────────────────────────────────────────────────

/// Extract the affected-row count returned by Ignite DML statements.
///
/// Ignite encodes the count as the first cell of the first result row, typed
/// as `Long` or (rarely) `Int`.  Returns `-1` if the cell is absent or has an
/// unexpected type.
pub(crate) fn extract_rows_affected(result: &QueryResult) -> i64 {
    result
        .rows
        .first()
        .and_then(|r| r.get(0))
        .and_then(|v| match v {
            IgniteValue::Long(n) => Some(*n),
            IgniteValue::Int(n)  => Some(*n as i64),
            _ => None,
        })
        .unwrap_or(-1)
}

// ─── Shared SQL execution logic ───────────────────────────────────────────────

pub(crate) async fn execute_sql_fields(
    conn: &IgniteConnection,
    req: SqlFieldsRequest,
) -> Result<QueryResult> {
    use crate::protocol::messages::{encode_cursor_get_page, SqlFieldsFirstPage, SqlFieldsPage};
    use crate::protocol::op_code;

    let include_field_names = req.include_field_names;
    let req_id = next_request_id();
    let payload = req.encode(op_code::QUERY_SQL_FIELDS, req_id);

    let mut response = conn
        .request(req_id, payload)
        .await
        .map_err(|e| match e {
            TransportError::Protocol(p) => IgniteError::Protocol(p),
            other => IgniteError::Transport(other),
        })?;

    let first = SqlFieldsFirstPage::decode(&mut response, include_field_names)
        .map_err(IgniteError::Protocol)?;

    let columns: Vec<Column> = first
        .field_names
        .iter()
        .map(|n| Column { name: n.clone() })
        .collect();

    let mut all_rows: Vec<Row> = first
        .rows
        .into_iter()
        .map(|values| Row::new(columns.clone(), values))
        .collect();

    let mut has_more = first.has_more;
    let cursor_id = first.cursor_id;
    let field_count = columns.len();

    // Fetch remaining pages
    while has_more {
        let req_id = next_request_id();
        let payload = encode_cursor_get_page(op_code::QUERY_SQL_FIELDS_CURSOR_GET_PAGE, req_id, cursor_id);
        let mut page_response = conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;

        let page = SqlFieldsPage::decode(&mut page_response, field_count)
            .map_err(IgniteError::Protocol)?;
        for values in page.rows {
            all_rows.push(Row::new(columns.clone(), values));
        }
        has_more = page.has_more;
    }

    // Close cursor (fire-and-forget is acceptable but let's do it properly)
    let close_req_id = next_request_id();
    let close_payload = crate::protocol::messages::encode_resource_close(close_req_id, cursor_id);
    let _ = conn.request(close_req_id, close_payload).await;

    Ok(QueryResult { columns, rows: all_rows })
}
