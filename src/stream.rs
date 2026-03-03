use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use crate::protocol::messages::{
    encode_cursor_get_page, encode_resource_close, SqlFieldsFirstPage, SqlFieldsPage,
};
use crate::protocol::op_code;
use crate::transport::{next_request_id, IgniteConnection};

use crate::error::{IgniteError, Result};
use crate::query::{Column, Row};

// ─── Public type ──────────────────────────────────────────────────────────────

/// A lazily-paged result stream returned by [`crate::IgniteClient::query_stream`]
/// and [`crate::transaction::Transaction::query_stream`].
///
/// Rows are yielded one at a time as the consumer polls the stream.  Subsequent
/// pages are fetched from Ignite only when the current page is exhausted — the
/// server-side cursor is never over-read.  The cursor is closed automatically
/// when the stream reaches the last row or is dropped mid-stream.
///
/// # Example
/// ```no_run
/// use futures::StreamExt;
/// // let mut stream = client.query_stream("SELECT id FROM t", vec![]).await?;
/// // while let Some(row) = stream.next().await {
/// //     let row = row?;
/// //     println!("{:?}", row.get(0));
/// // }
/// ```
pub struct QueryStream {
    /// Column metadata for the result set, available immediately (before any
    /// rows are consumed).
    pub columns: Vec<Column>,
    inner: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>,
}

impl QueryStream {
    pub(crate) fn new(
        columns: Vec<Column>,
        inner: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>,
    ) -> Self {
        Self { columns, inner }
    }
}

impl QueryStream {
    /// Drain the entire stream into a `Vec`, propagating the first error encountered.
    ///
    /// This is a convenience wrapper so callers don't need to import
    /// `futures::TryStreamExt` or manage the polling loop manually.
    ///
    /// # Example
    /// ```no_run
    /// # use ignite_client::{IgniteClient, IgniteClientConfig};
    /// # #[tokio::main] async fn main() {
    /// # let client = IgniteClient::new(IgniteClientConfig::new("localhost:10800"));
    /// let rows = client
    ///     .query_stream("SELECT id FROM PUBLIC.users", vec![])
    ///     .await.unwrap()
    ///     .collect_all()
    ///     .await.unwrap();
    /// println!("{} rows", rows.len());
    /// # }
    /// ```
    pub async fn collect_all(mut self) -> crate::error::Result<Vec<Row>> {
        use futures::StreamExt;
        let mut rows = Vec::new();
        while let Some(result) = self.next().await {
            rows.push(result?);
        }
        Ok(rows)
    }
}

impl Stream for QueryStream {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

// ─── Internal cursor state ────────────────────────────────────────────────────

struct CursorState {
    /// Rows buffered from the most-recently-fetched page.
    buffer: VecDeque<Row>,
    /// Column descriptors, cloned into each `Row`.
    columns: Vec<Column>,
    /// Ignite cursor handle.
    cursor_id: i64,
    /// Whether the server has more pages after the current one.
    has_more: bool,
    /// Number of fields per row (needed for subsequent page decoding).
    field_count: usize,
    /// Connection to send cursor-get-page / resource-close requests on.
    conn: Arc<IgniteConnection>,
}

impl Drop for CursorState {
    /// Guarantee that the server-side cursor is always closed — both when the
    /// stream is naturally exhausted and when `QueryStream` is dropped
    /// mid-iteration (e.g. via `.take(n)` or an early `break`).
    ///
    /// `tokio::spawn` is used because `drop` cannot be async.  The close is
    /// best-effort: if the spawn or request fails, Ignite will clean up the
    /// cursor when its own idle timeout expires.
    fn drop(&mut self) {
        let req_id = next_request_id();
        let payload = encode_resource_close(req_id, self.cursor_id);
        let conn = Arc::clone(&self.conn);
        tokio::spawn(async move {
            let _ = conn.request(req_id, payload).await;
        });
    }
}

// ─── Builder ──────────────────────────────────────────────────────────────────

/// Build a [`QueryStream`] from the first-page response of a `QUERY_SQL_FIELDS`
/// request.  Subsequent pages are fetched lazily as the consumer pulls rows.
///
/// `conn` must be a live connection to the same Ignite node that opened the
/// cursor (the cursor is node-local).
pub(crate) fn build_stream(
    conn: Arc<IgniteConnection>,
    first: SqlFieldsFirstPage,
) -> QueryStream {
    let columns: Vec<Column> = first
        .field_names
        .iter()
        .map(|n| Column { name: n.clone() })
        .collect();

    let field_count = columns.len();
    let cursor_id = first.cursor_id;
    let has_more = first.has_more;

    let buffer: VecDeque<Row> = first
        .rows
        .into_iter()
        .map(|vals| Row::new(columns.clone(), vals))
        .collect();

    let state = CursorState {
        buffer,
        columns: columns.clone(),
        cursor_id,
        has_more,
        field_count,
        conn,
    };

    let inner = futures::stream::unfold(state, |mut s| async move {
        loop {
            // ① Yield a buffered row.
            if let Some(row) = s.buffer.pop_front() {
                return Some((Ok(row), s));
            }

            // ② Buffer empty and no more pages — end stream.
            //    `Drop for CursorState` will send encode_resource_close when
            //    `s` is dropped here, so no explicit close call is needed.
            if !s.has_more {
                return None;
            }

            // ③ Fetch the next page and loop back to ①.
            let req_id = next_request_id();
            let payload = encode_cursor_get_page(
                op_code::QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
                req_id,
                s.cursor_id,
            );

            let mut response = match s.conn.request(req_id, payload).await {
                Err(e) => return Some((Err(IgniteError::Transport(e)), s)),
                Ok(r) => r,
            };

            let page = match SqlFieldsPage::decode(&mut response, s.field_count) {
                Err(e) => return Some((Err(IgniteError::Protocol(e)), s)),
                Ok(p) => p,
            };

            s.has_more = page.has_more;
            s.buffer = page
                .rows
                .into_iter()
                .map(|vals| Row::new(s.columns.clone(), vals))
                .collect();
            // Loop back to pop the first row from the new buffer.
        }
    });

    QueryStream::new(columns, Box::pin(inner))
}
