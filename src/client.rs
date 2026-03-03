use std::sync::Arc;

use crate::protocol::handshake::HandshakeRequest;
use crate::protocol::messages::{
    decode_cache_names_response, decode_tx_start_response, encode_cache_create_with_config,
    encode_cache_get_names, encode_tx_start, SqlFieldsRequest,
};
use crate::protocol::{cache_id, op_code, IgniteValue, StatementType, TxConcurrency, TxIsolation};
use crate::transport::{next_request_id, IgniteConnection};

use crate::cache::{destroy_cache_by_name, get_or_create_cache_by_name, IgniteCache};
use crate::error::{IgniteError, Result};
use crate::pool::{self, IgniteClientConfig, Pool};
use crate::query::{QueryResult, UpdateResult};
use crate::stream::{self, QueryStream};
use crate::transaction::{execute_sql_fields, extract_rows_affected, Transaction};

/// The main Ignite client.  Wraps a connection pool; cheap to clone.
#[derive(Clone)]
pub struct IgniteClient {
    pool: Pool,
    config: Arc<IgniteClientConfig>,
}

impl IgniteClient {
    /// Create a new client.  Opens the pool (no connections are made yet).
    pub fn new(config: IgniteClientConfig) -> Self {
        let config = Arc::new(config);
        Self {
            pool: pool::build_pool((*config).clone()),
            config,
        }
    }

    /// Execute a SELECT statement and return all rows.
    ///
    /// # Example
    /// ```no_run
    /// # use ignite_client::{IgniteClient, IgniteClientConfig, IgniteValue};
    /// # #[tokio::main] async fn main() {
    /// let client = IgniteClient::new(IgniteClientConfig::new("localhost:10800"));
    /// let result = client.query(
    ///     "SELECT id, name FROM PUBLIC.users WHERE active = ?",
    ///     vec![IgniteValue::Bool(true)],
    /// ).await.unwrap();
    /// for row in &result.rows {
    ///     println!("{:?}", row.values());
    /// }
    /// # }
    /// ```
    pub async fn query(&self, sql: &str, params: Vec<IgniteValue>) -> Result<QueryResult> {
        let conn_obj = self.pool.get().await?;
        let mut req = SqlFieldsRequest::new(sql, params);
        req.page_size = self.config.page_size as i32;
        execute_sql_fields(&conn_obj, req).await
    }

    /// Execute a SELECT and return rows lazily as a [`QueryStream`].
    ///
    /// The first page is fetched immediately; subsequent pages are fetched on
    /// demand as the stream is polled.  Use [`Self::query`] if you need all
    /// rows in a `Vec` up front.
    ///
    /// The underlying connection is borrowed from the pool for the request and
    /// returned immediately; the stream holds a shared handle (clone) to the
    /// same TCP connection via the multiplexing design.
    pub async fn query_stream(
        &self,
        sql: &str,
        params: Vec<IgniteValue>,
    ) -> Result<QueryStream> {
        use crate::protocol::messages::SqlFieldsFirstPage;
        use crate::protocol::op_code;

        let conn_obj = self.pool.get().await?;
        // Shallow-clone shares the underlying TCP connection; the pool Object
        // can be returned immediately (the slot becomes available again).
        let conn = Arc::new(conn_obj.clone());
        drop(conn_obj);

        let mut req = SqlFieldsRequest::new(sql, params);
        req.page_size = self.config.page_size as i32;
        let req_id = next_request_id();
        let payload = req.encode(op_code::QUERY_SQL_FIELDS, req_id);

        let mut resp = conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;

        let first = SqlFieldsFirstPage::decode(&mut resp, req.include_field_names)
            .map_err(IgniteError::Protocol)?;

        Ok(stream::build_stream(conn, first))
    }

    /// Execute a DML statement (INSERT/UPDATE/DELETE).
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn execute(&self, sql: &str, params: Vec<IgniteValue>) -> Result<UpdateResult> {
        let conn_obj = self.pool.get().await?;
        let req = SqlFieldsRequest {
            statement_type: StatementType::Update,
            ..SqlFieldsRequest::new(sql, params)
        };
        let result = execute_sql_fields(&conn_obj, req).await?;
        Ok(UpdateResult { rows_affected: extract_rows_affected(&result) })
    }

    /// Begin a new transaction with Pessimistic / ReadCommitted isolation (sensible default).
    pub async fn begin_transaction(&self) -> Result<Transaction> {
        self.begin_transaction_with(TxConcurrency::Pessimistic, TxIsolation::ReadCommitted, 0)
            .await
    }

    /// Begin a transaction with explicit concurrency/isolation settings.
    ///
    /// Opens a **dedicated** TCP connection for the transaction's lifetime so
    /// that the connection pool is not held hostage.  The connection is closed
    /// when the Transaction is dropped.
    pub async fn begin_transaction_with(
        &self,
        concurrency: TxConcurrency,
        isolation: TxIsolation,
        timeout_ms: i64,
    ) -> Result<Transaction> {
        // Open a dedicated connection for this transaction
        let hs = HandshakeRequest::new(
            self.config.username.clone(),
            self.config.password.clone(),
        );
        let tls = if self.config.use_tls {
            Some(
                crate::transport::build_tls_config(self.config.tls_accept_invalid_certs)
                    .map_err(IgniteError::Transport)?,
            )
        } else {
            None
        };
        let conn = IgniteConnection::connect(
            &self.config.address,
            hs,
            Some(self.config.connect_timeout),
            Some(self.config.request_timeout),
            tls,
        )
        .await
        .map_err(IgniteError::Transport)?;

        let req_id = next_request_id();
        let payload = encode_tx_start(
            op_code::TX_START,
            req_id,
            concurrency,
            isolation,
            timeout_ms,
            None,
        );

        let mut response = conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;

        let tx_id = decode_tx_start_response(&mut response)
            .map_err(IgniteError::Protocol)?;

        Ok(Transaction::new(tx_id, Arc::new(conn), self.config.page_size as i32))
    }

    /// Convenience: run a closure in a transaction, committing on success.
    /// The closure receives the transaction and must return it alongside its result.
    pub async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Transaction) -> Fut,
        Fut: std::future::Future<Output = Result<(Transaction, T)>>,
    {
        let tx = self.begin_transaction().await?;
        match f(tx).await {
            Ok((tx, result)) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(e) => Err(e),
        }
    }

    /// Pool status for observability.
    pub fn pool_status(&self) -> deadpool::managed::Status {
        self.pool.status()
    }

    // ── KV cache API ──────────────────────────────────────────────────────────

    /// Return a [`IgniteCache`] handle for a cache that is assumed to already
    /// exist.  This is a pure in-process operation (no network round-trip).
    pub fn cache(&self, name: &str) -> IgniteCache {
        IgniteCache::new(cache_id(name), self.pool.clone())
    }

    /// Create the named cache if it does not already exist, then return a
    /// handle to it.  Equivalent to `CACHE_GET_OR_CREATE_WITH_NAME`.
    pub async fn get_or_create_cache(&self, name: &str) -> Result<IgniteCache> {
        get_or_create_cache_by_name(name, &self.pool).await
    }

    /// Create the named cache with **TRANSACTIONAL** atomicity if it does not already exist,
    /// then return a handle to it.  Uses `CACHE_GET_OR_CREATE_WITH_CONFIGURATION` (op 1054).
    ///
    /// Required for caches that will be used inside KV transactions on Ignite ≥ 2.16, which
    /// forbids atomic-cache operations inside transactions.
    pub async fn get_or_create_transactional_cache(&self, name: &str) -> Result<IgniteCache> {
        let req_id = next_request_id();
        let payload = encode_cache_create_with_config(
            op_code::CACHE_GET_OR_CREATE_WITH_CONFIGURATION,
            req_id,
            name,
            true, // transactional = true
        );
        let conn = self.pool.get().await?;
        conn.request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;
        // Response body is void — success means the cache exists with TRANSACTIONAL atomicity.
        Ok(IgniteCache::new(cache_id(name), self.pool.clone()))
    }

    /// Destroy the named cache.  All data is permanently lost.
    pub async fn destroy_cache(&self, name: &str) -> Result<()> {
        destroy_cache_by_name(name, &self.pool).await
    }

    /// Return the names of all caches currently defined on the server.
    pub async fn cache_names(&self) -> Result<Vec<String>> {
        let req_id = next_request_id();
        let payload = encode_cache_get_names(op_code::CACHE_GET_NAMES, req_id);
        let conn = self.pool.get().await?;
        let mut resp = conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;
        decode_cache_names_response(&mut resp).map_err(IgniteError::Protocol)
    }
}
