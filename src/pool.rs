use std::fmt;
use std::time::Duration;

use deadpool::managed::{Manager, Metrics, RecycleError, RecycleResult, Timeouts};
use deadpool::Runtime;

use crate::protocol::handshake::HandshakeRequest;
use crate::transport::{IgniteConnection, TransportError};

// ─── Client configuration ─────────────────────────────────────────────────────

#[derive(Clone)]
pub struct IgniteClientConfig {
    /// "host:port" address of a cluster node
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub max_pool_size: usize,
    /// Maximum time allowed for TCP connect + TLS/handshake (also used as
    /// deadpool `wait` and `create` timeout).  Default: 10 s.
    pub connect_timeout: Duration,
    /// Per-request response deadline applied in every `send_and_receive` call.
    /// Default: 30 s.
    pub request_timeout: Duration,
    /// Number of rows fetched per server round-trip in SQL queries.
    /// A smaller value reduces memory pressure but increases round-trips.
    /// Default: 1024.
    pub page_size: usize,
    /// Connect using TLS/SSL.  Default: false.
    pub use_tls: bool,
    /// Skip server certificate verification — for development / self-signed
    /// certificates only.  Has no effect when `use_tls` is false.
    /// Default: false.
    pub tls_accept_invalid_certs: bool,
}

impl IgniteClientConfig {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            username: None,
            password: None,
            max_pool_size: 10,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            page_size: 1024,
            use_tls: false,
            tls_accept_invalid_certs: false,
        }
    }

    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_pool_size(mut self, size: usize) -> Self {
        self.max_pool_size = size;
        self
    }

    /// Override the TCP connect + handshake timeout (default: 10 s).
    pub fn with_connect_timeout(mut self, duration: Duration) -> Self {
        self.connect_timeout = duration;
        self
    }

    /// Override the per-request response timeout (default: 30 s).
    pub fn with_request_timeout(mut self, duration: Duration) -> Self {
        self.request_timeout = duration;
        self
    }

    /// Override the SQL query page size (rows per server round-trip).
    /// Default: 1024.  Use a smaller value (e.g. 1) to force multi-page
    /// fetches in tests, or a larger value to reduce round-trips for large
    /// result sets.
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size;
        self
    }

    /// Enable TLS for all connections.  Default: false.
    pub fn with_tls(mut self) -> Self {
        self.use_tls = true;
        self
    }

    /// Skip server certificate verification (development / self-signed certs only).
    /// Has no effect unless `with_tls()` is also called.
    pub fn with_tls_accept_invalid_certs(mut self) -> Self {
        self.tls_accept_invalid_certs = true;
        self
    }
}

impl fmt::Debug for IgniteClientConfig {
    /// Custom impl so the password is never printed in plaintext.
    /// Any logging of a config object will show `password: Some("[REDACTED]")`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IgniteClientConfig")
            .field("address", &self.address)
            .field("username", &self.username)
            .field("password", &self.password.as_deref().map(|_| "[REDACTED]"))
            .field("max_pool_size", &self.max_pool_size)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("page_size", &self.page_size)
            .field("use_tls", &self.use_tls)
            .field("tls_accept_invalid_certs", &self.tls_accept_invalid_certs)
            .finish()
    }
}

// ─── Connection pool internals ────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct IgniteConnectionManager {
    config: IgniteClientConfig,
}

impl IgniteConnectionManager {
    pub(crate) fn new(config: IgniteClientConfig) -> Self {
        Self { config }
    }
}

impl Manager for IgniteConnectionManager {
    type Type = IgniteConnection;
    type Error = TransportError;

    async fn create(&self) -> std::result::Result<IgniteConnection, TransportError> {
        let hs = HandshakeRequest::new(
            self.config.username.clone(),
            self.config.password.clone(),
        );
        let tls = if self.config.use_tls {
            Some(crate::transport::build_tls_config(self.config.tls_accept_invalid_certs)?)
        } else {
            None
        };
        IgniteConnection::connect(
            &self.config.address,
            hs,
            Some(self.config.connect_timeout),
            Some(self.config.request_timeout),
            tls,
        )
        .await
    }

    async fn recycle(
        &self,
        conn: &mut IgniteConnection,
        _metrics: &Metrics,
    ) -> RecycleResult<TransportError> {
        if conn.is_alive() {
            Ok(())
        } else {
            tracing::warn!(address = %self.config.address, "discarding dead connection from pool");
            Err(RecycleError::message("connection is dead"))
        }
    }
}

pub(crate) type Pool = deadpool::managed::Pool<IgniteConnectionManager>;

// Pool builder only errors when max_size=0; our config default (10) prevents that.
#[allow(clippy::expect_used)]
pub(crate) fn build_pool(config: IgniteClientConfig) -> Pool {
    let max_size = config.max_pool_size;
    let connect_timeout = config.connect_timeout;
    let manager = IgniteConnectionManager::new(config);
    Pool::builder(manager)
        .max_size(max_size)
        // Runtime must be specified whenever timeouts are used; Tokio1 matches
        // the workspace's tokio dependency.
        .runtime(Runtime::Tokio1)
        .timeouts(Timeouts {
            // wait: how long to block waiting for a free slot in the pool.
            wait: Some(connect_timeout),
            // create: how long `Manager::create` may take (TCP connect + handshake).
            create: Some(connect_timeout),
            // recycle: how long `Manager::recycle` may take.
            recycle: Some(Duration::from_secs(2)),
        })
        .build()
        .expect("pool construction is infallible with valid config")
}
