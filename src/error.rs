use thiserror::Error;

#[derive(Debug, Error)]
pub enum IgniteError {
    #[error("transport error: {0}")]
    Transport(#[from] crate::transport::TransportError),

    #[error("protocol error: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),

    #[error("pool error: {0}")]
    Pool(String),

    /// Returned by `check_active()` inside a transaction.  In practice this guard
    /// is unreachable from external code because `commit()` and `rollback()` both
    /// consume `self`, but the check is retained for defensive correctness.
    #[error("transaction already committed or rolled back")]
    TransactionFinished,

    /// Reserved for future use when stream cursor state is explicitly tracked.
    /// Currently never returned — stream resource-close is fire-and-forget.
    #[error("cursor already consumed or closed")]
    CursorClosed,

    /// Reserved for future single-row convenience helpers such as
    /// `QueryResult::exactly_one()`.  Currently never returned by any code path.
    #[error("query returned no rows")]
    NoRows,
}

pub type Result<T> = std::result::Result<T, IgniteError>;

impl From<deadpool::managed::PoolError<crate::transport::TransportError>> for IgniteError {
    fn from(e: deadpool::managed::PoolError<crate::transport::TransportError>) -> Self {
        IgniteError::Pool(e.to_string())
    }
}
