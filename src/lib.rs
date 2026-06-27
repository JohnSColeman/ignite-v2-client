pub(crate) mod protocol;
pub(crate) mod transport;

pub mod affinity;
pub mod cache;
pub(crate) mod channel;
pub mod discovery;
pub mod client;
pub mod error;
pub mod pool;
pub mod query;
pub mod stream;
pub mod transaction;

pub use cache::IgniteCache;
pub use client::IgniteClient;
pub use error::{IgniteError, Result};
pub use pool::IgniteClientConfig;
pub use protocol::ColumnType;
pub use query::{Column, QueryResult, Row, UpdateResult};
pub use stream::QueryStream;
pub use transaction::Transaction;

// Re-export core value types for convenience
pub use protocol::{
    ExpiryDuration, ExpiryPolicy, IgniteValue, ProtocolError, StatementType, TxConcurrency,
    TxIsolation, cache_id, java_hash,
};
