use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] crate::protocol::ProtocolError),

    #[error("connection closed while awaiting response for request_id={0}")]
    ConnectionClosed(i64),

    /// Not yet returned by any code path — reserved for client-side timeout enforcement
    /// (P0 production hardening item).
    #[error("timeout waiting for response")]
    Timeout,

    /// Returned when TLS handshake or configuration fails.
    #[error("TLS error: {0}")]
    Tls(String),
}

pub type Result<T> = std::result::Result<T, TransportError>;
