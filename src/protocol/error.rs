use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("buffer underflow: expected {expected} bytes, got {got}")]
    BufferUnderflow { expected: usize, got: usize },

    #[error("unknown type code: {0}")]
    UnknownTypeCode(u8),

    #[error("unexpected null where value required")]
    UnexpectedNull,

    #[error("invalid UTF-8 in string field")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("handshake failed (server version {server_major}.{server_minor}.{server_patch}): {message}")]
    HandshakeFailed {
        server_major: i16,
        server_minor: i16,
        server_patch: i16,
        message: String,
    },

    #[error("server error (status {status}): {message}")]
    ServerError { status: i32, message: String },

    #[error("decimal decode error: {0}")]
    DecimalError(String),
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
