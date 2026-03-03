use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::codec::{read_i16_le, read_i32_le, read_u8};
use super::types::type_code;
use super::error::{ProtocolError, Result};

/// Thin client type code (as opposed to node-to-node protocol).
const CLIENT_TYPE_THIN: u8 = 2;

/// Protocol version we request during handshake.
pub const PROTOCOL_MAJOR: i16 = 1;
pub const PROTOCOL_MINOR: i16 = 7;
pub const PROTOCOL_PATCH: i16 = 0;

#[derive(Debug, Clone)]
pub struct HandshakeRequest {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl HandshakeRequest {
    pub fn new(username: Option<String>, password: Option<String>) -> Self {
        Self { username, password }
    }

    /// Serialize to wire format.  The outer length-prefix framing is handled by
    /// the transport layer (LengthDelimitedCodec), so we only produce the payload.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(1); // handshake opcode
        buf.put_i16_le(PROTOCOL_MAJOR);
        buf.put_i16_le(PROTOCOL_MINOR);
        buf.put_i16_le(PROTOCOL_PATCH);
        buf.put_u8(CLIENT_TYPE_THIN);

        // Protocol 1.5+ (Ignite 2.8+) requires a feature-flags byte[] here,
        // inserted between client_code and the auth credentials.
        // We advertise no optional features: empty byte array (type 12, length 0).
        buf.put_u8(type_code::BYTE_ARRAY); // BYTE_ARRAY type code
        buf.put_i32_le(0);                 // zero length — no feature bits set

        // Auth credentials follow as nullable byte[] fields:
        //   None  -- type code 101 (NULL)
        //   Some  -- type code 12 (BYTE_ARRAY) + i32 LE length + UTF-8 bytes
        write_auth_field(&mut buf, self.username.as_deref());
        write_auth_field(&mut buf, self.password.as_deref());
        buf.freeze()
    }
}

/// Encode a nullable auth field (username / password) as the handshake wire
/// format requires: type code 101 for NULL, or type code 12 (BYTE_ARRAY) +
/// i32 LE length + raw UTF-8 bytes for a present value.
fn write_auth_field(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        None => buf.put_u8(type_code::NULL),
        Some(s) => {
            let bytes = s.as_bytes();
            buf.put_u8(type_code::BYTE_ARRAY);
            buf.put_i32_le(bytes.len() as i32);
            buf.put_slice(bytes);
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HandshakeResponse {
    pub success: bool,
    /// Populated on failure
    pub server_major: i16,
    pub server_minor: i16,
    pub server_patch: i16,
    pub error_message: Option<String>,
}

impl HandshakeResponse {
    /// Parse the server's handshake response payload (after length prefix is stripped).
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        let success_byte = read_u8(buf)?;
        if success_byte == 1 {
            // Success: the protocol defines exactly 1 = success, 0 = failure.
            return Ok(Self {
                success: true,
                server_major: 0,
                server_minor: 0,
                server_patch: 0,
                error_message: None,
            });
        }

        // Failure: server version + error string
        let server_major = read_i16_le(buf)?;
        let server_minor = read_i16_le(buf)?;
        let server_patch = read_i16_le(buf)?;

        // Error message: i32 len + bytes (no type code prefix for handshake error).
        // Treat a truncated/missing length field the same as "no message" (len = -1).
        let msg_len = read_i32_le(buf).unwrap_or(-1);
        let error_message = if msg_len > 0 {
            let len = msg_len as usize;
            if buf.remaining() >= len {
                let bytes = buf.copy_to_bytes(len).to_vec();
                Some(String::from_utf8(bytes).unwrap_or_else(|_| "<invalid utf8>".into()))
            } else {
                None
            }
        } else {
            None
        };

        Err(ProtocolError::HandshakeFailed {
            server_major,
            server_minor,
            server_patch,
            message: error_message.unwrap_or_else(|| "unknown error".into()),
        })
    }
}
