use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use super::codec::{read_byte_array_obj, read_i16_le, read_i32_le, read_u8, read_uuid_obj};
use super::error::{ProtocolError, Result};
use super::types::type_code;

/// Thin client type code (as opposed to node-to-node protocol).
const CLIENT_TYPE_THIN: u8 = 2;

/// Protocol version we request during handshake.
pub const PROTOCOL_MAJOR: i16 = 1;
pub const PROTOCOL_MINOR: i16 = 7;
pub const PROTOCOL_PATCH: i16 = 0;

/// Whether the client advertises the `DC_AWARE` protocol feature (bit 22).
///
/// When `true`, the server includes a second "data-center" partition map in
/// `CACHE_PARTITIONS` responses, which the client uses to route read-only ops
/// to a same-data-center backup owner (Java `OPS_ALLOWED_ON_BACKUPS`).  This
/// gates the extra fields in the `CACHE_PARTITIONS` request/response codec.
/// We deliberately do **not** advertise `ALL_AFFINITY_MAPPINGS`.
pub const ADVERTISE_DC_AWARE: bool = true;

/// Feature bitmask bytes advertised at handshake.  Java encodes the feature set
/// as a `BitSet` (little-endian): bit 22 (`DC_AWARE`) lives in byte 2, position
/// 6 → `0x40`.
fn advertised_feature_bytes() -> Vec<u8> {
    if ADVERTISE_DC_AWARE {
        vec![0x00, 0x00, 0x40]
    } else {
        Vec::new()
    }
}

/// Whether the server's advertised feature bitmask includes `DC_AWARE` (bit 22).
/// The negotiated feature is the intersection of what the client advertises and
/// what the server supports, so the DC-aware `CACHE_PARTITIONS` wire format must
/// only be used when **both** sides agree — otherwise the server desyncs.
pub fn server_supports_dc_aware(server_features: &[u8]) -> bool {
    // bit 22 → byte index 2, bit position 6 (little-endian BitSet).
    server_features.get(2).is_some_and(|b| b & 0x40 != 0)
}

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
        // inserted between client_code and the auth credentials.  We advertise
        // the DC_AWARE feature (see `advertised_feature_bytes`).
        let features = advertised_feature_bytes();
        buf.put_u8(type_code::BYTE_ARRAY); // BYTE_ARRAY type code
        buf.put_i32_le(features.len() as i32);
        buf.put_slice(&features);

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
    /// Server feature bitmask (protocol ≥ 1.7).  Empty when absent.
    pub features: Vec<u8>,
    /// Server node UUID (protocol ≥ 1.4, partition awareness).  `None` when the
    /// server did not advertise it.
    pub node_id: Option<Uuid>,
}

impl HandshakeResponse {
    /// Parse the server's handshake response payload (after length prefix is stripped).
    ///
    /// Success-path layout for protocol 1.7 (see Java
    /// `ClientRequestHandler.writeHandshake`):
    /// ```text
    /// [u8: 1]                                  success
    /// [u8: 12][i32 LE: len][len bytes]         feature bitmask (BITMAP_FEATURES ≥ 1.7)
    /// [u8: 10][i64 LE: msb][i64 LE: lsb]       server node UUID (PARTITION_AWARENESS ≥ 1.4)
    /// ```
    /// The two trailing fields are read only when bytes remain, so the parse is
    /// tolerant of older servers that omit them.
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        let success_byte = read_u8(buf)?;
        if success_byte == 1 {
            // Success: the protocol defines exactly 1 = success, 0 = failure.
            // Protocol ≥ 1.7 appends the feature bitmask, and ≥ 1.4 the server
            // node UUID.  Read them only when present so older servers (which
            // omit them) still decode cleanly.
            let features = if buf.remaining() > 0 {
                read_byte_array_obj(buf).unwrap_or_default()
            } else {
                Vec::new()
            };
            let node_id = if buf.remaining() > 0 {
                read_uuid_obj(buf).ok().flatten()
            } else {
                None
            };
            return Ok(Self {
                success: true,
                server_major: 0,
                server_minor: 0,
                server_patch: 0,
                error_message: None,
                features,
                node_id,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_success_parses_features_and_node_id() {
        let node = Uuid::from_u128(0x1234_5678_9ABC_DEF0_0FED_CBA9_8765_4321);
        let (msb, lsb) = node.as_u64_pair();

        let mut buf = BytesMut::new();
        buf.put_u8(1); // success
        // feature bitmask: typed BYTE_ARRAY, 2 bytes
        buf.put_u8(type_code::BYTE_ARRAY);
        buf.put_i32_le(2);
        buf.put_slice(&[0x01, 0x20]);
        // server node UUID: typed UUID, msb/lsb little-endian
        buf.put_u8(type_code::UUID);
        buf.put_i64_le(msb as i64);
        buf.put_i64_le(lsb as i64);

        let mut bytes = buf.freeze();
        let resp = HandshakeResponse::decode(&mut bytes).unwrap();

        assert!(resp.success);
        assert_eq!(resp.features, vec![0x01, 0x20]);
        assert_eq!(resp.node_id, Some(node));
    }

    #[test]
    fn server_dc_aware_negotiation() {
        assert!(server_supports_dc_aware(&[0x00, 0x00, 0x40]), "bit 22 set");
        assert!(!server_supports_dc_aware(&[0x00, 0x00, 0x00]), "bit 22 clear");
        assert!(!server_supports_dc_aware(&[0xFF, 0xFF]), "byte 2 absent");
        assert!(!server_supports_dc_aware(&[]), "empty features");
    }

    #[test]
    fn encode_advertises_dc_aware_feature_bit() {
        let req = HandshakeRequest::new(None, None);
        let b = req.encode();
        // Layout: [u8 op][i16 major][i16 minor][i16 patch][u8 client][u8 BYTE_ARRAY][i32 len][bytes]
        assert_eq!(b[8], type_code::BYTE_ARRAY);
        let len = i32::from_le_bytes([b[9], b[10], b[11], b[12]]) as usize;
        assert!(len >= 3, "feature bitmask should cover bit 22");
        let feats = &b[13..13 + len];
        // bit 22 → byte 2, bit position 6 → 0x40
        assert_eq!(feats[2] & 0x40, 0x40, "DC_AWARE (bit 22) must be advertised");
    }

    #[test]
    fn decode_success_minimal_without_trailing_fields() {
        // Older server: just the success byte, no features / node id.
        let mut buf = BytesMut::new();
        buf.put_u8(1);
        let mut bytes = buf.freeze();
        let resp = HandshakeResponse::decode(&mut bytes).unwrap();
        assert!(resp.success);
        assert!(resp.features.is_empty());
        assert_eq!(resp.node_id, None);
    }
}
