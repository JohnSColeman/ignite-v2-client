/// Common request/response framing for the Ignite binary protocol.
///
/// All operation requests share the same header:
///   [i16: op_code] [i64: request_id]
///
/// All responses share (Ignite 2.17 / protocol 1.7):
///   [i64: request_id] [i16: flags]
///   if flags & 0x02: [i64: topology_major] [i32: topology_minor]  (12 bytes total)
///   if flags & 0x01: [i32: status] [typed_string: error_message]   (error path)
///   if flags & 0x01 is NOT set: operation payload follows directly (success path)
///
/// Length-prefix framing ([i32: len]) is handled by the transport layer.
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::codec::{
    decode_value, encode_value, read_bool, read_i16_le, read_i32_le, read_i64_le,
    read_u8, write_bool, write_string, write_string_nullable,
};
use super::error::{ProtocolError, Result};
use super::types::{IgniteValue, StatementType, TxConcurrency, TxIsolation};

// ─── Response / request flag constants ───────────────────────────────────────

/// Response-header flag: affinity topology version is present (8 extra bytes follow).
const FLAG_TOPOLOGY: i16 = 0x02;
/// Response-header flag: the operation failed; status code and error message follow.
const FLAG_ERROR: i16 = 0x01;
/// Cache / SQL request flag: the request is part of an active transaction.
const CACHE_FLAG_TX: u8 = 0x02;

// ─── Request header ───────────────────────────────────────────────────────────

pub fn write_request_header(buf: &mut BytesMut, op_code: i16, request_id: i64) {
    buf.put_i16_le(op_code);
    buf.put_i64_le(request_id);
}

// ─── Response header ──────────────────────────────────────────────────────────

/// Read and validate the response header from a response payload buffer.
/// Returns `Ok(request_id)` on success or `Err(ServerError)` on error.
///
/// Ignite 2.17 / protocol 1.7 response format:
///   request_id: i64
///   flags: i16
///     flags & 0x02 → affinity topology version present: i64 major + i32 minor (12 bytes)
///     flags & 0x01 → error indicator: status i32 follows (always non-zero), then error message
///   if flags & 0x01 is NOT set → success, response body follows immediately
pub fn read_response_header(buf: &mut Bytes) -> Result<i64> {
    let request_id = read_i64_le(buf)?;

    let flags = read_i16_le(buf)?;
    if flags & FLAG_TOPOLOGY != 0 {
        let _ = read_i64_le(buf)?; // affinity topology version major — discard
        let _ = read_i32_le(buf)?; // affinity topology version minor — discard
    }
    if flags & FLAG_ERROR != 0 {
        // Error response: read status code and message
        let status = read_i32_le(buf)?;
        let msg = read_typed_error_message(buf)
            .unwrap_or_else(|| format!("error code {status}"));
        return Err(ProtocolError::ServerError { status, message: msg });
    }
    Ok(request_id)
}

/// Read an error message encoded as a typed binary object.
/// Returns None when the buffer is empty or the value is null.
fn read_typed_error_message(buf: &mut Bytes) -> Option<String> {
    use super::types::type_code;
    if buf.remaining() == 0 {
        return None;
    }
    let tc = read_u8(buf).ok()?;
    match tc {
        type_code::NULL => None,
        type_code::STRING => {
            let len = read_i32_le(buf).ok()?;
            if len <= 0 {
                return Some(String::new());
            }
            let len = len as usize;
            if buf.remaining() < len {
                return None;
            }
            let bytes = buf.copy_to_bytes(len).to_vec();
            Some(String::from_utf8(bytes).unwrap_or_else(|_| "<invalid utf8>".into()))
        }
        _ => Some(format!("(type_code={tc})")),
    }
}

// ─── OP_QUERY_SQL_FIELDS (2004) ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SqlFieldsRequest {
    /// 0 for schema-qualified queries (recommended), else java_hash(cache_name)
    pub cache_id: i32,
    /// SQL schema, e.g. "PUBLIC"
    pub schema: Option<String>,
    /// Rows per page (0 = server default, typically 1024)
    pub page_size: i32,
    /// Max rows returned, -1 for unlimited
    pub max_rows: i32,
    pub sql: String,
    pub params: Vec<IgniteValue>,
    pub statement_type: StatementType,
    pub distributed_joins: bool,
    pub local: bool,
    pub replicated_only: bool,
    pub enforce_join_order: bool,
    pub collocated: bool,
    pub lazy: bool,
    /// Timeout in milliseconds, 0 = no timeout
    pub timeout_ms: i64,
    pub include_field_names: bool,
    /// Active transaction id, -1 if none
    pub tx_id: Option<i32>,
}

impl SqlFieldsRequest {
    pub fn new(sql: impl Into<String>, params: Vec<IgniteValue>) -> Self {
        Self {
            cache_id: 0,
            schema: None, // null → server uses default PUBLIC schema
            page_size: 1024,
            max_rows: -1,
            sql: sql.into(),
            params,
            statement_type: StatementType::Any,
            distributed_joins: false,
            local: false,
            replicated_only: false,
            enforce_join_order: false,
            collocated: false,
            lazy: false,
            timeout_ms: 0,
            include_field_names: true,
            tx_id: None,
        }
    }

    pub fn with_tx(mut self, tx_id: i32) -> Self {
        self.tx_id = Some(tx_id);
        self
    }

    pub fn encode(&self, op_code: i16, request_id: i64) -> Bytes {
        let mut buf = BytesMut::new();
        write_request_header(&mut buf, op_code, request_id);
        buf.put_i32_le(self.cache_id);
        // Flags byte: CACHE_FLAG_TX (0x02) = transactional.  tx_id follows
        // immediately after this byte when the transactional flag is set.
        let flags: u8 = if self.tx_id.is_some() { CACHE_FLAG_TX } else { 0x00 };
        buf.put_u8(flags);
        if let Some(tx_id) = self.tx_id {
            buf.put_i32_le(tx_id);
        }
        write_string_nullable(&mut buf, self.schema.as_deref());
        buf.put_i32_le(self.page_size);
        buf.put_i32_le(self.max_rows);
        write_string(&mut buf, &self.sql);
        debug_assert!(self.params.len() <= i32::MAX as usize, "too many SQL parameters for wire format");
        buf.put_i32_le(self.params.len() as i32);
        for param in &self.params {
            encode_value(&mut buf, param);
        }
        buf.put_i8(self.statement_type as i8);
        write_bool(&mut buf, self.distributed_joins);
        write_bool(&mut buf, self.local);
        write_bool(&mut buf, self.replicated_only);
        write_bool(&mut buf, self.enforce_join_order);
        write_bool(&mut buf, self.collocated);
        write_bool(&mut buf, self.lazy);
        buf.put_i64_le(self.timeout_ms);
        write_bool(&mut buf, self.include_field_names);
        buf.freeze()
    }
}

#[derive(Debug, Clone)]
pub struct SqlFieldsFirstPage {
    pub cursor_id: i64,
    pub field_names: Vec<String>,
    pub rows: Vec<Vec<IgniteValue>>,
    pub has_more: bool,
}

impl SqlFieldsFirstPage {
    pub fn decode(buf: &mut Bytes, include_field_names: bool) -> Result<Self> {
        let cursor_id = read_i64_le(buf)?;
        let field_count = read_i32_le(buf)? as usize;

        let field_names = if include_field_names {
            let mut names = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                // Field names are typed strings: type_code (u8) + i32 len + bytes
                let val = decode_value(buf)?;
                names.push(match val {
                    IgniteValue::String(s) => s,
                    _ => String::new(),
                });
            }
            names
        } else {
            vec!["".to_string(); field_count]
        };

        let (rows, has_more) = decode_rows(buf, field_count)?;

        Ok(Self { cursor_id, field_names, rows, has_more })
    }
}

#[derive(Debug, Clone)]
pub struct SqlFieldsPage {
    pub rows: Vec<Vec<IgniteValue>>,
    pub has_more: bool,
}

impl SqlFieldsPage {
    pub fn decode(buf: &mut Bytes, field_count: usize) -> Result<Self> {
        let (rows, has_more) = decode_rows(buf, field_count)?;
        Ok(Self { rows, has_more })
    }
}

fn decode_rows(buf: &mut Bytes, field_count: usize) -> Result<(Vec<Vec<IgniteValue>>, bool)> {
    let row_count = read_i32_le(buf)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let mut row = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            row.push(decode_value(buf)?);
        }
        rows.push(row);
    }
    let has_more = read_bool(buf)?;
    Ok((rows, has_more))
}

// ─── Cursor get page request (2005) ──────────────────────────────────────────

pub fn encode_cursor_get_page(op_code: i16, request_id: i64, cursor_id: i64) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op_code, request_id);
    buf.put_i64_le(cursor_id);
    buf.freeze()
}

// ─── Resource close (0) ───────────────────────────────────────────────────────

pub fn encode_resource_close(request_id: i64, resource_id: i64) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, 0, request_id);
    buf.put_i64_le(resource_id);
    buf.freeze()
}

// ─── OP_TX_START (4000) ───────────────────────────────────────────────────────

pub fn encode_tx_start(
    op_code: i16,
    request_id: i64,
    concurrency: TxConcurrency,
    isolation: TxIsolation,
    timeout_ms: i64,
    label: Option<&str>,
) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op_code, request_id);
    // Ignite reads concurrency and isolation as single bytes (readByte()), not i32
    buf.put_i8(concurrency as i8);
    buf.put_i8(isolation as i8);
    buf.put_i64_le(timeout_ms);
    write_string_nullable(&mut buf, label);
    buf.freeze()
}

pub fn decode_tx_start_response(buf: &mut Bytes) -> Result<i32> {
    read_i32_le(buf)
}

// ─── OP_TX_END (4001) ────────────────────────────────────────────────────────

pub fn encode_tx_end(op_code: i16, request_id: i64, tx_id: i32, committed: bool) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op_code, request_id);
    buf.put_i32_le(tx_id);
    write_bool(&mut buf, committed);
    buf.freeze()
}

// ─── Cache operations ─────────────────────────────────────────────────────────

/// Write the common cache-op header:
/// `[i16: op_code][i64: req_id][i32: cache_id][u8: flags][i32: tx_id?]`
///
/// `flags` is `0x02` when `tx_id` is `Some`, `0x00` otherwise.
/// When `tx_id` is present the `[i32: tx_id]` is written immediately after the flags byte.
fn write_cache_header(buf: &mut BytesMut, op: i16, req_id: i64, cache_id: i32, tx_id: Option<i32>) {
    write_request_header(buf, op, req_id);
    buf.put_i32_le(cache_id);
    buf.put_u8(if tx_id.is_some() { CACHE_FLAG_TX } else { 0x00 });
    if let Some(id) = tx_id {
        buf.put_i32_le(id);
    }
}

/// Encode a single-key cache request (GET, CONTAINS_KEY, GET_AND_REMOVE, …).
pub fn encode_cache_key_req(
    op: i16,
    req_id: i64,
    cache_id: i32,
    key: &IgniteValue,
    tx_id: Option<i32>,
) -> Bytes {
    let mut buf = BytesMut::new();
    write_cache_header(&mut buf, op, req_id, cache_id, tx_id);
    encode_value(&mut buf, key);
    buf.freeze()
}

/// Encode a key+value cache request (PUT, REPLACE, GET_AND_PUT, PUT_IF_ABSENT, …).
pub fn encode_cache_kv_req(
    op: i16,
    req_id: i64,
    cache_id: i32,
    key: &IgniteValue,
    val: &IgniteValue,
    tx_id: Option<i32>,
) -> Bytes {
    let mut buf = BytesMut::new();
    write_cache_header(&mut buf, op, req_id, cache_id, tx_id);
    encode_value(&mut buf, key);
    encode_value(&mut buf, val);
    buf.freeze()
}

/// Encode a multi-key cache request (GET_ALL, REMOVE_ALL).
pub fn encode_cache_multi_key_req(
    op: i16,
    req_id: i64,
    cache_id: i32,
    keys: &[IgniteValue],
    tx_id: Option<i32>,
) -> Bytes {
    let mut buf = BytesMut::new();
    write_cache_header(&mut buf, op, req_id, cache_id, tx_id);
    debug_assert!(keys.len() <= i32::MAX as usize, "too many keys for wire format");
    buf.put_i32_le(keys.len() as i32);
    for k in keys {
        encode_value(&mut buf, k);
    }
    buf.freeze()
}

/// Encode a multi-kv cache request (PUT_ALL).
pub fn encode_cache_multi_kv_req(
    op: i16,
    req_id: i64,
    cache_id: i32,
    entries: &[(IgniteValue, IgniteValue)],
    tx_id: Option<i32>,
) -> Bytes {
    let mut buf = BytesMut::new();
    write_cache_header(&mut buf, op, req_id, cache_id, tx_id);
    debug_assert!(entries.len() <= i32::MAX as usize, "too many entries for wire format");
    buf.put_i32_le(entries.len() as i32);
    for (k, v) in entries {
        encode_value(&mut buf, k);
        encode_value(&mut buf, v);
    }
    buf.freeze()
}

/// Encode a `CACHE_GET_SIZE` (1055) request.
/// Payload after the cache header: `[i32: peek_mode_count=0]` (0 = use all partitions).
pub fn encode_cache_get_size(op: i16, req_id: i64, cache_id: i32, tx_id: Option<i32>) -> Bytes {
    let mut buf = BytesMut::new();
    write_cache_header(&mut buf, op, req_id, cache_id, tx_id);
    buf.put_i32_le(0); // 0 peek modes → all partitions
    buf.freeze()
}

/// Encode a `CACHE_GET_NAMES` (1050) request.
/// Uses only the basic request header — no cache_id or flags.
pub fn encode_cache_get_names(op: i16, req_id: i64) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op, req_id);
    buf.freeze()
}

/// Encode a `CACHE_GET_OR_CREATE_WITH_NAME` (1052) request.
/// Header is only `[op_code][req_id]` — no cache_id/flags prefix.
pub fn encode_cache_create_with_name(op: i16, req_id: i64, name: &str) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op, req_id);
    write_string(&mut buf, name);
    buf.freeze()
}

/// Encode a `CACHE_GET_OR_CREATE_WITH_CONFIGURATION` (op 1053) request with minimal config.
///
/// Wire format (after the standard `[op_code][req_id]` header):
/// ```text
/// [i32: config_total_length]                                           — byte count of everything that follows
/// [i16: property_count = 2]
/// [i16: prop_id = 0 (name)][typed STRING: cache_name]                  — typed: [u8:0x09][i32:len][utf8]
/// [i16: prop_id = 2 (atomicityMode)][i32: 0=TRANSACTIONAL | 1=ATOMIC]  — raw (untyped) int
/// ```
/// String property values are **typed** (0x09 type-code prefix).  Integer property values are raw.
/// `config_total_length` = 2 + (2+1+4+name_len) + (2+4) = 15 + name_len bytes.
///
/// Note: CacheAtomicityMode ordinals: 0=TRANSACTIONAL, 1=ATOMIC.
pub fn encode_cache_create_with_config(
    op: i16,
    req_id: i64,
    name: &str,
    transactional: bool,
) -> Bytes {
    let name_bytes = name.as_bytes();
    // config_total_length = i16:prop_count + prop0(i16:id + typed_string) + prop2(i16:id + i32:val)
    // typed_string = u8:type_code + i32:len + utf8 = 1 + 4 + name_len bytes
    let config_len: i32 = (2 + 2 + 1 + 4 + name_bytes.len() + 2 + 4) as i32;

    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op, req_id);

    buf.put_i32_le(config_len);     // config_total_length
    buf.put_i16_le(2i16);           // prop_count = 2

    // Property 0 — NAME: [i16: prop_id=0][typed STRING: name]  (type-code 0x09 + i32 len + utf8)
    buf.put_i16_le(0i16);
    write_string(&mut buf, name);   // [u8: 0x09][i32: len][utf8]

    // Property 2 — ATOMICITY MODE: [i16: prop_id=2][i32: raw int]
    // CacheAtomicityMode: 0=TRANSACTIONAL, 1=ATOMIC
    buf.put_i16_le(2i16);
    buf.put_i32_le(if transactional { 0 } else { 1 });

    buf.freeze()
}

/// Encode a `CACHE_DESTROY` (1056) request.
/// Header is only `[op_code][req_id]` followed by `[i32: cache_id]`.
pub fn encode_cache_destroy_req(op: i16, req_id: i64, cache_id: i32) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op, req_id);
    buf.put_i32_le(cache_id);
    buf.freeze()
}

/// Decode a single typed-value cache response (GET, GET_AND_PUT, GET_AND_REMOVE, GET_AND_REPLACE).
pub fn decode_cache_value_response(buf: &mut Bytes) -> super::error::Result<IgniteValue> {
    decode_value(buf)
}

/// Decode a `CACHE_GET_ALL` response: `[i32: count]([typed key][typed value])...`
pub fn decode_cache_get_all_response(
    buf: &mut Bytes,
) -> super::error::Result<Vec<(IgniteValue, IgniteValue)>> {
    let count = read_i32_le(buf)? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let k = decode_value(buf)?;
        let v = decode_value(buf)?;
        entries.push((k, v));
    }
    Ok(entries)
}

/// Decode a `CACHE_GET_SIZE` response: `[i64: size]`.
pub fn decode_cache_get_size_response(buf: &mut Bytes) -> super::error::Result<i64> {
    read_i64_le(buf)
}

/// Decode a `CACHE_GET_NAMES` response: `[i32: count]([typed string])...`
pub fn decode_cache_names_response(buf: &mut Bytes) -> super::error::Result<Vec<String>> {
    let count = read_i32_le(buf)? as usize;
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        names.push(match decode_value(buf)? {
            IgniteValue::String(s) => s,
            _ => String::new(),
        });
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_fields_request_encodes() {
        let req = SqlFieldsRequest::new("SELECT 1", vec![]);
        let bytes = req.encode(2004, 1);
        // Should at minimum have: i16 opcode + i64 req_id + i32 cache_id + ...
        assert!(bytes.len() > 14);
    }

    #[test]
    fn tx_encode_decode() {
        let payload = encode_tx_start(4000, 99, TxConcurrency::Pessimistic, TxIsolation::ReadCommitted, 5000, None);
        // Verify op_code at start
        let op = i16::from_le_bytes([payload[0], payload[1]]);
        assert_eq!(op, 4000);
    }
}
