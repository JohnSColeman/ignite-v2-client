use bigdecimal::BigDecimal;
use uuid::Uuid;

// ─── Type codes ──────────────────────────────────────────────────────────────

pub mod type_code {
    #![allow(dead_code)]
    pub const BYTE: u8       = 1;
    pub const SHORT: u8      = 2;
    pub const INT: u8        = 3;
    pub const LONG: u8       = 4;
    pub const FLOAT: u8      = 5;
    pub const DOUBLE: u8     = 6;
    pub const CHAR: u8       = 7;
    pub const BOOL: u8       = 8;
    pub const STRING: u8     = 9;
    pub const UUID: u8       = 10;
    pub const DATE: u8       = 11;
    pub const BYTE_ARRAY: u8 = 12;
    pub const SHORT_ARRAY: u8 = 13;
    pub const INT_ARRAY: u8  = 14;
    pub const LONG_ARRAY: u8 = 15;
    pub const FLOAT_ARRAY: u8 = 16;
    pub const DOUBLE_ARRAY: u8 = 17;
    pub const CHAR_ARRAY: u8 = 18;
    pub const BOOL_ARRAY: u8 = 19;
    pub const STRING_ARRAY: u8 = 20;
    pub const UUID_ARRAY: u8 = 21;
    pub const DATE_ARRAY: u8 = 22;
    pub const DECIMAL: u8    = 30;
    pub const DECIMAL_ARRAY: u8 = 31;
    pub const TIMESTAMP: u8  = 33;
    pub const TIMESTAMP_ARRAY: u8 = 34;
    pub const TIME: u8       = 36;
    pub const TIME_ARRAY: u8 = 37;
    pub const ENUM: u8       = 28;
    pub const ENUM_ARRAY: u8 = 29;
    pub const BINARY_OBJECT: u8 = 27;
    pub const COMPLEX_OBJECT: u8 = 103;
    pub const NULL: u8       = 101;
    pub const HANDLE: u8     = 104;
    pub const OBJECT_ARRAY: u8 = 23;
    pub const MAP: u8        = 25;
}

// ─── Op codes ─────────────────────────────────────────────────────────────────

pub mod op_code {
    #![allow(dead_code)]
    pub const RESOURCE_CLOSE: i16             = 0;
    pub const CACHE_GET: i16                  = 1000;
    pub const CACHE_PUT: i16                  = 1001;
    pub const CACHE_PUT_IF_ABSENT: i16        = 1002;
    pub const CACHE_GET_ALL: i16              = 1003;
    pub const CACHE_PUT_ALL: i16              = 1004;
    pub const CACHE_GET_AND_PUT: i16          = 1005;
    pub const CACHE_GET_AND_REMOVE: i16       = 1007;
    pub const CACHE_GET_AND_REPLACE: i16      = 1006;
    pub const CACHE_REPLACE: i16              = 1009;
    pub const CACHE_CONTAINS_KEY: i16         = 1011;
    pub const CACHE_REMOVE_KEY: i16           = 1019;
    pub const CACHE_REMOVE_KEYS: i16          = 1021;
    pub const CACHE_REMOVE_ALL: i16           = 1022;
    pub const CACHE_GET_NAMES: i16            = 1050;
    pub const CACHE_GET_OR_CREATE_WITH_NAME: i16 = 1052;
    pub const CACHE_CREATE_WITH_CONFIGURATION: i16        = 1051;
    pub const CACHE_GET_OR_CREATE_WITH_CONFIGURATION: i16 = 1053;
    pub const CACHE_DESTROY: i16              = 1056;
    pub const CACHE_GET_SIZE: i16             = 1020;
    pub const CACHE_PARTITIONS: i16           = 1100;
    pub const QUERY_SQL: i16                  = 2002;   // deprecated
    pub const QUERY_SQL_FIELDS: i16           = 2004;
    pub const QUERY_SQL_FIELDS_CURSOR_GET_PAGE: i16 = 2005;
    pub const TX_START: i16                   = 4000;
    pub const TX_END: i16                     = 4001;
}

// ─── Transaction enums ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TxConcurrency {
    Optimistic  = 0,
    Pessimistic = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TxIsolation {
    ReadCommitted   = 0,
    RepeatableRead  = 1,
    Serializable    = 2,
}

// ─── Statement types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum StatementType {
    Any    = 0,
    Select = 1,
    Update = 2,
}

// ─── Value enum ───────────────────────────────────────────────────────────────

/// Rust representation of a typed Ignite binary value.
#[derive(Debug, Clone, PartialEq)]
pub enum IgniteValue {
    /// SQL NULL — absence of a value for any type.
    Null,
    /// SQL BOOLEAN.
    Bool(bool),
    /// Signed 8-bit integer (TINYINT).
    Byte(i8),
    /// Signed 16-bit integer (SMALLINT).
    Short(i16),
    /// Signed 32-bit integer (INT).
    Int(i32),
    /// Signed 64-bit integer (BIGINT).
    Long(i64),
    /// Single-precision IEEE 754 float (REAL / FLOAT).
    Float(f32),
    /// Double-precision IEEE 754 float (DOUBLE).
    Double(f64),
    /// Unicode code point in the Basic Multilingual Plane (CHAR).
    Char(u16),
    /// UTF-8 string (VARCHAR / LONGVARCHAR).
    String(String),
    /// 128-bit universally unique identifier (UUID / CHAR(36)).
    Uuid(Uuid),
    /// Milliseconds from the Unix epoch (DATE).
    Date(i64),
    /// `(milliseconds_from_epoch, nanosecond_fraction)` (TIMESTAMP).
    Timestamp(i64, i32),
    /// Nanoseconds from midnight (TIME).
    Time(i64),
    /// Arbitrary-precision decimal number (DECIMAL / NUMERIC).
    Decimal(BigDecimal),
    /// Raw byte array (BINARY / VARBINARY).
    ByteArray(Vec<u8>),
    /// Payload bytes of an Ignite `BINARY_OBJECT` (type code 27), **without** the
    /// outer type-code wrapper.  When encoded, the codec writes the full
    /// `[u8: 27][i32: len][bytes][i32: offset=0]` frame automatically.
    /// When decoded, only the inner payload bytes (between the length prefix and
    /// the trailing offset) are stored here.
    RawObject(Vec<u8>),
}

// ─── Java hash ────────────────────────────────────────────────────────────────

/// Replicates Java's `String.hashCode()`.  Used to compute type IDs and field IDs
/// in the Binary Object format.
///
/// ```
/// use ignite_client::java_hash;
/// assert_eq!(java_hash("abc"), 96354);
/// assert_eq!(java_hash(""), 0);
/// ```
pub fn java_hash(s: &str) -> i32 {
    s.chars().fold(0i32, |h, c| {
        h.wrapping_mul(31).wrapping_add(c as i32)
    })
}

/// Cache ID is the Java hash of the upper-cased cache name.
pub fn cache_id(name: &str) -> i32 {
    java_hash(&name.to_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_hash_empty() {
        assert_eq!(java_hash(""), 0);
    }

    #[test]
    fn java_hash_known() {
        // Verified against Ignite Java source / manual calculation
        assert_eq!(java_hash("abc"), 96354);
        assert_eq!(java_hash("Hello"), 69609650);
        // "PUBLIC" wraps past i32::MAX → negative
        assert_eq!(java_hash("PUBLIC"), -1924094359_i32);
    }

    #[test]
    fn cache_id_case_insensitive() {
        assert_eq!(cache_id("myCache"), cache_id("MYCACHE"));
    }
}
