use bytes::{Buf, BufMut, Bytes, BytesMut};
use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};
use std::str::FromStr;
use uuid::Uuid;

use super::error::{ProtocolError, Result};
use super::types::{type_code, IgniteValue};

// ─── Low-level primitives ─────────────────────────────────────────────────────

fn require(buf: &Bytes, n: usize) -> Result<()> {
    if buf.remaining() < n {
        Err(ProtocolError::BufferUnderflow { expected: n, got: buf.remaining() })
    } else {
        Ok(())
    }
}

pub fn read_i8(buf: &mut Bytes) -> Result<i8> {
    require(buf, 1)?;
    Ok(buf.get_i8())
}

pub fn read_u8(buf: &mut Bytes) -> Result<u8> {
    require(buf, 1)?;
    Ok(buf.get_u8())
}

pub fn read_i16_le(buf: &mut Bytes) -> Result<i16> {
    require(buf, 2)?;
    Ok(buf.get_i16_le())
}

pub fn read_i32_le(buf: &mut Bytes) -> Result<i32> {
    require(buf, 4)?;
    Ok(buf.get_i32_le())
}

pub fn read_i64_le(buf: &mut Bytes) -> Result<i64> {
    require(buf, 8)?;
    Ok(buf.get_i64_le())
}

pub fn read_f32_le(buf: &mut Bytes) -> Result<f32> {
    require(buf, 4)?;
    Ok(buf.get_f32_le())
}

pub fn read_f64_le(buf: &mut Bytes) -> Result<f64> {
    require(buf, 8)?;
    Ok(buf.get_f64_le())
}

/// Reads a nullable raw String from a response payload: i32 length (-1 = null)
/// then UTF-8 bytes.  Response strings (field names, error messages) are NOT
/// prefixed with a type code — they use this compact length-prefixed format.
pub fn read_string_nullable(buf: &mut Bytes) -> Result<Option<String>> {
    let len = read_i32_le(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    require(buf, len)?;
    let bytes = buf.copy_to_bytes(len).to_vec();
    Ok(Some(String::from_utf8(bytes)?))
}

/// Reads a non-null String (panics if null marker returned from server — callers must
/// guard with read_string_nullable when the field is optional).
pub fn read_string(buf: &mut Bytes) -> Result<String> {
    read_string_nullable(buf)?.ok_or(ProtocolError::UnexpectedNull)
}

/// Reads a bool (1 byte, 0=false, else true).
pub fn read_bool(buf: &mut Bytes) -> Result<bool> {
    require(buf, 1)?;
    Ok(buf.get_u8() != 0)
}

// ─── Write helpers ────────────────────────────────────────────────────────────

/// Writes a nullable typed String: type code 101 (NULL) or type code 9 (STRING) +
/// i32 length + UTF-8 bytes.  Matches BinaryReaderExImpl.readString() on the server.
pub fn write_string_nullable(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        None => buf.put_u8(type_code::NULL),
        Some(s) => {
            let bytes = s.as_bytes();
            debug_assert!(bytes.len() <= i32::MAX as usize, "string too large for wire format");
            buf.put_u8(type_code::STRING);
            buf.put_i32_le(bytes.len() as i32);
            buf.put_slice(bytes);
        }
    }
}

pub fn write_string(buf: &mut BytesMut, s: &str) {
    write_string_nullable(buf, Some(s));
}

pub fn write_bool(buf: &mut BytesMut, v: bool) {
    buf.put_u8(v as u8);
}

// ─── Typed value decode ───────────────────────────────────────────────────────

/// Decode a typed binary object from buffer (reads type-code byte first).
pub fn decode_value(buf: &mut Bytes) -> Result<IgniteValue> {
    let tc = read_u8(buf)?;
    decode_value_with_code(buf, tc)
}

fn decode_value_with_code(buf: &mut Bytes, tc: u8) -> Result<IgniteValue> {
    use type_code::*;
    match tc {
        NULL => Ok(IgniteValue::Null),
        BOOL => {
            let v = read_u8(buf)? != 0;
            Ok(IgniteValue::Bool(v))
        }
        BYTE => Ok(IgniteValue::Byte(read_i8(buf)?)),
        SHORT => Ok(IgniteValue::Short(read_i16_le(buf)?)),
        INT => Ok(IgniteValue::Int(read_i32_le(buf)?)),
        LONG => Ok(IgniteValue::Long(read_i64_le(buf)?)),
        FLOAT => Ok(IgniteValue::Float(read_f32_le(buf)?)),
        DOUBLE => Ok(IgniteValue::Double(read_f64_le(buf)?)),
        CHAR => {
            require(buf, 2)?;
            Ok(IgniteValue::Char(buf.get_u16_le()))
        }
        STRING => {
            // String type code is followed directly by i32 len + bytes (no extra type byte)
            let s = read_string(buf)?;
            Ok(IgniteValue::String(s))
        }
        UUID => {
            // 16 bytes big-endian (MSB first, standard UUID byte order)
            require(buf, 16)?;
            let mut uuid_bytes = [0u8; 16];
            buf.copy_to_slice(&mut uuid_bytes);
            Ok(IgniteValue::Uuid(Uuid::from_bytes(uuid_bytes)))
        }
        DATE => {
            let ms = read_i64_le(buf)?;
            Ok(IgniteValue::Date(ms))
        }
        TIMESTAMP => {
            let ms = read_i64_le(buf)?;
            let ns_fraction = read_i32_le(buf)?;
            Ok(IgniteValue::Timestamp(ms, ns_fraction))
        }
        TIME => {
            let ns = read_i64_le(buf)?;
            Ok(IgniteValue::Time(ns))
        }
        DECIMAL => {
            let scale = read_i32_le(buf)?;
            let byte_count = read_i32_le(buf)?;
            if byte_count < 0 {
                return Err(ProtocolError::DecimalError(
                    format!("negative byte count: {byte_count}")
                ));
            }
            require(buf, byte_count as usize)?;
            let magnitude = buf.copy_to_bytes(byte_count as usize).to_vec();
            // Two's complement big-endian → BigInt
            let bigint = twos_complement_be_to_bigint(&magnitude);
            let s = format!("{}e-{}", bigint, scale);
            let decimal = BigDecimal::from_str(&s).map_err(|e| {
                ProtocolError::DecimalError(e.to_string())
            })?;
            Ok(IgniteValue::Decimal(decimal))
        }
        BYTE_ARRAY => {
            let len = read_i32_le(buf)?;
            if len < 0 { return Ok(IgniteValue::Null); }
            require(buf, len as usize)?;
            let data = buf.copy_to_bytes(len as usize).to_vec();
            Ok(IgniteValue::ByteArray(data))
        }
        // BINARY_OBJECT (type code 27) wire format:
        //   [i32 LE: payload_len] [payload_len bytes: payload] [i32 LE: offset]
        // `payload` is the serialised binary object; `offset` is the offset to
        // the object's start within the payload (always 0 when written by this crate).
        // RawObject stores the payload bytes without the outer type-code wrapper.
        BINARY_OBJECT => {
            let payload_len = read_i32_le(buf)?;
            if payload_len < 0 {
                return Err(ProtocolError::UnknownTypeCode(BINARY_OBJECT));
            }
            require(buf, payload_len as usize + 4)?;   // payload + trailing i32 offset
            let data = buf.copy_to_bytes(payload_len as usize).to_vec();
            let _offset = read_i32_le(buf)?;            // consumed but not stored
            Ok(IgniteValue::RawObject(data))
        }
        // COMPLEX_OBJECT, OBJECT_ARRAY, MAP, ENUM have variable-length formats that
        // require schema/type metadata to parse correctly.  Return an error rather
        // than silently consuming all remaining buffer bytes (which would corrupt
        // subsequent field decoding in multi-column rows).
        COMPLEX_OBJECT | OBJECT_ARRAY | MAP | ENUM => {
            Err(ProtocolError::UnknownTypeCode(tc))
        }
        other => Err(ProtocolError::UnknownTypeCode(other)),
    }
}

// ─── Two's complement big-endian bytes → BigInt ──────────────────────────────

fn twos_complement_be_to_bigint(bytes: &[u8]) -> BigInt {
    if bytes.is_empty() {
        return BigInt::from(0);
    }
    let sign_bit = bytes[0] & 0x80 != 0;
    if sign_bit {
        // Negative: invert + add 1 (two's complement)
        let mut inverted: Vec<u8> = bytes.iter().map(|b| !b).collect();
        // Add 1 with carry
        for b in inverted.iter_mut().rev() {
            let (v, carry) = b.overflowing_add(1);
            *b = v;
            if !carry { break; }
        }
        BigInt::from_bytes_be(Sign::Minus, &inverted)
    } else {
        BigInt::from_bytes_be(Sign::Plus, bytes)
    }
}

/// BigInt → two's complement big-endian bytes
fn bigint_to_twos_complement_be(n: &BigInt) -> Vec<u8> {
    match n.sign() {
        Sign::NoSign => vec![0x00],
        Sign::Plus => {
            let (_, mut bytes) = n.to_bytes_be();
            // Ensure high bit is 0 (not mistaken for negative)
            if bytes[0] & 0x80 != 0 {
                bytes.insert(0, 0x00);
            }
            bytes
        }
        Sign::Minus => {
            // Absolute value, subtract 1, invert bytes.
            // magnitude() returns &BigUint directly — no negation or unwrap needed.
            // In this arm magnitude ≥ 1, so subtracting 1u32 never underflows.
            let abs_minus1 = n.magnitude() - 1u32;
            let bytes = abs_minus1.to_bytes_be();
            let mut inverted: Vec<u8> = bytes.iter().map(|b| !b).collect();
            // Ensure high bit is 1 (negative)
            if inverted[0] & 0x80 == 0 {
                inverted.insert(0, 0xFF);
            }
            inverted
        }
    }
}

// ─── Typed value encode ───────────────────────────────────────────────────────

/// Encode an IgniteValue into buf (writes type-code byte + payload).
pub fn encode_value(buf: &mut BytesMut, val: &IgniteValue) {
    use type_code::*;
    match val {
        IgniteValue::Null => {
            buf.put_u8(NULL);
        }
        IgniteValue::Bool(v) => {
            buf.put_u8(BOOL);
            buf.put_u8(if *v { 1 } else { 0 });
        }
        IgniteValue::Byte(v) => {
            buf.put_u8(BYTE);
            buf.put_i8(*v);
        }
        IgniteValue::Short(v) => {
            buf.put_u8(SHORT);
            buf.put_i16_le(*v);
        }
        IgniteValue::Int(v) => {
            buf.put_u8(INT);
            buf.put_i32_le(*v);
        }
        IgniteValue::Long(v) => {
            buf.put_u8(LONG);
            buf.put_i64_le(*v);
        }
        IgniteValue::Float(v) => {
            buf.put_u8(FLOAT);
            buf.put_f32_le(*v);
        }
        IgniteValue::Double(v) => {
            buf.put_u8(DOUBLE);
            buf.put_f64_le(*v);
        }
        IgniteValue::Char(v) => {
            buf.put_u8(CHAR);
            buf.put_u16_le(*v);
        }
        IgniteValue::String(s) => {
            buf.put_u8(STRING);
            let bytes = s.as_bytes();
            debug_assert!(bytes.len() <= i32::MAX as usize, "String value too large for wire format");
            buf.put_i32_le(bytes.len() as i32);
            buf.put_slice(bytes);
        }
        IgniteValue::Uuid(u) => {
            buf.put_u8(UUID);
            buf.put_slice(u.as_bytes()); // big-endian 16 bytes
        }
        IgniteValue::Date(ms) => {
            buf.put_u8(DATE);
            buf.put_i64_le(*ms);
        }
        IgniteValue::Timestamp(ms, ns) => {
            buf.put_u8(TIMESTAMP);
            buf.put_i64_le(*ms);
            buf.put_i32_le(*ns);
        }
        IgniteValue::Time(ns) => {
            buf.put_u8(TIME);
            buf.put_i64_le(*ns);
        }
        IgniteValue::Decimal(d) => {
            buf.put_u8(DECIMAL);
            let (bigint, scale) = d.as_bigint_and_exponent();
            let magnitude = bigint_to_twos_complement_be(&bigint);
            buf.put_i32_le(scale as i32);
            buf.put_i32_le(magnitude.len() as i32);
            buf.put_slice(&magnitude);
        }
        IgniteValue::ByteArray(data) => {
            buf.put_u8(BYTE_ARRAY);
            debug_assert!(data.len() <= i32::MAX as usize, "ByteArray value too large for wire format");
            buf.put_i32_le(data.len() as i32);
            buf.put_slice(data);
        }
        IgniteValue::RawObject(data) => {
            // Encode as BINARY_OBJECT (type code 27):
            //   [u8: type_code=27] [i32: payload_len] [payload bytes] [i32: offset=0]
            buf.put_u8(BINARY_OBJECT);
            debug_assert!(data.len() <= i32::MAX as usize, "RawObject value too large for wire format");
            buf.put_i32_le(data.len() as i32);
            buf.put_slice(data);
            buf.put_i32_le(0);   // offset = 0: object starts at beginning of payload
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(val: IgniteValue) -> IgniteValue {
        let mut buf = BytesMut::new();
        encode_value(&mut buf, &val);
        let mut bytes = buf.freeze();
        decode_value(&mut bytes).unwrap()
    }

    #[test]
    fn roundtrip_null() {
        assert_eq!(roundtrip(IgniteValue::Null), IgniteValue::Null);
    }

    #[test]
    fn roundtrip_bool() {
        assert_eq!(roundtrip(IgniteValue::Bool(true)), IgniteValue::Bool(true));
        assert_eq!(roundtrip(IgniteValue::Bool(false)), IgniteValue::Bool(false));
    }

    #[test]
    fn roundtrip_int() {
        assert_eq!(roundtrip(IgniteValue::Int(-42)), IgniteValue::Int(-42));
        assert_eq!(roundtrip(IgniteValue::Int(i32::MAX)), IgniteValue::Int(i32::MAX));
    }

    #[test]
    fn roundtrip_long() {
        assert_eq!(roundtrip(IgniteValue::Long(i64::MIN)), IgniteValue::Long(i64::MIN));
    }

    #[test]
    fn roundtrip_string() {
        let s = "Hello, Ignite!".to_string();
        assert_eq!(roundtrip(IgniteValue::String(s.clone())), IgniteValue::String(s));
    }

    #[test]
    fn roundtrip_uuid() {
        let u = Uuid::new_v4();
        assert_eq!(roundtrip(IgniteValue::Uuid(u)), IgniteValue::Uuid(u));
    }

    #[test]
    fn roundtrip_timestamp() {
        let ts = IgniteValue::Timestamp(1_700_000_000_000, 123_456_789);
        assert_eq!(roundtrip(ts.clone()), ts);
    }

    #[test]
    fn roundtrip_byte() {
        assert_eq!(roundtrip(IgniteValue::Byte(i8::MIN)), IgniteValue::Byte(i8::MIN));
        assert_eq!(roundtrip(IgniteValue::Byte(0)), IgniteValue::Byte(0));
        assert_eq!(roundtrip(IgniteValue::Byte(i8::MAX)), IgniteValue::Byte(i8::MAX));
    }

    #[test]
    fn roundtrip_short() {
        assert_eq!(roundtrip(IgniteValue::Short(i16::MIN)), IgniteValue::Short(i16::MIN));
        assert_eq!(roundtrip(IgniteValue::Short(i16::MAX)), IgniteValue::Short(i16::MAX));
    }

    #[test]
    fn roundtrip_float() {
        assert_eq!(roundtrip(IgniteValue::Float(3.14_f32)), IgniteValue::Float(3.14_f32));
        assert_eq!(roundtrip(IgniteValue::Float(f32::NEG_INFINITY)), IgniteValue::Float(f32::NEG_INFINITY));
    }

    #[test]
    fn roundtrip_double() {
        assert_eq!(roundtrip(IgniteValue::Double(std::f64::consts::PI)), IgniteValue::Double(std::f64::consts::PI));
        assert_eq!(roundtrip(IgniteValue::Double(f64::MAX)), IgniteValue::Double(f64::MAX));
    }

    #[test]
    fn roundtrip_char() {
        assert_eq!(roundtrip(IgniteValue::Char(b'A' as u16)), IgniteValue::Char(b'A' as u16));
        assert_eq!(roundtrip(IgniteValue::Char(0x0410)), IgniteValue::Char(0x0410)); // Cyrillic А
    }

    #[test]
    fn roundtrip_date() {
        // 2024-01-15 as millis since epoch
        assert_eq!(roundtrip(IgniteValue::Date(1_705_276_800_000)), IgniteValue::Date(1_705_276_800_000));
        assert_eq!(roundtrip(IgniteValue::Date(0)), IgniteValue::Date(0));
        assert_eq!(roundtrip(IgniteValue::Date(-86_400_000)), IgniteValue::Date(-86_400_000));
    }

    #[test]
    fn roundtrip_time() {
        // 12:34:56.789 as nanoseconds from midnight
        let ns = (12 * 3600 + 34 * 60 + 56) * 1_000_000_000_i64 + 789_000_000;
        assert_eq!(roundtrip(IgniteValue::Time(ns)), IgniteValue::Time(ns));
        assert_eq!(roundtrip(IgniteValue::Time(0)), IgniteValue::Time(0));
    }

    #[test]
    fn roundtrip_byte_array() {
        let data = vec![0u8, 1, 127, 128, 255];
        assert_eq!(roundtrip(IgniteValue::ByteArray(data.clone())), IgniteValue::ByteArray(data));
        assert_eq!(roundtrip(IgniteValue::ByteArray(vec![])), IgniteValue::ByteArray(vec![]));
    }

    #[test]
    fn roundtrip_raw_object() {
        // Non-empty payload
        let data = vec![1u8, 2, 3, 42, 99];
        assert_eq!(roundtrip(IgniteValue::RawObject(data.clone())), IgniteValue::RawObject(data));
        // Empty payload
        assert_eq!(roundtrip(IgniteValue::RawObject(vec![])), IgniteValue::RawObject(vec![]));
    }

    #[test]
    fn roundtrip_decimal() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let cases = ["1.23", "0.001", "-99.9", "1000000", "0"];
        for s in &cases {
            let d = BigDecimal::from_str(s).unwrap();
            let result = roundtrip(IgniteValue::Decimal(d.clone()));
            if let IgniteValue::Decimal(back) = result {
                // Normalise before comparing (scale may differ for e.g. "0")
                assert_eq!(d.normalized(), back.normalized(), "mismatch for {s}");
            } else {
                panic!("expected Decimal, got {result:?}");
            }
        }
    }

    #[test]
    fn twos_complement_positive() {
        let n = BigInt::from(123456789_i64);
        let bytes = bigint_to_twos_complement_be(&n);
        let back = twos_complement_be_to_bigint(&bytes);
        assert_eq!(n, back);
    }

    #[test]
    fn twos_complement_negative() {
        let n = BigInt::from(-987654321_i64);
        let bytes = bigint_to_twos_complement_be(&n);
        let back = twos_complement_be_to_bigint(&bytes);
        assert_eq!(n, back);
    }

    #[test]
    fn twos_complement_zero() {
        let n = BigInt::from(0i32);
        let bytes = bigint_to_twos_complement_be(&n);
        let back = twos_complement_be_to_bigint(&bytes);
        assert_eq!(n, back);
    }

    #[test]
    fn twos_complement_boundary() {
        // Ensure values at byte boundaries don't gain/lose a sign-extension byte
        for v in [127i64, 128, -128, -129, 255, 256, -256, i32::MAX as i64, i32::MIN as i64] {
            let n = BigInt::from(v);
            let bytes = bigint_to_twos_complement_be(&n);
            let back = twos_complement_be_to_bigint(&bytes);
            assert_eq!(n, back, "boundary mismatch for {v}");
        }
    }
}
