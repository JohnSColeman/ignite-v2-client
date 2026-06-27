//! Server endpoint discovery — a port of Java `ClientClusterDiscovery`.
//!
//! After connecting, the client can ask the cluster for the full set of server
//! node thin-client endpoints (`CLUSTER_GROUP_GET_NODE_ENDPOINTS`, op 5102) and
//! open channels to nodes that were not in the configured address list, so
//! partition-aware routing can reach them.

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::protocol::codec::{read_i32_le, read_i64_le, read_string_obj};
use crate::protocol::error::Result;
use crate::protocol::messages::write_request_header;
use crate::protocol::op_code;

/// Topology-version marker meaning "unknown" — request a full snapshot.
pub const UNKNOWN_TOP_VER: i64 = -1;

/// A discovered server node's thin-client endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEndpoint {
    pub node_id: Uuid,
    pub port: i32,
    /// Advertised host addresses / names, in server order.
    pub addresses: Vec<String>,
}

/// Decoded `CLUSTER_GROUP_GET_NODE_ENDPOINTS` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEndpointsResponse {
    pub topology_version: i64,
    pub added: Vec<NodeEndpoint>,
    pub removed: Vec<Uuid>,
}

/// Encode a `CLUSTER_GROUP_GET_NODE_ENDPOINTS` (op 5102) request.
///
/// Body: `[i64: start_top_ver][i64: end_top_ver = UNKNOWN_TOP_VER]`.  Passing
/// `UNKNOWN_TOP_VER` as the start version asks the server for a full snapshot.
pub fn encode_node_endpoints_request(req_id: i64, start_top_ver: i64) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op_code::CLUSTER_GROUP_GET_NODE_ENDPOINTS, req_id);
    buf.put_i64_le(start_top_ver);
    buf.put_i64_le(UNKNOWN_TOP_VER);
    buf.freeze()
}

/// Decode a `CLUSTER_GROUP_GET_NODE_ENDPOINTS` response body (after the response
/// header has been stripped).
///
/// Wire format (Java `ClientClusterGroupGetNodesEndpointsResponse.encode`):
/// ```text
/// [i64: topVer]
/// [i32: addedCount]
///   per node: [i64 msb][i64 lsb]   (raw UUID, no type code)
///             [i32: port]
///             [i32: addrCount]
///               per addr: [typed string]
/// [i32: removedCount]
///   per node: [i64 msb][i64 lsb]
/// ```
pub fn decode_node_endpoints(buf: &mut Bytes) -> Result<NodeEndpointsResponse> {
    let topology_version = read_i64_le(buf)?;

    let added_cnt = read_i32_le(buf)?;
    let mut added = Vec::with_capacity(added_cnt.max(0) as usize);
    for _ in 0..added_cnt.max(0) {
        let node_id = read_raw_uuid(buf)?;
        let port = read_i32_le(buf)?;
        let addr_cnt = read_i32_le(buf)?;
        let mut addresses = Vec::with_capacity(addr_cnt.max(0) as usize);
        for _ in 0..addr_cnt.max(0) {
            if let Some(addr) = read_string_obj(buf)? {
                addresses.push(addr);
            }
        }
        added.push(NodeEndpoint {
            node_id,
            port,
            addresses,
        });
    }

    let removed_cnt = read_i32_le(buf)?;
    let mut removed = Vec::with_capacity(removed_cnt.max(0) as usize);
    for _ in 0..removed_cnt.max(0) {
        removed.push(read_raw_uuid(buf)?);
    }

    Ok(NodeEndpointsResponse {
        topology_version,
        added,
        removed,
    })
}

/// Read a raw UUID — two little-endian `i64` halves with **no** type-code byte
/// (Java `writeUuid` = `writeLong(msb); writeLong(lsb)`).  Distinct from the
/// type-code-prefixed `read_uuid_obj`.
fn read_raw_uuid(buf: &mut Bytes) -> Result<Uuid> {
    let msb = read_i64_le(buf)? as u64;
    let lsb = read_i64_le(buf)? as u64;
    Ok(Uuid::from_u64_pair(msb, lsb))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use crate::protocol::types::type_code;

    fn put_raw_uuid(buf: &mut BytesMut, u: Uuid) {
        let (msb, lsb) = u.as_u64_pair();
        buf.put_i64_le(msb as i64);
        buf.put_i64_le(lsb as i64);
    }

    fn put_typed_string(buf: &mut BytesMut, s: &str) {
        buf.put_u8(type_code::STRING);
        buf.put_i32_le(s.len() as i32);
        buf.put_slice(s.as_bytes());
    }

    #[test]
    fn encode_request_writes_header_and_two_versions() {
        let bytes = encode_node_endpoints_request(7, UNKNOWN_TOP_VER);
        let mut b = bytes.clone();
        assert_eq!(b.get_i16_le(), op_code::CLUSTER_GROUP_GET_NODE_ENDPOINTS);
        assert_eq!(b.get_i64_le(), 7); // req id
        assert_eq!(b.get_i64_le(), UNKNOWN_TOP_VER); // start
        assert_eq!(b.get_i64_le(), UNKNOWN_TOP_VER); // end
        assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn decode_full_snapshot_two_nodes() {
        let n1 = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
        let n2 = Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);

        let mut buf = BytesMut::new();
        buf.put_i64_le(5); // topVer
        buf.put_i32_le(2); // addedCount
        // node 1
        put_raw_uuid(&mut buf, n1);
        buf.put_i32_le(10801);
        buf.put_i32_le(2); // addrCount
        put_typed_string(&mut buf, "127.0.0.1");
        put_typed_string(&mut buf, "host-1");
        // node 2
        put_raw_uuid(&mut buf, n2);
        buf.put_i32_le(10802);
        buf.put_i32_le(1);
        put_typed_string(&mut buf, "127.0.0.1");
        // removed
        buf.put_i32_le(0);

        let mut bytes = buf.freeze();
        let resp = decode_node_endpoints(&mut bytes).unwrap();
        assert_eq!(resp.topology_version, 5);
        assert_eq!(resp.added.len(), 2);
        assert_eq!(
            resp.added[0],
            NodeEndpoint {
                node_id: n1,
                port: 10801,
                addresses: vec!["127.0.0.1".into(), "host-1".into()],
            }
        );
        assert_eq!(resp.added[1].node_id, n2);
        assert_eq!(resp.added[1].port, 10802);
        assert!(resp.removed.is_empty());
        assert_eq!(bytes.remaining(), 0, "all bytes consumed");
    }

    #[test]
    fn decode_with_removed_nodes() {
        let removed = Uuid::from_u128(0x3333_3333_3333_3333_3333_3333_3333_3333);
        let mut buf = BytesMut::new();
        buf.put_i64_le(9);
        buf.put_i32_le(0); // no added
        buf.put_i32_le(1); // removedCount
        put_raw_uuid(&mut buf, removed);

        let mut bytes = buf.freeze();
        let resp = decode_node_endpoints(&mut bytes).unwrap();
        assert!(resp.added.is_empty());
        assert_eq!(resp.removed, vec![removed]);
        assert_eq!(bytes.remaining(), 0);
    }
}
