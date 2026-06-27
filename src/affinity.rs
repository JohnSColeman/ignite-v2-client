//! Partition awareness / affinity routing.
//!
//! Computes the cluster node that owns a given cache key locally, so requests
//! can be sent straight to the primary node instead of being proxied by the
//! server.  Mirrors the Java thin client
//! (`org.apache.ignite.internal.client.thin`):
//!
//! * [`affinity_hash`] replicates the JVM `hashCode()` of a key value.
//! * [`partition`] / [`calculate_mask`] replicate
//!   `RendezvousAffinityFunction.calculatePartition`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::protocol::IgniteValue;
use crate::protocol::codec::{read_bool, read_i32_le, read_i64_le, read_uuid_obj};
use crate::protocol::error::Result;
use crate::protocol::messages::write_request_header;
use crate::protocol::op_code;

// ─── Affinity topology version ────────────────────────────────────────────────

/// The cluster topology version an affinity mapping applies to.
///
/// Ordered major-first then minor, matching Java
/// `AffinityTopologyVersion.compareTo` (the derived `Ord` compares fields in
/// declaration order).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct AffinityTopologyVersion {
    pub major: i64,
    pub minor: i32,
}

impl AffinityTopologyVersion {
    pub fn new(major: i64, minor: i32) -> Self {
        Self { major, minor }
    }
}

// ─── Cache affinity mapping ───────────────────────────────────────────────────

/// Per-cache `partition → primary node` mapping decoded from a
/// `CACHE_PARTITIONS` response.
#[derive(Debug, Clone)]
pub struct CacheAffinityMapping {
    pub cache_id: i32,
    /// `partition number → primary node UUID`, indexed by partition.  `None`
    /// marks a partition with no known primary (fall back to default channel).
    pub partition_nodes: Vec<Option<Uuid>>,
    /// Number of partitions (`== partition_nodes.len()`).
    pub parts: i32,
    /// Precomputed rendezvous mask for `parts` (see [`calculate_mask`]).
    pub mask: i32,
    /// Custom affinity key config: `type_id → field_id` (usually empty).
    pub key_cfg: HashMap<i32, i32>,
    /// `false` when the server marked these caches as not partition-aware.
    pub applicable: bool,
}

/// A decoded `CACHE_PARTITIONS` response.
#[derive(Debug, Clone)]
pub struct CachePartitionsResponse {
    pub version: AffinityTopologyVersion,
    pub mappings: Vec<CacheAffinityMapping>,
}

// ─── CACHE_PARTITIONS request / response codec ────────────────────────────────

/// Encode a `CACHE_PARTITIONS` (op 1101) request for the given cache ids.
///
/// With an empty feature bitmask advertised the body is simply
/// `[i32: count][i32: cacheId]…` — the `ALL_AFFINITY_MAPPINGS` boolean and the
/// `DC_AWARE` data-center string are omitted.
pub fn encode_cache_partitions_request(req_id: i64, cache_ids: &[i32]) -> Bytes {
    let mut buf = BytesMut::new();
    write_request_header(&mut buf, op_code::CACHE_PARTITIONS, req_id);
    debug_assert!(
        cache_ids.len() <= i32::MAX as usize,
        "too many cache ids for wire format"
    );
    buf.put_i32_le(cache_ids.len() as i32);
    for &id in cache_ids {
        buf.put_i32_le(id);
    }
    buf.freeze()
}

/// Decode a `CACHE_PARTITIONS` response body (after the response header has been
/// stripped) in the simplest wire format (empty feature bitmask: no
/// `DC_AWARE`, no `ALL_AFFINITY_MAPPINGS` fields).
pub fn decode_cache_partitions(buf: &mut Bytes) -> Result<CachePartitionsResponse> {
    let major = read_i64_le(buf)?;
    let minor = read_i32_le(buf)?;
    let version = AffinityTopologyVersion::new(major, minor);

    let mappings_cnt = read_i32_le(buf)?;
    let mut mappings = Vec::new();
    for _ in 0..mappings_cnt.max(0) {
        let applicable = read_bool(buf)?;
        let caches_cnt = read_i32_le(buf)?;

        if applicable {
            // cacheId + per-cache key configuration
            let mut cache_entries: Vec<(i32, HashMap<i32, i32>)> =
                Vec::with_capacity(caches_cnt.max(0) as usize);
            for _ in 0..caches_cnt.max(0) {
                let cache_id = read_i32_le(buf)?;
                let key_cfg = read_cache_key_configuration(buf)?;
                cache_entries.push((cache_id, key_cfg));
            }

            // primary partition → node mapping
            let partition_nodes = read_node_partitions(buf)?;
            let parts = partition_nodes.len() as i32;
            let mask = calculate_mask(parts);

            for (cache_id, key_cfg) in cache_entries {
                mappings.push(CacheAffinityMapping {
                    cache_id,
                    partition_nodes: partition_nodes.clone(),
                    parts,
                    mask,
                    key_cfg,
                    applicable: true,
                });
            }
        } else {
            for _ in 0..caches_cnt.max(0) {
                let cache_id = read_i32_le(buf)?;
                mappings.push(CacheAffinityMapping {
                    cache_id,
                    partition_nodes: Vec::new(),
                    parts: 0,
                    mask: -1,
                    key_cfg: HashMap::new(),
                    applicable: false,
                });
            }
        }
    }

    Ok(CachePartitionsResponse { version, mappings })
}

/// Read a cache key configuration: `[i32: count]([i32: typeId][i32: fieldId])…`.
fn read_cache_key_configuration(buf: &mut Bytes) -> Result<HashMap<i32, i32>> {
    let cnt = read_i32_le(buf)?;
    let mut cfg = HashMap::with_capacity(cnt.max(0) as usize);
    for _ in 0..cnt.max(0) {
        let type_id = read_i32_le(buf)?;
        let field_id = read_i32_le(buf)?;
        cfg.insert(type_id, field_id);
    }
    Ok(cfg)
}

/// Read a `partition → node` mapping block and build a `Vec` indexed by
/// partition (sized to `maxPart + 1`), mirroring Java
/// `ClientCacheAffinityMapping.readNodePartitions`.
fn read_node_partitions(buf: &mut Bytes) -> Result<Vec<Option<Uuid>>> {
    let nodes_cnt = read_i32_le(buf)?;
    if nodes_cnt <= 0 {
        return Ok(Vec::new());
    }

    let mut entries: Vec<(i32, Uuid)> = Vec::new();
    let mut max_part: i32 = -1;
    for _ in 0..nodes_cnt {
        let node = read_uuid_obj(buf)?;
        let part_cnt = read_i32_le(buf)?;
        for _ in 0..part_cnt.max(0) {
            let part = read_i32_le(buf)?;
            if part > max_part {
                max_part = part;
            }
            if let Some(node) = node {
                entries.push((part, node));
            }
        }
    }

    let mut partition_nodes = vec![None; (max_part + 1).max(0) as usize];
    for (part, node) in entries {
        if part >= 0 && (part as usize) < partition_nodes.len() {
            partition_nodes[part as usize] = Some(node);
        }
    }
    Ok(partition_nodes)
}

// ─── Partition math (RendezvousAffinityFunction) ──────────────────────────────

/// Java `IgniteUtils.safeAbs(int)`: `Math.abs`, but maps the overflow case
/// (`i32::MIN`, whose absolute value is not representable) to `0`.
pub fn safe_abs(i: i32) -> i32 {
    let a = i.wrapping_abs();
    if a < 0 { 0 } else { a }
}

/// Java `RendezvousAffinityFunction.calculateMask(parts)`.
///
/// Returns `parts - 1` when `parts` is a power of two (enables the fast
/// bit-mask partition path), otherwise `-1` (the modulo path).
pub fn calculate_mask(parts: i32) -> i32 {
    if parts & (parts - 1) == 0 {
        parts - 1
    } else {
        -1
    }
}

/// Java `RendezvousAffinityFunction.calculatePartition(key, mask, parts)`.
///
/// `hash` is the key's `hashCode()` (see [`affinity_hash`]).  When `mask >= 0`
/// (power-of-two partition count) the fast path `((h ^ (h >>> 16)) & mask)` is
/// used; otherwise `safeAbs(h % parts)`.
pub fn partition(hash: i32, mask: i32, parts: i32) -> i32 {
    if mask >= 0 {
        // (h ^ (h >>> 16)) & mask — `>>> 16` is Java's unsigned right shift.
        let mixed = hash ^ ((hash as u32) >> 16) as i32;
        mixed & mask
    } else {
        safe_abs(hash.wrapping_rem(parts))
    }
}

// ─── Key hashing (JVM hashCode per type) ──────────────────────────────────────

/// Replicate the JVM `hashCode()` of a primitive/String/UUID key value, as the
/// Ignite server computes it for affinity.  Returns `None` for value kinds the
/// client does not (yet) support routing for — the caller must then fall back
/// to the default channel.
pub fn affinity_hash(value: &IgniteValue) -> Option<i32> {
    // Folds a 64-bit value the way `Long.hashCode()` / the high/low halves of
    // `UUID.hashCode()` do: `(int)(v ^ (v >>> 32))`.
    fn fold_i64(v: i64) -> i32 {
        let u = v as u64;
        ((u ^ (u >> 32)) as u32) as i32
    }
    match value {
        IgniteValue::Byte(v) => Some(*v as i32),
        IgniteValue::Short(v) => Some(*v as i32),
        // Character.hashCode() is the UTF-16 code unit value.
        IgniteValue::Char(v) => Some(*v as i32),
        IgniteValue::Int(v) => Some(*v),
        IgniteValue::Long(v) => Some(fold_i64(*v)),
        IgniteValue::Bool(v) => Some(if *v { 1231 } else { 1237 }),
        IgniteValue::String(s) => Some(crate::protocol::java_hash(s)),
        IgniteValue::Uuid(u) => {
            let (msb, lsb) = u.as_u64_pair();
            Some(fold_i64(msb as i64) ^ fold_i64(lsb as i64))
        }
        // Float/Double/Decimal/temporal/byte-array/raw/null keys are not (yet)
        // routed: their JVM hashCode is risky to replicate or not well-defined
        // for affinity, so the caller falls back to the default channel.
        _ => None,
    }
}

// ─── Affinity context ─────────────────────────────────────────────────────────

#[derive(Default)]
struct AffinityState {
    /// Topology version the currently-held mappings apply to.
    version: AffinityTopologyVersion,
    /// `cache_id → mapping`.
    cache_aff: HashMap<i32, CacheAffinityMapping>,
    /// Cache ids with an in-flight `CACHE_PARTITIONS` fetch (single-flight guard).
    pending: HashSet<i32>,
}

/// Shared, thread-safe partition-awareness state for a client.
///
/// Mirrors Java `ClientCacheAffinityContext`: it holds the latest known
/// `partition → node` mappings, the topology version they apply to, and a guard
/// against concurrent duplicate refreshes.  All routing decisions are made under
/// a short non-blocking lock; the network fetch itself happens outside.
pub struct AffinityContext {
    enabled: AtomicBool,
    state: Mutex<AffinityState>,
}

impl AffinityContext {
    /// Create a context.  `enabled = false` makes every routing decision fall
    /// back to the default channel.
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            state: Mutex::new(AffinityState::default()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }

    /// The topology version the held mappings apply to.
    pub fn version(&self) -> AffinityTopologyVersion {
        match self.state.lock() {
            Ok(s) => s.version,
            Err(_) => AffinityTopologyVersion::default(),
        }
    }

    /// Compute the primary node owning `key` in cache `cache_id`, or `None` to
    /// fall back to the default channel (PA disabled, unknown cache, unsupported
    /// key, or unmapped partition).
    pub fn affinity_node(&self, cache_id: i32, key: &IgniteValue) -> Option<Uuid> {
        if !self.is_enabled() {
            return None;
        }
        let hash = affinity_hash(key)?; // unsupported key kind → fall back
        let state = self.state.lock().ok()?;
        let m = state.cache_aff.get(&cache_id)?;
        if !m.applicable || m.parts <= 0 {
            return None;
        }
        let part = partition(hash, m.mask, m.parts);
        m.partition_nodes.get(part as usize).copied().flatten()
    }

    /// Merge a decoded `CACHE_PARTITIONS` response into the held mappings,
    /// respecting topology-version ordering, and clear the affected caches from
    /// the pending set.
    pub fn apply_response(&self, resp: CachePartitionsResponse) {
        let mut state = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        if resp.version < state.version {
            // Stale response for an older topology — ignore, but still release
            // any pending claims it was answering.
            for m in &resp.mappings {
                state.pending.remove(&m.cache_id);
            }
            return;
        }
        if resp.version > state.version {
            // Newer topology: drop all old mappings; caches not covered here are
            // refetched lazily on next use.
            state.cache_aff.clear();
            state.version = resp.version;
        }
        for m in resp.mappings {
            state.pending.remove(&m.cache_id);
            state.cache_aff.insert(m.cache_id, m);
        }
    }

    /// Whether a `CACHE_PARTITIONS` fetch is warranted for `cache_id` given the
    /// newest topology version `observed` on a response header: true when the
    /// mapping is missing or older than the observed topology.
    pub fn needs_refresh(&self, cache_id: i32, observed: Option<(i64, i32)>) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let state = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return false,
        };
        if !state.cache_aff.contains_key(&cache_id) {
            return true;
        }
        if let Some((major, minor)) = observed
            && AffinityTopologyVersion::new(major, minor) > state.version
        {
            return true;
        }
        false
    }

    /// Single-flight claim: returns true if the caller should perform the fetch
    /// for `cache_id` (no other fetch in flight), false if one is already
    /// pending.
    pub fn try_claim_refresh(&self, cache_id: i32) -> bool {
        match self.state.lock() {
            // HashSet::insert returns true when the value was newly inserted.
            Ok(mut state) => state.pending.insert(cache_id),
            Err(_) => false,
        }
    }

    /// Release a refresh claim without applying a response (e.g. the fetch
    /// failed), so a later op can retry.
    pub fn release_refresh(&self, cache_id: i32) {
        if let Ok(mut state) = self.state.lock() {
            state.pending.remove(&cache_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use uuid::Uuid;

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Append a node UUID in `readUuid()` wire form: `[u8:10][i64 LE msb][i64 LE lsb]`.
    fn put_uuid(buf: &mut BytesMut, u: Uuid) {
        let (msb, lsb) = u.as_u64_pair();
        buf.put_u8(crate::protocol::types::type_code::UUID);
        buf.put_i64_le(msb as i64);
        buf.put_i64_le(lsb as i64);
    }

    // ── CACHE_PARTITIONS request encoder ─────────────────────────────────────

    #[test]
    fn encode_partitions_request_writes_header_and_cache_ids() {
        let bytes = encode_cache_partitions_request(7, &[100, 200]);
        let mut b = bytes.clone();
        assert_eq!(b.get_i16_le(), op_code::CACHE_PARTITIONS); // op
        assert_eq!(b.get_i64_le(), 7); // req id
        assert_eq!(b.get_i32_le(), 2); // count
        assert_eq!(b.get_i32_le(), 100);
        assert_eq!(b.get_i32_le(), 200);
        assert_eq!(b.remaining(), 0);
    }

    // ── CACHE_PARTITIONS response decoder ────────────────────────────────────

    #[test]
    fn decode_partitions_applicable_builds_partition_node_array() {
        let node_a = Uuid::from_u128(0xAAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA_AAAA);
        let node_b = Uuid::from_u128(0xBBBB_BBBB_BBBB_BBBB_BBBB_BBBB_BBBB_BBBB);

        let mut buf = BytesMut::new();
        buf.put_i64_le(5); // topVer major
        buf.put_i32_le(0); // topVer minor
        buf.put_i32_le(1); // mappingsCnt
        // ── mapping group ──
        buf.put_u8(1); // applicable = true
        buf.put_i32_le(1); // cachesCnt
        buf.put_i32_le(100); // cacheId
        buf.put_i32_le(0); // keyCfgCnt
        // ── readNodePartitions (primary) ──
        buf.put_i32_le(2); // nodesCnt
        put_uuid(&mut buf, node_a);
        buf.put_i32_le(2); // node A partCnt
        buf.put_i32_le(0);
        buf.put_i32_le(2);
        put_uuid(&mut buf, node_b);
        buf.put_i32_le(2); // node B partCnt
        buf.put_i32_le(1);
        buf.put_i32_le(3);

        let mut bytes = buf.freeze();
        let resp = decode_cache_partitions(&mut bytes).unwrap();

        assert_eq!(resp.version, AffinityTopologyVersion::new(5, 0));
        assert_eq!(resp.mappings.len(), 1);
        let m = &resp.mappings[0];
        assert!(m.applicable);
        assert_eq!(m.cache_id, 100);
        assert_eq!(m.parts, 4);
        assert_eq!(m.mask, 3); // 4 is a power of two
        assert!(m.key_cfg.is_empty());
        assert_eq!(
            m.partition_nodes,
            vec![Some(node_a), Some(node_b), Some(node_a), Some(node_b)]
        );
        assert_eq!(bytes.remaining(), 0, "all bytes consumed");
    }

    #[test]
    fn decode_partitions_reads_key_configuration() {
        let node = Uuid::from_u128(0xCCCC_CCCC_CCCC_CCCC_CCCC_CCCC_CCCC_CCCC);
        let mut buf = BytesMut::new();
        buf.put_i64_le(1);
        buf.put_i32_le(0);
        buf.put_i32_le(1); // mappingsCnt
        buf.put_u8(1); // applicable
        buf.put_i32_le(1); // cachesCnt
        buf.put_i32_le(42); // cacheId
        buf.put_i32_le(1); // keyCfgCnt
        buf.put_i32_le(777); // typeId
        buf.put_i32_le(888); // fieldId
        buf.put_i32_le(1); // nodesCnt
        put_uuid(&mut buf, node);
        buf.put_i32_le(1); // partCnt
        buf.put_i32_le(0);

        let mut bytes = buf.freeze();
        let resp = decode_cache_partitions(&mut bytes).unwrap();
        let m = &resp.mappings[0];
        assert_eq!(m.key_cfg.get(&777), Some(&888));
        assert_eq!(m.partition_nodes, vec![Some(node)]);
    }

    #[test]
    fn decode_partitions_not_applicable_group() {
        let mut buf = BytesMut::new();
        buf.put_i64_le(9);
        buf.put_i32_le(2);
        buf.put_i32_le(1); // mappingsCnt
        buf.put_u8(0); // applicable = false
        buf.put_i32_le(2); // cachesCnt
        buf.put_i32_le(200);
        buf.put_i32_le(201);

        let mut bytes = buf.freeze();
        let resp = decode_cache_partitions(&mut bytes).unwrap();
        assert_eq!(resp.version, AffinityTopologyVersion::new(9, 2));
        assert_eq!(resp.mappings.len(), 2);
        assert!(resp.mappings.iter().all(|m| !m.applicable));
        assert_eq!(resp.mappings[0].cache_id, 200);
        assert_eq!(resp.mappings[1].cache_id, 201);
        assert_eq!(bytes.remaining(), 0);
    }

    #[test]
    fn topology_version_orders_major_then_minor() {
        assert!(AffinityTopologyVersion::new(5, 0) > AffinityTopologyVersion::new(4, 9));
        assert!(AffinityTopologyVersion::new(5, 1) > AffinityTopologyVersion::new(5, 0));
        assert_eq!(
            AffinityTopologyVersion::new(5, 0),
            AffinityTopologyVersion::new(5, 0)
        );
    }

    // ── AffinityContext ──────────────────────────────────────────────────────

    fn mapping(cache_id: i32, nodes: Vec<Option<Uuid>>) -> CacheAffinityMapping {
        let parts = nodes.len() as i32;
        CacheAffinityMapping {
            cache_id,
            partition_nodes: nodes,
            parts,
            mask: calculate_mask(parts),
            key_cfg: HashMap::new(),
            applicable: true,
        }
    }

    fn response(major: i64, minor: i32, mappings: Vec<CacheAffinityMapping>) -> CachePartitionsResponse {
        CachePartitionsResponse {
            version: AffinityTopologyVersion::new(major, minor),
            mappings,
        }
    }

    #[test]
    fn affinity_node_routes_key_to_primary() {
        let a = Uuid::from_u128(0xA);
        let b = Uuid::from_u128(0xB);
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(
            5,
            0,
            vec![mapping(100, vec![Some(a), Some(b), Some(a), Some(b)])],
        ));
        // partition(Int(n)) == n & 3 for parts=4 (mask=3, n>>>16 == 0).
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(0)), Some(a));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(1)), Some(b));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(2)), Some(a));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(3)), Some(b));
    }

    #[test]
    fn affinity_node_unknown_cache_is_none() {
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(1, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        assert_eq!(ctx.affinity_node(999, &IgniteValue::Int(0)), None);
    }

    #[test]
    fn affinity_node_unsupported_key_is_none() {
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(1, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Float(1.0)), None);
    }

    #[test]
    fn affinity_node_disabled_is_none() {
        let ctx = AffinityContext::new(false);
        ctx.apply_response(response(1, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(0)), None);
    }

    #[test]
    fn affinity_node_not_applicable_mapping_is_none() {
        let ctx = AffinityContext::new(true);
        let mut m = mapping(100, vec![]);
        m.applicable = false;
        ctx.apply_response(response(1, 0, vec![m]));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(0)), None);
    }

    #[test]
    fn apply_response_newer_version_replaces_old_mappings() {
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(5, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        ctx.apply_response(response(6, 0, vec![mapping(200, vec![Some(Uuid::from_u128(0xB))])]));
        assert_eq!(ctx.version(), AffinityTopologyVersion::new(6, 0));
        // cache 100's mapping was dropped on the newer topology.
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(0)), None);
        assert_eq!(ctx.affinity_node(200, &IgniteValue::Int(0)), Some(Uuid::from_u128(0xB)));
    }

    #[test]
    fn apply_response_same_version_merges() {
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(5, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        ctx.apply_response(response(5, 0, vec![mapping(200, vec![Some(Uuid::from_u128(0xB))])]));
        assert_eq!(ctx.affinity_node(100, &IgniteValue::Int(0)), Some(Uuid::from_u128(0xA)));
        assert_eq!(ctx.affinity_node(200, &IgniteValue::Int(0)), Some(Uuid::from_u128(0xB)));
    }

    #[test]
    fn apply_response_older_version_ignored() {
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(5, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        ctx.apply_response(response(4, 0, vec![mapping(200, vec![Some(Uuid::from_u128(0xB))])]));
        assert_eq!(ctx.version(), AffinityTopologyVersion::new(5, 0));
        assert_eq!(ctx.affinity_node(200, &IgniteValue::Int(0)), None);
    }

    #[test]
    fn needs_refresh_logic() {
        let ctx = AffinityContext::new(true);
        ctx.apply_response(response(5, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        // missing cache → refresh
        assert!(ctx.needs_refresh(999, None));
        // present, no newer topology → no refresh
        assert!(!ctx.needs_refresh(100, Some((5, 0))));
        // present, but observed topology is newer → refresh
        assert!(ctx.needs_refresh(100, Some((6, 0))));
    }

    #[test]
    fn needs_refresh_disabled_is_false() {
        let ctx = AffinityContext::new(false);
        assert!(!ctx.needs_refresh(100, None));
    }

    #[test]
    fn try_claim_refresh_is_single_flight() {
        let ctx = AffinityContext::new(true);
        assert!(ctx.try_claim_refresh(100), "first claim wins");
        assert!(!ctx.try_claim_refresh(100), "second claim blocked");
        // applying a response for the cache clears its pending claim.
        ctx.apply_response(response(1, 0, vec![mapping(100, vec![Some(Uuid::from_u128(0xA))])]));
        assert!(ctx.try_claim_refresh(100), "claim available again after apply");
        // releasing also frees the claim.
        ctx.release_refresh(100);
        assert!(ctx.try_claim_refresh(100));
    }

    // ── safe_abs ──────────────────────────────────────────────────────────────

    #[test]
    fn safe_abs_basic() {
        assert_eq!(safe_abs(5), 5);
        assert_eq!(safe_abs(-5), 5);
        assert_eq!(safe_abs(0), 0);
    }

    #[test]
    fn safe_abs_min_value_maps_to_zero() {
        // Math.abs(Integer.MIN_VALUE) overflows back to MIN_VALUE (negative);
        // safeAbs clamps it to 0.
        assert_eq!(safe_abs(i32::MIN), 0);
    }

    // ── calculate_mask ────────────────────────────────────────────────────────

    #[test]
    fn calculate_mask_power_of_two() {
        assert_eq!(calculate_mask(1024), 1023);
        assert_eq!(calculate_mask(2), 1);
        assert_eq!(calculate_mask(1), 0);
    }

    #[test]
    fn calculate_mask_non_power_of_two() {
        assert_eq!(calculate_mask(1023), -1);
        assert_eq!(calculate_mask(100), -1);
    }

    // ── partition (power-of-two fast path) ───────────────────────────────────

    #[test]
    fn partition_power_of_two_simple() {
        // h>>>16 == 0 for small h, so result is h & mask.
        assert_eq!(partition(42, 1023, 1024), 42);
    }

    #[test]
    fn partition_power_of_two_mixes_high_bits() {
        // 65537 = 0x1_0001; (0x1_0001 ^ 0x1) = 0x1_0000; & 1023 = 0.
        assert_eq!(partition(65537, 1023, 1024), 0);
    }

    #[test]
    fn partition_power_of_two_negative_hash() {
        // h = -1 (0xFFFF_FFFF); h>>>16 = 0xFFFF; -1 ^ 0xFFFF = 0xFFFF_0000;
        // & 1023 = 0.
        assert_eq!(partition(-1, 1023, 1024), 0);
    }

    // ── partition (modulo path) ──────────────────────────────────────────────

    #[test]
    fn partition_modulo_path() {
        assert_eq!(partition(42, -1, 1000), 42);
        // -42 % 1000 = -42 (truncated), safeAbs → 42.
        assert_eq!(partition(-42, -1, 1000), 42);
    }

    #[test]
    fn partition_modulo_min_value() {
        // Integer.MIN_VALUE % 1000 = -648; safeAbs → 648.
        assert_eq!(partition(i32::MIN, -1, 1000), 648);
    }

    // ── affinity_hash (JVM hashCode per type) ────────────────────────────────

    #[test]
    fn hash_int_is_value() {
        assert_eq!(affinity_hash(&IgniteValue::Int(42)), Some(42));
        assert_eq!(affinity_hash(&IgniteValue::Int(-7)), Some(-7));
    }

    #[test]
    fn hash_long_xor_folds_high_word() {
        // Long.hashCode() = (int)(v ^ (v >>> 32)).
        assert_eq!(affinity_hash(&IgniteValue::Long(123456789)), Some(123456789));
        // 0x1_0000_0001 → low ^ high = 1 ^ 1 ... (int)(0x1_0000_0000) = 0.
        assert_eq!(affinity_hash(&IgniteValue::Long(0x1_0000_0001)), Some(0));
        assert_eq!(affinity_hash(&IgniteValue::Long(-1)), Some(0));
    }

    #[test]
    fn hash_bool_is_1231_or_1237() {
        assert_eq!(affinity_hash(&IgniteValue::Bool(true)), Some(1231));
        assert_eq!(affinity_hash(&IgniteValue::Bool(false)), Some(1237));
    }

    #[test]
    fn hash_byte_short_char() {
        assert_eq!(affinity_hash(&IgniteValue::Byte(-5)), Some(-5));
        assert_eq!(affinity_hash(&IgniteValue::Short(-300)), Some(-300));
        // Character.hashCode() == char value.
        assert_eq!(affinity_hash(&IgniteValue::Char(b'A' as u16)), Some(65));
    }

    #[test]
    fn hash_string_matches_java_string_hashcode() {
        assert_eq!(affinity_hash(&IgniteValue::String("abc".into())), Some(96354));
    }

    #[test]
    fn hash_uuid_xor_folds_both_words() {
        // UUID.hashCode() = (int)(msb ^ (msb>>>32)) ^ (int)(lsb ^ (lsb>>>32)).
        let u = Uuid::from_u128(0x0102030405060708_1112131415161718);
        let msb: u64 = 0x0102030405060708;
        let lsb: u64 = 0x1112131415161718;
        let fold = |v: u64| ((v ^ (v >> 32)) as u32) as i32;
        let expected = fold(msb) ^ fold(lsb);
        assert_eq!(affinity_hash(&IgniteValue::Uuid(u)), Some(expected));
    }

    #[test]
    fn hash_unsupported_kinds_return_none() {
        assert_eq!(affinity_hash(&IgniteValue::Null), None);
        assert_eq!(affinity_hash(&IgniteValue::Float(1.0)), None);
        assert_eq!(affinity_hash(&IgniteValue::Double(1.0)), None);
        assert_eq!(affinity_hash(&IgniteValue::ByteArray(vec![1, 2, 3])), None);
        assert_eq!(affinity_hash(&IgniteValue::RawObject(vec![1, 2, 3])), None);
    }
}
