//! Multi-node connection registry for partition-aware routing.
//!
//! Holds one deadpool [`Pool`] per configured address plus a learned
//! `node UUID → pool` index, mirroring Java `ReliableChannelImpl`'s
//! `nodeChannels` map and default-channel fallback.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tracing::debug;
use uuid::Uuid;

use crate::affinity::{
    AffinityContext, CachePartitionsResponse, decode_cache_partitions,
    encode_cache_partitions_request,
};
use crate::discovery::{
    NodeEndpointsResponse, UNKNOWN_TOP_VER, decode_node_endpoints, encode_node_endpoints_request,
};
use crate::error::{IgniteError, Result};
use crate::pool::{IgniteClientConfig, Pool, PooledConn, build_pool_for_addr};
use crate::transport::{IgniteConnection, next_request_id};

/// Pure pool-selection logic: maps node UUIDs to pool indices and round-robins
/// over the default set.  Separated from I/O so it is unit-testable.
pub(crate) struct PoolSelector {
    pool_count: usize,
    /// node UUID → index into the registry's pool vector.
    node_index: Mutex<HashMap<Uuid, usize>>,
    /// Round-robin cursor for default selection.
    rr: AtomicUsize,
}

impl PoolSelector {
    pub(crate) fn new(pool_count: usize) -> Self {
        Self {
            pool_count,
            node_index: Mutex::new(HashMap::new()),
            rr: AtomicUsize::new(0),
        }
    }

    /// Record that the pool at `idx` serves the node with UUID `node`.
    pub(crate) fn register_node(&self, node: Uuid, idx: usize) {
        if let Ok(mut map) = self.node_index.lock() {
            map.insert(node, idx);
        }
    }

    /// The pool index serving `node`, if known.
    pub(crate) fn index_for_node(&self, node: Uuid) -> Option<usize> {
        self.node_index.lock().ok()?.get(&node).copied()
    }

    /// The next default pool index (round-robin), in `0..pool_count`.
    pub(crate) fn next_default(&self) -> usize {
        if self.pool_count == 0 {
            return 0;
        }
        self.rr.fetch_add(1, Ordering::Relaxed) % self.pool_count
    }
}

// ─── ChannelRegistry ──────────────────────────────────────────────────────────

/// One connection pool per node — configured seed addresses plus any nodes
/// learned via endpoint discovery — keyed by node UUID, with a default
/// round-robin/fallback over the seeds. Mirrors Java `ReliableChannelImpl`'s
/// `nodeChannels` map.
pub(crate) struct ChannelRegistry {
    config: Arc<IgniteClientConfig>,
    /// Seed pools (indices `0..seed_count`) followed by discovered pools.
    pools: Mutex<Vec<Pool>>,
    /// Clone of the primary (seed 0) pool, kept lock-free for status checks.
    primary_pool: Pool,
    seed_count: usize,
    selector: PoolSelector,
    /// Newest affinity topology version observed on any response header.
    latest_topology: Mutex<Option<(i64, i32)>>,
    /// Single-flight guard so endpoint discovery runs at most once.
    discovery_done: AtomicBool,
}

impl ChannelRegistry {
    pub(crate) fn new(config: Arc<IgniteClientConfig>) -> Self {
        let pools: Vec<Pool> = config
            .addresses
            .iter()
            .map(|addr| build_pool_for_addr(&config, addr))
            .collect();
        let seed_count = pools.len();
        // config always has ≥ 1 address, so pools[0] exists.
        let primary_pool = pools[0].clone();
        Self {
            config,
            pools: Mutex::new(pools),
            primary_pool,
            seed_count,
            selector: PoolSelector::new(seed_count),
            latest_topology: Mutex::new(None),
            discovery_done: AtomicBool::new(false),
        }
    }

    /// Number of configured (seed) node pools.
    pub(crate) fn node_count(&self) -> usize {
        self.seed_count
    }

    /// deadpool status of the primary pool, for observability.
    pub(crate) fn primary_status(&self) -> deadpool::managed::Status {
        self.primary_pool.status()
    }

    /// A clone of the pool at `idx`, if it exists. Pools are `Arc`-backed so the
    /// clone is cheap and lets us release the lock before awaiting `get()`.
    fn pool_at(&self, idx: usize) -> Option<Pool> {
        self.pools.lock().ok()?.get(idx).cloned()
    }

    /// Check out a connection to the `target` node when it is known, otherwise a
    /// default round-robin connection.  Any miss (unknown node, dead target
    /// pool) falls back to the default set, so routing never changes results.
    pub(crate) async fn get(&self, target: Option<Uuid>) -> Result<PooledConn> {
        // Prefer the target node's pool; any miss (unknown node or unavailable
        // target pool) falls through to the default set so results never change.
        if let Some(node) = target
            && let Some(idx) = self.selector.index_for_node(node)
            && let Some(pool) = self.pool_at(idx)
            && let Ok(conn) = pool.get().await
        {
            return Ok(conn);
        }
        self.get_default().await
    }

    /// Round-robin checkout across the seed pool set, trying each pool once
    /// before giving up.  Learns the node UUID served by whichever pool answers.
    async fn get_default(&self) -> Result<PooledConn> {
        let n = self.seed_count;
        if n == 0 {
            return Err(IgniteError::Pool("no channels configured".into()));
        }
        let start = self.selector.next_default();
        let mut last_err: Option<IgniteError> = None;
        for off in 0..n {
            let idx = (start + off) % n;
            let Some(pool) = self.pool_at(idx) else {
                continue;
            };
            match pool.get().await {
                Ok(conn) => {
                    if let Some(node) = conn.node_id() {
                        self.selector.register_node(node, idx);
                    }
                    return Ok(conn);
                }
                Err(e) => last_err = Some(e.into()),
            }
        }
        Err(last_err.unwrap_or_else(|| IgniteError::Pool("all channels unavailable".into())))
    }

    // ── Endpoint discovery ────────────────────────────────────────────────────

    /// Add a pool for a discovered node endpoint if its UUID is not already
    /// known. Returns `true` when a new pool was added.
    fn add_endpoint(&self, node_id: Uuid, addr: &str) -> bool {
        if self.selector.index_for_node(node_id).is_some() {
            return false;
        }
        let pool = build_pool_for_addr(&self.config, addr);
        let idx = match self.pools.lock() {
            Ok(mut pools) => {
                pools.push(pool);
                pools.len() - 1
            }
            Err(_) => return false,
        };
        self.selector.register_node(node_id, idx);
        true
    }

    /// Discover all cluster node endpoints (single-flight) and open channels to
    /// nodes not already known, so affinity routing can reach nodes that were
    /// not in the configured address list. No-op when endpoint discovery is
    /// disabled in the config.
    pub(crate) async fn discover(&self) {
        if !self.config.endpoint_discovery.unwrap_or(true) {
            return;
        }
        if self.discovery_done.swap(true, Ordering::AcqRel) {
            return; // already done by another task
        }
        // Learn each seed node's UUID first so discovery does not re-add them.
        self.prime_seed_nodes().await;

        match self.fetch_endpoints().await {
            Ok(resp) => {
                let mut added = 0usize;
                for ep in &resp.added {
                    // Use the first advertised address; an unreachable choice is
                    // harmless — routing to that node just falls back to default.
                    if let Some(addr) = ep.addresses.first()
                        && self.add_endpoint(ep.node_id, &format!("{addr}:{}", ep.port))
                    {
                        added += 1;
                    }
                }
                debug!(
                    nodes = resp.added.len(),
                    added, "endpoint discovery complete"
                );
            }
            Err(e) => {
                // Allow a later op to retry.
                self.discovery_done.store(false, Ordering::Release);
                debug!(error = %e, "endpoint discovery failed; using configured nodes only");
            }
        }
    }

    /// Open a connection to each seed pool to learn (and register) its node UUID.
    async fn prime_seed_nodes(&self) {
        for idx in 0..self.seed_count {
            if let Some(pool) = self.pool_at(idx)
                && let Ok(conn) = pool.get().await
                && let Some(node) = conn.node_id()
            {
                self.selector.register_node(node, idx);
            }
        }
    }

    async fn fetch_endpoints(&self) -> Result<NodeEndpointsResponse> {
        let req_id = next_request_id();
        let payload = encode_node_endpoints_request(req_id, UNKNOWN_TOP_VER);
        let conn = self.get_default().await?;
        let mut resp = conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;
        decode_node_endpoints(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Record the newest topology version a connection has observed, so the
    /// affinity context can tell when its mappings are stale.
    pub(crate) fn observe_topology(&self, conn: &IgniteConnection) {
        if let Some(tv) = conn.topology_version()
            && let Ok(mut latest) = self.latest_topology.lock()
            && latest.is_none_or(|cur| tv > cur)
        {
            *latest = Some(tv);
        }
    }

    /// The newest topology version observed across all connections.
    fn latest_topology(&self) -> Option<(i64, i32)> {
        self.latest_topology.lock().ok().and_then(|l| *l)
    }

    /// Lazily (re)fetch the `partition → node` mapping for `cache_id` when it is
    /// missing or stale, guarded so only one fetch runs at a time per cache.
    pub(crate) async fn ensure_affinity(&self, affinity: &AffinityContext, cache_id: i32) {
        if !affinity.is_enabled() {
            return;
        }
        // Learn any cluster nodes not in the configured address list (once), so
        // routing can reach them.
        self.discover().await;
        if !affinity.needs_refresh(cache_id, self.latest_topology()) {
            return;
        }
        if !affinity.try_claim_refresh(cache_id) {
            return; // another task is already fetching this cache's mapping
        }
        match self.fetch_partitions(cache_id).await {
            Ok(resp) => {
                let applicable = resp.mappings.iter().filter(|m| m.applicable).count();
                debug!(
                    cache_id,
                    version = ?resp.version,
                    mappings = resp.mappings.len(),
                    applicable,
                    "fetched cache partitions"
                );
                affinity.apply_response(resp);
            }
            // Fail safe: drop the claim so a later op can retry; routing falls
            // back to the default channel in the meantime.
            Err(e) => {
                debug!(cache_id, error = %e, "cache partitions fetch failed; using default channel");
                affinity.release_refresh(cache_id);
            }
        }
    }

    async fn fetch_partitions(&self, cache_id: i32) -> Result<CachePartitionsResponse> {
        let conn = self.get_default().await?;
        // Use the DC-aware wire format only when this server negotiated it.
        let dc_aware = conn.dc_aware();
        let req_id = next_request_id();
        let payload = encode_cache_partitions_request(
            req_id,
            &[cache_id],
            dc_aware,
            self.config.data_center_id.as_deref(),
        );
        let mut resp = conn
            .request(req_id, payload)
            .await
            .map_err(IgniteError::Transport)?;
        self.observe_topology(&conn);
        decode_cache_partitions(&mut resp, dc_aware).map_err(IgniteError::Protocol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup_node() {
        let s = PoolSelector::new(3);
        let n = Uuid::from_u128(0xABC);
        assert_eq!(s.index_for_node(n), None);
        s.register_node(n, 2);
        assert_eq!(s.index_for_node(n), Some(2));
    }

    #[test]
    fn next_default_round_robins_and_wraps() {
        let s = PoolSelector::new(3);
        let seq: Vec<usize> = (0..7).map(|_| s.next_default()).collect();
        assert_eq!(seq, vec![0, 1, 2, 0, 1, 2, 0]);
    }

    #[test]
    fn next_default_single_pool_is_always_zero() {
        let s = PoolSelector::new(1);
        assert_eq!(s.next_default(), 0);
        assert_eq!(s.next_default(), 0);
    }
}
