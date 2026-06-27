//! Partition awareness / affinity routing integration tests.
//!
//! Prerequisites: Ignite running on localhost:10800 with no authentication.
//! For true multi-node routing, set `IGNITE_ADDRS` to a comma-separated list of
//! node addresses (e.g. `localhost:10800,localhost:10801,localhost:10802`);
//! otherwise the single default address is used, which still exercises the full
//! partition-awareness pipeline (handshake node UUID, `CACHE_PARTITIONS`
//! fetch/decode, local partition computation, routed send + fallback).
//!
//! Run: cargo test --test partition_awareness -- --nocapture --test-threads=1

use ignite_client::{IgniteClient, IgniteClientConfig, IgniteValue};

/// Initialise a tracing subscriber once so `RUST_LOG=ignite_client=debug` reveals
/// the partition-awareness routing decisions (e.g. "fetched cache partitions").
fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_test_writer()
            .try_init();
    });
}

fn addresses() -> Vec<String> {
    match std::env::var("IGNITE_ADDRS") {
        Ok(v) if !v.trim().is_empty() => v.split(',').map(|s| s.trim().to_string()).collect(),
        _ => vec!["localhost:10800".to_string()],
    }
}

fn client(partition_awareness: bool) -> IgniteClient {
    let cfg = IgniteClientConfig::new("localhost:10800")
        .with_addresses(addresses())
        .with_partition_awareness(partition_awareness);
    IgniteClient::new(cfg)
}

/// The core safety guarantee: every key read back returns exactly what was
/// written, regardless of whether partition awareness routed the request to the
/// owning node or fell back to the default channel.
#[tokio::test]
async fn pa_put_get_roundtrip_is_correct() {
    init_tracing();
    let c = client(true);
    // Cache names are treated case-insensitively (cache_id upper-cases them), so
    // use an upper-case name to match the server-side cache id.
    let cache = c
        .get_or_create_cache("PA_ROUNDTRIP")
        .await
        .expect("create cache");

    // A spread of Int keys lands in many different partitions.
    for k in 0..200i32 {
        cache
            .put(IgniteValue::Int(k), IgniteValue::Long(i64::from(k) * 7))
            .await
            .unwrap_or_else(|e| panic!("put {k} failed: {e}"));
    }
    for k in 0..200i32 {
        let got = cache
            .get(IgniteValue::Int(k))
            .await
            .unwrap_or_else(|e| panic!("get {k} failed: {e}"));
        assert_eq!(
            got,
            IgniteValue::Long(i64::from(k) * 7),
            "key {k} read back wrong value"
        );
    }

    c.destroy_cache("PA_ROUNDTRIP").await.ok();
}

/// Endpoint discovery: configure ONLY one node but force partition awareness on.
/// The client should discover the other cluster nodes (op
/// `CLUSTER_GROUP_GET_NODE_ENDPOINTS`), open channels to them, and still serve
/// every key correctly. Run with `RUST_LOG=ignite_client=debug` to observe the
/// `endpoint discovery complete` log with the number of nodes added.
#[tokio::test]
async fn pa_endpoint_discovery_reaches_unconfigured_nodes() {
    init_tracing();
    let cfg = IgniteClientConfig::new("localhost:10800").with_partition_awareness(true);
    let c = IgniteClient::new(cfg);

    let cache = c
        .get_or_create_cache("PA_DISCOVERY")
        .await
        .expect("create cache");

    for k in 0..120i32 {
        cache
            .put(IgniteValue::Int(k), IgniteValue::Int(k * 2))
            .await
            .unwrap_or_else(|e| panic!("put {k} failed: {e}"));
    }
    for k in 0..120i32 {
        let got = cache
            .get(IgniteValue::Int(k))
            .await
            .unwrap_or_else(|e| panic!("get {k} failed: {e}"));
        assert_eq!(got, IgniteValue::Int(k * 2), "key {k} read back wrong value");
    }

    c.destroy_cache("PA_DISCOVERY").await.ok();
}

/// Cache the DC read-hop demo uses.  It must be pre-defined on the cluster with
/// `FULL_SYNC` + `readFromBackup` (see `local-cluster/ignite-config.xml`) — the
/// server only emits a same-DC partition map for caches configured that way, and
/// the client cannot create a cache with those settings.  The literal must match
/// the cache name in that config file.
const DC_DEMO_CACHE: &str = "DC_DEMO";

/// The client's data-center id for the demo; must match a node DC id set by
/// `local-cluster/start.sh` in `IGNITE_DC_DEMO` mode (nodes 1 & 2 are `DC1`).
const DC_DEMO_CLIENT_DC: &str = "DC1";

/// DC-aware read-from-backup path. Best run on a DC-configured cluster
/// (`IGNITE_DC_DEMO=1 ./local-cluster/start.sh`, nodes 1&2 = DC1, node 3 = DC2)
/// with the pre-defined [`DC_DEMO_CACHE`] cache (FULL_SYNC + readFromBackup).
///
/// With the client's `dcId = DC1`, the server returns a same-DC partition map,
/// so reads of keys whose primary is the DC2 node route to a DC1 backup rather
/// than the primary. This exercises the full DC path — the request carries the
/// `dcId`, the response's DC partition map is decoded, and reads route through
/// it — and asserts every value still reads back correctly. (Against a non-DC
/// cluster the DC map is empty and reads fall back to the primary; the test
/// still passes.)
#[tokio::test]
async fn pa_dc_aware_read_path_is_correct() {
    init_tracing();
    let cfg = IgniteClientConfig::new("localhost:10800")
        .with_addresses(addresses())
        .with_data_center_id(DC_DEMO_CLIENT_DC);
    let c = IgniteClient::new(cfg);

    // `cache()` only builds a handle — it neither creates nor checks the cache.
    // Verify the DC-demo cache is actually provisioned so a missing cluster
    // setup fails here with guidance rather than a cryptic error on first `put`.
    let names = c.cache_names().await.expect("list cache names");
    assert!(
        names.iter().any(|n| n.eq_ignore_ascii_case(DC_DEMO_CACHE)),
        "cache {DC_DEMO_CACHE:?} not found — start the cluster from local-cluster/ \
         with `IGNITE_DC_DEMO=1 ./local-cluster/start.sh`. The cache must be \
         pre-defined with FULL_SYNC + readFromBackup (ignite-config.xml)."
    );
    let cache = c.cache(DC_DEMO_CACHE);

    for k in 0..300i32 {
        cache
            .put(IgniteValue::Int(k), IgniteValue::Int(k))
            .await
            .unwrap_or_else(|e| panic!("put {k} failed: {e}"));
    }
    // Reads route via the DC map; for DC2-primary keys this is a DC1 backup.
    for k in 0..300i32 {
        let got = cache
            .get(IgniteValue::Int(k))
            .await
            .unwrap_or_else(|e| panic!("get {k} failed: {e}"));
        assert_eq!(got, IgniteValue::Int(k), "key {k} read back wrong value");
    }
}

/// Partition awareness is a pure optimization: results must be identical whether
/// it is on or off.  Writes go through a PA-on client; reads are verified through
/// both a PA-on and a PA-off client.
#[tokio::test]
async fn pa_on_and_off_agree() {
    let writer = client(true);
    let reader_pa = client(true);
    let reader_plain = client(false);

    let name = "PA_PARITY";
    let cache_w = writer.get_or_create_cache(name).await.expect("create cache");

    let keys: Vec<i32> = (0..150).collect();
    for &k in &keys {
        cache_w
            .put(IgniteValue::String(format!("key-{k}")), IgniteValue::Int(k))
            .await
            .unwrap_or_else(|e| panic!("put key-{k} failed: {e}"));
    }

    let cache_pa = reader_pa.cache(name);
    let cache_plain = reader_plain.cache(name);
    for &k in &keys {
        let key = IgniteValue::String(format!("key-{k}"));
        let via_pa = cache_pa.get(key.clone()).await.expect("pa get");
        let via_plain = cache_plain.get(key).await.expect("plain get");
        assert_eq!(via_pa, IgniteValue::Int(k), "PA read wrong for key-{k}");
        assert_eq!(via_pa, via_plain, "PA and non-PA disagree for key-{k}");
    }

    writer.destroy_cache(name).await.ok();
}
