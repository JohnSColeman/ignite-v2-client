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
