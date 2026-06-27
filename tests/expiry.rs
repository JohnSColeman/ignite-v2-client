//! Cache entry expiry / TTL integration tests against a live Apache Ignite node.
//!
//! Exercises all three `ExpiryPolicy` durations end to end: an entry is removed
//! after its **creation** TTL, an overwrite resets the lifetime via the
//! **update** TTL, and a read resets it via the **access** TTL.
//!
//! Prerequisites: Ignite on localhost:10800 (any cluster — TTL is per-operation,
//! not a cache-config feature). Run: cargo test --test expiry -- --test-threads=1

use std::time::Duration;

use ignite_client::{ExpiryDuration, ExpiryPolicy, IgniteClient, IgniteClientConfig, IgniteValue};

const ADDR: &str = "localhost:10800";
const TTL_MS: u64 = 400;

fn client() -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR))
}

fn ttl() -> ExpiryDuration {
    ExpiryDuration::Millis(TTL_MS)
}

/// Sleep comfortably past the TTL (3×) to avoid timing flakiness.
async fn sleep_past_ttl() {
    tokio::time::sleep(Duration::from_millis(TTL_MS * 3)).await;
}

fn is_absent(v: &IgniteValue) -> bool {
    matches!(v, IgniteValue::Null)
}

/// Creation TTL: a freshly inserted entry disappears after its lifetime.
#[tokio::test]
async fn expiry_creation_ttl() {
    let c = client();
    let cache = c
        .get_or_create_cache("EXPIRY_CREATE")
        .await
        .expect("create cache");
    let key = IgniteValue::Int(1);

    // New entries live TTL_MS; updates/accesses leave the lifetime unchanged.
    let on_create = ExpiryPolicy::new(ttl(), ExpiryDuration::Unchanged, ExpiryDuration::Unchanged);
    cache
        .with_expiry_policy(on_create)
        .put(key.clone(), IgniteValue::Int(42))
        .await
        .expect("put");

    // Present immediately.
    assert_eq!(
        cache.get(key.clone()).await.expect("get before ttl"),
        IgniteValue::Int(42)
    );

    sleep_past_ttl().await;

    assert!(
        is_absent(&cache.get(key).await.expect("get after ttl")),
        "entry should have expired via the creation TTL"
    );
    c.destroy_cache("EXPIRY_CREATE").await.ok();
}

/// Update TTL: an eternal entry only starts expiring once it is overwritten.
#[tokio::test]
async fn expiry_update_ttl() {
    let c = client();
    let cache = c
        .get_or_create_cache("EXPIRY_UPDATE")
        .await
        .expect("create cache");
    let key = IgniteValue::Int(1);

    // Create the entry eternal so creation doesn't expire it.
    let eternal = ExpiryPolicy::new(
        ExpiryDuration::Eternal,
        ExpiryDuration::Unchanged,
        ExpiryDuration::Unchanged,
    );
    cache
        .with_expiry_policy(eternal)
        .put(key.clone(), IgniteValue::Int(1))
        .await
        .expect("put create");

    sleep_past_ttl().await;
    assert_eq!(
        cache.get(key.clone()).await.expect("get eternal"),
        IgniteValue::Int(1),
        "eternal entry should survive past the TTL"
    );

    // Overwrite (an update event) applies the TTL.
    let on_update = ExpiryPolicy::new(ExpiryDuration::Unchanged, ttl(), ExpiryDuration::Unchanged);
    cache
        .with_expiry_policy(on_update)
        .put(key.clone(), IgniteValue::Int(2))
        .await
        .expect("put update");

    sleep_past_ttl().await;
    assert!(
        is_absent(&cache.get(key).await.expect("get after update ttl")),
        "entry should have expired via the update TTL"
    );
    c.destroy_cache("EXPIRY_UPDATE").await.ok();
}

/// Access TTL: reading the entry (with an access policy) sets its lifetime.
#[tokio::test]
async fn expiry_access_ttl() {
    let c = client();
    let cache = c
        .get_or_create_cache("EXPIRY_ACCESS")
        .await
        .expect("create cache");
    let key = IgniteValue::Int(1);

    let eternal = ExpiryPolicy::new(
        ExpiryDuration::Eternal,
        ExpiryDuration::Unchanged,
        ExpiryDuration::Unchanged,
    );
    cache
        .with_expiry_policy(eternal)
        .put(key.clone(), IgniteValue::Int(7))
        .await
        .expect("put");

    // Reading via the access-TTL handle sets the lifetime to TTL_MS.
    let on_access = ExpiryPolicy::new(ExpiryDuration::Unchanged, ExpiryDuration::Unchanged, ttl());
    assert_eq!(
        cache
            .with_expiry_policy(on_access)
            .get(key.clone())
            .await
            .expect("access get"),
        IgniteValue::Int(7)
    );

    sleep_past_ttl().await;

    // Plain get (no policy) so the check itself doesn't reset the lifetime.
    assert!(
        is_absent(&cache.get(key).await.expect("get after access ttl")),
        "entry should have expired via the access TTL"
    );
    c.destroy_cache("EXPIRY_ACCESS").await.ok();
}
