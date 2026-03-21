//! Functional cache tests — Rust port of selected tests from:
//! - `FunctionalTest.java` (thin client internal tests)
//!   <https://github.com/apache/ignite/blob/master/modules/core/src/test/java/org/apache/ignite/internal/client/thin/FunctionalTest.java>
//!
//! **Java → Rust mapping**
//!
//! | Java test | Rust test |
//! |-----------|-----------|
//! | `testCacheManagement` | `functional_cache_management` |
//! | `testPutGet` | `functional_cache_put_get` |
//! | `testAtomicPutGet` | `functional_cache_atomic_put_get` |
//! | `testBatchPutGet` | `functional_cache_batch_put_get` |
//! | `testRemoveReplace` | `functional_cache_remove_replace` |
//!
//! **Omitted:**
//! - `testCacheConfiguration` — requires `QueryEntity`/`QueryIndex` configuration,
//!   not supported by the Rust thin client.
//! - `testDataTypes` — relies on Ignite `BinaryObject` marshalling API.
//! - `testTransactions` / `testTransactionsWithLabel` — basic commit/rollback
//!   already covered by `smoke_tx_kv_commit` and `smoke_tx_kv_rollback`.
//! - `testTransactionsLimit` — depends on server thread-pool configuration.
//! - `ThinClientNonTransactionalOperationsInTxTest` — `clear()` / `removeAll()`
//!   (no-argument variants) are not part of the Rust cache API.
//!
//! Prerequisites: Ignite running on localhost:10800 with no authentication.
//! Run: cargo test --test functional_cache -- --nocapture --test-threads=1

use ignite_client::{IgniteClient, IgniteClientConfig, IgniteValue};

const ADDR: &str = "localhost:10800";

fn client() -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Extract an `i64` from a `IgniteValue::Long`; panics with context on mismatch.
fn as_long(v: &IgniteValue, ctx: &str) -> i64 {
    match v {
        IgniteValue::Long(n) => *n,
        other => panic!("{ctx}: expected Long, got {other:?}"),
    }
}

/// Extract a `String` from `IgniteValue::String`; panics with context on mismatch.
fn as_str(v: &IgniteValue, ctx: &str) -> String {
    match v {
        IgniteValue::String(s) => s.clone(),
        other => panic!("{ctx}: expected String, got {other:?}"),
    }
}

// ─── 1. testCacheManagement ───────────────────────────────────────────────────

/// Ports `testCacheManagement`.
///
/// Tests the full cache lifecycle:
/// - `get_or_create_cache` creates the cache when it does not exist.
/// - `put` stores an entry; `get_size` reflects it.
/// - `cache_names` lists the cache while it exists.
/// - `destroy_cache` removes it; `cache_names` no longer includes it.
#[tokio::test]
async fn functional_cache_management() {
    const NAME: &str = "FC_MANAGEMENT";
    let c = client();

    // Ensure clean state.
    let _ = c.destroy_cache(NAME).await;

    let cache = c
        .get_or_create_cache(NAME)
        .await
        .expect("get_or_create_cache failed");

    cache
        .put(IgniteValue::Int(1), IgniteValue::String("val".into()))
        .await
        .expect("put failed");

    let size = cache.get_size().await.expect("get_size failed");
    assert_eq!(size, 1, "size should be 1 after one put");

    // Cache must appear in the names list.
    let names = c.cache_names().await.expect("cache_names failed");
    assert!(
        names.iter().any(|n| n == NAME),
        "cache_names should include {NAME}, got: {names:?}"
    );

    // Destroy and verify it disappears from the names list.
    c.destroy_cache(NAME).await.expect("destroy_cache failed");

    let names_after = c
        .cache_names()
        .await
        .expect("cache_names after destroy failed");
    assert!(
        !names_after.iter().any(|n| n == NAME),
        "{NAME} should be absent from cache_names after destroy, got: {names_after:?}"
    );
}

// ─── 2. testPutGet ────────────────────────────────────────────────────────────

/// Ports `testPutGet`.
///
/// Tests basic single-entry put / get / containsKey / remove round-trip.
/// The Java test exercises Integer→Person, Person→Integer, and Person→Person
/// key-value combinations backed by binary marshalling.  The Rust client uses
/// `IgniteValue` directly, so we verify Int→String and String→Long pairs.
#[tokio::test]
async fn functional_cache_put_get() {
    const NAME: &str = "FC_PUT_GET";
    let c = client();
    let _ = c.destroy_cache(NAME).await;
    let cache = c
        .get_or_create_cache(NAME)
        .await
        .expect("get_or_create_cache failed");

    // Miss before any put.
    assert_eq!(
        cache
            .get(IgniteValue::Int(1))
            .await
            .expect("get miss failed"),
        IgniteValue::Null,
        "absent key must return Null"
    );
    assert!(
        !cache
            .contains_key(IgniteValue::Int(1))
            .await
            .expect("contains_key failed"),
        "contains_key should be false before put"
    );

    // Put then hit.
    cache
        .put(IgniteValue::Int(1), IgniteValue::String("Alice".into()))
        .await
        .expect("put failed");

    assert!(
        cache
            .contains_key(IgniteValue::Int(1))
            .await
            .expect("contains_key after put failed"),
        "contains_key should be true after put"
    );
    assert_eq!(
        cache
            .get(IgniteValue::Int(1))
            .await
            .expect("get hit failed"),
        IgniteValue::String("Alice".into()),
        "get should return the stored value"
    );

    // Overwrite.
    cache
        .put(IgniteValue::Int(1), IgniteValue::String("Bob".into()))
        .await
        .expect("overwrite put failed");
    assert_eq!(
        cache
            .get(IgniteValue::Int(1))
            .await
            .expect("get overwrite failed"),
        IgniteValue::String("Bob".into()),
        "second put should overwrite the first"
    );

    // Remove then miss again.
    cache
        .remove(IgniteValue::Int(1))
        .await
        .expect("remove failed");
    assert!(
        !cache
            .contains_key(IgniteValue::Int(1))
            .await
            .expect("contains_key after remove failed"),
        "contains_key should be false after remove"
    );
    assert_eq!(
        cache
            .get(IgniteValue::Int(1))
            .await
            .expect("get after remove failed"),
        IgniteValue::Null,
        "get should return Null after remove"
    );

    // String key variant.
    cache
        .put(IgniteValue::String("key".into()), IgniteValue::Long(42))
        .await
        .expect("string-key put failed");
    assert_eq!(
        cache
            .get(IgniteValue::String("key".into()))
            .await
            .expect("string-key get failed"),
        IgniteValue::Long(42),
        "String→Long round-trip"
    );

    c.destroy_cache(NAME).await.expect("destroy failed");
}

// ─── 3. testAtomicPutGet ─────────────────────────────────────────────────────

/// Ports `testAtomicPutGet`.
///
/// Tests the atomic read-modify-write operations:
/// `get_and_put`, `get_and_remove`, `put_if_absent`, `get_and_replace`.
///
/// The Java original also tests `getAndPutIfAbsent` (returns old or stores new
/// atomically); the Rust cache API has `put_if_absent` (bool) and separate
/// `get_and_put`, so the semantic is covered by combining those.
#[tokio::test]
async fn functional_cache_atomic_put_get() {
    const NAME: &str = "FC_ATOMIC";
    let c = client();
    let _ = c.destroy_cache(NAME).await;
    let cache = c
        .get_or_create_cache(NAME)
        .await
        .expect("get_or_create_cache failed");

    // get_and_put on absent key → Null; then on existing key → old value.
    assert_eq!(
        cache
            .get_and_put(IgniteValue::Int(1), IgniteValue::String("1".into()))
            .await
            .expect("get_and_put absent failed"),
        IgniteValue::Null,
        "get_and_put on absent key must return Null"
    );
    assert_eq!(
        cache
            .get_and_put(IgniteValue::Int(1), IgniteValue::String("1.1".into()))
            .await
            .expect("get_and_put existing failed"),
        IgniteValue::String("1".into()),
        "get_and_put should return the previous value"
    );

    // get_and_remove returns old value, then Null on second call.
    assert_eq!(
        cache
            .get_and_remove(IgniteValue::Int(1))
            .await
            .expect("get_and_remove failed"),
        IgniteValue::String("1.1".into()),
        "get_and_remove should return the removed value"
    );
    assert_eq!(
        cache
            .get_and_remove(IgniteValue::Int(1))
            .await
            .expect("get_and_remove absent failed"),
        IgniteValue::Null,
        "get_and_remove on absent key must return Null"
    );

    // put_if_absent: first call stores; second call is a no-op.
    assert!(
        cache
            .put_if_absent(IgniteValue::Int(1), IgniteValue::String("1".into()))
            .await
            .expect("put_if_absent first failed"),
        "put_if_absent should return true on absent key"
    );
    assert!(
        !cache
            .put_if_absent(IgniteValue::Int(1), IgniteValue::String("1.1".into()))
            .await
            .expect("put_if_absent second failed"),
        "put_if_absent should return false when key already exists"
    );
    // Value must still be the first one stored.
    assert_eq!(
        cache
            .get(IgniteValue::Int(1))
            .await
            .expect("get after put_if_absent failed"),
        IgniteValue::String("1".into()),
        "put_if_absent must not overwrite an existing value"
    );

    // get_and_replace on existing key → old value; on absent key → Null.
    assert_eq!(
        cache
            .get_and_replace(IgniteValue::Int(1), IgniteValue::String("1.1".into()))
            .await
            .expect("get_and_replace existing failed"),
        IgniteValue::String("1".into()),
        "get_and_replace should return the old value"
    );
    assert_eq!(
        cache
            .get_and_replace(IgniteValue::Int(2), IgniteValue::String("2".into()))
            .await
            .expect("get_and_replace absent failed"),
        IgniteValue::Null,
        "get_and_replace on absent key must return Null"
    );
    // Key 2 must not have been created.
    assert_eq!(
        cache
            .get(IgniteValue::Int(2))
            .await
            .expect("get absent after get_and_replace failed"),
        IgniteValue::Null,
        "get_and_replace must not create a new entry"
    );

    c.destroy_cache(NAME).await.expect("destroy failed");
}

// ─── 4. testBatchPutGet ───────────────────────────────────────────────────────

/// Ports `testBatchPutGet`.
///
/// Tests bulk operations: `put_all`, `get_all`, `remove_all`.
/// Verifies that:
/// - `get_all` returns only entries for keys that exist (absent keys omitted).
/// - `remove_all` removes the specified subset, leaving the rest intact.
/// - `get_size` reflects the correct entry count after each mutation.
#[tokio::test]
async fn functional_cache_batch_put_get() {
    const NAME: &str = "FC_BATCH";
    let c = client();
    let _ = c.destroy_cache(NAME).await;
    let cache = c
        .get_or_create_cache(NAME)
        .await
        .expect("get_or_create_cache failed");

    // put_all — 10 entries (key=i, value=i*10).
    let entries: Vec<(IgniteValue, IgniteValue)> = (1i32..=10)
        .map(|i| (IgniteValue::Int(i), IgniteValue::Long(i as i64 * 10)))
        .collect();
    cache.put_all(entries).await.expect("put_all failed");

    // get_all — all 10 keys present.
    let keys: Vec<IgniteValue> = (1i32..=10).map(IgniteValue::Int).collect();
    let mut got = cache.get_all(keys).await.expect("get_all failed");
    assert_eq!(got.len(), 10, "get_all should return all 10 entries");
    got.sort_by_key(|(k, _)| match k {
        IgniteValue::Int(n) => *n,
        _ => 0,
    });
    for (i, (k, v)) in got.iter().enumerate() {
        let expected_i = i as i32 + 1;
        assert_eq!(*k, IgniteValue::Int(expected_i), "get_all key {i}");
        assert_eq!(
            as_long(v, &format!("get_all value {i}")),
            expected_i as i64 * 10
        );
    }

    // get_all with a mix of present and absent keys — absent keys are omitted.
    let mixed = vec![IgniteValue::Int(3), IgniteValue::Int(99)];
    let partial = cache.get_all(mixed).await.expect("get_all mixed failed");
    assert_eq!(
        partial.len(),
        1,
        "absent key should be omitted from get_all result"
    );
    assert_eq!(partial[0].0, IgniteValue::Int(3));
    assert_eq!(as_long(&partial[0].1, "partial value"), 30);

    // remove_all subset (keys 1–3).
    let remove_keys: Vec<IgniteValue> = (1i32..=3).map(IgniteValue::Int).collect();
    cache
        .remove_all(remove_keys)
        .await
        .expect("remove_all failed");

    // Keys 1–3 must be gone; 4–10 must remain.
    for i in 1i32..=3 {
        assert_eq!(
            cache
                .get(IgniteValue::Int(i))
                .await
                .expect("get after remove_all failed"),
            IgniteValue::Null,
            "key {i} should be absent after remove_all"
        );
    }
    for i in 4i32..=10 {
        assert_ne!(
            cache
                .get(IgniteValue::Int(i))
                .await
                .expect("get remaining failed"),
            IgniteValue::Null,
            "key {i} should still be present"
        );
    }

    let size = cache.get_size().await.expect("get_size failed");
    assert_eq!(size, 7, "7 entries should remain after removing 3 of 10");

    c.destroy_cache(NAME).await.expect("destroy failed");
}

// ─── 5. testRemoveReplace ────────────────────────────────────────────────────

/// Ports `testRemoveReplace`.
///
/// Tests `replace`, `remove`, and `remove_all` with a 100-entry dataset.
///
/// **Adapted:** the Java original also tests conditional replace
/// (`replace(key, oldVal, newVal)`) and conditional remove
/// (`remove(key, expectedVal)`).  These overloads do not exist in the Rust
/// cache API, so only the unconditional variants are tested here.
#[tokio::test]
async fn functional_cache_remove_replace() {
    const NAME: &str = "FC_REMOVE_REPLACE";
    let c = client();
    let _ = c.destroy_cache(NAME).await;
    let cache = c
        .get_or_create_cache(NAME)
        .await
        .expect("get_or_create_cache failed");

    // Bulk-insert 100 entries: key=i (Int), value=i as string.
    let entries: Vec<(IgniteValue, IgniteValue)> = (1i32..=100)
        .map(|i| (IgniteValue::Int(i), IgniteValue::String(i.to_string())))
        .collect();
    cache.put_all(entries).await.expect("put_all 100 failed");

    // replace on an existing key → true; value is updated.
    assert!(
        cache
            .replace(IgniteValue::Int(1), IgniteValue::String("one".into()))
            .await
            .expect("replace existing failed"),
        "replace on existing key must return true"
    );
    assert_eq!(
        cache
            .get(IgniteValue::Int(1))
            .await
            .expect("get after replace failed"),
        IgniteValue::String("one".into()),
        "replace must update the stored value"
    );

    // replace on an absent key → false; no entry is created.
    assert!(
        !cache
            .replace(IgniteValue::Int(101), IgniteValue::String("101".into()))
            .await
            .expect("replace absent failed"),
        "replace on absent key must return false"
    );
    assert_eq!(
        cache
            .get(IgniteValue::Int(101))
            .await
            .expect("get absent after replace failed"),
        IgniteValue::Null,
        "replace must not create a new entry"
    );

    // remove an existing key; verify it disappears.
    cache
        .remove(IgniteValue::Int(100))
        .await
        .expect("remove 100 failed");
    assert_eq!(
        cache
            .get(IgniteValue::Int(100))
            .await
            .expect("get after remove failed"),
        IgniteValue::Null,
        "key 100 must be absent after remove"
    );

    // Add an extra key (101) that was never in the original 100.
    cache
        .put(IgniteValue::Int(101), IgniteValue::String("101".into()))
        .await
        .expect("put 101 failed");

    // remove_all the original 1–99 keys (100 was already removed).
    let original_keys: Vec<IgniteValue> = (1i32..=99).map(IgniteValue::Int).collect();
    cache
        .remove_all(original_keys)
        .await
        .expect("remove_all original keys failed");

    // Only key 101 should remain.
    let size = cache
        .get_size()
        .await
        .expect("get_size after remove_all failed");
    assert_eq!(size, 1, "only key 101 should remain");
    assert_eq!(
        as_str(
            &cache
                .get(IgniteValue::Int(101))
                .await
                .expect("get 101 failed"),
            "get 101"
        ),
        "101",
        "key 101 must still be present with its value"
    );

    c.destroy_cache(NAME).await.expect("destroy failed");
}
