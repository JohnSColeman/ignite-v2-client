//! Transaction tests — Rust port of selected tests from:
//! - `BlockingTxOpsTest.java` (thin client internal tests)
//!   <https://github.com/apache/ignite/blob/master/modules/core/src/test/java/org/apache/ignite/internal/client/thin/BlockingTxOpsTest.java>
//!
//! **Java → Rust mapping**
//!
//! | Java test | Rust test |
//! |-----------|-----------|
//! | `testTransactionalConsistency` | `tx_sum_invariant` |
//! | `testBlockingOps` | `tx_all_cache_ops_inside_tx` |
//!
//! **Omitted:**
//! - `testCommitFutureChaining` — tests async commit-future chaining specific to
//!   Java's `putAsync` / `IgniteClientFuture`; Rust's async model has no analogue.
//! - The `@Parameterized` matrix (PESSIMISTIC/REPEATABLE_READ ×
//!   OPTIMISTIC/SERIALIZABLE) is represented by individual explicit variants
//!   in `tx_sum_invariant_optimistic`.
//!
//! Prerequisites: Ignite running on localhost:10800 with no authentication.
//! Run: cargo test --test transaction -- --nocapture --test-threads=1

use std::sync::Arc;

use ignite_client::{IgniteClient, IgniteClientConfig, IgniteValue, TxConcurrency, TxIsolation};

const ADDR: &str = "localhost:10800";
const TX_CACHE: &str = "TX_TEST_CACHE";
const TX_INVARIANT_CACHE: &str = "TX_INVARIANT_CACHE";

fn client() -> IgniteClient {
    IgniteClient::new(IgniteClientConfig::new(ADDR))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

fn iv_int(n: i32) -> IgniteValue {
    IgniteValue::Int(n)
}

fn as_int(v: &IgniteValue, ctx: &str) -> i32 {
    match v {
        IgniteValue::Int(n) => *n,
        other => panic!("{ctx}: expected Int, got {other:?}"),
    }
}

// ─── 1. testTransactionalConsistency ─────────────────────────────────────────

/// Ports `testTransactionalConsistency` (Pessimistic / ReadCommitted variant).
///
/// Spawns [`TASKS`] concurrent async tasks, each executing [`ITERS`]
/// iterations of a random transfer between two distinct keys in
/// [`KEY_COUNT`]-key space.  Each transfer either commits or rolls back
/// randomly.  The sum of all values across all keys must remain 0 throughout.
///
/// **Deadlock avoidance** (mirrors Java original): keys are always read and
/// written in ascending order within each transaction so that two concurrent
/// transactions never hold locks on the same keys in opposite order.
///
/// Java reference: `BlockingTxOpsTest.testTransactionalConsistency`
/// with `PESSIMISTIC / REPEATABLE_READ` parameters.
#[tokio::test]
async fn tx_sum_invariant() {
    run_sum_invariant(TxConcurrency::Pessimistic, TxIsolation::RepeatableRead).await;
}

/// Same invariant test using Optimistic / Serializable concurrency — the other
/// parameter set from the Java `@Parameterized` matrix.
#[tokio::test]
async fn tx_sum_invariant_optimistic() {
    run_sum_invariant(TxConcurrency::Optimistic, TxIsolation::Serializable).await;
}

async fn run_sum_invariant(concurrency: TxConcurrency, isolation: TxIsolation) {
    const KEY_COUNT: i32 = 10;
    const TASKS: usize = 5;
    const ITERS: usize = 100;

    let c = client();
    let _ = c.destroy_cache(TX_INVARIANT_CACHE).await;
    c.get_or_create_transactional_cache(TX_INVARIANT_CACHE)
        .await
        .expect("create transactional cache failed");

    // Initialise all keys to 0.
    let init_entries: Vec<(IgniteValue, IgniteValue)> =
        (0..KEY_COUNT).map(|k| (iv_int(k), iv_int(0))).collect();
    c.cache(TX_INVARIANT_CACHE)
        .put_all(init_entries)
        .await
        .expect("init put_all failed");

    let client = Arc::new(c);

    // Spawn TASKS concurrent tasks.  Each acquires its own transactions via
    // the shared client (Clone is cheap — internally Arc-backed).
    let mut handles = Vec::with_capacity(TASKS);
    for task_id in 0..TASKS {
        let client = Arc::clone(&client);
        let handle = tokio::spawn(async move {
            // Simple deterministic seed per task so results are reproducible.
            let mut seed: u64 = 0xdeadbeef ^ (task_id as u64 * 0x9e3779b9);
            let mut next = move || {
                // xorshift64 — fast, no external crates needed.
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed
            };

            for _ in 0..ITERS {
                let key1 = (next() % KEY_COUNT as u64) as i32;
                let mut key2 = (next() % KEY_COUNT as u64) as i32;
                if key2 == key1 {
                    key2 = (key1 + 1) % KEY_COUNT;
                }
                let amount = (next() % 100) as i32;
                let do_commit = next() % 2 == 0;

                // Always access in ascending key order to prevent deadlocks.
                let (lo, hi) = if key1 < key2 {
                    (key1, key2)
                } else {
                    (key2, key1)
                };
                // Transfer: lo -= amount, hi += amount  (or vice versa — direction
                // doesn't matter as long as both sides are updated together).

                // Retry loop for OPTIMISTIC conflicts ("Failed to prepare transaction").
                'retry: loop {
                    let tx = client
                        .begin_transaction_with(concurrency, isolation, 5_000)
                        .await
                        .expect("begin_transaction failed");

                    let tx_cache = tx.cache(TX_INVARIANT_CACHE);

                    let lo_val = match tx_cache.get(iv_int(lo)).await {
                        Ok(v) => as_int(&v, "lo get"),
                        Err(e) => {
                            let _ = tx.rollback().await;
                            if e.to_string().contains("Failed to prepare transaction") {
                                continue 'retry;
                            }
                            panic!("lo get error: {e}");
                        }
                    };
                    let hi_val = match tx_cache.get(iv_int(hi)).await {
                        Ok(v) => as_int(&v, "hi get"),
                        Err(e) => {
                            let _ = tx.rollback().await;
                            if e.to_string().contains("Failed to prepare transaction") {
                                continue 'retry;
                            }
                            panic!("hi get error: {e}");
                        }
                    };

                    if let Err(e) = tx_cache.put(iv_int(lo), iv_int(lo_val - amount)).await {
                        let _ = tx.rollback().await;
                        if e.to_string().contains("Failed to prepare transaction") {
                            continue 'retry;
                        }
                        panic!("lo put error: {e}");
                    }
                    if let Err(e) = tx_cache.put(iv_int(hi), iv_int(hi_val + amount)).await {
                        let _ = tx.rollback().await;
                        if e.to_string().contains("Failed to prepare transaction") {
                            continue 'retry;
                        }
                        panic!("hi put error: {e}");
                    }

                    if do_commit {
                        match tx.commit().await {
                            Ok(()) => break 'retry,
                            Err(e) if e.to_string().contains("Failed to prepare transaction") => {
                                continue 'retry;
                            }
                            Err(e) => panic!("commit error: {e}"),
                        }
                    } else {
                        tx.rollback().await.expect("rollback failed");
                        break 'retry;
                    }
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.expect("task panicked");
    }

    // Sum across all keys must be 0: every committed transfer moved the same
    // `amount` from one key to another; rollbacks are no-ops.
    let cache = client.cache(TX_INVARIANT_CACHE);
    let mut total = 0i64;
    for k in 0..KEY_COUNT {
        let v = cache.get(iv_int(k)).await.expect("final get failed");
        total += as_int(&v, "final sum") as i64;
    }
    assert_eq!(
        total, 0,
        "sum of all values must remain 0 after concurrent transfers"
    );

    client
        .destroy_cache(TX_INVARIANT_CACHE)
        .await
        .expect("destroy failed");
}

// ─── 2. testBlockingOps ──────────────────────────────────────────────────────

/// Ports `testBlockingOps`.
///
/// Verifies that **every** cache operation functions correctly when executed
/// inside an explicit transaction.  The Java test runs these operations
/// concurrently across multiple threads (mixing implicit and explicit
/// transactions); this Rust port focuses on correctness of each operation
/// within a single transaction, which is the observable semantic tested.
///
/// Operations covered (mirrors Java's `checkOpMultithreaded` invocations):
/// `put`, `get`, `contains_key`, `put_all`, `get_all`, `put_if_absent`,
/// `replace`, `get_and_put`, `get_and_remove`, `get_and_replace`,
/// `remove`, `remove_all`.
///
/// Java reference: `BlockingTxOpsTest.testBlockingOps`
#[tokio::test]
async fn tx_all_cache_ops_inside_tx() {
    let c = client();
    let _ = c.destroy_cache(TX_CACHE).await;
    c.get_or_create_transactional_cache(TX_CACHE)
        .await
        .expect("create transactional cache failed");

    // ── put + get ─────────────────────────────────────────────────────────────
    {
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        cache
            .put(iv_int(0), iv_int(0))
            .await
            .expect("tx put failed");
        let v = cache.get(iv_int(0)).await.expect("tx get failed");
        assert_eq!(as_int(&v, "tx get"), 0, "within-tx read must see own write");
        tx.commit().await.expect("commit failed");

        assert_eq!(
            as_int(
                &c.cache(TX_CACHE)
                    .get(iv_int(0))
                    .await
                    .expect("post-commit get"),
                "post-commit"
            ),
            0,
            "committed value must be visible outside tx"
        );
    }

    // ── contains_key ─────────────────────────────────────────────────────────
    {
        // key 0 already exists from previous block.
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        assert!(
            cache
                .contains_key(iv_int(0))
                .await
                .expect("tx contains_key failed"),
            "contains_key should see committed data"
        );
        // Key written inside the tx is also visible within the same tx.
        cache
            .put(iv_int(10), iv_int(10))
            .await
            .expect("tx put 10 failed");
        assert!(
            cache
                .contains_key(iv_int(10))
                .await
                .expect("tx contains_key own-write failed"),
            "contains_key should see own write within tx"
        );
        tx.commit().await.expect("commit failed");
    }

    // ── put_all + get_all ─────────────────────────────────────────────────────
    {
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        let entries: Vec<(IgniteValue, IgniteValue)> =
            (1i32..=3).map(|i| (iv_int(i), iv_int(i * 100))).collect();
        cache.put_all(entries).await.expect("tx put_all failed");

        let keys: Vec<IgniteValue> = (1i32..=3).map(iv_int).collect();
        let mut got = cache.get_all(keys).await.expect("tx get_all failed");
        assert_eq!(got.len(), 3, "tx get_all must return 3 entries");
        got.sort_by_key(|(k, _)| match k {
            IgniteValue::Int(n) => *n,
            _ => 0,
        });
        for (i, (_, v)) in got.iter().enumerate() {
            let expected = (i as i32 + 1) * 100;
            assert_eq!(as_int(v, "tx get_all value"), expected);
        }
        tx.commit().await.expect("commit failed");
    }

    // ── put_if_absent ─────────────────────────────────────────────────────────
    {
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        // Key 99 does not exist yet.
        let inserted = cache
            .put_if_absent(iv_int(99), iv_int(99))
            .await
            .expect("tx put_if_absent new failed");
        assert!(inserted, "put_if_absent on absent key should return true");
        // Second attempt on the same key within the same tx.
        let not_inserted = cache
            .put_if_absent(iv_int(99), iv_int(999))
            .await
            .expect("tx put_if_absent existing failed");
        assert!(
            !not_inserted,
            "put_if_absent on existing key should return false"
        );
        assert_eq!(
            as_int(
                &cache.get(iv_int(99)).await.expect("tx get 99"),
                "put_if_absent check"
            ),
            99,
            "value must not be overwritten by second put_if_absent"
        );
        tx.commit().await.expect("commit failed");
    }

    // ── replace ──────────────────────────────────────────────────────────────
    {
        // key 0 = 0 from first block.
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        let replaced = cache
            .replace(iv_int(0), iv_int(999))
            .await
            .expect("tx replace existing failed");
        assert!(replaced, "replace on existing key must return true");
        assert_eq!(
            as_int(
                &cache.get(iv_int(0)).await.expect("tx get after replace"),
                "replace check"
            ),
            999
        );
        // replace on absent key.
        let not_replaced = cache
            .replace(iv_int(1000), iv_int(1))
            .await
            .expect("tx replace absent failed");
        assert!(!not_replaced, "replace on absent key must return false");
        tx.commit().await.expect("commit failed");
    }

    // ── get_and_put ──────────────────────────────────────────────────────────
    {
        // key 0 = 999 from previous block.
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        let old = cache
            .get_and_put(iv_int(0), iv_int(0))
            .await
            .expect("tx get_and_put failed");
        assert_eq!(
            as_int(&old, "get_and_put old"),
            999,
            "get_and_put must return old value"
        );
        tx.commit().await.expect("commit failed");
    }

    // ── get_and_remove ────────────────────────────────────────────────────────
    {
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        // key 0 = 0 from previous block.
        let removed = cache
            .get_and_remove(iv_int(0))
            .await
            .expect("tx get_and_remove failed");
        assert_eq!(as_int(&removed, "get_and_remove value"), 0);
        // Key must be absent within the same tx after get_and_remove.
        assert_eq!(
            cache
                .get(iv_int(0))
                .await
                .expect("tx get after get_and_remove"),
            IgniteValue::Null,
            "key must be absent within tx after get_and_remove"
        );
        tx.commit().await.expect("commit failed");
    }

    // ── get_and_replace ───────────────────────────────────────────────────────
    {
        // key 10 = 10 from contains_key block.
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        let old = cache
            .get_and_replace(iv_int(10), iv_int(10_000))
            .await
            .expect("tx get_and_replace failed");
        assert_eq!(as_int(&old, "get_and_replace old"), 10);
        assert_eq!(
            as_int(
                &cache
                    .get(iv_int(10))
                    .await
                    .expect("tx get after get_and_replace"),
                "get_and_replace new"
            ),
            10_000
        );
        // get_and_replace on absent key.
        let null = cache
            .get_and_replace(iv_int(5000), iv_int(1))
            .await
            .expect("tx get_and_replace absent failed");
        assert_eq!(
            null,
            IgniteValue::Null,
            "get_and_replace on absent key must return Null"
        );
        tx.commit().await.expect("commit failed");
    }

    // ── remove + remove_all ───────────────────────────────────────────────────
    {
        let tx = c
            .begin_transaction()
            .await
            .expect("begin_transaction failed");
        let cache = tx.cache(TX_CACHE);
        // remove key 99 (from put_if_absent block).
        cache.remove(iv_int(99)).await.expect("tx remove failed");
        assert_eq!(
            cache.get(iv_int(99)).await.expect("tx get after remove"),
            IgniteValue::Null,
            "key must be absent within tx after remove"
        );
        // remove_all keys 1–3 (from put_all block).
        let bulk_keys: Vec<IgniteValue> = (1i32..=3).map(iv_int).collect();
        cache
            .remove_all(bulk_keys)
            .await
            .expect("tx remove_all failed");
        for k in 1i32..=3 {
            assert_eq!(
                cache.get(iv_int(k)).await.expect("tx get after remove_all"),
                IgniteValue::Null,
                "key {k} must be absent within tx after remove_all"
            );
        }
        tx.commit().await.expect("commit failed");
    }

    c.destroy_cache(TX_CACHE).await.expect("destroy failed");
}
