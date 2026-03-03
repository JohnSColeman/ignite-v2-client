use std::sync::Arc;

use bytes::Bytes;
use crate::protocol::codec::read_bool;
use crate::protocol::messages::{
    decode_cache_get_all_response, decode_cache_get_size_response, decode_cache_value_response,
    encode_cache_create_with_name, encode_cache_destroy_req, encode_cache_get_size,
    encode_cache_key_req, encode_cache_kv_req, encode_cache_multi_key_req,
    encode_cache_multi_kv_req,
};
use crate::protocol::op_code;
use crate::protocol::IgniteValue;
use crate::transport::{next_request_id, IgniteConnection};

use crate::error::{IgniteError, Result};
use crate::pool::Pool;

// ─── CacheSource ─────────────────────────────────────────────────────────────

/// Backing connection source for an [`IgniteCache`] handle.
///
/// - `Pool` — non-transactional: a connection is checked out from the pool for
///   each operation and returned immediately.
/// - `Tx` — transactional: every operation goes through the transaction's
///   dedicated connection and embeds the `tx_id` in the cache-header flags byte.
#[derive(Clone, Debug)]
pub(crate) enum CacheSource {
    Pool(Pool),
    Tx { tx_id: i32, conn: Arc<IgniteConnection> },
}

// ─── IgniteCache ─────────────────────────────────────────────────────────────

/// A handle to an Ignite cache, obtained via [`crate::IgniteClient::cache`],
/// [`crate::IgniteClient::get_or_create_cache`], or [`crate::transaction::Transaction::cache`].
///
/// Cheap to clone — internally holds an `i32` cache-id and either a pool
/// reference (Arc-backed) or a transaction connection (Arc-backed).
#[derive(Clone, Debug)]
pub struct IgniteCache {
    pub(crate) cache_id: i32,
    source: CacheSource,
}

impl IgniteCache {
    /// Create a non-transactional cache handle backed by the connection pool.
    pub(crate) fn new(cache_id: i32, pool: Pool) -> Self {
        Self { cache_id, source: CacheSource::Pool(pool) }
    }

    /// Create a transactional cache handle that routes all ops through a
    /// transaction's dedicated connection and embeds `tx_id` in every request.
    pub(crate) fn new_tx(cache_id: i32, tx_id: i32, conn: Arc<IgniteConnection>) -> Self {
        Self { cache_id, source: CacheSource::Tx { tx_id, conn } }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Returns the active transaction ID, or `None` for non-transactional handles.
    fn tx_id(&self) -> Option<i32> {
        match &self.source {
            CacheSource::Pool(_) => None,
            CacheSource::Tx { tx_id, .. } => Some(*tx_id),
        }
    }

    /// Obtain a connection (pool checkout or tx connection), send `payload`,
    /// and return the post-header response bytes.
    /// `IgniteConnection::request` already strips and validates the response
    /// header, so the returned `Bytes` contains only the operation payload.
    async fn send(&self, req_id: i64, payload: Bytes) -> Result<Bytes> {
        match &self.source {
            CacheSource::Pool(pool) => {
                let conn = pool.get().await?;
                conn.request(req_id, payload).await.map_err(IgniteError::Transport)
            }
            CacheSource::Tx { conn, .. } => {
                conn.request(req_id, payload).await.map_err(IgniteError::Transport)
            }
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Retrieve a value by key.  Returns `IgniteValue::Null` if the key is not present.
    pub async fn get(&self, key: IgniteValue) -> Result<IgniteValue> {
        let req_id = next_request_id();
        let payload =
            encode_cache_key_req(op_code::CACHE_GET, req_id, self.cache_id, &key, self.tx_id());
        let mut resp = self.send(req_id, payload).await?;
        decode_cache_value_response(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Store a key-value pair.  Overwrites any existing value.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn put(&self, key: IgniteValue, value: IgniteValue) -> Result<()> {
        let req_id = next_request_id();
        let payload = encode_cache_kv_req(
            op_code::CACHE_PUT,
            req_id,
            self.cache_id,
            &key,
            &value,
            self.tx_id(),
        );
        self.send(req_id, payload).await?;
        Ok(())
    }

    /// Store a key-value pair **only if the key is not already present**.
    /// Returns `true` if the value was stored, `false` if the key already existed.
    pub async fn put_if_absent(&self, key: IgniteValue, value: IgniteValue) -> Result<bool> {
        let req_id = next_request_id();
        let payload = encode_cache_kv_req(
            op_code::CACHE_PUT_IF_ABSENT,
            req_id,
            self.cache_id,
            &key,
            &value,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        read_bool(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Retrieve values for multiple keys.
    /// Returns only the pairs for keys that exist in the cache; absent keys are omitted.
    pub async fn get_all(
        &self,
        keys: Vec<IgniteValue>,
    ) -> Result<Vec<(IgniteValue, IgniteValue)>> {
        let req_id = next_request_id();
        let payload = encode_cache_multi_key_req(
            op_code::CACHE_GET_ALL,
            req_id,
            self.cache_id,
            &keys,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        decode_cache_get_all_response(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Store multiple key-value pairs.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn put_all(&self, entries: Vec<(IgniteValue, IgniteValue)>) -> Result<()> {
        let req_id = next_request_id();
        let payload = encode_cache_multi_kv_req(
            op_code::CACHE_PUT_ALL,
            req_id,
            self.cache_id,
            &entries,
            self.tx_id(),
        );
        self.send(req_id, payload).await?;
        Ok(())
    }

    /// Returns `true` if the cache contains the given key.
    pub async fn contains_key(&self, key: IgniteValue) -> Result<bool> {
        let req_id = next_request_id();
        let payload = encode_cache_key_req(
            op_code::CACHE_CONTAINS_KEY,
            req_id,
            self.cache_id,
            &key,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        read_bool(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Remove a key.
    ///
    /// Implemented via `CACHE_GET_AND_REMOVE` (op 1007) because the standalone
    /// `CACHE_REMOVE_KEY` (op 1019) does not remove only the specified key across
    /// all Ignite 2.x server configurations; the returned previous value is
    /// discarded.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn remove(&self, key: IgniteValue) -> Result<()> {
        self.get_and_remove(key).await?;
        Ok(())
    }

    /// Replace the value only if the key is already present.
    /// Returns `true` if the value was replaced, `false` if the key was absent.
    pub async fn replace(&self, key: IgniteValue, value: IgniteValue) -> Result<bool> {
        let req_id = next_request_id();
        let payload = encode_cache_kv_req(
            op_code::CACHE_REPLACE,
            req_id,
            self.cache_id,
            &key,
            &value,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        read_bool(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Atomically store a new value and return the previous value.
    /// Returns `IgniteValue::Null` if the key was not previously present.
    pub async fn get_and_put(&self, key: IgniteValue, value: IgniteValue) -> Result<IgniteValue> {
        let req_id = next_request_id();
        let payload = encode_cache_kv_req(
            op_code::CACHE_GET_AND_PUT,
            req_id,
            self.cache_id,
            &key,
            &value,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        decode_cache_value_response(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Atomically remove a key and return its previous value.
    /// Returns `IgniteValue::Null` if the key was not present.
    pub async fn get_and_remove(&self, key: IgniteValue) -> Result<IgniteValue> {
        let req_id = next_request_id();
        let payload = encode_cache_key_req(
            op_code::CACHE_GET_AND_REMOVE,
            req_id,
            self.cache_id,
            &key,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        decode_cache_value_response(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Replace the value only if the key is already present, and return the old value.
    /// Returns `IgniteValue::Null` if the key was absent (no change is made).
    pub async fn get_and_replace(
        &self,
        key: IgniteValue,
        value: IgniteValue,
    ) -> Result<IgniteValue> {
        let req_id = next_request_id();
        let payload = encode_cache_kv_req(
            op_code::CACHE_GET_AND_REPLACE,
            req_id,
            self.cache_id,
            &key,
            &value,
            self.tx_id(),
        );
        let mut resp = self.send(req_id, payload).await?;
        decode_cache_value_response(&mut resp).map_err(IgniteError::Protocol)
    }

    /// Remove all specified keys from the cache.
    ///
    /// Implemented as individual [`Self::remove`] calls because the server-side
    /// bulk `CACHE_REMOVE_KEYS` operation does not behave as expected across all
    /// Ignite 2.x versions with the thin-client wire format.
    #[must_use = "futures do nothing unless you `.await` them"]
    pub async fn remove_all(&self, keys: Vec<IgniteValue>) -> Result<()> {
        for key in keys {
            self.remove(key).await?;
        }
        Ok(())
    }

    /// Return the number of entries in the cache.
    pub async fn get_size(&self) -> Result<i64> {
        let req_id = next_request_id();
        let payload =
            encode_cache_get_size(op_code::CACHE_GET_SIZE, req_id, self.cache_id, self.tx_id());
        let mut resp = self.send(req_id, payload).await?;
        decode_cache_get_size_response(&mut resp).map_err(IgniteError::Protocol)
    }
}

// ─── Cache management helpers (used by IgniteClient) ─────────────────────────

/// Send `CACHE_GET_OR_CREATE_WITH_NAME` and return an `IgniteCache` handle.
/// `IgniteConnection::request` strips the response header automatically.
pub(crate) async fn get_or_create_cache_by_name(name: &str, pool: &Pool) -> Result<IgniteCache> {
    let req_id = next_request_id();
    let payload =
        encode_cache_create_with_name(op_code::CACHE_GET_OR_CREATE_WITH_NAME, req_id, name);

    let conn = pool.get().await?;
    // request() strips the response header; for this op the remaining body is empty.
    conn.request(req_id, payload)
        .await
        .map_err(IgniteError::Transport)?;

    let cid = crate::protocol::cache_id(name);
    Ok(IgniteCache::new(cid, pool.clone()))
}

/// Send `CACHE_DESTROY` for the given cache name.
/// `IgniteConnection::request` strips the response header automatically.
pub(crate) async fn destroy_cache_by_name(name: &str, pool: &Pool) -> Result<()> {
    let cid = crate::protocol::cache_id(name);
    let req_id = next_request_id();
    let payload = encode_cache_destroy_req(op_code::CACHE_DESTROY, req_id, cid);

    let conn = pool.get().await?;
    // request() strips the response header; for this op the remaining body is empty.
    conn.request(req_id, payload)
        .await
        .map_err(IgniteError::Transport)?;
    Ok(())
}
