use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures::{SinkExt, StreamExt};
use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, warn};

use crate::protocol::handshake::HandshakeRequest;
use crate::protocol::messages::read_response_header;

use super::error::{Result, TransportError};

// ─── Request ID generator ─────────────────────────────────────────────────────

static NEXT_REQUEST_ID: AtomicI64 = AtomicI64::new(1);

pub fn next_request_id() -> i64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

// ─── Pending map type ─────────────────────────────────────────────────────────

type PendingMap = Arc<Mutex<HashMap<i64, oneshot::Sender<std::result::Result<Bytes, TransportError>>>>>;

// ─── Writer / Reader type aliases ─────────────────────────────────────────────

/// Type-erased async write half — works for both plain TCP and TLS streams.
type AnyWrite = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;
/// Type-erased async read half — works for both plain TCP and TLS streams.
type AnyRead  = Box<dyn tokio::io::AsyncRead  + Unpin + Send>;

type Writer = FramedWrite<AnyWrite, LengthDelimitedCodec>;

// ─── AbortOnDrop ──────────────────────────────────────────────────────────────

/// Wraps a `tokio::task::AbortHandle` and calls `abort()` when dropped.
///
/// Stored as `Arc<AbortOnDrop>` inside `IgniteConnection`.  When the last
/// connection clone drops, the `Arc` drops, which drops `AbortOnDrop`, which
/// aborts the reader task.  This is important for TLS connections: dropping the
/// write half sends a TCP FIN but may not be enough to wake the reader task.
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// ─── IgniteConnection ─────────────────────────────────────────────────────────

/// A single async TCP (or TLS) connection to an Ignite node.
///
/// Multiple requests can be in-flight concurrently: each gets a unique
/// `request_id`; the background reader task routes responses via oneshot
/// channels stored in `pending`.
///
/// Uses type-erased stream halves so that both plain TCP and TLS are handled
/// through the same code path.  For plain TCP the underlying `OwnedWriteHalf`
/// is boxed, preserving its `Drop → TCP FIN` behaviour.  For TLS both halves
/// come from `tokio::io::split`.  In either case the reader task is forcibly
/// aborted by `AbortOnDrop` when the last connection clone drops.
#[derive(Clone)]
pub struct IgniteConnection {
    /// Write half — dropping the inner `OwnedWriteHalf` (for plain TCP) sends TCP FIN.
    writer: Arc<Mutex<Writer>>,
    /// In-flight request correlations.
    pending: PendingMap,
    /// Set to false by the reader task when the TCP connection closes.
    /// Checked inside the pending map lock so `send_and_receive` never inserts
    /// a oneshot sender that nobody will ever resolve.
    alive: Arc<AtomicBool>,
    /// Optional per-request response deadline applied in `send_and_receive`.
    /// `None` disables the timeout (useful in tests).
    request_timeout: Option<Duration>,
    /// Aborts the background reader task when the last connection clone drops.
    /// Only stored for its `Drop` side effect; never read directly.
    #[allow(dead_code)]
    reader_abort: Arc<AbortOnDrop>,
}

// ─── Construction ─────────────────────────────────────────────────────────────

impl IgniteConnection {
    /// Connect to `addr`, perform handshake, spawn reader task.
    ///
    /// * `connect_timeout` — maximum time allowed for TCP connect + handshake.
    ///   Pass `None` to disable the deadline (e.g. in unit tests).
    /// * `request_timeout` — per-request response deadline stored on the
    ///   returned connection and applied in every `send_and_receive` call.
    ///   Pass `None` to disable the deadline.
    /// * `tls_config` — optional TLS client configuration.  When `Some`, the
    ///   connection is wrapped in TLS after TCP connect and keepalive setup.
    ///   When `None` (default) a plain unencrypted connection is used.
    pub async fn connect(
        addr: &str,
        handshake: HandshakeRequest,
        connect_timeout: Option<Duration>,
        request_timeout: Option<Duration>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self> {
        let fut = Self::connect_inner(addr, handshake, request_timeout, tls_config);
        match connect_timeout {
            Some(dur) => tokio::time::timeout(dur, fut)
                .await
                .map_err(|_| TransportError::Timeout)?,
            None => fut.await,
        }
    }

    /// Internal connect logic extracted so `connect` can wrap it with an optional deadline.
    async fn connect_inner(
        addr: &str,
        handshake: HandshakeRequest,
        request_timeout: Option<Duration>,
        tls_config: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        // ── TCP keepalive ──────────────────────────────────────────────────────
        // Ask the OS to send keepalive probes after 60 s of inactivity, every
        // 15 s thereafter, so dead peers are detected without needing
        // application-level heartbeats.
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(15));
        SockRef::from(&stream).set_tcp_keepalive(&keepalive)?;

        // ── Build codec ───────────────────────────────────────────────────────
        // LengthDelimitedCodec: 4-byte LE length prefix that excludes itself;
        // num_skip(4) strips it on the read side.
        let build_codec = || {
            LengthDelimitedCodec::builder()
                .length_field_length(4)
                .length_field_offset(0)
                .length_adjustment(0)
                .num_skip(4)
                .little_endian()
                .new_codec()
        };

        // ── Split into type-erased halves ─────────────────────────────────────
        // For plain TCP we box the `OwnedWriteHalf`/`OwnedReadHalf` returned by
        // `into_split()` so that Drop on the write half still sends a TCP FIN.
        // For TLS we perform the TLS handshake first, then box the halves from
        // `tokio::io::split`.  In both cases the types are erased to
        // `Box<dyn AsyncWrite + Unpin + Send>` and `Box<dyn AsyncRead + Unpin + Send>`
        // so the rest of this function is transport-agnostic.
        let (any_read, any_write): (AnyRead, AnyWrite) = if let Some(cfg) = tls_config {
            // Extract hostname from "host:port" for TLS SNI.
            let host = addr.split(':').next().unwrap_or(addr);
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| TransportError::Tls(e.to_string()))?;
            let connector = tokio_rustls::TlsConnector::from(cfg);
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|e| TransportError::Tls(e.to_string()))?;
            let (r, w) = tokio::io::split(tls_stream);
            (Box::new(r), Box::new(w))
        } else {
            let (r, w) = stream.into_split();
            (Box::new(r), Box::new(w))
        };

        let mut framed_write = FramedWrite::new(any_write, build_codec());
        let mut framed_read  = FramedRead::new(any_read,  build_codec());

        // ── Handshake ─────────────────────────────────────────────────────────
        let hs_payload = handshake.encode();
        framed_write.send(hs_payload).await?;

        let hs_frame = framed_read
            .next()
            .await
            .ok_or_else(|| TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed during handshake",
            )))??;

        let mut hs_bytes = hs_frame.freeze();
        crate::protocol::handshake::HandshakeResponse::decode(&mut hs_bytes)
            .map_err(TransportError::Protocol)?;

        debug!("Ignite handshake successful");

        // ── Shared state ──────────────────────────────────────────────────────
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let pending_reader = Arc::clone(&pending);
        let writer = Arc::new(Mutex::new(framed_write));
        let alive = Arc::new(AtomicBool::new(true));
        let alive_reader = Arc::clone(&alive);

        // ── Reader task ───────────────────────────────────────────────────────
        let handle = tokio::spawn(async move {
            loop {
                match framed_read.next().await {
                    None => {
                        // Connection closed — mark dead and notify all pending waiters.
                        // alive flag is set INSIDE the map lock so that send_and_receive,
                        // which also checks alive inside the map lock, cannot insert a
                        // new oneshot sender after we drain (which would hang forever).
                        let mut map = pending_reader.lock().await;
                        alive_reader.store(false, Ordering::Release);
                        for (req_id, tx) in map.drain() {
                            let _ = tx.send(Err(TransportError::ConnectionClosed(req_id)));
                        }
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("transport read error: {e}");
                        let mut map = pending_reader.lock().await;
                        alive_reader.store(false, Ordering::Release);
                        for (req_id, tx) in map.drain() {
                            let _ = tx.send(Err(TransportError::ConnectionClosed(req_id)));
                        }
                        break;
                    }
                    Some(Ok(frame)) => {
                        let bytes = frame.freeze();
                        // Minimum valid response: i64 req_id + i16 flags = 10 bytes.
                        // (old guard of 12 was wrong for protocol 1.7 body-less replies)
                        if bytes.remaining() < 10 {
                            warn!("received undersized frame ({} bytes), skipping", bytes.remaining());
                            continue;
                        }
                        // Peek request_id (first 8 bytes of response payload).
                        // Safety: the 10-byte size guard above guarantees bytes[0..8] exists.
                        #[allow(clippy::expect_used)]
                        let req_id = i64::from_le_bytes(
                            bytes[0..8].try_into()
                                .expect("slice is 8 bytes: guaranteed by 10-byte size guard above"),
                        );
                        let mut map = pending_reader.lock().await;
                        if let Some(tx) = map.remove(&req_id) {
                            let _ = tx.send(Ok(bytes));
                        } else {
                            warn!("received response for unknown request_id={req_id}");
                        }
                    }
                }
            }
        });

        // Capture the abort handle before dropping `handle`.  When the last
        // `IgniteConnection` clone drops, `Arc<AbortOnDrop>` drops, which calls
        // `abort()` — preventing a lingering zombie reader task.
        let reader_abort = Arc::new(AbortOnDrop(handle.abort_handle()));

        Ok(Self { writer, pending, alive, request_timeout, reader_abort })
    }
}

// IgniteConnection::clone is derived above.
// All fields are Arc<_> or Copy, so the derived impl is a shallow clone — the new
// handle shares the same underlying TCP connection (writer, pending map, and alive
// flag).  Concurrent in-flight requests from different handles are safe.

impl fmt::Debug for IgniteConnection {
    /// Shows only whether the connection is currently alive.
    /// Internal framing state is not meaningful to expose.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IgniteConnection")
            .field("alive", &self.alive.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// ─── Request / Response ───────────────────────────────────────────────────────

impl IgniteConnection {
    /// Send an already-encoded request payload and wait for the response.
    ///
    /// The caller is responsible for including the standard request header
    /// (op_code + request_id) at the start of `payload`.
    ///
    /// Returns the response payload **including** the response header bytes
    /// (request_id + status + optional error).  Callers should call
    /// `read_response_header()` to strip and validate the header.
    ///
    /// If `self.request_timeout` is set and the server does not respond within
    /// that deadline, the pending map entry is cleaned up and
    /// `TransportError::Timeout` is returned.
    pub async fn send_and_receive(&self, request_id: i64, payload: Bytes) -> Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        {
            // Check alive and insert under the same lock the reader uses when it
            // drains the map.  This prevents the race where the reader exits with
            // an empty map (alive → false) and we then insert a sender that nobody
            // will ever resolve, causing an infinite hang on rx.await below.
            let mut map = self.pending.lock().await;
            if !self.alive.load(Ordering::Acquire) {
                return Err(TransportError::ConnectionClosed(request_id));
            }
            map.insert(request_id, tx);
        }

        {
            let mut writer = self.writer.lock().await;
            if let Err(e) = writer.send(payload).await {
                // Clean up the pending entry before propagating the write error so
                // the orphaned oneshot sender does not accumulate as a zombie entry.
                self.pending.lock().await.remove(&request_id);
                return Err(TransportError::Io(e));
            }
        }

        if let Some(dur) = self.request_timeout {
            match tokio::time::timeout(dur, rx).await {
                // inner: Result<Bytes, TransportError> — returned directly.
                Ok(Ok(inner)) => inner,
                // oneshot sender was dropped before sending (connection died).
                Ok(Err(_)) => Err(TransportError::ConnectionClosed(request_id)),
                Err(_elapsed) => {
                    // Timeout: remove the stale pending slot so the entry doesn't
                    // linger if the server eventually sends a late response.
                    let mut map = self.pending.lock().await;
                    map.remove(&request_id);
                    Err(TransportError::Timeout)
                }
            }
        } else {
            rx.await.map_err(|_| TransportError::ConnectionClosed(request_id))?
        }
    }

    /// Send `payload` and return the response payload **after** stripping and
    /// validating the response header.  Returns `Err` on server error or
    /// transport failure.
    pub async fn request(&self, request_id: i64, payload: Bytes) -> Result<Bytes> {
        let mut response = self.send_and_receive(request_id, payload).await?;
        let _req_id = read_response_header(&mut response).map_err(TransportError::Protocol)?;
        Ok(response)
    }

    /// Health-check: returns false once the reader task has observed a TCP close
    /// or read error.  Used by the connection pool's `recycle()` to discard dead
    /// connections before they can hang future callers.
    ///
    /// Synchronous — only reads an atomic flag; no I/O is performed.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

// ─── Framing note ─────────────────────────────────────────────────────────────
//
// The Ignite 2.x binary protocol frames messages as:
//   [i32 LE: payload_length] [payload_length bytes: payload]
//
// The payload length does NOT include the 4-byte length prefix itself.
//
// `LengthDelimitedCodec` with `.num_skip(4)` strips the length prefix from
// frames before delivering them, matching this layout exactly.
//
// For plain TCP, `TcpStream::into_split()` (OwnedWriteHalf / OwnedReadHalf)
// is used so that dropping the write half sends a TCP FIN.  For TLS,
// `tokio::io::split` is used instead (TlsStream is not splittable with
// into_split), and the reader task is cancelled via `AbortOnDrop` when the
// last `IgniteConnection` clone drops.
