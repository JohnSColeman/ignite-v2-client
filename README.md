# ignite-client

An async Rust thin client for **Apache Ignite 2.x**, implementing the
[Ignite Binary Client Protocol](https://ignite.apache.org/docs/latest/thin-clients/getting-started-with-thin-clients)
over TCP.

> **New in 0.3.0 — Partition awareness / affinity routing.** The client now
> connects to every configured cluster node, computes each key's primary node
> locally, and sends cache operations straight to it — eliminating the
> server-side proxy hop. It is a pure optimization with a fail-safe fallback to
> the default channel, so results never change. See
> [Partition awareness](#partition-awareness).

---

## Table of Contents

- [Features](#features)
- [Protocol Reference](#protocol-reference)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
  - [SELECT query](#select-query)
  - [Per-value type inspection](#per-value-type-inspection)
  - [DML](#dml)
  - [Transaction](#transaction-1)
  - [Transaction helper (auto-commit/rollback)](#transaction-helper-auto-commitrollback)
  - [KV cache](#kv-cache)
  - [Streaming cursor](#streaming-cursor)
  - [TLS](#tls)
  - [Authentication](#authentication)
- [API Reference](#api-reference)
  - [IgniteClientConfig](#igniteclientconfig)
  - [IgniteClient](#igniteclient)
  - [Transaction](#transaction)
  - [IgniteCache](#ignitecache)
  - [QueryResult / Row](#queryresult--row)
  - [Column and ColumnType](#column-and-columntype)
  - [QueryStream](#querystream)
  - [IgniteValue type system](#ignitevalue-type-system)
- [Architecture](#architecture)
  - [Request multiplexing](#request-multiplexing)
  - [Connection pool](#connection-pool)
  - [Transaction connections](#transaction-connections)
  - [Pagination](#pagination)
  - [Partition awareness](#partition-awareness)
- [Codec Details](#codec-details)
- [Comparison with Existing Rust Clients](#comparison-with-existing-rust-clients)
- [Running Tests](#running-tests)
  - [Local 3-node test cluster](#local-3-node-test-cluster)
- [License](#license)

---

## Features

| Capability | Status |
|---|---|
| Protocol 1.7.0 handshake | ✅ |
| Authentication (username/password) | ✅ |
| `OP_QUERY_SQL_FIELDS` (SELECT) | ✅ |
| Automatic cursor pagination | ✅ |
| DML: INSERT / UPDATE / DELETE | ✅ |
| Transactions (`TX_START` / `TX_END`) | ✅ |
| Drop-based rollback | ✅ |
| Async pipelining (multiple in-flight requests per connection) | ✅ |
| deadpool connection pool | ✅ |
| Full wire type coverage (Null, Bool, Byte…Long, Float, Double, String, UUID, Date, Time, Timestamp, Decimal, arrays) | ✅ |
| KV cache API (get, put, get_all, put_all, contains_key, remove, replace, …) | ✅ |
| TLS (`rustls` + native system CA bundle) | ✅ |
| Streaming `QueryStream` cursor | ✅ |
| TCP keepalive | ✅ |
| Client-side request timeouts | ✅ |
| Multi-node connection registry (one pool per node, keyed by node UUID) | ✅ |
| Partition awareness / affinity routing (primary-node routing for KV ops) | ✅ |
| Server endpoint discovery (auto-learn nodes not in the address list) | 🔲 Future |
| Backup-node routing for read-only ops | 🔲 Future |

---

## Protocol Reference

This client implements the **Apache Ignite 2.x Thin Client Binary Protocol**.

| Document | URL |
|---|---|
| Thin Client Overview | https://ignite.apache.org/docs/latest/thin-clients/getting-started-with-thin-clients |
| Binary Client Protocol spec | https://ignite.apache.org/docs/latest/binary-client-protocol/binary-client-protocol |
| Data Format (type codes, wire encoding) | https://ignite.apache.org/docs/latest/binary-client-protocol/data-format |
| SQL and Scan Queries operations | https://ignite.apache.org/docs/latest/binary-client-protocol/sql-and-scan-queries |
| Cache operations | https://ignite.apache.org/docs/latest/binary-client-protocol/key-value-queries |
| Transaction operations | https://ignite.apache.org/docs/latest/binary-client-protocol/transactions |
| Error codes | https://ignite.apache.org/docs/latest/binary-client-protocol/error-codes |

### Protocol version

The handshake negotiates **protocol version 1.7.0** — the highest version
supported by the Apache Ignite 2.x series.  GridGain 8.x (the commercial
fork) uses the same protocol version.

### Port

Ignite thin client port defaults to **10800**.

---

## Project Structure

This is a **single crate** — no workspace members.

```
ignite-client/
├── Cargo.toml              ← package manifest (ignite-v2-client, crate: ignite_client)
├── src/
│   ├── lib.rs              ← public re-exports
│   ├── client.rs           ← IgniteClient: query, execute, begin_transaction, cache, …
│   ├── transaction.rs      ← Transaction: query, execute, commit, rollback, cache, drop
│   ├── cache.rs            ← IgniteCache: get, put, get_all, put_all, remove, … (affinity-routed)
│   ├── affinity.rs         ← partition awareness: key hashing, rendezvous partition/mask,
│   │                          CACHE_PARTITIONS codec, AffinityContext (mappings + refresh)
│   ├── channel.rs          ← ChannelRegistry: one pool per node, node-UUID→pool routing,
│   │                          round-robin fallback, lazy mapping refresh
│   ├── stream.rs           ← QueryStream: lazily-paged streaming cursor
│   ├── query.rs            ← QueryResult, Row, Column, ColumnType, UpdateResult
│   ├── pool.rs             ← IgniteClientConfig, deadpool manager
│   ├── error.rs            ← IgniteError
│   ├── protocol/           ← pure codec layer: no I/O, no async
│   │   ├── mod.rs
│   │   ├── types.rs        ← IgniteValue enum + column_type(), ColumnType, op/type codes
│   │   ├── codec.rs        ← encode_value / decode_value roundtrip, UUID/byte-array readers
│   │   ├── handshake.rs    ← protocol 1.7.0 handshake encoding + server node-UUID parsing
│   │   ├── error.rs        ← ProtocolError
│   │   └── messages.rs     ← SqlFieldsRequest, TxStart/End, cache ops, response header (topology version)
│   └── transport/          ← async TCP layer
│       ├── mod.rs
│       ├── connection.rs   ← IgniteConnection (pipelined, multiplexed; node UUID, topology version)
│       ├── error.rs        ← TransportError
│       └── tls.rs          ← build_tls_config (rustls + native-certs)
├── tests/
│   ├── smoke.rs            ← 35 broad end-to-end integration tests
│   ├── metadata.rs         ← 10 schema/system-view metadata tests
│   ├── functional_query.rs ←  6 SQL query behaviour tests (pagination, mixed KV+SQL, errors)
│   ├── functional_cache.rs ←  5 KV cache API lifecycle and operation tests
│   ├── transaction.rs      ←  3 concurrent transaction correctness tests
│   └── partition_awareness.rs ← 2 affinity-routing tests (roundtrip + PA-on/off parity)
├── local-cluster/          ← scripts + config for a local 3-node test cluster
│   ├── ignite-config.xml   ← static-discovery node config (partitioned cache, thin connector)
│   ├── start.sh            ← launch 3 nodes on ports 10800/10801/10802
│   └── stop.sh             ← stop the local nodes
└── docs/
    └── partition-awareness-plan.md  ← design/port plan for the affinity feature
```

The `protocol` and `affinity` modules have no I/O dependency, so their codec and
routing-logic tests run without a live Ignite node.

---

## Quick Start

Add to `Cargo.toml`:

```toml
[dependencies]
ignite-v2-client = { path = "../ignite-client" }
tokio = { version = "1", features = ["full"] }
```

### SELECT query

```rust
use ignite_client::{IgniteClient, IgniteClientConfig, IgniteValue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = IgniteClientConfig::new("localhost:10800");
    let client = IgniteClient::new(config);

    let result = client
        .query(
            "SELECT id, name, score FROM PUBLIC.players WHERE active = ?",
            vec![IgniteValue::Bool(true)],
        )
        .await?;

    for row in &result.rows {
        let id    = row.get(0);
        let name  = row.get_by_name("NAME");
        let score = row.get(2);
        println!("{:?}  {:?}  {:?}", id, name, score);
    }
    Ok(())
}
```

### Per-value type inspection

`QueryResult::columns` exposes result-set column names.  The Ignite 2.x
`OP_QUERY_SQL_FIELDS` protocol carries only column names in the first-page
metadata — no per-column type codes.  Type information is available per value
via `IgniteValue::column_type()`, which reads the 1-byte wire tag that
accompanies every encoded value.  The only case that returns
`ColumnType::Unknown` is `IgniteValue::Null`.

```rust,ignore
use ignite_client::{ColumnType, IgniteValue};

let result = client
    .query("SELECT id, name, score FROM PUBLIC.players WHERE id = 1", vec![])
    .await?;

if let Some(row) = result.first_row() {
    for (i, col) in result.columns.iter().enumerate() {
        let value = row.get(i).unwrap();
        let value_t = value.column_type();   // always accurate for non-NULL values
        println!("column '{}': type={:?}, value={:?}", col.name, value_t, value);
    }
}

// Dispatch on value type while iterating rows.
for row in &result.rows {
    for value in row.values() {
        match value.column_type() {
            ColumnType::Int     => { /* handle i32 */ }
            ColumnType::String  => { /* handle varchar */ }
            ColumnType::Unknown => { /* value is NULL */ }
            _                   => { /* other types */ }
        }
    }
}
```

Column names are accessible immediately on a `QueryStream` before any rows are
consumed:

```rust,ignore
let stream = client
    .query_stream("SELECT id, name FROM PUBLIC.players", vec![])
    .await?;

for col in &stream.columns {
    println!("{}", col.name);
}
```

### DML

```rust,ignore
let updated = client
    .execute(
        "UPDATE PUBLIC.players SET score = ? WHERE id = ?",
        vec![IgniteValue::Int(42), IgniteValue::Int(7)],
    )
    .await?;

println!("rows affected: {}", updated.rows_affected);
```

### Transaction

```rust,ignore
let mut tx = client.begin_transaction().await?;

tx.execute(
    "INSERT INTO PUBLIC.accounts (id, balance) VALUES (?, ?)",
    vec![IgniteValue::Int(1), IgniteValue::Double(100.0)],
).await?;

tx.execute(
    "INSERT INTO PUBLIC.accounts (id, balance) VALUES (?, ?)",
    vec![IgniteValue::Int(2), IgniteValue::Double(200.0)],
).await?;

tx.commit().await?;
// If commit() is not called, Drop triggers a fire-and-forget rollback.
```

### Transaction helper (auto-commit/rollback)

```rust,ignore
let result = client
    .with_transaction(|mut tx| async move {
        let rows = tx.query("SELECT balance FROM PUBLIC.accounts WHERE id = ?",
            vec![IgniteValue::Int(1)]).await?;
        // ... business logic ...
        Ok((tx, rows))
    })
    .await?;
```

### KV cache

```rust,ignore
let cache = client.get_or_create_cache("my-cache").await?;

cache.put(IgniteValue::Int(1), IgniteValue::String("hello".into())).await?;
let val = cache.get(IgniteValue::Int(1)).await?;
println!("{:?}", val); // String("hello")
```

### Streaming cursor

```rust,ignore
use futures::StreamExt;

let mut stream = client
    .query_stream("SELECT id FROM PUBLIC.big_table", vec![])
    .await?;

while let Some(row) = stream.next().await {
    let row = row?;
    println!("{:?}", row.get(0));
}
```

### TLS

```rust,ignore
let config = IgniteClientConfig::new("localhost:10800")
    .with_tls();                         // use system CA bundle
    // .with_tls_accept_invalid_certs() // for self-signed / dev certs

let client = IgniteClient::new(config);
```

### Authentication

```rust,ignore
let config = IgniteClientConfig::new("localhost:10800")
    .with_auth("ignite", "ignite")
    .with_pool_size(20);
```

---

## API Reference

### IgniteClientConfig

```rust,ignore
pub struct IgniteClientConfig {
    pub address: String,                  // primary "host:port" (first of `addresses`)
    pub addresses: Vec<String>,           // all cluster node addresses (for routing)
    pub partition_awareness: Option<bool>,// None = auto (on when ≥ 2 addresses); Some(b) forces it
    pub username: Option<String>,
    pub password: Option<String>,
    pub max_pool_size: usize,             // default: 10 (per node)
    pub connect_timeout: Duration,        // default: 10 s
    pub request_timeout: Duration,        // default: 30 s
    pub page_size: usize,                 // SQL rows per round-trip, default: 1024
    pub use_tls: bool,                    // default: false
    pub tls_accept_invalid_certs: bool,   // default: false
}

impl IgniteClientConfig {
    pub fn new(address: impl Into<String>) -> Self;       // single-node convenience
    pub fn with_addresses(self, addresses: Vec<String>) -> Self;   // multi-node cluster
    pub fn with_partition_awareness(self, enabled: bool) -> Self;  // force PA on/off
    pub fn with_auth(self, username, password) -> Self;
    pub fn with_pool_size(self, size: usize) -> Self;
    pub fn with_connect_timeout(self, duration: Duration) -> Self;
    pub fn with_request_timeout(self, duration: Duration) -> Self;
    pub fn with_page_size(self, page_size: usize) -> Self;
    pub fn with_tls(self) -> Self;
    pub fn with_tls_accept_invalid_certs(self) -> Self;
}
```

`connect_timeout` is also used as the deadpool `wait` and `create` timeout.
`request_timeout` is applied per-request inside `send_and_receive`.
`new()` seeds `addresses` with the single address; `with_addresses()` replaces
the list and sets `address` to the first entry. Partition awareness is enabled
automatically once two or more addresses are configured (see
[Partition awareness](#partition-awareness)).

### IgniteClient

`IgniteClient` is `Clone` — share a single instance across tasks; it wraps an
`Arc`'d per-node channel registry and a shared affinity context. Cache
operations are automatically routed to the key's owning node when partition
awareness is enabled.

```rust,ignore
impl IgniteClient {
    pub fn new(config: IgniteClientConfig) -> Self;

    /// Execute a SELECT; returns all rows (cursor automatically paginated).
    pub async fn query(&self, sql: &str, params: Vec<IgniteValue>) -> Result<QueryResult>;

    /// Execute a SELECT; returns rows lazily as a QueryStream.
    pub async fn query_stream(&self, sql: &str, params: Vec<IgniteValue>) -> Result<QueryStream>;

    /// Execute INSERT / UPDATE / DELETE.
    pub async fn execute(&self, sql: &str, params: Vec<IgniteValue>) -> Result<UpdateResult>;

    /// Begin a transaction (Pessimistic / ReadCommitted by default).
    pub async fn begin_transaction(&self) -> Result<Transaction>;

    /// Begin a transaction with explicit settings.
    pub async fn begin_transaction_with(
        &self,
        concurrency: TxConcurrency,   // Optimistic | Pessimistic
        isolation: TxIsolation,        // ReadCommitted | RepeatableRead | Serializable
        timeout_ms: i64,               // 0 = no timeout
    ) -> Result<Transaction>;

    /// Run a closure inside a managed transaction.
    pub async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T>;

    /// Return a cache handle (no network call — cache must already exist).
    pub fn cache(&self, name: &str) -> IgniteCache;

    /// Get-or-create a cache with default (ATOMIC) atomicity.
    pub async fn get_or_create_cache(&self, name: &str) -> Result<IgniteCache>;

    /// Get-or-create a cache with TRANSACTIONAL atomicity (required for KV tx).
    pub async fn get_or_create_transactional_cache(&self, name: &str) -> Result<IgniteCache>;

    /// Destroy a cache and all its data.
    pub async fn destroy_cache(&self, name: &str) -> Result<()>;

    /// List all cache names defined on the server.
    pub async fn cache_names(&self) -> Result<Vec<String>>;

    /// Pool health / sizing diagnostics.
    pub fn pool_status(&self) -> deadpool::managed::Status;
}
```

### Transaction

```rust,ignore
impl Transaction {
    pub async fn query(&mut self, sql: &str, params: Vec<IgniteValue>) -> Result<QueryResult>;
    pub async fn query_stream(&mut self, sql: &str, params: Vec<IgniteValue>) -> Result<QueryStream>;
    pub async fn execute(&mut self, sql: &str, params: Vec<IgniteValue>) -> Result<UpdateResult>;
    pub async fn commit(self) -> Result<()>;
    pub async fn rollback(self) -> Result<()>;

    /// Return a cache handle bound to this transaction.
    pub fn cache(&self, name: &str) -> IgniteCache;

    // Drop triggers a fire-and-forget rollback if not already committed/rolled back.
}
```

Transaction isolation levels map to Ignite protocol values:

| `TxIsolation` | Protocol value |
|---|---|
| `ReadCommitted` | 0 |
| `RepeatableRead` | 1 |
| `Serializable` | 2 |

Transaction concurrency modes:

| `TxConcurrency` | Protocol value |
|---|---|
| `Optimistic` | 0 |
| `Pessimistic` | 1 |

> **Note:** DML inside thin-client transactions requires the Ignite node to be
> started with `-DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true` and the table must
> use `ATOMICITY=TRANSACTIONAL`.

### IgniteCache

Obtained via `client.cache()`, `client.get_or_create_cache()`, or
`transaction.cache()`.  Cheap to clone — holds an `i32` cache ID and either the
channel registry + affinity context (non-transactional, affinity-routed) or a
transaction connection.

> **Cache names are case-insensitive** — `cache_id()` upper-cases the name
> before hashing. Use a consistent case (the test suite uses upper-case names).

```rust,ignore
impl IgniteCache {
    pub async fn get(&self, key: IgniteValue) -> Result<IgniteValue>;
    pub async fn put(&self, key: IgniteValue, value: IgniteValue) -> Result<()>;
    pub async fn put_if_absent(&self, key: IgniteValue, value: IgniteValue) -> Result<bool>;
    pub async fn get_all(&self, keys: Vec<IgniteValue>) -> Result<Vec<(IgniteValue, IgniteValue)>>;
    pub async fn put_all(&self, entries: Vec<(IgniteValue, IgniteValue)>) -> Result<()>;
    pub async fn contains_key(&self, key: IgniteValue) -> Result<bool>;
    pub async fn remove(&self, key: IgniteValue) -> Result<()>;
    pub async fn replace(&self, key: IgniteValue, value: IgniteValue) -> Result<bool>;
    pub async fn get_and_put(&self, key: IgniteValue, value: IgniteValue) -> Result<IgniteValue>;
    pub async fn get_and_remove(&self, key: IgniteValue) -> Result<IgniteValue>;
    pub async fn get_and_replace(&self, key: IgniteValue, value: IgniteValue) -> Result<IgniteValue>;
    pub async fn remove_all(&self, keys: Vec<IgniteValue>) -> Result<()>;
    pub async fn get_size(&self) -> Result<i64>;
}
```

### QueryResult / Row

```rust,ignore
pub struct QueryResult {
    pub columns: Vec<Column>,  // result-set metadata; available before any row processing
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize;
    pub fn first_row(&self) -> Option<&Row>;
}

pub struct Row { /* ... */ }

impl Row {
    pub fn len(&self) -> usize;
    pub fn get(&self, index: usize) -> Option<&IgniteValue>;
    pub fn get_by_name(&self, name: &str) -> Option<&IgniteValue>; // case-insensitive
    pub fn columns(&self) -> &[Column];
    pub fn values(&self) -> &[IgniteValue];
}

pub struct UpdateResult {
    pub rows_affected: i64,  // -1 if server did not return a count
}
```

### Column and ColumnType

```rust,ignore
pub struct Column {
    /// Column name as returned by the server (matches the SQL alias or field name).
    pub name: String,
}

pub enum ColumnType {
    Boolean,
    Byte,       // TINYINT
    Short,      // SMALLINT
    Int,        // INT
    Long,       // BIGINT
    Float,      // REAL / FLOAT
    Double,     // DOUBLE
    Char,       // CHAR (single BMP code point)
    String,     // VARCHAR
    Uuid,       // UUID
    Date,       // DATE
    Timestamp,  // TIMESTAMP
    Time,       // TIME
    Decimal,    // DECIMAL / NUMERIC
    Binary,     // BINARY / VARBINARY
    Unknown,    // NULL value — type could not be determined
}

impl ColumnType {
    /// Return the canonical SQL type name, e.g. `"INT"`, `"VARCHAR"`, `"TIMESTAMP"`.
    pub fn as_str(&self) -> &'static str;
}

impl IgniteValue {
    /// Derive the `ColumnType` from this value's own 1-byte wire type tag.
    ///
    /// Always accurate for non-NULL values.  Returns `ColumnType::Unknown`
    /// only for `IgniteValue::Null` (a NULL carries no type tag in the protocol).
    pub fn column_type(&self) -> ColumnType;
}
```

### QueryStream

A lazily-paged result stream returned by `client.query_stream()` and
`transaction.query_stream()`.  Rows are yielded one at a time; subsequent pages
are fetched from the server only when the current page is exhausted.  The
server-side cursor is closed automatically when the stream is exhausted or
dropped mid-iteration.

```rust,ignore
pub struct QueryStream {
    pub columns: Vec<Column>,
    // implements futures::Stream<Item = Result<Row>>
}

impl QueryStream {
    /// Drain the entire stream into a Vec (convenience wrapper).
    pub async fn collect_all(self) -> Result<Vec<Row>>;
}
```

Use `futures::StreamExt` to drive the stream with `.next().await`.

### IgniteValue type system

`IgniteValue` maps every Ignite wire type to a Rust variant:

```rust
pub enum IgniteValue {
    Null,
    Bool(bool),
    Byte(i8),               // TINYINT
    Short(i16),             // SMALLINT
    Int(i32),               // INT
    Long(i64),              // BIGINT
    Float(f32),             // REAL / FLOAT
    Double(f64),            // DOUBLE
    Char(u16),              // CHAR (BMP code point)
    String(String),         // VARCHAR / LONGVARCHAR
    Uuid(uuid::Uuid),       // UUID / CHAR(36)
    Date(i64),              // milliseconds from Unix epoch
    Timestamp(i64, i32),    // (epoch_ms, nanosecond_fraction)
    Time(i64),              // nanoseconds from midnight
    Decimal(BigDecimal),    // DECIMAL / NUMERIC
    ByteArray(Vec<u8>),     // BINARY / VARBINARY
    RawObject(Vec<u8>),     // payload bytes of an Ignite BINARY_OBJECT (type 27)
}
```

Wire encoding follows the spec at
https://ignite.apache.org/docs/latest/binary-client-protocol/data-format

Notable encoding details:

- **UUID**: 16 bytes big-endian (most-significant bytes first, matching Java's
  `UUID.getMostSignificantBits()` / `getLeastSignificantBits()`)
- **Decimal**: `[i32: scale][i32: byte_count][bytes: two's-complement big-endian magnitude]`
- **Timestamp**: `[i64: epoch_ms][i32: nanoseconds_fraction]`
- **Null**: type code 101 with no payload; any typed field may be null

---

## Architecture

### Request multiplexing

A single `IgniteConnection` supports many concurrent requests without
serialising them through a mutex on reads:

```
Caller A ──request(id=1)──┐               ┌──response(id=1)──▶ Caller A
                          │  TCP socket   │
Caller B ──request(id=2)──┤ ─────────────▶│
                          │               │  background
Caller C ──request(id=3)──┘               │  reader task
                                          └──response(id=3)──▶ Caller C
                                             response(id=2)──▶ Caller B
```

Requests are written to a `Mutex<SplitSink>` (contended only on write, not on
read).  Each caller registers a `oneshot::Sender` in a shared `HashMap<i64,
Sender>` keyed by `request_id`.  The background reader task peeks the first 8
bytes of each response frame, looks up the sender, and delivers the payload.

This is the same design used by `tokio-postgres` and `redis-rs`.

### Connection pool

`IgniteClient` wraps a [deadpool](https://crates.io/crates/deadpool) managed
pool of `IgniteConnection` objects.  Pool behaviour:

- `max_pool_size` connections maximum (default 10)
- Each connection is health-checked on recycle via `is_alive()` (AtomicBool)
- Connections are created on demand, not pre-warmed
- `connect_timeout` is applied as both the deadpool `wait` and `create` timeout
- TCP keepalive is applied to every socket (60 s idle, 15 s interval)

### Transaction connections

Transactions use a **dedicated TCP connection** that is not drawn from the pool.
This avoids pool exhaustion when many concurrent long-running transactions are
in flight.  The connection is closed when the `Transaction` is dropped.

### Pagination

`OP_QUERY_SQL_FIELDS` returns a first page with a `cursor_id` and a `has_more`
flag.

- `client.query()` / `transaction.query()` — automatically fetches all
  subsequent pages via `OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE` and returns a
  fully materialised `QueryResult`.
- `client.query_stream()` / `transaction.query_stream()` — returns a
  `QueryStream` that fetches pages lazily as the consumer polls the stream.
  The server-side cursor is closed when the stream is exhausted or dropped.

The `page_size` config field controls how many rows are returned per server
round-trip (default 1024).

### Partition awareness

Without partition awareness every request goes to one configured node, and the
server silently re-routes each key to its owning node — an extra network hop.
With partition awareness the client computes the owning node locally and sends
the request straight to it. The design mirrors the Java thin client
(`org.apache.ignite.internal.client.thin`):

```
        ┌──────────────── IgniteClient ────────────────┐
        │  AffinityContext (Arc-shared)                 │
        │   • partition → primary-node UUID mappings    │
        │   • topology version + single-flight refresh  │
        └───────────────────────────────────────────────┘
                          │ affinity_node(cache_id, key)
                          ▼
   key ─▶ affinity_hash(key) ─▶ partition(hash, mask, parts) ─▶ node UUID
                          │
                          ▼
        ┌──────────── ChannelRegistry ─────────────┐
        │  pool[node-1]  pool[node-2]  pool[node-3] │  ← one deadpool pool per node
        │  node UUID → pool index (learned)         │
        └───────────────────────────────────────────┘
                          │ get(target) → owning node, else default round-robin
                          ▼
                  request sent to the primary node
```

How it works:

1. **Handshake** — connecting to each node learns its **node UUID** (parsed from
   the protocol 1.7 handshake success response) and keys that node's pool.
2. **Mapping fetch** — on first use of a cache, or after a topology change, the
   client issues `CACHE_PARTITIONS` (op 1101) and decodes the
   `partition → node` table. Fetches are single-flighted per cache.
3. **Routing** — for a single-key op, `affinity_hash(key)` (a faithful port of
   the JVM `hashCode()` for primitives / `String` / `UUID`) feeds the rendezvous
   `partition(...)` function; the resulting partition selects the primary node's
   pool.
4. **Topology tracking** — every response header carries the affinity topology
   version; a bump marks the held mappings stale and triggers a lazy refresh.

**Fail safe.** Partition awareness is a pure optimization. Any miss — PA
disabled, no mapping yet, unknown/unsupported key type, unknown node, or a dead
target pool — falls back to the default channel, so observable results are
always identical to the single-node path. Multi-key ops (`get_all`, `put_all`),
SQL, and transactions use the default channel.

**Enablement.** Auto-on when two or more addresses are configured; override with
`with_partition_awareness(true|false)`. Configure the cluster with:

```rust,ignore
let config = IgniteClientConfig::new("node1:10800")
    .with_addresses(vec![
        "node1:10800".into(),
        "node2:10800".into(),
        "node3:10800".into(),
    ]);
let client = IgniteClient::new(config);

let cache = client.get_or_create_cache("MY_CACHE").await?;
cache.put(IgniteValue::Int(42), IgniteValue::Long(7)).await?; // routed to key 42's node
```

Scope: routing currently covers primitive, `String`, and `UUID` keys (the types
with well-defined, exactly-replicable Java hash codes). Custom affinity-key
fields, backup-node routing, and server endpoint discovery are future work.

---

## Codec Details

The `src/protocol/` module contains the codec with no I/O dependency, making
it independently testable.

### Frame format

```
[i32 LE: payload_length]  ← length prefix handled by LengthDelimitedCodec
[payload bytes]           ← the codec layer strips the prefix before delivery
```

### Request payload format

```
[i16 LE: op_code]
[i64 LE: request_id]
[operation-specific fields …]
```

### Response payload format

```
[i64 LE: request_id]
[i32 LE: status]          ← 0 = success; non-zero = server error
if status != 0:
  [string: error_message]
if status == 0:
  [operation-specific response …]
```

### Java string hashing

Cache IDs and field IDs in the binary protocol are derived using Java's
`String.hashCode()` algorithm.  The `java_hash()` function in `types.rs`
replicates this:

```rust
pub fn java_hash(s: &str) -> i32 {
    s.chars().fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}
```

The `cache_id()` helper derives the cache ID from a cache name. The name is
upper-cased first, so cache names are effectively case-insensitive:

```rust
pub fn cache_id(cache_name: &str) -> i32 {
    java_hash(&cache_name.to_uppercase())
}
```

---

## Comparison with Existing Rust Clients

| | [vkulichenko](https://github.com/vkulichenko/ignite-rust-client) | [ptupitsyn fork](https://github.com/ptupitsyn/ignite-rust-client) | **this crate** |
|---|:---:|:---:|:---:|
| SQL queries | ✗ | ✗ | ✅ |
| Cursor pagination | ✗ | ✗ | ✅ |
| Streaming cursor | ✗ | ✗ | ✅ |
| Transactions | ✗ | ✗ | ✅ |
| Async I/O (tokio) | ✗ | ✗ | ✅ |
| Connection pool | ✗ | ✗ | ✅ |
| UUID / Date / Timestamp / Decimal | ✗ | ✗ | ✅ |
| Null handling | ✗ | ✗ | ✅ |
| Query parameters | ✗ | ✗ | ✅ |
| TLS | ✗ | ✗ | ✅ |
| KV get/put | ✅ | ✅ | ✅ |
| Partition awareness / affinity routing | ✗ | ✗ | ✅ |
| Last commit | 2020 | 2020 | 2026 |
| Intended use | learning exercise | abandoned fork | production |

The vkulichenko / ptupitsyn implementations are synchronous, blocking, cover
only `get` / `put` on primitives, and have not been updated since 2020.  They
are not suitable as a dependency baseline for production work.

---

## Running Tests

### Unit tests (no live node required)

```bash
cargo test --lib
```

Covers:

- `IgniteValue` codec roundtrip for every type (Null, Bool, Byte, Short, Int,
  Long, Float, Double, Char, String, UUID, Date, Timestamp, Time, Decimal,
  ByteArray, RawObject)
- Two's-complement Decimal encoding (positive, negative, zero, boundary)
- `java_hash()` against known Java reference values
- `SqlFieldsRequest` encode/decode
- Transaction start/end encoding
- **Partition awareness** (`src/affinity.rs`, `src/channel.rs`):
  - `affinity_hash()` JVM `hashCode` golden vectors per key type (Int, Long,
    Bool, Byte/Short/Char, String, UUID) and unsupported-kind fallback
  - rendezvous `partition()` / `calculate_mask()` / `safe_abs()` golden vectors
  - `CACHE_PARTITIONS` request encoder + response decoder (against byte fixtures)
  - `AffinityContext` routing decisions, topology-version ordering, single-flight
    refresh; `PoolSelector` node→pool mapping and round-robin fallback
  - `CACHE_PARTITIONS` opcode (1101), node-UUID and feature-bitmask handshake
    parsing, and response-header topology-version capture

### Integration tests (require a live Ignite 2.x node on localhost:10800)

All integration test modules connect to `localhost:10800` with no
authentication.  Start Ignite before running them — the easiest way is the
bundled [local 3-node cluster](#local-3-node-test-cluster).

DML-inside-transaction tests (used by `smoke.rs` and `transaction.rs`) require
the JVM system property that enables DML in thin-client transactions, and the
SQL `DATE` temporal test expects a UTC server timezone:

```bash
# Linux / macOS
IGNITE_HOME/bin/ignite.sh \
    -DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true \
    -Duser.timezone=UTC \
    config.xml

# Windows (example wrapper script)
C:\ignite\run-ignite.bat
```

(The `local-cluster/start.sh` script sets both of these automatically.)

The `partition_awareness.rs` module exercises affinity routing. Point it at the
whole cluster with the `IGNITE_ADDRS` environment variable (comma-separated);
without it the tests use the single default address and still exercise the full
routing pipeline against one node:

```bash
IGNITE_ADDRS=localhost:10800,localhost:10801,localhost:10802 \
  cargo test --test partition_awareness -- --nocapture --test-threads=1
```

**Always run integration tests with `--test-threads=1`.**  Parallel DDL
(CREATE / DROP TABLE) causes schema lock contention, and many concurrent
connections from parallel tests can exhaust Ignite's handshake thread pool,
producing `Unable to perform handshake within timeout` errors.

Run all integration tests across all modules in one command:

```bash
cargo test --tests -- --nocapture --test-threads=1
```

Or run individual modules:

```bash
cargo test --test smoke              -- --nocapture --test-threads=1
cargo test --test metadata           -- --nocapture --test-threads=1
cargo test --test functional_query   -- --nocapture --test-threads=1
cargo test --test functional_cache   -- --nocapture --test-threads=1
cargo test --test transaction        -- --nocapture --test-threads=1
cargo test --test partition_awareness -- --nocapture --test-threads=1
```

> **Windows note:** If Windows Smart App Control blocks newly compiled test
> binaries, redirect the build output to AppData:
> ```
> set CARGO_TARGET_DIR=%APPDATA%\ignite-test-target
> cargo test --tests -- --nocapture --test-threads=1
> ```

---

### `tests/smoke.rs` — 35 tests

Broad end-to-end coverage of every public API surface.  Tests are prefixed
`smoke_`.

- Basic connectivity (`SELECT 1`)
- SQL query and DML (`query`, `execute`)
- Multi-page cursor pagination
- `query_stream` (lazy streaming) and early-drop cursor close
- Configurable `page_size` (forces multi-page fetch)
- `begin_transaction`, `begin_transaction_with` (explicit concurrency/isolation/timeout)
- `with_transaction` (auto-commit and rollback/Drop paths)
- `Transaction::query`, `Transaction::execute`, `Transaction::query_stream`
- `Transaction::cache` (KV ops inside a transaction — commit and rollback)
- `IgniteCache`: get, put, put_if_absent, get_all, put_all, contains_key,
  remove, replace, get_and_put, get_and_remove, get_and_replace, get_size
- `cache_names`, `get_or_create_cache`, `destroy_cache`
- `Row::get`, `get_by_name`, `len`, `is_empty`, `columns`, `values`
- `QueryResult::columns` and `row_count`
- `IgniteValue::column_type()` for per-value type accuracy
- `pool_status()`, `with_pool_size()`, `with_auth()` builder fields
- Type roundtrips: Bool, Int, Long, Double, String, Byte, Short, Float,
  ByteArray, Decimal, UUID, Null
- Date, Time, Timestamp as query parameters
- Server-side error propagation
- TLS config builder (no network), TLS graceful failure to plaintext server
- Concurrent queries on a shared client

---

### `tests/metadata.rs` — 10 tests

Rust port of selected tests from Apache Ignite's `JdbcThinMetadataSelfTest.java`,
adapted to use `SYS.*` system views via `OP_QUERY_SQL_FIELDS` rather than JDBC
`DatabaseMetaData`.

| Test | What it verifies |
|---|---|
| `metadata_result_set_column_names_and_types` | JOIN query column names and per-value `column_type()` |
| `metadata_decimal_and_date_column_types` | `ColumnType::Decimal` and `ColumnType::Date` round-trip |
| `metadata_tables_visible_in_sys_tables_view` | Created table appears in `SYS.TABLES` |
| `metadata_public_tables_present_in_sys_tables_view` | Multiple tables visible together |
| `metadata_schemas_visible_in_sys_schemas_view` | `PUBLIC` and `SYS` schemas present |
| `metadata_sys_schemas_filter_returns_no_rows_for_unknown_pattern` | Empty result for unknown schema |
| `metadata_columns_visible_in_sys_table_columns_view` | Column names and PK flag in `SYS.TABLE_COLUMNS` |
| `metadata_pk_index_visible_in_sys_indexes_view` | PK and secondary index in `SYS.INDEXES` |
| `metadata_all_table_indexes_present_in_sys_indexes_view` | Indexes from multiple tables visible |
| `metadata_query_unknown_table_returns_error` | Non-existent table query returns `Err` |

---

### `tests/functional_query.rs` — 6 tests

Rust port of Apache Ignite's `FunctionalQueryTest.java` (indexing module).

| Test | Java source | What it verifies |
|---|---|---|
| `functional_query_sql_fields_pagination` | `testQueries` | 100-row insert; query 50 rows with small `page_size`; multi-page cursor |
| `functional_query_sql` | `testSql` | CREATE TABLE, INSERT, SELECT by parameter, SELECT with `page_size=1` |
| `functional_query_empty_table` | `testGettingEmptyResultWhenQueryingEmptyTable` | Empty table query returns non-error result with 0 rows |
| `functional_query_mixed_sql_and_cache` | `testMixedQueryAndCacheApiOperations` | SQL INSERT then KV `cache.put`; SELECT sees both |
| `functional_query_server_error` | `testSqlParameterValidation` | Invalid SQL / missing table returns `Err` |
| `functional_query_empty_sql` | `testEmptyQuery` | Empty SQL string returns `Err` |

---

### `tests/functional_cache.rs` — 5 tests

Rust port of selected tests from Apache Ignite's `FunctionalTest.java` (thin
client internal tests), covering the KV cache API.

| Test | Java source | What it verifies |
|---|---|---|
| `functional_cache_management` | `testCacheManagement` | Full cache lifecycle: create → `get_size` → name in `cache_names` → destroy → name absent |
| `functional_cache_put_get` | `testPutGet` | `put`, `get`, `contains_key`, overwrite, remove, multi-type keys |
| `functional_cache_atomic_put_get` | `testAtomicPutGet` | `get_and_put`, `get_and_remove`, `put_if_absent`, `get_and_replace` sequences |
| `functional_cache_batch_put_get` | `testBatchPutGet` | `put_all` / `get_all` (full + partial) / `remove_all` subset / `get_size` |
| `functional_cache_remove_replace` | `testRemoveReplace` | `replace` (true/false) and `remove` / `remove_all` on a 100-entry dataset |

---

### `tests/transaction.rs` — 3 tests

Rust port of selected tests from Apache Ignite's `BlockingTxOpsTest.java`.

| Test | Java source | What it verifies |
|---|---|---|
| `tx_sum_invariant` | `testTransactionalConsistency` (Pessimistic/RepeatableRead) | 5 concurrent tasks × 100 key-transfer iterations; sum of all values remains 0 |
| `tx_sum_invariant_optimistic` | `testTransactionalConsistency` (Optimistic/Serializable) | Same invariant with conflict-retry loop |
| `tx_all_cache_ops_inside_tx` | `testBlockingOps` | Every cache operation works correctly inside an explicit transaction: `put`, `get`, `contains_key`, `put_all`, `get_all`, `put_if_absent`, `replace`, `get_and_put`, `get_and_remove`, `get_and_replace`, `remove`, `remove_all` |

---

### `tests/partition_awareness.rs` — 2 tests

End-to-end affinity-routing tests. Set `IGNITE_ADDRS` to route across multiple
nodes; otherwise they run against the single default address.

| Test | What it verifies |
|---|---|
| `pa_put_get_roundtrip_is_correct` | A spread of keys `put`/`get` correctly with partition awareness on — the routed write/read path returns every stored value |
| `pa_on_and_off_agree` | Reading the same keys through a PA-on and a PA-off client yields identical results (the fail-safe guarantee: routing never changes observable results) |

Enable `RUST_LOG=ignite_client=debug` to see routing decisions, e.g.
`fetched cache partitions cache_id=… version=… mappings=… applicable=…`.

---

### Local 3-node test cluster

The `local-cluster/` directory contains everything needed to run a partitioned
3-node Apache Ignite cluster on localhost from a local Ignite binary:

| File | Purpose |
|---|---|
| `ignite-config.xml` | Node config: static localhost discovery, a partitioned cache, thin-client connector |
| `start.sh` | Launch three nodes sequentially on thin ports 10800 / 10801 / 10802 |
| `stop.sh` | Stop the local nodes |

Usage:

```bash
# point IGNITE_HOME at your local Apache Ignite install (contains bin/ignite.sh)
export IGNITE_HOME=/path/to/apache-ignite-x.y.z-bin

./local-cluster/start.sh          # forms a 3-node cluster, waits until ready
./local-cluster/stop.sh           # tears it down
```

`start.sh` auto-selects Java 11 (via `/usr/libexec/java_home -v 11` on macOS) and
sets `-DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true` and `-Duser.timezone=UTC` so the
full integration suite — including the transaction and temporal tests — passes.
Logs are written to `/tmp/ignite-node{1,2,3}.log`.

Run the complete suite against it:

```bash
export IGNITE_ADDRS=localhost:10800,localhost:10801,localhost:10802
cargo test --tests -- --test-threads=1
```

---

## Cargo.lock dependency pins

All dependencies are pinned to exact versions for reproducible builds.
The workspace root `Cargo.toml` `[workspace.dependencies]` section is the
single place to update them:

| Crate | Pinned version |
|---|---|
| `tokio` | `=1.50.0` |
| `tokio-util` | `=0.7.18` |
| `bytes` | `=1.11.1` |
| `thiserror` | `=2.0.18` |
| `tracing` | `=0.1.44` |
| `futures` | `=0.3.32` |
| `bigdecimal` | `=0.4.10` |
| `uuid` | `=1.21.0` |
| `deadpool` | `=0.13.0` |
| `async-trait` | `=0.1.89` |
| `num-bigint` | `=0.4.6` |
| `socket2` | `=0.6.2` |
| `rustls` | `=0.23.37` |
| `tokio-rustls` | `=0.26.4` |
| `rustls-native-certs` | `=0.8.3` |

---

## License

Apache License 2.0.  See [LICENSE](LICENSE).

Apache Ignite is a registered trademark of The Apache Software Foundation.
This project is not affiliated with or endorsed by the Apache Software
Foundation or GridGain Systems.
