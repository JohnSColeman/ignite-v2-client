# ignite-client

An async Rust thin client for **Apache Ignite 2.x**, implementing the
[Ignite Binary Client Protocol](https://ignite.apache.org/docs/latest/thin-clients/getting-started-with-thin-clients)
over TCP.

---

## Table of Contents

- [Features](#features)
- [Protocol Reference](#protocol-reference)
- [Workspace Structure](#workspace-structure)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [IgniteClientConfig](#igniteclientconfig)
  - [IgniteClient](#igniteclient)
  - [Transaction](#transaction)
  - [IgniteCache](#ignitecache)
  - [QueryResult / Row](#queryresult--row)
  - [QueryStream](#querystream)
  - [IgniteValue type system](#ignitevalue-type-system)
- [Architecture](#architecture)
  - [Request multiplexing](#request-multiplexing)
  - [Connection pool](#connection-pool)
  - [Transaction connections](#transaction-connections)
  - [Pagination](#pagination)
- [Codec Details](#codec-details)
- [Comparison with Existing Rust Clients](#comparison-with-existing-rust-clients)
- [Running Tests](#running-tests)
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
| Partition awareness / affinity routing | 🔲 Future |

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

## Workspace Structure

```
ignite-client/              ← workspace root AND main library crate
├── Cargo.toml              ← workspace manifest + root package (ignite-v2-client)
├── src/                    ← public API (crate name: ignite_client)
│   ├── lib.rs
│   ├── client.rs           ← IgniteClient: query, execute, begin_transaction, cache, …
│   ├── transaction.rs      ← Transaction: query, execute, commit, rollback, cache, drop
│   ├── cache.rs            ← IgniteCache: get, put, get_all, put_all, remove, …
│   ├── stream.rs           ← QueryStream: lazily-paged streaming cursor
│   ├── query.rs            ← QueryResult, Row, Column, UpdateResult
│   ├── pool.rs             ← IgniteClientConfig, deadpool manager
│   └── error.rs            ← IgniteError
├── tests/
│   └── smoke.rs            ← 35 end-to-end integration tests
├── ignite-protocol/        ← pure codec crate: no I/O, no async
│   └── src/
│       ├── types.rs        ← IgniteValue enum, op codes, type codes, tx enums
│       ├── codec.rs        ← encode_value / decode_value roundtrip
│       ├── handshake.rs    ← protocol 1.7.0 handshake encoding
│       └── messages.rs     ← SqlFieldsRequest, TxStart/End, cache ops, cursor pagination
└── ignite-transport/       ← async TCP layer
    └── src/
        ├── connection.rs   ← IgniteConnection (pipelined, multiplexed)
        └── tls.rs          ← build_tls_config (rustls + native-certs)
```

The separation of `ignite-protocol` from `ignite-transport` means the codec
can be tested in isolation (no Ignite node required) and can be used by other
transport implementations without modification.

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
    pub address: String,                  // "host:port"
    pub username: Option<String>,
    pub password: Option<String>,
    pub max_pool_size: usize,             // default: 10
    pub connect_timeout: Duration,        // default: 10 s
    pub request_timeout: Duration,        // default: 30 s
    pub page_size: usize,                 // SQL rows per round-trip, default: 1024
    pub use_tls: bool,                    // default: false
    pub tls_accept_invalid_certs: bool,   // default: false
}

impl IgniteClientConfig {
    pub fn new(address: impl Into<String>) -> Self;
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

### IgniteClient

`IgniteClient` is `Clone` — share a single instance across tasks; it wraps an
internal `Arc`'d pool.

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
`transaction.cache()`.  Cheap to clone — holds an `i32` cache ID and either a
pool reference or a transaction connection.

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
    pub columns: Vec<Column>,
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

---

## Codec Details

The `ignite-protocol` crate contains the codec with no I/O dependency, making
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

The `cache_id()` helper derives the cache ID from a cache name:

```rust
pub fn cache_id(cache_name: &str) -> i32 {
    java_hash(cache_name)
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
| Last commit | 2020 | 2020 | 2026 |
| Intended use | learning exercise | abandoned fork | production |

The vkulichenko / ptupitsyn implementations are synchronous, blocking, cover
only `get` / `put` on primitives, and have not been updated since 2020.  They
are not suitable as a dependency baseline for production work.

---

## Running Tests

### Unit tests (no live node required)

```bash
cargo test -p ignite-protocol
```

Covers:

- `IgniteValue` codec roundtrip for every type (Null, Bool, Byte, Short, Int,
  Long, Float, Double, Char, String, UUID, Date, Timestamp, Time, Decimal,
  ByteArray, RawObject)
- Two's-complement Decimal encoding (positive, negative, zero, boundary)
- `java_hash()` against known Java reference values
- `SqlFieldsRequest` encode/decode
- Transaction start/end encoding

### Smoke tests (requires a live Ignite 2.x node on localhost:10800)

Start an Ignite node.  DML-in-transaction tests require the JVM flag and a
TRANSACTIONAL-atomicity table (see `create_tx_table` in the test file):

```bash
# Linux / macOS
IGNITE_HOME/bin/ignite.sh -DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true config.xml

# Windows
C:\ignite\run-ignite.bat
```

Run all 35 smoke tests sequentially (parallel DDL causes schema lock contention):

```bash
cargo test --test smoke -- --nocapture --test-threads 1
```

> **Windows note:** If Windows Smart App Control blocks newly compiled binaries,
> build to AppData instead:
> `CARGO_TARGET_DIR="$APPDATA/ignite-test-target" cargo test --test smoke -- --nocapture --test-threads 1`

Smoke test coverage:

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
- `QueryResult::columns` field and `row_count`
- `pool_status()`, `with_pool_size()`, `with_auth()` builder fields
- Type roundtrips: Bool, Int, Long, Double, String, Byte, Short, Float,
  ByteArray, Decimal, UUID, Null
- Date, Time, Timestamp as query parameters
- Server-side error propagation
- TLS config builder (no network), TLS graceful failure to plaintext server
- Concurrent queries on a shared client

---

## Cargo.lock dependency pins

Due to MSRV constraints the following crates are pinned to exact versions.
If your project requires newer versions of these, the workspace root
`Cargo.toml` `[workspace.dependencies]` section is the single place to update
them:

| Crate | Pinned version | Reason |
|---|---|---|
| `tokio` | `=1.35.1` | system cargo 1.75 compatibility |
| `uuid` | `=1.6.1` | API stability |
| `deadpool` | `=0.10.0` | API stability |

---

## License

Apache License 2.0.  See [LICENSE](LICENSE).

Apache Ignite is a registered trademark of The Apache Software Foundation.
This project is not affiliated with or endorsed by the Apache Software
Foundation or GridGain Systems.
