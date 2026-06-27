# Plan: Partition Awareness / Affinity Routing for the Rust thin client

## Goal

Today every request goes through a single deadpool pool to one configured address;
the server silently re-routes each key to its owning node (an extra network hop).
Partition awareness lets the client compute the owning node for a key locally and
send the request straight to it — eliminating the proxy hop for `get`/`put`/etc.

The Java `org.apache.ignite.internal.client.thin` package
(`/Users/johncoleman/IdeaProjects/ignite/modules/thin-client`) is the reference.
The port maps cleanly:

| Java | Rust target |
|---|---|
| `TcpClientChannel` (one socket, knows `srvNodeId`, `srvTopVer`) | `IgniteConnection` + node-UUID + topology-version fields |
| `ReliableChannelImpl` (`nodeChannels: Map<UUID,…>`, default channel, fallback) | new `ChannelRegistry` (per-node deadpool pools + default) |
| `ClientCacheAffinityContext` (pending caches, last topology) | new `AffinityContext` (Arc-shared) |
| `ClientCacheAffinityMapping` (`partition→node` arrays, key cfg) | new `CacheAffinityMapping` |
| `RendezvousAffinityFunction.calculatePartition` | `affinity::partition()` |
| `BinaryObject.hashCode()` | `affinity::affinity_hash(&IgniteValue)` |

## Current-state findings (grounding)

- **Single-node only.** `IgniteClientConfig.address: String` → one deadpool `Pool`
  (`pool.rs:13`, `pool.rs:166`). No node identity, no topology, no routing.
- **Topology version is received but thrown away.** `read_response_header` reads the
  `FLAG_TOPOLOGY` (0x02) bytes and discards them (`messages.rs:53-56`). This is exactly
  Java's `AFFINITY_TOPOLOGY_CHANGED` flag — the trigger we need.
- **Handshake advertises zero features and ignores the success tail**
  (`handshake.rs:39-40`, `:80-90`). The server node UUID that ships in the 1.7 handshake
  success response is never parsed.
- **`CACHE_PARTITIONS` op-code exists but is unused** (`types.rs:76`).
  ⚠️ It's defined as `1100`; the Java enum is `1101`. **Must verify** before wiring —
  a wrong opcode here is a silent failure.
- **`java_hash` exists for strings only** (`types.rs:260`), used for `cache_id`.
  We need a general key hash.
- **`request()` strips and validates the header internally** (`connection.rs:348-352`),
  so callers never see the topology flag. The cache call path (`cache.rs:80-93`) always
  pulls from the single pool.

## Design decisions (recommended)

1. **Keep deadpool, one pool per node.** A `ChannelRegistry` holds
   `HashMap<Uuid, Pool>` keyed by node UUID, plus a default round-robin set for fallback.
   This preserves existing recycling/timeout behavior per node instead of rewriting the
   transport.
2. **Initial node set = explicitly configured addresses.** Connect to each, learn its
   UUID at handshake, build the map. Server-side endpoint *discovery* (Java's
   `ClientClusterDiscovery`) is deferred to a later phase — partition awareness works
   across the addresses the user lists, which is the standard baseline.
3. **Advertise an empty bitmask feature set** (do **not** opt into `ALL_AFFINITY_MAPPINGS`
   or `DC_AWARE`). This makes the server emit the *simplest* `CACHE_PARTITIONS` wire
   format (no `customMappingsRequired`/`dfltMapping` booleans, no DC mapping array) —
   the smallest correct decoder. Partition awareness itself is a protocol-version feature
   (≥1.4), already satisfied by 1.7.
4. **Scope initial key types to primitives + String + UUID.** These have well-defined
   Java `hashCode`s we can replicate and test exactly. Complex/`RawObject`/custom-affinity
   keys **fall back to the default channel** (still correct, just not optimized).
   Expand later.
5. **Always route to primary; lazy mapping refresh.** Mirror Java: fetch a cache's mapping
   on first use or after a topology-version bump, guarded against concurrent duplicate
   fetches. Backup-node routing for read ops is a later enhancement.
6. **Fail safe.** Any miss — PA disabled, no mapping, unknown node, dead channel,
   unsupported key — falls back to the default pool. Partition awareness is a pure
   optimization; it must never change observable results.

## Phased work

### Phase 0 — Protocol groundwork (no behavior change, fully unit-testable)
- **Handshake**: advertise the partition-awareness feature; parse the server **node UUID**
  (and bitmask) from the success response (`handshake.rs`). Store UUID on `IgniteConnection`.
- **Response header**: capture `(topology_changed, major, minor)` instead of discarding
  (`messages.rs:53-56`); expose it so the reader task can stash the latest topology
  version on the connection (`AtomicI64`s).
- **`affinity` module** (new): `affinity_hash(&IgniteValue) -> i32` (Java `hashCode` per
  type: Int=value, Long=`(int)(v ^ v>>>32)`, Bool=1231/1237, String=`java_hash`, etc.) +
  `partition(hash, parts)` (rendezvous mask logic) + `calculate_mask`. Golden-value unit
  tests derived from a small Java program.
- **`CACHE_PARTITIONS` codec** (new in `messages.rs`): request encoder (cache-id set,
  empty/DC fields omitted) + response decoder for the simplest wire format →
  `Vec<CacheAffinityMapping>`. **Verify opcode 1100 vs 1101 first.**

### Phase 1 — Multi-node connection registry
- `IgniteClientConfig`: add `addresses: Vec<String>` (keep `address` as single-element
  convenience).
- `ChannelRegistry`: build one `Pool` per configured address; after each node's first
  handshake, key it by UUID; retain a default ordered pool list for round-robin +
  fallback. Replace the single `pool` field in `IgniteClient`.
- Refactor `IgniteCache::send` to accept an optional target node UUID: present → that
  node's pool; absent/miss → default.

### Phase 2 — Affinity context + routing
- `AffinityContext` (Arc-shared): `current_version: AffinityTopologyVersion`,
  `cache_aff: HashMap<i32, CacheAffinityMapping>`, `pending: HashSet<i32>`,
  `enabled: bool`, an in-progress guard.
- `affinity_node(cache_id, &key) -> Option<Uuid>`: look up mapping →
  `partition(affinity_hash(key), parts)` → `partition→node` array.
- Lazy refresh: on each cache op, if mapping absent or
  `connection.topology_version > context.version`, issue `CACHE_PARTITIONS` (single-flight)
  then route.
- Wire `get/put/contains_key/remove/replace/get_and_*` through `affinity_node` →
  `send(target)`. Add a config toggle (default: on when ≥2 nodes and server supports it).

### Phase 3 — Robustness & enhancements (post-MVP)
- Channel-failure fallback + retry on the default channel (Java
  `applyOnNodeChannelWithFallback`).
- Backup-node routing for the read-only op set (`CACHE_GET`, `CONTAINS_KEY`, scan/SQL).
- Custom affinity-key field config (`keyCfg` type-id→field-id extraction).
- Server endpoint discovery so PA reaches nodes not in the initial address list.

## Testing strategy
- **Unit**: hash + partition golden vectors vs Java output; `CACHE_PARTITIONS` decoder
  against a captured byte fixture; routing-decision logic with a mocked mapping.
- **Integration**: a multi-node (3-node) Ignite docker cluster; add per-node send counters
  (tracing) and assert keys distribute across nodes and that results stay correct with PA
  on vs off.
- **Safety**: every fallback path returns identical results to the single-node path.

## Key risks
1. **`hashCode` fidelity** — the whole feature is wrong if the hash diverges from the
   server by one bit. Mitigation: narrow initial key types + golden tests.
2. **Opcode/wire-format mismatch** (`1100` vs `1101`; feature-gated optional fields).
   Mitigation: verify against Java, advertise empty bitmask, fixture-test the decoder.
3. **Concurrency** around lazy refresh and the registry. Mitigation: single-flight guard,
   Arc/Mutex, copy mapping arrays out under lock.

## Reference: simplest `CACHE_PARTITIONS` response wire format

With an empty bitmask feature set advertised, the server omits the `ALL_AFFINITY_MAPPINGS`
and `DC_AWARE` optional fields, leaving:

```
[topVer: i64][minorTopVer: i32][mappingsCnt: i32]
  per mapping:
    [applicable: bool][cachesCnt: i32]
    if applicable:
      per cache: [cacheId: i32][keyCfgCnt: i32] per: [typeId: i32][fieldId: i32]
      [primary partition mapping]:
        [nodesCnt: i32]
          per node: [nodeId: UUID][partCnt: i32] per: [part: i32]
    else:
      per cache: [cacheId: i32]
```

Build `partition → nodeId` as a `Vec<Uuid>` indexed by partition number (Java uses
`UUID[]` sized to `maxPart + 1`).
