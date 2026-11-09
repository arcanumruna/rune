# Lease topics: etcd-style leases in Rune, and a Rune-backed discovery library

Status: DRAFT 2026-11-07, revision 4, for owner review. Revision 3 applied the owner's rulings: "mirror etcd's lease
semantics exactly", "no lease simply expires … take over a lease? out of spec", and "the main purpose of lease topics
is to replace what the `discovery` library plus etcd do today, with exact parity". Revision 4 applies the builder
check (below). Branch read: `origin/main` @ 33de61d9; the key-width facts in §5.1 were re-read at 6f79348.

**Revision 4 change log (builder-check settlements, Coordinator's call, Herald consistency pass K15 R1–R11, F3,
B28).** Lease semantics (grant, keepalive, revoke, expiry, the etcd rows of §4) are unchanged; these fix places where
rev 3 could not be built or could expire early.
- **R1** Lease-topic keys are up to 128 bytes; the discovery path is `string(128)`. §5.1 lists the code that must
  change (§3.1, §5.1).
- **R2** `ScanLeases` walks the lease view in seeded-key-hash order: the resume token is the last hash, the prefix is
  filtered on the broker, a page is O(n), and the handoff's rule 3 is over hash ranges (§8.3, §12.4). Recipes find
  "the lowest version" from their shared `LeaseWatch`, not from a scan (§12.3).
- **R3** A promoted leader arms at the lease-clock time its view enters `Ready`, not at `refoldJudgedNow` (§9.2, §9.6).
- **R4** A new wheel state `Checkpointing{ckptOffset}`; keepalives park there. No checkpoint and no `Expire` is
  emitted from `Clearing`, `Checkpointing` or `Emitting`. A keepalive's `Checkpoint(0)` passes the emission gate or
  parks (§8.1, §9.5, §9.6).
- **R5** At `onAtRisk` the client fails every pending and unsent produce of the producers bound to that lease
  (§12.2).
- **R6** The caller-supplied-client form of the discovery library is fully asynchronous: it owns its own
  `LeaseSession` on that client, every operation is a state machine driven by `doWork(nowNs)`, and it has
  `stopAsync()` / `isStopped()`; the client outlives the library (§13).
- **R7** `LeaseSession<Capacity, OnLost, OnAtRisk>` is untyped, `add(topic, partition, keyBytes, version, ttlMs)`,
  and learns ends from an optional shared `LeaseWatch`; recipes share one `LeaseWatch` (§12.2, §12.3).
- **R8** `lastAppended` tracks only `Revoke` and `Update`, so contenders' grants cannot defer an `Expire` (§6.4, §9.3).
- **R9** The broker re-encodes every `Revoke` from the lease view, so it carries the full row (§6.2, §6.3).
- **R10** §19 describes Herald 02's real use (the `HERALD_*` producers, passive consumers, the `herald-pool`
  sidecar), with Herald's slot formula `nsId << 16 | roleId`, `roleId = role << 8 | partition` (B28).
- **R11** The `min_ttl` floor is a fixed catalog constant of 1 s; deadline arithmetic saturates; a discovery
  `Producer` needs `READ` and `WRITE` (§4, §5.2, §10, §14.1).
- **F3** `herald.idslots` is keyed `(slot, epoch)` and its value column is `fencingToken` (§19.4).
- **F7** Herald's server-API idempotency is a plain swept topic, not a lease; it is gone from §19. Herald's tenant
  delete grace is added (§19.6).
- **Vague items settled:** `onAtRisk` (§12.2), the async client API (§12.1), the `Expire` / `Checkpoint` data append
  (§9.1), the cleaner's fixed `lease_version` offset (§6.6), per-entry keepalive answers (§8.1), handoff overflow,
  `asOfOffset` and multi-filter merge (§12.4), recipe session ids (§12.3), a local `onLost` (§12.2), known-old
  duplicates per op (§6.3), dropping a topic with parked keepalives (§9.6).
**Owner ruling 2026-11-07 (transient lease topics are in scope).** `transient lease topic` is allowed. It persists
nothing: a lease on a transient topic does not survive a restart of every replica of its partition. After such a
restart the partition is empty, watchers get `PartitionDataReset` (43), and holders get `kErrLeaseNotFound` and grant
again. While any replica stays up, the ring is replicated in memory as for every transient topic, so a minority
failure loses no lease. Appendix A is now the design input for this, not deferred work.

**Owner rulings 2026-11-07 (transient restart):**
- **Nothing goes to disk for a transient lease topic.** No version floor, no borrowed leader epoch, no file. After a
  full restart the ring starts empty and versions start again from the ring's first offset.
- **Grants wait out `max_ttl` after a full restart.** Every grant on a reborn partition is refused until `max_ttl`
  has passed since the empty ring was created, so every old holder's lease has run out before the key can be held
  again. `max_ttl` comes from the topic's catalog definition, which Rune keeps for every topic. Holders learn of the
  loss earlier, at their next keepalive (`kErrLeaseNotFound` or `PartitionDataReset`).
- **Consequence, stated plainly:** fencing by version does not hold across a full restart of a transient lease
  topic. A writer frozen through the whole wait could come back with a version that matches a new holder's. A
  workload that needs fencing across a full restart uses a persisted lease topic.
- **Watchers** get `PartitionDataReset` (43), clear their state, and see each re-grant as a new key.

Still for the design pass (no owner question): **eviction must not drop a live lease's latest record** while the
cluster is up: an eviction floor at the oldest live lease's anchor, broker re-appends (checkpoint-style carry) to
move old anchors forward, followers evicting only below the leader's floor (the fetch response's `logStartOffset`),
and a reserve so `Expire` can always append.

Program: Herald on Rune (`arcanumruna/herald`, `docs/superpowers/specs/2026-11-07-herald-00-program-overview.md`,
decision D5: no etcd, Redis or Postgres, so Rune is the only store and bus; §13 rows "Lease semantics" and "Leases:
purpose and uses").

**Herald consistency pass (Coordinator's call, 2026-11-07).** This revision takes the changes Herald spec 02 §12.4
proposed. Herald serves each tenant from one active instance (Herald D16), so presence (§19.3) and auto-dissolve
(§19.5) use no leases, and friend-request expiry is a read-time row (§19.6). `herald.idslots` gets one row per minter
(§19.4). The discovery library may take a caller-supplied client (§13, a proposed change). §17 Q1 is settled by
Herald 00 D12. Lease semantics are unchanged.

**Sources for parity.**
- etcd v3, read at `main`:
  - `server/lease/lessor.go` and `server/lease/lease.go`;
  - `server/lease/leasepb` (the `Lease{ID, TTL, RemainingTTL}` checkpoint);
  - `server/etcdserver/v3_server.go` (LeaseGrant, LeaseRevoke, LeaseRenew, LeaseTimeToLive, LeaseLeases);
  - `client/v3/lease.go`;
  - `client/v3/concurrency/{session,mutex,election}.go`.
- The `discovery` library (header-only C++23):
  - `Producer.h`, `Consumer.h`, `ServiceCache.h`, `ServiceList.h`, `ServiceKey.h`, `SubscriptionFilter.h`,
    `ServiceInfo.h`, `ServiceClientProxy.h`, `CircuitBreaker.h`, `SendResult.h`, `Regions.h`;
  - its integration tests, `test/discovery/DiscoveryIntegrationTest.cpp`;
  - the etcd-cpp-apiv3 client it uses (`KeepAlive`, `Client`).
- Herald's use of discovery: `src/main.cpp:157-195`, `ChatConfig.h:171-180`, `ChatAsyncServerChannel.h`.

**How decisions are marked.**
- **(owner):** set by the owner.
- **(call):** set by the author.
- **(Coordinator's call):** set by the Herald spec coordinator.
- **Divergence:** a place where Rune cannot do what etcd does, with the reason.

**Numbering note (Coordinator's call).** The body uses symbolic names for every new number:
- error codes `kErrLeaseNotFound`, `kErrLeaseLimitExceeded`, `kErrLeaseTtlTooLarge`;
- `kClientVersionLeases`, `kBrokerVersionLeases`, the cluster feature level `kFeatureLevelLeases`;
- `VerdictViewKind::LeaseIndex`;
- the request ids of `LeaseKeepAlive`, `LeaseTimeToLive` and `ScanLeases`.

Concrete values are assigned at implementation time, after whichever of this work and checked-writes Part 11 merges
first. At 33de61d9 the free values are:
- error codes 62–64 (`ErrorCode` ends at 61, `Protocol.h:246`);
- client version 18 (`kClientVersion = 17`);
- broker version 11 (`kBrokerVersion = kBrokerVersionCheckedWrites = 10`, `Protocol.h:448`);
- feature level 6 (`kLatest = kCheckedWrites = 5`, `ReplicationMessages.h:481-483`);
- `VerdictViewKind` value 4 (`Verdict.h:18`).

Existing codes keep their numbers in the text:

| Code | Name |
|---|---|
| 39 | `InvalidRequest` |
| 45 | `NotAuthorized` |
| 51 | `NamespaceSuspended` |
| 52 | `NamespaceLimitExceeded` |
| 53 | `Throttled` |
| 56 | `KeyExists` |

## 1. Why

Herald today runs service discovery on etcd, through the `discovery` library:
- **Registration.** Every chat server registers its S2S endpoint under a lease (`Producer`, TTL
  `DISCO_LEASE_TTL_SECONDS`, default 15 s).
- **Lookup.** Every chat server watches the coordinator's registrations into a `ServiceCache` (`Consumer`, filter
  `sharedService("COORDINATOR_SERVICE")`). It picks a replica with `sendToSharedExact`, falling back to
  `sendSharedLeastLoaded`.

Herald decision D5 removes etcd, so Rune must provide what `discovery` + etcd provide:
- leases with a TTL, kept alive by the holder;
- keys that disappear when their lease ends;
- list-then-watch of a key prefix;
- the building blocks for leadership (etcd `concurrency`).

It must match them exactly (owner).

Herald also needs leases for other things (doc 00 §13):
- leadership (the tenant lease and singletons);
- worker fencing;
- short holds (case claims, invites, rate limits, break-glass grants);
- deadlines that need an expiry event (sanction and strike expiry, appeal tokens, the tenant delete grace).

Herald's presence and auto-dissolve use no leases (§19.3, §19.5).

Some of those TTLs are long (24 h, 400 days), and long TTLs use etcd-style checkpointing (owner).

**The owner's idea (owner, 2026-11-07).** "Lease topics in Rune, automatically backed by a view, which creates expiry
events after some time and propagates them, unless events keep updating the timestamp (the lease)." Revision 3 keeps
that shape, but takes its semantics from etcd. Keepalives refresh the deadline on the leader only, as etcd's
`LeaseRenew` does, and are not log records.

**The feature.** A **lease topic** is a typed, keyed, compacted topic. Each live key **is** a lease:
- **Granted.** A key is granted with a TTL, only if it has no live lease.
- **Kept alive.** The holder keeps it alive with `LeaseKeepAlive` requests to the partition leader.
- **Revoked.** It ends with a fenced revoke.
- **Expired.** If it is not kept alive for its TTL, the partition leader appends an `Expire` record. That record
  replicates, every view folds it, every consumer receives it, and compaction treats it as a tombstone.

The topic comes with an implicit view, `<topic>.leases`, that holds every live lease for point reads, paged scans and
read-your-writes.

## 2. Goals, non-goals, anti-goals

**Goals**
- **G1. Discovery parity (primary).** Everything Herald does with `discovery` + etcd works on Rune, with the same
  observable behaviour (§3):
  - service registration under a TTL;
  - re-registration after a lost lease;
  - load reporting;
  - list-then-watch into a `ServiceCache`;
  - replica selection;
  - passive mode.

  A **Rune-backed `discovery` library** keeps the public API of `Producer`, `Consumer`, `ServiceCache` and the rest.
  Only the connection argument changes. It is a deliverable with its own phase (§13, §20 Phase 3).
- **G2. etcd lease parity.** Lease semantics mirror etcd v3 exactly, row by row in §4:
  - grant, the minimum and maximum TTL, keepalive, revoke and TimeToLive;
  - leader-driven monotonic expiry;
  - the leader-change rule (re-arm at the full TTL, or at the checkpointed remaining TTL, plus an election extension);
  - checkpointing;
  - the `concurrency` Session, Mutex and Election patterns.

  Every place Rune differs is marked **Divergence**, with the reason.
- **G3. A lease always expires (owner).** Expiry never pauses: not in any namespace state, and not while a namespace
  is `ReadOnly` or `RejectAll`.
- **G4. No takeover (owner).** A key can be granted only when it has no live lease. This is etcd's
  `CreateRevision(key) == 0` compare. There is no takeover operation.
- **G5. Expiry is a log record.** The partition leader decides when a lease is due, on its own monotonic clock, and
  appends the `Expire`. Replicas, views and consumers converge from the log. This is rule 16: the timer decides only
  *when* the leader proposes an end; *whether* it ended is decided by the judged log.
- **G6. Fencing.** Every grant's version is its log offset. It is the analogue of etcd's `create_revision`, strictly
  increases per key, and is the fencing token. A stale holder's update, keepalive or revoke is refused.
- **G7. Exactly one visible end** per lease, under leader change, restart, catch-up and racing operations.
- **G8. Never early.** A lease never expires before its TTL has passed since the leader acknowledged its last
  keepalive. This holds across leader changes and whole-cluster restarts, with no cross-node clock comparison (§9.2).
- **G9. Prompt.** In steady state an abandoned lease ends within `ttl + tick + one duty cycle + commit`. After a leader
  change, it ends within the etcd promotion bound (§9.6).
- **G10. Zero cost for non-lease topics.** A keepalive costs no disk write and no replication in steady state.

**Non-goals**
- Leases that own several keys (etcd's attach-many). **Divergence D1**, §4.
- Fenced writes to other topics: a guarded resource checks the version itself (§18).
- Transactions on lease topics.
- `autoincrement` keys.
- The SQL, Redis and sink surfaces.
- Wall-clock time in any form. Revision 2's wall mode is removed: long TTLs use checkpoints (§9.5).

**Anti-goals.** Any of these makes the work a failure:
- two live holders of one key;
- a keepalive acknowledged for a lease that has ended;
- an expiry earlier than §9.2 allows;
- a lease that ends twice, or ends without a visible record;
- expiry pausing or stalling, for any reason;
- a lease kept alive by anything other than its own keepalives;
- a non-lease topic getting slower;
- a lease hot path that allocates, locks or makes a virtual call.

## 3. Discovery parity (primary)

This section maps every public API and behaviour of the `discovery` library onto lease topics. §13 specifies the
Rune-backed library. Gaps are numbered **DG1–DG9**, and each one has a resolution.

### 3.1 The discovery topic

One lease topic per deployment namespace, with one partition, so every consumer sees one total order, as etcd gives
(call):

```sql
lease topic 'herald-sys/discovery' {
    path      string(128),   -- <servicePrefix>/<region>/<cluster|_shared>/<serviceName>/<replicaId>/<incarnation>
    uri       string(128),
    load      uint32,        -- 0 = not reported
    mapId     uint64, clusterId uint64, regionId uint64,   -- 0 = absent (ServiceInfo's optionals)
    holderInfo string(64)
} partitions 1 replicas 3 key path with (lease.ttl = 15000);
```

The key is the etcd key path, extended with an `incarnation` segment (DG2). That keeps etcd's prefix layout, so
`ScanLeases` with a prefix is etcd's prefix `Get` (in hash order, not key order: §8.3). The path is at most 128 bytes,
the lease-topic key limit (§5.1, Coordinator's call, R1); the incarnation takes 17 of them (`/` plus 16 hex digits),
and the `Producer` refuses a longer path at construction.

### 3.2 Producer

| `discovery` today (etcd) | Rune-backed library | Gap |
|---|---|---|
| `Producer(etcdUrl, servicePrefix, region, ServiceKey identity, leaseTtlSeconds)` | `Producer(RuneEndpoint, servicePrefix, region, identity, leaseTtlSeconds)`. `RuneEndpoint` = bootstrap addresses, namespace, SCRAM credentials. All other arguments are unchanged. | **DG1** |
| ctor: `leasekeepalive(ttl)` = LeaseGrant plus a KeepAlive thread, retrying 30× with backoff | the ctor connects. The lease is granted with the key at `registerEndpoint`, because the key is the lease; the same 30× retry and backoff apply to connect and grant. In the caller-supplied-client form nothing blocks: connect is the caller's, and the grant with its retries is an async state machine (§13). | **DG4** |
| keepalive every `max(ttl−1, 1)` s (etcd-cpp `KeepAlive`), errors in `eptr` with no handler | `LeaseSession` keepalive every `ttl/3` (etcd's Go client cadence). Errors reach `onLost` / `onAtRisk`, which the Producer wires to re-registration. | **DG5** (fixes a latent loss: one late keepalive lost the lease) |
| `registerEndpoint(uri)`: `put(key, uri, leaseId)` | first call: grant (`Put`+`insert`) of row `{path, uri}`. A later call with a new uri is a fenced `Put`+`update` of `uri`, which does not refresh the lease, exactly like etcd's put. | — |
| key `<servicePrefix>/<region>/<cluster\|_shared>/<serviceName>/<replicaId>` | the same path plus `/<incarnation>`: a 64-bit random id drawn at each grant | **DG2** |
| restart of the same replicaId within the TTL: the new process's `put` overwrites the key and re-attaches it to the new lease | that would be a takeover, which is out of spec. The new incarnation is a new key, so the grant succeeds at once. Consumers keep the newest live incarnation per `ServiceKey` (§3.3). | **DG2** |
| `startLoadReporting(uri, loadFn, intervalSec = 5)`: re-put `{"uri","load"}` with the lease every interval; requires `ttl ≥ 2 × interval`; a failed put sets `leaseNeedsReinit` | the same timer issues a fenced `Put`+`update` of `load`. It does not keep the lease alive (etcd: a put does not renew). The `ttl ≥ 2 × interval` check is kept. `kErrLeaseNotFound` sets `leaseNeedsReinit`. | **DG3** (typed columns, not JSON) |
| watchdog every `ttl/2`: `get(key)`; if missing, `tryReinitializeLease` (a new lease) and `reRegister` | kept: a point read of the lease view (`getViewRowK`) every `ttl/2`; if absent, or on `onLost`, it grants a new incarnation and re-registers | — (superset: `onLost` reacts before the watchdog) |
| `stop()`: cancel the keepalive, then a blocking `leaserevoke` | cancel the keepalive, then a blocking fenced `Revoke` (the same blocking behaviour as BUG-005). The caller-supplied-client form has `stopAsync()` / `isStopped()` instead (§13). | — |
| `~Producer()` calls `stop()` | the same | — |

### 3.3 Consumer

| `discovery` today | Rune-backed library | Gap |
|---|---|---|
| `Consumer::create(etcdUrl, servicePrefix, region, filters, ServiceCache&)` → `shared_ptr` | `Consumer::create(RuneEndpoint, servicePrefix, region, filters, ServiceCache&)` | **DG1** |
| `run()`: for each filter, `ls(prefix)` (`prefix = regionPrefix + filter.toWatchPath()`; empty = the whole region), `handlePut` per entry, then `watchPrefix(prefix, index + 1)` | `run()`: one `LeaseWatch::startFromNow` over the topic with every filter's prefix: a `ScanLeases(prefix)` per distinct prefix (paged, with `asOfOffset`), then one subscription to the topic's partition from the minimum `asOf`, merged by the exact handoff rule (§12.4, including the multi-prefix merge). Events whose path does not match a filter are dropped client-side. | **DG7** |
| watch re-armed from `modified_index + 1`; empty event batches throttled by 250 ms; on error, re-initialise after 1 s | the subscription is push, so there are no empty batches and no throttle. On any break (disconnect, `NotLeader`, a resume failure), the consumer re-initialises after 1 s with the same scan + subscribe, as today. | — |
| PUT → `handlePut`: parse the key into a `ServiceKey`, apply the filter, parse the value (a plain uri or JSON `{uri, load, mapId, clusterId, regionId}`), update the cache | `Granted` / `Updated` → the same `handlePut` path. The `ServiceInfo` fields come from typed columns. `parseRegistrationValue` stays public for compatibility and tests. | **DG3** |
| DELETE → `handleDelete` → `cache.remove` | `Revoked` / `Expired` → `handleDelete`, for the current incarnation only (DG2) | — |
| one key per `ServiceKey` | the cache entry for a `ServiceKey` is its live incarnation with the highest `lease_version`. A newer grant replaces the entry (an etcd PUT with a new uri). The end of an older incarnation is ignored. The end of the current one falls back to the next-newest live incarnation if one exists, otherwise `remove`. | **DG2** |
| WebSocket connections per key (non-passive), reconnect backoff 250 ms–30 s + 0–750 ms jitter, a uri change reconnects, a load change calls `setLoad` | unchanged: transport-independent | — |
| `setAuthToken` (appends `?token=` to dials), `setPassiveMode`, `setMessageCallback`, `startBatchFlush(50 ms)`, `stop()` | unchanged | — |

### 3.4 Everything else

These are unchanged, because none of them touches etcd:
- `ServiceCache`: the lock-free snapshot, `update` / `remove` / `clear`, and `onAdd` / `onRemove`;
- selection: `get`, `getShared`, `roundRobin`, `roundRobinHealthy`, `leastLoaded` (seeded from `load`), `getExact`,
  `leastLoadedWithReplicaId`, and the shared variants;
- sending: `sendToExact`, `sendLeastLoaded`, `sendToSharedExact`, `sendSharedLeastLoaded`;
- the session API: `openSession*`, `sendViaSession`, `closeSession`;
- `ServiceList`, `ServiceKey`, `SubscriptionFilter` (`all`, `byCluster`, `byClusterAndService`, `sharedServices`,
  `sharedService`, `matches`), `ServiceInfo`, `SendResult`;
- `CircuitBreaker` (5 failures, 10 s), `ServiceClientProxy`, `Regions`.

### 3.5 The legacy Herald chat server's call sites (the parity target)

These are the call sites of today's chat server (the parity target and the first cutover). The Herald 02 runtime
uses the library differently; §19.1 describes that use (Coordinator's call, R10).

| Legacy Herald today | After the switch |
|---|---|
| `Producer{DISCO_URL, …, ServiceKey{serviceDomain, "CHAT_SERVICE", serviceName}, DISCO_LEASE_TTL_SECONDS}` and `registerEndpoint("ws://host:s2sPort/server")` | the same calls with a `RuneEndpoint` from `RUNE_BOOTSTRAP`, `RUNE_NAMESPACE` and credentials from the environment |
| `Consumer::create(…, {sharedService("COORDINATOR_SERVICE")}, cache)`, `setAuthToken(serviceJwt)`, `run()` | the same |
| `ChatAsyncServerChannel`: `sendToSharedExact`, falling back to `sendSharedLeastLoaded` | unchanged |
| environment `DISCO_URL`, `SERVICE_DOMAIN`, `LOCATION` | `DISCO_URL` is replaced (DG1); the other two are unchanged |

Herald uses no etcd transactions, TimeToLive, Mutex or Election today.

### 3.6 Gaps

| # | Gap | Resolution |
|---|---|---|
| DG1 | etcd URL → a Rune connection | `RuneEndpoint{bootstrap, namespace, credentials}`. Credentials come from the environment, never the repo. This is the one API change. |
| DG2 | no takeover, so a restart within the TTL cannot overwrite its own old key | an incarnation segment in the key; the consumer keeps the newest live incarnation per `ServiceKey`. Observable behaviour matches etcd: the restarted replica is visible at once, and only one entry per `ServiceKey` is in the cache. A zombie old process that still keeps alive is shadowed by the newer one, as etcd's overwrite shadowed it. |
| DG3 | a JSON value → typed columns | the Producer writes columns. `parseRegistrationValue` stays for tests and for mixed-transport migration. |
| DG4 | etcd grants a lease before any key; here the key is the lease | not observable: a Producer's lease exists only to carry its one key |
| DG5 | keepalive cadence `ttl−1` → `ttl/3` | deliberate: etcd's Go client cadence. One delayed keepalive no longer loses the lease. |
| DG6 | etcd-cpp's `KeepAlive` reports errors to an `eptr` nobody reads | `onLost` / `onAtRisk` callbacks; the Producer re-registers |
| DG7 | per-prefix etcd watches → one subscription per partition | the client filters by prefix. The discovery topic is small, so one consumer receiving the whole namespace's registry is cheap; a re-scan on every break replaces etcd's re-init on `ErrCompacted`. |
| DG8 | etcd's global revision orders every key | the discovery topic has one partition, so its offsets are a total order, as etcd's revision is |
| DG9 | `Mutex` / `Election` from etcd `concurrency` (not used by Herald today, needed for leadership) | `LeaseOwnership` / `LeaseElection` recipes (§12.3) |

## 4. etcd parity

| etcd v3 | Rune lease topics | Parity |
|---|---|---|
| **LeaseGrant(TTL, ID)**: ID chosen or assigned; `ErrLeaseExists` | `Put`+`insert` of the key. The lease id is `(topic, key, version)`, and `version` = the grant's offset, so it is unique and never reused. A live key answers `KeyExists` (56). | same; the id is assigned |
| `TTL > MaxLeaseTTL` (9 000 000 000 s) → `ErrLeaseTTLTooLarge` | `lease_ttl_ms > lease.max_ttl` → `kErrLeaseTtlTooLarge`. The `lease.max_ttl` default is 9 000 000 000 s, the hard ceiling. | same |
| TTL below `minLeaseTTL = ⌈(3 × electionTicks / 2) × heartbeat⌉` s is **raised** to it, not refused | `lease_ttl_ms < lease.min_ttl` (including 0) is raised to `lease.min_ttl` by the write hook. The floor of `lease.min_ttl` is the fixed catalog constant `kLeaseMinTtlFloorMs = 1000` (Coordinator's call, R11): above partition failover (`effectiveBrokerLivenessMs` = 450 ms plus the lease-view rebuild), and the same on every node, so catalog validation never depends on node config. | same (ms instead of s; a fixed floor instead of one derived from election ticks) |
| **LeaseKeepAlive** stream → `LeaseRenew` on the leader only, not through raft; `ErrNotPrimary` elsewhere | the `LeaseKeepAlive` request, served by the partition leader only, under the view-read lease. It writes no log record. Elsewhere it answers `NotLeader`. | same |
| renew of a missing lease → `ErrLeaseNotFound` | absent key or wrong version → `kErrLeaseNotFound` | same |
| renew of an expired-not-yet-revoked lease waits for the revoke (`ErrLeaseNotFound`) or demotion (`ErrNotPrimary`) | a due or `Emitting` lease parks the keepalive until its `Expire` folds (`kErrLeaseNotFound`) or the leader is demoted (`NotLeader`) | same |
| renew of a lease with a checkpointed remaining TTL first proposes `Checkpoint(remaining = 0)` through raft, synchronously | the same: the leader appends `Checkpoint(remaining = 0)` and answers after it folds | same |
| renew resets expiry to `now + TTL` (monotonic clock); the response carries the TTL | the leader re-arms its wheel at `leaseNow + ttl`; the response carries `ttlMs` | same |
| client keepalive cadence `TTL/3` (Go) | `LeaseSession` cadence `ttl/3` | same |
| **LeaseRevoke(ID)**: deletes the lease and its keys in one txn; `ErrLeaseNotFound` | fenced `Revoke(key, version)`; a wrong version or missing key → `kErrLeaseNotFound` | same for one key (D1) |
| **LeaseTimeToLive(ID, keys)**: TTL, granted TTL, attached keys; TTL −1 if not found | `LeaseTimeToLive(key)`: granted TTL, remaining TTL from the leader's wheel, version, holder; remaining −1 if not found. "Keys" is the key itself. | same (D1) |
| **LeaseLeases**: every lease id | `ScanLeases` with an empty prefix, paged | same, paged (D3) |
| expiry: the leader's `runLoop` every 500 ms finds expired leases and proposes `LeaseRevoke` through raft; followers never expire | the leader's wheel (1 ms tick) finds due leases, and the leader appends `Expire` records through replication; followers never expire | same (finer tick) |
| attached keys are deleted when the lease expires | the key is deleted: `Expire` is a tombstone for every view and for compaction | same (D1) |
| revoke rate limit 1000/s (`leaseRevokeRate`) | `lease.expiry_batch` per duty cycle, ≥ 100 000/s per partition (§10) | timing only: never earlier, often sooner |
| **Promote(extend = electionTimeout)**: every lease is re-armed at `now + extend + remainingTTL` (checkpointed) or `+ TTL`; if there are ≥ `leaseRevokeRate` leases, expiries are spread to 3/4 of the rate | the new leader re-arms every live lease at `T_ready + extend + (lease_remaining_ms > 0 ? remaining : ttl)`, where `T_ready` is the lease-clock time its lease view enters `Ready` (§9.6), with `extend = leaderHeartbeatTimeoutNs`. Nothing is spread: the batch emitter already bounds the rate. | same arming; no spreading (timing only) |
| **Demote**: every lease set to forever | on demotion the leader's wheel is discarded with its view; nothing is emitted | same |
| **Checkpointing**: every `checkpointInterval` (5 min default), for leases with remaining TTL > interval, at most `leaseCheckpointRate` (1000/s), in batches of 1000; persisted only with `CheckpointPersist` | `Checkpoint` records (lease op 3) every `lease.checkpoint_interval` (5 min default) for leases with remaining > interval, at most `lease.checkpoint_rate` (1000/s) per partition, batches of up to 1000. On by default; always persisted, being log records. | **Divergence D4** (default on; always persisted) |
| **Put with a lease** attaches the key; it does not renew | `Put`+`update` replaces the row's columns, fenced by version; it does not renew | same |
| Put on a key held by **another** lease re-attaches it (takeover) | refused: `Put`+`upsert` is `InvalidRequest`, and `Put`+`insert` on a live key is `KeyExists` | **owner ruling** (no takeover) |
| **DeleteRange** on a leased key deletes it; the lease lives on | refused (`InvalidRequest`, "use revoke"): deleting the key would end the lease without the version check | D1 |
| a lease owns many keys | one key per lease | **Divergence D1** |
| `create_revision` / `mod_revision` are global cluster revisions | `lease_version` (the grant's offset) and record offsets are per partition | **Divergence D2** |
| `Get(prefix)` at revision R, then `Watch(prefix, R + 1)`: one MVCC snapshot | paged `ScanLeases` with a per-page `asOfOffset`, then a subscription from the first page's `asOf`, merged by the handoff rule | **Divergence D3** |
| watch after compaction → `ErrCompacted`; the client re-lists | a subscription resumes on the compacted log; the discovery library re-scans on every break. Tombstones live `delete.retention.ms` (1 h default). | equivalent |
| **Session**(TTL 60 s default, `Done()` on loss, `Close` revokes) | `LeaseSession` (`ttl` default = the topic's `lease.ttl`; `onLost` ≈ `Done()`; `close()` revokes every key) | same, per key (D1) |
| **Mutex**: put `pfx/<leaseId>` guarded by `CreateRevision == 0`; the owner is the lowest create revision under `pfx`; `Lock` waits for predecessor deletes; `TryLock`; `IsOwner` | `LeaseOwnership`: grant `name/<sessionId>-<campaignNo>` (§12.3); the owner is the lowest `lease_version` under `name/`, read from the recipe's shared `LeaseWatch`; `lock` waits for its predecessor's `Revoked` / `Expired`; `tryLock`; `isOwner` | same within a partition (D2) |
| **Election**: `Campaign`, `Proclaim`, `Resign`, `Leader`, `Observe` | `LeaseElection` on the same pattern (§12.3) | same |
| fencing: the create revision of the owner's key | `lease_version` of the owner's key, strictly increasing per key and per partition | same within a partition (D2) |

**Divergences.**
- **D1. The key is the lease (Coordinator's call).** etcd's multi-key lease deletes all its keys atomically on
  expiry. In Rune, keys of a topic live in different partitions on different leaders, and lease topics refuse
  transactions (§6.5). Rune is atomic only within a partition, so a multi-key lease would expire part by part.
  - What Rune needs: discovery uses one key per lease (one `registerEndpoint` per Producer), and leadership,
    worker fencing, short holds and one-shot deadlines each need one key per lease too.
  - A holder with many keys keeps them alive together with one `LeaseSession` (batched keepalives).
  - Multi-key leases inside one partition are later work (§18).
- **D2. Revisions are per partition.** Rune has no cluster-wide order across partitions. A fencing token is compared
  only with tokens of the same key, which is all fencing needs. Ownership and Election topics have one partition, which
  gives them etcd's total order.
- **D3. List-then-watch is paged.** A Rune view is not MVCC: it holds the current state only. The handoff rule
  (§12.4) gives the same "snapshot + every later change, no gap, no duplicate" result as etcd's revision-based watch.
- **D4. Checkpoints are on by default, and always persisted.** etcd ships checkpointing off. Then every leader change
  re-arms a lease at its full TTL, and a 400-day lease under periodic leader changes would never expire. The owner
  ruled that long TTLs use checkpointing. A Checkpoint is a log record, so it is always durable; this matches etcd with
  `CheckpointPersist` on, which etcd added to fix the same restart problem.

**Timing differences that are not divergences.** etcd guarantees only "not before the TTL", and so does Rune. The
points below make Rune sooner, never earlier:
- a 1 ms tick instead of the 500 ms `runLoop`;
- ms TTLs instead of seconds;
- a higher expiry rate;
- no promotion spreading.

## 5. Declaration

### 5.1 Rune QL

```sql
lease topic <name> { <field> <type>, ... } [ partitions <n> ] [ replicas <r> ] key <field>
      [ with ( <setting> = <value>, ... ) ] ;
```

- **Shape.** A lease topic is typed and keyed. Without a field list and `key`, the DDL is refused with
  `lease topic needs a typed, keyed topic`.
- **Refused forms.** `autoincrement` is refused. `transient lease topic` is allowed (owner ruling 2026-11-07, top of this spec; design pending, Appendix A).
- **Cleanup.** `cleanup.policy` is fixed to `compact`. Retention other than −1 is refused: a live lease's latest
  record must never be deleted by retention. `delete.retention.ms` defaults to 1 h (call).
- **Key index.** The lease index is the topic's key index (§7.1); `key.index` is implied.
- **Reserved names.** User fields named `lease_*` are refused, because the broker adds four system columns (§6.1).
- **Fixed-width fields (amended 2026-11-08).** Every field of a lease topic is fixed-width: `string` and `bytes`
  without a width are refused with `InvalidSchema` ("a lease topic's fields are fixed-width"), at `define` and at
  `alter ... add`. The lease view holds every user column in a fixed-width row, and the revoke re-encode (§6.2) rebuilds
  the record from that row.
- **Key width (Coordinator's call, R1).** A lease topic's key is an integer or `string(N)` with N ≤ 128
  (`kMaxLeaseKeyBytes = kMaxViewKeyBytes = 128`). Every other topic keeps N ≤ 16. Discovery paths and election
  names need the width; it costs only lease-view memory, which `max-view-memory-bytes` already counts.

  **What the code does today, and what must change** (read at 6f79348):

  | Place | Today | Change |
  |---|---|---|
  | `AdminWorker.cpp:1460-1469` (`buildDefine_`) | refuses a key above `string(16)` through `isTopicKeyType` (`PrimType.h:19-26`, `kMaxTopicKeyBytes = 16`) | add `isLeaseTopicKeyType(type, width)` (N ≤ 128) and use it when `DefineTopic` carries a `LeaseSpec`; the refusal text names the lease limit |
  | `WriteState.h:118` (`TopicWriteSpec`) | a key wider than 16 gets `keyWidth = 0`, which means "the key cannot be checked", so the record-key = row-key check (`:246-258`) would silently switch off | use the lease rule for lease topics, so `keyWidth` is the real width (it is a `uint8_t`, which holds 128) |
  | `VerdictSource.cpp:155` (`addVerdictView`, key index) | throws above 16 bytes | accept up to 128 for `LeaseIndex` (member 0); `Member::keyWidth` is a `uint8_t`, fine |
  | client `Key::kMaxInline = 16` (`Key.h:39`), the producer ring's `SEND_RECORD` layout (`ProducerImpl.h:132-138`: a 16-byte inline key slot, value at byte 38), `TypedTopic`'s static asserts, `Client.cpp:58` | a record key is at most 16 bytes on the client | `LeaseTopic` writes need a second, lease-only `SEND_RECORD` form whose key slot is 128 bytes (selected per topic at open; fixed-size, no heap). Non-lease sends keep today's layout and cost. |
  | `ViewTable` (`ViewTable.cpp:86`, `keyWidth ≤ kMaxViewKeyBytes`), `kMaxViewKeyBytes = 128` (`ViewKeyEncoding.h:16`), `ViewKeyBuilder.cpp:60` | already 128 | none |
  | `getViewRowK` `kMaxKeyBytes = 128` (`DataMessages.h:969`), the gateway's tail buffer (`ClientGateway.cpp:10836`) | already 128 | none: the watchdog's point read fits |
  | the stored record format (varint key length, `RecordBatch`), the cleaner's `OffsetMap` (`string_view` keys), `partitionForKey(bytes)` | width-free | none |
  | `ScanLeases` prefix | 0–127 bytes | none |

  The lease view is not checkpointed (§7.1), so the v3 view-checkpoint key header is not on this path.

### 5.2 Settings

| Key | Values | Default | Alterable |
|---|---|---|---|
| `lease.ttl` | `min_ttl`–`max_ttl` ms | 30 000 | yes (the default the client library writes into new grants) |
| `lease.min_ttl` | from the floor `kLeaseMinTtlFloorMs` (1 000, a catalog constant, R11) up to `max_ttl` ms | 1 000 | yes |
| `lease.max_ttl` | `min_ttl` to 9 000 000 000 000 ms (etcd `MaxLeaseTTL`) | 9 000 000 000 000 | yes |
| `lease.max_keys` | 1–16 777 216 live leases per partition | 1 048 576 | no |
| `lease.checkpoint` | `on`, `off` | `on` (D4) | yes |
| `lease.checkpoint_interval` | 1 000–86 400 000 | 300 000 (etcd 5 min) | yes |
| `lease.checkpoint_rate` | 1–100 000 checkpoints/s per partition | 1 000 (etcd) | yes |
| `lease.expiry_batch` | 1–4096 `Expire` records per partition per duty cycle | 256 | no |

- **The TTL is fixed at grant.** This is etcd's rule: a lease's TTL never changes after grant.
  - Every record carries `lease_ttl_ms`. The write hook writes the raised value into the grant (§6.4), so arming
    always uses the TTL in the log, never a catalog value.
  - An alter of `min_ttl` or `max_ttl` changes only the checks on new grants. A live lease keeps its TTL until it
    ends.
- **Alters of the checkpoint keys** apply to checkpoints scheduled after the catalog event. Whether a lease is
  checkpointed never affects whether it ends, only how a later leader arms it.
- **Validation.** An out-of-range value is answered `InvalidRequest` (39) with a detail, as for every setting.

### 5.3 Admin wire and catalog

- **`LeaseSpec`.** `DefineTopic` carries an optional
  `LeaseSpec { ttlMs u64, minTtlMs u64, maxTtlMs u64, maxKeys u32, expiryBatch u16, checkpoint u8,
  checkpointIntervalMs u32, checkpointRate u32 }`. It is encoded like `Persistence`: absent means "not a lease topic".
- **One catalog event creates both objects (Coordinator's call).** One `CreateTopic` event creates the topic and its
  lease view (a real `ViewObject`).
  - `Applied` (`CatalogState.h:63-65`) gains `leaseViewObjectId`.
  - Dropping the topic drops the view, and `drop view <topic>.leases` is refused.
  - `AlterTopicConfig` handles the alterable keys.
- **Gates.**
  - Every broker must be at `kFeatureLevelLeases`.
  - Every lease surface (writes, `LeaseKeepAlive`, `LeaseTimeToLive`, `ScanLeases`, `getViewRow` on a lease view, and
    subscriptions to a lease topic) needs `kClientVersionLeases`. An older client's `op()` would read `Revoke` and
    `Expire` as upserts (`Subscription.h:131-132`). An older session gets `InvalidRequest` (39) naming the version.
  - The broker announces `kBrokerVersionLeases`.
- **Namespaces.**
  - A lease topic counts toward `maxTopics` and the partition limits.
  - Its lease view does not count toward `maxViews`.
  - Live leases count toward `max-leases` (§14.2).

## 6. Records

### 6.1 System columns

| Column | Type | Written by | Meaning |
|---|---|---|---|
| `lease_version` | `int64` | the broker on a grant; the client on update and revoke | the grant's offset: the lease id and fencing token |
| `lease_ttl_ms` | `uint64` | the client, raised by the broker to `min_ttl` | the TTL, fixed at grant |
| `lease_holder` | `uint64` | the client | an opaque holder id. Informational only: fencing uses the version. |
| `lease_remaining_ms` | `uint64` | the broker, on `Checkpoint` only; 0 elsewhere | the checkpointed remaining TTL (§9.5); 0 = none |

### 6.2 Lease operations

Record attribute bits 2–3 are reserved on every typed topic (`kAttrReservedMask = 0xFC`, `RecordBatch.h:43`). On a
lease topic they carry the lease op. On every other topic they stay refused, exactly as now.

| Op | Bits 2–3 | Intent | Who | Meaning |
|---|---|---|---|---|
| `Put` | 0 | `insert` | client | **grant**: only if no live lease (etcd `CreateRevision == 0`); otherwise `KeyExists` |
| `Put` | 0 | `update` | client | **update**: replace the row's user columns, fenced by `lease_version`; does **not** keep the lease alive |
| `Put` | 0 | `upsert` | — | refused, `InvalidRequest` ("no lease takeover") |
| `Revoke` | 1 | `update` | client | **revoke**, fenced by `lease_version` |
| `Expire` | 2 | 0 | broker | **expire**: judged on `lease_version` |
| `Checkpoint` | 3 | 0 | broker | **checkpoint**: sets `lease_remaining_ms`; judged on `lease_version` |

- **Every record carries the full row**, so a filtered watcher receives the end of every lease it saw start. The
  broker copies the row from the lease view into `Expire` and `Checkpoint`, re-encoded with the latest writer
  version.
- **The broker re-encodes every `Revoke` (Coordinator's call, R9).** A client revoke carries only the key and
  `lease_version` that matter (`LeaseSession.close()`, `release()` and `resign()` hold nothing else); its other
  columns are ignored. The write hook replaces the record with the lease view's row for that key, re-encoded with the
  latest writer version, in the grant's scratch-copy patch (§6.3), and recomputes the CRC32C. The re-encoded record
  carries the row's `lease_ttl_ms` and `lease_remaining_ms` (amended 2026-11-08), so it matches the row exactly.
  - If the lease view has no row for the key, or a different version, the record goes through unchanged and the
    judge answers `kErrLeaseNotFound`.
  - If a client `Update` or `Revoke` for the key is not yet judged (`lastAppended[k] >= judgedThrough`), or the lease
    index is not ready (§7.1), the lease view's row may be older than the log. The hook then answers the revoke
    `Throttled` (53) with a 1 ms retry-after (`kLeaseRetryAfterNs`, call), and the client library resends it. Only a
    revoke sent back-to-back behind the holder's own update, or one sent during a leadership rebuild, meets this
    (amended 2026-11-08: the bound is the judge's `judgedThrough`, not the fold cursor).
  - An update and a revoke of the same key in one batch are refused `InvalidRequest`.
- **Refusals.** Plain deletes (an empty value, `remove`, `removeExisting`) are refused with "use revoke". So are a
  client `Expire` or `Checkpoint`, and the `upsert` intent.

### 6.3 The version is the grant's offset

- **Stamping.** On a grant the write hook stamps `lease_version` with the offset the record is about to get. It runs
  on the partition's single writer, immediately before `appendNoSyncRestamp` (`LogSegment.h:122`). It uses the
  autoincrement scratch-copy patch (checked-writes §6.4) and recomputes the CRC32C.
- **Zero-copy.** Updates are not patched, so they stay zero-copy. Revokes are re-encoded (§6.2, R9), at the cost of
  one grant-sized patch.
- **Why it is safe.** Offsets strictly increase per partition. Truncation can reuse only uncommitted offsets. Lease
  writes need `acks=view`, and a verdict implies commit, so no client ever held a version that truncation took back.
- **The client learns its version from the ack.** The `View` ack gains a per-record `leaseVersion`.
  - **In the dedupe window,** an idempotent retry returns the original base offset (`ProducePathImpl.h:205-212`), so
    the same version.
  - **A known-old duplicate** (outside the window, `baseOffset = -1` today) was stored and judged long ago. It is
    answered per op from the lease view, never by matching `lease_holder` (Coordinator's call):

    | Duplicate of | `(key, version)` live now | not live | Why |
    |---|---|---|---|
    | grant | `VerdictUnknown` (status 14) | `VerdictUnknown` | the client never learned a version, so nothing proves the live row is its grant. The caller reads the key; `LeaseSession` and the recipes treat it as not granted and grant a new key (§12.3). |
    | `Put`+`update` | `Ok` | `kErrLeaseNotFound` | the client learned `version` from the grant's ack, after the grant folded. Live now means live when the original was judged, so it was accepted. |
    | `Revoke` | cannot happen | `kErrLeaseNotFound` | a version is never reused, so a judged revoke of `(k, v)` leaves `v` ended either way. For a revoke this answer means "that lease is over", and the client treats it as done. |

    **As implemented (amended 2026-11-08, Task 5c).**
    - The duplicate is answered at produce from the lease index, with replay status 3, and its ack still waits on
      the usual `View` fire condition.
    - On the wire a grant duplicate carries `DuplicateSequence` (21) as its per-record verdict. The client maps 21 on
      a lease grant to `VerdictUnknown` (Task 14).
    - A live update carries no verdict (accepted). Any other update or revoke carries `LeaseNotFound` (62).
    - Every lease-trailer entry of a known-old duplicate is `{version -1, ttl 0}`.
    - While the lease index is not ready, or the sink's verdict arena is full, the duplicate is answered `Throttled`
      (53) with a 1 ms retry-after.
  - **The lease ack trailer (amended 2026-11-08, Task 5c).** For a session at `kClientVersionLeases` (18) or above, a
    `View` ack on a lease topic carries one `{lease_version, lease_ttl_ms}` entry per record.
    - It is filled when the `View` ack fires, from the stored batch and the batch's rejected-record list.
    - A grant gets its own offset and its clamped TTL. An update gets the row's TTL while the row still holds that
      version, otherwise the record's. A rejected record gets `{-1, 0}`.
    - The run lives in the sink's preallocated verdict arena (2n 8-byte units), so it needs no allocation. An error
      ack frees the run and carries no trailer. A session below 18 gets no trailer.

### 6.4 Write rules (the write hook; each refusal is definite)

| Check | Answer |
|---|---|
| the session's announce has not reached the partition's worker yet | `NotLeader` (retriable, nothing stored) |
| session below `kClientVersionLeases`, or cluster below `kFeatureLevelLeases` | `InvalidRequest` |
| acks without the `View` flag | `InvalidRequest` ("lease writes need acks=view") |
| a non-idempotent producer | `InvalidRequest` (an explicit-key intent needs an idempotent producer and `View` acks, as in checked writes) |
| more than `kMaxLeaseBatchRecords` (2048) records in the batch | `InvalidRequest` |
| `upsert`, `insert` on a `Revoke`, a client `Expire` / `Checkpoint`, an empty value | `InvalidRequest` |
| `lease_remaining_ms ≠ 0` on a client record | `InvalidRequest` |
| update or revoke with `lease_version < 0` | `InvalidRequest`. Version 0 is valid: a partition's first grant has offset 0 (amended 2026-11-08). |
| an update and a revoke of the same key in one batch | `InvalidRequest` |
| grant with `lease_ttl_ms > lease.max_ttl` | `kErrLeaseTtlTooLarge` |
| grant with `lease_ttl_ms < lease.min_ttl` (0 included) | **not refused:** raised to `min_ttl` in the same scratch patch |
| update or revoke whose `lease_ttl_ms` differs from the live row's | not checked; the judge keeps the row's TTL |
| any grant while the namespace's view memory is over its limit | `NamespaceLimitExceeded` (52). Every grant, not only a new key's: the hook does no index lookup, and a grant of a live key would be `KeyExists` anyway (amended 2026-11-08). |
| a new-key grant while the node is over `max-leases` | `NamespaceLimitExceeded` (52) (Task 21) |
| namespace `ReadOnly` or `RejectAll` | `NamespaceSuspended` (51), as for any write |
| transactional produce | `InvalidRequest` |

**Order (amended 2026-11-08).** Batch-wide rules come first, in this order: announce, client version and feature level,
`View`, transaction, record count. Then the records are walked in order and the first failing record decides the
answer for the whole batch. Each refusal is definite and stores nothing. Namespace suspension is refused before the
hook, as for any write.

After the checks, the hook stores the batch's last offset in the `lastAppended` slot (§9.3) of each key the batch
**updates or revokes**, on the same worker, before the append. Grants do not touch it (Coordinator's call, R8). A
grant of a live key is judged `KeyExists` and changes nothing, so it cannot make a broker record stale. If grants
counted, contenders racing for a held slot (Herald 02 §8.3) could keep `lastAppended` ahead and defer an `Expire`
forever, breaking G3.

### 6.5 Transactions

Transactional produce to a lease topic is refused, as checked writes refuse it on constrained sources (checked-writes
§11.2).

### 6.6 Tombstones, checkpoints and compaction

- **Tombstones.** On a lease topic, `Revoke` and `Expire` are tombstones. One predicate,
  `isTombstone(record, topicTraits)`, replaces `value.empty()` at every site that sees lease records:
  - `LogCleaner.h:401`;
  - `ViewFold.cpp:177/343/377`;
  - `AggFold.cpp:628`;
  - `VerdictSource.cpp:36` and `:96`;
  - `Subscription.h:131-132`.

  The plan greps every other `value.empty()` caller and routes it, or proves the surface refuses lease topics.
- **Checkpoints are invisible to data folds.** A second predicate, `isLeaseControl(record)`, makes user-view folds and
  aggregate folds skip `Checkpoint` records. The client library's subscriptions drop them before the handler. Only
  the LeaseIndex judge applies them.
- **Compaction (amended 2026-11-08: the grant is kept).** For each live key the cleaner keeps three records:
  - the grant record of the live version (the record whose offset equals `lease_version`);
  - the latest `Checkpoint` whose `lease_version` equals the live version;
  - the latest non-`Checkpoint` record, as today.

  A `Checkpoint` never supersedes the data row, and a later grant or tombstone supersedes the old grant and
  checkpoint. The grant must stay because the judge keeps the row's TTL on update (§6.4): a log compacted to only the
  latest update would rebuild that update's TTL instead of the grant's. This is a merge gate for the branch (Task 8).
  - So a lease view rebuilt from a compacted log has every live lease, its row, its version and its checkpointed
    remaining time.
  - **How the cleaner reads it (Coordinator's call).** `LogCleaner::isTombstone(string_view)` (`LogCleaner.h:401`)
    sees only the value, so the cleaner gets a lease-topic mode and does no schema decode:
    - The lease op is in the per-record attributes, bits 2–3 (§6.2).
    - `lease_version` sits at a **fixed offset**: value bytes `[4, 12)`, little-endian `int64`. The broker places the
      four system columns first in the fixed block, in §6.1's order (`lease_version`, `lease_ttl_ms`, `lease_holder`,
      `lease_remaining_ms` at block offsets 0, 8, 16, 24), in every writer version. They can never be removed or
      reordered (`lease_*` names are refused, §5.1). The codec recomputes offsets per version from the live fields
      (`WriteState.h:188-193`), so fields that stay first keep offset 0. `TopicWriteSpec::installVersion` asserts the
      layout for every version of a lease topic, and refuses a version that breaks it.
    - In lease mode the cleaner keeps a second `OffsetMap` for checkpoints, holding the latest `Checkpoint` per key
      and its version. A checkpoint survives only if its version equals the latest data record's version and that
      record is not a tombstone.
    - The tombstone delete horizon is the cleaner's own KIP-534 stamp (`LogCleaner.cpp:907-939`), never a record
      timestamp. The legacy `nowNs` path (`:942-947`) must not run for a lease topic: broker records carry
      timestamp 0 (§9.1). The plan asserts `nowMs ≥ 0` there.
  - A rejected record is removed as in checked writes (§8.5).
- **Consumers.**
  - `record.op()` is `Delete` for `Revoke` and `Expire`.
  - `record.leaseOp()` returns `Granted` (when `lease_version == record.offset()`), `Updated`, `Revoked`, `Expired` or
    `Checkpoint`. The client library hides `Checkpoint`.
  - Keepalives are never visible, as in etcd, where `LeaseRenew` produces no watch event.

## 7. The lease index and its judgments

### 7.1 The lease view

- **What it is.** `<topic>.leases` is a catalog `ViewObject` of kind `VerdictViewKind::LeaseIndex`.
- **Partition-aligned.** There is one instance per source partition, on that partition's leader, like the key index.
  A point read is routed by the topic's partitioner; this routing is new work.
- **Rebuilt per leadership** from the local log, like `rebuildKeyIndex_` (`WorkerViewSet.cpp:1127-1143`), and not
  checkpointed.
- **Member 0 (call).** It is member 0 of the topic's constraint set and provides the key index. `topicHookFactsOf`
  (`CheckedWriteHook.h:57`) changes from `kind == KeyIndex` to `providesKeyIndex(kind)`.
- **Columns.**
  - every user column and system column of the live row;
  - `lease_granted_offset` (= `lease_version`);
  - `lease_updated_offset`, the offset of the last accepted record for the key.

  `lease_remaining_ms` holds the last accepted checkpoint's value. Deadlines are not columns: they live in the wheel
  (§10), so a rebuild of the same log gives the same rows.
- **Reads.**
  - `getViewRow` / `getViewRowK` with `minOffset`;
  - `ScanLeases` (§8.3);
  - `LeaseTimeToLive` (§8.2).

  All are served under the view-read lease and the readiness gate, and authorised as `READ` on the topic (new
  mapping).
- **Readiness (amended 2026-11-08).** The lease index is ready for a partition only when every one of these holds:
  - this node leads the partition, and its high watermark is confirmed;
  - the judge is activated for the current leadership generation, and the view folded under that same generation
    and the current lease layout;
  - the fold has caught up with the judge (`foldCursor >= judgedThrough`);
  - the judge is past every update the index could not record (`judgedThrough > leaseUntrackedThrough`).

  `leaseUntrackedThrough` is set at activation and raised when an update is appended that the index cannot track (no
  table yet, or no row for the key). The write hook, the revoke throttle and the known-old duplicate answers use the
  same check.

### 7.2 Judgment table (at fold, on the leader, in log order)

| Record | row absent | row present, versions equal | row present, versions differ |
|---|---|---|---|
| `Put`+`insert` (grant) | **accept**: new lease, `version = offset` | `KeyExists` | `KeyExists` |
| `Put`+`update` | `kErrLeaseNotFound` | **accept**: user columns replaced; version, TTL, remaining and deadline unchanged | `kErrLeaseNotFound` |
| `Revoke` | `kErrLeaseNotFound` | **accept**: the lease ends | `kErrLeaseNotFound` |
| `Expire` | defensive `StaleExpiry` | **accept**: the lease ends | defensive `StaleExpiry` |
| `Checkpoint` | defensive `StaleCheckpoint` | **accept**: `lease_remaining_ms` set | defensive `StaleCheckpoint` |

- **Capacity.** A grant past `lease.max_keys` live leases is rejected with `kErrLeaseLimitExceeded`. The live count
  is log-determined, so the verdict is the same on every leader and refold (checked-writes §8.4). Nothing else is
  refused for capacity.
- **Stale broker records never happen.** The emitter only appends what its fold state guarantees to be accepted
  (§9.3). One being judged stale is a bug: it is rejected and marked (so invisible), counted in
  `rune_lease_rejected_total{reason="stale_broker"}`, and logged once. The stress test asserts the counter stays 0.
- **Rejected records** are never folded and never delivered. The cleaner removes them, and their verdicts are durable
  in markers.

### 7.3 What fencing guarantees

- **One version per key.** Each key has at most one live version, and it belongs to the one grant at that offset.
- **Stale holders fail.** An update, keepalive or revoke under any other version gets `kErrLeaseNotFound`, which is
  definite.
- **Tokens only grow.** A later grant of the key has a larger version, across expiry, compaction, leader change and
  restart. A guarded resource that keeps the highest version it has seen is safe against a paused ex-holder.

## 8. Lease requests (not log records)

### 8.1 `LeaseKeepAlive`

- **Request:** `{ topicId, partition, entries[1..256] { key, version } }`, routed to the partition leader.
  `LeaseSession` batches its keys per partition.
- **Response:** `{ final u8, entries[] { index u16, code, ttlMs } }`, where `code` is `Ok`, `kErrLeaseNotFound`,
  `NotLeader`, `NamespaceSuspended`, `Throttled` or `UnknownTopic`.
- **Answered per entry (Coordinator's call).** A request is never held for its slowest entry.
  - The first response carries every entry decided at once. `final = 0` if any entry parked.
  - Each parked entry is answered later, in its own response with the same correlation id and its `index`. The
    response that answers the last parked entry has `final = 1`.
  - The client completes each entry on its own answer. A keepalive request costs one completion per entry, from the
    `LeaseSession`'s preallocated ring.
- **Per entry, on the leader's worker** (it arrives through the gateway's ring, as every request does):

| Lease state on the leader | Action |
|---|---|
| not leader, or lease view not `Ready`, or no view-read lease | `NotLeader` (retriable; the client refreshes metadata) |
| namespace `RejectAll` | `NamespaceSuspended`: the keepalive is refused, so the lease runs out within its TTL. Expiry never pauses (G3). |
| namespace `ReadOnly` | served (call): a keepalive writes nothing, like a read. A read-only tenant's services stay discoverable. |
| row absent, or `version` ≠ the row's | `kErrLeaseNotFound` |
| entry `Armed`, not due, `lease_remaining_ms == 0` | re-arm at `leaseNow + ttl`; `Ok(ttlMs)` |
| entry `Armed`, not due, `lease_remaining_ms > 0` | if the emission gate is open (§9.1: `Ready`, HW at the leadership start, `lastAppended[k] ≤ foldCursor`), append `Checkpoint(remaining = 0)` (etcd's synchronous checkpoint) and go to `Clearing`; otherwise park in `Armed` until the gate opens, then append (R4). After the checkpoint folds, re-arm and answer `Ok`. If a parked revoke folds first, answer `kErrLeaseNotFound`. |
| entry `Clearing` | park on the same `Checkpoint(0)`; answered with the keepalive that started it |
| entry `Checkpointing` (a scheduled `Checkpoint(R)` appended, not yet folded) | park. When `Checkpoint(R)` folds, the entry has `remaining > 0`, so it follows the row above: `Checkpoint(0)`, then `Ok` (R4). |
| entry due (deadline passed) or `Emitting` | park until its `Expire` folds (`kErrLeaseNotFound`) or the leader is demoted (`NotLeader`), as etcd waits on `revokec` / `demotec` |
| any parked entry, on demotion / topic drop | `NotLeader` / `UnknownTopic` |

- **The park table.** It is a per-worker, preallocated, fixed table of 4096 entries (call), indexed through the wheel
  entry. Each entry holds the session's reply slot and the entry `index`. When it is full, the entry is answered
  `Throttled` (53) with a 1 ms retry-after.
- **How long a park lasts (corrected in rev 4).** Not "one commit + fold": a park lasts until the record it waits
  for folds. That record is the entry's `Checkpoint(0)`, its scheduled `Checkpoint(R)`, or its `Expire`. If the
  record is still gated (view not `Ready`, or a client update or revoke in the tail), the park also covers the gate.
  If the partition cannot commit (min-ISR), it lasts until it can, or until demotion. The broker sets no timeout of
  its own (rule 16). The client's keepalive deadline turns a long park into a local timeout, which counts toward
  `onAtRisk` (§12.2); the broker's late answer is then dropped.
- **Why it needs the view-read lease.** A deposed leader that has not noticed must not acknowledge a keepalive (§9.2).
  The read lease (`2026-10-19-view-read-lease-and-sink-plane-design.md` A.3) gives exactly that.
- **Authorisation:** `WRITE` on the topic.
- **Rate.** Keepalives count toward the namespace's `produce-records-per-sec` (one per entry), so a keepalive flood
  is shed with `Throttled`.

### 8.2 `LeaseTimeToLive`

- **Request:** `{ topicId, key }`, routed to the key's partition leader.
- **Response:** `{ code, grantedTtlMs, remainingMs, version, holder }`.
  - `remainingMs = deadline − leaseNow`, rounded down; it is −1 when not found, as in etcd.
  - The answer is advisory: the holder must still use its own margin (§9.2).
- **Authorisation:** `READ` on the topic.

### 8.3 `ScanLeases`

`ScanLeases` is etcd's `LeaseLeases` plus a prefix `Get`, in **hash order** (Coordinator's call, R2). The lease
view is a `ViewTable`, an open-addressing hash table (`ViewTable.h:60-90`) with no key order. Its only prefix support
is a fixed-width `prefixWidth`, which a path prefix cannot use.
- **Request:** `{ viewId, partition, prefix (0–127 bytes; empty = all), resumeHash u64, first u8, maxRows (1–1024) }`.
- **Response:** `{ asOfOffset, rows[], lastHash u64, done u8 }`.
- **The order.** `h(k) = wyhash64(key bytes, seed = the source topic id)` (`rune/node/lease/LeaseHash.h`, pinned by
  `ScanLeasesRegression.ScanHashGoldenRegression`). wyhash, not xxh3: it is the hash the storage and view tables already
  link, so the scan adds no second hash function or dependency. It is the same on every node and for every table
  size, so it survives a leader change and table growth. It is not the table's own probe seed, which is per node.
- **A page.** It holds the live rows whose key starts with `prefix` (filtered on the broker) and whose `h(k)` is
  above `resumeHash` (or any hash when `first = 1`), taking the `maxRows` smallest hashes. It always ends on a hash
  boundary: keys with the same hash go in the same page, so a page may exceed `maxRows` by the ties. `lastHash` is
  the largest hash in the page and is the next request's `resumeHash`. `done = 1` when no live key has a larger hash.
- **`asOfOffset` is exclusive.** It is the fold cursor when the page was built, the first offset **not** reflected
  in the page. The page reflects every record at offset `< asOfOffset`.
- **Cost, stated plainly.** A page is one walk of the whole table on the leader's worker, in one duty cycle so the
  page is a consistent snapshot: O(n) in the table's capacity, about 1–2 ns per slot. A full scan of n keys costs
  O(n · ⌈n / maxRows⌉). That is cheap for the topics that scan (discovery: hundreds; elections: thousands; worker
  slots: ≤ 64 k), and expensive for a 1 M-lease topic, which should be read by key. An ordered secondary index is
  later work (§18).
- **Resume.** Each page resumes after `resumeHash`, at its own, later `asOfOffset`. Churn between pages never
  invalidates the scan.
- **Leader change.** A mid-scan change answers `NotLeader`, and the client resumes on the new leader.
- **Wire (Task 13b).** The gateway routes `ScanLeases` to the SOURCE topic's leader (the view id must be a lease view; any
  other id is answered like a missing one). Rows are `[keyLen u8][key][rowLen u32 LE][row payload]`, whole hash groups
  only, within `kScanLeasesMaxPageBytes`; a first group that alone exceeds it is `MessageTooLarge`. A tie group of more
  than 64 rows beyond `maxRows` is refused `Internal` and counted (`scanTieOverflow`), never answered wrong. The client
  error space has no NotReady: an index that is not ready answers `NotLeader`, as the lease reads do. Every refusal carries
  `done = 1` and an empty page. There is no scan fingerprint: lease evolution is append-only and fixed-width, so every
  AddField grows `rowLen`, and a reader detects a layout change by `rowLen` (the client's `LeasesRow::decode` refuses a
  wrong length). A lease topic whose row would not fit one page (`1 + keyWidth + 4 + rowLen > kScanLeasesMaxPageBytes`)
  is refused at `define` and at `alter ... add` (`InvalidSchema`).
- **Why it is new.** Today's `ScanViewRows` cannot serve it: it routes to partition 0, has no `asOf`, and its cursor
  is invalidated by churn (checked-writes §9).

## 9. Expiry

### 9.1 Who decides and who records

- **Folding is leader-only (Coordinator's call),** as today (`Worker.h:7360-7366`). The leader keeps two intrusive
  wheels next to its lease view:
  - the **deadline wheel**;
  - the **checkpoint wheel**, which holds only leases with `ttl > lease.checkpoint_interval` when checkpointing is
    on.
- **Arming.**
  - An accepted grant arms the key at `foldLeaseNow + ttl`.
  - A keepalive re-arms it (§8.1).
  - An accepted end drops it.
  - An accepted `Put`+`update` changes no deadline.

  `foldLeaseNow` is the `LeaseClock` (`CLOCK_BOOTTIME`), read once per fold batch.
- **Saturating arithmetic (R11).** Every deadline is `min(INT64_MAX, a + b + …)`, computed with saturating adds, in
  the deadline and checkpoint wheels, the promotion rule (§9.6), `LeaseTimeToLive` and the client margin (§12.2).
  `max_ttl` = 9 × 10¹² ms is 9 × 10¹⁸ ns, so `BOOTTIME + ttl + extend` would overflow `int64` after about 7 years of
  uptime. A saturated deadline is "not in this node's lifetime" and lands in wheel level 4.
- **Followers keep no deadline state at all.** Revision 2's shadow wheel is gone, because keepalives are not in the
  log. A promoted follower arms from the etcd rule (§9.6).
- **The emission gate.** The leader emits for a due key only while all of these hold:
  - the lease view is `Ready` under the current generation;
  - its HW has reached its leadership start offset (`ViewInstance.h:43-50`);
  - `lastAppended[k] ≤ foldCursor` (§9.3).

  **Namespace suspension is not in the gate (owner, G3).** Broker `Expire` and `Checkpoint` appends bypass the
  suspension refusal, which applies to client writes only.
- **The gate holds in `Armed` only (R4).** No `Expire` and no scheduled `Checkpoint` is built for an entry in
  `Clearing`, `Checkpointing` or `Emitting`. A due entry in one of those states waits for its record to fold and is
  re-examined in `Armed`.
- **Appending: a data batch, not a marker (Coordinator's call; rev 3's "as `emitMarkers_` does" is withdrawn).**
  `emitMarkers_` uses `appendControlBatch`, which consumers never see, and `PartitionReplica::append` wraps its input
  as the value of a one-record batch (`PartitionReplica.cpp:565-590`). Neither fits. The emitter builds one whole data
  `RecordBatch` of up to `lease.expiry_batch` records (or 1000 checkpoints). Each record has the lease key, the
  re-encoded row (§6.2) and the lease op in attribute bits 2–3. The batch is appended through a new
  `PartitionReplica::appendLeaseBatch(span)`, the data-batch sibling of `appendControlBatch`, with the same role
  gate, min-ISR gate and post-append sequence.
  - **Producer fields:** `producerId = −1`, `producerEpoch = −1`, `baseSequence = −1`, so the batch is not
    idempotent and the dedupe window never sees it. No dedupe is needed: the emitter never retries an append. A
    refused append wrote nothing, the entry stays `Armed` and due, and the next cycle rebuilds the record from fold
    state. An entry leaves `Armed` only on a successful append.
  - **Timestamps:** `firstTimestamp = maxTimestamp = 0` and every record's delta is 0, as `makeBatch_` does for broker
    batches (`PartitionReplica.cpp:74-79`). No wall clock is read, and no lease decision reads the field (G5).
    Consumers see timestamp 0 on `Expired` and the hidden `Checkpoint`.
  - **CRC and epoch:** the emitter computes the CRC32C once, over attributes through the records. The replica
    restamps the base offset in place and stamps the leader epoch (`stampLeaderEpoch`). Both precede the CRC region,
    as in `appendControlBatch`, so the CRC stays valid.
  - **Refusals:**
    - `kAppendMinIsr`: nothing is appended. The entries stay due, `rune_lease_expiry_backlog` counts them, and the
      next cycle tries again. While the partition cannot reach min-ISR, no `Expire` could become visible anyway. The
      append uses `acksAll = true`: an `Expire` is majority-durable, like any acks=all write.
    - `kAppendNotLeader`: the emitter stops, so a deposed leader appends nothing.
    - `kAppendBackPressure` cannot happen on a persisted lease topic. On a transient one it can (owner ruling 2026-11-07, top note): design pending.
- **Rule 16.** The TTL is a product semantic, not a convergence timeout. The timer decides only when the leader
  proposes an end. The judged log decides whether it ended. `extend` (§9.6) affects only *when* a promoted leader
  proposes, never *whether* an end is accepted.

### 9.2 Clocks, and why expiry is never early

- **Assumptions.** The read-lease assumptions hold (A.3):
  - C1: drift ρ = 1 %;
  - C2: the lease clock counts suspend;
  - C3: no two nodes' clocks are ever compared.

  The log carries durations and offsets, never instants. There is no wall clock anywhere in this design.
- **The holder's view.** A holder that sent a keepalive at its local time `t` and got `Ok` may treat the lease as held
  until `t + ttl·(1 − 2ρ) − d`, where `d` bounds one-way delay. Sending at `ttl/3` leaves a wide margin.
- **Steady state.** The leader acknowledged the keepalive at a time `a ≥ t`, and arms at `a + ttl` on its own clock.
  That is never early.
- **Across a leader change.**
  - **Rev 3's premise is withdrawn (R3).** Rev 3 assumed a new leader serves only after the old leader's read lease
    has run out. Read-lease A.8 does not wait for it on a move that keeps the old leader in the ISR. Rev 3 also armed
    at `refoldJudgedNow`, which can come before the old leader's last keepalive ack: up to about L(1+ρ) ≈ 202 ms after
    the `LeaderChange`, while `extend` = H ≈ 150 ms.
  - **The new premise (Coordinator's call).** The old leader acknowledges keepalives only while it is leader under
    its epoch and holds the view-read lease. The new leader's lease view enters `Ready` only when its HW has reached
    its leadership start offset (`ViewInstance.h:43-50`). The HW reaches it in one of two ways:
    - the old leader, still in the ISR, has fetched at the new epoch. It demoted before that fetch, so every
      acknowledgement `a` it gave comes, causally, before `T_ready`;
    - the old leader was removed from the ISR. The read-lease move fence (A.8) then waited out its read lease before
      the new leader served, so again every `a` comes before `T_ready`.

    This is causal order through messages, never a cross-node clock comparison (C3).
  - The new leader arms at `T_ready + extend + R` on its own lease clock, where `T_ready` is the time its view entered
    `Ready` and `R` is the checkpointed remaining time or the full TTL. Drift is absorbed by the holder's margin, as in
    steady state.
  - **Without a checkpoint,** `R = ttl`, so the deadline is `> a + ttl`.
  - **With a checkpoint** `R = deadline_old − c` taken at time `c`: every keepalive acknowledged after `c` first
    appended and folded `Checkpoint(0)` (§8.1), which clears `R`. So if `R > 0` is the latest checkpoint the new
    leader sees, every acknowledgement came before `c`, and `T_ready + extend + R > c + R = deadline_old`.
  - If `Checkpoint(0)` was appended but truncated, its keepalive was never acknowledged, because it answers after the
    fold.

  So expiry is never earlier than the deadline any leader acknowledged, for any timing.
- **Checkpoint values round up.** `R` is rounded up to ms (`⌈deadline − now⌉`), so rounding is only ever late.

### 9.3 Why a broker record is never stale

- **The `lastAppended` rule.** The emitter appends an `Expire` or `Checkpoint` for `k`, including a keepalive's
  `Checkpoint(0)`, only if no client update or revoke for `k` sits in the unfolded tail
  (`lastAppended[k] ≤ foldCursor`). Grants are not tracked (R8, §6.4). A grant in the tail meets a live row, is
  judged `KeyExists` and changes nothing.
- **Proof.** At emission the fold has applied every update and revoke for `k`, and any unfolded grant of `k` will be
  refused. So the lease view row is `k`'s state at log end.
  The broker record carries that row's version and folds before anything appended after it, so it is accepted. It can
  be lost only by truncation, which means it never committed.
- **A revoke or update racing an expiry.**
  - **Appended before the `Expire` is built:** the `Expire` waits until it folds. A revoke then drops the key; an
    update leaves it due, and the `Expire` follows next cycle.
  - **Appended after the `Expire`:** the `Expire` folds first. A revoke or update then gets `kErrLeaseNotFound`, which
    is correct, because the lease had run its TTL.
- **A keepalive racing an expiry.** Keepalives and the emitter run on the same worker thread:
  - a keepalive handled before the wheel poll re-arms the key, so no `Expire` is built;
  - one handled after the `Expire` was built parks, and is answered `kErrLeaseNotFound` (§8.1).
- **Liveness.** Only a stream of client updates or revokes of `k` arriving faster than commit + fold can defer an
  `Expire` indefinitely. Both need `WRITE` and the version, so their writer could revoke anyway. Grants from
  contenders, which need no version, cannot defer it (R8). The namespace produce-rate limits
  bound it, and `LeaseSession` never writes to a due key. Updates never renew (G2), so the deferral is never an
  extension: the `Expire` goes out in the first cycle where the tail holds nothing for `k`.

### 9.4 Exactly one end

- **Across a leadership change.** Two ends for one generation can be appended only across a change: leader A appends
  `E` and is deposed before it commits, then B appends `E′`.
  - If `E` reached B's log, B's refold applies it before `Ready`, so B never builds `E′`.
  - If it did not, `E` is truncated from A.
- **On one leader.** A revoke blocks the `Expire` through `lastAppended` until the revoke folds and drops the key.
- **Defence in depth.** The judge's version check rejects any second end.

### 9.5 Checkpoints (etcd's lease checkpoint)

- **Scheduling.** When checkpointing is on and `ttl > lease.checkpoint_interval`, the checkpoint wheel arms the key
  at `grantFold + interval`, and re-arms it after each checkpoint folds. etcd's `scheduleCheckpointIfNeeded` applies
  only when the remaining TTL exceeds the interval, and so does Rune.
- **Emission.** A due checkpoint becomes a `Checkpoint` record with `lease_remaining_ms = ⌈deadline − now⌉`.
  - It passes the same gate as `Expire`.
  - At most `lease.checkpoint_rate` records per one-second window aligned to the leader's ready time (so up to 2x the rate across a
    window boundary) and 1000 per batch are emitted, from the same scratch.
  - It is skipped when the remaining time is ≤ the interval, as in etcd.
- **In flight (R4).** Appending a scheduled `Checkpoint(R)` moves the entry to `Checkpointing{ckptOffset}`. A
  keepalive there parks. Without this state, a keepalive arriving after `Checkpoint(R)` is appended but before it
  folds would still see `remaining == 0`, re-arm and answer `Ok` with no clear. `R` would then fold, and a later
  leader would arm at `T_ready + extend + R`, which can be far earlier than `a + ttl` (a 24 h TTL with R = 10 min).
  When `Checkpoint(R)` folds, the entry returns to `Armed` and the checkpoint wheel re-arms it. A parked keepalive
  then sends `Checkpoint(0)`, through the gate (§8.1).
- **Clearing.** The first keepalive after a checkpoint appends `Checkpoint(0)` and answers after it folds (§8.1).
  So a lease writes at most two records per interval: a checkpoint and a clear. That is etcd's bound.
- **One-shot long leases** (a sanction's expiry, an appeal token) are granted once and never kept alive. They write one
  checkpoint per interval and are compacted to one. 1 M such leases on one partition at a 5 min interval need
  3 333 checkpoints/s, above the default rate. A lagging checkpoint only carries an older, larger remaining time, so
  a later leader arms it later, never earlier. Sizing: raise `lease.checkpoint_rate`, or add partitions.
- **A lagging checkpoint is safe for any timing.** It stores what the leader measured at emission. After it, the
  deadline only moves later (a keepalive clears it).

### 9.6 Leader change, restart, catch-up (etcd `Promote` / `Demote`)

**Wheel entry states.** Each leader wheel entry is node-local and indexed by lease-view row id:

| State | Fields | Entered when | Leaves when |
|---|---|---|---|
| `Armed` | `deadlineNs`, `checkpointNs` | grant fold, keepalive, promotion, the end of `Checkpointing` | it falls due and passes the gate (→ `Emitting`); a checkpoint falls due and passes the gate (→ `Checkpointing`); a keepalive with a checkpoint passes the gate (→ `Clearing`) |
| `Checkpointing` | `ckptOffset`, park index | it appended a scheduled `Checkpoint(R)` (R4) | that checkpoint folds (→ `Armed`; parked keepalives then go through the `Checkpoint(0)` row of §8.1); demotion (discarded, `NotLeader`) |
| `Clearing` | `clearOffset`, park index | it appended `Checkpoint(0)` for a keepalive | that checkpoint folds (→ `Armed`, re-armed, the keepalive answered); demotion (discarded, `NotLeader`) |
| `Emitting` | `emitOffset` | it appended `Expire` | the `Expire` folds (freed; parked keepalives get `kErrLeaseNotFound`); demotion (discarded, `NotLeader`) |

Only `Armed` emits (§9.1). In the other three states the entry has a record in flight. A deadline that passes there
is acted on when the entry is `Armed` again, so it is late, never early.

**Arming on promotion (etcd `Promote`; Coordinator's call, R3).** At the lease-clock time `T_ready` when the
lease view enters `Ready`, the new leader arms every live key at:

`T_ready + extend + (lease_remaining_ms > 0 ? lease_remaining_ms : lease_ttl_ms)` (saturating, §9.1)

where `extend = leaderHeartbeatTimeoutNs`, etcd's `extend = electionTimeout`. Emission starts at `Ready`.
- **The pass.** It links the entries in chunks of 4096 per duty cycle (call), so a 1 M-lease partition never stalls
  its worker. A key not yet linked cannot expire, which is late, never early. A keepalive for such a key is served as
  for `Armed`, and links it.
- **Why `T_ready`, not the refold time.** §9.2: every acknowledgement of the old leader comes before `T_ready`.

| Event | Effect | Bound |
|---|---|---|
| a follower becomes leader | it rebuilds the lease view and arms every key by the rule above; it emits from `Ready` | never early (§9.2). An abandoned lease ends by `T_ready + extend + R + tick + cycle + commit`. |
| a leader is demoted | its wheels and park table go with its view; parked keepalives get `NotLeader` | no lost expiry: the new leader arms everything |
| a whole-cluster restart | the same rule on the first leader | the same bound. A long lease keeps its checkpointed remaining time, so a 400-day sanction is late by at most `extend + one checkpoint interval`. |
| a follower catches up, or a divergent follower is truncated | nothing: followers hold no deadline state | — |
| topic dropped | wheels, park table and view released; no `Expire`. Every parked keepalive is answered `UnknownTopic` when the drop applies on the leader (Coordinator's call); `LeaseSession` reports `onLost(reason = TopicDropped)` | consumers get `UnknownTopic` |
| namespace suspended (`ReadOnly`, `RejectAll`) | expiry and checkpoints continue. `RejectAll` refuses keepalives and writes; `ReadOnly` refuses writes and serves keepalives. | G3 |
| namespace resumed | nothing to do: nothing paused | — |
| namespace deleted | the topic goes with it | — |

### 9.7 Expiry can never stall

The checked-writes judge stalls on:
- a full rejected index (`VerdictSource.cpp:381-383`);
- a member's capacity (`:373-374`);
- a `MarkerLedger` of 1024 uncommitted markers.

None of these can stop expiry:
- **Broker records take no rejection capacity.** `Expire` and `Checkpoint` are always accepted (§9.3).
- **The lease view never stalls on capacity.** Its rows are reserved up to `lease.max_keys`, and a grant past it is a
  verdict, not a stall.
- **Client rejections give their capacity back without any client's help.**
  - They are stale updates, revokes and failed grants. They must be marked (checked-writes §8.5–§8.6).
  - The index grows at 75 % (`growOwed`, `:385`) and is thinned by the cleaner.
  - The ledger is full only while the partition cannot commit, and then no `Expire` could become visible anyway.
  - Each rejection costs a client a produce against its rate limit.
- **Keepalives use no log capacity at all.** The exception is `Checkpoint(0)`, a broker record, which is always
  accepted.
- **The emitter never waits on a full structure.** Its scratch is fixed (§10), and a full batch continues next cycle.

## 10. The wheels

- **Shape.** The wheel is hierarchical and hashed (after Agrona's `DeadlineTimerWheel`), with **five levels** of 1024
  slots at a 1 ms base tick:

| Level | Tick | Covers |
|---|---|---|
| 0 | 1 ms | 1.024 s |
| 1 | 1.024 s | 17.5 min |
| 2 | 17.5 min | 12.4 days |
| 3 | 12.4 days | 34.8 years |
| 4 | 34.8 years | 35 600 years |

  That covers `MaxLeaseTTL` (285 years), and a saturated `INT64_MAX` deadline (§9.1). An entry cascades at most four
  times in its life.
- **Intrusive by row id.** `ViewTable` row ids are dense and stable. One 48-byte entry per live lease holds:
  - `deadlineNs`, `checkpointNs` (`lastAppended` stays in the lease table's aux, written by the produce hook; it is not in
    the entry);
  - the deadline-wheel links and the checkpoint-wheel links (`u32` each);
  - `level` ×2, `state`;
  - `emitOffset` / `ckptOffset` / `clearOffset` (one field: an entry is in at most one of those states), and the park
    index.

  Arm and re-arm are O(1) unlink plus link. There is no hash and no allocation.
- **Storage.** Entries are preallocated in chunks of 4096 on the cold path at 75 % use (the `growOwed` pattern), never
  past `lease.max_keys`, and never freed while the partition lives. 1 M leases need 48 MiB.
- **Cost.**
  - A keepalive entry is one lease-view lookup plus one re-link, under 100 ns on the worker.
  - `poll(nowNs)` is O(due + slots passed), and only on lease partitions. An idle partition costs one compare.
- **Throughput gate.** At least 100 000 `Expire` records/s appended per partition. A 10 000-lease mass expiry drains in
  40 cycles at the default batch.
- **Scratch.** One per-worker scratch of 1 MiB, shared by `Expire` and `Checkpoint` batches. A batch is at most
  `min(1 MiB, namespace maxBatchBytes when set, the broker's produce-intake batch limit)` bytes (`Worker.h` `NsHot`: the
  default 0 means unlimited). The intake term is the Coordinator's call (Task 10): a lease batch is never larger than a
  batch the broker accepts from a client, so it always replicates.

## 11. Rune QL and CLI

```sql
show leases 'herald-sys/discovery';                                   -- ScanLeases, every partition, limit 100
show leases 'herald-sys/discovery' where path like 'herald/eu/_shared/COORDINATOR_SERVICE/%';   -- prefix scan
show lease 'acme/herald.safety.case-claims' key 7;                    -- LeaseTimeToLive: granted, remaining, version
revoke lease 'acme/herald.safety.case-claims' key 7;                  -- reads the version, then a fenced Revoke
show topic 'acme/herald.safety.case-claims';                          -- lease settings and the live count
```

- **`revoke lease`** reads the live version and sends a fenced revoke. If the lease changed between the two, it
  reports `kErrLeaseNotFound`, and the operator runs it again. There is no force revoke: every end is fenced.
  Revision 2's version-0 force revoke is removed.
- **Access.** `show` needs `READ`, and `revoke lease` needs `WRITE`.

## 12. Client API (C++)

### 12.1 `LeaseTopic<T>`

**Async first (Coordinator's call).** Every lease operation follows the client's F8c pattern (`Completion.h`). It
takes a target (a `Completions&` queue that the owning thread polls, or a `CompletionHandler {fn, ctx}`) and a
`Token`. It is either rejected synchronously, or accepted, and then exactly one `Completion` follows. Nothing blocks,
nothing allocates, and there is no `std::function`. The `*AndWait` forms are thin wrappers for tools and tests, and
an agent thread never calls them (Herald 02 §3.3).

```cpp
// async: open, then ops; each completes once on `done` with the token given
client.openLeaseTopic<gen::CaseClaim>("acme/herald.safety.case-claims", done, tOpen);   // OpKind::OpenLeaseTopic
LeaseOptions opts{.ttl = 30min, .holder = nodeId};              // ttl unset = the topic's lease.ttl
claims.grant(row, opts, done, tGrant);        // LeaseGrant:   Ok(value = version, aux = ttlMs) | KeyExists | TtlTooLarge | VerdictUnknown
claims.update(row, version, done, tUpd);      // LeaseUpdate:  Ok | LeaseNotFound   (does not keep alive)
claims.revoke(key, version, done, tRev);      // LeaseRevoke:  Ok | LeaseNotFound   (row re-encoded by the broker, §6.2)
claims.keepAliveOnce(key, version, done, t);  // LeaseKeepAlive: Ok(aux = ttlMs) | LeaseNotFound
claims.timeToLive(key, done, t);              // LeaseTimeToLive: payload = granted, remaining (-1 = none), version, holder
claims.get(key, minOffset, done, t);          // GetViewRowK on the lease view

// blocking wrappers (tools, tests)
auto claims = client.openLeaseTopicAndWait<gen::CaseClaim>("acme/herald.safety.case-claims");
LeaseResult g = claims.grantAndWait(row, opts);
std::optional<gen::CaseClaimLeasesRow> cur = claims.getAndWait(key, /*minOffset=*/g.offset());

// Herald's tenant delete grace (herald-sys, §19.6): one-shot, 7 d, checkpointed; CancelDeleteTenant revokes it
graces.grant(gen::DeleteGrace{.nsId = ns}, LeaseOptions{.ttl = 7 * 24h, .holder = provisionerId}, done, tGrace);
```

- **New `OpKind`s:** `OpenLeaseTopic`, `LeaseGrant`, `LeaseUpdate`, `LeaseRevoke`, `LeaseKeepAlive` (one completion
  per entry, §8.1), `LeaseTimeToLive`, `ScanLeases`, `LeaseCampaign` (`LeaseElection::campaign` / `LeaseOwnership::claim`
  reaching ownership), `LeaseWatchReady` (`startFromNow`'s scan finished and the watch is live).
  - `value` is the version for `LeaseGrant`, and the `asOfOffset` for `ScanLeases` and `LeaseWatchReady`.
  - `aux` is `ttlMs`.
  - `errorCode` is the broker code.
  - A lease op's `Timeout` is its own duty-side deadline: `LeaseOptions.opTimeout`, default `ttl/3`.
- **Acks.** Writes send `Acks::All | Acks::View`. `LeaseResult` carries `version()` from the ack's `leaseVersion`,
  and the raised `ttlMs()`.
- **Errors.** `kErrLeaseNotFound`, `kErrLeaseLimitExceeded` and `kErrLeaseTtlTooLarge` are typed. `KeyExists` is
  checked-writes'. All are definite. A grant's `VerdictUnknown` means a known-old duplicate (§6.3): the caller reads
  the key.
- **Generated helpers.** The generated `T` gets `withLease(version, ttlMs, holder)`. There is no setter for
  `lease_remaining_ms`.
- **Gating.** The client gates on `kBrokerVersionLeases`.

**As built (Task 14a).**

- **OpKinds**, appended after `GetStats`: `OpenLeaseTopic, LeaseGrant, LeaseUpdate, LeaseRevoke, LeaseKeepAlive,
  LeaseTimeToLive, ScanLeases, LeaseCampaign, LeaseWatchReady`. Payload and fields per kind:

  | OpKind | `value` | `aux` | `reserved0` | payload |
  |---|---|---|---|---|
  | `OpenLeaseTopic` | topic id | `schemaAuxOf` | partition count | descriptor blob + key features + lease block (spec + `i32` lease-view topic id) |
  | `LeaseGrant` / `LeaseUpdate` | lease version | `ttlMs` | 0 | none |
  | `LeaseRevoke` | 0 | 0 | 0 | none |
  | `LeaseKeepAlive` | lease version | `ttlMs` | 0 | none |
  | `LeaseTimeToLive` | version | remaining ms | 0 | 32-byte `LeaseTtlInfo` |
  | `ScanLeases` | `asOfOffset` | last row hash | `rowCount \| done << 31` | the packed rows |
  | `LeaseCampaign`, `LeaseWatchReady` | reserved for Tasks 15-16 | | | |

- **Ack trailer to completion.** An accepted entry is `Ok`. A missing trailer entry is `StoredIdUnknown` (counted).
  A per-record verdict is `RecordsRejected`. Verdict 21 on a grant is `VerdictUnknown`. A batch error is
  `BrokerError`. A throttled batch (53) is re-sent.
- **Schema answer.** The lease block of `TopicSchemaResponse` is the 39-byte `LeaseSpec` plus an `i32`
  `leaseViewTopicId` (43 bytes); a 39-byte block without the id decodes with `-1`.
- **Write timeout (deviation).** Writes use a dedicated producer whose `produceTimeoutMs` and `deliveryTimeoutMs`
  both equal the handle's `opTimeoutMs` (default `ttl/3`, floor 1 ms). A per-call `LeaseOptions.opTimeoutMs`
  applies to the routed requests (keep-alive, time-to-live, scan) only. A write's `DeliveryFailed` is reported as
  `Timeout`.
- **Writes** accept a `Completions` queue only; `revoke` takes the full key row.
- **Generated helpers (Task 14b).** `generateHeader(evolving, version, nspace, isLease)`; `rune_schema_gen` passes `isLease` when
  the schema reply carries the lease block. With `isLease` the generator requires the first four fields to be exactly
  `kLeaseColumns` and every field fixed-width (else `std::invalid_argument`) and emits: `T& withLease(version, ttlMs, holder)`
  (sets the three columns and `lease_remaining_ms = 0`); no `lease_remaining_ms` setter on `Encoder` / `ClaimEncoder` (the
  member and the `Decoder` accessor stay); and `<T>LeasesRow` (alias `T::LeasesRow`) with a public member for every field live
  in `T`'s version plus `int64_t lease_granted_offset, lease_updated_offset`, `kRowLen`, and a constants-only
  `static std::optional<TLeasesRow> decode(span)` that walks the broker's row layout (null bitmap, one slot per field ever
  declared, dropped ones skipped, then the two offsets): a wrong length is `nullopt`, a null live slot is the zero value.
  `LeaseTopic<T>::getAndWait(row, minOffset = -1, timeout)` returns `std::optional<T::LeasesRow>` (absent key: `nullopt`;
  any other non-Ok answer throws `std::runtime_error`); `decodeRow(span)` forwards to `LeasesRow::decode` for the async
  `get`. Both `static_assert` on a type without `LeasesRow`. A header generated without `isLease` is byte-identical to before.
- **Subscriptions.** `Record::op()` is `Delete` for Revoke and Expire; `Record::leaseOp()` names the five forms.
  A `Checkpoint` record is dropped before the handler while the position still advances (plain subscriptions;
  group consumers do not deliver attributes).

### 12.2 `LeaseSession<Capacity, OnLost, OnAtRisk>` (etcd `concurrency.Session`)

**Untyped and multi-topic (Coordinator's call, R7).** A preallocated, fixed-capacity set of leases on **one** client
that one thread keeps alive, driven by `doWork(nowNs)` (the Invoker pattern). Herald keeps election keys
(`herald.elections`, 1 partition) and worker slots (`herald.workers`, 8 partitions) in one session. A process may
hold one session per client, for example one per tenant client for case claims (Herald F19).
- **`add(topic, partition, keyBytes, version, ttlMs) → LeaseRef`** takes over keepalives for a lease this client
  granted. `keyBytes` is at most 128 bytes (§5.1). `remove(ref)` stops keeping it alive without a revoke.
  `revoke(ref)` sends a fenced revoke and then removes it.
- **Completed correlation ids.** The gateway keeps no per-request state: after it answers a malformed non-final keepalive return with a final
  `Internal`, later returns for that correlation id are still forwarded, so the client drops any lease frame for a correlation id it has
  already completed.
- **Keepalive.** Each key is kept alive at `ttl/3`, batched per `(topic, partition)` into one `LeaseKeepAlive` of up
  to 256 entries, spread rather than bursty, through a preallocated completion ring. Answers arrive per entry (§8.1).
- **Ends from a `LeaseWatch` (optional).** `session.observe(watch)` connects a `LeaseWatch` (§12.4) over one of the
  session's topics. A delivered `Revoked` / `Expired` for a held `(key, version)` then fires `onLost` at once. Without
  a watch, the session learns ends only from keepalive answers and its margin. Several sessions and recipes share one
  watch per topic.
- **The clock.** `nowNs` is the client's injected monotonic clock: `CLOCK_BOOTTIME` on Linux, which counts suspend
  (C2), and `mach_continuous_time` on macOS. A holder's margin never uses a clock that stops in suspend.
- **The margin.** For the last keepalive sent at `t` that was answered `Ok`, the lease is held until
  `M = t + ttl·(1 − 2ρ) − d` (§9.2), computed with saturating arithmetic.
  - `d` comes from `LeaseOptions.maxDelay`, default 100 ms (call). It bounds one-way network delay plus the broker's
    ingress-to-append time, which is what a guarded write needs (Herald 02 states its own number).
  - It is validated at `add`: `ttl/2 + d < ttl·(1 − 2ρ)`, otherwise `InvalidArgument`. With `min_ttl` = 1 s, that
    allows up to d ≈ 480 ms.
- **`onAtRisk(ref, edge)` (Coordinator's call).**
  - **When.** `Entered` fires when `nowNs ≥ t + ttl/2` with no newer `Ok` (`LeaseOptions.atRiskFraction`, default
    1/2; it must leave `atRisk + d < M`), or at `M − d` if that is earlier. Every cause counts: a retriable error
    (`NotLeader`, `Throttled`, `NamespaceSuspended`), a timeout or missing reply, a disconnect, or a long park.
  - **Not terminal.** The session keeps sending keepalives. An `Ok` before `M` fires `Cleared`, and the lease was held
    throughout. If no `Ok` comes by `M`, `onLost` follows. `Entered` fires at most once per episode.
  - **Produce deadline (R5).** A producer can be bound to a lease (`ProducerOptions.guard = ref`, up to 8 producers
    per lease, on the session's client).
    - At `Entered`, before the callback runs, the client fails every **pending** produce of the bound producers with
      `OpStatus::DeliveryFailed` and `errorCode = LeaseAtRisk`. A pending produce was sent and is not acked, so its
      outcome is unknown: it may still be appended within `d` of its send. The client fails every **unsent**
      produce the same way; those were never sent.
    - From then on the client never sends or re-sends anything for those producers (no reconnect retry) until
      `Cleared`.
    - So every write of a guarded producer that can reach the log was sent before `Entered`, and is appended by
      `Entered + d < M`, which is before the key can expire. Herald 02 §4.3 relies on this.
    - `onLost` does the same if it comes first.
  - **Election.** `LeaseElection` does not resign at `Entered`. It suspends: `isLeader()` is false and the user's
    callback runs. It resumes at `Cleared` if its key is still the lowest. It resigns only on `resign()` or
    `close()`. A caller that wants stricter behaviour revokes and campaigns with a new key, as Herald F1 does.
- **`onLost(ref, reason)` ≈ `Session.Done()`.**
  - It fires on `kErrLeaseNotFound`, on `UnknownTopic`, on a watched `Expired` / `Revoked` for the held
    `(key, version)`, and when `M` passes with no `Ok`.
  - The key is dropped and is not kept alive again.
- **A local `onLost` leaves the key on the broker (stated plainly).** When `M` passes, the session cannot know the
  broker's deadline. It sends one best-effort fenced revoke if a connection is up, but the key may live on the broker
  until its deadline:
  - about `a + ttl` in steady state;
  - up to about `T_ready + extend + ttl` (or `+ R`) after a promotion (§9.6).

  Until then nobody else can grant it, so an election stays without a leader, and a slot without an owner, for up to
  that long. Herald's failover time includes it.
- **`close()`** revokes every held key, fenced (etcd `Session.Close`), as an async state machine. `isClosed()` reports
  when every revoke has answered.
- **No `std::function`.** The callbacks are template functor parameters.
- **Updates are separate.** `update(ref, rowBytes)` sends a fenced `Put`+`update` at once. It is a separate write and
  does not keep the lease alive (etcd: a put does not renew).

- **As built (Task 15) - deviations from the text above.**
  - **`grantSentNs`.** `add(topic, row, version, ttlMs, grantSentNs)` takes the client-clock time at which the grant (or
    the update that produced `version`) was SENT, from `Client::nowNs()`. The margin `M` and the at-risk deadline are
    computed from it, never from the reply time.
  - **`bindProducer` replaces `ProducerOptions.guard`.** `bindProducer(ref, producer)` binds a producer to one lease after
    the fact. At most 8 producers per lease; a 9th, a stale ref, or a producer of another client is refused with
    `InvalidArgument`. The bind itself travels through the producer's own ring (`kMsgGuardBind`), in FIFO order with its
    records; in Direct ingress it is applied in place. A ring-side refusal (unknown session) comes back as a `BindRefused`
    event and the producer stays blocked.
  - **Phase-preserving spread.** An entry's cadence is its own phase (`grant + k * ttl/3`). After a send the phase moves on
    by whole periods past `now + ttl/12` (the early window in which a request may carry an entry that is not yet due); it
    is never re-anchored to the send time, so a spread fleet does not collapse into one burst.
  - **`LeaseAtRisk` = 0xFF01.** A client-local error code (`ClientErrorCode::LeaseAtRisk`, not a broker code) carried by
    the `DeliveryFailed` completions of writes that the produce guard fails when a lease trips.
  - **Revoke template row.** The fenced revoke of a typed topic is a row encoded once from a default-constructed `T`
    (`setRevokeTemplate`, stored at `open`), so the session revokes by key alone and never needs the user's row. `T` must
    be default-constructible.
  - **A producer of an ended lease stays blocked.** After `Ended` (the lease left the session by loss, `remove`, or `close`)
    the bound producer does not resume: its writes keep failing with `LeaseAtRisk`. After `Tripped` it resumes only at
    `Rearmed` (`Cleared`).
  - **Guard events: one pool, ack and coalescing.** The duty thread posts guard events (Reserved, ReserveFailed, Tripped,
    Rearmed, Ended, BindRefused) into the session's queue from one session-level pool of `P = 2 * Capacity + 8` credits,
    reserved at construction and returned in the destructor after `GuardRelease`. Guard completions carry 0 units and 0
    bytes on the consumer side. The duty side posts only while `posted - acked < P`; the session sends `GuardAck(consumed)`
    at most once per `doWork`. Each slot keeps at most two pending edges, so Tripped, Rearmed, Tripped, Rearmed collapses to
    Tripped, Rearmed with the latest `armSeq`. The queue therefore never overflows however many edges occur between two
    session passes.
  - **Multi-entry keepalive credit.** The session reserves `48 * (1 + N)` bytes for a request of `N` entries. Each entry's
    completion returns 48 bytes (`reserved0` = the entry index); the terminal (`reserved0` = 0xFFFF) returns
    `48 * (1 + unanswered)`. A duplicate or out-of-range index is dropped. A whole-request answer is expanded by the
    client into per-entry completions for the entries not yet answered, then a terminal `Ok`.
  - **Token layout.** Tokens are limited to 48 bits: kind in bits 44-47 (keepalive, best-effort revoke, close revoke), the
    request index or slot in bits 32-43, the generation in the low 32 bits. `Capacity` is therefore at most 4095.
  - **No `offerAttempt_` check.** The recipe's check on the transport offer attempt is unreachable, because the guard blocks
    the producer before any re-send, and was not built. Its mutation (skip the guard's block in `appendRecord_`) turns
    `GuardBlocksReconnectResendRegression` red instead; that test has a second leg (trip, rearm, new in-flight writes, trip
    again: the second trip fails them and a reconnect sends no frame).
  - **Checkpoint D fixes (as built).** (H1) A guard slot that is re-armed or ended at a newer generation kills the
    producers still listed on it before it is reused; a stale generation is ignored (wrap-aware compare). (H2) A producer
    binds to at most one guard: the issuing thread keeps a flag and the duty side refuses a second bind (`BindRefused`,
    counted by `bindRefused()`). (H3) `kMsgGuardBind` is 24 bytes and carries `(sid, slot, gen, armSeq, atRiskNs)`; the
    duty side adopts the arm state from the bind, so the hub ring and the producer ring drain in any order (same
    generation applies only with a newer `armSeq`). `bindProducer` returns `BackPressured` until the guard is ready.
    (M1) A revoke refused as `InvalidArgument` or `Closed` is permanent: GuardEnd is posted, the entry is freed and
    `onLost(RevokeRefused)` fires from `revoke()` only. (M3) `add` refuses parameters that would flap: the at-risk offset
    is `min(partNs, ttl - ttl/50 - 2d)` and must be at least `ttl/3 + d`; the constructor throws when `3*num <= den`.
    A fixed-width topic refuses a key that does not fit the revoke key. (L3) The session destructor retries
    `GuardRelease` with the idle strategy until accepted or the client is closed. (L7) Lease control requests arriving
    at a full control table (256) complete with `Throttled`; the session reschedules them soon.

### 12.3 `LeaseOwnership` and `LeaseElection` (etcd `concurrency.Mutex` / `Election`)

These are client recipes over a one-partition lease topic keyed by `path string(N ≤ 128)`. `LeaseOwnership` is etcd's
`Mutex` renamed (owner's ruling, 2026-11-09): it is cluster-wide ownership of a name by lease records, never a thread
lock, and takes no lock in the client or the broker. Every operation is async
(§12.1), and the recipes run inside the session's `doWork`.
- **Shared watch (Coordinator's call, R7).** Recipes take `(session, watch, name)`. Every recipe on one topic in a
  process shares one `LeaseWatch` started with `startFromNow`, so a Herald gateway with about 2 000 elections runs one
  subscription, not 2 000.
  - The watch keeps, for each registered name, the live `(lease_version, key)` set under `name/`, in a preallocated
    min-heap (`Capacity` names, `Waiters` entries per name).
  - "The lowest version under `name/`" is read from there, never from a `ScanLeases`, which has no key order (§8.3).
  - A recipe acts only after the watch's `LeaseWatchReady`.
- **The key (Coordinator's call).** `name/<sessionId>-<campaignNo>`.
  - `sessionId` is a 64-bit random id that the `LeaseSession` draws at construction, as 16 hex digits.
  - `campaignNo` is a per-session counter of 8 hex digits, incremented at every `claim` / `campaign`.
  - So a re-campaign after a resign or a lost lease always grants a new key (Herald F1).
  - `name` is at most 128 − 26 = 102 bytes.
- **`LeaseOwnership(session, watch, name)`.**
  - **`claim()`** grants its key (always granted: the key is new). After the grant's ack and the watch passing that
    offset, the lock is held if its version is the lowest under `name/`. Otherwise it waits for the `Revoked` /
    `Expired` of its immediate predecessor, then re-checks. This is etcd's `waitDeletes`. `OpKind::LeaseCampaign`
    completes on ownership.
  - A grant answered `VerdictUnknown` (§6.3): the recipe reads its key. If it is live with
    `lease_holder == sessionId`, it adopts that version. Otherwise it grants with the next `campaignNo`.
  - **`tryClaim()`** grants, checks, and revokes its own key if it is not the lowest (`ErrClaimed`).
  - **`release()`** is a fenced revoke.
  - **`isOwner()`** compares its version with the lowest.
  - **`token()`** returns its `lease_version`.
- **`LeaseElection(session, watch, name)`.**
  - **`campaign(value)`** is a `claim` with the value as the row.
  - **`proclaim(value)`** is a fenced `Put`+`update`.
  - **`resign()`** is a revoke.
  - **`leader()`** returns the lowest version under `name/`, from the watch.
  - **`observe()`** reports each change of the lowest, from the watch.
  - At-risk behaviour is in §12.2.
- **A single-slot lock.** A key that is the lock itself, with no waiters (`acquire slot` → `KeyExists`), is etcd's
  `TryLock` with no queue. Herald's fenced workers use that form (§19.4).

### 12.4 Watching and listing

- **Subscribing.** `client.subscribe(topic, AllPartitions{}, opts.filter(...), h)` on a lease topic delivers
  `Granted`, `Updated`, `Revoked` and `Expired`; `Checkpoint` is dropped.
- **`LeaseWatch<T>`** dispatches those four. An untyped `LeaseWatchBase` serves sessions and recipes.
- **`LeaseWatch<T>::startFromNow(prefixes[], done, token)`** gives the exact handoff, per partition (R2):
  1. **Scan.** For each prefix, scan pages 1..n. Page *i* covers the hash range `(h_{i−1}, h_i]` (`h_0` = below
     every hash; the last page reaches the top), at `asOf_i`. `asOf` is exclusive (§8.3).
  2. **Subscribe** from the minimum `asOf` over all pages of all prefixes.
  3. **Apply** a record at offset *o* on key *k* only if *k* matches a prefix and `o ≥ asOf(page covering h(k))`, in
     that prefix's scan. Rule 3 is over hash ranges, never key order.
- **Several prefixes (Coordinator's call).** The watch first drops every prefix that extends another one in the list
  (`a/b/` under `a/`). Two path prefixes are then either nested, which was removed, or disjoint, so every key is
  covered by at most one scan. A Consumer with several filters therefore makes one scan per distinct prefix and one
  subscription.
- **Identity.** Leases are identified by `(key, lease_version)`. A `Granted` already held from the scan is the same
  lease, and an end for an unknown `(key, version)` is ignored.
- **Buffering and overflow (Coordinator's call).** Records that arrive during the scan are buffered within the
  watch's preallocated capacity (`LeaseWatchOptions.handoffBuffer`, default 4096 records).
  - On overflow, the watch does not drop or re-scan. It cancels the subscription and keeps its cursor, which is the
    offset after the last buffered record.
  - When the scan ends, it applies the buffer and re-subscribes from that cursor. The log holds the records, so
    nothing is lost.
  - `rune_client_lease_watch_handoff_resubscribes_total` counts these.
- **Ready.** `LeaseWatchReady` completes once every page is scanned and the buffer is applied.
- **Breaks.** On any later break, the watch re-runs `startFromNow` (§3.3).
- **No op filter in v1.**
- **As built (Task 16) - deviations from the text above.**
  - **Shape.** `LeaseWatchBase` (untyped; sessions and recipes) and `LeaseWatch<T>` (typed decoders only) live in
    `client-cpp/include/rune/client/lease/LeaseWatch.h`. `startFromNow(prefixes)` is synchronous to start (it returns
    `Accepted` or `InvalidArgument`); progress and the result are `doWork(nowNs)` and `isReady()`, plus a `Ready`
    event, because the whole watch runs on the owner thread. Events go to one handler and up to 8 observers (the
    `LeaseSession` is one: `observe(watch)`).
  - **Hash.** The scan hash moved to `protocol/include/rune/protocol/LeaseScanHash.h` (wyhash64 seeded with the source
    topic id), shared by the broker and the client; the broker's golden test and a client copy of it pin the values.
  - **Threads.** The subscription handler (duty thread) does one thing: it copies `{partition, offset, op, version,
    key, value}` into a standalone record ring (`Completions`), the only cross-thread memory. `doWork` drains it in
    order. Scan answers arrive on a second, registered `Completions`.
  - **Overflow.** The ring keeps marker space reserved for every partition. A record that does not fit the ring, or the
    handoff list while the partition scans (`handoffBuffer` records), becomes an `Overflow{partition, generation,
    cursor = that record's offset}` marker and the handler drops the rest of that subscription (generations make stale
    records and markers harmless). The cursor is the first record NOT buffered, not "the offset after the last buffered
    one" of the text above; both name the same record. A Live partition that overflows re-subscribes from the cursor
    at once; a scanning one finishes its scan, applies the list, then re-subscribes (`resubscribes()`).
  - **Page bounds.** Every page of every prefix keeps `(lastHash, asOf)`; the subscription is opened at the first
    page's `asOf` (the minimum) the moment that page answers, and rule 3 stays active for the partition's whole epoch.
  - **Breaks.** A broken subscription (or a non-retriable scan error) moves the partition to a re-run after a 1 ms
    deadline: live entries are marked unconfirmed, the partition is rescanned and re-subscribed, and when the scan
    ends every entry still unconfirmed is emitted `Vanished` and removed. A confirmed entry is not granted twice.
    `isReady()` stays true during a re-run (a recipe keeps seeing the old set); `Ready` is emitted once.
  - **Identity.** `(key, version)`: a `Granted` at another version of a held key emits `Vanished` for the old one,
    then `Granted`. `Updated` / `Revoked` / `Expired` for an unknown `(key, version)` are ignored.
  - **Memory.** The live set (open addressing, backward-shift deletion), name heaps, page bounds, handoff lists and the
    ring are allocated in the constructor; `doWork` and the record handler allocate nothing. Only (re)subscribing
    builds a `std::function` (cold).
  - **Names (Task 20 groundwork).** `registerName(name)` before `startFromNow`; a key belongs to `N` iff it starts with
    `N/` and the rest has no further `/`; `lowest(id)` is the live lease of the name with the smallest version
    (indexed min-heap per name).
  - **Not in this task.** The leader-change-mid-scan 3-node test and the compacted-log-start test move to Task 17.
    `rune_client_lease_watch_handoff_resubscribes_total` is not exported; `resubscribes()` is the observer.
  - **Checkpoint D fixes (as built).** (M2) A scan completing with `InvalidArgument`, or `BrokerError` of NotAuthorized,
    UnknownTopic or InvalidRequest, fails the watch; any other error moves the partition to a re-run with a per-partition
    backoff of 1 ms doubling to 1 s, reset when it reaches Live. (L1) `startFromNow` returns `InvalidArgument` when a
    maximal key plus row plus marker would not fit a record. (L2) The session registers as an observer of a watch
    (`observe`, at most 8); the watch notifies observers when destroyed and the session unobserves when destroyed.
    (L3) The watch destructor waits with the idle strategy until its subscriptions finish or the client is closed
    (Invoker mode pumps `doWork`). `ringOverflows()` counts record-ring overflows.

## 13. The Rune-backed discovery library (deliverable)

- **Where it lives.** In the `discovery` repository, as a second transport beside etcd (call). Herald switches by
  build option and configuration, and the etcd transport stays until Herald's cutover is green.
  - The public types and method names are those of §3: `Producer`, `Consumer`, `ServiceCache` and the rest.
  - Only the first constructor / `create` argument changes, `etcdUrl` → `RuneEndpoint` (DG1), plus the
    caller-supplied client overload below.
- **How it works.** It uses the Rune C++ client (`client-cpp`), which supplies `LeaseTopic`, `LeaseSession`,
  `LeaseWatch` and `ScanLeases`.
  - Given a `RuneEndpoint`, the library creates its own client and runs it on its own thread under the client's
    poll model.
  - Events go to the existing `ServiceCache` through its existing `update` / `remove` calls, so the cache's
    concurrency is unchanged.
- **Caller-supplied client (proposed change, from Herald spec 02 §12.4; Coordinator's call, for owner review;
  made buildable in rev 4, R6).** `create` also accepts an existing Rune `Client` that the caller already drives.
  - **No thread, no blocking.** The library creates no client and runs no thread of its own. It exposes a
    `doWork(nowNs)` agent that the caller polls from the loop that drives the client. Today's blocking steps become
    async state machines inside `doWork`, built on §12.1's completions:
    - the constructor's 30× retry with backoff (now a retry state with a deadline on `nowNs`);
    - the grant and re-registration;
    - the load-report update;
    - the watchdog's point read (an async `get`);
    - the Consumer's scan and subscribe (`LeaseWatch::startFromNow`);
    - `stop()`'s revoke.
  - **Its own session.** The library owns its **own** `LeaseSession` and `LeaseWatch` on the supplied client, and
    keeps no reference to any session of the caller's. The caller's other sessions on the same client are unaffected
    (§12.2).
  - **Stop.** `stopAsync()` starts the stop: it cancels keepalives, revokes the registration fenced, and closes the
    subscription. `isStopped()` turns true when every step has completed or timed out. Destroying the library before
    `isStopped()` is a precondition violation, and debug builds assert on it. The `RuneEndpoint` form keeps today's
    blocking `stop()` and `~Producer()`.
  - **Lifetime.** The client must outlive the library. The caller stops the library (`stopAsync()`, then poll until
    `isStopped()`) before it closes the client.
  - **Login and grants.** The client must already be logged in to the discovery topic's namespace with that topic's
    grants (§14.1). The library never logs in or out on it.
  - Events still reach `ServiceCache` through `update` / `remove`, so the cache's concurrency is unchanged.
  - **Why.** A Herald process already runs a Rune client on its RuneAgent thread, so it needs no second client,
    thread or session for discovery. The `RuneEndpoint` form stays for processes with no Rune client (the legacy game
    services).
  - **Tests.** The `DiscoveryIntegrationTest` suite runs in both forms. In the caller-supplied form, a test asserts
    that no call blocks: a stub transport that never answers, with a hard `doWork` iteration cap.
- **Behaviour.** It implements exactly the §3 tables: the incarnation key and newest-incarnation selection (DG2),
  typed values (DG3), the `ttl/3` keepalive (DG5), `onLost` re-registration (DG6), the watchdog point read, fenced
  load-report updates, a blocking revoke in `stop()`, and re-scan on every subscription break.
- **Provisioning.** It needs one lease topic per deployment namespace (§3.1). The library does not create the topic:
  the operator's DDL does, as an etcd cluster is provisioned today.
- **Tests.** The library's `DiscoveryIntegrationTest` suite runs unchanged against a real Rune broker in place of the
  docker etcd. That covers `ProducerKeepsLeaseAlive`, `LeaseRecovery_*`, `PassiveMode_*`, `Stress`, round-robin and
  least-loaded.
  - New cases cover: restart within the TTL (DG2), a lost lease mid-load-report, a partition-leader failover during
    registration, and a consumer that started from a compacted log.
- **Herald cutover.** It is a Herald task.
  - **The legacy chat server** (§3.5): `main.cpp` builds a `RuneEndpoint` from the environment, and nothing else
    changes.
  - **The Herald 02 runtime** (§19.1): it uses the caller-supplied-client form on its RuneAgent thread.

## 14. Security, namespaces and limits

### 14.1 Grants

| Action | Needs |
|---|---|
| grant, update, revoke, `LeaseKeepAlive` | `WRITE` on the topic |
| `getViewRow`, `ScanLeases`, `LeaseTimeToLive`, `show lease(s)`, subscribe | `READ` on the topic |
| `Expire`, `Checkpoint` | nobody: broker only |
| a discovery `Producer` | `WRITE` **and** `READ` on the discovery topic: its watchdog reads the lease view with `getViewRowK` (R11) |
| a discovery `Consumer` | `READ` |

Herald 02 gives `herald-pool`, the Wardstone sidecar's principal, `READ` as well as `WRITE` on `herald-sys/discovery`
(R11).

Lease view reads need `READ` on the topic; a `VIEW` grant on `<t>.leases` is not consulted (one rule: the gateway
authorises a read of a lease view as `READ` on its source topic, and a foreign namespace answers exactly like a missing id).

- **Fencing.** It uses the version only. `lease_holder` is informational and unauthenticated.
- **No takeover (owner).** No operation ends or replaces a live lease without its version. A principal with `WRITE`
  and `READ` can read a version and revoke, exactly as an etcd client with the lease ID can revoke it.

### 14.2 Limits

- **`lease.max_keys`** is a fold verdict, deterministic.
- **`max-leases` (Coordinator's call).**
  - It is a namespace limit on live leases, enforced per node at write time: a new-key grant gets
    `NamespaceLimitExceeded` (52) while over the limit.
  - It takes `NamespaceLimits.reserved1` (`NamespaceLimits.h:37`, renamed `maxLeases`). The field mask grows to 30
    (`:46`), behind `kFeatureLevelLeases`.
  - The default is 16 777 216.
- **Memory.** Lease-view memory counts toward `max-view-memory-bytes`. The view is never paused. While over the limit,
  new-key grants are refused, and keepalives, updates, revokes, expiry and checkpoints continue.
- **Payload.** A row is up to the topic's max message bytes. System columns take 32 bytes.
- **Keepalive rate.** Keepalives count toward `produce-records-per-sec`, one per entry (§8.1).

### 14.3 Suspend, resume, delete

| State | Client writes | Keepalives | Expiry and checkpoints |
|---|---|---|---|
| `ReadOnly` | refused (51) | served (call: they write nothing) | **continue** (owner) |
| `RejectAll` | refused (51) | refused (51), so the leases end within their TTL | **continue** (owner) |
| resumed | allowed | served | continue; nothing to re-arm |
| deleted | — | — | the topic goes with the namespace |

## 15. Hot path and metrics

### 15.1 Hot path

- **Non-lease topics** pay nothing new:
  - produce takes the existing `[[unlikely]]` per-partition trait branch;
  - the tombstone and control predicates test a trait in the same cache line as the value length;
  - replication is unchanged (there is no follower shadow).
- **Keepalive.** No disk, no replication, no allocation. One ring hop in, one lookup, one re-link, one ring hop out.
- **Grant.** One scratch-copy patch (< 1 µs p50 for ≤ 4 KiB, as checked-writes §6.4).
- **Updates** cost a checked-writes `update` plus the `lastAppended` store, and stay zero-copy. **Revokes** add one
  grant-sized re-encode from the lease view (R9).
- **Expiry and checkpoints** run off the produce path, on the leader's worker, in bounded batches.
- **Budgets (gates).**
  - keepalive request→response p99 < 50 µs end-to-end on a single node, the point-read budget;
  - a 256-entry keepalive p99 < 100 µs;
  - wheel arm and re-arm p99 < 50 ns;
  - poll with 1 M armed and nothing due < 100 ns;
  - expiry lag p99 < tick + 1 ms on a single node;
  - the lease-view point read stays within the < 50 µs gate.

### 15.2 Metrics

All metrics carry `{ns, ns_id, topic, partition}`.

| Metric | Kind | Meaning |
|---|---|---|
| `rune_lease_live` | gauge | leader: live leases |
| `rune_lease_ops_total{op}` | counter | `granted`, `updated`, `revoked`, `expired`, `checkpointed`, `cleared` |
| `rune_lease_keepalive_total{result}` | counter | `ok`, `not_found`, `not_leader`, `suspended`, `throttled`, `parked` |
| `rune_lease_rejected_total{reason}` | counter | `key_exists`, `not_found`, `limit`, `stale_broker` (must stay 0) |
| `rune_lease_expiry_lag_ns` | histogram | deadline → `Expire` appended |
| `rune_lease_expiry_backlog` / `rune_lease_checkpoint_backlog` | gauge | due entries not yet emitted |
| `rune_lease_expiry_deferred_total` | counter | expiries held by `lastAppended` |
| `rune_lease_park_full_total` | counter | keepalives answered `Throttled` because the park table was full |
| `rune_lease_wheel_bytes` | gauge | wheel memory |
| `rune_ns_leases` | gauge | per node: the namespace's live leases (`max-leases` usage) |

## 16. Tests

All tests follow `docs/testing.md`:
- injected lease clocks, advanced to the next deadline;
- deterministic `doWork` with hard caps;
- no sleeps;
- a `*Regression*` test for every fix.

### 16.1 Unit

- **Wheel.**
  - Arm, re-arm, cancel and cascade at every boundary of all five levels (0, 1, 1023, 1024, 1025 ticks per level).
  - `MaxLeaseTTL` and `+1`.
  - Poll returns exactly the due set.
  - The entry state machine `Armed` ↔ `Clearing`, `Armed` ↔ `Checkpointing`, `Armed` → `Emitting`, with demotion
    from each state. No `Expire` or checkpoint is built outside `Armed` (R4).
  - Saturating deadlines at `INT64_MAX − ttl` and with `max_ttl + extend` (R11).
  - Chunked growth, and zero allocations after the high-water mark.
- **Judgment table (§7.2).** Every record kind × absent, equal, different and at-capacity, asserting the code and the
  resulting row, including `remaining`.
- **Write hook.**
  - Every refusal row.
  - TTL 0, `min−1`, `min` (raised and unchanged), `max`, `max+1`.
  - Version stamping over batch sizes 1, 2 and 1000, with the CRC valid after the patch.
  - The `upsert` refusal.
  - `lastAppended` for every touched key.
  - The in-window retry, and the known-old duplicate of each op (§6.3's table).
  - `lastAppended` is set by updates and revokes, never by grants (R8).
  - Revoke re-encoding: the full row from the lease view, the CRC valid, `Throttled` behind an unfolded update (R9).
  - A lease key of 17 and 128 bytes accepted; 129 refused; a non-lease topic still refuses 17 (R1).
- **Keepalive (§8.1), table-driven over every state row.**
  - Not leader, not `Ready`, no read lease.
  - `RejectAll`, `ReadOnly`.
  - Absent, wrong version.
  - `Armed` with and without a checkpoint; `Checkpoint(0)` behind a closed gate (not `Ready`, an update in the tail).
  - Due, `Emitting`, `Clearing`, `Checkpointing`.
  - A batch with entries answered at once and parked entries answered later, each by `index`, the last `final = 1`.
  - Topic dropped with parked entries: `UnknownTopic`.
  - A full park table.
  - Parked then folded, parked then demoted.
- **Checkpoints.**
  - Scheduled only when `ttl > interval`.
  - Skipped when remaining ≤ interval.
  - The rate and batch caps.
  - `⌈⌉` rounding.
  - Cleared by the first keepalive only.
  - At most two records per interval per lease.
- **Promotion arming.** `extend + remaining` versus `extend + ttl`, over a seeded log of grants, checkpoints and clears.
- **Predicates.**
  - `isTombstone` and `isLeaseControl` at every §6.6 site.
  - A golden corpus for non-lease topics.
- **Emitter append (§9.1).** Producer fields −1, timestamps 0, the CRC valid after the base-offset and epoch
  restamp, nothing appended on `kAppendMinIsr` and the entry still due, the emitter stopping on `kAppendNotLeader`.
- **Cleaner.** It reads `lease_version` at value bytes `[4, 12)` across schema versions, and keeps the data row plus
  the latest same-version checkpoint, drops checkpoints of ended or
  re-granted leases, and a rebuild after cleaning equals one before it.
- **`ScanLeases` and the handoff rule** over a seeded churn log: scan + subscription = a fresh fold.
  - Hash-order pages, hash ties within one page, and resume across a leader change and a table grow.
  - The prefix filtered on the broker.
  - Exclusive `asOf`.
  - Several prefixes, nested and disjoint.
  - Handoff-buffer overflow, then re-subscribe from the cursor, with nothing lost or duplicated.
- **QL, DDL and catalog.** Every form and setting, the refusals, and replay creating both objects.
- **`LeaseSession`, `LeaseOwnership`, `LeaseElection` with a stub transport.**
  - Cadence `ttl/3`, and batching per partition.
  - `onLost` on each trigger; `onAtRisk(Entered)` at `t + ttl/2` for each cause (error, timeout, missing reply,
    disconnect, park), then `Cleared` on a late `Ok`; `maxDelay` validation.
  - The produce deadline: at `Entered`, the pending and unsent produces of bound producers fail with `LeaseAtRisk`,
    and nothing is re-sent until `Cleared` (R5).
  - One session over two topics and 8 partitions, batching per `(topic, partition)` (R7).
  - Recipes sharing one `LeaseWatch`; `name/<sessionId>-<campaignNo>` keys; a `VerdictUnknown` grant adopted or
    re-campaigned.
  - Ownership ordering by version, `waitDeletes` on the predecessor only, `tryClaim` cleaning up its key.
  - Election `observe` reporting each leader change once.

### 16.2 Broker integration (real broker, 1-node and 3-node)

- **Grant and expiry.** Grant → a second grant gets `KeyExists` → keepalives hold it → keepalives stop → `Expire`
  delivered and the view empty → a new grant has a larger version.
- **Keepalive racing expiry** at every point: before the poll, after the `Expire` is built, after it commits, after it
  folds. Each gives its §9.3 outcome, exactly one end, and `stale_broker` = 0.
- **Revoke and update racing expiry** at the same four points.
- **Checkpoint clear racing a leader kill.** `Checkpoint(0)` appended, the leader killed before it commits: the
  keepalive is not acknowledged, and the new leader arms from the old checkpoint. Then the same with the checkpoint
  committed.
- **Promotion keeps the old leader in the ISR (3-node, R3).** A move under read-lease A.8 with the old leader's last
  keepalive acknowledged after the `LeaderChange` but before its fetch at the new epoch: no `Expire` before that
  ack plus the TTL.
- **Checkpoint in flight (R4).** A keepalive between `Checkpoint(R)`'s append and its fold parks, then clears. A later
  leader never arms at `T_ready + extend + R` for that lease.
- **Contending grants (R8).** A stream of `KeyExists` grants on a due key does not defer its `Expire`.
- **Promotion (3-node).** No `Expire` before `T_ready + extend + R`, and every abandoned lease ended by the §9.6 bound. Run it
  with and without checkpoints.
- **Whole-cluster restart.** A 24 h one-shot lease checkpointed at 5 min keeps its remaining time ±1 interval, driven
  with an injected clock. A 400-day lease ends on time after three restarts.
- **No zombie acknowledgement (3-node).** Partition the leader: once the new leader is serving, the old one answers
  every keepalive `NotLeader`.
- **Fencing.** Holder A is paused past the TTL, B grants, then A's keepalive, update and revoke all get
  `kErrLeaseNotFound`. B's version is larger.
- **Compaction.** 10 000 updates and 100 checkpoints per key, then clean, then restart: the lease view and the
  remaining times are identical.
- **Gating.** A session one version below is refused on every lease surface.

**As built (Task 17).** The named scenarios are `LeaseIntegrationRegression`, `LeaseRaceRegression` (single node, injected lease clock),
`LeaseTrioRegression` and `LeaseWatchTrioRegression` (3 nodes, real clock, short TTLs). Deviations: the whole-cluster restart runs in
real time on the trio (the 24 h / 400-day legs are the single-node injected-clock `CompactionRestart`, scaled to 4 keys x 2 000
updates); compaction is not forced on a real broker (no seam); the leader-outage test uses 3 forced changes and measures on
the wall clock. The rejoin outage of 1.64-1.72 s (later 3.3-3.7 s) was a forgotten session, not a lease problem: a restarted gateway
dropped the keepalive Ping of a session it no longer knew, and the client only recovered at its response timeout. Fixed
(L22): session ids start at a per-boot random value, so a restarted node never reuses an id of its previous life; every keepalive Ping
echoes `[int32 LE egressStreamId][declared egress channel]` (the Pong ignores it; a channel over 255 bytes sends the empty echo); an
unknown-session Ping with that echo is answered `ErrorResponse{UnknownSession}` on the declared egress channel (a bounded 16-entry notice
table keyed by channel and stream), so the client reconnects at once; the client also keeps route hints for partition-routed control ops.
Limit: in response mode (`gatewayResponseControlPort != 0`) the notice publication does not connect to the client's surviving response
subscription, so recovery there is by the response timeout (`GatewayUnknownSessionRegression.DISABLED_ResponseModeRestartedNodeAnswersRegression`;
an issue is filed).
- **Namespaces.**
  - `ReadOnly`: keepalives served, writes refused, an unkept lease still expires.
  - `RejectAll`: keepalives refused, and every lease ends within TTL + bound.
  - Resume.
  - `max-leases` and view memory.

### 16.3 End to end (real client, real broker, default config)

- **Discovery parity.** The library's `DiscoveryIntegrationTest` suite on Rune (§13), plus:
  - a chat-server `Producer` and a coordinator `Consumer`: `sendToSharedExact` works, the producer is SIGKILLed, the
    consumer's cache drops it within `ttl + bound`, and the producer restarts within the TTL and is visible at once
    (DG2);
  - a Rune partition-leader failover during all of this.
- **Session scale.** 10 000 leases via `LeaseSession`, the process killed, `Expired` delivered by the bound.
- **Caller-supplied discovery client (R6).** A Herald-shaped agent drives one client with a Producer, a passive
  Consumer and 2 000 elections on one shared watch. `stopAsync()` reaches `isStopped()` with no blocking call.
- **Produce deadline (R5).** A guarded producer with sends in flight, its lease's leader partitioned: after
  `Entered`, no record of that producer is appended later than `Entered + d`.
- **Election.** Two `LeaseElection` campaigners: the leader is SIGSTOPped past the TTL, the other leads with a higher
  token, and the first wakes, gets `onLost`, and its guarded write is refused.

### 16.4 Model-checked stress

A seeded simulation of a 3-node in-process cluster runs the real worker, replication and fold code. It has random
grants, keepalive gaps, updates, revokes, leader kills, restarts, partitions, follower lag, compaction, checkpoint
intervals and clock advances. After every step it checks:
- **I1.** At most one live version per key.
- **I2.** Versions increase per key.
- **I3 (never early).** No accepted `Expire` before the latest deadline any leader acknowledged with a keepalive or
  grant, in real (simulated) time, with drift ρ.
- **I4.** Exactly one visible end per lease, and `stale_broker` = 0.
- **I5.** The lease view and user views equal a recompute from the log after every rebuild. Every checkpointed
  `remaining` is ≥ the true remaining time at its offset.
- **I6.** No keepalive acknowledged by a node that was not the serving leader at that moment.
- **I7 (liveness).** An abandoned lease ends within `ttl·(1+ρ) + tick + cycle + commit` in steady state, and within
  `T_ready + extend + R + tick + cycle + commit` after a promotion.
- **I8 (expiry never pauses).** Under `ReadOnly` and `RejectAll`, I7 still holds.

Runs: `--repeat until-fail:20`, and once under `-j` load.

**As built (Task 18).** Two legs on one model, because the injected lease clock works on a single node only: Leg S (one node,
injected clock, exact invariants, 20 seeds x 3 000 steps) and Leg T (3 nodes, real clock, rho = 1 % plus 2 ms, 20 seeds x 200
steps, at most 2 leader kills and restarts per seed). Skipped step kinds: partitions, follower lag, compaction. I5's checkpoint
bound is read from the lease view row (client subscriptions never see Checkpoint records); I6 is not checked directly (no
counter); end-record append time is the tap's receive time. Mutations M1, M3, M4, M13 were not red in 20 seeds on the legs that
could see them; M16 has no pause code. The stress found a silent stall, fixed: a subscription to records needing verdicts
(any lease topic, any verdict source) is served by the partition leader only, at any isolation, and is moved off a demoted
leader with NotLeader (a follower keeps no verdict state). See the plan's Task 18 note.

### 16.5 Mutation checks (each turns a test red)

- arm a promoted key without `extend`;
- arm a promoted key at the refold time instead of `T_ready`;
- drop the `Checkpointing` state (a keepalive during an in-flight checkpoint answered `Ok` at once);
- let a grant set `lastAppended`;
- send a revoke without the broker re-encode (a filtered watcher misses the end);
- answer a known-old update duplicate `kErrLeaseNotFound` while its version is live;
- re-send a guarded producer's request after `onAtRisk(Entered)`;
- order `ScanLeases` by the per-node probe seed (resume breaks across a leader change);
- arm at `ttl` ignoring the checkpoint, or at the checkpoint ignoring a later clear;
- answer a keepalive before `Checkpoint(0)` folds;
- serve a keepalive without the view-read lease;
- answer a due lease's keepalive `Ok`;
- drop the `lastAppended` rule;
- skip the version compare on update or revoke;
- let an update re-arm the deadline;
- pause expiry under `RejectAll`;
- accept `upsert`;
- refuse TTL 0 instead of raising it;
- let a `Checkpoint` supersede the data row in the cleaner;
- deliver a `Checkpoint` to a user view.

### 16.6 Benchmarks and gates

- `LeaseWheelBench`: arm, re-arm and poll at 1 k / 100 k / 1 M / 16 M leases.
- `LeaseKeepAliveBench`: 1-entry and 256-entry keepalives, run ABAB against a point read.
- `LeaseExpiryLagBench`: TTLs of 1 s, 15 s and 30 s with 10 k, 100 k and 1 M live leases, on 1-node and 3-node, with
  ≥ 100 k expiries/s in a mass expiry.
- `LeaseCheckpointBench`: 1 M one-shot leases at the default interval.
- `ProduceBench` and the replication bench on a non-lease topic, ABAB against tip, unchanged.
- Benchmarks are tracked as issues while there is no bench box, and the PR says so.

## 17. Owner question (settled)

**Q1. A Rune principal that works in many namespaces (Herald D12). Settled: option A (Herald 00 D12, owner
delegated; Coordinator's call).** Herald opens one Rune session per tenant namespace per Herald role
(`<ns>/herald-gateway`, `<ns>/herald-moderation`, `<ns>/herald-audit`, `<ns>/herald-safety`), each holding only that
role's grants, and `herald-sys` has the same per-role users. A process opens a tenant session only while it works
for that tenant: a gateway for the tenants it is active for, a fenced worker for the slots it holds. Only the
provisioning singleton holds cluster-admin credentials, for namespace DDL and users, never for data. Option B is not
built: Rune has no account level above namespaces (namespaces spec §1, §2). Nothing in this spec changes: lease
topics are reached through ordinary sessions, and §19.4 stands. The analysis is kept below.

| | A: a per-tenant principal (chosen; per role, as above) | B: a Rune "namespace service" role (not built) |
|---|---|---|
| Rune change | none | a security-model change: a cluster admin grants the role named operations on a list of namespaces, each grant audited in `__security`, with adversarial tests for escalation through the role |
| Herald cost | about 1 000 Rune sessions per Herald process (connections, SCRAM logins, per-session memory), and credential provisioning per tenant | one session per process |
| Isolation | unchanged: a session only ever sees its own namespace | one credential can reach every listed namespace, so its compromise reaches them all |
| Lease topics | §19.4 as written | the same topics, reached through one session |

(Also tracked in Herald spec 02.)

## 18. Possible later work (not in this spec)

- Multi-key leases inside one partition (D1).
- SQL, Redis and sink surfaces.
- Op filter terms.
- Revoke-by-holder.
- Binding the holder to the principal.
- Fenced writes to other topics.
- An ordered secondary index on the lease view, so `ScanLeases` pages cost O(page) rather than O(n) (§8.3).
- Aggregates over multi-partition lease topics.

## 19. Herald examples (doc 00 §13, "Leases: purpose and uses")

| Herald use | Lease topic | Section |
|---|---|---|
| service registration, cache and watch, replica selection | `herald-sys/discovery` via the Rune-backed `discovery` library | §3, §19.1 |
| leadership (the tenant lease, singletons) | `LeaseElection` on `herald-sys/herald.elections` | §19.2 |
| presence | none: `<tenant>/herald.presence` is a plain compacted topic fenced by the tenant epoch | §19.3 |
| worker fencing | `herald-sys/herald.workers` single-slot locks | §19.4 |
| auto-dissolve | none: the tenant's active instance, in memory, from `emptySinceNs` | §19.5 |
| deadlines with an expiry event (sanction and strike expiry, appeal tokens, the tenant delete grace) | one-shot long leases with checkpoints | §19.6 |
| short holds (invites, case claims, rate limits) | short leases, no checkpoints | §19.6 |
| server-API idempotency | none: `<tenant>/herald.api.idempotency` is a plain swept topic (Herald F7) | §19.6 |

Deadlines that need only read-time enforcement are instead `expiresAt` rows cleaned by Herald's `retention-sweep`
worker; friend requests are one (§19.6). Herald spec 02 chooses per object.

### 19.1 Discovery

Two users, both on `herald-sys/discovery` (Coordinator's call, R10):
- **The legacy chat server** is §3.5: chat servers are `Producer`s, and the coordinator lookup is a `Consumer` with
  `sharedService("COORDINATOR_SERVICE")`. This is the parity target.
- **The Herald 02 runtime** (Herald 02 §8.1, §8.4):
  - **Producers.** Every Herald process is a `Producer` at
    `herald/<region>/_shared/<SERVICE>/<podName>/<incarnation>`. The services are `HERALD_GATEWAY` (its uri is the
    pod's route), `HERALD_WORKER`, `HERALD_MODERATION`, `HERALD_AUDIT`, `HERALD_SAFETY` and `HERALD_PROVISIONER`.
    Gateways report their session count as `load` every 5 s.
  - **Consumers, all passive** (the library opens no connections):
    - gateways watch `HERALD_GATEWAY` and the Wardstone pools of their tenants (`sharedService("wardstone-<poolId>")`);
    - each worker-type process watches its own deployment, for rendezvous assignment.
  - **One client.** Each process uses the caller-supplied-client form (§13) on its RuneAgent thread. It runs no
    library thread and no second client. The library owns its own `LeaseSession` and `LeaseWatch` on that client.
  - **The `herald-pool` sidecar.** Each Wardstone instance runs a small registration sidecar, a `Producer` as the
    `herald-sys/herald-pool` principal, at `herald/<region>/_shared/wardstone-<poolId>/<instanceNo>/<incarnation>`. It
    uses the `RuneEndpoint` form and holds `READ` + `WRITE` on discovery (§14.1).
  - **Load.** A few hundred registrations, with keepalives every 5 s, which is trivial. Every path fits in 128 bytes.

### 19.2 Leadership

```sql
lease topic 'herald-sys/herald.elections' { path string(96), value string(64) }
      partitions 1 replicas 3 key path with (lease.ttl = 10000);
```

- **Names.** Herald 02 uses `tenant/<nsId>` (the tenant lease: one active instance per tenant) and
  `singleton/<role>`. Every gateway watches the whole topic with **one** `LeaseWatch` shared by all its elections
  (§12.3), about 2 000 per gateway.
- **Electing a leader.** `LeaseElection(session, watch, "singleton/transparency-schedule").campaign(podName)` runs on
  each candidate. The leader works with `token()` as its fencing token, stamped into every row it writes.
- **Stepping down.** On `onAtRisk(Entered)` it stops at once. The client fails the pending produces of its guarded
  producers (§12.2, R5). Herald then revokes and campaigns with a new key (Herald F1).
- **Failover time.** If the old leader died without a revoke, the standby wins only when the broker expires the old
  key: about `a + ttl`, or `T_ready + extend + ttl` across a Rune leader change (§12.2).
- **Rejecting a stale leader.** Readers of its output keep the highest token, so a paused ex-leader's late write is
  ignored.

### 19.3 Presence

Herald presence uses no lease (Coordinator's call, from Herald spec 02 §12.4 and Herald 01 §11.2).
- **Why no lease.** One active instance serves a tenant (Herald D16). It holds the presence map in memory and writes
  a row on every change. Viewers get updates from that map, never from Rune.
- **The topic.** `<tenant>/herald.presence` is a plain compacted topic keyed `actorId`, row
  `{status, epoch, sinceNs, activity, attrs}`. Herald spec 02 owns the layout.
- **Epoch fencing.** `epoch` is the tenant epoch: the `lease_version` of the tenant's election key (§19.2). A row is
  live only if its `epoch` equals the current one, and a row from an older epoch reads as `offline`. When the active
  instance dies, every actor it served reads as offline as soon as the standby wins.
- **What goes away.** Per-session presence leases, their keepalives, the home cell and `herald.presence.changes`
  (revision 3's earlier draft). Presence needs only the election from this spec.

### 19.4 Worker fencing

```sql
lease topic 'herald-sys/herald.workers' { slot uint64, tenant string(64), role uint16, holderInfo string(64) }
      partitions 8 replicas 3 key slot with (lease.ttl = 10000);
create topic 'herald-sys/herald.idslots' { id string(16), slot uint64, epoch int64, highWater uint64, fencingToken int64 }
      partitions 1 replicas 3 key id with (key.index = on);   -- id = slot (u64 big-endian) ‖ epoch (i64 big-endian)
create view 'herald-sys/herald.idslots_by_slot' key (slot, epoch) as select highWater, fencingToken
      from 'herald-sys/herald.idslots';                         -- scan by slot (Herald 02 owns the name)
```

- **The slot and the lock (Herald's formula, B28; Coordinator's call, R10).** `slot = nsId << 16 | roleId`, with
  `roleId = role << 8 | partition` (up to 256 partitions per role). The `role → u8` table lives in Herald 02. Rune
  never reuses an `nsId`. Each Herald process `grant`s the
  slots rendezvous hashing assigns it. This is a single-slot claim (etcd `TryLock`, §12.3), where `KeyExists` means
  someone holds it.
- **Fencing writes.** The worker writes with `version` as its fencing token.
- **Handing a slot back.** On a membership change it hands the slot back with a fenced revoke, so the next owner
  grants at once.
- **Minted ids stay unique (doc 00 §13 "Minted ids"; Coordinator's call, F3).**
  - **One row per minter and epoch.** `herald.idslots` is keyed `(slot, epoch)`.
    - The tenant's active instance uses `slot = nsId << 16` (roleId 0, which no worker uses). Its `epoch` is the
      tenant epoch (§19.2).
    - A fenced worker that mints uses its own `herald.workers` slot (`nsId << 16 | roleId`). Its `epoch` is the
      worker's `lease_version`. Worker-minted ids set the top bit, so they never meet the active instance's ids.
    - The value column `fencingToken` is the epoch the row was written under, so a reader checks a row without
      decoding the key.
  - **Starting.** A new minter scans `idslots_by_slot` for its slot, starting above the largest `highWater` over
    every row of that slot. It then inserts its own row `(slot, epoch)`, which is always new.
  - **Reserving.** It advances its own row's `highWater` with checked `update`s, and never mints past it.
  - **A stale minter** can only touch its own `(slot, oldEpoch)` row, so it can never lower or overwrite the new
    minter's. Herald 02 §5.2 gives the argument that every id the old minter used is below the new floor.
  - **Where it lives.** A plain keyed topic, not the lease row, because a lease row is deleted when the lease ends.
    Old epochs' rows are swept by Herald.
- **Scale.** 4 000 slots kept alive at 3.3 s is about 1 200 keepalive entries/s.

### 19.5 Auto-dissolve

Herald auto-dissolve uses no lease and no worker (Coordinator's call, from Herald spec 02 §12.4 and Herald 01 §6.3).
- **What dissolves.** An owner-bound room when its owner's last session closes, and an empty channel instance of a
  class with `autoDissolve` after its `autoDissolveAfterSec`.
- **Who acts.** The tenant's active instance, in memory. It is the single writer of channels and memberships, so it
  sees the last member leave or the owner's last session close, waits the delay, and writes the dissolve.
- **Failover.** The channel row carries `emptySinceNs`. A new active instance re-arms each deadline from it at
  attach, so a channel dissolves late, never early.
- **Why no lease.** One writer per tenant (Herald D16), and no other process needs an event at the deadline.

### 19.6 Deadlines with an expiry event

```sql
lease topic 'herald-sys/herald.tenant-delete-grace' { nsId uint32 }
      partitions 1 replicas 3 key nsId with (lease.ttl = 604800000);                -- 7 d, checkpointed
lease topic 'acme/herald.safety.sanction-expiry' { sanctionId uint64 }
      partitions 1 replicas 3 key sanctionId with (lease.ttl = 34560000000);        -- default 400 days; each grant
                                                                                    -- passes the remaining duration
```

- **One-shot leases.** A sanction's expiry is granted once and never kept alive. Herald uses them for sanction and
  strike expiry, appeal tokens and the tenant delete grace (Herald spec 02 §6).
- **The tenant delete grace (Coordinator's call).** `herald-sys/herald.tenant-delete-grace`, keyed `nsId`, is
  written by the provisioner (`herald-provisioner`: `WRITE` + `READ`).
  - A delete request grants it with a 7 d TTL.
  - `CancelDeleteTenant` revokes it, fenced.
  - Its `Expire` reaches the provisioning singleton, which re-checks holds before it deletes the namespace.
  - It is checkpointed, so a Rune leader change makes it late by at most `extend + one interval`.
- **Ending early.** Lifting or superseding a sanction is a fenced revoke.
- **Ending on time.** Otherwise the `Expire` carries the lapse to every consumer, with no sweeper.
- **Revoke versus expiry.** They race in the log, and exactly one wins (§9.3).
- **Leader changes and restarts.** Checkpoints keep them on time: late by at most `extend + one interval` (§9.6).
- **Cost.** One checkpoint per 5 min per lease, compacted to one per lease.
- **Short holds.** Short invites and moderation holds (30 s – 30 min) are the same pattern without checkpoints (TTL ≤
  interval).
- **Not leases (Coordinator's call, from Herald spec 02 §12.4).**
  - Friend requests are `herald.social.requests` rows with `expiresAtNs`, checked at read time and removed by
    `retention-sweep`: nothing needs an event when one lapses.
  - Herald's report quota is a plain compacted row.
  - **Server-API idempotency (Herald F7, rev 4)** is a plain swept topic `<tenant>/herald.api.idempotency` with
    `key.index` and a view. Its row holds a response hash and the `(partition, offset)` of the stored response
    record (≤ 8 KiB). Herald 02 §6.2 owns the columns. Rev 3's lease-based idempotency topic is removed.

**Requirements from the Herald program.**

| # | Requirement | Met by |
|---|---|---|
| 1 | a payload of at least 64 bytes for holder identity | rows up to max message bytes; `holderInfo` columns |
| 2 | a max TTL of at least 400 days | `lease.max_ttl` up to 9 × 10⁹ s with checkpoints (§9.5, §10) |
| 3 | tenant workers observe node-gone across namespaces | a `herald-sys` session plus tenant sessions (§17) |
| 4 | one fenced worker per (tenant, role) at about 1 k × a few | §19.4 |
| 5 | 1–30 s TTLs with a tight bound | 1 ms tick, the fixed 1 s `min_ttl` floor (R11), ≥ 100 k expiries/s |

## 20. Phases

### Phase 1: lease topics in the broker

1. **Wire and codes.**
   - The lease-op bits, the three error codes, the versions and the feature level.
   - `leaseVersion` in the `View` ack.
   - `LeaseKeepAlive`, `LeaseTimeToLive` and `ScanLeases`.
   - Tests: codec round trips; old sessions refused.
2. **Catalog and DDL.**
   - `LeaseSpec`, the one-event topic and view, and the system columns.
   - The settings, including the fixed `min_ttl` floor (R11).
   - The 128-byte lease-key rule at every §5.1 site (R1).
   - QL.
   - Tests: the QL and DDL tables; replay.
3. **Write hook.**
   - The refusals, the TTL raise, version stamping and `lastAppended`.
   - The known-old duplicate.
4. **LeaseIndex judge.** The §7.2 table, member 0, partition-aligned instances, rebuild per leadership.
5. **Predicates.** `isTombstone` and `isLeaseControl` at every site, plus the cleaner's lease mode (the fixed
   `lease_version` offset, the checkpoint map).
6. **Wheels.** The five-level wheel, the four-state entry machine (with `Checkpointing`), chunked storage, saturating
   deadlines. `LeaseWheelBench`.
7. **Expiry emitter.** The gate (with no suspension pause, `Armed` only), `lastAppended`, the scratch, and the data
   batch through `PartitionReplica::appendLeaseBatch` (§9.1).
8. **Keepalive and TimeToLive serving.** The read-lease gate, the park table, per-entry answers, `Checkpoint(0)`
   clearing through the gate, demotion and topic drop.
9. **Checkpoints.** Scheduling, the rate and batch caps, rounding, and promotion arming at `T_ready + extend` in
   chunks.
10. **Reads.** Point-read routing, `ScanLeases` in hash order with broker-side prefix filtering, `READ`-on-topic.
11. **C++ client.** The async `LeaseTopic` API and its `OpKind`s, the 128-byte lease `SEND_RECORD` form, the untyped
    `LeaseSession` with `onAtRisk` and the produce deadline (R5), and `LeaseWatch::startFromNow` with the hash-range
    handoff.
12. **Integration, e2e, stress and mutation checks** (§16.2–§16.5).

### Phase 2: recipes, limits, operations

1. `LeaseOwnership` and `LeaseElection`, with unit tests and the election e2e.
2. `max-leases`, the view-memory rule, the suspension matrix.
3. Metrics, `show lease(s)`, `revoke lease`.
4. Latency gates (§16.6).
5. Docs: `docs/api/ql.md`, `docs/api/client-cpp.md`, `docs/operations.md` (sizing checkpoint rate, the lease clock).

### Phase 3: the Rune-backed discovery library (deliverable)

1. A `RuneEndpoint` and the Rune transport for `Producer`, in both forms (`RuneEndpoint`, and the caller-supplied client
   with `doWork`, `stopAsync()` and `isStopped()`): incarnation keys, its own `LeaseSession`, the watchdog, load
   updates, `stop()`.
2. The Rune transport for `Consumer` (scan + subscribe, newest-incarnation selection, re-scan on break). The
   `ServiceCache` path is unchanged.
3. `DiscoveryIntegrationTest` against a real Rune broker, plus the DG2 and failover cases.
4. Herald cutover task: `main.cpp` builds the `RuneEndpoint`, and the discovery e2e runs on the default config.

## 21. Interaction matrix

**Operations:**
- grant, update, revoke;
- keepalive (single and batched), TimeToLive;
- point read, `ScanLeases`, watch (`startFromNow`);
- broker `Expire`, broker `Checkpoint`, keepalive-triggered `Checkpoint(0)`;
- alter of the TTL bounds and checkpoint keys, drop topic;
- Ownership `claim` / `tryClaim` / `release`;
- Election `campaign` / `proclaim` / `resign` / `observe`;
- discovery `registerEndpoint` / load report / `stop`, and Consumer `run`.

**Entities:**
- the same key and version;
- the same key with a stale version;
- another key in the partition;
- another partition;
- another lease topic;
- a non-lease topic on the same worker;
- a user view over the lease topic;
- another namespace;
- an old session;
- an old incarnation of the same `ServiceKey`.

**Timings:**
- sequential, and back-to-back before the ack;
- a keepalive, update or revoke at each of the four points around an `Expire`;
- a keepalive while `Clearing`, while `Checkpointing`, while `Emitting`, and while due-not-emitted;
- contenders' grants on a due key;
- a revoke back-to-back behind its own update;
- a known-old duplicate of each op;
- a handoff-buffer overflow mid-scan;
- `onAtRisk` with guarded produces pending and unsent;
- a leader change with an `Expire` or a `Checkpoint(0)` uncommitted, committed-not-folded, or folded;
- a leader change between a grant and its ack, and mid-scan;
- a leader partitioned but not yet deposed (a zombie);
- a restart (one node, then all nodes) between a checkpoint and the deadline;
- compaction mid-life;
- suspend `ReadOnly` / `RejectAll` mid-TTL, then resume;
- an alter between a grant and its expiry;
- drop topic with parked keepalives;
- a producer restart within the TTL.

**Expected outcomes, the parts that are not obvious:**

| Case | Expected |
|---|---|
| grant × same key × back-to-back | first in log order wins; the second gets `KeyExists` |
| `upsert` × any | `InvalidRequest` |
| keepalive × `Armed`, checkpoint set | `Checkpoint(0)` folds before `Ok`; the deadline is `now + ttl` |
| keepalive × due, not yet emitted | parked; `kErrLeaseNotFound` after the `Expire` folds |
| keepalive × `Checkpointing` | parked; `Checkpoint(0)` after `Checkpoint(R)` folds; `Ok` after the clear folds |
| keepalive batch × some entries parked | the decided entries answered at once; each parked one later by `index` |
| drop topic × parked keepalives | `UnknownTopic`; `onLost(TopicDropped)` |
| grant × due key, repeated | `KeyExists` each time; the `Expire` is not deferred |
| revoke × unfolded update of the same key | `Throttled` (1 ms), then accepted with the full row |
| known-old duplicate × grant / update / revoke | `VerdictUnknown` / `Ok` if live, else `kErrLeaseNotFound` / `kErrLeaseNotFound` |
| `onAtRisk` × guarded producer | pending and unsent produces fail with `LeaseAtRisk`; nothing re-sent until `Cleared` |
| local `onLost` × election | no leader until the broker's deadline (`a + ttl`, or `T_ready + extend + ttl`) |
| keepalive × zombie leader | `NotLeader` (no read lease) |
| keepalive × `RejectAll` | `NamespaceSuspended`; the lease ends within TTL + bound |
| keepalive × `ReadOnly` | `Ok` |
| update × live lease | columns replaced; the deadline unchanged; it expires on schedule if not kept alive |
| revoke × due | blocks the `Expire` until the revoke folds; one end |
| promotion × checkpoint set | armed at `T_ready + extend + remaining` |
| promotion × checkpoint cleared and the clear folded | armed at `T_ready + extend + ttl` |
| promotion × `Checkpoint(0)` truncated | armed from the old checkpoint; that keepalive was never acknowledged |
| whole restart × 400-day lease | ends late by at most `extend + interval` |
| TTL 0 or below `min_ttl` × grant | raised to `min_ttl`; the ack carries it |
| TTL above `max_ttl` × grant | `kErrLeaseTtlTooLarge` |
| alter `max_ttl` below a live lease's TTL | the live lease keeps its TTL; new grants are checked |
| user view × `Checkpoint` | not folded |
| user view × `Expire` | row deleted in the same fold batch as the lease view's |
| non-lease topic × any | byte-identical behaviour, unchanged bench |
| discovery × restart within TTL | the new incarnation visible at once; the old one's end ignored |
| discovery × current incarnation ends, an older one still live | the cache falls back to the older one |
| Ownership × holder expires | the next waiter by version becomes the owner; its token is larger |
| any op × another namespace | answers as missing |

## 22. Review focus

1. **etcd parity.** Check every row of §4 against the etcd source, especially the expired-lease wait, the synchronous
   `Checkpoint(0)`, `Promote(extend)`, and "put does not renew". Every difference must be a named Divergence (D1–D4).
2. **Discovery parity.** Check every row of §3 against the library's source and tests. DG1–DG9 must be the complete
   list of observable differences.
3. **Never early.** Keepalives are acknowledged only under the view-read lease. Promotion arms at
   `T_ready + extend + (remaining or ttl)`, and §9.2's causal argument covers a move under A.8. No keepalive is
   answered `Ok` while a checkpoint is in flight (`Checkpointing`). A keepalive after a checkpoint is answered only after `Checkpoint(0)` folds. Nothing
   compares clocks across nodes.
4. **Expiry never pauses** in any namespace state. The broker's appends bypass the suspension refusal.
5. **No takeover.** No path ends or replaces a live lease without its version: no `upsert`, no plain delete, no force
   revoke.
6. **Exactly one end, never stale.** The `lastAppended` rule and the keepalive park, for every interleaving with a
   leader change. `stale_broker` must stay 0.
7. **Checkpoint compaction.** The cleaner keeps the data row and the latest same-version checkpoint, and a rebuild
   gives the same remaining times.
8. **Predicate blast radius.** `isTombstone` and `isLeaseControl` at every site, unchanged for non-lease topics.
9. **No stall.** §9.7.
10. **Zero alloc, no virtual calls.** The wheels, the park table, the scratch, `LeaseSession`'s functors.
11. **Widths and orders.** Every §5.1 key-width site, and `ScanLeases`' hash order with its O(n) cost.
12. **Vacuous tests.** Each §16.5 mutation turns a test red, and the stress invariants are checked after every step.

## Appendix A. Transient lease topics (in scope since the owner ruling of 2026-11-07; design pending)

Transient lease topics are out of this spec (Coordinator's call). This is the analysis for a future spec.

- **Why it is separate.** Checked writes refuse key indexes on transient topics (`TopicFeatureRules.h:39-49`), because
  verdict markers in a ring are lost on restart. A lease topic on a ring also needs:
  - the latest record of every live lease, and its latest checkpoint, kept in the ring;
  - a full reset delivered to watchers (`PartitionDataReset`, 43).
- **The FIFO floor.** A transient lease partition would pin the ring's eviction floor with
  `InMemoryLog::setEvictionFloor` (`InMemoryLog.h:178`, not `truncateTo` at `:271`) at the oldest live lease's
  grant. So the ring must hold up to the longest TTL of traffic. Revision 3 makes that worse: TTLs reach 9 × 10⁹ s.
- **Markers need space too,** below that floor.
- **Ring-full behaviour.** Keepalives are not ring writes, so a full ring no longer turns back-pressure into lost
  leases. Grants and updates refused with `TransientLogFull` are the remaining cost.
- **Open for the future spec:** a sizing rule, or a lease-aware ring that moves a live lease's latest record forward.
