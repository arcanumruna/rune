# Rune namespaces (multi-tenancy) and topic truncate: design

**Status:** approved, 2026-10-26 (all open questions decided).
**Scope:** namespaces with dynamic create, suspend and delete; namespace-scoped users and grants; per-namespace limits set by the cluster admin and measured with arcanum-metrics; a `TruncateTopic` operation that also cuts the views over the topic.
**Out of scope:** CPU caps through dedicated threads or nodes, per-namespace encryption at rest (it is a reserved limit only), and an account level above namespaces.

## 1. Model
- **Namespace:** the isolation unit, created and limited by the cluster admin.
- **Contents:** topics, views, schemas, consumer groups, transactions and users all belong to exactly one namespace.
- **Names:** every object is named by the pair (namespace, name).
  - A bare name resolves in the session's namespace.
  - The qualified form is `ns/name`. Topic, view and group names can never contain `/` (`TopicName.h`), so the form
    is unambiguous. Only the cluster admin may name an object in a namespace other than its session's.
  - An object in another namespace answers exactly like a missing one: same error code, same message, same timing
    path. Nothing reveals that a name exists elsewhere.
  - **Indexes are keyed by `(nsId, name)`.** `ObjectIndex::byName` and the schema and view registries, global today,
    take the pair as their key, so the same name in two namespaces is two entries and a lookup can only find an
    object of the namespace it was given.
  - **Consumer group and transactional ids** are keyed by `(nsId, id)` everywhere they are stored or looked up
    (coordinators, `__consumer_offsets`, transaction state), as the internal-topic keys are (§4). Two namespaces can
    use the same group or transactional id without sharing a coordinator entry, offsets or producer fencing.
- **No cross-namespace references.** A view's sources, a topic's schema and a transaction's partitions all belong to
  the object's own namespace. DDL that names an object in another namespace is refused, for the cluster admin too.
- **`default` namespace:** always exists and can never be deleted.
- **Identity:** each namespace has a `nsId`, a u32 that the catalog assigns and never reuses. It is the namespace's incarnation: deleting `acme` and creating it again gives a new `nsId`, so stale state can never match the new namespace.
- **Disk layout:** `<data>/ns-<nsId>/topics/<name>/<partition>/...`.
- **No in-place upgrade.** A cluster with namespaces is a new cluster; existing data, users and grants are not migrated, so there is no upgrade rename and no mixed-version cluster.
- **Memory layout:** a per-node map from `nsId` to that namespace's objects, owned by the metadata or catalog agent.
- **Hot path:**
  - Topic and view ids stay globally unique, so the produce, consume and query paths never look up a name or an `nsId`.
  - A namespace is resolved once, when a name is turned into an id, and that lookup is checked against the session's grants.
  - Each topic and view carries its namespace's metric slot (`nsSlot`, §5.3) next to its id. Usage is charged to the
    namespace that owns the object, never to the session's, so a cluster admin producing into `acme` counts against `acme`.
  - The only hot-path additions are the slot add and the throttle compare of §5.4.

## 2. Roles and users
| Role | Can do |
|---|---|
| Cluster admin | Root: every operation in every namespace, data included, with no grant needed. Create, suspend and delete namespaces; set `NamespaceLimits`; appoint namespace admins. |
| Namespace admin | Within its own namespace: create, delete and change the password of users; grant and revoke permissions; run topic, view and schema DDL. |
| User | Whatever its grants allow, within its own namespace. |

- **User names** have the form `ns/user` and live in `__security` (SCRAM). Logging in fixes the session's namespace; `ConnectRequest` gets no namespace field.
- **Cluster admins** are users of `default` that hold the cluster-admin grant. Suspension never applies to a cluster-admin
  session, so suspending `default` cannot lock the admin out.
- **Roles** are namespaced like users (`ns/role`); a grant can only name a role of its own namespace.
- **Security enums** are frozen and only appended to: `SecurityOperation::Truncate = 7` and
  `SecurityResourceType::Namespace = 5` (the next free values in `SecurityTypes.h`); nothing is renumbered.
- **Escalation guards**, each enforced in the security agent and each covered by an adversarial test. A namespace admin can never:
  - grant cluster-admin;
  - grant on, or touch users in, another namespace;
  - raise a limit;
  - create or delete a namespace.
- Grants stay allow-only, and everything is denied by default.
- **The last-security-admin guard stays cluster-wide** (issue #121, `removesLastSecurityAdmin` in `SecurityAdmin.cpp`):
  a change is refused only when it would leave the cluster with no enabled cluster admin. A namespace may be left
  with no namespace admin; the cluster admin can always appoint a new one.

## 3. Catalog events
New event kinds go in `__catalog`; these kinds are already reserved there:
- `NamespaceCreated(nsId, name, limits)`;
- `NamespaceLimitsSet(nsId, limits)`;
- `NamespaceStateChanged(nsId, state)`;
- `NamespaceDeleted(nsId)`.

These events use a new catalog formatVer 2 rule set: v2 accepts the namespace kinds and applies every v1 kind exactly as v1 does. The formatVer 1 rules never change and keep refusing the namespace kinds. No version gate is needed: there are no mixed-version clusters and no in-place upgrade.

Users and grants for a namespace go in `__security`, tagged with its `nsId`. Every node applies the events in log order, and applying the same event twice changes nothing.

**New error codes**, appended to the wire enum and never renumbered: `NAMESPACE_SUSPENDED`, `NAMESPACE_LIMIT_EXCEEDED`,
`THROTTLED` (carries `retryAfterNs`) and `VIEW_PAUSED`. A deleted or deleting namespace answers like a missing one
(§1).

## 4. Lifecycle
A rich-state machine, owned by the catalog agent. Every state handles every event.

```
ACTIVE ⇄ SUSPENDED(mode: ReadOnly | RejectAll)
ACTIVE|SUSPENDED → DELETING(pendingTopics, pendingViews, pendingGroups, openTxns, users) → (removed)
```

**Suspend.**
- In `ReadOnly` mode, the gateway refuses produce, all DDL (TruncateTopic and user and grant changes included), transactions and consumer-group offset commits with `NAMESPACE_SUSPENDED`, and allows fetch, view reads and QL reads. Use it to freeze a namespace while its owner can still get data out.
  - Transactions open when the suspend applies are aborted by their coordinator, so none is left waiting for a
    commit it can never make.
- In `RejectAll` mode, every request and every login fails with `NAMESPACE_SUSPENDED`, and open sessions are closed.
- Every request type is listed in one table, allowed or refused per mode, and a test walks the whole table; a new
  request type cannot be added without a row.
- Neither mode applies to cluster-admin sessions (§2). Resume, delete and limit changes stay open to the cluster admin
  in every state.
- **`default` can be suspended** like any namespace. Its users other than the cluster admins are then held to the
  mode; the cluster admins, who live in `default`, are not (§2), so the cluster stays manageable.
- Suspending an already suspended namespace changes only its mode. Resuming an `ACTIVE` one changes nothing.

**`DELETING`** refuses every request, login, suspend, resume and limit change for that `nsId`, and answers like a
missing namespace (§1).

**Delete**, in this order:
1. Append `NamespaceDeleted`. New requests and logins for the namespace are rejected from then on.
2. Fence and close its sessions, and revoke its users.
3. Abort its open transactions.
4. Delete its topics, views and schemas through the existing `DropTopic` and `DropView` paths.
5. Purge its keys from `__consumer_offsets` and the transaction state with tombstones, and its users, roles and grants from `__security`.
6. On each replica, rename `ns-<nsId>` to `ns-<nsId>.deleting` (atomic), then remove it in the background.
7. Free its metric slots (§5.3). Until then the namespace still counts against `maxNamespaces`.

`NamespaceDeleted` is the only commit point; every owner then reconciles its own part idempotently. The name can be
created again while the old `nsId` is still `DELETING`: the new `nsId` cannot match any of the old state.

**Crash safety.** On boot, and when a follower catches up, any `ns-*.deleting` directory is removed, and any namespace marked deleted in the catalog has its remaining steps re-run. All steps are idempotent. Convergence comes from the log plus an idempotent restore, never from a timeout (rule 16).

**Internal topics** (`__consumer_offsets`, transaction state) stay shared, and every key starts with `nsId`. `__catalog` and `__security` are cluster-wide.

## 5. NamespaceLimits
Rune has no tiers or plans. It knows only the limits a cluster admin puts on a namespace and its users. Whatever
sells those limits (the Arcanum platform, an operator script) sits outside Rune and acts as the cluster admin.

### 5.1 Model
- **Set at create.** `CreateNamespace(name, limits)` carries the full limit record, and `NamespaceCreated(nsId,
  name, limits)` stores it. Namespace names match `[a-z0-9][a-z0-9-]{0,62}`; `__`-prefixed names are reserved. A
  limit the admin leaves out takes the cluster default *at that moment*, and the
  resolved value is written into the event. A later change to the cluster default never changes an existing
  namespace.
- **Changed by the cluster admin only.** `SetNamespaceLimits(name, limits)` appends `NamespaceLimitsSet(nsId,
  limits)` with the full resolved record (not a delta). Every node applies it in log order, and applying it twice
  changes nothing. A namespace admin can read its limits and usage, never change them.
- **Per-user limits** (connections and sessions per user) are fields of the namespace's record and apply to every
  user in it. Per-user and per-topic overrides are deferred.
- **One metric per limit.** Every limit is paired with a usage metric in arcanum-metrics (§5.3). The broker
  enforces against the same numbers an operator scrapes, so enforcement and monitoring never disagree.
- **Lowering a limit** never deletes objects or data by itself:
  - for counts, disk bytes and connections, new creates, produce or logins are refused until usage is back under
    the limit; existing sessions stay;
  - for the maximum retention, every topic's effective retention is clamped to the new maximum, so retention then
    deletes the older segments. This is the one limit whose lowering removes data, and the admin command says so.

### 5.2 Limits (v1)
| Group | Limit | Scope | Usage metric |
|---|---|---|---|
| Objects | topics; partitions per topic; total partitions; views; schemas; versions per schema; consumer groups; users | cluster | `rune_ns_objects{ns,ns_id,kind}` |
| Storage | disk bytes | per node | `rune_ns_disk_bytes{ns,ns_id}` |
| Storage | max retention time and bytes; compaction allowed; max message and batch size | per topic (checked at DDL / produce) | none (static ceilings) |
| Throughput | produce bytes/s and records/s; fetch bytes/s; request ops/s; query CPU cycles/s | per node | `rune_ns_*_total{ns,ns_id,thread}` |
| Connections | connections per namespace and per user; sessions per user; open transactions; max transaction time | per node | `rune_ns_connections{ns,ns_id}`, `rune_ns_open_txns{ns,ns_id}` |
| Views | materialized memory | per node | `rune_ns_view_bytes{ns,ns_id}` |
| Durability | min/max replication factor; lowest `acks`; lowest `min.insync` | per topic (DDL, connect) | none |
| Features | transactions, QL, views, SQL/Redis facades, anonymous read (§11, off by default); allowed auth mechanisms | namespace | none |

Deferred, kept as reserved fields so the record never changes shape: placement (nodes or racks), password policy,
per-namespace encryption key.

- **Cluster vs node scope.** Object counts are exact and cluster-wide, because every node applies the same
  `__catalog`. Rates, connections, disk and view memory are enforced per node, like Kafka quotas: each node
  measures only what it serves or stores, and the admin sets the per-node value. Cluster-wide aggregation of those
  would need cross-node traffic on every tick and is out of scope.
- **Disk bytes** count every segment, index and checkpoint the node stores for the namespace, leader and follower
  replicas alike. Only produce is refused when the node is over; replication into a follower is never refused
  (it would break the ISR), so a follower can go over by what is in flight.

### 5.3 Metrics (arcanum-metrics)
All Rune metrics, the limit metrics and every existing Aeron counter in `RoleCounter.h` (type ids
1'000'201..1'000'241), move to arcanum-metrics. Rune then has one metrics system; `cnc.dat` keeps only the media
driver's own counters. `docs/operations.md` and every tool that reads the old counters move with it.

**Library changes first** (phase 0, in arcanum-metrics). As it stands the library breaks Rune's rules: shared RMW
and CAS on slots, a `std::mutex` in the windowed metrics, `steady_clock`, `std::string` and exceptions on
registration, and no way to free a slot. It gains, mirroring Aeron's `CountersManager`:
- **Single-writer slots.** Each value slot has exactly one writer thread. It updates with a plain add and a release
  store (`std::atomic_ref` under TSan); readers use an acquire load. No `fetch_add`, no CAS, no mutex.
- **Rates from totals.** The hot path only bumps monotonic totals. Rates and windowed max/avg are computed by the
  reader from successive samples, so no bucket array or lock is written on the hot path.
- **Injected clock.** Every time-based function takes `nowNs` from the caller's advanceable clock.
- **Slot assignment.** The catalog agent picks a free `nsSlot` (0..`maxNamespaces`−1) when it applies
  `NamespaceCreated`, and tells every thread of the node `(nsSlot, nsId)` through its ring before any object of the
  namespace exists. Slots are local to the node; only `nsId` is cluster-wide.
- **Allocate and free with a registration id.** A slot block carries a state word (unused, allocated, freed) and a
  registration id. Rune uses the `nsId` as the id, and since an `nsId` is never reused a reader can always tell a
  reused slot from the old one. Allocation and free write the label (`ns="acme",ns_id="42"`) and run only on the
  control thread, never on the hot path. They return an error code instead of throwing and allocate nothing.

**Capacity is fixed at broker start.**
- `maxNamespaces` is a cluster setting checked by the catalog: `CreateNamespace` beyond it is refused with
  `NAMESPACE_LIMIT_EXCEEDED`.
- Each node sizes its metrics file at start for `maxNamespaces × (per-thread metrics × threads + per-node
  metrics)` plus the node's own metrics, and refuses to start if that is below the cluster's `maxNamespaces`.
- The set of metrics is a compile-time enum; nothing is registered by name at runtime.

**Ownership.** One writer per slot, always the thread that owns the state:
| Metric | Writer |
|---|---|
| object counts | catalog agent (applies `__catalog`) and security agent (users) |
| produce/fetch bytes and records, request ops, query cycles | each gateway/worker thread, its own slot per namespace |
| disk bytes | the worker that owns the partition, its own slot per namespace |
| connections, sessions, open transactions | the gateway / transaction coordinator that owns them |
| view bytes | the view's owning worker |

### 5.4 Enforcement
| Kind | Check | Refusal |
|---|---|---|
| Object counts, durability, features, static ceilings | at DDL, login or connect, on the thread that owns the count, against the current record | `NAMESPACE_LIMIT_EXCEEDED`; nothing is applied |
| Connections, sessions, open transactions | at login, connect, `BeginTxn`, by the owning thread | `NAMESPACE_LIMIT_EXCEEDED` |
| Rates | see below | `THROTTLED(retryAfterNs)` |
| Disk bytes | the aggregator sets the namespace's over-disk state on each thread | produce gets `NAMESPACE_LIMIT_EXCEEDED` |
| View memory | the view's worker, when an applied batch would pass the limit | the view moves to `PAUSED(OVER_LIMIT, offset)` (below) |

**Rates.** One aggregator on the node's control agent, driven by `doWork(nowNs)`:
1. Every tick it acquire-loads each thread's per-namespace totals and computes the rate over the window.
2. When a namespace is over, it sends `Throttle(nsSlot, nsId, untilNs)` to each thread through that thread's ring;
   when it is back under, `Throttle(nsSlot, nsId, 0)`. A thread ignores a `Throttle` whose `nsId` is not the one it
   holds for that slot, so a late message about a deleted namespace never throttles the slot's next owner.
3. Each thread keeps a preallocated `throttleUntilNs[maxNamespaces]` it alone owns. The hot-path check is one compare:
   `nowNs < throttleUntilNs[nsSlot]`. A throttled produce is refused with `THROTTLED(retryAfterNs)` before
   anything is appended, so nothing is stored or lost and the client retries after the delay. A client that
   ignores the delay is refused again at the cost of one branch.

This replaces the earlier per-thread token buckets (limit ÷ threads): the limit is never re-sliced when threads or
load shift. Overshoot is bounded by one tick's traffic and is covered by a test with a fixed tick and load.

**Views over the memory limit** stop applying instead of dropping rows, so a view always equals a recompute of its
log. Reads return `VIEW_PAUSED`. When the limit is raised, or the view's memory falls, it resumes from its offset.

**Hot-path budget.** The only hot-path additions are one owned-slot add per request and one throttle compare. Both
allocate nothing, and they must pass an ABAB bench against the CLAUDE.md latency budgets.

## 6. TruncateTopic
- **Wire request:**
  - `TruncateTopic(topicName, entries[(partition, beforeOffset)], DdlExtension)`.
  - `beforeOffset = -1` means "as far as is available".
  - It needs the `Truncate` grant on the topic.
  - An empty `entries` list means every partition. An unknown partition fails that entry only, with `INVALID_REQUEST`;
    the other entries still apply.
  - Internal (`__*`) topics are refused with `INVALID_REQUEST`, for every caller, the cluster admin included.
- **What it does:** it advances the partition's `logStartOffset` to the furthest offset available when the
  leader executes the request: `min(beforeOffset, HW, LSO)`, where the LSO (last stable offset) is the HW on a
  topic with no open transactions. Records of a transaction still open stay; everything below them goes. The leader
  resolves the offset once and writes it into the replicated record, so every replica applies that exact offset;
  the response returns it. A retry after a failed request resolves again on the current leader. The start never
  moves backwards: a `beforeOffset` at or below the current start changes nothing and returns the current start.
- **Durability:** the reply is sent only once every ISR replica reports a start at or above the new one (Kafka's
  DeleteRecords low watermark), so a leader change after the reply can never bring truncated records back: only ISR
  replicas are electable, and each of them already holds the new start in its checkpoint.
- **Topic kinds:** user topics, transactional or not, and transient topics can be truncated; only internal (`__*`)
  topics are refused.
- **Flow** (the Kafka DeleteRecords path; no new record kind):
  1. The leader resolves the offset, writes its log-start checkpoint (`writeLogStartCheckpoint_`, as retention does)
     and only then moves its in-memory start. From then on every `FetchResponse` carries the new `logStartOffset`
     (the field exists today). A start change completes the fetches parked on that partition, as a HW change does.
  2. **Follower adoption, disk and transient logs alike** (today only transient logs adopt, via
     `resetTransientLogTo_`). On any fetch response whose `logStartOffset` is above the follower's own start:
     - if it is at or below the follower's LEO, the follower checkpoints it and moves its start (as retention does);
     - if it is above the follower's LEO, the follower holds nothing the leader still has: it checkpoints the new
       start, drops every segment and resumes empty at that offset (Kafka: a follower out of range below the
       leader's start truncates fully). Boot recovery treats a checkpoint above every segment as an empty log at
       that offset, so a crash between the two steps converges.
  3. **Follower report.** `FetchRequest` gets a `logStartOffset` field (the follower's checkpointed start; no
     mixed-version clusters exist). The leader keeps it per follower slot; the ISR low watermark is the minimum over
     the ISR, the leader included. The truncate reply waits until the low watermark reaches the new start. An ISR
     shrink also completes it, since the replica that left can only come back through the fetch path and adopts the
     start there. A request timeout returns `REQUEST_TIMED_OUT` and the client retries; the retry resolves again
     and is idempotent, because a start never moves backwards.
  4. **A fetch below the leader's start** gets a record-less answer carrying the leader's start, so the follower
     takes step 2. Today the leader answers this only for transient logs; a disk follower below the first segment
     gets an empty read and no answer (`PartitionReplica::handleFetchRequest`, the `view.empty()` skip), which looks
     like an existing hang. Phase 3 starts with a failing repro test for it and fixes it on its own.
  5. Whole segments below the new start are deleted asynchronously, the same way retention deletes them.
  6. **Retention** uses the same path: a leader's retention start also reaches followers, as in Kafka.
  7. **New leader:** a leader elected before the reply may hold a lower start than the one resolved. Nothing was
     acknowledged, so that is correct; followers never move their start back, and the client's retry resolves on
     the new leader.
- **Response:** the new `logStartOffset` for each partition.
- **Consumers:** a fetch below the start returns `OFFSET_OUT_OF_RANGE`, which the client's reset policy handles, as it does after retention.
- **Views:** truncate cuts every view that reads the topic up to the same offset. Afterwards each view equals a
  recompute over the remaining log `[newStart, HW)` of its sources, so rows that came only from removed records
  are gone and aggregates no longer count them. This differs from retention, which leaves a view's state alone; a
  truncate is an explicit cut.
  - The view's worker rebuilds into a shadow state from the new start, keeps serving the old state meanwhile, and
    swaps once the shadow has caught up with the live state's offset. Readers never see a partly rebuilt view.
  - Every replica cuts its own views when it applies the new start.
  - The shadow counts against the namespace's view memory limit. A cut only removes records, so the view's current
    size bounds the shadow. The leader reserves that much before it resolves the offset; if it cannot, the truncate
    is refused with `NAMESPACE_LIMIT_EXCEEDED` before anything changes. A follower whose shadow cannot fit moves
    the view to `PAUSED(OVER_LIMIT, offset)` (§5.4) instead of diverging.
  - View snapshots taken below the new start are discarded, so a restart rebuilds from the new start instead of
    restoring rows that were cut.
- **Interactions:**
  - A transaction open across the truncate point keeps its producer state; only the records are gone.
  - On a compacted topic, truncate removes all records below the new start, however recent the key.

## 7. Phases
0. **Metrics:** the arcanum-metrics changes in §5.3 (single-writer slots, injected clock, allocate/free with a registration id, no-throw), then every Rune Aeron counter moved onto it, with `docs/operations.md`.
1. **Namespaces:**
   - catalog events and the `nsId` registry;
   - names and resolution, the `default` namespace, the disk layout;
   - `ns/user` users, roles and escalation guards;
   - the namespace admin requests and `rune namespace`.
2. **Lifecycle and limits:**
   - suspend and the crash-safe delete;
   - `nsId`-keyed internal topics;
   - per-namespace metric blocks, allocated on create and freed on delete;
   - enforcement of counts, storage, view memory and control-plane limits;
   - the rate aggregator, `Throttle` over the rings, and the ABAB bench.
3. **TruncateTopic:** first the below-start fetch fix (§6 Flow step 4, repro test first); then wire, broker, replication (follower adoption and the low watermark) and client; view cut and rebuild; `rune topic truncate`.

Each phase gets its own plan. The `rune namespace` and `rune topic truncate` commands (§10) ship with phases 1–3.
The Arcanum CLI work and NAT-safe egress have their own specs (§10).

## 8. Interaction matrix
Operations × entities × timings:
- **Operations:** create, suspend (`ReadOnly`, `RejectAll`, switch mode), resume, delete, recreate the same name, set limits (raise, lower below usage, lower max retention), truncate (`-1`, an explicit offset, below the current start, every partition).
- **Callers:** cluster admin, namespace admin, plain user, anonymous; in the namespace and in another one.
- **Entities:** user, transactional, transient and internal (`__*`) topics; views, including ones over several topics and paused ones; groups, open transactions, users, roles and live sessions in the namespace; other namespaces.
- **Timings:**
  - sequential;
  - back-to-back before the reply;
  - mid-delete at each step;
  - leader change mid-operation;
  - restart between steps;
  - follower offline during the delete or truncate, then catching up;
  - a follower whose LEO is below the new start (disk and transient), and a crash between its checkpoint and its segment drop;
  - an ISR shrink and a leader change while a truncate waits for the low watermark;
  - truncate during a view rebuild, and a second truncate before the first rebuild swaps;
  - a limit change between an aggregator tick and the `Throttle` reaching a thread;
  - a delete and recreate between a `Throttle` being sent and arriving (slot reuse);
  - a suspend while a transaction is open and while a truncate's view rebuild runs.

Required:
- table-driven unit tests;
- 1-node and 3-node broker integration tests;
- an e2e test on the default config;
- a seeded randomized model test of `create`/`delete`/`recreate` against produce, consume and DDL. Its invariant: no data is visible across namespaces, and nothing of a deleted `nsId` survives.

## 9. Review focus
- **Cross-namespace leaks:** in name resolution, the internal-topic keys, grants, metadata responses and error messages.
- **Escalation through a sibling operation:** a namespace admin reaching cluster-admin, another namespace, or the limits.
- **Truncate durability:** no reply before the ISR low watermark reaches the start; no replica's start ever moves back; a follower below the leader's start always gets an answer.
- **Delete convergence:** across crashes, a lagging follower and recreating the same name.
- **Suspend:** every request type sits in the allowed/refused table for both modes; open transactions are aborted; the cluster admin is never locked out.
- **Catalog versions:** a formatVer 1 reader still refuses the namespace kinds; a formatVer 2 reader applies every v1 event exactly as v1 does.
- **Truncate correctness:** the start never moves backwards, including after a leader change following the reply; the HW and LSO clamps hold, and an open transaction's records survive; internal topics are refused for everyone; after the cut every view equals a recompute of `[newStart, HW)`, including after a restart.
- **Metrics:** one writer per slot; a freed and reused slot never mixes two `nsId`s, in the metrics or in a late `Throttle`; usage is charged to the object's namespace; usage always equals the owner's state.
- **Hot path:** zero allocations and the ABAB bench for the slot add and the throttle compare.

## 10. Commands
- `rune topic truncate <topic> [--partition N] [--before <offset>]`: TruncateTopic (§6).
- `rune namespace create|delete|suspend|resume|list|limits`: cluster admin only. `limits` shows each limit next to
  its usage metric.
- User and grant management stays QL through `rune console` or `rune ql`, run as a namespace admin.

Split into their own specs:
- the `arcanum` platform CLI, the shared login context and `rune login`:
  [2026-10-26-arcanum-cli-design.md](2026-10-26-arcanum-cli-design.md);
- NAT-safe egress: [2026-10-26-rune-nat-egress-design.md](2026-10-26-rune-nat-egress-design.md).

## 11. Anonymous read access
Full design: arcanum-site `docs/specs/2026-10-24-arcanum-auth-design.md` §5. The `rune login` side is in the CLI spec (§10).
- **Off by default.** The `NamespaceLimits` anonymous-read flag defaults to off, and CLAUDE.md §8 records it. A
  cluster admin turns it on for `default` explicitly.
- **Anonymous access**, when on:
  - Under `GatewayAuthMode::Required`, a client that connects without starting SCRAM gets a new read-only anonymous session state. This is different from today's full-access `AuthAnonymous`, which exists only with `RUNE_AUTH=disabled`.
  - It runs as the namespace's built-in `anonymous` principal. Its only default grant is schema and catalog read, so `schema list|get|generate` and catalog reads work.
  - QL, view reads, produce, consume and all DDL return `UNAUTHORIZED`; the CLI adds a "run `rune login`" hint.
- **Controls:** a namespace admin can revoke the grant; the `NamespaceLimits` flag above turns the whole path on or off.
- **Scope:** an anonymous session only ever reaches `default`, because an anonymous client never logs in and
  `ConnectRequest` has no namespace field. A namespace that wants public reads publishes through `default`.
- **Limits:** anonymous sessions count against the namespace's connection and rate limits, with a separate small cap building on issue #236.
- **Tests:** adversarial checks that anonymous never passes any gate but schema read, including through a sibling operation, and never sees another namespace.
