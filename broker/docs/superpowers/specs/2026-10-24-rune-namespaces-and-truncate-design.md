# Rune namespaces (multi-tenancy) and topic truncate: design

**Status:** draft, 2026-10-24.
**Scope:** namespaces with dynamic create, suspend and delete; namespace-scoped users and grants; per-namespace limits set by the cluster admin and measured with arcanum-metrics; a `TruncateTopic` operation that also cuts the views over the topic.
**Out of scope:** CPU caps through dedicated threads or nodes, per-namespace encryption at rest (it is a reserved limit only), and an account level above namespaces.

## 1. Model
- **Namespace:** the isolation unit, created and limited by the cluster admin.
- **Contents:** topics, views, schemas, consumer groups, transactions and users all belong to exactly one namespace.
- **Names:** every object is named by the pair (namespace, name).
- **`default` namespace:** always exists and can never be deleted. A name without a namespace resolves to `default`, so existing clients keep working.
- **Identity:** each namespace has a `nsId`, a u32 that the catalog assigns and never reuses. It is the namespace's incarnation: deleting `acme` and creating it again gives a new `nsId`, so stale state can never match the new namespace.
- **Disk layout:** `<data>/ns-<nsId>/topics/<name>/<partition>/...`.
- **No in-place upgrade.** A cluster with namespaces is a new cluster; existing data, users and grants are not migrated, so there is no upgrade rename and no mixed-version cluster.
- **Memory layout:** a per-node map from `nsId` to that namespace's objects, owned by the metadata or catalog agent.
- **Hot path is unchanged:**
  - Topic and view ids stay globally unique, so the produce, consume and query paths never see `nsId`.
  - A namespace is resolved once, when a name is turned into an id.
  - That lookup is checked against the session's namespace grants.

## 2. Roles and users
| Role | Can do |
|---|---|
| Cluster admin | Root: every operation in every namespace, data included, with no grant needed. Create, suspend and delete namespaces; set `NamespaceLimits`; appoint namespace admins. |
| Namespace admin | Within its own namespace: create, delete and change the password of users; grant and revoke permissions; run topic, view and schema DDL. |
| User | Whatever its grants allow, within its own namespace. |

- **User names** have the form `ns/user` and live in `__security` (SCRAM). Logging in fixes the session's namespace; `ConnectRequest` gets no namespace field.
- **Escalation guards**, each enforced in the security agent and each covered by an adversarial test. A namespace admin can never:
  - grant cluster-admin;
  - grant on, or touch users in, another namespace;
  - raise a limit;
  - create or delete a namespace.
- Grants stay allow-only, and everything is denied by default.

## 3. Catalog events
New event kinds go in `__catalog`; these kinds are already reserved there:
- `NamespaceCreated(nsId, name, limits)`;
- `NamespaceLimitsSet(nsId, limits)`;
- `NamespaceStateChanged(nsId, state)`;
- `NamespaceDeleted(nsId)`.

These events use a new catalog formatVer 2 rule set: v2 accepts the namespace kinds and applies every v1 kind exactly as v1 does. The formatVer 1 rules never change and keep refusing the namespace kinds. No version gate is needed: there are no mixed-version clusters and no in-place upgrade.

Users and grants for a namespace go in `__security`, tagged with its `nsId`. Every node applies the events in log order, and applying the same event twice changes nothing.

## 4. Lifecycle
A rich-state machine, owned by the catalog agent. Every state handles every event.

```
ACTIVE ⇄ SUSPENDED(mode: ReadOnly | RejectAll)
ACTIVE|SUSPENDED → DELETING(pendingTopics, pendingViews, pendingGroups, openTxns, users) → (removed)
```

**Suspend.**
- In `ReadOnly` mode, the gateway refuses produce, all DDL, transactions and consumer-group offset commits with `NAMESPACE_SUSPENDED`, and allows fetch, view reads and QL reads. Use it to freeze a namespace while its owner can still get data out.
- In `RejectAll` mode, every request and every login fails with `NAMESPACE_SUSPENDED`, and open sessions are closed.

**Delete**, in this order:
1. Append `NamespaceDeleted`. New requests and logins for the namespace are rejected from then on.
2. Fence and close its sessions, and revoke its users.
3. Abort its open transactions.
4. Delete its topics, views and schemas through the existing `DropTopic` and `DropView` paths.
5. Purge its keys from `__consumer_offsets` and the transaction state with tombstones.
6. On each replica, rename `ns-<nsId>` to `ns-<nsId>.deleting` (atomic), then remove it in the background.

**Crash safety.** On boot, and when a follower catches up, any `ns-*.deleting` directory is removed, and any namespace marked deleted in the catalog has its remaining steps re-run. All steps are idempotent. Convergence comes from the log plus an idempotent restore, never from a timeout (rule 16).

**Internal topics** (`__consumer_offsets`, transaction state) stay shared, and every key starts with `nsId`. `__catalog` and `__security` are cluster-wide.

## 5. NamespaceLimits
Rune has no tiers or plans. It knows only the limits a cluster admin puts on a namespace and its users. Whatever
sells those limits (the Arcanum platform, an operator script) sits outside Rune and acts as the cluster admin.

### 5.1 Model
- **Set at create.** `CreateNamespace(name, limits)` carries the full limit record, and `NamespaceCreated(nsId,
  name, limits)` stores it. A limit the admin leaves out takes the cluster default *at that moment*, and the
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
| Features | transactions, QL, views, SQL/Redis facades, anonymous read (§11); allowed auth mechanisms | namespace | none |

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
2. When a namespace is over, it sends `Throttle(nsSlot, untilNs)` to each thread through that thread's ring; when it
   is back under, `Throttle(nsSlot, 0)`.
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
  - `beforeOffset = -1` means "up to the high watermark".
  - It needs the `Truncate` grant on the topic.
  - Internal (`__*`) topics are refused with `INVALID_REQUEST`, for every caller, the cluster admin included.
- **What it does:** it advances the partition's `logStartOffset` to the furthest offset available when the
  leader executes the request: `min(beforeOffset, HW, LSO)`, where the LSO (last stable offset) is the HW on a
  topic with no open transactions. Records of a transaction still open stay; everything below them goes. The offset
  is resolved once, at execution, and returned in the response; it is never re-resolved on retry or failover. It
  never moves the start backwards.
- **Topic kinds:** user topics, transactional or not, and transient topics can be truncated; only internal (`__*`)
  topics are refused.
- **Flow:**
  1. The leader appends a `LogStartAdvanced(partition, offset)` control record.
  2. That record replicates through the normal path, so followers apply the same start.
  3. Whole segments below the new start are deleted asynchronously, the same way retention deletes them.
- **Response:** the new `logStartOffset` for each partition.
- **Consumers:** a fetch below the start returns `OFFSET_OUT_OF_RANGE`, which the client's reset policy handles, as it does after retention.
- **Views:** truncate cuts every view that reads the topic up to the same offset. Afterwards each view equals a
  recompute over the remaining log `[newStart, HW)` of its sources, so rows that came only from removed records
  are gone and aggregates no longer count them. This differs from retention, which leaves a view's state alone; a
  truncate is an explicit cut.
  - The view's worker rebuilds into a shadow state from the new start, keeps serving the old state meanwhile, and
    swaps once the shadow has caught up with the live state's offset. Readers never see a partly rebuilt view.
  - The shadow counts against the namespace's view memory limit. If it cannot fit, the truncate is refused with
    `NAMESPACE_LIMIT_EXCEEDED` before anything changes.
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
   - CLI and admin commands.
2. **Lifecycle and limits:**
   - suspend and the crash-safe delete;
   - `nsId`-keyed internal topics;
   - per-namespace metric blocks, allocated on create and freed on delete;
   - enforcement of counts, storage, view memory and control-plane limits;
   - the rate aggregator, `Throttle` over the rings, and the ABAB bench.
3. **TruncateTopic:** wire, broker, replication and client; view cut and rebuild; the CLI.
4. **CLI:** `rune` reads the shared Arcanum login context; `arcanum` plugin dispatch; `topic truncate`, `topic list` and `namespace` commands. The platform commands in §10.2 come with the control-plane service.
5. **NAT-safe egress:** per-session egress on a client-opened flow; per-worker egress on by default; NAT e2e leg (§10.5).

## 8. Interaction matrix
Operations × entities × timings:
- **Operations:** create, suspend, resume, delete, recreate the same name, set limits (raise, lower below usage), truncate.
- **Entities:** topics, views, groups, open transactions, users and live sessions in the namespace; other namespaces.
- **Timings:**
  - sequential;
  - back-to-back before the reply;
  - mid-delete at each step;
  - leader change mid-operation;
  - restart between steps;
  - follower offline during the delete or truncate, then catching up;
  - truncate during a view rebuild, and a second truncate before the first rebuild swaps;
  - a limit change between an aggregator tick and the `Throttle` reaching a thread.

Required:
- table-driven unit tests;
- 1-node and 3-node broker integration tests;
- an e2e test on the default config;
- a seeded randomized model test of `create`/`delete`/`recreate` against produce, consume and DDL. Its invariant: no data is visible across namespaces, and nothing of a deleted `nsId` survives.

## 9. Review focus
- **Cross-namespace leaks:** in name resolution, the internal-topic keys, grants, metadata responses and error messages.
- **Escalation through a sibling operation:** a namespace admin reaching cluster-admin, another namespace, or the limits.
- **Delete convergence:** across crashes, a lagging follower and recreating the same name.
- **Truncate correctness:** the start never moves backwards; the HW clamp holds; after the cut every view equals a recompute of `[newStart, HW)`, including after a restart.
- **Metrics:** one writer per slot; a freed and reused slot never mixes two `nsId`s; usage always equals the owner's state.
- **Hot path:** zero allocations and the ABAB bench for the slot add and the throttle compare.

## 10. CLI: `arcanum` (platform) and `rune` (service)
The platform CLI is `arcanum`. Rune is one service under it; more services will follow.

**Two binaries, with one shared login.** The `kubectl`/`gcloud` split.
- **`arcanum`:** the platform CLI, for accounts, projects, deploy and teardown (§10.2).
- **`rune`:** the Rune service CLI, the existing binary with every command unchanged: `rune schema list`, `rune console` and so on. You never have to type an `arcanum` prefix.
- **Shared context:** both read the same login and current project from the Arcanum config directory. `rune` resolves the endpoint and credentials from there, so `--broker` is not needed after `arcanum login`.
- **Plugins:** `arcanum <service> <args>` runs the service's binary with those args, the way `git` runs its plugins, so `arcanum rune schema list` equals `rune schema list`. Future services ship their own binary the same way.
- **Self-hosted:** `rune` with no login works exactly as today, using `--broker` and `--user`/`--password-file`.

### 10.1 Platform model
Rune knows nothing of accounts, projects, tiers or billing. The platform model and its commands (`arcanum login`,
`deploy`, `teardown` and so on) are specified with the Arcanum control-plane service, a separate project.

The contract between them is small:
- the control plane acts as a cluster admin, through `rune namespace create|limits|suspend|resume|delete` or the
  same admin requests;
- whatever a plan sells is expressed only as a `NamespaceLimits` record (§5) or as a separate cluster;
- usage shown to a customer comes from the namespace's metrics (§5.3).

### 10.3 `rune` commands
- **Connection:** when signed in, the control plane hands out the namespace-admin SCRAM credential for the current project, and `rune` caches it with mode 0600.
- **Existing commands, unchanged:**
  - `console`;
  - `ql`;
  - `schema get|list|generate`;
  - `catalog export|replay|diff`;
  - `security bootstrap|gen-cluster-key`.
  - Self-hosted only: `security bootstrap` and `gen-cluster-key`. On the platform these are run by the control plane.
- **New commands from this spec:**
  - `rune topic truncate <topic> [--partition N] [--before <offset>]`: TruncateTopic (§6).
  - `rune namespace create|delete|suspend|resume|list|limits`: cluster admin only, so self-hosted or the control plane. `limits` shows each limit next to its usage metric.
  - User and grant management is QL through `console` or `ql`, as today; it runs as a namespace admin.
- **Review notes:**
  - `schema list` actually lists topics. Add `topic list` as the canonical command and keep `schema list` as an alias.
  - Error messages keep their existing `rune ...:` prefix.

### 10.4 Selecting and connecting to a deployment
- **Context:** (project, environment) picks the Rune deployment. Start with one Rune deployment per project; environments such as `--env dev|prod` can come later.
  - `arcanum use <project> [--env]` sets the current context.
  - `--project`/`--env` override it for one command, and `RUNE_PROJECT`/`RUNE_ENV` override it for a shell or CI.
- **Resolution:** the first time a context is used, `rune` calls the control plane with the `arcanum` token. It gets back:
  - the bootstrap broker endpoints (the rest of the cluster comes from metadata);
  - the namespace;
  - the cluster TLS identity, which the client pins;
  - a short-lived namespace-admin credential.

  These are cached with mode 0600, per context. On a connect or auth failure, `rune` refreshes once.
- **Precedence:** `--broker`/`--user` flags, then `RUNE_BROKER`/`RUNE_USER`, then the Arcanum context. Self-hosted use needs only the first two.
- **Apps:** apps never use a personal login.
  - `arcanum rune credentials create <name> --grants ...` creates a service user in the namespace and prints `rune://<ns>/<user>@host1:port,host2:port?tls=<fingerprint>` plus a secret.
  - The client library accepts that connection string.
  - `credentials revoke <name>` removes the user and fences its sessions.

### 10.5 Reachability through NAT
Clients run behind NAT: home routers, cloud NAT gateways, most Kubernetes pods. A broker can only reach them over a flow the client opened, the way memcached or DNS replies go to the request's source address.
- **Records egress is already NAT-safe.** It uses per-worker dynamic-MDC publications (`control-mode=dynamic`, `BrokerNode.cpp`). The client's status messages leave from its receive socket, and the broker sends to that observed source address. This is on in shard+fused mode, or with `perWorkerEgressEnabled`; it is off by default otherwise.
- **Per-session egress is not NAT-safe.** That covers ConnectResponse, acks, DDL replies, errors, and Records when per-worker egress is off.
  1. The client declares its own `egressChannel`, defaulting to `aeron:udp?endpoint=localhost:40200`, in ConnectRequest.
  2. The gateway's `SessionAnnouncer` makes the workers open a publication to that address.

  Behind NAT that address is private or unmapped, so the client never receives the replies.
- **Fix:**
  - move per-session egress onto a client-opened flow: dynamic MDC on the session egress, or Aeron response channels (`control-mode=response`);
  - make per-worker egress the default;
  - stop sending a declared endpoint in ConnectRequest.
- **Operations:**
  - one public UDP endpoint per node, plus each worker's egress control port;
  - security groups or firewalls opened for those ports;
  - client keepalive below typical NAT UDP timeouts (about 30 s).
- **Tests:** an e2e leg with the client behind a NAT, e.g. a docker network with masquerade, on the default config.

## 11. Rune login and anonymous read access
Full design: arcanum-site `docs/specs/2026-10-24-arcanum-auth-design.md` §5.
- **Two separate logins.**
  - The Arcanum CLI token is used only against the control plane and never reaches the broker.
  - Rune users log in to the broker with SCRAM via `rune login --user`. The CLI stores the derived `ClientKey`/`ServerKey` for each context, never the password. `rune logout` deletes them, and `rune whoami` shows both identities.
- **Anonymous access by default.**
  - Under `GatewayAuthMode::Required`, a client that connects without starting SCRAM gets a new read-only anonymous session state. This is different from today's full-access `AuthAnonymous`, which exists only with `RUNE_AUTH=disabled`.
  - It runs as the namespace's built-in `anonymous` principal. Its only default grant is schema and catalog read, so `schema list|get|generate` and catalog reads work.
  - QL, view reads, produce, consume and all DDL return `UNAUTHORIZED`; the CLI adds a "run `rune login`" hint.
- **Controls:** a namespace admin can revoke the grant, and a `NamespaceLimits` flag lets the cluster admin disable anonymous access.
- **Scope:** an anonymous session only ever sees its own namespace.
- **Limits:** anonymous sessions count against the namespace's connection and rate limits, with a separate small cap building on issue #236.
- **Tests:** adversarial checks that anonymous never passes any gate but schema read, including through a sibling operation, and never sees another namespace.

## 12. Open questions
Still to decide. Each has the recommended answer; a line is removed when the decision is written into the
section it belongs to.

**Code facts the spec must match**
3. `SecurityOperation` and `SecurityResourceType` are frozen enums with no `Truncate` or `Namespace`. Recommended:
   append `Truncate = 7` and `Namespace = 5`; never renumber.
4. `Log` already has a log-start-offset checkpoint, and the replication fetch carries `logStartOffset` (the Kafka
   DeleteRecords path). Recommended: truncate uses that path instead of a new `LogStartAdvanced` control record.
5. Roles exist (`CreateRole`, `GrantRole`). Recommended: roles are namespaced like users.

**Naming**
6. A bare name resolves in the session's namespace, not always in `default`. Recommended: yes.
8. Namespace name rules. Recommended: `[a-z0-9][a-z0-9-]{0,62}`; `__`-prefixed names are reserved.
9. `ObjectIndex::byName` is global. Recommended: key it by `(nsId, name)`.
10. Group and transactional ids. Recommended: keyed by `(nsId, id)`, as the internal-topic keys are.

**Admin**
13. The security-admin guard stays cluster-wide. Recommended: yes.

**Lifecycle**
14. `NamespaceDeleted` is the only commit point; every owner then reconciles idempotently. Recommended: yes.
15. Recreating a name while the old `nsId` is still `DELETING`. Recommended: allowed; the new `nsId` cannot match
    the old state.
17. `default` can be suspended. Recommended: yes.

**Scope**
20. Split NAT egress (§10.5) and the CLI work into their own specs. Recommended: yes.
21. Anonymous read (§11) is off by default, and CLAUDE.md §8 records it. Recommended: yes.
