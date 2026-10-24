# Rune namespaces (multi-tenancy) and topic truncate: design

**Status:** draft, 2026-10-24.
**Scope:** namespaces with dynamic create, suspend and delete; namespace-scoped users and grants; per-namespace limits set by the cluster admin; a `TruncateTopic` operation.
**Out of scope:** CPU caps through dedicated threads or nodes, per-namespace encryption at rest (it is a reserved limit only), and an account level above namespaces.

## 1. Model
- **Namespace:** the isolation unit. A platform account owns one or more namespaces.
- **Contents:** topics, views, schemas, consumer groups, transactions and users all belong to exactly one namespace.
- **Names:** every object is named by the pair (namespace, name).
- **`default` namespace:** always exists and can never be deleted. A name without a namespace resolves to `default`, so existing clients keep working.
- **Identity:** each namespace has a `nsId`, a u32 that the catalog assigns and never reuses. It is the namespace's incarnation: deleting `acme` and creating it again gives a new `nsId`, so stale state can never match the new namespace.
- **Disk layout:** `<data>/ns-<nsId>/<topic>-<partition>/...`.
  - First boot after upgrade: move the existing data into `ns-<defaultId>/`. Use one idempotent rename per directory, and finish any half-done move on the next boot.
- **Memory layout:** a per-node map from `nsId` to that namespace's objects, owned by the metadata or catalog agent.
- **Hot path is unchanged:**
  - Topic and view ids stay globally unique, so the produce, consume and query paths never see `nsId`.
  - A namespace is resolved once, when a name is turned into an id.
  - That lookup is checked against the session's namespace grants.

## 2. Roles and users
| Role | Can do |
|---|---|
| Cluster admin | Create, suspend and delete namespaces. Set `NamespaceLimits`. Appoint namespace admins. Everything else. |
| Namespace admin | Within its own namespace: create, delete and change the password of users; grant and revoke permissions; run topic, view and schema DDL. |
| User | Whatever its grants allow, within its own namespace. |

- **User names** have the form `ns/user` and live in `__security` (SCRAM). Logging in fixes the session's namespace.
- **Escalation guards**, each enforced in the security agent and each covered by an adversarial test. A namespace admin can never:
  - grant cluster-admin;
  - grant on, or touch users in, another namespace;
  - raise a limit;
  - create or delete a namespace.
- Grants stay allow-only, and everything is denied by default.

## 3. Catalog events
New event kinds go in `__catalog`; these kinds are already reserved there:
- `NamespaceCreated(nsId, name)`;
- `NamespaceLimitsSet(nsId, limits)`;
- `NamespaceStateChanged(nsId, state)`;
- `NamespaceDeleted(nsId)`.

Users and grants for a namespace go in `__security`, tagged with its `nsId`. Every node applies the events in log order, and applying the same event twice changes nothing.

## 4. Lifecycle
A rich-state machine, owned by the catalog agent. Every state handles every event.

```
ACTIVE ⇄ SUSPENDED(mode: ReadOnly | RejectAll)
ACTIVE|SUSPENDED → DELETING(pendingTopics, pendingViews, pendingGroups, openTxns, users) → (removed)
```

**Suspend.**
- In `ReadOnly` mode, the gateway rejects produce, DDL and transactional work, and allows fetch and reads.
- In `RejectAll` mode, every request fails with `NAMESPACE_SUSPENDED`, and open sessions are closed.

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
Only the cluster admin can set these, in one `NamespaceLimits` record per namespace.
- **Ceilings:** each limit is a ceiling. Namespace admins can set tighter values per user or per topic, never looser.
- **Unset:** an unset limit falls back to the cluster default.
- **Lowering a limit:** never removes existing objects or data. New creates are refused, and retention is clamped to the new maximum.

**Throughput**
- Produce rate, in bytes/s and records/s.
- Fetch rate, in bytes/s.
- Request rate, i.e. operations per second including admin and metadata calls.
- Query/view read rate and cost (CPU time per query).

**Storage**
- Total disk bytes for the namespace. When it's reached, produce is rejected; existing data is never deleted.
- Most retention time and bytes a topic may set.
- Whether compaction is allowed, and its settings.
- Largest allowed message size and batch size.

**Object counts**
- Topics, partitions per topic and total partitions.
- Views, plus memory per view (materialized state).
- Schemas, and the number of versions per schema.
- Consumer groups.
- Users.

**Connections**
- Concurrent connections, both per namespace and per user.
- Sessions per user.
- Open transactions, and the longest a transaction may run.

**Durability and placement**
- Replication factor, minimum and maximum, so nobody runs RF=1 on a shared cluster or uses up replicas.
- Lowest allowed `acks` and lowest `min.insync`, if you want a durability floor.
- Placement: which nodes or racks the namespace may use. This leaves room for dedicated nodes later.

**Security and features**
- Allowed auth mechanisms and the password policy.
- Feature flags, for example whether the namespace may use transactions, QL, views or SQL/Redis facades.
- Encryption at rest with its own key per namespace (later).

**State**
- Suspend, which makes the namespace read-only or rejects everything, e.g. when a bill isn't paid. Delete has the same lifecycle but goes further.

### Enforcement
| Kind | Where | How |
|---|---|---|
| Counts, retention, RF, acks, features, auth, placement | DDL, login, connect (control plane) | Checked against the limits; the request is refused with `NAMESPACE_LIMIT_EXCEEDED`. |
| Disk bytes | A storage agent sums segment sizes off the hot path and publishes a per-namespace over-limit flag to the gateways over the ring buffers. | Produce is rejected while the flag is set. |
| View memory | Charged when a row is inserted into a view or on view DDL, using the preallocated budget. | The write is refused. |
| Rates (produce, fetch, request, query CPU) | Each gateway or worker thread holds its own token bucket with a slice of the limit (limit ÷ threads). The bucket is preallocated and the check is branch-only. Query CPU is charged in `rdtsc` cycles. | The response carries a throttle time (backpressure). Data is never dropped. |

**Hot-path budget.** The only hot-path addition is the token-bucket check. It must allocate nothing and pass an ABAB bench against the CLAUDE.md latency budgets.

## 6. TruncateTopic
- **Wire request:**
  - `TruncateTopic(topicName, entries[(partition, beforeOffset)], DdlExtension)`.
  - `beforeOffset = -1` means "up to the high watermark".
  - It needs the `Truncate` grant on the topic.
- **What it does:** it advances the partition's `logStartOffset` to `min(beforeOffset, HW)`. It never moves the start backwards.
- **Flow:**
  1. The leader appends a `LogStartAdvanced(partition, offset)` control record.
  2. That record replicates through the normal path, so followers apply the same start.
  3. Whole segments below the new start are deleted asynchronously, the same way retention deletes them.
- **Response:** the new `logStartOffset` for each partition.
- **Consumers:** a fetch below the start returns `OFFSET_OUT_OF_RANGE`, which the client's reset policy handles, as it does after retention.
- **Views:** keep the state they have already materialized, as they do after retention. Truncate never rewinds a view.
- **Interactions:**
  - A transaction open across the truncate point keeps its producer state; only the records are gone.
  - On a compacted topic, truncate removes all records below the new start, however recent the key.

## 7. Phases
1. **Namespaces:**
   - catalog events and the `nsId` registry;
   - names and resolution, the `default` namespace, the disk layout and upgrade rename;
   - `ns/user` users, roles and escalation guards;
   - CLI and admin commands.
2. **Lifecycle and limits:**
   - suspend and the crash-safe delete;
   - `nsId`-keyed internal topics;
   - enforcement of counts, storage, memory and control-plane limits;
   - rate token buckets and the bench.
3. **TruncateTopic:** wire, broker, replication and client, plus the CLI.
4. **CLI:** `rune` reads the shared Arcanum login context; `arcanum` plugin dispatch; `topic truncate`, `topic list` and `namespace` commands. The platform commands in §10.2 come with the control-plane service.

## 8. Interaction matrix
Operations × entities × timings:
- **Operations:** create, suspend, resume, delete, recreate the same name, set limits, truncate.
- **Entities:** topics, views, groups, open transactions, users and live sessions in the namespace; other namespaces.
- **Timings:**
  - sequential;
  - back-to-back before the reply;
  - mid-delete at each step;
  - leader change mid-operation;
  - restart between steps;
  - follower offline during the delete or truncate, then catching up.

Required:
- table-driven unit tests;
- 1-node and 3-node broker integration tests;
- an e2e test on the default config;
- a seeded randomized model test of `create`/`delete`/`recreate` against produce, consume and DDL. Its invariant: no data is visible across namespaces, and nothing of a deleted `nsId` survives.

## 9. Review focus
- **Cross-namespace leaks:** in name resolution, the internal-topic keys, grants, metadata responses and error messages.
- **Escalation through a sibling operation:** a namespace admin reaching cluster-admin, another namespace, or the limits.
- **Delete convergence:** across crashes, a lagging follower and recreating the same name.
- **Truncate correctness:** the start never moves backwards; followers and views stay correct; the HW clamp holds.
- **Hot path:** zero allocations and the ABAB bench for the token buckets.

## 10. CLI: `arcanum` (platform) and `rune` (service)
The platform CLI is `arcanum`. Rune is one service under it; more services will follow.

**Two binaries, with one shared login.** The `kubectl`/`gcloud` split.
- **`arcanum`:** the platform CLI, for accounts, projects, deploy and teardown (§10.2).
- **`rune`:** the Rune service CLI, the existing binary with every command unchanged: `rune schema list`, `rune console` and so on. You never have to type an `arcanum` prefix.
- **Shared context:** both read the same login and current project from the Arcanum config directory. `rune` resolves the endpoint and credentials from there, so `--broker` is not needed after `arcanum login`.
- **Plugins:** `arcanum <service> <args>` runs the service's binary with those args, the way `git` runs its plugins, so `arcanum rune schema list` equals `rune schema list`. Future services ship their own binary the same way.
- **Self-hosted:** `rune` with no login works exactly as today, using `--broker` and `--user`/`--password-file`.

### 10.1 Platform model
- **Account:** owns projects, billing and users. You sign in with `arcanum login`.
- **Project:** a named group of deployments, one per service.
- **Deployment:** one service in one project, at one tier:

| Tier | Price | Rune deployment |
|---|---|---|
| `testing` | free | A namespace on a shared multi-tenant cluster. This spec covers it: limits are tight and the cluster admin is the platform. |
| `dev` | paid | A dedicated single-node cluster. The project gets the `default` namespace and its owner is the namespace admin. |
| `pro` | paid | A dedicated 3-node cluster (acks=all, failover), with higher limits and other benefits. |

The client API and the `rune` commands are the same on every tier. Only the endpoint and the limits differ.

### 10.2 Platform commands
These are served by the Arcanum control-plane service, not by the broker. That service is a separate project and comes later; this section fixes the command shape.

| Command | Does |
|---|---|
| `arcanum login` | Browser device-code sign-in. Stores a token in the user config directory with mode 0600, or in the OS keychain. |
| `arcanum logout` | Revokes the token and deletes the local credentials. |
| `arcanum whoami` | Shows the account, the current project and the token's expiry. |
| `arcanum list` | Lists the account's projects, each with its deployments, tier and status. |
| `arcanum project create\|delete <name>` | Creates or deletes a project. Delete refuses while the project still has deployments. |
| `arcanum use <project>` | Sets the current project. Every `arcanum` and `rune` command also takes `--project`. |
| `arcanum deploy rune --tier testing\|dev\|pro` | Provisions Rune for the current project (later). |
| `arcanum status [rune]` | Shows the deployment's state, endpoint, tier and limit usage. |
| `arcanum upgrade rune --tier <tier>` | Changes the tier (later). Moving data between `testing` and `dev`/`pro` is a migration job. |
| `arcanum teardown rune` | Destroys the deployment. You confirm by typing the project name. `testing` runs the namespace delete in §4. `dev`/`pro` destroy the cluster. |

Product decision for later: whether teardown is immediate, or keeps a recoverable grace period on paid tiers.

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
  - `rune namespace create|delete|suspend|resume|list|limits`: cluster admin only, so self-hosted or the control plane.
  - User and grant management is QL through `console` or `ql`, as today; it runs as a namespace admin.
- **Review notes:**
  - `schema list` actually lists topics. Add `topic list` as the canonical command and keep `schema list` as an alias.
  - Error messages keep their existing `rune ...:` prefix.
