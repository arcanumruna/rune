# Rune — Namespaces (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every topic, view, schema, user and role lives in exactly one namespace. A session belongs to the
namespace of the user it logged in as, sees only that namespace, and cannot tell a foreign object from a missing one.
The cluster admin is root and can see and name everything. A namespace admin runs users, grants and topic, view and
schema DDL inside its own namespace and can never escalate. Namespaces are created, listed and given limits (stored,
not enforced yet) through new admin requests and `rune namespace create|list|limits`. `default` always exists. The
produce, fetch and query hot paths are unchanged: ids stay global, and the namespace is resolved once, at name → id.

**Architecture:**
- **Catalog.** `__catalog` gets formatVer 2. It has the v1 layout plus a `u32 nsId` after `kind`, and four namespace
  kinds, 32–35. CatalogState keys every object by `(nsId, name)` and holds the namespace registry:
  - nsId → name, limits and state;
  - nsIds are never reused.
- **Security.** `__security` gets formatVer 2, built the same way:
  - users and roles are keyed by `(nsId, localName)`;
  - grants name only roles of their own namespace;
  - a new resource type `Namespace` carries the namespace-admin grant.
- **Broker, admin thread.** The AdminWorker validates the namespace DDL and appends it. The SecurityAdmin checks the
  namespace prefix of every security DDL against the catalog state, which lives on the same thread.
- **Broker, control thread.** The ObjectIndex, SchemaRegistry and ViewRegistry mirrors key by `(nsId, name)`. The
  gateway stamps each session with its nsId at login. It resolves every name through
  `resolveName(session, qualified)`, filters Metadata and listings by nsId, and compiles permissions per namespace.
- **Disk.** The layout becomes `<data>/ns-<nsId>/topics/<name>/<partition>`.
- **Clients.** The client, QL and CLI pass `ns/name` as plain strings.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja, real Aeron UDP for the
in-process BrokerNode tests, docker compose (3 nodes) on the shared Linux test VM for e2e.

**Spec:** `broker/docs/superpowers/specs/2026-10-24-rune-namespaces-and-truncate-design.md` (approved 2026-10-26).
This plan covers §1, §2, §3, the parts of §5.1 that are storage only, §7, §8, §9, §10 (`create|list|limits`) and §11.
Phase 2 (lifecycle and enforcement), Phase 3 (TruncateTopic) and Phase 0 (arcanum-metrics) each have their own plan.
The Self-review maps each spec section to its task.

**Builds on / order:** `main` after the authentication phase, catalog history and the issue batches merged before
2026-10-24. What exists today (line numbers are hints; use the quoted anchors):

- **Catalog codec (`codec/`):**
  - `rune/catalog/CatalogEvent.h`: `EventKind` 1..9, `kFormatV1 = 1`, `kCurrentFormatVer = kFormatV1`,
    `kReservedKindFirst = 32` and `kReservedKindLast = 63` ("formatVer 1 refuses them"), and an `Ops` variant of 9
    alternatives with a static_assert.
  - `src/catalog/CatalogEvent.cpp`: `checkFormatVer` (~:45), `checkKind` (~:49), `validate` (~:79), `decode` (~:270).
  - `src/catalog/CatalogState.cpp`: dispatch on formatVer (~:94-96), `previewFingerprint` (~:111-113).
  - `ApplyV1.cpp` and `CatalogFingerprint.h` are FROZEN.
  - Goldens: `codec/tests/catalog/golden/v1/basic.rcat`, `CatalogGoldenPins.h` and
    `codec/tools/make_catalog_golden.cpp`.
- **Security codec:**
  - `rune/security/SecurityEvent.h`: kinds 1..10, `kCurrentFormatVer = kFormatV1`.
  - `src/security/SecurityState.cpp`:
    - `hasSecurityAdmin` (~:37-44);
    - `validName` (~:101): `[A-Za-z0-9._@-]`, 1–128 characters, no `__` prefix;
    - `validGrant` (~:111-145): the ops allowed per resource type.
  - `src/security/ApplyV1.cpp` is FROZEN.
  - `protocol/include/rune/protocol/SecurityTypes.h`: `SecurityOperation` 0..6 and `SecurityResourceType` 0..4, with
    their validators.
- **Broker mirrors:**
  - `broker/libs/gateway/include/rune/gateway/ObjectIndex.h`: `ObjectEntry{name,type}`; `byName(name)` is a linear
    scan (#128).
  - `SchemaRegistry.h`: `nameToId_` (~:116). `ViewRegistry.h`: `nameToId_` (~:70).
  - `codec/include/rune/catalog/TopicName.h`: `checkTopicName` (~:37-48) refuses `/`.
- **Admin thread:**
  - `broker/libs/node/src/AdminWorker.cpp`: `startNext_` (~:732-823), `appendEvent_` (~:872-916), `buildEvent_`
    (~:1111-1159), name checks (~:1164, ~:1341).
  - `AdminChannels.h`: the `DdlRequest` variant (~:329).
  - `SecurityAdmin.cpp`: `removesLastSecurityAdmin` (~:326-334), `startDdl_` (~:337-454).
  - Both run on the admin thread (`AdminWorker.h` includes and owns the SecurityAdmin role).
- **Control thread:**
  - `ClientGateway.cpp`:
    - the dispatch switch (`switch (tid) {`, ~:1306-1405);
    - `compilePerms_` (~:8521, called at ~:9064, ~:9275 and ~:9282);
    - `handleAuthClientFirst_` (~:8920);
    - GetTopicSchema (~:8170-8185);
    - `denyGroupRequest_` (~:8721-8765) and `denyTxnRequest_` (~:8779-8830);
    - `handleMetadata` (~:10206).
  - `SessionAuth.h`: `AuthAuthenticated{userId,user,perms,loginNonce}` and `AuthAnonymous{}`.
  - `ScramServer.cpp`: the user lookup (~:105).
- **Disk paths:**
  - `TopicRegistry.cpp:67` (`root / "topics" / spec.name / std::to_string(p)`) and `:158`;
  - `BrokerNode.cpp:5784` and `:5875-5878`;
  - `TopicDirReconcile.cpp:154`, with the dropped-dir parser at `:57-65`;
  - `StorageIdentity.cpp:249`.
- **Wire:**
  - `protocol/include/rune/protocol/client/Protocol.h`: templates up to `SecurityBootstrap = 80` (81–95 free, see
    `Dispatch.h:67`); `ErrorCode` up to `UnstableOffsetCommit = 50`; `kClientVersion = 13`; `kBrokerVersion = 7`.
  - `SchemaMessages.h:25-33`: the `DdlExtension`.
- **QL:** `ql/src/Parser.cpp`: `parseName` (~:828), `parseCreateStatement` (~:855), `parseResource` (~:937),
  `parseGrantRevoke` (~:965).
- **CLI:** `client-cpp/src/cli/Cli.cpp`: dispatch (~:451-478), `printUsage` (~:88). `SecurityCommands.cpp`. CLI
  sources are listed in `client-cpp/CMakeLists.txt` (~:1714-1718).
- **Rigs:**
  - `broker/libs/node/tests/support/`: `AuthBroker.h`, `AuthRig.h`, `AuthWireClient.h`, `AuthTrio.h`,
    `TestCluster.h`, `CatalogTrio.h`, `SecurityRig.h`, `SecurityDdlRig.h`, `CatalogDdlRig.h`.
  - `broker/docker/auth-e2e.sh` and `catalog-e2e.sh`.

## Execution context

- **Branch:** `claude/tender-wozniak-co3crp` in this repo; every path is repo-relative. Before each task, merge
  `main` in if it moved (`git fetch origin main && git merge origin/main`). Push after every task with
  `git push -u origin claude/tender-wozniak-co3crp`.
- **Builds:** see README.md#build and `docs/testing.md`.
  - `build` (Debug, `BUILD_TESTING=ON`, `BUILD_INTEGRATION_TESTING=ON`) for everything;
  - `build-asan` and `build-tsan` at the checkpoints;
  - `build-bench` (Release, `BUILD_BENCHMARKS=ON`) for Task 10's hot-path bench.
  - Re-run cmake only after a `CMakeLists.txt` edit.
- **Test gate (CLAUDE.md rule 10):**
  - **Per task:** build, then `ctest --test-dir build -R '<filter>' --output-on-failure -j8`, where the filter is the
    one named in the task's "Run" step.
  - **Checkpoints:** `cd broker && scripts/run-regression.sh ../build 8`, then the same suite under `build-asan` and
    `build-tsan` (`-R Regression -LE latency`). A failure is bisected over the commits since the last green
    checkpoint.
  - **Checkpoints:**
    - **A** after Task 4: codec and wire only; nothing in the broker changes yet.
    - **B** after Task 8: the broker keys, disk and admin paths.
    - **C** after Task 11: sessions, gateway, QL, client and CLI. This is the first point where a namespace is
      reachable end to end.
    - **D** after Task 14: the matrix, the stress test and the 3-node slice.
    - **E** after Task 15: e2e and docs, before the final whole-branch review.
- **Docker e2e** (Task 15) runs on the shared Linux test VM from `$RUNE_VM_HOST`: hold the shared lock for the whole
  run, work only in your own directory, remove only containers and images you created, and check `df` before and
  after. Never on local Mac docker. The host's address and login are never written into the repo.
- **Commits** end with the session's `Co-Authored-By:` and `Claude-Session:` trailers.

## Global Constraints

**HARD RULES (CLAUDE.md §1, checked in every task review):**
1. **One owner per state.**
   - The catalog `NamespaceRegistry` belongs to `CatalogState`, which lives on the admin thread (applier) and in the
     control thread's mirror copy.
   - The control thread owns `Session.auth.nsId`.
   - The SecurityAdmin reads the admin thread's CatalogState directly. It is the same thread, so this needs no ring.
   - Nothing reads a namespace across threads except through the existing CatalogEffects / SecurityView mirror
     rings. No new ring is needed in Phase 1.
2. **Hot path untouched.**
   - Produce, fetch and GetViewRow by id do not change: there is no nsId check on any id-addressed path.
   - Cross-ns access by id is closed by permissions: a foreign id has no compiled permission bits (D9).
   - Task 10 proves the hot path is unchanged with an ABAB bench.
3. **Zero allocation in `resolveName`.**
   - `std::string_view` splitting.
   - The lookup is a flat `(nsId, name)` hash key. `ObjectIndex` moves from a linear scan to an
     `std::unordered_map<NsName, int32_t, NsNameHash>` keyed by a heterogeneous `NsNameView`. This also closes #128.
4. **Rich-state variants.** The namespace state is a `std::variant<NsActive, NsSuspended{mode}, NsDeleting{...}>`.
   Phase 1 only produces `NsActive`, but `ApplyV2` defines every transition (D5).
5. **Missing ≡ hidden.**
   - A foreign object answers with the exact code, message bytes and code path of a missing one.
   - One helper, `hiddenAsMissing(op)`, builds both answers, so the two can never diverge.
   - Tests compare the full response bytes, not only the code (Review Focus 1).
6. **No silent fallback.**
   - A name with two `/`, an empty part, or an unknown namespace prefix answers the op's missing code (for a non-admin)
     or `InvalidTopicName` / `InvalidRequest` (malformed for anyone, D7).
   - A v1 apply engine seeing kinds 32–35 halts, as it does today.
7. **Regression naming.** Every new test is `*Regression*`. The failing test is written first in every task.
8. **Fix on sight.** #128 (ObjectIndex linear scan) is fixed in Task 5 with its own regression test, and closed with
   the commit sha.

**Error codes** (client `ErrorCode`, `Protocol.h`), each gated on `clientVersion >= 14` (`kClientVersionNamespaces`):
- `NamespaceSuspended = 51`;
- `NamespaceLimitExceeded = 52`;
- `Throttled = 53`;
- `ViewPaused = 54`.

Phase 1 emits only:
- `NamespaceLimitExceeded`, when `maxNamespaces` is reached at CreateNamespace;
- `TopicExists` (25) for a duplicate namespace name. It is reused deliberately, so that no new code exists only for
  this.
- An older client gets `InvalidRequest` (39).

**Versions:**
- `kProtocolVersion` stays 3.
- `kClientVersion` becomes 14 (`kClientVersionNamespaces = 14`).
- `kBrokerVersion` becomes 8.
- Templates:
  - `NamespaceDdl = 81`, answered by `DdlResponse` (78);
  - `ListNamespaces = 82`;
  - `ListNamespacesResponse = 83`.
- `Dispatch.h`'s gap comment becomes 84–95.

**Configuration:**
- `RUNE_MAX_NAMESPACES` (default 1024; the cluster-wide cap checked at CreateNamespace).
- `RUNE_NS_DEFAULT_*` cluster default limits. They are read once at boot into `NamespaceLimitDefaults`. An omitted
  limit takes this value at the moment of the request, and the resolved value is written into the event (§5.1).
- `RUNE_ANONYMOUS_READ` stays off by default (§11).

## Review Focus

1. **Cross-namespace leak by name.** A user in `a` naming `b/t`, `t` (where only `b/t` exists) and `b/missing` gets
   byte-identical responses on every op. Tested by `NamespaceMatrixRegression.ForeignEqualsMissingBytesRegression`
   (Task 12).
2. **Leak by id.**
   - A Fetch, Produce or GetViewRow with a foreign topic id is refused NotAuthorized, the same as an ungranted local
     id.
   - The cluster admin succeeds.
   - Tested by `NamespaceGatewayRegression.ForeignIdNoPermsRegression` (Task 10).
3. **Leak through metadata and listings.**
   - Metadata (all topics), ListNamespaces, ListUsers, ListRoles and the view and schema lists show only the caller's
     namespace.
   - Error messages never contain a foreign name.
   - Tested by `NamespaceGatewayRegression.ListingsFilteredRegression` (Task 10) and
     `...ErrorMessageNoForeignNameRegression`.
4. **Leak through internal keys.** Two namespaces with the same topic, view, schema, user and role names coexist, and
   none of their state crosses. Tested by `NamespaceCatalogV2Regression.SameNameTwoNamespacesRegression` (Task 3),
   `NamespaceSecurityV2Regression.SameUserTwoNamespacesRegression` (Task 4) and
   `NamespaceDiskRegression.SameNameTwoNamespacesDirsRegression` (Task 6).
5. **Escalation through a sibling op.** A namespace admin cannot:
   - grant cluster-admin, `Namespace` Admin on another namespace, or any grant on a foreign role;
   - add a foreign user to a role;
   - create a user or role in another namespace;
   - raise its own limits;
   - create a namespace.

   Each attempt goes through every DDL entry point: SecurityDdl, QL text and the CLI. Tested by
   `NamespaceEscalationRegression.*` (Task 8, 30 cases).
6. **Catalog versions.**
   - v1 events still replay byte-for-byte to the v1 fingerprints.
   - v1 apply refuses kinds 32–35.
   - v2 goldens are pinned.
   - Tested by `CatalogV2GoldenRegression.*` and `CatalogV1FrozenRegression.RefusesNamespaceKindsRegression` (Task 2),
     and `SecurityV2GoldenRegression.*` (Task 4).
7. **Cross-namespace references.**
   - A view over a foreign source, or a topic with a foreign schema reference, is refused `ViewSourceNotFound` /
     `InvalidRequest`.
   - This holds even for the cluster admin.
   - Tested by `NamespaceCatalogV2Regression.CrossNsReferenceRefusedRegression` (Task 3) and its broker twin
     `NamespaceAdminRegression.CrossNsViewRefusedRegression` (Task 7).
8. **nsId never reused.** Tested by `NamespaceCatalogV2Regression.NsIdNeverReusedRegression` (Task 3). The delete
   kind is applied in the codec, and the next create receives `maxEverNsId + 1`.
9. **The default namespace and the cluster admin.**
   - `default` cannot be created, renamed or deleted.
   - The cluster admin is a `default` user with `Admin` on `Cluster ""`.
   - `hasSecurityAdmin` stays cluster-only: a namespace admin never counts toward the last-security-admin guard.
   - Tested by `NamespaceSecurityV2Regression.NsAdminIsNotSecurityAdminRegression` (Task 4).
10. **Login fixes the namespace.**
    - `b/alice` logs into `b`.
    - A bare `alice` logs into `default`.
    - Login as `nosuch/alice` fails exactly like a wrong password, with the same code and the same SCRAM path (no user
      enumeration).
    - Tested by `NamespaceLoginRegression.*` (Task 9).
11. **Groups and transactions outside `default`** are refused NotAuthorized (D8) before any coordinator state is
    touched. Tested by `NamespaceGatewayRegression.GroupTxnRefusedOutsideDefaultRegression` (Task 10).

## Decisions

- **D1. `default` is nsId 0.**
  - It is predefined in every `CatalogState` and never carried as an event.
  - Its directory is `ns-0`, and the first created namespace gets nsId 1.
  - The predefined record has the cluster default limits as resolved at boot. It is not fingerprinted; a v2
    `NamespaceLimitsSet(0, …)` is the only way to change it, and only the cluster admin can send one.
- **D2. Catalog formatVer 2 layout.** It is formatVer 1 with `u32 nsId` inserted after `u16 kind`; the body is
  unchanged.
  - For object kinds (1..9), `nsId` is the namespace of the object.
  - For namespace kinds (32..35), it is the subject namespace.
  - v1 events decode with `nsId = 0`.
  - `kCurrentFormatVer = 2`, so the broker writes only v2 from now on. With no upgrade and no mixed versions, only the
    goldens and the replayer read v1.
- **D3. Kind table.**
  - `EventKind` gains `NamespaceCreated = 32`, `NamespaceLimitsSet = 33`, `NamespaceStateChanged = 34` and
    `NamespaceDeleted = 35`.
  - `Ops` gains 4 alternatives (13 in total).
  - Since an alternative's index is no longer its kind, an explicit `constexpr std::array<EventKind, 13> kKindOfAlt`
    and `constexpr int altOf(EventKind)` replace the `index + 1` arithmetic.
  - A static_assert checks that both are bijective.
  - The ops tag byte stays `== kind`.
- **D4. Fingerprints.** The v2 object fingerprint is `mix(v1Fingerprint(obj), nsId)` with the existing `mix`. The
  namespace record fingerprint is a new `nsFingerprint(nsId, name, limits, state)`. `CatalogFingerprint.h` stays
  frozen; the new functions live in `CatalogFingerprintV2.h`.
- **D5. ApplyV2.**
  - `codec/src/catalog/ApplyV2.cpp` applies v1 kinds by calling the frozen v1 rule functions on the `(nsId, name)`
    keyed maps. To make that possible, ApplyV1's rules are reached through a key adaptor, without editing
    `ApplyV1.cpp`: the state's maps become `std::map<NsKey, …>`, and ApplyV1 is called with `nsId = 0`. If ApplyV1's
    code cannot be reused unedited, copy its rules into ApplyV2 verbatim and pin equality with the v1 golden replay
    (`CatalogV2Regression.V1KindsApplyIdenticallyRegression`).
  - Namespace kinds:
    - Created: the name is new and valid, and the nsId equals `nextNsId`.
    - LimitsSet: the namespace exists and the record is full.
    - StateChanged: allowed edges are ACTIVE ⇄ SUSPENDED(mode) and ACTIVE|SUSPENDED → DELETING.
    - Deleted: from DELETING only, and never for nsId 0.
  - Object kinds against a missing, or not-ACTIVE, nsId are refused.
- **D6. Names.**
  - Namespace names match `[a-z0-9][a-z0-9-]{0,62}`, and `default` is reserved.
  - Object local names keep today's rules.
  - The qualified form is `ns/name`, with exactly one `/`.
  - The canonical stored form is the local name plus the nsId. On the wire, a `default` object is bare and others are
    `ns/name`. `default/t` parses to `(0, t)`.
- **D7. Resolution (`rune/catalog/NsName.h`, shared by broker, client and QL).**
  - `parseQualified(sv) → {std::string_view ns (empty = caller's), std::string_view local} | Malformed`.
  - Gateway rule:
    - an empty ns resolves to the session nsId;
    - an explicit ns equal to the session's namespace resolves to it;
    - any other explicit ns resolves to `Hidden` for a non-admin, and to the ns's id for the cluster admin. An unknown
      ns gives `Hidden` for everyone (`Missing`).
  - `Malformed` (two `/`, an empty part) answers the op's existing malformed code (`InvalidTopicName` for topic and
    view names, `InvalidRequest` elsewhere) for everyone. Malformed names reveal nothing.
- **D8. Groups and transactions outside `default`.** Phase 1 has no nsId in group or txn keys. A session whose nsId is
  not 0 sending any group template (32–47) or txn template (48–54), and a cluster admin naming them through another
  namespace, is refused NotAuthorized in `denyGroupRequest_` / `denyTxnRequest_`, before coordinator lookup. Phase 2
  adds `(nsId, id)` keys and lifts this.
- **D9. Ids stay global.** `compilePerms_` resolves grant patterns with `byName(session.nsId, pattern)`, and prefix
  patterns match only that namespace's names. A foreign id is never in the table, so the bitmap is 0 → NotAuthorized
  (the existing code). The cluster admin's table is "all", which is today's admin path.
- **D10. Security formatVer 2.**
  - The layout is v1 plus `u32 nsId` after `kind`, the same as the catalog.
  - User and role names in events are local names, and the key is `(nsId, local)`.
  - `validName` is unchanged for the local part.
  - A role grant's `ResourcePattern` names objects of the role's own namespace only. The nsId is implicit, so a
    pattern can never name another namespace.
  - Role membership requires `user.nsId == role.nsId`.
  - New resource type `Namespace = 5`, whose valid ops are `Admin` and `Describe`, with an Exact pattern whose name
    equals the role's namespace name.
  - New op `Truncate = 7`, valid on `Topic`. It is stored now and enforced in Phase 3.
- **D11. Namespace admin.** A user holds `Admin` on `Namespace "<ns>"` through a role of the same ns. Only the cluster
  admin grants it (escalation guard). It permits, in its namespace:
  - CreateUser, DropUser, AlterUser of non-admin users;
  - CreateRole, DropRole, Grant and Revoke of anything except `Namespace` Admin and `Cluster` grants;
  - topic, view and schema DDL;
  - ListNamespaces of its own namespace;
  - reading its own namespace's limits.

  A `default` namespace admin is possible. `Cluster` grants are valid only in nsId 0.
- **D12. Session namespace.**
  - `AuthAuthenticated` and `AuthPasswordChangeRequired` gain `uint32_t nsId`.
  - SCRAM's username is `ns/user` or `user`.
  - `ScramServer` looks up `(nsId, local)` through the `SecurityView`. An unknown ns, malformed name or unknown user
    all take the existing fake-verifier path (no enumeration).
  - `AuthAnonymous` is nsId 0.
  - ConnectRequest is unchanged.
- **D13. Namespace DDL wire.**
  - `NamespaceDdl (81)`: `header | DdlExtension | kind u8 (1 = Create, 2 = SetLimits; 3..5 reserved for Phase 2) |
    name var16 | u8 limitsPresentMask[8] | NamespaceLimits fixed block`. It is answered by `DdlResponse (78)`.
  - `ListNamespaces (82)`: `header | u32 afterNsId | u16 max`.
  - `ListNamespacesResponse (83)`: `header | u16 count | repeated {u32 nsId, name var16, u8 state, NamespaceLimits} |
    u32 nextAfter`.
  - `NamespaceLimits` is ONE fixed little-endian struct in `rune/protocol/NamespaceLimits.h`, used by the wire, the
    codec and the client.
    - It holds every §5.2 field as `u64` (0 = unlimited where §5.2 allows it, otherwise the value) and `u32` feature
      bits.
    - Reserved fields are carried as zero.
    - `static_assert(sizeof(NamespaceLimits) == kNamespaceLimitsBytes)`.
- **D14. Disk.**
  - `rune/node/NamespacePaths.h`: `topicsRoot(dataDir, nsId) = dataDir / ("ns-" + std::to_string(nsId)) / "topics"`.
    Every disk path site uses it (Task 6).
  - Dropped-dir names keep today's suffix scheme under the ns root.
  - The reconcile walks every `ns-*` directory, and refuses any directory that is not `ns-<u32>` by logging it and
    leaving it untouched.
- **D15. Who may do what.**
  - CreateNamespace and SetNamespaceLimits: cluster admin only. Anyone else gets NotAuthorized, the code a known,
    ungranted cluster op answers today; namespaces are not hidden from their own admin.
  - ListNamespaces: the cluster admin sees all; a namespace admin or plain user sees its own only.
- **D16. The cluster admin's name.** It stays a `default` user. It may address any namespace's object with `ns/name`
  and is never hidden. Its own DDL in another namespace (for example creating `b/t`) is allowed and lands in `b`.

## Interaction matrix

The single source of truth is `broker/libs/node/tests/support/NamespaceMatrixExpectations.h`. It defines:
- `enum class Op`, `enum class Caller`, `enum class Target` and `enum class Timing`;
- `Outcome expect(Op, Caller, Target, Timing)`;
- `bool naRule(Op, Target)`;
- `std::array<ErrorCode, …> expectedCodes(Op)`, giving the missing code for each op.

The 1-node rig (Task 12), the seeded stress test (Task 13) and the 3-node slice (Task 14) all read it.

- **Ops (20):**
  - CreateNamespace, SetNamespaceLimits, ListNamespaces;
  - CreateTopic, AlterTopic, DropTopic, Produce, Fetch, Metadata, GetTopicSchema;
  - CreateView, DropView, GetViewRow;
  - CreateUser, DropUser, CreateRole, Grant, Revoke;
  - GroupJoin, TxnInit.
- **Callers (7):**
  - ClusterAdmin;
  - NsAdminSame (admin of the target's namespace);
  - NsAdminForeign (admin of another namespace);
  - UserGranted (in the target's namespace, holding the op's grant);
  - UserUngranted (same namespace, no grant);
  - UserForeign (another namespace, holding the same grant in its own namespace);
  - Anonymous (with `RUNE_ANONYMOUS_READ=on`; it reaches `default` only).
- **Targets (4):**
  - OwnNs (an existing object in the namespace under test);
  - ForeignNs (an existing object only in another namespace, named qualified);
  - Missing (no such object anywhere);
  - SameNameBoth (the same local name exists in both, named unqualified).

  For the namespace ops, a Target means the namespace itself.
- **Timings (5):**
  - Sequential;
  - BackToBack (a second identical op sent before the first's reply);
  - ConcurrentSameName (two callers creating the same name);
  - LeaderChange (the `__catalog` / `__security` leader is killed between the append and the reply);
  - Restart (the broker restarts between the op and the check).
- **Outcomes:** `Ok`, `Missing(expectedCodes(op))` (Hidden is `Missing` by construction), `NotAuthorized`,
  `Exists`, `LimitExceeded`, `Refused(code)`.
- **Rules in `expect`, evaluated in this order:**
  1. **Not applicable** (`naRule`): SameNameBoth for the 3 namespace ops. That is 3 × 7 = **21 n/a cells**.
  2. **Anonymous:**
     - Fetch, Metadata, GetTopicSchema and GetViewRow on a `default` OwnNs target give `Ok`.
     - ForeignNs and Missing give `Missing`.
     - Everything else gives `NotAuthorized`.
  3. **Namespace ops:**
     - Cluster admin: `Ok` (Create on OwnNs or ForeignNs, both existing, gives `Exists`; SetLimits or List on Missing
       gives `Missing`).
     - Anyone else: Create and SetLimits give `NotAuthorized`.
     - ListNamespaces gives `Ok`, filtered (the response must contain exactly the caller's namespace).
  4. **GroupJoin and TxnInit:** `NotAuthorized` for every caller whose session nsId is not 0, and for any target
     outside `default`. Otherwise, today's rules apply.
  5. **ForeignNs:**
     - ClusterAdmin and the target's own NsAdminSame give `Ok`. NsAdminSame is logged into the target's namespace and
       names it unqualified.
     - Everyone else gives `Missing`.
  6. **Missing:**
     - Create ops give `Ok`, when granted.
     - Others give `Missing`.
  7. **OwnNs and SameNameBoth:**
     - UserGranted, NsAdminSame and ClusterAdmin get today's result.
     - UserUngranted gets `NotAuthorized`.
     - NsAdminForeign and UserForeign get `Missing` on OwnNs. On SameNameBoth they get their own namespace's object,
       which the test checks by content.
- **Timing cells:**
  - Sequential is exercised for every cell.
  - The other 4 timings cover the 7 DDL ops (CreateNamespace, SetNamespaceLimits, CreateTopic, DropTopic, CreateView,
    CreateUser, Grant) × {ClusterAdmin, NsAdminSame} × {OwnNs}, which is 7 × 2 × 4 = **56 timing cells**.
  - Expected:
    - BackToBack: the second op is answered from request-id dedupe, with the identical reply.
    - ConcurrentSameName: exactly one `Ok` and one `Exists`.
    - LeaderChange: the retried op converges to one event (catalog dedupe) and the matrix check passes.
    - Restart: state is identical after the restart (fingerprint equal).
- **Pinned counts** (`NamespaceMatrixRegression.MatrixCountRegression`): 20 × 7 × 4 = **560 sequential cells**, of
  which **21 n/a** and **539 applicable**; **56 timing cells**; **595 executed in total**.

## File Structure

New:
- `protocol/include/rune/protocol/NamespaceLimits.h`: the D13 fixed struct and its codec.
- `protocol/include/rune/protocol/client/NamespaceMessages.h`: the wire for templates 81–83.
- `codec/include/rune/catalog/NsName.h`: `parseQualified`, `validNamespaceName`, `kDefaultNsId = 0`,
  `kDefaultNsName = "default"`.
- `codec/include/rune/catalog/NamespaceRegistry.h` and `codec/src/catalog/NamespaceRegistry.cpp`: the state and the
  `NsState` variant.
- `codec/include/rune/catalog/CatalogFingerprintV2.h`.
- `codec/src/catalog/ApplyV2.cpp`.
- `codec/src/security/ApplyV2.cpp`.
- `codec/tests/catalog/golden/v2/namespaces.rcat` and `codec/tests/security/golden/v2/namespaces.rsec`, with their pins.
- `broker/libs/node/include/rune/node/NamespacePaths.h`.
- `broker/libs/gateway/include/rune/gateway/NsResolve.h` and `.../src/NsResolve.cpp`: `resolveName`,
  `hiddenAsMissing`.
- `client-cpp/src/cli/NamespaceCommands.cpp`.
- `broker/libs/node/tests/support/NamespaceMatrixExpectations.h` and `NamespaceRig.h`.
- Tests:
  - `codec/tests/catalog/NamespaceCatalogV2Test.cpp`;
  - `codec/tests/security/NamespaceSecurityV2Test.cpp`;
  - `broker/libs/node/tests/Namespace{Disk,Admin,Escalation,Login,Gateway,Matrix,Stress,Trio}Test.cpp`;
  - `client-cpp/tests/NamespaceClientTest.cpp`;
  - `ql/tests/NamespaceQlTest.cpp`;
  - `broker/docker/namespace-e2e.sh`.

Modified (listed per task below).

---

### Task 1: protocol — error codes, enums, templates, NamespaceLimits, versions

Spec §2, §3, §5.1; D10, D13.

**Files:**
- Modify:
  - `protocol/include/rune/protocol/client/Protocol.h`: ErrorCodes 51–54, templates 81–83, `kClientVersion = 14`,
    `kClientVersionNamespaces = 14`, `kBrokerVersion = 8`;
  - `protocol/include/rune/protocol/client/Dispatch.h`: route 81–83 and set the gap comment to 84–95;
  - `protocol/include/rune/protocol/SecurityTypes.h`: `Truncate = 7`, `Namespace = 5`, and the validators.
- Create: `protocol/include/rune/protocol/NamespaceLimits.h`, `protocol/include/rune/protocol/client/NamespaceMessages.h`.
- Test: `protocol/tests/NamespaceMessagesTest.cpp`.

**Interfaces:**
```cpp
namespace rune::protocol {
struct NamespaceLimits {                       // little-endian, written byte-wise; §5.2 groups in order
    // objects
    uint64_t maxTopics, maxPartitionsPerTopic, maxPartitionsTotal, maxViews, maxSchemas, maxVersionsPerSchema,
             maxGroups, maxUsers;
    // storage
    uint64_t maxDiskBytes, maxRetentionMs, maxRetentionBytes, maxMessageBytes, maxBatchBytes;
    // throughput
    uint64_t produceBytesPerSec, produceRecordsPerSec, fetchBytesPerSec, requestOpsPerSec, queryCyclesPerSec;
    // connections
    uint64_t maxConnections, maxConnectionsPerUser, maxSessionsPerUser, maxOpenTxns, maxTxnTimeMs;
    // views
    uint64_t maxViewMemoryBytes;
    // durability
    uint64_t minReplicationFactor, maxReplicationFactor, lowestAcks, lowestMinInsync;
    // features (bit per feature: Compaction, Txns, Ql, Views, Sql, Redis, AnonymousRead, AuthScram)
    uint32_t featureBits;
    uint32_t reserved0;                        // placement / password policy / encryption key — zero in Phase 1
    uint64_t reserved1, reserved2, reserved3;
};
inline constexpr size_t kNamespaceLimitsBytes = 30 * 8 + 4 + 4 + 3 * 8;   // 272
inline constexpr size_t kNamespaceLimitFieldCount = 30;
void encodeLimits(const NamespaceLimits&, uint8_t* out);           // exactly kNamespaceLimitsBytes
bool decodeLimits(const uint8_t* in, size_t len, NamespaceLimits&); // false on short buffer
}
namespace rune::protocol::client {
enum class NamespaceDdlKind : uint8_t { Create = 1, SetLimits = 2 /* 3 Suspend, 4 Resume, 5 Delete: Phase 2 */ };
struct NamespaceDdlRequest { DdlExtension ext; NamespaceDdlKind kind; std::string name;
                             std::array<uint8_t, 4> presentMask; /* bit i = field i given; 30 fields → 4 bytes */
                             NamespaceLimits limits; };
struct NamespaceInfo { uint32_t nsId; std::string name; uint8_t state; NamespaceLimits limits; };
// encode*/decode* for 81, 82, 83 in the SecurityMessages.h style (var16 strings, bounds-checked).
}
```

- [ ] **Step 1: failing tests** in `NamespaceMessagesTest.cpp`:
  - `NamespaceMessagesRegression.LimitsRoundTripRegression`: fill every field with a distinct value
    (`i * 0x0101010101010101`), encode, decode, compare. The golden first 16 bytes are pinned as a hex literal.
  - `...LimitsSizePinnedRegression`: `kNamespaceLimitsBytes == 272` and `sizeof` agrees.
  - `...DdlRoundTripRegression`: Create and SetLimits with a partial mask.
  - `...DdlTruncatedAtEveryOffsetRegression`: decoding the encoded frame truncated at each offset 0..n-1 returns false
    and never reads past the end (the ASan run checks this).
  - `...ListResponseRoundTripRegression`: 0, 1 and 300 entries; `nextAfter` is carried.
  - `...SecurityEnumsRegression`: `validSecurityOperation(7)` and `validSecurityResourceType(5)` are true; 8 and 6 are
    false.
  - `...TemplateDispatchRegression`: 81, 82 and 83 route; 84 and 95 are UnknownTemplate.
  - `...ErrorCodeValuesRegression`: 51, 52, 53 and 54 are pinned.

  Mutation: change the order of two limit fields → RoundTrip's golden prefix fails. Drop the validator change →
  SecurityEnums fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceMessages|ProtocolDispatch|SecurityTypes' --output-on-failure -j8`.
- [ ] **Step 3:** the existing version-pin tests (`grep -rn "kClientVersion\b.*13\|kBrokerVersion.*7" */tests`) are
  updated to 14 and 8; nothing else changes.
- [ ] **Step 4:** commit `protocol: namespace error codes, templates 81-83, NamespaceLimits, client v14 / broker v8` and
  push.

### Task 2: codec catalog — formatVer 2 layout, kinds 32–35, kind table, goldens

Spec §3; D2, D3, D4.

**Files:**
- Modify:
  - `codec/include/rune/catalog/CatalogEvent.h`: kinds, `kFormatV2`, `kCurrentFormatVer = kFormatV2`, `Ops` with 13
    alternatives, `kKindOfAlt` / `altOf`, and `uint32_t nsId` on `CatalogEvent`;
  - `codec/src/catalog/CatalogEvent.cpp`: encode and decode the v2 header, `checkFormatVer`, `checkKind` (v1 refuses
    32–63, v2 accepts 32–35 and refuses 36–63), and `validate` for the 4 bodies;
  - `codec/tools/make_catalog_golden.cpp`: emit `golden/v2/namespaces.rcat`.
- Create: `codec/include/rune/catalog/NsName.h`, `codec/include/rune/catalog/CatalogFingerprintV2.h`.
- Test: `codec/tests/catalog/NamespaceCatalogV2Test.cpp`, `codec/tests/catalog/CatalogGoldenPins.h` (v2 pins).

**Interfaces:**
```cpp
enum class EventKind : uint16_t { /* 1..9 unchanged */, NamespaceCreated = 32, NamespaceLimitsSet = 33,
                                  NamespaceStateChanged = 34, NamespaceDeleted = 35 };
inline constexpr uint16_t kFormatV2 = 2;
enum class NsStateTag : uint8_t { Active = 0, SuspendedReadOnly = 1, SuspendedRejectAll = 2, Deleting = 3 };
struct NamespaceCreatedOps      { std::string name; rune::protocol::NamespaceLimits limits; };
struct NamespaceLimitsSetOps    { rune::protocol::NamespaceLimits limits; };
struct NamespaceStateChangedOps { NsStateTag to; };
struct NamespaceDeletedOps      {};
// CatalogEvent gains: uint32_t nsId = 0;   (v1 decode leaves it 0)
inline constexpr std::array<EventKind, 13> kKindOfAlt = { /* 1..9, 32, 33, 34, 35 */ };
constexpr int altOf(EventKind k);   // -1 for an unknown kind
// NsName.h
inline constexpr uint32_t kDefaultNsId = 0; inline constexpr std::string_view kDefaultNsName = "default";
bool validNamespaceName(std::string_view);           // [a-z0-9][a-z0-9-]{0,62}, != "default"
struct Qualified { std::string_view ns; std::string_view local; };   // ns empty = unqualified
enum class QualifyError : uint8_t { Malformed };
std::expected<Qualified, QualifyError> parseQualified(std::string_view);   // 0 or 1 '/', both parts non-empty
// CatalogFingerprintV2.h
uint64_t objectFingerprintV2(uint64_t v1Fingerprint, uint32_t nsId);
uint64_t nsFingerprint(uint32_t nsId, std::string_view name, const rune::protocol::NamespaceLimits&, NsStateTag);
```

- [ ] **Step 1: failing tests:**
  - `CatalogV2Regression.HeaderLayoutRegression`: a v2 CreateTopic with nsId 7 encodes to the pinned hex. The bytes
    at offset 4..7 are `07 00 00 00`, and the rest equals the v1 encoding shifted by 4.
  - `...KindTableBijectiveRegression`: altOf(kKindOfAlt[i]) == i for all i, altOf(10) == -1 and altOf(36) == -1.
  - `...NamespaceBodiesRoundTripRegression` for all 4 kinds.
  - `...TruncatedAtEveryOffsetRegression` (every kind; decode returns an error and never reads out of range).
  - `CatalogV1FrozenRegression.RefusesNamespaceKindsRegression`: a formatVer-1 frame with kind 32 is refused by
    `checkKind`.
  - `...V1GoldenStillReplaysRegression`: `golden/v1/basic.rcat` replays to the pinned v1 fingerprints, with nsId 0 on
    every decoded event.
  - `CatalogV2GoldenRegression.NamespacesGoldenPinnedRegression`: the new golden has 3 namespaces, 2 topics named `t`
    in different namespaces and a limits change, and replays to the pins.
  - `NsNameRegression.ParseQualifiedTableRegression`, 24 rows:
    - `t` → {"" , t}; `a/t` → {a,t};
    - `/t`, `a/`, `a/b/c`, `/` and `` are Malformed;
    - `default/t` → {default,t}.
  - `NsNameRegression.ValidNamespaceNameTableRegression`, 20 rows:
    - valid: `a`, `0`, `a-b`, a 63-character name;
    - invalid: `-a`, `A`, `a_b`, a 64-character name, `default`, ``, `__x`, `a/b`.

  Mutation: remove the `checkKind` v1 refusal → RefusesNamespaceKinds fails. Swap two entries of `kKindOfAlt` →
  Bijective fails.
- [ ] **Step 2:** implement. `kCurrentFormatVer` flips to 2. The catalog tests that construct events by hand and assert
  `formatVer == 1` are changed to read `kCurrentFormatVer` only where they test the writer; v1 decode tests keep 1.
- [ ] **Step 3:** run `ctest --test-dir build -R 'Catalog|NsName' --output-on-failure -j8`.
- [ ] **Step 4:** commit and push.

### Task 3: codec catalog — NamespaceRegistry, (nsId, name) keys, ApplyV2

Spec §1, §3, §5.1; D1, D5, D6.

**Files:**
- Modify:
  - `codec/include/rune/catalog/CatalogState.h`: maps keyed by `NsKey`, plus `const NamespaceRegistry& namespaces()`;
  - `codec/src/catalog/CatalogState.cpp`: dispatch on formatVer 2 → ApplyV2, and `previewFingerprint`.
- Create: `codec/include/rune/catalog/NamespaceRegistry.h`, `codec/src/catalog/NamespaceRegistry.cpp`,
  `codec/src/catalog/ApplyV2.cpp`.
- Test: `NamespaceCatalogV2Test.cpp`.

**Interfaces:**
```cpp
struct NsKey { uint32_t nsId; std::string name; auto operator<=>(const NsKey&) const = default; };
struct NsActive {};  struct NsSuspended { NsStateTag mode; };  struct NsDeleting {};
using NsState = std::variant<NsActive, NsSuspended, NsDeleting>;
struct NamespaceRecord { uint32_t nsId; std::string name; rune::protocol::NamespaceLimits limits; NsState state;
                         uint32_t version; uint64_t fingerprint; };
class NamespaceRegistry {
public:
    explicit NamespaceRegistry(const rune::protocol::NamespaceLimits& defaultNsLimits);   // predefines nsId 0
    const NamespaceRecord* byId(uint32_t) const;
    const NamespaceRecord* byName(std::string_view) const;
    uint32_t nextNsId() const;            // maxEver + 1; never decreases, survives Deleted
    size_t   liveCount() const;           // excludes Deleted, includes Deleting
    template <class F> void forEach(F&&) const;   // nsId order
};
// CatalogState: topics_/views_/schemas_ become std::map<NsKey, …>; byName(nsId, name); idToKey for the id maps.
```

- [ ] **Step 1: failing tests** in `NamespaceCatalogV2Regression.*`:
  - `DefaultPredefinedRegression`: a fresh state has nsId 0 `default`, `NsActive`, and `nextNsId() == 1`.
  - `CreateAssignsNextIdRegression`: Created(1,"a") is ok; Created(3,"b") is refused (`ApplyError::BadNsId`); Created(2,
    "a") is refused (name exists).
  - `CreateDefaultRefusedRegression`: Created(…,"default") is refused.
  - `NsIdNeverReusedRegression`:
    - create 1 and 2; then StateChanged(2, Deleting) and Deleted(2);
    - `nextNsId() == 3`;
    - Created(2,"c") is refused and Created(3,"b") is ok, reusing the name but not the id.
  - `StateEdgesTableRegression`: all 4×4 (from, to) pairs plus Deleted from each state, 20 rows, each expecting
    ok or refused per D5. Deleted(0) is refused.
  - `LimitsSetRegression`: a full record is replaced and the version bumps. A missing nsId is refused.
  - `SameNameTwoNamespacesRegression`:
    - CreateTopic `t` in 0 and in 1, CreateView `v` in both, a schema `s` in both;
    - `byName(0,"t") != byName(1,"t")`;
    - DropTopic `t` in 1 leaves `(0,t)`;
    - the fingerprints differ.
  - `ObjectInMissingOrDeletingNsRefusedRegression`.
  - `CrossNsReferenceRefusedRegression`:
    - CreateView in ns 1 whose `sourceNames` has a name existing only in ns 0 is refused `SourceNotFound`;
    - the same with the source in ns 1 is ok;
    - a topic schema ref to a ns-0 schema from ns 1 is refused.
  - `V1KindsApplyIdenticallyRegression`: re-encode every event of `golden/v1/basic.rcat` as v2 with nsId 0 and apply
    through ApplyV2. The per-object v1 fingerprints satisfy `objectFingerprintV2(v1, 0)`, and the object sets are
    equal.
  - `PreviewFingerprintV2Regression`.

  Mutation: in `nextNsId`, use `liveCount() + 1` → NeverReused fails. Drop the nsId from `NsKey` comparison →
  SameName fails.
- [ ] **Step 2:** implement ApplyV2 per D5. Reuse the v1 rules unedited where they are free functions over the maps;
  otherwise copy them into ApplyV2 and rely on `V1KindsApplyIdentically`.
- [ ] **Step 3:** run `ctest --test-dir build -R 'Catalog|NsName|NamespaceCatalog' --output-on-failure -j8`.
- [ ] **Step 4:** commit and push.

### Task 4: codec security — formatVer 2, (nsId, local) keys, Namespace grants

Spec §2, §3; D10, D11.

**Files:**
- Modify:
  - `codec/include/rune/security/SecurityEvent.h`: `kFormatV2`, `nsId` on the event;
  - the security encode and decode `.cpp`;
  - `codec/include/rune/security/SecurityState.h`: `users_` / `roles_` keyed by `(nsId, local)`, plus
    `userByName(nsId, local)` and `nsAdminOf(nsId)`;
  - `codec/src/security/SecurityState.cpp`: `validGrant` (Namespace and Truncate rows; the bound checks become
    `!validSecurityOperation(...)` / `!validSecurityResourceType(...)`). `hasSecurityAdmin` is unchanged, but takes
    users of nsId 0 only.
  - `codec/src/security/Permissions.cpp`: patterns scoped to the role's nsId; `Namespace` resource bits.
- Create: `codec/src/security/ApplyV2.cpp`, `codec/tests/security/golden/v2/namespaces.rsec`.
- Test: `codec/tests/security/NamespaceSecurityV2Test.cpp`, `SecurityGoldenPins.h`.

**Interfaces:**
```cpp
struct SecKey { uint32_t nsId; std::string local; auto operator<=>(const SecKey&) const = default; };
// EffectivePerms gains: uint32_t nsId; bool nsAdmin;   (nsAdmin = Admin on Namespace "<own ns name>")
// validGrant(roleNsId, roleNsName, GrantSpec):
//   Cluster: only roleNsId == 0;  Namespace: ops {Admin, Describe}, Exact, name == roleNsName;
//   Topic: today's ops + Truncate.
```

- [ ] **Step 1: failing tests** in `NamespaceSecurityV2Regression.*`:
  - `SameUserTwoNamespacesRegression`: `alice` exists in ns 0 and in ns 1 with distinct passwords, and dropping
    `(1,alice)` leaves `(0,alice)`.
  - `RoleMembershipSameNsOnlyRegression`: AddRole (user ns 1, role ns 0) is refused.
  - `ClusterGrantOnlyInDefaultRegression`.
  - `NamespaceGrantTableRegression`, 12 rows: ops × {own name, foreign name, empty, prefix pattern}.
  - `TruncateOnTopicValidRegression`, plus Truncate on View refused.
  - `NsAdminIsNotSecurityAdminRegression`: with a namespace admin in ns 1 and no `default` cluster admin enabled,
    `hasSecurityAdmin() == false`.
  - `PatternScopedToRoleNsRegression`: a role in ns 1 granting Read on prefix `t` compiles against the ns-1 names only.
    `Permissions` is given an index with `t1` in ns 0 and `t2` in ns 1, and only `t2`'s id gets the bit.
  - `V1GoldenStillReplaysRegression`.
  - `SecurityV2GoldenRegression.NamespacesGoldenPinnedRegression`.
  - The header layout pin and every-offset truncation for v2.

  Mutation: allow Cluster grants in any ns → `ClusterGrantOnlyInDefault` fails. Count ns-1 admins in
  `hasSecurityAdmin` → `NsAdminIsNotSecurityAdmin` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'Security|NamespaceSecurity' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Checkpoint A** (full Regression + ASan + TSan). The broker still builds: it writes v2 with nsId 0 everywhere, so
nothing outside the codec has changed behaviour.

### Task 5: broker — (nsId, name) mirrors; ObjectIndex hash (fixes #128)

Spec §1; D9; HARD RULE 3.

**Files:**
- Modify:
  - `broker/libs/gateway/include/rune/gateway/ObjectIndex.h` (and `.cpp`): `ObjectEntry{nsId,name,type}`, the
    `byName(nsId, name)` hash, `forEachInNs(nsId, f)`;
  - `SchemaRegistry.h` / `.cpp` and `ViewRegistry.h` / `.cpp`: `nameToId_` keyed by `NsName`;
  - `broker/libs/node/include/rune/node/CatalogApplier.h`: `CatalogEffects` entries carry `nsId`, plus
    `NamespaceUpserted{NamespaceRecord}`;
  - `BrokerNode.cpp`: `onCatalogTopicApplied_` (~:5687) and the boot ObjectIndex build (~:3989-3996) pass the nsId;
  - every `byName(` caller (`grep -rn "byName(" broker/libs | grep -v tests`), which passes `kDefaultNsId` for now.
    Task 10 replaces it with the session's nsId.
- Test: `broker/libs/gateway/tests/ObjectIndexTest.cpp` (extend), `broker/libs/node/tests/NamespaceMirrorTest.cpp`.

**Interfaces:**
```cpp
struct NsName     { uint32_t nsId; std::string name; bool operator==(const NsName&) const = default; };
struct NsNameView { uint32_t nsId; std::string_view name; };
struct NsNameHash { using is_transparent = void; size_t operator()(const NsName&) const; size_t operator()(NsNameView) const; };
struct NsNameEq   { using is_transparent = void; /* NsName×NsName, NsName×NsNameView, NsNameView×NsName */ };
class ObjectIndex { public:
    std::optional<int32_t> byName(uint32_t nsId, std::string_view name) const;   // O(1), zero alloc
    template <class F> void forEachInNs(uint32_t nsId, F&&) const;
    // onTopic/onView take nsId; onDropped unchanged (by id)
};
```

- [ ] **Step 1: failing tests:**
  - `ObjectIndexRegression.HashLookupZeroAllocRegression`: 10 000 entries; 1 000 000 lookups under the
    allocation-counting hook (`rune::test::AllocCounter`, already used by the hot-path tests:
    `grep -rn AllocCounter broker/libs | head -1`) make 0 allocations.
  - `ObjectIndexRegression.Issue128LinearScanRegression`: lookup time with 10 000 entries is less than 4× the time with
    10 entries, measured as the median of 101 runs. This deterministic ratio guard is labelled `latency`.
  - `ObjectIndexRegression.SameNameTwoNsRegression`.
  - `NamespaceMirrorRegression.EffectsCarryNsIdRegression`: drive a `CatalogApplier` over a v2 log with `t` in 0 and 1.
    The control-thread ObjectIndex, SchemaRegistry and ViewRegistry each resolve both, and a restart rebuild from the
    checkpoint gives the same result.

  Mutation: hash only `name` and compare only `name` → SameNameTwoNs fails.
- [ ] **Step 2:** implement. Run
  `ctest --test-dir build -R 'ObjectIndex|NamespaceMirror|SchemaRegistry|ViewRegistry|CatalogApplier' --output-on-failure -j8`.
- [ ] **Step 3:** commit `... (fixes #128)`, push, then
  `gh issue close 128 -c "Fixed in <sha>: ObjectIndex is a (nsId,name) hash (regression: ObjectIndexRegression.Issue128LinearScanRegression)"`.

### Task 6: broker — disk layout `ns-<nsId>`

Spec §1; D14.

**Files:**
- Create: `broker/libs/node/include/rune/node/NamespacePaths.h`.
- Modify:
  - `broker/libs/gateway/src/TopicRegistry.cpp:67, :158`;
  - `broker/libs/node/src/BrokerNode.cpp:5784, :5875-5878`;
  - `broker/libs/node/src/TopicDirReconcile.cpp`: walk `ns-*`, and parse the dropped-dir name under the ns root
    (`:57-65`, `:154`);
  - `broker/libs/node/src/StorageIdentity.cpp:249`;
  - `TopicSpec` / the `CreateTopic` posting, which carries the nsId to the registry.
- Test: `broker/libs/node/tests/NamespaceDiskTest.cpp`.

**Interfaces:**
```cpp
namespace rune::node {
std::filesystem::path nsRoot(const std::filesystem::path& dataDir, uint32_t nsId);      // dataDir/"ns-<nsId>"
std::filesystem::path topicsRoot(const std::filesystem::path& dataDir, uint32_t nsId);  // nsRoot/"topics"
std::optional<uint32_t> parseNsDir(std::string_view dirName);                           // "ns-<u32>" only
}
```

- [ ] **Step 1: failing tests:**
  - `NamespaceDiskRegression.PathsRegression`: `ns-0/topics/t/0`.
  - `...ParseNsDirTableRegression`: `ns-0`, `ns-4294967295`, `ns-4294967296`, `ns-`, `ns-01`, `ns--1`, `nsx1`,
    `topics`.
  - `...SameNameTwoNamespacesDirsRegression`: a real BrokerNode creates `t` in 0 and 1, both directories exist and
    hold different data, and a restart reopens both.
  - `...ReconcileDropsUnderNsRootRegression`: drop `(1,t)` while the broker is down; the reconcile removes only
    `ns-1/topics/t`.
  - `...ReconcileIgnoresForeignDirRegression`: a stray `<data>/junk` and `<data>/topics` are logged and left alone.
  - `grep -rn '"topics"' broker/libs --include=*.cpp | grep -v NamespacePaths` returns nothing. A source guard
    test is added to `broker/scripts/` in the style of `check_no_rune_topics.py`.

  Mutation: drop the nsId from `topicsRoot` → SameNameDirs fails.
- [ ] **Step 2:** implement. Run
  `ctest --test-dir build -R 'NamespaceDisk|TopicDirReconcile|TopicRegistry|StorageIdentity' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

### Task 7: broker admin — namespace DDL, scoped topic/view/schema DDL

Spec §3, §5.1, §7; D13, D15, D16.

**Files:**
- Modify:
  - `broker/libs/node/include/rune/node/AdminChannels.h`: `NamespaceDdlReq` and `ListNamespacesReq` in the
    `DdlRequest` variant; the existing topic, view and schema requests gain `uint32_t nsId`;
  - `broker/libs/node/src/AdminWorker.cpp`:
    - `startNext_`: a branch per new request;
    - `buildEvent_`: the v2 nsId and the namespace kinds;
    - name checks (~:1164, ~:1341): `validNamespaceName`, and the local part only for objects;
    - the limit-default resolution;
    - the `maxNamespaces` check;
  - the gateway dispatch: 81 and 82 forward to the admin thread like 64–79;
  - the cross-namespace reference check, done at validation through ApplyV2 preview (`previewFingerprint`), with no
    second copy.
- Test: `broker/libs/node/tests/NamespaceAdminTest.cpp` (on `CatalogDdlRig.h`).

**Interfaces:**
```cpp
struct NamespaceDdlReq   { RequestId rid; NamespaceDdlKind kind; std::string name; std::array<uint8_t,4> mask;
                           rune::protocol::NamespaceLimits limits; Principal principal; };
struct ListNamespacesReq { uint32_t afterNsId; uint16_t max; uint32_t callerNsId; bool clusterAdmin; };
// AdminWorker: NamespaceLimitDefaults defaults_ (from config, boot); resolveLimits(mask, given) → full record.
```

- [ ] **Step 1: failing tests** in `NamespaceAdminRegression.*`:
  - `CreateRegression`: Create `a` with mask {maxTopics} = 5. The event has maxTopics 5 and every other field equal to
    the defaults; nsId 1.
  - `CreateDuplicateRegression`: `TopicExists`.
  - `CreateInvalidNameRegression`: the 20-row name table. Any refusal is `InvalidRequest`.
  - `MaxNamespacesRegression`: with `RUNE_MAX_NAMESPACES=2`, `default` plus `a` exist; Create `b` →
    `NamespaceLimitExceeded` (client v14) and `InvalidRequest` (client v13).
  - `SetLimitsFullRecordIdempotentRegression`: the same SetLimits twice gives one event (dedupe) and an identical
    state. A different request id with the same record gives a second event and an identical fingerprint.
  - `DefaultsAtRequestTimeRegression`: change the defaults between two creates (rig reboot), and each event carries
    the default resolved at its own request.
  - `ListPagingRegression`: 300 namespaces, pages of 100, `nextAfter` chains; ns admin gets only its own.
  - `TopicDdlLandsInNsRegression`: CreateTopic with nsId 1 gives an event with nsId 1 and the directory `ns-1`.
  - `CrossNsViewRefusedRegression` (even as the cluster admin).
  - `BackToBackDedupeRegression`.
  - `LeaderChangeMidCreateRegression` (on `CatalogTrio.h`): kill the leader after the append and before the reply.
    The client retry with the same rid gives exactly one NamespaceCreated, and nsIds have no gap: the next create is
    nsId 2.

  Mutation: skip the default resolution → Defaults fails. Skip the `maxNamespaces` check → Max fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceAdmin|AdminWorker|CatalogDdl' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

### Task 8: broker security admin — ns-scoped security DDL and escalation guards

Spec §2; D10, D11, D15.

**Files:**
- Modify:
  - `broker/libs/node/src/SecurityAdmin.cpp`: `startDdl_` (~:337-454), which:
    - resolves the target name's namespace through the admin thread's `CatalogState::namespaces()`;
    - checks it is ACTIVE;
    - stamps the event nsId;
    - runs the guards.
  - `removesLastSecurityAdmin` (~:326-334) is unchanged, because it is cluster-only by construction.
  - The SecurityDdl wire name field carries `ns/local` or `local`.
- Test: `broker/libs/node/tests/NamespaceEscalationTest.cpp` (on `SecurityDdlRig.h` plus a catalog state with `a`
  and `b`).

**Guard function** (one place, called before append):
```cpp
enum class GuardVerdict : uint8_t { Allow, NotAuthorized, Missing };
GuardVerdict nsGuard(const CallerPerms& caller, uint32_t targetNsId, const SecurityDdl& ddl);
// ClusterAdmin → Allow for everything.
// caller.nsAdmin && targetNsId == caller.nsId → Allow unless the DDL grants/revokes Cluster or Namespace resources,
//     alters/drops a user who holds Namespace Admin, or creates a namespace → NotAuthorized.
// targetNsId != caller.nsId → Missing (hidden).  Otherwise today's rules.
```

- [ ] **Step 1: failing tests:** `NamespaceEscalationRegression.*`, table-driven, 30 rows. The caller is
  NsAdmin(a) unless noted.
  - Granting Admin on Cluster: NotAuthorized.
  - Granting Admin on Namespace "a": NotAuthorized.
  - Granting Admin on Namespace "b": Missing.
  - Grants to a role in `b`: Missing.
  - Adding `b/u` to role `a/r`: Missing.
  - CreateUser `b/u`: Missing.
  - CreateUser `u`, giving `a/u`: Allow.
  - CreateRole, Grant Read on Topic `t`: Allow.
  - Granting Truncate on `t`: Allow.
  - Dropping a fellow ns admin: NotAuthorized.
  - Changing the password of a fellow ns admin: NotAuthorized.
  - Changing its own password: Allow.
  - A plain user creating a user: NotAuthorized.
  - A plain user in `b` doing anything in `a`: Missing.
  - ClusterAdmin granting NsAdmin of `a` to `a/u`: Allow.
  - ClusterAdmin granting Namespace-Admin `a` to a role in `b`: refused `InvalidRequest` (D10, name must equal the
    role's ns).
  - Each verdict is checked through SecurityDdl. Rows 1, 6 and 8 are re-run through the QL text and CLI paths in
    Task 11.
  - Also:
    - `...MissingResponseBytesEqualRegression`: the Missing rows' DdlResponse bytes equal those for a nonexistent ns.
    - `...LastSecurityAdminUnaffectedRegression`: #121's guard still refuses dropping the last cluster admin, and
      dropping the last ns admin is allowed.

  Mutation: remove the Cluster-grant branch in `nsGuard` → row 1 fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceEscalation|SecurityAdmin|SecurityDdl' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Checkpoint B.**

### Task 9: gateway — session namespace from login

Spec §2, §11; D12.

**Files:**
- Modify:
  - `broker/libs/gateway/include/rune/gateway/SessionAuth.h`: `nsId` on `AuthAuthenticated` and
    `AuthPasswordChangeRequired`;
  - `ScramServer.cpp` (~:105): the lookup by `parseQualified(user)` → (nsId via the control-thread catalog mirror's
    `NamespaceRegistry`, local);
  - `ClientGateway.cpp` `handleAuthClientFirst_` (~:8920) and the auth-final path, which store the nsId;
  - the anonymous path, which sets nsId 0 and stays gated by `RUNE_ANONYMOUS_READ` (off).
- Test: `broker/libs/node/tests/NamespaceLoginTest.cpp` (`AuthBroker.h`, `AuthWireClient.h`).

- [ ] **Step 1: failing tests** in `NamespaceLoginRegression.*`:
  - `QualifiedLoginRegression`: `a/alice` logs in with session nsId 1.
  - `BareLoginIsDefaultRegression`.
  - `DefaultQualifiedEqualsBareRegression`: `default/alice` is the same user as `alice`.
  - `UnknownNsLikeWrongPasswordRegression`: `nosuch/alice`, `a/nosuch`, `a//x` and the right user with a wrong
    password all produce the same server-first message shape (the fake verifier), the same final code
    `AuthenticationFailed` and the same number of control-loop iterations to reply.
  - `SameNameDifferentNsDifferentPasswordRegression`.
  - `AnonymousOffByDefaultRegression`, and `AnonymousOnIsDefaultNsRegression`.

  Mutation: a fast-fail on an unknown ns → UnknownNsLikeWrongPassword fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceLogin|GatewayAuth|Scram' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

### Task 10: gateway — name resolution, permissions, listings, group/txn refusal

Spec §1, §2, §9; D7, D8, D9; HARD RULES 2, 3 and 5.

**Files:**
- Create: `broker/libs/gateway/include/rune/gateway/NsResolve.h`, `src/NsResolve.cpp`.
- Modify `ClientGateway.cpp`:
  - every name-taking handler:
    - DefineTopic, AlterTopic, DropTopic, AlterTopicConfig;
    - CreateView, DropView, AlterView;
    - GetTopicSchema (~:8170);
    - Metadata (`handleMetadata` ~:10206);
    - name-addressed Subscribe and Produce lookups;
    - SecurityDdl's name;
    - list requests;

    each calls `resolveName` and answers through `hiddenAsMissing`;
  - `compilePerms_` (~:8521) passes `session.nsId`;
  - `denyGroupRequest_` / `denyTxnRequest_` apply D8;
  - Metadata and the list responses are filtered by `forEachInNs`. The cluster admin gets every ns, with foreign names
    qualified.
- Test: `broker/libs/node/tests/NamespaceGatewayTest.cpp`; bench in `broker/bench/` (the existing produce and fetch
  latency benches).

**Interfaces:**
```cpp
enum class Resolved : uint8_t { Found, Missing, Malformed };
struct Resolution { Resolved r; uint32_t nsId; std::string_view local; };
Resolution resolveName(const Session&, const rune::catalog::NamespaceRegistry&, std::string_view qualified); // zero alloc
// One per op family: builds the exact response a missing object gets today (same code, same message text).
void hiddenAsMissing(Op op, const Session&, int64_t correlationId, ResponseSink&);
```

- [ ] **Step 1: failing tests** in `NamespaceGatewayRegression.*`:
  - `ResolveTableRegression`: 18 rows over (session ns, admin?, name) → Resolution, plus a zero-allocation check.
  - `ForeignIdNoPermsRegression`: a user in `a` with a Read grant on everything in `a` does a Fetch, Produce or
    GetViewRow by an id from `b` and gets NotAuthorized. The cluster admin gets Ok.
  - `ListingsFilteredRegression`: Metadata(all) for a user in `a` shows only `a` topics, with bare names. For the
    cluster admin it shows all, with `b/t` qualified.
  - `ErrorMessageNoForeignNameRegression`: every refusal text across the matrix ops contains no foreign
    namespace or object name. The test greps the response bytes for `b` names.
  - `GroupTxnRefusedOutsideDefaultRegression`: every template 32–54 from an `a` session gives NotAuthorized, and the
    coordinator's state is untouched (its counters stay the same).
  - `MalformedNameRegression`.

  Mutation: in `hiddenAsMissing`, change the message text for one op → the matrix ForeignEqualsMissingBytes test
  (Task 12) fails. Here `ResolveTable` row 7 fails.
- [ ] **Step 2: hot path proof.** `build-bench`, interleaved ABAB on this branch vs. `main` for the produce and fetch
  p99 benches (BENCHMARKING.md counts: ≥5 for latency). The p99 delta must be within the BENCHMARKING.md noise band.
  Record the numbers in the commit message.
- [ ] **Step 3:** implement. Run `ctest --test-dir build -R 'NamespaceGateway|GatewayAuth|AuthVisibility|Metadata' --output-on-failure -j8`.
- [ ] **Step 4:** commit and push.

### Task 11: QL, client API, CLI `rune namespace create|list|limits`

Spec §10; D6, D13.

**Files:**
- Modify:
  - `ql/src/Parser.cpp`: `parseName` (~:828) accepts a quoted `'ns/name'` string for users, roles and objects;
    `parseResource` (~:937) adds `NAMESPACE 'name'`; `parseGrantRevoke` (~:965) adds `TRUNCATE`.
  - `client-cpp/include/rune/client/Client.h`: `createNamespace`, `setNamespaceLimits`, `listNamespaces`;
  - the client `.cpp`;
  - `client-cpp/src/cli/Cli.cpp`: dispatch and usage;
  - `client-cpp/CMakeLists.txt`: add `NamespaceCommands.cpp`.
- Create: `client-cpp/src/cli/NamespaceCommands.cpp`.
- Test: `ql/tests/NamespaceQlTest.cpp`, `client-cpp/tests/NamespaceClientTest.cpp`, CLI tests next to the existing
  `SecurityCommands` tests.

**Interfaces:**
```cpp
// Client.h
Result<uint32_t>                        createNamespace(std::string_view name, const NamespaceLimitsPatch& = {});
Result<void>                            setNamespaceLimits(std::string_view name, const NamespaceLimits& full);
Result<std::vector<NamespaceInfo>>      listNamespaces();   // pages internally
// CLI
//   rune namespace create <name> [--max-topics N ...one flag per §5.2 field, kebab-case]
//   rune namespace list [--json]
//   rune namespace limits <name>                (print)
//   rune namespace limits <name> --set k=v ...  (read-modify-write the full record)
```

- [ ] **Step 1: failing tests:**
  - QL:
    - `NamespaceQlRegression.QualifiedNamesRegression`: `CREATE USER 'a/alice'`, `GRANT READ ON TOPIC 'a/t' TO ROLE
      'a/r'`, `GRANT ADMIN ON NAMESPACE 'a' TO ROLE 'a/admins'` and `GRANT TRUNCATE ON TOPIC t …` produce the
      expected wire.
    - Bare identifiers are unchanged.
    - `'a/b/c'` is a parse error.
    - The QL latency gate still passes (label `latency`).
  - Client: `NamespaceClientRegression.CreateListLimitsRegression` against an in-process broker. A v13 client sees
    `InvalidRequest` for `NamespaceLimitExceeded`.
  - CLI:
    - `NamespaceCliRegression.FlagsCoverEveryLimitRegression`: every `NamespaceLimits` field has a flag, checked
      against `kNamespaceLimitFieldCount`.
    - `...LimitsSetReadModifyWriteRegression`: `--set` changes one field and keeps the others.
    - `...UsageRegression`.
  - The Task 8 rows 1, 6 and 8 run through QL text and the CLI and give the same verdicts.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'NamespaceQl|NamespaceClient|NamespaceCli|Parser|Cli' --output-on-failure -j8`.
- [ ] **Step 3:** commit and push.

**Checkpoint C.**

### Task 12: matrix rig and count pin (1-node, real broker)

Spec §8.

**Files:**
- Create:
  - `broker/libs/node/tests/support/NamespaceMatrixExpectations.h`;
  - `broker/libs/node/tests/support/NamespaceRig.h`, built on `AuthBroker.h`. It sets up namespaces `a` and `b`, one
    admin and two users per namespace, a cluster admin, an `a/t`, `b/t` and `b/only` topic, views and schemas;
  - `broker/libs/node/tests/NamespaceMatrixTest.cpp`.

- [ ] **Step 1:** `NamespaceMatrixRegression.MatrixCountRegression`: iterate the enums; assert 560 / 21 / 539 / 56 /
  595.
- [ ] **Step 2:** `TEST_P` over all 539 applicable sequential cells: a real client against a real broker, with the
  default config plus `RUNE_ANONYMOUS_READ=on` only for the Anonymous rows. Each cell asserts the outcome and, for
  `Missing`, compares the full response bytes with the same op on a truly missing name
  (`...ForeignEqualsMissingBytesRegression` is the name of that comparison sub-check).
- [ ] **Step 3:** the 56 timing cells as `TEST_P` (`BackToBack`, `ConcurrentSameName` and `Restart` on the 1-node
  rig; `LeaderChange` on Task 14's trio). Each drive loop has an iteration cap and no sleeps.
- [ ] **Step 4:** mutation: make `resolveName` return Found for a foreign ns → more than 100 cells go red (record the
  count in the commit). Run `ctest --test-dir build -R NamespaceMatrix --output-on-failure -j8`, then
  `--repeat until-fail:20`.
- [ ] **Step 5:** commit and push.

### Task 13: seeded model-checked stress

Spec §8.

**Files:** create `broker/libs/node/tests/NamespaceStressTest.cpp`.

- [ ] `NamespaceStressRegression.SeededModelRegression`, with seeds 1..20 (the seed printed on failure):
  - Each step is chosen at random from: create a namespace; create or drop a topic, view or user in a random
    namespace; grant; a cross-namespace probe by name and by id; restart the broker every 50 steps.
  - The model is an in-test map of what exists per nsId.
  - Invariants after every step:
    1. No caller ever observes an object outside its namespace (except the cluster admin).
    2. Every Missing answer is byte-equal to the model-missing answer.
    3. The fingerprint after a restart equals the one before it.
    4. nsIds are strictly increasing.
  - The Phase 2 delete invariant (nothing of a deleted nsId survives) is left as a TODO in the model, with a pointer
    to the Phase 2 plan.
  - Iteration cap: 2 000 steps per seed. Also run under `-j8` load once.
- [ ] Mutation: drop the nsId from `compilePerms_` → a seed fails within 200 steps.
- [ ] Commit and push.

### Task 14: 3-node slice

Spec §8.

**Files:** create `broker/libs/node/tests/NamespaceTrioTest.cpp` (on `AuthTrio.h` / `CatalogTrio.h`).

- [ ] `NamespaceTrioRegression.*`:
  - `CreateReplicatesRegression`: every node's registry shows `a`, nsId 1.
  - `LeaderChangeMidCreateConvergesRegression`: this is the matrix LeaderChange timing for the 7 DDL ops.
  - `FollowerRestartCatchesUpNamespacesRegression`.
  - `LoginOnAnyNodeRegression`: `a/alice` logs in on each node with nsId 1.
  - `MetadataFilteredOnFollowerRegression`.
  - `DiskLayoutOnEveryNodeRegression`: every node has `ns-1/topics/t`.
  - Convergence comes from ISR plus idempotent restore; there is no timeout tuning. Drive loops are capped.
- [ ] Run `ctest --test-dir build -R 'NamespaceTrio|NamespaceMatrix.*LeaderChange' --output-on-failure -j8`. Commit and
  push.

**Checkpoint D.**

### Task 15: docker e2e and docs

Spec §8, §10; CLAUDE.md rule 11.

**Files:**
- Create: `broker/docker/namespace-e2e.sh`, modelled on `auth-e2e.sh`.
- Modify:
  - `docs/` operations page for namespaces: `rune namespace`, `ns/name`, roles and the anonymous flag;
  - `README.md` link;
  - root `CLAUDE.md` §8, which notes that Phase 1 is done and that group and txn requests outside `default` are
    refused until Phase 2.
- [ ] On the VM, holding the lock, with the default config on 3 nodes:
  1. bootstrap;
  2. `rune namespace create a`;
  3. the cluster admin creates `a/admin` with Namespace Admin;
  4. `a/admin` creates `a/alice`, topic `t` and a grant;
  5. `alice` produces and consumes `t`;
  6. a `default` user cannot see `t`;
  7. `rune namespace list` as `a/admin` shows only `a`;
  8. kill the catalog leader and repeat steps 5–7;
  9. restart the cluster and repeat steps 5–7;
  10. `rune namespace limits a --set max-topics=1`, and a read-back shows it. It is stored only; enforcement is
      Phase 2.

  Check `df` before and after, and clean up only your own containers.
- [ ] Commit and push.

**Checkpoint E**, then the final whole-branch review (most capable model, exhaustive and adversarial, against the
Review Focus list and the matrix).

## Spec gaps (decided here, recorded for review)

- The spec does not say which code a duplicate namespace name gets. This plan reuses `TopicExists` (25) (Global
  Constraints) instead of adding a code used only here.
- The spec leaves the namespace DDL wire open. D13 fixes it as one template with a kind byte, like SecurityDdl.
- The spec does not say whether the cluster admin may create objects inside another namespace. D16 allows it; the
  objects land in the target namespace.
- The spec does not settle anonymous access to a non-`default` namespace by qualified name. It gives `Missing`
  (§11 plus D7).

## Self-review (spec section → task)

| Spec | Task |
|---|---|
| §1 model, qualified names, missing ≡ hidden, keys, no cross refs, default, nsId, disk, ids global | 2, 3, 5, 6, 7, 10 |
| §2 roles, ns/user, ns/role, Truncate / Namespace enums, escalation, last-admin guard | 1, 4, 8, 9 |
| §3 catalog events, formatVer 2, security nsId tags, new error codes | 1, 2, 3, 4 |
| §5.1 names, defaults resolved into the event, SetLimits full and idempotent, ns admin reads only | 1, 7, 11 |
| §5.2 limit groups (the stored record) | 1 (struct), 11 (CLI flags); enforcement is Phase 2 |
| §7 Phase 1 scope | 1–15 |
| §8 matrix: unit, 1-node, 3-node, e2e, seeded model | 12, 13, 14, 15 |
| §9 review focus | Review Focus 1–11 |
| §10 `rune namespace create|list|limits` | 11 (the rest is Phase 2) |
| §11 anonymous → `default` only, off by default | 9, 12 |
| §4, §5.3, §5.4, §6 | Phase 2, Phase 0, Phase 3 plans |

## Execution handoff

Run the plan with superpowers:subagent-driven-development:
- one implementer per task (mid-tier model) with the exact recipe above;
- a task review after each task;
- checkpoints A–E as listed.

Implementers never spawn subagents; at most 3 agents run at once. Root-cause diagnosis of any red test stays with the
controller (CLAUDE.md rule 14).
