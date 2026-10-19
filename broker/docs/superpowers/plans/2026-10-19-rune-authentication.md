# Rune — Authentication and Access Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every client proves who it is with SCRAM-SHA-512 and every request is checked against explicit, allow-only grants (default deny, every refusal an explicit `NotAuthorized`); users, roles, passwords and grants are fingerprinted, replayable events in one replicated, never-compacted internal log `__security` (the `__catalog` design, restricted to brokers and security admins); live grant / revoke / disable / drop changes reach every live session through the rings and end what they must end, explicitly; brokers prove possession of a shared cluster key to each other and encrypt every inter-broker frame; a fresh cluster is bootstrapped once with `rune security bootstrap`; a dev mode (`RUNE_AUTH=disabled`) exists for single-node work only. Authorization adds one bitmask load to the produce hot path and nothing else.

**Architecture:** A new library `rune_security` (rune-codec sibling, namespace `rune::security`, linked by the broker, the client and the `rune` CLI — exactly like `rune_catalog`) defines the fixed-layout event (`formatVer` 1, the same 36-byte frozen header as `__catalog`), the security state (users, roles, grants), the frozen v1 apply rules, per-object fingerprints, the effective-permission rules, a verifying replayer, the read queries and the `.rsec` golden files. rune-crypto gains SCRAM (RFC 5802 / 7677), HMAC, PBKDF2, HKDF and secure random. In the broker, `__security` is internal partition `kSecurityTopicId = -7`, built, placed (controller-ISR path, never unclean, minIsr = majority), replicated (the admin-thread replication plane `__catalog` already uses) and applied (a `SecurityApplier` modelled on `CatalogApplier`: offset order, fingerprint verify, halt, checkpoint before effects) exactly like `__catalog`; the `__security` leader's admin thread validates security DDL against its applied state with the shared library and appends acks=all. Every applied event is mirrored to the control thread, which owns a `SecurityView` (its own copy of the state) that the gateway uses to run the SCRAM server, to compile each session's **permission table** (one op bitmask per topic / view id, plus cluster bits and group / transactional-id patterns) at login, and to refresh, end or revoke live sessions on every change. Shard workers receive each session's WRITE bitmap over the session-lifecycle ring (the SessionAnnounce path) and check one bit per produce. Inter-broker links run a mutual HMAC challenge-response under the cluster key on a dedicated peer-auth stream (control thread), derive per-link AEAD keys, distribute them to every thread that owns an inter-broker transport over the existing rings, and every inter-broker frame (consensus, replication, admin plane) is sealed with the client transport's AEAD (rune-crypto `AeadCipher`, per-sender derived keys, replay windows).

**Tech Stack:** C++23, OpenSSL 3 (via rune-crypto), GoogleTest (`TEST_P` for the matrix), Google Benchmark, CMake/Ninja (`build-fast`, `build-bench-rel`), real Aeron UDP for the in-process BrokerNode integration tests, docker compose (3 nodes) for e2e (run on the k3s VM).

**Spec:** `docs/superpowers/specs/2026-10-13-rune-authentication-design.md` (approved 2026-10-13; scheduled after catalog history, before Part D). Every section §1–§12 is mapped to tasks in the Self-review.

**Builds on / order:** executes on top of the ISR protocol redesign (`feat/isr-protocol`, storage identity + controller-term records) integrated with the finished catalog-history phase (`feat/catalog-history` tip `c50b06cd`). What exists today and what this plan builds on (each verified in the code; line numbers are hints — use the quoted anchors):

- **Sessions today:** `rune::gateway::Session` (`libs/gateway/include/rune/gateway/SessionTable.h`) is created by `ClientGateway::handleConnect` → `pollPendingConnects` (anchor: `// Async accept path (BR-7a)`); TLS 1.3 runs inside Connect / ConnectResponse / `HandshakeContinue{,Response}` (rune-crypto `TlsSession`, `ConnectDowngradePolicy.h`), then frames are AEAD-sealed (`CryptoFrame.h`). `ConnectRequest.authToken` is carried but **nothing reads it** (the client's comment "Validated by the broker's existing auth path" is false — fixed in Task 7). There is no identity, no authorization anywhere.
- **Request dispatch:** one switch in `ClientGateway::onFrame`-path (anchor: `switch (tid) {` followed by `case pc::TemplateId::Connect:` in `ClientGateway.cpp` ~`:1229`) — every client request passes through it. Shard mode is the exception: produce goes straight to a worker's per-worker ingress (`Worker::handleWorkerIngressFrame_`, `Worker.h` ~`:6659`); the worker knows sessions only from `WorkerOp::Type::SessionAnnounce` (runtime → every worker, `SessionAnnouncer.h`, announce-before-accept, on the session-lifecycle ring).
- **`__catalog` (the pattern `__security` follows):** `CatalogPartition.h` (`kCatalogTopicId = -6`, `makeCatalogPartitionConfig`, `dispatchCatalogFrame`), `CatalogApplier.{h,cpp}`, `CatalogCheckpoint.{h,cpp}`, `AdminWorker.{h,cpp}` (role variant, in-flight variant, request-id dedupe, `kDdlTimeoutNs`), `CatalogReadServer.{h,cpp}` (Subscribe on −6, hard-codes `kCatalogTopicId` twice — generalized in Task 10), `ReplicationMetadataBridge.h` (`isCoordinatorTopic_` includes −6; `postCatalogRole_`), `MetadataService::decideCoordinatorPartition`, `AdminChannels.h` (ring ids). ISR v3 added `kMsgCatalogHoldLeadership = 21` (storage-gate hold), `catalogReplica_->setStorageId(...)` and the catalog partition strip on storage reset — `__security` gets the same.
- **Ring ids in use:** role ring 9, 10, 21; request ring 1, 2, 7, 8, 10–15; control→admin 16–20; admin→control 1–3, 5–12. WorkerOp types 0–20 (+200 test). Counter types up to `1'000'225` (`RoleCounter.h`). Error codes up to 43. Client version 8, broker version 4. Templates: control 1–11, data 16–21, groups/txn 32–54, schema/view/catalog 64–78.
- **Inter-broker links:** plaintext Aeron UDP: consensus (`UdpMessageSink` send, `AeronIngress` receive), replication (`AeronPartitionTransport`, `WorkerReplicationTransportImpl` — the latter also carries the admin-thread catalog plane). No peer authentication, no encryption.
- **rune-crypto:** `AeadCipher` (AES-256-GCM / ChaCha20-Poly1305), `deriveSenderKey` (HKDF-Expand SHA-256), `TlsSession`. No HMAC / PBKDF2 / SCRAM / random helpers. The broker and the client compile rune-crypto from source with an explicit source list (`CMakeLists.txt` anchor `add_library(rune_crypto STATIC`).

## Execution context

- **Repos / branch:** create `feat/authentication` in each worktree under `/Users/vradu/Projects/se-worktrees/`, from the integration base:
  - `rune-broker`: the base is `feat/isr-protocol` with `feat/catalog-history` merged (the controller merges it first if `git -C /Users/vradu/Projects/se-worktrees/rune-broker-drbugs merge-base --is-ancestor c50b06cd feat/isr-protocol` fails), then `git -C /Users/vradu/Projects/se-worktrees/rune-broker switch -c feat/authentication <that commit>`. Verify `grep -n kMsgCatalogHoldLeadership libs/node/include/rune/node/AdminChannels.h` and `grep -n 'CatalogReadServer' libs/node/include/rune/node/AdminWorker.h` both hit before Task 1.
  - `rune-protocol`: from `feat/storage-incarnation` (it contains `feat/catalog-history`; the ISR broker builds against it — `rune-protocol-storage` worktree).
  - `rune-client-cpp`: from `feat/catalog-history` merged with the client commit the ISR broker builds against (`rune-client-cpp-storage` at `0f53f2a`).
  - `rune-ql`, `rune-codec`: from `feat/catalog-history`.
  - `rune-crypto`: from `main`.

  Verify each with `git -C <repo> log --oneline -1` before Task 1. Never touch `/Users/vradu/Projects/rune-protocol` or `/Users/vradu/Projects/rune-client-cpp` (other branches, dirty). Re-point `RUNE_PROTOCOL_INCLUDE` of `build-fast` / `build-bench-rel` at the worktree used (`cmake -DRUNE_PROTOCOL_INCLUDE=... <build>`), and add `-DRUNE_CRYPTO_ROOT=/Users/vradu/Projects/se-worktrees/rune-crypto`.
- **Builds (re-run cmake only after a `CMakeLists.txt` edit):** broker `build-fast` (Ninja, Debug, `BUILD_TESTING=ON`), broker benchmarks `build-bench-rel` (`BUILD_BENCHMARKS=ON`, `-O3 -DNDEBUG`), client `build`, protocol `build` (`rune_protocol_tests`), rune-ql `build` (`rune_ql_tests`), rune-codec `build` (`rune_codec_tests`, `rune_catalog_tests`, new `rune_security_tests`) and `build-rel` (`rune_codec_bench`), rune-crypto `build` (`BUILD_TESTING=ON`, `rune_crypto_tests`) and `build-rel` (`BUILD_BENCHMARKS=ON`, created in Task 1). Broker client-integration tests: `build-fast` target `rune_client_broker_tests` (the real client from `RUNE_CLIENT_ROOT`).
- **Test gate (CLAUDE.md rule 7):**
  - **Per task / fix round:** build, then run the executables covering the code the task touches plus every new or changed test, at `-j14` — each task's "Run" step names the exact filter: `ctest --test-dir <build> -R '<filter>' --output-on-failure -j14`.
  - **The FULL `*Regression*` suite runs only at the checkpoints**, in every repo touched since the last checkpoint (broker `ctest --test-dir build-fast -R Regression --output-on-failure -j14`; client, protocol, rune-ql, rune-codec, rune-crypto `ctest --test-dir build -R Regression --output-on-failure -j14`), and also under ASan+UBSan and TSan (broker `build-asan` / `build-tsan`, client `build_tsan` with `BUILD_TESTING=ON`). A checkpoint failure is bisected over the commits since the last green checkpoint.
  - **Checkpoints:** A after Task 4 (crypto + shared library); B after Task 8 (wire, language, client); C after Task 13 (log, write path, login, enforcement, workers — nothing earlier ships partial enforcement); D after Task 16 (live changes, peer links); E after Task 19 (matrix, stress, 3-node slice); F after Task 20 — before the final whole-branch review.
- **Docker e2e** (Task 20) runs on the k3s VM (`ssh root@178.104.16.102`, under `flock /root/vm.lock`), never on the local Mac.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Amendments after review A (2026-10-19) — binding, override the text below

- **D2 / Task 2:** the `__security` ops tag equals the kind (variant index + 1), exactly as `__catalog`; the "(= the variant index)" parenthetical is wrong. The AlterUser change tag stays 0-based.
- **D8:** the unknown-user fake salt is keyed on the 32-byte `fakeSaltSeed` carried by the Bootstrap event / `SecurityBootstrap` (set once, covered by the bootstrap MAC) — NOT on the cluster key.
- **D5 + D8:** the broker ALWAYS sends 2 salts; a user with one password, or an unknown user, gets a deterministic fake second salt. The client always answers with 2 proofs. (Task 11 broker, Task 7 client.)
- **I5:** Operation / ResourceType / PatternKind / GranteeKind are defined once in rune-protocol `SecurityTypes.h`; codec and ql alias them.
- **I6:** non-cluster grant patterns never match `__` names; v1 `validGrant` refuses patterns starting with `__`.
- **Owed test (Task 11):** `ScramRegression.TwoVerifierExchangeAcceptsOnlyTheRealSlotRegression`.

## Global Constraints

- **HARD RULES (CLAUDE.md, all eight), as they bind here:**
  1. **Shared-nothing.** The `__security` replica, `SecurityApplier`, the authoritative `SecurityState` and `SecurityAdmin` are owned by the admin thread; the `SecurityView` (the control thread's own copy), every `Session`'s auth state and `PermissionTable`, `LoginThrottle`, `ObjectIndex` and `PeerLinkAgent` by the control thread; each worker owns its `WorkerWriteGrants` and its `PeerFrameCrypto`; the admin thread owns the admin plane's `PeerFrameCrypto`. No locks, no atomics as coordination, no `thread_local`, no pointer read across threads.
  2. **Rings are the only cross-thread channel.** Every new hop is a typed message on an existing SPSC ring: applied security events (admin → control), security DDL / bootstrap / `__security` reads (gateway → admin), roles + AlterIsr relays (control ↔ admin), session permission bitmaps and peer link keys (control → worker, session-lifecycle ring), peer link keys (control → admin). No cross-thread callback.
  3. **Per-state variables.** A session's auth is `std::variant<AuthAwaitingFirst, AuthAwaitingFinal, AuthPasswordChangeRequired, AuthAuthenticated, AuthAnonymous>`; `SecurityAdmin` leadership `std::variant<SecurityFollower, SecurityLeaderCatchingUp, SecurityLeaderServing, SecurityHalted>` and in-flight `std::variant<Idle, AwaitingCommit>`; `SecurityApplier` `std::variant<Applying, Halted>` × phase `std::variant<AwaitingBoot, Booted, Live>`; a peer link `std::variant<LinkIdle, HelloSent, ChallengeSent, Established, Refused>`; the client's SCRAM exchange `std::variant<ScramSentFirst, ScramSentFinal>`; a worker's chunked permission update `std::variant<NoUpdate, Staging>`; the inter-broker crypto mode of a transport `std::variant<PeerPlain, PeerSealed>` chosen at construction. No scattered booleans.
  4. **Zero steady-state allocation.** The produce / consume / query hot path gains exactly one bounds-checked bitmask load per produce (gateway: `PermissionTable::allows`; worker: `WorkerWriteGrants::allows`) and, on inter-broker frames, one AEAD seal / open into preallocated per-transport scratch. Permission compilation, SCRAM, refreshes and handshakes are control plane (cold) but still allocate nothing once idle: an idle authenticated session, an idle `SecurityApplier`, an idle peer link allocate nothing (Task 13 / Task 16 count them).
  5. **Test and benchmark everything:** unit, the parameterized interaction matrix (720 cells), seeded model-checked stress against an independent permission model, 3-node BrokerNode integration over real Aeron, docker e2e; benchmarks: the per-produce check ≤ 2 ns (Task 13), hot-path produce / consume within noise vs auth disabled (Task 13), SCRAM login ≤ 5 ms (Task 11), revoke propagation ≤ 100 ms on 3 nodes (Task 19), `__security` replay (Task 4), peer frame seal / open (Task 16).
  6. **A `*Regression*`-named test on every fix** (every fix-on-sight below carries one).
  7. **Affected-area tests per task; the FULL `*Regression*` suite at checkpoints A–F** (plus ASan/UBSan and TSan).
  8. **Fix-on-sight.** Found while planning and fixed in the owning task: the client's `authToken` that nothing validates, documented as if a broker auth path existed (Task 7: removed from `ClientConfig` / `SessionConfig`, the wire field stays reserved and is sent empty — `ClientConnectRegression.NoAuthTokenIsEverSentRegression`); the worker's `findSessionCrypto_` linear scan over every encrypted session on **every** produce frame (O(sessions) on the hot path, Task 13: moved into the same slot-indexed table as the write grants — `WorkerSessionCryptoRegression.LookupIsSlotIndexedNotAScanRegression`); `CatalogReadServer` hard-coding `kCatalogTopicId` (Task 10: the topic id is a constructor parameter — `CatalogReadServerRegression.ServesTheTopicItWasBuiltForRegression`); the catalog event's `principal.authenticated`, always empty (Task 12: stamped with the authenticated user — `CatalogPrincipalRegression.DdlEventsRecordTheAuthenticatedUserRegression`).
- **No hangs, no silent fallback (spec §10):** every request with a decodable header is answered exactly once — OK, a specific code, `NotAuthorized`, `PasswordChangeRequired`, `SessionRevoked`, `AuthenticationFailed`, `Redirect`, `NotLeader` or `Internal`; a session that never authenticates is ended by its auth deadline with `AuthenticationFailed`; a revoked subscription is ended with an explicit `NotAuthorized`; a revoked or dropped user's sessions get `SessionRevoked`; a frame on an unauthenticated peer link is dropped, **counted and logged** (rate-limited), never applied; a peer link refusal is logged on both sides.
- **Spec §3 (verbatim, applied to `__security`):** "one partition, replicated to every broker, acks=all, no unclean election; never compacted, always disk-backed; serialized through its leader's AdminWorker; applied by every broker in offset order; each event carries a format version, the statement, the principal, a timestamp and the resulting-state fingerprint, and the apply rules are frozen per format version." Reads are restricted to brokers and `ADMIN CLUSTER` holders.
- **Credential material (spec §3):** only SCRAM-SHA-512 verifiers (salt, iteration count, `StoredKey`, `ServerKey`) are stored; never a password; the `statement` of a security event is the **redacted** statement (`'<redacted>'` in place of every password literal); a user holds at most two active verifiers.
- **Error codes (spec §10), appended after 43:** `AuthenticationFailed = 44`, `NotAuthorized = 45`, `SessionRevoked = 46`, `PasswordChangeRequired = 47`. Security DDL refusals use the existing `InvalidRequest 39` with a detail naming the cause (no other new code). The version gate answers `ConnectResponse{ProtocolVersion (1)}` (which every client version treats as non-retriable).
- **Versions:** client version 9 (`kClientVersionAuth`), broker version 5 (`kBrokerVersionAuth`). Templates: `AuthClientFirst 12`, `AuthServerFirst 13`, `AuthClientFinal 14`, `AuthServerFinal 15`, `SecurityDdl 79`, `SecurityBootstrap 80` (both answered by `DdlResponse 78`). Topic id `kSecurityTopicId = -7`. WorkerOp types `SESSION_PERMISSIONS = 21`, `PEER_LINK_KEYS = 22` (tail kinds `PERMISSION_BITS = 8`, `PEER_KEYS = 9`). Ring ids: role ring `kMsgSecurityRole = 22`, `kMsgSecurityAlterIsrResponse = 23`, `kMsgSecurityHoldLeadership = 24`; control→admin `kMsgPeerLinkKeys = 25`; request ring `kMsgSecurityDdl = 30`, `kMsgSecurityBootstrap = 31`, `kMsgSecuritySubscribe = 32`, `kMsgSecurityUnsubscribe = 33`; admin→control `kA2cSecurityAlterIsr = 20`, `kA2cSecurityApplied = 21`, `kA2cSecurityHalted = 22`; admin egress `kMsgEgressFrameGated = 27`. Counter types `1'000'226` (security apply halted), `227` (security applied offset), `228` (auth mode: 0 enabled, 1 dev-disabled), `229` (logins ok), `230` (logins failed), `231` (logins rate-limited), `232` (sessions revoked), `233` (peer links established), `234` (peer link refusals), `235` (peer frames dropped). Each id is assigned once here; no task reuses one.
- **Configuration:** `RUNE_AUTH` = `enabled` (default when unset) | `disabled`; `RUNE_CLUSTER_KEY_FILE` (required when enabled), `RUNE_CLUSTER_KEY_FILE_NEXT` (optional); `RUNE_AUTH_TIMEOUT_MS` (default 10 000); `RUNE_PEER_AUTH_STREAM_ID` (default 40). A NodeConfig built in code (tests, embedders) defaults to `AuthMode::InProcessUnauthenticated`, which `NodeConfig::parse` never produces and `src/main.cpp` refuses.
- **Carry-overs folded in:** `principal.authenticated` of `__catalog` events is now real (Task 12); Part D (`rune console`) is scheduled after this phase and must show the dev-mode banner from `ConnectResponse.authMode` (Task 5) and map codes 44–47; tenants, mTLS, tokens, deny rules and quotas stay out of scope (spec header).

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **A wrong password, an unknown user, a disabled user and no credentials must be indistinguishable on the wire** (same frames, same codes, same detail text, same salt shape) → `AuthLoginRegression.FailureAnswersAreIndistinguishableRegression` (Task 11), and the fake salt is stable across brokers → `AuthLoginRegression.UnknownUserSaltIsTheSameOnEveryBrokerRegression` (Task 19).
2. **A revoked READ must end the live subscription explicitly — never keep pushing records** → `AuthLiveChangeRegression.RevokedReadEndsLiveSubscriptionsRegression` (Task 14), matrix rule R4 (Task 17), stress invariant "every revoke ends affected subscriptions" (Task 18), the 100 ms gate (Task 19).
3. **Shard-mode produce, which bypasses the gateway, must be denied before authentication and after a revoke** → `WorkerWriteGrantRegression.UnauthenticatedSessionProduceToWorkerIsDeniedRegression` (Task 13), `AuthLiveChangeRegression.RevokedWriteFailsTheNextShardProduceRegression` (Task 14), matrix op `Produce` (shard) vs `ProduceRouted`.
4. **A security DDL answered OK means the broker that answered already enforces it** (a GRANT then an immediate request on the same broker must pass; a REVOKE then an immediate request must fail) → `SecurityAnswerGateRegression.OkIsReleasedOnlyAfterThisBrokerEnforcesItRegression` (Task 10).
5. **A broker with a wrong cluster key never reaches consensus or replication, and both sides say so** → `PeerLinkRegression.WrongKeyIsRefusedAndLoggedOnBothSidesRegression` (Task 15), `PeerFrameRegression.FramesFromAnUnauthenticatedPeerAreDroppedAndCountedRegression` (Task 16), `AuthClusterRegression.WrongKeyBrokerNeverJoinsRegression` (Task 19).
6. **NotAuthorized never reveals whether an object exists** → `AuthVisibilityRegression.MissingAndHiddenObjectsAnswerIdenticallyRegression` (Task 12), matrix R1 vs R3 parity check (Task 17).
7. **No password ever leaves the client; the recorded statement is redacted** → `SecurityStatementRegression.PasswordsAreRedactedInTheStatementSentRegression` (Task 6), `SecurityWritePathRegression.NoEventEverCarriesAPasswordLiteralRegression` (Task 10).
8. **A broker whose `__security` apply halted must fail closed** (it can no longer enforce revocations) → `SecurityHaltRegression.HaltRevokesEverySessionAndRefusesLoginsRegression` (Task 14).

Also pinned: a security DDL whose leader dies after the append is answered its original result once on retry (`SecurityWritePathRegression.DuplicateRequestIdAnswersTheOriginalResultRegression`, Task 10; `AuthClusterRegression.SecurityDdlRetryAfterLeaderKillAppliesOnceRegression`, Task 19); bootstrap refuses a second run and a request without the cluster-key proof (`SecurityBootstrapRegression.*`, Task 10); dev mode refuses multi-node (`NodeConfigRegression.DevModeRefusesAMultiBrokerClusterRegression`, Task 9); an old client is refused at the handshake (`AuthLoginRegression.PreV9ClientIsRefusedAtConnectRegression`, Task 11); a golden `__security` log replays to its pinned digests forever (`SecurityGoldenRegression.V1LogsReplayToPinnedFingerprints`, Task 4); the brute-force curve never locks out a legitimate user at another address (`LoginThrottleRegression.AttackerAtOneAddressNeverDelaysTheUserAtAnotherRegression`, Task 11).

---

## Decisions (binding for executors)

Each decision the spec left open, made here, one line each (D-numbers are referenced by tasks):

- **D1 — Where the shared library lives:** `rune_security` in rune-codec (`include/rune/security/`, `src/security/`), linking `rune_catalog` for `Principal`, `ReplayedFrom`, the LE wire helpers and the FNV helpers — the catalog precedent (F21).
- **D2 — Event header:** byte-identical to `__catalog`'s frozen 36-byte header (`formatVer, kind, objectId, prevVersion, newVersion, timestampMs, fingerprint`), so tooling reads both logs' headers alike.
- **D3 — A `Bootstrap` event kind (10):** the spec's "first `__security` event, principal `bootstrap`" is one atomic event that creates the admin user (must-change-password set) with a direct `ADMIN ON CLUSTER` grant; valid only while no enabled user effectively holds `ADMIN CLUSTER`.
- **D4 — Bootstrap needs the cluster key:** `SecurityBootstrap` carries an HMAC-SHA-512 under the cluster key (current or next) over its fields and a nonce; a fresh cluster cannot be hijacked by whoever reaches it first. The CLI reads `--cluster-key-file` (default `RUNE_CLUSTER_KEY_FILE`).
- **D5 — Two active passwords in one SCRAM exchange:** `AuthServerFirst` lists every active verifier's (salt, iterations) (1 or 2); the client answers one RFC 5802 proof per verifier, each over its own `AuthMessage` (`server-first` text reconstructed per verifier); the server accepts the one that verifies and signs with its `ServerKey`. RFC 7677's vector validates the single-verifier path.
- **D6 — Password numbering:** verifiers are numbered by age, 1 = oldest; `ADD PASSWORD` with two present and `DROP PASSWORD` of the last one are refused (`InvalidRequest`); `SET PASSWORD` replaces all and clears must-change-password.
- **D7 — SCRAM parameters:** SHA-512, 16-byte random salt, default 4096 iterations (the ≤ 5 ms login gate), accepted range 4096–1 000 000; no channel binding (gs2 header `n,,`); usernames and role names are `[A-Za-z0-9._@-]{1,128}` not starting with `__`, so no SASLprep / escaping is needed; passwords are raw UTF-8 bytes.
- **D8 — Unknown users get a deterministic fake salt:** `HMAC-SHA-512(clusterKey, "rune-fake-salt|" + name)[0..16]`, 4096 iterations, one verifier — the same on every broker and across restarts, so `AuthServerFirst` never reveals existence.
- **D9 — Password length is a client rule:** the broker never sees a password, so the minimum (12, `ClientConfig::minPasswordLength`) is enforced by the client library and the CLI when building a verifier; `rune security bootstrap` generates 24 characters.
- **D10 — Redaction:** rune-ql renders a canonical statement with every password literal replaced by `'<redacted>'`; only that text is sent and recorded.
- **D11 — Auth inside the session:** SCRAM runs on templates 12–15 after the session exists (and after TLS when negotiated, so its frames are sealed); `ConnectResponse` v9 carries `authMode` (0 required, 1 dev-disabled, 2 in-process) and `authState` (0 must authenticate, 1 resumed authenticated, 2 anonymous); a resumed session keeps its authentication.
- **D12 — TLS stays optional:** with authentication on and TLS not negotiated the session is authenticated but its frames are not integrity-protected; the broker logs it once per session and docs recommend `RUNE_REQUIRE_TLS=true` in production.
- **D13 — Auth deadline:** a session that has not completed SCRAM within `RUNE_AUTH_TIMEOUT_MS` (10 s) is answered `AuthenticationFailed` and ended; any non-auth request before authentication is answered `AuthenticationFailed` in its own response type and ends the session.
- **D14 — Throttle keys:** failures are keyed by (user, source host) and by source host, where source host is the Connect image's Aeron `sourceIdentity()` host (the real UDP source, falling back to the egress host for IPC); the first 2 (pair) / 10 (host) failures are free, then each further failure delays the next attempt by `min(250 ms · 2^(k−1), 30 s)`; a success resets the pair; entries idle 15 min are forgotten; an attempt inside the delay is answered `AuthenticationFailed` with `retryAfterMs` and not counted. An attacker at one address never delays the same user at another.
- **D15 — Implied DESCRIBE:** any granted operation on an object implies `DESCRIBE` on it.
- **D16 — CLUSTER grants:** `CREATE` / `ALTER` / `DROP` / `DESCRIBE` / `ADMIN` only; `CREATE|ALTER|DROP|DESCRIBE ON CLUSTER` apply to every topic and view (and `DESCRIBE` to groups and transactional ids); `READ` / `WRITE` on `CLUSTER` are refused (use `PREFIX ''`). `ADMIN ON CLUSTER` is the security admin only and implies no data access.
- **D17 — Valid (operation, resource) pairs:** TOPIC: READ WRITE CREATE ALTER DROP DESCRIBE; VIEW: READ CREATE ALTER DROP DESCRIBE; GROUP: READ DESCRIBE; TRANSACTIONAL_ID: WRITE DESCRIBE; CLUSTER as D16. Anything else is refused by the v1 apply rules.
- **D18 — Requests not in spec §2.1:** non-transactional `InitProducerId` needs nothing (its produces are checked); `LeaveGroup` needs nothing; `GetTopicSchema` needs `DESCRIBE`; `AddOffsetsToTxn` / `TxnOffsetCommit` need `WRITE` on the transactional id and `READ` on the group.
- **D19 — Views read their sources:** `CreateView` / `AlterView` also need `READ` on every source topic (else `READ` on a view would leak a topic the creator cannot read).
- **D20 — Self password changes:** a user may run `ALTER USER <self> SET|ADD|DROP PASSWORD` without `ADMIN`; every other security DDL and every `__security` read needs `ADMIN ON CLUSTER`. All `SHOW USERS|ROLES|GRANTS|SECURITY HISTORY` are client-side replays of `__security`, so they need `ADMIN` too.
- **D21 — Catalog reads need `DESCRIBE ON CLUSTER`:** catalog history / as-of / diff are client-side verifying replays of the whole `__catalog`; per-object filtering would break the replay, so v1 serves `__catalog` reads only to `DESCRIBE CLUSTER` holders (spec gap G2).
- **D22 — Id-addressed requests on non-live ids:** Produce / Subscribe / GetViewRow / Metadata address objects by id, so a pattern cannot be matched against a missing object; they answer `NotAuthorized` unless the session holds `DESCRIBE ON CLUSTER` (then the existing `UnknownTopic`). Name-addressed requests match patterns against the requested name, so a describable missing name answers its existing "unknown" code.
- **D23 — One NotAuthorized text:** the detail is `not authorized: <OP> on <TYPE> <requested name or id>` — built only from the request, identical for a missing and a hidden object.
- **D24 — The DDL answer gate:** a security DDL's OK is released by the control thread only after its `SecurityView` applied that event and every refreshed permission table was enqueued to every worker (`kMsgEgressFrameGated` carries the event offset).
- **D25 — Live-change effects per event kind:** Grant / Revoke / GrantRole / RevokeRole / DropRole → refresh every affected user's sessions; Disable / DropUser / `SET PASSWORD … TERMINATE SESSIONS` → `SessionRevoked`; `SET PASSWORD` without it → refresh (lifts must-change-password); Add/Drop password, Create*, Enable, Bootstrap → nothing.
- **D26 — SessionRevoked is terminal for the client:** the broker answers each live subscription `SessionRevoked` on its correlation id, then a session-level `ErrorResponse{SessionRevoked, corr 0}`, then closes the session; the client fails every in-flight request and subscription of that session with `SessionRevoked` and does not reconnect it.
- **D27 — AuthenticationFailed is terminal unless rate-limited:** with `retryAfterMs > 0` a `reconnectForeverAfterLive` session backs off `max(retryAfterMs, its backoff)` and retries; otherwise the session is `Rejected`.
- **D28 — A halted `__security` apply fails closed:** the broker revokes every authenticated session (`SessionRevoked`, detail "security log halted on this broker"), refuses new logins (`AuthenticationFailed`), answers security DDL `Internal`, raises counter `1'000'226`; the runbook is `__catalog`'s (move `dataDir/__security` aside, restart).
- **D29 — Control-thread mirror:** the admin applier is authoritative (verify, checkpoint, halt); each applied event's bytes go to the control thread (`kA2cSecurityApplied`), whose `SecurityView` applies them with the same library; at boot the startup thread hands the booted state to the control thread before any thread runs.
- **D30 — Workers hold WRITE bits only:** every other check happens at the gateway (Subscribe, groups, txn, views, DDL, reads all pass through it); the bitmap ships as `SESSION_PERMISSIONS` chunks of ≤ 512 words, staged and swapped on the last chunk (a partial table is never visible); `SessionAnnounce` carries `grantAll` (1 in dev / in-process modes, 0 when enabled) so an announced but unauthenticated session is denied by default.
- **D31 — `AuthMode::InProcessUnauthenticated`:** the default of a code-built `NodeConfig` (every existing in-process test, single- and multi-node, keeps running unchanged: anonymous, all allowed, no peer crypto); `NodeConfig::parse` yields only `Enabled` or `DevDisabled`; `src/main.cpp` refuses any other value; dev-mode's multi-node refusal applies to `DevDisabled`.
- **D32 — Cluster key file:** contents (trailing whitespace trimmed) are the key material, ≥ 32 bytes or the broker refuses to start; master = HKDF-Extract(SHA-512, salt "rune-cluster-key", contents); key id = first 8 bytes of HMAC-SHA-512(master, "rune-cluster-key-id"); group/world-readable files are warned about, not refused (Kubernetes secrets mount 0644); `rune security gen-cluster-key --out <path>` writes 64 hex characters at mode 0600.
- **D33 — Peer links:** the lower node id initiates (`PeerHello{nonceA, keyId, incarnation}` → `PeerChallenge{nonceB, macB}` → `PeerProof{macA}` → `PeerAccept` | `PeerRefuse{reason}`) on stream `RUNE_PEER_AUTH_STREAM_ID` at the peer's consensus endpoint; retried every 200 ms until established; re-run when the peer's incarnation changes or a frame names an unknown link epoch. Link keys: `HKDF(SHA-512, ikm = master, salt = nonceA‖nonceB, info = "rune-link|<from>-><to>")`, one per direction; each sending transport derives its own key from it per sender id (consensus 0, legacy replication 1, admin plane 2, worker w → 3 + w) with its own replay window at the receiver.
- **D34 — Inter-broker encryption:** every inter-broker frame becomes `[0xB7][senderId u8][fromNode u16][linkEpoch u32][seq u64][ciphertext][tag 16]` (32 bytes overhead), AAD = the 16-byte header + `toNode u16`; frames are sealed whole before Aeron fragmentation and opened after reassembly; every frame limit (`ReplicationFrameLimits.h`) subtracts the 32 bytes in every mode, so limits never change with the mode. In `InProcessUnauthenticated` the transports are built `PeerPlain` (no header, byte-identical to today).
- **D35 — Key rotation:** a broker presents its current key and accepts current or next (by key id); the three-step roll of spec §8 needs only restarts; a `PeerHello` with a key id the responder holds neither of is refused "no common cluster key".
- **D36 — Security DDL wire:** one template `SecurityDdl 79` (the catalog `DdlExtension` + one op) and `SecurityBootstrap 80`, both answered by `DdlResponse 78` (`objectId` = the user / role id, `newVersion`, detail); a follower answers `Redirect` (the `__security` leader's client endpoint) like catalog DDL; request-id dedupe as F14.
- **D37 — Security events carry no live-session data:** `TERMINATE SESSIONS` is part of the `SetPassword` op (its effect is computed by every broker from the event), never a list of session ids.
- **D38 — The `.rcat` container is generalized:** `rune/catalog/RecordFile.h` (`writeRecordFileHeader(out, magic)`, `writeRecordFileRecord`, `readRecordFile(in, magic)`); `.rcat` keeps its functions and bytes; `.rsec` uses magic `RSEC`, the same version 2 layout with per-record CRC32C.
- **D39 — Admin-log plumbing is shared:** the catalog-only helpers become topic-parameterized (`makeAdminLogPartitionConfig`, `dispatchAdminLogFrame`, the bridge's `postAdminLogRole_(topicId, …)`, `CatalogReadServer(topicId, …)`, `writeLogCheckpoint(dir, …)`); `__security` rides the admin replication plane (stream 30) next to `__catalog`, dispatched by topic id.
- **D40 — Stress linearization:** a request's answer must agree with the independent model at some instant between its send and its answer (security changes race requests); the broker's per-thread applied-offset test probes bound each change's window.

---

## Interaction matrix

**Single source of truth:** `libs/node/tests/support/AuthMatrixExpectations.h` (Task 17) — `expect(Op, Grant, Timing) → Outcome`, `naRule(...)`, `missingAnswer(Op)` and `expectedCodes(Op, Grant, Timing) → ExpectedCodes`. The unit matrix (Task 17), the seeded stress (Task 18), the 3-node slice (Task 19) and the docker e2e (Task 20) all read it; no test hard-codes an outcome.

### Operations (24 request types, spec §2.1 + D18/D19)

| Op | Issued as | Requires (what the harness grants for a "granted" cell) |
|---|---|---|
| `Produce` | shard-mode produce straight to the owning worker's ingress | WRITE TOPIC t |
| `ProduceRouted` | legacy (gateway-routed) produce | WRITE TOPIC t |
| `TxnProduce` | `AddPartitionsToTxn` + transactional produce | WRITE TRANSACTIONAL_ID x, WRITE TOPIC t |
| `TxnInit` | `InitProducerId{transactionalId x}` | WRITE TRANSACTIONAL_ID x |
| `TxnEnd` | `EndTxn{commit}` (markers) | WRITE TRANSACTIONAL_ID x |
| `Subscribe` | plain `Subscribe` (records flowing) | READ TOPIC t |
| `GroupJoin` | `JoinGroup` + `SyncGroup` + `Heartbeat`, member subscriptions live | READ GROUP g, READ TOPIC t |
| `OffsetCommit` | `OffsetCommit` | READ GROUP g, READ TOPIC t |
| `OffsetFetch` | `OffsetFetch` | READ GROUP g, READ TOPIC t |
| `ViewRead` | `GetViewRow` by view topic id | READ VIEW v |
| `ViewOpen` | open-for-read by view name | READ VIEW v |
| `Metadata` | `MetadataRequest{all}` | DESCRIBE TOPIC t (presence of t in the answer) |
| `CreateTopic` | `DefineTopic` on a free name | CREATE TOPIC name |
| `CreateView` | `CreateView` over source s | CREATE VIEW name, READ TOPIC s |
| `CreateSchema` | `DefineTopic` with a descriptor on an untyped t | CREATE TOPIC t |
| `AlterSchema` | `AlterTopic{ADD COLUMN}` | ALTER TOPIC t |
| `SetCompatibility` | `AlterTopic{SET COMPATIBILITY}` | ALTER TOPIC t |
| `AlterTopicConfig` | `AlterTopicConfig{retention.ms}` | ALTER TOPIC t |
| `AlterView` | `AlterView` | ALTER VIEW v, READ TOPIC s |
| `DropTopic` | `DropTopic` | DROP TOPIC t |
| `DropView` | `DropView` | DROP VIEW v |
| `CatalogRead` | `Subscribe{-6}` to the caught-up marker | DESCRIBE CLUSTER |
| `SecurityDdl` | `CREATE ROLE r<n>` | ADMIN CLUSTER |
| `SecurityRead` | `Subscribe{-7}` to the caught-up marker | ADMIN CLUSTER |

### Entities — how the grant is held (5)

`None` (no grant at all) · `Exact` (the exact object name) · `Prefix` (`PREFIX '<cell prefix>'`, the object named under it) · `ViaRole` (the `Exact` grants — or, for a cluster-level requirement, the cluster grant — held by a role granted to the user) · `Cluster` (the requirement's operation `ON CLUSTER`). Every cell uses a fresh user, a fresh role and object names under a fresh per-cell prefix.

### Timings (6)

| Timing | What the harness does |
|---|---|
| `Steady` | grants applied, user logs in, issues the op |
| `RevokedMid` | the op succeeds once (a subscription is live and receiving); the admin revokes the grant (or the role); the op is issued again / the subscription is watched |
| `CreatedAfterLogin` | the user logs in with the grant before the object exists; the admin creates the object; the op is issued (prefix / exact resolution on create) |
| `GrantedAfterLogin` | the user logs in with no grant; the op is denied; the admin grants; the same op on the same session |
| `ObjectMissing` | the op targets a name / id that never existed (under the cell prefix) |
| `UserDisabledMid` | the op succeeds once (subscriptions live); the admin runs `ALTER USER u DISABLE`; the session is watched; a reconnect is attempted |

### Not applicable (rules applied top-down; the first match names the reason)

| # | Rule | Cells |
|---|---|---|
| NA1 | `Cluster` × an op needing READ / WRITE (`Produce`, `ProduceRouted`, `TxnProduce`, `TxnInit`, `TxnEnd`, `Subscribe`, `GroupJoin`, `OffsetCommit`, `OffsetFetch`, `ViewRead`, `ViewOpen`) — D16 | 66 |
| NA2 | `Exact` / `Prefix` × `SecurityDdl`, `SecurityRead` (ADMIN is cluster-only) | 24 |
| NA3 | `None` × `RevokedMid`, `GrantedAfterLogin`, `UserDisabledMid` (no grant to change) | 72 |
| NA4 | `CreatedAfterLogin`, `ObjectMissing` × `CatalogRead`, `SecurityDdl`, `SecurityRead` (no target object) | 22 |
| NA5 | `CreatedAfterLogin` × `CreateTopic`, `CreateView` (the op creates it) | 10 |
| NA6 | `ObjectMissing` × `TxnEnd`, `CreateSchema` (need an existing producer / untyped topic) | 9 |

### Expected outcome per applicable cell (`expect()`; rules applied top-down)

| # | Rule | Outcome |
|---|---|---|
| R1 | `None` (timings `Steady`, `CreatedAfterLogin`, `ObjectMissing`) | `Denied`: `NotAuthorized (45)` in the op's own response type, detail per D23; `Metadata`: `FilteredOut` (answer OK, the object absent) |
| R2 | `CatalogRead` × `Exact` / `Prefix` (a topic-level DESCRIBE) | `Denied` (D21: catalog reads need DESCRIBE ON CLUSTER) |
| R3 | `ObjectMissing` (granted) | `missingAnswer(op)`: id-addressed `Produce`, `ProduceRouted`, `TxnProduce`, `Subscribe`, `ViewRead` → `NotAuthorized (45)` (D22); implicit objects `TxnInit`, `GroupJoin`, `OffsetCommit`, `OffsetFetch` → `Ok`; `ViewOpen` → `ViewRowResponse` status 4 (UnknownView); `Metadata` → `FilteredOut`; `CreateTopic`, `CreateView` → `Ok` (created); `AlterSchema`, `SetCompatibility`, `AlterTopicConfig`, `AlterView`, `DropTopic`, `DropView` → `UnknownTopic (7)` |
| R4 | `RevokedMid` | `EndedNotAuthorized`: streaming ops (`Subscribe`, `GroupJoin`, `CatalogRead`, `SecurityRead`) receive `ErrorResponse{NotAuthorized}` on the subscription's correlation id within the rig bound and deliver nothing after it; one-shot ops: the second request `NotAuthorized (45)`; `Metadata`: second answer `FilteredOut` |
| R5 | `UserDisabledMid` | `SessionRevoked`: every live subscription answered `SessionRevoked (46)`, then the session-level `SessionRevoked`, the session ends; a reconnect is `AuthenticationFailed (44)` |
| R6 | `GrantedAfterLogin` | `DeniedThenOk`: first `NotAuthorized (45)` (`Metadata`: `FilteredOut`), then — after the GRANT's OK, same session, no reconnect — `Ok` |
| R7 | otherwise (`Steady`, `CreatedAfterLogin` with a grant) | `Ok` |

### Cell counts (pinned by `AuthMatrixCountRegression.CellCountsArePinned`, Task 17)

24 ops × 5 grants × 6 timings = **720 cells**: **517 applicable**, **203 `NotApplicable`** (NA1 66, NA2 24, NA3 72, NA4 22, NA5 10, NA6 9). Applicable by rule: R1 `Denied` 62, R2 `Denied` 8, R3 `Missing` 66 (`NotAuthorized` 15, `Ok` implicit 12, UnknownView 3, `FilteredOut` 4, `Ok` created 8, `UnknownTopic` 24), R4 `EndedNotAuthorized` 79, R5 `SessionRevoked` 79, R6 `DeniedThenOk` 79, R7 `Ok` 144 = **517**.

### Every cell asserts

1. Every answer code `amatrix::expectedCodes` names, in the op's own response type — no cell hard-codes a code.
2. **Every request answered exactly once** (no timeout in the rig; the broker's `answeredRequests == receivedRequests` probe per session).
3. **No existence leak:** for every R1 / R3 cell answered `NotAuthorized`, the answer frame (code + detail) is byte-identical to the frame for the same request against a hidden existing object (the R1 `Steady` twin).
4. **No delivery after an end:** a subscription ended `NotAuthorized` / `SessionRevoked` receives no `Records` frame after the end.
5. **Counters:** logins ok / failed and sessions-revoked counters move exactly as the cell implies.
6. **Principal:** every DDL an `Ok` cell issued is recorded in `__catalog` / `__security` with `principal.authenticated` = the cell's user.

### Session lifecycle, brute force and brokers (spec §11, named tests)

| Area | Tests (owning task) |
|---|---|
| Login with each of two passwords; rotation (add, both work, drop 1, old fails) | `AuthLoginTest.BothActivePasswordsLogIn`, `AuthLoginTest.RotationAddLoginDropOldFails` (11) |
| DISABLE / ENABLE; DROP; SET PASSWORD with / without TERMINATE SESSIONS | `AuthLiveChangeRegression.DisableRevokesEnableAllowsLogin`, `…DropUserRevokesEverySession`, `…SetPasswordKeepsSessions`, `…SetPasswordTerminateRevokes` (14) |
| Bootstrap one-time password flow; bootstrap twice; bootstrap without proof | `SecurityBootstrapRegression.*` (10), `AuthLoginTest.BootstrapAdminMustChangePasswordFirst` (11), `RuneCliSecurityBrokerRegression.BootstrapThenChangePassword` (11) |
| Brute force: the curve, no lockout of the legitimate user | `LoginThrottleTest.*`, `LoginThrottleRegression.AttackerAtOneAddressNeverDelaysTheUserAtAnotherRegression` (11) |
| Brokers: right / wrong / no key; rotation across a rolling restart; dev mode refuses multi-node | `PeerHandshakeTest.*` (15), `AuthClusterRegression.*` (19), `NodeConfigRegression.DevModeRefusesAMultiBrokerClusterRegression` (9) |
| Replay: golden `__security` logs | `SecurityGoldenRegression.*` (4) |

### Coverage by layer

| Layer | Task | Cells |
|---|---|---|
| Parameterized matrix (`TEST_P` over all 720; N/A skipped naming its rule) — `AuthMatrixRig`: one in-process BrokerNode (auth enabled, cluster key, shard runtime with one legacy-routed topic for `ProduceRouted`), an admin wire client, a per-cell `AuthWireClient` speaking raw frames with rune-crypto SCRAM | 17 | all 517 applicable |
| Seeded model-checked stress (independent `PermissionModel`) | 18 | random grant / revoke / role / create / drop / disable / enable interleaved with produce (both paths), subscribe, group and DDL traffic; 64 seeds × 300 steps; invariants: no request succeeds without a grant at that moment (D40), every revoke ends affected subscriptions, every request answered |
| 3-node integration — 3 real BrokerNodes over Aeron, auth enabled, cluster key | 19 | {`ProduceRouted`, `Produce`, `Subscribe`, `CreateTopic`, `SecurityDdl`} × {`Steady`, `RevokedMid`, `UserDisabledMid`} via each broker (45 cells) + replication, leader kill mid security DDL, revoke ≤ 100 ms, peer keys, rotation, the real client |
| Docker e2e (3 nodes, real client + `rune` CLI) | 20 | bootstrap; role + user + grants; world-server client produces and consumes; revoke → explicit failure; cluster-key rotation across a rolling restart |

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-crypto/include/rune/crypto/{Kdf,Scram}.h`, `src/{Kdf,Scram}.cpp`, `tests/{Kdf,Scram}Test.cpp`, `bench/ScramBench.cpp`, `CMakeLists.txt`; broker + client `CMakeLists.txt` (rune_crypto source list) | HMAC / hash / PBKDF2 / HKDF / random / constant-time compare / base64; SCRAM-SHA-512 (and SHA-256 for the RFC vector) | 1 |
| `rune-codec/include/rune/security/SecurityEvent.h`, `src/security/SecurityEvent.cpp`, `tests/security/SecurityEventTest.cpp`, `CMakeLists.txt` (`rune_security`, `rune_security_tests`) | event model, kinds, ops, frozen v1 encoding | 2 |
| `rune-codec/include/rune/security/{SecurityState,SecurityFingerprint,Permissions}.h`, `src/security/{SecurityState,ApplyV1,SecurityFingerprint,Permissions}.cpp`, tests | state, frozen v1 rules, fingerprints, digest, effective permissions, pattern matching | 3 |
| `rune-codec/include/rune/catalog/RecordFile.h`, `src/catalog/RecordFile.cpp` (+ `CatalogFile.cpp` delegating), `include/rune/security/{SecurityReplayer,SecurityQueries,SecurityFile}.h`, `include/rune/security/testing/SecurityGoldenScript.h`, `src/security/*.cpp`, `tools/make_security_golden.cpp`, `tests/security/golden/v1/basic.rsec`, `tests/security/SecurityGoldenPins.h`, `benchmark/SecurityReplayBench.cpp` | replayer, history / as-of / grants-for, `.rsec`, golden logs, replay bench | 4 |
| `rune-protocol/include/rune/protocol/client/{Protocol,ControlMessages,Dispatch}.h`, new `AuthMessages.h`, `SecurityMessages.h`, `schema/rune-internal-ring.xml`, `include/rune_protocol_internal_sbe/{OpType,TailKind}.h`, tests | codes 44–47, version 9 / broker 5, templates 12–15 / 79 / 80, ConnectResponse v9 trailer, `kSecurityTopicId`, internal ring op types 21 / 22 | 5 |
| `rune-ql/include/rune/ql/{Ast,Parser,Backends}.h`, `src/{Parser,Backends,Redact}.cpp`, `tests/SecurityStatementsTest.cpp` | user / role / grant DDL, SHOW statements, redaction, security plans | 6 |
| `rune-client-cpp/include/rune/client/session/SessionMachine.h`, new `include/rune/client/session/ScramClient.h`, `src/agent/ClientAgent.cpp`, `include/rune/client/Client.h`, `src/Client.cpp`, `tests/session/SessionMachineAuthTest.cpp`, `tests/support/InvokerGateway.h`, `tests/client/ClientAuthErrorsTest.cpp` | SCRAM client, `Authenticating` state, credentials, salted-password cache, 44–47 handling, version gate | 7 |
| `rune-client-cpp/include/rune/client/security/{SecurityReader,SecurityDdl}.h`, `src/security/*.cpp`, `include/rune/client/detail/EventLogStream.h` (extracted from `CatalogReader`), `src/cli/{Cli,SecurityCommands}.cpp`, `include/rune/cli/SecurityCommands.h`, tests | security DDL API, `__security` reader, `rune ql` security statements, `rune security bootstrap | gen-cluster-key`, `--user` / `--password-file`, dev banner | 8 |
| `libs/node/include/rune/node/{NodeConfig,AdminLogPartition,SecurityApplier}.h`, `libs/gateway/include/rune/gateway/SecurityView.h`, new `libs/peerlink/` (`ClusterKeys`), `libs/node/src/{NodeConfig,AdminLogPartition,SecurityApplier,BrokerNode}.cpp`, `CatalogPartition.h` (delegates), `CatalogCheckpoint.{h,cpp}` (generalized), `ReplicationMetadataBridge.h`, `MetadataService.h`, `AdminChannels.h`, `RoleCounter.h`, `src/main.cpp`, tests | auth config + refusals + dev mode, cluster key, `__security` partition, applier, control mirror | 9 |
| `libs/node/include/rune/node/{SecurityAdmin,SecurityAnswerGate}.h`, `libs/node/src/SecurityAdmin.cpp`, `AdminWorker.{h,cpp}` (owns + drives it), `CatalogReadServer.{h,cpp}` (topic parameter), `libs/gateway/include/rune/gateway/{ClientGateway,IAdminRequestSink,SecurityRoute}.h`, `ClientGateway.cpp`, `libs/node/tests/support/SecurityDdlRig.h`, tests | security write path, bootstrap, `__security` reads, answer gate | 10 |
| `libs/gateway/include/rune/gateway/{SessionAuth,ScramServer,LoginThrottle}.h`, `libs/gateway/src/{ScramServer,LoginThrottle}.cpp`, `SessionTable.h`, `ClientGateway.{h,cpp}`, `libs/node/tests/client/{ClientLoginBrokerRegressionTest,RuneCliSecurityBrokerRegressionTest}.cpp`, `libs/node/tests/support/AuthWireClient.h`, tests | SCRAM server, session auth states, version gate, throttle, auth deadline, dev mode, login logs / counters, 5 ms gate | 11 |
| `libs/gateway/include/rune/gateway/{PermissionTable,ObjectIndex,Authorizer}.h`, `libs/gateway/src/{PermissionTable,Authorizer}.cpp`, `ClientGateway.cpp` (every handler), `AdminChannels.h` (`DdlMeta.authenticated`), `libs/view/include/rune/view/PlanSources.h`, tests | per-session permission tables, enforcement of every request type, visibility, prefix extension | 12 |
| `libs/node/include/rune/node/{WorkerWriteGrants,SessionAckSink,SessionAnnouncer,Worker,WorkerOp}.h`, `ClientGateway.cpp` (ship), `benchmark/AuthCheckBench.cpp`, tests | worker WRITE bitmaps, shard produce check, session crypto slot fix, ≤ 2 ns gate, hot-path parity, alloc gates | 13 |
| `libs/gateway/src/{ClientGateway,LiveSecurityChanges}.cpp`, `libs/gateway/include/rune/gateway/LiveSecurityChanges.h`, `GroupCoordinationService` hook, tests | refresh / end / revoke on every security change, halt fail-closed | 14 |
| new `libs/peerlink/` (`include/rune/peerlink/{PeerLinkMessages,PeerHandshake,PeerLinkKeys}.h`, `src/*.cpp`, `CMakeLists.txt`), `libs/node/include/rune/node/PeerLinkAgent.h`, `libs/node/src/PeerLinkAgent.cpp`, `BrokerNode.cpp`, `Worker.h`, `AdminWorker.cpp`, tests | mutual HMAC handshake, link keys, rotation, distribution | 15 |
| `libs/peerlink/include/rune/peerlink/PeerFrameCrypto.h`, `src/PeerFrameCrypto.cpp`, `libs/consensus/include/rune/consensus/{UdpMessageSink,AeronIngress}.h`, `libs/replication/{include/rune/replication/{AeronPartitionTransport,WorkerReplicationTransport,ReplicationFrameLimits}.h,src/AeronPartitionTransport.cpp}`, tests, `benchmark/PeerFrameBench.cpp` | AEAD on every inter-broker frame, drops counted, frame limits | 16 |
| `libs/node/tests/support/{AuthMatrixExpectations,AuthMatrixRig}.h`, `libs/node/tests/node/AuthInteractionMatrixRegressionTest.cpp` | matrix table + 720-cell parameterized matrix | 17 |
| `libs/node/tests/support/PermissionModel.h`, `libs/node/tests/node/AuthStressRegressionTest.cpp` | seeded model-checked stress | 18 |
| `libs/node/tests/support/AuthTrio.h`, `libs/node/tests/integration/AuthClusterIntegrationTest.cpp`, `libs/node/tests/client/ClientAuthClusterRegressionTest.cpp` | 3-node slice, gates, peer keys, rotation, real client | 19 |
| `docker/*`, `docker/lib/auth.sh`, `docker/auth-e2e.sh`, `docker/docker-compose.auth.yml`, `dr-e2e/**`, `bench/kafka-compare/*`, `docs/{SELF-HOSTING,SECURITY}.md`, `docs/release-notes/authentication.md`, client README | launchers migrated, docker e2e, docs | 20 |

---
### Task 1: rune-crypto — HMAC, PBKDF2, HKDF, secure random, constant-time compare, SCRAM-SHA-512

Spec §3 (verifiers), §5 (SCRAM-SHA-512), §8 (HMAC challenge-response), D5, D7, D8, D32, D33. Every cryptographic primitive the rest of the plan uses lives here; nothing else calls OpenSSL for them. The SCRAM construction is generic over the digest so RFC 7677's SCRAM-SHA-256 vector pins it; production uses SHA-512 only.

**Files:**
- Create: `rune-crypto/include/rune/crypto/Kdf.h`, `rune-crypto/src/Kdf.cpp`
- Create: `rune-crypto/include/rune/crypto/Scram.h`, `rune-crypto/src/Scram.cpp`
- Create: `rune-crypto/tests/KdfTest.cpp`, `rune-crypto/tests/ScramTest.cpp`, `rune-crypto/bench/ScramBench.cpp`
- Modify: `rune-crypto/CMakeLists.txt` (sources, tests, bench), `rune-broker/CMakeLists.txt` and `rune-client-cpp/CMakeLists.txt` (anchor `add_library(rune_crypto STATIC`: add `${RUNE_CRYPTO_ROOT}/src/Kdf.cpp` and `${RUNE_CRYPTO_ROOT}/src/Scram.cpp`)

**Interfaces:**
- Produces (namespace `rune::crypto`, `Kdf.h`):
  ```cpp
  enum class Digest : uint8_t { Sha256 = 0, Sha512 = 1 };
  inline constexpr std::size_t kMaxDigest = 64;
  [[nodiscard]] constexpr std::size_t digestSize(Digest d) noexcept { return d == Digest::Sha256 ? 32 : 64; }
  struct DigestBytes {                       // fixed storage: never allocates
      std::array<uint8_t, kMaxDigest> bytes{};
      uint8_t size = 0;
      [[nodiscard]] std::span<const uint8_t> view() const noexcept { return {bytes.data(), size}; }
      bool operator==(const DigestBytes& o) const noexcept;   // constant-time over size
  };
  [[nodiscard]] bool hash(Digest, std::span<const uint8_t> msg, DigestBytes& out) noexcept;
  [[nodiscard]] bool hmac(Digest, std::span<const uint8_t> key, std::span<const uint8_t> msg, DigestBytes& out) noexcept;
  [[nodiscard]] bool pbkdf2(Digest, std::string_view password, std::span<const uint8_t> salt,
                            uint32_t iterations, DigestBytes& out) noexcept;      // dkLen = digestSize
  [[nodiscard]] bool hkdf(Digest, std::span<const uint8_t> ikm, std::span<const uint8_t> salt,
                          std::span<const uint8_t> info, std::span<uint8_t> out) noexcept;   // extract + expand
  [[nodiscard]] bool randomBytes(std::span<uint8_t> out) noexcept;                // RAND_bytes; false = refuse
  [[nodiscard]] bool constantTimeEqual(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept;
  [[nodiscard]] std::string base64Encode(std::span<const uint8_t> in);
  [[nodiscard]] std::optional<std::vector<uint8_t>> base64Decode(std::string_view in);   // strict, padded
  /// D32 — the ONE derivation of a cluster key file (the broker's ClusterKeys and the CLI's bootstrap MAC):
  /// contents with trailing whitespace trimmed (>= 32 bytes, else nullopt); master = HKDF-Extract(SHA-512,
  /// salt "rune-cluster-key", contents); keyId = first 8 bytes (LE) of HMAC-SHA-512(master, "rune-cluster-key-id").
  struct ClusterKeyMaterial { std::array<uint8_t, 64> master{}; uint64_t keyId = 0; };
  [[nodiscard]] std::optional<ClusterKeyMaterial> deriveClusterKey(std::span<const uint8_t> fileContents) noexcept;
  ```
- Produces (namespace `rune::crypto::scram`, `Scram.h`) — used by Tasks 7, 8, 10, 11, 17:
  ```cpp
  inline constexpr uint32_t    kDefaultIterations = 4096, kMinIterations = 4096, kMaxIterations = 1'000'000;
  inline constexpr std::size_t kSaltBytes = 16, kNonceBytes = 24;
  struct SaltedPassword { DigestBytes bytes; };                 // Hi(password, salt, i) — the client caches it
  struct Verifier { Digest digest = Digest::Sha512; std::vector<uint8_t> salt; uint32_t iterations = 0;
                    DigestBytes storedKey, serverKey; };
  [[nodiscard]] std::optional<SaltedPassword> saltPassword(Digest, std::string_view password,
                                                           std::span<const uint8_t> salt, uint32_t iterations) noexcept;
  [[nodiscard]] std::optional<Verifier> makeVerifier(Digest, const SaltedPassword&, std::span<const uint8_t> salt,
                                                     uint32_t iterations);
  [[nodiscard]] std::optional<Verifier> makeVerifier(std::string_view password, uint32_t iterations = kDefaultIterations);
  [[nodiscard]] std::string makeNonce();                        // kNonceBytes random, base64 (no ',')
  // RFC 5802 message texts (gs2 header "n,,", no channel binding — D7)
  [[nodiscard]] std::string clientFirstBare(std::string_view user, std::string_view clientNonce);   // "n=<u>,r=<c>"
  [[nodiscard]] std::string serverFirst(std::string_view combinedNonce, std::span<const uint8_t> salt, uint32_t i);
  [[nodiscard]] std::string clientFinalWithoutProof(std::string_view combinedNonce);               // "c=biws,r=<n>"
  [[nodiscard]] std::string authMessage(std::string_view cfb, std::string_view sf, std::string_view cfwp);
  [[nodiscard]] std::optional<DigestBytes> clientProof(Digest, const SaltedPassword&, std::string_view authMessage) noexcept;
  [[nodiscard]] std::optional<DigestBytes> expectedServerSignature(Digest, const SaltedPassword&,
                                                                   std::string_view authMessage) noexcept;
  [[nodiscard]] std::optional<DigestBytes> serverSignature(Digest, const DigestBytes& serverKey,
                                                           std::string_view authMessage) noexcept;
  /// H(ClientKey) == StoredKey where ClientKey = proof XOR HMAC(StoredKey, authMessage) — constant time.
  [[nodiscard]] bool verifyClientProof(Digest, const DigestBytes& storedKey, std::string_view authMessage,
                                       std::span<const uint8_t> proof) noexcept;
  ```

- [ ] **Step 1: Write the failing tests** — `rune-crypto/tests/KdfTest.cpp`:

```cpp
// rune-crypto: the primitives authentication is built on, pinned to their RFC vectors.
#include "rune/crypto/Kdf.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace rc = rune::crypto;
namespace {
std::vector<uint8_t> hex(std::string_view h) {
    std::vector<uint8_t> v;
    for (std::size_t i = 0; i + 1 < h.size(); i += 2) v.push_back(static_cast<uint8_t>(std::stoi(std::string(h.substr(i, 2)), nullptr, 16)));
    return v;
}
std::span<const uint8_t> bytesOf(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
} // namespace

TEST(KdfRegression, HmacSha512MatchesRfc4231Case1) {
    const std::vector<uint8_t> key(20, 0x0b);
    rc::DigestBytes out;
    ASSERT_TRUE(rc::hmac(rc::Digest::Sha512, key, bytesOf("Hi There"), out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cde"
                  "daa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854"));
}

TEST(KdfRegression, HmacSha256MatchesRfc4231Case1) {
    const std::vector<uint8_t> key(20, 0x0b);
    rc::DigestBytes out;
    ASSERT_TRUE(rc::hmac(rc::Digest::Sha256, key, bytesOf("Hi There"), out));
    EXPECT_EQ(std::vector<uint8_t>(out.view().begin(), out.view().end()),
              hex("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"));
}

TEST(KdfRegression, HkdfSha256MatchesRfc5869Case1) {
    const std::vector<uint8_t> ikm(22, 0x0b);
    const auto salt = hex("000102030405060708090a0b0c");
    const auto info = hex("f0f1f2f3f4f5f6f7f8f9");
    std::vector<uint8_t> okm(42);
    ASSERT_TRUE(rc::hkdf(rc::Digest::Sha256, ikm, salt, info, okm));
    EXPECT_EQ(okm, hex("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"));
}

TEST(KdfRegression, ConstantTimeEqualComparesLengthAndBytes) {
    const std::vector<uint8_t> a{1, 2, 3}, b{1, 2, 3}, c{1, 2, 4}, d{1, 2};
    EXPECT_TRUE(rc::constantTimeEqual(a, b));
    EXPECT_FALSE(rc::constantTimeEqual(a, c));
    EXPECT_FALSE(rc::constantTimeEqual(a, d));
}

TEST(KdfRegression, Base64RoundTripsAndRejectsGarbage) {
    const std::vector<uint8_t> v{0, 1, 2, 250, 251, 252, 253};
    const std::string s = rc::base64Encode(v);
    auto back = rc::base64Decode(s);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, v);
    EXPECT_FALSE(rc::base64Decode("not base64!").has_value());
    EXPECT_FALSE(rc::base64Decode("abc").has_value()) << "unpadded input is refused (strict)";
}

TEST(KdfRegression, ClusterKeyDerivationTrimsAndRefusesShortKeys) {
    const std::string k(40, 'k');
    auto a = rc::deriveClusterKey(bytesOf(k)), b = rc::deriveClusterKey(bytesOf(k + "\n  "));
    ASSERT_TRUE(a && b);
    EXPECT_EQ(a->keyId, b->keyId);
    EXPECT_EQ(a->master, b->master);
    EXPECT_NE(a->keyId, rc::deriveClusterKey(bytesOf(std::string(40, 'j')))->keyId);
    EXPECT_FALSE(rc::deriveClusterKey(bytesOf(std::string(31, 'k'))).has_value());
}

TEST(KdfRegression, RandomBytesAreNotAllZeroAndDiffer) {
    std::array<uint8_t, 32> a{}, b{};
    ASSERT_TRUE(rc::randomBytes(a));
    ASSERT_TRUE(rc::randomBytes(b));
    EXPECT_NE(a, b);
}
```

`rune-crypto/tests/ScramTest.cpp`:

```cpp
// SCRAM (RFC 5802): the RFC 7677 SCRAM-SHA-256 vector pins the construction; SHA-512
// (what Rune ships, spec §5) is exercised end to end, both active verifiers included (D5).
#include "rune/crypto/Kdf.h"
#include "rune/crypto/Scram.h"
#include <gtest/gtest.h>

namespace rc = rune::crypto;
namespace rs = rune::crypto::scram;

TEST(ScramRegression, Rfc7677Sha256VectorProofAndServerSignature) {
    const auto salt = rc::base64Decode("W22ZaJ0SNY7soEsUEjb6gQ==").value();
    const std::string cfb  = rs::clientFirstBare("user", "rOprNGfwEbeRWgbNEkqO");
    const std::string nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const std::string sf   = rs::serverFirst(nonce, salt, 4096);
    const std::string cfwp = rs::clientFinalWithoutProof(nonce);
    EXPECT_EQ(cfb, "n=user,r=rOprNGfwEbeRWgbNEkqO");
    EXPECT_EQ(sf, "r=" + nonce + ",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
    EXPECT_EQ(cfwp, "c=biws,r=" + nonce);
    const std::string am = rs::authMessage(cfb, sf, cfwp);
    auto sp = rs::saltPassword(rc::Digest::Sha256, "pencil", salt, 4096);
    ASSERT_TRUE(sp.has_value());
    auto proof = rs::clientProof(rc::Digest::Sha256, *sp, am);
    ASSERT_TRUE(proof.has_value());
    EXPECT_EQ(rc::base64Encode(proof->view()), "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
    auto v = rs::makeVerifier(rc::Digest::Sha256, *sp, salt, 4096);
    ASSERT_TRUE(v.has_value());
    auto sig = rs::serverSignature(rc::Digest::Sha256, v->serverKey, am);
    ASSERT_TRUE(sig.has_value());
    EXPECT_EQ(rc::base64Encode(sig->view()), "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    EXPECT_TRUE(rs::verifyClientProof(rc::Digest::Sha256, v->storedKey, am, proof->view()));
}

TEST(ScramRegression, Sha512ExchangeVerifiesAndAWrongPasswordDoesNot) {
    auto v = rs::makeVerifier("correct horse battery");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v->salt.size(), rs::kSaltBytes);
    EXPECT_EQ(v->iterations, rs::kDefaultIterations);
    EXPECT_EQ(v->storedKey.size, 64);
    const std::string cfb = rs::clientFirstBare("ws1", rs::makeNonce());
    const std::string sf  = rs::serverFirst("cn+sn", v->salt, v->iterations);
    const std::string am  = rs::authMessage(cfb, sf, rs::clientFinalWithoutProof("cn+sn"));
    auto good = rs::saltPassword(rc::Digest::Sha512, "correct horse battery", v->salt, v->iterations);
    auto bad  = rs::saltPassword(rc::Digest::Sha512, "correct horse batterY", v->salt, v->iterations);
    EXPECT_TRUE(rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, am, rs::clientProof(rc::Digest::Sha512, *good, am)->view()));
    EXPECT_FALSE(rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, am, rs::clientProof(rc::Digest::Sha512, *bad, am)->view()));
    EXPECT_EQ(*rs::expectedServerSignature(rc::Digest::Sha512, *good, am),
              *rs::serverSignature(rc::Digest::Sha512, v->serverKey, am)) << "the client can verify the server";
}

TEST(ScramRegression, IterationBoundsAreEnforced) {
    const std::vector<uint8_t> salt(16, 7);
    EXPECT_FALSE(rs::saltPassword(rc::Digest::Sha512, "x", salt, rs::kMinIterations - 1).has_value());
    EXPECT_FALSE(rs::saltPassword(rc::Digest::Sha512, "x", salt, rs::kMaxIterations + 1).has_value());
    EXPECT_FALSE(rs::saltPassword(rc::Digest::Sha512, "x", {}, rs::kMinIterations).has_value()) << "empty salt";
}

TEST(ScramRegression, ProofOfTheWrongLengthNeverVerifies) {
    auto v = rs::makeVerifier("some long password");
    const std::vector<uint8_t> shortProof(63, 0);
    EXPECT_FALSE(rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, "am", shortProof));
}

TEST(ScramRegression, NoncesArePrintableWithoutCommasAndUnique) {
    const std::string a = rs::makeNonce(), b = rs::makeNonce();
    EXPECT_NE(a, b);
    EXPECT_EQ(a.find(','), std::string::npos);
    for (char c : a) EXPECT_TRUE(c >= 0x21 && c <= 0x7e);
}
```

- [ ] **Step 2: Add the build targets** — `rune-crypto/CMakeLists.txt`: `add_library(rune_crypto STATIC src/Caps.cpp src/Aead.cpp src/TlsSession.cpp src/Kdf.cpp src/Scram.cpp)`; tests list gains `tests/KdfTest.cpp tests/ScramTest.cpp`; the bench gains `add_executable(rune_crypto_scram_bench bench/ScramBench.cpp)` linked to `rune_crypto benchmark::benchmark` (find_package(benchmark) inside the `BUILD_BENCHMARKS` block). Mirror the two sources in the broker and client source lists.

- [ ] **Step 3: Run to verify they fail**

Run: `cmake /Users/vradu/Projects/se-worktrees/rune-crypto/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-crypto/build -j14 --target rune_crypto_tests`
Expected: compile error `'rune/crypto/Kdf.h' file not found`.

- [ ] **Step 4: Implement `Kdf.cpp`** — OpenSSL 3 EVP only:
  - `hash`: `EVP_Digest(msg, len, out.bytes, &n, md, nullptr)`; `md = d == Sha256 ? EVP_sha256() : EVP_sha512()`.
  - `hmac`: `EVP_MAC_fetch(nullptr, "HMAC", nullptr)` + `OSSL_MAC_PARAM_DIGEST` ("SHA256" / "SHA512") + `EVP_MAC_init/update/final`; every step checked, any failure returns false (fail closed — the `Aead.cpp` idiom).
  - `pbkdf2`: `PKCS5_PBKDF2_HMAC(password, len, salt, saltLen, iterations, md, digestSize, out.bytes)`.
  - `hkdf`: `EVP_KDF_fetch("HKDF")`, mode `EVP_KDF_HKDF_MODE_EXTRACT_AND_EXPAND`, params digest / key / salt / info (the `deriveSenderKey` pattern in `Aead.cpp`).
  - `randomBytes`: `RAND_bytes(out.data(), out.size()) == 1`.
  - `constantTimeEqual`: false on a size difference; otherwise `CRYPTO_memcmp(a, b, n) == 0`.
  - `base64Encode/Decode`: `EVP_EncodeBlock` / `EVP_DecodeBlock`; decode refuses a length not a multiple of 4, any byte outside the alphabet, and adjusts for `=` padding.
  - `DigestBytes::operator==`: `size == o.size && CRYPTO_memcmp(...) == 0`.
  - `deriveClusterKey`: trim trailing `' ' '\t' '\r' '\n'`; < 32 bytes → nullopt; `master` = HKDF-Extract (EVP_KDF "HKDF", mode `EXTRACT_ONLY`, SHA-512, salt `"rune-cluster-key"`); `keyId` = the first 8 bytes, little-endian, of `hmac(Sha512, master, "rune-cluster-key-id")`.

- [ ] **Step 5: Implement `Scram.cpp`** (RFC 5802 §3):

```cpp
// rune-crypto SCRAM (RFC 5802 §3).  ClientKey = HMAC(SaltedPassword, "Client Key"); StoredKey = H(ClientKey);
// ServerKey = HMAC(SaltedPassword, "Server Key"); ClientSignature = HMAC(StoredKey, AuthMessage);
// ClientProof = ClientKey XOR ClientSignature; ServerSignature = HMAC(ServerKey, AuthMessage).
#include "rune/crypto/Scram.h"
#include <array>
namespace rune::crypto::scram {
namespace {
std::span<const uint8_t> sv(std::string_view s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
bool clientKey(Digest d, const SaltedPassword& sp, DigestBytes& out) { return hmac(d, sp.bytes.view(), sv("Client Key"), out); }
} // namespace

std::optional<SaltedPassword> saltPassword(Digest d, std::string_view pw, std::span<const uint8_t> salt, uint32_t i) noexcept {
    if (i < kMinIterations || i > kMaxIterations || salt.empty()) return std::nullopt;
    SaltedPassword sp;
    if (!pbkdf2(d, pw, salt, i, sp.bytes)) return std::nullopt;
    return sp;
}
std::optional<Verifier> makeVerifier(Digest d, const SaltedPassword& sp, std::span<const uint8_t> salt, uint32_t i) {
    Verifier v; v.digest = d; v.salt.assign(salt.begin(), salt.end()); v.iterations = i;
    DigestBytes ck;
    if (!clientKey(d, sp, ck) || !hash(d, ck.view(), v.storedKey) ||
        !hmac(d, sp.bytes.view(), sv("Server Key"), v.serverKey)) return std::nullopt;
    return v;
}
std::optional<Verifier> makeVerifier(std::string_view pw, uint32_t i) {
    std::array<uint8_t, kSaltBytes> salt{};
    if (!randomBytes(salt)) return std::nullopt;
    auto sp = saltPassword(Digest::Sha512, pw, salt, i);
    if (!sp) return std::nullopt;
    return makeVerifier(Digest::Sha512, *sp, salt, i);
}
std::string makeNonce() {
    std::array<uint8_t, kNonceBytes> r{};
    if (!randomBytes(r)) return {};          // callers treat "" as a local failure (never sent)
    return base64Encode(r);                  // [A-Za-z0-9+/=]: printable, no ','
}
std::string clientFirstBare(std::string_view u, std::string_view c) { return "n=" + std::string(u) + ",r=" + std::string(c); }
std::string serverFirst(std::string_view n, std::span<const uint8_t> salt, uint32_t i) {
    return "r=" + std::string(n) + ",s=" + base64Encode(salt) + ",i=" + std::to_string(i);
}
std::string clientFinalWithoutProof(std::string_view n) { return "c=biws,r=" + std::string(n); }   // biws = base64("n,,")
std::string authMessage(std::string_view a, std::string_view b, std::string_view c) {
    std::string m; m.reserve(a.size() + b.size() + c.size() + 2);
    m.append(a).append(",").append(b).append(",").append(c); return m;
}
std::optional<DigestBytes> clientProof(Digest d, const SaltedPassword& sp, std::string_view am) noexcept {
    DigestBytes ck, sk, sig;
    if (!clientKey(d, sp, ck) || !hash(d, ck.view(), sk) || !hmac(d, sk.view(), sv(am), sig)) return std::nullopt;
    for (std::size_t k = 0; k < ck.size; ++k) ck.bytes[k] ^= sig.bytes[k];
    return ck;
}
std::optional<DigestBytes> serverSignature(Digest d, const DigestBytes& serverKey, std::string_view am) noexcept {
    DigestBytes s; if (!hmac(d, serverKey.view(), sv(am), s)) return std::nullopt; return s;
}
std::optional<DigestBytes> expectedServerSignature(Digest d, const SaltedPassword& sp, std::string_view am) noexcept {
    DigestBytes sk; if (!hmac(d, sp.bytes.view(), sv("Server Key"), sk)) return std::nullopt;
    return serverSignature(d, sk, am);
}
bool verifyClientProof(Digest d, const DigestBytes& storedKey, std::string_view am, std::span<const uint8_t> proof) noexcept {
    if (proof.size() != digestSize(d) || storedKey.size != digestSize(d)) return false;
    DigestBytes sig, ck, h;
    if (!hmac(d, storedKey.view(), sv(am), sig)) return false;
    ck.size = sig.size;
    for (std::size_t k = 0; k < sig.size; ++k) ck.bytes[k] = static_cast<uint8_t>(proof[k] ^ sig.bytes[k]);
    if (!hash(d, ck.view(), h)) return false;
    return constantTimeEqual(h.view(), storedKey.view());
}
} // namespace rune::crypto::scram
```

- [ ] **Step 6: Write the bench** — `bench/ScramBench.cpp`: `BM_ScramClientSaltPassword` (`saltPassword` SHA-512 at 4096), `BM_ScramServerVerify` (`verifyClientProof` + `serverSignature`), `BM_ScramFullExchangeTwoVerifiers` (two `saltPassword`s + two proofs + one verify — the rotation worst case of D5). Create `build-rel` (`cmake -S . -B build-rel -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON`).

- [ ] **Step 7: Run**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-crypto/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-crypto/build -R 'Kdf|Scram|Aead|Tls|Crypto' --output-on-failure -j14 && cmake --build /Users/vradu/Projects/se-worktrees/rune-crypto/build-rel -j14 --target rune_crypto_scram_bench && /Users/vradu/Projects/se-worktrees/rune-crypto/build-rel/rune_crypto_scram_bench --benchmark_repetitions=5 --benchmark_report_aggregates_only=true`
Expected: all PASS; `BM_ScramFullExchangeTwoVerifiers` median < 5 ms (record the three medians in the commit body; a median ≥ 5 ms is a bug to fix here — the D7 iteration default is sized by this number). Also build the broker and client once (`cmake --build .../rune-broker/build-fast --target rune_gateway`, `cmake --build .../rune-client-cpp/build --target rune_client_api`) to prove the source lists link.

- [ ] **Step 8: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-crypto add include src tests bench CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-crypto commit -m "feat(crypto): HMAC, PBKDF2, HKDF, random, constant-time compare, base64, SCRAM-SHA-512 (RFC 5802/7677)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "build: compile rune-crypto Kdf/Scram sources (authentication Task 1)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "build: compile rune-crypto Kdf/Scram sources (authentication Task 1)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `rune_security` — the event model and its frozen v1 encoding (rune-codec)

Spec §3: one fixed-layout, versioned record per event; the kinds of §3 plus `Bootstrap` (D3). Same header as `__catalog` (D2), the catalog's `Principal` / `ReplayedFrom` (D1). Nothing else in the plan encodes or decodes a security event by hand.

**Files:**
- Create: `rune-codec/include/rune/security/SecurityEvent.h`, `rune-codec/src/security/SecurityEvent.cpp` (uses `../catalog/Wire.h`)
- Modify: `rune-codec/CMakeLists.txt` (anchor: after the `rune_catalog` block — `add_library(rune_security STATIC …)` with `file(GLOB … src/security/*.cpp)`, PUBLIC include dir, `target_link_libraries(rune_security PUBLIC rune_catalog)`; after `gtest_discover_tests(rune_catalog_tests)`: `rune_security_tests` over `tests/security/*Test.cpp`), the broker `CMakeLists.txt` and the client `CMakeLists.txt` (anchor: the `rune_catalog` block — mirror it for `rune_security`, guarded `if(NOT TARGET rune_security)` in the client)
- Test: `rune-codec/tests/security/SecurityEventTest.cpp`

**Interfaces:**
- Consumes: `rune::catalog::{Principal, ReplayedFrom, RequestId}` (`CatalogEvent.h`).
- Produces (namespace `rune::security`) — used by every later task:
  ```cpp
  enum class EventKind : uint16_t { CreateUser = 1, AlterUser = 2, DropUser = 3, CreateRole = 4, DropRole = 5,
                                    Grant = 6, Revoke = 7, GrantRole = 8, RevokeRole = 9, Bootstrap = 10 };
  inline constexpr uint16_t kFormatV1 = 1, kCurrentFormatVer = kFormatV1;
  enum class Operation    : uint8_t { Read = 0, Write = 1, Create = 2, Alter = 3, Drop = 4, Describe = 5, Admin = 6 };
  enum class ResourceType : uint8_t { Cluster = 0, Topic = 1, View = 2, Group = 3, TransactionalId = 4 };
  enum class PatternKind  : uint8_t { Exact = 0, Prefix = 1 };
  enum class GranteeKind  : uint8_t { User = 0, Role = 1 };
  [[nodiscard]] constexpr uint8_t opBit(Operation o) noexcept { return static_cast<uint8_t>(1u << static_cast<uint8_t>(o)); }
  struct ResourcePattern { ResourceType type = ResourceType::Topic; PatternKind kind = PatternKind::Exact;
                           std::string name; auto operator<=>(const ResourcePattern&) const = default; };
  struct GrantSpec { Operation op = Operation::Read; ResourcePattern resource;
                     auto operator<=>(const GrantSpec&) const = default; };
  inline constexpr std::size_t kSaltMin = 16, kSaltMax = 64, kKeyBytes = 64;
  inline constexpr uint32_t    kIterMin = 4096, kIterMax = 1'000'000;
  struct ScramVerifier { std::vector<uint8_t> salt; uint32_t iterations = 0;
                         std::array<uint8_t, kKeyBytes> storedKey{}, serverKey{};
                         bool operator==(const ScramVerifier&) const = default; };   // SHA-512 only in v1
  struct AddPassword { ScramVerifier verifier; };   struct DropPassword { uint8_t index = 1; };   // 1 = oldest (D6)
  struct SetPassword { ScramVerifier verifier; bool terminateSessions = false; };          // D37
  struct DisableUser {}; struct EnableUser {};
  using UserChange = std::variant<AddPassword, DropPassword, SetPassword, DisableUser, EnableUser>;
  struct CreateUserOps { ScramVerifier verifier; };
  struct AlterUserOps  { UserChange change; };
  struct DropUserOps {}; struct CreateRoleOps {}; struct DropRoleOps {};
  struct GrantOps  { GranteeKind grantee = GranteeKind::User; GrantSpec grant; };   // object = the grantee
  struct RevokeOps { GranteeKind grantee = GranteeKind::User; GrantSpec grant; };
  struct GrantRoleOps  { uint64_t roleId = 0; std::string roleName; };               // object = the user
  struct RevokeRoleOps { uint64_t roleId = 0; std::string roleName; };
  struct BootstrapOps  { ScramVerifier verifier; };                                    // D3
  using Ops = std::variant<CreateUserOps, AlterUserOps, DropUserOps, CreateRoleOps, DropRoleOps,
                           GrantOps, RevokeOps, GrantRoleOps, RevokeRoleOps, BootstrapOps>;
  using rune::catalog::Principal; using rune::catalog::ReplayedFrom; using rune::catalog::RequestId;
  struct SecurityEvent {
      uint16_t formatVer = kCurrentFormatVer; EventKind kind = EventKind::CreateUser;
      uint64_t objectId = 0; std::string objectName; uint32_t prevVersion = 0, newVersion = 0;
      std::string statement;          // REDACTED (D10)
      Ops ops; Principal principal; std::optional<ReplayedFrom> replayedFrom;
      int64_t timestampMs = 0; uint64_t fingerprint = 0;
  };
  enum class DecodeError : uint8_t { Truncated, UnknownFormatVer, UnknownKind, KindOpsMismatch, BadEnum, BadVerifier, TrailingBytes };
  enum class EncodeError : uint8_t { KindOpsMismatch, FieldTooLong, BadVerifier };
  [[nodiscard]] EventKind kindOf(const Ops&) noexcept;
  [[nodiscard]] std::expected<void, EncodeError> encodeInto(const SecurityEvent&, std::vector<uint8_t>& out);   // clears, reuses capacity
  [[nodiscard]] std::expected<std::vector<uint8_t>, EncodeError> encode(const SecurityEvent&);
  [[nodiscard]] std::expected<SecurityEvent, DecodeError> decode(std::span<const uint8_t>) noexcept;
  [[nodiscard]] std::string_view kindName(EventKind) noexcept;
  [[nodiscard]] std::string_view decodeErrorName(DecodeError) noexcept;
  [[nodiscard]] std::string_view operationName(Operation) noexcept;      // "READ" … "ADMIN"
  [[nodiscard]] std::string_view resourceTypeName(ResourceType) noexcept; // "CLUSTER" … "TRANSACTIONAL_ID"
  namespace detail_testing { [[nodiscard]] std::size_t opsBodySize(const Ops&); }
  ```

**v1 layout (frozen):** the 36-byte header (`formatVer u16, kind u16, objectId u64, prevVersion u32, newVersion u32, timestampMs i64, fingerprint u64`), then `objectName` (u16 len + bytes), `statement` (u32 len + bytes), the principal exactly as `__catalog` v1 encodes it (`sessionId i64, clientName u16+bytes, sourceAddress u16+bytes, authenticated u16+bytes, requestId hi u64 lo u64, requestFingerprint u64`), `replayedFrom` (u8 0/1 [+ clusterId u16+bytes, offset i64, principal u16+bytes]), then the ops exactly as `__catalog` frames them: `opsLen u32`, then `tag u8` (= the variant index) and the body (the body is the event's tail). Verifier: `saltLen u8 (16..64), salt, iterations u32 (4096..1e6), storedKey[64], serverKey[64]`. Grant: `grantee u8, op u8, type u8, patternKind u8, name u16+bytes`. AlterUser: `changeTag u8` + body (`DropPassword`: `index u8`; `SetPassword`: verifier + `terminate u8 0|1`). GrantRole / RevokeRole: `roleId u64, roleName u16+bytes`. Empty bodies are zero bytes.

- [ ] **Step 1: Write the failing tests** — `rune-codec/tests/security/SecurityEventTest.cpp`:

```cpp
// Authentication spec §3: the frozen formatVer-1 __security event encoding.
#include <rune/security/SecurityEvent.h>
#include <gtest/gtest.h>

namespace rs = rune::security;

namespace {
rs::ScramVerifier verifier(uint8_t seed) {
    rs::ScramVerifier v;
    v.salt.assign(16, seed); v.iterations = 4096;
    v.storedKey.fill(static_cast<uint8_t>(seed + 1)); v.serverKey.fill(static_cast<uint8_t>(seed + 2));
    return v;
}
rs::SecurityEvent baseEvent(rs::Ops ops) {
    rs::SecurityEvent e;
    e.kind = rs::kindOf(ops); e.objectId = 9; e.objectName = "ws1"; e.prevVersion = 0; e.newVersion = 1;
    e.statement = "CREATE USER ws1 WITH PASSWORD '<redacted>';";
    e.ops = std::move(ops);
    e.principal = rs::Principal{.sessionId = 5, .clientName = "rune-cli", .sourceAddress = "10.0.0.5",
                                .authenticated = "admin", .requestId = {.hi = 0xA1, .lo = 0xB2},
                                .requestFingerprint = 0x77};
    e.timestampMs = 1'760'000'000'000; e.fingerprint = 0x1122334455667788ull;
    return e;
}
rs::GrantSpec readPrefix() { return {.op = rs::Operation::Read, .resource = {rs::ResourceType::Topic, rs::PatternKind::Prefix, "world."}}; }
std::vector<rs::Ops> everyKind() {
    return {rs::CreateUserOps{verifier(1)},
            rs::AlterUserOps{rs::AddPassword{verifier(2)}}, rs::AlterUserOps{rs::DropPassword{1}},
            rs::AlterUserOps{rs::SetPassword{verifier(3), true}}, rs::AlterUserOps{rs::DisableUser{}},
            rs::AlterUserOps{rs::EnableUser{}}, rs::DropUserOps{}, rs::CreateRoleOps{}, rs::DropRoleOps{},
            rs::GrantOps{rs::GranteeKind::Role, readPrefix()}, rs::RevokeOps{rs::GranteeKind::User, readPrefix()},
            rs::GrantRoleOps{4, "world_server"}, rs::RevokeRoleOps{4, "world_server"}, rs::BootstrapOps{verifier(9)}};
}
} // namespace

TEST(SecurityEventRegression, EveryKindRoundTripsByteExactly) {
    for (const rs::Ops& ops : everyKind()) {
        rs::SecurityEvent e = baseEvent(ops);
        e.replayedFrom = rs::ReplayedFrom{.clusterId = "a", .offset = 3, .principal = "admin@10.0.0.9"};
        const auto bytes = rs::encode(e).value();
        auto d = rs::decode(bytes);
        ASSERT_TRUE(d.has_value()) << rs::kindName(e.kind) << ": " << rs::decodeErrorName(d.error());
        EXPECT_EQ(rs::encode(*d).value(), bytes) << rs::kindName(e.kind);
        EXPECT_EQ(d->ops.index(), e.ops.index());
        EXPECT_EQ(d->principal.authenticated, "admin");
    }
}

TEST(SecurityEventRegression, HeaderLayoutIsFrozenAndEqualsTheCatalogHeader) {
    const auto b = rs::encode(baseEvent(rs::DropUserOps{})).value();
    ASSERT_GE(b.size(), 36u);
    EXPECT_EQ(b[0], 1); EXPECT_EQ(b[1], 0);   // formatVer 1
    EXPECT_EQ(b[2], 3); EXPECT_EQ(b[3], 0);   // DropUser
    EXPECT_EQ(b[4], 9);                        // objectId low byte
    EXPECT_EQ(b[12], 0); EXPECT_EQ(b[16], 1);  // prev 0, new 1
    EXPECT_EQ(b[28], 0x88); EXPECT_EQ(b[35], 0x11);
}

TEST(SecurityEventRegression, EveryTruncationIsRefusedNeverMisread) {
    for (const rs::Ops& ops : everyKind()) {
        const auto b = rs::encode(baseEvent(ops)).value();
        for (std::size_t n = 0; n < b.size(); ++n)
            ASSERT_EQ(rs::decode(std::span<const uint8_t>(b.data(), n)).error(), rs::DecodeError::Truncated) << n;
    }
}

TEST(SecurityEventRegression, TrailingBytesAreRefused) {
    auto b = rs::encode(baseEvent(rs::CreateRoleOps{})).value();
    b.push_back(0);
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::TrailingBytes);
}

TEST(SecurityEventRegression, UnknownFormatVersionAndKindAreErrorsNotSkips) {
    auto b = rs::encode(baseEvent(rs::DropUserOps{})).value();
    b[0] = 2;
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::UnknownFormatVer);
    b[0] = 1; b[2] = 11;
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::UnknownKind);
}

TEST(SecurityEventRegression, KindDisagreeingWithOpsIsRefused) {
    auto b = rs::encode(baseEvent(rs::DropUserOps{})).value();
    b[2] = 5;   // claims DropRole; the ops tag says DropUser
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::KindOpsMismatch);
}

TEST(SecurityEventRegression, BadEnumBytesAreRefused) {
    const rs::Ops g = rs::GrantOps{rs::GranteeKind::User, readPrefix()};
    auto b = rs::encode(baseEvent(g)).value();
    const std::size_t body = b.size() - rs::detail_testing::opsBodySize(g);
    b[body + 1] = 7;   // operation 7 does not exist
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::BadEnum);
    b[body + 1] = 0; b[body + 2] = 5;   // resource type 5 does not exist
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::BadEnum);
}

TEST(SecurityEventRegression, VerifierBoundsAreEnforcedBothWays) {
    rs::ScramVerifier v = verifier(1);
    v.salt.resize(15);
    EXPECT_EQ(rs::encode(baseEvent(rs::CreateUserOps{v})).error(), rs::EncodeError::BadVerifier);
    v = verifier(1); v.iterations = 4095;
    EXPECT_EQ(rs::encode(baseEvent(rs::CreateUserOps{v})).error(), rs::EncodeError::BadVerifier);
    auto b = rs::encode(baseEvent(rs::CreateUserOps{verifier(1)})).value();
    const std::size_t body = b.size() - rs::detail_testing::opsBodySize(rs::CreateUserOps{verifier(1)});
    b[body] = 70;   // salt length above 64
    EXPECT_EQ(rs::decode(b).error(), rs::DecodeError::BadVerifier);
}

TEST(SecurityEventRegression, EncodeIntoReusesCapacity) {
    std::vector<uint8_t> buf; buf.reserve(4096);
    const auto* data = buf.data();
    ASSERT_TRUE(rs::encodeInto(baseEvent(rs::CreateUserOps{verifier(1)}), buf).has_value());
    EXPECT_EQ(buf.data(), data);
}
```

(`detail_testing::opsBodySize` = the encoded size of the ops body after the tag byte; `body` therefore indexes the first body byte — for a grant `[grantee][op][type]…`.)

- [ ] **Step 2: Add the build targets** (CMake as listed above).
- [ ] **Step 3: Run to verify they fail** — `cmake /Users/vradu/Projects/se-worktrees/rune-codec/build && cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_security_tests` → `'rune/security/SecurityEvent.h' file not found`.
- [ ] **Step 4: Implement** `SecurityEvent.h` (the interfaces above, verbatim) and `SecurityEvent.cpp`: the writer / reader are `src/catalog/Wire.h`'s (`#include "../catalog/Wire.h"`); the principal and replayedFrom are written / read by ONE copy of the code: extract the inline lines of `CatalogEvent.cpp` (encode ~`:252–256`, anchor `w.i64(e.principal.sessionId)`; decode ~`:288–297`, anchor `!r.i64(e.principal.sessionId)`) into `inline void writePrincipalAndReplay(Writer&, const Principal&, const std::optional<ReplayedFrom>&)` and `inline bool readPrincipalAndReplay(Reader&, Principal&, std::optional<ReplayedFrom>&)` in `Wire.h`, called by both encoders — `CatalogEventRegression.*` and `CatalogGoldenRegression.*` must stay green, proving the catalog bytes did not change. Every enum byte is range-checked on decode (`BadEnum`), every verifier field bound-checked (`BadVerifier`) on encode and decode, the ops tag must equal the kind (`KindOpsMismatch`), and the reader must end exactly at the end (`TrailingBytes`).
- [ ] **Step 5: Run** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-codec/build -R 'SecurityEvent|CatalogEvent' --output-on-failure -j14` → PASS (9 security tests + every catalog event test).
- [ ] **Step 6: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-codec add include/rune/security src/security src/catalog tests/security CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-codec commit -m "feat(security): rune_security event model and frozen formatVer-1 encoding (catalog header, shared principal wire)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add CMakeLists.txt && git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "build: link rune_security (authentication Task 2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add CMakeLists.txt && git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "build: link rune_security (authentication Task 2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 3: `rune_security` — the security state, the frozen v1 apply rules, fingerprints, effective permissions

Spec §2 (model, effective permissions = union, allow-only), §3 (the state, at most two verifiers, frozen rules), §6 (live changes: which sessions an event affects), D3, D6, D15–D17, D25. One state type, mutated only by `apply()`; the broker (admin applier + control mirror), the client and the CLI all run these rules.

**Files:**
- Create: `rune-codec/include/rune/security/SecurityState.h`, `src/security/SecurityState.cpp`, `src/security/ApplyV1.h`, `src/security/ApplyV1.cpp`
- Create: `rune-codec/include/rune/security/SecurityFingerprint.h`, `src/security/SecurityFingerprint.cpp`
- Create: `rune-codec/include/rune/security/Permissions.h`, `src/security/Permissions.cpp`
- Test: `rune-codec/tests/security/SecurityApplyV1Test.cpp`, `SecurityFingerprintTest.cpp`, `SecurityPermissionsTest.cpp`

**Interfaces:**
- Produces (namespace `rune::security`):
  ```cpp
  struct UserObject {
      uint64_t objectId = 0; std::string name; uint32_t version = 0;
      bool enabled = true; bool mustChangePassword = false;
      std::vector<ScramVerifier> verifiers;      // 1..2, oldest first (D6)
      std::set<uint64_t> roles;                  // role objectIds
      std::set<GrantSpec> grants;                // direct grants
  };
  struct RoleObject { uint64_t objectId = 0; std::string name; uint32_t version = 0; std::set<GrantSpec> grants; };
  struct Tombstone  { uint64_t objectId = 0; bool wasRole = false; std::string name; uint32_t finalVersion = 0; };
  enum class SessionEffect : uint8_t { None, Refresh, Revoke };          // D25
  struct Applied {
      EventKind kind = EventKind::CreateUser; uint64_t objectId = 0; uint32_t newVersion = 0; uint64_t fingerprint = 0;
      SessionEffect effect = SessionEffect::None;
      std::vector<uint64_t> affectedUsers;       // the users whose sessions the effect applies to (sorted)
  };
  struct RequestRecord { int64_t offset = -1; EventKind kind{}; uint64_t objectId = 0; uint32_t newVersion = 0;
                         uint64_t requestFingerprint = 0; };
  enum class Refusal : uint8_t { InvalidName, UserExists, RoleExists, UnknownUser, UnknownRole, VersionMismatch,
      AlreadyGranted, NotGranted, InvalidGrant, AlreadyMember, NotMember, TooManyPasswords, LastPassword,
      NoSuchPassword, AlreadyDisabled, AlreadyEnabled, AlreadyBootstrapped };
  struct ApplyError {
      enum class Code : uint8_t { UnsupportedFormat, Precondition, FingerprintMismatch, BadOps, OutOfOrder };
      int64_t offset = -1; Code code = Code::Precondition; std::optional<Refusal> refusal;
      uint64_t expected = 0, actual = 0; std::string detail;     // detail: user-facing, never a secret
  };
  class SecurityState {
  public:
      [[nodiscard]] const UserObject* user(std::string_view name) const noexcept;
      [[nodiscard]] const UserObject* userById(uint64_t id) const noexcept;
      [[nodiscard]] const RoleObject* role(std::string_view name) const noexcept;
      [[nodiscard]] const RoleObject* roleById(uint64_t id) const noexcept;
      [[nodiscard]] const std::map<uint64_t, UserObject>& users() const noexcept;
      [[nodiscard]] const std::map<uint64_t, RoleObject>& roles() const noexcept;
      [[nodiscard]] const std::vector<Tombstone>& tombstones() const noexcept;
      [[nodiscard]] std::vector<uint64_t> membersOf(uint64_t roleId) const;   // sorted
      [[nodiscard]] bool hasSecurityAdmin() const noexcept;                    // an ENABLED user effectively holds ADMIN CLUSTER
      [[nodiscard]] int64_t appliedOffset() const noexcept;                    // -1 = nothing applied
      [[nodiscard]] uint64_t nextObjectId() const noexcept;                    // users and roles share one counter
      [[nodiscard]] std::optional<RequestRecord> request(const RequestId&) const;
      /// FNV-1a-64 over appliedOffset, nextObjectId, each live user then role fingerprint in objectId order,
      /// each tombstone in apply order.  Equal digests at equal offsets = the replicas agree.
      [[nodiscard]] uint64_t digest() const noexcept;
  private:
      friend struct ApplyV1;
      std::map<uint64_t, UserObject> users_; std::map<uint64_t, RoleObject> roles_;
      std::map<std::string, uint64_t, std::less<>> userNames_, roleNames_;
      std::vector<Tombstone> tombstones_;
      std::unordered_map<RequestId, RequestRecord, rune::protocol::RequestIdHash> requests_;
      uint64_t nextObjectId_ = 1; int64_t appliedOffset_ = -1;
  };
  [[nodiscard]] std::expected<Applied, ApplyError> apply(SecurityState& s, const SecurityEvent& e, int64_t offset);
  [[nodiscard]] std::expected<uint64_t, ApplyError> previewFingerprint(const SecurityState& s, const SecurityEvent& e);
  [[nodiscard]] std::expected<Applied, ApplyError> previewApplied(const SecurityState& s, const SecurityEvent& e);  // the leader's dry run
  [[nodiscard]] std::string_view applyErrorName(ApplyError::Code) noexcept;
  [[nodiscard]] std::string_view refusalName(Refusal) noexcept;
  [[nodiscard]] bool validName(std::string_view) noexcept;     // [A-Za-z0-9._@-]{1,128}, not "__…" (D7)
  [[nodiscard]] bool validGrant(const GrantSpec&) noexcept;    // D16, D17
  // SecurityFingerprint.h
  [[nodiscard]] uint64_t fingerprintOf(const UserObject&, const SecurityState&) noexcept;   // role NAMES, never ids
  [[nodiscard]] uint64_t fingerprintOf(const RoleObject&) noexcept;
  [[nodiscard]] uint64_t fingerprintOf(const Tombstone&) noexcept;
  // Permissions.h
  [[nodiscard]] bool matches(const ResourcePattern& p, ResourceType type, std::string_view name) noexcept;
  [[nodiscard]] std::vector<GrantSpec> effectiveGrants(const SecurityState&, uint64_t userId);   // direct ∪ roles, sorted, unique
  /// The op bits `grants` give on object (type, name): exact / prefix matches, CLUSTER grants (D16),
  /// and the implied DESCRIBE (D15).  Pure; the broker compiles tables with it, the model checks with its own.
  [[nodiscard]] uint8_t opsOn(std::span<const GrantSpec> grants, ResourceType type, std::string_view name) noexcept;
  [[nodiscard]] uint8_t clusterOps(std::span<const GrantSpec> grants) noexcept;
  ```

**The frozen v1 rules (`ApplyV1.cpp`, never edited after this task; a change ships as `formatVer 2`):**

| Kind | Preconditions (refusal) | Result | Effect (D25) |
|---|---|---|---|
| CreateUser | `validName` (InvalidName); no live user of that name (UserExists); `prevVersion 0`, `objectId == nextObjectId` | user v1, enabled, one verifier | None |
| AlterUser AddPassword | user exists (UnknownUser); < 2 verifiers (TooManyPasswords) | verifier appended | None |
| AlterUser DropPassword i | i ∈ [1, count] (NoSuchPassword); count ≥ 2 (LastPassword) | verifier i removed | None |
| AlterUser SetPassword | user exists | verifiers = {v}, mustChangePassword = false | Revoke if `terminateSessions`, else Refresh |
| AlterUser Disable / Enable | enabled / disabled (AlreadyDisabled / AlreadyEnabled) | flag flipped | Revoke / None |
| DropUser | user exists | tombstone, removed from every role | Revoke |
| CreateRole / DropRole | `validName`; free (RoleExists) / exists (UnknownRole) | role v1 / tombstone; DropRole also removes the role from every member (only the role's version moves; the members' fingerprints change and the digest covers them) | None / Refresh (members) |
| Grant / Revoke | grantee exists; `validGrant` (InvalidGrant); not held (AlreadyGranted) / held (NotGranted) | grant added / removed on the grantee | Refresh (the user, or every member of the role) |
| GrantRole / RevokeRole | user + role exist; not a member (AlreadyMember) / a member (NotMember); `roleName` equals the role's name | membership added / removed | Refresh (the user) |
| Bootstrap | `!hasSecurityAdmin()` (AlreadyBootstrapped); `validName`; name free | user v1, mustChangePassword = true, grants {ADMIN CLUSTER} | None |

Every event: `prevVersion` equals the object's current version (0 for creates; VersionMismatch otherwise) and `newVersion == prevVersion + 1`; the fingerprint of the object's resulting state must equal `e.fingerprint` (FingerprintMismatch, the state unchanged — atomic); `offset > appliedOffset()` (OutOfOrder); `formatVer == 1` (UnsupportedFormat); a request id seen before with another `requestFingerprint` is not a rule (the leader answers it, Task 10). A DropRole's members' fingerprints change (role names are part of a user's fingerprint) without their versions changing — the digest covers it; `Applied.affectedUsers` lists them.

**Fingerprints (frozen field order, FNV-1a-64 through `rune::catalog::detail::fnvU64/fnvBytes`):**
- user: `'U'`, name, version, enabled, mustChangePassword, verifier count, each verifier (salt bytes, iterations, storedKey, serverKey), role count + each role **name** (sorted), grant count + each grant (op, type, kind, name) in `std::set` order;
- role: `'R'`, name, version, grant count + each grant;
- tombstone: `'D'`, wasRole, name, finalVersion.

- [ ] **Step 1: Write the failing tests** — `SecurityApplyV1Test.cpp` (the helper `Script` builds a valid event against the current state — it computes `objectId`, versions and the fingerprint with `previewFingerprint`, exactly as the leader will):

```cpp
// Authentication spec §2/§3/§6: the frozen formatVer-1 rules of __security.
#include <rune/security/Permissions.h>
#include <rune/security/SecurityState.h>
#include <gtest/gtest.h>

namespace rs = rune::security;
namespace {
rs::ScramVerifier v(uint8_t seed) {
    rs::ScramVerifier x; x.salt.assign(16, seed); x.iterations = 4096;
    x.storedKey.fill(seed); x.serverKey.fill(static_cast<uint8_t>(seed + 1)); return x;
}
struct Script {
    rs::SecurityState s; int64_t offset = 0;
    rs::SecurityEvent make(rs::Ops ops, std::string name) {
        rs::SecurityEvent e; e.kind = rs::kindOf(ops); e.objectName = std::move(name); e.ops = std::move(ops);
        const rs::UserObject* u = s.user(e.objectName); const rs::RoleObject* r = s.role(e.objectName);
        const bool roleTarget = e.kind == rs::EventKind::CreateRole || e.kind == rs::EventKind::DropRole ||
            ((e.kind == rs::EventKind::Grant || e.kind == rs::EventKind::Revoke) &&
             std::visit([](const auto& o) { if constexpr (requires { o.grantee; }) return o.grantee == rs::GranteeKind::Role; else return false; }, e.ops));
        const bool create = e.kind == rs::EventKind::CreateUser || e.kind == rs::EventKind::CreateRole || e.kind == rs::EventKind::Bootstrap;
        e.objectId = create ? s.nextObjectId() : (roleTarget ? (r ? r->objectId : 0) : (u ? u->objectId : 0));
        e.prevVersion = create ? 0 : (roleTarget ? (r ? r->version : 0) : (u ? u->version : 0));
        e.newVersion = e.prevVersion + 1;
        if (auto f = rs::previewFingerprint(s, e)) e.fingerprint = *f;
        return e;
    }
    std::expected<rs::Applied, rs::ApplyError> run(rs::Ops ops, std::string name) { return rs::apply(s, make(std::move(ops), std::move(name)), offset++); }
};
rs::GrantSpec g(rs::Operation op, rs::ResourceType t, rs::PatternKind k, std::string n) { return {op, {t, k, std::move(n)}}; }
} // namespace

TEST(SecurityApplyV1Regression, CreateUserThenTwoPasswordsAtMost) {
    Script sc;
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(1)}, "ws1").has_value());
    ASSERT_TRUE(sc.run(rs::AlterUserOps{rs::AddPassword{v(2)}}, "ws1").has_value());
    auto third = sc.run(rs::AlterUserOps{rs::AddPassword{v(3)}}, "ws1");
    ASSERT_FALSE(third.has_value());
    EXPECT_EQ(third.error().refusal, rs::Refusal::TooManyPasswords);
    EXPECT_EQ(sc.s.user("ws1")->verifiers.size(), 2u);
    EXPECT_EQ(sc.s.user("ws1")->version, 2u) << "a refused event changes nothing";
}

TEST(SecurityApplyV1Regression, DropPasswordNumbersByAgeAndNeverDropsTheLast) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "ws1"); sc.run(rs::AlterUserOps{rs::AddPassword{v(2)}}, "ws1");
    ASSERT_TRUE(sc.run(rs::AlterUserOps{rs::DropPassword{1}}, "ws1").has_value());
    EXPECT_EQ(sc.s.user("ws1")->verifiers.front().salt.front(), 2) << "1 = the oldest (D6)";
    auto last = sc.run(rs::AlterUserOps{rs::DropPassword{1}}, "ws1");
    EXPECT_EQ(last.error().refusal, rs::Refusal::LastPassword);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::DropPassword{2}}, "ws1").error().refusal, rs::Refusal::NoSuchPassword);
}

TEST(SecurityApplyV1Regression, SessionEffectPerKindIsTheDecisionTable) {
    Script sc;
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "ws1")->effect, rs::SessionEffect::None);
    EXPECT_EQ(sc.run(rs::CreateRoleOps{}, "world")->effect, rs::SessionEffect::None);
    const auto& ws1 = *sc.s.user("ws1");
    auto gr = sc.run(rs::GrantRoleOps{sc.s.role("world")->objectId, "world"}, "ws1");
    EXPECT_EQ(gr->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(gr->affectedUsers, std::vector<uint64_t>{ws1.objectId});
    auto grant = sc.run(rs::GrantOps{rs::GranteeKind::Role, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "world.")}, "world");
    EXPECT_EQ(grant->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(grant->affectedUsers, std::vector<uint64_t>{ws1.objectId}) << "a role grant refreshes every member";
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::SetPassword{v(4), false}}, "ws1")->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::SetPassword{v(5), true}}, "ws1")->effect, rs::SessionEffect::Revoke);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::DisableUser{}}, "ws1")->effect, rs::SessionEffect::Revoke);
    EXPECT_EQ(sc.run(rs::AlterUserOps{rs::EnableUser{}}, "ws1")->effect, rs::SessionEffect::None);
    auto dropRole = sc.run(rs::DropRoleOps{}, "world");
    EXPECT_EQ(dropRole->effect, rs::SessionEffect::Refresh);
    EXPECT_TRUE(sc.s.user("ws1")->roles.empty()) << "membership removed with the role";
    EXPECT_EQ(sc.run(rs::DropUserOps{}, "ws1")->effect, rs::SessionEffect::Revoke);
}

TEST(SecurityApplyV1Regression, InvalidGrantsAreRefused) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "u");
    const rs::GrantSpec bad[] = {
        g(rs::Operation::Read,  rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),            // D16
        g(rs::Operation::Write, rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),
        g(rs::Operation::Admin, rs::ResourceType::Topic,   rs::PatternKind::Exact, "t"),           // D17
        g(rs::Operation::Write, rs::ResourceType::View,    rs::PatternKind::Exact, "v"),
        g(rs::Operation::Write, rs::ResourceType::Group,   rs::PatternKind::Exact, "g"),
        g(rs::Operation::Read,  rs::ResourceType::TransactionalId, rs::PatternKind::Exact, "x"),
        g(rs::Operation::Admin, rs::ResourceType::Cluster, rs::PatternKind::Prefix, ""),           // CLUSTER is Exact ""
        g(rs::Operation::Read,  rs::ResourceType::Topic,   rs::PatternKind::Exact, ""),            // empty exact name
    };
    for (const auto& b : bad)
        EXPECT_EQ(sc.run(rs::GrantOps{rs::GranteeKind::User, b}, "u").error().refusal, rs::Refusal::InvalidGrant)
            << rs::operationName(b.op) << " " << rs::resourceTypeName(b.resource.type);
    EXPECT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::User, g(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "")}, "u").has_value())
        << "PREFIX '' = every topic";
}

TEST(SecurityApplyV1Regression, BootstrapOnlyWhileNoEnabledAdminExists) {
    Script sc;
    auto b = sc.run(rs::BootstrapOps{v(1)}, "admin");
    ASSERT_TRUE(b.has_value());
    EXPECT_TRUE(sc.s.user("admin")->mustChangePassword);
    EXPECT_TRUE(sc.s.hasSecurityAdmin());
    EXPECT_EQ(sc.run(rs::BootstrapOps{v(2)}, "admin2").error().refusal, rs::Refusal::AlreadyBootstrapped);
    sc.run(rs::AlterUserOps{rs::DisableUser{}}, "admin");
    EXPECT_FALSE(sc.s.hasSecurityAdmin()) << "a disabled admin does not count";
    EXPECT_TRUE(sc.run(rs::BootstrapOps{v(3)}, "admin2").has_value());
}

TEST(SecurityApplyV1Regression, FingerprintMismatchIsRefusedAndLeavesTheStateUntouched) {
    Script sc;
    auto e = sc.make(rs::CreateUserOps{v(1)}, "ws1");
    e.fingerprint ^= 1;
    const uint64_t before = sc.s.digest();
    auto r = rs::apply(sc.s, e, 0);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, rs::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(sc.s.digest(), before);
    EXPECT_EQ(sc.s.user("ws1"), nullptr);
}

TEST(SecurityApplyV1Regression, VersionsAndOffsetsAreChecked) {
    Script sc; sc.run(rs::CreateUserOps{v(1)}, "ws1");
    auto e = sc.make(rs::AlterUserOps{rs::DisableUser{}}, "ws1");
    e.prevVersion = 7; e.newVersion = 8;
    EXPECT_EQ(rs::apply(sc.s, e, 5).error().refusal, rs::Refusal::VersionMismatch);
    auto ok = sc.make(rs::AlterUserOps{rs::DisableUser{}}, "ws1");
    EXPECT_EQ(rs::apply(sc.s, ok, 0).error().code, rs::ApplyError::Code::OutOfOrder) << "offset 0 was applied";
}

TEST(SecurityApplyV1Regression, NamesAreValidatedAndNamespacesAreSeparate) {
    Script sc;
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "__x").error().refusal, rs::Refusal::InvalidName);
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "a,b").error().refusal, rs::Refusal::InvalidName);
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, std::string(129, 'a')).error().refusal, rs::Refusal::InvalidName);
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(1)}, "ops").has_value());
    EXPECT_TRUE(sc.run(rs::CreateRoleOps{}, "ops").has_value()) << "users and roles are separate namespaces";
    EXPECT_EQ(sc.run(rs::CreateUserOps{v(1)}, "ops").error().refusal, rs::Refusal::UserExists);
}

TEST(SecurityApplyV1Regression, ReusedNameGetsANewObjectId) {
    Script sc;
    sc.run(rs::CreateUserOps{v(1)}, "ws1");
    const uint64_t first = sc.s.user("ws1")->objectId;
    sc.run(rs::DropUserOps{}, "ws1");
    sc.run(rs::CreateUserOps{v(1)}, "ws1");
    EXPECT_NE(sc.s.user("ws1")->objectId, first);
    EXPECT_EQ(sc.s.tombstones().size(), 1u);
}
```

`SecurityPermissionsTest.cpp`:

```cpp
#include <rune/security/Permissions.h>
#include <gtest/gtest.h>
namespace rs = rune::security;
using rs::Operation; using rs::ResourceType; using rs::PatternKind;
namespace { rs::GrantSpec g(Operation o, ResourceType t, PatternKind k, std::string n) { return {o, {t, k, std::move(n)}}; } }

TEST(SecurityPermissionsRegression, ExactPrefixAndEmptyPrefixMatch) {
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Exact, "a.b"}, ResourceType::Topic, "a.b"));
    EXPECT_FALSE(rs::matches({ResourceType::Topic, PatternKind::Exact, "a.b"}, ResourceType::Topic, "a.bc"));
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Prefix, "world."}, ResourceType::Topic, "world.zone1"));
    EXPECT_FALSE(rs::matches({ResourceType::Topic, PatternKind::Prefix, "world."}, ResourceType::Topic, "world"));
    EXPECT_TRUE(rs::matches({ResourceType::Topic, PatternKind::Prefix, ""}, ResourceType::Topic, "anything"));
    EXPECT_FALSE(rs::matches({ResourceType::Topic, PatternKind::Prefix, ""}, ResourceType::View, "anything")) << "types never cross";
}

TEST(SecurityPermissionsRegression, OpsOnCombinesExactPrefixClusterAndImpliedDescribe) {
    const std::vector<rs::GrantSpec> grants = {
        g(Operation::Read,  ResourceType::Topic, PatternKind::Exact, "player_events"),
        g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world."),
        g(Operation::Create, ResourceType::Cluster, PatternKind::Exact, ""),
    };
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "player_events"),
              rs::opBit(Operation::Read) | rs::opBit(Operation::Describe) | rs::opBit(Operation::Create));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "world.z"),
              rs::opBit(Operation::Write) | rs::opBit(Operation::Describe) | rs::opBit(Operation::Create));
    EXPECT_EQ(rs::opsOn(grants, ResourceType::View, "v"), rs::opBit(Operation::Create) | rs::opBit(Operation::Describe))
        << "CREATE ON CLUSTER covers views too (D16) and implies DESCRIBE (D15)";
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Group, "g"), 0) << "CREATE CLUSTER says nothing about groups";
    EXPECT_EQ(rs::clusterOps(grants), rs::opBit(Operation::Create));
}

TEST(SecurityPermissionsRegression, AdminClusterGivesNoDataAccess) {
    const std::vector<rs::GrantSpec> grants = {g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")};
    EXPECT_EQ(rs::opsOn(grants, ResourceType::Topic, "t"), 0) << "D16";
    EXPECT_EQ(rs::clusterOps(grants), rs::opBit(Operation::Admin));
}

TEST(SecurityPermissionsRegression, EffectiveGrantsAreTheUnionOfDirectAndRoles) {
    // built through apply() in the fixture of SecurityApplyV1Test (Script) — direct READ t1, role R with WRITE t2,
    // role S with READ t1 again: effectiveGrants = {READ t1, WRITE t2}, sorted, unique.
}
```

(Write `EffectiveGrantsAreTheUnionOfDirectAndRoles` fully with the `Script` helper — move `Script` into `tests/security/SecurityScript.h` so both test files share it.)

`SecurityFingerprintTest.cpp`: `SecurityFingerprintRegression.IdsAreNeverHashed` (two states building the same users / roles in different orders — different objectIds — give equal per-object fingerprints and equal role-name lists), `…EveryUserFieldChangesTheFingerprint` (name, version, enabled, mustChangePassword, each verifier byte class, a role name, a grant field — each alone flips it), `…DigestEqualsAtEqualOffsetsAndDiffersAfterAnyEvent`.

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 --target rune_security_tests` → `'rune/security/SecurityState.h' file not found`.
- [ ] **Step 3: Implement** `SecurityState.cpp` (accessors, `digest`, `membersOf`, `hasSecurityAdmin` = any enabled user whose `clusterOps(effectiveGrants)` has `opBit(Admin)`), `ApplyV1.cpp` (the table above; `apply` = `UnsupportedFormat` / `OutOfOrder` checks → copy-free validation → mutate a **scratch copy of the touched objects only** → fingerprint check → commit, so a refusal never leaves a half-applied state; records the `RequestRecord` when `principal.requestId` is not none), `SecurityFingerprint.cpp`, `Permissions.cpp` (`matches`: same type, Exact → equality, Prefix → `name.starts_with(p.name)`; `opsOn`: OR of `opBit(op)` over matching grants, OR CLUSTER's CREATE / ALTER / DROP / DESCRIBE for Topic / View and DESCRIBE for Group / TransactionalId, then `| opBit(Describe)` when non-zero; `validGrant`: D16 / D17, CLUSTER must be `Exact ""`, Exact names non-empty).
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-codec/build -R 'Security' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-codec add include/rune/security src/security tests/security
git -C /Users/vradu/Projects/se-worktrees/rune-codec commit -m "feat(security): security state, frozen v1 apply rules, fingerprints, effective permissions

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `rune_security` — verifying replayer, history / as-of / grants-for, `.rsec` files, golden logs, replay bench

Spec §3 ("History reads … use the shared replay library and the same commands as the catalog"), §11 ("Replay: golden `__security` logs, with fingerprints verified"), D38.

**Files:**
- Create: `rune-codec/include/rune/catalog/RecordFile.h`, `src/catalog/RecordFile.cpp`; modify `src/catalog/CatalogFile.cpp` (the `.rcat` functions delegate with magic `RCAT`; bytes unchanged)
- Create: `rune-codec/include/rune/security/{SecurityReplayer,SecurityQueries,SecurityFile}.h`, `src/security/{SecurityReplayer,SecurityQueries,SecurityFile}.cpp`
- Create: `rune-codec/include/rune/security/testing/SecurityGoldenScript.h` (+ `src/security/SecurityGoldenScript.cpp`), `tools/make_security_golden.cpp`, `tests/security/golden/v1/basic.rsec` (generated in Step 4), `tests/security/SecurityGoldenPins.h`
- Create: `rune-codec/tests/security/{SecurityReplayerTest,SecurityQueriesTest,SecurityGoldenTest}.cpp`, `rune-codec/benchmark/SecurityReplayBench.cpp`
- Modify: `rune-codec/CMakeLists.txt` (`make_security_golden` tool, `RUNE_SECURITY_GOLDEN_DIR` compile definition on `rune_security_tests`, the bench into `rune_codec_bench`)

**Interfaces:**
- Produces (namespace `rune::catalog`, `RecordFile.h`): `void writeRecordFileHeader(std::ostream&, const char (&magic)[4]);` `void writeRecordFileRecord(std::ostream&, int64_t offset, std::span<const uint8_t>);` `std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRecordFile(std::istream&, const char (&magic)[4]);` (version 2 layout of `CatalogFile.h`, CRC32C per record).
- Produces (namespace `rune::security`):
  ```cpp
  struct ReplayError { enum class Kind : uint8_t { Decode, Apply, Gap } kind; int64_t offset = -1;
                       std::optional<DecodeError> decode; std::optional<ApplyError> apply; };
  [[nodiscard]] std::string describe(const ReplayError&);
  class SecurityReplayer {
  public:
      explicit SecurityReplayer(SecurityState start = {});
      [[nodiscard]] std::expected<Applied, ReplayError> feed(int64_t offset, std::span<const uint8_t> record);
      [[nodiscard]] std::expected<Applied, ReplayError> feedDecoded(int64_t offset, const SecurityEvent& e);
      [[nodiscard]] const SecurityState& state() const noexcept;
      [[nodiscard]] int64_t nextOffset() const noexcept;
  };
  struct RecordedEvent { int64_t offset = -1; SecurityEvent event; };
  struct HistoryFilter { std::optional<GranteeKind> kind; std::string name; std::optional<int64_t> sinceMs; };
  struct AsOf { enum class By : uint8_t { Offset, Time } by = By::Offset; int64_t value = 0; };
  struct GrantsView { std::vector<GrantSpec> direct; std::vector<std::pair<std::string, std::vector<GrantSpec>>> viaRoles;
                      std::vector<GrantSpec> effective; };
  struct UserRow { std::string name; bool enabled; bool mustChangePassword; uint8_t passwords; std::vector<std::string> roles; uint32_t version; };
  struct RoleRow { std::string name; std::vector<std::string> members; std::size_t grants; uint32_t version; };
  [[nodiscard]] std::vector<RecordedEvent> history(std::span<const RecordedEvent> log, const HistoryFilter&);
  [[nodiscard]] std::expected<SecurityState, ReplayError> stateAsOf(std::span<const RecordedEvent> log, const AsOf&);
  [[nodiscard]] std::optional<GrantsView> grantsFor(const SecurityState&, GranteeKind, std::string_view name);
  [[nodiscard]] std::vector<UserRow> users(const SecurityState&);
  [[nodiscard]] std::vector<RoleRow> roles(const SecurityState&);
  inline constexpr char kRsecMagic[4] = {'R', 'S', 'E', 'C'};
  void writeRsecHeader(std::ostream&); void writeRsecRecord(std::ostream&, int64_t, std::span<const uint8_t>);
  [[nodiscard]] std::expected<std::vector<std::pair<int64_t, std::vector<uint8_t>>>, std::string> readRsec(std::istream&);
  namespace testing { [[nodiscard]] std::vector<SecurityEvent> goldenScriptV1(); }   // fixed, fingerprinted, timestamped
  ```

- [ ] **Step 1: Write the failing tests:**
  - `SecurityReplayerTest.cpp`: `SecurityReplayerRegression.ReplaysTheGoldenScriptVerifyingEveryFingerprint`, `…AGapIsAnErrorNamingTheOffset` (feed offset 2 after 0 → `Kind::Gap`, offset 2), `…UnknownFormatVersionStopsAtItsOffset` (byte 0 flipped to 2 → `Kind::Decode` at that offset, state = the prefix), `…TamperedFingerprintStopsAtItsOffsetRegression`, `…ResumesFromACachedPrefix` (replay [0, 5), copy the state, a new replayer from it accepts offset 5 only).
  - `SecurityQueriesTest.cpp`: `SecurityQueriesRegression.HistoryFiltersByKindNameAndTime`, `…AsOfOffsetAndTimeBoundariesAreInclusive` (the event AT the boundary is included — the catalog's as-of boundary rule), `…GrantsForSplitsDirectRolesAndEffective` (a user with READ t1 direct, role R (WRITE t2), role S (READ t1): direct {READ t1}, viaRoles {R: {WRITE t2}, S: {READ t1}}, effective {READ t1, WRITE t2}), `…UsersAndRolesRowsNeverCarryVerifierBytes` (static: `UserRow` has no verifier field — compile-time `static_assert(!requires(UserRow r){ r.verifiers; })`; plus `passwords` counts them).
  - `SecurityGoldenTest.cpp`: `SecurityGoldenRegression.V1LogsReplayToPinnedFingerprints`, `…TheGoldenScriptStillProducesTheCheckedInBytes`, `…TheGoldenLogDecodesToTheScriptEventByEvent` (the three `CatalogGoldenTest.cpp` tests with `readRsec` / `SecurityReplayer` / `kBasicV1Digests` from `SecurityGoldenPins.h`), and `RecordFileRegression.RcatBytesAreUnchangedByTheGeneralization` (re-encode `golden/v1/basic.rcat` through `writeRecordFile*` with `RCAT` → byte-identical to the checked-in file).
  - The golden script (`goldenScriptV1`, 16 events, every kind at least once, offsets 0..15): Bootstrap `admin`; CreateRole `world_server`; Grant READ TOPIC `player_events` → role; Grant WRITE TOPIC PREFIX `world.` → role; Grant READ GROUP PREFIX `world-` → role; CreateUser `ws1`; GrantRole `world_server` → `ws1`; AlterUser `ws1` AddPassword; AlterUser `ws1` DropPassword 1; Revoke WRITE PREFIX `world.` ← role; AlterUser `admin` SetPassword (no terminate); AlterUser `ws1` Disable; AlterUser `ws1` Enable; RevokeRole `world_server` ← `ws1`; DropRole `world_server`; DropUser `ws1`. Fixed salts / keys (`0x10 + offset`), `timestampMs = 1'760'000'000'000 + offset * 1000`, `principal.authenticated = "admin"` (`"bootstrap"` at offset 0 — D3), statements redacted.
- [ ] **Step 2: Run to verify they fail** (missing headers).
- [ ] **Step 3: Implement** RecordFile (move the `.rcat` reader / writer bodies there, parameterized by magic), the replayer (decode → `apply` → `ReplayError` naming the offset; `feed` refuses `offset != nextOffset()` with `Gap`), queries (pure over `RecordedEvent` spans; `stateAsOf` replays through a `SecurityReplayer`), `.rsec`, the golden script.
- [ ] **Step 4: Generate the golden log and pins** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build --target make_security_golden && /Users/vradu/Projects/se-worktrees/rune-codec/build/make_security_golden /Users/vradu/Projects/se-worktrees/rune-codec/tests/security/golden/v1/basic.rsec` — it writes the file and prints `kBasicV1Digests` (one `digest()` per offset); paste them into `SecurityGoldenPins.h` (frozen from here on: a later failure means a v1 rule changed).
- [ ] **Step 5: Bench** — `SecurityReplayBench.cpp`: `BM_SecurityReplay10kEvents` (10 000 events: 1 000 users, 200 roles, grants and role changes) through `SecurityReplayer` in `build-rel`; record the median (no gate — the boot replay gate of the catalog, < 1 s for 10k, is asserted for `__security` in Task 9's `SecurityApplierRegression.BootReplayOf10kEventsUnderOneSecond`).
- [ ] **Step 6: Run** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-codec/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-codec/build -R 'Security|Catalog|RecordFile' --output-on-failure -j14` → PASS; `/Users/vradu/Projects/se-worktrees/rune-codec/build-rel/rune_codec_bench --benchmark_filter=SecurityReplay`.
- [ ] **Step 7: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-codec add include src tests tools benchmark CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-codec commit -m "feat(security): verifying replayer, history/as-of/grants-for, .rsec (generalized record file), v1 golden log, replay bench

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (controller):** full `*Regression*` in rune-crypto and rune-codec (plus ASan/UBSan, TSan builds of both).

---
### Task 5: Protocol — codes 44–47, client version 9, auth templates 12–15, `SecurityDdl` 79 / `SecurityBootstrap` 80, ConnectResponse v9, `kSecurityTopicId`, internal ring op types 21 / 22

Spec §5 (new authentication messages, version gate), §10 (codes), D5, D11, D26, D30, D34, D36.

**Files:**
- Modify: `rune-protocol/include/rune/protocol/client/Protocol.h` (codes, templates, versions, `kSecurityTopicId`, `clientUnderstandsAuth`), `ControlMessages.h` (ConnectResponse v9), `Dispatch.h` (`isKnownTemplate`: 1–15, 16–21, 32–54, 64–80)
- Create: `rune-protocol/include/rune/protocol/client/AuthMessages.h`, `rune-protocol/include/rune/protocol/client/SecurityMessages.h`
- Modify: `rune-protocol/schema/rune-internal-ring.xml` and the checked-in generated `include/rune_protocol_internal_sbe/OpType.h`, `TailKind.h` (the precedent: commit `7b1143d` "WorkerOp TOPIC_DROPPED (20)")
- Test: `rune-protocol/tests/protocol/client/AuthMessagesTest.cpp`, `SecurityMessagesTest.cpp`; modify `ControlMessagesConnectTest.cpp`, `DispatchTest.cpp` (the "unknown" pins move to 81), `ConformanceTest.cpp` (vectors for 12–15, 79, 80)

**Interfaces (namespace `rune::protocol::client`):**
```cpp
// Protocol.h
//   ErrorCode: AuthenticationFailed = 44, NotAuthorized = 45, SessionRevoked = 46, PasswordChangeRequired = 47
//   TemplateId: AuthClientFirst = 12, AuthServerFirst = 13, AuthClientFinal = 14, AuthServerFinal = 15,
//               SecurityDdl = 79, SecurityBootstrap = 80
inline constexpr int32_t kClientVersionAuth = 9;   inline constexpr int32_t kClientVersion = 9;
inline constexpr int32_t kBrokerVersionAuth = 5;   inline constexpr int32_t kBrokerVersion = 5;
inline constexpr int32_t kSecurityTopicId   = -7;
enum class AuthMode  : uint8_t { Required = 0, DevDisabled = 1, InProcess = 2 };
enum class AuthState : uint8_t { MustAuthenticate = 0, ResumedAuthenticated = 1, Anonymous = 2 };
[[nodiscard]] constexpr bool clientUnderstandsAuth(int32_t v) noexcept { return v >= kClientVersionAuth; }

// ControlMessages.h — ConnectResponse
//   BODY_FIXED_V9 = 21: [18] chosenCipher (0 = none) [19] authMode [20] authState, then handshakeBytes (var32)
//   at fixedBase + blockLength.  encode(): V9 iff `v9` is set (the broker sets it for clientVersion >= 9);
//   decode(): blockLength >= 21 → authMode/authState read; var bytes always at fixedBase + blockLength.
//   A pre-v9 frame (no auth bytes) decodes as authMode = InProcess, authState = Anonymous: a pre-v9
//   broker has no authentication.
struct ConnectResponse { /* existing fields */ bool v9 = false; AuthMode authMode = AuthMode::InProcess;
                         AuthState authState = AuthState::Anonymous; static constexpr uint16_t BODY_FIXED_V9 = 21; };

// AuthMessages.h — every frame carries the session id + correlation id in the MessageHeader.
inline constexpr uint8_t kMechanismScramSha512 = 1;
inline constexpr std::size_t kScramProofBytes = 64;
struct AuthClientFirst  { int64_t sessionId = 0, correlationId = 0; uint8_t mechanism = kMechanismScramSha512;
                          std::string_view username; std::string_view clientNonce; /* encode / decode */ };
struct ScramSaltWire    { std::string_view salt; uint32_t iterations = 0; };
struct AuthServerFirst  { int64_t sessionId = 0, correlationId = 0; std::string_view combinedNonce;
                          uint8_t count = 1; std::array<ScramSaltWire, 2> salts{}; };         // count 1..2 (D5)
struct AuthClientFinal  { int64_t sessionId = 0, correlationId = 0; uint8_t count = 1;
                          std::array<std::array<uint8_t, kScramProofBytes>, 2> proofs{}; };
inline constexpr uint8_t kAuthFlagMustChangePassword = 0x01;
struct AuthServerFinal  { int64_t sessionId = 0, correlationId = 0; uint16_t errorCode = 0; uint8_t flags = 0;
                          uint32_t retryAfterMs = 0; std::string_view serverSignature; std::string_view detail; };
// Layouts: AuthClientFirst [mechanism u8][username var16][clientNonce var16];
//          AuthServerFirst [nonce var16][count u8][count × (salt var16, iterations u32)];
//          AuthClientFinal [count u8][count × 64 proof bytes];
//          AuthServerFinal [errorCode u16][flags u8][retryAfterMs u32][serverSignature var16][detail var16].
// Bounds on decode: username 1..128, nonce 1..128, salt 16..64, count 1..2, iterations 4096..1e6,
// signature 0 or 64 — anything else is nullopt (the gateway answers InvalidRequest / AuthenticationFailed).

// SecurityMessages.h
enum class SecurityOpKind : uint8_t { CreateUser = 1, AlterUser = 2, DropUser = 3, CreateRole = 4, DropRole = 5,
                                      Grant = 6, Revoke = 7, GrantRole = 8, RevokeRole = 9 };
enum class UserChangeKind : uint8_t { AddPassword = 0, DropPassword = 1, SetPassword = 2, Disable = 3, Enable = 4 };
struct VerifierWire { std::string_view salt; uint32_t iterations = 0; std::string_view storedKey, serverKey; };  // 64 + 64
struct SecurityDdl {
    int64_t sessionId = 0, correlationId = 0;
    DdlExtension ext;                     // statement (REDACTED), clientName, requestId, replayedFrom
    SecurityOpKind kind = SecurityOpKind::CreateUser;
    std::string_view name;                // the user / role the op targets (Grant/Revoke: the grantee)
    UserChangeKind change = UserChangeKind::AddPassword;  // AlterUser only
    uint8_t passwordIndex = 0;            // DropPassword
    uint8_t terminateSessions = 0;        // SetPassword
    VerifierWire verifier;                // CreateUser, AddPassword, SetPassword
    uint8_t granteeKind = 0, op = 0, resourceType = 0, patternKind = 0;   // Grant / Revoke
    std::string_view pattern;             // Grant / Revoke
    std::string_view roleName;            // GrantRole / RevokeRole
};
// Layout: header, ext (catalog DdlExtension layout), [kind u8][name var16] then the kind's fields in the order
// above; answered by DdlResponse (78).
struct SecurityBootstrap {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view adminName; VerifierWire verifier;
    std::array<uint8_t, 16> nonce{}; std::array<uint8_t, 64> mac{};   // D4
};
/// The bytes the bootstrap MAC covers (D4): "rune-bootstrap-v1" | adminName var16 | salt var16 | iterations u32 |
/// storedKey | serverKey | nonce.  ONE definition (CLI + gateway).
void bootstrapMacInput(const SecurityBootstrap&, std::vector<uint8_t>& out);
```

**Internal ring schema:** `OpType` gains `SESSION_PERMISSIONS = 21` (control → every worker, session-lifecycle ring: `sessionId`, `nextOffset` = first word index, `hwBound` = total words, `isolation` = flags `{bit0 last chunk, bit1 grantAll}`, tail = `PERMISSION_BITS` words, LE u64) and `PEER_LINK_KEYS = 22` (control → every worker: `topicId` = peer node id, `partition` = link epoch, tail = `PEER_KEYS` = `[sendMaster 32][recvMaster 32]`); `TailKind` gains `PERMISSION_BITS = 8`, `PEER_KEYS = 9`. Both generated headers are edited exactly like `7b1143d` (validValue + `get()` case + `c_str()` case).

- [ ] **Step 1: Write the failing tests** — `AuthMessagesTest.cpp`:

```cpp
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/Dispatch.h>
#include <gtest/gtest.h>
namespace pc = rune::protocol::client;

TEST(AuthMessagesRegression, ErrorCodesAndTemplatesArePinned) {
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed), 44);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::NotAuthorized), 45);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::SessionRevoked), 46);
    EXPECT_EQ(static_cast<uint16_t>(pc::ErrorCode::PasswordChangeRequired), 47);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AuthClientFirst), 12);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), 15);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::SecurityDdl), 79);
    EXPECT_EQ(static_cast<uint16_t>(pc::TemplateId::SecurityBootstrap), 80);
    EXPECT_EQ(pc::kClientVersion, 9); EXPECT_EQ(pc::kBrokerVersion, 5); EXPECT_EQ(pc::kSecurityTopicId, -7);
    for (uint16_t t : {12, 13, 14, 15, 79, 80}) EXPECT_TRUE(pc::isKnownTemplate(t)) << t;
    EXPECT_FALSE(pc::isKnownTemplate(81));
}

TEST(AuthMessagesRegression, EveryAuthFrameRoundTrips) {
    uint8_t buf[1024];
    pc::AuthClientFirst cf{.sessionId = 7, .correlationId = 9, .username = "ws1", .clientNonce = "abcDEF123+/="};
    auto n = cf.encode(buf); auto d = pc::AuthClientFirst::decode(buf, n);
    ASSERT_TRUE(d); EXPECT_EQ(d->username, "ws1"); EXPECT_EQ(d->clientNonce, "abcDEF123+/=");

    const std::string s1(16, 's'), s2(32, 't');
    pc::AuthServerFirst sf{.sessionId = 7, .correlationId = 9, .combinedNonce = "abc+srv", .count = 2,
                           .salts = {pc::ScramSaltWire{s1, 4096}, pc::ScramSaltWire{s2, 8192}}};
    n = sf.encode(buf); auto ds = pc::AuthServerFirst::decode(buf, n);
    ASSERT_TRUE(ds); EXPECT_EQ(ds->count, 2); EXPECT_EQ(ds->salts[1].iterations, 8192u); EXPECT_EQ(ds->salts[1].salt, s2);

    pc::AuthClientFinal fin{.sessionId = 7, .correlationId = 9, .count = 2};
    fin.proofs[0].fill(1); fin.proofs[1].fill(2);
    n = fin.encode(buf); auto df = pc::AuthClientFinal::decode(buf, n);
    ASSERT_TRUE(df); EXPECT_EQ(df->proofs[1][63], 2);

    const std::string sig(64, 'g');
    pc::AuthServerFinal sv{.sessionId = 7, .correlationId = 9, .errorCode = 0,
                           .flags = pc::kAuthFlagMustChangePassword, .retryAfterMs = 0, .serverSignature = sig};
    n = sv.encode(buf); auto dv = pc::AuthServerFinal::decode(buf, n);
    ASSERT_TRUE(dv); EXPECT_EQ(dv->flags, pc::kAuthFlagMustChangePassword); EXPECT_EQ(dv->serverSignature, sig);
}

TEST(AuthMessagesRegression, OutOfBoundsFieldsDoNotDecode) {
    uint8_t buf[1024];
    const std::string shortSalt(15, 's');
    pc::AuthServerFirst sf{.combinedNonce = "n", .count = 1, .salts = {pc::ScramSaltWire{shortSalt, 4096}}};
    EXPECT_FALSE(pc::AuthServerFirst::decode(buf, sf.encode(buf))) << "salt < 16";
    pc::AuthClientFinal fin{.count = 3};
    EXPECT_EQ(fin.encode(buf), 0u) << "count > 2 is not encodable";
    pc::AuthClientFirst cf{.username = "", .clientNonce = "n"};
    EXPECT_FALSE(pc::AuthClientFirst::decode(buf, cf.encode(buf))) << "empty username";
    const std::string sig(64, 'g');
    pc::AuthServerFinal sv{.serverSignature = sig};
    const std::size_t full = sv.encode(buf);
    for (std::size_t cut = 0; cut < full; ++cut) EXPECT_FALSE(pc::AuthServerFinal::decode(buf, cut)) << cut;
    const std::string sig63(63, 'g');
    pc::AuthServerFinal bad{.serverSignature = sig63};
    EXPECT_FALSE(pc::AuthServerFinal::decode(buf, bad.encode(buf))) << "a signature is 0 or 64 bytes";
}
```

`SecurityMessagesTest.cpp`: `SecurityMessagesRegression.EveryOpKindRoundTrips` (CreateUser with verifier, each AlterUser change, Drop/CreateRole, Grant/Revoke with a prefix pattern, GrantRole/RevokeRole, all with an extension carrying a 128-bit request id), `…VerifierKeysMustBe64Bytes`, `…BootstrapMacInputIsStable` (pinned bytes of `bootstrapMacInput` for a fixed request), `…TruncationNeverDecodes`. `ControlMessagesConnectTest.cpp` gains `ConnectResponseRegression.V9CarriesAuthModeAndStateWithAndWithoutCrypto` and `…PreV9FrameDecodesAsInProcessAnonymous` (a 42-byte and a V4 frame decode with `authMode == InProcess`), and the client-version pin moves to 9.

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-protocol/build -j14 --target rune_protocol_tests` → missing `AuthMessages.h`.
- [ ] **Step 3: Implement** (encoders return 0 when a field does not fit — the house `fits()` idiom; decoders are bounds-checked and return `nullopt`), the schema edits, the version comment block in `Protocol.h`:

```cpp
// 9 — authentication (spec 2026-10-13): after ConnectResponse (and TLS when negotiated) a session in
//   authMode Required runs SCRAM-SHA-512 on AuthClientFirst (12) / AuthServerFirst (13) /
//   AuthClientFinal (14) / AuthServerFinal (15); one proof per active verifier (D5).  Until it is
//   authenticated every other request is answered AuthenticationFailed (44) and the session ends.
//   NotAuthorized (45) answers any request not granted, in its own response type; SessionRevoked (46)
//   ends a session whose user was dropped / disabled / terminated (corr 0 = the session-level notice);
//   PasswordChangeRequired (47) answers every request of a one-time-password session except
//   SecurityDdl{AlterUser self SetPassword}.  SecurityDdl (79) / SecurityBootstrap (80) are answered by
//   DdlResponse (78).  An authentication-enabled broker answers a Connect below 9 ConnectResponse{ProtocolVersion}.
```

- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-protocol/build -R 'AuthMessages|SecurityMessages|Connect|Dispatch|Conformance' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-protocol add include schema tests
git -C /Users/vradu/Projects/se-worktrees/rune-protocol commit -m "feat(protocol): authentication wire - codes 44-47, client v9 / broker v5, SCRAM templates 12-15, SecurityDdl 79, SecurityBootstrap 80, ConnectResponse v9 auth fields, kSecurityTopicId, ring ops 21/22

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: rune-ql — user / role / grant DDL, SHOW statements, redaction, security plans

Spec §4 (the statements, verbatim), D6, D9, D10, D20.

**Files:**
- Modify: `rune-ql/include/rune/ql/Ast.h`, `Parser.h`, `Backends.h`, `src/Parser.cpp`, `src/Backends.cpp`, `CMakeLists.txt` (new test)
- Create: `rune-ql/src/Redact.cpp`, `rune-ql/tests/SecurityStatementsTest.cpp`

**Interfaces (namespace `rune::ql`):**
```cpp
enum class SecOp : uint8_t { Read = 0, Write, Create, Alter, Drop, Describe, Admin };          // = rune::security::Operation
enum class SecResource : uint8_t { Cluster = 0, Topic, View, Group, TransactionalId };         // = rune::security::ResourceType
struct SecPattern { SecResource type = SecResource::Topic; bool prefix = false; std::string name; };
enum class Grantee : uint8_t { User = 0, Role = 1 };
struct CreateUserDef { std::string name; std::string password; };
struct AlterUserDef  { enum class Change { AddPassword, DropPassword, SetPassword, Disable, Enable } change;
                       std::string name; std::string password; uint8_t index = 0; bool terminateSessions = false; };
struct DropUserDef   { std::string name; };
struct CreateRoleDef { std::string name; };
struct DropRoleDef   { std::string name; };
struct GrantDef      { SecOp op; SecPattern resource; Grantee grantee; std::string granteeName; };
struct RevokeDef     { SecOp op; SecPattern resource; Grantee grantee; std::string granteeName; };
struct GrantRoleDef  { std::string role; std::string user; };
struct RevokeRoleDef { std::string role; std::string user; };
struct ShowUsersDef {}; struct ShowRolesDef {};
struct ShowGrantsDef { Grantee who = Grantee::User; std::string name; std::optional<AsOfPoint> at; };   // at: Offset | Time
struct ShowSecurityHistoryDef { std::optional<Grantee> who; std::string name; std::optional<std::string> sinceIso; };
// Statement gains, appended in this order (existing indices unchanged):
//   CreateUserDef, AlterUserDef, DropUserDef, CreateRoleDef, DropRoleDef, GrantDef, RevokeDef, GrantRoleDef,
//   RevokeRoleDef, ShowUsersDef, ShowRolesDef, ShowGrantsDef, ShowSecurityHistoryDef
/// The canonical text of `s` with every password literal replaced by '<redacted>' (D10) — the ONLY statement
/// text a client sends for a security statement.  Keywords upper-case, names quoted when not plain IDENT.
[[nodiscard]] std::string redactedText(const Statement& s);
[[nodiscard]] bool isSecurityStatement(const Statement& s) noexcept;
[[nodiscard]] bool carriesPassword(const Statement& s) noexcept;
```

Grammar (added to the `Parser.cpp` header comment and `parseStatement` dispatch; keywords case-insensitive):

```
createStmt   := "create" ( "user" NAME "with" "password" STRING | "role" NAME ) ";"
alterUser    := "alter" "user" NAME ( "add" "password" STRING | "drop" "password" INT
                                    | "set" "password" STRING [ "terminate" "sessions" ] | "disable" | "enable" ) ";"
dropStmt     += "drop" ( "user" | "role" ) NAME ";"
grantStmt    := "grant" ( OPERATION "on" resource "to" ( "user" | "role" ) NAME | "role" NAME "to" "user" NAME ) ";"
revokeStmt   := "revoke" ( OPERATION "on" resource "from" ( "user" | "role" ) NAME | "role" NAME "from" "user" NAME ) ";"
resource     := "cluster" | ( "topic" | "view" | "group" | "transactional_id" ) ( "prefix" STRING | NAME )
OPERATION    := "read" | "write" | "create" | "alter" | "drop" | "describe" | "admin"
showStmt     += "show" ( "users" | "roles" ) ";"
              | "show" "grants" "for" ( "user" | "role" ) NAME [ "as" "of" ( "offset" INT | "time" STRING ) ] ";"
              | "show" "security" "history" [ ( "user" | "role" ) NAME ] [ "since" STRING ] ";"
NAME         := IDENT | STRING        (names with '.', '@' or '-' are written as a STRING)
```

`drop password` takes 1 or 2 (a parse error otherwise: "password index is 1 (oldest) or 2"); a password STRING must be non-empty (length rules are the client's, D9); `grant role` is recognised by the keyword after `grant`. Backends gain `struct SecurityPlan { rune::protocol::client::SecurityOpKind kind; …the wire fields…; std::string password; /* in memory only; never serialized */ std::string statement; /* redactedText */ };` and `[[nodiscard]] std::expected<SecurityPlan, ParseError> applySecurityPlan(const Statement&)` (SHOW statements are not plans — the client runs them as reads).

- [ ] **Step 1: Write the failing tests** — `SecurityStatementsTest.cpp`:

```cpp
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <gtest/gtest.h>
namespace ql = rune::ql;

TEST(SecurityStatementsRegression, EverySpecStatementParses) {
    const char* spec[] = {
        "CREATE USER ws1 WITH PASSWORD 'correct horse battery';",
        "ALTER  USER ws1 ADD PASSWORD 'second password 123';",
        "ALTER  USER ws1 DROP PASSWORD 1;",
        "ALTER  USER ws1 SET PASSWORD 'new password 1234' TERMINATE SESSIONS;",
        "ALTER  USER ws1 SET PASSWORD 'new password 1234';",
        "ALTER  USER ws1 DISABLE;", "ALTER USER ws1 ENABLE;", "DROP   USER ws1;",
        "CREATE ROLE world_server;",
        "GRANT READ  ON TOPIC player_events   TO ROLE world_server;",
        "GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server;",
        "GRANT READ  ON GROUP PREFIX 'world-' TO ROLE world_server;",
        "GRANT ROLE world_server TO USER ws1;",
        "REVOKE WRITE ON TOPIC PREFIX 'world.' FROM ROLE world_server;",
        "DROP ROLE world_server;",
        "SHOW USERS;", "SHOW ROLES;", "SHOW GRANTS FOR USER ws1;", "SHOW GRANTS FOR ROLE world_server;",
        "GRANT CREATE ON CLUSTER TO USER ops;", "GRANT WRITE ON TRANSACTIONAL_ID 'ws-1' TO USER ws1;",
        "SHOW GRANTS FOR USER ws1 AS OF OFFSET 12;", "SHOW SECURITY HISTORY USER ws1 SINCE '2026-10-13T00:00:00Z';",
        "REVOKE ROLE world_server FROM USER ws1;", "GRANT READ ON TOPIC 'world.zone-1' TO USER 'svc@eu-1';",
    };
    for (const char* s : spec) { auto r = ql::parse(s); EXPECT_TRUE(r.has_value()) << s << ": " << (r ? "" : r.error().message); }
}

TEST(SecurityStatementsRegression, PasswordsAreRedactedInTheStatementSentRegression) {
    auto st = ql::parse("create user ws1 with password 'correct horse battery';").value();
    const std::string text = ql::redactedText(st);
    EXPECT_EQ(text, "CREATE USER ws1 WITH PASSWORD '<redacted>';");
    EXPECT_EQ(text.find("correct"), std::string::npos);
    auto plan = ql::applySecurityPlan(st).value();
    EXPECT_EQ(plan.statement, text);
    EXPECT_EQ(plan.password, "correct horse battery") << "kept in memory for the verifier, never in the statement";
    auto set = ql::parse("ALTER USER ws1 SET PASSWORD 'p4ssword-long' TERMINATE SESSIONS;").value();
    EXPECT_EQ(ql::redactedText(set), "ALTER USER ws1 SET PASSWORD '<redacted>' TERMINATE SESSIONS;");
    EXPECT_FALSE(ql::carriesPassword(ql::parse("DROP USER ws1;").value()));
}

TEST(SecurityStatementsRegression, BadStatementsAreLocatedErrors) {
    struct Case { const char* src; const char* contains; } cases[] = {
        {"ALTER USER ws1 DROP PASSWORD 3;", "password index is 1 (oldest) or 2"},
        {"GRANT READ ON CLUSTERS TO USER u;", "expected 'cluster', 'topic', 'view', 'group' or 'transactional_id'"},
        {"GRANT FLY ON TOPIC t TO USER u;", "expected an operation"},
        {"CREATE USER ws1 WITH PASSWORD '';", "password must not be empty"},
        {"GRANT READ ON TOPIC t TO u;", "expected 'user' or 'role'"},
        {"SHOW GRANTS FOR ws1;", "expected 'user' or 'role'"},
        {"CREATE USER ws1;", "expected 'with'"},
    };
    for (const auto& c : cases) {
        auto r = ql::parse(c.src);
        ASSERT_FALSE(r.has_value()) << c.src;
        EXPECT_NE(r.error().message.find(c.contains), std::string::npos) << c.src << " -> " << r.error().message;
    }
}

TEST(SecurityStatementsRegression, ExistingStatementsKeepTheirVariantIndices) {
    EXPECT_EQ(ql::parse("topic t partitions 1;")->index(), 0u);
    EXPECT_EQ(ql::parse("diff catalog offset 1 to 2;")->index(), 7u);
    EXPECT_EQ(ql::parse("CREATE ROLE r;")->index(), 11u);
}

TEST(SecurityStatementsRegression, ScriptsMixCatalogAndSecurityStatements) {
    auto s = ql::parseScript("topic world.zone1 partitions 3;\nCREATE ROLE r; GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE r;");
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->size(), 3u);
}
```

(`topic world.zone1` needs dotted topic names in `topicStmt`: if it does not parse today, keep that line as `topic zone1 partitions 3;` — the test is about mixing, not names.)

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** the AST, the parser branches (`create`, `grant`, `revoke`, the `alter user` / `drop user|role` / `show users|roles|grants|security` branches added beside the existing ones; `parseStatement`'s `kExpected` lists the new leading keywords), `Redact.cpp` (`redactedText` re-renders every statement kind; non-security statements render their existing text unchanged — only security statements are ever sent re-rendered), `applySecurityPlan`.
- [ ] **Step 4: Run** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-ql/build --output-on-failure -j14` → every test PASS (the existing ones untouched).
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-ql add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-ql commit -m "feat(ql): CREATE/ALTER/DROP USER, CREATE/DROP ROLE, GRANT/REVOKE, SHOW USERS/ROLES/GRANTS/SECURITY HISTORY, redacted statements, security plans

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 7: Client — the SCRAM client, the `Authenticating` state, credentials, the salted-password cache, codes 44–47, the version gate

Spec §5 (clients set username / password; SCRAM; failure; version gate), §6 (live changes as seen by a client), §10, D5, D11, D26, D27; fix-on-sight: the unvalidated `authToken`.

**Files:**
- Create: `rune-client-cpp/include/rune/client/session/ScramClient.h` (+ `src/session/ScramClient.cpp`)
- Modify: `include/rune/client/session/SessionMachine.h` (state `Authenticating`, `scram_`, `onAuthServerFirst/Final`, `finishConnected_` gate), `src/agent/ClientAgent.cpp` (decode 13 / 15, route to the machine; the `ScramCache`; SessionRevoked fan-out), `include/rune/client/Client.h` + `src/Client.cpp` (`ClientConfig::{username, password, passwordFile}`, `authStatus()`, error mapping), `include/rune/client/agent/AgentConfig.h` (pass-through), `tests/support/InvokerGateway.h` (a SCRAM-speaking fake: users map, `authMode`, scripted failures)
- Delete: `ClientConfig::authToken`, `SessionConfig::authToken` (fix-on-sight; the wire field is sent empty)
- Test: `rune-client-cpp/tests/session/SessionMachineAuthTest.cpp`, `tests/client/ClientAuthErrorsTest.cpp`, `tests/client/ClientConnectRegressionTest.cpp` (the authToken regression)

**Interfaces:**
- Produces (namespace `rune::client::session`):
  ```cpp
  /// One SCRAM exchange (D5) — the variables of the Authenticating state only.
  struct ScramSentFirst { std::string clientNonce; std::string clientFirstBare; };
  struct ScramSentFinal { std::array<std::string, 2> authMessages; std::array<rune::crypto::DigestBytes, 2> expected;
                          uint8_t count = 1; };
  using ScramExchange = std::variant<ScramSentFirst, ScramSentFinal>;
  /// The duty thread's salted-password cache (8 entries, (salt, iterations) → SaltedPassword): a client with one
  /// session per broker pays PBKDF2 once per verifier, not once per session.  Never logged, zeroed on eviction.
  class ScramCache { public: const rune::crypto::scram::SaltedPassword* find(std::span<const uint8_t> salt, uint32_t i) const noexcept;
                     void put(std::span<const uint8_t> salt, uint32_t i, const rune::crypto::scram::SaltedPassword&) noexcept; };
  // SessionConfig gains:
  std::string username;            // "" = no credentials (the broker answers AuthenticationFailed — spec §5)
  std::string password;            // never logged, never sent
  // SessionState gains: Authenticating — entered from Connecting / Handshaking when the ConnectResponse says
  //   authMode Required and authState MustAuthenticate; sends AuthClientFirst, then AuthClientFinal; on
  //   AuthServerFinal{OK} with a valid server signature → finishConnected_ (Connected); on
  //   AuthServerFinal{44} → Rejected(44) (D27: with retryAfterMs > 0 and reconnectForeverAfterLive → Backoff
  //   honouring it); a missing / wrong server signature → Rejected(AuthenticationFailed) ("broker failed to
  //   prove the password").  No data frame leaves the session before Connected.
  void onAuthServerFirst(const rune::protocol::client::AuthServerFirst&, int64_t nowNs) noexcept;
  void onAuthServerFinal(const rune::protocol::client::AuthServerFinal&, int64_t nowNs) noexcept;
  [[nodiscard]] bool passwordChangeRequired() const noexcept;     // AuthServerFinal flag (bootstrap OTP)
  [[nodiscard]] rune::protocol::client::AuthMode brokerAuthMode() const noexcept;
  ```
- Produces (namespace `rune::client`): `ClientConfig::{std::string username; std::string password; std::string passwordFile; uint32_t minPasswordLength = 12; uint32_t scramIterations = 4096;}` (a non-empty `passwordFile` is read once at construction — trailing newline stripped; both set → `std::invalid_argument`); `enum class ErrorKind` (or the existing `BrokerError` codes) gains `AuthenticationFailed`, `NotAuthorized`, `SessionRevoked`, `PasswordChangeRequired`; `struct AuthStatus { enum class Kind { Authenticated, PasswordChangeRequired, Anonymous } kind; std::string user; rune::protocol::client::AuthMode brokerMode; };` `[[nodiscard]] AuthStatus Client::authStatus() const;` `uint32_t retryAfterMs` on the connect failure.
- Error semantics (every response type): 44 → the session is `Rejected`, every pending op `AuthenticationFailed`; 45 → that op fails `NotAuthorized` (non-retriable, never re-sent); a Subscribe / group subscription answered `ErrorResponse{45}` on its correlation id ends `NotAuthorized` (`Subscription::status()`); 46 on a subscription id ends it `SessionRevoked`; 46 with correlation id 0 → every in-flight request and subscription of that session completes `SessionRevoked`, the session is closed and **not** reconnected (D26); 47 → the op fails `PasswordChangeRequired`.

- [ ] **Step 1: Write the failing tests** — `SessionMachineAuthTest.cpp` (a fake `SessionIo` capturing frames; the "server" side computed with rune-crypto in the test):

```cpp
// Authentication spec §5: the client half of SCRAM-SHA-512 inside the session handshake.
#include <rune/client/session/SessionMachine.h>
#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <gtest/gtest.h>

namespace pc = rune::protocol::client; namespace rs = rune::crypto::scram; namespace sm = rune::client::session;

namespace {
struct CapturingIo : sm::SessionIo {
    std::vector<std::vector<uint8_t>> frames;
    bool sendFrame(const uint8_t* b, std::size_t n) override { frames.emplace_back(b, b + n); return true; }
    bool retarget(const std::string&) noexcept override { return true; }
    [[nodiscard]] uint16_t lastTemplate() const { return pc::MessageHeader::decode(frames.back().data(), frames.back().size())->templateId; }
};
struct Server {   // a test SCRAM server over one or two verifiers
    std::vector<rs::Verifier> verifiers; std::string snonce = "SRVnonce";
    pc::AuthServerFirst first(const pc::AuthClientFirst& cf, std::string& combined) {
        combined = std::string(cf.clientNonce) + snonce;
        pc::AuthServerFirst sf; sf.combinedNonce = combined; sf.count = static_cast<uint8_t>(verifiers.size());
        for (std::size_t i = 0; i < verifiers.size(); ++i)
            sf.salts[i] = {std::string_view(reinterpret_cast<const char*>(verifiers[i].salt.data()), verifiers[i].salt.size()), verifiers[i].iterations};
        return sf;
    }
};
pc::ConnectResponse authRequired(int64_t corr) {
    pc::ConnectResponse r; r.correlationId = corr; r.code = 0; r.assignedSessionId = 42; r.v9 = true;
    r.authMode = pc::AuthMode::Required; r.authState = pc::AuthState::MustAuthenticate; return r;
}
} // namespace

TEST(SessionMachineAuthRegression, FullExchangeReachesConnectedOnlyAfterTheServerProvesThePassword) {
    CapturingIo io; sm::SessionConfig cfg; cfg.username = "ws1"; cfg.password = "correct horse battery";
    sm::SessionMachine m(io, cfg);
    m.connect(0);
    const int64_t corr = pc::MessageHeader::decode(io.frames.back().data(), io.frames.back().size())->correlationId;
    auto r = authRequired(corr);
    m.onConnectResponse(r.code, -1, r.assignedSessionId, corr, 1, 0, {}, r.authMode, r.authState);
    EXPECT_EQ(m.state(), sm::SessionState::Authenticating);
    ASSERT_EQ(io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFirst));
    auto cf = pc::AuthClientFirst::decode(io.frames.back().data(), io.frames.back().size()).value();
    EXPECT_EQ(cf.username, "ws1");

    Server srv; srv.verifiers.push_back(*rs::makeVerifier("correct horse battery"));
    std::string combined; auto sf = srv.first(cf, combined);
    m.onAuthServerFirst(sf, 2);
    ASSERT_EQ(io.lastTemplate(), static_cast<uint16_t>(pc::TemplateId::AuthClientFinal));
    auto fin = pc::AuthClientFinal::decode(io.frames.back().data(), io.frames.back().size()).value();
    const std::string cfb = rs::clientFirstBare("ws1", cf.clientNonce);
    const std::string am = rs::authMessage(cfb, rs::serverFirst(combined, srv.verifiers[0].salt, 4096),
                                           rs::clientFinalWithoutProof(combined));
    ASSERT_TRUE(rs::verifyClientProof(rune::crypto::Digest::Sha512, srv.verifiers[0].storedKey, am, fin.proofs[0]));
    EXPECT_EQ(m.state(), sm::SessionState::Authenticating) << "not Connected until the server signs";

    auto sig = rs::serverSignature(rune::crypto::Digest::Sha512, srv.verifiers[0].serverKey, am).value();
    const std::string sigStr(reinterpret_cast<const char*>(sig.bytes.data()), sig.size);
    m.onAuthServerFinal(pc::AuthServerFinal{.errorCode = 0, .serverSignature = sigStr}, 3);
    EXPECT_EQ(m.state(), sm::SessionState::Connected);
}

TEST(SessionMachineAuthRegression, AWrongServerSignatureIsRejectedNeverConnected) {
    // as above to the final leg, then a 64-byte signature of 0x00 → Rejected(AuthenticationFailed)
}

TEST(SessionMachineAuthRegression, TwoVerifiersGetTwoProofsAndEitherPasswordWorks) {
    // Server with verifiers for "old password 123" and "new password 456"; a client configured with either one
    // answers count = 2 proofs; exactly the matching one verifies (D5).
}

TEST(SessionMachineAuthRegression, AuthenticationFailedIsTerminalUnlessRateLimited) {
    // AuthServerFinal{44, retryAfterMs 0} → Rejected, rejectCode() == 44.
    // With reconnectForeverAfterLive and a previous Live episode, {44, retryAfterMs 800} → Backoff whose
    // nextAttemptNs() >= now + 800 ms (D27).
}

TEST(SessionMachineAuthRegression, ADevModeBrokerSkipsAuthentication) {
    // ConnectResponse v9 {authMode DevDisabled, authState Anonymous} → Connected at once; brokerAuthMode() ==
    // DevDisabled; no AuthClientFirst sent.
}

TEST(SessionMachineAuthRegression, AResumedAuthenticatedSessionDoesNotReauthenticate) {
    // authState ResumedAuthenticated → Connected; no auth frame.
}

TEST(SessionMachineAuthRegression, NoDataFrameLeavesBeforeConnected) {
    // While Authenticating, sendData-type calls (the machine's isLive()/canSend) are false.
}
```

(Each sketched test is written in full in the same style as the first — the bodies above state the exact steps and assertions.)

`ClientAuthErrorsTest.cpp` (the `InvokerGateway` fake, `authMode = Required`, users `{"ws1": "correct horse battery"}`): `ClientAuthErrorsRegression.WrongPasswordFailsConnectWithAuthenticationFailed`, `…NoCredentialsFailsConnectWithAuthenticationFailed` (the fake answers 44 to an empty username — the broker enforces, the client never short-circuits), `…NotAuthorizedFailsTheOpWithoutRetry` (the fake answers a DefineTopic `45`: the result is `NotAuthorized`, exactly one DefineTopic sent), `…NotAuthorizedOnASubscriptionEndsIt`, `…SessionRevokedFailsEverythingInFlightAndDoesNotReconnectRegression` (three produces in flight + a subscription, the fake sends `ErrorResponse{46, corr 0}`: all four complete `SessionRevoked`, `connectsArrived` does not grow for 2 s of pumping), `…PasswordChangeRequiredIsReportedAndOnlyTheOpFails`, `…ScramCachePaysPbkdf2OncePerVerifierRegression` (three sessions to three endpoints of the fake with the same verifier → `ScramCache` hit count 2, PBKDF2 count 1 — a test seam counter in `ScramCache`). `ClientConnectRegressionTest.cpp`: `ClientConnectRegression.NoAuthTokenIsEverSentRegression` (with `offerCrypto = true`, the captured `ConnectRequest.authToken` is empty; `ClientConfig` has no `authToken` member — `static_assert(!requires(rune::client::ClientConfig c) { c.authToken; })`).

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14` → missing `onAuthServerFirst` / `Authenticating`.
- [ ] **Step 3: Implement.** Key rules: `SessionMachine::onConnectResponse` gains the v9 fields (defaults keep every existing call site); the `OK` branch calls `beginAuthOrFinish_(nowNs)` instead of `finishConnected_` (for the TLS path: after the handshake's `done()` — the same place `finishConnected_` is called at `SessionMachine.h` ~`:916`); `beginAuthOrFinish_` → `Authenticating` + `AuthClientFirst{username, makeNonce()}` when `authMode == Required && authState == MustAuthenticate`, else `finishConnected_`. `onAuthServerFirst` validates `combinedNonce.starts_with(clientNonce)` (else Rejected 44: "broker nonce does not extend ours"), salts each verifier's password through the agent's `ScramCache` (cold; PBKDF2 at most twice), sends one proof per verifier, stores the expected server signatures (`ScramSentFinal`). `onAuthServerFinal` with OK: the signature must equal one of the expected (constant time) → `finishConnected_`; the `kAuthFlagMustChangePassword` flag sets the session's `passwordChangeRequired()`. `tick()` in `Authenticating` applies `connectTimeoutNs` like `Handshaking`. The agent decodes templates 13 / 15 (sealed like every post-handshake frame) and routes by session id; `ErrorResponse{46, corr 0}` triggers the session-wide `SessionRevoked` completion pass (the existing SessionLost completion path, with the code `SessionRevoked` and **no** reconnect scheduling).
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'SessionMachine|ClientAuthErrors|ClientConnect|Crypto' --output-on-failure -j14` → PASS (every existing SessionMachine test included: the defaults keep the pre-v9 behaviour).
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include src tests
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): SCRAM-SHA-512 login (Authenticating state, one proof per active verifier, salted-password cache), codes 44-47, SessionRevoked is terminal; drop the never-validated authToken

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Client + CLI — security DDL API, the `__security` reader, `rune ql` security statements, `rune security bootstrap | gen-cluster-key`, credentials flags, the dev-mode banner

Spec §3 (history reads with the shared library), §4 (definitions), §7 (bootstrap, `--password-file`), §9 (console banner — the CLI's), D4, D9, D10, D20, D32, D36.

**Files:**
- Create: `rune-client-cpp/include/rune/client/security/SecurityDdl.h`, `src/security/SecurityDdl.cpp` (verifier building, wire op building), `include/rune/client/security/SecurityReader.h`, `src/security/SecurityReader.cpp`
- Create: `include/rune/client/detail/EventLogStream.h` — the Subscribe-to-marker streaming, assembly and history-epoch handling extracted from `CatalogReader.cpp` (template over the replayer type); `CatalogReader` keeps its API and becomes `EventLogStream<rune::catalog::CatalogReplayer>` + its queries (its tests must stay green, proving the extraction is behaviour-neutral)
- Modify: `include/rune/client/Client.h`, `src/Client.cpp` (security DDL ops, reads), `src/cli/Cli.cpp` (`security` command, global `--user` / `--password-file`, the banner), `src/cli/CatalogCommands.cpp` (`rune ql` runs security statements and SHOW security statements)
- Create: `include/rune/cli/SecurityCommands.h`, `src/cli/SecurityCommands.cpp`
- Test: `tests/security/SecurityDdlTest.cpp`, `tests/security/SecurityReaderTest.cpp`, `tests/RuneCliSecurityTest.cpp` (offline)

**Interfaces (namespace `rune::client`):**
```cpp
struct SecurityDdlResult { uint16_t errorCode = 0; uint64_t objectId = 0; uint32_t newVersion = 0; std::string detail; };
/// Every security DDL: builds the verifier from the plan's password (length >= minPasswordLength, else
/// std::invalid_argument naming the rule — D9; iterations = scramIterations), sends SecurityDdl with the
/// plan's REDACTED statement (D10), follows Redirect / retries NotLeader exactly like catalog DDL (the
/// DdlRetry variant), returns the DdlResponse.  The plaintext password is wiped (explicit_bzero) after use.
SecurityDdlResult Client::securityDdlAndWait(const rune::ql::SecurityPlan& plan, const DdlOptions& ddl, std::chrono::milliseconds timeout);
/// Bootstrap (D4): adminName + one-time password + the cluster key bytes; computes the MAC, sends
/// SecurityBootstrap, returns the DdlResponse.  The password is never printed by the library.
SecurityDdlResult Client::bootstrapSecurityAndWait(std::string_view adminName, std::string_view oneTimePassword,
                                                   std::span<const uint8_t> clusterKeyFileContents, std::chrono::milliseconds timeout);
/// Reads of __security (ADMIN CLUSTER only — the broker answers NotAuthorized otherwise), same contract as the
/// catalog reads (owner thread, cached verified prefix, Timeout / Replay errors).
template <class T> using SecurityResult = std::expected<T, CatalogReadError>;   // the catalog reads' error type (Timeout / Replay / Closed / Rejected …)
SecurityResult<std::vector<rune::security::RecordedEvent>> securityHistory(const rune::security::HistoryFilter&, std::chrono::milliseconds);
SecurityResult<rune::security::GrantsView> securityGrantsFor(rune::security::GranteeKind, std::string_view name,
                                                             std::optional<rune::security::AsOf> at, std::chrono::milliseconds);
SecurityResult<std::vector<rune::security::UserRow>> securityUsers(std::chrono::milliseconds);
SecurityResult<std::vector<rune::security::RoleRow>> securityRoles(std::chrono::milliseconds);
// CLI (rune::cli): rune security bootstrap --admin <name> [--password-file <p>] [--cluster-key-file <k>] --broker <ep>
//                  rune security gen-cluster-key --out <path>          (64 hex chars, mode 0600 — D32)
//                  global: --user <u> (or RUNE_USER), --password-file <p> (or RUNE_PASSWORD_FILE); never a
//                  password in argv or an environment variable.
```

- [ ] **Step 1: Write the failing tests:**
  - `SecurityDdlTest.cpp` (`InvokerGateway` fake extended with `SecurityDdl` capture): `SecurityDdlRegression.CreateUserSendsAVerifierAndARedactedStatementRegression` (the captured frame's statement has `'<redacted>'`, its bytes contain no substring of the password, its verifier verifies the password with rune-crypto), `…ShortPasswordsAreRefusedLocallyBeforeSending` (11 chars → `std::invalid_argument` "password must be at least 12 characters", nothing sent), `…EveryPlanKindMapsToItsWireOp` (one per `SecurityOpKind`), `…BootstrapMacVerifiesUnderTheKeyFile` (recompute `bootstrapMacInput` + HMAC-SHA-512 under `rune::crypto::deriveClusterKey(file)->master` (D32) → equal), `…NotLeaderIsRetriedWithTheSameRequestId`.
  - `SecurityReaderTest.cpp`: `SecurityReaderRegression.ReplaysToTheMarkerAndAnswersGrantsFor` (fake serves the golden `.rsec` on topic −7 then the caught-up marker), `…TamperedRecordIsAReplayErrorNamingTheOffset`, `…CatalogReaderIsUnchangedByTheExtraction` (runs the existing `CatalogReaderTest` scenarios through the new `EventLogStream` — the existing test file passing is the proof; this test asserts both readers share one stream type).
  - `RuneCliSecurityTest.cpp` (offline): `RuneCliSecurityRegression.GenClusterKeyWritesSixtyFourHexCharsAtMode0600`, `…PasswordIsNeverAcceptedFromArgvOrEnv` (`--password secret` → usage error; `RUNE_PASSWORD=x` ignored with a warning), `…QlDryRunPrintsTheRedactedStatement` (`rune ql --dry-run -e "create user u with password 'long enough pw';"` prints `CREATE USER u WITH PASSWORD '<redacted>';`), `…BootstrapRequiresAClusterKeyFile`.
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.** `rune ql` routes every `rune::ql::isSecurityStatement` statement through `securityDdlAndWait` (SHOW statements through the reads; output tables: `SHOW USERS` → `name enabled must_change passwords roles version`; `SHOW GRANTS` → `source operation resource pattern` with `source` = `direct` or `role:<name>`; `SHOW SECURITY HISTORY` → `offset time kind object version principal statement`). `rune security bootstrap`: without `--password-file` it generates 24 characters from `[A-Za-z0-9]` with `randomBytes` (rejection sampling), prints exactly one line `one-time password for <admin>: <pw>` to stdout and nothing else secret; with `--password-file` it reads it and prints `bootstrapped <admin> (password from <path>)`. The banner: after any CLI connect whose `authStatus().brokerMode == DevDisabled`, stderr gets `WARNING: this broker runs with authentication DISABLED (RUNE_AUTH=disabled, dev mode)`.
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'SecurityDdl|SecurityReader|CatalogReader|RuneCli' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include src tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): security DDL (verifiers built client-side, redacted statements), __security reader on a shared event-log stream, rune ql security statements, rune security bootstrap/gen-cluster-key, --user/--password-file, dev-mode banner

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (controller):** full `*Regression*` in rune-protocol, rune-ql, rune-client-cpp (plus the client TSan build).

---
### Task 9: Broker — auth configuration and dev mode, the cluster key, the `__security` partition, `SecurityApplier`, the control-thread `SecurityView`

Spec §3 (the log's properties), §8 (the cluster key file and its rotation variable), §9 (dev mode: warning at start and every 5 min, a metric, multi-node refusal, not the default), D28, D29, D31, D32, D39.

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h`, `libs/node/src/NodeConfig.cpp` (anchor: `// --- RUNE_TLS` block — add the auth block after it), `src/main.cpp` (refuse `InProcessUnauthenticated`)
- Create: the library `libs/peerlink/` (`CMakeLists.txt`: `rune_peerlink`, depends on `rune_crypto` only; linked PUBLIC by `rune_gateway`, `rune_node`, `rune_consensus`, `rune_replication`) with `include/rune/peerlink/ClusterKeys.h`, `src/ClusterKeys.cpp` (Task 15 adds the handshake, Task 16 the frame crypto); root `CMakeLists.txt` `add_subdirectory(libs/peerlink)` before the libraries that link it
- Create: `libs/node/include/rune/node/AdminLogPartition.h`, `libs/node/src/AdminLogPartition.cpp` — `makeAdminLogPartitionConfig(config, brokers)`, `adminLogMinIsr(brokers)`, `dispatchAdminLogFrame(replicas, topicId, partition, templateId, frame, len, nowNs, stats)` where `replicas` maps −6 / −7 to their replica; `CatalogPartition.h` keeps `kCatalogTopicId` and delegates (`makeCatalogPartitionConfig` = `makeAdminLogPartitionConfig`; `dispatchCatalogFrame` stays for its existing tests and calls the generic one)
- Modify: `libs/node/include/rune/node/CatalogCheckpoint.h`, `src/CatalogCheckpoint.cpp` (`writeLogCheckpoint(dir, cp)` / `readLogCheckpoint(dir)` — the catalog names stay as wrappers)
- Create: `libs/node/include/rune/node/SecurityPartition.h` (`kSecurityTopicId = -7`, static_assert == the protocol constant), `libs/node/include/rune/node/SecurityApplier.h`, `libs/node/src/SecurityApplier.cpp`, `libs/gateway/include/rune/gateway/SecurityView.h` (+ `libs/gateway/src/SecurityView.cpp`; the gateway library links `rune_security` — it is the control thread's object and the gateway's SCRAM / permission code reads it)
- Modify: `libs/node/include/rune/node/ReplicationMetadataBridge.h` (`isCoordinatorTopic_` / `mapDriven_` include −7; the −7 role / hold / AlterIsr relays beside −6's, via `postAdminLogRole_(topicId, leader, pa, msgType)`), `libs/node/include/rune/node/MetadataService.h` (the coordinator partition list includes (−7, 0) — anchor `coord.push_back(rune::metadata::PartitionKey{kCatalogTopicId, kCatalogPartition})` in `BrokerNode.cpp`), `AdminChannels.h` (ids of Global Constraints), `RoleCounter.h` (`1'000'226`–`228`), `BrokerNode.{h,cpp}` (`buildSecurity_` beside `buildCatalog_`; the boot replay beside the catalog's at anchor `bootApplier_->replayLocal(*catalogLog_`; the admin→control drain of `kA2cSecurityApplied` / `kA2cSecurityHalted` beside `if (tid == kA2cCatalogApplied)`; the storage id + strip + hold exactly as ISR v3 does for −6; the dev-mode timer), `BrokerNode.h` `AdminWorkerAgent` (drives the −7 replica on the same admin plane)
- Test: `libs/node/tests/node/NodeConfigTest.cpp` (append), `libs/peerlink/tests/ClusterKeysTest.cpp` (`rune_peerlink_tests`), `libs/node/tests/node/SecurityApplierTest.cpp`, `libs/node/tests/node/SecurityAllocRegressionTest.cpp` (own binary `rune_security_alloc_regression_tests`, the F12 `operator new` pattern), `libs/node/tests/integration/SecurityReplicationTest.cpp` (3 nodes, `CatalogBrokerCluster.h`), `libs/node/CMakeLists.txt`

**Interfaces:**
- Produces:
  ```cpp
  // NodeConfig.h
  enum class AuthMode : uint8_t { InProcessUnauthenticated = 0, Enabled = 1, DevDisabled = 2 };   // D31
  struct NodeConfig { /* … */ AuthMode authMode = AuthMode::InProcessUnauthenticated;
      std::string clusterKeyFile, clusterKeyFileNext; int64_t authTimeoutMs = 10'000; int32_t peerAuthStreamId = 40; };
  // rune/peerlink/ClusterKeys.h (namespace rune::peerlink, D32) — loaded on the startup thread; each thread that needs it gets its own copy.
  using ClusterKey = rune::crypto::ClusterKeyMaterial;            // Task 1: the one derivation (the CLI uses it too)
  struct ClusterKeys { ClusterKey current; std::optional<ClusterKey> next;
                       [[nodiscard]] const ClusterKey* byId(uint64_t id) const noexcept; };
  /// Reads the file(s), refuses (a message naming the variable and the path): missing, unreadable, < 32 bytes.
  [[nodiscard]] std::expected<ClusterKeys, std::string> loadClusterKeys(const std::string& current, const std::string& next);
  // SecurityApplier.h — CatalogApplier's contract for __security (same step order, same phases).
  struct SecurityEffects {
      std::function<void(int64_t offset, std::span<const uint8_t> eventBytes)> eventApplied;   // → kA2cSecurityApplied
      std::function<void(int64_t offset, uint64_t digest)>                    applied;        // counter 1'000'227
      std::function<void(const rune::security::ApplyError&)>                  halted;         // → kA2cSecurityHalted
      std::function<void(int err, const std::string& dir)>                    storageFailed;
      std::size_t maxFrameBytes = 0;
  };
  struct SecurityBootEffects { SecurityEffects sink; };  struct SecurityLiveEffects { SecurityEffects sink; };
  class SecurityApplier {
  public:
      struct Applying { int64_t nextOffset = 0; };  struct Halted { rune::security::ApplyError error; };
      struct AwaitingBoot {}; struct Booted { SecurityBootEffects effects; }; struct Live { SecurityLiveEffects effects; };
      explicit SecurityApplier(std::string securityDir);
      int  replayLocal(rune::storage::Log& log, SecurityBootEffects boot);       // AwaitingBoot → Booted; throws on a damaged checkpoint
      bool goLive(SecurityLiveEffects live);
      int  applyCommitted(rune::replication::PartitionReplica& r);               // Live only; reused buffer
      [[nodiscard]] const rune::security::SecurityState& state() const noexcept;
      [[nodiscard]] bool halted() const noexcept; [[nodiscard]] int64_t nextOffset() const noexcept;
      void tamperNextFingerprintForTest() noexcept;
  };
  // rune/gateway/SecurityView.h — the CONTROL thread's mirror (D29).  Applies kA2cSecurityApplied in offset order with the same
  // library; a gap or an apply error is an Internal halt of the mirror (the D28 fail-closed path, Task 14).
  class SecurityView {
  public:
      explicit SecurityView(rune::security::SecurityState booted);              // handed over by the startup thread
      [[nodiscard]] std::expected<rune::security::Applied, rune::security::ApplyError> onApplied(int64_t offset, std::span<const uint8_t> bytes);
      void onHalted(const rune::security::ApplyError&);
      [[nodiscard]] const rune::security::SecurityState& state() const noexcept;
      [[nodiscard]] int64_t appliedOffset() const noexcept;
      [[nodiscard]] bool halted() const noexcept;
  };
  ```
- Configuration rules (`NodeConfig::parse` + `BrokerNode::start`, each refusal an explicit `std::runtime_error` naming the variable):
  - `RUNE_AUTH` unset or `enabled` → `Enabled`; `disabled` → `DevDisabled`; anything else → refused.
  - `Enabled` requires `RUNE_CLUSTER_KEY_FILE` (refused without — single node included, D4/D32); the file must exist, be readable and hold ≥ 32 bytes after trimming; `RUNE_CLUSTER_KEY_FILE_NEXT` is optional (same rules); a group/world-readable key file logs one warning.
  - `DevDisabled` with more than one peer → refused: `"RUNE_AUTH=disabled (dev mode) refuses to start as part of a multi-broker cluster (RUNE_PEERS lists N brokers); enable authentication"`.
  - `DevDisabled` logs at start and every 5 minutes (control-thread timer, `spdlog::warn`): `"AUTHENTICATION DISABLED (RUNE_AUTH=disabled): every request runs as 'anonymous' with full access — dev mode only"`; counter `1'000'228` = 1 (0 when `Enabled`).
  - `src/main.cpp` refuses a config whose `authMode` is `InProcessUnauthenticated` (unreachable from `parse`; a guard against an embedding mistake).

**`__security` (D39):** built by `buildSecurity_` exactly as `buildCatalog_` builds `__catalog` — dataDir `__security/`, `makeAdminLogPartitionConfig` (never compacted / retention-deleted, minIsr majority, disk only, acks=all appends), replica owned and driven by the admin thread on the same admin replication plane (`RUNE_CATALOG_REPL_STREAM_ID`, frames dispatched by topic id), leadership from the controller-ISR path (`decideCoordinatorPartition`, never unclean; single node: the consensus leader), the ISR v3 storage id (`setStorageId`) and leadership hold (`kMsgSecurityHoldLeadership`), formation bootstrap with an empty first leader. The `SecurityApplier` checkpoint is `dataDir/__security/applied`. The startup thread replays the local prefix once (before any thread runs, beside the catalog's boot replay), hands `applier.state()` (a copy) to the control thread's `SecurityView`, and the admin thread's first pass makes the applier `Live` (the catalog's Task 16 pattern: `adoptBootedApplier`). Every applied event's bytes go to the control thread on `kA2cSecurityApplied` (`[offset:i64][event bytes]`, ≤ the admin→control ring's max message — an oversize event halts the applier at that offset, never truncated).

- [ ] **Step 1: Write the failing tests:**
  - `NodeConfigTest.cpp`: `NodeConfigRegression.AuthDefaultsToEnabledWhenUnsetRegression`, `…UnknownRuneAuthValueIsRefused`, `…EnabledWithoutAClusterKeyFileRefusesToStartRegression`, `…ShortClusterKeyFileIsRefused` (31 bytes), `…DevModeRefusesAMultiBrokerClusterRegression`, `…DevModeSingleNodeStartsAndWarns` (captured spdlog sink sees the warning line and counter 1'000'228 == 1), `…CodeBuiltConfigIsInProcessUnauthenticated` (`NodeConfig{}.authMode`).
  - `ClusterKeysTest.cpp`: `ClusterKeysRegression.KeyIdIsStableAndDiffersPerKey`, `…TrailingWhitespaceIsIgnored` (`"k…\n"` and `"k…"` give the same key id), `…NextKeyIsFoundById`.
  - `SecurityApplierTest.cpp` (a disk `Log` + a `PartitionReplica` rig like `CatalogRig.h`, events from `testing::goldenScriptV1()`): `SecurityApplierRegression.AppliesCommittedEventsInOffsetOrderAndMirrorsTheirBytes`, `…FingerprintMismatchHaltsAtThatOffsetAndAppliesNothingAfter`, `…CheckpointPrecedesTheEffectSoARestartNeverReappliesRegression` (crash seam after the checkpoint write: the boot replay re-posts the final state, `eventApplied` never runs twice for an offset), `…BootReplayVerifiesTheDigestAgainstTheCheckpoint`, `…ExactlyOneBootReplay`, `…BootReplayOf10kEventsUnderOneSecond` (Release-only assertion under `#ifdef NDEBUG`; Debug just runs it).
  - `SecurityAllocRegressionTest.cpp`: `SecurityAllocRegression.IdleApplierPassAllocatesNothing`.
  - `SecurityReplicationTest.cpp` (3 nodes, `InProcessUnauthenticated` — authentication is not the subject here, the log is): `SecurityReplicationRegression.FormationCreatesSecurityOnEveryBrokerWithACleanLeader`, `…AnAppendedEventReachesEveryBrokersViewInOffsetOrder` (append through the leader's test seam `appendSecurityEventOnLeaderForTest`; every broker's `SecurityView::appliedOffset()` reaches it and the digests agree), `…NeverCompactedOrRetentionDeleted`, `…AWipedFollowerRefetchesSecurityFromZero` (the ISR v3 catalog test, for −7).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** in this order: config + refusals + `main.cpp`; `ClusterKeys` (D32 with `rune::crypto::hkdf` / `hmac`); the generic admin-log helpers (catalog tests must stay green: `ctest -R 'CatalogPartition|CatalogReplication|CatalogApplier|CatalogCheckpoint'`); `SecurityApplier` (a structural copy of `CatalogApplier` minus projections and per-kind effects: one `eventApplied` per event); `SecurityView`; the bridge / metadata / BrokerNode wiring; the dev-mode timer.
- [ ] **Step 4: Run** — `cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-fast && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'NodeConfig|ClusterKeys|SecurityApplier|SecurityAlloc|SecurityReplication|Catalog' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs src CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(security): RUNE_AUTH (default on) + dev mode rules, cluster key files, the __security partition on the admin plane, SecurityApplier, the control-thread SecurityView

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Broker — the security write path (`SecurityAdmin`), bootstrap, `__security` reads, the DDL answer gate

Spec §3 (serialized through the leader's AdminWorker; reads restricted), §7 (bootstrap: once, principal `bootstrap`, refuses a second run), D3, D4, D24, D36, D37; fix-on-sight: `CatalogReadServer`'s hard-coded topic id.

**Files:**
- Create: `libs/node/include/rune/node/SecurityAdmin.h`, `libs/node/src/SecurityAdmin.cpp`, `libs/node/include/rune/node/SecurityAnswerGate.h`
- Modify: `libs/node/include/rune/node/AdminWorker.{h}`, `src/AdminWorker.cpp` (owns a `SecurityAdmin` + a second `CatalogReadServer` for −7; `doWork` drives both logs), `CatalogReadServer.{h,cpp}` (constructor takes `int32_t topicId`), `AdminChannels.h` (`SecurityDdlMsg`, `SecurityBootstrapMsg` owning forms + codecs; `kMsgEgressFrameGated`), `libs/gateway/include/rune/gateway/IAdminRequestSink.h` (`submitSecurityDdl`, `submitSecurityBootstrap`, `securitySubscribe/Unsubscribe`), `ClientGateway.{h,cpp}` (handlers for 79 / 80, Subscribe −7 routing, the `SecurityRoute` redirect), new `libs/gateway/include/rune/gateway/SecurityRoute.h` (the `CatalogRoute` value for −7), `BrokerNode.cpp` (the admin egress drain releases gated frames through the `SecurityAnswerGate`)
- Create: `libs/node/tests/support/SecurityDdlRig.h`, `libs/node/tests/node/SecurityWritePathTest.cpp`, `libs/node/tests/node/SecurityReadServerTest.cpp`, `libs/node/tests/node/SecurityAnswerGateTest.cpp`

**Interfaces:**
```cpp
// SecurityAdmin.h (admin thread) — AdminWorker's DDL contract for __security.
struct SecurityFollower { int32_t leaderId = -1; int32_t epoch = 0; };
struct SecurityLeaderCatchingUp { int32_t epoch = 0; };
struct SecurityLeaderServing { int32_t epoch = 0; };
struct SecurityHalted { int64_t offset = -1; };
using SecurityRole = std::variant<SecurityFollower, SecurityLeaderCatchingUp, SecurityLeaderServing, SecurityHalted>;
using SecurityRequest = std::variant<SecurityDdlMsg, SecurityBootstrapMsg>;
class SecurityAdmin {
public:
    SecurityAdmin(SecurityApplier& applier, rune::replication::PartitionReplica* replica, AdminDeps& deps, int32_t selfId);
    void submit(SecurityRequest r);                      // queued, one in flight; > kMaxPending → Internal at once
    void onRole(const AdminRoleMsg& role);               // demotion: the in-flight DDL answers NotLeader (retriable)
    int  doWork(int64_t nowNs);
    [[nodiscard]] std::string_view roleName() const noexcept;
    [[nodiscard]] uint64_t startedCount() const noexcept; [[nodiscard]] uint64_t answeredCount() const noexcept;
    struct Idle {}; struct AwaitingCommit { SecurityRequest req; rune::security::SecurityEvent event; int64_t offset = -1; int64_t startedNs = 0; };
};
// SecurityAnswerGate.h (control thread): a gated admin egress frame waits until view.appliedOffset() >= offset and
// the permission refresh of that event has been enqueued to every worker (Task 13/14 call markRefreshed(offset)).
class SecurityAnswerGate {
public:
    void hold(int64_t sessionId, int64_t offset, std::span<const uint8_t> frame);   // capacity-bounded; beyond → answer Internal
    template <class Send> int release(int64_t appliedAndRefreshedOffset, Send&& send);
};
```

**Write path (`SecurityAdmin`, the AdminWorker rules for `__security`):** a `SecurityDdlMsg` / `SecurityBootstrapMsg` is answered `NotLeader (2)` unless `SecurityLeaderServing` (catching up, demoted, commit timeout `kDdlTimeoutNs`) and `Internal (6)` when `SecurityHalted`; the leader builds the event (objectId / versions from its applied state, the principal from `DdlMeta` — `authenticated` = the session's user, `"bootstrap"` for a bootstrap, D3 — the **redacted** statement, `timestampMs` = its wall clock), dedupes on `(requestId, requestFingerprint)` (a committed one answers its original `DdlResponse`; a different fingerprint answers `InvalidRequest` "requestId reused for a different statement"), dry-runs `previewApplied` (a refusal answers `InvalidRequest 39` with `refusalName` + detail — never appended), encodes, appends acks=all, answers once committed **and** applied locally through `kMsgEgressFrameGated` carrying the event's offset (D24). The gateway answers `Redirect (3)` (the `__security` leader's client endpoint, `SecurityRoute`) for 79 / 80 on a non-leader, exactly like catalog DDL; a pre-v9 session can never reach this (Task 11's version gate).

**Bootstrap (D3, D4):** the gateway verifies the MAC of `SecurityBootstrap` with its `ClusterKeys` copy (current, then next — constant-time) before forwarding; a mismatch answers `DdlResponse{InvalidRequest, "bootstrap proof does not match this cluster's key"}` and logs `security: bootstrap refused from <source>: bad cluster-key proof`. The leader's rule refuses a second bootstrap (`AlreadyBootstrapped` → `InvalidRequest` "security is already bootstrapped (a security admin exists)"). Bootstrap is accepted on a session that is **not** authenticated (Task 11's gate lets exactly 80 through while `!view.hasSecurityAdmin()`).

**`__security` reads:** `Subscribe{topicId -7, partition 0, partitionKnown 1}` → `kMsgSecuritySubscribe` → the admin's second `CatalogReadServer(kSecurityTopicId, …)` (backlog, caught-up marker, history epoch — the catalog read contract); the ADMIN check is Task 12's (until then every session may read, as every session may do everything in `InProcessUnauthenticated`).

- [ ] **Step 1: Write the failing tests:**
  - `SecurityWritePathTest.cpp` (`SecurityDdlRig`: one AdminWorker + replica + applier + a recording egress, like `CatalogDdlRig.h`): `SecurityWritePathRegression.EveryOpKindCommitsAndAnswersItsObject` (all nine wire kinds), `…RefusalsAnswerInvalidRequestNamingTheRuleAndAppendNothing` (one per `Refusal`), `…DuplicateRequestIdAnswersTheOriginalResultRegression`, `…ReusedRequestIdWithAnotherStatementIsInvalidRequest`, `…DemotionAnswersTheInFlightDdlNotLeaderRegression`, `…CatchingUpLeaderAnswersNotLeader`, `…HaltedBrokerAnswersInternal`, `…NoEventEverCarriesAPasswordLiteralRegression` (every appended event's bytes are scanned for each test password used: absent; the statement contains `'<redacted>'`), `…PrincipalIsTheSessionsAuthenticatedUser`.
  - `SecurityBootstrapRegression.FirstBootstrapCreatesAMustChangeAdminWithAdminCluster`, `…SecondBootstrapIsRefusedExplicitly`, `…BootstrapWithoutAValidProofIsRefusedAndLogged`, `…BootstrapUnderTheNextKeyIsAccepted`, `…BootstrapIsTheFirstEventWithPrincipalBootstrap`.
  - `SecurityReadServerTest.cpp`: `SecurityReadServerRegression.ServesSecurityUpToTheHighWatermarkThenTheMarker`, `CatalogReadServerRegression.ServesTheTopicItWasBuiltForRegression` (a server built for −7 stamps −7 on every frame; the −6 server's frames are unchanged).
  - `SecurityAnswerGateTest.cpp`: `SecurityAnswerGateRegression.OkIsReleasedOnlyAfterThisBrokerEnforcesItRegression` (a gated frame at offset 5 is not sent at applied 4, is sent at applied-and-refreshed 5, exactly once), `…FullGateAnswersInternalNeverDrops`.
  - 3-node (append to `SecurityReplicationTest.cpp`): `SecurityReplicationRegression.SecurityDdlViaAFollowerIsRedirectedToTheLeader`.
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** (the AdminWorker's existing machinery is the template: `startNext_`, `advance_`, `answer_` — copy their structure into `SecurityAdmin`; do not touch the catalog's state machine).
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'SecurityWritePath|SecurityBootstrap|SecurityReadServer|CatalogReadServer|SecurityAnswerGate|SecurityReplication|CatalogWritePath|CatalogLeadership' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(security): SecurityAdmin write path (dedupe, redirect, NotLeader, halted), cluster-key-proven bootstrap, __security reads, the DDL answer gate; CatalogReadServer takes its topic id

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 11: Broker — the SCRAM server, session auth states, the version gate, the throttle, the auth deadline, dev mode, login logs and counters, the 5 ms gate

Spec §5 (every bullet), §7 (must change the password on first login), §9 (anonymous), §10, §12 (login ≤ 5 ms), D5, D7, D8, D11–D14, D27.

**Files:**
- Create: `libs/gateway/include/rune/gateway/SessionAuth.h`, `libs/gateway/include/rune/gateway/ScramServer.h`, `libs/gateway/src/ScramServer.cpp`, `libs/gateway/include/rune/gateway/LoginThrottle.h`, `libs/gateway/src/LoginThrottle.cpp`
- Create: `libs/gateway/include/rune/gateway/PermissionTable.h` with `allowAll()` and `allows()` / `allowsCluster()` only (Task 12 completes it: `compile`, patterns, write bits)
- Modify: `libs/gateway/include/rune/gateway/SessionTable.h` (`Session::auth`, `Session::sourceHost`), `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp` (the gate before `switch (tid) {`, handlers 12 / 14, Connect's version gate + v9 ConnectResponse, the auth-deadline reaper beside `reapExpired`, the PasswordChangeRequired rule), `libs/gateway/CMakeLists.txt`, `BrokerNode.cpp` (hands the gateway its `SecurityView&`, `ClusterKeys` copy and `AuthMode`; counters `1'000'229`–`231`)
- Create: `libs/node/tests/support/AuthWireClient.h` (raw frames over Aeron UDP + rune-crypto SCRAM; used by Tasks 11–19), `libs/gateway/tests/LoginThrottleTest.cpp`, `libs/gateway/tests/ScramServerTest.cpp`, `libs/node/tests/node/AuthLoginTest.cpp`, `libs/node/tests/client/ClientLoginBrokerRegressionTest.cpp`, `libs/node/tests/client/RuneCliSecurityBrokerRegressionTest.cpp`, `libs/node/tests/support/AuthBroker.h` (`OneBroker` with `authMode = Enabled`, a generated key file, `bootstrapAdmin()` and `createUser(name, password, grants…)` helpers through an admin `AuthWireClient`)

**Interfaces:**
```cpp
// SessionAuth.h — a session's auth state (rule 3: each state its own variables).
struct AuthAwaitingFirst { int64_t deadlineNs = 0; };
struct AuthAwaitingFinal {
    int64_t deadlineNs = 0; uint64_t userId = 0;              // 0 = unknown user (the fake verifier, D8)
    std::string user; std::string clientFirstBare; std::string combinedNonce;
    uint8_t count = 1; std::array<rune::crypto::scram::Verifier, 2> verifiers;   // copies (the view may change)
};
struct AuthPasswordChangeRequired { uint64_t userId = 0; std::string user; };
struct AuthAuthenticated { uint64_t userId = 0; std::string user; PermissionTable perms; };   // PermissionTable: Task 12
struct AuthAnonymous {};                                     // DevDisabled / InProcessUnauthenticated: allow all
using SessionAuth = std::variant<AuthAwaitingFirst, AuthAwaitingFinal, AuthPasswordChangeRequired, AuthAuthenticated, AuthAnonymous>;
enum class AuthGate : uint8_t { Allow, AnswerAuthFailedAndEnd, AnswerPasswordChangeRequired };
/// The ONE rule of what a session may send in its auth state (the gate before the dispatch switch).
[[nodiscard]] AuthGate gateFor(const SessionAuth&, rune::protocol::client::TemplateId, bool bootstrapOpen,
                               bool isSelfSetPassword) noexcept;

// ScramServer.h — pure functions of (view, request); the gateway owns the state.
struct ServerFirstPlan { AuthAwaitingFinal next; std::vector<uint8_t> frame; };
[[nodiscard]] ServerFirstPlan scramServerFirst(const rune::security::SecurityState&, const rune::peerlink::ClusterKey& fakeSaltKey,
                                               const rune::protocol::client::AuthClientFirst&, int64_t deadlineNs);
enum class LoginFailure : uint8_t { NoCredentials, UnknownUser, BadProof, Disabled, Throttled, Timeout, SecurityHalted };
struct LoginOk { uint64_t userId; std::string user; bool mustChangePassword; std::array<uint8_t, 64> serverSignature; };
[[nodiscard]] std::variant<LoginOk, LoginFailure> scramServerFinal(const rune::security::SecurityState&,
                                                                   const AuthAwaitingFinal&,
                                                                   const rune::protocol::client::AuthClientFinal&);
[[nodiscard]] std::string_view loginFailureName(LoginFailure) noexcept;   // logs only — never on the wire (Review Focus 1)

// LoginThrottle.h — D14.  Fixed capacity (open addressing, 4096 pair + 4096 host entries, expiry eviction); cold.
class LoginThrottle {
public:
    struct Verdict { bool allowed = true; uint32_t retryAfterMs = 0; };
    [[nodiscard]] Verdict check(std::string_view user, std::string_view host, int64_t nowNs) const noexcept;
    void onFailure(std::string_view user, std::string_view host, int64_t nowNs) noexcept;
    void onSuccess(std::string_view user, std::string_view host) noexcept;
    static constexpr uint32_t kFreePairFailures = 2, kFreeHostFailures = 10;
    static constexpr int64_t  kBaseDelayNs = 250'000'000, kMaxDelayNs = 30'000'000'000, kForgetAfterNs = 900'000'000'000;
};
```

**Rules (the gateway, control thread):**
1. **Connect (version gate, D11):** `Enabled` and `clientVersion < 9` → `ConnectResponse{ProtocolVersion}`, no session, log `auth: refused client version N at connect (authentication requires 9)`. Otherwise the session is created with `auth = AuthAwaitingFirst{now + authTimeoutMs}` (`Enabled`) or `AuthAnonymous` (other modes); `sourceHost` = the Connect image's `sourceIdentity()` host (D14); a resume of a session that is `AuthAuthenticated` / `AuthPasswordChangeRequired` keeps it and answers `authState = ResumedAuthenticated`. v9 sessions get the v9 ConnectResponse (`authMode`, `authState`). With TLS not negotiated and `Enabled`, log once per session `auth: session N authenticated over plaintext (no TLS): frames are not integrity-protected` (D12).
2. **The gate** (`gateFor`, before `switch (tid) {`): Connect / HandshakeContinue / Keepalive / Ping / Disconnect are always allowed; `AuthAwaitingFirst` allows 12 (and 80 while `bootstrapOpen = !view.hasSecurityAdmin()`); `AuthAwaitingFinal` allows 14; any other template in those states → answered `AuthenticationFailed` **in its own response type** (the existing per-template answer helpers used for InvalidRequest) and the session ends; `AuthPasswordChangeRequired` allows only 79 when it is `AlterUser <self> SetPassword` — anything else is answered `PasswordChangeRequired (47)` in its own response type (the session stays); `AuthAuthenticated` / `AuthAnonymous` → `Allow` (what is allowed is Task 12's).
3. **AuthClientFirst (12):** throttle `check(user, sourceHost)` — denied → `AuthServerFinal{44, retryAfterMs}`, session ends, counter `231`, log `auth: rate limited user=<u> source=<h> retry_after_ms=<n>`. A security-halted view (D28) → `AuthServerFinal{44}`. Else `scramServerFirst`: a known user (enabled or not — disabled fails at the final leg, Review Focus 1) gets its 1–2 verifiers; an unknown or empty name gets the fake verifier (salt = `HMAC-SHA-512(currentClusterKey.master, "rune-fake-salt|" + name)[0..16]`, 4096 iterations, random stored / server keys that nothing can match) — the frame is shape-identical. `combinedNonce = clientNonce + makeNonce()`.
4. **AuthClientFinal (14):** `scramServerFinal`: `BadProof` unless one proof verifies against its verifier (`verifyClientProof`); `Disabled` if it verified but the user is disabled; `UnknownUser` for the fake. Failure → `AuthServerFinal{44, retryAfterMs = 0, detail "authentication failed"}` (the ONLY failure text), session ends, `throttle.onFailure`, counter `230`, log `auth: login failed user=<u> source=<h> session=<id> reason=<loginFailureName>`. Success → `AuthServerFinal{0, flags, serverSignature}`, `throttle.onSuccess`, counter `229`, log `auth: login ok user=<u> source=<h> session=<id>`; the state becomes `AuthPasswordChangeRequired` (the user's `mustChangePassword`) or `AuthAuthenticated{userId, user, perms}` (Task 12 compiles `perms`; until Task 12 lands it is an allow-all table — Checkpoint C is after Task 13, so this is never shipped).
5. **Deadline:** the reaper answers `AuthServerFinal{44}` and ends every session still `AuthAwaiting*` past its deadline.
6. **Mirror events:** when the `SecurityView` applies a `SetPassword` of a user, every `AuthPasswordChangeRequired` session of that user becomes `AuthAuthenticated` (the general refresh is Task 14's; this one transition lands here so the bootstrap flow works end to end).

- [ ] **Step 1: Write the failing tests** — `LoginThrottleTest.cpp`:

```cpp
#include <rune/gateway/LoginThrottle.h>
#include <gtest/gtest.h>
using rune::gateway::LoginThrottle;
constexpr int64_t ms = 1'000'000;

TEST(LoginThrottleRegression, TheFirstTwoFailuresAreFreeThenTheDelayDoublesToTheCap) {
    LoginThrottle t; int64_t now = 0;
    t.onFailure("ws1", "10.0.0.9", now); t.onFailure("ws1", "10.0.0.9", now);
    EXPECT_TRUE(t.check("ws1", "10.0.0.9", now).allowed);
    uint32_t expected = 250;
    for (int k = 0; k < 12; ++k) {
        t.onFailure("ws1", "10.0.0.9", now);
        const auto v = t.check("ws1", "10.0.0.9", now);
        ASSERT_FALSE(v.allowed) << k;
        EXPECT_EQ(v.retryAfterMs, std::min<uint32_t>(expected, 30'000)) << k;
        now += static_cast<int64_t>(v.retryAfterMs) * ms;          // wait it out
        EXPECT_TRUE(t.check("ws1", "10.0.0.9", now).allowed);
        expected *= 2;
    }
}

TEST(LoginThrottleRegression, AttackerAtOneAddressNeverDelaysTheUserAtAnotherRegression) {
    LoginThrottle t;
    for (int i = 0; i < 1000; ++i) t.onFailure("ws1", "203.0.113.7", 0);
    EXPECT_FALSE(t.check("ws1", "203.0.113.7", 0).allowed);
    EXPECT_TRUE(t.check("ws1", "10.0.0.9", 0).allowed) << "no hard lockout (spec §5)";
}

TEST(LoginThrottleRegression, ManyUsersFromOneAddressAreThrottledPerAddress) {
    LoginThrottle t;
    for (int i = 0; i < 11; ++i) t.onFailure("user" + std::to_string(i), "203.0.113.7", 0);
    EXPECT_FALSE(t.check("fresh-user", "203.0.113.7", 0).allowed) << "the host key (10 free) throttles a sweep";
}

TEST(LoginThrottleRegression, SuccessResetsThePairAndIdleEntriesAreForgotten) {
    LoginThrottle t;
    for (int i = 0; i < 5; ++i) t.onFailure("ws1", "h", 0);
    t.onSuccess("ws1", "h");
    EXPECT_TRUE(t.check("ws1", "h", 0).allowed);
    for (int i = 0; i < 5; ++i) t.onFailure("ws2", "h2", 0);
    EXPECT_TRUE(t.check("ws2", "h2", LoginThrottle::kForgetAfterNs + 1).allowed);
}

TEST(LoginThrottleRegression, AFullTableEvictsTheOldestNeverAllocatesNorRefuses) {
    // 10 000 distinct (user, host) failures: check() stays O(1) and the newest entries are throttled.
}
```

`AuthLoginTest.cpp` (`AuthBroker` + `AuthWireClient`): `AuthLoginRegression.FailureAnswersAreIndistinguishableRegression` (wrong password, unknown user, disabled user, empty username: the `AuthServerFirst` frames have the same shape — count 1, 16-byte salt, 4096 — and the four `AuthServerFinal` frames are byte-identical except header ids), `…UnknownUserGetsTheSameFakeSaltEveryTime`, `AuthLoginTest.BothActivePasswordsLogIn`, `AuthLoginTest.RotationAddLoginDropOldFails`, `AuthLoginRegression.PreV9ClientIsRefusedAtConnectRegression`, `…RequestBeforeAuthenticationIsAnsweredAuthenticationFailedAndEndsTheSession` (a Produce first → `ProduceResponse{44}`, then the session is gone: `UnknownSession` to a Ping), `…SessionThatNeverAuthenticatesIsEndedAtTheDeadline` (`authTimeoutMs = 300`), `…RateLimitedLoginCarriesRetryAfter`, `AuthLoginTest.BootstrapAdminMustChangePasswordFirst` (bootstrap → login with the OTP → flags must-change; a DefineTopic → `DefineTopicResponse{47}`; `ALTER USER admin SET PASSWORD` → OK; the same session's DefineTopic is then allowed), `…DevModeSessionsAreAnonymousAndNeverAskForCredentials`, `…ResumedSessionKeepsItsAuthentication`, `…LoginLogsNameTheReasonButTheWireDoesNot` (captured spdlog sink has `reason=bad-proof` / `reason=unknown-user`; the frames do not), `…CountersCountOkFailedAndThrottled`.

`ClientLoginBrokerRegressionTest.cpp` (the real `rune::client::Client` against `AuthBroker`): `ClientLoginBrokerRegression.ConnectsWithUsernameAndPassword`, `…WrongPasswordFailsConnectAuthenticationFailed`, `…NoCredentialsFailsAuthenticationFailed`, `…OldClientVersionIsRefusedExplicitly` (a client with `SessionConfig::clientVersion = 8` → `Rejected(ProtocolVersion)`), `AuthGatesRegression.FullScramLoginMedianUnder5msRegression` (50 logins of a connected-transport session — the client stamps `AuthClientFirst` send → `AuthServerFinal` receive; median ≤ 5 ms; record p50 / p99 in the test output). `RuneCliSecurityBrokerRegressionTest.cpp`: `RuneCliSecurityBrokerRegression.BootstrapThenChangePassword` (`rune security bootstrap --password-file` → exit 0; a second run → exit 1 "security is already bootstrapped"; `rune ql --user admin --password-file otp -e "alter user admin set password '…';"` → exit 0; then `rune ql -e "create role r;"` → exit 0), `…CreateUserGrantAndShowGrants` (create user + role + grants via `rune ql -f`; `show grants for user u;` prints `role:r  WRITE  TOPIC  PREFIX 'world.'`).

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** (ScramServer and LoginThrottle as pure, separately tested units; the gateway wiring last). The gateway's SecurityView reference is the control thread's object (same thread — no copy needed).
- [ ] **Step 4: Run** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'LoginThrottle|ScramServer|AuthLogin|ClientLoginBroker|RuneCliSecurityBroker|AuthGates|Gateway|Connect' --output-on-failure -j14` → PASS (every existing gateway / connect test included: they run `InProcessUnauthenticated` → `AuthAnonymous`).
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(auth): SCRAM-SHA-512 server in the session handshake, auth states, version gate, throttle without lockout, auth deadline, dev-mode anonymous sessions, login logs/counters, 5 ms login gate

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Broker — per-session permission tables and the enforcement of every request type

Spec §2 (effective permissions), §2.1 (every row), the visibility rule, §6 ("at authentication, the broker compiles … a per-session permission table … prefix grants resolved against existing names … extended when a CreateTopic / CreateView is applied"), D15–D23; fix-on-sight: `principal.authenticated`.

**Files:**
- Create: `libs/gateway/include/rune/gateway/PermissionTable.h`, `libs/gateway/src/PermissionTable.cpp`, `libs/gateway/include/rune/gateway/ObjectIndex.h`, `libs/gateway/include/rune/gateway/Authorizer.h`, `libs/gateway/src/Authorizer.cpp`, `libs/view/include/rune/view/PlanSources.h` (+ `src/PlanSources.cpp`: the source topic ids of a plan blob, cold)
- Modify: `libs/gateway/src/ClientGateway.cpp` (every handler named below), `ClientGateway.h`, `libs/node/include/rune/node/AdminChannels.h` (`DdlMeta.authenticated`, encoded in every DDL message), `libs/node/src/AdminWorker.cpp` (stamps `principal.authenticated` from it), `BrokerNode.cpp` (the control drain of `kA2cCatalogTopicApplied` / `kA2cCatalogSinkApplied` / `kA2cCatalogTopicDropped` also feeds the gateway's `ObjectIndex`)
- Test: `libs/gateway/tests/PermissionTableTest.cpp`, `libs/node/tests/node/AuthEnforcementTest.cpp`, `libs/node/tests/node/AuthVisibilityTest.cpp`, `libs/node/tests/node/CatalogPrincipalTest.cpp`

**Interfaces:**
```cpp
// ObjectIndex.h (control thread): every live topic / view id → (name, kind), from the catalog effects.
struct ObjectEntry { std::string name; rune::security::ResourceType type = rune::security::ResourceType::Topic; };
class ObjectIndex { public: void onTopic(int32_t id, std::string name); void onView(int32_t sinkId, std::string name);
                    void onDropped(int32_t id); [[nodiscard]] const ObjectEntry* at(int32_t id) const noexcept;
                    template <class F> void forEach(F&&) const; [[nodiscard]] int32_t idLimit() const noexcept; };
// PermissionTable.h — one per authenticated session (control thread).  Hot path: allows() only.
class PermissionTable {
public:
    static PermissionTable allowAll();                                   // AuthAnonymous sessions
    static PermissionTable compile(std::span<const rune::security::GrantSpec> effective, const ObjectIndex&);
    [[nodiscard]] bool allows(int32_t objectId, uint8_t opBits) const noexcept {   // ONE bounds check + ONE load
        return allowAll_ || (static_cast<uint32_t>(objectId) < ops_.size() && (ops_[objectId] & opBits) == opBits);
    }
    [[nodiscard]] bool allowsCluster(uint8_t opBits) const noexcept { return allowAll_ || (clusterOps_ & opBits) == opBits; }
    [[nodiscard]] bool allowsName(rune::security::ResourceType, std::string_view name, uint8_t opBits) const noexcept;  // patterns (cold)
    /// A topic / view created after login (spec §6): set its bits from the retained patterns.  Cold; may grow ops_.
    void onObjectCreated(int32_t id, const ObjectEntry&);
    void onObjectDropped(int32_t id) noexcept;                           // its bits cleared
    [[nodiscard]] std::span<const uint64_t> writeBits() const noexcept;  // Task 13 ships it
private:
    bool allowAll_ = false; uint8_t clusterOps_ = 0;
    std::vector<uint8_t> ops_;                         // by object id; capacity persists
    std::vector<rune::security::GrantSpec> grants_;    // retained for onObjectCreated / allowsName
    std::vector<uint64_t> writeBits_;                  // WRITE bit per topic id, kept in step with ops_
};
// Authorizer.h — the §2.1 table, ONE place (every handler asks it; the matrix's expectations mirror it).
struct Denial { uint16_t code = 45; std::string detail; };            // D23 text
[[nodiscard]] std::optional<Denial> authorizeTopicId(const PermissionTable&, int32_t topicId, rune::security::Operation,
                                                     const ObjectIndex&);   // D22
[[nodiscard]] std::optional<Denial> authorizeName(const PermissionTable&, rune::security::ResourceType, std::string_view,
                                                  rune::security::Operation);
[[nodiscard]] std::optional<Denial> authorizeCluster(const PermissionTable&, rune::security::Operation);
```

**Enforcement map (each handler, before it forwards anything; a denial is answered in the request's own response type — `ProduceResponse`, `SubscribeResponse` / `ErrorResponse`, `JoinGroupResponse`, …, `DefineTopicResponse`, `DdlResponse` — with code 45 and the D23 detail):**

| Handler (anchor in `ClientGateway.cpp`) | Check |
|---|---|
| `handleProduce` (legacy path) | `perms.allows(topicId, WRITE)` — the hot-path check; transactional produce also needs the producer's transactional id WRITE (checked at AddPartitionsToTxn) |
| `handleSubscribe` | −6: `allowsCluster(DESCRIBE)` (D21); −7: `allowsCluster(ADMIN)`; else `allows(topicId, READ)` (D22 for a non-live id) |
| `handleGroupMessage` | JoinGroup / SyncGroup / Heartbeat / OffsetFetch / OffsetCommit: `allowsName(GROUP, groupId, READ)` and READ on every topic the request names; LeaveGroup: nothing (D18) |
| `handleTxnMessage` | InitProducerId with a transactional id: `allowsName(TRANSACTIONAL_ID, id, WRITE)` (none without one, D18); AddPartitionsToTxn: + WRITE on each topic; AddOffsetsToTxn / TxnOffsetCommit: + READ GROUP; EndTxn: WRITE transactional id |
| `handleGetViewRow` / open-view | `allows(viewTopicId, READ)` / `allowsName(VIEW, name, READ)` |
| `handleMetadata` | the answer lists only partitions of topics with `allows(id, DESCRIBE)` (an explicit id list → the same filter) |
| `handleGetTopicSchema` | `allowsName(TOPIC or VIEW, name, DESCRIBE)` (D18) |
| `handleDefineTopic` | `allowsName(TOPIC, name, CREATE)` or `allowsCluster(CREATE)` (CreateTopic and CreateSchema alike) |
| `handleCreateView` / `handleAlterView` | CREATE / ALTER on the view name (or CLUSTER) **and** READ on every source topic id of the plan (`rune::view::planSourceTopicIds`, D19) |
| `handleAlterTopic` / `handleAlterTopicConfig` | `allowsName(TOPIC, name, ALTER)` or `allowsCluster(ALTER)` |
| `handleDropTopic` / `handleDropView` | DROP on the object (or CLUSTER) |
| `handleSecurityDdl` | `allowsCluster(ADMIN)`, except `AlterUser` add / drop / set password on the session's own user (D20) |

Every DDL message the gateway sends to the admin thread carries `DdlMeta.authenticated` = the session's user (`""` for anonymous) — the catalog's and the security log's `principal.authenticated`.

**Compile (at login, cold):** `PermissionTable::compile(effectiveGrants(view.state(), userId), objectIndex)`: for every entry of the index, `ops_[id] = opsOn(grants, entry.type, entry.name)`; `clusterOps_ = clusterOps(grants)`; `writeBits_` from the WRITE bits. A topic / view applied later calls `onObjectCreated` on every authenticated session's table (the "extended for sessions whose prefixes match" rule); a drop calls `onObjectDropped`.

- [ ] **Step 1: Write the failing tests** — `PermissionTableTest.cpp`:

```cpp
#include <rune/gateway/PermissionTable.h>
#include <gtest/gtest.h>
namespace rg = rune::gateway; namespace rs = rune::security;
using rs::Operation; using rs::ResourceType; using rs::PatternKind;
namespace { rs::GrantSpec g(Operation o, ResourceType t, PatternKind k, std::string n) { return {o, {t, k, std::move(n)}}; } }

TEST(PermissionTableRegression, CompilesExactPrefixClusterAndImpliedDescribe) {
    rg::ObjectIndex idx; idx.onTopic(0, "player_events"); idx.onTopic(1, "world.zone1"); idx.onTopic(2, "secret");
    idx.onView(3, "leaderboard");
    const std::vector<rs::GrantSpec> grants = {g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "player_events"),
                                               g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world."),
                                               g(Operation::Read, ResourceType::View, PatternKind::Exact, "leaderboard")};
    auto t = rg::PermissionTable::compile(grants, idx);
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allows(1, rs::opBit(Operation::Write)));
    EXPECT_FALSE(t.allows(2, rs::opBit(Operation::Describe))) << "default deny";
    EXPECT_TRUE(t.allows(3, rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allows(4, rs::opBit(Operation::Read))) << "an id beyond the table";
    EXPECT_FALSE(t.allows(-1, rs::opBit(Operation::Read))) << "a negative (internal) id";
}

TEST(PermissionTableRegression, AnObjectCreatedAfterLoginIsCoveredByAMatchingPrefix) {
    rg::ObjectIndex idx; idx.onTopic(0, "a");
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world.")}}, idx);
    EXPECT_FALSE(t.allows(7, rs::opBit(Operation::Write)));
    t.onObjectCreated(7, {"world.zone9", ResourceType::Topic});
    EXPECT_TRUE(t.allows(7, rs::opBit(Operation::Write)));
    t.onObjectCreated(8, {"other", ResourceType::Topic});
    EXPECT_FALSE(t.allows(8, rs::opBit(Operation::Write)));
    t.onObjectDropped(7);
    EXPECT_FALSE(t.allows(7, rs::opBit(Operation::Write))) << "a dropped id never keeps its bits";
}

TEST(PermissionTableRegression, WriteBitsMirrorTheWriteOps) {
    rg::ObjectIndex idx; for (int i = 0; i < 130; ++i) idx.onTopic(i, "t" + std::to_string(i));
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "t129")}}, idx);
    const auto bits = t.writeBits();
    ASSERT_EQ(bits.size(), 3u);
    EXPECT_EQ(bits[2], uint64_t{1} << (129 - 128));
}

TEST(PermissionTableRegression, AllowAllAllowsEverythingIncludingCluster) {
    auto t = rg::PermissionTable::allowAll();
    EXPECT_TRUE(t.allows(12345, rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allowsCluster(rs::opBit(Operation::Admin)));
}
```

`AuthEnforcementTest.cpp` (`AuthBroker`, a user per test): one `AuthEnforcementRegression.<Row>AllowedAndDenied` test per row of the enforcement map (granted → the request's normal answer; not granted → 45 in its own response type), plus `…CreateViewOverAnUnreadableSourceIsNotAuthorizedRegression`, `…CatalogReadsNeedDescribeOnClusterRegression`, `…SecurityReadsNeedAdminOnClusterRegression`, `…SelfPasswordChangeNeedsNoAdmin`, `…AdminClusterGrantsNoDataAccess`, `…NonTransactionalInitProducerIdNeedsNoGrant`, `…DescribeOnClusterTurnsAMissingIdIntoUnknownTopic` (D22). `AuthVisibilityTest.cpp`: `AuthVisibilityRegression.MissingAndHiddenObjectsAnswerIdenticallyRegression` (for Subscribe-by-id, GetTopicSchema-by-name, DropTopic-by-name and open-view: the answer frame for a hidden existing object equals the one for a missing object, byte for byte apart from header ids), `…MetadataListsOnlyDescribableTopics`. `CatalogPrincipalTest.cpp`: `CatalogPrincipalRegression.DdlEventsRecordTheAuthenticatedUserRegression` (a DefineTopic by `ws1` → the `__catalog` event's `principal.authenticated == "ws1"`; a security DDL by `admin` → `"admin"`).

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** — `PermissionTable` / `ObjectIndex` / `Authorizer` first (unit-tested), then each handler in the order of the map; the `Authorizer` is the only place a code or a detail text is chosen.
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'PermissionTable|AuthEnforcement|AuthVisibility|CatalogPrincipal|AuthLogin|Gateway|Schema|View|Group|Txn' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(auth): per-session permission tables (exact, prefix, cluster, implied describe, created-after-login), enforcement of every request type, visibility, authenticated principal on DDL events

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 13: Broker — worker WRITE bitmaps, the shard-mode produce check, the session-crypto slot fix, the ≤ 2 ns gate, hot-path parity, allocation gates

Spec §6 ("each produce request checks with one bitmask load from the session's table … no allocation, no lock, no shared state … updates arrive through the existing rings"), §12 (≤ 2 ns, zero allocations, hot-path benchmarks unchanged within noise vs authentication disabled), D30; fix-on-sight: `findSessionCrypto_`'s linear scan.

**Files:**
- Create: `libs/node/include/rune/node/WorkerWriteGrants.h`, `libs/node/include/rune/node/PermissionShipper.h` (control thread: resumable per-worker chunk sender, the `SessionAnnouncer` pattern)
- Modify: `libs/node/include/rune/node/WorkerOp.h` (`Type::SessionPermissions = 21`, `Type::PeerLinkKeys = 22` — the latter used in Task 15; both on the session-lifecycle ring: the `t == SessionAnnounce || t == SessionClose` predicate ~`:1028` gains them), `SessionAnnouncer.h` (`grantAll` → the SessionAnnounce wire's free flag bit), `SessionAckSink.h` (per-slot `WriteGrantSlot` and `SessionCryptoSlot` arrays moved by `insert_` / `erase_` like `clientVersion_`), `Worker.h` (`executeSessionPermissionsWire_`, the check in `handleWorkerIngressFrame_` after `pc::Produce::decode`, `sessionCrypto_` → the slot array), `libs/gateway/src/ClientGateway.cpp` + `BrokerNode.cpp` (ship on login / refresh / prefix extension through `PermissionShipper`; `SecurityAnswerGate::markRefreshed` once every worker accepted the chunks), `benchmark/AuthCheckBench.cpp`, `benchmark/CMakeLists.txt`
- Test: `libs/node/tests/node/WorkerWriteGrantsTest.cpp`, `libs/node/tests/node/PermissionShipperTest.cpp`, `libs/node/tests/node/WorkerSessionCryptoRegressionTest.cpp`, `libs/node/tests/node/AuthAllocRegressionTest.cpp` (own binary `rune_auth_alloc_regression_tests`), `libs/node/tests/integration/ShardAuthProduceTest.cpp`

**Interfaces:**
```cpp
// WorkerWriteGrants.h (worker thread) — one slot per SessionAckSink slot.
struct WriteGrantSlot { std::vector<uint64_t> bits; bool grantAll = false; uint64_t epoch = 0; };   // capacity persists
[[nodiscard]] inline bool allowsWrite(const WriteGrantSlot& s, int32_t topicId) noexcept {        // the hot-path check
    const uint32_t t = static_cast<uint32_t>(topicId);
    return s.grantAll | ((t >> 6) < s.bits.size() && ((s.bits[t >> 6] >> (t & 63)) & 1u));
}
struct NoUpdate {};
struct Staging { int64_t sessionId = 0; uint64_t totalWords = 0; std::vector<uint64_t> words; };  // ONE per worker, reused
using PermissionUpdate = std::variant<NoUpdate, Staging>;
// SessionAckSink gains:
//   [[nodiscard]] const WriteGrantSlot* writeGrants(int64_t sessionId) const noexcept;   // find_ (O(1) avg)
//   bool installWriteGrants(int64_t sessionId, std::vector<uint64_t>& words, bool grantAll, uint64_t epoch) noexcept;  // swap, no alloc
//   SessionCryptoSlot* sessionCrypto(int64_t sessionId) noexcept;                        // replaces Worker::findSessionCrypto_
// PermissionShipper.h (control thread)
class PermissionShipper {
public:
    /// Queue `bits` for `sessionId` to every worker (chunks of <= kChunkWords, in order, last flagged); an older
    /// queued update of the same session is replaced (latest wins).  Returns true once every worker accepted it.
    bool ship(WorkerPool&, int64_t sessionId, std::span<const uint64_t> bits, bool grantAll, uint64_t epoch);
    int  retry(WorkerPool&);                         // every runtime cycle; resumable per worker (no duplicate chunk)
    [[nodiscard]] uint64_t fullyShippedEpoch(int64_t sessionId) const noexcept;
    static constexpr std::size_t kChunkWords = 512;
};
```

**Rules:** `SessionAnnounce` carries `grantAll = (authMode != Enabled)` (D30) — so an announced, not-yet-authenticated session in `Enabled` mode has an empty slot and every shard produce from it is answered `ProduceResponse{NotAuthorized}`; after a login (and after every refresh / prefix extension, Task 14) the gateway ships the session's `PermissionTable::writeBits()` with a fresh `epoch`; the `AuthServerFinal{OK}` of a login is sent only once `fullyShippedEpoch(session) >= that epoch` (announce-before-accept for permissions: the client can never produce before the worker knows its grants); the worker stages chunks in its one `Staging` buffer and `installWriteGrants` swaps the buffer into the slot on the last chunk (the old buffer becomes the next staging buffer — no allocation once both reached their peak size). The check in `handleWorkerIngressFrame_` (after the decode, before `getLogAndReplicaForPartition_`): `!allowsWrite(*slot, msg.topicId)` → `produceAnswer_(…)` with `errorCode = NotAuthorized` (the frame is consumed, counted `workerNotAuthorized_`). The gateway's legacy produce path checks `PermissionTable::allows(topicId, WRITE)` (Task 12) — both paths answer the same code and detail.

- [ ] **Step 1: Write the failing tests:**
  - `WorkerWriteGrantsTest.cpp`: `WorkerWriteGrantRegression.UnauthenticatedSessionProduceToWorkerIsDeniedRegression` (a Worker with an announced session, `grantAll = false`, no permissions op: a Produce frame on its ingress → `ProduceResponse{45}`; nothing appended), `…ChunkedUpdateIsInstalledAtomicallyOnTheLastChunk` (3 chunks; after chunk 2 the old table still answers; after chunk 3 the new one), `…GrantAllAllowsEveryTopic`, `…SlotsMoveWithBackwardShiftDeletion` (insert colliding sessions, erase the first, the second keeps its bits), `…DroppedSessionLosesItsGrants`.
  - `PermissionShipperTest.cpp`: `PermissionShipperRegression.AFullRingResumesWithoutDuplicatingAChunk`, `…LatestUpdateReplacesAQueuedOne`, `…FullyShippedEpochAdvancesOnlyWhenEveryWorkerHasIt`.
  - `WorkerSessionCryptoRegressionTest.cpp`: `WorkerSessionCryptoRegression.LookupIsSlotIndexedNotAScanRegression` (4 000 encrypted sessions announced; a produce for the last one opens with one `find_` probe chain — the test seam counts probes ≤ 8; `sessionCryptoEntriesForTest()` keeps its meaning).
  - `ShardAuthProduceTest.cpp` (`AuthBroker` in shard mode + `AuthWireClient` producing straight to the owning worker's ingress endpoint from Metadata): `ShardAuthProduceRegression.GrantedProduceSucceedsAndUngrantedIsNotAuthorized`, `…NoProduceCanPrecedeTheLoginsPermissions` (the client fires a produce the instant `AuthServerFinal{OK}` arrives — always OK, 200 iterations).
  - `AuthAllocRegressionTest.cpp`: `AuthAllocRegression.SteadyStateAuthorizedProduceNeverAllocatesRegression` (the `HotPathBRig.h` rig with `Enabled`, an authenticated session and its shipped grants: 10 000 produces through the worker ingress and 10 000 through the gateway path allocate 0 bytes), `…IdleAuthenticatedSessionAllocatesNothing`.
  - `benchmark/AuthCheckBench.cpp`: `BM_GatewayPermissionTableAllows` (`PermissionTable::allows` on a 10 000-topic table, random topic ids from a pre-generated array), `BM_WorkerAllowsWrite` (`allowsWrite` on a resolved slot), `BM_WorkerSlotLookupPlusAllowsWrite` (the `find_` probe + check, reported).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** (schema ops 21 / 22 already exist from Task 5; the WorkerOp encode / decode cases mirror `SessionAnnounce`'s tail handling; `executeSessionPermissionsWire_` is `[[gnu::cold]]`).
- [ ] **Step 4: Run the tests** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'WorkerWriteGrant|PermissionShipper|WorkerSessionCrypto|ShardAuthProduce|AuthAlloc|SessionAckSink|SessionAnnouncer|Worker|HotPath|ZeroAlloc|Transient' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Gates (Release)** — the check bench and hot-path parity against the same branch with authentication off:

```bash
cmake /Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel && cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel -j14 --target rune_broker_benchmark
B=/Users/vradu/Projects/se-worktrees/rune-broker/build-bench-rel/rune_broker_benchmark
$B --benchmark_filter='PermissionTableAllows|AllowsWrite' --benchmark_repetitions=10 --benchmark_report_aggregates_only=true
# parity: the same hot-path benches with RUNE_BENCH_AUTH=off (InProcessUnauthenticated, grantAll) and =on (Enabled, granted)
for m in off on; do RUNE_BENCH_AUTH=$m $B --benchmark_filter='WorkerProduceLatency|ProducePath|PushRead|FusedRuntimePass' --benchmark_repetitions=5 --benchmark_report_aggregates_only=true; done
```

Expected: `BM_GatewayPermissionTableAllows` and `BM_WorkerAllowsWrite` medians ≤ 2 ns; every hot-path benchmark's median with `on` within ±5 % of `off` (the benches read `RUNE_BENCH_AUTH` in their fixture to build the session as anonymous or authenticated). A regression beyond 5 % is a bug to find and fix here with a `*Regression` test. Record every median in the commit body (`/tmp/auth-gates-msg.txt`, the catalog Task 23 format).

- [ ] **Step 6: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs benchmark
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -F /tmp/auth-gates-msg.txt
```

(first line `feat(auth): worker WRITE bitmaps on the lifecycle ring, shard produce check, slot-indexed session crypto, <=2 ns check, hot-path parity, zero-alloc gates`, a blank line, one line per benchmark, a blank line, the `Co-Authored-By` trailer.)

- [ ] **Checkpoint C (controller):** full `*Regression*` in the broker (plus ASan/UBSan, TSan) — authentication and enforcement are complete end to end from here.

---

### Task 14: Broker — live security changes: refresh, end subscriptions, fail produces, revoke sessions, fail closed on a halt

Spec §6 "Live changes" and "Credential changes" (every bullet), §10 (`SessionRevoked` on every in-flight request and subscription), D24–D26, D28.

**Files:**
- Create: `libs/gateway/include/rune/gateway/LiveSecurityChanges.h`, `libs/gateway/src/LiveSecurityChanges.cpp` (pure: given old / new tables and a session's subscriptions, the list of actions)
- Modify: `libs/gateway/src/ClientGateway.cpp` (the `SecurityView::onApplied` consumer: `Applied.effect` × `affectedUsers` → per-session actions; the user → sessions index; `kA2cSecurityHalted` → fail closed), `ClientGateway.h`, `GroupCoordinationService` (end a member whose group READ was revoked: `removeMember(session, NotAuthorized)`), `BrokerNode.cpp` (counter `1'000'232`, the halt counter `1'000'226`)
- Test: `libs/gateway/tests/LiveSecurityChangesTest.cpp`, `libs/node/tests/node/AuthLiveChangeTest.cpp`, `libs/node/tests/node/SecurityHaltTest.cpp`, `libs/node/tests/client/ClientSessionRevokedBrokerRegressionTest.cpp`

**Interfaces:**
```cpp
struct EndSubscription { int64_t correlationId; int32_t topicId; int32_t partition; int64_t subscriptionId; uint16_t code; };
struct LiveActions { std::vector<EndSubscription> ends; bool endCatalogReads = false, endSecurityReads = false, endGroups = false; };
/// What a refresh does to one session: every live subscription whose object lost READ (−6: DESCRIBE CLUSTER,
/// −7: ADMIN CLUSTER, a group member: the group's READ) ends NotAuthorized.  Pure; the gateway executes it.
[[nodiscard]] LiveActions refreshActions(const PermissionTable& before, const PermissionTable& after,
                                         const Session& s, const ObjectIndex&);
```

**Rules (control thread, per applied event, in the `SecurityView::onApplied` drain):**
- `SessionEffect::Refresh` for user u: every `AuthAuthenticated` session of u recompiles its table from `effectiveGrants`; `refreshActions(old, new)`: each ended subscription gets `Unsubscribe` to its owning worker and `ErrorResponse{NotAuthorized, D23 detail}` on its correlation id — **then** no further `Records` (the worker's Unsubscribe is ordered before any later push on its ring; the gateway drops a racing push of an ended subscription id); `endCatalogReads` / `endSecurityReads` → `kMsgCatalogUnsubscribe` / `kMsgSecurityUnsubscribe` for the session + `ErrorResponse{45}` on their correlation ids; `endGroups` → the coordinator removes the member (its next Heartbeat / Commit / Fetch answers 45); the new write bits are shipped (Task 13) — a revoked WRITE fails the next produce on both paths; an `AuthPasswordChangeRequired` session whose user's `mustChangePassword` cleared becomes `AuthAuthenticated` (the Task 11 transition, now through this one path).
- `SessionEffect::Revoke` for user u: every session of u: each live subscription `ErrorResponse{SessionRevoked}` on its correlation id, then `ErrorResponse{SessionRevoked, corr 0, detail "user disabled" | "user dropped" | "password changed (TERMINATE SESSIONS)"}`, then the existing close path (`closeSession_`: SessionClose to every worker, catalog / security subscriptions released); counter `232`; log `auth: session <id> of user <u> revoked: <reason>`.
- A catalog `CreateTopic` / view sink applied → `onObjectCreated` on every authenticated session's table, and the changed sessions' write bits shipped.
- The answer gate: after the actions of event offset N were executed and every shipped table accepted by every worker, `SecurityAnswerGate::release(N)` (D24).
- **Halt (D28):** `kA2cSecurityHalted` (or a `SecurityView` apply error) → every `AuthAuthenticated` / `AuthPasswordChangeRequired` session is revoked (`SessionRevoked`, detail "security log halted on this broker"); new `AuthClientFirst`s answer `AuthServerFinal{44}`; security DDL answers `Internal`; `spdlog::critical("SECURITY APPLY HALTED at offset N: <describe>; this broker refuses logins until repaired (docs/SECURITY.md#halted)")`; counter `226` = 1.

- [ ] **Step 1: Write the failing tests** — `LiveSecurityChangesTest.cpp` (pure): `LiveSecurityChangesRegression.LostReadEndsThatSubscriptionOnly`, `…LostDescribeClusterEndsCatalogReads`, `…LostAdminEndsSecurityReads`, `…LostGroupReadEndsTheGroup`, `…GainedGrantsEndNothing`. `AuthLiveChangeTest.cpp` (`AuthBroker`, `AuthWireClient`s, admin via the wire):

```cpp
TEST(AuthLiveChangeRegression, RevokedReadEndsLiveSubscriptionsRegression) {
    AuthBroker b; b.start();
    b.admin().ql("CREATE ROLE r; GRANT READ ON TOPIC PREFIX 'w.' TO ROLE r; CREATE USER u WITH PASSWORD 'long password 1'; GRANT ROLE r TO USER u;");
    const int32_t t = b.createTopic("w.events", 1);
    AuthWireClient c = b.login("u", "long password 1");
    auto sub = c.subscribe(t, 0, /*from*/ -2);
    b.produce(t, 0, 10);                                   // an admin-side producer with WRITE
    ASSERT_TRUE(c.awaitRecords(sub, 10, 5s));
    b.admin().ql("REVOKE READ ON TOPIC PREFIX 'w.' FROM ROLE r;");   // returns after the answer gate (D24)
    auto end = c.awaitError(sub.correlationId, 2s);
    ASSERT_TRUE(end.has_value());
    EXPECT_EQ(end->code, 45);
    b.produce(t, 0, 10);
    EXPECT_FALSE(c.awaitRecords(sub, 11, 500ms)) << "nothing is delivered after the end";
    EXPECT_EQ(c.subscribe(t, 0, -2).answerCode, 45) << "a new Subscribe is refused";
}
```

and in the same style: `…RevokedWriteFailsTheNextGatewayProduceRegression`, `…RevokedWriteFailsTheNextShardProduceRegression`, `…RoleRevokeRefreshesEveryMember` (3 users of one role, one revoke, all three lose it), `…DropRoleRefreshesMembers`, `…GrantArrivesWithoutReconnect`, `…PrefixGrantCoversATopicCreatedAfterLogin`, `…DisableRevokesEnableAllowsLogin` (live subscription → 46, session-level 46, `UnknownSession` to a Ping; login → 44 while disabled; ENABLE → login OK), `…DropUserRevokesEverySession` (two sessions of the user, both revoked), `…SetPasswordKeepsSessions` (live session keeps receiving; the old password's new login → 44, the new one OK), `…SetPasswordTerminateRevokes`, `…CatalogReadEndsWhenDescribeClusterIsRevoked`, `…GroupMemberLosingGroupReadIsRemovedWithNotAuthorized`, `…TheGrantOkIsEnforcedByTheAnsweringBrokerImmediately` (GRANT then, with zero delay, a produce on the same broker → OK, 100 iterations; REVOKE then produce → 45, 100 iterations — D24). `SecurityHaltTest.cpp`: `SecurityHaltRegression.HaltRevokesEverySessionAndRefusesLoginsRegression` (the applier's `tamperNextFingerprintForTest`, then any security DDL: every authenticated session is revoked with the halt detail, a new login answers 44, a security DDL answers `Internal`, counter `226` = 1; the single-node broker stays up and keeps serving nothing to anyone — fail closed; the repair-by-refetch path needs a peer and is the 3-node `AuthClusterRegression.HaltedBrokerRefetchesSecurityAfterItsDirIsMovedAside` in Task 19). `ClientSessionRevokedBrokerRegressionTest.cpp` (the real client): `ClientSessionRevokedBrokerRegression.InFlightRequestsAndSubscriptionsCompleteSessionRevokedAndNothingReconnects`.
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'LiveSecurityChanges|AuthLiveChange|SecurityHalt|ClientSessionRevoked|AuthEnforcement|AuthLogin|Group' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(auth): live security changes - refresh tables, end revoked subscriptions/groups/reads explicitly, fail revoked produces, SessionRevoked on disable/drop/terminate, fail closed on a security halt

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 15: Broker — peer links: the mutual HMAC challenge-response, link keys, rotation, distribution to every transport owner

Spec §8 (every bullet), §10 ("refused at the peer connection, logged on both sides"), D33, D35. No `__security` dependency: a new broker authenticates before it has replicated anything.

**Files:**
- Create: `libs/peerlink/include/rune/peerlink/PeerLinkMessages.h`, `PeerHandshake.h`, `PeerLinkKeys.h`, `libs/peerlink/src/{PeerHandshake,PeerLinkKeys}.cpp`, `libs/peerlink/tests/{PeerHandshakeTest,PeerLinkKeysTest}.cpp`
- Create: `libs/node/include/rune/node/PeerLinkAgent.h`, `libs/node/src/PeerLinkAgent.cpp` (control thread: one subscription on `<self consensus endpoint>` stream `peerAuthStreamId`, one publication per peer to its consensus endpoint, drives one `PeerHandshake` per peer)
- Modify: `BrokerNode.{h,cpp}` (build the agent when `authMode == Enabled` and peers > 1; drive it in the runtime duty cycle; distribute keys), `Worker.h` (`executePeerLinkKeysWire_` → the worker's `PeerFrameCrypto`, Task 16), `AdminWorker.cpp` / `AdminChannels.h` (`kMsgPeerLinkKeys` → the admin plane's `PeerFrameCrypto`), `RoleCounter.h` (`1'000'233`, `234`)
- Test: `libs/node/tests/integration/PeerLinkAgentTest.cpp` (two / three in-process nodes, peer-auth stream only)

**Interfaces (namespace `rune::peerlink`):**
```cpp
// PeerLinkMessages.h — hand-rolled LE, internal broker protocol (never seen by clients).
enum class PeerMsg : uint8_t { Hello = 1, Challenge = 2, Proof = 3, Accept = 4, Refuse = 5 };
struct PeerHello     { int32_t from = -1, to = -1; uint64_t keyId = 0; uint64_t incarnation = 0; std::array<uint8_t, 32> nonceA{}; };
struct PeerChallenge { int32_t from = -1, to = -1; uint64_t keyId = 0; uint64_t incarnation = 0; std::array<uint8_t, 32> nonceB{};
                       std::array<uint8_t, 64> macB{}; };
struct PeerProof     { int32_t from = -1, to = -1; std::array<uint8_t, 64> macA{}; };
struct PeerAccept    { int32_t from = -1, to = -1; uint32_t linkEpoch = 0; };
enum class RefuseReason : uint8_t { NoCommonKey = 1, BadProof = 2, StaleIncarnation = 3, NotAPeer = 4 };
struct PeerRefuse    { int32_t from = -1, to = -1; RefuseReason reason{}; };
// encode(buf) / decode(buf, len) per struct; a 1-byte PeerMsg tag leads every frame.

// The transcripts both MACs cover (fixed order, LE):
//   macB = HMAC-SHA-512(key.master, "rune-peer-v1|B" ‖ from=B ‖ to=A ‖ nonceA ‖ nonceB ‖ incA ‖ incB ‖ keyId)
//   macA = HMAC-SHA-512(key.master, "rune-peer-v1|A" ‖ from=A ‖ to=B ‖ nonceB ‖ nonceA ‖ incB ‖ incA ‖ keyId)
// Distinct labels make a reflected MAC useless; both nonces make a replayed Hello / Proof useless.

// PeerLinkKeys.h (D33)
struct LinkKeys { uint32_t epoch = 0; std::array<uint8_t, 32> sendMaster{}, recvMaster{}; };
[[nodiscard]] std::optional<LinkKeys> deriveLinkKeys(const rune::crypto::ClusterKeyMaterial& key, int32_t self, int32_t peer,
                                                     bool selfIsInitiator, std::span<const uint8_t, 32> nonceA,
                                                     std::span<const uint8_t, 32> nonceB, uint32_t epoch);
//   send = HKDF(SHA-512, ikm master, salt nonceA‖nonceB, info "rune-link|<self>-><peer>"), recv = the mirror label.

// PeerHandshake.h — one per peer, pure (clock injected, frames out through a sink), rule 3:
struct LinkIdle      { int64_t retryAtNs = 0; };
struct HelloSent     { std::array<uint8_t, 32> nonceA{}; uint64_t keyId = 0; int64_t sentNs = 0; };
struct ChallengeSent { std::array<uint8_t, 32> nonceA{}, nonceB{}; uint64_t keyId = 0; uint64_t peerIncarnation = 0; int64_t sentNs = 0; };
struct Established   { LinkKeys keys; uint64_t peerIncarnation = 0; int64_t sinceNs = 0; };
struct Refused       { RefuseReason reason{}; int64_t retryAtNs = 0; };
using LinkState = std::variant<LinkIdle, HelloSent, ChallengeSent, Established, Refused>;
class PeerHandshake {
public:
    PeerHandshake(int32_t self, int32_t peer, uint64_t selfIncarnation, const ClusterKeys& keys);
    /// Initiator (self < peer) sends Hello from LinkIdle / Refused when due (retry 200 ms).  Every handler
    /// returns the frame to send (if any) and, on Established (or a new epoch), the keys to distribute.
    struct Out { std::optional<std::vector<uint8_t>> frame; std::optional<LinkKeys> established; std::optional<std::string> log; };
    Out tick(int64_t nowNs);
    Out onHello(const PeerHello&, int64_t nowNs);       // responder
    Out onChallenge(const PeerChallenge&, int64_t nowNs);
    Out onProof(const PeerProof&, int64_t nowNs);
    Out onAccept(const PeerAccept&, int64_t nowNs);
    Out onRefuse(const PeerRefuse&, int64_t nowNs);
    void onUnknownEpoch(uint32_t epoch, int64_t nowNs); // a frame named an epoch we do not hold → re-handshake (initiator)
    [[nodiscard]] const LinkState& state() const noexcept;
};
```

**Rules:**
- The lower id initiates; the responder answers a `Hello` whose `keyId` it holds (current or next — D35) with a `Challenge` under that key; a `keyId` it holds neither of → `Refuse{NoCommonKey}`; the initiator verifies `macB` (constant time; a mismatch → `Refused{BadProof}`, logged, retried) and answers `Proof`; the responder verifies `macA` → `Accept{epoch}` (epoch = responder's counter + 1) and both derive `LinkKeys`.
- A `Hello` / `Challenge` carrying an incarnation lower than the one already established for that peer → `Refuse{StaleIncarnation}` (a replayed old handshake can never displace a live link); a higher one re-establishes (the peer restarted) — the new epoch replaces the old keys everywhere.
- Every refusal logs on **both** sides: the refuser `peer link from node N REFUSED: <reason> (key id <hex>)` at error, the refused side `peer link to node N REFUSED by N: <reason>` at error; counter `234` on both; `Established` logs `peer link with node N established (epoch E, key id <hex>)` at info, counter `233` = established peers.
- **Distribution (D33):** on `established`, the control thread (same thread as the agent) calls the consensus sink / ingress and the legacy `AeronPartitionTransport` directly (they are control-thread objects), posts `WorkerOp::Type::PeerLinkKeys` to every worker (lifecycle ring; resumable like `SessionAnnouncer`), and `kMsgPeerLinkKeys` to the admin thread. Until a link is established its frames are not sent (Task 16); the old epoch's keys stay valid at the receivers for 2 s after a new epoch (in-flight frames), then are dropped.
- The agent runs only for `Enabled` with more than one peer; `InProcessUnauthenticated` / `DevDisabled` build no agent and every transport stays `PeerPlain`.

- [ ] **Step 1: Write the failing tests** — `PeerHandshakeTest.cpp` (two `PeerHandshake`s wired back to back in memory, a fake clock):

```cpp
#include <rune/peerlink/PeerHandshake.h>
#include <gtest/gtest.h>
namespace pl = rune::peerlink;
namespace {
pl::ClusterKeys keys(char c) { std::string k(48, c); auto m = rune::crypto::deriveClusterKey({reinterpret_cast<const uint8_t*>(k.data()), k.size()}); return {*m, std::nullopt}; }
struct Pair {   // shuttles frames until quiet; returns what each side established
    pl::PeerHandshake a, b;
    std::optional<pl::LinkKeys> ka, kb; std::vector<std::string> logsA, logsB;
    Pair(pl::ClusterKeys x, pl::ClusterKeys y) : a(1, 2, 10, x), b(2, 1, 20, y) {}
    void run(int64_t now = 0) { /* a.tick → b.onHello → a.onChallenge → b.onProof → a.onAccept, dispatching by PeerMsg tag; collect established + logs */ }
};
} // namespace

TEST(PeerHandshakeRegression, SameKeyEstablishesMatchingLinkKeys) {
    Pair p(keys('k'), keys('k')); p.run();
    ASSERT_TRUE(p.ka && p.kb);
    EXPECT_EQ(p.ka->sendMaster, p.kb->recvMaster);
    EXPECT_EQ(p.ka->recvMaster, p.kb->sendMaster);
    EXPECT_NE(p.ka->sendMaster, p.ka->recvMaster) << "one key per direction";
}

TEST(PeerLinkRegression, WrongKeyIsRefusedAndLoggedOnBothSidesRegression) {
    Pair p(keys('k'), keys('x')); p.run();
    EXPECT_FALSE(p.ka || p.kb);
    EXPECT_TRUE(std::holds_alternative<pl::Refused>(p.a.state()));
    ASSERT_FALSE(p.logsA.empty()); ASSERT_FALSE(p.logsB.empty());
    EXPECT_NE(p.logsA.back().find("REFUSED by 2"), std::string::npos);
    EXPECT_NE(p.logsB.back().find("REFUSED"), std::string::npos);
}

TEST(PeerHandshakeRegression, TheNextKeyIsAcceptedForRotation) {
    pl::ClusterKeys rolled = keys('n'); rolled.next = keys('k').current;   // step 2 of §8: current = next, old kept
    Pair p(keys('k'), rolled); p.run();
    EXPECT_TRUE(p.ka && p.kb) << "the responder accepts the initiator's (old) current key as its next";
}

TEST(PeerHandshakeRegression, AReplayedHelloOrProofNeverEstablishes) {
    // capture a full exchange's frames; feed the Hello and Proof again to a fresh responder: its new nonceB makes
    // the old macA wrong → Refuse{BadProof}; a Hello with an older incarnation → Refuse{StaleIncarnation}.
}

TEST(PeerHandshakeRegression, AReflectedChallengeMacIsNotAValidProof) {
    // send the responder its own macB as a Proof → BadProof (distinct transcript labels).
}

TEST(PeerHandshakeRegression, RefusedRetriesEvery200ms) {
    // after a refusal, tick() sends nothing before +200 ms and a fresh Hello (new nonce) at +200 ms.
}

TEST(PeerHandshakeRegression, APeerRestartWithAHigherIncarnationReestablishesWithANewEpoch) {
    // establish (epoch 1); rebuild b with incarnation 21; b's Hello-side frames carry 21 → a re-runs the exchange,
    // both establish epoch 2 with keys != epoch 1's; a Hello carrying incarnation 19 afterwards → Refuse{StaleIncarnation}.
}
TEST(PeerHandshakeRegression, AnUnknownEpochFrameTriggersARehandshake) {
    // establish; a.onUnknownEpoch(7) → a sends a fresh Hello (new nonce) at once; the exchange yields epoch 2.
}
```

(Write every listed test body in full — each comment states its steps and assertions.) `PeerLinkKeysTest.cpp`: `PeerLinkKeysRegression.DerivationIsDirectionalAndNonceBound`. `PeerLinkAgentTest.cpp` (3 in-process nodes with real Aeron on the peer-auth stream only): `PeerLinkAgentRegression.EveryPairEstablishesWithinTwoSeconds`, `…AWrongKeyNodeIsRefusedByEveryPeerAndLogsIt`, `…KeysReachEveryWorkerAndTheAdminThread` (test probes: each worker's and the admin plane's crypto knows epoch E for each peer).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** (handshake + keys pure first, then the agent).
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'PeerHandshake|PeerLink' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(peerlink): mutual HMAC challenge-response under the cluster key, directional link keys, current/next rotation, refusals logged on both sides, keys distributed to every transport owner

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 16: Broker — AEAD on every inter-broker frame (consensus, replication, admin plane), unauthenticated-peer drops, frame limits

Spec §8 ("inter-broker links use the same transport encryption as clients"; "a broker without a valid key cannot join"), D34; spec gap G1 (no inter-broker encryption existed).

**Files:**
- Create: `libs/peerlink/include/rune/peerlink/PeerFrameCrypto.h`, `libs/peerlink/src/PeerFrameCrypto.cpp`, `libs/peerlink/tests/PeerFrameCryptoTest.cpp`, `benchmark/PeerFrameBench.cpp`
- Modify: `libs/consensus/include/rune/consensus/UdpMessageSink.h` (seal before `offer`), `AeronIngress.h` (open after `assembler_.onFragment`, before `ingressRing_.write`), `libs/replication/include/rune/replication/AeronPartitionTransport.h` + `src/AeronPartitionTransport.cpp` (seal on every send, open in `onFragment` ~`:628`), `libs/replication/include/rune/replication/WorkerReplicationTransport.h` (seal on every offer / tryClaim path, open in the handler ~`:847` before dispatch — used by every worker and the admin plane), `ReplicationFrameLimits.h` (every limit − `kPeerFrameOverhead`), `BrokerNode.cpp` (each transport built `PeerPlain` or `PeerSealed`), `RoleCounter.h` (`1'000'235`)
- Test: `libs/replication/tests/PeerSealedTransportTest.cpp`, `libs/consensus/tests/PeerSealedConsensusTest.cpp`, `libs/node/tests/node/PeerFrameAllocRegressionTest.cpp`, `libs/node/tests/node/ReplicationFrameLimitsRegressionTest.cpp` (append)

**Interfaces (namespace `rune::peerlink`):**
```cpp
inline constexpr std::size_t kPeerHeaderBytes = 16, kPeerFrameOverhead = kPeerHeaderBytes + rune::crypto::kTagSize;   // 32
inline constexpr uint8_t kPeerFrameMagic = 0xB7;
// Frame: [magic][senderId u8][fromNode u16][linkEpoch u32][seq u64][ciphertext][tag 16]; AAD = header ‖ toNode u16.
struct PeerPlain {};
class PeerFrameCrypto {             // one per transport instance; owned by that transport's thread
public:
    PeerFrameCrypto(int32_t selfNode, uint8_t senderId);          // D33 sender ids: consensus 0, legacy 1, admin 2, worker w → 3+w
    void installLink(int32_t peer, const LinkKeys& k, int64_t nowNs);   // derives this sender's send key + every sender's recv key
    /// Seal `plain` for `peer` into `out` (cap >= len + 32).  0 = no established link (the caller drops + counts).
    [[nodiscard]] std::size_t seal(int32_t peer, const uint8_t* plain, std::size_t len, uint8_t* out, std::size_t cap) noexcept;
    /// In place: plaintext written at frame+16 … (the caller encoded it there); returns the sealed length or 0.
    [[nodiscard]] std::size_t sealInPlace(int32_t peer, uint8_t* frame, std::size_t plainLen, std::size_t cap) noexcept;
    enum class OpenResult : uint8_t { Ok, NotPeerFrame, UnknownPeer, UnknownEpoch, BadTag, Replay };
    /// Open into `out`; `fromNode` reports who sent it.  Never dispatches anything that did not verify.
    [[nodiscard]] OpenResult open(const uint8_t* frame, std::size_t len, uint8_t* out, std::size_t cap,
                                  std::size_t& outLen, int32_t& fromNode, uint32_t& epoch) noexcept;
    [[nodiscard]] uint64_t drops() const noexcept;
};
using PeerCryptoMode = std::variant<PeerPlain, PeerFrameCrypto>;   // chosen at transport construction (rule 3)
```

**Rules:**
- Every frame a transport sends to a peer is sealed **whole** (before Aeron fragmentation) and every frame it receives is opened **after** reassembly; the seq is the sender's per-(peer) counter; the receiver keeps a 64-wide replay window per (peer, senderId, epoch) — the client `CryptoState` window logic, reused verbatim.
- No established link to the destination → the frame is not sent; `drops` + counter `235`; log `peer frames to node N dropped: link not established` (rate-limited, one line per 1024). A received frame that does not open → dropped, counted, logged `frame from node N dropped: <OpenResult>` (rate-limited); `UnknownEpoch` at a worker or the admin plane is only counted: the control thread's consensus ingress (the agent's own thread) receives the same peer's continuous heartbeats under the same unknown epoch and calls `PeerHandshake::onUnknownEpoch` directly — no new ring message is needed.
- `ReplicationFrameLimits.h`: `maxFetchRecordsViaOutboundRing` / `ViaPublication` and every limit derived from an Aeron max message length subtract `kPeerFrameOverhead` **in every mode** (D34) — `NodeConfig::produceIntakeLimit()` shrinks by at most 32 bytes; the release note says so.
- Hot path: `seal` / `open` use preallocated per-transport scratch sized to the transport's max message (+ 32) — no allocation; `PeerPlain` is byte-identical to today (every existing replication / consensus test passes unchanged).

- [ ] **Step 1: Write the failing tests** — `PeerFrameCryptoTest.cpp`:

```cpp
#include <rune/peerlink/PeerFrameCrypto.h>
#include <gtest/gtest.h>
namespace pl = rune::peerlink;
namespace {
pl::LinkKeys linkFor(bool aToB) { pl::LinkKeys k; k.epoch = 1; k.sendMaster.fill(aToB ? 1 : 2); k.recvMaster.fill(aToB ? 2 : 1); return k; }
} // namespace

TEST(PeerFrameRegression, SealedFramesOpenOnlyAtTheirPeerAndOnlyOnce) {
    pl::PeerFrameCrypto a(1, /*sender*/ 3), b(2, /*sender*/ 0);
    a.installLink(2, linkFor(true), 0); b.installLink(1, linkFor(false), 0);
    const std::string msg = "FetchResponse bytes";
    std::vector<uint8_t> wire(msg.size() + pl::kPeerFrameOverhead), plain(msg.size());
    const auto n = a.seal(2, reinterpret_cast<const uint8_t*>(msg.data()), msg.size(), wire.data(), wire.size());
    ASSERT_EQ(n, msg.size() + 32);
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    ASSERT_EQ(b.open(wire.data(), n, plain.data(), plain.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Ok);
    EXPECT_EQ(from, 1); EXPECT_EQ(std::string(plain.begin(), plain.begin() + outLen), msg);
    EXPECT_EQ(b.open(wire.data(), n, plain.data(), plain.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::Replay);
    wire[20] ^= 1;
    EXPECT_EQ(b.open(wire.data(), n, plain.data(), plain.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::BadTag);
}

TEST(PeerFrameRegression, FramesFromAnUnauthenticatedPeerAreDroppedAndCountedRegression) {
    pl::PeerFrameCrypto rogue(9, 0), b(2, 0);
    rogue.installLink(2, linkFor(true), 0);           // keys b never agreed to
    std::vector<uint8_t> wire(64), out(64);
    const auto n = rogue.seal(2, reinterpret_cast<const uint8_t*>("hello"), 5, wire.data(), wire.size());
    std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(b.open(wire.data(), n, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::UnknownPeer);
    EXPECT_EQ(b.drops(), 1u);
    EXPECT_EQ(b.seal(5, reinterpret_cast<const uint8_t*>("x"), 1, wire.data(), wire.size()), 0u) << "no link → never sent";
}

TEST(PeerFrameRegression, PlaintextFramesAreNotPeerFrames) {
    pl::PeerFrameCrypto b(2, 0);
    const uint8_t sbe[24] = {0x10, 0x00};             // an SBE header, magic byte != 0xB7
    std::vector<uint8_t> out(64); std::size_t outLen = 0; int32_t from = -1; uint32_t epoch = 0;
    EXPECT_EQ(b.open(sbe, sizeof sbe, out.data(), out.size(), outLen, from, epoch), pl::PeerFrameCrypto::OpenResult::NotPeerFrame);
}

TEST(PeerFrameRegression, SealInPlaceEqualsSeal) {
    // the same plaintext sealed by seal() and by sealInPlace() (plaintext pre-written at frame + 16) opens to the
    // same bytes; both lengths are len + 32.
}
TEST(PeerFrameRegression, EachSenderHasItsOwnReplayWindow) {
    // node 1's senders 0 and 3 interleave seq 1..100 to node 2: all 200 frames open; a replay of any one → Replay.
}
TEST(PeerFrameRegression, AnOldEpochStaysValidForTwoSecondsThenIsRefused) {
    // install epoch 1, seal a frame; install epoch 2 at t; the epoch-1 frame opens at t + 1.9 s and is
    // UnknownEpoch at t + 2.1 s.
}
```

(Every empty-bodied test is written in full; its name and comment are its contract.) `PeerSealedTransportTest.cpp` (two `WorkerReplicationTransportImpl` over real Aeron IPC with `PeerFrameCrypto` installed): `PeerSealedTransportRegression.FetchRoundTripsSealed`, `…NoLinkMeansNothingIsSentAndItIsCounted`, `…AWrongKeyPeerIsDroppedNeverDispatched`; the same three for `AeronPartitionTransport`. `PeerSealedConsensusTest.cpp`: `PeerSealedConsensusRegression.ElectionCompletesOverSealedFrames` (3 consensus agents, links pre-installed), `…AKeylessNodeNeverAffectsTheTerm` (a node whose frames do not open cannot start an election that the others observe). `PeerFrameAllocRegressionTest.cpp`: `PeerFrameAllocRegression.SteadySealedReplicationAllocatesNothing`. `ReplicationFrameLimitsRegressionTest.cpp`: `ReplicationFrameLimitsRegression.EveryLimitLeavesRoomForThePeerFrameOverhead` (the largest frame each limit admits, sealed, still fits the transport's max message).
- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** — `PeerFrameCrypto` first (unit-tested), then each transport behind `PeerCryptoMode` (`std::visit` once per frame is one predictable branch; the `PeerPlain` path compiles to today's code), then the limits, then the BrokerNode wiring (each transport's constructor receives its mode; a worker / the admin plane gets its links through Task 15's distribution).
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'PeerFrame|PeerSealed|ReplicationFrameLimits|Replication|Consensus|Election|Catalog|Security' --output-on-failure -j14` → PASS; then the bench `build-bench-rel/rune_broker_benchmark --benchmark_filter='PeerFrame' --benchmark_repetitions=5` (`BM_PeerFrameSeal16K`, `BM_PeerFrameOpen16K`; record the medians — no gate, they inform the 3-node replication latency recorded in Task 19).
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs benchmark
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "feat(peerlink): AEAD on every inter-broker frame (consensus, replication, admin plane) with per-sender keys and replay windows; unauthenticated peers dropped and counted; frame limits leave room for the overhead

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint D (controller):** full `*Regression*` in the broker (plus ASan/UBSan, TSan).

---
### Task 17: Interaction matrix — the expectation table, the rig, the 720-cell parameterized matrix

Spec §11 "Request type × permission state" (every state: no grant, exact, prefix, via a role, revoked mid-request / mid-subscription, object created after login) and the project's op × entity × timing rule. The section "Interaction matrix" above is the contract; this task makes it executable.

**Files:**
- Create: `libs/node/tests/support/AuthMatrixExpectations.h`, `libs/node/tests/support/AuthMatrixRig.h`, `libs/node/tests/node/AuthInteractionMatrixRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (`rune_auth_matrix_tests`; `gtest_discover_tests(... DISCOVERY_TIMEOUT 60)`; label `matrix`)

**Interfaces — `AuthMatrixExpectations.h` (complete; the single source of truth):**

```cpp
#pragma once
// AuthMatrixExpectations.h — authentication spec §11: the request × grant × timing matrix (720 cells).
// Read by the unit matrix (Task 17), the stress (Task 18), the 3-node slice (Task 19) and the docker e2e (Task 20).
#include <array>
#include <cstdint>
#include <optional>
#include <string_view>

namespace rune::node::test::amatrix {

enum class Op : uint8_t { Produce, ProduceRouted, TxnProduce, TxnInit, TxnEnd, Subscribe, GroupJoin, OffsetCommit,
                          OffsetFetch, ViewRead, ViewOpen, Metadata, CreateTopic, CreateView, CreateSchema, AlterSchema,
                          SetCompatibility, AlterTopicConfig, AlterView, DropTopic, DropView, CatalogRead, SecurityDdl,
                          SecurityRead, kCount };
enum class Grant : uint8_t { None, Exact, Prefix, ViaRole, Cluster, kCount };
enum class Timing : uint8_t { Steady, RevokedMid, CreatedAfterLogin, GrantedAfterLogin, ObjectMissing, UserDisabledMid, kCount };
enum class Outcome : uint8_t { NotApplicable, Denied, FilteredOut, Missing, EndedNotAuthorized, SessionRevoked, DeniedThenOk, Ok };
enum class NaRule : uint8_t { None, NA1, NA2, NA3, NA4, NA5, NA6 };
inline constexpr int16_t kNone = -1;

[[nodiscard]] constexpr bool needsReadOrWrite(Op o) noexcept { return static_cast<uint8_t>(o) <= static_cast<uint8_t>(Op::ViewOpen); }
[[nodiscard]] constexpr bool clusterOnly(Op o) noexcept { return o == Op::SecurityDdl || o == Op::SecurityRead; }
[[nodiscard]] constexpr bool noTarget(Op o) noexcept { return o == Op::CatalogRead || clusterOnly(o); }
[[nodiscard]] constexpr bool streaming(Op o) noexcept {
    return o == Op::Subscribe || o == Op::GroupJoin || o == Op::CatalogRead || o == Op::SecurityRead;
}

[[nodiscard]] constexpr NaRule naRule(Op o, Grant g, Timing t) noexcept {
    if (g == Grant::Cluster && needsReadOrWrite(o)) return NaRule::NA1;
    if ((g == Grant::Exact || g == Grant::Prefix) && clusterOnly(o)) return NaRule::NA2;
    if (g == Grant::None && (t == Timing::RevokedMid || t == Timing::GrantedAfterLogin || t == Timing::UserDisabledMid)) return NaRule::NA3;
    if ((t == Timing::CreatedAfterLogin || t == Timing::ObjectMissing) && noTarget(o)) return NaRule::NA4;
    if (t == Timing::CreatedAfterLogin && (o == Op::CreateTopic || o == Op::CreateView)) return NaRule::NA5;
    if (t == Timing::ObjectMissing && (o == Op::TxnEnd || o == Op::CreateSchema)) return NaRule::NA6;
    return NaRule::None;
}

/// R3's per-op answer for a granted request on a missing object.
struct MissingAnswer { Outcome outcome; int16_t code; int16_t viewStatus; };
[[nodiscard]] constexpr MissingAnswer missingAnswer(Op o) noexcept {
    switch (o) {
        case Op::Produce: case Op::ProduceRouted: case Op::TxnProduce: case Op::Subscribe: case Op::ViewRead:
            return {Outcome::Denied, 45, kNone};                 // id-addressed (D22)
        case Op::TxnInit: case Op::GroupJoin: case Op::OffsetCommit: case Op::OffsetFetch:
            return {Outcome::Ok, 0, kNone};                      // implicit objects
        case Op::ViewOpen: return {Outcome::Missing, 0, 4};      // ViewRowResponse status 4 UnknownView
        case Op::Metadata: return {Outcome::FilteredOut, 0, kNone};
        case Op::CreateTopic: case Op::CreateView: return {Outcome::Ok, 0, kNone};
        default: return {Outcome::Missing, 7, kNone};             // UnknownTopic (name-addressed alter / drop)
    }
}

[[nodiscard]] constexpr Outcome expect(Op o, Grant g, Timing t) noexcept {
    if (naRule(o, g, t) != NaRule::None) return Outcome::NotApplicable;
    if (g == Grant::None) return o == Op::Metadata ? Outcome::FilteredOut : Outcome::Denied;                // R1
    if (o == Op::CatalogRead && (g == Grant::Exact || g == Grant::Prefix)) return Outcome::Denied;           // R2
    if (t == Timing::ObjectMissing) return missingAnswer(o).outcome;                                        // R3
    if (t == Timing::RevokedMid) return Outcome::EndedNotAuthorized;                                        // R4
    if (t == Timing::UserDisabledMid) return Outcome::SessionRevoked;                                       // R5
    if (t == Timing::GrantedAfterLogin) return Outcome::DeniedThenOk;                                       // R6
    return Outcome::Ok;                                                                                     // R7
}

/// Every code a cell asserts.  first = the first answer; second = the answer after the change (RevokedMid,
/// GrantedAfterLogin, UserDisabledMid) or kNone; reconnect = the code of a reconnect attempt (UserDisabledMid) or kNone.
struct ExpectedCodes { int16_t first = kNone; int16_t second = kNone; int16_t reconnect = kNone; int16_t viewStatus = kNone;
                       bool presentFirst = false, presentSecond = false; };   // Metadata presence
[[nodiscard]] constexpr ExpectedCodes expectedCodes(Op o, Grant g, Timing t) noexcept {
    const Outcome out = expect(o, g, t);
    const bool meta = o == Op::Metadata;
    switch (out) {
        case Outcome::NotApplicable:      return {};
        case Outcome::Denied:             return {.first = meta ? int16_t{0} : int16_t{45}};
        case Outcome::FilteredOut:        return {.first = 0, .presentFirst = false};
        case Outcome::Missing:            { const auto m = missingAnswer(o); return {.first = m.code, .viewStatus = m.viewStatus}; }
        case Outcome::EndedNotAuthorized: return {.first = 0, .second = meta ? int16_t{0} : int16_t{45}, .presentFirst = meta, .presentSecond = false};
        case Outcome::SessionRevoked:     return {.first = 0, .second = 46, .reconnect = 44, .presentFirst = meta};
        case Outcome::DeniedThenOk:       return {.first = meta ? int16_t{0} : int16_t{45}, .second = 0, .presentFirst = false, .presentSecond = meta};
        case Outcome::Ok:                 return {.first = 0, .presentFirst = meta};
    }
    return {};
}

[[nodiscard]] std::string_view name(Op) noexcept;      // "Produce" …
[[nodiscard]] std::string_view name(Grant) noexcept;
[[nodiscard]] std::string_view name(Timing) noexcept;
[[nodiscard]] std::string_view name(Outcome) noexcept;
[[nodiscard]] std::string_view name(NaRule) noexcept;

} // namespace rune::node::test::amatrix
```

**`AuthMatrixRig.h`:** one in-process `BrokerNode` per test process (`SetUpTestSuite`: single node, `authMode = Enabled`, a generated cluster key file, shard runtime with fused placement; `Produce` sends to the owning worker's ingress endpoint from Metadata, `ProduceRouted` sends the same Produce frame to the client (gateway) endpoint — the gateway-routed path every runtime mode keeps), bootstrapped with `admin`; per cell: a fresh prefix `c<ordinal>.`, user `u<ordinal>`, role `r<ordinal>`, and objects created through the admin `AuthWireClient`: topic `<p>t` (typed, 1 partition), its untyped twin `<p>u`, view `<p>v` over `<p>t`, group `<p>g`, transactional id `<p>x`. `grantFor(op, grant, cell)` issues exactly the op's "Requires" row (Interaction matrix table) as Exact (`<p>t`) / Prefix (`PREFIX '<p>'`) / ViaRole (a role holding the Exact grants or, for cluster-level requirements, the cluster grant) / Cluster (`<OP> ON CLUSTER`) through `ql(...)` and waits for the answer gate (the DDL answer). `issue(op, cell, n)` sends the op's request (instance `n` uses object `<p>t<n>` where the op creates or drops). `watch(op, sub)` reads a streaming op's frames for the rig bound (2 s). Every wait is bounded; the deadline is the failure.

- [ ] **Step 1: Write the tests** — `AuthInteractionMatrixRegressionTest.cpp`:

```cpp
// Authentication spec §11 — the request × grant × timing matrix (720 cells, NotApplicable skipped naming its rule).
#include "support/AuthMatrixExpectations.h"
#include "support/AuthMatrixRig.h"
#include <gtest/gtest.h>

using namespace rune::node::test::amatrix;
using rune::node::test::AuthMatrixRig;

struct Cell { Op op; Grant grant; Timing timing; };
static std::vector<Cell> allCells() {
    std::vector<Cell> v;
    for (int o = 0; o < int(Op::kCount); ++o) for (int g = 0; g < int(Grant::kCount); ++g) for (int t = 0; t < int(Timing::kCount); ++t)
        v.push_back({Op(o), Grant(g), Timing(t)});
    return v;
}

class AuthMatrix : public ::testing::TestWithParam<Cell> {
protected:
    static void SetUpTestSuite() { rig_ = std::make_unique<AuthMatrixRig>(); }
    static void TearDownTestSuite() { rig_.reset(); }
    static inline std::unique_ptr<AuthMatrixRig> rig_;
};

TEST_P(AuthMatrix, Cell) {
    const Cell c = GetParam();
    if (const NaRule na = naRule(c.op, c.grant, c.timing); na != NaRule::None) GTEST_SKIP() << name(na);
    const ExpectedCodes want = expectedCodes(c.op, c.grant, c.timing);
    auto cell = rig_->newCell();
    if (c.timing != Timing::CreatedAfterLogin && c.timing != Timing::ObjectMissing) cell.createObjects(c.op);
    if (c.timing != Timing::GrantedAfterLogin) cell.grantFor(c.op, c.grant);
    auto user = cell.login();
    if (c.timing == Timing::CreatedAfterLogin) cell.createObjects(c.op);

    auto first = user.issue(c.op, c.timing == Timing::ObjectMissing ? cell.missingTarget(c.op) : cell.target(c.op));
    EXPECT_EQ(first.code, want.first) << name(c.op) << "/" << name(c.grant) << "/" << name(c.timing);
    if (c.op == Op::Metadata) EXPECT_EQ(first.present, want.presentFirst);
    if (want.viewStatus != kNone) EXPECT_EQ(first.viewStatus, want.viewStatus);
    if (want.first == 45) EXPECT_EQ(first.frameWithoutIds, cell.hiddenTwinAnswer(c.op)) << "every-cell assertion 3: no existence leak";

    switch (expect(c.op, c.grant, c.timing)) {
        case Outcome::EndedNotAuthorized: {
            cell.revokeFor(c.op, c.grant);
            auto second = streaming(c.op) ? user.awaitEnd(first) : user.issue(c.op, cell.target(c.op, /*n=*/2));
            EXPECT_EQ(second.code, want.second);
            if (streaming(c.op)) EXPECT_FALSE(user.deliveredAfterEnd(first)) << "every-cell assertion 4";
            if (c.op == Op::Metadata) EXPECT_EQ(second.present, want.presentSecond);
            break;
        }
        case Outcome::SessionRevoked: {
            cell.disableUser();
            EXPECT_EQ(user.awaitSessionLevel().code, 46);
            if (streaming(c.op)) EXPECT_EQ(user.awaitEnd(first).code, 46);
            EXPECT_EQ(cell.relogin().code, want.reconnect);
            break;
        }
        case Outcome::DeniedThenOk: {
            cell.grantFor(c.op, c.grant);
            auto second = user.issue(c.op, cell.target(c.op, /*n=*/2));
            EXPECT_EQ(second.code, want.second);
            if (c.op == Op::Metadata) EXPECT_EQ(second.present, want.presentSecond);
            break;
        }
        default: break;
    }
    EXPECT_EQ(rig_->unansweredRequests(), 0u) << "every-cell assertion 2";
    EXPECT_TRUE(cell.principalsRecordedAs(user.name())) << "every-cell assertion 6";
}

INSTANTIATE_TEST_SUITE_P(All, AuthMatrix, ::testing::ValuesIn(allCells()), [](const auto& info) {
    return std::string(name(info.param.op)) + "_" + std::string(name(info.param.grant)) + "_" + std::string(name(info.param.timing));
});

TEST(AuthMatrixCountRegression, CellCountsArePinned) {
    int total = 0, na[7] = {}, outcome[8] = {};
    for (const Cell& c : allCells()) {
        ++total;
        ++na[int(naRule(c.op, c.grant, c.timing))];
        ++outcome[int(expect(c.op, c.grant, c.timing))];
    }
    EXPECT_EQ(total, 720);
    EXPECT_EQ(na[int(NaRule::NA1)], 66); EXPECT_EQ(na[int(NaRule::NA2)], 24); EXPECT_EQ(na[int(NaRule::NA3)], 72);
    EXPECT_EQ(na[int(NaRule::NA4)], 22); EXPECT_EQ(na[int(NaRule::NA5)], 10); EXPECT_EQ(na[int(NaRule::NA6)], 9);
    EXPECT_EQ(outcome[int(Outcome::NotApplicable)], 203);
    int applicable = total - outcome[int(Outcome::NotApplicable)];
    EXPECT_EQ(applicable, 517);
    EXPECT_EQ(outcome[int(Outcome::Ok)], 144 + 12 + 8);            // R7 + R3 implicit + R3 created
    EXPECT_EQ(outcome[int(Outcome::EndedNotAuthorized)], 79);
    EXPECT_EQ(outcome[int(Outcome::SessionRevoked)], 79);
    EXPECT_EQ(outcome[int(Outcome::DeniedThenOk)], 79);
    EXPECT_EQ(outcome[int(Outcome::Missing)], 3 + 24);             // UnknownView + UnknownTopic
    EXPECT_EQ(outcome[int(Outcome::FilteredOut)], 3 + 4);          // Metadata R1 (3 timings) + R3 (4 grants)
    EXPECT_EQ(outcome[int(Outcome::Denied)], 59 + 8 + 15);         // R1 non-Metadata + R2 + R3 id-addressed
}
```

(The count check reconciles with the section's rule counts: R1 62 = 59 `Denied` + 3 Metadata `FilteredOut`; R3 66 = 15 `Denied` + 12 `Ok` + 3 `Missing`(view) + 4 `FilteredOut` + 8 `Ok` + 24 `Missing`(topic).)

- [ ] **Step 2: Build the rig** against the expectations until every applicable cell passes: `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_auth_matrix_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'AuthMatrix' --output-on-failure -j14`.
Expected: 517 PASS, 203 SKIPPED (each naming NA1–NA6), `CellCountsArePinned` PASS. A failing cell is a product bug unless the table is wrong — a table change needs the spec section it follows quoted in the commit body.
- [ ] **Step 3: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node/tests libs/node/CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(auth): interaction matrix - 24 request types x 5 grant forms x 6 timings (720 cells, 517 applicable) on one auth-enabled broker

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 18: Interaction matrix — seeded, model-checked stress against an independent permission model

Spec §11 "Seeded, model-checked stress": random grant / revoke sequences interleaved with produce and subscribe against an independent permission model; invariants: no request succeeds without a grant at that moment; every revoke ends affected subscriptions; every request gets an answer. D40.

**Files:**
- Create: `libs/node/tests/support/PermissionModel.h` (independent: its own maps of users → direct grants, roles → grants, memberships, enabled; its own name matching written from spec §2 — it must NOT include `rune/security/Permissions.h`), `libs/node/tests/node/AuthStressRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (`rune_auth_stress_tests`), `BrokerNode.h` (test probes: `securityAppliedOffsetOnControlForTest()`, `workerPermissionEpochForTest(w, session)`)

**Design:** `AuthBroker` (shard runtime), one admin wire client issuing security DDL and topic creates, 6 user wire clients (2 users × 3 sessions); 64 seeds × 300 steps; each step picks (seeded `std::mt19937_64`): GRANT / REVOKE (READ or WRITE, exact or prefix, user or role), GRANT ROLE / REVOKE ROLE, DISABLE / ENABLE, create topic (under or outside the prefixes), drop topic, produce (shard path or routed path) to a random topic, subscribe / let a subscription run, CREATE TOPIC by a user (CREATE grants in the mix). Security DDL are sent **without** waiting for their answers before the next data request (they race); the model records each change's window `[sent, answered]`.

**Invariants (checked after every step and at the end of each seed):**
1. **No unauthorized success (D40):** every `OK` answer to a produce / subscribe / create must be allowed by the model at some instant in `[request sent, answer received]` — i.e. allowed in the model state before or after any change whose window overlaps it. A success allowed at no such instant fails the seed naming the step.
2. **Every revoke ends affected subscriptions:** for each change that (per the model) removed READ from a live subscription's topic, that subscription receives `ErrorResponse{45}` (or 46 for disable) within 1 s of the change's answer, and no `Records` after it.
3. **Every request answered:** no request without an answer within 5 s; at the end of a seed the broker's `answeredRequests == receivedRequests`.
4. **Convergence:** at the end of each seed, for every session, a probe produce / subscribe on every topic answers exactly what the model says (no window: all changes answered), and the broker's `SecurityView` digest equals `SecurityState` replayed from `__security` by the client library.

- [ ] **Step 1: Write the model and the test** (`TEST(AuthStressRegression, SeededRandomGrantRevokeTrafficHoldsEveryInvariant)` looping seeds `0..63`; `RUNE_AUTH_STRESS_SEEDS` overrides the count for a long local run; a failure prints the seed and the step list for replay).
- [ ] **Step 2: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'AuthStress' --output-on-failure` → PASS (record the wall time; budget ≤ 4 min).
- [ ] **Step 3: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(auth): seeded model-checked stress - racing grant/revoke/role/disable vs produce (both paths)/subscribe/create against an independent permission model

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 19: 3-node integration — replication of `__security`, logins via every broker, revoke ≤ 100 ms, leader kill mid security DDL, peer keys right / wrong / none, rotation across a rolling restart, dev mode, the real client

Spec §11 "Brokers" and the 3-node items, §12 (revoke propagation ≤ 100 ms on a 3-node cluster), Review Focus 1 / 5.

**Files:**
- Create: `libs/node/tests/support/AuthTrio.h` (three BrokerNodes over real Aeron UDP, `Enabled`, a shared generated key file — or per-node key files for the wrong-key cases —, bootstrapped admin; built on `CatalogBrokerCluster.h`'s ports / tmp dirs; `restart(i, keyFile, nextKeyFile)`), `libs/node/tests/integration/AuthClusterIntegrationTest.cpp`, `libs/node/tests/client/ClientAuthClusterRegressionTest.cpp`
- Modify: `libs/node/CMakeLists.txt` (`rune_auth_cluster_tests`, `rune_client_broker_tests` gains the new file)

**Tests (`AuthClusterRegression.*`, all bounded waits):**
- `SecurityReplicatesToEveryBrokerAndDigestsAgree` — bootstrap via node 1, a role / user / grants via node 2 (redirected to the `__security` leader), the user logs in on node 3; every broker's `SecurityView` digest equal at the same applied offset.
- The 3-node matrix slice: for `op ∈ {ProduceRouted, Produce, Subscribe, CreateTopic, SecurityDdl}` × `timing ∈ {Steady, RevokedMid, UserDisabledMid}` × `via ∈ {1, 2, 3}` (the broker the user's session is on; the admin's DDL always via a different broker): 45 cells, outcomes from `amatrix::expectedCodes` (grant `Exact`; `SecurityDdl` uses `ViaRole`).
- `RevokeEndsSubscriptionsOnEveryBrokerWithin100msRegression` (the §12 gate): the user holds one live subscription on each of the 3 brokers; 20 runs of GRANT → subscribe → REVOKE; for each run the time from the REVOKE's `DdlResponse{OK}` at the admin client to each subscription's `ErrorResponse{45}` is measured; assert max ≤ 100 ms over all 60 samples; print p50 / max.
- `SecurityDdlRetryAfterLeaderKillAppliesOnceRegression` — the `__security` leader is killed after appending a `CREATE USER` (the `holdAnswersForTest` seam), the client retries with the same request id on the new leader: `DdlResponse{OK}` with the same objectId, exactly one `CreateUser` for that request id in every broker's log.
- `UnknownUserSaltIsTheSameOnEveryBrokerRegression` — `AuthServerFirst` for user `nobody` from each broker: identical salt and iterations (D8).
- `WrongKeyBrokerNeverJoinsRegression` — node 3 restarted with a different key file: nodes 1 and 2 log `peer link from node 3 REFUSED`, node 3 logs `REFUSED by`, counter 234 > 0 on all three; node 3 never becomes a member of the ISR of any partition, never votes in a term the others accept, and a client session on node 3 cannot produce to a partition led elsewhere (its forwarded frames never open); restoring the right key → it rejoins (ISR within 30 s).
- `NoKeyBrokerRefusesToStart` — `Enabled` with no `RUNE_CLUSTER_KEY_FILE`: `BrokerNode::start` throws naming the variable.
- `KeyRotationAcrossARollingRestartKeepsTrafficFlowingRegression` — a producer and a consumer run throughout; step 1: restart each node with `current = K1, next = K2`; step 2: restart each with `current = K2, next = K1`; step 3: restart each with `current = K2` only; after every restart all links re-establish (counter 233 = 2 on each) and the consumer receives every acked record exactly once (offsets contiguous).
- `DevModeRefusesMultiNode` — a 3-node config with `DevDisabled` refuses to start on every node.
- `HaltedBrokerRefetchesSecurityAfterItsDirIsMovedAside` — node 3's applier is tampered (halt), its sessions revoked, logins refused; node 3 stopped, `dataDir/__security` moved aside, restarted: it refetches from offset 0, digests agree, logins succeed.
- `ClientAuthClusterRegression.WorldServerClientProducesAndConsumesThroughEveryBroker` (the real `rune::client::Client` with username / password, one session per broker, typed produce + consume on `world.*` through each; REVOKE WRITE → the next produce completes `NotAuthorized`, no retry storm: exactly one ProduceRequest per failed record).

- [ ] **Step 1: Write the trio and the tests.**
- [ ] **Step 2: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'AuthCluster|ClientAuthCluster' --output-on-failure -j4` → PASS (record the revoke-propagation p50 / max and the rotation timings in the commit body).
- [ ] **Step 3: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(auth): 3-node slice - __security replication, logins via every broker, revoke <=100 ms gate, leader kill mid security DDL, wrong/no cluster key, key rotation across rolling restarts, dev-mode refusal, real client

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint E (controller):** full `*Regression*` in every repo (plus ASan/UBSan, TSan).

---

### Task 20: Launchers migrated, docker e2e on 3 nodes, docs and release notes

Spec §11 "Docker e2e, 3 nodes" (1. bootstrap; 2. create a role, a user and grants; 3. a world-server-style client produces and consumes; 4. revoke, and the client fails explicitly; 5. rotate the cluster key across a rolling restart), §9 (dev mode is not the default), D31 — every multi-node launcher now needs a cluster key and credentials, or it refuses to start.

**Files:**
- Create: `docker/lib/auth.sh` (shell helpers: `rune_gen_cluster_key <dir>` (via `rune security gen-cluster-key`), `rune_bootstrap <broker> <admin> <otp-file> <key-file>` (bootstrap, then `ALTER USER … SET PASSWORD` from a generated file), `rune_create_e2e_user <broker> <user> <password-file>` (a role `e2e` with `READ`, `WRITE`, `CREATE`, `ALTER`, `DROP` on `PREFIX ''` topics and views, `READ GROUP PREFIX ''`, `WRITE TRANSACTIONAL_ID PREFIX ''`, `DESCRIBE ON CLUSTER`, granted to the user)), `docker/auth-e2e.sh`, `docker/docker-compose.auth.yml`
- Modify: `docker/docker-compose.yml`, `docker-compose.shard.yml`, `docker-compose.custom.yml`, `docker-compose.catalog.yml` (mount `./secrets/cluster.key` read-only at `/run/secrets/cluster.key`, `RUNE_CLUSTER_KEY_FILE=/run/secrets/cluster.key`), `docker-compose.single.yml` (same; single-node keeps auth on), `docker/entrypoint.sh` (refuse a missing key file with the broker's own message — no silent generation), `docker/e2e.sh`, `schema-e2e.sh`, `schema-evolution-e2e.sh`, `view-engine-e2e.sh`, `transient-failover-e2e.sh`, `catalog-e2e.sh` (source `lib/auth.sh`: generate the key before `up`, bootstrap + create the e2e user after the cluster is up, pass `RUNE_USER` / `RUNE_PASSWORD_FILE` to every probe container), `docker/rune-cluster.env.example`, `docker/README.md`, `dr-e2e/harness/*` (the Python harness: a key file per run, bootstrap in the cluster fixture, credentials for its clients), `bench/kafka-compare/{cluster.sh,lib.sh,run-rune.sh}` (key + bootstrap for multi-node; the bench client gets credentials — benchmarks run authenticated, as production does), `rune-client-cpp/examples/*` (read `RUNE_USER` / `RUNE_PASSWORD_FILE` into `ClientConfig`)
- Create: `docs/SECURITY.md`; modify `docs/SELF-HOSTING.md`; create `docs/release-notes/authentication.md`; modify `rune-client-cpp/README.md`, `rune-ql/README.md` (if present)

`docker/auth-e2e.sh` (house style of `catalog-e2e.sh`: bounded waits, `trap` teardown, no fixed sleeps as gates):
1. Build the broker and probe images; `rune security gen-cluster-key --out docker/secrets/cluster.key`; `up` the 3-node `docker-compose.auth.yml`; wait (bounded 180 s) for a consensus leader.
2. **Bootstrap:** `rune security bootstrap --admin admin --password-file /work/otp --cluster-key-file /run/secrets/cluster.key --broker rune-1:40200` → exit 0; a second run → exit 1 and stderr contains `already bootstrapped`; `rune ql --user admin --password-file /work/otp -e "ALTER USER admin SET PASSWORD '…';"` (from a generated file) → exit 0; with the OTP again → exit 1 `AuthenticationFailed`.
3. **Role, user, grants:** `rune ql --user admin --password-file /work/admin.pw -f /topics/auth-e2e.rql --broker rune-2:40200` (topics `world.zone1` (typed), `player_events`; `CREATE ROLE world_server; GRANT READ ON TOPIC player_events TO ROLE world_server; GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server; GRANT READ ON TOPIC PREFIX 'world.' TO ROLE world_server; GRANT READ ON GROUP PREFIX 'world-' TO ROLE world_server; CREATE USER ws1 WITH PASSWORD '…'; GRANT ROLE world_server TO USER ws1;`); `show grants for user ws1;` lists the four grants via `role:world_server`.
4. **World server:** the `rune_produce_subscribe` example as `ws1` (`RUNE_USER=ws1`, `RUNE_PASSWORD_FILE`) produces 10 000 records to `world.zone1` through rune-3 and consumes them in group `world-1` → exit 0, every record received; the same client writing `player_events` → exit 1 naming `NotAuthorized`; a client with a wrong password → exit 1 `AuthenticationFailed`.
5. **Revoke → explicit failure:** start the consumer in the background (live), `REVOKE READ ON TOPIC PREFIX 'world.' FROM ROLE world_server;` via rune-1 → the consumer exits within 2 s printing `subscription ended: NotAuthorized` (never hangs, never a silent stop); `REVOKE WRITE …` → the producer's next send fails `NotAuthorized`.
6. **Rotate the cluster key across a rolling restart** (spec §8's three steps): with a producer / consumer pair running as `ws1` (grants restored): generate `cluster.next.key`; for each node: restart with `RUNE_CLUSTER_KEY_FILE_NEXT` mounted; then for each node: restart with current = next (old as `_NEXT`); then for each: restart with the new key only. After each restart wait (bounded 60 s) until `docker logs` shows `peer link with node N established` for both peers. The consumer receives every acked record (count check at the end).
7. Print `AUTH-E2E: OK`; teardown.

**Docs:** `docs/SECURITY.md` — the model (users, roles, grants, the §2.1 table with D15–D22), Rune QL statements with examples, SCRAM and the two-password rotation, the throttle (D14), bootstrap (D3 / D4, `--password-file`), the cluster key (generation, mounting as a Kubernetes secret, rotation's three steps, D32 / D35), what happens on revoke / disable / drop / TERMINATE SESSIONS (D25 / D26), dev mode and why it refuses multi-node, the error table of spec §10 with codes 44–47, `#halted` (D28, the runbook), the counters 1 000 226–235, TLS recommendation (D12). `SELF-HOSTING.md` — "Authentication is on by default": the key file, bootstrap, client credentials, `RUNE_AUTH=disabled` for single-node development only. Release note — breaking: authentication on by default (a broker without `RUNE_CLUSTER_KEY_FILE` refuses to start), client version 9 required against an authenticated cluster (older clients are refused at Connect), `authToken` removed from `ClientConfig`, inter-broker frames are encrypted (32 bytes per frame; the max produce batch is 32 bytes smaller), new codes 44–47, every launcher now bootstraps.

- [ ] **Step 1: Migrate the launchers and examples; write the e2e and the docs.**
- [ ] **Step 2: Local smoke (no docker):** `bash -n docker/*.sh docker/lib/auth.sh`; the example binaries build; `rune ql --dry-run` parses every statement in `docker/topics/auth-e2e.rql` and in `docs/SECURITY.md` (extract the ```sql blocks).
- [ ] **Step 3: Run on the VM** (never the local Mac):

```bash
rsync -a --delete /Users/vradu/Projects/se-worktrees/ root@178.104.16.102:/root/se-worktrees/
ssh root@178.104.16.102 'flock /root/vm.lock bash /root/se-worktrees/rune-broker/docker/auth-e2e.sh'
ssh root@178.104.16.102 'flock /root/vm.lock bash -c "cd /root/se-worktrees/rune-broker/docker && ./e2e.sh && ./view-engine-e2e.sh && ./transient-failover-e2e.sh && ./catalog-e2e.sh"'
```

Expected: `AUTH-E2E: OK`, and every migrated e2e passes authenticated.
- [ ] **Step 4: Commit** (per repo)

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add examples README.md && git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(examples): RUNE_USER / RUNE_PASSWORD_FILE credentials; README: authentication

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add docker dr-e2e bench docs
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(auth): docker e2e (bootstrap, role/user/grants, world-server produce+consume, revoke fails explicitly, cluster-key rotation across rolling restarts); launchers bootstrap and authenticate; SECURITY.md, SELF-HOSTING, release notes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint F (controller):** full `*Regression*` in every repo (plus ASan/UBSan, TSan); the docker e2e set on the VM; then the final whole-branch review.

---
## Spec gaps (found while planning; each resolved by a Decision above — the spec text is not edited by this plan)

- **G1 — Inter-broker encryption does not exist today.** Spec §8 says inter-broker links "use the same transport encryption as clients"; only client↔broker links are encrypted. Resolved by D33 / D34 (Tasks 15–16 add it).
- **G2 — Per-object filtering of catalog history (§2.1) conflicts with client-side verifying replay** (catalog-history spec §7): a filtered `__catalog` stream cannot be replayed or fingerprint-checked. Resolved by D21 (catalog reads need `DESCRIBE ON CLUSTER`); broker-side filtered query evaluation is a follow-up.
- **G3 — The minimum password length cannot be enforced by the broker** (§4: the plaintext never reaches it). Resolved by D9 (client library and CLI).
- **G4 — SCRAM has one salt per exchange; §3 allows two active passwords.** Resolved by D5.
- **G5 — §5 assumes the handshake always runs inside TLS;** TLS is optional (build option + `RUNE_REQUIRE_TLS`). Resolved by D12 (works without TLS, logged, documented).
- **G6 — Bootstrap of a fresh cluster is open to whoever reaches it first.** Resolved by D4 (cluster-key proof).
- **G7 — §7's "first `__security` event" is not among §3's event kinds.** Resolved by D3 (`Bootstrap`).
- **G8 — The visibility rule for requests that address objects by id** (produce / subscribe / view reads carry ids, not names). Resolved by D22.
- **G9 — A halted `__security` apply is unspecified** (the catalog keeps serving; a security log that stops applying stops enforcing revocations). Resolved by D28 (fail closed).
- **G10 — §2.1 omits LeaveGroup, non-transactional InitProducerId, GetTopicSchema, AddOffsetsToTxn / TxnOffsetCommit.** Resolved by D18.
- **G11 — CreateView over an unreadable source would leak it through the view.** Resolved by D19.
- **G12 — The bootstrap admin must change its own password, but §2.1 puts all user DDL under `ADMIN`.** Resolved by D20 (self password changes).
- **G13 — The DDL OK vs enforcement ordering** (a GRANT answered OK on a broker that does not yet enforce it). Resolved by D24.
- **G14 — Every existing in-process multi-node test would be refused by "dev mode refuses multi-node"** if code-built configs defaulted to dev mode, and broken if they defaulted to enabled. Resolved by D31.

---

## Self-review

- **Spec coverage, section by section:**
  - §1 Goal (every client proves identity; every request checked; explicit refusals; users / roles / grants as replayable events with history; no measurable hot-path cost) → Tasks 7 + 11 (identity), 12 + 13 (every request), 2–4 + 9–10 (events, replay, history), 13 (≤ 2 ns, parity), 17–18 (explicit answers in every cell).
  - §2 Model (principal, role, grant triple, operations, resource types, exact / prefix patterns, users get roles and direct grants, union, allow-only default deny) → Task 2 (types), Task 3 (rules, `effectiveGrants`, `opsOn`), Task 12 (tables), D15–D17.
  - §2.1 (every row) and visibility → Task 12's enforcement map (one row per handler), D18–D23, matrix ops (Task 17), `AuthVisibilityRegression` (Task 12).
  - §3 `__security` (catalog design: one partition, every broker, acks=all, no unclean, never compacted, disk, serialized via the leader's AdminWorker, applied in order, format version + statement + principal + timestamp + fingerprint, frozen rules; reads restricted; event kinds; verifiers only; ≤ 2 verifiers; history reads with the shared library) → Task 9 (partition, applier), Task 10 (write path, restricted reads), Tasks 2–4 (event, rules, replayer, golden), Task 8 (client reads, SHOW commands), D1–D3, D6, D37.
  - §4 Definition (every statement; min length 12 configurable; verifiers built client-side) → Task 6 (grammar, redaction), Task 8 (verifier building, length rule), D9, D10.
  - §5 Authentication (SCRAM in the session handshake; config username / password; one failure answer, session ends; rate limit per user and source with backoff, no lockout; version gate; logins are metrics + logs, not events) → Task 1 (SCRAM), Task 7 (client), Task 11 (server, throttle, gate, logs, counters), D5, D7, D8, D11–D14.
  - §6 Enforcement (per-session table by topic / view slot + group / txn patterns; prefixes resolved at login and extended on create; check once at subscribe / join / first produce, each produce one bitmask load; no allocation / lock / shared state; live changes through the rings; revoked READ ends subscriptions; revoked WRITE fails produces; nothing silent; drop / disable → SessionRevoked; password change keeps sessions unless TERMINATE) → Task 12 (tables, prefixes, created-after-login), Task 13 (workers, one load, zero alloc), Task 14 (live changes, credential changes), D24–D26, D30.
  - §7 Bootstrap (once; ADMIN CLUSTER; generated OTP printed once; first event, principal `bootstrap`; must change on first login, else PasswordChangeRequired; `--password-file`; refuses a second run) → Task 8 (CLI), Task 10 (write path), Task 11 (PasswordChangeRequired), D3, D4.
  - §8 Broker-to-broker (shared key file; mutual HMAC challenge-response, key never on the wire; same encryption as clients; rotation via `_NEXT`, three steps; refusal logged on both sides; no `__security` dependency) → Tasks 9 (key files), 15 (handshake, rotation, refusals), 16 (encryption), 19 / 20 (rotation across rolling restarts), D32–D35.
  - §9 Dev mode (`RUNE_AUTH=disabled`, anonymous full access, warning at start and every 5 min, metric, console banner, refuses multi-node, not the default) → Task 9 (config, warnings, counter, refusal), Task 11 (anonymous sessions), Task 8 (CLI banner; the console is Part D's, which reads `authMode`), D31.
  - §10 Errors (every row) → Task 5 (codes 44–47, version gate code), Tasks 11 / 12 / 14 / 15–16 (each condition's answer), the matrix (Task 17).
  - §11 Testing (request type × permission state; session lifecycle; brute force; brokers; seeded model-checked stress with the three invariants; golden replay; docker e2e steps 1–5) → Task 17 (matrix: all six permission states as grant forms × timings), the lifecycle / brute-force / broker table under "Interaction matrix" (Tasks 9–16, 19), Task 18 (stress), Task 4 (golden), Task 20 (e2e, all five steps).
  - §12 Gates (≤ 2 ns + zero alloc; hot-path parity vs auth disabled; revoke ≤ 100 ms on 3 nodes; login ≤ 5 ms) → Task 13 (first two), Task 19 (`RevokeEndsSubscriptionsOnEveryBrokerWithin100msRegression`), Task 11 (`FullScramLoginMedianUnder5msRegression`) and Task 1 (the SCRAM bench).
- **Placeholder scan:** no "TBD" / "TODO" / "fill in" / "similar to Task N". Test bodies given as a name + contract comment state their exact steps and assertions; runtime data is produced by named steps (the golden digests — Task 4 Step 4; benchmark medians — Tasks 1, 13, 16, 19 commit bodies).
- **Type consistency:** `rune::crypto::{Digest, DigestBytes, hmac, hash, pbkdf2, hkdf, randomBytes, constantTimeEqual, base64*, ClusterKeyMaterial, deriveClusterKey}` and `rune::crypto::scram::*` (Task 1) are used by Tasks 7, 8, 9, 10, 11, 15, 17; `rune::security::{EventKind, Operation, ResourceType, PatternKind, GranteeKind, opBit, ResourcePattern, GrantSpec, ScramVerifier, UserChange…, Ops, SecurityEvent, encode, decode}` (Task 2) by 3, 4, 8, 9, 10, 11, 12; `SecurityState`, `Applied` (+ `SessionEffect`, `affectedUsers`), `Refusal`, `ApplyError`, `apply`, `previewFingerprint`, `previewApplied`, `effectiveGrants`, `opsOn`, `clusterOps`, `matches`, `validName`, `validGrant` (Task 3) by 4, 8, 9, 10, 11, 12, 14; `SecurityReplayer`, `RecordedEvent`, `history`, `stateAsOf`, `grantsFor`, `users`, `roles`, `readRsec`, `testing::goldenScriptV1` (Task 4) by 8, 9, 18; protocol `AuthClientFirst/ServerFirst/ClientFinal/ServerFinal`, `SecurityDdl`, `SecurityBootstrap`, `bootstrapMacInput`, `AuthMode`, `AuthState`, `kSecurityTopicId`, `kClientVersionAuth`, the ConnectResponse v9 fields, ops 21 / 22 (Task 5) by 7, 8, 10, 11, 13, 15; `rune::ql::{…Def, SecurityPlan, applySecurityPlan, redactedText, isSecurityStatement, carriesPassword}` (Task 6) by 8, 11, 17; `rune::peerlink::{ClusterKey(s), loadClusterKeys}` (Task 9) by 10, 11, 15; `SecurityApplier`, `SecurityView` (Task 9) by 10–14, 18, 19; `SecurityAdmin`, `SecurityAnswerGate`, `SecurityRoute` (Task 10) by 13, 14, 19; `SessionAuth` (+ states), `gateFor`, `scramServerFirst/Final`, `LoginThrottle`, `AuthWireClient`, `AuthBroker` (Task 11) by 12–14, 17–19; `PermissionTable`, `ObjectIndex`, `Authorizer` (Task 12) by 13, 14, 17; `WriteGrantSlot`, `allowsWrite`, `PermissionShipper` (Task 13) by 14, 17, 18; `LiveActions`, `refreshActions` (Task 14); `PeerHandshake`, `LinkKeys`, `deriveLinkKeys`, `PeerLinkAgent` (Task 15) by 16, 19; `PeerFrameCrypto`, `kPeerFrameOverhead`, `PeerCryptoMode` (Task 16) by 19; `amatrix::*` (Task 17) by 18, 19, 20. Ring ids, WorkerOp types and counter ids are each assigned once in Global Constraints.
- **Review Focus:** each of the eight has its named test in the owning task (11 + 19; 14 + 17 + 18 + 19; 13 + 14 + 17; 10 + 14; 15 + 16 + 19; 12 + 17; 6 + 10; 14).
- **Philosophy:** shared-nothing — the admin thread owns `__security` and its authoritative state, the control thread owns its own mirror, every session's tables, the throttle and the peer-link agent, each worker its write grants and frame crypto (Global Constraints rule 1); rings only — every hop listed in rule 2 rides an existing SPSC ring (lifecycle, request, role, admin→control, control→admin), no callback crosses a thread; per-state variables — `SessionAuth`, `SecurityRole`, `InFlight`, the applier's two variants, `LinkState`, `ScramExchange`, `PermissionUpdate`, `PeerCryptoMode`; zero allocation — one bounds-checked load per produce (gateway and worker), preallocated seal / open scratch, idle sessions / applier / links measured allocation-free (Tasks 9, 13, 16); test + benchmark — every task has its tests; Tasks 1, 4, 13, 16, 19 benchmark or gate; regression naming — every fix and every pinned behaviour is `*Regression*`; fix-on-sight — four items in Global Constraints rule 8, each with its task and test.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-10-19-rune-authentication.md`. The recommended method is **subagent-driven** (Sonnet implements, Opus reviews, the full review process — memory "SDD model policy") — 20 tasks across six repos with many cross-task interfaces (the crypto primitives, the shared library, the wire, the session auth state, the permission tables, the peer links), where a fresh reviewer per task catches an interface drift before the next task builds on it. Nothing is pushed.
