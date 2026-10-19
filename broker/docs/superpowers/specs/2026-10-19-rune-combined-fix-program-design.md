# Rune — combined auth fix + carryover redesign program — design

**Status:** DRAFT 2026-10-19, for user approval. It amends the binding authentication spec
(`2026-10-13-rune-authentication-design.md`) and plan decisions (`plans/2026-10-19-rune-authentication.md` header) where §3
says so, and amends the three carryover redesigns where §16 says so.
**Inputs:** the exhaustive review (`.superpowers/sdd/2026-10-19-rune-authentication/full-review/pass{1,2,3}-*.md`), the parked
ledger (`parked-items.md`) and the three carryover specs (RFR = replica-facts-ring, ADW = activity-driven-worker-pass, VLS =
view-read-lease-and-sink-plane).
**Index:** `full-review/consolidated-index.md` — 189 distinct defects `X001–X189` (every review/parked id is an alias of a
row). This spec cites `X` rows; the index carries the original ids, severities and statuses.
**Order:** this program runs before auth Tasks 17–20, which then run with the additions in §19.
**Repos:** rune-broker, rune-protocol, rune-client-cpp (incl. the `rune` CLI), rune-crypto, rune-codec, rune-ql. Branch `feat/auth`.

## 1. Root causes

The 162 OPEN defects come from twelve root causes. Each is one section, and each has one design.

| § | Root cause | Critical | Main rows |
|---|---|---|---|
| 4 | A session's identity is a guessable number in a plaintext header; TLS is optional, downgradable and absent from the CLI | X001 | X001–X011 |
| 5 | The worker decides whether to enforce from an unrelated resource (the ack sink) | X012 | X012–X014 |
| 6 | Enforcement is declared when a ring *accepts* an op, not when the owner *applied* it; terminal frames race the owner's pushes | X015 | X015–X026 |
| 7 | A broker's `__security` view has no freshness: catching-up, stale and halted all look "serving" | X027 | X027–X035 |
| 8 | Ids that a request names (producer, member, txn markers) are trusted instead of bound to the caller | X036, X037 | X036–X045 |
| 9 | Authorization runs on the wrong kind / before C1 / leaks through structured answers | — | X046–X062 |
| 10 | Login throttle and credential paths are charged or keyed wrong | — | X063–X076 |
| 11 | The peer handshake acts on unauthenticated input and switches keys non-atomically | X077 | X077–X100 |
| 12 | Secrets at rest / in memory and log output are not protected | — | X102–X107 |
| 13 | Hot-path and control-plane costs are unmeasured or O(N) | — | X108–X122 |
| 14 | Tests pass with the property broken | — | X123–X141 |
| 15 | Parked catalog / ISR / ledger items | — | X142–X173 |
| 16 | The three carryover redesigns (RFR, ADW, VLS) | X188 | X185–X188 |

## 2. Cross-cutting decisions

| # | Decision | Rationale (alternative rejected) |
|---|---|---|
| CD1 | **TLS is mandatory for every client session when `RUNE_AUTH=enabled`.** A plaintext Connect is answered `HandshakeFailed (29)`. | Only per-frame AEAD under a session key binds a frame to its session; image/source binding is forgeable over UDP (rejected); random ids alone are visible on-path (rejected as the fix, kept as depth, CD7). Closes X001, X002, X066's precondition. |
| CD2 | **`RUNE_TLS` build option defaults ON.** `RUNE_AUTH=enabled` in a build without TLS refuses to start (exit 2). Any `RUNE_TLS_*` / `RUNE_REQUIRE_TLS` in a non-TLS build refuses to start. | X003. A silent no-op is a fail-open configuration. |
| CD3 | `RUNE_REQUIRE_TLS` is implied by `RUNE_AUTH=enabled`; `RUNE_REQUIRE_TLS=false` with auth enabled refuses to start. It keeps its meaning only in dev mode. | One rule, no contradictory combination. |
| CD4 | **SCRAM channel binding `tls-exporter` (RFC 9266) is mandatory now** (gs2 header `p=tls-exporter,,`). No `n`/`y` path exists, so there is nothing to downgrade. | X008, X007's relay case. TLS is always present (CD1), so binding costs one exporter call per login. Deferring it would freeze a wire that must change later. |
| CD5 | Client: `requireCrypto` defaults to true whenever credentials are set; the expected TLS host defaults to the gateway endpoint's host (DNS name → `SSL_set1_host`, IP → `SSL_set1_ip_asc`); system trust with no host check is refused. Pre-TLS frames can never make a session terminal. | X006, X007, X004. "Degrade, don't fail" is rejected for credentialed clients. |
| CD6 | **Per-destination c2b keys and sequence spaces:** `c2b_d = HKDF-Expand(c2bMaster, "rune-c2b|" ‖ u16 d)`, d = 0 gateway, 1+w worker w; d is also in the AAD. | X009. One AAD field instead of per-receiver seq coordination (rejected: shared counter across threads). |
| CD7 | Session ids keep the node prefix; the low 40 bits come from the CSPRNG (collision-checked). | Depth only: cuts blind DoS (forged HandshakeContinue / Disconnect against pending connects). |
| CD8 | **Peer links get forward secrecy now:** X25519 ephemeral shares in Hello/Challenge, covered by the MACs, mixed into the link key. | X084. The handshake is being rewritten for X077–X092 anyway; the wire is unfrozen. Cost ≈ 100 µs per (rate-limited) handshake, cold. |
| CD9 | **Unauthenticated peer input never changes an established link.** Rekey is non-destructive, either side may initiate, and new keys are installed receive-first on both sides before anyone sends under them. | X077–X079, X086. Rate limits alone (rejected) still let an attacker pick the moment. |
| CD10 | **Sealed multi-node clusters require per-worker replication planes** for user partitions (`RUNE_AUTH=enabled`, ≥1 peer, `perWorkerReplication=false` refuses to start). Shard mode, the default, already sets it. The legacy transport keeps only internal/control traffic. | X083. Moving seal to workers before the outbound ring (rejected) still opens on the control thread. |
| CD11 | **New retriable error code `AuthUnavailable = 48`:** "this broker cannot authenticate or authorize right now" (catching up, stale, security-halted). Clients back off and fail over to another seed. A halted broker answers 48, not terminal 46. | X027, X028, X032. Overloading 44 (terminal by D27) or NotLeader (wrong meaning) is rejected. |
| CD12 | **The VLS Part A read lease is generalized into a `BrokerLease`** consumed by GetViewRow *and* security freshness. | One quorum-backed proof that a broker is in contact with the controller; a second mechanism (rejected) would duplicate the clock/drift reasoning. |
| CD13 | **Enforcement is acknowledged:** a new `EnforceFence{seq}` worker op (and admin message) follows the ops it fences on the same FIFO ring; the owner answers `FenceAck{ring, seq}`. D24 releases per event when every fence it needs is acked; a held answer has a deadline. The Task 13 R4 hold is deleted. | X016, X019, X021, X025. A per-session pending flag on the worker (rejected) needs cross-ring knowledge the worker lacks. |
| CD14 | **The owner of a push publication emits the terminal frame** (45/46) of a subscription it pushes, after its last Records. | X017. Ordering by FIFO, not by timing; the gateway cannot order frames on a worker's publication. |
| CD15 | **Last-admin guard:** a security DDL that would leave no enabled user effectively holding `ADMIN CLUSTER` is refused unless it carries `FORCE`. | X028. Keeps the D3 recovery window (review C I2) but makes reopening deliberate. |
| CD16 | **Producer ids are MAC-tagged to the principal:** `pid = counter31 << 32 | tag32`, tag = SipHash-2-4(k_pid, counter ‖ principalId); verified once per (session, pid) and cached. Transactional batches need a coordinator-registered partition (KIP-890 style). | X037. A replicated pid→owner table on every leader (rejected) costs a lookup and a replication path; Kafka's "trust WRITE holders" (rejected) is exactly the bug. |
| CD17 | Group member ids are 128-bit random and bound to the owning session; every member-naming request is refused (`UnknownMember`) unless the member belongs to the caller. LeaveGroup needs READ on the group. | X039, X040. |
| CD18 | Self password changes need no extra proof of the current password; they are audited (warn + counter, source host). | X066's single-frame hijack is impossible under CD1+CD4; an in-process compromise already holds the password, so a proof-in-DDL protocol (rejected) adds complexity for nothing. |
| CD19 | The `__security` leader renders the recorded statement from the ops for every security event; client text is ignored. | X062: removes the redaction heuristic and audit forgery in one move. |
| CD20 | Cluster key file: refuse group- or world-readable in Enabled mode (launchers / k8s `defaultMode: 0400`); `dataDir/__security` 0700, files 0600. | X102. A warning (D32) is rejected: the file is password-equivalent material. |
| CD21 | A credentialed Client that has never been Live retries `AuthenticationFailed` without `retryAfterMs` twice (500 ms, 1 s) before `Rejected`. | X031. Majority-of-views gating on the leader (rejected) needs cross-broker acks for every user DDL. |
| CD22 | Views have **definer rights** (SQL view semantics): a creator's later loss of source READ does not stop the view; it is logged and counted. | X057. Re-checking every view on every revoke (rejected) turns a revoke into a catalog scan and breaks running views silently. |
| CD23 | D21 stays (catalog reads need DESCRIBE ON CLUSTER); spec §2.1 is amended to say so. | X054. Per-object filtering breaks the verifying replay. |
| CD24 | Under auth the dynamic-MDC record fallback is disabled; an unresolvable record egress ends the Subscribe with `Internal` and detail "record egress unavailable". | X020. A per-session sealed MDC (rejected) still broadcasts metadata and wastes egress. |
| CD25 | Peer frame header stays 16 bytes; NodeConfig refuses `workerCount > 252`, more than 16 peers, node ids > 65535 when Enabled. | X082. Widening sender id (rejected) changes every frame limit for no deployment need. |
| CD26 | All perf gates run on the quiet bench host (VM) in `build-bench-rel`; a gate measured on a loaded box is not evidence. | X121, X109–X112. |

## 3. Amendments to the binding spec and plan decisions

| Where | Amendment |
|---|---|
| Spec §5 | SCRAM runs inside mandatory TLS with `tls-exporter` channel binding (CD1, CD4). Brute force: per-user component added (§10). New code 48 (CD11). |
| Spec §2.1 | LeaveGroup: READ group + own member. Idempotent InitProducerId: any WRITE grant. TxnOffsetCommit: + READ each topic + own member/generation. DefineTopic with a descriptor on an existing topic: ALTER (+DESCRIBE). Catalog reads: DESCRIBE ON CLUSTER (CD23). Views: definer rights (CD22). |
| Spec §6 / D24 | An OK guarantees enforcement **on the answering broker** once every owner thread acked (CD13); other brokers enforce within the staleness bound F (§7). |
| Spec §8 / D33–D35 | Peer handshake v2 (§11): FS, symmetric initiation, receive-first install, Hello lists both key ids. AAD gains the stream id; receivers accept only their role's sender id. |
| Spec §10 / D26–D28 | 48 added. D26: terminal frames emitted by the push owner (CD14); D27: CD21; D28: a halted broker ends sessions with 48 (failover), not 46. |
| Spec §11 / §12 | Matrix gains the ownership, transport and back-pressure dimensions (§19); gates in §13. |
| D3 / D11 | Last-admin guard (CD15). `authState = Anonymous` only on a never-bootstrapped cluster that is `Serving` (§7). |
| D12 | Replaced by CD1–CD3. |
| D14 | §10 (first-leg charging, per-user backoff, keyed SipHash tables, source from the fragment's image). |
| D18 | Replaced by the §2.1 row above. |
| D22 | Pass-through only for `id ≥ 0`; an id at or above this broker's applied topic horizon answers NotLeader (§9). |
| D30 | Workers hold shared permission tables by id; deny flavours (44/45/47) and the horizon ship with the table (§6, §9). |
| D31 | Code-built configs additionally choose `ClientTransport::{RequireTls, PlaintextForTest}`; `parse()` yields only `RequireTls` when Enabled; `main.cpp` refuses `PlaintextForTest`. |
| D32 | CD20. |

## 4. Session identity and transport binding (X001–X011)

**Design.**
- Broker (`ClientGateway`, `NodeConfig`, `TlsStartupPolicy`): in Enabled mode the transport policy is `RequireTls{certPath, keyPath}`; a Connect without a live server TLS session at the end of the handshake is refused (`HandshakeFailed`); `resumeAuthenticated_` is deleted for plaintext sessions (every Enabled session is encrypted). `cipherList` is intersected with the supported set; no intersection → `HandshakeFailed` (X010). Session ids per CD7. The v10 ConnectResponse fixes the review A M8 decode quirks (X011; re-itemize from `review-A.md:137`).
- Per-destination keys (CD6): the gateway derives `c2b_0`, ships `c2b_{1+w}` to worker w in the second announce wave (replacing the shared key, `ClientGateway.cpp:2681-2704`); each receiver keeps its own window. Client `ClientAgent::sealForDestination_` keeps one `sendSeq` per destination.
- Channel binding (CD4): rune-crypto `TlsSession::exportChannelBinding()` → 32 bytes (label `EXPORTER-Channel-Binding`, empty context). Client-first gs2 header `p=tls-exporter,,`; client-final `c=` = base64(gs2 header ‖ cb). The server refuses any other gs2 header (44, same frame as every failure).
- Client (CD5): `ClientConfig::insecurePlaintextAuth` (default false; tests and dev only, logs warn once) is the only way to send credentials without TLS. `sessionEncrypted()` accessor. Pre-TLS ErrorResponse / ConnectResponse refusals (incl. `ProtocolVersion`) move the session to `Backoff`, never `Rejected`; after 5 identical refusals over ≥ 60 s → `Rejected`. Session-fatal notices (44/46/48 corr 0) are accepted only sealed on the encrypted session; `AuthServerFinal` must match `correlationId_` and `sessionId_` (X004).
- CLI (X005): `--tls-ca <file>`, `--tls-host <name>`, `--insecure-plaintext` (refused with credentials unless the broker is dev mode); env `RUNE_TLS_CA_FILE`, `RUNE_TLS_HOST`. Every `LiveBroker::open` maps them into `ClientConfig`.

**State machine (client `SessionMachine`, transport sub-state).** `PreTls{attempt, lastRefusal}` → `TlsHandshaking{hs}` → `Sealed{keys[dest], sendSeq[dest]}`; the auth info flags become `AuthPhase = Anonymous | BootstrapOnly | Authenticated{mustChange}` (X115 part).

**Wire.** Client version 10 (`kClientVersionAuthV2`), broker version 6 (`kBrokerVersionAuthV2`); Enabled refuses client < 10 at the version gate. No new templates. gs2/cbind per RFC 5802/9266. AAD of c2b frames = header ‖ sub-header ‖ u16 destination.

**Redesigns.** None directly; the second announce wave carries the per-worker key (ADW keeps the lifecycle drain unchanged).

**Tests.** `AuthSessionBindingRegression.APlaintextConnectToAnAuthenticatingBrokerIsRefusedRegression`,
`…AFrameNamingAnotherSessionsIdNeverOpensRegression`, `…SpoofedDisconnectNeverEndsAnotherSessionRegression`;
`NodeConfigRegression.{EnabledWithoutTlsBuildRefusesToStart, TlsVariablesInANonTlsBuildRefuseToStart, RequireTlsFalseWithAuthRefusesToStart}Regression`;
`GatewayCryptoRegression.AWorkerBoundFrameNeverOpensAtTheGatewayRegression`, `…AFrameForWorkerANeverOpensAtWorkerBRegression`;
`ScramChannelBindingRegression.{ARelayedExchangeFailsTheBinding, AnUnboundGs2HeaderIsRefused}Regression`;
`SessionMachineAuthRegression.{ACredentialedClientNeverLogsInOverAStrippedConnect, APreTlsRefusalNeverRejects, AnUnsealedSessionNoticeIsIgnored, AnAuthServerFinalWithAForeignCorrelationIsIgnored}Regression`;
`ClientCryptoRegression.SystemTrustWithoutAHostIsRefusedRegression`; `RuneCliSecurityBrokerRegression.BootstrapAndDdlOverTlsRegression`;
`GatewayCryptoRegression.UnsupportedCipherIsRefusedNotHungRegression`.
**Matrix.** ops {Connect, AuthClientFirst/Final, Produce (G, WS), Subscribe, SecurityDdl, Disconnect, HandshakeContinue} × entity {own session, another session's id, pending connect, encrypted receiver gateway / worker A / worker B} × timing {pre-TLS, mid-handshake, sealed, after resume attempt, replayed frame}.
**Gates.** TLS login cold ≤ 5 ms native (§13); sealed produce parity in §13.

## 5. Enforcement mode and per-worker ingress (X012–X014)

**Design.** `NodeConfig::parse` refuses `perWorkerIngressEnabled && !shardMode()` with Enabled (and the TLS-startup twin of `mustRefuseNonShardPerWorkerEgressTls` for ingress). The worker gains `EnforcementMode = NotEnforcing{}` (dev/in-process, set from `authMode` before runners start) | `Enforcing{sink}`; `Enforcing` without a sink denies every produce (fail closed) — the `if (ackSink_)` branch is deleted. Gateway: `AuthAnonymous` in Required mode → `denyAllPerms_`; `Session` construction takes the auth state explicitly (no default). Counter 228 gains value 2 = in-process unauthenticated, and `BrokerNode` logs a warn at start for it (X014).

**Tests.** `NodeConfigRegression.PerWorkerIngressWithoutShardRefusedWhenAuthEnabledRegression`, `WorkerEnforcementRegression.AnEnforcingWorkerWithoutASinkDeniesEveryProduceRegression`, `AuthGateRegression.AnAnonymousSessionOnAnAuthenticatingBrokerIsDeniedEverythingRegression`, `RoleCounterRegression.InProcessReportsItsOwnAuthModeRegression`.
**Matrix.** {shard, non-shard+per-worker ingress, non-shard+per-worker egress, legacy} × {Enabled, Dev, InProcess} × {startup, first frame} — every cell refuses at start or enforces.

## 6. Enforcement acknowledgement and live-change ordering (X015–X026)

**Design.**
1. **One FIFO per worker** (`LifecycleOutbox`, control thread, preallocated): SessionAnnounce, SessionPermissions chunks, SessionClose and fences share one queue with seqs assigned at push, drained in order to the lifecycle ring (X022). `PermissionShipper` and `SessionAnnouncer` become producers into it.
2. **Fences (CD13).** `WorkerOp::EnforceFence{seq}` (type 23) is valid on the lifecycle ring and on the op ring. On processing it the worker emits `FenceAck{ring, seq}` on its RFR facts ring (§16.1, edge record, flushed this pass). The admin thread gets `kMsgEnforceFence` (control→admin, 26) and answers `kA2cFenceAck` (admin→control, 24).
3. **D24 per event.** Applying security event E computes `need[w][ring] = lastSeq` of the ops E's refresh pushed and pushes one fence per (worker, ring) touched; the held OK `HeldAnswer{offset, need, deadlineNs, frame}` (fixed-capacity pool) releases when every `acked ≥ need`. Later events never delay earlier ones (X021). Deadline `RUNE_SECURITY_GATE_DEADLINE_MS` (5 000): the answer becomes `Internal` with detail `committed at offset N; enforcement on broker B not confirmed`, counter 244. Login finals and created-after-login (X019): the AuthServerFinal and DefineTopic/CreateView OKs are held on the same fences (the `DdlAnswerGate` gains fence targets for the applied catalog event). `heldLoginFinals_` becomes `HeldAnswer` entries (X026). The worker R4 hold is deleted (X025).
4. **Subscriptions.** Per subscription at the gateway: `AwaitingEgress{w, corr}` → `Forwarded{w, corr}` → `Ending{w, corr, code, fence}` → erased on FenceAck; `GatewayPushed{corr}` for the legacy path; `AdminRead{corr}` → `AdminEnding{corr, code, fence}`. An end in `AwaitingEgress` removes the waiter and answers directly (X015: the resolve drain only forwards waiters still in `AwaitingEgress`). Authorization ends travel as `WorkerOp::EndSubscription{sessionId, subId, corr, code}` (type 24) **on the op ring** (the Subscribe's FIFO) followed by a fence; the worker ends the cursor and writes `ErrorResponse{code, corr}` on its own push publication after its last Records (CD14). The admin read servers do the same on the admin egress for −6/−7. End-reads are per-session state (`Session::adminReadEnds` bitmask + per-read corr registry, max 64 reads per session, the 65th Subscribe refused `InvalidRequest`), never a capped queue (X017, X024).
5. **Session lifecycle.** `Open` → `Revoking{fences, deadline}` (EndSubscription(46) per sub, SessionClose, fences) → on acks the gateway sends `ErrorResponse{46, corr 0}` → `Closing{deadline}` (drops inbound, drains the spill, then closes; `kClosingDrainNs` 2 s). `failLogin_` also goes through `Closing` (X018). SessionClose is fenced, so the D24 OK of DISABLE/DROP waits for the worker to drop the slot (X016).
6. **Chunks.** A worker that cannot install an update (any drop path) clears the slot to deny, logs at warn (rate-limited), and acks the fence with `failed`; the gateway re-ships from scratch (X023).
7. **MDC.** CD24 (X020).

**Wire / ids.** WorkerOp 23 `EnforceFence`, 24 `EndSubscription`; control→admin 26 `kMsgEnforceFence`; admin→control 24 `kA2cFenceAck`; facts-ring record type 2 `FenceAck{u8 ring, u64 seq, u8 status}`; counter 244 (gate deadline answers), 237 (max fence lag, ns). Client unchanged (it already drops Records after an end).

**Redesigns.** RFR: facts ring carries FenceAck (§16.1). ADW: `EndSubscription` is served in the `pushOwnedSubscriptions_` step; FenceAck emission at the facts-flush step; fences are worker-wide (no slot) (§16.2).

**Tests.** `AuthLiveChangeRegression.{RevokeDuringRecordEgressResolveNeverStartsTheSubscription, ShardRevokeOkMeansTheWorkerAlreadyRefuses, NoRecordAfterTheEndOnPerWorkerEgress, RevokedSessionUnderBackPressureStillGetsSessionRevoked, DisableOkMeansTheWorkerDroppedTheSlot, SecurityReadEndNeverDroppedUnderAdminRingPressure, SixtyFifthAdminReadIsRefused, GateReleasesPerEventUnderShipChurn, GateDeadlineAnswersInternalWithTheOffset}Regression`; `ShardAuthProduceRegression.ATopicCreatedAfterLoginIsWritableAtTheWorkerOnItsOkRegression`; `WorkerWriteGrantRegression.ADroppedChunkDeniesAndAcksFailedRegression`; `LifecycleOutboxRegression.AnnounceNeverOvertakenByPermissionsRegression`; `AuthEnforcementRegression.NoRecordIsEverBroadcastToAnUnauthorizedSubscriberRegression`. Each race test uses a seam that delays the worker's lifecycle/op drain and runs ≥ 100 iterations.
**Matrix.** ops {Subscribe, GroupJoin subs, CatalogRead, SecurityRead, Produce WS, Produce G, login final, DefineTopic} × entity {legacy push, per-worker egress, admin read, two sessions same user} × timing {before egress resolve, mid-resolve, resolve timeout, ring full, egress spill full, worker pass mid-drain, fence ack delayed past deadline, concurrent second event}. Invariants: no record after the end frame; every request answered once; OK ⇒ the answering broker's owners enforce.
**Gates.** Revoke→end ≤ 100 ms (Task 19); fence round trip ≤ 2 worker passes (bench `BM_EnforceFenceRoundTrip`); 0 allocations in steady state.

## 7. Security-log freshness (X027–X035)

**Design.** A control-thread state machine `SecurityServing`:

| State | Variables | Logins / −7 reads / security DDL on this broker | Existing authenticated sessions |
|---|---|---|---|
| `CatchingUp` | `sinceNs, bootNs` | 48 | — (none can exist) |
| `Serving` | `lastFreshNs` | normal | normal |
| `Stale` | `sinceNs` | 48 | keep running until `sinceNs + R`, then session-level 48 → `Closing` |
| `Halted` | `cause, offset, nodeId` | 48 (D28 amended) | session-level 48 → `Closing` (failover, not terminal) |

- **Fresh** means: the `BrokerLease` (CD12, §16.3) is `Leased`, and either (leader) the `__security` replica is leader at the epoch this broker's metadata names and `SecurityAdmin` is `LeaderServing`, or (follower) the replica's last leader contact `leaderContactNs` (RFR fact, §16.1) is within F and `SecurityView.applied ≥ servedHw` of that fact. `CatchingUp → Serving` additionally needs a fact with `leaderContactNs > bootNs`. Single node: `Serving` at handover.
- Bounds: F = `RUNE_SECURITY_MAX_STALENESS_MS` (1 000), R = `RUNE_SECURITY_STALE_SESSION_END_MS` (10 000); parse refuses F < 2× fetch keepalive or R ≤ F. A `__security` leader failover longer than F produces retriable 48s, never data access with stale grants beyond R.
- **Bootstrap window (X028).** `authState = Anonymous` only when no Bootstrap was ever applied (`fakeSaltSeed` unset) **and** `Serving`. A reopened window keeps `MustAuthenticate` and lets an `AwaitingFirst` session send template 80. Last-admin guard (CD15) in `SecurityAdmin` validation: `DropUser`, `AlterUser DISABLE`, `Revoke`, `RevokeRole`, `DropRole` that leave no enabled effective ADMIN holder are refused `InvalidRequest` "would leave no security admin; add FORCE" unless the op carries `FORCE`.
- **Mirror integrity.** A malformed `kA2cSecurityApplied/Halted` halts the mirror (`failSecurityClosed_`) (X029). `postSecurityResponse` never drops: a gated OK that does not fit answers `Internal` via the reserved fallback slot; release asserts become checks (X030). The applier halts on any control batch in `__security`, and `appendControlRecord` refuses negative topic ids (X033). Halted details name the node (X034).
- **Admin ring coupling (X035).** The control thread always drains the admin egress ring; catalog frames go into `DdlAnswerGate`, security frames into the `HeldAnswer` pool. Separate admin intake rings for read-server traffic vs DDL (CAT-L-8: request ring split into `kAdminDdlRing` and `kAdminReadRing`), so read pressure never pauses DDL intake.
- **Lagging credential (X031).** CD21 (client).
- **Client (X032).** 48 anywhere → `Backoff{retryAfterMs or 250 ms·2^k ≤ 5 s}` and rotate to the next seed; in-flight idempotent produces resend after reconnect; subscriptions resume; DDL answered 48 is retriable.

**Wire.** `ErrorCode::AuthUnavailable = 48` (AuthServerFirst/Final `errorCode`, ErrorResponse corr 0, DdlResponse, SubscribeResponse); `retryAfterMs`: the existing AuthServerFinal field; ErrorResponse and DdlResponse v10 gain a trailing `retryAfterMs u32` (0 = none). SecurityDdl ext gains `u8 flags` (bit0 `FORCE`); rune-ql accepts `FORCE` at the end of `DROP USER`, `ALTER USER … DISABLE`, `REVOKE …`, `REVOKE ROLE …`, `DROP ROLE …`. Counter 236 (serving state 0..3), 238 (48 answers).

**Redesigns.** RFR ReplicaFacts gains `leaderContactNs` (§16.1); VLS lease becomes `BrokerLease` (§16.3). This group therefore lands after both.

**Tests.** `AuthRestartRegression.{ARestartedBrokerNeverAcceptsADisabledUsersOldPassword, AWipedBrokerAnswersRetriableUntilCaughtUp, ANewBrokerNeverAdvertisesTheBootstrapWindow, APartitionedBrokerGoesStaleThenEndsSessionsRetriably}Regression`; `SecurityLastAdminRegression.{DisablingTheLastAdminNeedsForce, ForceReopensTheWindowAndUsersStillLogIn}Regression` (real client); `SecurityHaltRegression.HaltEndsSessionsWithAuthUnavailableAndTheClientFailsOverRegression`; `SecurityViewRegression.AMalformedAppliedFrameHaltsTheMirrorRegression`; `SecurityApplierRegression.AControlBatchHaltsTheApplierRegression`; `ClientAuthUnavailableRegression.{BacksOffAndRotatesSeeds, NeverLiveClientRetriesAFirst44Twice}Regression`; `AdminRingRegression.ReadPressureNeverPausesDdlIntakeRegression`.
**Matrix.** ops {login, −7 Subscribe, security DDL, produce/subscribe of an existing session} × entity {leader, follower, single node, new broker, wiped broker} × timing {boot before first fact, catch-up mid-log, partition < F, F < partition < R, > R, leader failover, halt, lease lapse}. Run in Task 19's 3-node rig (§19).

## 8. Request ownership binding (X036–X045)

**Design.**
- **Control batches (X036).** Gateway `handleProduce` and worker `handleWorkerIngressFrame_` refuse any batch with `kAttrControl` (`InvalidRequest`, never appended). Markers come only from the coordinator path.
- **Producer identity (X037, CD16).** `ProducerIdAllocator` issues `counter` (31 bits, exhaustion refused explicitly); `TransactionCoordinator::initProducerId` returns `pid = counter << 32 | SipHash-2-4(k_pid, counter ‖ principalId)[0..4]`, `k_pid = HKDF-Expand(master, "rune-pid-tag", 16)`. The principal id (the user's `__security` object id) ships with the session's permission table to workers. Gateway and worker verify on the first produce of a pid per session and cache it in a 4-entry per-session `VerifiedPids` array (hot path: one compare); mismatch → 45, counter 239; after 16 mismatches the session is answered 45 and ended (`Closing`). Transactional ids: when InitProducerId for a txn id comes from a different principal than the current pid's, the coordinator allocates a new pid (the old one is fenced by the txn state). **Transactional batches** (KIP-890 style): AddPartitionsToTxn makes the coordinator send `RegisterTxnPartition{pid, epoch, topicId, partition}` over the existing D9 marker channel and answers the client only after the leader's ack; the leader's PSM records `OpenTxn{epoch}`; a TRANSACTIONAL batch without it → `InvalidTxnState`, not appended. In dev/in-process modes the tag is not checked (principal 0).
- **InitProducerId (X038).** Idempotent: the session must hold WRITE on at least one object or pattern; token bucket 64/s per session (excess `InvalidRequest`).
- **Groups (X039, X040, CD17).** Member id = 32 hex chars from the CSPRNG. `GroupCoordinationService` keeps `memberSessionMap_` authoritative: JoinGroup with an existing member id only from its owning session; SyncGroup, Heartbeat, LeaveGroup, OffsetCommit answer `UnknownMember` unless the member maps to `msg.sessionId`. LeaveGroup needs READ group (gateway). OffsetCommit validates member + generation; empty member + generation −1 is accepted only when the group has no members (simple-consumer commit).
- **TxnOffsetCommit (X040, X042).** Body v10 adds `memberId str16, generationId i32`; the coordinator validates them against the group; the gateway checks READ on each entry's topic.
- **Fetch-all (X041).** The gateway passes the session's table to `OffsetFetch` fetch-all; entries whose topic the session cannot READ are omitted.
- **Reconciliation (X043).** Delete `groupMemberSubs_` / `addGroupSubscription`. On a session's `Closing`/revoke the group service evicts its members (rebalance); on loss of READ on a subscribed topic the member is evicted (`removeUnauthorizedMembers` with a topic check).
- **Bounds (X044, X045).** `sessionTimeoutMs` ∈ [`RUNE_GROUP_MIN_SESSION_TIMEOUT_MS` 1 000, `RUNE_GROUP_MAX_SESSION_TIMEOUT_MS` 300 000] else `InvalidRequest`; at most `RUNE_MAX_GROUPS` (100 000) coordinators (excess `InvalidRequest` "group limit reached", counted); a coordinator with no members is pruned after its rebalance timeout (offsets stay).

**State machines.** Per partition PSM entry: `Idempotent{epoch, seqWindow}` | `TxnRegistered{epoch, seqWindow}` | `TxnOpen{epoch, firstOffset, seqWindow}`. Per member: `Joining{session}` | `Stable{session, generation}` | `Leaving{}`.

**Wire.** TxnOffsetCommit v10 fields; `RegisterTxnPartition` + ack on the internal marker channel (new message ids allocated next to the D9 marker messages); counters 239 (pid tag refusals), 240 (ownership refusals). pid stays i64 on the wire.

**Tests.** `ProduceValidationRegression.AClientControlBatchIsRefusedOn{Gateway,Worker}Regression`; `ProducerIdentityRegression.{AForeignPrincipalCannotFenceAProducer, AForeignPrincipalCannotSwallowABatch, AnUnregisteredTransactionalBatchIsRefused, SixteenTagFailuresEndTheSession, APidSurvivesFailoverForItsOwner}Regression`; `GroupOwnershipRegression.{LeaveNeedsGroupRead, AnotherSessionsMemberIsUnknownFor{Sync,Heartbeat,Leave,Commit,Rejoin}, MemberIdsAreRandom, TxnOffsetCommitChecksMemberAndTopicRead, FetchAllOmitsUnreadableTopics, ARevokedSessionsMembersAreEvicted, SessionTimeoutOutsideBoundsIsRefused, GroupLimitIsExplicit}Regression`. Invert `LeaveGroupNeedsNoGrant` and `NonTransactionalInitProducerIdNeedsNoGrant`.
**Matrix.** ops {Produce idempotent/txn/control, InitProducerId, AddPartitionsToTxn, EndTxn, TxnOffsetCommit, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch all/explicit} × entity {own id, another session same principal, another principal, guessed id, expired id} × timing {steady, after failover, after rejoin, after revoke, mid-rebalance}.
**Gates.** Produce p50/p99 parity with the pid cache (§13); `BM_PidTagVerify` cold ≤ 50 ns.

## 9. Authorization semantics, visibility and error codes (X046–X062)

- **Horizon (X046).** Every shipped table carries `topicHorizon` = this broker's applied `nextTopicId`. Gateway and worker: an id `≥ topicHorizon` answers `NotLeader` (retriable) before any 45; `0 ≤ id < horizon` and not live follows D22; negative ids never pass D22 (X049). (Catalog ids are monotonic, `ApplyV1.cpp:92-94`; P3 pins that with a test.)
- **Deny flavours (X050).** Shipped per session: `Deny44 | Deny47 | Table{id}`; the worker answers 44/47 like the gateway.
- **Kind-correct checks (X047, X048).** GetTopicSchema resolves the name's kind in `ObjectIndex` and checks only that kind (missing name: either). DefineTopic with a descriptor on an existing untyped topic needs ALTER + DESCRIBE on it.
- **Leak-free answers (X051, X053).** When the requester cannot DESCRIBE the colliding object: `TopicExists` with id −1, version 0, no name; fold-collision and "read by view" details name only the requested name; ViewNotAlterable becomes 45 for non-DESCRIBE holders. GetViewRow authorizes before the C1 gate in every configuration.
- **Admin re-check (X052).** `SecurityAdmin::startDdl_` re-evaluates ADMIN (or the self-password rule) for `meta.authenticated` against its applied state; catalog DDL likewise for the op's permission. Refusal `NotAuthorized`.
- **Dedupe (X055).** The dedupe key includes `principal.authenticated`.
- **Client codes (X056, X059, X060).** ProducerImpl maps 44/45/46/47 to a definite not-appended terminal status; 48 to retriable (§7). One `authErrorName` table in the client used by the CLI. `Completion` gains `retryAfterMs` (v10 API; `aux` keeps it for one release).
- **Statements and audit (X061, X062, CD19).** The leader renders every security statement from the ops (`statementHasUnredactedPassword` deleted); `replayedFrom.principal` is checked against the op's history or cleared; the user name `bootstrap` is reserved (`validName`). `ddlMetaOf_` records `sourceHost` and keeps the egress channel as `replyChannel`.
- **Spec/semantics (X054, X057, X058).** CD23; CD22 (a REVOKE that removes a view creator's source READ logs `view v keeps reading s under definer rights` and counts); `opsOn(Cluster)` and `clusterOps()` agree (D15 applied in both).

**Tests.** `AuthEnforcementRegression.{ATopicPlacedButNotAppliedHereIsRetriableNotUnauthorized (G and WS), NegativeIdsNeverPassD22, WorkerAnswers44And47LikeTheGateway}Regression`; `AuthVisibilityRegression.{AViewGrantNeverDescribesATopic, CreateSchemaOnAnExistingTopicNeedsAlter, CollisionsNeverRevealHiddenObjects, GetViewRowAuthorizesBeforeC1WithoutAPool}Regression`; `SecurityWritePathRegression.{TheRecordedStatementIsRenderedFromTheOps, DedupeIsPerPrincipal, AdminRecheckRefusesARevokedPrincipal}Regression`; `CatalogPrincipalRegression.SourceHostIsRecordedRegression`; `ClientProduceRegression.NotAuthorizedIsDefinitelyNotAppendedRegression`; `PermissionsRegression.ClusterOpsAgreeWithOpsOnRegression`.

## 10. Login, throttle and credential hardening (X063–X076)

- **Charging (X063, X065).** `AuthClientFirst` charges a pending failure to (user, host), host and user; success refunds it; an abandoned first leg stays charged. `check` runs at both legs. At most 4 sessions in `AwaitingFinal` per (user, host) and 64 per host (excess 44 with `retryAfterMs`). The server always computes both fake slots and selects (constant work per first leg).
- **Keys (X064, X067).** Add a per-user backoff: 20 free failures, then `min(50 ms·2^(k−1), 2 s)`; a success from a (user, host) pair known for the user in the last 15 min bypasses it (no lockout of the legitimate host). Tables keyed by SipHash-2-4 with a per-process random key. Source host comes from the fragment's own `Header` image passed through `onClientFrame_`, never `imageBySessionId`.
- **Text rules (X068, X069).** `decodeClientFirst` refuses a nonce containing `,` or non-printable bytes, longer than 96 bytes, and usernames failing `validName`; refused names are logged hex-escaped; per-attempt logs are rate-limited (§12).
- **Bootstrap (X070).** MAC key `HKDF-Expand(master, "rune-bootstrap-mac-v2", 64)`; `SecurityBootstrap` v10 adds `issuedAtMs`; the broker refuses `|now − issuedAtMs| > 10 min`; nonce dedupe stays in the log.
- **Client PBKDF2 (X071).** `ClientConfig::maxAcceptedIterations` (default 100 000): a higher count fails the login locally (44-equivalent, logged). Off-duty-thread PBKDF2 is DEFERRED (§20): the cap bounds the stall.
- **Slots and config (X072, X073).** At most 256 `AuthAwaiting*` sessions per source host; `RUNE_AUTH_TIMEOUT_MS` ∈ [1 000, 600 000].
- **Fake iterations (X074).** Pinned to the iteration count of the Bootstrap event's admin verifier (already in the event; no format change); the modal recompute is deleted (also removes X119's map).
- **Self password (X066, CD18).** Logged at warn with user and source host, counter 241 shared with "audited security events".
- **Docs/verify (X075, X076).** Assert in `ScramCache` that one Client uses one user name; document. Verify the bootstrap-only CLI keepalive against the lease; add a keepalive if the round trip can exceed it.

**Tests.** `LoginThrottleRegression.{ParallelSessionsCannotMultiplyGuesses, AbandonedFirstLegsAreCharged, PerUserBackoffNeverLocksOutAKnownHost, KeysAreUnguessable}Regression`; `AuthLoginRegression.{FirstLegWorkIsIdenticalForUnknownUsers (op-count seam), ATwoPasswordUserIsIndistinguishable, SourceHostIsTheFramesImage}Regression`; `ScramServerRegression.{InvalidNonceIsRefused, LongNonceIsRefused}Regression`; `SecurityBootstrapRegression.{AStaleBootstrapIsRefused, MacUsesTheDerivedKey}Regression`; `ScramClientRegression.IterationsAboveTheCapFailLocallyRegression`; `AuthLoginRegression.FakeIterationsArePinnedAtBootstrapRegression`.
**Matrix.** {first leg, final leg, abandoned} × {known user, unknown, disabled, two-password slot 0/1} × {fresh, throttled pair, throttled host, throttled user, parallel N}.

## 11. Peer link v2 (X077–X100)

**Handshake (CD8, CD9).** Messages on the peer-auth stream (all MAC'd under the cluster key except Refuse):
`PeerHello{v=2, nonceA, keyIds[2], incarnation, epoch, ephA}` → `PeerChallenge{nonceB, keyId, incarnation, ephB, macB}` → `PeerProof{macA}` → `PeerAccept{confirmB}` → `PeerConfirm{confirmA}`; `PeerRefuse{reason}` is informational only. `macB/macA/confirm*` cover every field of both sides incl. ephemerals and epoch. Link keys: `prk = HKDF-Extract(salt = nonceA‖nonceB, ikm = master ‖ X25519(eph))`, `k_dir = HKDF-Expand(prk, "rune-link-v2|<from>-><to>|" ‖ epoch, 64)`; per-sender keys as D33. The responder picks `current` if it holds `keyIds[0]`, else `next` (X092). `epoch` is a random non-zero u32 chosen by the initiator, refused if equal to one held (X090). Incarnation is informational (restart detection); the high-water refusal is deleted (X086).

**State machines (per peer, control thread).** Two orthogonal machines; unauthenticated input can touch only the second, bounded:
- `LinkKeys`: `NoKeys{}` | `Keyed{sendEpoch, recv[3]{epoch, retireAtNs}, peerIncarnation}`.
- `Exchange`: `None{nextAllowedNs}` | `HelloSent{nonceA, ephPriv, epoch, sentNs, tries}` | `ChallengeSent{nonceA, nonceB, ephPriv, peerEph, epoch, sentNs}` | `RecvInstalling{epoch, keys, fence, role, pendingFrame}` | `AwaitingAccept{epoch, keys, sentNs}` | `AwaitingConfirm{epoch, sentNs}` | `SendSwitching{epoch, fence}`.

Install protocol (no loss, X079): A verifies Challenge → `RecvInstalling` (fan-out `PeerLinkKeys{recvOnly}` + fence to every local thread, §6) → on acks sends Proof. B verifies Proof → `RecvInstalling` → on acks sends Accept. A on Accept → fan-out send switch → sends Confirm. B on Confirm → send switch. Old epochs stay receive-only until `retireAtNs = switchNs + 2 s`; three receive slots. Lost Accept/Confirm are retransmitted unchanged by timer.

Rules: either side with `NoKeys` initiates (lower id wins simultaneous Hellos). A Hello while `Keyed` starts a responder exchange without touching `LinkKeys`; one exchange per peer; ≤ 1 new exchange per 200 ms per peer; ≤ 8 handshake frames processed per duty pass. A Refuse, a bad MAC, a stale or unknown field is counted (rate-limited log) and ignored (X078). `UnknownEpoch` frames are counted only; a rekey starts locally only when the peer's link has produced **no authenticated frame for 1 s while frames keep arriving** (X077). `Out` uses a fixed `std::array<uint8_t, kMaxPeerLinkFrame>` and fixed-format log buffers (X085).

**Frames.**
- Receivers accept only their role's sender id (consensus 0, legacy 1, admin 2, worker plane w → 3+w), checked before any crypto; each receiver derives exactly that key at install (no lazy derivation) (X081). AAD = 16-byte header ‖ `toNode u16` ‖ `streamId i32`. Payload sender/member ids must equal the authenticated `fromNode` where the payload carries one (consensus member id, replication `fromNode`) (X093).
- Legacy ABORT (X080): check window (no commit) → open → dispatch → commit iff not ABORT; redelivery re-opens. `pendingOpened_` deleted.
- CD10, CD25 config refusals (X082, X083). Scratch sized from `ReplicationFrameLimits` at construction; distinct `TooLarge` drop result (X087). `AeadCipher::make()` returns `std::optional`; `installLink` reports and logs failure (X088). The 16 KiB consensus limit applies in every mode; oversize warns rate-limited (X089). Counters: 233 = `Keyed` peers, 234 = authenticated refusals only, new 242 = unauthenticated handshake frames dropped, 243 = rekeys, malformed exported (X091). Seal failure wipes the claimed region before `abort()` (X094). Worker checks `TailKind == PEER_KEYS` (X096). `Kdf.h` comment corrected (X100).
- A sealed receiver that sees ≥ 16 plain (non-0xB7) frames from a configured peer logs once: "peer N is not authenticating: enable authentication on every broker at once (full stop)" (X098). Mixed mode stays unsupported; Task 20 documents the stop-all enablement.

**Wire / ids.** Peer-auth version byte 2 (v1 frames refused, counted); peer frame header unchanged (D34) apart from the AAD; `PeerConfirm` is a new peer-auth message kind. Counters 242, 243.

**Redesigns.** RFR facts ring carries the fan-out FenceAck (§16.1). ADW: none (key install is a worker-wide lifecycle op).

**Tests.** `PeerLinkRegression.{AForgedUnknownEpochNeverRehandshakes, ForgedRefuseChallengeHelloNoLinkNeverDisruptALink, ARestartedPeerRelinksUnderAFlood, AFreshDataDirPeerRejoins, RekeyUnderTrafficLosesNoFrameOn{Consensus,Legacy,Admin,Worker}, InitiatorFallsBackToTheNextKey, ForwardSecrecyRecordedTrafficStaysSealedAfterKeyLeak (test vector), HandshakeFloodBoundsControlPassTime}Regression`; `PeerFrameRegression.{ABogusSenderIsRefusedBeforeCrypto (0 allocs), AFrameReplayedOntoAnotherPlaneFails, AbortRedeliveryWithTwoImagesLosesNothing, ScratchSizedFromConfig, SealFailureLeavesNoPlaintext}Regression`; `NodeConfigRegression.{TooManyWorkersRefused, TooManyPeersRefused, NodeIdAbove65535Refused, SealedLegacyReplicationRefused}Regression`; `PeerFrameAllocRegression.{SealedLegacyTransportSteadyStateNeverAllocates, SealedConsensusNeverAllocates, IdleKeyedLinkNeverAllocates}Regression`; TSan run of the rekey-under-traffic test. Fix `AKeylessNodeNeverAffectsTheTerm` to have node 3 send.
**Matrix.** ops {Hello, Challenge, Proof, Accept, Confirm, Refuse, data frame, UnknownEpoch frame} × entity {genuine peer, forged sender, replayed frame, restarted peer, fresh-dir peer, peer with next key only} × timing {NoKeys, mid-exchange, Keyed, rekeying, within grace, after retire, flood}.
**Gates.** §13 peer gates.

## 12. Secrets, file modes and logging (X102–X107)

- **At rest (X102, CD20).** `Log` gains a per-log file mode; `dataDir/__security` 0700, its segments, indexes and checkpoint 0600. Enabled mode refuses a group/world-readable cluster key file. Any future `.rsec` export writes 0600 with `O_EXCL`; the `SecurityFile.h` comment is corrected now (X105).
- **Logging (X103, X104).** One `RateLimitedLog` site primitive (per-site counter, first then 1/1024, suppressed count exported as counter 245). Applied to: worker produce decrypt failure, require-TLS refusal, bad bootstrap proof, every login failure/rate-limit line, peer refusals/oversize, oversized inbound datagram (now warned).
- **Wiping (X106, X107).** rune-crypto: `OPENSSL_cleanse` on scope exit for ClientKey, SaltedPassword and HMAC scratch; `ClusterKey`, `AeadKey`, `PeerFrameCrypto::EpochState`, `PeerHandshake` keys and retired epochs wipe in destructors and on retire; ring slots carrying keys are cleansed after decode; stack copies in `Worker::executePeerLinkKeysWire_` / `AdminWorker::onPeerLinkKeys` wiped. Client: `validatedConfig` reads the password file into a reserved `SecretString`; `installSelfPassword_` stores only the derived cache; `AgentConfig`/`SessionConfig` hold a reference to one secret owned by the Client. rune-ql: `CreateUserDef/AlterUserDef/SecurityPlan::password` and parser token buffers use a wiping string.

**Tests.** `SecurityAtRestRegression.{SecurityLogFilesAreOwnerOnly, AWorldReadableClusterKeyRefusesToStart}Regression`; `WorkerSessionCryptoRegression.DecryptFailuresAreCountedAndRateLimitedRegression`; `RateLimitedLogRegression.FloodLogsOnceThenCountsRegression`; `SecretWipeRegression.{CryptoScratchIsCleansed, RetiredPeerEpochIsCleansed, QlPasswordIsCleansed, ClientHoldsOneSecretCopy}Regression` (scan-after-scope seam).

## 13. Performance, philosophy cleanups and gates (X108–X122)

- **Permission scale (X108).** `PermissionTablePool` on the control thread: one compiled table per distinct effective-grant signature (sorted grant ids; plain refcount, single thread). Workers mirror the pool: `SessionPermissions` ships a table once per worker by table id and sessions reference it (`Table{id}` deny-flavour, §9). Updates ship only changed 64-bit words (delta chunks, staged and applied on the last chunk). Refresh is resumable at 256 sessions per control pass; the fences of §6 wait for it. Compile resolves prefixes with a sorted name index (binary search per grant), not `opsOn` per object.
- **Hot path (X113, X114, X117, X118).** `admitDirect` returns the slot index; grants, table id, crypto and `VerifiedPids` are slot-indexed (one probe). Deny text built in a fixed stack buffer. The Required-mode gate is folded into the dispatch lookup (one header decode, one probe). `lastProbes_` only in test builds.
- **Control plane (X116, X119).** Auth deadlines live in a hashed timing wheel (no second scan); `refreshDerived_` map deleted (X074 pins the count).
- **Flags (X115, X120).** `WriteGrantSlot` → `std::variant<GrantAll, Deny44, Deny47, Table{id, epoch}>`; `LiveActions` → variant per action; `PermissionTable::allowAll_` → mode variant; `ControlledShutdown/MetadataService::securityHalted` → the §7 state; `drainPendingReleases` `static bool sWarned` → per-instance `RateLimitedLog`; ISR `invalidRequestWarned_` → `RateLimitedLog`.
- **Gates** (quiet host, CD26; numbers recorded in `docs/benchmarks/auth.json`):

| Bench | Gate |
|---|---|
| `AuthCheckBench` per-produce check, cache-hot / cache-cold (100k topics × 10k sessions, randomized) | ≤ 2 ns / ≤ 10 ns; 0 allocs |
| `WorkerProduceLatency` + `FusedRuntimePass` with a sink, announced sessions, `RUNE_BENCH_AUTH` A/B, TLS sealed | p50 ≤ +2 %, p99 ≤ +5 % vs auth off; NFR p50 50 µs / p99 200 µs |
| `CatalogHotPathBench` `BM_GatewayViewRowServed` A/B Required; new subscribe-push latency bench | same as above; 0 allocs (`AuthAllocRegression.SteadyStateAuthorized{Consume,ViewRow}NeverAllocatesRegression`) |
| `BM_RoleRefresh10kSessions100kTopics` | max control pass ≤ 200 µs; complete ≤ 2 s; ring messages ≤ workers × distinct tables + deltas |
| `ScramBench` / `ClientLoginBrokerRegressionTest` cold (no cache, two slots, TLS + binding) | ≤ 5 ms native; warm reported |
| `PeerFrameBench` + `BM_PeerLegacyFetchRoundTrip_{Plain,Sealed}` + `BM_PeerConsensusHeartbeatRtt_{Plain,Sealed}` (spins bounded; timeouts fail the gate) | sealed p99 ≤ plain p99 + 5 µs at 1 KiB; 0 drops |
| `BM_PeerHandshakeFlood` | ≤ +10 µs control pass at 10k forged frames/s |
| `BM_EnforceFenceRoundTrip`, `BM_PidTagVerify` | ≤ 2 worker passes; cold ≤ 50 ns |

Latency-gate tests (`DutyPassLatencyGate.*` etc.) run serialized on the quiet host at checkpoints and are excluded from `-j14` local runs (X121).

## 14. Test-quality fixes (X123–X141)

- Invert/repair: `AuthBootstrapWindowRegression.TheWindowReopens…` (asserts MustAuthenticate + a real-client login, X123); `FailureAnswersAreIndistinguishable` adds a two-password user and an op-count parity seam; `LeaveGroupNeedsNoGrant`, `NonTransactionalInitProducerIdNeedsNoGrant` inverted (X127); `SecurityHaltTest.cpp:53` and `AuthLiveChangeTest.cpp:225` assert `gatewayHasSessionForTest(sid) == false` (X128); every denial test asserts LEO unchanged (or no Records within the window) (X129); `AKeylessNodeNeverAffectsTheTerm` makes node 3 send (X125).
- Rig: `AuthRig::{syncGroup, leaveGroup, offsetFetchAll, loginSlot(i)}`; `AuthWireClient` TLS mode; a back-pressure seam (egress window, lifecycle drain delay, admin ring fill) in `AuthBroker`; shard + per-worker egress variants of every live-change test (X124, X126).
- New coverage listed in §§4–12 closes X123–X127, X130 (legacy fixture plaintext/tamper/replay, both directions, admin-plane `__security` sealed end to end), X131 (`SecurityReplicationRegression.{TruncationBelowAppliedHaltsExplicitly, DeposedLeaderRetryReappends}`), X132 (`ScramRegression.TwoVerifierExchangeAcceptsOnlyTheRealSlotRegression` at crypto level), X133 (an external SHA-512 SCRAM vector generated by an independent implementation, checked in), X134 (`CatalogReadServerRegression.SecurityFramesMatchCatalogFramingRegression`), X135 (`SecurityAllocRegression.IdleAdminPassWithSecurityNeverAllocatesRegression`), X136 (separate seams), X137–X140 (catalog test debt: refused-answer counter, relay thunk, config value compare, tmp-dir cleanup in the rig teardown, gate ctests registered in `build-fast`, the 8 drop-during-create cells un-skipped, F6 bench fails instead of skipping).

## 15. Parked catalog / ISR / ledger items (X142–X173)

Done in phase P10 as one sweep (each fix with its `*Regression*`):
- **Client failover (X142, X144, X143).** A Rejected or Closing session fails every control op and group fetch homed on it at once with its terminal code (no own-deadline wait); verify the txn-producer revoke pass and add `TransactionalProducerRegression.RevokeFailsEveryInFlightTxnOpRegression`; remove the single-batch assumption in GroupConsumerHub/RecordsView (`GroupConsumerRegression.MultiBatchRecordsFrameDeliversEveryBatchRegression`).
- **Admin rings (X145, X146).** The `static_assert`; confirm `MaxDescriptorFitsBothAdminRingsRegression` passes at the P0 base.
- **Catalog (X147–X149, X151, X152, X154–X158).** Re-itemize CAT-L-1 and CAT-L-3 from the catalog task-5/task-7 re-review files into the plan; slot enum → variant; Minor-9 test + `detail` lifetime; rebalance excludes halted brokers (`kNodeSecurityHalted` and catalog halt); step-0 marker-dir check, F2 assertion, retired-replica params; coPartitionGroup gets its worker at runtime; DDL admission vs FD budget; checked `metadataServiceForTest()`; drop path N1/N2/R1 (R1: client DDL deadline > broker timeout + hold); R2-M1 restart test; GetViewRow not counted as DDL; `GatewayDataFixture` reps>1.
- **Final round (X159, X163, X165).** ConsensusCandidacy flake triage; dr-bugs stage a/b, gate-script N1/N2, reliable shutdown broadcast M3; TLS subscribe resend regression.
- **ISR (X170, X171, X173).** Joiner crash-window unlink + I-S4 `setFailClosed` for Joiner; CP2 M1–M4, M7, M9, M10 (counters without the admin worker), M12 (test-only branches off hot paths, via compile-time seams), M13; CP3 m1–m11, m14 (m3: the non-coordinator internal report is answered exactly once; m4 report cadence backs off; m5–m11 tests strengthened). M7/M9/M13 are re-itemized from `isr-cp2-review.md` when the plan is written.

## 16. Amendments to the three carryover redesigns

### 16.1 RFR (replica facts ring) — X185, X122
- The per-owner `FactsRing` also carries record type 2 `FenceAck{u8 ring, u64 seq, u8 status}` (§6, §11), an **edge** record flushed in the same pass; the admin thread's acks use the existing a2c ring (`kA2cFenceAck`) because its facts ring is optional for admin-owned replicas.
- `ReplicaFacts` gains `i64 leaderContactNs` (follower: monotonic time of the last fetch response from the current leader epoch; progress-class, conflated at 1 ms). The `__security` replica's facts feed §7 on the control thread from `ReplicaFactsTable`.
- RFR step 3 (control FactsRing) must land before §6; its matrix gains entity `__security` follower and timing "leader contact lost".
- The ISR-L-9 per-fetch lookup is measured in RFR's `BM_WorkerPassIdle`.

### 16.2 ADW (activity-driven worker pass) — X186
- Pass order gains, in `drainIngress_`, the processing of `EnforceFence` (records the ack for the facts flush) and `EndSubscription` (marks the subscription for termination in `pushOwnedSubscriptions_`, which writes the terminal frame after any queued Records). Neither marks a slot ready.
- The R4 hold no longer exists (§6), so `pollWorkerIngress_` never returns `kIngressHeld` for authorization; the `deferred` list keeps only transport refusals.
- The equivalence harness (`ActivitySchedulerEquivalenceRegression`) adds lifecycle ops (permissions, close, fence, end-subscription) to its event alphabet; the matrix gains timing "fence in the same pass as a produce for that session".
- ADW lands after §6 (both rewrite `Worker.h` intake; one order avoids a double rebase).

### 16.3 VLS (view read lease and sink plane) — X187, X188
- Part A's `ReadLease` is renamed `BrokerLease` and lives on the control thread with a public read-only accessor for §7 (same thread). Templates 62/63, `ρ`, `L`, `QL` unchanged. Workers keep `WorkerOp::ReadLease{leaseEnd}`.
- Part A's matrix gains ops {login, −7 read} and the zombie/stale-controller tests also assert §7 answers 48 on the zombie.
- Part B: under auth, a Produce to a sink id is 45 (VIEW holds no WRITE, D17) before the `InvalidRequest` refusal; a Subscribe on a sink id by a READ-VIEW holder gets `InvalidRequest`. B.3's refusals are authorized first. B.6 stays a user decision (default replica-less). Part B is Critical (X188) and moves to phase P0b (it is independent of auth).

## 17. Id registry (all new ids, assigned once)

| Kind | Id | Name |
|---|---|---|
| Error code | 48 | `AuthUnavailable` |
| Versions | client 10, broker 6 | `kClientVersionAuthV2`, `kBrokerVersionAuthV2` |
| WorkerOp | 23, 24 | `EnforceFence`, `EndSubscription` |
| control→admin | 26 | `kMsgEnforceFence` |
| admin→control | 24 | `kA2cFenceAck` |
| Admin request rings | — | split into `kAdminDdlRing` / `kAdminReadRing` (X035) |
| Facts-ring record | 2 | `FenceAck` |
| Peer-auth | version 2, kind `PeerConfirm` | §11 |
| Counters | 1'000'236 … 1'000'245 | serving state; fence lag; 48 answers; pid tag refusals; ownership refusals; audited self-password / security events; unauth peer handshake drops; peer rekeys; gate deadline answers; rate-limited log suppressions |
| Config | — | `RUNE_SECURITY_MAX_STALENESS_MS` 1000, `RUNE_SECURITY_STALE_SESSION_END_MS` 10000, `RUNE_SECURITY_GATE_DEADLINE_MS` 5000, `RUNE_GROUP_MIN_SESSION_TIMEOUT_MS` 1000, `RUNE_GROUP_MAX_SESSION_TIMEOUT_MS` 300000, `RUNE_MAX_GROUPS` 100000; client `maxAcceptedIterations` 100000, `insecurePlaintextAuth`; CLI `--tls-ca`, `--tls-host`, `--insecure-plaintext`, `RUNE_TLS_CA_FILE`, `RUNE_TLS_HOST` |
| Wire fields (v10) | — | TxnOffsetCommit `memberId, generationId`; SecurityDdl ext `flags` (FORCE); SecurityBootstrap `issuedAtMs`; ErrorResponse / DdlResponse trailing `retryAfterMs u32`; internal marker channel `RegisterTxnPartition` + ack |

## 18. Implementation phases

Each phase ends with a checkpoint: build, affected-area tests, then the FULL `*Regression*` suite in every repo the phase touched, under ASan+UBSan and TSan (broker `build-asan`/`build-tsan`, client `build_tsan`). Docker/dr-e2e only on the k3s VM.

| Phase | Content | Depends on | Repos |
|---|---|---|---|
| P0 | Guards that need no redesign: §5 (X012–X014); X036; X015 (waiter check); X039 READ + owner check; X041; X047; X049; X029, X030, X033; CD20 file modes (X102); RateLimitedLog (X103); CD25 + CD10 config refusals (X082, X083); CD2 TLS-var refusals (X003); verify X146 | — | broker, client (small) |
| P0b | VLS Part B (X188) | — | broker, client (metadata) |
| P1 | §4 session identity & transport (CD1–CD7), v10 versions, test-rig migration to `PlaintextForTest` + TLS column | P0 | protocol, crypto, broker, client, CLI |
| P2 | RFR with §16.1 amendments | P0 | broker |
| P3 | §6 enforcement acknowledgement + §13 permission pool / delta / one-probe + §9 horizon & deny flavours | P1, P2 | broker, protocol (internal ring ids) |
| P4 | ADW with §16.2 amendments | P3 | broker |
| P5 | VLS Part A as `BrokerLease` (§16.3) | P2 | broker, protocol, client (`knownLeaderEpoch`) |
| P6 | §7 security freshness, code 48, last-admin guard, client 48 + CD21 | P3, P5 | broker, protocol, client, ql |
| P7 | §8 ownership binding | P1 (P3 for revoke-on-16-failures) | broker, protocol, client |
| P8 | §11 peer link v2 | P2 (fence acks); parallel with P3–P7 | broker (peerlink, consensus, replication), crypto |
| P9 | §9 (rest), §10, §12 (wiping) | P1, P6 | broker, crypto, codec, ql, client |
| P10 | §13 gates + cleanups, §14, §15 sweep; checkpoint **CP-F**: everything in X168 (full suites all repos, sanitizers, dr-e2e 15 cases + docker on VM, quiet-host benches) | all | all |

After CP-F: auth Tasks 17–20 with §19, then the final whole-branch review.

## 19. What auth Tasks 17–20 must additionally cover

- **Task 17 (matrix).** Add ops `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetFetchAll`, `InitProducerIdIdempotent`, `TxnOffsetCommit`, `ProduceForeignPid`, `ProduceControlBatch`, `GetTopicSchema`; add entity `OwnedByOther` (id of another session / principal) to every op that names a member, producer, txn, subscription or view id; add timings `BackPressure` (egress full, lifecycle drain delayed) and `BrokerStale`; run every cell over TLS and in both shard+per-worker-egress and legacy runtimes. Every cell additionally asserts nothing was appended / delivered on denial (RESIDUAL-8), the session is gone from the table on revoke (RESIDUAL-7), and no Records after the terminal frame on the owner's publication. Re-pin cell counts.
- **Task 18 (stress).** Alphabet adds ownership ops, foreign pids, control batches, broker restart / staleness, fence delays; invariants add "no record after end", "OK ⇒ enforced on the answering broker", "no foreign pid accepted".
- **Task 19 (3-node).** Shard mode + TLS; restart with a missed revoke (48 then Serving, old password refused); wiped and new broker; partition < F, F..R, > R; forged peer frames (UnknownEpoch, Refuse, Hello flood) never break links; rekey under traffic loses nothing; key roll with NEXT unset on step-1 initiators; fresh-data-dir rejoin; sealed per-worker replication bench gates; a GRANT enforced cluster-wide within F (document the D24 scope).
- **Task 20 (launchers, docker e2e, docs).** Launchers generate TLS certs and mount cluster keys 0400 (k8s `defaultMode: 0400`); CLI `--tls-ca/--tls-host` in every script; docker e2e runs with TLS required; the mid-DDL kill after the CLI connects (X167); docs: mandatory TLS, the stop-all enablement procedure (X098), code 48 and the staleness bounds, the D24 scope, D28 runbook (48), definer-rights views (CD22), FORCE, PBKDF2 cap, Aeron framing is unauthenticated (X095), plan text vs code (X189).

## 20. Deliberately deferred minors

| Row | Reason |
|---|---|
| X099 (PEER-12) | Copy elimination is an optimization; done only if the §13 sealed gate fails. |
| X071 (part: off-thread PBKDF2) | The iteration cap bounds the duty-thread stall; a salting ring is a client threading change for no remaining risk. |
| X150 (CAT-L-5 M7) | Catalog log growth needs a new formatVer and its own design before release. |
| X153 (CAT-L-7) | Electability of a lagging-on-create replica depends on an acks=1 durability decision outside auth. |
| X155 (part: segment.bytes immutable) | Feature, not a defect. |
| X160 (HOL stall) | Own spec next phase (already ruled). |
| X161 (clock-backwards stall) | Needs the consensus clock design; tests use the injected clock. |
| X164 (handshake-complete ack) | Needs a spec; failover works without it. |
| X176, X178–X180, X182, X184 (ISR §9 residuals) | Each is an ISR protocol design (cluster id on the wire, old-controller race, KIP-966, node-control cut, first-epoch blind spot, feature level < 2); none is reachable by a client. |

## 21. Not resolved here

1. **VLS B.6** (replica-less sinks vs keeping a changelog) is still a user decision; the design assumes the default (replica-less).
2. **CD10 forces per-worker replication for sealed multi-node.** Shard mode (the default) already does; non-shard legacy-runtime deployments with auth must switch. P0 must confirm per-worker replication passes the dr-e2e suite in non-shard mode before the refusal ships; if it does not, the fallback is worker-side seal plus a gated control-thread open (§13 peer gate).
3. **KIP-890 registration channel (§8):** the exact D9 marker-channel message layout and whether a hanging-txn reaper already exists were not traced (OPMATRIX-2 scenario 3 is PLAUSIBLE); P7's first step traces them.
4. **pid tag (CD16)** assumes no code treats pids as dense or small (PSM arrays, allocator blocks); P7's first step verifies.
5. **Bootstrap `issuedAtMs`** assumes CLI and broker wall clocks agree within 10 minutes; operators with worse skew need NTP (documented).
6. **REDTEAM-4 (X067)** depends on Aeron image demultiplexing for colliding session ids, unverified; the fix (fragment's own image) is correct either way.
7. **CAT-L-1, CAT-L-3, ISR-L-2 M7/M9/M13** are unitemised in the inputs; they are re-itemized from the per-task review files when the plan is written.
