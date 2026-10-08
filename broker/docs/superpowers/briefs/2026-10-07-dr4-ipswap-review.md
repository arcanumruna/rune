# DR-4 IP-swap fix — review (2026-10-07)

Scope: uncommitted diff on wt26 HEAD dc274e8 (`dr4-fix.diff`): numeric peer adds
(`PeerResolutionInbox::addChannel`), IP-change re-add in the three peer-publication
owners, Election CANVASS NLT exception, tests, harness `RUNE_IMAGE_TAG`.
Read-only review; nothing built or run.

**Verdict: APPROVE-WITH-CHANGES.** The transport half (items 1–2) is correct in its
main path and shared-nothing-clean. It needs two MEDIUM fixes: an abandoned
in-flight registration leaks, and the legacy plane releases a publication from the
worker thread. The Election change (item 3) has one MAJOR finding: it weakens the
durable-term rule that the codebase's own ack-term safety argument depends on.
Change the gate, or replace it, before merging.

---

## What was verified OK

- **Threading of the attach-time drain.** `attachResolution()` is called from
  `buildReplication()` (BrokerNode.cpp:842, 903, 935). The `UdpMessageSink` ctor
  is called from `buildMultiNodeConsensus()` (BrokerNode.cpp:697). Both run on the
  startup thread inside `start()` (lines 446/472), which is before
  `agentRunner_->start()` (514), the shard runners (536), and
  `resolverHub_->start()` (568). The ring is therefore drained by its only consumer
  while the producer is still idle, and ownership passes to the owner thread through
  thread creation. At drain time `peers_`, `resolving_` and `pubCount_` are all
  still empty, so the drain only fills the inbox entries. Safe.
- **Re-add storms.** The hub re-posts a confirmation about every 1 s. All three
  owners `continue` when `ip == boundIp`. `boundIp` is refreshed on every add
  (`registerPeer` / `registerPub_` / `addNow_`), so a confirmation of the same
  address never triggers a re-add. The three new tests pin this.
- **Hot path.** No new per-send or per-pass work.
  - `UdpMessageSink::send` and `AeronPartitionTransport::poll` still call
    `inbox_.poll` only. The new code runs inside the event callback.
  - `AeronPubRegistry` keeps `pubs_` (the hot flat array) unchanged. The new
    cold data lives in a separate `cold_` array placed after `pubs_`.
  - `drivePeerResolve` still early-outs on an empty `resolving_`.
- **Numeric channel vs. string matching.** No code matches publications by
  `channel()`; lookups go through the registration id. `AeronPartitionTransport`
  `PeerLink::host` is still built from the configured (hostname) egress channel, so
  image attribution behaves as before.
  `withEndpointHost` keeps the `:port` and every `|param`. It does not confuse
  `control-endpoint=` with `endpoint=`.
- **IPv6 and numeric hosts.** These are unmanaged (`hostIdx == -1`), so
  `addressOf` returns false and `addChannel` returns the channel unchanged. No
  behaviour change.
- **`UdpMessageSink` `++unresolved_`.** This is needed because `resolvePending`
  early-outs at 0 and the event callback runs before that check. It is harmless
  when the value is too high, because `resolvePending` recomputes it.
- **`AeronPartitionTransport` link edges.** After a re-add, `pub.reset()` produces
  a down edge, which drops the held frames. The new publication then produces an up
  edge, which re-sends the markers. This is the intended reconnect semantics.
- **Rewritten `*AfterANonDnsAddFailure` tests.** The driver hub cannot resolve the
  hostname at all, so these tests prove that the adds are numeric. That is good
  direct evidence for item 1.

---

## MAJOR

### MAJOR-1 — The Election CANVASS exception re-opens "follow a deposed leader" whenever the restored `leadershipTermId_` lags the deposed term (Election.cpp:951)

The patch accepts an NLT in CANVASS when `leadershipTermIdArg > leadershipTermId_`,
even if `nextLeadershipTermId < candidateTermId_`. After a restart,
`leadershipTermId_` is only the TermLog value, while the durable `candidateTermId_`
is the only record of the highest term this node participated in. The patch
assumes the higher `candidateTermId_` came from the node's own failed candidacies.
It can also come from a **granted vote**, or from an adopted higher term.

Counterexample (3 nodes Y, Z, X):

1. Y leads term 3. It was elected with Z while X was down, so X's TermLog stays at 2.
2. Y gets partitioned from Z.
3. Z and X elect term 4: X **grants** its vote and persists `candidateTermId = 4`.
4. X crashes before Z's NLT arrives. TermLog is still 2, and `votedTermId_` (not
   persisted) is lost.
5. X restarts in CANVASS and receives the zombie Y's heartbeat NLT with term 3:
   `3 > 2`, so the NLT is now **accepted**.
6. X follows Y in term 3. `ackTermId()` = max(0, 3) = 3, so X acks the zombie's
   internal-ISR states. Y can now commit through {Y, X} in term 3, while Z
   appended a term-4 entry and wins any later log comparison on term.

This is the Raft durable-currentTerm violation that the existing design explicitly
relies on:

- `Election.h:459-466` (`votedTermId_`, round-4 LOW-R4-4): *"the durable
  candidateTermId_ makes the election ignore the stale term's NewLeadershipTerm (a
  node never re-follows a deposed leader)"*.
- `AckTerm.h:16-18`: *"a node that stood at T+1 ignores T's NewLeadershipTerm
  (Election onNewLeadershipTerm 'stale ballot')"*.

After this patch both comments are false. The DR-10 LOW-6 protection now holds only
in the equal-term special case (`leadershipTermIdArg == leadershipTermId_`).

**Is it worse than Aeron?** No. Aeron's CANVASS branch accepts any NLT whatever the
candidateTermId. But rune deliberately tightened this rule and built the
internal-ISR ack-term safety argument on top of it, so the patch is a regression
against rune's own invariant. The scenario is narrow: it needs a crash right after
a vote grant, plus a reachable zombie. But it ends in silent committed loss.

**Required change (pick one):**

- **(a) Persist the granted-vote term** (`votedTermId_`) in the NodeStateFile, next
  to the candidate term, using the same fsync. Then gate the exception on
  `leadershipTermIdArg >= persistedVotedTermId`: the node never granted, adopted, or
  followed anything newer than the sender's term. Own candidacies do not raise
  `votedTermId_`, so DR-4 still converges. The grant case above is refused. Any
  path that adopts a higher term (A1 / ClosedLeaderStepDown adoption) must also
  raise the persisted value.
- **(b) The etcd answer (raft.go, "leader at a lower term" with CheckQuorum).** Keep
  the stale-ballot refusal. When a CANVASS node receives a lower-term NLT, it
  answers the leader with its higher term (for example a CanvassPosition or
  RequestVote carrying `candidateTermId_`). The live leader steps down for a member
  that is provably not following it, and a fresh election at a term above
  `candidateTermId_` takes the stranded node back in. This also closes MEDIUM-3.
  Stickiness must not suppress this specific signal.

Either way, add a negative regression test next to
`CanvassFollowsLeaderBelowOwnCandidateTermRegression`: grant a vote at term 4,
restart with TermLog at 2, send an NLT for term 3, and assert the node is still not
following.

---

## MEDIUM

### MEDIUM-1 — Re-adding over an in-flight registration leaks a driver publication on the stale address (all three owners)

Affected code:

- `UdpMessageSink::onHostResolved_`: the `registered && !pub` case falls through to
  the address check (UdpMessageSink.h:218-237).
- `AeronPartitionTransport::onHostResolved_` (AeronPartitionTransport.cpp:1173-1192).
- `AeronPubRegistry::onHostResolved_` loop 1, which does this explicitly
  (WorkerReplicationTransport.h:514-524).

In all three, `registrationId` / `regId` is overwritten while the previous add is
still unanswered. The Aeron C++ `ClientConductor` only releases a publication when
an `ExclusivePublication` object is destroyed (`releaseExclusivePublication`), and
that object only exists once `findExclusivePublication` has been called. A
registration that is never found stays in `m_exclusivePublicationByRegistrationId`
for the life of the client. `onCheckManagedResources` reaps only log-buffer and
image lingers, not this map.

So the driver keeps a NetworkPublication on the **old** address, which is the other
member. It sends setup frames and heartbeats there forever, and leaves a dead
image on that member's subscription. Each additional flap leaks one more.

**Fix (simplest, and time-independent):**

- **`UdpMessageSink` and `AeronPartitionTransport`:** do not re-add while an add is
  in flight. Change the branch to `if (!slot.pub) continue;` after the
  `!registered` case. When the add completes, `boundIp` is still the stale address,
  so the next hub confirmation (about 1 s later; the hub always re-posts) takes the
  resolved-path re-add, which releases the publication properly.
- **`AeronPubRegistry`:** drop loop 1 for the same reason. The resolved-path loop 2
  catches the publication once `probe_` installs it.

### MEDIUM-2 — Legacy planes release a node-client publication on the worker thread (WorkerReplicationTransport.h:541)

For a legacy per-worker plane (`broker_ != nullptr`), `pubs_[i].pub` came from the
**node** client through `PubAddBroker`. The new `pubs_[i].pub.reset()` runs on the
worker thread inside `drivePeerResolve`. If that is the last reference, the
`ExclusivePublication` destructor calls `ClientConductor::releaseExclusivePublication`.
That takes the node client's admin `recursive_mutex` and writes to the driver proxy
from the worker thread.

This is exactly the cross-thread admin-lock path that `PubAddBroker` exists to
prevent (PubAddBroker.h:5-9, review MEDIUM-2). Legacy planes are opt-in
(`perWorkerReplication` defaults to false), so this does not affect the default
config.

**Fix:**

- **Preferred:** hand the old `PubPtr` back to the control thread through a
  `kRelease` broker message, the mirror of the existing holder hand-off.
- **Alternative:** skip the IP-change re-add for brokered planes and record that
  in the backlog, together with the item below.

**Deferring `PubAddBroker`'s hostname channel is acceptable.** It still adds on the
hostname channel, which is resolved by the driver's cache. That cache receives the
hub event before the owner (hub program order), the path is opt-in and not the
default, and a stale-cache add self-corrects on the next confirmation (`boundIp`
mismatch, then a re-add). To make it complete, have `requestAdd` carry the numeric
address, or have the broker hold an inbox. Track this in the backlog.

### MEDIUM-3 — A residual stranding remains in the equal-term case (Election.cpp:951, pre-existing)

Scenario: a follower X of a live term-T leader restarts with `leadershipTermId_ = T`.
Before the first heartbeat reaches it, X runs one or more of its own failed
candidacies, which raise `candidateTermId_` to T+1 or higher. This can happen
whenever the leader's publication to X is slow to connect or misrouted.

X now refuses the leader's NLT: the term is equal, so the new exception does not
apply. The leader's RequestVote stickiness denies X's candidacies, so X is stranded
exactly as in DR-4. Whether X reaches this state depends on timing (canvass timeout
vs. heartbeat arrival), which breaks the time-independent convergence rule.

Option (b) of MAJOR-1 closes this. Option (a) does not. If you choose (a), log
MEDIUM-3 to the backlog with option (b) as the fix.

---

## LOW

- **LOW-1 (WorkerReplicationTransport.h:541-546).** `pubs_[i].pub.reset()` runs
  before `resolving_.push_back(...)`. If the push fails (the `Resolving` string copy
  allocates), the peer is left with no publication and no `Resolving` entry, so it
  stays down forever: loop 2 skips it on `!pub`, and loop 1 has no entry for it.
  Push first and reset after the push succeeds. The old publication then survives
  until the next confirmation retries.
- **LOW-2 (AeronPartitionTransport.cpp:1191).** `++peer.attempts` makes the later
  `"resolved after {} failed attempt(s)"` info log count IP-change re-adds as
  failures. This is cosmetic: use a separate counter or reword the log.
- **LOW-3 (UnresolvedPeerRegressionTest.cpp:439-472).** Three test issues:
  - The `*ReAddsAfterANonDnsAddFailureRegression` tests now assert *no* re-add, so
    their names are wrong.
  - `LaggingDriverCache::driverCanAddNow` and `ownerConfirmation` are now unused.
  - The fix-round-2 MEDIUM-A property (re-add on the next confirmation after a
    *non-DNS* add failure, such as fd exhaustion) has lost its only coverage, while
    `onFailed_` / `onPubFailed_` and their generation logic are still live.

  Rename the tests and keep a fault-injected case for that path. A channel the
  driver transiently rejects would do.
- **LOW-4 (UnresolvedPeerRegressionTest.cpp:359-437).** The IP-change tests assert
  only the re-add **counts**. They would also pass if the re-add went back onto the
  old address. Where reachable, assert that the re-added publication's `channel()`
  contains `endpoint=127.0.0.2:<port>`. That proves the numeric target, which is
  the actual fix, on loopback.
- **LOW-5 (EpochRestartRegressionTest.cpp:216-224).** The "controller leads a
  partition" lambda returns `true` on both branches, so the `settledWith(...) ==
  controller` loop is dead code and the assertion is just "all three agree on a
  leader". Simplify it, or actually wait for the controller to lead a partition.
  Also, the brief does not claim this test was RED at HEAD. If it is green at HEAD,
  it pins recovery but is not a regression test for this fix. Note that in the
  comment.
- **LOW-6 (PeerResolverHub, pre-existing).** If a hostname has several A records
  and `getaddrinfo` ordering is not stable, the "changed address" event fires every
  re-check. That now means a publication re-add every second. Docker container
  names and k8s per-pod DNS return one record, so this is backlog material:
  re-add only when the bound address is no longer in the record set. The setup is
  also IPv4-only (the hub uses `AF_INET`), so an IPv6-only hostname never resolves.
  That is unchanged by this patch.
- **LOW-7 (dr-e2e/harness/cluster.py:76).** A leftover `RUNE_IMAGE_TAG` in a shell
  silently runs a stale image against the current SHA, which works against
  [docker-image-tag-policy]. Log the effective tag, and its source (env or git),
  at cluster start.
- **LOW-8 (UdpMessageSink.h:231).** `fprintf(stderr, …)` while the replication
  owners use `spdlog::warn`. Acceptable if the consensus library intentionally
  avoids spdlog. Otherwise, make it consistent.

---

## Summary table

| ID | Sev | Where | One-liner |
|---|---|---|---|
| MAJOR-1 | MAJOR | Election.cpp:951 | A CANVASS node can follow a deposed leader after it granted a newer vote; this breaks the LOW-R4-4 / AckTerm invariant. Persist the granted term, or use the etcd lower-term step-down. |
| MEDIUM-1 | MEDIUM | UdpMessageSink.h:218, APT.cpp:1173, WRT.h:514 | A re-add over an in-flight registration leaks a driver publication on the stale address. |
| MEDIUM-2 | MEDIUM | WRT.h:541 | A legacy plane releases a node-client publication on the worker thread (admin lock). |
| MEDIUM-3 | MEDIUM | Election.cpp:951 | Equal-term stranding remains, and is timing-dependent. |
| LOW-1..8 | LOW | various | reset-before-push, log wording, test names/coverage, count-only asserts, dead lambda, multi-A flaps, image-tag logging, stderr logging. |
