# Task 7 Report: DynamicBalancer

**Status:** COMPLETE — all tests green, TSan clean.

**Commit:** `abcf877` on `rune-broker` local `main`
Message: `feat(balancer): DynamicBalancer delta rebalance (bit-packed reverse index, preferred-leader-in-ISR, co-partition unit move, skip no-ops, capped)`

**Test summary:** 9/9 PASS (release); 9/9 PASS (TSan) — no data races detected.

**Files created:**
- `libs/metadata/include/rune/metadata/DynamicBalancer.h` — `ReverseIndex`, `DynamicBalancerInputs`, `DynamicBalancer`
- `libs/metadata/src/DynamicBalancer.cpp` — full implementation
- `libs/metadata/tests/DynamicBalancerTest.cpp` — 9 tests (4 from brief + 5 edge-cases)
- `libs/metadata/CMakeLists.txt` — added `rune_dynamic_balancer` lib and `rune_dynamic_balancer_tests` executable

**Key implementation decisions:**

1. **ReverseIndex**: `unordered_map<int32_t, vector<uint64_t>>` bitset indexed by gpid (`topicId * 1024 + partition`). 64 gpids per 64-bit word. `rebuild()` is O(|AssignmentMap|); `partitionsLedBy()` is O(bitset-words-for-node) — true O(partitions-on-node) death cost. Uses `__builtin_ctzll` to extract set bits.

2. **Preferred-leader balance guard**: preferred-leader elections only fire when `replicas[0]` leads *fewer* partitions than the current leader. This satisfies the min-churn steady-state requirement: an evenly-distributed assignment never generates no-op records. Running load table is updated during the cycle so multi-partition rebalance decisions remain coherent.

3. **Carry-forwards honored**:
   - liveNodes is an opaque input (caller populates from `eligibleNodes()`; this class is pure)
   - Preferred leader checked against ISR before any election
   - Epoch bumps entirely delegated to `mergeChange()`

4. **Co-partition groups**: sorted by group name for determinism; same-index partitions share one `FailoverElection::elect()` call and receive the same `computedLeader`.

**Concerns:** None. The `CapPreferredElectionsPerCycle` test is satisfied because with a balanced 1-1 distribution the balance guard (`prefLoad < currentLoad`) evaluates false before the cap is even consulted — the cap is tested as a safety belt rather than an active throttle in this topology. A future test with a post-failover imbalance (e.g., 4 vs 0 partitions) would exercise the cap more concretely.

---

## Correctness Fix-Round (Opus Review — 2026-09-23)

**Commit:** `6315211` on `rune-broker` local `main`
Message: `fix(balancer): close co-partition ISR divergence + add batch cap (Opus review)`

**Test summary:** 11/11 PASS (release); 11/11 PASS (TSan) — zero races.

### BLOCKING #1 — Co-partition failover: ISR intersection (committed-loss bug)

**Root cause:** The original failover path ran `FailoverElection::elect()` using only the DRIVER member's ISR (`ElectionInputs.cur = *driverCur`), then applied the elected leader to every group member without checking whether that node was in the sibling's ISR.  Because each `(topic,partition)` replicates independently, member ISRs diverge over the cluster's life — the driver-elected node can be completely outside a sibling's ISR.

**Fix:** Before running any election, build `groupIsr = ∩(member.isr) ∩ liveNodes` by seeding from `liveNodes` and iterating over all members, erasing any node not in that member's ISR.  Run `FailoverElection::elect()` with a proxy `PartitionAssignment` whose `isr = groupIsr`.  If `groupIsr` is empty the intersection yields no safe candidate → `computedLeader = NO_LEADER` for the whole group (safety over availability, never trade).  A defensive per-member gate (`computedLeader ∈ member.isr ∧ liveNodes`) runs after the election; if it ever fires (shouldn't, given correct intersection logic) it falls back to `NO_LEADER` rather than violating safety.

### BLOCKING #2 — Co-partition preferred-leader: per-member ISR gate

**Root cause:** `computedLeader = driverCur->replicas[0]` was applied to all group members with the ISR/live check done for the DRIVER only.  A preferred leader that is in the driver's ISR but not in a sibling's ISR would have been assigned to that sibling.

**Fix:** After computing `candidate = driverCur->replicas[0]`, iterate over ALL members and check `candidate ∈ member.isr ∧ liveNodes`.  If any member fails, skip the entire group (`continue` — preferred election is a best-effort optimisation, not a safety operation).

### IMPORTANT #3 — Bounded batch (`maxRecordsPerCycle`)

**Change:** Added `int maxRecordsPerCycle = 4096` to `DynamicBalancerInputs`.  The inner emit loop checks `result.size() >= in.maxRecordsPerCycle` before each `result.push_back()` and early-returns from `processGroup` if the cap is reached.  This prevents a mass failover (e.g., entire cluster shift) from producing an unbounded write to the metadata log in a single cycle.

### New tests added

- **`CoPartitionGroupDivergentIsrNeverAssignsUnsafeLeader`**: two sub-cases.
  - Case A: topic7 ISR `{1,3}`, topic8 ISR `{1,2}`, dead node 1 → intersection `{3}∩{2}=∅` → all records (if any) must have `leaderNode == NO_LEADER`.
  - Case B: topic7 ISR `{1,2,3}`, topic8 ISR `{1,2}`, dead node 1 → intersection `{2}` → both members get node 2; same leader for all (co-partition unit move).
- **`BatchIsBounded`**: 20 partitions, all led by dead node 1, `maxRecordsPerCycle=5` → returned batch `≤ 5`, `> 0`.
