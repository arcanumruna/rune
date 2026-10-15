# Rune Broker — Disaster-Recovery E2E Suite Reference (DR-9)

This document is the authoritative reference for the `dr-e2e/` test suite.
It is generated from the actual test source files and is accurate to the code.

---

## Table of Contents

1. [Overview and How to Run](#1-overview-and-how-to-run)
2. [Fault Taxonomy and Ticket Map](#2-fault-taxonomy-and-ticket-map)
3. [Invariant Set](#3-invariant-set)
4. [Durability Contract](#4-durability-contract)
5. [Docker Desktop Caveats and Linux-CI-Only Skips](#5-docker-desktop-caveats-and-linux-ci-only-skips)
6. [Bug Catalogue (DR-BUG-1..6)](#6-bug-catalogue)
7. [Status Summary Table](#7-status-summary-table)

---

## 1. Overview and How to Run

### Prerequisites

- **Docker Desktop** (macOS) or Docker Engine (Linux). netem is proven on Docker Desktop's LinuxKit VM; iptables-in-netns is unreliable there (Linux CI only).
- **Images built once before running:**
  - Broker images: `cd rune-broker && docker compose -f docker/docker-compose.yml build`
  - Client image: `rune-dr-client:<client sha>` — the harness builds it itself (five build contexts); `DR_CLIENT_IMAGE` must carry a matching `rune.client.sha` label
  - Netem sidecar (`rune-netem:dev`): built automatically on first network-fault test

### Install and run

```bash
cd rune-broker/dr-e2e
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run the full suite (minus soak cases):
pytest -v -m "not soak"

# Run the full suite including soak:
pytest -v

# Rebuild broker images on bring-up:
DR_E2E_BUILD=1 pytest -v

# Run only baseline (no faults, fastest):
pytest -v -m baseline

# Run network-fault tests only:
pytest -v -m netem

# Run all fault tests (no netem, no soak):
pytest -v -m "fault and not netem and not soak"
```

### Markers

| Marker      | Meaning                                                                      |
|-------------|------------------------------------------------------------------------------|
| `baseline`  | No-fault correctness floor (DR-1, DR-2 quick matrix)                        |
| `fault`     | Fault injection cases (all DR-3..15 active tests)                           |
| `netem`     | Requires the netem sidecar and `netem_ready` fixture; network-fault cases   |
| `soak`      | Heavy, long-running variants (DR-2 soak matrix, DR-8 chaos soak)            |

### Fixtures

| Fixture         | Scope    | Description                                                             |
|-----------------|----------|-------------------------------------------------------------------------|
| `cluster`       | session  | Brings up the 3-node cluster once for the whole session. Used by baseline (DR-1, DR-2). The cluster's log persists across cases; each case gets a disjoint CID window via `_next_cid_base()`. |
| `fresh_cluster` | function | Full `up`/leader-wait/`down -v` cycle per test. Used by all fault tests (DR-3..15) that mutate cluster state. Teardown always calls `faults.heal_all()` then `c.down()`. |
| `netem_ready`   | session  | Builds the `rune-netem:dev` sidecar image once. All `@pytest.mark.netem` tests implicitly depend on it. |

### Serial execution

The suite uses a **single compose project** (`rune-dr` for session-cluster, `rune-dr-fault` for `fresh_cluster`). Do not run two pytest sessions in parallel against the same project — they share the Docker network namespace. The `fresh_cluster` fixture brings up and tears down a fresh cluster per test, so any two `fault`-marked tests that run in the same session are automatically isolated.

### Environment variable

| Variable       | Default | Effect                                            |
|----------------|---------|---------------------------------------------------|
| `DR_E2E_BUILD` | `0`     | Set to `1` to force `compose build` on cluster up |

---

## 2. Fault Taxonomy and Ticket Map

### Quick legend

- **acks=All** — majority-durable: every acked record is flushed to a quorum of ISR nodes before the `ProduceResponse` is sent (GR-9 / Milestone C).
- **acks=Leader** — leader-page-cache-durable: records are acknowledged after a group-commit (~10 ms window) on the leader only; a small tail may be un-replicated at hard crash.
- **CID window** — each ticket uses a disjoint integer range for client IDs (cid) so that records from different test runs never intermingle on the shared (module-scoped) cluster log.

---

### DR-1 — Baseline parametric matrix

**Fixture:** `cluster` (module-scoped)  
**Markers:** `baseline`  
**CID window:** `n * 1000` (n = run counter, max ~16 000)

**Matrix:** `clients × {1, 3}` × `keys × {1, 4}` × `from × {earliest, latest}` × `target × {leader, follower}` = **16 cases**

| Scenario | Key assertions |
|----------|---------------|
| `from=earliest` (all 8 sub-cases) | All clients exit cleanly; all produced records delivered; `assert_no_committed_loss`, `assert_per_key_ordering`, `assert_exactly_once`, `assert_single_leader` before and after |
| `from=latest` (all 8 sub-cases) | Producer count is fully verified; weaker delivery check (delivered ⊆ committed — no exact count required since some records precede the subscribe); `assert_per_key_ordering`, `assert_exactly_once` |

**Notes:**
- `target=follower` cases use the serving-node fallback introduced to work around DR-BUG-1 (now fixed); the leader/follower axis still varies the case id and `assert_single_leader` is asserted against the real leader.
- The session cluster's log is persistent; disjoint CID windows ensure earlier runs' records never confuse invariants.

---

### DR-2 — Multi-client concurrent baseline

**Fixture:** `cluster` (module-scoped)  
**Markers:** `baseline` (quick matrix), `soak` (heavy matrix)  
**CID window:** `100 000 + n * 2000`

**Quick matrix:** `clients × {1, 3, 5}` × `data_state × {(1,100),(4,500)}` × `partitions × {1,3}` × `target × {leader,follower}` × `repetition × 2` = **48 cases**

**Soak matrix:** `clients × {3, 5}` × `data_state × {(16,1000)}` × `partitions × {1,3}` × `repetition × 1` = **4 cases**

| Scenario | Key assertions |
|----------|---------------|
| Quick (all 48 cases) | All `n` clients exit cleanly (exit=0, rc=0); all produce counts match; partition coverage verified for `partitions=3` (`produced_parts == {0,1,2}`); `assert_no_committed_loss`, `assert_per_key_ordering`, `assert_exactly_once`, `assert_single_leader` |
| Soak (4 cases) | Same invariants; 16 keys × 1000 records × 3-5 clients = 3000–5000 records total |

**Notes:**
- `target=follower` cases exercise redirect-follow: follower returns a Redirect; client retargets to the leader. DR-BUG-2 fixed the head-of-line-blocking follower redirect that caused intermittent connect timeouts under concurrent client load.
- `connect_retries=0` is enforced for all cases after DR-BUG-2 fix (first-try connect success required).
- For `partitions=3`, at least 18 distinct keys are used to guarantee FNV-1a routing covers all 3 partitions deterministically.

---

### DR-3 — Leader kill

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `300 000 + n * 3000`

**Parametrized cases:**
- `test_dr3_leader_kill`: `timing × {idle, mid_produce}` × `n_clients × {1, 3}` × `rep × {0, 1}` = **8 cases**
- `test_dr3_rolling_leader_kill`: `rounds × {2, 3}` = **2 cases**

Total: **10 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| `idle` | Commit acks=All, then SIGKILL the idle leader | Re-election (new leader ≠ old); `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once`; post-recovery produce+subscribe round-trip |
| `mid_produce` | SIGKILL leader while rate-limited (40 Hz) producers are running | Same as idle; in-flight produces that bail are acceptable — only acked records checked |
| `rolling` | Kill leader → wait for new leader → restart killed node → repeat N rounds | After N rounds: single produce+subscribe succeeds; `assert_single_leader`; no committed loss on acked set |

All producers use `acks=all` (GR-9). Every `PRODUCED` line is majority-committed. The `no_committed_loss` assertion is therefore iron-clad.

---

### DR-4 — Quorum scenarios

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `400 000 + n * 3000`

**Cases:**
- `test_dr4_quorum_holds_kill_follower`: `n_clients × {1, 3}` = **2 cases**
- `test_dr4_quorum_lost_safety_and_recovery`: **1 case**

Total: **3 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Quorum holds (kill 1 follower) | ISR shrinks to 2/3; leader keeps serving acks=All; post-kill produce+subscribe | Leader unchanged; `assert_single_leader`; `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once` |
| Quorum lost (kill 2/3) | Kill leader + 1 follower; lone survivor MUST NOT serve | Produce to survivor must FAIL; no two leader IDs among running nodes; on restart of both killed nodes: re-election, `assert_single_leader`, `assert_no_committed_loss`, post-recovery round-trip |

---

### DR-5 — Network partition (netem 100% loss)

**Fixture:** `fresh_cluster`  
**Markers:** `fault`, `netem`  
**CID window:** `500 000 + n * 3000`

**Cases:**
- `test_dr5_leader_isolation`: `rep × {0, 1}` = **2 cases**
- `test_dr5_follower_isolation`: `rep × {0, 1}` = **2 cases**

Total: **4 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Leader isolation | `partition(leader)` → majority re-elects; produce to isolated leader MUST FAIL; heal; ex-leader rejoins as follower | No split-brain; `assert_single_leader` on majority side during partition; full `assert_single_leader` after heal; `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once`; post-heal round-trip |
| Follower isolation | `partition(follower)` → leader + third keep serving acks=All; heal; follower catches up | Leader unchanged; mid-partition acks=All produce succeeds; after heal: no committed loss, full ordering and exactly-once |

Uses `_assert_single_leader_among(majority_svcs)` during partition to avoid false split-brain alerts from isolated node's stale log.

---

### DR-6 — Packet loss and delay (netem)

**Fixture:** `fresh_cluster`  
**Markers:** `fault`, `netem`  
**CID window:** `600 000 + n * 3000`

**Cases:**
- `test_dr6_follower_lossy`: `loss_pct × {5,10,20}` × `delay_ms × {0,20}` × `rep × {0,1}` = **12 cases**
- `test_dr6_leader_lossy`: `loss_pct × {5,10}` × `delay_ms × {20,50}` × `rep × {0,1}` = **8 cases**

Total: **20 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Follower lossy | netem loss on one follower during sustained produce/subscribe to leader | **Hard:** committed-loss=0; `assert_per_key_ordering`; `assert_exactly_once`. **Election-stability:** at loss ≤ 10% leader MUST NOT change; at 20% a re-election is tolerated and documented. After heal: `assert_single_leader` |
| Leader lossy | netem loss + delay on leader's eth0 during sustained produce/subscribe | **Hard:** committed-loss=0; `assert_per_key_ordering`; `assert_exactly_once`. At loss=5% + delay=20ms leader MUST NOT change; higher intensity re-election tolerated. Client may bail during re-election — only acked set checked |

Aeron's reliability layer (retransmit / NAK) absorbs loss; the broker's push-on-append + fetch backstop keeps replication consistent.

---

### DR-7 — Node crash and restart / durability recovery

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `700 000 + n * 3000`

**Cases:**
- `test_dr7_follower_crash_restart_catchup`: `follower_idx × {0,1}` × `rep × {0,1}` = **4 cases**
- `test_dr7_leader_crash_restart_rejoins_follower`: `rep × {0,1}` = **2 cases**
- `test_dr7_sigterm_vs_sigkill_parity`: `sig × {SIGTERM, SIGKILL}` = **2 cases**
- `test_dr7_full_cluster_cold_restart`: **1 case**

Total: **9 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Follower crash+restart+catch-up | SIGKILL follower; produce while down (quorum via leader + other follower); restart; catch-up | All 3 nodes up after restart; `assert_no_committed_loss` over pre+during sets; `assert_per_key_ordering`; `assert_exactly_once`; ISR healed (fresh acks=All produce+subscribe succeeds) |
| Leader crash+restart+rejoin-as-follower | SIGKILL leader; new leader elected; produce to new leader; restart old; old rejoins as follower | `assert_single_leader` with new leader; old leader not re-elected; log convergence verified via subscribe from old leader |
| SIGTERM vs SIGKILL parity | Both graceful and ungraceful stops must leave committed set intact | committed-loss=0 in both cases (durability is independent of shutdown type) |
| Full cold restart | SIGKILL all 3; restart all 3; elect; assert acks=All set survives | `assert_single_leader`; `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once`; post-restart round-trip |

---

### DR-8 — Combined chaos and soak

**Fixture:** `fresh_cluster`  
**Markers:** `fault`, `netem` (S1, S3), `fault` (S2)  
**CID window:** `1 000 000 + n * 3000`

**Cases:**
- `test_dr8_leader_kill_netem`: **1 case** — `@pytest.mark.xfail` (DR-BUG-6, see below)
- `test_dr8_rolling_restart_churn`: `restart_rounds × {2,3}` = **2 cases**
- `test_dr8_chaos_soak`: **1 case** — `@pytest.mark.netem`, `@pytest.mark.soak`

Total: **4 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| S1: Leader-kill + follower packet loss (combined) | Apply netem 10% loss on one follower; start rate-limited producers; SIGKILL leader mid-produce | **xfail (DR-BUG-6).** Durability assertion: committed-loss=0 on acked set. Re-election + single-leader on majority side. Post-recovery round-trip. |
| S2: Rolling restarts + client churn | 3 concurrent acks=All producers; kill/restart leader N times (quorum preserved); 30-record check to new leader each round | `assert_no_committed_loss` across all round committed sets; `assert_per_key_ordering`; `assert_exactly_once`; `assert_single_leader`; ISR healed after all rounds |
| S3: Bounded chaos-monkey soak | 12 rounds (seed=42) of random faults: {kill-follower+restart, netem-loss+clear, pause+unpause-follower, kill-leader+restart}; background producer | One fault active at a time; quorum preserved throughout; post-soak: `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once`; `assert_single_leader`; final round-trip |

---

### DR-10 — Zombie / paused-leader split-brain

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `800 000 + n * 3000`

**Cases:**
- `test_dr10_pause_idle_leader_zombie_fencing`: `n_clients × {1,3}` = **2 cases**
- `test_dr10_pause_during_produce_zombie_fencing`: `n_clients × {1,3}` = **2 cases**
- `test_dr10_short_vs_long_pause`: `pause_case × {short, long}` = **2 cases**
- `test_dr10_repeated_flap_zombie`: `flap_rounds × {3,5}` = **2 cases** — `@pytest.mark.xfail` (harness determinism on macOS DNS)

Total: **8 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Pause idle leader (zombie fencing) | `docker pause` (SIGSTOP cgroup-freeze) the leader; majority elects new leader; zombie MUST NOT serve writes; unpause; zombie steps down | `assert_zombie_cannot_serve_write`; `assert_no_concurrent_leaders`; post-unpause: `assert_single_leader`; no committed loss; post-recovery round-trip |
| Pause during produce | `docker pause` leader while rate-limited producers are running | Same fencing assertions; in-flight produces to zombie fail (acceptable); every ACKED record survives |
| Short vs long pause | SHORT (~1.5 s, may or may not trigger election) vs LONG (20 s, guaranteed election) | Long: zombie fenced; no two write-accepting leaders. Short: adaptive — if election occurred assert zombie fenced; if not assert same leader resumed cleanly |
| Repeated flap (xfail) | Pause → unpause the leader 3-5 times in succession | Each round: zombie fenced; new leader serves; cumulative committed set intact. xfail due to Docker Desktop DNS transient after rapid pause/unpause cycles |

Election timing: `electionTimeoutMs=150ms`, heartbeat cadence 20ms (per NodeConfig defaults).

---

### DR-11 — Asymmetric / partial / flapping partitions

**Fixture:** `fresh_cluster`  
**Markers:** `fault`, `netem`  
**CID window:** `900 000 + n * 3000`

**Cases:**
- `test_dr11_flapping_leader`: **1 case** — 4 partition/heal cycles, netem 100% loss
- `test_dr11_flapping_follower`: **1 case** — 4 ISR-shrink/re-expand cycles
- `test_dr11_rapid_flap_livelock_guard`: **1 case** — 5 fast cycles (0.8 s partition), livelock detection
- `test_dr11_asymmetric_one_way_partition_placeholder`: **1 case** — `@pytest.mark.skip` (Linux-CI only)
- `test_dr11_partial_quorum_partition_placeholder`: **1 case** — `@pytest.mark.skip` (Linux-CI only)

Total: **5 cases** (3 active, 2 skipped)

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Flapping leader | Isolate/heal the current leader 4 times (1.5 s windows); inter-flap produce to whoever wins each election | Isolated leader CANNOT serve (no split-brain); `wait_for_leader` converges after flaps (no livelock); `assert_single_leader`; stability re-check after 5 s; `assert_no_committed_loss` |
| Flapping follower | Isolate/heal the same follower 4 times; ISR shrinks then re-expands; produce during each isolation window | Leader UNCHANGED (2/3 majority never lost); `assert_single_leader`; `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once` |
| Rapid flap / livelock guard | 5 fast partition/heal cycles (0.8 s); isolated node CANNOT serve; key assertion: cluster converges to 1 stable leader within 30 s | `wait_for_leader` with 30 s timeout (TimeoutError = livelock = blocker); stability confirm; `assert_no_committed_loss` |
| Asymmetric one-way (skip) | Per-peer `iptables` DROP — unreliable on Docker Desktop LinuxKit nf modules | Enable on Linux CI |
| Partial quorum (skip) | Leader reachable to one follower only — needs per-peer netem not available on Docker Desktop | Enable on Linux CI with per-peer traffic shaping |

---

### DR-12 — Packet reorder, duplication, and MTU / large-batch fragmentation

**Fixture:** `fresh_cluster`  
**Markers:** `fault`, `netem`  
**CID window:** `1 000 000 + n * 3000`

**Cases:**
- S1 `test_dr12_follower_reorder`: `reorder_pct × {25,50}` × `rep × {0,1}` = **4 cases**
- S2a `test_dr12_follower_dup`: `dup_pct × {5,10}` × `rep × {0,1}` = **4 cases**
- S2b `test_dr12_leader_dup`: `dup_pct × {5,10}` × `rep × {0,1}` = **4 cases**
- S3 `test_dr12_follower_reorder_and_dup`: `param × {light,heavy}` × `rep × {0,1}` = **4 cases**
- S4 `test_dr12_large_batch_fragmentation`: `rep × {0,1}` = **2 cases**
- S5 `test_dr12_mtu_reduction_large_batch_placeholder`: **1 case** — `@pytest.mark.skip` (Linux-CI only)

Total: **19 cases** (18 active, 1 skipped)

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Follower reorder (S1) | netem reorder 25/50% + 20 ms delay on one follower | **Hard:** committed-loss=0; per-key ordering (Aeron restores wire reorder); exactly-once. **Election-stability:** at reorder ≤ 25% leader MUST NOT change |
| Follower dup (S2a) | netem duplicate 5/10% on one follower | No app-level dup delivery (Aeron dedup); committed-loss=0; leader MUST NOT change |
| Leader dup (S2b) | netem duplicate 5/10% on leader's eth0 | Same hard invariants; at dup ≤ 5% leader stable; at 10% re-election tolerated |
| Combined reorder+dup (S3) | light: reorder=20%, dup=5%, delay=15ms; heavy: reorder=40%, dup=10%, delay=20ms | Hard invariants hold for both; leader stable at light intensity |
| Large batch fragmentation (S4) | 500 records × 18 keys + reorder=30%, dup=8%, delay=20ms on follower | Exercises Aeron FragmentAssembler; committed-loss=0; per-key ordering; exactly-once. NOTE: `dr_client.cpp` payload is fixed ~25 bytes/record — a `RUNE_RECORD_BYTES` knob is needed for true multi-fragment stress (controller TODO) |
| MTU reduction (S5, skip) | `ip link set dev eth0 mtu 1200` via netem sidecar; two-level fragmentation | Enable on Linux CI where container-netns MTU changes are reliable. `faults.set_mtu()` is implemented in `harness/faults.py` |

---

### DR-13 — Client faults (crash, reconnect storm, slow subscriber, scale)

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `1 200 000 + n * 5000`

**Cases:**
- `test_dr13_scale`: `n_clients × {50}` = **1 case**
- `test_dr13_reconnect_storm`: `n_storm_clients × {15}` = **1 case**
- `test_dr13_slow_subscriber_isolation`: **1 case**
- `test_dr13_slow_subscriber_isolation_concurrent`: **1 case** — `@pytest.mark.skip` (harness extension needed)
- `test_dr13_client_crash_mid_produce`: **1 case**

Total: **5 cases** (4 active, 1 skipped)

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Scale (50 concurrent clients) | 50 `produce_subscribe` containers; 1.2 s per-client stagger; 40 records × 18 keys each | All 50 exit cleanly (exit=0, rc=0); total produced = 50 × 40; `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once`; `assert_single_leader` |
| Reconnect storm (15 clients) | 15 rate-limited producers (acks=All); SIGKILL leader mid-run; clients reconnect via redirect-follow | New leader elected; `assert_no_committed_loss` on acked set; `assert_per_key_ordering`; `assert_exactly_once`; post-recovery round-trip; clients that bail on failover are acceptable |
| Slow subscriber isolation | 2-phase: phase 1 = producer + fast subscriber (no slow sub running); phase 2 = slow subscriber (80 ms/record, CB-4 back-pressure) | Phase 1 completes < 120 s (broker not stalled by slow sub); both phases: committed-loss=0; `assert_exactly_once`; `assert_per_key_ordering` |
| Slow sub concurrent (skip) | All three (producer + fast sub + slow sub) simultaneously — requires per-client timing API not yet in `run_clients` | Enable once harness exposes per-client completion timestamps |
| Client crash mid-produce | Rate-limited producer (acks=All); kill its container via `docker kill` mid-run; weaker invariant | Cluster remains healthy (3 nodes, single leader); post-crash produce+subscribe round-trip succeeds; exactly-once on post-crash batch |

---

### DR-14 — Storage / disk / shared-memory faults

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `1 300 000 + n * 3000`

**Cases:**
- `test_dr14_torn_log_tail_recovery`: `target_role × {follower, leader}` = **2 cases**
- `test_dr14_mid_log_corruption_detection`: **1 case**
- `test_dr14_shm_size_sufficient`: **1 case**
- `test_dr14_shm_exhaustion_injection`: **1 case** — `@pytest.mark.skip`
- `test_dr14_fsync_failure_graceful_degradation`: **1 case** — `@pytest.mark.skip`
- `test_dr14_disk_full_graceful_degradation`: **1 case** — `@pytest.mark.skip`

Total: **7 cases** (4 active, 3 skipped)

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Torn log tail — follower | SIGKILL follower; truncate last 128 bytes of its named Docker volume via `faults.truncate_log_tail()`; restart | Acceptable outcomes: (a) recovers — re-fetches torn tail from leader; or (b) exits non-zero (clean refusal). In either case: `assert_no_committed_loss`; `assert_per_key_ordering`; `assert_exactly_once` from surviving nodes. Recovered node is also subscribed to verify no corrupt records served. |
| Torn log tail — leader | SIGKILL leader; new leader elected + produces more; corrupt old leader's tail; restart old as follower | Same accept criteria as follower case; additional check that the new leader's intact copy provides the authoritative committed set |
| Mid-log corruption | SIGKILL follower; overwrite 16 bytes at offset 64 (skipping header) of first log segment; restart | Acceptable: truncate-from-corruption + re-fetch OR exit non-zero. NEVER: node starts and serves a corrupt record. Surviving cluster: `assert_no_committed_loss` from leader |
| shm size sufficient | Existence check: `fresh_cluster.up()` succeeded + leader elected → shm=256 MiB is enough for Aeron CnC | Produce+subscribe round-trip confirms data plane operational |
| shm exhaustion (skip) | Fill /dev/shm to force Aeron CnC alloc failure | Enable on Linux CI with `shm_size: 1m` compose override |
| fsync failure (skip) | Inject EIO on fsync via FUSE/dm-error | Enable on Linux CI with FUSE error-injector; expected: no false ack for non-fsynced record |
| Disk full (skip) | Fill RUNE_DATA_DIR to 100% via capped tmpfs | Enable on Linux CI; expected: ENOSPC surfaced as ProduceResponse error, not silent record loss |

Volume access for mutation uses `faults._exec_on_volume()` (mounts the named Docker volume into a temporary alpine container via `docker run --rm -v <vol>:/data`).

---

### DR-15 — Failover data-integrity deep-dive

**Fixture:** `fresh_cluster`  
**Markers:** `fault`  
**CID window:** `1 400 000 + n * 3000`

**Cases:**
- `test_dr15_tail_loss_quantification`: `acks × {leader, all}` = **2 cases**
- `test_dr15_no_phantom_read`: **1 case**
- `test_dr15_log_divergence_guard`: **1 case**
- `test_dr15_sigterm_vs_sigkill_acks_all_parity`: `sig × {SIGTERM, SIGKILL}` = **2 cases**
- `test_dr15_all_nodes_crash_cold_restart_consistent`: **1 case**

Total: **7 cases**

| Scenario | Description | Key assertions |
|----------|-------------|----------------|
| Tail-loss quantification (acks=Leader vs acks=All) | Produce at rate_hz=40 with the given acks mode; SIGKILL leader at t=1.5 s; subscribe from new leader; count missing records | acks=All: `n_lost == 0` (GR-9). acks=Leader: `n_lost <= 50` (12× safety margin over ~4 records at 10 ms group-commit × 40 Hz); ordering + exactly-once on surviving records |
| No phantom read | Background subscriber runs from EARLIEST before kill; acks=Leader produce races the kill; post-failover subscriber on new leader | `bg_delivered_ids ⊆ authoritative_ids` (where authoritative = new leader's post-failover log); `n_phantom == 0` (GR-6 HW-bound confirmed empirically); acks=All pre-kill batch: `assert_no_committed_loss` |
| Log divergence guard | Commit acks=All; SIGKILL old leader; produce more to new leader; restart old as follower; subscribe from both | `missing_on_old == 0` (old leader did not truncate too much); `phantom_on_old == 0` (old leader has no stale entries); full convergence by (cid, key, seq) |
| SIGTERM vs SIGKILL acks=All parity | 150-record acks=All batch; SIGTERM or SIGKILL the leader; subscribe from new leader | committed-loss=0 in both cases; `assert_per_key_ordering`; `assert_exactly_once` |
| All-3-node cold restart consistent | Commit acks=All; SIGKILL all 3; restart all 3; elect; subscribe from all nodes | `assert_no_committed_loss` from new leader; per-follower convergence check (`missing_on_follower == 0`, `phantom_on_follower == 0`); post-restart round-trip |

---

## 3. Invariant Set

All invariant checkers live in `harness/invariants.py`. Records are identified by the triple `(cid, key, seq)` encoded in the value payload by `dr_client.cpp`.

### `assert_no_committed_loss(results: Results)`

> Every acked (committed) record must be delivered to at least one subscriber.

- `produced_ids = {(cid, key, seq) for m in results.all_produced if m.cid >= 0}`
- `delivered_ids = {(cid, key, seq) for m in results.all_delivered if m.cid >= 0}`
- `missing = produced_ids - delivered_ids`
- Fails with the first 20 missing `(cid, key, seq)` tuples if `missing` is non-empty.

This is the **primary durability invariant**. For `acks=All` it is iron-clad (GR-9): every produced record was majority-flushed before the ack. For `acks=Leader` it is a **bounded loss** check (DR-15 quantifies the ceiling).

### `assert_per_key_ordering(results: Results)`

> For each `(subscribing client, source key)` pair, delivered records arrive in strictly increasing `seq` order.

- Scoped to the current run's records only (filtered by `run_rids` membership, which uses per-run disjoint CID windows to exclude records from earlier cases on the shared (module-scoped) cluster).
- Checks each subscribing client's own delivered stream independently.
- Fails immediately on the first out-of-order `seq` with context.

### `assert_exactly_once(results: Results)`

> No committed record is delivered more than once to the same subscriber.

- Scoped to current-run records only (same `run_rids` filter).
- Per-subscriber: a record arriving at multiple subscribers is correct (shared log); a record arriving twice at the same subscriber is a violation.
- Fails listing up to 20 duplicate `(cid, key, seq)` records.

### `assert_single_leader(cluster, timeout_s=30) -> int`

> At any given Raft term, exactly one node claims leadership.

Term-aware algorithm:
1. Parse all running nodes' logs for `"node <id> is LEADER term <T>"` (or legacy `"node <id> is LEADER"` → term −1).
2. Find `T_max` = highest term observed across all running nodes.
3. Among announcements at `T_max`, assert exactly one distinct `node_id`.

Returns the agreed leader id. This avoids false split-brain alarms from stale log entries of previous leaders after a partition heal — a critical correctness property proven by DR-5's isolation + heal cycle.

### `assert_resume_from_offset(before, after, resume_offset)`

> A subscriber that resumed from `offset(resume_offset)` must not receive any record below that offset.

### `assert_durable_after_restart(before, after)`

> Every record committed before a restart is still delivered after it.

Both `assert_resume_from_offset` and `assert_durable_after_restart` are available in the harness for future use; the current DR-1..15 suite uses them implicitly through `assert_no_committed_loss` + subscribe-from-earliest patterns.

---

## 4. Durability Contract

### acks=Leader

- The leader writes batches to its page-cache and issues a `ProduceResponse` after a **group-commit** window (~10 ms).
- The batch is pushed to followers asynchronously via push-on-append replication.
- **On a hard leader crash (SIGKILL):** a tail of up to ~10 ms of acked records may not yet have been replicated to any follower. The new leader will truncate these un-replicated records during log reconciliation. This is **bounded tail-loss**, not a bug — it is the stated trade-off of `acks=Leader`.
- DR-15 scenario 1 (`test_dr15_tail_loss_quantification`) quantifies this: the loss ceiling is **≤ 50 records** at 40 Hz (a 12× safety margin over the theoretical ~4 records at 10 ms × 40 Hz).

### acks=All

- The leader waits for a majority of its ISR (In-Sync Replicas) to acknowledge the batch before sending a `ProduceResponse`.
- Implemented via **push-on-append + HW (High-Water Mark)**. On each batch append, the leader immediately pushes the batch to all ISR followers. Once a majority has acknowledged, the leader advances the HW and sends the ack.
- **On any leader crash:** every acked record was majority-flushed before the ack. The new leader is guaranteed to have the record. `assert_no_committed_loss` must pass with zero missing records.
- **On quorum loss (ISR < `minIsr`):** the leader refuses to accept new writes (acks=All produce must fail). The `minIsr` gate is set to 2 for the 3-node cluster. DR-4 `test_dr4_quorum_lost_safety_and_recovery` verifies this.
- **DR-BUG-3** (now fixed) found a case where `acks=All` was acked with ISR shrunk below `minIsr`. The fix adds a drain-time min-ISR gate + HW clamp.

### The central contract

| Mode | Ack when | Loses on hard leader crash? | min-ISR gated? |
|------|----------|-----------------------------|----------------|
| `acks=Leader` | After group-commit on leader page-cache | **Yes** — bounded tail (≤ ~10 ms × produce rate) | No |
| `acks=All` | After majority ISR flush + HW advance | **No** — zero committed loss guaranteed | **Yes** (minIsr=2) |

All fault tests in DR-3..15 use `acks=all` by default (GR-9 / Milestone C). DR-15 is the dedicated `acks=Leader` characterisation.

---

## 5. Docker Desktop Caveats and Linux-CI-Only Skips

### What works on Docker Desktop (macOS LinuxKit VM)

- `docker pause` / `docker unpause` (cgroup freeze / SIGCONT) — proven reliable.
- `tc qdisc` netem via the `rune-netem:dev` privileged sidecar (`--net=container:<cid> --cap-add=NET_ADMIN`) — proven: 100% loss qdisc turns `ping` from 0% to 100% packet loss and heals cleanly.
- netem `loss`, `delay`, `reorder`, `duplicate` — all supported via the sidecar.
- Full isolation partition via netem 100% loss — this is `faults.partition()`.

### What is unreliable on Docker Desktop

| Feature | Root cause | Workaround |
|---------|-----------|------------|
| `iptables -j DROP` in `--net=container:` sidecar | LinuxKit kernel may lack required nf modules (`nf_tables`, `xt_*`) | Use netem 100% loss for full isolation (portable) |
| Per-peer directional / asymmetric partition | Needs per-peer `iptables` DROP rules keyed on container IPs | Skip; enable on Linux CI |
| Per-peer partial-quorum partition | Needs per-peer netem `matchall` or iptables — not achievable with eth0-scoped netem | Skip; enable on Linux CI |
| MTU reduction via `ip link set dev eth0 mtu <N>` | macOS LinuxKit VM's veth pair MTU may be controlled by the hypervisor layer; `ip link set` may succeed silently with no effect, or race with Aeron media driver in-flight sends | Skip; enable on Linux CI |
| fsync failure injection (FUSE/dm-error) | Not available on Docker Desktop / macOS Rosetta | Skip; enable on Linux CI with a FUSE error-injector |
| Disk-full injection (capped tmpfs) | Docker Desktop's virtual disk cannot be reliably capped per-container | Skip; enable on Linux CI with `--mount type=tmpfs,target=/var/lib/rune,tmpfs-size=16777216` |
| shm exhaustion (constrained shm mount) | `shm_size` constraints unreliable on Docker Desktop / macOS Rosetta | Skip; enable on Linux CI |

### Specific xfail cases and their reasons

**`test_dr8_leader_kill_netem` — `@pytest.mark.xfail(strict=False)`**

Blocked on **DR-BUG-6** (leader completeness gap). The combined fault of leader-kill + packet-loss can elect a node that is lagging in partition data. The KIP-101 epoch-lineage fix (preventing log divergence) is implemented and unit-clean, but does not yet engage in the production E2E consensus/bridge path. An `acks=All`-committed record can therefore be lost across this specific failover. Leader-kill alone (DR-3) and loss alone (DR-6) both pass with zero committed loss. See DR-BUG-6 in the bug catalogue below.

**`test_dr10_repeated_flap_zombie` — `@pytest.mark.xfail(strict=False)`**

Blocked on **harness determinism**, not a broker bug. Under sub-second repeated pause/unpause of the leader, Docker Desktop's embedded DNS proxy transiently fails to resolve container names. The `_identify_leader_with_container()` helper then raises `RuntimeError` (leader container id is None), not a split-brain or committed-loss assertion. The critical zombie-leader split-brain safety is proven by the 6 deterministic DR-10 cases (idle, during-produce, short+long pause). Enable on Linux CI where DNS + pause/unpause timing is deterministic.

---

## 6. Bug Catalogue

The following bugs were surfaced by the DR-E2E suite during development and are now fixed or explicitly deferred.

| ID | Component | Description | Status |
|----|-----------|-------------|--------|
| **DR-BUG-1** | Client (`ProducerHub`) | Client startup SIGSEGV race — `ProducerHub` data race on concurrent initialization caused SIGSEGV when multiple clients started simultaneously. Manifested as `exit_code=139` in DR-1 follower-targeted cases. | **FIXED** — wait-free snapshot. Commit: `e56a527`. |
| **DR-BUG-2** | Broker (follower `ClientGateway`) | Connect-under-load stall — the follower's `ClientGateway` handled redirects via a **blocking** `resolvePublication()` call (up to 5 s of 1 ms sleeps) on its single duty-cycle thread. Under N concurrent clients all redirecting to the leader, each redirect was dispatched serially; one slow egress warm-up head-of-line-blocked the entire follower gateway and delayed the last client's leader connect past its deadline. | **FIXED** — redirect now goes through the same async, fair, non-blocking pending-connect machinery as the leader-accept path (`pollPendingConnects`). Commit: `77dea04`. Client-side fast-retry on `NOT_CONNECTED` also fixed. Commit: `8906828`. |
| **DR-BUG-3** | Broker (ISR / HW gate) | `acks=All` acked with ISR shrunk below `minIsr` (minority commit) — the leader sent a `ProduceResponse` OK even when only 1 of 3 nodes was in the ISR, violating the `minIsr=2` gate and the majority-durable contract. | **FIXED** — drain-time min-ISR gate + HW clamp. Verified by DR-4 `test_dr4_quorum_lost_safety_and_recovery`. |
| **DR-BUG-4** | Broker (`UdpMessageSink`) | Restarted nodes crash on startup — `UdpMessageSink` performed a synchronous `resolve` of its peer's address on the duty-cycle thread during co-restart. Under concurrent node restarts the resolve could throw (peer not yet up), crashing the restarting node. | **FIXED** — async resolve, matching the pending-connect machinery pattern used for the follower redirect fix. |
| **DR-BUG-5** | Broker (replication routing) | Replication routing keyed by `partitionId` only (multi-topic collision) — when multiple topics were active (DR-2 `partitions=3` cases added the "multi" topic alongside "events"), the replication router dispatched based on `partitionId` alone. Partitions from different topics that share the same `partitionId` were misrouted. | **FIXED** — `topicId` added to wire format and routing key. Includes transport send-site `topicId` stamp and client `ProduceResponse` `errorCode` handling. |
| **DR-BUG-6** | Broker (leader completeness / Raft log reconciliation) | Leader-completeness gap — combined leader-kill + packet-loss can elect a partition-data-lagging node whose committed tail has not been reconciled before it begins serving. An `acks=All`-committed record can then be absent from the new leader's log (lost). A KIP-101 epoch-lineage fix is implemented (no divergence, unit-clean) but does **not** yet engage in the production E2E consensus/bridge path. | **OPEN — Blocker / Deferred.** This is the one open durability gap in the suite. `test_dr8_leader_kill_netem` is `@pytest.mark.xfail` specifically for this reason. Leader-kill alone (DR-3) and loss alone (DR-6) both pass with zero committed loss; only the combination triggers this gap. Requires a focused fix before this xfail can be removed. |

---

## 7. Status Summary Table

### DR tickets

| Ticket | Test(s) | Cases | Status | Notes |
|--------|---------|-------|--------|-------|
| **DR-1** | `test_dr1_baseline` | 16 | **PASS** | Baseline parametric matrix; session cluster |
| **DR-2** | `test_dr2_multiclient` (quick), `test_dr2_multiclient_soak` (soak) | 48 quick + 4 soak | **PASS** | Quick: baseline; soak: requires `-m soak` |
| **DR-3** | `test_dr3_leader_kill`, `test_dr3_rolling_leader_kill` | 10 | **PASS** | acks=All; idle + mid_produce + rolling |
| **DR-4** | `test_dr4_quorum_holds_kill_follower`, `test_dr4_quorum_lost_safety_and_recovery` | 3 | **PASS** | Safety + recovery under quorum loss |
| **DR-5** | `test_dr5_leader_isolation`, `test_dr5_follower_isolation` | 4 | **PASS** | netem 100% loss partition |
| **DR-6** | `test_dr6_follower_lossy`, `test_dr6_leader_lossy` | 20 | **PASS** | Loss/delay; committed-loss=0; election-stability per intensity |
| **DR-7** | `test_dr7_follower_crash_restart_catchup`, `test_dr7_leader_crash_restart_rejoins_follower`, `test_dr7_sigterm_vs_sigkill_parity`, `test_dr7_full_cluster_cold_restart` | 9 | **PASS** | Crash+restart; SIGTERM vs SIGKILL parity; cold restart |
| **DR-8** | `test_dr8_leader_kill_netem` (xfail), `test_dr8_rolling_restart_churn`, `test_dr8_chaos_soak` | 4 | **PARTIAL** — 1 xfail (DR-BUG-6), 2 pass, 1 soak pass | Chaos capstone; S1 blocked on DR-BUG-6 |
| **DR-10** | `test_dr10_pause_idle_leader_zombie_fencing`, `test_dr10_pause_during_produce_zombie_fencing`, `test_dr10_short_vs_long_pause`, `test_dr10_repeated_flap_zombie` (xfail) | 8 | **PARTIAL** — 6 pass, 2 xfail (harness DNS) | Zombie-leader split-brain fencing |
| **DR-11** | `test_dr11_flapping_leader`, `test_dr11_flapping_follower`, `test_dr11_rapid_flap_livelock_guard`, 2 × skip | 5 | **PARTIAL** — 3 pass, 2 skip (Linux-CI) | Asymmetric/partial partitions need iptables |
| **DR-12** | 5 × scenarios, 1 × skip | 19 | **PARTIAL** — 18 pass, 1 skip (Linux-CI MTU) | Reorder/dup/fragmentation; MTU needs Linux CI |
| **DR-13** | `test_dr13_scale`, `test_dr13_reconnect_storm`, `test_dr13_slow_subscriber_isolation`, concurrent (skip), `test_dr13_client_crash_mid_produce` | 5 | **PARTIAL** — 4 pass, 1 skip (harness API) | Client fault resilience |
| **DR-14** | 4 active + 3 skip | 7 | **PARTIAL** — 4 pass, 3 skip (Linux-CI fsync/disk/shm) | Storage corruption, shm, disk-full |
| **DR-15** | `test_dr15_tail_loss_quantification`, `test_dr15_no_phantom_read`, `test_dr15_log_divergence_guard`, `test_dr15_sigterm_vs_sigkill_acks_all_parity`, `test_dr15_all_nodes_crash_cold_restart_consistent` | 7 | **PASS** | Integrity deep-dive; acks=Leader tail-loss quantified |

### Bug tracker

| ID | Status |
|----|--------|
| DR-BUG-1 | **FIXED** (ProducerHub data race → wait-free snapshot) |
| DR-BUG-2 | **FIXED** (follower gateway blocking redirect → async pending-connect) |
| DR-BUG-3 | **FIXED** (acks=All minority commit → min-ISR gate + HW clamp) |
| DR-BUG-4 | **FIXED** (restarted node UdpMessageSink sync resolve → async) |
| DR-BUG-5 | **FIXED** (replication routing partitionId-only → topicId+partitionId key) |
| DR-BUG-6 | **OPEN — Blocker** (leader-completeness: combined kill+loss elects lagging node; KIP-101 epoch fix implemented but not yet on E2E path; `test_dr8_leader_kill_netem` xfail) |
