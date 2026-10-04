# Rune 3-node throughput & latency — findings, improvements, current config (2026-10-02)

Living record of every performance improvement made in the 2026-10-01/02 push toward the
targets, with the measurement that justified each, the commit, and the current best config.
Update this document whenever a change lands or a measurement changes.

## 1. Targets

- **Throughput:** ≥ 159k rec/s — 3 nodes, acks=all, 1 KiB values, on the 4-core k3s-node-01
  (Kafka on the same box: 156k).
- **Latency:** client send → ack **p99 < 100 µs**, measured open-loop at the target rate.
- **Client:** batches heavily, low CPU (no spinning thread) — it runs inside other services.

## 2. Box and method

- k3s-node-01 (UAT, k3s stopped): 4 vCPU, 16 GB, ext4 on `/dev/sda1`, `/dev/shm` 3.8 GB.
  3 brokers + client co-located on loopback. Shard mode pins one busy-spin thread per node to
  cores 1–3; core 0 runs the client and every other thread.
- Every run preceded by `scripts/vm-clean.sh`. Throughput runs repeated (±10 % run-to-run).
- **rune-fr (TSC, ~10 ns/event)** is the primary instrument. Uprobes were tried and rejected:
  ~1000 passes/s instead of ~250k/s with probes on — they perturb the thing measured.
- Analyzers in `bench/kafka-compare/`: `frshard.py` (cross-node per-batch waterfall),
  `frstage.py` (per-pass stage costs), `aeronhops.py` (Aeron-internal spans + cross-node send→insert).

### 2.1 Instrumentation added (diag branch `diag/fr-shard-waterfall`, wt20; rune-fr `diag/fr-shard-waterfall`)

- 25 `Sh*` waterfall sites keyed `(topic,part,offset)`: append, push attempt/sent/refused,
  fetch recv, serve decision, answer sent/refused, HW advance, waiter park/fire, ack claimed,
  follower apply entry/done, fast-ack sent/refused, fetch sent/refused, plane poll fragments
  (aggregated), pub refusals, pass idle/driver work (aggregated).
- Per-pass stage TSC sums (driver, conductor, worker sub-stages, fused runtime sub-stages).
- Storage write split: `StoragePwrite` (pwritev only), `StorageRoll`, `StorageIndex`.
- **Cross-process alignment fix:** traces now carry `rune_fr_anchor` (absolute TSC + calibration);
  analyzers align on anchor *differences* (converting each process's absolute TSC with its own
  calibration drifted ~11 s on a box with months of uptime).
- Exact per-worker pairing for the runtime→worker ring (`ReplFragRecv` value = owning worker,
  per-worker sequence) — the old global-vs-per-worker sequence mismatch produced bogus 8–40 s numbers.
- **rune-fr inside the Aeron C driver:** C API (`rune_fr_c_now/span/mark`, in `Registry.cpp`
  so it is always linked) + 24 Aeron sites; Aeron 1.41.4 driver sources patched with weak hooks
  wrapping 20 functions (duty cycles, data send/insert keyed by stream/term-offset, send/recv
  syscalls, SM/NAK/loss/retransmit, term cleaning). Patch script: scratchpad `aeron_fr_patch.py`.
  On the VM the patched `libaeron_driver_static.a` replaces the package lib — **original backed up
  at `/root/rune-e5/aeron-backup/`** (restore before any non-diag use).
  Known gap: send→insert pairing keyed by `(stream&0xFF, termOffset)` collides across
  publications (all start at offset 0) → next build keys on `(session_id, termOffset)`.

## 3. Improvements — chronological, with evidence

Throughput numbers: 3-node acks=all 1 KiB, window 8000, 300k records unless noted.

| # | Change | Commit / branch | Evidence before → after |
|---|---|---|---|
| F25 | Shard-mode metadata advertised `ingressStreamId = workerIngressBase+w`, which only existed on the shard endpoint → endpoint-unaware clients `NOT_CONNECTED` forever (bench hang). Advertise `clientStreamId_`; shard ingress subscribes that stream on its own port. | `e302e5f` (feat/t13-worker-acks) | hang → 20000/20000 acked |
| F27 | Replication sends refused on back-pressure were dropped while the push cursor advanced; recovery only by resend timer → acks=all timeouts → idempotent cascade (200000/200000 failed). Sends report success; per-follower cursors advance only on accepted sends; failed answers re-served next pass. | `28044dc`, `ba9b7d5` (fix/f27-replication-send-never-dropped); round 2 (R1 dead-peer stall) in flight | 0 succeeded → all succeeded once window/driver fixes landed |
| T13 | Worker-side acks (session publications, announce-before-accept, per-session pause, SessionFailed, no timer drops). | `410c11d…2872f97` (complete) | removes the runtime ack relay |
| F30 | Optimization #5 (fsync off the append thread) was only active in shard mode: thread placement fsynced inline every 20 ms per follower + 10 ms owned-log sync, never submitted deferred seals; Strict acks didn't wait for seals. All modes now go through the io_uring backend; truncation low-water mark. | `43bc28c`, `91ce0b8` (fix/f30-follower-flush-off-worker) — approved | inline `StorageFlush` n=274 → 4; inbound p90 3–4 ms → 0.9–2.4 ms |
| A1 | Shard pass re-runs its invoker driver duty while it reports work (≤16). One duty moved ~1 datagram/stream. | `db886a3` (perf/shard-single-driver) | 0 → 200000/200000 at 4.7–12.4k; plane polls 1 → ~8 fragments |
| MTU | 64 KiB MTU (`RUNE_AERON_MTU=65504`) on loopback replication; window 4 MiB with 16 MiB socket buffers (16 MiB window caused 98 % UDP drops: 30M rcvbuf errors in one run). | config | 12.4k → 61.5k (shard) ; netfilter share ~20 % → ~3 % |
| A4 | Fused runtime rides worker 0's driver (one driver per worker; the extra runtime driver cost ~15–17 µs/pass of idle polling). | `02951ef` | runtime 18–22 → 2–2.6 µs/pass; pass 40 → 20 µs; 61k → 73–79k |
| SPARSE | `RUNE_AERON_TERM_SPARSE=false`: pre-allocated term buffers (sparse terms page-faulted ~15 % of the shard thread; also F26 SIGBUS). | `bf8527e` | 73–79k → **100–105k** |
| DEFAULTS | The §4 config is the broker DEFAULT, auto-derived from the host (`HostDefaults.h`): shard + fused, 1 worker; 16 MiB non-sparse terms; 32 MiB rings; uring with auto-fallback to portable; MTU 65504 on loopback / NIC MTU − 28 (IPv6 − 48) aligned 32, clamped ≤ SO_SNDBUF; socket buffer min(16 MiB, rmem/wmem_max) + loud WARN naming the sysctls; window pow2(min(4 MiB, buf/4, term/2)); shard base ports + core layout derived. Explicit env/config always wins (clamped only where `aeron_driver_init` would EINVAL). One startup line logs the effective config. | integ/perf-defaults (wt26) | e2e: see the "defaults = tuned" row below |
| AUTO-MTU BUG | Integrated defaults, broker `7ad0b2a`: a wildcard client bind (cluster.sh-style `0.0.0.0`, peers `127.0.0.1`) derived the NIC MTU **1472** instead of 65504. Fix: the MTU follows where datagrams go (peers + the advertised client endpoint = the peer host); a wildcard bind adds nothing. RED→GREEN `HostDefaultsRegression.WildcardBindWithLoopbackPeersIsLoopbackRegression` + `BrokerNodeRegression.ClusterShStyleWildcardBindDerivesLoopbackMtuRegression`. | fix on integ/perf-defaults (after `7ad0b2a`) | controller, k3s, shard-invoker saturate client `6c43f00`, 300k acks=all: auto MTU 1472 → **14–18k rec/s**; explicit `RUNE_AERON_MTU=65504` → **136,234 rec/s** |
| CLIENT | The old client (pre-`460dd12`, which never set the Produce frame's producerId/epoch/baseSequence) is now REJECTED by design: the broker cross-checks frame identity vs the RecordBatch header (idempotence-window merge). | `1540239` merged | old bench client fails; use client ≥ `460dd12` |
| DEFAULTS = TUNED | 3-node e2e on k3s-node-01: Run 1 = `cluster.sh` with NO tuning env (broker defaults), Run 2 = the explicit §4 env; same topic (`bench:4`, majority/all-in-sync, minIsr 2) and client. Must match (and be ≥ 100k rec/s, 0 failed). | broker: integ/perf-defaults head with the auto-MTU fix; final client | **PENDING — controller re-run with NO env overrides (target ≈ 136k, see §4.1)** |
| ENV | Loopback fast path on k3s-node-01 (`lo` NOTRACK + ACCEPT ahead of ~1055 kube rules / 201 chains). Revert: `/root/rune-e5/iptables-lo-revert.sh`. | box config | small-send syscall p50 30 → 19.5 µs, p99 110 → 67 µs; throughput unchanged (98–103k) |

Measured and **not** the cause (ruled out with data): storage `pwrite` is not I/O-bound
(off-CPU stacks: 0 samples in dirty throttling/locks — slow writes were CPU descheduling under
oversubscription); `fallocate` preallocation works (64 MB allocated per active segment);
`kPushFlowWindow` (16384 records) is not the limiter; netfilter alone is not the per-packet floor.

## 4. Current best config (100–105k rec/s run)

> **Since integ/perf-defaults (2026-10-02) every broker value below is the DEFAULT**,
> derived from the host at start (`libs/node/include/rune/node/HostDefaults.h`); a broker
> started with none of these variables logs `effective config: mode=shard placement=fused
> workers=1 coreBase=… term=16777216 sparse=false mtu=65504 socketBuffer=16777216
> rcvWindow=4194304 rings{…33554432…}` on this box.  `bench/kafka-compare/cluster.sh`
> passes a knob only when the caller sets it.

Broker (env, via `replbench.sh`/`cluster.sh` in shard mode):
```
RUNE_SHARD=1  (RUNE_WORKER_MODE=shard, RUNE_RUNTIME_PLACEMENT=fused, 1 worker/node, pinned busy-spin)
RUNE_AERON_MTU=65504
RUNE_AERON_RCV_WINDOW=4194304
RUNE_AERON_SOCKET_BUFFER_BYTES=16777216
RUNE_AERON_TERM_BUFFER_BYTES=16777216
RUNE_AERON_TERM_SPARSE=false
RUNE_REPL_OUTBOUND_RING_BYTES=33554432
RUNE_WORKER_INGRESS_RING_BYTES=33554432
RUNE_WORKER_RETURN_RING_BYTES=33554432
RUNE_IO_BACKEND=uring
RUNE_MIN_ISR=2   topic bench:4 (4 partitions), tier=majority:durability=all-in-sync
```
Hard constraints: window ≤ socket buffer ≤ sysctl rmem/wmem_max (16 MiB); window ≤ term/2;
with 64 KiB datagrams keep window well below SO_RCVBUF (kernel truesize) or packets drop.
Sysctls: `net.core.{r,w}mem_{max,default}=16777216`, `netdev_max_backlog=100000`.

Client (`rune_produce_throughput`, current main `2a9d99f` + F8 pending merge):
```
RUNE_BENCH_MODE=acked RUNE_BENCH_ACKS=all RUNE_BENCH_VALUE_BYTES=1024
RUNE_BENCH_WINDOW=RUNE_BENCH_MAX_IN_FLIGHT=8000 RUNE_LINGER_US=1000 RUNE_BATCH_BYTES=2097152
RUNE_IDLE_MODE=backoff   (client still sends produce via the runtime gateway — not ingressEndpoint yet)
```

### 4.1 Defaults = tuned — e2e proof (DONE 2026-10-02, broker b4321be, client 6c43f00)

Controller runs on k3s-node-01, broker `integ/perf-defaults@b4321be`, client
`feat/sharded-invoker-client@6c43f00` (T16 direct-to-worker + EOS rounds + T18 bench), clean
Release builds, RUNE_FS_LOG=0, RUNE_LOG_LEVEL=warn, under `/root/vm.lock`, 3-node acks=all 1 KiB,
topic bench:8 (majority, durability=all-in-sync, minIsr 2), harness `/root/e2e/{perf,gate}.sh`.

| run | broker env | client | result |
|---|---|---|---|
| default config (NO RUNE_AERON_* / ring env) | none | `rune_produce_throughput --mode saturate --shards 1 --records 300000 --linger-us 0` | **112,854 rec/s**, errors=0. Effective: `mode=shard placement=fused workers=1 io=uring term=16 MiB sparse=false mtu=65504 socketBuffer=16 MiB rcvWindow=4 MiB` |
| 7ad0b2a (auto-MTU bug) | none | same | 14–18k rec/s; effective `mtu=1472` (wildcard bind treated as NIC) → fixed in b4321be |
| 7ad0b2a + explicit `RUNE_AERON_MTU=65504` | MTU only | same | 136,234 rec/s, errors=0 |
| client MTU sweep at broker mtu 1472 | — | client mtu 1408 / 65504 / 65504+1 MiB batch | 18.3k / 14.0k / 15.2k — client MTU is NOT the lever |
| shard_gate correctness, 3 client modes (agent / shard-invoker / client-invoker) | none | 100k acks=all, `--acked-file` + verify | all: failed=0 duplicates=0 missing=0 ok_missing=0 failed_stored=0 |
| direct-to-worker proof (open-loop 20k, FR on) | — | 460k records acked | `GatewayHandle` (fires right after the gateway decodes ANY produce) = 0 on all 3 nodes → no produce through the gateway |
| pre-460dd12 client | — | legacy bench | every record rejected (frame/header identity check) — by design |

Caveats / found along the way:
- One correctness run with `/dev/shm` at 852 MiB free (harness left the previous run's 3 GB work
  dir) reported 192 failed (reported honestly: failed_stored=0, ok_missing=0) on node 0's
  partitions. Hypothesis: non-sparse 16 MiB terms can't be allocated when shm is short —
  non-sparse must be capacity-aware, not just "is tmpfs". Harness fixed (stop() removes the work
  dir); hypothesis to be confirmed with FR.
- LATENCY (open-loop, explicit MTU 65504, 7ad0b2a): 20k/s → achieved 17.1k, p50 1.36 ms,
  p99 14.0 s; 50k/s → 42.5k, p50 3.9 ms, p99 4.9 s.  Multi-second stall + 1.4 ms base hop:
  next = full per-hop FR instrumentation (feat/fr-hop-instrumentation) and diagnosis.

### 4.2 Latency work, 2026-10-02 evening (k3s-node-01, clean box, logs off, FR off unless stated)

**Harness correction:** every controller harness script sourced the t15b-gates header, which EXPORTS
term 16 MiB / rcv window 4 MiB / socket 16 MiB / rings 16 MiB.  So the §4.1 "no tuning env" run was not a
pure-default run (term/window/socket equal the derived defaults; rings 16 vs 32 MiB).  Overrides are now
applied after the header; a true pure-default run is still owed.

| finding | evidence (single node unless stated) |
|---|---|
| single node vs 3 node, acks=leader 20k/s open-loop | p50 80-84 us vs 218-243 us (replication + cluster contention ~3x) |
| background fsync (10 ms/log) | acks=leader 3-node: p50 790 -> 424 us, p99 218 -> 37 ms when off (journal + io_uring io-wq preemption) |
| busy-spin vs backoff workers | no difference in p50/p99 (ABAB) -> backoff (frees cores) |
| client pinning / CPU steal / UDP loss / netfilter | ruled out (0% runq wait, 0% steal, no drops, NOTRACK active) |
| **16 MiB terms on low-volume ack/egress pubs** | term fills ~10 s -> rotation cleaning stalls 100+ ms -> client back-pressure: p99 11-31 ms. 1 MiB ack/egress terms: p99 **311-352 us** (from ~30 ms) |
| duty cap 1 + early ack flush + end-of-pass send (ABAB n=10) | p50 75.5 -> **66.3 us** (-12%, 9/10), p99 246 -> **211 us** (-14%, 7/10) |
| **3-node acks=all saturation, 1 KiB, combo + no fsync + 1 MiB control terms** | global term 16 MiB: 127.6/120.9/121.8/119.3k (median ~121k); **global term 1 MiB: 124.3/124.1/130.4/125.1k (median ~125k)** -> 1 MiB terms are not slower; drop 16 MiB terms (no rotation stalls, ~16x less /dev/shm) |

Single node now: p50 ~66 us, p99 ~211 us.  Remaining p99 = ~2 worker passes (pass p50 42 us / p99 115 us,
dominated by ~3 in-pass Aeron driver duty cycles).  Next lever under test: dedicated driver thread.
Experimental patches (env-gated, VM build /root/frcap only): RUNE_IOWQ_CORES, RUNE_WORKER_SYNC_INTERVAL_MS,
RUNE_SHARD_BACKOFF, RUNE_END_PASS_SEND, RUNE_EARLY_ACK_FLUSH, RUNE_DRIVER_DUTY_PER_PASS,
RUNE_AERON_CONTROL_TERM_BYTES; rune-fr: bounded tape (RUNE_FR_TAPE_RECORDS_PER_THREAD), Aeron site mask.

### 4.3 MTU 1408 vs 65504 (2026-10-03, k3s-node-01, interleaved ABAB n=5, clean box, logs off)

3-node acks=all, 1 KiB, saturate, 1.5M records, linger 0, term 1 MiB / rcvWindow 256 KiB, ack/egress term 1 MiB,
duty1+early+send, fsync off; broker RUNE_AERON_MTU and client --mtu set to the same value.  Build: /root/frcap
(env-gated experiments; FR compiled in but OFF; VM Aeron = FR-hooked lib whose wrappers rdtscp unconditionally,
~4-5% of a worker core — restore stock Aeron before final numbers).

| mtu | rec/s (5 rounds) | median | p50 / p99 (saturation = queueing) | CPU busy |
|---|---|---|---|---|
| 1408  | 43,137 / 48,347 / 45,468 / 47,810 / 49,033 | **47,810** | ~86 ms / ~420 ms | all 4 cores ~98% |
| 65504 | 167,271 / 154,628 / 171,834 / 166,445 / 166,930 | **166,930** | ~26 ms / ~115 ms | cpu0 ~98%, workers ~90% |

All 10 runs errors=0.  65504 (loopback only) beats Kafka's 156k on this box; 1408 (the real-network MTU) is 3.5x
slower: per-datagram kernel UDP cost (~45x more datagrams).  64 KiB MTU is not usable over real networks
(IP fragmentation) -> fix = UDP GSO/GRO transport bindings (spec 2026-10-03-aeron-udp-gso-gro-design.md).
Longer runs (1.5M vs 300k records) explain the higher 65504 numbers vs §4.2 (more steady state).
Client saturation profile (same day): crc32c = 28% of client CPU from ~6.6 rebuilds per sent frame on back-pressure
(LAT-1 h); clear_page from 64 KiB skb allocation ~7% of client CPU.

### 4.4 Quick-and-dirty UDP GSO/GRO assessment (2026-10-03, k3s-node-01, interleaved, stopped after 2 rounds)

Build: QD bindings (rune_gso_bindings.c from the GSO plan) env-gated RUNE_QD_GSO=1 in /root/frcap broker
EmbeddedDriver + /root/qd client copy (max_messages_per_send=16 when on).  Same binaries for every arm; config as §4.3.
GSO/GRO proven active: every on-arm logged "rune-gso: UDP GSO active ... seg=1408" and "UDP GRO active" on all brokers.

| arm | round 1 | round 2 |
|---|---|---|
| MTU 1408, GSO off | 44,570 | 44,700 |
| MTU 1408, GSO on  | **135,833** | **140,619** |
| MTU 65504, GSO off (loopback only) | 165,406 | — |

All errors=0.  GSO/GRO = 3.1x at the real-network MTU.  Remaining gap to 65504 (~15-18%) = Aeron per-fragment
work (~47 fragments per 64 KiB batch): suspected broker FragmentAssembler reassembly copy, per-fragment publication
header writes, 16-datagram send cap -> to be profiled (1408+GSO vs 65504) before fixing.  Proper implementation: GSO-1.

### 4.5 3-node acks=all latency (2026-10-03, k3s-node-01, 20k rec/s open loop, 1 KiB, MTU 65504)

**Cause of the ~13 ms acks=all p50:** the driver duty cap (duty1).  fsync off, everything else equal:
no flags 688 us / p99 9.6 ms; +duty1 24.2 ms / 45 ms; +early ack 672 us; +end-pass send 750 us; +early+end-pass 668 us.
Follower 20 ms fsync off: 14.2 -> 13.3 ms (minor).  -> LAT-1 (k) follower fsync off, (l) remove duty1, (m) remove end-pass.

**Per-batch waterfall** (rune-fr on client + 3 brokers, Aeron hooks SendData/RecvInsert/Sm*/SendSyscall/NAK/Recv-
Dispatch only, rings 64 MiB, tape 11M, dump right after the client exits; overhead check: p50 660 us vs 688 us clean;
completeness gate: 101,754 client batch offers = broker appends = waiter fires; all 101,754 batches traced).
offer -> ack p50 553 us.  Critical follower = the one whose ack reached the leader last.

| # | hop | p50 us | p90 us |
|---|---|---|---|
| 01 | client offer -> client driver send | 9 | 16 |
| 02 | client send -> leader driver recv | 46 | 140 |
| 03 | leader driver recv -> worker IngressFrame | 32 | 93 |
| 04 | IngressFrame -> append done | 8 | 17 |
| 05 | append -> push sent | 26 | 90 |
| 06 | push -> leader driver send | 27 | 61 |
| 07 | leader send -> follower driver recv | 53 | 192 |
| 08 | follower driver recv -> apply done (apply ~14) | 79 | 207 |
| 09 | apply done -> fast-ack sent | 1 | 1 |
| 10 | fast-ack -> follower driver send | 51 | 121 |
| 11 | follower send -> leader driver recv | 35 | 125 |
| 12 | leader recv (last ack) -> HW>=X | 44 | 122 |
| 13 | HW -> waiter fire | 20 | 54 |
| 14 | waiter fire -> ack claimed | 2 | 5 |
| 15 | ack claimed -> leader driver send | 27 | 55 |
| 16 | leader send -> client driver recv | ~0 (clock +-10) | 32 |
| 17 | client driver recv -> ClientAckRecv | 6 | 12 |
| -- | non-critical follower's ack arrived earlier by | 75 | 386 |

Real work ~30 us; ~85% is hand-off waiting (app -> driver send -> kernel -> driver recv -> app poll, ~9 hand-offs, each
a fraction of a ~42 us worker pass).  Levers toward 100 us: (1) send-now: run the publication's driver send right after
an offer (hops 05/06/10/15, ~130 us); (2) receive->handle-now: poll/dispatch the image in the same pass the driver
inserted it (03/08/12 + part of 02/07/11, ~150 us); (3) fire+claim acks inside the HW-advance step (13); all-in-sync's
slower-follower wait (75 us) shrinks with (1)+(2).  Client side total ~31 us (batch 14, send 10, ack poll 7).
Harness fixes needed for a valid capture: rune-fr export raced the background drainer (segfault with 64 MiB rings) ->
stopBackgroundDrain() before dumpAll (VM build); dump must not run inside the measured window; tape sized to the
client thread's event rate; idle per-loop Aeron hooks masked out (RUNE_FR_AERON_MASK=0x87B0F8).

### 4.6 Experiments that did NOT help + worker pass composition (2026-10-03, 20k acks=all, fsync off)

- Run-to-completion (driver receiver+sender called inside the pass, unconditionally): p50 worse (+100-300 us, 2 rounds).
- Conditional send-now (driver sender after a stage that offered): p50 479/471 -> 569/569 us (2 pairs, backoff). Reverted.
  Extra driver calls cost more than the hand-off they save (each sender call walks every publication + own sendmsg).
- Reference baseline with backoff workers: p50 ~475 us.

Pass composition (rune-fr, every span kept, backoff workers, Aeron duty hooks on; NOTE: 33-38M records dropped by ring
overflow at this event rate, so absolute counts are low-biased; the shares are the signal).  Pass p50 38-42 us,
8.7-12.9k passes/s per worker, workers 80-92% busy at 20k rec/s.

| per pass (avg over 3 workers) | us/pass | % of pass |
|---|---|---|
| Aeron driver duty (runs ~2.2x per pass) | 47-59 | **64-67** |
|   receiver duty: ~5 transports polled with recvmmsg each (~11-12 recv syscalls/pass, mostly empty, ~1.5 us each) | 24-29 | 32-34 |
|   sender duty: ~9 publications walked per duty, ~1 real sendmsg/pass (7-8 us) | 15-19 | 21 |
|   conductor duty | 7-9 | 10 |
| our replication poll (apply + fast-ack) | 10-14 | 13-15 |
| our client ingress | 4-8 | 6-9 |
| fused runtime | 3-4 | 4-5 |
| replica doWork (push/ISR/HW) | 2-4 | 3-4 |
| everything else (acks, disk, cold tail, conductor invoke) | ~2 | ~3 |

Aeron's transport poller iterates recvmmsg over every transport when there are <= 5
(AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD) and only uses epoll above that -> each worker pays ~5 empty recv
syscalls per duty, ~2.2 duties per pass.  Levers (no busy-spin): (1) fewer receive transports per worker (one
channel/port, many streams) -> 1-2 recv syscalls per duty; (2) one driver duty per pass unless the previous one
moved data (the "repeat while work" loop runs ~2.2x, second pass mostly housekeeping); (3) idle publications/images
walked every duty (~20 PubSend/ImageClean per pass) - fewer publications per worker.

### 4.7 Per-pass driver + control plane off the worker (2026-10-03, backoff workers, MTU 65504, fsync off)

| arm | 20k acks=all p50 / p99 | saturation rec/s |
|---|---|---|
| A base (fused, whole-driver loop) | 448 us / 9.8 ms | **200,506** |
| B thread placement + per-pass driver | 3,441 us / 40.9 ms | 91,574 |
| C thread placement only | 3,408 us / 42.2 ms | - |
| D per-pass driver only (conductor once, sender+receiver while moving data) | 430 us / 15.0 ms | - |

0 errors, 0 election/leader log lines in every arm.  Per-pass driver: neutral (430 vs 448, one pair).
Thread placement: 7.6x worse latency and -54% throughput -> the runtime thread (backoff park up to 200 us + its own
sleep-ns driver) is still on the produce->ack path in thread placement; "runtime off the data path" does not hold.
Both QD changes reverted.  New reference: saturation 200.5k rec/s with BACKOFF workers (MTU 65504, loopback).

### 4.8 REFERENCE CONFIG from here on: MTU 1408 + UDP GSO/GRO, backoff workers (2026-10-03)

65504 / loopback numbers are labelled comparisons only (user rule).  QD GSO build (broker RUNE_QD_GSO=1 + /root/qd
client), GSO/GRO proven active (19 "rune-gso: UDP G..." log lines across brokers, client install OK), fsync off,
term 1 MiB / ctl 1 MiB, 3-node acks=all 1 KiB, one run each:

| | p50 | p99 | rec/s |
|---|---|---|---|
| 20k open loop | **664 us** | 31.5 ms | 20,000 |
| saturation (1.5M records) | 22 ms (queueing) | 166 ms | **186,546** |

(Same build at MTU 65504: 448 us / 200,506 rec/s.)  All 0 errors.

### 4.9 Rune vs Kafka, small payloads (2026-10-03, k3s-node-01, 3 nodes on one box, 8 partitions, acks=all, no fsync)

Rune: MTU 1408 + QD GSO/GRO, backoff workers, 1 MiB terms, fsync off (leader + follower), early ack flush,
client deferred CRC (QD).  Kafka 4.0.0 KRaft (OpenJDK 17), RF3 min.insync=2, idempotent, no compression, 1 GiB heap
per broker (8 GiB box).  One run per cell, 0 errors everywhere.

Baseline (Kafka untuned: linger 5 / batch 256 KiB for throughput, linger 0 for latency; its latency INCLUDES JVM
warm-up, Rune's excludes 3 s):

| | Rune 64 B | Kafka 64 B | Rune 128 B | Kafka 128 B |
|---|---|---|---|---|
| saturation rec/s | 1,430,056 | 429,701 | 1,011,759 | 303,804 |
| p50 @20k | 424 us | 10 ms | 461 us | 17 ms |
| p99 @20k | 6.3 ms | 220 ms | 10.4 ms | 796 ms |
| p99.9 @20k | 13.7 ms | 263 ms | 23.3 ms | 921 ms |

Kafka tuning sweep (64 B, warm cluster, after a warm-up pass; stopped by the user before the last broker variant and
the final warmed 128 B / latency pass):
- producer (broker fetchers 4, net 3 / io 4, 1 MiB socket buffers, G1 MaxGCPauseMillis=20 + AlwaysPreTouch):
  linger 5: 541k (256 KiB) / 518k (1 MiB); linger 10: 630k / 619k; linger 20: 610k / **688k**; linger 50: 632k / 605k.
- broker (linger 20, 1 MiB): fetchers 1 net 3 io 4: 663k; fetchers 4 net 3 io 4: 674k; fetchers 1 net 4 io 8: 555k;
  fetchers 4 net 4 io 8: 631k.  Fewer threads win on 4 cores.
=> best tuned Kafka at 64 B ~ 674-688k rec/s vs Rune 1.43M (~2.1x).  Tuned Kafka latency not measured.

### 4.10 Integ on PURE defaults reproduces the tuned config (2026-10-03, bee4476, k3s-node-01)

No tuning env at all (harness sizes unset; the VM's stale cluster.sh forced 32 MiB terms until replaced by the repo
copy).  Effective config: term 1 MiB, ctlTerm 1 MiB, fsyncMs{worker=0 follower=0}, backoff workers, rcvWindow 256 KiB,
MTU 65504 (loopback, no GSO yet), 3-node acks=all 1 KiB:
- 20k open loop: p50 455 us / p99 14.2 ms (tuned QD: 448 us / 9.8 ms), 0 errors.
- saturation: 193,830 rec/s (tuned QD 200,506; early ack flush A5 not in the defaults yet), 0 errors.
OPEN: CPU_BUSY ~99% on all 4 cores even at 20k rec/s with BACKOFF workers -> the backoff idle effectively never
parks between passes (spin/yield phases).  "Never busy-spin" holds by name only; tune the backoff (backlog A7).

### 4.11 GSO-1: proper UDP GSO/GRO transport bindings (2026-10-03, k3s-node-01, interleaved ABAB, logs off)

Build: rune-protocol feat/gso-1 25cdbbb (rune_gso_bindings) + broker feat/gso-1 13c8e4d (EmbeddedDriver default on,
RUNE_AERON_UDP_GSO) on integ/perf-defaults (A1-A4), pure defaults otherwise; harness /root/e2e/tp.sh (vm-clean per
run, RUNE_FS_LOG=0, log level warn), 3-node acks=all, 1 KiB, saturate 1.5M records, linger 0, shards 1, term 1 MiB,
rcvWindow 256 KiB, ctlTerm 1 MiB, broker RUNE_AERON_MTU = client --mtu.  Off arm = RUNE_AERON_UDP_GSO=0 +
RUNE_CLIENT_AERON_UDP_GSO=0 (stock Aeron, 2 msgs/send); on arm = defaults (GSO+GRO, 16 msgs/send).  Same binaries
per ABAB.  VM Aeron = the FR-hooked driver lib (same as §4.3/§4.4; stock-lib restore still pending, §8).
Proof per on-run: 10-11 "rune-gso: UDP GSO active" + 8 "UDP GRO active" lines across the 3 brokers, 4 on the client,
effective config `gso=on` (off-runs: 0 lines, `gso=off`), 0 WARN fallbacks.  All runs errors=0.

| ABAB (client) | arm | rec/s per round | median | p50 / p99 (saturation queueing) | CPU busy |
|---|---|---|---|---|---|
| A: 6c43f00 + GSO diff, n=5 | 1408 off | 55,647 / 56,544 / 58,425 / 59,980 / 56,795 | **56,795** | ~62 ms / ~330 ms | ~98-100% all cores |
| | 1408 on | 178,022 / 194,133 / 174,143 / 182,051 / 187,450 | **182,051** | ~20 ms / ~115 ms | cpu0 ~99%, others ~95% |
| | 65504 on (loopback, labelled) | 207,589 / 210,719 / 218,805 / 209,534 / 204,127 | **209,534** | ~17-26 ms / ~74-142 ms | cpu0 ~99%, others ~93% |
| B: wt8 feat/client-gso 4a7dea5, n=4 (stopped: rounds agree) | 1408 off | 56,205 / 59,446 / 59,084 / 56,337 | **57,711** | ~81 ms / ~350 ms | ~98-100% |
| | 1408 on | 188,697 / 187,195 / 166,852 / 180,922 | **184,059** | ~22 ms / ~150 ms | cpu0 ~99%, others ~95% |

GSO/GRO = **3.2x at the real-network MTU** (vs QD §4.4: 3.1x); MTU 1408 + GSO reaches ~87% of the loopback-64K number.
Real-driver tests (broker rune_udp_gso_tests 11/11, client rune_transport_gso_tests 4/4) prove GSO sends carry >1
datagram, GRO splits, mixed peers, injected EAGAIN/EIO, 5% loss + NAK recovery, IPv6.

### 4.12 A7: shard idle that actually idles + epoll-first transport polling (2026-10-04, k3s-node-01, interleaved)

Binaries: A = broker perf/a7-idle base 770a9ba + client perf/a7-client-idle base ff60185 (= integ incl. GSO);
B = broker 60d6bdf + client fc509e8 (park-until-work shard idle + epoll-first Aeron polling + parking invoker bench,
all default on).  Release, gcc-13, conan toolchains of /root/rune-e5; rune-protocol f933fb9.  Config: reference §4.8
(MTU 1408, GSO/GRO on, backoff workers, RUNE_FS_LOG=0, RUNE_LOG_LEVEL=warn, fsync off), /root/e2e/tp.sh (vm-clean per
run), 3 nodes x 1 fused worker (cores 1/2/3), acks=all, 1 KiB, linger 0, shards 1, term 1 MiB, rcvWindow 256 KiB,
ctlTerm 1 MiB.  CPU = utime+stime deltas from /proc/<pid>/task/*/stat over a 5-7 s window mid-run (after the
producer exists); no perf/syscall counting inside A/B windows.  Order idle A,B; r20k A,B; sat A,B; r20k B,A; sat B,A.
Proof of path (B): "poll=epoll-first" per driver, "idle=park-until-work{wakeFds=2 spins=0 yields=0 parkNs=1000-200000}"
per worker.  All runs errors=0.

**Measured before tuning (A arm, probes).**  20k: each worker ~75-80% CPU, client ~75-80%, system idle ~0-1%,
sys ~47%.  Only 24-160 sched_yield/s and 6-24 clock_nanosleep/s per worker: the backoff is NOT spinning/yielding —
an instrumented pass (A7DIAG) showed >95% of passes doing work (~8-10k passes/s, ~4 driver duty cycles each,
driver ~70-80 us + worker ~25 us per pass).  Per worker ~175k recvmmsg/s (Aeron recvmmsg-s EVERY transport each duty
cycle while a poller holds <= 5 transports) + ~28k epoll_wait/s; profile top = syscall entry + __recvmmsg.  So the
brief's hypothesis (frames reset the backoff, threads live in spin/yield) is FALSE at 20k: the CPU is per-batch work
(linger 0: every record is a batch, ~30 us CPU per batch per node vs ~6 us/record at saturation) plus empty
polling.  Idle cluster: 39% per worker, the Aeron backoff (each "spin" = a full ~35 us syscall pass) re-armed by
heartbeats — that one IS idle-strategy waste.

| run | arm | worker CPU % (3 nodes) | client % | proc total | sys idle | p50 / p99 / p999 us | rec/s |
|---|---|---|---|---|---|---|---|
| idle (no producer) | A | 41.0 / 39.5 / 37.5 | – | 134% | 63% | – | – |
| | B | 24.8 / 22.8 / 22.1 | – | 86% | 75% | – | – |
| 20k r1 | A | 86.5 / 82.7 / 73.8 | 74.6 | 326% | 1% | 594 / 17,695 / 31,457 | 20,000 |
| | B | 77.9 / 75.1 / 71.7 | 71.8 | 305% | 4% | 541 / 23,986 / 73,925 | 20,000 |
| 20k r2 | B | 75.9 / 75.7 / 74.4 | 71.4 | 307% | 3% | 627 / 30,147 / 52,429 | 20,000 |
| | A | 86.2 / 82.8 / 81.1 | 82.2 | 341% | 1% | 610 / 29,622 / 53,215 | 20,000 |
| sat r1 (3M records) | A | 61.9 / 58.4 / 53.4 | 71.8 | 254% | 2% | 26,346 / 148,898 | 160,682 |
| | B | 65.0 / 62.1 / 60.0 | 41.3 | 238% | 9% | 24,248 / 145,752 | 161,727 |
| sat r2 | B | 62.1 / 61.2 / 61.2 | 40.4 | 237% | 10% | 24,379 / 148,898 | 159,902 |
| | A | 62.9 / 60.3 / 52.7 | 66.0 | 250% | 2% | 22,938 / 156,238 | 167,305 |

Result: idle cluster worker CPU **39% -> 23%** (B parks: 0 yields, ~3.9k ppoll/s = mostly the 200 us park bound kept
for ring/timer work).  20k: worker CPU **~82% -> ~75%**, client ~78% -> ~71%, process total 334% -> 306%; p50 equal
(602 vs 584 us); p99 r2 equal (29.6 vs 30.1 ms), r1 B worse (24.0 vs 17.7 ms) — the 20k p99/p999 is scheduler noise
on a full box in both arms, no regression claimed or excluded beyond that.  Saturation **164.0k -> 160.8k rec/s mean
(-2%, inside the A arm's own 4% round spread)**, bench thread 69% -> 41% CPU, system idle 2% -> 10%.  Epoll-first cut
per-worker recvmmsg ~175k/s -> ~30k/s (epoll_wait ~28k -> ~88k/s): passes got cheaper, so they got more frequent —
the workers still find work every pass at 20k.  Tried and dropped: a 20 us post-work batch-window sleep (interrupt
moderation) — worker CPU 75% -> 65% but p50 510 -> 1,057 us.  **The large CPU drop at 20k is NOT available from the
idle strategy**: it needs less per-batch work (batching at linger 0, fewer syscalls per replicated batch, cheaper
pass composition — §4.6).

#### 4.12.1 A7 fix round 1 (review C1/M1/M2/D1/D3) — A/B (2026-10-04, k3s-node-01)

Binaries: A = A7 as reviewed (broker 60d6bdf + client fc509e8); B = fix round 1 (broker 5dcd4fd + client 7e98e33,
rune-protocol 8636742: EPOLLERR consumed with SO_ERROR, Aeron 10 spins / 20 yields before the first park, io_uring
ring fd in the wake set, 1 us timer slack on shard runners).  Release, gcc-13, conan toolchains of /root/rune-e5.
Config as §4.12 (MTU 1408, GSO/GRO on, RUNE_FS_LOG=0, RUNE_LOG_LEVEL=warn, 3 nodes x 1 fused worker, acks=all, 1 KiB,
linger 0, term 1 MiB, rcvWindow 256 KiB, ctlTerm 1 MiB), /root/e2e/tp.sh with vm-clean per run; one round per arm,
interleaved A,B per scenario (fsync: a second round B,A).  CPU = utime+stime over a 5-13 s window mid-run.  Proof of
path (B): `idle=park-until-work{wakeFds=3 spins=10 yields=20 parkNs=1000-200000}` per worker (2 poller fds + the
io_uring fd), `poll=epoll-first` per driver.  All runs errors=0.  Scripts / raw: /root/a7r/{run,ab,ab2}.sh, res/.

| run | arm | worker CPU % (3 nodes) | client % | proc total | sys idle | p50 / p99 / p999 us | rec/s |
|---|---|---|---|---|---|---|---|
| idle (no producer) | A | 23.9 / 23.4 / 22.6 | – | 86% | 76% | – | – |
| | B | 31.8 / 31.6 / 30.8 | – | 110% | 69% | – | – |
| 20k | A | 77.7 / 77.3 / 74.8 | 71.7 | 310% | 5% | 492 / 31,588 / 63,439 | 20,002 |
| | B | 88.1 / 84.9 / 84.4 | 70.8 | 338% | 2% | 467 / 13,304 / 28,836 | 20,000 |
| sat (3M records) | A | 67.8 / 64.0 / 62.1 | 43.5 | 247% | 11% | 24,904 / 136,315 | 168,024 |
| | B | 70.8 / 66.3 / 64.8 | 45.0 | 259% | 8% | 21,627 / 139,461 | 168,663 |
| 20k + fsync 5 ms (leader + follower) r1 | A | 46.4 / 44.9 / 41.9 | 41.2 | 198% | 7% | 2,802 / 14.9 s / 15.0 s | 17,251 |
| | B | 41.8 / 35.6 / 27.3 | 12.2 | 129% | 59% | 6.5 s / 13.9 s / 14.0 s | 8,252 |
| 20k + fsync 5 ms r2 | B | 38.5 / 33.5 / 33.0 | 30.7 | 149% | 26% | 9,634 / 9.9 s / 10.0 s | 17,243 |
| | A | 33.6 / 29.9 / 19.5 | 12.7 | 109% | 68% | 1.5 s / 9.9 s / 10.0 s | 20,003 |

Peer-kill (idle cluster, `kill -9` of follower node 0; gateway/leader node 1), survivors' worker CPU over 5.6-5.8 s
while node 0 is down:

| arm | before the kill (3 nodes) | node 0 down (2 survivors) | sys idle before -> down |
|---|---|---|---|
| A | 24.4 / 23.6 / 23.1 | **95.1 / 85.7** (vcsw/s 3 / 469: no parks) | 77% -> 48% |
| B | 37.1 / 35.9 / 34.6 | **34.9 / 33.2** (vcsw/s ~3.1k: parking) | 67% -> 79% |

Result: **C1 reproduced in production on A and fixed in B** — with a peer down, A's surviving workers spin at 86-95%
(ICMP-unreachable EPOLLERR left pending, the poller epoll fd permanently ready); B's stay at their idle level.  The M1
ruling (Aeron spin/yield phase before the first park) costs CPU, as expected: idle worker **23% -> 31%** (still below
pre-A7's 39%), 20k worker **~77% -> ~86%** (pre-A7: ~82%; the spin/yield passes are cheaper than pre-A7's because
epoll-first polling stays).  20k p50 492 -> 467 us, p99 31.6 -> 13.3 ms (one run each; the 20k p99 spread is 17-30 ms
run to run in §4.12, no improvement claimed).  Saturation equal (168.0k vs 168.7k, +0.4%).  **fsync 5 ms at 20k is past
this box in both arms**: every run has p99 >= 9.9 s, and which arm collapses harder flips between rounds (r1 A 17.3k /
B 8.3k; r2 A 20.0k / B 17.2k with A's p50 1.5 s) — this scenario does not resolve the io_uring wake's latency effect;
the wake itself is proven by ParkUntilWorkIdleRegression.IoUringCompletionEndsParkRegression (Linux).  Open: a
sustainable durable-tier latency A/B (longer fsync interval or lower rate, ABAB) and §4.12 D2's n >= 15 ABAB.

### 4.13 D2: non-worker threads off the worker cores + backoff node-driver idle (2026-10-04, k3s-node-01)

Binaries: before = integ/perf-defaults 1767c3d; after = fix/d2-thread-placement 68c3a62 (wt35).  Release, gcc-13,
conan toolchains of /root/rune-e5, rune_broker + rune_role only; client = /root/d2/rune-client-cpp rune_produce_throughput
(same in every arm).  Config: reference §4.8 (MTU 1408, GSO/GRO on, backoff workers, RUNE_FS_LOG=0, RUNE_LOG_LEVEL=warn,
fsync off), /root/e2e/tp.sh (vm-clean per run), 3 nodes x 1 worker (explicit RUNE_WORKER_CORE_BASE 1/2/3), acks=all,
1 KiB, open loop 20k rec/s, 20 s, linger 0, shards 1.  One run per arm.  Per-thread CPU / last CPU / allowed list /
involuntary switches from /proc/<pid>/task/*/{stat,status} over a ~5 s window mid-run (scripts: /root/d2b/results/ab*.sh).
Proof of path (after): effective config `nonWorker=0 drvIdle=backoff` on every node; every non-worker thread
`Cpus_allowed_list=0`, workers 1/2/3.  All runs errors=0, ~20,000 rec/s.

| arm | p50 / p99 / p999 us | worker involuntary switches /5 s (3 nodes) | worker CPU % | node driver (threaded) CPU % | runtime thread CPU % |
|---|---|---|---|---|---|
| thread, before | 8,651 / 187,695 / 250,610 | 65.5k / 71.7k / 40.0k (sum 177k) | 83-85 | 47-48 (allowed 0-3) | 13-21 (allowed 0-3) |
| thread, after | 13,173 / 184,549 / 263,193 | 396 / 437 / 369 (sum 1.2k) | 99-105 | 20 each (allowed 0) | 13-18 (allowed 0) |
| fused, before | 598 / 40,370 / 65,536 | 9.4k / 4.8k / 6.6k (sum 20.8k) | 117-120 | – (invoker) | – (on worker) |
| fused, after | 549 / 50,594 / 81,265 | 557 / 355 / 414 (sum 1.3k) | 120-127 | – (invoker) | – (on worker) |

Result: the diagnosed mechanism is gone — worker preemption **177k -> 1.2k involuntary switches /5 s (thread, ~150x)**
and **20.8k -> 1.3k (fused, ~16x)**; the backoff idle cut each threaded node driver **~47% -> ~20% CPU**.  **Thread
placement latency did NOT recover**: p50 8.7 -> 13.2 ms, p99 188 -> 185 ms (single runs; p50/p99 at this level are
scheduler-noise-sized, so neither better nor worse is claimed).  The contention moved: core 0 now carries 3 node drivers
(~60%) + 3 runtime threads (~45%) + admin/cleaner + a share of the unpinned client — more than one core — and those
threads now take 21-23k (drivers) and 8-18k (runtimes) involuntary switches /5 s each (before: 0.6-3k and 2.6-6.7k).  So
thread placement on this box is bound by three co-located control planes sharing one core; the next lever is the control
plane's own CPU (why a control-plane-only node driver still burns ~20% at backoff, the runtime ~15%), localized with an FR
waterfall on the after binary.  Fused: p50 598 -> 549 us, p99 40 -> 51 ms (one run each; the 20k fused p99 spread is
17-30 ms run-to-run in §4.12 and 43 ms in the D2 diagnosis — no regression claimed or excluded beyond that).

#### 4.13.1 D2 round 2: the control plane actually idles (2026-10-04, k3s-node-01)

Follow-up to the waterfall on 68c3a62 (thread placement, gate 60,643 = 60,643): every app-side hop was fast; the 13.4 ms
p50 sat in RECEIVE delays (bench client receiving acks p50 ~5 ms; worker receives p90 5-7.5 ms).  Core 0 carried the
three nodes' control planes (~1 core), so the unpinned client time-sliced with a worker; pinned to core 0 it was starved
by the control planes instead (13.4 -> 8.7 ms).

Diagnosis (68c3a62, thread placement, `perf trace -s` on one broker over 5 s, IDLE cluster): node driver thread 88,034
recvmmsg (87,381 empty), 5,062 sched_yield, 2,250 nanosleep - Aeron backoff (10 spins, 20 yields, park 1 us doubling to
1 ms) re-arms on every heartbeat / status message, and every spin or yield pass is a whole duty cycle (a recvmmsg per
socket, ~60-90 us).  Runtime thread: 14,912 sched_yield + 17,296 nanosleep (10 spins, 20 yields, 1 us -> 200 us).
Idle, the control plane (3 drivers + 3 runtimes + admin + cleaner) used ~76% of core 0 with ~2.8k involuntary switches /s
per driver.  Driver idle shape sweep (same binary, AERON_SHARED_IDLE_STRATEGY_INIT_ARGS, idle, per driver): default 11.6%
-> 1-0-50us-1ms 9.5% -> 1-0-100us-2ms 7% -> 1-0-250us-4ms 5.5%; involuntary switches 2.9k/s -> ~1/s once yields are 0.

Fix (rune-broker fix/d2-thread-placement **2013c2e**, wt35): shard mode, control-plane threads park without yielding -
`NodeConfig::controlIdleShape` = 10 spins, 0 yields, park 100 us doubling to `RUNE_CONTROL_IDLE_MAX_PARK_US` (default
2000) for the runtime thread (thread placement) and the admin thread; the threaded node driver gets the same shape as
Aeron backoff init args with ONE spin pass (`1-0-100000-2000000`; `RUNE_AERON_DRIVER_IDLE_ARGS` overrides).  Legacy mode
unchanged; never busy-spin.  Logged `drvIdle=backoff{1-0-100000-2000000} ctlIdle=10-0-100us-2000us`.  Worker idle and
worker invoker drivers untouched (A7).

A/B: before = 68c3a62, after = 2013c2e (the box built the same tree), same config as §4.13 (tp.sh, 16 MiB
terms, MTU 1408 GSO/GRO, logs off, 3 nodes x 1 worker on cores 1/2/3, non-worker set {0}), 20k rec/s open loop, 1 KiB,
acks=all, linger 0, 20 s.  Thread arms: bench client pinned to core 0 (`--core 0`) in both.  Fused-after: client
unpinned, the conditions of the 68c3a62 fused row above.  One run per arm, interleaved, all errors=0, ~20,000 rec/s.
Per-thread CPU over a 5 s window mid-run (/root/d2c/tsamp.py; raw: /root/d2c/res/ab/ on the box).

| arm | p50 / p99 / p999 us | node drivers (3) | runtimes (3) | admin+cleaner (6) | client | workers (each) |
|---|---|---|---|---|---|---|
| thread, before (68c3a62) | 15,073 / 371,196 / 482,345 | 44.8% (invol 2.3-2.4k/s each) | 25.7% | 6.2% | 23.5% (starved, invol 3.7k/s) | 58-59% |
| thread, after (2013c2e) | **545 / 162,529 / 234,881** | **19.1%** (invol ~80/s) | **7.8%** (invol 0) | 5.6% | 67.9% | 92-93% |
| fused, after (client unpinned) | 447 / 45,351 / 73,925 | - (invoker) | - (on worker) | 6.0% | 94.3% | 95-97% |

Idle cluster (thread placement, 5 s, no client): control plane before 75.6% of core 0 (drivers 34.8, runtimes 31.6, admin
5.2, cleaner 4.0) -> after **43.4%** (drivers 21.7, runtimes 12.7, admin 3.4, cleaner 5.6); workers 97% -> 84% (sum of 3).

Result: **thread placement p50 15.1 ms -> 0.55 ms** (client pinned to core 0 in both arms) and p99 371 -> 163 ms; the
control plane's CPU at 20k drops **~77% -> ~33% of core 0**, and the client on core 0 gets the CPU it needs (23% -> 68%).
Thread p50 is now in the fused range.  Fused after (447 / 45,351 us) vs 68c3a62 fused (549 / 50,594) and integ (598 /
40,370): no regression (single runs; fused p99 spread 17-51 ms).  Workers do more CPU at the same 20k after (58 -> 92%):
the client is no longer starved, so it no longer sends coalesced bursts - at linger 0 that is one record per batch, the
per-batch work A8 targets.  The thread p99 (163 ms) is still far above fused (45 ms): next, an FR waterfall on 2013c2e.
Remaining idle cost: each node driver ~7% (~800 wakeups/s x ~85 us duty cycle: recvmmsg per socket); A7's epoll-first
polling, once merged, applies to the node driver too and should cut the per-cycle cost.

### 4.14 A8: work per batch at 20k rec/s — what batching can and cannot do (2026-10-04, k3s-node-01)

Question: can the per-batch work at linger 0 be cut (client coalescing, per-pass push / fast-ack / ack batching, fewer
syscalls per replicated batch) for a large CPU drop at 20k rec/s with no p50/p99/saturation loss?
Binaries: A = integ/perf-defaults 50c8dbb (broker) + client feat/client-gso ff60185 + f1a4fa0 (bench only: `batches=` /
`records_per_batch=` in the invoker RESULT line).  B = the same sources + rune-fr 5fe5464 (the only product change, see
below).  Release, gcc-13, conan toolchains of /root/rune-e5.  Config: §4.8 reference (MTU 1408 + GSO/GRO, backoff workers,
RUNE_FS_LOG=0, RUNE_LOG_LEVEL=warn, fsync off), 3 nodes x 1 fused worker, acks=all, 1 KiB, linger 0, shards 1, term 1 MiB,
rcvWindow 256 KiB, ctl term 1 MiB, tp.sh with vm-clean per run (scripts and raw results: /root/a8/).

**Measured first (A, 20k, one worker, 4 s `perf trace -s` + 4 s `perf record`):**
- Records per batch at 20k = **1.03** (460,000 records / ~446k batch acks, all 4 runs; sat ~50-52).  Arrivals are
  50 us apart and round-robin over 8 partitions; the invoker client already sends everything appended in one pass as
  one frame per partition (spec §4.2, ClientShard "end-of-pass batching"), and a pass almost never holds 2 records of
  one partition.  Idea (1) is already implemented; there is nothing more for it to coalesce at 20k.
- Per worker per second: recvmmsg 66.5k (78.6% empty), epoll_wait 11.5k, pwrite64 + pwritev 12.2k, sends
  (sendmsg + sendto + sendmmsg) 8.1k, sched_yield 143, clock_nanosleep 5.  A node sends ~29.5k replication + ack
  messages/s (6.7k batches led: 2 pushes + 1 ack each; 13.3k applies: 1 fast-ack each) in 8.1k send syscalls =
  **~3.6 messages per send syscall**: pushes, fast-acks and acks to the same peer already share one datagram per pass
  (one ExclusivePublication per peer per worker; Aeron packs every message offered before the sender duty).  Idea (2)
  is what the transport already does; per-pass the worker has ~1 batch per partition to push, so there is no second
  batch to fold into one push.
- Profile (worker): kernel 56%, rune_broker 25%, libc 18%; syscall_enter_from_user_mode 10.0% + __recvmmsg 9.7% (the
  empty polls, idea (3) — A7's epoll-first, not on this base, removes most of them), copies 3%, netfilter ~2%, pwrite +
  ext4 ~2.5%, **rune_fr_c_now 2.6% + rune_fr_c_span 0.5%**.  Client: recvmmsg 13.5% + syscall entry 10.3%,
  **rune_fr_c_now 4.5% + rune_fr_c_span 1.2%**.
- The workers never park at 20k: ~1 wake event (non-empty receive) every ~70 us per worker, and Aeron backoff needs
  10 consecutive empty passes before its first yield and 30 before the first park.  So worker CPU at 20k is set by the
  idle shape + the event rate, not by the per-batch work: cutting work per event makes passes shorter, not fewer
  (same CPU%).  The box is saturated in every arm (system idle 0-2%).

**Found waste, fixed:** the patched Aeron C hooks call `rune_fr_c_now()` at every duty-cycle span start, and it read the
TSC (rdtscp, slow in this VM) even with the recorder disabled.  rune-fr 5fe5464: returns 0 while disabled (a span
started at 0 already records nothing).  RED-first test `HopPrimitivesRegression.CApiNowIsZeroWhileDisabledRegression`.
Proof of path: B's `rune_fr_c_now` disassembly checks `rune_fr::g_enabled` before any rdtscp (A: rdtscp first), in both
rune_broker and rune_produce_throughput.

**Tried and dropped:** repeat the shard pass's driver duty (A1 loop) only while a backlog may be left, instead of
after any work.  A byte threshold (one full send batch) left receive backlogs of small datagrams (2 per transport
per duty): the 20k run collapsed to p50 26 ms / ~2 records per batch.  A receive-full counter in the GSO bindings fixed
the receive side on paper, but the Linux tests showed window-limited sends (31,680 B < 32,768 B threshold) also escape
any byte signal.  A correct signal needs send- and receive-side "full" counts through the bindings, and it is capped
at 20k by the idle shape above.  Kept for reference: rune-broker branch perf/a8-duty-gating-wip (86017d8).

A/B (interleaved A,B per scenario, 2 rounds; per-thread CPU over a ~8 s window mid-run, /proc/<pid>/task/*/stat):

| | A r1 / r2 | B r1 / r2 |
|---|---|---|
| 20k worker CPU (mean of 3) | 77.3 / 74.2% (mean 75.8%) | 75.0 / 75.7% (mean 75.3%) |
| 20k client CPU | 78.3 / 80.8% | 76.5 / 76.6% (-3 pts) |
| 20k records per batch | 1.03 / 1.03 | 1.03 / 1.03 |
| 20k p50 | 561 / 619 us | 643 / 553 us |
| 20k p99 | 15.5 / 26.3 ms | 29.2 / 37.2 ms |
| sat rec/s (3M records) | 153,592 / 156,827 (mean 155.2k) | 173,191 / 161,865 (mean **167.5k, +8%**) |
| sat worker CPU (mean of 3) | 57.7 / 60.0% | 58.3 / 59.1% |

All runs errors=0, gso=on, 20,000 rec/s achieved.  Result: **the 20k CPU goal is not met**: worker CPU is unchanged and
the client is -3 points.  The rdtscp removal shows up where passes are work-bound: saturation is +8%, and B beats A in
both rounds (B's lower round, 161.9k, is above A's higher, 156.8k).  p50 is neutral (two rounds each, which overlap).
The 20k p99 is higher in both B rounds (29-37 vs 16-26 ms), but stays inside the 13-51 ms run-to-run spread already
recorded for this config (§4.12, §4.13); n=2 does not prove it neutral.

What would move 20k CPU: (a) an idle that parks after an empty pass without the 30-pass spin/yield phase, which
needs the eventfd doorbell on cross-thread rings (A9), because ring and timer work has no fd; (b) A7's epoll-first, for
the ~20% of each worker spent in empty recvmmsg; (c) fewer events per record at the same latency.  Every message is
already coalesced per pass, so (c) means coalescing across passes, i.e. a linger, i.e. latency.  The D2 "58% at a
starved client" number is exactly that trade: bursts produce idle gaps long enough to park.

### 4.15 A9: ring doorbells — cross-thread ring work wakes a parked shard worker (2026-10-04, k3s-node-01)

Question: with an eventfd doorbell on the cross-thread rings (so ring work ends a park like a datagram does), can the
shard worker drop A7's Aeron spin/yield phase (review M1) and park longer, for a large 20k worker-CPU cut with no
p50/p99/saturation loss?
Binaries: A = broker perf/a7-idle a3b89e1 (A7 fix round 1 + re-review: doorbell-less park-until-work, 10 spins /
20 yields, 1 µs .. 200 µs parks).  B = perf/a9-doorbell 2b81b03 (doorbell on the worker's op / session rings, portable
completions, ret_ space wait, fused-runtime rings on worker 0; doorbell shape 0 spins / 0 yields, park 1 ms .. 1 ms;
worker-path claims retry ADMIN_ACTION once).  Same client in both arms (rune-client-cpp perf/a7-client-idle 7e98e33),
same rune-protocol 8bd5d23 / rune-fr 5fe5464.  Release, gcc-13, conan toolchains of /root/rune-e5.  Config: §4.12
(MTU 1408, GSO/GRO on, RUNE_FS_LOG=0, RUNE_LOG_LEVEL=warn, fsync off, 3 nodes x 1 fused worker on cores 1/2/3,
acks=all, 1 KiB, linger 0, term 1 MiB, rcvWindow 256 KiB, ctlTerm 1 MiB), /root/e2e/tp.sh with vm-clean per run.
CPU = utime+stime over a 5-8 s window mid-run (/proc/<pid>/task/*/stat).  Order: idle A,B; r20k A,B; sat A,B; r20k
B,A; sat B,A; then (follow-up arm "B-hyb" = the B binary with RUNE_IDLE_SPINS=10 RUNE_IDLE_YIELDS=20, i.e. doorbell +
Aeron spin/yield + 1 ms park) idle, r20k.  Proof of path: B `idle=park-until-work{wakeFds=4 doorbell=5rings spins=0
yields=0 parkNs=1000000-1000000}` per worker (2 poller fds + io_uring fd + the doorbell; 5 rings = op + session + the
3 admin → control rings of the fused runtime); A `wakeFds=3 spins=10 yields=20 parkNs=1000-200000`.  All runs
errors=0, gso=on.

| run | arm | worker CPU % (3 nodes) | client % | proc total | sys idle | p50 / p99 / p999 us | rec/s |
|---|---|---|---|---|---|---|---|
| idle (no producer) | A | 31.8 / 29.8 / 28.5 | – | 106% | 71% | – | – |
| | B | 10.5 / 9.8 / 9.8 | – | 46% | 87% | – | – |
| | B-hyb | 19.1 / 18.1 / 17.6 | – | 72% | 78% | – | – |
| 20k r1 | A | 85.5 / 84.4 / 82.4 | 73.1 | 334% | 4% | 377 / 22,020 / 49,021 | 20,000 |
| | B | 76.6 / 75.0 / 71.4 | 67.3 | 299% | 5% | 518 / 27,918 / 78,119 | 20,000 |
| 20k r2 | B | 77.8 / 76.9 / 75.2 | 68.2 | 308% | 6% | 471 / 11,600 / 45,351 | 20,000 |
| | A | 84.6 / 79.2 / 74.4 | 73.7 | 321% | 2% | 395 / 19,792 / 40,108 | 20,004 |
| 20k (follow-up) | B-hyb | 86.6 / 86.3 / 85.3 | 74.0 | 341% | 4% | 356 / 9,503 / 36,438 | 19,999 |
| sat r1 (3M records) | A | 68.0 / 65.1 / 64.2 | 43.9 | 254% | 8% | 22,938 / 147,849 | 161,429 |
| | B | 68.8 / 68.3 / 59.9 | 43.9 | 252% | 9% | 22,676 / 131,596 | 169,671 |
| sat r2 | B | 66.0 / 66.0 / 62.8 | 49.2 | 254% | 8% | 23,462 / 128,451 | 169,578 |
| | A | 72.2 / 65.3 / 62.4 | 43.6 | 253% | 8% | 21,627 / 106,955 | 173,820 |

Probe before the A/B (B binary, single runs, not interleaved): 20k with the default 1 ms .. 1 ms park: workers
76.6% mean, p50 453 us, p99 13.4 ms, ~1.0k parks/s per worker; with RUNE_IDLE_DOORBELL_MIN_PARK_US=1 (doubling from
1 µs): 77.7%, p50 467 us, p99 23.3 ms — the doubling buys nothing once ring work has an fd.

Result:
- **Idle cluster: worker CPU 30.0% -> 10.0% (B) / 18.3% (B-hyb)**, process 106% -> 46% / 72%, system idle 71% ->
  87% / 78%.  Parks are ~860/s per worker (the 1 ms timer bound) instead of ~3.2k/s (200 µs).  This is free: every
  wake source with work has an fd, so longer parks lose nothing.
- **20k, B (spin/yield off): worker CPU 81.8% -> 75.5% mean (-6.3 pts), client 73.4% -> 67.8%, process 328% ->
  304%; but p50 386 -> 495 us (+109 us; both B rounds, 518 / 471, above both A rounds, 377 / 395).**  p99 is inside the
  usual 10-30 ms spread (B 27.9 / 11.6 ms vs A 22.0 / 19.8 ms).  Mechanism: at 20k an event lands every ~70 us
  (§4.14); A's 30-pass spin/yield phase (~300 us of polling) spans that gap, so A never parks (A: 36-134
  voluntary switches/s per worker) and reacts at once; B parks after the first empty pass (464-1,313 vcsw/s) and each
  event then pays a park wake — on this saturated box (system idle 2-6%) the worker's pinned core is often running
  the unpinned client or softirq when its fd fires.  The 6 points of CPU A spends at 20k ARE the polling that buys
  the p50.
- **B-hyb (doorbell + Aeron spin/yield + 1 ms park): 20k unchanged — workers 86.1% (A r1 84.1%), p50 356 us (A 377 /
  395), p99 9.5 ms — and idle 30.0% -> 18.3%.**  That is the shipped default (perf/a9-doorbell 0367963: auto
  spin/yield = Aeron 10 / 20 with or without the doorbell; measured here through env on the 2b81b03 binary); spins/yields 0 stay an explicit opt-in
  (RUNE_IDLE_SPINS=0 RUNE_IDLE_YIELDS=0) for CPU-bound boxes that can pay ~0.1 ms p50.
- **Saturation 167.6k -> 169.6k rec/s mean (+1.2%, inside A's 161-174k round spread)**; passes are work-bound there,
  the idle shape does not matter.
- So the brief's 20k goal (win back M1's +8-9 points with no p50 loss) is **not available from the idle strategy on
  this box**: the doorbell removes the correctness reason for the spin/yield phase (fd-less ring work no longer waits
  for the park cap — review MEDIUM-2 closed), but the latency reason remains while the box is saturated.  What would
  move it: fewer events per record (cross-pass coalescing = latency), a cheaper pass (§4.6), or headroom (the client
  off the worker cores).
- Multi-worker arm (2 workers per node): not run — the harness refuses the layout on this box ("3 nodes x 2 workers
  from core 1 needs cores 1..6 but the box has 4"; pinned busy-spin-safe layout).  The cross-thread doorbell path
  (runtime → worker 1 ops, worker 1 → fused runtime returns) is covered by rune_shard_mode_tests on Linux (3 nodes x
  2 fused workers, real UDP) and rune_worker_doorbell_tests (parked shard runner, real invoker driver, 2 s park:
  a cross-thread op is taken in well under 500 ms).
- VM benchmark time ~28 min (3 probes, the 10-run A/B + 2 refused mw runs, 2 follow-up runs).  Raw summaries were
  copied off the box; /root/a9 deleted.

## 5. Where the time goes now (measured)

> **VALIDITY CORRECTION (2026-10-02 ~11:40) — read before using ANY number in this section.**
> 1. **All client-latency numbers here are for the wrong path.** The bench client
>    (`rune_produce_throughput` @ client 8e3d136) does not consume `MetadataResponse.ingressEndpoint`
>    (client T16 not implemented), so every produce goes client → node client endpoint → gateway →
>    runtime → WorkerOp ring → worker → back, instead of the designed client → worker ingress
>    (40500+w) direct path. These numbers do not measure the shard design; latency benchmarking is
>    **suspended until client T16 lands**.
> 2. **Every FR run with `RUNE_FR_RING_BYTES=268435456` was on a degraded 2-node cluster.**
>    Six `rune_broker` OOM kills in dmesg: 3 brokers × ~1.4–1.9 GB anon (256 MiB per-thread FR
>    rings) + ~0.7 GB shm each on 7.7 GB. The window=1 "p99 113 ms / 340–586 ms" results are
>    failover stalls, not service time. Fixes: harness refuses a run with < 3 live brokers
>    (`INVALID-RUN`), dumps FR one node at a time (waits on the tape, which is written last), and
>    FR runs use 64 MiB rings. `e2e.py` asserts all 4 traces exist.
> 3. Treat the per-hop table below as indicative only (gateway path, possibly degraded cluster).

At saturation the client p50 (~65 ms) is window queueing, not service time. The latency floor
(16 in flight, ~10k rec/s) is far above target:

| hop (FR, low load) | p50 | p99 |
|---|---|---|
| append → push sent | 27 µs | 412 µs |
| push sent → follower applied | 266 µs | 1.4 ms |
| fast-ack sent → leader received | 108 µs | 1.0 ms |
| append → ack claimed (broker) | 542 µs | 2.8 ms |
| client send → ack | 1.2 ms | 3.1 ms |

Per shard pass (~16–22 µs): Rune's own logic ~2.5–7 µs; **one Aeron driver duty ~13–16 µs**:
- receiver duty 8.5 µs p50 — Aeron iterates `recvmmsg` over every receive socket while there are
  ≤ 5 (`AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD`), mostly empty (75 % EAGAIN);
- a single loopback UDP send syscall: ≤256 B p50 19.5 µs / p99 67 µs (after the iptables fast
  path); the sender's syscall runs the receiver's network stack inline;
- Aeron sender: `network_publication_max_messages_per_send` = 2, sender/receiver io vectors = 2
  (max 16) — at most 2 datagrams per syscall per publication per duty;
- sends cannot coalesce across publications (plane×peers, per-session ack pubs, consensus,
  gateway egress, status messages) — at low load every small message is its own syscall.
Also: a message produced by the worker in pass N is sent by pass N+1's driver duty (≈1 extra pass
per hop × ~4 hops).

## 6. Next steps (in order, each measured with the same FR setup)

0. **PREREQUISITE for any further measurement:** client T15b (ShardGroup) → client T16
   (`ingressEndpoint`, produce straight to the owning worker) → client T17/T18 (open-loop
   `--rate`, full histogram). Only then re-baseline latency and throughput.

1. Profile the remaining ~20 µs per tiny loopback send (kernel stacks of `sendmsg`).
2. Separate tests: `network_publication_max_messages_per_send` + sender/receiver io-vector
   capacity raised (plumbed as NodeConfig options); iptables fast path on vs off.
3. Aeron poller: always epoll (threshold) — one `epoll_wait` + reads of ready sockets only.
4. Pass ordering: a send-side driver duty at the end of the pass so worker output leaves the
   same pass.
5. Client: produce straight to the worker (`ingressEndpoint`, client T16) — removes the gateway
   hop and its copy; batching without linger; sleeping idle (low CPU); open-loop `--rate` bench
   with full histogram (client T17/T18) — the only valid way to measure p99 at 159k.
6. Push once per batch instead of once per follower (multi-destination publication) — halves
   push copies + term cleaning.
7. Session-exact Aeron hop pairing `(session_id, termOffset)`.

## 7. Open tickets found along the way

- **DUR-1 (after the latency work): replication-based durability without per-write fsync.** Measured: the 10 ms
  background fsync halves the median and costs 6x on p99 (acks=leader, 20k/s: p50 790 -> 424 us, p99 218 -> 37 ms when
  off). Before relaxing it by default, verify/implement: unclean-shutdown truncation of the unflushed tail (KIP-101),
  no leadership/ISR/HW eligibility for a replica restarted after an unclean shutdown until caught up (KIP-966-style),
  explicit flush-interval config (Strict tier unchanged), minIsr gating, rack-aware placement, and DR fault tests
  (kill -9 all, hard crash with unflushed data: no acknowledged record lost). Details: broker SDD ledger DUR-1.
  DR testing for DUR-1 (and from then on for every durability/replication change):
  - PRIMARY: very fast in-process integration tests that drive disaster scenarios deterministically (no Docker,
    no sleeps, injected clock + fault injection at the storage/transport seams): unclean-shutdown truncation,
    leader crash with an unflushed tail, follower restart after unclean shutdown (no ISR/HW eligibility until caught
    up), ISR shrink/expand under faults, KIP-101 epoch truncation, partial writes / torn batches, producer-state
    rebuild after crash (EOS: no duplicate, no OOS for stored batches), split-brain / stale leader fencing,
    kill-all with acks=all -> no acknowledged record lost.  Seconds per suite; part of the default test run.
  - SECONDARY: the dockerized e2e DR matrix (dr-e2e) re-run on k3s-node-01 and extended with the same edge cases
    (kill -9 all, hard crash with unflushed data, rolling restarts under load, netem loss/delay/partition,
    disk-full, slow follower) as the real-process confirmation.


- **LAT-1 (after throughput/p99 analysis): productionize the env-gated latency experiments.** Today they exist only as
  uncommitted, env-gated patches in the VM build /root/frcap/rune-broker. Turn each into real code, default-derived
  config, tests and an Opus review, then re-prove with ABAB on k3s-node-01:
  (a) per-stream term sizing: 1 MiB terms on low-volume ack/egress/control publications (RUNE_AERON_CONTROL_TERM_BYTES;
      removes 100+ ms rotation stalls) and evaluate 1 MiB as the global default (3-node 1 MiB >= 16 MiB, §4.2);
  (b) early ack flush right after the ingress stage (RUNE_EARLY_ACK_FLUSH);
  (c) end-of-pass driver send when the pass did work (RUNE_END_PASS_SEND);
  (d) driver duty cap per pass = 1 (RUNE_DRIVER_DUTY_PER_PASS);
  (e) worker fsync interval as config (RUNE_WORKER_SYNC_INTERVAL_MS; final semantics owned by DUR-1);
  (f) io_uring io-wq affinity (IORING_REGISTER_IOWQ_AFF, RUNE_IOWQ_CORES) keeping io-wq off worker cores;
  (g) backoff vs busy-spin worker idle (RUNE_SHARD_BACKOFF; no measured latency difference -> backoff frees cores).
  (k) follower fsync off by default: the follower's hard-coded 20 ms coalesced flush (kFollowerFlushIntervalNs,
      PartitionReplica.cpp) becomes configurable and OFF by default, same as the leader worker sync (e) — durability
      via replication (Kafka model); final semantics owned by DUR-1.  QD knob on the VM build: RUNE_FOLLOWER_FLUSH_MS.
      (20k acks=all A/B with it effectively off: p50 14.2 -> 13.3 ms — not the main cost, but drop it.)
  (l) REMOVE duty1 (driver duty cap per pass = 1, item d): it starves the in-pass Aeron driver on the 3-node
      replication path.  20k acks=all, fsync off: p50 688 us without it vs 24.2 ms with it (p99 9.6 vs 45 ms).
      It only helped single-node (client acks only).  Do not productionize (d); keep the uncapped duty loop.
  (m) REMOVE end-of-pass send (item c, RUNE_END_PASS_SEND): no benefit on 3-node acks=all (20k, fsync off:
      p50 750 us / p99 10.6 ms with it vs 688 us / 9.6 ms without).  Do not productionize (c).
  (n) NEVER busy-spin (user rule 2026-10-03): shard workers default to backoff idle (today busy-spin when a core
      base is set; RUNE_SHARD_BACKOFF is the VM experiment knob; measured: no p50/p99 difference), no dedicated
      busy-spin driver threads, client agents back off.  All perf numbers from here on are taken with backoff.
  Client CRC/encode waste (2026-10-03 saturation profile, 3-node acks=all: crc32c_hw = 28% of client CPU; uprobes
  counted 35,897 buildAppend calls CRC-ing 1.52 GB in 2 s vs ~116 MB/s produced = ~6.6 builds per sent frame):
  (h) no rebuild on back-pressure: probe the publication (position < publication limit, the exact Aeron refusal
      condition) BEFORE encoding; a refused offer must not rebuild header + record copy + CRC every agent cycle.
      Records keep joining the held batch.  WIP in rune-client-cpp-wt4 (uncommitted): canOffer() on
      AeronTransport/ClientAgent/AgentProduceSink/CountingSink + ProducerImpl::sinkCanOffer_ gate in encodeAndSend and
      encodeAndSendTier2; ProducerSink.BackPressuredSinkIsNotReEncodedRegression RED (50 rebuilds/50 cycles) -> GREEN.
      Remaining: full client suites, Opus review, commit, re-profile on the box.
  (i) incremental CRC patch: Tier-2 send stamp and retransmit re-sequencing re-CRC the whole batch after patching
      producerId/epoch/baseSequence.  Use CRC32C linearity (crc32c_combine-style shift of the XOR delta, O(log n))
      instead of a full rescan; bit-exact tests vs full recompute (random patches, every field offset, len 0..64 KiB).
  (j) faster CRC32C (only if still in the top of the profile after h+i): 3-way interleaved _mm_crc32_u64 streams
      combined with PCLMUL (~8 B/cycle vs ~2.7 B/cycle single chain), shared in rune-protocol (broker validation too).
  Also: drop the extra full-frame copy (encodeScratch_ -> keepFrame): encode once into the retained retransmit
  buffer and offer from it.  MTU stays network-sized (1408 default; no 64 KiB MTU over real networks) -> offer() for
  multi-MTU batches; real-network throughput lever = UDP GSO/GRO transport binding (separate spec).
  Each item: RED-first test where it is a behavior change, *Regression* marker where it fixes a bug (rotation stall),
  no allocation/virtual/std::function on the hot path, numbers in this doc.

- **GSO-1 (proper implementation, after the quick-and-dirty assessment): Aeron UDP GSO/GRO transport bindings.**
  Spec: docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md.  Plan (TDD, 7 tasks, real-driver tests for
  mixed peers / EAGAIN / loss+NAK / EIO fallback / IPv6, full suites + DR e2e, perf ABAB, Kafka-vs-Rune over veth
  MTU 1500): docs/superpowers/plans/2026-10-03-aeron-udp-gso-gro.md.  Motivation: MTU 1408 = 47.8k vs MTU 65504 =
  166.9k rec/s (§4.3).  The quick-and-dirty assessment build (env-gated, VM only) is NOT the deliverable.

- F26 Aeron shm budget counts per driver, not per publication (SIGBUS mid-run) — partly addressed
  by `RUNE_AERON_TERM_SPARSE=false` (fails at creation instead).
- F27 R1 dead-peer stall (fix round 2 in flight).
- F28 default rcv window 128 KiB vs ~1 MiB batches; validate window vs socket buffer at startup.
- F29 one-off read-map miss on a freshly rolled segment.
- F30-M1 seal in flight across an ownership move (latent; moves throw today).
- Broker main (from client C10): pipelined transactional batches → OutOfOrderSequence;
  **coordinator segfault on EndTxn**; group subscribe redirect (client).

## 8. VM state to restore after benchmarking

- Aeron driver lib: restore `/root/rune-e5/aeron-backup/libaeron_driver_static.a` into
  `/root/.conan2/p/b/aeron5913a5b3cc7e3/p/lib/` and the driver sources from `driver-c-src`.
- iptables: `/root/rune-e5/iptables-lo-revert.sh`.
- `perf_event_paranoid` back to 4; `systemctl start k3s` (regenerates kube chains).
