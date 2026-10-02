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
