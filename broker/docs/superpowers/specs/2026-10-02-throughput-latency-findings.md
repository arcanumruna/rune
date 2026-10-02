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
| ENV | Loopback fast path on k3s-node-01 (`lo` NOTRACK + ACCEPT ahead of ~1055 kube rules / 201 chains). Revert: `/root/rune-e5/iptables-lo-revert.sh`. | box config | small-send syscall p50 30 → 19.5 µs, p99 110 → 67 µs; throughput unchanged (98–103k) |

Measured and **not** the cause (ruled out with data): storage `pwrite` is not I/O-bound
(off-CPU stacks: 0 samples in dirty throttling/locks — slow writes were CPU descheduling under
oversubscription); `fallocate` preallocation works (64 MB allocated per active segment);
`kPushFlowWindow` (16384 records) is not the limiter; netfilter alone is not the per-packet floor.

## 4. Current best config (100–105k rec/s run)

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
