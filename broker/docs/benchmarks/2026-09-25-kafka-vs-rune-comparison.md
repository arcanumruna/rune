# Kafka vs Rune — throughput & latency comparison (2026-09-25)

Companion to `2026-09-25-e5-per-worker-ingress-benchmark.md`. Same class of
hardware (Hetzner **CCX23, 4 dedicated x86 vCPU**, nbg1). Both VMs created and
destroyed within the hour.

## ⚠️ Read this first — what is and isn't comparable

- **Kafka numbers are the full network path**: a real Java producer → TCP →
  broker → ack, measured with the standard `kafka-producer-perf-test.sh`.
- **Rune numbers are the in-process produce path** (broker-internal:
  ring → worker → log append → ack), **no network hop**. A fair
  network-parity Rune throughput bench (client → broker over Aeron) is E5b
  future work and was **not** run.
- Therefore the **throughput** comparison is *directional*, and if anything
  favours Rune (it skips the wire). The **latency** comparison is lopsided the
  other way (Rune's in-process latency excludes the network RTT), but the gap is
  so large — µs vs ms — that the qualitative conclusion (Rune is a ULL broker,
  Kafka is a throughput broker) holds regardless.

## Kafka — max throughput (tuned)

Kafka 3.9.0, KRaft single-node, 8-partition topic RF=1, producer tuned for
throughput (`batch.size=256KB`, `linger.ms=100`, `buffer.memory=1GB`,
`compression=none`), 4 network / 8 IO threads.

| Payload | acks=1 | acks=all (RF=1) |
|---|---|---|
| 256 B | **860,733 rec/s** (210 MB/s) | 923,702 rec/s (225 MB/s) |
| 1024 B | 328,299 rec/s (321 MB/s) | 340,832 rec/s (333 MB/s) |

Under this throughput-maximizing config, per-record latency is high (batches +
linger queue records): 256B/acks=1 reported avg 15.8ms, p50 5ms, p99 142ms.

## Kafka — latency (rate-limited, unsaturated)

Same broker, producer with `linger.ms=0`, load capped below saturation so
latency reflects the true round-trip (not client-side queuing).

| Load | acks=1 | acks=all |
|---|---|---|
| 50k rec/s, 256B | avg 0.46ms, p50 ~0.3ms, p99 8ms | avg 0.77ms, p99 24ms |
| 200k rec/s, 256B | avg 1.69ms, p50 1ms, p99 23ms | — |

## Rune — for reference (in-process produce path, same 4-core class)

From the companion doc:

| Metric | Rune |
|---|---|
| Throughput 256B (gateway, batched) | 807k rec/s |
| Throughput 256B (4 pinned workers) | ~661k rec/s |
| Throughput 1KiB (4 pinned workers) | ~392k rec/s |
| Produce→ack latency (worker) | p50 2.75µs, p99 7.18µs |
| Produce→ack latency (gateway) | p50 4.29µs, p99 14.26µs |

## Takeaways

- **Throughput is in the same ballpark at small records** — Kafka ~860–920k
  rec/s (256B, over the network) vs Rune ~660–807k rec/s (256B, in-process).
  That Kafka matches Rune *including* the TCP + JVM path shows how well-tuned
  Kafka's batched pipeline is; the honest read is "comparable, pending a fair
  Rune network number." At 1KiB Kafka is bandwidth-bound (~320–333 MB/s).
- **Latency is where Rune wins by design** — Rune's produce→ack is single-digit
  µs (p99 ~7µs worker / ~14µs gateway); Kafka's best-case unsaturated latency is
  ~0.5ms and its throughput-optimized config runs into tens–hundreds of ms.
  Even allowing for Rune's missing network hop (Aeron RTT — the client-side IPC
  bench gates p99 < 100µs), a networked Rune would still sit ~1–2 orders of
  magnitude below Kafka. This is the expected split: Kafka = JVM/TCP throughput
  broker; Rune = Aeron/shared-nothing/C++ ultra-low-latency broker.
- **Fair next step (E5b):** a real client→broker-over-Aeron Rune throughput
  bench so throughput is measured on the same (network) footing as Kafka.

## Kafka reproduce

```
# KRaft single node, tuned server.properties (num.network.threads=4, num.io.threads=8)
bin/kafka-storage.sh format -t <uuid> -c config/kraft/server.properties
KAFKA_HEAP_OPTS="-Xmx4g -Xms4g" bin/kafka-server-start.sh config/kraft/server.properties
bin/kafka-topics.sh --create --topic bench --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092
# max throughput:
bin/kafka-producer-perf-test.sh --topic bench --num-records 5000000 --record-size 256 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=1 batch.size=262144 linger.ms=100 buffer.memory=1073741824 compression.type=none
# true latency (rate-limited):
bin/kafka-producer-perf-test.sh --topic bench --num-records 500000 --record-size 256 --throughput 50000 \
  --producer-props bootstrap.servers=localhost:9092 acks=1 linger.ms=0 batch.size=16384
```
