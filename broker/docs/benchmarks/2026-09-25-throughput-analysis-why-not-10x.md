# Why Rune only *ties* Kafka on throughput despite 100× lower latency (2026-09-25)

**Short answer: our benchmarks measured latency, not throughput.** They drove the
produce path **serially, one record (or ten) at a time**, so the "throughput"
number was essentially `1 / latency`. Kafka's number was measured with ~1000-record
batches and deep pipelining — *saturated* throughput. We compared apples (our
`1/latency`) to oranges (Kafka's saturated pipeline) and they happened to land close.
Rune's produce path is architecturally batch-amortized just like Kafka's, so with
equal batching + pipelining it should convert its latency edge into multiples of
Kafka's throughput.

## Evidence 1 — our "throughput" was serial 1/latency

`WorkerProduceLatencyBench` (the ~661k/s "4-worker" figure):

```cpp
for (auto _ : state) {          // each iteration = ONE record
    op.recordCount = 1;         // batch of 1
    enqueue(op);
    while (!acked) worker->doWork();   // WAIT for this record's ack …
}                                       // … before the next record starts
SetItemsProcessed(iterations);  // "items/sec" = iterations/sec = 1/latency
```

- Batch size = **1 record**. In-flight concurrency = **1** (wait-for-ack before next).
- `241k/s` solo = `1 / 4.15µs`. The 4-worker `661k/s` is just 4 independent serial
  loops. **This is a latency benchmark; its records/sec is the inverse of latency,
  not a throughput ceiling.**

`GatewayDataBench` (the ~807k/s figure): batches **10 records/batch**
(`batches_per_sec=80.4k`, `records_per_sec=804k`, `batch_bytes=351`). Better, but
still tiny batches and limited pipelining.

## Evidence 2 — Kafka's number is a fully-batched, pipelined pipeline

Kafka `producer-perf-test` (our run): `batch.size=256KB`, `linger.ms=100`,
`max.in.flight=5`, 8 partitions, 256B records ⇒ **~1000 records per batch**.

Broker side (`LogSegment.append` → `FileRecords.append`):
```java
int written = records.writeFullyTo(channel);   // ONE sequential write of the WHOLE batch
```
The whole ~1000-record batch is written as an opaque blob; the index is updated
**per-batch** (every `indexIntervalBytes`), never per-record. CRC/validation is
done once per batch. Kafka's per-record broker cost on the hot path ≈ 0.

## Evidence 3 — Rune's broker does the SAME opaque batch append

`Worker::executeProduceWire_`:
```cpp
// re-stamp 8-byte base offset once, then:
log->appendNoSync(tail, tailLen, lastOffset);   // ONE append of the whole batch blob
```
- The batch (`tailLen` bytes = the entire pre-encoded RecordBatch of `recordCount`
  records) is appended in one shot, zero-copy from the ingress ring.
- `handleProduceBatch` validates producer-state / sequence **once per batch**.
- No per-record iteration on the broker hot path — identical model to Kafka.

**So there is no per-record broker tax in Rune.** The measured parity is purely a
function of how much the *benchmark* batched.

## The math (Little's Law): throughput = concurrency ÷ latency

| | batch | in-flight | per-batch overhead | records/s |
|---|---|---|---|---|
| Kafka (measured) | ~1000 | 5 × 8 part | ~ms/request | ~860k |
| Rune GatewayBench (measured) | 10 | ~1 | **~12.4µs/batch** (1/80.4k) | 807k |
| Rune WorkerBench (measured) | 1 | 1 | ~4µs/record | 241k/core |

The decisive comparison is **per-batch overhead**: Rune ~**12µs/batch** vs Kafka
~**milliseconds/request** — roughly the same 100× gap as the latency numbers. Rune
reached the *same* 807k as Kafka while doing **~80,000 batches/s** against Kafka's
**~hundreds of requests/s**, i.e. Rune paid its (100× cheaper) overhead 100× more
often because it batched 100× less. Batch equally and the latency edge becomes a
throughput edge.

### Predicted ceiling (NOT yet measured — honesty)

If Rune carried Kafka-scale batches (~1000 × 256B ≈ 256KB) at its measured
~80k batches/s/gateway, the arithmetic gives tens of millions of records/s — which
memory/append bandwidth would cap first. Realistically the ceiling is
**bandwidth-bound at several million records/s per core**, i.e. the ~10× you
expected, *while keeping* low per-batch latency (batching is a knob:
`batchBytes`/`lingerUs`, `maxInFlight`). This is a prediction from the architecture
+ the 12µs/batch datapoint, **not a measurement**.

## What to actually do (E5b throughput bench)

Build a real saturated-throughput bench and measure it, so the Kafka comparison is
on equal footing:

1. **Batch**: use `RecordBatchBuilder` (or the client `Producer` with
   `batchBytes≈256KB`, `batchRecords≈1000`) so each append carries ~1000 records.
2. **Pipeline**: `maxInFlight` high (≥ 32) so the worker never waits for an ack to
   start the next batch (the current bench's fatal serialization).
3. **Fan out**: drive all 8 partitions / 4 workers concurrently and sum.
4. Report records/s + MB/s + p50/p99 **at the batched operating point**, plus a
   latency-vs-throughput curve (vary batch/linger) — that curve is the real story:
   Rune should sit far below Kafka on the latency axis at every throughput point.
5. For a true head-to-head, run it over the network (client → broker over Aeron),
   matching Kafka's `producer-perf-test` footing.

## Bottom line

The "Kafka ties us" result is a **benchmark artifact**: we measured `1/latency`
(batch=1, in-flight=1) and 10-record batches against Kafka's ~1000-record pipelined
saturation. Rune's broker append is batch-amortized and zero-copy exactly like
Kafka's, and its per-batch overhead is ~100× lower — so once we batch and pipeline
like Kafka, the 100× latency advantage should translate into a large throughput
advantage. The number just hasn't been measured yet; the E5b bench above is how we
prove it.
