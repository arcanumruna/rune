# Release note: Phase 2b F - the unified transactional Producer

One `Producer` type now covers plain, acked and transactional produce. The separate `TransactionalProducer` is gone.

## 1. Breaking: `TransactionalProducer` is removed

`Client::transactionalProducer(id)` and the `TransactionalProducer` / `TransactionalProducerImpl` headers no longer exist.
Create a `Producer` with `ProducerConfig::transactionalId` instead; its lifecycle is `initTransactions`,
`beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction` (token forms and `*AndWait`
forms), plus `flush`, `state()`, `producerId()`, `producerEpoch()`, `transactionActive()` and `transactional()`.

| Before | Now |
|---|---|
| `client.transactionalProducer("id")` | `client.producer({.acks = Acks::All, .transactionalId = "id"})` |
| records only through the transactional type's own `send` | every produce path takes part: `send`, `trySend`, `batch()`, `tryClaim`, `claimBatch`, a typed claim (`openTopicAndWait<T>(name, producer)`) |
| rewind a consumer after an abort: re-create it | `GroupConsumer::seekToCommittedAndWait()` (or `seek(partition, offset)`) |

Rules the client enforces:

- `transactionalId` is at most 255 bytes; `acks = None` with a `transactionalId` is rejected (`std::invalid_argument`).
- A transactional producer outside a transaction refuses every produce path with the new `SendResult::NotInTransaction`
  (the blocking `send` / `sendTo` throw `std::logic_error`).
- On a plain producer the lifecycle token calls return `OpResult::InvalidArgument` and the `*AndWait` forms throw
  `std::logic_error("<op>: producer has no transactionalId")`.
- A newer producer with the same `transactionalId` fences the older one (`TxnState::FatalError`).
- The status getters work on the thread that created the producer (or the one that called `bindBatchThread()`).
- `ClientShard` refuses a `transactionalId`.

## 2. Acked producers ask the broker for an id on connect

A producer without a `transactionalId` now sends `InitProducerId` with an empty transactional id when it connects and
stamps the id the broker assigns. A resend after a reconnect carries the same id and sequence numbers, so the broker
deduplicates it. Records sent before the id arrives are held, never sent with a guessed id. `Acks::None` is unchanged
(no id, no deduplication).

## 3. New: `SendResult::NotInTransaction`

Added to `SendResult`. Code that switches over `SendResult` must handle it (a plain producer never returns it).

## 4. New: Tier-2 slot pool knob

`ProducerConfig::tier2Slots` (default 64) sizes the preallocated pool of `batch()` slots; a batch no longer allocates
per batch. With every slot in use `batch()` returns a refused builder whose `send()` is `BackPressured`; size it at
least twice `maxInFlight`. `tier2SlotBytes` (0 = derived) sets the slot capacity.

## 5. Tested end to end

`broker/docker/eos-e2e.sh` runs the loop on 3 authenticated brokers: 200 000 records through a consumer group and a
transactional producer on every produce path, every 7th transaction aborted and redone after `seekToCommitted`, the
transaction coordinator and an `eos_out` partition leader killed mid-run, a zombie producer fenced. A `read_committed`
reader then finds every id exactly once with `v' = 2 * v`. The example is `client-cpp/examples/eos_probe.cpp`.
