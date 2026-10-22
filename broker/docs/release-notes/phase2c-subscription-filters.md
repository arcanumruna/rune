# Release note: Phase 2c — subscription filters

A subscription, or a consumer group, can now ask the broker for only the records whose fields match. The broker
evaluates the filter on the partition leader, next to the log, and sends the matching records only. A client API
reference is in [docs/api/client-cpp.md](../../../docs/api/client-cpp.md#subscription-filters).

## 1. Semantics

- **What a filter is.** Up to 8 terms ANDed; each term is one value or up to 64 values (`anyOf`) of one fixed-width field:
  `Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`, `Enum`, `Bool`, or `string(N)` up to 16. Never `string`, `bytes` or
  `float64`. An empty filter is no filter.
- **Typed topics only.** The filter names fields by their durable tag, so the topic needs a schema. A subscription on an
  untyped topic is refused.
- **No match, no delivery.** A record with no value (a tombstone), one too short to hold the filtered field, one whose
  schema version does not carry the field, and one whose schema version the broker does not know after
  `RUNE_FILTER_VERSION_WAIT_MS` are excluded and counted. The first absent-field exclusion per subscription and version is
  logged (`filtered field absent`).
- **Isolation first.** `read_committed` hides open and aborted transactions before the filter runs; the last stable
  offset bound is unchanged.
- **Delivery.** A batch whose records all match is sent as stored. Otherwise the broker sends a rebuilt batch with the
  matching records, their original offsets, and the source batch's `baseOffset` and `lastOffsetDelta`. Offsets therefore
  have holes.
- **Positions.** A filtered subscription's position moves past the records the filter skipped. The broker sends a
  position note after `RUNE_FILTER_NOTE_INTERVAL_MS` or `RUNE_FILTER_NOTE_RECORDS` skipped records, so the position
  reaches the log end on a quiet filter. A consumer group's committed offset follows the same way.
- **Moves.** A subscription that follows a leader move re-subscribes with the same filter bytes from its position.
- **Consumer groups.** One filter per group, byte-identical, fixed by the first member and changeable once the group is
  empty. A member with another filter is refused at JoinGroup (`group filter mismatch`).
- **Refusals are explicit.** A filter the broker cannot serve is answered `InvalidRequest` (39) with a detail
  (`filter needs a typed topic`, a malformed or unknown term, `filter capacity: ...`); the client never falls back to
  an unfiltered subscription.
- **Schema changes.** Tags are durable, so a filter survives ALTER. A field added later matches only records that carry
  it; a field dropped later matches nothing in the records written after the drop.

## 2. Wire and versions

- Client version 11 and broker version 6 (`kClientVersionFilter`, `kBrokerVersionFilter`). The filter is a trailing field
  of `Subscribe` and `JoinGroup`, and the refusal detail a trailing field of `SubscribeResponse` and
  `JoinGroupResponse`, all present only at version 11. An unfiltered `Subscribe` is byte-identical to version 10.
- New append-only values: `OpType::FILTER_SCHEMA = 24`, `TailKind::FILTER = 11`, the admin-to-control `kA2cFilterSchema =
  25`, `SubscriptionFailReason::BrokerTooOld = 9`, `GroupFailReason::FilterMismatch = 6`; `Record::partition` and
  `SubscriptionStatus::batchBytes` are appended members.

## 3. Upgrade

**Upgrade every broker before any client subscribes with a filter.** A broker older than version 6 would ignore the
trailing filter field and deliver every record. The client therefore checks the broker version of the session it
sends the `Subscribe` to and, below 6, sends nothing and ends the subscription with `BrokerTooOld`. In a mixed cluster
a filtered subscription whose partition is led by an old broker ends the same way; unfiltered subscriptions are
unaffected.

Nothing else changes for existing traffic: unfiltered subscriptions, producers and groups behave and perform as before.
The catalog has no new event, no layout change and no fingerprint change; topic schemas and `GetTopicSchema` are
unchanged.

## 4. Configuration

Six knobs, all in [docs/configuration.md](../../../docs/configuration.md):

| Variable | Default |
|---|---|
| `RUNE_FILTER_MAX_SUBS_PER_PARTITION` | `4096` |
| `RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION` | `4096` |
| `RUNE_FILTER_NOTE_INTERVAL_MS` | `50` |
| `RUNE_FILTER_NOTE_RECORDS` | `65536` |
| `RUNE_FILTER_VERSION_WAIT_MS` | `5000` |
| `RUNE_FILTER_MATCH_SCRATCH` | `65536` |

A subscription over a cap is refused, never queued; the worker reserves its tables at the same caps.

## 5. Legacy worker mode

`RUNE_WORKER_MODE=legacy` does not serve filters. A filtered `Subscribe` is refused with `filter needs
RUNE_WORKER_MODE=shard`. Shard mode is the default.
