# Rune Phase 2c — Subscription filters (DRAFT, awaiting user review)

**Status:** draft; the open questions are resolved (§13). It awaits the user's final approval. Scheduled after Phase 2b Part A.
**Repos:** rune-codec (generator), rune-protocol (Subscribe filter field), rune-broker (evaluation and push), rune-client-cpp (API and encoder), rune-ql (`WHERE` → filter, later with Part D's console).

## 1. Goal

A subscriber names the records it wants by **typed field values**, for example `worldId == x`. The broker drops everything else before it reaches the network.

The motivating case is an MMORPG:
- The deployment has 200,000 players.
- Each world server needs about 1,000 of them.
- The events live on one shared topic keyed by player.

Without a filter, every world server receives everything and throws away about 99.5%.

Success means all of the following:
- Network and client load fall in proportion to selectivity.
- Broker cost per record stays **O(1) regardless of the number of filtered subscribers**.
- The produce→deliver latency class is unchanged (p50 50 µs / p99 200 µs).

The benchmark gates in §11 prove this.

## 2. Non-goals (v1)

- Ranges (`<`, `>`), `OR`, `NOT`.
- Filtering on variable-length fields (strings, bytes).
- Floating-point fields.
- Expressions over two fields.
- Changing a live subscription's filter. Instead, unsubscribe and re-subscribe from `position()`; nothing is lost or duplicated.
- Filters on untyped topics. They are rejected explicitly (§9).
- View sources that use filters. §12 explains how the same compiled predicate serves them later.

## 3. Subscription model: in addition to keyed subscriptions, not a replacement

A subscription is **which partitions** plus an **optional filter**.

| Partition selection | API today | With a filter |
|---|---|---|
| The partition a key hashes to | `subscribe(topic, key, opts, h)` | unchanged; the filter applies within that partition |
| An explicit partition | `subscribe(topic, partition, opts, h)` | unchanged; the filter applies |
| **Every partition of the topic** (new) | — | `subscribe(topic, AllPartitions{}, opts, h)` |
| A consumer group's assignment | `subscribeGroup(group, topics, cfg)` | `cfg.filter(...)` applies to every assigned partition |

- Code without a filter behaves exactly as today, byte for byte on the wire.
- `subscribe(topic, key)` keeps its meaning: it delivers **the whole partition** that the key hashes to. For exactly one key's records on a typed topic with a key field, combine the two: `subscribe(topic, key, opts.filter(E::Filter{ .playerId = key }))`. The partition selection keeps the broker to one partition, and the filter keeps it to one key. The documentation states this explicitly, because the partition-wide meaning of `subscribe(topic, key)` has surprised users.
- A consumer group has **one filter**, shared by every member (§13.3). A member with a different filter is rejected with an explicit error. Committed offsets stay log offsets (§7).

## 4. API: a functional definition, checked at compile time

For each schema, the rune-codec generator also emits a **filter-by-example** struct, `E::Filter`. It has one optional member for every fixed-width, non-float field. A subscriber sets the members it cares about, and every member that is set becomes one term. Set members combine with AND.

```cpp
using E = rune::gen::PlayerEvent;

client.subscribe(topic, AllPartitions{}, opts.filter(E::Filter{ .worldId = worldX }), h);

client.subscribe(topic, AllPartitions{},
                 opts.filter(E::Filter{ .worldId = worldX, .channel = anyOf(1, 2, 3) }), h);

cfg.filter(E::Filter{ .worldId = worldX });        // a consumer group
```

- **It reads as data, not as an expression.** A member is one of:
  - unset, which is the default and means "any value";
  - a plain value, meaning equality;
  - `anyOf(v...)`, meaning one of these values (IN).

  Nothing is evaluated on the client. The struct is a description that the client encodes and the broker applies to each record.
- **Generated member type:** `rune::Match<T>`, where `T` is the field's C++ type.
  - It holds the state (unset, Eq or In) and the values inline, with up to 64 values in a fixed array.
  - So an `E::Filter` is a plain aggregate that can be copied, built on the stack, and kept as a member of a world-server object.
- **Compile-time checks:**
  - **Value types:** a value of the wrong type for the field does not compile.
  - **Unknown or ineligible fields:** a field that does not exist does not compile. Variable-length and float fields have no member, so they do not compile either.
  - **`anyOf` limit:** more than 64 constant values is a `static_assert`. `anyOf(span)`, with runtime values, is checked when the filter is encoded and fails explicitly before anything is sent.
  - **Designated-initializer order:** C++ requires designated initializers in declaration order, so members follow the schema's field order. Out-of-order designators are a compile error, not a runtime surprise. The generated header's comment lists the order.
  - **Term limit:** at most 8 terms. A schema with more than 8 eligible fields still generates every member. Setting more than 8 of them fails explicitly at encode time.
- **An empty filter is no filter.** `E::Filter{}` with nothing set is an unfiltered subscription and encodes as zero length.
- **Version independence:** each member is generated with its field's **durable tag** (from schema evolution), so a filter survives `ALTER` of the topic's schema. After regeneration, a newly added field simply appears as a new member.
- **Cost:**
  - Encoding walks the set members in tag order with straight stores into a fixed stack buffer: no parsing, no heap allocation.
  - For a filter whose values are all constants, `constexpr` encoding is possible.
- **Runtime builder,** for tools, the console and QL:

  ```cpp
  Filter::on(descriptor).field("worldId").eq(42).field("channel").in({1, 2, 3})
  ```

  - It resolves names against the topic's schema descriptor when it is built.
  - It produces the **same encoding** as `E::Filter`.
  - An unknown name, a type mismatch or an exceeded limit is an explicit error before anything is sent.
  - Rune QL `... WHERE worldId = 42 AND channel IN (1,2,3)` lowers to this builder.

## 5. Encoding: what Subscribe carries

The encoding is flat and fixed-width little-endian. It has no varints, so the encoder is straight stores and the decoder is bounded reads.

```
filter := [formatVer u8 = 1][termCount u8 (1..8)] term{termCount}
term   := [tag u16][primType u8][op u8: 1=Eq, 2=In][count u8 (Eq: 1; In: 1..64)]
          [value × count]     // each value is the field's fixed width (FixedStr: its declared length)
```

- `worldId == x` (u32) takes 2 + 5 + 4 = **11 bytes**.
- The largest legal filter is bounded: 8 × (5 + 64 × 8) + 2 = 4,138 bytes. That is far below the transport's maximum message length, so a valid filter always fits one Subscribe frame.
- **In the wire message:** `Subscribe` (templateId 18) gains a second variable-length field after `key`, a `u16` length prefix followed by the filter bytes. The field is present only when the session's client version is 7 or higher (`kClientVersionFilter`).
  - A v7 client sends a zero length for "no filter".
  - Older clients and brokers never see the field.
  - A v7 client connected to a broker that predates filters is refused at session setup by the existing version negotiation, so a filter is never silently dropped.
- **Canonical form:** the client sorts `in` values and sorts terms by tag before encoding. Two equal filters therefore encode identically, which the broker's index relies on (§6).

## 6. Broker evaluation

The broker evaluates filters **once per record, at the push point that owns the subscription**:
- the shard worker, for owned subscriptions;
- the gateway control thread, in the legacy worker mode.

All filter state is owned by that thread (shared-nothing). Registration and removal arrive through the existing subscription rings.

**At Subscribe (cold path):**
1. Decode and validate the filter against the topic's schema. Every tag must exist in some version; the type must match the field's type; the field must be fixed-width, non-float; the limits must hold. Any failure is answered explicitly (§9).
2. Pick the **index term**: the first term in canonical order, normally `worldId`.
3. Build a **per-version accessor table** for each term's tag. For every schema version of the topic, store either (offset, width) or "absent in this version". The table lives in a preallocated slab. When `ALTER` adds a version, it is extended through the existing schema-distribution ring, before any record of the new version can reach this push point.

**Per partition:** an **index** maps each distinct index-field tag to an open-addressing table from value to a list of filtered subscriptions.
- In practice there is one index field, because every world server filters on `worldId`.
- Unfiltered subscriptions stay on today's path, untouched.

**Per record:**
1. Read the record's writer version (u16), which every typed record already carries.
2. Look up the accessor for each distinct index field: an array index.
3. Read the fixed-width value; one hash lookup gives the candidate subscriptions.
4. For each candidate with more than one term, check the remaining terms: an equality compare, or a probe of the sorted `in` list.

The cost is O(1) in the number of filtered subscribers. Work grows only with the number of **matching** subscribers.

A field that is absent in the record's version means **no match**. Under three-valued logic, an unknown value excludes the row, so a record is never delivered by mistake.

All tables and slabs are sized from the configuration and preallocated. Steady state allocates nothing. Exceeding a configured cap answers the Subscribe explicitly (§9) and never allocates.

## 7. Delivery: batch rebuild, positions, transactions, moves

**Batch rebuild:**
- A filtered subscription whose source batch is entirely matching gets the **original batch**, zero-copy.
- A partly matching batch is **rebuilt**: the matching records are copied into the subscription's preallocated egress buffer as a new batch. Each record keeps its **original offset**, and the batch header and CRC are recomputed.
- A batch with no matches sends nothing.
- Unfiltered subscribers are unaffected.

**Position progress:** a long run of non-matching records would otherwise leave the client's `position()` stale. The broker sends the existing v6 **position note**, an empty batch carrying an offset. It sends one when a filtered subscription has skipped records for a configurable interval (default 50 ms) or a configurable record count (default 64 Ki). `position()` therefore reflects the log, and group commits move forward on quiet filters.

**read_committed:** the filter applies **after** isolation.
- Control batches and aborted or open transaction data are withheld exactly as today.
- Committed records are then filtered.
- The LSO bound is unchanged.

**Moved subscriptions (E2):** the re-subscribe after `NotLeader`, a Redirect or a position note carries the **same filter bytes** and resumes from `position()`. The resume floor and the straddle filter operate on offsets, so they are unchanged.

**Consumer groups:**
- Committed offsets are log offsets, as today.
- A member restarting from a committed offset re-applies its filter from that offset.
- The transient lineage rules (E2 C1) are unchanged.

## 8. Schema evolution

- Filters name fields by durable tag, and tags are never reused. A filter compiled against version V keeps its meaning against later versions.
- **Field dropped in a later version:** records of that version do not match the term. `ALTER` is not blocked. The admin answer lists the live filtered subscriptions that reference the dropped tag, so operators see the effect.
- **Field retyped (when retype ships):** the per-version accessor table gives the type for each version. A term whose encoded type does not match a version's type treats that version as absent, so nothing matches.
- **A record whose version is unknown at this push point:** this cannot happen, because versions are distributed before any record of that version is produced. If it ever does, it is counted and treated as no match. It is never a crash and never a silent delivery.

## 9. Errors (every request answered; no silent fallback)

| Condition | Answer |
|---|---|
| A filter on an untyped topic | `InvalidRequest` (39), detail "filter needs a typed topic" |
| A malformed encoding, an unknown tag, a type mismatch, a var-len or float field, a limit exceeded | `InvalidRequest` (39), detail naming the term index |
| A filter-index or slab capacity cap is reached | `InvalidRequest` (39), detail "filter capacity"; a config knob, documented |
| A group member whose filter differs from the group's | `InvalidRequest` (39) at JoinGroup, detail "group filter mismatch" (§13.3) |
| A broker that predates filters | refused at session version negotiation (§5) |

The client surfaces each of these as a failed `Subscription` with the error. It never quietly falls back to an unfiltered subscription.

## 10. Interaction matrix (tests)

Every cell is covered by a parameterized unit matrix. Shared-state paths get a seeded, model-checked stress over the index. A BrokerNode integration slice and a docker e2e leg complete the coverage.

- **Selection:** key / partition / AllPartitions / group.
- **Filter:** none / Eq / In / Eq∧In / several index fields across subscribers.
- **Topic:** disk / transient; typed with and without a key field.
- **Isolation:** read_uncommitted / read_committed (open, committed and aborted transactions).
- **Batch:** all match / partial / none / a straddling resume.
- **Timing:**
  - steady;
  - a long non-matching run (position notes);
  - a leader move mid-stream;
  - a restart;
  - a transient ring eviction;
  - an `ALTER` that adds a field, drops the filtered field, or adds a version mid-stream;
  - a subscribe racing the `ALTER`.
- **Errors:** every row of §9.

**Invariants asserted in every cell:**
- Exactly the matching committed records are delivered: none extra, none missing, no duplicates, in offset order.
- `position()` is monotonic and eventually reaches the log end on a quiet filter.
- Unfiltered subscribers see byte-identical batches to today.

## 11. Benchmark gates

- **Per-record broker cost** with 1, 50 and 500 filtered subscribers on one partition, all with distinct `worldId` values. The cost must be flat across subscriber counts, within 10%. Target ≤ 5 ns per record for the index lookup on a hit, and for a miss.
- **Batch rebuild cost** at 1%, 10% and 50% selectivity, per matched byte.
- **Unfiltered path regression:** ≤ 2% versus the pre-2c baseline, and zero allocations in the steady state (an allocation gate).
- **MMORPG scenario,** over the docker cluster:
  - real client and brokers over Aeron;
  - 200 world servers, each filtering `worldId`;
  - 1M events/s spread over P partitions;
  - measure produce→deliver p50/p99 and network bytes per world server, compared with the same load unfiltered.
  - Gate: the latency class is held (p50 ≤ 50 µs, p99 ≤ 200 µs), and bytes per server are within 1.2× of the ideal (that world's events only).

## 12. Later: the same predicate for views

The compiled filter (term list, plus the per-version accessor table) is the **Filter** operator that the view-engine spec lists as a follow-on: a predicate that drops records before the fold. Phase 2c builds it as a standalone component owned by the push point, so a later view-source filter reuses it unchanged.

## 13. Decisions (user review, 2026-10-13)

1. **Position-note cadence on quiet filters:** keep the defaults: every 50 ms or every 64 Ki skipped records, whichever comes first. Both are configurable.
2. **Capacity caps:** preallocated from config.
   - The defaults are 4,096 filtered subscriptions per partition and 4,096 distinct index values per partition. That is ample for hundreds of world servers.
   - Exceeding a cap answers the Subscribe explicitly (§9).
3. **Consumer groups require one filter.**
   - Every member of a group must send byte-identical filter bytes; the canonical encoding (§5) makes equal filters identical.
   - The first member to join fixes the group's filter. The filter can change only once the group is empty.
   - A member whose filter differs is rejected at JoinGroup with `InvalidRequest` (39), detail "group filter mismatch: the group uses filter <hex>". It is never admitted.
