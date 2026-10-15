# Release note: Phase 2b Part C — typed topics route by key

Typed topics now route each record by its schema's key field. Before this
change every typed record went to partition 0 with an empty key. The changes
below affect upgrades.

## 1. An empty key now maps to partition 0

`rune::protocol::partitionForKey` now maps an empty key to partition 0 on every
topic. It used to map it to `fnv1a64("") % n`. The broker uses this function
for subscribe, produce without an explicit partition and GetViewRow. The client
uses it for `Key::partition`. On a topic with one partition nothing changes.

**Upgrade brokers and clients together.** Until both sides run the new code,
they put `Key{}` on different partitions. See section 4.

## 2. Typed data written before the upgrade is not moved

Typed records written before the upgrade sit on partition 0 with an empty key.
After the upgrade, new records for the same entity go to the partition their
key hashes to. On a topic with more than one partition:

- **Ordering per key** is split at the upgrade point. Older records for a key
  are on partition 0 and newer ones are on the hash partition.
- **Compaction** does not merge the old and new records for a key. The old
  records have an empty key, so they never share a compaction key with the new
  ones.
- **`subscribe(handle, Key(k))`** reads only the hash partition. It does not
  return records written before the upgrade.

Historical data is not rewritten or moved. If you need one partition per key
across the upgrade, re-produce the history after upgrading.

## 3. Schema lookups use the topic's defined name

GetTopicSchema, AlterTopic and DefineTopic's TopicExists check now find a topic
by the name it was given in DefineTopic. Before, they used the name inside its
schema descriptor. A client that looked up a topic by its descriptor name, when
that name differs from the topic name, now gets `UnknownTopic`. Use the topic
name instead.

Records written before the topic-name field existed have no topic name. They
are still found by their descriptor name.

## 4. Mixed versions disagree on `Key{}`

| Writer          | Reader / router | `Key{}` on an n-partition topic   |
|-----------------|-----------------|-----------------------------------|
| new client      | new broker      | partition 0 on both sides         |
| old client      | new broker      | client: `fnv1a64("") % n`; broker: 0 |
| new client      | old broker      | client: 0; broker: `fnv1a64("") % n` |

In the mixed rows, a record sent with `Key{}` and a `subscribe(handle, Key{})`
can resolve to different partitions. Keys that are not empty are not affected.

## 5. GetTopicSchema reports the partition count

`TopicSchemaResponse` now carries `partitionCount` after the descriptor blob.
The header version and block length are unchanged.

- Old clients stop reading after the blob and ignore the new field.
- New clients connected to an old broker get no count. They treat the topic as
  having one partition and send every typed record to partition 0, which is
  the old behaviour.

The reported count is the topic's real partition count. If DefineTopic is
called on a topic that already exists without a schema, such as a
boot topic (at the time of this release, boot topics were declared with an
environment variable, since removed - see SELF-HOSTING.md, "Topics are created
by DDL"), the schema is registered with that topic's existing
partition count. The count in the request is ignored, and the topic is not
resized.

## Related changes in this release

- CREATE VIEW now fails with `TopicExists` when the view name is already used
  by a topic, typed or untyped. Before, the existing topic became the view's
  sink.
- The code generator now rejects a schema that has more than one key field.
