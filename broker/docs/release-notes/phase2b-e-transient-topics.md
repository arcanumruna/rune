# Release note: Phase 2b Part E — transient topics

Memory topics are renamed transient topics, and a transient topic now keeps
its metadata across restarts. The changes below affect upgrades.

Status: in this release a transient topic can be defined, survives restart as
an empty topic, and replicates. Produce/subscribe on transient topics is
completed by the transient data-path work (Part E2).

## 1. Memory is now transient

| Old name                          | New name                            | Old name still works? |
|-----------------------------------|-------------------------------------|-----------------------|
| `persistence=memory` (`RUNE_TOPICS`) | `persistence=transient`          | Yes, one warning per entry |
| `RUNE_MEMORY_LOG_RING_BYTES`      | `RUNE_TRANSIENT_LOG_RING_BYTES`     | Yes, one warning      |

`RUNE_TRANSIENT_LOG_RING_BYTES` must be a power of two. The default is 8 MiB.
If both ring-size variables are set, the canonical name wins and the alias
still logs its warning. Rune QL uses `transient topic …;`. The client uses
`Persistence::Transient` on `defineTopic`.

## 2. DefineTopic carries a persistence byte

DefineTopic now carries a persistence byte. A client that does not send it is
treated as defining a disk topic.

## 3. TopicSchema trailer

Disk records are byte-identical to before. A transient record adds one byte
after the topic-name trailer.

An older broker that reads a catalog containing such a record rejects it:
its `TopicSchema::deserialize` accepts only the exact name bytes after the
name length, so the extra byte makes the record malformed.

- At boot, runtime topic recovery throws and the node refuses to boot on a
  damaged topic catalog.
- On a leader rebuild of the schema registry, the broker logs a critical
  error and aborts the rebuild.

Do not run an older broker against a data dir or cluster that holds a
transient topic.

## 4. Restart semantics

- Metadata survives. A runtime-defined transient topic is recreated as
  transient on boot.
- Data does not survive. A restarted node rejoins with the topic empty. After
  a full-cluster restart the topic is empty and offsets restart from 0.
- Persistence is fixed at definition. `alter topic` carries it forward and
  cannot change it.

## 5. A view over a transient topic must be transient

A persisted view over a transient source (`Client::createView(..., persistence)` / the
`CreateView` persistence byte) is rejected with error 37 (`ViewSourceTransient`). Use a
transient view. Rune QL `create view` arrives later.

`alter topic` naming a view is rejected with error 38 (`ViewNotAlterable`).

Changing a `RUNE_TOPICS` topic's `persistence=` between restarts is unsupported: the schema and
view checks keep the mode recorded when the topic was defined.

## 6. Upgrade order

**Upgrade brokers before clients.** A new client that defines a transient
topic against an older broker silently gets a disk topic, because the older
broker ignores the persistence byte.
