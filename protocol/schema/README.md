# SBE schemas (rune-protocol)

`rune-internal-ring.xml` — SBE message schema for Rune's **internal, process-local**
ring-buffer messages (`WorkerOp`, `WorkerReturn`, `FlushRequest`, `FlushCompletion`).
These travel only between threads in one process over the SPSC/MPSC byte rings; some
fields (`replica`, `logPtr`) are raw in-process pointers carried as opaque `uint64`.
They MUST NOT cross a real wire or be persisted. SBE is used for the wrap-in-place
flyweight codec (encode directly into the claimed ring slot; zero-copy decode) and
offset-safety, not for cross-process schema evolution. Distinct `schemaId=7` from the
client-facing wire schema (`schemaId=120`).

## Regenerating the C++ flyweights

Generated headers live under `include/rune_protocol_internal_sbe/` (namespace
`rune::protocol::internal::sbe`) and are committed. To regenerate after editing the
schema:

```sh
JAVA=/Applications/CLion.app/Contents/jbr/Contents/Home/bin/java   # or any JDK 17+
"$JAVA" -Dsbe.target.language=cpp -Dsbe.output.dir=include \
    -jar tools/sbe-all-1.40.2.jar schema/rune-internal-ring.xml
```

The `sbe-all` fat jar (real-logic Simple Binary Encoding, `uk.co.real-logic:sbe-all`,
now the aeron-io project) is vendored under `tools/` for reproducible, offline codegen.

## `rune-replication.xml` (schemaId 112) - version 4 (ISR protocol v3)

Version 4 is additive: every new field is `sinceVersion="4"`, an old decoder reads the fields it knows, and a new decoder
treats an absent field as 0 (or -1 where noted). The wire minimum stays 3.

| Message | New in v4 |
|---|---|
| `FetchRequest` (hand-rolled codec, block 44 -> 60) | `storageId int64` (the follower's storage identity), `lastEpochStartOffset int64` (where the follower's copy of `lastFetchedEpoch` starts; -1 = not held / unknown, never compared) |
| `FetchResponse` | error code `-11` `kErrEpochHistoryMismatch`; payload `{int32 epoch, int64 leaderStartOffset}` |
| `AlterIsr` (block 92 -> 232) | `proposalId int64` (correlation only, never part of the compare-and-set), `isrStorage int64[16]` (the storage id of each proposed member), `sendSeq int32` |
| `AlterIsrResponse` (block 26 -> 46) | `proposalId int64`, `controllerEpoch int64`, `sendSeq int32` (echo / term of the answering controller; 0 = a v3 peer) |

A below-committed report is an `AlterIsr` with `isrCount = kLeaderEpochBump`, `isr[1] = 2048`
(`kBumpLeaderBelowCommitted`), `isr[2]` the floor (clamped to int32; informational), `isr[3]`/`isr[4]` the reporter's process
incarnation (low/high 32 bits) and `proposalId` the report id; an old controller answers it Fenced.
The node-control register carries the storage id in `isr[7]`/`isr[8]` and its origin in `isr[9]`. The broker's
hand-rolled codecs (`ReplicationMessages.h`) are the reference for block clamping.
