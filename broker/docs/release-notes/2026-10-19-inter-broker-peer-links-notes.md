# Release note - inter-broker authentication and encryption (peer links)

**What changes.** With `RUNE_AUTH` enabled and more than one broker, brokers prove possession of the cluster key to each
other (`RUNE_CLUSTER_KEY_FILE`, optionally `RUNE_CLUSTER_KEY_FILE_NEXT` for a rolling rotation) on the peer-auth stream
(`RUNE_PEER_AUTH_STREAM_ID`) before any consensus, replication or admin-plane frame is accepted, and every inter-broker
frame is then sealed with AES-256-GCM (consensus, controller / replication, every worker's replication plane, the
admin plane carrying `__catalog` and `__security`). A broker without the right key cannot join: its frames are never
sent (no link) and nobody opens them. Refusals are logged on both sides (`peer link from node N REFUSED: ...` /
`peer link to node N REFUSED by N: ...`) and counted (`1'000'234`); established links are counter `1'000'233`;
dropped inter-broker frames are counter `1'000'235`.

**Frame limits shrink by 32 bytes - in every mode.** A sealed frame carries a 16-byte header and a 16-byte tag, so every
inter-broker frame limit derived from an Aeron maximum message length (`ReplicationFrameLimits.h`: the FetchResponse
records ceiling through the outbound ring and through the replication publication) now subtracts those 32 bytes
whether or not the broker seals, so limits never depend on the mode. `produceIntakeLimit()` - and therefore the largest
produce batch a broker accepts - is **at most 32 bytes smaller** than before (with the default 1 MiB term:
the records ceiling of one FetchResponse goes from 131026 to 130994 bytes). A client that produced a batch within 32 bytes of the old
limit gets `MessageTooLarge` now; every other client is unaffected.

**Unchanged.** `InProcessUnauthenticated` and `DevDisabled` (single node only) build every transport `PeerPlain`: the
bytes on the wire are exactly what they were. Mixing sealed and unsealed brokers in one cluster is not supported (the
authentication mode is cluster-wide configuration).
