# Release note - followers catch up across compacted ranges (#789)

**What changes.** Replication schema v5 adds `FetchResponse.fromOffset`, the offset the leader read the records from.
A disk-backed follower whose log end falls inside a range the leader's cleaner removed now appends the leader's next
surviving batch, as long as that read started at or below its log end. Before, it refused the batch as a gap and
stopped catching up, and the leader's high watermark stalled behind it. In-memory (transient) followers still refuse any
gap.

**Frame limits shrink by 8 bytes - in every mode.** The FetchResponse framing grows from 46 to 54 bytes, so the
FetchResponse records ceiling (applied single-node too) and therefore `produceIntakeLimit()`, the largest produce batch
a broker accepts, are **at most 8 bytes smaller** than before (with the default 1 MiB term the records ceiling goes from
130994 to 130986 bytes). A client that produced a batch within 8 bytes of the old limit now gets `MessageTooLarge`.

**Mixed versions.** A v4 broker's FetchResponse decodes with no read origin, so a v5 follower applies it contiguously
as before. The minimum supported replication version is unchanged.
