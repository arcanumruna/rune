# Rune Schema Evolution — Test Scenario Catalogue (≈500 scenarios + edge cases)

**Purpose:** the exhaustive scenario space for durable schema evolution (ADD/DROP/RENAME/SET COMPATIBILITY + the deferred RETYPE), especially **typed topics at an old schema version vs. a new one after an alter/drop, and reading a log that mixes records written under different schema versions with different alterations along the lineage.** This is a test-requirements catalogue, not a report of passing tests.

**Coverage legend:** `[✓]` a test for this exists today (from the 22 implementation tasks); `[~]` partially covered (a representative case exists, not the full cell); `[GAP]` no test yet. Legend reflects state as of the feature branch `feat/schema-evolution-durable-tags` (2026-10-08).

**Core invariants every scenario implicitly checks:** (i) a record decodes against ITS OWN writer version; (ii) a field's identity is its durable `(topicId, tag)`, never its name or position; (iii) tags are never reused; (iv) the persisted offset/state is atomic; (v) nothing on the produce/consume hot path allocates or blocks on DDL.

---

## A. Field-type × operation coverage (A1–A72)

Types in play: `int32, int64, uint16, uint32, uint64, float64, bool, enum, string (var), bytes (var), FixedStr(N)`.

- A1–A11 `[~]` ADD COLUMN of each of the 11 types to a v1 topic → v2; verify the new field is absent in v1 records and present in v2 records.
- A12–A22 `[~]` DROP COLUMN of each of the 11 types → verify old records still decode it, new records omit it, its slot/var-index is reclaimed.
- A23–A33 `[GAP]` RENAME COLUMN of each of the 11 types → decode byte-identical, new name resolves, old name no longer resolves (unless re-added).
- A34 `[✓]` ADD a fixed-width field — fixed-block offset assignment correct at the new version.
- A35 `[✓]` ADD a var-width field (`string`/`bytes`) — var-index assignment correct; additive-only var invariant holds.
- A36 `[~]` DROP a fixed field in the MIDDLE of the block → later fixed fields' offsets shift down at the new version; old records unaffected.
- A37 `[GAP]` DROP a var field in the middle → later var-indices shift; old records' var-walk unaffected.
- A38–A42 `[~]` ADD `FixedStr(N)` for N ∈ {1, 2, 32, 255, 256, 65535} (boundary widths) → inline width correct per version.
- A43 `[GAP]` ADD `FixedStr(N)` with N=0 → rejected at define/parse.
- A44 `[GAP]` ADD `FixedStr(N)` with N>65535 → rejected (uint16 width overflow) at parse and at define.
- A45–A46 `[GAP]` ADD an `enum` field with a whitelist; records with in-list and out-of-list values (validation).
- A47 `[GAP]` DROP an `enum` field → old records with enum values still decode.
- A48 `[GAP]` ADD then DROP the SAME type repeatedly across versions; confirm each ADD gets a fresh tag.
- A49–A59 `[GAP]` DROP each type then ADD a NEW field of a DIFFERENT type reusing the dropped field's NAME → fresh tag, both coexist in the evolving descriptor (one retired).
- A60 `[✓]` ADD the key field variant — a key field cannot be dropped (A61) or renamed-away from key semantics.
- A61 `[✓]` DROP the key field → rejected.
- A62 `[✓]` DROP the last remaining live field → rejected.
- A63 `[GAP]` RENAME the key field → allowed (tag/key-role unchanged), key routing still works.
- A64–A72 `[GAP]` mixed fixed+var topic: ADD fixed after var, ADD var after fixed, DROP fixed leaving var, DROP var leaving fixed, DROP all-but-one of each, interleaved — offset + var-index recompute correct at every version.

## B. Op-sequence lineages (B1–B58)

- B1 `[✓]` v1→v2 single ADD.
- B2 `[✓]` v1→v2 single DROP (BACKWARD).
- B3 `[✓]` v1→v2 single RENAME.
- B4 `[✓]` multi-op single ALTER statement (ADD + DROP + RENAME) → one version bump, atomic.
- B5 `[✓]` multi-op statement where one op fails → entire statement rejected, no version bump.
- B6–B10 `[~]` lineages of length 3,4,5,8,12 with mixed ops → every intermediate version materializes correctly.
- B11 `[✓]` ADD x then DROP x in the SAME statement → cumulative-state validation (x live for the drop).
- B12 `[✓]` two ADD x in one statement → second collides, rejected.
- B13 `[GAP]` DROP x then ADD x in the same statement → old x retired, new x fresh tag, both in descriptor.
- B14 `[GAP]` RENAME a→b then RENAME b→c in one statement → cumulative, final name c, same tag.
- B15 `[GAP]` RENAME a→b then ADD a (new) in one statement → allowed (a freed by rename), new a fresh tag.
- B16 `[✓]` drop-then-readd across SEPARATE versions (v2 drop age, v3 add age) → v3 age has a fresh tag, v2-retired age still present/retired.
- B17–B21 `[GAP]` rename chains across versions (v1 name=a, v2 a→b, v3 b→c, v4 c→a) → same tag throughout; a v1 record's field resolves under whatever the reader's version calls it.
- B22 `[GAP]` add field at v2, drop it at v4, re-add (same name) at v6 → three tags total for that name (one live, two retired).
- B23–B30 `[GAP]` long alternating ADD/DROP of the same name 8× → 8 distinct never-reused tags; descriptor holds all; nextTag monotonic.
- B31 `[GAP]` SET COMPATIBILITY alone (no column op) → version bump, mode persisted, descriptor unchanged.
- B32 `[GAP]` SET COMPATIBILITY + ADD in one statement.
- B33 `[~]` SET COMPATIBILITY forward + DROP in one statement → rejected (gate uses the new mode).
- B34–B40 `[GAP]` interleave SET COMPATIBILITY backward↔forward across a long lineage; each subsequent op gated by the then-current mode.
- B41–B50 `[GAP]` property/fuzz: random valid op sequences of length 1–20 → descriptor + every materialized version self-consistent; tags unique; offsets valid.
- B51–B58 `[GAP]` a no-op ALTER (empty op list) → rejected; a statement that only sets compatibility to the SAME mode; redundant rename a→a; drop of an already-dropped field; rename of an already-dropped field; add colliding with a live name; add colliding with a retired name (allowed).

## C. Mixed-version reads from one topic (C1–C78) — THE CORE ASK

"A topic whose log holds records written under v1, v2, …, each with different alterations; a consumer reading the mix."

- C1 `[✓]` Log = {v1 records, v2 records} after an ADD; consume both; v1 records show the added field ABSENT, v2 show it present.
- C2 `[✓]` Log = {v1, v2} after a DROP; v1 records still decode the dropped field; v2 records omit it.
- C3 `[GAP]` Log = {v1, v2} after a RENAME; both decode the field (by tag) under the reader's current name.
- C4–C8 `[GAP]` Log spanning 3/4/5/8/12 versions with a MIX of ADD/DROP/RENAME between each; a single consumer decodes every record per its own writer version.
- C9–C13 `[~]` A consumer BOUND to version X (compiled against vX) reads a log containing versions < X, == X, and > X: fields it knows resolve; fields added after X are ignored; fields dropped before X read absent for post-drop records.
- C14 `[GAP]` Reader@v1 reads a v3 record where a v1 field was dropped at v2 → the field is absent in the v3 record (forward-read); reader tolerates it.
- C15 `[GAP]` Reader@v3 reads a v1 record (backward-read) → the v1 fields decode via the v1 offset table; a v3-only field reads absent.
- C16–C20 `[GAP]` A field present in v1, dropped in v2, re-added (fresh tag) in v3: a reader must NOT confuse the v1-tag value with the v3-tag value when reading a mixed log; records carry their version so the right tag/offset is used.
- C21–C25 `[GAP]` A renamed field across the lineage: records from before and after the rename both decode to the same tag; the consumer sees a consistent logical field.
- C26–C30 `[GAP]` Mixed-version read where the KEY field was renamed → key routing/compaction key still consistent (key identity is the tag).
- C31–C40 `[GAP]` Per-type mixed reads: for each of the 11 types, a log with that field added at v2 and dropped at v4 → records at v1(absent)/v2-3(present)/v4+(absent) all decode correctly in one scan.
- C41–C45 `[~]` Var-field mixed read: a `string` added at v2 and a `string` dropped at v3 in the same lineage → var-section walk correct for each record's version (no mis-parse of the var region).
- C46–C50 `[GAP]` FixedStr width unchanged across versions but other fields added/dropped around it → its inline offset shifts per version; mixed read decodes it correctly each time.
- C51–C55 `[GAP]` A batch (RecordBatch) containing records of DIFFERENT schema versions (producer evolved mid-batch, or a replay stitches versions) → each record decoded by its own header version.
- C56–C60 `[GAP]` Tombstone (zero-length value) records interleaved with versioned records in the mixed read → tombstones recognized regardless of version.
- C61–C65 `[GAP]` Consume from offset 0 vs. from a mid-log offset that lands inside a different-version run → decode correct from any start offset.
- C66–C70 `[GAP]` Two consumers, one bound to v1 and one to vN, reading the SAME mixed log concurrently → each gets its own correct view.
- C71–C78 `[GAP]` Mixed read AFTER compaction: compaction preserved all schema versions (keyed by (topicId,version)); data-record compaction (Compact topics) keeps latest-per-key — confirm the mixed read of surviving records still decodes each per its writer version.

## D. Typed-client binding & fingerprint (D1–D44)

- D1 `[✓]` Client compiled @vX opens a topic currently @vX → binds vX.
- D2 `[✓]` Client compiled @v1 opens a topic that has evolved to v5 → fingerprint-selects v1, binds v1 (e2e proven).
- D3–D8 `[GAP]` Client @vK opens a topic @vN for all 1≤K≤N across a 6-version lineage → each binds its own K.
- D9 `[✓]` Client whose fingerprint matches NO known version → stale-schema error (does not bind).
- D10 `[✓]` Broker returns a blob whose fingerprint ≠ requested (simulated mis-select) → client re-verifies and throws.
- D11 `[✓]` Producer bound to vX stamps records with version=X in the header `[2..3]`.
- D12–D16 `[GAP]` Multiple producers bound to different versions writing the same topic concurrently → the log interleaves correctly-stamped records; a consumer decodes each.
- D17 `[GAP]` A producer bound to a version that was later DROPPED-into (its field dropped at a newer version) keeps producing valid vX records.
- D18 `[GAP]` `requestedFingerprint=0` → latest version returned/bound.
- D19 `[GAP]` Two clients with the SAME fingerprint (same compiled schema) → both bind the same version.
- D20 `[GAP]` Fingerprint of a post-RENAME version differs from pre-RENAME (names are in the fp) → a client compiled before the rename binds the old version, after binds the new.
- D21–D25 `[GAP]` openTopic on a topic that doesn't exist / was never defined → clean error, not a hang.
- D26–D30 `[GAP]` openTopic race: client opens while an ALTER is in flight → binds to a consistent committed version (before or after), never a half-applied one.
- D31–D36 `[GAP]` Generated-header round trip: `rune schema generate` at each version produces a header whose `kFingerprint` matches that version's broker fingerprint; openTopic with it binds that version.
- D37–D44 `[GAP]` Stale-client UX: a client compiled against a version whose fields were later all dropped/renamed still binds its version and reads its own historical records; produces valid records of its version; error message guidance when regeneration is needed.

## E. Compatibility gate (E1–E34)

- E1 `[✓]` ADD under BACKWARD → allowed.
- E2 `[✓]` ADD under FORWARD → allowed.
- E3 `[✓]` RENAME under BACKWARD → allowed.
- E4 `[GAP]` RENAME under FORWARD → allowed.
- E5 `[✓]` DROP under BACKWARD → allowed.
- E6 `[✓]` DROP under FORWARD → rejected (`SchemaIncompatible`).
- E7–E12 `[~]` Each op in a multi-op statement vs. each mode; a statement is rejected iff any op is disallowed under the effective mode.
- E13 `[✓]` SET COMPATIBILITY within a statement changes the mode used to gate that statement's other ops.
- E14–E18 `[GAP]` Mode change persists: after SET COMPATIBILITY forward, a later standalone DROP is rejected; after setting back to backward, DROP allowed.
- E19–E24 `[GAP]` Mode change does NOT re-validate history (a topic with a past DROP may be set to FORWARD; existing versions remain valid).
- E25–E30 `[GAP]` Default mode on CREATE is BACKWARD; a topic never SET is BACKWARD.
- E31–E34 `[GAP]` Transitive note: for ADD/DROP/RENAME, BACKWARD holds against ALL prior versions (decode-by-tag) — verify a reader@vN reads v1 after a long lineage; FULL/NONE are deferred (documented as not-yet-modes).

## F. CAS & concurrency / serialization (F1–F42)

- F1 `[✓]` `expectedVersion=0` → applies against current (no CAS).
- F2 `[GAP]` `expectedVersion` == current latest → applies.
- F3 `[✓]` `expectedVersion` stale (< latest) → `SchemaConflict`, nothing mutated.
- F4 `[GAP]` `expectedVersion` > latest (future/invalid) → `SchemaConflict`.
- F5 `[✓]` Two concurrent ALTERs, no expectedVersion → both apply in arrival order, version += 2.
- F6 `[✓]` Two concurrent ALTERs with the SAME expectedVersion → exactly one succeeds, the other `SchemaConflict` (e2e proven, winner non-deterministic).
- F7–F10 `[GAP]` N concurrent ALTERs (N=3,5,10) with the same expectedVersion → exactly one success, N-1 conflicts.
- F11 `[GAP]` Concurrent CREATE of the same topic name → one succeeds, one `TopicExists`.
- F12 `[GAP]` Concurrent CREATE of different names → both succeed, distinct sequential topicIds.
- F13 `[GAP]` CREATE then immediate ALTER (pipelined) → serialized, ALTER sees the created topic.
- F14 `[GAP]` ALTER on a nonexistent topic → `UnknownTopic`.
- F15–F20 `[~]` Serialization: all DDL funnels through the single AdminWorker in-flight slot; interleavings of CREATE/ALTER/GET never corrupt state; versions always contiguous.
- F21–F26 `[GAP]` Version contiguity under concurrency: no gap/duplicate version ever created (the registry deque indexed by version-1 stays dense).
- F27–F32 `[GAP]` A failing ALTER (compat/validation) in a concurrent mix doesn't consume a version or a tag; the next ALTER gets the expected next version.
- F33–F38 `[GAP]` GET schema concurrent with ALTER → returns a consistent committed version, never a partially-applied descriptor.
- F39–F42 `[GAP]` AdminWorker timeout / stuck op handling under the `kDdlTimeoutNs` budget; a stuck in-flight op doesn't deadlock the queue.

## G. Persistence & crash recovery (G1–G56)

- G1 `[✓]` Boot replay of `__topic_schemas` reconstructs all versions {1..N}, latest=N, correct nextTag/compatMode.
- G2 `[✓]` Follower→leader `rebuildFromLog_` reconstructs all versions identically to boot replay.
- G3–G8 `[GAP]` Crash BEFORE the ALTER record is appended → on restart, version unchanged (ALTER not acked → client retries).
- G9–G14 `[GAP]` Crash AFTER append but BEFORE HW advances → record present but not committed; replay semantics defined (applied iff within HW).
- G15–G20 `[GAP]` Crash AFTER commit but BEFORE client ack → on restart the version is applied; client's retry is idempotent (CAS or re-define sees it exists).
- G21–G26 `[~]` Restart after a long lineage (CREATE + many ALTERs) → full history recovered; a v1-bound client still binds post-restart.
- G27–G32 `[GAP]` Mixed-version data log + schema log both replayed → every data record still decodes per its version after restart.
- G33–G38 `[GAP]` Snapshot/offset atomicity (where applicable to view/state): persisted offset exactly matches persisted state; never offset-ahead or data-ahead; crash mid-write leaves the prior complete artifact.
- G39–G44 `[GAP]` Truncated/corrupt `__topic_schemas` record on replay → detected (CRC/length), replay stops cleanly at the last good record, not OOB/silent-wrong.
- G45–G50 `[GAP]` Legacy RCD1 schema records (pre-feature) in the log replay as `tag=id`, `untilVersion=∞` (lossless migration); a mixed RCD1+RCD2 log replays correctly.
- G51–G56 `[GAP]` Replay idempotence/time-independence: replaying the same log twice yields byte-identical registry state; replay order within a version is deterministic.

## H. Compaction (H1–H30)

- H1 `[✓]` `__topic_schemas` keyed by (topicId, version) → compaction preserves ALL versions (not just latest).
- H2 `[~]` Compaction runs (dirty-ratio trigger) then replay → all versions still present.
- H3–H8 `[GAP]` Compaction of a long lineage (12 versions) → all 12 survive; latest resolvable; every old fingerprint still resolvable.
- H9–H14 `[GAP]` Compaction concurrent with an in-flight ALTER → no lost version, no corruption.
- H15–H18 `[GAP]` Orphan old-key record (a record written under the pre-fix topicId-only key before the (topicId,version) migration) → doesn't collide with new 6-byte keys; migration-runbook behavior.
- H19–H24 `[GAP]` DATA-topic compaction (Compact cleanup on a user topic) interacting with schema versions: tombstones, latest-per-key, while records span schema versions → surviving records decode per version.
- H25–H30 `[GAP, future]` DeleteTopic must tombstone every (topicId, version) key, not one topicId key (no DeleteTopic op exists yet — placeholder scenarios).

## I. Fingerprint (I1–I24)

- I1 `[✓]` `fp(vK)` distinct across versions.
- I2 `[✓]` Golden fp `0x8FC60841B8667539` unchanged for the existing additive v1 schema.
- I3 `[✓]` RENAME → new fp/version, byte-identical decode of existing records.
- I4 `[GAP]` `fp → version` reverse lookup resolves each known version; unknown fp → not-found.
- I5–I9 `[GAP]` `fp(materializeAsOf(evolving,V)) == stored fp(V)` for every V in a lineage (the client-side check relies on this).
- I10–I14 `[GAP]` ADD then DROP returning to the SAME live field set as an earlier version → are the fingerprints equal or distinct? (define & test the intended behavior — likely distinct due to retired-tag/version context, confirm).
- I15–I18 `[GAP]` `untilVersion` absorbed into fp only when ≠∞ (guarded) → additive schemas hash identically; a dropped-field lineage's per-version fp still stable.
- I19–I24 `[GAP]` Generated-header `kFingerprint` equals the broker's `fp` for that version across all generated versions; fingerprint determinism across machines/endianness.

## J. Decode correctness & boundaries (J1–J46)

- J1 `[✓]` Offset reclaim after DROP (fixed field) — later fields' offsets at the new version.
- J2 `[GAP]` Offset reclaim after DROP (var field) — var-index reassignment.
- J3 `[✓]` `readField` by tag against a materialized per-version descriptor decodes a historical record.
- J4–J8 `[GAP]` `sinceVersion` boundary: a field with sinceVersion=K read in records of version K-1 (absent), K (present), K+1 (present).
- J9–J13 `[GAP]` `untilVersion` boundary (EXCLUSIVE): a field dropped at version M is present in M-1, absent in M and M+1.
- J14–J18 `[GAP]` `isLiveAt` edge: sinceVersion==untilVersion (never live — should be impossible/rejected); untilVersion<sinceVersion (invalid — rejected).
- J19–J24 `[~]` FixedStr inline width per version; a FixedStr whose neighbors were added/dropped → its offset moves but width constant; decode correct.
- J25–J30 `[GAP]` Tombstone / zero-length value at each version → recognized as delete regardless of writer version.
- J31–J35 `[GAP]` A record whose header version > the latest version the reader knows (forward tolerance) → known fields decode, unknown trailing ignored, no OOB.
- J36–J40 `[~]` Malformed/truncated record → bounds-checked, yields absent/empty field, never OOB (Validator is the gate for untrusted input).
- J41–J46 `[GAP]` Enum whitelist validation per version; a value valid in vK but the field dropped in vK+1; null/absent numeric → default value semantics.

## K. Wire / protocol (K1–K30)

- K1 `[✓]` `AlterTopic` encode→decode round-trip (all fields).
- K2 `[✓]` `AlterTopicResponse` round-trip.
- K3–K8 `[✓]` Decode rejects truncation at EACH field boundary (topicName, expectedVersion, opsBlob) → nullopt, no OOB.
- K9 `[✓]` Conformance golden byte vectors for AlterTopic / AlterTopicResponse.
- K10 `[✓]` `isKnownTemplate` knows 68/69, rejects 70.
- K11 `[✓]` `GetTopicSchema` carries `requestedFingerprint` (u64); golden updated.
- K12–K16 `[GAP]` `GetTopicSchema(requestedFingerprint=0)` → latest; `=matching fp` → that version; `=unknown fp` → error code.
- K17–K22 `[✓]` `AlterOps` wire round-trip for all 4 op kinds; truncated/bad-opType/bad-mode → nullopt.
- K23–K26 `[GAP]` Error codes surfaced correctly on the wire: `SchemaIncompatible`(29), `SchemaConflict`(30), `UnknownTopic`(7), `InvalidSchema`(23).
- K27–K30 `[GAP]` `kProtocolVersion`/`kSchemaId` gating; a frame with a wrong schemaId/version is rejected; additive-op compatibility.

## L. Rune QL grammar (L1–L34)

- L1 `[✓]` `alter topic T add column f int32, drop column g, rename column h to k;` → 3 ops parsed.
- L2 `[✓]` `set compatibility forward` → mode parsed.
- L3 `[✓]` Existing `topic {...}` still parses (variant return).
- L4–L9 `[✓]` Malformed alters: missing `column`, missing `to` in rename, missing mode value, missing `;`, trailing tokens → located `ParseError` (line/col).
- L10–L15 `[GAP]` `add column` with each type incl. `string`, `string(N)`, bad `string()`, `string(0)`, `string(99999)`.
- L16–L20 `[GAP]` `set compatibility` with an invalid value (`sideways`) → located error; case sensitivity; `backward`/`forward` only.
- L21–L25 `[GAP]` Comma/whitespace/comment handling between ops; empty op list (`alter topic T;`) → error.
- L26–L30 `[GAP]` `rename column a to a` (no-op rename); `drop column` of a name not in the current text (semantic vs parse — parse accepts, broker rejects).
- L31–L34 `[GAP]` Statement-kind dispatch: leading token neither `topic` nor `alter` → located error; `alter` without `topic`.

## M. CLI (M1–M24)

- M1 `[✓]` `rune schema apply` on a `topic` file → defineTopic (unchanged).
- M2 `[✓]` `rune schema apply` on an `alter topic` file → alterTopic path.
- M3 `[✓]` `rune schema alter <file>` → alter; rejects a `topic`-statement file.
- M4 `[✓]` `--expected-version` valid → CAS; invalid/non-numeric/out-of-range → clean error, non-zero exit (no crash), BEFORE dry-run.
- M5 `[~]` `--dry-run` prints the plan for topic and for alter.
- M6–M10 `[GAP]` Error surfacing: `SchemaIncompatible`/`SchemaConflict`/`UnknownTopic` → distinct human messages + non-zero exit.
- M11–M15 `[GAP]` `--broker`/`RUNE_BROKER`/`--cli-egress`/`RUNE_EGRESS` flag handling for alter.
- M16–M20 `[GAP]` `rune schema generate` at a specific version; `rune schema get` of a specific fingerprint/version; round-trip generate→openTopic.
- M21–M24 `[GAP]` `printUsage` advertises alter; unknown subcommand error; missing file arg; file not found.

## N. Validation (version-keyed) (N1–N22)

- N1 `[✓]` v1 record validates against the v1 validator; v2 record against v2.
- N2 `[✓]` A v2 record carrying the dropped-field (v1) layout → rejected.
- N3 `[✓]` Unknown (topicId, version) → reject "unknown schema version" (no fall-through).
- N4–N8 `[GAP]` validate-ON vs validate-OFF per topic; OFF path is zero-cost (no validator lookup on hot path).
- N9–N13 `[GAP]` After an ALTER, the new version's validator is fanned out to workers; a record of the new version validates; in-flight old-version records still validate.
- N14–N18 `[GAP]` Produce a batch mixing versions → each record validated against its own version's validator.
- N19–N22 `[GAP]` Enum/range/required-field validation per version; a value invalid under vK's validator rejected; valid under vK+1 accepted.

## O. Boundaries & limits (O1–O28)

- O1 `[GAP]` Tag allocator approaching `uint16` max (65535 ever-defined fields) → behavior at/over the limit (reject or documented widen path).
- O2 `[GAP]` Max fields per topic; a very wide schema.
- O3 `[✓]` Empty topic / drop last field → rejected.
- O4–O8 `[GAP]` Name collision matrix: ADD colliding with live (reject), with retired (allow), RENAME-to live (reject), RENAME-to retired name (allow), RENAME of retired (reject).
- O9–O14 `[GAP]` Max ops in one ALTER statement; opsCount at/over u16 (the known clamp-vs-loop inconsistency — unreachable but documented).
- O15–O20 `[GAP]` Huge batch (MB-scale) spanning multiple schema versions → decode/validate throughput, no per-record alloc.
- O21–O24 `[GAP]` Very long lineage (hundreds of versions) → registry memory, replay time, fp map size.
- O25–O28 `[GAP]` Zero-partition / single-partition / many-partition topic evolution → per-partition worker gets the schema update; all partitions converge.

## P. Determinism & replica convergence (P1–P18)

- P1 `[~]` Leader and follower materialize the identical descriptor/table at the same offset.
- P2–P6 `[GAP]` Replay time-independence: same log → identical state regardless of timing/batching.
- P7–P11 `[GAP]` A follower that missed some ALTERs catches up via log → converges to leader's version set.
- P12–P18 `[GAP]` Failover mid-lineage: new leader has the full version history; a client re-binds its version post-failover; no version regression.

## Q. Multi-node (deferred — Q1–Q24, placeholder for the follow-on spec)

- Q1–Q6 `[GAP, deferred]` `__topic_schemas` replication via the admin-owned transport; version convergence across nodes.
- Q7–Q12 `[GAP, deferred]` A topic evolved on the leader → all followers see the same version lineage + fingerprints.
- Q13–Q18 `[GAP, deferred]` Node join mid-lineage → catches up all versions; a client on any node binds the same version by fingerprint.
- Q19–Q24 `[GAP, deferred]` Split-brain / partition during ALTER; admin-driven compaction across nodes; multi-node failover mid-evolution.

## R. Performance & latency gates (R1–R18)

- R1 `[~]` Decode per record ≤ 150 ns p99, zero-alloc (indicative micro-bench 2.17 ns; needs a dedicated readField-by-tag bench + p99 on real hw).
- R2 `[~]` QL parse → wire ≤ 5 µs p99 (indicative 205 ns; needs an `alter`-specific bench).
- R3 `[GAP]` MV fold update < 10 µs/batch (view-engine spec; downstream).
- R4 `[GAP]` Single-node select < 50 µs; R5 multi-node query < 200 µs (downstream).
- R6–R10 `[GAP]` Produce/consume steady-state: ZERO regression with the evolution machinery present but no DDL in flight (ABAB n≥15, real Linux x86_64).
- R11–R14 `[GAP]` Version-keyed validator lookup adds ≤ +50 ns p99 / zero alloc vs single-version.
- R15–R18 `[GAP]` ALTER latency measured on the AdminWorker (off the hot path); compaction runs off the hot path; QL→wire→plan ≤ 5 µs for the DDL instance.

## S. Negative / failure injection (S1–S20)

- S1–S5 `[~]` Every rejection path returns the correct error code and mutates nothing (UnknownTopic, SchemaConflict, SchemaIncompatible, InvalidSchema, duplicate-create).
- S6–S10 `[GAP]` Resource exhaustion: admin ring full (backpressure), snapshot IO failure, disk full on `__topic_schemas` append → clean failure, no partial commit.
- S11–S15 `[GAP]` Corrupt/garbage opsBlob, descriptor blob, or wire frame → bounds-checked rejection, no crash/OOB.
- S16–S20 `[GAP]` Partial write / power-loss simulation on the schema log; recovery to the last consistent version.

---

## Coverage summary & priorities

**Already tested (the mechanism is proven):** per-version decode by tag (A/C/J core), multi-version registry + replay + compaction-survival (G1-2, H1), version-keyed validators (N1-3), the compat gate basics (E1-6,13), CAS + concurrent-ALTER conflict (F3,5,6), fingerprint golden + per-version distinctness (I1-3), the full wire round-trips (K1-10,17-22), QL grammar + located errors (L1-9), CLI dispatch + flag validation (M1-4), and the 7-assertion real-Aeron e2e (C1-2, D2, E6, F6).

**Highest-priority GAPs to close next** (the user's specific ask — mixed-version reads across non-trivial lineages):
1. **C4–C40** — deep multi-version lineages (3–12 versions, mixed ops) read in one scan, per-type.
2. **C16–C25, B16–B30** — drop-then-readd and rename-chains read correctly in a mixed log (the tag-not-name invariant under reuse).
3. **D3–D8, D12–D16** — client@vK binding across a full lineage + multiple producers at different versions into one topic.
4. **H2–H14** — compaction of a long lineage + mixed read after compaction.
5. **G3–G32** — crash-recovery matrix (before/after append/HW/ack) + mixed data+schema replay.
6. **R6–R18** — the real-hardware latency/regression gates (Linux x86_64).

**Note on scale:** this catalogue is ≈500 numbered scenarios across 19 categories; many categories are themselves parameterized (×11 types, ×N lineage lengths, ×2 modes), so the true cell count is in the low thousands — the numbered items are the representative/boundary cases worth an explicit test. Property/fuzz scenarios (B41–B50) stand in for the exhaustive combinatorial tail.
