# Schema-Evolution Test Gap-Closure — Implementation Plan

> Execute subagent-driven. Fresh implementer per task + review. Tests only (no production-logic changes except test-infra/docker wiring where noted). All work on the already-merged `feat/schema-evolution-durable-tags` worktrees under `~/Projects/se-worktrees/`.

**Goal:** implement the highest-value `[GAP]` scenarios from `docs/superpowers/specs/2026-10-08-rune-schema-evolution-test-matrix.md` — especially **reading a log that mixes records written under different schema versions across a non-trivial evolution lineage**, plus binding, recovery, and compaction.

**Spec:** the test-matrix doc (scenario IDs referenced per task).

## Global Constraints
- Tests only; do not change production schema logic. Test-infra/docker wiring may change (GC7).
- Each test pins a concrete invariant; name regression-style (`*Regression*`) where it guards a rule. Run the repo's regression suite FIRST.
- Build recipes: codec — conan `-r conancenter`, `cmake_layout` (`build/build/Release`), `rune_codec_tests`. broker — x86_64 cross (`-s arch=x86_64`, `-DRUNE_FR_DIR=~/Projects/rune-fr`, `build-fast`, targets `rune_node_tests`/`rune_schema_gateway_tests`/`rune_schema_persistence_tests`). client — x86_64 cross + `-DRUNE_CRYPTO_ROOT=~/Projects/rune-crypto` + openssl.
- The durable invariant under test everywhere: a record decodes against ITS writer version; identity is `(topicId, tag)`, never name/position; tags never reused.

---

### GC1 (rune-codec): deep mixed-version-read lineages + drop-readd + rename-chain
**Scenarios:** C4–C13, C16–C25, B16–B30. **Files:** new `tests/MixedVersionReadTest.cpp` (+ extend `EvolveTest.cpp`); wire into `CMakeLists.txt` `rune_codec_tests`.
**Approach:** build an evolving `Descriptor` through a long lineage (e.g. v1 {id,age}; v2 +score; v3 drop age; v4 rename name→fullName; v5 +age again [fresh tag]). For a set of records each stamped with a different writer version and encoded against `materializeAsOf(evolving, writerVersion)`, decode each via `readFieldByTag` against its own version and assert field present/absent/value is correct for that version.
**Key assertions:**
- A v1 record and a v5 record both carrying an "age" field decode to DIFFERENT tags (v1 age = original tag, v5 age = fresh tag); reading a mixed {v1,v5} set never cross-reads the two.
- A rename-chain field (name→fullName→…) resolves to the SAME tag in records from every version; value identical across the rename boundary.
- A dropped field (age dropped at v3): records v1/v2 decode age; records v3/v4 report it absent; one scan over {v1..v5} is correct per record.
- Forward-read: a reader materialized at vK reading a v(K+n) record ignores tags added after K; reading a v(K-n) record still decodes K's fields that existed then.

### GC2 (rune-codec): decode boundary matrix
**Scenarios:** J2, J4–J18, J37, J41–46 (enum). **Files:** extend `EvolveTest.cpp`/`RecordReaderTest.cpp`.
**Key assertions:** `sinceVersion` boundary (field at K: absent in K-1, present in K/K+1); `untilVersion` EXCLUSIVE (dropped at M: present M-1, absent M/M+1); var-field drop → var-index reassignment correct for the live set per version; a FixedStr whose neighbours were added/dropped keeps its width but shifts offset per version; enum whitelist validation per version.

### GC3 (rune-broker): version-keyed validator / registry mixed-version read
**Scenarios:** C9–C13, C51–55, N9–N18. **Files:** extend `libs/node/tests/node/WorkerReplicationValidatorTest.cpp` + `libs/node/tests/node/SchemaRegistryTest.cpp` (+ `ProduceValidationTest.cpp`).
**Key assertions:** install validators for v1..vN (materialized per version via the registry); feed a batch/sequence of records stamped with mixed versions → each validated against its own version's validator; a record stamped vK carrying a layout from a different version is rejected; `byIdVersion`/`byFingerprint` resolve every historical version after a long lineage; a produce batch mixing versions validates each record correctly on the hot path (no cross-version false reject).

### GC4 (rune-broker): crash-recovery matrix
**Scenarios:** G3–G20 subset, G27–G32. **Files:** extend `libs/node/tests/integration/SchemaPersistenceTest.cpp`.
**Key assertions:** replay a `__topic_schemas` log truncated at various points (before/after a version's record is complete) → recovers exactly the complete-version prefix, never a partial/garbled version; HW-gated application (a record past HW not applied); a mixed data-log + schema-log replay → every data record still decodes per its writer version after restart; replaying the same log twice is byte-identical (time-independence).

### GC5 (rune-broker): compaction × long-lineage mixed-read
**Scenarios:** H3–H14, C71–78. **Files:** extend `SchemaPersistenceTest.cpp` (reuse the real `runBoundedCompaction` harness from the existing `OldVersionsSurviveCompactionRegression`).
**Key assertions:** build a 12-version lineage, run a real compaction pass on `__topic_schemas`, assert ALL 12 versions survive (every `(topicId,version)` key retained) and `byFingerprint(fp(vK))` resolves for every K; then a mixed read of records from several of those versions decodes each per its version post-compaction.

### GC6 (rune-client-cpp + broker): binding across a full lineage
**Scenarios:** D3–D11. **Files:** extend `tests/client/OpenTopicGateTest.cpp` (loopback) + `libs/node/tests/integration/SchemaGatewayTest.cpp` (broker side).
**Key assertions:** for a topic evolved through v1..vN, a client compiled against each vK (distinct `kFingerprint`) `openTopic`s and binds exactly vK (fingerprint-select returns that version's materialized blob; client re-verifies); a client with an unknown fingerprint is rejected; `send` stamps the bound vK into the record header for each K; the broker's `GetTopicSchema(requestedFingerprint=fp(vK))` returns version K for every K.

### GC7 (e2e): multi-producer-different-versions + long-lineage reopen — GATED on the docker-crypto fix
**Scenarios:** D12–D16, C51–55 (cross-stack). **Files:** `docker/Dockerfile.rune-ql-probe` + `docker/schema-evolution-e2e.sh` (+ compose build-context); new probe variant if needed.
**Step 1 (prereq / fix-on-sight):** the TLS merge added a `rune-crypto` sibling dep to rune-client-cpp, but the probe image build doesn't supply it → the e2e probe currently fails to build. Fix the docker wiring: add a `rune-crypto` build context / `COPY --from=rune-crypto src …` to `Dockerfile.rune-ql-probe` (mirror how `rune-protocol`/`rune-fr` are supplied), and the matching `--build-context`/compose entry in `schema-evolution-e2e.sh`. Re-run the EXISTING `schema-evolution-e2e.sh` and confirm it's green again (restores the #16 gate post-merge).
**Step 2 (new scenario):** extend the e2e so TWO producers built against DIFFERENT schema versions (v1 and v3) both write the same topic; a consumer reads the mixed log and decodes each record per its writer version; and a v1-compiled probe reopens after evolution to vN (already partially covered — extend to a longer lineage). Assert genuinely (non-zero exit on any mismatch). Run it green firsthand; if docker genuinely can't run here, report honestly (do not claim green).

---
## Execution notes
- Order: GC1, GC2 (codec, fast) → GC3, GC4, GC5 (broker) → GC6 (client) → GC7 (e2e, last; includes the docker-crypto prereq). GC1-GC6 are integration/unit (no docker) and independent; GC7 is the only docker/e2e one.
- Each task: implement → regression-first + targeted tests green → commit on the repo's `feat/schema-evolution-durable-tags` (already == its merged base) → per-task review → fix-loops. No push.
