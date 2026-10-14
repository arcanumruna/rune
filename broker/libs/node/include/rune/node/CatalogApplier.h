#pragma once
// CatalogApplier.h — every broker's apply of `__catalog` (catalog history spec §5.3,
// ruling 8).
//
// "Each broker applies strictly in offset order from its last applied offset; for
//  each event: apply under its formatVer, verify the fingerprint, update the local
//  catalog, distribute to the workers.  A mismatch stops apply loudly; the broker
//  keeps serving what it applied and accepts no further catalog change."
//
// Per committed record (applyCommitted), in this order:
//   1. read the batch at nextOffset into a REUSED buffer (readBuf_, sized once);
//   2. rune::catalog::decode → rune::catalog::apply(state_, event, offset) — the
//      shared library's frozen rules + fingerprint verification (atomic: the state
//      changes only on success);
//   3. any error (a decode error, an apply refusal, a fingerprint mismatch, a
//      gap, an unreadable log) → Halted: spdlog::critical + the `halted` effect
//      (control thread: metric + log); NEVER skipped, nothing after it is applied;
//   4. writeCatalogCheckpoint(dir, {offset, digest}) — BEFORE any side effect
//      (ruling 8): a crash after it re-runs the effects through the BOOT sink, a
//      crash before it re-applies the event live — never twice, never skipped.
//      A failed write is a storage failure (the `storageFailed` effect →
//      kA2cStorageFailure, the broker's halt path) and halts the applier;
//   5. projections: SchemaRegistry / ViewRegistry (applyRecord / erase);
//   6. effects by kind into the CURRENT PHASE's sink;
//   7. lastApplied_ = {offset, applied} (F28); the `applied` effect.
//
// Before step 4 (Live), every effect frame the event will post is sized against
// the sink's maxFrameBytes (CatalogEffectFrames.h): an oversize effect HALTS at
// that offset (fix round 1, C1) — no input-derived size ever aborts.  Such a halt
// (and a checkpoint / projection failure) happens after the library applied the
// event: Halted::stagedOffset names it — state() then INCLUDES that event, whose
// projections / effects never ran (lastApplied() / the checkpoint stay before it).
//
// replayLocal (boot) replays [0, checkpoint] through steps 1–3 and 5 (no
// checkpoint rewrite, no per-event effects) and verifies the resulting digest
// against the checkpoint's (a difference = local log damage → Halted).  Its BOOT
// sink is COALESCED (fix round 1, I1): after the replay it posts the FINAL state
// only — each live topic (topicApplied [+ schemaChanged]), then each live view
// (sinkApplied + viewInstalled) — and ONE `applied`; nothing for a dropped
// object.  Its effect volume is O(live catalog), never O(history).  It runs
// exactly once, from AwaitingBoot.  A checkpoint at or past the local log end
// REFUSES to start (throws: the log lost applied events — review M8).
//
// A read that makes no progress below the bound is an explicit halt naming its
// cause (Absent / empty / short / oversize — fix round 1, I2), and a record or a
// control batch past nextOffset is a gap halt (I3): nothing is ever skipped.
//
// Hard rules: owned by the admin thread (shared-nothing — the registries and the
// replica are admin-owned too); effects leave the thread only through the sink,
// whose production form encodes ring messages (AdminWorkerAgent); the state
// machine is std::variant<Applying, Halted> and the effects phase
// std::variant<AwaitingBoot, Booted, Live> — boot vs live effects are selected by
// the PHASE, never by a flag (F4); an idle applyCommitted allocates nothing.

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::replication { class PartitionReplica; }

namespace rune::node {

class SchemaRegistry;
class ViewRegistry;

/// A view's sink topic (F27): a view sink has no TopicObject — the applier builds
/// this from the ViewDef.  `keyField` = the view's key (the sink is a keyed,
/// compacted table today — Task 19's CreateTopicReq carried it the same way).
struct SinkSpec {
    int32_t                      topicId    = -1;
    std::string                  name;
    int32_t                      partitions = 1;
    rune::catalog::Persistence   persistence = rune::catalog::Persistence::Disk;
    std::string                  keyField;
};

/// The admin-thread effect sink.  Production = ring encoders (AdminWorkerAgent);
/// tests = recorders.  An empty function is skipped.
struct CatalogEffects {
    std::function<void(const rune::catalog::TopicObject&)>               topicApplied;     // CreateTopic only
    std::function<void(const SinkSpec&)>                                 sinkApplied;      // sink of Create/AlterView (F27)
    std::function<void(int32_t topicId, const std::string& name)>        topicDropped;     // DropTopic, retired sink
    std::function<void(int32_t topicId, std::span<const rune::catalog::ConfigChange>)> topicConfig;
    std::function<void(const rune::catalog::TopicObject&)>               schemaChanged;    // validators fan-out
    std::function<void(const rune::catalog::ViewObject&)>                viewInstalled;    // recovery-ness = the phase (F4)
    std::function<void(int32_t viewTopicId)>                             viewUninstalled;
    std::function<void(int64_t offset, uint64_t digest)>                 applied;          // metric + tests
    std::function<void(const rune::catalog::ApplyError&)>               halted;
    /// The checkpoint could not be made durable (errno, catalog dir) — the broker's
    /// storage-failure halt path (kA2cStorageFailure).
    std::function<void(int err, const std::string& dir)>                 storageFailed;
    /// The largest effect frame this sink can carry (0 = unlimited).  Production:
    /// the admin→control ring's max message (fix round 1, C1).
    std::size_t maxFrameBytes = 0;
};

/// Boot vs live effects are selected by the applier's PHASE, never by a flag (hard rule 3, F4).
struct BootEffects { CatalogEffects sink; };   // replayLocal: every view install is a RECOVERY install
struct LiveEffects { CatalogEffects sink; };   // applyCommitted: every view install is a commit install

/// The reused read buffer's size (capacity persists; one event never exceeds the
/// admin request ring's max message, far below it).
inline constexpr std::size_t kCatalogReadChunk = std::size_t{1} << 20;
/// A batch larger than the buffer grows it (doubling, capacity kept — never a
/// per-pass allocation) up to this cap; past it the read is an explicit halt.
inline constexpr std::size_t kCatalogReadMaxBuffer = std::size_t{64} << 20;

class CatalogApplier {
public:
    struct Applying { int64_t nextOffset = 0; };
    /// stagedOffset >= 0: the library applied that event into state() but its
    /// projections / effects never ran (an effect too large, a checkpoint or a
    /// projection failure — fix round 1, M1).  -1: state() is the effected prefix.
    struct Halted   { rune::catalog::ApplyError error; int64_t stagedOffset = -1; };
    struct AwaitingBoot {};                          // constructed; the one boot replay has not run
    struct Booted { BootEffects effects; };          // replayLocal ran (exactly once); not yet live
    struct Live   { LiveEffects effects; };          // applyCommitted runs here only
    struct AppliedAt { int64_t offset = -1; rune::catalog::Applied applied{}; };   // F28

    CatalogApplier(std::string catalogDir, SchemaRegistry& schemas, ViewRegistry& views);

    CatalogApplier(CatalogApplier&&) noexcept            = default;
    CatalogApplier& operator=(CatalogApplier&&) noexcept = default;
    CatalogApplier(const CatalogApplier&)                = delete;
    CatalogApplier& operator=(const CatalogApplier&)     = delete;

    /// Boot: replay [0, checkpoint] from the local log through `boot` — ONLY from
    /// AwaitingBoot (→ Booted).  Returns the number of events applied (a halted
    /// replay included).  Any other phase: logs an error, applies nothing, returns
    /// -1 (exactly one boot replay).  Throws std::runtime_error — the broker must
    /// refuse to start — when the checkpoint is unreadable or names an offset at or
    /// past the local log end (review M8: never replay a shorter history).
    int replayLocal(rune::replication::PartitionReplica& r, BootEffects boot);
    /// Booted → Live; AwaitingBoot → Live only when no checkpoint exists (an empty
    /// catalog — fix round 1, M2: with one, the boot replay must run first or the
    /// whole history would be re-applied live).  Refused: an error log, the phase
    /// unchanged, false.  The broker calls it once, on the admin thread.
    bool goLive(LiveEffects live);
    /// Steady: apply every committed record in [nextOffset, hw) — Live only (any
    /// other phase, or halted: 0, nothing applied).  Returns the events applied.
    int applyCommitted(rune::replication::PartitionReplica& r);

    [[nodiscard]] const rune::catalog::CatalogState& state() const noexcept { return state_; }
    [[nodiscard]] const AppliedAt& lastApplied() const noexcept { return lastApplied_; }   // F28
    [[nodiscard]] bool halted() const noexcept { return std::holds_alternative<Halted>(st_); }
    /// The halt's cause (nullptr while applying).
    [[nodiscard]] const rune::catalog::ApplyError* haltError() const noexcept;
    /// Halted with an event staged in state() but not effected (M1); -1 otherwise.
    [[nodiscard]] int64_t stagedOffset() const noexcept;
    [[nodiscard]] std::size_t readBufferBytesForTest() const noexcept { return readBuf_.size(); }
    [[nodiscard]] int64_t nextOffset() const noexcept;           // -1 when halted
    [[nodiscard]] std::string_view stateName() const noexcept;   // "Applying" | "Halted"
    [[nodiscard]] std::string_view phaseName() const noexcept;   // "AwaitingBoot" | "Booted" | "Live"
    [[nodiscard]] const std::string& catalogDir() const noexcept { return dir_; }
    /// The matrix FingerprintMismatch seam (test-only): the NEXT decoded event's
    /// fingerprint is flipped (^1) before it is applied.
    void tamperNextFingerprintForTest() noexcept { tamperXor_ = 1; }
    /// I2 seam (test-only): the next `count` log reads answer `status` with
    /// `bytes` (-1 = every read) instead of reading the log.
    void injectReadForTest(int status, std::vector<uint8_t> bytes, int count) {
        readFault_ = ReadFault{status, std::move(bytes), count};
    }

private:
    // The step list is the phase's: Booted (replayLocal) skips the checkpoint
    // rewrite, Live (applyCommitted) writes it before the effects.
    int  applyRange_(rune::replication::PartitionReplica& r, int64_t bound);
    bool applyRecord_(int64_t offset, std::span<const uint8_t> bytes);
    void project_(const rune::catalog::CatalogEvent& e, const rune::catalog::Applied& a);
    void effects_(const CatalogEffects& sink, const rune::catalog::CatalogEvent& e, const rune::catalog::Applied& a);
    void halt_(rune::catalog::ApplyError err, int64_t stagedOffset = -1);
    void emitFinalState_();   // the coalesced boot sink (I1)
    [[nodiscard]] const CatalogEffects* sink_() const noexcept;

    std::string                                  dir_;
    SchemaRegistry*                              schemas_;
    ViewRegistry*                                views_;
    rune::catalog::CatalogState                  state_;
    std::variant<Applying, Halted>               st_;
    std::variant<AwaitingBoot, Booted, Live>     phase_;
    AppliedAt                                    lastApplied_;
    std::vector<uint8_t>                         readBuf_;      // kCatalogReadChunk, capacity kept
    uint64_t                                     tamperXor_ = 0;
    struct ReadFault { int status = 0; std::vector<uint8_t> bytes; int count = 0; };
    ReadFault                                    readFault_;   // count 0 = none (test seam)
};

} // namespace rune::node
