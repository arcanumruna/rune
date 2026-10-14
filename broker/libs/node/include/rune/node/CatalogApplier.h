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
// replayLocal (boot) replays [0, checkpoint] through steps 1–3 and 5–7 into the
// BOOT sink (no checkpoint rewrite) and verifies the resulting digest against the
// checkpoint's (a difference = local log damage → Halted).  It runs exactly once,
// from AwaitingBoot.  A checkpoint at or past the local log end REFUSES to start
// (throws: the log lost applied events — review M8).
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
};

/// Boot vs live effects are selected by the applier's PHASE, never by a flag (hard rule 3, F4).
struct BootEffects { CatalogEffects sink; };   // replayLocal: every view install is a RECOVERY install
struct LiveEffects { CatalogEffects sink; };   // applyCommitted: every view install is a commit install

/// The reused read buffer's size (capacity persists; one event never exceeds the
/// admin request ring's max message, far below it).
inline constexpr std::size_t kCatalogReadChunk = std::size_t{1} << 20;

class CatalogApplier {
public:
    struct Applying { int64_t nextOffset = 0; };
    struct Halted   { rune::catalog::ApplyError error; };
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
    /// AwaitingBoot | Booted → Live.  The broker calls it once, on the admin thread.
    void goLive(LiveEffects live);
    /// Steady: apply every committed record in [nextOffset, hw) — Live only (any
    /// other phase, or halted: 0, nothing applied).  Returns the events applied.
    int applyCommitted(rune::replication::PartitionReplica& r);

    [[nodiscard]] const rune::catalog::CatalogState& state() const noexcept { return state_; }
    [[nodiscard]] const AppliedAt& lastApplied() const noexcept { return lastApplied_; }   // F28
    [[nodiscard]] bool halted() const noexcept { return std::holds_alternative<Halted>(st_); }
    /// The halt's cause (nullptr while applying).
    [[nodiscard]] const rune::catalog::ApplyError* haltError() const noexcept;
    [[nodiscard]] int64_t nextOffset() const noexcept;           // -1 when halted
    [[nodiscard]] std::string_view stateName() const noexcept;   // "Applying" | "Halted"
    [[nodiscard]] std::string_view phaseName() const noexcept;   // "AwaitingBoot" | "Booted" | "Live"
    [[nodiscard]] const std::string& catalogDir() const noexcept { return dir_; }
    /// The matrix FingerprintMismatch seam (test-only): the NEXT decoded event's
    /// fingerprint is flipped (^1) before it is applied.
    void tamperNextFingerprintForTest() noexcept { tamperXor_ = 1; }

private:
    // The step list is the phase's: Booted (replayLocal) skips the checkpoint
    // rewrite, Live (applyCommitted) writes it before the effects.
    int  applyRange_(rune::replication::PartitionReplica& r, int64_t bound);
    bool applyRecord_(int64_t offset, std::span<const uint8_t> bytes);
    void project_(const rune::catalog::CatalogEvent& e, const rune::catalog::Applied& a);
    void effects_(const CatalogEffects& sink, const rune::catalog::CatalogEvent& e, const rune::catalog::Applied& a);
    void halt_(rune::catalog::ApplyError err);
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
};

} // namespace rune::node
