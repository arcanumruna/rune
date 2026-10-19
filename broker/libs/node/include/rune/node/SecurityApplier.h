#pragma once
// SecurityApplier.h — every broker's apply of `__security` (authentication spec §3; plan D28/D29/D39).
//
// CatalogApplier's contract for `__security`, minus the projections and the per-kind effects (one `eventApplied`
// per event).  Per committed record (applyCommitted), in this order:
//   1. read the batch at nextOffset into a REUSED buffer (readBuf_);
//   2. rune::security::decode → rune::security::apply(state_, event, offset) — the shared library's frozen rules and
//      fingerprint verification (atomic: the state changes only on success);
//   3. any error (a decode error, an apply refusal, a fingerprint mismatch, a gap, an unreadable log, an event too
//      large to mirror) → Halted: spdlog::critical + the `halted` effect; NEVER skipped, nothing after it applied;
//   4. writeLogCheckpoint(dir, {offset, digest}) — BEFORE the effect: a crash after it is repaired by the boot
//      replay (which re-applies the event into the state handed to the control thread, posting no per-event effect),
//      a crash before it re-applies the event live — never twice, never skipped.  A failed write is a storage
//      failure (`storageFailed` → the broker's halt path) and halts the applier;
//   5. `eventApplied(offset, bytes)` (the event's bytes → the control thread's SecurityView, D29) and `applied`.
//
// An event whose mirror frame (`8 + bytes`) would exceed the sink's maxFrameBytes HALTS at that offset before it
// is applied (never truncated, D29).
//
// replayLocal (boot, STARTUP thread, before any thread runs) replays [0, checkpoint] through steps 1–3 (no
// checkpoint rewrite, no per-event effects) and verifies the resulting digest against the checkpoint's (a
// difference = local log damage → Halted).  It runs exactly once, from AwaitingBoot; a checkpoint at or past the
// local log end REFUSES to start (throws).  The broker hands `state()` (a copy) to the control thread's SecurityView;
// postBootStash then posts the `applied` metric and a boot-time halt once the admin agent exists.
//
// Hard rules: owned by the admin thread (shared-nothing); effects leave the thread only through the sink (ring
// encoders in production); the state machine is std::variant<Applying, Halted> and the effects phase
// std::variant<AwaitingBoot, Booted, Live> — boot vs live selected by the PHASE, never a flag; an idle
// applyCommitted allocates nothing.

#include <rune/storage/ZeroedBytes.h>
#include <rune/security/SecurityState.h>

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::replication { class PartitionReplica; }
namespace rune::storage { class Log; }

namespace rune::node {

/// The admin-thread effect sink.  Production = ring encoders (AdminWorkerAgent); tests = recorders.  An empty
/// function is skipped.
struct SecurityEffects {
    /// Live only: one applied event's bytes (valid inside the call), for the control thread's SecurityView
    /// (kA2cSecurityApplied = [offset:i64][bytes]).
    std::function<void(int64_t offset, std::span<const uint8_t> eventBytes)> eventApplied;
    std::function<void(int64_t offset, uint64_t digest)>                    applied;        // metric 1'000'227
    std::function<void(const rune::security::ApplyError&)>                  halted;         // kA2cSecurityHalted
    /// The checkpoint could not be made durable (errno, directory) — the broker's storage-failure halt path.
    std::function<void(int err, const std::string& dir)>                    storageFailed;
    /// The largest mirror frame this sink can carry (0 = unlimited).  Production: the admin→control ring's max message.
    std::size_t maxFrameBytes = 0;
};
struct SecurityBootEffects { SecurityEffects sink; };   // replayLocal
struct SecurityLiveEffects { SecurityEffects sink; };   // applyCommitted

/// The reused read buffer's size (capacity persists; security events are a few hundred bytes).
inline constexpr std::size_t kSecurityReadChunk = std::size_t{1} << 17;
/// A batch larger than the buffer grows it (doubling, capacity kept) up to this cap; past it the read is a halt.
inline constexpr std::size_t kSecurityReadMaxBuffer = std::size_t{16} << 20;

class SecurityApplier {
public:
    struct Applying { int64_t nextOffset = 0; };
    /// stagedOffset >= 0: the library applied that event into state() but its checkpoint could not be made durable
    /// (the event was never mirrored / effected).  -1: state() is the effected prefix.
    struct Halted   { rune::security::ApplyError error; int64_t stagedOffset = -1; };
    struct AwaitingBoot {};                           // constructed; the one boot replay has not run
    struct Booted { SecurityBootEffects effects; };   // replayLocal ran (exactly once); not yet live
    struct Live   { SecurityLiveEffects effects; };   // applyCommitted runs here only

    explicit SecurityApplier(std::string securityDir);

    SecurityApplier(SecurityApplier&&) noexcept            = default;
    SecurityApplier& operator=(SecurityApplier&&) noexcept = default;
    SecurityApplier(const SecurityApplier&)                = delete;
    SecurityApplier& operator=(const SecurityApplier&)     = delete;

    /// Boot: replay [0, checkpoint] from the local log — ONLY from AwaitingBoot (→ Booted).  Returns the number of
    /// events applied (a halted replay included).  Any other phase: logs an error, applies nothing, returns -1
    /// (exactly one boot replay).  Throws std::runtime_error — the broker must refuse to start — when the checkpoint
    /// is unreadable or names an offset at or past the local log end.
    int replayLocal(rune::storage::Log& log, SecurityBootEffects boot);
    int replayLocal(rune::replication::PartitionReplica& r, SecurityBootEffects boot);
    /// The boot effects that need a running broker, posted ONCE after the admin agent exists: the `applied` metric
    /// and — when the replay halted — the `halted` effect.
    void postBootStash(const SecurityEffects& sink) const;
    /// Booted → Live; AwaitingBoot → Live only when no checkpoint exists (an empty log).  Refused: an error log,
    /// the phase unchanged, false.  The broker calls it once, on the admin thread.
    bool goLive(SecurityLiveEffects live);
    /// Steady: apply every committed record in [nextOffset, hw) — Live only (any other phase, or halted: 0).
    /// Returns the events applied.
    int applyCommitted(rune::replication::PartitionReplica& r);

    [[nodiscard]] const rune::security::SecurityState& state() const noexcept { return state_; }
    [[nodiscard]] bool halted() const noexcept { return std::holds_alternative<Halted>(st_); }
    [[nodiscard]] const rune::security::ApplyError* haltError() const noexcept;
    [[nodiscard]] int64_t stagedOffset() const noexcept;
    /// The offset of the last event fully applied and effected (-1 = none).
    [[nodiscard]] int64_t lastAppliedOffset() const noexcept { return lastAppliedOffset_; }
    [[nodiscard]] int64_t nextOffset() const noexcept;           // -1 when halted
    [[nodiscard]] std::string_view stateName() const noexcept;   // "Applying" | "Halted"
    [[nodiscard]] std::string_view phaseName() const noexcept;   // "AwaitingBoot" | "Booted" | "Live"
    [[nodiscard]] const std::string& securityDir() const noexcept { return dir_; }
    [[nodiscard]] std::size_t readBufferBytesForTest() const noexcept { return readBuf_.size(); }
    /// Test-only: the NEXT decoded event's fingerprint is flipped (^1) before it is applied.
    void tamperNextFingerprintForTest() noexcept { tamperXor_ = 1; }

private:
    int  replay_(rune::storage::Log* log, int64_t leo, SecurityBootEffects boot);
    int  applyRange_(rune::storage::Log* log, int64_t bound);
    bool applyRecord_(int64_t offset, std::span<const uint8_t> bytes);
    void halt_(rune::security::ApplyError err, int64_t stagedOffset = -1);
    [[nodiscard]] const SecurityEffects* sink_() const noexcept;

    std::string                                  dir_;
    rune::security::SecurityState                state_;
    std::variant<Applying, Halted>               st_;
    std::variant<AwaitingBoot, Booted, Live>     phase_;
    int64_t                                      lastAppliedOffset_ = -1;
    rune::storage::ZeroedBytes                   readBuf_;
    uint64_t                                     tamperXor_ = 0;
};

} // namespace rune::node
