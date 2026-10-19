// SecurityApplier.cpp — offset-ordered, verified apply of `__security` (authentication spec §3; plan D28/D29/D39).
// See SecurityApplier.h for the step list.  A structural copy of CatalogApplier minus the projections and the
// per-kind effects.
#include <rune/node/SecurityApplier.h>

#include <rune/node/CatalogCheckpoint.h>

#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <utility>

namespace rune::node {

namespace sec = rune::security;

namespace {

sec::ApplyError decodeFailure(int64_t offset, sec::DecodeError e) {
    sec::ApplyError err;
    err.offset = offset;
    err.code   = e == sec::DecodeError::UnknownFormatVer ? sec::ApplyError::Code::UnsupportedFormat
                                                         : sec::ApplyError::Code::BadOps;
    err.detail = "the record does not decode: " + std::string{sec::decodeErrorName(e)};
    return err;
}

sec::ApplyError localFailure(int64_t offset, sec::ApplyError::Code code, std::string detail) {
    sec::ApplyError err;
    err.offset = offset;
    err.code   = code;
    err.detail = std::move(detail);
    return err;
}

} // namespace

SecurityApplier::SecurityApplier(std::string securityDir)
    : dir_(std::move(securityDir)), st_(Applying{0}), phase_(AwaitingBoot{}), readBuf_(kSecurityReadChunk) {}

int64_t SecurityApplier::stagedOffset() const noexcept {
    const Halted* h = std::get_if<Halted>(&st_);
    return h ? h->stagedOffset : -1;
}

const sec::ApplyError* SecurityApplier::haltError() const noexcept {
    const Halted* h = std::get_if<Halted>(&st_);
    return h ? &h->error : nullptr;
}

int64_t SecurityApplier::nextOffset() const noexcept {
    const Applying* a = std::get_if<Applying>(&st_);
    return a ? a->nextOffset : -1;
}

std::string_view SecurityApplier::stateName() const noexcept {
    return std::holds_alternative<Applying>(st_) ? "Applying" : "Halted";
}

std::string_view SecurityApplier::phaseName() const noexcept {
    switch (phase_.index()) {
    case 0:  return "AwaitingBoot";
    case 1:  return "Booted";
    default: return "Live";
    }
}

const SecurityEffects* SecurityApplier::sink_() const noexcept {
    if (const Booted* b = std::get_if<Booted>(&phase_)) return &b->effects.sink;
    if (const Live* l = std::get_if<Live>(&phase_)) return &l->effects.sink;
    return nullptr;
}

// ─── replayLocal — the one boot replay of [0, checkpoint] ────────────────────
int SecurityApplier::replayLocal(rune::replication::PartitionReplica& r, SecurityBootEffects boot) {
    rune::storage::Log* log = r.log();
    return replay_(log, log ? log->logEndOffset() : r.logEndOffset(), std::move(boot));
}

int SecurityApplier::replayLocal(rune::storage::Log& log, SecurityBootEffects boot) {
    return replay_(&log, log.logEndOffset(), std::move(boot));
}

int SecurityApplier::replay_(rune::storage::Log* log, int64_t leo, SecurityBootEffects boot) {
    if (!std::holds_alternative<AwaitingBoot>(phase_)) {
        spdlog::error("SecurityApplier: replayLocal refused in phase {} — the boot prefix is applied exactly once "
                      "(a second replay would re-apply recorded request ids)", phaseName());
        return -1;
    }
    phase_ = Booted{std::move(boot)};

    // Throws (refuse to start) on an unreadable / corrupt checkpoint.
    const std::optional<LogCheckpoint> ckpt = readLogCheckpoint(dir_, kSecurityLogKind);
    if (auto refusal = logCheckpointBeyondLog(ckpt, leo, dir_, kSecurityLogKind)) {
        spdlog::critical("SecurityApplier: {}", *refusal);
        throw std::runtime_error(*refusal);
    }
    if (!ckpt) {
        spdlog::info("SecurityApplier: no applied checkpoint in {} — nothing to replay (log end {})", dir_, leo);
        std::get<Booted>(phase_).effects = SecurityBootEffects{};
        return 0;
    }

    const int applied = applyRange_(log, ckpt->appliedOffset + 1);
    if (!halted() && nextOffset() != ckpt->appliedOffset + 1) {
        halt_(localFailure(nextOffset(), sec::ApplyError::Code::Precondition,
                           "boot replay stopped at offset " + std::to_string(nextOffset()) +
                               " before the checkpointed offset " + std::to_string(ckpt->appliedOffset) +
                               " — the local __security log is missing or cannot serve applied events"));
    }
    if (!halted()) {
        const uint64_t digest = state_.digest();
        if (digest != ckpt->digest) {
            sec::ApplyError err = localFailure(ckpt->appliedOffset, sec::ApplyError::Code::FingerprintMismatch,
                                               "boot replay of [0, " + std::to_string(ckpt->appliedOffset) +
                                                   "] ends at a security digest other than the checkpoint's — local "
                                                   "__security log damage");
            err.expected = ckpt->digest;
            err.actual   = digest;
            halt_(std::move(err));
        } else {
            spdlog::info("SecurityApplier: boot replay applied {} event(s), [0, {}], digest {:#x} verified",
                         applied, ckpt->appliedOffset, digest);
        }
    }
    // The boot sink serves the replay only; release its captures (the caller's locals die with it) — a halt in
    // Booted posts nothing, postBootStash delivers it.
    std::get<Booted>(phase_).effects = SecurityBootEffects{};
    return applied;
}

// ─── postBootStash — the boot effects that need the running broker ────────────
void SecurityApplier::postBootStash(const SecurityEffects& sink) const {
    if (lastAppliedOffset_ >= 0 && sink.applied) sink.applied(lastAppliedOffset_, state_.digest());
    if (const Halted* h = std::get_if<Halted>(&st_); h != nullptr && sink.halted) sink.halted(h->error);
}

bool SecurityApplier::goLive(SecurityLiveEffects live) {
    if (std::holds_alternative<Live>(phase_)) {
        spdlog::error("SecurityApplier: goLive refused — already Live");
        return false;
    }
    if (std::holds_alternative<AwaitingBoot>(phase_)) {
        // AwaitingBoot → Live is an EMPTY log's path only.  With a checkpoint the boot replay must run first, or
        // every applied event would be re-applied live (mirrored again, checkpoints rewritten).
        bool hasCheckpoint = true;
        try {
            hasCheckpoint = readLogCheckpoint(dir_, kSecurityLogKind).has_value();
        } catch (const std::exception& ex) {
            spdlog::error("SecurityApplier: goLive refused — the checkpoint is unreadable: {}", ex.what());
            return false;
        }
        if (hasCheckpoint) {
            spdlog::error("SecurityApplier: goLive refused in AwaitingBoot — {} holds an applied checkpoint; "
                          "replayLocal must run first", dir_);
            return false;
        }
    }
    phase_ = Live{std::move(live)};
    return true;
}

// ─── applyCommitted — steady state: [nextOffset, HW) ──────────────────────────
int SecurityApplier::applyCommitted(rune::replication::PartitionReplica& r) {
    if (!std::holds_alternative<Live>(phase_)) return 0;
    const Applying* a = std::get_if<Applying>(&st_);
    if (a == nullptr) return 0;   // halted: no further apply, ever
    const int64_t hw = r.highWatermark();
    if (a->nextOffset >= hw) return 0;   // idle: one compare, no allocation
    return applyRange_(r.log(), hw);
}

// ─── applyRange_ — read [nextOffset, bound) and apply record by record ───────
int SecurityApplier::applyRange_(rune::storage::Log* log, int64_t bound) {
    namespace st = rune::storage;
    if (log == nullptr) {
        halt_(localFailure(nextOffset(), sec::ApplyError::Code::Precondition,
                           "the __security replica has no disk log (the security log is always disk-backed)"));
        return 0;
    }
    int applied = 0;
    auto cannotRead = [&](int64_t at, const std::string& cause) {
        halt_(localFailure(at, sec::ApplyError::Code::Precondition,
                           "cannot read committed offset " + std::to_string(at) + " (below " +
                               std::to_string(bound) + "): " + cause));
    };
    while (const Applying* a = std::get_if<Applying>(&st_)) {
        const int64_t from = a->nextOffset;
        if (from >= bound) break;

        st::ReadIntoStatus status = st::ReadIntoStatus::Ok;
        int err = 0;
        std::span<const uint8_t> bytes;
        {
            const auto maxBytes = static_cast<int32_t>(readBuf_.size() / 2);
            const st::LogReadView v = log->readInto(from, maxBytes, readBuf_.data(), readBuf_.size(), status, bound, &err);
            if (!v.empty()) bytes = std::span<const uint8_t>(v.data, v.len);
        }
        switch (status) {
        case st::ReadIntoStatus::Ok:
            break;
        case st::ReadIntoStatus::FirstTooLarge:
            // Grow the reused buffer (capacity kept: never a per-pass allocation) and retry.
            if (readBuf_.size() >= kSecurityReadMaxBuffer) {
                cannotRead(from, "oversize — the batch does not fit the " + std::to_string(kSecurityReadMaxBuffer) +
                                     " B read buffer cap");
                return applied;
            }
            spdlog::warn("SecurityApplier: the batch at offset {} exceeds the {} B read buffer — growing it", from,
                         readBuf_.size());
            readBuf_.reset(std::min(readBuf_.size() * 2, kSecurityReadMaxBuffer));
            continue;
        case st::ReadIntoStatus::Absent:
            cannotRead(from, "Absent — the log holds no batch at or after it");
            return applied;
        case st::ReadIntoStatus::Unreadable:
            cannotRead(from, "unreadable — torn or corrupt bytes");
            return applied;
        case st::ReadIntoStatus::IoError:
            cannotRead(from, "I/O error: " + std::string{std::strerror(err)});
            return applied;
        }
        if (bytes.empty()) {
            cannotRead(from, "empty — the read returned no bytes");
            return applied;
        }
        if (bytes.size() < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            cannotRead(from, "short — " + std::to_string(bytes.size()) + " bytes, less than one batch header");
            return applied;
        }

        std::size_t pos = 0;
        while (pos < bytes.size() && !halted()) {
            const uint8_t*    bs  = bytes.data() + pos;
            const std::size_t rem = bytes.size() - pos;
            if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader reader(bs, rem);
            if (!reader.ensureValid()) {
                halt_(localFailure(reader.baseOffset(), sec::ApplyError::Code::Precondition,
                                   "a __security batch fails its CRC / length check"));
                break;
            }
            const auto tl = static_cast<std::size_t>(reader.totalLength());
            if (reader.isControl()) {
                // Not a security event (never written by the leader); its offsets are consumed — but only
                // CONTIGUOUSLY: a control batch past nextOffset hides a gap, never skipped over.
                Applying& cur = std::get<Applying>(st_);
                if (reader.baseOffset() > cur.nextOffset) {
                    halt_(localFailure(cur.nextOffset, sec::ApplyError::Code::Precondition,
                                       "a gap in __security: the next batch (a control batch) starts at offset " +
                                           std::to_string(reader.baseOffset())));
                    break;
                }
                const int64_t end = reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (end > cur.nextOffset) cur.nextOffset = std::min(end, bound);
                pos += tl;
                continue;
            }
            reader.forEachRecord([&](std::string_view /*key*/, std::string_view val, int64_t off, int64_t /*ts*/) {
                const Applying* cur = std::get_if<Applying>(&st_);
                if (cur == nullptr || off >= bound) return;   // halted, or past the commit bound
                if (off < cur->nextOffset) return;            // applied before (an earlier pass)
                if (off != cur->nextOffset) {
                    halt_(localFailure(cur->nextOffset, sec::ApplyError::Code::Precondition,
                                       "a gap in __security: the next record is at offset " + std::to_string(off)));
                    return;
                }
                if (applyRecord_(off, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()),
                                                               val.size())))
                    ++applied;
            });
            pos += tl;
        }
        const Applying* after = std::get_if<Applying>(&st_);
        if (after == nullptr) break;   // halted (explicit already)
        if (after->nextOffset == from) {
            cannotRead(from, "no record at it in the batches read (the read ended before it)");
            break;
        }
    }
    return applied;
}

// ─── applyRecord_ — steps 2–5 for ONE record ─────────────────────────────────
bool SecurityApplier::applyRecord_(int64_t offset, std::span<const uint8_t> bytes) {
    const bool live = std::holds_alternative<Live>(phase_);
    const SecurityEffects* sink = sink_();
    // D29: the event's bytes go to the control thread in ONE ring message; an event that cannot be mirrored in
    // full halts here (before it is applied) — never truncated, never skipped.
    if (live && sink != nullptr && sink->maxFrameBytes != 0 && 8 + bytes.size() > sink->maxFrameBytes) {
        halt_(localFailure(offset, sec::ApplyError::Code::Precondition,
                           "the event is " + std::to_string(bytes.size()) +
                               " bytes, too large to mirror to the control thread (the admin→control ring carries " +
                               std::to_string(sink->maxFrameBytes) + " per message)"));
        return false;
    }
    auto ev = sec::decode(bytes);
    if (!ev) {
        halt_(decodeFailure(offset, ev.error()));
        return false;
    }
    if (tamperXor_ != 0) [[unlikely]] {
        ev->fingerprint ^= tamperXor_;
        tamperXor_ = 0;
    }
    auto a = sec::apply(state_, *ev, offset);
    if (!a) {
        halt_(std::move(a.error()));
        return false;
    }
    // From here on the library holds the event: a halt names it staged.
    if (live) {
        // Step 4: the checkpoint precedes the effect.
        if (!writeLogCheckpoint(dir_, LogCheckpoint{offset, state_.digest()}, kSecurityLogKind)) {
            const int e = errno;
            sec::ApplyError err = localFailure(offset, sec::ApplyError::Code::Precondition,
                                               "the applied checkpoint could not be made durable in " + dir_ +
                                                   " (errno " + std::to_string(e) + ": " + std::strerror(e) +
                                                   ") — storage failure; the event was not mirrored");
            if (sink && sink->storageFailed) sink->storageFailed(e, dir_);
            halt_(std::move(err), offset);
            return false;
        }
    }

    std::get<Applying>(st_).nextOffset = offset + 1;
    lastAppliedOffset_ = offset;
    // Step 5, LIVE only: the boot sink serves the replay with no per-event effect (postBootStash).
    if (live && sink != nullptr) {
        if (sink->eventApplied) sink->eventApplied(offset, bytes);
        if (sink->applied) sink->applied(offset, state_.digest());
    }
    return true;
}

void SecurityApplier::halt_(sec::ApplyError err, int64_t stagedOffset) {
    spdlog::critical("SecurityApplier: apply HALTED at offset {} — {} {} (expected={:#x} actual={:#x}{}); this broker "
                     "fails closed: it revokes every authenticated session, refuses logins and security changes "
                     "until an operator intervenes (move dataDir/__security aside and restart to refetch it from a "
                     "healthy broker)",
                     err.offset, sec::applyErrorName(err.code), err.detail, err.expected, err.actual,
                     stagedOffset >= 0 ? "; the event is staged in the security state, its mirror never ran" : "");
    st_ = Halted{std::move(err), stagedOffset};
    if (const SecurityEffects* sink = sink_(); sink && sink->halted) sink->halted(std::get<Halted>(st_).error);
}

} // namespace rune::node
