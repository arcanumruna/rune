// CatalogReadServer.cpp — catalog history Task 12.  See CatalogReadServer.h.
#include <rune/node/CatalogReadServer.h>

#include <rune/node/CatalogPartition.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <random>
#include <type_traits>

namespace rune::node {

namespace pc = rune::protocol::client;

namespace {
constexpr auto kOk              = static_cast<int16_t>(pc::ErrorCode::OK);
constexpr auto kInternal        = static_cast<int16_t>(pc::ErrorCode::Internal);
constexpr auto kMessageTooLarge = static_cast<int16_t>(pc::ErrorCode::MessageTooLarge);
constexpr auto kNotLeader       = static_cast<int16_t>(pc::ErrorCode::NotLeader);
constexpr int64_t kFromEarliest = -2;
static_assert(pc::SubscribeResponse::ENCODED_SIZE <= kCatalogReadMaxFrameBytes);
static_assert(kCatalogReadBufferBytes / 2 >= kCatalogReadMaxBatchBytes,
              "one read must hold the largest servable batch whole");

/// A fresh history epoch: random 64 bits (two brokers, or two incarnations of one
/// data dir, never share a value), mixed with the node prefix and the clock.
uint64_t freshEpoch(int64_t idPrefix) {
    std::random_device rd;
    uint64_t v = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    v ^= static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) * 0x9e3779b97f4a7c15ULL;
    v ^= static_cast<uint64_t>(idPrefix);
    return v;
}
}   // namespace

CatalogReadServer::CatalogReadServer(rune::replication::PartitionReplica& catalog, CatalogReadEgress send,
                                     std::size_t maxSubs, int64_t idPrefix)
    : catalog_(catalog), send_(std::move(send)), maxSubs_(maxSubs), idPrefix_(idPrefix),
      readBuf_(kCatalogReadBufferBytes), frameBuf_(kCatalogReadMaxFrameBytes) {
    subs_.reserve(maxSubs_);
    // M3: every owed answer has a bounded source — refusals / re-answers (gated by
    // intakePaused), one ended answer per subscription, one pause ack per control
    // entry (<= 2 x maxSubs) — so the reservation is never exceeded.
    owedCapacity_ = 4 * maxSubs_ + 2 * kIntakeSlack;
    owed_.reserve(owedCapacity_);
    loadEpoch_();
}

// ─── History epoch (Task 13 fix round 2) ───────────────────────────────────────
void CatalogReadServer::loadEpoch_() {
    if (const rune::storage::Log* log = catalog_.log(); log != nullptr && !log->dir().empty()) {
        epochDir_  = log->dir();
        epochPath_ = (std::filesystem::path{epochDir_} / kCatalogHistoryEpochFile).string();
        epochTmp_  = epochPath_ + ".tmp";
    }
    const std::optional<CatalogHistoryEpoch> e =
        epochPath_.empty() ? std::nullopt : readCatalogHistoryEpoch(epochPath_);
    if (!e) {
        historyEpoch_ = freshEpoch(idPrefix_);
        spdlog::info("CatalogReadServer: no durable catalog history epoch at '{}' — a fresh one ({:#x}); "
                     "every client cache of this broker is re-read", epochPath_, historyEpoch_);
        return;
    }
    historyEpoch_ = e->epoch;
    durable_      = e;
    if (e->servedFloor == 0) return;
    // The anchor: the batch holding servedFloor-1 must still be the one the epoch
    // was recorded at — else the log lost / replaced what was served under it.
    CatalogHistoryEpoch now = *e;
    if (readAnchor_(e->servedFloor - 1, now) && now.anchorBase == e->anchorBase &&
        now.anchorLeaderEpoch == e->anchorLeaderEpoch && now.anchorCrc == e->anchorCrc)
        return;
    bumpEpoch_("at boot the log no longer holds the batch the epoch was anchored at (lost or replaced "
               "bytes below the served floor)");
}

void CatalogReadServer::bumpEpoch_(const char* why) {
    ++historyEpoch_;
    ++epochBumps_;
    spdlog::critical("CatalogReadServer: catalog history epoch -> {:#x} — {}; every client cache is re-read "
                     "from 0", historyEpoch_, why);
}

bool CatalogReadServer::readAnchor_(int64_t offset, CatalogHistoryEpoch& e) {
    namespace b = rune::protocol::batch;
    rune::storage::Log* log = catalog_.log();
    if (log == nullptr || offset < 0 || offset >= catalog_.logEndOffset()) return false;
    rune::storage::ReadIntoStatus status = rune::storage::ReadIntoStatus::Ok;
    const rune::storage::LogReadView v = log->readInto(offset, static_cast<int32_t>(kCatalogReadMaxBatchBytes),
                                                       readBuf_.data(), readBuf_.size(), status);
    if (status != rune::storage::ReadIntoStatus::Ok) return false;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(b::kRecordBatchOverhead) <= v.len) {
        rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
        const int32_t tl = r.totalLength();
        if (tl < b::kRecordBatchOverhead || pos + static_cast<std::size_t>(tl) > v.len) return false;
        const int64_t base = r.baseOffset();
        if (base <= offset && offset <= base + r.lastOffsetDelta()) {
            e.anchorBase        = base;
            e.anchorLeaderEpoch = r.partitionLeaderEpoch();
            e.anchorCrc         = rune::protocol::detail::getLE32(v.data + pos + b::kCrcOffset);
            return true;
        }
        pos += static_cast<std::size_t>(tl);
    }
    return false;
}

bool CatalogReadServer::ensureDurable_(int64_t floor) {
    if (durable_ && durable_->epoch == historyEpoch_ && durable_->servedFloor >= floor) return true;
    CatalogHistoryEpoch e{historyEpoch_, floor, -1, -1, 0};
    if (floor > 0 && !readAnchor_(floor - 1, e)) {
        if (epochWriteFailures_++ % 1024 == 0)
            spdlog::error("CatalogReadServer: the anchor batch at offset {} is unreadable — catalog reads wait "
                          "(failures so far: {})", floor - 1, epochWriteFailures_);
        return false;
    }
    if (!epochPath_.empty() && !writeCatalogHistoryEpoch(epochPath_, epochTmp_, epochDir_, e)) {
        if (epochWriteFailures_++ % 1024 == 0)
            spdlog::error("CatalogReadServer: writing the catalog history epoch to '{}' failed ({}) — catalog "
                          "reads wait until it is durable (failures so far: {})", epochPath_,
                          std::strerror(errno), epochWriteFailures_);
        return false;
    }
    durable_ = e;
    return true;
}

const char* CatalogReadServer::phaseName(int64_t subscriptionId) const noexcept {
    for (const Sub& s : subs_) {
        if (s.subscriptionId != subscriptionId) continue;
        if (std::holds_alternative<Answering>(s.phase)) return "Answering";
        if (std::holds_alternative<Opening>(s.phase)) return "Opening";
        if (std::holds_alternative<Backlog>(s.phase)) return "Backlog";
        if (std::holds_alternative<Live>(s.phase)) return "Live";
        if (std::holds_alternative<PausePending>(s.phase)) return "PausePending";
        if (std::holds_alternative<Paused>(s.phase)) return "Paused";
        return "Ending";
    }
    return "";
}

// ─── Ingress ──────────────────────────────────────────────────────────────────
void CatalogReadServer::subscribe(int64_t session, int64_t corr, int64_t fromOffset) {
    // A re-sent Subscribe is ONE subscription: re-answered, never a second stream.
    for (const Sub& s : subs_) {
        if (s.session != session || s.corr != corr) continue;
        if (std::holds_alternative<Answering>(s.phase)) return;   // its answer is still owed
        if (!sendSubscribeResponse_(session, corr, kOk, s.subscriptionId))
            owe_(OwedError{session, corr, kOk, OwedKind::SubscribeAnswer, s.subscriptionId});
        return;
    }
    if (subs_.size() >= maxSubs_) {
        ++refused_;
        spdlog::error("CatalogReadServer: catalog Subscribe (session {}, corr {}) refused — {} catalog "
                      "subscriptions already served (the cap) — answered Internal (refused so far: {})",
                      session, corr, subs_.size(), refused_);
        if (!owed_.empty() || !sendSubscribeResponse_(session, corr, kInternal, 0))
            owe_(OwedError{session, corr, kInternal, OwedKind::SubscribeAnswer, 0});
        return;
    }
    (void)checkTruncation_();
    raiseFloor_();
    const int64_t hw = servedHw_;   // C1: never a HW that regressed with a leader change
    int64_t next = 0;
    if (fromOffset == kFromEarliest) next = 0;
    else if (fromOffset < 0)         next = hw;                       // latest (−1) / any other sentinel
    else                             next = std::min(fromOffset, hw); // never ahead of the committed prefix
    acceptedFrom_[acceptedCount_++ % kAcceptedFromKept] = fromOffset;
    const int64_t id = kCatalogSubscriptionIdBase | idPrefix_ | ++nextId_;
    Sub s{session, corr, id, Answering{next}, nowNs_ + kProbeIntervalNs};
    if (owed_.empty() && sendSubscribeResponse_(session, corr, kOk, id)) s.phase = Opening{Backlog{next}};
    subs_.push_back(s);   // reserved: never allocates
    spdlog::debug("CatalogReadServer: catalog Subscribe session={} corr={} subId={:#x} from={} (HW {})", session,
                  corr, id, next, hw);
}

std::vector<int64_t> CatalogReadServer::acceptedFromOffsetsForTest() const {
    std::vector<int64_t> out;
    const uint64_t kept = std::min<uint64_t>(acceptedCount_, kAcceptedFromKept);
    for (uint64_t i = acceptedCount_ - kept; i < acceptedCount_; ++i) out.push_back(acceptedFrom_[i % kAcceptedFromKept]);
    return out;
}

void CatalogReadServer::unsubscribe(int64_t session, int64_t subscriptionId) {
    for (std::size_t i = 0; i < subs_.size();) {
        const Sub& s = subs_[i];
        if (s.session == session && (subscriptionId == -1 || s.subscriptionId == subscriptionId)) {
            // M1: a pause not yet acknowledged is acknowledged anyway — the
            // control thread's entry for it is cleared, never leaked.
            if (std::holds_alternative<PausePending>(s.phase) && subscriptionId != -1)
                owe_(OwedError{session, 0, 0, OwedKind::PauseAck, s.subscriptionId});
            subs_[i] = subs_.back();
            subs_.pop_back();
            continue;
        }
        ++i;
    }
    if (subscriptionId == -1)   // the session is gone: nothing owed to it is deliverable
        std::erase_if(owed_, [session](const OwedError& e) { return e.session == session; });
}

// ─── Flow control (fix round 0) ────────────────────────────────────────────────
void CatalogReadServer::onPaused(int64_t session, int64_t subscriptionId, int64_t baseOffset, bool droppedMarker) {
    // Task 13 fix round 2: a dropped NOTE (head kNoteHeadBase - cursor) — every
    // later frame of the stream was dropped with it: rewind to the note's cursor
    // and re-open (the note again).  Any other dropped frame: the client holds the
    // note already — resume without it.
    const bool    noteDropped = baseOffset <= kNoteHeadBase;
    const int64_t from        = noteDropped ? kNoteHeadBase - baseOffset : baseOffset;
    auto rewound = [&](const Resumable& cur) -> Resumable {
        if (const auto* b = std::get_if<Backlog>(&cur)) return Backlog{std::min(from, b->nextOffset)};
        const Live& l = std::get<Live>(cur);
        const int64_t at = std::min(from, l.nextOffset);
        // I1: the marker is re-sent only if IT was dropped, or a data frame
        // BEFORE it was (the marker behind it was in flight, dropped too).  A
        // dropped data frame at the marker's offset follows the marker.
        if ((droppedMarker && !noteDropped) || at < l.markedAt) return Backlog{at};
        return Live{at, l.markedAt};
    };
    auto resumption = [&](const Resumable& r) -> Resumption {
        if (noteDropped) return Opening{r};
        return std::visit([](const auto& x) -> Resumption { return x; }, r);
    };
    for (Sub& s : subs_) {
        if (s.session != session || s.subscriptionId != subscriptionId) continue;
        Resumption resumeAs{Backlog{0}};
        if (const auto* b = std::get_if<Backlog>(&s.phase))      resumeAs = resumption(rewound(*b));
        else if (const auto* l = std::get_if<Live>(&s.phase))    resumeAs = resumption(rewound(*l));
        else if (const auto* o = std::get_if<Opening>(&s.phase)) resumeAs = Opening{rewound(o->then)};
        else break;   // Answering (no Records sent yet) / already paused: just (re-)acknowledge
        s.phase = PausePending{resumeAs};
        if (owed_.empty() && sendPauseAck_(session, subscriptionId)) s.phase = Paused{resumeAs};
        return;
    }
    // Unknown (unsubscribed meanwhile) or not pausable: the ack still clears the
    // control thread's entry for it.
    if (!owed_.empty() || !sendPauseAck_(session, subscriptionId))
        owe_(OwedError{session, 0, 0, OwedKind::PauseAck, subscriptionId});
}

void CatalogReadServer::onResume(int64_t session, int64_t subscriptionId) {
    for (Sub& s : subs_) {
        if (s.session != session || s.subscriptionId != subscriptionId) continue;
        if (const auto* p = std::get_if<Paused>(&s.phase))
            s.phase = std::visit([](const auto& r) -> Phase { return r; }, p->resumeAs);
        return;
    }
}

// ─── Egress ───────────────────────────────────────────────────────────────────
bool CatalogReadServer::sendReliable_(int64_t session, std::size_t n) {
    return send_(CatalogReadFrameHead{session, 0, -1}, std::span<const uint8_t>(frameBuf_.data(), n));
}

bool CatalogReadServer::sendEpochFrame_(const Sub& s, int64_t recordsBaseOffset, int64_t rewindTo) {
    uint8_t epoch[kCatalogHistoryEpochBytes];
    for (std::size_t i = 0; i < kCatalogHistoryEpochBytes; ++i) epoch[i] = static_cast<uint8_t>(historyEpoch_ >> (8 * i));
    pc::Records r;
    r.sessionId      = s.session;
    r.correlationId  = s.corr;
    r.subscriptionId = s.subscriptionId;
    r.topicId        = kCatalogTopicId;
    r.partition      = kCatalogPartition;
    r.baseOffset     = recordsBaseOffset;
    r.recordBatch    = std::span<const uint8_t>(epoch, kCatalogHistoryEpochBytes);
    const std::size_t n = r.encode(frameBuf_.data());
    return send_(CatalogReadFrameHead{s.session, s.subscriptionId, rewindTo}, std::span<const uint8_t>(frameBuf_.data(), n));
}

bool CatalogReadServer::sendPauseAck_(int64_t session, int64_t subId) {
    return send_(CatalogReadFrameHead{session, subId, -1}, {});
}

bool CatalogReadServer::sendSubscribeResponse_(int64_t session, int64_t corr, int16_t code, int64_t subId) {
    pc::SubscribeResponse r;
    r.sessionId      = session;
    r.correlationId  = corr;
    r.errorCode      = code;
    r.partition      = kCatalogPartition;
    r.subscriptionId = subId;
    return sendReliable_(session, r.encode(frameBuf_.data()));
}

bool CatalogReadServer::sendError_(const OwedError& e) {
    if (e.kind == OwedKind::PauseAck) return sendPauseAck_(e.session, e.subscriptionId);
    if (e.kind == OwedKind::SubscribeAnswer) return sendSubscribeResponse_(e.session, e.corr, e.code, e.subscriptionId);
    pc::ErrorResponse r;
    r.sessionId     = e.session;
    r.correlationId = e.corr;
    r.code          = e.code;
    return sendReliable_(e.session, r.encode(frameBuf_.data()));
}

bool CatalogReadServer::sendRecords_(const Sub& s, int64_t baseOffset, std::span<const uint8_t> batches) {
    pc::Records r;
    r.sessionId      = s.session;
    r.correlationId  = s.corr;
    r.subscriptionId = s.subscriptionId;
    r.topicId        = kCatalogTopicId;
    r.partition      = kCatalogPartition;
    r.baseOffset     = baseOffset;
    r.recordBatch    = batches;
    const std::size_t n = r.encode(frameBuf_.data());   // <= kCatalogReadMaxFrameBytes by construction
    return send_(CatalogReadFrameHead{s.session, s.subscriptionId, baseOffset},
                 std::span<const uint8_t>(frameBuf_.data(), n));
}

int CatalogReadServer::flushOwed_() {
    std::size_t sent = 0;
    while (sent < owed_.size() && sendError_(owed_[sent])) ++sent;
    if (sent != 0) owed_.erase(owed_.begin(), owed_.begin() + static_cast<std::ptrdiff_t>(sent));
    return static_cast<int>(sent);
}

// The floor rises only from the replica's committed HW (never the applier's
// applied offset: after a truncation that would lift it back over the cut).
void CatalogReadServer::raiseFloor_() noexcept {
    const int64_t hw = catalog_.highWatermark();
    if (hw > servedHw_) servedHw_ = hw;
    if (floorEpoch_ < 0) floorEpoch_ = catalog_.epoch();
}

// N2: the replica's log generation catches a truncate-and-regrow inside one
// replica pass (the LEO alone would be back above the floor); the LEO check is
// the backstop for a log cut under the replica.  Once per truncation.
bool CatalogReadServer::checkTruncation_() {
    int64_t cut = INT64_MAX;
    const uint64_t gen = catalog_.logGeneration();
    if (gen != seenLogGen_) {
        // More than one change since the last look: an earlier cut may have been
        // lower than the last one recorded — assume the whole history changed.
        cut = gen == seenLogGen_ + 1 ? catalog_.lastTruncationOffset() : 0;
        seenLogGen_ = gen;
    }
    cut = std::min(cut, catalog_.logEndOffset());
    // What was served under the current epoch: this run's floor, and — across a
    // restart — the floor the epoch file recorded.
    int64_t floor = servedHw_;
    if (durable_ && durable_->epoch == historyEpoch_) floor = std::max(floor, durable_->servedFloor);
    if (cut >= floor) return false;   // only uncommitted bytes changed (the normal follower case)
    spdlog::critical("CatalogReadServer: the __catalog log was cut to {} — below the committed offset {} already "
                     "served (an unclean / deficient election upstream) — ending every catalog subscription once "
                     "(NotLeader: resubscribe); the floor restarts at the new committed history (epoch {})",
                     cut, floor, catalog_.epoch());
    for (std::size_t i = 0; i < subs_.size(); ++i) {
        if (std::holds_alternative<Ending>(subs_[i].phase)) continue;
        ++truncationEnds_;
        end_(i, kNotLeader, "the log was truncated below a served high watermark", cut);
    }
    std::erase_if(subs_, [](const Sub& x) { return std::holds_alternative<Ending>(x.phase); });
    servedHw_   = std::min(catalog_.highWatermark(), cut);
    floorEpoch_ = catalog_.epoch();
    // A new committed history: a new history epoch, durable at once (subscribed or
    // not) — a crash from here on can never serve the regrown log under the old one.
    bumpEpoch_("the log was truncated below a served high watermark");
    (void)ensureDurable_(servedHw_);   // a failure is retried before anything is served
    return true;
}

void CatalogReadServer::owe_(const OwedError& e) {
    if (owed_.size() >= owedCapacity_) [[unlikely]] {
        // Unreachable by the bounds above (intakePaused gates the only open-ended
        // source); never a silent loss — but never growth either.
        spdlog::critical("CatalogReadServer: {} owed catalog answers (the capacity) — answer to session {} "
                         "corr {} not queued", owed_.size(), e.session, e.corr);
        return;
    }
    owed_.push_back(e);   // reserved: never allocates
}

// The subscription becomes Ending (removed after the pass — M2 keeps indices
// stable); its explicit answer is sent or owed.
void CatalogReadServer::end_(std::size_t index, int16_t code, const char* why, int64_t at) {
    Sub& s = subs_[index];
    ++ended_;
    spdlog::error("CatalogReadServer: catalog subscription {:#x} (session {}) ended at offset {} — {} — answered "
                  "{} on its Subscribe (ended so far: {})", s.subscriptionId, s.session, at, why, code, ended_);
    if (std::holds_alternative<PausePending>(s.phase))   // M1: its pause still needs its ack
        owe_(OwedError{s.session, 0, 0, OwedKind::PauseAck, s.subscriptionId});
    s.phase = Ending{};
    const OwedError e{s.session, s.corr, code, OwedKind::EndedAnswer, s.subscriptionId};
    if (!owed_.empty() || !sendError_(e)) owe_(e);
}

// ─── pump ─────────────────────────────────────────────────────────────────────
// One read of [nextOffset, hw) (whole batches, never past the HW), sent as
// Records frames of coalesced batches.  nextOffset advances past what was sent.
CatalogReadServer::PushResult CatalogReadServer::pushRange_(Sub& s, int64_t& nextOffset, int64_t hw) {
    namespace st = rune::storage;
    namespace b  = rune::protocol::batch;
    constexpr PushResult kBlocked{Push::Blocked};
    if (nextOffset >= hw) return PushResult{Push::Idle};
    st::Log* log = catalog_.log();
    if (log == nullptr) return PushResult{Push::Ended, kInternal, "the __catalog replica has no log"};

    st::ReadIntoStatus status = st::ReadIntoStatus::Ok;
    int err = 0;
    const auto maxBytes = static_cast<int32_t>(readBuf_.size() / 2);
    const st::LogReadView v = log->readInto(nextOffset, maxBytes, readBuf_.data(), readBuf_.size(), status, hw, &err);
    switch (status) {
    case st::ReadIntoStatus::Ok:            break;
    case st::ReadIntoStatus::FirstTooLarge: return PushResult{Push::Ended, kMessageTooLarge,
                                                              "a stored batch larger than the read buffer"};
    case st::ReadIntoStatus::Absent:        return PushResult{Push::Ended, kInternal, "no batch at a committed offset"};
    case st::ReadIntoStatus::Unreadable:    return PushResult{Push::Ended, kInternal, "torn or corrupt bytes"};
    case st::ReadIntoStatus::IoError:       return PushResult{Push::Ended, kInternal, "an I/O error"};
    }
    if (v.empty()) return PushResult{Push::Ended, kInternal, "an empty read below the HW"};

    std::size_t pos = 0, frameStart = 0, frameLen = 0;
    int64_t frameBase = 0, frameLast = -1;
    bool any = false, atBound = false;
    auto flush = [&]() -> bool {
        if (frameLen == 0) return true;
        if (!sendRecords_(s, frameBase, std::span<const uint8_t>(v.data + frameStart, frameLen))) return false;
        nextOffset = frameLast + 1;
        frameLen   = 0;
        return true;
    };
    while (pos + static_cast<std::size_t>(b::kRecordBatchOverhead) <= v.len) {
        rune::protocol::RecordBatchReader r(v.data + pos, v.len - pos);
        const int32_t tl = r.totalLength();
        if (tl < b::kRecordBatchOverhead || pos + static_cast<std::size_t>(tl) > v.len) break;   // a cut tail
        const int64_t base = r.baseOffset();
        const int64_t last = base + r.lastOffsetDelta();
        if (base >= hw || last >= hw) { atBound = true; break; }   // committed data only
        if (last < nextOffset) { pos += static_cast<std::size_t>(tl); continue; }
        any = true;
        if (!r.ensureValid()) {
            if (!flush()) return kBlocked;
            return PushResult{Push::Ended, kInternal, "a batch fails its CRC / length check"};
        }
        if (r.isControl()) {   // never a catalog event: its offsets are consumed, never delivered
            if (!flush()) return kBlocked;
            nextOffset = last + 1;
            pos += static_cast<std::size_t>(tl);
            continue;
        }
        const auto len = static_cast<std::size_t>(tl);
        if (len > kCatalogReadMaxBatchBytes) {
            if (!flush()) return kBlocked;
            return PushResult{Push::Ended, kMessageTooLarge, "a stored batch larger than one Records frame"};
        }
        if (frameLen != 0 && frameLen + len > kCatalogReadCoalesceBytes && !flush()) return kBlocked;
        if (frameLen == 0) { frameStart = pos; frameBase = base; }
        frameLen += len;
        frameLast = last;
        pos += len;
    }
    if (!flush()) return kBlocked;
    if (!any && atBound) return PushResult{Push::Idle};   // the next batch is not wholly committed yet
    if (!any) return PushResult{Push::Ended, kInternal, "a read below the HW yields no batch"};
    return PushResult{Push::Progress};
}

int CatalogReadServer::pump(int64_t nowNs) {
    nowNs_ = nowNs;
    int work = pump();
    if (!subs_.empty() && owed_.empty()) work += probe_(nowNs);
    return work;
}

// Fix round 3 (F3b): at most one probe per subscription per interval; a full
// ring keeps it due (retried next pass).  No allocation.
int CatalogReadServer::probe_(int64_t nowNs) {
    int sent = 0;
    for (Sub& s : subs_) {
        if (nowNs < s.probeDueNs) continue;
        if (!send_(CatalogReadFrameHead{s.session, s.subscriptionId, kProbeOffset}, {})) break;
        s.probeDueNs = nowNs + kProbeIntervalNs;
        ++sent;
    }
    return sent;
}

int CatalogReadServer::pump() {
    int work = 0;
    if (!owed_.empty()) [[unlikely]] {
        work += flushOwed_();
        if (!owed_.empty()) return work;   // the egress is full: nothing else fits this pass
    }
    // C1: the committed floor is monotonic.  A replica whose log now ends below
    // a HW already served truncated committed data (only an unclean / deficient
    // election upstream can): every subscription ends EXPLICITLY with the
    // retriable NotLeader — the client resubscribes — never served past the
    // truncation, never rewound silently.  Checked with no subscription too: the
    // history epoch must change (durably) whatever is subscribed.
    if (checkTruncation_()) [[unlikely]] return work + 1;
    if (subs_.empty()) return work;
    raiseFloor_();
    const int64_t hw = servedHw_;
    // Nothing is served under an epoch, or above the floor it recorded, before
    // that is durable (a restart then never re-uses the epoch for another history).
    if (!ensureDurable_(hw)) [[unlikely]] return work;
    // The marker claims "the whole committed prefix": only while this replica's
    // HW is current (a leader, or a follower its current leader answered) —
    // during a leader change the backlog is still served up to the floor.
    const bool hwCurrent = catalog_.committedHwKnown();

    // M2: round-robin.
    const std::size_t n = subs_.size();
    const std::size_t first = rr_ % n;
    for (std::size_t k = 0; k < n; ++k) {
        const std::size_t i = (first + k) % n;
        Sub& s = subs_[i];
        if (auto* a = std::get_if<Answering>(&s.phase)) {
            if (!sendSubscribeResponse_(s.session, s.corr, kOk, s.subscriptionId)) break;
            ++work;
            s.phase = Opening{Backlog{a->nextOffset}};
        }
        if (auto* pp = std::get_if<PausePending>(&s.phase)) {
            if (!sendPauseAck_(s.session, s.subscriptionId)) break;
            ++work;
            s.phase = Paused{pp->resumeAs};
        }
        if (auto* o = std::get_if<Opening>(&s.phase)) {
            // The stream's history-epoch note: before every Records frame of it.
            const int64_t at = std::visit([](const auto& r) { return r.nextOffset; }, o->then);
            if (!sendEpochFrame_(s, -1, kNoteHeadBase - at)) break;
            ++work;
            s.phase = std::visit([](const auto& r) -> Phase { return r; }, o->then);
        }
        if (std::holds_alternative<Paused>(s.phase) || std::holds_alternative<Ending>(s.phase)) continue;
        int64_t* next = std::holds_alternative<Backlog>(s.phase) ? &std::get<Backlog>(s.phase).nextOffset
                                                                 : &std::get<Live>(s.phase).nextOffset;
        const int64_t before = *next;
        const PushResult r = pushRange_(s, *next, hw);
        if (r.kind == Push::Blocked) {
            work += *next != before ? 1 : 0;
            break;
        }
        if (r.kind == Push::Ended) {
            end_(i, r.code, r.why, *next);
            ++work;
            continue;
        }
        if (r.kind == Push::Progress) ++work;
        if (auto* bl = std::get_if<Backlog>(&s.phase); bl != nullptr && bl->nextOffset >= hw && hwCurrent) {
            // The caught-up marker (ruling 10): one empty Records at the HW — the
            // Backlog → Live transition is the "once".
            if (!sendEpochFrame_(s, bl->nextOffset, bl->nextOffset)) break;
            s.phase = Live{bl->nextOffset, bl->nextOffset};
            ++work;
        }
    }
    // Every pass starts one subscription later (rr_ += 1), blocked or not: a
    // full ring never pins the order (the blocked one keeps its cursor).
    rr_ = (rr_ % n) + 1;
    std::erase_if(subs_, [](const Sub& x) { return std::holds_alternative<Ending>(x.phase); });   // no allocation
    return work;
}

} // namespace rune::node
