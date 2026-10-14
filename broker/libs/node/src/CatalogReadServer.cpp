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
#include <cstring>
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
}

const char* CatalogReadServer::phaseName(int64_t subscriptionId) const noexcept {
    for (const Sub& s : subs_) {
        if (s.subscriptionId != subscriptionId) continue;
        if (std::holds_alternative<Answering>(s.phase)) return "Answering";
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
    noteCommittedFloor(catalog_.highWatermark());
    const int64_t hw = servedHw_;   // C1: never a HW that regressed with a leader change
    int64_t next = 0;
    if (fromOffset == kFromEarliest) next = 0;
    else if (fromOffset < 0)         next = hw;                       // latest (−1) / any other sentinel
    else                             next = std::min(fromOffset, hw); // never ahead of the committed prefix
    const int64_t id = kCatalogSubscriptionIdBase | idPrefix_ | ++nextId_;
    Sub s{session, corr, id, Answering{next}};
    if (owed_.empty() && sendSubscribeResponse_(session, corr, kOk, id)) s.phase = Backlog{next};
    subs_.push_back(s);   // reserved: never allocates
    spdlog::debug("CatalogReadServer: catalog Subscribe session={} corr={} subId={:#x} from={} (HW {})", session,
                  corr, id, next, hw);
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
    for (Sub& s : subs_) {
        if (s.session != session || s.subscriptionId != subscriptionId) continue;
        Resumable resumeAs{Backlog{0}};
        if (const auto* b = std::get_if<Backlog>(&s.phase)) {
            resumeAs = Backlog{std::min(baseOffset, b->nextOffset)};
        } else if (const auto* l = std::get_if<Live>(&s.phase)) {
            const int64_t at = std::min(baseOffset, l->nextOffset);
            // I1: the marker is re-sent only if IT was dropped, or a data frame
            // BEFORE it was (the marker behind it was in flight, dropped too).  A
            // dropped data frame at the marker's offset follows the marker.
            if (droppedMarker || at < l->markedAt) resumeAs = Backlog{at};
            else                                   resumeAs = Live{at, l->markedAt};
        } else {
            break;   // Answering (no Records sent yet) / already paused: just (re-)acknowledge
        }
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

int CatalogReadServer::pump() {
    int work = 0;
    if (!owed_.empty()) [[unlikely]] {
        work += flushOwed_();
        if (!owed_.empty()) return work;   // the egress is full: nothing else fits this pass
    }
    if (subs_.empty()) return work;

    // C1: the committed floor is monotonic.  A replica whose log now ends below
    // a HW already served truncated committed data (only an unclean / deficient
    // election upstream can): every subscription ends EXPLICITLY with the
    // retriable NotLeader — the client resubscribes — never served past the
    // truncation, never rewound silently.
    const int64_t leo = catalog_.logEndOffset();
    if (leo < servedHw_) [[unlikely]] {
        spdlog::critical("CatalogReadServer: the __catalog log ends at {} — below the committed offset {} already "
                         "served — ending every catalog subscription (NotLeader: resubscribe)", leo, servedHw_);
        for (std::size_t i = 0; i < subs_.size(); ++i) {
            ++truncationEnds_;
            end_(i, kNotLeader, "the log was truncated below a served high watermark", leo);
        }
        std::erase_if(subs_, [](const Sub& x) { return std::holds_alternative<Ending>(x.phase); });
        servedHw_ = std::min(catalog_.highWatermark(), leo);
        return work + 1;
    }
    noteCommittedFloor(catalog_.highWatermark());
    const int64_t hw = servedHw_;
    // The marker claims "the whole committed prefix": only while this replica's
    // HW is current (a leader, or a follower its current leader answered) —
    // during a leader change the backlog is still served up to the floor.
    const bool hwCurrent = catalog_.committedHwKnown();

    // M2: round-robin.
    const std::size_t n = subs_.size();
    std::size_t first = rr_ % n;
    bool blocked = false;
    for (std::size_t k = 0; k < n && !blocked; ++k) {
        const std::size_t i = (first + k) % n;
        Sub& s = subs_[i];
        if (auto* a = std::get_if<Answering>(&s.phase)) {
            if (!sendSubscribeResponse_(s.session, s.corr, kOk, s.subscriptionId)) { blocked = true; first = i; break; }
            ++work;
            s.phase = Backlog{a->nextOffset};
        }
        if (auto* pp = std::get_if<PausePending>(&s.phase)) {
            if (!sendPauseAck_(s.session, s.subscriptionId)) { blocked = true; first = i; break; }
            ++work;
            s.phase = Paused{pp->resumeAs};
        }
        if (std::holds_alternative<Paused>(s.phase) || std::holds_alternative<Ending>(s.phase)) continue;
        int64_t* next = std::holds_alternative<Backlog>(s.phase) ? &std::get<Backlog>(s.phase).nextOffset
                                                                 : &std::get<Live>(s.phase).nextOffset;
        const int64_t before = *next;
        const PushResult r = pushRange_(s, *next, hw);
        if (r.kind == Push::Blocked) {
            work += *next != before ? 1 : 0;
            blocked = true;
            first = i;
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
            if (!sendRecords_(s, bl->nextOffset, {})) { blocked = true; first = i; break; }
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
