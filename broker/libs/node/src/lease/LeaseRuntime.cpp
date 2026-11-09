// LeaseRuntime.cpp - lease topics Task 10: see LeaseRuntime.h.
#include <rune/node/lease/LeaseRuntime.h>

#include <rune/node/lease/LeaseIndex.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <exception>
#include <new>
#include <string_view>

namespace rune::node::lease {

namespace {
using rune::replication::PartitionReplica;

constexpr uint16_t kCodeLeaseNotFound = static_cast<uint16_t>(rune::protocol::client::ErrorCode::LeaseNotFound);
constexpr uint16_t kCodeThrottled     = static_cast<uint16_t>(rune::protocol::client::ErrorCode::Throttled);
constexpr uint16_t kCodeNotLeader     = static_cast<uint16_t>(rune::protocol::client::ErrorCode::NotLeader);

/// A storage failure surfaced by appendLeaseBatch (it throws): handled as a refusal of the whole batch.
constexpr int64_t kAppendThrew = INT64_MIN;

enum class Fit : uint8_t { Ok, Full, Oversized };
/// Whether the record (key, value of `len` bytes) as record number `n` of the batch under construction fits.  EXACT sizes
/// (builtRecordBytes), never a worst case: a lone record over `byteCap` still goes out when it fits `hardCap`.
[[nodiscard]] inline Fit fitRecord(const rune::protocol::RecordBatchBuilder& b, uint32_t n, std::size_t keyLen, std::size_t len, std::size_t byteCap,
                                   std::size_t hardCap) noexcept {
    const std::size_t rec = rune::protocol::builtRecordBytes(static_cast<int64_t>(n), keyLen, len);
    const std::size_t ovh = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    if (b.bodyBytes() + rec + ovh <= byteCap) return Fit::Ok;
    if (n > 0) return Fit::Full;
    return ovh + rec <= hardCap ? Fit::Ok : Fit::Oversized;
}

/// appendLeaseBatch, with a throw turned into kAppendThrew.
[[nodiscard]] inline int64_t appendBatch(PartitionReplica& replica, std::span<const uint8_t> batch, LeaseRuntimeStats& stats, const char* what) {
    try {
        return replica.appendLeaseBatch(batch);
    } catch (const std::exception& ex) {
        ++stats.appendErrors;
        if ((stats.appendErrors & (stats.appendErrors - 1)) == 0) spdlog::warn("lease runtime: {} append threw: {} (error {})", what, ex.what(), stats.appendErrors);
    } catch (...) {
        ++stats.appendErrors;
        if ((stats.appendErrors & (stats.appendErrors - 1)) == 0) spdlog::warn("lease runtime: {} append threw (error {})", what, stats.appendErrors);
    }
    return kAppendThrew;
}

/// Encodes `row` at the writer for a Checkpoint record and stores `remainingMs` in its lease_remaining_ms column.  The value length, or 0
/// when the row cannot be encoded (or the value is too short to carry the column).
[[nodiscard]] inline std::size_t encodeCheckpointValue(LeaseEmitWork& work, const uint8_t* row, const LeaseIndexRef& ref, const LeaseWriter& writer,
                                                       const uint16_t* slotOf, uint64_t remainingMs) noexcept {
    namespace rec = rune::protocol::record;
    const std::size_t len = encodeLeaseValue(row, *ref.layout, *ref.outDesc, *writer.descriptor, writer.version, work.valueTmp.data(),
                                             work.valueTmp.size(), slotOf);
    if (len < rec::kLeaseRemainingValueOffset + 8) return 0;
    for (int i = 0; i < 8; ++i) work.valueTmp[rec::kLeaseRemainingValueOffset + static_cast<std::size_t>(i)] = static_cast<uint8_t>(remainingMs >> (8 * i));
    return len;
}

/// ceil_ms(deadline - now) for a deadline after `nowNs` (the remaining time a Checkpoint(R) carries).
[[nodiscard]] inline uint64_t ceilRemainingMs(int64_t deadlineNs, int64_t nowNs) noexcept {
    const int64_t diff = deadlineNs - nowNs;
    return static_cast<uint64_t>(diff / 1'000'000) + (diff % 1'000'000 != 0 ? 1u : 0u);
}

[[nodiscard]] inline bool inflight(LeaseEntryState s) noexcept {
    return s == LeaseEntryState::Checkpointing || s == LeaseEntryState::Clearing || s == LeaseEntryState::Emitting;
}
}  // namespace

bool LeaseRuntime::coverOrCount_(uint32_t rid, bool& growFailed) {
    if (timers_.covers(rid)) return true;
    try {
        if (reserveFor_(rid)) return true;
    } catch (const std::bad_alloc&) {
        ++stats_.growFailures;
        growFailed = true;
        return false;
    }
    ++stats_.ridPastMaxKeys;
    return false;
}

void LeaseRuntime::scheduleIfNone_(uint32_t rid, uint64_t ttlMs, int64_t leaseNowNs, const rune::protocol::LeaseSpec& spec) noexcept {
    if (timers_.entry(rid).checkpointNs != kLeaseNoCheckpoint) return;
    const int64_t at = checkpointTime(spec, ttlMs, leaseNowNs);
    if (at != kLeaseNoCheckpoint) timers_.scheduleCheckpoint(rid, at);
}

uint32_t LeaseRuntime::checkpointTokens_(const rune::protocol::LeaseSpec& spec, int64_t leaseNowNs) noexcept {
    const int64_t window = leaseNowNs > readyNs_ ? (leaseNowNs - readyNs_) / 1'000'000'000 : 0;
    if (window != ckptWindow_) {
        ckptWindow_ = window;
        ckptUsed_   = 0;
    }
    return spec.checkpointRate > ckptUsed_ ? spec.checkpointRate - ckptUsed_ : 0;
}

bool LeaseRuntime::reserveFor_(uint32_t rid) {
    if (rid >= timers_.cap()) return false;
    const std::size_t need = (static_cast<std::size_t>(rid / kLeaseEntryChunk) + 1) * kLeaseEntryChunk;
    if (clearPos_.size() < need) clearPos_.resize(need, 0);
    return timers_.reserveFor(rid);
}

void LeaseRuntime::growIfOwed_() {
    if (!timers_.growOwed()) return;
    const std::size_t need = std::min<std::size_t>(timers_.cap(), static_cast<std::size_t>(timers_.capacity()) + kLeaseEntryChunk);
    if (clearPos_.size() < need) clearPos_.resize(need, 0);
    timers_.growIfOwed();
}

void LeaseRuntime::compactClears_() noexcept {
    uint64_t w = clearHead_;
    for (uint64_t r = clearHead_; r != clearTail_; ++r) {
        const uint32_t rid = clearRing_[r & clearMask_];
        uint8_t& f = timers_.flags(rid);
        if ((f & kLeaseClearWanted) == 0 || clearPos_[rid] != static_cast<uint32_t>(r)) continue;   // stale: not the rid's live entry
        if (timers_.parkHead(rid) == kLeaseNil) {   // answered meanwhile: out of the ring, flag clear
            f = static_cast<uint8_t>(f & ~kLeaseClearWanted);
            continue;
        }
        clearPos_[rid] = static_cast<uint32_t>(w);   // writes go at w <= r: a later stale entry of this rid can never match it
        clearRing_[w++ & clearMask_] = rid;
    }
    clearTail_ = w;
}

bool LeaseRuntime::clearRoom_(uint32_t rid) noexcept {
    if ((timers_.flags(rid) & kLeaseClearWanted) != 0) return true;
    if (clearTail_ - clearHead_ <= clearMask_) return true;
    compactClears_();
    return clearTail_ - clearHead_ <= clearMask_;
}

bool LeaseRuntime::flagClear_(uint32_t rid) noexcept {
    if ((timers_.flags(rid) & kLeaseClearWanted) != 0) return true;
    if (!clearRoom_(rid)) return false;
    uint8_t& f = timers_.flags(rid);
    f = static_cast<uint8_t>(f | kLeaseClearWanted);
    clearPos_[rid] = static_cast<uint32_t>(clearTail_);
    clearRing_[clearTail_ & clearMask_] = rid;
    ++clearTail_;
    return true;
}

LeaseRuntime::KaStep LeaseRuntime::kaStep_(uint32_t rid, int64_t leaseNowNs, bool remainingPositive, uint64_t ttlMs, bool gateOpen,
                                           const rune::protocol::LeaseSpec& spec) noexcept {
    const LeaseEntry& e = timers_.entry(rid);
    const bool due      = e.deadlineNs <= leaseNowNs;
    const LeaseStepOut out = leaseStep({e.state, LeaseEntryEvent::Keepalive, gateOpen, due, remainingPositive});
    switch (out.action) {
        case LeaseAction::RearmAnswerOk:
            timers_.rearm(rid, satAdd(leaseNowNs, msToNsSat(ttlMs)));
            scheduleIfNone_(rid, ttlMs, leaseNowNs, spec);
            return {false, false, 0, ttlMs};
        case LeaseAction::AppendClear:   // the entry stays Armed: flushClears appends the Checkpoint(0)
            return {true, true, 0, 0};
        case LeaseAction::Park:
            // Armed and not due can only mean the gate is closed with remaining > 0: the clear path waits for it
            return {true, e.state == LeaseEntryState::Armed && !due, 0, 0};
        case LeaseAction::None:
        case LeaseAction::Link:
        case LeaseAction::AppendExpire:
        case LeaseAction::AppendCheckpoint:
        case LeaseAction::ReturnArmedReplayParked:
        case LeaseAction::ReturnArmedAnswerParkedOk:
        case LeaseAction::FreeAnswerParkedNotFound:
        case LeaseAction::DiscardAnswerParkedNotLeader:
        case LeaseAction::Unschedule:
        case LeaseAction::Defer:
        case LeaseAction::ReplayParked: break;
    }
    assert(false && "a Keepalive step produced an action the keepalive path does not own");
    return {false, false, kCodeLeaseNotFound, 0};
}

void LeaseRuntime::replay_(uint32_t rid, int64_t leaseNowNs, bool remainingPositive, uint64_t ttlMs, LeaseParkTable& parks,
                           const rune::protocol::LeaseSpec& spec) noexcept {
    uint32_t& head = timers_.parkHead(rid);
    if (head == kLeaseNil) return;
    // The gate is not consulted here: AppendClear and a gate-closed Park both leave the chain parked with the clear flag set, and
    // flushClears re-checks the gate before it appends.  The decision depends only on the entry's state and the row's values, so it is
    // taken ONCE for the whole chain.
    const KaStep st = kaStep_(rid, leaseNowNs, remainingPositive, ttlMs, /*gateOpen=*/true, spec);
    if (!st.park) parks.answerChain(head, st.code, st.ttlMs);
    else if (st.wantFlag && !flagClear_(rid)) parks.answerChain(head, kCodeThrottled, 1);   // a replay never takes a new slot; no ring room = Throttled
}

void LeaseRuntime::foldStep_(uint32_t rid, LeaseEntryEvent event, const LeaseEvent& ev, int64_t leaseNowNs, LeaseParkTable& parks,
                             const rune::protocol::LeaseSpec& spec) noexcept {
    const LeaseEntry& e = timers_.entry(rid);
    const LeaseEntryState from = e.state;
    const LeaseStepOut out = leaseStep({from, event, /*gateOpen=*/true, e.deadlineNs <= leaseNowNs, ev.remainingMs > 0});
    switch (out.action) {
        case LeaseAction::None:
            if (out.next != from) timers_.setState(rid, out.next);
            return;
        case LeaseAction::ReturnArmedAnswerParkedOk:   // Clearing: its own Checkpoint(0) folded
            timers_.setState(rid, out.next);
            timers_.rearm(rid, satAdd(leaseNowNs, msToNsSat(ev.ttlMs)));
            scheduleIfNone_(rid, ev.ttlMs, leaseNowNs, spec);
            parks.answerChain(timers_.parkHead(rid), 0, ev.ttlMs);
            return;
        case LeaseAction::FreeAnswerParkedNotFound:    // the key ended (its Expire folded, or a revoke): answered BEFORE Free resets the entry
            parks.answerChain(timers_.parkHead(rid), kCodeLeaseNotFound, 0);
            timers_.setState(rid, out.next);
            return;
        case LeaseAction::ReturnArmedReplayParked:
        case LeaseAction::ReplayParked:
            if (out.next != from) timers_.setState(rid, out.next);
            // A BrokerRejected event carries no row values: treat the remaining time as positive, so every non-due keepalive takes the
            // clear path, where flushClears reads the row (remaining 0: Ok at once; else it appends the Checkpoint(0)).
            if (event == LeaseEntryEvent::OwnRecordRejected) replay_(rid, leaseNowNs, /*remainingPositive=*/true, 0, parks, spec);
            else replay_(rid, leaseNowNs, ev.remainingMs > 0, ev.ttlMs, parks, spec);
            return;
        case LeaseAction::DiscardAnswerParkedNotLeader:
        case LeaseAction::Link:
        case LeaseAction::AppendExpire:
        case LeaseAction::AppendCheckpoint:
        case LeaseAction::AppendClear:
        case LeaseAction::Park:
        case LeaseAction::Unschedule:
        case LeaseAction::Defer:
        case LeaseAction::RearmAnswerOk: break;
    }
    assert(false && "a fold event produced an action only the keepalive path or a demotion owns");
}

void LeaseRuntime::onEvents(std::span<const LeaseEvent> events, int64_t leaseNowNs, LeaseParkTable& parks, const rune::protocol::LeaseSpec& spec,
                            const LeaseIndexRef* ref) noexcept {
    for (const LeaseEvent& ev : events) {
        const uint32_t rid = ev.rid;
        if (ev.kind == LeaseEventKind::Granted) {
            bool reserved = timers_.covers(rid);
            if (!reserved) {   // cold growth: one fold can add more grants than the 75 % headroom
                try {
                    reserved = reserveFor_(rid);
                } catch (const std::bad_alloc&) {
                    // The grant has no entry to arm, and a later link of its (live) row would use the PROMOTION deadline, which can be before
                    // g + ttl: an acked lease must never expire early.  The worker destroys this runtime after the fold (rebuildOwed) and the
                    // next one, created at a ready time >= g, promotes the row at a deadline >= g + extend + ttl.
                    ++stats_.growFailures;
                    rebuildOwed_ = true;
                    continue;
                }
            }
            if (!reserved) {
                ++stats_.ridPastMaxKeys;
                if (!ridPastWarned_) {
                    ridPastWarned_ = true;
                    spdlog::warn("lease runtime: topic {} partition {}: a lease row id ({}) is past maxKeys; the grant is not tracked (counted, further ones silent)",
                                 topicId_, partition_, rid);
                }
                assert(false && "a lease row id past maxKeys");
                continue;
            }
            if (timers_.entry(rid).state != LeaseEntryState::Free) {
                assert(false && "a Granted event found its entry armed or in flight");
                ++stats_.grantNotFree;
                parks.answerChain(timers_.parkHead(rid), kCodeLeaseNotFound, 0);
                timers_.setState(rid, LeaseEntryState::Free);
            }
            timers_.noteUse(rid);
            timers_.arm(rid, satAdd(leaseNowNs, msToNsSat(ev.ttlMs)), checkpointTime(spec, ev.ttlMs, leaseNowNs));
            continue;
        }
        if (ev.kind == LeaseEventKind::BrokerRejected) {
            ++stats_.brokerRejectedSeen;
            if (ev.rid == kLeaseNil || !timers_.covers(rid)) continue;
            const LeaseEntry& e = timers_.entry(rid);
            if (inflight(e.state) && e.inflightOffset == ev.offset) {
                // The refused record carries the version of the row it was built from.  A row that is gone, or that moved on to another
                // version, means the in-flight record is stale: the entry is Free (a new grant of the key arms it afresh), the parks answered.
                if (ref != nullptr && ref->table != nullptr && ref->layout != nullptr) {
                    uint32_t liveRid = 0;   // the row id of the key this rid held: gone (erased) or reused by another row both read as moved on
                    const uint8_t* const row = ref->table->find(rune::view::ViewKey::fromBytes(ref->table->keyOf(rid)), liveRid);
                    if (row == nullptr || liveRid != rid || leaseRowVersion(row, *ref->layout) != ev.version) {
                        parks.answerChain(timers_.parkHead(rid), kCodeLeaseNotFound, 0);
                        timers_.setState(rid, LeaseEntryState::Free);
                        ++stats_.rejectedStale;
                        continue;
                    }
                }
                // Back to Armed at the stored deadline (relinked: past due means due on the next poll, rebuilt from the current row).  A rejected
                // Checkpoint(R) leaves the stored checkpoint time in the past: reschedule it one interval from now BEFORE the relink (as the
                // own-fold path does), else the next pass would append the same checkpoint again.
                if (e.state == LeaseEntryState::Checkpointing && e.deadlineNs > leaseNowNs)
                    timers_.scheduleCheckpoint(rid, checkpointTime(spec, ceilRemainingMs(e.deadlineNs, leaseNowNs), leaseNowNs));
                const bool wasArmed = e.state == LeaseEntryState::Armed;
                foldStep_(rid, LeaseEntryEvent::OwnRecordRejected, ev, leaseNowNs, parks, spec);
                if (!wasArmed && timers_.entry(rid).state == LeaseEntryState::Armed) ++stats_.ownRecordRejected;
            }
            continue;   // otherwise a stale record of another leadership
        }
        const bool present = ev.kind == LeaseEventKind::Updated || ev.kind == LeaseEventKind::Checkpointed;   // the row is in the table: the event carries its values
        if (present && !timers_.covers(rid)) {   // a row created by a fold (an absent-row replay) past the entry store: cover it cold
            bool growFailed = false;
            if (!coverOrCount_(rid, growFailed)) {
                if (growFailed) rebuildOwed_ = true;   // as the Granted case: a Free entry on a live row would link at the promotion deadline
                continue;
            }
        }
        if (!timers_.covers(rid)) continue;
        const LeaseEntry& e = timers_.entry(rid);
        // A fold of a present row whose entry is Free (the promotion pass has not reached it, or the row was created by this fold) links it
        // first, at the promotion deadline the pass would have used, then the event is stepped as usual.
        if (present && e.state == LeaseEntryState::Free) {
            timers_.noteUse(rid);
            timers_.arm(rid, promotionDeadline(ev.ttlMs, ev.remainingMs), checkpointTime(spec, ev.remainingMs > 0 ? ev.remainingMs : ev.ttlMs, leaseNowNs));
        }
        switch (ev.kind) {
            case LeaseEventKind::Updated:
                foldStep_(rid, LeaseEntryEvent::OtherUpdateFolds, ev, leaseNowNs, parks, spec);
                break;
            case LeaseEventKind::Checkpointed: {
                const bool own = e.state == LeaseEntryState::Checkpointing && e.inflightOffset == ev.offset;
                // The entry's own Checkpoint(R) folded: its next checkpoint is one interval from now (while R still exceeds it).  Scheduled
                // BEFORE setState(Armed), which links the stored checkpoint time: else the old, past time would fire again at once.
                if (own) timers_.scheduleCheckpoint(rid, checkpointTime(spec, ev.remainingMs, leaseNowNs));
                foldStep_(rid, e.inflightOffset == ev.offset ? LeaseEntryEvent::OwnRecordFolds : LeaseEntryEvent::OtherUpdateFolds, ev, leaseNowNs, parks, spec);
                break;
            }
            case LeaseEventKind::Ended:
                foldStep_(rid, e.state == LeaseEntryState::Emitting && e.inflightOffset == ev.offset ? LeaseEntryEvent::OwnRecordFolds
                                                                                                    : LeaseEntryEvent::EndFolds, ev, leaseNowNs, parks, spec);
                break;
            case LeaseEventKind::Granted:
            case LeaseEventKind::BrokerRejected: break;   // handled above
        }
    }
    try {
        growIfOwed_();   // cold, once per cycle
    } catch (const std::bad_alloc&) {
        timers_.noteGrowFailed();   // growIfOwed cleared growOwed_ before it allocated: owe it again, the next cycle retries
        ++stats_.growFailures;
    }
}

LeaseKaOutcome LeaseRuntime::keepAlive(const LeaseIndexRef& ref, std::span<const uint8_t> key, int64_t version, int64_t judgedThrough,
                                       int64_t leaseNowNs, LeaseParkTable& parks, uint32_t& req, int64_t sessionId, int64_t correlationId,
                                       uint16_t index, bool owedRoom, const rune::protocol::LeaseSpec& spec) {
    if (ref.table == nullptr || ref.layout == nullptr || key.size() != ref.table->keyWidth()) return {false, kCodeLeaseNotFound, 0};   // a wrong-width key is no key
    uint32_t rid = 0;
    const uint8_t* const row = ref.table->find(rune::view::ViewKey::fromBytes(key), rid);
    if (row == nullptr || leaseRowVersion(row, *ref.layout) != version) return {false, kCodeLeaseNotFound, 0};
    try {
        if (!timers_.covers(rid) && !reserveFor_(rid)) return {false, kCodeThrottled, 1};   // cold growth
    } catch (const std::bad_alloc&) {
        ++stats_.growFailures;
        return {false, kCodeThrottled, 1};
    }
    const uint64_t ttl       = leaseRowTtl(row, *ref.layout);
    const uint64_t remaining = leaseRowRemaining(row, *ref.layout);
    if (timers_.entry(rid).state == LeaseEntryState::Free) {   // a row the promotion pass has not linked yet: armed at its promotion deadline
        timers_.noteUse(rid);
        timers_.arm(rid, promotionDeadline(ttl, remaining), checkpointTime(spec, remaining > 0 ? remaining : ttl, leaseNowNs));
    }
    const bool gateOpen = leaseLastAppended(*ref.table, rid) < judgedThrough;
    LeaseKaOutcome out{false, 0, 0};
    // The park path must not change the entry when it cannot park: decide with a dry run first, then commit.
    const LeaseEntry& e = timers_.entry(rid);
    const bool due = e.deadlineNs <= leaseNowNs;
    const LeaseStepOut peek = leaseStep({e.state, LeaseEntryEvent::Keepalive, gateOpen, due, remaining > 0});
    if (peek.action == LeaseAction::Park || peek.action == LeaseAction::AppendClear) {
        const bool needFlag = peek.action == LeaseAction::AppendClear || (e.state == LeaseEntryState::Armed && !due);
        // The dry run: a park that needs a clear-ring slot and finds none (after a compaction) is Throttled, never parked unflagged.
        if (!owedRoom || parks.full() || (needFlag && !clearRoom_(rid)) ||
            (req == kLeaseNil && (req = parks.openRequest(sessionId, correlationId)) == kLeaseNil)) {
            out = {false, kCodeThrottled, 1};
        } else {
            const uint32_t slot = parks.park(req, topicId_, partition_, rid, index, timers_.parkHead(rid));
            assert(slot != kLeaseNil);
            (void)slot;
            if (needFlag) {
                [[maybe_unused]] const bool flagged = flagClear_(rid);   // room was checked above
                assert(flagged);
            }
            out = {true, 0, 0};
        }
    } else {
        const KaStep st = kaStep_(rid, leaseNowNs, remaining > 0, ttl, gateOpen, spec);
        assert(!st.park);
        out = {false, st.code, st.ttlMs};
    }
    try {
        growIfOwed_();   // cold
    } catch (const std::bad_alloc&) {
        // growIfOwed cleared growOwed_ BEFORE it allocated (LeaseEntryStore.h): owe the growth again so the next call retries.
        timers_.noteGrowFailed();
        ++stats_.growFailures;
    }
    return out;
}

uint32_t LeaseRuntime::emit(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough, int64_t leaseNowNs,
                            std::size_t byteCap, std::size_t hardCap, uint32_t maxRecords, PartitionReplica& replica) {
    namespace rec = rune::protocol::record;
    if (writer.descriptor == nullptr || ref.table == nullptr) return 0;
    // The writer's slot map, once per emit: a writer the layout cannot cover encodes nothing (no row is polled for it).
    const uint16_t* const slotOf = ref.cover != nullptr
        ? leaseSlotMapFor(*ref.cover, writer.version, writer.installSeq, *ref.layout, *ref.outDesc, *writer.descriptor) : nullptr;
    if (ref.cover != nullptr && slotOf == nullptr) {
        ++stats_.encodeFailures;
        return 0;
    }
    maxRecords = std::min<uint32_t>(maxRecords, rune::protocol::kMaxLeaseExpiryBatch);
    work.builder.reset(0, 0);
    uint32_t n = 0;
    bool full  = false;
    auto sink = [&](uint32_t rid) -> bool {
        if (full) return false;
        assert(timers_.entry(rid).state == LeaseEntryState::Armed);   // only Armed is linked
        if (leaseLastAppended(*ref.table, rid) >= judgedThrough) {   // an update or revoke of the key is appended but not judged: the re-encode would miss it
            ++stats_.expiryDeferred;
            return false;
        }
        const uint8_t* row = ref.table->rowOf(rid);
        const std::span<const uint8_t> key = ref.table->keyOf(rid);
        const std::size_t len = encodeLeaseValue(row, *ref.layout, *ref.outDesc, *writer.descriptor, writer.version, work.valueTmp.data(),
                                                 work.valueTmp.size(), slotOf);
        if (len == 0) {
            ++stats_.encodeFailures;
            return false;
        }
        switch (fitRecord(work.builder, n, key.size(), len, byteCap, hardCap)) {
            case Fit::Ok: break;
            case Fit::Full: full = true; return false;
            case Fit::Oversized: ++stats_.oversizedDeferred; return false;   // only this row waits; the others go on
        }
        work.builder.append(std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
                            std::string_view(reinterpret_cast<const char*>(work.valueTmp.data()), len), /*timestamp=*/0,
                            rec::withLeaseOp(0, rec::LeaseOp::Expire));
        work.rids[n++] = rid;
        return true;   // the wheel unlinks it; its state stays Armed until the append result is known
    };
    (void)timers_.pollDeadline(leaseNowNs, sink, maxRecords);
    if (n == 0) return 0;
    work.builder.buildInto(work.batch);
    const int64_t base = appendBatch(replica, std::span<const uint8_t>(work.batch.data(), work.batch.size()), stats_, "expire");
    if (base >= 0) {
        for (uint32_t i = 0; i < n; ++i) timers_.setState(work.rids[i], LeaseEntryState::Emitting, base + static_cast<int64_t>(i));
        lastBatchBytes_ = work.batch.size();
        stats_.expiriesAppended += n;
        ++stats_.expiryBatches;
        return n;
    }
    if (base == kAppendThrew) {
        // counted and logged by appendBatch: the entries stay Armed and are due again next cycle
    } else if (base == PartitionReplica::kAppendNotLeader) {
        ++stats_.notLeaderStops;   // the runtime is destroyed next cycle when the role change is seen
    } else if (base == PartitionReplica::kAppendMinIsr || base == PartitionReplica::kAppendBackPressure) {
        stats_.expiryBacklog += n;
    } else {
        assert(false && "appendLeaseBatch refused a batch the emitter built");
        ++stats_.encodeFailures;
    }
    for (uint32_t i = 0; i < n; ++i) timers_.relink(work.rids[i]);   // Armed again, due on the next cycle
    return 0;
}

uint32_t LeaseRuntime::flushClears(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough,
                                   int64_t leaseNowNs, std::size_t byteCap, std::size_t hardCap, PartitionReplica& replica, LeaseParkTable& parks,
                                   const rune::protocol::LeaseSpec& spec) {
    namespace rec = rune::protocol::record;
    if (clearHead_ == clearTail_) return 0;
    if (writer.descriptor == nullptr || ref.table == nullptr || ref.layout == nullptr) return 0;   // the parks wait for an index that can encode
    const uint16_t* const slotOf = ref.cover != nullptr   // once per flush, not per row
        ? leaseSlotMapFor(*ref.cover, writer.version, writer.installSeq, *ref.layout, *ref.outDesc, *writer.descriptor) : nullptr;
    if (ref.cover != nullptr && slotOf == nullptr) {
        ++stats_.encodeFailures;
        return 0;
    }
    work.builder.reset(0, 0);
    uint32_t n = 0;
    const uint64_t visit = clearTail_ - clearHead_;
    uint64_t visited     = 0;
    bool full            = false;
    // A popped rid goes back on the ring (deferred): its own pop freed the slot, so there is always room.
    auto repush = [this](uint32_t rid) noexcept {
        [[maybe_unused]] const bool ok = flagClear_(rid);
        assert(ok);
    };
    // Stale ring entries (flag clear: the entry was reset; or position not current: a newer live entry of the rid exists) are popped for free and NOT pushed back: they count toward
    // neither the record nor the byte limit.  When a limit stops the walk, the entries left behind stay as they are (flagged, parked).
    while (visited < visit && n < rune::protocol::kMaxLeaseExpiryBatch && !full) {
        const uint32_t rid = clearRing_[clearHead_ & clearMask_];
        ++clearHead_;
        ++visited;
        uint8_t& flag = timers_.flags(rid);
        // Live only if the flag is set AND this is the position the rid was pushed at; anything else is stale and skipped for free.
        if ((flag & kLeaseClearWanted) == 0 || clearPos_[rid] != static_cast<uint32_t>(clearHead_ - 1)) continue;
        flag = static_cast<uint8_t>(flag & ~kLeaseClearWanted);
        ++stats_.clearExamined;
        const LeaseEntry& e = timers_.entry(rid);
        if (e.state != LeaseEntryState::Armed || e.parkHead == kLeaseNil) continue;   // answered or changed meanwhile
        if (e.deadlineNs <= leaseNowNs) continue;   // due: the emitter expires it, its parks are answered at the Expire fold
        const uint8_t* row = ref.table->rowOf(rid);
        // Nothing to clear (the row was cleared meanwhile): a plain keepalive.  Linearization: a keepalive parked behind an UNJUDGED update of
        // the same key and answered Ok here from the folded row is ordered BEFORE that update - the update has not folded, an update never
        // renews, and a revoke that folds later ends the key (the holder's next keepalive then gets LeaseNotFound).
        if (leaseRowRemaining(row, *ref.layout) == 0) {
            const uint64_t ttl = leaseRowTtl(row, *ref.layout);
            timers_.rearm(rid, satAdd(leaseNowNs, msToNsSat(ttl)));
            scheduleIfNone_(rid, ttl, leaseNowNs, spec);
            parks.answerChain(timers_.parkHead(rid), 0, ttl);
            continue;
        }
        if (leaseLastAppended(*ref.table, rid) >= judgedThrough) {   // a client update of the key is appended but not judged
            ++stats_.clearDeferred;
            repush(rid);
            continue;
        }
        const std::span<const uint8_t> key = ref.table->keyOf(rid);
        const std::size_t len = encodeCheckpointValue(work, row, ref, writer, slotOf, /*remainingMs=*/0);   // lease_remaining_ms = 0
        if (len == 0) {
            ++stats_.encodeFailures;
            ++stats_.clearDeferred;
            repush(rid);
            continue;
        }
        switch (fitRecord(work.builder, n, key.size(), len, byteCap, hardCap)) {
            case Fit::Ok: break;
            case Fit::Full: ++stats_.clearDeferred; repush(rid); full = true; continue;
            case Fit::Oversized: ++stats_.oversizedDeferred; ++stats_.clearDeferred; repush(rid); continue;
        }
        work.builder.append(std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
                            std::string_view(reinterpret_cast<const char*>(work.valueTmp.data()), len), /*timestamp=*/0,
                            rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
        work.rids[n++] = rid;   // stays Armed and linked until the append result is known
    }
    if (n == 0) return 0;
    work.builder.buildInto(work.batch);
    const int64_t base = appendBatch(replica, std::span<const uint8_t>(work.batch.data(), work.batch.size()), stats_, "clear");
    if (base >= 0) {
        for (uint32_t i = 0; i < n; ++i) timers_.setState(work.rids[i], LeaseEntryState::Clearing, base + static_cast<int64_t>(i));
        lastBatchBytes_ = work.batch.size();
        stats_.clearsAppended += n;
        ++stats_.clearBatches;
        return n;
    }
    if (base == PartitionReplica::kAppendNotLeader) ++stats_.notLeaderStops;
    for (uint32_t i = 0; i < n; ++i) {   // refused (any code): they stay Armed and linked, wanted again next cycle
        ++stats_.clearDeferred;
        repush(work.rids[i]);
    }
    return 0;
}

uint32_t LeaseRuntime::promote(const LeaseIndexRef& ref, const rune::protocol::LeaseSpec& spec, int64_t leaseNowNs) {
    if (promoteDone_ || ref.table == nullptr || ref.layout == nullptr) return 0;
    const uint32_t high = ref.table->ridHighWater();   // re-read every pass: the table grows while the pass walks
    uint32_t budget     = promoteChunk_;
    uint32_t walked     = 0;
    while (promoteCursor_ < high && budget != 0) {
        --budget;
        ++walked;
        const uint32_t rid = promoteCursor_;
        if (leaseRowLive(*ref.table, rid)) {
            bool growFailed = false;
            if (!timers_.covers(rid) && !coverOrCount_(rid, growFailed)) {
                if (growFailed) break;   // not advanced: retried next pass
                ++promoteCursor_;        // past maxKeys: nothing to link
                continue;
            }
            if (timers_.entry(rid).state == LeaseEntryState::Free) {   // a grant fold or a keepalive already armed the others
                const uint8_t* const row = ref.table->rowOf(rid);
                const uint64_t ttl       = leaseRowTtl(row, *ref.layout);
                const uint64_t remaining = leaseRowRemaining(row, *ref.layout);
                const LeaseStepOut step = leaseStep({LeaseEntryState::Free, LeaseEntryEvent::Arm, true, false, remaining > 0});
                assert(step.action == LeaseAction::Link);
                if (step.action == LeaseAction::Link) {
                    timers_.noteUse(rid);
                    timers_.arm(rid, promotionDeadline(ttl, remaining), checkpointTime(spec, remaining > 0 ? remaining : ttl, leaseNowNs));
                    ++stats_.promotionLinked;
                }
            }
        }
        ++promoteCursor_;
    }
    if (promoteCursor_ >= high) promoteDone_ = true;
    try {
        growIfOwed_();   // cold, once per pass
    } catch (const std::bad_alloc&) {
        timers_.noteGrowFailed();
        ++stats_.growFailures;
    }
    return walked;
}

uint32_t LeaseRuntime::emitCheckpoints(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough,
                                       int64_t leaseNowNs, std::size_t byteCap, std::size_t hardCap, const rune::protocol::LeaseSpec& spec,
                                       PartitionReplica& replica) {
    namespace rec = rune::protocol::record;
    if (writer.descriptor == nullptr || ref.table == nullptr || ref.layout == nullptr) return 0;
    if (timers_.checkpoint().nextCheckNs() > leaseNowNs) return 0;   // the idle path: nothing due
    const uint32_t tokens = checkpointTokens_(spec, leaseNowNs);
    if (tokens == 0) return 0;
    const uint16_t* const slotOf = ref.cover != nullptr
        ? leaseSlotMapFor(*ref.cover, writer.version, writer.installSeq, *ref.layout, *ref.outDesc, *writer.descriptor) : nullptr;
    if (ref.cover != nullptr && slotOf == nullptr) {
        ++stats_.encodeFailures;
        return 0;
    }
    const uint32_t maxRecords = std::min<uint32_t>(kLeaseCheckpointBatchMax, tokens);
    work.builder.reset(0, 0);
    uint32_t n = 0;
    bool full  = false;
    uint64_t remainingMs = 0;
    auto sink = [&](uint32_t rid) -> bool {
        if (full) return false;
        assert(timers_.entry(rid).state == LeaseEntryState::Armed);   // only Armed is linked
        const LeaseEntry& e = timers_.entry(rid);
        // The decision is the step table's (Armed x CheckpointDue): unschedule / defer / append.  Inputs: due; the remaining time (ceil ms)
        // against the interval; the per-key gate.
        const bool due = e.deadlineNs <= leaseNowNs;
        remainingMs    = due ? 0 : ceilRemainingMs(e.deadlineNs, leaseNowNs);
        const bool wanted = spec.checkpoint != 0 && !due && remainingMs > spec.checkpointIntervalMs;
        const bool gate   = leaseLastAppended(*ref.table, rid) < judgedThrough;   // else an update or revoke of the key is appended but not judged
        const LeaseStepOut step = leaseStep({LeaseEntryState::Armed, LeaseEntryEvent::CheckpointDue, gate, due, true, wanted});
        switch (step.action) {
            case LeaseAction::Unschedule:   // turned off by an alter, due (it expires first), or about to end: a later keepalive reschedules
                timers_.scheduleCheckpoint(rid, kLeaseNoCheckpoint);
                return true;
            case LeaseAction::Defer:
                ++stats_.checkpointDeferred;
                return false;
            case LeaseAction::AppendCheckpoint: break;
            default: assert(false && "a CheckpointDue step produced an action the checkpoint sink does not own"); return false;
        }
        const uint8_t* row = ref.table->rowOf(rid);
        const std::span<const uint8_t> key = ref.table->keyOf(rid);
        // A checkpoint may be skipped (spec 9.5): a row that cannot be encoded, or whose lone record exceeds the hard cap, is unscheduled (the
        // sink unlinked it) and the next keepalive re-arm reschedules it - never left forever-deferred in the wheel.
        const std::size_t len = encodeCheckpointValue(work, row, ref, writer, slotOf, remainingMs);
        if (len == 0) {
            ++stats_.encodeFailures;
            timers_.scheduleCheckpoint(rid, kLeaseNoCheckpoint);
            return true;
        }
        switch (fitRecord(work.builder, n, key.size(), len, byteCap, hardCap)) {
            case Fit::Ok: break;
            case Fit::Full: full = true; return false;
            case Fit::Oversized: ++stats_.oversizedDeferred; timers_.scheduleCheckpoint(rid, kLeaseNoCheckpoint); return true;
        }
        work.builder.append(std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
                            std::string_view(reinterpret_cast<const char*>(work.valueTmp.data()), len), /*timestamp=*/0,
                            rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
        work.rids[n++] = rid;
        return true;   // the wheel unlinked it; its state stays Armed until the append result is known
    };
    (void)timers_.pollCheckpoint(leaseNowNs, sink, maxRecords);
    if (n == 0) return 0;
    work.builder.buildInto(work.batch);
    const int64_t base = appendBatch(replica, std::span<const uint8_t>(work.batch.data(), work.batch.size()), stats_, "checkpoint");
    if (base >= 0) {
        for (uint32_t i = 0; i < n; ++i) timers_.setState(work.rids[i], LeaseEntryState::Checkpointing, base + static_cast<int64_t>(i));
        lastBatchBytes_ = work.batch.size();
        stats_.checkpointsAppended += n;
        ++stats_.checkpointBatches;
        ckptUsed_ += n;
        return n;
    }
    if (base == PartitionReplica::kAppendNotLeader) ++stats_.notLeaderStops;
    else if (base == PartitionReplica::kAppendMinIsr || base == PartitionReplica::kAppendBackPressure) stats_.checkpointBacklog += n;
    else if (base != kAppendThrew) {
        assert(false && "appendLeaseBatch refused a batch the emitter built");
        ++stats_.encodeFailures;
    }
    for (uint32_t i = 0; i < n; ++i) timers_.scheduleCheckpoint(work.rids[i], leaseNowNs);   // Armed, due again next cycle; no tokens spent
    return 0;
}

}  // namespace rune::node::lease
