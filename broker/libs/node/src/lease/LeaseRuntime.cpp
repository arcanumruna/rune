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

[[nodiscard]] inline bool inflight(LeaseEntryState s) noexcept {
    return s == LeaseEntryState::Checkpointing || s == LeaseEntryState::Clearing || s == LeaseEntryState::Emitting;
}
}  // namespace

void LeaseRuntime::compactClears_() noexcept {
    uint64_t w = clearHead_;
    for (uint64_t r = clearHead_; r != clearTail_; ++r) {
        const uint32_t rid = clearRing_[r & clearMask_];
        uint8_t& f = timers_.flags(rid);
        if ((f & kLeaseClearWanted) == 0) continue;   // stale (the entry was reset) or a duplicate of one kept already
        f = static_cast<uint8_t>(f & ~kLeaseClearWanted);   // kept ones are marked seen by a clear flag, restored below
        if (timers_.parkHead(rid) == kLeaseNil) continue;   // answered meanwhile: out of the ring, flag clear
        clearRing_[w++ & clearMask_] = rid;
    }
    clearTail_ = w;
    for (uint64_t r = clearHead_; r != clearTail_; ++r) {
        uint8_t& f = timers_.flags(clearRing_[r & clearMask_]);
        f = static_cast<uint8_t>(f | kLeaseClearWanted);
    }
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
    clearRing_[clearTail_ & clearMask_] = rid;
    ++clearTail_;
    return true;
}

LeaseRuntime::KaStep LeaseRuntime::kaStep_(uint32_t rid, int64_t leaseNowNs, bool remainingPositive, uint64_t ttlMs, bool gateOpen) noexcept {
    const LeaseEntry& e = timers_.entry(rid);
    const bool due      = e.deadlineNs <= leaseNowNs;
    const LeaseStepOut out = leaseStep({e.state, LeaseEntryEvent::Keepalive, gateOpen, due, remainingPositive});
    switch (out.action) {
        case LeaseAction::RearmAnswerOk:
            timers_.rearm(rid, satAdd(leaseNowNs, msToNsSat(ttlMs)));
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
        case LeaseAction::ReplayParked: break;
    }
    assert(false && "a Keepalive step produced an action the keepalive path does not own");
    return {false, false, kCodeLeaseNotFound, 0};
}

void LeaseRuntime::replay_(uint32_t rid, int64_t leaseNowNs, bool remainingPositive, uint64_t ttlMs, LeaseParkTable& parks) noexcept {
    uint32_t& head = timers_.parkHead(rid);
    if (head == kLeaseNil) return;
    // The gate is not consulted here: AppendClear and a gate-closed Park both leave the chain parked with the clear flag set, and
    // flushClears re-checks the gate before it appends.  The decision depends only on the entry's state and the row's values, so it is
    // taken ONCE for the whole chain.
    const KaStep st = kaStep_(rid, leaseNowNs, remainingPositive, ttlMs, /*gateOpen=*/true);
    if (!st.park) parks.answerChain(head, st.code, st.ttlMs);
    else if (st.wantFlag && !flagClear_(rid)) parks.answerChain(head, kCodeThrottled, 1);   // a replay never takes a new slot; no ring room = Throttled
}

void LeaseRuntime::foldStep_(uint32_t rid, LeaseEntryEvent event, const LeaseEvent& ev, int64_t leaseNowNs, LeaseParkTable& parks) noexcept {
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
            if (event == LeaseEntryEvent::OwnRecordRejected) replay_(rid, leaseNowNs, /*remainingPositive=*/true, 0, parks);
            else replay_(rid, leaseNowNs, ev.remainingMs > 0, ev.ttlMs, parks);
            return;
        case LeaseAction::DiscardAnswerParkedNotLeader:
        case LeaseAction::Link:
        case LeaseAction::AppendExpire:
        case LeaseAction::AppendCheckpoint:
        case LeaseAction::AppendClear:
        case LeaseAction::Park:
        case LeaseAction::RearmAnswerOk: break;
    }
    assert(false && "a fold event produced an action only the keepalive path or a demotion owns");
}

void LeaseRuntime::onEvents(std::span<const LeaseEvent> events, int64_t leaseNowNs, LeaseParkTable& parks, const LeaseIndexRef* ref) noexcept {
    for (const LeaseEvent& ev : events) {
        const uint32_t rid = ev.rid;
        if (ev.kind == LeaseEventKind::Granted) {
            if (!timers_.covers(rid) && !timers_.reserveFor(rid)) {   // cold growth: one fold can add more grants than the 75 % headroom
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
            // Task 12 adds the checkpoint time.
            timers_.arm(rid, satAdd(leaseNowNs, msToNsSat(ev.ttlMs)), kLeaseNoCheckpoint);
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
                // Back to Armed at the stored deadline (relinked: past due means due on the next poll, rebuilt from the current row).
                const bool wasArmed = e.state == LeaseEntryState::Armed;
                foldStep_(rid, LeaseEntryEvent::OwnRecordRejected, ev, leaseNowNs, parks);
                if (!wasArmed && timers_.entry(rid).state == LeaseEntryState::Armed) ++stats_.ownRecordRejected;
            }
            continue;   // otherwise a stale record of another leadership
        }
        if (!timers_.covers(rid)) continue;
        const LeaseEntry& e = timers_.entry(rid);
        switch (ev.kind) {
            case LeaseEventKind::Updated:
                foldStep_(rid, LeaseEntryEvent::OtherUpdateFolds, ev, leaseNowNs, parks);
                break;
            case LeaseEventKind::Checkpointed:
                // Task 12 adds the checkpoint re-arming.
                foldStep_(rid, e.inflightOffset == ev.offset ? LeaseEntryEvent::OwnRecordFolds : LeaseEntryEvent::OtherUpdateFolds, ev, leaseNowNs, parks);
                break;
            case LeaseEventKind::Ended:
                foldStep_(rid, e.state == LeaseEntryState::Emitting && e.inflightOffset == ev.offset ? LeaseEntryEvent::OwnRecordFolds
                                                                                                    : LeaseEntryEvent::EndFolds, ev, leaseNowNs, parks);
                break;
            case LeaseEventKind::Granted:
            case LeaseEventKind::BrokerRejected: break;   // handled above
        }
    }
    if (timers_.growOwed()) timers_.growIfOwed();   // cold, once per cycle
}

LeaseKaOutcome LeaseRuntime::keepAlive(const LeaseIndexRef& ref, std::span<const uint8_t> key, int64_t version, int64_t judgedThrough,
                                       int64_t leaseNowNs, LeaseParkTable& parks, uint32_t& req, int64_t sessionId, int64_t correlationId,
                                       uint16_t index, bool owedRoom) {
    if (ref.table == nullptr || ref.layout == nullptr || key.size() != ref.table->keyWidth()) return {false, kCodeLeaseNotFound, 0};   // a wrong-width key is no key
    uint32_t rid = 0;
    const uint8_t* const row = ref.table->find(rune::view::ViewKey::fromBytes(key), rid);
    if (row == nullptr || leaseRowVersion(row, *ref.layout) != version) return {false, kCodeLeaseNotFound, 0};
    try {
        if (!timers_.covers(rid) && !timers_.reserveFor(rid)) return {false, kCodeThrottled, 1};   // cold growth
    } catch (const std::bad_alloc&) {
        return {false, kCodeThrottled, 1};
    }
    const uint64_t ttl       = leaseRowTtl(row, *ref.layout);
    const uint64_t remaining = leaseRowRemaining(row, *ref.layout);
    if (timers_.entry(rid).state == LeaseEntryState::Free) {   // a row the promotion pass has not linked yet (Task 12): armed at its promotion deadline
        timers_.noteUse(rid);
        timers_.arm(rid, promotionDeadline(ttl, remaining), kLeaseNoCheckpoint);
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
        const KaStep st = kaStep_(rid, leaseNowNs, remaining > 0, ttl, gateOpen);
        assert(!st.park);
        out = {false, st.code, st.ttlMs};
    }
    try {
        if (timers_.growOwed()) timers_.growIfOwed();   // cold; a failure is retried by the next call
    } catch (const std::bad_alloc&) {
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
                                   int64_t leaseNowNs, std::size_t byteCap, std::size_t hardCap, PartitionReplica& replica, LeaseParkTable& parks) {
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
    // Stale ring rids (flag clear: the entry was reset, or a duplicate of one handled already) are popped for free: they count toward
    // neither the record nor the byte limit.  When a limit stops the walk, the entries left behind stay as they are (flagged, parked).
    while (visited < visit && n < rune::protocol::kMaxLeaseExpiryBatch && !full) {
        const uint32_t rid = clearRing_[clearHead_ & clearMask_];
        ++clearHead_;
        ++visited;
        uint8_t& flag = timers_.flags(rid);
        if ((flag & kLeaseClearWanted) == 0) continue;
        flag = static_cast<uint8_t>(flag & ~kLeaseClearWanted);
        const LeaseEntry& e = timers_.entry(rid);
        if (e.state != LeaseEntryState::Armed || e.parkHead == kLeaseNil) continue;   // answered or changed meanwhile
        if (e.deadlineNs <= leaseNowNs) continue;   // due: the emitter expires it, its parks are answered at the Expire fold
        const uint8_t* row = ref.table->rowOf(rid);
        if (leaseRowRemaining(row, *ref.layout) == 0) {   // nothing to clear (the row was cleared meanwhile): a plain keepalive
            const uint64_t ttl = leaseRowTtl(row, *ref.layout);
            timers_.rearm(rid, satAdd(leaseNowNs, msToNsSat(ttl)));
            parks.answerChain(timers_.parkHead(rid), 0, ttl);
            continue;
        }
        if (leaseLastAppended(*ref.table, rid) >= judgedThrough) {   // a client update of the key is appended but not judged
            ++stats_.clearDeferred;
            repush(rid);
            continue;
        }
        const std::span<const uint8_t> key = ref.table->keyOf(rid);
        const std::size_t len = encodeLeaseValue(row, *ref.layout, *ref.outDesc, *writer.descriptor, writer.version, work.valueTmp.data(),
                                                 work.valueTmp.size(), slotOf);
        if (len < rec::kLeaseRemainingValueOffset + 8) {
            ++stats_.encodeFailures;
            ++stats_.clearDeferred;
            repush(rid);
            continue;
        }
        std::memset(work.valueTmp.data() + rec::kLeaseRemainingValueOffset, 0, 8);   // lease_remaining_ms = 0
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

}  // namespace rune::node::lease
