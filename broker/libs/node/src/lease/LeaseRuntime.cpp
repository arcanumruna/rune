// LeaseRuntime.cpp - lease topics Task 10: see LeaseRuntime.h.
#include <rune/node/lease/LeaseRuntime.h>

#include <rune/node/lease/LeaseIndex.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/replication/PartitionReplica.h>

#include <algorithm>
#include <cassert>
#include <string_view>

namespace rune::node::lease {

namespace {
using rune::replication::PartitionReplica;

/// Steps the state machine for `rid` and applies the next state.  The action is ignored here: Task 11 wires the park / answer actions.
inline LeaseEntryState step(LeaseTimers& timers, uint32_t rid, LeaseEntryEvent event) noexcept {
    const LeaseEntry& e = timers.entry(rid);
    const LeaseStepOut out = leaseStep({e.state, event, /*gateOpen=*/false, /*due=*/false, /*remainingPositive=*/false});
    // Task 11: out.action (Park / RearmAnswerOk / ReturnArmedReplayParked / ReturnArmedAnswerParkedOk / FreeAnswerParkedNotFound /
    // DiscardAnswerParkedNotLeader / ReplayParked) is performed by the keepalive path; Task 10 applies the state only.
    if (out.next != e.state) timers.setState(rid, out.next);
    return out.next;
}

[[nodiscard]] inline bool inflight(LeaseEntryState s) noexcept {
    return s == LeaseEntryState::Checkpointing || s == LeaseEntryState::Clearing || s == LeaseEntryState::Emitting;
}
}  // namespace

void LeaseRuntime::onEvents(std::span<const LeaseEvent> events, int64_t leaseNowNs) noexcept {
    for (const LeaseEvent& ev : events) {
        const uint32_t rid = ev.rid;
        if (ev.kind == LeaseEventKind::Granted) {
            if (!timers_.covers(rid) && !timers_.reserveFor(rid)) {   // cold growth: one fold can add more grants than the 75 % headroom
                assert(false && "a lease row id past maxKeys");
                continue;
            }
            if (timers_.entry(rid).state != LeaseEntryState::Free) {
                assert(false && "a Granted event found its entry armed or in flight");
                ++stats_.grantNotFree;
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
                // Back to Armed at the stored deadline (relinked: past due means due on the next poll, rebuilt from the current row).
                if (step(timers_, rid, LeaseEntryEvent::OwnRecordRejected) == LeaseEntryState::Armed) ++stats_.ownRecordRejected;
            }
            continue;   // otherwise a stale record of another leadership
        }
        if (!timers_.covers(rid)) continue;
        const LeaseEntry& e = timers_.entry(rid);
        switch (ev.kind) {
            case LeaseEventKind::Updated:
                step(timers_, rid, LeaseEntryEvent::OtherUpdateFolds);
                break;
            case LeaseEventKind::Checkpointed:
                // Task 12 adds the checkpoint re-arming; here only the state is applied.
                step(timers_, rid, e.inflightOffset == ev.offset ? LeaseEntryEvent::OwnRecordFolds : LeaseEntryEvent::OtherUpdateFolds);
                break;
            case LeaseEventKind::Ended:
                step(timers_, rid, e.state == LeaseEntryState::Emitting && e.inflightOffset == ev.offset ? LeaseEntryEvent::OwnRecordFolds
                                                                                                          : LeaseEntryEvent::EndFolds);
                break;
            case LeaseEventKind::Granted:
            case LeaseEventKind::BrokerRejected: break;   // handled above
        }
    }
    if (timers_.growOwed()) timers_.growIfOwed();   // cold, once per cycle
}

uint32_t LeaseRuntime::emit(LeaseEmitWork& work, const LeaseIndexRef& ref, const LeaseWriter& writer, int64_t judgedThrough, int64_t leaseNowNs,
                            std::size_t byteCap, uint32_t maxRecords, PartitionReplica& replica) noexcept {
    namespace rec = rune::protocol::record;
    if (writer.descriptor == nullptr || ref.table == nullptr) return 0;
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
        const uint16_t* const slotOf = ref.cover != nullptr
            ? leaseSlotMapFor(*ref.cover, writer.version, writer.installSeq, *ref.layout, *ref.outDesc, *writer.descriptor) : nullptr;
        const std::size_t len = encodeLeaseValue(row, *ref.layout, *ref.outDesc, *writer.descriptor, writer.version, work.valueTmp.data(),
                                                 work.valueTmp.size(), slotOf);
        if (len == 0) {
            ++stats_.encodeFailures;
            return false;
        }
        if (work.builder.bodyBytes() + rune::protocol::kMaxRecordFramingBytes + key.size() + len +
                static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) > byteCap) {
            full = true;
            return false;
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
    const int64_t base = replica.appendLeaseBatch(std::span<const uint8_t>(work.batch.data(), work.batch.size()));
    if (base >= 0) {
        for (uint32_t i = 0; i < n; ++i) timers_.setState(work.rids[i], LeaseEntryState::Emitting, base + static_cast<int64_t>(i));
        stats_.expiriesAppended += n;
        ++stats_.expiryBatches;
        return n;
    }
    if (base == PartitionReplica::kAppendNotLeader) {
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

}  // namespace rune::node::lease
