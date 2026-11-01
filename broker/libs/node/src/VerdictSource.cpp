#include <rune/node/VerdictSource.h>

#include <rune/codec/Record.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>

namespace rune::node {

using rune::view::Verdict;

namespace {


uint16_t writerVersionOf(std::string_view value, uint16_t fallback) noexcept {
    return value.size() >= 4 ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) | (static_cast<uint8_t>(value[3]) << 8)) : fallback;
}

}  // namespace

bool PartitionWriteState::isVerdictSource() const noexcept { return verdicts != nullptr || !constraintSets.latest().empty(); }

bool PartitionWriteState::isConstrainedSource() const noexcept {
    const ConstraintSet& s = constraintSets.latest();
    for (uint8_t i = 0; i < s.count; ++i) {
        const auto k = s.members[i].kind;
        if (k == rune::view::VerdictViewKind::Unique || k == rune::view::VerdictViewKind::Check || k == rune::view::VerdictViewKind::TestReject) return true;
    }
    return false;
}

VerdictSource::VerdictSource(int32_t srcTopicId, int32_t partition) : srcTopicId_(srcTopicId), partition_(partition) {
    members_.reserve(kMaxVerdictViewsPerSource);
    active_.reserve(kMaxVerdictViewsPerSource);
    scratchIdx_.reserve(kMaxVerdictViewsPerSource);
    scratch_.resize(cr::markerBatchBytes(cr::kMaxMarkerEntries));
    entryScratch_.reserve(cr::kMaxMarkerEntries);
}

void VerdictSource::addVerdictView(ViewInstance* inst, rune::view::VerdictViewKind kind, uint64_t viewObjectId, uint64_t planFingerprint) {
    Member m;
    m.inst = inst;
    m.kind = kind;
    m.objectId = viewObjectId;
    m.fingerprint = planFingerprint;
    m.prospect.reserve(inst->layout, inst->agg ? &inst->agg->layout : nullptr);
    auto at = std::lower_bound(members_.begin(), members_.end(), viewObjectId, [](const Member& x, uint64_t id) { return x.objectId < id; });
    at = members_.insert(at, std::move(m));
    if (!needsActivation_) discardCheckpointAhead_(*at);
    reindex_();
    recomputeCap_();
}

// P4-4: a checkpoint that outran every marker this node holds (a crash lost the log tail carrying them) cannot be trusted.
void VerdictSource::discardCheckpointAhead_(Member& m) {
    if (!m.inst->persisted || m.inst->foldCursor <= judgeCursor_ || hooks_.discard == nullptr) return;
    hooks_.discard(hooks_.ctx, *m.inst);
    m.inst->foldCursor = logStart_;
    ++stats_.verdictCheckpointsDiscarded;
}

void VerdictSource::removeVerdictView(int32_t viewTopicId) {
    std::erase_if(members_, [&](const Member& m) { return m.inst->plan.viewTopicId == viewTopicId; });
    reindex_();
    recomputeCap_();
}

void VerdictSource::markLeaving(int32_t viewTopicId) noexcept {
    for (Member& m : members_)
        if (m.inst->plan.viewTopicId == viewTopicId) m.leaving = true;
}

void VerdictSource::installTestVerdictViewForTest(ViewInstance* inst, uint64_t viewObjectId, TestRejectRule rule) {
    addVerdictView(inst, rune::view::VerdictViewKind::TestReject, viewObjectId, rule.fingerprint());
    for (Member& m : members_)
        if (m.objectId == viewObjectId && m.fingerprint == rule.fingerprint()) m.rule = rule;
}

bool VerdictSource::retiringForTest(uint64_t viewObjectId) const noexcept {
    return std::any_of(members_.begin(), members_.end(), [&](const Member& m) { return m.objectId == viewObjectId && m.retiring; });
}

bool VerdictSource::resolveSet_(const ConstraintSet& set, std::vector<std::size_t>& out) const {
    out.clear();
    for (uint8_t i = 0; i < set.count; ++i) {
        const ConstraintSetMember& want = set.members[i];
        const auto match = [&](const Member& m) { return !m.retiring && m.objectId == want.viewObjectId && m.fingerprint == want.planFingerprint; };
        auto it = std::find_if(members_.begin(), members_.end(), [&](const Member& m) { return match(m) && !m.leaving; });
        if (it == members_.end()) it = std::find_if(members_.begin(), members_.end(), match);   // a dropped view judges until its drop
        if (it == members_.end()) return false;
        out.push_back(static_cast<std::size_t>(it - members_.begin()));
    }
    return true;
}

bool VerdictSource::reindex_() {
    if (!resolveSet_(activeSet_, active_)) {
        active_.clear();
        return false;
    }
    waitingForInstall_ = false;
    // A dropped view whose plan was installed again (#786): the new member took its place here and it retires, released once a
    // committed marker covers what it judged.
    for (std::size_t i = 0; i < members_.size(); ++i) {
        Member& m = members_[i];
        if (!m.leaving || m.retiring || std::find(active_.begin(), active_.end(), i) != active_.end()) continue;
        if (std::any_of(activeSet_.members.begin(), activeSet_.members.begin() + activeSet_.count,
                        [&](const ConstraintSetMember& w) { return w.viewObjectId == m.objectId && w.planFingerprint == m.fingerprint; })) {
            m.retiring = true;
            m.leaveOffset = judgeCursor_ - 1;
        }
    }
    return true;
}

bool VerdictSource::hasMember(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept {
    return std::any_of(members_.begin(), members_.end(), [&](const Member& m) { return m.objectId == viewObjectId && m.fingerprint == planFingerprint; });
}

bool VerdictSource::hasView(int32_t viewTopicId) const noexcept {
    return std::any_of(members_.begin(), members_.end(), [&](const Member& m) { return m.inst->plan.viewTopicId == viewTopicId; });
}

void VerdictSource::recomputeCap_() noexcept {
    checkpointCap_ = INT64_MAX;
    for (const Member& m : members_)
        if (m.inst->persisted && m.inst->maxRecordsPerPass > 0) checkpointCap_ = std::min(checkpointCap_, m.inst->maxRecordsPerPass);
}

void VerdictSource::bindLeader(rune::replication::PartitionReplica* replica, uint64_t leadershipGeneration, int64_t logStart) noexcept {
    replica_  = replica;
    logStart_ = logStart;
    if (leadershipGeneration == generation_) return;
    generation_      = leadershipGeneration;
    needsActivation_ = true;
    stalled_         = false;
    stallReason_     = StallReason::None;
}

int64_t VerdictSource::extendOverEmptySets_(int64_t offset) const noexcept {
    while (state_->constraintSets.setAt(offset)->empty()) {
        const int64_t a = state_->constraintSets.nextChangeAtOrAfter(offset);
        if (a == INT64_MAX) break;
        offset = a + 1;
    }
    return offset;
}

void VerdictSource::activate_(PartitionWriteState& state) {
    state_ = &state;
    needsActivation_ = false;
    if (state.rejected.capacity() == 0) state.rejected.reserve(kIndexReserve);
    judgeCursor_ = std::max(extendOverEmptySets_(state.rejected.coveredThrough()), logStart_);
    emitFrom_    = std::max(extendOverEmptySets_(state.markers.committedThrough()), logStart_);
    state.rejected.noteCovered(judgeCursor_);
    for (Member& m : members_) discardCheckpointAhead_(m);
    activeSet_ = *state.constraintSets.setAt(judgeCursor_);
    setFrom_   = judgeCursor_;
    sawEmptyChange_ = judgedRecord_ = false;
    reindex_();
    recomputeCap_();
}

void VerdictSource::stall_(StallReason reason) noexcept {
    if (stalled_) return;
    ++stats_.verdictStalls;
    stalled_     = true;
    stallReason_ = reason;
    spdlog::error("verdict judge stalled: topic {} partition {}: {}", srcTopicId_, partition_,
                  reason == StallReason::Full ? "rejected index full" : reason == StallReason::OutOfOrder ? "rejection out of order" : "view capacity exhausted");
}

void VerdictSource::waitForInstall_() noexcept {
    if (waitingForInstall_) return;
    waitingForInstall_ = true;
    ++stats_.judgeWaitsForInstall;
    spdlog::info("verdict judge waits for a view install: topic {} partition {}", srcTopicId_, partition_);
}

rune::view::PrepareStatus VerdictSource::prepare_(Member& m, const rune::view::SourceRecord& rec) noexcept {
    ViewInstance& i = *m.inst;
    if (i.agg) {
        rune::view::AggregateFold f{i.plan, i.layout, i.agg->layout, i.vdc, i.table, i.agg->contributors, i.agg->groups, i.agg->blocksOrNull()};
        return rune::view::prepareAggregate(f, rec, m.prospect);
    }
    rune::view::ProjectionFold f{i.plan, i.layout, i.vdc, i.table, rune::view::viewKeyType(i.plan)};
    return rune::view::prepareProjection(f, rec, m.prospect);
}

void VerdictSource::apply_(Member& m, const rune::view::SourceRecord& rec) noexcept {
    ViewInstance& i = *m.inst;
    if (i.agg) {
        rune::view::AggregateFold f{i.plan, i.layout, i.agg->layout, i.vdc, i.table, i.agg->contributors, i.agg->groups, i.agg->blocksOrNull()};
        rune::view::applyAggregate(f, rec, m.prospect);
    } else {
        rune::view::ProjectionFold f{i.plan, i.layout, i.vdc, i.table, rune::view::viewKeyType(i.plan)};
        rune::view::applyProjection(f, rec, m.prospect);
    }
    const rune::view::FoldStats& st = m.prospect.stats;
    i.counters.skippedMalformed += st.skippedMalformed;
    i.counters.skippedNoGroup   += st.skippedNoGroup;
    i.counters.aggOverflow      += st.aggOverflow;
    i.counters.aggNonFinite     += st.aggNonFinite;
    i.counters.aggInconsistent  += st.aggInconsistent;
    i.counters.aggRescans       += st.aggRescans;
}

bool VerdictSource::rejects_(const Member& m, const rune::view::SourceRecord& rec) const noexcept {
    if (m.kind != rune::view::VerdictViewKind::TestReject || rec.value.empty()) return false;   // tombstones are never rejected
    switch (m.rule.mode) {
    case TestRejectRule::Mode::TagEquals: {
        if (rec.writer == nullptr) return false;
        for (const auto& f : rec.writer->fields) {
            if (f.name != "tag") continue;
            const auto v = rune::codec::readField(*rec.writer, {reinterpret_cast<const uint8_t*>(rec.value.data()), rec.value.size()}, f.id);
            return v.present && (v.i == m.rule.tag || v.u == m.rule.tag);
        }
        return false;
    }
    case TestRejectRule::Mode::OffsetSet:
        return std::find(m.rule.offsets.begin(), m.rule.offsets.begin() + m.rule.offsetCount, rec.offset) != m.rule.offsets.begin() + m.rule.offsetCount;
    case TestRejectRule::Mode::CapLiveKeys:
        return m.prospect.op == rune::view::ProspectOp::Upsert && !m.prospect.rows[0].existed && m.inst->table.size() >= m.rule.cap;
    }
    return false;
}

Verdict VerdictSource::judge(const rune::view::SourceRecord& rec) noexcept {
    ++judgeCalls_;
    for (const std::size_t i : active_) {
        if (prepare_(members_[i], rec) == rune::view::PrepareStatus::CapacityExhausted) {
            stall_(StallReason::Capacity);
            return {};
        }
    }
    for (const std::size_t i : active_) {
        if (!rejects_(members_[i], rec)) continue;
        const Verdict v{static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation),
                        rune::view::constraintIdOf(members_[i].objectId, 1)};
        if (!state_->rejected.add(rec.offset, v.code, v.constraintId)) {
            stall_(state_->rejected.size() >= state_->rejected.capacity() ? StallReason::Full : StallReason::OutOfOrder);
            return {};
        }
        if (state_->rejected.size() * 4 >= state_->rejected.capacity() * 3) state_->growOwed = true;
        ++stats_.rejectionsConstraint;
        ++rejectedThisPass_;
        auto* pc = std::find_if(stats_.perConstraint.begin(), stats_.perConstraint.begin() + stats_.perConstraintCount,
                                [&](const auto& c) { return c.constraintId == v.constraintId; });
        if (pc == stats_.perConstraint.begin() + stats_.perConstraintCount && stats_.perConstraintCount < stats_.perConstraint.size())
            *pc = {v.constraintId, 0}, ++stats_.perConstraintCount;
        if (pc != stats_.perConstraint.begin() + stats_.perConstraintCount) ++pc->rejections;
        return v;
    }
    for (const std::size_t i : active_) apply_(members_[i], rec);
    return {};
}

int VerdictSource::replay_(Member& m, const rune::storage::LogReadView& view, int64_t upTo, const rune::view::AbortedTxnFilter& aborted) {
    ViewInstance& inst = *m.inst;
    const rune::view::FoldSkip skip{.aborted = aborted, .rejected = &state_->rejected};
    int64_t cursor = inst.foldCursor;
    const int64_t bound = inst.maxRecordsPerPass > 0 ? std::min(upTo, cursor + inst.maxRecordsPerPass) : upTo;
    const rune::view::FoldStats st =
        inst.agg ? rune::view::foldAggregateBatch(inst.plan, inst.layout, inst.agg->layout, inst.vdc, inst.table, inst.agg->contributors,
                                                  inst.agg->groups, inst.agg->blocksOrNull(), view, bound, cursor, skip)
                 : rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view, bound, cursor, skip);
    inst.foldCursor = cursor;
    inst.counters.skippedAborted   += st.skippedAborted;
    inst.counters.skippedMalformed += st.skippedMalformed;
    inst.counters.skippedNoGroup   += st.skippedNoGroup;
    inst.counters.aggOverflow      += st.aggOverflow;
    inst.counters.aggNonFinite     += st.aggNonFinite;
    inst.counters.aggInconsistent  += st.aggInconsistent;
    inst.counters.aggRescans       += st.aggRescans;
    return static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted + st.skippedMalformed + st.skippedNoGroup + st.skippedRejected);
}

int VerdictSource::judgeBatches_(const rune::storage::LogReadView& view, int64_t limit, const rune::view::AbortedTxnFilter& aborted) {
    namespace proto = rune::protocol;
    int work = 0;
    ViewInstance& first = *members_[active_.front()].inst;
    std::size_t pos = 0;
    while (pos < view.len && !stalled_ && judgeCursor_ < limit) {
        proto::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) break;
        const int16_t attrs = r.attributes();
        const int64_t last = r.baseOffset() + r.lastOffsetDelta();
        if (r.baseOffset() >= limit) break;
        if (last < judgeCursor_) {
            pos += static_cast<std::size_t>(r.totalLength());
            continue;
        }
        const bool skipBatch = (attrs & proto::batch::kAttrControl) != 0 || aborted.skips(r, attrs);
        if (skipBatch) {
            if (last >= limit) break;
            judgeCursor_ = last + 1;
            pos += static_cast<std::size_t>(r.totalLength());
            ++work;
            continue;
        }
        judgeCursor_ = std::max(judgeCursor_, r.baseOffset());
        bool halt = false;
        const bool walked = rune::protocol::forEachRecordSpan(view.data + pos, view.len - pos, [&](const rune::protocol::RecordSpan& span) {
            if (halt || stalled_) return;
            if (span.offset >= limit) { halt = true; return; }
            if (span.offset < judgeCursor_) return;
            rune::view::SourceRecord rec{span.key, span.value, span.offset, rune::protocol::record::intentOf(span.attributes), nullptr};
            if (!span.value.empty()) rec.writer = &first.vdc.materializedFor(writerVersionOf(span.value, first.vdc.defaultVersion()));
            lastIntent_ = rec.intent;
            (void)judge(rec);
            if (stalled_) return;
            judgeCursor_ = span.offset + 1;
            judgedRecord_ = true;
            ++work;
        });
        if (!walked && !halt && !stalled_) break;   // a record that does not fit its batch: stop, never advance past it
        if (stalled_) break;
        judgeCursor_ = halt ? limit : std::max(judgeCursor_, last + 1);
        pos += static_cast<std::size_t>(r.totalLength());
    }
    for (const std::size_t i : active_) members_[i].inst->foldCursor = judgeCursor_;
    return work;
}

int VerdictSource::emitMarkers_(PartitionWriteState& state, int64_t nowNs) {
    if (generation_ == kUnbound || judgeCursor_ <= emitFrom_ || !(judgedRecord_ || sawEmptyChange_)) return 0;
    const bool due = rejectedThisPass_ > 0 || sawEmptyChange_ || nowNs - lastMarkerNs_ >= intervalNs_ ||
                     (checkpointCap_ != INT64_MAX && judgeCursor_ - emitFrom_ >= checkpointCap_);
    if (!due) return 0;
    int work = 0;
    while (emitFrom_ < judgeCursor_ && !state.markers.full()) {
        const int64_t from = emitFrom_;
        int64_t finalThrough = judgeCursor_;
        entryScratch_.clear();
        for (int64_t pos = from;;) {
            const int64_t off = state.rejected.nextRejectedAtOrAfter(pos);
            if (off >= judgeCursor_) break;
            if (entryScratch_.size() == cr::kMaxMarkerEntries) { finalThrough = off; break; }
            const auto* e = state.rejected.find(off);
            entryScratch_.push_back(cr::MarkerEntry{static_cast<uint32_t>(off - from), e->code, e->constraintId});
            pos = off + 1;
        }
        const cr::MarkerHeader h{from, finalThrough, state.constraintSets.setAt(finalThrough)->id, static_cast<uint32_t>(entryScratch_.size())};
        const std::size_t n = cr::buildVerdictMarker(scratch_, h, entryScratch_);
        const std::span<const uint8_t> batch(scratch_.data(), n);
        int64_t base;
        if (replica_ != nullptr) {
            base = replica_->appendControlBatch(batch, false);
        } else {
            base = std::max(testNextOffset_, judgeCursor_);
            testNextOffset_ = base + 1;
            if (captureAppended_) appended_.emplace_back(batch.begin(), batch.end());
        }
        if (base == rune::replication::PartitionReplica::kAppendNotLeader) break;
        if (base < 0) { ++stats_.markerAppendRetries; break; }
        (void)state.markers.onMarker(base, finalThrough);
        state.rejected.noteCovered(finalThrough);
        ++stats_.markersWritten;
        emitFrom_     = finalThrough;
        lastMarkerNs_ = nowNs;
        ++work;
    }
    if (emitFrom_ >= judgeCursor_) sawEmptyChange_ = judgedRecord_ = false;
    return work;
}

int VerdictSource::pushCheckpoints_(PartitionWriteState& state) {
    if (hooks_.checkpoint == nullptr) return 0;
    int work = 0;
    for (Member& m : members_) {
        ViewInstance& inst = *m.inst;
        // P4-4: a persisted member's delta leaves only once a marker in the local log covers the cursor it is stamped with.
        if (inst.persisted && state.markers.appendedThrough() < inst.foldCursor && !inst.pendingValid && !inst.resetOwed) continue;
        work += hooks_.checkpoint(hooks_.ctx, inst);
    }
    return work;
}

int VerdictSource::dropRetired_(PartitionWriteState& state) {
    const std::size_t before = members_.size();
    std::array<int32_t, kMaxVerdictViewsPerSource> gone{};
    std::size_t n = 0;
    std::erase_if(members_, [&](const Member& m) {
        if (!m.retiring || state.markers.committedThrough() <= m.leaveOffset) return false;
        if (n < gone.size()) gone[n++] = m.inst->plan.viewTopicId;
        return true;
    });
    if (members_.size() == before) return 0;
    if (hooks_.retired != nullptr)
        for (std::size_t i = 0; i < n; ++i) hooks_.retired(hooks_.ctx, gone[i], partition_);
    reindex_();
    recomputeCap_();
    return static_cast<int>(before - members_.size());
}

int VerdictSource::requestChange(const ConstraintSet& next, bool refresh) {
    if (state_ == nullptr || generation_ == kUnbound || needsActivation_) return kChangeDeferred;
    if (!refresh && next.sameMembers(state_->constraintSets.latest())) return kChangeUnchanged;
    ConstraintSet logged = next;
    logged.id = state_->constraintSets.latest().id + 1;
    std::array<cr::ChangeMember, kMaxVerdictViewsPerSource> wire{};
    for (uint8_t i = 0; i < logged.count; ++i)
        wire[i] = cr::ChangeMember{logged.members[i].viewObjectId, static_cast<uint8_t>(logged.members[i].kind), logged.members[i].planFingerprint};
    const std::size_t n = cr::buildConstraintSetChange(scratch_, logged.id, std::span<const cr::ChangeMember>(wire.data(), logged.count));
    const std::span<const uint8_t> batch(scratch_.data(), n);
    int64_t base;
    if (replica_ != nullptr) {
        base = replica_->appendControlBatch(batch, false);
    } else {
        base = std::max(testNextOffset_, judgeCursor_);
        testNextOffset_ = base + 1;
        if (captureAppended_) appended_.emplace_back(batch.begin(), batch.end());
    }
    if (base < 0) return kChangeDeferred;
    state_->constraintSets.onChange(base, logged);
    return static_cast<int>(base);
}

void verdictOnHw(PartitionWriteState& state, int64_t hw) noexcept {
    state.markers.onHw(hw);
    state.rejected.setWatermark(state.markers.committedThrough());
}

}  // namespace rune::node
