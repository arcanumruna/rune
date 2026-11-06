#include <rune/node/VerdictSource.h>

#include <rune/codec/Record.h>
#include <rune/view/CheckEval.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace rune::node {

using rune::view::Verdict;

namespace {


uint16_t writerVersionOf(std::string_view value, uint16_t fallback) noexcept {
    return value.size() >= 4 ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) | (static_cast<uint8_t>(value[3]) << 8)) : fallback;
}

}  // namespace

rune::view::PrepareStatus prepareKeyIndex(const KeyIndexFold& f, const rune::view::SourceRecord& rec, rune::view::Prospect& p) noexcept {
    p.clear();
    if (rec.key.size() != f.keyWidth) {
        p.stats.skippedMalformed = 1;
        return rune::view::PrepareStatus::Malformed;
    }
    rune::view::ProspectRow& r = p.rows[0];
    r.key      = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(rec.key.data()), rec.key.size()});
    r.existed  = f.table.get(r.key) != nullptr;
    r.removed  = rec.value.empty();
    r.image    = nullptr;
    p.rowCount = 1;
    p.op       = r.removed ? rune::view::ProspectOp::Erase : rune::view::ProspectOp::Upsert;
    return rune::view::PrepareStatus::Ok;
}

Verdict evaluateIntent(const rune::view::SourceRecord& rec, const rune::view::Prospect& p) noexcept {
    using rune::protocol::client::ErrorCode;
    if (p.op == rune::view::ProspectOp::None) return {};
    const bool existed = p.rows[0].existed;
    if (rec.intent == static_cast<uint8_t>(rune::protocol::record::Intent::Insert) && !rec.value.empty() && existed)
        return {static_cast<uint16_t>(ErrorCode::KeyExists), 0};
    if (rec.intent == static_cast<uint8_t>(rune::protocol::record::Intent::Existing) && !existed)
        return {static_cast<uint16_t>(ErrorCode::KeyNotFound), 0};
    return {};
}

void applyKeyIndex(KeyIndexFold& f, const rune::view::SourceRecord&, const rune::view::Prospect& p) noexcept {
    if (p.op == rune::view::ProspectOp::Upsert) *f.table.put(p.rows[0].key) = 1;
    else if (p.op == rune::view::ProspectOp::Erase) f.table.erase(p.rows[0].key);
}

rune::view::FoldStats foldKeyIndexBatch(KeyIndexFold& f, const rune::storage::LogReadView& view, int64_t bound, int64_t& cursor,
                                        const rune::view::FoldSkip& skipRef) noexcept {
    const rune::view::FoldSkip skip = skipRef;   // #537: by value (see AbortedTxnFilter::skips)
    rune::view::FoldStats stats{};
    rune::view::Prospect  p;
    const int64_t from = cursor;
    bool stopped = false, corrupt = false;
    int64_t walkedUpto = view.baseOffset;
    std::size_t pos = 0;
    while (pos < view.len) {
        rune::protocol::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) {
            corrupt = true;
            break;
        }
        const int16_t attrs = r.attributes();
        const int64_t end   = r.baseOffset() + r.lastOffsetDelta() + 1;
        if ((attrs & rune::protocol::batch::kAttrControl) != 0) {
            ++stats.skippedControl;
        } else if (rune::view::isAbortedBatch(skip, r, attrs)) [[unlikely]] {
            if (end > bound) { stopped = true; break; }
            ++stats.skippedAborted;
        } else {
            r.forEachRecord([&](std::string_view key, std::string_view value, int64_t offset, int64_t) {
                if (stopped) return;
                if (offset >= bound) { stopped = true; return; }
                if (offset < from) return;
                if (rune::view::isRejectedRecord(skip, offset)) [[unlikely]] {
                    ++stats.skippedRejected;
                    return;
                }
                const rune::view::SourceRecord rec{key, value, offset, 0, nullptr};
                if (prepareKeyIndex(f, rec, p) == rune::view::PrepareStatus::Malformed) {
                    ++stats.skippedMalformed;
                    return;
                }
                applyKeyIndex(f, rec, p);
                ++(value.empty() ? stats.deleted : stats.applied);
            });
            if (stopped) break;
        }
        pos += static_cast<std::size_t>(r.totalLength());
        walkedUpto = end;
    }
    cursor = std::max(from, corrupt ? walkedUpto : std::min(view.nextOffset, bound));
    return stats;
}

Verdict evaluateConstraints(const ConstraintFold& f, const rune::view::SourceRecord& rec, const rune::view::Prospect& p) noexcept {
    using rune::protocol::client::ErrorCode;
    if (rec.value.empty()) return {};
    const auto violation = [&](uint16_t ordinal) { return Verdict{static_cast<uint16_t>(ErrorCode::ConstraintViolation), rune::view::constraintIdOf(f.plan.viewObjectId, ordinal)}; };
    if (f.plan.unique && p.rowCount > 0 && p.rows[0].ownedByOther) return violation(0);
    for (const rune::view::CompiledConstraint& c : f.plan.constraints) {
        for (std::size_t i = 0; i < p.rowCount; ++i) {
            const rune::view::ProspectRow& row = p.rows[i];
            if (row.removed || row.promoted) continue;
            if (rune::view::evaluatePredicate(c.predicate, f.layout, row.image) != rune::view::PredicateResult::False) continue;
            if (rune::view::predicateReadsChanged(c.predicate, f.layout, row.existed ? f.table.get(row.key) : nullptr, row.image)) return violation(c.ordinal);
        }
    }
    return {};
}

rune::view::CompiledViewPlan keyIndexPlan(int32_t srcTopicId, uint16_t keyFieldId) {
    rune::view::CompiledViewPlan plan;
    plan.viewName          = "__key_index";
    plan.viewTopicId       = -1;
    plan.keyFieldId        = {keyFieldId};
    plan.sourceTopicId     = {srcTopicId};
    plan.outputFingerprint = kKeyIndexPlanFingerprint;
    plan.persistence       = 1;
    plan.sizing.chunkBytes = static_cast<uint32_t>(kKeyIndexChunkBytes);
    return plan;
}

rune::view::RowLayout keyIndexRowLayout() {
    rune::view::RowLayout l;
    l.rowLen = 1;
    return l;
}

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
    addMember_(inst, kind, viewObjectId, planFingerprint, false);
}

void VerdictSource::addCandidate(ViewInstance* inst, rune::view::VerdictViewKind kind, uint64_t viewObjectId, uint64_t planFingerprint) {
    addMember_(inst, kind, viewObjectId, planFingerprint, true);
}

void VerdictSource::removeCandidate(uint64_t viewObjectId, uint64_t planFingerprint) {
    if (std::erase_if(members_, [&](const Member& m) { return m.candidate && m.objectId == viewObjectId && m.fingerprint == planFingerprint; }) == 0) return;
    reindex_();
    recomputeCap_();
}

bool VerdictSource::hasCandidate(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept {
    return std::any_of(members_.begin(), members_.end(), [&](const Member& m) { return m.candidate && m.objectId == viewObjectId && m.fingerprint == planFingerprint; });
}

bool VerdictSource::hasCandidates() const noexcept {
    return std::any_of(members_.begin(), members_.end(), [](const Member& m) { return m.candidate; });
}

bool VerdictSource::candidateCaughtUp(uint64_t viewObjectId, uint64_t planFingerprint) const noexcept {
    return activated() && std::any_of(members_.begin(), members_.end(), [&](const Member& m) {
        return m.candidate && m.objectId == viewObjectId && m.fingerprint == planFingerprint && checkpointSettled(*m.inst) && m.inst->foldCursor >= judgeCursor_;
    });
}

void VerdictSource::addMember_(ViewInstance* inst, rune::view::VerdictViewKind kind, uint64_t viewObjectId, uint64_t planFingerprint, bool candidate) {
    Member m;
    m.candidate = candidate;
    m.inst = inst;
    m.kind = kind;
    m.objectId = viewObjectId;
    m.fingerprint = planFingerprint;
    m.prospect.reserve(inst->layout, inst->agg ? &inst->agg->layout : nullptr);
    if (kind == rune::view::VerdictViewKind::KeyIndex) {
        const uint16_t keyField = inst->plan.keyFieldId[0];
        const auto it = std::find_if(inst->evolvingSrc.fields.begin(), inst->evolvingSrc.fields.end(), [&](const auto& f) { return f.id == keyField; });
        const uint32_t width = it == inst->evolvingSrc.fields.end() ? 0 : rune::codec::fieldWidth(*it);
        if (width == 0 || width > rune::codec::kMaxTopicKeyBytes) throw std::invalid_argument("key index: the source key field must be fixed-width, 1 to 16 bytes");
        m.keyWidth = static_cast<uint8_t>(width);
    }
    auto at = std::lower_bound(members_.begin(), members_.end(), viewObjectId, [](const Member& x, uint64_t id) { return x.objectId < id; });
    at = members_.insert(at, std::move(m));
    if (!needsActivation_ && generation_ != kUnbound) discardCheckpointAhead_(*at);
    reindex_();
    recomputeCap_();
}

// P4-4: a checkpoint that outran every marker this node holds (a crash lost the log tail carrying them) cannot be trusted.
void VerdictSource::discardCheckpointAhead_(Member& m) {
    if (!m.inst->persisted || !checkpointSettled(*m.inst) || m.inst->foldCursor <= judgeCursor_ || hooks_.discard == nullptr) return;
    hooks_.discard(hooks_.ctx, *m.inst, ViewDiscardReason::AheadOfVerdicts);   // the owner rebuilds it from the log; the cursor is its business
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
    // §8.5: the markers in this leader's log commit with it (a leader never truncates), so it marks only above them.  Not from
    // W: a lagging HW puts W below ranges the cleaner may already have compacted, and a re-mark from this index would disagree.
    emitFrom_    = std::max(extendOverEmptySets_(state.markers.appendedThrough()), logStart_);
    state.rejected.noteCovered(judgeCursor_);
    for (Member& m : members_) discardCheckpointAhead_(m);
    activeSet_ = *state.constraintSets.setAt(judgeCursor_);
    setFrom_   = judgeCursor_;
    sawEmptyChange_ = false;
    judgedRecord_   = judgeCursor_ > std::max(emitFrom_, state.markers.committedMarkerEnd());   // the committed marker itself is no record to cover
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
    // #791 diagnostics (cold, once per wait): the first member of the active set no installed view matches, and where that set starts.
    uint64_t missingObject = 0, missingFingerprint = 0;
    for (uint8_t i = 0; i < activeSet_.count; ++i) {
        const ConstraintSetMember& want = activeSet_.members[i];
        const bool have = std::any_of(members_.begin(), members_.end(), [&](const Member& m) {
            return !m.retiring && m.objectId == want.viewObjectId && m.fingerprint == want.planFingerprint;
        });
        if (!have) { missingObject = want.viewObjectId; missingFingerprint = want.planFingerprint; break; }
    }
    spdlog::warn("verdict judge waits for a view install: topic {} partition {}: missing member (object {}, fingerprint {}) of set {} "
                 "(change at offset {}), judged through {}, {} members installed, {} of {} active",
                 srcTopicId_, partition_, missingObject, missingFingerprint, activeSet_.id, setFrom_, judgeCursor_, members_.size(),
                 active_.size(), activeSet_.count);
}

rune::view::PrepareStatus VerdictSource::prepare_(Member& m, const rune::view::SourceRecord& rec) noexcept {
    ViewInstance& i = *m.inst;
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
        const KeyIndexFold f{i.table, m.keyWidth};
        return prepareKeyIndex(f, rec, m.prospect);
    }
    if (i.agg) {
        rune::view::AggregateFold f = aggregateFoldOf(i);
        return rune::view::prepareAggregate(f, rec, m.prospect);
    }
    rune::view::ProjectionFold f = projectionFoldOf(i);
    return rune::view::prepareProjection(f, rec, m.prospect);
}

void VerdictSource::apply_(Member& m, const rune::view::SourceRecord& rec) noexcept {
    ViewInstance& i = *m.inst;
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
        KeyIndexFold f{i.table, m.keyWidth};
        applyKeyIndex(f, rec, m.prospect);
        stats_.keyIndexSkippedMalformed += m.prospect.stats.skippedMalformed;
        return;
    }
    if (i.agg) {
        rune::view::AggregateFold f = aggregateFoldOf(i);
        rune::view::applyAggregate(f, rec, m.prospect);
    } else {
        rune::view::ProjectionFold f = projectionFoldOf(i);
        rune::view::applyProjection(f, rec, m.prospect);
    }
    const rune::view::FoldStats& st = m.prospect.stats;
    i.counters.skippedMalformed += st.skippedMalformed;
    i.counters.skippedNoGroup   += st.skippedNoGroup;
    i.counters.aggOverflow      += st.aggOverflow;
    i.counters.aggNonFinite     += st.aggNonFinite;
    i.counters.aggInconsistent  += st.aggInconsistent;
    i.counters.aggRescans       += st.aggRescans;
    i.counters.skippedNoKey     += st.skippedNoKey;
    i.counters.ownerMoves       += st.ownerMoves;
    i.counters.uniquePromotions += st.uniquePromotions;
}

// The key index is never checkpointed: nothing will encode its dirty rows, so they are dropped (WorkerViewSet::discardDirty_'s rule).
void VerdictSource::discardKeyIndexDirty_(ViewInstance& inst) noexcept {
    inst.table.drainDirtyKeyed([](uint32_t, const uint8_t*, bool, std::span<const uint8_t>) noexcept {});
}

Verdict VerdictSource::verdictOf_(const Member& m, const rune::view::SourceRecord& rec) const noexcept {
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) return evaluateIntent(rec, m.prospect);
    if (m.kind == rune::view::VerdictViewKind::Unique || m.kind == rune::view::VerdictViewKind::Check)
        return evaluateConstraints(ConstraintFold{m.inst->plan, m.inst->layout, m.inst->table}, rec, m.prospect);
    if (m.kind != rune::view::VerdictViewKind::TestReject || rec.value.empty()) return {};   // tombstones are never rejected
    const Verdict reject{static_cast<uint16_t>(rune::protocol::client::ErrorCode::ConstraintViolation), rune::view::constraintIdOf(m.objectId, 1)};
    switch (m.rule.mode) {
    case TestRejectRule::Mode::TagEquals: {
        if (rec.writer == nullptr) return {};
        for (const auto& f : rec.writer->fields) {
            if (f.name != "tag") continue;
            const auto v = rune::codec::readField(*rec.writer, {reinterpret_cast<const uint8_t*>(rec.value.data()), rec.value.size()}, f.id);
            return v.present && (v.i == m.rule.tag || v.u == m.rule.tag) ? reject : Verdict{};
        }
        return {};
    }
    case TestRejectRule::Mode::OffsetSet:
        return std::find(m.rule.offsets.begin(), m.rule.offsets.begin() + m.rule.offsetCount, rec.offset) != m.rule.offsets.begin() + m.rule.offsetCount ? reject : Verdict{};
    case TestRejectRule::Mode::CapLiveKeys:
        return m.prospect.op == rune::view::ProspectOp::Upsert && !m.prospect.rows[0].existed && m.inst->table.size() >= m.rule.cap ? reject : Verdict{};
    }
    return {};
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
        const Verdict v = verdictOf_(members_[i], rec);
        if (v.accepted()) continue;
        if (!state_->rejected.add(rec.offset, v.code, v.constraintId)) {
            stall_(state_->rejected.size() >= state_->rejected.capacity() ? StallReason::Full : StallReason::OutOfOrder);
            return {};
        }
        if (state_->rejected.size() * 4 >= state_->rejected.capacity() * 3) state_->growOwed = true;
        using rune::protocol::client::ErrorCode;
        if (v.code == static_cast<uint16_t>(ErrorCode::KeyExists)) ++stats_.rejectionsKeyExists;
        else if (v.code == static_cast<uint16_t>(ErrorCode::KeyNotFound)) ++stats_.rejectionsKeyNotFound;
        else ++stats_.rejectionsConstraint;
        ++rejectedThisPass_;
        // 56 and 57 file under constraintId 0 here; rejectionsKeyExists / rejectionsKeyNotFound tell them apart.
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

void VerdictSource::noteKeyZeroRejection_(const Verdict& v, const rune::view::SourceRecord& rec) noexcept {
    if (v.code != static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyExists)) {
        ++state_->autoIncStats.idsBurned;
        return;
    }
    ++state_->autoIncStats.assignedKeyExists;   // an assigned id the key index already holds: the counter reused it
    spdlog::critical("AUTOINC-ID-REUSE topic={} partition={} id={}", srcTopicId_, partition_, keyIndexWord(rec.key));
}

int VerdictSource::replay_(Member& m, const rune::storage::LogReadView& view, int64_t upTo, const rune::view::AbortedTxnFilter& aborted) {
    ViewInstance& inst = *m.inst;
    const rune::view::FoldSkip skip{.aborted = aborted, .rejected = &state_->rejected};
    int64_t cursor = inst.foldCursor;
    const int64_t bound = inst.maxRecordsPerPass > 0 ? std::min(upTo, cursor + inst.maxRecordsPerPass) : upTo;
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
        KeyIndexFold f{inst.table, m.keyWidth};
        const rune::view::FoldStats st = foldKeyIndexBatch(f, view, bound, cursor, skip);
        inst.foldCursor = cursor;
        inst.counters.skippedAborted += st.skippedAborted;
        discardKeyIndexDirty_(inst);
        return static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted + st.skippedMalformed + st.skippedRejected);
    }
    rune::view::FoldStats st;
    if (inst.keyed && inst.agg) {
        rune::view::AggregateFold f = aggregateFoldOf(inst);
        st = rune::view::foldKeyedAggregateBatch(f, inst.prospect, view, bound, cursor, skip);
    } else if (inst.keyed) {
        rune::view::ProjectionFold f = projectionFoldOf(inst);
        st = rune::view::foldProjectionBatch(f, inst.prospect, view, bound, cursor, skip);
    } else {
        st = inst.agg ? rune::view::foldAggregateBatch(inst.plan, inst.layout, inst.agg->layout, inst.vdc, inst.table, inst.agg->contributors,
                                                       inst.agg->groups, inst.agg->blocksOrNull(), view, bound, cursor, skip)
                      : rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view, bound, cursor, skip);
    }
    inst.foldCursor = cursor;
    inst.counters.skippedAborted   += st.skippedAborted;
    inst.counters.skippedMalformed += st.skippedMalformed;
    inst.counters.skippedNoGroup   += st.skippedNoGroup;
    inst.counters.aggOverflow      += st.aggOverflow;
    inst.counters.aggNonFinite     += st.aggNonFinite;
    inst.counters.aggInconsistent  += st.aggInconsistent;
    inst.counters.aggRescans       += st.aggRescans;
    inst.counters.skippedNoKey     += st.skippedNoKey;
    inst.counters.ownerMoves       += st.ownerMoves;
    inst.counters.uniquePromotions += st.uniquePromotions;
    if (inst.claims) inst.counters.uniqueConflicts = inst.claims->extras();
    return static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted + st.skippedMalformed + st.skippedNoGroup + st.skippedNoKey + st.skippedRejected);
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
        // P6-13: a key-0 batch this leadership appended (record() wrote its window entry); an import or a rebuilt entry is not one.
        // Coverage gap: a producer more than 32 key-0 batches ahead of a lagging or held judge has evicted the entry, so that
        // batch's rejected ids go uncounted in idsBurned and a KeyExists on it raises no AUTOINC-ID-REUSE.
        const bool keyZero = state_->assignedBases.producers() != 0 && r.producerId() >= 0 && state_->assignedBases.isKeyZeroBatch(r.producerId(), r.baseOffset());
        const bool skipBatch = (attrs & proto::batch::kAttrControl) != 0 || aborted.skips(r, attrs);
        if (skipBatch) {
            if (last >= limit) break;
            if (keyZero && (attrs & proto::batch::kAttrControl) == 0) [[unlikely]] state_->autoIncStats.idsBurned += static_cast<uint64_t>(r.lastOffsetDelta()) + 1;   // aborted
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
            const Verdict v = judge(rec);
            if (stalled_) return;
            if (keyZero && !v.accepted()) [[unlikely]] noteKeyZeroRejection_(v, rec);
            judgeCursor_ = span.offset + 1;
            judgedRecord_ = true;
            ++work;
        });
        if (!walked && !halt && !stalled_) break;   // a record that does not fit its batch: stop, never advance past it
        if (stalled_) break;
        judgeCursor_ = halt ? limit : std::max(judgeCursor_, last + 1);
        pos += static_cast<std::size_t>(r.totalLength());
    }
    for (const std::size_t i : active_) {
        ViewInstance& inst = *members_[i].inst;
        inst.foldCursor = judgeCursor_;
        if (members_[i].kind == rune::view::VerdictViewKind::KeyIndex) discardKeyIndexDirty_(inst);
        if (inst.claims) inst.counters.uniqueConflicts = inst.claims->extras();
    }
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
        if (inst.persisted && state.markers.appendedThrough() < inst.foldCursor && !inst.pendingValid && checkpointSettled(inst)) continue;
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

int64_t VerdictSource::requestChange(const ConstraintSet& next, bool refresh) {
    if (state_ == nullptr || generation_ == kUnbound || needsActivation_) return kChangeDeferred;
    if (!refresh && next.sameMembers(state_->constraintSets.latest())) return kChangeUnchanged;
    ConstraintSet logged = next;
    logged.id = state_->constraintSets.latest().id + 1;
    std::array<cr::ChangeMember, kMaxVerdictViewsPerSource> wire{};
    for (uint8_t i = 0; i < logged.count; ++i)
        wire[i] = cr::ChangeMember{logged.members[i].viewObjectId, static_cast<uint8_t>(logged.members[i].kind), logged.members[i].planFingerprint};
    const std::size_t n = cr::buildConstraintSetChange(scratch_, logged.id, logged.catalogThrough, std::span<const cr::ChangeMember>(wire.data(), logged.count));
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
    return base;
}

void verdictOnHw(PartitionWriteState& state, int64_t hw) noexcept {
    state.markers.onHw(hw);
    state.rejected.setWatermark(state.markers.committedThrough());
}

}  // namespace rune::node
