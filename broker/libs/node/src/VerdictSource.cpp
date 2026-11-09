#include <cassert>
#include <rune/node/VerdictSource.h>
#include <rune/node/lease/LeaseEntryStore.h>

#include <rune/codec/Record.h>
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
    Member m;
    m.inst = inst;
    m.kind = kind;
    m.objectId = viewObjectId;
    m.fingerprint = planFingerprint;
    m.prospect.reserve(inst->layout, inst->agg ? &inst->agg->layout : nullptr);
    if (kind == rune::view::VerdictViewKind::KeyIndex || kind == rune::view::VerdictViewKind::LeaseIndex) {
        const bool lease = kind == rune::view::VerdictViewKind::LeaseIndex;
        const uint16_t keyField = inst->plan.keyFieldId[0];
        const auto it = std::find_if(inst->evolvingSrc.fields.begin(), inst->evolvingSrc.fields.end(), [&](const auto& f) { return f.id == keyField; });
        const uint32_t width = it == inst->evolvingSrc.fields.end() ? 0 : rune::codec::fieldWidth(*it);
        if (width == 0 || width > (lease ? rune::codec::kMaxLeaseKeyBytes : rune::codec::kMaxTopicKeyBytes))
            throw std::invalid_argument(lease ? "lease index: the source key field must be fixed-width, 1 to 128 bytes"
                                              : "key index: the source key field must be fixed-width, 1 to 16 bytes");
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
    // A lease index may have been rebuilt with another row layout (a new column) since the prospect was reserved; its rows
    // change ids with the rebuild, so the events of the old table are void.
    for (Member& m : members_)
        if (m.kind == rune::view::VerdictViewKind::LeaseIndex) {
            m.prospect.reserve(m.inst->layout, nullptr);
            m.inst->rebuildEpoch = hooks_.nextLeaseEpoch != nullptr ? hooks_.nextLeaseEpoch(hooks_.ctx) : m.inst->rebuildEpoch + 1;
        }
    state.leaseEvents.buf.clear();
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

// Cold (once per wait): the judge or a replay holds at a lease record whose writer layout is not installed on this worker yet.
void VerdictSource::noteLayoutWait_(int64_t offset, uint16_t version) noexcept {
    if (layoutWaitVersion_ == version) return;   // once per (topic, partition, version): this source is the (topic, partition)
    layoutWaitVersion_ = version;
    ++stats_.leaseLayoutWaits;
    spdlog::warn("lease index waits for a schema layout: topic {} partition {}: the record at offset {} is written at version {}, which is "
                 "not installed (or has a field the index layout lacks); held until it is",
                 srcTopicId_, partition_, offset, version);
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
    if (m.kind == rune::view::VerdictViewKind::LeaseIndex) {
        // The writer is the spec's descriptor of the record's version (the one the produce hook validated against), never the
        // instance's own evolving copy: that lags the spec after a Drop and when the schema ops arrive in either order.
        rune::view::SourceRecord r = rec;
        r.writer = state_ != nullptr && state_->spec != nullptr ? leaseWriterOf(*state_->spec, r.value) : nullptr;
        const LeaseIndexFold f{i.table, i.layout, i.evolvingSrc, m.keyWidth, &i.leaseCover};
        const bool refused = r.writer == nullptr && state_ != nullptr && state_->spec != nullptr && r.value.size() >= 4 &&
                             state_->spec->versionRefused(detail::le16(r.value.data() + 2));   // a refused version is a definite answer
        return prepareLeaseIndex(f, r, m.prospect, refused, state_ != nullptr && state_->spec != nullptr ? leaseInstallSeqOf(*state_->spec, r.value) : 0u);
    }
    bool skip = false;
    const rune::view::SourceRecord r = dataRecord_(rec, skip);
    if (skip) {   // a Checkpoint: invisible to a data member
        m.prospect.clear();
        return rune::view::PrepareStatus::Ok;
    }
    if (i.agg) {
        rune::view::AggregateFold f{i.plan, i.layout, i.agg->layout, i.vdc, i.table, i.agg->contributors, i.agg->groups, i.agg->blocksOrNull()};
        return rune::view::prepareAggregate(f, r, m.prospect);
    }
    rune::view::ProjectionFold f{i.plan, i.layout, i.vdc, i.table, rune::view::viewKeyType(i.plan)};
    return rune::view::prepareProjection(f, r, m.prospect);
}

// What a data member (not the key / lease index) sees of `rec` (spec 6.6): on a lease topic a Checkpoint is skipped and a
// Revoke / Expire becomes a tombstone (empty value, no writer).  One helper for prepare_, apply_ and the verdict, so they agree.
rune::view::SourceRecord VerdictSource::dataRecord_(const rune::view::SourceRecord& rec, bool& skip) const noexcept {
    rune::view::SourceRecord r = rec;
    skip = false;
    if (state_ == nullptr || state_->spec == nullptr || !state_->spec->lease) return r;
    if (!rune::view::leaseDataView({.lease = true}, r.attributes, r.value)) {
        skip = true;
        return r;
    }
    if (r.value.empty()) r.writer = nullptr;
    return r;
}

void VerdictSource::apply_(Member& m, const rune::view::SourceRecord& rec) noexcept {
    ViewInstance& i = *m.inst;
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
        KeyIndexFold f{i.table, m.keyWidth};
        applyKeyIndex(f, rec, m.prospect);
        stats_.keyIndexSkippedMalformed += m.prospect.stats.skippedMalformed;
        return;
    }
    if (m.kind == rune::view::VerdictViewKind::LeaseIndex) {
        stats_.keyIndexSkippedMalformed += m.prospect.stats.skippedMalformed;
        LeaseIndexFold f{i.table, i.layout, i.evolvingSrc, m.keyWidth, &i.leaseCover};
        LeaseEvent ev;
        if (applyLeaseRecord(f, rec, m.prospect, ev)) state_->leaseEvents.buf.push_back(ev);   // grows only past kLeaseFoldEventCap (judgeBatches_ counts it)
        return;
    }
    bool skip = false;
    const rune::view::SourceRecord r = dataRecord_(rec, skip);
    if (skip) return;   // a Checkpoint applies nothing (prepare_ cleared the prospect)
    if (i.agg) {
        rune::view::AggregateFold f{i.plan, i.layout, i.agg->layout, i.vdc, i.table, i.agg->contributors, i.agg->groups, i.agg->blocksOrNull()};
        rune::view::applyAggregate(f, r, m.prospect);
    } else {
        rune::view::ProjectionFold f{i.plan, i.layout, i.vdc, i.table, rune::view::viewKeyType(i.plan)};
        rune::view::applyProjection(f, r, m.prospect);
    }
    const rune::view::FoldStats& st = m.prospect.stats;
    i.counters.skippedMalformed += st.skippedMalformed;
    i.counters.skippedNoGroup   += st.skippedNoGroup;
    i.counters.aggOverflow      += st.aggOverflow;
    i.counters.aggNonFinite     += st.aggNonFinite;
    i.counters.aggInconsistent  += st.aggInconsistent;
    i.counters.aggRescans       += st.aggRescans;
}

// The key index is never checkpointed: nothing will encode its dirty rows, so they are dropped (WorkerViewSet::discardDirty_'s rule).
void VerdictSource::discardKeyIndexDirty_(ViewInstance& inst) noexcept {
    inst.table.drainDirtyKeyed([](uint32_t, const uint8_t*, bool, std::span<const uint8_t>) noexcept {});
}

Verdict VerdictSource::verdictOf_(const Member& m, const rune::view::SourceRecord& rec) const noexcept {
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) return evaluateIntent(rec, m.prospect);
    if (m.kind == rune::view::VerdictViewKind::LeaseIndex) {
        using rune::protocol::client::ErrorCode;
        // A malformed lease record (a wrong key width, a value shorter than the system columns, a projection that does not fit)
        // is refused, never accepted without an apply: an accepted record the index never saw would let a second grant through.
        // The replay skips rejected offsets, so the log stays deterministic.
        if (m.prospect.op == rune::view::ProspectOp::None) {
            // A broker Expire / Checkpoint whose key has no row is a stale broker record, even when its projection is malformed:
            // it has no producer to retry, so it takes the reserved stale-broker slot like any other (judge()).
            namespace lr = rune::protocol::record;
            const lr::LeaseOp op = lr::leaseOpOf(rec.attributes);
            if ((op == lr::LeaseOp::Expire || op == lr::LeaseOp::Checkpoint) && lr::intentOf(rec.attributes) == 0 && !m.prospect.rows[0].existed)
                return {rune::view::kVerdictStaleBroker, 0};
            return {static_cast<uint16_t>(ErrorCode::InvalidRequest), 0};
        }
        if (rec.value.empty()) return {static_cast<uint16_t>(ErrorCode::InvalidRequest), 0};   // a lease record carries its full row
        const ViewInstance& i = *m.inst;
        const uint8_t* row = m.prospect.rows[0].existed ? i.table.rowOf(m.prospect.rows[0].rid) : nullptr;   // found once, by prepare_
        const LeaseRowState st{row != nullptr, row != nullptr ? leaseRowVersion(row, i.layout) : 0, row != nullptr ? leaseRowTtl(row, i.layout) : 0};
        const uint32_t maxKeys = state_ != nullptr && state_->spec != nullptr && state_->spec->lease ? state_->spec->lease->maxKeys : UINT32_MAX;
        return {judgeLease(st, rec.attributes, leaseRecordVersion(rec.value), leaseRecordRemaining(rec.value), i.table.size(), maxKeys), 0};
    }
    if (m.kind != rune::view::VerdictViewKind::TestReject) return {};
    bool skipped = false;
    const rune::view::SourceRecord dr = dataRecord_(rec, skipped);
    if (skipped || dr.value.empty()) return {};   // tombstones (a lease Revoke / Expire too) and lease checkpoints are never rejected
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
        const rune::view::PrepareStatus ps = prepare_(members_[i], rec);
        if (ps == rune::view::PrepareStatus::CapacityExhausted) {
            stall_(StallReason::Capacity);
            return {};
        }
        if (ps == rune::view::PrepareStatus::Pending) return Verdict::held();   // the record's layout is not installed yet: no verdict, the caller holds
    }
    for (const std::size_t i : active_) {
        const Verdict v = verdictOf_(members_[i], rec);
        if (v.accepted()) continue;
        // A stale broker record (Expire / Checkpoint) has no producer to retry: when the index is full it takes the one
        // reserved slot, so a broker record never stalls the judge.  Everything else stalls on a full index, as before.
        const bool stale = v.code == rune::view::kVerdictStaleBroker;
        const uint64_t outOfOrder = stale ? state_->rejected.outOfOrder() : 0;
        assert(!v.isHeld() && "a held verdict is never recorded");
        bool added = state_->rejected.add(rec.offset, v.code, v.constraintId);
        if (!added && stale && state_->rejected.outOfOrder() == outOfOrder) added = state_->rejected.add(rec.offset, v.code, v.constraintId, /*reserved=*/true);
        if (!added) {
            stall_(state_->rejected.size() >= state_->rejected.capacity() ? StallReason::Full : StallReason::OutOfOrder);
            return {};
        }
        if (state_->rejected.size() * 4 >= state_->rejected.capacity() * 3) state_->growOwed = true;
        using rune::protocol::client::ErrorCode;
        if (v.code == static_cast<uint16_t>(ErrorCode::KeyExists)) ++stats_.rejectionsKeyExists;
        else if (v.code == static_cast<uint16_t>(ErrorCode::KeyNotFound)) ++stats_.rejectionsKeyNotFound;
        else if (v.code == static_cast<uint16_t>(ErrorCode::LeaseNotFound)) ++stats_.rejectionsLeaseNotFound;
        else if (v.code == static_cast<uint16_t>(ErrorCode::LeaseLimitExceeded)) ++stats_.rejectionsLeaseLimit;
        else if (stale) {
            ++stats_.leaseRejectedStaleBroker;
            if (!state_->staleBrokerLogged) {
                state_->staleBrokerLogged = true;
                spdlog::warn("lease judge: topic {} partition {}: a stale broker record (offset {}) was rejected; further ones are counted only",
                             srcTopicId_, partition_, rec.offset);
            }
            // The wheels learn that a record of theirs was refused (LeaseEntryEvent::OwnRecordRejected).  Live judging only: judge() is
            // the live path (replay and rebuild fold through foldLeaseIndexBatch, which never reaches here), the same path that pushes the
            // accepted events in apply_().  A record is accepted (apply_ pushes one event) XOR rejected (this branch pushes one), so the
            // fold-event cap accounting (one event per record) holds.
            if (members_[i].kind == rune::view::VerdictViewKind::LeaseIndex) {
                const rune::view::ProspectRow& pr = members_[i].prospect.rows[0];
                LeaseEvent ev;
                ev.kind   = LeaseEventKind::BrokerRejected;
                ev.rid    = pr.existed ? pr.rid : lease::kLeaseNil;
                ev.offset = rec.offset;
                ev.version = leaseRecordVersion(rec.value);   // the version the refused record carries (the entry compares it with its row)
                state_->leaseEvents.buf.push_back(ev);
            }
        }
        else ++stats_.rejectionsConstraint;
        if (members_[i].kind == rune::view::VerdictViewKind::LeaseIndex && members_[i].prospect.op == rune::view::ProspectOp::None)
            stats_.keyIndexSkippedMalformed += members_[i].prospect.stats.skippedMalformed;
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
    const rune::view::FoldSkip skip{.aborted = aborted, .rejected = &state_->rejected,
                                    .traits = {.lease = state_->spec != nullptr && state_->spec->lease}};
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
    if (m.kind == rune::view::VerdictViewKind::LeaseIndex) {
        LeaseIndexFold f{inst.table, inst.layout, inst.evolvingSrc, m.keyWidth, &inst.leaseCover};
        bool held = false;
        uint16_t heldVersion = 0;
        const rune::view::FoldStats st = foldLeaseIndexBatch(f, *state_->spec, m.prospect, view, bound, cursor, skip, &held, &heldVersion);
        inst.foldCursor = cursor;
        if (held) noteLayoutWait_(cursor, heldVersion);   // the replay stops AT the record; the next duty cycle retries
        else layoutWaitVersion_ = -1;
        inst.counters.skippedAborted += st.skippedAborted;
        discardKeyIndexDirty_(inst);
        return static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted + st.skippedMalformed + st.skippedRejected);
    }
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
    const bool leaseSource = std::any_of(active_.begin(), active_.end(), [&](std::size_t i) { return members_[i].kind == rune::view::VerdictViewKind::LeaseIndex; });
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
        if (leaseSource) {
            // Back-pressure: never drop an event, never stall.  A batch that would overflow a non-empty buffer waits for the
            // consumer; one that alone exceeds the whole cap is judged into an empty buffer (it grows once, counted).
            const std::size_t records = static_cast<std::size_t>(last - r.baseOffset() + 1);
            const std::size_t held = state_->leaseEvents.buf.size();
            if (held != 0 && held + records > kLeaseFoldEventCap) break;
            if (held == 0 && records > kLeaseFoldEventCap) ++stats_.leaseEventOverflows;
        }
        judgeCursor_ = std::max(judgeCursor_, r.baseOffset());
        bool halt = false, hold = false;
        const bool walked = rune::protocol::forEachRecordSpan(view.data + pos, view.len - pos, [&](const rune::protocol::RecordSpan& span) {
            if (halt || hold || stalled_) return;
            if (span.offset >= limit) { halt = true; return; }
            if (span.offset < judgeCursor_) return;
            rune::view::SourceRecord rec{span.key, span.value, span.offset, rune::protocol::record::intentOf(span.attributes), nullptr, span.attributes};
            if (!span.value.empty())
                rec.writer = leaseSource ? leaseWriterOf(*state_->spec, span.value)
                                         : &first.vdc.materializedFor(writerVersionOf(span.value, first.vdc.defaultVersion()));
            lastIntent_ = rec.intent;
            const Verdict v = judge(rec);
            if (v.isHeld()) {   // hold AT this record: no verdict, no advance (like back-pressure); the next duty cycle retries
                hold = true;
                noteLayoutWait_(span.offset, writerVersionOf(span.value, 0));
                return;
            }
            layoutWaitVersion_ = -1;
            if (stalled_) return;
            if (keyZero && !v.accepted()) [[unlikely]] noteKeyZeroRejection_(v, rec);
            judgeCursor_ = span.offset + 1;
            judgedRecord_ = true;
            ++work;
        });
        if (hold) break;   // judgeCursor_ is the held record's offset
        if (!walked && !halt && !stalled_) break;   // a record that does not fit its batch: stop, never advance past it
        if (stalled_) break;
        judgeCursor_ = halt ? limit : std::max(judgeCursor_, last + 1);
        pos += static_cast<std::size_t>(r.totalLength());
    }
    for (const std::size_t i : active_) {
        members_[i].inst->foldCursor = judgeCursor_;
        if (members_[i].kind == rune::view::VerdictViewKind::KeyIndex || members_[i].kind == rune::view::VerdictViewKind::LeaseIndex)
            discardKeyIndexDirty_(*members_[i].inst);
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
    return base;
}

void verdictOnHw(PartitionWriteState& state, int64_t hw) noexcept {
    state.markers.onHw(hw);
    state.rejected.setWatermark(state.markers.committedThrough());
}

}  // namespace rune::node
