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

// The key sets' other producers' markers, and the one-step set's keys at least (an ordinary transaction has no record bound).
constexpr std::size_t kTxnMarkersReserve = 1024, kOneStepKeysReserve = 1024;

}  // namespace

void TxnKeySet::reserve(std::size_t keys, std::size_t markers) {
    std::size_t slots = 16;
    while (slots < 2 * keys) slots <<= 1;
    slots_.assign(slots, Key{});
    used_.clear();
    used_.reserve(keys);
    markers_.clear();
    markers_.reserve(markers);
    pending_.clear();
    pending_.reserve(markers);
    keyCap_ = keys;
    markersOverflowed_ = false;
}

void TxnKeySet::clear() noexcept {
    for (const uint32_t i : used_) slots_[i] = Key{};
    used_.clear();
    clearMarkers();
}

void TxnKeySet::clearMarkers() noexcept {
    markers_.clear();
    pending_.clear();
    markersOverflowed_ = false;
}

// The key's slot, or the free one it would take (at most half the slots are used).
std::size_t TxnKeySet::slotOf_(std::string_view key) const noexcept {
    uint64_t h = 0xcbf2'9ce4'8422'2325ull;
    for (const char c : key) h = (h ^ static_cast<uint8_t>(c)) * 0x100'0000'01b3ull;
    const std::size_t mask = slots_.size() - 1;
    for (std::size_t i = static_cast<std::size_t>(h) & mask;; i = (i + 1) & mask) {
        const Key& k = slots_[i];
        if (!k.used || (k.len == key.size() && std::memcmp(k.bytes.data(), key.data(), key.size()) == 0)) return i;
    }
}

bool TxnKeySet::note(std::string_view key, int64_t offset) noexcept {
    if (key.size() > rune::codec::kMaxTopicKeyBytes) return true;
    if (slots_.empty()) return false;
    const std::size_t i = slotOf_(key);
    Key& k = slots_[i];
    if (!k.used) {
        if (used_.size() == keyCap_) return false;
        k.used = true;
        k.len = static_cast<uint8_t>(key.size());
        std::memcpy(k.bytes.data(), key.data(), key.size());
        used_.push_back(static_cast<uint32_t>(i));
    }
    k.last = offset;
    return true;
}

TxnKeySet::Key* TxnKeySet::find(std::string_view key) noexcept {
    if (key.size() > rune::codec::kMaxTopicKeyBytes || slots_.empty()) return nullptr;
    Key& k = slots_[slotOf_(key)];
    return k.used ? &k : nullptr;
}

void TxnKeySet::noteMarker(int64_t producerId, int64_t offset, bool commit) noexcept {
    if (markers_.size() == markers_.capacity()) {
        markersOverflowed_ = true;
        return;
    }
    if (const auto p = std::find(pending_.begin(), pending_.end(), producerId); p != pending_.end()) {
        *p = pending_.back();
        pending_.pop_back();
    }
    markers_.push_back({producerId, offset, commit, false});
}

void TxnKeySet::noteJudge(int64_t producerId, int16_t epoch, int64_t first, int64_t offset) noexcept {
    if (markers_.size() == markers_.capacity()) {
        markersOverflowed_ = true;
        return;
    }
    if (!awaits(producerId)) pending_.push_back(producerId);
    markers_.push_back({producerId, offset, false, true, epoch, first});
}

bool TxnKeySet::awaits(int64_t producerId) const noexcept { return std::find(pending_.begin(), pending_.end(), producerId) != pending_.end(); }

bool TxnKeySet::resolve(int64_t producerId, int64_t offset, bool commit) noexcept {
    if (!awaits(producerId)) return false;
    noteMarker(producerId, offset, commit);
    return true;
}

TxnKeySet::Applied TxnKeySet::appliedAt(int64_t producerId, int16_t epoch, int64_t base) const noexcept {
    const auto of = [&](const Marker& x) { return x.producerId == producerId; };
    const auto decides = [&](const Marker& x) { return of(x) && x.offset > base && (!x.judge || (x.epoch == epoch && x.first <= base)); };
    const auto e = std::find_if(markers_.begin(), markers_.end(), decides);
    if (e == markers_.end()) return markersOverflowed_ ? Applied::Unknown : Applied::No;
    const auto m = e->judge ? std::find_if(e + 1, markers_.end(), [&](const Marker& x) { return of(x) && !x.judge; }) : e;
    if (m == markers_.end()) return Applied::Unknown;
    return m->commit ? Applied::Yes : Applied::No;
}

void TxnOverlayPool::reserveKeys(std::size_t keys) {
    keysWanted_ = std::max(keysWanted_, keys);
    keysPending_ = false;
    for (std::size_t i = 0; i < keys_.size(); ++i) {
        if (keys_[i].keyCapacity() >= keysWanted_) continue;
        if (inUse_[i]) keysPending_ = true;
        else keys_[i].reserve(keysWanted_, kTxnMarkersReserve);
    }
    if (oneStep_.keyCapacity() == 0) oneStep_.reserve(kOneStepKeysReserve, kTxnMarkersReserve);
}

rune::view::PrepareStatus prepareKeyIndex(const KeyIndexFold& f, const rune::view::SourceRecord& rec, rune::view::Prospect& p,
                                          rune::view::TxnOverlay* chain) noexcept {
    p.clear();
    if (rec.key.size() != f.keyWidth) {
        p.stats.skippedMalformed = 1;
        return rune::view::PrepareStatus::Malformed;
    }
    if (chain != nullptr && !rune::view::rowsFit(*chain, {&f.table})) return rune::view::PrepareStatus::CapacityExhausted;
    rune::view::TxnAccess x{chain, f.locks, f.member};
    rune::view::TxnTable table(&f.table, rune::view::OverlayTable::KeyIndex, chain != nullptr || f.locks != nullptr ? &x : nullptr);
    rune::view::ProspectRow& r = p.rows[0];
    r.key      = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(rec.key.data()), rec.key.size()});
    r.existed  = table.get(r.key) != nullptr;
    r.removed  = rec.value.empty();
    r.image    = nullptr;
    p.rowCount = 1;
    p.op       = r.removed ? rune::view::ProspectOp::Erase : rune::view::ProspectOp::Upsert;
    if (chain != nullptr && !x.locked) {
        if (r.removed) table.erase(r.key);
        else *table.put(r.key) = 1;
    }
    if (x.locked) return rune::view::PrepareStatus::Locked;
    return chain != nullptr && chain->full() ? rune::view::PrepareStatus::CapacityExhausted : rune::view::PrepareStatus::Ok;
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
                                        const rune::view::FoldSkip& skip, rune::view::TxnFold* txn) {
    rune::view::FoldStats stats{};
    rune::view::Prospect  p;
    rune::view::walkFold(view, bound, cursor, skip, txn, stats, [&](std::string_view key, std::string_view value, int64_t offset) {
        const rune::view::SourceRecord rec{key, value, offset, 0, nullptr};
        if (prepareKeyIndex(f, rec, p) == rune::view::PrepareStatus::Malformed) {
            ++stats.skippedMalformed;
            return;
        }
        applyKeyIndex(f, rec, p);
        ++(value.empty() ? stats.deleted : stats.applied);
    });
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
            // F11: inside a transaction the before-image is the chain's prior image, which a plain in-order fold would hold.
            const uint8_t* before = !row.existed ? nullptr : p.chained ? row.before : f.table.get(row.key);
            if (rune::view::predicateReadsChanged(c.predicate, f.layout, before, row.image)) return violation(c.ordinal);
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

VerdictSource::VerdictSource(int32_t srcTopicId, int32_t partition)
    : srcTopicId_(srcTopicId), partition_(partition), sourceKey_((static_cast<uint64_t>(static_cast<uint32_t>(srcTopicId)) << 32) | static_cast<uint32_t>(partition)) {
    members_.reserve(kMaxVerdictViewsPerSource);
    active_.reserve(kMaxVerdictViewsPerSource);
    scratchIdx_.reserve(kMaxVerdictViewsPerSource);
    scratch_.resize(cr::markerBatchBytes(cr::kMaxMarkerEntries));
    entryScratch_.reserve(cr::kMaxMarkerEntries);
}

void VerdictSource::bindTxn(TxnOverlayPool* pool, std::span<rune::view::TxnOverlay> batchOverlay, int32_t txnJudgedRecordsMax) {
    txnPool_ = pool;
    batchOverlay_ = batchOverlay;
    txnJudgedRecordsMax_ = txnJudgedRecordsMax;
    pool->reserveKeys(static_cast<std::size_t>(std::max(txnJudgedRecordsMax, 0)));
}

VerdictSource::OverlayNeed VerdictSource::overlayNeed() const noexcept {
    OverlayNeed n;
    const auto rowLen = [&](const rune::view::ViewTable& t) { n.maxRowLen = std::max(n.maxRowLen, t.rowLen()); };
    for (const Member& m : members_) {
        const ViewInstance& inst = *m.inst;
        rowLen(inst.table);
        if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
            n.rowsPerRecord += 1;
        } else if (inst.agg) {
            n.rowsPerRecord += 4 * rune::view::RowOverlay::kSlotsPerTable + 3;
            rowLen(inst.agg->contributors);
            rowLen(inst.agg->groups);
            if (inst.agg->blocks) rowLen(*inst.agg->blocks);
            if (inst.agg->groupIds) rowLen(*inst.agg->groupIds);
        } else {
            // A projection's write set: the view row at its old and new key, its owner row, and with unique claims the claim and
            // extra rows at both keys; plus one.
            n.rowsPerRecord += 3 + (inst.claims ? 4 : 0) + 1;
        }
        if (inst.owners) rowLen(*inst.owners);
        if (inst.claims) {
            rowLen(inst.claims->indexTable());
            rowLen(inst.claims->extrasTable());
        }
    }
    return n;
}

void VerdictSource::releaseTxnOverlays(TxnJudgeState& txn) noexcept {
    txn.releaseOverlays([&](int32_t i) { txnPool_->release(i); });
    if (jx_.atMarker && jx_.overlay >= 0) txnPool_->release(jx_.overlay);   // a deferred transaction's overlay is its entry's
    releaseOneStep();
    jx_ = {};
    resetChainLive_();
    refreshLocks_();
}

void VerdictSource::addVerdictView(ViewInstance* inst, rune::view::VerdictViewKind kind, uint64_t viewObjectId, uint64_t planFingerprint,
                                   std::span<const uint8_t> planRecordPayload) {
    addMember_(inst, kind, viewObjectId, planFingerprint, false, planRecordPayload);
}

void VerdictSource::addCandidate(ViewInstance* inst, rune::view::VerdictViewKind kind, uint64_t viewObjectId, uint64_t planFingerprint,
                                 std::span<const uint8_t> planRecordPayload) {
    addMember_(inst, kind, viewObjectId, planFingerprint, true, planRecordPayload);
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

void VerdictSource::addMember_(ViewInstance* inst, rune::view::VerdictViewKind kind, uint64_t viewObjectId, uint64_t planFingerprint, bool candidate,
                              std::span<const uint8_t> planRecordPayload) {
    Member m;
    m.candidate = candidate;
    m.planRecordPayload.assign(planRecordPayload.begin(), planRecordPayload.end());
    if (scratch_.size() < cr::verdictPlanBatchBytes(planRecordPayload.size())) scratch_.resize(cr::verdictPlanBatchBytes(planRecordPayload.size()));
    m.inst = inst;
    m.kind = kind;
    m.objectId = viewObjectId;
    m.fingerprint = planFingerprint;
    m.prospect.reserve(inst->layout, inst->agg ? &inst->agg->layout : nullptr);
    std::array<bool, 256> taken{};
    for (const Member& x : members_) taken[x.slot] = true;
    m.slot = static_cast<uint8_t>(std::find(taken.begin(), taken.end(), false) - taken.begin());
    if (kind == rune::view::VerdictViewKind::KeyIndex) {
        const uint16_t keyField = inst->plan.keyFieldId[0];
        const auto it = std::find_if(inst->evolvingSrc.fields.begin(), inst->evolvingSrc.fields.end(), [&](const auto& f) { return f.id == keyField; });
        const uint32_t width = it == inst->evolvingSrc.fields.end() ? 0 : rune::codec::fieldWidth(*it);
        if (width == 0 || width > rune::codec::kMaxTopicKeyBytes) throw std::invalid_argument("key index: the source key field must be fixed-width, 1 to 16 bytes");
        m.keyWidth = static_cast<uint8_t>(width);
    }
    auto at = std::lower_bound(members_.begin(), members_.end(), viewObjectId, [](const Member& x, uint64_t id) { return x.objectId < id; });
    at = members_.insert(at, std::move(m));
    if (!needsActivation_ && generation_ != kUnbound && at->inst->persisted) discardAhead_(*at);
    reindex_();
    recomputeCap_();
}

// P4-4: a checkpoint that outran every marker this node holds (a crash lost the log tail carrying them) cannot be trusted.  On
// activation the same holds for a transient member that folded the judge's run-ahead past K: it is judged again from K, so its
// rows go (counted apart: no checkpoint was discarded).
void VerdictSource::discardAhead_(Member& m) {
    if (!checkpointSettled(*m.inst) || m.inst->foldCursor <= judgeCursor_ || hooks_.discard == nullptr) return;
    hooks_.discard(hooks_.ctx, *m.inst, ViewDiscardReason::AheadOfVerdicts);   // the owner rebuilds it from the log; the cursor is its business
    if (m.inst->persisted) ++stats_.verdictCheckpointsDiscarded;
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
    ++membersEpoch_;
    if (!resolveSet_(activeSet_, active_)) {
        active_.clear();
        return false;
    }
    waitingForInstall_ = false;
    for (const std::size_t i : active_) members_[i].candidate = false;   // the logged set names it: a swap another leader's markers already passed
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
    if (!state.txn.reserved()) state.txn.reserve();
    if (ordinary_.capacity() == 0) {
        ordinary_.reserve(TxnJudgeState::kMaxTxns);
        owedMarkers_.reserve(TxnJudgeState::kMaxTxns);
    }
    // Part 11: the transactions are judged anew from K.  The overlays go back first; nothing is cut from the table, so the owed
    // markers stay owed.
    releaseTxnOverlays(state.txn);
    state.txn.truncateFrom(INT64_MAX);
    ordinary_.clear();
    probe_ = {};
    txnStop_ = TxnStop::None;
    ordinaryApplied_ = -1;
    ordinaryWalk_ = {};
    finalizePid_ = finalizeApplied_ = -1;
    overlayWaitAt_ = lockPausedAt_ = -1;
    // An empty set is passed only below the LSO: an open transaction there is decided before K passes it.
    const int64_t covered = state.rejected.coveredThrough();
    judgeCursor_ = std::max(std::min(extendOverEmptySets_(covered), std::max(covered, lso_)), logStart_);
    // K never passes an undecided transaction (P11-2), so a rejection at or above it is this judge's own run-ahead, which no marker
    // covers: it is judged again.
    state.rejected.truncateFrom(judgeCursor_);
    // §8.5: the markers in this leader's log commit with it (a leader never truncates), so it marks only above them.  Not from
    // W: a lagging HW puts W below ranges the cleaner may already have compacted, and a re-mark from this index would disagree.
    emitFrom_    = std::max(extendOverEmptySets_(state.markers.appendedThrough()), logStart_);
    state.rejected.noteCovered(judgeCursor_);
    state.retainedPlans.pruneLeftBelow(state.rejected.watermark(), state.constraintSets);   // the members that left below W judge nothing more
    for (Member& m : members_) discardAhead_(m);
    activeSet_ = *state.constraintSets.setAt(judgeCursor_);
    setFrom_   = judgeCursor_;
    sawEmptyChange_ = false;
    judgedRecord_   = judgeCursor_ > std::max(emitFrom_, state.markers.committedMarkerEnd());   // the committed marker itself is no record to cover
    reindex_();
    // A dropped view whose judge was not driven past its drop (demoted meanwhile) is no member of the sets from here on: retire it.
    for (Member& m : members_) {
        if (!m.leaving || m.retiring || m.candidate) continue;
        const auto names = [&](const ConstraintSet& set) {
            return std::any_of(set.members.begin(), set.members.begin() + set.count, [&](const ConstraintSetMember& w) { return w.viewObjectId == m.objectId && w.planFingerprint == m.fingerprint; });
        };
        bool named = names(activeSet_);
        for (int64_t a = state.constraintSets.nextChangeAtOrAfter(judgeCursor_); !named && a != INT64_MAX; a = state.constraintSets.nextChangeAtOrAfter(a + 1))
            named = names(*state.constraintSets.setAt(a + 1));
        if (named) continue;
        m.retiring = true;
        m.leaveOffset = judgeCursor_ - 1;
    }
    recomputeCap_();
}

void VerdictSource::stall_(StallReason reason) noexcept {
    if (stalled_) return;
    ++stats_.verdictStalls;
    stalled_     = true;
    releaseOneStep();   // a stalled judge is not driven: the other sources must not wait for it
    stallReason_ = reason;
    spdlog::error("verdict judge stalled: topic {} partition {}: {}", srcTopicId_, partition_,
                  reason == StallReason::Full ? "rejected index full" : reason == StallReason::OutOfOrder ? "rejection out of order" : "view capacity exhausted");
}

void VerdictSource::waitForInstall_(const ConstraintSet& set) noexcept {
    // The first member of the set no installed view matches: the owner may install it from the log (P9-8).
    waitingFor_ = ConstraintSetMember{};
    for (uint8_t i = 0; i < set.count; ++i) {
        const ConstraintSetMember& want = set.members[i];
        const bool have = std::any_of(members_.begin(), members_.end(), [&](const Member& m) {
            return !m.retiring && m.objectId == want.viewObjectId && m.fingerprint == want.planFingerprint;
        });
        if (!have) { waitingFor_ = want; break; }
    }
    releaseOneStep();   // not driven until the view is installed again
    if (waitingForInstall_) return;
    waitingForInstall_ = true;
    ++stats_.judgeWaitsForInstall;
    // #791 diagnostics (cold, once per wait): where the set starts.
    spdlog::warn("verdict judge waits for a view install: topic {} partition {}: missing member (object {}, fingerprint {}) of set {} "
                 "(change at offset {}), judged through {}, {} members installed, {} of {} active",
                 srcTopicId_, partition_, waitingFor_.viewObjectId, waitingFor_.planFingerprint, set.id, setFrom_, judgeCursor_, members_.size(),
                 active_.size(), set.count);
}

rune::view::PrepareStatus VerdictSource::prepare_(Member& m, const rune::view::SourceRecord& rec, rune::view::TxnOverlay* chain,
                                                  const rune::view::TxnLockSet* locks) noexcept {
    if (rec.offset == failCapacityAt_) [[unlikely]] {
        failCapacityAt_ = -1;
        return rune::view::PrepareStatus::CapacityExhausted;
    }
    ViewInstance& i = *m.inst;
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
        const KeyIndexFold f{i.table, m.keyWidth, m.slot, locks};
        return prepareKeyIndex(f, rec, m.prospect, chain);
    }
    if (i.agg) {
        rune::view::AggregateFold f = aggregateFoldOf(i);
        f.member = m.slot;
        f.locks = locks;
        return rune::view::prepareAggregate(f, rec, m.prospect, chain);
    }
    rune::view::ProjectionFold f = projectionFoldOf(i);
    f.member = m.slot;
    f.locks = locks;
    return rune::view::prepareProjection(f, rec, m.prospect, chain);
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
        return m.prospect.op == rune::view::ProspectOp::Upsert && !m.prospect.rows[0].existed &&
                       static_cast<int64_t>(m.inst->table.size()) + m.chainLive >= static_cast<int64_t>(m.rule.cap) ? reject : Verdict{};
    }
    return {};
}

Verdict VerdictSource::judge(const rune::view::SourceRecord& rec) noexcept {
    ++judgeCalls_;
    for (const std::size_t i : active_) {
        const rune::view::PrepareStatus s = prepare_(members_[i], rec, nullptr, lockSet_());
        if (s == rune::view::PrepareStatus::CapacityExhausted) {
            stall_(StallReason::Capacity);
            return {};
        }
        if (s == rune::view::PrepareStatus::Locked) {   // P11-4: the judge pauses here, records nothing
            txnStop_ = TxnStop::Locked;
            notePause_(rec.offset);
            return {};
        }
    }
    for (const std::size_t i : active_) {
        const Verdict v = verdictOf_(members_[i], rec);
        if (v.accepted()) continue;
        return recordRejection_(rec.offset, v) ? v : Verdict{};
    }
    for (const std::size_t i : active_) apply_(members_[i], rec);
    return {};
}

// False: the index refused it and the judge stalled.
bool VerdictSource::recordRejection_(int64_t offset, const Verdict& v) noexcept {
    if (!state_->rejected.add(offset, v.code, v.constraintId)) {
        stall_(state_->rejected.size() >= state_->rejected.capacity() ? StallReason::Full : StallReason::OutOfOrder);
        return false;
    }
    countRejection_(v);
    return true;
}

void VerdictSource::countRejection_(const Verdict& v) noexcept {
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
}

// A pause on a held row (P11-4) counts once per record; each pass it lasts adds to foldPausedOnTxnLockNs.
void VerdictSource::notePause_(int64_t offset) noexcept {
    TxnJudgeStats& st = state_->txn.stats;
    if (lockPausedAt_ != offset) {
        lockPausedAt_ = offset;
        ++st.lockPauses;
    } else if (nowNs_ > lockPausedNs_) {
        st.foldPausedOnTxnLockNs += static_cast<uint64_t>(nowNs_ - lockPausedNs_);
    }
    lockPausedNs_ = nowNs_;
}

// Design 3.1: the coverage never lies inside (first, M] of an applied span, or a restart there would meet M with none of T's records
// ahead of it.  A hold inside the span counts from its first record.
void VerdictSource::endOrdinary_(const OrdinaryTxn& o) noexcept {
    closeSpan_(o.producerId);
    for (OrdinaryTxn& x : ordinary_)
        if (const int64_t at = std::min(x.first, x.holdFrom); o.first < at && at <= o.marker) x.holdFrom = o.first;
    state_->txn.holdBelowSpan(o.first, o.marker);
    ordinary_.erase(ordinary_.begin() + static_cast<std::ptrdiff_t>(markerAt_));
    ordinaryApplied_ = -1;
    ordinaryWalk_ = {};
    releaseOneStep();
    txnStop_ = TxnStop::None;
    judgeCursor_ = o.marker + 1;
    syncMemberCursors_();
}

// A member joined, left or the active set switched while T was judged (its overlay keys name member slots): T is judged anew.
void VerdictSource::restartJudgement_() noexcept {
    jx_.members = membersEpoch_;
    if (jx_.overlay < 0) return;
    txnPool_->at(jx_.overlay).clear();
    txnPool_->keysAt(jx_.overlay).clear();
    jx_.from = jx_.first;
    jx_.records = 0;
    jx_.scanning = jx_.done = jx_.endedCommit = false;
    jx_.endedAt = -1;
    jx_.answer = {};
    jx_.resolvePid = jx_.resolveFrom = -1;
    resetChainLive_();
}

void VerdictSource::noteMarkerWithoutVote_(const OrdinaryTxn& o, bool applied) noexcept {
    ++state_->txn.stats.markerWithoutVote;
    spdlog::error("verdict judge: topic {} partition {}: producer {} committed at {} without a vote; judged at the marker: {}", srcTopicId_, partition_,
                  o.producerId, o.marker, applied ? "applied" : "rejected");
}

void VerdictSource::countVote_(const TxnVoteAnswer& a) noexcept {
    using rune::protocol::client::ErrorCode;
    TxnJudgeStats& st = state_->txn.stats;
    if (a.code == 0) {
        ++st.votesYes;
        return;
    }
    ++st.votesNo;
    switch (static_cast<ErrorCode>(a.code)) {
    case ErrorCode::ConstraintViolation: ++st.votesNoConstraint; break;
    case ErrorCode::KeyExists:           ++st.votesNoKeyExists; break;
    case ErrorCode::KeyNotFound:         ++st.votesNoKeyNotFound; break;
    case ErrorCode::TxnTooLarge:         ++st.votesNoTooLarge; break;
    case ErrorCode::TxnConflict:         ++st.votesNoConflict; break;
    default: break;
    }
}

// Prepares every active member for `rec` through `chain` and evaluates them in set order.  Failed: `failed` is the first rejection.
VerdictSource::ChainStep VerdictSource::chainRecord_(const rune::view::SourceRecord& rec, rune::view::TxnOverlay& chain, Verdict& failed) noexcept {
    lastIntent_ = rec.intent;
    for (const std::size_t i : active_) {
        const rune::view::PrepareStatus s = prepare_(members_[i], rec, &chain, lockSet_());
        if (s == rune::view::PrepareStatus::Locked) return ChainStep::Locked;
        if (s == rune::view::PrepareStatus::CapacityExhausted) {
            stall_(StallReason::Capacity);
            return ChainStep::Stalled;
        }
    }
    for (const std::size_t i : active_) {
        failed = verdictOf_(members_[i], rec);
        if (!failed.accepted()) return ChainStep::Failed;
    }
    for (const std::size_t i : active_) {
        Member& m = members_[i];
        const rune::view::Prospect& p = m.prospect;
        m.chainLive += (p.op == rune::view::ProspectOp::Upsert && !p.rows[0].existed) - (p.op == rune::view::ProspectOp::Erase && p.rows[0].existed);
    }
    return ChainStep::Ok;
}

// A record through every active member's normal path, no lock checked (its transaction was judged with them).  False: stalled.
bool VerdictSource::applyRecord_(const rune::view::SourceRecord& rec) noexcept {
    lastIntent_ = rec.intent;
    for (const std::size_t i : active_) {
        if (prepare_(members_[i], rec, nullptr, nullptr) == rune::view::PrepareStatus::CapacityExhausted) {
            stall_(StallReason::Capacity);
            return false;
        }
    }
    for (const std::size_t i : active_) apply_(members_[i], rec);
    judgedRecord_ = true;
    return true;
}

// P11-6: an atomic batch judged as a unit through the batch overlay.  All pass: applied in this step; one fails: every record is
// rejected with its verdict.  False: the judge stays at the batch (a pause, a stall, a record that does not fit).
bool VerdictSource::judgeAtomic_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, bool keyZero) noexcept {
    rune::view::TxnOverlay& chain = batchOverlay_.front();
    Verdict failed{};
    ChainStep step = ChainStep::Ok;
    bool walked = true;
    // More records than the overlay is sized for (the knob): rejected whole, the same on every leader, never a capacity stall.
    // TODO(Part 11 Task 12): rule 10's answer for it.
    const bool tooLarge = r.recordCount() > txnJudgedRecordsMax_;
    if (tooLarge) {
        failed = {static_cast<uint16_t>(rune::protocol::client::ErrorCode::TxnTooLarge), 0};
        step = ChainStep::Failed;
    } else {
        chain.clear();
        resetChainLive_();
        walked = rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& span) {
            if (step == ChainStep::Ok) step = chainRecord_(recordOf_(span), chain, failed);
        });
        chain.clear();
        resetChainLive_();
    }
    if (!walked || step == ChainStep::Stalled) return false;
    if (step == ChainStep::Locked) {
        txnStop_ = TxnStop::Locked;
        notePause_(r.baseOffset());
        return false;
    }
    if (step == ChainStep::Failed) {
        if (state_->rejected.size() + static_cast<std::size_t>(r.lastOffsetDelta()) + 1 > state_->rejected.capacity()) {
            stall_(StallReason::Full);
            return false;
        }
        bool ok = true;
        (void)rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& span) {
            if (!ok) return;
            ok = recordRejection_(span.offset, failed);
            if (ok && keyZero) [[unlikely]] noteKeyZeroRejection_(failed, recordOf_(span));
        });
        judgedRecord_ = true;
        state_->txn.stats.atomicTooLarge += ok && tooLarge;
        return ok;
    }
    // The chained prepare was every member's capacity check: each record was prepared against the state its apply sees, through a
    // row overlay that holds at least what the plain prepare's does, and the tables grow.  So the batch applies whole here or not at
    // all, and nothing of it outlives a stall, a re-activation or a rescan.
    bool ok = true;
    (void)rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& span) {
        if (ok) ok = applyRecord_(recordOf_(span));
    });
    return ok;
}

// Starts judging T at `end`: an overlay of the pool, its key set cleared.  False: none is free, the judge waits there (counted once).
bool VerdictSource::beginJudgement_(int64_t producerId, int64_t first, int64_t end, bool atMarker) noexcept {
    const int32_t o = txnPool_->acquire(sourceKey_);
    if (o < 0) {
        if (overlayWaitAt_ != end) {
            overlayWaitAt_ = end;
            ++state_->txn.stats.txnOverlayWaits;
        }
        return false;
    }
    overlayWaitAt_ = -1;
    jx_ = TxnJudgement{producerId, first, end, first, o, 0, atMarker};
    jx_.members = membersEpoch_;
    jx_.cap = static_cast<int32_t>(std::min<std::size_t>(static_cast<std::size_t>(std::max(txnJudgedRecordsMax_, 0)), txnPool_->keysAt(o).keyCapacity()));
    txnPool_->keysAt(o).clear();
    resetChainLive_();
    refreshLocks_();
    return true;
}

void VerdictSource::endJudgement_() noexcept {
    jx_ = {};
    resetChainLive_();
    refreshLocks_();
}

// Another producer's marker or TxnJudge record, noted for P11-3 and P11-3a.
void VerdictSource::noteControl_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, int64_t self, TxnKeySet& keys) noexcept {
    namespace cm = rune::storage::control_marker;
    if ((r.attributes() & rune::protocol::batch::kAttrTransactional) != 0) {
        if (r.producerId() == self) return;
        if (const int type = cm::controlType(batch, len); type != cm::kUnparseable) keys.noteMarker(r.producerId(), r.baseOffset(), type == cm::kCommit);
        return;
    }
    if (len != cr::kTxnJudgeBatchBytes) return;
    if (const auto j = cr::parseTxnJudge(batch, len); j && j->producerId != self) keys.noteJudge(j->producerId, j->producerEpoch, j->firstOffset, r.baseOffset());
}

// The chained read: T's records in order, every other producer's marker and TxnJudge noted for the scan.  False: answered (the
// first failing record), or a pause, which restarts T from its first record with a cleared overlay (F3), or a stall.
bool VerdictSource::readTxnBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, const rune::view::AbortedTxnFilter& aborted) noexcept {
    namespace proto = rune::protocol;
    namespace cm = rune::storage::control_marker;
    constexpr int16_t kTxnControl = proto::batch::kAttrTransactional | proto::batch::kAttrControl;
    const int16_t attrs = r.attributes();
    TxnKeySet& keys = txnPool_->keysAt(jx_.overlay);
    if ((attrs & proto::batch::kAttrControl) != 0) {
        // The producer's own COMMIT / ABORT inside (first, end): T ended before its TxnJudge, which judges nothing (abort before J).
        if (const int type = (attrs & kTxnControl) == kTxnControl && r.producerId() == jx_.producerId && r.baseOffset() > jx_.first ? cm::controlType(batch, len) : cm::kUnparseable;
            type != cm::kUnparseable) {
            jx_.endedAt = r.baseOffset();
            jx_.endedCommit = type == cm::kCommit;
            jx_.done = true;
            return false;
        }
        noteControl_(r, batch, len, jx_.producerId, keys);
        return true;
    }
    if ((attrs & kTxnControl) != proto::batch::kAttrTransactional || r.producerId() != jx_.producerId || aborted.skips(r, attrs)) return true;
    rune::view::TxnOverlay& chain = txnPool_->at(jx_.overlay);
    ChainStep step = ChainStep::Ok;
    (void)proto::forEachRecordSpan(batch, len, [&](const proto::RecordSpan& span) {
        if (step != ChainStep::Ok) return;
        if (jx_.records >= jx_.cap || !keys.note(span.key, span.offset)) {
            jx_.answer = {static_cast<uint16_t>(rune::protocol::client::ErrorCode::TxnTooLarge), 0, span.offset};
            step = ChainStep::Failed;
            return;
        }
        ++jx_.records;
        Verdict failed{};
        step = chainRecord_(recordOf_(span), chain, failed);
        if (step == ChainStep::Failed) {
            jx_.answer = {failed.code, failed.constraintId, span.offset};
        } else if (step == ChainStep::Locked) {
            notePause_(span.offset);
            chain.clear();
            keys.clear();
            resetChainLive_();
            jx_.from = jx_.first;
            jx_.records = 0;
        }
    });
    if (step == ChainStep::Failed) jx_.done = true;
    return step == ChainStep::Ok;
}

// P11-3: an accepted record of another writer on a key of T, above T's last record on it, answers no (TxnConflict).  A record of
// another transaction is accepted when it is applied below the end (TxnKeySet::appliedAt, from the log alone, so every leader and
// every pass timing answers the same).  A deferred one whose marker lies past the end stops the scan until resolveMarkers_ finds it.
// A marker list that overflowed takes it as applied: a spurious no, never a wrong yes.
bool VerdictSource::scanTxnBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, const rune::view::AbortedTxnFilter& aborted) noexcept {
    namespace proto = rune::protocol;
    const int16_t attrs = r.attributes();
    if ((attrs & proto::batch::kAttrControl) != 0 || aborted.skips(r, attrs)) return true;
    TxnKeySet& keys = txnPool_->keysAt(jx_.overlay);
    TxnKeySet::Applied applied = TxnKeySet::Applied::Yes;
    if ((attrs & proto::batch::kAttrTransactional) != 0) {
        if (r.producerId() == jx_.producerId) return true;
        if (!keys.markersOverflowed()) applied = keys.appliedAt(r.producerId(), r.producerEpoch(), r.baseOffset());
        if (applied == TxnKeySet::Applied::No) return true;
    }
    int64_t hit = -1;
    (void)proto::forEachRecordSpan(batch, len, [&](const proto::RecordSpan& span) {
        if (hit >= 0) return;
        const TxnKeySet::Key* k = keys.find(span.key);
        if (k != nullptr && span.offset > k->last && !state_->rejected.isRejected(span.offset)) hit = span.offset;
    });
    if (hit < 0) return true;
    if (applied == TxnKeySet::Applied::Unknown) {
        jx_.resolvePid = r.producerId();
        return false;
    }
    jx_.answer = {static_cast<uint16_t>(rune::protocol::client::ErrorCode::TxnConflict), 0, hit};
    jx_.done = true;
    return false;
}

void VerdictSource::noteKeyZeroRejection_(const Verdict& v, const rune::view::SourceRecord& rec) noexcept {
    if (v.code != static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyExists)) {
        ++state_->autoIncStats.idsBurned;
        return;
    }
    ++state_->autoIncStats.assignedKeyExists;   // an assigned id the key index already holds: the counter reused it
    spdlog::critical("AUTOINC-ID-REUSE topic={} partition={} id={}", srcTopicId_, partition_, keyIndexWord(rec.key));
}

int VerdictSource::replay_(Member& m, const rune::storage::LogReadView& view, int64_t upTo, const rune::view::AbortedTxnFilter& aborted,
                           rune::view::SpanReader reader) {
    ViewInstance& inst = *m.inst;
    const rune::view::FoldSkip skip{.aborted = aborted, .rejected = &state_->rejected};
    int64_t cursor = inst.foldCursor;
    const int64_t bound = inst.maxRecordsPerPass > 0 ? std::min(upTo, cursor + inst.maxRecordsPerPass) : upTo;
    struct JudgeCtx { VerdictSource* self; Member* m; } jc{this, &m};
    rune::view::TxnFold txnFold{inst.spanFence, txnPool_->supersedeKeys, reader, inst.maxRecordsPerPass,
                                {&jc, [](void* c, const uint8_t* b, std::size_t len, int64_t at) {
                                     auto* j = static_cast<JudgeCtx*>(c);
                                     return j->self->judgeStep_(*j->m, b, len, at);
                                 }}};
    rune::view::TxnFold* const txn = &txnFold;
    if (m.kind == rune::view::VerdictViewKind::KeyIndex) {
        KeyIndexFold f{inst.table, m.keyWidth};
        const rune::view::FoldStats st = foldKeyIndexBatch(f, view, bound, cursor, skip, txn);
        inst.foldCursor = cursor;
        inst.counters.skippedAborted += st.skippedAborted;
        discardKeyIndexDirty_(inst);
        return static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted + st.skippedMalformed + st.skippedRejected);
    }
    rune::view::FoldStats st;
    if (inst.keyed && inst.agg) {
        rune::view::AggregateFold f = aggregateFoldOf(inst);
        st = rune::view::foldKeyedAggregateBatch(f, inst.prospect, view, bound, cursor, skip, txn);
    } else if (inst.keyed) {
        rune::view::ProjectionFold f = projectionFoldOf(inst);
        st = rune::view::foldProjectionBatch(f, inst.prospect, view, bound, cursor, skip, txn);
    } else {
        st = inst.agg ? rune::view::foldAggregateBatch(inst.plan, inst.layout, inst.agg->layout, inst.vdc, inst.table, inst.agg->contributors,
                                                       inst.agg->groups, inst.agg->blocksOrNull(), view, bound, cursor, skip, txn)
                      : rune::view::foldBatch(inst.plan, inst.layout, inst.vdc, inst.table, view, bound, cursor, skip, txn);
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
    inst.counters.txnSuperseded    += st.txnSuperseded;
    if (inst.claims) inst.counters.uniqueConflicts = inst.claims->extras();
    return static_cast<int>(st.applied + st.deleted + st.skippedControl + st.skippedAborted + st.skippedMalformed + st.skippedNoGroup + st.skippedNoKey + st.skippedRejected);
}

// D2/D3: a TxnJudge in a member's replay.  Decided (its entry Finalized, or replaced by the producer's next transaction): a
// committed transaction applies here, an aborted one is dropped (a replaced entry's aborted batches are skipped by the filter).  Undecided: an active member drops the span, and finalizeLocked_ applies it
// to every active member; any other member (a candidate, a joiner) waits here until it is decided.
rune::view::JudgeDecision VerdictSource::judgeStep_(Member& m, const uint8_t* batch, std::size_t len, int64_t at) noexcept {
    if (len != cr::kTxnJudgeBatchBytes) return {};
    const auto j = cr::parseTxnJudge(batch, len);
    if (!j) return {};
    const DeferredTxn* t = state_->txn.find(j->producerId);
    if (t != nullptr && t->judgeOffset == at) {
        if (t->phase != TxnPhase::Finalized) {
            const bool active = std::find(active_.begin(), active_.end(), static_cast<std::size_t>(&m - members_.data())) != active_.end();
            return {active ? rune::view::JudgeStep::Drop : rune::view::JudgeStep::Wait, j->producerId};
        }
        if (!t->commit) return {rune::view::JudgeStep::Drop, j->producerId};
    }
    const bool backfill = j->firstOffset < m.replayFrom;
    if (backfill) ++state_->txn.stats.checkpointBackfills;
    return {rune::view::JudgeStep::Apply, j->producerId, backfill ? j->firstOffset : -1};
}

// The judge applied (or dropped) a transaction to every active member: a span a member's replay opened for it is closed.
void VerdictSource::closeSpan_(int64_t producerId) noexcept {
    for (const std::size_t i : active_) members_[i].inst->spanFence.close(producerId);
}

int VerdictSource::judgeBatches_(const rune::storage::LogReadView& view, int64_t limit, int64_t hardLimit, const rune::view::AbortedTxnFilter& aborted) {
    namespace proto = rune::protocol;
    int work = 0;
    txnStop_ = TxnStop::None;
    refreshLocks_();
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
        const bool control = (attrs & proto::batch::kAttrControl) != 0;
        const bool skipBatch = control || aborted.skips(r, attrs);
        if (skipBatch) {
            if (last >= hardLimit) break;
            // P11-4: a TxnJudge whose transaction is not judged yet stops the judge.
            if (control && (attrs & proto::batch::kAttrTransactional) == 0 && static_cast<std::size_t>(r.totalLength()) == cr::kTxnJudgeBatchBytes) {
                const auto j = cr::parseTxnJudge(view.data + pos, cr::kTxnJudgeBatchBytes);
                const DeferredTxn* t = j ? state_->txn.find(j->producerId) : nullptr;
                if (t != nullptr && t->judgeOffset == r.baseOffset() && (t->phase == TxnPhase::AwaitJudge || t->phase == TxnPhase::Judging)) {
                    judgeCursor_ = r.baseOffset();
                    judgePid_ = t->producerId;
                    txnStop_ = TxnStop::Judge;
                    break;
                }
            }
            if (control && (attrs & proto::batch::kAttrTransactional) != 0 && !ordinary_.empty()) {   // a COMMIT / ABORT
                const auto o = std::find_if(ordinary_.begin(), ordinary_.end(), [&](const OrdinaryTxn& t) { return t.producerId == r.producerId() && t.marker == r.baseOffset(); });
                if (o != ordinary_.end()) {
                    judgeCursor_ = r.baseOffset();
                    markerAt_ = static_cast<std::size_t>(o - ordinary_.begin());
                    txnStop_ = TxnStop::Marker;
                    break;
                }
            }
            if (control && (attrs & proto::batch::kAttrTransactional) != 0 && endsBeforeJudge_(r, view.data + pos, static_cast<std::size_t>(r.totalLength()))) {
                judgeCursor_ = r.baseOffset();
                markerAt_ = ordinary_.size() - 1;
                txnStop_ = TxnStop::Marker;
                break;
            }
            if (keyZero && (attrs & proto::batch::kAttrControl) == 0) [[unlikely]] state_->autoIncStats.idsBurned += static_cast<uint64_t>(r.lastOffsetDelta()) + 1;   // aborted
            judgeCursor_ = last + 1;
            pos += static_cast<std::size_t>(r.totalLength());
            ++work;
            continue;
        }
        if ((attrs & proto::batch::kAttrTransactional) != 0) {   // P11-2: deferred or ordinary, skipped here; else the judge waits
            if (last >= hardLimit) break;
            if (!txnBatchDeferred_(r.producerId(), r.baseOffset(), r.lastOffsetDelta() + 1)) {
                judgeCursor_ = std::max(judgeCursor_, r.baseOffset());
                if (probe_.producerId != r.producerId() || probe_.first != r.baseOffset()) probe_ = TxnProbe{r.producerId(), r.baseOffset(), r.baseOffset()};
                txnStop_ = TxnStop::Probe;
                break;
            }
            judgeCursor_ = last + 1;
            pos += static_cast<std::size_t>(r.totalLength());
            ++work;
            continue;
        }
        // P11-6: an atomic batch is judged whole (one past txnJudgedRecordsMax is rejected whole); without a batch overlay, per record.
        if ((attrs & proto::batch::kAttrAtomic) != 0 && !batchOverlay_.empty() && judgeCursor_ <= r.baseOffset()) {
            if (last >= hardLimit) break;
            judgeCursor_ = r.baseOffset();
            if (!judgeAtomic_(r, view.data + pos, static_cast<std::size_t>(r.totalLength()), keyZero)) break;
            judgeCursor_ = last + 1;
            judgedRecord_ = true;
            pos += static_cast<std::size_t>(r.totalLength());
            ++work;
            continue;
        }
        judgeCursor_ = std::max(judgeCursor_, r.baseOffset());
        bool halt = false;
        const bool walked = rune::protocol::forEachRecordSpan(view.data + pos, view.len - pos, [&](const rune::protocol::RecordSpan& span) {
            if (halt || stalled_ || txnStop_ == TxnStop::Locked) return;
            if (span.offset >= limit) { halt = true; return; }
            if (span.offset < judgeCursor_) return;
            const rune::view::SourceRecord rec = recordOf_(span);
            lastIntent_ = rec.intent;
            const Verdict v = judge(rec);
            if (stalled_ || txnStop_ == TxnStop::Locked) return;
            if (keyZero && !v.accepted()) [[unlikely]] noteKeyZeroRejection_(v, rec);
            judgeCursor_ = span.offset + 1;
            judgedRecord_ = true;
            ++work;
        });
        if (stalled_ || txnStop_ == TxnStop::Locked) break;
        if (!walked && !halt) break;   // a record that does not fit its batch: stop, never advance past it
        judgeCursor_ = halt ? limit : std::max(judgeCursor_, last + 1);
        pos += static_cast<std::size_t>(r.totalLength());
    }
    syncMemberCursors_();
    return work;
}

void VerdictSource::syncMemberCursors_() noexcept {
    for (const std::size_t i : active_) {
        ViewInstance& inst = *members_[i].inst;
        inst.foldCursor = judgeCursor_;
        if (members_[i].kind == rune::view::VerdictViewKind::KeyIndex) discardKeyIndexDirty_(inst);
        if (inst.claims) inst.counters.uniqueConflicts = inst.claims->extras();
    }
}

rune::view::SourceRecord VerdictSource::recordOf_(const rune::protocol::RecordSpan& span) noexcept {
    ViewInstance& first = *members_[active_.front()].inst;
    rune::view::SourceRecord rec{span.key, span.value, span.offset, rune::protocol::record::intentOf(span.attributes), nullptr};
    if (!span.value.empty()) rec.writer = &first.vdc.materializedFor(writerVersionOf(span.value, first.vdc.defaultVersion()));
    return rec;
}

// Abort before J: the producer's COMMIT / ABORT lies between its entry's first record and its TxnJudge, so T ended before the J (a
// TxnJudge appended late, or by another leader).  The entry is decided from the log at once: Finalized, its J judges nothing.  A
// commit's batches were passed as deferred: they are applied at this marker as an ordinary transaction's (true: the caller stops
// there, the entry last in ordinary_).  An abort only ends the entry.
bool VerdictSource::endsBeforeJudge_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len) noexcept {
    namespace cm = rune::storage::control_marker;
    DeferredTxn* t = state_->txn.find(r.producerId());
    if (t == nullptr || t->markerOffset >= 0 || t->phase == TxnPhase::Finalized || r.baseOffset() <= t->firstOffset || r.baseOffset() >= t->judgeOffset) return false;
    const int type = cm::controlType(batch, len);
    if (type == cm::kUnparseable || (type == cm::kCommit && ordinary_.size() == ordinary_.capacity())) return false;
    if (t->overlay >= 0) txnPool_->release(t->overlay);
    t->overlay = -1;
    t->markerOffset = r.baseOffset();
    t->commit = type == cm::kCommit;
    t->phase = TxnPhase::Finalized;
    if (!t->commit) return false;
    ordinary_.push_back(OrdinaryTxn{t->producerId, t->firstOffset, r.baseOffset(), true});
    return true;
}

// A batch of a deferred transaction (its committed TxnJudge known) or of an ordinary one (its marker found): the judge passes it.
bool VerdictSource::txnBatchDeferred_(int64_t producerId, int64_t base, int32_t records) noexcept {
    if (DeferredTxn* t = state_->txn.find(producerId);
        t != nullptr && t->firstOffset <= base && (t->markerOffset < 0 || base < t->markerOffset) && t->judgeOffset < passHw_) {
        t->records += records;
        return true;
    }
    return std::any_of(ordinary_.begin(), ordinary_.end(), [&](const OrdinaryTxn& o) { return o.producerId == producerId && o.first <= base && base < o.marker; });
}

// One batch above probe_.first.  A TxnJudge the feed could not record (a full table, or the producer's entry holds an overlay) is
// offered again here; a refusal waits, counted once.  Accepted, T is deferred and its marker is owed (feedOwedMarkers_).
VerdictSource::ProbeStep VerdictSource::probeBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len) noexcept {
    namespace proto = rune::protocol;
    namespace cm = rune::storage::control_marker;
    const int16_t attrs = r.attributes();
    const int64_t base = r.baseOffset();
    if ((attrs & proto::batch::kAttrControl) == 0 || base <= probe_.first) return ProbeStep::Next;
    if ((attrs & proto::batch::kAttrTransactional) != 0) {
        const int type = r.producerId() == probe_.producerId ? cm::controlType(batch, len) : cm::kUnparseable;
        if (type == cm::kUnparseable) return ProbeStep::Next;
        if (ordinary_.size() == ordinary_.capacity()) {
            if (probe_.waitAt != base) { probe_.waitAt = base; ++state_->txn.stats.ordinaryWaits; }
            return ProbeStep::Wait;
        }
        ordinary_.push_back(OrdinaryTxn{probe_.producerId, probe_.first, base, type == cm::kCommit});
        return ProbeStep::Classified;
    }
    const auto c = cr::parseControl(batch, len);
    if (!c || c->type != cr::kTxnJudge) return ProbeStep::Next;
    const auto j = cr::parseTxnJudge(*c);
    if (!j || j->producerId != probe_.producerId || j->firstOffset > probe_.first) return ProbeStep::Next;
    // The producer's newer entry replaced this transaction's: it is applied at its marker like an ordinary one.
    if (const DeferredTxn* e = state_->txn.find(j->producerId); e != nullptr && e->judgeOffset >= base) return ProbeStep::Next;
    if (state_->txn.onJudgeRecord(base, *j) == nullptr) {
        if (probe_.waitAt != base) { probe_.waitAt = base; ++state_->txn.stats.judgeRecordRefusals; }
        return ProbeStep::Wait;
    }
    // The feed dropped the producer's marker if it came before this: no entry took it.  One that comes after is fed as usual.
    std::erase_if(owedMarkers_, [&](const OwedMarker& m) { return m.producerId == j->producerId; });
    if (owedMarkers_.size() == owedMarkers_.capacity()) pruneOwedMarkers_();   // then at most one per unpaid entry: it fits
    owedMarkers_.push_back(OwedMarker{j->producerId, base, base + 1});
    return ProbeStep::Classified;
}

// An owed marker's batch: the producer's first COMMIT / ABORT above its TxnJudge is that transaction's.  True: fed.
bool VerdictSource::owedMarkerBatch_(const OwedMarker& m, const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len) noexcept {
    namespace proto = rune::protocol;
    namespace cm = rune::storage::control_marker;
    constexpr int16_t kTxnControl = proto::batch::kAttrTransactional | proto::batch::kAttrControl;
    if ((r.attributes() & kTxnControl) != kTxnControl || r.producerId() != m.producerId || r.baseOffset() <= m.judgeOffset) return false;
    const int type = cm::controlType(batch, len);
    if (type == cm::kUnparseable) return false;
    state_->txn.onMarker(m.producerId, r.baseOffset(), type == cm::kCommit);
    return true;
}

// Drops the owed markers that are paid: the entry has its marker (found here or fed since), or it is gone or replaced.
void VerdictSource::pruneOwedMarkers_() noexcept {
    std::erase_if(owedMarkers_, [&](const OwedMarker& m) {
        const DeferredTxn* t = state_->txn.find(m.producerId);
        return t == nullptr || t->judgeOffset != m.judgeOffset || t->markerOffset >= 0;
    });
}

// The collect walk of ordinaryWalk_'s segment: other producers' markers, and the keys of T's records from `from`, until a new key
// does not fit (`end`); past it, a key already noted only takes the later offset as its last.
void VerdictSource::collectOrdinaryBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, int64_t producerId, TxnKeySet& keys) noexcept {
    namespace proto = rune::protocol;
    const int16_t attrs = r.attributes();
    if ((attrs & proto::batch::kAttrControl) != 0) {
        noteControl_(r, batch, len, producerId, keys);
        return;
    }
    if ((attrs & proto::batch::kAttrTransactional) == 0 || r.producerId() != producerId) return;
    OrdinaryWalk& w = ordinaryWalk_;
    (void)proto::forEachRecordSpan(batch, len, [&](const proto::RecordSpan& span) {
        if (span.offset < w.from) return;
        if (w.end < 0) {
            if (!keys.note(span.key, span.offset)) w.end = span.offset;
        } else if (TxnKeySet::Key* k = keys.find(span.key)) {
            k->last = span.offset;
        }
    });
}

// P11-3a, the same test as P11-3 with the marker as the end: an accepted record of another writer above T's last record on its key
// supersedes the key, a plain record or one of a transaction applied below the marker (TxnKeySet::appliedAt, or the lookup).  False:
// such a record's transaction is not decided by the set's markers; ordinaryWalk_ asks for a fresh list from the batch, or, when the
// list starts there or holds every marker, the lookup of its decision.
bool VerdictSource::checkOrdinaryBatch_(const rune::protocol::RecordBatchReader& r, const uint8_t* batch, std::size_t len, int64_t producerId,
                                        const rune::view::AbortedTxnFilter& aborted, TxnKeySet& keys) noexcept {
    namespace proto = rune::protocol;
    const int16_t attrs = r.attributes();
    if ((attrs & proto::batch::kAttrControl) != 0 || aborted.skips(r, attrs)) return true;
    const bool txn = (attrs & proto::batch::kAttrTransactional) != 0;
    if (txn && r.producerId() == producerId) return true;
    const auto hits = [&](auto&& fn) {
        (void)proto::forEachRecordSpan(batch, len, [&](const proto::RecordSpan& span) {
            TxnKeySet::Key* k = keys.find(span.key);
            if (k != nullptr && span.offset > k->last && !state_->rejected.isRejected(span.offset)) fn(*k);
        });
    };
    if (txn) {
        bool hit = false;
        hits([&](TxnKeySet::Key&) { hit = true; });
        if (!hit) return true;
        OrdinaryWalk& w = ordinaryWalk_;
        TxnLookup& l = w.lookup;
        const int64_t base = r.baseOffset();
        TxnKeySet::Applied applied;
        if (l.decided && l.producerId == r.producerId() && l.epoch == r.producerEpoch() && base >= l.base && base < l.to)
            applied = l.applied ? TxnKeySet::Applied::Yes : TxnKeySet::Applied::No;
        else
            applied = keys.appliedAt(r.producerId(), r.producerEpoch(), base);
        if (applied == TxnKeySet::Applied::No) return true;
        if (applied == TxnKeySet::Applied::Unknown) {
            if (keys.markersOverflowed() && w.markersFrom < base) w.refill = true;
            else l = {r.producerId(), base, base + 1, -1, r.producerEpoch()};
            return false;
        }
    }
    hits([](TxnKeySet::Key& k) { k.superseded = true; });
    return true;
}

// The ordinary transaction's records in one batch, in [ordinaryApplied_, end): a superseded key applies nothing (P11-3a), the rest go
// through every member's apply path.  It has no constrained member and no intent, so nothing is evaluated; a row a judged
// transaction holds pauses it (P11-4).
bool VerdictSource::applyTxnBatch_(const uint8_t* batch, std::size_t len, TxnKeySet& keys, int64_t end) noexcept {
    bool ok = true;
    (void)rune::protocol::forEachRecordSpan(batch, len, [&](const rune::protocol::RecordSpan& span) {
        if (!ok || span.offset < ordinaryApplied_ || span.offset >= end) return;
        if (const TxnKeySet::Key* k = keys.find(span.key); k == nullptr || !k->superseded) {
            const rune::view::SourceRecord rec = recordOf_(span);
            lastIntent_ = rec.intent;
            for (const std::size_t i : active_) {
                const rune::view::PrepareStatus s = prepare_(members_[i], rec, nullptr, lockSet_());
                if (s == rune::view::PrepareStatus::CapacityExhausted) stall_(StallReason::Capacity);
                if (s == rune::view::PrepareStatus::Locked) {
                    notePause_(span.offset);
                    txnPool_->parkOneStep(oneStepClaim_, true);   // the lock clears at the holder's marker, which may wait for a vote the waiter judges
                }
                if (s == rune::view::PrepareStatus::CapacityExhausted || s == rune::view::PrepareStatus::Locked) {
                    ok = false;
                    return;
                }
            }
            for (const std::size_t i : active_) apply_(members_[i], rec);
            judgedRecord_ = true;
        }
        ordinaryApplied_ = span.offset + 1;
    });
    return ok;
}

// P11-2: the markers cover [emitFrom, judgedThrough()); a deferred transaction holds them until it is decided.
int VerdictSource::emitMarkers_(PartitionWriteState& state, int64_t nowNs) {
    const int64_t through = judgedThrough();
    if (generation_ == kUnbound || through <= emitFrom_ || !(judgedRecord_ || sawEmptyChange_)) return 0;
    const bool due = rejectedThisPass_ > 0 || sawEmptyChange_ || nowNs - lastMarkerNs_ >= intervalNs_ ||
                     (checkpointCap_ != INT64_MAX && through - emitFrom_ >= checkpointCap_);
    if (!due) return 0;
    int work = 0;
    while (emitFrom_ < through && !state.markers.full()) {
        const int64_t from = emitFrom_;
        int64_t finalThrough = through;
        entryScratch_.clear();
        for (int64_t pos = from;;) {
            const int64_t off = state.rejected.nextRejectedAtOrAfter(pos);
            if (off >= through) break;
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
    if (emitFrom_ >= judgeCursor_) sawEmptyChange_ = judgedRecord_ = false;   // not judgedThrough(): the run-ahead records still need a marker
    return work;
}

int VerdictSource::pushCheckpoints_(PartitionWriteState& state) {
    if (hooks_.checkpoint == nullptr) return 0;
    int work = 0;
    for (Member& m : members_) {
        ViewInstance& inst = *m.inst;
        if (inst.persisted && !inst.pendingValid && checkpointSettled(inst)) {
            // P4-4: a persisted member's delta leaves only once a marker in the local log covers the cursor it is stamped with, or
            // while it runs ahead of deferred transactions (P11-8 (a)).
            if (state.markers.appendedThrough() < inst.foldCursor && !runsAheadAt_(inst.foldCursor)) continue;
            // P11-8 (b): nor while a Locked transaction judged below its cursor is not applied (its delta would stamp rows the apply
            // changes).
            if (state.txn.lockedBelow(inst.foldCursor)) {
                ++state.txn.stats.vetoedCheckpointPushes;
                continue;
            }
        }
        // A push the span fence vetoes (P11-0) leaves nothing durable at the cursor: the run-ahead cap still counts from the last one.
        const bool vetoed = inst.spanFence.fenced() || inst.spanFence.straddles(inst.foldCursor);
        work += hooks_.checkpoint(hooks_.ctx, inst);
        if (!inst.pendingValid && !vetoed) m.pushedAt = inst.foldCursor;
    }
    return work;
}

// P11-8 (a): K is held below `cursor` only by deferred transactions, so a checkpoint there is backfilled on replay from their J (one
// judged below it is (b)'s veto).  An ordinary transaction's records below it have no J to backfill from.
bool VerdictSource::runsAheadAt_(int64_t cursor) const noexcept {
    return judgedThrough() < cursor &&
           std::none_of(ordinary_.begin(), ordinary_.end(), [&](const OrdinaryTxn& o) { return o.first < cursor; });
}

// Where the checkpoint cap counts from while the judge runs ahead: the persisted members' last push, at least the markers' coverage.
// With no persisted member the cap is off (INT64_MAX).
int64_t VerdictSource::runAheadFrom_() const noexcept {
    int64_t from = INT64_MAX;
    for (const Member& m : members_)
        if (m.inst->persisted && m.inst->maxRecordsPerPass > 0) from = std::min(from, std::max(m.pushedAt, emitFrom_));
    return from;
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
    const auto append = [&](std::span<const uint8_t> batch) {
        if (replica_ != nullptr) return replica_->appendControlBatch(batch, false);
        const int64_t at = std::max(testNextOffset_, judgeCursor_);
        testNextOffset_ = at + 1;
        if (captureAppended_) appended_.emplace_back(batch.begin(), batch.end());
        return at;
    };
    // P9-8: a joining Unique or Check member's plan goes into the log just below the change that names it.
    const ConstraintSet& latest = state_->constraintSets.latest();
    for (uint8_t i = 0; i < logged.count; ++i) {
        const ConstraintSetMember& want = logged.members[i];
        if (std::find(latest.members.begin(), latest.members.begin() + latest.count, want) != latest.members.begin() + latest.count) continue;
        const auto m = std::find_if(members_.begin(), members_.end(), [&](const Member& x) { return x.objectId == want.viewObjectId && x.fingerprint == want.planFingerprint && !x.planRecordPayload.empty(); });
        if (m == members_.end()) continue;
        const std::size_t pn = cr::wrapVerdictPlan(scratch_, m->planRecordPayload);
        if (append(std::span<const uint8_t>(scratch_.data(), pn)) < 0) return kChangeDeferred;
    }
    const std::size_t n = cr::buildConstraintSetChange(scratch_, logged.id, logged.catalogThrough, std::span<const cr::ChangeMember>(wire.data(), logged.count));
    const int64_t base = append(std::span<const uint8_t>(scratch_.data(), n));
    if (base < 0) return kChangeDeferred;
    state_->constraintSets.onChange(base, logged);
    return base;
}

void verdictOnHw(PartitionWriteState& state, int64_t hw) noexcept {
    state.markers.onHw(hw);
    state.rejected.setWatermark(state.markers.committedThrough());
}

}  // namespace rune::node
