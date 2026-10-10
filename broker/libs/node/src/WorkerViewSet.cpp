// libs/node/src/WorkerViewSet.cpp — Task 15.
#include <rune/node/WorkerViewSet.h>

#include <cstring>
#include <utility>

namespace rune::node {

namespace {

// Round `n` up to a power of two (>= 1).  ViewTable requires a power-of-two
// initial index slot count.
std::size_t ceilPow2(std::size_t n) noexcept {
    if (n <= 1) return 1;
    std::size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

void writeI32LE(uint8_t* dst, int32_t v) noexcept {
    const uint32_t u = static_cast<uint32_t>(v);
    dst[0] = static_cast<uint8_t>(u);
    dst[1] = static_cast<uint8_t>(u >> 8);
    dst[2] = static_cast<uint8_t>(u >> 16);
    dst[3] = static_cast<uint8_t>(u >> 24);
}

} // namespace

WorkerViewSet::WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir)
    : channel_(channel), viewDataDir_(std::move(viewDataDir)) {}

void WorkerViewSet::installView(rune::view::CompiledViewPlan plan,
                                rune::view::RowLayout layout,
                                rune::codec::Descriptor evolvingSrc, bool persisted) {
    const int32_t viewTopicId = plan.viewTopicId;

    // Reactor index: every source topic of this view points at it.  Registered
    // before the move so plan.sourceTopicId is still readable.
    for (int32_t srcTopicId : plan.sourceTopicId) {
        auto& ids = subscriptionIndex_[srcTopicId];
        bool present = false;
        for (int32_t v : ids) present = present || (v == viewTopicId);
        if (!present) ids.push_back(viewTopicId);
    }

    templates_.insert_or_assign(
        viewTopicId,
        ViewTemplate{std::move(plan), std::move(layout), std::move(evolvingSrc), persisted});
}

ViewInstance* WorkerViewSet::getOrCreate_(int32_t viewTopicId, int32_t partition) {
    const uint64_t key = instKey(viewTopicId, partition);
    auto it = views_.find(key);
    if (it != views_.end()) return &it->second;

    auto tit = templates_.find(viewTopicId);
    if (tit == templates_.end()) return nullptr;  // defensive: no plan installed
    const ViewTemplate& tpl = tit->second;

    // ViewTable sizing from the WITH-clause knobs: capacity -> initial index
    // slots (rounded up to a power of two; 0 => the table's own default),
    // chunkBytes -> row-slab chunk size.  A fresh table + fresh foldCursor PER
    // partition (independent of every other partition of this view).
    const std::size_t indexSlots = tpl.plan.sizing.capacity != 0
                                       ? ceilPow2(tpl.plan.sizing.capacity)
                                       : std::size_t{4096};
    const std::size_t chunkBytes = tpl.plan.sizing.chunkBytes != 0
                                       ? static_cast<std::size_t>(tpl.plan.sizing.chunkBytes)
                                       : (std::size_t{32} << 20);

    auto [ins, inserted] = views_.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(tpl.plan, tpl.layout, tpl.evolvingSrc, tpl.persisted,
                              partition, indexSlots, chunkBytes));
    (void)inserted;
    return &ins->second;
}

void WorkerViewSet::installViewForRecovery(
    rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
    rune::codec::Descriptor evolvingSrc, bool persisted, int32_t partition,
    std::span<const uint8_t> base,
    std::span<const std::span<const uint8_t>> deltasInOrder, int64_t committedTail) {
    const int32_t viewTopicId = plan.viewTopicId;

    // Register the plan template + reactor index exactly as the normal path does
    // (moves plan/layout/evolvingSrc into templates_).
    installView(std::move(plan), std::move(layout), std::move(evolvingSrc), persisted);

    // Eagerly build the (view, partition) instance from the just-installed
    // template (recovery is partition-specific; it must not wait for a lazy
    // first driveFolds).
    ViewInstance* inst = getOrCreate_(viewTopicId, partition);
    if (inst == nullptr) return;  // defensive: template must exist (just installed)

    // RECOVERING: load this instance's on-disk checkpoint into its table.  O is
    // the highest applied checkpoint offset (the cursor that WAS persisted — the
    // exclusive "resume from" offset, see buildAndPushCheckpoint_), or -1 when
    // nothing applied (empty/missing checkpoint) -> foldCursor starts at 0.
    const int64_t recoveredO = rune::view::replay(inst->table, inst->layout, base, deltasInOrder);
    inst->foldCursor   = std::max<int64_t>(recoveredO, 0);
    inst->lastEncodedO = recoveredO;  // the durable offset we recovered from (may be -1)

    // REBUILDING_VIEWS unless the source is already caught up (then Ready now:
    // e.g. a brand-new view over an empty source, committedTail == 0).  Local
    // decision only — no peer/cluster input.
    inst->readiness = (inst->foldCursor >= committedTail) ? ViewReadiness::Ready
                                                          : ViewReadiness::RebuildingViews;
}

ViewReadiness WorkerViewSet::readiness(int32_t viewTopicId, int32_t partition) const {
    auto it = views_.find(instKey(viewTopicId, partition));
    if (it == views_.end()) return ViewReadiness::Ready;  // nothing hosted => nothing to gate
    return it->second.readiness;
}

const uint8_t* WorkerViewSet::getRow(int32_t viewTopicId, int32_t partition, uint64_t key,
                                     const rune::view::RowLayout*& outLayout,
                                     RowStatus* outStatus) const {
    auto it = views_.find(instKey(viewTopicId, partition));
    if (it == views_.end()) {
        // Serve the layout from the template if the view is installed but this
        // partition has not folded yet; row is still absent.
        auto tit = templates_.find(viewTopicId);
        outLayout = tit != templates_.end() ? &tit->second.layout : nullptr;
        if (outStatus != nullptr) *outStatus = RowStatus::NotFound;
        return nullptr;
    }
    outLayout = &it->second.layout;
    // SERVE-GUARD: a view still rebuilding after a restart must NEVER serve a
    // half-built row — surface a RETRIABLE not-ready instead (Review-focus #5).
    if (it->second.readiness != ViewReadiness::Ready) {
        if (outStatus != nullptr) *outStatus = RowStatus::NotReady;
        return nullptr;
    }
    const uint8_t* row = it->second.table.get(key);
    if (outStatus != nullptr) *outStatus = (row != nullptr) ? RowStatus::Ok : RowStatus::NotFound;
    return row;
}

int WorkerViewSet::buildAndPushCheckpoint_(ViewInstance& inst) {
    const uint32_t rowLen = inst.layout.rowLen;

    dirtyEntries_.clear();
    rowOffsets_.clear();
    rowScratch_.clear();

    // First pass: drain, copying upsert row bytes into rowScratch_ and recording
    // their offsets (rowScratch_ may reallocate, so defer pointer fixup).
    inst.table.drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
        rune::view::DirtyEntry e;
        e.rid     = rid;
        e.deleted = deleted;
        e.key     = key;
        e.row     = nullptr;  // fixed up after rowScratch_ stops growing
        if (!deleted && row != nullptr) {
            rowOffsets_.push_back(rowScratch_.size());
            rowScratch_.insert(rowScratch_.end(), row, row + rowLen);
        } else {
            rowOffsets_.push_back(SIZE_MAX);
        }
        dirtyEntries_.push_back(e);
    });

    if (dirtyEntries_.empty()) return 0;  // nothing changed this batch — no delta

    // Fixup: rowScratch_ is now stable; point each upsert entry at its bytes.
    for (std::size_t i = 0; i < dirtyEntries_.size(); ++i) {
        if (rowOffsets_[i] != SIZE_MAX) {
            dirtyEntries_[i].row = rowScratch_.data() + rowOffsets_[i];
        }
    }

    // Encode the delta at this batch's snapshot offset O == the folded cursor.
    inst.pendingDelta.clear();
    rune::view::encodeDelta(dirtyEntries_, inst.foldCursor, rowLen, inst.pendingDelta);
    inst.lastEncodedO = inst.foldCursor;  // attempted/encoded offset, NOT durable
    inst.pendingValid = true;

    (void)tryPushDelta_(inst);  // on back-pressure the delta is held for next pass
    return 1;                   // a delta was encoded this pass (real work)
}

bool WorkerViewSet::tryPushDelta_(ViewInstance& inst) {
    const std::size_t frameLen =
        ViewSnapshotChannel::kDeltaHeaderBytes + inst.pendingDelta.size();

    const auto claim =
        channel_.deltas.tryClaim(ViewSnapshotChannel::kDeltaMsgType, frameLen);
    if (claim < 0) return false;  // ring full: hold, never drop

    uint8_t* dst = channel_.deltas.base() + claim;
    writeI32LE(dst, inst.plan.viewTopicId);
    writeI32LE(dst + 4, inst.partition);
    std::memcpy(dst + ViewSnapshotChannel::kDeltaHeaderBytes,
                inst.pendingDelta.data(), inst.pendingDelta.size());
    channel_.deltas.commit(claim);

    inst.pendingValid = false;
    return true;
}

} // namespace rune::node
