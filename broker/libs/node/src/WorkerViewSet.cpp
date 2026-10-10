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

    // ViewTable sizing from the WITH-clause knobs: capacity -> initial index
    // slots (rounded up to a power of two; 0 => the table's own default),
    // chunkBytes -> row-slab chunk size.
    const std::size_t indexSlots = plan.sizing.capacity != 0
                                       ? ceilPow2(plan.sizing.capacity)
                                       : std::size_t{4096};
    const std::size_t chunkBytes = plan.sizing.chunkBytes != 0
                                       ? static_cast<std::size_t>(plan.sizing.chunkBytes)
                                       : (std::size_t{32} << 20);

    // Reactor index: every source topic of this view points at it.  Register
    // before the move so plan.sourceTopicId is still readable.
    for (int32_t srcTopicId : plan.sourceTopicId) {
        auto& ids = subscriptionIndex_[srcTopicId];
        bool present = false;
        for (int32_t v : ids) present = present || (v == viewTopicId);
        if (!present) ids.push_back(viewTopicId);
    }

    views_.emplace(std::piecewise_construct, std::forward_as_tuple(viewTopicId),
                   std::forward_as_tuple(std::move(plan), std::move(layout),
                                         std::move(evolvingSrc), persisted,
                                         indexSlots, chunkBytes));
}

const uint8_t* WorkerViewSet::getRow(int32_t viewTopicId, uint64_t key,
                                     const rune::view::RowLayout*& outLayout) const {
    auto it = views_.find(viewTopicId);
    if (it == views_.end()) {
        outLayout = nullptr;
        return nullptr;
    }
    outLayout = &it->second.layout;
    return it->second.table.get(key);
}

void WorkerViewSet::buildAndPushCheckpoint_(ViewInstance& inst, int32_t partition) {
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

    if (dirtyEntries_.empty()) return;  // nothing changed this batch — no delta

    // Fixup: rowScratch_ is now stable; point each upsert entry at its bytes.
    for (std::size_t i = 0; i < dirtyEntries_.size(); ++i) {
        if (rowOffsets_[i] != SIZE_MAX) {
            dirtyEntries_[i].row = rowScratch_.data() + rowOffsets_[i];
        }
    }

    // Encode the delta at this batch's snapshot offset O == the folded cursor.
    inst.pendingDelta.clear();
    rune::view::encodeDelta(dirtyEntries_, inst.foldCursor, rowLen, inst.pendingDelta);
    inst.lastCheckpointO  = inst.foldCursor;
    inst.pendingPartition = partition;
    inst.pendingValid     = true;

    (void)tryPushDelta_(inst);  // on back-pressure the delta is held for next pass
}

bool WorkerViewSet::tryPushDelta_(ViewInstance& inst) {
    const std::size_t frameLen =
        ViewSnapshotChannel::kDeltaHeaderBytes + inst.pendingDelta.size();

    const auto claim =
        channel_.deltas.tryClaim(ViewSnapshotChannel::kDeltaMsgType, frameLen);
    if (claim < 0) return false;  // ring full: hold, never drop

    uint8_t* dst = channel_.deltas.base() + claim;
    writeI32LE(dst, inst.plan.viewTopicId);
    writeI32LE(dst + 4, inst.pendingPartition);
    std::memcpy(dst + ViewSnapshotChannel::kDeltaHeaderBytes,
                inst.pendingDelta.data(), inst.pendingDelta.size());
    channel_.deltas.commit(claim);

    inst.pendingValid = false;
    return true;
}

} // namespace rune::node
