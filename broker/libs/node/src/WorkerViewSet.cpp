// libs/node/src/WorkerViewSet.cpp — Task 15.
#include <rune/node/WorkerViewSet.h>
#include <rune/node/AdminChannels.h>   // Task 19: ViewPlanUpdateMsg + decodeViewPlanUpdate

#include <rune/codec/Serialize.h>      // rune::codec::deserialize
#include <rune/view/ViewPlan.h>        // rune::view::deserializeCompiledViewPlan
#include <rune/view/ViewRow.h>         // rune::view::buildRowLayout

#include <spdlog/spdlog.h>

#include <cstring>
#include <fstream>
#include <optional>
#include <span>
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

uint32_t readU32LE(const uint8_t* p) noexcept {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

// Task 19b: this worker's checkpoint delta-file path for (view, partition).
// MUST match ViewSnapshotIoAgent::deltaFilePath_ exactly (same viewDataDir_,
// same name) so recovery reopens the very file the IO agent appended to.
std::string deltaFilePath(const std::string& dir, int32_t viewTopicId, int32_t partition) {
    return dir + "/view-" + std::to_string(static_cast<uint32_t>(viewTopicId)) +
           "-p" + std::to_string(partition) + ".delta";
}

// Read a whole file into a byte vector; returns empty on any error (a missing
// checkpoint is a valid cold start, not a failure).
std::vector<uint8_t> readWholeFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return {};
    const std::streamoff n = f.tellg();
    if (n <= 0) return {};
    std::vector<uint8_t> out(static_cast<std::size_t>(n));
    f.seekg(0);
    f.read(reinterpret_cast<char*>(out.data()), n);
    if (!f) return {};
    return out;
}

} // namespace

// ---------------------------------------------------------------------------
// splitViewCheckpointRecords — see the header for the contract + wire format.
// ---------------------------------------------------------------------------
std::vector<std::span<const uint8_t>>
splitViewCheckpointRecords(std::span<const uint8_t> bytes) {
    // Per-record fixed header = magic u32 + O i64 + rowLen u32 + count u32.
    static constexpr std::size_t kHdr      = 4 + 8 + 4 + 4;
    static constexpr std::size_t kEntryHdr = 4 + 1 + 8;  // rid u32 + flags u8 + key u64
    static constexpr std::size_t kCrc      = 4;

    std::vector<std::span<const uint8_t>> out;
    const std::size_t total = bytes.size();
    std::size_t pos = 0;
    while (pos + kHdr <= total) {
        const std::size_t recStart = pos;
        const uint32_t rowLen = readU32LE(bytes.data() + pos + 12);
        const uint32_t count  = readU32LE(bytes.data() + pos + 16);
        std::size_t p = pos + kHdr;
        bool ok = true;
        for (uint32_t i = 0; i < count; ++i) {
            if (p + kEntryHdr > total) { ok = false; break; }
            const uint8_t flags = bytes[p + 4];
            p += kEntryHdr;
            if (flags == 0) {  // upsert carries rowLen payload bytes; a delete does not
                if (p + rowLen > total) { ok = false; break; }
                p += rowLen;
            }
        }
        if (!ok) break;                 // truncated entries → drop this trailing record
        if (p + kCrc > total) break;    // truncated CRC → drop this trailing record
        p += kCrc;
        out.push_back(bytes.subspan(recStart, p - recStart));
        pos = p;
    }
    return out;
}

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

int WorkerViewSet::drainInstalls() {
    // Single consumer (this worker thread) of the control->worker install ring.
    return channel_.install.drain(
        [this](int32_t tid, aeron::concurrent::AtomicBuffer& b,
               aeron::util::index_t off, aeron::util::index_t len) {
            if (tid != ViewSnapshotChannel::kInstallMsgType) return;
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const ViewPlanUpdateMsg m = decodeViewPlanUpdate(p, static_cast<std::size_t>(len));

            std::optional<rune::view::CompiledViewPlan> plan =
                rune::view::deserializeCompiledViewPlan(
                    std::span<const uint8_t>(m.planBlob.data(), m.planBlob.size()));
            if (!plan) {
                spdlog::error("WorkerViewSet: install for viewTopicId={} dropped — "
                              "compiled plan failed to deserialize", m.viewTopicId);
                return;
            }
            rune::codec::Descriptor outDesc;
            rune::codec::Descriptor srcDesc;
            try {
                outDesc = rune::codec::deserialize(
                    std::span<const uint8_t>(m.outDescBlob.data(), m.outDescBlob.size()));
                srcDesc = rune::codec::deserialize(
                    std::span<const uint8_t>(m.srcDescBlob.data(), m.srcDescBlob.size()));
            } catch (const std::exception& ex) {
                spdlog::error("WorkerViewSet: install for viewTopicId={} dropped — "
                              "descriptor deserialize failed: {}", m.viewTopicId, ex.what());
                return;
            }
            rune::view::RowLayout layout;
            try {
                layout = rune::view::buildRowLayout(outDesc);
            } catch (const std::exception& ex) {
                spdlog::error("WorkerViewSet: install for viewTopicId={} dropped — "
                              "buildRowLayout failed: {}", m.viewTopicId, ex.what());
                return;
            }
            const int32_t viewTopicId = plan->viewTopicId;
            installView(std::move(*plan), std::move(layout), std::move(srcDesc),
                        /*persisted=*/m.persistence == 0);
            // Task 19b: a RESTART-recovery install registers the template here
            // but defers the per-(view, partition) instance to
            // materializeRecoveryInstancesFor (checkpoint load + serve-guard),
            // driven once this worker knows which partitions it owns.  A plain
            // (create-time) install never marks the view, so its instances are
            // created serve-ready by driveFolds exactly as before.
            if (m.recovery) recoveryViews_.insert(viewTopicId);
        },
        /*limit=*/8);
}

void WorkerViewSet::materializeRecoveryInstancesFor(int32_t srcTopicId, int32_t partition,
                                                    int64_t committedTail) {
    if (recoveryViews_.empty()) return;  // no recovery view installed — nothing to do
    auto sit = subscriptionIndex_.find(srcTopicId);
    if (sit == subscriptionIndex_.end()) return;

    // Copy the interested-view id list: installViewForRecovery → installView may
    // touch subscriptionIndex_ (a new source key could rehash it, invalidating
    // `sit`), so iterate a stable snapshot.
    const std::vector<int32_t> interested = sit->second;
    for (int32_t viewTopicId : interested) {
        if (recoveryViews_.find(viewTopicId) == recoveryViews_.end()) continue;  // not a recovery view
        if (views_.find(instKey(viewTopicId, partition)) != views_.end()) continue;  // already hosted
        auto tit = templates_.find(viewTopicId);
        if (tit == templates_.end()) continue;  // defensive: template must exist

        // COLD, once-per-instance: load this (view, partition)'s checkpoint
        // file (base + deltas, concatenated by the IO agent) from disk and
        // split it back into Task-12 records.  A missing/empty file is a valid
        // cold start — installViewForRecovery then starts at foldCursor 0 and
        // the committed log is replayed in full by driveRebuild.
        const std::vector<uint8_t> file =
            readWholeFile(deltaFilePath(viewDataDir_, viewTopicId, partition));
        const std::vector<std::span<const uint8_t>> records =
            splitViewCheckpointRecords(std::span<const uint8_t>(file));

        const ViewTemplate& tpl = tit->second;
        installViewForRecovery(tpl.plan, tpl.layout, tpl.evolvingSrc, tpl.persisted, partition,
                               /*base=*/std::span<const uint8_t>{},
                               std::span<const std::span<const uint8_t>>(records.data(), records.size()),
                               committedTail);
    }
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
