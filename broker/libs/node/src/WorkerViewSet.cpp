// libs/node/src/WorkerViewSet.cpp — Task 15.
#include <rune/node/WorkerViewSet.h>
#include <rune/node/AdminChannels.h>   // Task 19: ViewPlanUpdateMsg + decodeViewPlanUpdate

#include <rune/codec/Serialize.h>      // rune::codec::deserialize
#include <rune/view/ViewPlan.h>        // rune::view::deserializeCompiledViewPlan
#include <rune/view/ViewRow.h>         // rune::view::buildRowLayout
#include <rune/view/AggState.h>        // isAggregateView, buildAggLayout, kAggTable*
#include <rune/view/ViewCheckpoint.h>  // v1/v2 checkpoint magics

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

// Replays checkpoint records into one instance: v1 into the output table for a
// Replace view; v2 (all three tables, offset-atomic) for an aggregate view.
int64_t replayInstance(ViewInstance& inst, std::span<const uint8_t> base,
                       std::span<const std::span<const uint8_t>> deltas) {
    if (!inst.agg) return rune::view::replay(inst.table, inst.layout, base, deltas);
    rune::view::ViewTable* tables[3];
    tables[rune::view::kAggTableView]         = &inst.table;
    tables[rune::view::kAggTableContributors] = &inst.agg->contributors;
    tables[rune::view::kAggTableGroups]       = &inst.agg->groups;
    uint32_t lens[3];
    lens[rune::view::kAggTableView]         = inst.layout.rowLen;
    lens[rune::view::kAggTableContributors] = inst.agg->layout.contributorRow.rowLen;
    lens[rune::view::kAggTableGroups]       = inst.agg->layout.groupRow.rowLen;
    int64_t last = -1;
    if (!base.empty()) {
        const auto a = rune::view::applyMultiDelta(tables, lens, base);
        if (!a) return -1;
        last = a->O;
    }
    const int64_t r = rune::view::replayMulti(tables, lens, deltas);
    return r >= 0 ? r : last;
}

} // namespace

// ---------------------------------------------------------------------------
// splitViewCheckpointRecords — see the header for the contract + wire format.
// ---------------------------------------------------------------------------
std::vector<std::span<const uint8_t>>
splitViewCheckpointRecords(std::span<const uint8_t> bytes) {
    static constexpr std::size_t kEntryHdr = 4 + 1 + 8;  // rid u32 + flags u8 + key u64
    static constexpr std::size_t kCrc      = 4;
    const std::size_t total = bytes.size();

    // Walks `count` entries of `rowLen` from `p`; 0 on truncation, else the end.
    auto walkEntries = [&](std::size_t p, uint32_t count, uint32_t rowLen) -> std::size_t {
        for (uint32_t i = 0; i < count; ++i) {
            if (p + kEntryHdr > total) return 0;
            const uint8_t flags = bytes[p + 4];
            p += kEntryHdr;
            if (flags == 0) {  // upsert carries rowLen payload bytes; a delete does not
                if (p + rowLen > total) return 0;
                p += rowLen;
            }
        }
        return p;
    };
    // v1: [magic][O i64][rowLen u32][count u32] entries [crc]
    auto v1End = [&](std::size_t pos) -> std::size_t {
        if (pos + 20 > total) return 0;
        const std::size_t p = walkEntries(pos + 20, readU32LE(bytes.data() + pos + 16),
                                          readU32LE(bytes.data() + pos + 12));
        return (p == 0 || p + kCrc > total) ? 0 : p + kCrc;
    };
    // v2: [magic][O i64][tableCount u8] { [id u8][rowLen u32][count u32] entries }* [crc]
    auto v2End = [&](std::size_t pos) -> std::size_t {
        if (pos + 13 > total) return 0;
        const uint8_t tableCount = bytes[pos + 12];
        std::size_t p = pos + 13;
        for (uint8_t t = 0; t < tableCount; ++t) {
            if (p + 9 > total) return 0;
            const uint32_t rowLen = readU32LE(bytes.data() + p + 1);
            const uint32_t count  = readU32LE(bytes.data() + p + 5);
            p = walkEntries(p + 9, count, rowLen);
            if (p == 0) return 0;
        }
        return p + kCrc > total ? 0 : p + kCrc;
    };

    std::vector<std::span<const uint8_t>> out;
    std::size_t pos = 0;
    while (pos + 4 <= total) {
        const uint32_t magic = readU32LE(bytes.data() + pos);
        std::size_t end = 0;
        if (magic == rune::view::kViewCheckpointMagic)        end = v1End(pos);
        else if (magic == rune::view::kViewCheckpointMagicV2) end = v2End(pos);
        if (end == 0) break;   // unknown magic or truncated trailing record
        out.push_back(bytes.subspan(pos, end - pos));
        pos = end;
    }
    return out;
}

WorkerViewSet::WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir)
    : channel_(channel), viewDataDir_(std::move(viewDataDir)) {}

void WorkerViewSet::installView(rune::view::CompiledViewPlan plan,
                                rune::view::RowLayout layout,
                                rune::codec::Descriptor evolvingSrc, bool persisted,
                                bool underRecovery) {
    const int32_t viewTopicId = plan.viewTopicId;

    // Reactor index: every source topic of this view points at it.  Registered
    // before the move so plan.sourceTopicId is still readable.
    for (int32_t srcTopicId : plan.sourceTopicId) {
        auto& ids = subscriptionIndex_[srcTopicId];
        bool present = false;
        for (int32_t v : ids) present = present || (v == viewTopicId);
        if (!present) ids.push_back(viewTopicId);
    }

    // Bug A: insert_or_assign would otherwise RESET a previously-latched
    // underRecovery back to its default (false) on a re-install (e.g.
    // installViewForRecovery re-registering the same template for a second
    // partition) unless the caller re-asserts it — every recovery call site
    // below does.
    templates_.insert_or_assign(
        viewTopicId,
        ViewTemplate{std::move(plan), std::move(layout), std::move(evolvingSrc), persisted,
                     underRecovery});
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
                        /*persisted=*/m.persistence == 0, /*underRecovery=*/m.recovery);
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

ViewInstance* WorkerViewSet::getOrCreate_(int32_t viewTopicId, int32_t partition,
                                          int64_t committedTail) {
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
    ViewInstance& inst = ins->second;

    // Phase 2a: an aggregate view also owns its contributor + group aux tables.
    // Built BEFORE any recovery load below, so a checkpoint (v2) restores all
    // three tables, and a transient re-fold starts from EMPTY aux tables.
    if (rune::view::isAggregateView(inst.plan)) {
        auto al = rune::view::buildAggLayout(inst.plan, inst.evolvingSrc);
        if (!al) {
            spdlog::error("WorkerViewSet: aggregate view {} partition {} not hosted — a compiled "
                          "aggregate references a field missing from its source descriptor",
                          viewTopicId, partition);
            views_.erase(ins);
            return nullptr;
        }
        inst.agg.emplace(std::move(*al), indexSlots, chunkBytes);
    }

    // Bug A fix (whole-branch re-review): a view installed FOR RESTART
    // RECOVERY must never serve a lazily-created DEFAULT-READY instance, even
    // after pruneRecoveryIfAllReady_ already erased viewTopicId from
    // recoveryViews_ because some OTHER partition of this same view reached
    // Ready earlier in this very driveViews_ pass (hasRecovery() can go false
    // before every owned partition has even been materialized once). The
    // underRecovery latch lives on the TEMPLATE — independent of, and never
    // reset by, that prune — so this path is correct regardless of prune
    // timing. Load this instance's own on-disk checkpoint (same file
    // materializeRecoveryInstancesFor would have read) and set readiness from
    // ITS OWN foldCursor vs. committedTail; a view with no cold checkpoint
    // file is a valid cold start (foldCursor 0, RebuildingViews) exactly as it
    // would be via the pre-materialized path.
    if (tpl.underRecovery) {
        loadRecoveryCheckpoint_(inst, viewTopicId, partition, committedTail);
    }
    return &inst;
}

void WorkerViewSet::loadRecoveryCheckpoint_(ViewInstance& inst, int32_t viewTopicId,
                                            int32_t partition, int64_t committedTail) {
    const std::vector<uint8_t> file =
        readWholeFile(deltaFilePath(viewDataDir_, viewTopicId, partition));
    const std::vector<std::span<const uint8_t>> records =
        splitViewCheckpointRecords(std::span<const uint8_t>(file));

    const int64_t recoveredO = replayInstance(
        inst, /*base=*/std::span<const uint8_t>{},
        std::span<const std::span<const uint8_t>>(records.data(), records.size()));
    inst.foldCursor   = std::max<int64_t>(recoveredO, 0);
    inst.lastEncodedO = recoveredO;  // the durable offset we recovered from (may be -1)
    inst.readiness = (inst.foldCursor >= committedTail) ? ViewReadiness::Ready
                                                        : ViewReadiness::RebuildingViews;
}

void WorkerViewSet::installViewForRecovery(
    rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
    rune::codec::Descriptor evolvingSrc, bool persisted, int32_t partition,
    std::span<const uint8_t> base,
    std::span<const std::span<const uint8_t>> deltasInOrder, int64_t committedTail) {
    const int32_t viewTopicId = plan.viewTopicId;

    // Register the plan template + reactor index exactly as the normal path
    // does (moves plan/layout/evolvingSrc into templates_).  underRecovery is
    // deliberately left FALSE on this call: this function constructs + loads
    // THIS (view, partition) instance explicitly below, from the CALLER-
    // supplied base/deltasInOrder (which may be synthetic test spans, not
    // necessarily this worker's on-disk file) — latching underRecovery=true
    // before the getOrCreate_ below would make it redundantly re-read +
    // re-apply viewDataDir_'s on-disk file for this SAME instance via
    // loadRecoveryCheckpoint_.  The latch is set true right after (below),
    // so it is in place before this call returns.
    installView(std::move(plan), std::move(layout), std::move(evolvingSrc), persisted);

    // Eagerly build the (view, partition) instance from the just-installed
    // template (recovery is partition-specific; it must not wait for a lazy
    // first driveFolds).  underRecovery is still false at this point, so
    // getOrCreate_ default-constructs a plain Ready instance (no auto-load);
    // its readiness is set definitively by the explicit replay() below.
    ViewInstance* inst = getOrCreate_(viewTopicId, partition, committedTail);
    if (inst == nullptr) return;  // defensive: template must exist (just installed)

    // Bug A (whole-branch review): latch the template's underRecovery=true
    // NOW, so that a LATER getOrCreate_ for a SIBLING partition of this same
    // view (not yet hosted here) still loads its own checkpoint instead of
    // defaulting to a half-built Ready instance — correctness independent of
    // pruneRecoveryIfAllReady_'s prune timing (recoveryViews_ may already have
    // dropped this viewTopicId by the time that sibling partition is driven).
    {
        auto tit = templates_.find(viewTopicId);
        if (tit != templates_.end()) tit->second.underRecovery = true;
    }

    // RECOVERING: load this instance's on-disk checkpoint into its table.  O is
    // the highest applied checkpoint offset (the cursor that WAS persisted — the
    // exclusive "resume from" offset, see buildAndPushCheckpoint_), or -1 when
    // nothing applied (empty/missing checkpoint) -> foldCursor starts at 0.
    const int64_t recoveredO = replayInstance(*inst, base, deltasInOrder);
    inst->foldCursor   = std::max<int64_t>(recoveredO, 0);
    inst->lastEncodedO = recoveredO;  // the durable offset we recovered from (may be -1)

    // REBUILDING_VIEWS unless the source is already caught up (then Ready now:
    // e.g. a brand-new view over an empty source, committedTail == 0).  Local
    // decision only — no peer/cluster input.
    inst->readiness = (inst->foldCursor >= committedTail) ? ViewReadiness::Ready
                                                          : ViewReadiness::RebuildingViews;
}

void WorkerViewSet::pruneRecoveryIfAllReady_(int32_t viewTopicId) {
    if (recoveryViews_.find(viewTopicId) == recoveryViews_.end()) return;  // not a recovery view
    for (const auto& kv : views_) {
        // instKey packs viewTopicId in the high 32 bits (see instKey()).
        if (static_cast<int32_t>(static_cast<uint32_t>(kv.first >> 32)) != viewTopicId) continue;
        if (kv.second.readiness != ViewReadiness::Ready) return;  // still rebuilding somewhere
    }
    recoveryViews_.erase(viewTopicId);
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
        // (view, partition) instance does not exist yet.
        auto tit = templates_.find(viewTopicId);
        outLayout = tit != templates_.end() ? &tit->second.layout : nullptr;
        // SERVE-GUARD (restart recovery): a view installed FOR RECOVERY whose
        // instance has not been materialized yet (e.g. this worker's owned source
        // replica is still being re-assigned post-restart, so driveViews_ has not
        // run materializeRecoveryInstancesFor for it) must answer a RETRIABLE
        // NotReady — NOT a terminal AbsentKey.  Otherwise a restarted broker
        // reports "key absent" for a durable row that is merely not reloaded yet,
        // and a client waiting for recovery gives up early.  A plain (non-recovery)
        // view keeps the AbsentKey/NotFound answer: its instance is created
        // serve-ready on the first driveFolds, so a missing instance there means
        // the partition simply has not folded and the key is genuinely absent.
        const bool underRecovery =
            tit != templates_.end() &&
            (tit->second.underRecovery ||
             recoveryViews_.find(viewTopicId) != recoveryViews_.end());
        if (outStatus != nullptr)
            *outStatus = underRecovery ? RowStatus::NotReady : RowStatus::NotFound;
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
    dirtyEntries_.clear();
    rowOffsets_.clear();
    rowScratch_.clear();

    // Drain one table into the shared scratch (row bytes copied — drainDirty's
    // pointer is only valid inside the callback; rowScratch_ may reallocate, so
    // pointers are fixed up below once it stops growing).
    auto drainInto = [&](rune::view::ViewTable& t, uint32_t rowLen) {
        t.drainDirty([&](uint32_t rid, const uint8_t* row, bool deleted, uint64_t key) {
            rune::view::DirtyEntry e;
            e.rid     = rid;
            e.deleted = deleted;
            e.key     = key;
            e.row     = nullptr;
            if (!deleted && row != nullptr) {
                rowOffsets_.push_back(rowScratch_.size());
                rowScratch_.insert(rowScratch_.end(), row, row + rowLen);
            } else {
                rowOffsets_.push_back(SIZE_MAX);
            }
            dirtyEntries_.push_back(e);
        });
    };
    auto fixup = [&] {
        for (std::size_t i = 0; i < dirtyEntries_.size(); ++i) {
            if (rowOffsets_[i] != SIZE_MAX) dirtyEntries_[i].row = rowScratch_.data() + rowOffsets_[i];
        }
    };

    inst.pendingDelta.clear();
    if (!inst.agg) {
        // Replace view: unchanged Phase 1 v1 record over the output table.
        drainInto(inst.table, inst.layout.rowLen);
        if (dirtyEntries_.empty()) return 0;  // nothing changed this batch — no delta
        fixup();
        rune::view::encodeDelta(dirtyEntries_, inst.foldCursor, inst.layout.rowLen,
                                inst.pendingDelta);
    } else {
        // Aggregate view: output rows + contributor map + group accumulators in
        // ONE v2 record, so recovery always restores all three at the same O.
        std::size_t b[4];
        b[0] = 0;
        drainInto(inst.table, inst.layout.rowLen);
        b[1] = dirtyEntries_.size();
        drainInto(inst.agg->contributors, inst.agg->layout.contributorRow.rowLen);
        b[2] = dirtyEntries_.size();
        drainInto(inst.agg->groups, inst.agg->layout.groupRow.rowLen);
        b[3] = dirtyEntries_.size();
        if (dirtyEntries_.empty()) return 0;
        fixup();
        auto sub = [&](int i) {
            return std::span<const rune::view::DirtyEntry>(dirtyEntries_.data() + b[i],
                                                          b[i + 1] - b[i]);
        };
        const rune::view::TableDelta tds[3] = {
            {rune::view::kAggTableView, inst.layout.rowLen, sub(0)},
            {rune::view::kAggTableContributors, inst.agg->layout.contributorRow.rowLen, sub(1)},
            {rune::view::kAggTableGroups, inst.agg->layout.groupRow.rowLen, sub(2)}};
        rune::view::encodeMultiDelta(tds, inst.foldCursor, inst.pendingDelta);
    }
    inst.lastEncodedO = inst.foldCursor;  // attempted/encoded offset, NOT durable
    inst.pendingValid = true;
    (void)tryPushDelta_(inst);            // on back-pressure the delta is held for next pass
    return 1;                             // a delta was encoded this pass (real work)
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
