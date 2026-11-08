// libs/node/src/WorkerViewSet.cpp — Task 15.
#include <rune/node/WorkerViewSet.h>
#include <rune/node/AdminChannels.h>   // Task 19: ViewPlanUpdateMsg + decodeViewPlanUpdate

#include <rune/codec/Serialize.h>      // rune::codec::deserialize
#include <rune/codec/UnicodeLower.h>   // unicodeVersionSupported
#include <rune/codec/ViewKeyEncoding.h>  // decodeKeyComponent
#include <rune/protocol/client/DataMessages.h>   // ViewRowResponse::Status
#include <rune/view/ViewPlan.h>        // rune::view::deserializeCompiledViewPlan
#include <rune/view/ViewRow.h>         // rune::view::buildRowLayout
#include <rune/view/AggState.h>        // isAggregateView, buildAggLayout, kAggTable*
#include <rune/view/ViewCheckpoint.h>  // v1/v2/v3 checkpoint magics

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <utility>
#include <variant>

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

// Which checkpoint files a (view, partition) had on disk at recovery.  Issue #537: a legacy file
// (legacyViewCheckpointFilePath) may hold aborted-transaction data and is never replayed.  Issue #590: recovery
// never deletes anything -- it only reads; the IO agent (the one filesystem toucher) backs files up when told to.
// Task 19b: this (view, partition)'s checkpoint bytes -- the very file the IO agent appends to (viewCheckpointFilePath).
// A pure read: cold, once per instance, on the worker that owns it.
RecoveredCheckpoint readCheckpointFile(const std::string& dir, int32_t viewTopicId, int32_t partition) {
    RecoveredCheckpoint rc;
    rc.bytes = readWholeFile(viewCheckpointFilePath(dir, viewTopicId, partition));
    std::error_code ec;
    const bool legacy = std::filesystem::exists(legacyViewCheckpointFilePath(dir, viewTopicId, partition), ec);
    const bool committed = !rc.bytes.empty();
    rc.origin = committed ? (legacy ? CheckpointOrigin::CommittedAndLegacy : CheckpointOrigin::Committed)
                          : (legacy ? CheckpointOrigin::LegacyOnly : CheckpointOrigin::None);
    return rc;
}

const char* checkpointFormat(const ViewInstance& inst) noexcept { return inst.keyed ? "v3" : inst.agg ? "v2" : "v1"; }

// The row lengths of UniqueClaims' two tables (UniqueClaims.h): index [owner W][extraCount u32][head W][flags u8] and
// extras [N K][prev W][next W][links u8][image].
struct ClaimsRowLens { uint32_t index, extras; };
ClaimsRowLens claimsRowLensOf(const ViewInstance& inst) noexcept {
    const uint32_t K = inst.keyed->keyWidth, W = inst.keyed->wireKeyWidth;
    return {2 * W + 5, K + 2 * W + 1 + inst.layout.rowLen};
}

// Replays checkpoint records into one instance: v1 into the output table for a
// Replace view; v2 (all three tables, or four with Min/Max value blocks; offset-atomic) for an aggregate view;
// v3 for a keyed view: the view table and the owner map (a projection), or the view table, contributors, groups, blocks and groupIds
// (an aggregate; nextGroupId is then 1 + the largest gid recovered).
// Same semantics as rune::view::replay/replayMulti (a bad base => -1; stop at
// the first bad record; return the last applied O), plus ONE warning when the
// replay stops before the end of the supplied records — so a wrong-version
// record (v1 in an aggregate file, v2 in a Replace file) or mid-file
// corruption is visible rather than a silent shorter recovery.
int64_t replayInstance(ViewInstance& inst, std::span<const uint8_t> base,
                       std::span<const std::span<const uint8_t>> deltas,
                       std::size_t& appliedOut) {
    appliedOut = 0;
    rune::view::ViewTable* tables[8] = {};
    uint32_t lens[8] = {};
    uint8_t keyLens[8] = {};
    std::size_t nTables = 0;
    if (inst.keyed && inst.agg) {
        using namespace rune::view;
        const AggLayout& a = inst.agg->layout;
        const uint8_t K = static_cast<uint8_t>(a.groupKeyWidth);
        tables[kKeyedTableView]         = &inst.table;
        tables[kKeyedTableContributors] = &inst.agg->contributors;
        tables[kKeyedTableGroups]       = &inst.agg->groups;
        tables[kKeyedTableGroupIds]     = &*inst.agg->groupIds;
        lens[kKeyedTableView] = inst.layout.rowLen;  keyLens[kKeyedTableView] = K;
        lens[kKeyedTableContributors] = a.contributorRow.rowLen;  keyLens[kKeyedTableContributors] = 8;
        lens[kKeyedTableGroups] = a.groupRow.rowLen;  keyLens[kKeyedTableGroups] = 8;
        lens[kKeyedTableGroupIds] = 8;  keyLens[kKeyedTableGroupIds] = K;
        lens[kKeyedTableOwners] = 0;  keyLens[kKeyedTableOwners] = 0;
        lens[kKeyedTableBlocks] = a.blockRow.rowLen;  keyLens[kKeyedTableBlocks] = 8;
        if (inst.agg->blocks) tables[kKeyedTableBlocks] = &*inst.agg->blocks;
        nTables = 6;
    } else if (inst.keyed) {
        tables[rune::view::kKeyedTableView] = &inst.table;
        lens[rune::view::kKeyedTableView]    = inst.layout.rowLen;
        keyLens[rune::view::kKeyedTableView] = static_cast<uint8_t>(inst.keyed->keyWidth);
        nTables = 1;
        if (inst.owners) {
            tables[rune::view::kKeyedTableOwners] = &*inst.owners;
            lens[rune::view::kKeyedTableOwners]    = inst.keyed->keyWidth;
            keyLens[rune::view::kKeyedTableOwners] = inst.keyed->wireKeyWidth;
            nTables = 2;
        }
        if (inst.claims) {
            tables[rune::view::kKeyedTableUniqueIndex]  = &inst.claims->indexTable();
            tables[rune::view::kKeyedTableUniqueExtras] = &inst.claims->extrasTable();
            const ClaimsRowLens c = claimsRowLensOf(inst);
            lens[rune::view::kKeyedTableUniqueIndex]  = c.index;
            lens[rune::view::kKeyedTableUniqueExtras] = c.extras;
            keyLens[rune::view::kKeyedTableUniqueIndex]  = static_cast<uint8_t>(inst.keyed->keyWidth);
            keyLens[rune::view::kKeyedTableUniqueExtras] = inst.keyed->wireKeyWidth;
            nTables = 8;
        }
    } else if (inst.agg) {
        tables[rune::view::kAggTableView]         = &inst.table;
        tables[rune::view::kAggTableContributors] = &inst.agg->contributors;
        tables[rune::view::kAggTableGroups]       = &inst.agg->groups;
        lens[rune::view::kAggTableView]         = inst.layout.rowLen;
        lens[rune::view::kAggTableContributors] = inst.agg->layout.contributorRow.rowLen;
        lens[rune::view::kAggTableGroups]       = inst.agg->layout.groupRow.rowLen;
        nTables = 3;
        if (inst.agg->blocks) {   // Task 5b: Min/Max value blocks, table id 3
            tables[rune::view::kAggTableBlocks] = &*inst.agg->blocks;
            lens[rune::view::kAggTableBlocks]   = inst.agg->layout.blockRow.rowLen;
            nTables = 4;
        }
    }
    auto apply = [&](std::span<const uint8_t> rec) -> std::optional<rune::view::AppliedDelta> {
        if (inst.keyed)
            return rune::view::applyKeyedDelta(std::span<rune::view::ViewTable* const>(tables, nTables),
                                               std::span<const uint32_t>(lens, nTables), std::span<const uint8_t>(keyLens, nTables), rec);
        if (inst.agg)
            return rune::view::applyMultiDelta(std::span<rune::view::ViewTable* const>(tables, nTables),
                                               std::span<const uint32_t>(lens, nTables), rec);
        return rune::view::applyDelta(inst.table, inst.layout, rec);
    };
    // The groupIds table is empty before a replay, so every row it holds afterwards is dirty: the one cold scan for nextGroupId.
    const auto gidsDone = [&] {
        if (!(inst.keyed && inst.agg)) return;
        inst.agg->nextGroupId = 0;
        inst.agg->groupIds->drainDirtyKeyed([&](uint32_t, const uint8_t* row, bool deleted, std::span<const uint8_t>) {
            if (!deleted) inst.agg->nextGroupId = std::max(inst.agg->nextGroupId, rune::view::loadU64(row) + 1);
        });
    };
    int64_t last = -1;
    if (!base.empty()) {
        const auto a = apply(base);
        if (!a) {
            spdlog::warn("WorkerViewSet: view {} partition {} checkpoint BASE rejected ({} "
                         "record expected) — recovering from an empty table",
                         inst.plan.viewTopicId, inst.partition, checkpointFormat(inst));
            return -1;
        }
        last = a->O;
        ++appliedOut;
    }
    for (std::size_t i = 0; i < deltas.size(); ++i) {
        const auto a = apply(deltas[i]);
        if (!a) {
            spdlog::warn("WorkerViewSet: view {} partition {} checkpoint replay stopped at "
                         "record {} of {} ({} record expected) — recovered O {}",
                         inst.plan.viewTopicId, inst.partition, i, deltas.size(),
                         checkpointFormat(inst), last);
            break;
        }
        last = a->O;
        ++appliedOut;
    }
    gidsDone();
    if (inst.claims) {
        inst.claims->recountAfterReplay();
        inst.counters.uniqueConflicts = inst.claims->extras();
    }
    return last;
}

// ONE stable, greppable line per successful recovery replay (control plane —
// once per (view, partition) restart, never on the fold hot path).  The e2e
// greps "VIEW-RECOVERED view=<id>" to prove a checkpoint (not a cold re-fold)
// restored the view.  Nothing applied (cold start) is not a recovery: no line.
void logRecovered(const ViewInstance& inst, int64_t recoveredO, std::size_t applied) {
    if (applied == 0) return;
    spdlog::info("VIEW-RECOVERED view={} partition={} O={} records={} format={}",
                 inst.plan.viewTopicId, inst.partition, recoveredO, applied,
                 checkpointFormat(inst));
}

// Most source records one fold pass may cover so the pass's checkpoint delta
// (worst case) fits ONE ring message of `maxMsg` payload bytes; 0 if not even
// one record fits.  Worst case per source record: Replace dirties 1 output row;
// an aggregate dirties rune::view::aggDirtyBoundPerRecord rows per table (2a:
// contributor + old/new group + old/new output row; linked views also the
// contributor's two old neighbours and the new group's old head; Min/Max views
// also the old group's swap-moved contributor and up to 3 value-block chunks).
int64_t maxRecordsPerPassFor(const ViewInstance& inst, std::size_t maxMsg) {
    using namespace rune::view;
    std::size_t fixed = ViewSnapshotChannel::kDeltaHeaderBytes + kCheckpointCrcBytes;
    std::size_t perRecord = 0;
    if (inst.keyed && inst.agg) {
        const AggLayout& a = inst.agg->layout;
        const std::size_t nTables = (a.numBlockCols > 0 ? 4 : 3) + 1;   // view, contributors, groups, blocks?, groupIds
        fixed += kCheckpointV3HeaderBytes + nTables * kCheckpointV3TableHeaderBytes;
        const AggDirtyBound b = aggDirtyBoundPerRecord(a);
        const std::size_t aux = checkpointV3EntryHeaderBytes(8), wide = checkpointV3EntryHeaderBytes(a.groupKeyWidth);
        perRecord = b.contributorRows * (aux + a.contributorRow.rowLen) + b.groupRows * (aux + a.groupRow.rowLen) +
                    b.viewRows * (wide + inst.layout.rowLen) + b.blockRows * (aux + a.blockRow.rowLen) + b.groupIdRows * (wide + 8);
    } else if (inst.keyed) {
        const KeyedFoldSpec& k = *inst.keyed;
        fixed += kCheckpointV3HeaderBytes + (inst.claims ? 4 : k.ownerMapped ? 2 : 1) * kCheckpointV3TableHeaderBytes;
        // A move: the old row's delete, the new row's upsert and, with an owner map, the owner's upsert.
        perRecord = checkpointV3EntryHeaderBytes(static_cast<uint8_t>(k.keyWidth)) * 2 + inst.layout.rowLen;
        if (k.ownerMapped) perRecord += checkpointV3EntryHeaderBytes(k.wireKeyWidth) + k.keyWidth;
        // Claims: a move touches two index entries and up to kMaxExtrasRowsPerRecord extras.
        if (inst.claims) {
            const ClaimsRowLens c = claimsRowLensOf(inst);
            perRecord += 2 * (checkpointV3EntryHeaderBytes(static_cast<uint8_t>(k.keyWidth)) + c.index) + kMaxExtrasRowsPerRecord * (checkpointV3EntryHeaderBytes(k.wireKeyWidth) + c.extras);
        }
    } else if (inst.agg) {
        const AggLayout& a = inst.agg->layout;
        const std::size_t nTables = a.numBlockCols > 0 ? 4 : 3;
        fixed += kCheckpointV2HeaderBytes + nTables * kCheckpointV2TableHeaderBytes;
        const AggDirtyBound b = aggDirtyBoundPerRecord(a);
        perRecord = b.contributorRows * (kCheckpointEntryHeaderBytes + a.contributorRow.rowLen) +
                    b.groupRows * (kCheckpointEntryHeaderBytes + a.groupRow.rowLen) +
                    b.viewRows * (kCheckpointEntryHeaderBytes + inst.layout.rowLen) +
                    b.blockRows * (kCheckpointEntryHeaderBytes + a.blockRow.rowLen);
    } else {
        fixed += kCheckpointV1HeaderBytes;
        perRecord = kCheckpointEntryHeaderBytes + inst.layout.rowLen;
    }
    if (maxMsg <= fixed) return 0;
    return static_cast<int64_t>((maxMsg - fixed) / perRecord);
}

} // namespace

WorkerViewSet::WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir, uint64_t hashSeed)
    : channel_(channel), viewDataDir_(std::move(viewDataDir)), hashSeed_(hashSeed) {
    txnOverlays_.supersedeKeys.seed(hashSeed);
}

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
    // underRecovery back to its default (false) on a re-install unless the
    // caller re-asserts it.  installViewForRecovery no longer re-installs an
    // existing template.
    templates_.insert_or_assign(
        viewTopicId,
        ViewTemplate{std::move(plan), std::move(layout), std::move(evolvingSrc), persisted,
                     underRecovery});
}

bool WorkerViewSet::uninstallView(int32_t viewTopicId) {
    // A verdict view that a judge has taken in stays (Retiring) until a committed marker passes its drop: records up to the
    // drop are still judged with it, on every replica (P4-6).
    if (const auto tit = templates_.find(viewTopicId); tit != templates_.end() && tit->second.isVerdict) {
        const bool judged = std::any_of(verdictSources_.begin(), verdictSources_.end(), [&](const auto& kv) {
            return kv.second->hasView(viewTopicId);
        });
        if (judged) {
            for (auto& kv : verdictSources_) kv.second->markLeaving(viewTopicId);
            tit->second.retiring = true;
            ++verdictEpoch_;
            return true;
        }
    }
    return eraseView_(viewTopicId);
}

bool WorkerViewSet::eraseView_(int32_t viewTopicId) {
    ++verdictEpoch_;
    if (const auto tit = templates_.find(viewTopicId); tit != templates_.end() && tit->second.isVerdict)
        for (auto& kv : verdictSources_) kv.second->removeVerdictView(viewTopicId);
    bool had = templates_.erase(viewTopicId) != 0;
    for (auto it = views_.begin(); it != views_.end();) {
        if (static_cast<int32_t>(static_cast<uint32_t>(it->first >> 32)) == viewTopicId) {
            releaseMem_(it->second);   // Task 12: the namespace's view memory falls with the view
            it = views_.erase(it);   // frees the instance's table / aggregate / pending delta
            had = true;
        } else {
            ++it;
        }
    }
    for (auto it = subscriptionIndex_.begin(); it != subscriptionIndex_.end();) {
        std::erase(it->second, viewTopicId);
        it = it->second.empty() ? subscriptionIndex_.erase(it) : std::next(it);
    }
    had = (recoveryViews_.erase(viewTopicId) != 0) || had;
    if (had) spdlog::info("WorkerViewSet: view viewTopicId={} uninstalled (stops folding, instances freed)",
                          viewTopicId);
    return had;
}

int WorkerViewSet::drainInstalls() {
    // Single consumer (this worker thread) of the control->worker install ring.
    return channel_.install.drain(
        [this](int32_t tid, aeron::concurrent::AtomicBuffer& b,
               aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            if (tid == ViewSnapshotChannel::kCatalogMarkMsgType) {
                int64_t appliedThrough;
                std::memcpy(&appliedThrough, p, sizeof appliedThrough);
                catalogThrough_ = std::max(catalogThrough_, appliedThrough);
                if (installsSinceMark_) { installsSinceMark_ = false; ++verdictEpoch_; }   // the sync reruns even when the position did not move
                return;
            }
            if (tid != ViewSnapshotChannel::kInstallMsgType) return;
            const ViewPlanUpdateMsg m = decodeViewPlanUpdate(p, static_cast<std::size_t>(len));
            if (m.uninstall) {   // catalog history Task 8: DropView / AlterView retired it
                if (isVerdictTemplate_(m.viewTopicId)) installsSinceMark_ = true;
                (void)uninstallView(m.viewTopicId);
                return;
            }

            std::optional<rune::view::CompiledViewPlan> plan =
                rune::view::deserializeCompiledViewPlan(
                    std::span<const uint8_t>(m.planBlob.data(), m.planBlob.size()));
            if (!plan) {
                spdlog::error("WorkerViewSet: install for viewTopicId={} dropped — "
                              "compiled plan failed to deserialize", m.viewTopicId);
                noteRefused_(0, rune::view::verdictPlanFingerprint(m.planBlob));
                return;
            }
            if (plan->unicodeVersion != 0 && !rune::codec::unicodeVersionSupported(plan->unicodeVersion)) {
                ++installRefusals_;
                noteRefused_(plan->viewObjectId, rune::view::verdictPlanFingerprint(m.planBlob));
                const auto part = [](uint32_t v, int shift) { return (v >> shift) & 0xFFu; };
                spdlog::critical("VIEW-UNICODE-UNSUPPORTED view={} unicode={}.{}.{} — this broker has {}.{}.{}; the view is not installed",
                                 m.viewTopicId, part(plan->unicodeVersion, 16), part(plan->unicodeVersion, 8), part(plan->unicodeVersion, 0),
                                 part(rune::codec::kUnicodeVersion, 16), part(rune::codec::kUnicodeVersion, 8), part(rune::codec::kUnicodeVersion, 0));
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
                noteRefused_(plan->viewObjectId, rune::view::verdictPlanFingerprint(m.planBlob));
                return;
            }
            rune::view::RowLayout layout;
            try {
                layout = rune::view::buildRowLayout(outDesc);
            } catch (const std::exception& ex) {
                spdlog::error("WorkerViewSet: install for viewTopicId={} dropped — "
                              "buildRowLayout failed: {}", m.viewTopicId, ex.what());
                noteRefused_(plan->viewObjectId, rune::view::verdictPlanFingerprint(m.planBlob));
                return;
            }
            const int32_t viewTopicId = plan->viewTopicId;
            const std::optional<rune::view::VerdictViewKind> kind = rune::view::verdictKindOf(*plan);
            const uint64_t objectId = plan->viewObjectId;
            installView(std::move(*plan), std::move(layout), std::move(srcDesc),
                        /*persisted=*/m.persistence == 0, /*underRecovery=*/m.recovery);
            if (kind) {   // a unique or check view: its source's judge drives it
                installsSinceMark_ = true;
                ViewTemplate& tpl = templates_.at(viewTopicId);
                ++verdictEpoch_;
                tpl.isVerdict = true;
                tpl.verdictKind = *kind;
                tpl.verdictObjectId = objectId;
                tpl.verdictFingerprint = rune::view::verdictPlanFingerprint(m.planBlob);
                tpl.planRecordPayload.resize(rune::storage::control_record::verdictPlanPayloadBytes(m.planBlob.size(), m.outDescBlob.size(), m.srcDescBlob.size()));
                if (rune::storage::control_record::writeVerdictPlanPayload(tpl.planRecordPayload, {objectId, tpl.verdictFingerprint, static_cast<uint8_t>(*kind)}, m.planBlob, m.outDescBlob, m.srcDescBlob) == 0)
                    tpl.planRecordPayload.clear();   // over the cap: AdminWorker refuses such a view (P9-9)
            }
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

int WorkerViewSet::drainSnapshotAcks(int limit) {
    // A legacy-superseded frame the ring had no room for: retried here (cold, empty almost always).
    while (!legacyOwed_.empty()) {
        const uint64_t key = legacyOwed_.back();
        if (!tryPushLegacySuperseded_(static_cast<int32_t>(static_cast<uint32_t>(key >> 32)), static_cast<int32_t>(static_cast<uint32_t>(key)))) break;
        legacyOwed_.pop_back();
    }
    return channel_.done.drain(
        [this](int32_t type, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            if (type == ViewSnapshotChannel::kDoneMsgType && len >= static_cast<aeron::util::index_t>(ViewSnapshotChannel::kDoneMsgBytes)) {
                // A discard in flight: the ack is the old stream's (the agent acks in order, its discard last).
                if (const auto it = views_.find(instKey(static_cast<int32_t>(readU32LE(p)), static_cast<int32_t>(readU32LE(p + 4)))); it != views_.end() && checkpointSettled(it->second))
                    it->second.lastDurableO = std::max(it->second.lastDurableO, static_cast<int64_t>(readU32LE(p + 8) | (uint64_t{readU32LE(p + 12)} << 32)));
                return;
            }
            if (type != kDiscardDoneMsgType || len < kDiscardDoneMsgBytes) return;
            onDiscardDone_(static_cast<int32_t>(readU32LE(p)), static_cast<int32_t>(readU32LE(p + 4)),
                           static_cast<int32_t>(readU32LE(p + 8)), static_cast<ViewDiscardReason>(p[12]));
        },
        limit);
}

void WorkerViewSet::onDiscardDone_(int32_t viewTopicId, int32_t partition, int32_t status, ViewDiscardReason reason) {
    const auto it = views_.find(instKey(viewTopicId, partition));
    if (it == views_.end()) return;
    ViewInstance& inst = it->second;
    if (reason == ViewDiscardReason::LegacySuperseded) {
        // Fire and forget, and possibly in flight next to a real discard of this instance: it never touches CheckpointState.
        if (status != 0) {
            ++inst.counters.viewBackupFailures;
            spdlog::warn("VIEW-BACKUP-FAILED view={} partition={} reason=legacy-superseded errno={} ({}) -- the superseded legacy "
                         "checkpoint file stays in place; the committed rows are unaffected",
                         viewTopicId, partition, status, std::strerror(status));
        }
        return;
    }
    const auto* pending = std::get_if<CheckpointDiscardPending>(&inst.checkpoint);
    if (pending == nullptr) {               // a fire-and-forget discard (legacy file next to a committed one) or a stale ack
        if (status != 0)
            spdlog::warn("WorkerViewSet: view {} partition {}: discard of a superseded checkpoint file failed: {}", viewTopicId, partition,
                         std::strerror(status));
        return;
    }
    if (status == 0) {
        inst.checkpoint = CheckpointLive{};  // foldCursor stays 0: the rebuild starts from the log
        return;
    }
    ++inst.counters.viewBackupFailures;
    spdlog::error("VIEW-BACKUP-FAILED view={} partition={} reason={} errno={} ({}) -- the view stays NotReady; fix the disk or "
                  "permissions and restart, or set RUNE_VIEW_BACKUP=0 to accept the loss of the old checkpoint",
                  viewTopicId, partition, viewDiscardReasonName(pending->reason), status, std::strerror(status));
    inst.checkpoint = CheckpointDiscardFailed{pending->reason, status};
}

void WorkerViewSet::materializeRecoveryInstancesFor(int32_t srcTopicId, int32_t partition,
                                                    int64_t committedTail, const SourceLogBounds& b) {
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
        const RecoveredCheckpoint rc = readCheckpointFile(viewDataDir_, viewTopicId, partition);
        const ViewCheckpointScan scan = scanViewCheckpoint(std::span<const uint8_t>(rc.bytes));
        const std::vector<std::span<const uint8_t>>& records = scan.records;

        const ViewTemplate& tpl = tit->second;
        std::size_t applied = 0;
        installViewForRecovery(tpl.plan, tpl.layout, tpl.evolvingSrc, tpl.persisted, partition,
                               /*base=*/std::span<const uint8_t>{},
                               std::span<const std::span<const uint8_t>>(records.data(), records.size()),
                               committedTail, &applied);
        if (const auto vit = views_.find(instKey(viewTopicId, partition)); vit != views_.end())
            applyRecoveredCheckpoint_(vit->second, rc, scan, applied, b);
    }
}

int WorkerViewSet::reconcileTransient_(ViewInstance& inst, const TransientBounds& tb) {
    const int32_t viewTopicId = inst.plan.viewTopicId;
    if (std::holds_alternative<CursorFresh>(inst.transientCursor)) {             // new instance: start at logStart
        inst.transientCursor = CursorLive{tb.incarnation, tb.logGeneration};
        if (inst.foldCursor == tb.logStart) return 0;
        inst.foldCursor = tb.logStart;
        return 1;
    }
    if (std::holds_alternative<CursorRecoveredUnreconciled>(inst.transientCursor)) {   // older life of the source
        ++inst.counters.transientSourceResets;
        spdlog::info("WorkerViewSet: view {} partition {}: recovered checkpoint cursor {} is from an earlier life of its "
                     "transient source (now [{}, {})); keeping the checkpoint rows, re-folding from offset {} (resets={})",
                     viewTopicId, inst.partition, inst.foldCursor, tb.logStart, tb.logEnd, tb.logStart,
                     inst.counters.transientSourceResets);
        inst.transientCursor = CursorLive{tb.incarnation, tb.logGeneration};
        inst.foldCursor      = tb.logStart;
        return 1;
    }
    CursorLive& live = std::get<CursorLive>(inst.transientCursor);
    if (live.incarnation != tb.incarnation) [[unlikely]] {                      // SOURCE_RECREATED
        // A NEW replica + ring for this partition on this node: its offsets may
        // name different records.  Keep the rows (re-folding retained records is
        // idempotent under the keyed table semantics), re-fold from logStart.
        ++inst.counters.transientSourceResets;
        spdlog::warn("WorkerViewSet: view {} partition {}: its transient source was recreated "
                     "(replica incarnation {} -> {}) under cursor {}; keeping the rows, re-folding "
                     "from offset {} (resets={})",
                     viewTopicId, inst.partition, live.incarnation, tb.incarnation, inst.foldCursor,
                     tb.logStart, inst.counters.transientSourceResets);
        live            = CursorLive{tb.incarnation, tb.logGeneration};
        inst.foldCursor = tb.logStart;
        return 1;
    }
    int moved = 0;
    if (live.logGeneration != tb.logGeneration) [[unlikely]] {                  // SOURCE_CUT
        // The ring's existing content changed at/above cutOffset (a truncation,
        // or a follower ring reset).  Records the view folded at/above the cut
        // may differ or be gone: re-fold from the cut (or the ring start).
        // The replica keeps only the LAST cut: when the generation moved by
        // more than one since this pass, an earlier cut may lie below it, so
        // the cut is unknown — re-fold from the ring start (the same
        // exactly-one-cut guard as Worker::waiterReplica_ and
        // ClientGateway::inlineWaiterStillValid_).
        const bool oneCut   = tb.logGeneration == live.logGeneration + 1;
        const int64_t cut   = oneCut ? tb.cutOffset : tb.logStart;
        live.logGeneration = tb.logGeneration;
        if (inst.foldCursor > cut) {
            ++inst.counters.transientSourceResets;
            const int64_t to = std::max(cut, tb.logStart);
            spdlog::warn("WorkerViewSet: view {} partition {}: its transient source changed at offset {} "
                         "below cursor {} ({} cut since the last pass); keeping the rows, re-folding from "
                         "offset {} (resets={})",
                         viewTopicId, inst.partition, cut, inst.foldCursor,
                         oneCut ? "one" : "more than one", to, inst.counters.transientSourceResets);
            inst.foldCursor = to;
            moved = 1;
        }
    }
    if (inst.foldCursor > tb.logEnd) {                                          // SOURCE RESTARTED
        ++inst.counters.transientSourceResets;
        spdlog::info("WorkerViewSet: view {} partition {}: cursor {} is above its transient source end {} (the source "
                     "restarted under it); keeping the rows, re-folding from offset {} (resets={})",
                     viewTopicId, inst.partition, inst.foldCursor, tb.logEnd, tb.logStart,
                     inst.counters.transientSourceResets);
        inst.foldCursor = tb.logStart;
        return 1;
    }
    if (inst.foldCursor < tb.logStart) {                                        // EVICTED
        inst.counters.transientRecordsEvicted += static_cast<uint64_t>(tb.logStart - inst.foldCursor);
        inst.foldCursor = tb.logStart;
        return 1;
    }
    return moved;
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

    // A Keyed plan's table takes the key's width and prefix; every table of every instance takes the worker's hash seed.
    rune::view::ViewTableKeys keys{8, 0, hashSeed_, hashMask_};
    std::optional<rune::view::KeyedFoldSpec> keyed;
    if (rune::view::keyModeOf(tpl.plan) == rune::view::ViewKeyMode::Keyed) {
        keyed = rune::view::buildKeyedFoldSpec(tpl.plan, tpl.evolvingSrc);
        if (!keyed) {
            spdlog::error("WorkerViewSet: keyed view {} partition {} not hosted — a key column is missing from its source descriptor",
                          viewTopicId, partition);
            return nullptr;
        }
        keys.keyWidth    = static_cast<uint8_t>(keyed->keyWidth);
        keys.prefixWidth = static_cast<uint8_t>(keyed->prefixWidth);
    }

    auto [ins, inserted] = views_.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(tpl.plan, tpl.layout, tpl.evolvingSrc, tpl.persisted,
                              partition, indexSlots, chunkBytes, keys));
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
        inst.agg.emplace(std::move(*al), indexSlots, chunkBytes, inst.tableKeys);
    }
    if (keyed) {   // after the aggregate tables: a keyed aggregate's prospect holds their rows
        inst.keyed = std::move(keyed);
        buildKeyedTables(inst, indexSlots, chunkBytes);
    }

    // Persisted: bound each fold pass so its checkpoint delta always fits one
    // delta-ring message (an oversized claim THROWS on this worker thread).
    if (inst.persisted) {
        inst.maxRecordsPerPass = maxRecordsPerPassFor(
            inst, static_cast<std::size_t>(channel_.deltas.maxMsgLength()));
        if (inst.maxRecordsPerPass < 1) {
            spdlog::error("WorkerViewSet: persisted view {} partition {} not hosted — one source "
                          "record's worst-case checkpoint delta exceeds the delta ring's max "
                          "message length ({} bytes)",
                          viewTopicId, partition, channel_.deltas.maxMsgLength());
            views_.erase(ins);
            return nullptr;
        }
    }
    if (passCapForTest_ > 0) inst.maxRecordsPerPass = passCapForTest_;

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
    const RecoveredCheckpoint rc = readCheckpointFile(viewDataDir_, viewTopicId, partition);
    const ViewCheckpointScan scan = scanViewCheckpoint(std::span<const uint8_t>(rc.bytes));
    const std::vector<std::span<const uint8_t>>& records = scan.records;

    std::size_t applied = 0;
    const int64_t recoveredO = replayInstance(
        inst, /*base=*/std::span<const uint8_t>{},
        std::span<const std::span<const uint8_t>>(records.data(), records.size()), applied);
    logRecovered(inst, recoveredO, applied);
    discardDirty_(inst);  // restored rows == the durable checkpoint: nothing to re-send
    inst.foldCursor   = std::max<int64_t>(recoveredO, 0);
    if (recoveredO >= 0) inst.transientCursor = CursorRecoveredUnreconciled{};
    inst.lastEncodedO = recoveredO;  // the durable offset we recovered from (may be -1)
    inst.lastDurableO = recoveredO;
    inst.readiness = (inst.foldCursor >= committedTail) ? ViewReadiness::Ready
                                                        : ViewReadiness::RebuildingViews;
    applyRecoveredCheckpoint_(inst, rc, scan, applied, SourceLogBounds{});   // bounds unknown here: the first driveView_ pass repeats the check
}

void WorkerViewSet::applyRecoveredCheckpoint_(ViewInstance& inst, const RecoveredCheckpoint& rc, const ViewCheckpointScan& scan, std::size_t applied,
                                              const SourceLogBounds& b) {
    // The committed file's class (C7).  A torn tail is repaired by the IO agent before its first append (the worker never
    // writes the file) and removes nothing recovery could decode; a corrupt file is backed up and rebuilt.
    CheckpointOrigin origin = rc.origin;
    bool corrupt = false;
    if (const auto* torn = std::get_if<ViewCheckpointTornTail>(&scan.tail)) {
        spdlog::info("VIEW-CHECKPOINT-TORN-TAIL view={} partition={} cleanLen={} dropped={}", inst.plan.viewTopicId, inst.partition,
                     torn->cleanLen, rc.bytes.size() - torn->cleanLen);
        if (scan.records.empty()) {   // nothing but a torn first record: as if there were no committed file
            if (origin == CheckpointOrigin::Committed) origin = CheckpointOrigin::None;
            else if (origin == CheckpointOrigin::CommittedAndLegacy) origin = CheckpointOrigin::LegacyOnly;
        }
    } else if (std::holds_alternative<ViewCheckpointCorrupt>(scan.tail)) {
        corrupt = true;
    }
    switch (origin) {
        case CheckpointOrigin::None: return;
        case CheckpointOrigin::LegacyOnly:   // the legacy format is never loaded: back it up and rebuild from the log
            beginDiscard_(inst, ViewDiscardReason::Legacy);
            return;
        case CheckpointOrigin::CommittedAndLegacy:   // the committed file is the truth; the superseded legacy one is only backed up
            if (!tryPushLegacySuperseded_(inst.plan.viewTopicId, inst.partition))
                legacyOwed_.push_back(instKey(inst.plan.viewTopicId, inst.partition));
            [[fallthrough]];
        case CheckpointOrigin::Committed:
            // Unreadable: a record failed its CRC / framing, or the replay stopped before the last verified record.
            if (corrupt || applied < scan.records.size()) {
                beginDiscard_(inst, ViewDiscardReason::Unreadable);
            } else if (inst.foldCursor > b.logEnd) {   // #159: rows folded from records the source log no longer holds
                ++inst.counters.checkpointsAheadOfLog;
                beginDiscard_(inst, ViewDiscardReason::AheadOfLog);
            }
            return;
    }
}

bool WorkerViewSet::tryPushLegacySuperseded_(int32_t viewTopicId, int32_t partition) {
    const auto claim = channel_.deltas.tryClaim(kDiscardMsgType, static_cast<std::size_t>(kDiscardMsgBytes));
    if (claim < 0) return false;
    uint8_t* dst = channel_.deltas.base() + claim;
    writeI32LE(dst, viewTopicId);
    writeI32LE(dst + 4, partition);
    dst[8] = static_cast<uint8_t>(ViewDiscardReason::LegacySuperseded);
    dst[9] = dst[10] = dst[11] = 0;
    channel_.deltas.commit(claim);
    return true;
}

void WorkerViewSet::installViewForRecovery(
    rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
    rune::codec::Descriptor evolvingSrc, bool persisted, int32_t partition,
    std::span<const uint8_t> base,
    std::span<const std::span<const uint8_t>> deltasInOrder, int64_t committedTail, std::size_t* appliedOut) {
    const int32_t viewTopicId = plan.viewTopicId;

    // Register the plan template + reactor index exactly as the normal path
    // does (moves plan/layout/evolvingSrc into templates_) when none exists yet.
    // underRecovery is deliberately FALSE here (cleared on an existing
    // template): this function constructs + loads
    // THIS (view, partition) instance explicitly below, from the CALLER-
    // supplied base/deltasInOrder (which may be synthetic test spans, not
    // necessarily this worker's on-disk file) — latching underRecovery=true
    // before the getOrCreate_ below would make it redundantly re-read +
    // re-apply viewDataDir_'s on-disk file for this SAME instance via
    // loadRecoveryCheckpoint_.  The latch is set true right after (below),
    // so it is in place before this call returns.
    // An existing template is kept as it is: re-registering it would wipe what drainInstalls learned of a unique or check
    // view no judge has attached yet (a candidate still building at a restart), and the member would leave the set.
    if (const auto tit = templates_.find(viewTopicId); tit != templates_.end())
        tit->second.underRecovery = false;
    else
        installView(std::move(plan), std::move(layout), std::move(evolvingSrc), persisted);

    // Eagerly build the (view, partition) instance from the template (recovery is partition-specific; it must not wait for a lazy
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
    std::size_t applied = 0;
    const int64_t recoveredO = replayInstance(*inst, base, deltasInOrder, applied);
    if (appliedOut != nullptr) *appliedOut = applied;
    logRecovered(*inst, recoveredO, applied);
    discardDirty_(*inst);  // restored rows == the durable checkpoint: nothing to re-send
    inst->foldCursor   = std::max<int64_t>(recoveredO, 0);
    if (recoveredO >= 0) inst->transientCursor = CursorRecoveredUnreconciled{};
    inst->lastEncodedO = recoveredO;  // the durable offset we recovered from (may be -1)
    inst->lastDurableO = recoveredO;

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
        if (kv.second.readiness == ViewReadiness::Recovering ||
            kv.second.readiness == ViewReadiness::RebuildingViews) return;  // still rebuilding somewhere
    }
    recoveryViews_.erase(viewTopicId);
}

ViewReadiness WorkerViewSet::readiness(int32_t viewTopicId, int32_t partition) const {
    auto it = views_.find(instKey(viewTopicId, partition));
    if (it == views_.end()) return ViewReadiness::Ready;  // nothing hosted => nothing to gate
    return it->second.readiness;
}

const ViewInstance* WorkerViewSet::serveInstance_(int32_t viewTopicId, int32_t partition,
                                                  const rune::view::RowLayout*& outLayout, RowStatus* outStatus) const {
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
    // Task 12 (D26): a view paused over its namespace's memory limit answers Paused (never a stale row).
    if (std::holds_alternative<ViewPaused>(it->second.pause)) {
        if (outStatus != nullptr) *outStatus = RowStatus::Paused;
        return nullptr;
    }
    // SERVE-GUARD: a view still rebuilding after a restart must NEVER serve a
    // half-built row — surface a RETRIABLE not-ready instead (Review-focus #5).
    if (it->second.readiness != ViewReadiness::Ready) {
        if (outStatus != nullptr) *outStatus = RowStatus::NotReady;
        return nullptr;
    }
    return &it->second;
}

const uint8_t* WorkerViewSet::getRow(int32_t viewTopicId, int32_t partition, uint64_t key,
                                     const rune::view::RowLayout*& outLayout,
                                     RowStatus* outStatus) const {
    const ViewInstance* inst = serveInstance_(viewTopicId, partition, outLayout, outStatus);
    if (inst == nullptr) return nullptr;
    // P7-10: only a LegacyU64 view with an 8-byte key can be named by a u64.
    if (inst->keyed || inst->plan.keyWidth != 8) {
        if (outStatus != nullptr) *outStatus = RowStatus::BadKey;
        return nullptr;
    }
    const uint8_t* row = inst->table.get(key);
    if (outStatus != nullptr) *outStatus = (row != nullptr) ? RowStatus::Ok : RowStatus::NotFound;
    return row;
}

const uint8_t* WorkerViewSet::getRowK(int32_t viewTopicId, int32_t partition, std::span<const uint8_t> key,
                                      const rune::view::RowLayout*& outLayout, RowStatus* outStatus) const {
    const ViewInstance* inst = serveInstance_(viewTopicId, partition, outLayout, outStatus);
    if (inst == nullptr) return nullptr;
    const uint8_t* row = nullptr;
    if (inst->keyed) {
        if (key.size() != inst->keyed->keyWidth) {
            if (outStatus != nullptr) *outStatus = RowStatus::BadKey;
            return nullptr;
        }
        row = inst->table.get(rune::view::ViewKey::fromBytes(key));
    } else {
        // LegacyU64: the one column in its view-key encoding, back to the u64 the table is keyed by.
        const rune::codec::PrimType type = inst->plan.keyComponents.empty() ? rune::view::viewKeyType(inst->plan) : inst->plan.keyComponents[0].type;
        if (key.size() != rune::codec::fixedSize(type)) {
            if (outStatus != nullptr) *outStatus = RowStatus::BadKey;
            return nullptr;
        }
        const rune::codec::FieldValue v = rune::codec::decodeKeyComponent(type, 0, key);
        const bool isSigned = type == rune::codec::PrimType::Int32 || type == rune::codec::PrimType::Int64;
        row = inst->table.get(isSigned ? static_cast<uint64_t>(v.i) : v.u);
    }
    if (outStatus != nullptr) *outStatus = (row != nullptr) ? RowStatus::Ok : RowStatus::NotFound;
    return row;
}

WorkerViewSet::ScanPage WorkerViewSet::scanPage(int32_t viewTopicId, int32_t partition, std::span<const uint8_t> prefix,
                                                std::span<const uint8_t> resumeToken, uint16_t limit, std::span<uint8_t> out) {
    using Status = rune::protocol::client::ViewRowResponse;
    ScanPage page;
    const auto tit = templates_.find(viewTopicId);
    if (tit == templates_.end()) {
        page.status = Status::kUnknownView;
        return page;
    }
    const rune::view::CompiledViewPlan& plan = tit->second.plan;
    const uint16_t prefixWidth = rune::view::prefixWidthOf(plan);
    if (rune::view::keyModeOf(plan) != rune::view::ViewKeyMode::Keyed || prefixWidth == 0 || prefix.size() != prefixWidth ||
        (!resumeToken.empty() && resumeToken.size() != rune::view::kScanTokenBytes)) {
        page.status = Status::kInvalidRequest;
        return page;
    }
    const rune::view::RowLayout* layout = nullptr;
    RowStatus st{};
    if (serveInstance_(viewTopicId, partition, layout, &st) == nullptr) {
        // As GetViewRowK: an instance this broker does not host is not an empty page.
        page.status = st != RowStatus::NotFound ? Status::kNotReady : Status::kAbsentKey;
        return page;
    }
    ViewInstance& inst = views_.find(instKey(viewTopicId, partition))->second;
    const uint32_t rowLen = layout->rowLen;
    const std::size_t budget = std::min<std::size_t>(out.size(), kMaxScanPageBytes);
    const uint32_t maxRows = limit == 0 || limit > 256 ? 256 : limit;
    std::size_t used = 0;
    bool tooLarge = false;
    const rune::view::PrefixScan scan = inst.table.scanPrefix(
        prefix, resumeToken, std::span<uint8_t, rune::view::kScanTokenBytes>(page.nextToken),
        [&](std::span<const uint8_t>, const uint8_t* payload) noexcept {
            if (page.rows == maxRows) return false;
            if (used + 4 + rowLen > budget) {
                tooLarge = page.rows == 0;
                return false;
            }
            std::memcpy(out.data() + used, &rowLen, 4);
            std::memcpy(out.data() + used + 4, payload, rowLen);
            used += 4 + rowLen;
            ++page.rows;
            return true;
        });
    page.bytes = static_cast<uint32_t>(used);
    if (scan == rune::view::PrefixScan::CursorGone) {
        page.status = Status::kCursorInvalidated;
        return page;
    }
    if (tooLarge) {
        page.status = Status::kRowTooLarge;
        return page;
    }
    page.status       = Status::kOk;
    page.nextTokenLen = scan == rune::view::PrefixScan::Stopped ? rune::view::kScanTokenBytes : 0;
    ++inst.counters.scanPages;
    inst.counters.scanRows += page.rows;
    return page;
}


int WorkerViewSet::buildAndPushKeyedCheckpoint_(ViewInstance& inst) {
    keyedEntries_.clear();
    rowOffsets_.clear();
    rowScratch_.clear();
    keyScratch_.clear();

    // As buildAndPushCheckpoint_: drainDirtyKeyed's key and row views die with the callback, so both are copied.
    auto drainInto = [&](rune::view::ViewTable& t, uint32_t rowLen) {
        t.drainDirtyKeyed([&](uint32_t rid, const uint8_t* row, bool deleted, std::span<const uint8_t> key) {
            rowOffsets_.push_back(deleted ? SIZE_MAX : rowScratch_.size());
            if (!deleted) rowScratch_.insert(rowScratch_.end(), row, row + rowLen);
            keyScratch_.insert(keyScratch_.end(), key.begin(), key.end());
            keyedEntries_.push_back(rune::view::KeyedDirtyEntry{rid, deleted, {}, nullptr});
        });
    };
    // The record's tables in id order: the view, then the owner map (a projection) or contributors, groups, blocks and groupIds.
    struct Part { rune::view::ViewTable* table; uint8_t id; uint8_t keyLen; uint32_t rowLen; };
    Part parts[7];
    std::size_t nParts = 0;
    const rune::view::KeyedFoldSpec& k = *inst.keyed;
    if (inst.agg) {
        AggInstance& a = *inst.agg;
        const uint8_t gk = static_cast<uint8_t>(a.layout.groupKeyWidth);
        parts[nParts++] = {&inst.table, rune::view::kKeyedTableView, gk, inst.layout.rowLen};
        parts[nParts++] = {&a.contributors, rune::view::kKeyedTableContributors, 8, a.layout.contributorRow.rowLen};
        parts[nParts++] = {&a.groups, rune::view::kKeyedTableGroups, 8, a.layout.groupRow.rowLen};
        if (a.blocks) parts[nParts++] = {&*a.blocks, rune::view::kKeyedTableBlocks, 8, a.layout.blockRow.rowLen};
        parts[nParts++] = {&*a.groupIds, rune::view::kKeyedTableGroupIds, gk, 8};
    } else {
        parts[nParts++] = {&inst.table, rune::view::kKeyedTableView, static_cast<uint8_t>(k.keyWidth), inst.layout.rowLen};
        if (inst.owners) parts[nParts++] = {&*inst.owners, rune::view::kKeyedTableOwners, k.wireKeyWidth, k.keyWidth};
        if (inst.claims) {   // in the same record as tables 0 and 1: a recovery never sees the rows without their claims
            const ClaimsRowLens c = claimsRowLensOf(inst);
            parts[nParts++] = {&inst.claims->indexTable(), rune::view::kKeyedTableUniqueIndex, static_cast<uint8_t>(k.keyWidth), c.index};
            parts[nParts++] = {&inst.claims->extrasTable(), rune::view::kKeyedTableUniqueExtras, k.wireKeyWidth, c.extras};
        }
    }
    std::size_t b[8] = {};
    for (std::size_t t = 0; t < nParts; ++t) {
        drainInto(*parts[t].table, parts[t].rowLen);
        b[t + 1] = keyedEntries_.size();
    }
    if (keyedEntries_.empty()) return 0;

    std::size_t keyPos = 0;
    for (std::size_t t = 0; t < nParts; ++t) {
        for (std::size_t i = b[t]; i < b[t + 1]; ++i) {
            keyedEntries_[i].key = std::span<const uint8_t>(keyScratch_.data() + keyPos, parts[t].keyLen);
            keyPos += parts[t].keyLen;
            if (rowOffsets_[i] != SIZE_MAX) keyedEntries_[i].row = rowScratch_.data() + rowOffsets_[i];
        }
    }
    rune::view::KeyedTableDelta tds[7];
    for (std::size_t t = 0; t < nParts; ++t) {
        tds[t] = {parts[t].id, parts[t].keyLen, parts[t].rowLen, std::span<const rune::view::KeyedDirtyEntry>(keyedEntries_.data() + b[t], b[t + 1] - b[t])};
    }
    inst.pendingDelta.clear();
    rune::view::encodeKeyedDelta(std::span<const rune::view::KeyedTableDelta>(tds, nParts), inst.foldCursor, inst.pendingDelta);
    inst.lastEncodedO = inst.foldCursor;
    inst.pendingValid = true;
    (void)tryPushDelta_(inst);
    return 1;
}

int WorkerViewSet::buildAndPushCheckpoint_(ViewInstance& inst) {
    if (!checkpointSettled(inst)) return 0;   // belt and braces: driveView_ step 0a already returned
    // P11-0: no checkpoint inside an open span (a refold from it would never see the span's records below it) or an application in
    // progress.  The dirty rows wait for the first cursor outside every span.
    if (inst.spanFence.fenced() || inst.spanFence.straddles(inst.foldCursor)) [[unlikely]] {
        if (viewTablesDirty(inst)) ++inst.counters.vetoedCheckpointPushes;   // a push refused, not a pass with nothing to push
        return 0;
    }
    if (inst.keyed) return buildAndPushKeyedCheckpoint_(inst);
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
        // ONE v2 record (+ Min/Max value blocks), so recovery always restores every table at the same O.
        std::size_t b[5];
        b[0] = 0;
        drainInto(inst.table, inst.layout.rowLen);
        b[1] = dirtyEntries_.size();
        drainInto(inst.agg->contributors, inst.agg->layout.contributorRow.rowLen);
        b[2] = dirtyEntries_.size();
        drainInto(inst.agg->groups, inst.agg->layout.groupRow.rowLen);
        b[3] = dirtyEntries_.size();
        if (inst.agg->blocks) drainInto(*inst.agg->blocks, inst.agg->layout.blockRow.rowLen);
        b[4] = dirtyEntries_.size();
        if (dirtyEntries_.empty()) return 0;
        fixup();
        auto sub = [&](int i) {
            return std::span<const rune::view::DirtyEntry>(dirtyEntries_.data() + b[i],
                                                          b[i + 1] - b[i]);
        };
        const rune::view::TableDelta tds[4] = {
            {rune::view::kAggTableView, inst.layout.rowLen, sub(0)},
            {rune::view::kAggTableContributors, inst.agg->layout.contributorRow.rowLen, sub(1)},
            {rune::view::kAggTableGroups, inst.agg->layout.groupRow.rowLen, sub(2)},
            {rune::view::kAggTableBlocks, inst.agg->layout.blockRow.rowLen, sub(3)}};
        const std::size_t nTables = inst.agg->blocks ? 4 : 3;   // 2a / Last-only: unchanged 3-table bytes
        rune::view::encodeMultiDelta(std::span<const rune::view::TableDelta>(tds, nTables),
                                     inst.foldCursor, inst.pendingDelta);
    }
    inst.lastEncodedO = inst.foldCursor;  // attempted/encoded offset, NOT durable
    inst.pendingValid = true;
    (void)tryPushDelta_(inst);            // on back-pressure the delta is held for next pass
    return 1;                             // a delta was encoded this pass (real work)
}

bool WorkerViewSet::tryPushDelta_(ViewInstance& inst) {
    const std::size_t frameLen =
        ViewSnapshotChannel::kDeltaHeaderBytes + inst.pendingDelta.size();
    if (inst.pendingSent != 0 || frameLen > static_cast<std::size_t>(channel_.deltas.maxMsgLength())) [[unlikely]] return tryPushDeltaParts_(inst);

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

// P11-9: a delta past one ring message (a claim that large throws), as part frames; on back-pressure the rest goes next pass.
bool WorkerViewSet::tryPushDeltaParts_(ViewInstance& inst) {
    const std::size_t room = static_cast<std::size_t>(channel_.deltas.maxMsgLength()) - ViewSnapshotChannel::kDeltaPartHeaderBytes;
    while (inst.pendingSent < inst.pendingDelta.size()) {
        const std::size_t n = std::min(room, inst.pendingDelta.size() - inst.pendingSent);
        const auto claim = channel_.deltas.tryClaim(ViewSnapshotChannel::kDeltaPartMsgType, ViewSnapshotChannel::kDeltaPartHeaderBytes + n);
        if (claim < 0) return false;   // ring full: the run resumes here next pass
        uint8_t* dst = channel_.deltas.base() + claim;
        writeI32LE(dst, inst.plan.viewTopicId);
        writeI32LE(dst + 4, inst.partition);
        writeI32LE(dst + 8, static_cast<int32_t>(inst.pendingSent));
        dst[12] = inst.pendingSent + n < inst.pendingDelta.size() ? 1 : 0;
        dst[13] = dst[14] = dst[15] = 0;
        std::memcpy(dst + ViewSnapshotChannel::kDeltaPartHeaderBytes, inst.pendingDelta.data() + inst.pendingSent, n);
        channel_.deltas.commit(claim);
        inst.pendingSent += n;
    }
    inst.pendingSent  = 0;
    inst.pendingValid = false;
    return true;
}

void WorkerViewSet::installConstrainedViewForTest(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout, rune::codec::Descriptor src, bool persisted) {
    const int32_t viewTopicId = plan.viewTopicId;
    const rune::view::VerdictViewKind kind = *rune::view::verdictKindOf(plan);
    const uint64_t objectId = plan.viewObjectId;
    const uint64_t fingerprint = rune::view::verdictPlanFingerprint(rune::view::serialize(plan));
    installView(std::move(plan), std::move(layout), std::move(src), persisted);
    ViewTemplate& tpl = templates_.at(viewTopicId);
    ++verdictEpoch_;
    tpl.isVerdict = true;
    tpl.verdictKind = kind;
    tpl.verdictObjectId = objectId;
    tpl.verdictFingerprint = fingerprint;
}

void WorkerViewSet::installTestVerdictViewForTest(rune::view::CompiledViewPlan plan, rune::view::RowLayout layout,
                                                  rune::codec::Descriptor src, bool persisted, uint64_t viewObjectId, TestRejectRule rule, bool underRecovery) {
    const int32_t viewTopicId = plan.viewTopicId;
    installView(std::move(plan), std::move(layout), std::move(src), persisted, underRecovery);
    ViewTemplate& tpl = templates_.at(viewTopicId);
    ++verdictEpoch_;
    tpl.isVerdict = true;
    tpl.verdictKind = rune::view::VerdictViewKind::TestReject;
    tpl.verdictObjectId = viewObjectId;
    tpl.verdictFingerprint = rule.fingerprint();
    tpl.testRule = rule;
}

void WorkerViewSet::dropSourceTopic(int32_t srcTopicId) {
    const auto ofTopic = [&](const auto& kv) { return static_cast<int32_t>(kv.first >> 32) == srcTopicId; };
    std::erase_if(verdictSources_, [&](const auto& kv) {
        if (!ofTopic(kv)) return false;
        releaseJudge_(*kv.second);
        if (PartitionWriteState* s = kv.second->boundState()) s->verdicts = nullptr;   // the state may outlive its judge
        return true;
    });
    for (auto it = keyIndexes_.begin(); it != keyIndexes_.end();) {
        if (!ofTopic(*it)) { ++it; continue; }
        releaseMem_(it->second);
        it = keyIndexes_.erase(it);
    }
}

// Cold.  The key index is built over the source's evolving schema, as every view is: the judge decodes each writer version through it.
// nullptr until the schema has landed on this worker, or when the key cannot be indexed (the topic has no key).
static const rune::codec::Descriptor* keyIndexSource(const TopicWriteSpec& spec) {
    if (!spec.evolving || spec.keyWidth == 0) return nullptr;
    const bool hasKey = std::any_of(spec.evolving->fields.begin(), spec.evolving->fields.end(), [&](const auto& f) { return f.id == spec.keyFieldId; });
    return hasKey ? &*spec.evolving : nullptr;
}

// A key index no leadership has activated yet: its first activation keeps it (it holds nothing), a later one rebuilds it.
static constexpr uint64_t kNeverActivated = UINT64_MAX;
// A key index adopted under an active leadership: it folds that leadership's writes, so the next activation rebuilds it.
static constexpr uint64_t kAdoptedLive = UINT64_MAX - 1;

static ViewInstance freshKeyIndex(int32_t srcTopicId, int32_t partition, const TopicWriteSpec& spec, const rune::codec::Descriptor& src,
                                  rune::view::ViewTableKeys keys) {
    ViewInstance inst(keyIndexPlan(srcTopicId, spec.keyFieldId), keyIndexRowLayout(), src, /*persist=*/false,
                      partition, /*indexSlots=*/4096, kKeyIndexChunkBytes, keys);
    inst.maxRecordsPerPass = kKeyIndexReplayRecordsPerPass;
    inst.foldLeadership = kNeverActivated;
    return inst;
}

// A new leadership starts from the local log: an instance carried over could hold what a follower-time compaction removed.
void WorkerViewSet::rebuildKeyIndex_(int32_t srcTopicId, int32_t partition, VerdictSource& judge, const TopicWriteSpec& spec, uint64_t leaderGen) {
    const auto it = keyIndexes_.find(instKey(srcTopicId, partition));
    if (it == keyIndexes_.end() || it->second.foldLeadership == leaderGen) return;   // already fresh for this leadership (the judge may be held)
    if (it->second.foldLeadership != kNeverActivated) {
        const rune::codec::Descriptor* src = keyIndexSource(spec);
        if (src == nullptr) {   // the spec can no longer be indexed: nothing stale stays behind
            judge.removeVerdictView(it->second.plan.viewTopicId);
            eraseKeyIndex_(it->first);
            return;
        }
        releaseMem_(it->second);
        it->second = freshKeyIndex(srcTopicId, partition, spec, *src, rune::view::ViewTableKeys{spec.keyWidth, 0, hashSeed_, hashMask_});
        ++judge.statsMut().keyIndexRebuilds;
    }
    it->second.foldCursor = judge.logStart();
    it->second.foldLeadership = leaderGen;
}

namespace {
/// The set in force at `from`, or a change record above it, names the plan: the judge still judges with it, or will after a switch.
bool namedFrom(const PartitionWriteState& write, int64_t from, uint64_t viewObjectId, uint64_t planFingerprint) {
    const auto names = [&](const ConstraintSet& set) {
        return std::any_of(set.members.begin(), set.members.begin() + set.count, [&](const ConstraintSetMember& m) { return m.viewObjectId == viewObjectId && m.planFingerprint == planFingerprint; });
    };
    if (names(*write.constraintSets.setAt(from))) return true;
    for (int64_t a = write.constraintSets.nextChangeAtOrAfter(from); a != INT64_MAX; a = write.constraintSets.nextChangeAtOrAfter(a + 1))
        if (names(*write.constraintSets.setAt(a + 1))) return true;
    return false;
}
}  // namespace

VerdictSource* WorkerViewSet::ensureVerdictSource(int32_t srcTopicId, int32_t partition, PartitionWriteState& write) {
    const auto sit = subscriptionIndex_.find(srcTopicId);
    bool anyTemplate = false;
    if (sit != subscriptionIndex_.end())
        for (const int32_t id : sit->second) anyTemplate = anyTemplate || isVerdictTemplate_(id);
    const uint64_t key = instKey(srcTopicId, partition);
    auto vit = verdictSources_.find(key);
    if (!anyTemplate && !write.spec->keyIndex && write.constraintSets.latest().empty()) {
        if (vit == verdictSources_.end()) return nullptr;
        // The set was dropped: the judge stays until it has passed the drop and holds no member.
        if (vit->second->hasMembers() || vit->second->judgedThrough() <= write.constraintSets.lastChangeOffset()) {
            installRetainedPlan_(srcTopicId, *vit->second, write);
            return vit->second.get();
        }
        releaseJudge_(*vit->second);
        write.verdicts = nullptr;
        verdictSources_.erase(vit);
        eraseKeyIndex_(key);
        return nullptr;
    }
    if (vit == verdictSources_.end()) {
        auto v = std::make_unique<VerdictSource>(srcTopicId, partition);
        v->setViewHooks(VerdictViewHooks{this, &checkpointHook_, &discardHook_, &retiredHook_});
        v->setVerdictIntervalNs(verdictIntervalNs_);
        v->holdJudgeForTest(holdJudges_);
        v->bindTxn(&txnOverlays_, batchOverlay_, txnJudgedRecordsMax_);
        vit = verdictSources_.emplace(key, std::move(v)).first;
    }
    VerdictSource* judge = vit->second.get();
    const std::size_t membersBefore = judge->memberCountForTest();
    if (txnOverlays_.pending()) [[unlikely]] {
        txnOverlays_.reserveKeys(0);
        txnOverlays_.reserveFor(0, 0);
    }
    judge->bindState(&write);
    write.verdicts = judge;
    if (write.spec->keyIndex && !judge->hasMember(0, kKeyIndexPlanFingerprint)) {
        if (const rune::codec::Descriptor* src = keyIndexSource(*write.spec)) {
            ViewInstance& inst = keyIndexes_.try_emplace(key, freshKeyIndex(srcTopicId, partition, *write.spec, *src, rune::view::ViewTableKeys{write.spec->keyWidth, 0, hashSeed_, hashMask_})).first->second;
            if (judge->activated()) inst.foldLeadership = kAdoptedLive;
            judge->addVerdictView(&inst, rune::view::VerdictViewKind::KeyIndex, 0, kKeyIndexPlanFingerprint);
        }
    }
    installRetainedPlan_(srcTopicId, *judge, write);
    const auto sit2 = subscriptionIndex_.find(srcTopicId);   // the install may have added the source
    for (const int32_t id : sit2 == subscriptionIndex_.end() ? std::span<const int32_t>{} : std::span<const int32_t>(sit2->second)) {
        const ViewTemplate& tpl = templates_.at(id);
        if (!tpl.isVerdict || judge->hasView(id)) continue;
        // A retiring view still judges the records below its drop, on the partitions whose activated judge's log names it (a fresh judge may already be past the drop).
        if (tpl.retiring && (!judge->activated() || !namedFrom(write, judge->judgedThrough(), tpl.verdictObjectId, tpl.verdictFingerprint))) continue;
        // P9-5: the plan of a logged member's object under another fingerprint builds as a candidate (the member keeps judging), unless a
        // logged set the judge has yet to judge with names it: that is a member of record.  While this worker's catalog is behind the
        // logged set a plan is a stale one or not yet known to be a replacement: nothing is built.
        const ConstraintSet& logged = write.constraintSets.latest();
        const bool candidate = tpl.verdictKind != rune::view::VerdictViewKind::TestReject &&
            std::any_of(logged.members.begin(), logged.members.begin() + logged.count,
                        [&](const ConstraintSetMember& m) { return m.viewObjectId == tpl.verdictObjectId && m.planFingerprint != tpl.verdictFingerprint; }) &&
            !namedFrom(write, judge->judgedThrough(), tpl.verdictObjectId, tpl.verdictFingerprint);
        if (candidate && catalogBehind_(srcTopicId, *judge, write)) continue;
        ViewInstance* inst = getOrCreate_(id, partition, INT64_MAX);
        if (inst == nullptr) continue;
        if (tpl.verdictKind == rune::view::VerdictViewKind::TestReject) judge->installTestVerdictViewForTest(inst, tpl.verdictObjectId, tpl.testRule);
        else if (candidate) judge->addCandidate(inst, tpl.verdictKind, tpl.verdictObjectId, tpl.verdictFingerprint, tpl.planRecordPayload);
        else judge->addVerdictView(inst, tpl.verdictKind, tpl.verdictObjectId, tpl.verdictFingerprint, tpl.planRecordPayload);
        if (tpl.retiring) judge->markLeaving(id);
    }
    if (judge->memberCountForTest() != membersBefore) reserveTxnOverlays_(*judge);
    return judge;
}

// The reserve bound (Task 4's precondition): rows >= txnJudgedRecordsMax x the judge's per-record need, rows as long as its
// longest table's.  Only grows: one judge's members never shrink another's overlays.
void WorkerViewSet::reserveTxnOverlays_(const VerdictSource& judge) {
    const auto need = judge.overlayNeed();
    const std::size_t rows = static_cast<std::size_t>(std::max(txnJudgedRecordsMax_, 0)) * need.rowsPerRecord;
    txnOverlays_.reserveFor(rows, need.maxRowLen);
    // The atomic batch's overlay: a batch has at most a batch's records, bounded by the same cap as a transaction.
    if (rows > batchOverlayRows_ || need.maxRowLen > batchOverlayRowLen_) {
        batchOverlayRows_ = std::max(batchOverlayRows_, rows);
        batchOverlayRowLen_ = std::max(batchOverlayRowLen_, need.maxRowLen);
        batchOverlay_[0].reserve(batchOverlayRows_, batchOverlayRowLen_);
    }
}

void WorkerViewSet::installRetainedPlan_(int32_t srcTopicId, VerdictSource& judge, const PartitionWriteState& write) {
    if (!judge.waitingForInstall()) return;
    const ConstraintSetMember& want = judge.waitingFor();
    const RetainedVerdictPlan* r = write.retainedPlans.find(want.viewObjectId, want.planFingerprint);
    // A lagging catalog keeps the judge waiting for its own install (#788); one that refused the plan must not be overruled by the log.
    if (r == nullptr || std::any_of(refusedPlans_.begin(), refusedPlans_.end(), [&](const RefusedPlan& x) { return x.fingerprint == r->planFingerprint; })) return;
    if (judge.retainedTriedEpoch == verdictEpoch_ && judge.retainedTriedFingerprint == r->planFingerprint) return;
    if (catalogBehind_(srcTopicId, judge, write)) return;
    judge.retainedTriedEpoch = verdictEpoch_;   // decoded once per judge and template change, whatever comes of it
    judge.retainedTriedFingerprint = r->planFingerprint;
    std::optional<rune::view::CompiledViewPlan> plan = rune::view::deserializeCompiledViewPlan(r->plan);
    rune::codec::Descriptor outDesc, srcDesc;
    rune::view::RowLayout layout;
    if (plan && (plan->unicodeVersion == 0 || rune::codec::unicodeVersionSupported(plan->unicodeVersion))) {
        try {
            outDesc = rune::codec::deserialize(r->outputDescriptor);
            srcDesc = rune::codec::deserialize(r->sourceDescriptor);
            layout = rune::view::buildRowLayout(outDesc);
        } catch (const std::exception&) {
            plan.reset();
        }
    } else {
        plan.reset();
    }
    if (!plan) {
        if (std::find(unreadablePlans_.begin(), unreadablePlans_.end(), r->planFingerprint) == unreadablePlans_.end()) {
            unreadablePlans_.push_back(r->planFingerprint);
            spdlog::critical("VERDICT-PLAN-UNREADABLE view={} fp={:#x}", r->viewObjectId, r->planFingerprint);
        }
        return;
    }
    if (templates_.contains(plan->viewTopicId)) return;   // the catalog's own template names it: the attach below judges with that one
    const int32_t viewTopicId = plan->viewTopicId;
    // In memory whatever the plan says: it only judges up to its drop and serves no reads.
    installView(std::move(*plan), std::move(layout), std::move(srcDesc), /*persisted=*/false);
    ViewTemplate& tpl = templates_.at(viewTopicId);
    tpl.isVerdict = true;
    tpl.retiring = true;
    tpl.verdictKind = r->kind;
    tpl.verdictObjectId = r->viewObjectId;
    tpl.verdictFingerprint = r->planFingerprint;
    ++verdictEpoch_;
    ++judge.statsMut().retainedPlanInstalls;
}

bool WorkerViewSet::catalogBehind_(int32_t srcTopicId, VerdictSource& judge, const PartitionWriteState& write) {
    const ConstraintSet& logged = write.constraintSets.latest();
    if (catalogThrough_ >= logged.catalogThrough && !installsSinceMark_) return false;
    // Either the logged set was computed at a catalog position this worker has not applied, or a plan install or uninstall is
    // drained whose mark is not: any difference from the installed templates is a newer DDL change, never a stale one.  The sync
    // reruns when a mark, an install or a new logged change moves one of its keys.
    spdlog::debug("constraint set sync: topic {} partition {}: catalog gate closed, applied through {} (installs since the mark: {}) < logged set {} computed at {}; keeping the logged set",
                  srcTopicId, judge.partition(), catalogThrough_, installsSinceMark_, logged.id, logged.catalogThrough);
    return true;
}

int WorkerViewSet::syncConstraintSet_(int32_t srcTopicId, VerdictSource& judge, PartitionWriteState& write) {
    if (!judge.hasCandidates() && judge.constraintSyncEpoch == verdictEpoch_ && judge.constraintSyncChange == write.constraintSets.lastChangeOffset() && judge.constraintSyncCatalog == catalogThrough_) return 0;
    const ConstraintSet& logged = write.constraintSets.latest();
    const auto sit = subscriptionIndex_.find(srcTopicId);
    std::vector<ConstraintSetMember>& want = constraintWant_;
    want.clear();
    // A logged member keeps its place unless its template retired or was replaced; one whose plan is not installed here is kept
    // (the judge waits for it, it is never dropped by a guess).
    // #791 diagnostics: why each logged member stayed or went (0 live, 1 replaced by another fingerprint, 2 gone, 3 kept: not installed here).
    std::array<uint8_t, kMaxVerdictViewsPerSource> why{};
    std::array<uint64_t, kMaxVerdictViewsPerSource> replacedBy{};
    for (uint8_t i = 0; i < logged.count; ++i) {
        const ConstraintSetMember& m = logged.members[i];
        bool live = false, gone = false;
        if (sit != subscriptionIndex_.end()) {
            for (const int32_t id : sit->second) {
                const ViewTemplate& tpl = templates_.at(id);
                if (!tpl.isVerdict || tpl.verdictObjectId != m.viewObjectId) continue;
                if (tpl.verdictFingerprint == m.planFingerprint) (tpl.retiring ? gone : live) = true;
                else if (!tpl.retiring) { gone = true; replacedBy[i] = tpl.verdictFingerprint; }   // replaced by a plan with another fingerprint
            }
        }
        why[i] = live ? 0 : (gone ? (replacedBy[i] != 0 ? 1 : 2) : 3);
        // P9-5: the logged member keeps judging while its replacement builds as a candidate; the swap is asked for once it caught up.
        const bool building = replacedBy[i] != 0 && judge.hasMember(m.viewObjectId, m.planFingerprint) &&
                              judge.hasCandidate(m.viewObjectId, replacedBy[i]) && !judge.candidateCaughtUp(m.viewObjectId, replacedBy[i]);
        // A replacement this node refused leaves no template: the member stays and the judge fails closed rather than dropping a live constraint.
        const bool refused = !live && replacedBy[i] == 0 && std::any_of(refusedPlans_.begin(), refusedPlans_.end(), [&](const RefusedPlan& r) { return r.viewObjectId == m.viewObjectId; });
        if (live || !gone || building || refused) want.push_back(m);   // a retiring view's plan installed again stays (#786)
    }
    if (sit != subscriptionIndex_.end()) {
        for (const int32_t id : sit->second) {
            const ViewTemplate& tpl = templates_.at(id);
            if (!tpl.isVerdict) continue;
            if (tpl.retiring) {
                const bool inLogged = std::any_of(want.begin(), want.end(), [&](const ConstraintSetMember& m) { return m.viewObjectId == tpl.verdictObjectId && m.planFingerprint == tpl.verdictFingerprint; });
                const bool wasLogged = std::any_of(logged.members.begin(), logged.members.begin() + logged.count, [&](const ConstraintSetMember& m) { return m.viewObjectId == tpl.verdictObjectId && m.planFingerprint == tpl.verdictFingerprint; });
                if (!namedFrom(write, judge.judgedThrough(), tpl.verdictObjectId, tpl.verdictFingerprint))
                    judge.removeCandidate(tpl.verdictObjectId, tpl.verdictFingerprint);   // its view went before any change named it
                if (!inLogged && !wasLogged && !judge.hasView(id)) retiredQueue_.push_back({id, judge.partition()});   // never joined a set
                continue;
            }
            const bool present = std::any_of(want.begin(), want.end(), [&](const ConstraintSetMember& m) { return m.viewObjectId == tpl.verdictObjectId && m.planFingerprint == tpl.verdictFingerprint; });
            const bool building = judge.hasCandidate(tpl.verdictObjectId, tpl.verdictFingerprint) && !judge.candidateCaughtUp(tpl.verdictObjectId, tpl.verdictFingerprint) &&
                                  std::any_of(want.begin(), want.end(), [&](const ConstraintSetMember& m) { return m.viewObjectId == tpl.verdictObjectId; });
            if (!present && !building) want.push_back(ConstraintSetMember{tpl.verdictObjectId, tpl.verdictKind, tpl.verdictFingerprint});
        }
    }
    if (write.spec->keyIndex && std::none_of(want.begin(), want.end(), [](const ConstraintSetMember& m) { return m.kind == rune::view::VerdictViewKind::KeyIndex; }))
        want.push_back(ConstraintSetMember{0, rune::view::VerdictViewKind::KeyIndex, kKeyIndexPlanFingerprint});
    std::sort(want.begin(), want.end(), [](const ConstraintSetMember& a, const ConstraintSetMember& b) { return a.viewObjectId < b.viewObjectId; });
    ConstraintSet desired;
    desired.catalogThrough = catalogThrough_;   // a change request is stamped with the position it was computed at
    desired.count = static_cast<uint8_t>(std::min(want.size(), desired.members.size()));
    std::copy_n(want.begin(), desired.count, desired.members.begin());
    int work = 0;
    if (!catalogBehind_(srcTopicId, judge, write) && !desired.sameMembers(logged)) {
        const int64_t r = judge.requestChange(desired);
        if (r == VerdictSource::kChangeDeferred) return 0;   // not yet bound or activated: ask again next pass
        work = r >= 0 ? 1 : 0;
        // #791 diagnostics (cold: only when the logged set differs from what the installed views want).  A legitimate change logs
        // here too, so it is debug detail; the strings are built only when debug is on.
        if (spdlog::should_log(spdlog::level::debug)) {
            const auto d = judge.diagSnapshot();
            spdlog::debug("constraint set sync: topic {} partition {}: logged set {} ({} members) -> wanted {} members, change at offset {} (result {}), "
                          "leader generation {}, catalog gate open: applied through {} >= logged set computed at {}", srcTopicId, judge.partition(), logged.id, logged.count,
                          desired.count, write.constraintSets.lastChangeOffset(), r, d.leaderGeneration, catalogThrough_, logged.catalogThrough);
            for (uint8_t i = 0; i < logged.count; ++i) {
                static constexpr const char* kWhy[] = {"live", "replaced", "gone", "kept (plan not installed here)"};
                spdlog::debug("constraint set sync: topic {} partition {}: member object {} fingerprint {}: {}{}", srcTopicId, judge.partition(),
                              logged.members[i].viewObjectId, logged.members[i].planFingerprint, kWhy[why[i]],
                              why[i] == 1 ? " by fingerprint " + std::to_string(replacedBy[i]) : std::string());
            }
        }
    }
    judge.constraintSyncEpoch = verdictEpoch_;
    judge.constraintSyncChange = write.constraintSets.lastChangeOffset();
    judge.constraintSyncCatalog = catalogThrough_;
    return work;
}

int WorkerViewSet::flipVerdictMembers_(int32_t srcTopicId, int32_t partition, int64_t bound, uint64_t leaderGen, bool leaderHwConfirmed, NsTag ns) {
    if (const auto kit = keyIndexes_.find(instKey(srcTopicId, partition)); kit != keyIndexes_.end()) {
        adoptNs_(kit->second, ns);
        chargeMem_(kit->second);
    }
    const auto sit = subscriptionIndex_.find(srcTopicId);
    if (sit == subscriptionIndex_.end()) return 0;
    for (const int32_t id : sit->second) {
        if (!isVerdictTemplate_(id)) continue;
        const auto vit = views_.find(instKey(id, partition));
        if (vit == views_.end()) continue;
        ViewInstance& inst = vit->second;
        adoptNs_(inst, ns);
        chargeMem_(inst);
        if (inst.foldLeadership != leaderGen) {
            inst.foldLeadership = leaderGen;
            if (inst.readiness == ViewReadiness::Ready) inst.readiness = ViewReadiness::CatchingUp;
        }
        if (inst.readiness != ViewReadiness::Ready && inst.foldCursor >= bound && checkpointSettled(inst)) {
            inst.readiness = leaderHwConfirmed ? ViewReadiness::Ready : ViewReadiness::CatchingUp;
            pruneRecoveryIfAllReady_(id);
        }
    }
    return 0;
}

int WorkerViewSet::finishRetired_() {
    if (retiredQueue_.empty()) return 0;
    int work = 0;
    std::vector<Retired> queue;
    queue.swap(retiredQueue_);
    for (const Retired& r : queue) {
        const auto tit = templates_.find(r.viewTopicId);
        if (tit == templates_.end() || !tit->second.isVerdict || !tit->second.retiring) continue;
        eraseInstance_(r.viewTopicId, r.partition);
        const bool judged = std::any_of(verdictSources_.begin(), verdictSources_.end(), [&](const auto& s) {
            return s.second->hasView(r.viewTopicId);
        });
        if (!judged) (void)eraseView_(r.viewTopicId);   // another partition's judge may still judge with it
        ++work;
    }
    return work;
}

void WorkerViewSet::eraseInstance_(int32_t viewTopicId, int32_t partition) {
    if (const auto it = views_.find(instKey(viewTopicId, partition)); it != views_.end()) {
        releaseMem_(it->second);
        views_.erase(it);
    }
}

int WorkerViewSet::pushVerdictCheckpoint_(ViewInstance& inst) {
    int work = 0;
    if (!checkpointSettled(inst)) {
        // A discard in flight: the frame goes out (after a held delta), then the instance waits for its ack.  Nothing is checkpointed.
        if (std::holds_alternative<CheckpointDiscardOwed>(inst.checkpoint)) return tryPushDiscard_(inst) ? 1 : 0;
        return 0;
    }
    if (inst.pendingValid) {
        ++work;
        if (!tryPushDelta_(inst)) return work;
    }
    if (!inst.persisted) {
        discardDirty_(inst);
        return work;
    }
    return work + buildAndPushCheckpoint_(inst);
}

// The ONE way an instance's persisted rows are thrown away (recovery, the log-start clamp, a verdict member that outran its
// markers).  Invariant: afterwards the instance has no rows, is NotReady and folds nothing until its checkpoint is Live again.
// The checkpoint FILE is moved into backup/ (or removed, with backups off) by the view-snapshot IO agent; the worker never
// touches the filesystem.  A delta held for the ring still belongs to the old stream: it goes out before the discard frame, so it
// lands in the backup.
void WorkerViewSet::beginDiscard_(ViewInstance& inst, ViewDiscardReason reason) {
    if (!checkpointSettled(inst)) return;   // idempotent: one discard at a time
    const std::size_t indexSlots = inst.plan.sizing.capacity != 0 ? ceilPow2(inst.plan.sizing.capacity) : std::size_t{4096};
    const std::size_t chunkBytes = inst.plan.sizing.chunkBytes != 0 ? static_cast<std::size_t>(inst.plan.sizing.chunkBytes) : (std::size_t{32} << 20);
    inst.table = rune::view::ViewTable(inst.layout, indexSlots, chunkBytes, inst.tableKeys);
    if (inst.agg) inst.agg.emplace(rune::view::buildAggLayout(inst.plan, inst.evolvingSrc).value(), indexSlots, chunkBytes, inst.tableKeys);
    if (inst.keyed) buildKeyedTables(inst, indexSlots, chunkBytes);
    inst.foldCursor = 0;
    inst.spanFence.clear();
    inst.lastEncodedO = -1;
    inst.lastDurableO = -1;
    inst.transientCursor = CursorFresh{};
    inst.readiness = ViewReadiness::RebuildingViews;
    inst.counters.uniqueConflicts = 0;
    ++inst.counters.checkpointDiscards;
    chargeMem_(inst);
    spdlog::info("VIEW-DISCARD view={} partition={} reason={}", inst.plan.viewTopicId, inst.partition, viewDiscardReasonName(reason));
    if (!inst.persisted) return;            // an in-memory view has nothing on disk to save
    inst.checkpoint = CheckpointDiscardOwed{reason};
    (void)tryPushDiscard_(inst);
}

bool WorkerViewSet::startAtLogStart_(ViewInstance& inst, int64_t logStart) {
    const int64_t from = inst.foldCursor;
    if (startAtLogStart(inst, logStart)) {
        spdlog::info("VIEW-START-AT-LOG-START view={} partition={} from={} logStart={}", inst.plan.viewTopicId, inst.partition, from, logStart);
        return true;
    }
    beginDiscard_(inst, ViewDiscardReason::BelowLogStart);
    return false;
}

bool WorkerViewSet::tryPushDiscard_(ViewInstance& inst) {
    const auto* owed = std::get_if<CheckpointDiscardOwed>(&inst.checkpoint);
    if (owed == nullptr) return false;
    if (inst.pendingValid && !tryPushDelta_(inst)) return false;   // the held delta belongs to the old stream: it precedes the frame
    const auto claim = channel_.deltas.tryClaim(kDiscardMsgType, static_cast<std::size_t>(kDiscardMsgBytes));
    if (claim < 0) return false;            // ring full: retried next pass
    uint8_t* dst = channel_.deltas.base() + claim;
    writeI32LE(dst, inst.plan.viewTopicId);
    writeI32LE(dst + 4, inst.partition);
    dst[8] = static_cast<uint8_t>(owed->reason);
    dst[9] = dst[10] = dst[11] = 0;
    channel_.deltas.commit(claim);
    inst.checkpoint = CheckpointDiscardPending{owed->reason};
    return true;
}

} // namespace rune::node
