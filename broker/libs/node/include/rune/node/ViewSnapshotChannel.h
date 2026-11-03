#pragma once
// ViewSnapshotChannel.h — Task 13: view-worker -> dedicated view-snapshot-IO
// handoff.  Mirrors CleanerViewChannel.h's shared-nothing two-ring pattern:
//
//   worker -> IO  `deltas`: one Task-12 checkpoint delta (or base) per
//                 message, prefixed with a tiny routing header so the IO
//                 thread knows which (viewTopicId, partition) delta file to
//                 append it to.  The worker claims a ring slot and encodes
//                 directly into it (rune::view::encodeDelta's output, or any
//                 prefix of it) -- zero extra copy on the worker side.
//   IO -> worker  `done`: one durable-offset ack per flushed generation, once
//                 ViewSnapshotIoAgent has fdatasync'd the bytes (see
//                 ViewSnapshotIoAgent.h for the offset-atomic rule: `done`'s
//                 O is only ever a value that is ALREADY durable on disk).
//
// `deltas` wire format (little-endian), one message = one frame:
//   [viewTopicId i32][partition i32][delta bytes...]
// where `delta bytes` is exactly one rune::view::encodeDelta() wire record
// -- itself [magic u32][O i64][rowLen u32][count u32]{entries...}[crc32c u32]
// (see libs/view/include/rune/view/ViewCheckpoint.h).  The IO agent reads the
// embedded O directly out of the delta bytes; it is NOT duplicated in the
// routing header.
//
// `done` wire format: [viewTopicId i32][partition i32][O i64] -- the latest
// durable O for that (viewTopicId, partition) after a grouped fdatasync.
//
// Discard and backup protocol (view checkpoint backups).  Whenever the worker decides to throw away a view
// instance's persisted rows it sends ONE kDiscardMsgType frame on `deltas`
// ([view i32][part i32][reason u8][pad 3]), after every delta the old stream still owes the file.  The IO agent (the only
// thread that touches the checkpoint files) MOVES the live file into `<viewDataDir>/backup/` (link + unlink, then fsync the
// backup dir and the view dir; with RUNE_VIEW_BACKUP=0 it unlinks instead), prunes old backups down to `keep`, and only then
// acks with kDiscardDoneMsgType ([view i32][part i32][status i32], 0 = OK, else errno) on `done`.  The worker folds nothing
// for that instance until the ack arrives; a failed backup leaves the view NotReady for the rest of the boot.
//
// Both rings are owned together so construction/destruction order between
// the worker side and the IO side is trivially correct (single struct, no
// separate lifetime to track) -- same shape as CleanerViewChannel.

#include <rune/node/OneToOneByteRing.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace rune::node {

/// Why a view instance's persisted rows are being thrown away.  The numeric values are on the wire and in the backup file
/// names' reason tag: never renumber.
enum class ViewDiscardReason : uint8_t {
    Legacy = 1,            ///< pre-#537 checkpoint (may hold aborted-transaction rows): backed up, view rebuilt
    LegacySuperseded = 2,  ///< a legacy file next to a committed one: the legacy file is backed up, nothing rebuilt
    Unreadable = 3,        ///< a checkpoint record failed its CRC / decode
    AheadOfLog = 4,        ///< checkpoint offset above the source log end
    AheadOfVerdicts = 5,   ///< a verdict member's checkpoint outran its markers
    BelowLogStart = 6,     ///< the cursor fell below the source's logStart with rows in the table
};
constexpr const char* viewDiscardReasonName(ViewDiscardReason r) noexcept {
    switch (r) {
        case ViewDiscardReason::Legacy: return "legacy";
        case ViewDiscardReason::LegacySuperseded: return "legacy-superseded";
        case ViewDiscardReason::Unreadable: return "unreadable";
        case ViewDiscardReason::AheadOfLog: return "ahead-of-log";
        case ViewDiscardReason::AheadOfVerdicts: return "ahead-of-verdicts";
        case ViewDiscardReason::BelowLogStart: return "below-log-start";
    }
    return "unknown";
}
constexpr bool discardTargetsLegacy(ViewDiscardReason r) noexcept {
    return r == ViewDiscardReason::Legacy || r == ViewDiscardReason::LegacySuperseded;
}
inline constexpr int32_t kDiscardMsgType = 5;        // worker -> IO on `deltas`
inline constexpr int32_t kDiscardDoneMsgType = 6;    // IO -> worker on `done`
inline constexpr int32_t kDiscardMsgBytes = 12;      // [view i32][part i32][reason u8][pad 3]
inline constexpr int32_t kDiscardDoneMsgBytes = 12;  // [view i32][part i32][status i32]: 0 = OK, else errno

/// RUNE_VIEW_BACKUP / RUNE_VIEW_BACKUPS_KEPT.
struct ViewBackupPolicy {
    bool     enabled = true;
    uint32_t keep    = 3;
};
inline std::string viewBackupDir(const std::string& viewDataDir) { return viewDataDir + "/backup"; }

// The per-(view, partition) checkpoint file under the view data dir — the ONE
// name the IO agent appends to and recovery reloads.  Issue #537: since the fold
// skips aborted transactions the file is "view-<id>-p<n>.committed.delta".  A
// checkpoint written before that fix ("view-<id>-p<n>.delta",
// legacyViewCheckpointFilePath) may hold rows / aggregate contributions of
// ABORTED transactions, so it is never loaded: recovery backs it up (kDiscardMsgType)
// and re-folds the view from the source log (views = a re-computation from the log).
inline std::string viewCheckpointFilePath(const std::string& dir, int32_t viewTopicId, int32_t partition) {
    return dir + "/view-" + std::to_string(static_cast<uint32_t>(viewTopicId)) + "-p" + std::to_string(partition) +
           ".committed.delta";
}
inline std::string legacyViewCheckpointFilePath(const std::string& dir, int32_t viewTopicId, int32_t partition) {
    return dir + "/view-" + std::to_string(static_cast<uint32_t>(viewTopicId)) + "-p" + std::to_string(partition) +
           ".delta";
}

struct ViewSnapshotChannel {
    static constexpr int32_t     kDeltaMsgType     = 1;
    static constexpr int32_t     kDoneMsgType      = 2;
    // Task 19: control -> worker compiled-view-plan install.  One frame = one
    // rune::node::ViewPlanUpdateMsg (AdminChannels.h) encoded via
    // encodeViewPlanUpdate(install, kInstallMsgType, msg).  The worker drains it
    // in its own doWork (WorkerViewSet::drainInstalls) and calls installView on
    // ITS OWN thread — shared-nothing (installView is never called cross-thread).
    static constexpr int32_t     kInstallMsgType   = 3;
    // 4 was the checkpoint-truncate "reset" frame.  Retired (replaced by kDiscardMsgType = 5, discard + backup): the value is never reused.
    static constexpr std::size_t kDeltaHeaderBytes = 8;  // viewTopicId i32 + partition i32
    static constexpr std::size_t kDoneMsgBytes      = 16; // viewTopicId i32 + partition i32 + O i64

    OneToOneByteRing deltas{1 << 20};  // worker -> IO
    OneToOneByteRing done{4096};       // IO -> worker (durable-offset acks)
    OneToOneByteRing install{1 << 20}; // control -> worker (compiled view plans; Task 19)
};

} // namespace rune::node
