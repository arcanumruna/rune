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
// Both rings are owned together so construction/destruction order between
// the worker side and the IO side is trivially correct (single struct, no
// separate lifetime to track) -- same shape as CleanerViewChannel.

#include <rune/node/OneToOneByteRing.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace rune::node {

// The per-(view, partition) checkpoint file under the view data dir — the ONE
// name the IO agent appends to and recovery reloads.  Issue #537: since the fold
// skips aborted transactions the file is "view-<id>-p<n>.committed.delta".  A
// checkpoint written before that fix ("view-<id>-p<n>.delta",
// legacyViewCheckpointFilePath) may hold rows / aggregate contributions of
// ABORTED transactions, so it is never loaded: recovery removes it and re-folds
// the view from the source log (views = a re-computation from the log).
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
    // worker -> IO on `deltas`: [viewTopicId i32][partition i32].  The checkpoint file of that (view, partition) is truncated to
    // empty: the worker discarded a checkpoint that outran the verdict markers of its source (Part 04, P4-4) and refolds it.
    static constexpr int32_t     kResetMsgType     = 4;
    static constexpr std::size_t kDeltaHeaderBytes = 8;  // viewTopicId i32 + partition i32
    static constexpr std::size_t kDoneMsgBytes      = 16; // viewTopicId i32 + partition i32 + O i64

    OneToOneByteRing deltas{1 << 20};  // worker -> IO
    OneToOneByteRing done{4096};       // IO -> worker (durable-offset acks)
    OneToOneByteRing install{1 << 20}; // control -> worker (compiled view plans; Task 19)
};

} // namespace rune::node
