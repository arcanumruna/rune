#pragma once
// CleanerViewChannel.h — Task 10 round 4: the LogCleanerAgent's replica view.
//
// The cleaner runs on its own thread.  It used to iterate
// ReplicaRegistry::allReplicas() directly — a std::vector the control thread
// mutates (add / remove) — and could hold a replica the control thread then
// FREED (collectRetired): a cross-thread data race + use-after-free.
//
// Shared-nothing replacement (rings only):
//   control → cleaner  `view`: the registry's replica set for generation G,
//                       chunked: [gen:u64][total:u32][index:u32][count:u32][pad:u32]
//                       [count × replica pointer:u64].  A new generation restarts
//                       at index 0; the cleaner swaps its OWN vector only when a
//                       generation is complete.
//   cleaner → control  `ack`: [gen:u64] once the cleaner swapped to G — from then
//                       on it holds no replica outside G.
// The control thread frees a retired replica only when BOTH every worker and the
// cleaner acked a generation at or above its removal (NodeRuntimeAgent).
// Owned by NodeRuntimeAgent (outlives the cleaner: BrokerNode stops + destroys
// the cleaner first).
#include <rune/node/OneToOneByteRing.h>

#include <cstdint>

namespace rune::node {

struct CleanerViewChannel {
    static constexpr int32_t     kViewMsgType   = 1;
    static constexpr int32_t     kAckMsgType    = 2;
    static constexpr std::size_t kHeaderBytes   = 24;
    static constexpr uint32_t    kPtrsPerChunk  = 4096;   // 32 KiB + header << view.maxMsgLength()
    OneToOneByteRing view{1 << 20};   // control → cleaner
    OneToOneByteRing ack{4096};       // cleaner → control
};

} // namespace rune::node
