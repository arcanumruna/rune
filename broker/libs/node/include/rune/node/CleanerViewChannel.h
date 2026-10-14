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

// Catalog history Task 8: the `view` ring also carries a topic's cleaner POLICY
// OVERRIDE (an AlterTopicConfig the catalog applied — ruling 5: the cleaner
// consumes the alterable keys, the replica's construction-time config is never
// touched):
//   kPolicyMsgType: [topicId:i32][count:u16][(key:u8, value:i64) * count]
//   key = rune::protocol::ConfigKey's byte (RetentionMs .. MaxCompactionLagMs;
//   MinCleanableDirtyRatioPpm in parts per million).  Entries MERGE into the
//   topic's override (an AlterTopicConfig names only the keys it changes);
//   count == 0 CLEARS the topic's override (its drop).  Idempotent.
struct CleanerViewChannel {
    static constexpr int32_t     kViewMsgType   = 1;
    static constexpr int32_t     kAckMsgType    = 2;
    static constexpr int32_t     kPolicyMsgType = 3;
    static constexpr std::size_t kPolicyHeaderBytes = 4 + 2;
    static constexpr std::size_t kPolicyEntryBytes  = 1 + 8;
    static constexpr std::size_t kHeaderBytes   = 24;
    static constexpr uint32_t    kPtrsPerChunk  = 4096;   // 32 KiB + header << view.maxMsgLength()
    OneToOneByteRing view{1 << 20};   // control → cleaner
    OneToOneByteRing ack{4096};       // cleaner → control
};

} // namespace rune::node
