#pragma once
// ViewSnapshotIoAgent.h — Task 13: dedicated view-checkpoint IO thread.
//
// Mirrors LogCleanerAgent: a rune::consensus::Agent driven by its own
// AgentRunner thread, draining a worker->IO ring and doing pure file IO off
// the produce/consume hot path.  Shared-nothing: the only cross-thread
// communication is the two OneToOneByteRings in ViewSnapshotChannel; this
// agent's file-handle map and counters are plain, single-thread-owned
// members (no atomics, no locks -- it is the ONLY writer of every field
// below, always called from the same AgentRunner thread).
//
// Each doWork(nowNs) cycle:
//   1. Drain `channel.deltas` (bounded limit). For each frame:
//        [viewTopicId i32][partition i32][delta bytes...]
//      lazily open (or reuse) the per-(viewTopicId,partition) delta file and
//      append the delta bytes to it (appendNoSync-style: write(), no fsync
//      per message -- fsyncs are GROUPED, see below). Track the highest O
//      seen for that key since the last fsync (parsed straight out of the
//      delta bytes' own Task-12 header: [magic u32][O i64]...).
//   2. OFFSET-ATOMIC fsync grouping: once nowNs - lastFsyncNs_ >= fsyncMs*1e6
//      (or the drain came back empty while bytes are still pending), run ONE
//      fdatasync per dirty fd this cycle, THEN -- only after every dirty fd
//      has synced -- advance lastDurableOffset_[(view,part)] to the O that
//      was appended before the sync, and post a `done` ack carrying it. A
//      delta's O NEVER becomes "durable" before its bytes are fdatasync'd:
//      a crash between append and fsync leaves lastDurableOffset() at the
//      PRIOR fsync'd O, never the just-appended one.
//
// Back-pressure: `done` is a small fixed ring (4096 B); if it is momentarily
// full the ack is simply retried next cycle (pendingAcks_ holds it) -- never
// dropped, and lastDurableOffset() is already correct in-process regardless
// (the ack is only a worker-side notification; readers of this agent, e.g.
// the Task 16 readiness gate, call lastDurableOffset() directly).

#include <rune/consensus/ConsensusAgent.h>
#include <rune/node/ViewSnapshotChannel.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::node {

class ViewSnapshotIoAgent final : public rune::consensus::Agent {
public:
    /// @param channel      The worker<->IO ring pair. Not owned; must outlive
    ///                     this agent (same lifetime discipline as
    ///                     CleanerViewChannel / LogCleanerAgent).
    /// @param viewDataDir  Directory the per-(viewTopicId,partition) delta
    ///                     files are created in. Created if missing.
    /// @param fsyncMs      Grouped-fsync window in milliseconds. Default 50.
    explicit ViewSnapshotIoAgent(ViewSnapshotChannel& channel,
                                 std::string viewDataDir,
                                 int64_t fsyncMs = 50);
    ~ViewSnapshotIoAgent() override;

    ViewSnapshotIoAgent(const ViewSnapshotIoAgent&)            = delete;
    ViewSnapshotIoAgent& operator=(const ViewSnapshotIoAgent&) = delete;

    int doWork(int64_t nowNs) override;
    const char* name() const noexcept override { return "rune-view-io"; }

    /// Durable O for (viewTopicId, partition): the highest O whose bytes are
    /// CONFIRMED fdatasync'd. -1 if nothing has ever been made durable for
    /// this key (used by the Task 16 readiness gate + this task's own test).
    [[nodiscard]] int64_t lastDurableOffset(int32_t viewTopicId, int32_t partition) const noexcept;

    /// Path of the delta file for (viewTopicId, partition) -- test seam (also
    /// usable by recovery code that needs to reopen + replay the file).
    [[nodiscard]] std::string deltaFilePathForTest(int32_t viewTopicId, int32_t partition) const;

private:
    struct Key {
        int32_t viewTopicId;
        int32_t partition;
        bool operator==(const Key& o) const noexcept {
            return viewTopicId == o.viewTopicId && partition == o.partition;
        }
    };
    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            return (static_cast<size_t>(static_cast<uint32_t>(k.viewTopicId)) << 32) ^
                   static_cast<size_t>(static_cast<uint32_t>(k.partition));
        }
    };
    // IO-thread-owned per-(view,partition) file state. No atomics: this
    // agent is the sole reader/writer of every FileState, always from the
    // same AgentRunner thread (shared-nothing -- see file header).
    struct FileState {
        int     fd              = -1;
        int64_t pendingO        = -1;  // highest O appended since the last fsync (-1 = none pending)
        int64_t lastDurableO    = -1;  // highest O CONFIRMED fdatasync'd
        bool    dirtySinceSync  = false;
    };

    void handleDeltaFrame_(const uint8_t* p, std::size_t len);
    FileState& fileFor_(const Key& key);
    std::string deltaFilePath_(const Key& key) const;
    void flushDue_(int64_t nowNs, bool idle);
    void postDone_(int32_t viewTopicId, int32_t partition, int64_t O);
    void drainPendingAcks_();

    ViewSnapshotChannel& channel_;
    std::string          viewDataDir_;
    int64_t              fsyncMs_;
    int64_t              lastFsyncNs_   = 0;

    std::unordered_map<Key, FileState, KeyHash> files_;

    // A `done` ack the ring was too full to accept last cycle; retried next
    // cycle before any new acks (never lost, never reordered ahead of a
    // newer ack for the SAME key since flushDue_ only ever queues the
    // latest durable O per key per pass).
    struct PendingAck { int32_t viewTopicId; int32_t partition; int64_t O; };
    std::vector<PendingAck> pendingAcks_;
};

} // namespace rune::node
