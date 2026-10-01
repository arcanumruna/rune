#pragma once
// ---------------------------------------------------------------------------
// TransactionCoordinationService.h — TX-3: wires TransactionCoordinator +
// ReplicatedTxnStateStore into the broker duty cycle; translates client txn
// requests into durable state transitions.
//
// Namespace: rune::node
// C++23. Single-writer (doWork driven). No locks. No hot-path allocation.
//
// ## Responsibilities
//
//   - Five request handlers (InitProducerId / AddPartitionsToTxn /
//     AddOffsetsToTxn / TxnOffsetCommit / EndTxn): decode the client-protocol
//     frame (TxnMessages.h), call TransactionCoordinator, persist the resulting
//     TransactionMetadata via ReplicatedTxnStateStore::put(acksAll=true) BEFORE
//     responding, then encode the response into the caller-supplied buffer.
//   - NotCoordinator fence: when isLocalCoordinator()==false every handler
//     encodes a response with errorCode=NotCoordinator.
//   - doWork(nowNs): drives TransactionCoordinator::doWork (timeout aborts);
//     resolves pending markers (Task 8).
//
// ## Constructor deps
//
//   - ReplicatedTxnStateStore*    txnState — non-owning; must outlive.
//   - rune::gateway::TopicRegistry* topics  — non-owning; may be null.
//   - std::function<bool()>  isLeader       — leader query callback.
//   - std::function<NodeId()> leaderId      — current leader nodeId callback.
//
// ## Zero-alloc behaviour (CG-7 rule)
//
//   Handlers allocate nothing on the response-encode path.  txnIdScratch_
//   and groupIdScratch_ are member std::strings that amortise allocation:
//   assign(data, size) reuses capacity.  Partition scratch vectors are
//   pre-reserved members.  No per-call std::string construction for txnId
//   lookup — transparent comparators (std::less<>) on the coordinator map.
// ---------------------------------------------------------------------------

#include <spdlog/spdlog.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/Types.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/client/TxnMessages.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Forward declaration — ClientGateway lives in rune_gateway; node→gateway direction is fine.
namespace rune::gateway { class ClientGateway; }

namespace rune::node {

using NodeId        = int32_t;
using TransactionalId = rune::transaction::TransactionalId;

// TX-3: TransactionCoordinationService implements ITxnRequestHandler so that
// ClientGateway can dispatch txn messages through the interface without
// introducing a rune_gateway→rune_node symbol dependency (mirrors CG-4 pattern).
class TransactionCoordinationService : public rune::gateway::ITxnRequestHandler {
public:
    // -----------------------------------------------------------------------
    // Construction
    //
    // @param txnState   ReplicatedTxnStateStore — non-owning; must outlive.
    // @param topics     TopicRegistry — non-owning; may be null.
    // @param isLeader   Callable → true when this node is the cluster leader.
    // @param leaderId   Callable → current cluster leader NodeId.
    // -----------------------------------------------------------------------
    TransactionCoordinationService(
        rune::transaction::ReplicatedTxnStateStore* txnState,
        rune::gateway::TopicRegistry*               topics,
        std::function<bool()>                       isLeader = [] { return true; },
        std::function<NodeId()>                     leaderId = [] { return NodeId{0}; });

    // Non-copyable, non-movable (holds coordinator + pre-reserved scratch).
    TransactionCoordinationService(const TransactionCoordinationService&)            = delete;
    TransactionCoordinationService& operator=(const TransactionCoordinationService&) = delete;

    // -----------------------------------------------------------------------
    // doWork — drive TransactionCoordinator::doWork (timeout aborts);
    // resolve pending markers (stub for Task 8).
    //
    // Returns work count (0 when idle in steady state).
    // Must be called from ONE thread only (duty-cycle thread).
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs);

    // -----------------------------------------------------------------------
    // setGateway — wire in the ClientGateway for marker append (Task 8).
    // Non-owning; lifetime must exceed the service.
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setGateway(rune::gateway::ClientGateway* gateway) noexcept {
        gateway_ = gateway;
    }

    // -----------------------------------------------------------------------
    // setAppendFnOverride — TEST SEAM ONLY.
    // When gateway_ is null, doWork uses this function to resolve pending
    // markers.  The fn maps (TopicPartition, markerBytes) to a log append.
    // Allows testing the doWork→resolvePendingMarkers path without Aeron.
    // Set to nullptr to clear (production: never call this).
    // Single-writer: call only on the duty-cycle thread.
    // -----------------------------------------------------------------------
    void setAppendFnOverride(
        std::function<int64_t(std::string_view topic, int32_t partition,
                              std::span<const uint8_t> bytes)> fn) noexcept {
        appendFnOverride_ = std::move(fn);
    }

    // -----------------------------------------------------------------------
    // isLocalCoordinator — true when this node is the cluster leader.
    // v1 seam: all transactions are coordinated by the cluster leader.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool isLocalCoordinator() const;

    // -----------------------------------------------------------------------
    // coordinatorFor — returns the cluster-leader nodeId for any txnId.
    // v1 seam (future: per-txnId hash routing).
    // -----------------------------------------------------------------------
    [[nodiscard]] NodeId coordinatorFor(const TransactionalId& txnId) const;

    // -----------------------------------------------------------------------
    // Handlers — decode the request from buf[0..len-1], act, encode the
    // response into out[] (reused by caller), return bytes written.
    //
    // When isLocalCoordinator()==false: encode the protocol response with
    // errorCode = NotCoordinator and return that size.
    //
    // buf and out MUST NOT overlap.
    // Never throws.  Never allocates from heap in steady state.
    // -----------------------------------------------------------------------
    size_t handleInitProducerId (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleAddPartitions  (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleAddOffsets     (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleTxnOffsetCommit(const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;
    size_t handleEndTxn         (const uint8_t* buf, size_t len,
                                  int64_t nowNs, uint8_t* out, size_t outCap) override;

    /// Task 10 fix 3: a control marker this node could not append (NotLeader /
    /// append failure).  Recorded + logged — never silent; counts as resolved.
    void onControlMarkerRefused(int32_t topicId, int32_t partition,
                                int64_t producerId, int16_t errorCode) override;
    /// Task 10 round 5: an asynchronously appended marker landed.
    void onControlMarkerLanded(int32_t topicId, int32_t partition, int64_t producerId) override;
    [[nodiscard]] std::size_t awaitingMarkerTxnsForTest() const noexcept { return awaiting_.size(); }
    [[nodiscard]] uint64_t transactionsCompletedForTest() const noexcept { return transactionsCompleted_; }
    [[nodiscard]] uint64_t controlMarkersRefusedForTest() const noexcept { return controlMarkersRefused_; }

private:
    uint64_t controlMarkersRefused_ = 0;
    uint64_t transactionsCompleted_ = 0;
    // Task 10 round 5: Prepare* transactions whose markers are still in flight,
    // keyed by the markers' producerId (one transaction per producer at a time:
    // the next one is refused with ConcurrentTransactions until this completes).
    struct AwaitingMarkers {
        rune::transaction::TransactionalId txnId;
        int     outstanding   = 0;
        bool    respPending   = false;   // a deferred EndTxn response
        int64_t sessionId     = 0;
        int64_t correlationId = 0;
    };
    std::unordered_map<int64_t, AwaitingMarkers> awaiting_;
    bool dispatchMarkers_(const rune::transaction::TransactionalId& txnId, int64_t producerId,
                          int16_t producerEpoch, int32_t coordinatorEpoch, bool commit,
                          const std::vector<rune::transaction::TopicPartition>& parts,
                          int64_t respSessionId = 0, int64_t respCorrelationId = 0);
    void resolveMarker_(int64_t producerId);
    void completeAwaiting_(int64_t producerId, bool respond);
    [[nodiscard]] int concurrentIfPreparing_(int err) const;
    // -----------------------------------------------------------------------
    // Members
    // -----------------------------------------------------------------------
    rune::transaction::ReplicatedTxnStateStore* txnState_;    // non-owning
    rune::gateway::TopicRegistry*               topics_;      // non-owning; may be null
    std::function<bool()>                       isLeaderFn_;
    std::function<NodeId()>                     leaderIdFn_;

    // CG-4 failover pattern: detect Follower→Leader transition to rebuild index.
    bool wasLeader_ = false;

    // ClientGateway — non-owning; may be null (Task 8 marker append).
    rune::gateway::ClientGateway*               gateway_ = nullptr;

    // TEST SEAM: override appendFn used by doWork when gateway_ is null.
    // Signature: (topic, partition, markerBytes) → baseOffset or -1.
    std::function<int64_t(std::string_view, int32_t, std::span<const uint8_t>)>
                                                appendFnOverride_;

    // Zero-alloc scratch containers for doWork marker-drive path.
    // Cleared+refilled each invocation — pre-reserved to avoid steady-state alloc.
    // PendingInfo: commit flag keyed by producerId.
    struct PendingInfo { bool commit; };
    std::unordered_map<rune::transaction::ProducerId, PendingInfo> pendingByPidScratch_;
    // MarkerNotify: per-marker data for notifyControlMarker post-processing.
    struct MarkerNotify {
        int32_t topicId;
        int32_t partition;
        int64_t producerId;
        int64_t markerOffset;
        bool    commit;
    };
    std::vector<MarkerNotify>                   toNotifyScratch_;

    // Per-transactionalId coordinator (single global coordinator, v1 seam).
    // One coordinator manages all transactions on this node.
    rune::transaction::TransactionCoordinator   coordinator_;

    // CG-7 zero-alloc: per-handler scratch strings.
    // Assigned via assign(data, size) — no heap alloc when capacity >= size.
    // Never holds state across call boundaries.
    std::string txnIdScratch_;    ///< txnId span → reusable std::string
    std::string groupIdScratch_;  ///< groupId span → reusable std::string

    // Pre-reserved scratch for partition vectors (addPartitions, endTxn).
    std::vector<rune::transaction::TopicPartition> partitionScratch_;

    // Pre-reserved scratch for partition results (addPartitions / txnOffsetCommit
    // response path).  Cleared+refilled each call — no per-call vector alloc.
    std::vector<rune::protocol::client::TxnPartitionResult> partResultScratch_;

    // Member scratch for the offsets map passed to addOffsetsToTxn
    // (handleTxnOffsetCommit).  Cleared+refilled each call — no per-call map alloc.
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsetsScratch_;

    // Scratch string for topic-name fallback ("topic_<id>") — avoids the
    // per-partition temporary "topic_" + std::to_string(topicId) alloc.
    std::string topicNameScratch_;
};

} // namespace rune::node
