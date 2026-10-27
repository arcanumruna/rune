#pragma once
// ITxnRequestHandler.h — TX-3: abstract interface for transaction request
// handling, owned by the gateway lib so ClientGateway depends only on this
// interface rather than on rune_node symbols.
//
// TransactionCoordinationService (in rune_node) implements this interface.
// ClientGateway holds an ITxnRequestHandler* (set via setTxnService) and
// dispatches txn messages (templateIds 45, 47, 49, 51, 53) through it.
// rune_gateway therefore has NO direct dependency on rune_node symbols at
// link time (mirrors IGroupRequestHandler / CG-4 pattern).
//
// Namespace: rune::gateway
// C++23.

#include <rune/coordination/NsScopedId.h>

#include <cstddef>
#include <climits>
#include <cstdint>

namespace rune::gateway {

/// Fix round 3 (I3): the minimum answer buffer every ITxnRequestHandler call gets.
inline constexpr std::size_t kTxnMinOutCap = 64;

struct ITxnRequestHandler {
    virtual ~ITxnRequestHandler() = default;

    // Namespaces Phase 2 Task 5 (D19): `ctx` is the request's namespace context; the transactional id (and the group
    // id of AddOffsets / TxnOffsetCommit) is turned into its scoped id right after decode; an id with the reserved
    // prefix is answered InvalidRequest; an empty transactional id stays empty.  Responses echo the client's ids.
    //
    // Each method decodes the request from buf[0..len-1], acts, encodes the
    // response into out[0..outCap-1], and returns the number of bytes written.
    //
    // No-hang contract (fix round 2, I3): EVERY outcome writes an answer — a
    // body that does not decode answers InvalidRequest, a response that does
    // not fit outCap answers MessageTooLarge (RequestRejection.h).  0 means
    // exactly one thing: the answer is DEFERRED and the handler sends it later via
    // ClientGateway::sendControlFrame - (#358) ANY handler whose answer depends on a
    // __transaction_state record (EndTxn: its Prepare* record, not its markers - #38)
    // that is not committed yet (sent once the replica's HW passes it, or
    // answered NotCoordinator if the coordinator unloads first).
    //
    // Fix round 3 (I3): outCap is ALWAYS >= kTxnMinOutCap (the gateway passes
    // its 68 KiB scratch — static_assert-ed at the call site), which holds every
    // fixed-size txn response and every rejection (kMaxRejectionBytes).  A
    // handler therefore never has to "fail to answer": a smaller outCap is a
    // caller contract breach and halts loudly (never a silent 0 = "deferred").
    //
    // buf and out MUST NOT overlap.
    // outCap is the capacity of out[] in bytes.
    // Never throws.  Never allocates from heap in steady state.

    // InitProducerId (templateId 45) — allocate or refresh a producer identity.
    virtual size_t handleInitProducerId (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap,
                                         const rune::coordination::NsRequestContext& ctx = {}) = 0;

    // AddPartitionsToTxn (templateId 47) — record partitions in the transaction.
    virtual size_t handleAddPartitions  (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap,
                                         const rune::coordination::NsRequestContext& ctx = {}) = 0;

    // AddOffsetsToTxn (templateId 49) — bind consumer-group offsets to the txn.
    virtual size_t handleAddOffsets     (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap,
                                         const rune::coordination::NsRequestContext& ctx = {}) = 0;

    // TxnOffsetCommit (templateId 51) — commit consumer offsets within the txn.
    virtual size_t handleTxnOffsetCommit(const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap,
                                         const rune::coordination::NsRequestContext& ctx = {}) = 0;

    // EndTxn (templateId 53) — begin commit or abort of the transaction.
    virtual size_t handleEndTxn         (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap,
                                         const rune::coordination::NsRequestContext& ctx = {}) = 0;

    // A COMMIT/ABORT control marker this node could not append (NotLeader,
    // MinIsrViolation, ...).  Never silently dropped and never "landed": the
    // coordinator retries it until it lands (or drops it if the partition is
    // gone).  Cold path; default no-op for test doubles.
    //
    // F15 m3: coordinatorEpoch is the epoch the marker was dispatched under
    // (kAnyCoordinatorEpoch when the gateway no longer knows it); a coordinator
    // ignores replies for an epoch it no longer serves.
    static constexpr int32_t kAnyCoordinatorEpoch = INT32_MIN;
    virtual void onControlMarkerRefused(int32_t /*topicId*/, int32_t /*partition*/,
                                        int64_t /*producerId*/, int16_t /*errorCode*/,
                                        int32_t /*coordinatorEpoch*/) {}

    // Task 10 round 5: an asynchronously appended marker LANDED (the gateway
    // already fed its authoritative offset to the LSO / aborted index).  The
    // coordinator completes the transaction only once every participant
    // partition's marker landed.  Cold path; default no-op for test doubles.
    virtual void onControlMarkerLanded(int32_t /*topicId*/, int32_t /*partition*/,
                                       int64_t /*producerId*/, int32_t /*coordinatorEpoch*/) {}

    // F15-FU1 (#112, KIP-890 part 1): may a transactional data batch of (producerId,
    // producerEpoch) open a transaction on (topicId, partition)?  Asked by a partition
    // leader for the first such batch of a producer with no open transaction there.
    // Returns a client ErrorCode: 0 = the partition is registered in the producer's
    // ONGOING transaction and that registration is durable; NotCoordinator /
    // ConcurrentTransactions = cannot answer now (retriable); ProducerFenced /
    // InvalidProducerEpoch / InvalidTxnState = never (non-retriable).  Synchronous,
    // control thread, cold (once per transaction and partition).  Default (test
    // doubles): NotCoordinator.
    static constexpr int16_t kVerifyNotCoordinator = 14;   // pc::ErrorCode::NotCoordinator
    virtual int16_t verifyTxnPartition(int64_t /*producerId*/, int16_t /*producerEpoch*/, int32_t /*topicId*/,
                                       int32_t /*partition*/, int64_t /*nowNs*/) {
        return kVerifyNotCoordinator;
    }
    // #112: true when this node coordinates transactions (verifyTxnPartition answers
    // here); otherwise coordinatorNode() names the node that does (-1 = unknown).
    virtual bool    coordinatesLocally() const { return false; }
    virtual int32_t coordinatorNode() const { return -1; }
    // #112: false while the cluster's metadata feature level predates the
    // verification (an older coordinator would never answer a forwarded ask): a
    // partition led here whose coordinator is REMOTE then admits the batch as
    // before #112.  A local coordinator always verifies.
    virtual bool    clusterVerifiesTxnPartitions() const { return true; }
};

} // namespace rune::gateway
