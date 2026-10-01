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

#include <cstddef>
#include <cstdint>

namespace rune::gateway {

struct ITxnRequestHandler {
    virtual ~ITxnRequestHandler() = default;

    // Each method decodes the request from buf[0..len-1], acts, encodes the
    // response into out[0..outCap-1], and returns the number of bytes written
    // (0 on decode error or unrecoverable failure).
    //
    // buf and out MUST NOT overlap.
    // outCap is the capacity of out[] in bytes.
    // Never throws.  Never allocates from heap in steady state.

    // InitProducerId (templateId 45) — allocate or refresh a producer identity.
    virtual size_t handleInitProducerId (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap) = 0;

    // AddPartitionsToTxn (templateId 47) — record partitions in the transaction.
    virtual size_t handleAddPartitions  (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap) = 0;

    // AddOffsetsToTxn (templateId 49) — bind consumer-group offsets to the txn.
    virtual size_t handleAddOffsets     (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap) = 0;

    // TxnOffsetCommit (templateId 51) — commit consumer offsets within the txn.
    virtual size_t handleTxnOffsetCommit(const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap) = 0;

    // EndTxn (templateId 53) — begin commit or abort of the transaction.
    virtual size_t handleEndTxn         (const uint8_t* buf, size_t len,
                                         int64_t nowNs,
                                         uint8_t* out, size_t outCap) = 0;

    // Task 10 fix 3: a COMMIT/ABORT control marker this node could not append
    // (the partition is no longer here: NotLeader).  Never silently dropped —
    // the coordinator is told.  Cold path; default no-op for test doubles.
    virtual void onControlMarkerRefused(int32_t /*topicId*/, int32_t /*partition*/,
                                        int64_t /*producerId*/, int16_t /*errorCode*/) {}

    // Task 10 round 5: an asynchronously appended marker LANDED (the gateway
    // already fed its authoritative offset to the LSO / aborted index).  The
    // coordinator completes the transaction only once every participant
    // partition's marker landed.  Cold path; default no-op for test doubles.
    virtual void onControlMarkerLanded(int32_t /*topicId*/, int32_t /*partition*/,
                                       int64_t /*producerId*/) {}
};

} // namespace rune::gateway
