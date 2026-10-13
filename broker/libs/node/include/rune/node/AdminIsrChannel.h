#pragma once
// AdminIsrChannel.h — the `__catalog` replica's ISR channel (catalog history
// Task 6, ruling 7).
//
// The catalog replica is admin-thread-owned, but its ISR is committed by the
// controller (Kafka AlterPartition, the coordinator-partition path).  The
// controller channel (AeronPartitionTransport) is control-thread-owned, so the
// replica never calls it: its ISR channel is this ring adapter.  sendAlterIsr
// encodes the verbatim SBE AlterIsrMsg as ONE kA2cCatalogAlterIsr frame on the
// admin→control ring (producer = the admin thread, which drives the replica);
// the control thread decodes it and calls replTransport_->sendAlterIsr.
// Every other send is unused on an ISR channel and refuses (false).
//
// The controller's answer comes back the other way: the control thread posts
// the AlterIsrResponse VERBATIM (every field, leaderId included — the E2
// shard-mode lesson: a relay that dropped leaderId left it -1, so the replica
// never matched its own lineage / leaderless-report acks) as one
// kMsgCatalogAlterIsrResponse frame on the control→admin role ring.
//
// A full ring is never a silent drop: sendAlterIsr returns false (the replica
// counts the refusal and re-sends on its kAlterIsrResendNs cadence); a full role
// ring makes the control thread leave the Aeron fragment unconsumed (re-delivered).

#include <rune/node/AdminChannels.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/replication/PartitionReplica.h>     // PartitionTransport
#include <rune/replication/ReplicationMessages.h>  // AlterIsrMsg, AlterIsrResponseMsg

#include <cstddef>
#include <cstdint>

namespace rune::node {

class AdminIsrChannel final : public rune::replication::PartitionTransport {
public:
    explicit AdminIsrChannel(OneToOneByteRing& adminToControl) noexcept : ring_(adminToControl) {}

    AdminIsrChannel(const AdminIsrChannel&)            = delete;
    AdminIsrChannel& operator=(const AdminIsrChannel&) = delete;

    /// (Admin thread) propose an ISR change / epoch bump / leaderless report.
    bool sendAlterIsr(const rune::replication::AlterIsrMsg& msg) override {
        // tryClaim THROWS beyond the ring's max message (capacity / 8): refuse
        // instead (unreachable on the 1 MiB admin→control ring; never a throw
        // out of the replica's doWork).
        if (rune::replication::AlterIsrMsg::TOTAL_SIZE > static_cast<std::size_t>(ring_.maxMsgLength())) [[unlikely]] {
            ++refused_;
            return false;
        }
        const auto idx = ring_.tryClaim(kA2cCatalogAlterIsr, rune::replication::AlterIsrMsg::TOTAL_SIZE);
        if (idx < 0) {
            ++refused_;
            return false;
        }
        const std::size_t n = msg.encode(ring_.base() + idx, rune::replication::AlterIsrMsg::TOTAL_SIZE);
        if (n == 0) [[unlikely]] {          // unreachable: TOTAL_SIZE was claimed
            ring_.abort(idx);
            ++refused_;
            return false;
        }
        ring_.commit(idx);
        ++sent_;
        return true;
    }

    // Not a data transport: the catalog's data plane is its own
    // WorkerReplicationTransport.  Never called on an ISR channel.
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return false; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {
        return false;
    }

    [[nodiscard]] uint64_t sent() const noexcept { return sent_; }
    [[nodiscard]] uint64_t refused() const noexcept { return refused_; }

private:
    OneToOneByteRing& ring_;
    uint64_t sent_    = 0;
    uint64_t refused_ = 0;
};

/// (Control thread) decode a kA2cCatalogAlterIsr frame.  false = malformed.
[[nodiscard]] inline bool decodeCatalogAlterIsr(const uint8_t* p, std::size_t n,
                                                rune::replication::AlterIsrMsg& out) noexcept {
    return rune::replication::AlterIsrMsg::decode(p, n, out);
}

/// (Control thread) relay the controller's answer for (-6, 0) to the admin
/// thread, verbatim.  false = role ring full (the caller retries).
[[nodiscard]] inline bool encodeCatalogAlterIsrResponse(OneToOneByteRing& roleRing,
                                                        const rune::replication::AlterIsrResponseMsg& r) {
    if (rune::replication::AlterIsrResponseMsg::TOTAL_SIZE > static_cast<std::size_t>(roleRing.maxMsgLength()))
        [[unlikely]] return false;   // tryClaim would throw; unreachable on the 4 KiB role ring
    const auto idx = roleRing.tryClaim(kMsgCatalogAlterIsrResponse,
                                       rune::replication::AlterIsrResponseMsg::TOTAL_SIZE);
    if (idx < 0) return false;
    if (r.encode(roleRing.base() + idx, rune::replication::AlterIsrResponseMsg::TOTAL_SIZE) == 0) [[unlikely]] {
        roleRing.abort(idx);
        return false;
    }
    roleRing.commit(idx);
    return true;
}

/// (Admin thread) decode a kMsgCatalogAlterIsrResponse frame.  false = malformed.
[[nodiscard]] inline bool decodeCatalogAlterIsrResponse(const uint8_t* p, std::size_t n,
                                                        rune::replication::AlterIsrResponseMsg& out) noexcept {
    return rune::replication::AlterIsrResponseMsg::decode(p, n, out);
}

} // namespace rune::node
