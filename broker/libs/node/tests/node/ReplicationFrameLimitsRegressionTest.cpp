// ReplicationFrameLimitsRegressionTest.cpp — authentication plan Task 16 (D34): every inter-broker frame limit leaves
// room for the 32-byte peer frame (header 16 + tag 16) in EVERY mode, so the limits never change with the crypto
// mode: the largest frame each limit admits, sealed, still fits the transport's max message.
#include <rune/node/NodeConfig.h>
#include <rune/peerlink/PeerFrameCrypto.h>
#include <rune/replication/ReplicationFrameLimits.h>
#include <rune/replication/WorkerReplicationTransport.h>

#include <gtest/gtest.h>

namespace rr = rune::replication;
namespace pl = rune::peerlink;

TEST(ReplicationFrameLimitsRegression, EveryLimitLeavesRoomForThePeerFrameOverhead) {
    static_assert(pl::kPeerFrameOverhead == 32);
    for (const int64_t term : {64LL * 1024, 256LL * 1024, 1LL << 20, 16LL << 20, 128LL << 20}) {
        const int64_t records = rr::maxFetchRecordsViaPublication(term);
        ASSERT_GT(records, 0) << "term " << term;
        const int64_t frame = static_cast<int64_t>(rr::FetchResponseMsg::encodedSize(static_cast<std::size_t>(records)));
        EXPECT_LE(frame + static_cast<int64_t>(pl::kPeerFrameOverhead), rr::aeronMaxMessageLength(term))
            << "the largest FetchResponse the publication limit admits, sealed, must fit one Aeron message (term " << term << ")";
        EXPECT_EQ(rr::WorkerReplicationTransport::maxFetchResponseBytesFor(static_cast<int32_t>(std::min<int64_t>(term, INT32_MAX))),
                  static_cast<int32_t>(std::min<int64_t>(records, INT32_MAX)))
            << "the worker plane's static ceiling is the same formula";
    }
    for (const int64_t ring : {1LL << 20, 8LL << 20, 32LL << 20}) {
        const int64_t records = rr::maxFetchRecordsViaOutboundRing(ring);
        ASSERT_GT(records, 0) << "ring " << ring;
        const int64_t frame = static_cast<int64_t>(rr::FetchResponseMsg::encodedSize(static_cast<std::size_t>(records)));
        EXPECT_LE(static_cast<int64_t>(rr::kOutboundEnvelopeOverhead) + frame + static_cast<int64_t>(pl::kPeerFrameOverhead), ring / 8)
            << "the outbound-ring hop leaves the same room (ring " << ring << ")";
    }
}

TEST(ReplicationFrameLimitsRegression, TheLimitsShrinkByExactlyThePeerOverheadInEveryMode) {
    const int64_t term = 1LL << 20;
    const int64_t unsealed = rr::aeronMaxMessageLength(term) - static_cast<int64_t>(rr::FetchResponseMsg::encodedSize(0));
    EXPECT_EQ(rr::maxFetchRecordsViaPublication(term), unsealed - 32);
    const int64_t ring = 32LL << 20;
    const int64_t ringOld = ring / 8 - static_cast<int64_t>(rr::kOutboundEnvelopeOverhead) - static_cast<int64_t>(rr::FetchResponseMsg::encodedSize(0));
    EXPECT_EQ(rr::maxFetchRecordsViaOutboundRing(ring), ringOld - 32);
}

TEST(ReplicationFrameLimitsRegression, ThePlaneCeilingSubtractsThePeerOverheadWithoutSealing) {
    // A plane built PeerPlain keeps the same ceiling as a sealed one: limits never depend on the mode.
    EXPECT_EQ(rr::WorkerReplicationTransport::maxFetchResponseBytesFor(1 << 20),
              static_cast<int32_t>(rr::maxFetchRecordsViaPublication(1 << 20)));
}

TEST(ReplicationFrameLimitsRegression, ProduceIntakeLimitShrinksByAtMostThePeerOverhead) {
    // The intake limit is bounded by these transport limits (NodeConfig::produceIntakeLimit); it moved by <= 32 B.
    rune::node::NodeConfig cfg;
    cfg.aeronTermBufferBytes = 1 << 20;
    const int64_t before = std::min(rr::maxFetchRecordsViaOutboundRing(cfg.replicationOutboundRingBytes) + 32,
                                    rr::maxFetchRecordsViaPublication(cfg.aeronTermBufferBytes) + 32);
    const int64_t after  = std::min(rr::maxFetchRecordsViaOutboundRing(cfg.replicationOutboundRingBytes),
                                    rr::maxFetchRecordsViaPublication(cfg.aeronTermBufferBytes));
    EXPECT_EQ(before - after, 32);
    EXPECT_LE(cfg.produceIntakeLimit(), before) << "never above the pre-Task-16 transport bound";
    EXPECT_GE(cfg.produceIntakeLimit(), 0);
}
