// tests/producer/ClientPerWorkerRoutingTest.cpp
//
// E5.1: Client per-partition ingress-stream routing tests.
//
// Tests (no Aeron / no real transport — pure unit test on ProducerImpl):
//
//   PWR1: PerPartitionStreamSelection
//         metadata assigns partition 0 → stream S0, partition 1 → stream S1;
//         after updateIngressRoute, produce to both — assert each Produce frame
//         is sent with the correct ingressStreamId.
//
//   PWR2: RedirectPreservesStream
//         produce to partition 0 (stream S0); simulate a REDIRECT by calling
//         updateIngressRoute with the SAME stream id but (conceptually) a new
//         channel (channel change is transport-level, not ProducerImpl-level);
//         assert that the next produce for partition 0 still uses S0.
//
//   PWR3: FallbackToLegacyStream
//         produce WITHOUT calling updateIngressRoute (so ingressStreamId == 0);
//         assert the sendFn receives ingressStreamId == 0 (legacy/default path).
//
//   PWR4: MetadataCacheRoundTrip
//         encode a MetadataResponse with ingressStreamId fields, decode it, and
//         verify MetadataCache.ingressStreamIdFor() returns the correct stream
//         for each partition.  (No Aeron; wire-level round-trip only.)

#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/metadata/MetadataCache.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/Key.h>

#include <rune/protocol/client/MetadataMessages.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <span>
#include <vector>

namespace rprod = rune::client::producer;
namespace mc    = rune::client::metadata;
namespace rcc   = rune::client::codec;
namespace rpc   = rune::protocol::client;

using rune::client::Key;

// ---------------------------------------------------------------------------
// Helpers — fake sendFn that records (ingressStreamId, frame bytes)
// ---------------------------------------------------------------------------

struct FakeSend {
    struct Record {
        int32_t             topicId;
        int32_t             partition;
        int32_t             streamId;
        std::vector<uint8_t> frame;
    };

    std::vector<Record> calls;

    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> fn() {
        return [this](int32_t topicId, int32_t partition, int32_t sid,
                      const uint8_t* buf, std::size_t len) -> bool {
            Record r;
            r.topicId   = topicId;
            r.partition = partition;
            r.streamId  = sid;
            r.frame.assign(buf, buf + len);
            calls.push_back(std::move(r));
            return true;  // always accept
        };
    }

    /// Return the last ingressStreamId offered, or -1 if none.
    int32_t lastStreamId() const {
        if (calls.empty()) return -1;
        return calls.back().streamId;
    }

    /// Return the ingressStreamId of all offers for a given call index.
    int32_t streamIdAt(std::size_t i) const {
        if (i >= calls.size()) return -1;
        return calls[i].streamId;
    }

    void clear() { calls.clear(); }
};

// ---------------------------------------------------------------------------
// Build a minimal ProducerImpl with default ProducerConfig.
// Uses a shared atomic corrId counter (as in production).
// ---------------------------------------------------------------------------


static std::unique_ptr<rprod::ProducerImpl> makeProducerImpl(
    rune::client::Acks acks = rune::client::Acks::None)
{
    rune::client::ProducerConfig cfg;
    cfg.acks           = acks;
    cfg.lingerUs       = 1;      // flush immediately
    cfg.batchBytes     = 1;      // flush on any byte
    cfg.maxInFlight    = 8;
    cfg.produceTimeoutMs = 30000;
    return std::make_unique<rprod::ProducerImpl>(cfg);
}

// ---------------------------------------------------------------------------
// Drive one drain + flush cycle synchronously (no Aeron, injected clock).
// Returns number of batches flushed.
// ---------------------------------------------------------------------------

static int drainAndFlush(rprod::ProducerImpl& impl,
                          FakeSend&            fake,
                          int64_t              sessionId = 42)
{
    const int64_t nowNs = 1'000'000'000LL; // arbitrary past time → triggers linger
    impl.drainRing(nowNs);
    return impl.flushBatches(nowNs, sessionId, fake.fn());
}

// ===========================================================================
// PWR1: Per-partition stream selection
// ===========================================================================

TEST(ClientPerWorkerRouting, PerPartitionStreamSelection) {
    constexpr int32_t kTopic = 1;
    constexpr int32_t kS0    = 200;
    constexpr int32_t kS1    = 201;

    auto impl = makeProducerImpl();

    // Register per-worker routes BEFORE producing.
    impl->updateIngressRoute(kTopic, 0, kS0);
    impl->updateIngressRoute(kTopic, 1, kS1);

    FakeSend fake;

    // Produce to partition 0.
    const std::vector<uint8_t> val = {0x01};
    impl->submitSendRecord(kTopic, 0, Key{}, std::span<const uint8_t>(val),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);

    ASSERT_EQ(fake.calls.size(), 1u) << "expected one frame for partition 0";
    EXPECT_EQ(fake.lastStreamId(), kS0)
        << "partition 0 must use stream S0=" << kS0;

    // Produce to partition 1.
    fake.clear();
    const std::vector<uint8_t> val2 = {0x02};
    impl->submitSendRecord(kTopic, 1, Key{}, std::span<const uint8_t>(val2),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);

    ASSERT_EQ(fake.calls.size(), 1u) << "expected one frame for partition 1";
    EXPECT_EQ(fake.lastStreamId(), kS1)
        << "partition 1 must use stream S1=" << kS1;
}

// ===========================================================================
// PWR2: REDIRECT preserves per-partition stream id
//
// Simulate a REDIRECT: the channel changes (transport-level concern, not
// modelled here), but the ingressStreamId for the partition stays the same.
// updateIngressRoute is called AGAIN with the same streamId (as would happen
// when a fresh MetadataResponse arrives after the redirect, or when the
// existing streamId is re-confirmed).  The produce must still use S0.
// ===========================================================================

TEST(ClientPerWorkerRouting, RedirectPreservesStream) {
    constexpr int32_t kTopic = 2;
    constexpr int32_t kS0    = 300;

    auto impl = makeProducerImpl();
    impl->updateIngressRoute(kTopic, 0, kS0);

    FakeSend fake;

    // First produce.
    const std::vector<uint8_t> val = {0xAA};
    impl->submitSendRecord(kTopic, 0, Key{}, std::span<const uint8_t>(val),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);
    ASSERT_EQ(fake.calls.size(), 1u);
    EXPECT_EQ(fake.lastStreamId(), kS0) << "before redirect: stream S0";

    // Simulate REDIRECT: channel changes but stream id is preserved.
    // In production: AeronTransport::retarget() clears pubCache_ and
    // the stream id is stable (worker stream = partition-local, not node-global).
    // Here we re-call updateIngressRoute with the SAME stream id (the
    // MetadataResponse after the redirect still advertises the same worker stream
    // for this partition — the worker's stream id doesn't change on failover).
    impl->updateIngressRoute(kTopic, 0, kS0);  // same streamId

    fake.clear();
    const std::vector<uint8_t> val2 = {0xBB};
    impl->submitSendRecord(kTopic, 0, Key{}, std::span<const uint8_t>(val2),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);
    ASSERT_EQ(fake.calls.size(), 1u);
    EXPECT_EQ(fake.lastStreamId(), kS0) << "after redirect: stream S0 preserved";
}

// ===========================================================================
// PWR3: Fallback to legacy stream (ingressStreamId == 0)
//
// When no updateIngressRoute has been called (old broker / v1 protocol),
// the produce must use ingressStreamId == 0 (the default stream).
// ===========================================================================

TEST(ClientPerWorkerRouting, FallbackToLegacyStream) {
    constexpr int32_t kTopic = 3;

    auto impl = makeProducerImpl();
    // No updateIngressRoute call → ingressStreamId defaults to 0.

    FakeSend fake;

    const std::vector<uint8_t> val = {0xFF};
    impl->submitSendRecord(kTopic, 0, Key{}, std::span<const uint8_t>(val),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);

    ASSERT_EQ(fake.calls.size(), 1u) << "expected one frame";
    EXPECT_EQ(fake.lastStreamId(), 0)
        << "no ingressStreamId set → must use legacy stream 0";
}

// ===========================================================================
// PWR4: MetadataCache round-trip with ingressStreamId
//
// Encode a MetadataResponse with per-worker ingressStreamId fields, decode it
// through the Codec layer, populate MetadataCache, and verify
// ingressStreamIdFor() returns the correct streams.
// ===========================================================================

TEST(ClientPerWorkerRouting, MetadataCacheRoundTrip) {
    // Build a wire MetadataResponse with two partitions, each with distinct
    // ingressStreamIds.
    std::vector<rpc::BrokerEntry>    rpBrokers;
    std::vector<rpc::PartitionEntry> rpParts;

    {
        rpc::BrokerEntry b0;
        b0.nodeId = 0; b0.host = "w0"; b0.port = 9000;
        rpBrokers.push_back(b0);

        rpc::BrokerEntry b1;
        b1.nodeId = 1; b1.host = "w1"; b1.port = 9001;
        rpBrokers.push_back(b1);
    }
    {
        rpc::PartitionEntry p0;
        p0.topicId = 10; p0.partition = 0;
        p0.leaderId = 0; p0.leaderEpoch = 1;
        p0.ingressStreamId = 500;  // worker 0's stream
        rpParts.push_back(p0);

        rpc::PartitionEntry p1;
        p1.topicId = 10; p1.partition = 1;
        p1.leaderId = 1; p1.leaderEpoch = 1;
        p1.ingressStreamId = 501;  // worker 1's stream
        rpParts.push_back(p1);
    }

    rpc::MetadataResponse msg;
    msg.sessionId     = 1;
    msg.correlationId = 42;
    msg.brokers       = std::span<const rpc::BrokerEntry>(rpBrokers);
    msg.partitions    = std::span<const rpc::PartitionEntry>(rpParts);

    std::vector<uint8_t> buf(4096);
    const std::size_t n = msg.encode(buf.data());
    ASSERT_GT(n, 0u) << "MetadataResponse encode failed";

    // Decode through Codec.
    auto decoded = rcc::decodeMetadataResponse(buf.data(), n);
    ASSERT_TRUE(decoded.has_value()) << "decodeMetadataResponse failed";

    // Verify ingressStreamId is propagated through the Codec layer.
    ASSERT_EQ(decoded->partitions.size(), 2u);
    EXPECT_EQ(decoded->partitions[0].ingressStreamId, 500);
    EXPECT_EQ(decoded->partitions[1].ingressStreamId, 501);

    // Populate MetadataCache.
    mc::MetadataCache cache;
    std::vector<mc::BrokerEntry>    cb;
    std::vector<mc::PartitionEntry> cp;

    for (const auto& b : decoded->brokers) {
        mc::BrokerEntry e;
        e.nodeId = b.nodeId; e.port = b.port; e.host = std::string(b.host);
        cb.push_back(std::move(e));
    }
    for (const auto& p : decoded->partitions) {
        mc::PartitionEntry e;
        e.topicId        = p.topicId;
        e.partition      = p.partition;
        e.leaderId       = p.leaderId;
        e.leaderEpoch    = p.leaderEpoch;
        e.ingressStreamId = p.ingressStreamId;
        cp.push_back(e);
    }
    cache.updateFromResponse(cb, cp);

    // Verify MetadataCache stores and returns the ingressStreamIds.
    EXPECT_EQ(cache.ingressStreamIdFor(10, 0), 500)
        << "partition 0 must map to stream 500";
    EXPECT_EQ(cache.ingressStreamIdFor(10, 1), 501)
        << "partition 1 must map to stream 501";

    // Verify leaders are correct too.
    EXPECT_EQ(cache.leaderFor(10, 0), 0);
    EXPECT_EQ(cache.leaderFor(10, 1), 1);

    // Verify that a zero-RTT leader update PRESERVES the ingressStreamId.
    cache.updatePartitionLeader(10, 0, 1, 5);
    EXPECT_EQ(cache.ingressStreamIdFor(10, 0), 500)
        << "zero-RTT leader update must preserve ingressStreamId";
    EXPECT_EQ(cache.leaderFor(10, 0), 1)
        << "zero-RTT update must flip the leader";
}

// ===========================================================================
// PWR5: ProducerImpl stream update after metadata arrival
//
// Simulate the sequence:
//   1. Produce without route → uses stream 0.
//   2. updateIngressRoute with S0.
//   3. Produce again → uses S0.
// This models the real sequence: first produce before metadata arrives
// (legacy fallback), then metadata populates the route.
// ===========================================================================

TEST(ClientPerWorkerRouting, StreamUpdatesAfterMetadata) {
    constexpr int32_t kTopic = 4;
    constexpr int32_t kS0    = 400;

    auto impl = makeProducerImpl();
    FakeSend fake;

    // Before metadata: uses default stream 0.
    const std::vector<uint8_t> val = {0x10};
    impl->submitSendRecord(kTopic, 0, Key{}, std::span<const uint8_t>(val),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);
    ASSERT_EQ(fake.calls.size(), 1u);
    EXPECT_EQ(fake.lastStreamId(), 0) << "before metadata: legacy stream 0";

    // Metadata arrives: update route.
    impl->updateIngressRoute(kTopic, 0, kS0);

    fake.clear();
    const std::vector<uint8_t> val2 = {0x11};
    impl->submitSendRecord(kTopic, 0, Key{}, std::span<const uint8_t>(val2),
                            /*wantAck=*/false, 0);
    drainAndFlush(*impl, fake);
    ASSERT_EQ(fake.calls.size(), 1u);
    EXPECT_EQ(fake.lastStreamId(), kS0) << "after metadata: per-worker stream S0";
}
