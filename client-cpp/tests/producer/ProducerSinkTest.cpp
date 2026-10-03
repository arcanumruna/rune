// tests/producer/ProducerSinkTest.cpp — concrete (non-std::function) produce sinks
// and the flat partition table.  No Aeron: ProducerImpl is driven directly.
#include <rune/client/Key.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

using rune::client::Acks;
using rune::client::Key;
using rune::client::ProducerConfig;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;

namespace {
constexpr int64_t kNow = 1'000'000'000;

ProducerConfig immediateCfg() {
    ProducerConfig c;
    c.acks = Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 64 * 1024;
    c.maxInFlight = 8;
    return c;
}

template <class Sink>
int pump(ProducerImpl& impl, const Sink& sink) {
    int flushed = 0;
    for (int i = 0; i < 100'000; ++i) {
        const int d = impl.drainRing(kNow);
        flushed += impl.flushBatches(kNow, /*sessionId=*/42, sink);
        if (d == 0) break;
    }
    return flushed;
}
} // namespace

TEST(ProducerSink, ConcreteSinkSeesEveryFrame) {
    ProducerImpl impl(immediateCfg());
    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    const std::vector<uint8_t> v(100, 0x5A);
    for (int32_t p = 0; p < 4; ++p) {
        ASSERT_TRUE(impl.submitSendRecord(1, p, Key(static_cast<uint64_t>(p)), v, false, 0));
    }
    pump(impl, sink);
    EXPECT_EQ(frames, 4u);
    EXPECT_GT(bytes, 4u * 100u);
}

TEST(ProducerSink, ThousandPartitionsKeepTheirOwnStreams) {
    ProducerImpl impl(immediateCfg());
    for (int32_t p = 0; p < 1000; ++p) impl.updateIngressRoute(7, p, 5000 + p);
    std::map<int32_t, int32_t> streamByPartition;
    const ProducerImpl::SendFn sink = [&](int32_t, int32_t p, int32_t stream, const uint8_t*,
                                          std::size_t) {
        streamByPartition[p] = stream;
        return true;
    };
    const std::vector<uint8_t> v(8, 1);
    for (int32_t p = 0; p < 1000; ++p) {
        ASSERT_TRUE(impl.submitSendRecord(7, p, Key(static_cast<uint64_t>(p)), v, false, 0));
        pump(impl, sink);
    }
    ASSERT_EQ(streamByPartition.size(), 1000u);
    for (const auto& [p, stream] : streamByPartition) ASSERT_EQ(stream, 5000 + p);
}

// ---------------------------------------------------------------------------
// Backlog B1/B2/B4 (LAT-1 h/i): CRC only once the transport accepted the frame,
// incremental identity stamps, the retained frame IS the encoded frame.
// ---------------------------------------------------------------------------
#include <rune/client/transport/FrameCrc.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

namespace {
ProducerConfig ackedCfg() {
    ProducerConfig c;
    c.acks              = Acks::Leader;
    c.lingerUs          = 0;
    c.batchBytes        = 64 * 1024;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 600'000;
    return c;
}

// The Produce frame's RecordBatch: decodes, CRC valid; returns its record count (-1 = invalid).
int32_t validBatchRecords(const std::vector<uint8_t>& frame) {
    const auto p = rune::protocol::client::Produce::decode(frame.data(), frame.size());
    if (!p) return -1;
    rune::protocol::RecordBatchReader rd(p->recordBatch.data(), p->recordBatch.size());
    if (!rd.ensureValid()) return -1;
    if (rd.producerId() != p->producerId || rd.baseSequence() != p->baseSequence ||
        rd.producerEpoch() != p->producerEpoch) {
        return -1;
    }
    return rd.recordCount();
}
}  // namespace

TEST(ProducerSinkRegression, BackPressuredBatchCrcIsComputedOnlyOnAcceptRegression) {
    // Saturation profile (LAT-1 h): ~6.6 full rebuilds (header + record copy +
    // CRC) per sent frame under back-pressure.  Now a refused offer must reach
    // the sink with its CRC still pending, and the CRC is computed exactly once.
    ProducerImpl impl(ackedCfg());
    uint64_t frames = 0, bytes = 0, seals = 0, unsealed = 0;
    int left = 5;
    int64_t corr = 0;
    std::vector<uint8_t> last;
    CountingSink sink{&frames, &bytes, 5, &left, &corr};
    sink.seals = &seals;
    sink.unsealedRefusals = &unsealed;
    sink.lastFrame = &last;
    const std::vector<uint8_t> v(1024, 0x42);
    for (int i = 0; i < 50; ++i) ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), v, false, 0));
    ASSERT_EQ(impl.drainRing(kNow, 100), 50);
    for (int r = 0; r < 6; ++r) impl.flushBatches(kNow + r, 42, sink);
    EXPECT_EQ(frames, 1u);
    EXPECT_EQ(unsealed, 5u) << "every refused offer must carry an UNSEALED frame (no CRC computed)";
    EXPECT_EQ(seals, 1u) << "the CRC is computed exactly once, on accept";
    EXPECT_EQ(validBatchRecords(last), 50);
}

TEST(ProducerSinkCrc, RecordsJoinTheHeldBatchWhileBackPressured) {
    ProducerImpl impl(ackedCfg());
    uint64_t frames = 0, bytes = 0, seals = 0;
    int left = 1;
    std::vector<uint8_t> last;
    CountingSink sink{&frames, &bytes, 1, &left};
    sink.seals = &seals;
    sink.lastFrame = &last;
    const std::vector<uint8_t> v(300, 0x17);
    for (int i = 0; i < 10; ++i) ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), v, false, 0));
    impl.drainRing(kNow, 100);
    impl.flushBatches(kNow, 42, sink);  // refused
    ASSERT_EQ(frames, 0u);
    for (int i = 10; i < 25; ++i) ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), v, false, 0));
    impl.drainRing(kNow, 100);
    impl.flushBatches(kNow + 1, 42, sink);  // accepted: the grown batch
    ASSERT_EQ(frames, 1u);
    EXPECT_EQ(seals, 1u);
    EXPECT_EQ(validBatchRecords(last), 25);
}

TEST(ProducerSinkCrc, RetransmitReSendsTheSealedAcceptedFrame) {
    // B4: the retained retransmit frame is the encoded frame (swapped, not
    // copied) and carries the CRC the sink sealed on accept.
    ProducerImpl impl(ackedCfg());
    uint64_t frames = 0, bytes = 0;
    std::vector<uint8_t> accepted;
    CountingSink sink{&frames, &bytes};
    sink.lastFrame = &accepted;
    const std::vector<uint8_t> v(700, 0x33);
    for (int i = 0; i < 40; ++i) ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t(i)), v, false, 0));
    impl.drainRing(kNow, 100);
    ASSERT_EQ(impl.flushBatches(kNow, 42, sink), 1);
    ASSERT_EQ(validBatchRecords(accepted), 40);

    std::vector<uint8_t> resent;
    const ProducerImpl::SendFn capture = [&](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
        resent.assign(b, b + n);
        return true;
    };
    ASSERT_EQ(impl.retransmitStalled(kNow + 2'000'000'000LL, capture), 1);
    ASSERT_EQ(resent.size(), accepted.size());
    EXPECT_EQ(validBatchRecords(resent), 40);
    // Identical except the attempt tag inside the correlation id (bytes 16..23).
    for (std::size_t i = 0; i < resent.size(); ++i) {
        if (i >= 16 && i < 24) continue;
        ASSERT_EQ(resent[i], accepted[i]) << "byte " << i;
    }
}

TEST(ProducerSinkCrc, Tier2StampKeepsTheCrcValidAcrossRetries) {
    // B2: the Tier-2 identity stamp (repeated on every back-pressured retry)
    // keeps the pre-built batch's CRC valid without a rescan.
    ProducerImpl impl(ackedCfg());
    rune::protocol::RecordBatchBuilder b(0, 123);
    for (int i = 0; i < 30; ++i) b.append("key", std::string(400, char('a' + i % 26)), 200 + i);
    std::vector<uint8_t> batch;
    b.buildInto(batch);
    ASSERT_EQ(impl.submitTier2Batch(1, 0, 0, 30, batch), rune::client::producer::RejectReason::None);
    uint64_t frames = 0, bytes = 0;
    int left = 3;
    std::vector<uint8_t> last;
    CountingSink sink{&frames, &bytes, 3, &left};
    sink.lastFrame = &last;
    impl.drainRing(kNow, 10);
    for (int r = 0; r < 4; ++r) impl.flushBatches(kNow + r, 42, sink);
    ASSERT_EQ(frames, 1u);
    EXPECT_EQ(validBatchRecords(last), 30);
    const auto p = rune::protocol::client::Produce::decode(last.data(), last.size());
    ASSERT_TRUE(p);
    EXPECT_EQ(p->producerId, impl.producerId());
    EXPECT_EQ(p->baseSequence, 0);
}
