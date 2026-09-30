// tests/producer/ProducerSinkTest.cpp — concrete (non-std::function) produce sinks
// and the flat partition table.  No Aeron: ProducerImpl is driven directly.
#include <rune/client/Key.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <map>
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
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(immediateCfg(), corr);
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
    std::atomic<int64_t> corr{1};
    ProducerImpl impl(immediateCfg(), corr);
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
