// tests/producer/ProducerAcksFlagsTest.cpp - checked writes Part 03: Acks as a flag set, creation checks, the wire
// byte, the broker-version gate (BrokerTooOld) and the 4096-record View batch.
#include "../support/AckLedger.h"
#include "../support/FrameCapture.h"

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/shard/ShardConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::Acks;
using rc::Key;
using rc::OpStatus;
using rc::ProducerConfig;
using rc::SendResult;
using rc::producer::ProducerImpl;
using test_support::AckLedger;

namespace {

constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);

int g_ctr = 0;
rc::ClientConfig offlineCfg() {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = 4611;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = 4612;
    c.dataDir  = std::filesystem::temp_directory_path().string() + "/acks_flags_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    return c;
}

ProducerConfig cfgOf(Acks acks, int32_t batchRecords = 0) {
    ProducerConfig c;
    c.acks             = acks;
    c.lingerUs         = 0;
    c.batchBytes       = 1 << 20;
    c.maxFrameBytes    = 1 << 20;
    c.batchRecords     = batchRecords;
    c.maxInFlight      = 8;
    c.produceTimeoutMs = 1'000;
    c.deliveryTimeoutMs = 10'000;
    return c;
}

struct Frames {
    std::vector<uint8_t> acks;
    std::vector<int32_t> records;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rc::codec::decodeProduce(b, n);
            if (!v) return false;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            acks.push_back(v->acks);
            records.push_back(r.recordCount());
            return true;
        };
    }
};

void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    while (impl.drainRing(kNow) != 0) {}
    (void)impl.flushBatches(kNow, 42, sink);
}

}  // namespace

TEST(CheckedWritesAcks, FlagOperatorsAndHas) {
    EXPECT_EQ(static_cast<uint8_t>(Acks::All | Acks::View), 6);
    EXPECT_TRUE(has(Acks::All | Acks::View, Acks::View));
    EXPECT_FALSE(has(Acks::Leader, Acks::All));
    EXPECT_EQ((Acks::Leader | Acks::View) & Acks::View, Acks::View);
    EXPECT_FALSE(has(Acks::All, Acks::None));
    EXPECT_EQ(normalized(Acks::Leader | Acks::All), Acks::All);
    EXPECT_EQ(normalized(Acks::Leader | Acks::All | Acks::View), Acks::All | Acks::View);
    EXPECT_EQ(normalized(Acks::Leader | Acks::View), Acks::Leader | Acks::View);
}

// Matrix C: 10 Acks values x {plain, transactional, shard}.
enum class Kind : uint8_t { Plain, Transactional, Shard };
class CheckedWritesAcksMatrix : public ::testing::TestWithParam<std::tuple<uint8_t, Kind>> {};

TEST_P(CheckedWritesAcksMatrix, CreationAndWireByte) {
    const Acks acks = static_cast<Acks>(std::get<0>(GetParam()));
    const Kind kind = std::get<1>(GetParam());
    const bool valid = acksValid(acks);
    const bool noneTxn = kind == Kind::Transactional && acks == Acks::None;
    const bool expectThrow = !valid || noneTxn;

    if (kind == Kind::Shard) {
        rc::shard::ShardGroupConfig g;
        g.producer.acks = acks;
        if (expectThrow) {
            EXPECT_THROW((void)rc::shard::shardProducerConfig(g), std::invalid_argument);
            EXPECT_THROW(rc::shard::validateShardGroupConfig(g, 1), std::invalid_argument);
        } else {
            EXPECT_NO_THROW((void)rc::shard::shardProducerConfig(g));
            EXPECT_NO_THROW(rc::shard::validateShardGroupConfig(g, 1));
        }
    } else {
        rc::Client client(offlineCfg());
        ProducerConfig cfg = cfgOf(acks);
        if (kind == Kind::Transactional) cfg.transactionalId = "tx-acks";
        if (expectThrow) EXPECT_THROW((void)client.producer(cfg), std::invalid_argument);
        else EXPECT_NO_THROW((void)client.producer(cfg));
    }
    if (expectThrow || acks == Acks::None || kind != Kind::Plain) return;

    // The wire byte of a plain producer: Leader 0, All 1, Leader|View 2, All|View 3.
    ProducerImpl impl(cfgOf(acks), 1 << 16);
    Frames cap;
    const auto sink = cap.fn();
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    pump(impl, sink);
    ASSERT_EQ(cap.acks.size(), 1u);
    const bool all = has(acks, Acks::All), view = has(acks, Acks::View);
    EXPECT_EQ(cap.acks[0], (all ? 1 : 0) | (view ? 2 : 0));
}

INSTANTIATE_TEST_SUITE_P(
    All, CheckedWritesAcksMatrix,
    ::testing::Combine(::testing::Values(uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{4}, uint8_t{5}, uint8_t{6},
                                         uint8_t{3}, uint8_t{7}, uint8_t{0x08}, uint8_t{0x80}),
                       ::testing::Values(Kind::Plain, Kind::Transactional, Kind::Shard)));

TEST(CheckedWritesAcksRegression, ViewProducerOnAnOldBrokerFailsLocallyBrokerTooOldRegression) {
    ProducerImpl impl(cfgOf(Acks::All | Acks::View), 1 << 16);
    AckLedger L;
    L.attach(impl);
    Frames cap;
    const auto sink = cap.fn();
    impl.setBrokerVersion(rpc::kBrokerVersionCheckedWrites - 1);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, true, AckLedger::hi(1)));
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{2}), kVal, true, AckLedger::hi(2)));
    pump(impl, sink);
    EXPECT_TRUE(cap.acks.empty()) << "nothing is offered to an old broker";
    EXPECT_TRUE(L.exactlyOnce(1, 3));
    EXPECT_EQ(L.status(1), OpStatus::BrokerTooOld);
    EXPECT_EQ(L.status(2), OpStatus::BrokerTooOld);
    EXPECT_FALSE(impl.hasInFlight());

    impl.setBrokerVersion(rpc::kBrokerVersionCheckedWrites);
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{3}), kVal, true, AckLedger::hi(3)));
    ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(uint64_t{4}), kVal, true, AckLedger::hi(4)));
    pump(impl, sink);
    ASSERT_FALSE(cap.acks.empty());
    for (uint8_t a : cap.acks) EXPECT_EQ(a, 3);

    // 0 = unknown sends; an All-only producer is never gated.
    ProducerImpl unknown(cfgOf(Acks::All | Acks::View), 1 << 16);
    Frames cap2;
    const auto sink2 = cap2.fn();
    unknown.setBrokerVersion(0);
    ASSERT_TRUE(unknown.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    pump(unknown, sink2);
    EXPECT_EQ(cap2.acks.size(), 1u);
    ProducerImpl allOnly(cfgOf(Acks::All), 1 << 16);
    Frames cap3;
    const auto sink3 = cap3.fn();
    allOnly.setBrokerVersion(6);
    ASSERT_TRUE(allOnly.submitSendRecord(1, 0, Key(uint64_t{1}), kVal, false, 0));
    pump(allOnly, sink3);
    ASSERT_EQ(cap3.acks.size(), 1u);
    EXPECT_EQ(cap3.acks[0], 1);
}

TEST(CheckedWritesAcks, ViewProducerFlushesAt4096Records) {
    {
        ProducerImpl impl(cfgOf(Acks::All | Acks::View), 1 << 22);
        Frames cap;
        const auto sink = cap.fn();
        for (uint64_t i = 0; i < 4100; ++i) ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(i), kVal, false, 0)) << i;
        pump(impl, sink);
        ASSERT_EQ(cap.records.size(), 1u) << "one frame per partition in flight; the 4 left over wait for its ack";
        EXPECT_EQ(cap.records[0], 4096);
        EXPECT_EQ(cap.acks[0], 3);
    }
    {
        ProducerImpl impl(cfgOf(Acks::All | Acks::View, 100), 1 << 22);
        Frames cap;
        const auto sink = cap.fn();
        for (uint64_t i = 0; i < 250; ++i) {
            ASSERT_TRUE(impl.submitSendRecord(1, 0, Key(i), kVal, false, 0));
            pump(impl, sink);
        }
        ASSERT_FALSE(cap.records.empty());
        for (int32_t n : cap.records) EXPECT_LE(n, 100);
    }
    {
        ProducerImpl plain(cfgOf(Acks::All), 1 << 22);
        Frames cap;
        const auto sink = cap.fn();
        for (uint64_t i = 0; i < 4100; ++i) ASSERT_TRUE(plain.submitSendRecord(1, 0, Key(i), kVal, false, 0));
        pump(plain, sink);
        EXPECT_TRUE(cap.records.empty() || cap.records[0] > 4096) << "an All producer has no 4096 cap";
    }
}

TEST(CheckedWritesAcks, ViewBatchesAreBoundedForClaimsAndTier2) {
    ProducerImpl view(cfgOf(Acks::All | Acks::View), 1 << 28);
    EXPECT_EQ(view.claimBatchShape(Key(uint64_t{1}), 4096, 8), SendResult::Ok);
    EXPECT_EQ(view.claimBatchShape(Key(uint64_t{1}), 4097, 8), SendResult::MessageTooLarge);
    ProducerImpl plain(cfgOf(Acks::All), 1 << 28);
    EXPECT_EQ(plain.claimBatchShape(Key(uint64_t{1}), 4097, 8), SendResult::Ok);

    rc::Client client(offlineCfg());
    auto p = client.producer(cfgOf(Acks::All | Acks::View));
    auto b = p.batch(rc::TopicHandle{7, 4}, Key(uint64_t{1}));
    const std::vector<uint8_t> v(1, 1);
    for (int i = 0; i < 4096; ++i) b.add(v);
    EXPECT_THROW(b.add(v), std::length_error);
}

TEST(CheckedWritesResults, AppendOnlyValues) {
    EXPECT_EQ(static_cast<int>(SendResult::WriteNotAllowed), static_cast<int>(SendResult::KeyTooLong) + 1);
    EXPECT_EQ(static_cast<int>(OpStatus::BrokerTooOld), static_cast<int>(OpStatus::InvalidArgument) + 1);
    rc::Completion c;
    c.reserved0 = 0x0007'0001;
    c.aux       = 42;
    EXPECT_EQ(rc::constraintIdOf(c), 0x0007'0001u);
    EXPECT_EQ(rc::assignedKeyOf(c), 42u);
}
