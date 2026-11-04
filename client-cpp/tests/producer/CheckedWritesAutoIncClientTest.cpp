// tests/producer/CheckedWritesAutoIncClientTest.cpp - checked writes Part 06 Task 5: modulo routing, sticky and
// homogeneous key-0 batches, the autoincrement local refusals, the old-broker gate, and the partitioner a typed open carries.
#include "../support/AckLedger.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <poll.h>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::Acks;
using rc::Key;
using rc::OpStatus;
using rc::SendResult;
using rc::TopicHandle;
using rc::TopicKind;
using rc::WriteIntent;
using rune::protocol::Partitioner;
using rc::producer::ProducerImpl;

namespace {

constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);

rc::ProducerConfig cfgOf(Acks acks, bool transactional = false) {
    rc::ProducerConfig c;
    c.acks              = acks;
    c.lingerUs          = 0;
    c.batchBytes        = 1 << 20;
    c.maxFrameBytes     = 1 << 20;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 10'000;
    if (transactional) c.transactionalId = "tx-autoinc";
    return c;
}

TopicHandle moduloTopic(int32_t partitions) {
    return TopicHandle{.topicId = 3, .partitionCount = partitions, .kind = TopicKind::Typed, .partitioner = Partitioner::Modulo};
}

Key keyZero() { return Key(uint64_t{0}); }

bool allZero(std::string_view k) {
    for (const char c : k) {
        if (c != 0) return false;
    }
    return !k.empty();
}

// Every frame the producer offers: its partition and, per record, the key (as bytes).
struct Captured {
    struct Frame {
        int32_t                  partition = -1;
        std::vector<std::string> keys;
    };
    std::vector<Frame> frames;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t partition, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rc::codec::decodeProduce(b, n);
            if (!v) return false;
            Frame f;
            f.partition = partition;
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            r.forEachRecord([&](std::string_view key, std::string_view, int64_t, int64_t, uint8_t) { f.keys.emplace_back(key); });
            frames.push_back(std::move(f));
            return true;
        };
    }
    std::size_t records() const {
        std::size_t n = 0;
        for (const Frame& f : frames) n += f.keys.size();
        return n;
    }
};

// A Producer over a bare impl (no Client): the test thread is its duty thread.
struct Bare {
    ProducerImpl* impl;
    rc::Producer  p;
    Captured      cap;
    explicit Bare(const rc::ProducerConfig& c, int32_t ringCap = 1 << 16) : impl(new ProducerImpl(c, ringCap)), p(impl) {}
    // Drains and flushes until a pass moves nothing: a batch sealed by a kind change lets the next record in.
    void pump() {
        const auto sink = cap.fn();
        for (;;) {
            const std::size_t before = cap.frames.size();
            int drained = 0;
            while (const int n = impl->drainRing(kNow)) drained += n;
            (void)impl->flushBatches(kNow, 42, sink);
            if (drained == 0 && cap.frames.size() == before) break;
        }
    }
};

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("cw_autoinc_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
        return dirs.back();
    }
} g_dataDirs;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir         = g_dataDirs.next();
    c.runnerMode      = rc::RunnerMode::Invoker;
    return c;
}

struct Rig {
    std::unique_ptr<rc::Client>  client;   // constructed first: owns the driver
    test_support::InvokerGateway gw;       // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client->doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
    // Runs fn (a blocking call on this thread) while another thread pumps the gateway until the call returned.
    template <class Fn>
    void whilePumping(Fn&& fn) {
        int stop[2];
        ASSERT_EQ(::pipe(stop), 0);
        std::thread t([&] {
            pollfd pfd{stop[0], POLLIN, 0};
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (::poll(&pfd, 1, 0) == 0 && std::chrono::steady_clock::now() < deadline) gw.pump();
        });
        const auto release = [&] {
            const char b = 0;
            (void)!::write(stop[1], &b, 1);
            t.join();
            ::close(stop[0]);
            ::close(stop[1]);
        };
        try {
            fn();
        } catch (...) {
            release();
            throw;
        }
        release();
    }
};

rune::codec::Descriptor stubDescriptor() {
    return rune::codec::DescriptorBuilder("autoinc_stub", 1).add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true).build();
}
struct StubRecord {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "autoinc_stub";
    std::size_t encodedSize() const noexcept { return 1; }
    std::size_t encode(uint8_t* b) const noexcept { b[0] = 1; return 1; }
};
const uint64_t StubRecord::kFingerprint = rune::codec::fingerprint(stubDescriptor());

}  // namespace

TEST(CheckedWritesAutoIncClient, ModuloHandleRoutesExplicitIdsByModulo) {
    Bare b(cfgOf(Acks::All | Acks::View));
    b.impl->setBrokerVersion(rpc::kBrokerVersionCheckedWrites);
    ASSERT_EQ(b.p.trySend(moduloTopic(4), Key(uint64_t{4097}), kVal, WriteIntent::Update), SendResult::Ok);
    b.pump();
    ASSERT_EQ(b.cap.frames.size(), 1u);
    EXPECT_EQ(b.cap.frames[0].partition, 1);
}

TEST(CheckedWritesAutoIncClient, KeyZeroCreatesAreSticky) {
    Bare b(cfgOf(Acks::All));
    const TopicHandle topic = moduloTopic(4);
    std::vector<int32_t> partitionOf;
    for (int i = 0; i < 130; ++i) {
        ASSERT_EQ(b.p.trySend(topic, keyZero(), kVal, WriteIntent::Insert), SendResult::Ok) << i;
        if (i % 40 == 39) b.pump();   // the ring holds a few dozen records
    }
    b.pump();
    for (const Captured::Frame& f : b.cap.frames) partitionOf.insert(partitionOf.end(), f.keys.size(), f.partition);
    ASSERT_EQ(partitionOf.size(), 130u);
    for (int i = 0; i < 130; ++i) EXPECT_EQ(partitionOf[static_cast<std::size_t>(i)], i / 64) << "record " << i;
}

TEST(CheckedWritesAutoIncClient, KeyZeroBatchesAreHomogeneous) {
    Bare b(cfgOf(Acks::All | Acks::View));
    b.impl->setBrokerVersion(rpc::kBrokerVersionCheckedWrites);
    const TopicHandle topic = moduloTopic(1);
    ASSERT_EQ(b.p.trySend(topic, keyZero(), kVal, WriteIntent::Insert), SendResult::Ok);
    ASSERT_EQ(b.p.trySend(topic, keyZero(), kVal, WriteIntent::Insert), SendResult::Ok);
    ASSERT_EQ(b.p.trySend(topic, Key(uint64_t{5}), kVal, WriteIntent::Update), SendResult::Ok);
    ASSERT_EQ(b.p.trySend(topic, keyZero(), kVal, WriteIntent::Insert), SendResult::Ok);
    b.p.flush();
    b.pump();
    ASSERT_EQ(b.cap.frames.size(), 3u);
    EXPECT_EQ(b.cap.frames[0].keys.size(), 2u);
    EXPECT_EQ(b.cap.frames[1].keys.size(), 1u);
    EXPECT_EQ(b.cap.frames[2].keys.size(), 1u);
    for (const Captured::Frame& f : b.cap.frames) {
        for (const std::string& k : f.keys) EXPECT_EQ(allZero(k), allZero(f.keys[0]));
    }
    EXPECT_EQ(b.impl->debugIdStrideForTest(3, 0, 0), 1u);
    EXPECT_EQ(b.impl->debugIdStrideForTest(3, 0, 1), 0u);
    EXPECT_EQ(b.impl->debugIdStrideForTest(3, 0, 2), 1u);
}

namespace {
struct RefusalCase {
    const char* name;
    Acks        acks;
    WriteIntent intent;
    const char* text;
};
const RefusalCase kRefusalCases[] = {
    {"a non-idempotent producer creating", Acks::None, WriteIntent::Insert, "autoincrement creates need an idempotent producer"},
    {"an acks=leader producer creating", Acks::Leader, WriteIntent::Insert, "autoincrement creates need acks All"},
    {"a live upsert", Acks::All | Acks::View, WriteIntent::Upsert, "an autoincrement topic takes no plain upserts"},
};
template <class Fn>
void expectRefusal(const RefusalCase& c, Fn&& blockingSend) {
    try {
        blockingSend();
        ADD_FAILURE() << "send did not throw";
    } catch (const std::invalid_argument& e) {
        EXPECT_NE(std::string(e.what()).find(c.text), std::string::npos) << e.what();
    }
}
}  // namespace

TEST(CheckedWritesAutoIncClient, LocalRefusals) {
    for (const RefusalCase& c : kRefusalCases) {
        SCOPED_TRACE(c.name);
        Bare b(cfgOf(c.acks));
        const TopicHandle topic = moduloTopic(2);
        const Key key = c.intent == WriteIntent::Insert ? keyZero() : Key(uint64_t{7});
        EXPECT_EQ(b.p.trySend(topic, key, kVal, c.intent), SendResult::WriteNotAllowed);
        EXPECT_EQ(b.p.trySendTo(topic, 0, key, kVal, c.intent), SendResult::WriteNotAllowed);
        test_support::AckLedger ledger;
        ledger.attach(b.p);
        EXPECT_EQ(b.p.trySend(topic, key, kVal, rc::AckToken{1}, c.intent), SendResult::WriteNotAllowed);
        expectRefusal(c, [&] { b.p.send(topic, key, kVal, c.intent); });
        expectRefusal(c, [&] { b.p.send(topic, key, kVal, rc::AckToken{1}, c.intent); });
        b.pump();
        EXPECT_TRUE(b.cap.frames.empty()) << "nothing is queued";
        EXPECT_TRUE(ledger.recs.empty()) << "no completion follows";
    }
}

TEST(CheckedWritesAutoIncClient, TombstoneUpsertOnAModuloHandleIsAllowed) {
    Bare b(cfgOf(Acks::All | Acks::View));
    EXPECT_EQ(b.p.trySend(moduloTopic(2), Key(uint64_t{7}), std::span<const uint8_t>{}), SendResult::Ok);
}

TEST(CheckedWritesAutoIncClient, KeyZeroCreateNeedsNoViewAndWorksInATransaction) {
    {
        Bare b(cfgOf(Acks::All));
        b.impl->setBrokerVersion(rpc::kBrokerVersionCheckedWrites);
        EXPECT_EQ(b.p.trySend(moduloTopic(1), keyZero(), kVal, WriteIntent::Insert), SendResult::Ok);
        b.pump();
        EXPECT_EQ(b.cap.frames.size(), 1u);
    }
    {
        Bare b(cfgOf(Acks::All, /*transactional=*/true));
        b.impl->setTxnGate(ProducerImpl::TxnGate::Open);
        EXPECT_EQ(b.p.trySend(moduloTopic(1), keyZero(), kVal, WriteIntent::Insert), SendResult::Ok);
    }
}

TEST(CheckedWritesAutoIncClient, KeyZeroToAnOldBrokerIsBrokerTooOld) {
    Bare b(cfgOf(Acks::All));
    b.impl->setBrokerVersion(rpc::kBrokerVersionCheckedWrites - 1);
    test_support::AckLedger ledger;
    ledger.attach(b.p);
    for (rc::Token t = 1; t <= 3; ++t)
        ASSERT_EQ(b.p.trySend(moduloTopic(1), keyZero(), kVal, rc::AckToken{t}, WriteIntent::Insert), SendResult::Ok);
    b.p.flush();
    b.pump();
    EXPECT_TRUE(b.cap.frames.empty());
    ASSERT_TRUE(ledger.exactlyOnce(1, 4));
    for (rc::Token t = 1; t <= 3; ++t) EXPECT_EQ(ledger.status(t), OpStatus::BrokerTooOld);
}

TEST(CheckedWritesAutoIncClientRegression, FnvHandlesRouteAsBeforeRegression) {
    constexpr int32_t kParts = 7;
    Bare b(cfgOf(Acks::All), 1 << 18);
    const TopicHandle topic{3, kParts, TopicKind::Typed};
    std::mt19937_64 rng(42);
    for (int i = 0; i < 1000; ++i) {
        const Key key(static_cast<uint64_t>(rng()));
        ASSERT_EQ(b.p.trySend(topic, key, kVal), SendResult::Ok);
    }
    b.pump();
    ASSERT_EQ(b.cap.records(), 1000u);
    for (const Captured::Frame& f : b.cap.frames) {
        for (const std::string& k : f.keys) {
            const Key key(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size()));
            EXPECT_EQ(f.partition, key.partition(kParts));
        }
    }
}

TEST(CheckedWritesAutoIncClient, OpenTopicCarriesThePartitioner) {
    Rig r(invokerCfg(4651, 4652));
    r.gw.replySchemas = true;
    r.gw.schemaBlob   = rune::codec::serialize(stubDescriptor());
    r.connect();
    rc::Producer shared = r.client->producer(cfgOf(Acks::All));

    const auto partitionerOf = [&] {
        Partitioner own{}, borrowed{};
        r.whilePumping([&] {
            own      = r.client->openTopicAndWait<StubRecord>("autoinc_stub").handle().partitioner;
            borrowed = r.client->openTopicAndWait<StubRecord>("autoinc_stub", shared).handle().partitioner;
        });
        EXPECT_EQ(own, borrowed);
        return own;
    };
    EXPECT_EQ(partitionerOf(), Partitioner::Fnv1a);
    rune::protocol::TopicKeyFeatures f;
    f.keyIndex      = true;
    f.autoIncrement = true;
    f.partitioner   = Partitioner::Modulo;
    r.gw.schemaKeyFeatures = f;
    EXPECT_EQ(partitionerOf(), Partitioner::Modulo);
}

TEST(CheckedWritesAutoIncClient, RefusedAndBackPressuredCreatesDoNotAdvanceTheStickyRun) {
    Bare b(cfgOf(Acks::All), 1 << 11);   // a ring of a few records
    const TopicHandle topic = moduloTopic(4);
    for (int i = 0; i < 70; ++i)
        ASSERT_EQ(b.p.trySend(topic, keyZero(), std::span<const uint8_t>{}, WriteIntent::Insert), SendResult::WriteNotAllowed);
    int accepted = 0, backPressured = 0;
    while (accepted < 130) {
        const SendResult r = b.p.trySend(topic, keyZero(), kVal, WriteIntent::Insert);
        if (r == SendResult::Ok) {
            ++accepted;
        } else {
            ASSERT_EQ(r, SendResult::BackPressured);
            ++backPressured;
            b.pump();
        }
    }
    b.pump();
    EXPECT_GT(backPressured, 0);
    std::vector<int32_t> partitionOf;
    for (const Captured::Frame& f : b.cap.frames) partitionOf.insert(partitionOf.end(), f.keys.size(), f.partition);
    ASSERT_EQ(partitionOf.size(), 130u);
    for (int i = 0; i < 130; ++i) EXPECT_EQ(partitionOf[static_cast<std::size_t>(i)], i / 64) << "record " << i;
}

TEST(CheckedWritesAutoIncClientRegression, SchemaKeyFeaturesRoundTripToTheResultRegression) {
    Rig r(invokerCfg(4653, 4654));
    r.gw.replySchemas = true;
    r.gw.schemaBlob   = rune::codec::serialize(stubDescriptor());
    rune::protocol::TopicKeyFeatures f;
    f.keyIndex             = true;
    f.autoIncrement        = true;
    f.partitioner          = Partitioner::Modulo;
    f.autoIncrementMax     = 1'000'000;
    f.autoIncrementReserve = 77;
    r.gw.schemaKeyFeatures = f;
    r.connect();
    rc::TopicSchemaResult result;
    r.whilePumping([&] { result = r.client->getTopicSchemaAndWait("autoinc_stub", StubRecord::kFingerprint); });
    EXPECT_EQ(result.errorCode, 0);
    ASSERT_TRUE(result.hasKeyFeatures);
    EXPECT_EQ(result.keyFeatures, f);
    EXPECT_EQ(result.descriptorBlob, r.gw.schemaBlob);
}
