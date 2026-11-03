// tests/producer/CheckedWritesIntentsTest.cpp - checked writes Part 05 Task 3: WriteIntent on the raw producer.  The
// intent rides the ring flags byte to the record's attributes; the local refusals (P5-9); sendAndWait; the key
// features a DefineTopic carries and the broker-version gate on them.
#include "../support/AckLedger.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <poll.h>
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
using rc::producer::ProducerImpl;

namespace {

constexpr int64_t kNow = 1'000'000'000;
const std::vector<uint8_t> kVal(16, 0x11);
const std::vector<uint8_t> kNoValue;

rc::ProducerConfig cfgOf(Acks acks, bool transactional = false) {
    rc::ProducerConfig c;
    c.acks              = acks;
    c.lingerUs          = 0;
    c.batchBytes        = 1 << 20;
    c.maxFrameBytes     = 1 << 20;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 1'000;
    c.deliveryTimeoutMs = 10'000;
    if (transactional) c.transactionalId = "tx-intents";
    return c;
}

// Every frame the producer offers: the raw bytes, and per record the attributes byte.
struct Captured {
    std::vector<std::vector<uint8_t>> frames;
    std::vector<uint8_t>              attrs;
    bool                              crcValid = true;
    std::vector<int64_t>              corrs;
    ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            const auto v = rc::codec::decodeProduce(b, n);
            if (!v) return false;
            frames.emplace_back(b, b + n);
            corrs.push_back(v->correlationId);
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            crcValid = crcValid && r.ensureValid();
            r.forEachRecord([&](std::string_view, std::string_view, int64_t, int64_t, uint8_t attributes) {
                attrs.push_back(attributes);
            });
            return true;
        };
    }
};

// A Producer over a bare impl (no Client): the test thread is its duty thread.
struct Bare {
    ProducerImpl* impl;
    rc::Producer  p;
    Captured      cap;
    explicit Bare(const rc::ProducerConfig& c) : impl(new ProducerImpl(c, 1 << 16)), p(impl) {}
    void pump() {
        const auto sink = cap.fn();
        while (impl->drainRing(kNow) != 0) {}
        (void)impl->flushBatches(kNow, 42, sink);
    }
};

// A zigzag varint at f[pos], pos advanced.
int64_t readZigzag(const std::vector<uint8_t>& f, std::size_t& pos) {
    uint64_t v = 0;
    for (int shift = 0;; shift += 7) {
        const uint8_t b = f[pos++];
        v |= static_cast<uint64_t>(b & 0x7f) << shift;
        if ((b & 0x80) == 0) break;
    }
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
}

// A produce frame without its wall-clock content, whatever the varint widths: the batch length fields, CRC and
// base / max timestamps are zeroed, and each record is reduced to attributes + the bytes after its timestamp delta.
std::vector<uint8_t> withoutClock(std::vector<uint8_t> f) {
    const auto v = rc::codec::decodeProduce(f.data(), f.size());
    EXPECT_TRUE(v.has_value());
    if (!v) return f;
    const std::size_t batch = static_cast<std::size_t>(v->recordBatch.data() - f.data());
    for (std::size_t i = batch - 4; i < batch; ++i) f[i] = 0;        // the batch's length in the produce frame
    for (std::size_t i = 8; i < 12; ++i) f[batch + i] = 0;           // batchLength
    for (std::size_t i = 17; i < 21; ++i) f[batch + i] = 0;          // crc
    for (std::size_t i = 27; i < 43; ++i) f[batch + i] = 0;          // base + max timestamp
    const int32_t records = static_cast<int32_t>(f[batch + 57]) | (f[batch + 58] << 8) | (f[batch + 59] << 16) | (f[batch + 60] << 24);
    std::vector<uint8_t> out(f.begin(), f.begin() + static_cast<std::ptrdiff_t>(batch + 61));
    std::size_t pos = batch + 61;
    for (int32_t i = 0; i < records; ++i) {
        const int64_t len = readZigzag(f, pos);
        const std::size_t end = pos + static_cast<std::size_t>(len);
        out.push_back(f[pos++]);                                     // attributes
        (void)readZigzag(f, pos);                                    // timestamp delta
        out.insert(out.end(), f.begin() + static_cast<std::ptrdiff_t>(pos), f.begin() + static_cast<std::ptrdiff_t>(end));
        pos = end;
    }
    return out;
}

// The first frame of ProducerImpl::submitSendRecord(1, 0, Key(k), kVal ({} for k = 3), false, 0) for k = 1..4 on an
// All | View producer, flushed with correlation id 42, captured by building this test at the commit before the
// intent change (04624aae).
const std::vector<uint8_t> kGoldenUpsertFrame = {
    0x18, 0x00, 0x10, 0x00, 0x78, 0x00, 0x03, 0x00, 0x2a, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03, 0x01, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x57, 0x7e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0xa9, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x9d, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x02, 0x69,
    0x76, 0xca, 0x10, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0xee, 0x04, 0x1d,
    0x9d, 0xa1, 0x01, 0x00, 0x00, 0xee, 0x04, 0x1d, 0x9d, 0xa1, 0x01, 0x00,
    0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x57, 0x7e, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x00, 0x00, 0x10,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x11, 0x11, 0x11,
    0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
    0x11, 0x00, 0x3c, 0x00, 0x00, 0x02, 0x10, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x20, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
    0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x00, 0x1c, 0x00, 0x00,
    0x04, 0x10, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x3c, 0x00, 0x00, 0x06, 0x10, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x20, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
    0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x00,
};

// Every data dir a test handed out, removed when the test binary exits.
struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("cw_intents_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
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
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
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
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
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

}  // namespace

TEST(CheckedWritesIntents, IntentBitsReachTheRecordAttributes) {
    Bare b(cfgOf(Acks::All | Acks::View));
    b.impl->setBrokerVersion(rpc::kBrokerVersionCheckedWrites);
    const TopicHandle topic{1, 1, TopicKind::Typed};
    ASSERT_EQ(b.p.trySend(topic, Key(uint64_t{1}), kVal, WriteIntent::Insert), SendResult::Ok);
    ASSERT_EQ(b.p.trySend(topic, Key(uint64_t{2}), kVal, WriteIntent::Update), SendResult::Ok);
    ASSERT_EQ(b.p.trySend(topic, Key(uint64_t{3}), kNoValue, WriteIntent::Update), SendResult::Ok);
    ASSERT_EQ(b.p.trySend(topic, Key(uint64_t{4}), kVal), SendResult::Ok);
    b.pump();
    ASSERT_EQ(b.cap.frames.size(), 1u);
    EXPECT_EQ(b.cap.attrs, (std::vector<uint8_t>{1, 2, 2, 0}));
    EXPECT_TRUE(b.cap.crcValid);
}

TEST(CheckedWritesIntents, UpsertFramesAreByteIdenticalRegression) {
    const auto frameOf = [](auto&& send) {
        Bare b(cfgOf(Acks::All | Acks::View));
        const TopicHandle topic{1, 1, TopicKind::Typed};
        for (uint64_t k = 1; k <= 4; ++k) EXPECT_EQ(send(b.p, topic, k), SendResult::Ok);
        b.pump();
        EXPECT_EQ(b.cap.frames.size(), 1u);
        return b.cap.frames.empty() ? std::vector<uint8_t>{} : b.cap.frames[0];
    };
    const auto value = [](uint64_t k) { return std::span<const uint8_t>(k == 3 ? kNoValue : kVal); };
    const auto plain = frameOf([&](rc::Producer& p, TopicHandle t, uint64_t k) { return p.trySend(t, Key(k), value(k)); });
    const auto explicitUpsert = frameOf([&](rc::Producer& p, TopicHandle t, uint64_t k) {
        return p.trySend(t, Key(k), value(k), WriteIntent::Upsert);
    });
    EXPECT_EQ(withoutClock(plain), withoutClock(explicitUpsert));
    EXPECT_EQ(withoutClock(plain), withoutClock(kGoldenUpsertFrame));
}

namespace {
struct RefusalCase {
    const char* name;
    Acks        acks;
    bool        transactional;
    TopicKind   kind;
    WriteIntent intent;
    bool        emptyValue;
    const char* text;
};
const RefusalCase kRefusalCases[] = {
    {"insert on a raw handle", Acks::All | Acks::View, false, TopicKind::Raw, WriteIntent::Insert, false, "intents need a typed topic"},
    {"update without View", Acks::All, false, TopicKind::Typed, WriteIntent::Update, false, "intents need acks including View"},
    {"upsert with View on a raw handle", Acks::All | Acks::View, false, TopicKind::Raw, WriteIntent::Upsert, false, "acks View needs a typed topic"},
    {"insert in a transaction", Acks::All | Acks::View, true, TopicKind::Typed, WriteIntent::Insert, false, "intents inside a transaction need Part G"},
    {"insert without a value", Acks::All | Acks::View, false, TopicKind::Typed, WriteIntent::Insert, true, "an insert needs a value"},
};
std::span<const uint8_t> valueOf(const RefusalCase& c) {
    return c.emptyValue ? std::span<const uint8_t>(kNoValue) : std::span<const uint8_t>(kVal);
}
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

TEST(CheckedWritesIntents, LocalRefusalsAreWriteNotAllowed) {
    for (const RefusalCase& c : kRefusalCases) {
        SCOPED_TRACE(c.name);
        Bare b(cfgOf(c.acks, c.transactional));
        const TopicHandle topic{3, 1, c.kind};
        const auto value = valueOf(c);
        EXPECT_EQ(b.p.trySend(topic, Key(uint64_t{1}), value, c.intent), SendResult::WriteNotAllowed);
        EXPECT_EQ(b.p.trySendTo(topic, 0, Key(uint64_t{1}), value, c.intent), SendResult::WriteNotAllowed);
        test_support::AckLedger ledger;
        ledger.attach(b.p);
        EXPECT_EQ(b.p.trySend(topic, Key(uint64_t{1}), value, rc::AckToken{1}, c.intent), SendResult::WriteNotAllowed);
        expectRefusal(c, [&] { b.p.send(topic, Key(uint64_t{1}), value, c.intent); });
        expectRefusal(c, [&] { b.p.send(topic, Key(uint64_t{1}), value, rc::AckToken{1}, c.intent); });
        b.pump();
        EXPECT_TRUE(b.cap.frames.empty()) << "nothing is queued";
        EXPECT_TRUE(ledger.recs.empty()) << "no completion follows";
    }
}

TEST(CheckedWritesIntents, RefusalsLeaveTheCompletionsQueueAlone) {
    rc::Client client(invokerCfg(4631, 4632));
    for (const RefusalCase& c : kRefusalCases) {
        SCOPED_TRACE(c.name);
        rc::Producer    p = client.producer(cfgOf(c.acks, c.transactional));
        rc::Completions cq(client);
        const TopicHandle topic{3, 1, c.kind};
        const auto value = valueOf(c);
        EXPECT_EQ(p.trySend(topic, Key(uint64_t{1}), value, cq, 1, c.intent), SendResult::WriteNotAllowed);
        expectRefusal(c, [&] { p.send(topic, Key(uint64_t{1}), value, cq, 1, c.intent); });
        expectRefusal(c, [&] { p.send(topic, Key(uint64_t{1}), value, rc::AckToken{1}, c.intent); });
        for (int i = 0; i < 50; ++i) client.doWork();
        int polled = 0;
        cq.poll([&](const rc::Completion&, std::span<const uint8_t>) { ++polled; });
        EXPECT_EQ(polled, 0);
    }
}

TEST(CheckedWritesIntents, ViewUpsertOnAnUnknownHandleIsUnchecked) {
    Bare b(cfgOf(Acks::All | Acks::View));
    EXPECT_EQ(b.p.trySend(TopicHandle{3, 1, TopicKind::Unknown}, Key(uint64_t{1}), kVal), SendResult::Ok);
    b.pump();
    EXPECT_EQ(b.cap.frames.size(), 1u);
}

TEST(CheckedWritesIntents, UpsertOnARawHandleIsUnchecked) {
    Bare b(cfgOf(Acks::All));
    const TopicHandle raw{3, 1, TopicKind::Raw};
    EXPECT_EQ(b.p.trySend(raw, Key(uint64_t{1}), kVal), SendResult::Ok);
    b.pump();
    EXPECT_EQ(b.cap.frames.size(), 1u);
    EXPECT_EQ(b.cap.attrs, (std::vector<uint8_t>{0}));
}

TEST(CheckedWritesIntents, IntentOnAnOldBrokerIsBrokerTooOld) {
    Bare b(cfgOf(Acks::All | Acks::View));
    b.impl->setBrokerVersion(rpc::kBrokerVersionCheckedWrites - 1);
    test_support::AckLedger ledger;
    ledger.attach(b.p);
    const TopicHandle topic{1, 1, TopicKind::Typed};
    ASSERT_EQ(b.p.trySend(topic, Key(uint64_t{1}), kVal, rc::AckToken{1}, WriteIntent::Insert), SendResult::Ok);
    b.pump();
    EXPECT_TRUE(b.cap.frames.empty());
    ASSERT_TRUE(ledger.exactlyOnce(1, 2));
    EXPECT_EQ(ledger.status(1), OpStatus::BrokerTooOld);
}

TEST(CheckedWritesIntents, DirectModeAndDeferredRecordsKeepTheirIntent) {
    rc::ProducerConfig c = cfgOf(Acks::Leader);
    c.maxInFlight        = 1;
    ProducerImpl impl(c);
    impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr, 1});
    test_support::ScriptedAcks acks;
    acks.attach(impl);
    Captured cap;
    const auto sink = cap.fn();
    const auto h = acks.cb([&](int64_t) {
        // inside the producer's own ack callback: the record is deferred, then applied with its intent
        EXPECT_EQ(impl.trySubmit(1, 0, Key(uint64_t{2}), kVal, 0, nullptr, 1), SendResult::Ok);
        EXPECT_EQ(impl.deferredCount(), 1u);
    });
    ASSERT_EQ(impl.trySubmit(1, 0, Key(uint64_t{1}), kVal, h, nullptr, 2), SendResult::Ok);   // appended directly
    impl.submitFlush();
    (void)impl.flushBatches(2, 42, sink);
    ASSERT_EQ(cap.frames.size(), 1u);
    impl.onProduceResponse(cap.corrs[0], 100, 0);
    impl.submitFlush();
    (void)impl.flushBatches(3, 42, sink);
    ASSERT_EQ(cap.frames.size(), 2u);
    EXPECT_EQ(impl.deferredCount(), 0u);
    EXPECT_EQ(cap.attrs, (std::vector<uint8_t>{2, 1}));
}

TEST(CheckedWritesIntents, SendAndWaitReturnsThePerRecordVerdict) {
    constexpr uint16_t kViolation = 56;
    Rig r(invokerCfg(4633, 4634));
    r.gw.idempotence = true;
    r.connect();
    rc::Producer p = r.client->producer(cfgOf(Acks::All | Acks::View));
    const TopicHandle topic = r.client->topic("t");
    ASSERT_TRUE(r.drive([&] { return p.producerId() >= 0; }));

    r.gw.produceVerdicts = std::vector<uint8_t>{0, 0, uint8_t(kViolation), 0, 0, 0, 0, 0};   // record 0: code 56, constraint 0
    rc::Completion refused;
    r.whilePumping([&] { refused = p.sendAndWait(topic, Key(uint64_t{1}), kVal, WriteIntent::Insert, std::chrono::seconds(8)); });
    EXPECT_EQ(refused.op, rc::OpKind::Send);
    EXPECT_EQ(refused.status, OpStatus::BrokerError);
    EXPECT_EQ(refused.errorCode, kViolation);
    EXPECT_EQ(rc::constraintIdOf(refused), 0u);

    r.gw.produceVerdicts = std::vector<uint8_t>{};
    rc::Completion accepted;
    r.whilePumping([&] { accepted = p.sendAndWait(topic, Key(uint64_t{2}), kVal, WriteIntent::Insert, std::chrono::seconds(8)); });
    EXPECT_EQ(accepted.status, OpStatus::Ok);
    ASSERT_FALSE(r.gw.log.empty());
    EXPECT_EQ(accepted.value, r.gw.log.back().baseOffset);

    EXPECT_THROW((void)p.sendAndWait(TopicHandle{1, 1, TopicKind::Raw}, Key(uint64_t{3}), kVal, WriteIntent::Insert,
                                     std::chrono::seconds(1)),
                 std::invalid_argument);
}

TEST(CheckedWritesIntents, SendAndWaitReturnsATimeoutCompletionRegression) {
    Rig r(invokerCfg(4635, 4636));
    r.gw.idempotence = true;
    r.connect();
    rc::Producer p = r.client->producer(cfgOf(Acks::All | Acks::View));
    const TopicHandle topic = r.client->topic("t");
    ASSERT_TRUE(r.drive([&] { return p.producerId() >= 0; }));

    // The gateway is not pumped: the produce is never answered.
    rc::Completion c;
    ASSERT_NO_THROW(c = p.sendAndWait(topic, Key(uint64_t{1}), kVal, WriteIntent::Insert, std::chrono::milliseconds(300)));
    EXPECT_EQ(c.status, OpStatus::Timeout);
}

TEST(CheckedWritesIntents, DefineSendsKeyFeaturesOnlyWhenSet) {
    namespace pc = rune::protocol::client;
    const auto defineWith = [](int32_t brokerVersion, int32_t in, rc::TopicOptions topic,
                               std::vector<pc::DefineTopic>* decoded, std::size_t* sent) {
        Rig r(invokerCfg(in, in + 1));
        r.gw.advertisedBrokerVersion = brokerVersion;
        r.connect();
        std::vector<std::vector<uint8_t>> frames;
        r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int ep, const uint8_t* f, std::size_t len) {
            frames.emplace_back(f, f + len);
            const auto h = pc::MessageHeader::decode(f, len);
            r.gw.sendError(ep, static_cast<int16_t>(pc::ErrorCode::InvalidRequest), "captured", h ? h->sessionId : 0,
                           h ? h->correlationId : 0);
        };
        test_support::TestCq cq(*r.client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) {
            return r.client->defineTopic("t", 1, "", {}, rc::Persistence::Disk, topic, {}, q, t);
        });
        EXPECT_TRUE(r.drive([&] { return f.ready(); }));
        for (const auto& fr : frames) decoded->push_back(*pc::DefineTopic::decode(fr.data(), fr.size()));
        *sent = frames.size();
        return f.status();
    };
    {
        std::vector<pc::DefineTopic> d;
        std::size_t                  sent = 0;
        EXPECT_EQ(defineWith(rpc::kBrokerVersion, 4635, rc::TopicOptions{}, &d, &sent), OpStatus::BrokerError);
        ASSERT_EQ(sent, 1u);
        EXPECT_FALSE(d[0].hasKeyFeatures);
    }
    rc::TopicOptions indexed;
    indexed.keyFeatures.keyIndex = true;
    {
        std::vector<pc::DefineTopic> d;
        std::size_t                  sent = 0;
        EXPECT_EQ(defineWith(rpc::kBrokerVersion, 4637, indexed, &d, &sent), OpStatus::BrokerError);
        ASSERT_EQ(sent, 1u);
        EXPECT_TRUE(d[0].hasKeyFeatures);
        EXPECT_TRUE(d[0].keyFeatures.keyIndex);
    }
    {
        std::vector<pc::DefineTopic> d;
        std::size_t                  sent = 0;
        EXPECT_EQ(defineWith(rpc::kBrokerVersionCheckedWrites - 1, 4639, indexed, &d, &sent), OpStatus::BrokerTooOld);
        EXPECT_EQ(sent, 0u) << "nothing is sent to a broker that cannot decode the trailer";
    }
}

TEST(CheckedWritesIntents, DefineQueuedBeforeTheSeedConnectsIsBrokerTooOld) {
    namespace pc = rune::protocol::client;
    Rig r(invokerCfg(4641, 4642));
    r.gw.advertisedBrokerVersion = rpc::kBrokerVersionCheckedWrites - 1;
    int sent = 0;
    r.gw.templateHandlers[pc::DefineTopic::TEMPLATE_ID] = [&](int, const uint8_t*, std::size_t) { ++sent; };
    ASSERT_TRUE(r.drive([&] { return r.gw.ready(); }));
    test_support::TestCq cq(*r.client);
    auto connect = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client->connect(q, t); });
    rc::TopicOptions indexed;
    indexed.keyFeatures.keyIndex = true;
    auto define = cq.issue([&](rc::Completions& q, rc::Token t) {   // issued before the seed answered the connect
        return r.client->defineTopic("t", 1, "", {}, rc::Persistence::Disk, indexed, {}, q, t);
    });
    ASSERT_TRUE(define.accepted());
    ASSERT_TRUE(r.drive([&] { return define.ready(); }));
    EXPECT_EQ(define.status(), OpStatus::BrokerTooOld);
    EXPECT_EQ(sent, 0);
    ASSERT_TRUE(r.drive([&] { return connect.ready(); }));
}
