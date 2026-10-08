// tests/client/OpenTopicGateTest.cpp
//
// Task 11 — openTopic<T> fingerprint gate + TypedTopic send path via the
// in-process Aeron loopback gateway (same pattern as ClientSchemaTest.cpp).
//
// The gateway echoes back a TopicSchemaResponse carrying a real descriptor
// blob.  Stub types with matching / mismatching kFingerprint drive the
// pass / fail branches of the gate.
//
// Test matrix:
//   1. OpenTopicResolvesOnMatch  — StubT_Match: kFingerprint == blob fingerprint → resolves
//   2. OpenTopicFailsOnMismatch  — StubT_Mismatch: kFingerprint != blob fingerprint → throws
//   3. OpenTopicSendDoesNotThrow — After a Match resolve, send() does not throw (fire-and-forget)

#include "../support/TestCompletions.h"
#include "../support/AeronTestErrors.h"
#include <gtest/gtest.h>
#include <rune/client/Client.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/codec/Codec.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
using namespace rune::codec;

namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;
namespace rpc = rune::protocol::client;

static constexpr auto kFutureTimeout = 10s;
static constexpr int  kPollMax       = 3'000;
static constexpr auto kPollSleep     = 2ms;

namespace {
static std::atomic<int> g_dirCtr{0};

std::string uniqueDir() {
    const int c = g_dirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/cc6_opentopicgate_test_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}
} // namespace

// ---------------------------------------------------------------------------
// Minimal loopback gateway (responds to Connect + GetTopicSchema)
// ---------------------------------------------------------------------------
struct GateGateway {
    std::string aeronDir;
    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::Subscription>         sub_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    int32_t ingressStream = 0;
    int32_t egressStream  = 0;

    bool replyConnectOK      = true;
    bool replyGetTopicSchema = true;

    int32_t              getTopicId   = 0x40000001;
    uint16_t             getVersion   = 1;
    uint16_t             getErrorCode = 0;
    std::vector<uint8_t> getBlob;

    // Task 19: fingerprint-selecting GetTopicSchema responses (the broker's
    // Task 13/18 behavior).  A nonzero requestedFingerprint is looked up in
    // this table; when absent the reply is an error (unknown schema — an
    // old generated client whose version the broker has no record of).  A
    // zero requestedFingerprint ("latest") always uses the plain getXxx
    // fields above, regardless of this table.
    struct FingerprintOverride {
        int32_t               topicId;
        uint16_t              version;
        std::vector<uint8_t>  blob;
    };
    std::map<uint64_t, FingerprintOverride> fingerprintOverrides;
    uint16_t unknownFingerprintErrorCode = 1;  // any nonzero "not found" code

    uint8_t replyBuf[4096]{};

    explicit GateGateway(std::string dir) : aeronDir(std::move(dir)) {}
    ~GateGateway() { stop(); }

    void start(int32_t inStream, int32_t egStream) {
        ingressStream = inStream;
        egressStream  = egStream;

        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) throw std::runtime_error("GateGateway: connect failed");

        int64_t subId = aeron_->addSubscription("aeron:ipc", ingressStream);
        int64_t pubId = aeron_->addExclusivePublication("aeron:ipc", egressStream);

        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub_) sub_ = aeron_->findSubscription(subId);
            if (!pub_) pub_ = aeron_->findExclusivePublication(pubId);
            if (sub_ && pub_) break;
            std::this_thread::sleep_for(kPollSleep);
        }
        if (!sub_ || !pub_)
            throw std::runtime_error("GateGateway: timed out");

        assembler_ = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf,
                   aeron::util::index_t            offset,
                   aeron::util::index_t            len,
                   aeron::Header&)
            {
                onFrame(reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                        static_cast<std::size_t>(len));
            });
    }

    void stop() {
        assembler_.reset();
        pub_.reset();
        sub_.reset();
        aeron_.reset();
    }

    int poll(int limit = 16) {
        if (!sub_ || !assembler_) return 0;
        return sub_->poll(assembler_->handler(), limit);
    }

    bool sendFrame(const uint8_t* buf, std::size_t len) {
        if (!pub_) return false;
        const auto ilen = static_cast<aeron::util::index_t>(len);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), ilen);
        for (int i = 0; i < 500; ++i) {
            if (pub_->offer(ab, 0, ilen) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;

        switch (static_cast<rcc::TemplateId>(*tidOpt)) {

        case rcc::TemplateId::Connect: {
            auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt || !replyConnectOK) break;
            std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                0, 42, opt->sessionId, opt->correlationId);
            if (n > 0) sendFrame(replyBuf, n);
            break;
        }

        case rcc::TemplateId::GetTopicSchema: {
            auto opt = rpc::GetTopicSchema::decode(buf, len);
            if (!opt || !replyGetTopicSchema) break;

            rpc::TopicSchemaResponse resp;
            resp.sessionId      = opt->sessionId;
            resp.correlationId  = opt->correlationId;

            if (opt->requestedFingerprint != 0) {
                auto it = fingerprintOverrides.find(opt->requestedFingerprint);
                if (it != fingerprintOverrides.end()) {
                    resp.topicId        = it->second.topicId;
                    resp.version        = it->second.version;
                    resp.errorCode      = 0;
                    resp.descriptorBlob = it->second.blob.data();
                    resp.descriptorLen  = static_cast<uint32_t>(it->second.blob.size());
                } else {
                    resp.topicId        = -1;
                    resp.version        = 0;
                    resp.errorCode      = unknownFingerprintErrorCode;
                    resp.descriptorBlob = nullptr;
                    resp.descriptorLen  = 0;
                }
            } else {
                resp.topicId        = getTopicId;
                resp.version        = getVersion;
                resp.errorCode      = getErrorCode;
                resp.descriptorBlob = getBlob.data();
                resp.descriptorLen  = static_cast<uint32_t>(getBlob.size());
            }

            std::vector<uint8_t> scratch(resp.encodedSize() + 32u);
            const std::size_t n = resp.encode(scratch.data());
            if (n > 0) sendFrame(scratch.data(), n);
            break;
        }

        default: break;
        }
    }
};

// ---------------------------------------------------------------------------
// GateHarness — RAII test wrapper
// ---------------------------------------------------------------------------
struct GateHarness {
    std::string agentDir;
    std::string aeronDir;
    ClientConfig clientCfg;

    std::unique_ptr<Client>       client;
    std::unique_ptr<GateGateway>  gw;

    int32_t ingressStream;
    int32_t egressStream;

    explicit GateHarness(int32_t inStream, int32_t egStream)
        : ingressStream(inStream), egressStream(egStream)
    {
        agentDir = uniqueDir();
        aeronDir = agentDir + "/aeron";
    }

    void build() {
        clientCfg.gatewayChannel   = "aeron:ipc";
        clientCfg.gatewayStreamId  = ingressStream;
        clientCfg.egressChannel    = "aeron:ipc";
        clientCfg.egressStreamId   = egressStream;
        clientCfg.dataDir          = agentDir;
        clientCfg.connectTimeoutMs = 5000;
        clientCfg.pingTimeoutMs    = 5000;

        client = std::make_unique<Client>(clientCfg);
        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<GateGateway>(aeronDir);
        gw->start(ingressStream, egressStream);
    }

    template<typename Pred>
    bool pumpUntil(Pred pred, int limit = kPollMax) {
        for (int i = 0; i < limit; ++i) {
            gw->poll(16);
            if (pred()) return true;
            std::this_thread::sleep_for(kPollSleep);
        }
        return false;
    }

    void connectAndWait() {
        test_support::TestCq futCq(*client);
        auto fut = futCq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
        pumpUntil([&]() {
            return client->state() == rcs::SessionState::Connected;
        });
        ASSERT_TRUE(fut.waitReady())
            << "connect() future did not resolve";
        ASSERT_TRUE(fut.ok()) << fut.describe() << " connect() failed";
    }

    ~GateHarness() {
        if (client) client->close();
        gw.reset();
        client.reset();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
};

// ---------------------------------------------------------------------------
// Descriptor + stub types
// ---------------------------------------------------------------------------
static Descriptor makeCharsDescriptor() {
    return DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .build();
}

struct StubT_Match {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0xAB; buf[1] = 0xCD; buf[2] = 0xEF; buf[3] = 0x00;
        return 4;
    }
};
const uint64_t StubT_Match::kFingerprint = fingerprint(makeCharsDescriptor());

struct StubT_Mismatch {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0x00; buf[1] = 0x01; buf[2] = 0x02; buf[3] = 0x03;
        return 4;
    }
};
const uint64_t StubT_Mismatch::kFingerprint = fingerprint(makeCharsDescriptor()) ^ 0xFF;

// ---------------------------------------------------------------------------
// Task 19: version-binding stub types.
//
// StubV1 is what a client compiled against schema version 1 looks like;
// StubV1::kVersion is deliberately 42 — NOT 1 — so a test asserting the
// stamped bytes == 1 cannot be passing by accident (it would fail if send()
// stamped the codegen-baked kVersion instead of the broker-bound version).
// ---------------------------------------------------------------------------
static Descriptor makeCharsV1Descriptor() {
    return DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .build();
}
static Descriptor makeCharsV2Descriptor() {
    return DescriptorBuilder("characters", 2)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .add("level",  PrimType::Int32)
        .build();
}

struct StubV1 {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    static constexpr uint16_t kVersion = 42;  // deliberately != 1 (see note above)
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        // Mirrors the real codegen layout: [blockLength:u16 LE][version:u16 LE].
        buf[0] = 0x04; buf[1] = 0x00;
        buf[2] = static_cast<uint8_t>(kVersion & 0xffu);
        buf[3] = static_cast<uint8_t>((kVersion >> 8) & 0xffu);
        return 4;
    }
};
const uint64_t StubV1::kFingerprint = fingerprint(makeCharsV1Descriptor());

/// Captures the exact bytes TypedTopic::send() hands to the producer — the
/// test seam used to assert the stamped version without a live Aeron round
/// trip (TypedTopic's second template param exists for exactly this).
struct StubCaptureProducer {
    explicit StubCaptureProducer(std::vector<uint8_t>* captured) noexcept : captured_(captured) {}
    void send(rune::client::TopicHandle, const rune::client::Key&,
             std::span<const uint8_t> value) noexcept {
        if (captured_) captured_->assign(value.begin(), value.end());
    }
    std::vector<uint8_t>* captured_ = nullptr;
};

/// Pumps a GateGateway from a helper thread while the test thread blocks
/// inside a Client *AndWait call (same pattern as HandleLifetimeTest's
/// GatewayPump, adapted to GateGateway::poll()).
struct GateGatewayPump {
    GateGateway&       gw;
    std::atomic<bool>  stopFlag{false};
    std::thread        t;
    explicit GateGatewayPump(GateGateway& g)
        : gw(g), t([this] {
              while (!stopFlag.load(std::memory_order_acquire)) {
                  if (gw.poll(16) == 0) std::this_thread::sleep_for(kPollSleep);
              }
          }) {}
    void stop() {
        if (t.joinable()) {
            stopFlag.store(true, std::memory_order_release);
            t.join();
        }
    }
    ~GateGatewayPump() { stop(); }
};

// ===========================================================================
// Test 1: openTopic<StubT_Match> resolves when kFingerprint matches
// ===========================================================================
TEST(OpenTopicGate, OpenTopicResolvesOnMatch) {
    GateHarness h(1300, 1301);
    h.build();
    h.connectAndWait();

    Descriptor d    = makeCharsDescriptor();
    h.gw->getBlob      = serialize(d);
    h.gw->getTopicId   = 0x40000001;
    h.gw->getVersion   = 1;
    h.gw->getErrorCode = 0;

    // F8c C3: getTopicSchema token op + TypedTopic::fromSchema on this thread
    // (no std::async continuation thread).
    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });

    h.pumpUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "openTopic<StubT_Match> future did not resolve";

    ASSERT_NO_THROW({
        auto tt = TypedTopic<StubT_Match>::fromSchema(*h.client, fut.outcome().c, fut.outcome().payload);
        (void)tt;
    }) << "openTopic<StubT_Match> should resolve without throwing";
}

// ===========================================================================
// Test 2: openTopic<StubT_Mismatch> fails with stale-type error
// ===========================================================================
TEST(OpenTopicGate, OpenTopicFailsOnMismatch) {
    GateHarness h(1302, 1303);
    h.build();
    h.connectAndWait();

    Descriptor d    = makeCharsDescriptor();
    h.gw->getBlob      = serialize(d);
    h.gw->getTopicId   = 0x40000001;
    h.gw->getVersion   = 1;
    h.gw->getErrorCode = 0;

    // F8c C3: getTopicSchema token op + TypedTopic::fromSchema on this thread
    // (no std::async continuation thread).
    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });

    h.pumpUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "openTopic<StubT_Mismatch> future did not resolve with error";

    EXPECT_THROW({
        auto tt = TypedTopic<StubT_Mismatch>::fromSchema(*h.client, fut.outcome().c, fut.outcome().payload);
        (void)tt;
    }, std::exception) << "openTopic<StubT_Mismatch> must throw (stale type)";
}

// ===========================================================================
// Test 3: After a Match resolve, send() does not throw
// ===========================================================================
TEST(OpenTopicGate, OpenTopicSendDoesNotThrow) {
    GateHarness h(1304, 1305);
    h.build();
    h.connectAndWait();

    Descriptor d    = makeCharsDescriptor();
    h.gw->getBlob      = serialize(d);
    h.gw->getTopicId   = 0x40000001;
    h.gw->getVersion   = 1;
    h.gw->getErrorCode = 0;

    // F8c C3: getTopicSchema token op + TypedTopic::fromSchema on this thread
    // (no std::async continuation thread).
    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });

    h.pumpUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "openTopic resolve timed out";

    TypedTopic<StubT_Match> tt =
        TypedTopic<StubT_Match>::fromSchema(*h.client, fut.outcome().c, fut.outcome().payload);

    // send() is fire-and-forget (void, noexcept); must not crash.
    // The ring may drop it (no active broker session on the runner), but
    // the buffer-reuse contract and encode path must work without UB.
    StubT_Match rec;
    ASSERT_NO_THROW(tt.send(rec)) << "send() must not throw";
    // Call send again to exercise buffer-reuse (same capacity, no realloc).
    ASSERT_NO_THROW(tt.send(rec)) << "second send() must not throw";
}

// ===========================================================================
// Task 19: version-binding typed gate + record version stamping
// ===========================================================================

// ---------------------------------------------------------------------------
// Test 4: openTopicAndWait<StubV1> binds the version whose fingerprint
// matches T::kFingerprint — NOT latest — and every send() stamps that bound
// version onto the record header (bytes [2..3], overwriting the codegen-baked
// kVersion).
//
// The topic is at v2 ("latest" — what a plain getTopicSchemaAndWait(name)
// would return), but the broker also still knows v1's materialized
// descriptor and answers a GetTopicSchema(requestedFingerprint =
// StubV1::kFingerprint) with {version=1, v1 blob} (Task 13's fingerprint-
// selecting lookup, Task 18's wire support).
// ===========================================================================
TEST(OpenTopicGate, BindsToVersionWhoseFingerprintMatchesAndStamps) {
    GateHarness h(1306, 1307);
    h.build();
    h.connectAndWait();

    // "Latest" (v2) — must NOT be what StubV1 binds to.
    h.gw->getTopicId   = 0x40000002;
    h.gw->getVersion   = 2;
    h.gw->getBlob      = serialize(makeCharsV2Descriptor());
    h.gw->getErrorCode = 0;

    // The fingerprint-selecting answer for StubV1's compiled-against version.
    h.gw->fingerprintOverrides[StubV1::kFingerprint] = GateGateway::FingerprintOverride{
        /*topicId=*/0x40000001, /*version=*/1, serialize(makeCharsV1Descriptor())};

    TypedTopic<StubV1> tt = [&] {
        GateGatewayPump pump(*h.gw);
        return h.client->openTopicAndWait<StubV1>("characters");
    }();

    EXPECT_EQ(tt.boundVersion(), 1u)
        << "openTopicAndWait<StubV1> must bind version 1 (its fingerprint's "
           "version), not latest (2)";

    // send() through the real (Aeron) producer must still not throw.
    StubV1 rec;
    ASSERT_NO_THROW(tt.send(rec)) << "send() must not throw";

    // Assert the stamped bytes via the TypedTopic<T, ProducerT> test seam,
    // bound to the SAME version openTopicAndWait returned above — proves
    // send() stamps the broker-bound version (1), not StubV1::kVersion (42,
    // deliberately different — see the stub's definition).
    std::vector<uint8_t> captured;
    TypedTopic<StubV1, StubCaptureProducer> stampTopic(
        StubCaptureProducer{&captured}, /*topicId=*/0x40000001, tt.boundVersion());
    stampTopic.send(rec);

    ASSERT_GE(captured.size(), 4u);
    const uint16_t stampedVersion =
        static_cast<uint16_t>(captured[2]) | (static_cast<uint16_t>(captured[3]) << 8);
    EXPECT_EQ(stampedVersion, 1u)
        << "send() must stamp the broker-bound version (1), not the "
           "codegen-baked StubV1::kVersion (42)";
}

// ===========================================================================
// Test 5: an unknown fingerprint (the broker has no version matching
// T::kFingerprint — e.g. an old generated client whose schema predates the
// topic's recorded history, or a genuinely stale one) still throws the
// "generated type is stale" error.
// ===========================================================================
TEST(OpenTopicGate, StaleFingerprintStillThrows) {
    GateHarness h(1308, 1309);
    h.build();
    h.connectAndWait();

    // "Latest" is v2; the broker's fingerprintOverrides table is left empty,
    // so ANY requestedFingerprint lookup (including StubV1's) is "not found".
    h.gw->getTopicId   = 0x40000002;
    h.gw->getVersion   = 2;
    h.gw->getBlob      = serialize(makeCharsV2Descriptor());
    h.gw->getErrorCode = 0;

    GateGatewayPump pump(*h.gw);
    EXPECT_THROW((void)h.client->openTopicAndWait<StubV1>("characters"), std::runtime_error)
        << "openTopicAndWait must throw when the broker has no version "
           "matching T::kFingerprint";
}

// ===========================================================================
// Regression: the version stamp always reflects the bound version, never the
// codegen-baked kVersion — pinned with a version that is NOT 1 so a test that
// accidentally asserted the baked version instead would fail loudly.
// ===========================================================================
TEST(OpenTopicGate, SendStampsBoundVersionNotCodegenVersionRegression) {
    std::vector<uint8_t> captured;
    TypedTopic<StubV1, StubCaptureProducer> topic(
        StubCaptureProducer{&captured}, /*topicId=*/1, /*version=*/7);

    StubV1 rec;
    topic.send(rec);

    ASSERT_GE(captured.size(), 4u);
    const uint16_t stampedVersion =
        static_cast<uint16_t>(captured[2]) | (static_cast<uint16_t>(captured[3]) << 8);
    EXPECT_EQ(stampedVersion, 7u)
        << "regression: send() must overwrite StubV1's baked kVersion (42) "
           "with the bound version (7)";
    EXPECT_NE(stampedVersion, StubV1::kVersion)
        << "regression: the stamped version must never be the codegen-baked "
           "kVersion once a different version is bound";
}
