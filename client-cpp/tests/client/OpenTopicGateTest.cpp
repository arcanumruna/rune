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
    return std::string(std::filesystem::temp_directory_path().string() + "/cc6_opentopicgate_test_") +
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
    int32_t              getPartitionCount = 0;    // Phase 2b C: trailing partitionCount
    bool                 omitPartitionCount = false; // simulate a pre-2b broker (no trailing i32)

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
        int32_t               partitionCount = 0;   // Phase 2b C
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
                    resp.partitionCount = it->second.partitionCount;
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
                resp.partitionCount = getPartitionCount;
            }

            std::vector<uint8_t> scratch(resp.encodedSize() + 32u);
            const std::size_t n = resp.encode(scratch.data());
            // A pre-2b broker's frame stops after the descriptor blob.
            if (n > 0) sendFrame(scratch.data(), omitPartitionCount ? n - 4u : n);
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
        StubCaptureProducer{&captured}, TopicHandle{0x40000001, 1}, tt.boundVersion());
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
// Regression: a broker that answers Ok (errorCode 0) to a fingerprint-
// selecting GetTopicSchema but hands back a descriptor blob whose ACTUAL
// fingerprint does not equal T::kFingerprint (a broker mis-selection bug —
// e.g. a lookup-table off-by-one landing on the wrong version) must still be
// caught client-side: openTopicAndWait must never bind a typed client to the
// wrong shape just because the broker said Ok.  This is the client-side
// re-check restored for consistency with TypedTopic::fromSchema, which has
// always performed it on its own completion path.
// ===========================================================================
TEST(OpenTopicGate, BrokerMisselectedBlobStillThrowsRegression) {
    GateHarness h(1310, 1311);
    h.build();
    h.connectAndWait();

    // "Latest" is v2.
    h.gw->getTopicId   = 0x40000002;
    h.gw->getVersion   = 2;
    h.gw->getBlob      = serialize(makeCharsV2Descriptor());
    h.gw->getErrorCode = 0;

    // Simulate a broker mis-selection: it answers StubV1's fingerprint
    // request with errorCode 0 (success) and version=1, but the blob it
    // actually sends back is v2's descriptor — whose fingerprint does NOT
    // match StubV1::kFingerprint.
    h.gw->fingerprintOverrides[StubV1::kFingerprint] = GateGateway::FingerprintOverride{
        /*topicId=*/0x40000001, /*version=*/1, serialize(makeCharsV2Descriptor())};

    GateGatewayPump pump(*h.gw);
    EXPECT_THROW((void)h.client->openTopicAndWait<StubV1>("characters"), std::runtime_error)
        << "openTopicAndWait must throw when the broker's Ok response carries "
           "a blob whose fingerprint doesn't match T::kFingerprint, even "
           "though errorCode==0";
}

// ===========================================================================
// Regression: the version stamp always reflects the bound version, never the
// codegen-baked kVersion — pinned with a version that is NOT 1 so a test that
// accidentally asserted the baked version instead would fail loudly.
// ===========================================================================
TEST(OpenTopicGate, SendStampsBoundVersionNotCodegenVersionRegression) {
    std::vector<uint8_t> captured;
    TypedTopic<StubV1, StubCaptureProducer> topic(
        StubCaptureProducer{&captured}, TopicHandle{1, 1}, /*version=*/7);

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

// ===========================================================================
// GC6 — typed-client binding across a full schema lineage (test-matrix D3-D11)
//
// A single topic ("characters") evolved through FOUR versions. v3 DROPS a
// field present at v1/v2 ("name"); v4 re-adds a field in its place
// ("displayName") — the drop/rename-successor lineage member required by
// GC6. A client compiled against EACH vK must fingerprint-select and bind
// EXACTLY vK (never "latest"), and every send() must stamp the BOUND
// version K into the record header, not the stub's own (deliberately
// different) codegen-baked kVersion.
// ===========================================================================
static Descriptor makeLineageV1Descriptor() {
    return DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .build();
}
static Descriptor makeLineageV2Descriptor() {
    return DescriptorBuilder("characters", 2)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name",   PrimType::String)
        .add("level",  PrimType::Int32)
        .build();
}
// v3 DROPS "name" (present at v1/v2) — the dropped-field lineage member.
static Descriptor makeLineageV3Descriptor() {
    return DescriptorBuilder("characters", 3)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("level",  PrimType::Int32)
        .build();
}
// v4 adds "displayName" in place of the dropped "name" — the
// rename-successor lineage member.
static Descriptor makeLineageV4Descriptor() {
    return DescriptorBuilder("characters", 4)
        .add("id",          PrimType::Int64)
        .add("userId",      PrimType::Int64, 1, /*isKey=*/true)
        .add("level",       PrimType::Int32)
        .add("displayName", PrimType::String)
        .build();
}

// Each StubLineageVK is "what a client compiled against schema version K
// looks like": kFingerprint comes from that version's own descriptor
// (distinct per K), and kVersion is DELIBERATELY not K (and distinct from
// every other stub's kVersion), exactly like StubV1 above — so a stamp
// assertion that accidentally checked the codegen-baked kVersion instead of
// the broker-bound version would fail loudly for every K.
struct StubLineageV1 {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    static constexpr uint16_t kVersion = 101;
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0x04; buf[1] = 0x00;
        buf[2] = static_cast<uint8_t>(kVersion & 0xffu);
        buf[3] = static_cast<uint8_t>((kVersion >> 8) & 0xffu);
        return 4;
    }
};
const uint64_t StubLineageV1::kFingerprint = fingerprint(makeLineageV1Descriptor());

struct StubLineageV2 {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    static constexpr uint16_t kVersion = 102;
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0x04; buf[1] = 0x00;
        buf[2] = static_cast<uint8_t>(kVersion & 0xffu);
        buf[3] = static_cast<uint8_t>((kVersion >> 8) & 0xffu);
        return 4;
    }
};
const uint64_t StubLineageV2::kFingerprint = fingerprint(makeLineageV2Descriptor());

// The dropped-field version's compiled client.
struct StubLineageV3 {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    static constexpr uint16_t kVersion = 103;
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0x04; buf[1] = 0x00;
        buf[2] = static_cast<uint8_t>(kVersion & 0xffu);
        buf[3] = static_cast<uint8_t>((kVersion >> 8) & 0xffu);
        return 4;
    }
};
const uint64_t StubLineageV3::kFingerprint = fingerprint(makeLineageV3Descriptor());

struct StubLineageV4 {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    static constexpr uint16_t kVersion = 104;
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0x04; buf[1] = 0x00;
        buf[2] = static_cast<uint8_t>(kVersion & 0xffu);
        buf[3] = static_cast<uint8_t>((kVersion >> 8) & 0xffu);
        return 4;
    }
};
const uint64_t StubLineageV4::kFingerprint = fingerprint(makeLineageV4Descriptor());

// A descriptor that was NEVER part of this lineage (never served at any
// version, not even latest) — used to prove the stale-schema error is a
// genuine "not found" even when the broker's table carries a long history.
static Descriptor makeLineageUnknownDescriptor() {
    return DescriptorBuilder("characters", 999)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("bogus",  PrimType::Int32)
        .build();
}
struct StubLineageUnknown {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "characters";
    static constexpr uint16_t kVersion = 999;
    size_t encodedSize() const noexcept { return 4; }
    size_t encode(uint8_t* buf) const noexcept {
        buf[0] = 0x04; buf[1] = 0x00;
        buf[2] = static_cast<uint8_t>(kVersion & 0xffu);
        buf[3] = static_cast<uint8_t>((kVersion >> 8) & 0xffu);
        return 4;
    }
};
const uint64_t StubLineageUnknown::kFingerprint = fingerprint(makeLineageUnknownDescriptor());

// Opens "characters" as StubT against an already-connected harness, asserts
// the bind lands on EXACTLY expectedVersion (never "latest"), then asserts
// send() stamps expectedVersion — not StubT::kVersion — into header
// bytes [2..3].
template <typename StubT>
static void expectLineageClientBindsExactVersionAndStamps(GateHarness& h, uint16_t expectedVersion) {
    TypedTopic<StubT> tt = [&] {
        GateGatewayPump pump(*h.gw);
        return h.client->openTopicAndWait<StubT>("characters");
    }();

    EXPECT_EQ(tt.boundVersion(), expectedVersion)
        << "a client compiled against v" << expectedVersion
        << " must bind exactly v" << expectedVersion << ", never latest";

    StubT rec;
    ASSERT_NO_THROW(tt.send(rec)) << "send() must not throw for v" << expectedVersion;

    // Capture the exact bytes send() hands the producer, bound to the SAME
    // version openTopicAndWait returned above.
    std::vector<uint8_t> captured;
    TypedTopic<StubT, StubCaptureProducer> stampTopic(
        StubCaptureProducer{&captured}, TopicHandle{0x40000010, 1}, tt.boundVersion());
    stampTopic.send(rec);

    ASSERT_GE(captured.size(), 4u);
    const uint16_t stampedVersion =
        static_cast<uint16_t>(captured[2]) | (static_cast<uint16_t>(captured[3]) << 8);
    EXPECT_EQ(stampedVersion, expectedVersion)
        << "send() must stamp the broker-bound version (" << expectedVersion
        << "), not the codegen-baked kVersion (" << StubT::kVersion << ")";
    EXPECT_NE(stampedVersion, StubT::kVersion)
        << "stamped version must never equal the (deliberately different) "
           "codegen-baked kVersion for v" << expectedVersion;
}

// ===========================================================================
// GC6 / D3-D8, D11: a topic currently at "latest" = v4 still answers
// fingerprint-selecting GetTopicSchema for EVERY historical version in its
// lineage. Clients compiled against v1, v2, v3 (the dropped-field version)
// and v4 (== latest here) each bind their own K and stamp K, never latest
// and never each other's K.
// ===========================================================================
TEST(OpenTopicGate, LineageBindsExactVersionForEachCompiledClientRegression) {
    GateHarness h(1312, 1313);
    h.build();
    h.connectAndWait();

    // "Latest" is v4.
    h.gw->getTopicId   = 0x40000010;
    h.gw->getVersion   = 4;
    h.gw->getBlob      = serialize(makeLineageV4Descriptor());
    h.gw->getErrorCode = 0;

    // The broker's fingerprint-selecting GetTopicSchema answers for every
    // historical version in the lineage (the broker-side per-version
    // resolution itself is covered by other tasks; this harness only needs
    // to emulate its observable behavior).
    h.gw->fingerprintOverrides[StubLineageV1::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 1, serialize(makeLineageV1Descriptor())};
    h.gw->fingerprintOverrides[StubLineageV2::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 2, serialize(makeLineageV2Descriptor())};
    h.gw->fingerprintOverrides[StubLineageV3::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 3, serialize(makeLineageV3Descriptor())};
    h.gw->fingerprintOverrides[StubLineageV4::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 4, serialize(makeLineageV4Descriptor())};

    expectLineageClientBindsExactVersionAndStamps<StubLineageV1>(h, 1);
    expectLineageClientBindsExactVersionAndStamps<StubLineageV2>(h, 2);
    expectLineageClientBindsExactVersionAndStamps<StubLineageV3>(h, 3); // dropped-field version
    expectLineageClientBindsExactVersionAndStamps<StubLineageV4>(h, 4); // == latest here
}

// ===========================================================================
// GC6 / D9: a client whose fingerprint matches NO version the broker has
// ever served — even though the broker's table carries a full 4-version
// lineage, not an empty one — must still throw the stale-schema error.
// ===========================================================================
TEST(OpenTopicGate, LineageUnknownFingerprintStillThrowsRegression) {
    GateHarness h(1314, 1315);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId   = 0x40000010;
    h.gw->getVersion   = 4;
    h.gw->getBlob      = serialize(makeLineageV4Descriptor());
    h.gw->getErrorCode = 0;

    h.gw->fingerprintOverrides[StubLineageV1::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 1, serialize(makeLineageV1Descriptor())};
    h.gw->fingerprintOverrides[StubLineageV2::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 2, serialize(makeLineageV2Descriptor())};
    h.gw->fingerprintOverrides[StubLineageV3::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 3, serialize(makeLineageV3Descriptor())};
    h.gw->fingerprintOverrides[StubLineageV4::kFingerprint] = GateGateway::FingerprintOverride{
        0x40000010, 4, serialize(makeLineageV4Descriptor())};

    GateGatewayPump pump(*h.gw);
    EXPECT_THROW((void)h.client->openTopicAndWait<StubLineageUnknown>("characters"), std::runtime_error)
        << "openTopicAndWait must throw when the broker's full lineage table "
           "still has no version matching T::kFingerprint";
}

// ===========================================================================
// Issue #435: a lineage request an OLD broker answers UnknownTopic (it predates lineage) must not read as "the topic
// does not exist" when the latest schema of that topic is served.
// ===========================================================================
TEST(OpenTopicGateRegression, LineageAgainstAnOldBrokerNamesTheVersionSkewRegression) {
    GateHarness h(1330, 1331);
    h.build();
    h.connectAndWait();
    h.gw->getTopicId                  = 0x40000011;
    h.gw->getVersion                  = 1;
    h.gw->getBlob                     = serialize(makeCharsDescriptor());
    h.gw->getErrorCode                = 0;   // the latest schema IS served
    h.gw->unknownFingerprintErrorCode = 7;   // ... but the lineage fingerprint is "unknown topic"
    GateGatewayPump pump(*h.gw);
    const std::string msg = h.client->lineageFailure("t", 7, 5s);
    EXPECT_NE(msg.find("does not serve the schema lineage"), std::string::npos) << msg;
    EXPECT_NE(msg.find("upgrade"), std::string::npos) << msg;
    EXPECT_EQ(msg.find("does not exist"), std::string::npos) << msg;
}

TEST(OpenTopicGateRegression, LineageOfAMissingTopicSaysItDoesNotExistRegression) {
    GateHarness h(1332, 1333);
    h.build();
    h.connectAndWait();
    h.gw->getErrorCode                = 7;
    h.gw->unknownFingerprintErrorCode = 7;
    GateGatewayPump pump(*h.gw);
    const std::string msg = h.client->lineageFailure("t", 7, 5s);
    EXPECT_NE(msg.find("topic 't' does not exist"), std::string::npos) << msg;
    // Any other code is named as is, with no probe.
    const std::string other = h.client->lineageFailure("t", 45, 5s);
    EXPECT_NE(other.find("did not serve the schema lineage of 't' (error 45)"), std::string::npos) << other;
}

// Adversarial review of C8 (#435): the probe that disambiguates UnknownTopic must never turn a probe timeout or a
// refused probe into "does not exist" (or "upgrade"), and must end within its own timeout.
TEST(OpenTopicGateRegression, LineageProbeTimeoutFallsBackToThePlainCodeAdversarialRegression) {
    GateHarness h(1334, 1335);
    h.build();
    h.connectAndWait();
    h.gw->replyGetTopicSchema = false;   // the broker never answers the probe
    GateGatewayPump pump(*h.gw);
    const auto t0  = std::chrono::steady_clock::now();
    const std::string msg = h.client->lineageFailure("t", 7, 300ms);
    const auto took = std::chrono::steady_clock::now() - t0;
    EXPECT_NE(msg.find("did not serve the schema lineage of 't' (error 7)"), std::string::npos) << msg;
    EXPECT_EQ(msg.find("does not exist"), std::string::npos) << "an unanswered probe proves nothing: " << msg;
    EXPECT_EQ(msg.find("upgrade"), std::string::npos) << msg;
    EXPECT_LT(took, 5s) << "the probe is bounded by its timeout";
}

TEST(OpenTopicGateRegression, LineageProbeRefusedNamesTheProbeCodeAdversarialRegression) {
    GateHarness h(1336, 1337);
    h.build();
    h.connectAndWait();
    h.gw->unknownFingerprintErrorCode = 7;
    GateGatewayPump pump(*h.gw);
    for (const uint16_t probeCode : {uint16_t{45}, uint16_t{1}, uint16_t{0x7FFF}}) {
        h.gw->getErrorCode = probeCode;
        const std::string msg = h.client->lineageFailure("t", 7, 5s);
        EXPECT_NE(msg.find("(error " + std::to_string(probeCode) + ")"), std::string::npos) << probeCode << ": " << msg;
        EXPECT_EQ(msg.find("does not exist"), std::string::npos) << probeCode << ": " << msg;
        EXPECT_EQ(msg.find("upgrade"), std::string::npos) << probeCode << ": " << msg;
    }
}

// ===========================================================================
// Phase 2b Part C — the broker-reported partition count reaches the client:
// TopicSchemaResponse trailing partitionCount -> Completion.reserved0 ->
// TopicSchemaResult.partitionCount.
// ===========================================================================
TEST(OpenTopicGateRegression, GetTopicSchemaReportsBrokerPartitionCount) {
    GateHarness h(1316, 1317);
    h.build();
    h.connectAndWait();

    h.gw->getBlob           = serialize(makeCharsDescriptor());
    h.gw->getTopicId        = 0x40000001;
    h.gw->getVersion        = 1;
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = 8;

    // Token form: the completion itself carries it in reserved0.
    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });
    h.pumpUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());
    ASSERT_TRUE(fut.ok()) << fut.describe();
    EXPECT_EQ(fut.outcome().c.reserved0, 8u);
    EXPECT_EQ(fut.schemaResult().partitionCount, 8);

    // *AndWait form.
    GateGatewayPump pump(*h.gw);
    const TopicSchemaResult r = h.client->getTopicSchemaAndWait("characters");
    EXPECT_EQ(r.errorCode, 0u);
    EXPECT_EQ(r.topicId, 0x40000001);
    EXPECT_EQ(r.partitionCount, 8);
}

TEST(OpenTopicGateRegression, PreTwoBBrokerReportsZeroPartitionCount) {
    GateHarness h(1318, 1319);
    h.build();
    h.connectAndWait();

    h.gw->getBlob            = serialize(makeCharsDescriptor());
    h.gw->getTopicId         = 0x40000001;
    h.gw->getVersion         = 1;
    h.gw->getErrorCode       = 0;
    h.gw->getPartitionCount  = 8;      // encoded, then cut off below
    h.gw->omitPartitionCount = true;   // the 40-byte pre-2b frame

    GateGatewayPump pump(*h.gw);
    const TopicSchemaResult r = h.client->getTopicSchemaAndWait("characters");
    EXPECT_EQ(r.errorCode, 0u);
    EXPECT_FALSE(r.descriptorBlob.empty()) << "the pre-2b frame must still decode fully";
    EXPECT_EQ(r.partitionCount, 0) << "not reported -> 0, never garbage";
}

// ===========================================================================
// Phase 2b Part C — the typed topic binds the broker-reported partition count
// (both open paths), and falls back to 1 against a pre-2b broker.
// ===========================================================================
TEST(OpenTopicGateRegression, OpenTopicAndWaitBindsBrokerPartitionCount) {
    GateHarness h(1320, 1321);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId        = 0x40000002;
    h.gw->getVersion        = 2;
    h.gw->getBlob           = serialize(makeCharsV2Descriptor());
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = 8;
    GateGateway::FingerprintOverride ov{/*topicId=*/0x40000001, /*version=*/1,
                                        serialize(makeCharsV1Descriptor())};
    ov.partitionCount = 8;
    h.gw->fingerprintOverrides[StubV1::kFingerprint] = ov;

    GateGatewayPump pump(*h.gw);
    TypedTopic<StubV1> tt = h.client->openTopicAndWait<StubV1>("characters");
    EXPECT_EQ(tt.handle().topicId, 0x40000001);
    EXPECT_EQ(tt.handle().partitionCount, 8)
        << "regression: TypedTopic was hard-wired to TopicHandle{id, 1}";
    EXPECT_EQ(tt.boundVersion(), 1u);
}

TEST(OpenTopicGateRegression, FromSchemaBindsBrokerPartitionCount) {
    GateHarness h(1322, 1323);
    h.build();
    h.connectAndWait();

    h.gw->getBlob           = serialize(makeCharsDescriptor());
    h.gw->getTopicId        = 0x40000001;
    h.gw->getVersion        = 1;
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = 8;

    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });
    h.pumpUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());

    TypedTopic<StubT_Match> tt =
        TypedTopic<StubT_Match>::fromSchema(*h.client, fut.outcome().c, fut.outcome().payload);
    EXPECT_EQ(tt.handle().topicId, 0x40000001);
    EXPECT_EQ(tt.handle().partitionCount, 8);
}

TEST(OpenTopicGateRegression, PreTwoBBrokerTypedTopicFallsBackToOnePartition) {
    GateHarness h(1324, 1325);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId         = 0x40000001;
    h.gw->getVersion         = 1;
    h.gw->getBlob            = serialize(makeCharsDescriptor());
    h.gw->getErrorCode       = 0;
    h.gw->omitPartitionCount = true;   // pre-2b frame: no trailing partitionCount
    h.gw->fingerprintOverrides[StubT_Match::kFingerprint] = GateGateway::FingerprintOverride{
        /*topicId=*/0x40000001, /*version=*/1, serialize(makeCharsDescriptor())};

    GateGatewayPump pump(*h.gw);
    TypedTopic<StubT_Match> tt = h.client->openTopicAndWait<StubT_Match>("characters");
    EXPECT_EQ(tt.handle().partitionCount, 1)
        << "unreported (0) must become 1 — partition 0 as before 2b, never an out-of-range partition";
}

// Review carry-over: ANY non-positive reported count (not only 0) is "not
// reported" — a negative count must never reach Key::partition() (modulo by a
// negative / out-of-range partition).  Falls back to 1, like a pre-2b broker.
TEST(OpenTopicGateRegression, NegativeReportedPartitionCountFallsBackToOnePartition) {
    GateHarness h(1326, 1327);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId        = 0x40000001;
    h.gw->getVersion        = 1;
    h.gw->getBlob           = serialize(makeCharsDescriptor());
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = -3;
    GateGateway::FingerprintOverride ov{/*topicId=*/0x40000001, /*version=*/1,
                                        serialize(makeCharsDescriptor())};
    ov.partitionCount = -3;
    h.gw->fingerprintOverrides[StubT_Match::kFingerprint] = ov;

    {
        GateGatewayPump pump(*h.gw);
        TypedTopic<StubT_Match> tt = h.client->openTopicAndWait<StubT_Match>("characters");
        EXPECT_EQ(tt.handle().partitionCount, 1) << "openTopicAndWait: negative reported count -> 1";
    }

    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });
    h.pumpUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());
    TypedTopic<StubT_Match> tt2 =
        TypedTopic<StubT_Match>::fromSchema(*h.client, fut.outcome().c, fut.outcome().payload);
    EXPECT_EQ(tt2.handle().partitionCount, 1) << "fromSchema: negative reported count -> 1";
}
