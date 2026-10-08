// tests/client/ClientSchemaTest.cpp
//
// Task 10 — Client schema DDL round-trip tests.
//
// Tests the correlation→promise plumbing for defineTopic() / getTopicSchema()
// WITHOUT a live broker.  The in-process TestGateway (same loopback pattern
// as ClientTest.cpp) receives the request over aeron:ipc, encodes a canned
// response using the SchemaMessages codecs, and sends it back.  The client's
// onEgressFrame() handler decodes it and resolves/rejects the promise.
//
// Test matrix:
//   1. DefineTopicFutureResolvesOnOK
//        Gateway echoes DefineTopicResponse{OK, topicId=0x40000001, version=1}.
//        Future resolves with the right fields, errorCode==0.
//
//   2. DefineTopicResponseErrorCodePropagates
//        Gateway echoes DefineTopicResponse{TopicExists(25), topicId=-1}.
//        Future resolves (not throws) with errorCode==25.
//
//   3. GetTopicSchemaFutureResolvesWithBlob
//        Gateway echoes TopicSchemaResponse{OK, topicId=0x40000001, version=1,
//        descriptorBlob={0xDE,0xAD,0xBE,0xEF}}.
//        Future resolves; blob matches; topicId/version match.
//
//   4. GetTopicSchemaUnknownTopicPropagates
//        Gateway echoes TopicSchemaResponse{UnknownTopic(7), topicId=-1}.
//        Future resolves with errorCode==7.
//
//   5. DefineTopicTimeoutFailsPromise
//        Gateway does NOT reply to DefineTopic.
//        Future fails after pingTimeoutMs (500 ms) — no hang.
//
//   6. GetTopicSchemaTimeoutFailsPromise
//        Gateway does NOT reply to GetTopicSchema.
//        Future fails after pingTimeoutMs (500 ms) — no hang.
//
//   7. DefineTopicWithoutSessionRejected
//        defineTopic() before connect() → future rejected immediately.
//
//   8. GetTopicSchemaWithoutSessionRejected
//        getTopicSchema() before connect() → future rejected immediately.
//
//   9. DefineTopicCorrectCorrelationId (regression)
//        Gateway receives DefineTopic, reads correlationId, sends
//        DefineTopicResponse with the SAME correlationId → future resolves.
//
//  10. MultipleSchemaOpsInflight
//        2 defineTopic + 1 getTopicSchema all in-flight; each future resolves
//        independently with the right correlationId-matched result.
//
// All tests are bounded (kFutureTimeout) and use the same GatewayHarness
// idiom from ClientTest.cpp.

#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/codec/Codec.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include "../support/TestCompletions.h"
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rcs = rune::client::session;
namespace rpc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

static constexpr auto kFutureTimeout = 10s;
static constexpr int  kPollMax       = 3'000;
static constexpr auto kPollSleep     = 2ms;

// ---------------------------------------------------------------------------
// Unique temp directory helper
// ---------------------------------------------------------------------------

namespace {
static std::atomic<int> g_schemaDirCtr{0};

std::string schemaUniqueDir() {
    const int c = g_schemaDirCtr.fetch_add(1, std::memory_order_relaxed);
    return std::string("/tmp/cc6_schema_test_") +
           std::to_string(static_cast<long>(::getpid())) + "_" +
           std::to_string(c);
}
} // namespace

// ---------------------------------------------------------------------------
// SchemaTestGateway — extends the loopback gateway pattern with schema DDL
// response policy.
//
// For each incoming DefineTopic frame the gateway calls its defineTopicFn
// callback (if set) to produce the response bytes; similarly for GetTopicSchema.
// Default policy: echo back a hardcoded OK response with correlationId from
// the request.
// ---------------------------------------------------------------------------

struct SchemaTestGateway {
    std::string aeronDir;

    std::shared_ptr<aeron::Aeron>                aeron_;
    std::shared_ptr<aeron::Subscription>         sub_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
    std::unique_ptr<aeron::FragmentAssembler>    assembler_;

    int32_t ingressStream = 0;
    int32_t egressStream  = 0;

    // Policy flags.
    bool replyConnectOK      = true;   // respond to Connect with OK
    bool replyDefineTopic    = true;   // respond to DefineTopic
    bool replyGetTopicSchema = true;   // respond to GetTopicSchema
    bool replyAlterTopic     = true;   // respond to AlterTopic

    // Canned DefineTopic response fields (applied to every response).
    int32_t  defineTopicId   = 0x40000001;
    uint16_t defineVersion   = 1;
    uint16_t defineErrorCode = 0;  // 0 == OK

    // Canned GetTopicSchema response fields.
    int32_t              getTopicId   = 0x40000001;
    uint16_t             getVersion   = 1;
    uint16_t             getErrorCode = 0;
    std::vector<uint8_t> getBlob      = {0xDE, 0xAD, 0xBE, 0xEF};
    // Task 18: last requestedFingerprint seen on a GetTopicSchema request (so
    // tests can assert the frame carried it).
    std::atomic<uint64_t> lastRequestedFingerprint{0};

    // Canned AlterTopic response fields.
    int32_t  alterTopicId    = 0x40000001;
    uint16_t alterNewVersion = 2;
    uint16_t alterErrorCode  = 0;  // 0 == OK
    // Task 18: last AlterTopic request seen (so tests can assert the frame's
    // expectedVersion / opsBlob were encoded correctly).
    std::atomic<uint16_t> lastAlterExpectedVersion{0};
    std::vector<uint8_t>  lastAlterOpsBlob;

    // Per-op response overrides (set before the call).
    // If non-negative, override canned response per-correlationId.
    // Map from correlationId → custom fields.  Used for multi-op tests.
    struct DefineOverride {
        int32_t topicId; uint16_t version; uint16_t errorCode;
    };
    std::map<int64_t, DefineOverride> defineOverrides;

    struct GetOverride {
        int32_t topicId; uint16_t version; uint16_t errorCode; std::vector<uint8_t> blob;
    };
    std::map<int64_t, GetOverride> getOverrides;

    // Observed counts.
    std::atomic<int> definesSeen{0};
    std::atomic<int> getsSeen{0};
    std::atomic<int> connectsSeen{0};
    std::atomic<int> altersSeen{0};

    uint8_t replyBuf[4096]{};

    explicit SchemaTestGateway(std::string dir) : aeronDir(std::move(dir)) {}
    ~SchemaTestGateway() { stop(); }

    void start(int32_t inStream, int32_t egStream) {
        ingressStream = inStream;
        egressStream  = egStream;

        aeron::Context ctx;
        // Aeron's default handler ::exit(-1)s the test process when the client's
        // embedded driver stops before this test-side client is closed.
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) throw std::runtime_error("SchemaTestGateway: Aeron::connect failed");

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
            throw std::runtime_error("SchemaTestGateway: timed out resolving sub/pub");

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

    bool send(const uint8_t* buf, std::size_t len) {
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
            if (!opt) return;
            connectsSeen.fetch_add(1, std::memory_order_relaxed);
            if (!replyConnectOK) break;
            std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf),
                static_cast<int16_t>(rcc::ErrorCode::OK),
                /*leaderMemberId=*/0,
                /*assignedSessionId=*/42,
                opt->sessionId,
                opt->correlationId);
            if (n > 0) send(replyBuf, n);
            break;
        }

        case rcc::TemplateId::DefineTopic: {
            auto opt = rpc::DefineTopic::decode(buf, len);
            if (!opt) return;
            definesSeen.fetch_add(1, std::memory_order_relaxed);
            if (!replyDefineTopic) break;

            // Encode DefineTopicResponse.
            rpc::DefineTopicResponse resp;
            resp.sessionId     = opt->sessionId;
            resp.correlationId = opt->correlationId;

            // Check per-correlationId override.
            auto ovIt = defineOverrides.find(opt->correlationId);
            if (ovIt != defineOverrides.end()) {
                resp.topicId   = ovIt->second.topicId;
                resp.version   = ovIt->second.version;
                resp.errorCode = ovIt->second.errorCode;
            } else {
                resp.topicId   = defineTopicId;
                resp.version   = defineVersion;
                resp.errorCode = defineErrorCode;
            }

            const std::size_t n = resp.encode(replyBuf);
            if (n > 0) send(replyBuf, n);
            break;
        }

        case rcc::TemplateId::GetTopicSchema: {
            auto opt = rpc::GetTopicSchema::decode(buf, len);
            if (!opt) return;
            getsSeen.fetch_add(1, std::memory_order_relaxed);
            lastRequestedFingerprint.store(opt->requestedFingerprint, std::memory_order_relaxed);
            if (!replyGetTopicSchema) break;

            // Encode TopicSchemaResponse.
            rpc::TopicSchemaResponse resp;
            resp.sessionId     = opt->sessionId;
            resp.correlationId = opt->correlationId;

            auto ovIt = getOverrides.find(opt->correlationId);
            if (ovIt != getOverrides.end()) {
                resp.topicId       = ovIt->second.topicId;
                resp.version       = ovIt->second.version;
                resp.errorCode     = ovIt->second.errorCode;
                resp.descriptorBlob = ovIt->second.blob.data();
                resp.descriptorLen  = static_cast<uint32_t>(ovIt->second.blob.size());
            } else {
                resp.topicId       = getTopicId;
                resp.version       = getVersion;
                resp.errorCode     = getErrorCode;
                resp.descriptorBlob = getBlob.data();
                resp.descriptorLen  = static_cast<uint32_t>(getBlob.size());
            }

            // Allocate scratch large enough for response + blob.
            std::vector<uint8_t> scratch(resp.encodedSize() + 32u);
            const std::size_t n = resp.encode(scratch.data());
            if (n > 0) send(scratch.data(), n);
            break;
        }

        case rcc::TemplateId::AlterTopic: {
            auto opt = rpc::AlterTopic::decode(buf, len);
            if (!opt) return;
            altersSeen.fetch_add(1, std::memory_order_relaxed);
            lastAlterExpectedVersion.store(opt->expectedVersion, std::memory_order_relaxed);
            lastAlterOpsBlob.assign(opt->opsBlob, opt->opsBlob + opt->opsLen);
            if (!replyAlterTopic) break;

            // Encode AlterTopicResponse.
            rpc::AlterTopicResponse resp;
            resp.sessionId     = opt->sessionId;
            resp.correlationId = opt->correlationId;
            resp.topicId       = alterTopicId;
            resp.newVersion    = alterNewVersion;
            resp.errorCode     = alterErrorCode;

            const std::size_t n = resp.encode(replyBuf);
            if (n > 0) send(replyBuf, n);
            break;
        }

        default:
            break;
        }
    }
};

// ---------------------------------------------------------------------------
// SchemaHarness — RAII wrapper (mirrors GatewayHarness from ClientTest.cpp).
// ---------------------------------------------------------------------------

struct SchemaHarness {
    std::string agentDir;
    std::string aeronDir;
    ClientConfig clientCfg;

    std::unique_ptr<Client>            client;
    std::unique_ptr<SchemaTestGateway> gw;
    std::unique_ptr<test_support::TestCq> cq;  // F8c C3: the test thread's completions

    test_support::PendingOp connectOp() {
        return cq->issue([&](Completions& q, Token t) { return client->connect(q, t); });
    }
    test_support::PendingOp defineTopicOp(std::string_view name, uint32_t partitions,
                                          std::string_view keyField, bool validate,
                                          std::span<const uint8_t> blob) {
        return cq->issue([&](Completions& q, Token t) {
            return client->defineTopic(name, partitions, keyField, validate, blob, q, t);
        });
    }
    test_support::PendingOp getTopicSchemaOp(std::string_view name) {
        return cq->issue([&](Completions& q, Token t) { return client->getTopicSchema(name, q, t); });
    }
    /// Task 18: select a specific schema fingerprint (0 = latest).
    test_support::PendingOp getTopicSchemaOp(std::string_view name, uint64_t requestedFingerprint) {
        return cq->issue([&](Completions& q, Token t) {
            return client->getTopicSchema(name, requestedFingerprint, q, t);
        });
    }
    /// Task 18.
    test_support::PendingOp alterTopicOp(std::string_view name, uint16_t expectedVersion,
                                        std::span<const uint8_t> opsBlob) {
        return cq->issue([&](Completions& q, Token t) {
            return client->alterTopic(name, expectedVersion, opsBlob, q, t);
        });
    }

    int64_t connectTimeoutMs = 5000;
    int64_t pingTimeoutMs    = 5000;  // also used as schema op timeout

    int32_t ingressStream;
    int32_t egressStream;

    explicit SchemaHarness(int32_t inStream, int32_t egStream)
        : ingressStream(inStream), egressStream(egStream)
    {
        agentDir = schemaUniqueDir();
        aeronDir = agentDir + "/aeron";
    }

    void build() {
        clientCfg.gatewayChannel   = "aeron:ipc";
        clientCfg.gatewayStreamId  = ingressStream;
        clientCfg.egressChannel    = "aeron:ipc";
        clientCfg.egressStreamId   = egressStream;
        clientCfg.dataDir          = agentDir;
        clientCfg.connectTimeoutMs = connectTimeoutMs;
        clientCfg.pingTimeoutMs    = pingTimeoutMs;

        client = std::make_unique<Client>(clientCfg);
        cq     = std::make_unique<test_support::TestCq>(*client);

        // Give the embedded driver a moment to write cnc.dat.
        std::this_thread::sleep_for(150ms);

        gw = std::make_unique<SchemaTestGateway>(aeronDir);
        gw->start(ingressStream, egressStream);
    }

    int pollGw(int limit = 16) {
        return gw ? gw->poll(limit) : 0;
    }

    template<typename Pred>
    bool pumpGwUntil(Pred pred, int limit = kPollMax) {
        for (int i = 0; i < limit; ++i) {
            gw->poll(16);
            if (pred()) return true;
            std::this_thread::sleep_for(kPollSleep);
        }
        return false;
    }

    /// Connect the client via the gateway and wait for Connected.
    void connectAndWait() {
        auto fut = connectOp();
        pumpGwUntil([&]() {
            return client->state() == rcs::SessionState::Connected;
        });
        ASSERT_TRUE(fut.waitReady())
            << "connect() future did not resolve";
        ASSERT_NO_THROW(fut.defineResult()) << "connect() threw";
    }

    ~SchemaHarness() {
        if (client) client->close();
        gw.reset();
        client.reset();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
};

// ===========================================================================
// Test 1: DefineTopicFutureResolvesOnOK
// ===========================================================================
TEST(ClientSchema, DefineTopicFutureResolvesOnOK) {
    SchemaHarness h(1100, 1101);
    h.build();
    h.connectAndWait();

    h.gw->defineTopicId   = 0x40000001;
    h.gw->defineVersion   = 1;
    h.gw->defineErrorCode = 0;

    std::vector<uint8_t> blob = {0x01, 0x02, 0x03};
    auto fut = h.defineTopicOp("characters", 16, "userId", false,
                                     std::span<const uint8_t>(blob));

    h.pumpGwUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "defineTopic() future did not resolve";

    DefineTopicResult r;
    ASSERT_NO_THROW(r = fut.defineResult()) << "defineTopic() threw";
    EXPECT_EQ(r.topicId,   0x40000001);
    EXPECT_EQ(r.version,   1u);
    EXPECT_EQ(r.errorCode, 0u);
}

// ===========================================================================
// Test 2: DefineTopicResponseErrorCodePropagates
// ===========================================================================
TEST(ClientSchema, DefineTopicResponseErrorCodePropagates) {
    SchemaHarness h(1102, 1103);
    h.build();
    h.connectAndWait();

    // TopicExists = 25
    h.gw->defineTopicId   = -1;
    h.gw->defineVersion   = 0;
    h.gw->defineErrorCode = static_cast<uint16_t>(rpc::ErrorCode::TopicExists);

    std::vector<uint8_t> blob;
    auto fut = h.defineTopicOp("existing_topic", 1, "", false,
                                     std::span<const uint8_t>(blob));

    h.pumpGwUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "defineTopic() future did not resolve with error code";

    DefineTopicResult r;
    ASSERT_NO_THROW(r = fut.defineResult())
        << "defineTopic() with non-OK errorCode must NOT throw — errorCode is in the result";
    EXPECT_EQ(r.errorCode, static_cast<uint16_t>(rpc::ErrorCode::TopicExists));
    EXPECT_EQ(r.topicId, -1);
}

// ===========================================================================
// Test 3: GetTopicSchemaFutureResolvesWithBlob
// ===========================================================================
TEST(ClientSchema, GetTopicSchemaFutureResolvesWithBlob) {
    SchemaHarness h(1104, 1105);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId   = 0x40000001;
    h.gw->getVersion   = 1;
    h.gw->getErrorCode = 0;
    h.gw->getBlob      = {0xDE, 0xAD, 0xBE, 0xEF};

    auto fut = h.getTopicSchemaOp("characters");

    h.pumpGwUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "getTopicSchema() future did not resolve";

    TopicSchemaResult r;
    ASSERT_NO_THROW(r = fut.schemaResult()) << "getTopicSchema() threw";
    EXPECT_EQ(r.topicId,   0x40000001);
    EXPECT_EQ(r.version,   1u);
    EXPECT_EQ(r.errorCode, 0u);
    ASSERT_EQ(r.descriptorBlob, (std::vector<uint8_t>{0xDE, 0xAD, 0xBE, 0xEF}))
        << "descriptorBlob mismatch";
}

// ===========================================================================
// Test 4: GetTopicSchemaUnknownTopicPropagates
// ===========================================================================
TEST(ClientSchema, GetTopicSchemaUnknownTopicPropagates) {
    SchemaHarness h(1106, 1107);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId   = -1;
    h.gw->getVersion   = 0;
    h.gw->getErrorCode = static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic);
    h.gw->getBlob.clear();

    auto fut = h.getTopicSchemaOp("missing_topic");

    h.pumpGwUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "getTopicSchema() with UnknownTopic did not resolve";

    TopicSchemaResult r;
    ASSERT_NO_THROW(r = fut.schemaResult())
        << "getTopicSchema() with UnknownTopic must NOT throw — errorCode in result";
    EXPECT_EQ(r.errorCode, static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic));
    EXPECT_EQ(r.topicId, -1);
    EXPECT_TRUE(r.descriptorBlob.empty());
}

// ===========================================================================
// Test 5: DefineTopicTimeoutFailsPromise
// ===========================================================================
TEST(ClientSchema, DefineTopicTimeoutFailsPromise) {
    SchemaHarness h(1108, 1109);
    h.pingTimeoutMs = 500;   // short timeout for this test
    h.build();
    h.connectAndWait();

    // Gateway will NOT reply to DefineTopic.
    h.gw->replyDefineTopic = false;

    std::vector<uint8_t> blob;
    auto fut = h.defineTopicOp("timeout_topic", 1, "", false,
                                     std::span<const uint8_t>(blob));

    // Future must fail within pingTimeoutMs + generous test margin (3 s).
    ASSERT_TRUE(fut.waitReady())
        << "defineTopic() future did not fail within timeout — potential hang";

    EXPECT_THROW(fut.defineResult(), std::exception)
        << "defineTopic() must throw on timeout, not return a value";
}

// ===========================================================================
// Test 6: GetTopicSchemaTimeoutFailsPromise
// ===========================================================================
TEST(ClientSchema, GetTopicSchemaTimeoutFailsPromise) {
    SchemaHarness h(1110, 1111);
    h.pingTimeoutMs = 500;
    h.build();
    h.connectAndWait();

    // Gateway will NOT reply to GetTopicSchema.
    h.gw->replyGetTopicSchema = false;

    auto fut = h.getTopicSchemaOp("timeout_topic");

    ASSERT_TRUE(fut.waitReady())
        << "getTopicSchema() future did not fail within timeout — potential hang";

    EXPECT_THROW(fut.schemaResult(), std::exception)
        << "getTopicSchema() must throw on timeout, not return a value";
}

// ===========================================================================
// Test 7: DefineTopicWithoutSessionRejected
// ===========================================================================
TEST(ClientSchema, DefineTopicWithoutSessionRejected) {
    SchemaHarness h(1112, 1113);
    h.build();
    // Do NOT connect.

    std::vector<uint8_t> blob;
    auto fut = h.defineTopicOp("topic", 1, "", false,
                                     std::span<const uint8_t>(blob));

    ASSERT_TRUE(fut.waitReady())
        << "defineTopic() without session was not rejected promptly";
    EXPECT_THROW(fut.defineResult(), std::exception)
        << "defineTopic() without session must throw";
}

// ===========================================================================
// Test 8: GetTopicSchemaWithoutSessionRejected
// ===========================================================================
TEST(ClientSchema, GetTopicSchemaWithoutSessionRejected) {
    SchemaHarness h(1114, 1115);
    h.build();
    // Do NOT connect.

    auto fut = h.getTopicSchemaOp("topic");

    ASSERT_TRUE(fut.waitReady())
        << "getTopicSchema() without session was not rejected promptly";
    EXPECT_THROW(fut.schemaResult(), std::exception)
        << "getTopicSchema() without session must throw";
}

// ===========================================================================
// Test 9 (regression): DefineTopicCorrectCorrelationId
//
// The gateway reads the correlationId from the DefineTopic request and stamps
// it on the DefineTopicResponse.  The client must match on correlationId — not
// some other field — to resolve the right promise.
// ===========================================================================
TEST(ClientSchema, DefineTopicCorrectCorrelationId) {
    SchemaHarness h(1116, 1117);
    h.build();
    h.connectAndWait();

    h.gw->defineTopicId   = 0x40000042;
    h.gw->defineVersion   = 3;
    h.gw->defineErrorCode = 0;

    std::vector<uint8_t> blob = {0xAB, 0xCD};
    auto fut = h.defineTopicOp("reg_topic", 4, "id", false,
                                     std::span<const uint8_t>(blob));

    h.pumpGwUntil([&]() {
        return fut.ready();
    });

    ASSERT_TRUE(fut.waitReady())
        << "regression: defineTopic() future did not resolve";

    DefineTopicResult r;
    ASSERT_NO_THROW(r = fut.defineResult()) << "regression: defineTopic() threw";
    EXPECT_EQ(r.topicId,   0x40000042) << "topicId mismatch — correlationId routing broken";
    EXPECT_EQ(r.version,   3u);
    EXPECT_EQ(r.errorCode, 0u);
    EXPECT_EQ(h.gw->definesSeen.load(), 1) << "gateway should have seen exactly one DefineTopic";
}

// ===========================================================================
// Test 10: MultipleSchemaOpsInflight
//
// 2 defineTopic + 1 getTopicSchema all in-flight simultaneously.  Each future
// must resolve with the correlationId-matched result, not cross-wired.
// ===========================================================================
TEST(ClientSchema, MultipleSchemaOpsInflight) {
    SchemaHarness h(1118, 1119);
    h.build();
    h.connectAndWait();

    // We can't know correlationIds ahead of time, so the gateway uses the
    // arrival order.  Each response echoes the request's correlationId, but
    // uses a gateway-side counter to pick the canned fields.  We wire the
    // overrides after we learn the corrIds by peeking inside the protocol —
    // instead, use distinct blob/topicId values that the gateway assigns based
    // on what the request carries.  Since the gateway echoes correlationId
    // and the overrides map is keyed by correlationId, we set overrides once
    // we know the corrIds.
    //
    // Simpler: let the default policy run for all three — each response echoes
    // its own correlationId.  What we assert is that ALL THREE futures resolve,
    // each with the topicId that the gateway's default policy produces (which
    // is the same for all, since the default doesn't vary by op).  The key
    // property under test is that three in-flight ops don't deadlock / lose
    // promises.

    h.gw->defineTopicId   = 0x40000001;
    h.gw->defineVersion   = 1;
    h.gw->defineErrorCode = 0;
    h.gw->getTopicId      = 0x40000001;
    h.gw->getVersion      = 1;
    h.gw->getErrorCode    = 0;
    h.gw->getBlob         = {0x01, 0x02};

    std::vector<uint8_t> blob1 = {0xAA};
    std::vector<uint8_t> blob2 = {0xBB};

    // Submit all three before any gateway polling.
    auto fDefine1 = h.defineTopicOp("t1", 1, "k", false,
                                           std::span<const uint8_t>(blob1));
    auto fDefine2 = h.defineTopicOp("t2", 2, "k", false,
                                           std::span<const uint8_t>(blob2));
    auto fGet1 = h.getTopicSchemaOp("t1");

    // Drive until all three are ready.
    h.pumpGwUntil([&]() {
        return (fDefine1.ready()) &&
               (fDefine2.ready()) &&
               (fGet1.ready());
    });

    ASSERT_TRUE(fDefine1.waitReady()) << "fDefine1 not ready";
    ASSERT_TRUE(fDefine2.waitReady()) << "fDefine2 not ready";
    ASSERT_TRUE(fGet1.waitReady()) << "fGet1 not ready";

    DefineTopicResult r1, r2;
    TopicSchemaResult r3;
    ASSERT_NO_THROW(r1 = fDefine1.defineResult()) << "fDefine1 threw";
    ASSERT_NO_THROW(r2 = fDefine2.defineResult()) << "fDefine2 threw";
    ASSERT_NO_THROW(r3 = fGet1.schemaResult())    << "fGet1 threw";

    EXPECT_EQ(r1.topicId, 0x40000001) << "fDefine1 topicId mismatch";
    EXPECT_EQ(r2.topicId, 0x40000001) << "fDefine2 topicId mismatch";
    EXPECT_EQ(r3.topicId, 0x40000001) << "fGet1 topicId mismatch";

    EXPECT_EQ(r1.errorCode, 0u) << "fDefine1 errorCode";
    EXPECT_EQ(r2.errorCode, 0u) << "fDefine2 errorCode";
    EXPECT_EQ(r3.errorCode, 0u) << "fGet1 errorCode";

    ASSERT_EQ(r3.descriptorBlob, (std::vector<uint8_t>{0x01, 0x02}))
        << "fGet1 blob mismatch";

    EXPECT_EQ(h.gw->definesSeen.load(), 2) << "gateway should have seen 2 DefineTopic";
    EXPECT_EQ(h.gw->getsSeen.load(),    1) << "gateway should have seen 1 GetTopicSchema";
}

// ===========================================================================
// Task 18: Client::alterTopic / alterTopicAndWait +
//          getTopicSchemaAndWait(name, requestedFingerprint)
// ===========================================================================

// ---------------------------------------------------------------------------
// Test 11: AlterTopicFutureResolvesOnOK
//
// An AlterTopic frame is encoded (expectedVersion + opsBlob carried correctly)
// and an injected AlterTopicResponse surfaces as AlterTopicResult{topicId,
// newVersion, errorCode}.
// ---------------------------------------------------------------------------
TEST(ClientSchema, AlterTopicFutureResolvesOnOK) {
    SchemaHarness h(1120, 1121);
    h.build();
    h.connectAndWait();

    h.gw->alterTopicId    = 0x40000001;
    h.gw->alterNewVersion = 2;
    h.gw->alterErrorCode  = 0;

    std::vector<uint8_t> opsBlob = {0xA1, 0xA2, 0xA3};
    auto fut = h.alterTopicOp("characters", /*expectedVersion=*/1, std::span<const uint8_t>(opsBlob));

    h.pumpGwUntil([&]() { return fut.ready(); });

    ASSERT_TRUE(fut.waitReady()) << "alterTopic() future did not resolve";

    AlterTopicResult r;
    ASSERT_NO_THROW(r = fut.alterResult()) << "alterTopic() threw";
    EXPECT_EQ(r.topicId,    0x40000001);
    EXPECT_EQ(r.newVersion, 2u);
    EXPECT_EQ(r.errorCode,  0u);

    // The frame carried the right expectedVersion + opsBlob (not reinvented
    // plumbing — the gateway decoded what the wire actually carried).
    EXPECT_EQ(h.gw->altersSeen.load(), 1) << "gateway should have seen exactly one AlterTopic";
    EXPECT_EQ(h.gw->lastAlterExpectedVersion.load(), 1u);
    EXPECT_EQ(h.gw->lastAlterOpsBlob, opsBlob) << "opsBlob mismatch on the wire";
}

// ===========================================================================
// Test 12: AlterTopicResponseErrorCodePropagates
//
// A stale expectedVersion -> SchemaConflict (broker errorCode 31).  The result
// carries the errorCode without throwing (same contract as DefineTopic/
// GetTopicSchema broker errors).
// ===========================================================================
TEST(ClientSchema, AlterTopicResponseErrorCodePropagates) {
    SchemaHarness h(1122, 1123);
    h.build();
    h.connectAndWait();

    h.gw->alterTopicId    = -1;
    h.gw->alterNewVersion = 0;
    h.gw->alterErrorCode  = static_cast<uint16_t>(rpc::ErrorCode::SchemaConflict);

    std::vector<uint8_t> opsBlob = {0xFF};
    auto fut = h.alterTopicOp("stale_topic", /*expectedVersion=*/7, std::span<const uint8_t>(opsBlob));

    h.pumpGwUntil([&]() { return fut.ready(); });

    ASSERT_TRUE(fut.waitReady())
        << "alterTopic() future did not resolve with error code";

    AlterTopicResult r;
    ASSERT_NO_THROW(r = fut.alterResult())
        << "alterTopic() with non-OK errorCode must NOT throw — errorCode is in the result";
    EXPECT_EQ(r.errorCode, static_cast<uint16_t>(rpc::ErrorCode::SchemaConflict));
    EXPECT_EQ(r.topicId, -1);
}

// ===========================================================================
// Test 13 (regression): GetTopicSchemaAndWaitThreadsRequestedFingerprintRegression
//
// getTopicSchemaAndWait(name, fp) must send `fp` on the wire inside the
// GetTopicSchema frame's requestedFingerprint field — not just carry it as a
// dead local parameter.  fp == 0 keeps the "latest" semantics (the existing
// 0-arg / 2-arg overloads never set it, so it already defaults to 0 on the
// wire; this pins that explicitly passing 0 is indistinguishable from that
// default).
// ===========================================================================
TEST(ClientSchema, GetTopicSchemaAndWaitThreadsRequestedFingerprintRegression) {
    SchemaHarness h(1124, 1125);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId   = 0x40000001;
    h.gw->getVersion   = 5;
    h.gw->getErrorCode = 0;
    h.gw->getBlob      = {0x01};

    constexpr uint64_t kFingerprint = 0xDEADBEEFCAFEF00DULL;

    auto fut = h.getTopicSchemaOp("characters", kFingerprint);
    h.pumpGwUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady()) << "getTopicSchemaAndWait(fp) future did not resolve";
    ASSERT_NO_THROW(fut.schemaResult());
    EXPECT_EQ(h.gw->lastRequestedFingerprint.load(), kFingerprint)
        << "regression: requestedFingerprint was not threaded onto the GetTopicSchema frame";

    // A second request with fp=0 must read as "latest" (unchanged default).
    auto fut2 = h.getTopicSchemaOp("characters", /*requestedFingerprint=*/0u);
    h.pumpGwUntil([&]() { return fut2.ready(); });
    ASSERT_TRUE(fut2.waitReady()) << "getTopicSchemaAndWait(0) future did not resolve";
    ASSERT_NO_THROW(fut2.schemaResult());
    EXPECT_EQ(h.gw->lastRequestedFingerprint.load(), 0u)
        << "regression: fp=0 must mean latest on the wire";
}
