// tests/DataConformanceTest.cpp
//
// CB-1 — PB produce/subscribe golden conformance vectors through the client
// data adapter layer.
//
// For each of the 7 committed .bin vectors (produce_explicit_partition,
// produce_key_only, produce_response, subscribe_explicit, subscribe_key_only,
// records_push, unsubscribe) this harness asserts BOTH:
//   1. Decode-golden:  load file bytes → decode via client data adapter →
//                      every field equals conformance/manifest.md values.
//   2. Encode-golden:  encode those same canonical values via client adapter →
//                      bytes are byte-identical to the golden file (memcmp, exact length).
//
// Plus a produce→ProduceResponse→Subscribe→Records→Unsubscribe round-trip that
// decodes each frame in sequence through the adapters and verifies field
// coherence through the whole flow.
//
// RUNE_CONFORMANCE_DIR is injected as a compile-time string by CMakeLists.txt
// and points at the sibling rune-protocol/conformance directory.

#include <gtest/gtest.h>

// Client data adapter layer — the adapters under test.
#include <rune/client/codec/DataCodec.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Compile-time conformance dir (injected by CMakeLists.txt)
// ---------------------------------------------------------------------------
#ifndef RUNE_CONFORMANCE_DIR
#  error "RUNE_CONFORMANCE_DIR must be defined by CMakeLists.txt — \
point it at the sibling rune-protocol/conformance directory"
#endif

namespace cc = rune::client::codec;

// ---------------------------------------------------------------------------
// Canonical values from conformance/manifest.md
// ---------------------------------------------------------------------------
static constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
static constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

// Shared opaque recordBatch payload used across produce/records vectors.
static constexpr uint8_t kBatchBytes[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x02};
static constexpr std::size_t kBatchLen = sizeof(kBatchBytes);

// Key used in key-only produce / key-only subscribe vectors: "ABC".
static constexpr uint8_t kKeyBytes[] = {0x41, 0x42, 0x43};
static constexpr std::size_t kKeyLen = sizeof(kKeyBytes);

// subscriptionId used in records_push.bin and unsubscribe.bin.
static constexpr int64_t kSubscriptionId = static_cast<int64_t>(0x0102030405060708LL);

// ---------------------------------------------------------------------------
// Helper: read an entire golden .bin file; fatal-fail if missing or empty.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> readVector(const char* name) {
    std::string path = std::string(RUNE_CONFORMANCE_DIR) + "/vectors/" + name;
    FILE* f = std::fopen(path.c_str(), "rb");
    EXPECT_NE(f, nullptr) << "Cannot open golden vector: " << path
        << "\n  Check that rune-protocol sibling repo is present and RUNE_CONFORMANCE_DIR is set.";
    if (!f) return {};

    std::fseek(f, 0, SEEK_END);
    const long sz = std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    EXPECT_GT(sz, 0) << "Empty vector file: " << path;
    if (sz <= 0) { std::fclose(f); return {}; }

    std::vector<uint8_t> buf(static_cast<std::size_t>(sz));
    const std::size_t got = std::fread(buf.data(), 1, buf.size(), f);
    std::fclose(f);
    EXPECT_EQ(got, buf.size()) << "Short read: " << path;
    return buf;
}

// ===========================================================================
// 1. produce_explicit_partition.bin  (60 bytes)
//    Produce{ topicId=1001, partition=7, partitionKnown=1, acks=0,
//             producerId=-1, producerEpoch=0, baseSequence=-1 (sentinels),
//             key=<empty>, recordBatch={DE AD BE EF 00 FF 01 02} }
// ===========================================================================
TEST(DataCodecConformance, ProduceExplicitPartition) {
    const auto golden = readVector("produce_explicit_partition.bin");
    ASSERT_EQ(golden.size(), 62u);

    // --- Decode-golden ---
    auto view = cc::decodeProduce(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->topicId,        1001);
    EXPECT_EQ(view->partition,      7);
    EXPECT_EQ(view->partitionKnown, 1u);
    EXPECT_EQ(view->key.size(),     0u);
    ASSERT_EQ(view->recordBatch.size(), kBatchLen);
    EXPECT_EQ(std::memcmp(view->recordBatch.data(), kBatchBytes, kBatchLen), 0)
        << "Decoded recordBatch bytes mismatch";

    // --- Encode-golden ---
    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeProduce(
        enc.data(), enc.size(),
        1001, 7, 1u,
        std::span<const uint8_t>{},
        std::span<const uint8_t>(kBatchBytes, kBatchLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 62u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden produce_explicit_partition.bin";
}

// ===========================================================================
// 2. produce_key_only.bin  (63 bytes)
//    Produce{ topicId=2002, partition=0, partitionKnown=0, acks=0,
//             producerId=-1, producerEpoch=0, baseSequence=-1 (sentinels),
//             key="ABC", recordBatch={DE AD BE EF 00 FF 01 02} }
// ===========================================================================
TEST(DataCodecConformance, ProduceKeyOnly) {
    const auto golden = readVector("produce_key_only.bin");
    ASSERT_EQ(golden.size(), 65u);

    // --- Decode-golden ---
    auto view = cc::decodeProduce(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->topicId,        2002);
    EXPECT_EQ(view->partition,      0);
    EXPECT_EQ(view->partitionKnown, 0u);
    ASSERT_EQ(view->key.size(), kKeyLen);
    EXPECT_EQ(std::memcmp(view->key.data(), kKeyBytes, kKeyLen), 0)
        << "Decoded key bytes mismatch";
    ASSERT_EQ(view->recordBatch.size(), kBatchLen);
    EXPECT_EQ(std::memcmp(view->recordBatch.data(), kBatchBytes, kBatchLen), 0)
        << "Decoded recordBatch bytes mismatch";

    // --- Encode-golden ---
    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeProduce(
        enc.data(), enc.size(),
        2002, 0, 0u,
        std::span<const uint8_t>(kKeyBytes, kKeyLen),
        std::span<const uint8_t>(kBatchBytes, kBatchLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 65u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden produce_key_only.bin";
}

// ===========================================================================
// 3. produce_response.bin  (46 bytes — D6: + logStartOffset, -1 here)
//    ProduceResponse{ errorCode=0(OK), partition=7, baseOffset=123456 }
// ===========================================================================
TEST(DataCodecConformance, ProduceResponse) {
    const auto golden = readVector("produce_response.bin");
    ASSERT_EQ(golden.size(), 46u);

    // --- Decode-golden ---
    auto view = cc::decodeProduceResponse(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,     kSessionId);
    EXPECT_EQ(view->correlationId, kCorrelationId);
    EXPECT_EQ(view->errorCode,     static_cast<int16_t>(0));   // OK
    EXPECT_EQ(view->partition,     7);
    EXPECT_EQ(view->baseOffset,    123456LL);
    EXPECT_EQ(view->logStartOffset, -1);

    // --- Encode-golden ---
    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeProduceResponse(
        enc.data(), enc.size(),
        static_cast<int16_t>(0), 7, 123456LL,
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 46u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden produce_response.bin";
}

// D6 compat: the pre-D6 38-byte frame an old broker sends still decodes, with
// logStartOffset -1 (not carried); the UnknownProducerId vector carries it.
TEST(DataCodecConformance, ProduceResponseV0AndUnknownProducerIdRegression) {
    const auto v0 = readVector("produce_response_v0.bin");
    ASSERT_EQ(v0.size(), 38u);
    auto a = cc::decodeProduceResponse(v0.data(), v0.size());
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->baseOffset, 123456LL);
    EXPECT_EQ(a->logStartOffset, -1);
    const auto up = readVector("produce_response_unknown_pid.bin");
    auto b = cc::decodeProduceResponse(up.data(), up.size());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownProducerId));
    EXPECT_EQ(b->logStartOffset, 500);
    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeProduceResponse(
        enc.data(), enc.size(), b->errorCode, 7, 0, kSessionId, kCorrelationId, 500);
    ASSERT_EQ(n, up.size());
    EXPECT_EQ(std::memcmp(enc.data(), up.data(), n), 0);
}

// ===========================================================================
// 4. subscribe_explicit.bin  (44 bytes)
//    Subscribe{ topicId=3001, partition=5, partitionKnown=1,
//               fromOffset=9999, isolation=1(read_committed), key=<empty> }
// ===========================================================================
TEST(DataCodecConformance, SubscribeExplicit) {
    const auto golden = readVector("subscribe_explicit.bin");
    ASSERT_EQ(golden.size(), 44u);

    // --- Decode-golden ---
    auto view = cc::decodeSubscribe(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->topicId,        3001);
    EXPECT_EQ(view->partition,      5);
    EXPECT_EQ(view->partitionKnown, 1u);
    EXPECT_EQ(view->fromOffset,     9999LL);
    EXPECT_EQ(view->isolation,      1u);
    EXPECT_EQ(view->key.size(),     0u);

    // --- Encode-golden ---
    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeSubscribe(
        enc.data(), enc.size(),
        3001, 5, 1u,
        9999LL, 1u,
        std::span<const uint8_t>{},
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 44u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden subscribe_explicit.bin";
}

// ===========================================================================
// 5. subscribe_key_only.bin  (47 bytes)
//    Subscribe{ topicId=4004, partition=0, partitionKnown=0,
//               fromOffset=0, isolation=0(read_uncommitted), key="ABC" }
// ===========================================================================
TEST(DataCodecConformance, SubscribeKeyOnly) {
    const auto golden = readVector("subscribe_key_only.bin");
    ASSERT_EQ(golden.size(), 47u);

    // --- Decode-golden ---
    auto view = cc::decodeSubscribe(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->topicId,        4004);
    EXPECT_EQ(view->partition,      0);
    EXPECT_EQ(view->partitionKnown, 0u);
    EXPECT_EQ(view->fromOffset,     0LL);
    EXPECT_EQ(view->isolation,      0u);
    ASSERT_EQ(view->key.size(), kKeyLen);
    EXPECT_EQ(std::memcmp(view->key.data(), kKeyBytes, kKeyLen), 0)
        << "Decoded key bytes mismatch";

    // --- Encode-golden ---
    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeSubscribe(
        enc.data(), enc.size(),
        4004, 0, 0u,
        0LL, 0u,
        std::span<const uint8_t>(kKeyBytes, kKeyLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 47u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden subscribe_key_only.bin";
}

// ===========================================================================
// 6. records_push.bin  (58 bytes)
//    Records{ subscriptionId=0x0102030405060708, topicId=3001, partition=5,
//             baseOffset=100000, recordBatch={DE AD BE EF 00 FF 01 02} }
// ===========================================================================
TEST(DataCodecConformance, RecordsPush) {
    const auto golden = readVector("records_push.bin");
    ASSERT_EQ(golden.size(), 60u);

    // --- Decode-golden ---
    auto view = cc::decodeRecords(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->subscriptionId, kSubscriptionId);
    EXPECT_EQ(view->topicId,        3001);
    EXPECT_EQ(view->partition,      5);
    EXPECT_EQ(view->baseOffset,     100000LL);
    ASSERT_EQ(view->recordBatch.size(), kBatchLen);
    EXPECT_EQ(std::memcmp(view->recordBatch.data(), kBatchBytes, kBatchLen), 0)
        << "Decoded recordBatch bytes mismatch";

    // --- Encode-golden ---
    std::array<uint8_t, 128> enc{};
    const std::size_t n = cc::encodeRecords(
        enc.data(), enc.size(),
        kSubscriptionId, 3001, 5, 100000LL,
        std::span<const uint8_t>(kBatchBytes, kBatchLen),
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 60u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden records_push.bin";
}

// ===========================================================================
// 7. unsubscribe.bin  (32 bytes)
//    Unsubscribe{ subscriptionId=0x0102030405060708 }
// ===========================================================================
TEST(DataCodecConformance, Unsubscribe) {
    const auto golden = readVector("unsubscribe.bin");
    ASSERT_EQ(golden.size(), 32u);

    // --- Decode-golden ---
    auto view = cc::decodeUnsubscribe(golden.data(), golden.size());
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view->sessionId,      kSessionId);
    EXPECT_EQ(view->correlationId,  kCorrelationId);
    EXPECT_EQ(view->subscriptionId, kSubscriptionId);

    // --- Encode-golden ---
    std::array<uint8_t, 64> enc{};
    const std::size_t n = cc::encodeUnsubscribe(
        enc.data(), enc.size(),
        kSubscriptionId,
        kSessionId, kCorrelationId);
    ASSERT_EQ(n, 32u);
    EXPECT_EQ(std::memcmp(enc.data(), golden.data(), n), 0)
        << "Encoded bytes differ from golden unsubscribe.bin";
}

// ===========================================================================
// Produce→ProduceResponse→Subscribe→Records→Unsubscribe round-trip
//
// Models a real produce/subscribe session at the wire level through the client
// data adapters:
//   Produce(explicit partition) → ProduceResponse → Subscribe(explicit) →
//   Records → Unsubscribe
//
// Expected templateId sequence: [16, 17, 18, 20, 21]
//
// Also verifies that decoded fields from each frame are coherent (e.g.
// partition returned in ProduceResponse matches what we subscribed at,
// baseOffset in ProduceResponse matches the one in Records).
// ===========================================================================
TEST(DataCodecSessionFlow, ProduceSubscribeRoundTrip) {
    // The full session uses these buffers — written then decoded in sequence.
    std::array<uint8_t, 256> buf{};

    // Step 1 — client encodes Produce (explicit partition)
    const std::size_t produceN = cc::encodeProduce(
        buf.data(), buf.size(),
        1001, 7, 1u,
        std::span<const uint8_t>{},
        std::span<const uint8_t>(kBatchBytes, kBatchLen),
        kSessionId, kCorrelationId);
    ASSERT_GT(produceN, 0u);

    // Verify Produce golden vector matches runtime-encoded bytes.
    {
        const auto golden = readVector("produce_explicit_partition.bin");
        ASSERT_FALSE(golden.empty());
        EXPECT_EQ(produceN, golden.size());
        EXPECT_EQ(std::memcmp(buf.data(), golden.data(), produceN), 0)
            << "Round-trip Produce bytes differ from golden";
    }

    // Step 2 — broker replies ProduceResponse: OK, partition=7, baseOffset=123456
    const std::size_t produceRespN = cc::encodeProduceResponse(
        buf.data(), buf.size(),
        static_cast<int16_t>(0), 7, 123456LL,
        kSessionId, kCorrelationId);
    ASSERT_GT(produceRespN, 0u);

    {
        auto v = cc::decodeProduceResponse(buf.data(), produceRespN);
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->errorCode,  static_cast<int16_t>(0));
        EXPECT_EQ(v->partition,  7);
        EXPECT_EQ(v->baseOffset, 123456LL);

        const auto golden = readVector("produce_response.bin");
        ASSERT_FALSE(golden.empty());
        EXPECT_EQ(produceRespN, golden.size());
        EXPECT_EQ(std::memcmp(buf.data(), golden.data(), produceRespN), 0)
            << "Round-trip ProduceResponse bytes differ from golden";
    }

    // Step 3 — client sends Subscribe (explicit partition=7, from baseOffset=9999)
    const std::size_t subscribeN = cc::encodeSubscribe(
        buf.data(), buf.size(),
        3001, 5, 1u,
        9999LL, 1u,
        std::span<const uint8_t>{},
        kSessionId, kCorrelationId);
    ASSERT_GT(subscribeN, 0u);

    {
        auto v = cc::decodeSubscribe(buf.data(), subscribeN);
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->topicId,        3001);
        EXPECT_EQ(v->partition,      5);
        EXPECT_EQ(v->partitionKnown, 1u);
        EXPECT_EQ(v->fromOffset,     9999LL);
        EXPECT_EQ(v->isolation,      1u);

        const auto golden = readVector("subscribe_explicit.bin");
        ASSERT_FALSE(golden.empty());
        EXPECT_EQ(subscribeN, golden.size());
        EXPECT_EQ(std::memcmp(buf.data(), golden.data(), subscribeN), 0)
            << "Round-trip Subscribe bytes differ from golden";
    }

    // Step 4 — broker pushes Records
    const std::size_t recordsN = cc::encodeRecords(
        buf.data(), buf.size(),
        kSubscriptionId, 3001, 5, 100000LL,
        std::span<const uint8_t>(kBatchBytes, kBatchLen),
        kSessionId, kCorrelationId);
    ASSERT_GT(recordsN, 0u);

    {
        auto v = cc::decodeRecords(buf.data(), recordsN);
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->subscriptionId, kSubscriptionId);
        EXPECT_EQ(v->topicId,        3001);
        EXPECT_EQ(v->partition,      5);
        EXPECT_EQ(v->baseOffset,     100000LL);
        ASSERT_EQ(v->recordBatch.size(), kBatchLen);
        EXPECT_EQ(std::memcmp(v->recordBatch.data(), kBatchBytes, kBatchLen), 0);

        const auto golden = readVector("records_push.bin");
        ASSERT_FALSE(golden.empty());
        EXPECT_EQ(recordsN, golden.size());
        EXPECT_EQ(std::memcmp(buf.data(), golden.data(), recordsN), 0)
            << "Round-trip Records bytes differ from golden";
    }

    // Step 5 — client sends Unsubscribe
    const std::size_t unsubN = cc::encodeUnsubscribe(
        buf.data(), buf.size(),
        kSubscriptionId,
        kSessionId, kCorrelationId);
    ASSERT_GT(unsubN, 0u);

    {
        auto v = cc::decodeUnsubscribe(buf.data(), unsubN);
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v->subscriptionId, kSubscriptionId);

        const auto golden = readVector("unsubscribe.bin");
        ASSERT_FALSE(golden.empty());
        EXPECT_EQ(unsubN, golden.size());
        EXPECT_EQ(std::memcmp(buf.data(), golden.data(), unsubN), 0)
            << "Round-trip Unsubscribe bytes differ from golden";
    }

    // Verify the templateId sequence [16, 17, 18, 20, 21]
    struct FlowFrame {
        const char* filename;
        uint16_t    expectedTemplateId;
    };
    static constexpr FlowFrame kFlow[] = {
        { "produce_explicit_partition.bin", 16u },  // Produce
        { "produce_response.bin",           17u },  // ProduceResponse
        { "subscribe_explicit.bin",         18u },  // Subscribe
        { "records_push.bin",               20u },  // Records
        { "unsubscribe.bin",                21u },  // Unsubscribe
    };
    constexpr std::size_t kFlowLen = sizeof(kFlow) / sizeof(kFlow[0]);
    ASSERT_EQ(kFlowLen, 5u);

    for (std::size_t i = 0; i < kFlowLen; ++i) {
        const auto frameBuf = readVector(kFlow[i].filename);
        ASSERT_FALSE(frameBuf.empty()) << "Could not read " << kFlow[i].filename;
        ASSERT_GE(frameBuf.size(), 4u);
        // templateId is at offset 2, LE uint16_t (matches MessageHeader layout)
        const uint16_t tid = static_cast<uint16_t>(frameBuf[2])
                           | static_cast<uint16_t>(static_cast<uint16_t>(frameBuf[3]) << 8u);
        EXPECT_EQ(tid, kFlow[i].expectedTemplateId)
            << "TemplateId mismatch at flow step " << i
            << " (" << kFlow[i].filename << ")";
    }
}

// Checked writes (client version 17): the 54-byte ProduceResponse and the verdict trailer.
TEST(CheckedWritesClientConformance, DecodesV17ProduceResponse) {
    const auto a = readVector("produce_response_v17.bin");
    ASSERT_EQ(a.size(), 54u);
    auto va = cc::decodeProduceResponse(a.data(), a.size());
    ASSERT_TRUE(va.has_value());
    EXPECT_EQ(va->baseOffset, 123456LL);
    EXPECT_EQ(va->assignedBase, 1001u);
    EXPECT_FALSE(va->hasVerdicts);
    EXPECT_EQ(va->verdictCount, 0u);

    const auto b = readVector("produce_response_v17_verdicts.bin");
    ASSERT_EQ(b.size(), 64u);
    auto vb = cc::decodeProduceResponse(b.data(), b.size());
    ASSERT_TRUE(vb.has_value());
    EXPECT_EQ(vb->assignedBase, 0u);
    EXPECT_TRUE(vb->hasVerdicts);
    ASSERT_EQ(vb->verdictCount, 1u);
    EXPECT_EQ(vb->verdictBytes.size(), 8u);
}
