// ClientCodecBenchmark.cpp — CP-2 + CP-3 + CP-4 + CP-6 + PB-1 codec benchmarks.
//
// Benchmarks the ConnectRequest, ConnectResponse, Keepalive, Disconnect,
// ErrorResponse, Ping, and Pong encode + decode paths.  Inputs are pre-built
// outside the timed loop; DoNotOptimize guards outputs.
//
//   BM_ConnectRequest_Encode    — encode a realistic ConnectRequest into a stack buffer
//   BM_ConnectRequest_Decode    — decode a pre-encoded ConnectRequest frame
//   BM_ConnectResponse_Encode   — encode a ConnectResponse into a stack buffer
//   BM_ConnectResponse_Decode   — decode a pre-encoded ConnectResponse frame
//   BM_Keepalive_Encode         — encode a Keepalive (24-byte header-only) frame
//   BM_Keepalive_Decode         — decode a pre-encoded Keepalive frame
//   BM_Disconnect_Encode        — encode a Disconnect (26-byte) frame
//   BM_Disconnect_Decode        — decode a pre-encoded Disconnect frame
//   BM_ErrorResponse_Encode     — encode an ErrorResponse with realistic message
//   BM_ErrorResponse_Decode     — decode a pre-encoded ErrorResponse frame
//   BM_Ping_Encode              — encode a Ping with 32-byte echo payload
//   BM_Ping_Decode              — decode a pre-encoded Ping frame (32-byte echo)
//   BM_Ping_Encode_Empty        — encode a Ping with empty echo (baseline)
//   BM_Pong_Encode              — encode a Pong with 32-byte echo payload
//   BM_Pong_Decode              — decode a pre-encoded Pong frame (32-byte echo)
//
// CP-5/6: extend this file with the new message types following the same pattern.

#include <benchmark/benchmark.h>
#include <array>
#include <cstdint>
#include <optional>
#include <string>

#include "rune/protocol/client/ControlMessages.h"
#include "rune/protocol/client/DataMessages.h"
#include "rune/protocol/client/Dispatch.h"
#include "rune/protocol/client/PingMessages.h"

namespace rpc = rune::protocol::client;

namespace {

// ---------------------------------------------------------------------------
// Pre-built inputs (constructed once at startup).
// ---------------------------------------------------------------------------

// A realistic ConnectRequest: 24-B header + 8-B fixed + 2-B var prefix + channel.
constexpr std::string_view kChannel = "aeron:udp?endpoint=192.168.1.42:40123";

// Produce/ProduceResponse: a 64-byte opaque recordBatch payload.
static const std::vector<uint8_t> kBatchPayload(64, 0xAB);
static const uint8_t kProduceKey[] = {0x01, 0x02, 0x03, 0x04};

rpc::Produce makeProduce() {
    rpc::Produce msg;
    msg.sessionId      = 0x1122334455667788LL;
    msg.correlationId  = 0x0A0B0C0D0E0F1011LL;
    msg.topicId        = 42;
    msg.partition      = 3;
    msg.partitionKnown = 0;
    msg.key            = std::span<const uint8_t>(kProduceKey, sizeof(kProduceKey));
    msg.recordBatch    = std::span<const uint8_t>(kBatchPayload.data(), kBatchPayload.size());
    return msg;
}

rpc::ProduceResponse makeProduceResponse() {
    rpc::ProduceResponse msg;
    msg.sessionId     = 0x1122334455667788LL;
    msg.correlationId = 0x0A0B0C0D0E0F1011LL;
    msg.errorCode     = 0;
    msg.partition     = 3;
    msg.baseOffset    = 0x0000000001234567LL;
    return msg;
}

rpc::ConnectRequest makeRequest() {
    rpc::ConnectRequest req;
    req.sessionId      = 0x0102030405060708LL;
    req.correlationId  = 0x0A0B0C0D0E0F1011LL;
    req.egressStreamId = 7;
    req.clientVersion  = 42;
    req.egressChannel  = kChannel;
    return req;
}

rpc::ConnectResponse makeResponse() {
    rpc::ConnectResponse resp;
    resp.sessionId         = 0x1122334455667788LL;
    resp.correlationId     = -1LL;
    resp.code              = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
    resp.leaderMemberId    = -3;
    resp.assignedSessionId = 0x7FFFFFFFFFFFFFFFLL;
    return resp;
}

// A realistic ErrorResponse message payload.
constexpr std::string_view kErrorMsg = "cluster not available — redirect to leader";

// A representative Ping/Pong echo payload: 32 bytes of opaque data.
// 32 bytes is a representative round-trip payload (e.g., a small request ID + nonce).
static const std::string kEchoPayload(32, '\xAB');

rpc::Keepalive makeKeepalive() {
    rpc::Keepalive ka;
    ka.sessionId     = 0xABCDEF0123456789LL;
    ka.correlationId = 0x9876543210FEDCBALL;
    return ka;
}

rpc::Disconnect makeDisconnect() {
    rpc::Disconnect dis;
    dis.sessionId     = 0xDEADBEEFCAFEBABELL;
    dis.correlationId = 0x1122334455667788LL;
    dis.reason        = -1;
    return dis;
}

rpc::ErrorResponse makeErrorResponse() {
    rpc::ErrorResponse err;
    err.sessionId     = 0x0102030405060708LL;
    err.correlationId = 0x0A0B0C0D0E0F1011LL;
    err.code          = static_cast<int16_t>(rpc::ErrorCode::NotLeader);
    err.message       = kErrorMsg;
    return err;
}

rpc::Ping makePing() {
    rpc::Ping ping;
    ping.sessionId     = 0xDEADBEEFCAFEBABELL;
    ping.correlationId = 0x0102030405060708LL;
    ping.echo          = kEchoPayload;
    return ping;
}

rpc::Pong makePong() {
    rpc::Pong pong;
    pong.sessionId     = 0xDEADBEEFCAFEBABELL;
    pong.correlationId = 0x0102030405060708LL;
    pong.echo          = kEchoPayload;
    return pong;
}

// Pre-encoded frames (built once; reused across iterations).
struct Frames {
    // ConnectRequest encoded frame: header(24) + fixed(8) + var(2+38) = 72 bytes
    static constexpr std::size_t kReqBufSize = 256;
    std::array<uint8_t, kReqBufSize> reqBuf{};
    std::size_t reqLen = 0;

    // ConnectResponse encoded frame: 38 bytes
    std::array<uint8_t, 64> respBuf{};
    std::size_t respLen = 0;

    // Keepalive encoded frame: 24 bytes
    std::array<uint8_t, 32> kaBuf{};
    std::size_t kaLen = 0;

    // Disconnect encoded frame: 26 bytes
    std::array<uint8_t, 32> disBuf{};
    std::size_t disLen = 0;

    // ErrorResponse encoded frame: 28 + kErrorMsg.size() bytes
    static constexpr std::size_t kErrBufSize = 256;
    std::array<uint8_t, kErrBufSize> errBuf{};
    std::size_t errLen = 0;

    // Ping encoded frame: 24 + 2 + 32 = 58 bytes
    std::array<uint8_t, 128> pingBuf{};
    std::size_t pingLen = 0;

    // Pong encoded frame: 24 + 2 + 32 = 58 bytes
    std::array<uint8_t, 128> pongBuf{};
    std::size_t pongLen = 0;

    // Produce encoded frame: 24 + 9 + 2 + 4 (key) + 2 + 64 (batch) = 105 bytes
    static constexpr std::size_t kProduceBufSize = 256;
    std::array<uint8_t, kProduceBufSize> produceBuf{};
    std::size_t produceLen = 0;

    // ProduceResponse encoded frame: 38 bytes
    std::array<uint8_t, 64> produceRespBuf{};
    std::size_t produceRespLen = 0;

    Frames() {
        reqLen         = makeRequest().encode(reqBuf.data());
        respLen        = makeResponse().encode(respBuf.data());
        kaLen          = makeKeepalive().encode(kaBuf.data());
        disLen         = makeDisconnect().encode(disBuf.data());
        errLen         = makeErrorResponse().encode(errBuf.data());
        pingLen        = makePing().encode(pingBuf.data());
        pongLen        = makePong().encode(pongBuf.data());
        produceLen     = makeProduce().encode(produceBuf.data());
        produceRespLen = makeProduceResponse().encode(produceRespBuf.data());
    }
};

// Singleton frames instance — constructed before benchmarks run.
const Frames& frames() {
    static Frames f;
    return f;
}

} // namespace

// ---------------------------------------------------------------------------
// BM_ConnectRequest_Encode
// ---------------------------------------------------------------------------
static void BM_ConnectRequest_Encode(benchmark::State& state) {
    const rpc::ConnectRequest req = makeRequest();
    std::array<uint8_t, 256> buf{};

    for (auto _ : state) {
        std::size_t n = req.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(frames().reqLen));
}
BENCHMARK(BM_ConnectRequest_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ConnectRequest_Decode
// ---------------------------------------------------------------------------
static void BM_ConnectRequest_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::ConnectRequest::decode(f.reqBuf.data(), f.reqLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.reqLen));
}
BENCHMARK(BM_ConnectRequest_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ConnectResponse_Encode
// ---------------------------------------------------------------------------
static void BM_ConnectResponse_Encode(benchmark::State& state) {
    const rpc::ConnectResponse resp = makeResponse();
    std::array<uint8_t, 64> buf{};

    for (auto _ : state) {
        std::size_t n = resp.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(rpc::ConnectResponse::ENCODED_SIZE));
}
BENCHMARK(BM_ConnectResponse_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ConnectResponse_Decode
// ---------------------------------------------------------------------------
static void BM_ConnectResponse_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::ConnectResponse::decode(f.respBuf.data(), f.respLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.respLen));
}
BENCHMARK(BM_ConnectResponse_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Keepalive_Encode
// ---------------------------------------------------------------------------
static void BM_Keepalive_Encode(benchmark::State& state) {
    const rpc::Keepalive ka = makeKeepalive();
    std::array<uint8_t, 32> buf{};

    for (auto _ : state) {
        std::size_t n = ka.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(rpc::Keepalive::ENCODED_SIZE));
}
BENCHMARK(BM_Keepalive_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Keepalive_Decode
// ---------------------------------------------------------------------------
static void BM_Keepalive_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::Keepalive::decode(f.kaBuf.data(), f.kaLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.kaLen));
}
BENCHMARK(BM_Keepalive_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Disconnect_Encode
// ---------------------------------------------------------------------------
static void BM_Disconnect_Encode(benchmark::State& state) {
    const rpc::Disconnect dis = makeDisconnect();
    std::array<uint8_t, 32> buf{};

    for (auto _ : state) {
        std::size_t n = dis.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(rpc::Disconnect::ENCODED_SIZE));
}
BENCHMARK(BM_Disconnect_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Disconnect_Decode
// ---------------------------------------------------------------------------
static void BM_Disconnect_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::Disconnect::decode(f.disBuf.data(), f.disLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.disLen));
}
BENCHMARK(BM_Disconnect_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ErrorResponse_Encode
// ---------------------------------------------------------------------------
static void BM_ErrorResponse_Encode(benchmark::State& state) {
    const rpc::ErrorResponse err = makeErrorResponse();
    std::array<uint8_t, 256> buf{};

    for (auto _ : state) {
        std::size_t n = err.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(frames().errLen));
}
BENCHMARK(BM_ErrorResponse_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_ErrorResponse_Decode
// ---------------------------------------------------------------------------
static void BM_ErrorResponse_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::ErrorResponse::decode(f.errBuf.data(), f.errLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.errLen));
}
BENCHMARK(BM_ErrorResponse_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Ping_Encode  — 32-byte echo payload
// ---------------------------------------------------------------------------
static void BM_Ping_Encode(benchmark::State& state) {
    const rpc::Ping ping = makePing();
    std::array<uint8_t, 128> buf{};

    for (auto _ : state) {
        std::size_t n = ping.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(frames().pingLen));
}
BENCHMARK(BM_Ping_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Ping_Decode  — 32-byte echo payload
// ---------------------------------------------------------------------------
static void BM_Ping_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::Ping::decode(f.pingBuf.data(), f.pingLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.pingLen));
}
BENCHMARK(BM_Ping_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Ping_Encode_Empty  — empty echo (baseline / lower bound)
// ---------------------------------------------------------------------------
static void BM_Ping_Encode_Empty(benchmark::State& state) {
    rpc::Ping ping;
    ping.sessionId     = 0xDEADBEEFCAFEBABELL;
    ping.correlationId = 0x0102030405060708LL;
    // echo is empty (default)
    std::array<uint8_t, 64> buf{};

    for (auto _ : state) {
        std::size_t n = ping.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    // empty Ping is 26 bytes
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(26));
}
BENCHMARK(BM_Ping_Encode_Empty)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Pong_Encode  — 32-byte echo payload
// ---------------------------------------------------------------------------
static void BM_Pong_Encode(benchmark::State& state) {
    const rpc::Pong pong = makePong();
    std::array<uint8_t, 128> buf{};

    for (auto _ : state) {
        std::size_t n = pong.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(frames().pongLen));
}
BENCHMARK(BM_Pong_Encode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_Pong_Decode  — 32-byte echo payload
// ---------------------------------------------------------------------------
static void BM_Pong_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::Pong::decode(f.pongBuf.data(), f.pongLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.pongLen));
}
BENCHMARK(BM_Pong_Decode)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_checkFrame_ValidKeepalive — checkFrame on a valid 24-byte Keepalive frame.
//
// Keepalive is the smallest (header-only) frame and represents the fast path:
// all checks pass in order with minimal branch mis-prediction.
// ---------------------------------------------------------------------------
static void BM_checkFrame_ValidKeepalive(benchmark::State& state) {
    const Frames& f = frames();
    // kaBuf holds an encoded Keepalive frame (24 bytes, all valid).
    for (auto _ : state) {
        rpc::FrameCheck fc = rpc::checkFrame(f.kaBuf.data(), f.kaLen);
        benchmark::DoNotOptimize(fc);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(f.kaLen));
}
BENCHMARK(BM_checkFrame_ValidKeepalive)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_peekTemplateId_ValidKeepalive — peekTemplateId (wraps checkFrame).
// ---------------------------------------------------------------------------
static void BM_peekTemplateId_ValidKeepalive(benchmark::State& state) {
    const Frames& f = frames();
    for (auto _ : state) {
        auto tid = rpc::peekTemplateId(f.kaBuf.data(), f.kaLen);
        benchmark::DoNotOptimize(tid);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(f.kaLen));
}
BENCHMARK(BM_peekTemplateId_ValidKeepalive)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// BM_checkFrame_ValidConnectRequest — checkFrame on a larger variable-length frame.
// ---------------------------------------------------------------------------
static void BM_checkFrame_ValidConnectRequest(benchmark::State& state) {
    const Frames& f = frames();
    for (auto _ : state) {
        rpc::FrameCheck fc = rpc::checkFrame(f.reqBuf.data(), f.reqLen);
        benchmark::DoNotOptimize(fc);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(f.reqLen));
}
BENCHMARK(BM_checkFrame_ValidConnectRequest)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------
// PB-1: Produce / ProduceResponse encode + decode benchmarks.
// ---------------------------------------------------------------------------

// BM_Produce_Encode — encode a Produce with 4-byte key + 64-byte recordBatch.
static void BM_Produce_Encode(benchmark::State& state) {
    const rpc::Produce prod = makeProduce();
    std::array<uint8_t, 256> buf{};

    for (auto _ : state) {
        std::size_t n = prod.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(frames().produceLen));
}
BENCHMARK(BM_Produce_Encode)->Unit(benchmark::kNanosecond);

// BM_Produce_Decode — decode a pre-encoded Produce frame.
static void BM_Produce_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::Produce::decode(f.produceBuf.data(), f.produceLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.produceLen));
}
BENCHMARK(BM_Produce_Decode)->Unit(benchmark::kNanosecond);

// BM_ProduceResponse_Encode — encode a ProduceResponse (fixed 38 bytes).
static void BM_ProduceResponse_Encode(benchmark::State& state) {
    const rpc::ProduceResponse resp = makeProduceResponse();
    std::array<uint8_t, 64> buf{};

    for (auto _ : state) {
        std::size_t n = resp.encode(buf.data());
        benchmark::DoNotOptimize(n);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(rpc::ProduceResponse::ENCODED_SIZE));
}
BENCHMARK(BM_ProduceResponse_Encode)->Unit(benchmark::kNanosecond);

// BM_ProduceResponse_Decode — decode a pre-encoded ProduceResponse frame.
static void BM_ProduceResponse_Decode(benchmark::State& state) {
    const Frames& f = frames();

    for (auto _ : state) {
        auto result = rpc::ProduceResponse::decode(f.produceRespBuf.data(),
                                                    f.produceRespLen);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() *
                            static_cast<int64_t>(f.produceRespLen));
}
BENCHMARK(BM_ProduceResponse_Decode)->Unit(benchmark::kNanosecond);
