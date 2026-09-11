// ClientCodecBenchmark.cpp — CP-2 baseline codec benchmark.
//
// Benchmarks the ConnectRequest and ConnectResponse encode + decode paths.
// Inputs are pre-built outside the timed loop; DoNotOptimize guards outputs.
//
//   BM_ConnectRequest_Encode   — encode a realistic ConnectRequest into a stack buffer
//   BM_ConnectRequest_Decode   — decode a pre-encoded ConnectRequest frame
//   BM_ConnectResponse_Encode  — encode a ConnectResponse into a stack buffer
//   BM_ConnectResponse_Decode  — decode a pre-encoded ConnectResponse frame
//
// CP-3/4: extend this file with the new message types following the same pattern.

#include <benchmark/benchmark.h>
#include <array>
#include <cstdint>
#include <optional>
#include <string>

#include "rune/protocol/client/ControlMessages.h"

namespace rpc = rune::protocol::client;

namespace {

// ---------------------------------------------------------------------------
// Pre-built inputs (constructed once at startup).
// ---------------------------------------------------------------------------

// A realistic ConnectRequest: 24-B header + 8-B fixed + 2-B var prefix + channel.
constexpr std::string_view kChannel = "aeron:udp?endpoint=192.168.1.42:40123";

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

// Pre-encoded frames (built once; reused across iterations).
struct Frames {
    // ConnectRequest encoded frame: header(24) + fixed(8) + var(2+38) = 72 bytes
    static constexpr std::size_t kReqBufSize = 256;
    std::array<uint8_t, kReqBufSize> reqBuf{};
    std::size_t reqLen = 0;

    // ConnectResponse encoded frame: 38 bytes
    std::array<uint8_t, 64> respBuf{};
    std::size_t respLen = 0;

    Frames() {
        reqLen  = makeRequest().encode(reqBuf.data());
        respLen = makeResponse().encode(respBuf.data());
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
