// micro_repl_send.cpp — pinned single-threaded micro-benchmark isolating the CPU
// cost of the replication FetchResponse SEND path, BEFORE vs AFTER lever #2.
//
// BEFORE (#2 not applied): PartitionReplica encodes the FetchResponse into a 4 MiB
//   encodeScratch_ (records memcpy #1), decodes it back, sendFetchResponse re-encodes
//   into a wire buffer (records memcpy #2), enqueueOutbound OutboundFrame::putPayload
//   copies wire into the ring slot (records memcpy #3).  => 3 copies of the record bytes
//   + a decode parse, per replication frame.
// AFTER (#2 applied): makeFetchResponse builds the struct (no encode), enqueueFetchResponse
//   encodes the FetchResponse straight into the OutboundFrame payload in the ring slot
//   (records memcpy #1 only).  => 1 copy, no scratch, no decode.
//
// Buffers are reused across iterations (mirrors the real reused encodeScratch_/wire and
// the tryClaim'd ring slot), so this measures pure encode/copy CPU with zero allocation
// and zero OS/scheduling noise (pin with taskset). ns/op is box-independent.
#include <rune/replication/ReplicationMessages.h>
#include <rune_protocol_internal_sbe/OutboundFrame.h>
#include <rune_protocol_internal_sbe/MessageHeader.h>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <vector>
#include <span>

using namespace rune::replication;
namespace isbe = ::rune::protocol::internal::sbe;

int main(int argc, char** argv) {
    const std::size_t recBytes = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 8192;
    const int         iters    = argc > 2 ? std::atoi(argv[2])                 : 2'000'000;

    std::vector<uint8_t> records(recBytes, 0xAB);
    std::span<const uint8_t> recSpan(records.data(), records.size());

    std::vector<uint8_t> scratch(4 * 1024 * 1024);   // encodeScratch_ (4 MiB, reused)
    std::vector<uint8_t> wire(2 * 1024 * 1024);       // sendFetchResponse wire buffer
    std::vector<uint8_t> ring(2 * 1024 * 1024);       // stands in for the outbound-ring slot

    volatile uint64_t sink = 0;

    auto beforePath = [&]() {
        // 1. PartitionReplica::onFetchRequest: encode into encodeScratch_
        FetchResponseMsg::encode(scratch.data(), 0, /*epoch*/7, /*part*/0, /*hw*/100,
                                 /*lso*/0, /*err*/0, recSpan, /*topicId*/0);
        // 2. decode round-trip
        auto resp = FetchResponseMsg::decode(scratch.data(), 0);
        // 3. sendFetchResponse: re-encode into wire
        const std::size_t frame = FetchResponseMsg::encodedSize(resp.records.size());
        FetchResponseMsg::encode(wire.data(), 0, resp.leaderEpoch, resp.partition,
                                 resp.highWatermark, resp.logStartOffset, resp.errorCode,
                                 resp.records, resp.topicId);
        // 4. enqueueOutbound: OutboundFrame wrap + putPayload(wire) into ring
        const std::size_t oframe =
            isbe::MessageHeader::encodedLength() + isbe::OutboundFrame::computeLength(frame);
        isbe::OutboundFrame enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(ring.data()), 0, oframe);
        enc.targetNodeId(1);
        enc.putPayload(reinterpret_cast<const char*>(wire.data()),
                       static_cast<std::uint32_t>(frame));
        sink += ring[oframe - 1];
    };

    auto afterPath = [&]() {
        // 1. makeFetchResponse: build the struct (zero-copy records view)
        FetchResponseMsg resp{};
        resp.leaderEpoch = 7; resp.partition = 0; resp.highWatermark = 100;
        resp.logStartOffset = 0; resp.errorCode = 0; resp.topicId = 0; resp.records = recSpan;
        // 2. enqueueFetchResponse: encode DIRECTLY into the OutboundFrame payload in the ring
        const std::size_t payloadLen = FetchResponseMsg::encodedSize(resp.records.size());
        const std::size_t oframe =
            isbe::MessageHeader::encodedLength() + isbe::OutboundFrame::computeLength(payloadLen);
        isbe::OutboundFrame enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(ring.data()), 0, oframe);
        enc.targetNodeId(1);
        const std::uint64_t lenPos = enc.sbePosition();
        detail::write_le<std::uint32_t>(ring.data() + lenPos,
                                        static_cast<std::uint32_t>(payloadLen));
        uint8_t* payload = ring.data() + lenPos + isbe::OutboundFrame::payloadHeaderLength();
        FetchResponseMsg::encode(payload, 0, resp.leaderEpoch, resp.partition,
                                 resp.highWatermark, resp.logStartOffset, resp.errorCode,
                                 resp.records, resp.topicId);
        enc.sbePosition(lenPos + isbe::OutboundFrame::payloadHeaderLength() + payloadLen);
        sink += ring[oframe - 1];
    };

    for (int i = 0; i < 100'000; ++i) { beforePath(); afterPath(); }   // warm up

    auto bench = [&](auto fn) -> double {
        double best = 1e18;
        for (int t = 0; t < 5; ++t) {                                   // min of 5 trials = least noise
            auto s = std::chrono::steady_clock::now();
            for (int i = 0; i < iters; ++i) fn();
            auto e = std::chrono::steady_clock::now();
            double ns = std::chrono::duration<double, std::nano>(e - s).count() / iters;
            if (ns < best) best = ns;
        }
        return best;
    };

    double b = bench(beforePath), a = bench(afterPath);
    std::printf("recBytes=%zu iters=%d  BEFORE=%.1f ns/op  AFTER=%.1f ns/op  "
                "delta=%.1f ns  (%.1f%% faster)  [sink=%llu]\n",
                recBytes, iters, b, a, b - a, 100.0 * (b - a) / b,
                (unsigned long long)sink);
    return 0;
}
