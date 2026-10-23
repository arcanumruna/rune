// bench/ShardRouteBench.cpp — T15b M3: the app-thread cost of
// ShardGroup::trySend (hash the key, pick the owning shard, check ownership,
// claim + write into that shard's producer ring).  Before M3 the key was hashed
// up to 3 times per send (group, shard, producer); after it, once.
//
// Agent ShardGroup (N shards, no broker: nothing is sent, the runners drain
// into builders), acks=None.  Each sample builds a fresh group and times
// ROUTE_SENDS consecutive trySend calls until the first non-Ok (the producer
// ring / builders fill) — only accepted sends are timed.  Prints one line per
// sample and the median ns/send.  Build the same file against both trees and
// run them interleaved (A/B/A/B...).
// Env: ROUTE_SHARDS (2), ROUTE_SENDS (4000), ROUTE_SAMPLES (9), ROUTE_KEY_BYTES (16).
#include <filesystem>
#include <rune/client/shard/ClientShard.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;

static int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v && *v) ? std::atoll(v) : d;
}

int main() {
    const auto shards   = static_cast<uint32_t>(envI64("ROUTE_SHARDS", 2));
    const auto sends    = envI64("ROUTE_SENDS", 4000);
    const auto samples  = envI64("ROUTE_SAMPLES", 9);
    const auto keyBytes = static_cast<std::size_t>(envI64("ROUTE_KEY_BYTES", 16));

    std::vector<rc::Key> keys;
    keys.reserve(static_cast<std::size_t>(sends));
    for (int64_t i = 0; i < sends; ++i) {
        std::vector<uint8_t> k(keyBytes, 'k');
        for (std::size_t b = 0; b < sizeof(int64_t) && b < keyBytes; ++b) k[b] = static_cast<uint8_t>((i >> (8 * b)) & 0xFF);
        keys.emplace_back(std::span<const uint8_t>(k));
    }
    const std::vector<uint8_t> val(16, 0x5a);

    std::vector<double> nsPerSend;
    for (int64_t s = 0; s < samples; ++s) {
        rs::ShardGroupConfig g;
        g.base.dataDir = std::filesystem::temp_directory_path().string() + "/rune_route_bench_" + std::to_string(::getpid()) + "_" + std::to_string(s);
        g.base.gatewayStreamId = 5100;
        g.base.egressStreamId  = 5101;
        rc::TopicSpec t;
        t.name           = "t";
        t.topicId        = 1;
        t.partitionCount = 8;
        g.base.topics.push_back(t);
        g.shardCount        = shards;
        g.producer.acks     = rc::Acks::None;
        g.producer.lingerUs = 0;
        rs::ShardGroup grp(g);
        const rc::TopicHandle topic = grp.shard(0).topic("t");
        int64_t               ok    = 0;
        const auto            t0    = std::chrono::steady_clock::now();
        for (int64_t i = 0; i < sends; ++i) {
            if (grp.trySend(topic, keys[static_cast<std::size_t>(i)], val) != rc::SendResult::Ok) break;
            ++ok;
        }
        const auto t1 = std::chrono::steady_clock::now();
        const double ns =
            static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
            static_cast<double>(ok > 0 ? ok : 1);
        std::printf("sample %lld: %lld sends, %.2f ns/send\n", static_cast<long long>(s),
                    static_cast<long long>(ok), ns);
        nsPerSend.push_back(ns);
    }
    std::sort(nsPerSend.begin(), nsPerSend.end());
    std::printf("median %.2f ns/send\n", nsPerSend[nsPerSend.size() / 2]);
    // Context: the cost of ONE key hash (what M3 removes per extra hash).
    {
        volatile int32_t sink = 0;
        const int        reps = 200;
        const auto       t0   = std::chrono::steady_clock::now();
        for (int r = 0; r < reps; ++r) {
            for (const auto& k : keys) sink = sink + k.partition(8);
        }
        const auto t1 = std::chrono::steady_clock::now();
        std::printf("hash %.2f ns/key.partition\n",
                    static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) /
                        (static_cast<double>(reps) * static_cast<double>(keys.size())));
    }
    return 0;
}
