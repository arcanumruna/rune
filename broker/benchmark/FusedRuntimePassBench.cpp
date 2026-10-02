// FusedRuntimePassBench.cpp — Task 11 (spec §3.2): what the runtime duty cycle
// costs worker 0's pass when it is FUSED into it, vs `thread` placement.
//
// A REAL single-node shard-mode BrokerNode (1 worker, 8 partitions, unpinned so
// the shard uses the backoff idle — the pass work is measured, never the parks)
// is driven either idle or with a real wire client producing (acks=1) to the
// worker shard.  The numbers come from the flight recorder's own spans, enabled
// for the Runtime category only — no extra instrumentation on the duty cycle:
//   ShardPass     — one whole worker pass (fused: + runtime driver, conductor,
//                   NodeRuntimeAgent::doWork)
//   RuntimeDoWork — one NodeRuntimeAgent duty cycle (either placement)
// Reported as counters (ns): pass_p50/p99/p999/max, rt_p50/p99/p999/max.
//
// Arg `tickUs` (fused only; ignored for thread placement): the runtime slow-tick
// interval for the O(partitions) failover scan — 0 = every cycle (the pre-
// ae07765 behaviour), 10000 = the production 10 ms.  ONE binary for the ABAB:
//   --benchmark_filter='BM_ShardPassRuntimePlacement/fused:1/load:1/tickUs:(0|10000)'
// (an empty/-1 arg would be the placement default; both values are explicit here).
//
// Local (Mac) numbers are indicative only; perf claims come from the k3s box.
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>

#include "ShardTestClient.h"

#include <rune_fr/FlightRecorder.h>

#include <benchmark/benchmark.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

namespace {

namespace fs = std::filesystem;
using rune::node::BrokerNode;
using rune::node::NodeConfig;
using rune::node::PeerConfig;
using rune::node::TopicSpec;
using rune::node::test::ShardTestClient;
using rune::node::test::freeUdpPort;
using rune::node::test::freeUdpPortBlocks;
using rune::node::test::makeBatch;

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;
constexpr int32_t kPartitions    = 8;

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_fusedbench_") + tag + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

NodeConfig benchConfig(const fs::path& dataDir, bool fused, uint16_t clientPort, uint16_t consPort,
                       uint16_t replPort, uint16_t ib, uint16_t rb, uint16_t eg) {
    NodeConfig cfg;
    cfg.nodeId = 0;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.workerCount = 1;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = fused ? NodeConfig::RuntimePlacement::Fused : NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort = ib;
    cfg.workerReplBasePort = rb;
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = eg;
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = 1;
    cfg.aeronTermBufferBytes = 1 << 20;
    PeerConfig pc;
    pc.id = 0; pc.host = "127.0.0.1";
    pc.consensusPort = consPort; pc.clientPort = clientPort; pc.replicationPort = replPort;
    pc.replBasePort = rb; pc.ingressBasePort = ib; pc.workerCount = 1;
    cfg.peers.push_back(pc);
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = "bt"; t.partitionCount = kPartitions; t.topicId = 0;
    cfg.topics.push_back(t);
    return cfg;
}

void report(benchmark::State& state) {
    rune_fr::Recorder::instance().drain();
    for (const auto& s : rune_fr::collectSiteStats()) {
        const char* prefix = s.site == rune_fr::Site::ShardPass     ? "pass_"
                           : s.site == rune_fr::Site::RuntimeDoWork ? "rt_" : nullptr;
        if (!prefix) continue;
        const std::string p(prefix);
        state.counters[p + "n"]    = static_cast<double>(s.count);
        state.counters[p + "p50"]  = static_cast<double>(s.p50);
        state.counters[p + "p99"]  = static_cast<double>(s.p99);
        state.counters[p + "p999"] = static_cast<double>(s.p999);
        state.counters[p + "max"]  = static_cast<double>(s.max);
    }
}

// Args: {fused (0/1), load (0 = idle, 1 = acks=1 produce round trips), tickUs}
void BM_ShardPassRuntimePlacement(benchmark::State& state) {
    const bool fused = state.range(0) != 0;
    const bool load  = state.range(1) != 0;
    const int64_t tickNs = state.range(2) * 1'000;
    spdlog::set_level(spdlog::level::warn);
    TmpDir dir{fused ? "fused" : "thread"}, cdir{"client"};
    const auto blocks = freeUdpPortBlocks(3, 1);
    const NodeConfig cfg = benchConfig(dir.path, fused, freeUdpPort(), freeUdpPort(), freeUdpPort(),
                                       blocks.at(0), blocks.at(1), blocks.at(2));
    BrokerNode node{cfg};
    if (fused) node.setSlowTickIntervalForTest(tickNs);
    node.start();
    if (!node.awaitLeader(std::chrono::seconds{10})) { state.SkipWithError("no leader"); return; }
    ShardTestClient c(cdir.path);
    const std::string gateway = cfg.clientEndpoint.channel;
    const std::string ingress = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(blocks.at(0));
    const int64_t sid = c.connect(gateway, kClientStream);
    if (sid <= 0) { state.SkipWithError("connect failed"); return; }
    const auto batch = makeBatch(1);
    if (!c.produceUntilOk(ingress, kClientStream, sid, 0, 0, 1, batch)) {
        state.SkipWithError("warm-up produce failed"); return;
    }

    rune_fr::Recorder::instance().drain();
    rune_fr::Recorder::instance().reset();
    rune_fr::setCategoryMask(rune_fr::categoryBit(rune_fr::Category::Runtime));
    rune_fr::enable();
    int64_t corr = 1'000'000;
    int32_t part = 0;
    for (auto _ : state) {
        if (load) {
            const int64_t k = ++corr;
            if (!c.produce(ingress, kClientStream, sid, k, 0, part, 1, batch) || !c.awaitProduceResponse(k)) {
                state.SkipWithError("produce lost"); break;
            }
            part = (part + 1) % kPartitions;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});   // let the shard spin idle
        }
    }
    rune_fr::disable();
    report(state);
    node.stop();
}

BENCHMARK(BM_ShardPassRuntimePlacement)
    ->ArgNames({"fused", "load", "tickUs"})
    ->Args({0, 0, 0})->Args({1, 0, 0})->Args({1, 0, 10'000})
    ->Args({0, 1, 0})->Args({1, 1, 0})->Args({1, 1, 10'000})
    ->Iterations(2000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

} // namespace
