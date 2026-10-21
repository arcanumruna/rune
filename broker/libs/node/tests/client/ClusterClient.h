#pragma once
// ClusterClient.h — catalog history Task 22 (F7): the REAL client (rune::client::Client, Thread runner)
// against the three in-process BrokerNodes of support/CatalogBrokerCluster.h's ThreeNodes.
//   clientConfigFor(c, id)  — a ClientConfig seeded on broker `id`'s client endpoint (own embedded driver,
//                             UDP egress, a data dir under that broker's tmp dir);
//   connectClient(c, id|cfg) — a connected Client;
//   aFollowerOfTheCatalog(c) — a running broker that is not the settled catalog leader.
// Every wait is bounded (the deadline is the failure, never a hang).
#include "support/CatalogBrokerCluster.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/client/Client.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>
#include <chrono>
#include <memory>
#include <string>

namespace rune::node::test::catalog_cluster {

inline rune::client::ClientConfig clientConfigFor(ThreeNodes& c, int id) {
    static int seq = 0;
    rune::client::ClientConfig cfg;
    cfg.gatewayChannel  = c.clientChannel(id);
    cfg.gatewayStreamId = kClientStream;
    cfg.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    cfg.egressStreamId  = 2;
    cfg.dataDir         = (c.clientsDir->path / ("client" + std::to_string(id) + "_" + std::to_string(seq++))).string();
    return cfg;
}

inline std::unique_ptr<rune::client::Client> connectClient(ThreeNodes&, rune::client::ClientConfig cfg) {
    auto client = std::make_unique<rune::client::Client>(cfg);
    client->connectAndWait(std::chrono::seconds(30));
    return client;
}
inline std::unique_ptr<rune::client::Client> connectClient(ThreeNodes& c, int id) {
    return connectClient(c, clientConfigFor(c, id));
}

/// The leader of (topicId, partition) as broker `n`'s control thread sees it (-1: none / unknown).
inline int32_t leaderOn(BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onControlThreadForTest([&n, topicId, partition]() -> int32_t {
        const auto* ms = n.metadataServiceForTest();
        if (ms == nullptr) return -1;
        const auto* pa = ms->assignment(topicId, partition);
        return pa != nullptr ? pa->leaderNode : -1;
    });
}

/// Every running broker agrees on one live leader for every partition of `topicId` (bounded, no sleep).
inline bool awaitLeaders(ThreeNodes& c, int32_t topicId, int32_t parts, std::vector<int32_t>& leaders,
                         std::chrono::milliseconds within) {
    using Clock = std::chrono::steady_clock;
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline; std::this_thread::yield()) {
        leaders.assign(static_cast<std::size_t>(parts), -1);
        bool ok = true;
        const auto running = c.runningNodes();
        for (int32_t p = 0; p < parts && ok; ++p) {
            for (int id : running) {
                const int32_t l = leaderOn(c.node(id), topicId, p);
                if (l <= 0 || std::find(running.begin(), running.end(), l) == running.end()) { ok = false; break; }
                if (leaders[static_cast<std::size_t>(p)] == -1) leaders[static_cast<std::size_t>(p)] = l;
                else if (leaders[static_cast<std::size_t>(p)] != l) { ok = false; break; }
            }
        }
        if (ok) return true;
    }
    return false;
}

/// The consensus leader (controller) once one runs (bounded).
inline int32_t awaitController(ThreeNodes& c) {
    using Clock = std::chrono::steady_clock;
    int32_t ctl = -1;
    for (const auto deadline = Clock::now() + std::chrono::seconds(30); Clock::now() < deadline && ctl < 0; std::this_thread::yield())
        for (BrokerNode* n : c.live())
            if (n->isLeader()) ctl = n->nodeIdForTest();
    return ctl;
}

inline int32_t aFollowerOfTheCatalog(ThreeNodes& c) {
    const int32_t leader = c.settledLeader();
    for (int32_t id : c.runningNodes())
        if (id != leader) return id;
    return -1;
}

} // namespace rune::node::test::catalog_cluster
