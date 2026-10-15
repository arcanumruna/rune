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

#include <atomic>
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

inline int32_t aFollowerOfTheCatalog(ThreeNodes& c) {
    const int32_t leader = c.settledLeader();
    for (int32_t id : c.runningNodes())
        if (id != leader) return id;
    return -1;
}

} // namespace rune::node::test::catalog_cluster
