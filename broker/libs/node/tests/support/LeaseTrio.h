#pragma once
// LeaseTrio.h - lease topics Task 17 (decision 4): THREE real BrokerNodes (ThreeNodes, real Aeron UDP, shard mode, default config) hosting a
// REAL client and a replicated lease topic.  Unlike LeaseBrokerRig there is no injected lease clock: on a multi-node cluster the view read
// leases are stamped by the election on the real clock, so these tests run the lease clock in real time with short TTLs (every wait is
// bounded; the deadline is the failure).  The leader of a partition is read from the nodes' metadata and killed with a REAL crash
// (BrokerNode::crashForTest); a node is brought back with startNode over its data dir.
#include "client/ClusterClient.h"
#include "gen/lease_claim.gen.h"
#include "support/CatalogBrokerCluster.h"

#include <rune/client/Client.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/Worker.h>
#include <rune/protocol/LeaseSpec.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rune::node::test {

namespace lt3 {
namespace rc = rune::client;
using rune::gen::LeaseClaim;
using Clock = std::chrono::steady_clock;

inline LeaseClaim row(std::string_view key, uint64_t a = 0) {
    LeaseClaim r;
    if (!key.empty()) std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    r.a = a;
    return r;
}
inline std::vector<uint8_t> claimBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                      .addFixedStr("claim", 32, 1, true)
                                      .add("a", rune::codec::PrimType::UInt64)
                                      .addFixedStr("b", 8)
                                      .build());
}
}  // namespace lt3

using namespace std::chrono_literals;

class LeaseTrio {
public:
    using Client = rune::client::Client;
    using Lt     = rune::client::LeaseTopic<rune::gen::LeaseClaim>;
    struct Topic { int32_t id = -1; int32_t partitions = 0; std::string name; };
    /// What node `id`'s worker shows for (topic, partition): the lease runtime's fence, the ring's start / end and the carry count.
    struct RuntimeSnap {
        bool    has        = false;   ///< the node owns the partition and has a lease runtime
        uint8_t fence      = 0;       ///< FenceState code (3 = Open, 2 = Fencing)
        int64_t readyNs    = 0;
        int64_t deadlineNs = 0;       ///< the Fencing deadline (0 when not Fencing)
        uint64_t carried   = 0;       ///< Carry records appended
        bool    hasRing    = false;
        int64_t ringStart  = -1;      ///< memoryLog()->logStartOffset()
        int64_t ringEnd    = -1;      ///< memoryLog()->logEndOffset()
        uint64_t liveBytes = 0;       ///< live bytes of the lease anchor list (0: no index)
        uint64_t usedBytes = 0;       ///< ring bytes held (wrap padding included)
        uint64_t capBytes  = 0;       ///< the ring's capacity
        uint64_t resets    = 0;       ///< follower ring resets to the leader's start (PartitionReplica::transientLogResetsForTest)
    };

    explicit LeaseTrio(const std::string& tag, std::function<void(NodeConfig&)> tweak = {})
        : c_(catalog_cluster::ThreeNodes::startWith(tag, [t = std::move(tweak)](NodeConfig& cfg) { if (t) t(cfg); })) {
        if (!c_.awaitCatalogLeader(30s)) throw std::runtime_error("LeaseTrio: no catalog leader");
        seed_ = catalog_cluster::aFollowerOfTheCatalog(c_);
    }
    ~LeaseTrio() { if (client_) { client_->close(); client_.reset(); } }

    catalog_cluster::ThreeNodes& nodes() noexcept { return c_; }
    /// The node the client is seeded on.
    [[nodiscard]] int32_t seedNode() const noexcept { return seed_; }
    /// The real client (seeded on a follower of the catalog, so it survives the catalog leader's death), connected lazily.
    Client& client() {
        if (!client_) {
            auto cfg = catalog_cluster::clientConfigFor(c_, seed_);
            cfg.ddlTimeoutMs = 60'000;
            client_ = catalog_cluster::connectClient(c_, cfg);
        }
        return *client_;
    }
    /// A lease topic of replication factor 3 (acks=all is the producer default of the lease handle).
    Topic defineLease(const std::string& name, uint32_t partitions, uint32_t ttlMs, uint32_t minMs, uint32_t maxMs, bool checkpoint = false,
                      uint32_t checkpointIntervalMs = 1000) {
        rune::client::TopicOptions to;
        to.replicationFactor = 3;
        rune::protocol::LeaseSpec s{};
        s.ttlMs = ttlMs;
        s.minTtlMs = minMs;
        s.maxTtlMs = maxMs;
        s.maxKeys = 100'000;
        s.expiryBatch = 64;
        s.checkpoint = checkpoint ? 1 : 0;
        s.checkpointIntervalMs = checkpointIntervalMs;
        s.checkpointRate = 1000;
        to.lease = s;
        to.config.cleanupPolicy = 1;
        const auto r = client().defineTopicAndWait(name, partitions, "claim", lt3::claimBlob(), rune::client::Persistence::Disk, to, {}, 90s);
        if (r.errorCode != 0) throw std::runtime_error("define lease " + name + ": " + r.detail);
        Topic t{r.topicId, static_cast<int32_t>(partitions), name};
        std::vector<int32_t> leaders;
        if (!catalog_cluster::awaitLeaders(c_, t.id, t.partitions, leaders, 60s)) throw std::runtime_error("no partition leaders for " + name);
        return t;
    }
    /// A TRANSIENT lease topic of replication factor 3 (the ring is the log; an explicit max_ttl is required).
    Topic defineTransientLease(const std::string& name, uint32_t partitions, uint32_t ttlMs, uint32_t minMs, uint32_t maxMs) {
        rune::client::TopicOptions to;
        to.replicationFactor = 3;
        rune::protocol::LeaseSpec s{};
        s.ttlMs = ttlMs;
        s.minTtlMs = minMs;
        s.maxTtlMs = maxMs;
        s.maxKeys = 100'000;
        s.expiryBatch = 64;
        s.checkpoint = 0;
        s.checkpointIntervalMs = 1000;
        s.checkpointRate = 1000;
        to.lease = s;
        to.config.cleanupPolicy = 1;
        const auto r = client().defineTopicAndWait(name, partitions, "claim", lt3::claimBlob(), rune::client::Persistence::Transient, to, {}, 90s);
        if (r.errorCode != 0) throw std::runtime_error("define transient lease " + name + ": " + r.detail);
        Topic t{r.topicId, static_cast<int32_t>(partitions), name};
        std::vector<int32_t> leaders;
        if (!catalog_cluster::awaitLeaders(c_, t.id, t.partitions, leaders, 60s)) throw std::runtime_error("no partition leaders for " + name);
        return t;
    }
    /// The lease runtime / ring of (topic, partition) on running node `nodeId`, read on its worker thread (has == false: not owned there).
    RuntimeSnap runtimeOn(int32_t nodeId, int32_t topicId, int32_t partition) {
        BrokerNode& n = c_.node(nodeId);
        for (int32_t w = 0; w < n.workerCountForTest(); ++w) {
            const RuntimeSnap s = n.onWorkerThreadForTest(w, [topicId, partition](rune::node::Worker& wk) {
                RuntimeSnap r;
                const auto* rt = wk.leaseRuntimeForTest(topicId, partition);
                if (const auto* rep = wk.findOwnedReplicaForTest(topicId, partition); rep != nullptr && rep->memoryLog() != nullptr) {
                    const auto* mem = rep->memoryLog();
                    r.hasRing   = true;
                    r.ringStart = mem->logStartOffset();
                    r.ringEnd   = mem->logEndOffset();
                    r.usedBytes = mem->bytesFrom(0);
                    r.capBytes  = rep->transientCapacityBytes();
                    r.resets    = rep->transientLogResetsForTest();
                }
                r.liveBytes = wk.leaseTransientLiveBytesForTest(topicId, partition);
                if (rt == nullptr) return r;
                r.has     = true;
                r.fence   = rt->fenceStateCode();
                r.readyNs = rt->readyNs();
                if (const auto* f = std::get_if<rune::node::lease::LeaseRuntime::FenceFencing>(&rt->fenceState())) r.deadlineNs = f->deadlineNs;
                r.carried = rt->stats().carried;
                return r;
            });
            if (s.has || s.hasRing) return s;
        }
        return {};
    }

    /// Opens the topic; the client's catalog view may trail the DDL for a moment ("does not exist" / "generated type is stale"): bounded retries.
    Lt open(const Topic& t, uint32_t opTimeoutMs = 0) {
        const auto end = lt3::Clock::now() + 90s;
        rune::client::LeaseTopicConfig cfg;
        cfg.opTimeoutMs = opTimeoutMs;   // the handle's op timeout = the writer's delivery timeout; 0 = ttl / 3
        for (;;) {
            try { return client().openLeaseTopicAndWait<rune::gen::LeaseClaim>(t.name, cfg, 30s); }
            catch (const std::exception&) { if (lt3::Clock::now() >= end) throw; std::this_thread::sleep_for(200ms); }
        }
    }

    /// The node that leads (topic, partition) as the running nodes agree (-1: none).
    int32_t leaderOf(const Topic& t, int32_t partition) {
        std::vector<int32_t> leaders;
        if (!catalog_cluster::awaitLeaders(c_, t.id, t.partitions, leaders, 30s)) return -1;
        return leaders[static_cast<std::size_t>(partition)];
    }
    /// Crashes the leader of (topic, partition) (a REAL crash) and waits (bounded) for a new leader among the survivors; returns the dead id.
    int32_t killLeader(const Topic& t, int32_t partition) {
        const int32_t old = leaderOf(t, partition);
        if (old < 1) throw std::runtime_error("killLeader: no leader");
        c_.crash(old);
        const auto end = lt3::Clock::now() + 60s;
        while (lt3::Clock::now() < end) {
            std::vector<int32_t> leaders;
            if (catalog_cluster::awaitLeaders(c_, t.id, t.partitions, leaders, 2s) && leaders[static_cast<std::size_t>(partition)] != old) return old;
        }
        throw std::runtime_error("killLeader: no new leader");
    }
    /// Brings a crashed node back over its data dir and waits (bounded) until every running node agrees on the partition leaders again.
    void restartNode(int id, const Topic& t) {
        c_.startNode(id);
        std::vector<int32_t> leaders;
        if (!catalog_cluster::awaitLeaders(c_, t.id, t.partitions, leaders, 60s)) throw std::runtime_error("restartNode: no leaders");
    }

    /// The ISR of (topic, partition) as running node `viewer` sees it.
    std::vector<int32_t> isrOn(int32_t viewer, const Topic& t, int32_t partition) {
        BrokerNode& n = c_.node(viewer);
        return n.onControlThreadForTest([&n, &t, partition]() -> std::vector<int32_t> {
            const auto* ms = n.metadataServiceForTest();
            if (ms == nullptr) return {};
            const auto* pa = ms->assignment(t.id, partition);
            return pa != nullptr ? pa->isr : std::vector<int32_t>{};
        });
    }
    /// The preferred replica (replicas[0]) of (topic, partition) as running node `viewer` sees it (-1: unknown).
    int32_t preferredOn(int32_t viewer, const Topic& t, int32_t partition) {
        BrokerNode& n = c_.node(viewer);
        return n.onControlThreadForTest([&n, &t, partition]() -> int32_t {
            const auto* ms = n.metadataServiceForTest();
            if (ms == nullptr) return -1;
            const auto* pa = ms->assignment(t.id, partition);
            return pa != nullptr && !pa->replicas.empty() ? pa->replicas[0] : -1;
        });
    }
    /// True when every running node reports `want` in-sync replicas for the partition (bounded).
    bool awaitIsrSize(const Topic& t, int32_t partition, std::size_t want, std::chrono::milliseconds within) {
        return until(within, [&] {
            for (int id : c_.runningNodes())
                if (isrOn(id, t, partition).size() != want) return false;
            return true;
        });
    }

    /// Polls (bounded) until `pred` holds; each poll is a client round trip or a short yield.
    template <class Pred>
    static bool until(std::chrono::milliseconds within, Pred&& pred) {
        const auto end = lt3::Clock::now() + within;
        const int64_t cap = within.count() * 4 + 16;   // a backstop for the iteration count
        for (int64_t i = 0; i < cap && lt3::Clock::now() < end; ++i) {
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return pred();
    }

private:
    catalog_cluster::ThreeNodes c_;
    int32_t                     seed_ = -1;
    std::unique_ptr<Client>     client_;
};

}  // namespace rune::node::test
