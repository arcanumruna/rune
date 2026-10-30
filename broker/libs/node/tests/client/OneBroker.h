#pragma once
// OneBroker.h — catalog history Task 11 (F7): ONE real BrokerNode started
// in-process (single node, real Aeron UDP) for the REAL client
// (rune::client::Client, Thread runner).  Reused by Tasks 13 and 15.
//
//   connectClient()  — a connected Client whose gatewayChannel is the node's
//                      client endpoint (its own embedded driver, UDP egress);
//   catalogEvents()  — the node's __catalog log, read on the admin thread and
//                      decoded with rune_catalog (every event, offset order);
//   endpoint()       — "host:port" of the client endpoint (the CLI's --broker);
//   runSchemaLifecycle — (Task 13) a typed topic's define, ALTER ADD, ALTER
//                      RENAME and SET COMPATIBILITY through the client's *AndWait;
//   catalogSubscribeFromOffsets — (Task 13) the fromOffset of every catalog
//                      Subscribe the node's CatalogReadServer accepted;
//   catalogHistoryEpoch — (Task 13 fix round 2) the epoch it serves;
//   idDescriptor / addColumnOps / viewPlanOver — DDL payload builders.
//
// Every wait is bounded (the deadline is the failure, never a hang).
#include "support/CatalogBrokerCluster.h"
#include "support/TestNodeConfig.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/client/Client.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

class OneBroker {
public:
    /// Legacy (the default, what every earlier suite pins) or the shipped shard runtime (Phase 2c: filtered
    /// subscriptions are served in shard mode only).  `tweak` edits the NodeConfig before every start.
    enum class Runtime { Legacy, Shard };

    explicit OneBroker(const std::string& tag, Runtime runtime = Runtime::Legacy,
                       std::function<void(NodeConfig&)> tweak = {})
        : dir_(std::filesystem::temp_directory_path() /
               ("rune_onebroker_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq_()++))) {
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
        NodeConfig cfg;
        if (runtime == Runtime::Shard) {
            const auto ports = catalog_cluster::Ports::discover(1);
            cfg = catalog_cluster::makeConfig(1, dir_ / "broker", ports, 1, catalog_cluster::Mode::Shard);
            clientPort_   = ports.client[0];
            clientStream_ = catalog_cluster::kClientStream;
        } else {
            pinLegacyRuntime(cfg);
            cfg.nodeId = 0;
            cfg.dataDir = (dir_ / "broker").string();
            cfg.aeronDir = (dir_ / "broker" / "aeron").string();
            cfg.consensusStreamId = 10;
            PeerConfig self;
            self.id = 0;
            self.host = "127.0.0.1";
            self.consensusPort = rune::aeronutil::testing::allocFreeUdpPort();
            cfg.peers = {self};
            clientPort_ = rune::aeronutil::testing::allocFreeUdpPort();
            cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort_);
            cfg.clientEndpoint.streamId = 1;
            cfg.workerCount = 1;
        }
        if (tweak) tweak(cfg);
        tweak_   = std::move(tweak);
        cfg_     = cfg;
        channel_ = cfg.clientEndpoint.channel;
        startNode_();
    }
    ~OneBroker() {
        clients_.clear();
        if (node_) node_->stop();
        node_.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }
    OneBroker(const OneBroker&)            = delete;
    OneBroker& operator=(const OneBroker&) = delete;

    /// A connected real client (owned here too, so it never outlives the broker).
    rune::client::Client* connectClient() {
        rune::client::ClientConfig c;
        c.gatewayChannel  = channel_;
        c.gatewayStreamId = clientStream_;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = (dir_ / ("client" + std::to_string(clients_.size()))).string();
        auto client = std::make_unique<rune::client::Client>(c);
        client->connectAndWait(std::chrono::seconds(20));
        clients_.push_back(std::move(client));
        return clients_.back().get();
    }

    /// A fresh ClientConfig seeded on the node (own embedded driver, UDP egress, own data dir); the caller owns the
    /// Client it builds.
    [[nodiscard]] rune::client::ClientConfig clientConfig() {
        rune::client::ClientConfig c;
        c.gatewayChannel  = channel_;
        c.gatewayStreamId = clientStream_;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = (dir_ / ("own_client" + std::to_string(ownClients_++))).string();
        return c;
    }

    /// A clean stop and a start on the same data dir and ports (the node recovers its topics and logs).  Every
    /// client connected before it loses its session.
    void restart() {
        clients_.clear();
        node_->stop();
        node_.reset();
        startNode_();
    }

    [[nodiscard]] BrokerNode& node() noexcept { return *node_; }
    [[nodiscard]] std::string endpoint() const { return "127.0.0.1:" + std::to_string(clientPort_); }
    [[nodiscard]] const std::string& clientChannel() const noexcept { return channel_; }

    /// The node's whole __catalog log (committed or not), decoded, in offset order.
    std::vector<rune::catalog::RecordedEvent> catalogEvents() {
        BrokerNode& n = *node_;
        return n.onAdminThreadForTest([&n]() {
            std::vector<rune::catalog::RecordedEvent> out;
            rune::replication::PartitionReplica* r = n.catalogReplicaForTest();
            if (r == nullptr) return out;
            rune::storage::Log& log = *r->log();
            int64_t next = 0;
            const int64_t leo = log.logEndOffset();
            while (next < leo) {
                const std::vector<uint8_t> data = log.read(next, 1 << 22);
                if (data.empty()) break;
                const int64_t before = next;
                std::size_t pos = 0;
                while (pos < data.size()) {
                    rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                    if (!reader.ensureValid()) break;
                    const auto tl = static_cast<std::size_t>(reader.totalLength());
                    reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                        if (off < next) return;
                        auto ev = rune::catalog::decode(
                            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                        if (!ev) throw std::logic_error("OneBroker: a __catalog record does not decode");
                        out.push_back(rune::catalog::RecordedEvent{off, std::move(*ev)});
                        next = off + 1;
                    });
                    if (tl == 0) break;
                    pos += tl;
                }
                if (next == before) break;
            }
            return out;
        });
    }

    /// Task 13: four schema events on `name` through the real client — a typed
    /// CreateTopic {id, hp}, ALTER ADD mana, ALTER RENAME hp → health, SET
    /// COMPATIBILITY FORWARD.  Throws on any refusal (a test precondition).
    void runSchemaLifecycle(rune::client::Client& client, const std::string& name) {
        using namespace std::chrono_literals;
        const auto created = client.defineTopicAndWait(name, 1, "id", idDescriptor(name),
                                                       rune::client::Persistence::Disk, {}, {}, 10s);
        if (created.errorCode != 0) throw std::runtime_error("runSchemaLifecycle: define: " + created.detail);
        rune::codec::AlterOp rename{};
        rename.type    = rune::codec::AlterOpType::Rename;
        rename.name    = "hp";
        rename.newName = "health";
        rune::codec::AlterOp compat{};
        compat.type = rune::codec::AlterOpType::SetCompatibility;
        compat.mode = rune::codec::CompatMode::Forward;
        const std::vector<std::vector<uint8_t>> alters = {addColumnOps("mana"), rune::codec::encodeAlterOps({rename}),
                                                          rune::codec::encodeAlterOps({compat})};
        for (const auto& ops : alters) {
            const auto r = client.alterTopicAndWait(name, 0, ops, {}, 10s);
            if (r.errorCode != 0) throw std::runtime_error("runSchemaLifecycle: alter: " + r.detail);
        }
    }

    /// Task 15: all nine event kinds through the real client, 10 events in this order —
    /// 0 CreateTopic players (typed {id, hp}), 1 CreateTopic events (untyped), 2 CreateSchema events,
    /// 3 AlterSchema events (ADD mana), 4 SetCompatibility events (FORWARD), 5 AlterTopicConfig events
    /// (retention.ms 60000), 6 CreateView v (over players), 7 AlterView v (wider output), 8 DropView v,
    /// 9 DropTopic players.  Throws on any refusal (a test precondition).
    void runEveryKind() {
        using namespace std::chrono_literals;
        rune::client::Client& c = *connectClient();
        auto check = [](uint16_t code, const std::string& detail, const char* what) {
            if (code != 0) throw std::runtime_error(std::string("runEveryKind: ") + what + ": " + detail);
        };
        rune::client::DdlOptions ddl;
        ddl.statement = "topic players { id int64, hp int64 } partitions 1 key id;";
        auto r1 = c.defineTopicAndWait("players", 1, "id", idDescriptor("players"), rune::client::Persistence::Disk,
                                       {}, ddl, 10s);
        check(r1.errorCode, r1.detail, "CreateTopic players");
        ddl.statement = "topic events partitions 1;";
        auto r2 = c.defineTopicAndWait("events", 1, "", {}, rune::client::Persistence::Disk, {}, ddl, 10s);
        check(r2.errorCode, r2.detail, "CreateTopic events");
        ddl.statement = {};
        auto r3 = c.defineTopicAndWait("events", 1, "id", idDescriptor("events"), rune::client::Persistence::Disk,
                                       {}, ddl, 10s);
        check(r3.errorCode, r3.detail, "CreateSchema events");
        auto r4 = c.alterTopicAndWait("events", 0, addColumnOps("mana"), ddl, 10s);
        check(r4.errorCode, r4.detail, "AlterSchema events");
        rune::codec::AlterOp compat{};
        compat.type = rune::codec::AlterOpType::SetCompatibility;
        compat.mode = rune::codec::CompatMode::Forward;
        auto r5 = c.alterTopicAndWait("events", 0, rune::codec::encodeAlterOps({compat}), ddl, 10s);
        check(r5.errorCode, r5.detail, "SetCompatibility events");
        const rune::protocol::client::ConfigChangeWire ch{1, 60'000};
        auto r6 = c.alterTopicConfigAndWait("events", 0, std::span(&ch, 1), ddl, 10s);
        check(r6.errorCode, r6.detail, "AlterTopicConfig events");
        auto r7 = c.createViewAndWait("v", "id", 0, viewPlanOver("players"), ddl, 10s);
        check(r7.errorCode, r7.detail, "CreateView v");
        auto r8 = c.alterViewAndWait("v", "id", 0, 1, viewPlanOver("players", true), ddl, 10s);
        check(r8.errorCode, r8.detail, "AlterView v");
        auto r9 = c.dropViewAndWait("v", ddl, 10s);
        check(r9.errorCode, r9.detail, "DropView v");
        auto r10 = c.dropTopicAndWait("players", ddl, 10s);
        check(r10.errorCode, r10.detail, "DropTopic players");
    }

    /// Task 13: the requested fromOffset of each catalog Subscribe the node's
    /// CatalogReadServer accepted, in order (read on the admin thread).
    /// Task 13 fix round 2: the history epoch the node's CatalogReadServer serves.
    uint64_t catalogHistoryEpoch() {
        BrokerNode& n = *node_;
        return n.onAdminThreadForTest([&n]() {
            const auto* rs = n.adminWorkerAgentForTest()->worker().readServer();
            return rs != nullptr ? rs->historyEpoch() : uint64_t{0};
        });
    }
    std::vector<int64_t> catalogSubscribeFromOffsets() {
        BrokerNode& n = *node_;
        return n.onAdminThreadForTest([&n]() {
            const auto* rs = n.adminWorkerAgentForTest()->worker().readServer();
            return rs != nullptr ? rs->acceptedFromOffsetsForTest() : std::vector<int64_t>{};
        });
    }

    /// A typed descriptor: id (Int64 key) + hp (Int64).
    [[nodiscard]] static std::vector<uint8_t> idDescriptor(const std::string& name = "u") {
        return rune::codec::serialize(rune::codec::DescriptorBuilder(name, 1)
                                          .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
                                          .add("hp", rune::codec::PrimType::Int64)
                                          .build());
    }
    /// ALTER ops: ADD COLUMN `field` Int64.
    [[nodiscard]] static std::vector<uint8_t> addColumnOps(const std::string& field) {
        rune::codec::AlterOp op{};
        op.type     = rune::codec::AlterOpType::Add;
        op.name     = field;
        op.primType = rune::codec::PrimType::Int64;
        return rune::codec::encodeAlterOps({op});
    }
    /// An authored {id, hp} Replace view over `source` (its CURRENT catalog topic
    /// id, read on the admin thread); `extra` adds hp2 (an ALTER VIEW's new output).
    [[nodiscard]] std::vector<uint8_t> viewPlanOver(const std::string& source, bool extra = false) {
        BrokerNode& n = *node_;
        const int32_t srcId = n.onAdminThreadForTest([&n, &source]() -> int32_t {
            const auto* t = n.adminWorkerAgentForTest()->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, source);
            return t != nullptr ? t->topicId : -1;
        });
        namespace rv = rune::view;
        rv::ViewPlan p;
        p.viewName = "v";
        p.key      = "id";
        p.sources.push_back(rv::SourceSpec{srcId, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        if (extra)
            p.output.push_back(rv::AuthoredField{"hp2", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        return rv::serializeAuthored(p);
    }

private:
    void startNode_() {
        if (tweak_) tweak_(cfg_);   // re-read on every start: a test that changes what the tweak captures restarts with it
        node_ = std::make_unique<BrokerNode>(cfg_);
        node_->start();
        if (!node_->awaitLeader(std::chrono::seconds(30)))
            throw std::runtime_error("OneBroker: the node never became the leader");
    }
    static int& seq_() {
        static int s = 0;   // single-threaded test binaries
        return s;
    }
    std::filesystem::path                              dir_;
    uint16_t                                           clientPort_ = 0;
    std::string                                        channel_;
    int32_t                                            clientStream_ = 1;
    int                                                ownClients_ = 0;
    NodeConfig                                         cfg_;
    std::function<void(NodeConfig&)>                   tweak_;
    std::unique_ptr<BrokerNode>                        node_;
    std::vector<std::unique_ptr<rune::client::Client>> clients_;
};

} // namespace rune::node::test
