// FilterBrokerRegressionTest.cpp - Phase 2c Task 13: broker-side subscription filters against REAL brokers and
// the REAL client: every selection {Key, Partition, AllPartitions, Group} x filter {Eq, EqAndIn} x timing
// {Steady, LeaderMove, RestartResubscribe, AlterDrop, RcAborted}, plus one test per refusal row of the spec
// (E1, E3-E6, E8, E9, E10, E12), a control (an unfiltered neighbour) and the quiet-filter group commit.
//
// Rig: Invoker-mode clients (no client threads: every handler runs on THIS thread inside doWork(), so there is
// no mutex, no atomic and no sleep; every wait is a bounded pump loop - the deadline is the failure, never a
// hang).  Topics come from DDL only.  A cell produces a deterministic seeded stream of player_event records (the
// record's hp field carries its sequence number, so a delivered record is identified by its own bytes) and
// asserts, per subscription, the EXACT delivered set against the model (nothing missing, nothing extra, no
// duplicate), every delivered record's fields (a `worldId == 7` filter delivers only worldId-7 records), and the
// position reaching each partition's end.  The expected outcome of the timing comes from
// FilterMatrixExpectations.h (Task 14's table).
//
// Single-broker cells run on OneBroker in the shipped shard runtime; the LeaderMove cells need a cluster
// (ThreeNodes, the partition leader is crashed mid-stream).  Registered in rune_client_failover_broker_tests
// (RUN_SERIAL): it kills and restarts brokers.
#include "client/ClusterClient.h"
#include "client/OneBroker.h"
#include "filter/PlayerEventSchema.h"
#include "player_event.gen.h"
#include "support/FilterMatrixExpectations.h"
#include "support/ShardTestClient.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/FilterBuilder.h>
#include <rune/codec/Flyweight.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test::catalog_cluster;
namespace rc = rune::client;
namespace fm = rune::node::filtermatrix;
namespace fly = rune::codec::fly;
using E     = rune::gen::PlayerEvent;
using Clock = std::chrono::steady_clock;

namespace {

constexpr int32_t  kParts       = 4;
constexpr int      kRecords     = 2000;
constexpr uint64_t kPlayers     = 24;   // playerId in [0, 24)
constexpr uint32_t kWorld       = 7;    // the filtered world
constexpr uint16_t kInvalidRequest = 39;
const char*        kTopic       = "player_event";

// ---------------------------------------------------------------------------------------------------------
// The player_event schema over its three versions (v1; v2 ADD guild; v3 DROP channel), and the model.
// ---------------------------------------------------------------------------------------------------------

struct Schema {
    std::vector<rune::codec::Descriptor> materialized;   // index = version (0 unused)
    Schema() {
        using namespace rune::codec;
        const Descriptor v1 = rune::examples::playerEventDescriptor();
        uint16_t nextTag = 9;
        auto v2 = applyOps(v1, {AlterOp{AlterOpType::Add, "guild", "", PrimType::UInt32, 0, {}}}, nextTag, 2);
        if (!v2) throw std::runtime_error("schema: add guild");
        auto v3 = applyOps(*v2, {AlterOp{AlterOpType::Drop, "channel", "", PrimType::Int32, 0, {}}}, nextTag, 3);
        if (!v3) throw std::runtime_error("schema: drop channel");
        materialized.push_back(v1);   // placeholder at index 0
        materialized.push_back(materializeAsOf(v1, 1));
        materialized.push_back(materializeAsOf(*v2, 2));
        materialized.push_back(materializeAsOf(*v3, 3));
    }
};
const Schema& schema() {
    static const Schema s;
    return s;
}

std::vector<uint8_t> addGuildOps() {
    return rune::codec::encodeAlterOps(
        {rune::codec::AlterOp{rune::codec::AlterOpType::Add, "guild", "", rune::codec::PrimType::UInt32, 0, {}}});
}
std::vector<uint8_t> dropChannelOps() {
    return rune::codec::encodeAlterOps(
        {rune::codec::AlterOp{rune::codec::AlterOpType::Drop, "channel", "", rune::codec::PrimType::Int32, 0, {}}});
}

struct Ev {
    int64_t  seq      = 0;   // carried in the record's hp field
    uint64_t playerId = 0;
    uint32_t worldId  = 0;
    int32_t  channel  = 0;
    uint32_t guild    = 0;
    uint16_t version  = 1;
    int32_t  part     = 0;
    bool     visible  = true;   // false: the record sits in an aborted transaction
};

/// [blockLength u16][version u16][fixed block at the materialized offsets of `version`]; fields absent there
/// (guild in v1, channel in v3) are skipped.  No variable-length section (name stays empty).
std::vector<uint8_t> encodeEv(const Ev& e) {
    const rune::codec::Descriptor& m = schema().materialized[e.version];
    std::vector<uint8_t> out(4 + m.blockLength, 0);
    fly::store<uint16_t>(out.data(), static_cast<uint16_t>(m.blockLength));
    fly::store<uint16_t>(out.data() + 2, e.version);
    uint8_t* blk = out.data() + 4;
    auto at = [&](const char* n) -> uint8_t* {
        const auto* f = m.find(n);
        return f != nullptr ? blk + f->offset : nullptr;
    };
    if (auto* p = at("playerId")) fly::store<uint64_t>(p, e.playerId);
    if (auto* p = at("worldId")) fly::store<uint32_t>(p, e.worldId);
    if (auto* p = at("channel")) fly::store<int32_t>(p, e.channel);
    if (auto* p = at("zone")) fly::storeFixedStr(p, "eu", 8);
    if (auto* p = at("kind")) fly::store<int32_t>(p, static_cast<int32_t>(1 + e.seq % 3));
    if (auto* p = at("alive")) fly::store<bool>(p, (e.seq & 1) != 0);
    if (auto* p = at("hp")) fly::store<int64_t>(p, e.seq);
    if (auto* p = at("guild")) fly::store<uint32_t>(p, e.guild);
    return out;
}

struct Decoded {
    bool     ok         = false;
    int64_t  seq        = -1;
    uint64_t playerId   = 0;
    uint32_t worldId    = 0;
    bool     hasChannel = false;
    int32_t  channel    = 0;
    uint16_t version    = 0;
};
Decoded decodeRecord(std::span<const uint8_t> v) {
    Decoded d;
    if (v.size() < 4) return d;
    const auto bl  = fly::load<uint16_t>(v.data());
    d.version      = fly::load<uint16_t>(v.data() + 2);
    if (d.version < 1 || d.version > 3 || v.size() < 4u + bl) return d;
    const rune::codec::Descriptor& m = schema().materialized[d.version];
    if (bl != m.blockLength) return d;
    const uint8_t* blk = v.data() + 4;
    d.playerId = fly::load<uint64_t>(blk + m.find("playerId")->offset);
    d.worldId  = fly::load<uint32_t>(blk + m.find("worldId")->offset);
    d.seq      = fly::load<int64_t>(blk + m.find("hp")->offset);
    if (const auto* f = m.find("channel")) {
        d.hasChannel = true;
        d.channel    = fly::load<int32_t>(blk + f->offset);
    }
    d.ok = true;
    return d;
}

struct Cuts {
    int v2From = kRecords;   // seq >= v2From is written as v2
    int v3From = kRecords;   // seq >= v3From is written as v3
    int abortedFrom = kRecords;   // seq >= abortedFrom sits in an aborted transaction
};

std::vector<Ev> makeModel(uint64_t seed, const Cuts& cuts) {
    std::mt19937_64 rng(seed);
    std::vector<Ev> m(kRecords);
    for (int i = 0; i < kRecords; ++i) {
        Ev& e    = m[static_cast<std::size_t>(i)];
        e.seq    = i;
        e.playerId = rng() % kPlayers;
        e.worldId  = static_cast<uint32_t>(rng() % 16);
        e.channel  = static_cast<int32_t>(1 + rng() % 4);
        e.guild    = static_cast<uint32_t>(rng() % 5);
        e.version  = i >= cuts.v3From ? 3 : i >= cuts.v2From ? 2 : 1;
        e.part     = rc::Key(e.playerId).partition(kParts);
        e.visible  = i < cuts.abortedFrom;
    }
    return m;
}

// ---------------------------------------------------------------------------------------------------------
// Filters, predicates and per-subscription checks.
// ---------------------------------------------------------------------------------------------------------

enum class Selection : uint8_t { Key, Partition, AllPartitions, Group };
enum class FKind : uint8_t { Eq, EqAndIn };
enum class TimingK : uint8_t { Steady, LeaderMove, RestartResubscribe, AlterDrop, RcAborted };

const char* name(Selection s) { return s == Selection::Key ? "Key" : s == Selection::Partition ? "Partition" : s == Selection::AllPartitions ? "AllPartitions" : "Group"; }
const char* name(FKind f) { return f == FKind::Eq ? "Eq" : "EqAndIn"; }
const char* name(TimingK t) {
    switch (t) {
    case TimingK::Steady: return "Steady";
    case TimingK::LeaderMove: return "LeaderMove";
    case TimingK::RestartResubscribe: return "RestartResubscribe";
    case TimingK::AlterDrop: return "AlterDrop";
    case TimingK::RcAborted: return "RcAborted";
    }
    return "?";
}

/// What a subscription wants: `key` set = that player's records (the spec §3 idiom), else the filtered world;
/// EqAndIn adds `channel IN (1, 2)`.
struct Pred {
    fm::Op                  op = fm::Op::Eq;
    std::optional<uint64_t> key;
    uint32_t                world = kWorld;
};

rune::codec::FilterBytes filterFor(const Pred& p) {
    const bool in = p.op == fm::Op::EqAndIn;
    if (p.key) {
        const uint64_t k = *p.key;
        return in ? E::Filter{.playerId = k, .channel = rune::anyOf(1, 2)}.encode() : E::Filter{.playerId = k}.encode();
    }
    return in ? E::Filter{.worldId = p.world, .channel = rune::anyOf(1, 2)}.encode() : E::Filter{.worldId = p.world}.encode();
}

/// The model's verdict.  A v3 record has no channel: a term on it excludes the record (AbsentExcluded).
bool matches(const Pred& p, const Ev& e) {
    if (p.key ? e.playerId != *p.key : e.worldId != p.world) return false;
    if (p.op == fm::Op::EqAndIn) return e.version < 3 && (e.channel == 1 || e.channel == 2);
    return true;
}
/// The same verdict from the DELIVERED bytes alone.
bool matchesDecoded(const Pred& p, const Decoded& d) {
    if (p.key ? d.playerId != *p.key : d.worldId != p.world) return false;
    if (p.op == fm::Op::EqAndIn) return d.hasChannel && (d.channel == 1 || d.channel == 2);
    return true;
}

struct Check {
    std::string           label;
    Pred                  pred;
    std::set<int32_t>     parts;
    const std::vector<Ev>* model = nullptr;
    std::map<int64_t, int> seen;
    std::vector<std::string> bad;   // immediate violations (a foreign record, a mangled field)

    void onRecord(int32_t partition, std::span<const uint8_t> value) {
        const Decoded d = decodeRecord(value);
        if (!d.ok || d.seq < 0 || d.seq >= static_cast<int64_t>(model->size())) {
            bad.push_back(label + ": undecodable record");
            return;
        }
        const Ev& e = (*model)[static_cast<std::size_t>(d.seq)];
        if (d.playerId != e.playerId || d.worldId != e.worldId || d.version != e.version ||
            d.hasChannel != (e.version < 3) || (d.hasChannel && d.channel != e.channel))
            bad.push_back(label + ": seq " + std::to_string(d.seq) + " arrived with mangled fields");
        if (!matchesDecoded(pred, d))
            bad.push_back(label + ": seq " + std::to_string(d.seq) + " does not match the filter (world " +
                          std::to_string(d.worldId) + ", player " + std::to_string(d.playerId) + ")");
        if (partition != e.part || parts.count(partition) == 0)
            bad.push_back(label + ": seq " + std::to_string(d.seq) + " arrived on partition " + std::to_string(partition));
        if (!e.visible) bad.push_back(label + ": seq " + std::to_string(d.seq) + " belongs to an aborted transaction");
        ++seen[d.seq];
    }
    [[nodiscard]] std::set<int64_t> expected(int upTo) const {
        std::set<int64_t> out;
        for (int i = 0; i < upTo; ++i) {
            const Ev& e = (*model)[static_cast<std::size_t>(i)];
            if (e.visible && parts.count(e.part) != 0 && matches(pred, e)) out.insert(e.seq);
        }
        return out;
    }
    [[nodiscard]] bool covers(int upTo) const {
        for (const int64_t s : expected(upTo))
            if (seen.count(s) == 0) return false;
        return true;
    }
    void verifyExact(int upTo) const {
        const auto want = expected(upTo);
        int shown = 0;
        for (const int64_t s : want) {
            const auto it = seen.find(s);
            if (it == seen.end() && shown++ < 6) ADD_FAILURE() << label << ": seq " << s << " never delivered";
            else if (it != seen.end() && it->second != 1 && shown++ < 6)
                ADD_FAILURE() << label << ": seq " << s << " delivered " << it->second << " times";
        }
        for (const auto& [s, n] : seen)
            if (want.count(s) == 0 && shown++ < 6) ADD_FAILURE() << label << ": seq " << s << " delivered but not expected";
        for (std::size_t i = 0; i < bad.size() && i < 6; ++i) ADD_FAILURE() << bad[i];
        EXPECT_TRUE(bad.empty()) << label << ": " << bad.size() << " violations";
    }
};

// ---------------------------------------------------------------------------------------------------------
// Pumping.
// ---------------------------------------------------------------------------------------------------------

using Clients = std::vector<rc::Client*>;

template <class P>
bool pumpUntil(const Clients& cs, P&& pred, std::chrono::seconds budget = 90s) {
    for (const auto dl = Clock::now() + budget; Clock::now() < dl; std::this_thread::yield()) {
        for (rc::Client* c : cs) (void)c->doWork();
        if (pred()) return true;
    }
    return false;
}
void pumpFor(const Clients& cs, std::chrono::milliseconds d) {
    for (const auto dl = Clock::now() + d; Clock::now() < dl; std::this_thread::yield())
        for (rc::Client* c : cs) (void)c->doWork();
}

// ---------------------------------------------------------------------------------------------------------
// The cluster: OneBroker (shard runtime) or ThreeNodes.
// ---------------------------------------------------------------------------------------------------------

struct Cluster {
    std::unique_ptr<rune::node::test::OneBroker> one;
    std::unique_ptr<ThreeNodes>                  trio;
    int32_t                                      ctl = -1;
    std::unique_ptr<TmpDir>                      scratch;
    std::unique_ptr<rune::node::test::ShardTestClient> oc;   // OffsetFetch probe (solo only)
    std::map<int, int64_t>                       ocSids;   // node id -> the probe's session on it

    static Cluster solo(const std::string& tag, std::function<void(rune::node::NodeConfig&)> tweak = {},
                        rune::node::test::OneBroker::Runtime rt = rune::node::test::OneBroker::Runtime::Shard) {
        Cluster c;
        c.one     = std::make_unique<rune::node::test::OneBroker>(tag, rt, std::move(tweak));
        c.scratch = std::make_unique<TmpDir>(tag + "_scratch");
        return c;
    }
    static Cluster three(const std::string& tag) {
        Cluster c;
        c.trio    = std::make_unique<ThreeNodes>(ThreeNodes::start(tag));
        c.scratch = std::make_unique<TmpDir>(tag + "_scratch");
        if (!c.trio->awaitCatalogLeader(30s)) throw std::runtime_error("no catalog leader");
        c.ctl = awaitController(*c.trio);
        if (c.ctl <= 0) throw std::runtime_error("no controller");
        return c;
    }

    rc::ClientConfig cfg(std::vector<rc::TopicSpec> topics, int via = 0) {
        rc::ClientConfig k = one ? one->clientConfig() : clientConfigFor(*trio, via > 0 ? via : ctl);
        k.topics                       = std::move(topics);
        k.runnerMode                   = rc::RunnerMode::Invoker;
        k.connectTimeoutMs             = 30'000;
        k.pingTimeoutMs                = 30'000;
        k.aeronClientLivenessTimeoutNs = 120'000'000'000LL;   // a broker crash blocks this thread for a while
        return k;
    }
    std::unique_ptr<rc::Client> open(std::vector<rc::TopicSpec> topics, int via = 0,
                                     std::function<void(const std::string&)> onError = {}) {
        auto c = std::make_unique<rc::Client>(cfg(std::move(topics), via));
        if (onError) c->onError(std::move(onError));
        c->connectAndWait(30s);
        return c;
    }

    rc::TopicSpec define(const std::string& name, int32_t parts, std::vector<uint8_t> blob, const std::string& keyField,
                         rc::Persistence persistence = rc::Persistence::Disk) {
        rc::ClientConfig k = cfg({});
        k.runnerMode       = rc::RunnerMode::Thread;
        auto admin         = std::make_unique<rc::Client>(k);
        admin->connectAndWait(30s);
        rc::TopicOptions to;
        to.replicationFactor = one ? 1 : 3;
        const auto def = admin->defineTopicAndWait(name, static_cast<uint32_t>(parts), keyField, blob,
                                                   /*validate=*/false, persistence, to, {}, 60s);
        if (def.errorCode != 0) throw std::runtime_error("define " + name + ": " + def.detail);
        admin->close();
        if (trio) {
            std::vector<int32_t> leaders;
            if (!awaitLeaders(*trio, def.topicId, parts, leaders, 30s)) throw std::runtime_error("no leaders for " + name);
        }
        return rc::TopicSpec{name, def.topicId, parts};
    }
    rc::TopicSpec definePlayerEvent(rc::Persistence persistence = rc::Persistence::Disk) {
        return define(kTopic, kParts, rune::codec::serialize(rune::examples::playerEventDescriptor()), "playerId", persistence);
    }
    void alter(const std::string& name, const std::vector<uint8_t>& ops) {
        rc::ClientConfig k = cfg({});
        k.runnerMode       = rc::RunnerMode::Thread;
        auto admin         = std::make_unique<rc::Client>(k);
        admin->connectAndWait(30s);
        const auto r = admin->alterTopicAndWait(name, 0, ops, {}, 60s);
        admin->close();
        if (r.errorCode != 0) throw std::runtime_error("alter " + name + ": " + r.detail);
    }

    /// The partition's high watermark on its leader.
    int64_t end(int32_t topicId, int32_t partition) {
        auto hw = [](rune::replication::PartitionReplica* r) -> int64_t { return r != nullptr ? r->highWatermark() : -1; };
        if (one) return one->node().onOwnerThreadForTest(topicId, partition, hw);
        for (int id : trio->runningNodes()) {
            const int32_t l = leaderOn(trio->node(id), topicId, partition);
            if (l > 0 && trio->running(l)) return trio->node(l).onOwnerThreadForTest(topicId, partition, hw);
        }
        return -1;
    }
    std::vector<int64_t> ends(int32_t topicId) {
        std::vector<int64_t> v;
        for (int32_t p = 0; p < kParts; ++p) v.push_back(end(topicId, p));
        return v;
    }

    /// The group's committed offsets: one OffsetFetch per partition, asked of every running broker until the
    /// coordinator answers (the others say NotCoordinator).  -2: nobody answered yet.
    std::vector<int64_t> committed(const std::string& group, int32_t topicId) {
        const int32_t stream = rune::node::test::catalog_cluster::kClientStream;
        std::vector<std::pair<std::string, int>> nodes;   // channel, node id
        if (one) nodes.emplace_back(one->clientChannel(), 0);
        else for (int id : trio->runningNodes()) nodes.emplace_back(trio->clientChannel(id), id);
        if (!oc) {
            oc = std::make_unique<rune::node::test::ShardTestClient>(scratch->path / "oc");
            oc->setClientVersion(rune::protocol::client::kClientVersion);
        }
        std::vector<int64_t> out(kParts, -2);
        for (const auto& [channel, id] : nodes) {
            auto it = ocSids.find(id);
            if (it == ocSids.end()) it = ocSids.emplace(id, oc->connect(channel, stream)).first;
            if (it->second <= 0) { ocSids.erase(it); continue; }
            for (int32_t p = 0; p < kParts; ++p) {
                if (out[static_cast<std::size_t>(p)] != -2) continue;
                const auto r = oc->offsetFetch(channel, stream, it->second, group, topicId, p);
                if (r && r->second == 0) out[static_cast<std::size_t>(p)] = r->first;
            }
        }
        return out;
    }
    void restart() {
        oc.reset();
        ocSids.clear();
        one->restart();
    }
};

// ---------------------------------------------------------------------------------------------------------
// The reader: one client side, plain subscriptions or a group consumer, over a set of Checks.
// ---------------------------------------------------------------------------------------------------------

struct HandleSpec {
    std::size_t check;
    int32_t     part;
    uint64_t    key = 0;   // Key selection only
};

struct Reader {
    Selection             sel;
    rc::TopicHandle       th{};
    int32_t               topicId = 0;
    rc::IsolationLevel    iso = rc::IsolationLevel::ReadUncommitted;
    std::deque<Check>     checks;
    std::vector<HandleSpec> specs;
    std::string           group;

    // live state
    std::vector<std::unique_ptr<rc::Subscription>> subs;
    std::unique_ptr<rc::SubscriptionSet>           set;
    std::vector<rc::Subscription*>                 live;   // one per spec, in spec order
    std::optional<rc::GroupConsumer>               gc;

    Reader(Selection s, fm::Op op, const std::vector<Ev>& model, std::string groupId) : sel(s), group(std::move(groupId)) {
        auto mk = [&](std::string label, Pred p, std::set<int32_t> parts) -> std::size_t {
            Check c;
            c.label = std::move(label);
            c.pred  = p;
            c.parts = std::move(parts);
            c.model = &model;
            checks.push_back(std::move(c));
            return checks.size() - 1;
        };
        std::set<int32_t> all;
        for (int32_t p = 0; p < kParts; ++p) all.insert(p);
        switch (s) {
        case Selection::Key:
            for (uint64_t k = 0; k < kPlayers; ++k) {
                const int32_t p = rc::Key(k).partition(kParts);
                specs.push_back({mk("key " + std::to_string(k), Pred{op, k}, {p}), p, k});
            }
            break;
        case Selection::Partition:
            for (int32_t p = 0; p < kParts; ++p) specs.push_back({mk("partition " + std::to_string(p), Pred{op, std::nullopt}, {p}), p});
            break;
        case Selection::AllPartitions: {
            const std::size_t c = mk("all partitions", Pred{op, std::nullopt}, all);
            for (int32_t p = 0; p < kParts; ++p) specs.push_back({c, p});
            break;
        }
        case Selection::Group: {
            const std::size_t c = mk("group", Pred{op, std::nullopt}, all);
            for (int32_t p = 0; p < kParts; ++p) specs.push_back({c, p});
            break;
        }
        }
    }
    Reader(const Reader&) = delete;

    [[nodiscard]] bool isGroup() const { return sel == Selection::Group; }

    [[nodiscard]] rc::SubscribeOptions optsFor(const HandleSpec& h, const std::vector<int64_t>* resume, std::size_t i) const {
        rc::SubscribeOptions o;
        o.from      = resume != nullptr ? rc::From::offset((*resume)[i]) : rc::From::earliest();
        o.isolation = iso == rc::IsolationLevel::ReadCommitted ? rc::Isolation::ReadCommitted : rc::Isolation::ReadUncommitted;
        o.filter(filterFor(checks[h.check].pred));
        return o;
    }

    /// Plain subscriptions (all but Group).  `resume`: each handle's position() of a previous incarnation.
    void subscribe(rc::Client& client, const std::vector<int64_t>* resume = nullptr) {
        subs.clear();
        set.reset();
        live.clear();
        if (sel == Selection::AllPartitions && resume == nullptr) {
            Check* chk = &checks[specs[0].check];
            set = std::make_unique<rc::SubscriptionSet>(client.subscribe(
                th, rc::AllPartitions{}, optsFor(specs[0], nullptr, 0),
                [chk](const rc::Record& r) { chk->onRecord(r.partition, r.value); }));
            for (const HandleSpec& h : specs) live.push_back(&(*set)[h.part]);
            return;
        }
        // (A SubscriptionSet takes one From for all partitions, so a resume subscribes per partition.)
        for (std::size_t i = 0; i < specs.size(); ++i) {
            const HandleSpec& h = specs[i];
            Check* chk = &checks[h.check];
            auto handler = [chk](const rc::Record& r) { chk->onRecord(r.partition, r.value); };
            if (sel == Selection::Key)
                subs.push_back(std::make_unique<rc::Subscription>(client.subscribe(th, rc::Key(h.key), optsFor(h, resume, i), handler)));
            else
                subs.push_back(std::make_unique<rc::Subscription>(client.subscribe(th, h.part, optsFor(h, resume, i), handler)));
            live.push_back(subs.back().get());
        }
    }

    /// The group consumer (Group only): the callbacks go on before connect.
    void joinGroup(rc::Client& client) {
        rc::GroupConfig g;
        g.autoCommit          = false;
        g.autoOffsetReset     = rc::OffsetReset::Earliest;
        g.isolation           = iso;
        g.sessionTimeoutMs    = 30'000;
        g.commitSyncTimeoutMs = 30'000;
        g.filter(filterFor(checks[0].pred));
        gc.emplace(client.subscribeGroup(group, {kTopic}, g));
        Check* chk = &checks[0];
        gc->onRecords([chk](const rc::RecordSet& rs) {
            const int32_t part = rs.partition;
            rs.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
                chk->onRecord(part, {reinterpret_cast<const uint8_t*>(v.data()), v.size()});
            });
        });
    }

    [[nodiscard]] bool covers(int upTo) const {
        for (const Check& c : checks)
            if (!c.covers(upTo)) return false;
        return true;
    }
    /// A plain subscription that ended on its own (refused, lost) is a failure, not a timeout.
    [[nodiscard]] bool anyFinished() const {
        for (const rc::Subscription* s : live)
            if (s->finished()) return true;
        return gc && gc->finished();
    }
    [[nodiscard]] std::vector<int64_t> positions() const {
        std::vector<int64_t> v;
        for (const rc::Subscription* s : live) v.push_back(s->position());
        return v;
    }
    void stop(const Clients& cs) {
        if (gc) gc->close();
        for (rc::Subscription* s : live) s->unsubscribe();
        pumpUntil(cs, [&] {
            for (const rc::Subscription* s : live)
                if (!s->finished()) return false;
            return !gc || gc->finished();
        }, 30s);
        live.clear();
        subs.clear();
        set.reset();
        gc.reset();
    }
    void verifyExact(int upTo) const {
        for (const Check& c : checks) c.verifyExact(upTo);
    }
};

// ---------------------------------------------------------------------------------------------------------
// The feeder: the model onto the topic through one producer.
// ---------------------------------------------------------------------------------------------------------

struct Feeder {
    rc::Client&             client;
    rc::Producer            p;
    rc::TopicHandle         th;
    const std::vector<Ev>&  model;
    bool                    transactional;
    std::size_t             sent = 0, acked = 0, failed = 0;

    Feeder(rc::Client& c, rc::TopicHandle t, const std::vector<Ev>& m, std::string txnId = {})
        : client(c), p(c.producer(config(txnId))), th(t), model(m), transactional(!txnId.empty()) {
        p.onAck(&Feeder::onAck, this);
    }
    Feeder(const Feeder&) = delete;

    static rc::ProducerConfig config(std::string txnId) {
        rc::ProducerConfig cfg;
        cfg.acks                   = rc::Acks::All;
        cfg.lingerUs               = 2'000;
        cfg.maxInFlight            = 16;
        cfg.produceTimeoutMs       = 30'000;
        cfg.deliveryTimeoutMs      = 60'000;
        cfg.transactionOpTimeoutMs = 45'000;
        cfg.transactionalId        = std::move(txnId);
        return cfg;
    }
    static void onAck(void* ctx, rc::Token, int64_t, rc::OpStatus s, uint16_t) noexcept {
        auto* self = static_cast<Feeder*>(ctx);
        ++self->acked;
        if (s != rc::OpStatus::Ok) ++self->failed;
    }
    void send(int from, int to) {
        for (int i = from; i < to; ++i) {
            const Ev& e = model[static_cast<std::size_t>(i)];
            const auto bytes = encodeEv(e);
            p.send(th, rc::Key(e.playerId), bytes, rc::AckToken{static_cast<rc::Token>(i + 1)});
            ++sent;
        }
    }
    /// Plain producer: every record acked (acks=all).
    void flush() {
        p.flush();
        EXPECT_TRUE(pumpUntil({&client}, [&] { return acked == sent; }, 90s)) << "acked " << acked << " of " << sent;
        EXPECT_EQ(failed, 0u);
    }
    void feed(int from, int to) { send(from, to); flush(); }
};

// ---------------------------------------------------------------------------------------------------------
// The cell.
// ---------------------------------------------------------------------------------------------------------

fm::Timing matrixTiming(TimingK t) {
    switch (t) {
    case TimingK::Steady: return fm::Timing::SteadyPartial;
    case TimingK::LeaderMove: return fm::Timing::OwnershipMove;
    case TimingK::RestartResubscribe: return fm::Timing::OwnershipMove;
    case TimingK::AlterDrop: return fm::Timing::AlterDropFilteredField;
    case TimingK::RcAborted: return fm::Timing::AbortedTxn;
    }
    return fm::Timing::SteadyPartial;
}

/// Wait for every plain subscription to reach its partition's end (or the group to commit it).
void awaitEnds(Cluster& cl, Reader& rd, const Clients& cs, const char* what) {
    std::vector<int64_t> last;
    const bool ok = pumpUntil(cs, [&] {
        if (rd.anyFinished()) return true;
        if (rd.isGroup()) return true;   // a group's progress is the commit (awaitGroupCommitted, below)
        const auto pos = rd.positions();
        last = pos;
        for (std::size_t i = 0; i < pos.size(); ++i)
            if (pos[i] != cl.end(rd.topicId, rd.specs[i].part)) return false;
        return true;
    }, 90s);
    ASSERT_TRUE(ok) << what << ": positions never reached the partition ends";
    for (std::size_t i = 0; i < rd.live.size() && !rd.isGroup(); ++i) {
        EXPECT_FALSE(rd.live[i]->finished()) << what << ": handle " << i << " ended (failReason "
                                             << static_cast<int>(rd.live[i]->failReason()) << ", broker code "
                                             << rd.live[i]->brokerErrorCode() << ")";
        EXPECT_EQ(rd.live[i]->position(), cl.end(rd.topicId, rd.specs[i].part))
            << what << ": handle " << i << " (" << rd.checks[rd.specs[i].check].label << ")";
    }
}

/// Group: commit until the committed offsets are the partition ends (the notes carried the group past every
/// record the filter skipped).
bool awaitGroupCommitted(Cluster& cl, Reader& rd, const Clients& cs, std::chrono::seconds budget = 90s) {
    return pumpUntil(cs, [&] {
        try { rd.gc->commitSyncAndWait(10s); } catch (const std::exception&) { return false; }
        const auto got = cl.committed(rd.group, rd.topicId);
        for (int32_t p = 0; p < kParts; ++p)
            if (got[static_cast<std::size_t>(p)] != cl.end(rd.topicId, p)) return false;
        return true;
    }, budget);
}

uint64_t seedOf(Selection s, FKind f, TimingK t) {
    return 0x5eed0000ull + static_cast<uint64_t>(s) * 100 + static_cast<uint64_t>(f) * 10 + static_cast<uint64_t>(t);
}

struct CellParam {
    Selection s;
    FKind     f;
    TimingK   t;
};
std::string cellName(const ::testing::TestParamInfo<CellParam>& i) {
    return std::string(name(i.param.s)) + "_" + name(i.param.f) + "_" + name(i.param.t);
}

void runCell(const CellParam& cp, rc::Persistence persistence = rc::Persistence::Disk) {
    const fm::Op       op     = cp.f == FKind::Eq ? fm::Op::Eq : fm::Op::EqAndIn;
    const bool         rc_    = cp.t == TimingK::RcAborted;
    const fm::Entity   entity = rc_ ? (persistence == rc::Persistence::Disk ? fm::Entity::DiskRC : fm::Entity::TransientRC)
                                    : (persistence == rc::Persistence::Disk ? fm::Entity::DiskRU : fm::Entity::TransientRU);
    const fm::Outcome  want   = fm::expect(op, entity, matrixTiming(cp.t));
    const std::string  tag    = std::string("fb_") + name(cp.s)[0] + name(cp.f)[0] + name(cp.t)[0] + name(cp.t)[1];

    // The matrix's verdict for this timing, pinned: a change of the table must be a conscious edit here.
    switch (cp.t) {
    case TimingK::Steady: EXPECT_EQ(want, fm::Outcome::Rebuilt); break;
    case TimingK::LeaderMove:
    case TimingK::RestartResubscribe: EXPECT_EQ(want, fm::Outcome::ResumeExact); break;
    case TimingK::AlterDrop:
        EXPECT_EQ(want, cp.f == FKind::Eq ? fm::Outcome::Delivered : fm::Outcome::AbsentExcluded);
        break;
    case TimingK::RcAborted: EXPECT_EQ(want, fm::Outcome::NeverDelivered); break;
    }

    Cuts cuts;
    if (cp.t == TimingK::AlterDrop) { cuts.v2From = 700; cuts.v3From = 1350; }
    if (cp.t == TimingK::RcAborted) cuts.abortedFrom = 1000;
    const std::vector<Ev> model = makeModel(seedOf(cp.s, cp.f, cp.t), cuts);

    // Sanity: the cell actually filters (a selection that is empty would pass vacuously).
    {
        Reader probe(cp.s, op, model, "probe");
        std::size_t total = 0, nonEmpty = 0;
        for (const Check& c : probe.checks) { total += c.expected(kRecords).size(); nonEmpty += !c.expected(kRecords).empty(); }
        ASSERT_GT(total, 20u);
        ASSERT_GT(nonEmpty, 0u);
    }

    Cluster cl = cp.t == TimingK::LeaderMove ? Cluster::three(tag) : Cluster::solo(tag);
    const rc::TopicSpec topic = cl.definePlayerEvent(persistence);
    int seed = 0;   // the broker the client is seeded on (ThreeNodes)
    int32_t crashed = -1;
    if (cl.trio) {
        std::vector<int32_t> leaders;
        ASSERT_TRUE(awaitLeaders(*cl.trio, topic.topicId, kParts, leaders, 30s));
        // Crash the leader of partition 0 unless it is the controller / group coordinator; seed on the controller.
        crashed = leaders[0] != cl.ctl ? leaders[0] : (leaders[1] != cl.ctl ? leaders[1] : leaders[2]);
        ASSERT_NE(crashed, cl.ctl) << "every partition is led by the controller; the coordinator must survive";
        seed    = cl.ctl != crashed ? cl.ctl : cl.trio->runningNodes().front() == crashed ? cl.trio->runningNodes().back() : cl.trio->runningNodes().front();
    }

    Reader rd(cp.s, op, model, std::string("g_") + tag);
    rd.topicId = topic.topicId;
    rd.iso     = rc_ ? rc::IsolationLevel::ReadCommitted : rc::IsolationLevel::ReadUncommitted;

    std::unique_ptr<rc::Client> producerClient = cl.open({topic}, seed);
    std::unique_ptr<rc::Client> readerClient;   // Group: its own client (one group consumer per client)
    rd.th = producerClient->topic(kTopic);
    ASSERT_TRUE(rd.th.topicId >= 0);

    auto clients = [&]() -> Clients {
        Clients v{producerClient.get()};
        if (readerClient) v.push_back(readerClient.get());
        return v;
    };
    // Starts the reader (plain subscriptions or the group).
    auto startReader = [&](const std::vector<int64_t>* resume) {
        if (rd.isGroup()) {
            readerClient = std::make_unique<rc::Client>(cl.cfg({topic}, seed));
            rd.joinGroup(*readerClient);
            readerClient->connectAndWait(30s);
        } else {
            rd.subscribe(*producerClient, resume);
        }
    };
    auto finish = [&](int upTo, const char* what) {
        const Clients cs = clients();
        ASSERT_TRUE(pumpUntil(cs, [&] { return rd.anyFinished() || rd.covers(upTo); })) << what << ": the expected set never arrived";
        ASSERT_FALSE(rd.anyFinished()) << what << ": a subscription ended on its own";
        awaitEnds(cl, rd, cs, what);
        if (rd.isGroup()) EXPECT_TRUE(awaitGroupCommitted(cl, rd, cs)) << what << ": the group's commit never reached the partition ends";
        pumpFor(cs, 300ms);   // anything stray (a duplicate, a late foreign record) shows up now
        rd.verifyExact(upTo);
    };

    switch (cp.t) {
    case TimingK::Steady: {
        Feeder f(*producerClient, rd.th, model);
        f.feed(0, 1000);
        startReader(nullptr);
        f.feed(1000, kRecords);
        finish(kRecords, "steady");
        break;
    }
    case TimingK::LeaderMove: {
        ASSERT_GT(crashed, 0);
        Feeder f(*producerClient, rd.th, model);
        f.feed(0, 1000);
        startReader(nullptr);
        ASSERT_TRUE(pumpUntil(clients(), [&] { return rd.covers(1000) || rd.anyFinished(); })) << "the first half never arrived";
        cl.trio->crash(crashed);
        std::vector<int32_t> leaders;
        ASSERT_TRUE(awaitLeaders(*cl.trio, topic.topicId, kParts, leaders, 60s)) << "no leaders after the crash";
        f.feed(1000, kRecords);
        finish(kRecords, "leader move");
        break;
    }
    case TimingK::RestartResubscribe: {
        {
            Feeder f(*producerClient, rd.th, model);
            f.feed(0, 1000);
            startReader(nullptr);
            const Clients cs = clients();
            ASSERT_TRUE(pumpUntil(cs, [&] { return rd.covers(1000) || rd.anyFinished(); })) << "the first half never arrived";
            ASSERT_FALSE(rd.anyFinished());
            awaitEnds(cl, rd, cs, "before the restart");
            if (rd.isGroup()) ASSERT_TRUE(awaitGroupCommitted(cl, rd, cs)) << "the group never committed before the restart";
        }
        const std::vector<int64_t> resume = rd.isGroup() ? std::vector<int64_t>{} : rd.positions();
        rd.stop(clients());
        // The restart ends every session: the application builds a new client and subscribes again from position().
        readerClient.reset();
        producerClient.reset();
        cl.restart();
        producerClient = cl.open({topic}, seed);
        rd.th = producerClient->topic(kTopic);
        startReader(rd.isGroup() ? nullptr : &resume);
        Feeder f(*producerClient, rd.th, model);
        f.feed(1000, kRecords);
        finish(kRecords, "restart");
        break;
    }
    case TimingK::AlterDrop: {
        Feeder f(*producerClient, rd.th, model);
        f.feed(0, cuts.v2From);
        startReader(nullptr);
        cl.alter(kTopic, addGuildOps());
        f.feed(cuts.v2From, cuts.v3From);
        cl.alter(kTopic, dropChannelOps());
        f.feed(cuts.v3From, kRecords);
        // The model: a v3 record has no channel, so a channel term excludes it (AbsentExcluded); a worldId term
        // still matches it (Delivered).  The expectation table above pins the same verdict.
        finish(kRecords, "alter drop");
        break;
    }
    case TimingK::RcAborted: {
        Feeder f(*producerClient, rd.th, model, "fb-txn-" + tag);
        f.p.initTransactionsAndWait(60s);
        startReader(nullptr);
        f.p.beginTransaction();
        f.send(0, 1000);
        f.p.commitTransactionAndWait(60s);
        f.p.beginTransaction();
        f.send(1000, kRecords);
        f.p.abortTransactionAndWait(60s);
        finish(kRecords, "read_committed");   // the model marks the aborted half (seq >= 1000) invisible
        break;
    }
    }
    for (rc::Client* c : clients()) c->close();
}

class FilterBrokerCell : public ::testing::TestWithParam<CellParam> {};

TEST_P(FilterBrokerCell, Cell) { runCell(GetParam()); }

std::vector<CellParam> allCells() {
    std::vector<CellParam> v;
    for (Selection s : {Selection::Key, Selection::Partition, Selection::AllPartitions, Selection::Group})
        for (FKind f : {FKind::Eq, FKind::EqAndIn})
            for (TimingK t : {TimingK::Steady, TimingK::LeaderMove, TimingK::RestartResubscribe, TimingK::AlterDrop, TimingK::RcAborted})
                v.push_back({s, f, t});
    return v;
}
INSTANTIATE_TEST_SUITE_P(FilterBrokerRegression, FilterBrokerCell, ::testing::ValuesIn(allCells()), cellName);


// ---------------------------------------------------------------------------------------------------------
// Refusal rows and the controls.  Each refused subscription ends RejectedByBroker (InvalidRequest, 39) and the
// broker's reason reaches onError.
// ---------------------------------------------------------------------------------------------------------

struct Errors {
    std::vector<std::string> v;
    [[nodiscard]] bool has(std::string_view needle) const {
        for (const auto& e : v)
            if (e.find(needle) != std::string::npos) return true;
        return false;
    }
    [[nodiscard]] std::string all() const {
        std::string s;
        for (const auto& e : v) s += "[" + e + "] ";
        return s;
    }
};

std::unique_ptr<rc::Client> openWithErrors(Cluster& cl, const rc::TopicSpec& t, Errors& errs) {
    return cl.open({t}, 0, [&errs](const std::string& m) { errs.v.push_back(m); });
}

/// Subscribe partition `part` with `filter` and pump until it is bound or finished.
std::unique_ptr<rc::Subscription> subscribeAndSettle(rc::Client& c, rc::TopicHandle th, int32_t part,
                                                     const rune::codec::FilterBytes& filter, rc::RecordHandler handler = [](const rc::Record&) {}) {
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    o.filter(filter);
    auto sub = std::make_unique<rc::Subscription>(c.subscribe(th, part, o, std::move(handler)));
    const bool settled = pumpUntil({&c}, [&] { return sub->finished() || sub->subscriptionId() != 0; }, 30s);
    EXPECT_TRUE(settled) << "the Subscribe was never answered";
    return sub;
}

void expectRefused(rc::Subscription& sub, rc::Client& c, const Errors& errs, std::string_view detail) {
    pumpUntil({&c}, [&] { return sub.finished() && errs.has(detail); }, 30s);
    EXPECT_TRUE(sub.finished());
    EXPECT_EQ(sub.failReason(), rc::SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), kInvalidRequest);
    EXPECT_TRUE(errs.has(detail)) << "onError never carried '" << detail << "': " << errs.all();
}
void expectAdmitted(rc::Subscription& sub, const char* what) {
    EXPECT_FALSE(sub.finished()) << what << ": ended (failReason " << static_cast<int>(sub.failReason()) << ")";
    EXPECT_NE(sub.subscriptionId(), 0) << what << ": never bound";
}

TEST(FilterBrokerRegression, UntypedTopicRefusedRegression) {
    // E1: a filter on a topic defined without a descriptor.
    Cluster cl = Cluster::solo("fb_e1");
    const rc::TopicSpec t = cl.define("untyped_events", 1, {}, "");
    Errors errs;
    auto c = openWithErrors(cl, t, errs);
    auto sub = subscribeAndSettle(*c, c->topic("untyped_events"), 0, E::Filter{.worldId = kWorld}.encode());
    expectRefused(*sub, *c, errs, "filter needs a typed topic");
}

TEST(FilterBrokerRegression, SchemaMismatchRefusedRegression) {
    // E3-E6: the client's descriptor disagrees with the topic's.  worldId retyped UInt64 (type mismatch), name
    // (a string on the broker) and pos (a float64 on the broker) retyped as integers, and a tag the topic never had.
    using namespace rune::codec;
    const Descriptor edited = DescriptorBuilder("player_event")
        .addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, true, {})
        .addTagged("worldId", 1, PrimType::UInt64, 1, 0xFFFF, false, {})
        .addTagged("channel", 2, PrimType::Int32, 1, 0xFFFF, false, {})
        .addFixedStrTagged("zone", 3, 8, 1, 0xFFFF, false)
        .addTagged("kind", 4, PrimType::Enum, 1, 0xFFFF, false, {1, 2, 3})
        .addTagged("alive", 5, PrimType::Bool, 1, 0xFFFF, false, {})
        .addTagged("hp", 6, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("name", 7, PrimType::UInt64, 1, 0xFFFF, false, {})
        .addTagged("pos", 8, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("ghost", 20, PrimType::UInt32, 1, 0xFFFF, false, {})
        .build();
    Cluster cl = Cluster::solo("fb_e36");
    const rc::TopicSpec t = cl.definePlayerEvent();
    Errors errs;
    auto c = openWithErrors(cl, t, errs);
    const rc::TopicHandle th = c->topic(kTopic);
    struct Row { const char* field; const char* detail; };
    for (const Row& r : {Row{"ghost", "(tag 20): unknown tag"}, Row{"worldId", "(tag 1): type mismatch"},
                         Row{"name", "(tag 7): variable-length field"}, Row{"pos", "(tag 8): float field"}}) {
        auto bytes = FilterBuilder::on(edited).field(r.field).eq(std::int64_t{7}).build();
        ASSERT_TRUE(bytes.has_value()) << r.field;
        auto sub = subscribeAndSettle(*c, th, 0, *bytes);
        expectRefused(*sub, *c, errs, r.detail);
    }
    // The session is unharmed: a filter that matches the topic is admitted.
    auto ok = subscribeAndSettle(*c, th, 0, E::Filter{.worldId = kWorld}.encode());
    expectAdmitted(*ok, "a valid filter after the refusals");
}

/// The seeded model the checks below feed to the topic and read back.
struct Stream {
    std::vector<Ev> model = makeModel(0xfeed, Cuts{});
};

TEST(FilterBrokerRegression, SubscriptionCapRefusedRegression) {
    // E8: RUNE_FILTER_MAX_SUBS_PER_PARTITION=4.  Four filtered subscriptions fit on partition 0, the fifth is
    // refused with the knob named; the four keep delivering; capacity returns when one leaves.
    Cluster cl = Cluster::solo("fb_e8", [](rune::node::NodeConfig& c) { c.filterMaxSubsPerPartition = 4; });
    const rc::TopicSpec t = cl.definePlayerEvent();
    Errors errs;
    auto c = openWithErrors(cl, t, errs);
    const rc::TopicHandle th = c->topic(kTopic);
    Stream st;
    std::deque<Check> checks;
    auto open = [&](uint32_t world) {
        Check chk;
        chk.label = "world " + std::to_string(world);
        chk.pred  = Pred{fm::Op::Eq, std::nullopt, world};
        chk.parts = {0};
        chk.model = &st.model;
        checks.push_back(std::move(chk));
        Check* cp = &checks.back();
        return subscribeAndSettle(*c, th, 0, filterFor(cp->pred), [cp](const rc::Record& r) { cp->onRecord(r.partition, r.value); });
    };
    std::vector<std::unique_ptr<rc::Subscription>> subs;
    for (uint32_t w = 1; w <= 4; ++w) {
        subs.push_back(open(w));
        expectAdmitted(*subs.back(), "within the cap");
    }
    auto fifth = open(5);
    expectRefused(*fifth, *c, errs, "filter capacity: 4 filtered subscriptions on topic");
    EXPECT_TRUE(errs.has("RUNE_FILTER_MAX_SUBS_PER_PARTITION")) << errs.all();
    for (std::size_t i = 0; i < subs.size(); ++i) expectAdmitted(*subs[i], "an admitted subscription after the refusal");

    // Capacity returns: the first one leaves, a sixth is admitted.
    subs[0]->unsubscribe();
    ASSERT_TRUE(pumpUntil({c.get()}, [&] { return subs[0]->finished(); }, 30s));
    auto sixth = open(6);
    expectAdmitted(*sixth, "after capacity returned");

    Feeder f(*c, th, st.model);
    f.feed(0, kRecords);
    const Clients cs{c.get()};
    ASSERT_TRUE(pumpUntil(cs, [&] {
        for (std::size_t i = 1; i < checks.size(); ++i)
            if (i != 4 && !checks[i].covers(kRecords)) return false;   // checks[4] is the refused world 5
        return true;
    }));
    pumpFor(cs, 300ms);
    for (std::size_t i = 1; i < checks.size(); ++i)
        if (i != 4) checks[i].verifyExact(kRecords);
    EXPECT_TRUE(checks[4].seen.empty()) << "the refused subscription delivered";
}

TEST(FilterBrokerRegression, IndexValueCapRefusedRegression) {
    // E9: RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION=4.  Four distinct worlds fit, a fifth value is refused with
    // the knob named, and another subscription reusing an admitted value is admitted.
    Cluster cl = Cluster::solo("fb_e9", [](rune::node::NodeConfig& c) { c.filterMaxIndexValuesPerPartition = 4; });
    const rc::TopicSpec t = cl.definePlayerEvent();
    Errors errs;
    auto c = openWithErrors(cl, t, errs);
    const rc::TopicHandle th = c->topic(kTopic);
    Stream st;
    std::deque<Check> checks;
    auto open = [&](uint32_t world) {
        Check chk;
        chk.label = "world " + std::to_string(world);
        chk.pred  = Pred{fm::Op::Eq, std::nullopt, world};
        chk.parts = {0};
        chk.model = &st.model;
        checks.push_back(std::move(chk));
        Check* cp = &checks.back();
        return subscribeAndSettle(*c, th, 0, filterFor(cp->pred), [cp](const rc::Record& r) { cp->onRecord(r.partition, r.value); });
    };
    std::vector<std::unique_ptr<rc::Subscription>> subs;
    for (uint32_t w = 0; w < 4; ++w) {
        subs.push_back(open(w));
        expectAdmitted(*subs.back(), "within the cap");
    }
    auto fifth = open(4);
    expectRefused(*fifth, *c, errs, "filter capacity: 4 index values on topic");
    EXPECT_TRUE(errs.has("RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION")) << errs.all();
    auto again = open(0);   // an admitted value is shared, not a new index value
    expectAdmitted(*again, "an admitted value reused");

    Feeder f(*c, th, st.model);
    f.feed(0, kRecords);
    const Clients cs{c.get()};
    ASSERT_TRUE(pumpUntil(cs, [&] {
        for (std::size_t i = 0; i < checks.size(); ++i)
            if (i != 4 && !checks[i].covers(kRecords)) return false;
        return true;
    }));
    pumpFor(cs, 300ms);
    for (std::size_t i = 0; i < checks.size(); ++i)
        if (i != 4) checks[i].verifyExact(kRecords);
}

TEST(FilterBrokerRegression, GroupFilterMismatchRefusedRegression) {
    // E10: the first member fixes the group's filter; a second client with another filter is refused at JoinGroup
    // (FilterMismatch), the first keeps consuming.
    Cluster cl = Cluster::solo("fb_e10");
    const rc::TopicSpec t = cl.definePlayerEvent();
    Stream st;
    Errors errsB;
    auto a = cl.open({t}, 0, {});
    Feeder f(*a, a->topic(kTopic), st.model);
    f.feed(0, 1000);

    Reader first(Selection::Group, fm::Op::Eq, st.model, "g_e10");
    first.topicId = t.topicId;
    auto ca = std::make_unique<rc::Client>(cl.cfg({t}));
    first.joinGroup(*ca);
    ca->connectAndWait(30s);

    // The first member is in (it consumes the first half) before B arrives, so it is the one that fixed the filter.
    ASSERT_TRUE(pumpUntil({a.get(), ca.get()}, [&] { return first.covers(1000); })) << "the first member never consumed";

    // B: the same group, a different world.
    Reader second(Selection::Group, fm::Op::Eq, st.model, "g_e10");
    second.checks[0].pred.world = kWorld + 1;
    auto cb = std::make_unique<rc::Client>(cl.cfg({t}));
    cb->onError([&errsB](const std::string& m) { errsB.v.push_back(m); });
    second.joinGroup(*cb);
    cb->connectAndWait(30s);

    const Clients cs{a.get(), ca.get(), cb.get()};
    ASSERT_TRUE(pumpUntil(cs, [&] { return second.gc->finished(); })) << "the second member was never refused";
    EXPECT_EQ(second.gc->failReason(), rc::GroupFailReason::FilterMismatch);
    EXPECT_TRUE(errsB.has("group filter mismatch")) << errsB.all();
    EXPECT_TRUE(second.checks[0].seen.empty()) << "the refused member consumed";

    // The first member keeps consuming: the first half, then the second half produced after the refusal.
    ASSERT_TRUE(pumpUntil(cs, [&] { return first.covers(1000); })) << "the first member stopped consuming";
    f.feed(1000, kRecords);
    ASSERT_TRUE(pumpUntil(cs, [&] { return first.covers(kRecords) || first.anyFinished(); })) << "the first member stopped consuming";
    EXPECT_FALSE(first.anyFinished());
    pumpFor(cs, 300ms);
    first.verifyExact(kRecords);
}

TEST(FilterBrokerRegression, LegacyModeRefusedRegression) {
    // E12: the legacy worker mode does not serve filters; it says so, it does not deliver everything.
    Cluster cl = Cluster::solo("fb_e12", {}, rune::node::test::OneBroker::Runtime::Legacy);
    const rc::TopicSpec t = cl.definePlayerEvent();
    Errors errs;
    auto c = openWithErrors(cl, t, errs);
    auto sub = subscribeAndSettle(*c, c->topic(kTopic), 0, E::Filter{.worldId = kWorld}.encode());
    expectRefused(*sub, *c, errs, "filter needs RUNE_WORKER_MODE=shard");
}

TEST(FilterBrokerRegression, UnfilteredNeighbourSeesEveryRecordRegression) {
    // Control: a filtered subscription beside an unfiltered one on the same partition changes nothing for the
    // neighbour - it still sees every record, byte-identical fields, once.
    Cluster cl = Cluster::solo("fb_ctl");
    const rc::TopicSpec t = cl.definePlayerEvent();
    Stream st;
    auto c = cl.open({t});
    const rc::TopicHandle th = c->topic(kTopic);
    std::set<int32_t> p0{0};

    Check filtered;
    filtered.label = "filtered";
    filtered.pred  = Pred{fm::Op::Eq, std::nullopt, kWorld};
    filtered.parts = p0;
    filtered.model = &st.model;
    // The neighbour wants everything: every world.  Its predicate is checked per record below, not by Pred.
    std::map<int64_t, int> all;
    int mangled = 0;
    rc::SubscribeOptions plain;
    plain.from = rc::From::earliest();
    rc::Subscription neighbour = c->subscribe(th, 0, plain, [&](const rc::Record& r) {
        const Decoded d = decodeRecord(r.value);
        if (!d.ok) { ++mangled; return; }
        const Ev& e = st.model[static_cast<std::size_t>(d.seq)];
        if (d.playerId != e.playerId || d.worldId != e.worldId || d.channel != e.channel) ++mangled;
        ++all[d.seq];
    });
    rc::SubscribeOptions fo;
    fo.from = rc::From::earliest();
    fo.filter(filterFor(filtered.pred));
    rc::Subscription filteredSub = c->subscribe(th, 0, fo, [&](const rc::Record& r) { filtered.onRecord(r.partition, r.value); });

    Feeder f(*c, th, st.model);
    f.feed(0, kRecords);
    const Clients cs{c.get()};
    std::size_t want = 0;
    for (const Ev& e : st.model) want += e.part == 0;
    ASSERT_GT(want, 100u);
    ASSERT_TRUE(pumpUntil(cs, [&] { return all.size() == want && filtered.covers(kRecords); }));
    pumpFor(cs, 300ms);
    EXPECT_EQ(all.size(), want);
    for (const auto& [seq, n] : all) {
        EXPECT_EQ(n, 1) << "seq " << seq;
        EXPECT_EQ(st.model[static_cast<std::size_t>(seq)].part, 0);
    }
    EXPECT_EQ(mangled, 0);
    filtered.verifyExact(kRecords);
    const int64_t end = cl.end(t.topicId, 0);
    ASSERT_TRUE(pumpUntil(cs, [&] { return neighbour.position() == end && filteredSub.position() == end; }))
        << "positions " << neighbour.position() << " / " << filteredSub.position() << " vs end " << end;
    // The filter's saving is visible against the neighbour.
    EXPECT_LT(filteredSub.batchBytes(), neighbour.batchBytes());
}

TEST(FilterBrokerRegression, GroupCommitAdvancesOnAQuietFilterRegression) {
    // A group filter that matches nothing: the position notes alone carry the group's commit to every
    // partition's end.
    Cluster cl = Cluster::solo("fb_quiet");
    const rc::TopicSpec t = cl.definePlayerEvent();
    Stream st;
    auto pc = cl.open({t});
    Feeder f(*pc, pc->topic(kTopic), st.model);
    f.feed(0, kRecords);

    Reader rd(Selection::Group, fm::Op::Eq, st.model, "g_quiet");
    rd.topicId = t.topicId;
    rd.checks[0].pred.world = 99;
    auto gcClient = std::make_unique<rc::Client>(cl.cfg({t}));
    rd.joinGroup(*gcClient);
    gcClient->connectAndWait(30s);
    const Clients cs{pc.get(), gcClient.get()};
    ASSERT_TRUE(awaitGroupCommitted(cl, rd, cs)) << "the committed offsets never reached the partition ends";
    const auto got = cl.committed(rd.group, t.topicId);
    for (int32_t p = 0; p < kParts; ++p) {
        EXPECT_GT(cl.end(t.topicId, p), 0);
        EXPECT_EQ(got[static_cast<std::size_t>(p)], cl.end(t.topicId, p)) << "partition " << p;
    }
    EXPECT_TRUE(rd.checks[0].seen.empty());
    EXPECT_TRUE(rd.checks[0].bad.empty());
}

TEST(FilterBrokerRegression, TransientSteadyRegression) {
    // The transient replica serves the same filtered stream (AllPartitions x Eq, steady).
    runCell({Selection::AllPartitions, FKind::Eq, TimingK::Steady}, rc::Persistence::Transient);
}

}  // namespace
