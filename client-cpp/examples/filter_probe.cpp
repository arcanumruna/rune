// examples/filter_probe.cpp — Phase 2c Task 17: broker-side subscription filters on 3 authenticated brokers
// (docker/filter-e2e.sh).
//
// One binary, RUNE_MODE selects the leg.  Every leg is single-threaded: the Client(s) run in RunnerMode::Invoker, so
// every callback runs on this thread inside Client::doWork() and the probe needs no lock, no atomic, no hand-off.
// The records are deterministic: RUNE_SEED + FILTER_E2E_N define the model, so `consume` rebuilds exactly what
// `produce` wrote and checks each subscription's delivered set against it.
//
//   setup      defines player_event (8 partitions, rf 3, key playerId, validate=false) and untyped_t (untyped);
//              prints FILTER_TOPIC_ID / FILTER_UNTYPED_ID and FILTER-PROBE-SETUP: OK.
//   produce    FILTER_E2E_N records (default 40000) at FILTER_E2E_RATE per second: worldId uniform in [0, 200),
//              channel in [1, 4], 1% empty-value records (the Producer cannot send a null value; the filter excludes both
//              the same way: shorter than the 4-byte header), preceded by one committed and one aborted transaction of
//              worldId 7 (a Producer with transactionalId).  hp carries the send time (CLOCK_REALTIME ns), pos the
//              sequence number.  Writes <FILTER_E2E_SHARED>/ends (every partition's end offset, from the acks) when done,
//              and <FILTER_E2E_SHARED>/progress (acked records) while it runs.  FILTER_E2E_DURATION_S > 0: an unbounded
//              stream for that many seconds instead (the scenario's load; no transactions, no ends file).
//   consume    five subscriptions, each checked for its EXACT set against the model, no duplicate, per-partition order,
//              and position() at each partition's end:  (a) AllPartitions worldId == 7, read_committed; (b) AllPartitions
//              worldId == 8 and channel in {1, 2}; (c) Key(k) + playerId == k; (d) group worlds-9, two members,
//              worldId == 9, read_committed; (e) one unfiltered Partition(0) control.  A SessionLost re-subscribes from
//              position().  Prints FILTER-CONSUME: OK sets=5.
//   errors     the refusals: E1 (untyped topic), E3-E6 (a stale descriptor), E8 (the subscription cap), E10 (a second group
//              member with another filter).  Each must surface its detail.  Prints FILTER-ERRORS: OK.
//   alter      ALTER ... ADD guild (v2), 2000 v2 records, DROP channel (v3), 2000 v3 records: (b) must receive no v3
//              record and its v1/v2 matches are exact.  Prints FILTER-ALTER: OK.
//   world      FILTER_E2E_WORLDS_PER_PROC world servers (default 25) from FILTER_E2E_WORLD_BASE: each a SubscriptionSet
//              (AllPartitions + worldId == w), or unfiltered with client-side dropping (FILTER_E2E_UNFILTERED=1), for
//              FILTER_E2E_SECONDS; prints per world
//              WORLD w=<w> records=<n> bytes=<batchBytes> ideal_bytes=<n * one-record batch> p50_us=<..> p99_us=<..>.
//   selfcheck  no broker: the model, the filters and the record codec against each other.
//
// Environment: RUNE_TARGET, RUNE_EGRESS_HOST + RUNE_EGRESS_PORT (client i listens on port + i), RUNE_DATA_DIR,
// RUNE_USER / RUNE_PASSWORD_FILE, RUNE_CLIENT_TOPIC_TABLE ("player_event:<id>:8,untyped_t:<id>:1"), RUNE_SEED,
// RUNE_TIMEOUT_MS, FILTER_E2E_SHARED (a writable directory shared between legs).

#include "ExampleAuth.h"
#include "filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/FilterBuilder.h>
#include <rune/codec/Flyweight.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
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
#include <vector>

using namespace rune::client;
namespace fly = rune::codec::fly;
using E     = rune::gen::PlayerEvent;
using Clock = std::chrono::steady_clock;

namespace {

constexpr int32_t  kParts       = 8;
constexpr int      kTxnRecs     = 20;       // per transaction
constexpr uint64_t kPlayers     = 64;       // playerId in [0, 64)
constexpr uint64_t kKeyPlayer   = 5;        // subscription (c)
constexpr uint32_t kWorlds      = 200;
constexpr const char* kTopicName   = "player_event";
constexpr const char* kUntypedName = "untyped_t";

// ---- environment ---------------------------------------------------------------------------------------------------

std::string envStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : def;
}
long envInt(const char* name, long def) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::strtol(v, nullptr, 10) : def;
}

std::vector<TopicSpec> parseTopics(const std::string& s) {
    std::vector<TopicSpec> out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = comma == std::string::npos ? s.size() + 1 : comma + 1;
        const auto c1 = tok.find(':');
        const auto c2 = c1 == std::string::npos ? c1 : tok.find(':', c1 + 1);
        if (c2 == std::string::npos) continue;
        TopicSpec ts;
        ts.name           = tok.substr(0, c1);
        ts.topicId        = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}

int64_t realtimeNs() noexcept {
    timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
}

void fail(const char* tag, const std::string& msg) { std::fprintf(stderr, "%s: FAIL %s\n", tag, msg.c_str()); }

// ---- the schema over its three versions (v1; v2 ADD guild; v3 DROP channel) ---------------------------------------

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

// ---- the model -----------------------------------------------------------------------------------------------------

enum class Cls : uint8_t { Plain, Tomb, TxnCommit, TxnAbort };

struct Ev {
    int64_t  seq      = 0;
    uint64_t playerId = 0;
    uint32_t worldId  = 0;
    int32_t  channel  = 0;
    uint32_t guild    = 0;
    uint16_t version  = 1;
    Cls      cls      = Cls::Plain;
    int32_t  part     = 0;
};

/// seq [0, kTxnRecs): the committed transaction, [kTxnRecs, 2 * kTxnRecs): the aborted one (both worldId 7, never
/// player kKeyPlayer so subscription (c) is not affected by an aborted record), then `n` plain records (1% empty).
std::vector<Ev> makeModel(uint64_t seed, int64_t n) {
    std::mt19937_64 rng(seed);
    std::vector<Ev> m;
    m.reserve(static_cast<std::size_t>(2 * kTxnRecs + n));
    for (int64_t i = 0; i < 2 * kTxnRecs + n; ++i) {
        Ev e;
        e.seq      = i;
        e.playerId = rng() % kPlayers;
        e.worldId  = static_cast<uint32_t>(rng() % kWorlds);
        e.channel  = static_cast<int32_t>(1 + rng() % 4);
        const uint64_t tombRoll = rng() % 100;
        if (i < kTxnRecs) { e.cls = Cls::TxnCommit; e.worldId = 7; }
        else if (i < 2 * kTxnRecs) { e.cls = Cls::TxnAbort; e.worldId = 7; }
        else e.cls = tombRoll == 0 ? Cls::Tomb : Cls::Plain;
        if (e.cls == Cls::TxnCommit || e.cls == Cls::TxnAbort) { if (e.playerId == kKeyPlayer) e.playerId = kKeyPlayer + 1; }
        e.part = Key(e.playerId).partition(kParts);
        m.push_back(e);
    }
    return m;
}

/// v2 / v3 records of the alter leg: every 20th is world 8 (the filtered one), the rest uniform.
std::vector<Ev> makeAlterModel(uint64_t seed, int64_t firstSeq, int count, uint16_t version) {
    std::mt19937_64 rng(seed ^ (0x9e3779b97f4a7c15ull * version));
    std::vector<Ev> m;
    for (int i = 0; i < count; ++i) {
        Ev e;
        e.seq      = firstSeq + i;
        e.playerId = rng() % kPlayers;
        e.worldId  = i % 20 == 0 ? 8u : static_cast<uint32_t>(rng() % kWorlds);
        e.channel  = static_cast<int32_t>(1 + rng() % 4);
        e.guild    = static_cast<uint32_t>(rng() % 5);
        e.version  = version;
        e.part     = Key(e.playerId).partition(kParts);
        m.push_back(e);
    }
    return m;
}

// ---- the record codec (any of the three versions) --------------------------------------------------------------------

/// [blockLength u16][version u16][fixed block at the materialized offsets of `version`]; fields absent in that
/// version are skipped.  hp = sendNs, pos = seq.  No variable-length section.
std::vector<uint8_t> encodeVersioned(const Ev& e, int64_t sendNs) {
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
    if (auto* p = at("hp")) fly::store<int64_t>(p, sendNs);
    if (auto* p = at("pos")) fly::store<double>(p, static_cast<double>(e.seq));
    if (auto* p = at("guild")) fly::store<uint32_t>(p, e.guild);
    return out;
}

/// A v1 record through the generated encoder (the same bytes path every typed client writes).
std::size_t encodeV1(const Ev& e, int64_t sendNs, uint8_t* buf) {
    E ev;
    ev.playerId = e.playerId;
    ev.worldId  = e.worldId;
    ev.channel  = e.channel;
    ev.zone     = {'e', 'u'};
    ev.kind     = static_cast<int32_t>(1 + e.seq % 3);
    ev.alive    = (e.seq & 1) != 0;
    ev.hp       = sendNs;
    ev.pos      = static_cast<double>(e.seq);
    return ev.encode(buf);
}

struct Decoded {
    bool     ok         = false;
    uint16_t version    = 0;
    int64_t  seq        = -1;
    int64_t  sendNs     = 0;
    uint64_t playerId   = 0;
    uint32_t worldId    = 0;
    bool     hasChannel = false;
    int32_t  channel    = 0;
};
Decoded decodeAny(std::span<const uint8_t> v) {
    Decoded d;
    if (v.size() < 4) return d;
    const auto bl = fly::load<uint16_t>(v.data());
    d.version     = fly::load<uint16_t>(v.data() + 2);
    if (d.version < 1 || d.version > 3 || v.size() < 4u + bl) return d;
    const rune::codec::Descriptor& m = schema().materialized[d.version];
    if (bl != m.blockLength) return d;
    const uint8_t* blk = v.data() + 4;
    d.playerId = fly::load<uint64_t>(blk + m.find("playerId")->offset);
    d.worldId  = fly::load<uint32_t>(blk + m.find("worldId")->offset);
    d.sendNs   = fly::load<int64_t>(blk + m.find("hp")->offset);
    d.seq      = static_cast<int64_t>(fly::load<double>(blk + m.find("pos")->offset));
    if (const auto* f = m.find("channel")) {
        d.hasChannel = true;
        d.channel    = fly::load<int32_t>(blk + f->offset);
    }
    d.ok = true;
    return d;
}

// ---- the five selections' predicates -----------------------------------------------------------------------------------

enum class Pk : uint8_t { World7, World8Channel12, Player, World9, Control0, World8Alter };

const char* pkName(Pk k) {
    switch (k) {
    case Pk::World7: return "a: AllPartitions worldId==7 read_committed";
    case Pk::World8Channel12: return "b: AllPartitions worldId==8 and channel in {1,2}";
    case Pk::Player: return "c: Key(k) playerId==k";
    case Pk::World9: return "d: group worlds-9 worldId==9 read_committed";
    case Pk::Control0: return "e: unfiltered Partition(0)";
    case Pk::World8Alter: return "b': AllPartitions worldId==8 and channel in {1,2} across ALTER";
    }
    return "?";
}

rune::codec::FilterBytes filterOf(Pk k) {
    switch (k) {
    case Pk::World7: return E::Filter{.worldId = 7u}.encode();
    case Pk::World8Channel12:
    case Pk::World8Alter: return E::Filter{.worldId = 8u, .channel = rune::anyOf(1, 2)}.encode();
    case Pk::Player: return E::Filter{.playerId = kKeyPlayer}.encode();
    case Pk::World9: return E::Filter{.worldId = 9u}.encode();
    case Pk::Control0: return {};
    }
    return {};
}

/// Does the model record belong in the selection's set?  (A read_committed selection never sees an aborted record;
/// an empty-value record matches no filter; the control sees everything on its partition.)
bool inSet(Pk k, const Ev& e) {
    if (k == Pk::Control0) return e.part == 0 && e.cls != Cls::Tomb;
    if (e.cls == Cls::Tomb) return false;
    switch (k) {
    case Pk::World7: return e.cls != Cls::TxnAbort && e.worldId == 7;
    case Pk::World8Channel12:
    case Pk::World8Alter: return e.cls == Cls::Plain && e.worldId == 8 && e.version < 3 && (e.channel == 1 || e.channel == 2);
    case Pk::Player: return e.playerId == kKeyPlayer;
    case Pk::World9: return e.cls == Cls::Plain && e.worldId == 9;
    case Pk::Control0: return false;
    }
    return false;
}
/// The same verdict from the DELIVERED bytes alone (a foreign record is a violation).
bool decodedMatches(Pk k, const Decoded& d) {
    switch (k) {
    case Pk::World7: return d.worldId == 7;
    case Pk::World8Channel12:
    case Pk::World8Alter: return d.worldId == 8 && d.hasChannel && (d.channel == 1 || d.channel == 2);
    case Pk::Player: return d.playerId == kKeyPlayer;
    case Pk::World9: return d.worldId == 9;
    case Pk::Control0: return true;
    }
    return false;
}

/// One selection's checker: the exact set, no duplicate (a group: at-least-once, so duplicates are counted, not failed),
/// per-partition order, no foreign record.
struct Sink {
    Pk                          kind;
    std::string                 label;
    std::set<int32_t>           parts;
    const std::vector<Ev>*      model = nullptr;     // seq -> Ev
    int64_t                     seqBase = 0;         // model[seq - seqBase]
    bool                        group = false;
    std::set<int64_t>           expected;
    int64_t                     expectedEmpty = 0;   // control only: the empty-value records on its partition
    std::map<int64_t, int>      seen;
    std::size_t                 covered = 0;
    int64_t                     emptySeen = 0;
    int64_t                     rewinds = 0;         // group only: records at or below the partition's highest seq seen
    int64_t                     records = 0;
    std::map<int32_t, int64_t>  lastSeq;
    std::vector<std::string>    bad;

    Sink(Pk k, std::set<int32_t> p, const std::vector<Ev>* mdl, int64_t base, bool isGroup)
        : kind(k), label(pkName(k)), parts(std::move(p)), model(mdl), seqBase(base), group(isGroup) {
        for (const Ev& e : *model) {
            if (parts.count(e.part) == 0) continue;
            if (inSet(kind, e)) expected.insert(e.seq);
            else if (kind == Pk::Control0 && e.cls == Cls::Tomb) ++expectedEmpty;
        }
    }
    [[nodiscard]] bool complete() const { return covered == expected.size() && (kind != Pk::Control0 || emptySeen >= expectedEmpty); }

    void onRecord(int32_t partition, std::span<const uint8_t> value) {
        ++records;
        if (value.size() < 4) {
            if (kind == Pk::Control0) { ++emptySeen; return; }
            note("an empty-value record reached a filtered subscription");
            return;
        }
        const Decoded d = decodeAny(value);
        const std::int64_t idx = d.seq - seqBase;
        if (!d.ok || idx < 0 || idx >= static_cast<int64_t>(model->size())) { note("undecodable record"); return; }
        const Ev& e = (*model)[static_cast<std::size_t>(idx)];
        if (d.playerId != e.playerId || d.worldId != e.worldId || d.version != e.version || d.hasChannel != (e.version < 3) ||
            (d.hasChannel && d.channel != e.channel))
            note("seq " + std::to_string(d.seq) + " arrived with mangled fields");
        if (!decodedMatches(kind, d))
            note("seq " + std::to_string(d.seq) + " does not match the filter (world " + std::to_string(d.worldId) + ", player " +
                 std::to_string(d.playerId) + ")");
        if (partition != e.part || parts.count(partition) == 0)
            note("seq " + std::to_string(d.seq) + " arrived on partition " + std::to_string(partition));
        if (group) {   // at-least-once: a rewind is allowed, but only as many as there were assignments (checked by the caller)
            auto it = lastSeq.find(partition);
            if (it != lastSeq.end() && d.seq <= it->second) ++rewinds;
            lastSeq[partition] = std::max(d.seq, it == lastSeq.end() ? d.seq : it->second);
        } else {
            auto it = lastSeq.find(partition);
            if (it != lastSeq.end() && d.seq <= it->second) note("seq " + std::to_string(d.seq) + " out of order on partition " + std::to_string(partition));
            lastSeq[partition] = std::max(d.seq, it == lastSeq.end() ? d.seq : it->second);
        }
        if (++seen[d.seq] == 1 && expected.count(d.seq) != 0) ++covered;
    }
    void note(std::string s) { if (bad.size() < 20) bad.push_back(label + ": " + std::move(s)); }

    /// The exact verdict; returns the number of violations and prints the first few.
    int verify(const char* tag) const {
        int violations = 0, shown = 0, dups = 0;
        for (const int64_t s : expected) {
            const auto it = seen.find(s);
            if (it == seen.end()) { ++violations; if (shown++ < 5) fail(tag, label + ": seq " + std::to_string(s) + " never delivered"); }
            else if (it->second != 1) { ++dups; if (!group) { ++violations; if (shown++ < 5) fail(tag, label + ": seq " + std::to_string(s) + " delivered " + std::to_string(it->second) + " times"); } }
        }
        for (const auto& [s, n] : seen) {
            (void)n;
            if (expected.count(s) == 0) { ++violations; if (shown++ < 5) fail(tag, label + ": seq " + std::to_string(s) + " delivered but not expected"); }
        }
        if (kind == Pk::Control0 && emptySeen != expectedEmpty) { ++violations; fail(tag, label + ": " + std::to_string(emptySeen) + " empty-value records, want " + std::to_string(expectedEmpty)); }
        for (const auto& b : bad) { ++violations; if (shown++ < 8) fail(tag, b); }
        std::printf("%s SET %s expected=%zu delivered=%zu duplicates=%d violations=%d\n", tag, label.c_str(), expected.size(), seen.size(), dups, violations);
        return violations;
    }
};

// ---- client plumbing -------------------------------------------------------------------------------------------------

struct Env {
    std::string target, egressHost, dataDir;
    int         egressPort = 40500;
    int         timeoutMs = 20000;
    uint64_t    seed = 1;
    int64_t     n = 40000;
    std::string shared;
    std::vector<TopicSpec> topics;
};
Env readEnv() {
    Env e;
    e.target     = envStr("RUNE_TARGET");
    e.egressHost = envStr("RUNE_EGRESS_HOST");
    e.egressPort = static_cast<int>(envInt("RUNE_EGRESS_PORT", 40500));
    e.dataDir    = envStr("RUNE_DATA_DIR");
    e.timeoutMs  = static_cast<int>(envInt("RUNE_TIMEOUT_MS", 20000));
    e.seed       = static_cast<uint64_t>(envInt("RUNE_SEED", 1));
    e.n          = envInt("FILTER_E2E_N", 40000);
    e.shared     = envStr("FILTER_E2E_SHARED", "/shared");
    e.topics     = parseTopics(envStr("RUNE_CLIENT_TOPIC_TABLE"));
    return e;
}
ClientConfig makeConfig(const Env& e, int idx) {
    ClientConfig cfg;
    cfg.gatewayChannel   = e.target;
    cfg.gatewayStreamId  = static_cast<int32_t>(envInt("RUNE_TARGET_STREAM", 1));
    cfg.egressChannel    = "aeron:udp?endpoint=" + e.egressHost + ":" + std::to_string(e.egressPort + idx);
    cfg.egressStreamId   = static_cast<int32_t>(envInt("RUNE_EGRESS_STREAM", 2));
    cfg.dataDir          = e.dataDir + "/c" + std::to_string(idx);
    cfg.connectTimeoutMs = e.timeoutMs;
    cfg.pingTimeoutMs    = e.timeoutMs;
    cfg.runnerMode       = RunnerMode::Invoker;
    cfg.topics           = e.topics;
    rune::examples::applyEnvCredentials(cfg);
    return cfg;
}

using Clients = std::vector<Client*>;
/// FILTER_E2E_IDLE_US > 0: an idle pump sleeps that long instead of spinning (the scenario runs eight world-server
/// containers next to three brokers on 4 vCPUs: spinning clients would be the load being measured).
int g_idleUs = 0;
void pumpAll(const Clients& cs) {
    int work = 0;
    for (Client* c : cs) work += c->doWork();
    if (work != 0) return;
    if (g_idleUs > 0) std::this_thread::sleep_for(std::chrono::microseconds(g_idleUs));
    else std::this_thread::yield();
}
template <class P>
bool pumpUntil(const Clients& cs, P&& pred, int seconds) {
    for (const auto dl = Clock::now() + std::chrono::seconds(seconds); Clock::now() < dl;) {
        pumpAll(cs);
        if (pred()) return true;
    }
    return false;
}
void pumpFor(const Clients& cs, int ms) {
    for (const auto dl = Clock::now() + std::chrono::milliseconds(ms); Clock::now() < dl;) pumpAll(cs);
}

void writeFileAtomic(const std::string& path, const std::string& text) {
    const std::string tmp = path + ".tmp";
    { std::ofstream f(tmp, std::ios::trunc); f << text; }
    std::rename(tmp.c_str(), path.c_str());
}
std::optional<std::vector<int64_t>> readEnds(const std::string& path) {
    std::ifstream f(path);
    if (!f) return std::nullopt;
    std::vector<int64_t> ends(kParts, -1);
    long p, off;
    while (f >> p >> off) if (p >= 0 && p < kParts) ends[static_cast<std::size_t>(p)] = off;
    for (const int64_t v : ends) if (v < 0) return std::nullopt;
    return ends;
}

// ---- setup -----------------------------------------------------------------------------------------------------------

int runSetup(const Env& env) {
    Env e = env;
    e.topics.clear();
    Client c(makeConfig(e, 0));
    c.connectAndWait();
    TopicOptions to;
    to.replicationFactor = 3;
    const auto blob = rune::codec::serialize(rune::examples::playerEventDescriptor());
    const auto def = c.defineTopicAndWait(kTopicName, kParts, "playerId", blob, /*validate=*/false, Persistence::Disk, to, {},
                                          std::chrono::seconds(60));
    if (def.errorCode != 0) { fail("FILTER-PROBE-SETUP", std::string("define player_event: ") + def.detail); return 1; }
    TopicOptions uo;
    uo.replicationFactor = 3;
    const auto und = c.defineTopicAndWait(kUntypedName, 1, "", std::span<const uint8_t>{}, /*validate=*/false, Persistence::Disk, uo,
                                          {}, std::chrono::seconds(60));
    if (und.errorCode != 0) { fail("FILTER-PROBE-SETUP", std::string("define untyped_t: ") + und.detail); return 1; }
    std::printf("FILTER_TOPIC_ID=%d\nFILTER_UNTYPED_ID=%d\n", def.topicId, und.topicId);
    std::printf("FILTER-PROBE-SETUP: OK\n");
    std::fflush(stdout);
    return 0;
}

// ---- produce ---------------------------------------------------------------------------------------------------------

struct AckState {
    int64_t acked = 0, failed = 0;
    std::array<int64_t, kParts> end{};
    int32_t firstFailure = 0;
};
constexpr Token kTokenBase = 8;
Token tokenOf(int64_t seq, int32_t part) { return static_cast<Token>(((seq << 3) | part) + kTokenBase); }
int32_t partOfToken(Token t) { return static_cast<int32_t>((t - kTokenBase) & 7); }
void onAck(void* ctx, Token token, int64_t offset, OpStatus status, uint16_t) noexcept {
    auto* a = static_cast<AckState*>(ctx);
    ++a->acked;
    if (status != OpStatus::Ok) { ++a->failed; a->firstFailure = static_cast<int32_t>(status); return; }
    const int32_t p = partOfToken(token);
    if (offset >= 0) a->end[static_cast<std::size_t>(p)] = std::max(a->end[static_cast<std::size_t>(p)], offset + 1);
}

ProducerConfig plainProducerConfig(const Env& env) {
    ProducerConfig pc;
    pc.acks                   = Acks::All;
    pc.lingerUs               = static_cast<int32_t>(envInt("FILTER_E2E_LINGER_US", 1000));
    pc.maxInFlight            = 16;
    pc.produceTimeoutMs       = 30'000;
    pc.deliveryTimeoutMs      = 120'000;
    pc.transactionOpTimeoutMs = 45'000;
    (void)env;
    return pc;
}

/// Sends the model's record `e` (a tombstone as an empty value) through `send`.
template <class Send>
void sendRecord(const Ev& e, Send&& send) {
    uint8_t buf[256];
    if (e.cls == Cls::Tomb) { send(std::span<const uint8_t>(buf, 0)); return; }
    const std::size_t n = encodeV1(e, realtimeNs(), buf);
    send(std::span<const uint8_t>(buf, n));
}

int runProduce(const Env& env) {
    const long durationS = envInt("FILTER_E2E_DURATION_S", 0);
    const long rate      = envInt("FILTER_E2E_RATE", 1000);
    const std::string progressFile = envStr("FILTER_E2E_PROGRESS_FILE");
    Client c(makeConfig(env, 0));
    c.connectAndWait();
    const TopicHandle th = c.topic(kTopicName);
    if (!th.valid() || th.partitionCount != kParts) { fail("FILTER-PRODUCE", "player_event is not in RUNE_CLIENT_TOPIC_TABLE"); return 1; }
    const std::vector<Ev> model = makeModel(env.seed, durationS > 0 ? 0 : env.n);

    if (durationS == 0) {   // one committed and one aborted transaction of worldId 7
        ProducerConfig tc = plainProducerConfig(env);
        tc.transactionalId = "filter-txn-1";
        Producer tp = c.producer(tc);
        tp.initTransactionsAndWait();
        for (int round = 0; round < 2; ++round) {
            tp.beginTransaction();
            for (int i = 0; i < kTxnRecs; ++i) {
                const Ev& e = model[static_cast<std::size_t>(round * kTxnRecs + i)];
                sendRecord(e, [&](std::span<const uint8_t> v) { tp.send(th, Key(e.playerId), v); });
            }
            if (round == 0) tp.commitTransactionAndWait();
            else { tp.flushAndWait(); tp.abortTransactionAndWait(); }
        }
    }

    AckState acks;
    Producer p = c.producer(plainProducerConfig(env));
    p.onAck(&onAck, &acks);
    std::mt19937_64 rng(env.seed ^ 0x5eedull);
    const int64_t first = 2 * kTxnRecs;
    const int64_t total = durationS > 0 ? INT64_MAX : env.n;
    const auto start = Clock::now();
    const auto stop  = durationS > 0 ? start + std::chrono::seconds(durationS) : Clock::time_point::max();
    int64_t sent = 0;
    auto lastProgress = Clock::now();
    while (sent < total && Clock::now() < stop) {
        const double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
        const int64_t due = std::min<int64_t>(total, static_cast<int64_t>(elapsed * static_cast<double>(rate)) + 1);
        int burst = 0;
        while (sent < due && burst < 4000) {
            Ev e;
            if (durationS > 0) {   // the scenario's stream: worlds uniform, never an empty record
                e.seq      = first + sent;
                e.playerId = rng() % kPlayers;
                e.worldId  = static_cast<uint32_t>(rng() % kWorlds);
                e.channel  = static_cast<int32_t>(1 + rng() % 4);
                e.part     = Key(e.playerId).partition(kParts);
            } else {
                e = model[static_cast<std::size_t>(first + sent)];
            }
            sendRecord(e, [&](std::span<const uint8_t> v) { p.send(th, Key(e.playerId), v, AckToken{tokenOf(e.seq, e.part)}); });
            ++sent;
            ++burst;
        }
        if (c.doWork() == 0 && g_idleUs > 0) std::this_thread::sleep_for(std::chrono::microseconds(g_idleUs));
        if (acks.failed != 0) { fail("FILTER-PRODUCE", "a record failed, status " + std::to_string(acks.firstFailure)); return 1; }
        if (!progressFile.empty() && Clock::now() - lastProgress > std::chrono::milliseconds(250)) {
            writeFileAtomic(progressFile, std::to_string(acks.acked) + "\n");
            lastProgress = Clock::now();
        }
    }
    p.flush();
    const bool drained = pumpUntil({&c}, [&] { return acks.acked >= sent || acks.failed != 0; }, 300);
    if (!progressFile.empty()) writeFileAtomic(progressFile, std::to_string(acks.acked) + "\n");
    if (!drained || acks.failed != 0 || acks.acked != sent) {
        fail("FILTER-PRODUCE", "acked=" + std::to_string(acks.acked) + " failed=" + std::to_string(acks.failed) + " of " + std::to_string(sent));
        return 1;
    }
    if (durationS == 0) {
        std::string text;
        for (int32_t q = 0; q < kParts; ++q) text += std::to_string(q) + " " + std::to_string(acks.end[static_cast<std::size_t>(q)]) + "\n";
        for (int32_t q = 0; q < kParts; ++q)
            if (acks.end[static_cast<std::size_t>(q)] <= 0) { fail("FILTER-PRODUCE", "partition " + std::to_string(q) + " got no acked record"); return 1; }
        writeFileAtomic(env.shared + "/ends", text);
    }
    std::printf("FILTER-PRODUCE: OK sent=%lld\n", static_cast<long long>(sent));
    std::fflush(stdout);
    return 0;
}

// ---- consume ---------------------------------------------------------------------------------------------------------

/// One partition's subscription of a selection, re-subscribed from position() after a SessionLost.
struct PartHandle {
    Sink*                          sink = nullptr;
    int32_t                        part = 0;
    Subscription*                  cur = nullptr;
    std::unique_ptr<Subscription>  owned;
    std::function<Subscription(From)> reopen;
    int                            resubs = 0;
};

SubscribeOptions optionsFor(Pk k, From from) {
    SubscribeOptions o;
    o.from      = from;
    o.isolation = (k == Pk::World7) ? Isolation::ReadCommitted : Isolation::ReadUncommitted;
    if (k != Pk::Control0) o.filter(filterOf(k));
    return o;
}

/// Finished handles: a SessionLost re-subscribes from position(); anything else ends the leg.
bool supervise(std::vector<PartHandle>& hs, const char* tag, std::string& why) {
    for (PartHandle& h : hs) {
        if (!h.cur->finished()) continue;
        const SubscriptionFailReason r = h.cur->failReason();
        if (r != SubscriptionFailReason::SessionLost) {
            why = h.sink->label + " partition " + std::to_string(h.part) + " ended: failReason " + std::to_string(static_cast<int>(r)) +
                  ", broker code " + std::to_string(h.cur->brokerErrorCode());
            return false;
        }
        const int64_t at = h.cur->position();
        h.owned = std::make_unique<Subscription>(h.reopen(From::offset(at)));
        h.cur   = h.owned.get();
        ++h.resubs;
        std::fprintf(stderr, "%s: %s partition %d lost its session; re-subscribed from %lld\n", tag, h.sink->label.c_str(), h.part,
                     static_cast<long long>(at));
    }
    return true;
}

int runConsume(const Env& env) {
    const int timeoutS = static_cast<int>(envInt("FILTER_E2E_CONSUME_TIMEOUT_S", 580));
    const std::vector<Ev> model = makeModel(env.seed, env.n);
    std::set<int32_t> all;
    for (int32_t q = 0; q < kParts; ++q) all.insert(q);
    const int32_t keyPart = Key(kKeyPlayer).partition(kParts);
    Sink sa(Pk::World7, all, &model, 0, false), sb(Pk::World8Channel12, all, &model, 0, false), sc(Pk::Player, {keyPart}, &model, 0, false),
         sd(Pk::World9, all, &model, 0, true), se(Pk::Control0, {0}, &model, 0, false);
    for (const Sink* s : {&sa, &sb, &sc, &sd, &se}) {
        if (s->expected.empty()) { fail("FILTER-CONSUME", s->label + ": the model leaves nothing to receive (raise FILTER_E2E_N)"); return 1; }
    }

    // Plain subscriptions on client 0; the two group members on clients 1 and 2 (a plain subscription never shares a
    // Client with a group consumer: the seed session is then the coordinator's).
    Client plain(makeConfig(env, 0));
    Client g1c(makeConfig(env, 1)), g2c(makeConfig(env, 2));
    GroupConfig gcfg;
    gcfg.isolation       = IsolationLevel::ReadCommitted;
    gcfg.sessionTimeoutMs = 30000;
    gcfg.filter(filterOf(Pk::World9));
    auto onGroupRecords = [&sd](const RecordSet& rs) {
        const int32_t part = rs.partition;
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
            sd.onRecord(part, {reinterpret_cast<const uint8_t*>(v.data()), v.size()});
        });
    };
    GroupConsumer gc1 = g1c.subscribeGroup("worlds-9", {kTopicName}, gcfg);
    GroupConsumer gc2 = g2c.subscribeGroup("worlds-9", {kTopicName}, gcfg);
    int assignEvents = 0;   // onAssigned calls of both members: every one is a generation a redelivery may follow
    gc1.onAssigned([&assignEvents](std::vector<TopicPartition>) { ++assignEvents; });
    gc2.onAssigned([&assignEvents](std::vector<TopicPartition>) { ++assignEvents; });
    gc1.onRecords(onGroupRecords);
    gc2.onRecords(onGroupRecords);
    const Clients cs{&plain, &g1c, &g2c};
    plain.connectAndWait();
    g1c.connectAndWait();
    g2c.connectAndWait();
    const TopicHandle th = plain.topic(kTopicName);
    if (!th.valid() || th.partitionCount != kParts) { fail("FILTER-CONSUME", "player_event is not in RUNE_CLIENT_TOPIC_TABLE"); return 1; }

    std::vector<PartHandle> handles;
    handles.reserve(2 * kParts + 4);
    auto handlerFor = [](Sink* s) { return [s](const Record& r) { s->onRecord(r.partition, r.value); }; };
    // (a), (b): AllPartitions (one SubscriptionSet each), then per-partition re-subscribes.
    SubscriptionSet seta = plain.subscribe(th, AllPartitions{}, optionsFor(Pk::World7, From::earliest()), handlerFor(&sa));
    SubscriptionSet setb = plain.subscribe(th, AllPartitions{}, optionsFor(Pk::World8Channel12, From::earliest()), handlerFor(&sb));
    for (int32_t q = 0; q < kParts; ++q) {
        PartHandle ha, hb;
        ha.sink = &sa; ha.part = q; ha.cur = &seta[q];
        ha.reopen = [&plain, th, q, handlerFor, &sa](From f) { return plain.subscribe(th, q, optionsFor(Pk::World7, f), handlerFor(&sa)); };
        hb.sink = &sb; hb.part = q; hb.cur = &setb[q];
        hb.reopen = [&plain, th, q, handlerFor, &sb](From f) { return plain.subscribe(th, q, optionsFor(Pk::World8Channel12, f), handlerFor(&sb)); };
        handles.push_back(std::move(ha));
        handles.push_back(std::move(hb));
    }
    // (c): Key(k) + the entity filter.
    Subscription subc = plain.subscribe(th, Key(kKeyPlayer), optionsFor(Pk::Player, From::earliest()), handlerFor(&sc));
    {
        PartHandle h;
        h.sink = &sc; h.part = keyPart; h.cur = &subc;
        h.reopen = [&plain, th, keyPart, handlerFor, &sc](From f) { return plain.subscribe(th, keyPart, optionsFor(Pk::Player, f), handlerFor(&sc)); };
        handles.push_back(std::move(h));
    }
    // (e): the unfiltered control.
    Subscription sube = plain.subscribe(th, 0, optionsFor(Pk::Control0, From::earliest()), handlerFor(&se));
    {
        PartHandle h;
        h.sink = &se; h.part = 0; h.cur = &sube;
        h.reopen = [&plain, th, handlerFor, &se](From f) { return plain.subscribe(th, 0, optionsFor(Pk::Control0, f), handlerFor(&se)); };
        handles.push_back(std::move(h));
    }

    std::optional<std::vector<int64_t>> ends;
    auto reached = [&] {
        if (!ends) ends = readEnds(env.shared + "/ends");
        if (!ends) return false;
        for (const Sink* s : {&sa, &sb, &sc, &sd, &se}) if (!s->complete()) return false;
        for (const PartHandle& h : handles) if (h.cur->position() < (*ends)[static_cast<std::size_t>(h.part)]) return false;
        return true;
    };
    std::printf("FILTER-CONSUME: READY\n");
    std::fflush(stdout);
    const auto deadline = Clock::now() + std::chrono::seconds(timeoutS);
    auto lastCheck = Clock::now();
    std::string why;
    bool done = false;
    while (Clock::now() < deadline) {
        pumpAll(cs);
        if (Clock::now() - lastCheck < std::chrono::milliseconds(50)) continue;
        lastCheck = Clock::now();
        if (!supervise(handles, "FILTER-CONSUME", why)) { fail("FILTER-CONSUME", why); return 1; }
        if (gc1.finished() || gc2.finished()) {
            fail("FILTER-CONSUME", "a group member ended: failReason " + std::to_string(static_cast<int>(gc1.finished() ? gc1.failReason() : gc2.failReason())));
            return 1;
        }
        if (reached()) { done = true; break; }
    }
    if (!done) {
        std::string state;
        for (const Sink* s : {&sa, &sb, &sc, &sd, &se}) state += "\n  " + s->label + ": covered " + std::to_string(s->covered) + " of " + std::to_string(s->expected.size());
        for (const PartHandle& h : handles) state += "\n  " + h.sink->label + " partition " + std::to_string(h.part) + " position " + std::to_string(h.cur->position()) + (ends ? " end " + std::to_string((*ends)[static_cast<std::size_t>(h.part)]) : " (no ends file)");
        fail("FILTER-CONSUME", "timed out after " + std::to_string(timeoutS) + " s" + state);
        return 1;
    }
    pumpFor(cs, 3000);   // a quiet window: an extra or duplicate delivery lands now
    int violations = 0;
    for (const Sink* s : {&sa, &sb, &sc, &sd, &se}) violations += s->verify("FILTER-CONSUME");
    for (const PartHandle& h : handles) {
        if (h.cur->position() < (*ends)[static_cast<std::size_t>(h.part)]) {
            ++violations;
            fail("FILTER-CONSUME", h.sink->label + " partition " + std::to_string(h.part) + ": position " + std::to_string(h.cur->position()) + " short of the end");
        }
    }
    // M2: a group may rewind (at-least-once) only after a generation change: no more rewinding partitions than assignments.
    std::printf("FILTER-CONSUME: group worlds-9 assignments=%d rewound-records=%lld duplicates-allowed-only-after-an-assignment\n", assignEvents, static_cast<long long>(sd.rewinds));
    if (sd.rewinds > 0 && assignEvents <= 2) {
        ++violations;
        fail("FILTER-CONSUME", "group worlds-9 redelivered " + std::to_string(sd.rewinds) + " records without a rebalance (assignments " + std::to_string(assignEvents) + ")");
    }
    int resubs = 0;
    for (const PartHandle& h : handles) resubs += h.resubs;
    std::printf("FILTER-CONSUME: resubscribes=%d batchBytes a=%llu b=%llu c=%llu e=%llu\n", resubs,
                static_cast<unsigned long long>(seta.batchBytes()), static_cast<unsigned long long>(setb.batchBytes()),
                static_cast<unsigned long long>(subc.batchBytes()), static_cast<unsigned long long>(sube.batchBytes()));
    if (violations != 0) { fail("FILTER-CONSUME", std::to_string(violations) + " violations"); return 1; }
    gc1.close();
    gc2.close();
    std::printf("FILTER-CONSUME: OK sets=5\n");
    std::fflush(stdout);
    return 0;
}

// ---- errors ----------------------------------------------------------------------------------------------------------

struct Errors {
    std::vector<std::string> v;
    [[nodiscard]] bool has(std::string_view needle) const {
        for (const auto& e : v) if (e.find(needle) != std::string::npos) return true;
        return false;
    }
    [[nodiscard]] std::string all() const {
        std::string s;
        for (const auto& e : v) s += "[" + e + "] ";
        return s;
    }
};

int runErrors(const Env& env) {
    const char* tag = "FILTER-ERRORS";
    const int cap   = static_cast<int>(envInt("FILTER_E2E_CAP", 256));
    Errors errs;
    Client c(makeConfig(env, 0));
    c.onError([&errs](const std::string& m) { errs.v.push_back(m); });
    c.connectAndWait();
    const TopicHandle th = c.topic(kTopicName), uth = c.topic(kUntypedName);
    if (!th.valid() || !uth.valid()) { fail(tag, "player_event / untyped_t missing from RUNE_CLIENT_TOPIC_TABLE"); return 1; }
    int bad = 0;

    auto settle = [&](TopicHandle t, int32_t part, const rune::codec::FilterBytes& f) {
        SubscribeOptions o;
        o.from = From::earliest();
        o.filter(f);
        auto sub = std::make_unique<Subscription>(c.subscribe(t, part, o, [](const Record&) {}));
        if (!pumpUntil({&c}, [&] { return sub->finished() || sub->subscriptionId() != 0; }, 60)) { ++bad; fail(tag, "a Subscribe was never answered"); }
        return sub;
    };
    auto expectRefused = [&](Subscription& sub, const char* row, std::string_view detail) {
        pumpUntil({&c}, [&] { return sub.finished() && errs.has(detail); }, 30);
        if (!sub.finished() || sub.failReason() != SubscriptionFailReason::RejectedByBroker || sub.brokerErrorCode() != 39) {
            ++bad; fail(tag, std::string(row) + ": not refused (finished " + std::to_string(sub.finished()) + ", failReason " + std::to_string(static_cast<int>(sub.failReason())) + ", broker code " + std::to_string(sub.brokerErrorCode()) + ")");
        } else if (!errs.has(detail)) {
            ++bad; fail(tag, std::string(row) + ": onError never carried '" + std::string(detail) + "': " + errs.all());
        } else {
            std::printf("%s %s refused: %.*s\n", tag, row, static_cast<int>(detail.size()), detail.data());
        }
    };

    // E1: a filter on an untyped topic.
    { auto s = settle(uth, 0, E::Filter{.worldId = 7u}.encode()); expectRefused(*s, "E1", "filter needs a typed topic"); }

    // E3-E6: the client's descriptor disagrees with the topic's (worldId retyped, name / pos retyped as integers, a tag the topic never had).
    {
        using namespace rune::codec;
        const Descriptor stale = DescriptorBuilder("player_event")
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
        struct Row { const char* label; const char* field; const char* detail; };
        for (const Row& r : {Row{"E3 (unknown tag)", "ghost", "(tag 20): unknown tag"}, Row{"E4 (type mismatch)", "worldId", "(tag 1): type mismatch"},
                             Row{"E5 (variable-length)", "name", "(tag 7): variable-length field"}, Row{"E6 (float field)", "pos", "(tag 8): float field"}}) {
            auto bytes = FilterBuilder::on(stale).field(r.field).eq(std::int64_t{7}).build();
            if (!bytes) { ++bad; fail(tag, std::string(r.label) + ": the builder refused locally"); continue; }
            auto s = settle(th, 0, *bytes);
            expectRefused(*s, r.label, r.detail);
        }
        auto ok = settle(th, 0, E::Filter{.worldId = 7u}.encode());
        if (ok->finished() || ok->subscriptionId() == 0) { ++bad; fail(tag, "a valid filter after the refusals was not admitted"); }
    }

    // E8: the per-partition subscription cap.  Filtered subscriptions on partition 0 are admitted up to the cap and the next
    // one is refused naming the knob; the admitted ones stay bound; capacity returns when one leaves.  (Subscriptions of
    // an earlier leg may still count until their sessions are reaped, so the refusal may come a little before `cap`.)
    {
        std::vector<std::unique_ptr<Subscription>> subs;
        bool refused = false;
        for (int i = 0; i < cap + 1 && !refused; ++i) {
            auto s = settle(th, 0, E::Filter{.worldId = static_cast<uint32_t>(i % 150)}.encode());
            if (s->finished()) {
                refused = true;
                expectRefused(*s, "E8", "filter capacity");
                if (!errs.has("RUNE_FILTER_MAX_SUBS_PER_PARTITION")) { ++bad; fail(tag, "E8: the detail does not name the knob: " + errs.all()); }
                std::printf("%s E8 refused after %zu admitted (cap %d)\n", tag, subs.size(), cap);
                // Earlier legs' filtered subscriptions on partition 0 (a, b, c, d) may still count until their sessions are reaped.
                constexpr int kSlack = 4;
                const int admitted = static_cast<int>(subs.size());
                if (admitted > cap || admitted < cap - kSlack) { ++bad; fail(tag, "E8: refused after " + std::to_string(admitted) + " admitted, the cap is " + std::to_string(cap) + " (slack " + std::to_string(kSlack) + ")"); }
            }
            subs.push_back(std::move(s));
        }
        if (!refused) { ++bad; fail(tag, "E8: " + std::to_string(subs.size()) + " subscriptions admitted, the cap is " + std::to_string(cap)); }
        else {
            for (std::size_t i = 0; i + 1 < subs.size(); ++i)
                if (subs[i]->finished()) { ++bad; fail(tag, "E8: an admitted subscription ended after the refusal"); break; }
            subs.front()->unsubscribe();   // capacity returns
            if (!pumpUntil({&c}, [&] { return subs.front()->finished(); }, 30)) { ++bad; fail(tag, "E8: the unsubscribe never finished"); }
            auto again = settle(th, 0, E::Filter{.worldId = 151u}.encode());
            if (again->finished() || again->subscriptionId() == 0) { ++bad; fail(tag, "E8: capacity did not return after an unsubscribe"); }
            subs.push_back(std::move(again));
        }
        for (auto& s : subs) s->unsubscribe();
        if (!pumpUntil({&c}, [&] { for (auto& s : subs) if (!s->finished()) return false; return true; }, 60)) { ++bad; fail(tag, "E8: the subscriptions never all finished after unsubscribe"); }
    }

    // E10: a second member of the same group with another filter.
    {
        Errors errsB;
        Client a(makeConfig(env, 1)), b(makeConfig(env, 2));
        b.onError([&errsB](const std::string& m) { errsB.v.push_back(m); });
        GroupConfig ga, gb;
        ga.filter(E::Filter{.worldId = 9u});
        gb.filter(E::Filter{.worldId = 10u});
        std::size_t assignedA = 0;
        GroupConsumer ca = a.subscribeGroup("g_e10", {kTopicName}, ga);
        ca.onAssigned([&](std::vector<TopicPartition> parts) { assignedA += parts.size(); });
        GroupConsumer cb = b.subscribeGroup("g_e10", {kTopicName}, gb);
        a.connectAndWait();
        if (!pumpUntil({&a}, [&] { return assignedA != 0 || ca.finished(); }, 90) || ca.finished()) { ++bad; fail(tag, "E10: the first member never got an assignment"); }
        b.connectAndWait();
        if (!pumpUntil({&a, &b}, [&] { return cb.finished(); }, 90)) { ++bad; fail(tag, "E10: the second member was never refused"); }
        else if (cb.failReason() != GroupFailReason::FilterMismatch) { ++bad; fail(tag, "E10: failReason " + std::to_string(static_cast<int>(cb.failReason())) + ", want FilterMismatch"); }
        else if (!errsB.has("group filter mismatch")) { ++bad; fail(tag, "E10: onError never carried 'group filter mismatch': " + errsB.all()); }
        else std::printf("%s E10 refused: group filter mismatch\n", tag);
        if (ca.finished()) { ++bad; fail(tag, "E10: the first member ended"); }
        ca.close();
    }
    if (bad != 0) { fail(tag, std::to_string(bad) + " violations"); return 1; }
    std::printf("FILTER-ERRORS: OK\n");
    std::fflush(stdout);
    return 0;
}

// ---- alter -----------------------------------------------------------------------------------------------------------

int runAlter(const Env& env) {
    const char* tag = "FILTER-ALTER";
    constexpr int kAlterRecs = 2000;
    const std::vector<Ev> v1 = makeModel(env.seed, env.n);
    const int64_t v2First = static_cast<int64_t>(v1.size());
    const std::vector<Ev> v2 = makeAlterModel(env.seed, v2First, kAlterRecs, 2);
    const int64_t v3First = v2First + kAlterRecs;
    const std::vector<Ev> v3 = makeAlterModel(env.seed, v3First, kAlterRecs, 3);
    std::vector<Ev> all = v1;
    all.insert(all.end(), v2.begin(), v2.end());
    all.insert(all.end(), v3.begin(), v3.end());
    std::set<int32_t> parts;
    for (int32_t q = 0; q < kParts; ++q) parts.insert(q);
    Sink sb(Pk::World8Alter, parts, &all, 0, false);
    std::size_t v12 = 0, v3m = 0;
    for (const int64_t s : sb.expected) (s >= v3First ? v3m : v12)++;
    if (v12 == 0 || v3m != 0) { fail(tag, "the model is wrong (expected v1/v2 matches " + std::to_string(v12) + ", v3 " + std::to_string(v3m) + ")"); return 1; }

    Client c(makeConfig(env, 0));
    c.connectAndWait();
    const TopicHandle th = c.topic(kTopicName);
    SubscriptionSet set = c.subscribe(th, AllPartitions{}, optionsFor(Pk::World8Alter, From::earliest()), [&sb](const Record& r) { sb.onRecord(r.partition, r.value); });
    std::vector<PartHandle> handles;
    for (int32_t q = 0; q < kParts; ++q) {
        PartHandle h;
        h.sink = &sb; h.part = q; h.cur = &set[q];
        h.reopen = [&c, th, q, &sb](From f) { return c.subscribe(th, q, optionsFor(Pk::World8Alter, f), [&sb](const Record& r) { sb.onRecord(r.partition, r.value); }); };
        handles.push_back(std::move(h));
    }
    std::string why;
    auto wait = [&](auto&& pred, int seconds, const char* what) {
        const auto dl = Clock::now() + std::chrono::seconds(seconds);
        while (Clock::now() < dl) {
            pumpAll({&c});
            if (!supervise(handles, tag, why)) { fail(tag, why); return false; }
            if (pred()) return true;
        }
        fail(tag, std::string("timed out waiting for ") + what);
        return false;
    };
    // The v1 matches first (the live subscription sees the schema change happen under it).
    std::size_t v1Matches = 0;
    for (const int64_t s : sb.expected) if (s < v2First) ++v1Matches;
    if (!wait([&] { return sb.covered >= v1Matches; }, 120, "the v1 matches")) return 1;

    ProducerConfig pc = plainProducerConfig(env);
    AckState acks;
    Producer p = c.producer(pc);
    p.onAck(&onAck, &acks);
    auto produce = [&](const std::vector<Ev>& evs) {
        for (const Ev& e : evs) {
            const std::vector<uint8_t> bytes = encodeVersioned(e, realtimeNs());
            p.send(th, Key(e.playerId), bytes, AckToken{tokenOf(e.seq, e.part)});
        }
        p.flush();
        return wait([&] { return acks.acked >= static_cast<int64_t>(evs.size()) + 0 && acks.failed == 0; }, 120, "the produce acks");
    };
    auto alter = [&](const std::vector<uint8_t>& ops, const char* what) {
        const auto r = c.alterTopicAndWait(kTopicName, 0, ops, {}, std::chrono::seconds(60));
        if (r.errorCode != 0) { fail(tag, std::string(what) + ": " + r.detail); return false; }
        std::printf("%s %s -> schema version %u\n", tag, what, static_cast<unsigned>(r.newVersion));
        return true;
    };
    if (!alter(addGuildOps(), "ADD guild")) return 1;
    acks = AckState{};
    if (!produce(v2) || acks.failed != 0) return 1;
    if (!alter(dropChannelOps(), "DROP channel")) return 1;
    acks = AckState{};
    if (!produce(v3) || acks.failed != 0) return 1;
    std::vector<int64_t> ends(acks.end.begin(), acks.end.end());   // v3 is the last record of every partition it reached
    // Partitions that got no v3 record keep their v2 end: re-derive from the v2 acks is not needed, position follows notes.
    if (!wait([&] {
            if (!sb.complete()) return false;
            for (const PartHandle& h : handles) if (ends[static_cast<std::size_t>(h.part)] > 0 && h.cur->position() < ends[static_cast<std::size_t>(h.part)]) return false;
            return true;
        }, 120, "the matches and the partition ends"))
        return 1;
    pumpFor({&c}, 3000);
    const int violations = sb.verify(tag);
    bool anyV3 = false;
    for (const auto& [s, n] : sb.seen) { (void)n; if (s >= v3First) anyV3 = true; }
    if (anyV3) { fail(tag, "a v3 record (no channel field) reached the subscription"); return 1; }
    if (violations != 0) { fail(tag, std::to_string(violations) + " violations"); return 1; }
    std::printf("FILTER-ALTER: OK\n");
    std::fflush(stdout);
    return 0;
}

// ---- world -----------------------------------------------------------------------------------------------------------

struct World {
    uint32_t id = 0;
    bool     unfiltered = false;
    bool     measuring = false;
    int64_t  records = 0, foreign = 0;
    std::vector<uint32_t> latUs;
    std::size_t oneRecordBytes = 0;
    std::size_t keyValueBytes = 0;      // key + value of one record: the physical floor of a delivered record's bytes
                                        // (oneRecordBytes above: a one-record batch of this world's record shape, the ideal per record)
    std::optional<SubscriptionSet> set;
    uint64_t bytesAtStart = 0;
};

int runWorld(const Env& env) {
    const char* tag = "FILTER-WORLD";
    const int  count  = static_cast<int>(envInt("FILTER_E2E_WORLDS_PER_PROC", 25));
    const int  base   = static_cast<int>(envInt("FILTER_E2E_WORLD_BASE", 0));
    const long secs   = envInt("FILTER_E2E_SECONDS", 30);
    const bool unfilt = envInt("FILTER_E2E_UNFILTERED", 0) != 0;
    Client c(makeConfig(env, 0));
    c.connectAndWait();
    const TopicHandle th = c.topic(kTopicName);
    if (!th.valid() || th.partitionCount != kParts) { fail(tag, "player_event is not in RUNE_CLIENT_TOPIC_TABLE"); return 1; }
    std::vector<std::unique_ptr<World>> worlds;
    for (int i = 0; i < count; ++i) {
        auto w = std::make_unique<World>();
        w->id = static_cast<uint32_t>(base + i);
        w->unfiltered = unfilt;
        w->latUs.reserve(1u << 16);
        World* wp = w.get();
        SubscribeOptions o;
        o.from = From::latest();
        if (!unfilt) o.filter(E::Filter{.worldId = wp->id});
        w->set.emplace(c.subscribe(th, AllPartitions{}, o, [wp](const Record& r) {
            if (r.value.size() < 4) return;
            E ev;
            ev.decode(r.value);
            if (ev.worldId != wp->id) { if (!wp->unfiltered) ++wp->foreign; return; }
            if (!wp->measuring) return;
            ++wp->records;
            if (wp->keyValueBytes == 0) wp->keyValueBytes = r.key.size() + r.value.size();
            if (wp->oneRecordBytes == 0) {
                rune::protocol::RecordBatchBuilder b(0, 0);
                b.append(std::string_view(reinterpret_cast<const char*>(r.key.data()), r.key.size()),
                         std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size()), 0);
                wp->oneRecordBytes = b.build().size();
            }
            const int64_t lat = (realtimeNs() - ev.hp) / 1000;
            wp->latUs.push_back(static_cast<uint32_t>(std::clamp<int64_t>(lat, 0, 0xFFFFFFFFLL)));
        }));
        worlds.push_back(std::move(w));
    }
    auto bound = [&] {
        for (auto& w : worlds) {
            if (w->set->finished()) return true;   // surfaced below
            for (int32_t q = 0; q < kParts; ++q) if ((*w->set)[q].subscriptionId() == 0) return false;
        }
        return true;
    };
    if (!pumpUntil({&c}, bound, 120)) { fail(tag, "the subscriptions never all bound"); return 1; }
    for (auto& w : worlds) if (w->set->finished()) { fail(tag, "world " + std::to_string(w->id) + ": a subscription ended"); return 1; }
    // SubscriptionStatus reaches the handle through a small ring: a handle that does not read lets it fill, and the next read
    // then returns a stale snapshot (the cause of the first scenario's bytes undercount).  Reading every set often keeps
    // the snapshot fresh, and the final value is read after unsubscribing (the final snapshot is exact).
    auto pumpReading = [&](int ms) {
        auto nextRead = Clock::now();
        for (const auto dl = Clock::now() + std::chrono::milliseconds(ms); Clock::now() < dl;) {
            pumpAll({&c});
            if (Clock::now() >= nextRead) {
                for (auto& w : worlds) (void)w->set->batchBytes();
                nextRead = Clock::now() + std::chrono::milliseconds(20);
            }
        }
    };
    pumpReading(2000);   // settle: the first batches
    pumpReading(100);    // the reads above freed the rings; let the duty side republish its dirty snapshots
    for (auto& w : worlds) { w->measuring = true; w->bytesAtStart = w->set->batchBytes(); w->records = 0; w->latUs.clear(); }
    std::printf("FILTER-WORLD: READY worlds=%d\n", count);
    std::fflush(stdout);
    pumpReading(static_cast<int>(secs * 1000));
    int endedEarly = 0;
    for (auto& w : worlds) if (w->set->finished()) ++endedEarly;
    for (auto& w : worlds) { w->measuring = false; w->set->unsubscribe(); }
    pumpUntil({&c}, [&] { for (auto& w : worlds) if (!w->set->finished()) return false; return true; }, 60);
    int bad = 0;
    if (endedEarly != 0) { ++bad; fail(tag, std::to_string(endedEarly) + " world servers had a subscription end during the run"); }
    for (auto& w : worlds) {
        std::sort(w->latUs.begin(), w->latUs.end());
        auto pct = [&](double q) { return w->latUs.empty() ? 0.0 : static_cast<double>(w->latUs[std::min(w->latUs.size() - 1, static_cast<std::size_t>(q * static_cast<double>(w->latUs.size())))]); };
        const uint64_t bytes = w->set->batchBytes() - w->bytesAtStart;
        std::printf("WORLD w=%u records=%lld bytes=%llu ideal_bytes=%llu p50_us=%.0f p99_us=%.0f\n", w->id, static_cast<long long>(w->records),
                    static_cast<unsigned long long>(bytes), static_cast<unsigned long long>(static_cast<uint64_t>(w->records) * w->oneRecordBytes),
                    pct(0.50), pct(0.99));
        if (w->foreign != 0) { ++bad; fail(tag, "world " + std::to_string(w->id) + ": " + std::to_string(w->foreign) + " foreign records reached a filtered subscription"); }
        if (w->records == 0) { ++bad; fail(tag, "world " + std::to_string(w->id) + ": no records"); }
        // The physical floor: every delivered record carries at least its key and value.
        if (bytes < static_cast<uint64_t>(w->records) * w->keyValueBytes) {
            ++bad;
            fail(tag, "world " + std::to_string(w->id) + ": " + std::to_string(bytes) + " batch bytes for " + std::to_string(w->records) + " records of at least " + std::to_string(w->keyValueBytes) + " bytes: the bytes are undercounted");
        }
    }
    std::printf("FILTER-WORLD: %s\n", bad == 0 ? "OK" : "FAIL");
    std::fflush(stdout);
    return bad == 0 ? 0 : 1;
}

// ---- selfcheck -------------------------------------------------------------------------------------------------------

int selfcheck() {
    int bad = 0;
    auto expect = [&](bool ok, const char* what) { if (!ok) { std::fprintf(stderr, "FILTER-SELFCHECK: FAIL %s\n", what); ++bad; } };
    const std::vector<Ev> m = makeModel(7, 40000);
    const std::vector<Ev> m2 = makeModel(7, 40000);
    bool same = m.size() == m2.size();
    for (std::size_t i = 0; same && i < m.size(); ++i) same = m[i].playerId == m2[i].playerId && m[i].worldId == m2[i].worldId && m[i].cls == m2[i].cls;
    expect(same, "the model is deterministic");
    std::set<int32_t> all;
    for (int32_t q = 0; q < kParts; ++q) all.insert(q);
    Sink a(Pk::World7, all, &m, 0, false), b(Pk::World8Channel12, all, &m, 0, false), c(Pk::Player, {Key(kKeyPlayer).partition(kParts)}, &m, 0, false),
         d(Pk::World9, all, &m, 0, true), e(Pk::Control0, {0}, &m, 0, false);
    expect(a.expected.size() > kTxnRecs && b.expected.size() > 50 && c.expected.size() > 100 && d.expected.size() > 50 && e.expected.size() > 1000, "every selection has a set");
    expect(e.expectedEmpty > 0, "the control sees empty-value records");
    for (int64_t i = kTxnRecs; i < 2 * kTxnRecs; ++i) expect(a.expected.count(i) == 0, "aborted records are not in (a)");
    for (int64_t i = 0; i < kTxnRecs; ++i) expect(a.expected.count(i) == 1, "committed records are in (a)");
    // codec: every record the model can produce round-trips through both encoders, and the filters agree with the model
    int checked = 0;
    for (const Ev& ev : m) {
        if (ev.cls == Cls::Tomb) continue;
        uint8_t buf[256];
        const std::size_t n = encodeV1(ev, 123, buf);
        const Decoded d1 = decodeAny({buf, n});
        const std::vector<uint8_t> w = encodeVersioned(ev, 123);
        const Decoded d2 = decodeAny(w);
        if (!d1.ok || !d2.ok || d1.seq != ev.seq || d2.seq != ev.seq || d1.worldId != ev.worldId || d2.playerId != ev.playerId || d1.sendNs != 123 || !d1.hasChannel) { expect(false, "record round trip"); break; }
        if (n != w.size() + 4 && n < w.size()) { expect(false, "v1 sizes"); break; }
        for (Pk k : {Pk::World7, Pk::World8Channel12, Pk::Player, Pk::World9}) {
            if (inSet(k, ev) && !decodedMatches(k, d1)) { expect(false, "model and decoded verdicts agree"); break; }
        }
        if (++checked >= 5000) break;
    }
    // v2 / v3 records decode with the right fields
    const std::vector<Ev> a2 = makeAlterModel(7, 100000, 40, 2), a3 = makeAlterModel(7, 100040, 40, 3);
    for (const Ev& ev : a2) { const Decoded x = decodeAny(encodeVersioned(ev, 5)); expect(x.ok && x.version == 2 && x.hasChannel && x.seq == ev.seq, "v2 round trip"); }
    for (const Ev& ev : a3) { const Decoded x = decodeAny(encodeVersioned(ev, 5)); expect(x.ok && x.version == 3 && !x.hasChannel && x.seq == ev.seq, "v3 round trip"); }
    // the generated filter and the runtime builder encode the same bytes
    {
        auto rb = rune::codec::FilterBuilder::on(rune::examples::playerEventDescriptor()).field("worldId").eq(8u).field("channel").in({1, 2}).build();
        expect(rb.has_value() && rb->span().size() == filterOf(Pk::World8Channel12).span().size() &&
               std::equal(rb->span().begin(), rb->span().end(), filterOf(Pk::World8Channel12).span().begin()), "FilterBuilder == E::Filter");
    }
    // the sink counts what it should
    {
        Sink s(Pk::World7, all, &m, 0, false);
        for (const int64_t seq : s.expected) {
            const Ev& ev = m[static_cast<std::size_t>(seq)];
            uint8_t buf[256];
            const std::size_t n = encodeV1(ev, 1, buf);
            s.onRecord(ev.part, {buf, n});
        }
        expect(s.complete() && s.verify("FILTER-SELFCHECK") == 0, "a sink fed its own set verifies");
        const Ev& dup = m[static_cast<std::size_t>(*s.expected.begin())];
        uint8_t buf[256];
        const std::size_t n = encodeV1(dup, 1, buf);
        s.onRecord(dup.part, {buf, n});
        expect(s.verify("FILTER-SELFCHECK(expected violation)") != 0, "a duplicate is a violation");
    }
    if (bad == 0) std::printf("FILTER-SELFCHECK: OK\n");
    return bad == 0 ? 0 : 1;
}

}  // namespace

int main() {
    const std::string mode = envStr("RUNE_MODE");
    if (mode == "selfcheck") return selfcheck();
    const Env env = readEnv();
    g_idleUs = static_cast<int>(envInt("FILTER_E2E_IDLE_US", 0));
    if (env.target.empty() || env.egressHost.empty() || env.dataDir.empty()) {
        std::fprintf(stderr, "FILTER-PROBE: RUNE_TARGET, RUNE_EGRESS_HOST and RUNE_DATA_DIR are required\n");
        return 1;
    }
    try {
        if (mode == "setup") return runSetup(env);
        if (mode == "produce") return runProduce(env);
        if (mode == "consume") return runConsume(env);
        if (mode == "errors") return runErrors(env);
        if (mode == "alter") return runAlter(env);
        if (mode == "world") return runWorld(env);
        std::fprintf(stderr, "FILTER-PROBE: RUNE_MODE must be setup, produce, consume, errors, alter, world or selfcheck (got '%s')\n", mode.c_str());
        return 1;
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "FILTER-PROBE: FAIL %s: %s\n", mode.c_str(), ex.what());
        return 1;
    }
}
