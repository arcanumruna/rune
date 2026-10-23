// EosBrokerRegressionTest.cpp - Phase 2b F Task 11: the unified transactional Producer (rune::client::Producer with
// ProducerConfig::transactionalId) against THREE real in-process BrokerNodes over Aeron UDP: exactly-once across
// every produce path, fencing, an in-doubt commit, a partition-leader crash, the abort -> seekToCommitted -> redo
// loop, and a plain acked producer's dedup across a reconnect.
//
// Rig: ThreeNodes (rf 3 topics created by DDL) + Invoker-mode clients.  Invoker mode runs no client threads, so a
// subscription handler, a group onRecords and every completion run on THIS thread inside doWork(): no mutex, no
// atomic, no sleep anywhere - every wait is a bounded pump loop (the deadline is the failure, never a hang).
//
// Read-back is the real client's own read_committed / read_uncommitted subscription.  A negative check ("this
// record is invisible") is only valid once the reader has demonstrably advanced past the records in question, so
// every test ends its writes with a committed SENTINEL transaction on every partition: a read_committed reader
// that has seen the sentinels has necessarily resolved everything before them (a read_committed partition stream
// is ordered and blocked at the last stable offset).
//
// The plain slice reads client-cpp's TxnMatrixExpectations.h (R1 / R2); the fencing test reads R9 and the
// refusal test R4-R6.  Crash tests live in rune_client_failover_broker_tests (RUN_SERIAL).
#include "client/ClusterClient.h"
#include "gen/position.gen.h"
#include "support/TxnMatrixExpectations.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/CoordinatorLink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerRef.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
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
namespace mx = rune::client::txnmatrix;
namespace rpc = rune::protocol::client;
using rune::gen::Position;
using Clock = std::chrono::steady_clock;

namespace {

constexpr auto    kDisk  = rc::Persistence::Disk;
constexpr int32_t kParts = 4;
constexpr std::size_t kLen = 16;   // "id=" + 13 digits

constexpr mx::Op kDataOps[] = {mx::Op::Send,       mx::Op::SendTo,     mx::Op::TrySendCq, mx::Op::TrySendAck,
                               mx::Op::Batch,      mx::Op::TryClaim,   mx::Op::ClaimBatch, mx::Op::TypedSend,
                               mx::Op::TypedClaim};

// ---------------------------------------------------------------------------------------------------------
// Cluster plumbing (as ClientFailoverBrokerRegressionTest's, with the partition count a parameter).
// ---------------------------------------------------------------------------------------------------------

/// The typed schema of `eos.pos` - the descriptor the client's claim-schema codegen compiles Position from.
rune::codec::Descriptor positionDescriptor() {
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    return DescriptorBuilder("position")
        .add("userId", PrimType::Int64, 1, true)
        .add("zone", PrimType::Int32)
        .add("x", PrimType::Float64)
        .add("y", PrimType::Float64)
        .add("z", PrimType::Float64)
        .add("flags", PrimType::UInt16)
        .add("seq", PrimType::UInt32)
        .add("tick", PrimType::UInt64)
        .add("alive", PrimType::Bool)
        .add("kind", PrimType::Enum, 1, false, {0, 1, 2})
        .addFixedStr("tag", 8)
        .build();
}

/// One 3-node cluster + the helpers every test shares.
struct Env {
    ThreeNodes c;
    int32_t    ctl = -1;
    explicit Env(const char* tag) : c(ThreeNodes::start(tag)) {
        if (!c.awaitCatalogLeader(30s)) throw std::runtime_error("no catalog leader");
        ctl = awaitController(c);
        if (ctl <= 0) throw std::runtime_error("no controller");
    }

    /// Define a topic (rf 3) by DDL and wait until every partition has a live leader.
    rc::TopicSpec define(const std::string& name, int32_t parts, const std::string& keyField = {},
                         std::vector<uint8_t> blob = {}, bool validate = false) {
        auto admin = connectClient(c, ctl);
        rc::TopicOptions to;
        to.replicationFactor = 3;
        const auto def = admin->defineTopicAndWait(name, parts, keyField, blob, validate, kDisk, to, {}, 60s);
        admin->close();
        if (def.errorCode != 0) throw std::runtime_error("define " + name + ": " + def.detail);
        std::vector<int32_t> leaders;
        if (!awaitLeaders(c, def.topicId, parts, leaders, 30s)) throw std::runtime_error("no leaders for " + name);
        return rc::TopicSpec{name, def.topicId, parts};
    }
    rc::TopicSpec defineTyped(const std::string& name, int32_t parts) {
        const rune::codec::Descriptor desc = positionDescriptor();
        if (rune::codec::fingerprint(desc) != Position::kFingerprint)
            throw std::runtime_error("the test descriptor is not the generated schema");
        return define(name, parts, "userId", rune::codec::serialize(desc), true);
    }

    int32_t leaderOfPartition(int32_t topicId, int32_t partition) {
        return leaderOn(c.node(c.runningNodes().front()), topicId, partition);
    }
    /// A running broker other than `a` (and `b`).
    int32_t otherThan(int32_t a, int32_t b = -1) {
        for (int id : c.runningNodes())
            if (id != a && id != b) return id;
        return -1;
    }

    rc::ClientConfig cfg(int seed, std::vector<rc::TopicSpec> topics) {
        auto k = clientConfigFor(c, seed);
        k.topics                       = std::move(topics);
        k.runnerMode                   = rc::RunnerMode::Invoker;
        k.connectTimeoutMs             = 30'000;
        k.pingTimeoutMs                = 30'000;
        k.aeronClientLivenessTimeoutNs = 120'000'000'000LL;   // a broker crash blocks this thread for a while
        return k;
    }
    std::unique_ptr<rc::Client> open(int seed, std::vector<rc::TopicSpec> topics) {
        return connectClient(c, cfg(seed, std::move(topics)));
    }
};

// ---------------------------------------------------------------------------------------------------------
// Values, identities, expectations.
// ---------------------------------------------------------------------------------------------------------

std::string valueFor(int64_t id) {
    char b[32];
    std::snprintf(b, sizeof b, "id=%013lld", static_cast<long long>(id));
    return std::string(b);
}
int64_t idOf(std::string_view v) { return v.size() == kLen && v.rfind("id=", 0) == 0 ? std::atoll(std::string(v.substr(3)).c_str()) : -1; }
std::span<const uint8_t> bytes(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}
std::string keyBytes(const rc::Key& k) { return std::string(reinterpret_cast<const char*>(k.bytes().data()), k.size()); }

/// The smallest id >= `from` whose Key routes to `partition`; `from` is advanced past it.
int64_t idForPartition(int32_t parts, int32_t partition, int64_t& from) {
    for (;; ++from)
        if (rc::Key(from).partition(parts) == partition) return from++;
}

/// The int64 a Key(int64_t) holds (for diagnostics).
int64_t idOfKey(const std::string& k) {
    uint64_t v = 0;
    std::memcpy(&v, k.data(), std::min<std::size_t>(8, k.size()));
    return static_cast<int64_t>(v);
}

/// What a reader must see: untyped records by their value text, typed ones by their key bytes.
struct Expected {
    std::set<std::string> values, keys;
    void merge(const Expected& o) {
        values.insert(o.values.begin(), o.values.end());
        keys.insert(o.keys.begin(), o.keys.end());
    }
    [[nodiscard]] std::size_t size() const { return values.size() + keys.size(); }
};

struct Seen {
    std::map<std::string, int> byValue, byKey;
};

bool covers(const Seen& s, const Expected& e) {
    if (s.byValue.size() < e.values.size() || s.byKey.size() < e.keys.size()) return false;
    for (const auto& v : e.values) if (s.byValue.count(v) == 0) return false;
    for (const auto& k : e.keys) if (s.byKey.count(k) == 0) return false;
    return true;
}

/// Exactly the expected records, each exactly once: nothing missing, nothing extra, no duplicate.
void expectExactlyOnce(const Seen& s, const Expected& e, const std::string& what) {
    int bad = 0;
    auto note = [&](const std::string& why, const std::string& item) {
        if (++bad <= 5) ADD_FAILURE() << what << ": " << why << " [" << item << "]";
    };
    for (const auto& v : e.values) {
        const auto it = s.byValue.find(v);
        if (it == s.byValue.end()) note("missing", v);
        else if (it->second != 1) note("seen " + std::to_string(it->second) + " times", v);
    }
    for (const auto& [v, n] : s.byValue) if (e.values.count(v) == 0) note("unexpected", v);
    for (const auto& k : e.keys) {
        const auto it = s.byKey.find(k);
        if (it == s.byKey.end()) note("missing typed key", std::to_string(k.size()));
        else if (it->second != 1) note("typed key seen " + std::to_string(it->second) + " times", std::to_string(k.size()));
    }
    for (const auto& [k, n] : s.byKey) if (e.keys.count(k) == 0) note("unexpected typed key", std::to_string(k.size()));
    if (bad > 5) ADD_FAILURE() << what << ": " << bad << " mismatches in all";
}

// ---------------------------------------------------------------------------------------------------------
// A reader: its own Invoker client + one subscription per partition of every topic.
// ---------------------------------------------------------------------------------------------------------

struct Reader {
    std::unique_ptr<rc::Client>   client;
    Seen                          seen;
    std::vector<rc::Subscription> subs;

    Reader(Env& e, int via, const std::vector<rc::TopicSpec>& topics, int32_t typedTopicId, rc::Isolation iso)
        : client(e.open(via, topics)) {
        rc::SubscribeOptions o;
        o.from      = rc::From::earliest();
        o.isolation = iso;
        for (const rc::TopicSpec& t : topics) {
            const rc::TopicHandle th = client->topic(t.name);
            const bool typed = t.topicId == typedTopicId;
            for (int32_t p = 0; p < t.partitionCount; ++p)
                subs.push_back(client->subscribe(th, p, o, [this, typed](const rc::Record& r) {
                    if (typed) ++seen.byKey[std::string(reinterpret_cast<const char*>(r.key.data()), r.key.size())];
                    else ++seen.byValue[std::string(reinterpret_cast<const char*>(r.value.data()), r.value.size())];
                }));
        }
    }
    ~Reader() {
        for (auto& s : subs) s.unsubscribe();
        client->close();
    }
    /// Pump until every expected record was delivered (bounded).
    /// `alsoPump`: a writer client whose retries must keep running while this reader waits (Invoker mode runs
    /// nothing on its own).
    bool await(const Expected& e, std::chrono::seconds budget = 90s, rc::Client* alsoPump = nullptr) {
        for (const auto deadline = Clock::now() + budget; Clock::now() < deadline; std::this_thread::yield()) {
            (void)client->doWork();
            if (alsoPump != nullptr) (void)alsoPump->doWork();
            if (covers(seen, e)) return true;
        }
        int shown = 0;   // what never arrived (a failed await is diagnosed, not just reported)
        for (const auto& v : e.values) if (seen.byValue.count(v) == 0 && shown++ < 8) ADD_FAILURE() << "never delivered: " << v;
        for (const auto& k : e.keys) if (seen.byKey.count(k) == 0 && shown++ < 8) ADD_FAILURE() << "never delivered typed key #" << idOfKey(k);
        return false;
    }
};

// ---------------------------------------------------------------------------------------------------------
// A writer: one producer (transactional or not) on one client, all nine data paths, the lifecycle as tokens.
// ---------------------------------------------------------------------------------------------------------

struct TxnResult {
    rc::OpStatus status    = rc::OpStatus::Closed;
    uint16_t     errorCode = 0;
    bool         done      = false;
    [[nodiscard]] bool ok() const { return done && status == rc::OpStatus::Ok; }
    [[nodiscard]] std::string str() const {
        return done ? "status=" + std::to_string(static_cast<int>(status)) + " code=" + std::to_string(errorCode)
                    : std::string("not completed");
    }
};

using TypedRef = rc::TypedTopic<Position, rc::ProducerRef>;

class Writer {
public:
    Writer(rc::Client& cl, rc::Producer& p, rc::TopicHandle th, int32_t parts, TypedRef* typed = nullptr)
        : client_(cl), p_(p), th_(th), parts_(parts), typed_(typed), cq_(cl) {
        p_.onAck(&Writer::onAck, this);
    }
    void alsoPump(rc::Client* c) { extra_.push_back(c); }

    /// One pump of every client this writer drives, then its completions.
    void step() {
        (void)client_.doWork();
        for (rc::Client* c : extra_) (void)c->doWork();
        drain();
        std::this_thread::yield();
    }
    void drain() {
        (void)cq_.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
            ++got_[d.token].times;
            got_[d.token].c = d;
        });
    }
    template <class P>
    bool until(P&& pred, std::chrono::seconds budget = 60s) {
        const auto deadline = Clock::now() + budget;
        for (;;) {
            if (pred()) return true;
            if (Clock::now() >= deadline) return false;
            step();
        }
    }

    // ---- lifecycle (token forms: the status is the assertion's input, not an exception) ----
    TxnResult wait(rc::OpResult r, rc::Token tok, std::chrono::seconds budget = 60s) {
        TxnResult out;
        if (r != rc::OpResult::Accepted) {
            ADD_FAILURE() << "txn op refused synchronously: " << static_cast<int>(r);
            return out;
        }
        if (!until([&] { return got_.count(tok) != 0; }, budget)) return out;
        const Done& d = got_[tok];
        out.done = true; out.status = d.c.status; out.errorCode = d.c.errorCode;
        EXPECT_EQ(d.times, 1) << "a txn op completes exactly once";
        return out;
    }
    rc::Token tok() { return nextToken_++; }
    TxnResult init(std::chrono::seconds b = 60s)   { const auto t = tok(); return wait(p_.initTransactions(cq_, t), t, b); }
    TxnResult commit(std::chrono::seconds b = 60s) { const auto t = tok(); return wait(p_.commitTransaction(cq_, t), t, b); }
    TxnResult abort(std::chrono::seconds b = 60s)  { const auto t = tok(); return wait(p_.abortTransaction(cq_, t), t, b); }
    TxnResult flush(std::chrono::seconds b = 60s)  { const auto t = tok(); return wait(p_.flush(cq_, t), t, b); }
    TxnResult sendOffsets(std::span<const rc::OffsetEntry> o, std::string_view group, std::chrono::seconds b = 60s) {
        const auto t = tok();
        return wait(p_.sendOffsetsToTransaction(o, group, cq_, t), t, b);
    }
    /// #125 (KIP-447): the offsets carry the consumer-group member they belong to.
    TxnResult sendOffsets(std::span<const rc::OffsetEntry> o, const rc::GroupMetadata& g, std::chrono::seconds b = 60s) {
        const auto t = tok();
        return wait(p_.sendOffsetsToTransaction(o, g, cq_, t), t, b);
    }
    /// Issue a commit/abort and return its token without waiting (the in-doubt tests).
    rc::Token issueCommit() {
        const auto t = tok();
        EXPECT_EQ(p_.commitTransaction(cq_, t), rc::OpResult::Accepted);
        return t;
    }
    TxnResult await(rc::Token t, std::chrono::seconds b = 60s) {
        TxnResult out;
        if (!until([&] { return got_.count(t) != 0; }, b)) return out;
        out.done = true; out.status = got_[t].c.status; out.errorCode = got_[t].c.errorCode;
        return out;
    }
    void begin() { p_.beginTransaction(); }

    // ---- the data paths ----
    /// Issue `op` for `id` (batch ops take id, id+1, id+2).  Adds what a reader must see to `exp`.  Returns the
    /// number of ids used.
    int issue(mx::Op op, int64_t id, Expected& exp) {
        const rc::Key key(id);
        const std::string v = valueFor(id);
        switch (op) {
        case mx::Op::Send:
            p_.send(th_, key, bytes(v));
            exp.values.insert(v);
            return 1;
        case mx::Op::SendTo:
            p_.sendTo(th_, static_cast<int32_t>(id % parts_), key, bytes(v));
            exp.values.insert(v);
            return 1;
        case mx::Op::TrySendCq: {
            const rc::Token t = tok();
            cqTokens_.insert(t);
            EXPECT_EQ(spin([&] { return p_.trySend(th_, key, bytes(v), cq_, t); }), rc::SendResult::Ok);
            exp.values.insert(v);
            return 1;
        }
        case mx::Op::TrySendAck: {
            const rc::Token t = tok();
            ackTokens_.insert(t);
            EXPECT_EQ(spin([&] { return p_.trySend(th_, key, bytes(v), rc::AckToken{t}); }), rc::SendResult::Ok);
            exp.values.insert(v);
            return 1;
        }
        case mx::Op::Batch: {
            rc::BatchBuilder b = p_.batch(th_, key);
            for (int i = 0; i < 3; ++i) {
                const std::string vi = valueFor(id + i);
                b.add(bytes(vi));
                exp.values.insert(vi);
            }
            EXPECT_EQ(b.send(), rc::OpResult::Accepted);
            return 3;
        }
        case mx::Op::TryClaim:
            EXPECT_EQ(spin([&] {
                          rc::Claim c = p_.tryClaim(th_, key, static_cast<uint32_t>(kLen));
                          if (!c) return c.result();
                          std::memcpy(c.buffer().data(), v.data(), kLen);
                          return c.commit() ? rc::SendResult::Ok : rc::SendResult::Closed;
                      }),
                      rc::SendResult::Ok);
            exp.values.insert(v);
            return 1;
        case mx::Op::ClaimBatch:
            EXPECT_EQ(spin([&] {
                          rc::BatchClaim c = p_.claimBatch(th_, key, 3, static_cast<uint32_t>(kLen));
                          if (!c) return c.result();
                          for (uint32_t i = 0; i < 3; ++i) {
                              const std::string vi = valueFor(id + i);
                              std::memcpy(c.record(i).data(), vi.data(), kLen);
                          }
                          return c.commit() ? rc::SendResult::Ok : rc::SendResult::Closed;
                      }),
                      rc::SendResult::Ok);
            for (int i = 0; i < 3; ++i) exp.values.insert(valueFor(id + i));
            return 3;
        case mx::Op::TypedSend: {
            if (typed_ == nullptr) throw std::logic_error("no typed topic");
            Position rec;
            fill(rec, id);
            typed_->send(rec);
            exp.keys.insert(keyBytes(rc::Key(id)));
            return 1;
        }
        case mx::Op::TypedClaim: {
            if (typed_ == nullptr) throw std::logic_error("no typed topic");
            bool done = false;
            for (const auto dl = Clock::now() + 30s; !done && Clock::now() < dl;) {
                auto w = typed_->claim(id);
                if (w) {
                    w.zone(3).x(1.5).y(-2.25).z(1e9).flags(0xBEEF).seq(7).tick(9).alive(true).kind(2).tag("claim");
                    EXPECT_TRUE(w.commit());
                    done = true;
                } else if (w.result() == rc::SendResult::BackPressured || w.result() == rc::SendResult::ClaimHeld) {
                    step();
                } else {
                    ADD_FAILURE() << "typed claim refused: " << static_cast<int>(w.result());
                    break;
                }
            }
            if (done) exp.keys.insert(keyBytes(rc::Key(id)));
            return 1;
        }
        case mx::Op::SendOffsets:
            break;
        }
        throw std::logic_error("not a data op");
    }

    /// A committed sentinel transaction: one record on every partition (of the untyped topic, and of the typed one
    /// when there is one).  Once a read_committed reader saw these, everything before them was resolved.
    TxnResult sentinel(Expected& exp, int64_t& cursor) {
        begin();
        for (int32_t part = 0; part < parts_; ++part) {
            (void)issue(mx::Op::Send, idForPartition(parts_, part, cursor), exp);
            if (typed_ != nullptr) (void)issue(mx::Op::TypedSend, idForPartition(parts_, part, cursor), exp);
        }
        return commit();
    }

    /// Every record-ack token completed exactly once, Ok.
    void expectAcksOk(const char* what) {
        EXPECT_TRUE(until([&] { return acksDone(); })) << what << ": not every record ack arrived";
        for (const rc::Token t : cqTokens_) {
            const auto it = got_.find(t);
            ASSERT_NE(it, got_.end()) << what << ": cq token " << t << " never completed";
            EXPECT_EQ(it->second.times, 1) << what << ": cq token " << t;
            EXPECT_EQ(it->second.c.status, rc::OpStatus::Ok) << what << ": cq token " << t;
        }
        for (const rc::Token t : ackTokens_) {
            const auto it = ackDone_.find(t);
            ASSERT_NE(it, ackDone_.end()) << what << ": ack token " << t << " never completed";
            EXPECT_EQ(it->second.times, 1) << what << ": ack token " << t;
            EXPECT_EQ(it->second.status, rc::OpStatus::Ok) << what << ": ack token " << t;
        }
    }
    [[nodiscard]] bool acksDone() const {
        for (const rc::Token t : cqTokens_) if (got_.count(t) == 0) return false;
        for (const rc::Token t : ackTokens_) if (ackDone_.count(t) == 0) return false;
        return true;
    }
    [[nodiscard]] rc::Producer& producer() { return p_; }
    [[nodiscard]] rc::Client& client() { return client_; }

private:
    struct Done { int times = 0; rc::Completion c{}; };
    struct AckDone { int times = 0; rc::OpStatus status = rc::OpStatus::Closed; uint16_t code = 0; };

    static void fill(Position& rec, int64_t id) {
        rec.userId = id; rec.zone = 3; rec.x = 1.5; rec.y = -2.25; rec.z = 1e9; rec.flags = 0xBEEF; rec.seq = 7;
        rec.tick = 9; rec.alive = true; rec.kind = 2;
        std::memcpy(rec.tag.data(), "send", 4);
    }
    static void onAck(void* ctx, rc::Token t, int64_t, rc::OpStatus s, uint16_t ec) noexcept {
        auto& d = static_cast<Writer*>(ctx)->ackDone_[t];
        ++d.times; d.status = s; d.code = ec;
    }
    template <class F>
    rc::SendResult spin(F&& f) {
        rc::SendResult r = rc::SendResult::BackPressured;
        for (const auto dl = Clock::now() + 30s; Clock::now() < dl;) {
            r = f();
            if (r != rc::SendResult::BackPressured && r != rc::SendResult::QueueFull && r != rc::SendResult::ClaimHeld) return r;
            step();
        }
        return r;
    }

    rc::Client&                 client_;
    rc::Producer&               p_;
    rc::TopicHandle             th_;
    int32_t                     parts_;
    TypedRef*                   typed_;
    rc::Completions             cq_;
    rc::Token                   nextToken_ = 1;
    std::vector<rc::Client*>    extra_;
    std::map<rc::Token, Done>   got_;
    std::map<rc::Token, AckDone> ackDone_;
    std::set<rc::Token>         cqTokens_, ackTokens_;
};

rc::ProducerConfig txnConfig(const std::string& txnId) {
    rc::ProducerConfig p;
    p.acks                   = rc::Acks::All;
    p.lingerUs               = 1;
    p.batchBytes             = 1;
    p.maxInFlight            = 16;
    p.produceTimeoutMs       = 30'000;
    p.deliveryTimeoutMs      = 40'000;
    p.transactionOpTimeoutMs = 25'000;
    p.transactionalId        = txnId;
    return p;
}
rc::ProducerConfig plainConfig() {
    auto p = txnConfig({});
    p.transactionOpTimeoutMs = 60'000;
    return p;
}

/// The batch producer ids stored in one partition (read on the owner thread of the node leading it).
std::set<int64_t> storedProducerIds(rune::node::BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onOwnerThreadForTest(topicId, partition, [](rune::replication::PartitionReplica& r) {
        std::set<int64_t> ids;
        rune::storage::Log& log = *r.log();
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
                if (!reader.isControl()) {
                    ids.insert(reader.producerId());
                    reader.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) {
                        if (off >= next) next = off + 1;
                    });
                }
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return ids;
    });
}

/// After a crash: every running broker agrees on one live leader for every partition of the topics (so a reader
/// created now routes its Subscribes to live leaders).
void awaitSettled(Env& e, std::initializer_list<rc::TopicSpec> topics) {
    for (const rc::TopicSpec& t : topics) {
        std::vector<int32_t> leaders;
        if (!awaitLeaders(e.c, t.topicId, t.partitionCount, leaders, 60s)) throw std::runtime_error("leaders never settled: " + t.name);
    }
}

}  // namespace

// =========================================================================================================
// 1. Every produce path, committed and aborted.
// =========================================================================================================
TEST(EosBrokerRegression, EveryPathCommittedIsVisibleOnceAbortedNeverRegression) {
    Env e("eos_paths");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    const rc::TopicSpec typed = e.defineTyped("eos.pos", kParts);
    const int seed = e.otherThan(e.ctl);
    auto cl = e.open(seed, {plain, typed});
    rc::Producer p = cl->producer(txnConfig("eos-paths"));
    TypedRef tt = cl->openTopicAndWait<Position>("eos.pos", p, 30s);
    Writer w(*cl, p, cl->topic("eos.plain"), kParts, &tt);
    ASSERT_TRUE(w.init().ok());

    Expected committed, aborted;
    int64_t id = 1000;
    for (const mx::Op op : kDataOps) {
        SCOPED_TRACE(mx::name(op));
        ASSERT_EQ(mx::expect(op, mx::Entity::TxnNewPartition, mx::Timing::Steady), mx::Outcome::CommittedVisible);
        w.begin();
        id += w.issue(op, id, committed);
        const TxnResult c = w.commit();
        ASSERT_TRUE(c.ok()) << "commit: " << c.str();
        w.begin();
        id += w.issue(op, id, aborted);
        ASSERT_TRUE(w.flush().ok());   // the records really reach the log before the abort (else "invisible" is vacuous)
        const TxnResult a = w.abort();
        ASSERT_TRUE(a.ok()) << "abort: " << a.str();
    }
    int64_t cursor = 500'000;
    ASSERT_TRUE(w.sentinel(committed, cursor).ok());
    w.expectAcksOk("every path");
    {   // #112: the partitions led away from the coordinator verified their registrations over the cluster plane.
        uint64_t forwarded = 0, served = 0;
        for (const int id : e.c.runningNodes()) {
            rune::node::BrokerNode& n = e.c.node(id);
            forwarded += n.onControlThreadForTest([&] { return n.gatewayForTest()->txnVerifiesForwardedForTest(); });
            served += n.onControlThreadForTest([&] { return n.gatewayForTest()->txnVerifiesServedForPeersForTest(); });
        }
        EXPECT_GT(forwarded, 0u) << "no partition leader asked a remote coordinator (the cross-node verification)";
        EXPECT_GT(served, 0u) << "the coordinator never answered a remote leader's verification";
    }

    const int via = e.otherThan(seed);
    {   // read_committed: exactly the committed records, each once - no aborted one.
        Reader rcRead(e, via, {plain, typed}, typed.topicId, rc::Isolation::ReadCommitted);
        ASSERT_TRUE(rcRead.await(committed)) << "read_committed never delivered every committed record";
        expectExactlyOnce(rcRead.seen, committed, "read_committed");
    }
    {   // read_uncommitted: the aborted records DID reach the log (the invisibility above is not vacuous).
        Expected all = committed;
        all.merge(aborted);
        Reader ru(e, via, {plain, typed}, typed.topicId, rc::Isolation::ReadUncommitted);
        ASSERT_TRUE(ru.await(all)) << "read_uncommitted never delivered the aborted records";
    }
    p.close();
    cl->close();
}

// =========================================================================================================
// 2. All nine paths mixed inside one transaction.
// =========================================================================================================
TEST(EosBrokerRegression, MixedPathsInOneTransactionRegression) {
    Env e("eos_mixed");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    const rc::TopicSpec typed = e.defineTyped("eos.pos", kParts);
    auto cl = e.open(e.otherThan(e.ctl), {plain, typed});
    rc::Producer p = cl->producer(txnConfig("eos-mixed"));
    TypedRef tt = cl->openTopicAndWait<Position>("eos.pos", p, 30s);
    Writer w(*cl, p, cl->topic("eos.plain"), kParts, &tt);
    ASSERT_TRUE(w.init().ok());

    Expected committed, aborted;
    int64_t id = 10'000;
    for (int round = 0; round < 3; ++round) {   // commit, abort, commit
        const bool commitIt = round != 1;
        w.begin();
        // Interleave the paths so one batch in flight, one claim and one void send share partitions.
        for (int pass = 0; pass < 2; ++pass)
            for (const mx::Op op : kDataOps) id += w.issue(op, id, commitIt ? committed : aborted);
        if (!commitIt) ASSERT_TRUE(w.flush().ok());   // the aborted records really reach the log first
        const TxnResult r = commitIt ? w.commit() : w.abort();
        ASSERT_TRUE(r.ok()) << (commitIt ? "commit: " : "abort: ") << r.str();
    }
    int64_t cursor = 500'000;
    ASSERT_TRUE(w.sentinel(committed, cursor).ok());
    w.expectAcksOk("mixed");

    const int via = e.otherThan(e.ctl);
    Reader rcRead(e, via, {plain, typed}, typed.topicId, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(committed));
    expectExactlyOnce(rcRead.seen, committed, "read_committed");
    Expected all = committed;
    all.merge(aborted);
    Reader ru(e, via, {plain, typed}, typed.topicId, rc::Isolation::ReadUncommitted);
    ASSERT_TRUE(ru.await(all));
    p.close();
    cl->close();
}

// =========================================================================================================
// 3. A zombie producer is fenced and its records are invisible.
// =========================================================================================================
TEST(EosBrokerRegression, ZombieIsFencedAndItsRecordsAreInvisibleRegression) {
    Env e("eos_zombie");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    const int sa = e.otherThan(e.ctl), sb = e.otherThan(sa), sr = e.otherThan(sa, sb);
    auto ca = e.open(sa, {plain});
    auto cb = e.open(sb, {plain});
    rc::Producer pa = ca->producer(txnConfig("eos-zombie"));
    rc::Producer pb = cb->producer(txnConfig("eos-zombie"));
    Writer wa(*ca, pa, ca->topic("eos.plain"), kParts);
    Writer wb(*cb, pb, cb->topic("eos.plain"), kParts);
    wa.alsoPump(cb.get());
    wb.alsoPump(ca.get());

    ASSERT_TRUE(wa.init().ok());
    // A: a transaction with records on every partition, flushed into the log (acked by the partition leaders).
    Expected aRecs, bRecs;
    wa.begin();
    int64_t cur = 100;
    for (int rep = 0; rep < 3; ++rep)
        for (int32_t part = 0; part < kParts; ++part) (void)wa.issue(mx::Op::Send, idForPartition(kParts, part, cur), aRecs);
    ASSERT_TRUE(wa.flush().ok());

    // B: the same transactionalId initializes -> A's epoch is dead.
    ASSERT_TRUE(wb.init().ok());
    wb.begin();
    cur = 1000;
    for (int rep = 0; rep < 3; ++rep)
        for (int32_t part = 0; part < kParts; ++part) (void)wb.issue(mx::Op::Send, idForPartition(kParts, part, cur), bRecs);

    // The zombie's commit must not succeed.  The matrix says Fenced (R9); a real partition leader may answer a
    // fenced transactional produce with InvalidProducerEpoch (17) first, which makes A's txn abortable and its
    // abort meet 18 - accepted, and recorded as a divergence.
    ASSERT_EQ(mx::expect(mx::Op::Send, mx::Entity::TxnAddedPartition, mx::Timing::Fenced), mx::Outcome::Fenced);
    const TxnResult ac = wa.commit();
    ASSERT_TRUE(ac.done) << "the zombie's commit must complete (never hang)";
    EXPECT_NE(ac.status, rc::OpStatus::Ok) << "the zombie's commit must not succeed";
    const bool fencedStatus = ac.status == rc::OpStatus::Fenced;
    const bool fencedCode   = ac.status == rc::OpStatus::BrokerError && (ac.errorCode == 17 || ac.errorCode == 18);
    EXPECT_TRUE(fencedStatus || fencedCode) << "zombie commit: " << ac.str();
    if (!fencedStatus)
        std::fprintf(stderr, "[eos][divergence] zombie commit: matrix R9 says Fenced, the real broker gave %s\n",
                     ac.str().c_str());
    const TxnResult aa = wa.abort();
    EXPECT_TRUE(aa.done) << "the zombie's abort must complete (never hang)";
    EXPECT_NE(aa.status, rc::OpStatus::Ok) << "a fenced producer cannot abort successfully: " << aa.str();

    // B commits its own records.
    ASSERT_TRUE(wb.commit().ok());
    int64_t cursor = 500'000;
    ASSERT_TRUE(wb.sentinel(bRecs, cursor).ok());

    Reader rcRead(e, sr, {plain}, -1, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(bRecs)) << "read_committed never delivered B's records";
    expectExactlyOnce(rcRead.seen, bRecs, "read_committed (A's records must be absent)");
    Expected all = bRecs;
    all.merge(aRecs);
    Reader ru(e, sr, {plain}, -1, rc::Isolation::ReadUncommitted);
    ASSERT_TRUE(ru.await(all)) << "the zombie's records must be in the log (else the check above is vacuous)";
    pa.closeImmediately();
    pb.close();
    ca->close();
    cb->close();
}

// =========================================================================================================
// 4. An in-doubt commit: the coordinator (and the seed) crashes after EndTxn was offered; the commit times out and
//    its retry commits the records exactly once.
// =========================================================================================================
TEST(EosBrokerRegression, InDoubtCommitRetryCommitsOnceRegression) {
    Env e("eos_indoubt");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    const int ctl = e.ctl;
    auto cl = e.open(ctl, {plain});   // the coordinator is the seed's broker
    // The session is declared stale after ~10 s; an op timeout below that makes the first commit a deterministic
    // Timeout (InDoubt), so the RETRY branch always runs.
    auto icfg = txnConfig("eos-indoubt");
    icfg.transactionOpTimeoutMs = 7'000;
    rc::Producer p = cl->producer(icfg);
    Writer w(*cl, p, cl->topic("eos.plain"), kParts);
    ASSERT_TRUE(w.init().ok());

    Expected recs;
    w.begin();
    int64_t cur = 100;
    for (int rep = 0; rep < 3; ++rep)
        for (int32_t part = 0; part < kParts; ++part) (void)w.issue(mx::Op::Send, idForPartition(kParts, part, cur), recs);
    ASSERT_TRUE(w.flush().ok());

    const rc::Token ct = w.issueCommit();
    (void)cl->doWork();                     // the EndTxn is offered on the wire; no answer is read yet
    const auto t0 = Clock::now();
    e.c.crash(ctl);                         // the coordinator AND the seed
    TxnResult first = w.await(ct, 90s);
    const auto took = Clock::now() - t0;
    ASSERT_TRUE(first.done) << "the in-doubt commit must complete, not hang";
    EXPECT_EQ(first.status, rc::OpStatus::Timeout) << "the commit's answer cannot arrive within the op timeout: " << first.str();
    EXPECT_LT(took, 12s) << "the Timeout is the op deadline, not a hang";
    const TxnResult retry = w.commit(90s);   // the in-doubt commit is retried: EndTxn(commit) is idempotent
    EXPECT_TRUE(retry.ok()) << "the retried commit must be Ok, got " << retry.str();
    int64_t cursor = 500'000;
    ASSERT_TRUE(w.sentinel(recs, cursor).ok());

    const int via = e.otherThan(ctl);
    awaitSettled(e, {plain});
    Reader rcRead(e, via, {plain}, -1, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(recs));
    expectExactlyOnce(rcRead.seen, recs, "read_committed after the in-doubt commit");
    p.close();
    cl->close();
}

// =========================================================================================================
// 5. A data partition leader crashes with batches of an open transaction in flight.
// =========================================================================================================
TEST(EosBrokerRegression, LeaderCrashMidTransactionResendsAndCommitsOnceRegression) {
    Env e("eos_leader");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    // The victim leads a data partition but is NOT the controller (the transaction coordinator): this test is
    // about a lost DATA leader; the coordinator crash is the in-doubt test's.
    int32_t victim = -1;
    for (int32_t part = 0; part < kParts && victim < 0; ++part) {
        const int32_t l = e.leaderOfPartition(plain.topicId, part);
        if (l != e.ctl) victim = l;
    }
    ASSERT_GT(victim, 0) << "some partition is led by a non-controller broker";
    const int seed = e.otherThan(victim, e.ctl);
    ASSERT_GT(seed, 0);
    auto cl = e.open(seed, {plain});
    rc::Producer p = cl->producer(txnConfig("eos-leader"));
    Writer w(*cl, p, cl->topic("eos.plain"), kParts);
    ASSERT_TRUE(w.init().ok());

    constexpr mx::Op kMix[] = {mx::Op::Send, mx::Op::TrySendCq, mx::Op::Batch, mx::Op::TryClaim, mx::Op::ClaimBatch};
    Expected recs;
    w.begin();
    int64_t id = 1000;
    auto sendSome = [&](int n) {
        for (int i = 0; i < n; ++i) id += w.issue(kMix[static_cast<std::size_t>(id) % 5], id, recs);
    };
    sendSome(60);
    for (int i = 0; i < 20; ++i) w.step();   // some of it acked, some of it in flight
    sendSome(60);
    (void)cl->doWork();                       // one pass: frames to the victim are on the wire, unacked
    e.c.crash(victim);
    sendSome(120);                            // sent while the leader is gone: re-routed to the new leader
    const TxnResult c = w.commit(150s);
    ASSERT_TRUE(c.ok()) << "the commit after a data-leader crash: " << c.str();
    int64_t cursor = 500'000;
    ASSERT_TRUE(w.sentinel(recs, cursor).ok());
    w.expectAcksOk("leader crash");

    awaitSettled(e, {plain});
    Reader rcRead(e, seed, {plain}, -1, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(recs));
    expectExactlyOnce(rcRead.seen, recs, "read_committed after the leader crash");
    p.close();
    cl->close();
}

// =========================================================================================================
// 6. consume -> transform -> produce, every 5th transaction aborted, rewound with seekToCommitted, redone.
// =========================================================================================================
TEST(EosBrokerRegression, AbortRewindRedoOutputEqualsInputRegression) {
    constexpr int kInput = 10'000;
    constexpr int kBatch = 200;
    Env e("eos_loop");
    const rc::TopicSpec in  = e.define("eos.in", kParts);
    const rc::TopicSpec out = e.define("eos.out", kParts);

    // The input: kInput plain acked records.
    {
        auto ci = e.open(e.otherThan(e.ctl), {in});
        rc::Producer pi = ci->producer(plainConfig());
        Writer wi(*ci, pi, ci->topic("eos.in"), kParts);
        Expected sink;
        for (int i = 0; i < kInput; ++i) (void)wi.issue(mx::Op::TrySendCq, i, sink);
        wi.expectAcksOk("input");
        pi.close();
        ci->close();
    }

    // The consumer (group, manual offsets, read_committed) and the transactional producer.
    struct Item { int32_t partition; int64_t offset; int64_t id; };
    std::vector<Item> pending;
    auto cc = std::make_unique<rc::Client>(e.cfg(e.otherThan(e.ctl), {in}));
    rc::GroupConfig gcfg;
    gcfg.autoCommit        = false;
    gcfg.autoOffsetReset   = rc::OffsetReset::Earliest;
    gcfg.isolation         = rc::IsolationLevel::ReadCommitted;
    gcfg.sessionTimeoutMs  = 30'000;
    gcfg.commitSyncTimeoutMs = 30'000;
    rc::GroupConsumer gc = cc->subscribeGroup("eos-g", {"eos.in"}, gcfg);
    gc.onRecords([&](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
            pending.push_back(Item{rs.partition, off, idOf(v)});
        });
    });
    cc->connectAndWait(30s);

    auto cp = e.open(e.otherThan(e.ctl), {out});
    rc::Producer pp = cp->producer(txnConfig("eos-loop"));
    Writer w(*cp, pp, cp->topic("eos.out"), kParts);
    w.alsoPump(cc.get());
    ASSERT_TRUE(w.init().ok());

    constexpr mx::Op kPaths[] = {mx::Op::Send, mx::Op::TrySendCq, mx::Op::TryClaim};
    Expected want;
    int committed = 0, attempts = 0, aborts = 0;
    while (committed < kInput) {
        ASSERT_TRUE(w.until([&] { return static_cast<int>(pending.size()) >= kBatch || committed + static_cast<int>(pending.size()) >= kInput; }, 120s))
            << "the consumer stalled at committed=" << committed << " pending=" << pending.size();
        const auto n = std::min<std::size_t>(kBatch, pending.size());
        std::vector<Item> take(pending.begin(), pending.begin() + static_cast<std::ptrdiff_t>(n));   // a prefix: contiguous per partition
        pending.erase(pending.begin(), pending.begin() + static_cast<std::ptrdiff_t>(n));
        w.begin();
        Expected scratch;
        std::map<int32_t, int64_t> next;   // partition -> offset to commit
        for (const Item& it : take) {
            ASSERT_GE(it.id, 0);
            (void)w.issue(kPaths[static_cast<std::size_t>(it.id) % 3], it.id, scratch);
            next[it.partition] = std::max(next[it.partition], it.offset + 1);
        }
        std::vector<rc::OffsetEntry> offs;
        for (const auto& [part, off] : next) offs.push_back(rc::OffsetEntry{in.topicId, part, off});
        ASSERT_TRUE(w.sendOffsets(offs, "eos-g").ok()) << "sendOffsetsToTransaction";
        ++attempts;
        if (attempts % 5 == 0) {
            ASSERT_TRUE(w.abort().ok());
            // Everything delivered before the rewind applies is stale; the command applies at issue (Invoker owner),
            // so what arrives while the wait pumps is the NEW stream and must be kept.
            pending.clear();
            gc.seekToCommittedAndWait(60s);   // rewind to the committed offsets, without a restart
            ++aborts;
        } else {
            ASSERT_TRUE(w.commit().ok());
            committed += static_cast<int>(take.size());
            want.merge(scratch);
        }
    }
    EXPECT_GE(aborts, 10) << "the loop must really abort and redo";
    EXPECT_EQ(committed, kInput);
    EXPECT_EQ(static_cast<int>(want.values.size()), kInput) << "each input id produced exactly once by a committed txn";
    int64_t cursor = 5'000'000;
    ASSERT_TRUE(w.sentinel(want, cursor).ok());
    w.expectAcksOk("loop");

    {
        Reader rcRead(e, e.otherThan(e.ctl), {out}, -1, rc::Isolation::ReadCommitted);
        ASSERT_TRUE(rcRead.await(want, 120s));
        expectExactlyOnce(rcRead.seen, want, "output == input");
    }
    gc.close();
    pp.close();
    cp->close();
    cc->close();
}

// =========================================================================================================
// 7. A produce outside a transaction is refused by the client and never reaches the broker.
// =========================================================================================================
TEST(EosBrokerRegression, ProduceOutsideATransactionIsRefusedAgainstARealBrokerRegression) {
    // The refusal rules (R4 BeforeInit, R5 OutsideTxn, R6 AfterCommitIssued) from the matrix, not hard-coded.
    for (const mx::Timing t : {mx::Timing::BeforeInit, mx::Timing::OutsideTxn, mx::Timing::AfterCommitIssued})
        ASSERT_EQ(mx::expect(mx::Op::Send, mx::Entity::TxnAddedPartition, t), mx::Outcome::Refused) << mx::name(t);

    Env e("eos_refuse");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    const rc::TopicSpec typed = e.defineTyped("eos.pos", kParts);
    auto cl = e.open(e.otherThan(e.ctl), {plain, typed});
    rc::Producer p = cl->producer(txnConfig("eos-refuse"));
    TypedRef tt = cl->openTopicAndWait<Position>("eos.pos", p, 30s);
    Writer w(*cl, p, cl->topic("eos.plain"), kParts, &tt);
    const rc::TopicHandle th = cl->topic("eos.plain");
    rc::Completions cq(*cl);

    auto expectRefused = [&](const char* when) {
        SCOPED_TRACE(when);
        const std::string v = valueFor(7);
        const rc::Key key(int64_t{7});
        EXPECT_EQ(p.trySend(th, key, bytes(v)), rc::SendResult::NotInTransaction);
        EXPECT_EQ(p.trySend(th, key, bytes(v), cq, 1), rc::SendResult::NotInTransaction);
        EXPECT_EQ(p.trySend(th, key, bytes(v), rc::AckToken{2}), rc::SendResult::NotInTransaction);
        EXPECT_EQ(p.trySendTo(th, 0, key, bytes(v)), rc::SendResult::NotInTransaction);
        EXPECT_THROW(p.send(th, key, bytes(v)), std::logic_error);
        EXPECT_THROW(p.sendTo(th, 0, key, bytes(v)), std::logic_error);
        {
            rc::Claim c = p.tryClaim(th, key, static_cast<uint32_t>(kLen));
            EXPECT_FALSE(static_cast<bool>(c));
            EXPECT_EQ(c.result(), rc::SendResult::NotInTransaction);
        }
        {
            rc::BatchClaim c = p.claimBatch(th, key, 3, static_cast<uint32_t>(kLen));
            EXPECT_FALSE(static_cast<bool>(c));
            EXPECT_EQ(c.result(), rc::SendResult::NotInTransaction);
        }
        {
            auto c = tt.claim(int64_t{7});
            EXPECT_FALSE(static_cast<bool>(c));
            EXPECT_EQ(c.result(), rc::SendResult::NotInTransaction);
        }
        Position rec;
        rec.userId = 7;
        EXPECT_THROW(tt.send(rec), std::logic_error);
    };

    expectRefused("before initTransactions");
    ASSERT_TRUE(w.init().ok());
    expectRefused("initialized, no transaction");

    Expected recs;
    w.begin();
    (void)w.issue(mx::Op::Send, 100, recs);
    const rc::Token ct = w.issueCommit();
    expectRefused("commit issued, not completed");
    ASSERT_TRUE(w.await(ct).ok());
    expectRefused("after the commit");

    // A second transaction proves the producer is healthy; the sentinel resolves everything before it.
    int64_t cursor = 500'000;
    ASSERT_TRUE(w.sentinel(recs, cursor).ok());

    Reader rcRead(e, e.otherThan(e.ctl), {plain, typed}, typed.topicId, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(recs));
    expectExactlyOnce(rcRead.seen, recs, "read_committed: only the transactional records");
    Reader ru(e, e.otherThan(e.ctl), {plain, typed}, typed.topicId, rc::Isolation::ReadUncommitted);
    ASSERT_TRUE(ru.await(recs));
    expectExactlyOnce(ru.seen, recs, "read_uncommitted: a refused produce wrote nothing");
    p.close();
    cl->close();
}

// =========================================================================================================
// 8. A plain acked producer: a batch the crashed leader answered but the client never saw is re-sent after the
//    reconnect and deduplicated under the broker-assigned id.
// =========================================================================================================
TEST(ProducerIdBrokerRegression, RetriedBatchAcrossAReconnectIsDeduplicatedRegression) {
    Env e("pid_dedup");
    const rc::TopicSpec plain = e.define("pid.plain", kParts);
    const int32_t victim = e.leaderOfPartition(plain.topicId, 0);
    ASSERT_GT(victim, 0);
    auto cl = e.open(victim, {plain});   // the seed IS the leader: its death is a session loss
    rc::Producer p = cl->producer(plainConfig());
    Writer w(*cl, p, cl->topic("pid.plain"), kParts);

    Expected recs;
    int64_t cur = 1000;
    // Everything onto the victim's partition: many one-record batches (maxInFlight 16) are in flight when it dies.
    for (int i = 0; i < 200; ++i) (void)w.issue(mx::Op::TrySendCq, idForPartition(kParts, 0, cur), recs);
    const int64_t pid0 = p.producerId();
    EXPECT_GE(pid0, 0) << "the id is the broker's";
    (void)cl->doWork();   // the unacked frames are on the wire
    e.c.crash(victim);
    w.expectAcksOk("dedup");
    EXPECT_GT(p.framesResent(), 0u) << "frames were RE-SENT after the reconnect: the dedup below is not vacuous";
    EXPECT_EQ(p.producerId(), pid0) << "a reconnect keeps the broker-assigned id";

    const int via = e.otherThan(victim);
    awaitSettled(e, {plain});
    Reader ru(e, via, {plain}, -1, rc::Isolation::ReadUncommitted);
    ASSERT_TRUE(ru.await(recs));
    // Let the stream settle: a duplicate batch would be delivered right behind the original.
    for (int i = 0; i < 2000; ++i) { (void)ru.client->doWork(); std::this_thread::yield(); }
    expectExactlyOnce(ru.seen, recs, "the log holds one copy of every record");
    std::set<int64_t> ids;
    for (int32_t part = 0; part < kParts; ++part) {
        const int32_t l = e.leaderOfPartition(plain.topicId, part);
        const auto s = storedProducerIds(e.c.node(l), plain.topicId, part);
        ids.insert(s.begin(), s.end());
    }
    EXPECT_EQ(ids, (std::set<int64_t>{pid0})) << "every stored batch carries the broker-assigned id";
    p.close();
    cl->close();
}

// =========================================================================================================
// 9. The matrix's real-broker plain slice: PlainAcked x 9 ops x {Steady, SessionLostMid}.
// =========================================================================================================
TEST(ProducerIdBrokerRegression, PlainAckedEveryPathRegression) {
    // The outcomes come from the matrix (R1 / R2), for every one of the 18 cells.
    for (const mx::Op op : kDataOps) {
        ASSERT_FALSE(mx::naRule(op, mx::Entity::PlainAcked, mx::Timing::Steady).has_value());
        ASSERT_FALSE(mx::naRule(op, mx::Entity::PlainAcked, mx::Timing::SessionLostMid).has_value());
        ASSERT_EQ(mx::expect(op, mx::Entity::PlainAcked, mx::Timing::Steady), mx::Outcome::Delivered);
        ASSERT_EQ(mx::expect(op, mx::Entity::PlainAcked, mx::Timing::SessionLostMid), mx::Outcome::DeliveredOnce);
    }
    Env e("pid_plain");
    const rc::TopicSpec plain = e.define("pid.plain", kParts);
    const rc::TopicSpec typed = e.defineTyped("pid.pos", kParts);
    const int32_t victim = e.leaderOfPartition(plain.topicId, 0);
    ASSERT_GT(victim, 0);
    auto cl = e.open(victim, {plain, typed});
    rc::Producer p = cl->producer(plainConfig());
    TypedRef tt = cl->openTopicAndWait<Position>("pid.pos", p, 30s);
    Writer w(*cl, p, cl->topic("pid.plain"), kParts, &tt);

    Expected recs;
    int64_t id = 1000;
    // Steady (R1): every path delivers; acked forms complete Ok.
    for (const mx::Op op : kDataOps) id += w.issue(op, id, recs);
    w.expectAcksOk("steady");
    p.flush();
    for (int i = 0; i < 300; ++i) w.step();   // the paths with no ack token are on the wire and answered
    const uint64_t resentSteady = p.framesResent();
    const int64_t pid0 = p.producerId();
    EXPECT_GE(pid0, 0);
    EXPECT_NE(pid0, rc::producer::kRigProducerId);

    // SessionLostMid (R2): all nine paths issued, frames on the wire, the seed/leader dies.
    for (const mx::Op op : kDataOps) id += w.issue(op, id, recs);
    (void)cl->doWork();
    e.c.crash(victim);
    w.expectAcksOk("session lost mid");
    p.flush();   // a plain producer: seal what is batched; the readers below keep pumping this client
    EXPECT_EQ(p.producerId(), pid0);

    const int via = e.otherThan(victim);
    awaitSettled(e, {plain, typed});
    Reader ru(e, via, {plain, typed}, typed.topicId, rc::Isolation::ReadUncommitted);
    ASSERT_TRUE(ru.await(recs, 90s, cl.get()));
    Reader rcRead(e, via, {plain, typed}, typed.topicId, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(recs, 90s, cl.get()));
    for (int i = 0; i < 2000; ++i) { (void)ru.client->doWork(); (void)rcRead.client->doWork(); std::this_thread::yield(); }
    EXPECT_GT(p.framesResent(), resentSteady) << "frames were RE-SENT after the session loss: the exactly-once "
                                                 "checks are not vacuous";
    expectExactlyOnce(ru.seen, recs, "read_uncommitted: every record once");
    expectExactlyOnce(rcRead.seen, recs, "read_committed: every record once");

    std::set<int64_t> ids;
    for (const rc::TopicSpec& t : {plain, typed})
        for (int32_t part = 0; part < kParts; ++part) {
            const auto s = storedProducerIds(e.c.node(e.leaderOfPartition(t.topicId, part)), t.topicId, part);
            ids.insert(s.begin(), s.end());
        }
    EXPECT_EQ(ids, (std::set<int64_t>{pid0})) << "every batch carries the broker-assigned id";
    p.close();
    cl->close();
}

// =========================================================================================================
// 10. The transaction coordinator (the controller) crashes under a client seeded on ANOTHER broker.
// RED (before the fix): the client's coordinator session pointed at the dead node and gave up only after its
// whole connect budget (minutes): every EndTxn waited on it - the commit hung to its op deadline, and so did
// every retry.  The client now gives a coordinator session that stays down up and asks the seed again.
// =========================================================================================================
TEST(EosBrokerRegression, CoordinatorCrashedUnderANonSeedClientCommitsAfterTheMoveRegression) {
    Env e("eos_coord");
    const rc::TopicSpec plain = e.define("eos.plain", kParts);
    const int seed = e.otherThan(e.ctl);
    auto cl = e.open(seed, {plain});
    rc::Producer p = cl->producer(txnConfig("eos-coord"));
    Writer w(*cl, p, cl->topic("eos.plain"), kParts);
    ASSERT_TRUE(w.init().ok());

    Expected recs;
    w.begin();
    int64_t cur = 100;
    for (int rep = 0; rep < 3; ++rep)
        for (int32_t part = 0; part < kParts; ++part) (void)w.issue(mx::Op::Send, idForPartition(kParts, part, cur), recs);
    ASSERT_TRUE(w.flush().ok());   // the data is in the log, the partitions are in the transaction

    e.c.crash(e.ctl);              // the coordinator dies; the client's seed is a follower
    const auto t0 = Clock::now();
    const TxnResult c = w.commit(60s);
    const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0);
    ASSERT_TRUE(c.ok()) << "the commit after a coordinator crash: " << c.str();
    EXPECT_LT(took, 45s) << "the coordinator was re-discovered, not waited on until the op deadline";
    int64_t cursor = 500'000;
    ASSERT_TRUE(w.sentinel(recs, cursor).ok()) << "the producer keeps working with the new coordinator";

    awaitSettled(e, {plain});
    Reader rcRead(e, seed, {plain}, -1, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(recs));
    expectExactlyOnce(rcRead.seen, recs, "read_committed after the coordinator crash");
    p.close();
    cl->close();
}

// =========================================================================================================
// 11. The offsets of a committed transaction reach the group; those of an aborted one never do.
// RED (before the fix): the broker's transaction coordinator had no offset sink - TxnOffsetCommit was accepted and
// the committed offsets were silently dropped, so seekToCommitted (and every restart) went back to the log start
// and a consume-transform-produce pipeline reprocessed everything.
// =========================================================================================================
TEST(EosBrokerRegression, TransactionalOffsetsReachTheGroupOnlyOnCommitRegression) {
    constexpr int kInput = 1000;
    Env e("eos_offsets");
    const rc::TopicSpec in = e.define("eos.in", kParts);
    {
        auto ci = e.open(e.otherThan(e.ctl), {in});
        rc::Producer pi = ci->producer(plainConfig());
        Writer wi(*ci, pi, ci->topic("eos.in"), kParts);
        Expected sink;
        for (int i = 0; i < kInput; ++i) (void)wi.issue(mx::Op::TrySendCq, i, sink);
        wi.expectAcksOk("input");
        pi.close();
        ci->close();
    }
    struct Range { int64_t first = -1, count = 0; };
    std::map<int32_t, Range> seen;   // partition -> what the consumer was handed since the last reset
    auto cc = std::make_unique<rc::Client>(e.cfg(e.otherThan(e.ctl), {in}));
    rc::GroupConfig gcfg;
    gcfg.autoCommit      = false;   // only the transactions commit offsets
    gcfg.autoOffsetReset = rc::OffsetReset::Earliest;
    rc::GroupConsumer gc = cc->subscribeGroup("eos-offsets-g", {"eos.in"}, gcfg);
    gc.onRecords([&](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) {
            Range& r = seen[rs.partition];
            if (r.count == 0 || off < r.first) r.first = off;
            ++r.count;
        });
    });
    cc->connectAndWait(30s);
    auto total = [&] { int64_t n = 0; for (const auto& [p, r] : seen) n += r.count; return n; };
    auto pumpUntil = [&](auto&& pred) {
        for (const auto dl = Clock::now() + 60s; Clock::now() < dl; std::this_thread::yield()) {
            (void)cc->doWork();
            if (pred()) return true;
        }
        return false;
    };
    ASSERT_TRUE(pumpUntil([&] { return total() >= kInput; })) << "the consumer reads the whole input";
    int64_t perPartition[kParts] = {};
    for (const auto& [p, r] : seen) perPartition[p] = r.count;

    auto cp = e.open(e.otherThan(e.ctl), {in});
    rc::Producer pp = cp->producer(txnConfig("eos-offsets-tx"));
    Writer w(*cp, pp, cp->topic("eos.in"), kParts);
    w.alsoPump(cc.get());
    ASSERT_TRUE(w.init().ok());

    // A transaction that COMMITS p0 = 100: the group's committed offset becomes 100.
    {
        w.begin();
        const std::vector<rc::OffsetEntry> offs{rc::OffsetEntry{in.topicId, 0, 100}};
        ASSERT_TRUE(w.sendOffsets(offs, "eos-offsets-g").ok());
        ASSERT_TRUE(w.commit().ok());
    }
    seen.clear();
    gc.seekToCommittedAndWait(60s);
    ASSERT_TRUE(pumpUntil([&] { return total() >= kInput - 100; }));
    EXPECT_EQ(seen[0].first, 100) << "p0 resumes at the transactionally committed offset";
    EXPECT_EQ(seen[0].count, perPartition[0] - 100);
    for (int32_t p = 1; p < kParts; ++p) EXPECT_EQ(seen[p].first, 0) << "p" << p << " has no committed offset: the log start";

    // A transaction that ABORTS p0 = 200: the committed offset stays 100.
    {
        w.begin();
        const std::vector<rc::OffsetEntry> offs{rc::OffsetEntry{in.topicId, 0, 200}};
        ASSERT_TRUE(w.sendOffsets(offs, "eos-offsets-g").ok());
        ASSERT_TRUE(w.abort().ok());
    }
    seen.clear();
    gc.seekToCommittedAndWait(60s);
    ASSERT_TRUE(pumpUntil([&] { return total() >= kInput - 100; }));
    EXPECT_EQ(seen[0].first, 100) << "the aborted transaction's offsets never reached the group";
    gc.close();
    pp.close();
    cp->close();
    cc->close();
}

// =========================================================================================================
// 12. Issue #125 (KIP-447) against real brokers: offsets sent with the consumer's GroupMetadata commit for the current
// member and generation; a stale generation or an unknown member is fenced (abortable, nothing reaches the group).
// The producer runs on the GroupConsumer's Client (the member is bound to that session).
// =========================================================================================================
TEST(EosBrokerRegression, TxnOffsetsCarryingTheMemberAreFencedByGenerationAndMemberRegression) {
    constexpr int kInput = 200;
    Env e("eos_kip447");
    const rc::TopicSpec in = e.define("eos.in", kParts);
    {
        auto ci = e.open(e.otherThan(e.ctl), {in});
        rc::Producer pi = ci->producer(plainConfig());
        Writer wi(*ci, pi, ci->topic("eos.in"), kParts);
        Expected sink;
        for (int i = 0; i < kInput; ++i) (void)wi.issue(mx::Op::TrySendCq, i, sink);
        wi.expectAcksOk("input");
        pi.close();
        ci->close();
    }
    struct Range { int64_t first = -1, count = 0; };
    std::map<int32_t, Range> seen;
    auto cc = std::make_unique<rc::Client>(e.cfg(e.otherThan(e.ctl), {in}));
    rc::GroupConfig gcfg;
    gcfg.autoCommit      = false;
    gcfg.autoOffsetReset = rc::OffsetReset::Earliest;
    rc::GroupConsumer gc = cc->subscribeGroup("eos-kip447-g", {"eos.in"}, gcfg);
    gc.onRecords([&](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) {
            Range& r = seen[rs.partition];
            if (r.count == 0 || off < r.first) r.first = off;
            ++r.count;
        });
    });
    cc->connectAndWait(30s);
    auto total = [&] { int64_t n = 0; for (const auto& [p, r] : seen) n += r.count; return n; };
    rc::Producer pp = cc->producer(txnConfig("eos-kip447-tx"));   // the consumer's own Client
    Writer w(*cc, pp, cc->topic("eos.in"), kParts);
    ASSERT_TRUE(w.until([&] { return total() >= kInput && !gc.groupMetadata().memberId.empty(); }))
        << "the consumer joined and read the input";
    ASSERT_TRUE(w.init().ok());
    const rc::GroupMetadata meta = gc.groupMetadata();
    ASSERT_EQ(meta.groupId, "eos-kip447-g");
    ASSERT_GT(meta.generationId, 0);

    // The current member and generation: committed.
    {
        w.begin();
        const std::vector<rc::OffsetEntry> offs{rc::OffsetEntry{in.topicId, 0, 10}};
        const TxnResult r = w.sendOffsets(offs, meta);
        ASSERT_TRUE(r.ok()) << r.str();
        ASSERT_TRUE(w.commit().ok());
    }
    // A stale generation and an unknown member: fenced, abortable; the abort ends the transaction cleanly.
    struct Bad { const char* what; rc::GroupMetadata m; uint16_t code; };
    rc::GroupMetadata stale = meta;
    stale.generationId -= 1;
    rc::GroupMetadata stranger = meta;
    stranger.memberId = "eos-kip447-g-999-00000000000000000000000000000000";
    for (const Bad& b : {Bad{"a stale generation", stale, static_cast<uint16_t>(rpc::ErrorCode::IllegalGeneration)},
                         Bad{"an unknown member", stranger, static_cast<uint16_t>(rpc::ErrorCode::UnknownMember)}}) {
        w.begin();
        const std::vector<rc::OffsetEntry> offs{rc::OffsetEntry{in.topicId, 0, 150}};
        const TxnResult r = w.sendOffsets(offs, b.m);
        ASSERT_TRUE(r.done) << b.what;
        EXPECT_EQ(r.status, rc::OpStatus::BrokerError) << b.what << ": " << r.str();
        EXPECT_EQ(r.errorCode, b.code) << b.what;
        EXPECT_EQ(pp.state(), rc::TxnState::AbortableError) << b.what;
        ASSERT_TRUE(w.abort().ok()) << b.what;
    }
    // Only the committed transaction's offset reached the group.
    seen.clear();
    gc.seekToCommittedAndWait(60s);
    ASSERT_TRUE(w.until([&] { return seen.count(0) != 0 && seen[0].count > 0; }));
    EXPECT_EQ(seen[0].first, 10) << "p0 resumes at the committed offset; the fenced ones never landed";
    gc.close();
    pp.close();
    cc->close();
}

// =========================================================================================================
// 12. PR #490 eos-e2e step 3a, in process: consume -> transform -> produce with seekToCommitted rewinds, the
// transaction coordinator (the controller) crashes mid-pipeline under clients seeded on another broker.  Every
// failed transaction is aborted and rewound (seekToCommitted) like eos_probe does; the pipeline must resume and end
// with output == input.  (eos-e2e on 493cc253: seekToCommitted kept answering "the session to the broker was lost"
// and the pipeline gave up.)
// =========================================================================================================
TEST(EosBrokerRegression, PipelineSurvivesTheCoordinatorCrashWithSeekToCommittedRewindsRegression) {
    constexpr int kInput = 4'000;
    constexpr int kBatch = 100;
    Env e("eos_pipe_crash");
    const rc::TopicSpec in  = e.define("eos.in", kParts);
    const rc::TopicSpec out = e.define("eos.out", kParts);
    const int seed = e.otherThan(e.ctl);
    {
        auto ci = e.open(seed, {in});
        rc::Producer pi = ci->producer(plainConfig());
        Writer wi(*ci, pi, ci->topic("eos.in"), kParts);
        Expected sink;
        for (int i = 0; i < kInput; ++i) (void)wi.issue(mx::Op::TrySendCq, i, sink);
        wi.expectAcksOk("input");
        pi.close();
        ci->close();
    }
    struct Item { int32_t partition; int64_t offset; int64_t id; };
    std::vector<Item> pending;
    auto cc = std::make_unique<rc::Client>(e.cfg(seed, {in}));
    rc::GroupConfig gcfg;
    gcfg.autoCommit          = false;
    gcfg.autoOffsetReset     = rc::OffsetReset::Earliest;
    gcfg.isolation           = rc::IsolationLevel::ReadCommitted;
    gcfg.sessionTimeoutMs    = 30'000;
    gcfg.commitSyncTimeoutMs = 30'000;
    rc::GroupConsumer gc = cc->subscribeGroup("eos-crash-g", {"eos.in"}, gcfg);
    gc.onRecords([&](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
            pending.push_back(Item{rs.partition, off, idOf(v)});
        });
    });
    cc->connectAndWait(30s);
    auto cp = e.open(seed, {out});
    rc::Producer pp = cp->producer(txnConfig("eos-crash"));
    Writer w(*cp, pp, cp->topic("eos.out"), kParts);
    w.alsoPump(cc.get());
    ASSERT_TRUE(w.init().ok());

    Expected want;
    int committed = 0, failed = 0, rewindFailures = 0;
    bool crashed = false;
    std::string lastRewindError;
    const auto deadline = Clock::now() + 240s;
    while (committed < kInput && Clock::now() < deadline) {
        if (!w.until([&] { return static_cast<int>(pending.size()) >= kBatch || committed + static_cast<int>(pending.size()) >= kInput; }, 60s))
            continue;
        const auto n = std::min<std::size_t>(kBatch, pending.size());
        std::vector<Item> take(pending.begin(), pending.begin() + static_cast<std::ptrdiff_t>(n));
        pending.erase(pending.begin(), pending.begin() + static_cast<std::ptrdiff_t>(n));
        w.begin();
        Expected scratch;
        std::map<int32_t, int64_t> next;
        for (const Item& it : take) {
            (void)w.issue(mx::Op::Send, it.id, scratch);
            next[it.partition] = std::max(next[it.partition], it.offset + 1);
        }
        if (!crashed && committed >= kInput * 30 / 100) {
            e.c.crash(e.ctl);   // the coordinator AND the controller, mid-transaction
            crashed = true;
        }
        std::vector<rc::OffsetEntry> offs;
        for (const auto& [part, off] : next) offs.push_back(rc::OffsetEntry{in.topicId, part, off});
        const bool sent = w.sendOffsets(offs, "eos-crash-g", 30s).ok();
        if (sent && w.commit(30s).ok()) {
            committed += static_cast<int>(take.size());
            want.merge(scratch);
            continue;
        }
        ++failed;
        (void)w.abort(30s);
        pending.clear();
        try {
            gc.seekToCommittedAndWait(30s);
        } catch (const std::exception& ex) {
            ++rewindFailures;
            lastRewindError = ex.what();
            ASSERT_LT(rewindFailures, 20) << "the pipeline cannot rewind after the coordinator crash: " << lastRewindError;
        }
    }
    ASSERT_TRUE(crashed);
    ASSERT_EQ(committed, kInput) << "the pipeline stalled after the coordinator crash (failed txns " << failed
                                 << ", rewind failures " << rewindFailures << ": " << lastRewindError << ")";
    int64_t cursor = 6'000'000;
    ASSERT_TRUE(w.sentinel(want, cursor).ok());
    awaitSettled(e, {out});
    Reader rcRead(e, seed, {out}, -1, rc::Isolation::ReadCommitted);
    ASSERT_TRUE(rcRead.await(want, 120s));
    expectExactlyOnce(rcRead.seen, want, "output == input after the coordinator crash");
    gc.close();
    pp.close();
    cp->close();
    cc->close();
}


// =========================================================================================================
// #494: eos-e2e step 3a's failure, isolated: right after the group / transaction coordinator (the controller)
// crashes, a rewind (seekToCommitted) issued the way eos_probe retries it - up to 20 attempts, 200 ms apart -
// must succeed.  The client gives a crashed coordinator's session up only after kCoordDownFallbackNs (5 s); until
// then every seekToCommitted used to complete SessionLost at once, so a caller with a ~4 s retry budget gave up.
// =========================================================================================================
TEST(EosBrokerRegression, SeekToCommittedRightAfterTheCoordinatorCrashSucceedsWithinTheProbesRetryBudgetRegression) {
    Env e("eos_seek_crash");
    const rc::TopicSpec in = e.define("eos.in", kParts);
    const int seed = e.otherThan(e.ctl);
    auto cc = std::make_unique<rc::Client>(e.cfg(seed, {in}));
    rc::GroupConfig gcfg;
    gcfg.autoCommit       = false;
    gcfg.autoOffsetReset  = rc::OffsetReset::Earliest;
    gcfg.sessionTimeoutMs = 10'000;
    rc::GroupConsumer gc = cc->subscribeGroup("eos-seek-g", {"eos.in"}, gcfg);
    gc.onRecords([](const rc::RecordSet&) {});
    cc->connectAndWait(30s);
    // Stable in the group (a rewind needs it): pump until seekToCommitted succeeds once.
    bool stable = false;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline && !stable;) {
        try { gc.seekToCommittedAndWait(5s); stable = true; } catch (const std::exception&) { (void)cc->doWork(); }
    }
    ASSERT_TRUE(stable) << "the consumer never reached Stable";
    e.c.crash(e.ctl);
    int failures = 0;
    std::string last;
    bool ok = false;
    for (int attempt = 0; attempt < 20 && !ok; ++attempt) {
        try { gc.seekToCommittedAndWait(30s); ok = true; }
        catch (const std::exception& ex) {
            ++failures; last = ex.what();
            for (const auto until = Clock::now() + 200ms; Clock::now() < until;) (void)cc->doWork();   // eos_probe: pumpFor(200)
        }
    }
    EXPECT_TRUE(ok) << "seekToCommitted failed " << failures << " times after the coordinator crash: " << last;
    gc.close();
    cc->close();
}
