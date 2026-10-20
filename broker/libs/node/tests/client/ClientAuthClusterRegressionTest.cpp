// ClientAuthClusterRegressionTest.cpp - authentication Task 19: the REAL client (rune::client::Client, username /
// password) against THREE real authenticating BrokerNodes (support/AuthTrio.h): one client session per broker, produce
// and consume on `world.*` through each, then REVOKE WRITE: the next produce through every broker completes
// NotAuthorized, with no retry storm - exactly one refusal at the brokers per failed record.
#include "support/AuthTrio.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/client/Client.h>
#include <rune/client/TypedTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Record.h>
#include <rune/codec/Serialize.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include "support/SecurityBrokerCluster.h"

#include <gtest/gtest.h>

#include <map>
#include <mutex>
#include <set>

using namespace rune::node;
using namespace rune::node::test;
namespace rc = rune::client;
using namespace std::chrono_literals;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;

namespace {

constexpr int32_t kParts = 3;

rc::ClientConfig configFor(AuthTrio& t, int id, const std::string& user, const std::string& password, int seq) {
    rc::ClientConfig c;
    c.gatewayChannel  = t.channel(id);
    c.gatewayStreamId = AuthWireClient::kClientStream;
    c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
    c.egressStreamId  = 2;
    c.dataDir         = (t.cluster().clientsDir->path / ("rc" + std::to_string(id) + "_" + std::to_string(seq))).string();
    c.username        = user;
    c.password        = password;
    return c;
}

/// The typed schema of world.zone: worldId is the record key; seq and hp are payload.
rune::codec::Descriptor worldEventDescriptor() {
    return rune::codec::DescriptorBuilder("world_event", 1)
        .add("worldId", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("seq", rune::codec::PrimType::Int64)
        .add("hp", rune::codec::PrimType::Int64)
        .build();
}

/// A hand-written record type that follows the generated-struct contract TypedTopic<T> needs (what
/// `rune schema generate` would emit): kFingerprint, kSchemaName, encodedSize / encode, key().  Wire = the codec's
/// fixed block: [blockLength:u16][version:u16] then worldId, seq, hp as little-endian int64.
struct WorldEvent {
    static const uint64_t kFingerprint;
    static constexpr std::string_view kSchemaName = "world_event";
    static constexpr uint16_t kVersion = 1, kBlockLength = 24;
    int64_t worldId = 0, seq = 0, hp = 0;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + kBlockLength; }
    [[nodiscard]] std::size_t encode(uint8_t* b) const noexcept {
        auto put = [&](std::size_t off, uint64_t v) { for (int i = 0; i < 8; ++i) b[off + i] = static_cast<uint8_t>(v >> (8 * i)); };
        b[0] = kBlockLength & 0xff; b[1] = kBlockLength >> 8; b[2] = kVersion & 0xff; b[3] = kVersion >> 8;
        put(4, static_cast<uint64_t>(worldId)); put(12, static_cast<uint64_t>(seq)); put(20, static_cast<uint64_t>(hp));
        return encodedSize();
    }
    [[nodiscard]] int64_t key() const noexcept { return worldId; }
};
const uint64_t WorldEvent::kFingerprint = rune::codec::fingerprint(worldEventDescriptor());

/// Decode a consumed record through the descriptor (the reader a generated decode() would be).
WorldEvent decodeWorldEvent(const rune::codec::Descriptor& d, std::span<const uint8_t> rec) {
    WorldEvent e;
    e.worldId = rune::codec::readField(d, rec, 0).i;
    e.seq = rune::codec::readField(d, rec, 1).i;
    e.hp = rune::codec::readField(d, rec, 2).i;
    return e;
}

uint64_t refusals(BrokerNode& n) {
    uint64_t total = n.gatewayNotAuthorizedForTest();
    for (int32_t w = 0; w < n.workerCountForTest(); ++w)
        total += n.onWorkerThreadForTest(w, [](Worker& wk) { return wk.workerNotAuthorizedForTest(); });
    return total;
}

}  // namespace

TEST(ClientAuthClusterRegression, WorldServerClientProducesAndConsumesThroughEveryBroker) {
    AuthTrio t("rclient");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "world-server", "world-server-password");
    t.grant(1, "world-server", SO::Create, ST::Topic, SP::Prefix, "world.");
    t.grant(1, "world-server", SO::Describe, ST::Topic, SP::Prefix, "world.");
    t.grant(1, "world-server", SO::Write, ST::Topic, SP::Prefix, "world.");
    t.grant(1, "world-server", SO::Read, ST::Topic, SP::Prefix, "world.");

    int cfgSeq = 0;
    const rune::codec::Descriptor desc = worldEventDescriptor();
    ASSERT_EQ(desc.blockLength, WorldEvent::kBlockLength) << "the hand-written WorldEvent wire matches the descriptor";

    // Define the topic TYPED (key field worldId, broker-side validation on) through a client on broker 1 (retry: the
    // grants reach broker 1 asynchronously).
    rc::TopicSpec spec;
    {
        std::unique_ptr<rc::Client> admin;
        ASSERT_TRUE(AuthTrio::until(60s, [&] {
            try { admin = std::make_unique<rc::Client>(configFor(t, 1, "world-server", "world-server-password", cfgSeq++)); admin->connectAndWait(30s); return true; }
            catch (const std::exception&) { admin.reset(); return false; }
        }));
        rc::TopicOptions to;
        to.replicationFactor = 3;
        rc::DefineTopicResult def{};
        ASSERT_TRUE(AuthTrio::until(60s, [&] {
            def = admin->defineTopicAndWait("world.zone", kParts, "worldId", rune::codec::serialize(desc), /*validate=*/true,
                                            rc::Persistence::Disk, to, {}, 60s);
            return def.errorCode == 0;
        })) << def.detail;
        spec = rc::TopicSpec{"world.zone", def.topicId, kParts};
        admin->close();
    }

    // One real client per broker; each opens world.zone TYPED (the fingerprint gate through that broker).
    std::array<std::unique_ptr<rc::Client>, 3> clients;
    std::vector<rc::TypedTopic<WorldEvent>> typed;
    for (int id : {1, 2, 3}) {
        auto cfg = configFor(t, id, "world-server", "world-server-password", cfgSeq++);
        cfg.topics = {spec};
        auto& cl = clients[static_cast<std::size_t>(id - 1)];
        ASSERT_TRUE(AuthTrio::until(60s, [&] {
            try { cl = std::make_unique<rc::Client>(cfg); cl->connectAndWait(30s); return true; }
            catch (const std::exception&) { cl.reset(); return false; }
        })) << "client on broker " << id;
        EXPECT_EQ(cl->authStatus().kind, rc::AuthStatus::Kind::Authenticated);
        bool bound = false;
        AuthTrio::until(60s, [&] {   // Describe on world.* has to reach this broker
            try { typed.push_back(cl->openTopicAndWait<WorldEvent>("world.zone", 20s)); bound = true; return true; }
            catch (const std::exception&) { return false; }
        });
        ASSERT_TRUE(bound) << "the fingerprint gate must bind WorldEvent through broker " << id;
        EXPECT_EQ(typed.back().handle().partitionCount, kParts);
        EXPECT_EQ(typed.back().boundVersion(), WorldEvent::kVersion);
    }

    // Typed produce through each broker (TypedTopic::send routes by worldId over the partitions).
    constexpr int kPer = 6;
    auto hpOf = [](int64_t world) { return world * 10 + 7; };
    for (int idx = 0; idx < 3; ++idx)
        for (int i = 0; i < kPer; ++i) {
            const int64_t world = idx * 100 + i;
            typed[static_cast<std::size_t>(idx)].send(WorldEvent{world, i, hpOf(world)});
        }

    // Typed consume: every client reads every partition from the start and decodes all 3 * kPer events, field by field.
    for (int idx = 0; idx < 3; ++idx) {
        std::mutex mu;
        std::map<int64_t, WorldEvent> seen;
        int decodeErrors = 0;
        std::vector<rc::Subscription> subs;
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        const rc::TopicHandle th = clients[static_cast<std::size_t>(idx)]->topic("world.zone");
        for (int32_t p = 0; p < kParts; ++p)
            subs.push_back(clients[static_cast<std::size_t>(idx)]->subscribe(th, p, o, [&](const rc::Record& r) {
                std::lock_guard lk(mu);
                const WorldEvent e = decodeWorldEvent(desc, r.value);
                if (e.hp != hpOf(e.worldId)) ++decodeErrors;
                seen[e.worldId] = e;
            }));
        EXPECT_TRUE(AuthTrio::until(60s, [&] { std::lock_guard lk(mu); return seen.size() >= static_cast<std::size_t>(3 * kPer); }))
            << "broker " << idx + 1 << " decoded " << seen.size();
        for (auto& sub : subs) sub.unsubscribe();
        std::lock_guard lk(mu);
        EXPECT_EQ(decodeErrors, 0);
        for (int b = 0; b < 3; ++b)
            for (int i = 0; i < kPer; ++i) {
                const auto it = seen.find(b * 100 + i);
                ASSERT_NE(it, seen.end()) << "client " << idx + 1 << " missing event " << b * 100 + i;
                EXPECT_EQ(it->second.seq, i);
                EXPECT_EQ(it->second.hp, hpOf(b * 100 + i));
            }
    }

    // REVOKE WRITE; every broker must enforce it before the counting starts (probe with a raw session per broker).
    const auto rev = t.revoke(2, "world-server", SO::Write, ST::Topic, SP::Prefix, "world.");
    ASSERT_EQ(rev.errorCode, 0) << rev.detail;
    ASSERT_TRUE(t.awaitDigestsAgree(0, 30s));
    AuthWireClient& probe = t.wire();
    for (int id : {1, 2, 3}) {
        const auto s = t.loginOn(probe, id, "world-server", "world-server-password");
        ASSERT_TRUE(AuthTrio::until(20s, [&] {
            const int64_t corr = probe.raw().nextCorrelationId();
            probe.raw().produce(t.channel(id), AuthWireClient::kClientStream, s.sid, corr, spec.topicId, 0, 1, makeBatch(1));
            const auto a = probe.raw().awaitProduceResponseUntil(corr, Clock3::now() + 3s);
            return a && a->errorCode == 45;
        })) << "broker " << id << " must enforce the REVOKE";
    }
    uint64_t before = 0;
    for (int id : {1, 2, 3}) before += refusals(t.node(id));

    // The next typed produce through every broker's client completes NotAuthorized - once each, no retries.  (The
    // per-record completion goes through the client's Producer with the record encoded exactly as TypedTopic::send does:
    // the bound schema version stamped on the header.)
    constexpr int kFail = 4;
    for (int idx = 0; idx < 3; ++idx) {
        auto& cl = *clients[static_cast<std::size_t>(idx)];
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::All;
        pcfg.lingerUs = 1;
        pcfg.batchBytes = 1;
        pcfg.produceTimeoutMs = 30'000;
        pcfg.deliveryTimeoutMs = 40'000;
        auto producer = cl.producer(pcfg);
        rc::Completions cq(cl);
        std::map<rc::Token, rc::Completion> done;
        auto poll = [&] { (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d; }); };
        for (int i = 0; i < kFail; ++i) {
            const WorldEvent e{idx * 100 + 50 + i, i, hpOf(idx * 100 + 50 + i)};
            std::vector<uint8_t> bytes(e.encodedSize());
            e.encode(bytes.data());
            bytes[2] = static_cast<uint8_t>(typed[static_cast<std::size_t>(idx)].boundVersion() & 0xff);
            bytes[3] = static_cast<uint8_t>(typed[static_cast<std::size_t>(idx)].boundVersion() >> 8);
            const rc::Token tok = static_cast<rc::Token>(i + 1);
            for (const auto dl = Clock3::now() + 30s; Clock3::now() < dl;) {
                if (producer.trySend(typed[static_cast<std::size_t>(idx)].handle(), rc::Key(e.key()), bytes, cq, tok) == rc::SendResult::Ok) break;
                poll();
                std::this_thread::yield();
            }
        }
        for (const auto dl = Clock3::now() + 60s; Clock3::now() < dl && done.size() < static_cast<std::size_t>(kFail);) {
            poll();
            std::this_thread::sleep_for(2ms);
        }
        ASSERT_EQ(done.size(), static_cast<std::size_t>(kFail)) << "broker " << idx + 1;
        for (const auto& [tok, d] : done) {
            EXPECT_EQ(d.status, rc::OpStatus::BrokerError) << "broker " << idx + 1 << " token " << tok;
            EXPECT_EQ(d.errorCode, 45) << "NotAuthorized";
        }
        producer.close();
    }
    uint64_t after = 0;
    for (int id : {1, 2, 3}) after += refusals(t.node(id));
    EXPECT_EQ(after - before, static_cast<uint64_t>(3 * kFail)) << "exactly one ProduceRequest (refused) per failed record: no retry storm";

    for (auto& c : clients) c->close();
}

// PR #410 review, adversarial pass (#114 through the REAL client on THREE brokers): a user without ADMIN, seeded at a
// broker that is NOT the __security leader, changes its own password.  The DDL is redirected to the leader and rides
// the client's session there - it must be re-signed for THAT session's login (a proof made for the seed's session is
// refused).  The added password then logs in on every broker.
TEST(ClientAuthClusterRegression, AnOwnPasswordChangeRedirectedToTheSecurityLeaderIsSignedForThatSessionAdversarialRegression) {
    AuthTrio t("rcselfpw");
    t.start();
    t.bootstrapAdmin();
    t.createUser(1, "ws1", AuthTrio::kPw);
    const int leader = settledSecurityLeader(t.cluster());
    ASSERT_GT(leader, 0);
    const int seed = leader % 3 + 1;   // a follower of __security
    std::unique_ptr<rc::Client> c;
    int seq = 0;
    ASSERT_TRUE(AuthTrio::until(60s, [&] {
        try { c = std::make_unique<rc::Client>(configFor(t, seed, "ws1", AuthTrio::kPw, seq++)); c->connectAndWait(30s); return true; }
        catch (const std::exception&) { c.reset(); return false; }
    }));
    auto plan = rune::ql::applySecurityPlan(rune::ql::parse("alter user ws1 add password 'ws1-redirected-pw-123';").value());
    ASSERT_TRUE(plan.has_value());
    const rc::SecurityDdlResult r = c->securityDdlAndWait(std::move(*plan), rc::DdlOptions{}, 60s);
    EXPECT_EQ(r.errorCode, 0u) << "seed " << seed << " leader " << leader << ": " << r.detail;
    c->close();
    for (int id : {1, 2, 3})
        EXPECT_TRUE(AuthTrio::until(30s, [&] {
            try { (void)t.login(id, "ws1", "ws1-redirected-pw-123"); return true; } catch (const std::exception&) { return false; }
        })) << "node " << id;
}
