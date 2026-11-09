// LeaseRecipeBrokerTest.cpp - lease topics Task 20 (spec 12.3, 16.1, 16.2): LeaseOwnership / LeaseElection against REAL brokers and the real
// client on the default config (a Thread-mode client): one node, and the trio with a real leader kill.  Two participants are two
// sessions + two watches + two hosts on one client (two processes in production).  The lease clock runs in real time (short ttls); every wait
// is bounded.
#include "client/OneBroker.h"
#include "support/LeaseTrio.h"

#include <rune/client/lease/LeaseOwnership.h>
#include <rune/client/lease/LeaseRecipe.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseWatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test;
namespace rc = rune::client;
using rune::gen::LeaseClaim;
using Clk = std::chrono::steady_clock;

namespace {

constexpr uint32_t kCap = 4;

struct Done {
    std::vector<rc::Completion> v;
    static void fn(void* c, const rc::Completion& x, std::span<const uint8_t>) noexcept { static_cast<Done*>(c)->v.push_back(x); }
    [[nodiscard]] rc::CompletionTarget target() { return rc::CompletionTarget(rc::CompletionHandler{&Done::fn, this}); }
};

/// One candidate: its own session (own holder id), watch and host, one ownership recipe for the name.
struct Participant {
    using Host = rc::LeaseRecipeHost<kCap>;
    struct Fwd {
        Host* host = nullptr;
        int   unclaimedLost = 0;
    };
    struct OnLostF {
        Fwd* f;
        void operator()(rc::LeaseRef r, rc::LostReason w) const {
            if (f->host == nullptr || !f->host->onLost(r, w)) ++f->unclaimedLost;
        }
    };
    struct OnRiskF {
        Fwd* f;
        void operator()(rc::LeaseRef r, rc::AtRiskEdge e) const { if (f->host != nullptr) (void)f->host->onAtRisk(r, e); }
    };
    using Session = rc::LeaseSession<4, OnLostF, OnRiskF>;

    rc::Client&                           client;
    Fwd                                   fwd;
    std::vector<rc::LostReason>           lost;
    std::unique_ptr<Session>              session;
    std::unique_ptr<rc::LeaseWatch<LeaseClaim>> watch;
    std::unique_ptr<Host>                 host;
    std::unique_ptr<rc::LeaseOwnership>   own;
    Done                                  done;

    static void onLost(void* c, rc::LostReason w) { static_cast<Participant*>(c)->lost.push_back(w); }

    Participant(rc::Client& c, rc::LeaseTopic<LeaseClaim>& lt, std::string_view name) : client(c) {
        session = std::make_unique<Session>(c, OnLostF{&fwd}, OnRiskF{&fwd}, rc::LeaseSessionOptions{});
        rc::LeaseWatchOptions wo;
        wo.names          = 2;
        wo.waitersPerName = 8;
        wo.maxLive        = 64;
        watch = std::make_unique<rc::LeaseWatch<LeaseClaim>>(c, lt, wo);
        EXPECT_TRUE(session->observe(*watch));
        host     = std::make_unique<Host>(c, *session, *watch, lt);
        fwd.host = host.get();
        own      = std::make_unique<rc::LeaseOwnership>(*host, name, rc::LeaseRecipeCallbacks{this, &Participant::onLost, nullptr});
        EXPECT_TRUE(own->valid());
    }
    ~Participant() {
        own.reset();
        fwd.host = nullptr;
        host.reset();
        watch.reset();
        session.reset();
    }
    void start() {
        const std::vector<uint8_t> none;
        const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
        ASSERT_EQ(watch->startFromNow(prefixes), rc::OpResult::Accepted);
    }
    void work() {
        const int64_t now = client.nowNs();
        session->doWork(now);
        watch->doWork(now);
        host->doWork(now);
    }
};

/// Drives `ps` on this thread until `pred` holds or `within` passes (a 2 ms idle between passes).
template <class P>
bool driveUntil(std::initializer_list<Participant*> ps, std::chrono::milliseconds within, P&& pred) {
    const auto end = Clk::now() + within;
    while (Clk::now() < end) {
        for (Participant* p : ps) p->work();
        if (pred()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

rune::protocol::LeaseSpec spec(uint32_t ttlMs) {
    return rune::protocol::LeaseSpec{.ttlMs = ttlMs, .minTtlMs = 1000, .maxTtlMs = 600'000, .maxKeys = 1000, .expiryBatch = 64,
                                     .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};
}

}  // namespace

// A holder that stops heartbeating (a SIGSTOP, a partition): its lease expires, the next waiter owns with a LARGER fencing token, and the
// stalled holder learns it lost the name when it runs again.  Default config, real client, real single-node broker.
TEST(LeaseRecipeBrokerRegression, HolderExpiresNextWaiterOwnsRegression) {
    OneBroker b("lease-recipe-1", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    rc::TopicOptions to;
    to.lease                = spec(1500);
    to.config.cleanupPolicy = 1;
    const auto def = c.defineTopicAndWait("claims", 1, "claim",
                                          rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                                                     .addFixedStr("claim", 32, 1, true)
                                                                     .add("a", rune::codec::PrimType::UInt64)
                                                                     .addFixedStr("b", 8)
                                                                     .build()),
                                          rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);

    Participant a(c, lt, "own");
    Participant bb(c, lt, "own");
    a.start();
    bb.start();
    ASSERT_TRUE(driveUntil({&a, &bb}, 30s, [&] { return a.watch->isReady() && bb.watch->isReady(); }));

    {   // Thread mode: the duty thread is the one producer of a Completions queue, so a queue target is refused (a handler works)
        rc::Completions q(c);
        EXPECT_EQ(a.own->claim(q, 9), rc::OpResult::InvalidArgument);
    }
    ASSERT_EQ(a.own->claim(a.done.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntil({&a, &bb}, 30s, [&] { return a.done.v.size() == 1; }));
    ASSERT_EQ(a.done.v[0].status, rc::OpStatus::Ok) << a.done.v[0].errorCode;
    const int64_t tokenA = a.done.v[0].value;
    ASSERT_TRUE(a.own->isOwner());

    ASSERT_EQ(bb.own->claim(bb.done.target(), 2), rc::OpResult::Accepted);
    // The holder is alive for more than two ttls: the session keeps it, the waiter keeps waiting.
    EXPECT_FALSE(driveUntil({&a, &bb}, 3500ms, [&] { return !bb.done.v.empty() || !a.own->isOwner(); })) << "A is alive: B waits";
    EXPECT_TRUE(a.own->isOwner());

    // A stops heartbeating (it is not driven any more); its lease expires on the broker; B owns.
    ASSERT_TRUE(driveUntil({&bb}, 30s, [&] { return !bb.done.v.empty(); })) << "the next waiter owns after the holder's lease expired";
    ASSERT_EQ(bb.done.v[0].status, rc::OpStatus::Ok) << bb.done.v[0].errorCode;
    const int64_t tokenB = bb.done.v[0].value;
    EXPECT_GT(tokenB, tokenA) << "the new owner's fencing token is larger";
    EXPECT_TRUE(bb.own->isOwner());
    EXPECT_EQ(bb.own->token(), tokenB);
    const auto leader = bb.own->owner();
    ASSERT_TRUE(leader.has_value());
    EXPECT_EQ(leader->first, tokenB);

    // A runs again: it learns the lease is gone (the watch's Expired, or a keepalive answered LeaseNotFound) and reports the loss once.
    ASSERT_TRUE(driveUntil({&a, &bb}, 30s, [&] { return !a.lost.empty(); })) << "the stalled holder is told it lost the name";
    EXPECT_FALSE(a.own->isOwner());
    EXPECT_EQ(a.lost.size(), 1u);
    EXPECT_EQ(a.fwd.unclaimedLost, 0) << "the host claimed the session's callback";
    EXPECT_TRUE(bb.own->isOwner()) << "B is undisturbed";
}

// A leader kill with the lease already folded: the owner keeps the name across the failover (its lease and keepalives follow the new leader),
// the waiter keeps waiting, and when the owner releases, the waiter owns with a larger token.  Three real brokers, acks=all.
TEST(LeaseRecipeTrioRegression, LeaderChangeFoldedRegression) {
    LeaseTrio t("lease-recipe-trio");
    const auto topic = t.defineLease("claims", 1, 30'000, 1000, 600'000);
    auto lt = t.open(topic);
    ASSERT_TRUE(t.awaitIsrSize(topic, 0, 3, 30s));

    Participant a(t.client(), lt, "elect");
    Participant bb(t.client(), lt, "elect");
    a.start();
    bb.start();
    ASSERT_TRUE(driveUntil({&a, &bb}, 60s, [&] { return a.watch->isReady() && bb.watch->isReady(); }));

    ASSERT_EQ(a.own->claim(a.done.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntil({&a, &bb}, 60s, [&] { return a.done.v.size() == 1; }));
    ASSERT_EQ(a.done.v[0].status, rc::OpStatus::Ok) << a.done.v[0].errorCode;
    const int64_t tokenA = a.done.v[0].value;
    ASSERT_EQ(bb.own->claim(bb.done.target(), 2), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntil({&a, &bb}, 30s, [&] { return bb.watch->liveCount() == 2 && a.watch->liveCount() == 2; })) << "both keys are folded";

    // The kill: the leader of the lease partition crashes (a real crash); the survivors elect a new one.
    std::thread killer;
    std::atomic<int32_t> killed{0};
    killer = std::thread([&] { killed = t.killLeader(topic, 0); });
    const bool driven = driveUntil({&a, &bb}, 120s, [&] { return killed.load() != 0; });
    killer.join();
    ASSERT_TRUE(driven);
    ASSERT_GT(killed.load(), 0);

    // After the failover both watches converge on the same two keys; A still owns, B still waits.
    ASSERT_TRUE(driveUntil({&a, &bb}, 120s, [&] { return a.watch->isReady() && bb.watch->isReady() && a.watch->liveCount() == 2 && bb.watch->liveCount() == 2; }))
        << "the watches converge after the leader change";
    EXPECT_TRUE(a.own->isOwner()) << "an acked, folded grant survives the leader change";
    EXPECT_EQ(a.own->token(), tokenA);
    EXPECT_TRUE(bb.done.v.empty()) << "the waiter is not promoted by the failover";
    EXPECT_TRUE(a.lost.empty());

    // The owner releases: the waiter owns with a larger token.
    ASSERT_EQ(a.own->release(), rc::OpResult::Accepted);
    ASSERT_TRUE(driveUntil({&a, &bb}, 120s, [&] { return !bb.done.v.empty(); })) << "the waiter owns after the release";
    ASSERT_EQ(bb.done.v[0].status, rc::OpStatus::Ok) << bb.done.v[0].errorCode;
    EXPECT_GT(bb.done.v[0].value, tokenA);
    EXPECT_TRUE(bb.own->isOwner());
    EXPECT_FALSE(a.own->isOwner());
    EXPECT_TRUE(a.lost.empty()) << "a release is not a loss";
}
