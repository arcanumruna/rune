// LeaseRecipeBrokerTest.cpp - lease topics Task 20 (spec 12.3, 16.1, 16.2): LeaseOwnership / LeaseElection against REAL brokers and the real
// client on the default config (a Thread-mode client): one node, and the trio with a real leader kill.  Two participants are two
// sessions + two watches + two hosts on one client (two processes in production).  The lease clock runs in real time (short ttls); every wait
// is bounded.
#include "client/OneBroker.h"
#include "support/LeaseMatrixExpectations.h"
#include "support/LeaseTrio.h"

#include <rune/client/lease/LeaseElection.h>
#include <rune/client/lease/LeaseOwnership.h>
#include <rune/client/lease/LeaseRecipe.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseWatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
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

// =================================================================================================================================
// The recipes' column of the interaction matrix (plan "Interaction matrix", batch F): every applicable sequential cell of the 7 recipe ops
// runs against ONE real 1-node broker (default config, a Thread-mode client), checked against LeaseMatrixExpectations.h `expect`: the
// op's outcome, its effect on the lease it addresses, and that every other lease (the control lease C, and L itself unless the cell
// says L changes) is byte-identical.  Each participant is its own session + watch + host on one client; every wait is bounded.
// =================================================================================================================================
namespace {

namespace lm = rune::node::test::leasematrix;
using Lt = rc::LeaseTopic<LeaseClaim>;

/// A candidate of ONE name on ONE lease topic (an ownership or an election recipe), with its own session, watch and host.
struct Side {
    using Host    = rc::LeaseRecipeHost<kCap>;
    using Fwd     = Participant::Fwd;
    using Session = Participant::Session;
    Fwd                                         fwd;
    std::unique_ptr<Session>                    session;
    std::unique_ptr<rc::LeaseWatch<LeaseClaim>> watch;
    std::unique_ptr<Host>                       host;
    std::unique_ptr<rc::LeaseOwnership>         own;
    std::unique_ptr<rc::LeaseElection>          elect;
    rc::Client&                                 client;
    rc::Completions                             pq;       // a proclaim completes into a Completions queue (a session update does)
    std::vector<rc::Completion>                 pdone;
    Done                                        done;
    std::vector<rc::LostReason>                 lost;
    std::vector<int64_t>                        observed;

    static void onLost(void* c, rc::LostReason w) { static_cast<Side*>(c)->lost.push_back(w); }
    static void onSeen(void* c, int64_t v, std::span<const uint8_t>, std::span<const uint8_t>) { static_cast<Side*>(c)->observed.push_back(v); }

    Side(rc::Client& c, Lt& lt, std::string_view name, bool election) : client(c), pq(c) {
        session = std::make_unique<Session>(c, Participant::OnLostF{&fwd}, Participant::OnRiskF{&fwd}, rc::LeaseSessionOptions{});
        rc::LeaseWatchOptions wo;
        wo.names          = 2;
        wo.waitersPerName = 8;
        wo.maxLive        = 64;
        watch = std::make_unique<rc::LeaseWatch<LeaseClaim>>(c, lt, wo);
        EXPECT_TRUE(session->observe(*watch));
        host     = std::make_unique<Host>(c, *session, *watch, lt);
        fwd.host = host.get();
        const rc::LeaseRecipeCallbacks cb{this, &Side::onLost, nullptr};
        if (election) elect = std::make_unique<rc::LeaseElection>(*host, name, sizeof(LeaseClaim) + 64, cb);
        else own = std::make_unique<rc::LeaseOwnership>(*host, name, cb);
        EXPECT_TRUE(valid());
        const std::vector<uint8_t> prefix(name.begin(), name.end());
        const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(prefix)};
        EXPECT_EQ(watch->startFromNow(prefixes), rc::OpResult::Accepted);
    }
    ~Side() {
        own.reset();
        elect.reset();
        fwd.host = nullptr;
        host.reset();
        watch.reset();
        session.reset();
    }
    [[nodiscard]] bool valid() const { return own ? own->valid() : elect->valid(); }
    [[nodiscard]] bool isOwner() const { return own ? own->isOwner() : elect->isLeader(); }
    [[nodiscard]] int64_t token() const { return own ? own->token() : elect->token(); }
    [[nodiscard]] LeaseClaim keyRow() const {   // the lease row of the recipe's current key
        const auto k = own ? own->key() : elect->key();
        LeaseClaim r;
        if (!k.empty()) std::memcpy(r.claim.data(), k.data(), std::min<std::size_t>(k.size(), r.claim.size()));
        return r;
    }
    rc::OpResult claim(Done& d, rc::Token t) {
        if (own) return own->claim(d.target(), t);
        return elect->campaign(LeaseClaim{}, d.target(), t);
    }
    rc::OpResult release() { return own ? own->release() : elect->resign(); }
    void work() {
        const int64_t now = client.nowNs();
        session->doWork(now);
        watch->doWork(now);
        host->doWork(now);
        (void)pq.poll([&](const rc::Completion& x, std::span<const uint8_t>) { pdone.push_back(x); });
    }
};

struct RCell { lm::Op op; lm::Entity ent; };
std::string rCellName(const testing::TestParamInfo<RCell>& i) { return std::string(lm::name(i.param.op)) + "_" + lm::name(i.param.ent); }
std::vector<RCell> recipeCells() {
    std::vector<RCell> v;
    for (std::size_t o = 0; o < lm::kOps; ++o)
        if (lm::isRecipe(lm::Op(o)))
            for (std::size_t e = 0; e < lm::kEntities; ++e)
                if (!lm::naRule(lm::Op(o), lm::Entity(e))) v.push_back({lm::Op(o), lm::Entity(e)});
    return v;
}

struct Seen {
    bool     present = false;
    int64_t  version = 0;
    uint64_t a       = 0;
};
Seen seenOf(Lt& lt, const LeaseClaim& r) {
    const auto row = lt.getAndWait(r);
    return row ? Seen{true, row->lease_version, row->a} : Seen{};
}
bool sameSeen(const Seen& x, const Seen& y) { return x.present == y.present && x.version == y.version && x.a == y.a; }

const std::vector<uint8_t>& claimBlobBytes() {
    static const std::vector<uint8_t> b = rune::codec::serialize(
        rune::codec::DescriptorBuilder("lease_claim").addFixedStr("claim", 32, 1, true).add("a", rune::codec::PrimType::UInt64).addFixedStr("b", 8).build());
    return b;
}

class LeaseMatrixRecipeRegression : public testing::TestWithParam<RCell> {
public:
    static void SetUpTestSuite() {
        broker_ = std::make_unique<OneBroker>("lease-matrix-recipe", OneBroker::Runtime::Shard);
        client_ = broker_->connectClient();
        rc::TopicOptions to;
        to.lease                = spec(20'000);
        to.config.cleanupPolicy = 1;
        for (const char* n : {"claims", "claims2"}) {
            const auto def = client_->defineTopicAndWait(n, 1, "claim", claimBlobBytes(), rc::Persistence::Disk, to, {}, 60s);
            ASSERT_EQ(def.errorCode, 0) << def.detail;
        }
        const auto plainBlob = rune::codec::serialize(
            rune::codec::DescriptorBuilder("plain_t").add("id", rune::codec::PrimType::UInt64, 1, true).add("a", rune::codec::PrimType::UInt64).build());
        const auto plain = client_->defineTopicAndWait("plain", 1, "id", plainBlob, rc::Persistence::Disk, rc::TopicOptions{}, {}, 60s);
        ASSERT_EQ(plain.errorCode, 0) << plain.detail;
    }
    static void TearDownTestSuite() {
        client_ = nullptr;
        broker_.reset();
    }

protected:
    static std::unique_ptr<OneBroker> broker_;
    static rc::Client*                client_;
    static int                        seq_;
};
std::unique_ptr<OneBroker> LeaseMatrixRecipeRegression::broker_;
rc::Client*                LeaseMatrixRecipeRegression::client_ = nullptr;
int                        LeaseMatrixRecipeRegression::seq_    = 0;

}  // namespace

TEST_P(LeaseMatrixRecipeRegression, SequentialCellRegression) {
    namespace lmx = rune::node::test::leasematrix;
    ASSERT_NE(client_, nullptr);
    rc::Client&        c   = *client_;
    const lm::Op       op  = GetParam().op;
    const lm::Entity   ent = GetParam().ent;
    const lm::Outcome  ex  = lm::expect(op, ent, lm::Timing::Sequential);
    const bool         election = op == lm::Op::ElectCampaign || op == lm::Op::ElectProclaim || op == lm::Op::ElectResign || op == lm::Op::ElectObserve;
    const std::string  at = std::string(lm::name(op)) + " x " + lm::name(ent);

    if (ent == lm::Entity::NonLeaseSameWorker) {   // a plain topic cannot be opened as a lease topic, so no host or recipe can be built over it
        lm::Result got = lm::Result::Ok;
        try { (void)c.openLeaseTopicAndWait<LeaseClaim>("plain", {}, 15s); } catch (const std::exception&) { got = lm::Result::NotALeaseTopic; }
        EXPECT_EQ(int(got), int(ex.result)) << at;
        return;
    }

    char stem[8];
    std::snprintf(stem, sizeof stem, "c%02d", ++seq_);
    const std::string nL = std::string(stem) + "a", nO2 = std::string(stem) + "b", nC = std::string(stem) + "c";
    auto lt1 = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);
    auto lt2 = c.openLeaseTopicAndWait<LeaseClaim>("claims2", {}, 30s);
    Lt&  sceneLt   = ent == lm::Entity::OtherLeaseTopic ? lt2 : lt1;
    const std::string sceneName = ent == lm::Entity::OtherKeySamePart ? nO2 : nL;
    const bool sceneIsL = ent == lm::Entity::SameKeyVer || ent == lm::Entity::StaleVer;

    // The participants: the scene's owner O and candidate K (other sessions), the control C, and L's owner when the scene is elsewhere.
    Side O(c, sceneLt, sceneName, election);
    Side K(c, sceneLt, sceneName, election);
    Side C(c, lt1, nC, election);
    std::unique_ptr<Side> LO;
    if (!sceneIsL) LO = std::make_unique<Side>(c, lt1, nL, election);
    const auto driveAll = [&](std::chrono::milliseconds within, auto&& pred) {
        const auto end = Clk::now() + within;
        while (Clk::now() < end) {
            O.work(); K.work(); C.work();
            if (LO) LO->work();
            if (pred()) return true;
            std::this_thread::sleep_for(2ms);
        }
        return pred();
    };
    ASSERT_TRUE(driveAll(30s, [&] { return O.watch->isReady() && K.watch->isReady() && C.watch->isReady() && (!LO || LO->watch->isReady()); }));
    const auto owns = [&](Side& sd, rc::Token t) {
        ASSERT_EQ(sd.claim(sd.done, t), rc::OpResult::Accepted) << at;
        ASSERT_TRUE(driveAll(30s, [&] { return sd.done.v.size() == 1; })) << at;
        ASSERT_EQ(sd.done.v[0].status, rc::OpStatus::Ok) << at << " code " << sd.done.v[0].errorCode;
        ASSERT_TRUE(sd.isOwner());
        sd.done.v.clear();
    };
    owns(C, 1);
    owns(O, 2);
    if (LO) owns(*LO, 3);
    if (HasFatalFailure()) return;

    const int64_t   tokenO  = O.token();
    const LeaseClaim rowO   = O.keyRow();
    const LeaseClaim rowC   = C.keyRow();
    const LeaseClaim rowL   = LO ? LO->keyRow() : rowO;
    const Seen beforeO = seenOf(sceneLt, rowO), beforeC = seenOf(lt1, rowC), beforeL = seenOf(lt1, rowL);
    ASSERT_TRUE(beforeO.present && beforeC.present && beforeL.present);
    const auto absentSoon = [&](Lt& lt, const LeaseClaim& r) {
        return driveAll(30s, [&] { return !lt.getAndWait(r).has_value(); });
    };

    lm::Result got = lm::Result::Ok;
    std::optional<Seen> lEarly;   // L as seen at the cell's own observation point (a claim's cleanup releases the owner afterwards)
    switch (op) {
    case lm::Op::OwnClaim: case lm::Op::ElectCampaign: {   // K waits behind O; O's lease is unchanged until O lets go, then K owns with a larger token
        ASSERT_EQ(K.claim(K.done, 10), rc::OpResult::Accepted) << at;
        EXPECT_FALSE(driveAll(800ms, [&] { return !K.done.v.empty(); })) << at << ": the claim waits behind the owner";
        EXPECT_TRUE(seenOf(sceneLt, K.keyRow()).present) << at << ": the claimer's own key is created";
        EXPECT_TRUE(O.isOwner());
        EXPECT_TRUE(sameSeen(beforeO, seenOf(sceneLt, rowO))) << at << ": the owner's lease is unchanged while the claim waits";
        lEarly = seenOf(lt1, rowL);
        ASSERT_EQ(O.release(), rc::OpResult::Accepted);
        ASSERT_TRUE(driveAll(30s, [&] { return K.done.v.size() == 1; })) << at;
        got = K.done.v[0].status == rc::OpStatus::Ok ? lm::Result::Ok : lm::Result::InvalidRequest;
        EXPECT_GT(K.done.v[0].value, tokenO) << at << ": the new owner's token is larger";
        EXPECT_TRUE(K.isOwner());
        break;
    }
    case lm::Op::OwnTryClaim: {
        ASSERT_EQ(K.own->tryClaim(K.done.target(), 11), rc::OpResult::Accepted) << at;
        ASSERT_TRUE(driveAll(30s, [&] { return K.done.v.size() == 1; })) << at;
        got = K.done.v[0].errorCode == static_cast<uint16_t>(rc::ClientErrorCode::LeaseClaimed) ? lm::Result::Claimed : lm::Result::InvalidRequest;
        EXPECT_FALSE(K.isOwner());
        EXPECT_TRUE(absentSoon(sceneLt, K.keyRow())) << at << ": the claimer's own key was revoked again";
        EXPECT_TRUE(sameSeen(beforeO, seenOf(sceneLt, rowO))) << at << ": the owner's lease is unchanged";
        break;
    }
    case lm::Op::OwnRelease: case lm::Op::ElectResign: {
        if (ent == lm::Entity::StaleVer) {   // L is revoked out of band at its version before the op
            ASSERT_TRUE(sceneLt.revokeAndWait(rowO, tokenO).ok());
            const rc::OpResult r = O.release();
            EXPECT_TRUE(r == rc::OpResult::Accepted || r == rc::OpResult::InvalidArgument) << at;
            got = lm::Result::LeaseNotFound;
            ASSERT_TRUE(driveAll(30s, [&] { return !O.isOwner(); })) << at;
            EXPECT_FALSE(seenOf(sceneLt, rowO).present);
        } else {
            ASSERT_EQ(O.release(), rc::OpResult::Accepted) << at;
            got = lm::Result::Ok;
            EXPECT_FALSE(O.isOwner());
            EXPECT_TRUE(absentSoon(sceneLt, rowO)) << at << ": the lease ends";
        }
        break;
    }
    case lm::Op::ElectProclaim: {
        LeaseClaim nr;
        nr.a = 99;
        if (ent == lm::Entity::StaleVer) {
            ASSERT_TRUE(sceneLt.revokeAndWait(rowO, tokenO).ok());
            const rc::OpResult r = O.elect->proclaim(nr, O.pq, 12);
            EXPECT_TRUE(r == rc::OpResult::Accepted || r == rc::OpResult::InvalidArgument) << at;
            if (r == rc::OpResult::Accepted) {
                ASSERT_TRUE(driveAll(30s, [&] { return O.pdone.size() == 1; })) << at;
                EXPECT_NE(O.pdone[0].status, rc::OpStatus::Ok) << at;
            }
            got = lm::Result::LeaseNotFound;
            ASSERT_TRUE(driveAll(30s, [&] { return !O.isOwner(); })) << at;
            EXPECT_FALSE(seenOf(sceneLt, rowO).present);
        } else {
            ASSERT_EQ(O.elect->proclaim(nr, O.pq, 12), rc::OpResult::Accepted) << at;
            ASSERT_TRUE(driveAll(30s, [&] { return O.pdone.size() == 1; })) << at;
            got = O.pdone[0].status == rc::OpStatus::Ok ? lm::Result::Ok : lm::Result::InvalidRequest;
            EXPECT_TRUE(driveAll(30s, [&] { return seenOf(sceneLt, rowO).a == 99u; })) << at << ": the row is replaced";
            EXPECT_TRUE(O.isOwner());
            EXPECT_EQ(seenOf(sceneLt, rowO).version, tokenO) << at << ": a proclaim keeps the lease's version";
        }
        break;
    }
    case lm::Op::ElectObserve: {
        ASSERT_TRUE(K.elect->observe(&Side::onSeen, &K));
        ASSERT_TRUE(driveAll(30s, [&] { return !K.observed.empty(); })) << at;
        got = K.observed.size() == 1 && K.observed[0] == tokenO ? lm::Result::Ok : lm::Result::InvalidRequest;
        EXPECT_TRUE(sameSeen(beforeO, seenOf(sceneLt, rowO))) << at << ": observing changes nothing";
        break;
    }
    default: FAIL() << "not a recipe op: " << at;
    }

    EXPECT_EQ(int(got), int(ex.result)) << at << ": the outcome";
    // L and the control lease: unchanged unless the cell says L changes (the scene IS L for SameKeyVer / StaleVer).
    EXPECT_TRUE(sameSeen(beforeC, seenOf(lt1, rowC))) << at << ": the control lease is unchanged";
    EXPECT_TRUE(C.isOwner());
    if (ent == lm::Entity::StaleVer) {
        EXPECT_TRUE(ex.result == lm::Result::LeaseNotFound);
    } else if (sceneIsL) {
        const Seen afterL = lEarly ? *lEarly : seenOf(lt1, rowL);
        const bool changed = afterL.present != beforeL.present || afterL.a != beforeL.a || afterL.version != beforeL.version;
        EXPECT_EQ(changed, ex.lChanged) << at << ": L changed exactly as expected";
    } else {
        EXPECT_TRUE(sameSeen(beforeL, seenOf(lt1, rowL))) << at << ": L is unchanged by an op on another entity";
        EXPECT_TRUE(LO->isOwner());
    }
    EXPECT_TRUE(O.lost.empty() || ent == lm::Entity::StaleVer) << at << ": no loss was reported";
}

INSTANTIATE_TEST_SUITE_P(Cells, LeaseMatrixRecipeRegression, testing::ValuesIn(recipeCells()), rCellName);
