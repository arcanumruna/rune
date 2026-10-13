// tests/client/ClientSubscribeNotLeaderRegressionTest.cpp
//
// Regression: silent-failure gap.  A pending plain Subscribe answered with a
// BARE NotLeader (code 2, empty message — e.g. a topic whose definition has not
// committed yet) used to be neither retried nor failed: it sat pending until
// subscribeTimeoutMs and then finished with failReason None and no onError.
//
//   (a) bare NotLeader once, then OK  -> the Subscribe binds active.
//   (b) bare NotLeader forever        -> explicit failure (NotLeader) within the
//                                        retry budget, onError fires.
//   (c) no answer ever                -> explicit TimedOut failure + onError.

#include "../support/TestCompletions.h"
#include "../support/AeronTestErrors.h"
#include <rune/client/Client.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/codec/Codec.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/MetadataMessages.h>

#include <Aeron.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace rcc = rune::client::codec;
namespace rp  = rune::protocol::client;

namespace {
std::atomic<int> g_ctr{0};
std::string uniqueDir(const char* prefix) {
    return std::string("/tmp/") + prefix + "_" + std::to_string(static_cast<long>(::getpid())) +
           "_" + std::to_string(g_ctr.fetch_add(1));
}

enum class SubMode { OkAfterOneNotLeader, AlwaysNotLeader, Silent, MovesAfterBind, MovesMidBatch };

struct SubGateway {
    std::atomic<int> subscribesSeen{0};
    SubMode          mode;
    // MovesAfterBind (test thread only: poll() runs here).
    std::vector<rcc::SubscribeView> subscribes;   // key span not kept
    std::vector<int64_t>            unsubscribedIds;
    static constexpr int64_t kSessionId = 77;

    std::shared_ptr<aeron::Aeron>                aeronC;
    std::shared_ptr<aeron::Subscription>         sub;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    std::unique_ptr<aeron::FragmentAssembler>    asmb;
    uint8_t replyBuf[2048]{};

    SubGateway(const std::string& aeronDir, int32_t inStr, int32_t egStr, SubMode m) : mode(m) {
        aeron::Context ctx;
        ctx.errorHandler(test_support::countAeronError);
        ctx.aeronDir(aeronDir);
        aeronC = aeron::Aeron::connect(ctx);
        const int64_t subId = aeronC->addSubscription("aeron:ipc", inStr);
        const int64_t pubId = aeronC->addExclusivePublication("aeron:ipc", egStr);
        const auto dl = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < dl) {
            if (!sub) sub = aeronC->findSubscription(subId);
            if (!pub) pub = aeronC->findExclusivePublication(pubId);
            if (sub && pub) break;
            std::this_thread::sleep_for(1ms);
        }
        asmb = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                   aeron::util::index_t len, aeron::Header&) {
                onFrame(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
            });
    }
    ~SubGateway() { asmb.reset(); pub.reset(); sub.reset(); aeronC.reset(); }

    int poll(int lim = 16) { return sub && asmb ? sub->poll(asmb->handler(), lim) : 0; }

    bool send(const uint8_t* b, std::size_t l) {
        const auto il = static_cast<aeron::util::index_t>(l);
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), il);
        for (int i = 0; i < 500; ++i) {
            if (pub->offer(ab, 0, il) > 0) return true;
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void sendMetadata(int64_t sessionId, int64_t corr) {
        std::vector<rp::BrokerEntry> brokers;
        rp::BrokerEntry b0; b0.nodeId = 0; b0.host = "aeron:ipc"; b0.port = 0;
        brokers.push_back(b0);
        std::vector<rp::PartitionEntry> parts;
        rp::PartitionEntry p; p.topicId = 1; p.partition = 0; p.leaderId = 0; p.leaderEpoch = 1;
        parts.push_back(p);
        rp::MetadataResponse msg;
        msg.sessionId = sessionId; msg.correlationId = corr;
        msg.brokers = std::span<const rp::BrokerEntry>(brokers);
        msg.partitions = std::span<const rp::PartitionEntry>(parts);
        const std::size_t n = msg.encode(replyBuf);
        if (n > 0) send(replyBuf, n);
    }

    void pushRecords(int64_t subId, int64_t base, int n, int64_t sessionId, int64_t corr) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        for (int i = 0; i < n; ++i) b.append("k", "v", 1000 + i);
        const auto batch = b.build();
        const std::size_t len = rcc::encodeRecords(replyBuf, sizeof(replyBuf), subId, 1, 0, base,
                                                   std::span<const uint8_t>(batch), sessionId, corr);
        if (len > 0) send(replyBuf, len);
    }
    // The move script (MovesAfterBind / MovesMidBatch).  Defaults: the first
    // Subscribe binds (id 9) and delivers offsets 0..2; then this node stops
    // leading the partition — optionally a position note (empty-batch Records,
    // client version 6) — and NotLeader (or Redirect) on that Subscribe's
    // correlation id.  The re-subscribe binds (id 10) and delivers 3..4.
    std::vector<std::pair<int64_t, int>> firstPushes{{0, 3}};
    std::vector<std::pair<int64_t, int>> secondPushes{{3, 2}};
    int16_t     moveCode = static_cast<int16_t>(rp::ErrorCode::NotLeader);
    std::string moveMessage;            // Redirect: the endpoint to follow
    int64_t     noteBase = -1;          // >= 0: send a position note first
    void movesAfterBind(const rcc::SubscribeView& v, int k) {
        rcc::SubscribeView copy = v;
        copy.key = {};
        subscribes.push_back(copy);
        const int64_t subId = k == 1 ? 9 : 10;
        std::size_t n = rcc::encodeSubscribeResponse(replyBuf, sizeof(replyBuf), 0, v.partition, subId,
                                                     v.sessionId, v.correlationId);
        if (n > 0) send(replyBuf, n);
        if (k == 1) {
            for (const auto& [base, cnt] : firstPushes) pushRecords(9, base, cnt, v.sessionId, v.correlationId);
            if (noteBase >= 0) {
                n = rcc::encodeRecords(replyBuf, sizeof(replyBuf), 9, 1, 0, noteBase, std::span<const uint8_t>{},
                                       v.sessionId, v.correlationId);
                if (n > 0) send(replyBuf, n);
            }
            n = rcc::encodeErrorResponse(replyBuf, sizeof(replyBuf), moveCode, moveMessage, v.sessionId,
                                         v.correlationId);
            if (n > 0) send(replyBuf, n);
        } else if (k == 2) {
            for (const auto& [base, cnt] : secondPushes) pushRecords(10, base, cnt, v.sessionId, v.correlationId);
        }
    }

    void onFrame(const uint8_t* buf, std::size_t len) {
        const auto tidOpt = rcc::peekTemplate(buf, len);
        if (!tidOpt) return;
        const auto tid = static_cast<rcc::TemplateId>(*tidOpt);
        if (tid == rcc::TemplateId::Connect) {
            const auto opt = rcc::decodeConnectRequest(buf, len);
            if (!opt) return;
            const std::size_t n = rcc::encodeConnectResponse(
                replyBuf, sizeof(replyBuf), static_cast<int16_t>(rcc::ErrorCode::OK), 0, kSessionId,
                opt->sessionId, opt->correlationId);
            if (n > 0) send(replyBuf, n);
        } else if (tid == rcc::TemplateId::MetadataRequest) {
            const auto opt = rp::MetadataRequest::decode(buf, len);
            if (opt) sendMetadata(opt->sessionId, opt->correlationId);
        } else if (tid == rcc::TemplateId::Unsubscribe) {
            if (const auto opt = rcc::decodeUnsubscribe(buf, len)) unsubscribedIds.push_back(opt->subscriptionId);
        } else if (tid == rcc::TemplateId::Subscribe) {
            const auto opt = rcc::decodeSubscribe(buf, len);
            if (!opt) return;
            const int k = subscribesSeen.fetch_add(1) + 1;
            if (mode == SubMode::Silent) return;
            if (mode == SubMode::MovesAfterBind || mode == SubMode::MovesMidBatch) { movesAfterBind(*opt, k); return; }
            std::size_t n;
            if (mode == SubMode::AlwaysNotLeader || k == 1) {
                n = rcc::encodeErrorResponse(replyBuf, sizeof(replyBuf),
                        static_cast<int16_t>(rp::ErrorCode::NotLeader), std::string_view{},
                        opt->sessionId, opt->correlationId);
            } else {
                n = rcc::encodeSubscribeResponse(replyBuf, sizeof(replyBuf), 0, opt->partition,
                        /*subscriptionId=*/9, opt->sessionId, opt->correlationId);
            }
            if (n > 0) send(replyBuf, n);
        }
    }
};

struct Rig {
    std::string agentDir;
    std::unique_ptr<Client> client;
    std::unique_ptr<SubGateway> gw;
    std::atomic<int> errors{0};

    Rig(const char* name, int32_t inStr, int32_t egStr, SubMode m, int64_t subTimeoutMs) {
        agentDir = uniqueDir(name);
        ClientConfig cfg;
        cfg.gatewayChannel = "aeron:ipc"; cfg.gatewayStreamId = inStr;
        cfg.egressChannel  = "aeron:ipc"; cfg.egressStreamId  = egStr;
        cfg.dataDir = agentDir;
        cfg.connectTimeoutMs = 8000; cfg.pingTimeoutMs = 600000;  // no ping-driven reconnect: it would re-send the pending Subscribe and mask the gap
        cfg.subscribeTimeoutMs = subTimeoutMs;
        cfg.idleMode = IdleMode::LowLatency;
        TopicSpec ts; ts.name = "inventory"; ts.topicId = 1; ts.partitionCount = 1;
        cfg.topics.push_back(ts);
        client = std::make_unique<Client>(cfg);
        client->onError([this](const std::string&) { errors.fetch_add(1); });
        std::this_thread::sleep_for(150ms);
        gw = std::make_unique<SubGateway>(agentDir + "/aeron", inStr, egStr, m);
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rune::client::Completions& q, rune::client::Token t) { return client->connect(q, t); });
        bool ok = false;
        for (int i = 0; i < 6000; ++i) {
            gw->poll(16);
            if (f.ready()) { ok = true; break; }
            std::this_thread::sleep_for(2ms);
        }
        EXPECT_TRUE(ok);
        f.check();
    }
    ~Rig() {
        client->close();
        try { std::filesystem::remove_all(agentDir); } catch (...) {}
    }
    Subscription subscribe(Isolation iso = Isolation::ReadUncommitted, RecordHandler h = [](const Record&) {},
                           From from = From::earliest()) {
        auto th = client->topic("inventory");
        SubscribeOptions o; o.from = from; o.isolation = iso;
        return client->subscribe(th, 0, o, std::move(h));
    }
};
}  // namespace

TEST(SubscribeNotLeaderRegression, BareNotLeaderOnceThenOkBinds) {
    Rig r("snl_a", 4700, 4701, SubMode::OkAfterOneNotLeader, 60'000);
    auto sub = r.subscribe();
    bool bound = false;
    // Within ~5 s: the retry is paced by sub-second backoff.  (Some unrelated
    // mechanism re-sends a pending Subscribe at ~10 s; binding by then would
    // not prove the NotLeader retry.)
    for (int i = 0; i < 2500; ++i) {
        r.gw->poll(16);
        if (sub.subscriptionId() != 0) { bound = true; break; }
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(bound) << "gateway saw " << r.gw->subscribesSeen.load() << " Subscribe(s)";
    EXPECT_TRUE(sub.active());
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::None);
    EXPECT_GE(r.gw->subscribesSeen.load(), 2);
    EXPECT_EQ(r.errors.load(), 0);
    sub.unsubscribe();
}

TEST(SubscribeNotLeaderRegression, BareNotLeaderForeverFailsExplicitlyWithinBudget) {
    // 60 s subscribe timeout: the failure must come from the retry budget, not the clock.
    Rig r("snl_b", 4710, 4711, SubMode::AlwaysNotLeader, 60'000);
    const auto t0 = std::chrono::steady_clock::now();
    auto sub = r.subscribe();
    bool failed = false;
    for (int i = 0; i < 6000 * 3; ++i) {
        r.gw->poll(16);
        if (sub.finished()) { failed = true; break; }
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(failed) << "gateway saw " << r.gw->subscribesSeen.load() << " Subscribe(s)";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 20s);
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::RejectedByBroker);
    EXPECT_EQ(sub.brokerErrorCode(), static_cast<uint16_t>(rp::ErrorCode::NotLeader));
    EXPECT_GE(r.errors.load(), 1);
    EXPECT_GE(r.gw->subscribesSeen.load(), 2);
}

TEST(SubscribeNotLeaderRegression, NoAnswerFailsWithExplicitTimeout) {
    Rig r("snl_c", 4720, 4721, SubMode::Silent, 1'500);
    auto sub = r.subscribe();
    bool failed = false;
    for (int i = 0; i < 6000; ++i) {
        r.gw->poll(16);
        if (sub.finished()) { failed = true; break; }
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(failed);
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::TimedOut);
    EXPECT_GE(r.errors.load(), 1);
}

// Rune E2 Task 13 (Task 11 carry): a BOUND read_committed subscription whose
// node stops leading the partition.  The broker answers its Subscribe
// correlation id with a bare NotLeader and drops it (it can no longer serve
// read_committed).  The client used to ignore that answer (it only retried
// PENDING Subscribes): the subscription stayed bound to a node that would never
// push again — a silent stall.  It now moves: a new Subscribe (fresh
// correlation id) from its resume position, read_committed, partition known;
// the leader binds it and committed data keeps flowing, every record once.
TEST(SubscribeNotLeaderRegression, BoundSubscriptionAnsweredNotLeaderMovesFromItsPositionRegression) {
    Rig r("snl_d", 4730, 4731, SubMode::MovesAfterBind, 60'000);
    std::atomic<uint32_t> seenMask{0};
    std::atomic<int>      seen{0};
    auto sub = r.subscribe(Isolation::ReadCommitted, [&](const Record& rec) {
        if (rec.offset >= 0 && rec.offset < 32) seenMask.fetch_or(1u << rec.offset);
        seen.fetch_add(1);
    });
    bool moved = false;
    for (int i = 0; i < 2500 && !moved; ++i) {   // ~5 s: the NotLeader retry is paced sub-second
        r.gw->poll(16);
        moved = sub.subscriptionId() == 10 && sub.position() == 5;
        if (!moved) std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(moved) << "SILENT STALL: the subscription never moved off the node that stopped leading "
                       << "(gateway saw " << r.gw->subscribesSeen.load() << " Subscribe(s); id "
                       << sub.subscriptionId() << ", position " << sub.position() << ")";
    ASSERT_EQ(r.gw->subscribes.size(), 2u);
    const auto& first = r.gw->subscribes[0];
    const auto& again = r.gw->subscribes[1];
    EXPECT_NE(again.correlationId, first.correlationId) << "the moved Subscribe reused the old instance's identity";
    EXPECT_EQ(again.fromOffset, 3) << "not resumed at the subscription's position";
    EXPECT_EQ(again.isolation, static_cast<uint8_t>(Isolation::ReadCommitted));
    EXPECT_EQ(again.partitionKnown, 1);
    EXPECT_EQ(again.partition, 0);
    EXPECT_EQ(seen.load(), 5) << "a record was lost or delivered twice across the move";
    EXPECT_EQ(seenMask.load(), 0x1Fu);
    EXPECT_TRUE(sub.active());
    EXPECT_EQ(sub.failReason(), SubscriptionFailReason::None);
    EXPECT_EQ(r.errors.load(), 0);
    ASSERT_FALSE(r.gw->unsubscribedIds.empty()) << "the old instance was not released";
    EXPECT_EQ(r.gw->unsubscribedIds.front(), 9);
    sub.unsubscribe();
}

// Rune E2 Task 13 fix round 1: the moved subscription resumes at position 3,
// but the new leader pushes the WHOLE batch containing it (offsets 0..4 —
// brokers never split a batch).  Records 0..2 were already delivered: they
// must not reach the handler again.  RED: 0..2 were delivered twice (8 records).
TEST(SubscribeNotLeaderRegression, MovedSubscriptionDropsTheStraddlingBatchBelowItsPositionRegression) {
    Rig r("snl_e", 4740, 4741, SubMode::MovesMidBatch, 60'000);
    r.gw->secondPushes = {{0, 5}};   // the new leader's batch STRADDLES the resume position 3
    std::atomic<uint32_t> seenMask{0};
    std::atomic<int>      seen{0};
    auto sub = r.subscribe(Isolation::ReadCommitted, [&](const Record& rec) {
        if (rec.offset >= 0 && rec.offset < 32) seenMask.fetch_or(1u << rec.offset);
        seen.fetch_add(1);
    });
    bool moved = false;
    for (int i = 0; i < 2500 && !moved; ++i) {
        r.gw->poll(16);
        moved = sub.subscriptionId() == 10 && sub.position() == 5;
        if (!moved) std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(moved) << "id " << sub.subscriptionId() << ", position " << sub.position();
    for (int i = 0; i < 50; ++i) { r.gw->poll(16); std::this_thread::sleep_for(2ms); }   // let any duplicate land
    EXPECT_EQ(seen.load(), 5) << "records below the resume position were delivered again";
    EXPECT_EQ(seenMask.load(), 0x1Fu);
    sub.unsubscribe();
}

// ---- Task 13 fix round 2 (M3 / M4) -----------------------------------------
namespace {
struct Seen {
    std::atomic<int>      count{0};
    std::atomic<uint64_t> mask{0};
    std::atomic<int>      dupes{0};
    RecordHandler handler() {
        return [this](const Record& rec) {
            if (rec.offset >= 0 && rec.offset < 64) {
                const uint64_t bit = uint64_t{1} << rec.offset;
                if (mask.fetch_or(bit) & bit) dupes.fetch_add(1);
            }
            count.fetch_add(1);
        };
    }
};
bool driveUntil(Rig& r, const std::function<bool()>& pred, int iters = 2500) {
    for (int i = 0; i < iters; ++i) {
        r.gw->poll(16);
        if (pred()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}
void settle(Rig& r) { for (int i = 0; i < 50; ++i) { r.gw->poll(16); std::this_thread::sleep_for(2ms); } }
}  // namespace

// M4: the floor never drops a batch WHOLLY below it — after a move that is not
// a re-delivery (a reset / restarted transient log's new data): delivered.
TEST(SubscribeNotLeaderRegression, MovedSubscriptionDeliversABatchWhollyBelowItsPositionRegression) {
    Rig r("snl_f", 4750, 4751, SubMode::MovesAfterBind, 60'000);
    r.gw->secondPushes = {{0, 2}, {3, 2}};   // 0..1 wholly below position 3, then 3..4
    Seen seen;
    auto sub = r.subscribe(Isolation::ReadCommitted, seen.handler());
    ASSERT_TRUE(driveUntil(r, [&] { return sub.subscriptionId() == 10 && sub.position() == 5; }))
        << "id " << sub.subscriptionId() << ", position " << sub.position();
    settle(r);
    EXPECT_EQ(seen.count.load(), 3 + 2 + 2) << "the wholly-below batch was dropped (or something else was)";
    sub.unsubscribe();
}

// M4: the floor is ONE-SHOT — it applies to the first batch after the move
// only.  Here the first batch (3..4) does not straddle; a later straddling one
// (2..5) is delivered whole: a later batch below the position is not one the
// resume asked for.
TEST(SubscribeNotLeaderRegression, MovedSubscriptionFloorIsOneShotRegression) {
    Rig r("snl_g", 4760, 4761, SubMode::MovesAfterBind, 60'000);
    r.gw->secondPushes = {{3, 2}, {2, 4}};
    Seen seen;
    auto sub = r.subscribe(Isolation::ReadCommitted, seen.handler());
    ASSERT_TRUE(driveUntil(r, [&] { return sub.subscriptionId() == 10 && seen.count.load() >= 3 + 2 + 4; }))
        << "id " << sub.subscriptionId() << ", seen " << seen.count.load();
    settle(r);
    EXPECT_EQ(seen.count.load(), 3 + 2 + 4) << "the floor filtered a batch after the first";
    sub.unsubscribe();
}

// M4: a moved subscription answered REDIRECT (the demoted node names the
// leader) follows it: retarget + reconnect + the re-armed Subscribe resent on
// the new session, from the position.
TEST(SubscribeNotLeaderRegression, MovedSubscriptionAnsweredRedirectFollowsTheLeaderRegression) {
    Rig r("snl_h", 4770, 4771, SubMode::MovesAfterBind, 60'000);
    r.gw->moveCode    = static_cast<int16_t>(rp::ErrorCode::Redirect);
    r.gw->moveMessage = "aeron:ipc";   // the leader's endpoint (same fake gateway)
    Seen seen;
    auto sub = r.subscribe(Isolation::ReadCommitted, seen.handler());
    ASSERT_TRUE(driveUntil(r, [&] { return sub.subscriptionId() == 10 && sub.position() == 5; }))
        << "SILENT STALL: a moved subscription answered Redirect never re-subscribed (id " << sub.subscriptionId()
        << ", position " << sub.position() << ", Subscribes " << r.gw->subscribesSeen.load() << ")";
    ASSERT_EQ(r.gw->subscribes.size(), 2u);
    EXPECT_EQ(r.gw->subscribes[1].fromOffset, 3);
    EXPECT_NE(r.gw->subscribes[1].correlationId, r.gw->subscribes[0].correlationId);
    settle(r);
    EXPECT_EQ(seen.count.load(), 5);
    EXPECT_EQ(seen.dupes.load(), 0);
    EXPECT_EQ(r.errors.load(), 0);
    sub.unsubscribe();
}

// M3: a LATEST subscription bound but with nothing delivered must resume from
// the broker's bind-time resolved position (its position note), never
// re-resolve Latest — records produced around the move would be lost.
TEST(SubscribeNotLeaderRegression, MovedLatestSubscriptionWithNothingDeliveredResumesAtThePositionNoteRegression) {
    Rig r("snl_i", 4780, 4781, SubMode::MovesAfterBind, 60'000);
    r.gw->firstPushes  = {};          // bound, nothing delivered
    r.gw->noteBase     = 7;           // the broker resolved Latest to 7 at bind time
    r.gw->secondPushes = {{7, 2}};
    Seen seen;
    auto sub = r.subscribe(Isolation::ReadCommitted, seen.handler(), From::latest());
    ASSERT_TRUE(driveUntil(r, [&] { return sub.subscriptionId() == 10 && sub.position() == 9; }))
        << "id " << sub.subscriptionId() << ", position " << sub.position();
    ASSERT_EQ(r.gw->subscribes.size(), 2u);
    EXPECT_EQ(r.gw->subscribes[0].fromOffset, -1) << "precondition: subscribed Latest";
    EXPECT_EQ(r.gw->subscribes[1].fromOffset, 7) << "the move re-resolved Latest instead of resuming at the note";
    EXPECT_EQ(seen.count.load(), 2);
    sub.unsubscribe();
}
