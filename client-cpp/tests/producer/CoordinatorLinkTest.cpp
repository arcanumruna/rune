// tests/producer/CoordinatorLinkTest.cpp - Phase 2b F Task 2: CoordinatorLink (FD5 / FD6) and the
// ordinal routing of coordinator answers through ProducerHub.  No threads, no sleeps, no atomics: the
// "clock" is the nowNs each call is handed.
#include "../support/AllocCounter.h"
#include "../support/FakeCoordinator.h"

#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/producer/CoordinatorLink.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace rc = rune::client;
namespace rp = rune::client::producer;
using rp::CoordAnswer;
using rp::CoordinatorLink;
using rp::ReqKind;
using rp::Verdict;
using test_support::FakeCoordinator;
namespace rpc = rune::protocol::client;

namespace {
constexpr int16_t kConcurrent   = static_cast<int16_t>(rpc::ErrorCode::ConcurrentTransactions);   // 27
constexpr int16_t kNotCoord     = static_cast<int16_t>(rpc::ErrorCode::NotCoordinator);           // 14
constexpr int16_t kFenced       = static_cast<int16_t>(rpc::ErrorCode::ProducerFenced);           // 18
constexpr int16_t kBadEpoch     = static_cast<int16_t>(rpc::ErrorCode::InvalidProducerEpoch);     // 17
constexpr int16_t kInvalidState = static_cast<int16_t>(rpc::ErrorCode::InvalidTxnState);          // 19
constexpr int16_t kNotAuth      = static_cast<int16_t>(rpc::ErrorCode::NotAuthorized);            // 45
constexpr int16_t kInvalidReq   = static_cast<int16_t>(rpc::ErrorCode::InvalidRequest);           // 39
constexpr int64_t kMs           = 1'000'000;
constexpr int64_t kFar          = int64_t{1} << 60;

/// Encode a request of `kind` into the slot (the link never looks past the 24-byte header).
void encode(CoordinatorLink::Slot& s) {
    if (s.kind == ReqKind::EndTxn) {
        rpc::EndTxn m;
        m.correlationId = s.corrId;
        m.commit        = 1;
        s.frameLen = static_cast<uint16_t>(m.encode(s.frame.data()));
    } else {
        rpc::InitProducerId m;
        m.correlationId = s.corrId;
        s.frameLen = static_cast<uint16_t>(m.encode(s.frame.data()));
    }
}

CoordinatorLink::Slot* send(CoordinatorLink& link, ReqKind kind, int64_t deadlineNs = int64_t{1} << 60) {
    CoordinatorLink::Slot* s = link.acquire(kind, deadlineNs);
    if (s == nullptr) return nullptr;
    encode(*s);
    link.submit(*s);
    return s;
}

CoordAnswer asAnswer(const test_support::AnswerFrame& f, ReqKind kind, int64_t corr, int16_t ec) {
    return {kind, corr, ec, std::span<const uint8_t>(f.data(), f.len), false};
}

int pumpNoExpiry(CoordinatorLink& link, int64_t now) {
    return link.pump(now, [](CoordinatorLink::Slot&) { FAIL() << "unexpected expiry"; });
}
}  // namespace

TEST(CoordinatorLink, AcquireSubmitPumpOffersOnceAndInFlightUntilAnswered) {
    FakeCoordinator fake;
    CoordinatorLink link(3, fake.sink());
    CoordinatorLink::Slot* s = link.acquire(ReqKind::EndTxn, kFar);
    ASSERT_NE(s, nullptr);
    EXPECT_TRUE(rp::coordcorr::isCoord(s->corrId));
    EXPECT_EQ(rp::coordcorr::ordinal(s->corrId), 3u);
    encode(*s);
    // Acquired but not submitted: nothing is offered.
    EXPECT_EQ(pumpNoExpiry(link, 10), 0);
    link.submit(*s);
    EXPECT_EQ(pumpNoExpiry(link, 10), 1);
    ASSERT_EQ(fake.offered.size(), 1u);
    EXPECT_EQ(fake.offered[0].corrId, s->corrId);
    EXPECT_EQ(fake.offered[0].sessionId, 77) << "the frame is stamped with the coordinator session";
    EXPECT_TRUE(std::holds_alternative<CoordinatorLink::InFlight>(s->state));
    // InFlight: later passes offer nothing.
    EXPECT_EQ(pumpNoExpiry(link, 20), 0);
    EXPECT_EQ(fake.offered.size(), 1u);
    // The answer settles it: handed back, then released.
    const auto f = FakeCoordinator::answer(ReqKind::EndTxn, s->corrId, 0);
    CoordinatorLink::Slot* done = link.onAnswer(asAnswer(f, ReqKind::EndTxn, s->corrId, 0), 30);
    ASSERT_EQ(done, s);
    EXPECT_EQ(link.inUse(), 1u);
    link.release(*done);
    EXPECT_EQ(link.inUse(), 0u);
}

TEST(CoordinatorLink, NotConnectedHoldsTheFrameUntilASessionExists) {
    FakeCoordinator fake;
    fake.sid = 0;
    CoordinatorLink link(0, fake.sink());
    ASSERT_NE(send(link, ReqKind::InitProducerId), nullptr);
    EXPECT_EQ(pumpNoExpiry(link, 1), 0);
    EXPECT_TRUE(fake.offered.empty());
    fake.sid = 5;
    EXPECT_EQ(pumpNoExpiry(link, 2), 1);
    EXPECT_EQ(fake.offered.at(0).sessionId, 5);
    // A transiently refused offer (Retry) stays Unsent and is re-offered next pass.
    FakeCoordinator busy;
    busy.next = rp::CoordinatorSink::Offer::Retry;
    CoordinatorLink l2(0, busy.sink());
    CoordinatorLink::Slot* s = send(l2, ReqKind::EndTxn);
    EXPECT_EQ(pumpNoExpiry(l2, 1), 0);
    busy.next = rp::CoordinatorSink::Offer::Accepted;
    EXPECT_EQ(pumpNoExpiry(l2, 2), 1);
    EXPECT_EQ(busy.offeredFor(s->corrId), 1u);
}

TEST(CoordinatorLink, ConcurrentTransactionsBacksOffOnTheDutyClockNotCycles) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    CoordinatorLink::Slot* s = send(link, ReqKind::AddOffsets);
    ASSERT_NE(s, nullptr);
    ASSERT_EQ(pumpNoExpiry(link, 0), 1);
    int64_t now = 1000;
    for (uint32_t attempt = 1; attempt <= 8; ++attempt) {
        const auto f = FakeCoordinator::answer(ReqKind::AddOffsets, s->corrId, kConcurrent);
        EXPECT_EQ(link.onAnswer(asAnswer(f, ReqKind::AddOffsets, s->corrId, kConcurrent), now), nullptr);
        const int64_t delay = attempt == 8 ? 100 * kMs : (int64_t{1} << (attempt - 1)) * kMs;
        EXPECT_EQ(CoordinatorLink::backoffNs(attempt), delay) << "attempt " << attempt;
        ASSERT_TRUE(std::holds_alternative<CoordinatorLink::Backoff>(s->state));
        const std::size_t before = fake.offered.size();
        // Any number of duty passes before untilNs offer nothing (wall time, not cycles).
        for (int i = 0; i < 1000; ++i) EXPECT_EQ(pumpNoExpiry(link, now + delay - 1), 0);
        EXPECT_EQ(fake.offered.size(), before);
        EXPECT_EQ(pumpNoExpiry(link, now + delay), 1) << "due exactly at untilNs";
        EXPECT_EQ(fake.offered.size(), before + 1);
        now += delay;
    }
    EXPECT_EQ(CoordinatorLink::backoffNs(30), 100 * kMs) << "capped";
    EXPECT_EQ(fake.offeredFor(s->corrId), 9u) << "the same id on every re-offer";
}

TEST(CoordinatorLink, NotCoordinatorWaitsForTheMoveThenResends) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    CoordinatorLink::Slot* s = send(link, ReqKind::EndTxn);
    ASSERT_EQ(pumpNoExpiry(link, 0), 1);
    const auto f = FakeCoordinator::errorResponse(s->corrId, rpc::ErrorCode::NotCoordinator, "node2");
    CoordAnswer a{ReqKind::EndTxn, s->corrId, kNotCoord, std::span<const uint8_t>(f.data(), f.len), true};
    EXPECT_EQ(link.onAnswer(a, 10), nullptr);
    EXPECT_TRUE(std::holds_alternative<CoordinatorLink::AwaitCoordinator>(s->state));
    for (int i = 0; i < 100; ++i) EXPECT_EQ(pumpNoExpiry(link, 1'000'000'000 + i), 0) << "no move, no re-send";
    EXPECT_EQ(fake.offered.size(), 1u);
    link.onCoordinatorMoved();
    EXPECT_EQ(pumpNoExpiry(link, 2'000'000'000), 1);
    ASSERT_EQ(fake.offered.size(), 2u);
    EXPECT_EQ(fake.offered[1].corrId, s->corrId) << "the same correlation id";
    // A second NotCoordinator waits for the NEXT move, not the one already seen.
    EXPECT_EQ(link.onAnswer(a, 3), nullptr);
    EXPECT_EQ(pumpNoExpiry(link, 4), 0);
    link.onCoordinatorMoved();
    EXPECT_EQ(pumpNoExpiry(link, 5), 1);
}

TEST(CoordinatorLink, SessionChangeResendsInFlightWithTheSameCorrId) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    CoordinatorLink::Slot* s = send(link, ReqKind::AddPartitions);
    ASSERT_EQ(pumpNoExpiry(link, 1), 1);
    ASSERT_EQ(fake.offered.size(), 1u);
    fake.sid = 88;
    link.onSessionChanged();
    EXPECT_TRUE(std::holds_alternative<CoordinatorLink::Unsent>(s->state));
    EXPECT_EQ(pumpNoExpiry(link, 2), 1);
    ASSERT_EQ(fake.offered.size(), 2u);
    EXPECT_EQ(fake.offered[1].corrId, fake.offered[0].corrId);
    EXPECT_EQ(fake.offered[0].sessionId, 77);
    EXPECT_EQ(fake.offered[1].sessionId, 88) << "re-stamped with the new session";
    // The old session's late answer still settles it (idempotent at the coordinator).
    const auto f = FakeCoordinator::answer(ReqKind::AddPartitions, s->corrId, 0);
    EXPECT_EQ(link.onAnswer(asAnswer(f, ReqKind::AddPartitions, s->corrId, 0), 3), s);
}

TEST(CoordinatorLink, DeadlineExpiresIntoOnExpiredExactlyOnce) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    CoordinatorLink::Slot* s = send(link, ReqKind::EndTxn, 5000);
    const int64_t corr = s->corrId;
    EXPECT_EQ(pumpNoExpiry(link, 4999), 1);
    int expired = 0;
    int64_t seen = 0;
    auto count = [&](CoordinatorLink::Slot& x) { ++expired; seen = x.corrId; };
    EXPECT_EQ(link.pump(5000, count), 1);
    EXPECT_EQ(expired, 1);
    EXPECT_EQ(seen, corr);
    EXPECT_EQ(link.pump(6000, count), 0);
    EXPECT_EQ(link.pump(7000, count), 0);
    EXPECT_EQ(expired, 1) << "exactly once";
    EXPECT_EQ(link.inUse(), 0u) << "the expired slot is free again";
    // A late answer for the expired request finds no slot.
    const auto f = FakeCoordinator::answer(ReqKind::EndTxn, corr, 0);
    EXPECT_EQ(link.onAnswer(asAnswer(f, ReqKind::EndTxn, corr, 0), 8000), nullptr);
    // An expired request that never found a session is not offered at all.
    FakeCoordinator down;
    down.sid = 0;
    CoordinatorLink l2(0, down.sink());
    ASSERT_NE(send(l2, ReqKind::InitProducerId, 10), nullptr);
    int e2 = 0;
    EXPECT_EQ(l2.pump(10, [&](CoordinatorLink::Slot&) { ++e2; }), 1);
    EXPECT_EQ(e2, 1);
    EXPECT_TRUE(down.offered.empty());
}

TEST(CoordinatorLink, NoFreeSlotReturnsNullNeverAllocates) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    std::vector<CoordinatorLink::Slot*> held;
    held.reserve(CoordinatorLink::kMaxCoordRequests);
    for (std::size_t i = 0; i < CoordinatorLink::kMaxCoordRequests; ++i) {
        CoordinatorLink::Slot* s = link.acquire(ReqKind::EndTxn, kFar);
        ASSERT_NE(s, nullptr) << i;
        held.push_back(s);
    }
    EXPECT_EQ(link.acquire(ReqKind::EndTxn, kFar), nullptr) << "the 9th acquire";
    EXPECT_EQ(link.inUse(), CoordinatorLink::kMaxCoordRequests);
    for (auto* s : held) link.release(*s);
    EXPECT_EQ(link.inUse(), 0u);

    // 10 000 acquire / submit / pump / answer / release cycles allocate nothing (the fake's capture vector
    // is pre-reserved; its push_back stays within capacity).
    fake.offered.clear();
    alloc_counter::startThisThread();
    for (int i = 0; i < 10'000; ++i) {
        CoordinatorLink::Slot* s = link.acquire(ReqKind::AddPartitions, kFar);
        if (s == nullptr) break;
        encode(*s);
        link.submit(*s);
        (void)link.pump(i, [](CoordinatorLink::Slot&) {});
        const auto f = FakeCoordinator::answer(ReqKind::AddPartitions, s->corrId, 0);
        CoordinatorLink::Slot* done = link.onAnswer(asAnswer(f, ReqKind::AddPartitions, s->corrId, 0), i);
        if (done == nullptr) break;
        link.release(*done);
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0);

    EXPECT_EQ(link.inUse(), 0u);
}

// FD6 verdict table (Kafka parity + auth).
TEST(CoordinatorLink, VerdictTableFollowsFD6) {
    using V = Verdict;
    auto c = CoordinatorLink::classify;
    for (ReqKind k : {ReqKind::InitProducerId, ReqKind::AddPartitions, ReqKind::AddOffsets, ReqKind::TxnOffsetCommit,
                      ReqKind::EndTxn}) {
        EXPECT_EQ(c(k, 0), V::Ok);
        EXPECT_EQ(c(k, kConcurrent), V::Retry);
        EXPECT_EQ(c(k, kNotCoord), V::Rediscover);
        EXPECT_EQ(c(k, kFenced), V::Fenced);
        EXPECT_EQ(c(k, kBadEpoch), V::Fenced);
        EXPECT_EQ(c(k, kInvalidReq), V::Reject) << "39 is never retried";
    }
    EXPECT_EQ(c(ReqKind::InitProducerId, kInvalidState), V::Fatal);
    EXPECT_EQ(c(ReqKind::InitProducerId, kNotAuth), V::Fatal);
    EXPECT_EQ(c(ReqKind::EndTxn, kNotAuth), V::Fatal);
    EXPECT_EQ(c(ReqKind::EndTxn, kInvalidState), V::Fatal);
    EXPECT_EQ(c(ReqKind::EndTxn, 99), V::Retry) << "EndTxn is retried until 0 / 19 / 17 / 18";
    for (ReqKind k : {ReqKind::AddPartitions, ReqKind::AddOffsets, ReqKind::TxnOffsetCommit}) {
        EXPECT_EQ(c(k, kNotAuth), V::Abortable);
        EXPECT_EQ(c(k, kInvalidState), V::Abortable);
        EXPECT_EQ(c(k, 99), V::Abortable);
    }
    for (int16_t auth = 44; auth <= 47; ++auth) EXPECT_NE(c(ReqKind::AddPartitions, auth), V::Retry) << auth;
}

TEST(CoordinatorLink, NonRetriableAnswersAreHandedBackAndLateOnesIgnored) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    CoordinatorLink::Slot* s = send(link, ReqKind::AddPartitions);
    ASSERT_EQ(pumpNoExpiry(link, 1), 1);
    // A 19 settles (the caller classifies Abortable).
    const auto f = FakeCoordinator::answer(ReqKind::AddPartitions, s->corrId, kInvalidState);
    CoordinatorLink::Slot* done = link.onAnswer(asAnswer(f, ReqKind::AddPartitions, s->corrId, kInvalidState), 2);
    ASSERT_EQ(done, s);
    link.release(*done);
    // The same answer again (a duplicate): no live slot.
    EXPECT_EQ(link.onAnswer(asAnswer(f, ReqKind::AddPartitions, s->corrId, kInvalidState), 3), nullptr);
    // A wrong-kind answer for a live id is not taken for its request.
    CoordinatorLink::Slot* t = send(link, ReqKind::EndTxn);
    const auto g = FakeCoordinator::answer(ReqKind::InitProducerId, t->corrId, 0);
    EXPECT_EQ(link.onAnswer(asAnswer(g, ReqKind::InitProducerId, t->corrId, 0), 4), nullptr);
    // A duplicate 27 while the re-send is already pending does not double the backoff.
    ASSERT_EQ(pumpNoExpiry(link, 5), 1);
    const auto r = FakeCoordinator::answer(ReqKind::EndTxn, t->corrId, kConcurrent);
    EXPECT_EQ(link.onAnswer(asAnswer(r, ReqKind::EndTxn, t->corrId, kConcurrent), 6), nullptr);
    EXPECT_EQ(link.onAnswer(asAnswer(r, ReqKind::EndTxn, t->corrId, kConcurrent), 7), nullptr);
    EXPECT_EQ(t->attempt, 1u);
}

// ---------------------------------------------------------------------------
// Routing by producer ordinal.
// ---------------------------------------------------------------------------

namespace {
rc::ProducerConfig plainCfg() {
    rc::ProducerConfig c;
    c.acks = rc::Acks::None;
    c.lingerUs = 0;
    c.batchBytes = 16 * 1024;
    return c;
}
}  // namespace

TEST(CoordinatorLinkRegression, AResponseReachesOnlyItsProducerRegression) {
    FakeCoordinator fake;
    rp::ProducerHub hub;
    hub.setCoordinatorSink(fake.sink());
    std::vector<std::unique_ptr<rp::ProducerImpl>> ps;
    for (int i = 0; i < 3; ++i) {
        ps.push_back(std::make_unique<rp::ProducerImpl>(plainCfg(), 1 << 16));
        hub.add(ps.back().get());
    }
    rp::ProducerImpl& p1 = *ps[1];
    rp::ProducerImpl& p2 = *ps[2];
    ASSERT_EQ(p1.ordinal(), 1u);
    ASSERT_EQ(p2.ordinal(), 2u);
    ASSERT_TRUE(p1.hasCoordinator());
    ASSERT_TRUE(p2.hasCoordinator());
    CoordinatorLink::Slot* s1 = send(*p1.coordinatorLink(), ReqKind::InitProducerId);
    CoordinatorLink::Slot* s2 = send(*p2.coordinatorLink(), ReqKind::InitProducerId);
    ASSERT_NE(s1, nullptr);
    ASSERT_NE(s2, nullptr);
    const int64_t corr1 = s1->corrId;
    const int64_t corr2 = s2->corrId;
    EXPECT_NE(corr1, corr2);
    EXPECT_EQ(rp::coordcorr::ordinal(corr2), 2u);

    const auto f2 = FakeCoordinator::answer(ReqKind::InitProducerId, corr2, 0);
    EXPECT_TRUE(hub.onCoordinatorResponse(f2.templateId, f2.data(), f2.len, 10));
    EXPECT_EQ(p2.coordAnswersSeen(), 1u);
    EXPECT_EQ(p2.lastCoordAnswerCorrId(), corr2);
    EXPECT_EQ(p2.coordinatorLink()->inUse(), 0u) << "the settled slot is released";
    EXPECT_EQ(p1.coordAnswersSeen(), 0u) << "never broadcast";
    EXPECT_EQ(ps[0]->coordAnswersSeen(), 0u);

    const auto f1 = FakeCoordinator::answer(ReqKind::InitProducerId, corr1, 0);
    EXPECT_TRUE(hub.onCoordinatorResponse(f1.templateId, f1.data(), f1.len, 11));
    EXPECT_EQ(p1.coordAnswersSeen(), 1u);
    EXPECT_EQ(p2.coordAnswersSeen(), 1u);

    // An ErrorResponse{NotCoordinator} for p2's id reaches p2 only, and parks its request.
    CoordinatorLink::Slot* s2b = send(*p2.coordinatorLink(), ReqKind::EndTxn);
    ASSERT_NE(s2b, nullptr);
    (void)p2.pumpCoordinator(12);
    const auto ne = FakeCoordinator::errorResponse(s2b->corrId, rpc::ErrorCode::NotCoordinator, "node3");
    EXPECT_TRUE(hub.onCoordinatorResponse(ne.templateId, ne.data(), ne.len, 13));
    EXPECT_EQ(p2.coordAnswersSeen(), 2u);
    EXPECT_EQ(p1.coordAnswersSeen(), 1u);
    EXPECT_TRUE(std::holds_alternative<CoordinatorLink::AwaitCoordinator>(s2b->state));

    // An id naming no producer, and a non-coordinator id, reach nobody.
    const auto nobody = FakeCoordinator::answer(ReqKind::EndTxn, rp::coordcorr::make(9, 0, 1), 0);
    EXPECT_FALSE(hub.onCoordinatorResponse(nobody.templateId, nobody.data(), nobody.len, 14));
    const auto client = FakeCoordinator::answer(ReqKind::EndTxn, (int64_t{1} << 59) | 5, 0);
    EXPECT_FALSE(hub.onCoordinatorResponse(client.templateId, client.data(), client.len, 15));
    EXPECT_EQ(p1.coordAnswersSeen() + p2.coordAnswersSeen() + ps[0]->coordAnswersSeen(), 3u);

    // The move event reaches the links: p2's parked request re-sends on the next pass.
    const std::size_t before = fake.offered.size();
    hub.onCoordinatorMoved();
    EXPECT_EQ(p2.pumpCoordinator(20), 1);
    EXPECT_EQ(fake.offered.size(), before + 1);
    // A session change re-sends in-flight requests.
    hub.onCoordinatorSessionChanged();
    EXPECT_EQ(p2.pumpCoordinator(21), 1);
    EXPECT_EQ(fake.offered.size(), before + 2);
}

TEST(CoordinatorLinkRegression, CoordIdsNeverCollideWithBatchOrFetchIdsRegression) {
    // The tag is distinct from every tag in use: batches (62), fetch (61), offset-fetch (60) and the
    // Client's own counter (59) - the plan's 1 << 59 overlapped the last one.
    static_assert((rp::coordcorr::kTag & rp::batchcorr::kTag) == 0);
    static_assert(rp::coordcorr::kTag != rc::group::GroupConsumerHub::kFetchCorrTag);
    for (uint32_t ord : {0u, 1u, 4095u}) {
        for (uint8_t slot : {uint8_t{0}, uint8_t{7}, uint8_t{255}}) {
            for (uint32_t seq : {0u, 1u, 0xFFFFFFFFu}) {
                const int64_t id = rp::coordcorr::make(ord, slot, seq);
                EXPECT_TRUE(rp::coordcorr::isCoord(id));
                EXPECT_FALSE(rp::batchcorr::isBatch(id)) << "a coordinator id is never taken for a batch";
                EXPECT_FALSE(rc::group::GroupConsumerHub::isFetchCorrId(id));
                EXPECT_EQ(id & rc::group::GroupConsumerHub::kOffsetFetchCorrTag, 0);
                EXPECT_EQ(rp::coordcorr::ordinal(id), ord);
                EXPECT_EQ(rp::coordcorr::slot(id), slot);
            }
        }
    }
    for (uint32_t x : {0u, 1u, 12345u}) {
        EXPECT_FALSE(rp::coordcorr::isCoord(rp::batchcorr::make(3, 9, x)));
        EXPECT_FALSE(rp::coordcorr::isCoord(rc::group::GroupConsumerHub::kFetchCorrTag | x));
        EXPECT_FALSE(rp::coordcorr::isCoord(rc::group::GroupConsumerHub::kOffsetFetchCorrTag | x));
        EXPECT_FALSE(rp::coordcorr::isCoord(rp::coordcorr::kClientCounterTag | x))
            << "the Client's counter ids (every control op, the legacy txn producer) are not coordinator ids";
    }
    EXPECT_FALSE(rp::coordcorr::isCoord(-5)) << "keepalive ids are negative";
    EXPECT_FALSE(rp::coordcorr::isCoord(0));
}

// ---------------------------------------------------------------------------
// Fix round 1.
// ---------------------------------------------------------------------------

TEST(CoordinatorLinkRegression, AStaleAnswerAfterOrdinalReuseIsIgnoredRegression) {
    // Ordinals are reused only once all 4096 fresh ones are taken: fill 4095 link-less producers, then the
    // linked one takes the last fresh ordinal; releasing it makes its ordinal the next one handed out.
    FakeCoordinator fake;
    rp::ProducerHub hub;
    std::vector<std::unique_ptr<rp::ProducerImpl>> fillers;
    fillers.reserve(4095);
    for (int i = 0; i < 4095; ++i) {
        fillers.push_back(std::make_unique<rp::ProducerImpl>(plainCfg(), 1 << 10));
        hub.add(fillers.back().get());
    }
    hub.setCoordinatorSink(fake.sink());
    auto oldP = std::make_unique<rp::ProducerImpl>(plainCfg(), 1 << 10);
    hub.add(oldP.get());
    const uint32_t ord = oldP->ordinal();
    ASSERT_EQ(ord, 4095u);
    CoordinatorLink::Slot* so = send(*oldP->coordinatorLink(), ReqKind::InitProducerId);
    ASSERT_NE(so, nullptr);
    const int64_t oldCorr = so->corrId;
    hub.remove(oldP.get());
    oldP.reset();

    auto newP = std::make_unique<rp::ProducerImpl>(plainCfg(), 1 << 10);
    hub.add(newP.get());
    ASSERT_EQ(newP->ordinal(), ord) << "the ordinal was reused";
    CoordinatorLink::Slot* sn = send(*newP->coordinatorLink(), ReqKind::InitProducerId);
    ASSERT_NE(sn, nullptr);
    const int64_t newCorr = sn->corrId;
    EXPECT_NE(newCorr, oldCorr) << "ids never repeat across incarnations of an ordinal";


    // The old producer's late answer is dropped by the new link (its request stays pending)...
    const auto stale = FakeCoordinator::answer(ReqKind::InitProducerId, oldCorr, 0);
    (void)hub.onCoordinatorResponse(stale.templateId, stale.data(), stale.len, 5);
    EXPECT_EQ(newP->coordinatorLink()->inUse(), 1u) << "the stale answer settled nothing";
    EXPECT_TRUE(std::holds_alternative<CoordinatorLink::Unsent>(sn->state));
    // ...and the new producer's own answer is accepted.
    const auto mine = FakeCoordinator::answer(ReqKind::InitProducerId, newCorr, 0);
    EXPECT_TRUE(hub.onCoordinatorResponse(mine.templateId, mine.data(), mine.len, 6));
    EXPECT_EQ(newP->coordinatorLink()->inUse(), 0u) << "its own answer settled it";
    hub.remove(newP.get());
}

TEST(CoordinatorLinkRegression, AnIdleLinkTouchesNoSlotMemoryRegression) {
    FakeCoordinator fake;
    rp::ProducerHub hub;
    hub.setCoordinatorSink(fake.sink());
    rc::ProducerConfig cfg = plainCfg();
    cfg.acks = rc::Acks::All;
    rp::ProducerImpl p(cfg, 1 << 16);
    hub.add(&p);
    ASSERT_TRUE(p.hasCoordinator());
    for (int i = 0; i < 1000; ++i) EXPECT_EQ(p.pumpCoordinator(i), 0);
    const rp::CoordinatorLink& link = *p.coordinatorLink();
    EXPECT_EQ(link.scans(), 0u) << "no live request: the duty pass never walks the slots";
    CoordinatorLink::Slot* s = send(*p.coordinatorLink(), ReqKind::EndTxn);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(p.pumpCoordinator(1), 1);
    EXPECT_GE(link.scans(), 1u);
    p.coordinatorLink()->release(*s);
    const uint64_t scans = link.scans();
    for (int i = 0; i < 1000; ++i) (void)p.pumpCoordinator(i);
    EXPECT_EQ(link.scans(), scans) << "idle again";
}

TEST(CoordinatorLinkRegression, AnAwaitCoordinatorRequestTimesOutAtItsDeadlineRegression) {
    FakeCoordinator fake;
    CoordinatorLink link(0, fake.sink());
    CoordinatorLink::Slot* s = send(link, ReqKind::EndTxn, 9000);
    ASSERT_EQ(pumpNoExpiry(link, 1), 1);
    const auto f = FakeCoordinator::errorResponse(s->corrId, rpc::ErrorCode::NotCoordinator, "node2");
    CoordAnswer a{ReqKind::EndTxn, s->corrId, kNotCoord, std::span<const uint8_t>(f.data(), f.len), true};
    ASSERT_EQ(link.onAnswer(a, 2), nullptr);
    ASSERT_TRUE(std::holds_alternative<CoordinatorLink::AwaitCoordinator>(s->state));
    EXPECT_EQ(pumpNoExpiry(link, 8999), 0) << "no move: still waiting";
    int expired = 0;
    EXPECT_EQ(link.pump(9000, [&](CoordinatorLink::Slot&) { ++expired; }), 1);
    EXPECT_EQ(expired, 1) << "completes Timeout at its deadline";
    EXPECT_EQ(link.inUse(), 0u);
    // Fatal offers (consumed, never delivered) expire too.
    FakeCoordinator dead;
    dead.next = rp::CoordinatorSink::Offer::Fatal;
    CoordinatorLink l2(0, dead.sink());
    ASSERT_NE(send(l2, ReqKind::InitProducerId, 500), nullptr);
    ASSERT_EQ(pumpNoExpiry(l2, 1), 1);
    int e2 = 0;
    EXPECT_EQ(l2.pump(500, [&](CoordinatorLink::Slot&) { ++e2; }), 1);
    EXPECT_EQ(e2, 1);
}

TEST(CoordinatorLinkRegression, AnErrorResponseForACoordinatorIdSettlesItWithItsErrorRegression) {
    FakeCoordinator fake;
    rp::ProducerHub hub;
    hub.setCoordinatorSink(fake.sink());
    rp::ProducerImpl p(plainCfg(), 1 << 16);
    hub.add(&p);
    // Link level: a non-NotCoordinator ErrorResponse (45 NotAuthorized on AddPartitions) is handed back.
    CoordinatorLink::Slot* s = send(*p.coordinatorLink(), ReqKind::AddPartitions);
    ASSERT_NE(s, nullptr);
    const int64_t corr = s->corrId;
    ASSERT_EQ(p.pumpCoordinator(1), 1);
    const auto e = FakeCoordinator::errorResponse(corr, rpc::ErrorCode::NotAuthorized, "denied");
    CoordAnswer a{ReqKind::AddPartitions, corr, kNotAuth, std::span<const uint8_t>(e.data(), e.len), true};
    EXPECT_EQ(CoordinatorLink::classify(ReqKind::AddPartitions, kNotAuth), Verdict::Abortable);
    // Hub level: it is routed to its producer (and settles the slot instead of waiting for the deadline).
    EXPECT_TRUE(hub.onCoordinatorResponse(e.templateId, e.data(), e.len, 2));
    EXPECT_EQ(p.coordAnswersSeen(), 1u);
    EXPECT_EQ(p.coordinatorLink()->inUse(), 0u);
    // And a 39 InvalidRequest for an EndTxn settles it too (never retried).
    CoordinatorLink::Slot* t = send(*p.coordinatorLink(), ReqKind::EndTxn);
    ASSERT_NE(t, nullptr);
    const auto bad = FakeCoordinator::errorResponse(t->corrId, rpc::ErrorCode::InvalidRequest, "bad");
    EXPECT_TRUE(hub.onCoordinatorResponse(bad.templateId, bad.data(), bad.len, 3));
    EXPECT_EQ(p.coordinatorLink()->inUse(), 0u);
    (void)a;
}
