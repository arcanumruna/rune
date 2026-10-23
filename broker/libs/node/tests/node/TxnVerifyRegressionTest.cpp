// TxnVerifyRegressionTest.cpp — issue #112 (F15-FU1, KIP-890 part 1): the partition worker's
// transaction-registration verification state (RcState::admitTxnBatch / onVerifyAnswer / onMarker
// over TxnVerifySet).  Pure owner-thread state: no worker, no Aeron.
//
// The worker + gateway + coordinator slices live in F15EosRegressionTest.cpp (TxnVerifyRegression.*),
// the cross-node forwarding in D9CrossNodeMarkerTest.cpp (TxnVerifyRemoteRegression.*).
#include <rune/node/RcState.h>
#include <rune/protocol/client/Protocol.h>

#include "support/AllocWindow.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <random>

namespace {

using rune::node::RcState;
using rune::node::TxnAdmission;
using rune::node::TxnVerifySet;
namespace pc = rune::protocol::client;
using K = TxnAdmission::Kind;

constexpr int16_t code(pc::ErrorCode e) { return static_cast<int16_t>(e); }

struct Rc {
    RcState  rc;
    uint64_t nextReq = 0;
    Rc() { rc.reserve(64); }
    TxnAdmission admit(int64_t pid, int16_t epoch, bool canRequest = true) {
        return rc.admitTxnBatch(pid, epoch, nextReq, canRequest);
    }
};

} // namespace

// A producer with no open transaction here: its first batch asks, every batch while the ask is out is
// refused retriably WITHOUT a second ask, and the confirmation admits the next one.
TEST(TxnVerifyStateRegression, FirstBatchAsksOnceAndIsAdmittedOnceVerifiedRegression) {
    Rc s;
    const TxnAdmission a = s.admit(7, 0);
    ASSERT_EQ(a.kind, K::RequestVerify);
    EXPECT_NE(a.reqId, 0u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(s.admit(7, 0).kind, K::Retry) << "a pending ask is never re-sent";
    EXPECT_EQ(s.rc.verifyRequests(), 1u);
    s.rc.onVerifyAnswer(7, 0, a.reqId, 0);
    EXPECT_EQ(s.admit(7, 0).kind, K::Admit);
    EXPECT_EQ(s.admit(7, 0).kind, K::Admit) << "Verified holds until the append (or the marker) consumes it";
}

// The coordinator's refusal (never registered, fenced, wrong epoch) answers the next batch with that code —
// non-retriable — once; a later batch asks again (the producer may have registered since).
TEST(TxnVerifyStateRegression, RefusalAnswersItsCodeOnceThenAsksAgainRegression) {
    for (const auto c : {pc::ErrorCode::InvalidTxnState, pc::ErrorCode::ProducerFenced,
                         pc::ErrorCode::InvalidProducerEpoch}) {
        Rc s;
        const TxnAdmission a = s.admit(9, 2);
        ASSERT_EQ(a.kind, K::RequestVerify);
        s.rc.onVerifyAnswer(9, 2, a.reqId, code(c));
        const TxnAdmission r = s.admit(9, 2);
        EXPECT_EQ(r.kind, K::Refuse);
        EXPECT_EQ(r.code, code(c));
        const TxnAdmission again = s.admit(9, 2);
        EXPECT_EQ(again.kind, K::RequestVerify);
        EXPECT_NE(again.reqId, a.reqId);
    }
}

// "Cannot answer now" codes drop the entry: the client's retry asks again (never a refusal).
TEST(TxnVerifyStateRegression, RetriableAnswerAsksAgainOnTheNextBatchRegression) {
    for (const auto c : {pc::ErrorCode::NotCoordinator, pc::ErrorCode::NotLeader,
                         pc::ErrorCode::ConcurrentTransactions, pc::ErrorCode::Internal}) {
        Rc s;
        const TxnAdmission a = s.admit(3, 0);
        ASSERT_EQ(a.kind, K::RequestVerify);
        s.rc.onVerifyAnswer(3, 0, a.reqId, code(c));
        EXPECT_EQ(s.rc.verifySet().size(), 0u);
        const TxnAdmission b = s.admit(3, 0);
        EXPECT_EQ(b.kind, K::RequestVerify) << "code " << static_cast<int>(c);
        EXPECT_GT(b.reqId, a.reqId);
    }
}

// Ruling (c), the late-batch race: the producer's marker lands while the ask is out → the answer that was in
// flight (a confirmation computed before the transaction ended) finds no Pending entry and admits nothing.
TEST(TxnVerifyStateRegression, MarkerWhileTheAskIsOutDropsTheLateConfirmationRegression) {
    for (const bool commit : {true, false}) {
        Rc s;
        const TxnAdmission a = s.admit(5, 1);
        ASSERT_EQ(a.kind, K::RequestVerify);
        s.rc.onMarker(5, 100, commit);
        EXPECT_EQ(s.rc.verifySet().size(), 0u);
        s.rc.onVerifyAnswer(5, 1, a.reqId, 0);
        EXPECT_EQ(s.rc.verifyStaleAnswers(), 1u);
        const TxnAdmission b = s.admit(5, 1);
        EXPECT_EQ(b.kind, K::RequestVerify) << "a late batch after the marker must be verified again, never admitted";
    }
}

// The marker clears a Verified entry too (the late batch of an ended transaction is re-verified).
TEST(TxnVerifyStateRegression, MarkerClearsAVerifiedEntryRegression) {
    Rc s;
    const TxnAdmission a = s.admit(5, 1);
    s.rc.onVerifyAnswer(5, 1, a.reqId, 0);
    ASSERT_EQ(s.admit(5, 1).kind, K::Admit);
    s.rc.onMarker(5, 10, false);
    EXPECT_EQ(s.admit(5, 1).kind, K::RequestVerify);
}

// An answer applies only to the Pending entry of its exact request: an older request's answer (another epoch,
// or one superseded by a new ask) is dropped.
TEST(TxnVerifyStateRegression, SupersededOrForeignAnswersAreDroppedRegression) {
    Rc s;
    const TxnAdmission e0 = s.admit(4, 0);
    const TxnAdmission e1 = s.admit(4, 1);   // a new epoch supersedes the ask
    ASSERT_EQ(e1.kind, K::RequestVerify);
    s.rc.onVerifyAnswer(4, 0, e0.reqId, 0);   // the old epoch's confirmation
    EXPECT_EQ(s.admit(4, 1).kind, K::Retry) << "the superseded answer admitted the new epoch";
    s.rc.onVerifyAnswer(4, 1, e0.reqId, 0);   // right epoch, wrong request
    EXPECT_EQ(s.admit(4, 1).kind, K::Retry);
    s.rc.onVerifyAnswer(8, 1, e1.reqId, 0);   // another producer
    EXPECT_EQ(s.admit(4, 1).kind, K::Retry);
    EXPECT_EQ(s.rc.verifyStaleAnswers(), 3u);
    s.rc.onVerifyAnswer(4, 1, e1.reqId, 0);
    EXPECT_EQ(s.admit(4, 1).kind, K::Admit);
}

// No room to send an ask (the worker's outbox is full): Retry WITHOUT opening a verification, so nothing is
// ever Pending without a request in flight.
TEST(TxnVerifyStateRegression, NoRoomToAskAnswersRetryWithoutAPendingEntryRegression) {
    Rc s;
    EXPECT_EQ(s.admit(6, 0, /*canRequest=*/false).kind, K::Retry);
    EXPECT_EQ(s.rc.verifySet().size(), 0u);
    EXPECT_EQ(s.rc.verifyRequests(), 0u);
    EXPECT_EQ(s.admit(6, 0).kind, K::RequestVerify);
}

// A full set refuses a NEW producer retriably; a Rejected entry is evicted to make room.
TEST(TxnVerifyStateRegression, FullSetRefusesNewProducersRetriablyAndEvictsRejectedRegression) {
    Rc s;
    std::vector<TxnAdmission> asks;
    for (int64_t pid = 0; pid < static_cast<int64_t>(RcState::kTxnVerifyCapacity); ++pid) {
        asks.push_back(s.admit(pid, 0));
        ASSERT_EQ(asks.back().kind, K::RequestVerify);
    }
    EXPECT_EQ(s.admit(1000, 0).kind, K::Retry);
    EXPECT_EQ(s.rc.verifyFull(), 1u);
    s.rc.onVerifyAnswer(17, 0, asks[17].reqId, code(pc::ErrorCode::InvalidTxnState));
    EXPECT_EQ(s.admit(1000, 0).kind, K::RequestVerify) << "the Rejected entry was not evicted";
    for (int64_t pid = 0; pid < static_cast<int64_t>(RcState::kTxnVerifyCapacity); ++pid) {
        if (pid == 17) continue;
        EXPECT_EQ(s.admit(pid, 0).kind, K::Retry) << "pid " << pid << " lost its pending entry";
    }
}

// The open-addressing set against a model, seeded: insert / find / erase / evict over colliding probe chains.
TEST(TxnVerifyStateRegression, SetMatchesAModelOverRandomOpsRegression) {
    std::mt19937_64 rng(0x112);
    for (int round = 0; round < 20; ++round) {
        TxnVerifySet set;
        set.reserve(16);
        std::map<int64_t, TxnVerifySet::State> model;
        for (int op = 0; op < 4000; ++op) {
            const int64_t pid = static_cast<int64_t>(rng() % 48);
            switch (rng() % 4) {
            case 0: case 1: {
                if (set.find(pid) != nullptr) break;
                TxnVerifySet::Entry* e = set.insert(pid);
                if (model.size() < 16) {
                    ASSERT_NE(e, nullptr);
                } else if (e != nullptr) {   // evicted a Rejected entry
                    bool hadRejected = false;
                    for (auto it = model.begin(); it != model.end(); ++it)
                        if (it->second == TxnVerifySet::State::Rejected && set.find(it->first) == nullptr) {
                            model.erase(it); hadRejected = true; break;
                        }
                    ASSERT_TRUE(hadRejected);
                }
                if (e != nullptr) {
                    e->state = (rng() % 3 == 0) ? TxnVerifySet::State::Rejected : TxnVerifySet::State::Pending;
                    model[pid] = e->state;
                }
                break;
            }
            case 2: set.erase(pid); model.erase(pid); break;
            default: break;
            }
            ASSERT_EQ(set.size(), model.size());
            for (int64_t q = 0; q < 48; ++q) {
                const auto* e = set.find(q);
                const auto it = model.find(q);
                ASSERT_EQ(e != nullptr, it != model.end()) << "round " << round << " op " << op << " pid " << q;
                if (e != nullptr) ASSERT_EQ(e->state, it->second);
            }
        }
    }
}

// The produce-path decision never allocates once reserved (ownership apply).
TEST(TxnVerifyStateRegression, AdmissionAnswersAndMarkersAreAllocationFreeRegression) {
    Rc s;
    (void)s.admit(1, 0);   // warm
    rune::testing::AllocWindow w;
    for (int i = 0; i < 1000; ++i) {
        const int64_t pid = 100 + (i % 50);
        const TxnAdmission a = s.admit(pid, 0);
        if (a.kind == K::RequestVerify) s.rc.onVerifyAnswer(pid, 0, a.reqId, i % 3 == 0 ? code(pc::ErrorCode::InvalidTxnState) : 0);
        (void)s.admit(pid, 0);
        if (i % 7 == 0) s.rc.onMarker(pid, i, true);
    }
    EXPECT_EQ(w.count(), 0u);
}

// ---- PR #589 adversarial review ---------------------------------------------------------------------------------

// Fencing: a batch of an OLDER epoch (a zombie after the producer re-initialised) must never supersede the entry of
// a newer epoch — neither a Pending ask (its answer would be dropped as stale) nor a Verified one (the live
// producer's admission would be lost).  The zombie is fenced without asking.
TEST(TxnVerifyStateRegression, OlderEpochNeverSupersedesANewerEntryAdversarialRegression) {
    for (const bool verified : {false, true}) {
        SCOPED_TRACE(verified ? "Verified" : "Pending");
        Rc s;
        const TxnAdmission live = s.admit(11, 5);
        ASSERT_EQ(live.kind, K::RequestVerify);
        if (verified) s.rc.onVerifyAnswer(11, 5, live.reqId, 0);
        const TxnAdmission zombie = s.admit(11, 4);
        EXPECT_NE(zombie.kind, K::Admit);
        EXPECT_NE(zombie.kind, K::RequestVerify) << "the zombie re-opened the verification under its old epoch";
        if (!verified) s.rc.onVerifyAnswer(11, 5, live.reqId, 0);
        EXPECT_EQ(s.admit(11, 5).kind, K::Admit) << "the live producer's verification was superseded by a zombie";
    }
}

// Replay: an answer delivered twice (a re-sent op, a duplicated plane frame) after its batch was appended and the
// transaction ended must never re-admit a batch of the ended transaction.
TEST(TxnVerifyStateRegression, ReplayedConfirmationAfterAppendAndMarkerAdmitsNothingAdversarialRegression) {
    Rc s;
    const TxnAdmission a = s.admit(21, 0);
    ASSERT_EQ(a.kind, K::RequestVerify);
    s.rc.onVerifyAnswer(21, 0, a.reqId, 0);
    ASSERT_EQ(s.admit(21, 0).kind, K::Admit);
    s.rc.onVerifyAnswer(21, 0, a.reqId, 0);   // replay while Verified: no-op
    s.rc.onMarker(21, 5, /*commit=*/true);
    s.rc.onVerifyAnswer(21, 0, a.reqId, 0);   // replay after the marker
    EXPECT_EQ(s.admit(21, 0).kind, K::RequestVerify) << "a replayed confirmation admitted a batch after the marker";
    EXPECT_GE(s.rc.verifyStaleAnswers(), 2u);
}

// Input bounds: an answer carrying a code the worker does not know is a refusal (passed through once), never an
// admission and never a retry loop; epochs at the int16 limits behave like any other.
TEST(TxnVerifyStateRegression, UnknownAnswerCodeAndEpochLimitsAdversarialRegression) {
    for (const int16_t epoch : {int16_t{0}, int16_t{32767}, int16_t{-1}}) {
        Rc s;
        const TxnAdmission a = s.admit(31, epoch);
        ASSERT_EQ(a.kind, K::RequestVerify);
        s.rc.onVerifyAnswer(31, epoch, a.reqId, 999);
        const TxnAdmission r = s.admit(31, epoch);
        EXPECT_EQ(r.kind, K::Refuse);
        EXPECT_EQ(r.code, 999);
    }
}

// Resources: the set stays bounded when every producer's ask resolves to a refusal (Rejected entries are evicted
// for new producers, never leaked), over many more producers than its capacity.
TEST(TxnVerifyStateRegression, ManyRefusedProducersNeverExhaustTheSetAdversarialRegression) {
    Rc s;
    for (int64_t pid = 0; pid < 20 * static_cast<int64_t>(RcState::kTxnVerifyCapacity); ++pid) {
        const TxnAdmission a = s.admit(pid, 0);
        ASSERT_EQ(a.kind, K::RequestVerify) << "pid " << pid << " was starved by refused producers";
        s.rc.onVerifyAnswer(pid, 0, a.reqId, code(pc::ErrorCode::InvalidTxnState));
    }
    EXPECT_LE(s.rc.verifySet().size(), RcState::kTxnVerifyCapacity);
}

// ---- PR #589 re-check 1 (adversarial, epoch ordering) ------------------------------------------------------------

// A Rejected entry of a bogus NEWER epoch never locks the real (older-epoch) producer out: it is superseded and the
// real producer verifies; a Verified entry stays intact after a zombie is fenced against it; a zombie against a
// Pending entry is refused retriably without a second ask, even with no room to ask.
TEST(TxnVerifyStateRegression, EpochOrderingAgainstEveryEntryStateAdversarialRegression) {
    {   // Rejected (bogus higher epoch) -> superseded by the real one
        Rc s;
        const TxnAdmission bogus = s.admit(11, 900);
        ASSERT_EQ(bogus.kind, K::RequestVerify);
        s.rc.onVerifyAnswer(11, 900, bogus.reqId, code(pc::ErrorCode::InvalidProducerEpoch));
        const TxnAdmission real = s.admit(11, 3);
        ASSERT_EQ(real.kind, K::RequestVerify) << "a Rejected bogus epoch locked the real producer out";
        s.rc.onVerifyAnswer(11, 3, real.reqId, 0);
        EXPECT_EQ(s.admit(11, 3).kind, K::Admit);
    }
    {   // Verified: zombie fenced, the live admission intact (twice)
        Rc s;
        const TxnAdmission live = s.admit(12, 5);
        s.rc.onVerifyAnswer(12, 5, live.reqId, 0);
        for (int i = 0; i < 2; ++i) {
            const TxnAdmission z = s.admit(12, 4);
            EXPECT_EQ(z.kind, K::Refuse);
            EXPECT_EQ(z.code, code(pc::ErrorCode::ProducerFenced));
        }
        EXPECT_EQ(s.admit(12, 5).kind, K::Admit);
    }
    {   // Pending: zombie Retry, no ask (also with no room), no capacity counter; the live answer still applies
        Rc s;
        const TxnAdmission live = s.admit(13, 5);
        EXPECT_EQ(s.admit(13, 4).kind, K::Retry);
        EXPECT_EQ(s.admit(13, 4, /*canRequest=*/false).kind, K::Retry);
        EXPECT_EQ(s.rc.verifyRequests(), 1u) << "the zombie sent an ask";
        s.rc.onVerifyAnswer(13, 5, live.reqId, 0);
        EXPECT_EQ(s.admit(13, 5).kind, K::Admit);
        // The live entry answered retriably instead: the zombie may then ask (the coordinator fences it).
        Rc t;
        const TxnAdmission l2 = t.admit(14, 5);
        t.rc.onVerifyAnswer(14, 5, l2.reqId, code(pc::ErrorCode::NotCoordinator));
        EXPECT_EQ(t.admit(14, 4).kind, K::RequestVerify);
    }
}
