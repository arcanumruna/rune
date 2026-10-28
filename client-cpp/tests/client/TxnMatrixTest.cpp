// tests/client/TxnMatrixTest.cpp - Phase 2b F Task 12: the transaction interaction matrix.
//
// 10 ops x 4 entities x 8 timings = 320 cells.  The expectations live in tests/support/TxnMatrixExpectations.h
// (expect() / naRule()); this file is the rig that proves them: a real Client in Invoker mode over Aeron IPC to
// the InvokerGateway in its transaction-aware mode (a read-committed log, scripted 19 / 18 answers, a fence,
// dropped sessions, the per-partition frame order).  Single-threaded, condition-driven: every wait is a bounded
// deadline loop, never a sleep.  A fresh transactional id / group / topic name per cell.
//
// Every applicable cell asserts the plan's five points:
//   1. the outcome's codes from expect() (no cell hard-codes one);
//   2. every accepted op completes exactly once, and a refused op had no side effect (the fake saw no frame);
//   3. no transactional batch before its partition's AddPartitions OK (the fake's frame-order log);
//   4. the TRANSACTIONAL attribute is on every batch of a transactional producer and on no plain one;
//   5. zero allocation inside the op (a window around the issue call, after a warm-up pass of the same cell).
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/TxnMatrixExpectations.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerRef.h>

#include "gen/position.gen.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>
#include <unistd.h>

namespace rc = rune::client;
namespace mx = rune::client::txnmatrix;
using Gw     = test_support::InvokerGateway;

// ---------------------------------------------------------------------------------------------------------------
// The pinned cell counts (the expectations header is the single source; this proves its arithmetic).
// ---------------------------------------------------------------------------------------------------------------
TEST(TxnMatrixCountRegression, CellCountsArePinned) {
    int total = 0, applicable = 0, na = 0;
    int naRules[3]{};
    int rules[9]{};   // by Outcome
    int refusedAt[mx::kTimings]{};   // R4 / R5 / R6 are the Refused outcome at BeforeInit / OutsideTxn / AfterCommitIssued
    for (uint8_t o = 0; o < mx::kOps; ++o) {
        for (uint8_t e = 0; e < mx::kEntities; ++e) {
            for (uint8_t t = 0; t < mx::kTimings; ++t) {
                const auto op = static_cast<mx::Op>(o);
                const auto en = static_cast<mx::Entity>(e);
                const auto tm = static_cast<mx::Timing>(t);
                ++total;
                if (const auto r = mx::naRule(op, en, tm)) {
                    ++na;
                    ++naRules[static_cast<uint8_t>(*r)];
                } else {
                    ++applicable;
                    ++rules[static_cast<uint8_t>(mx::expect(op, en, tm))];
                    if (mx::expect(op, en, tm) == mx::Outcome::Refused) ++refusedAt[t];
                }
            }
        }
    }
    EXPECT_EQ(total, 320);
    EXPECT_EQ(applicable, 188);
    EXPECT_EQ(na, 132);
    EXPECT_EQ(naRules[static_cast<uint8_t>(mx::NaRule::NA1)], 120);
    EXPECT_EQ(naRules[static_cast<uint8_t>(mx::NaRule::NA2)], 4);
    EXPECT_EQ(naRules[static_cast<uint8_t>(mx::NaRule::NA3)], 8);
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::Delivered)], 18);       // R1
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::DeliveredOnce)], 9);    // R2
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::AtMostOnce)], 9);       // R3
    // R4 / R5 / R6 share the Refused outcome, told apart by their timing: 19 cells each (2 transactional entities x 10
    // ops, less the NA3 offsets cell of the new-partition entity).  R7..R11 one outcome each: 19 per timing.
    EXPECT_EQ(refusedAt[static_cast<uint8_t>(mx::Timing::BeforeInit)], 19) << "R4";
    EXPECT_EQ(refusedAt[static_cast<uint8_t>(mx::Timing::OutsideTxn)], 19) << "R5";
    EXPECT_EQ(refusedAt[static_cast<uint8_t>(mx::Timing::AfterCommitIssued)], 19) << "R6";
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::Refused)], refusedAt[static_cast<uint8_t>(mx::Timing::BeforeInit)] +
                                                                    refusedAt[static_cast<uint8_t>(mx::Timing::OutsideTxn)] +
                                                                    refusedAt[static_cast<uint8_t>(mx::Timing::AfterCommitIssued)])
        << "Refused occurs at no other timing";
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::CommittedVisible)], 19);
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::FailsTxn)], 19);
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::Fenced)], 19);
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::CommittedVisibleOnce)], 19);
    EXPECT_EQ(rules[static_cast<uint8_t>(mx::Outcome::RedoneOnce)], 19);
}

// #369: expected() is the single source of every status code a handler asserts: its outcome is expect()'s, and its
// codes are consistent per rule.  The cell counts per (rule, op status, end status) are pinned from the plan.
TEST(TxnMatrixCountRegression, ExpectedStatusCodesAreConsistentPerRuleRegression) {
    // counts[outcome][opStatus][endStatus]
    int counts[9][3][3]{};
    int r9OpFenced = 0, r9OpOk = 0;
    for (uint8_t o = 0; o < mx::kOps; ++o) {
        for (uint8_t e = 0; e < mx::kEntities; ++e) {
            for (uint8_t t = 0; t < mx::kTimings; ++t) {
                const auto op = static_cast<mx::Op>(o);
                const auto en = static_cast<mx::Entity>(e);
                const auto tm = static_cast<mx::Timing>(t);
                if (mx::naRule(op, en, tm)) continue;
                const mx::Expected x = mx::expected(op, en, tm);
                ASSERT_EQ(x.outcome, mx::expect(op, en, tm)) << mx::name(op) << "/" << mx::name(en) << "/" << mx::name(tm);
                // A code is carried only by BrokerError; Ok and Fenced pin none.
                EXPECT_EQ(x.opStatus == mx::Status::BrokerError, x.opCode == mx::kInvalidTxnState)
                    << mx::name(op) << "/" << mx::name(en) << "/" << mx::name(tm);
                EXPECT_EQ(x.endStatus == mx::Status::BrokerError, x.endCode == mx::kInvalidTxnState)
                    << mx::name(op) << "/" << mx::name(en) << "/" << mx::name(tm);
                ++counts[static_cast<uint8_t>(x.outcome)][static_cast<uint8_t>(x.opStatus)][static_cast<uint8_t>(x.endStatus)];
                if (x.outcome == mx::Outcome::Fenced) (x.opStatus == mx::Status::Fenced ? r9OpFenced : r9OpOk)++;
            }
        }
    }
    auto at = [&](mx::Outcome o, mx::Status op, mx::Status end) {
        return counts[static_cast<uint8_t>(o)][static_cast<uint8_t>(op)][static_cast<uint8_t>(end)];
    };
    using O = mx::Outcome;
    using S = mx::Status;
    EXPECT_EQ(at(O::Delivered, S::Ok, S::Ok), 18) << "R1";
    EXPECT_EQ(at(O::DeliveredOnce, S::Ok, S::Ok), 9) << "R2";
    EXPECT_EQ(at(O::AtMostOnce, S::Ok, S::Ok), 9) << "R3";
    EXPECT_EQ(at(O::Refused, S::BrokerError, S::Ok), 57) << "R4-R6: the op is refused with 19, the commit before it is Ok";
    EXPECT_EQ(at(O::CommittedVisible, S::Ok, S::Ok), 19) << "R7";
    EXPECT_EQ(at(O::FailsTxn, S::BrokerError, S::BrokerError), 19) << "R8: the op and the commit both carry 19";
    // R9: an op that needs the coordinator (the new partition's 9 data ops and the offsets leg) completes Fenced; an
    // added partition's 9 data ops are acknowledged by their leader (Ok); every commit / later op is Fenced.
    EXPECT_EQ(at(O::Fenced, S::Fenced, S::Fenced), 10) << "R9 op Fenced";
    EXPECT_EQ(at(O::Fenced, S::Ok, S::Fenced), 9) << "R9 op Ok";
    EXPECT_EQ(r9OpFenced, 10);
    EXPECT_EQ(r9OpOk, 9);
    EXPECT_EQ(at(O::CommittedVisibleOnce, S::Ok, S::Ok), 19) << "R10";
    EXPECT_EQ(at(O::RedoneOnce, S::Ok, S::Ok), 19) << "R11";
    int sum = 0;
    for (auto& a : counts) for (auto& b : a) for (int c : b) sum += c;
    EXPECT_EQ(sum, 188) << "every applicable cell is in exactly one bucket above";
}

namespace {

int g_ctr = 0;

struct DirSweeper {
    std::vector<std::string> dirs;
    ~DirSweeper() {
        for (const auto& d : dirs) {
            std::error_code ec;
            std::filesystem::remove_all(d, ec);
        }
    }
};
DirSweeper g_sweeper;

struct Cell {
    mx::Op     op;
    mx::Entity ent;
    mx::Timing tm;
    [[nodiscard]] int id() const noexcept {
        return (static_cast<int>(op) * mx::kEntities + static_cast<int>(ent)) * mx::kTimings + static_cast<int>(tm);
    }
};

std::span<const uint8_t> bytesOf(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

int64_t keyForPartition(int32_t partition) {
    for (int64_t u = 1;; ++u) {
        if (rc::Key(u).partition(2) == partition) return u;
    }
}

constexpr int32_t kTopicId = 1;
using PositionTopic        = rc::TypedTopic<rune::gen::Position, rc::ProducerRef>;

// What one op issue produced, as the cell sees it.
struct Issued {
    bool     accepted = false;   // the op was taken (tokens: exactly one completion follows)
    bool     refused  = false;   // the synchronous refusal (NotInTransaction result / std::logic_error)
    bool     threw    = false;
    long     allocs   = 0;       // allocations inside the issue window (only meaningful when !threw)
    bool     measured = false;
    rc::Token token   = 0;       // token forms (cq / AckToken / sendOffsets); 0 = none
    bool     viaCq    = false;   // the completion arrives in the cq (else in the onAck handler)
    int      records  = 0;       // data records accepted
    std::vector<std::string> values;   // the bytes expected in the log (empty for SendOffsets)
};

struct AckRec {
    int64_t  baseOffset;
    rc::OpStatus status;
    uint16_t errorCode;
};

// The rig: one Client (Invoker) + the transaction-aware gateway + the producer under test.
class Rig {
public:
    Cell cell;
    int  cid;
    std::string topicName, txnId, group;
    rc::ClientConfig cfg;
    std::unique_ptr<rc::Client> client;
    std::unique_ptr<Gw>         gw;
    std::unique_ptr<rc::Completions> cq;
    std::optional<rc::Producer>      p;
    rc::TopicHandle                  topic{};
    std::optional<PositionTopic>     typed;
    std::optional<rc::GroupConsumer> gc;

    std::map<rc::Token, std::vector<rc::Completion>> comps;   // cq completions by token
    std::map<rc::Token, std::vector<AckRec>>          acks;    // onAck handler by token
    std::vector<rc::BatchAck>                         batchAcks;
    std::deque<std::string>                           keep;    // payload storage (stable addresses)
    std::vector<int64_t>                              seen;    // group consumer: offsets delivered on partition 0
    int64_t                                           k0, k1;  // keys routing to partition 0 / 1
    int                                               acceptedRecords = 0;   // every data record any op accepted
    std::multiset<std::string>                        expectVisible;         // partition 0, read_committed at the end

    explicit Rig(Cell c) : cell(c), cid(c.id()), k0(keyForPartition(0)), k1(keyForPartition(1)) {
        const std::string n = std::to_string(cid);
        topicName           = "mx-topic-" + n;
        txnId               = "mx-txn-" + n;
        group               = "mx-group-" + n;
        cfg.gatewayChannel  = "aeron:ipc";
        cfg.gatewayStreamId = 6000 + 2 * cid;
        cfg.egressChannel   = "aeron:ipc";
        cfg.egressStreamId  = 6001 + 2 * cid;
        cfg.dataDir         = std::string(std::filesystem::temp_directory_path().string() + "/f_txnmx_") + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
        g_sweeper.dirs.push_back(cfg.dataDir);
        cfg.runnerMode = rc::RunnerMode::Invoker;
        rc::TopicSpec ts;
        ts.name           = topicName;
        ts.topicId        = kTopicId;
        ts.partitionCount = 2;
        cfg.topics.push_back(ts);
        client = std::make_unique<rc::Client>(cfg);
        gw     = std::make_unique<Gw>(cfg.dataDir + "/aeron", cfg.gatewayChannel, cfg.gatewayStreamId,
                                      cfg.egressChannel, cfg.egressStreamId);
        gw->idempotence  = true;   // the broker model: dedup + the log a reader sees
        gw->initPidReply = !mx::isPlain(c.ent);   // the transactional InitProducerId is answered; a plain one gets its own id
        gw->replyTxn     = true;   // AddPartitions / AddOffsets / TxnOffsetCommit / EndTxn are answered
        gw->uniqueSessionIds = true;   // an expired session is replaced by a NEW id, as a real broker does
        gw->newIdOnResume    = true;
        gw->partitions   = {{kTopicId, 0, 0, 1, cfg.gatewayStreamId, ""}, {kTopicId, 1, 0, 1, cfg.gatewayStreamId, ""}};
        cq = std::make_unique<rc::Completions>(*client);
    }
    ~Rig() {
        typed.reset();
        gc.reset();
        p.reset();
        if (client) client->close();
    }

    // ---- driving ---------------------------------------------------------------------------------------------
    void pollCq() {
        cq->poll([&](const rc::Completion& c, std::span<const uint8_t>) { comps[c.token].push_back(c); });
    }
    void step() {
        client->doWork();
        gw->pump();
        pollCq();
    }
    template <class P>
    bool drive(P&& pred, std::chrono::milliseconds limit = std::chrono::seconds(10)) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            step();
            if (pred()) return true;
        }
        return false;
    }
    void settle(int passes = 300) {
        for (int i = 0; i < passes; ++i) step();
    }
    bool await(rc::Token t) { return drive([&] { return comps.count(t) != 0; }); }
    [[nodiscard]] const rc::Completion& done(rc::Token t) const { return comps.at(t).back(); }

    // ---- setup -----------------------------------------------------------------------------------------------
    void connect() {
        ASSERT_TRUE(drive([&] { return gw->ready(); }));
        ASSERT_EQ(client->connect(*cq, 1), rc::OpResult::Accepted);
        ASSERT_TRUE(await(1));
        ASSERT_EQ(done(1).status, rc::OpStatus::Ok);
        ASSERT_TRUE(drive([&] { return gw->metadataRequests > 0; }));
    }
    [[nodiscard]] bool plain() const { return mx::isPlain(cell.ent); }

    rc::ProducerConfig producerCfg() const {
        rc::ProducerConfig c;
        c.lingerUs          = 0;
        c.batchRecords      = 100;
        c.maxInFlight       = 8;
        c.produceTimeoutMs  = 30'000;
        c.deliveryTimeoutMs = 60'000;
        switch (cell.ent) {
        case mx::Entity::PlainNoAck: c.acks = rc::Acks::None; break;
        case mx::Entity::PlainAcked: c.acks = rc::Acks::All; break;
        default:
            c.acks            = rc::Acks::Leader;
            c.transactionalId = txnId;
            break;
        }
        return c;
    }
    static void onAckThunk(void* ctx, rc::Token token, int64_t baseOffset, rc::OpStatus st, uint16_t ec) noexcept {
        static_cast<Rig*>(ctx)->acks[token].push_back({baseOffset, st, ec});
    }
    static void onBatchAckThunk(void* ctx, const rc::BatchAck& a) noexcept {
        static_cast<Rig*>(ctx)->batchAcks.push_back(a);
    }
    void makeProducer() {
        p.emplace(client->producer(producerCfg()));
        p->onAck(&Rig::onAckThunk, this);
        p->onBatchAck(&Rig::onBatchAckThunk, this);
        topic = client->topic(topicName);
        typed.emplace(rc::ProducerRef(*p), topic, uint16_t{1});
        batchAcks.reserve(256);
    }

    // ---- the transaction lifecycle ---------------------------------------------------------------------------
    void init(rc::Token tok = 100) {
        ASSERT_EQ(p->initTransactions(*cq, tok), rc::OpResult::Accepted);
        ASSERT_TRUE(await(tok));
        ASSERT_EQ(done(tok).status, rc::OpStatus::Ok) << "errorCode " << done(tok).errorCode;
        ASSERT_TRUE(drive([&] { return p->state() == rc::TxnState::Ready; }));
    }
    void begin() {
        p->beginTransaction();
        ASSERT_TRUE(drive([&] { return p->state() == rc::TxnState::InTransaction; }));
    }
    void commitOk(rc::Token tok) {
        ASSERT_EQ(p->commitTransaction(*cq, tok), rc::OpResult::Accepted);
        ASSERT_TRUE(await(tok));
        ASSERT_EQ(done(tok).status, rc::OpStatus::Ok) << "errorCode " << done(tok).errorCode;
        ASSERT_TRUE(drive([&] { return p->state() == rc::TxnState::Ready; }));
    }
    void abortOk(rc::Token tok) {
        ASSERT_EQ(p->abortTransaction(*cq, tok), rc::OpResult::Accepted);
        ASSERT_TRUE(await(tok));
        ASSERT_EQ(done(tok).status, rc::OpStatus::Ok) << "errorCode " << done(tok).errorCode;
        ASSERT_TRUE(drive([&] { return p->state() == rc::TxnState::Ready; }));
    }

    // ---- records ---------------------------------------------------------------------------------------------
    // One void record on `partition` (key chosen for it); values are tracked by the caller.
    void plainRecord(int32_t partition, const std::string& v) {
        keep.push_back(v);
        const rc::Key key(partition == 0 ? k0 : k1);
        ASSERT_EQ(p->trySend(topic, key, bytesOf(keep.back())), rc::SendResult::Ok);
        ++acceptedRecords;
    }
    // A record that adds partition 0 to the transaction; waits until it is acknowledged.
    void priorRecord(const std::string& v) {
        const int frames = gw->produceFrames;
        ASSERT_NO_FATAL_FAILURE(plainRecord(0, v));
        ASSERT_TRUE(drive([&] { return gw->produceFrames > frames && batchAcksCoverAccepted(); }));
    }
    [[nodiscard]] int ackedRecords() const {
        int n = 0;
        for (const auto& a : batchAcks) n += a.recordCount;
        return n;
    }
    [[nodiscard]] bool batchAcksCoverAccepted() const { return ackedRecords() >= acceptedRecords; }

    // The bytes a Position op writes.
    static std::string positionBytes(int64_t userId, int32_t zone) {
        rune::gen::Position rec;
        rec.userId = userId;
        rec.zone   = zone;
        std::string out(rec.encodedSize(), '\0');
        rec.encode(reinterpret_cast<uint8_t*>(out.data()));
        return out;
    }

    // Issue `op` once.  `tag` makes its values unique; `measure` wraps the issue call in an allocation window.
    // The values are built BEFORE the window.  The refusal forms: a void send / Tier-2 send throws
    // std::logic_error, trySend / tryClaim return SendResult::NotInTransaction.
    Issued issue(mx::Op op, int tag, bool measure, rc::Token token) {
        Issued is;
        const std::string t = std::to_string(tag);
        const rc::Key     key(k0);
        std::vector<const std::string*> v;
        auto mk = [&](const std::string& s) -> const std::string& { keep.push_back(s); return keep.back(); };
        switch (op) {
        case mx::Op::Send: case mx::Op::SendTo: case mx::Op::TrySendCq: case mx::Op::TrySendAck:
            v.push_back(&mk(std::string(mx::name(op)) + "-" + t));
            break;
        case mx::Op::Batch:
            for (int i = 0; i < 3; ++i) v.push_back(&mk("batch-" + t + "-" + std::to_string(i)));
            break;
        case mx::Op::TryClaim:
            v.push_back(&mk("claim-" + t));
            break;
        case mx::Op::ClaimBatch:
            for (int i = 0; i < 3; ++i) {
                char buf[16];
                std::snprintf(buf, sizeof buf, "cb%03d-%d.", tag % 1000, i);
                v.push_back(&mk(std::string(buf, 8)));
            }
            break;
        case mx::Op::TypedSend: case mx::Op::TypedClaim:
            v.push_back(&mk(positionBytes(k0, tag)));
            break;
        case mx::Op::SendOffsets:
            break;
        }
        for (const auto* s : v) is.values.push_back(*s);
        is.records = static_cast<int>(v.size());
        rune::gen::Position rec;
        rec.userId = k0;
        rec.zone   = tag;
        const rc::OffsetEntry offs{kTopicId, 0, offsetValue};
        const std::string     grp = group;

        auto run = [&](auto&& body) {
            if (measure) alloc_counter::startThisThread();
            body();
            if (measure) {
                is.allocs   = alloc_counter::stopThisThread();
                is.measured = true;
            }
        };
        auto fromSend = [&](rc::SendResult r) {
            if (r == rc::SendResult::Ok) is.accepted = true;
            else if (r == rc::SendResult::NotInTransaction) is.refused = true;
            else ADD_FAILURE() << "unexpected SendResult " << static_cast<int>(r);
        };
        switch (op) {
        case mx::Op::Send:
            run([&] {
                try { p->send(topic, key, bytesOf(*v[0])); is.accepted = true; }
                catch (const std::logic_error&) { is.refused = is.threw = true; }
            });
            break;
        case mx::Op::SendTo:
            run([&] {
                try { p->sendTo(topic, 0, key, bytesOf(*v[0])); is.accepted = true; }
                catch (const std::logic_error&) { is.refused = is.threw = true; }
            });
            break;
        case mx::Op::TrySendCq:
            is.token = token;
            is.viaCq = true;
            run([&] { fromSend(p->trySend(topic, key, bytesOf(*v[0]), *cq, token)); });
            break;
        case mx::Op::TrySendAck:
            is.token = token;
            run([&] { fromSend(p->trySend(topic, key, bytesOf(*v[0]), rc::AckToken{token})); });
            break;
        case mx::Op::Batch:
            run([&] {
                try {
                    rc::BatchBuilder bb = p->batch(topic, key);
                    bb.add(bytesOf(*v[0])).add(bytesOf(*v[1])).add(bytesOf(*v[2]));
                    const rc::OpResult r = bb.send();
                    if (r == rc::OpResult::Accepted) is.accepted = true;
                    else ADD_FAILURE() << "batch send " << static_cast<int>(r);
                } catch (const std::logic_error&) { is.refused = is.threw = true; }
            });
            break;
        case mx::Op::TryClaim:
            run([&] {
                rc::Claim c = p->tryClaim(topic, key, static_cast<uint32_t>(v[0]->size()));
                if (c) {
                    std::memcpy(c.buffer().data(), v[0]->data(), v[0]->size());
                    is.accepted = c.commit();
                } else {
                    fromSend(c.result());
                }
            });
            break;
        case mx::Op::ClaimBatch:
            run([&] {
                rc::BatchClaim bc = p->claimBatch(topic, key, 3, 8);
                if (bc) {
                    for (uint32_t i = 0; i < 3; ++i) std::memcpy(bc.record(i).data(), v[i]->data(), 8);
                    is.accepted = bc.commit();
                } else {
                    fromSend(bc.result());
                }
            });
            break;
        case mx::Op::TypedSend:
            run([&] {
                try { typed->send(rec); is.accepted = true; }
                catch (const std::logic_error&) { is.refused = is.threw = true; }
            });
            break;
        case mx::Op::TypedClaim:
            run([&] {
                auto w = typed->claim(k0);
                if (w) {
                    w.zone(tag);
                    is.accepted = w.commit();
                } else {
                    fromSend(w.result());
                }
            });
            break;
        case mx::Op::SendOffsets:
            is.token = token;
            is.viaCq = true;
            run([&] {
                const rc::OpResult r = p->sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&offs, 1), grp, *cq, token);
                if (r == rc::OpResult::Accepted) is.accepted = true;
                else ADD_FAILURE() << "sendOffsets " << static_cast<int>(r);
            });
            break;
        }
        // #369: an issue that ends neither accepted nor refused (a claim whose commit() returned false, ...) used to pass
        // every cell that did not ASSERT_TRUE(is.accepted).
        if (!is.accepted && !is.refused) {
            ADD_FAILURE() << "op " << mx::name(op) << " ended neither accepted nor refused";
        }
        if (is.accepted) acceptedRecords += is.records;
        return is;
    }
    int64_t offsetValue = 5;   // what SendOffsets sends

    // ---- reads (the fake's log) ------------------------------------------------------------------------------
    [[nodiscard]] std::vector<std::string> committed(int32_t partition) const {
        std::vector<std::string> out;
        for (const auto& v : gw->committedValues(partition)) out.emplace_back(v.begin(), v.end());
        std::sort(out.begin(), out.end());
        return out;
    }
    // A read_uncommitted reader: every batch, open or aborted included.
    [[nodiscard]] std::vector<std::string> uncommitted(int32_t partition) const {
        std::vector<std::string> out;
        for (const auto& b : gw->log) {
            if (b.partition != partition) continue;
            for (const auto& v : b.values) out.emplace_back(v.begin(), v.end());
        }
        std::sort(out.begin(), out.end());
        return out;
    }
    [[nodiscard]] static std::vector<std::string> sorted(const std::multiset<std::string>& m) {
        return {m.begin(), m.end()};
    }

    // Counters a refused op must leave untouched.
    struct Wire {
        int produce, addPartitions, addOffsets, txnOffsets, endTxn, inits;
        std::size_t frames, logBatches;
        bool operator==(const Wire&) const = default;
    };
    [[nodiscard]] Wire wire() const {
        return {gw->produceFrames, gw->addPartitions, gw->addOffsets, gw->txnOffsetCommits, gw->endTxns, gw->initPids,
                gw->txnFrames.size(), gw->log.size()};
    }

    // ---- checks that hold in every cell ----------------------------------------------------------------------
    // 3: no transactional batch before its partition's AddPartitions OK (within the transaction); 4: attributes.
    void checkFrameOrderAndAttributes() {
        std::set<int32_t> added;
        for (const auto& f : gw->txnFrames) {
            switch (f.kind) {
            case Gw::TxnFrameKind::AddPartitionsOk:
                if (f.code == 0) added.insert(f.partition);
                break;
            case Gw::TxnFrameKind::EndTxn:
                if (f.code == 0) added.clear();   // a decided transaction forgets its partitions
                break;
            case Gw::TxnFrameKind::Produce:
                if (plain()) {
                    EXPECT_FALSE(f.transactional) << "a plain producer's batch carries the TRANSACTIONAL attribute";
                } else {
                    EXPECT_TRUE(f.transactional) << "a transactional producer's batch lacks the TRANSACTIONAL attribute";
                    EXPECT_EQ(added.count(f.partition), 1u) << "partition " << f.partition << " produced before its AddPartitions OK";
                }
                break;
            default: break;
            }
        }
        for (const auto& b : gw->log) {
            if (plain()) EXPECT_EQ(b.fate, Gw::TxnFate::NotTxn);
            else EXPECT_NE(b.fate, Gw::TxnFate::NotTxn);
        }
    }
    // 2 (exactly once): a token completes once; every accepted record is in exactly one BatchAck, never more.
    void checkExactlyOnce(const Issued& is, bool allRecordsAckedExactly) {
        if (is.accepted && is.token != 0) {
            const std::size_t n = is.viaCq ? (comps.count(is.token) ? comps[is.token].size() : 0)
                                           : (acks.count(is.token) ? acks[is.token].size() : 0);
            EXPECT_EQ(n, 1u) << "token " << is.token << " completed " << n << " times";
        }
        if (is.refused && is.token != 0) {
            EXPECT_EQ(comps.count(is.token), 0u) << "a refused op completed";
            EXPECT_EQ(acks.count(is.token), 0u) << "a refused op completed";
        }
        if (allRecordsAckedExactly) {
            EXPECT_EQ(ackedRecords(), acceptedRecords) << "every accepted record is reported in exactly one BatchAck";
        } else {
            EXPECT_LE(ackedRecords(), acceptedRecords) << "a record was reported twice";
        }
    }
    void checkAllocs(const Issued& is) {
        if (is.measured && !is.threw) EXPECT_EQ(is.allocs, 0) << "the op allocated " << is.allocs << " times";
    }
};

// Drives one cell.  Every status / error code a handler asserts is read from `want` = mx::expected() (#369): the
// op's own completion (opStatus / opCode) and the commit's / later ops' (endStatus / endCode).
class CellRunner {
public:
    explicit CellRunner(Cell c) : r(c), c_(c), want(mx::expected(c.op, c.ent, c.tm)), outcome(want.outcome) {}
    void run();

private:
    Rig         r;
    Cell        c_;
    mx::Expected want;
    mx::Outcome outcome;

    [[nodiscard]] static rc::OpStatus statusFor(mx::Status s) {
        switch (s) {
        case mx::Status::Ok:          return rc::OpStatus::Ok;
        case mx::Status::BrokerError: return rc::OpStatus::BrokerError;
        case mx::Status::Fenced:      return rc::OpStatus::Fenced;
        }
        return rc::OpStatus::Ok;
    }
    [[nodiscard]] rc::OpStatus opStatus() const { return statusFor(want.opStatus); }
    [[nodiscard]] rc::OpStatus endStatus() const { return statusFor(want.endStatus); }

    void plainSteady();
    void plainSessionLost();
    void txnSteady();
    void txnRefused();
    void txnAbortable();
    void txnFenced();
    void txnSessionLost();
    void txnRewind();

    [[nodiscard]] bool isOffsets() const { return c_.op == mx::Op::SendOffsets; }
    [[nodiscard]] bool added() const { return c_.ent == mx::Entity::TxnAddedPartition; }
    // A whole warm-up transaction with the same op (committed): warms every first-use path of the cell.
    void warmup();
    // begin; (Added: a record that adds partition 0).
    void beginTxn(bool willCommit);
    void setupGroup();
    void groupReady();
    [[nodiscard]] static rc::OpStatus statusOf(Rig& r, const Issued& is) {
        return is.viaCq ? r.comps[is.token].back().status : r.acks[is.token].back().status;
    }
    [[nodiscard]] static uint16_t codeOf(Rig& r, const Issued& is) {
        return is.viaCq ? r.comps[is.token].back().errorCode : r.acks[is.token].back().errorCode;
    }
    [[nodiscard]] bool tokenDone(const Issued& is) {
        return is.token == 0 || (is.viaCq ? r.comps.count(is.token) != 0 : r.acks.count(is.token) != 0);
    }
    void checkVisible(bool uncommittedToo) {
        EXPECT_EQ(r.committed(0), Rig::sorted(r.expectVisible)) << "read_committed on partition 0";
        if (uncommittedToo) EXPECT_EQ(r.uncommitted(0), Rig::sorted(r.expectVisible)) << "read_uncommitted on partition 0";
    }
};

constexpr rc::Token kTokInit = 100, kTokWarm = 101, kTokWarmCommit = 102, kTokOp = 110, kTokOffs = 111, kTokCommit = 120,
                    kTokAbort = 121, kTokSeek = 130, kTokWarmOffs = 112;

void CellRunner::warmup() {
    // begin; [prior]; op(warm); commit - the values stay committed and expected.  Its offsets differ from the
    // cell's own, so a later "committed offset" check tells the two transactions apart.
    const int64_t keepOffset = r.offsetValue;
    r.offsetValue            = 3;
    struct Restore { Rig& r; int64_t v; ~Restore() { r.offsetValue = v; } } restore{r, keepOffset};
    r.begin();
    if (::testing::Test::HasFatalFailure()) return;
    if (added()) {
        r.priorRecord("warm-prior");
        if (::testing::Test::HasFatalFailure()) return;
        r.expectVisible.insert("warm-prior");
    }
    const Issued w = r.issue(c_.op, 900, /*measure=*/false, kTokWarm);
    ASSERT_TRUE(w.accepted) << "the warm-up op was refused";
    for (const auto& v : w.values) r.expectVisible.insert(v);
    if (isOffsets()) {
        ASSERT_TRUE(r.await(kTokWarm));
        ASSERT_EQ(r.done(kTokWarm).status, rc::OpStatus::Ok);
    }
    r.settle();
    r.commitOk(kTokWarmCommit);
}

void CellRunner::beginTxn(bool willCommit) {
    r.begin();
    if (::testing::Test::HasFatalFailure()) return;
    if (added()) {
        r.priorRecord("pre");
        if (willCommit) r.expectVisible.insert("pre");
    }
}

void CellRunner::setupGroup() {
    Gw& gw             = *r.gw;
    gw.groupProtocol   = true;
    gw.holdJoins       = true;   // released once the consumer asked to join
    gw.replyPongs      = true;
    gw.groupAssignment = {{kTopicId, 0}};
    gw.committedOffsets[{kTopicId, 0}] = 0;
    rc::GroupConfig gcfg;
    gcfg.autoCommit       = false;
    gcfg.sessionTimeoutMs = 30000;
    r.gc.emplace(r.client->subscribeGroup(r.group, {r.topicName}, gcfg));
    r.gc->onRecords([&r = r](const rc::RecordSet& rs) {
        if (rs.partition != 0) return;
        rs.forEachRecord([&](auto, auto, int64_t offset, int64_t) { r.seen.push_back(offset); });
    });
}

void CellRunner::groupReady() {
    ASSERT_TRUE(r.drive([&] { return !r.gw->pendingJoins.empty(); }));
    r.gw->releaseJoins();
    ASSERT_TRUE(r.drive([&] { return r.gw->latestSubscription(kTopicId, 0) != nullptr; })) << "no subscription";
}

void CellRunner::run() {
    if (c_.tm == mx::Timing::AfterAbortRewind) setupGroup();
    ASSERT_NO_FATAL_FAILURE(r.connect());
    r.gw->framesReceived = 0;
    ASSERT_NO_FATAL_FAILURE(r.makeProducer());
    if (c_.tm == mx::Timing::AfterAbortRewind) ASSERT_NO_FATAL_FAILURE(groupReady());
    if (r.plain()) {
        if (c_.tm == mx::Timing::Steady) plainSteady();
        else plainSessionLost();
    } else {
        switch (c_.tm) {
        case mx::Timing::Steady: txnSteady(); break;
        case mx::Timing::BeforeInit:
        case mx::Timing::OutsideTxn:
        case mx::Timing::AfterCommitIssued: txnRefused(); break;
        case mx::Timing::AbortableError: txnAbortable(); break;
        case mx::Timing::Fenced: txnFenced(); break;
        case mx::Timing::SessionLostMid: txnSessionLost(); break;
        case mx::Timing::AfterAbortRewind: txnRewind(); break;
        }
    }
    if (::testing::Test::HasFatalFailure()) return;
    r.checkFrameOrderAndAttributes();
}


// ---------------------------------------------------------------------------------------------------------------
// R1: plain x Steady - every record is delivered once; PlainAcked batches carry the broker-assigned id,
// PlainNoAck -1/-1/-1.
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::plainSteady() {
    ASSERT_EQ(outcome, mx::Outcome::Delivered);
    if (c_.ent == mx::Entity::PlainAcked) {
        ASSERT_TRUE(r.drive([&] { return r.p->producerId() >= 0; })) << "no broker-assigned id";
    }
    // A warm-up pass of the same cell, then the measured op.
    const Issued w = r.issue(c_.op, 900, false, kTokWarm);
    ASSERT_TRUE(w.accepted);
    for (const auto& v : w.values) r.expectVisible.insert(v);
    r.settle();
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    ASSERT_TRUE(is.accepted) << "refused: a plain producer has no gate";
    for (const auto& v : is.values) r.expectVisible.insert(v);
    ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted() && tokenDone(is); }));
    r.settle();
    r.checkAllocs(is);
    r.checkExactlyOnce(is, true);
    if (is.token != 0) {
        EXPECT_EQ(statusOf(r, is), opStatus());
        if (c_.ent == mx::Entity::PlainNoAck) EXPECT_EQ(is.viaCq ? r.comps[is.token].back().value : r.acks[is.token].back().baseOffset, -1);
    }
    checkVisible(true);   // a read_committed and a read_uncommitted reader each see every record once
    for (const auto& b : r.gw->log) {
        if (c_.ent == mx::Entity::PlainAcked) {
            EXPECT_GE(b.producerId, 7000) << "the id on the wire is the broker's answer (FD4)";
            EXPECT_LT(b.producerId, 7100);
            EXPECT_EQ(b.producerEpoch, 0);
        } else {
            EXPECT_EQ(b.producerId, -1);
            EXPECT_EQ(b.producerEpoch, -1);
            EXPECT_EQ(b.baseSequence, -1);
        }
    }
    if (c_.ent == mx::Entity::PlainAcked) {
        EXPECT_EQ(r.gw->plainInitPids, 1);
    }
}

// R2 / R3: plain x SessionLostMid.
void CellRunner::plainSessionLost() {
    const bool acked = c_.ent == mx::Entity::PlainAcked;
    ASSERT_EQ(outcome, acked ? mx::Outcome::DeliveredOnce : mx::Outcome::AtMostOnce);
    if (acked) ASSERT_TRUE(r.drive([&] { return r.p->producerId() >= 0; }));
    const Issued w = r.issue(c_.op, 900, false, kTokWarm);
    ASSERT_TRUE(w.accepted);
    for (const auto& v : w.values) r.expectVisible.insert(v);
    ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted(); }));
    const int frames = r.gw->produceFrames;
    if (acked) r.gw->dropAnswer.insert(frames + 1);   // the batch is appended, its answer is lost
    const int connects = r.gw->connects;
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    ASSERT_TRUE(is.accepted);
    for (const auto& v : is.values) r.expectVisible.insert(v);
    ASSERT_TRUE(r.drive([&] { return r.gw->produceFrames > frames; })) << "the batch never reached the fake";
    r.gw->expireSession(r.gw->lastUniqueSessionId());
    ASSERT_TRUE(r.drive([&] { return r.gw->connects > connects; })) << "the client did not reconnect";
    if (acked) {
        ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted() && tokenDone(is); })) << "the batch was never acknowledged";
        EXPECT_GE(r.gw->produceFrames, frames + 2) << "the batch was not re-sent on the new session";
        EXPECT_GE(r.gw->windowDupAnswers, 1) << "the re-sent batch was not answered as a duplicate";
    }
    r.settle();
    // statusOf() reads the token's completion: wait for it (an absent one would insert an empty vector and .back()
    // would be undefined behaviour - a crash instead of a clean failure).
    ASSERT_TRUE(r.drive([&] { return tokenDone(is); })) << "the op's token never completed";
    r.checkAllocs(is);
    r.checkExactlyOnce(is, acked);
    if (is.token != 0) {
        EXPECT_EQ(statusOf(r, is), opStatus());
    }
    if (acked) {
        checkVisible(true);   // exactly one copy of every record, the re-sent batch deduplicated
    } else {
        // zero or one copy of the op's records, never two; the warm-up's are all there.
        const auto got = r.committed(0);
        std::multiset<std::string> have(got.begin(), got.end());
        for (const auto& v : w.values) EXPECT_EQ(have.count(v), 1u);
        for (const auto& v : is.values) EXPECT_LE(have.count(v), 1u) << "at-most-once broke: a copy was re-sent";
    }
}

// ---------------------------------------------------------------------------------------------------------------
// R7: txn x Steady
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::txnSteady() {
    ASSERT_EQ(outcome, mx::Outcome::CommittedVisible);
    ASSERT_NO_FATAL_FAILURE(r.init());
    ASSERT_NO_FATAL_FAILURE(warmup());
    ASSERT_NO_FATAL_FAILURE(beginTxn(true));
    const bool gateCheck = !added() && !isOffsets();   // a new partition: hold its AddPartitions answer
    if (gateCheck) r.gw->holdAddPartitions = true;
    const std::size_t mark = r.gw->txnFrames.size();
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    ASSERT_TRUE(is.accepted);
    for (const auto& v : is.values) r.expectVisible.insert(v);
    if (gateCheck) {
        // The partition's records must not leave before its AddPartitions OK: hold the answer, let the duty cycle
        // run, and look at the wire.
        ASSERT_TRUE(r.drive([&] { return r.gw->heldAddPartitions_.size() == 1; })) << "no AddPartitionsToTxn for the new partition";
        r.settle(500);
        for (std::size_t i = mark; i < r.gw->txnFrames.size(); ++i) {
            EXPECT_NE(r.gw->txnFrames[i].kind, Gw::TxnFrameKind::Produce) << "a Produce left before the AddPartitions OK";
        }
        r.gw->releaseAddPartitions();
    }
    ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted() && tokenDone(is); }));
    r.settle();
    if (isOffsets()) {
        EXPECT_EQ(statusOf(r, is), opStatus());
        EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 3) << "the offsets are staged until the commit";
    }
    r.commitOk(kTokCommit);
    r.checkAllocs(is);
    r.checkExactlyOnce(is, true);
    if (is.token != 0) EXPECT_EQ(statusOf(r, is), opStatus());
    EXPECT_EQ(r.comps[kTokCommit].size(), 1u);
    checkVisible(true);
    EXPECT_EQ(r.gw->visibleBatches(0, Gw::TxnFate::Open), 0u) << "nothing left undecided";
    EXPECT_EQ(r.gw->visibleBatches(0, Gw::TxnFate::Aborted), 0u);
    int reqs = 0, offs = 0, tocs = 0;
    for (std::size_t i = mark; i < r.gw->txnFrames.size(); ++i) {
        const auto& f = r.gw->txnFrames[i];
        reqs += f.kind == Gw::TxnFrameKind::AddPartitionsReq && f.partition == 0;
        offs += f.kind == Gw::TxnFrameKind::AddOffsets;
        tocs += f.kind == Gw::TxnFrameKind::TxnOffsetCommit;
    }
    if (isOffsets()) {
        EXPECT_EQ(reqs, 0);
        EXPECT_EQ(offs, 1);
        EXPECT_EQ(tocs, 1);
        EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 5) << "the group's committed offset equals the sent one after the commit";
    } else {
        EXPECT_EQ(reqs, added() ? 0 : 1) << (added() ? "an added partition needs no AddPartitions" : "exactly one AddPartitions for a new partition");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// R4 / R5 / R6: txn x BeforeInit / OutsideTxn / AfterCommitIssued - refused, with no side effect.
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::txnRefused() {
    ASSERT_EQ(outcome, mx::Outcome::Refused);
    bool commitIssued = false;
    switch (c_.tm) {
    case mx::Timing::BeforeInit:
        break;
    case mx::Timing::OutsideTxn:
        ASSERT_NO_FATAL_FAILURE(r.init());
        if (added()) ASSERT_NO_FATAL_FAILURE(warmup());   // outside a transaction: after a completed commit
        break;
    default:   // AfterCommitIssued: begin; one record; commit issued (not completed)
        ASSERT_NO_FATAL_FAILURE(r.init());
        ASSERT_NO_FATAL_FAILURE(warmup());
        ASSERT_NO_FATAL_FAILURE(r.begin());
        if (added()) {
            ASSERT_NO_FATAL_FAILURE(r.priorRecord("r1"));
            r.expectVisible.insert("r1");
        } else {
            // A new partition for the op: the one record goes to the other partition.
            ASSERT_NO_FATAL_FAILURE(r.plainRecord(1, "r1"));
            ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted(); }));
        }
        ASSERT_EQ(r.p->commitTransaction(*r.cq, kTokCommit), rc::OpResult::Accepted);
        commitIssued = true;
        break;
    }
    const auto before = r.wire();
    const uint32_t reserved = r.cq->reservedBytes();
    const std::size_t mark  = r.gw->txnFrames.size();
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    r.checkAllocs(is);
    if (isOffsets()) {
        // An offsets leg is a token op: accepted, then refused by the machine with InvalidTxnState.
        ASSERT_TRUE(is.accepted);
        ASSERT_TRUE(r.await(kTokOp));
        EXPECT_EQ(r.done(kTokOp).status, opStatus());
        EXPECT_EQ(r.done(kTokOp).errorCode, want.opCode);
    } else {
        EXPECT_TRUE(is.refused) << "the op was not refused";
        EXPECT_FALSE(is.accepted);
        EXPECT_EQ(r.cq->reservedBytes(), reserved) << "a refused op kept credit";
        // Void forms throw, the rest return NotInTransaction: nothing reaches the gateway.
        const bool voidForm = c_.op == mx::Op::Send || c_.op == mx::Op::SendTo || c_.op == mx::Op::Batch || c_.op == mx::Op::TypedSend;
        EXPECT_EQ(is.threw, voidForm);
    }
    if (commitIssued) {
        ASSERT_TRUE(r.await(kTokCommit));
        EXPECT_EQ(r.done(kTokCommit).status, endStatus()) << "errorCode " << r.done(kTokCommit).errorCode;
        EXPECT_EQ(r.done(kTokCommit).errorCode, want.endCode);
        EXPECT_EQ(r.comps[kTokCommit].size(), 1u);
        ASSERT_TRUE(r.drive([&] { return r.p->state() == rc::TxnState::Ready; }));
    }
    r.settle();
    const auto after = r.wire();
    EXPECT_EQ(after.produce, before.produce) << "a refused op produced";
    EXPECT_EQ(after.addPartitions, before.addPartitions);
    EXPECT_EQ(after.addOffsets, before.addOffsets);
    EXPECT_EQ(after.txnOffsets, before.txnOffsets);
    EXPECT_EQ(after.logBatches, before.logBatches);
    EXPECT_EQ(after.inits, before.inits);
    for (std::size_t i = mark; i < r.gw->txnFrames.size(); ++i) {
        const auto k = r.gw->txnFrames[i].kind;
        EXPECT_TRUE(k == Gw::TxnFrameKind::EndTxn) << "frame kind " << static_cast<int>(k) << " left for a refused op";
    }
    r.checkExactlyOnce(is, false);
    if (c_.tm == mx::Timing::BeforeInit) EXPECT_EQ(r.p->state(), rc::TxnState::Uninitialized);
    EXPECT_EQ(r.committed(0), Rig::sorted(r.expectVisible)) << "only what was committed before the op is visible";
    if (c_.tm == mx::Timing::AfterCommitIssued && !added()) EXPECT_EQ(r.committed(1), (std::vector<std::string>{"r1"}));
    if (c_.tm == mx::Timing::BeforeInit) EXPECT_TRUE(r.gw->log.empty());
}

// ---------------------------------------------------------------------------------------------------------------
// R8: txn x AbortableError
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::txnAbortable() {
    ASSERT_EQ(outcome, mx::Outcome::FailsTxn);
    ASSERT_NO_FATAL_FAILURE(r.init());
    ASSERT_NO_FATAL_FAILURE(warmup());
    ASSERT_NO_FATAL_FAILURE(beginTxn(false));
    // The fake answers the next AddPartitions InvalidTxnState.  A new partition: that is the op's own
    // AddPartitions.  An added partition needs none, so a probe record to the other partition draws it and the
    // op then meets an already poisoned transaction.
    r.gw->txnCoordCodes.push_back(19);
    if (added()) {
        ASSERT_NO_FATAL_FAILURE(r.plainRecord(1, "probe"));
        ASSERT_TRUE(r.drive([&] { return r.p->state() == rc::TxnState::AbortableError; })) << "the transaction was not poisoned";
        r.settle();
    }
    const auto before = r.wire();
    const std::size_t mark = r.gw->txnFrames.size();
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    r.checkAllocs(is);
    if (!added()) {
        ASSERT_TRUE(is.accepted) << "the gate is open until the machine learns of the failure";
        ASSERT_TRUE(r.drive([&] { return r.p->state() == rc::TxnState::AbortableError; })) << "the transaction was not poisoned";
    }
    if (isOffsets()) EXPECT_TRUE(is.accepted);
    r.settle();
    const rc::Token ct = kTokCommit;
    ASSERT_EQ(r.p->commitTransaction(*r.cq, ct), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(ct));
    EXPECT_EQ(r.done(ct).status, endStatus());
    EXPECT_EQ(r.done(ct).errorCode, want.endCode);
    EXPECT_EQ(r.comps[ct].size(), 1u);
    ASSERT_NO_FATAL_FAILURE(r.abortOk(kTokAbort));
    EXPECT_EQ(r.p->state(), rc::TxnState::Ready);
    // A record held for its failed AddPartitions completes with the transaction's failure as soon as the producer learns
    // of it (#368: Kafka's sender fails undrained batches with the last error at once) - long before the abort, and not
    // at deliveryTimeoutMs; the abort only ends the transaction.  Pinned by
    // TxnMatrixRegression.UnsentRecordsOfAPoisonedTxnCompleteAtOnceRegression.
    if (is.accepted && is.token != 0) {
        ASSERT_TRUE(r.drive([&] { return tokenDone(is); })) << "the op's token never completed";
        EXPECT_EQ(statusOf(r, is), opStatus());
        EXPECT_EQ(codeOf(r, is), want.opCode);
    }
    r.settle();
    const auto after = r.wire();
    EXPECT_EQ(after.produce, before.produce) << "the failed transaction's op reached the log";
    for (std::size_t i = mark; i < r.gw->txnFrames.size(); ++i) {
        const auto k = r.gw->txnFrames[i].kind;
        const bool ownAddPartitions = !added() && (k == Gw::TxnFrameKind::AddPartitionsReq || k == Gw::TxnFrameKind::AddPartitionsOk);
        EXPECT_TRUE(k == Gw::TxnFrameKind::EndTxn || ownAddPartitions) << "frame kind " << static_cast<int>(k) << " left after the failure";
    }
    r.checkExactlyOnce(is, false);
    EXPECT_EQ(r.committed(0), Rig::sorted(r.expectVisible)) << "only the warm-up transaction is visible";
    EXPECT_TRUE(r.committed(1).empty());
    EXPECT_EQ(r.gw->visibleBatches(0, Gw::TxnFate::Open) + r.gw->visibleBatches(1, Gw::TxnFate::Open), 0u);
    for (std::size_t i = mark; i < r.gw->txnFrames.size(); ++i) {
        const auto& f = r.gw->txnFrames[i];
        if (f.kind == Gw::TxnFrameKind::EndTxn) EXPECT_EQ(f.partition, 0) << "the failed transaction was committed";
    }
    if (isOffsets()) EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 3) << "only the warm-up's offsets are committed";
}

// ---------------------------------------------------------------------------------------------------------------
// R9: txn x Fenced
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::txnFenced() {
    ASSERT_EQ(outcome, mx::Outcome::Fenced);
    ASSERT_NO_FATAL_FAILURE(r.init());
    ASSERT_NO_FATAL_FAILURE(warmup());
    ASSERT_NO_FATAL_FAILURE(beginTxn(false));
    // A second init of the same transactional id bumps the epoch: this producer's next coordinator request is 18.
    r.gw->fenceTransactionalId(r.txnId, r.gw->initPid);
    const int endTxns0 = r.gw->endTxns;
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    r.checkAllocs(is);
    ASSERT_TRUE(is.accepted) << "the handle's gate is open until the machine learns of the fence";
    ASSERT_EQ(r.p->commitTransaction(*r.cq, kTokCommit), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(kTokCommit));
    EXPECT_EQ(r.done(kTokCommit).status, endStatus());
    EXPECT_EQ(r.comps[kTokCommit].size(), 1u);
    ASSERT_TRUE(r.drive([&] { return tokenDone(is); }));
    r.settle();
    // R9, early: for a new partition's data op and for an offsets leg the fence is learned from the AddPartitions /
    // AddOffsets 18, BEFORE the end phase - the machine is already FatalError when the commit would send its EndTxn, so
    // none leaves (a producer that still sent it would have the coordinator just answer 18 again, which the
    // completions alone cannot tell).  An added partition's batch goes straight to its leader: the coordinator
    // reveals the fence only at the EndTxn, so exactly that one frame is sent.
    if (isOffsets() || !added()) {
        EXPECT_EQ(r.gw->endTxns, endTxns0) << "an EndTxn left a producer that already knew it was fenced";
    } else {
        EXPECT_EQ(r.gw->endTxns, endTxns0 + 1) << "the commit's EndTxn is the frame that reveals the fence";
    }
    if (is.token != 0) {
        const rc::OpStatus st = statusOf(r, is);
        if (isOffsets() || !added()) {
            EXPECT_EQ(st, opStatus()) << "an op that needs the coordinator completes " << mx::name(want.opStatus);
        } else {
            // An added partition's batch goes straight to the partition leader; it may be acknowledged before the
            // coordinator reveals the fence at the commit (EndTxn follows the data): the token is Ok.  Its records
            // are never visible (checked below).
            EXPECT_EQ(st, opStatus()) << "the leader acknowledged the batch before the coordinator revealed the fence";
        }
    }
    EXPECT_EQ(r.p->state(), rc::TxnState::FatalError);
    EXPECT_EQ(r.p->producerId(), -1) << "producerId() is -1 after a fence";
    r.checkExactlyOnce(is, false);
    EXPECT_EQ(r.committed(0), Rig::sorted(r.expectVisible)) << "nothing of the fenced transaction is visible";
    EXPECT_TRUE(r.committed(1).empty());
    if (isOffsets()) EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 3) << "the offsets of a fenced producer never commit";
    // No further frame leaves a fenced producer; every later op completes Fenced.
    const auto wire0 = r.wire();
    ASSERT_EQ(r.p->commitTransaction(*r.cq, kTokAbort), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(kTokAbort));
    EXPECT_EQ(r.done(kTokAbort).status, endStatus());
    r.settle(600);
    EXPECT_EQ(r.wire(), wire0) << "a frame left a fenced producer";
}

// ---------------------------------------------------------------------------------------------------------------
// R10: txn x SessionLostMid
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::txnSessionLost() {
    ASSERT_EQ(outcome, mx::Outcome::CommittedVisibleOnce);
    ASSERT_NO_FATAL_FAILURE(r.init());
    ASSERT_NO_FATAL_FAILURE(warmup());
    ASSERT_NO_FATAL_FAILURE(beginTxn(true));
    const int frames   = r.gw->produceFrames;
    const int connects = r.gw->connects;
    if (isOffsets()) {
        r.gw->holdTxnReplies = true;   // the leg is processed, its answer never leaves
    } else {
        r.gw->dropAnswer.insert(frames + 1);   // the batch is appended, its answer is lost
    }
    const Issued is = r.issue(c_.op, 1, true, kTokOp);
    ASSERT_TRUE(is.accepted);
    for (const auto& v : is.values) r.expectVisible.insert(v);
    if (isOffsets()) {
        ASSERT_TRUE(r.drive([&] { return r.gw->txnRequestsUnanswered >= 1; })) << "the offsets leg never reached the fake";
    } else {
        ASSERT_TRUE(r.drive([&] { return r.gw->produceFrames > frames; })) << "the batch never reached the fake";
    }
    r.gw->expireSession(r.gw->lastUniqueSessionId());
    r.gw->holdTxnReplies = false;
    ASSERT_TRUE(r.drive([&] { return r.gw->connects > connects; })) << "the client did not reconnect";
    ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted() && tokenDone(is); })) << "the op was never completed (addOffsets " << r.gw->addOffsets << ", txnOffsetCommits " << r.gw->txnOffsetCommits
        << ", unanswered " << r.gw->txnRequestsUnanswered << ", connects " << r.gw->connects << ", state " << static_cast<int>(r.p->state()) << ")";
    if (!isOffsets()) EXPECT_GE(r.gw->produceFrames, frames + 2) << "the batch was not re-sent on the new session";
    r.settle();
    ASSERT_EQ(r.p->commitTransaction(*r.cq, kTokCommit), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(kTokCommit));
    rc::Token last = kTokCommit;
    if (r.done(kTokCommit).status == rc::OpStatus::Timeout) {   // in doubt: a retried commit settles it
        ASSERT_TRUE(r.drive([&] { return r.p->state() == rc::TxnState::Ready || r.p->state() == rc::TxnState::InTransaction; }));
        ASSERT_EQ(r.p->commitTransaction(*r.cq, kTokAbort), rc::OpResult::Accepted);
        ASSERT_TRUE(r.await(kTokAbort));
        last = kTokAbort;
    }
    EXPECT_EQ(r.done(last).status, endStatus()) << "errorCode " << r.done(last).errorCode;
    ASSERT_TRUE(r.drive([&] { return r.p->state() == rc::TxnState::Ready; }));
    r.checkAllocs(is);
    r.checkExactlyOnce(is, true);
    if (is.token != 0) EXPECT_EQ(statusOf(r, is), opStatus());
    checkVisible(true);   // every record exactly once: the re-sent batch was deduplicated by (pid, epoch, sequence)
    EXPECT_EQ(r.gw->visibleBatches(0, Gw::TxnFate::Open), 0u);
    if (isOffsets()) EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 5);
}

// ---------------------------------------------------------------------------------------------------------------
// R11: txn x AfterAbortRewind
// ---------------------------------------------------------------------------------------------------------------
void CellRunner::txnRewind() {
    ASSERT_EQ(outcome, mx::Outcome::RedoneOnce);
    ASSERT_NO_FATAL_FAILURE(r.init());
    r.offsetValue = 5;   // the consumer read offsets 0..4: the position to commit is 5
    // The group consumer reads the input.
    const auto sub0 = *r.gw->latestSubscription(kTopicId, 0);
    r.gw->pushRecords(sub0, 0, 5);
    ASSERT_TRUE(r.drive([&] { return r.seen.size() >= 5; }));

    // ---- attempt 1: the op and the consumed position, then an abort ----
    ASSERT_NO_FATAL_FAILURE(beginTxn(false));
    const Issued first = r.issue(c_.op, 11, false, kTokWarm);
    ASSERT_TRUE(first.accepted);
    if (!isOffsets()) {
        const rc::OffsetEntry e{kTopicId, 0, 5};
        ASSERT_EQ(r.p->sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), r.group, *r.cq, kTokWarmOffs), rc::OpResult::Accepted);
        ASSERT_TRUE(r.await(kTokWarmOffs));
        EXPECT_EQ(r.done(kTokWarmOffs).status, rc::OpStatus::Ok);
    }
    ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted() && tokenDone(first); }));
    if (first.token != 0) {
        ASSERT_TRUE(r.await(kTokWarm) || !first.viaCq);
        EXPECT_EQ(statusOf(r, first), opStatus());
    }
    ASSERT_NO_FATAL_FAILURE(r.abortOk(kTokAbort));
    r.settle();
    r.checkExactlyOnce(first, false);   // the first attempt's token completed exactly once (it was never checked)
    EXPECT_TRUE(r.committed(0).empty()) << "the aborted attempt is visible";
    EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 0) << "an aborted attempt's offsets must not commit";
    const std::size_t abortedBatches = r.gw->visibleBatches(0, Gw::TxnFate::Aborted);
    if (!isOffsets()) EXPECT_GE(abortedBatches, 1u);

    // ---- rewind the consumer to the committed offset ----
    ASSERT_EQ(r.gc->seekToCommitted(rc::CompletionTarget(*r.cq), kTokSeek), rc::OpResult::Accepted);
    ASSERT_TRUE(r.await(kTokSeek));
    EXPECT_EQ(r.done(kTokSeek).status, rc::OpStatus::Ok);
    const auto* sub1 = r.gw->latestSubscription(kTopicId, 0);
    ASSERT_NE(sub1, nullptr);
    EXPECT_NE(sub1->corrId, sub0.corrId);
    EXPECT_EQ(sub1->fromOffset, 0);
    const auto resub = *sub1;
    r.gw->pushRecords(resub, 0, 5);
    ASSERT_TRUE(r.drive([&] { return r.seen.size() >= 10; }));
    ASSERT_EQ(r.seen.size(), 10u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(r.seen[static_cast<std::size_t>(5 + i)], i) << "redelivery gap or duplicate";

    // ---- attempt 2: the same op again, committed ----
    ASSERT_NO_FATAL_FAILURE(beginTxn(true));
    const Issued redo = r.issue(c_.op, 22, true, kTokOp);
    ASSERT_TRUE(redo.accepted);
    for (const auto& v : redo.values) r.expectVisible.insert(v);
    if (!isOffsets()) {
        const rc::OffsetEntry e{kTopicId, 0, 5};
        ASSERT_EQ(r.p->sendOffsetsToTransaction(std::span<const rc::OffsetEntry>(&e, 1), r.group, *r.cq, kTokOffs), rc::OpResult::Accepted);
        ASSERT_TRUE(r.await(kTokOffs));
        EXPECT_EQ(r.done(kTokOffs).status, rc::OpStatus::Ok);
    }
    ASSERT_TRUE(r.drive([&] { return r.batchAcksCoverAccepted() && tokenDone(redo); }));
    ASSERT_NO_FATAL_FAILURE(r.commitOk(kTokCommit));
    r.settle();
    r.checkAllocs(redo);
    r.checkExactlyOnce(redo, true);
    if (redo.token != 0) EXPECT_EQ(statusOf(r, redo), opStatus());
    EXPECT_EQ(r.committed(0), Rig::sorted(r.expectVisible)) << "the redo is committed and visible exactly once";
    EXPECT_EQ(r.gw->visibleBatches(0, Gw::TxnFate::Aborted), abortedBatches) << "the aborted attempt stays aborted";
    EXPECT_EQ((r.gw->committedOffsets[{kTopicId, 0}]), 5) << "the group's committed offset advanced exactly by the redo";
}

// ---------------------------------------------------------------------------------------------------------------
// The parameterized matrix: all 320 cells; N/A ones skip naming their rule.
// ---------------------------------------------------------------------------------------------------------------
class TxnMatrixTest : public ::testing::TestWithParam<Cell> {};

TEST_P(TxnMatrixTest, Cell) {
    const Cell c = GetParam();
    if (const auto rule = mx::naRule(c.op, c.ent, c.tm)) {
        GTEST_SKIP() << "NA: " << mx::name(*rule);
    }
    CellRunner runner(c);
    ASSERT_NO_FATAL_FAILURE(runner.run());
}

std::vector<Cell> allCells() {
    std::vector<Cell> v;
    for (uint8_t o = 0; o < mx::kOps; ++o) {
        for (uint8_t e = 0; e < mx::kEntities; ++e) {
            for (uint8_t t = 0; t < mx::kTimings; ++t) {
                v.push_back({static_cast<mx::Op>(o), static_cast<mx::Entity>(e), static_cast<mx::Timing>(t)});
            }
        }
    }
    return v;
}

std::string cellName(const ::testing::TestParamInfo<Cell>& i) {
    return std::string(mx::name(i.param.op)) + "_" + mx::name(i.param.ent) + "_" + mx::name(i.param.tm);
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(TxnMatrix, TxnMatrixTest, ::testing::ValuesIn(allCells()), cellName);

// #368: the record whose AddPartitions failed completes with the poison status within a few passes of the failure -
// not at the abort and not at deliveryTimeoutMs (60 s here; the loop is bounded by passes, so a token that waits for
// either never completes inside it).
TEST(TxnMatrixRegression, UnsentRecordsOfAPoisonedTxnCompleteAtOnceRegression) {
    Rig r(Cell{mx::Op::TrySendAck, mx::Entity::TxnNewPartition, mx::Timing::AbortableError});
    ASSERT_NO_FATAL_FAILURE(r.connect());
    r.gw->framesReceived = 0;
    ASSERT_NO_FATAL_FAILURE(r.makeProducer());
    ASSERT_NO_FATAL_FAILURE(r.init());
    ASSERT_NO_FATAL_FAILURE(r.begin());
    r.gw->txnCoordCodes.push_back(19);   // the next AddPartitions is answered InvalidTxnState
    const Issued is = r.issue(mx::Op::TrySendAck, 1, false, kTokOp);
    ASSERT_TRUE(is.accepted);
    ASSERT_TRUE(r.drive([&] { return r.p->state() == rc::TxnState::AbortableError; })) << "the transaction was not poisoned";
    bool done = false;
    for (int pass = 0; pass < 3000 && !done; ++pass) {
        r.step();
        done = r.acks.count(kTokOp) != 0;
    }
    ASSERT_TRUE(done) << "the unsent record's token did not complete while the transaction waited for its abort";
    ASSERT_EQ(r.acks[kTokOp].size(), 1u);
    const mx::Expected ex = mx::expected(mx::Op::TrySendAck, mx::Entity::TxnNewPartition, mx::Timing::AbortableError);
    ASSERT_EQ(ex.opStatus, mx::Status::BrokerError);
    EXPECT_EQ(r.acks[kTokOp][0].status, rc::OpStatus::BrokerError);
    EXPECT_EQ(r.acks[kTokOp][0].errorCode, ex.opCode);
    EXPECT_EQ(r.p->state(), rc::TxnState::AbortableError) << "no abort was issued: the transaction still waits for one";
    ASSERT_NO_FATAL_FAILURE(r.abortOk(kTokAbort));
    r.settle();
    EXPECT_EQ(r.acks[kTokOp].size(), 1u) << "completed once, not again at the abort";
    EXPECT_EQ(r.gw->visibleBatches(0, Gw::TxnFate::Open) + r.gw->visibleBatches(1, Gw::TxnFate::Open), 0u);
}
