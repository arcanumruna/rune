// Phase 2b Part B Task 8b: the zero-copy claim API against ONE real authenticating BrokerNode (AuthBroker: auth on,
// real Aeron UDP, the real produce path - WRITE-bitmap check, schema validator, log append).  Tasks 7/8 prove claims
// against the in-process InvokerGateway test double; these prove them against the real broker, in both runner modes.
//
// "Consume back" reads the partition logs on each partition's owner thread (BrokerNode::onOwnerThreadForTest): the
// exact stored key, value, offset and RecordBatch boundary of every record - stronger than a subscription handler
// (which would also need cross-thread plumbing a test may not use).  Every wait is a deadline loop; nothing sleeps.
#include "gen/position.gen.h"
#include "support/AuthBroker.h"
#include "support/AuthRig.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace rc = rune::client;
using namespace std::chrono_literals;
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using rune::gen::Position;

namespace {

constexpr const char* kPw = "claim-user-password-1";
constexpr int32_t kParts  = 4;

/// One stored record, as the broker's partition log holds it.
struct Rec {
    int32_t     partition  = 0;
    int64_t     offset     = 0;
    int64_t     batchBase  = 0;   ///< the RecordBatch's base offset
    int32_t     batchCount = 0;   ///< the RecordBatch's record count
    std::string key, value;
};

/// Every record of the partition, in offset order (read on the partition's owner thread).
std::vector<Rec> readPartition(BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onOwnerThreadForTest(topicId, partition, [partition](rune::replication::PartitionReplica& r) {
        std::vector<Rec> out;
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
                    const int64_t base = reader.baseOffset();
                    const int32_t cnt  = reader.recordCount();
                    reader.forEachRecord([&](std::string_view k, std::string_view v, int64_t off, int64_t) {
                        if (off < next) return;
                        out.push_back(Rec{partition, off, base, cnt, std::string(k), std::string(v)});
                        next = off + 1;
                    });
                }
                if (tl == 0) break;
                pos += tl;
            }
            if (next == before) break;
        }
        return out;
    });
}

std::vector<Rec> readTopic(BrokerNode& n, int32_t topicId) {
    std::vector<Rec> all;
    for (int32_t p = 0; p < kParts; ++p) {
        auto v = readPartition(n, topicId, p);
        all.insert(all.end(), v.begin(), v.end());
    }
    return all;
}

std::string keyBytes(const rc::Key& k) {
    return std::string(reinterpret_cast<const char*>(k.bytes().data()), k.size());
}

/// The typed schema of `claim.pos` - the same descriptor the client's claim-schema codegen compiles Position from.
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

/// One authenticating broker with a "writer" (CREATE/DESCRIBE/WRITE/READ on claim.*) and a "viewer" (no WRITE).
struct ClaimRig {
    AuthBroker b;
    explicit ClaimRig(const std::string& tag) : b(tag) {
        b.bootstrapAdmin();
        b.createUser("writer", kPw,
                     {grants::prefix(SO::Create, ST::Topic, "claim."), grants::prefix(SO::Describe, ST::Topic, "claim."),
                      grants::prefix(SO::Write, ST::Topic, "claim."), grants::prefix(SO::Read, ST::Topic, "claim.")});
        b.createUser("viewer", kPw,
                     {grants::prefix(SO::Describe, ST::Topic, "claim."), grants::prefix(SO::Read, ST::Topic, "claim.")});
    }

    std::unique_ptr<rc::Client> connect(const std::string& user, rc::RunnerMode mode,
                                        std::vector<rc::TopicSpec> topics = {}) {
        rc::ClientConfig c;
        c.gatewayChannel  = b.channel();
        c.gatewayStreamId = 1;
        c.egressChannel   = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
        c.egressStreamId  = 2;
        c.dataDir         = b.clientDir().string();
        c.username        = user;
        c.password        = kPw;
        c.runnerMode      = mode;
        c.topics          = std::move(topics);
        auto cl = std::make_unique<rc::Client>(c);
        cl->connectAndWait(30s);
        return cl;
    }

    /// Define a topic as "writer" (Thread-mode client, closed again); returns its id.
    int32_t define(const std::string& name, const std::string& keyField, std::vector<uint8_t> blob, bool validate) {
        auto cl = connect("writer", rc::RunnerMode::Thread);
        const auto r = cl->defineTopicAndWait(name, kParts, keyField, blob, validate, rc::Persistence::Disk, {}, {}, 30s);
        EXPECT_EQ(r.errorCode, 0) << r.detail;
        cl->close();
        return r.topicId;
    }
};

/// Pump the client (Invoker owner) and test `pred` until it holds or the deadline passes.
template <class Pred>
bool pumpUntil(rc::Client& c, bool invoker, Pred&& pred, std::chrono::seconds budget = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline) {
        if (invoker) (void)c.doWork();
        if (pred()) return true;
        std::this_thread::yield();
    }
    return false;
}

const char* modeName(rc::RunnerMode m) { return m == rc::RunnerMode::Invoker ? "Invoker" : "Thread"; }

rc::ProducerConfig producerCfg() {
    rc::ProducerConfig p;
    p.acks     = rc::Acks::Leader;
    p.lingerUs = 0;
    return p;
}

std::string pattern(char tag, uint64_t k, std::size_t len) {
    std::string s(len, tag);
    for (std::size_t i = 0; i < len; ++i) s[i] = static_cast<char>(tag + static_cast<char>((k * 7 + i) % 23));
    return s;
}

}  // namespace

TEST(ClaimBrokerRegression, TryClaimRoundTripsThroughTheBrokerInBothRunnerModesRegression) {
    ClaimRig rig("claim_rt");
    int modeIdx = 0;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Thread, rc::RunnerMode::Invoker}) {
        SCOPED_TRACE(modeName(mode));
        const bool invoker = mode == rc::RunnerMode::Invoker;
        const std::string name = "claim.rt" + std::to_string(modeIdx++);
        const int32_t tid = rig.define(name, "", {}, false);
        ASSERT_GE(tid, 0);
        auto cl = rig.connect("writer", mode, {rc::TopicSpec{name, tid, kParts}});
        auto p = cl->producer(producerCfg());
        const rc::TopicHandle topic = cl->topic(name);
        ASSERT_EQ(topic.topicId, tid);
        rc::Completions cq(*cl);

        constexpr uint64_t kKeys = 8;
        constexpr std::size_t kLen = 37;
        for (uint64_t k = 0; k < kKeys; ++k) {
            const rc::Key key(k + 1000);
            {
                rc::Claim c = p.tryClaim(topic, key, kLen, cq, static_cast<rc::Token>(2 * k + 1));
                ASSERT_TRUE(c) << static_cast<int>(c.result());
                ASSERT_EQ(c.buffer().size(), kLen);
                const std::string v = pattern('A', k, kLen);
                std::memcpy(c.buffer().data(), v.data(), kLen);
                ASSERT_TRUE(c.commit());
            }
            const std::string sv = pattern('a', k, kLen);
            ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size()),
                                cq, static_cast<rc::Token>(2 * k + 2)),
                      rc::SendResult::Ok);
        }
        p.flush();
        std::map<rc::Token, rc::Completion> done;
        ASSERT_TRUE(pumpUntil(*cl, invoker, [&] {
            (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d; });
            return done.size() == 2 * kKeys;
        })) << "acked " << done.size() << " of " << 2 * kKeys;
        for (const auto& [tok, d] : done) {
            EXPECT_EQ(d.status, rc::OpStatus::Ok) << "token " << tok;
            EXPECT_EQ(d.errorCode, 0) << "token " << tok;
            EXPECT_GE(d.value, 0) << "an acked record has its offset";
        }

        const std::vector<Rec> recs = readTopic(rig.b.node(), tid);
        ASSERT_EQ(recs.size(), 2 * kKeys);
        for (uint64_t k = 0; k < kKeys; ++k) {
            const rc::Key key(k + 1000);
            std::vector<const Rec*> mine;
            for (const Rec& r : recs)
                if (r.key == keyBytes(key)) mine.push_back(&r);
            ASSERT_EQ(mine.size(), 2u) << "key " << k;
            EXPECT_EQ(mine[0]->partition, key.partition(kParts)) << "the claim routes by the key's hash";
            EXPECT_EQ(mine[1]->partition, mine[0]->partition) << "the same key's send(span) lands on the claim's partition";
            EXPECT_EQ(mine[0]->value, pattern('A', k, kLen)) << "the claimed bytes arrive exactly";
            EXPECT_EQ(mine[1]->value, pattern('a', k, kLen));
            EXPECT_LT(mine[0]->offset, mine[1]->offset) << "per-key order: the claim was committed first";
        }
        p.close();
        cl->close();
    }
}

TEST(ClaimBrokerRegression, ClaimBatchIsOneAtomicBatchOnTheBrokerRegression) {
    ClaimRig rig("claim_batch");
    int modeIdx = 0;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Thread, rc::RunnerMode::Invoker}) {
        SCOPED_TRACE(modeName(mode));
        const bool invoker = mode == rc::RunnerMode::Invoker;
        const std::string name = "claim.batch" + std::to_string(modeIdx++);
        const int32_t tid = rig.define(name, "", {}, false);
        ASSERT_GE(tid, 0);
        auto cl = rig.connect("writer", mode, {rc::TopicSpec{name, tid, kParts}});
        auto p = cl->producer(producerCfg());
        rc::Completions bq(*cl);   // per-batch acks: the credit model returns n units per BatchAck of n
        p.batchAcksTo(bq);
        const rc::TopicHandle topic = cl->topic(name);
        const rc::Key key(uint64_t{4242});
        const int32_t part = key.partition(kParts);
        constexpr uint32_t kN = 8, kRec = 21;

        auto sendStr = [&](const std::string& s) {
            ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(s.data()), s.size())),
                      rc::SendResult::Ok);
        };
        int32_t acked = 0;
        std::vector<rc::BatchAck> acks;
        auto awaitAcks = [&](int32_t total) {
            return pumpUntil(*cl, invoker, [&] {
                (void)bq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                    const rc::BatchAck a = rc::batchAckOf(c);
                    acks.push_back(a);
                    acked += a.recordCount;
                });
                return acked >= total;
            });
        };
        auto claimEight = [&](char tag) {
            rc::BatchClaim b = p.claimBatch(topic, key, kN, kRec);
            ASSERT_TRUE(b) << static_cast<int>(b.result());
            ASSERT_EQ(b.count(), kN);
            for (uint32_t i = 0; i < kN; ++i) {
                ASSERT_EQ(b.record(i).size(), kRec);
                const std::string v = pattern(tag, i, kRec);
                std::memcpy(b.record(i).data(), v.data(), kRec);
            }
            ASSERT_TRUE(b.commit());
        };

        // flush() is a request; the Invoker owner's own duty cycle seals the batch, so it pumps once before the next send.
        auto fence = [&] {
            p.flush();
            if (invoker) (void)cl->doWork();
        };

        // Case 1 (fenced): flush() fences keep the neighbours out of the claimed batch - the broker sees pre (1 record),
        // the claimed batch (exactly 8, one BatchAck of 8) and post (1 record), at offsets 0..9 with no gaps.
        // A fence is deterministic only once the sealed batch is acked: in Thread mode flush() is a request the
        // runner may not have served yet, so each fence waits for its batch's ack before the next send.
        sendStr("pre-record");
        fence();
        ASSERT_TRUE(awaitAcks(1)) << "acked records " << acked;
        claimEight('B');
        fence();
        ASSERT_TRUE(awaitAcks(9)) << "acked records " << acked;
        sendStr("post-record");
        fence();
        ASSERT_TRUE(awaitAcks(10)) << "acked records " << acked;
        EXPECT_EQ(acked, 10);
        EXPECT_EQ(std::count_if(acks.begin(), acks.end(), [](const rc::BatchAck& a) { return a.recordCount == 8; }), 1)
            << "exactly one BatchAck of 8 for the claimed batch";
        for (const rc::BatchAck& a : acks) EXPECT_EQ(a.errorCode, 0);
        {
            const std::vector<Rec> recs = readPartition(rig.b.node(), tid, part);
            ASSERT_EQ(recs.size(), kN + 2u);
            for (std::size_t i = 0; i < recs.size(); ++i) EXPECT_EQ(recs[i].offset, static_cast<int64_t>(i)) << "no gaps";
            EXPECT_EQ(recs.front().value, "pre-record");
            EXPECT_EQ(recs.back().value, "post-record");
            for (uint32_t i = 0; i < kN; ++i) {
                const Rec& r = recs[1 + i];
                EXPECT_EQ(r.value, pattern('B', i, kRec)) << "record " << i;
                EXPECT_EQ(r.key, keyBytes(key));
                EXPECT_EQ(r.batchBase, 1) << "record " << i << " is in the RecordBatch that starts after the pre-record";
                EXPECT_EQ(r.batchCount, static_cast<int32_t>(kN)) << "that RecordBatch holds exactly the 8 claimed records";
            }
            EXPECT_NE(recs.front().batchBase, 1);
            EXPECT_NE(recs.back().batchBase, 1);
        }

        // Case 2 (unfenced): a claim may share its RecordBatch with the neighbours the producer was about to send
        // anyway (it joins an open batch only when it fits whole).  Its 8 records are still contiguous, in ONE
        // RecordBatch, with no foreign record between them, and the acks account for every record.
        sendStr("pre2");
        claimEight('C');
        sendStr("post2");
        p.flush();
        ASSERT_TRUE(awaitAcks(20)) << "acked records " << acked;
        EXPECT_EQ(acked, 20);
        {
            const std::vector<Rec> recs = readPartition(rig.b.node(), tid, part);
            ASSERT_EQ(recs.size(), 2u * (kN + 2u));
            for (std::size_t i = 0; i < recs.size(); ++i) EXPECT_EQ(recs[i].offset, static_cast<int64_t>(i)) << "no gaps";
            EXPECT_EQ(recs[10].value, "pre2");
            EXPECT_EQ(recs[19].value, "post2");
            for (uint32_t i = 0; i < kN; ++i) {
                const Rec& r = recs[11 + i];
                EXPECT_EQ(r.value, pattern('C', i, kRec)) << "record " << i << " contiguous after pre2";
                EXPECT_EQ(r.batchBase, recs[11].batchBase) << "record " << i << " is in the claim's one RecordBatch";
                EXPECT_GE(r.batchCount, static_cast<int32_t>(kN));
            }
        }
        p.close();
        cl->close();
    }
}

TEST(ClaimBrokerRegression, TypedClaimMatchesTypedSendOnTheBrokerRegression) {
    ClaimRig rig("claim_typed");
    const rune::codec::Descriptor desc = positionDescriptor();
    ASSERT_EQ(rune::codec::fingerprint(desc), Position::kFingerprint) << "the test's descriptor is the generated schema";
    int modeIdx = 0;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Thread, rc::RunnerMode::Invoker}) {
        SCOPED_TRACE(modeName(mode));
        const bool invoker = mode == rc::RunnerMode::Invoker;
        const std::string name = "claim.pos" + std::to_string(modeIdx++);
        // validate=true: the broker's validator checks every record against the descriptor on the produce path.
        const int32_t tid = rig.define(name, "userId", rune::codec::serialize(desc), true);
        ASSERT_GE(tid, 0);
        auto cl = rig.connect("writer", mode);
        rc::TypedTopic<Position> topic = cl->openTopicAndWait<Position>(name, 30s);
        ASSERT_EQ(topic.handle().topicId, tid);
        ASSERT_EQ(topic.handle().partitionCount, kParts);

        const std::vector<int64_t> users = {0, 1, 2, 7, 42, 1000, 123456789, -1, -77, int64_t{1} << 40};
        for (const int64_t u : users) {
            Position rec;
            rec.userId = u;
            rec.zone   = 3;
            rec.x      = 1.5;
            rec.y      = -2.25;
            rec.z      = 1e9;
            rec.flags  = 0xBEEF;
            rec.seq    = 4'000'000'000u;
            rec.tick   = 0x0102030405060708ull;
            rec.alive  = true;
            rec.kind   = 2;
            std::memcpy(rec.tag.data(), "north", 5);
            topic.send(rec);
            auto w = topic.claim(u);
            ASSERT_TRUE(w) << static_cast<int>(w.result());
            w.zone(3).x(1.5).y(-2.25).z(1e9).flags(0xBEEF).seq(4'000'000'000u).tick(0x0102030405060708ull).alive(true).kind(2).tag("north");
            ASSERT_TRUE(w.commit());
        }
        std::vector<Rec> recs;
        ASSERT_TRUE(pumpUntil(*cl, invoker, [&] {
            recs = readTopic(rig.b.node(), tid);
            return recs.size() >= 2 * users.size();
        })) << "stored " << recs.size() << " of " << 2 * users.size() << " (a validator refusal drops the record)";
        ASSERT_EQ(recs.size(), 2 * users.size());
        for (const int64_t u : users) {
            const rc::Key key(u);
            std::vector<const Rec*> mine;
            for (const Rec& r : recs)
                if (r.key == keyBytes(key)) mine.push_back(&r);
            ASSERT_EQ(mine.size(), 2u) << "userId " << u;
            EXPECT_EQ(mine[0]->partition, mine[1]->partition) << "claim(" << u << ") lands where send(rec) does";
            EXPECT_EQ(mine[0]->partition, key.partition(kParts));
            EXPECT_EQ(mine[0]->value, mine[1]->value) << "claim and send store the same bytes, schema-version stamp included";
            ASSERT_EQ(mine[0]->value.size(), Position::Encoder::kEncodedLength);
            EXPECT_EQ(static_cast<uint8_t>(mine[0]->value[2]), topic.boundVersion()) << "the bound version is stamped";
            EXPECT_LT(mine[0]->offset, mine[1]->offset) << "per-key order: send, then claim";
        }
        cl->close();
    }
}

TEST(ClaimBrokerRegression, ClaimToAnUnwritableTopicCompletesNotAuthorizedRegression) {
    ClaimRig rig("claim_noauth");
    int modeIdx = 0;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Thread, rc::RunnerMode::Invoker}) {
        SCOPED_TRACE(modeName(mode));
        const bool invoker = mode == rc::RunnerMode::Invoker;
        const std::string name = "claim.ro" + std::to_string(modeIdx++);
        const int32_t tid = rig.define(name, "", {}, false);
        ASSERT_GE(tid, 0);
        auto cl = rig.connect("viewer", mode, {rc::TopicSpec{name, tid, kParts}});   // READ + DESCRIBE, no WRITE
        auto p = cl->producer(producerCfg());
        const rc::TopicHandle topic = cl->topic(name);
        rc::Completions cq(*cl);
        const rc::Key key(uint64_t{9});
        constexpr rc::Token kClaimTok = 1, kSendTok = 2;
        {
            rc::Claim c = p.tryClaim(topic, key, 16, cq, kClaimTok);
            ASSERT_TRUE(c) << "the client cannot know the grant: the claim is granted, the broker refuses the produce";
            std::memset(c.buffer().data(), 0x5A, c.buffer().size());
            ASSERT_TRUE(c.commit());
        }
        const std::string sv(16, '\x5A');
        ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size()), cq,
                            kSendTok),
                  rc::SendResult::Ok);
        p.flush();
        std::map<rc::Token, rc::Completion> done;
        ASSERT_TRUE(pumpUntil(*cl, invoker, [&] {
            (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d; });
            return done.size() == 2;
        })) << "completions " << done.size();
        for (const rc::Token t : {kClaimTok, kSendTok}) {
            EXPECT_EQ(done[t].status, rc::OpStatus::BrokerError) << "token " << t;
            EXPECT_EQ(done[t].errorCode, kNotAuthorized) << "token " << t << ": a claim completes exactly like a send";
        }
        EXPECT_TRUE(readTopic(rig.b.node(), tid).empty()) << "nothing was appended";
        p.close();
        cl->close();
    }
}

TEST(ClaimBrokerRegression, AbortedClaimLeavesNothingOnTheBrokerRegression) {
    ClaimRig rig("claim_abort");
    int modeIdx = 0;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Thread, rc::RunnerMode::Invoker}) {
        SCOPED_TRACE(modeName(mode));
        const bool invoker = mode == rc::RunnerMode::Invoker;
        const std::string name = "claim.abort" + std::to_string(modeIdx++);
        const int32_t tid = rig.define(name, "", {}, false);
        ASSERT_GE(tid, 0);
        auto cl = rig.connect("writer", mode, {rc::TopicSpec{name, tid, kParts}});
        auto p = cl->producer(producerCfg());
        rc::Completions bq(*cl);
        p.batchAcksTo(bq);   // every claim reserves credit; aborted ones must return it
        const rc::TopicHandle topic = cl->topic(name);
        const rc::Key key(uint64_t{77});
        const int32_t part = key.partition(kParts);
        {
            rc::Claim c = p.tryClaim(topic, key, 32);
            ASSERT_TRUE(c);
            std::memset(c.buffer().data(), 0xEE, c.buffer().size());
            c.abort();
            EXPECT_FALSE(c.commit()) << "an aborted claim cannot be committed";
        }
        {
            rc::Claim c = p.tryClaim(topic, key, 32);   // destroyed without commit
            ASSERT_TRUE(c);
            std::memset(c.buffer().data(), 0xEF, c.buffer().size());
        }
        {
            rc::BatchClaim b = p.claimBatch(topic, key, 4, 16);   // aborted batch
            ASSERT_TRUE(b);
            b.abort();
        }
        {
            rc::BatchClaim b = p.claimBatch(topic, key, 4, 16);   // destroyed batch
            ASSERT_TRUE(b);
            std::memset(b.record(0).data(), 0xEF, b.record(0).size());
        }
        const std::string sv = "the-only-record";
        ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size())),
                  rc::SendResult::Ok)
            << "aborted claims left the producer able to send";
        p.flush();
        int32_t acked = 0;
        ASSERT_TRUE(pumpUntil(*cl, invoker, [&] {
            (void)bq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                const rc::BatchAck a = rc::batchAckOf(c);
                EXPECT_EQ(a.errorCode, 0);
                acked += a.recordCount;
            });
            return acked >= 1;
        }));
        EXPECT_EQ(acked, 1) << "only the send is acked: aborted claims complete nothing";

        const std::vector<Rec> all = readTopic(rig.b.node(), tid);
        ASSERT_EQ(all.size(), 1u) << "the consumer sees only the send";
        EXPECT_EQ(all[0].partition, part);
        EXPECT_EQ(all[0].offset, 0) << "no offset was burned by an aborted claim";
        EXPECT_EQ(all[0].value, sv);
        // The producer's credit survived the aborts: a second send is accepted and lands at offset 1.
        ASSERT_EQ(p.trySend(topic, key, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size())),
                  rc::SendResult::Ok);
        p.flush();
        ASSERT_TRUE(pumpUntil(*cl, invoker, [&] {
            (void)bq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { acked += rc::batchAckOf(c).recordCount; });
            return acked >= 2;
        }));
        const std::vector<Rec> two = readPartition(rig.b.node(), tid, part);
        ASSERT_EQ(two.size(), 2u);
        EXPECT_EQ(two[1].offset, 1) << "contiguous offsets, no gap";
        p.close();
        cl->close();
    }
}

// #418, through the real client and broker: a typed reader generated before a mid-block DROP never misreads a record
// written after it.  Without the topic's lineage the newer record is reported unknown (Decoder::known() false,
// decode() false); with Client::lineageAndWait<Position> it reads every field right, the dropped one absent.
TEST(ClaimBrokerRegression, ATypedReaderPlacesRecordsWrittenAfterAMidBlockDropRegression) {
    ClaimRig rig("claim_lineage");
    rig.b.createUser("evolver", kPw,
                     {grants::prefix(SO::Create, ST::Topic, "claim."), grants::prefix(SO::Describe, ST::Topic, "claim."),
                      grants::prefix(SO::Write, ST::Topic, "claim."), grants::prefix(SO::Read, ST::Topic, "claim."),
                      grants::prefix(SO::Alter, ST::Topic, "claim.")});
    const std::string name = "claim.lin";
    const rune::codec::Descriptor v1 = positionDescriptor();
    const int32_t tid = rig.define(name, "userId", rune::codec::serialize(v1), /*validate=*/true);
    ASSERT_GE(tid, 0);
    auto cl = rig.connect("evolver", rc::RunnerMode::Thread);
    rc::TypedTopic<Position> typed = cl->openTopicAndWait<Position>(name, 30s);
    Position a;
    a.userId = 11; a.zone = 3; a.x = 1.5; a.y = -2.25; a.z = 8; a.flags = 7; a.seq = 9; a.tick = 10; a.alive = true; a.kind = 1;
    std::memcpy(a.tag.data(), "first", 5);
    typed.send(a);

    // v2: DROP zone - every field after it moves 4 bytes down.
    rune::codec::AlterOp drop{}; drop.type = rune::codec::AlterOpType::Drop; drop.name = "zone";
    const auto altered = cl->alterTopicAndWait(name, 0, rune::codec::encodeAlterOps({drop}), {}, 30s);
    ASSERT_EQ(altered.errorCode, 0) << altered.detail;
    ASSERT_EQ(altered.newVersion, 2u);
    uint16_t nextTag = static_cast<uint16_t>(v1.fields.size());
    const auto evolving = rune::codec::applyOps(v1, {drop}, nextTag, 2);
    ASSERT_TRUE(evolving.has_value());
    const rune::codec::Descriptor at2 = rune::codec::materializeAsOf(*evolving, 2);

    // A v2 writer's record (what a client generated at v2 sends), through the validating produce path.
    std::vector<uint8_t> b(4 + at2.blockLength, 0);
    b[0] = static_cast<uint8_t>(at2.blockLength); b[1] = static_cast<uint8_t>(at2.blockLength >> 8); b[2] = 2;
    const auto put = [&](const char* f, const void* v, std::size_t n) { std::memcpy(b.data() + 4 + at2.find(f)->offset, v, n); };
    const int64_t uid = 22; const double x = 4.5, y = 5.5, z = 6.5; const uint16_t flags = 0x1234; const uint32_t seq = 77;
    const uint64_t tick = 88; const uint8_t alive = 1; const int32_t kind = 2;
    put("userId", &uid, 8); put("x", &x, 8); put("y", &y, 8); put("z", &z, 8); put("flags", &flags, 2); put("seq", &seq, 4);
    put("tick", &tick, 8); put("alive", &alive, 1); put("kind", &kind, 4); put("tag", "second\0\0", 8);
    auto p = cl->producer(producerCfg());
    p.send(rc::TopicHandle{tid, kParts}, rc::Key(uid), std::span<const uint8_t>(b));
    p.flush();

    std::vector<Rec> recs;
    ASSERT_TRUE(pumpUntil(*cl, false, [&] { recs = readTopic(rig.b.node(), tid); return recs.size() >= 2; }))
        << "stored " << recs.size() << " of 2 (a validator refusal drops the record)";
    const Rec* r1 = nullptr; const Rec* r2 = nullptr;
    for (const Rec& r : recs) (static_cast<uint8_t>(r.value[2]) == 1 ? r1 : r2) = &r;
    ASSERT_NE(r1, nullptr); ASSERT_NE(r2, nullptr);
    const auto bytes = [](const Rec* r) { return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(r->value.data()), r->value.size()); };

    const Position::Decoder old1(bytes(r1));
    ASSERT_TRUE(old1.known());
    EXPECT_EQ(old1.zone(), 3); EXPECT_EQ(old1.x(), 1.5); EXPECT_EQ(old1.seq(), 9u);
    const Position::Decoder old2(bytes(r2));
    EXPECT_FALSE(old2.known()) << "a v1 type cannot place a v2 record on its own";
    EXPECT_EQ(old2.x(), 0.0) << "never the shifted bytes";
    Position s;
    EXPECT_FALSE(s.decode(bytes(r2)));

    const Position::Lineage lin = cl->lineageAndWait<Position>(name, 30s);
    EXPECT_EQ(lin.maxVersion(), 2u);
    const Position::Decoder now2(bytes(r2), lin);
    ASSERT_TRUE(now2.known());
    EXPECT_EQ(now2.userId(), uid); EXPECT_EQ(now2.key(), uid);
    EXPECT_EQ(now2.zone(), 0) << "dropped at v2: absent";
    EXPECT_EQ(now2.x(), x); EXPECT_EQ(now2.y(), y); EXPECT_EQ(now2.z(), z);
    EXPECT_EQ(now2.flags(), flags); EXPECT_EQ(now2.seq(), seq); EXPECT_EQ(now2.tick(), tick);
    EXPECT_TRUE(now2.alive()); EXPECT_EQ(now2.kind(), kind); EXPECT_EQ(now2.tag().substr(0, 6), "second");
    ASSERT_TRUE(s.decode(bytes(r2), lin));
    EXPECT_EQ(s.zone, 0); EXPECT_EQ(s.x, x); EXPECT_EQ(s.tick, tick);
    ASSERT_TRUE(s.decode(bytes(r1), lin));
    EXPECT_EQ(s.zone, 3) << "the lineage places the v1 record too";
    cl->close();
}

// Adversarial review of PR #434 (#418): the lineage fetch through the real client and broker, attacked.  The topic
// evolves AGAIN after a reader fetched its lineage (a schema change between the fetch and the consume); the lineage
// is asked for a topic that does not exist, for a topic of another schema, and by a principal without DESCRIBE.
TEST(ClaimBrokerRegression, ALineageGoesStaleOnTheNextAlterIsRefetchedAndIsRefusedWhereItMustAdversarialRegression) {
    ClaimRig rig("claim_lineage_adv");
    rig.b.createUser("evolver", kPw,
                     {grants::prefix(SO::Create, ST::Topic, "claim."), grants::prefix(SO::Describe, ST::Topic, "claim."),
                      grants::prefix(SO::Write, ST::Topic, "claim."), grants::prefix(SO::Read, ST::Topic, "claim."),
                      grants::prefix(SO::Alter, ST::Topic, "claim.")});
    rig.b.createUser("outsider", kPw, {grants::prefix(SO::Describe, ST::Topic, "elsewhere.")});
    const std::string name = "claim.lin2";
    const rune::codec::Descriptor v1 = positionDescriptor();
    ASSERT_GE(rig.define(name, "userId", rune::codec::serialize(v1), /*validate=*/true), 0);
    const std::string other = "claim.other";
    ASSERT_GE(rig.define(other, "k",
                         rune::codec::serialize(rune::codec::DescriptorBuilder("other")
                                                    .add("k", rune::codec::PrimType::Int64, 1, true)
                                                    .build()),
                         /*validate=*/true),
              0);
    auto cl = rig.connect("evolver", rc::RunnerMode::Thread);

    const auto alter = [&](rune::codec::AlterOp op, uint16_t expectVersion) {
        const auto r = cl->alterTopicAndWait(name, 0, rune::codec::encodeAlterOps({op}), {}, 30s);
        ASSERT_EQ(r.errorCode, 0) << r.detail;
        ASSERT_EQ(r.newVersion, expectVersion);
    };
    rune::codec::AlterOp drop{}; drop.type = rune::codec::AlterOpType::Drop; drop.name = "zone";
    alter(drop, 2);
    const Position::Lineage lin2 = cl->lineageAndWait<Position>(name, 30s);
    ASSERT_EQ(lin2.maxVersion(), 2u);
    rune::codec::AlterOp drop2{}; drop2.type = rune::codec::AlterOpType::Drop; drop2.name = "x";
    alter(drop2, 3);   // y, z, ... move again

    // A v3 writer's record, laid out from the v3 layout the client computes itself.
    uint16_t nextTag = static_cast<uint16_t>(v1.fields.size());
    auto ev = rune::codec::applyOps(v1, {drop}, nextTag, 2);
    ASSERT_TRUE(ev.has_value());
    ev = rune::codec::applyOps(*ev, {drop2}, nextTag, 3);
    ASSERT_TRUE(ev.has_value());
    const rune::codec::Descriptor at3 = rune::codec::materializeAsOf(*ev, 3);
    std::vector<uint8_t> b(4 + at3.blockLength, 0);
    b[0] = static_cast<uint8_t>(at3.blockLength); b[1] = static_cast<uint8_t>(at3.blockLength >> 8); b[2] = 3;
    const int64_t uid = 33; const double y = 7.25; const uint64_t tick = 99;
    std::memcpy(b.data() + 4 + at3.find("userId")->offset, &uid, 8);
    std::memcpy(b.data() + 4 + at3.find("y")->offset, &y, 8);
    std::memcpy(b.data() + 4 + at3.find("tick")->offset, &tick, 8);

    Position s;
    EXPECT_FALSE(s.decode(b, lin2)) << "a lineage fetched before the next ALTER does not know v3: reported, not misread";
    EXPECT_EQ(s.y, 0.0);
    const Position::Decoder staleD(b, lin2);
    EXPECT_FALSE(staleD.known());
    EXPECT_EQ(staleD.y(), 0.0);
    const Position::Lineage lin3 = cl->lineageAndWait<Position>(name, 30s);
    ASSERT_EQ(lin3.maxVersion(), 3u);
    ASSERT_TRUE(s.decode(b, lin3));
    EXPECT_EQ(s.userId, uid);
    EXPECT_EQ(s.y, y);
    EXPECT_EQ(s.tick, tick);
    EXPECT_EQ(s.x, 0.0) << "dropped at v3: absent";
    EXPECT_EQ(s.zone, 0) << "dropped at v2: absent";

    EXPECT_THROW((void)cl->lineageAndWait<Position>("claim.nope", 30s), std::runtime_error) << "no such topic";
    EXPECT_THROW((void)cl->lineageAndWait<Position>(other, 30s), std::invalid_argument) << "another schema";
    cl->close();

    auto out = rig.connect("outsider", rc::RunnerMode::Thread);
    EXPECT_THROW((void)out->lineageAndWait<Position>(name, 30s), std::runtime_error)
        << "the lineage is a DESCRIBE like any schema read";
    const auto plain = out->getTopicSchemaAndWait(name, 30s);
    EXPECT_NE(plain.errorCode, 0) << "and the plain read is refused alike";
    out->close();
}
