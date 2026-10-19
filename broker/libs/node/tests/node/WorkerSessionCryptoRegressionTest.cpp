// WorkerSessionCryptoRegressionTest.cpp - authentication Task 13 fix-on-sight: Worker::findSessionCrypto_ was a linear
// scan over every encrypted session on EVERY produce frame (O(sessions) on the hot path).  The lookup is now one hash
// and a short probe chain in a slot-indexed table.
#include <rune/crypto/Aead.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

using namespace rune::node;
namespace pc = rune::protocol::client;

namespace {
WorkerOp encryptedAnnounce(int64_t sid) {
    WorkerOp a;
    a.type = WorkerOp::Type::SessionAnnounce; a.grantAll = true;
    a.sessionId = sid;
    a.egressStreamId = 7;
    a.egressChannel = "aeron:ipc";
    a.sessionEncrypted = true;
    a.senderCipher = static_cast<uint8_t>(rune::crypto::Cipher::Aes256Gcm);
    a.c2bKeyValid = true;
    for (std::size_t i = 0; i < sizeof(a.c2bKey); ++i) a.c2bKey[i] = static_cast<uint8_t>(sid + i);
    return a;
}
std::vector<uint8_t> produceFrameFor(int64_t sid) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    const auto batch = b.build();
    pc::Produce m;
    m.sessionId = sid; m.correlationId = 1; m.topicId = 1; m.partition = 0; m.partitionKnown = 1;
    m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    return f;
}
}  // namespace

TEST(WorkerSessionCryptoRegression, LookupIsSlotIndexedNotAScanRegression) {
    Worker worker(0);
    constexpr int kSessions = 4000;
    for (int64_t sid = 1; sid <= kSessions; ++sid) {
        while (!worker.tryEnqueueOp(encryptedAnnounce(sid))) worker.doWork(sid);   // the lifecycle ring drains
    }
    for (int i = 0; i < 8 && worker.sessionCryptoEntriesForTest() < static_cast<std::size_t>(kSessions); ++i)
        worker.doWork(10'000 + i);
    ASSERT_EQ(worker.sessionCryptoEntriesForTest(), static_cast<std::size_t>(kSessions));
    EXPECT_EQ(worker.sessionCryptoEntriesForTest(), worker.sessionCryptoCountForTest()) << "entries keep their meaning";

    // A produce for the LAST announced session (the worst case of the old scan) finds its entry in a short probe chain.
    // The frame is not sealed, so the open fails closed (dropped) - but the lookup it did is what is measured.
    const auto frame = produceFrameFor(kSessions);
    EXPECT_EQ(worker.handleIngressFrameForTest(frame.data(), frame.size(), 1), 0) << "an unsealed frame of an encrypted session is dropped";
    EXPECT_GE(worker.sessionCryptoLastProbesForTest(), 1u);
    EXPECT_LE(worker.sessionCryptoLastProbesForTest(), 8u) << "O(1) average, not O(sessions)";

    for (const int64_t sid : {int64_t{1}, int64_t{2000}, int64_t{3999}}) {
        ASSERT_TRUE(worker.sessionCryptoLookupForTest(sid));
        EXPECT_LE(worker.sessionCryptoLastProbesForTest(), 8u);
    }
    EXPECT_FALSE(worker.sessionCryptoLookupForTest(kSessions + 1)) << "an unknown (plaintext) session has no entry";
}

TEST(WorkerSessionCryptoRegression, ClosingASessionKeepsEveryOtherEntryReachableRegression) {
    Worker worker(0);
    for (int64_t sid = 1; sid <= 500; ++sid) {
        while (!worker.tryEnqueueOp(encryptedAnnounce(sid))) worker.doWork(sid);
    }
    for (int i = 0; i < 4; ++i) worker.doWork(10'000 + i);
    ASSERT_EQ(worker.sessionCryptoCountForTest(), 500u);
    for (int64_t sid = 1; sid <= 500; sid += 2) {   // close every odd session: backward-shift deletions everywhere
        WorkerOp c;
        c.type = WorkerOp::Type::SessionClose;
        c.sessionId = sid;
        while (!worker.tryEnqueueOp(c)) worker.doWork(sid);
    }
    for (int i = 0; i < 4; ++i) worker.doWork(20'000 + i);
    EXPECT_EQ(worker.sessionCryptoCountForTest(), 250u);
    for (int64_t sid = 1; sid <= 500; ++sid)
        EXPECT_EQ(worker.sessionCryptoLookupForTest(sid), sid % 2 == 0) << "session " << sid;
}
