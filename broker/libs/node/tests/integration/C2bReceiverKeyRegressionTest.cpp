// C2bReceiverKeyRegressionTest.cpp - issue #116: client->broker (c2b) frames are bound to ONE receiver.
//
// Before #116 the client sealed every c2b frame under one key and one seq space, and the broker gave that same key to
// the gateway and to every shard worker, each with its own replay window.  A frame sealed for worker W therefore also
// opened at the gateway (or at worker V), whose window had not seen its seq: the gateway forwarded it and a
// non-idempotent produce was appended twice.
//
// Now a session whose client (>= 12) and broker (>= 7) speak receiver keys seals each frame for one receiver: the id
// rides CryptoSubHeader.senderId (in the AAD; 0 = the gateway, w+1 = worker w) and the key is
// deriveReceiverKey(c2b, id).  The SessionAnnouncer ships worker w its own derived key; the gateway keeps the raw key
// (receiver 0).  A frame naming another receiver is dropped BEFORE the AEAD (counted), and would not open anyway.
//
// Rig: a REAL ClientGateway (makeForTest) in front of a REAL Worker that owns the partition (GatewayProduceRig), the
// REAL SessionAnnouncer, and frames sealed exactly as the client's sealFrame seals them.  Single-threaded, injected
// time, no sleeps.
#include "support/GatewayProduceRig.h"

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/gateway/CryptoState.h>
#include <rune/node/SessionAnnouncer.h>
#include <rune/protocol/client/CryptoEnvelope.h>

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

using rune::node::test::GatewayProduceRig;
using rune::node::test::ProduceTopicMode;
namespace pc = rune::protocol::client;
namespace cr = rune::crypto;

namespace {

constexpr cr::Cipher kCipher = cr::Cipher::Aes256Gcm;

struct SessionKeys {
    cr::AeadKey c2b{}, b2c{};
    uint8_t c2bPrefix[4] = {9, 8, 7, 6};
    uint8_t b2cPrefix[4] = {1, 2, 3, 4};
    SessionKeys() {
        for (std::size_t i = 0; i < sizeof c2b.bytes; ++i) {
            c2b.bytes[i] = static_cast<uint8_t>(i * 7 + 3);
            b2c.bytes[i] = static_cast<uint8_t>(i * 5 + 1);
        }
    }
    /// The key a client seals a frame for `receiverId` with (receiver 0 = the raw c2b key).
    cr::AeadKey receiverKey(uint8_t receiverId) const {
        cr::AeadKey k{};
        EXPECT_TRUE(cr::deriveReceiverKey(c2b, receiverId, k));
        return k;
    }
};

/// Seal `plain` ([24B header][body]) for `receiverId` at `seq`, exactly as the client's codec::sealFrame does.
std::vector<uint8_t> sealFor(const cr::AeadKey& key, const uint8_t prefix[4], uint8_t receiverId, uint64_t seq,
                             const std::vector<uint8_t>& plain) {
    cr::AeadCipher c(kCipher, key, prefix);
    std::vector<uint8_t> out(plain.size() + pc::kCryptoOverhead);
    std::memcpy(out.data(), plain.data(), pc::MessageHeader::ENCODED_SIZE);
    pc::CryptoSubHeader sub;
    sub.cipher   = static_cast<uint8_t>(kCipher);
    sub.senderId = receiverId;
    sub.seq      = seq;
    sub.encode(out.data() + pc::MessageHeader::ENCODED_SIZE);
    const std::size_t n = c.seal(seq, out.data(), pc::kCryptoHeaderBytes, plain.data() + pc::MessageHeader::ENCODED_SIZE,
                                 plain.size() - pc::MessageHeader::ENCODED_SIZE, out.data() + pc::kCryptoHeaderBytes);
    EXPECT_GT(n, 0u);
    out.resize(pc::kCryptoHeaderBytes + n);
    return out;
}

/// The gateway's half of the handshake for `sid` (ClientGateway::handleHandshakeContinue_): raw c2b opens ingress;
/// receiver keys iff the session's client speaks them.
void installGatewayCrypto(rune::gateway::CryptoState& c, const SessionKeys& k, int32_t clientVersion) {
    c.encrypted = true;
    c.cipher = kCipher;
    c.recv.emplace(kCipher, k.c2b, k.c2bPrefix);
    c.send.emplace(kCipher, k.b2c, k.b2cPrefix);
    c.b2c = k.b2c;
    std::memcpy(c.b2cPrefix, k.b2cPrefix, 4);
    c.c2b = k.c2b;
    std::memcpy(c.c2bPrefix, k.c2bPrefix, 4);
    c.receiverKeys = pc::sessionUsesReceiverKeys(clientVersion, pc::kBrokerVersion);
}

/// Announce `sid`'s keys to every worker of `pool` through the REAL SessionAnnouncer (the gateway's second wave).
void announceKeys(rune::node::WorkerPool& pool, int64_t sid, const SessionKeys& k, int32_t clientVersion) {
    rune::node::SessionAnnouncer ann(/*grantAll=*/true);
    ASSERT_TRUE(ann.announce(pool, sid, "aeron:ipc", 7, clientVersion, /*sessionEncrypted=*/true, /*keyReady=*/true,
                             k.b2c, k.b2cPrefix, kCipher, k.c2b, k.c2bPrefix));
}

int64_t records(GatewayProduceRig& r) { return r.registry->log(GatewayProduceRig::kTopic, 0)->logEndOffset(); }

struct ReceiverRig {
    GatewayProduceRig r{ProduceTopicMode::StaticRouted};
    SessionKeys k;
    explicit ReceiverRig(int32_t clientVersion = pc::kClientVersion) {
        installGatewayCrypto(*r.gw.sessionCryptoForTest(r.sid), k, clientVersion);
        announceKeys(r.pool, r.sid, k, clientVersion);
        r.pass();   // worker 0 installs the announced keys
    }
    rune::node::Worker& worker() { return r.pool.worker(0); }
};

}  // namespace

// A produce sealed for worker 0 (receiver 1) and appended there, then replayed byte for byte to the gateway, is dropped
// by the gateway (wrong receiver) - never forwarded, never appended twice.  The gateway's own seq space is untouched:
// the client's first gateway-bound frame (seq 1) is still served.
// RED (mutation): ship the raw c2b key to every worker in SessionAnnouncer and drop the receiver checks - the frame
// then opens at the gateway (or not at the worker at all).
TEST(GatewayCryptoRegression, AWorkerBoundFrameNeverOpensAtTheGatewayRegression) {
    ReceiverRig rig;
    auto& r = rig.r;
    const int64_t before = records(r);
    const auto toWorker = sealFor(rig.k.receiverKey(1), rig.k.c2bPrefix, /*receiverId=*/1, /*seq=*/1, r.produceFrame(100));
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(toWorker.data(), toWorker.size(), r.now), 1);
    r.pass();
    ASSERT_EQ(records(r), before + 1) << "worker 0 opened and appended the frame sealed for it";
    EXPECT_EQ(rig.worker().decryptFailuresForTest(), 0u);

    const uint64_t okBefore = r.produceOk;
    (void)r.gw.applyClientFrameForTest(toWorker.data(), toWorker.size());
    for (int i = 0; i < 8; ++i) r.pass();
    EXPECT_EQ(r.gw.c2bWrongReceiverDropsForTest(), 1u) << "the gateway refused a frame bound for worker 0";
    EXPECT_EQ(records(r), before + 1) << "the replayed produce was appended twice";
    EXPECT_EQ(r.produceOk, okBefore) << "the replay was answered (forwarded)";

    // The gateway's own seq space starts at 1, independent of worker 0's.
    const auto toGateway = sealFor(rig.k.c2b, rig.k.c2bPrefix, /*receiverId=*/0, /*seq=*/1, r.produceFrame(101));
    (void)r.gw.applyClientFrameForTest(toGateway.data(), toGateway.size());
    for (int i = 0; i < 8 && records(r) == before + 1; ++i) r.pass();
    EXPECT_EQ(records(r), before + 2) << "a gateway-bound frame is served";
    EXPECT_EQ(r.gw.c2bWrongReceiverDropsForTest(), 1u);
}

// A frame sealed for worker 0 replayed at worker 1 is dropped before the AEAD (wrong receiver), never opened; the
// frame sealed for worker 1 opens there.  Two real workers, the real announcer.
// RED (mutation): the same raw key on every worker and no receiver check - worker 1 opens worker 0's frame.
TEST(GatewayCryptoRegression, AFrameForOneWorkerNeverOpensAtAnotherRegression) {
    rune::node::WorkerPool pool{2, rune::metadata::ShardRouter(2), 1 << 22, 1 << 25, 64, 64};
    pool.setRunnersStartedForTest(true);
    SessionKeys k;
    constexpr int64_t kSid = 77;
    announceKeys(pool, kSid, k, pc::kClientVersion);
    for (int i = 0; i < 4; ++i) { (void)pool.worker(0).doWork(1'000 + i); (void)pool.worker(1).doWork(1'000 + i); }
    ASSERT_TRUE(pool.worker(1).sessionCryptoLookupForTest(kSid));

    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    const auto batch = b.build();
    pc::Produce m;
    m.sessionId = kSid; m.correlationId = 1; m.topicId = 0; m.partition = 0; m.partitionKnown = 1;
    m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
    m.recordBatch = std::span<const uint8_t>(batch);
    std::vector<uint8_t> plain(m.encodedSize());
    m.encode(plain.data());

    const auto forW0 = sealFor(k.receiverKey(1), k.c2bPrefix, 1, 1, plain);
    EXPECT_EQ(pool.worker(1).handleIngressFrameForTest(forW0.data(), forW0.size(), 2'000), 0) << "dropped";
    EXPECT_EQ(pool.worker(1).c2bWrongReceiverDropsForTest(), 1u);
    EXPECT_EQ(pool.worker(1).decryptFailuresForTest(), 0u) << "refused before any AEAD (no trial decrypt)";

    const auto forW1 = sealFor(k.receiverKey(2), k.c2bPrefix, 2, 1, plain);
    (void)pool.worker(1).handleIngressFrameForTest(forW1.data(), forW1.size(), 2'001);
    EXPECT_EQ(pool.worker(1).decryptFailuresForTest(), 0u) << "worker 1's own frame opens (its own derived key)";
    EXPECT_EQ(pool.worker(1).c2bWrongReceiverDropsForTest(), 1u);
}

// A gateway-bound frame (receiver 0, the raw key) sent straight to worker 0 is dropped there and never appended; the
// same bytes still serve at the gateway (worker 0's refusal committed no seq anywhere).
// RED (mutation): drop the worker's receiver check and ship the raw key - worker 0 appends it.
TEST(GatewayCryptoRegression, AGatewayBoundFrameNeverOpensAtAWorkerRegression) {
    ReceiverRig rig;
    auto& r = rig.r;
    const int64_t before = records(r);
    const auto toGateway = sealFor(rig.k.c2b, rig.k.c2bPrefix, /*receiverId=*/0, /*seq=*/1, r.produceFrame(200));
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(toGateway.data(), toGateway.size(), r.now), 0);
    r.pass();
    EXPECT_EQ(rig.worker().c2bWrongReceiverDropsForTest(), 1u);
    EXPECT_EQ(rig.worker().decryptFailuresForTest(), 0u);
    EXPECT_EQ(records(r), before) << "a gateway-bound frame was appended by a worker";

    (void)r.gw.applyClientFrameForTest(toGateway.data(), toGateway.size());
    for (int i = 0; i < 8 && records(r) == before; ++i) r.pass();
    EXPECT_EQ(records(r), before + 1) << "the gateway serves its own frame";
    EXPECT_EQ(r.gw.c2bWrongReceiverDropsForTest(), 0u);
}

// A legacy session (client 11) keeps the pre-#116 contract: the raw c2b key on every worker, senderId 0, and a direct
// produce to the worker is appended.  The gateway accepts senderId 0 as before.
TEST(GatewayCryptoRegression, ALegacyClientStillProducesStraightToAWorkerRegression) {
    ReceiverRig rig(pc::kClientVersionReceiverKeys - 1);
    auto& r = rig.r;
    ASSERT_FALSE(r.gw.sessionCryptoForTest(r.sid)->receiverKeys);
    const int64_t before = records(r);
    const auto legacy = sealFor(rig.k.c2b, rig.k.c2bPrefix, /*receiverId=*/0, /*seq=*/1, r.produceFrame(300));
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(legacy.data(), legacy.size(), r.now), 1);
    r.pass();
    EXPECT_EQ(records(r), before + 1) << "a legacy direct produce is appended";
    EXPECT_EQ(rig.worker().c2bWrongReceiverDropsForTest(), 0u);
    EXPECT_EQ(rig.worker().decryptFailuresForTest(), 0u);
    const auto legacyGw = sealFor(rig.k.c2b, rig.k.c2bPrefix, 0, /*seq=*/2, r.produceFrame(301));
    (void)r.gw.applyClientFrameForTest(legacyGw.data(), legacyGw.size());
    for (int i = 0; i < 8 && records(r) == before + 1; ++i) r.pass();
    EXPECT_EQ(records(r), before + 2) << "the legacy gateway path is unchanged";
    EXPECT_EQ(r.gw.c2bWrongReceiverDropsForTest(), 0u);
}

// ── Adversarial review of PR #528 (issue #116) ──────────────────────────────────────────────────────────────────────

// Input bounds + window state: frames naming every wrong receiver (the gateway 0, another worker 2, the top worker 64,
// and the out-of-range 65 / 255), each at seq 1 and sealed under the key it names, are dropped by worker 0 BEFORE any
// AEAD, append nothing and commit no seq - worker 0's own seq-1 frame is still appended afterwards.  The gateway does
// the same for frames naming 1, 64, 65, 255, then serves its own seq 1.
// RED (mutation): drop either receiver check - the frames then go to the AEAD and are counted as decrypt failures.
TEST(GatewayCryptoRegression, WrongReceiverFramesAtEveryBoundCommitNoWindowStateAdversarialRegression) {
    ReceiverRig rig;
    auto& r = rig.r;
    const int64_t before = records(r);
    int64_t corr = 400;
    for (const uint8_t wrong : {uint8_t{0}, uint8_t{2}, uint8_t{64}, uint8_t{65}, uint8_t{255}}) {
        SCOPED_TRACE(static_cast<int>(wrong));
        const auto f = sealFor(rig.k.receiverKey(wrong), rig.k.c2bPrefix, wrong, /*seq=*/1, r.produceFrame(++corr));
        EXPECT_EQ(rig.worker().handleIngressFrameForTest(f.data(), f.size(), r.now), 0);
        r.pass();
    }
    EXPECT_EQ(rig.worker().c2bWrongReceiverDropsForTest(), 5u);
    EXPECT_EQ(rig.worker().decryptFailuresForTest(), 0u) << "a wrong-receiver frame reached the AEAD";
    EXPECT_EQ(records(r), before) << "a wrong-receiver frame was appended";
    const auto mine = sealFor(rig.k.receiverKey(1), rig.k.c2bPrefix, 1, /*seq=*/1, r.produceFrame(++corr));
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(mine.data(), mine.size(), r.now), 1);
    r.pass();
    EXPECT_EQ(records(r), before + 1) << "worker 0's seq 1 must still open: the drops committed no window state";

    for (const uint8_t wrong : {uint8_t{1}, uint8_t{64}, uint8_t{65}, uint8_t{255}}) {
        const auto f = sealFor(rig.k.receiverKey(wrong), rig.k.c2bPrefix, wrong, /*seq=*/1, r.produceFrame(++corr));
        (void)r.gw.applyClientFrameForTest(f.data(), f.size());
        for (int i = 0; i < 4; ++i) r.pass();
    }
    EXPECT_EQ(r.gw.c2bWrongReceiverDropsForTest(), 4u);
    EXPECT_EQ(records(r), before + 1);
    const auto gw = sealFor(rig.k.c2b, rig.k.c2bPrefix, 0, /*seq=*/1, r.produceFrame(++corr));
    (void)r.gw.applyClientFrameForTest(gw.data(), gw.size());
    for (int i = 0; i < 8 && records(r) == before + 1; ++i) r.pass();
    EXPECT_EQ(records(r), before + 2) << "the gateway's seq 1 must still open";
}

// AAD binding: a frame sealed for worker 1 (receiver 2) whose clear subheader is rewritten to name worker 0 passes
// worker 0's pre-AEAD receiver check but never opens (the id is in the AAD and the key is worker 1's), is never
// appended, and poisons no window: worker 0's own seq-1 frame still opens.  Same for a worker frame rewritten to name
// the gateway.
TEST(GatewayCryptoRegression, ASubheaderRewrittenToNameTheTargetNeverOpensAdversarialRegression) {
    ReceiverRig rig;
    auto& r = rig.r;
    const int64_t before = records(r);
    auto forged = sealFor(rig.k.receiverKey(2), rig.k.c2bPrefix, 2, /*seq=*/1, r.produceFrame(500));
    auto toGateway = forged;
    pc::CryptoSubHeader sub = *pc::CryptoSubHeader::decode(forged.data() + pc::MessageHeader::ENCODED_SIZE,
                                                           forged.size() - pc::MessageHeader::ENCODED_SIZE);
    sub.senderId = 1;
    sub.encode(forged.data() + pc::MessageHeader::ENCODED_SIZE);
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(forged.data(), forged.size(), r.now), 0);
    r.pass();
    EXPECT_EQ(rig.worker().decryptFailuresForTest(), 1u) << "the forged id passed the check and failed the AEAD";
    EXPECT_EQ(records(r), before);
    const auto mine = sealFor(rig.k.receiverKey(1), rig.k.c2bPrefix, 1, /*seq=*/1, r.produceFrame(501));
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(mine.data(), mine.size(), r.now), 1);
    r.pass();
    EXPECT_EQ(records(r), before + 1);

    sub.senderId = 0;
    sub.encode(toGateway.data() + pc::MessageHeader::ENCODED_SIZE);
    (void)r.gw.applyClientFrameForTest(toGateway.data(), toGateway.size());
    for (int i = 0; i < 8; ++i) r.pass();
    EXPECT_EQ(records(r), before + 1) << "a worker frame relabelled for the gateway was forwarded";
    const auto gw = sealFor(rig.k.c2b, rig.k.c2bPrefix, 0, /*seq=*/1, r.produceFrame(502));
    (void)r.gw.applyClientFrameForTest(gw.data(), gw.size());
    for (int i = 0; i < 8 && records(r) == before + 1; ++i) r.pass();
    EXPECT_EQ(records(r), before + 2) << "the failed relabelled frame poisoned the gateway's window";
}

// Truncation sweep: worker 0's own frame cut at EVERY byte offset is never appended (and never crashes the worker);
// the full frame is then appended exactly once, and a byte-identical replay is not appended again.  The gateway's own
// frame gets the same sweep.
TEST(GatewayCryptoRegression, AReceiverFrameTruncatedAtEveryOffsetNeverAppendsAdversarialRegression) {
    ReceiverRig rig;
    auto& r = rig.r;
    const int64_t before = records(r);
    const auto full = sealFor(rig.k.receiverKey(1), rig.k.c2bPrefix, 1, /*seq=*/1, r.produceFrame(600));
    for (std::size_t cut = 0; cut < full.size(); ++cut) {
        std::vector<uint8_t> part(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(cut));
        (void)rig.worker().handleIngressFrameForTest(part.empty() ? nullptr : part.data(), part.size(), r.now);
        r.pass();
        ASSERT_EQ(records(r), before) << "a frame truncated at " << cut << " was appended";
    }
    EXPECT_EQ(rig.worker().handleIngressFrameForTest(full.data(), full.size(), r.now), 1);
    r.pass();
    EXPECT_EQ(records(r), before + 1) << "the full frame opens: no truncated copy committed its seq";
    (void)rig.worker().handleIngressFrameForTest(full.data(), full.size(), r.now);
    r.pass();
    EXPECT_EQ(records(r), before + 1) << "a replay to the same worker was appended twice";

    const auto gfull = sealFor(rig.k.c2b, rig.k.c2bPrefix, 0, /*seq=*/1, r.produceFrame(601));
    for (std::size_t cut = pc::MessageHeader::ENCODED_SIZE; cut < gfull.size(); ++cut) {
        std::vector<uint8_t> part(gfull.begin(), gfull.begin() + static_cast<std::ptrdiff_t>(cut));
        (void)r.gw.applyClientFrameForTest(part.data(), part.size());
        r.pass();
        ASSERT_EQ(records(r), before + 1) << "a gateway frame truncated at " << cut << " was forwarded";
    }
    (void)r.gw.applyClientFrameForTest(gfull.data(), gfull.size());
    for (int i = 0; i < 8 && records(r) == before + 1; ++i) r.pass();
    EXPECT_EQ(records(r), before + 2);
}

// ── Shard-mode first produce racing the key (the gateway's second announce wave) ────────────────────────────────────

namespace {
/// The gateway's FIRST (parked) wave: the session is encrypted, its keys do not exist yet (pollPendingConnects).
void announceFirstWave(rune::node::WorkerPool& pool, int64_t sid, int32_t clientVersion) {
    rune::node::SessionAnnouncer ann(/*grantAll=*/true);
    ASSERT_TRUE(ann.announce(pool, sid, "aeron:ipc", 7, clientVersion, /*sessionEncrypted=*/true, /*keyReady=*/false));
}
}  // namespace

// A shard client holds its ConnectResponse after the first wave and may send its first sealed produce straight to a
// worker before the second wave (the real keys) is applied there.  That frame is HELD (kIngressHeld: the controlled
// poll ABORTs, the frame stays in its image), never dropped, and commits no seq; once the keys land the SAME bytes are
// appended exactly once.  Covers the legacy (shared raw key) and the receiver-key client.
// RED (mutation): remove the recvPending hold in Worker::handleWorkerIngressFrame_ - the frame is dropped as a decrypt
// failure (returns 0) and never appended (the ShardClientWithRequireCryptoFailsOverToALiveBroker flake).
TEST(GatewayCryptoRegression, AProduceBeforeTheKeyWaveIsHeldThenAppendedOnceRegression) {
    for (const int32_t clientVersion : {pc::kClientVersion, pc::kClientVersionReceiverKeys - 1}) {
        SCOPED_TRACE(clientVersion);
        GatewayProduceRig r{ProduceTopicMode::StaticRouted};
        SessionKeys k;
        installGatewayCrypto(*r.gw.sessionCryptoForTest(r.sid), k, clientVersion);
        announceFirstWave(r.pool, r.sid, clientVersion);
        r.pass();   // worker 0 applies the parked first wave
        auto& w = r.pool.worker(0);
        const bool receiverKeys = pc::sessionUsesReceiverKeys(clientVersion, pc::kBrokerVersion);
        const auto key = receiverKeys ? k.receiverKey(1) : k.c2b;
        const auto frame = sealFor(key, k.c2bPrefix, receiverKeys ? 1 : 0, /*seq=*/1, r.produceFrame(400));
        const int64_t before = records(r);

        for (int i = 0; i < 3; ++i)   // redelivered while the key is still missing: held every time
            EXPECT_EQ(w.handleIngressFrameForTest(frame.data(), frame.size(), r.now), -1) << "held, not dropped";
        EXPECT_EQ(w.intakeKeyHoldsForTest(), 3u);
        EXPECT_EQ(w.decryptFailuresForTest(), 0u) << "never reached the AEAD without a key";
        r.pass();
        EXPECT_EQ(records(r), before);

        announceKeys(r.pool, r.sid, k, clientVersion);
        r.pass();   // the second wave lands
        EXPECT_EQ(w.handleIngressFrameForTest(frame.data(), frame.size(), r.now), 1) << "the held frame opens";
        r.pass();
        EXPECT_EQ(records(r), before + 1);
        EXPECT_EQ(w.handleIngressFrameForTest(frame.data(), frame.size(), r.now), 0) << "a replay is still refused";
        r.pass();
        EXPECT_EQ(records(r), before + 1) << "appended exactly once";
        EXPECT_EQ(w.decryptFailuresForTest(), 1u) << "only the replay failed";
    }
}

// The hold is ONLY for the pending first wave: a keyed wave that shipped no c2b key (an upstream derivation failure)
// still fails closed at once (dropped, counted) - a hold there would stall the client's image forever.
// RED (mutation): hold on `!cr->recv` alone - this frame returns -1 instead of 0.
TEST(GatewayCryptoRegression, AKeyedWaveWithoutAKeyStillDropsAtOnceRegression) {
    GatewayProduceRig r{ProduceTopicMode::StaticRouted};
    SessionKeys k;
    installGatewayCrypto(*r.gw.sessionCryptoForTest(r.sid), k, pc::kClientVersion);
    announceFirstWave(r.pool, r.sid, pc::kClientVersion);
    r.pass();
    rune::node::WorkerOp op;   // a keyed wave whose derivations both failed (SessionAnnouncer's fail-closed op)
    op.type             = rune::node::WorkerOp::Type::SessionAnnounce;
    op.sessionId        = r.sid;
    op.egressStreamId   = 7;
    op.egressChannel    = "aeron:ipc";
    op.clientVersion    = pc::kClientVersion;
    op.grantAll         = true;
    op.sessionEncrypted = true;
    ASSERT_TRUE(r.pool.worker(0).tryEnqueueOp(std::move(op)));
    r.pass();
    auto& w = r.pool.worker(0);
    const auto frame = sealFor(k.receiverKey(1), k.c2bPrefix, 1, /*seq=*/1, r.produceFrame(401));
    EXPECT_EQ(w.handleIngressFrameForTest(frame.data(), frame.size(), r.now), 0);
    EXPECT_EQ(w.intakeKeyHoldsForTest(), 0u);
    EXPECT_EQ(w.decryptFailuresForTest(), 1u);
}
