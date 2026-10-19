// SecurityApplierTest.cpp — authentication Task 9: every broker's SecurityApplier (spec §3; plan D28/D29/D39):
// offset-ordered apply of COMMITTED events only, verified fingerprints, an explicit halt (never a skip), the applied
// checkpoint before any effect, exactly one boot replay of [0, checkpoint], and every applied event's bytes mirrored
// to the control thread.  Real Log + a leader PartitionReplica (SecurityRig); the events are the frozen golden script.

#include "support/SecurityEventChain.h"
#include "support/SecurityRig.h"

#include <rune/node/CatalogCheckpoint.h>
#include <rune/security/SecurityReplayer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <vector>

using rune::node::test::SecurityRig;
namespace sec = rune::security;
namespace fs  = std::filesystem;

namespace {

/// The library's own replay of golden events [0, n): the digest every broker must agree on.
uint64_t libraryDigest(const std::vector<sec::SecurityEvent>& events, std::size_t n) {
    sec::SecurityReplayer r;
    for (std::size_t i = 0; i < n; ++i) {
        auto bytes = sec::encode(events[i]);
        EXPECT_TRUE(bytes.has_value());
        auto a = r.feed(static_cast<int64_t>(i), *bytes);
        EXPECT_TRUE(a.has_value());
    }
    return r.state().digest();
}

} // namespace

TEST(SecurityApplierRegression, AppliesCommittedEventsInOffsetOrderAndMirrorsTheirBytes) {
    SecurityRig rig("ap_order");
    rig.appendEvents(0, 5);          // golden events 0..4, committed
    rig.appendUncommitted(5);        // event 5 appended, HW held at 5 (follower silent)
    EXPECT_GT(rig.applier.applyCommitted(rig.replica()), 0);
    EXPECT_EQ(rig.applier.nextOffset(), 5) << "never past the HW";
    EXPECT_EQ(rig.applier.lastAppliedOffset(), 4);
    ASSERT_EQ(rig.rec.mirrored.size(), 5u);
    for (int64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(rig.rec.mirrored[static_cast<std::size_t>(i)].offset, i);
        // The mirrored bytes ARE the committed record (the control thread replays them with the same library).
        EXPECT_EQ(rig.rec.mirrored[static_cast<std::size_t>(i)].bytes,
                  SecurityRig::encodeEvent(rig.golden()[static_cast<std::size_t>(i)]));
    }
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    EXPECT_EQ(rig.applier.state().appliedOffset(), 4);
    EXPECT_EQ(rig.applier.state().digest(), libraryDigest(rig.golden(), 5));

    // The rest commits (follower acks) and applies, still in order, exactly once.
    rig.appendEvents(6, 16);   // 5 is already in the log: the golden order is preserved by the log itself
    (void)rig.applier.applyCommitted(rig.replica());
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0) << "idle: nothing to apply";
}

TEST(SecurityApplierRegression, FingerprintMismatchHaltsAtThatOffsetAndAppliesNothingAfter) {
    SecurityRig rig("ap_halt");
    rig.appendEvents(0, 3);
    rig.appendTampered(3);           // event 3 with fingerprint ^ 1
    rig.appendEvents(4, 6);
    rig.applier.applyCommitted(rig.replica());
    EXPECT_TRUE(rig.applier.halted());
    EXPECT_EQ(rig.applier.stateName(), "Halted");
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 3);
    EXPECT_EQ(rig.rec.halts[0].code, sec::ApplyError::Code::FingerprintMismatch);
    EXPECT_NE(rig.rec.halts[0].expected, rig.rec.halts[0].actual);
    EXPECT_EQ(rig.rec.appliedOffsets.back(), 2) << "what was applied stays applied";
    EXPECT_EQ(rig.applier.state().appliedOffset(), 2);
    EXPECT_EQ(rig.applier.nextOffset(), -1);
    // Nothing after the halt is ever applied, however many passes run.
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0);
    EXPECT_EQ(rig.rec.mirrored.size(), 3u);
}

TEST(SecurityApplierRegression, TamperSeamHaltsLikeADamagedRecord) {
    SecurityRig rig("ap_tamper");
    rig.appendEvents(0, 2);
    rig.applier.tamperNextFingerprintForTest();
    rig.applier.applyCommitted(rig.replica());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 0);
    EXPECT_EQ(rig.rec.halts[0].code, sec::ApplyError::Code::FingerprintMismatch);
    EXPECT_TRUE(rig.rec.mirrored.empty());
}

TEST(SecurityApplierRegression, UndecodableRecordHaltsNeverSkipped) {
    SecurityRig rig("ap_garbage");
    rig.appendEvents(0, 2);
    rig.appendRawCommitted({0xde, 0xad, 0xbe, 0xef});   // not a security event
    rig.appendEvents(2, 4);
    rig.applier.applyCommitted(rig.replica());
    EXPECT_TRUE(rig.applier.halted());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 2);
    EXPECT_EQ(rig.rec.mirrored.size(), 2u);
}

TEST(SecurityApplierRegression, OversizeEventHaltsAtThatOffsetNeverTruncated) {
    SecurityRig rig("ap_oversize", /*goLive=*/false);
    rig.appendEvents(0, 2);
    auto live = rig.rec.liveEffects();
    live.sink.maxFrameBytes = 8 + 40;   // far below one event (a verifier alone is 150 bytes)
    ASSERT_TRUE(rig.applier.goLive(std::move(live)));
    rig.applier.applyCommitted(rig.replica());
    EXPECT_TRUE(rig.applier.halted());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 0);
    EXPECT_TRUE(rig.rec.mirrored.empty()) << "an event that cannot be mirrored in full is never mirrored at all";
    EXPECT_EQ(rig.applier.state().appliedOffset(), -1) << "and never applied";
}

TEST(SecurityApplierRegression, CheckpointPrecedesTheEffectSoARestartNeverReappliesRegression) {
    SecurityRig rig("ap_ckpt");
    rig.appendEvents(0, 6);
    rig.rec.onEventApplied = [](int64_t off) { if (off == 3) throw std::runtime_error("crash after the checkpoint"); };
    EXPECT_THROW((void)rig.applier.applyCommitted(rig.replica()), std::runtime_error);
    // The crash seam fired inside the effect of offset 3: the checkpoint already names it.
    auto ck = rune::node::readLogCheckpoint(rig.securityDir(), rune::node::kSecurityLogKind);
    ASSERT_TRUE(ck.has_value());
    EXPECT_EQ(ck->appliedOffset, 3);

    rig.restart();
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), 4) << "replays [0, 3]";
    EXPECT_FALSE(rig.applier.halted());
    EXPECT_EQ(rig.applier.nextOffset(), 4);
    EXPECT_EQ(rig.applier.state().appliedOffset(), 3);
    EXPECT_TRUE(rig.rec.bootMirrored.empty()) << "the boot replay mirrors nothing per event";
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    (void)rig.applier.applyCommitted(rig.replica());
    // eventApplied ran for the offsets AFTER 3 only: offset 3 is never effected twice.
    std::vector<int64_t> offsets;
    for (const auto& m : rig.rec.mirrored) offsets.push_back(m.offset);
    EXPECT_EQ(offsets, (std::vector<int64_t>{4, 5}));
}

TEST(SecurityApplierRegression, CrashBeforeTheCheckpointReappliesTheEventLive) {
    SecurityRig rig("ap_ckpt_before");
    rig.appendEvents(0, 4);
    (void)rig.applier.applyCommitted(rig.replica());   // 0..3 applied, checkpoint at 3
    rig.restart();
    // Simulate "crash before the checkpoint of offset 3": rewind the checkpoint to 2 with the digest of [0, 2].
    sec::SecurityReplayer r;
    for (std::size_t i = 0; i < 3; ++i) { auto b = sec::encode(rig.golden()[i]); ASSERT_TRUE(r.feed(static_cast<int64_t>(i), *b)); }
    ASSERT_TRUE(rune::node::writeLogCheckpoint(rig.securityDir(), {2, r.state().digest()}, rune::node::kSecurityLogKind));
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), 3);
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    (void)rig.applier.applyCommitted(rig.replica());
    ASSERT_EQ(rig.rec.mirrored.size(), 1u);
    EXPECT_EQ(rig.rec.mirrored[0].offset, 3) << "the unfinished event is applied live, not skipped";
}

TEST(SecurityApplierRegression, BootReplayVerifiesTheDigestAgainstTheCheckpoint) {
    SecurityRig rig("ap_digest");
    rig.appendEvents(0, 10);
    (void)rig.applier.applyCommitted(rig.replica());
    const uint64_t digest = rig.applier.state().digest();
    rig.restart();
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), 10);
    EXPECT_FALSE(rig.applier.halted());
    EXPECT_EQ(rig.applier.state().digest(), digest);
    rig.applier.postBootStash(rig.rec.effects(false));
    ASSERT_EQ(rig.rec.appliedOffsets.size(), 1u) << "ONE coalesced applied for the whole boot prefix";
    EXPECT_EQ(rig.rec.appliedOffsets[0], 9);
    EXPECT_EQ(rig.rec.appliedDigests[0], digest);
    EXPECT_TRUE(rig.rec.halts.empty());

    // A checkpoint whose digest the log does not reproduce = local log damage: Halted, delivered by postBootStash.
    rig.restart();
    ASSERT_TRUE(rune::node::writeLogCheckpoint(rig.securityDir(), {9, digest ^ 0xff}, rune::node::kSecurityLogKind));
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), 10);
    EXPECT_TRUE(rig.applier.halted());
    ASSERT_NE(rig.applier.haltError(), nullptr);
    EXPECT_EQ(rig.applier.haltError()->code, sec::ApplyError::Code::FingerprintMismatch);
    rig.rec.halts.clear();   // the boot sink may observe the halt as it happens; production's boot sink does not
    rig.applier.postBootStash(rig.rec.effects(false));
    ASSERT_EQ(rig.rec.halts.size(), 1u) << "postBootStash delivers a boot-time halt once the broker is running";
}

TEST(SecurityApplierRegression, CheckpointBeyondTheLogRefusesToStart) {
    SecurityRig rig("ap_beyond");
    rig.appendEvents(0, 3);
    (void)rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_TRUE(rune::node::writeLogCheckpoint(rig.securityDir(), {7, 1}, rune::node::kSecurityLogKind));
    EXPECT_THROW((void)rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), std::runtime_error);
    // A damaged checkpoint file is local damage too: it throws, never "none".
    rig.restart();
    { std::ofstream f(fs::path(rig.securityDir()) / rune::node::kCatalogCheckpointFile, std::ios::binary | std::ios::trunc); f << "junk"; }
    EXPECT_THROW((void)rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), std::runtime_error);
}

TEST(SecurityApplierRegression, ACatalogCheckpointIsNeverReadAsASecurityOne) {
    SecurityRig rig("ap_magic");
    rig.appendEvents(0, 2);
    ASSERT_TRUE(rune::node::writeLogCheckpoint(rig.securityDir(), {1, 5}, rune::node::kCatalogLogKind));
    rig.restart();
    EXPECT_THROW((void)rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), std::runtime_error);
}

TEST(SecurityApplierRegression, ExactlyOneBootReplay) {
    SecurityRig rig("ap_once");
    rig.appendEvents(0, 3);
    (void)rig.applier.applyCommitted(rig.replica());
    rig.restart();
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), 3);
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), -1) << "a second replay is refused";
    EXPECT_EQ(rig.applier.phaseName(), "Booted");
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_FALSE(rig.applier.goLive(rig.rec.liveEffects())) << "already Live";
    EXPECT_EQ(rig.applier.replayLocal(rig.log(), rig.rec.bootEffects()), -1);

    // AwaitingBoot -> Live is an EMPTY log's path only: with a checkpoint the boot replay must run first.
    rig.restart();
    EXPECT_EQ(rig.applier.phaseName(), "AwaitingBoot");
    EXPECT_FALSE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_EQ(rig.applier.phaseName(), "AwaitingBoot");
}

TEST(SecurityApplierRegression, BootReplayOf10kEventsUnderOneSecond) {
    // 10 000 CreateUser events, committed, applied live, then replayed from the checkpoint.
    SecurityRig rig("ap_10k");
    rune::node::test::SecurityEventChain chain;
    for (int i = 0; i < 10'000; ++i) rig.appendEventCommitted(chain.createUser("user" + std::to_string(i)));
    // The checkpoint a live broker would hold after applying all 10k (one fsync each — not what is measured).
    const uint64_t digest = chain.state().digest();
    ASSERT_EQ(chain.state().appliedOffset(), 9'999);
    ASSERT_TRUE(rune::node::writeLogCheckpoint(rig.securityDir(), {9'999, digest}, rune::node::kSecurityLogKind));
    rig.restart();
    const auto t0 = std::chrono::steady_clock::now();
    const int n = rig.applier.replayLocal(rig.log(), rig.rec.bootEffects());
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_EQ(n, 10'000);
    EXPECT_FALSE(rig.applier.halted());
    EXPECT_EQ(rig.applier.state().digest(), digest);
#ifdef NDEBUG
    EXPECT_LT(ms, 1000) << "the boot replay of 10k security events";
#else
    RecordProperty("boot_replay_ms", static_cast<int>(ms));   // Debug just runs it
#endif
}
