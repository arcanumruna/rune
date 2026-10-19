#include "~/Projects/rune-broker-wt32/libs/storage/tests/storage/RecoveryPointTest.cpp"

// MAJOR candidate: a checkpoint write completed (durable) while the log was
// unowned; the new owner resets (outcome unknown) and then truncates between
// the persisted R and the landed value.  The lowering persist is skipped.
TEST(Rereview, ResetForNewOwnerThenTruncate_DiskRNeverAboveLeo) {
    const auto dir = freshDir("rr_reset_trunc");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    ASSERT_EQ(log->recoveryPoint(), bases[2]);
    const int64_t P = log->persistedRecoveryPoint();
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    ASSERT_EQ(performWrite(w), 0);           // LANDED durably; completion went to the unowned worker
    log->resetRecoveryCheckpointForNewOwner();   // fresh owner: outcome "unknown"
    ASSERT_LE(P, bases[1] + 1);
    log->truncateTo(bases[1] + 1);
    const int64_t leo = log->logEndOffset();
    crashKeepPageCache(log);
    EXPECT_LE(readPersistedR(dir), leo) << "on-disk R " << readPersistedR(dir)
        << " is ABOVE the truncated LEO " << leo << " (P=" << P << ")";
    fs::remove_all(dir);
}

// LOW candidate: two inline writes during ONE in-flight checkpoint; the second
// is torn -> recovery falls back to the superseded in-flight slot (higher R).
TEST(Rereview, SecondInlineWriteTornWhileInFlight_StaleHighSlotWins) {
    const auto dir = freshDir("rr_double_inline");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log); completeFrontSeal(*log); completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    log->truncateTo(bases[2] + 1);           // inline #1 -> active slot
    ASSERT_EQ(performWrite(w), 0);           // the in-flight (abandoned) write lands, value bases[3]
    log->truncateTo(bases[1] + 1);           // inline #2 -> SAME slot (still InFlight)
    const int64_t leo = log->logEndOffset();
    const int active = Log::readRecoveryPointFile(dir.string()).slot;
    crashKeepPageCache(log);
    uint8_t garbage[Log::kRpSlotBytes]; std::memset(garbage, 0xAB, sizeof garbage);
    writeSlot(dir, active, garbage, sizeof garbage);   // inline #2 torn by power loss
    EXPECT_LE(readPersistedR(dir), leo) << "torn inline write fell back to stale R " << readPersistedR(dir);
    fs::remove_all(dir);
}

// The MAJOR end-to-end: after the reset+truncate, re-append (divergent data),
// roll with async seals that never complete, power loss -> offset gap.
TEST(Rereview, ResetForNewOwnerThenTruncate_PowerLoss_NoOffsetGap) {
    const auto dir = freshDir("rr_reset_gap");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log); completeFrontSeal(*log); completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    ASSERT_EQ(performWrite(w), 0);
    log->resetRecoveryCheckpointForNewOwner();
    log->truncateTo(bases[0] + 1);
    flushActive(*log);
    // Re-append past the old R with async seals that never complete.
    while (log->logEndOffset() < w.offset * 0 + bases[3] + 2) appendOne(*log, "x");
    crashWithPowerLoss(log);
    auto rec = std::make_unique<Log>(dir.string(), kRoll);
    rec->recover();
    expectGapFree(*rec);
    fs::remove_all(dir);
}

TEST(Rereview, Control_CompletionObserved_NoOffsetGap) {
    const auto dir = freshDir("rr_ctrl_gap");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log); completeFrontSeal(*log); completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    ASSERT_EQ(performWrite(w), 0);
    log->recoveryCheckpointCompleted(w.seq, 0);
    log->truncateTo(bases[0] + 1);
    flushActive(*log);
    // Re-append past the old R with async seals that never complete.
    while (log->logEndOffset() < w.offset * 0 + bases[3] + 2) appendOne(*log, "x");
    crashWithPowerLoss(log);
    auto rec = std::make_unique<Log>(dir.string(), kRoll);
    rec->recover();
    expectGapFree(*rec);
    fs::remove_all(dir);
}
