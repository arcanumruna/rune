// CatalogApplierTest.cpp — catalog history Task 7: every broker's CatalogApplier
// (spec §5.3, ruling 8): offset-ordered apply of COMMITTED events only, verified
// fingerprints, an explicit halt (never a skip), the applied checkpoint before any
// side effect, exactly one boot replay of [0, checkpoint], and the production
// effect sink (AdminWorkerAgent) on the admin→control ring.
//
// Real Log + a leader PartitionReplica (CatalogRig, support/CatalogRig.h); the
// events are the frozen golden script (rune::catalog::testing::goldenScriptV1).

#include "support/CatalogRig.h"

#include <rune/node/AdminChannels.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/BrokerNode.h>   // AdminWorkerAgent (the production sinks)
#include <rune/node/CatalogApplier.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/CatalogEffectFrames.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

using rune::node::test::CatalogRig;
namespace cat = rune::catalog;
namespace fs  = std::filesystem;

namespace {

std::size_t indexOf(const std::vector<std::string>& seq, const std::string& s) {
    return static_cast<std::size_t>(std::find(seq.begin(), seq.end(), s) - seq.begin());
}
bool has(const std::vector<std::string>& seq, const std::string& s) { return indexOf(seq, s) < seq.size(); }

} // namespace

// ═══ The plan's tests (Task 7, Step 1) ═══════════════════════════════════════

TEST(CatalogApplierRegression, AppliesCommittedEventsInOffsetOrderOnly) {
    CatalogRig rig("ap_order");                       // log + leader replica (ISR {self, 2}) + registries + recorder
    rig.appendEvents(0, 5);                            // golden events 0..4, committed
    rig.appendUncommitted(5);                          // event 5 appended, HW held at 5 (2 silent)
    EXPECT_GT(rig.applier.applyCommitted(rig.replica()), 0);
    EXPECT_EQ(rig.applier.nextOffset(), 5) << "never past the HW";
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2, 3, 4}));
    EXPECT_EQ(rig.applier.lastApplied().offset, 4) << "F28: the leader answers a requestId-less DDL from it";
    EXPECT_EQ(rig.applier.lastApplied().applied.kind, rune::catalog::EventKind::AlterSchema);   // golden offset 4
}

TEST(CatalogApplierRegression, FingerprintMismatchHaltsApplyRaisesTheMetricAndKeepsServingRegression) {
    CatalogRig rig("ap_halt");
    rig.appendEvents(0, 3);
    rig.appendTampered(3);                             // event 3 with fingerprint ^ 1
    rig.appendEvents(4, 6);
    rig.applier.applyCommitted(rig.replica());
    EXPECT_TRUE(rig.applier.halted());
    EXPECT_EQ(rig.applier.stateName(), "Halted");
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 3);
    EXPECT_EQ(rig.rec.halts[0].code, rune::catalog::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(rig.rec.appliedOffsets.back(), 2) << "what was applied stays applied (still served)";
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0) << "halted: no further apply, ever";
    EXPECT_NE(rig.schemas.byName("players"), nullptr) << "projection of the applied prefix intact";
}

TEST(CatalogApplierRegression, CheckpointPrecedesSideEffectsSoARestartNeverQuarantinesRegression) {
    CatalogRig rig("ap_ckpt");
    rig.appendEvents(0, 2);
    std::optional<rune::node::CatalogCheckpoint> seenAtEffect;
    rig.rec.onTopicApplied = [&] { seenAtEffect = rune::node::readCatalogCheckpoint(rig.catalogDir()); };
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(seenAtEffect.has_value()) << "the checkpoint exists before the first side effect";
    EXPECT_GE(seenAtEffect->appliedOffset, 0);
}

// Stronger than the above (which passes once ANY earlier checkpoint exists): at the
// first side effect of EVERY event, the durable checkpoint already names THAT event.
TEST(CatalogApplierRegression, EveryEventsCheckpointPrecedesItsFirstSideEffectRegression) {
    CatalogRig rig("ap_ckpt_each");
    rig.appendEvents(0, 3);   // three CreateTopic events: topicApplied is each one's first effect
    std::vector<int64_t> seen;
    rig.rec.onTopicApplied = [&] {
        const auto c = rune::node::readCatalogCheckpoint(rig.catalogDir());
        seen.push_back(c ? c->appliedOffset : -1);
    };
    rig.applier.applyCommitted(rig.replica());
    EXPECT_EQ(seen, (std::vector<int64_t>{0, 1, 2}));
}

TEST(CatalogApplierRegression, BootReplaysExactlyTheCheckpointedPrefix) {
    CatalogRig rig("ap_boot");
    rig.appendEvents(0, 10);
    rig.applier.applyCommitted(rig.replica());
    const uint64_t digest = rig.applier.state().digest();
    rig.restart();                                      // same dir: log recovered, fresh applier (AwaitingBoot) + registries
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 10);
    EXPECT_EQ(rig.applier.phaseName(), "Booted");
    EXPECT_EQ(rig.applier.state().digest(), digest);
    EXPECT_FALSE(rig.rec.bootViewInstalls.empty()) << "golden views reinstalled through the BOOT sink (recovery installs)";
    EXPECT_TRUE(rig.rec.liveViewInstalls.empty()) << "the phase, not a flag, selects the sink (F4)";
}

TEST(CatalogApplierRegression, ExactlyOneBootReplayRegression) {
    // F4: a second replay of the same prefix would re-apply recorded request ids (Precondition → halt).
    CatalogRig rig("ap_once");
    rig.appendEvents(0, 6);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 6);
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), -1) << "refused, explicitly";
    EXPECT_FALSE(rig.applier.halted());
    rig.applier.goLive(rig.rec.liveEffects());
    EXPECT_EQ(rig.applier.phaseName(), "Live");
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), -1);
    EXPECT_EQ(rig.applier.nextOffset(), 6);
}

TEST(CatalogApplierRegression, BootDigestDisagreeingWithTheCheckpointHalts) {
    CatalogRig rig("ap_dmg");
    rig.appendEvents(0, 4);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {3, 0xBAD}));
    rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects());
    EXPECT_TRUE(rig.applier.halted());
}

TEST(CatalogApplierRegression, AlterViewEffectsRunInstallBeforeUninstallAndRetireTheOldSink) {
    CatalogRig rig("ap_view");
    rig.appendEvents(0, 11);                            // through golden AlterView top_players (offset 10)
    rig.applier.applyCommitted(rig.replica());
    const auto& seq = rig.rec.sequence;                 // e.g. "sinkApplied:3", "viewInstalled:3", …
    // Golden script: CreateView top_players (offset 9) took sink topic id 3; AlterView (offset 10) takes sink 4 (F2).
    auto at = [&](const std::string& s) {
        const auto i = static_cast<std::size_t>(std::find(seq.begin(), seq.end(), s) - seq.begin());
        EXPECT_LT(i, seq.size()) << "effect '" << s << "' never ran";
        return i;
    };
    EXPECT_LT(at("sinkApplied:4"), at("viewInstalled:4")) << "view sinks go through sinkApplied (F27)";
    EXPECT_LT(at("viewInstalled:4"), at("viewUninstalled:3"));
    EXPECT_LT(at("viewUninstalled:3"), at("topicDropped:3"));
}

// ═══ Review M8 (Task 6 carry-over): never replay a shorter history ═══════════

TEST(CatalogApplierRegression, CheckpointAtOrPastTheLocalLogEndRefusesToBootRegression) {
    CatalogRig rig("ap_short");
    rig.appendEvents(0, 3);
    rig.applier.applyCommitted(rig.replica());
    const uint64_t digest = rig.applier.state().digest();
    // The log now ends at 3.  A checkpoint naming offset 3 (or beyond) means this
    // broker applied events its log no longer holds (lost writes / a swapped dir).
    for (const int64_t applied : {int64_t{3}, int64_t{9}}) {
        rig.restart();
        ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {applied, digest}));
        try {
            (void)rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects());
            ADD_FAILURE() << "a checkpoint at " << applied << " past the log end (3) booted";
        } catch (const std::runtime_error& e) {
            const std::string what = e.what();
            EXPECT_NE(what.find("Refusing to start"), std::string::npos) << what;
            EXPECT_NE(what.find("ends at 3"), std::string::npos) << what;
        }
        EXPECT_TRUE(rig.rec.appliedOffsets.empty()) << "nothing of the shorter history was applied";
    }
    // The checkpoint the applier really wrote (offset 2 < LEO 3) boots.
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {2, digest}));
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 3);
    EXPECT_FALSE(rig.applier.halted());
}

// ═══ Every halt is explicit; nothing is skipped; nothing is applied twice ═══

TEST(CatalogApplierRegression, UndecodableRecordHaltsAndNothingAfterItIsAppliedRegression) {
    CatalogRig rig("ap_decode");
    rig.appendEvents(0, 3);
    rig.appendRawCommitted({0x01, 0x00, 0x01});          // offset 3: formatVer 1, truncated — not a catalog event
    rig.appendEvents(3, 5);                              // offsets 4, 5: valid events behind it
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 3);
    ASSERT_TRUE(rig.applier.halted());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 3);
    EXPECT_EQ(rig.rec.halts[0].code, cat::ApplyError::Code::BadOps);
    EXPECT_NE(rig.rec.halts[0].detail.find("does not decode"), std::string::npos) << rig.rec.halts[0].detail;
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2})) << "never skipped past";
    EXPECT_EQ(rig.applier.nextOffset(), -1);
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0);
    ASSERT_NE(rig.applier.haltError(), nullptr);
    EXPECT_EQ(rig.applier.haltError()->offset, 3);
}

TEST(CatalogApplierRegression, ApplyRefusalHaltsAtItsOffsetRegression) {
    CatalogRig rig("ap_refuse");
    rig.appendEvents(0, 1);
    rig.appendEvents(0, 1);                              // CreateTopic players again: the rules refuse it
    rig.appendEvents(1, 2);
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(rig.applier.halted());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 1);
    EXPECT_EQ(rig.rec.halts[0].code, cat::ApplyError::Code::Precondition);
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0}));
    EXPECT_EQ(std::count(rig.rec.sequence.begin(), rig.rec.sequence.end(), "topicApplied:0"), 1)
        << "the refused event posted no effect";
}

// Fix round 1 (M5): the crash points are SIMULATED, not inferred.
TEST(CatalogApplierRegression, RestartNeverAppliesAnEventTwiceNorSkipsOneRegression) {
    // (a) A crash AFTER offset 2's checkpoint, inside its first side effect: the
    //     effect throws out of applyCommitted (the process "dies" there).
    {
        CatalogRig rig("ap_crash_a");
        rig.appendEvents(0, 5);
        rig.rec.onTopicAppliedId = [](int32_t id) {
            if (id == 2) throw std::runtime_error("simulated crash in offset 2's first side effect");
        };
        EXPECT_THROW((void)rig.applier.applyCommitted(rig.replica()), std::runtime_error);
        const auto ck = rune::node::readCatalogCheckpoint(rig.catalogDir());
        ASSERT_TRUE(ck.has_value());
        EXPECT_EQ(ck->appliedOffset, 2) << "the checkpoint of offset 2 was durable before its effect ran";

        rig.restart();
        ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 3) << "boot replays [0, 2]";
        EXPECT_TRUE(has(rig.rec.sequence, "topicApplied:2")) << "offset 2's effect re-runs through the BOOT sink";
        EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{2})) << "the coalesced boot sink: one applied";
        const std::size_t bootSeq = rig.rec.sequence.size();
        ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
        EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 2);
        const std::vector<std::string> live(rig.rec.sequence.begin() + static_cast<std::ptrdiff_t>(bootSeq),
                                            rig.rec.sequence.end());
        EXPECT_FALSE(has(live, "topicApplied:2")) << "offset 2 is never applied live a second time";
        EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{2, 3, 4}));
    }
    // (b) A crash BEFORE offset 2's checkpoint: the durable checkpoint still names 1.
    {
        CatalogRig rig("ap_crash_b");
        rig.appendEvents(0, 5);
        rig.applier.applyCommitted(rig.replica());
        const uint64_t digestAt1 = rig.rec.appliedDigests.at(1);
        rig.restart();
        ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {1, digestAt1}));
        ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 2) << "boot replays [0, 1]";
        EXPECT_FALSE(has(rig.rec.sequence, "topicApplied:2"));
        ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
        EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 3);
        EXPECT_TRUE(has(rig.rec.sequence, "topicApplied:2")) << "offset 2 is re-applied live: not skipped";
        EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{1, 2, 3, 4}));
        EXPECT_FALSE(rig.applier.halted());
    }
}

TEST(CatalogApplierRegression, CheckpointWriteFailureIsAStorageFailureAndPostsNoEffectRegression) {
    if (::geteuid() == 0) GTEST_SKIP() << "root ignores directory permissions";
    CatalogRig rig("ap_ckfail");
    rig.appendEvents(0, 2);
    ASSERT_EQ(::chmod(rig.catalogDir().c_str(), 0500), 0);   // the log stays readable; no new file can be created
    const int applied = rig.applier.applyCommitted(rig.replica());
    ASSERT_EQ(::chmod(rig.catalogDir().c_str(), 0755), 0);
    EXPECT_EQ(applied, 0);
    EXPECT_TRUE(rig.applier.halted());
    ASSERT_EQ(rig.rec.storageFailures.size(), 1u) << "posted as a storage failure (the broker's halt path)";
    EXPECT_EQ(rig.rec.storageFailures[0].second, rig.catalogDir());
    EXPECT_TRUE(rig.rec.sequence.empty()) << "no side effect without a durable checkpoint";
    EXPECT_TRUE(rig.rec.appliedOffsets.empty());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 0);
    // A restart re-applies offset 0 (no checkpoint survived): nothing was skipped.
    rig.restart();
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0);
    rig.applier.goLive(rig.rec.liveEffects());
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 2);
}

TEST(CatalogApplierRegression, NothingAppliesBeforeGoLiveAndAnEmptyCatalogBootsToOffsetZero) {
    CatalogRig rig("ap_phase");
    rig.appendEvents(0, 3);
    rig.restart();                                       // AwaitingBoot, no checkpoint yet
    EXPECT_EQ(rig.applier.phaseName(), "AwaitingBoot");
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0) << "applyCommitted runs in Live only";
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0) << "no checkpoint: nothing to replay";
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0) << "Booted is not Live";
    rig.applier.goLive(rig.rec.liveEffects());
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 3);
    EXPECT_EQ(rig.rec.liveViewInstalls.size(), 0u);
}

TEST(CatalogApplierRegression, FingerprintSeamFlipsExactlyTheNextEvent) {
    CatalogRig rig("ap_seam");
    rig.appendEvents(0, 2);
    rig.applier.applyCommitted(rig.replica());
    rig.applier.tamperNextFingerprintForTest();
    rig.appendEvents(2, 3);
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(rig.applier.halted());
    EXPECT_EQ(rig.rec.halts.at(0).offset, 2);
    EXPECT_EQ(rig.rec.halts.at(0).code, cat::ApplyError::Code::FingerprintMismatch);
}

// ═══ The whole golden script: projections + every effect kind ════════════════

TEST(CatalogApplierRegression, GoldenScriptProjectsTheRegistriesAndPostsEveryEffectKind) {
    CatalogRig rig("ap_golden");
    rig.appendEvents(0, rig.golden().size());
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), static_cast<int>(rig.golden().size()));
    ASSERT_FALSE(rig.applier.halted());
    const auto& seq = rig.rec.sequence;
    for (const char* s : {"topicApplied:0", "topicApplied:1", "topicApplied:2", "topicApplied:6", "topicApplied:7",
                          "sinkApplied:3", "sinkApplied:4", "sinkApplied:5", "sinkApplied:8", "sinkApplied:9",
                          "schemaChanged:1", "schemaChanged:0", "schemaChanged:6", "topicConfig:1",
                          "viewUninstalled:5", "topicDropped:5", "topicDropped:2", "topicDropped:0",
                          "viewUninstalled:8", "topicDropped:8"})
        EXPECT_TRUE(has(seq, s)) << s;
    EXPECT_EQ(rig.rec.liveViewInstalls, (std::vector<int32_t>{3, 4, 5, 8, 9}));
    EXPECT_LT(indexOf(seq, "viewUninstalled:5"), indexOf(seq, "topicDropped:5")) << "DropView: plan stops, then sink";

    // SchemaRegistry: the reused (untyped) players has no schema; presence is typed (v3:
    // CreateSchema, AlterSchema; SetCompatibility keeps the version); events typed.
    EXPECT_EQ(rig.schemas.byName("players"), nullptr) << "dropped at 16, reused untyped at 17";
    EXPECT_EQ(rig.schemas.byId(0), nullptr);
    const rune::node::RegistryEntry* presence = rig.schemas.byName("presence");
    ASSERT_NE(presence, nullptr);
    EXPECT_EQ(presence->topicId, 6);
    EXPECT_EQ(presence->compatMode, rune::codec::CompatMode::Forward);
    EXPECT_EQ(presence->version, rig.applier.state().topicByName("presence")->schema->schemaVersion);
    ASSERT_NE(rig.schemas.byName("events"), nullptr);
    EXPECT_EQ(rig.schemas.byName("events")->topicId, 1);

    // ViewRegistry: only the live ev_view (sink 9); its typed output is in the SchemaRegistry.
    EXPECT_EQ(rig.views.byName("top_players"), nullptr);
    ASSERT_NE(rig.views.byName("ev_view"), nullptr);
    EXPECT_EQ(rig.views.byName("ev_view")->viewTopicId, 9);
    EXPECT_EQ(rig.views.all().size(), 1u);
    ASSERT_NE(rig.schemas.byName("ev_view"), nullptr);
    EXPECT_EQ(rig.schemas.byName("ev_view")->topicId, 9);
    EXPECT_EQ(rig.schemas.byId(8), nullptr) << "the retired sink's output is erased";
    EXPECT_EQ(rig.schemas.byId(3), nullptr);

    // Boot replay of the same history reproduces the same projections and digest.
    const uint64_t digest = rig.applier.state().digest();
    rig.restart();
    EXPECT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), static_cast<int>(rig.golden().size()));
    EXPECT_EQ(rig.applier.state().digest(), digest);
    EXPECT_EQ(rig.rec.bootViewInstalls, (std::vector<int32_t>{9})) << "coalesced: the live view only (I1)";
    EXPECT_FALSE(has(rig.rec.sequence, "topicDropped:0")) << "no create/drop pair for a dropped object";
    EXPECT_FALSE(has(rig.rec.sequence, "viewUninstalled:3"));
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{22})) << "one applied, at the checkpoint";
    ASSERT_NE(rig.views.byName("ev_view"), nullptr);
    EXPECT_EQ(rig.views.all().size(), 1u);
    ASSERT_NE(rig.schemas.byName("presence"), nullptr);
}

// ═══ The production sink (AdminWorkerAgent) on the admin→control ring ═════════

namespace {

struct Frame {
    int32_t              type;
    std::vector<uint8_t> bytes;
};
std::vector<Frame> drainFailureRing(rune::node::AdminWorkerAgent& agent) {
    std::vector<Frame> out;
    // A fixed number of passes (a wrapped ring answers its end padding as a pass of 0 frames).
    for (int i = 0; i < 64; ++i) {
        (void)agent.failureRing().drain([&](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                            aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            out.push_back(Frame{tid, std::vector<uint8_t>(p, p + len)});
        }, 1024);
    }
    return out;
}
std::vector<int32_t> typesOf(const std::vector<Frame>& f) {
    std::vector<int32_t> t;
    for (const auto& x : f) t.push_back(x.type);
    return t;
}

} // namespace

TEST(CatalogApplierRegression, ProductionSinkEncodesEveryEffectOnTheAdminToControlRingInApplyOrder) {
    using namespace rune::node;
    CatalogRig rig("ap_prod");
    AdminWorkerAgent agent(rig.schemas, /*selfId=*/1, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    rig.applier.goLive(agent.catalogLiveEffects());
    rig.appendEvents(0, 11);   // through AlterView top_players
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), 11);
    const auto frames = drainFailureRing(agent);
    // 0 CreateTopic players (typed, validate) → filter schema + applied + validator + filter schema (Phase 2c: a
    // typed topic's FilterSchema goes before TopicApplied and again after the validator; idempotent);
    // 1, 2 CreateTopic untyped; 3 CreateSchema events (validate false) → no validator, but a filter schema;
    // 4, 5 AlterSchema players → validator + filter schema; 6 SetCompatibility players → likewise;
    // 7, 8 AlterTopicConfig events; 9 CreateView;
    // 10 AlterView: new sink, new plan, old plan out, old sink dropped.
    const std::vector<int32_t> expect{
        kA2cFilterSchema, kA2cCatalogTopicApplied, kA2cSchemaUpdate, kA2cFilterSchema, kA2cCatalogApplied,   // 0
        kA2cCatalogTopicApplied, kA2cCatalogApplied,                      // 1
        kA2cCatalogTopicApplied, kA2cCatalogApplied,                      // 2
        kA2cFilterSchema, kA2cCatalogApplied,                             // 3
        kA2cSchemaUpdate, kA2cFilterSchema, kA2cCatalogApplied,           // 4
        kA2cSchemaUpdate, kA2cFilterSchema, kA2cCatalogApplied,           // 5
        kA2cSchemaUpdate, kA2cFilterSchema, kA2cCatalogApplied,           // 6
        kA2cCatalogTopicConfig, kA2cCatalogApplied,                       // 7
        kA2cCatalogTopicConfig, kA2cCatalogApplied,                       // 8
        kA2cCatalogSinkApplied, kA2cViewPlanUpdate, kA2cCatalogApplied,   // 9 (Task 12: a sink's own type)
        kA2cCatalogSinkApplied, kA2cViewPlanUpdate, kA2cViewUninstall, kA2cCatalogTopicDropped,
        kA2cCatalogApplied,                                               // 10
    };
    ASSERT_EQ(typesOf(frames), expect);

    CatalogTopicAppliedMsg players;
    FilterSchemaMsg playersFilter;
    ASSERT_TRUE(decodeFilterSchema(frames[0].bytes.data(), frames[0].bytes.size(), playersFilter));
    EXPECT_EQ(playersFilter.topicId, 0);
    EXPECT_EQ(playersFilter.version, 1);
    EXPECT_FALSE(playersFilter.blob.empty()) << "the evolving descriptor";
    ASSERT_TRUE(decodeCatalogTopicApplied(frames[1].bytes.data(), frames[1].bytes.size(), players));
    EXPECT_EQ(players.topicId, 0);
    EXPECT_EQ(players.name, "players");
    EXPECT_EQ(players.partitions, 8);
    EXPECT_EQ(players.replicationFactor, 3);
    EXPECT_EQ(players.config, std::get<cat::CreateTopicOps>(rig.golden()[0].ops).config) << "the exact TopicConfig";

    CatalogTopicConfigMsg cfg;
    ASSERT_TRUE(decodeCatalogTopicConfig(frames[20].bytes.data(), frames[20].bytes.size(), cfg));
    EXPECT_EQ(cfg.topicId, 1);
    ASSERT_EQ(cfg.count, 1);
    EXPECT_EQ(cfg.entries[0], (CatalogConfigEntry{static_cast<uint8_t>(cat::ConfigKey::RetentionMs), 60'000}));

    CatalogTopicAppliedMsg sink;
    ASSERT_TRUE(decodeCatalogTopicApplied(frames[24].bytes.data(), frames[24].bytes.size(), sink));
    EXPECT_EQ(sink.topicId, 3);
    EXPECT_EQ(sink.name, "top_players");
    EXPECT_EQ(sink.partitions, 2);
    EXPECT_EQ(sink.replicationFactor, 0) << "a view sink lives on every broker";
    EXPECT_EQ(sink.config.cleanupPolicy, 1) << "a view sink is a keyed, compacted table";
    EXPECT_EQ(sink.config.partitionKey, "id");

    const ViewPlanUpdateMsg plan = decodeViewPlanUpdate(frames[25].bytes.data(), frames[25].bytes.size());
    EXPECT_EQ(plan.viewTopicId, 3);
    EXPECT_FALSE(plan.recovery) << "the LIVE sink posts commit installs";
    EXPECT_FALSE(plan.srcDescBlob.empty()) << "the source (players) descriptor travels with the plan";
    EXPECT_EQ(decodeViewUninstall(frames[29].bytes.data(), frames[29].bytes.size()), 3);
    CatalogTopicDroppedMsg dropped;
    ASSERT_TRUE(decodeCatalogTopicDropped(frames[30].bytes.data(), frames[30].bytes.size(), dropped));
    EXPECT_EQ(dropped.topicId, 3);
    EXPECT_EQ(dropped.name, "top_players");
    int64_t off = -1;
    uint64_t digest = 0;
    ASSERT_TRUE(decodeCatalogApplied(frames.back().bytes.data(), frames.back().bytes.size(), off, digest));
    EXPECT_EQ(off, 10);
    EXPECT_EQ(digest, rig.applier.state().digest());

    // The BOOT sink of the same history posts RECOVERY installs.
    rig.restart();
    AdminWorkerAgent agent2(rig.schemas, 1, AdminDeps{});
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent2.catalogBootEffects()), 11);
    int recoveryInstalls = 0;
    for (const auto& f : drainFailureRing(agent2)) {
        if (f.type != kA2cViewPlanUpdate) continue;
        EXPECT_TRUE(decodeViewPlanUpdate(f.bytes.data(), f.bytes.size()).recovery) << "the BOOT sink: recovery installs";
        ++recoveryInstalls;
    }
    EXPECT_EQ(recoveryInstalls, 1) << "coalesced: the one live view (top_players, sink 4)";
}

TEST(CatalogApplierRegression, ProductionSinkHaltAndRingFullAreNeverDroppedRegression) {
    using namespace rune::node;
    CatalogRig rig("ap_prodfull");
    AdminWorkerAgent agent(rig.schemas, 1, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    rig.applier.goLive(agent.catalogLiveEffects());
    // Fill the admin→control ring.
    int filler = 0;
    for (const std::size_t size : {std::size_t{4096}, std::size_t{64}, std::size_t{1}}) {
        const std::vector<uint8_t> junk(size, 0xEE);
        while (agent.failureRing().write(junk.data(), junk.size(), 0x7777)) ++filler;
    }
    ASSERT_GT(filler, 0);
    rig.appendEvents(0, 3);
    rig.appendTampered(3);
    rig.applier.applyCommitted(rig.replica());
    ASSERT_TRUE(rig.applier.halted());
    EXPECT_GT(agent.catalogBacklogForTest(), 0u) << "ring full: the effects wait in the backlog";

    // Drain the filler, flush the backlog: every effect arrives, in apply order, halt last.
    std::vector<int32_t> types;
    for (int pass = 0; pass < 64 && (agent.catalogBacklogForTest() > 0 || pass == 0); ++pass) {
        for (const auto& f : drainFailureRing(agent))
            if (f.type != 0x7777) types.push_back(f.type);
        (void)agent.flushCatalogBacklogForTest();
    }
    for (const auto& f : drainFailureRing(agent)) types.push_back(f.type);
    EXPECT_EQ(agent.catalogBacklogForTest(), 0u);
    const std::vector<int32_t> expect{kA2cFilterSchema, kA2cCatalogTopicApplied, kA2cSchemaUpdate, kA2cFilterSchema,
                                      kA2cCatalogApplied,
                                      kA2cCatalogTopicApplied, kA2cCatalogApplied,
                                      kA2cCatalogTopicApplied, kA2cCatalogApplied,
                                      kA2cCatalogHalted};
    EXPECT_EQ(types, expect);
}

// ═══ CatalogCheckpoint: the file ═════════════════════════════════════════════

TEST(CatalogCheckpointRegression, RoundTripsAndAMissingFileIsNone) {
    CatalogRig rig("ck_rt");
    EXPECT_FALSE(rune::node::readCatalogCheckpoint(rig.catalogDir()).has_value());
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {41, 0xFEEDFACECAFEBEEFULL}));
    EXPECT_EQ(rune::node::readCatalogCheckpoint(rig.catalogDir()),
              (std::optional<rune::node::CatalogCheckpoint>{{41, 0xFEEDFACECAFEBEEFULL}}));
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {42, 7}));
    EXPECT_EQ(rune::node::readCatalogCheckpoint(rig.catalogDir())->appliedOffset, 42);
    EXPECT_FALSE(fs::exists(fs::path{rig.catalogDir()} / "applied.tmp")) << "the tmp file was renamed into place";
}

TEST(CatalogCheckpointRegression, ADamagedFileThrowsInsteadOfReadingAsNoneRegression) {
    // "None" would re-apply the whole history live (every effect twice) — damage is explicit.
    CatalogRig rig("ck_dmg");
    const fs::path file = fs::path{rig.catalogDir()} / rune::node::kCatalogCheckpointFile;
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {5, 9}));
    {   // flip one byte of the digest
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(17);
        f.put('\x5A');
    }
    EXPECT_THROW((void)rune::node::readCatalogCheckpoint(rig.catalogDir()), std::runtime_error);
    {   // truncated
        std::ofstream f(file, std::ios::binary | std::ios::trunc);
        f << "RCAT";
    }
    EXPECT_THROW((void)rune::node::readCatalogCheckpoint(rig.catalogDir()), std::runtime_error);
}

// ═══ Registry erase (Task 7 additions) ═══════════════════════════════════════

TEST(SchemaRegistryEraseTest, EraseForgetsEveryVersionAndOnlyItsOwnName) {
    rune::codec::DescriptorBuilder d("t");
    d.add("id", rune::codec::PrimType::Int64, 1, true);
    rune::node::SchemaRegistry reg;
    rune::node::TopicSchema ts;
    ts.topicId = 4; ts.version = 1; ts.descriptorBlob = rune::codec::serialize(d.build()); ts.topicName = "a";
    reg.applyRecord(ts);
    ts.version = 2;
    reg.applyRecord(ts);
    rune::node::TopicSchema other = ts;
    other.topicId = 5; other.version = 1; other.topicName = "b";
    reg.applyRecord(other);
    EXPECT_TRUE(reg.erase(4));
    EXPECT_EQ(reg.byId(4), nullptr);
    EXPECT_EQ(reg.byIdVersion(4, 1), nullptr);
    EXPECT_EQ(reg.byName("a"), nullptr);
    ASSERT_NE(reg.byName("b"), nullptr);
    EXPECT_FALSE(reg.erase(4)) << "unknown id: no-op";
    // A reused name re-pointed to a NEW id is not erased with the old id.
    ts.topicId = 6; ts.version = 1; ts.topicName = "b";
    reg.applyRecord(ts);
    EXPECT_TRUE(reg.erase(5));
    ASSERT_NE(reg.byName("b"), nullptr);
    EXPECT_EQ(reg.byName("b")->topicId, 6);
}

TEST(ViewRegistryEraseTest, EraseRetiresTheSlotKeepsOtherPointersAndOnlyItsOwnName) {
    rune::node::ViewRegistry views;
    views.applyRecord(rune::node::ViewRecord{3, "v", {1, 2}, 0, {}});
    views.applyRecord(rune::node::ViewRecord{5, "w", {3}, 1, {}});
    const rune::node::ViewRecord* w = views.byViewTopicId(5);
    ASSERT_NE(w, nullptr);
    EXPECT_TRUE(views.erase(3));
    EXPECT_EQ(views.byViewTopicId(3), nullptr);
    EXPECT_EQ(views.byName("v"), nullptr);
    EXPECT_EQ(views.byViewTopicId(5), w) << "the other slot's pointer stays valid";
    ASSERT_EQ(views.all().size(), 1u);
    EXPECT_EQ(views.all()[0]->viewTopicId, 5);
    EXPECT_FALSE(views.erase(3));
    // AlterView: the same name on a fresh sink, then the old one erased.
    views.applyRecord(rune::node::ViewRecord{9, "w", {4}, 0, {}});
    EXPECT_TRUE(views.erase(5));
    ASSERT_NE(views.byName("w"), nullptr);
    EXPECT_EQ(views.byName("w")->viewTopicId, 9);
}

// ═══ The kA2c catalog effect codecs ══════════════════════════════════════════

TEST(CatalogEffectWireTest, EveryMessageRoundTripsAndAMalformedFrameIsRefused) {
    using namespace rune::node;
    rune::protocol::TopicConfig c;
    c.cleanupPolicy = 2; c.retentionMs = 123; c.minCleanableDirtyRatioPpm = 250'000;
    c.coPartitionGroup = "grp"; c.partitionKey = "k";
    std::vector<uint8_t> buf(catalogTopicAppliedWireLen("topic", c));
    writeCatalogTopicApplied(buf.data(), 7, "topic", 4, 2, 1, c);
    CatalogTopicAppliedMsg a;
    ASSERT_TRUE(decodeCatalogTopicApplied(buf.data(), buf.size(), a));
    EXPECT_EQ(a.topicId, 7); EXPECT_EQ(a.name, "topic"); EXPECT_EQ(a.partitions, 4);
    EXPECT_EQ(a.replicationFactor, 2); EXPECT_EQ(a.persistence, 1); EXPECT_EQ(a.config, c);
    EXPECT_FALSE(decodeCatalogTopicApplied(buf.data(), buf.size() - 1, a)) << "short";
    buf.push_back(0);
    EXPECT_FALSE(decodeCatalogTopicApplied(buf.data(), buf.size(), a)) << "trailing bytes";

    std::vector<uint8_t> d(catalogTopicDroppedWireLen("gone"));
    writeCatalogTopicDropped(d.data(), 3, "gone");
    CatalogTopicDroppedMsg dm;
    ASSERT_TRUE(decodeCatalogTopicDropped(d.data(), d.size(), dm));
    EXPECT_EQ(dm.topicId, 3); EXPECT_EQ(dm.name, "gone");
    EXPECT_FALSE(decodeCatalogTopicDropped(d.data(), 5, dm));

    std::vector<uint8_t> cf(catalogTopicConfigWireLen(2));
    writeCatalogTopicConfigHeader(cf.data(), 1, 2);
    writeCatalogTopicConfigEntry(cf.data(), 0, 1, 60'000);
    writeCatalogTopicConfigEntry(cf.data(), 1, 4, 250'000);
    CatalogTopicConfigMsg cm;
    ASSERT_TRUE(decodeCatalogTopicConfig(cf.data(), cf.size(), cm));
    EXPECT_EQ(cm.count, 2);
    EXPECT_EQ(cm.entries[1], (CatalogConfigEntry{4, 250'000}));
    writeCatalogTopicConfigEntry(cf.data(), 1, 99, 0);   // not a ConfigKey
    EXPECT_FALSE(decodeCatalogTopicConfig(cf.data(), cf.size(), cm));

    uint8_t u[kViewUninstallWireLen];
    writeViewUninstall(u, 12);
    EXPECT_EQ(decodeViewUninstall(u, sizeof u), 12);
    EXPECT_EQ(decodeViewUninstall(u, 3), -1);

    std::vector<uint8_t> h(kCatalogHaltedFixedBytes + 6);
    writeCatalogHalted(h.data(), 17, 0xA, 0xB, "detail");
    CatalogHaltedMsg hm;
    ASSERT_TRUE(decodeCatalogHalted(h.data(), h.size(), hm));
    EXPECT_EQ(hm.offset, 17); EXPECT_EQ(hm.expected, 0xAu); EXPECT_EQ(hm.actual, 0xBu); EXPECT_EQ(hm.detail, "detail");
    EXPECT_FALSE(decodeCatalogHalted(h.data(), 10, hm));

    uint8_t ap[kCatalogAppliedWireLen];
    writeCatalogApplied(ap, 9, 0xD1);
    int64_t off = 0;
    uint64_t dg = 0;
    ASSERT_TRUE(decodeCatalogApplied(ap, sizeof ap, off, dg));
    EXPECT_EQ(off, 9); EXPECT_EQ(dg, 0xD1u);
    EXPECT_FALSE(decodeCatalogApplied(ap, 8, off, dg));
}

// ═══ Fix round 1 ═════════════════════════════════════════════════════════════

namespace {

namespace gd = rune::catalog::testing::golden_detail;

/// A serialized descriptor of `fields` fields with ~`nameLen`-byte names.
std::vector<uint8_t> wideDescriptor(const std::string& name, int fields, std::size_t nameLen, bool key = true) {
    rune::codec::DescriptorBuilder d(name);
    for (int i = 0; i < fields; ++i) {
        std::string f = "f" + std::to_string(i) + "_";
        f.resize(nameLen, 'x');
        d.add(f, rune::codec::PrimType::Int64, 1, key && i == 0);
    }
    return rune::codec::serialize(d.build());
}

/// offset 0: CreateTopic `wide` (typed, `validate`, a `fields`-field descriptor);
/// then (withView) offset 1: CreateView `wv` over it with an equally wide output.
std::vector<cat::CatalogEvent> wideScript(int fields, bool validate, bool withView) {
    gd::ScriptBuilder b;
    cat::CreateTopicOps o;
    o.partitions = 1;
    o.schema = cat::SchemaDef{.keyField = "", .validate = validate,
                              .descriptorBlob = wideDescriptor("wide", fields, 40, false),
                              .nextTag = static_cast<uint16_t>(fields)};
    b.createTopic("wide", std::move(o), "CREATE TOPIC wide (...)");
    if (withView) {
        cat::ViewDef v;
        v.sinkPartitions = 1;
        v.keyField = "f0_";
        v.keyField.resize(40, 'x');
        v.sourceNames = {"wide"};
        v.normalizedPlanBlob = {0x01, 0x00, 0x00, 0x00, 0x00};
        v.compiledPlanBlob = {0xC0, 0x00, 0x00, 0x00, 0x00};
        v.outputDescriptorBlob = wideDescriptor("wv", fields, 40, true);
        b.createView("wv", std::move(v), "CREATE VIEW wv AS SELECT * FROM wide");
    }
    return std::move(b).take();
}

} // namespace

// C1 — follower side: an oversize effect HALTS apply at its offset; nothing aborts.
TEST(CatalogApplierRegression, OversizeViewEffectHaltsAtItsOffsetAndNeverAbortsRegression) {
    using namespace rune::node;
    // ~2000 fields x ~46 B: each descriptor ~95 KB (< the 128 KiB frame), the view
    // plan carries the output AND the source descriptor: ~190 KB > 128 KiB.
    const auto events = wideScript(2000, /*validate=*/false, /*withView=*/true);
    CatalogRig rig("ap_bigview");
    AdminWorkerAgent agent(rig.schemas, 1, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(agent.catalogLiveEffects()));
    for (const auto& e : events) rig.appendEventCommitted(e);
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 1) << "the topic applied; the view halted";
    ASSERT_TRUE(rig.applier.halted()) << "an oversize effect halts — the process is still here";
    ASSERT_NE(rig.applier.haltError(), nullptr);
    EXPECT_EQ(rig.applier.haltError()->offset, 1);
    EXPECT_NE(rig.applier.haltError()->detail.find("ViewPlanUpdate"), std::string::npos) << rig.applier.haltError()->detail;
    EXPECT_EQ(agent.catalogEffectsRefusedForTest(), 0u) << "sized before posting: nothing reached the refusal backstop";
    // M1: the halted state names the staged event; nothing of it was effected or made durable.
    EXPECT_EQ(rig.applier.stagedOffset(), 1);
    EXPECT_EQ(rig.applier.lastApplied().offset, 0);
    EXPECT_NE(rig.applier.state().viewByName("wv"), nullptr) << "staged in state()";
    EXPECT_EQ(rig.views.byName("wv"), nullptr) << "never projected";
    EXPECT_EQ(rune::node::readCatalogCheckpoint(rig.catalogDir())->appliedOffset, 0) << "never checkpointed";
    // The metric frame is on the ring, after the topic's effects.
    const auto frames = drainFailureRing(agent);
    ASSERT_FALSE(frames.empty());
    EXPECT_EQ(frames.back().type, kA2cCatalogHalted);
    CatalogHaltedMsg h;
    ASSERT_TRUE(decodeCatalogHalted(frames.back().bytes.data(), frames.back().bytes.size(), h));
    EXPECT_EQ(h.offset, 1);
    // A restart halts at the same offset again (never checkpointed → re-applied live → halts).
    rig.restart();
    AdminWorkerAgent agent2(rig.schemas, 1, AdminDeps{});
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent2.catalogBootEffects()), 1);
    ASSERT_TRUE(rig.applier.goLive(agent2.catalogLiveEffects()));
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0);
    EXPECT_TRUE(rig.applier.halted());
    EXPECT_EQ(rig.applier.haltError()->offset, 1);
}

TEST(CatalogApplierRegression, OversizeValidatorHaltsAtItsOffsetAndNeverAbortsRegression) {
    using namespace rune::node;
    // ~3000 fields x ~46 B ≈ 140 KB materialized: its validator frame exceeds 128 KiB.
    const auto events = wideScript(3000, /*validate=*/true, /*withView=*/false);
    CatalogRig rig("ap_bigval");
    AdminWorkerAgent agent(rig.schemas, 1, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(agent.catalogLiveEffects()));
    rig.appendEventCommitted(events.at(0));
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0);
    ASSERT_TRUE(rig.applier.halted());
    EXPECT_EQ(rig.applier.haltError()->offset, 0);
    EXPECT_NE(rig.applier.haltError()->detail.find("SchemaUpdate"), std::string::npos) << rig.applier.haltError()->detail;
    EXPECT_EQ(rig.applier.stagedOffset(), 0);
    EXPECT_FALSE(rune::node::readCatalogCheckpoint(rig.catalogDir()).has_value());
    const auto frames = drainFailureRing(agent);
    ASSERT_EQ(frames.size(), 1u) << "only the halt: no partial effect of the event";
    EXPECT_EQ(frames[0].type, kA2cCatalogHalted);
}

// C1 — leader side (Task 9 wires it): the dry run refuses such an event BEFORE it
// is ever appended; every golden event passes.
TEST(CatalogApplierRegression, LeaderDryRunRefusesAnEventWhoseEffectsCannotBePostedRegression) {
    using namespace rune::node;
    {
        SchemaRegistry reg;
        AdminWorkerAgent agent(reg, 1, AdminDeps{});
        EXPECT_EQ(catalogEffectMaxFrame(), static_cast<std::size_t>(agent.failureRing().maxMsgLength()))
            << "the leader's gate and the follower's sink use one frame limit";
    }
    const auto view = wideScript(2000, false, true);
    cat::CatalogState s;
    ASSERT_FALSE(catalogEventEffectsRefusal(s, view[0]).has_value()) << "the wide topic alone fits";
    ASSERT_TRUE(cat::apply(s, view[0], 0).has_value());
    const auto refusal = catalogEventEffectsRefusal(s, view[1]);
    ASSERT_TRUE(refusal.has_value()) << "the oversize view is refused before it is appended";
    EXPECT_NE(refusal->find("CreateView 'wv'"), std::string::npos) << *refusal;
    EXPECT_NE(refusal->find("ViewPlanUpdate"), std::string::npos) << *refusal;
    EXPECT_EQ(s.viewByName("wv"), nullptr) << "a dry run: the caller's state is untouched";

    const auto val = wideScript(3000, true, false);
    EXPECT_TRUE(catalogEventEffectsRefusal(cat::CatalogState{}, val[0]).has_value());

    const auto golden = rune::catalog::testing::goldenScriptV1();
    cat::CatalogState g;
    for (std::size_t i = 0; i < golden.size(); ++i) {
        EXPECT_FALSE(catalogEventEffectsRefusal(g, golden[i]).has_value()) << "golden offset " << i;
        ASSERT_TRUE(cat::apply(g, golden[i], static_cast<int64_t>(i)).has_value());
    }
}

// I1 — the coalesced boot sink: a big history boots without halting, posting the FINAL state only (no
// create/drop history).  Workers start empty, so boot posts every registered version's validator: the
// volume is O(sum of versions), larger than the ring — the backlog grows and drains in order, never aborts.
TEST(CatalogApplierRegression, CoalescedBootSurvivesAHistoryLargerThanTheBacklogRegression) {
    using namespace rune::node;
    gd::ScriptBuilder b;
    {
        cat::CreateTopicOps o;
        o.schema = cat::SchemaDef{.keyField = "", .validate = true,
                                  .descriptorBlob = wideDescriptor("grow", 300, 40, false), .nextTag = 300};
        b.createTopic("grow", std::move(o), "CREATE TOPIC grow (...)");
    }
    for (int i = 0; i < 400; ++i) {
        std::string f = "added_" + std::to_string(i) + "_";
        f.resize(40, 'y');
        b.alterSchema("grow", {gd::addOp(f, rune::codec::PrimType::Int32)}, "ALTER TOPIC grow ADD ...");
    }
    const auto events = std::move(b).take();
    // The naive volume: one validator frame per schema version (what the boot now posts).
    std::size_t naive = 0;
    {
        cat::CatalogState s;
        for (std::size_t i = 0; i < events.size(); ++i) {
            ASSERT_TRUE(cat::apply(s, events[i], static_cast<int64_t>(i)).has_value());
            naive += schemaUpdateWireLen(materializedDescriptorOf(*s.topicByName("grow")).size());
        }
    }
    ASSERT_GT(naive, static_cast<std::size_t>(kAdminToControlRingBytes) * 5) << "naive volume > ring + 4x-ring backlog";

    CatalogRig rig("ap_bigboot");
    for (const auto& e : events) rig.appendEventCommitted(e);
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), static_cast<int>(events.size()));   // checkpoint at the end
    rig.restart();
    AdminWorkerAgent agent(rig.schemas, 1, AdminDeps{});
    // Nothing drains the ring during the boot replay (it runs before the admin thread).
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), static_cast<int>(events.size()));
    EXPECT_FALSE(rig.applier.halted()) << "a big history is a backlog, never a halt";
    std::vector<Frame> frames;
    for (int pass = 0; pass < 4000; ++pass) {
        for (auto& f : drainFailureRing(agent)) frames.push_back(std::move(f));
        if (agent.catalogBacklogForTest() == 0) break;
        (void)agent.flushCatalogBacklogForTest();
    }
    for (auto& f : drainFailureRing(agent)) frames.push_back(std::move(f));
    ASSERT_EQ(agent.catalogBacklogForTest(), 0u);
    ASSERT_EQ(frames.size(), 405u) << "one topic: its filter schema (before the topic, and again after the validators), "
                                      "its 401 versions' validators, one applied";
    EXPECT_EQ(frames[0].type, kA2cFilterSchema);
    EXPECT_EQ(frames[1].type, kA2cCatalogTopicApplied);
    EXPECT_EQ(frames[403].type, kA2cFilterSchema);
    EXPECT_EQ(frames.back().type, kA2cCatalogApplied);
    for (std::size_t i = 0; i < 401; ++i) {
        ASSERT_EQ(frames[2 + i].type, kA2cSchemaUpdate) << i;
        EXPECT_EQ(decodeSchemaUpdate(frames[2 + i].bytes.data(), frames[2 + i].bytes.size()).version, i + 1)
            << "ascending versions";
    }
}

// 5cc480fa made every ALTER re-post the whole history (quadratic).  A LIVE event posts exactly the version it
// created or changed; BOOT (workers start empty) posts every registered version.
TEST(CatalogApplierRegression, LiveAlterPostsOnlyTheNewVersionsValidatorRegression) {
    using namespace rune::node;
    gd::ScriptBuilder b;
    {
        cat::CreateTopicOps o;
        o.schema = cat::SchemaDef{.keyField = "", .validate = true,
                                  .descriptorBlob = wideDescriptor("vt", 4, 8, false), .nextTag = 4};
        b.createTopic("vt", std::move(o), "CREATE TOPIC vt (...)");
    }
    for (int i = 0; i < 3; ++i)
        b.alterSchema("vt", {gd::addOp("extra" + std::to_string(i), rune::codec::PrimType::Int32)}, "ALTER TOPIC vt ADD");
    const auto events = std::move(b).take();

    CatalogRig rig("ap_live_ver");
    AdminWorkerAgent agent(rig.schemas, 1, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(agent.catalogLiveEffects()));
    for (const auto& e : events) rig.appendEventCommitted(e);
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), 4);
    std::vector<uint16_t> live;
    for (const auto& f : drainFailureRing(agent))
        if (f.type == kA2cSchemaUpdate) live.push_back(decodeSchemaUpdate(f.bytes.data(), f.bytes.size()).version);
    EXPECT_EQ(live, (std::vector<uint16_t>{1, 2, 3, 4})) << "exactly one validator per event, each its own version";

    rig.restart();
    AdminWorkerAgent agent2(rig.schemas, 1, AdminDeps{});
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent2.catalogBootEffects()), 4);
    std::vector<uint16_t> boot;
    for (const auto& f : drainFailureRing(agent2))
        if (f.type == kA2cSchemaUpdate) boot.push_back(decodeSchemaUpdate(f.bytes.data(), f.bytes.size()).version);
    EXPECT_EQ(boot, (std::vector<uint16_t>{1, 2, 3, 4})) << "boot posts every registered version";
}

// Boot posts every version, so it sizes every version: an OLDER version can be larger than the current one (a
// dropped field) and must fit the frame too.
TEST(CatalogApplierRegression, TopicFramesSizeEveryVersionBootWillPostRegression) {
    using namespace rune::node;
    gd::ScriptBuilder b;
    {
        cat::CreateTopicOps o;
        o.schema = cat::SchemaDef{.keyField = "", .validate = true,
                                  .descriptorBlob = wideDescriptor("sh", 40, 40, false), .nextTag = 40};
        b.createTopic("sh", std::move(o), "CREATE TOPIC sh (...)");
    }
    std::vector<rune::codec::AlterOp> drops;
    for (int i = 0; i < 30; ++i) {
        std::string f = "f" + std::to_string(i) + "_";
        f.resize(40, 'x');
        drops.push_back(gd::dropOp(f));
    }
    b.alterSchema("sh", std::move(drops), "ALTER TOPIC sh DROP ...");
    const auto events = std::move(b).take();
    cat::CatalogState s;
    for (std::size_t i = 0; i < events.size(); ++i) ASSERT_TRUE(cat::apply(s, events[i], static_cast<int64_t>(i)).has_value());
    const cat::TopicObject& t = *s.topicByName("sh");
    ASSERT_EQ(t.schema->schemaVersion, 2);
    const std::size_t v1 = schemaUpdateWireLen(materializedDescriptorOf(t, 1).size());
    const std::size_t v2 = schemaUpdateWireLen(materializedDescriptorOf(t, 2).size());
    ASSERT_GT(v1, v2) << "the dropped fields make the older version the larger frame";
    const std::size_t cap = v1 - 1;
    ASSERT_GE(cap, catalogTopicAppliedWireLen(t.name, t.config));
    // The EVOLVING descriptor (FilterSchema frame, Phase 2c) keeps the dropped fields, so it is at least as large as v1.
    const std::size_t fsz = filterSchemaWireLen(t.schema->evolvingBlob.size());
    ASSERT_GE(fsz, v1);
    const auto live = topicEffectFramesError(t, cap, 2);
    ASSERT_TRUE(live.has_value()) << "the FilterSchema frame does not fit under this cap";
    EXPECT_EQ(live->find("SchemaUpdate"), std::string::npos) << "live posts the current validator only: it fits: " << *live;
    const auto boot = topicEffectFramesError(t, cap, 1);
    ASSERT_TRUE(boot.has_value());
    EXPECT_NE(boot->find("SchemaUpdate"), std::string::npos) << "boot posts version 1 too: it does not fit: " << *boot;
    EXPECT_FALSE(topicEffectFramesError(t, fsz, 1).has_value());
}

// Phase 2c: the FilterSchema frame carries the EVOLVING descriptor and is posted for every typed topic (validate or not).
// The leader's dry run must refuse a CREATE / ALTER whose evolving descriptor cannot fit one admin->control frame,
// before anything is appended (else every broker drops the frame after commit: silent loss for filtered clients).
TEST(CatalogApplierRegression, LeaderDryRunRefusesAFilterSchemaFrameThatCannotBePostedRegression) {
    using namespace rune::node;
    {   // CREATE: ~3000 fields, validate = false (no validator frame): only the FilterSchema frame is too large
        const auto events = wideScript(3000, /*validate=*/false, /*withView=*/false);
        const auto refusal = catalogEventEffectsRefusal(cat::CatalogState{}, events.at(0));
        ASSERT_TRUE(refusal.has_value()) << "the oversize CREATE is refused before it is appended";
        EXPECT_NE(refusal->find("FilterSchema"), std::string::npos) << *refusal;
    }
    {   // ALTER growing past the limit: the CREATE fits, the ALTER does not
        gd::ScriptBuilder b;
        cat::CreateTopicOps o;
        o.partitions = 1;
        o.schema = cat::SchemaDef{.keyField = "", .validate = false,
                                  .descriptorBlob = wideDescriptor("grow", 2500, 40, false), .nextTag = 2500};
        b.createTopic("grow", std::move(o), "CREATE TOPIC grow (...)");
        std::vector<rune::codec::AlterOp> ops;
        for (int i = 0; i < 600; ++i) {
            std::string f = "added_" + std::to_string(i) + "_";
            f.resize(40, 'y');
            ops.push_back(gd::addOp(f, rune::codec::PrimType::Int32));
        }
        b.alterSchema("grow", std::move(ops), "ALTER TOPIC grow ADD ...");
        const auto events = std::move(b).take();
        cat::CatalogState s;
        ASSERT_FALSE(catalogEventEffectsRefusal(s, events.at(0)).has_value()) << "the CREATE fits";
        ASSERT_TRUE(cat::apply(s, events.at(0), 0).has_value());
        const auto refusal = catalogEventEffectsRefusal(s, events.at(1));
        ASSERT_TRUE(refusal.has_value()) << "the ALTER that grows the evolving descriptor past the frame is refused";
        EXPECT_NE(refusal->find("FilterSchema"), std::string::npos) << *refusal;
        EXPECT_EQ(s.appliedOffset(), 0) << "a dry run: nothing applied";
    }
    // the follower's boot sizing agrees
    {
        const auto events = wideScript(3000, false, false);
        cat::CatalogState s;
        ASSERT_TRUE(cat::apply(s, events.at(0), 0).has_value());
        EXPECT_TRUE(topicEffectFramesError(*s.topicByName("wide"), catalogEffectMaxFrame(), 1).has_value());
    }
}

// I1 — the backlog never aborts: it grows (capacity kept) and drains in order.
TEST(CatalogApplierRegression, EffectBacklogGrowsInsteadOfAbortingAndDrainsInOrderRegression) {
    using namespace rune::node;
    CatalogRig rig("ap_backlog");
    AdminWorkerAgent agent(rig.schemas, 1, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(agent.catalogLiveEffects()));
    for (const std::size_t size : {std::size_t{4096}, std::size_t{64}, std::size_t{1}}) {
        const std::vector<uint8_t> junk(size, 0xEE);
        while (agent.failureRing().write(junk.data(), junk.size(), 0x7777)) {}
    }
    // ~6 MiB of backlog: 50 validated topics with ~95 KB descriptors (beyond the 4 MiB initial size).
    gd::ScriptBuilder b;
    for (int i = 0; i < 60; ++i) {
        cat::CreateTopicOps o;
        o.schema = cat::SchemaDef{.keyField = "", .validate = true,
                                  .descriptorBlob = wideDescriptor("t" + std::to_string(i), 2000, 40, false),
                                  .nextTag = 2000};
        b.createTopic("t" + std::to_string(i), std::move(o), "CREATE TOPIC");
    }
    for (const auto& e : std::move(b).take()) rig.appendEventCommitted(e);
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 60);
    EXPECT_FALSE(rig.applier.halted());
    EXPECT_GT(agent.catalogBacklogBytesForTest(), static_cast<std::size_t>(kAdminToControlRingBytes) * 4);
    std::vector<int32_t> types;
    for (int pass = 0; pass < 1000 && (agent.catalogBacklogForTest() > 0 || pass == 0); ++pass) {
        for (const auto& f : drainFailureRing(agent))
            if (f.type != 0x7777) types.push_back(f.type);
        (void)agent.flushCatalogBacklogForTest();
    }
    for (const auto& f : drainFailureRing(agent)) types.push_back(f.type);
    EXPECT_EQ(agent.catalogBacklogForTest(), 0u);
    ASSERT_EQ(types.size(), 300u);
    for (std::size_t i = 0; i < 60; ++i) {
        EXPECT_EQ(types[5 * i], kA2cFilterSchema);
        EXPECT_EQ(types[5 * i + 1], kA2cCatalogTopicApplied);
        EXPECT_EQ(types[5 * i + 2], kA2cSchemaUpdate);
        EXPECT_EQ(types[5 * i + 3], kA2cFilterSchema);
        EXPECT_EQ(types[5 * i + 4], kA2cCatalogApplied);
    }
}

// I2 — no progress below the HW is an explicit halt, one cause each.
namespace {
void expectReadHalt(const char* tag, rune::storage::ReadIntoStatus status, std::vector<uint8_t> bytes, int count,
                    const std::string& cause) {
    CatalogRig rig(tag);
    rig.appendEvents(0, 2);
    rig.applier.injectReadForTest(static_cast<int>(status), std::move(bytes), count);
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0);
    ASSERT_TRUE(rig.applier.halted()) << cause << ": a stall below the HW must halt, not retry silently";
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 0);
    EXPECT_NE(rig.rec.halts[0].detail.find("cannot read committed offset 0"), std::string::npos) << rig.rec.halts[0].detail;
    EXPECT_NE(rig.rec.halts[0].detail.find(cause), std::string::npos) << rig.rec.halts[0].detail;
}
} // namespace

TEST(CatalogApplierRegression, AbsentReadBelowTheHighWatermarkHaltsRegression) {
    expectReadHalt("ap_absent", rune::storage::ReadIntoStatus::Absent, {}, 1, "Absent");
}
TEST(CatalogApplierRegression, EmptyReadBelowTheHighWatermarkHaltsRegression) {
    expectReadHalt("ap_empty", rune::storage::ReadIntoStatus::Ok, {}, 1, "empty");
}
TEST(CatalogApplierRegression, ShortReadBelowTheHighWatermarkHaltsRegression) {
    expectReadHalt("ap_shortread", rune::storage::ReadIntoStatus::Ok, std::vector<uint8_t>(10, 0), 1, "short");
}
TEST(CatalogApplierRegression, OversizeBatchPastTheBufferCapHaltsRegression) {
    expectReadHalt("ap_oversize", rune::storage::ReadIntoStatus::FirstTooLarge, {}, -1, "oversize");
}
TEST(CatalogApplierRegression, ALargeBatchGrowsTheReadBufferOnceNotEveryPassRegression) {
    CatalogRig rig("ap_grow");
    rig.appendEvents(0, 2);
    EXPECT_EQ(rig.applier.readBufferBytesForTest(), rune::node::kCatalogReadChunk);
    rig.applier.injectReadForTest(static_cast<int>(rune::storage::ReadIntoStatus::FirstTooLarge), {}, 1);
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 2);
    EXPECT_FALSE(rig.applier.halted());
    EXPECT_EQ(rig.applier.readBufferBytesForTest(), 2 * rune::node::kCatalogReadChunk) << "grown once, capacity kept";
    rig.appendEvents(2, 4);
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 2);
    EXPECT_EQ(rig.applier.readBufferBytesForTest(), 2 * rune::node::kCatalogReadChunk) << "no regrowth per pass";
}

// I3 — gaps, from hand-built batches appended straight to the Log (Log::append
// does not check contiguity), re-committed by the restart's follower ack.
namespace {
std::vector<uint8_t> handBuiltBatch(int64_t baseOffset, const std::vector<uint8_t>& value, bool control) {
    rune::protocol::RecordBatchBuilder bb(baseOffset, 1'760'000'000'000);
    bb.setControl(control);
    bb.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(value.data()), value.size()),
              1'760'000'000'000);
    std::vector<uint8_t> out;
    bb.buildInto(out);
    return out;
}
} // namespace

TEST(CatalogApplierRegression, GapInTheLogHaltsAtTheMissingOffsetRegression) {
    CatalogRig rig("ap_gap");
    rig.appendEvents(0, 3);                                         // offsets 0..2, LEO 3
    const auto batch = handBuiltBatch(4, CatalogRig::encodeEvent(rig.golden()[3]), /*control=*/false);
    rig.log().append(batch, /*lastOffset=*/4);                      // offset 3 never written
    rig.restart();
    ASSERT_EQ(rig.replica().highWatermark(), 5);
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 3);
    ASSERT_TRUE(rig.applier.halted());
    ASSERT_EQ(rig.rec.halts.size(), 1u);
    EXPECT_EQ(rig.rec.halts[0].offset, 3) << "halts at the MISSING offset";
    EXPECT_NE(rig.rec.halts[0].detail.find("a gap in __catalog"), std::string::npos) << rig.rec.halts[0].detail;
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2})) << "offset 4 is never applied past the gap";
}

TEST(CatalogApplierRegression, ControlBatchPastAGapHaltsRegression) {
    CatalogRig rig("ap_ctlgap");
    rig.appendEvents(0, 3);                                         // LEO 3
    rig.log().append(handBuiltBatch(4, {0x00, 0x00, 0x00, 0x00}, /*control=*/true), /*lastOffset=*/4);
    const auto ev = handBuiltBatch(5, CatalogRig::encodeEvent(rig.golden()[3]), /*control=*/false);
    rig.log().append(ev, /*lastOffset=*/5);
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 3);
    ASSERT_TRUE(rig.applier.halted()) << "a control batch must never carry nextOffset over a gap";
    EXPECT_EQ(rig.rec.halts.at(0).offset, 3);
    EXPECT_NE(rig.rec.halts.at(0).detail.find("control batch"), std::string::npos) << rig.rec.halts.at(0).detail;
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2}));
}

TEST(CatalogApplierRegression, ContiguousControlBatchIsConsumedNotAGap) {
    CatalogRig rig("ap_ctlok");
    rig.appendEvents(0, 3);
    rig.log().append(handBuiltBatch(3, {0x00, 0x00, 0x00, 0x00}, /*control=*/true), /*lastOffset=*/3);
    rig.log().append(handBuiltBatch(4, CatalogRig::encodeEvent(rig.golden()[3]), false), /*lastOffset=*/4);
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0);
    ASSERT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 4);
    EXPECT_FALSE(rig.applier.halted());
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2, 4}));
}

// M2 — AwaitingBoot → Live only for an empty catalog.
TEST(CatalogApplierRegression, GoLiveWithoutTheBootReplayIsRefusedWhenACheckpointExistsRegression) {
    CatalogRig rig("ap_golive");
    rig.appendEvents(0, 3);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    EXPECT_FALSE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_EQ(rig.applier.phaseName(), "AwaitingBoot");
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 0) << "nothing re-applied live";
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 3);
    EXPECT_TRUE(rig.applier.goLive(rig.rec.liveEffects()));
    EXPECT_FALSE(rig.applier.goLive(rig.rec.liveEffects())) << "already Live";
}

// ═══ Catalog history Task 16: the startup boot ═══════════════════════════════

// The broker's startup phase replays over the catalog LOG alone (no replica or transport exists yet):
// the same state, the same digest, the same one-replay rule as the replica form.
TEST(CatalogApplierRegression, ReplayLocalOverTheLogAloneBootsTheSameStateAsTheReplicaFormRegression) {
    CatalogRig rig("ap_logform");
    rig.appendEvents(0, 10);
    rig.applier.applyCommitted(rig.replica());
    const uint64_t digest = rig.applier.state().digest();
    rig.restart();
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::CatalogApplier boot(rig.catalogDir(), schemas, views);
    EXPECT_EQ(boot.replayLocal(rig.log(), rig.rec.bootEffects()), 10);
    EXPECT_EQ(boot.phaseName(), "Booted");
    EXPECT_EQ(boot.state().digest(), digest);
    EXPECT_NE(schemas.byName("players"), nullptr) << "the registries are the applier's projections";
    EXPECT_EQ(boot.replayLocal(rig.log(), rig.rec.bootEffects()), -1) << "exactly one boot replay";
}

TEST(CatalogApplierRegression, ReplayLocalOverTheLogRefusesACheckpointAheadOfTheLogRegression) {
    CatalogRig rig("ap_logahead");
    rig.appendEvents(0, 4);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {rig.log().logEndOffset() + 2, 0x1234}));
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::CatalogApplier boot(rig.catalogDir(), schemas, views);
    try {
        (void)boot.replayLocal(rig.log(), rig.rec.bootEffects());
        FAIL() << "a checkpoint ahead of the log must refuse to boot";
    } catch (const std::runtime_error& ex) {
        const std::string what = ex.what();
        EXPECT_NE(what.find("offset " + std::to_string(rig.log().logEndOffset() + 2)), std::string::npos) << what;
        EXPECT_NE(what.find("ends at " + std::to_string(rig.log().logEndOffset())), std::string::npos) << what;
    }
}

// The boot sink the BROKER passes collects the topics; everything that needs a running broker is
// posted afterwards from the applied state by postBootStash — validators, view sink gate + recovery
// install, the metric — and never a topic create / drop.
TEST(CatalogApplierRegression, PostBootStashPostsOnlyWhatNeedsARunningBrokerRegression) {
    CatalogRig rig("ap_stash");
    rig.appendEvents(0, rig.golden().size());
    rig.applier.applyCommitted(rig.replica());
    ASSERT_FALSE(rig.applier.halted());
    const uint64_t digest = rig.applier.state().digest();
    const int64_t  last   = rig.applier.lastApplied().offset;
    rig.restart();
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::CatalogApplier boot(rig.catalogDir(), schemas, views);
    std::vector<int32_t> topics, sinks;
    rune::node::CatalogEffects collect;
    collect.topicApplied = [&](const cat::TopicObject& t) { topics.push_back(t.topicId); };
    collect.sinkApplied  = [&](const rune::node::SinkSpec& s) { sinks.push_back(s.topicId); };
    ASSERT_GT(boot.replayLocal(rig.log(), rune::node::BootEffects{std::move(collect)}), 0);
    EXPECT_EQ(topics.size(), boot.state().topics().size());
    EXPECT_EQ(sinks.size(), boot.state().views().size());
    EXPECT_TRUE(rig.rec.sequence.empty()) << "the boot sink given to replayLocal is the only one that ran";

    boot.postBootStash(rig.rec.bootEffects().sink);
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{last}));
    EXPECT_EQ(rig.rec.appliedDigests, (std::vector<uint64_t>{digest}));
    EXPECT_TRUE(rig.rec.halts.empty());
    EXPECT_FALSE(rig.rec.bootViewInstalls.empty()) << "every live view reinstalls through the BOOT sink";
    std::size_t sinkPosts = 0, validatorPosts = 0;
    for (const std::string& s : rig.rec.sequence) {
        validatorPosts += s.rfind("schemaChanged:", 0) == 0 ? 1 : 0;
        EXPECT_NE(s.rfind("topicApplied:", 0), 0u) << s << ": topics are the startup phase's";
        EXPECT_NE(s.rfind("topicDropped:", 0), 0u) << s << ": a dropped object posts nothing";
        sinkPosts += s.rfind("sinkApplied:", 0) == 0 ? 1 : 0;
    }
    EXPECT_EQ(sinkPosts, boot.state().views().size());
    std::size_t typed = 0;
    for (const auto* t : boot.state().topics()) typed += t->schema ? 1 : 0;
    ASSERT_GT(typed, 0u) << "the golden catalog holds typed topics";
    EXPECT_EQ(validatorPosts, typed) << "one validator push per typed topic (its current schema version)";
}

TEST(CatalogApplierRegression, PostBootStashDeliversTheHaltOfAHaltedBootRegression) {
    CatalogRig rig("ap_stash_halt");
    rig.appendEvents(0, 5);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {4, 0xBAD}));
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::CatalogApplier boot(rig.catalogDir(), schemas, views);
    (void)boot.replayLocal(rig.log(), rune::node::BootEffects{});
    ASSERT_TRUE(boot.halted());
    boot.postBootStash(rig.rec.bootEffects().sink);
    ASSERT_EQ(rig.rec.halts.size(), 1u) << "the control thread reports the halt once the broker runs";
    EXPECT_EQ(rig.rec.halts[0].code, cat::ApplyError::Code::FingerprintMismatch);
}

// B3: the dropped ids are the complement of the live ids below nextTopicId — a tombstone records the
// object, not its topic id, and an AlterView-retired sink has no tombstone at all.
TEST(CatalogApplierRegression, DroppedTopicIdsAreEveryAssignedIdNoLiveTopicOrSinkHoldsRegression) {
    CatalogRig rig("ap_dropped");
    rig.appendEvents(0, rig.golden().size());
    rig.applier.applyCommitted(rig.replica());
    ASSERT_FALSE(rig.applier.halted());
    const cat::CatalogState& s = rig.applier.state();
    ASSERT_FALSE(s.tombstones().empty()) << "the golden script drops topics and views";
    const std::vector<int32_t> dropped = rune::node::droppedTopicIds(s);
    std::set<int32_t> live;
    for (const auto* t : s.topics()) live.insert(t->topicId);
    for (const auto* v : s.views()) live.insert(v->def.viewTopicId);
    EXPECT_EQ(dropped.size() + live.size(), static_cast<std::size_t>(s.nextTopicId()));
    EXPECT_TRUE(std::is_sorted(dropped.begin(), dropped.end()));
    for (const int32_t id : dropped) {
        EXPECT_GE(id, 0);
        EXPECT_LT(id, s.nextTopicId());
        EXPECT_EQ(live.count(id), 0u) << id << " is live";
    }
    EXPECT_TRUE(rune::node::droppedTopicIds(cat::CatalogState{}).empty()) << "an empty catalog";
}

// F4: the admin thread never replays — it adopts the applier the startup phase booted, and its FIRST pass
// makes it Live.  An applier that did not boot is a refusal to start, never a second replay.
TEST(CatalogApplierRegression, AdminWorkerAdoptsOnlyABootedApplierAndGoesLiveOnItsFirstPassRegression) {
    CatalogRig rig("ap_adopt");
    rig.appendEvents(0, 3);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::AdminDeps deps;
    rune::node::AdminWorker worker(schemas, 0, deps, &views);

    rune::node::CatalogApplier unbooted(rig.catalogDir(), schemas, views);
    EXPECT_THROW(worker.adoptBootedApplier(std::move(unbooted), rune::node::LiveEffects{}), std::runtime_error)
        << "an AwaitingBoot applier would replay on the admin thread";

    rune::node::CatalogApplier booted(rig.catalogDir(), schemas, views);
    ASSERT_EQ(booted.replayLocal(rig.log(), rune::node::BootEffects{}), 3);
    ASSERT_EQ(booted.phaseName(), "Booted");
    worker.adoptBootedApplier(std::move(booted), rig.rec.liveEffects());
    EXPECT_EQ(worker.applier().phaseName(), "Booted") << "still Booted until the admin thread's first pass";
    EXPECT_EQ(worker.applier().nextOffset(), 3);
    (void)worker.doWork(1'000'000);
    EXPECT_EQ(worker.applier().phaseName(), "Live");
    EXPECT_EQ(worker.applier().nextOffset(), 3) << "nothing replayed";
    EXPECT_TRUE(rig.rec.sequence.empty()) << "and no effect re-posted";
}

// R4 (fix round 1): postBootStash posts exactly what the boot accepted — an object the replay refused as oversize
// (it halted on it) is skipped here, never posted as a sink / install the control thread would build.
TEST(CatalogApplierRegression, PostBootStashSkipsObjectsTheBootRefusedAsOversizeRegression) {
    CatalogRig rig("ap_stash_big");
    rig.appendEvents(0, rig.golden().size());
    rig.applier.applyCommitted(rig.replica());
    ASSERT_FALSE(rig.applier.halted());
    rig.restart();
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::CatalogApplier boot(rig.catalogDir(), schemas, views);
    ASSERT_GT(boot.replayLocal(rig.log(), rune::node::BootEffects{}), 0);
    ASSERT_FALSE(boot.state().views().empty());
    // Per object (topic or view), the smallest frame cap it fits; pick a cap just below the largest so SOME objects
    // fit and not all (the golden catalog's objects differ in frame size).
    std::size_t biggest = 0;
    std::vector<std::size_t> topicMin, viewMin;
    for (const auto* t : boot.state().topics()) {
        std::size_t cap = 1;
        while (cap < (std::size_t{1} << 20) && rune::node::topicEffectFramesError(*t, cap, 1)) ++cap;
        topicMin.push_back(cap);
        biggest = std::max(biggest, cap);
    }
    for (const auto* v : boot.state().views()) {
        std::size_t cap = 1;
        while (cap < (std::size_t{1} << 20) && rune::node::viewEffectFramesError(*v, boot.state(), cap)) ++cap;
        viewMin.push_back(cap);
        biggest = std::max(biggest, cap);
    }
    const std::size_t cap = biggest - 1;
    std::size_t fitTopics = 0, fitViews = 0;
    for (const std::size_t m : topicMin) fitTopics += m <= cap ? 1 : 0;
    for (const std::size_t m : viewMin) fitViews += m <= cap ? 1 : 0;
    ASSERT_LT(fitTopics + fitViews, topicMin.size() + viewMin.size()) << "something is oversize at this cap";
    ASSERT_GT(fitTopics + fitViews, 0u) << "and something fits";
    std::size_t sinks = 0, installs = 0, validators = 0;
    rune::node::CatalogEffects fx;
    fx.maxFrameBytes = cap;
    fx.sinkApplied    = [&](const rune::node::SinkSpec&) { ++sinks; };
    fx.viewInstalled  = [&](const cat::ViewObject&) { ++installs; };
    fx.schemaChanged  = [&](const cat::TopicObject&, uint16_t) { ++validators; };
    boot.postBootStash(fx);
    std::size_t typedFitting = 0;
    std::size_t i = 0;
    for (const auto* t : boot.state().topics()) typedFitting += (topicMin[i++] <= cap && t->schema) ? 1 : 0;
    EXPECT_EQ(sinks, fitViews) << "only the views whose frames fit the sink";
    EXPECT_EQ(installs, fitViews);
    EXPECT_EQ(validators, typedFitting) << "and only the topics whose frames fit";
}

// R5 (fix round 1): the boot sink serves the replay only; its captures are released when the replay ends.
TEST(CatalogApplierRegression, BootSinkIsReleasedWhenTheReplayEndsRegression) {
    CatalogRig rig("ap_release");
    rig.appendEvents(0, 4);
    rig.applier.applyCommitted(rig.replica());
    rig.restart();
    rune::node::SchemaRegistry schemas;
    rune::node::ViewRegistry views;
    rune::node::CatalogApplier boot(rig.catalogDir(), schemas, views);
    auto token = std::make_shared<int>(7);
    auto bootEffects = [&token] {   // a prvalue: no moved-from std::function (libc++ keeps a copy) outlives the call
        rune::node::CatalogEffects fx;
        fx.topicApplied = [token](const cat::TopicObject&) {};
        return rune::node::BootEffects{std::move(fx)};
    };
    ASSERT_GT(boot.replayLocal(rig.log(), bootEffects()), 0);
    EXPECT_EQ(token.use_count(), 1) << "the Booted phase must not keep the caller's captures alive";
    // and the no-checkpoint path
    CatalogRig empty("ap_release_empty");
    rune::node::SchemaRegistry s2;
    rune::node::ViewRegistry v2;
    rune::node::CatalogApplier boot2(empty.catalogDir(), s2, v2);
    ASSERT_EQ(boot2.replayLocal(empty.log(), bootEffects()), 0);
    EXPECT_EQ(token.use_count(), 1);
}
