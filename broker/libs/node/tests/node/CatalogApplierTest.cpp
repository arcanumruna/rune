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
#include <rune/node/BrokerNode.h>   // AdminWorkerAgent (the production sinks)
#include <rune/node/CatalogApplier.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
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

TEST(CatalogApplierRegression, RestartNeverAppliesAnEventTwiceNorSkipsOneRegression) {
    CatalogRig rig("ap_crash");
    rig.appendEvents(0, 5);
    rig.applier.applyCommitted(rig.replica());
    const std::vector<uint64_t> digests = rig.rec.appliedDigests;   // digest after each offset 0..4
    ASSERT_EQ(digests.size(), 5u);
    const auto ck = rune::node::readCatalogCheckpoint(rig.catalogDir());
    ASSERT_TRUE(ck.has_value());
    EXPECT_EQ(*ck, (rune::node::CatalogCheckpoint{4, digests[4]})) << "one checkpoint per applied event";

    // (a) Crash AFTER offset 4's checkpoint, before its effects: boot re-runs them
    //     through the BOOT sink; live resumes at 5 — offset 4 is never applied live again.
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 5);
    rig.applier.goLive(rig.rec.liveEffects());
    rig.appendEvents(5, 8);
    const std::size_t bootCount = rig.rec.appliedOffsets.size();
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 3);
    EXPECT_EQ(std::vector<int64_t>(rig.rec.appliedOffsets.begin() + static_cast<std::ptrdiff_t>(bootCount),
                                   rig.rec.appliedOffsets.end()),
              (std::vector<int64_t>{5, 6, 7}));

    // (b) Crash BEFORE offset 4's checkpoint (the checkpoint still names 3): boot
    //     replays [0, 3]; live applies 4..7 — offset 4 is not skipped.
    rig.restart();
    ASSERT_TRUE(rune::node::writeCatalogCheckpoint(rig.catalogDir(), {3, digests[3]}));
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 4);
    rig.applier.goLive(rig.rec.liveEffects());
    EXPECT_EQ(rig.applier.applyCommitted(rig.replica()), 4);
    EXPECT_EQ(rig.rec.appliedOffsets, (std::vector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7})) << "each offset exactly once";
    EXPECT_FALSE(rig.applier.halted());
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
    EXPECT_EQ(rig.rec.bootViewInstalls, (std::vector<int32_t>{3, 4, 5, 8, 9}));
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
    AdminWorkerAgent agent(rig.schemas, /*replica=*/nullptr, /*selfId=*/1, {1}, AdminDeps{});
    rig.restart();
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent.catalogBootEffects()), 0);
    rig.applier.goLive(agent.catalogLiveEffects());
    rig.appendEvents(0, 11);   // through AlterView top_players
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), 11);
    const auto frames = drainFailureRing(agent);
    // 0 CreateTopic players (typed, validate) → applied + validator; 1, 2 CreateTopic untyped;
    // 3 CreateSchema events (validate false) → no validator; 4, 5 AlterSchema players → validator;
    // 6 SetCompatibility players → validator; 7, 8 AlterTopicConfig events; 9 CreateView;
    // 10 AlterView: new sink, new plan, old plan out, old sink dropped.
    const std::vector<int32_t> expect{
        kA2cCatalogTopicApplied, kA2cSchemaUpdate, kA2cCatalogApplied,    // 0
        kA2cCatalogTopicApplied, kA2cCatalogApplied,                      // 1
        kA2cCatalogTopicApplied, kA2cCatalogApplied,                      // 2
        kA2cCatalogApplied,                                               // 3
        kA2cSchemaUpdate, kA2cCatalogApplied,                             // 4
        kA2cSchemaUpdate, kA2cCatalogApplied,                             // 5
        kA2cSchemaUpdate, kA2cCatalogApplied,                             // 6
        kA2cCatalogTopicConfig, kA2cCatalogApplied,                       // 7
        kA2cCatalogTopicConfig, kA2cCatalogApplied,                       // 8
        kA2cCatalogTopicApplied, kA2cViewPlanUpdate, kA2cCatalogApplied,  // 9
        kA2cCatalogTopicApplied, kA2cViewPlanUpdate, kA2cViewUninstall, kA2cCatalogTopicDropped,
        kA2cCatalogApplied,                                               // 10
    };
    ASSERT_EQ(typesOf(frames), expect);

    CatalogTopicAppliedMsg players;
    ASSERT_TRUE(decodeCatalogTopicApplied(frames[0].bytes.data(), frames[0].bytes.size(), players));
    EXPECT_EQ(players.topicId, 0);
    EXPECT_EQ(players.name, "players");
    EXPECT_EQ(players.partitions, 8);
    EXPECT_EQ(players.replicationFactor, 3);
    EXPECT_EQ(players.config, std::get<cat::CreateTopicOps>(rig.golden()[0].ops).config) << "the exact TopicConfig";

    CatalogTopicConfigMsg cfg;
    ASSERT_TRUE(decodeCatalogTopicConfig(frames[14].bytes.data(), frames[14].bytes.size(), cfg));
    EXPECT_EQ(cfg.topicId, 1);
    ASSERT_EQ(cfg.count, 1);
    EXPECT_EQ(cfg.entries[0], (CatalogConfigEntry{static_cast<uint8_t>(cat::ConfigKey::RetentionMs), 60'000}));

    CatalogTopicAppliedMsg sink;
    ASSERT_TRUE(decodeCatalogTopicApplied(frames[18].bytes.data(), frames[18].bytes.size(), sink));
    EXPECT_EQ(sink.topicId, 3);
    EXPECT_EQ(sink.name, "top_players");
    EXPECT_EQ(sink.partitions, 2);
    EXPECT_EQ(sink.replicationFactor, 0) << "a view sink lives on every broker";
    EXPECT_EQ(sink.config.cleanupPolicy, 1) << "a view sink is a keyed, compacted table";
    EXPECT_EQ(sink.config.partitionKey, "id");

    const ViewPlanUpdateMsg plan = decodeViewPlanUpdate(frames[19].bytes.data(), frames[19].bytes.size());
    EXPECT_EQ(plan.viewTopicId, 3);
    EXPECT_FALSE(plan.recovery) << "the LIVE sink posts commit installs";
    EXPECT_FALSE(plan.srcDescBlob.empty()) << "the source (players) descriptor travels with the plan";
    EXPECT_EQ(decodeViewUninstall(frames[23].bytes.data(), frames[23].bytes.size()), 3);
    CatalogTopicDroppedMsg dropped;
    ASSERT_TRUE(decodeCatalogTopicDropped(frames[24].bytes.data(), frames[24].bytes.size(), dropped));
    EXPECT_EQ(dropped.topicId, 3);
    EXPECT_EQ(dropped.name, "top_players");
    int64_t off = -1;
    uint64_t digest = 0;
    ASSERT_TRUE(decodeCatalogApplied(frames.back().bytes.data(), frames.back().bytes.size(), off, digest));
    EXPECT_EQ(off, 10);
    EXPECT_EQ(digest, rig.applier.state().digest());

    // The BOOT sink of the same history posts RECOVERY installs.
    rig.restart();
    AdminWorkerAgent agent2(rig.schemas, nullptr, 1, {1}, AdminDeps{});
    ASSERT_EQ(rig.applier.replayLocal(rig.replica(), agent2.catalogBootEffects()), 11);
    int recoveryInstalls = 0;
    for (const auto& f : drainFailureRing(agent2)) {
        if (f.type != kA2cViewPlanUpdate) continue;
        EXPECT_TRUE(decodeViewPlanUpdate(f.bytes.data(), f.bytes.size()).recovery) << "the BOOT sink: recovery installs";
        ++recoveryInstalls;
    }
    EXPECT_EQ(recoveryInstalls, 2);
}

TEST(CatalogApplierRegression, ProductionSinkHaltAndRingFullAreNeverDroppedRegression) {
    using namespace rune::node;
    CatalogRig rig("ap_prodfull");
    AdminWorkerAgent agent(rig.schemas, nullptr, 1, {1}, AdminDeps{});
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
    const std::vector<int32_t> expect{kA2cCatalogTopicApplied, kA2cSchemaUpdate, kA2cCatalogApplied,
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
