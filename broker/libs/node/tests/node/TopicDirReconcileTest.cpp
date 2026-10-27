// TopicDirReconcileTest.cpp — catalog history Task 16: boot's topic-dir reconcile against
// the BOOTED catalog (reconcileTopicDirs), on real files.  The function never deletes a
// topic dir (it only quarantines), purges only the dropped/ entries the catalog marks
// dropped, claims names exactly or by ASCII case fold, and never follows a symlink.
//
// Ported from the retired RuntimeTopicRecoveryTest's reconcile cases (the marker /
// placeholder / damage-latch machinery died with the module); every test here is named
// *Regression so the suite runs with the checkpoint gate.
#include <rune/gateway/TopicDirMarker.h>
#include <rune/node/NamespacePaths.h>
#include <rune/node/TopicDirReconcile.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;
using rune::node::TopicSpec;

namespace {

struct TempDataDir {
    fs::path path;
    explicit TempDataDir(const std::string& tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_tdr_" + tag + "_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(seq++));
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TempDataDir() { std::error_code ec; fs::remove_all(path, ec); }
};

void touch(const fs::path& p, const std::string& content = "x") {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary) << content;
}

/// A catalog topic's dir as the registry makes it: the topic-id marker names the incarnation.
void mark(const fs::path& topicDir, int32_t id) {
    fs::create_directories(topicDir);
    ASSERT_TRUE(rune::gateway::writeTopicIdMarker(topicDir, id));
}

TopicSpec spec(const std::string& name, int32_t id) {
    TopicSpec s;
    s.name = name; s.topicId = id; s.partitionCount = 1;
    return s;
}

constexpr int64_t kNow = 1'700'000'000;

bool quarantineHas(const fs::path& dataDir, const std::string& prefix) {
    std::error_code ec;
    for (fs::directory_iterator it(rune::node::quarantineRoot(dataDir, 0), ec), end; !ec && it != end; it.increment(ec))
        if (it->path().filename().string().rfind(prefix, 0) == 0) return true;
    return false;
}
const std::vector<int32_t> kNoDrops;

} // namespace

TEST(TopicDirReconcileRegression, UnclaimedTopicDirIsQuarantinedNeverDeletedRegression) {
    TempDataDir d("quar");
    touch(rune::node::topicsRoot(d.path, 0) / "live" / "0" / "seg.log");
    mark(rune::node::topicsRoot(d.path, 0) / "live", 0);
    touch(rune::node::topicsRoot(d.path, 0) / "ghost" / "0" / "seg.log", "precious");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 0)}, kNoDrops, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(r.purged.empty());
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "ghost"));
    EXPECT_EQ(r.quarantined[0], rune::node::quarantineRoot(d.path, 0) / ("ghost-" + std::to_string(kNow)));
    std::ifstream in(r.quarantined[0] / "0" / "seg.log");
    std::string back; in >> back;
    EXPECT_EQ(back, "precious") << "the data is moved intact, never deleted";
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "live" / "0" / "seg.log")) << "a live topic's dir is not touched";
}

// B8: a live topic whose dir differs only in ASCII case is claimed (on a case-insensitive
// filesystem it IS the live topic's dir) — NOT quarantined.
TEST(TopicDirReconcileRegression, CaseVariantOfALiveNameIsNotQuarantinedRegression) {
    TempDataDir d("case");
    touch(rune::node::topicsRoot(d.path, 0) / "Orders" / "0" / "seg.log");
    mark(rune::node::topicsRoot(d.path, 0) / "Orders", 0);
    touch(rune::node::topicsRoot(d.path, 0) / "EVENTS" / "0" / "seg.log");
    mark(rune::node::topicsRoot(d.path, 0) / "EVENTS", 1);
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0), spec("events", 1)}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "Orders" / "0" / "seg.log"));
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "EVENTS" / "0" / "seg.log"));
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(d.path, 0)));
}

// B8: the ASCII fold is ASCII only — a non-letter difference is a different name.
TEST(TopicDirReconcileRegression, OnlyAsciiCaseFoldsClaimADirRegression) {
    TempDataDir d("fold");
    touch(rune::node::topicsRoot(d.path, 0) / "orders_2" / "0" / "seg.log");
    touch(rune::node::topicsRoot(d.path, 0) / "order" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0)}, kNoDrops, kNow);
    EXPECT_EQ(r.quarantined.size(), 2u);
}

// R1.3 step 0 (fix round 1): an OLDER incarnation's dir under a re-created name goes to dropped/ and is purged
// in the same run; the live topic starts empty (createTopicAt then creates a fresh dir).
TEST(TopicDirReconcileRegression, OlderIncarnationMarkerGoesToDroppedAndIsPurgedRegression) {
    TempDataDir d("older");
    touch(rune::node::topicsRoot(d.path, 0) / "t" / "0" / "seg.log", "old life");
    mark(rune::node::topicsRoot(d.path, 0) / "t", 1);
    const std::vector<int32_t> dropped = {1};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("t", 4)}, dropped, kNow);
    ASSERT_EQ(r.retired.size(), 1u);
    EXPECT_EQ(r.retired[0], rune::node::droppedRoot(d.path, 0) / "1.t");
    ASSERT_EQ(r.purged.size(), 1u);
    EXPECT_EQ(r.purged[0], rune::node::droppedRoot(d.path, 0) / "1.t") << "purged in the same run";
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "t")) << "the live topic starts empty";
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0) / "1.t"));
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(d.path, 0)));
}

// F4 (fix round 2): a dir carrying a Valid marker is a catalog topic dir even without a partition dir.
TEST(TopicDirReconcileRegression, MarkerOnlyDirOfADroppedIdIsRetiredRegression) {
    TempDataDir d("markeronly");
    mark(rune::node::topicsRoot(d.path, 0) / "t", 2);   // no partition dir
    const std::vector<int32_t> dropped = {2};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("t", 4)}, dropped, kNow);
    ASSERT_EQ(r.retired.size(), 1u);
    EXPECT_EQ(r.retired[0], rune::node::droppedRoot(d.path, 0) / "2.t");
    ASSERT_EQ(r.purged.size(), 1u);
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "t"));
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(d.path, 0)));
}

TEST(TopicDirReconcileRegression, MarkerNamingANonDroppedIdOrUnattributedDataIsQuarantinedRegression) {
    TempDataDir d("unattributed");
    // a marker naming an id that is neither the claim nor dropped
    touch(rune::node::topicsRoot(d.path, 0) / "a" / "0" / "seg.log", "x");
    mark(rune::node::topicsRoot(d.path, 0) / "a", 9);
    // markerless + data, claimed by a CATALOG topic
    touch(rune::node::topicsRoot(d.path, 0) / "b" / "0" / "seg.log", "x");
    // markerless + empty: kept (createTopicAt adopts it)
    fs::create_directories(rune::node::topicsRoot(d.path, 0) / "c" / "0");
    touch(rune::node::topicsRoot(d.path, 0) / "c" / "0" / "seg.log", "");
    // a fold-claimed dir carrying a dropped id's marker
    touch(rune::node::topicsRoot(d.path, 0) / "T" / "0" / "seg.log", "x");
    mark(rune::node::topicsRoot(d.path, 0) / "T", 3);
    const std::vector<int32_t> dropped = {3};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(),
                                                  {spec("a", 5), spec("b", 6), spec("c", 7), spec("t", 8)}, dropped,
                                                  kNow);
    EXPECT_TRUE(quarantineHas(d.path, "a-")) << "marker 9 is neither the claim (5) nor dropped";
    EXPECT_TRUE(quarantineHas(d.path, "b-")) << "unattributed data";
    EXPECT_FALSE(quarantineHas(d.path, "c-"));
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "c"));
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "T"));
    ASSERT_EQ(r.retired.size(), 1u);
    EXPECT_EQ(r.retired[0], rune::node::droppedRoot(d.path, 0) / "3.T");
}

// Task 20 fix round 1 (I3): a crash between applying a DROP and retiring its dir leaves an UNCLAIMED dir whose
// marker names the dropped id.  It is retired and purged like a claimed one - never quarantined forever.
TEST(TopicDirReconcileRegression, UnclaimedDirOfADroppedIdIsRetiredAndPurgedNeverQuarantinedRegression) {
    TempDataDir d("unclaimeddropped");
    touch(rune::node::topicsRoot(d.path, 0) / "t" / "0" / "seg.log", "dropped life");
    mark(rune::node::topicsRoot(d.path, 0) / "t", 3);
    touch(rune::node::topicsRoot(d.path, 0) / "m" / "topic-id.placeholder");   // not a topic dir at all: left alone
    mark(rune::node::topicsRoot(d.path, 0) / "marker", 3);                      // marker only, no partition dir
    const std::vector<int32_t> dropped = {3};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    ASSERT_EQ(r.retired.size(), 2u);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_EQ(r.purged.size(), 2u) << "the retires of this run are purged in it";
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "t"));
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "marker"));
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0) / "3.t"));
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(d.path, 0)));
}

TEST(TopicDirReconcileRegression, UnclaimedDirWithANonDroppedMarkerIdIsStillQuarantinedRegression) {
    TempDataDir d("unclaimedlive");
    touch(rune::node::topicsRoot(d.path, 0) / "t" / "0" / "seg.log", "unknown life");
    mark(rune::node::topicsRoot(d.path, 0) / "t", 9);   // 9 is not dropped, nobody claims "t"
    const std::vector<int32_t> dropped = {3};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_TRUE(r.retired.empty());
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(quarantineHas(d.path, "t-"));
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "t"));
}

// B8 + the dropped/ rules: only the entries the catalog marks dropped are purged.
TEST(TopicDirReconcileRegression, DroppedDirIsPurgedOnlyWhenTheCatalogMarksItsIdDroppedRegression) {
    TempDataDir d("drops");
    touch(rune::node::droppedRoot(d.path, 0) / "3.old" / "0" / "seg.log");        // id 3: dropped
    touch(rune::node::droppedRoot(d.path, 0) / "5.half" / "0" / "seg.log");       // id 5: dropped (interrupted purge)
    touch(rune::node::droppedRoot(d.path, 0) / "1.live" / "0" / "seg.log");       // id 1: LIVE — not ours to delete
    touch(rune::node::droppedRoot(d.path, 0) / "99.future" / "0" / "seg.log");    // id 99: the catalog knows nothing of it
    touch(rune::node::droppedRoot(d.path, 0) / "notanid" / "0" / "seg.log");      // unparseable
    touch(rune::node::droppedRoot(d.path, 0) / "03.padded" / "0" / "seg.log");    // the writer never pads
    const std::vector<int32_t> dropped = {3, 5};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 1)}, dropped, kNow);
    EXPECT_EQ(r.purged.size(), 2u);
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0) / "3.old"));
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0) / "5.half"));
    for (const char* kept : {"1.live", "99.future", "notanid", "03.padded"})
        EXPECT_TRUE(fs::exists(rune::node::droppedRoot(d.path, 0) / kept / "0" / "seg.log")) << kept << " is not marked dropped";
}

// The edge case "a dropped/ entry left over from an interrupted purge": a half-removed
// entry (some segments gone) is finished by remove_all; the run is idempotent.
TEST(TopicDirReconcileRegression, InterruptedPurgeIsFinishedAndRerunIsIdempotentRegression) {
    TempDataDir d("interrupted");
    touch(rune::node::droppedRoot(d.path, 0) / "4.t" / "0" / "00000000000000000000.index");   // the .log already gone
    touch(rune::node::droppedRoot(d.path, 0) / "4.t" / "1" / "00000000000000000000.log");
    const std::vector<int32_t> dropped = {4};
    const auto first = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_EQ(first.purged.size(), 1u);
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0) / "4.t"));
    const auto second = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_TRUE(second.purged.empty());
    EXPECT_TRUE(second.quarantined.empty());
}

// The edge case "a crash during reconcile": the first run quarantined some dirs and died;
// the re-run quarantines exactly the rest (never touching what moved), and a third run is
// a no-op — the end state is the same as an uninterrupted run.
TEST(TopicDirReconcileRegression, CrashDuringReconcileRerunFinishesCleanlyAndIsIdempotentRegression) {
    TempDataDir d("crash");
    for (const char* n : {"a", "b", "c"}) touch(rune::node::topicsRoot(d.path, 0) / n / "0" / "seg.log", n);
    touch(rune::node::topicsRoot(d.path, 0) / "live" / "0" / "seg.log");
    mark(rune::node::topicsRoot(d.path, 0) / "live", 0);
    // The crashed run moved "a" (one atomic rename) and then died before the others.
    fs::create_directories(rune::node::quarantineRoot(d.path, 0));
    fs::rename(rune::node::topicsRoot(d.path, 0) / "a", rune::node::quarantineRoot(d.path, 0) / ("a-" + std::to_string(kNow)));
    const auto rerun = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 0)}, kNoDrops, kNow);
    EXPECT_EQ(rerun.quarantined.size(), 2u);
    std::set<std::string> q;
    for (const auto& e : fs::directory_iterator(rune::node::quarantineRoot(d.path, 0))) q.insert(e.path().filename().string());
    EXPECT_EQ(q, (std::set<std::string>{"a-" + std::to_string(kNow), "b-" + std::to_string(kNow),
                                        "c-" + std::to_string(kNow)}));
    const auto third = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 0)}, kNoDrops, kNow);
    EXPECT_TRUE(third.quarantined.empty());
    EXPECT_TRUE(third.purged.empty());
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "live" / "0" / "seg.log"));
}

// A catalog with only drops: nothing is live, so every topic dir is an orphan (the dropped
// ones were already renamed away by the retire sequence) and the dropped/ leftovers go.
TEST(TopicDirReconcileRegression, OnlyDropsEveryUnclaimedDirIsQuarantinedAndTheDroppedPurgedRegression) {
    TempDataDir d("onlydrops");
    touch(rune::node::topicsRoot(d.path, 0) / "stray" / "0" / "seg.log");
    touch(rune::node::droppedRoot(d.path, 0) / "0.gone" / "0" / "seg.log");
    touch(rune::node::droppedRoot(d.path, 0) / "1.gone2" / "0" / "seg.log");
    const std::vector<int32_t> dropped = {0, 1};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_EQ(r.quarantined.size(), 1u);
    EXPECT_EQ(r.purged.size(), 2u);
    EXPECT_TRUE(fs::is_empty(rune::node::droppedRoot(d.path, 0)));
}

// A re-created name after a drop: the name is live again (under a NEW id); its dir — which
// the retire sequence moved away at the drop — is simply absent and the data of the dropped
// incarnation (dropped/<oldId>.<name>) is purged by ITS id, never the live topic's.
TEST(TopicDirReconcileRegression, ReCreatedNameKeepsItsDirAndOnlyTheOldIdsDroppedDataGoesRegression) {
    TempDataDir d("recreate");
    touch(rune::node::topicsRoot(d.path, 0) / "players" / "0" / "seg.log", "new incarnation");
    mark(rune::node::topicsRoot(d.path, 0) / "players", 1);
    touch(rune::node::droppedRoot(d.path, 0) / "0.players" / "0" / "seg.log", "old incarnation");
    const std::vector<int32_t> dropped = {0};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("players", 1)}, dropped, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_EQ(r.purged.size(), 1u);
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0) / "0.players"));
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "players" / "0" / "seg.log"));
}

TEST(TopicDirReconcileRegression, EmptyCatalogAndEmptyDataDirIsANoOpRegression) {
    TempDataDir d("empty");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(r.purged.empty());
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0))) << "nothing is created";
    EXPECT_FALSE(fs::exists(rune::node::quarantineRoot(d.path, 0)));
    EXPECT_FALSE(fs::exists(rune::node::droppedRoot(d.path, 0)));
}

TEST(TopicDirReconcileRegression, QuarantineNameCollisionGetsASuffixRegression) {
    TempDataDir d("q-collide");
    fs::create_directories(rune::node::quarantineRoot(d.path, 0) / ("gone-" + std::to_string(kNow)));
    touch(rune::node::quarantineRoot(d.path, 0) / ("gone-" + std::to_string(kNow) + "-1"));   // a FILE also collides
    touch(rune::node::topicsRoot(d.path, 0) / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(rune::node::quarantineRoot(d.path, 0) / ("gone-" + std::to_string(kNow) + "-2") / "0" / "seg.log"));
    EXPECT_TRUE(fs::is_empty(rune::node::quarantineRoot(d.path, 0) / ("gone-" + std::to_string(kNow))))
        << "an existing quarantine entry is never overwritten";
}

TEST(TopicDirReconcileRegression, QuarantinePathThatIsAFileMovesNothingRegression) {
    TempDataDir d("q-file");
    touch(rune::node::quarantineRoot(d.path, 0), "not a dir");
    touch(rune::node::topicsRoot(d.path, 0) / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "gone" / "0" / "seg.log")) << "nothing is moved, nothing deleted";
    EXPECT_TRUE(fs::is_regular_file(rune::node::quarantineRoot(d.path, 0)));
}

TEST(TopicDirReconcileRegression, NonTopicEntriesInTopicsDirAreLeftAloneRegression) {
    TempDataDir d("non-topic");
    touch(rune::node::topicsRoot(d.path, 0) / "README");                          // a plain file
    touch(rune::node::topicsRoot(d.path, 0) / "notes" / "todo.txt");              // files directly under <name>/
    touch(rune::node::topicsRoot(d.path, 0) / "weird" / "abc" / "seg.log");       // non-numeric partition dir
    touch(rune::node::topicsRoot(d.path, 0) / "nested" / "0" / "deeper" / "x");   // a sub-dir inside a partition
    fs::create_directories(rune::node::topicsRoot(d.path, 0) / "empty");          // no partition dir at all
    touch(d.path / "elsewhere" / "0" / "seg.log");                // outside ns-0/topics/: never scanned
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    for (const char* p : {"README", "notes/todo.txt", "weird/abc/seg.log", "nested/0/deeper/x", "empty"})
        EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / p)) << p;
    EXPECT_TRUE(fs::exists(d.path / "elsewhere" / "0" / "seg.log"));
}

// Nothing outside dataDir/ns-0/topics (and ns-0/dropped/) is touched — the retired catalog dirs and
// the live __catalog among them (B7: left untouched, no deletion, no reads).
TEST(TopicDirReconcileRegression, RetiredCatalogDirsAndOtherDataDirChildrenAreNeverTouchedRegression) {
    TempDataDir d("outside");
    touch(d.path / "__topic_schemas" / "00000000000000000000.log", "garbage");
    touch(d.path / "__views" / "00000000000000000000.log", "garbage");
    touch(d.path / "__views" / "0" / "seg.log");
    touch(d.path / "__catalog" / "00000000000000000000.log", "catalog");
    touch(rune::node::topicsRoot(d.path, 0) / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_EQ(r.quarantined.size(), 1u);
    for (const char* p : {"__topic_schemas/00000000000000000000.log", "__views/00000000000000000000.log",
                          "__views/0/seg.log", "__catalog/00000000000000000000.log"})
        EXPECT_TRUE(fs::exists(d.path / p)) << p;
    std::set<std::string> top;
    for (const auto& e : fs::directory_iterator(d.path)) top.insert(e.path().filename().string());
    EXPECT_EQ(top, (std::set<std::string>{"__catalog", "__topic_schemas", "__views", "ns-0"}));
}

TEST(TopicDirReconcileRegression, SymlinksAreNeverFollowedRegression) {
    TempDataDir d("symlink");
    TempDataDir outside("symlink-outside");
    touch(outside.path / "victim" / "0" / "00000000000000000000.log");
    touch(outside.path / "dropvictim" / "x");
    fs::create_directories(rune::node::topicsRoot(d.path, 0));
    fs::create_directory_symlink(outside.path / "victim", rune::node::topicsRoot(d.path, 0) / "linked");
    touch(rune::node::topicsRoot(d.path, 0) / "mixed" / "0" / "seg.log");
    fs::create_symlink(outside.path / "victim" / "0" / "00000000000000000000.log",
                       rune::node::topicsRoot(d.path, 0) / "mixed" / "0" / "link.log");        // a symlink inside a partition
    fs::create_directories(rune::node::droppedRoot(d.path, 0));
    fs::create_directory_symlink(outside.path / "dropvictim", rune::node::droppedRoot(d.path, 0) / "7.linked");
    const std::vector<int32_t> dropped = {7};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_TRUE(r.quarantined.empty()) << "a symlinked dir is never moved; a dir holding a symlink is no topic layout";
    EXPECT_TRUE(r.purged.empty()) << "a symlinked dropped/ entry is never removed through";
    EXPECT_TRUE(fs::exists(outside.path / "victim" / "0" / "00000000000000000000.log"));
    EXPECT_TRUE(fs::exists(outside.path / "dropvictim" / "x"));
    EXPECT_TRUE(fs::is_symlink(rune::node::topicsRoot(d.path, 0) / "linked"));
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "mixed" / "0" / "seg.log"));
}

TEST(TopicDirReconcileRegression, SymlinkedTopicsOrQuarantineRootIsNeverFollowedRegression) {
    {
        TempDataDir d("symlink-root");
        TempDataDir outside("symlink-root-outside");
        touch(outside.path / "gone" / "0" / "seg.log");
        fs::create_directories(rune::node::nsRoot(d.path, 0));
        fs::create_directory_symlink(outside.path, rune::node::topicsRoot(d.path, 0));
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
        EXPECT_TRUE(r.quarantined.empty());
        EXPECT_TRUE(fs::exists(outside.path / "gone" / "0" / "seg.log"));
    }
    {
        TempDataDir d("symlink-q");
        TempDataDir outside("symlink-q-outside");
        touch(rune::node::topicsRoot(d.path, 0) / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, rune::node::quarantineRoot(d.path, 0));
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
        EXPECT_TRUE(r.quarantined.empty()) << "never moves data through a symlinked quarantine";
        EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "gone" / "0" / "seg.log"));
        EXPECT_TRUE(fs::is_empty(outside.path));
    }
}

// R2: the boot reconcile decision table, every row (first match wins).
TEST(TopicDirReconcileRegression, DecideBootReconcileMatrixRegression) {
    using rune::node::BootReconcile;
    using rune::node::CatchUpSentinel;
    using K = CatchUpSentinel::Kind;
    const CatchUpSentinel none{K::Absent, -1}, pending{K::Pending, -1}, caught{K::CaughtUp, 10};
    auto d = [](bool halted, int64_t applied, bool marked, bool droppedEntries, CatchUpSentinel s) {
        return rune::node::decideBootReconcile(halted, applied, marked, droppedEntries, s);
    };
    // halted: Skip, whatever the sentinel
    for (const CatchUpSentinel& s : {none, pending, caught}) EXPECT_EQ(d(true, 20, true, true, s), BootReconcile::Skip);
    // CaughtUp
    EXPECT_EQ(d(false, 10, true, true, caught), BootReconcile::RunNowAndClearSentinel);
    EXPECT_EQ(d(false, 99, false, false, caught), BootReconcile::RunNowAndClearSentinel);
    EXPECT_EQ(d(false, 9, true, true, caught), BootReconcile::DeferAndWritePending) << "applied less than the sentinel";
    EXPECT_EQ(d(false, -1, true, true, caught), BootReconcile::DeferAndWritePending);
    // Pending
    EXPECT_EQ(d(false, 50, true, true, pending), BootReconcile::Defer);
    EXPECT_EQ(d(false, -1, false, false, pending), BootReconcile::Defer);
    // nothing applied: only MARKED dirs / dropped entries defer
    EXPECT_EQ(d(false, -1, true, false, none), BootReconcile::DeferAndWritePending);
    EXPECT_EQ(d(false, -1, false, true, none), BootReconcile::DeferAndWritePending);
    EXPECT_EQ(d(false, -1, false, false, none), BootReconcile::RunNow) << "legacy / static markerless dirs reconcile at once";
    // a catalog with history
    EXPECT_EQ(d(false, 0, true, true, none), BootReconcile::RunNow);
}

TEST(TopicDirReconcileRegression, CatchUpSentinelRoundTripsAndMalformedReadsAsPendingRegression) {
    using K = rune::node::CatchUpSentinel::Kind;
    TempDataDir d("sentinel");
    EXPECT_EQ(rune::node::readCatchUpSentinel(d.path.string()).kind, K::Absent);
    ASSERT_TRUE(rune::node::writeCatchUpSentinel(d.path.string(), -1));
    EXPECT_EQ(rune::node::readCatchUpSentinel(d.path.string()).kind, K::Pending);
    ASSERT_TRUE(rune::node::writeCatchUpSentinel(d.path.string(), 41));
    const auto s = rune::node::readCatchUpSentinel(d.path.string());
    EXPECT_EQ(s.kind, K::CaughtUp);
    EXPECT_EQ(s.offset, 41);
    touch(d.path / rune::node::kCatchUpSentinelFile, "caughtup banana\n");
    EXPECT_EQ(rune::node::readCatchUpSentinel(d.path.string()).kind, K::Pending) << "never trusted";
    ASSERT_TRUE(rune::node::removeCatchUpSentinel(d.path.string()));
    EXPECT_EQ(rune::node::readCatchUpSentinel(d.path.string()).kind, K::Absent);
    EXPECT_TRUE(rune::node::removeCatchUpSentinel(d.path.string())) << "absent -> true";
}

// ---------------------------------------------------------------------------
// VLS B.2 (M14): a view sink owns no dir.  An old sink dir (an earlier build made a replica for it) that holds no
// record is removed at boot; a live topic's dir and a sink dir holding records are not touched as a sink.
// ---------------------------------------------------------------------------
#include <rune/gateway/TopicRegistry.h>

TEST(ViewSinkBootRegression, ReconcileRemovesAnOldEmptySinkDirAndLeavesLiveTopicsAlone) {
    TempDataDir d("sinkdir");
    mark(rune::node::topicsRoot(d.path, 0) / "players", 1);
    fs::create_directories(rune::node::topicsRoot(d.path, 0) / "players" / "0");
    mark(rune::node::topicsRoot(d.path, 0) / "top", 2);   // the old sink replica's dir: marker + empty partition dir
    fs::create_directories(rune::node::topicsRoot(d.path, 0) / "top" / "0");
    TopicSpec sink = spec("top", 2);
    sink.viewSink = true;
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("players", 1), sink}, {}, kNow);
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "top")) << "the sink's empty dir is removed";
    EXPECT_EQ(r.removedSinkDirs.size(), 1u);
    EXPECT_TRUE(fs::exists(rune::node::topicsRoot(d.path, 0) / "players")) << "a live topic's dir stays";
    EXPECT_TRUE(r.quarantined.empty());
}

TEST(ViewSinkBootRegression, RegistryHoldsASinkAsACatalogOnlySlotWithNoLogNoDirAndDropsAtOnce) {
    TempDataDir d("sinkreg");
    rune::gateway::TopicRegistry reg({}, d.path.string());
    TopicSpec sink = spec("top", 3);
    sink.viewSink = true;
    sink.partitionCount = 2;
    ASSERT_EQ(reg.createTopicAt(3, sink), 3);
    EXPECT_EQ(reg.state(3), rune::gateway::TopicRegistry::TopicState::ViewSink);
    EXPECT_TRUE(reg.isViewSink(3));
    EXPECT_EQ(reg.topic(3), nullptr) << "no serving spec: never a data-plane target";
    EXPECT_EQ(reg.log(3, 0), nullptr);
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(d.path, 0) / "top")) << "no dir";
    ASSERT_NE(reg.viewSinkSpec(3), nullptr);
    EXPECT_EQ(reg.viewSinkSpec(3)->partitionCount, 2);
    EXPECT_GE(reg.partitionForKey(3, std::span<const uint8_t>{}), 0) << "GetViewRow still resolves key -> partition";
    EXPECT_EQ(reg.createTopicAt(3, sink), 3) << "the same effect again is idempotent";
    ASSERT_TRUE(reg.dropTopic(3));
    EXPECT_EQ(reg.state(3), rune::gateway::TopicRegistry::TopicState::Dropped);
    EXPECT_FALSE(reg.topicIdForName("top").has_value()) << "the name is free at once (no logs to retire)";
    EXPECT_EQ(reg.createTopicAt(3, sink), -1) << "a dropped id is never refilled";
}
