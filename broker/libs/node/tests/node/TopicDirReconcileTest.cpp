// TopicDirReconcileTest.cpp — catalog history Task 16: boot's topic-dir reconcile against
// the BOOTED catalog (reconcileTopicDirs), on real files.  The function never deletes a
// topic dir (it only quarantines), purges only the dropped/ entries the catalog marks
// dropped, claims names exactly or by ASCII case fold, and never follows a symlink.
//
// Ported from the retired RuntimeTopicRecoveryTest's reconcile cases (the marker /
// placeholder / damage-latch machinery died with the module); every test here is named
// *Regression so the suite runs with the checkpoint gate.
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

TopicSpec spec(const std::string& name, int32_t id) {
    TopicSpec s;
    s.name = name; s.topicId = id; s.partitionCount = 1;
    return s;
}

constexpr int64_t kNow = 1'700'000'000;
const std::vector<int32_t> kNoDrops;

} // namespace

TEST(TopicDirReconcileRegression, UnclaimedTopicDirIsQuarantinedNeverDeletedRegression) {
    TempDataDir d("quar");
    touch(d.path / "topics" / "live" / "0" / "seg.log");
    touch(d.path / "topics" / "ghost" / "0" / "seg.log", "precious");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 0)}, kNoDrops, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(r.purged.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics" / "ghost"));
    EXPECT_EQ(r.quarantined[0], d.path / "quarantine" / ("ghost-" + std::to_string(kNow)));
    std::ifstream in(r.quarantined[0] / "0" / "seg.log");
    std::string back; in >> back;
    EXPECT_EQ(back, "precious") << "the data is moved intact, never deleted";
    EXPECT_TRUE(fs::exists(d.path / "topics" / "live" / "0" / "seg.log")) << "a live topic's dir is not touched";
}

// B8: a live topic whose dir differs only in ASCII case is claimed (on a case-insensitive
// filesystem it IS the live topic's dir) — NOT quarantined.
TEST(TopicDirReconcileRegression, CaseVariantOfALiveNameIsNotQuarantinedRegression) {
    TempDataDir d("case");
    touch(d.path / "topics" / "Orders" / "0" / "seg.log");
    touch(d.path / "topics" / "EVENTS" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0), spec("events", 1)}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(d.path / "topics" / "Orders" / "0" / "seg.log"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "EVENTS" / "0" / "seg.log"));
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));
}

// B8: the ASCII fold is ASCII only — a non-letter difference is a different name.
TEST(TopicDirReconcileRegression, OnlyAsciiCaseFoldsClaimADirRegression) {
    TempDataDir d("fold");
    touch(d.path / "topics" / "orders_2" / "0" / "seg.log");
    touch(d.path / "topics" / "order" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("orders", 0)}, kNoDrops, kNow);
    EXPECT_EQ(r.quarantined.size(), 2u);
}

// The edge case "a topic dir without a topic-id marker": the retired marker is not a
// thing any more — an unclaimed dir with none is quarantined like any other, and a
// claimed one keeps whatever (stale) marker file it carries, unread and untouched.
TEST(TopicDirReconcileRegression, MarkerlessAndStaleMarkerDirsAreJudgedByTheCatalogAloneRegression) {
    TempDataDir d("marker");
    touch(d.path / "topics" / "nomarker" / "0" / "seg.log");
    touch(d.path / "topics" / "oldmarker" / "topic-id", "pending 7\n");     // an older build's leftover
    touch(d.path / "topics" / "oldmarker" / "0" / "seg.log");
    touch(d.path / "topics" / "claimed" / "topic-id", "pending 7\n");
    touch(d.path / "topics" / "claimed" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("claimed", 0)}, kNoDrops, kNow);
    EXPECT_EQ(r.quarantined.size(), 2u) << "no marker decides anything: the unclaimed dirs are quarantined";
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("nomarker-" + std::to_string(kNow)) / "0" / "seg.log"));
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("oldmarker-" + std::to_string(kNow)) / "topic-id"));
    std::ifstream in(d.path / "topics" / "claimed" / "topic-id");
    std::string line; std::getline(in, line);
    EXPECT_EQ(line, "pending 7") << "a claimed dir's marker is left exactly as it was";
}

// B8 + the dropped/ rules: only the entries the catalog marks dropped are purged.
TEST(TopicDirReconcileRegression, DroppedDirIsPurgedOnlyWhenTheCatalogMarksItsIdDroppedRegression) {
    TempDataDir d("dropped");
    touch(d.path / "dropped" / "3.old" / "0" / "seg.log");        // id 3: dropped
    touch(d.path / "dropped" / "5.half" / "0" / "seg.log");       // id 5: dropped (interrupted purge)
    touch(d.path / "dropped" / "1.live" / "0" / "seg.log");       // id 1: LIVE — not ours to delete
    touch(d.path / "dropped" / "99.future" / "0" / "seg.log");    // id 99: the catalog knows nothing of it
    touch(d.path / "dropped" / "notanid" / "0" / "seg.log");      // unparseable
    touch(d.path / "dropped" / "03.padded" / "0" / "seg.log");    // the writer never pads
    const std::vector<int32_t> dropped = {3, 5};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 1)}, dropped, kNow);
    EXPECT_EQ(r.purged.size(), 2u);
    EXPECT_FALSE(fs::exists(d.path / "dropped" / "3.old"));
    EXPECT_FALSE(fs::exists(d.path / "dropped" / "5.half"));
    for (const char* kept : {"1.live", "99.future", "notanid", "03.padded"})
        EXPECT_TRUE(fs::exists(d.path / "dropped" / kept / "0" / "seg.log")) << kept << " is not marked dropped";
}

// The edge case "a dropped/ entry left over from an interrupted purge": a half-removed
// entry (some segments gone) is finished by remove_all; the run is idempotent.
TEST(TopicDirReconcileRegression, InterruptedPurgeIsFinishedAndRerunIsIdempotentRegression) {
    TempDataDir d("interrupted");
    touch(d.path / "dropped" / "4.t" / "0" / "00000000000000000000.index");   // the .log already gone
    touch(d.path / "dropped" / "4.t" / "1" / "00000000000000000000.log");
    const std::vector<int32_t> dropped = {4};
    const auto first = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_EQ(first.purged.size(), 1u);
    EXPECT_FALSE(fs::exists(d.path / "dropped" / "4.t"));
    const auto second = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_TRUE(second.purged.empty());
    EXPECT_TRUE(second.quarantined.empty());
}

// The edge case "a crash during reconcile": the first run quarantined some dirs and died;
// the re-run quarantines exactly the rest (never touching what moved), and a third run is
// a no-op — the end state is the same as an uninterrupted run.
TEST(TopicDirReconcileRegression, CrashDuringReconcileRerunFinishesCleanlyAndIsIdempotentRegression) {
    TempDataDir d("crash");
    for (const char* n : {"a", "b", "c"}) touch(d.path / "topics" / n / "0" / "seg.log", n);
    touch(d.path / "topics" / "live" / "0" / "seg.log");
    // The crashed run moved "a" (one atomic rename) and then died before the others.
    fs::create_directories(d.path / "quarantine");
    fs::rename(d.path / "topics" / "a", d.path / "quarantine" / ("a-" + std::to_string(kNow)));
    const auto rerun = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 0)}, kNoDrops, kNow);
    EXPECT_EQ(rerun.quarantined.size(), 2u);
    std::set<std::string> q;
    for (const auto& e : fs::directory_iterator(d.path / "quarantine")) q.insert(e.path().filename().string());
    EXPECT_EQ(q, (std::set<std::string>{"a-" + std::to_string(kNow), "b-" + std::to_string(kNow),
                                        "c-" + std::to_string(kNow)}));
    const auto third = rune::node::reconcileTopicDirs(d.path.string(), {spec("live", 0)}, kNoDrops, kNow);
    EXPECT_TRUE(third.quarantined.empty());
    EXPECT_TRUE(third.purged.empty());
    EXPECT_TRUE(fs::exists(d.path / "topics" / "live" / "0" / "seg.log"));
}

// A catalog with only drops: nothing is live, so every topic dir is an orphan (the dropped
// ones were already renamed away by the retire sequence) and the dropped/ leftovers go.
TEST(TopicDirReconcileRegression, OnlyDropsEveryUnclaimedDirIsQuarantinedAndTheDroppedPurgedRegression) {
    TempDataDir d("onlydrops");
    touch(d.path / "topics" / "stray" / "0" / "seg.log");
    touch(d.path / "dropped" / "0.gone" / "0" / "seg.log");
    touch(d.path / "dropped" / "1.gone2" / "0" / "seg.log");
    const std::vector<int32_t> dropped = {0, 1};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_EQ(r.quarantined.size(), 1u);
    EXPECT_EQ(r.purged.size(), 2u);
    EXPECT_TRUE(fs::is_empty(d.path / "dropped"));
}

// A re-created name after a drop: the name is live again (under a NEW id); its dir — which
// the retire sequence moved away at the drop — is simply absent and the data of the dropped
// incarnation (dropped/<oldId>.<name>) is purged by ITS id, never the live topic's.
TEST(TopicDirReconcileRegression, ReCreatedNameKeepsItsDirAndOnlyTheOldIdsDroppedDataGoesRegression) {
    TempDataDir d("recreate");
    touch(d.path / "topics" / "players" / "0" / "seg.log", "new incarnation");
    touch(d.path / "dropped" / "0.players" / "0" / "seg.log", "old incarnation");
    const std::vector<int32_t> dropped = {0};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {spec("players", 1)}, dropped, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_EQ(r.purged.size(), 1u);
    EXPECT_FALSE(fs::exists(d.path / "dropped" / "0.players"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "players" / "0" / "seg.log"));
}

TEST(TopicDirReconcileRegression, EmptyCatalogAndEmptyDataDirIsANoOpRegression) {
    TempDataDir d("empty");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(r.purged.empty());
    EXPECT_FALSE(fs::exists(d.path / "topics")) << "nothing is created";
    EXPECT_FALSE(fs::exists(d.path / "quarantine"));
    EXPECT_FALSE(fs::exists(d.path / "dropped"));
}

TEST(TopicDirReconcileRegression, QuarantineNameCollisionGetsASuffixRegression) {
    TempDataDir d("q-collide");
    fs::create_directories(d.path / "quarantine" / ("gone-" + std::to_string(kNow)));
    touch(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-1"));   // a FILE also collides
    touch(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    ASSERT_EQ(r.quarantined.size(), 1u);
    EXPECT_TRUE(fs::exists(d.path / "quarantine" / ("gone-" + std::to_string(kNow) + "-2") / "0" / "seg.log"));
    EXPECT_TRUE(fs::is_empty(d.path / "quarantine" / ("gone-" + std::to_string(kNow))))
        << "an existing quarantine entry is never overwritten";
}

TEST(TopicDirReconcileRegression, QuarantinePathThatIsAFileMovesNothingRegression) {
    TempDataDir d("q-file");
    touch(d.path / "quarantine", "not a dir");
    touch(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    EXPECT_TRUE(fs::exists(d.path / "topics" / "gone" / "0" / "seg.log")) << "nothing is moved, nothing deleted";
    EXPECT_TRUE(fs::is_regular_file(d.path / "quarantine"));
}

TEST(TopicDirReconcileRegression, NonTopicEntriesInTopicsDirAreLeftAloneRegression) {
    TempDataDir d("non-topic");
    touch(d.path / "topics" / "README");                          // a plain file
    touch(d.path / "topics" / "notes" / "todo.txt");              // files directly under <name>/
    touch(d.path / "topics" / "weird" / "abc" / "seg.log");       // non-numeric partition dir
    touch(d.path / "topics" / "nested" / "0" / "deeper" / "x");   // a sub-dir inside a partition
    fs::create_directories(d.path / "topics" / "empty");          // no partition dir at all
    touch(d.path / "elsewhere" / "0" / "seg.log");                // outside topics/: never scanned
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_TRUE(r.quarantined.empty());
    for (const char* p : {"topics/README", "topics/notes/todo.txt", "topics/weird/abc/seg.log",
                          "topics/nested/0/deeper/x", "topics/empty", "elsewhere/0/seg.log"})
        EXPECT_TRUE(fs::exists(d.path / p)) << p;
}

// Nothing outside dataDir/topics (and dropped/) is touched — the retired catalog dirs and
// the live __catalog among them (B7: left untouched, no deletion, no reads).
TEST(TopicDirReconcileRegression, RetiredCatalogDirsAndOtherDataDirChildrenAreNeverTouchedRegression) {
    TempDataDir d("outside");
    touch(d.path / "__topic_schemas" / "00000000000000000000.log", "garbage");
    touch(d.path / "__views" / "00000000000000000000.log", "garbage");
    touch(d.path / "__views" / "0" / "seg.log");
    touch(d.path / "__catalog" / "00000000000000000000.log", "catalog");
    touch(d.path / "topics" / "gone" / "0" / "seg.log");
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
    EXPECT_EQ(r.quarantined.size(), 1u);
    for (const char* p : {"__topic_schemas/00000000000000000000.log", "__views/00000000000000000000.log",
                          "__views/0/seg.log", "__catalog/00000000000000000000.log"})
        EXPECT_TRUE(fs::exists(d.path / p)) << p;
    std::set<std::string> top;
    for (const auto& e : fs::directory_iterator(d.path)) top.insert(e.path().filename().string());
    EXPECT_EQ(top, (std::set<std::string>{"__catalog", "__topic_schemas", "__views", "quarantine", "topics"}));
}

TEST(TopicDirReconcileRegression, SymlinksAreNeverFollowedRegression) {
    TempDataDir d("symlink");
    TempDataDir outside("symlink-outside");
    touch(outside.path / "victim" / "0" / "00000000000000000000.log");
    touch(outside.path / "dropvictim" / "x");
    fs::create_directories(d.path / "topics");
    fs::create_directory_symlink(outside.path / "victim", d.path / "topics" / "linked");
    touch(d.path / "topics" / "mixed" / "0" / "seg.log");
    fs::create_symlink(outside.path / "victim" / "0" / "00000000000000000000.log",
                       d.path / "topics" / "mixed" / "0" / "link.log");        // a symlink inside a partition
    fs::create_directories(d.path / "dropped");
    fs::create_directory_symlink(outside.path / "dropvictim", d.path / "dropped" / "7.linked");
    const std::vector<int32_t> dropped = {7};
    const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, dropped, kNow);
    EXPECT_TRUE(r.quarantined.empty()) << "a symlinked dir is never moved; a dir holding a symlink is no topic layout";
    EXPECT_TRUE(r.purged.empty()) << "a symlinked dropped/ entry is never removed through";
    EXPECT_TRUE(fs::exists(outside.path / "victim" / "0" / "00000000000000000000.log"));
    EXPECT_TRUE(fs::exists(outside.path / "dropvictim" / "x"));
    EXPECT_TRUE(fs::is_symlink(d.path / "topics" / "linked"));
    EXPECT_TRUE(fs::exists(d.path / "topics" / "mixed" / "0" / "seg.log"));
}

TEST(TopicDirReconcileRegression, SymlinkedTopicsOrQuarantineRootIsNeverFollowedRegression) {
    {
        TempDataDir d("symlink-root");
        TempDataDir outside("symlink-root-outside");
        touch(outside.path / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "topics");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
        EXPECT_TRUE(r.quarantined.empty());
        EXPECT_TRUE(fs::exists(outside.path / "gone" / "0" / "seg.log"));
    }
    {
        TempDataDir d("symlink-q");
        TempDataDir outside("symlink-q-outside");
        touch(d.path / "topics" / "gone" / "0" / "seg.log");
        fs::create_directory_symlink(outside.path, d.path / "quarantine");
        const auto r = rune::node::reconcileTopicDirs(d.path.string(), {}, kNoDrops, kNow);
        EXPECT_TRUE(r.quarantined.empty()) << "never moves data through a symlinked quarantine";
        EXPECT_TRUE(fs::exists(d.path / "topics" / "gone" / "0" / "seg.log"));
        EXPECT_TRUE(fs::is_empty(outside.path));
    }
}

// INTERIM (until Task 17): a static RUNE_TOPICS topic never shares a broker with a catalog topic.
TEST(StaticCatalogCollisionRegression, NameIdAndCaseVariantCollisionsRefuseBootNamingBoth) {
    using rune::node::staticCatalogCollision;
    const std::vector<TopicSpec> catalog = {spec("ddl_ready", 0), spec("orders", 1)};
    {
        const auto r = staticCatalogCollision({spec("orders", 0)}, catalog);
        ASSERT_TRUE(r.has_value());
        EXPECT_NE(r->find("'orders' (id 0) collides by name with the catalog topic 'orders' (id 1)"), std::string::npos) << *r;
    }
    {
        const auto r = staticCatalogCollision({spec("ORDERS", 5)}, catalog);
        ASSERT_TRUE(r.has_value()) << "a case variant shares the directory";
        EXPECT_NE(r->find("by name"), std::string::npos) << *r;
    }
    {
        const auto r = staticCatalogCollision({spec("extra", 0)}, catalog);
        ASSERT_TRUE(r.has_value()) << "static id 0 is catalog topic ddl_ready's";
        EXPECT_NE(r->find("'extra' (id 0) collides by topic id with the catalog topic 'ddl_ready' (id 0)"),
                  std::string::npos) << *r;
    }
    {
        const auto r = staticCatalogCollision({spec("extra", -1)}, catalog);   // id = its position (0)
        ASSERT_TRUE(r.has_value());
        EXPECT_NE(r->find("by topic id"), std::string::npos) << *r;
    }
    EXPECT_FALSE(staticCatalogCollision({spec("extra", 7)}, catalog).has_value()) << "no shared name or id";
    EXPECT_FALSE(staticCatalogCollision({spec("events", 0)}, {}).has_value()) << "static topics alone boot";
    EXPECT_FALSE(staticCatalogCollision({}, catalog).has_value()) << "catalog topics alone boot";
}
