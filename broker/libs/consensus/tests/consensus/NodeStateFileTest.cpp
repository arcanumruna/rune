#include <gtest/gtest.h>
#include <rune/consensus/NodeStateFile.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <unistd.h>

using namespace rune::consensus;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// RAII unique temp-file helper
// ---------------------------------------------------------------------------
struct TempNsf {
    fs::path path;
    explicit TempNsf(const char* tag = "") {
        static std::atomic<uint64_t> g_tempSeq{0};
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        auto uniq = std::to_string(::getpid()) + "_" +
                    std::to_string(g_tempSeq.fetch_add(1, std::memory_order_relaxed)) + "_" +
                    std::to_string(ts);
        path = fs::temp_directory_path() /
               ("rune_nsf_" + uniq + std::string(tag) + ".dat");
        fs::remove(path);
    }
    ~TempNsf() {
        std::error_code ec;
        fs::remove(path, ec);
    }
};

// ---------------------------------------------------------------------------
// 1. Fresh file defaults
// ---------------------------------------------------------------------------
TEST(NodeStateFile, FreshFileDefaultCandidateTermId) {
    TempNsf t("_fresh1");
    NodeStateFile nsf(t.path.string());
    EXPECT_EQ(nsf.candidateTermId(), NodeStateFile::NULL_VALUE);
}

TEST(NodeStateFile, FreshFileDefaultTimestamp) {
    TempNsf t("_fresh2");
    NodeStateFile nsf(t.path.string());
    EXPECT_EQ(nsf.candidateTermTimestamp(), 0);
}

TEST(NodeStateFile, FreshFileDefaultLogPosition) {
    TempNsf t("_fresh3");
    NodeStateFile nsf(t.path.string());
    EXPECT_EQ(nsf.candidateTermLogPosition(), 0);
}

// ---------------------------------------------------------------------------
// 2. Basic propose: higher id accepted, all fields set
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ProposeHigherIdReturnsNewId) {
    TempNsf t("_propose1");
    NodeStateFile nsf(t.path.string());
    Term result = nsf.proposeMaxCandidateTermId(5, 100, 1000);
    EXPECT_EQ(result, 5);
}

TEST(NodeStateFile, ProposeHigherIdSetsCandidateTermId) {
    TempNsf t("_propose2");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    EXPECT_EQ(nsf.candidateTermId(), 5);
}

TEST(NodeStateFile, ProposeHigherIdSetsLogPosition) {
    TempNsf t("_propose3");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    EXPECT_EQ(nsf.candidateTermLogPosition(), 100);
}

TEST(NodeStateFile, ProposeHigherIdSetsTimestamp) {
    TempNsf t("_propose4");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    EXPECT_EQ(nsf.candidateTermTimestamp(), 1000);
}

// ---------------------------------------------------------------------------
// 3. Monotonic: lower id is ignored
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ProposeLowerIdIgnoredReturnsExisting) {
    TempNsf t("_mono1");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);

    Term result = nsf.proposeMaxCandidateTermId(3, 999, 9999);
    EXPECT_EQ(result, 5);
}

TEST(NodeStateFile, ProposeLowerIdDoesNotOverwriteCandidateTermId) {
    TempNsf t("_mono2");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    nsf.proposeMaxCandidateTermId(3, 999, 9999);
    EXPECT_EQ(nsf.candidateTermId(), 5);
}

TEST(NodeStateFile, ProposeLowerIdDoesNotOverwriteLogPosition) {
    TempNsf t("_mono3");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    nsf.proposeMaxCandidateTermId(3, 999, 9999);
    EXPECT_EQ(nsf.candidateTermLogPosition(), 100);
}

TEST(NodeStateFile, ProposeLowerIdDoesNotOverwriteTimestamp) {
    TempNsf t("_mono4");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    nsf.proposeMaxCandidateTermId(3, 999, 9999);
    EXPECT_EQ(nsf.candidateTermTimestamp(), 1000);
}

// ---------------------------------------------------------------------------
// 4. Equal id is ignored (no-op, returns current)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ProposeEqualIdNoChange) {
    TempNsf t("_equal1");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);

    Term result = nsf.proposeMaxCandidateTermId(5, 200, 2000);
    EXPECT_EQ(result, 5);
    EXPECT_EQ(nsf.candidateTermId(),          5);
    EXPECT_EQ(nsf.candidateTermLogPosition(), 100);  // unchanged
    EXPECT_EQ(nsf.candidateTermTimestamp(),   1000); // unchanged
}

// ---------------------------------------------------------------------------
// 5. Higher id overwrites all three fields
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ProposeHigherIdOverwritesAllFields) {
    TempNsf t("_higher1");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(5, 100, 1000);
    nsf.proposeMaxCandidateTermId(7, 200, 2000);

    EXPECT_EQ(nsf.candidateTermId(),          7);
    EXPECT_EQ(nsf.candidateTermLogPosition(), 200);
    EXPECT_EQ(nsf.candidateTermTimestamp(),   2000);
}

// ---------------------------------------------------------------------------
// 6. Persistence across reopen (no fileSyncLevel)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, PersistsAcrossReopen) {
    TempNsf t("_reopen1");
    {
        NodeStateFile nsf(t.path.string());
        nsf.proposeMaxCandidateTermId(5, 100, 1000);
    }
    {
        NodeStateFile nsf2(t.path.string());
        EXPECT_EQ(nsf2.candidateTermId(),          5);
        EXPECT_EQ(nsf2.candidateTermLogPosition(), 100);
        EXPECT_EQ(nsf2.candidateTermTimestamp(),   1000);
    }
}

// ---------------------------------------------------------------------------
// 7. Persistence across reopen with fileSyncLevel=1 (msync path)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, PersistsAcrossReopenWithSync) {
    TempNsf t("_reopen_sync");
    {
        NodeStateFile nsf(t.path.string(), /*fileSyncLevel=*/1);
        nsf.proposeMaxCandidateTermId(8, 400, 5000);
    }
    {
        NodeStateFile nsf2(t.path.string(), /*fileSyncLevel=*/1);
        EXPECT_EQ(nsf2.candidateTermId(),          8);
        EXPECT_EQ(nsf2.candidateTermLogPosition(), 400);
        EXPECT_EQ(nsf2.candidateTermTimestamp(),   5000);
    }
}

// ---------------------------------------------------------------------------
// 8. Fresh file defaults survive reopen (null value persists)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, FreshFileDefaultsSurviveReopen) {
    TempNsf t("_fresh_reopen");
    {
        NodeStateFile nsf(t.path.string());
        // Don't propose anything — just create the file.
    }
    {
        NodeStateFile nsf2(t.path.string());
        EXPECT_EQ(nsf2.candidateTermId(),          NodeStateFile::NULL_VALUE);
        EXPECT_EQ(nsf2.candidateTermTimestamp(),   0);
        EXPECT_EQ(nsf2.candidateTermLogPosition(), 0);
    }
}

// ---------------------------------------------------------------------------
// 9. Sequence of increasing proposals — last wins
// ---------------------------------------------------------------------------
TEST(NodeStateFile, SequenceOfIncreasingProposals) {
    TempNsf t("_seq1");
    NodeStateFile nsf(t.path.string());

    for (Term id = 1; id <= 10; ++id) {
        Term result = nsf.proposeMaxCandidateTermId(id, id * 10, id * 100);
        EXPECT_EQ(result, id);
    }

    EXPECT_EQ(nsf.candidateTermId(),          10);
    EXPECT_EQ(nsf.candidateTermLogPosition(), 100);
    EXPECT_EQ(nsf.candidateTermTimestamp(),   1000);
}

// ---------------------------------------------------------------------------
// 10. Sequence of increasing proposals — latest persists across reopen
// ---------------------------------------------------------------------------
TEST(NodeStateFile, SequenceOfIncreasingProposalsSurviveReopen) {
    TempNsf t("_seq_reopen");
    {
        NodeStateFile nsf(t.path.string());
        for (Term id = 1; id <= 5; ++id) {
            nsf.proposeMaxCandidateTermId(id, id * 50, id * 500);
        }
    }
    {
        NodeStateFile nsf2(t.path.string());
        EXPECT_EQ(nsf2.candidateTermId(),          5);
        EXPECT_EQ(nsf2.candidateTermLogPosition(), 250);
        EXPECT_EQ(nsf2.candidateTermTimestamp(),   2500);
    }
}

// ---------------------------------------------------------------------------
// 11. Propose from NULL_VALUE baseline
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ProposeFromNullValue) {
    TempNsf t("_null_base");
    NodeStateFile nsf(t.path.string());
    // NULL_VALUE == -1; any non-negative id should be accepted.
    Term result = nsf.proposeMaxCandidateTermId(0, 0, 0);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(nsf.candidateTermId(), 0);
}

// ---------------------------------------------------------------------------
// 12. proposeMaxCandidateTermId is idempotent when called with same args
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ProposeSameArgsTwiceIdempotent) {
    TempNsf t("_idem");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(4, 80, 800);
    nsf.proposeMaxCandidateTermId(4, 80, 800);

    EXPECT_EQ(nsf.candidateTermId(),          4);
    EXPECT_EQ(nsf.candidateTermLogPosition(), 80);
    EXPECT_EQ(nsf.candidateTermTimestamp(),   800);
}

// ---------------------------------------------------------------------------
// 13. fileSyncLevel=0 default — still persists (no msync, but page is dirty)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, FileSyncLevel0StillPersistsOnReopen) {
    TempNsf t("_sync0");
    {
        NodeStateFile nsf(t.path.string(), /*fileSyncLevel=*/0);
        nsf.proposeMaxCandidateTermId(3, 30, 300);
    }
    {
        NodeStateFile nsf2(t.path.string(), /*fileSyncLevel=*/0);
        EXPECT_EQ(nsf2.candidateTermId(), 3);
    }
}

// ---------------------------------------------------------------------------
// 14. Large term IDs (near INT64_MAX / 2)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, LargeTermIds) {
    TempNsf t("_large");
    constexpr int64_t big = static_cast<int64_t>(1) << 40;
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(big, big + 1, big + 2);

    EXPECT_EQ(nsf.candidateTermId(),          big);
    EXPECT_EQ(nsf.candidateTermLogPosition(), big + 1);
    EXPECT_EQ(nsf.candidateTermTimestamp(),   big + 2);
}

// ---------------------------------------------------------------------------
// 15. Multiple reopen/propose cycles maintain monotonicity
// ---------------------------------------------------------------------------
TEST(NodeStateFile, MultipleReopenCyclesMaintainMonotonicity) {
    TempNsf t("_cycles");

    Term last = NodeStateFile::NULL_VALUE;
    for (Term id : {Term{1}, Term{3}, Term{2}, Term{5}, Term{4}, Term{7}}) {
        {
            NodeStateFile nsf(t.path.string());
            nsf.proposeMaxCandidateTermId(id, id * 10, id * 100);
        }
        {
            NodeStateFile nsf2(t.path.string());
            last = std::max(last, id);
            EXPECT_EQ(nsf2.candidateTermId(), last);
        }
    }
    // Final max should be 7
    NodeStateFile final_nsf(t.path.string());
    EXPECT_EQ(final_nsf.candidateTermId(), 7);
}

// ---------------------------------------------------------------------------
// 16. NULL_VALUE sentinel is exactly -1
// ---------------------------------------------------------------------------
TEST(NodeStateFile, NullValueSentinelIsMinusOne) {
    EXPECT_EQ(NodeStateFile::NULL_VALUE, int64_t{-1});
}

// ---------------------------------------------------------------------------
// 17. Read candidateTermId immediately after proposeMaxCandidateTermId
//     returns the proposed value (acquire fence path exercised)
// ---------------------------------------------------------------------------
TEST(NodeStateFile, ReadAfterProposeAcquireFence) {
    TempNsf t("_acquire_fence");
    NodeStateFile nsf(t.path.string());
    nsf.proposeMaxCandidateTermId(42, 420, 4200);

    // The acquire-load must see 42 even without any synchronization between
    // the write and the read (same thread, but the fence must be correct).
    EXPECT_EQ(nsf.candidateTermId(), 42);
}
