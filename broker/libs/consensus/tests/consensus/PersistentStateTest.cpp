#include <gtest/gtest.h>
#include <rune/consensus/PersistentState.h>
#include <filesystem>
#include <stdexcept>

using namespace rune::consensus;
namespace fs = std::filesystem;

/// RAII helper: unique temp file path, removed on destruction.
struct TempFile {
    fs::path path;
    explicit TempFile(std::string_view suffix) {
        path = fs::temp_directory_path() /
               ("rune_ps_test_" + std::to_string(
                   std::chrono::steady_clock::now().time_since_epoch().count()) +
                std::string(suffix));
        fs::remove(path); // ensure clean start
    }
    ~TempFile() {
        std::error_code ec;
        fs::remove(path, ec);
        fs::remove(fs::path(path.string() + ".tmp"), ec);
    }
};

#include <chrono>

// ---------------------------------------------------------------------------
TEST(PersistentState, DefaultsOnFreshPath) {
    TempFile tmp(".bin");
    PersistentState ps(tmp.path.string());
    EXPECT_EQ(ps.currentTerm(), 0);
    EXPECT_EQ(ps.votedFor(),    kNoVote);
}

// ---------------------------------------------------------------------------
TEST(PersistentState, SetCurrentTermPersistedAcrossReopen) {
    TempFile tmp(".bin");
    {
        PersistentState ps(tmp.path.string());
        ps.setCurrentTerm(7);
        EXPECT_EQ(ps.currentTerm(), 7);
        EXPECT_EQ(ps.votedFor(),    kNoVote); // term advance resets vote
    }
    {
        PersistentState ps2(tmp.path.string());
        EXPECT_EQ(ps2.currentTerm(), 7);
        EXPECT_EQ(ps2.votedFor(),    kNoVote);
    }
}

// ---------------------------------------------------------------------------
TEST(PersistentState, SetVotedForPersistedAcrossReopen) {
    TempFile tmp(".bin");
    {
        PersistentState ps(tmp.path.string());
        ps.setCurrentTerm(3);
        ps.setVotedFor(42);
        EXPECT_EQ(ps.votedFor(), 42);
    }
    {
        PersistentState ps2(tmp.path.string());
        EXPECT_EQ(ps2.currentTerm(), 3);
        EXPECT_EQ(ps2.votedFor(),    42);
    }
}

// ---------------------------------------------------------------------------
TEST(PersistentState, SetCurrentTermResetsVotedFor) {
    TempFile tmp(".bin");
    PersistentState ps(tmp.path.string());
    ps.setCurrentTerm(5);
    ps.setVotedFor(99);
    EXPECT_EQ(ps.votedFor(), 99);

    // Advance term — vote must be cleared
    ps.setCurrentTerm(6);
    EXPECT_EQ(ps.currentTerm(), 6);
    EXPECT_EQ(ps.votedFor(),    kNoVote);
}

// ---------------------------------------------------------------------------
TEST(PersistentState, SetCurrentTermResetVotedForSurvivesReopen) {
    TempFile tmp(".bin");
    {
        PersistentState ps(tmp.path.string());
        ps.setCurrentTerm(5);
        ps.setVotedFor(99);
        ps.setCurrentTerm(6); // must clear vote and persist
    }
    {
        PersistentState ps2(tmp.path.string());
        EXPECT_EQ(ps2.currentTerm(), 6);
        EXPECT_EQ(ps2.votedFor(),    kNoVote);
    }
}

// ---------------------------------------------------------------------------
TEST(PersistentState, UpdateSetsBothAtomicallyAndSurvivesReopen) {
    TempFile tmp(".bin");
    {
        PersistentState ps(tmp.path.string());
        ps.update(10, 7);
        EXPECT_EQ(ps.currentTerm(), 10);
        EXPECT_EQ(ps.votedFor(),    7);
    }
    {
        PersistentState ps2(tmp.path.string());
        EXPECT_EQ(ps2.currentTerm(), 10);
        EXPECT_EQ(ps2.votedFor(),    7);
    }
}

// ---------------------------------------------------------------------------
TEST(PersistentState, SameTermNoOpOnSetCurrentTerm) {
    TempFile tmp(".bin");
    PersistentState ps(tmp.path.string());
    ps.setCurrentTerm(3);
    ps.setVotedFor(5);
    ps.setCurrentTerm(3); // same term — should be a no-op, vote unchanged
    EXPECT_EQ(ps.currentTerm(), 3);
    EXPECT_EQ(ps.votedFor(),    5);
}

// ---------------------------------------------------------------------------
TEST(PersistentState, ThrowsOnDecreasingTerm) {
    TempFile tmp(".bin");
    PersistentState ps(tmp.path.string());
    ps.setCurrentTerm(5);
    EXPECT_THROW(ps.setCurrentTerm(4), std::invalid_argument);
    EXPECT_THROW(ps.update(3, 1),      std::invalid_argument);
}

// ---------------------------------------------------------------------------
TEST(PersistentState, MultipleUpdates) {
    TempFile tmp(".bin");
    PersistentState ps(tmp.path.string());
    for (Term t = 1; t <= 20; ++t) {
        ps.setCurrentTerm(t);
        ps.setVotedFor(static_cast<NodeId>(t * 10));
    }
    EXPECT_EQ(ps.currentTerm(), 20);
    EXPECT_EQ(ps.votedFor(),    200);

    PersistentState ps2(tmp.path.string());
    EXPECT_EQ(ps2.currentTerm(), 20);
    EXPECT_EQ(ps2.votedFor(),    200);
}
