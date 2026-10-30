// CheckedWritesMinFoldCursorTest.cpp - checked writes Part 03 Task 8: WorkerViewSet::minFoldCursor.
#include <rune/codec/Descriptor.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/PlanCompiler.h>

#include <gtest/gtest.h>

#include <climits>
#include <filesystem>
#include <string>
#include <unistd.h>

namespace {

constexpr int32_t kSrc = 5;

rune::codec::Descriptor charSourceV1() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
}

rune::view::ViewPlan hpView(const char* name) {
    rune::view::ViewPlan v;
    v.viewName    = name;
    v.key         = "characterId";
    v.sources     = {{kSrc, "characterId"}};
    v.output      = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                     {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    return v;
}

// [blockLength u16][version u16][id i64][hp i32]
std::vector<uint8_t> character(int64_t id, int32_t hp) {
    std::vector<uint8_t> b;
    auto le = [&](uint64_t v, int n) { for (int i = 0; i < n; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i))); };
    le(12, 2); le(1, 2); le(static_cast<uint64_t>(id), 8); le(static_cast<uint32_t>(hp), 4);
    return b;
}

// A batch of `n` upserts at offsets [0, n).
std::vector<uint8_t> upserts(int n) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    for (int i = 0; i < n; ++i) {
        std::string key(8, '\0');
        key[0] = static_cast<char>(i + 1);
        const auto v = character(i + 1, 10);
        b.append(key, std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0);
    }
    return b.build();
}

struct Set {
    rune::node::ViewSnapshotChannel channel;
    std::string dir = (std::filesystem::temp_directory_path() / ("rune-cw-mfc-" + std::to_string(::getpid()))).string();
    rune::node::WorkerViewSet vs{channel, dir};
    rune::codec::Descriptor src = charSourceV1();
    ~Set() { std::error_code ec; std::filesystem::remove_all(dir, ec); }

    void install(const char* name, int32_t viewTopicId) {
        const rune::codec::Descriptor* srcs[] = {&src};
        auto comp = rune::view::compileView(hpView(name), srcs, viewTopicId).value();
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);
    }
    // Folds [cursor, bound) of (kSrc, partition) from a two-record batch.
    void fold(int32_t partition, int64_t bound, uint64_t leaderGen = 0) {
        const auto batch = upserts(2);
        auto readRange = [&](int64_t, int64_t) { return rune::storage::LogReadView{batch.data(), batch.size(), 0, 2}; };
        vs.driveFolds(kSrc, partition, readRange, bound, rune::view::kNoSkip, 0, nullptr, leaderGen);
    }
};

}  // namespace

TEST(CheckedWritesMinFoldCursor, NoInterestedViewIsInt64Max) {
    Set s;
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0), INT64_MAX);
    s.install("a", 0x40000001);
    EXPECT_EQ(s.vs.minFoldCursor(6, 0), INT64_MAX);
}

TEST(CheckedWritesMinFoldCursor, InstalledButNeverFoldedCountsZero) {
    Set s;
    s.install("a", 0x40000001);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0), 0);
}

TEST(CheckedWritesMinFoldCursor, TracksTheSlowestView) {
    Set s;
    s.install("a", 0x40000001);
    s.install("b", 0x40000002);
    // Both views fold in one driveFolds pass; make b lag by installing it after a has folded.
    s.fold(0, 2);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0), 2);
    Set t;
    t.install("a", 0x40000001);
    t.fold(0, 2);
    t.install("b", 0x40000002);
    EXPECT_EQ(t.vs.minFoldCursor(kSrc, 0), 0) << "b has no instance yet";
    t.fold(0, 2);
    EXPECT_EQ(t.vs.minFoldCursor(kSrc, 0), 2);
}

TEST(CheckedWritesMinFoldCursor, PartitionsAreIndependent) {
    Set s;
    s.install("a", 0x40000001);
    s.fold(0, 2);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0), 2);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 1), 0);
}

TEST(CheckedWritesMinFoldCursorRegression, OtherLeadershipCountsZeroRegression) {
    Set s;
    s.install("a", 0x40000001);
    s.fold(0, 2, /*leaderGen=*/3);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0, 3), 2);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0, 4), 0);
    EXPECT_EQ(s.vs.minFoldCursor(kSrc, 0), 2);
}
