// ViewSnapshotIoAgentTest.cpp — Task 13: ViewSnapshotChannel + ViewSnapshotIoAgent.
//
// Deterministic (CLAUDE.md §4/§5): time comes ONLY from an injected
// rune::consensus::ManualNanoClock (the codebase's actual deterministic
// NanoClock impl -- NanoClock.h; no "CachedNanoClock" type exists in this
// repo). The test advances the clock and calls doWork() once per expected
// transition, under a hard iteration cap -- never a wall-clock sleep, never
// an unbounded while(cond) doWork() loop.

#include <gtest/gtest.h>

#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/consensus/NanoClock.h>

#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace rune::view;
using rune::codec::DescriptorBuilder;
using rune::codec::PrimType;
using rune::codec::FieldValue;

namespace {

// Self-contained temp directory (no shared test-only helper exists in
// libs/node/tests/node yet) -- removed recursively on destruction.
struct TempDir {
    fs::path dir;
    TempDir() {
        dir = fs::temp_directory_path() /
              ("rune_view_snapshot_io_test_" + std::to_string(static_cast<unsigned long long>(
                  reinterpret_cast<uintptr_t>(this))));
        fs::create_directories(dir);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(dir, ec); }
    [[nodiscard]] std::string path() const { return dir.string(); }
};

RowLayout testLayout() {
    return buildRowLayout(DescriptorBuilder("v", 1)
        .add("id", PrimType::Int64, 1, true)
        .add("hp", PrimType::Int32)
        .build());
}

void setHp(uint8_t* row, const RowLayout& layout, int32_t hp) {
    FieldValue v{};
    v.type = PrimType::Int32;
    v.present = true;
    v.i = hp;
    writeSlot(row, layout, 1, v);
}

// Encodes one upsert delta (key=1, hp=hp) at offset O, then frames it for
// ViewSnapshotChannel::deltas: [viewTopicId i32][partition i32][delta bytes].
std::vector<uint8_t> frameDelta(int32_t viewTopicId, int32_t partition, int64_t O,
                                 int32_t hp, const RowLayout& layout) {
    ViewTable scratch(layout);
    uint8_t* row = scratch.put(1);
    setHp(row, layout, hp);

    DirtyEntry entry{/*rid=*/0, /*deleted=*/false, /*key=*/1, /*row=*/scratch.get(1)};
    std::vector<uint8_t> deltaBytes;
    const size_t written = encodeDelta(std::span<const DirtyEntry>(&entry, 1), O, layout.rowLen, deltaBytes);
    EXPECT_GT(written, 0u);

    std::vector<uint8_t> frame(rune::node::ViewSnapshotChannel::kDeltaHeaderBytes + deltaBytes.size());
    uint8_t* p = frame.data();
    for (int i = 0; i < 4; ++i) p[i]     = static_cast<uint8_t>(static_cast<uint32_t>(viewTopicId) >> (i * 8));
    for (int i = 0; i < 4; ++i) p[4 + i] = static_cast<uint8_t>(static_cast<uint32_t>(partition)   >> (i * 8));
    std::memcpy(p + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes, deltaBytes.data(), deltaBytes.size());
    return frame;
}

std::vector<uint8_t> readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

constexpr int32_t kViewTopicId = 0x40000007;
constexpr int32_t kPartition   = 0;
constexpr int kHardCap         = 4;

} // namespace

TEST(ViewSnapshotIoAgent, AppendsDeltaAndGroupsFsync) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs=*/50);
    rune::consensus::ManualNanoClock clk(0);

    const RowLayout layout = testLayout();
    const auto frame = frameDelta(kViewTopicId, kPartition, /*O=*/10, /*hp=*/42, layout);
    ASSERT_TRUE(ch.deltas.write(frame.data(), frame.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));

    int iterations = 0;
    for (; iterations < kHardCap && agent.lastDurableOffset(kViewTopicId, kPartition) < 10; ++iterations) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL); // 60ms, past the 50ms fsync window
    }
    ASSERT_LT(iterations, kHardCap) << "did not converge within the hard iteration cap";
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), 10);

    // The delta file exists on disk and CRC-validates: reopen + replay it
    // via Task 12's applyDelta() into a fresh table.
    const std::string path = agent.deltaFilePathForTest(kViewTopicId, kPartition);
    ASSERT_TRUE(fs::exists(path));
    const auto bytes = readFile(path);
    ASSERT_FALSE(bytes.empty());

    ViewTable replayed(layout);
    auto applied = applyDelta(replayed, layout, bytes);
    ASSERT_TRUE(applied.has_value());
    EXPECT_EQ(applied->O, 10);
    EXPECT_EQ(applied->count, 1u);
    const uint8_t* row = replayed.get(1);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(readSlot(row, layout, 1).i, 42);
}

TEST(ViewSnapshotIoAgentRegression, DurableOffsetDoesNotAdvanceBeforeFsyncWindow) {
    // Offset-atomic (CLAUDE.md §3/§7 regression gate): lastDurableOffset must
    // stay at its PRIOR value until bytes are actually fdatasync'd -- never
    // advance merely because the bytes were appended/write()'d.
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs=*/50);
    rune::consensus::ManualNanoClock clk(0);

    const RowLayout layout = testLayout();
    const auto frame = frameDelta(kViewTopicId, kPartition, /*O=*/10, /*hp=*/7, layout);
    ASSERT_TRUE(ch.deltas.write(frame.data(), frame.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));

    // One doWork() call at the SAME instant the delta landed: the bytes are
    // appended to the file, but fsyncMs has not elapsed -- durable offset
    // must remain -1 (unknown/never-synced), proving the append and the
    // fsync are genuinely decoupled.
    agent.doWork(clk.nowNs());
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), -1);

    // Advancing past the window and pumping once more makes it durable.
    clk.advance(60LL * 1'000'000LL);
    agent.doWork(clk.nowNs());
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), 10);
}

// Review round 1, IMPORTANT 2: a FATAL write failure must (a) never advance
// lastDurableOffset past the failed delta, (b) surface via hasError(), and
// (c) permanently stop the stream (no later delta for the same key is ever
// appended again -- a torn record with no resync marker would otherwise
// silently misalign every later record's CRC on replay).
TEST(ViewSnapshotIoAgentRegression, FatalWriteFailureStopsStreamAndNeverAdvancesOffset) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs=*/50);
    rune::consensus::ManualNanoClock clk(0);
    const RowLayout layout = testLayout();

    // First delta lands and becomes durable normally.
    auto frame1 = frameDelta(kViewTopicId, kPartition, /*O=*/10, /*hp=*/1, layout);
    ASSERT_TRUE(ch.deltas.write(frame1.data(), frame1.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));
    for (int i = 0; i < kHardCap && agent.lastDurableOffset(kViewTopicId, kPartition) < 10; ++i) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }
    ASSERT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), 10);
    ASSERT_FALSE(agent.hasError(kViewTopicId, kPartition));

    // Force the NEXT write for this key to fail deterministically (reopens
    // the fd O_RDONLY -- a portable stand-in for a real short/failed write).
    agent.forceWriteFailureForTest(kViewTopicId, kPartition);

    auto frame2 = frameDelta(kViewTopicId, kPartition, /*O=*/20, /*hp=*/2, layout);
    ASSERT_TRUE(ch.deltas.write(frame2.data(), frame2.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));
    for (int i = 0; i < kHardCap; ++i) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }

    // The failed delta's O never becomes durable; the error is surfaced.
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), 10);
    EXPECT_TRUE(agent.hasError(kViewTopicId, kPartition));

    // A THIRD delta for the SAME key, pushed after the error, must also be
    // rejected -- no further appends to a permanently-stopped stream.
    auto frame3 = frameDelta(kViewTopicId, kPartition, /*O=*/30, /*hp=*/3, layout);
    ASSERT_TRUE(ch.deltas.write(frame3.data(), frame3.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));
    for (int i = 0; i < kHardCap; ++i) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), 10);

    // What IS on disk is still exactly the first, fully-durable record --
    // replay recovers precisely up to it (CRC-validated), nothing more.
    const std::string path = agent.deltaFilePathForTest(kViewTopicId, kPartition);
    const auto bytes = readFile(path);
    ViewTable replayed(layout);
    auto applied = applyDelta(replayed, layout, bytes);
    ASSERT_TRUE(applied.has_value());
    EXPECT_EQ(applied->O, 10);
}

// Review round 1, IMPORTANT 1: a transient ::open() failure must be retried,
// never permanently cached. Uses a FILE sitting where the view-data
// directory should be, so every ::open() of the per-key delta file fails
// with ENOTDIR until the obstruction is removed -- a portable stand-in for a
// real transient failure (fd-ulimit, momentary ENOSPC/EACCES, ...).
TEST(ViewSnapshotIoAgentRegression, OpenFailureIsRetriedNotPermanentlyBlackholed) {
    TempDir parent;
    const std::string badDir = parent.path() + "/viewdata";
    {
        std::ofstream blocker(badDir, std::ios::binary);
        blocker << "not a directory";
    }
    ASSERT_TRUE(fs::exists(badDir));
    ASSERT_FALSE(fs::is_directory(badDir));

    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, badDir, /*fsyncMs=*/50);
    rune::consensus::ManualNanoClock clk(0);
    const RowLayout layout = testLayout();

    auto frame1 = frameDelta(kViewTopicId, kPartition, /*O=*/10, /*hp=*/1, layout);
    ASSERT_TRUE(ch.deltas.write(frame1.data(), frame1.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));

    agent.doWork(clk.nowNs()); // ::open() fails (ENOTDIR) -- must NOT be cached
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), -1);
    EXPECT_FALSE(agent.hasError(kViewTopicId, kPartition)) << "an open failure is transient, not FATAL";
    EXPECT_GE(agent.openFailureCount(), 1) << "a persistent open failure must be observable, not silent";
    clk.advance(60LL * 1'000'000LL);

    // Clear the obstruction: remove the blocking file, create the real dir.
    ASSERT_TRUE(fs::remove(badDir));
    fs::create_directories(badDir);

    // Re-push the same delta (stand-in for a real worker's retry-on-back-
    // pressure) and prove the key is NOT blackholed: it converges normally.
    auto frame1b = frameDelta(kViewTopicId, kPartition, /*O=*/10, /*hp=*/1, layout);
    ASSERT_TRUE(ch.deltas.write(frame1b.data(), frame1b.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));
    for (int i = 0; i < kHardCap && agent.lastDurableOffset(kViewTopicId, kPartition) < 10; ++i) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }
    EXPECT_EQ(agent.lastDurableOffset(kViewTopicId, kPartition), 10);
    EXPECT_FALSE(agent.hasError(kViewTopicId, kPartition));
}

// REGRESSION (Phase 2a): an aggregate view's checkpoint is a v2 multi-table
// record. The agent used to drop any non-v1 magic, so aggregate state never
// reached disk and silently reset on restart.
TEST(ViewSnapshotIoAgentRegression, MultiTableDeltaIsPersistedNotDropped) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs=*/50);
    rune::consensus::ManualNanoClock clk(0);

    uint8_t row[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    const DirtyEntry e{0, false, 5, row};
    const TableDelta tds[] = {{0, 8, {&e, 1}}, {1, 8, {&e, 1}}};
    std::vector<uint8_t> delta;
    encodeMultiDelta(tds, /*O=*/12, delta);
    std::vector<uint8_t> frame(rune::node::ViewSnapshotChannel::kDeltaHeaderBytes + delta.size());
    for (int i = 0; i < 4; ++i) frame[i]     = uint8_t(uint32_t(kViewTopicId) >> (i * 8));
    for (int i = 0; i < 4; ++i) frame[4 + i] = uint8_t(uint32_t(kPartition) >> (i * 8));
    std::memcpy(frame.data() + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes, delta.data(), delta.size());
    ASSERT_TRUE(ch.deltas.write(frame.data(), frame.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));

    int it = 0;
    for (; it < kHardCap && agent.lastDurableOffset(kViewTopicId, kPartition) < 12; ++it) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }
    ASSERT_LT(it, kHardCap) << "v2 delta was dropped: durable offset never reached 12";
    EXPECT_EQ(readFile(agent.deltaFilePathForTest(kViewTopicId, kPartition)), delta);
}
