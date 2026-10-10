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
