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
#include <rune/node/WorkerViewSet.h>
#include <rune/consensus/NanoClock.h>

#include <rune/view/ViewCheckpoint.h>
#include <rune/view/ViewTable.h>
#include <rune/view/ViewRow.h>
#include <rune/codec/Descriptor.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <tuple>
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


// ===========================================================================
// View checkpoint backups (batch 1, C1): the discard frame moves the live file into backup/ before anything is rebuilt.
// ===========================================================================
namespace {

using rune::node::ViewBackupPolicy;
using rune::node::ViewDiscardReason;

std::string committedPath(const std::string& dir, int32_t view, int32_t part) {
    return rune::node::viewCheckpointFilePath(dir, view, part);
}
std::string legacyPath(const std::string& dir, int32_t view, int32_t part) {
    return rune::node::legacyViewCheckpointFilePath(dir, view, part);
}

std::vector<uint8_t> frameDiscard(int32_t view, int32_t part, ViewDiscardReason reason) {
    std::vector<uint8_t> f(rune::node::kDiscardMsgBytes, 0);
    for (int i = 0; i < 4; ++i) f[i]     = static_cast<uint8_t>(static_cast<uint32_t>(view) >> (i * 8));
    for (int i = 0; i < 4; ++i) f[4 + i] = static_cast<uint8_t>(static_cast<uint32_t>(part) >> (i * 8));
    f[8] = static_cast<uint8_t>(reason);
    return f;
}

struct DoneMsg { int32_t type = 0, view = 0, part = 0, status = 0; };

// Reads at most one kDiscardDoneMsgType frame off `ch.done` (kDoneMsgType acks are skipped).
bool takeDiscardDone(rune::node::ViewSnapshotChannel& ch, DoneMsg& out) {
    bool got = false;
    ch.done.drain([&](int32_t type, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len) {
        if (type != rune::node::kDiscardDoneMsgType || got || len < rune::node::kDiscardDoneMsgBytes) return;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        auto rd = [&](int o) { return static_cast<int32_t>(p[o] | (p[o + 1] << 8) | (p[o + 2] << 16) | (static_cast<uint32_t>(p[o + 3]) << 24)); };
        out = DoneMsg{type, rd(0), rd(4), rd(8)};
        got = true;
    }, 64);
    return got;
}

// Drives the agent (the clock advancing past the fsync window every pass) until a discard ack arrives or kBackupCap passes.
constexpr int kBackupCap = 32;
bool pollDone(rune::node::ViewSnapshotChannel& ch, rune::node::ViewSnapshotIoAgent& agent, rune::consensus::ManualNanoClock& clk, DoneMsg& out) {
    for (int i = 0; i < kBackupCap; ++i) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
        if (takeDiscardDone(ch, out)) return true;
    }
    return false;
}

void sendDiscard(rune::node::ViewSnapshotChannel& ch, int32_t view, int32_t part, ViewDiscardReason r) {
    const auto f = frameDiscard(view, part, r);
    ASSERT_TRUE(ch.deltas.write(f.data(), f.size(), rune::node::kDiscardMsgType));
}

// Appends a delta (hp at offset O) through the agent and drives until it is durable.
void appendDurable(rune::node::ViewSnapshotChannel& ch, rune::node::ViewSnapshotIoAgent& agent, rune::consensus::ManualNanoClock& clk,
                   int32_t view, int32_t part, int64_t O, int32_t hp, const RowLayout& layout) {
    const auto f = frameDelta(view, part, O, hp, layout);
    ASSERT_TRUE(ch.deltas.write(f.data(), f.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));
    int i = 0;
    for (; i < kBackupCap && agent.lastDurableOffset(view, part) < O; ++i) {
        agent.doWork(clk.nowNs());
        clk.advance(60LL * 1'000'000LL);
    }
    ASSERT_LT(i, kBackupCap) << "delta never became durable";
}

constexpr int32_t kBView = 7;

} // namespace

TEST(ViewBackupRegression, DiscardMovesTheCommittedFileIntoBackupRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), /*fsyncMs=*/0);
    rune::consensus::ManualNanoClock clk(0);
    const RowLayout layout = testLayout();
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, layout);
    appendDurable(ch, agent, clk, kBView, 0, 20, 2, layout);
    const auto F = readFile(committedPath(dir.path(), kBView, 0));
    ASSERT_FALSE(F.empty());

    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    DoneMsg d;
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.type, rune::node::kDiscardDoneMsgType);
    EXPECT_EQ(d.view, kBView);
    EXPECT_EQ(d.part, 0);
    EXPECT_EQ(d.status, 0);
    EXPECT_FALSE(fs::exists(committedPath(dir.path(), kBView, 0)));
    const auto backups = rune::node::listViewBackups(dir.path(), kBView, 0);
    ASSERT_EQ(backups.size(), 1u);
    EXPECT_EQ(fs::path(backups[0]).filename().string(), "view-7-p0.0000000001.unreadable.delta");
    EXPECT_EQ(readFile(backups[0]), F);

    // A later delta starts a fresh file: exactly its own frame, no stale bytes.
    appendDurable(ch, agent, clk, kBView, 0, 30, 3, layout);
    const auto f3 = frameDelta(kBView, 0, 30, 3, layout);
    const std::vector<uint8_t> delta3(f3.begin() + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes, f3.end());
    EXPECT_EQ(readFile(committedPath(dir.path(), kBView, 0)), delta3);
}

TEST(ViewBackupRegression, KnobOffRemovesWithoutBackupRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0, ViewBackupPolicy{false, 3});
    rune::consensus::ManualNanoClock clk(0);
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, testLayout());
    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    DoneMsg d;
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.status, 0);
    EXPECT_FALSE(fs::exists(committedPath(dir.path(), kBView, 0)));
    EXPECT_FALSE(fs::exists(rune::node::viewBackupDir(dir.path()))) << "backups are off: no backup dir";
}

TEST(ViewBackupRegression, BackupFailureAcksErrnoAndKeepsTheLiveFileRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0);
    rune::consensus::ManualNanoClock clk(0);
    const RowLayout layout = testLayout();
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, layout);
    appendDurable(ch, agent, clk, kBView + 1, 0, 10, 1, layout);
    const auto F = readFile(committedPath(dir.path(), kBView, 0));
    { std::ofstream blocker(rune::node::viewBackupDir(dir.path()), std::ios::binary); blocker << "not a directory"; }

    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    DoneMsg d;
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.status, ENOTDIR);
    EXPECT_EQ(readFile(committedPath(dir.path(), kBView, 0)), F) << "a failed backup never touches the live file";
    EXPECT_FALSE(agent.hasError(kBView + 1, 0));
}

TEST(ViewBackupRegression, RetentionKeepsTheNewestAcrossAgentRestartsRegression) {
    TempDir dir;
    const RowLayout layout = testLayout();
    std::vector<uint8_t> cycle3;
    // Cycles 1-2 on one agent, cycle 3 on a new one (a cold sequence rescan of the backup dir).
    auto runCycles = [&](int from, int to) {
        rune::node::ViewSnapshotChannel ch;
        rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0, ViewBackupPolicy{true, 2});
        rune::consensus::ManualNanoClock clk(0);
        for (int cycle = from; cycle <= to; ++cycle) {
            appendDurable(ch, agent, clk, kBView, 0, 10 * cycle, cycle, layout);
            if (cycle == 3) cycle3 = readFile(committedPath(dir.path(), kBView, 0));
            sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
            DoneMsg d;
            ASSERT_TRUE(pollDone(ch, agent, clk, d));
            ASSERT_EQ(d.status, 0);
        }
    };
    runCycles(1, 2);
    ASSERT_EQ(rune::node::listViewBackups(dir.path(), kBView, 0).size(), 2u);
    runCycles(3, 3);
    const auto backups = rune::node::listViewBackups(dir.path(), kBView, 0);
    ASSERT_EQ(backups.size(), 2u);
    EXPECT_EQ(fs::path(backups[0]).filename().string(), "view-7-p0.0000000002.unreadable.delta");
    EXPECT_EQ(fs::path(backups[1]).filename().string(), "view-7-p0.0000000003.unreadable.delta");
    EXPECT_EQ(readFile(backups[1]), cycle3);
}

TEST(ViewBackupRegression, DiscardOfAnAbsentFileAcksOkIdempotentRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0);
    rune::consensus::ManualNanoClock clk(0);
    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    DoneMsg d;
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.status, 0);
    EXPECT_TRUE(rune::node::listViewBackups(dir.path(), kBView, 0).empty());

    // Crash replay: a real discard, acked, then the same frame again.
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, testLayout());
    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    ASSERT_EQ(d.status, 0);
    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.status, 0);
    EXPECT_EQ(rune::node::listViewBackups(dir.path(), kBView, 0).size(), 1u);
}

TEST(ViewBackupRegression, LegacyReasonTargetsTheLegacyFileRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0);
    rune::consensus::ManualNanoClock clk(0);
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, testLayout());
    const auto C = readFile(committedPath(dir.path(), kBView, 0));
    const std::vector<uint8_t> L = {'l', 'e', 'g', 'a', 'c', 'y', 1, 2, 3};
    { std::ofstream o(legacyPath(dir.path(), kBView, 0), std::ios::binary); o.write(reinterpret_cast<const char*>(L.data()), L.size()); }

    sendDiscard(ch, kBView, 0, ViewDiscardReason::LegacySuperseded);
    DoneMsg d;
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.status, 0);
    EXPECT_FALSE(fs::exists(legacyPath(dir.path(), kBView, 0)));
    const auto backups = rune::node::listViewBackups(dir.path(), kBView, 0);
    ASSERT_EQ(backups.size(), 1u);
    EXPECT_EQ(fs::path(backups[0]).filename().string(), "view-7-p0.0000000001.legacy-superseded.delta");
    EXPECT_EQ(readFile(backups[0]), L);
    EXPECT_EQ(readFile(committedPath(dir.path(), kBView, 0)), C) << "the committed file is untouched";
}

TEST(ViewBackupRegression, ErroredStreamIsClearedByDiscardRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0);
    rune::consensus::ManualNanoClock clk(0);
    agent.forceErroredForTest(kBView, 0);
    ASSERT_TRUE(agent.hasError(kBView, 0));
    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    DoneMsg d;
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    ASSERT_EQ(d.status, 0);
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, testLayout());
    EXPECT_FALSE(agent.hasError(kBView, 0));
    EXPECT_EQ(agent.lastDurableOffset(kBView, 0), 10);
}

TEST(ViewBackupRegression, FullDoneRingDefersTheDiscardAckRegression) {
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0);
    rune::consensus::ManualNanoClock clk(0);
    appendDurable(ch, agent, clk, kBView, 0, 10, 1, testLayout());
    // Nobody drains `done`: fill it with durable-offset acks.
    uint8_t ack[rune::node::ViewSnapshotChannel::kDoneMsgBytes] = {};
    int filled = 0;
    while (ch.done.write(ack, sizeof(ack), rune::node::ViewSnapshotChannel::kDoneMsgType)) {
        ASSERT_LT(++filled, 100000);
    }
    sendDiscard(ch, kBView, 0, ViewDiscardReason::Unreadable);
    for (int i = 0; i < 4; ++i) { agent.doWork(clk.nowNs()); clk.advance(60LL * 1'000'000LL); }
    EXPECT_EQ(agent.pendingAcksForTest(), 1u) << "the ack is held, not dropped";
    DoneMsg d;
    EXPECT_FALSE(takeDiscardDone(ch, d)) << "no room for it yet: drained frames were all kDoneMsgType";
    // Drain the ring (every ack was a kDoneMsgType filler, plus none for the discard) and drive: the ack arrives.
    ASSERT_TRUE(pollDone(ch, agent, clk, d));
    EXPECT_EQ(d.status, 0);
    EXPECT_EQ(agent.pendingAcksForTest(), 0u);
}

TEST(ViewBackupRegression, DoneRingIsDrainedByTheWorkerSoAcksDoNotAccumulateRegression) {
    // Production had no consumer of `done`, so once its 4 KiB filled every later ack piled up in pendingAcks_ forever.  Start with
    // the ring nearly full of unconsumed acks (the state after ~170 flushes), then flush 40 more times: the worker's drain keeps
    // pendingAcks_ at 0.  (Each flush costs a real fsync, so the fill is simulated instead of repeating it 170 times.)
    TempDir dir;
    rune::node::ViewSnapshotChannel ch;
    rune::node::ViewSnapshotIoAgent agent(ch, dir.path(), 0);
    rune::node::WorkerViewSet vs(ch, dir.path());
    rune::consensus::ManualNanoClock clk(0);
    const RowLayout layout = testLayout();
    uint8_t filler[rune::node::ViewSnapshotChannel::kDoneMsgBytes] = {};
    int filled = 0;
    while (ch.done.write(filler, sizeof(filler), rune::node::ViewSnapshotChannel::kDoneMsgType)) ASSERT_LT(++filled, 100000);
    ASSERT_EQ(vs.drainSnapshotAcks(filled - 5), filled - 5);   // 5 slots free, 5 fillers left
    for (int i = 1; i <= 40; ++i) {
        const auto f = frameDelta(kBView, 0, i, i, layout);
        ASSERT_TRUE(ch.deltas.write(f.data(), f.size(), rune::node::ViewSnapshotChannel::kDeltaMsgType));
        agent.doWork(clk.nowNs());   // appends
        clk.advance(60LL * 1'000'000LL);
        agent.doWork(clk.nowNs());   // fsync + ack
        vs.drainSnapshotAcks();
    }
    EXPECT_EQ(agent.lastDurableOffset(kBView, 0), 40);
    EXPECT_EQ(agent.pendingAcksForTest(), 0u);
}
