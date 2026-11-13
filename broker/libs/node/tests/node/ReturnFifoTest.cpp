// ReturnFifoTest.cpp - #987: the worker return FIFO's chunk byte ring.  A chunk-carrying answer queued under back-
// pressure is copied into a preallocated byte ring in queue order and freed in the same order; no allocation.
#include <rune/node/ReturnFifo.h>

#include <gtest/gtest.h>

#include <deque>
#include <random>
#include <vector>

using namespace rune::node;

namespace {
constexpr std::size_t kArena = 4096;   // a small ring: the wrap-around and the full case are exercised constantly

std::vector<uint8_t> bytesOf(std::mt19937& rng, std::size_t n, int64_t tag) {
    std::vector<uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<uint8_t>(rng() ^ static_cast<uint32_t>(tag * 31 + int64_t(i)));
    return v;
}
WorkerReturn answer(int64_t tag) {
    WorkerReturn r;
    r.type = WorkerReturn::Type::LeaseKeepAliveResult;
    r.correlationId = tag;
    return r;
}
}  // namespace

// Chunks leave in FIFO order, byte-exact, across 100 000 push / pop operations with seeded sizes (wrap-around
// included). Mutation m2 (the arena free is not advanced on pop): the ring fills up and every later push is ChunkFull
// -> red.
TEST(ReturnFifoChunkRegression, ChunksSurviveWrapAroundByteExactInOrderRegression) {
    ReturnFifo<64> f(kArena);
    std::mt19937 rng(987);
    struct Want { int64_t tag; std::vector<uint8_t> bytes; };
    std::deque<Want> want;
    int64_t tag = 0;
    std::size_t pushed = 0, popped = 0;
    for (int op = 0; op < 100'000; ++op) {
        const bool doPush = want.empty() || (want.size() < 60 && rng() % 100 < 55);
        if (doPush) {
            const std::size_t n = rng() % 5 == 0 ? 0 : 1 + rng() % 700;
            const auto b = bytesOf(rng, n, tag);
            const ChunkRun run{b.data(), static_cast<uint32_t>(b.size())};
            const ChunkPush r = f.pushWithChunk(answer(tag), &run, 1, static_cast<uint32_t>(b.size()));
            if (r == ChunkPush::Ok) { want.push_back({tag, b}); ++tag; ++pushed; }
            else EXPECT_EQ(r, ChunkPush::ChunkFull) << "the slot ring (64) is never full here: 60 at most";
        } else {
            ReturnFifoEntry& e = f.front();
            ASSERT_EQ(e.worker.correlationId, want.front().tag);
            const auto got = f.chunkOf(e);
            ASSERT_EQ(got.size(), want.front().bytes.size());
            ASSERT_TRUE(std::equal(got.begin(), got.end(), want.front().bytes.begin())) << "op " << op;
            ASSERT_TRUE(e.worker.chunk.empty()) << "an arena entry owns no vector";
            f.popFront();
            want.pop_front();
            ++popped;
        }
    }
    EXPECT_GT(pushed, 20'000u);
    while (!want.empty()) { f.popFront(); want.pop_front(); ++popped; }
    EXPECT_EQ(pushed, popped);
    EXPECT_EQ(f.chunkArenaUsed(), 0u) << "the ring is empty after the last pop";
}

// ChunkFull exactly when the bytes do not fit (including the skip to the next wrap); nothing is pushed then.
TEST(ReturnFifoChunkRegression, ChunkFullExactlyWhenTheBytesDoNotFitRegression) {
    ReturnFifo<16> f(1024);
    std::vector<uint8_t> a(600, 0xA1), b(500, 0xB2), c(424, 0xC3);
    const auto run = [](const std::vector<uint8_t>& v) { return ChunkRun{v.data(), static_cast<uint32_t>(v.size())}; };
    ChunkRun ra = run(a), rb = run(b), rc = run(c);
    ASSERT_EQ(f.pushWithChunk(answer(1), &ra, 1, 600), ChunkPush::Ok);          // [0, 600)
    EXPECT_EQ(f.pushWithChunk(answer(2), &rb, 1, 500), ChunkPush::ChunkFull)    // 500 > 424 left
        << "no room";
    EXPECT_EQ(f.size(), 1u) << "a refused push leaves nothing queued";
    ASSERT_EQ(f.pushWithChunk(answer(3), &rc, 1, 424), ChunkPush::Ok);          // exactly the 424 that are left
    EXPECT_EQ(f.chunkArenaUsed(), 1024u);
    std::vector<uint8_t> one(1, 0xEE);
    ChunkRun r1 = run(one);
    EXPECT_EQ(f.pushWithChunk(answer(4), &r1, 1, 1), ChunkPush::ChunkFull) << "one byte over";
    f.popFront();                                                              // frees [0, 600)
    ASSERT_EQ(f.pushWithChunk(answer(5), &rb, 1, 500), ChunkPush::Ok) << "the freed 600 B are at the start of the ring";
    std::vector<uint8_t> d(200, 0xD4);
    ChunkRun rd = run(d);
    EXPECT_EQ(f.pushWithChunk(answer(6), &rd, 1, 200), ChunkPush::ChunkFull) << "100 B left only";
    d.resize(100);
    rd = run(d);
    ASSERT_EQ(f.pushWithChunk(answer(6), &rd, 1, 100), ChunkPush::Ok);
    EXPECT_EQ(f.pushWithChunk(answer(8), &r1, 1, 1), ChunkPush::ChunkFull) << "exactly full again";
    std::vector<uint8_t> huge(2048, 0);
    ChunkRun rh = run(huge);
    EXPECT_EQ(f.pushWithChunk(answer(7), &rh, 1, 2048), ChunkPush::ChunkFull) << "longer than the whole ring";
    // drain: every byte is accounted
    while (!f.empty()) f.popFront();
    EXPECT_EQ(f.chunkArenaUsed(), 0u);
}

// A chunk-less entry, a Txn entry and arena chunks interleave in order; a slot-full FIFO says SlotsFull.
TEST(ReturnFifoChunkRegression, ChunklessAndTxnEntriesInterleaveInOrderRegression) {
    ReturnFifo<4> f(256);
    std::vector<uint8_t> a(10, 1), b(20, 2);
    ChunkRun ra{a.data(), 10}, rb{b.data(), 20};
    ASSERT_EQ(f.pushWithChunk(answer(1), &ra, 1, 10), ChunkPush::Ok);
    ASSERT_TRUE(f.push(answer(2)));                     // no chunk
    TxnReturn t{};
    ASSERT_TRUE(f.push(t));
    ASSERT_EQ(f.pushWithChunk(answer(4), &rb, 1, 20), ChunkPush::Ok);
    EXPECT_EQ(f.pushWithChunk(answer(5), &ra, 1, 10), ChunkPush::SlotsFull);
    EXPECT_EQ(f.chunkArenaUsed(), 30u);
    ASSERT_EQ(f.front().worker.correlationId, 1);
    EXPECT_EQ(f.chunkOf(f.front()).size(), 10u);
    f.popFront();
    ASSERT_EQ(f.front().worker.correlationId, 2);
    EXPECT_EQ(f.chunkOf(f.front()).size(), 0u);
    f.popFront();
    ASSERT_EQ(f.front().tag, ReturnFifoEntry::Tag::Txn);
    f.popFront();
    ASSERT_EQ(f.front().worker.correlationId, 4);
    EXPECT_EQ(f.chunkOf(f.front()).size(), 20u);
    EXPECT_EQ(f.chunkArenaUsed(), 20u);
    f.popFront();
    EXPECT_TRUE(f.empty());
    EXPECT_EQ(f.chunkArenaUsed(), 0u);
}
