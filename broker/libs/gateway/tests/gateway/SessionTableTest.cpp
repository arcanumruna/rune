// SessionTableTest.cpp — Unit tests for SessionTable (BR-4).
//
// Tests:
//   1. nextSessionId — monotonic and distinct
//   2. add + find — present after add
//   3. find missing — returns nullptr
//   4. remove existing — returns true, session gone
//   5. remove missing — returns false
//   6. touch updates lastSeenNs
//   7. touch on missing session — no-op (no crash)
//   8. size tracks add and remove correctly
//   9. many sessions — all findable
//  10. add + remove + re-add (same id) — find returns new entry

#include <rune/gateway/SessionTable.h>

#include <gtest/gtest.h>

#include <memory>

namespace rune::gateway {
namespace {

// A minimal stub ExclusivePublication for unit tests — we just need a
// non-null shared_ptr; we never call any Aeron methods on it.
static std::shared_ptr<aeron::ExclusivePublication> makeStubPub() {
    // Wrap a nullptr in a shared_ptr with a no-op deleter so the table can
    // hold and release it without crashing.
    return std::shared_ptr<aeron::ExclusivePublication>(
        nullptr, [](aeron::ExclusivePublication*) {});
}

// ===========================================================================
// TEST 1: nextSessionId — monotonic and distinct
// ===========================================================================
TEST(SessionTableTest, NextSessionId_MonotonicDistinct) {
    SessionTable t;

    const int64_t id1 = t.nextSessionId();
    const int64_t id2 = t.nextSessionId();
    const int64_t id3 = t.nextSessionId();

    EXPECT_GE(id1, 1);
    EXPECT_GT(id2, id1);
    EXPECT_GT(id3, id2);
}

// ===========================================================================
// TEST 2: add + find — session present after add
// ===========================================================================
TEST(SessionTableTest, AddFind_SessionPresent) {
    SessionTable t;

    const int64_t sid = t.nextSessionId();
    t.add(sid, makeStubPub(), 1000LL);

    Session* s = t.find(sid);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(s->sessionId,  sid);
    EXPECT_EQ(s->lastSeenNs, 1000LL);
}

// ===========================================================================
// TEST 3: find missing — returns nullptr
// ===========================================================================
TEST(SessionTableTest, Find_Missing_ReturnsNullptr) {
    SessionTable t;

    EXPECT_EQ(t.find(999), nullptr);
    EXPECT_EQ(t.find(0),   nullptr);
    EXPECT_EQ(t.find(-1),  nullptr);
}

// ===========================================================================
// TEST 4: remove existing — returns true; session gone
// ===========================================================================
TEST(SessionTableTest, Remove_Existing_ReturnsTrueAndGone) {
    SessionTable t;

    const int64_t sid = t.nextSessionId();
    t.add(sid, makeStubPub(), 0LL);

    EXPECT_NE(t.find(sid), nullptr);

    const bool removed = t.remove(sid);
    EXPECT_TRUE(removed);
    EXPECT_EQ(t.find(sid), nullptr);
}

// ===========================================================================
// TEST 5: remove missing — returns false
// ===========================================================================
TEST(SessionTableTest, Remove_Missing_ReturnsFalse) {
    SessionTable t;

    EXPECT_FALSE(t.remove(42));
    EXPECT_FALSE(t.remove(0));
}

// ===========================================================================
// TEST 6: touch updates lastSeenNs
// ===========================================================================
TEST(SessionTableTest, Touch_UpdatesLastSeenNs) {
    SessionTable t;

    const int64_t sid = t.nextSessionId();
    t.add(sid, makeStubPub(), 100LL);

    t.touch(sid, 999LL);

    Session* s = t.find(sid);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(s->lastSeenNs, 999LL);
}

// ===========================================================================
// TEST 7: touch on missing session — no-op, no crash
// ===========================================================================
TEST(SessionTableTest, Touch_Missing_NoOp) {
    SessionTable t;

    // Must not crash or throw.
    EXPECT_NO_THROW(t.touch(12345, 999LL));
    EXPECT_EQ(t.size(), 0u);
}

// ===========================================================================
// TEST 8: size tracks add and remove
// ===========================================================================
TEST(SessionTableTest, Size_TracksAddAndRemove) {
    SessionTable t;

    EXPECT_EQ(t.size(), 0u);

    const int64_t id1 = t.nextSessionId();
    t.add(id1, makeStubPub(), 0LL);
    EXPECT_EQ(t.size(), 1u);

    const int64_t id2 = t.nextSessionId();
    t.add(id2, makeStubPub(), 0LL);
    EXPECT_EQ(t.size(), 2u);

    t.remove(id1);
    EXPECT_EQ(t.size(), 1u);

    t.remove(id2);
    EXPECT_EQ(t.size(), 0u);
}

// ===========================================================================
// TEST 9: many sessions — all findable
// ===========================================================================
TEST(SessionTableTest, ManySessions_AllFindable) {
    SessionTable t;

    constexpr int N = 100;
    std::vector<int64_t> ids;
    ids.reserve(N);

    for (int i = 0; i < N; ++i) {
        int64_t sid = t.nextSessionId();
        ids.push_back(sid);
        t.add(sid, makeStubPub(), static_cast<int64_t>(i));
    }

    EXPECT_EQ(t.size(), static_cast<std::size_t>(N));

    for (int i = 0; i < N; ++i) {
        Session* s = t.find(ids[i]);
        ASSERT_NE(s, nullptr) << "session " << ids[i] << " not found";
        EXPECT_EQ(s->sessionId,  ids[i]);
        EXPECT_EQ(s->lastSeenNs, static_cast<int64_t>(i));
    }
}

// ===========================================================================
// TEST 10: add → remove → re-add with the same explicit id
// (not a nextSessionId() scenario, but verifies the map handles re-insert)
// ===========================================================================
TEST(SessionTableTest, ReAdd_AfterRemove_FindsNewEntry) {
    SessionTable t;

    const int64_t sid = t.nextSessionId();
    t.add(sid, makeStubPub(), 1LL);
    t.remove(sid);

    // Re-add with the same id and different nowNs.
    t.add(sid, makeStubPub(), 42LL);

    Session* s = t.find(sid);
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(s->lastSeenNs, 42LL);
}

} // namespace
} // namespace rune::gateway
