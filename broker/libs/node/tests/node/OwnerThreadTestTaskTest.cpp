// OwnerThreadTestTaskTest.cpp — F9a: the owner-thread test hook decides
// run-or-skip from the immutable deadline alone (no Pending/Running/Cancelled
// CAS) and reports the decision as a ring event; the test side never unwinds
// while the callable may still run.
#include <rune/node/OwnerThreadTestTask.h>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <thread>

using namespace rune::node;
using namespace std::chrono_literals;
using Clock = OwnerThreadTestTask::Clock;

namespace {
template <class F>
auto makeControlTask(F f, Clock::time_point deadline) {
    using Impl = detail::OwnerThreadTestTaskImpl<F, OwnerThreadTestTask::Target::ControlThread>;
    return std::make_shared<Impl>(-1, -1, std::move(f), deadline);
}
} // namespace

TEST(OwnerThreadTestTaskRegression, OwnerRunsBeforeDeadline) {
    int ran = 0;
    auto task = makeControlTask([&ran] { return ++ran; }, Clock::now() + 10s);
    auto fut  = task->future();
    std::thread owner([&task] { task->runOnOwner(nullptr); });
    EXPECT_EQ(awaitOwnerThreadTestTask(*task, fut, 10'000ms), 1);
    owner.join();
    EXPECT_EQ(ran, 1);
}

// The owner reaches the task only after the deadline: it must SKIP (never call
// the callable, which may reference a test frame that already unwound) and the
// test side must throw the timeout.
TEST(OwnerThreadTestTaskRegression, OwnerPastDeadlineSkipsAndTestThrows) {
    bool ran = false;
    auto task = makeControlTask([&ran] { ran = true; return 0; }, Clock::now() + 20ms);
    auto fut  = task->future();
    std::thread owner([&task] {
        std::this_thread::sleep_for(60ms);      // owner busy past the deadline
        task->runOnOwner(nullptr);
    });
    EXPECT_THROW(awaitOwnerThreadTestTask(*task, fut, 20ms), std::runtime_error);
    owner.join();
    EXPECT_FALSE(ran) << "a task reached after its deadline must never run";
}

// The owner never reaches the task (wedged / stopped): the test throws after
// the deadline + grace instead of hanging; the callable never runs.
TEST(OwnerThreadTestTaskRegression, OwnerNeverReachesTaskTestThrows) {
    bool ran = false;
    auto task = makeControlTask([&ran] { ran = true; return 0; }, Clock::now() + 10ms);
    auto fut  = task->future();
    const auto t0 = Clock::now();
    EXPECT_THROW(awaitOwnerThreadTestTask(*task, fut, 10ms), std::runtime_error);
    EXPECT_LT(Clock::now() - t0, kOwnerDecisionGrace + 2s);
    task->runOnOwner(nullptr);                 // a late owner still only skips
    EXPECT_FALSE(ran);
}

// The owner STARTED before the deadline and is still running when the deadline
// passes: the test must wait for completion (never unwind under the callable).
TEST(OwnerThreadTestTaskRegression, StartedBeforeDeadlineIsAwaitedPastIt) {
    int ran = 0;
    auto task = makeControlTask([&ran] {
        std::this_thread::sleep_for(300ms);     // outlives the 100 ms deadline
        return ++ran;
    }, Clock::now() + 100ms);
    auto fut  = task->future();
    std::thread owner([&task] { task->runOnOwner(nullptr); });
    EXPECT_EQ(awaitOwnerThreadTestTask(*task, fut, 100ms), 1);
    owner.join();
    EXPECT_EQ(ran, 1);
}
