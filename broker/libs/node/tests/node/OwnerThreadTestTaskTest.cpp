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
    std::thread owner([&task] { task->runOnOwner(nullptr); });
    EXPECT_EQ(awaitOwnerThreadTestTask(*task, 10'000ms), 1);
    owner.join();
    EXPECT_EQ(ran, 1);
}

// The callable's exception is delivered to the test thread (ring completion,
// no std::promise).
TEST(OwnerThreadTestTaskRegression, CallableExceptionIsRethrownOnTestThread) {
    auto task = makeControlTask([]() -> int { throw std::logic_error("boom"); }, Clock::now() + 10s);
    std::thread owner([&task] { task->runOnOwner(nullptr); });
    EXPECT_THROW(awaitOwnerThreadTestTask(*task, 10'000ms), std::logic_error);
    owner.join();
}

// The owner reaches the task only after the deadline: it must SKIP (never call
// the callable) and the test side must throw the timeout.
TEST(OwnerThreadTestTaskRegression, OwnerPastDeadlineSkipsAndTestThrows) {
    bool ran = false;
    auto task = makeControlTask([&ran] { ran = true; return 0; }, Clock::now() + 20ms);
    std::thread owner([&task] {
        std::this_thread::sleep_for(60ms);      // owner busy past the deadline
        task->runOnOwner(nullptr);
    });
    EXPECT_THROW(awaitOwnerThreadTestTask(*task, 20ms), std::runtime_error);
    owner.join();
    EXPECT_FALSE(ran) << "a task reached after its deadline must never run";
}

// Fix round 1 (deadline UAF): past the deadline the test waits for the OWNER's
// decision with no bound of its own — it never returns while the owner could
// still pick the task up.  (The old 2 s grace returned early, leaving a window
// in which a just-started callable ran against an unwound frame.)
TEST(OwnerThreadTestTaskRegression, TestNeverReturnsBeforeOwnerDecision) {
    auto task = makeControlTask([] { return 0; }, Clock::now() + 10ms);
    static constexpr auto kOwnerLate = 2500ms;   // > the old 2 s grace
    const auto t0 = Clock::now();
    std::thread owner([&task] {
        std::this_thread::sleep_for(kOwnerLate);
        task->runOnOwner(nullptr);
    });
    EXPECT_THROW(awaitOwnerThreadTestTask(*task, 10ms), std::runtime_error);
    EXPECT_GE(Clock::now() - t0, kOwnerLate) << "returned before the owner decided";
    owner.join();
}

// The owner STARTED before the deadline and is still running when the deadline
// passes: the test must wait for completion (never unwind under the callable).
TEST(OwnerThreadTestTaskRegression, StartedBeforeDeadlineIsAwaitedPastIt) {
    int ran = 0;
    auto task = makeControlTask([&ran] {
        std::this_thread::sleep_for(300ms);     // outlives the 100 ms deadline
        return ++ran;
    }, Clock::now() + 100ms);
    std::thread owner([&task] { task->runOnOwner(nullptr); });
    EXPECT_EQ(awaitOwnerThreadTestTask(*task, 100ms), 1);
    owner.join();
    EXPECT_EQ(ran, 1);
}

// Shutdown skip (BrokerNode::stop drains queued tasks): the waiter is released.
TEST(OwnerThreadTestTaskRegression, ShutdownSkipReleasesWaiter) {
    bool ran = false;
    auto task = makeControlTask([&ran] { ran = true; return 0; }, Clock::now() + 10s);
    auto* ref = new OwnerThreadTestTaskRef(task);
    std::thread owner([ref] { skipOwnerThreadTestTask(ref); });
    EXPECT_THROW(awaitOwnerThreadTestTask(*task, 10'000ms), std::runtime_error);
    owner.join();
    EXPECT_FALSE(ran);
}
