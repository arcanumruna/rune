// D2: non-worker broker threads are pinned by inheritance from the start() thread.
#include <rune/node/ThreadAffinity.h>

#include <gtest/gtest.h>

#include <thread>
#include <vector>

using namespace rune::node;

TEST(ThreadAffinity, EmptySetNeverPinsRegression) {
    EXPECT_EQ(pinCurrentThreadToCpus({}), EINVAL);
    ScopedThreadAffinity none({});
    EXPECT_EQ(none.result(), -1) << "nothing requested: mask untouched";
}

#if defined(__linux__)
// Pinned scope: a thread created inside inherits the set; the creator's mask is restored on exit.
TEST(ThreadAffinity, ScopedPinIsInheritedAndRestoredRegression) {
    const std::vector<int32_t> before = currentThreadCpus();
    // Review L4: with one allowed CPU the pin cannot change the mask (vacuous); pin to a CPU that does.
    if (before.size() < 2) GTEST_SKIP() << "needs >= 2 allowed CPUs";
    const std::vector<int32_t> target{before.back()};
    ASSERT_NE(target, before);
    std::vector<int32_t> child;
    {
        ScopedThreadAffinity pin(target);
        ASSERT_EQ(pin.result(), 0);
        EXPECT_EQ(currentThreadCpus(), target);
        std::thread t([&] { child = currentThreadCpus(); });
        t.join();
    }
    EXPECT_EQ(child, target) << "threads created in the scope inherit the non-worker set";
    EXPECT_EQ(currentThreadCpus(), before) << "the caller's mask is restored";
}
#else
TEST(ThreadAffinity, NoOpOffLinuxRegression) {
    EXPECT_EQ(pinCurrentThreadToCpus({0}), ENOTSUP);
    EXPECT_TRUE(currentThreadCpus().empty());
    ScopedThreadAffinity pin({0});
    EXPECT_EQ(pin.result(), ENOTSUP);
}
#endif
