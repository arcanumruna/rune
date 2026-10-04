// ParkUntilWorkIdleTest.cpp — A7: the shard-worker idle strategy's shape (spin -> yield -> park, one action
// per idle pass, reset on work) and its park-until-work semantics on readiness fds (a pipe stands in for the
// driver's epoll fds here; the real-driver wake is EmbeddedDriverTest's DriverWakeFds tests, Linux).
#include <rune/node/ParkUntilWorkIdle.h>

#include <gtest/gtest.h>

#include <unistd.h>

#include <chrono>
#include <thread>

using rune::node::IdleShape;
using rune::node::ParkUntilWorkIdle;
using Phase = ParkUntilWorkIdle::Phase;
using namespace std::chrono_literals;

namespace {
struct Pipe {
    int fds[2]{-1, -1};
    Pipe() { EXPECT_EQ(::pipe(fds), 0); }
    ~Pipe() { ::close(fds[0]); ::close(fds[1]); }
    int  readEnd() const { return fds[0]; }
    void write1() const { const char b = 'x'; ASSERT_EQ(::write(fds[1], &b, 1), 1); }
    void drain1() const { char b; ASSERT_EQ(::read(fds[0], &b, 1), 1); }
};
int64_t msSince(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
}
} // namespace

TEST(ParkUntilWorkIdle, SpinsThenYieldsThenParksOneActionPerPass) {
    ParkUntilWorkIdle idle(IdleShape{2, 3, 1'000, 8'000}, nullptr, 0);
    EXPECT_EQ(idle.phase(), Phase::NotIdle);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Spinning);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Spinning);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Yielding);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Yielding);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Yielding);
    EXPECT_EQ(idle.parks(), 0u);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Parking);
    EXPECT_EQ(idle.parks(), 1u);
}

TEST(ParkUntilWorkIdle, ParkPeriodDoublesToMax) {
    ParkUntilWorkIdle idle(IdleShape{0, 0, 1'000, 5'000}, nullptr, 0);
    EXPECT_EQ(idle.nextParkNs(), 1'000);
    idle.idle(0); EXPECT_EQ(idle.nextParkNs(), 2'000);
    idle.idle(0); EXPECT_EQ(idle.nextParkNs(), 4'000);
    idle.idle(0); EXPECT_EQ(idle.nextParkNs(), 5'000) << "capped at maxPark";
    idle.idle(0); EXPECT_EQ(idle.nextParkNs(), 5'000);
    EXPECT_EQ(idle.parks(), 4u);
}

TEST(ParkUntilWorkIdle, WorkResetsToStart) {
    ParkUntilWorkIdle idle(IdleShape{1, 1, 1'000, 4'000}, nullptr, 0);
    for (int i = 0; i < 4; ++i) idle.idle(0);
    ASSERT_EQ(idle.phase(), Phase::Parking);
    idle.idle(3);
    EXPECT_EQ(idle.phase(), Phase::NotIdle);
    EXPECT_EQ(idle.nextParkNs(), 1'000);
    idle.idle(0); EXPECT_EQ(idle.phase(), Phase::Spinning) << "a reset restarts the spin phase";
}

// The derived wake shape (0 spins, 0 yields): the FIRST empty pass parks — the regression this fixes is a
// worker that polled ~30 syscall-heavy passes (~1 ms) before every park and so never parked under load.
TEST(ParkUntilWorkIdleRegression, WakeShapeParksOnFirstEmptyPass) {
    Pipe p;
    const int fd = p.readEnd();
    ParkUntilWorkIdle idle(IdleShape{0, 0, 1'000, 200'000}, &fd, 1);
    EXPECT_EQ(idle.wakeFdCount(), 1);
    idle.idle(0);
    EXPECT_EQ(idle.phase(), Phase::Parking);
    EXPECT_EQ(idle.parks(), 1u);
}

TEST(ParkUntilWorkIdle, ReadyFdEndsParkImmediately) {
    Pipe p;
    const int fd = p.readEnd();
    p.write1();
    ParkUntilWorkIdle idle(IdleShape{0, 0, 2'000'000'000, 2'000'000'000}, &fd, 1);   // 2 s park
    const auto t0 = std::chrono::steady_clock::now();
    idle.idle(0);
    EXPECT_LT(msSince(t0), 500) << "a readable fd must not wait out the park period";
    EXPECT_EQ(idle.readinessWakes(), 1u);
}

TEST(ParkUntilWorkIdle, ParkedThreadWakesWhenFdBecomesReady) {
    Pipe p;
    const int fd = p.readEnd();
    ParkUntilWorkIdle idle(IdleShape{0, 0, 5'000'000'000, 5'000'000'000}, &fd, 1);   // 5 s park
    // No sleep (tests never sleep): the write races the park — before it (level-triggered: the park returns at
    // once) or during it (the parked thread wakes).  Either way the 5 s park must end on readiness.
    const auto t0 = std::chrono::steady_clock::now();
    std::thread writer([&] { p.write1(); });
    idle.idle(0);
    const auto waited = msSince(t0);
    writer.join();
    EXPECT_LT(waited, 2'000) << "parked until work, not until the timeout";
    EXPECT_EQ(idle.readinessWakes(), 1u);
}

TEST(ParkUntilWorkIdle, NoReadinessParksForThePeriod) {
    Pipe p;
    const int fd = p.readEnd();
    ParkUntilWorkIdle idle(IdleShape{0, 0, 20'000'000, 20'000'000}, &fd, 1);   // 20 ms
    const auto t0 = std::chrono::steady_clock::now();
    idle.idle(0);
    EXPECT_GE(msSince(t0), 15) << "nothing readable: the park period still bounds the wait";
    EXPECT_EQ(idle.readinessWakes(), 0u);
    // Level semantics: once drained, the fd no longer ends parks.
    p.write1();
    idle.idle(0);
    EXPECT_EQ(idle.readinessWakes(), 1u);
    p.drain1();
    const auto t1 = std::chrono::steady_clock::now();
    idle.idle(0);
    EXPECT_GE(msSince(t1), 15);
    EXPECT_EQ(idle.readinessWakes(), 1u);
}

TEST(ParkUntilWorkIdle, SkipsNegativeFdsAndCapsCount) {
    int fds[ParkUntilWorkIdle::kMaxWakeFds + 3];
    Pipe p;
    for (auto& f : fds) f = p.readEnd();
    fds[0] = -1;
    ParkUntilWorkIdle idle(IdleShape{}, fds, static_cast<int>(std::size(fds)));
    EXPECT_EQ(idle.wakeFdCount(), ParkUntilWorkIdle::kMaxWakeFds);
    ParkUntilWorkIdle none(IdleShape{}, fds, 1);   // only the -1
    EXPECT_EQ(none.wakeFdCount(), 0);
}

TEST(ParkUntilWorkIdle, SanitizesShape) {
    ParkUntilWorkIdle idle(IdleShape{-1, -2, 0, -5}, nullptr, 0);
    EXPECT_EQ(idle.shape().maxSpins, 0);
    EXPECT_EQ(idle.shape().maxYields, 0);
    EXPECT_EQ(idle.shape().minParkNs, 1);
    EXPECT_EQ(idle.shape().maxParkNs, 1);
}
