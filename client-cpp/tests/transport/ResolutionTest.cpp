// tests/transport/ResolutionTest.cpp — Resolution state machine, fake registry,
// injected clock (no sleeps, no Aeron).
#include <rune/client/transport/Resolution.h>

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <variant>

using rune::client::transport::Resolution;
using rune::client::transport::ResolveStatus;

namespace {

struct FakeHandle { int64_t regId; };

struct FakeRegistry {
    int     adds = 0;
    int     finds = 0;
    int64_t nextRegId = 100;
    int     readyOnFind = -1;      // find() #N returns a handle; -1 = never
    bool    rejectOnFind = false;
    bool    throwOnAdd = false;

    int64_t add() {
        if (throwOnAdd) throw std::runtime_error("invalid channel");
        ++adds;
        return nextRegId++;
    }
    std::shared_ptr<FakeHandle> find(int64_t regId) {
        ++finds;
        if (rejectOnFind) throw std::runtime_error("registration rejected");
        if (readyOnFind >= 0 && finds >= readyOnFind) {
            return std::make_shared<FakeHandle>(FakeHandle{regId});
        }
        return nullptr;
    }
};

using Res = Resolution<FakeHandle>;
constexpr int64_t kTimeout = 1'000;
constexpr int64_t kBackoff = 50;

} // namespace

TEST(Resolution, UnstartedIssuesExactlyOneRegistration) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Pending);
    EXPECT_EQ(reg.adds, 1);
    EXPECT_EQ(reg.finds, 0);
    ASSERT_TRUE(std::holds_alternative<Res::Resolving>(r.state()));
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).regId, 100);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).deadlineNs, kTimeout);
}

TEST(Resolution, BecomesReadyWhenRegistryHasHandleAndIsSticky) {
    FakeRegistry reg;
    reg.readyOnFind = 2;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Pending);    // add
    EXPECT_EQ(r.poll(reg, 10), ResolveStatus::Pending);   // find #1
    EXPECT_EQ(r.poll(reg, 20), ResolveStatus::Ready);     // find #2
    ASSERT_NE(r.ready(), nullptr);
    EXPECT_EQ(r.ready()->regId, 100);
    const int findsAtReady = reg.finds;
    EXPECT_EQ(r.poll(reg, 5'000'000), ResolveStatus::Ready);
    EXPECT_EQ(reg.finds, findsAtReady) << "Ready never touches the registry again";
}

TEST(Resolution, TimesOutExactlyAtDeadline) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    EXPECT_EQ(r.poll(reg, kTimeout - 1), ResolveStatus::Pending);
    EXPECT_EQ(r.poll(reg, kTimeout), ResolveStatus::Failed);
    ASSERT_TRUE(std::holds_alternative<Res::TimedOut>(r.state()));
    EXPECT_EQ(std::get<Res::TimedOut>(r.state()).retryAtNs, kTimeout + kBackoff);
    EXPECT_FALSE(r.rejected());
}

TEST(Resolution, RetriesWithFreshRegistrationAfterBackoff) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    r.poll(reg, kTimeout);                                           // TimedOut
    EXPECT_EQ(r.poll(reg, kTimeout + kBackoff - 1), ResolveStatus::Failed);
    EXPECT_EQ(reg.adds, 1);
    EXPECT_EQ(r.poll(reg, kTimeout + kBackoff), ResolveStatus::Pending);
    EXPECT_EQ(reg.adds, 2);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).regId, 101);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).deadlineNs,
              kTimeout + kBackoff + kTimeout);
}

TEST(Resolution, DriverRejectionIsTerminal) {
    FakeRegistry reg;
    reg.rejectOnFind = true;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    EXPECT_EQ(r.poll(reg, 1), ResolveStatus::Failed);
    EXPECT_TRUE(r.rejected());
    EXPECT_EQ(r.poll(reg, 1'000'000'000), ResolveStatus::Failed);
    EXPECT_EQ(reg.adds, 1) << "a rejected channel is never re-registered";
}

TEST(Resolution, AddThrowIsTerminal) {
    FakeRegistry reg;
    reg.throwOnAdd = true;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Failed);
    EXPECT_TRUE(r.rejected());
}

TEST(Resolution, ResetDropsTheHandle) {
    FakeRegistry reg;
    reg.readyOnFind = 1;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    r.poll(reg, 1);
    ASSERT_NE(r.ready(), nullptr);
    std::weak_ptr<FakeHandle> weak = *r.readyShared();
    r.reset();
    EXPECT_EQ(r.ready(), nullptr);
    EXPECT_TRUE(weak.expired());
    EXPECT_TRUE(std::holds_alternative<Res::Unstarted>(r.state()));
}
