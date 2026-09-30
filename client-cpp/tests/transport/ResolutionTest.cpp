// tests/transport/ResolutionTest.cpp — Resolution state machine, fake registry,
// injected clock (no sleeps, no Aeron).
#include <rune/client/transport/Resolution.h>

#include <gtest/gtest.h>

#include <memory>
#include <set>
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
    int     rejectFindsLeft = 0;   // the next N find() calls throw (transient error)
    int     throwAddsLeft = 0;     // the next N add() calls throw (transient error)
    std::set<int64_t> live;        // registrations the "driver" still holds

    int64_t add() {
        if (throwOnAdd) throw std::runtime_error("invalid channel");
        if (throwAddsLeft > 0) {
            --throwAddsLeft;
            throw std::runtime_error("transient add failure");
        }
        ++adds;
        live.insert(nextRegId);
        return nextRegId++;
    }
    std::shared_ptr<FakeHandle> find(int64_t regId) {
        ++finds;
        if (rejectOnFind || rejectFindsLeft > 0) {
            if (rejectFindsLeft > 0) --rejectFindsLeft;
            live.erase(regId);  // Aeron erases an errored registration before throwing
            throw std::runtime_error("registration rejected");
        }
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

// Regression (Task 7 review I1): a timeout used to re-add() without releasing or
// re-checking the first registration, so every timeout leaked one live driver
// registration (a publication with its log buffers).  After the backoff the SAME
// registration must be resumed: exactly one live registration, and it is reused
// when it finally resolves.
TEST(Resolution, TimeoutResumesSameRegistrationNoLeakRegression) {
    FakeRegistry reg;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);                                                  // add -> 100
    EXPECT_EQ(r.poll(reg, kTimeout), ResolveStatus::Failed);         // TimedOut
    ASSERT_TRUE(std::holds_alternative<Res::TimedOut>(r.state()));
    EXPECT_EQ(std::get<Res::TimedOut>(r.state()).regId, 100);
    EXPECT_EQ(std::get<Res::TimedOut>(r.state()).retryAtNs, kTimeout + kBackoff);
    EXPECT_EQ(r.poll(reg, kTimeout + kBackoff - 1), ResolveStatus::Failed);
    EXPECT_EQ(r.poll(reg, kTimeout + kBackoff), ResolveStatus::Pending);   // resumed
    ASSERT_TRUE(std::holds_alternative<Res::Resolving>(r.state()));
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).regId, 100);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).deadlineNs, kTimeout + kBackoff + kTimeout);
    // A second timeout + retry still holds exactly one registration.
    r.poll(reg, 2 * kTimeout + kBackoff);
    r.poll(reg, 2 * kTimeout + 2 * kBackoff);
    EXPECT_EQ(reg.adds, 1);
    EXPECT_EQ(reg.live, std::set<int64_t>{100}) << "exactly one live registration";
    reg.readyOnFind = 0;
    EXPECT_EQ(r.poll(reg, 2 * kTimeout + 2 * kBackoff + 1), ResolveStatus::Ready);
    EXPECT_EQ(r.ready()->regId, 100) << "the original registration is the one reused";
}

// Regression (Task 7 review I2): a driver rejection used to be terminal, so one
// transient error killed the slot until stop().  It must retry with a fresh
// registration once the backoff (injected clock) has elapsed.
TEST(Resolution, TransientRejectionRecoversAfterBackoffRegression) {
    FakeRegistry reg;
    reg.rejectFindsLeft = 1;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);                                                  // add -> 100
    EXPECT_EQ(r.poll(reg, 1), ResolveStatus::Failed);                // find throws
    ASSERT_TRUE(r.rejected());
    EXPECT_EQ(std::get<Res::Rejected>(r.state()).retryAtNs, 1 + kBackoff);
    EXPECT_EQ(r.poll(reg, kBackoff), ResolveStatus::Failed) << "backoff not yet elapsed";
    EXPECT_EQ(reg.adds, 1);
    EXPECT_EQ(r.poll(reg, 1 + kBackoff), ResolveStatus::Pending);    // fresh add -> 101
    EXPECT_EQ(reg.adds, 2);
    EXPECT_EQ(std::get<Res::Resolving>(r.state()).regId, 101);
    reg.readyOnFind = 0;
    EXPECT_EQ(r.poll(reg, 2 + kBackoff), ResolveStatus::Ready);
    EXPECT_EQ(r.ready()->regId, 101);
    EXPECT_EQ(reg.live, std::set<int64_t>{101}) << "the errored registration is gone";
}

TEST(Resolution, TransientAddFailureRecoversAfterBackoffRegression) {
    FakeRegistry reg;
    reg.throwAddsLeft = 1;
    Res r{kTimeout, kBackoff};
    EXPECT_EQ(r.poll(reg, 0), ResolveStatus::Failed);
    EXPECT_TRUE(r.rejected());
    EXPECT_EQ(r.poll(reg, kBackoff - 1), ResolveStatus::Failed);
    EXPECT_EQ(r.poll(reg, kBackoff), ResolveStatus::Pending);
    reg.readyOnFind = 0;
    EXPECT_EQ(r.poll(reg, kBackoff + 1), ResolveStatus::Ready);
}

TEST(Resolution, PersistentRejectionRetriesOnlyAtBackoffPace) {
    FakeRegistry reg;
    reg.rejectOnFind = true;
    Res r{kTimeout, kBackoff};
    r.poll(reg, 0);
    for (int64_t t = 1; t < kBackoff; ++t) EXPECT_EQ(r.poll(reg, t), ResolveStatus::Failed);
    EXPECT_EQ(reg.adds, 1) << "no re-registration inside the backoff window";
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
