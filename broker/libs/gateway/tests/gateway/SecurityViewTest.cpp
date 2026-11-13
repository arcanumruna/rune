// SecurityViewTest — the control thread's mirror of __security (authentication plan D29): it applies the admin
// applier's event bytes in offset order with the same library, reaches the same digest, and halts (fail closed) on a
// gap or an apply error.
#include "support/SecurityEventChain.h"

#include <rune/gateway/SecurityView.h>

#include <rune/security/SecurityReplayer.h>
#include <rune/security/testing/SecurityGoldenScript.h>

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>
#include <vector>

namespace sec = rune::security;
using rune::gateway::SecurityView;

namespace {
std::vector<std::vector<uint8_t>> goldenBytes() {
    std::vector<std::vector<uint8_t>> out;
    for (const auto& e : sec::testing::goldenScriptV1()) {
        auto b = sec::encode(e);
        EXPECT_TRUE(b.has_value());
        out.push_back(std::move(*b));
    }
    return out;
}
}  // namespace

TEST(SecurityViewRegression, MirrorsEveryEventInOffsetOrderToTheSameDigest) {
    const auto bytes = goldenBytes();
    sec::SecurityReplayer lib;
    SecurityView view{sec::SecurityState{}};
    EXPECT_EQ(view.appliedOffset(), -1);
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        ASSERT_TRUE(lib.feed(static_cast<int64_t>(i), bytes[i]).has_value());
        auto a = view.onApplied(static_cast<int64_t>(i), bytes[i]);
        ASSERT_TRUE(a.has_value()) << "offset " << i;
        EXPECT_EQ(view.appliedOffset(), static_cast<int64_t>(i));
    }
    EXPECT_EQ(view.state().digest(), lib.state().digest());
    EXPECT_FALSE(view.halted());
    EXPECT_EQ(view.stateName(), "Mirroring");
    EXPECT_TRUE(view.state().hasSecurityAdmin() == lib.state().hasSecurityAdmin());
}

TEST(SecurityViewRegression, ResumesFromTheBootedStateHandedOverByTheStartupThread) {
    const auto bytes = goldenBytes();
    sec::SecurityReplayer boot;
    for (std::size_t i = 0; i < 6; ++i) ASSERT_TRUE(boot.feed(static_cast<int64_t>(i), bytes[i]).has_value());
    SecurityView view{boot.state()};   // a COPY: the booted applier keeps its own
    EXPECT_EQ(view.appliedOffset(), 5);
    ASSERT_TRUE(view.onApplied(6, bytes[6]).has_value());
    EXPECT_EQ(view.appliedOffset(), 6);
}

TEST(SecurityViewRegression, AGapHaltsTheMirrorAndAppliesNothingMore) {
    const auto bytes = goldenBytes();
    SecurityView view{sec::SecurityState{}};
    ASSERT_TRUE(view.onApplied(0, bytes[0]).has_value());
    auto gap = view.onApplied(2, bytes[2]);   // offset 1 never arrived
    ASSERT_FALSE(gap.has_value());
    EXPECT_EQ(gap.error().code, sec::ApplyError::Code::OutOfOrder);
    EXPECT_TRUE(view.halted());
    EXPECT_EQ(view.appliedOffset(), 0);
    // Even the right next event is refused once halted: the first cause stays.
    auto after = view.onApplied(1, bytes[1]);
    ASSERT_FALSE(after.has_value());
    EXPECT_EQ(after.error().code, sec::ApplyError::Code::OutOfOrder);
    EXPECT_EQ(view.appliedOffset(), 0);
}

TEST(SecurityViewRegression, ARepeatedOffsetHaltsTheMirror) {
    const auto bytes = goldenBytes();
    SecurityView view{sec::SecurityState{}};
    ASSERT_TRUE(view.onApplied(0, bytes[0]).has_value());
    ASSERT_FALSE(view.onApplied(0, bytes[0]).has_value());
    EXPECT_TRUE(view.halted());
}

TEST(SecurityViewRegression, AnUndecodableOrTamperedRecordHaltsTheMirror) {
    const auto bytes = goldenBytes();
    {
        SecurityView view{sec::SecurityState{}};
        auto r = view.onApplied(0, std::vector<uint8_t>{1, 0, 2});   // formatVer 1, then truncated
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, sec::ApplyError::Code::BadOps);
        EXPECT_TRUE(view.halted());
    }
    {
        auto events = sec::testing::goldenScriptV1();
        events[0].fingerprint ^= 1;
        auto b = sec::encode(events[0]);
        SecurityView view{sec::SecurityState{}};
        auto r = view.onApplied(0, *b);
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error().code, sec::ApplyError::Code::FingerprintMismatch);
        EXPECT_EQ(view.appliedOffset(), -1) << "nothing of the refused event is applied";
    }
}

TEST(SecurityViewRegression, AnAdminReportedHaltStopsTheMirrorToo) {
    const auto bytes = goldenBytes();
    SecurityView view{sec::SecurityState{}};
    ASSERT_TRUE(view.onApplied(0, bytes[0]).has_value());
    sec::ApplyError e;
    e.offset = 1;
    e.code = sec::ApplyError::Code::FingerprintMismatch;
    view.onHalted(e);
    EXPECT_TRUE(view.halted());
    ASSERT_NE(view.haltError(), nullptr);
    EXPECT_EQ(view.haltError()->offset, 1);
    EXPECT_FALSE(view.onApplied(1, bytes[1]).has_value());
    view.onHalted(sec::ApplyError{});   // idempotent: the first cause stays
    EXPECT_EQ(view.haltError()->offset, 1);
}

// ---------------------------------------------------------------------------------------------------------------------
// Issue #269 / #238: a user event never rebuilds the derived data; the fake iteration count is the one pinned by the
// first Bootstrap.
// ---------------------------------------------------------------------------------------------------------------------
namespace {
using rune::node::test::SecurityEventChain;
using rune::node::test::testVerifier;

sec::ScramVerifier verifierWith(uint8_t seed, uint32_t iterations) {
    auto v = testVerifier(seed);
    v.iterations = iterations;
    return v;
}

sec::GrantSpec topicGrant(sec::Operation op, const std::string& name) {
    return sec::GrantSpec{op, sec::ResourcePattern{sec::ResourceType::Topic, sec::PatternKind::Exact, name}};
}

/// Apply the chain's latest event to the view at its offset.
void applyLatest(SecurityEventChain& chain, SecurityView& view, const sec::SecurityEvent& ev) {
    const auto bytes = sec::encode(ev);
    ASSERT_TRUE(bytes.has_value());
    const auto offset = static_cast<int64_t>(chain.count()) - 1;
    const auto applied = view.onApplied(offset, *bytes);
    ASSERT_TRUE(applied.has_value()) << (applied ? "" : applied.error().detail);
}
}  // namespace

TEST(SecurityViewRegression, FakeIterationsStayThePinnedBootstrapCountAfterEveryEventRegression) {
    SecurityEventChain chain;
    SecurityView view{sec::SecurityState{}};
    sec::BootstrapOps b;
    b.verifier = verifierWith(1, 4096);
    b.fakeSaltSeed.fill(7);
    applyLatest(chain, view, chain.emit(b, "admin", "BOOTSTRAP", "bootstrap"));
    ASSERT_EQ(view.fakeIterations(), 4096u);

    std::mt19937_64 rng(0x269);   // fixed seed
    const uint32_t kCounts[] = {4096, 5000, 6000, 7000};
    std::vector<std::string> users, roles;
    auto pick = [&](std::size_t n) { return static_cast<std::size_t>(rng() % n); };
    int applied = 1;
    for (int i = 0; i < 2500; ++i) {
        const auto seed = static_cast<uint8_t>(i);
        const std::string tag = std::to_string(i);
        switch (rng() % 8) {
            case 0: case 1: {
                users.push_back("u" + tag);
                const auto v = verifierWith(seed, kCounts[pick(4)]);
                applyLatest(chain, view, chain.emit(sec::CreateUserOps{v}, users.back(), "CREATE USER u" + tag + ";"));
                break;
            }
            case 2: {
                if (users.empty()) continue;
                const auto v = verifierWith(seed, kCounts[pick(4)]);
                applyLatest(chain, view, chain.emit(sec::AlterUserOps{sec::SetPassword{v, false}},
                                                    users[pick(users.size())], "ALTER USER ... SET PASSWORD;"));
                break;
            }
            case 3: {
                if (users.empty()) continue;
                const std::string& u = users[pick(users.size())];
                const auto* cur = chain.state().userByName(rune::catalog::kDefaultNsId, u);
                const auto v = verifierWith(seed, kCounts[pick(4)]);
                if (cur->verifiers.size() < 2)
                    applyLatest(chain, view, chain.emit(sec::AlterUserOps{sec::AddPassword{v}}, u,
                                                        "ALTER USER ... ADD PASSWORD;"));
                else
                    applyLatest(chain, view, chain.emit(sec::AlterUserOps{sec::DropPassword{1}}, u,
                                                        "ALTER USER ... DROP PASSWORD 1;"));
                break;
            }
            case 4: {
                if (users.empty()) continue;
                const std::size_t k = pick(users.size());
                const std::string u = users[k];
                users.erase(users.begin() + static_cast<std::ptrdiff_t>(k));
                applyLatest(chain, view, chain.emit(sec::DropUserOps{}, u, "DROP USER ...;"));
                break;
            }
            case 5: {
                if (users.empty()) continue;
                const auto g = topicGrant(sec::Operation::Read, "t" + tag);
                applyLatest(chain, view, chain.emit(sec::GrantOps{sec::GranteeKind::User, g}, users[pick(users.size())],
                                                    "GRANT ...;"));
                break;
            }
            case 6: {
                roles.push_back("r" + tag);
                applyLatest(chain, view, chain.emit(sec::CreateRoleOps{}, roles.back(), "CREATE ROLE r" + tag + ";"));
                break;
            }
            default: {
                if (roles.empty()) continue;
                const std::size_t k = pick(roles.size());
                const std::string r = roles[k];
                roles.erase(roles.begin() + static_cast<std::ptrdiff_t>(k));
                applyLatest(chain, view, chain.emit(sec::DropRoleOps{}, r, "DROP ROLE ...;"));
                break;
            }
        }
        if (::testing::Test::HasFatalFailure()) return;
        ++applied;
        ASSERT_EQ(view.fakeIterations(), 4096u) << "after event " << applied;
    }
    EXPECT_GE(applied, 2000);
    EXPECT_FALSE(view.halted());
}

TEST(SecurityViewRegression, UserEventsDoNotRebuildTheDerivedCountsRegression) {
    SecurityEventChain chain;
    sec::BootstrapOps b;
    b.verifier = verifierWith(1, 4096);
    b.fakeSaltSeed.fill(7);
    (void)chain.emit(b, "admin", "BOOTSTRAP", "bootstrap");
    (void)chain.createUser("w");
    SecurityView view{chain.state()};
    const uint64_t rebuilds = view.derivedFullRebuildsForTest();
    {
        auto ev = chain.createUser("alice");
        applyLatest(chain, view, ev);
        ev = chain.emit(sec::AlterUserOps{sec::SetPassword{verifierWith(5, 5000), false}}, "alice", "ALTER USER ...;");
        applyLatest(chain, view, ev);
        const auto g = topicGrant(sec::Operation::Read, "t");
        ev = chain.emit(sec::GrantOps{sec::GranteeKind::User, g}, "alice", "GRANT ...;");
        applyLatest(chain, view, ev);
        ev = chain.emit(sec::RevokeOps{sec::GranteeKind::User, g}, "alice", "REVOKE ...;");
        applyLatest(chain, view, ev);
        ev = chain.emit(sec::DropUserOps{}, "alice", "DROP USER alice;");
        applyLatest(chain, view, ev);
    }
    EXPECT_EQ(view.derivedFullRebuildsForTest(), rebuilds) << "no user / grant event recounts the users";
    EXPECT_EQ(view.fakeIterations(), 4096u);
}

// Issue #238: the iteration count advertised for an unknown name is the one of the FIRST Bootstrap's verifier, fixed
// for the life of the cluster; no later change of the real accounts' counts moves it.
TEST(SecurityViewRegression, FakeIterationsDoNotChangeWhenTheRealModeChangesRegression) {
    SecurityEventChain chain;
    SecurityView view{sec::SecurityState{}};
    std::vector<std::vector<uint8_t>> log;   // the record bytes, in offset order
    auto step = [&](const sec::SecurityEvent& ev) {
        const auto bytes = sec::encode(ev);
        ASSERT_TRUE(bytes.has_value());
        log.push_back(*bytes);
        const auto applied = view.onApplied(static_cast<int64_t>(log.size()) - 1, *bytes);
        ASSERT_TRUE(applied.has_value()) << (applied ? "" : applied.error().detail);
    };
    EXPECT_EQ(view.fakeIterations(), 4096u) << "before any bootstrap: the default";
    sec::BootstrapOps b;
    b.verifier = verifierWith(1, 8192);
    b.fakeSaltSeed.fill(7);
    step(chain.emit(b, "admin", "BOOTSTRAP", "bootstrap"));
    ASSERT_EQ(view.fakeIterations(), 8192u);
    for (int i = 0; i < 3; ++i) {
        const std::string n = "a" + std::to_string(i);
        step(chain.emit(sec::CreateUserOps{verifierWith(static_cast<uint8_t>(10 + i), 4096)}, n,
                                            "CREATE USER " + n + ";"));
    }
    EXPECT_EQ(view.fakeIterations(), 8192u) << "3 users at 4096 do not move it";
    for (int i = 0; i < 5; ++i) {
        const std::string n = "b" + std::to_string(i);
        step(chain.emit(sec::CreateUserOps{verifierWith(static_cast<uint8_t>(20 + i), 16384)}, n,
                                            "CREATE USER " + n + ";"));
    }
    EXPECT_EQ(view.fakeIterations(), 8192u) << "5 users at 16384 do not move it";
    for (int i = 0; i < 3; ++i) {
        const std::string n = "a" + std::to_string(i);
        step(chain.emit(sec::DropUserOps{}, n, "DROP USER " + n + ";"));
    }
    for (int i = 0; i < 5; ++i) {
        const std::string n = "b" + std::to_string(i);
        step(chain.emit(sec::AlterUserOps{sec::SetPassword{verifierWith(static_cast<uint8_t>(30 + i), 5000), false}},
                                            n, "ALTER USER " + n + ";"));
    }
    EXPECT_EQ(view.fakeIterations(), 8192u) << "dropping users and altering passwords do not move it";
    // every admin gone, a SECOND bootstrap with another count: still the first one
    step(chain.emit(sec::AlterUserOps{sec::DisableUser{}}, "admin", "ALTER USER admin DISABLE;"));
    sec::BootstrapOps b2;
    b2.verifier = verifierWith(2, 20000);
    b2.fakeSaltSeed.fill(9);
    step(chain.emit(b2, "admin2", "BOOTSTRAP", "bootstrap"));
    EXPECT_EQ(view.fakeIterations(), 8192u) << "a later Bootstrap never moves it";

    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    // The restart twin: a fresh view replaying the same log reaches the same value, and so does one booted from the
    // final state.
    SecurityView replay{sec::SecurityState{}};
    for (std::size_t i = 0; i < log.size(); ++i) ASSERT_TRUE(replay.onApplied(static_cast<int64_t>(i), log[i]).has_value());
    EXPECT_EQ(replay.fakeIterations(), 8192u);
    EXPECT_EQ(SecurityView{chain.state()}.fakeIterations(), 8192u);
    EXPECT_EQ(chain.state().fakeIterations(), std::optional<uint32_t>{8192});
}
