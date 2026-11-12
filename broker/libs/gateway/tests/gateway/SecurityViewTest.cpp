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
// Issue #269: the fake-iteration mode is kept incrementally; a user event never recounts every user.
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

/// The brute-force answer: the most common verifier iteration count over all users; ties: the smallest count; no
/// verifier at all: 4096.
uint32_t bruteForceFakeIterations(const sec::SecurityState& st) {
    std::map<uint32_t, std::size_t> counts;
    for (const auto& [id, u] : st.users())
        for (const auto& v : u.verifiers) ++counts[v.iterations];
    uint32_t best = 4096;
    std::size_t bestN = 0;
    for (const auto& [it, n] : counts)
        if (n > bestN) { best = it; bestN = n; }
    return best;
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

TEST(SecurityViewRegression, FakeIterationsMatchesAFullRecountAfterEveryEventRegression) {
    SecurityEventChain chain;
    SecurityView view{sec::SecurityState{}};
    sec::BootstrapOps b;
    b.verifier = verifierWith(1, 4096);
    b.fakeSaltSeed.fill(7);
    applyLatest(chain, view, chain.emit(b, "admin", "BOOTSTRAP", "bootstrap"));
    ASSERT_EQ(view.fakeIterations(), bruteForceFakeIterations(chain.state()));

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
        ASSERT_EQ(view.fakeIterations(), bruteForceFakeIterations(view.state())) << "after event " << applied;
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
    EXPECT_EQ(view.fakeIterations(), bruteForceFakeIterations(view.state()));
}
