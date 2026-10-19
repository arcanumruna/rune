// benchmark/SecurityReplayBench.cpp — the library replay of a 10 000-event __security log
// (auth plan Task 4).  One iteration = a full verifying replay of all 10 000 events through a
// fresh SecurityReplayer: decode, gap check, apply, fingerprint verify.  No gate here; the
// broker-level boot gate (< 1 s for 10k) is asserted for __security in Task 9.
#include <benchmark/benchmark.h>
#include <rune/security/SecurityReplayer.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace {
namespace rs = rune::security;

constexpr int kEvents = 10'000;
constexpr int kUsers  = 1'000;
constexpr int kRoles  = 200;

rs::ScramVerifier verifier(uint64_t seed) {
    rs::ScramVerifier v;
    v.salt.assign(16, static_cast<uint8_t>(seed)); v.iterations = 4096;
    v.storedKey.fill(static_cast<uint8_t>(seed + 1)); v.serverKey.fill(static_cast<uint8_t>(seed + 2));
    return v;
}

/// A valid log: 1 000 users and 200 roles are created first, then grants, role memberships, password
/// rotations, disable / enable, revokes and role-membership removal until 10 000 events.
/// Every event is stamped with previewFingerprint, as the leader does.
std::vector<std::vector<uint8_t>> buildLog() {
    rs::SecurityState s;
    std::vector<std::vector<uint8_t>> out;
    out.reserve(kEvents);
    auto emit = [&](rs::Ops ops, const std::string& name, bool roleTarget) {
        const auto i = static_cast<int64_t>(out.size());
        rs::SecurityEvent e;
        e.kind = rs::kindOf(ops); e.ops = std::move(ops); e.objectName = name;
        const bool create = e.kind == rs::EventKind::CreateUser || e.kind == rs::EventKind::CreateRole;
        if (create) { e.objectId = s.nextObjectId(); e.prevVersion = 0; }
        else if (roleTarget) { const auto* r = s.role(name); e.objectId = r->objectId; e.prevVersion = r->version; }
        else { const auto* u = s.user(name); e.objectId = u->objectId; e.prevVersion = u->version; }
        e.newVersion = e.prevVersion + 1;
        e.statement = "-- bench " + std::to_string(i);
        e.principal = rs::Principal{.sessionId = 1, .clientName = "bench", .sourceAddress = "local", .authenticated = "admin",
                                    .requestId = rs::RequestId{0xBE7C, static_cast<uint64_t>(i) + 1},
                                    .requestFingerprint = static_cast<uint64_t>(i)};
        e.timestampMs = 1'760'000'000'000 + i;
        auto fp = rs::previewFingerprint(s, e);
        if (!fp) { std::fprintf(stderr, "bench log %lld: %s\n", static_cast<long long>(i), fp.error().detail.c_str()); std::abort(); }
        e.fingerprint = *fp;
        if (auto a = rs::apply(s, e, i); !a) { std::fprintf(stderr, "bench apply %lld: %s\n", static_cast<long long>(i), a.error().detail.c_str()); std::abort(); }
        out.push_back(rs::encode(e).value());
    };
    for (int r = 0; r < kRoles; ++r) emit(rs::CreateRoleOps{}, "role" + std::to_string(r), true);
    for (int u = 0; u < kUsers; ++u) emit(rs::CreateUserOps{verifier(static_cast<uint64_t>(u))}, "user" + std::to_string(u), false);
    // The remaining 8 800 events, cycling per user: grant to a role, add the user to a role, add a
    // password, revoke the role grant, remove the membership, drop the extra password, disable, enable.
    for (int step = 0; static_cast<int>(out.size()) < kEvents; ++step) {
        const int stage = (step / kUsers) % 8;
        const int u = step % kUsers;
        const std::string user = "user" + std::to_string(u);
        const std::string role = "role" + std::to_string(u % kRoles);
        const rs::GrantSpec g{rs::Operation::Read, {rs::ResourceType::Topic, rs::PatternKind::Exact, "topic" + std::to_string(u)}};
        switch (stage) {
        case 0: emit(rs::GrantOps{rs::GranteeKind::Role, g}, role, true); break;
        case 1: emit(rs::GrantRoleOps{s.role(role)->objectId, role}, user, false); break;
        case 2: emit(rs::AlterUserOps{rs::AddPassword{verifier(static_cast<uint64_t>(u) + 7)}}, user, false); break;
        case 3: emit(rs::RevokeOps{rs::GranteeKind::Role, g}, role, true); break;
        case 4: emit(rs::RevokeRoleOps{s.role(role)->objectId, role}, user, false); break;
        case 5: emit(rs::AlterUserOps{rs::DropPassword{1}}, user, false); break;
        case 6: emit(rs::AlterUserOps{rs::DisableUser{}}, user, false); break;
        default: emit(rs::AlterUserOps{rs::EnableUser{}}, user, false); break;
        }
    }
    return out;
}

void BM_SecurityReplay10kEvents(benchmark::State& st) {
    static const std::vector<std::vector<uint8_t>> log = buildLog();
    for (auto _ : st) {
        rs::SecurityReplayer r;
        for (std::size_t i = 0; i < log.size(); ++i) {
            auto a = r.feed(static_cast<int64_t>(i), log[i]);
            if (!a) { st.SkipWithError(rs::describe(a.error()).c_str()); return; }
            benchmark::DoNotOptimize(a->fingerprint);
        }
        benchmark::DoNotOptimize(r.state().digest());
    }
    st.SetItemsProcessed(st.iterations() * static_cast<int64_t>(log.size()));
}
} // namespace

BENCHMARK(BM_SecurityReplay10kEvents)->Unit(benchmark::kMillisecond);
