// src/security/SecurityGoldenScript.cpp — see rune/security/testing/SecurityGoldenScript.h (FROZEN).
#include <rune/security/testing/SecurityGoldenScript.h>
#include <rune/security/SecurityState.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace rune::security::testing {
namespace {

constexpr int64_t  kBaseTimestampMs        = 1'760'000'000'000;
constexpr uint64_t kRequestIdHi            = 0x5EC0;
constexpr uint64_t kRequestFingerprintBase = 0xF00D'0000;

ScramVerifier verifierAt(uint64_t offset) {
    ScramVerifier v;
    v.salt.assign(16, static_cast<uint8_t>(0x10 + offset));
    v.iterations = 4096;
    v.storedKey.fill(static_cast<uint8_t>(0x30 + offset));
    v.serverKey.fill(static_cast<uint8_t>(0x50 + offset));
    return v;
}
GrantSpec spec(Operation op, ResourceType t, PatternKind k, std::string name) { return GrantSpec{op, {t, k, std::move(name)}}; }

class Builder {
public:
    /// Targets are named by their CURRENT live name; ids and versions come from the state.
    void emit(Ops ops, const std::string& name, std::string statement) {
        const auto i = static_cast<uint64_t>(events_.size());
        SecurityEvent e;
        e.kind = kindOf(ops); e.ops = std::move(ops); e.objectName = name; e.statement = std::move(statement);
        const bool roleTarget = e.kind == EventKind::CreateRole || e.kind == EventKind::DropRole ||
            (e.kind == EventKind::Grant && std::get<GrantOps>(e.ops).grantee == GranteeKind::Role) ||
            (e.kind == EventKind::Revoke && std::get<RevokeOps>(e.ops).grantee == GranteeKind::Role);
        const bool create = e.kind == EventKind::CreateUser || e.kind == EventKind::CreateRole || e.kind == EventKind::Bootstrap;
        if (create) {
            e.objectId = s_.nextObjectId(); e.prevVersion = 0;
        } else if (roleTarget) {
            const RoleObject* r = s_.role(name);
            if (!r) throw std::logic_error("golden script step " + std::to_string(i) + ": no role " + name);
            e.objectId = r->objectId; e.prevVersion = r->version;
        } else {
            const UserObject* u = s_.user(name);
            if (!u) throw std::logic_error("golden script step " + std::to_string(i) + ": no user " + name);
            e.objectId = u->objectId; e.prevVersion = u->version;
        }
        e.newVersion = e.prevVersion + 1;
        e.principal = Principal{.sessionId = 0x5E55'2000 + static_cast<int64_t>(i % 3), .clientName = "golden-cli",
                                .sourceAddress = "aeron:udp?endpoint=10.0.0.7:40123",
                                .authenticated = i == 0 ? "bootstrap" : "admin",
                                .requestId = RequestId{kRequestIdHi, i + 1},
                                .requestFingerprint = kRequestFingerprintBase + i};
        e.timestampMs = kBaseTimestampMs + 1000 * static_cast<int64_t>(i);
        auto fp = previewFingerprint(s_, e);
        if (!fp) throw std::logic_error("golden script step " + std::to_string(i) + ": " + fp.error().detail);
        e.fingerprint = *fp;
        if (auto a = apply(s_, e, static_cast<int64_t>(i)); !a)
            throw std::logic_error("golden script step " + std::to_string(i) + ": " + a.error().detail);
        events_.push_back(std::move(e));
    }
    uint64_t roleId(const std::string& n) const { return s_.role(n)->objectId; }
    uint64_t next() const { return events_.size(); }
    std::vector<SecurityEvent> take() { return std::move(events_); }
private:
    SecurityState s_;
    std::vector<SecurityEvent> events_;
};

} // namespace

std::vector<SecurityEvent> goldenScriptV1() {
    Builder b;
    const GrantSpec readEvents = spec(Operation::Read, ResourceType::Topic, PatternKind::Exact, "player_events");
    const GrantSpec writeWorld = spec(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world.");
    const GrantSpec readGroup  = spec(Operation::Read, ResourceType::Group, PatternKind::Prefix, "world-");
    BootstrapOps boot{verifierAt(b.next())};
    for (std::size_t i = 0; i < boot.fakeSaltSeed.size(); ++i) boot.fakeSaltSeed[i] = static_cast<uint8_t>(0x60 + i);
    b.emit(boot, "admin", "BOOTSTRAP USER admin WITH PASSWORD '<redacted>';");
    b.emit(CreateRoleOps{}, "world_server", "CREATE ROLE world_server;");
    b.emit(GrantOps{GranteeKind::Role, readEvents}, "world_server", "GRANT READ ON TOPIC player_events TO ROLE world_server;");
    b.emit(GrantOps{GranteeKind::Role, writeWorld}, "world_server", "GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE world_server;");
    b.emit(GrantOps{GranteeKind::Role, readGroup}, "world_server", "GRANT READ ON GROUP PREFIX 'world-' TO ROLE world_server;");
    b.emit(CreateUserOps{verifierAt(b.next())}, "ws1", "CREATE USER ws1 WITH PASSWORD '<redacted>';");
    b.emit(GrantRoleOps{b.roleId("world_server"), "world_server"}, "ws1", "GRANT ROLE world_server TO ws1;");
    b.emit(AlterUserOps{AddPassword{verifierAt(b.next())}}, "ws1", "ALTER USER ws1 ADD PASSWORD '<redacted>';");
    b.emit(AlterUserOps{DropPassword{1}}, "ws1", "ALTER USER ws1 DROP PASSWORD 1;");
    b.emit(RevokeOps{GranteeKind::Role, writeWorld}, "world_server", "REVOKE WRITE ON TOPIC PREFIX 'world.' FROM ROLE world_server;");
    b.emit(AlterUserOps{SetPassword{verifierAt(b.next()), false}}, "admin", "ALTER USER admin SET PASSWORD '<redacted>';");
    b.emit(AlterUserOps{DisableUser{}}, "ws1", "ALTER USER ws1 DISABLE;");
    b.emit(AlterUserOps{EnableUser{}}, "ws1", "ALTER USER ws1 ENABLE;");
    b.emit(RevokeRoleOps{b.roleId("world_server"), "world_server"}, "ws1", "REVOKE ROLE world_server FROM ws1;");
    b.emit(DropRoleOps{}, "world_server", "DROP ROLE world_server;");
    b.emit(DropUserOps{}, "ws1", "DROP USER ws1;");
    return b.take();
}

} // namespace rune::security::testing
