// Redact.cpp — the canonical, password-free text of a security statement (D10).
// Only this text is ever sent to the broker and recorded in __security.
#include "rune/ql/Ast.h"
#include <type_traits>

namespace rune::ql {
namespace {

constexpr const char* kRedacted = "'<redacted>'";

bool plainIdent(const std::string& n) noexcept {
    if (n.empty()) return false;
    for (std::size_t i = 0; i < n.size(); ++i) {
        const char c = n[i];
        const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        if (!(alpha || (i > 0 && c >= '0' && c <= '9'))) return false;
    }
    return true;
}
// A quote inside a literal is doubled (SQL) - the same rule as the leader's rune::security::securityStatementText.
std::string quoted(const std::string& n) {
    std::string out = "'";
    for (const char c : n) { out += c; if (c == '\'') out += '\''; }
    return out + "'";
}
std::string name(const std::string& n) { return plainIdent(n) ? n : quoted(n); }

const char* opText(SecOp o) noexcept {
    switch (o) {
        case SecOp::Read: return "READ";       case SecOp::Write: return "WRITE";
        case SecOp::Create: return "CREATE";   case SecOp::Alter: return "ALTER";
        case SecOp::Drop: return "DROP";       case SecOp::Describe: return "DESCRIBE";
        case SecOp::Admin: return "ADMIN";
    }
    return "?";
}
const char* granteeText(Grantee g) noexcept { return g == Grantee::User ? "USER" : "ROLE"; }

std::string resource(const SecPattern& p) {
    switch (p.type) {
        case SecResource::Cluster: return "CLUSTER";
        case SecResource::Topic: break;
        case SecResource::View: break;
        case SecResource::Group: break;
        case SecResource::TransactionalId: break;
    }
    const char* t = p.type == SecResource::Topic ? "TOPIC" : p.type == SecResource::View ? "VIEW"
                  : p.type == SecResource::Group ? "GROUP" : "TRANSACTIONAL_ID";
    return std::string(t) + (p.prefix ? " PREFIX " + quoted(p.name) : " " + name(p.name));
}

std::string asOf(const std::optional<AsOfPoint>& at) {
    if (!at) return {};
    if (at->by == AsOfPoint::By::Time) return " AS OF TIME " + quoted(at->timeIso);
    return " AS OF OFFSET " + std::to_string(at->number);
}

} // namespace

std::string redactedText(const Statement& s) {
    return std::visit([](const auto& d) -> std::string {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, CreateUserDef>)
            return "CREATE USER " + name(d.name) + " WITH PASSWORD " + kRedacted + ";";
        else if constexpr (std::is_same_v<T, AlterUserDef>) {
            const std::string h = "ALTER USER " + name(d.name) + " ";
            switch (d.change) {
                case AlterUserDef::Change::AddPassword:  return h + "ADD PASSWORD " + kRedacted + ";";
                case AlterUserDef::Change::DropPassword: return h + "DROP PASSWORD " + std::to_string(d.index) + ";";
                case AlterUserDef::Change::SetPassword:
                    return h + "SET PASSWORD " + kRedacted + (d.terminateSessions ? " TERMINATE SESSIONS;" : ";");
                case AlterUserDef::Change::Disable: return h + (d.force ? "DISABLE FORCE;" : "DISABLE;");
                case AlterUserDef::Change::Enable:  return h + "ENABLE;";
            }
            return {};
        }
        else if constexpr (std::is_same_v<T, DropUserDef>)   return "DROP USER " + name(d.name) + (d.force ? " FORCE;" : ";");
        else if constexpr (std::is_same_v<T, CreateRoleDef>) return "CREATE ROLE " + name(d.name) + ";";
        else if constexpr (std::is_same_v<T, DropRoleDef>)   return "DROP ROLE " + name(d.name) + (d.force ? " FORCE;" : ";");
        else if constexpr (std::is_same_v<T, GrantDef>)
            return std::string("GRANT ") + opText(d.op) + " ON " + resource(d.resource) + " TO " + granteeText(d.grantee) + " " + name(d.granteeName) + ";";
        else if constexpr (std::is_same_v<T, RevokeDef>)
            return std::string("REVOKE ") + opText(d.op) + " ON " + resource(d.resource) + " FROM " + granteeText(d.grantee) + " " + name(d.granteeName) + (d.force ? " FORCE;" : ";");
        else if constexpr (std::is_same_v<T, GrantRoleDef>)  return "GRANT ROLE " + name(d.role) + " TO USER " + name(d.user) + ";";
        else if constexpr (std::is_same_v<T, RevokeRoleDef>)
            return "REVOKE ROLE " + name(d.role) + " FROM USER " + name(d.user) + (d.force ? " FORCE;" : ";");
        else if constexpr (std::is_same_v<T, ShowUsersDef>)  return "SHOW USERS;";
        else if constexpr (std::is_same_v<T, ShowRolesDef>)  return "SHOW ROLES;";
        else if constexpr (std::is_same_v<T, ShowGrantsDef>)
            return std::string("SHOW GRANTS FOR ") + granteeText(d.who) + " " + name(d.name) + asOf(d.at) + ";";
        else if constexpr (std::is_same_v<T, ShowSecurityHistoryDef>) {
            std::string t = "SHOW SECURITY HISTORY";
            if (d.who) t += std::string(" ") + granteeText(*d.who) + " " + name(d.name);
            if (d.sinceIso) t += " SINCE " + quoted(*d.sinceIso);
            return t + ";";
        }
        else return {};   // catalog statements: the caller's source text is sent, never a re-rendering
    }, s);
}

bool isSecurityStatement(const Statement& s) noexcept { return s.index() >= 8; }

bool carriesPassword(const Statement& s) noexcept {
    if (std::holds_alternative<CreateUserDef>(s)) return true;
    if (const auto* a = std::get_if<AlterUserDef>(&s))
        return a->change == AlterUserDef::Change::AddPassword || a->change == AlterUserDef::Change::SetPassword;
    return false;
}

} // namespace rune::ql
