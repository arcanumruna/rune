// SecurityStatementText.cpp — see the header.  Mirrors rune::ql::redactedText (ql/src/Redact.cpp) rule for rule.
#include <rune/security/SecurityStatementText.h>

namespace rune::security {
namespace {

constexpr const char* kRedacted = "'<redacted>'";

bool plainIdent(std::string_view n) noexcept {
    if (n.empty()) return false;
    for (std::size_t i = 0; i < n.size(); ++i) {
        const char c = n[i];
        const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        if (!(alpha || (i > 0 && c >= '0' && c <= '9'))) return false;
    }
    return true;
}
// A string literal: a quote inside is doubled (SQL), so no name or pattern can close the literal and make the recorded
// text read as another statement (PR #525 review: a raw wire client's pattern is not bound by the QL lexer).
std::string quoted(std::string_view n) {
    std::string out = "'";
    for (const char c : n) { out += c; if (c == '\'') out += '\''; }
    return out + "'";
}
std::string ident(std::string_view n) { return plainIdent(n) ? std::string(n) : quoted(n); }

// SecurityOperation: Read 0, Write 1, Create 2, Alter 3, Drop 4, Describe 5, Admin 6, Truncate 7.
const char* opText(uint8_t o) noexcept {
    switch (o) {
        case 0: return "READ";   case 1: return "WRITE";    case 2: return "CREATE"; case 3: return "ALTER";
        case 4: return "DROP";   case 5: return "DESCRIBE"; case 6: return "ADMIN"; case 7: return "TRUNCATE";
        default: return nullptr;
    }
}

// SecurityResourceType: Cluster 0, Topic 1, View 2, Group 3, TransactionalId 4, Namespace 5.  False: unknown.
bool resource(uint8_t type, uint8_t patternKind, std::string_view pattern, std::string& out) {
    if (patternKind > 1) return false;
    const char* t = nullptr;
    switch (type) {
        case 0: out = "CLUSTER"; return true;
        case 1: t = "TOPIC"; break;
        case 2: t = "VIEW"; break;
        case 3: t = "GROUP"; break;
        case 4: t = "TRANSACTIONAL_ID"; break;
        case 5: t = "NAMESPACE"; break;
        default: return false;
    }
    out = std::string(t) + (patternKind == 1 ? " PREFIX " + quoted(pattern) : " " + ident(pattern));
    return true;
}

} // namespace

std::string securityStatementText(uint8_t kind, std::string_view name, uint8_t change, uint8_t passwordIndex,
                                  uint8_t terminateSessions, uint8_t force, uint8_t granteeKind, uint8_t op,
                                  uint8_t resourceType, uint8_t patternKind, std::string_view pattern,
                                  std::string_view roleName) {
    const char* forceTail = force != 0 ? " FORCE;" : ";";
    switch (kind) {
        case 1: return "CREATE USER " + ident(name) + " WITH PASSWORD " + kRedacted + ";";
        case 2: {
            const std::string h = "ALTER USER " + ident(name) + " ";
            switch (change) {
                case 0: return h + "ADD PASSWORD " + kRedacted + ";";
                case 1: return h + "DROP PASSWORD " + std::to_string(passwordIndex) + ";";
                case 2: return h + "SET PASSWORD " + kRedacted + (terminateSessions != 0 ? " TERMINATE SESSIONS;" : ";");
                case 3: return h + (force != 0 ? "DISABLE FORCE;" : "DISABLE;");
                case 4: return h + "ENABLE;";
                default: return {};
            }
        }
        case 3: return "DROP USER " + ident(name) + forceTail;
        case 4: return "CREATE ROLE " + ident(name) + ";";
        case 5: return "DROP ROLE " + ident(name) + forceTail;
        case 6: case 7: {
            const char* o = opText(op);
            std::string res;
            if (o == nullptr || granteeKind > 1 || !resource(resourceType, patternKind, pattern, res)) return {};
            const char* who = granteeKind == 0 ? "USER" : "ROLE";
            if (kind == 6) return std::string("GRANT ") + o + " ON " + res + " TO " + who + " " + ident(name) + ";";
            return std::string("REVOKE ") + o + " ON " + res + " FROM " + who + " " + ident(name) + forceTail;
        }
        case 8: return "GRANT ROLE " + ident(roleName) + " TO USER " + ident(name) + ";";
        case 9: return "REVOKE ROLE " + ident(roleName) + " FROM USER " + ident(name) + forceTail;
        default: return {};
    }
}

} // namespace rune::security
