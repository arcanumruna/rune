// src/cli/QlEngine.cpp — the `rune ql` engine (see QlEngine.h): moved out of CatalogCommands.cpp unchanged, plus
// PrepareOptions (--validate / --expected-version), LiveOptions and QlRun::lastErrorCode / OutputStyle.
#include "QlEngine.h"

#include "rune/cli/CatalogCommands.h"
#include "rune/cli/SecurityCommands.h"

#include <rune/codec/AlterOps.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <cstdio>
#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <type_traits>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/view/ViewPlanAuthor.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <chrono>
#include <thread>
#endif

namespace rune::cli::engine {

using namespace rune::cli::support;

std::string hex16(uint64_t v) {
    char b[17];
    std::snprintf(b, sizeof b, "%016llx", static_cast<unsigned long long>(v));
    return b;
}

namespace {


// days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
int64_t daysFromCivil(int64_t y, int64_t m, int64_t d) noexcept {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t yoe = y - era * 400;
    const int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}
void civilFromDays(int64_t z, int64_t& y, int64_t& m, int64_t& d) noexcept {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int64_t doe = z - era * 146097;
    const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = yoe + era * 400;
    const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int64_t mp  = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp + (mp < 10 ? 3 : -9);
    y += m <= 2;
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ`
std::string formatUtcMs(int64_t ms) {
    int64_t secs = ms / 1000, rem = ms % 1000;
    if (rem < 0) { rem += 1000; --secs; }
    int64_t days = secs / 86400, sod = secs % 86400;
    if (sod < 0) { sod += 86400; --days; }
    int64_t y = 0, mo = 0, d = 0;
    civilFromDays(days, y, mo, d);
    char b[40];
    std::snprintf(b, sizeof b, "%04lld-%02lld-%02lldT%02lld:%02lld:%02lld.%03lldZ", static_cast<long long>(y),
                  static_cast<long long>(mo), static_cast<long long>(d), static_cast<long long>(sod / 3600),
                  static_cast<long long>(sod % 3600 / 60), static_cast<long long>(sod % 60), static_cast<long long>(rem));
    return b;
}

/// 'YYYY-MM-DDTHH:MM:SSZ' (rune-ql already validated the shape) → epoch ms.
std::optional<int64_t> parseIsoMs(std::string_view s) noexcept {
    if (s.size() != 20) return std::nullopt;
    auto num = [&](std::size_t at, std::size_t n) {
        int64_t v = 0;
        for (std::size_t i = 0; i < n; ++i) v = v * 10 + (s[at + i] - '0');
        return v;
    };
    return (daysFromCivil(num(0, 4), num(5, 2), num(8, 2)) * 86400 + num(11, 2) * 3600 + num(14, 2) * 60 + num(17, 2)) *
           1000;
}

std::string oneLine(std::string s) {
    for (char& c : s)
        if (c == '\n' || c == '\r' || c == '\t') c = ' ';
    return s;
}

std::string principalText(const cat::Principal& p) {
    if (!p.authenticated.empty()) return p.authenticated;
    if (p.clientName.empty() && p.sourceAddress.empty()) return "-";
    return p.clientName + "@" + p.sourceAddress;
}
} // namespace

std::string orDash(std::string s) { return s.empty() ? std::string("-") : s; }

std::array<std::string, kHistoryColumns> historyColumns(const cat::RecordedEvent& r) {
    const cat::CatalogEvent& e = r.event;
    std::string replayed = "-";
    if (e.replayedFrom) replayed = e.replayedFrom->clusterId + "@" + std::to_string(e.replayedFrom->offset);
    return {std::to_string(r.offset),
            formatUtcMs(e.timestampMs),
            std::string(cat::kindName(e.kind)),
            orDash(e.objectName) + "(" + std::to_string(e.objectId) + ")",
            std::to_string(e.prevVersion) + "\xE2\x86\x92" + std::to_string(e.newVersion),   // prev→new
            principalText(e.principal),
            std::move(replayed),
            orDash(oneLine(e.statement))};
}


const char* persistenceName(cat::Persistence p) noexcept { return p == cat::Persistence::Transient ? "transient" : "disk"; }
const char* compatName(rune::codec::CompatMode m) noexcept {
    return m == rune::codec::CompatMode::Forward ? "forward" : "backward";
}

std::string descriptorText(const std::vector<uint8_t>& blob) {
    try {
        return rune::codec::toText(rune::codec::deserialize(std::span<const uint8_t>(blob.data(), blob.size())));
    } catch (const std::exception& e) {
        return std::string("(undecodable descriptor: ") + e.what() + ")\n";
    }
}

std::string_view errorCodeName(uint16_t code) noexcept {
    static constexpr std::string_view kNames[] = {
        "OK", "ProtocolVersion", "NotLeader", "Redirect", "SessionExpired", "UnknownSession", "Internal",
        "UnknownTopic", "UnknownPartition", "NotPartitionLeader", "MinIsrViolation", "StaleGeneration",
        "UnknownMember", "RebalanceInProgress", "NotCoordinator", "IllegalGeneration", "GroupDead",
        "InvalidProducerEpoch", "ProducerFenced", "InvalidTxnState", "OutOfOrderSequence", "DuplicateSequence",
        "MessageTooLarge", "InvalidSchema", "SchemaValidationFailed", "TopicExists", "StorageError",
        "ConcurrentTransactions", "UnknownProducerId", "HandshakeFailed", "SchemaIncompatible", "SchemaConflict",
        "ViewExists", "ViewSourceNotFound", "ViewFieldUnresolved", "ViewKeyInvalid", "ViewNotCoPartitioned",
        "ViewSourceTransient", "ViewNotAlterable", "InvalidRequest", "InvalidTopicName", "ViewSourceIsView",
        "TransientLogFull", "PartitionDataReset", "AuthenticationFailed", "NotAuthorized", "SessionRevoked",
        "PasswordChangeRequired"};
#ifdef RUNE_CLI_LIVE_ENABLED
    static_assert(std::size(kNames) == static_cast<std::size_t>(rune::protocol::client::ErrorCode::PasswordChangeRequired) + 1,
                  "errorCodeName: one name per rune::protocol::client::ErrorCode");
    static_assert(static_cast<uint16_t>(rune::protocol::client::ErrorCode::TopicExists) == 25,
                  "errorCodeName: the table is indexed by the enum's values");
#endif
    return code < std::size(kNames) ? kNames[code] : std::string_view("UnknownErrorCode");
}

/// `<CodeName> (<code>) <detail>`
std::string brokerFailure(uint16_t code, const std::string& detail) {
    std::string s(errorCodeName(code));
    s += " (" + std::to_string(code) + ")";
    if (!detail.empty()) s += " " + detail;
    return s;
}

namespace {
constexpr uint64_t kMaxVersion = 65535;   // schema versions are u16: never truncated
constexpr uint64_t kMaxObjectVersion = 4294967295ull;   // topic / view object versions are u32

cat::ObjectType objectType(ql::ObjectKind k) noexcept {
    switch (k) {
        case ql::ObjectKind::Topic:  return cat::ObjectType::Topic;
        case ql::ObjectKind::View:   return cat::ObjectType::View;
        case ql::ObjectKind::Schema: return cat::ObjectType::Schema;
    }
    return cat::ObjectType::Topic;
}

} // namespace

std::string requestIdHex(const cat::RequestId& id) { return hex16(id.hi) + hex16(id.lo); }

/// Parse-and-plan one statement.  nullopt + `why` when it is refused (versions beyond u16, a bad
/// setting, an undecodable time).
namespace {
std::optional<Prepared> prepareSecurity(const ql::Statement& st, std::string& why) {
    if (const auto* u = std::get_if<ql::ShowUsersDef>(&st)) { (void)u; PSecShow p; p.kind = PSecShow::Kind::Users; p.text = ql::redactedText(st); return Prepared{std::move(p)}; }
    if (std::holds_alternative<ql::ShowRolesDef>(st)) { PSecShow p; p.kind = PSecShow::Kind::Roles; p.text = ql::redactedText(st); return Prepared{std::move(p)}; }
    if (const auto* g = std::get_if<ql::ShowGrantsDef>(&st)) {
        PSecShow p;
        p.kind = PSecShow::Kind::Grants;
        p.who  = g->who;
        p.name = g->name;
        p.text = ql::redactedText(st);
        if (g->at) {
            switch (g->at->by) {
                case ql::AsOfPoint::By::Offset: p.at = sec::AsOf{sec::AsOf::By::Offset, g->at->number}; break;
                case ql::AsOfPoint::By::Time: {
                    const auto ms = parseIsoMs(g->at->timeIso);
                    if (!ms) { why = "invalid time '" + g->at->timeIso + "'"; return std::nullopt; }
                    p.at = sec::AsOf{sec::AsOf::By::Time, *ms};
                    break;
                }
                case ql::AsOfPoint::By::Version:
                    why = "SHOW GRANTS … AS OF VERSION is not defined: use AS OF OFFSET <n> or AS OF TIME '<iso>'";
                    return std::nullopt;
            }
        }
        return Prepared{std::move(p)};
    }
    if (const auto* h = std::get_if<ql::ShowSecurityHistoryDef>(&st)) {
        PSecShow p;
        p.kind = PSecShow::Kind::History;
        p.text = ql::redactedText(st);
        if (h->who) p.filter.kind = *h->who;
        p.filter.name = h->name;
        if (h->sinceIso) {
            const auto ms = parseIsoMs(*h->sinceIso);
            if (!ms) { why = "invalid time '" + *h->sinceIso + "'"; return std::nullopt; }
            p.filter.sinceMs = *ms;
        }
        return Prepared{std::move(p)};
    }
    auto plan = ql::applySecurityPlan(st);
    if (!plan) { why = plan.error().message; return std::nullopt; }
    return Prepared{PSecurity{std::move(*plan)}};
}

} // namespace

std::optional<Prepared> prepare(const ql::Statement& st, const PrepareOptions& opts, std::string& why) {
    auto checkVersion = [&](int64_t v, std::string_view kind, uint64_t max) {
        if (v < 0 || static_cast<uint64_t>(v) > max) {
            why = "version " + std::to_string(v) + " is out of range: versions of a " + std::string(kind) + " are 0.." +
                  std::to_string(max);
            return false;
        }
        return true;
    };
    try {
        return std::visit(
            [&](const auto& s) -> std::optional<Prepared> {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, ql::TopicDef>) {
                    return Prepared{PDefine{ql::applyPlan(s), opts.validate}};
                } else if constexpr (std::is_same_v<T, ql::AlterTopicDef>) {
                    return Prepared{PAlter{ql::applyAlterPlan(s, opts.expectedVersion)}};
                } else if constexpr (std::is_same_v<T, ql::AlterTopicConfigDef>) {
                    auto plan = ql::applyConfigPlan(s);
                    if (!plan) {
                        why = plan.error().message;
                        return std::nullopt;
                    }
                    return Prepared{PAlterConfig{std::move(*plan)}};
                } else if constexpr (std::is_same_v<T, ql::DropTopicDef>) {
                    return Prepared{PDropTopic{s.name}};
                } else if constexpr (std::is_same_v<T, ql::DropViewDef>) {
                    return Prepared{PDropView{s.name}};
                } else if constexpr (std::is_same_v<T, ql::ShowHistoryDef>) {
                    PHistory p;
                    p.text = "show history";
                    if (s.kind) {
                        p.filter.type = objectType(*s.kind);
                        p.filter.name = s.name;
                        p.text += " " + std::string(cat::objectTypeName(*p.filter.type)) + " " + s.name;
                    }
                    if (s.sinceIso) {
                        const auto ms = parseIsoMs(*s.sinceIso);
                        if (!ms) {
                            why = "invalid time '" + *s.sinceIso + "'";
                            return std::nullopt;
                        }
                        p.filter.sinceMs = *ms;
                        p.text += " since '" + *s.sinceIso + "'";
                    }
                    return Prepared{std::move(p)};
                } else if constexpr (std::is_same_v<T, ql::ShowAsOfDef>) {
                    PAsOf p;
                    p.kind = objectType(s.kind);
                    p.name = s.name;
                    p.at.type = p.kind;
                    p.at.name = s.name;
                    p.text = "show " + std::string(cat::objectTypeName(p.kind)) + " " + s.name + " as of ";
                    switch (s.at.by) {
                        case ql::AsOfPoint::By::Version:
                            if (!checkVersion(s.at.number, cat::objectTypeName(p.kind),
                                              p.kind == cat::ObjectType::Schema ? kMaxVersion : kMaxObjectVersion))
                                return std::nullopt;
                            p.at.by = cat::AsOf::By::Version;
                            p.at.value = s.at.number;
                            p.text += "version " + std::to_string(s.at.number);
                            break;
                        case ql::AsOfPoint::By::Offset:
                            p.at.by = cat::AsOf::By::Offset;
                            p.at.value = s.at.number;
                            p.text += "offset " + std::to_string(s.at.number);
                            break;
                        case ql::AsOfPoint::By::Time: {
                            const auto ms = parseIsoMs(s.at.timeIso);
                            if (!ms) {
                                why = "invalid time '" + s.at.timeIso + "'";
                                return std::nullopt;
                            }
                            p.at.by = cat::AsOf::By::TimeMs;
                            p.at.value = *ms;
                            p.text += "time '" + s.at.timeIso + "'";
                            break;
                        }
                    }
                    return Prepared{std::move(p)};
                } else if constexpr (!std::is_same_v<T, ql::DiffDef>) {
                    return prepareSecurity(st, why);   // user / role / grant DDL and SHOW … (authentication Task 8)
                } else {
                    PDiff p;
                    if (s.scope == ql::DiffDef::Scope::Schema) {
                        if (!checkVersion(s.from, "schema", kMaxVersion) || !checkVersion(s.to, "schema", kMaxVersion)) return std::nullopt;
                        p.a = {cat::AsOf::By::Version, s.from, cat::ObjectType::Schema, s.name};
                        p.b = {cat::AsOf::By::Version, s.to, cat::ObjectType::Schema, s.name};
                        p.onlyTopic = s.name;
                        p.restrictToTopic = true;
                        p.text = "diff schema " + s.name + " version " + std::to_string(s.from) + " to " + std::to_string(s.to);
                    } else {
                        p.a = {cat::AsOf::By::Offset, s.from, cat::ObjectType::Topic, {}};
                        p.b = {cat::AsOf::By::Offset, s.to, cat::ObjectType::Topic, {}};
                        p.text = "diff catalog offset " + std::to_string(s.from) + " to " + std::to_string(s.to);
                    }
                    return Prepared{std::move(p)};
                }
            },
            st);
    } catch (const std::exception& e) {
        why = e.what();
        return std::nullopt;
    }
}

void printPlan(const Prepared& p, const std::optional<cat::RequestId>& rid, std::ostream& out) {
    std::visit(
        [&](const auto& s) {
            using T = std::decay_t<decltype(s)>;
            if constexpr (std::is_same_v<T, PDefine>) {
                const auto& d = s.plan;
                out << "topic " << d.name << " partitions " << d.partitions << " replicas " << d.replicas << " persistence "
                    << (d.persistence == ql::Persistence::Transient ? "transient" : "disk");
                if (!d.keyField.empty()) out << " key " << d.keyField;
                if (s.validate) out << " validate";
                out << "\n";
                if (d.descriptorBlob.empty()) out << "  (untyped)\n";
                else out << descriptorText(d.descriptorBlob);
            } else if constexpr (std::is_same_v<T, PAlter>) {
                out << "alter topic " << s.plan.name << " (" << s.plan.opsBlob.size() << " byte op list)\n";
                try {
                    if (auto ops = rune::codec::decodeAlterOps(
                            std::span<const uint8_t>(s.plan.opsBlob.data(), s.plan.opsBlob.size()))) {
                        for (const auto& op : *ops) {
                            out << "  ";
                            switch (op.type) {
                                case rune::codec::AlterOpType::Add: out << "ADD " << op.name; break;
                                case rune::codec::AlterOpType::Drop: out << "DROP " << op.name; break;
                                case rune::codec::AlterOpType::Rename: out << "RENAME " << op.name << " -> " << op.newName; break;
                                case rune::codec::AlterOpType::SetCompatibility:
                                    out << "SET_COMPATIBILITY " << compatName(op.mode);
                                    break;
                            }
                            out << "\n";
                        }
                    }
                } catch (const std::exception&) {
                }
                if (s.plan.expectedVersion != 0) out << "  expected version " << s.plan.expectedVersion << "\n";
            } else if constexpr (std::is_same_v<T, PAlterConfig>) {
                out << "alter topic " << s.plan.name << " set";
                for (const auto& [key, value] : s.plan.changes) out << " key#" << int{key} << " = " << value;
                out << "\n";
            } else if constexpr (std::is_same_v<T, PDropTopic>) {
                out << "drop topic " << s.name << "\n";
            } else if constexpr (std::is_same_v<T, PDropView>) {
                out << "drop view " << s.name << "\n";
            } else if constexpr (std::is_same_v<T, PSecurity>) {
                out << s.plan.statement << "\n";   // the REDACTED statement (D10): a password literal never prints
            } else {
                out << s.text << "\n";
            }
        },
        p);
    if (rid) out << "  request-id " << requestIdHex(*rid) << "\n";
}

/// Reads the whole script text: a file, or @p stdinStream for "-".
bool readScript(const std::string& path, std::istream& stdinStream, std::string& text, std::ostream& err) {
    if (path == "-") {
        std::ostringstream buf;
        buf << stdinStream.rdbuf();
        text = buf.str();
        return true;
    }
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        err << "rune: cannot open file: " << path << "\n";
        return false;
    }
    std::ostringstream buf;
    buf << f.rdbuf();
    if (f.bad()) {
        err << "rune: cannot read file: " << path << "\n";
        return false;
    }
    text = buf.str();
    return true;
}

/// The text of each ';'-terminated statement of `src`, comments (`-- …`) and blanks between
/// statements dropped, ';' inside a '…' string kept (the rune-ql lexical rules).
std::vector<std::string> splitStatements(std::string_view src) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < src.size()) {
        // skip blanks / comments before a statement
        while (i < src.size()) {
            const char c = src[i];
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') ++i;
            else if (c == '-' && i + 1 < src.size() && src[i + 1] == '-') { while (i < src.size() && src[i] != '\n') ++i; }
            else break;
        }
        if (i >= src.size()) break;
        const std::size_t start = i;
        bool inString = false;
        bool ended = false;
        while (i < src.size()) {
            const char c = src[i++];
            if (inString) { if (c == '\'') inString = false; }
            else if (c == '\'') inString = true;
            else if (c == '-' && i < src.size() && src[i] == '-') { while (i < src.size() && src[i] != '\n') ++i; }
            else if (c == ';') { ended = true; break; }
        }
        std::string text(src.substr(start, i - start));
        if (!ended) { out.push_back(std::move(text)); break; }   // unterminated tail: parseScript refused it already
        out.push_back(std::move(text));
    }
    return out;
}

#ifdef RUNE_CLI_LIVE_ENABLED

static std::string eventLogReadFailure(std::string_view what, const rc::CatalogReadError& e) {
    static constexpr const char* kKinds[] = {"timed out", "verification failed", "client closed", "refused by the broker",
                                             "cache full", "record too large"};
    std::string s(what);
    s += " read ";
    s += kKinds[static_cast<std::size_t>(e.kind)];
    if (!e.detail.empty()) s += ": " + e.detail;
    if (e.brokerErrorCode != 0) s += " (" + brokerFailure(e.brokerErrorCode, {}) + ")";
    return s;
}
std::string catalogReadFailure(const rc::CatalogReadError& e) { return eventLogReadFailure("catalog", e); }
std::string securityReadFailure(const rc::CatalogReadError& e) { return eventLogReadFailure("security", e); }

int64_t steadyNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
/// Waits (bounded by kCatalogWait) until the client's catalog reader holds `rid`.  The seed broker
/// serves the catalog up to ITS OWN high watermark, which can trail the leader that answered the DDL.
std::optional<cat::RequestRecord> awaitOwnWrite(rc::Client& c, const cat::RequestId& rid, std::string& why) {
    auto& reader = c.catalogReader();
    const int64_t deadline = steadyNowMs() + std::chrono::duration_cast<std::chrono::milliseconds>(kCatalogWait).count();
    return awaitRequestVisible(
        [&]() -> std::optional<std::string> {
            const int64_t left = std::max<int64_t>(1, deadline - steadyNowMs());
            if (auto r = reader.refresh(std::chrono::milliseconds(left)); !r) return catalogReadFailure(r.error());
            return std::nullopt;
        },
        [&] { return reader.state().request(rid); }, steadyNowMs,
        [] { std::this_thread::sleep_for(std::chrono::milliseconds(20)); }, deadline, why);
}


LiveOptions liveOptionsOf(const support::ParsedArgs& a, const std::string& endpoint, bool verbose) {
    LiveOptions o;
    o.endpoint     = endpoint;
    o.egress       = a.value("--cli-egress");
    o.streamIn     = a.value("--cli-stream-in");
    o.streamOut    = a.value("--cli-stream-out");
    o.dataDir      = a.value("--cli-data-dir");
    o.verbose      = verbose;
    o.user         = a.value("--user");
    o.passwordFile = a.value("--password-file");
    return o;
}

bool LiveBroker::open(const LiveOptions& o, std::ostream& err, bool withCredentials, bool quietFailure) {
    rc::ClientConfig cfg;
    cfg.gatewayChannel = toChannel(o.endpoint);
    cfg.clientName     = "rune-cli";
    cfg.runnerMode     = rc::RunnerMode::Invoker;
    cfg.traceDdlRetries = o.verbose;
    std::string egress = o.egress;
    if (egress.empty()) {
        if (const char* env = std::getenv("RUNE_EGRESS"); env != nullptr) egress = env;
    }
    if (egress.empty()) {
        if (const std::string ep = udpEndpointOf(cfg.gatewayChannel); !ep.empty()) {
            std::string why;
            const auto local = localEgressFor(ep, why);
            if (!local) {
                err << "rune: " << why << "; set --cli-egress (or RUNE_EGRESS)\n";
                return false;
            }
            egress = *local;
        }
    }
    if (!egress.empty()) cfg.egressChannel = egress;
    try {
        if (!o.streamIn.empty()) cfg.gatewayStreamId = std::stoi(o.streamIn);
        if (!o.streamOut.empty()) cfg.egressStreamId = std::stoi(o.streamOut);
    } catch (const std::exception&) {
        err << "rune: --cli-stream-in/--cli-stream-out must be integers\n";
        return false;
    }
    if (!o.dataDir.empty()) {
        cfg.dataDir = o.dataDir;   // the caller's directory, used as is (and left in place)
    } else {   // a private scratch directory per client, removed on close
        dir_ = ScopedDir(std::filesystem::temp_directory_path() /
                         ("rune-cli-" + std::to_string(::getpid()) + "-" + std::to_string(reinterpret_cast<std::uintptr_t>(this)) + "-" +
                          std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())));
        cfg.dataDir = dir_.path().string();
    }
    // Authentication (Task 8): who to log in as.  --user / RUNE_USER and --password-file / RUNE_PASSWORD_FILE;
    // a password is NEVER taken from argv (parseArgs refuses --password) or from the environment.
    // Issue #121: the only connection the CLI opens without credentials is `rune security bootstrap`'s.
    cfg.securityBootstrapOnly = !withCredentials;
    if (withCredentials) {
        applyCredentials(cfg, o.user, o.passwordFile);
        if (!o.password.empty()) {   // an interactively prompted password replaces any file (the client refuses both)
            cfg.password = o.password;
            cfg.passwordFile.clear();
        }
    }
    ddlTimeout_ = std::chrono::milliseconds(cfg.ddlTimeoutMs);
    verbose_ = o.verbose;
    try {
        client_ = std::make_unique<rc::Client>(cfg);
        if (!cfg.password.empty()) rc::detail::secureZero(cfg.password.data(), cfg.password.size());
        client_->connectAndWait(kConnectWait);
        printDevBannerIfAny(*client_, err);   // dev mode: say so, loudly
        return true;
    } catch (const std::exception& e) {
        lastFailure_ = e.what();
        if (std::string_view(e.what()).find("timed out") != std::string_view::npos)
            err << "rune: connect to " << o.endpoint << " timed out\n";
        else if (!quietFailure)
            err << "rune error: " << e.what() << "\n";
        close();
        return false;
    }
}

void LiveBroker::close() noexcept {
    if (client_) {
        try {
            client_->close();
        } catch (...) {
        }
        client_.reset();
    }
    dir_ = ScopedDir();
}

void LiveBroker::flushVerbose(std::ostream& err) {
    if (!verbose_ || !client_) return;
    std::vector<rc::Client::DdlRetryNote> notes;
    const std::size_t dropped = client_->takeDdlRetryNotes(notes);
    for (const auto& n : notes) {
        if (n.kind == rc::Client::DdlRetryNote::Kind::Redirect)
            err << "rune: redirect " << n.redirects << "/" << rc::Client::kMaxDdlRedirects << " -> " << n.text << "\n";
        else
            err << "rune: NotLeader" << (n.text.empty() ? "" : " (" + n.text + ")") << ", retrying in " << n.backoffMs
                << " ms\n";
    }
    if (dropped != 0) err << "rune: " << dropped << " further retry notes dropped\n";
}


/// A read that follows a DDL waits until the reader (the seed broker's own HW) shows that DDL.
bool QlRun::awaitLastDdl() {
    if (!lastDdl) return true;
    std::string why;
    if (awaitOwnWrite(broker.client(), *lastDdl, why)) return true;
    return fail("the catalog read did not show this script's last DDL (request id " + requestIdHex(*lastDdl) + "): " + why);
}

bool QlRun::failRead(const std::string& msg, const rc::CatalogReadError& e) {
    lastErrorCode = e.brokerErrorCode;
    return fail(msg);
}

bool QlRun::failBroker(uint16_t code, const std::string& detail) {
    lastErrorCode = code;
    return fail(brokerFailure(code, detail));
}

bool QlRun::fail(const std::string& msg) {
    broker.flushVerbose(err);
    if (script) err << (origin.empty() ? std::string{} : origin + ":") << "statement " << index << ": " << msg << "\n";
    else err << "rune: " << msg << "\n";
    return false;
}

bool QlRun::exec(const Prepared& p, const std::string& text, const std::optional<cat::RequestId>& rid) {
    lastErrorCode = 0;
    rc::DdlOptions ddl;
    ddl.statement = text;
    if (rid) ddl.requestId = *rid;
    return guarded([&] { return std::visit([&](const auto& s) { return run(s, ddl); }, p); });
}

bool QlRun::run(const PDefine& s, const rc::DdlOptions& ddl) {
    const auto& d = s.plan;
    rc::TopicOptions topic;
    topic.replicationFactor = d.replicas;
    topic.config = d.config;   // F32: the whole struct, never field by field
    const auto r = broker.client().defineTopicAndWait(
        d.name, d.partitions, d.keyField, std::span<const uint8_t>(d.descriptorBlob.data(), d.descriptorBlob.size()),
        s.validate, d.persistence == ql::Persistence::Transient ? rc::Persistence::Transient : rc::Persistence::Disk, topic,
        ddl, broker.ddlTimeout());
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastDdl = r.requestId;
    if (style == OutputStyle::Console) {
        out << "ok " << (d.persistence == ql::Persistence::Transient ? "transient topic " : "topic ") << d.name;
        if (!d.descriptorBlob.empty()) out << " v" << r.version;
        out << " (" << d.partitions << (d.partitions == 1 ? " partition)" : " partitions)") << "\n";
    } else {
        out << "created topic '" << d.name << "' topicId=" << r.topicId << " version=" << r.version << "\n";
    }
    return true;
}
bool QlRun::run(const PAlter& s, const rc::DdlOptions& ddl) {
    const auto& d = s.plan;
    const auto r = broker.client().alterTopicAndWait(
        d.name, d.expectedVersion, std::span<const uint8_t>(d.opsBlob.data(), d.opsBlob.size()), ddl, broker.ddlTimeout());
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastDdl = r.requestId;
    if (style == OutputStyle::Console) out << "ok topic " << d.name << " v" << r.newVersion << "\n";
    else out << "altered topic '" << d.name << "' version=" << r.newVersion << "\n";
    return true;
}
bool QlRun::run(const PAlterConfig& s, const rc::DdlOptions& ddl) {
    std::vector<rpc::ConfigChangeWire> changes;
    for (const auto& [key, value] : s.plan.changes) changes.push_back(rpc::ConfigChangeWire{key, value});
    const auto r = broker.client().alterTopicConfigAndWait(s.plan.name, 0, changes, ddl, broker.ddlTimeout());
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastDdl = r.requestId;
    if (style == OutputStyle::Console) out << "ok topic " << s.plan.name << " config v" << r.newVersion << "\n";
    else out << "altered config of topic '" << s.plan.name << "' version=" << r.newVersion << "\n";
    return true;
}
bool QlRun::run(const PDropTopic& s, const rc::DdlOptions& ddl) {
    const auto r = broker.client().dropTopicAndWait(s.name, ddl, broker.ddlTimeout());
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastDdl = r.requestId;
    if (style == OutputStyle::Console) out << "ok dropped topic " << s.name << "\n";
    else out << "dropped topic '" << s.name << "'\n";
    return true;
}
// ---- authentication (Task 8): security DDL and SHOW … ---------------------------------------------------------
bool QlRun::awaitLastSecurityDdl() {
    if (!lastSecurityDdl) return true;
    auto& reader = broker.client().securityReader();
    const int64_t deadline = steadyNowMs() + std::chrono::duration_cast<std::chrono::milliseconds>(kCatalogWait).count();
    std::string why = "not visible before the deadline";
    for (;;) {
        const int64_t left = std::max<int64_t>(1, deadline - steadyNowMs());
        if (auto r = reader.refresh(std::chrono::milliseconds(left)); !r) {
            why = securityReadFailure(r.error());
            lastErrorCode = r.error().brokerErrorCode;
            break;
        }
        if (reader.state().request(*lastSecurityDdl)) return true;
        if (steadyNowMs() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fail("the security read did not show this script's last DDL (request id " + requestIdHex(*lastSecurityDdl) +
                "): " + why);
}

bool QlRun::run(const PSecurity& s, const rc::DdlOptions& ddl) {
    ql::SecurityPlan plan = s.plan;   // the client wipes this copy; the Prepared's own is wiped by its destructor
    const auto r = broker.client().securityDdlAndWait(std::move(plan), ddl, broker.ddlTimeout());
    s.wipe();
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastSecurityDdl = r.requestId;
    out << "ok: " << s.plan.statement << " (object " << r.objectId << ", version " << r.newVersion << ")\n";
    return true;
}

bool QlRun::run(const PSecShow& s, const rc::DdlOptions&) {
    if (!awaitLastSecurityDdl()) return false;
    auto& c = broker.client();
    switch (s.kind) {
        case PSecShow::Kind::Users: {
            const auto r = c.securityUsers(kCatalogWait);
            if (!r) return failRead(securityReadFailure(r.error()), r.error());
            out << formatUsersTable(*r);
            return true;
        }
        case PSecShow::Kind::Roles: {
            const auto r = c.securityRoles(kCatalogWait);
            if (!r) return failRead(securityReadFailure(r.error()), r.error());
            out << formatRolesTable(*r);
            return true;
        }
        case PSecShow::Kind::Grants: {
            const auto r = c.securityGrantsFor(s.who, s.name, s.at, kCatalogWait);
            if (!r) return failRead(securityReadFailure(r.error()), r.error());
            out << formatGrantsTable(*r);
            return true;
        }
        case PSecShow::Kind::History: {
            const auto r = c.securityHistory(s.filter, kCatalogWait);
            if (!r) return failRead(securityReadFailure(r.error()), r.error());
            out << formatSecurityHistoryTable(*r);
            return true;
        }
    }
    return false;
}

bool QlRun::run(const PDropView& s, const rc::DdlOptions& ddl) {
    const auto r = broker.client().dropViewAndWait(s.name, ddl, broker.ddlTimeout());
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastDdl = r.requestId;
    if (style == OutputStyle::Console) out << "ok dropped view " << s.name << "\n";
    else out << "dropped view '" << s.name << "'\n";
    return true;
}

bool QlRun::run(const PHistory& s, const rc::DdlOptions&) {
    if (!awaitLastDdl()) return false;
    const auto r = broker.client().catalogHistory(s.filter, kCatalogWait);
    if (!r) return failRead(catalogReadFailure(r.error()), r.error());
    static const std::array<std::string, kHistoryColumns> kHeader = {"offset", "time(UTC)", "kind", "object(objectId)",
                                                                     "version", "principal", "replayedFrom", "statement"};
    std::vector<std::array<std::string, kHistoryColumns>> rows{kHeader};
    for (const cat::RecordedEvent& e : *r) rows.push_back(historyColumns(e));
    std::array<std::size_t, kHistoryColumns> width{};
    for (const auto& row : rows)
        for (std::size_t c = 0; c < kHistoryColumns; ++c) width[c] = std::max(width[c], row[c].size());
    for (const auto& row : rows) {
        for (std::size_t c = 0; c < kHistoryColumns; ++c) {
            out << row[c];
            if (c + 1 != kHistoryColumns) out << std::string(width[c] - row[c].size() + 2, ' ');
        }
        out << "\n";
    }
    return true;
}

bool QlRun::run(const PAsOf& s, const rc::DdlOptions&) {
    if (!awaitLastDdl()) return false;
    const auto r = broker.client().catalogAsOf(s.at, kCatalogWait);
    if (!r) return failRead(catalogReadFailure(r.error()), r.error());
    if (s.kind == cat::ObjectType::View) {
        const cat::ViewObject* v = r->viewByName(s.name);
        if (v == nullptr) return fail("no view '" + s.name + "' at that point of the catalog");
        out << formatViewDefinition(*v);
        return true;
    }
    const cat::TopicObject* t = r->topicByName(s.name);
    if (t == nullptr) return fail("no topic '" + s.name + "' at that point of the catalog");
    out << (s.kind == cat::ObjectType::Schema ? formatSchemaDefinition(*t) : formatTopicDefinition(*t));
    return true;
}

bool QlRun::run(const PDiff& s, const rc::DdlOptions&) {
    if (!awaitLastDdl()) return false;
    const auto r = broker.client().catalogDiff(s.a, s.b, kCatalogWait);
    if (!r) return failRead(catalogReadFailure(r.error()), r.error());
    std::size_t shown = 0;
    for (const cat::DiffEntry& d : *r) {
        if (s.restrictToTopic && !(d.type == cat::ObjectType::Topic && d.name == s.onlyTopic)) continue;
        out << formatDiffEntry(d) << "\n";
        ++shown;
    }
    if (shown == 0) out << "(no differences)\n";
    return true;
}

#endif   // RUNE_CLI_LIVE_ENABLED

} // namespace rune::cli::engine
