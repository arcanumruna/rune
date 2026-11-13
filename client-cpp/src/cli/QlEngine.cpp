// src/cli/QlEngine.cpp — the `rune ql` engine (see QlEngine.h): moved out of CatalogCommands.cpp unchanged, plus
// PrepareOptions (--expected-version), LiveOptions and QlRun::lastErrorCode / OutputStyle.
#include "QlEngine.h"

#include "rune/cli/CatalogCommands.h"
#include "rune/cli/SecurityCommands.h"

#include <rune/client/Completion.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <charconv>
#include <cstdio>
#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <poll.h>
#include <rune/client/detail/SecureZero.h>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/view/ViewKeyBuilder.h>
#include <rune/client/view/ViewPlanAuthor.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <chrono>
#include <thread>
#endif

namespace rune::cli::engine {

using namespace rune::cli::support;

// Short aliases are private to this translation unit (issue #317: the header spells the namespaces out).
namespace cat = rune::catalog;
namespace ql  = rune::ql;
namespace sec = rune::security;
#ifdef RUNE_CLI_LIVE_ENABLED
namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
#endif

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

std::string joined(const std::vector<std::string>& parts) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) out += (i == 0 ? "" : ", ") + parts[i];
    return out;
}

std::string literalText(const ql::Literal& l) {
    switch (l.kind) {
        case ql::Literal::Kind::Integer: return (l.negative && l.magnitude != 0 ? "-" : "") + std::to_string(l.magnitude);
        case ql::Literal::Kind::Bool:    return l.boolean ? "true" : "false";
        case ql::Literal::Kind::String:  return l.lowered ? "lower('" + l.text + "')" : "'" + l.text + "'";
    }
    return {};
}

std::string keyItemText(const ql::KeyItem& k) {
    return k.fn == ql::KeyFunction::Lower ? "lower(" + k.column + ") as " + k.outName : k.column;
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
    // Issue #225: the auth names have one source, rune::client::authErrorName.
    if (const char* a = rune::client::authErrorName(code)) return a;
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
        "PasswordChangeRequired", "AuthUnavailable", "RecordEgressUnavailable", "UnstableOffsetCommit",
        "NamespaceSuspended", "NamespaceLimitExceeded", "Throttled", "ViewPaused",
        "ConstraintViolation", "KeyExists", "KeyNotFound", "AutoIncrementExhausted", "ViewKeyExpressionInvalid",
        "ViewConstraintInvalid", "ViewUniqueInvalid", "TxnTooLarge", "TxnConflict",
        "LeaseNotFound", "LeaseLimitExceeded", "LeaseTtlTooLarge", "SchemaVersionRetired"};
#ifdef RUNE_CLI_LIVE_ENABLED
    static_assert(std::size(kNames) == static_cast<std::size_t>(rune::protocol::client::ErrorCode::SchemaVersionRetired) + 1,
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
    PrepareScope scope;
    return prepare(st, opts, scope, why);
}

std::optional<Prepared> prepare(const ql::Statement& st, const PrepareOptions& opts, PrepareScope& scope, std::string& why) {
    auto checkVersion = [&](int64_t v, std::string_view kind, uint64_t max) {
        if (v < 0 || static_cast<uint64_t>(v) > max) {
            why = "version " + std::to_string(v) + " is out of range: versions of a " + std::string(kind) + " are 0.." +
                  std::to_string(max);
            return false;
        }
        return true;
    };
    auto forget = [&](const std::string& topic) {
        std::erase_if(scope.topics, [&](const ql::TopicDef& t) { return t.name == topic; });
    };
    try {
        return std::visit(
            [&](const auto& s) -> std::optional<Prepared> {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, ql::TopicDef>) {
                    PDefine p{ql::applyPlan(s)};
                    scope.topics.push_back(s);
                    return Prepared{std::move(p)};
                } else if constexpr (std::is_same_v<T, ql::AlterTopicDef>) {
                    forget(s.name);   // its fields may have changed: a view over it is authored when it runs
                    return Prepared{PAlter{ql::applyAlterPlan(s, opts.expectedVersion)}};
                } else if constexpr (std::is_same_v<T, ql::AlterTopicConfigDef>) {
                    auto plan = ql::applyConfigPlan(s);
                    if (!plan) {
                        why = plan.error().message;
                        return std::nullopt;
                    }
                    return Prepared{PAlterConfig{std::move(*plan)}};
                } else if constexpr (std::is_same_v<T, ql::DropTopicDef>) {
                    forget(s.name);
                    return Prepared{PDropTopic{s.name}};
                } else if constexpr (std::is_same_v<T, ql::DropViewDef>) {
                    return Prepared{PDropView{s.name}};
                } else if constexpr (std::is_same_v<T, ql::CreateViewDef>) {
                    PCreateView p{s, {}, {}};
                    const auto source = std::find_if(scope.topics.rbegin(), scope.topics.rend(), [&](const ql::TopicDef& t) { return t.name == s.source; });
                    if (source != scope.topics.rend()) {
                        const ql::ViewSource src = ql::viewSourceOf(*source);
                        auto plan = ql::applyViewPlan(s, src);
                        if (!plan) {
                            why = plan.error().message;
                            return std::nullopt;
                        }
                        for (const auto& f : plan->output) {
                            const auto* column = src.descriptor.find(f.srcField);
                            p.outputTypes.push_back(ql::typeWord(f.type, column != nullptr ? column->fixedLen : uint16_t{0}));
                        }
                        p.scriptPlan = std::move(*plan);
                    }
                    return Prepared{std::move(p)};
                } else if constexpr (std::is_same_v<T, ql::SelectDef>) {
                    return Prepared{PSelect{s}};
                } else if constexpr (std::is_same_v<T, ql::ShowTopicDef>) {
                    return Prepared{PShowTopic{s.name}};
                } else if constexpr (std::is_same_v<T, ql::ShowViewDef>) {
                    return Prepared{PShowView{s.name}};
                } else if constexpr (std::is_same_v<T, ql::ShowLeasesDef>) {
                    why = "'show leases' is not available yet";   // Task 22 prepares it; refused before anything is issued
                    return std::nullopt;
                } else if constexpr (std::is_same_v<T, ql::ShowLeaseDef>) {
                    why = "'show lease' is not available yet";
                    return std::nullopt;
                } else if constexpr (std::is_same_v<T, ql::RevokeLeaseDef>) {
                    why = "'revoke lease' is not available yet";
                    return std::nullopt;
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
                const auto& kf = d.keyFeatures;
                if (kf.autoIncrement) out << " autoincrement";
                if (kf.keyIndex) out << " key.index";
                if (kf.autoIncrementReserve != rune::protocol::kAutoIncrementReserveDefault) out << " autoincrement.reserve " << kf.autoIncrementReserve;
                if (kf.autoIncrementMax != rune::protocol::kAutoIncrementMaxDefault) out << " autoincrement.max " << kf.autoIncrementMax;
                if (d.lease) {
                    const auto& l = *d.lease;
                    out << " lease lease.ttl " << l.ttlMs << " lease.min_ttl " << l.minTtlMs << " lease.max_ttl " << l.maxTtlMs
                        << " lease.max_keys " << l.maxKeys << " lease.expiry_batch " << l.expiryBatch << " lease.checkpoint "
                        << (l.checkpoint != 0 ? "on" : "off") << " lease.checkpoint_interval " << l.checkpointIntervalMs
                        << " lease.checkpoint_rate " << l.checkpointRate;
                }
                if (d.descriptorBlob.empty()) out << " raw\n";
                else out << "\n" << descriptorText(d.descriptorBlob);
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
                                case rune::codec::AlterOpType::RetireBelow:
                                    out << "RETIRE VERSION < " << op.version;
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
            } else if constexpr (std::is_same_v<T, PCreateView>) {
                const auto& d = s.def;
                const bool usesLower = std::any_of(d.key.begin(), d.key.end(), [](const ql::KeyItem& k) { return k.fn == ql::KeyFunction::Lower; });
                std::vector<std::string> key;
                for (const auto& k : d.key) key.push_back(keyItemText(k));
                out << "create " << (d.orReplace ? "or replace " : "") << (d.unique ? "unique " : "") << (d.transient ? "transient " : "")
                    << "view " << d.name << " source " << d.source << " key " << (key.size() == 1 ? key[0] : "(" + joined(key) + ")");
                if (usesLower) {
                    const uint32_t v = rune::codec::kUnicodeVersion;
                    out << " unicode " << (v >> 16) << "." << ((v >> 8) & 0xFF) << "." << (v & 0xFF);
                }
                out << "\n";
                if (!s.scriptPlan) {
                    out << "  (source resolved when the statement runs)\n";
                } else {
                    const auto& plan = *s.scriptPlan;
                    std::vector<std::string> output;
                    for (std::size_t i = 0; i < plan.output.size(); ++i) {
                        const auto& f = plan.output[i];
                        static constexpr const char* kRules[] = {"replace", "count", "sum", "avg", "min", "max", "last", "count", "lastnonnull"};
                        const std::string rule = f.rule == rune::view::MergeRule::CountAll ? "count(*)" : std::string(kRules[static_cast<std::size_t>(f.rule)]) + "(" + f.srcField + ")";
                        output.push_back(f.name + " " + s.outputTypes[i] + " " + rule);
                    }
                    out << "  output " << joined(output) << "\n";
                    for (const auto& c : plan.constraints) out << "  constraint " << c.name << " check (" << ql::predicateText(c.predicate) << ")\n";
                    out << "  plan authored v" << (rune::view::needsV2(plan) ? 2 : 1) << "\n";
                }
            } else if constexpr (std::is_same_v<T, PSelect>) {
                const auto& d = s.def;
                std::vector<std::string> where;
                for (const auto& w : d.where) where.push_back(w.column + " = " + literalText(w.value));
                out << "select " << (d.columns.empty() ? "*" : joined(d.columns)) << " from " << d.view << " where ";
                for (std::size_t i = 0; i < where.size(); ++i) out << (i == 0 ? "" : " and ") << where[i];
                if (d.limit) out << " limit " << *d.limit;
                out << "\n";
            } else if constexpr (std::is_same_v<T, PShowTopic>) {
                out << "show topic " << s.name << "\n";
            } else if constexpr (std::is_same_v<T, PShowView>) {
                out << "show view " << s.name << "\n";
            } else if constexpr (std::is_same_v<T, PShowLeases>) {
                const auto& d = s.def;
                out << "show leases " << d.topic;
                if (d.prefix) out << " where " << d.keyColumn << " like '" << *d.prefix << "%'";
                out << " limit " << d.limit << "\n";
            } else if constexpr (std::is_same_v<T, PShowLease>) {
                out << "show lease " << s.def.topic << " key " << literalText(s.def.key) << "\n";
            } else if constexpr (std::is_same_v<T, PRevokeLease>) {
                out << "revoke lease " << s.def.topic << " key " << literalText(s.def.key) << "\n";
            } else if constexpr (std::is_same_v<T, PSecurity>) {
                out << s.plan.statement << "\n";   // the REDACTED statement (D10): a password literal never prints
            } else {
                out << s.text << "\n";
            }
        },
        p);
    if (rid) out << "  request-id " << requestIdHex(*rid) << "\n";
}

/// Reads the whole script text: a file, or @p stdinStream for "-".  The text may hold password literals (#305): it is
/// read through a fixed buffer and grown with appendWiped, so no copy of it is freed unwiped (no stringstream).
bool readScript(const std::string& path, std::istream& stdinStream, std::string& text, std::ostream& err) {
    char chunk[4096];
    struct ChunkWipe { char* b; ~ChunkWipe() { rune::client::detail::secureZero(b, sizeof chunk); } } chunkWipe{chunk};
    rune::client::detail::wipeString(text);
    if (path == "-") {
        // Read through the stream's buffer, so a redirected std::cin (or any other stream) is honoured.  #672: the
        // process stdin may be O_NONBLOCK.  std::cin is stdio-synced, so a read that hit EAGAIN shows as a short read
        // with ferror(stdin) set and errno EAGAIN: wait for input and go on - it is not end of input.  Any other stdin
        // error fails the command rather than running a prefix.
        const bool processStdin = &stdinStream == &std::cin;
        // PR #704 review: the script passes through stdio's stdin buffer (a pipe's later bursts are read into it): the
        // consumed buffer is wiped on every exit, so no password literal stays there for the rest of the process.
        struct StdioWipe {
            bool on;
            ~StdioWipe() {
#ifdef __GLIBC__
                // Everything but the unread window [read_ptr, read_end) is stale (consumed, or left by an earlier fill).
                FILE* f = stdin;
                if (!on || f->_IO_buf_base == nullptr) return;
                char* lo = f->_IO_buf_base;
                char* hi = f->_IO_buf_end;
                char* rp = f->_IO_read_ptr != nullptr ? f->_IO_read_ptr : lo;
                char* re = f->_IO_read_end != nullptr ? f->_IO_read_end : lo;
                if (rp < lo || rp > hi || re < rp || re > hi) { rp = lo; re = lo; }
                rune::client::detail::secureZero(lo, static_cast<std::size_t>(rp - lo));
                rune::client::detail::secureZero(re, static_cast<std::size_t>(hi - re));
#endif
            }
        } stdioWipe{processStdin};
        std::streambuf* sb = stdinStream.rdbuf();
        while (sb != nullptr) {
            // A stale error flag is cleared first, so ferror() below speaks for this read only.  EOF is never cleared:
            // a tty would need a second end-of-input.  A flag that survives (error AND end of file, left by earlier
            // use of the process stdin) is not this read's: only a flag this read raised counts (a redirected
            // std::cin never touches stdio at all).
            if (processStdin && std::ferror(stdin) && !std::feof(stdin)) std::clearerr(stdin);
            const bool staleError = processStdin && std::ferror(stdin);
            errno = 0;
            const std::streamsize n = sb->sgetn(chunk, sizeof chunk);
            const int readErrno = errno;
            if (n > 0) rune::client::detail::appendWiped(text, std::string_view(chunk, static_cast<std::size_t>(n)));
            if (n == static_cast<std::streamsize>(sizeof chunk)) continue;
            if (!processStdin || staleError || !std::ferror(stdin)) {
                if (n > 0) continue;   // a short read of a plain stream: the next one reports the end
                break;
            }
            if (readErrno == EINTR) continue;
            if (readErrno == EAGAIN || readErrno == EWOULDBLOCK) {
                pollfd p{STDIN_FILENO, POLLIN, 0};
                (void)::poll(&p, 1, -1);
                continue;
            }
            std::clearerr(stdin);
            rune::client::detail::wipeString(text);
            err << "rune: cannot read the script from stdin\n";
            return false;
        }
        return true;
    }
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        err << "rune: cannot open file: " << path << "\n";
        return false;
    }
    bool ok = true;
    for (;;) {
        const ssize_t n = ::read(fd, chunk, sizeof chunk);
        if (n == 0) break;
        if (n < 0) {
            if (errno == EINTR) continue;
            ok = false;
            break;
        }
        rune::client::detail::appendWiped(text, std::string_view(chunk, static_cast<std::size_t>(n)));
    }
    ::close(fd);
    if (!ok) {
        rune::client::detail::wipeString(text);
        err << "rune: cannot read file: " << path << "\n";
        return false;
    }
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
std::optional<cat::RequestRecord> awaitOwnWrite(rc::Client& c, const cat::RequestId& rid, std::string& why, uint16_t& brokerCode) {
    brokerCode = 0;
    auto& reader = c.catalogReader();
    const int64_t deadline = steadyNowMs() + std::chrono::duration_cast<std::chrono::milliseconds>(kCatalogWait).count();
    return awaitRequestVisible(
        [&]() -> std::optional<std::string> {
            const int64_t left = std::max<int64_t>(1, deadline - steadyNowMs());
            if (auto r = reader.refresh(std::chrono::milliseconds(left)); !r) {
                brokerCode = r.error().brokerErrorCode;   // Issue #309
                return catalogReadFailure(r.error());
            }
            return std::nullopt;
        },
        [&] { return reader.state().requestOfAnyPrincipal(rid); }, steadyNowMs,
        [] { std::this_thread::sleep_for(std::chrono::milliseconds(20)); }, deadline, why);
}


LiveOptions liveOptionsOf(const support::ParsedArgs& a, const std::string& endpoint, bool verbose) {
    LiveOptions o;
    o.endpoint     = endpoint;
    o.egress       = a.value("--cli-egress");
    o.gatewayResponsePort = a.value("--gateway-response-port");
    o.streamIn     = a.value("--cli-stream-in");
    o.streamOut    = a.value("--cli-stream-out");
    o.dataDir      = a.value("--cli-data-dir");
    o.aeronDir     = a.value("--cli-aeron-dir");
    o.verbose      = verbose;
    o.user         = a.value("--user");
    o.passwordFile = a.value("--password-file");
    o.tlsCa        = a.value("--tls-ca");
    o.tlsHost      = a.value("--tls-host");
    o.insecurePlaintext = a.has("--insecure-plaintext");
    return o;
}

bool LiveBroker::open(const LiveOptions& o, std::ostream& err, bool withCredentials, bool quietFailure) {
    close();   // #672: the previous client (and its scratch dir) goes before a new one is built - a ScopedDir is never replaced under a live client
    rc::ClientConfig cfg;
    cfg.gatewayChannel = toChannel(o.endpoint);
    cfg.clientName     = "rune-cli";
    cfg.runnerMode     = rc::RunnerMode::Invoker;
    cfg.nanoClock      = o.nanoClock;
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
        cfg.gatewayResponseControlPort = gatewayResponsePortOf(o.gatewayResponsePort);   // NAT-safe egress (D7)
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return false;
    }
    try {
        if (!o.streamIn.empty()) cfg.gatewayStreamId = std::stoi(o.streamIn);
        if (!o.streamOut.empty()) cfg.egressStreamId = std::stoi(o.streamOut);
    } catch (const std::exception&) {
        err << "rune: --cli-stream-in/--cli-stream-out must be integers\n";
        return false;
    }
    try {
        cfg.dataDir = resolveDataDir(o.dataDir, dir_);   // empty = a private scratch directory, removed on close (#315)
        cfg.aeronDir = o.aeronDir;
    } catch (const std::exception& e) {   // #680: a bad TMPDIR is a clean failure, not an exception past the secrets
        lastFailure_ = std::string("cannot create the client's data directory: ") + e.what();
        if (!quietFailure) err << "rune: " << lastFailure_ << "\n";
        return false;
    }
    // Authentication (Task 8): who to log in as.  --user / RUNE_USER and --password-file / RUNE_PASSWORD_FILE;
    // a password is NEVER taken from argv (parseArgs refuses --password) or from the environment.
    // Issue #121: the only connection the CLI opens without credentials is `rune security bootstrap`'s.
    cfg.securityBootstrapOnly = !withCredentials;
    // cfg.password (the prompted one) is wiped on every path out, a throw from the Client constructor included.
    struct CfgPasswordWipe { rc::ClientConfig& c; ~CfgPasswordWipe() { rc::detail::wipeString(c.password); } } cfgPasswordWipe{cfg};
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
        applyTls(cfg, o.tlsCa, o.tlsHost, o.insecurePlaintext);   // issue #13: after the credentials / bootstrap mode
    } catch (const std::exception& e) {
        lastFailure_ = e.what();
        if (!quietFailure) err << "rune: " << e.what() << "\n";   // a quiet caller reports lastFailure() itself
        close();
        return false;
    }
    try {
        client_ = std::make_unique<rc::Client>(cfg);
        rc::detail::wipeString(cfg.password);   // the Client holds its own copy now
        client_->connectAndWait(kConnectWait);
        printDevBannerIfAny(*client_, err);   // dev mode: say so, loudly
        return true;
    } catch (const std::exception& e) {
        lastFailure_ = e.what();
        if (const std::string_view what = e.what(); what.find("timed out") != std::string_view::npos) {
            // "connect to <ep> timed out" is what the e2e retry scripts grep; the response-port hint follows it.
            const auto hint = what.find(" (gateway response port");
            err << "rune: connect to " << o.endpoint << " timed out" << (hint == std::string_view::npos ? "" : what.substr(hint)) << "\n";
        }
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
    uint16_t code = 0;
    if (awaitOwnWrite(broker.client(), *lastDdl, why, code)) return true;
    if (code != 0) lastErrorCode = code;   // Issue #309: the exit status carries the broker's refusal
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
    topic.keyFeatures = d.keyFeatures;
    topic.lease = d.lease;
    const auto r = broker.client().defineTopicAndWait(
        d.name, d.partitions, d.keyField, std::span<const uint8_t>(d.descriptorBlob.data(), d.descriptorBlob.size()),
        d.persistence == ql::Persistence::Transient ? rc::Persistence::Transient : rc::Persistence::Disk, topic,
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
        if (reader.state().requestOfAnyPrincipal(*lastSecurityDdl)) return true;
        if (steadyNowMs() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fail("the security read did not show this script's last DDL (request id " + requestIdHex(*lastSecurityDdl) +
                "): " + why);
}

bool QlRun::run(const PSecurity& s, const rc::DdlOptions& ddl) {
    // The client wipes its copy (the by-value parameter); the Prepared's own is wiped here and by its destructor.
    const auto r = broker.client().securityDdlAndWait(s.plan, ddl, broker.ddlTimeout());
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
        const cat::ViewObject* v = r->viewByName(rune::catalog::kDefaultNsId, s.name);
        if (v == nullptr) return fail("no view '" + s.name + "' at that point of the catalog");
        out << formatViewDefinition(*v);
        return true;
    }
    const cat::TopicObject* t = r->topicByName(rune::catalog::kDefaultNsId, s.name);
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

// ---- select: planning and printing ----------------------------------------------------------------------------------

std::expected<SelectRead, std::string> planSelect(const ql::SelectDef& def, const rune::view::ViewPlan& authored,
                                                  const rune::codec::Descriptor& source, const rune::codec::Descriptor& output,
                                                  int32_t sourcePartitions, rune::protocol::Partitioner partitioner) {
    using rune::codec::PrimType;
    auto builder = rc::view::ViewKeyBuilder::fromPlan(authored, source, sourcePartitions, partitioner);
    if (!builder) return std::unexpected(std::move(builder.error()));
    const auto key = builder->components();
    const std::size_t n = key.size();

    std::vector<const ql::WhereTerm*> named(n, nullptr);
    bool foreign = false;
    for (const auto& w : def.where) {
        const auto c = std::find_if(key.begin(), key.end(), [&](const auto& k) { return k.outName == w.column; });
        if (c == key.end()) {
            foreign = true;
            continue;
        }
        named[static_cast<std::size_t>(c - key.begin())] = &w;   // the parser refuses a column named twice
    }
    std::size_t leading = 0, count = 0;
    while (leading < n && named[leading] != nullptr) ++leading;
    for (const auto* t : named) count += t != nullptr;
    auto columns = [&](std::size_t k) {
        std::vector<std::string> names;
        for (std::size_t i = 0; i < k; ++i) names.push_back(key[i].outName);
        return joined(names);
    };

    SelectRead read;
    if (!foreign && count == n) {
        read.kind = SelectRead::Kind::Point;
    } else if (!foreign && leading == count && n >= 2 && count == n - 1) {
        read.kind = SelectRead::Kind::Scan;
    } else if (!foreign && leading == count && count >= 1 && count < n - 1) {
        return std::unexpected("where names only the leading (" + columns(count) + "); a read by a shorter prefix needs an ordered index");
    } else if (n == 1) {
        return std::unexpected("where must name the key column (" + columns(1) + ")");
    } else {
        return std::unexpected("where must name every key column (" + columns(n) + ") for a point read, or the leading (" + columns(n - 1) + ") for a scan");
    }
    if (def.limit && read.kind == SelectRead::Kind::Point) return std::unexpected("limit is only valid on a scan");
    read.limit = def.limit.value_or(0);   // the parser refuses `limit 0`, so 0 only ever means no limit

    for (std::size_t i = 0; i < n; ++i) {
        if (named[i] == nullptr) continue;
        const auto& c   = key[i];
        const auto& lit = named[i]->value;
        const std::string doesNotFit = literalText(lit) + " does not fit " + ql::typeWord(c.type, c.srcWidth) + " column '" + c.outName + "'";
        switch (c.type) {
            case PrimType::Int32:
            case PrimType::Int64: {
                const uint64_t limit = c.type == PrimType::Int32 ? uint64_t{1} << 31 : uint64_t{1} << 63;
                if (lit.kind != ql::Literal::Kind::Integer || lit.magnitude > (lit.negative ? limit : limit - 1)) return std::unexpected(doesNotFit);
                builder->setInt(c.outName, static_cast<int64_t>(lit.negative ? 0 - lit.magnitude : lit.magnitude));
                break;
            }
            case PrimType::UInt16:
            case PrimType::UInt32:
            case PrimType::UInt64:
            case PrimType::Enum: {
                const uint64_t max = c.type == PrimType::UInt16 ? 0xFFFF : c.type == PrimType::UInt64 ? ~uint64_t{0} : 0xFFFF'FFFF;
                if (lit.kind != ql::Literal::Kind::Integer || (lit.negative && lit.magnitude != 0) || lit.magnitude > max) return std::unexpected(doesNotFit);
                builder->setUInt(c.outName, lit.magnitude);
                break;
            }
            case PrimType::Bool:
                if (lit.kind != ql::Literal::Kind::Bool) return std::unexpected(doesNotFit);
                builder->setBool(c.outName, lit.boolean);
                break;
            case PrimType::FixedStr: {
                if (lit.kind != ql::Literal::Kind::String) return std::unexpected(doesNotFit);
                std::string text = lit.text;
                if (lit.lowered && c.keyFunction == rune::codec::kKeyFunctionNone) {   // a lower() key component lowers it in the builder
                    std::vector<uint8_t> lowered(rune::codec::maxLoweredWidth(static_cast<uint32_t>(text.size())));
                    const auto r = rune::codec::lowerUtf8(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(text.data()), text.size()), lowered);
                    if (!r.ok) return std::unexpected(literalText(lit) + " is not valid UTF-8");
                    text.assign(reinterpret_cast<const char*>(lowered.data()), r.len);
                }
                if (text.size() > c.srcWidth)
                    return std::unexpected(literalText(lit) + " is " + std::to_string(text.size()) + " bytes; column '" + c.outName + "' is " + ql::typeWord(c.type, c.srcWidth));
                builder->setString(c.outName, text);
                break;
            }
            default:
                return std::unexpected(doesNotFit);
        }
    }

    auto bytes = read.kind == SelectRead::Kind::Point ? builder->key() : builder->prefix();
    if (!bytes) return std::unexpected(std::move(bytes.error()));
    read.keyOrPrefix = std::move(*bytes);
    if (read.kind == SelectRead::Kind::Point) read.partition = builder->partition();

    if (def.columns.empty()) {
        for (std::size_t i = 0; i < output.fields.size(); ++i) read.slots.push_back(static_cast<uint16_t>(i));
    } else {
        for (const auto& name : def.columns) {
            const auto f = std::find_if(output.fields.begin(), output.fields.end(), [&](const auto& d) { return d.name == name; });
            if (f == output.fields.end()) return std::unexpected("column '" + name + "' is not a column of view '" + authored.viewName + "'");
            read.slots.push_back(static_cast<uint16_t>(f - output.fields.begin()));
        }
    }
    return read;
}

bool appendScanPage(std::vector<std::vector<uint8_t>>& rows, std::vector<std::vector<uint8_t>>&& page, uint32_t limit) {
    for (auto& row : page) {
        if (limit != 0 && rows.size() >= limit) break;
        rows.push_back(std::move(row));
    }
    return limit != 0 && rows.size() >= limit;
}

std::string formatRows(const rune::codec::Descriptor& output, std::span<const uint16_t> slots, std::span<const std::vector<uint8_t>> rows) {
    using rune::codec::PrimType;
    std::vector<PrimType> types;
    std::vector<uint16_t> widths;
    for (const auto& f : output.fields) {
        types.push_back(f.type);
        widths.push_back(f.fixedLen);
    }
    const auto layout = rc::view::buildRowLayout(types, widths);

    std::vector<std::vector<std::string>> table(1);
    for (const uint16_t slot : slots) table[0].push_back(output.fields[slot].name);
    for (const auto& row : rows) {
        auto& line = table.emplace_back();
        for (const uint16_t slot : slots) {
            const auto v = rc::view::readSlot(row, layout, slot);
            if (!v.valid) throw std::runtime_error("a row does not fit the view's output layout");
            if (!v.present) { line.push_back("NULL"); continue; }
            switch (v.type) {
                case PrimType::Int32:
                case PrimType::Int64:    line.push_back(std::to_string(v.i)); break;
                case PrimType::Float64: {
                    char buf[64];
                    line.emplace_back(buf, std::to_chars(buf, buf + sizeof buf, v.f).ptr);
                    break;
                }
                case PrimType::Bool:     line.push_back(v.b ? "true" : "false"); break;
                case PrimType::FixedStr: line.emplace_back(v.bytes.substr(0, v.bytes.find('\0'))); break;
                default:                 line.push_back(std::to_string(v.u)); break;
            }
        }
    }
    std::vector<std::size_t> width(slots.size(), 0);
    for (const auto& line : table)
        for (std::size_t c = 0; c < line.size(); ++c) width[c] = std::max(width[c], line[c].size());
    std::string out;
    for (const auto& line : table) {
        for (std::size_t c = 0; c < line.size(); ++c) {
            out += line[c];
            if (c + 1 != line.size()) out += std::string(width[c] - line[c].size() + 2, ' ');
        }
        out += "\n";
    }
    return out + "(" + std::to_string(rows.size()) + " rows)\n";
}

// ---- stats -------------------------------------------------------------------------------------------------------

namespace {

std::string statsRefusal(int32_t partition, uint16_t status) {
    if (status == rc::StatsResult::kBrokerTooOld) return std::string(kStatsBrokerTooOld);
    std::string why;
    switch (status) {
        case rc::StatsResult::kUnknownObject:  why = "unknown object"; break;
        case rc::StatsResult::kInvalidRequest: why = "invalid request"; break;
        case rc::StatsResult::kBusy:           why = "the partition is busy, try again"; break;
        case rc::StatsResult::kNotAuthorized:  why = "not authorized"; break;
        case rc::StatsResult::kNotLeader:      why = "the partition's leader was not reached"; break;
        default:                               why = "status " + std::to_string(status); break;
    }
    return "stats of partition " + std::to_string(partition) + ": " + why;
}

std::string viewReadRefusal(uint16_t status) {
    switch (status) {
        case rc::ViewRowResult::kNotReady:       return "the view is not ready yet";
        case rc::ViewRowResult::kSchemaMismatch: return "the view's output changed; run it again";
        case rc::ViewRowResult::kUnknownView:    return "unknown view";
        case rc::ViewRowResult::kInvalidRequest: return "invalid request";
        case rc::ViewRowResult::kBusy:           return "the partition is busy, try again";
        case rc::ViewRowResult::kRowTooLarge:    return "the row is too large to send";
        case rc::ViewRowResult::kNotAuthorized:  return "not authorized";
        case rc::ViewRowResult::kNotLeader:      return "the partition's leader was not reached";
        case rc::ViewRowResult::kViewPaused:     return "the view is paused over its namespace's memory limit";
        case rc::ViewRowResult::kThrottled:      return "throttled";
        default:                                 return "view read status " + std::to_string(status);
    }
}

std::expected<void, std::string> partitionRange(int32_t partitions, int32_t only, int32_t& from, int32_t& to) {
    from = 0;
    to   = partitions;
    if (only < 0) return {};
    if (only >= partitions) return std::unexpected("--partition " + std::to_string(only) + " is out of range: 0.." + std::to_string(partitions - 1));
    from = only;
    to   = only + 1;
    return {};
}

}  // namespace

std::string constraintLabel(const std::optional<rc::ConstraintName>& name, uint32_t constraintId) {
    if (name) return name->view + "." + name->constraint;
    char hex[16];
    std::snprintf(hex, sizeof hex, "0x%08x", constraintId);
    return hex;
}

namespace {

// A miss refreshes the catalog and may free the objects the stats were read for, so this runs after the last use of them.
void labelConstraints(rc::Client& c, std::vector<PartitionStats>& stats) {
    for (PartitionStats& ps : stats)
        for (const auto& k : ps.result.constraints) ps.constraintLabels.push_back(constraintLabel(c.constraintName(k.constraintId), k.constraintId));
}

}  // namespace

std::expected<std::vector<PartitionStats>, std::string> topicStats(rc::Client& c, const cat::TopicObject& t, int32_t only) {
    int32_t from = 0, to = 0;
    if (auto r = partitionRange(t.partitions, only, from, to); !r) return std::unexpected(std::move(r.error()));
    std::vector<PartitionStats> out;
    for (int32_t p = from; p < to; ++p) {
        auto r = c.getStatsAndWait(rpc::StatsScope::TopicPartition, t.topicId, p, kCatalogWait);
        if (r.status != rc::StatsResult::kOk) return std::unexpected(statsRefusal(p, r.status));
        PartitionStats ps{p, std::move(r), {}};
        out.push_back(std::move(ps));
    }
    labelConstraints(c, out);
    return out;
}

std::expected<std::vector<PartitionStats>, std::string> viewStats(rc::Client& c, const cat::CatalogState& state, const cat::ViewObject& v, int32_t only) {
    const auto plan = rune::view::deserializeAuthored(v.def.normalizedPlanBlob);
    if (!plan) return std::unexpected("the plan of view '" + v.name + "' does not decode");
    const cat::TopicObject* source = v.def.sourceNames.empty() ? nullptr : state.topicByName(v.nsId, v.def.sourceNames[0]);
    if (source == nullptr) return std::unexpected("the source of view '" + v.name + "' is not in the catalog");
    int32_t from = 0, to = 0;
    if (auto r = partitionRange(v.def.sinkPartitions, only, from, to); !r) return std::unexpected(std::move(r.error()));
    std::vector<PartitionStats> out;
    bool sourceReadable = true;
    for (int32_t p = from; p < to; ++p) {
        auto r = c.getStatsAndWait(rpc::StatsScope::ViewPartition, v.def.viewTopicId, p, kCatalogWait);
        if (r.status != rc::StatsResult::kOk) return std::unexpected(statsRefusal(p, r.status));
        PartitionStats ps{p, std::move(r), {}};
        if (plan->unique || !plan->constraints.empty()) {   // the rejections are counted where the verdicts are: the source partition
            rc::StatsResult src;
            src.status = rc::StatsResult::kNotAuthorized;
            if (sourceReadable) src = c.getStatsAndWait(rpc::StatsScope::TopicPartition, source->topicId, p, kCatalogWait);
            if (src.status == rc::StatsResult::kNotAuthorized) {   // the view's counters are readable without the source's
                sourceReadable = false;
                ps.constraintsNote = "not authorized on source '" + source->name + "'";
            } else if (src.status != rc::StatsResult::kOk) {
                return std::unexpected(statsRefusal(p, src.status));
            }
            for (const auto& k : src.constraints)
                if ((k.constraintId >> 16) == (v.objectId & 0xFFFF)) ps.result.constraints.push_back(k);
        }
        out.push_back(std::move(ps));
    }
    labelConstraints(c, out);
    return out;
}

std::expected<std::string, std::string> showStatsText(const std::expected<std::vector<PartitionStats>, std::string>& stats) {
    if (stats) return formatStatsText(*stats);
    if (stats.error() == kStatsBrokerTooOld) return "  (" + std::string(kStatsBrokerTooOld) + ")\n";
    return std::unexpected(stats.error());
}

// ---- views, select, show ------------------------------------------------------------------------------------------

bool QlRun::refreshCatalog() {
    if (!awaitLastDdl()) return false;
    if (auto r = broker.client().catalogReader().refresh(kCatalogWait); !r) return failRead(catalogReadFailure(r.error()), r.error());
    return true;
}

bool QlRun::run(const PCreateView& s, const rc::DdlOptions& ddl) {
    if (!refreshCatalog()) return false;
    const cat::CatalogState& state = broker.client().catalogReader().state();
    const cat::TopicObject* source = state.topicByName(cat::kDefaultNsId, s.def.source);
    if (source == nullptr) return fail("no topic '" + s.def.source + "'");
    if (!source->schema) return fail("view source must be a typed topic");
    ql::ViewSource src;
    src.name       = source->name;
    src.topicId    = source->topicId;
    src.keyField   = source->schema->keyField;
    src.descriptor = rune::codec::deserialize(source->schema->evolvingBlob);
    src.partitions = source->partitions;
    src.transient  = source->persistence == cat::Persistence::Transient;
    auto plan = ql::applyViewPlan(s.def, src);
    if (!plan) return fail(plan.error().message);

    const std::vector<uint8_t> blob = rune::view::serializeAuthored(*plan);
    const std::string keyField = rc::view::viewKeyField(*plan, plan->key);
    const cat::ViewObject* existing = state.viewByName(cat::kDefaultNsId, s.def.name);
    if (existing != nullptr && s.def.orReplace) {
        const auto r = broker.client().alterViewAndWait(s.def.name, keyField, plan->persistence, existing->version, blob, ddl, broker.ddlTimeout());
        broker.flushVerbose(err);
        if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
        lastDdl = r.requestId;
        if (style == OutputStyle::Console) out << "ok view " << s.def.name << "\n";
        else out << "altered view '" << s.def.name << "' version=" << r.newVersion << "\n";
        return true;
    }
    const auto r = broker.client().createViewAndWait(s.def.name, keyField, plan->persistence, blob, ddl, broker.ddlTimeout());
    broker.flushVerbose(err);
    if (r.errorCode != 0) return failBroker(r.errorCode, r.detail);
    lastDdl = r.requestId;
    if (style == OutputStyle::Console) out << "ok view " << s.def.name << "\n";
    else out << "created view '" << s.def.name << "' viewTopicId=" << r.viewTopicId << "\n";
    return true;
}

bool QlRun::run(const PSelect& s, const rc::DdlOptions&) {
    if (!refreshCatalog()) return false;
    const cat::CatalogState& state = broker.client().catalogReader().state();
    const cat::ViewObject* view = state.viewByName(cat::kDefaultNsId, s.def.view);
    if (view == nullptr) return fail("no view '" + s.def.view + "'");
    const auto plan = rune::view::deserializeAuthored(view->def.normalizedPlanBlob);
    if (!plan) return fail("the plan of view '" + view->name + "' does not decode");
    const cat::TopicObject* source = view->def.sourceNames.empty() ? nullptr : state.topicByName(view->nsId, view->def.sourceNames[0]);
    if (source == nullptr || !source->schema) return fail("the source of view '" + view->name + "' is not a typed topic in the catalog");
    const rune::codec::Descriptor output = rune::codec::deserialize(view->def.outputDescriptorBlob);
    const auto read = planSelect(s.def, *plan, rune::codec::deserialize(source->schema->evolvingBlob), output, source->partitions,
                                 source->schema->keyFeatures.partitioner);
    if (!read) return fail(read.error());

    std::vector<std::vector<uint8_t>> rows;
    if (read->kind == SelectRead::Kind::Point) {
        const auto r = broker.client().getViewRowAndWait(view->def.viewTopicId, read->keyOrPrefix, rc::ViewReadOptions{.partition = read->partition}, kCatalogWait);
        if (r.status == rc::ViewRowResult::kAbsentKey) {
            out << "(no row)\n";
            return true;
        }
        if (r.status != rc::ViewRowResult::kOk) return fail(viewReadRefusal(r.status));
        rows.push_back(r.rowBytes);
    } else {
        rc::ViewScanOptions page;
        for (;;) {
            const uint32_t want = read->limit == 0 ? kScanPageRows : std::min<uint32_t>(read->limit - static_cast<uint32_t>(rows.size()), kScanPageRows);
            auto r = broker.client().scanViewAndWait(view->def.viewTopicId, read->keyOrPrefix, static_cast<uint16_t>(want), page, kCatalogWait);
            if (r.status != rc::ViewRowResult::kOk) return fail(r.status == rc::ViewRowResult::kCursorInvalidated
                                                                    ? "the scan was invalidated by a concurrent change; run it again"
                                                                    : viewReadRefusal(r.status));
            const bool atLimit = appendScanPage(rows, std::move(r.rows), read->limit);
            if (r.complete() || atLimit) break;
            page.resumeToken = std::move(r.nextToken);
        }
    }
    out << formatRows(output, read->slots, rows);
    return true;
}

bool QlRun::run(const PShowTopic& s, const rc::DdlOptions&) {
    if (!refreshCatalog()) return false;
    const cat::TopicObject* t = broker.client().catalogReader().state().topicByName(cat::kDefaultNsId, s.name);
    if (t == nullptr) return fail("no topic '" + s.name + "'");
    out << formatTopicDefinition(*t);
    const auto stats = showStatsText(topicStats(broker.client(), *t, -1));
    if (!stats) return fail(stats.error());
    out << *stats;
    return true;
}

// Task 22 replaces these: the lease statements are refused at prepare, so a script never reaches run() with one.
bool QlRun::run(const PShowLeases&, const rc::DdlOptions&) { throw std::logic_error("unreachable: refused at prepare"); }
bool QlRun::run(const PShowLease&, const rc::DdlOptions&) { throw std::logic_error("unreachable: refused at prepare"); }
bool QlRun::run(const PRevokeLease&, const rc::DdlOptions&) { throw std::logic_error("unreachable: refused at prepare"); }

bool QlRun::run(const PShowView& s, const rc::DdlOptions&) {
    if (!refreshCatalog()) return false;
    const cat::CatalogState& state = broker.client().catalogReader().state();
    const cat::ViewObject* v = state.viewByName(cat::kDefaultNsId, s.name);
    if (v == nullptr) return fail("no view '" + s.name + "'");
    out << formatViewDefinition(*v);
    const auto stats = showStatsText(viewStats(broker.client(), state, *v, -1));
    if (!stats) return fail(stats.error());
    out << *stats;
    return true;
}

#endif   // RUNE_CLI_LIVE_ENABLED

} // namespace rune::cli::engine
