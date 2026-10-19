// src/cli/CatalogCommands.cpp
//
// Catalog history Task 15 — `rune ql` (any statement) and `rune catalog export | replay | diff`.
//
// Everything that talks to a broker goes through the real rune::client::Client
// (RunnerMode::Invoker: the CLI thread is the duty thread, no other thread exists) and is
// bounded by the client's own DDL deadline (ClientConfig::ddlTimeoutMs) or the catalog read
// timeout below — a missing broker, a lost leader or a silent cluster is an explicit error,
// never a hang.  Nothing is ever written to `out` for a failed export.
#include "rune/cli/CatalogCommands.h"
#include "rune/cli/SecurityCommands.h"
#include "CliSupport.h"

#include <rune/client/detail/SecureZero.h>
#include "rune/ql/Backends.h"
#include "rune/ql/Parser.h"

#include <rune/catalog/CatalogFile.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <charconv>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <type_traits>
#include <variant>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/client/view/ViewPlanAuthor.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>
#endif

namespace rune::cli {

namespace cat = rune::catalog;
namespace ql  = rune::ql;
namespace sec = rune::security;
using namespace rune::cli::support;

// ═══════════════════════════════════════════════════════════════════════════
// Pure helpers
// ═══════════════════════════════════════════════════════════════════════════
namespace {

std::string hex16(uint64_t v) {
    char b[17];
    std::snprintf(b, sizeof b, "%016llx", static_cast<unsigned long long>(v));
    return b;
}

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

std::string orDash(std::string s) { return s.empty() ? std::string("-") : s; }

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

constexpr std::size_t kHistoryColumns = 8;
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

} // namespace

std::string formatHistoryRow(const cat::RecordedEvent& r) {
    const auto cols = historyColumns(r);
    std::string s;
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i != 0) s += "  ";
        s += cols[i];
    }
    return s;
}

namespace {
std::string replayHead(const cat::RecordedEvent& source) {
    return "replay aborted at source offset " + std::to_string(source.offset) + " (" +
           std::string(cat::kindName(source.event.kind)) + " '" + source.event.objectName + "'): ";
}
} // namespace

std::optional<std::string> verifyReplayStep(const cat::RecordedEvent& source, const cat::RecordedEvent& target) {
    const std::string head = replayHead(source);
    if (!target.event.replayedFrom)
        return head + "target event at offset " + std::to_string(target.offset) + " carries no replayedFrom";
    if (target.event.replayedFrom->offset != source.offset)
        return head + "target event at offset " + std::to_string(target.offset) + " replays source offset " +
               std::to_string(target.event.replayedFrom->offset);
    if (target.event.kind != source.event.kind)
        return head + "target event is a " + std::string(cat::kindName(target.event.kind));
    if (target.event.fingerprint != source.event.fingerprint)
        return head + "target fingerprint 0x" + hex16(target.event.fingerprint) + " != source 0x" +
               hex16(source.event.fingerprint);
    return std::nullopt;
}

std::optional<std::string> verifyReplayStep(const cat::RecordedEvent& source, const cat::RecordedEvent& target,
                                            std::string_view clusterId) {
    if (auto m = verifyReplayStep(source, target)) return m;
    if (target.event.replayedFrom->clusterId != clusterId)
        return replayHead(source) + "target event at offset " + std::to_string(target.offset) + " replays cluster '" +
               target.event.replayedFrom->clusterId + "', not '" + std::string(clusterId) + "'";
    return std::nullopt;
}

cat::RequestId replayRequestId(std::string_view sourceClusterId, int64_t sourceOffset) noexcept {
    uint64_t h = 0xcbf29ce484222325ull;   // FNV-1a 64
    for (const char c : sourceClusterId) {
        h ^= static_cast<uint8_t>(c);
        h *= 0x100000001b3ull;
    }
    return cat::RequestId{h, static_cast<uint64_t>(sourceOffset)};
}

std::optional<cat::RequestId> parseRequestId(std::string_view hex32) noexcept {
    if (hex32.size() != 32) return std::nullopt;
    uint64_t words[2] = {0, 0};
    for (std::size_t w = 0; w < 2; ++w) {
        for (std::size_t i = 0; i < 16; ++i) {
            const char c = hex32[w * 16 + i];
            uint64_t d = 0;
            if (c >= '0' && c <= '9') d = static_cast<uint64_t>(c - '0');
            else if (c >= 'a' && c <= 'f') d = static_cast<uint64_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') d = static_cast<uint64_t>(c - 'A' + 10);
            else return std::nullopt;
            words[w] = (words[w] << 4) | d;
        }
    }
    return cat::RequestId{words[0], words[1]};
}

std::string formatSchemaDefinition(const cat::TopicObject& t) {
    std::ostringstream o;
    if (!t.schema) {
        o << "schema " << t.name << ": (untyped topic: no schema)\n";
        return o.str();
    }
    const cat::SchemaState& s = *t.schema;
    o << "schema " << t.name << " version " << s.schemaVersion << "\n"
      << "  compatibility " << compatName(s.compat) << "\n"
      << "  keyField " << (s.keyField.empty() ? "-" : s.keyField) << "\n"
      << "  validate " << (s.validate ? "yes" : "no") << "\n"
      << descriptorText(s.evolvingBlob);
    return o.str();
}

std::string formatTopicDefinition(const cat::TopicObject& t) {
    const cat::TopicConfig& c = t.config;
    std::ostringstream o;
    o << "topic " << t.name << " (objectId " << t.objectId << ", version " << t.version << ")\n"
      << "  partitions " << t.partitions << "\n"
      << "  replicationFactor " << t.replicationFactor << (t.replicationFactor == 0 ? " (every broker)" : "") << "\n"
      << "  persistence " << persistenceName(t.persistence) << "\n"
      << "  config.cleanupPolicy " << int{c.cleanupPolicy} << "\n"
      << "  config.retentionMs " << c.retentionMs << "\n"
      << "  config.retentionBytes " << c.retentionBytes << "\n"
      << "  config.deleteRetentionMs " << c.deleteRetentionMs << "\n"
      << "  config.minCleanableDirtyRatioPpm " << c.minCleanableDirtyRatioPpm << "\n"
      << "  config.segmentRollBytes " << c.segmentRollBytes << "\n"
      << "  config.minCompactionLagMs " << c.minCompactionLagMs << "\n"
      << "  config.maxCompactionLagMs " << c.maxCompactionLagMs << "\n"
      << "  config.retentionCheckMs " << c.retentionCheckMs << "\n"
      << "  config.fileDeleteDelayMs " << c.fileDeleteDelayMs << "\n"
      << "  config.durability " << int{c.durability} << "\n"
      << "  config.durabilityTier " << int{c.durabilityTier} << "\n"
      << "  config.coPartitionGroup " << orDash(c.coPartitionGroup) << "\n"
      << "  config.partitionKey " << orDash(c.partitionKey) << "\n";
    if (t.schema) o << formatSchemaDefinition(t);
    else o << "  (untyped: no schema)\n";
    return o.str();
}

std::string formatViewDefinition(const cat::ViewObject& v) {
    std::ostringstream o;
    o << "view " << v.name << " (objectId " << v.objectId << ", version " << v.version << ")\n"
      << "  sources";
    for (const auto& s : v.def.sourceNames) o << " " << s;
    o << "\n  key " << orDash(v.def.keyField) << "\n"
      << "  sinkPartitions " << v.def.sinkPartitions << "\n"
      << "  persistence " << persistenceName(v.def.persistence) << "\n"
      << "  output\n"
      << descriptorText(v.def.outputDescriptorBlob);
    return o.str();
}

std::string formatDiffEntry(const cat::DiffEntry& d) {
    const std::string what = std::string(cat::objectTypeName(d.type)) + " " + d.name;
    switch (d.change) {
        case cat::DiffEntry::Change::Added:   return "+ " + what;
        case cat::DiffEntry::Change::Removed: return "- " + what;
        case cat::DiffEntry::Change::Changed: break;
    }
    std::string s = "~ " + what + ":";
    for (std::size_t i = 0; i < d.fields.size(); ++i) s += (i == 0 ? " " : ", ") + d.fields[i];
    return s;
}

void printCatalogUsage(std::ostream& err) {
    err << "  rune ql (-e \"<statement>\" | -f <file|->) [--dry-run] [--verbose] [--request-id <32 hex digits>]\n"
        << "          --broker host:port\n"
        << "      Execute DDL (the statement text is recorded in the catalog) and reads (SHOW HISTORY,\n"
        << "      SHOW ... AS OF, DIFF). --dry-run parses and prints the plan without a broker.\n"
        << "      -f runs EVERY statement in order and stops at the first error (\"statement N: ...\").\n"
        << "      --request-id fixes the DDL's id (with -f, statement N uses lo + N - 1): re-running a\n"
        << "      DDL that already applied returns its original result.\n"
        << "      --verbose prints every followed Redirect and NotLeader retry to stderr.\n"
        << "  rune catalog export [--up-to <offset>] --broker host:port > file.rcat\n"
        << "  rune catalog replay --from <file.rcat|host:port> --into host:port [--up-to <offset>]\n"
        << "          [--cluster-id <id>]    (--cluster-id defaults to the --from argument verbatim)\n"
        << "  rune catalog diff --from <file.rcat|host:port> --to <file.rcat|host:port>\n"
        << "      Exit 0: no differences; 3: differences (printed); 1: error; 2: usage.\n";
}

// (argument parsing and the live broker connection moved to CliSupport.h: shared with SecurityCommands.cpp)
#ifdef RUNE_CLI_LIVE_ENABLED
namespace {

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;


std::string eventLogReadFailure(std::string_view what, const rc::CatalogReadError& e) {
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

rc::Persistence clientPersistence(cat::Persistence p) noexcept {
    return p == cat::Persistence::Transient ? rc::Persistence::Transient : rc::Persistence::Disk;
}

} // namespace
#endif   // RUNE_CLI_LIVE_ENABLED

// ═══════════════════════════════════════════════════════════════════════════
// rune ql
// ═══════════════════════════════════════════════════════════════════════════
namespace {

constexpr uint64_t kMaxVersion = 65535;   // schema versions are u16: never truncated
constexpr uint64_t kMaxObjectVersion = 4294967295ull;   // topic / view object versions are u32

/// A statement ready to run: every refusal that needs no broker happened while preparing it.
struct PDefine      { ql::DefineTopicPlan plan; };
struct PAlter       { ql::AlterTopicPlan plan; };
struct PAlterConfig { ql::AlterTopicConfigPlan plan; };
struct PDropTopic   { std::string name; };
struct PDropView    { std::string name; };
struct PHistory     { cat::HistoryFilter filter; std::string text; };
/// Authentication (Task 8): a security DDL — the plan holds the plaintext password, wiped when the statement is done.
struct PSecurity {
    mutable ql::SecurityPlan plan;
    PSecurity() = default;
    explicit PSecurity(ql::SecurityPlan p) : plan(std::move(p)) {}
    PSecurity(PSecurity&&) noexcept = default;
    PSecurity& operator=(PSecurity&&) noexcept = default;
    PSecurity(const PSecurity&) = delete;
    PSecurity& operator=(const PSecurity&) = delete;
    ~PSecurity() { wipe(); }
    void wipe() const noexcept {
        if (!plan.password.empty()) rune::client::detail::secureZero(plan.password.data(), plan.password.size());
    }
};
/// SHOW USERS / ROLES / GRANTS / SECURITY HISTORY — client-side replays of `__security`.
struct PSecShow {
    enum class Kind { Users, Roles, Grants, History } kind = Kind::Users;
    sec::GranteeKind who = sec::GranteeKind::User;       // Grants
    std::string name;                                    // Grants / History filter
    std::optional<sec::AsOf> at;                         // Grants
    sec::HistoryFilter filter;                           // History
    std::string text;
};
struct PAsOf        { cat::ObjectType kind = cat::ObjectType::Topic; std::string name; cat::AsOf at; std::string text; };
struct PDiff        { cat::AsOf a, b; std::string onlyTopic; bool restrictToTopic = false; std::string text; };
using Prepared = std::variant<PDefine, PAlter, PAlterConfig, PDropTopic, PDropView, PHistory, PAsOf, PDiff, PSecurity, PSecShow>;

cat::ObjectType objectType(ql::ObjectKind k) noexcept {
    switch (k) {
        case ql::ObjectKind::Topic:  return cat::ObjectType::Topic;
        case ql::ObjectKind::View:   return cat::ObjectType::View;
        case ql::ObjectKind::Schema: return cat::ObjectType::Schema;
    }
    return cat::ObjectType::Topic;
}

/// Statement `n` of a script (1-based) sees this id: {hi, lo + n - 1}.
struct IdSeq {
    std::optional<cat::RequestId> base;
    [[nodiscard]] cat::RequestId at(std::size_t n) const { return {base->hi, base->lo + (n - 1)}; }
};

std::string requestIdHex(const cat::RequestId& id) { return hex16(id.hi) + hex16(id.lo); }

/// Parse-and-plan one statement.  nullopt + `why` when it is refused (versions beyond u16, a bad
/// setting, an undecodable time).
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

std::optional<Prepared> prepare(const ql::Statement& st, std::string& why) {
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
                    return Prepared{PDefine{ql::applyPlan(s)}};
                } else if constexpr (std::is_same_v<T, ql::AlterTopicDef>) {
                    return Prepared{PAlter{ql::applyAlterPlan(s, 0)}};
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

/// Reads the whole script text: a file, or stdin for "-".
bool readScript(const std::string& path, std::string& text, std::ostream& err) {
    if (path == "-") {
        std::ostringstream buf;
        buf << std::cin.rdbuf();
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
struct QlRun {
    LiveBroker& broker;
    std::ostream& out;
    std::ostream& err;
    bool script = false;   // -f: errors read "statement N: …"
    std::size_t index = 1;
    std::optional<cat::RequestId> lastDdl;   // the last DDL this run issued: reads after it must show it

    /// A read that follows a DDL waits until the reader (the seed broker's own HW) shows that DDL.
    bool awaitLastDdl() {
        if (!lastDdl) return true;
        std::string why;
        if (awaitOwnWrite(broker.client(), *lastDdl, why)) return true;
        return fail("the catalog read did not show this script's last DDL (request id " + requestIdHex(*lastDdl) + "): " + why);
    }

    bool fail(const std::string& msg) {
        broker.flushVerbose(err);
        if (script) err << "statement " << index << ": " << msg << "\n";
        else err << "rune: " << msg << "\n";
        return false;
    }

    template <class F>
    bool guarded(F&& f) {
        try {
            return f();
        } catch (const std::exception& e) {
            return fail(std::string(e.what()));
        }
    }

    bool exec(const Prepared& p, const std::string& text, const std::optional<cat::RequestId>& rid) {
        rc::DdlOptions ddl;
        ddl.statement = text;
        if (rid) ddl.requestId = *rid;
        return guarded([&] { return std::visit([&](const auto& s) { return run(s, ddl); }, p); });
    }

    bool run(const PDefine& s, const rc::DdlOptions& ddl) {
        const auto& d = s.plan;
        rc::TopicOptions topic;
        topic.replicationFactor = d.replicas;
        topic.config = d.config;   // F32: the whole struct, never field by field
        const auto r = broker.client().defineTopicAndWait(
            d.name, d.partitions, d.keyField, std::span<const uint8_t>(d.descriptorBlob.data(), d.descriptorBlob.size()),
            false, d.persistence == ql::Persistence::Transient ? rc::Persistence::Transient : rc::Persistence::Disk, topic,
            ddl, broker.ddlTimeout());
        broker.flushVerbose(err);
        if (r.errorCode != 0) return fail(brokerFailure(r.errorCode, r.detail));
        lastDdl = r.requestId;
        out << "created topic '" << d.name << "' topicId=" << r.topicId << " version=" << r.version << "\n";
        return true;
    }
    bool run(const PAlter& s, const rc::DdlOptions& ddl) {
        const auto& d = s.plan;
        const auto r = broker.client().alterTopicAndWait(
            d.name, d.expectedVersion, std::span<const uint8_t>(d.opsBlob.data(), d.opsBlob.size()), ddl, broker.ddlTimeout());
        broker.flushVerbose(err);
        if (r.errorCode != 0) return fail(brokerFailure(r.errorCode, r.detail));
        lastDdl = r.requestId;
        out << "altered topic '" << d.name << "' version=" << r.newVersion << "\n";
        return true;
    }
    bool run(const PAlterConfig& s, const rc::DdlOptions& ddl) {
        std::vector<rpc::ConfigChangeWire> changes;
        for (const auto& [key, value] : s.plan.changes) changes.push_back(rpc::ConfigChangeWire{key, value});
        const auto r = broker.client().alterTopicConfigAndWait(s.plan.name, 0, changes, ddl, broker.ddlTimeout());
        broker.flushVerbose(err);
        if (r.errorCode != 0) return fail(brokerFailure(r.errorCode, r.detail));
        lastDdl = r.requestId;
        out << "altered config of topic '" << s.plan.name << "' version=" << r.newVersion << "\n";
        return true;
    }
    bool run(const PDropTopic& s, const rc::DdlOptions& ddl) {
        const auto r = broker.client().dropTopicAndWait(s.name, ddl, broker.ddlTimeout());
        broker.flushVerbose(err);
        if (r.errorCode != 0) return fail(brokerFailure(r.errorCode, r.detail));
        lastDdl = r.requestId;
        out << "dropped topic '" << s.name << "'\n";
        return true;
    }
    // ---- authentication (Task 8): security DDL and SHOW … ---------------------------------------------------------
    std::optional<cat::RequestId> lastSecurityDdl;   // a SHOW after a security DDL waits until the reader shows it

    bool awaitLastSecurityDdl() {
        if (!lastSecurityDdl) return true;
        auto& reader = broker.client().securityReader();
        const int64_t deadline = steadyNowMs() + std::chrono::duration_cast<std::chrono::milliseconds>(kCatalogWait).count();
        std::string why = "not visible before the deadline";
        for (;;) {
            const int64_t left = std::max<int64_t>(1, deadline - steadyNowMs());
            if (auto r = reader.refresh(std::chrono::milliseconds(left)); !r) { why = securityReadFailure(r.error()); break; }
            if (reader.state().request(*lastSecurityDdl)) return true;
            if (steadyNowMs() >= deadline) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return fail("the security read did not show this script's last DDL (request id " + requestIdHex(*lastSecurityDdl) +
                    "): " + why);
    }

    bool run(const PSecurity& s, const rc::DdlOptions& ddl) {
        ql::SecurityPlan plan = s.plan;   // the client wipes this copy; the Prepared's own is wiped by its destructor
        const auto r = broker.client().securityDdlAndWait(std::move(plan), ddl, broker.ddlTimeout());
        s.wipe();
        broker.flushVerbose(err);
        if (r.errorCode != 0) return fail(brokerFailure(r.errorCode, r.detail));
        lastSecurityDdl = r.requestId;
        out << "ok: " << s.plan.statement << " (object " << r.objectId << ", version " << r.newVersion << ")\n";
        return true;
    }

    bool run(const PSecShow& s, const rc::DdlOptions&) {
        if (!awaitLastSecurityDdl()) return false;
        auto& c = broker.client();
        switch (s.kind) {
            case PSecShow::Kind::Users: {
                const auto r = c.securityUsers(kCatalogWait);
                if (!r) return fail(securityReadFailure(r.error()));
                out << formatUsersTable(*r);
                return true;
            }
            case PSecShow::Kind::Roles: {
                const auto r = c.securityRoles(kCatalogWait);
                if (!r) return fail(securityReadFailure(r.error()));
                out << formatRolesTable(*r);
                return true;
            }
            case PSecShow::Kind::Grants: {
                const auto r = c.securityGrantsFor(s.who, s.name, s.at, kCatalogWait);
                if (!r) return fail(securityReadFailure(r.error()));
                out << formatGrantsTable(*r);
                return true;
            }
            case PSecShow::Kind::History: {
                const auto r = c.securityHistory(s.filter, kCatalogWait);
                if (!r) return fail(securityReadFailure(r.error()));
                out << formatSecurityHistoryTable(*r);
                return true;
            }
        }
        return false;
    }

    bool run(const PDropView& s, const rc::DdlOptions& ddl) {
        const auto r = broker.client().dropViewAndWait(s.name, ddl, broker.ddlTimeout());
        broker.flushVerbose(err);
        if (r.errorCode != 0) return fail(brokerFailure(r.errorCode, r.detail));
        lastDdl = r.requestId;
        out << "dropped view '" << s.name << "'\n";
        return true;
    }

    bool run(const PHistory& s, const rc::DdlOptions&) {
        if (!awaitLastDdl()) return false;
        const auto r = broker.client().catalogHistory(s.filter, kCatalogWait);
        if (!r) return fail(catalogReadFailure(r.error()));
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

    bool run(const PAsOf& s, const rc::DdlOptions&) {
        if (!awaitLastDdl()) return false;
        const auto r = broker.client().catalogAsOf(s.at, kCatalogWait);
        if (!r) return fail(catalogReadFailure(r.error()));
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

    bool run(const PDiff& s, const rc::DdlOptions&) {
        if (!awaitLastDdl()) return false;
        const auto r = broker.client().catalogDiff(s.a, s.b, kCatalogWait);
        if (!r) return fail(catalogReadFailure(r.error()));
        std::size_t shown = 0;
        for (const cat::DiffEntry& d : *r) {
            if (s.restrictToTopic && !(d.type == cat::ObjectType::Topic && d.name == s.onlyTopic)) continue;
            out << formatDiffEntry(d) << "\n";
            ++shown;
        }
        if (shown == 0) out << "(no differences)\n";
        return true;
    }
};
#endif   // RUNE_CLI_LIVE_ENABLED

} // namespace

int cmdQl(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("ql", args, {"-e", "-f", "--request-id", "--broker"}, {"--dry-run", "--verbose"}, a, err)) {
        printCatalogUsage(err);
        return 2;
    }
    if (a.has("-e") == a.has("-f")) return usageError("ql", err, "exactly one of -e <statement> and -f <file|-> is required");
    const bool dryRun = a.has("--dry-run");

    IdSeq ids;
    if (a.has("--request-id")) {
        ids.base = parseRequestId(a.value("--request-id"));
        if (!ids.base) return usageError("ql", err, "--request-id must be exactly 32 hex digits");
        if (ids.base->none())
            return usageError("ql", err, "--request-id 00000000000000000000000000000000 means 'none': give a non-zero id");
    }

    // ── parse everything up front: nothing is issued unless every statement parses and plans ──
    const bool script = a.has("-f");
    std::string text;
    if (script) {
        if (!readScript(a.value("-f"), text, err)) return 1;
    } else {
        text = a.value("-e");
    }
    std::vector<ql::Statement> statements;
    std::vector<std::string> texts;
    if (script) {
        auto parsed = ql::parseScript(text);
        if (!parsed) {
            const auto& e = parsed.error();
            err << "statement " << e.statementIndex << ": " << e.error.line << ":" << e.error.col << ": " << e.error.message << "\n";
            return 1;
        }
        statements = std::move(*parsed);
        texts = splitStatements(text);
        if (texts.size() != statements.size()) {
            err << "rune: internal error: split " << texts.size() << " statements, parsed " << statements.size() << "\n";
            return 1;
        }
    } else {
        auto parsed = ql::parse(text);
        if (!parsed) {
            if (const auto many = ql::parseScript(text); many && many->size() > 1)
                return usageError("ql", err,
                                  "-e takes exactly one statement (got " + std::to_string(many->size()) + "); use -f for a script");
            const auto& e = parsed.error();
            err << "rune: " << e.line << ":" << e.col << ": " << e.message << "\n";
            return 1;
        }
        statements.push_back(std::move(*parsed));
        texts.push_back(text);
    }

    if (ids.base && statements.size() > 1 &&
        ids.base->lo > std::numeric_limits<uint64_t>::max() - (statements.size() - 1)) {
        err << "rune: --request-id: the low 64 bits would overflow at statement "
            << (std::numeric_limits<uint64_t>::max() - ids.base->lo) + 2 << " of " << statements.size()
            << " (statement N uses lo + N - 1); choose a smaller id\n";
        return 1;
    }

    std::vector<Prepared> prepared;
    for (std::size_t i = 0; i < statements.size(); ++i) {
        std::string why;
        auto p = prepare(statements[i], why);
        if (!p) {
            if (script) err << "statement " << (i + 1) << ": " << why << "\n";
            else err << "rune: " << why << "\n";
            return 1;
        }
        prepared.push_back(std::move(*p));
    }

    auto idOf = [&](std::size_t i) -> std::optional<cat::RequestId> {
        return ids.base ? std::optional<cat::RequestId>(ids.at(i + 1)) : std::nullopt;
    };

    if (prepared.empty()) return 0;   // an empty script: nothing to issue, no broker contact

    if (dryRun) {
        for (std::size_t i = 0; i < prepared.size(); ++i) printPlan(prepared[i], idOf(i), out);
        return 0;
    }

#ifdef RUNE_CLI_LIVE_ENABLED
    std::string endpoint = a.value("--broker");
    if (endpoint.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env != nullptr) endpoint = env;
    }
    if (endpoint.empty()) return usageError("ql", err, "--broker host:port is required (or --dry-run)");

    LiveBroker broker;
    if (!broker.open(a, endpoint, a.has("--verbose"), err)) return 1;
    QlRun run{broker, out, err, script, 1};
    for (std::size_t i = 0; i < prepared.size(); ++i) {
        run.index = i + 1;
        if (!run.exec(prepared[i], texts[i], idOf(i))) return 1;   // nothing after the failing statement is issued
    }
    broker.flushVerbose(err);
    return 0;
#else
    err << "rune ql: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build. Use --dry-run.\n";
    return 1;
#endif
}

// ═══════════════════════════════════════════════════════════════════════════
// rune catalog
// ═══════════════════════════════════════════════════════════════════════════
namespace {

bool endsWith(std::string_view s, std::string_view suffix) { return s.size() >= suffix.size() && s.substr(s.size() - suffix.size()) == suffix; }

/// A `.rcat` path (an existing regular file, or one named *.rcat) versus a `host:port` cluster.
bool isRcatFile(const std::string& arg) {
    std::error_code ec;
    return endsWith(arg, ".rcat") || std::filesystem::is_regular_file(arg, ec);
}

/// Replays `events` (offset order) through the verifying CatalogReplayer, up to and including `upTo`
/// (nullopt = all).  `events` is truncated to what was verified.  Returns an error text naming the
/// offending offset, nullopt when every event verified.
std::optional<std::string> verifyEvents(std::vector<cat::RecordedEvent>& events, std::optional<int64_t> upTo,
                                        cat::CatalogState* finalState = nullptr) {
    if (upTo) {
        const auto end = std::find_if(events.begin(), events.end(), [&](const cat::RecordedEvent& e) { return e.offset > *upTo; });
        events.erase(end, events.end());
    }
    cat::CatalogReplayer replayer;
    for (const cat::RecordedEvent& e : events) {
        if (auto r = replayer.feedDecoded(e.offset, e.event); !r) return cat::describe(r.error());
    }
    if (finalState != nullptr) *finalState = replayer.state();
    return std::nullopt;
}

/// Reads a `.rcat` file: every record decoded and verified (a flipped bit anywhere is an error naming its offset).
bool readRcatFile(const std::string& path, std::vector<cat::RecordedEvent>& events, std::ostream& err) {
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        err << "rune: cannot open file: " << path << "\n";
        return false;
    }
    auto records = cat::readRcat(f);
    if (!records) {
        err << "rune: " << path << ": " << records.error() << "\n";
        return false;
    }
    cat::CatalogReplayer replayer;   // decode + the gap / apply / fingerprint checks, naming the offset
    for (const auto& [offset, bytes] : *records) {
        if (auto r = replayer.feed(offset, std::span<const uint8_t>(bytes.data(), bytes.size())); !r) {
            err << "rune: " << path << ": " << cat::describe(r.error()) << "\n";
            return false;
        }
        auto ev = cat::decode(std::span<const uint8_t>(bytes.data(), bytes.size()));
        if (!ev) {   // cannot happen after a successful feed; never skipped silently
            err << "rune: " << path << ": offset " << offset << ": " << cat::decodeErrorName(ev.error()) << "\n";
            return false;
        }
        events.push_back(cat::RecordedEvent{offset, std::move(*ev)});
    }
    return true;
}

#ifdef RUNE_CLI_LIVE_ENABLED
/// Reads a cluster's whole committed catalog (verified by the client's reader).
bool readCluster(const ParsedArgs& a, const std::string& endpoint, std::vector<cat::RecordedEvent>& events, std::ostream& err) {
    LiveBroker broker;
    if (!broker.open(a, endpoint, false, err)) return false;
    try {
        auto& reader = broker.client().catalogReader();
        if (auto r = reader.refresh(kCatalogWait); !r) {
            err << "rune: " << endpoint << ": " << catalogReadFailure(r.error()) << "\n";
            return false;
        }
        const auto span = reader.events();
        events.assign(span.begin(), span.end());
        return true;
    } catch (const std::exception& e) {
        err << "rune: " << endpoint << ": " << e.what() << "\n";
        return false;
    }
}
#endif

/// A catalog source: a `.rcat` file or a `host:port` cluster → its verified events.
bool loadSource(const ParsedArgs& a, const std::string& arg, std::vector<cat::RecordedEvent>& events, std::ostream& err) {
    if (isRcatFile(arg)) return readRcatFile(arg, events, err);
#ifdef RUNE_CLI_LIVE_ENABLED
    return readCluster(a, arg, events, err);
#else
    (void)a;
    err << "rune: reading cluster '" << arg << "' requires RUNE_CLI_LIVE_ENABLED and a real Aeron build\n";
    return false;
#endif
}

// ── export ──────────────────────────────────────────────────────────────────

int cmdExport(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("catalog export", args, {"--up-to", "--broker"}, {}, a, err)) {
        printCatalogUsage(err);
        return 2;
    }
    std::optional<int64_t> upTo;
    if (a.has("--up-to")) {
        upTo = parseOffset(a.value("--up-to"));
        if (!upTo) return usageError("catalog export", err, "--up-to must be a non-negative integer offset");
    }
#ifdef RUNE_CLI_LIVE_ENABLED
    std::string endpoint = a.value("--broker");
    if (endpoint.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env != nullptr) endpoint = env;
    }
    if (endpoint.empty()) return usageError("catalog export", err, "--broker host:port is required");

    std::vector<cat::RecordedEvent> events;
    if (!readCluster(a, endpoint, events, err)) return 1;
    if (auto bad = verifyEvents(events, upTo)) {
        err << "rune: catalog of " << endpoint << " does not verify: " << *bad << "\n";
        return 1;
    }
    // Build the whole file first: a failure leaves `out` untouched (no partial file claimed as complete).
    std::ostringstream file(std::ios::binary);
    cat::writeRcatHeader(file);
    for (const cat::RecordedEvent& e : events) {
        auto bytes = cat::encode(e.event);
        if (!bytes) {
            err << "rune: offset " << e.offset << ": cannot encode: " << cat::encodeErrorName(bytes.error()) << "\n";
            return 1;
        }
        cat::writeRcatRecord(file, e.offset, std::span<const uint8_t>(bytes->data(), bytes->size()));
    }
    if (!file) {
        err << "rune: cannot build the .rcat file\n";
        return 1;
    }
    out << file.str();
    out.flush();
    if (!out) {
        err << "rune: cannot write the .rcat file to the output\n";
        return 1;
    }
    return 0;
#else
    err << "rune catalog export: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

// ── diff ────────────────────────────────────────────────────────────────────

int cmdDiff(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("catalog diff", args, {"--from", "--to"}, {}, a, err)) {
        printCatalogUsage(err);
        return 2;
    }
    if (!a.has("--from") || !a.has("--to")) return usageError("catalog diff", err, "--from and --to are required");
    cat::CatalogState states[2];
    const std::string srcs[2] = {a.value("--from"), a.value("--to")};
    for (int i = 0; i < 2; ++i) {
        std::vector<cat::RecordedEvent> events;
        if (!loadSource(a, srcs[i], events, err)) return 1;
        if (auto bad = verifyEvents(events, std::nullopt, &states[i])) {
            err << "rune: " << srcs[i] << " does not verify: " << *bad << "\n";
            return 1;
        }
    }
    const auto diffs = cat::diff(states[0], states[1]);
    for (const cat::DiffEntry& d : diffs) out << formatDiffEntry(d) << "\n";
    return diffs.empty() ? 0 : 3;
}

// ── replay ──────────────────────────────────────────────────────────────────
#ifdef RUNE_CLI_LIVE_ENABLED

std::optional<std::string> failureOf(uint16_t code, const std::string& detail) {
    if (code == 0) return std::nullopt;
    return brokerFailure(code, detail);
}

/// The viewPlan blob of a CreateView / AlterView event with its sources remapped onto the TARGET's topic ids.
std::expected<std::vector<uint8_t>, std::string> remappedViewPlan(const cat::ViewDef& def, const cat::CatalogState& target) {
    auto plan = rc::view::deserializeAuthored(
        std::span<const uint8_t>(def.normalizedPlanBlob.data(), def.normalizedPlanBlob.size()));
    if (!plan) return std::unexpected("the event's normalized view plan does not decode");
    std::vector<int32_t> ids;
    for (const std::string& name : def.sourceNames) {
        const cat::TopicObject* t = target.topicByName(name);
        if (t == nullptr) return std::unexpected("source topic '" + name + "' does not exist on the target");
        ids.push_back(t->topicId);
    }
    if (!rc::view::remapSources(*plan, ids)) return std::unexpected("the view's source count does not match its plan");
    return rc::view::serializeAuthored(*plan);
}

/// Issues `src`'s DDL on the target (state `st` = the target's applied catalog).  nullopt = the broker
/// accepted it; otherwise why not.
std::optional<std::string> issueEvent(rc::Client& c, const cat::CatalogState& st, const cat::RecordedEvent& src,
                                      const rc::DdlOptions& ddl, std::chrono::milliseconds t) {
    const cat::CatalogEvent& e = src.event;
    return std::visit(
        [&](const auto& o) -> std::optional<std::string> {
            using T = std::decay_t<decltype(o)>;
            if constexpr (std::is_same_v<T, cat::CreateTopicOps>) {
                if (o.partitions < 1) return "partitions " + std::to_string(o.partitions) + " < 1";
                rc::TopicOptions topic;
                topic.replicationFactor = o.replicationFactor;
                topic.config = o.config;   // F32: assigned whole
                static const std::vector<uint8_t> kNoBlob;
                const std::vector<uint8_t>& blob = o.schema ? o.schema->descriptorBlob : kNoBlob;
                const auto r = c.defineTopicAndWait(e.objectName, static_cast<uint32_t>(o.partitions),
                                                    o.schema ? o.schema->keyField : std::string(),
                                                    std::span<const uint8_t>(blob.data(), blob.size()),
                                                    o.schema ? o.schema->validate : false, clientPersistence(o.persistence),
                                                    topic, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::CreateSchemaOps>) {
                const cat::TopicObject* top = st.topicByName(e.objectName);
                if (top == nullptr) return "topic '" + e.objectName + "' does not exist on the target";
                const auto r = c.defineTopicAndWait(
                    e.objectName, static_cast<uint32_t>(top->partitions), o.schema.keyField,
                    std::span<const uint8_t>(o.schema.descriptorBlob.data(), o.schema.descriptorBlob.size()),
                    o.schema.validate, clientPersistence(top->persistence), {}, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::AlterSchemaOps>) {
                const cat::TopicObject* top = st.topicByName(e.objectName);
                if (top == nullptr || !top->schema) return "topic '" + e.objectName + "' has no schema on the target";
                // F20: the event stores durable tags; the target holds the same definition, so they map to its names.
                auto ops = cat::unresolveAlterOps(*top->schema, std::span<const cat::ResolvedAlterOp>(o.ops));
                if (!ops) return "cannot map the event's field tags onto the target schema: " + ops.error();
                const auto blob = rune::codec::encodeAlterOps(*ops);
                const auto r = c.alterTopicAndWait(e.objectName, 0, blob, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::SetCompatibilityOps>) {
                rune::codec::AlterOp op{};
                op.type = rune::codec::AlterOpType::SetCompatibility;
                op.mode = o.mode;
                const auto blob = rune::codec::encodeAlterOps({op});
                const auto r = c.alterTopicAndWait(e.objectName, 0, blob, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::AlterTopicConfigOps>) {
                std::vector<rpc::ConfigChangeWire> changes;
                for (const cat::ConfigChange& ch : o.changes)
                    changes.push_back(rpc::ConfigChangeWire{static_cast<uint8_t>(ch.key), ch.value});
                const auto r = c.alterTopicConfigAndWait(e.objectName, 0, changes, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::CreateViewOps>) {
                auto blob = remappedViewPlan(o.view, st);
                if (!blob) return blob.error();
                const auto r = c.createViewAndWait(e.objectName, o.view.keyField,
                                                   o.view.persistence == cat::Persistence::Transient ? 1 : 0,
                                                   std::span<const uint8_t>(blob->data(), blob->size()), ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::AlterViewOps>) {
                auto blob = remappedViewPlan(o.view, st);
                if (!blob) return blob.error();
                const auto r = c.alterViewAndWait(e.objectName, o.view.keyField,
                                                  o.view.persistence == cat::Persistence::Transient ? 1 : 0, 0,
                                                  std::span<const uint8_t>(blob->data(), blob->size()), ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::DropTopicOps>) {
                const auto r = c.dropTopicAndWait(e.objectName, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else {
                static_assert(std::is_same_v<T, cat::DropViewOps>);
                const auto r = c.dropViewAndWait(e.objectName, ddl, t);
                return failureOf(r.errorCode, r.detail);
            }
        },
        e.ops);
}

int cmdReplay(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("catalog replay", args, {"--from", "--into", "--up-to", "--cluster-id"}, {}, a, err)) {
        printCatalogUsage(err);
        return 2;
    }
    if (!a.has("--from") || !a.has("--into")) return usageError("catalog replay", err, "--from and --into are required");
    std::optional<int64_t> upTo;
    if (a.has("--up-to")) {
        upTo = parseOffset(a.value("--up-to"));
        if (!upTo) return usageError("catalog replay", err, "--up-to must be a non-negative integer offset");
    }
    const std::string from = a.value("--from"), into = a.value("--into");
    const std::string clusterId = a.has("--cluster-id") ? a.value("--cluster-id") : from;
    if (clusterId.empty()) return usageError("catalog replay", err, "--cluster-id must not be empty");

    // 1. Read and VERIFY the source before anything is issued.
    std::vector<cat::RecordedEvent> source;
    if (!loadSource(a, from, source, err)) {
        err << "rune: the source could not be read; nothing was issued\n";
        return 1;
    }
    cat::CatalogState sourceFinal;
    if (auto bad = verifyEvents(source, upTo, &sourceFinal)) {
        err << "rune: source " << from << " does not verify; nothing was issued: " << *bad << "\n";
        return 1;
    }
    if (source.empty()) {
        out << "nothing to replay\n";
        return 0;
    }

    // 2. Replay into the target, one event at a time, verifying each step.
    LiveBroker target;
    if (!target.open(a, into, false, err)) return 1;
    rc::Client& client = target.client();
    const std::chrono::milliseconds ddlTimeout = target.ddlTimeout();

    std::size_t reissued = 0, deduplicated = 0;
    for (const cat::RecordedEvent& src : source) {
        const std::string head = "replay aborted at source offset " + std::to_string(src.offset) + " (" +
                                 std::string(cat::kindName(src.event.kind)) + " '" + src.event.objectName + "'): ";
        const cat::RequestId rid = replayRequestId(clusterId, src.offset);
        try {
            auto& reader = client.catalogReader();
            if (auto r = reader.refresh(kCatalogWait); !r) {
                err << "rune: " << head << "reading the target catalog: " << catalogReadFailure(r.error()) << "\n";
                return 1;
            }
            // A rerun after a crash: this step already applied (same request id) — its original result stands.
            auto done = reader.state().request(rid);
            if (done) {
                ++deduplicated;
            } else {
                rc::DdlOptions ddl;
                ddl.statement = src.event.statement;
                ddl.requestId = rid;
                ddl.replayedFrom = rc::DdlOptions::ReplayedFrom{
                    clusterId, src.offset, src.event.principal.clientName + "@" + src.event.principal.sourceAddress};
                if (auto why = issueEvent(client, reader.state(), src, ddl, ddlTimeout)) {
                    err << "rune: " << head << *why << "\n";
                    return 1;
                }
                ++reissued;
                std::string why;
                done = awaitOwnWrite(client, rid, why);
                if (!done) {
                    err << "rune: " << head << "the target's catalog (read from " << into << ") did not show request id "
                        << requestIdHex(rid) << " within " << kCatalogWait.count() << " s: " << why << "\n";
                    return 1;
                }
            }
            const auto events = reader.events();
            const cat::RecordedEvent* tgt = nullptr;
            if (done->offset >= 0 && static_cast<std::size_t>(done->offset) < events.size() &&
                events[static_cast<std::size_t>(done->offset)].offset == done->offset)
                tgt = &events[static_cast<std::size_t>(done->offset)];
            if (tgt == nullptr) {
                err << "rune: " << head << "the target's event at offset " << done->offset << " is not in its catalog\n";
                return 1;
            }
            if (auto bad = verifyReplayStep(src, *tgt, clusterId)) {
                err << "rune: " << *bad << "\n";
                return 1;
            }
        } catch (const std::exception& ex) {
            err << "rune: " << head << ex.what() << "\n";
            return 1;
        }
    }
    try {
        auto& reader = client.catalogReader();
        if (auto r = reader.refresh(kCatalogWait); !r) {
            err << "rune: replay into " << into << ": reading the target catalog: " << catalogReadFailure(r.error()) << "\n";
            return 1;
        }
        std::vector<std::string> wrong;
        for (const cat::DiffEntry& d : cat::diff(sourceFinal, reader.state()))
            if (d.change != cat::DiffEntry::Change::Added) wrong.push_back(formatDiffEntry(d));   // Added = only on the target: allowed
        if (!wrong.empty()) {
            err << "rune: replay into " << into << ": every step verified, but the target now differs from the source at offset "
                << source.back().offset << " (the target changed these objects after they were replayed):\n";
            for (const std::string& w : wrong) err << "  " << w << "\n";
            return 1;
        }
    } catch (const std::exception& ex) {
        err << "rune: replay into " << into << ": " << ex.what() << "\n";
        return 1;
    }
    out << "replayed " << source.size() << " events (source offsets 0.." << source.back().offset << ") into " << into << ": "
        << reissued << " issued, " << deduplicated << " already applied\n";
    return 0;
}
#endif   // RUNE_CLI_LIVE_ENABLED

} // namespace

int cmdCatalog(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    if (args.empty()) {
        err << "rune catalog: subcommand required (export|replay|diff)\n";
        printCatalogUsage(err);
        return 2;
    }
    const std::string& sub = args[0];
    const std::vector<std::string> rest(args.begin() + 1, args.end());
    if (sub == "export") return cmdExport(rest, out, err);
    if (sub == "diff") return cmdDiff(rest, out, err);
    if (sub == "replay") {
#ifdef RUNE_CLI_LIVE_ENABLED
        return cmdReplay(rest, out, err);
#else
        err << "rune catalog replay: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
        return 1;
#endif
    }
    err << "rune catalog: unknown subcommand '" << sub << "'\n";
    printCatalogUsage(err);
    return 2;
}

} // namespace rune::cli
