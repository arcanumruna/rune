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
#include "QlEngine.h"

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
using namespace rune::cli::engine;

// ═══════════════════════════════════════════════════════════════════════════
// Pure helpers
// ═══════════════════════════════════════════════════════════════════════════

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
      << "  kind " << (t.schema ? "typed" : "raw") << "\n"
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
          "          [--expected-version N]\n"
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



rc::Persistence clientPersistence(cat::Persistence p) noexcept {
    return p == cat::Persistence::Transient ? rc::Persistence::Transient : rc::Persistence::Disk;
}

} // namespace
#endif   // RUNE_CLI_LIVE_ENABLED

// ═══════════════════════════════════════════════════════════════════════════
// rune ql
// ═══════════════════════════════════════════════════════════════════════════

int cmdQl(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("ql", args, {"-e", "-f", "--request-id", "--broker", "--expected-version"}, {"--dry-run", "--verbose"}, a, err)) {
        printCatalogUsage(err);
        return 2;
    }
    if (a.has("-e") == a.has("-f")) return usageError("ql", err, "exactly one of -e <statement> and -f <file|-> is required");
    const bool dryRun = a.has("--dry-run");

    PrepareOptions popts;
    if (a.has("--expected-version")) {
        const std::string v = a.value("--expected-version");
        uint32_t n = 0;
        const auto res = std::from_chars(v.data(), v.data() + v.size(), n);
        if (v.empty() || res.ec != std::errc{} || res.ptr != v.data() + v.size() || n > 65535)
            return usageError("ql", err, "--expected-version must be a non-negative integer (0..65535)");
        popts.expectedVersion = static_cast<uint16_t>(n);
    }

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
        if (!readScript(a.value("-f"), in, text, err)) return 1;
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
        auto p = prepare(statements[i], popts, why);
        if (!p) {
            if (script) err << "statement " << (i + 1) << ": " << why << "\n";
            else err << "rune: " << why << "\n";
            return 1;
        }
        prepared.push_back(std::move(*p));
    }

    // Review B I7: a password literal on the command line (-e) is visible to `ps` and the shell history — the same
    // reason `--password` is refused.  A statement that carries one must come from a file or stdin.
    if (!script) {
        for (const Prepared& p : prepared) {
            if (const auto* sp = std::get_if<PSecurity>(&p); sp != nullptr && !sp->plan.password.empty()) {
                return usageError("ql", err,
                                  "a password literal on the command line is visible to ps and shell history; put the "
                                  "statement in a file (-f <file>) or pipe it (-f -)");
            }
        }
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
    if (!broker.open(liveOptionsOf(a, endpoint, a.has("--verbose")), err)) return 1;
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
    if (!broker.open(liveOptionsOf(a, endpoint, false), err)) return false;
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
        const cat::TopicObject* t = target.topicByName(rune::catalog::kDefaultNsId, name);
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
    if (e.nsId != 0 && static_cast<uint16_t>(e.kind) < cat::kReservedKindFirst)   // the client DDL addresses `default` only
        return "an event of namespace " + std::to_string(e.nsId) + " cannot be replayed yet";
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
                const cat::TopicObject* top = st.topicByName(e.nsId, e.objectName);
                if (top == nullptr) return "topic '" + e.objectName + "' does not exist on the target";
                const auto r = c.defineTopicAndWait(
                    e.objectName, static_cast<uint32_t>(top->partitions), o.schema.keyField,
                    std::span<const uint8_t>(o.schema.descriptorBlob.data(), o.schema.descriptorBlob.size()),
                    o.schema.validate, clientPersistence(top->persistence), {}, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else if constexpr (std::is_same_v<T, cat::AlterSchemaOps>) {
                const cat::TopicObject* top = st.topicByName(e.nsId, e.objectName);
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
            } else if constexpr (std::is_same_v<T, cat::DropViewOps>) {
                const auto r = c.dropViewAndWait(e.objectName, ddl, t);
                return failureOf(r.errorCode, r.detail);
            } else {   // Namespace events are not replayed: namespace DDL is cluster-admin only, issued with `rune namespace`
                static_assert(std::is_same_v<T, cat::NamespaceCreatedOps> || std::is_same_v<T, cat::NamespaceLimitsSetOps> ||
                              std::is_same_v<T, cat::NamespaceStateChangedOps> || std::is_same_v<T, cat::NamespaceDeletedOps> ||
                              std::is_same_v<T, cat::NamespaceDeleteStepOps>);
                return std::string("a namespace event (") + std::string(cat::kindName(e.kind)) + ") cannot be replayed yet";
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
    if (!target.open(liveOptionsOf(a, into, false), err)) return 1;
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
                uint16_t brokerCode = 0;   // Issue #309: replay keeps returning 1
                done = awaitOwnWrite(client, rid, why, brokerCode);
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
