// src/cli/NamespaceCommands.cpp — see rune/cli/NamespaceCommands.h.
#include "rune/cli/NamespaceCommands.h"

#include "CliSupport.h"
#include "QlEngine.h"

#include <array>
#include <charconv>
#include <cstdlib>
#include <iostream>
#include <sstream>

#include <unistd.h>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#endif

namespace rune::cli {

using namespace rune::cli::support;
namespace rcl = rune::client;
namespace rp  = rune::protocol;
namespace rpc = rune::protocol::client;

namespace {

std::optional<uint64_t> parseU64(std::string_view s) noexcept {
    if (s.empty()) return std::nullopt;
    uint64_t v = 0;
    const auto r = std::from_chars(s.data(), s.data() + s.size(), v);
    if (r.ec != std::errc{} || r.ptr != s.data() + s.size()) return std::nullopt;
    return v;
}

const char* stateName(uint8_t s) noexcept {
    switch (s) {
        case 0: return "active";
        case 1: return "suspended-read-only";
        case 2: return "suspended-reject-all";
        case 3: return "deleting";
        default: return "unknown";
    }
}

std::string jsonString(std::string_view s) {
    std::string o = "\"";
    for (const char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (c == '"' || c == '\\') {
            o += '\\';
            o += ch;
        } else if (c < 0x20) {
            static constexpr char kHex[] = "0123456789abcdef";
            o += "\\u00";
            o += kHex[c >> 4];
            o += kHex[c & 15];
        } else {
            o += ch;
        }
    }
    o += '"';
    return o;
}

int namespaceUsageError(std::string_view cmd, std::ostream& err, const std::string& msg) {
    err << "rune namespace " << cmd << ": " << msg << "\n";
    printNamespaceUsage(err);
    return 2;
}

/// The positional <name>: the first argument when it is not an option.
bool takeName(std::vector<std::string>& args, std::string& name) {
    if (args.empty() || args[0].rfind("--", 0) == 0) return false;
    name = args[0];
    args.erase(args.begin());
    return !name.empty();
}

std::string endpointOf(const ParsedArgs& a) {
    std::string endpoint = a.value("--broker");
    if (endpoint.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env != nullptr) endpoint = env;
    }
    return endpoint;
}

#ifdef RUNE_CLI_LIVE_ENABLED
constexpr uint16_t kUnknownTopicCode = 7;   // wire ErrorCode::UnknownTopic: a namespace that does not exist

int refused(std::ostream& err, std::string_view what, const rcl::DdlResult& r) {
    err << "rune namespace " << what << ": " << engine::brokerFailure(r.errorCode, r.detail) << "\n";
    return 1;
}
#endif

int cmdCreate(std::vector<std::string> args, std::ostream& out, std::ostream& err) {
    std::string name;
    if (!takeName(args, name)) return namespaceUsageError("create", err, "a namespace <name> is required");
    std::vector<std::string> rest;
    auto patch = parseNamespaceLimitFlags(args, rest);
    if (!patch) return namespaceUsageError("create", err, patch.error());
    ParsedArgs a;
    if (!parseArgs("namespace create", rest, {"--broker"}, {}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("create", err, "--broker host:port is required");
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const auto r = broker.client().createNamespace(name, *patch, broker.ddlTimeout());
        if (!r) return refused(err, "create", r.error());
        out << "created namespace " << name << " (nsId " << *r << ")\n";
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace create: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

int cmdList(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    ParsedArgs a;
    if (!parseArgs("namespace list", args, {"--broker"}, {"--json"}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("list", err, "--broker host:port is required");
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const auto r = broker.client().listNamespaces(broker.ddlTimeout());
        if (!r) return refused(err, "list", r.error());
        out << (a.has("--json") ? formatNamespacesJson(*r) : formatNamespacesTable(*r));
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace list: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

int cmdLimits(std::vector<std::string> args, std::ostream& out, std::ostream& err) {
    std::string name;
    if (!takeName(args, name)) return namespaceUsageError("limits", err, "a namespace <name> is required");
    // `--set k=v` may repeat: pull them out before the strict parser (which refuses a flag given twice).
    std::vector<std::string> sets, rest;
    for (std::size_t i = 0; i < args.size(); ++i) {
        if (args[i] == "--set") {
            if (i + 1 >= args.size()) return namespaceUsageError("limits", err, "--set needs a <limit>=N value");
            sets.push_back(args[++i]);
        } else {
            rest.push_back(args[i]);
        }
    }
    ParsedArgs a;
    if (!parseArgs("namespace limits", rest, {"--broker"}, {}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    // Validate the assignments before connecting (a typo never reaches the broker).
    const auto patch = namespaceLimitSetsPatch(sets);
    if (!patch) return namespaceUsageError("limits", err, patch.error());
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("limits", err, "--broker host:port is required");
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const auto findIn = [&](const std::vector<rpc::NamespaceInfo>& v) -> const rpc::NamespaceInfo* {
            for (const auto& n : v)
                if (n.name == name) return &n;
            return nullptr;
        };
        const auto noNamespace = [&] {
            err << "rune namespace limits: no namespace '" << name << "'\n";
            return 1;
        };
        if (sets.empty()) {
            const auto all = broker.client().listNamespaces(broker.ddlTimeout());
            if (!all) return refused(err, "limits", all.error());
            const rpc::NamespaceInfo* ns = findIn(*all);
            if (ns == nullptr) return noNamespace();
            out << formatNamespaceLimits(ns->limits);
            return 0;
        }
        // Issue #641: send only the named limits; the broker merges them onto the CURRENT record, so a concurrent
        // change to another limit survives.  No pre-list: a caller who may not see the namespace still gets the
        // broker's own refusal (NotAuthorized names itself).  The result is re-read to print what is stored now.
        const auto r = broker.client().setNamespaceLimits(name, *patch, broker.ddlTimeout());
        if (!r) {
            if (r.error().errorCode == kUnknownTopicCode) return noNamespace();
            return refused(err, "limits", r.error());
        }
        const auto after = broker.client().listNamespaces(broker.ddlTimeout());
        if (!after) return refused(err, "limits", after.error());
        const rpc::NamespaceInfo* now = findIn(*after);
        if (now == nullptr) return noNamespace();   // dropped between the patch and the re-read
        out << "updated limits of namespace " << name << "\n";
        out << formatNamespaceLimits(now->limits);
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace limits: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

int cmdSuspend(std::vector<std::string> args, std::ostream& out, std::ostream& err) {
    std::string name;
    if (!takeName(args, name)) return namespaceUsageError("suspend", err, "a namespace <name> is required");
    ParsedArgs a;
    if (!parseArgs("namespace suspend", args, {"--broker"}, {"--read-only", "--reject-all"}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    if (a.has("--read-only") == a.has("--reject-all"))
        return namespaceUsageError("suspend", err, "exactly one of --read-only or --reject-all is required");
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("suspend", err, "--broker host:port is required");
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const bool readOnly = a.has("--read-only");
        const auto r = broker.client().suspendNamespace(
            name, readOnly ? rpc::NsSuspendMode::ReadOnly : rpc::NsSuspendMode::RejectAll, broker.ddlTimeout());
        if (!r) return refused(err, "suspend", r.error());
        out << "suspended namespace " << name << (readOnly ? " (read-only)" : " (reject-all)") << "\n";
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace suspend: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

int cmdResume(std::vector<std::string> args, std::ostream& out, std::ostream& err) {
    std::string name;
    if (!takeName(args, name)) return namespaceUsageError("resume", err, "a namespace <name> is required");
    ParsedArgs a;
    if (!parseArgs("namespace resume", args, {"--broker"}, {}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("resume", err, "--broker host:port is required");
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const auto r = broker.client().resumeNamespace(name, broker.ddlTimeout());
        if (!r) return refused(err, "resume", r.error());
        out << "resumed namespace " << name << "\n";
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace resume: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

int cmdDelete(std::vector<std::string> args, std::istream& in, bool interactive, std::ostream& out, std::ostream& err) {
    std::string name;
    if (!takeName(args, name)) return namespaceUsageError("delete", err, "a namespace <name> is required");
    ParsedArgs a;
    if (!parseArgs("namespace delete", args, {"--broker"}, {"--yes"}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("delete", err, "--broker host:port is required");
    // Deleting drops every object of the namespace: without --yes it is confirmed on a terminal and refused anywhere else.
    if (!a.has("--yes")) {
        if (!interactive) {
            err << "rune namespace delete: refusing to delete namespace '" << name
                << "' without --yes (not a terminal: nothing to confirm with)\n";
            return 1;
        }
        err << "Delete namespace '" << name << "' and every topic, view, user and group in it? This cannot be undone. [y/N] "
            << std::flush;
        std::string answer;
        std::getline(in, answer);
        if (answer != "y" && answer != "Y" && answer != "yes" && answer != "YES") {
            err << "rune namespace delete: aborted\n";
            return 1;
        }
    }
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const auto r = broker.client().deleteNamespace(name, broker.ddlTimeout());
        if (!r) return refused(err, "delete", r.error());
        out << "deleting namespace " << name << " (its sessions are closed and its objects dropped in steps)\n";
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace delete: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

int cmdUsage(std::vector<std::string> args, std::ostream& out, std::ostream& err) {
    std::string name;
    if (!takeName(args, name)) return namespaceUsageError("usage", err, "a namespace <name> is required");
    ParsedArgs a;
    if (!parseArgs("namespace usage", args, {"--broker"}, {"--json"}, a, err)) {
        printNamespaceUsage(err);
        return 2;
    }
    const std::string endpoint = endpointOf(a);
    if (endpoint.empty()) return namespaceUsageError("usage", err, "--broker host:port is required");
#ifdef RUNE_CLI_LIVE_ENABLED
    engine::LiveBroker broker;
    if (!broker.open(engine::liveOptionsOf(a, endpoint, false), err)) return 1;
    try {
        const auto r = broker.client().namespaceUsage(name, broker.ddlTimeout());
        if (!r) return refused(err, "usage", r.error());
        out << (a.has("--json") ? formatNamespaceUsageJson(name, *r) : formatNamespaceUsage(name, *r));
        return 0;
    } catch (const std::exception& e) {
        err << "rune: " << e.what() << "\n";
        return 1;
    }
#else
    (void)out;
    err << "rune namespace usage: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build.\n";
    return 1;
#endif
}

} // namespace

// ═══════════════════════════════════════════════════════════════════════════
// Pure helpers
// ═══════════════════════════════════════════════════════════════════════════
std::expected<rcl::NamespaceLimitsPatch, std::string>
parseNamespaceLimitFlags(const std::vector<std::string>& args, std::vector<std::string>& rest) {
    rcl::NamespaceLimitsPatch patch;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string& a = args[i];
        const auto idx = a.rfind("--", 0) == 0 ? rcl::namespaceLimitIndex(std::string_view(a).substr(2)) : std::nullopt;
        if (!idx) {
            rest.push_back(a);
            continue;
        }
        if (i + 1 >= args.size()) return std::unexpected(a + " needs a value");
        if (patch.has(*idx)) return std::unexpected(a + " given twice");
        const auto v = parseU64(args[++i]);
        if (!v || !patch.set(*idx, *v))
            return std::unexpected(a + ": '" + args[i] + "' is not a valid value (a non-negative integer that fits the limit)");
    }
    return patch;
}

std::expected<rcl::NamespaceLimitsPatch, std::string> namespaceLimitSetsPatch(const std::vector<std::string>& sets) {
    rcl::NamespaceLimitsPatch patch;
    for (const std::string& s : sets) {
        const auto eq = s.find('=');
        if (eq == std::string::npos || eq == 0 || eq + 1 == s.size())
            return std::unexpected("--set '" + s + "': expected <limit>=N");
        const std::string_view key = std::string_view(s).substr(0, eq);
        const auto idx = rcl::namespaceLimitIndex(key);
        if (!idx) return std::unexpected("--set '" + s + "': unknown limit '" + std::string(key) + "'");
        if (patch.has(*idx)) return std::unexpected("--set: '" + std::string(key) + "' given twice");
        const auto v = parseU64(std::string_view(s).substr(eq + 1));
        if (!v || !patch.set(*idx, *v))
            return std::unexpected("--set '" + s + "': not a valid value (a non-negative integer that fits the limit)");
    }
    return patch;
}

std::expected<rp::NamespaceLimits, std::string>
applyNamespaceLimitSets(rp::NamespaceLimits base, const std::vector<std::string>& sets) {
    const auto patch = namespaceLimitSetsPatch(sets);
    if (!patch) return std::unexpected(patch.error());
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i)
        if (patch->has(i)) (void)rcl::namespaceLimitSet(base, i, rcl::namespaceLimitGet(patch->values, i));
    return base;
}

std::string formatNamespacesTable(const std::vector<rpc::NamespaceInfo>& v) {
    std::size_t idW = 4, nameW = 4;
    for (const auto& n : v) {
        idW = std::max(idW, std::to_string(n.nsId).size());
        nameW = std::max(nameW, n.name.size());
    }
    std::ostringstream o;
    auto row = [&](const std::string& id, const std::string& name, const std::string& state) {
        o << id << std::string(idW - id.size() + 2, ' ') << name << std::string(nameW - name.size() + 2, ' ') << state
          << "\n";
    };
    row("nsId", "name", "state");
    for (const auto& n : v) row(std::to_string(n.nsId), n.name, stateName(n.state));
    return o.str();
}

std::string formatNamespacesJson(const std::vector<rpc::NamespaceInfo>& v) {
    std::ostringstream o;
    o << "[";
    for (std::size_t k = 0; k < v.size(); ++k) {
        const auto& n = v[k];
        o << (k ? "," : "") << "{\"nsId\":" << n.nsId << ",\"name\":" << jsonString(n.name) << ",\"state\":\""
          << stateName(n.state) << "\",\"limits\":{";
        for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i)
            o << (i ? "," : "") << "\"" << rcl::kNamespaceLimitFields[i].name << "\":" << rcl::namespaceLimitGet(n.limits, i);
        o << "}}";
    }
    o << "]\n";
    return o.str();
}

std::string formatNamespaceLimits(const rp::NamespaceLimits& l) {
    std::size_t w = 0;
    for (const auto& f : rcl::kNamespaceLimitFields) w = std::max(w, f.name.size());
    std::ostringstream o;
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i) {
        const auto& f = rcl::kNamespaceLimitFields[i];
        o << f.name << std::string(w - f.name.size() + 2, ' ') << rcl::namespaceLimitGet(l, i) << "\n";
    }
    return o.str();
}

namespace {
// The fields whose usage this build can report (see ClientGateway::handleNamespaceUsage); the rest print `-`.
bool hasUsage(std::size_t i) noexcept {
    switch (i) {
        case 0: case 1: case 2: case 3: case 8: case 13: case 14: case 15: case 16: case 17: case 18: case 19: case 20: case 23:
            return true;
        default:
            return false;
    }
}
}  // namespace

std::string formatNamespaceUsage(std::string_view name, const rpc::NamespaceUsageResponse& r) {
    std::size_t w = 5;
    for (const auto& f : rcl::kNamespaceLimitFields) w = std::max(w, f.name.size());
    std::ostringstream o;
    o << "namespace " << name << " (" << stateName(r.state) << ")\n";
    auto row = [&](const std::string& k, const std::string& v, const std::string& u) {
        o << k << std::string(w - k.size() + 2, ' ') << v << std::string(v.size() < 12 ? 12 - v.size() : 0, ' ') << "  " << u << "\n";
    };
    row("limit", "value", "usage");
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i)
        row(std::string(rcl::kNamespaceLimitFields[i].name), std::to_string(rcl::namespaceLimitGet(r.limits, i)),
            hasUsage(i) ? std::to_string(r.usage[i]) : std::string("-"));
    return o.str();
}

std::string formatNamespaceUsageJson(std::string_view name, const rpc::NamespaceUsageResponse& r) {
    std::ostringstream o;
    o << "{\"name\":" << jsonString(name) << ",\"state\":\"" << stateName(r.state) << "\",\"limits\":{";
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i)
        o << (i ? "," : "") << "\"" << rcl::kNamespaceLimitFields[i].name << "\":" << rcl::namespaceLimitGet(r.limits, i);
    o << "},\"usage\":{";
    bool first = true;
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i) {
        if (!hasUsage(i)) continue;
        o << (first ? "" : ",") << "\"" << rcl::kNamespaceLimitFields[i].name << "\":" << r.usage[i];
        first = false;
    }
    o << "}}\n";
    return o.str();
}

void printNamespaceUsage(std::ostream& err) {
    err << "  rune namespace create <name> [--<limit> N ...] --broker host:port\n"
        << "      Creates a namespace (ADMIN ON CLUSTER).  Limits not named take the broker's defaults.\n"
        << "  rune namespace list [--json] --broker host:port\n"
        << "      Lists every namespace: nsId, name, state (and every limit with --json).\n"
        << "  rune namespace limits <name> [--set <limit>=N ...] --broker host:port\n"
        << "      Prints the namespace's limits; with --set, changes the named ones (the others keep their values).\n"
        << "  rune namespace suspend <name> --read-only|--reject-all --broker host:port\n"
        << "      Suspends a namespace (ADMIN ON CLUSTER): read-only serves reads and refuses writes; reject-all ends its sessions.\n"
        << "  rune namespace resume <name> --broker host:port\n"
        << "      Resumes a suspended namespace.\n"
        << "  rune namespace delete <name> [--yes] --broker host:port\n"
        << "      Deletes a namespace and every object in it.  Asks on a terminal; without --yes it refuses anywhere else.\n"
        << "  rune namespace usage <name> [--json] --broker host:port\n"
        << "      Prints each limit next to its current usage (a cluster admin, or DESCRIBE ON NAMESPACE in its own namespace).\n"
        << "  <limit>, 0 = unlimited where the limit allows it:";
    for (std::size_t i = 0; i < rcl::kNamespaceLimitFields.size(); ++i)
        err << (i % 4 == 0 ? "\n      " : " ") << rcl::kNamespaceLimitFields[i].name;
    err << "\n";
}

int cmdNamespace(const std::vector<std::string>& args, std::ostream& out, std::ostream& err) {
    return cmdNamespace(args, std::cin, ::isatty(STDIN_FILENO) != 0, out, err);
}

int cmdNamespace(const std::vector<std::string>& args, std::istream& in, bool interactive, std::ostream& out, std::ostream& err) {
    if (args.empty()) {
        err << "rune namespace: a subcommand is required\n";
        printNamespaceUsage(err);
        return 2;
    }
    std::vector<std::string> rest(args.begin() + 1, args.end());
    if (args[0] == "create") return cmdCreate(std::move(rest), out, err);
    if (args[0] == "list") return cmdList(rest, out, err);
    if (args[0] == "limits") return cmdLimits(std::move(rest), out, err);
    if (args[0] == "suspend") return cmdSuspend(std::move(rest), out, err);
    if (args[0] == "resume") return cmdResume(std::move(rest), out, err);
    if (args[0] == "delete") return cmdDelete(std::move(rest), in, interactive, out, err);
    if (args[0] == "usage") return cmdUsage(std::move(rest), out, err);
    err << "rune namespace: unknown subcommand '" << args[0] << "'\n";
    printNamespaceUsage(err);
    return 2;
}

} // namespace rune::cli
