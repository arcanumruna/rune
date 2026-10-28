// include/rune/cli/NamespaceCommands.h
//
// Namespaces Task 11 — `rune namespace create | list | limits`, and the pure helpers behind them (limit flags,
// `--set k=v`, the list table / JSON).  Namespace DDL is cluster-admin only (ADMIN ON CLUSTER).
//
//   rune namespace create <name> [--<limit> N ...]        unnamed limits take the broker's current defaults
//   rune namespace list [--json]
//   rune namespace limits <name>                           prints every limit
//   rune namespace limits <name> --set <limit>=N ...       read-modify-write: the other limits keep their values
//
// <limit> is one of the kebab-case names in rune/client/NamespaceLimitFields.h (max-topics, ..., feature-bits).
// Credentials and TLS as every connecting command (`--user`, `--password-file`, `--tls-ca`, ...).
//
// Exit codes: 0 success, 1 an explicit error (a message on stderr), 2 a usage error.
#pragma once

#include <rune/client/NamespaceLimitFields.h>
#include <rune/protocol/client/NamespaceMessages.h>

#include <expected>
#include <ostream>
#include <string>
#include <vector>

namespace rune::cli {

/// Pulls every `--<limit> N` pair out of `args` into a patch; the other arguments are appended to `rest` in order.
/// An unknown `--max-*`-style name is left in `rest` (parseArgs refuses it).  Error: a missing / non-numeric /
/// out-of-range value, or the same limit twice.
[[nodiscard]] std::expected<rune::client::NamespaceLimitsPatch, std::string>
parseNamespaceLimitFlags(const std::vector<std::string>& args, std::vector<std::string>& rest);

/// Issue #641: the patch of `k=v` assignments - exactly the named limits are present, so sending it changes only
/// those (the broker merges onto the current record).  Same errors as applyNamespaceLimitSets.
[[nodiscard]] std::expected<rune::client::NamespaceLimitsPatch, std::string>
namespaceLimitSetsPatch(const std::vector<std::string>& sets);

/// Applies `k=v` assignments (each `k` a limit name) to `base`.  Error: a malformed assignment, an unknown name, a
/// value that does not fit, or the same name twice.
[[nodiscard]] std::expected<rune::protocol::NamespaceLimits, std::string>
applyNamespaceLimitSets(rune::protocol::NamespaceLimits base, const std::vector<std::string>& sets);

/// `nsId  name  state` (aligned, header row, each row '\n'-terminated).
[[nodiscard]] std::string formatNamespacesTable(const std::vector<rune::protocol::client::NamespaceInfo>&);
/// `[{"nsId":1,"name":"a","state":"active","limits":{"max-topics":0,...}},...]` + '\n'.
[[nodiscard]] std::string formatNamespacesJson(const std::vector<rune::protocol::client::NamespaceInfo>&);
/// One `<limit>  <value>` line per field, in field order.
[[nodiscard]] std::string formatNamespaceLimits(const rune::protocol::NamespaceLimits&);

/// Prints the `rune namespace` usage lines.
void printNamespaceUsage(std::ostream& err);

/// `rune namespace create|list|limits …`: args are everything after "namespace".
int cmdNamespace(const std::vector<std::string>& args, std::ostream& out, std::ostream& err);

} // namespace rune::cli
