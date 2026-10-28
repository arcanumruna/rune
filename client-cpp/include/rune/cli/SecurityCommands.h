// include/rune/cli/SecurityCommands.h
//
// Authentication Task 8 — `rune security bootstrap | gen-cluster-key`, and the pure helpers `rune ql` uses for the
// security statements (SHOW USERS / ROLES / GRANTS / SECURITY HISTORY tables, the one-time password).
//
// Credentials (every command that connects): `--user <u>` (or RUNE_USER) and `--password-file <path>` (or
// RUNE_PASSWORD_FILE).  A password is NEVER accepted on the command line (`--password` is a usage error) and never
// read from an environment variable (RUNE_PASSWORD is ignored with a warning).
//
// Exit codes: 0 success, 1 an explicit error (a message on stderr), 2 a usage error.
#pragma once

#include <rune/security/SecurityQueries.h>

#include <ostream>
#include <string>
#include <vector>

namespace rune::cli {

/// 24 characters from [A-Za-z0-9], drawn with rejection sampling from rune-crypto's randomBytes (no modulo bias).
/// Empty on a randomness failure (never a weak password).
[[nodiscard]] std::string generateOneTimePassword();

/// D32: writes 64 hex characters (32 random bytes, no newline) to `path`, mode 0600, refusing to overwrite an
/// existing file.  false + `why` on failure.
[[nodiscard]] bool writeClusterKeyFile(const std::string& path, std::string& why);

/// SHOW tables (aligned columns, header row, each row '\n'-terminated).
///   users:   name  enabled  must_change  passwords  roles  version
///   roles:   name  members  grants  version
///   grants:  source  operation  resource  pattern        (source = direct | role:<name>)
///   history: offset  time(UTC)  kind  object(objectId)  version  principal  statement
[[nodiscard]] std::string formatUsersTable(const std::vector<rune::security::UserRow>&);
[[nodiscard]] std::string formatRolesTable(const std::vector<rune::security::RoleRow>&);
[[nodiscard]] std::string formatGrantsTable(const rune::security::GrantsView&);
[[nodiscard]] std::string formatSecurityHistoryTable(const std::vector<rune::security::RecordedEvent>&);

/// Reads the whole file at `path` into `bytes` (a key file) without leaving an unwiped copy behind.  Regular files
/// are read by size; FIFOs and other non-regular files are read to EOF (capped at 64 KiB).  A read error or an
/// oversize file wipes `bytes`, prints `rune: ...` to `err` and returns false: never a truncated key.
[[nodiscard]] bool readFileBytes(const std::string& path, std::string& bytes, std::ostream& err);

/// Prints the `rune security` usage lines.
void printSecurityUsage(std::ostream& err);

/// `rune security bootstrap|gen-cluster-key …`: args are everything after "security".
int cmdSecurity(const std::vector<std::string>& args, std::ostream& out, std::ostream& err);

} // namespace rune::cli
