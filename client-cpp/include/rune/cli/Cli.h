// include/rune/cli/Cli.h
//
// Task 8 — rune CLI entrypoint (unit-testable).
//
// rune::cli::run() is the testable core of the `rune` CLI.
// main() in tools/rune_cli.cpp is a thin wrapper that calls it.
//
#pragma once
#include <ostream>
#include <string>
#include <vector>

namespace rune::cli {

/// Run the rune CLI with the given arguments, writing output to @p out and
/// errors to @p err.
///
/// @param args  Argument list (excluding the program name itself).
///              e.g. {"schema","apply","file.rql","--dry-run"}
/// @param out   Stream for normal output.
/// @param err   Stream for error/diagnostic output.
/// @returns     Exit code: 0 on success, non-zero on error.
int run(std::vector<std::string> args, std::ostream& out, std::ostream& err);

} // namespace rune::cli
