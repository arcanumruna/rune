// include/rune/cli/Console.h — Phase 2b Part D: `rune console` (interactive Rune QL / -f scripts / --dry-run).
#pragma once
#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace rune::cli::console {

/// `args[0]` is "console".  Interactive input and `-f -` are read from @p in.
int cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err);

} // namespace rune::cli::console
