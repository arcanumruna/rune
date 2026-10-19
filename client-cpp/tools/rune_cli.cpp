// tools/rune_cli.cpp
//
// Task 8 — rune CLI entry point.
//
// This file contains only main(); all logic lives in src/cli/Cli.cpp so that
// it can be unit-tested without a main() symbol conflict with gtest_main.
//

#include "rune/cli/Cli.h"

#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv)
{
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(argc - 1));
    for (int i = 1; i < argc; ++i) {
        args.emplace_back(argv[i]);
    }
    return rune::cli::run(std::move(args), std::cout, std::cerr);
}
