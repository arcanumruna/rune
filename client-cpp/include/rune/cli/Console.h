// include/rune/cli/Console.h — Phase 2b Part D: `rune console` (interactive Rune QL / -f scripts / --dry-run).
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace rune::cli::console {

/// `args[0]` is "console".  Interactive input and `-f -` are read from @p in.
int cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err);

/// A line of input; false at EOF.
struct LineSource {
    virtual ~LineSource() = default;
    virtual bool next(std::string& line) = 0;
};

/// TTY input: poll(2) on `fd` every 50 ms; between polls calls idle(ctx) (pumps the Invoker client, DD9).
/// Owns its read buffer (fixed 64 KiB); a longer line is reported on `err` ("input line too long") and discarded.
struct PollingFdLineSource final : LineSource {
    PollingFdLineSource(int fd, void (*idle)(void*) noexcept, void* ctx) : fd_(fd), idle_(idle), ctx_(ctx) {}
    bool next(std::string& line) override;

    uint64_t       idlePumps = 0;        // test seam
    std::ostream*  err       = nullptr;  // where the "input line too long" error goes (null: silent)

private:
    int                        fd_;
    void                     (*idle_)(void*) noexcept;
    void*                      ctx_;
    std::array<char, 64 * 1024> buf_{};
    std::size_t                len_ = 0;
    bool                       discarding_ = false;   // inside an over-long line: drop input up to its newline
};

/// Test seam: run the live console reading `fd` (a pipe in tests) through PollingFdLineSource.
/// Returns what `rune console` would exit with; `*idlePumps` (when non-null) receives the idle pump count.
int runLiveOnFdForTest(const std::vector<std::string>& args, int fd, std::ostream& out, std::ostream& err,
                       uint64_t* idlePumps);

} // namespace rune::cli::console
