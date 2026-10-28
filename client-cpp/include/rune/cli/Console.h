// include/rune/cli/Console.h — Phase 2b Part D: `rune console` (interactive Rune QL / -f scripts / --dry-run).
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <istream>
#include <ostream>
#include <string>
#include <string_view>
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
    ~PollingFdLineSource() override;   // wipes the read buffer
    PollingFdLineSource(const PollingFdLineSource&) = delete;
    PollingFdLineSource& operator=(const PollingFdLineSource&) = delete;
    bool next(std::string& line) override;

    /// Test seam: the whole read buffer (consumed lines must not linger in it).
    [[nodiscard]] std::string_view heldBytesForTest() const noexcept { return {buf_.data(), buf_.size()}; }

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

/// Reads one password line from `fd` byte-wise (nothing after the newline is consumed) into a fixed, wiped stack
/// buffer; '\r' is skipped.  false at EOF with nothing read, or when the line is longer than 1024 bytes (`password`
/// is left empty then).
bool readPasswordLine(int fd, std::string& password);

/// Prompts "password for <user>: " on `err` and reads the password from `fd` with echo off.  The terminal is restored
/// on every path, including SIGINT / SIGTERM / SIGHUP (re-raised after the restore) and SIGTSTP (restored while
/// stopped, quiet again on continue).  false on EOF / error.
bool promptPasswordOnFd(int fd, const std::string& user, std::string& password, std::ostream& err);

/// Test seam: run the live console reading `fd` (a pipe in tests) through PollingFdLineSource.
/// Returns what `rune console` would exit with; `*idlePumps` (when non-null) receives the idle pump count.
int runLiveOnFdForTest(const std::vector<std::string>& args, int fd, std::ostream& out, std::ostream& err,
                       uint64_t* idlePumps);

/// Test seam: the live console exactly as `rune console` runs it on the process stdin (std::cin, any fd 0: a TTY, a
/// pipe or a file), with the Client's duty clock replaced by @p nanoClock (null: the steady clock).
int runLiveOnStdinForTest(const std::vector<std::string>& args, int64_t (*nanoClock)() noexcept, std::ostream& out,
                          std::ostream& err, uint64_t* idlePumps);

} // namespace rune::cli::console
