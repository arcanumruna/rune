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

/// Issue #301: the process exit status for a broker error code.  Codes 4..124 pass through unchanged; 1..3 would
/// collide with the console's own codes (1 error, 2 usage, 3 diff) and >124 does not fit a shell status, so both map
/// to 125 (the message on stderr names the real code).  0 is never returned.
int consoleExitStatus(uint16_t brokerCode) noexcept;

/// Issue #308: after a failed statement - the code that ends the session (the session's reject code, else
/// SessionRevoked when the statement failed with it), or 0 when the session is still usable.
uint16_t consoleSessionEndCode(uint16_t sessionRejectCode, uint16_t statementErrorCode) noexcept;

/// The reject code of a rejected session as the unsigned broker code (the client keeps it as an int16_t: a code
/// >= 0x8000 from a newer broker is negative there); 0 when the session was not rejected.
uint16_t consoleRejectCode(int16_t sessionRejectCode) noexcept;

/// What a script (-f) run exits with: like the interactive console, a session that ended exits with its end code
/// (consoleExitStatus), else the failing statement's status @p batchStatus.
int consoleScriptExitStatus(int batchStatus, bool sessionEnded, uint16_t endCode) noexcept;

/// What `rune console` exits with when the connect / login at start fails with @p failureText (the client's
/// message): AuthenticationFailed is the same status a refused reconnect gives; every other failure is 1.
int consoleOpenFailureStatus(std::string_view failureText) noexcept;

/// An input line holds fewer than this many bytes (the line without its newline); a longer one is reported and
/// dropped (Issue #307).  Shared by the fd and the stream line sources.
inline constexpr std::size_t kMaxInputLineBytes = 64 * 1024;
/// The error both line sources print for a dropped line.
inline constexpr std::string_view kInputLineTooLongMessage = "error: input line too long (64 KiB); the line was discarded\n";

/// A line of input; false at EOF.
struct LineSource {
    virtual ~LineSource() = default;
    virtual bool next(std::string& line) = 0;

    /// Issue #307: true when the most recent next() returned an empty placeholder for an input line that was too
    /// long (the line was dropped); false after every other return.
    bool lastLineDiscarded = false;
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

    /// Test seam (#672): replaces ::read so a test can inject EAGAIN after poll reported POLLIN.
    void setReadFnForTest(ssize_t (*fn)(int, void*, std::size_t)) noexcept { readFn_ = fn; }

    uint64_t       idlePumps = 0;        // test seam
    std::ostream*  err       = nullptr;  // where the "input line too long" error goes (null: silent)

private:
    ssize_t                  (*readFn_)(int, void*, std::size_t) = &::read;
    int                        fd_;
    void                     (*idle_)(void*) noexcept;
    void*                      ctx_;
    std::array<char, kMaxInputLineBytes> buf_{};
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

/// Test seam (Issue #307): the interactive loop of `rune console --dry-run` over @p src.
int runInteractiveDryRunForTest(LineSource& src, std::ostream& out, std::ostream& err);

/// Test seam: the live console exactly as `rune console` runs it on the process stdin (std::cin, any fd 0: a TTY, a
/// pipe or a file), with the Client's duty clock replaced by @p nanoClock (null: the steady clock).
int runLiveOnStdinForTest(const std::vector<std::string>& args, int64_t (*nanoClock)() noexcept, std::ostream& out,
                          std::ostream& err, uint64_t* idlePumps);

} // namespace rune::cli::console
