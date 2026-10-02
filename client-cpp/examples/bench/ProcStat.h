// examples/bench/ProcStat.h — idle check + per-thread CPU split for the bench
// (spec §5).  Linux /proc readers; empty results elsewhere (macOS).  Cold path
// only (before / after a run): the parsers allocate.
#pragma once

#include <charconv>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#include <unistd.h>
#endif

namespace rune::bench {

struct CpuTimes {
    uint64_t busy  = 0;
    uint64_t total = 0;
};

namespace detail {
/// Next whitespace-separated token of `s` starting at `pos` (advances pos); empty at the end.
inline std::string_view nextToken(std::string_view s, std::size_t& pos) noexcept {
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r')) ++pos;
    const std::size_t b = pos;
    while (pos < s.size() && s[pos] != ' ' && s[pos] != '\t' && s[pos] != '\n' && s[pos] != '\r') ++pos;
    return s.substr(b, pos - b);
}
inline bool parseU64(std::string_view t, uint64_t& out) noexcept {
    if (t.empty()) return false;
    const auto [p, ec] = std::from_chars(t.data(), t.data() + t.size(), out, 10);
    return ec == std::errc{} && p == t.data() + t.size();
}
} // namespace detail

/// "cpuN user nice system idle iowait irq softirq steal ..." -> (N, times); "cpu" -> (-1, times).
[[nodiscard]] inline std::optional<std::pair<int, CpuTimes>> parseProcStatCpuLine(std::string_view line) {
    std::size_t pos = 0;
    const std::string_view label = detail::nextToken(line, pos);
    if (label.substr(0, 3) != "cpu") return std::nullopt;
    int cpu = -1;
    if (label.size() > 3) {
        uint64_t n = 0;
        if (!detail::parseU64(label.substr(3), n) || n > 1'000'000) return std::nullopt;
        cpu = static_cast<int>(n);
    }
    uint64_t f[8] = {};
    for (uint64_t& x : f) {
        if (!detail::parseU64(detail::nextToken(line, pos), x)) return std::nullopt;
    }
    CpuTimes t;
    t.busy  = f[0] + f[1] + f[2] + f[5] + f[6] + f[7];  // user nice system irq softirq steal
    t.total = t.busy + f[3] + f[4];                     // + idle iowait
    return std::make_pair(cpu, t);
}

/// "/proc/<pid>/task/<tid>/stat" -> (comm, utime + stime ticks).  The comm may
/// contain spaces and parentheses: it runs to the LAST ')'.
[[nodiscard]] inline std::optional<std::pair<std::string, uint64_t>> parseTaskStat(std::string_view s) {
    const auto open  = s.find('(');
    const auto close = s.rfind(')');
    if (open == std::string_view::npos || close == std::string_view::npos || close < open) return std::nullopt;
    const std::string_view rest = s.substr(close + 1);
    std::size_t pos = 0;
    uint64_t utime = 0, stime = 0;
    for (int i = 3; i <= 15; ++i) {  // field 3 = state ... field 14 = utime, 15 = stime
        const std::string_view field = detail::nextToken(rest, pos);
        if (field.empty()) return std::nullopt;
        if (i == 14 && !detail::parseU64(field, utime)) return std::nullopt;
        if (i == 15 && !detail::parseU64(field, stime)) return std::nullopt;
    }
    return std::make_pair(std::string(s.substr(open + 1, close - open - 1)), utime + stime);
}

[[nodiscard]] inline std::vector<CpuTimes> readPerCpuTimes() {
    std::vector<CpuTimes> out;
#if defined(__linux__)
    std::ifstream f("/proc/stat");
    std::string line;
    while (std::getline(f, line)) {
        const auto p = parseProcStatCpuLine(line);
        if (!p || p->first < 0) continue;
        if (static_cast<std::size_t>(p->first) >= out.size()) out.resize(static_cast<std::size_t>(p->first) + 1);
        out[static_cast<std::size_t>(p->first)] = p->second;
    }
#endif
    return out;
}

[[nodiscard]] inline std::vector<double> busyFractions(const std::vector<CpuTimes>& a,
                                                       const std::vector<CpuTimes>& b) {
    std::vector<double> out;
    for (std::size_t i = 0; i < a.size() && i < b.size(); ++i) {
        const double dt = b[i].total > a[i].total ? static_cast<double>(b[i].total - a[i].total) : 0.0;
        const double db = b[i].busy > a[i].busy ? static_cast<double>(b[i].busy - a[i].busy) : 0.0;
        out.push_back(dt > 0 ? db / dt : 0.0);
    }
    return out;
}

/// CPU seconds consumed by the calling thread (Linux), -1 elsewhere.
[[nodiscard]] inline double readThreadSelfCpuSeconds() {
#if defined(__linux__)
    std::ifstream f("/proc/thread-self/stat");
    const std::string s((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    const auto t = parseTaskStat(s);
    return t ? static_cast<double>(t->second) / static_cast<double>(::sysconf(_SC_CLK_TCK)) : -1.0;
#else
    return -1.0;
#endif
}

/// "0,1,2,3"-style list of CPUs this process may run on (Linux), "n/a" elsewhere.
[[nodiscard]] inline std::string cpuAffinityList() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) != 0) return "n/a";
    std::string out;
    for (int c = 0; c < CPU_SETSIZE; ++c) {
        if (CPU_ISSET(c, &set)) out += (out.empty() ? "" : ",") + std::to_string(c);
    }
    return out;
#else
    return "n/a";
#endif
}

} // namespace rune::bench
