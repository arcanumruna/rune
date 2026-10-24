#pragma once
// LogSafe.h - issue #232: a client-supplied name (a login username, an egress channel) formatted into a log line.
// `{}` of a raw name lets a `\n` forge a whole log line; LogSafeName writes printable ASCII 0x21..0x7e (except `\`)
// verbatim and every other byte - space, `\`, control bytes, >= 0x7f - as `\xHH` (lowercase hex), so a name is one
// unambiguous token.  Empty -> `""`.  No allocation beyond fmt's own buffer (cold: log lines only).

#include <fmt/format.h>

#include <string_view>

namespace rune::gateway {

struct LogSafeName {
    std::string_view v;
};

}  // namespace rune::gateway

template <>
struct fmt::formatter<rune::gateway::LogSafeName> {
    constexpr auto parse(fmt::format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const rune::gateway::LogSafeName& n, FormatContext& ctx) const -> decltype(ctx.out()) {
        auto out = ctx.out();
        if (n.v.empty()) {
            *out++ = '"';
            *out++ = '"';
            return out;
        }
        constexpr char kHex[] = "0123456789abcdef";
        for (const char ch : n.v) {
            const auto b = static_cast<unsigned char>(ch);
            if (b >= 0x21 && b <= 0x7e && b != '\\') {
                *out++ = ch;
            } else {
                *out++ = '\\';
                *out++ = 'x';
                *out++ = kHex[b >> 4];
                *out++ = kHex[b & 0x0f];
            }
        }
        return out;
    }
};
