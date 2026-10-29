// rune-ql parser — tokenizer + recursive-descent DDL parser
// Grammar:
//   statement   := topicStmt | transientStmt | alterStmt | dropStmt | showStmt | diffStmt
//                | createStmt | grantStmt | revokeStmt                       (authentication: users / roles / grants)
//   transientStmt := "transient" topicStmt                 (Phase 2b E: persistence = Transient)
//   topicStmt   := "topic" IDENT [ "{" field ("," field)* ","? "}" ] [ "partitions" INT ] [ "replicas" INT ] [ "key" IDENT ]
//                  [ "with" "(" createSetting ("," createSetting)* ")" ] ";"
//                  (an untyped topic has no "{…}" and no "key"; a typed topic requires both;
//                   partitions defaults to 1; replicas is 0..32767, 0 = the broker's default)
//   createSetting := setting
//                  | "cleanup.policy" "=" ("delete" | "compact" | "compact_delete")
//                  | ("segment.bytes" | "retention.check.ms" | "file.delete.delay.ms") "=" SIGNED_INT
//                  | "durability" "=" ("majority" | "all-in-sync") | "tier" "=" ("leader" | "majority" | "strict")
//                  | ("copartition.group" | "partition.key") "=" STRING
//                  (creation-only; a key may appear once)
//   alterStmt   := "alter" "topic" IDENT ( alterOp ("," alterOp)* | "set" setting ("," setting)* ) ";"
//                  ("alter transient …" and "set persistence …" are rejected:
//                   a topic's persistence is fixed when it is defined)
//   setting     := CONFIGKEY "=" SIGNED_INT | "min.cleanable.dirty.ratio" "=" DECIMAL     (DECIMAL in [0,1], <= 6 places → ppm)
//   SIGNED_INT  := ["-"] INT  ('-' directly before the digits); any int64 — ranges are the catalog library's (topicConfigError)
//   CONFIGKEY   := "retention.ms" | "retention.bytes" | "delete.retention.ms" | "min.compaction.lag.ms" | "max.compaction.lag.ms"
//   dropStmt    := "drop" ("topic" | "view") IDENT ";"  |  "drop" ( "user" | "role" ) NAME ";"
//   createStmt  := "create" ( "user" NAME "with" "password" STRING | "role" NAME ) ";"
//   alterUser   := "alter" "user" NAME ( "add" "password" STRING | "drop" "password" INT         (1 oldest | 2)
//                                      | "set" "password" STRING [ "terminate" "sessions" ] | "disable" | "enable" ) ";"
//   grantStmt   := "grant" ( OPERATION "on" resource "to" ( "user" | "role" ) NAME | "role" NAME "to" "user" NAME ) ";"
//   revokeStmt  := "revoke" ( OPERATION "on" resource "from" ( "user" | "role" ) NAME | "role" NAME "from" "user" NAME ) ";"
//   resource    := "cluster" | ( "topic" | "view" | "group" | "transactional_id" ) ( "prefix" STRING | NAME )
//   OPERATION   := "read" | "write" | "create" | "alter" | "drop" | "describe" | "admin"
//   showSecurity:= "show" ( "users" | "roles" ) ";"
//                | "show" "grants" "for" ( "user" | "role" ) NAME [ "as" "of" ( "offset" INT | "time" STRING ) ] ";"
//                | "show" "security" "history" [ ( "user" | "role" ) NAME ] [ "since" STRING ] ";"
//   NAME        := IDENT | STRING        (names with '.', '@' or '-' are written as a STRING; a password STRING is non-empty)
//   showStmt    := "show" "history" [ ("topic" | "view" | "schema") IDENT ] [ "since" STRING ] ";"
//                | "show" ("schema" | "view" | "topic") IDENT "as" "of" ( "version" INT | "time" STRING | "offset" INT ) ";"
//   diffStmt    := "diff" "schema" IDENT "version" INT "to" INT ";"
//                | "diff" "catalog" "offset" INT "to" INT ";"
//   STRING      := '…' (no escapes); time strings are ISO-8601 UTC 'YYYY-MM-DDTHH:MM:SSZ'
//   Keywords are case-insensitive; names (IDENT) keep their case.  Dotted keys and
//   all-in-sync are written WITHOUT spaces ("retention . ms" is an error).
//   A parse() input is exactly ONE statement; parseScript() takes many.
//   alterOp     := "add" "column" field
//                | "drop" "column" IDENT
//                | "rename" "column" IDENT "to" IDENT
//                | "set" "compatibility" ("backward" | "forward")
//   field       := IDENT type
//   type        := int32|int64|uint16|uint32|uint64|float64|bool|bytes
//              | string
//              | string "(" INT ")"
#include "rune/ql/Parser.h"
#include "ConfigKeys.h"
#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>

namespace rune::ql {

// ── Tokenizer ────────────────────────────────────────────────────────────────

enum class TK : uint8_t {
    Ident,
    Int,
    LBrace,  // {
    RBrace,  // }
    LParen,  // (
    RParen,  // )
    Comma,   // ,
    Semi,    // ;
    Dot,     // .
    Dash,    // -
    Eq,      // =
    String,  // '…'   (text = the characters between the quotes)
    BadString, // ' with no closing quote
    Unknown,   // a character no token starts with, or digits glued to letters ("3replicas")
    Eof,
};

struct Token {
    TK      kind{TK::Eof};
    std::string_view text;
    std::size_t line{1};
    std::size_t col{1};
    bool        adj{false};   // starts exactly where the previous token ended (no blank / comment between)
};

struct Lexer {
    std::string_view src;
    std::size_t pos{0};
    std::size_t line{1};
    std::size_t col{1};

    // Advance position, tracking line/col
    void advance(std::size_t n = 1) noexcept {
        for (std::size_t i = 0; i < n && pos < src.size(); ++i) {
            if (src[pos] == '\n') { ++line; col = 1; }
            else                  { ++col;           }
            ++pos;
        }
    }

    void skipWsAndComments() noexcept {
        while (pos < src.size()) {
            char c = src[pos];
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                advance();
            } else if (c == '-' && pos + 1 < src.size() && src[pos + 1] == '-') {
                // line comment — skip until newline
                while (pos < src.size() && src[pos] != '\n') advance();
            } else {
                break;
            }
        }
    }

    Token next() noexcept {
        skipWsAndComments();
        if (pos >= src.size()) return {TK::Eof, {}, line, col};

        std::size_t tline = line, tcol = col;
        char c = src[pos];

        // Single-character punctuation
        switch (c) {
            case '{': advance(); return {TK::LBrace, src.substr(pos - 1, 1), tline, tcol};
            case '}': advance(); return {TK::RBrace, src.substr(pos - 1, 1), tline, tcol};
            case '(': advance(); return {TK::LParen,  src.substr(pos - 1, 1), tline, tcol};
            case ')': advance(); return {TK::RParen,  src.substr(pos - 1, 1), tline, tcol};
            case ',': advance(); return {TK::Comma,   src.substr(pos - 1, 1), tline, tcol};
            case ';': advance(); return {TK::Semi,    src.substr(pos - 1, 1), tline, tcol};
            case '.': advance(); return {TK::Dot,     src.substr(pos - 1, 1), tline, tcol};
            case '-': advance(); return {TK::Dash,    src.substr(pos - 1, 1), tline, tcol};
            case '=': advance(); return {TK::Eq,      src.substr(pos - 1, 1), tline, tcol};
            case '\'': {
                advance();
                const std::size_t start = pos;
                while (pos < src.size() && src[pos] != '\'' && src[pos] != '\n') advance();
                if (pos >= src.size() || src[pos] != '\'')
                    return {TK::BadString, src.substr(start - 1, pos - start + 1), tline, tcol};
                const auto body = src.substr(start, pos - start);
                advance();   // closing quote
                return {TK::String, body, tline, tcol};
            }
            default: break;
        }

        // Integer literal
        if (c >= '0' && c <= '9') {
            std::size_t start = pos;
            while (pos < src.size() && src[pos] >= '0' && src[pos] <= '9') advance();
            if (pos < src.size() && ((src[pos] >= 'a' && src[pos] <= 'z') || (src[pos] >= 'A' && src[pos] <= 'Z') || src[pos] == '_')) {
                while (pos < src.size() && ((src[pos] >= 'a' && src[pos] <= 'z') || (src[pos] >= 'A' && src[pos] <= 'Z') ||
                                            (src[pos] >= '0' && src[pos] <= '9') || src[pos] == '_')) advance();
                return {TK::Unknown, src.substr(start, pos - start), tline, tcol};
            }
            return {TK::Int, src.substr(start, pos - start), tline, tcol};
        }

        // Identifier / keyword
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
            std::size_t start = pos;
            while (pos < src.size()) {
                char d = src[pos];
                if ((d >= 'a' && d <= 'z') || (d >= 'A' && d <= 'Z') ||
                    (d >= '0' && d <= '9') || d == '_') {
                    advance();
                } else break;
            }
            return {TK::Ident, src.substr(start, pos - start), tline, tcol};
        }

        // Unknown character — its own token kind, so no IDENT position (a name) can accept it.
        advance();
        return {TK::Unknown, src.substr(pos - 1, 1), tline, tcol};
    }
};

// ── Parser ───────────────────────────────────────────────────────────────────

struct Parser {
    Lexer  lex;
    Token  cur;
    // Set on reaching a PASSWORD keyword and kept for the rest of the statement: from then on an error
    // names the KIND of the offending token, never its text (a mistyped, unquoted or quote-containing
    // password must not reach the terminal or a CI log).
    bool   redactTokens{false};

    explicit Parser(std::string_view src) noexcept : lex{src} {
        cur = lex.next();
    }

    // ---- helpers ----

    [[nodiscard]] ParseError err(std::string msg) const noexcept {
        return ParseError{cur.line, cur.col, std::move(msg)};
    }

    [[nodiscard]] ParseError errAt(const Token& t, std::string msg) const noexcept {
        return ParseError{t.line, t.col, std::move(msg)};
    }

    void advance() noexcept {
        const std::size_t el = lex.line, ec = lex.col;   // where the current token ended
        cur = lex.next();
        cur.adj = (cur.line == el && cur.col == ec);
    }

    [[nodiscard]] static bool ieq(std::string_view a, std::string_view b) noexcept {
        if (a.size() != b.size()) return false;
        for (std::size_t i = 0; i < a.size(); ++i)
            if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i])))
                return false;
        return true;
    }
    // Keywords are case-insensitive (names are not).
    [[nodiscard]] bool isKw(std::string_view kw) const noexcept { return cur.kind == TK::Ident && ieq(cur.text, kw); }
    // A string literal may be a password: its text (or an unterminated tail, which runs to the end of the line) is NEVER
    // part of any error message, in any statement.  This is the class guard; `redactTokens` only widens it.
    [[nodiscard]] static bool isStringTok(const Token& t) noexcept { return t.kind == TK::String || t.kind == TK::BadString; }
    [[nodiscard]] static std::string quotedText(const Token& t) {
        if (t.kind == TK::BadString) return "an unterminated string";
        if (t.kind == TK::String) return "a string";
        return "'" + std::string(t.text) + "'";
    }
    [[nodiscard]] std::string got() const {
        if (cur.kind == TK::Eof) return "end of input";
        if (isStringTok(cur)) return quotedText(cur);
        if (redactTokens) {
            switch (cur.kind) {
            case TK::Ident: return "an identifier";
            case TK::Int: return "a number";
            case TK::String: return "a string";
            case TK::BadString: return "an unterminated string";
            default: return "a symbol";
            }
        }
        if (cur.kind == TK::Unknown && cur.text.size() == 1) {
            const auto c = static_cast<unsigned char>(cur.text[0]);
            if (c < 0x21 || c >= 0x7F) {
                static constexpr char hex[] = "0123456789ABCDEF";
                return std::string("byte 0x") + hex[c >> 4] + hex[c & 0xF];
            }
        }
        return "'" + std::string(cur.text) + "'";
    }

    // Consume the current token and advance; returns it.
    Token consume() noexcept {
        Token t = cur;
        advance();
        return t;
    }

    // Expect current token to be of kind k; advance and return it, or return error.
    std::expected<Token, ParseError> expect(TK k, std::string_view what) noexcept {
        if (cur.kind != k) {
            return std::unexpected(err(
                std::string("expected ") + std::string(what) + ", got " + got()));
        }
        return consume();
    }

    // Expect current token to be TK::Ident with exact text `kw` (text-matched keyword);
    // advance and return it, or return a located error.
    std::expected<Token, ParseError> expectKeyword(std::string_view kw) noexcept {
        if (!isKw(kw)) {
            return std::unexpected(err(
                std::string("expected '") + std::string(kw) + "', got " + got()));
        }
        return consume();
    }

    // Parse a non-negative integer from cur (which must be TK::Int).
    std::expected<uint64_t, ParseError> parseInt() noexcept {
        if (cur.kind != TK::Int) {
            return std::unexpected(err(
                std::string("expected integer, got ") + got()));
        }
        uint64_t v = 0;
        auto [ptr, ec] = std::from_chars(cur.text.data(), cur.text.data() + cur.text.size(), v);
        if (ec != std::errc{}) {
            return std::unexpected(err("integer literal overflow"));
        }
        advance();
        return v;
    }

    // ---- type parsing ----
    // Returns FieldDef with just type/fixedLen filled; name is set by the field parser.
    std::expected<FieldDef, ParseError> parseType() noexcept {
        if (cur.kind != TK::Ident) {
            return std::unexpected(err("expected type keyword"));
        }
        Token tk = consume();
        using PT = rune::codec::PrimType;
        if (tk.text == "int32")   return FieldDef{"", PT::Int32, 0};
        if (tk.text == "int64")   return FieldDef{"", PT::Int64, 0};
        if (tk.text == "uint16")  return FieldDef{"", PT::UInt16, 0};
        if (tk.text == "uint32")  return FieldDef{"", PT::UInt32, 0};
        if (tk.text == "uint64")  return FieldDef{"", PT::UInt64, 0};
        if (tk.text == "float64") return FieldDef{"", PT::Float64, 0};
        if (tk.text == "bool")    return FieldDef{"", PT::Bool, 0};
        if (tk.text == "bytes")   return FieldDef{"", PT::Bytes, 0};
        if (tk.text == "string") {
            if (cur.kind == TK::LParen) {
                advance(); // consume '('
                auto nval = parseInt();
                if (!nval) return std::unexpected(std::move(nval.error()));
                if (*nval < 1) {
                    return std::unexpected(ParseError{tk.line, tk.col,
                        "string(N) width must be >= 1"});
                }
                if (*nval > 0xFFFFu) {
                    return std::unexpected(ParseError{tk.line, tk.col,
                        "string(N) width must be <= 65535"});
                }
                auto rparen = expect(TK::RParen, ")");
                if (!rparen) return std::unexpected(std::move(rparen.error()));
                uint16_t width = static_cast<uint16_t>(*nval);
                return FieldDef{"", PT::FixedStr, width};
            }
            return FieldDef{"", PT::String, 0};
        }
        // Unknown type
        return std::unexpected(errAt(tk,
            std::string("unknown type ") + quotedText(tk)));
    }

    // ---- field parsing ----
    std::expected<FieldDef, ParseError> parseField() noexcept {
        auto nameToken = expect(TK::Ident, "field name");
        if (!nameToken) return std::unexpected(std::move(nameToken.error()));

        auto fd = parseType();
        if (!fd) return std::unexpected(std::move(fd.error()));

        fd->name = std::string(nameToken->text);
        return fd;
    }

    // ---- value helpers ----

    // A non-negative integer that must fit [0, max]; the error names `what`.
    std::expected<int64_t, ParseError> parseBoundedInt(std::string_view what, int64_t max) noexcept {
        Token t = cur;
        if (cur.kind != TK::Int) {
            return std::unexpected(err(
                std::string("expected integer for ") + std::string(what) + ", got " + got()));
        }
        uint64_t v = 0;
        auto [ptr, ec] = std::from_chars(t.text.data(), t.text.data() + t.text.size(), v);
        if (ec != std::errc{} || v > static_cast<uint64_t>(max)) {
            return std::unexpected(errAt(t, std::string(what) + " must be <= " + std::to_string(max)));
        }
        advance();
        return static_cast<int64_t>(v);
    }

    // SIGNED_INT := ["-"] INT, the '-' written directly before the digits; any int64.
    // No range check here: the catalog library's topicConfigError is the ONE range
    // authority (ruling 4), so e.g. retention.ms = -1 (unlimited) parses.
    std::expected<int64_t, ParseError> parseSignedInt(std::string_view what) noexcept {
        const Token start = cur;
        bool neg = false;
        if (cur.kind == TK::Dash) {
            neg = true;
            advance();
            if (cur.kind != TK::Int || !cur.adj) {
                return std::unexpected(err("expected digits right after '-' for " + std::string(what) + ", got " + got()));
            }
        }
        if (cur.kind != TK::Int) {
            return std::unexpected(err("expected integer for " + std::string(what) + ", got " + got()));
        }
        constexpr uint64_t kMaxMag = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        uint64_t mag = 0;
        auto [ptr, ec] = std::from_chars(cur.text.data(), cur.text.data() + cur.text.size(), mag);
        if (ec != std::errc{} || mag > kMaxMag + (neg ? 1u : 0u)) {
            return std::unexpected(errAt(start, std::string(what) + " is outside the 64-bit signed range"));
        }
        advance();
        if (!neg) return static_cast<int64_t>(mag);
        if (mag == kMaxMag + 1) return std::numeric_limits<int64_t>::min();
        return -static_cast<int64_t>(mag);
    }

    // IDENT ("." IDENT)* with no blanks around the dots, lower-cased (keys are keywords).
    std::expected<std::string, ParseError> parseDottedKey() noexcept {
        auto first = expect(TK::Ident, "setting name");
        if (!first) return std::unexpected(std::move(first.error()));
        std::string key(first->text);
        while (cur.kind == TK::Dot && cur.adj) {
            advance();
            if (cur.kind != TK::Ident || !cur.adj) {
                return std::unexpected(err("expected a name right after '.' in a setting name, got " + got()));
            }
            key += '.';
            key += cur.text;
            advance();
        }
        for (auto& c : key) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return key;
    }

    // WORD ("-" WORD)* (e.g. all-in-sync), lower-cased.
    std::expected<std::string, ParseError> parseWord(std::string_view what) noexcept {
        auto first = expect(TK::Ident, what);
        if (!first) return std::unexpected(std::move(first.error()));
        std::string w(first->text);
        while (cur.kind == TK::Dash && cur.adj) {
            advance();
            if (cur.kind != TK::Ident || !cur.adj) return std::unexpected(err("expected a word after '-', got " + got()));
            w += '-';
            w += cur.text;
            advance();
        }
        for (auto& c : w) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return w;
    }

    // DECIMAL in [0, 1] with at most 6 decimal places → parts-per-million.
    std::expected<int64_t, ParseError> parseRatioPpm() noexcept {
        Token it = cur;
        if (cur.kind != TK::Int) {
            return std::unexpected(err("expected a decimal in [0, 1], got " + got()));
        }
        uint64_t whole = 0;
        auto [p1, e1] = std::from_chars(it.text.data(), it.text.data() + it.text.size(), whole);
        if (e1 != std::errc{} || whole > 1) {
            return std::unexpected(errAt(it, "min.cleanable.dirty.ratio must be between 0 and 1"));
        }
        advance();
        uint64_t frac = 0;
        if (cur.kind == TK::Dot && cur.adj) {
            advance();
            if (cur.kind != TK::Int || !cur.adj) {
                return std::unexpected(err("expected digits after '.', got " + got()));
            }
            if (cur.text.size() > 6) {
                return std::unexpected(err("min.cleanable.dirty.ratio has at most 6 decimal places"));
            }
            std::from_chars(cur.text.data(), cur.text.data() + cur.text.size(), frac);
            for (std::size_t i = cur.text.size(); i < 6; ++i) frac *= 10;
            advance();
        }
        const int64_t ppm = static_cast<int64_t>(whole) * 1'000'000 + static_cast<int64_t>(frac);
        if (ppm > 1'000'000) {
            return std::unexpected(errAt(it, "min.cleanable.dirty.ratio must be between 0 and 1"));
        }
        return ppm;
    }

    std::expected<std::string, ParseError> parseString(std::string_view what) noexcept {
        if (cur.kind == TK::BadString) return std::unexpected(err("unterminated string literal"));
        if (cur.kind != TK::String) {
            return std::unexpected(err(std::string("expected a quoted string for ") + std::string(what) + ", got " + got()));
        }
        std::string v(cur.text);
        advance();
        return v;
    }

    // 'YYYY-MM-DDTHH:MM:SSZ' (UTC), validated by shape and field ranges.
    std::expected<std::string, ParseError> parseIsoTime(std::string_view what) noexcept {
        Token t = cur;
        auto s = parseString(what);
        if (!s) return std::unexpected(std::move(s.error()));
        auto num = [&](std::size_t at, std::size_t n) { int v = 0; for (std::size_t i = 0; i < n; ++i) v = v * 10 + ((*s)[at + i] - '0'); return v; };
        bool ok = s->size() == 20;
        if (ok) {
            static constexpr std::string_view shape = "dddd-dd-ddTdd:dd:ddZ";
            for (std::size_t i = 0; i < 20 && ok; ++i)
                ok = shape[i] == 'd' ? ((*s)[i] >= '0' && (*s)[i] <= '9') : (*s)[i] == shape[i];
        }
        if (ok) {
            const int y = num(0, 4);
            const int mo = num(5, 2), d = num(8, 2), h = num(11, 2), mi = num(14, 2), se = num(17, 2);
            const bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
            static constexpr int kDays[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
            ok = mo >= 1 && mo <= 12 && h <= 23 && mi <= 59 && se <= 59 && d >= 1 &&
                 d <= kDays[mo - 1] + ((mo == 2 && leap) ? 1 : 0);
        }
        if (!ok) {
            return std::unexpected(errAt(t, "invalid time '" + *s + "': expected ISO-8601 UTC like '2026-10-13T12:00:00Z'"));
        }
        return s;
    }

    // setting := key "=" value, validated against the shared table.
    // alterOnly: only the mutable settings are accepted (`alter topic … set`).
    std::expected<ConfigSetting, ParseError> parseSetting(bool alterOnly) noexcept {
        Token keyTok = cur;
        auto key = parseDottedKey();
        if (!key) return std::unexpected(std::move(key.error()));
        const auto* spec = detail::findSetting(*key);
        if (!spec || (alterOnly && spec->alterKey == 0)) {
            return std::unexpected(errAt(keyTok, "unknown or immutable topic setting '" + *key + "'"));
        }
        auto eq = expect(TK::Eq, "'='");
        if (!eq) return std::unexpected(std::move(eq.error()));

        ConfigSetting cs;
        cs.key = std::string(spec->name);
        switch (spec->value) {
            case detail::Value::Int: {
                auto v = parseSignedInt(spec->name);
                if (!v) return std::unexpected(std::move(v.error()));
                cs.value = *v;
                break;
            }
            case detail::Value::Ratio: {
                auto v = parseRatioPpm();
                if (!v) return std::unexpected(std::move(v.error()));
                cs.value = *v;
                break;
            }
            case detail::Value::Cleanup:
            case detail::Value::Durability:
            case detail::Value::Tier: {
                Token wt = cur;
                auto w = parseWord(spec->name);
                if (!w) return std::unexpected(std::move(w.error()));
                int64_t v = -1;
                if (spec->value == detail::Value::Cleanup) {
                    v = *w == "delete" ? 0 : *w == "compact" ? 1 : *w == "compact_delete" ? 2 : -1;
                } else if (spec->value == detail::Value::Durability) {
                    v = *w == "majority" ? 0 : *w == "all-in-sync" ? 1 : -1;
                } else {
                    v = *w == "leader" ? 0 : *w == "majority" ? 1 : *w == "strict" ? 2 : -1;
                }
                if (v < 0) {
                    return std::unexpected(errAt(wt, "invalid value '" + *w + "' for " + std::string(spec->name)));
                }
                cs.value = v;
                cs.text  = std::move(*w);
                break;
            }
            case detail::Value::String: {
                auto v = parseString(spec->name);
                if (!v) return std::unexpected(std::move(v.error()));
                cs.text = std::move(*v);
                break;
            }
        }
        return cs;
    }

    // setting ("," setting)*  — a key may appear once.
    std::expected<std::vector<ConfigSetting>, ParseError> parseSettings(bool alterOnly) noexcept {
        std::vector<ConfigSetting> out;
        while (true) {
            Token at = cur;
            auto s = parseSetting(alterOnly);
            if (!s) return std::unexpected(std::move(s.error()));
            for (const auto& e : out) {
                if (e.key == s->key) return std::unexpected(errAt(at, "duplicate setting '" + s->key + "'"));
            }
            out.push_back(std::move(*s));
            if (cur.kind == TK::Comma) { advance(); continue; }
            break;
        }
        return out;
    }

    // ---- top-level statement: "topic" ----
    // The statement ends at its ';'; what may follow is the caller's concern
    // (parse(): nothing; parseScript(): the next statement).
    std::expected<TopicDef, ParseError> parseTopicStatement() noexcept {
        if (!isKw("topic")) {
            return std::unexpected(err("expected 'topic'"));
        }
        advance();

        auto nameToken = expect(TK::Ident, "topic name");
        if (!nameToken) return std::unexpected(std::move(nameToken.error()));
        TopicDef td;
        td.name = std::string(nameToken->text);

        // optional "{" fields "}" — absent = an untyped topic
        const bool typed = (cur.kind == TK::LBrace);
        if (typed) {
            advance();
            // fields: field (, field)* ,?
            while (true) {
                if (cur.kind == TK::RBrace) break;   // trailing comma / empty

                auto fd = parseField();
                if (!fd) return std::unexpected(std::move(fd.error()));
                for (const auto& existing : td.fields) {
                    if (existing.name == fd->name) {
                        return std::unexpected(err(
                            std::string("duplicate field name '") + fd->name + "'"));
                    }
                }
                td.fields.push_back(std::move(*fd));

                if (cur.kind == TK::Comma) advance();
                else break;
            }
            if (td.fields.empty()) {
                return std::unexpected(err("topic must have at least one field"));
            }
            auto rb = expect(TK::RBrace, "}");
            if (!rb) return std::unexpected(std::move(rb.error()));
        }

        if (isKw("partitions")) {
            advance();

            Token partTok = cur;  // save location before parseInt advances past it
            auto partVal = parseInt();
            if (!partVal) return std::unexpected(std::move(partVal.error()));
            if (*partVal < 1) {
                return std::unexpected(ParseError{partTok.line, partTok.col,
                    "partitions must be >= 1"});
            }
            if (*partVal > 0xFFFFFFFFu) {
                return std::unexpected(ParseError{partTok.line, partTok.col,
                    "partitions must be <= 4294967295"});
            }
            td.partitions = static_cast<uint32_t>(*partVal);
        }

        if (isKw("replicas")) {
            advance();
            auto rv = parseBoundedInt("replicas", std::numeric_limits<int16_t>::max());
            if (!rv) return std::unexpected(std::move(rv.error()));
            td.replicas = static_cast<int16_t>(*rv);
        }

        if (isKw("key")) {
            if (!typed) {
                return std::unexpected(err("'key' needs a field list: an untyped topic (no '{…}') has no key"));
            }
            advance();
            auto keyToken = expect(TK::Ident, "key field name");
            if (!keyToken) return std::unexpected(std::move(keyToken.error()));
            td.keyField = std::string(keyToken->text);

            bool found = false;
            for (const auto& f : td.fields) {
                if (f.name == td.keyField) { found = true; break; }
            }
            if (!found) {
                return std::unexpected(errAt(*keyToken,
                    std::string("key field '") + td.keyField + "' is not a declared field"));
            }
        } else if (typed) {
            return std::unexpected(err("expected 'key', got " + got()));
        }

        if (isKw("with")) {
            advance();
            auto lp = expect(TK::LParen, "'('");
            if (!lp) return std::unexpected(std::move(lp.error()));
            auto ss = parseSettings(false);
            if (!ss) return std::unexpected(std::move(ss.error()));
            td.with = std::move(*ss);
            auto rp = expect(TK::RParen, "')'");
            if (!rp) return std::unexpected(std::move(rp.error()));
        }

        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return td;
    }

    // ---- alter operations ----
    //   "add" "column" field
    // | "drop" "column" IDENT
    // | "rename" "column" IDENT "to" IDENT
    // | "set" "compatibility" ("backward" | "forward")
    std::expected<AlterOp, ParseError> parseAlterOp() noexcept {
        if (cur.kind != TK::Ident) {
            return std::unexpected(err("expected alter operation ('add', 'drop', 'rename', or 'set')"));
        }

        if (isKw("add")) {
            advance();
            auto col = expectKeyword("column");
            if (!col) return std::unexpected(std::move(col.error()));

            auto fd = parseField();
            if (!fd) return std::unexpected(std::move(fd.error()));

            AlterOp op;
            op.kind = AlterOpKind::Add;
            op.field = std::move(*fd);
            return op;
        }

        if (isKw("drop")) {
            advance();
            auto col = expectKeyword("column");
            if (!col) return std::unexpected(std::move(col.error()));

            auto nameToken = expect(TK::Ident, "column name");
            if (!nameToken) return std::unexpected(std::move(nameToken.error()));

            AlterOp op;
            op.kind = AlterOpKind::Drop;
            op.name = std::string(nameToken->text);
            return op;
        }

        if (isKw("rename")) {
            advance();
            auto col = expectKeyword("column");
            if (!col) return std::unexpected(std::move(col.error()));

            auto oldNameToken = expect(TK::Ident, "column name");
            if (!oldNameToken) return std::unexpected(std::move(oldNameToken.error()));

            auto to = expectKeyword("to");
            if (!to) return std::unexpected(std::move(to.error()));

            auto newNameToken = expect(TK::Ident, "new column name");
            if (!newNameToken) return std::unexpected(std::move(newNameToken.error()));

            AlterOp op;
            op.kind = AlterOpKind::Rename;
            op.name = std::string(oldNameToken->text);
            op.newName = std::string(newNameToken->text);
            return op;
        }

        if (isKw("set")) {
            advance();
            // Spec E: persistence is fixed at definition — reject the spelling a
            // user would try with an explicit message, not "expected 'compatibility'".
            if (isKw("persistence")) {
                return std::unexpected(err(
                    "alter topic cannot change persistence: a topic is transient or "
                    "durable for life (define a new topic instead)"));
            }
            auto compat = expectKeyword("compatibility");
            if (!compat) return std::unexpected(std::move(compat.error()));

            if (cur.kind != TK::Ident) {
                return std::unexpected(err("expected 'backward' or 'forward'"));
            }
            Compatibility mode;
            if (isKw("backward"))      mode = Compatibility::Backward;
            else if (isKw("forward"))  mode = Compatibility::Forward;
            else {
                return std::unexpected(err(
                    std::string("expected 'backward' or 'forward', got ") + quotedText(cur)));
            }
            advance();

            AlterOp op;
            op.kind = AlterOpKind::SetCompatibility;
            op.mode = mode;
            return op;
        }

        return std::unexpected(err(
            std::string("unknown alter operation ") + quotedText(cur)));
    }

    // ---- top-level statement: "alter" ----
    std::expected<Statement, ParseError> parseAlterStatement() noexcept {
        // "alter"
        auto alterTok = expectKeyword("alter");
        if (!alterTok) return std::unexpected(std::move(alterTok.error()));

        // Spec E: "alter transient topic …" would read as changing persistence.
        if (isKw("transient")) {
            return std::unexpected(err(
                "alter topic cannot change persistence: a topic is transient or "
                "durable for life (define a new topic instead)"));
        }

        if (isKw("user")) return parseAlterUser();

        // "topic"
        auto topicTok = expectKeyword("topic");
        if (!topicTok) return std::unexpected(std::move(topicTok.error()));

        // topic name
        auto nameToken = expect(TK::Ident, "topic name");
        if (!nameToken) return std::unexpected(std::move(nameToken.error()));
        AlterTopicDef ad;
        ad.name = std::string(nameToken->text);

        // "set" setting ("," setting)*   (topic config) — but "set compatibility …" is an alterOp
        if (isKw("set")) {
            // one-token lookahead: a copy of the lexer state
            Lexer save = lex; Token savedCur = cur;
            advance();
            const bool isCompat = isKw("compatibility") || isKw("persistence");
            lex = save; cur = savedCur;
            if (!isCompat) {
                advance();   // "set"
                auto ss = parseSettings(true);
                if (!ss) return std::unexpected(std::move(ss.error()));
                auto semi = expect(TK::Semi, ";");
                if (!semi) return std::unexpected(std::move(semi.error()));
                return Statement(std::in_place_type<AlterTopicConfigDef>, std::move(ad.name), std::move(*ss));
            }
        }

        // ops: op ("," op)*
        while (true) {
            auto op = parseAlterOp();
            if (!op) return std::unexpected(std::move(op.error()));
            ad.ops.push_back(std::move(*op));

            if (cur.kind == TK::Comma) {
                advance();
                continue;
            }
            break;
        }

        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<AlterTopicDef>, std::move(ad));
    }

    // ---- security statements (authentication) ----
    // NAME := IDENT | STRING (non-empty).  A quoted name may be namespace-qualified, 'ns/name' (namespaces D7):
    // at most one '/', with a non-empty part on each side.  'a/b/c', 'a//x', '/x' and 'a/' are parse errors; the
    // broker resolves (and hides) the namespace itself.
    std::expected<std::string, ParseError> parseName(std::string_view what) noexcept {
        if (cur.kind == TK::Ident) { std::string v(cur.text); advance(); return v; }
        if (cur.kind == TK::BadString) return std::unexpected(err("unterminated string literal"));
        if (cur.kind == TK::String) {
            if (cur.text.empty()) return std::unexpected(err(std::string(what) + " must not be empty"));
            const std::size_t slash = cur.text.find('/');
            if (slash != std::string_view::npos &&
                (slash == 0 || slash + 1 == cur.text.size() || cur.text.find('/', slash + 1) != std::string_view::npos))
                return std::unexpected(err(std::string(what) +
                                           " must be 'name' or 'namespace/name' (one '/', both parts non-empty)"));
            std::string v(cur.text);
            advance();
            return v;
        }
        return std::unexpected(err(std::string("expected ") + std::string(what) + " (an identifier or a quoted string), got " + got()));
    }

    std::expected<std::string, ParseError> parsePassword() noexcept {
        Token t = cur;
        auto p = parseString("the password");
        if (!p) return p;
        if (p->empty()) return std::unexpected(errAt(t, "password must not be empty"));
        return p;
    }

    std::expected<Grantee, ParseError> parseGranteeKind() noexcept {
        if (isKw("user")) { advance(); return Grantee::User; }
        if (isKw("role")) { advance(); return Grantee::Role; }
        return std::unexpected(err("expected 'user' or 'role', got " + got()));
    }

    std::expected<Statement, ParseError> parseCreateStatement() noexcept {
        advance();   // "create"
        if (isKw("role")) {
            advance();
            auto n = parseName("role name");
            if (!n) return std::unexpected(std::move(n.error()));
            auto semi = expect(TK::Semi, ";");
            if (!semi) return std::unexpected(std::move(semi.error()));
            return Statement(std::in_place_type<CreateRoleDef>, CreateRoleDef{std::move(*n)});
        }
        if (!isKw("user")) return std::unexpected(err("expected 'user' or 'role' after 'create', got " + got()));
        advance();
        auto n = parseName("user name");
        if (!n) return std::unexpected(std::move(n.error()));
        auto w = expectKeyword("with");
        if (!w) return std::unexpected(std::move(w.error()));
        auto pw = expectKeyword("password");
        if (!pw) return std::unexpected(std::move(pw.error()));
        redactTokens = true;
        auto p = parsePassword();
        if (!p) return std::unexpected(std::move(p.error()));
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<CreateUserDef>, CreateUserDef{std::move(*n), std::move(*p)});
    }

    // at "user" (after "alter")
    std::expected<Statement, ParseError> parseAlterUser() noexcept {
        advance();   // "user"
        AlterUserDef a;
        auto n = parseName("user name");
        if (!n) return std::unexpected(std::move(n.error()));
        a.name = std::move(*n);
        if (isKw("add") || isKw("set")) {
            const bool add = isKw("add");
            advance();
            auto pw = expectKeyword("password");
            if (!pw) return std::unexpected(std::move(pw.error()));
            redactTokens = true;
            auto p = parsePassword();
            if (!p) return std::unexpected(std::move(p.error()));
            a.password = std::move(*p);
            a.change = add ? AlterUserDef::Change::AddPassword : AlterUserDef::Change::SetPassword;
            if (!add && isKw("terminate")) {
                advance();
                auto ss = expectKeyword("sessions");
                if (!ss) return std::unexpected(std::move(ss.error()));
                a.terminateSessions = true;
            }
        } else if (isKw("drop")) {
            advance();
            auto pw = expectKeyword("password");
            if (!pw) return std::unexpected(std::move(pw.error()));
            Token it = cur;
            auto i = parseInt();
            if (!i) return std::unexpected(std::move(i.error()));
            if (*i != 1 && *i != 2) return std::unexpected(errAt(it, "password index is 1 (oldest) or 2"));
            a.change = AlterUserDef::Change::DropPassword;
            a.index = static_cast<uint8_t>(*i);
        } else if (isKw("disable")) {
            advance();
            a.change = AlterUserDef::Change::Disable;
            a.force = acceptForce();
        } else if (isKw("enable")) {
            advance();
            a.change = AlterUserDef::Change::Enable;
        } else {
            return std::unexpected(err("expected 'add', 'drop', 'set', 'disable' or 'enable' after the user name, got " + got()));
        }
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<AlterUserDef>, std::move(a));
    }

    std::expected<SecOp, ParseError> parseOperation() noexcept {
        static constexpr std::pair<std::string_view, SecOp> kOps[] = {
            {"read", SecOp::Read}, {"write", SecOp::Write}, {"create", SecOp::Create}, {"alter", SecOp::Alter},
            {"drop", SecOp::Drop}, {"describe", SecOp::Describe}, {"admin", SecOp::Admin},
            {"truncate", SecOp::Truncate}};
        if (cur.kind == TK::Ident)
            for (const auto& [kw, op] : kOps)
                if (ieq(cur.text, kw)) { advance(); return op; }
        return std::unexpected(err("expected an operation ('read', 'write', 'create', 'alter', 'drop', 'describe', 'admin' or 'truncate'), got " + got()));
    }

    std::expected<SecPattern, ParseError> parseResource() noexcept {
        SecPattern r;
        if (isKw("cluster")) { advance(); r.type = SecResource::Cluster; return r; }
        if (isKw("topic")) r.type = SecResource::Topic;
        else if (isKw("view")) r.type = SecResource::View;
        else if (isKw("group")) r.type = SecResource::Group;
        else if (isKw("transactional_id")) r.type = SecResource::TransactionalId;
        else if (isKw("namespace")) {
            // NAMESPACE 'name': an exact, unqualified namespace name (no PREFIX form, no '/').
            advance();
            Token at = cur;
            if (isKw("prefix")) {
                Lexer save = lex; Token savedCur = cur;
                advance();
                const bool patterned = cur.kind == TK::String || cur.kind == TK::BadString;
                lex = save; cur = savedCur;
                if (patterned) return std::unexpected(errAt(at, "a namespace name is exact (no PREFIX form)"));
            }
            auto n = parseName("a namespace name");
            if (!n) return std::unexpected(std::move(n.error()));
            if (n->find('/') != std::string::npos) return std::unexpected(errAt(at, "a namespace name has no '/'"));
            r.type = SecResource::Namespace;
            r.name = std::move(*n);
            return r;
        }
        else return std::unexpected(err("expected 'cluster', 'topic', 'view', 'group', 'transactional_id' or 'namespace', got " + got()));
        advance();
        if (isKw("prefix")) {
            // `prefix 'x'` is a prefix pattern; a bare `prefix` is an object named prefix.
            Lexer save = lex; Token savedCur = cur;
            advance();
            if (cur.kind == TK::String || cur.kind == TK::BadString) {
                auto s = parseString("the prefix");
                if (!s) return std::unexpected(std::move(s.error()));
                r.prefix = true;
                r.name = std::move(*s);
                return r;
            }
            lex = save; cur = savedCur;
        }
        auto n = parseName("an object name");
        if (!n) return std::unexpected(std::move(n.error()));
        r.name = std::move(*n);
        return r;
    }

    std::expected<Statement, ParseError> parseGrantRevoke(bool grant) noexcept {
        advance();   // "grant" | "revoke"
        const char* link = grant ? "to" : "from";
        if (isKw("role")) {
            advance();
            auto role = parseName("role name");
            if (!role) return std::unexpected(std::move(role.error()));
            auto l = expectKeyword(link);
            if (!l) return std::unexpected(std::move(l.error()));
            auto u = expectKeyword("user");
            if (!u) return std::unexpected(std::move(u.error()));
            auto user = parseName("user name");
            if (!user) return std::unexpected(std::move(user.error()));
            const bool force = !grant && acceptForce();
            auto semi = expect(TK::Semi, ";");
            if (!semi) return std::unexpected(std::move(semi.error()));
            if (grant) return Statement(std::in_place_type<GrantRoleDef>, GrantRoleDef{std::move(*role), std::move(*user)});
            return Statement(std::in_place_type<RevokeRoleDef>, RevokeRoleDef{std::move(*role), std::move(*user), force});
        }
        auto op = parseOperation();
        if (!op) return std::unexpected(std::move(op.error()));
        auto on = expectKeyword("on");
        if (!on) return std::unexpected(std::move(on.error()));
        auto res = parseResource();
        if (!res) return std::unexpected(std::move(res.error()));
        auto l = expectKeyword(link);
        if (!l) return std::unexpected(std::move(l.error()));
        auto who = parseGranteeKind();
        if (!who) return std::unexpected(std::move(who.error()));
        auto n = parseName(*who == Grantee::User ? "user name" : "role name");
        if (!n) return std::unexpected(std::move(n.error()));
        const bool force = !grant && acceptForce();
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        if (grant) return Statement(std::in_place_type<GrantDef>, GrantDef{*op, std::move(*res), *who, std::move(*n)});
        return Statement(std::in_place_type<RevokeDef>, RevokeDef{*op, std::move(*res), *who, std::move(*n), force});
    }

    /// Issue #121: an optional trailing FORCE (DISABLE / DROP USER / DROP ROLE / REVOKE): consumed when present.
    bool acceptForce() noexcept {
        if (!isKw("force")) return false;
        advance();
        return true;
    }

    // at "grants"
    std::expected<Statement, ParseError> parseShowGrants() noexcept {
        advance();
        auto f = expectKeyword("for");
        if (!f) return std::unexpected(std::move(f.error()));
        ShowGrantsDef d;
        auto who = parseGranteeKind();
        if (!who) return std::unexpected(std::move(who.error()));
        d.who = *who;
        auto n = parseName(*who == Grantee::User ? "user name" : "role name");
        if (!n) return std::unexpected(std::move(n.error()));
        d.name = std::move(*n);
        if (isKw("as")) {
            advance();
            auto of = expectKeyword("of");
            if (!of) return std::unexpected(std::move(of.error()));
            AsOfPoint at;
            if (isKw("offset")) {
                advance();
                auto o = parseBoundedInt("offset", std::numeric_limits<int64_t>::max());
                if (!o) return std::unexpected(std::move(o.error()));
                at.by = AsOfPoint::By::Offset;
                at.number = *o;
            } else if (isKw("time")) {
                advance();
                auto t = parseIsoTime("'as of time'");
                if (!t) return std::unexpected(std::move(t.error()));
                at.by = AsOfPoint::By::Time;
                at.timeIso = std::move(*t);
            } else {
                return std::unexpected(err("expected 'offset' or 'time' after 'as of', got " + got()));
            }
            d.at = std::move(at);
        }
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<ShowGrantsDef>, std::move(d));
    }

    // at "security"
    std::expected<Statement, ParseError> parseShowSecurityHistory() noexcept {
        advance();
        auto h = expectKeyword("history");
        if (!h) return std::unexpected(std::move(h.error()));
        ShowSecurityHistoryDef d;
        if (isKw("user") || isKw("role")) {
            auto who = parseGranteeKind();
            if (!who) return std::unexpected(std::move(who.error()));
            d.who = *who;
            auto n = parseName(*who == Grantee::User ? "user name" : "role name");
            if (!n) return std::unexpected(std::move(n.error()));
            d.name = std::move(*n);
        }
        if (isKw("since")) {
            advance();
            auto t = parseIsoTime("'since'");
            if (!t) return std::unexpected(std::move(t.error()));
            d.sinceIso = std::move(*t);
        }
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<ShowSecurityHistoryDef>, std::move(d));
    }

    // ---- drop / show / diff ----
    std::expected<Statement, ParseError> parseDropStatement() noexcept {
        advance();   // "drop"
        if (isKw("user") || isKw("role")) {
            const bool isUser = isKw("user");
            advance();
            auto n = parseName(isUser ? "user name" : "role name");
            if (!n) return std::unexpected(std::move(n.error()));
            const bool force = acceptForce();
            auto semi = expect(TK::Semi, ";");
            if (!semi) return std::unexpected(std::move(semi.error()));
            if (isUser) return Statement(std::in_place_type<DropUserDef>, DropUserDef{std::move(*n), force});
            return Statement(std::in_place_type<DropRoleDef>, DropRoleDef{std::move(*n), force});
        }
        const bool isTopic = isKw("topic");
        if (!isTopic && !isKw("view")) {
            return std::unexpected(err("expected 'topic', 'view', 'user' or 'role' after 'drop', got " + got()));
        }
        advance();
        auto nameToken = expect(TK::Ident, isTopic ? "topic name" : "view name");
        if (!nameToken) return std::unexpected(std::move(nameToken.error()));
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        if (isTopic) return Statement(std::in_place_type<DropTopicDef>, DropTopicDef{std::string(nameToken->text)});
        return Statement(std::in_place_type<DropViewDef>, DropViewDef{std::string(nameToken->text)});
    }

    // "topic" | "view" | "schema" at cur, if any (not consumed).
    [[nodiscard]] std::optional<ObjectKind> objectKindHere() const noexcept {
        if (isKw("topic")) return ObjectKind::Topic;
        if (isKw("view"))  return ObjectKind::View;
        if (isKw("schema")) return ObjectKind::Schema;
        return std::nullopt;
    }

    std::expected<Statement, ParseError> parseShowStatement() noexcept {
        advance();   // "show"
        if (isKw("users") || isKw("roles")) {
            const bool users = isKw("users");
            advance();
            auto semi = expect(TK::Semi, ";");
            if (!semi) return std::unexpected(std::move(semi.error()));
            if (users) return Statement(std::in_place_type<ShowUsersDef>);
            return Statement(std::in_place_type<ShowRolesDef>);
        }
        if (isKw("grants")) return parseShowGrants();
        if (isKw("security")) return parseShowSecurityHistory();
        if (isKw("history")) {
            advance();
            ShowHistoryDef sh;
            if (auto k = objectKindHere()) {
                sh.kind = k;
                advance();
                auto nameToken = expect(TK::Ident, "object name");
                if (!nameToken) return std::unexpected(std::move(nameToken.error()));
                sh.name = std::string(nameToken->text);
            }
            if (isKw("since")) {
                advance();
                auto t = parseIsoTime("'since'");
                if (!t) return std::unexpected(std::move(t.error()));
                sh.sinceIso = std::move(*t);
            }
            auto semi = expect(TK::Semi, ";");
            if (!semi) return std::unexpected(std::move(semi.error()));
            return Statement(std::in_place_type<ShowHistoryDef>, std::move(sh));
        }
        auto kind = objectKindHere();
        if (!kind) {
            return std::unexpected(err("expected 'history', 'schema', 'view', 'topic', 'users', 'roles', 'grants' or 'security' after 'show', got " + got()));
        }
        advance();
        auto nameToken = expect(TK::Ident, "object name");
        if (!nameToken) return std::unexpected(std::move(nameToken.error()));
        ShowAsOfDef sa;
        sa.kind = *kind;
        sa.name = std::string(nameToken->text);
        auto as = expectKeyword("as");
        if (!as) return std::unexpected(std::move(as.error()));
        auto of = expectKeyword("of");
        if (!of) return std::unexpected(std::move(of.error()));
        constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
        if (isKw("version") || isKw("offset")) {
            const bool ver = isKw("version");
            advance();
            auto n = parseBoundedInt(ver ? "version" : "offset", kMax);
            if (!n) return std::unexpected(std::move(n.error()));
            sa.at.by = ver ? AsOfPoint::By::Version : AsOfPoint::By::Offset;
            sa.at.number = *n;
        } else if (isKw("time")) {
            advance();
            auto t = parseIsoTime("'as of time'");
            if (!t) return std::unexpected(std::move(t.error()));
            sa.at.by = AsOfPoint::By::Time;
            sa.at.timeIso = std::move(*t);
        } else {
            return std::unexpected(err("expected 'version', 'time' or 'offset' after 'as of', got " + got()));
        }
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<ShowAsOfDef>, std::move(sa));
    }

    std::expected<Statement, ParseError> parseDiffStatement() noexcept {
        advance();   // "diff"
        DiffDef d;
        constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
        if (isKw("schema")) {
            advance();
            auto nameToken = expect(TK::Ident, "schema name");
            if (!nameToken) return std::unexpected(std::move(nameToken.error()));
            d.scope = DiffDef::Scope::Schema;
            d.name = std::string(nameToken->text);
            auto v = expectKeyword("version");
            if (!v) return std::unexpected(std::move(v.error()));
        } else if (isKw("catalog")) {
            advance();
            d.scope = DiffDef::Scope::Catalog;
            auto o = expectKeyword("offset");
            if (!o) return std::unexpected(std::move(o.error()));
        } else {
            return std::unexpected(err("expected 'schema' or 'catalog' after 'diff', got " + got()));
        }
        const char* what = d.scope == DiffDef::Scope::Schema ? "version" : "offset";
        auto from = parseBoundedInt(what, kMax);
        if (!from) return std::unexpected(std::move(from.error()));
        auto to = expectKeyword("to");
        if (!to) return std::unexpected(std::move(to.error()));
        auto toV = parseBoundedInt(what, kMax);
        if (!toV) return std::unexpected(std::move(toV.error()));
        d.from = *from;
        d.to = *toV;
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));
        return Statement(std::in_place_type<DiffDef>, std::move(d));
    }

    // ---- statement dispatch (parse() and parseScript() share this) ----
    std::expected<Statement, ParseError> parseStatement() noexcept {
        constexpr const char* kExpected = "expected 'topic', 'transient', 'alter', 'drop', 'show', 'diff', 'create', 'grant' or 'revoke'";
        if (cur.kind != TK::Ident) {
            return std::unexpected(err(std::string(kExpected) + ", got " + got()));
        }
        if (isKw("topic")) {
            auto r = parseTopicStatement();
            if (!r) return std::unexpected(std::move(r.error()));
            return Statement(std::in_place_type<TopicDef>, std::move(*r));
        }
        if (isKw("transient")) {
            advance();
            if (!isKw("topic")) {
                return std::unexpected(err("expected 'topic' after 'transient', got " + got()));
            }
            auto r = parseTopicStatement();
            if (!r) return std::unexpected(std::move(r.error()));
            r->persistence = Persistence::Transient;
            return Statement(std::in_place_type<TopicDef>, std::move(*r));
        }
        if (isKw("alter")) return parseAlterStatement();
        if (isKw("drop"))  return parseDropStatement();
        if (isKw("show"))  return parseShowStatement();
        if (isKw("diff"))  return parseDiffStatement();
        if (isKw("create")) return parseCreateStatement();
        if (isKw("grant"))  return parseGrantRevoke(true);
        if (isKw("revoke")) return parseGrantRevoke(false);
        return std::unexpected(err(std::string(kExpected) + ", got " + got()));
    }
};

// ── Public API ───────────────────────────────────────────────────────────────

std::expected<Statement, ParseError> parse(std::string_view src) noexcept {
    Parser p{src};
    auto r = p.parseStatement();
    if (!r) return r;
    if (p.cur.kind != TK::Eof) {
        return std::unexpected(p.err("expected end of input after statement 1; "
                                     "use a script (rune ql -f) for several statements"));
    }
    return r;
}

std::expected<std::vector<Statement>, ScriptError> parseScript(std::string_view src) noexcept {
    Parser p{src};
    std::vector<Statement> out;
    while (p.cur.kind != TK::Eof) {
        auto r = p.parseStatement();
        if (!r) return std::unexpected(ScriptError{out.size() + 1, std::move(r.error())});
        out.push_back(std::move(*r));
    }
    return out;
}

// Phase 2b Part D: the console's continuation rule, on the SAME lexer as parse(),
// so comments ('--') and strings ('...') are skipped exactly as the parser skips them.
InputState classifyInput(std::string_view src) noexcept {
    Lexer lex{src};
    Token t = lex.next();
    if (t.kind == TK::Eof) return InputState::Empty;
    TK last = t.kind;
    for (t = lex.next(); t.kind != TK::Eof; t = lex.next()) last = t.kind;
    return last == TK::Semi ? InputState::Complete : InputState::Incomplete;
}

} // namespace rune::ql
