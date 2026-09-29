// rune-ql parser — tokenizer + recursive-descent DDL parser
// Grammar:
//   statement := "topic" IDENT "{" field ("," field)* ","? "}" "partitions" INT "key" IDENT ";"
//   field     := IDENT type
//   type      := int32|int64|uint16|uint32|uint64|float64|bool|bytes
//              | string
//              | string "(" INT ")"
#include "rune/ql/Parser.h"
#include <algorithm>
#include <charconv>
#include <string_view>

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
    Eof,
};

struct Token {
    TK      kind{TK::Eof};
    std::string_view text;
    std::size_t line{1};
    std::size_t col{1};
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
            default: break;
        }

        // Integer literal
        if (c >= '0' && c <= '9') {
            std::size_t start = pos;
            while (pos < src.size() && src[pos] >= '0' && src[pos] <= '9') advance();
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

        // Unknown character — return a 1-char Ident so the parser can produce an error
        advance();
        return {TK::Ident, src.substr(pos - 1, 1), tline, tcol};
    }
};

// ── Parser ───────────────────────────────────────────────────────────────────

struct Parser {
    Lexer  lex;
    Token  cur;

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

    void advance() noexcept { cur = lex.next(); }

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
                std::string("expected ") + std::string(what) + ", got '" + std::string(cur.text) + "'"));
        }
        return consume();
    }

    // Parse a non-negative integer from cur (which must be TK::Int).
    std::expected<uint64_t, ParseError> parseInt() noexcept {
        if (cur.kind != TK::Int) {
            return std::unexpected(err(
                std::string("expected integer, got '") + std::string(cur.text) + "'"));
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
            std::string("unknown type '") + std::string(tk.text) + "'"));
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

    // ---- top-level statement ----
    std::expected<TopicDef, ParseError> parseStatement() noexcept {
        // "topic"
        if (cur.kind != TK::Ident || cur.text != "topic") {
            return std::unexpected(err("expected 'topic'"));
        }
        advance();

        // topic name
        auto nameToken = expect(TK::Ident, "topic name");
        if (!nameToken) return std::unexpected(std::move(nameToken.error()));
        TopicDef td;
        td.name = std::string(nameToken->text);

        // "{"
        auto lb = expect(TK::LBrace, "{");
        if (!lb) return std::unexpected(std::move(lb.error()));

        // fields: field (, field)* ,?
        while (true) {
            // trailing comma: if we see '}' immediately, stop
            if (cur.kind == TK::RBrace) break;

            auto fd = parseField();
            if (!fd) return std::unexpected(std::move(fd.error()));

            // duplicate field check
            for (const auto& existing : td.fields) {
                if (existing.name == fd->name) {
                    return std::unexpected(err(
                        std::string("duplicate field name '") + fd->name + "'"));
                }
            }
            td.fields.push_back(std::move(*fd));

            if (cur.kind == TK::Comma) {
                advance(); // consume ','
                // allow trailing comma: loop continues, will see '}' next iteration
            } else {
                break; // no comma — must be '}'
            }
        }

        if (td.fields.empty()) {
            return std::unexpected(err("topic must have at least one field"));
        }

        // "}"
        auto rb = expect(TK::RBrace, "}");
        if (!rb) return std::unexpected(std::move(rb.error()));

        // "partitions"
        if (cur.kind != TK::Ident || cur.text != "partitions") {
            return std::unexpected(err("expected 'partitions'"));
        }
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

        // "key"
        if (cur.kind != TK::Ident || cur.text != "key") {
            return std::unexpected(err("expected 'key'"));
        }
        advance();

        auto keyToken = expect(TK::Ident, "key field name");
        if (!keyToken) return std::unexpected(std::move(keyToken.error()));
        td.keyField = std::string(keyToken->text);

        // validate key references a declared field
        bool found = false;
        for (const auto& f : td.fields) {
            if (f.name == td.keyField) { found = true; break; }
        }
        if (!found) {
            return std::unexpected(errAt(*keyToken,
                std::string("key field '") + td.keyField + "' is not a declared field"));
        }

        // ";"
        auto semi = expect(TK::Semi, ";");
        if (!semi) return std::unexpected(std::move(semi.error()));

        // ensure nothing follows (except EOF)
        if (cur.kind != TK::Eof) {
            return std::unexpected(err("unexpected token after statement"));
        }

        return td;
    }
};

// ── Public API ───────────────────────────────────────────────────────────────

std::expected<TopicDef, ParseError> parse(std::string_view src) noexcept {
    Parser p{src};
    return p.parseStatement();
}

} // namespace rune::ql
