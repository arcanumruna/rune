#pragma once
#include <cstdint>
#include <expected>
#include <string_view>
#include <vector>
#include "rune/ql/Ast.h"
namespace rune::ql {

/// Exactly ONE statement; anything but blanks / comments after its ';' is a ParseError.
std::expected<Statement, ParseError> parse(std::string_view src) noexcept;

struct ScriptError { std::size_t statementIndex = 0; ParseError error; };   // 1-based

/// Every ';'-terminated statement, in order.  Strict: a statement that fails to parse
/// (including an unterminated tail) is an error naming its index; nothing partial is returned.
std::expected<std::vector<Statement>, ScriptError> parseScript(std::string_view src) noexcept;

/// Console input state of buffered text (rune console's continuation prompt).
///   Empty      - only whitespace / comments;
///   Incomplete - has tokens and the last one is not ';' (or an unterminated '...' string);
///   Complete   - the last token outside comments and strings is ';'.
enum class InputState : uint8_t { Empty, Incomplete, Complete };
[[nodiscard]] InputState classifyInput(std::string_view src) noexcept;

} // namespace rune::ql
