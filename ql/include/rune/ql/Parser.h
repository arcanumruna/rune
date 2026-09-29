#pragma once
#include <expected>
#include <string_view>
#include "rune/ql/Ast.h"
namespace rune::ql {
std::expected<TopicDef, ParseError> parse(std::string_view src) noexcept;
} // namespace rune::ql
