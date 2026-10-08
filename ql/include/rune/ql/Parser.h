#pragma once
#include <expected>
#include <string_view>
#include <variant>
#include "rune/ql/Ast.h"
namespace rune::ql {
std::expected<std::variant<TopicDef, AlterTopicDef>, ParseError> parse(std::string_view src) noexcept;
} // namespace rune::ql
