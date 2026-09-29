#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <rune/codec/PrimType.h>
namespace rune::ql {
struct FieldDef { std::string name; rune::codec::PrimType type = rune::codec::PrimType::Int32; uint16_t fixedLen = 0; };
struct TopicDef { std::string name; std::vector<FieldDef> fields; uint32_t partitions = 1; std::string keyField; };
struct ParseError { std::size_t line = 0; std::size_t col = 0; std::string message; };
} // namespace rune::ql
