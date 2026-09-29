#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "rune/ql/Ast.h"
#include <rune/codec/Descriptor.h>

namespace rune::ql {

[[nodiscard]] rune::codec::Descriptor toDescriptor(const TopicDef&);

struct DefineTopicPlan {
    std::string          name;
    uint32_t             partitions;
    std::string          keyField;
    std::vector<uint8_t> descriptorBlob;
};

[[nodiscard]] DefineTopicPlan applyPlan(const TopicDef&);

} // namespace rune::ql
