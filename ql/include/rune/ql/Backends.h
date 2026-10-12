#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "rune/ql/Ast.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/AlterOps.h>

namespace rune::ql {

[[nodiscard]] rune::codec::Descriptor toDescriptor(const TopicDef&);

struct DefineTopicPlan {
    std::string          name;
    uint32_t             partitions;
    std::string          keyField;
    std::vector<uint8_t> descriptorBlob;
    Persistence          persistence = Persistence::Disk;   // Phase 2b E
};

[[nodiscard]] DefineTopicPlan applyPlan(const TopicDef&);

struct AlterTopicPlan {
    std::string          name;
    uint16_t             expectedVersion;
    std::vector<uint8_t> opsBlob;
};

[[nodiscard]] AlterTopicPlan applyAlterPlan(const AlterTopicDef&, uint16_t expectedVersion = 0);

} // namespace rune::ql
