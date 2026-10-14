#pragma once
#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <expected>
#include <rune/protocol/CatalogTypes.h>
#include "rune/ql/Ast.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/AlterOps.h>

namespace rune::ql {

[[nodiscard]] rune::codec::Descriptor toDescriptor(const TopicDef&);

/// The ONE definition of a topic's creation config lives in rune-protocol (no copy here).
using TopicCreateConfig = rune::protocol::TopicConfig;

struct DefineTopicPlan {
    std::string          name;
    uint32_t             partitions;
    std::string          keyField;
    std::vector<uint8_t> descriptorBlob;
    Persistence          persistence = Persistence::Disk;   // Phase 2b E
    int16_t              replicas = 0;                      // 0 = broker default
    TopicCreateConfig    config;                            // from `with (…)`; defaults when absent
};

[[nodiscard]] DefineTopicPlan applyPlan(const TopicDef&);

struct AlterTopicPlan {
    std::string          name;
    uint16_t             expectedVersion;
    std::vector<uint8_t> opsBlob;
};

[[nodiscard]] AlterTopicPlan applyAlterPlan(const AlterTopicDef&, uint16_t expectedVersion = 0);

struct AlterTopicConfigPlan {
    std::string name;
    std::vector<std::pair<uint8_t /*rune::protocol::ConfigKey*/, int64_t>> changes;
};

[[nodiscard]] std::expected<AlterTopicConfigPlan, ParseError> applyConfigPlan(const AlterTopicConfigDef&);

} // namespace rune::ql
