#pragma once
#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <expected>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/SecurityMessages.h>
#include "rune/ql/Ast.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/ViewPlanAuthored.h>
#include <span>
#include <string_view>

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
    rune::protocol::TopicKeyFeatures keyFeatures{};         // `autoincrement`, key.index, autoincrement.*
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

/// A security DDL statement ready for the client: the wire fields of SecurityDdl (79) minus the verifier,
/// which the client derives from `password` (SCRAM, D9).  `password` lives in memory only and is never
/// serialized or rendered; `statement` is redactedText().
struct SecurityPlan {
    rune::protocol::client::SecurityOpKind    kind = rune::protocol::client::SecurityOpKind::CreateUser;
    std::string                               name;                       // user / role / grantee the op targets
    rune::protocol::client::UserChangeKind    change = rune::protocol::client::UserChangeKind::AddPassword;   // AlterUser
    uint8_t                                   passwordIndex = 0;          // DropPassword
    uint8_t                                   terminateSessions = 0;      // SetPassword
    uint8_t                                   granteeKind = 0, op = 0, resourceType = 0, patternKind = 0;     // Grant / Revoke
    std::string                               pattern;                    // Grant / Revoke
    std::string                               roleName;                   // GrantRole / RevokeRole
    bool                                      force = false;              // issue #121: FORCE (the last admin may go)
    std::string                               password;                   // CreateUser / AddPassword / SetPassword
    std::string                               statement;                  // redactedText
};

/// Only the DDL statements are plans; SHOW statements (client-side replays) and catalog statements are not.
[[nodiscard]] std::expected<SecurityPlan, ParseError> applySecurityPlan(const Statement&);

/// The topic a `create view` reads, as the planner needs it.  An empty `descriptor` is an untyped topic.
struct ViewSource {
    std::string name;
    int32_t topicId = -1;              // -1: defined earlier in a script that has not run (--dry-run only)
    std::string keyField;
    rune::codec::Descriptor descriptor;
    int32_t partitions = 1;
    bool transient = false;
};

[[nodiscard]] ViewSource viewSourceOf(const TopicDef&);

/// A `create view` as the authored ViewPlan (P8-4); `orReplace` is the caller's.  Refusals that only the broker can
/// judge (identity, widths, partitions, keyless sources) are not made here.
[[nodiscard]] std::expected<rune::view::ViewPlan, ParseError> applyViewPlan(const CreateViewDef&, const ViewSource&);

/// "characters <= 12": the text of a plan predicate.
[[nodiscard]] std::string predicateText(std::span<const rune::view::PredicateNode> nodes);

} // namespace rune::ql
