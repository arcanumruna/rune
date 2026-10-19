#include "rune/ql/Backends.h"
#include "ConfigKeys.h"
#include <rune/codec/Serialize.h>
#include <stdexcept>

namespace rune::ql {

rune::codec::Descriptor toDescriptor(const TopicDef& td) {
    rune::codec::DescriptorBuilder builder(td.name);
    for (const auto& field : td.fields) {
        const bool isKey = (field.name == td.keyField);
        if (field.type == rune::codec::PrimType::FixedStr) {
            builder.addFixedStr(field.name, field.fixedLen, 1, isKey);
        } else {
            builder.add(field.name, field.type, 1, isKey);
        }
    }
    return builder.build();
}

namespace {

void applySetting(TopicCreateConfig& c, const ConfigSetting& s) {
    const auto* spec = detail::findSetting(s.key);
    if (!spec) throw std::logic_error("unknown topic setting '" + s.key + "' (not produced by the parser)");
    using F = detail::Field;
    switch (spec->field) {
        case F::RetentionMs:         c.retentionMs = s.value; break;
        case F::RetentionBytes:      c.retentionBytes = s.value; break;
        case F::DeleteRetentionMs:   c.deleteRetentionMs = s.value; break;
        case F::MinCleanableRatio:   c.minCleanableDirtyRatioPpm = static_cast<int32_t>(s.value); break;
        case F::MinCompactionLagMs:  c.minCompactionLagMs = s.value; break;
        case F::MaxCompactionLagMs:  c.maxCompactionLagMs = s.value; break;
        case F::CleanupPolicy:       c.cleanupPolicy = static_cast<uint8_t>(s.value); break;
        case F::SegmentBytes:        c.segmentRollBytes = s.value; break;
        case F::RetentionCheckMs:    c.retentionCheckMs = s.value; break;
        case F::FileDeleteDelayMs:   c.fileDeleteDelayMs = s.value; break;
        case F::Durability:          c.durability = static_cast<uint8_t>(s.value); break;
        case F::Tier:                c.durabilityTier = static_cast<uint8_t>(s.value); break;
        case F::CoPartitionGroup:    c.coPartitionGroup = s.text; break;
        case F::PartitionKey:        c.partitionKey = s.text; break;
    }
}

} // namespace

DefineTopicPlan applyPlan(const TopicDef& td) {
    DefineTopicPlan plan;
    plan.name = td.name;
    plan.partitions = td.partitions;
    plan.keyField = td.keyField;
    if (!td.fields.empty()) plan.descriptorBlob = rune::codec::serialize(toDescriptor(td));   // untyped → empty
    plan.persistence = td.persistence;
    plan.replicas = td.replicas;
    for (const auto& s : td.with) applySetting(plan.config, s);
    return plan;
}

std::expected<AlterTopicConfigPlan, ParseError> applyConfigPlan(const AlterTopicConfigDef& d) {
    AlterTopicConfigPlan plan;
    plan.name = d.name;
    for (const auto& s : d.settings) {
        const auto* spec = detail::findSetting(s.key);
        if (!spec || spec->alterKey == 0) {
            return std::unexpected(ParseError{0, 0, "unknown or immutable topic setting '" + s.key + "'"});
        }
        plan.changes.emplace_back(spec->alterKey, s.value);
    }
    return plan;
}

namespace {

rune::codec::AlterOpType toCodecOpType(AlterOpKind kind) {
    switch (kind) {
        case AlterOpKind::Add:              return rune::codec::AlterOpType::Add;
        case AlterOpKind::Drop:             return rune::codec::AlterOpType::Drop;
        case AlterOpKind::Rename:           return rune::codec::AlterOpType::Rename;
        case AlterOpKind::SetCompatibility: return rune::codec::AlterOpType::SetCompatibility;
    }
    throw std::logic_error("unhandled AlterOpKind");
}

rune::codec::CompatMode toCodecCompatMode(Compatibility mode) {
    switch (mode) {
        case Compatibility::Backward: return rune::codec::CompatMode::Backward;
        case Compatibility::Forward:  return rune::codec::CompatMode::Forward;
    }
    throw std::logic_error("unhandled Compatibility");
}

rune::codec::AlterOp toCodecOp(const AlterOp& op) {
    rune::codec::AlterOp out{};
    out.type = toCodecOpType(op.kind);
    switch (op.kind) {
        case AlterOpKind::Add:
            out.name = op.field.name;
            out.primType = op.field.type;
            out.fixedLen = op.field.fixedLen;
            break;
        case AlterOpKind::Drop:
            out.name = op.name;
            break;
        case AlterOpKind::Rename:
            out.name = op.name;
            out.newName = op.newName;
            break;
        case AlterOpKind::SetCompatibility:
            out.mode = toCodecCompatMode(op.mode);
            break;
    }
    return out;
}

} // namespace

AlterTopicPlan applyAlterPlan(const AlterTopicDef& ad, uint16_t expectedVersion) {
    std::vector<rune::codec::AlterOp> codecOps;
    codecOps.reserve(ad.ops.size());
    for (const auto& op : ad.ops) {
        codecOps.push_back(toCodecOp(op));
    }
    return {
        ad.name,
        expectedVersion,
        rune::codec::encodeAlterOps(codecOps)
    };
}

std::expected<SecurityPlan, ParseError> applySecurityPlan(const Statement& st) {
    namespace pc = rune::protocol::client;
    SecurityPlan p;
    const auto grantFields = [&p](SecOp op, const SecPattern& r, Grantee g, const std::string& who) {
        p.name = who;
        p.granteeKind = static_cast<uint8_t>(g);
        p.op = static_cast<uint8_t>(op);
        p.resourceType = static_cast<uint8_t>(r.type);
        p.patternKind = r.prefix ? 1 : 0;
        p.pattern = r.name;
    };
    if (const auto* d = std::get_if<CreateUserDef>(&st)) {
        p.kind = pc::SecurityOpKind::CreateUser; p.name = d->name; p.password = d->password;
    } else if (const auto* d = std::get_if<AlterUserDef>(&st)) {
        p.kind = pc::SecurityOpKind::AlterUser; p.name = d->name;
        switch (d->change) {
            case AlterUserDef::Change::AddPassword:  p.change = pc::UserChangeKind::AddPassword; p.password = d->password; break;
            case AlterUserDef::Change::DropPassword: p.change = pc::UserChangeKind::DropPassword; p.passwordIndex = d->index; break;
            case AlterUserDef::Change::SetPassword:
                p.change = pc::UserChangeKind::SetPassword; p.password = d->password;
                p.terminateSessions = d->terminateSessions ? 1 : 0; break;
            case AlterUserDef::Change::Disable: p.change = pc::UserChangeKind::Disable; break;
            case AlterUserDef::Change::Enable:  p.change = pc::UserChangeKind::Enable; break;
        }
    } else if (const auto* d = std::get_if<DropUserDef>(&st)) {
        p.kind = pc::SecurityOpKind::DropUser; p.name = d->name;
    } else if (const auto* d = std::get_if<CreateRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::CreateRole; p.name = d->name;
    } else if (const auto* d = std::get_if<DropRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::DropRole; p.name = d->name;
    } else if (const auto* d = std::get_if<GrantDef>(&st)) {
        p.kind = pc::SecurityOpKind::Grant; grantFields(d->op, d->resource, d->grantee, d->granteeName);
    } else if (const auto* d = std::get_if<RevokeDef>(&st)) {
        p.kind = pc::SecurityOpKind::Revoke; grantFields(d->op, d->resource, d->grantee, d->granteeName);
    } else if (const auto* d = std::get_if<GrantRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::GrantRole; p.name = d->user; p.roleName = d->role;
    } else if (const auto* d = std::get_if<RevokeRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::RevokeRole; p.name = d->user; p.roleName = d->role;
    } else {
        return std::unexpected(ParseError{0, 0, "not a security DDL statement (SHOW statements are client-side reads)"});
    }
    p.statement = redactedText(st);
    return p;
}

} // namespace rune::ql
