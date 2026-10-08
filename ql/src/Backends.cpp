#include "rune/ql/Backends.h"
#include <rune/codec/Serialize.h>

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

DefineTopicPlan applyPlan(const TopicDef& td) {
    return {
        td.name,
        td.partitions,
        td.keyField,
        rune::codec::serialize(toDescriptor(td))
    };
}

namespace {

rune::codec::AlterOpType toCodecOpType(AlterOpKind kind) {
    switch (kind) {
        case AlterOpKind::Add:              return rune::codec::AlterOpType::Add;
        case AlterOpKind::Drop:             return rune::codec::AlterOpType::Drop;
        case AlterOpKind::Rename:           return rune::codec::AlterOpType::Rename;
        case AlterOpKind::SetCompatibility: return rune::codec::AlterOpType::SetCompatibility;
    }
    return rune::codec::AlterOpType::Add;
}

rune::codec::CompatMode toCodecCompatMode(Compatibility mode) {
    switch (mode) {
        case Compatibility::Backward: return rune::codec::CompatMode::Backward;
        case Compatibility::Forward:  return rune::codec::CompatMode::Forward;
    }
    return rune::codec::CompatMode::Backward;
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

} // namespace rune::ql
