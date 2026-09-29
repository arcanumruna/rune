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

} // namespace rune::ql
