// src/Evolve.cpp
#include <rune/codec/Evolve.h>

namespace rune::codec {

Descriptor materializeAsOf(const Descriptor& evolving, uint16_t version) {
    Descriptor d;
    d.name    = evolving.name;
    d.version = version;

    d.fields.reserve(evolving.fields.size());
    for (const auto& f : evolving.fields) {
        if (!isLiveAt(f, version)) continue;
        FieldDescriptor fv = f;
        fv.untilVersion = 0xFFFF;
        d.fields.push_back(std::move(fv));
    }

    d.blockLength = computeOffsets(d.fields);
    return d;
}

} // namespace rune::codec
