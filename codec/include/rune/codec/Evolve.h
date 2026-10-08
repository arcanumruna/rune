// include/rune/codec/Evolve.h
#pragma once
#include <cstdint>
#include <rune/codec/Descriptor.h>

namespace rune::codec {

// Produce a Descriptor containing only the fields of `evolving` that are
// live at `version` (per isLiveAt), in tag-allocation (declaration) order,
// with fixed offsets / var-indices / blockLength recomputed for exactly
// that live set. Each returned field keeps its original `id` (durable tag),
// `type`, `fixedLen`, `sinceVersion`, `isKey`, and `enumValues`; its
// `untilVersion` is reset to 0xFFFF in the returned view, since within this
// per-version snapshot the field is simply present for the view's lifetime.
//
// readField(materializeAsOf(d, v), record, tag) then decodes a record
// written against schema version v correctly, even if `d` has since
// evolved further (fields added/retired at later versions).
Descriptor materializeAsOf(const Descriptor& evolving, uint16_t version);

} // namespace rune::codec
