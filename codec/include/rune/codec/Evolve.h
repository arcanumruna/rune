// include/rune/codec/Evolve.h
#pragma once
#include <cstdint>
#include <expected>
#include <string>
#include <vector>
#include <rune/codec/AlterOps.h>
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

// Error from the pure evolution engine (checkCompatibility / applyOps).
// This is what the broker's AdminWorker surfaces back to the ALTER caller
// when a requested schema change is rejected.
struct EvolveError {
    std::string message;
};

// Validate `ops` against the schema's declared `mode` *before* applying
// them. The only rule today: under Forward compatibility, DROP is
// disallowed (dropping a field breaks old readers written against the
// wider schema, which Forward compatibility promises to support). ADD,
// RENAME and SET_COMPATIBILITY are always allowed by this check; the mode
// itself is not mutated here -- that is the caller's (broker's) job.
[[nodiscard]] std::expected<void, EvolveError> checkCompatibility(
    const std::vector<AlterOp>& ops, CompatMode mode);

// Apply `ops`, in order, to `currentEvolving` -- the cumulative "evolving"
// Descriptor that carries every field ever declared (live and retired) --
// producing the next cumulative Descriptor. Each ADD allocates a fresh
// durable tag from `nextTag` (advanced in place) and stamps
// sinceVersion=newVersion/untilVersion=0xFFFF; each DROP retires the live
// field by setting untilVersion=newVersion; each RENAME changes a live
// field's name only (tag/type untouched); SET_COMPATIBILITY is a no-op
// here (the mode is tracked by the caller).
//
// "Live" for collision/lookup purposes means untilVersion==0xFFFF (not yet
// retired) -- a retired name is free to reuse and gets a brand-new tag, so
// the old (retired) and new (live) fields of the same name coexist in the
// returned Descriptor.
//
// Atomic: on the first rejected op, returns that EvolveError and leaves
// both `currentEvolving` (read-only) and `nextTag` untouched -- no tag is
// burned and no partial Descriptor is observable. The caller discards the
// whole call on error and may retry.
[[nodiscard]] std::expected<Descriptor, EvolveError> applyOps(
    const Descriptor& currentEvolving, const std::vector<AlterOp>& ops,
    uint16_t& nextTag, uint16_t newVersion);

} // namespace rune::codec
