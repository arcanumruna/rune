// libs/view/src/PlanCompiler.cpp
//
// CreateView-time plan compilation: resolve authored field/key names against
// the upstream source Descriptor(s), validate Phase 1's fixed-width /
// integer-key constraints, and derive the view's own output Descriptor,
// fingerprint, and RowLayout. Control-plane only (runs once per CreateView,
// off the hot path) -- not performance-sensitive, but still allocation-shape
// disciplined: no shared mutable state, no throws out of compileView.
#include <rune/view/PlanCompiler.h>

#include <algorithm>
#include <rune/codec/Fingerprint.h>

namespace rune::view {

namespace {

// Counts how many fields in `desc` are named `name`, returning the first
// match via `outField` (valid only when the count is exactly 1). Descriptor::
// find() returns the FIRST match, which would silently paper over a
// duplicate-resolving name -- so callers that need to reject ambiguity must
// count for themselves rather than trust find()'s result alone.
size_t countFieldMatches(const rune::codec::Descriptor& desc, std::string_view name,
                          const rune::codec::FieldDescriptor*& outField) {
    size_t count = 0;
    outField = nullptr;
    for (const auto& f : desc.fields) {
        if (f.name == name) {
            if (outField == nullptr) {
                outField = &f;
            }
            ++count;
        }
    }
    return count;
}

bool requiresNumeric(MergeRule rule) {
    switch (rule) {
        case MergeRule::Sum:
        case MergeRule::Avg:
        case MergeRule::Min:
        case MergeRule::Max:
            return true;
        default:
            return false;
    }
}

// Per-output-field resolution result, carried between the resolve pass and
// the output-descriptor/CompiledField build pass below.
struct ResolvedOutputField {
    uint16_t srcFieldId  = 0;
    uint16_t fixedLen    = 0;  // FixedStr only; 0 otherwise
};

} // namespace

std::expected<CompileResult, CompileError> compileView(
    const ViewPlan& authored,
    std::span<const rune::codec::Descriptor* const> sourceDescriptors,
    int32_t viewTopicId) {

    if (authored.sources.empty()) {
        return std::unexpected(CompileError{"view '" + authored.viewName +
                                             "' must declare at least one source"});
    }
    // Whole-branch review Bug C: the rest of the Phase 1 pipeline is
    // single-source only (fold uses source[0], co-location uses
    // sourceTopicId[0], one foldCursor per (view, partition)). Accepting 2+
    // sources here would silently compile a plan the rest of the engine
    // cannot correctly drive -- reject fail-closed instead. Join/multi-source
    // resolution is Phase 3 (the per-source vectors below are already sized
    // for it, but are not yet exercised with more than one entry).
    if (authored.sources.size() != 1) {
        return std::unexpected(CompileError{
            "view '" + authored.viewName + "' must declare exactly one source " +
            "(Phase 1 views are single-source; got " +
            std::to_string(authored.sources.size()) + ")"});
    }
    if (authored.output.empty()) {
        return std::unexpected(CompileError{"view '" + authored.viewName +
                                             "' must declare at least one output field"});
    }
    if (sourceDescriptors.size() != authored.sources.size()) {
        return std::unexpected(CompileError{
            "view '" + authored.viewName +
            "': sourceDescriptors count does not match sources count"});
    }
    for (const auto* d : sourceDescriptors) {
        if (d == nullptr) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': null source descriptor"});
        }
    }

    // --- 1. Resolve + validate every authored output field against its source. ---
    std::vector<ResolvedOutputField> resolved;
    resolved.reserve(authored.output.size());

    for (const auto& af : authored.output) {
        if (af.sourceIdx >= sourceDescriptors.size()) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' references out-of-range sourceIdx " + std::to_string(af.sourceIdx)});
        }
        const rune::codec::Descriptor& srcDesc = *sourceDescriptors[af.sourceIdx];

        const rune::codec::FieldDescriptor* fd = nullptr;
        const size_t matches = countFieldMatches(srcDesc, af.srcField, fd);
        if (matches == 0) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' references unknown source field '" + af.srcField + "'"});
        }
        if (matches > 1) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' source field name '" + af.srcField +
                "' resolves ambiguously (duplicate name in source schema)"});
        }

        if (!isFixedWidthColumn(fd->type)) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' source field '" + af.srcField +
                "' is var-len; Phase 1 views support fixed-width columns only"});
        }

        if (requiresNumeric(af.rule) && !isNumeric(fd->type)) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' merge rule requires a numeric source field, but '" + af.srcField +
                "' is not numeric"});
        }

        if (af.type != fd->type) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' declared type does not match resolved source field '" + af.srcField +
                "' type (Phase 1 Replace does not cast)"});
        }

        ResolvedOutputField rof;
        rof.srcFieldId = fd->id;
        rof.fixedLen   = fd->fixedLen;
        resolved.push_back(rof);
    }

    // Output field names must be unique -- a duplicate would make the output
    // Descriptor itself ambiguous to read back (Descriptor::find returns the
    // first match).
    for (size_t i = 0; i < authored.output.size(); ++i) {
        for (size_t j = i + 1; j < authored.output.size(); ++j) {
            if (authored.output[i].name == authored.output[j].name) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': duplicate output field name '" +
                    authored.output[i].name + "'"});
            }
        }
    }

    // --- 2. Resolve each source's key field -> per-source keyFieldId. ---
    std::vector<uint16_t> keyFieldIds;
    keyFieldIds.reserve(authored.sources.size());

    for (size_t i = 0; i < authored.sources.size(); ++i) {
        const auto& src = authored.sources[i];
        const rune::codec::Descriptor& srcDesc = *sourceDescriptors[i];

        const rune::codec::FieldDescriptor* fd = nullptr;
        const size_t matches = countFieldMatches(srcDesc, src.keyField, fd);
        if (matches == 0) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': source " + std::to_string(i) +
                " key field '" + src.keyField + "' not found"});
        }
        if (matches > 1) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': source " + std::to_string(i) +
                " key field '" + src.keyField +
                "' resolves ambiguously (duplicate name in source schema)"});
        }
        if (!isFixedWidthColumn(fd->type)) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': source " + std::to_string(i) +
                " key field '" + src.keyField + "' is var-len; keys must be fixed-width"});
        }
        // Whole-branch review Bug B: restrict VIEW KEY fields to 8-byte integer
        // types only (Int64/UInt64) -- see isEightByteKeyType's doc comment in
        // ViewTypes.h for why. Narrow-key (Int32/UInt32/UInt16) support is
        // DEFERRED until the GetViewRow wire carries the key's native width.
        // This check is scoped to the KEY field only: non-key integer output
        // columns still use the unrestricted isIntegerKeyType/isNumeric checks
        // above.
        if (!isEightByteKeyType(fd->type)) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': source " + std::to_string(i) +
                " key field '" + src.keyField +
                "' is not an 8-byte Phase 1 key type (Int64/UInt64 only -- narrow "
                "integer keys are deferred, see isEightByteKeyType)"});
        }

        keyFieldIds.push_back(fd->id);
    }

    // --- 3. The view's key must appear by name in the output. ---
    const auto keyIt = std::find_if(authored.output.begin(), authored.output.end(),
                                     [&](const AuthoredField& af) {
                                         return af.name == authored.key;
                                     });
    if (keyIt == authored.output.end()) {
        return std::unexpected(CompileError{
            "view '" + authored.viewName + "': key field '" + authored.key +
            "' does not appear in output"});
    }
    const size_t keyOutIdx =
        static_cast<size_t>(std::distance(authored.output.begin(), keyIt));

    // --- 4. Build the output ordering: key field first, then the rest in ---
    //        their original relative order.
    std::vector<size_t> order;
    order.reserve(authored.output.size());
    order.push_back(keyOutIdx);
    for (size_t i = 0; i < authored.output.size(); ++i) {
        if (i != keyOutIdx) {
            order.push_back(i);
        }
    }

    // --- 5. Build the output Descriptor + CompiledField list in that order. ---
    rune::codec::DescriptorBuilder builder(authored.viewName, 1);
    std::vector<CompiledField> compiledOutput;
    compiledOutput.reserve(order.size());

    for (size_t slot = 0; slot < order.size(); ++slot) {
        const size_t origIdx       = order[slot];
        const AuthoredField& af    = authored.output[origIdx];
        const ResolvedOutputField& rof = resolved[origIdx];
        const bool isKeyField      = (origIdx == keyOutIdx);

        if (af.type == rune::codec::PrimType::FixedStr) {
            builder.addFixedStr(af.name, rof.fixedLen, /*sinceVersion*/ 1, isKeyField);
        } else {
            builder.add(af.name, af.type, /*sinceVersion*/ 1, isKeyField);
        }

        CompiledField cf;
        cf.outSlot    = static_cast<uint16_t>(slot);
        cf.type       = af.type;
        cf.sourceIdx  = af.sourceIdx;
        cf.srcFieldId = rof.srcFieldId;
        cf.rule       = af.rule;
        compiledOutput.push_back(cf);
    }

    rune::codec::Descriptor outputDescriptor;
    RowLayout rowLayout;
    try {
        outputDescriptor = builder.build();
        rowLayout         = buildRowLayout(outputDescriptor);
    } catch (const std::exception& e) {
        return std::unexpected(CompileError{
            "view '" + authored.viewName + "': failed to build output schema: " + e.what()});
    }

    CompiledViewPlan plan;
    plan.viewName   = authored.viewName;
    plan.viewTopicId = viewTopicId;
    plan.keyFieldId = std::move(keyFieldIds);
    plan.sourceTopicId.reserve(authored.sources.size());
    for (const auto& src : authored.sources) {
        plan.sourceTopicId.push_back(src.topicId);
    }
    plan.outputFingerprint = rune::codec::fingerprint(outputDescriptor);
    plan.output            = std::move(compiledOutput);
    plan.persistence        = authored.persistence;
    plan.sizing             = authored.sizing;

    CompileResult result;
    result.plan             = std::move(plan);
    result.outputDescriptor = std::move(outputDescriptor);
    result.rowLayout        = std::move(rowLayout);
    return result;
}

} // namespace rune::view
