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
#include <optional>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/ViewKeyEncoding.h>

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

bool isAggregateRule(MergeRule r) {
    switch (r) {
        case MergeRule::Count: case MergeRule::CountAll: case MergeRule::Sum: case MergeRule::Avg:
        case MergeRule::Min: case MergeRule::Max: case MergeRule::Last: case MergeRule::LastNonNull:
            return true;
        case MergeRule::Replace:
            return false;
    }
    return false;
}

const char* primName(rune::codec::PrimType t) {
    using P = rune::codec::PrimType;
    switch (t) {
        case P::Int32: return "Int32";   case P::Int64: return "Int64";
        case P::UInt16: return "UInt16"; case P::UInt32: return "UInt32";
        case P::UInt64: return "UInt64"; case P::Float64: return "Float64";
        case P::Bool: return "Bool";     case P::Enum: return "Enum";
        case P::FixedStr: return "FixedStr";
        case P::String: return "String"; case P::Bytes: return "Bytes";
    }
    return "an unknown type";
}

// The output type an aggregate rule produces from a source field of type `src`
// (Global Constraints: fixed by rule, never cast). nullopt = not aggregatable.
// Callers have already rejected var-len sources and non-numeric Min/Max.
std::optional<rune::codec::PrimType> aggregateOutputType(MergeRule r, rune::codec::PrimType src) {
    using P = rune::codec::PrimType;
    switch (r) {
        case MergeRule::Count:
        case MergeRule::CountAll: return P::Int64;
        case MergeRule::Avg:      return isNumeric(src) ? std::optional<P>(P::Float64) : std::nullopt;
        case MergeRule::Sum:
            switch (src) {
                case P::Int32: case P::Int64:                    return P::Int64;
                case P::UInt16: case P::UInt32: case P::UInt64:  return P::UInt64;
                case P::Float64:                                 return P::Float64;
                default:                                         return std::nullopt;
            }
        case MergeRule::Min:
        case MergeRule::Max:      return isNumeric(src) ? std::optional<P>(src) : std::nullopt;
        case MergeRule::Last:
        case MergeRule::LastNonNull: return src;
        case MergeRule::Replace:  return std::nullopt;
    }
    return std::nullopt;
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
        if (af.rule == MergeRule::CountAll) {
            if (!af.srcField.empty()) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' is Count(*), which takes no source field"});
            }
            if (af.type != rune::codec::PrimType::Int64) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' is Count(*), which produces Int64; declare output type Int64"});
            }
            ResolvedOutputField rof;
            rof.srcFieldId = kNoSourceField;
            resolved.push_back(rof);
            continue;
        }
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

        if (isAggregateRule(af.rule)) {
            const auto out = aggregateOutputType(af.rule, fd->type);
            if (!out) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' cannot aggregate source field '" + af.srcField + "' of " +
                    primName(fd->type)});
            }
            if (af.type != *out) {
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': output field '" + af.name +
                    "' aggregates " + primName(fd->type) + ", which produces " +
                    primName(*out) + "; declare output type " + primName(*out)});
            }
        } else if (af.type != fd->type) {
            return std::unexpected(CompileError{
                "view '" + authored.viewName + "': output field '" + af.name +
                "' declared type does not match resolved source field '" + af.srcField +
                "' type (Replace does not cast)"});
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

        keyFieldIds.push_back(fd->id);
    }

    const auto fail = [&](CompileErrorKind kind, const std::string& why) {
        return std::unexpected(CompileError{"view '" + authored.viewName + "': " + why, kind});
    };
    const auto keyList = [](const std::vector<KeyComponent>& list) {
        std::string out;
        for (const KeyComponent& c : list) {
            if (!out.empty()) out += ", ";
            out += c.keyFunction == rune::codec::kKeyFunctionLower ? "lower(" + c.column + ")" : c.column;
        }
        return out;
    };

    // --- 3. The key components: resolve each against the source, check its type and encoded width. ---
    const rune::codec::Descriptor& srcDesc = *sourceDescriptors[0];
    const bool v1Shaped = !needsV2(authored);
    const std::vector<KeyComponent> components = authored.keyComponents.empty()
        ? std::vector<KeyComponent>{{authored.key, 0, authored.key}}
        : authored.keyComponents;

    struct ResolvedKey {
        const rune::codec::FieldDescriptor* fd;
        uint32_t width;
    };
    std::vector<ResolvedKey> keys;
    uint32_t keyWidth = 0;
    bool usesLower = false;
    for (size_t i = 0; i < components.size(); ++i) {
        const KeyComponent& c = components[i];
        const rune::codec::FieldDescriptor* fd = nullptr;
        const size_t matches = countFieldMatches(srcDesc, c.column, fd);
        if (matches == 0) {
            return fail(CompileErrorKind::FieldUnresolved, "key column '" + c.column + "' is not a field of the source");
        }
        if (matches > 1) {
            return fail(CompileErrorKind::FieldUnresolved, "key column '" + c.column + "' resolves ambiguously (duplicate name in source schema)");
        }
        const uint32_t width = rune::codec::viewKeyComponentWidth(fd->type, fd->fixedLen, c.keyFunction);
        if (width == 0) {
            switch (c.keyFunction) {
                case rune::codec::kKeyFunctionNone:
                    return fail(CompileErrorKind::KeyExpressionInvalid, "key column '" + c.column + "' must be an integer, enum, bool or string(N)");
                case rune::codec::kKeyFunctionLower:
                    return fail(CompileErrorKind::KeyExpressionInvalid,
                                "lower() needs a string(N) column; '" + c.column + "' is " + primName(fd->type));
                case rune::codec::kKeyFunctionCasefold:
                    return fail(CompileErrorKind::KeyExpressionInvalid, "casefold() is reserved");
                default:
                    return fail(CompileErrorKind::KeyExpressionInvalid, "unknown key function " + std::to_string(c.keyFunction));
            }
        }
        for (size_t j = 0; j < i; ++j) {
            if (components[j].outName == c.outName) {
                return fail(CompileErrorKind::KeyExpressionInvalid, "duplicate key column '" + c.outName + "'");
            }
        }
        if (c.keyFunction == rune::codec::kKeyFunctionLower) {
            usesLower = true;
            for (const AuthoredField& af : authored.output) {
                if (af.name == c.outName) {
                    return fail(CompileErrorKind::KeyExpressionInvalid, "duplicate key column '" + c.outName + "'");
                }
            }
        }
        keyWidth += width;
        keys.push_back({fd, width});
    }
    if (keyWidth > rune::codec::kMaxViewKeyBytes) {
        return fail(CompileErrorKind::KeyExpressionInvalid, "view key is " + std::to_string(keyWidth) + " bytes; the limit is " +
                                                            std::to_string(rune::codec::kMaxViewKeyBytes));
    }

    // --- 4. The Unicode version is pinned exactly when some component lowercases. ---
    if (authored.unicodeVersion != 0 && !usesLower) {
        return fail(CompileErrorKind::KeyExpressionInvalid, "unicodeVersion is set but no key component uses lower()");
    }
    if (authored.unicodeVersion == 0 && usesLower) {
        return fail(CompileErrorKind::KeyExpressionInvalid, "lower() needs a pinned Unicode version");
    }
    if (authored.unicodeVersion != 0 && !rune::codec::unicodeVersionSupported(authored.unicodeVersion)) {
        const auto dotted = [](uint32_t v) {
            return std::to_string(v >> 16) + "." + std::to_string((v >> 8) & 0xff) + "." + std::to_string(v & 0xff);
        };
        return fail(CompileErrorKind::KeyExpressionInvalid, "lower() is pinned to Unicode " + dotted(authored.unicodeVersion) +
                                                            "; this broker has " + dotted(rune::codec::kUnicodeVersion));
    }

    // --- 5. Aggregate views (table semantics): every column that is not a key column is an aggregate. ---
    const auto isKeyColumn = [&](const std::string& name) {
        return std::any_of(components.begin(), components.end(), [&](const KeyComponent& c) { return c.outName == name; });
    };
    const bool aggregate = std::any_of(authored.output.begin(), authored.output.end(),
                                       [](const AuthoredField& af) { return af.rule != MergeRule::Replace; });
    const auto nonAggregateColumn = [&](const std::string& name) {
        return fail(CompileErrorKind::Generic, "column '" + name + "' in an aggregate view must be an aggregate (Count, Count(*), Sum, Avg, "
                                               "Min, Max, Last, LastNonNull); only the group-by column may use Replace");
    };
    if (aggregate && v1Shaped) {
        const auto keyIt = std::find_if(authored.output.begin(), authored.output.end(),
                                        [&](const AuthoredField& af) { return af.name == components[0].outName; });
        if (keyIt == authored.output.end()) {
            return fail(CompileErrorKind::Generic, "key field '" + authored.key + "' does not appear in output");
        }
        if (keyIt->rule != MergeRule::Replace || keyIt->srcField != authored.sources[0].keyField) {
            return fail(CompileErrorKind::Generic, "the group-by column '" + authored.key + "' must use Replace and project the group-by source field '" +
                                                   authored.sources[0].keyField + "'");
        }
        for (const AuthoredField& af : authored.output) {
            if (&af != &*keyIt && af.rule == MergeRule::Replace) return nonAggregateColumn(af.name);
        }
    } else if (aggregate) {
        const bool sameGroup = authored.groupBy.size() == components.size() &&
            std::equal(components.begin(), components.end(), authored.groupBy.begin(), [](const KeyComponent& k, const KeyComponent& g) {
                return k.column == g.column && k.keyFunction == g.keyFunction;
            });
        if (!sameGroup) {
            return fail(CompileErrorKind::KeyInvalid, "group by (" + keyList(authored.groupBy) + ") must equal the key (" + keyList(components) + ")");
        }
        const auto sourceKey = std::find_if(srcDesc.fields.begin(), srcDesc.fields.end(),
                                            [](const rune::codec::FieldDescriptor& f) { return f.isKey; });
        if (sourceKey == srcDesc.fields.end() || !isIntegerKeyType(sourceKey->type)) {
            return fail(CompileErrorKind::KeyExpressionInvalid, "aggregate views need an integer source key of at most 8 bytes");
        }
        for (const AuthoredField& af : authored.output) {
            if (af.rule == MergeRule::Replace && !isKeyColumn(af.name)) return nonAggregateColumn(af.name);
        }
        if (authored.unique) return fail(CompileErrorKind::UniqueInvalid, "unique needs a projection view");
        keyFieldIds[0] = keys[0].fd->id;
    } else if (!authored.groupBy.empty()) {
        return fail(CompileErrorKind::KeyInvalid, "group by needs an aggregate column");
    }

    // --- 6. Output columns: the key columns in component order, then the remaining select items. ---
    //        A plain key column that is also selected must project its source field unchanged;
    //        otherwise it is added; a lower() key column is derived from the encoded key bytes.
    struct OutputColumn {
        AuthoredField af;
        ResolvedOutputField rof;
    };
    std::vector<OutputColumn> columns;
    columns.reserve(authored.output.size() + components.size());
    std::vector<bool> selectedAsKey(authored.output.size(), false);
    for (size_t i = 0; i < components.size(); ++i) {
        const KeyComponent& c = components[i];
        const rune::codec::FieldDescriptor& fd = *keys[i].fd;
        if (c.keyFunction == rune::codec::kKeyFunctionLower) {
            columns.push_back({{c.outName, rune::codec::PrimType::FixedStr, 0, c.column, MergeRule::Replace},
                               {kDerivedKeyField, static_cast<uint16_t>(keys[i].width)}});
            continue;
        }
        const auto sel = std::find_if(authored.output.begin(), authored.output.end(),
                                      [&](const AuthoredField& af) { return af.name == c.outName; });
        if (sel == authored.output.end()) {
            columns.push_back({{c.outName, fd.type, 0, c.column, MergeRule::Replace}, {fd.id, fd.fixedLen}});
            continue;
        }
        if (sel->rule != MergeRule::Replace || sel->srcField != c.column) {
            return fail(CompileErrorKind::KeyExpressionInvalid, "key column '" + c.column + "' must project source field '" + c.column + "' unchanged");
        }
        const size_t selIdx = static_cast<size_t>(std::distance(authored.output.begin(), sel));
        selectedAsKey[selIdx] = true;
        columns.push_back({*sel, resolved[selIdx]});
    }
    for (size_t i = 0; i < authored.output.size(); ++i) {
        if (!selectedAsKey[i]) columns.push_back({authored.output[i], resolved[i]});
    }

    // --- 7. Identity rule: a projection must keep the source record identifiable. ---
    if (!aggregate && !authored.unique) {
        const bool hasSourceKey = std::any_of(components.begin(), components.end(), [&](const KeyComponent& c) {
            const rune::codec::FieldDescriptor* fd = srcDesc.find(c.column);
            return c.keyFunction == rune::codec::kKeyFunctionNone && fd->id == keyFieldIds[0];
        });
        if (!hasSourceKey) {
            return fail(CompileErrorKind::KeyExpressionInvalid, "key does not identify the source record; declare the view unique or include " +
                                                                authored.sources[0].keyField);
        }
    }

    // --- 8. Constraints: structural checks only; each Column node resolves to an output slot. ---
    std::vector<CompiledConstraint> constraints;
    for (size_t i = 0; i < authored.constraints.size(); ++i) {
        const AuthoredConstraint& ac = authored.constraints[i];
        const auto refuse = [&](const std::string& why) {
            return fail(CompileErrorKind::ConstraintInvalid, "constraint '" + ac.name + "': " + why);
        };
        if (ac.name.empty()) return refuse("needs a name");
        for (size_t j = 0; j < i; ++j) {
            if (authored.constraints[j].name == ac.name) return refuse("duplicate constraint name");
        }
        if (ac.predicate.empty()) return refuse("the predicate is empty");

        CompiledConstraint cc{static_cast<uint16_t>(i + 1), ac.name, {}};
        std::vector<rune::codec::PrimType> nodeType(ac.predicate.size());
        for (size_t n = 0; n < ac.predicate.size(); ++n) {
            const PredicateNode& node = ac.predicate[n];
            CompiledPredicateNode cn;
            cn.op = node.op;
            cn.a = node.a;
            cn.b = node.b;
            cn.constType = node.constType;
            cn.constBytes = node.constBytes;
            switch (node.op) {
                case PredicateOp::Column: {
                    const auto col = std::find_if(columns.begin(), columns.end(), [&](const OutputColumn& oc) { return oc.af.name == node.column; });
                    if (col == columns.end()) return refuse("'" + node.column + "' is not a column of the view");
                    cn.outSlot = static_cast<uint16_t>(std::distance(columns.begin(), col));
                    nodeType[n] = col->af.type;
                    break;
                }
                case PredicateOp::Const:
                    nodeType[n] = node.constType;
                    break;
                case PredicateOp::Not:
                    if (node.a >= n) return refuse("node " + std::to_string(n) + " references a later node");
                    break;
                case PredicateOp::And: case PredicateOp::Or:
                    if (node.a >= n || node.b >= n) return refuse("node " + std::to_string(n) + " references a later node");
                    break;
                default: {   // Eq, Ne, Lt, Le, Gt, Ge
                    if (node.a >= n || node.b >= n) return refuse("node " + std::to_string(n) + " references a later node");
                    const PredicateOp opA = ac.predicate[node.a].op, opB = ac.predicate[node.b].op;
                    const bool columnAndConst = (opA == PredicateOp::Column && opB == PredicateOp::Const) ||
                                                (opA == PredicateOp::Const && opB == PredicateOp::Column);
                    if (!columnAndConst) return refuse("a comparison needs a column and a constant");
                    if (nodeType[node.a] != nodeType[node.b]) return refuse("the constant's type does not match the column's type");
                    break;
                }
            }
            cc.predicate.push_back(cn);
        }
        constraints.push_back(std::move(cc));
    }

    // --- 9. Build the output Descriptor + CompiledField list in that order. ---
    rune::codec::DescriptorBuilder builder(authored.viewName, 1);
    std::vector<CompiledField> compiledOutput;
    compiledOutput.reserve(columns.size());

    for (size_t slot = 0; slot < columns.size(); ++slot) {
        const AuthoredField& af        = columns[slot].af;
        const ResolvedOutputField& rof = columns[slot].rof;
        const bool isKeyField          = slot < components.size();

        if (af.type == rune::codec::PrimType::FixedStr) {
            try {
                builder.addFixedStr(af.name, rof.fixedLen, /*sinceVersion*/ 1, isKeyField);
            } catch (const std::exception& e) {   // zero-width FixedStr (#667): a DDL refusal, never a throw
                return std::unexpected(CompileError{
                    "view '" + authored.viewName + "': failed to build output schema: " + e.what()});
            }
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
    for (size_t i = 0; i < components.size(); ++i) {
        plan.keyComponents.push_back({keys[i].fd->id, components[i].keyFunction, static_cast<uint16_t>(keys[i].width),
                                      static_cast<uint16_t>(i), keys[i].fd->type});
    }
    plan.keyWidth           = static_cast<uint16_t>(keyWidth);
    plan.unique             = authored.unique;
    plan.constraints        = std::move(constraints);
    plan.unicodeVersion     = authored.unicodeVersion;

    CompileResult result;
    result.plan             = std::move(plan);
    result.outputDescriptor = std::move(outputDescriptor);
    result.rowLayout        = std::move(rowLayout);
    return result;
}

} // namespace rune::view
