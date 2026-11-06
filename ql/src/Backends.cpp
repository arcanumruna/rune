#include "rune/ql/Backends.h"
#include "ConfigKeys.h"
#include <rune/codec/Serialize.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <charconv>
#include <cstring>
#include <stdexcept>
#include <vector>

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

void applySetting(DefineTopicPlan& plan, const ConfigSetting& s) {
    auto& c = plan.config;
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
        case F::KeyIndex:            plan.keyFeatures.keyIndex = s.value != 0; break;
        case F::AutoIncrementReserve: plan.keyFeatures.autoIncrementReserve = static_cast<uint32_t>(s.value); break;
        case F::AutoIncrementMax:    plan.keyFeatures.autoIncrementMax = static_cast<uint64_t>(s.value); break;
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
    for (const auto& f : td.fields) plan.keyFeatures.autoIncrement |= f.autoIncrement;
    for (const auto& s : td.with) applySetting(plan, s);
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
            case AlterUserDef::Change::Disable: p.change = pc::UserChangeKind::Disable; p.force = d->force; break;
            case AlterUserDef::Change::Enable:  p.change = pc::UserChangeKind::Enable; break;
        }
    } else if (const auto* d = std::get_if<DropUserDef>(&st)) {
        p.kind = pc::SecurityOpKind::DropUser; p.name = d->name; p.force = d->force;
    } else if (const auto* d = std::get_if<CreateRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::CreateRole; p.name = d->name;
    } else if (const auto* d = std::get_if<DropRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::DropRole; p.name = d->name; p.force = d->force;
    } else if (const auto* d = std::get_if<GrantDef>(&st)) {
        p.kind = pc::SecurityOpKind::Grant; grantFields(d->op, d->resource, d->grantee, d->granteeName);
    } else if (const auto* d = std::get_if<RevokeDef>(&st)) {
        p.kind = pc::SecurityOpKind::Revoke; grantFields(d->op, d->resource, d->grantee, d->granteeName); p.force = d->force;
    } else if (const auto* d = std::get_if<GrantRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::GrantRole; p.name = d->user; p.roleName = d->role;
    } else if (const auto* d = std::get_if<RevokeRoleDef>(&st)) {
        p.kind = pc::SecurityOpKind::RevokeRole; p.name = d->user; p.roleName = d->role; p.force = d->force;
    } else {
        return std::unexpected(ParseError{0, 0, "not a security DDL statement (SHOW statements are client-side reads)"});
    }
    p.statement = redactedText(st);
    return p;
}

ViewSource viewSourceOf(const TopicDef& td) {
    ViewSource src;
    src.name = td.name;
    src.keyField = td.keyField;
    if (!td.fields.empty()) src.descriptor = toDescriptor(td);
    src.partitions = static_cast<int32_t>(td.partitions);
    src.transient = td.persistence == Persistence::Transient;
    return src;
}

namespace {

using rune::codec::PrimType;
using rune::view::MergeRule;
using rune::view::PredicateNode;
using rune::view::PredicateOp;

struct ColType { PrimType type = PrimType::Int32; uint16_t width = 0; };

std::string typeWord(const ColType& c) {
    switch (c.type) {
        case PrimType::Int32:    return "int32";
        case PrimType::Int64:    return "int64";
        case PrimType::UInt16:   return "uint16";
        case PrimType::UInt32:   return "uint32";
        case PrimType::UInt64:   return "uint64";
        case PrimType::Float64:  return "float64";
        case PrimType::Bool:     return "bool";
        case PrimType::Enum:     return "enum";
        case PrimType::String:   return "string";
        case PrimType::Bytes:    return "bytes";
        case PrimType::FixedStr: return "string(" + std::to_string(c.width) + ")";
    }
    return "unknown";
}

const rune::codec::FieldDescriptor* sourceField(const rune::codec::Descriptor& d, const std::string& name) {
    for (const auto& f : d.fields) {
        if (f.name == name && rune::codec::isLiveAt(f, d.version)) return &f;
    }
    return nullptr;
}

ColType colOf(const rune::codec::FieldDescriptor& f) { return {f.type, f.fixedLen}; }

const char* aggName(ItemKind k) {
    switch (k) {
        case ItemKind::Count: case ItemKind::CountAll: return "count";
        case ItemKind::Sum:  return "sum";
        case ItemKind::Avg:  return "avg";
        case ItemKind::Min:  return "min";
        case ItemKind::Max:  return "max";
        case ItemKind::Last: return "last";
        default:             return "";
    }
}

MergeRule ruleOf(ItemKind k) {
    switch (k) {
        case ItemKind::CountAll: return MergeRule::CountAll;
        case ItemKind::Count:    return MergeRule::Count;
        case ItemKind::Sum:      return MergeRule::Sum;
        case ItemKind::Avg:      return MergeRule::Avg;
        case ItemKind::Min:      return MergeRule::Min;
        case ItemKind::Max:      return MergeRule::Max;
        case ItemKind::Last:     return MergeRule::Last;
        default:                 return MergeRule::Replace;
    }
}

std::string literalText(const Literal& l) {
    switch (l.kind) {
        case Literal::Kind::Integer: return (l.negative && l.magnitude != 0 ? "-" : "") + std::to_string(l.magnitude);
        case Literal::Kind::String:  return "'" + l.text + "'";
        case Literal::Kind::Bool:    return l.boolean ? "true" : "false";
    }
    return {};
}

// The literal in the column's row-slot encoding (P8-5); nullopt = it does not fit.
std::optional<std::array<uint8_t, 16>> constBytesOf(const Literal& l, const ColType& c) {
    std::array<uint8_t, 16> out{};
    switch (c.type) {
        case PrimType::Int32: case PrimType::Int64: case PrimType::UInt16: case PrimType::UInt32:
        case PrimType::UInt64: case PrimType::Enum: {
            if (l.kind != Literal::Kind::Integer) return std::nullopt;
            const uint32_t size = rune::codec::fixedSize(c.type);
            const bool isSigned = c.type == PrimType::Int32 || c.type == PrimType::Int64 || c.type == PrimType::Enum;
            const uint64_t negLimit = uint64_t{1} << (size * 8 - 1);
            if (isSigned) {
                if (l.negative ? l.magnitude > negLimit : l.magnitude >= negLimit) return std::nullopt;
            } else if (l.negative ? l.magnitude != 0 : (size < 8 && l.magnitude >> (size * 8) != 0)) {
                return std::nullopt;
            }
            const uint64_t v = l.negative ? ~l.magnitude + 1 : l.magnitude;
            for (uint32_t i = 0; i < size; ++i) out[i] = static_cast<uint8_t>(v >> (8 * i));
            return out;
        }
        case PrimType::Float64: {
            if (l.kind != Literal::Kind::Integer) return std::nullopt;
            const double d = l.negative ? -static_cast<double>(l.magnitude) : static_cast<double>(l.magnitude);
            std::memcpy(out.data(), &d, sizeof d);
            return out;
        }
        case PrimType::Bool:
            if (l.kind != Literal::Kind::Bool) return std::nullopt;
            out[0] = l.boolean ? 1 : 0;
            return out;
        case PrimType::FixedStr:
            if (l.kind != Literal::Kind::String || l.text.size() > c.width) return std::nullopt;
            std::memcpy(out.data(), l.text.data(), l.text.size());
            return out;
        default: return std::nullopt;
    }
}

std::string constText(const PredicateNode& n) {
    const auto* b = n.constBytes.data();
    auto le = [&](uint32_t size) { uint64_t v = 0; for (uint32_t i = 0; i < size; ++i) v |= uint64_t{b[i]} << (8 * i); return v; };
    switch (n.constType) {
        case PrimType::Int32: case PrimType::Enum: return std::to_string(static_cast<int32_t>(le(4)));
        case PrimType::Int64:    return std::to_string(static_cast<int64_t>(le(8)));
        case PrimType::UInt16:   return std::to_string(le(2));
        case PrimType::UInt32:   return std::to_string(le(4));
        case PrimType::UInt64:   return std::to_string(le(8));
        case PrimType::Bool:     return b[0] ? "true" : "false";
        case PrimType::Float64: {
            double d;
            std::memcpy(&d, b, sizeof d);
            char buf[40];
            const auto r = std::to_chars(buf, buf + sizeof buf, d);
            return std::string(buf, r.ptr);
        }
        case PrimType::FixedStr: {
            std::size_t len = 0;
            while (len < n.constBytes.size() && b[len] != 0) ++len;
            return "'" + std::string(reinterpret_cast<const char*>(b), len) + "'";
        }
        default: return "?";
    }
}

bool isGroup(PredicateOp op) { return op == PredicateOp::And || op == PredicateOp::Or; }

// Children precede their parent (the checked-write predicate wire form), so every child index is below its node's own.
void renderNode(std::span<const PredicateNode> nodes, std::size_t i, std::string& out) {
    const auto& n = nodes[i];
    auto child = [&](std::size_t c, bool parenGroups) {
        const bool paren = parenGroups && isGroup(nodes[c].op);
        if (paren) out += '(';
        renderNode(nodes, c, out);
        if (paren) out += ')';
    };
    switch (n.op) {
        case PredicateOp::Column: out += n.column; return;
        case PredicateOp::Const:  out += constText(n); return;
        case PredicateOp::Not:    out += "not ("; renderNode(nodes, n.a, out); out += ')'; return;
        case PredicateOp::And: case PredicateOp::Or: {
            // A chain of 65535 terms is left-deep: walk its spine instead of recursing down it.
            std::vector<std::size_t> spine{i};
            while (isGroup(nodes[nodes[spine.back()].a].op)) spine.push_back(nodes[spine.back()].a);
            out.append(spine.size() - 1, '(');
            renderNode(nodes, nodes[spine.back()].a, out);
            for (std::size_t k = spine.size(); k-- > 0;) {
                const auto& g = nodes[spine[k]];
                out += g.op == PredicateOp::And ? " and " : " or ";
                child(g.b, true);
                if (k != 0) out += ')';
            }
            return;
        }
        default: break;
    }
    const char* op = "=";
    switch (n.op) {
        case PredicateOp::Ne: op = "<>"; break;
        case PredicateOp::Lt: op = "<"; break;
        case PredicateOp::Le: op = "<="; break;
        case PredicateOp::Gt: op = ">"; break;
        case PredicateOp::Ge: op = ">="; break;
        default: break;
    }
    child(n.a, false); out += ' '; out += op; out += ' '; child(n.b, false);
}

ParseError planError(std::string msg) { return ParseError{0, 0, std::move(msg)}; }

} // namespace

std::string predicateText(std::span<const rune::view::PredicateNode> nodes) {
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        const auto& n = nodes[i];
        if (n.op == PredicateOp::Column || n.op == PredicateOp::Const) continue;
        if (n.a >= i || (n.op != PredicateOp::Not && n.b >= i)) return "<invalid predicate>";
    }
    std::string out;
    if (!nodes.empty()) renderNode(nodes, nodes.size() - 1, out);
    return out;
}

std::expected<rune::view::ViewPlan, ParseError> applyViewPlan(const CreateViewDef& def, const ViewSource& src) {
    using rune::view::KeyComponent;
    const auto& desc = src.descriptor;
    if (desc.fields.empty()) return std::unexpected(planError("view source must be a typed topic"));

    bool star = false, aggregate = false;
    for (const auto& it : def.items) {
        star |= it.kind == ItemKind::Star;
        aggregate |= it.kind >= ItemKind::CountAll;
    }
    if (star && (aggregate || !def.groupBy.empty())) return std::unexpected(planError("'*' is not valid in an aggregate view"));
    if (aggregate && def.groupBy.empty()) return std::unexpected(planError("an aggregate view needs 'group by'"));
    if (!aggregate && !def.groupBy.empty()) return std::unexpected(planError("group by needs an aggregate item"));

    auto missing = [&](const std::string& c) { return planError("column '" + c + "' is not a field of '" + src.name + "'"); };
    for (const auto& k : def.key) if (!sourceField(desc, k.column)) return std::unexpected(missing(k.column));
    for (const auto& it : def.items) {
        if (it.kind != ItemKind::Star && it.kind != ItemKind::CountAll && !sourceField(desc, it.column)) return std::unexpected(missing(it.column));
    }
    for (const auto& g : def.groupBy) if (!sourceField(desc, g.column)) return std::unexpected(missing(g.column));

    rune::view::ViewPlan plan;
    plan.viewName = def.name;
    plan.persistence = def.transient ? 1 : 0;
    plan.unique = def.unique;
    const bool plainKey = def.key.size() == 1 && def.key[0].fn == KeyFunction::None;
    auto component = [](const KeyItem& k) {
        return KeyComponent{k.column, static_cast<uint8_t>(k.fn), k.outName.empty() ? k.column : k.outName};
    };
    if (plainKey) plan.key = def.key[0].column;
    else for (const auto& k : def.key) plan.keyComponents.push_back(component(k));

    bool usesLower = false;
    for (const auto& k : def.key) usesLower |= k.fn == KeyFunction::Lower;
    for (const auto& g : def.groupBy) usesLower |= g.fn == KeyFunction::Lower;
    if (usesLower) plan.unicodeVersion = rune::codec::kUnicodeVersion;

    // A plan that needs no v2 field is the v1 shape: its group column rides as the source key field and `group by` is implied.
    const bool v2 = def.unique || !def.constraints.empty() || usesLower || !plainKey;
    if (!def.groupBy.empty() && (v2 || !(def.groupBy.size() == 1 && def.groupBy[0].fn == KeyFunction::None &&
                                         def.groupBy[0].column == def.key[0].column))) {
        for (const auto& g : def.groupBy) {
            KeyComponent c{g.column, static_cast<uint8_t>(g.fn), g.column};
            for (const auto& k : def.key) {
                if (k.column == g.column && k.fn == g.fn) c.outName = component(k).outName;
            }
            plan.groupBy.push_back(std::move(c));
        }
    }
    plan.sources.push_back({src.topicId, aggregate && !v2 ? def.groupBy[0].column : src.keyField});

    std::vector<ColType> types;   // parallel to plan.output
    auto addReplace = [&](const std::string& name, const rune::codec::FieldDescriptor& f) {
        plan.output.push_back({name, f.type, 0, f.name, MergeRule::Replace});
        types.push_back(colOf(f));
    };
    for (const auto& k : def.key) {
        if (k.fn != KeyFunction::None) continue;
        bool named = star;
        for (const auto& it : def.items) named |= it.kind == ItemKind::Column && it.column == k.column && (it.alias.empty() || it.alias == k.column);
        if (!named) addReplace(k.column, *sourceField(desc, k.column));
    }
    for (const auto& it : def.items) {
        if (it.kind == ItemKind::Star) {
            for (const auto& f : desc.fields) if (rune::codec::isLiveAt(f, desc.version)) addReplace(f.name, f);
        } else if (it.kind == ItemKind::Column) {
            addReplace(it.alias.empty() ? it.column : it.alias, *sourceField(desc, it.column));
        } else if (it.kind == ItemKind::CountAll) {
            plan.output.push_back({it.alias, PrimType::Int64, 0, "", MergeRule::CountAll});
            types.push_back({PrimType::Int64, 0});
        } else {
            const auto& f = *sourceField(desc, it.column);
            const auto rule = ruleOf(it.kind);
            const auto out = rune::view::aggregateOutputType(rule, f.type);
            if (!out) return std::unexpected(planError(std::string(aggName(it.kind)) + "() cannot aggregate '" + it.column + "' of type " + typeWord(colOf(f))));
            plan.output.push_back({it.alias, *out, 0, f.name, rule});
            types.push_back({*out, *out == f.type ? f.fixedLen : uint16_t{0}});
        }
    }

    for (const auto& c : def.constraints) {
        rune::view::AuthoredConstraint ac;
        ac.name = c.name;
        const std::string prefix = "constraint '" + c.name + "': ";
        for (const auto& n : c.predicate) {
            PredicateNode out{static_cast<PredicateOp>(n.op), n.a, n.b, n.column, {}, {}};
            if (n.op == PredOp::Column) {
                bool found = false;
                for (std::size_t i = 0; i < plan.output.size() && !found; ++i) found = plan.output[i].name == n.column;
                if (!found) return std::unexpected(planError(prefix + "column '" + n.column + "' is not a column of the view"));
            }
            ac.predicate.push_back(std::move(out));
        }
        // A comparison's constant takes the compared column's type; the grammar makes it `column cmp literal`.
        for (std::size_t i = 0; i < c.predicate.size(); ++i) {
            const auto& n = c.predicate[i];
            if (n.op < PredOp::Eq || n.op > PredOp::Ge) continue;
            const auto& colNode = ac.predicate[n.a];
            const auto& lit = c.predicate[n.b].value;
            std::size_t col = 0;
            while (plan.output[col].name != colNode.column) ++col;
            const ColType& ct = types[col];
            if (rune::codec::isVarLen(ct.type) || (ct.type == PrimType::FixedStr && ct.width > 16)) {
                return std::unexpected(planError(prefix + "check compares fixed-width columns of at most 16 bytes; '" + colNode.column + "' is " + typeWord(ct)));
            }
            const auto bytes = constBytesOf(lit, ct);
            if (!bytes) return std::unexpected(planError(prefix + literalText(lit) + " does not fit " + typeWord(ct) + " column '" + colNode.column + "'"));
            ac.predicate[n.b].constType = ct.type;
            ac.predicate[n.b].constBytes = *bytes;
        }
        plan.constraints.push_back(std::move(ac));
    }
    return plan;
}

} // namespace rune::ql
