// libs/view/src/CheckEval.cpp
#include <rune/view/CheckEval.h>

#include <array>
#include <cmath>
#include <cstring>

namespace rune::view {

namespace {

using rune::codec::FieldValue;
using rune::codec::PrimType;

constexpr int kUnordered = 2;

int three(bool less, bool greater) noexcept { return less ? -1 : (greater ? 1 : 0); }

// -1 / 0 / 1 by the column type's order; kUnordered when a Float64 operand is not finite.
int compareValues(PrimType type, const FieldValue& x, const FieldValue& y) noexcept {
    switch (type) {
        case PrimType::Int32:
        case PrimType::Int64: return three(x.i < y.i, x.i > y.i);
        case PrimType::UInt16:
        case PrimType::UInt32:
        case PrimType::UInt64:
        case PrimType::Enum: return three(x.u < y.u, x.u > y.u);
        case PrimType::Bool: return three(x.b < y.b, x.b > y.b);
        case PrimType::Float64:
            if (!std::isfinite(x.f) || !std::isfinite(y.f)) return kUnordered;
            return three(x.f < y.f, x.f > y.f);
        case PrimType::FixedStr: {
            const int c = std::memcmp(x.bytes.data(), y.bytes.data(), x.bytes.size());
            return three(c < 0, c > 0);
        }
        case PrimType::String:
        case PrimType::Bytes: break;
    }
    return kUnordered;
}

PredicateResult compare(PredicateOp op, int c) noexcept {
    if (c == kUnordered) return PredicateResult::Unknown;
    bool r = false;
    switch (op) {
        case PredicateOp::Eq: r = c == 0; break;
        case PredicateOp::Ne: r = c != 0; break;
        case PredicateOp::Lt: r = c < 0; break;
        case PredicateOp::Le: r = c <= 0; break;
        case PredicateOp::Gt: r = c > 0; break;
        case PredicateOp::Ge: r = c >= 0; break;
        default: break;
    }
    return r ? PredicateResult::True : PredicateResult::False;
}

} // namespace

PredicateResult evaluatePredicate(std::span<const CompiledPredicateNode> nodes, const RowLayout& layout,
                                  const uint8_t* row) noexcept {
    std::array<PredicateResult, kMaxPredicateNodes> result;
    for (std::size_t i = 0; i < nodes.size(); ++i) {
        const CompiledPredicateNode& n = nodes[i];
        switch (n.op) {
            case PredicateOp::Column:
            case PredicateOp::Const:
                result[i] = PredicateResult::Unknown;
                break;
            case PredicateOp::Not:
                result[i] = result[n.a] == PredicateResult::Unknown ? PredicateResult::Unknown
                          : result[n.a] == PredicateResult::True    ? PredicateResult::False
                                                                    : PredicateResult::True;
                break;
            case PredicateOp::And: {
                const PredicateResult x = result[n.a], y = result[n.b];
                result[i] = x == PredicateResult::False || y == PredicateResult::False ? PredicateResult::False
                          : x == PredicateResult::Unknown || y == PredicateResult::Unknown ? PredicateResult::Unknown
                                                                                          : PredicateResult::True;
                break;
            }
            case PredicateOp::Or: {
                const PredicateResult x = result[n.a], y = result[n.b];
                result[i] = x == PredicateResult::True || y == PredicateResult::True ? PredicateResult::True
                          : x == PredicateResult::Unknown || y == PredicateResult::Unknown ? PredicateResult::Unknown
                                                                                          : PredicateResult::False;
                break;
            }
            default: {   // Eq, Ne, Lt, Le, Gt, Ge: one Column and one Const child, in either order
                const CompiledPredicateNode& l = nodes[n.a];
                const CompiledPredicateNode& r = nodes[n.b];
                const CompiledPredicateNode& col = l.op == PredicateOp::Column ? l : r;
                const CompiledPredicateNode& cst = l.op == PredicateOp::Column ? r : l;
                if (isNull(row, layout, col.outSlot)) {
                    result[i] = PredicateResult::Unknown;
                    break;
                }
                const FieldValue c = readSlot(row, layout, col.outSlot);
                const FieldValue k = decodeSlotValue(cst.constBytes.data(), cst.constType, layout.slotWidth[col.outSlot]);
                result[i] = compare(n.op, l.op == PredicateOp::Column ? compareValues(c.type, c, k) : compareValues(c.type, k, c));
                break;
            }
        }
    }
    return result[nodes.size() - 1];
}

bool predicateReadsChanged(std::span<const CompiledPredicateNode> nodes, const RowLayout& layout, const uint8_t* before,
                           const uint8_t* after) noexcept {
    if (before == nullptr) return true;
    for (const CompiledPredicateNode& n : nodes) {
        if (n.op != PredicateOp::Column) continue;
        const bool nullBefore = isNull(before, layout, n.outSlot);
        if (nullBefore != isNull(after, layout, n.outSlot)) return true;
        if (!nullBefore && std::memcmp(before + layout.slotOffset[n.outSlot], after + layout.slotOffset[n.outSlot],
                                       layout.slotWidth[n.outSlot]) != 0) {
            return true;
        }
    }
    return false;
}

std::optional<std::string> constraintShapeError(const CompiledConstraint& c, const RowLayout& layout,
                                                const rune::codec::Descriptor& output) {
    const std::string prefix = "constraint '" + c.name + "': ";
    if (c.predicate.size() > kMaxPredicateNodes) return prefix + "at most " + std::to_string(kMaxPredicateNodes) + " predicate nodes";
    for (const CompiledPredicateNode& n : c.predicate) {
        if (n.op == PredicateOp::Column && layout.slotWidth[n.outSlot] > 16) {
            return prefix + "check compares fixed-width columns of at most 16 bytes; '" + output.fields[n.outSlot].name +
                   "' is string(" + std::to_string(layout.slotWidth[n.outSlot]) + ")";
        }
    }
    return std::nullopt;
}

} // namespace rune::view
