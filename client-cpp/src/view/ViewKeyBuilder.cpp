// src/view/ViewKeyBuilder.cpp
#include <rune/client/view/ViewKeyBuilder.h>

#include <rune/codec/Record.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Utf8.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/protocol/PartitionHash.h>

#include <algorithm>
#include <limits>

namespace rune::client::view {

namespace {

using rune::codec::PrimType;

constexpr bool isIntegerType(PrimType t) noexcept {
    return t == PrimType::Int32 || t == PrimType::Int64 || t == PrimType::UInt16 || t == PrimType::UInt32 ||
           t == PrimType::UInt64 || t == PrimType::Enum;
}

constexpr bool isSignedType(PrimType t) noexcept { return t == PrimType::Int32 || t == PrimType::Int64; }

// The largest value an unsigned or signed column of `t` holds.
constexpr uint64_t maxOf(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32:  return std::numeric_limits<int32_t>::max();
        case PrimType::Int64:  return std::numeric_limits<int64_t>::max();
        case PrimType::UInt16: return std::numeric_limits<uint16_t>::max();
        case PrimType::UInt32:
        case PrimType::Enum:   return std::numeric_limits<uint32_t>::max();
        default:               return std::numeric_limits<uint64_t>::max();
    }
}

} // namespace

std::expected<ViewKeyBuilder, std::string> ViewKeyBuilder::fromPlan(const rune::view::ViewPlan& authored,
                                                                    const rune::codec::Descriptor& sourceDescriptor,
                                                                    int32_t sourcePartitions, rune::protocol::Partitioner p) {
    const std::vector<KeyComponent> authoredComponents =
        authored.keyComponents.empty() ? std::vector<KeyComponent>{{authored.key, 0, authored.key}} : authored.keyComponents;

    ViewKeyBuilder b;
    b.sourcePartitions_ = sourcePartitions;
    b.partitioner_      = p;
    uint32_t width = 0;
    for (const KeyComponent& kc : authoredComponents) {
        const rune::codec::FieldDescriptor* fd = sourceDescriptor.find(kc.column);
        if (fd == nullptr) return std::unexpected("key column '" + kc.column + "' is not a field of the source");
        const uint32_t w = rune::codec::viewKeyComponentWidth(fd->type, fd->fixedLen, kc.keyFunction);
        if (w == 0) return std::unexpected("key column '" + kc.column + "' cannot be a view key component");
        width += w;
        b.components_.push_back({kc.column, kc.outName, fd->type, static_cast<uint16_t>(rune::codec::fieldWidth(*fd)),
                                 static_cast<uint16_t>(w), kc.keyFunction});
    }
    if (width > rune::codec::kMaxViewKeyBytes) return std::unexpected("the view key is wider than 128 bytes");
    b.keyWidth_ = static_cast<uint16_t>(width);
    b.values_.resize(b.components_.size());
    if (b.components_.size() == 1 && b.components_[0].keyFunction == rune::codec::kKeyFunctionNone && !authored.sources.empty() &&
        b.components_[0].column == authored.sources[0].keyField) {
        b.sourceKeyed_   = true;
        b.wireKeyWidth_ = b.components_[0].srcWidth;
    }
    return b;
}

std::expected<ViewKeyBuilder, std::string> ViewKeyBuilder::fromCatalog(const rune::catalog::CatalogState& s, std::string_view viewName) {
    const rune::catalog::ViewObject* view = s.viewByName(0, viewName);
    if (view == nullptr) return std::unexpected("no view named '" + std::string(viewName) + "'");
    const auto plan = rune::view::deserializeAuthored(view->def.normalizedPlanBlob);
    if (!plan) return std::unexpected("view '" + std::string(viewName) + "': its plan does not decode");
    if (view->def.sourceNames.empty()) return std::unexpected("view '" + std::string(viewName) + "' has no source");
    const rune::catalog::TopicObject* src = s.topicByName(view->nsId, view->def.sourceNames[0]);
    if (src == nullptr || !src->schema) return std::unexpected("the source of view '" + std::string(viewName) + "' has no schema");
    rune::codec::Descriptor descriptor;
    try {
        descriptor = rune::codec::deserialize(src->schema->evolvingBlob);
    } catch (const std::exception& e) {
        return std::unexpected("the source schema of view '" + std::string(viewName) + "' does not decode: " + e.what());
    }
    return fromPlan(*plan, descriptor, src->partitions, src->schema->keyFeatures.partitioner);
}

ViewKeyBuilder::Value* ViewKeyBuilder::slot_(std::string_view column) {
    for (std::size_t i = 0; i < components_.size(); ++i) {
        if (components_[i].column == column || components_[i].outName == column) return &values_[i];
    }
    if (error_.empty()) error_ = "'" + std::string(column) + "' is not a key column of the view";
    return nullptr;
}

ViewKeyBuilder& ViewKeyBuilder::setInt(std::string_view column, int64_t v) {
    if (Value* s = slot_(column)) { s->kind = Kind::Int; s->i = v; }
    return *this;
}

ViewKeyBuilder& ViewKeyBuilder::setUInt(std::string_view column, uint64_t v) {
    if (Value* s = slot_(column)) { s->kind = Kind::UInt; s->u = v; }
    return *this;
}

ViewKeyBuilder& ViewKeyBuilder::setBool(std::string_view column, bool v) {
    if (Value* s = slot_(column)) { s->kind = Kind::Bool; s->u = v ? 1 : 0; }
    return *this;
}

ViewKeyBuilder& ViewKeyBuilder::setString(std::string_view column, std::string_view utf8) {
    if (Value* s = slot_(column)) { s->kind = Kind::Str; s->s.assign(utf8); }
    return *this;
}

std::expected<void, std::string> ViewKeyBuilder::encode_(std::size_t idx, std::span<uint8_t> out) const {
    const Component& c = components_[idx];
    const Value& v     = values_[idx];
    rune::codec::FieldValue fv;
    fv.type    = c.type;
    fv.present = true;
    const auto bad = [&](std::string_view why) { return std::unexpected(c.outName + " " + std::string(why)); };

    if (isIntegerType(c.type)) {
        if (v.kind == Kind::Int) {
            const bool fits = isSignedType(c.type) ? (c.type == PrimType::Int64 || (v.i >= std::numeric_limits<int32_t>::min() && v.i <= std::numeric_limits<int32_t>::max()))
                                                    : (v.i >= 0 && static_cast<uint64_t>(v.i) <= maxOf(c.type));
            if (!fits) return bad("is out of range: " + std::to_string(v.i));
            fv.i = v.i;
            fv.u = static_cast<uint64_t>(v.i);
        } else if (v.kind == Kind::UInt) {
            if (v.u > maxOf(c.type)) return bad("is out of range: " + std::to_string(v.u));
            fv.i = static_cast<int64_t>(v.u);
            fv.u = v.u;
        } else {
            return bad("is an integer column");
        }
    } else if (c.type == PrimType::Bool) {
        if (v.kind != Kind::Bool) return bad("is a bool column");
        fv.b = v.u != 0;
    } else {   // FixedStr
        if (v.kind != Kind::Str) return bad("is a string column");
        if (v.s.size() > c.srcWidth) return bad("is longer than " + std::to_string(c.srcWidth) + " bytes");
        const std::span<const uint8_t> bytes(reinterpret_cast<const uint8_t*>(v.s.data()), v.s.size());
        if (!rune::codec::validateUtf8(bytes).ok()) return bad("is not valid UTF-8");
        fv.bytes = v.s;
    }
    if (!rune::codec::encodeKeyComponent(c.type, c.keyFunction, fv, out)) return bad("cannot be encoded");
    return {};
}

std::expected<std::vector<uint8_t>, std::string> ViewKeyBuilder::build_(std::size_t count) const {
    std::vector<uint8_t> out;
    out.reserve(keyWidth_);
    for (std::size_t i = 0; i < count; ++i) {
        out.resize(out.size() + components_[i].width);
        if (auto ok = encode_(i, std::span<uint8_t>(out).last(components_[i].width)); !ok) return std::unexpected(std::move(ok.error()));
    }
    return out;
}

std::expected<std::vector<uint8_t>, std::string> ViewKeyBuilder::key() const {
    if (!error_.empty()) return std::unexpected(error_);
    for (std::size_t i = 0; i < components_.size(); ++i) {
        if (values_[i].kind == Kind::Unset) return std::unexpected(components_[i].outName + " is not set");
    }
    return build_(components_.size());
}

std::expected<std::vector<uint8_t>, std::string> ViewKeyBuilder::prefix() const {
    if (!error_.empty()) return std::unexpected(error_);
    std::size_t set = 0;
    while (set < components_.size() && values_[set].kind != Kind::Unset) ++set;
    if (set == 0) return std::unexpected(components_[0].outName + " is not set");
    if (set == components_.size()) return std::unexpected("every component is set: use key()");
    for (std::size_t i = set + 1; i < components_.size(); ++i) {
        if (values_[i].kind != Kind::Unset) return std::unexpected(components_[set].outName + " is not set but " + components_[i].outName + " is");
    }
    return build_(set);
}

int32_t ViewKeyBuilder::partition() const noexcept {
    if (sourcePartitions_ <= 1 || !sourceKeyed_ || values_[0].kind == Kind::Unset) return -1;
    // The wire key the producer routes: the column's bytes, little-endian at its width; a string's N bytes, zero padded.
    uint8_t wire[128] = {};
    const Component& c = components_[0];
    const Value& v     = values_[0];
    if (c.type == PrimType::FixedStr) {
        if (v.kind != Kind::Str || v.s.size() > c.srcWidth) return -1;
        std::copy(v.s.begin(), v.s.end(), wire);
    } else {
        const uint64_t raw = v.kind == Kind::Int ? static_cast<uint64_t>(v.i) : v.u;
        for (uint16_t k = 0; k < wireKeyWidth_ && k < 8; ++k) wire[k] = static_cast<uint8_t>(raw >> (8 * k));
    }
    return rune::protocol::partitionFor(std::span<const uint8_t>(wire, wireKeyWidth_), sourcePartitions_, partitioner_);
}

void ViewKeyBuilder::clear() noexcept {
    for (Value& v : values_) v = Value{};
    error_.clear();
}

} // namespace rune::client::view
