// include/rune/client/view/ViewKeyBuilder.h
//
// Builds the key bytes of a wide view key - the ones getViewRowK() / scanView() take - from typed component values.
// The bytes come from rune::codec::encodeKeyComponent, the function the broker folds a view's key with, so a key built
// here is byte-identical to the broker's.  partition() routes the source-keyed component with the producer's
// rune::protocol::partitionFor, the partition ViewReadOptions::partition wants on a multi-partition source.
#pragma once

#include <rune/catalog/CatalogState.h>
#include <rune/client/view/ViewPlanAuthor.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/PrimType.h>
#include <rune/protocol/CatalogTypes.h>

#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace rune::client::view {

class ViewKeyBuilder {
public:
    struct Component {
        std::string           column;       // the source column
        std::string           outName;      // the name it carries in the view
        rune::codec::PrimType type;         // the source column's type
        uint16_t              srcWidth;     // the source column's width in bytes
        uint16_t              width;        // bytes it occupies in the key
        uint8_t               keyFunction;  // rune::codec::kKeyFunction*
    };

    /// `authored` is the plan the view was created with, `sourceDescriptor` its first source's schema.
    static std::expected<ViewKeyBuilder, std::string> fromPlan(const rune::view::ViewPlan& authored,
                                                               const rune::codec::Descriptor& sourceDescriptor,
                                                               int32_t sourcePartitions = 1,
                                                               rune::protocol::Partitioner p = {});
    /// The view `viewName` (default namespace) of `s`: its plan, its first source's schema, partition count and partitioner.
    static std::expected<ViewKeyBuilder, std::string> fromCatalog(const rune::catalog::CatalogState& s, std::string_view viewName);

    ViewKeyBuilder& setInt(std::string_view column, int64_t v);   // any integer component; range-checked in key()
    ViewKeyBuilder& setUInt(std::string_view column, uint64_t v);
    ViewKeyBuilder& setBool(std::string_view column, bool v);
    ViewKeyBuilder& setString(std::string_view column, std::string_view utf8);   // string(N); a lower() component folds it in key()

    /// Every component set.  An error names the component: unset, out of range, longer than N bytes, invalid UTF-8.
    [[nodiscard]] std::expected<std::vector<uint8_t>, std::string> key() const;
    /// The leading components set, the rest not: a scan prefix.
    [[nodiscard]] std::expected<std::vector<uint8_t>, std::string> prefix() const;
    /// -1 for a 1-partition source or a key not routed by one source column; else the producer's routing of the wire key.
    [[nodiscard]] int32_t partition() const noexcept;
    [[nodiscard]] uint16_t keyWidth() const noexcept { return keyWidth_; }
    [[nodiscard]] std::span<const Component> components() const noexcept { return components_; }
    void clear() noexcept;

private:
    enum class Kind : uint8_t { Unset, Int, UInt, Bool, Str };
    struct Value {
        Kind        kind = Kind::Unset;
        int64_t     i    = 0;
        uint64_t    u    = 0;
        std::string s;
    };

    Value* slot_(std::string_view column);
    [[nodiscard]] std::expected<void, std::string> encode_(std::size_t idx, std::span<uint8_t> out) const;
    [[nodiscard]] std::expected<std::vector<uint8_t>, std::string> build_(std::size_t count) const;

    std::vector<Component>        components_;
    std::vector<Value>            values_;
    uint16_t                      keyWidth_ = 0;
    int32_t                       sourcePartitions_ = 1;
    rune::protocol::Partitioner   partitioner_{};
    bool                          sourceKeyed_ = false;   // one component, the source's key column, no key function
    uint16_t                      wireKeyWidth_ = 0;      // the source key's width: what the producer routes
    std::string                   error_;                 // the first set*() that named no component
};

} // namespace rune::client::view
