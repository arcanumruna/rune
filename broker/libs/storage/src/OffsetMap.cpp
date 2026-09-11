#include <rune/storage/OffsetMap.h>

namespace rune::storage {

void OffsetMap::put(std::string_view key, Offset offset) {
    auto [it, inserted] = map_.emplace(std::string(key), offset);
    if (!inserted && offset > it->second) {
        it->second = offset;
    }
}

std::optional<Offset> OffsetMap::get(std::string_view key) const {
    auto it = map_.find(std::string(key));
    if (it == map_.end()) return std::nullopt;
    return it->second;
}

} // namespace rune::storage
