#include "rune/node/ViewRegistry.h"
#include <cstring>

namespace rune::node {

// ---------------------------------------------------------------------------
// ViewRegistry
// ---------------------------------------------------------------------------
void ViewRegistry::applyRecord(const ViewRecord& v) {
    const auto it = idToIndex_.find(v.viewTopicId);
    if (it != idToIndex_.end()) {
        // Re-apply for an EXISTING viewTopicId: if the incoming record
        // renamed the view (viewName differs from what's currently stored),
        // the OLD name's nameToId_ entry must be erased first — otherwise
        // byName(oldName) would keep resolving to this (now-renamed) slot
        // forever, a stale-mapping hole in the by-name lookup contract.
        // (Phase 1 never actually renames a view, but the registry must not
        // silently corrupt its own index if a future DDL path does.)
        const ViewRecord& existing = records_[it->second];
        if (existing.viewName != v.viewName) {
            const auto staleIt = nameToId_.find(existing.viewName);
            if (staleIt != nameToId_.end() && staleIt->second == v.viewTopicId) {
                nameToId_.erase(staleIt);
            }
        }
        nameToId_[v.viewName] = v.viewTopicId;

        // Overwrite the existing slot in place — records_ (a deque) never
        // moves existing elements on assignment, so any `const ViewRecord*`
        // a caller already holds into this slot now simply observes the
        // updated definition (same stability guarantee as SchemaRegistry's
        // append-only deque, just applied to a replace-in-place update).
        records_[it->second] = v;
        return;
    }

    nameToId_[v.viewName] = v.viewTopicId;
    idToIndex_[v.viewTopicId] = records_.size();
    records_.push_back(v);
}

const ViewRecord* ViewRegistry::byName(std::string_view name) const {
    const auto it = nameToId_.find(std::string(name));
    if (it == nameToId_.end()) return nullptr;
    return byViewTopicId(it->second);
}

const ViewRecord* ViewRegistry::byViewTopicId(int32_t viewTopicId) const {
    const auto it = idToIndex_.find(viewTopicId);
    if (it == idToIndex_.end()) return nullptr;
    return &records_[it->second];
}

std::vector<const ViewRecord*> ViewRegistry::all() const {
    std::vector<const ViewRecord*> out;
    out.reserve(idToIndex_.size());
    for (const auto& v : records_) {
        if (v.viewTopicId < 0) continue;   // a retired (erased) slot
        out.push_back(&v);
    }
    return out;
}

bool ViewRegistry::erase(int32_t viewTopicId) {
    const auto it = idToIndex_.find(viewTopicId);
    if (it == idToIndex_.end()) return false;
    ViewRecord& slot = records_[it->second];
    const auto nameIt = nameToId_.find(slot.viewName);
    if (nameIt != nameToId_.end() && nameIt->second == viewTopicId) nameToId_.erase(nameIt);
    // Retired in place: viewTopicId = -1, no blobs (the blobs' memory is freed).
    // The slot is deliberately NOT reused (fix round 1, M3): a caller may still
    // hold a pointer to it, which must read "gone", never silently alias another
    // view.  One empty ViewRecord per DropView / AlterView for the process life —
    // cold, bounded by the DDL count.
    slot = ViewRecord{};
    idToIndex_.erase(it);
    return true;
}

} // namespace rune::node
