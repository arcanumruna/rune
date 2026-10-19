#pragma once
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::node {

/// One view as the ViewRegistry holds it — the CatalogApplier's projection of a
/// ViewObject (catalog history Task 16: the retired `__views` wire record and its
/// serialize / deserialize are gone — `__catalog` is the catalog).  The view's
/// OUTPUT descriptor rides along so the typed output topic can be registered in the
/// SchemaRegistry (GetTopicSchema / openView) exactly as created: the compiled plan
/// drops field names + FixedStr widths, so the descriptor (and its fingerprint)
/// cannot be reconstructed from compiledPlanBlob alone.
struct ViewRecord {
    int32_t              viewTopicId = -1;
    std::string           viewName;
    std::vector<uint8_t>  compiledPlanBlob;
    uint8_t               persistence = 0;
    std::vector<uint8_t>  outputDescriptorBlob;  // Task 19b: rune::codec::serialize(outputDescriptor)
};

/// Single-threaded, admin-thread-owned view catalog. No locks, no atomics —
/// caller owns the thread. Mirrors SchemaRegistry's shape/contract exactly.
class ViewRegistry {
public:
    /// Apply (leader-commit or replica-apply) one view record. Overwrites any
    /// existing record for the same viewTopicId (views are not versioned —
    /// unlike TopicSchema — a re-apply replaces the prior definition in place).
    /// If the re-applied record renames the view (viewName differs from the
    /// stored one), the OLD name's byName() mapping is erased so it does not
    /// keep stale-resolving to the renamed slot.
    void applyRecord(const ViewRecord& v);

    /// Look up by view name. Returns nullptr if absent.
    [[nodiscard]] const ViewRecord* byName(std::string_view name) const;

    /// Look up by the view's own (runtime) topic id. Returns nullptr if absent.
    [[nodiscard]] const ViewRecord* byViewTopicId(int32_t viewTopicId) const;

    /// All currently-registered views (stable pointers into the backing store).
    [[nodiscard]] std::vector<const ViewRecord*> all() const;

    /// Catalog history Task 7: forget the view whose sink is `viewTopicId` (a
    /// DropView / AlterView applied).  Its slot is retired in place (the deque
    /// never moves the others — their pointers stay valid); a pointer to the
    /// retired slot then reads an empty record (viewTopicId -1).  Its name stops
    /// resolving unless it already names another id.  Unknown id: no-op.
    bool erase(int32_t viewTopicId);

private:
    // std::deque, NOT std::vector/std::unordered_map<..., ViewRecord>:
    // callers may hold a `const ViewRecord*` from byName/byViewTopicId across
    // a subsequent applyRecord() of a DIFFERENT view. A vector's push_back/
    // resize may reallocate and invalidate every existing element pointer; a
    // deque's growth at the back never invalidates references/pointers to
    // already-existing elements (mirrors SchemaRegistry::byId_'s rationale).
    std::deque<ViewRecord> records_;
    // viewTopicId -> index into records_. Re-applying an existing viewTopicId
    // overwrites that slot in place (records_[idx] = v), which keeps any
    // already-issued pointer to THAT slot valid as a view of the latest
    // definition — same stability guarantee, just applied to the updated slot
    // rather than only to untouched ones.
    std::unordered_map<int32_t, std::size_t> idToIndex_;
    std::unordered_map<std::string, int32_t>  nameToId_;
};

} // namespace rune::node
