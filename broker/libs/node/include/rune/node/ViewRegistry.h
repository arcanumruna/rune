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

/// Wire record stored on the __views internal topic (kViewsTopicId = -5).
/// Mirrors TopicSchema (SchemaRegistry.h): a durable, flat little-endian
/// record that survives restart.
///
/// serialize() LE layout:
///   viewTopicId:i32,
///   viewNameLen:u16 + viewName bytes,
///   compiledPlanLen:u32 + compiledPlanBlob bytes (= Task 8's
///     rune::view::serialize(CompiledViewPlan)),
///   persistence:u8  (0 = persisted, 1 = transient).
struct ViewRecord {
    int32_t              viewTopicId = -1;
    std::string           viewName;
    std::vector<uint8_t>  compiledPlanBlob;
    uint8_t               persistence = 0;

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    [[nodiscard]] static std::optional<ViewRecord> deserialize(std::span<const uint8_t> buf);
};

/// Single-threaded, admin-thread-owned view catalog. No locks, no atomics —
/// caller owns the thread. Mirrors SchemaRegistry's shape/contract exactly.
class ViewRegistry {
public:
    /// Apply (leader-commit or replica-apply) one view record. Overwrites any
    /// existing record for the same viewTopicId (views are not versioned —
    /// unlike TopicSchema — a re-apply replaces the prior definition in place).
    void applyRecord(const ViewRecord& v);

    /// Look up by view name. Returns nullptr if absent.
    [[nodiscard]] const ViewRecord* byName(std::string_view name) const;

    /// Look up by the view's own (runtime) topic id. Returns nullptr if absent.
    [[nodiscard]] const ViewRecord* byViewTopicId(int32_t viewTopicId) const;

    /// Startup replay: clear and re-apply a span of records.
    void rebuildFromRecords(std::span<const ViewRecord> records);

    /// All currently-registered views (stable pointers into the backing store).
    [[nodiscard]] std::vector<const ViewRecord*> all() const;

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
