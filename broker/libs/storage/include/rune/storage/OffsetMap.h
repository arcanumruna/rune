#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <rune/storage/ProducerTails.h>
#include <rune/storage/TxnCleanState.h>

namespace rune::storage {

using Offset = int64_t;

/// Key → latest-offset dedup map.
///
/// Mirrors Kafka SkimpyOffsetMap semantics:
///   put(key, offset)  — record that `key` was last seen at `offset`.
///                       Overwrites only if the new offset is LARGER.
///   get(key)          — returns the latest offset for the key, if present.
///   size()            — number of distinct keys tracked.
///   clear()           — reset the map.
///
/// Used by LogCleaner.buildOffsetMap() to build a key→latest-offset view
/// over the "dirty" (not-yet-compacted) portion of the log.
class OffsetMap {
public:
    OffsetMap() = default;

    /// Record that `key` was last written at `offset`.
    /// If `key` is already present, only updates if offset > existing.
    void put(std::string_view key, Offset offset);

    /// Return the latest offset for `key`, or std::nullopt if absent.
    std::optional<Offset> get(std::string_view key) const;

    /// Number of distinct keys in the map.
    std::size_t size() const noexcept { return map_.size(); }

    /// Clear all entries.
    void clear() noexcept { map_.clear(); tails_.clear(); txn_.clear(); }

    /// The last data batch of every producer in the scanned range (retained by
    /// compaction — see ProducerTails.h).  Filled by buildOffsetMapFromBases.
    ProducerTails&       producerTails() noexcept { return tails_; }
    const ProducerTails& producerTails() const noexcept { return tails_; }
    /// D3b: the pass's transaction state (aborted ranges, LSO, removable
    /// markers — see TxnCleanState.h).  Filled by buildOffsetMapFromBases.
    TxnCleanState&       txnState() noexcept { return txn_; }
    const TxnCleanState& txnState() const noexcept { return txn_; }

private:
    std::unordered_map<std::string, Offset> map_;
    ProducerTails tails_;
    TxnCleanState txn_;
};

} // namespace rune::storage
