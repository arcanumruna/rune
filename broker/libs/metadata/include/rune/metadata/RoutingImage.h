// RoutingImage.h — immutable, zero-alloc, O(1) partition→leader lookup.
//
// Design
// ──────
// The image is a contiguous flat array of Entry, indexed directly by a
// "global partition id" (gpid).  The caller supplies a gpid-assignment function
// when building from an AssignmentMap; for the direct build() overload the
// BuildEntry carries the gpid directly.
//
// Id scheme
// ─────────
// The RoutingImage is agnostic to how gpids are assigned; that is the
// responsibility of the control-plane builder.  A typical scheme:
//
//   gpid = topicId * partitionsPerTopic + partition   (dense, contiguous)
//
// The image stores entries for gpid in [0, maxGpid].  Gaps (unassigned gpids)
// are filled with the NO_LEADER sentinel {-1, -1}.
//
// Publish / read discipline (Aeron / Kafka MetadataCache pattern)
// ───────────────────────────────────────────────────────────────
// - The current image is held in a RoutingImageHolder as:
//     std::atomic<std::shared_ptr<const RoutingImage>>
// - Publisher: builds a new image, calls holder.publish(img) — release-store.
// - Reader: calls holder.current() — acquire-load — captures a local
//   shared_ptr once and uses it for the entire call.  The old image stays
//   alive as long as any reader holds a shared_ptr to it.
// - This is a single-writer / many-reader pattern.  Multiple concurrent
//   readers are safe.  Multiple concurrent writers are NOT (use external
//   locking or a single control-plane writer).
//
// Hot-path guarantees
// ───────────────────
// lookup() is:
//   - noexcept
//   - O(1) — direct array index
//   - zero-allocation — no heap operation after construction
//   - returns a const reference into the immutable flat array

#pragma once

#include "rune/metadata/AssignmentMap.h"
#include "rune/metadata/PartitionAssignment.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace rune::metadata {

// ─────────────────────────────────────────────────────────────────────────────
// RoutingImage — immutable after construction
// ─────────────────────────────────────────────────────────────────────────────

class RoutingImage {
public:
    // ── Sentinel values ──────────────────────────────────────────────────────

    static constexpr int32_t NO_LEADER_NODE_ID = -1;
    static constexpr int32_t NO_LEADER_EPOCH   = -1;

    // ── Entry (the hot-path data) ─────────────────────────────────────────────

    struct Entry {
        int32_t leaderNodeId;  ///< Leader node id, or NO_LEADER_NODE_ID (-1).
        int32_t leaderEpoch;   ///< Leader epoch, or NO_LEADER_EPOCH (-1).
    };

    // ── Build entry (control-plane input) ────────────────────────────────────

    struct BuildEntry {
        int32_t globalPartitionId;
        int32_t leaderNodeId;
        int32_t leaderEpoch;
    };

    // ── Factory: build from a list of BuildEntry ─────────────────────────────

    /// Construct an immutable RoutingImage from a list of {gpid, leader, epoch}
    /// entries.  May allocate (called on the control plane, not the hot path).
    ///
    /// Entries with the same gpid: last write wins (undefined order).
    /// Gaps in [0, maxGpid] are filled with the NO_LEADER sentinel.
    static std::shared_ptr<const RoutingImage>
    build(const std::vector<BuildEntry>& entries)
    {
        // Control-plane allocation — shared_ptr construction is fine here.
        return std::shared_ptr<RoutingImage>(new RoutingImage(entries));
    }

    // ── Factory: build from AssignmentMap ────────────────────────────────────

    /// Build from an AssignmentMap with a caller-supplied gpid-assignment
    /// function:  gpid = gpidOf(topicId, partition).
    ///
    /// The function must return a non-negative int32_t for every live entry.
    /// A negative gpid (buggy gpidOf) is defensively skipped — never used to
    /// index the flat array — so a control-plane bug cannot cause an
    /// out-of-bounds write or a pathological allocation.  The assert fires in
    /// debug builds to surface the contract violation early.
    template<typename GpidFn>
    static std::shared_ptr<const RoutingImage>
    buildFromMap(const AssignmentMap& amap, GpidFn&& gpidOf)
    {
        std::vector<BuildEntry> entries;
        amap.forEach([&](int32_t topicId, int32_t partition,
                         const PartitionAssignment& pa)
        {
            const int32_t gpid = gpidOf(topicId, partition);
            assert(gpid >= 0 && "gpidOf must return a non-negative gpid");
            if (gpid < 0) return;  // release-safe: skip rather than corrupt
            entries.push_back(BuildEntry{gpid, pa.leaderNode, pa.leaderEpoch});
        });
        return build(entries);
    }

    // ── Hot-path lookup ───────────────────────────────────────────────────────

    /// Look up globalPartitionId.
    /// O(1), noexcept, zero-allocation.
    /// Returns the NO_LEADER sentinel if gpid is out of range or unassigned.
    [[nodiscard]]
    const Entry& lookup(int32_t globalPartitionId) const noexcept
    {
        // Unsigned cast: negative gpid maps to a large value → out-of-range.
        const auto idx = static_cast<uint32_t>(globalPartitionId);
        if (idx >= static_cast<uint32_t>(entries_.size())) {
            return kSentinel_;
        }
        return entries_[idx];
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// Number of slots in the flat array (maxGpid + 1).
    int32_t capacity() const noexcept
    {
        return static_cast<int32_t>(entries_.size());
    }

    // ── Non-copyable, non-movable (immutable value object) ───────────────────

    RoutingImage(const RoutingImage&)            = delete;
    RoutingImage& operator=(const RoutingImage&) = delete;
    RoutingImage(RoutingImage&&)                 = delete;
    RoutingImage& operator=(RoutingImage&&)      = delete;

private:
    // Private constructor — use build() factory.
    explicit RoutingImage(const std::vector<BuildEntry>& entries)
    {
        if (entries.empty()) return;

        // Determine required array size (maxGpid + 1).
        // Negative gpids are defensively ignored (never used to size or index
        // the array) so a buggy caller cannot trigger an out-of-bounds write
        // or a pathological allocation in a release build.
        int32_t maxGpid = -1;
        for (const auto& e : entries) {
            assert(e.globalPartitionId >= 0 && "gpid must be non-negative");
            if (e.globalPartitionId > maxGpid) {
                maxGpid = e.globalPartitionId;
            }
        }
        if (maxGpid < 0) return;  // no valid (non-negative) gpids

        // Fill with sentinel, then overwrite assigned slots.
        entries_.assign(static_cast<std::size_t>(maxGpid + 1),
                        Entry{NO_LEADER_NODE_ID, NO_LEADER_EPOCH});
        for (const auto& e : entries) {
            if (e.globalPartitionId < 0) continue;  // release-safe skip
            entries_[static_cast<std::size_t>(e.globalPartitionId)] =
                Entry{e.leaderNodeId, e.leaderEpoch};
        }
    }

    // Contiguous flat array: entries_[gpid] = {leaderNodeId, leaderEpoch}.
    std::vector<Entry> entries_;

    // Static sentinel returned for all out-of-range / unassigned lookups.
    // Its address is stable (static storage), so lookup() can return a
    // const reference without allocation.
    static constexpr Entry kSentinel_{NO_LEADER_NODE_ID, NO_LEADER_EPOCH};
};

// ─────────────────────────────────────────────────────────────────────────────
// RoutingImageHolder — publish/read with acquire/release memory ordering
//
// Single-writer / many-reader.
// Publisher:  holder.publish(newImage)  — release-store
// Reader:     auto snap = holder.current() — acquire-load; use snap for the
//             entire call; do NOT call current() again within the same call.
//
// Implementation note
// ───────────────────
// We use std::atomic_load_explicit / std::atomic_store_explicit on a
// std::shared_ptr, which are available in C++11/14/17/20/23 without
// requiring std::atomic<std::shared_ptr<T>> (C++20 specialisation, not yet
// available on Apple libc++ as of Xcode 17 / macOS SDK).
//
// The free-function atomic ops on shared_ptr use the same acquire/release
// discipline as the C++20 specialisation: store with release ordering
// ensures the pointee's writes are visible to any load with acquire ordering.
// ─────────────────────────────────────────────────────────────────────────────

class RoutingImageHolder {
public:
    RoutingImageHolder() = default;

    // Non-copyable/movable — the holder is owned by the control plane.
    RoutingImageHolder(const RoutingImageHolder&)            = delete;
    RoutingImageHolder& operator=(const RoutingImageHolder&) = delete;
    RoutingImageHolder(RoutingImageHolder&&)                 = delete;
    RoutingImageHolder& operator=(RoutingImageHolder&&)      = delete;

    /// Publish a new image.  Called by the single control-plane writer.
    /// Release-store: all writes to *img are visible to any thread that
    /// observes the new pointer via current().
    void publish(std::shared_ptr<const RoutingImage> img) noexcept
    {
        // atomic_store_explicit with release — mirrors Aeron counter release fence.
        std::atomic_store_explicit(&current_, std::move(img),
                                   std::memory_order_release);
    }

    /// Acquire-load the current image.
    /// Readers must capture the result into a local shared_ptr and use that
    /// snapshot for the duration of the call — do NOT call current() again.
    /// Returns nullptr if no image has been published yet.
    [[nodiscard]]
    std::shared_ptr<const RoutingImage> current() const noexcept
    {
        // atomic_load_explicit with acquire — mirrors Aeron counter acquire fence.
        return std::atomic_load_explicit(&current_, std::memory_order_acquire);
    }

private:
    // Guarded by std::atomic_load/store_explicit (free-function atomic ops
    // on shared_ptr from <memory>).
    std::shared_ptr<const RoutingImage> current_{nullptr};
};

} // namespace rune::metadata
