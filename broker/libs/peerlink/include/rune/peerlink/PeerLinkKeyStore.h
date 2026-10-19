#pragma once
// rune/peerlink/PeerLinkKeyStore.h — what ONE owner thread knows about its peer links (authentication plan Task 15).
//
// Every transport owner (the control thread's consensus sink / ingress / legacy replication transport, each worker,
// the admin plane) holds its OWN store, fed over a ring by the control thread's PeerLinkAgent — nothing is shared.
// A new epoch for a peer replaces the current one; the old epoch stays findable for kRetainNs (frames in flight
// under it), then is dropped.  Fixed storage: install / find never allocate.
#include <rune/peerlink/PeerLinkKeys.h>

#include <array>
#include <cstdint>
#include <optional>

namespace rune::peerlink {

class PeerLinkKeyStore {
public:
    static constexpr int64_t     kRetainNs = 2'000'000'000;   // the old epoch's grace period (D33)
    static constexpr std::size_t kMaxPeers = 16;              // == FollowerSlots::kMaxReplicas

    /// A link for `peer` was established at epoch keys.epoch.  False only when the table is full (cold: configuration
    /// bounds the cluster size well below kMaxPeers).
    bool install(int32_t peer, const LinkKeys& keys, int64_t nowNs) noexcept {
        Slot* s = slotOf_(peer, /*create=*/true);
        if (s == nullptr) return false;
        if (s->current && s->current->epoch != keys.epoch) s->previous = Retired{*s->current, nowNs + kRetainNs};
        s->current = keys;
        if (s->previous && s->previous->keys.epoch == keys.epoch) s->previous.reset();   // the same number reused
        return true;
    }

    /// The current epoch for `peer`; 0 = no established link.
    [[nodiscard]] uint32_t currentEpoch(int32_t peer) const noexcept {
        const Slot* s = slotOf_(peer);
        return (s != nullptr && s->current) ? s->current->epoch : 0u;
    }

    [[nodiscard]] const LinkKeys* current(int32_t peer) const noexcept {
        const Slot* s = slotOf_(peer);
        return (s != nullptr && s->current) ? &*s->current : nullptr;
    }

    /// The keys of `epoch` for `peer`: the current epoch, or the previous one while its grace period lasts.
    [[nodiscard]] const LinkKeys* find(int32_t peer, uint32_t epoch, int64_t nowNs) const noexcept {
        const Slot* s = slotOf_(peer);
        if (s == nullptr) return nullptr;
        if (s->current && s->current->epoch == epoch) return &*s->current;
        if (s->previous && s->previous->keys.epoch == epoch && nowNs < s->previous->untilNs) return &s->previous->keys;
        return nullptr;
    }

    [[nodiscard]] std::size_t peerCount() const noexcept { return used_; }

private:
    struct Retired { LinkKeys keys; int64_t untilNs = 0; };
    struct Slot {
        int32_t                peer = -1;
        std::optional<LinkKeys> current;
        std::optional<Retired>  previous;
    };

    [[nodiscard]] const Slot* slotOf_(int32_t peer) const noexcept {
        for (std::size_t i = 0; i < used_; ++i) if (slots_[i].peer == peer) return &slots_[i];
        return nullptr;
    }
    Slot* slotOf_(int32_t peer, bool create) noexcept {
        for (std::size_t i = 0; i < used_; ++i) if (slots_[i].peer == peer) return &slots_[i];
        if (!create || used_ == kMaxPeers) return nullptr;
        slots_[used_].peer = peer;
        return &slots_[used_++];
    }

    std::array<Slot, kMaxPeers> slots_{};
    std::size_t                 used_ = 0;
};

} // namespace rune::peerlink
