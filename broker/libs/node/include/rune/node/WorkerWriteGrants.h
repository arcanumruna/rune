#pragma once
// WorkerWriteGrants.h - a shard worker's per-session WRITE grants (authentication Task 13, spec §6, D30).
//
// The control thread ships each authenticated session's WRITE bitmap (PermissionTable::writeBits()) to every worker as
// SESSION_PERMISSIONS chunks on the session-lifecycle ring.  The worker owns the result outright (shared-nothing): one
// WriteGrantSlot per SessionAckSink slot, checked on every worker-ingress produce with ONE bounds check + ONE load.
//
// Chunked updates are staged in the worker's single Staging buffer and swapped into the slot on the LAST chunk, so a
// partially delivered table is never visible.  A session announced without grantAll starts with an empty slot: every
// produce from it is answered NotAuthorized until its permissions arrive (default deny).
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <variant>
#include <vector>

#include <rune/protocol/ProducerIdBinding.h>

namespace rune::node {

struct WriteGrantSlot {
    std::vector<uint64_t> bits;      ///< bit (id & 63) of word (id >> 6); capacity persists
    bool                  grantAll = false;
    uint32_t              ownerTag = 0;   ///< issue #122: pidOwnerTag(the session's principal) - next to grantAll, so
                                          ///< the per-frame check reads no other cache line than allowsWrite
    uint64_t              epoch    = 0;   ///< the shipped epoch these bits belong to
};

/// The hot-path check.
[[nodiscard]] inline bool allowsWrite(const WriteGrantSlot& s, int32_t topicId) noexcept {
    const uint32_t t = static_cast<uint32_t>(topicId);
    return s.grantAll | ((t >> 6) < s.bits.size() && ((s.bits[t >> 6] >> (t & 63)) & 1u));
}

/// SESSION_PERMISSIONS wire flags (the op's `isolation` byte).
inline constexpr uint8_t kPermFlagLast     = 0x01;
inline constexpr uint8_t kPermFlagGrantAll = 0x02;

struct NoUpdate {};
struct Staging {
    int64_t  sessionId  = 0;
    uint64_t totalWords = 0;
    uint64_t filled     = 0;     ///< words received so far (the next chunk must start here)
    std::vector<uint64_t> words;
};
using PermissionUpdate = std::variant<NoUpdate, Staging>;

enum class PermissionChunkResult : uint8_t { Staged, Installed, Dropped };

/// The worker's chunk assembler: ONE per worker; the buffer circulates between it, the slots and `spare_` so nothing
/// allocates once the buffers reached their peak size.  `Sink` provides installWriteGrants(sid, words&, grantAll, epoch,
/// owner).  `owner` (issue #122) is the session's principal; every chunk of an update carries the same one.
class PermissionStager {
public:
    template <class Sink>
    PermissionChunkResult apply(Sink& sink, int64_t sessionId, uint64_t firstWord, uint64_t totalWords, uint8_t flags,
                                uint64_t epoch, const uint8_t* tail, std::size_t tailLen, uint64_t owner = 0) {
        static_assert(sizeof(uint64_t) == 8);
        if (tailLen % 8 != 0 || firstWord > totalWords || (tailLen / 8) > totalWords - firstWord) return drop_();
        const std::size_t count = tailLen / 8;
        if (firstWord == 0) {                                  // a first chunk (re)starts the staging
            Staging st;
            st.sessionId  = sessionId;
            st.totalWords = totalWords;
            st.words      = std::move(spare_);                 // capacity persists
            st.words.clear();
            st.words.resize(static_cast<std::size_t>(totalWords));
            state_ = std::move(st);
        }
        Staging* st = std::get_if<Staging>(&state_);
        if (st == nullptr || st->sessionId != sessionId || st->totalWords != totalWords ||
            firstWord != st->filled) return drop_();
        if (count != 0) std::memcpy(st->words.data() + firstWord, tail, tailLen);   // little-endian hosts only
        st->filled = firstWord + count;
        if ((flags & kPermFlagLast) == 0) return PermissionChunkResult::Staged;
        if (st->filled != totalWords) return drop_();
        const bool ok = sink.installWriteGrants(sessionId, st->words, (flags & kPermFlagGrantAll) != 0, epoch, owner);
        spare_ = std::move(st->words);                         // the swapped-out slot buffer: the next staging buffer
        state_ = NoUpdate{};
        return ok ? PermissionChunkResult::Installed : PermissionChunkResult::Dropped;   // unknown / closed session
    }
    [[nodiscard]] bool staging() const noexcept { return std::holds_alternative<Staging>(state_); }
    [[nodiscard]] uint64_t droppedChunks() const noexcept { return dropped_; }

private:
    PermissionChunkResult drop_() noexcept {
        if (auto* st = std::get_if<Staging>(&state_)) spare_ = std::move(st->words);
        state_ = NoUpdate{};
        ++dropped_;
        return PermissionChunkResult::Dropped;
    }
    PermissionUpdate      state_;
    std::vector<uint64_t> spare_;
    uint64_t              dropped_ = 0;
};

} // namespace rune::node
