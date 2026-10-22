#pragma once
// WorkerWriteGrants.h - a shard worker's WRITE grants (authentication Task 13, spec §6, D30; issue #259).
//
// The control thread compiles ONE table per distinct grant set (gateway PermissionPool) and ships each table's WRITE
// bitmap ONCE to every worker as SESSION_PERMISSIONS ops on the session-lifecycle ring; a session is BOUND to its
// table by a one-op bind.  The worker owns the tables outright (shared-nothing): a fixed store of WritePermTable (stable
// addresses), and one WriteGrantSlot per SessionAckSink slot pointing at its table - checked on every worker-ingress
// produce with ONE pointer load, ONE bounds check and ONE bit load.
//
// SESSION_PERMISSIONS variants (the op's `isolation` flags):
//   kPermFlagTable  a chunk of table `sessionId`'s full bitmap (nextOffset = first word, hwBound = total words);
//                   staged and swapped in on the LAST chunk (a partial table is never visible);
//   kPermFlagPatch  words [nextOffset, +n) of table `sessionId`, resized to hwBound words (a created / dropped object);
//   kPermFlagBind   session `sessionId` now uses table `nextOffset` (-1 = deny everything); correlationId = epoch,
//                   producerId = the principal (#122);
//   kPermFlagDrop   table `sessionId` is freed: every slot still pointing at it is reset to deny, its bits cleared.
// A session announced without grantAll starts denied: every produce from it is answered NotAuthorized until its bind
// arrives (default deny).
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <variant>
#include <vector>

#include <rune/protocol/ProducerIdBinding.h>

namespace rune::node {

struct WritePermTable {
    std::vector<uint64_t> bits;      ///< bit (id & 63) of word (id >> 6); capacity persists
    bool                  grantAll = false;
};
/// The two fixed tables: nothing (default deny) and everything (dev / in-process announces).
[[nodiscard]] inline const WritePermTable* denyWriteTable() noexcept { static const WritePermTable t{}; return &t; }
[[nodiscard]] inline const WritePermTable* allowAllWriteTable() noexcept { static const WritePermTable t{{}, true}; return &t; }

struct WriteGrantSlot {
    const WritePermTable* table    = denyWriteTable();   ///< never null
    uint32_t              ownerTag = 0;   ///< issue #122: pidOwnerTag(the session's principal)
    uint64_t              epoch    = 0;   ///< the bind epoch this slot holds
};

/// The hot-path check.
[[nodiscard]] inline bool allowsWrite(const WriteGrantSlot& s, int32_t topicId) noexcept {
    const WritePermTable& p = *s.table;
    const uint32_t t = static_cast<uint32_t>(topicId);
    return p.grantAll | ((t >> 6) < p.bits.size() && ((p.bits[t >> 6] >> (t & 63)) & 1u));
}

/// SESSION_PERMISSIONS wire flags (the op's `isolation` byte).
inline constexpr uint8_t kPermFlagLast     = 0x01;
inline constexpr uint8_t kPermFlagGrantAll = 0x02;
inline constexpr uint8_t kPermFlagTable    = 0x04;   ///< issue #259: the variants above
inline constexpr uint8_t kPermFlagPatch    = 0x08;
inline constexpr uint8_t kPermFlagBind     = 0x10;
inline constexpr uint8_t kPermFlagDrop     = 0x20;
/// Issue #196: an enforcement fence on the session-lifecycle ring (correlationId = the fence sequence number): the
/// worker answers SessionSignal FenceAck{sessionId 1} - every permission item, bind and SessionClose queued before it
/// on this ring is installed.  Carries no table.
inline constexpr uint8_t kPermFlagFence    = 0x40;
/// The worker's table store bound (== gateway PermissionPool::kMaxTables).
inline constexpr std::size_t kMaxWritePermTables = 4096;

struct NoUpdate {};
struct Staging {
    int64_t  sessionId  = 0;
    uint64_t totalWords = 0;
    uint64_t filled     = 0;     ///< words received so far (the next chunk must start here)
    std::vector<uint64_t> words;
};
using PermissionUpdate = std::variant<NoUpdate, Staging>;

enum class PermissionChunkResult : uint8_t { Staged, Installed, Dropped };

/// The worker's chunk assembler of a full TABLE: ONE per worker; the buffer circulates between it, the tables and
/// `spare_` so nothing allocates once the buffers reached their peak size.  `Sink` provides
/// installWriteGrants(key, words&, grantAll, epoch, owner) - `key` the table id (issue #259).
class PermissionStager {
public:
    template <class Sink>
    PermissionChunkResult apply(Sink& sink, int64_t sessionId, uint64_t firstWord, uint64_t totalWords, uint8_t flags,
                                uint64_t epoch, const uint8_t* tail, std::size_t tailLen, uint64_t owner) {
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
