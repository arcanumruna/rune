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
//   (a Table / Patch item whose op `topicId` is kPermBitmapImport carries the IMPORT bitmap of the table instead of the
//   WRITE one; a table without such an item denies IMPORT everywhere)
//   kPermFlagBind   session `sessionId` now uses table `nextOffset` (-1 = deny everything, -2 = its namespace is
//                   suspended ReadOnly: deny everything, answered NamespaceSuspended); correlationId = epoch,
//                   producerId = the principal (#122);
//   kPermFlagDrop   table `sessionId` is freed: every slot still pointing at it is reset to deny, its bits cleared.
//   kPermFlagFence / kPermFlagSecurityBehind: no table (see below).
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
    std::vector<uint64_t> importBits;   ///< the same layout for IMPORT; empty = IMPORT nowhere
};
/// The two fixed tables: nothing (default deny) and everything (dev / in-process announces).
[[nodiscard]] inline const WritePermTable* denyWriteTable() noexcept { static const WritePermTable t{}; return &t; }
[[nodiscard]] inline const WritePermTable* allowAllWriteTable() noexcept { static const WritePermTable t{{}, true}; return &t; }
/// Namespaces Phase 2: denies everything like denyWriteTable(), but its address says WHY - the session's namespace is
/// suspended ReadOnly - so the worker's cold refusal answers NamespaceSuspended instead of NotAuthorized.
[[nodiscard]] inline const WritePermTable* suspendedWriteTable() noexcept { static const WritePermTable t{}; return &t; }

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

[[nodiscard]] inline bool allowsImport(const WriteGrantSlot& s, int32_t topicId) noexcept {
    const WritePermTable& p = *s.table;
    const uint32_t t = static_cast<uint32_t>(topicId);
    return p.grantAll | ((t >> 6) < p.importBits.size() && ((p.importBits[t >> 6] >> (t & 63)) & 1u));
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
/// Issue #603: this broker's `__security` view is behind committed events it holds (nextOffset = the lag level, see
/// kSecurityLag*; 0 = no longer behind).  While behind, the worker answers every ingress produce AuthUnavailable
/// (nothing appended) and
/// pushes no subscription.  The control thread ships the clear only once every applied event's enforcement fence was
/// acknowledged on both rings, so the revoked bitmaps and the ended subscriptions are installed first.  Carries no
/// table.
inline constexpr uint8_t kPermFlagSecurityBehind = 0x80;
/// Issues #603 / #506: the level a kPermFlagSecurityBehind item carries in `nextOffset` (one lag LEVEL, not two flags).
/// RefuseAll: every ingress produce is answered AuthUnavailable (#603).  DeferDenials: the view holds events it has not
/// applied (#506) - a produce the worker would ALLOW is served, one it would DENY is answered AuthUnavailable.  A value
/// above DeferDenials is treated as RefuseAll (fail closed).
inline constexpr uint8_t kSecurityLagCurrent      = 0;
inline constexpr uint8_t kSecurityLagRefuseAll    = 1;
inline constexpr uint8_t kSecurityLagDeferDenials = 2;
/// SESSION_PERMISSIONS `topicId` of a Table / Patch item: which bitmap of the table it carries.
inline constexpr int32_t kPermBitmapWrite  = 0;
inline constexpr int32_t kPermBitmapImport = 1;
/// #500: `topicId` of a kPermFlagSecurityBehind item that carries the broker's applied topic FRONTIER (the catalog's
/// nextTopicId, in `nextOffset`) instead of a lag level.  The lag-level item carries kPermBitmapWrite (0) here, so the
/// two never alias.  A worker answers a produce it would deny for a topic id at or above the frontier NotLeader (not
/// applied here yet), never the terminal NotAuthorized.
inline constexpr int32_t kPermLagTopicFrontier = 2;
/// A kPermFlagSecurityBehind item whose `topicId` is kPermLagReset is a RESET: the control thread's shipper queue
/// overflowed for this worker (it was a whole queue behind and skipped items), so everything it knows about tables and
/// bindings may be stale.  The worker drops every table, denies every session, discards a half-staged table and refuses
/// every produce (retriable AuthUnavailable) until the snapshot that follows ends with a lag level.  Distinct from the
/// lag-level item (kPermBitmapWrite) and the frontier item (kPermLagTopicFrontier).
inline constexpr int32_t kPermLagReset = 3;
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

/// What applying one SESSION_PERMISSIONS op did.  The lifecycle ring is in-process, FIFO and written only by the
/// gateway's PermissionShipper, so only two failures exist:
///   UnknownSession  a Bind for a session this worker no longer has (it closed while the bind was queued): benign;
///   Violation       a structurally impossible op (bad length / bounds / table id / chunk order / variant flags): a
///                   broker bug - the worker halts on it (Worker::executeSessionPermissionsWire_).
/// Staged = a chunk of a table was stored, the table is not complete yet; Applied = the op took effect.
enum class PermApply : uint8_t { Applied, Staged, UnknownSession, Violation };

/// The worker's chunk assembler of a full TABLE: ONE per worker; the buffer circulates between it, the tables and
/// `spare_` so nothing allocates once the buffers reached their peak size.  `Sink` provides
/// installWriteGrants(key, words&, grantAll, epoch, owner) and installImportGrants(key, words&) - `key` the table id
/// (issue #259).
class PermissionStager {
public:
    template <class Sink>
    PermApply apply(Sink& sink, int64_t sessionId, uint64_t firstWord, uint64_t totalWords, uint8_t flags,
                    uint64_t epoch, const uint8_t* tail, std::size_t tailLen, uint64_t owner, int32_t bitmap) {
        static_assert(sizeof(uint64_t) == 8);
        if (tailLen % 8 != 0) return drop_("table chunk tail is not whole words");
        if (firstWord > totalWords || (tailLen / 8) > totalWords - firstWord)
            return drop_("table chunk words lie outside the table");
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
            firstWord != st->filled) return drop_("table chunk is not the next one of the staged table");
        if (count != 0) std::memcpy(st->words.data() + firstWord, tail, tailLen);   // little-endian hosts only
        st->filled = firstWord + count;
        if ((flags & kPermFlagLast) == 0) return PermApply::Staged;
        if (st->filled != totalWords) return drop_("last table chunk does not complete the table");
        const bool ok = bitmap == kPermBitmapImport
            ? sink.installImportGrants(sessionId, st->words)
            : sink.installWriteGrants(sessionId, st->words, (flags & kPermFlagGrantAll) != 0, epoch, owner);
        spare_ = std::move(st->words);                         // the swapped-out slot buffer: the next staging buffer
        state_ = NoUpdate{};
        if (!ok) { reason_ = "table id beyond the store"; ++dropped_; return PermApply::Violation; }
        return PermApply::Applied;
    }
    /// #442: forget a half-staged table (a reset: the chunks that follow belong to a snapshot, never to it).
    void discard() noexcept {
        if (auto* st = std::get_if<Staging>(&state_)) spare_ = std::move(st->words);
        state_ = NoUpdate{};
    }
    /// Why the last Violation was returned (a static string; meaningful only after a Violation).
    [[nodiscard]] const char* violationReason() const noexcept { return reason_; }
    [[nodiscard]] bool staging() const noexcept { return std::holds_alternative<Staging>(state_); }
    [[nodiscard]] uint64_t droppedChunks() const noexcept { return dropped_; }   // Violations returned

private:
    PermApply drop_(const char* reason) noexcept {
        if (auto* st = std::get_if<Staging>(&state_)) spare_ = std::move(st->words);
        state_ = NoUpdate{};
        ++dropped_;
        reason_ = reason;
        return PermApply::Violation;
    }
    PermissionUpdate      state_;
    std::vector<uint64_t> spare_;
    uint64_t              dropped_ = 0;
    const char*           reason_  = "";
};

} // namespace rune::node
