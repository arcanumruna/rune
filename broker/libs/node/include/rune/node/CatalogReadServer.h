#pragma once
// CatalogReadServer.h — catalog history Task 12 (spec §7, ruling 10, F19): any
// broker serves a Subscribe on `__catalog` (topic id −6) from ITS OWN replica,
// up to its OWN high watermark — committed data only, on a leader, a follower or
// a broker whose apply halted alike.  Serving the existing Subscribe is
// transport, not a read API: history / as-of / diff / replay stay client-side.
//
// Owned and driven by the ADMIN thread (the `__catalog` replica's owner — hard
// rule 1).  The gateway forwards a catalog Subscribe / Unsubscribe / session
// close on the gateway → admin request ring (kMsgCatalogSubscribe /
// kMsgCatalogUnsubscribe); every answer — the SubscribeResponse, the Records
// frames, the caught-up marker, a refusal — leaves through ONE egress (the
// admin → control catalog-read ring, then the session's egress), so the client
// sees them in order.
//
// One subscription = one explicit phase (hard rule 3, F29 — no caughtUpSent flag):
//   Answering — accepted; its SubscribeResponse{OK} is not on the egress yet;
//   Backlog   — pushing [nextOffset, HW); on reaching the HW it sends the
//               caught-up marker Records{baseOffset = nextOffset (== the HW it
//               reached), empty payload} and becomes Live (the transition is the
//               "once");
//   Live      — the marker was sent; pushes new commits as they arrive.
// Egress full: the subscription keeps its phase and nextOffset and resumes next
// pass (bounded memory: nothing queues here; the egress ring and the session's
// spill are both bounded, and the gateway expires a session its spill cannot
// hold — explicitly, SessionExpired — which ends its subscriptions here).
//
// A subscription is ended explicitly — ErrorResponse{code} on its Subscribe's
// correlation id, then removed — when its log cannot be read below the HW
// (Internal: torn / absent / I/O error) or one stored batch can never fit one
// egress frame (MessageTooLarge: the leader bounds every event below it,
// kCatalogMaxEventBytes).  Never a silent stall.
//
// Zero steady-state allocation (hard rule 4): the subscription table, the read
// buffer and the frame buffer are preallocated; an idle pass reads nothing.

#include <rune/gateway/IAdminRequestSink.h>   // kCatalogSubscriptionIdBase

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <variant>
#include <vector>

namespace rune::replication {
class PartitionReplica;
}

namespace rune::node {

/// Catalog subscription ids: bit 62 set — never a gateway id (node prefix << 40
/// | a 40-bit counter), so the gateway routes an Unsubscribe by the id alone.
inline constexpr int64_t kCatalogSubscriptionIdBase = rune::gateway::kCatalogSubscriptionIdBase;

/// The admin → control catalog-read ring (one per broker).  Its max message
/// (capacity / 8) bounds one egress frame: [session:i64][frame].
inline constexpr int kCatalogReadRingBytes = 1 << 21;   // 2 MiB → 256 KiB frames
/// The largest frame the server emits (SubscribeResponse / Records / ErrorResponse).
inline constexpr std::size_t kCatalogReadMaxFrameBytes = static_cast<std::size_t>(kCatalogReadRingBytes) / 8 - 8;
/// Records framing: the 24-byte header + 24-byte body + the u32 batch-length prefix.
inline constexpr std::size_t kCatalogRecordsOverhead = 24 + 24 + 4;
/// The largest stored batch one Records frame can carry.
inline constexpr std::size_t kCatalogReadMaxBatchBytes = kCatalogReadMaxFrameBytes - kCatalogRecordsOverhead;
/// The largest ENCODED event the catalog leader may append: one event is one
/// single-record batch (61 B batch header + the record framing + the 8-byte key),
/// so every event a leader writes is readable by every catalog subscriber.
inline constexpr std::size_t kCatalogMaxEventBytes = kCatalogReadMaxBatchBytes - 512;
/// Batches are coalesced into one Records frame up to this many bytes (a larger
/// single batch goes alone, up to kCatalogReadMaxBatchBytes).
inline constexpr std::size_t kCatalogReadCoalesceBytes = 64 * 1024;
/// The preallocated read buffer; one pass reads at most half of it per subscription.
inline constexpr std::size_t kCatalogReadBufferBytes = std::size_t{1} << 20;

/// Sends one encoded client frame to `session` (admin → control, then the
/// session's egress).  false = the egress is full: nothing was sent, retry later.
using CatalogReadEgress = std::function<bool(int64_t session, std::span<const uint8_t> frame)>;

class CatalogReadServer {
public:
    struct Answering { int64_t nextOffset = 0; };   // SubscribeResponse{OK} owed
    struct Backlog   { int64_t nextOffset = 0; };   // pushing [nextOffset, HW); the marker on leaving
    struct Live      { int64_t nextOffset = 0; };   // marker sent; pushes new commits
    using Phase = std::variant<Answering, Backlog, Live>;
    struct Sub {
        int64_t session        = 0;
        int64_t corr           = 0;   // the creating Subscribe's correlation id (every frame carries it)
        int64_t subscriptionId = 0;
        Phase   phase;
    };

    /// \param idPrefix the node's cluster id prefix ((nodeId + 1) << 40, as the
    ///                 gateway's): catalog subscription ids stay cluster-unique.
    CatalogReadServer(rune::replication::PartitionReplica& catalog, CatalogReadEgress send,
                      std::size_t maxSubs = 64, int64_t idPrefix = 0);

    /// A catalog Subscribe.  fromOffset −2 (earliest) → 0; −1 (latest) or any
    /// other negative → the HW; an explicit offset above the HW → the HW.
    /// Answered SubscribeResponse{OK} (on the next pump, before any Records) —
    /// or, beyond maxSubs, SubscribeResponse{Internal} with a log line.  A
    /// re-sent Subscribe (same session + correlation id) is re-answered with the
    /// existing subscription (no second stream).
    void subscribe(int64_t session, int64_t corr, int64_t fromOffset);
    /// Remove `subscriptionId` of `session` (an id another session owns is
    /// ignored); −1 = every subscription of the session (its close).
    void unsubscribe(int64_t session, int64_t subscriptionId);
    /// Owed answers first, then per subscription: [nextOffset, HW) in whole
    /// batches (at most half the read buffer per subscription per pass), then the
    /// marker for a Backlog subscription at the HW.  Returns frames sent + subs ended.
    int pump();

    [[nodiscard]] std::size_t subscriptions() const noexcept { return subs_.size(); }
    [[nodiscard]] std::size_t maxSubscriptions() const noexcept { return maxSubs_; }
    /// Subscribes refused beyond maxSubs / subscriptions ended by a read failure.
    [[nodiscard]] uint64_t refusedSubscribes() const noexcept { return refused_; }
    [[nodiscard]] uint64_t endedSubscriptions() const noexcept { return ended_; }
    /// The phase name of `subscriptionId` ("Answering" | "Backlog" | "Live"; "" = none).
    [[nodiscard]] const char* phaseName(int64_t subscriptionId) const noexcept;

private:
    /// A refusal / an ended subscription's answer the egress could not take yet.
    struct OwedError {
        int64_t session = 0;
        int64_t corr    = 0;
        int16_t code    = 0;
        bool    asSubscribeResponse = false;   // a Subscribe's answer (else ErrorResponse: an ended sub)
        int64_t subscriptionId = 0;
    };

    enum class Push : uint8_t { Progress, Blocked, Idle, Ended };
    /// Ended: why (the answer code + a reason for the log line).
    struct PushResult {
        Push        kind = Push::Idle;
        int16_t     code = 0;
        const char* why  = "";
    };
    PushResult pushRange_(Sub& s, int64_t& nextOffset, int64_t hw);
    bool sendSubscribeResponse_(int64_t session, int64_t corr, int16_t code, int64_t subId);
    bool sendError_(const OwedError& e);
    bool sendRecords_(const Sub& s, int64_t baseOffset, std::span<const uint8_t> batches);
    void end_(std::size_t index, int16_t code, const char* why, int64_t at);
    int  flushOwed_();

    rune::replication::PartitionReplica& catalog_;
    CatalogReadEgress                    send_;
    std::size_t                          maxSubs_;
    int64_t                              idPrefix_;
    int64_t                              nextId_ = 0;
    std::vector<Sub>                     subs_;       // reserved maxSubs_
    std::vector<OwedError>               owed_;       // reserved; grows only past maxSubs_ refusals in one burst
    std::vector<uint8_t>                 readBuf_;    // kCatalogReadBufferBytes
    std::vector<uint8_t>                 frameBuf_;   // kCatalogReadMaxFrameBytes
    uint64_t                             refused_ = 0;
    uint64_t                             ended_   = 0;
};

} // namespace rune::node
