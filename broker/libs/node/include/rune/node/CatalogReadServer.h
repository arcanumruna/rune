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
//   PausePending / Paused — the SESSION's egress is back-pressured (fix round 0):
//               the control thread could not offer one of its Records frames
//               and reported it (onPaused, with that frame's baseOffset).  The
//               subscription rewinds to that offset, acknowledges the pause on
//               the egress ring (PausePending until the ack is on it — every
//               frame of it still in flight precedes the ack, so the control
//               thread drops exactly those) and sends nothing until onResume
//               (the session's window has room again), then continues from its
//               own cursor.  The log IS the per-subscription buffer: a paused
//               reader holds no bytes anywhere, never grows the session's spill,
//               and is never expired for being slow; other readers and the DDL
//               answers (their own ring) never wait for it.
// Egress RING full: the subscription keeps its phase and nextOffset and resumes
// next pass (nothing queues here).
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
#include <array>
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
/// (capacity / 8) bounds one egress frame: [CatalogReadFrameHead][client frame].
inline constexpr int kCatalogReadRingBytes = 1 << 21;   // 2 MiB → 256 KiB frames
/// One catalog-read ring frame's head (fix round 0):
///   subscriptionId 0              — a RELIABLE answer (SubscribeResponse /
///                                   ErrorResponse): queued under back-pressure;
///   subscriptionId ≠ 0 + a frame  — a FLOW-CONTROLLED Records frame at baseOffset:
///                                   offered, never spilled (back-pressure pauses it);
///   subscriptionId ≠ 0, no frame  — the subscription's PAUSE ACK (nothing of it follows
///                                   until it is resumed).
struct CatalogReadFrameHead {
    int64_t session        = 0;
    int64_t subscriptionId = 0;
    int64_t baseOffset     = -1;
};
inline constexpr std::size_t kCatalogReadHeadBytes = 24;
/// An empty frame with this baseOffset is a session-liveness PROBE (fix round 3);
/// an empty frame with baseOffset -1 is a pause ack.
inline constexpr int64_t kProbeOffset = -2;
/// The largest frame the server emits (SubscribeResponse / Records / ErrorResponse).
inline constexpr std::size_t kCatalogReadMaxFrameBytes =
    static_cast<std::size_t>(kCatalogReadRingBytes) / 8 - kCatalogReadHeadBytes;
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

/// Sends one encoded client frame (empty: a pause ack) to `head.session` (admin →
/// control, then the session's egress).  false = the ring is full: nothing was
/// sent, retry later.
using CatalogReadEgress = std::function<bool(const CatalogReadFrameHead& head, std::span<const uint8_t> frame)>;

class CatalogReadServer {
public:
    struct Answering { int64_t nextOffset = 0; };   // SubscribeResponse{OK} owed
    struct Backlog   { int64_t nextOffset = 0; };   // pushing [nextOffset, HW); the marker on leaving
    struct Live      { int64_t nextOffset = 0; int64_t markedAt = 0; };   // marker sent (at markedAt); live
    using Resumable = std::variant<Backlog, Live>;
    struct PausePending { Resumable resumeAs; };   // paused; its ack is not on the ring yet
    struct Paused       { Resumable resumeAs; };   // paused + acked: nothing sent until onResume
    struct Ending       {};                        // ended this pass (its answer is owed / sent); removed after it
    using Phase = std::variant<Answering, Backlog, Live, PausePending, Paused, Ending>;
    struct Sub {
        int64_t session        = 0;
        int64_t corr           = 0;   // the creating Subscribe's correlation id (every frame carries it)
        int64_t subscriptionId = 0;
        Phase   phase;
        int64_t probeDueNs     = 0;   // fix round 3: when its session-liveness probe is next due
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
    /// Fix round 3 (F3b): the same pass at `nowNs` — every subscription also
    /// sends a session-liveness PROBE (an empty frame, baseOffset kProbeOffset)
    /// every kProbeIntervalNs; the control thread answers onSessionGone for a
    /// session that no longer exists.  So a session close the gateway could not
    /// queue is never lost: its subscriptions are reclaimed within one interval.
    int pump(int64_t nowNs);
    static constexpr int64_t kProbeIntervalNs = 1'000'000'000;
    /// control → admin: `session` is gone — every subscription of it ends (its close).
    void onSessionGone(int64_t session) { unsubscribe(session, -1); }
    /// control → admin (fix round 0): the session's egress could not take this
    /// subscription's Records frame at `baseOffset` — rewind to it and pause
    /// (acknowledged on the egress ring).  An unknown subscription is acked too
    /// (the control thread clears its entry).
    /// `droppedMarker`: the dropped frame was the caught-up marker (an empty
    /// Records) — re-sent once; a dropped data frame at the marker's offset
    /// never repeats the marker (fix round 1, I1).
    void onPaused(int64_t session, int64_t subscriptionId, int64_t baseOffset, bool droppedMarker);
    /// control → admin: the session's egress has room again — continue from the cursor.
    void onResume(int64_t session, int64_t subscriptionId);

    /// Fix round 1 (C1) / round 2 (N1): the committed prefix this server serves
    /// from — fed ONLY by the replica's committed HW, monotonic within one log
    /// history: a follower's HW reads 0 after every leader change until the new
    /// leader answers it, which must never rewind a clamp or fake a marker.  A
    /// truncation below it (unclean / survivor election, deficient leader) ends
    /// every read once and restarts it at the post-truncation committed HW,
    /// tagged with the epoch it restarted in (floorEpoch()).
    [[nodiscard]] int64_t servedHighWatermark() const noexcept { return servedHw_; }
    [[nodiscard]] int32_t floorEpoch() const noexcept { return floorEpoch_; }
    /// Fix round 1 (M3): while true the admin takes no catalog request off the
    /// request ring (the gateway answers the retriable NotLeader) — the owed
    /// answers stay within their preallocated capacity.
    [[nodiscard]] bool intakePaused() const noexcept { return owed_.size() + kIntakeSlack > owedCapacity_; }
    /// Subscriptions ended because the replica truncated below a served HW.
    [[nodiscard]] uint64_t truncationEnds() const noexcept { return truncationEnds_; }
    [[nodiscard]] std::size_t subscriptions() const noexcept { return subs_.size(); }
    [[nodiscard]] std::size_t maxSubscriptions() const noexcept { return maxSubs_; }
    /// Subscribes refused beyond maxSubs / subscriptions ended by a read failure.
    [[nodiscard]] uint64_t refusedSubscribes() const noexcept { return refused_; }
    [[nodiscard]] uint64_t endedSubscriptions() const noexcept { return ended_; }
    /// "Answering" | "Backlog" | "Live" | "PausePending" | "Paused"; "" = none.
    [[nodiscard]] const char* phaseName(int64_t subscriptionId) const noexcept;
    /// Catalog history Task 13 (tests): the requested fromOffset of each catalog
    /// Subscribe this server ACCEPTED (beyond the cap: not recorded), oldest
    /// first — the last kAcceptedFromKept of them (a fixed array: no growth).
    static constexpr std::size_t kAcceptedFromKept = 32;
    [[nodiscard]] std::vector<int64_t> acceptedFromOffsetsForTest() const;

private:
    /// A refusal / an ended subscription's answer the egress could not take yet.
    enum class OwedKind : uint8_t { SubscribeAnswer, EndedAnswer, PauseAck };
    struct OwedError {
        int64_t  session = 0;
        int64_t  corr    = 0;
        int16_t  code    = 0;
        OwedKind kind    = OwedKind::SubscribeAnswer;
        int64_t  subscriptionId = 0;
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
    bool sendPauseAck_(int64_t session, int64_t subId);
    int  probe_(int64_t nowNs);
    int64_t nowNs_ = 0;
    bool sendReliable_(int64_t session, std::size_t n);
    void end_(std::size_t index, int16_t code, const char* why, int64_t at);
    void owe_(const OwedError& e);
    /// N1 / N2: a truncation below the floor since the last pass → end every
    /// read once, restart the floor.  true = it happened (the pass ends).
    bool checkTruncation_();
    void raiseFloor_() noexcept;
    static constexpr std::size_t kIntakeSlack = 32;   // > one request-ring drain (16)
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
    std::size_t                          owedCapacity_ = 0;
    int64_t                              servedHw_ = 0;   // the committed floor (C1, N1)
    int32_t                              floorEpoch_ = -1;   // the replica epoch the floor (re)started in
    uint64_t                             seenLogGen_ = 0;    // N2: the replica's log generation already checked
    std::size_t                          rr_ = 0;          // round-robin start (M2)
    uint64_t                             truncationEnds_ = 0;
    std::array<int64_t, kAcceptedFromKept> acceptedFrom_{};   // a ring of the last accepted fromOffsets (tests)
    uint64_t                             acceptedCount_ = 0;
    uint64_t                             refused_ = 0;
    uint64_t                             ended_   = 0;
};

} // namespace rune::node
