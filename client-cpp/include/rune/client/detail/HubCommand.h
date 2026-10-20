// include/rune/client/detail/HubCommand.h
//
// F8a — app→duty-thread commands for the subscriber and group-consumer hubs.
//
// Shared-nothing law: the duty thread (Thread-mode runner, or the Invoker
// owner) OWNS every SubscriberHub / GroupConsumerHub table and every agent-
// side field of SubscriptionImpl / GroupConsumerImpl.  An app-thread API call
// (subscribe / unsubscribe / commit / commitSync / close / subscribeGroup)
// never touches that state: it builds one of these commands and hands it to a
// HubCommandSink, which (in a real Client) writes it onto the agent's M2O
// command ring (ring_msg::HubCommandMsg).  The duty thread drains the ring and
// applies the command to the hub that owns the state.  No atomics, no locks.
//
// This header is dependency-free on purpose: the group library does not link
// the agent, so the command type lives here and the agent owns only its
// on-ring encoding.

#pragma once

#include <cstdint>

namespace rune::client::detail {

struct HubCommand {
    enum class Op : uint16_t {
        // ---- SubscriberHub ---------------------------------------------------
        /// handle = heap subscriber::SubscribeRequest* (ownership passes to the
        /// duty thread, which adopts it into the pending table and sends the
        /// encoded Subscribe frame).
        SubscribeAdd    = 1,
        /// a = the subscription's Subscribe correlationId (its identity).
        Unsubscribe     = 2,

        // ---- GroupConsumerHub -------------------------------------------------
        /// handle = the GroupConsumerImpl* (F8c C7: no shared_ptr holder) —
        /// adopted by the duty thread (its share of the impl, §3.3).
        GroupAdd        = 10,
        /// handle = GroupConsumerImpl* (identity only; resolved against the
        /// hub's own list, never dereferenced unless found there).
        GroupCommit     = 11,
        /// handle = GroupConsumerImpl* (identity); a = the app's Token; b / c =
        /// the completion target (detail::encodeTarget, F8c C4).  Exactly one
        /// completion follows (the duty thread / close() delivers it).
        GroupCommitSync = 12,
        /// handle = GroupConsumerImpl* (identity).
        GroupClose      = 13,
        /// handle = GroupConsumerImpl* (identity); a = which callback
        /// (0 onAssigned, 1 onRevoked, 2 onRecords); b = heap std::function*
        /// of the matching type (ownership passes to the duty thread, which
        /// installs it and frees the holder).  Always via the ring (never
        /// applied directly: replacing a callback while it runs is UB).
        GroupSetCallback = 14,
        /// Phase 2b F (Task 10): token ops (exactly one completion, like
        /// GroupCommitSync).  handle = GroupConsumerImpl* (identity); a = Token,
        /// b / c = the completion target.
        /// GroupSeekToCommitted: rewind every assigned partition to the committed offset.
        GroupSeekToCommitted = 15,
        /// GroupSeek: d = the offset, e = (topicId << 32 | partition) of the partition to restart there.
        GroupSeek            = 16,

        // ---- Client (F8c) ------------------------------------------------------
        /// handle = detail::CompletionRing* of a client-bound Completions: the
        /// duty registry adopts it (C2) and answers with a registration record
        /// carrying its CqRef (or -1: registry full, not adopted).
        CqAdd           = 20,
        /// handle = the CompletionRing*, a = its CqRef: the duty side unlinks
        /// it from the registry and frees it (§3.3 case (a)).
        CqRelease       = 21,
        /// handle = heap std::function holder of the Client callback chosen by
        /// a (0 onConnected, 1 onDisconnected, 2 onError); ownership passes to
        /// the duty thread, which installs it and frees the holder (C3).
        ClientSetCallback = 22,

        // 30-37 retired (Phase 2b F: the hub commands of the removed separate transactional handle; transactions are Producer +
        // transactionalId, their control ops travel the producer ring) - never reuse.

        // ---- Producer (F8c C6a) -------------------------------------------------
        /// handle = producer::ProducerImpl* (kept alive by the hub / the issuing
        /// handle); a = Token, b / c = the completion target (encodeTarget).  The
        /// issuer already closed the intake (tail bit): the duty side drains.
        ProducerBeginClose = 40,
        /// handle = producer::ProducerImpl*: closeImmediately (no completion of
        /// its own; a pending begin-close token completes Closed).
        ProducerCloseNow   = 41,

        // ---- Handle release (F8c C7, spec §3.3) ---------------------------------
        /// handle = the impl, a = HandleKind.  The handle gives its share of
        /// the impl to the duty side: the duty side first applies the handle's
        /// destructor semantics (Producer:
        /// closeImmediately unless a close is already in progress;
        /// Subscription: unsubscribe; GroupConsumer: close), then — once
        /// nothing on the duty side references the impl any more — detaches
        /// its side of the impl's ownership word.  Whichever of the two sides
        /// detaches LAST frees the impl (one RMW each, nobody waits).
        HandleRelease   = 50,
        /// F8c C7: Client::close() — begin-close every producer;
        /// a = Token, b / c = target; completes Ok once
        /// every one of them settled (each bounded by its own deadline).
        ClientDrain     = 51,
    };

    Op       op     = Op::SubscribeAdd;
    uint64_t handle = 0;
    int64_t  a      = 0;
    int64_t  b      = 0;
    int64_t  c      = 0;  ///< F8c C4: second completion-target word
    int64_t  d      = 0;  ///< F8c C5: op payload (a heap holder for txn sends / offsets)
    int64_t  e      = 0;  ///< Phase 2b F (Task 10): second payload word (GroupSeek's partition)
};

/// F8c C7: which impl type a HandleRelease carries.
enum class HandleKind : int64_t {
    Producer      = 1,
    // 2 retired (Phase 2b F: TxnProducer) - never reuse.
    Subscription  = 3,
    GroupConsumer = 4,
};

/// F8c C7 test seam: called by every handle impl's destructor (Producer,
/// Subscription and GroupConsumer impls) — counts and
/// attributes frees.  Set it before any thread that may free an impl runs and
/// clear it after they are joined (never written while another thread reads
/// it).  nullptr in production.
inline void (*testOnImplFree)(HandleKind kind, const void* impl) noexcept = nullptr;

/// Result of handing a HubCommand to a sink.
enum class HubSubmit : uint8_t {
    /// On the command ring: the duty thread (or Client::close()'s final drain)
    /// will apply it.  Ownership of any heap payload passed to the duty thread.
    Accepted,
    /// NOT submitted (the Client is already closed).  The caller still owns
    /// any heap payload it put in the command and must free/fail it.
    /// F8c C2: linearizable — the agent command ring's closed bit lives in its
    /// tail word, so a command is either Accepted (before the close: drained
    /// by close()) or Closed (never written).  F8a's indeterminate
    /// ClosedAfterSubmit outcome (and its accepted leak) no longer exists.
    Closed,
    /// F8c C7 fix round 2 (F1) — HandleRelease only: the command ring was full,
    /// so the sink handed the handle's share over WITHOUT a command: it marked
    /// the impl's release orphaned (fenced at the current ring tail), detached
    /// the handle's share (freeing the impl if the duty side had already let
    /// go) and raised the ring's orphan hint; the duty side discovers it and
    /// applies the release in FIFO order.  The caller must not touch the impl.
    Orphaned,
};

/// F8a fix4 M1: verdict of a duty-thread DIRECT send (Subscribe /
/// Unsubscribe / OffsetCommit / LeaveGroup straight to the transport).
///   Accepted — on the wire.
///   Retry    — transiently refused (back-pressure, not connected, admin
///              action): the owner keeps it and retries next duty cycle.
///   Fatal    — the transport can never carry it (publication closed, max
///              position, frame over the max message length): the owner
///              drops it and fails / finishes the request — never re-offered.
enum class SendResult : uint8_t { Accepted, Retry, Fatal };

/// Lifecycle ops: delivery is guaranteed (applied directly on the duty thread;
/// otherwise a full ring is waited out on duty-thread progress) — never throw
/// for a full ring.
[[nodiscard]] constexpr bool isLifecycle(HubCommand::Op op) noexcept {
    return op == HubCommand::Op::Unsubscribe || op == HubCommand::Op::GroupClose ||
           op == HubCommand::Op::CqRelease || op == HubCommand::Op::ProducerBeginClose ||
           op == HubCommand::Op::ProducerCloseNow ||
           op == HubCommand::Op::HandleRelease;
}

/// Plain function pointer + opaque context (no std::function, no virtual):
/// set once when the handle is created, invoked on the caller's thread.
/// Non-lifecycle ops may throw (ring full past the bounded wait / cannot pump).
using HubCommandSink = HubSubmit (*)(void* ctx, const HubCommand& cmd);

} // namespace rune::client::detail
