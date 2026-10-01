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
        /// handle = heap std::shared_ptr<GroupConsumerImpl>* (adopted + freed by
        /// the duty thread).
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

        // ---- TransactionalProducer (F8c C5) ------------------------------------
        // handle = TransactionalProducerImpl* (identity; resolved against the
        // Client's own list on the ring path, never dereferenced unless found).
        // Token ops: a = Token, b / c = completion target (encodeTarget).
        /// InitProducerId.
        TxnInit         = 30,
        /// beginTransaction (no completion).
        TxnBegin        = 31,
        /// d = heap producer::TxnSendRecord* (ownership passes; no completion).
        TxnSend         = 32,
        /// d = heap producer::TxnOffsetsPayload* (ownership passes).
        TxnSendOffsets  = 33,
        /// d = 1 commit / 0 abort.
        TxnEnd          = 34,

        // ---- Producer (F8c C6a) -------------------------------------------------
        /// handle = producer::ProducerImpl* (kept alive by the hub / the issuing
        /// handle); a = Token, b / c = the completion target (encodeTarget).  The
        /// issuer already closed the intake (tail bit): the duty side drains.
        ProducerBeginClose = 40,
        /// handle = producer::ProducerImpl*: closeImmediately (no completion of
        /// its own; a pending begin-close token completes Closed).
        ProducerCloseNow   = 41,
    };

    Op       op     = Op::SubscribeAdd;
    uint64_t handle = 0;
    int64_t  a      = 0;
    int64_t  b      = 0;
    int64_t  c      = 0;  ///< F8c C4: second completion-target word
    int64_t  d      = 0;  ///< F8c C5: op payload (a heap holder for txn sends / offsets)
};

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
           op == HubCommand::Op::ProducerCloseNow;
}

/// Plain function pointer + opaque context (no std::function, no virtual):
/// set once when the handle is created, invoked on the caller's thread.
/// Non-lifecycle ops may throw (ring full past the bounded wait / cannot pump).
using HubCommandSink = HubSubmit (*)(void* ctx, const HubCommand& cmd);

} // namespace rune::client::detail
