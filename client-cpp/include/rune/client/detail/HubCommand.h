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
        /// handle = GroupConsumerImpl* (identity); a = heap std::promise<void>*
        /// (ownership passes to the duty thread, which resolves it).
        GroupCommitSync = 12,
        /// handle = GroupConsumerImpl* (identity).
        GroupClose      = 13,
        /// handle = GroupConsumerImpl* (identity); a = which callback
        /// (0 onAssigned, 1 onRevoked, 2 onRecords); b = heap std::function*
        /// of the matching type (ownership passes to the duty thread, which
        /// installs it and frees the holder).  Always via the ring (never
        /// applied directly: replacing a callback while it runs is UB).
        GroupSetCallback = 14,
    };

    Op       op     = Op::SubscribeAdd;
    uint64_t handle = 0;
    int64_t  a      = 0;
    int64_t  b      = 0;
};

/// Result of handing a HubCommand to a sink.
enum class HubSubmit : uint8_t {
    /// On the command ring: the duty thread (or Client::close()'s final drain)
    /// will apply it.  Ownership of any heap payload passed to the duty thread.
    Accepted,
    /// NOT submitted (the Client is already closed).  The caller still owns
    /// any heap payload it put in the command and must free/fail it.
    Closed,
    /// Submitted, but racing Client::close(): it may or may not have been
    /// drained.  Heap-payload ownership is INDETERMINATE — the caller must
    /// neither free it nor rely on it being applied (an accepted shutdown
    /// leak, same as Client::producerWithRingCap_'s fix3 M1 branch).
    ClosedAfterSubmit,
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
    return op == HubCommand::Op::Unsubscribe || op == HubCommand::Op::GroupClose;
}

/// Plain function pointer + opaque context (no std::function, no virtual):
/// set once when the handle is created, invoked on the caller's thread.
/// Non-lifecycle ops may throw (ring full past the bounded wait / cannot pump).
using HubCommandSink = HubSubmit (*)(void* ctx, const HubCommand& cmd);

} // namespace rune::client::detail
