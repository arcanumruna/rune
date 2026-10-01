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

/// Plain function pointer + opaque context (no std::function, no virtual):
/// set once when the handle is created, invoked on the caller's (app) thread.
/// May throw (ring full and the bounded wait timed out / cannot pump).
using HubCommandSink = HubSubmit (*)(void* ctx, const HubCommand& cmd);

} // namespace rune::client::detail
