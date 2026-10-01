// src/subscriber/Subscription.cpp
//
// CB-4 — Subscription: RAII handle implementation.
// F8a — shared-nothing: the handle owns only app-side state; unsubscribe is a
// command on the agent ring, and position/subscriptionId/liveness arrive via
// the subscription's status ring.

#include <rune/client/subscriber/Subscription.h>

#include <cstdio>
#include <exception>
#include <utility>

namespace rune::client {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

Subscription::Subscription(std::shared_ptr<SubscriptionImpl> impl,
                           detail::HubCommandSink            sink,
                           void*                             sinkCtx,
                           std::thread::id                   dutyThread) noexcept
    : impl_(std::move(impl))
    , sink_(sink)
    , sinkCtx_(sinkCtx)
    , dutyThread_(dutyThread)
{}

// ---------------------------------------------------------------------------
// Destructor — RAII unsubscribe (never throws)
// ---------------------------------------------------------------------------

Subscription::~Subscription() {
    if (moved_) return;
    try {
        unsubscribe();
    } catch (const std::exception& e) {
        std::fprintf(stderr,
            "rune-client: ~Subscription: unsubscribe command not delivered: %s\n", e.what());
    } catch (...) {
        std::fprintf(stderr,
            "rune-client: ~Subscription: unsubscribe command not delivered (unknown error)\n");
    }
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

Subscription::Subscription(Subscription&& other) noexcept
    : impl_(std::move(other.impl_))
    , sink_(other.sink_)
    , sinkCtx_(other.sinkCtx_)
    , dutyThread_(other.dutyThread_)
    , moved_(other.moved_)
    , unsubscribed_(other.unsubscribed_)
    , cache_(other.cache_)
{
    other.moved_ = true; // prevent dtor from unsubscribing
}

Subscription& Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        // Unsubscribe the current subscription before overwriting.
        if (!moved_) {
            try { unsubscribe(); } catch (...) {
                std::fprintf(stderr,
                    "rune-client: Subscription move-assign: unsubscribe command not delivered\n");
            }
        }

        impl_         = std::move(other.impl_);
        sink_         = other.sink_;
        sinkCtx_      = other.sinkCtx_;
        dutyThread_   = other.dutyThread_;
        moved_        = other.moved_;
        unsubscribed_ = other.unsubscribed_;
        cache_        = other.cache_;
        other.moved_  = true;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// unsubscribe() — idempotent; a lifecycle command for the duty thread
// ---------------------------------------------------------------------------

void Subscription::unsubscribe() {
    if (!impl_ || moved_ || sink_ == nullptr) return;

    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::Unsubscribe;
    cmd.a  = impl_->correlationId;

    if (onDutyThread_()) {
        // Inside a callback (Thread mode): the sink applies it directly on this
        // thread, which owns the state.  The hub's unsubscribe is idempotent;
        // the handle's app-side flags are left alone (they are the owner's).
        (void)sink_(sinkCtx_, cmd);
        return;
    }

    if (unsubscribed_) return;
    unsubscribed_ = true;
    // The duty side already finished this subscription (timed out, failed, or
    // the Client closed): nothing to undo, and the Client may be gone — never
    // touch the sink again.
    if (impl_->status.closed()) return;
    // Lifecycle command: guaranteed delivery (progress-based wait on a full
    // ring, never a timeout).  Closed / ClosedAfterSubmit: the close path
    // finishes the subscription.
    (void)sink_(sinkCtx_, cmd);
}

// ---------------------------------------------------------------------------
// Accessors — duty path reads duty-owned fields; app path drains the status ring
// ---------------------------------------------------------------------------

void Subscription::drainStatus_() const {
    if (impl_) (void)impl_->status.drainLatest(cache_);
}

int64_t Subscription::position() const {
    if (impl_ && onDutyThread_()) return impl_->nextOffset;
    drainStatus_();
    return cache_.nextOffset;
}

bool Subscription::active() const {
    if (!impl_ || moved_) return false;
    if (onDutyThread_()) return impl_->active;
    if (unsubscribed_) return false;
    return !impl_->status.closed();
}

bool Subscription::finished() const noexcept {
    return !impl_ || impl_->status.closed();
}

int64_t Subscription::subscriptionId() const {
    if (impl_ && onDutyThread_()) return impl_->subscriptionId;
    drainStatus_();
    return cache_.subscriptionId;
}

} // namespace rune::client
