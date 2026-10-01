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
                           void*                             sinkCtx) noexcept
    : impl_(std::move(impl))
    , sink_(sink)
    , sinkCtx_(sinkCtx)
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
        moved_        = other.moved_;
        unsubscribed_ = other.unsubscribed_;
        cache_        = other.cache_;
        other.moved_  = true;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// unsubscribe() — idempotent; a command to the duty thread
// ---------------------------------------------------------------------------

void Subscription::unsubscribe() {
    if (!impl_ || moved_ || unsubscribed_) return;
    unsubscribed_ = true;

    // The duty side already finished this subscription (timed out, failed, or
    // the Client closed): nothing to undo, and the Client may be gone — never
    // touch the sink again.
    if (impl_->status.closed() || sink_ == nullptr) return;

    detail::HubCommand cmd;
    cmd.op = detail::HubCommand::Op::Unsubscribe;
    cmd.a  = impl_->correlationId;
    try {
        (void)sink_(sinkCtx_, cmd);  // Closed / ClosedAfterSubmit: the close path deactivates it
    } catch (...) {
        unsubscribed_ = false;  // not delivered: a retry may still unsubscribe
        throw;
    }
}

// ---------------------------------------------------------------------------
// Accessors — drain the status ring into the handle's own cache
// ---------------------------------------------------------------------------

void Subscription::drainStatus_() const {
    if (impl_) (void)impl_->status.drainLatest(cache_);
}

int64_t Subscription::position() const {
    drainStatus_();
    return cache_.nextOffset;
}

bool Subscription::active() const {
    if (!impl_ || moved_ || unsubscribed_) return false;
    return !impl_->status.closed();
}

int64_t Subscription::subscriptionId() const {
    drainStatus_();
    return cache_.subscriptionId;
}

} // namespace rune::client
