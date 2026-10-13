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

Subscription::Subscription(SubscriptionImpl*      impl,
                           detail::HubCommandSink sink,
                           void*                  sinkCtx,
                           std::thread::id        dutyThread) noexcept
    : impl_(impl)
    , sink_(sink)
    , sinkCtx_(sinkCtx)
    , dutyThread_(dutyThread)
{}

// ---------------------------------------------------------------------------
// Destructor — F8c C7 release (never throws, never waits for the duty side)
// ---------------------------------------------------------------------------

Subscription::~Subscription() { release_(); }

void Subscription::release_() noexcept {
    SubscriptionImpl* impl = std::exchange(impl_, nullptr);
    if (impl == nullptr) return;  // moved-from
    if (sink_ == nullptr) {
        // Standalone handle (no duty side was ever attached): ours alone.
        delete impl;
        return;
    }
    // The duty side already detached (Client close handed the impl back, or
    // it was never registered): never touch the Client — it may be gone.
    if (!impl->status.producerDetached()) {
        detail::HubCommand cmd;
        cmd.op     = detail::HubCommand::Op::HandleRelease;
        cmd.handle = reinterpret_cast<uint64_t>(impl);
        cmd.a      = static_cast<int64_t>(detail::HandleKind::Subscription);
        try {
            // Accepted: the duty side unsubscribes it and later detaches its
            // share.  Closed: a close() on another thread hands it back.
            // Orphaned: the sink handed the share over itself (F8c C7 F1).
            if (sink_(sinkCtx_, cmd) == detail::HubSubmit::Orphaned) return;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "rune-client: ~Subscription: release not delivered: %s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "rune-client: ~Subscription: release not delivered (unknown error)\n");
        }
    }
    // The handle's last access: the side that detaches last frees the impl.
    if (impl->status.detachConsumer()) delete impl;
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

Subscription::Subscription(Subscription&& other) noexcept
    : impl_(std::exchange(other.impl_, nullptr))
    , sink_(other.sink_)
    , sinkCtx_(other.sinkCtx_)
    , dutyThread_(other.dutyThread_)
    , unsubscribed_(other.unsubscribed_)
    , cache_(other.cache_)
{}

Subscription& Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        release_();  // the current subscription: unsubscribed + released
        impl_         = std::exchange(other.impl_, nullptr);
        sink_         = other.sink_;
        sinkCtx_      = other.sinkCtx_;
        dutyThread_   = other.dutyThread_;
        unsubscribed_ = other.unsubscribed_;
        cache_        = other.cache_;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// unsubscribe() — idempotent; a lifecycle command for the duty thread
// ---------------------------------------------------------------------------

void Subscription::unsubscribe() {
    if (!impl_ || sink_ == nullptr) return;

    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::Unsubscribe;
    // F8c C3: the identity is the impl ADDRESS (the Subscribe correlationId is
    // duty-assigned at adoption and never read by an app thread).  The duty
    // side only compares it against the impls its own tables hold — it never
    // dereferences it on the ring path (fix3 I3: the direct path may).
    cmd.handle = reinterpret_cast<uint64_t>(impl_);

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
    // ring, never a timeout).  Closed: the close path
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
    if (!impl_) return false;
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

SubscriptionFailReason Subscription::failReason() const {
    if (impl_ && onDutyThread_()) return impl_->failReason;
    drainStatus_();
    return cache_.failReason;
}

uint32_t Subscription::dataResets() const {
    if (impl_ && onDutyThread_()) return impl_->dataResets;
    drainStatus_();
    return cache_.dataResets;
}

uint16_t Subscription::brokerErrorCode() const {
    if (impl_ && onDutyThread_()) return impl_->brokerErrorCode;
    drainStatus_();
    return cache_.brokerErrorCode;
}

} // namespace rune::client
