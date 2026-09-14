// src/subscriber/Subscription.cpp
//
// CB-4 — Subscription: RAII handle implementation.

#include <rune/client/subscriber/Subscription.h>

#include <utility>

namespace rune::client {

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

Subscription::Subscription(std::shared_ptr<SubscriptionImpl> impl,
                            std::function<void(int64_t)>       unsubFn) noexcept
    : impl_(std::move(impl))
    , unsubFn_(std::move(unsubFn))
    , moved_(false)
{}

// ---------------------------------------------------------------------------
// Destructor — RAII unsubscribe
// ---------------------------------------------------------------------------

Subscription::~Subscription() {
    if (!moved_) {
        unsubscribe();
    }
}

// ---------------------------------------------------------------------------
// Move semantics
// ---------------------------------------------------------------------------

Subscription::Subscription(Subscription&& other) noexcept
    : impl_(std::move(other.impl_))
    , unsubFn_(std::move(other.unsubFn_))
    , moved_(other.moved_)
{
    other.moved_ = true; // prevent dtor from unsubscribing
}

Subscription& Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        // Unsubscribe the current subscription before overwriting.
        unsubscribe();

        impl_        = std::move(other.impl_);
        unsubFn_     = std::move(other.unsubFn_);
        moved_       = other.moved_;
        other.moved_ = true;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// unsubscribe() — idempotent, thread-safe
// ---------------------------------------------------------------------------

void Subscription::unsubscribe() {
    if (!impl_ || moved_) return;

    // Atomically mark inactive.  Only the first caller wins (CAS).
    bool expected = true;
    if (impl_->active.compare_exchange_strong(
            expected, false,
            std::memory_order_acq_rel, std::memory_order_relaxed))
    {
        // We won the race: send the Unsubscribe frame via the hub callback.
        // unsubFn_ receives the subscriptionId; it may be 0 if the
        // SubscribeResponse has not arrived yet (no-op on the broker side;
        // the hub will simply not find the id in its pending map).
        if (unsubFn_) {
            const int64_t sid = impl_->subscriptionId.load(std::memory_order_acquire);
            unsubFn_(sid);
        }
    }
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

int64_t Subscription::position() const noexcept {
    if (!impl_) return 0;
    return impl_->nextOffset.load(std::memory_order_acquire);
}

bool Subscription::active() const noexcept {
    if (!impl_) return false;
    return impl_->active.load(std::memory_order_acquire);
}

int64_t Subscription::subscriptionId() const noexcept {
    if (!impl_) return 0;
    return impl_->subscriptionId.load(std::memory_order_acquire);
}

} // namespace rune::client
