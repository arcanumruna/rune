// src/subscriber/Subscription.cpp
//
// CB-4 — Subscription: RAII handle implementation.
// F8a — shared-nothing: the handle owns only app-side state; unsubscribe is a
// command on the agent ring, and position/subscriptionId/liveness arrive via
// the subscription's status ring.

#include <rune/client/subscriber/Subscription.h>

#include <cstdio>
#include <exception>
#include <stdexcept>
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
    , pausable_(other.pausable_)
    , paused_(other.paused_)
    , appSeq_(other.appSeq_)
    , cache_(other.cache_)
{
    other.paused_ = false;   // moved-from: impl_ is null, but leave no stale view behind
    other.appSeq_ = 0;
    other.cache_  = SubscriptionStatus{};
}

Subscription& Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        release_();  // the current subscription: unsubscribed + released
        impl_         = std::exchange(other.impl_, nullptr);
        sink_         = other.sink_;
        sinkCtx_      = other.sinkCtx_;
        dutyThread_   = other.dutyThread_;
        unsubscribed_ = other.unsubscribed_;
        pausable_     = other.pausable_;
        paused_       = other.paused_;
        appSeq_       = other.appSeq_;
        cache_        = other.cache_;
        other.paused_ = false;
        other.appSeq_ = 0;
        other.cache_  = SubscriptionStatus{};
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
    if (!impl_) return;
    (void)impl_->status.drainLatest(cache_);
    // The hub echoes the number of the last command of this handle it applied.  Once it equals the number of commands sent,
    // every one of them is reflected and the snapshot's `paused` is the truth (it includes a pause or resume made on the
    // duty thread, e.g. inside a handler); before that, a snapshot may pre-date a command still in flight: this handle's
    // own view stands.
    if (cache_.appSeqApplied == appSeq_) paused_ = cache_.paused;
}

int64_t Subscription::position() const {
    if (impl_ && onDutyThread_()) return impl_->nextOffset;
    drainStatus_();
    return cache_.nextOffset;
}

bool Subscription::active() const {
    if (!impl_) return false;
    if (onDutyThread_()) return impl_->active && !impl_->paused;
    drainStatus_();
    if (unsubscribed_ || paused_) return false;
    return !impl_->status.closed();
}

bool Subscription::paused() const {
    if (!impl_) return false;
    if (onDutyThread_()) return impl_->paused;
    drainStatus_();
    return paused_ && !unsubscribed_ && !impl_->status.closed();
}

// ---------------------------------------------------------------------------
// pause() / resume() - lifecycle commands (guaranteed delivery, like unsubscribe())
// ---------------------------------------------------------------------------

bool Subscription::pause(int64_t resumeAt) {
    if (!impl_ || sink_ == nullptr || !pausable_) return false;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::SubscribePause;
    cmd.handle = reinterpret_cast<uint64_t>(impl_);   // identity only, as for unsubscribe()
    cmd.a      = resumeAt;
    if (onDutyThread_()) {
        // A callback on the duty thread (Thread mode): the sink applies it on this thread and returns the hub's verdict.
        // No field of this handle is touched (the application thread owns them); it learns the result from the status.
        return sink_(sinkCtx_, cmd) == detail::HubSubmit::Applied;
    }
    // The application thread - or the Invoker owner, which is also the duty thread: send the command with this handle's
    // number.  An Invoker owner's sink applies it at once and answers with the hub's verdict; elsewhere it is queued.
    if (unsubscribed_ || impl_->status.closed()) return false;
    drainStatus_();
    const uint32_t seq = appSeq_ + 1;
    cmd.b = seq;
    // A position above the last snapshot is only decidable by the thread that owns the hub: ask the sink to apply it
    // directly (it does nothing when the caller is not that thread).
    const bool onlyIfDirect = resumeAt >= 0 && resumeAt > cache_.nextOffset;
    cmd.c = onlyIfDirect ? 1 : 0;
    switch (sink_(sinkCtx_, cmd)) {
    case detail::HubSubmit::Applied:
        break;
    case detail::HubSubmit::Accepted:
        if (onlyIfDirect) return false;   // not decided here: the stale-snapshot bound refuses it, as before
        break;
    default:   // Refused, Closed, Orphaned
        return false;
    }
    appSeq_ = seq;
    paused_ = true;
    return true;
}

bool Subscription::resume() {
    if (!impl_ || sink_ == nullptr || !pausable_) return false;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::SubscribeResume;
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
    if (onDutyThread_()) return sink_(sinkCtx_, cmd) == detail::HubSubmit::Applied;
    drainStatus_();
    if (unsubscribed_ || !paused_ || impl_->status.closed()) return false;
    const uint32_t seq = appSeq_ + 1;
    cmd.b = seq;
    const detail::HubSubmit r = sink_(sinkCtx_, cmd);
    if (r != detail::HubSubmit::Applied && r != detail::HubSubmit::Accepted) return false;
    appSeq_ = seq;
    paused_ = false;
    return true;
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

uint64_t Subscription::batchBytes() const {
    if (impl_ && onDutyThread_()) return impl_->batchBytes;
    drainStatus_();
    return cache_.batchBytes;
}

uint16_t Subscription::brokerErrorCode() const {
    if (impl_ && onDutyThread_()) return impl_->brokerErrorCode;
    drainStatus_();
    return cache_.brokerErrorCode;
}

// ---------------------------------------------------------------------------
// SubscriptionSet (Phase 2c)
// ---------------------------------------------------------------------------

Subscription& SubscriptionSet::operator[](int32_t partition) {
    if (partition < 0 || static_cast<std::size_t>(partition) >= subs_.size())
        throw std::out_of_range("SubscriptionSet: partition out of range");
    return subs_[static_cast<std::size_t>(partition)];
}

int64_t SubscriptionSet::position(int32_t partition) const {
    if (partition < 0 || static_cast<std::size_t>(partition) >= subs_.size())
        throw std::out_of_range("SubscriptionSet: partition out of range");
    return subs_[static_cast<std::size_t>(partition)].position();
}

bool SubscriptionSet::active() const {
    for (const auto& s : subs_) if (s.active()) return true;
    return false;
}

bool SubscriptionSet::finished() const {
    for (const auto& s : subs_) if (!s.finished()) return false;
    return true;
}

uint64_t SubscriptionSet::batchBytes() const {
    uint64_t sum = 0;
    for (const auto& s : subs_) sum += s.batchBytes();
    return sum;
}

void SubscriptionSet::unsubscribe() {
    for (auto& s : subs_) s.unsubscribe();
}

} // namespace rune::client
