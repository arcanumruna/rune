// src/group/GroupConsumer.cpp
//
// CG-5 — GroupConsumer handle implementation.
//
// The handle is a thin wrapper around GroupConsumerImpl (shared_ptr).
// All hot-path work (state machine, records routing) runs in GroupConsumerHub
// on the runner thread.

#include <rune/client/group/GroupConsumer.h>

#include <cassert>
#include <stdexcept>

namespace rune::client {

// ---------------------------------------------------------------------------
// GroupConsumer — constructor / destructor / move
// ---------------------------------------------------------------------------

GroupConsumer::GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl) noexcept
    : impl_(std::move(impl))
{}

GroupConsumer::~GroupConsumer() {
    if (!moved_ && impl_) {
        close();
    }
}

GroupConsumer::GroupConsumer(GroupConsumer&& o) noexcept
    : impl_(std::move(o.impl_))
    , moved_(o.moved_)
{
    o.moved_ = true;
}

GroupConsumer& GroupConsumer::operator=(GroupConsumer&& o) noexcept {
    if (this != &o) {
        if (!moved_ && impl_) close();
        impl_    = std::move(o.impl_);
        moved_   = o.moved_;
        o.moved_ = true;
    }
    return *this;
}

// ---------------------------------------------------------------------------
// Callback registration — SETUP PHASE (before connect())
// ---------------------------------------------------------------------------

void GroupConsumer::onAssigned(std::function<void(std::vector<TopicPartition>)> cb) {
    assert(impl_);
    impl_->cbAssigned = std::move(cb);
}

void GroupConsumer::onRevoked(std::function<void(std::vector<TopicPartition>)> cb) {
    assert(impl_);
    impl_->cbRevoked = std::move(cb);
}

void GroupConsumer::onRecords(std::function<void(const RecordSet&)> cb) {
    assert(impl_);
    impl_->cbRecords = std::move(cb);
}

// ---------------------------------------------------------------------------
// commit() — wait-free from app thread
// ---------------------------------------------------------------------------

void GroupConsumer::commit() {
    assert(impl_);
    // Release store: runner thread does an acquire load to pick this up.
    impl_->commitRequested.store(true, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// commitSync() — cold path; runner resolves the promise after OffsetCommitResponse
// ---------------------------------------------------------------------------

std::future<void> GroupConsumer::commitSync() {
    assert(impl_);

    std::promise<void> prom;
    auto fut = prom.get_future();

    {
        std::lock_guard<std::mutex> lk(impl_->commitSyncMtx);
        if (impl_->commitSyncPending) {
            // Already one in flight — fail the old one and replace.
            try {
                impl_->commitSyncPromise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("commitSync() superseded by a newer call")));
            } catch (...) {}
        }
        impl_->commitSyncPromise = std::move(prom);
        impl_->commitSyncPending = true;
    }

    // Also set the fast-path flag so the runner sends the commit promptly.
    impl_->commitRequested.store(true, std::memory_order_release);

    return fut;
}

// ---------------------------------------------------------------------------
// close()
// ---------------------------------------------------------------------------

void GroupConsumer::close() {
    if (impl_) {
        impl_->active.store(false, std::memory_order_release);
        // Resolve any pending commitSync promise with an error.
        std::lock_guard<std::mutex> lk(impl_->commitSyncMtx);
        if (impl_->commitSyncPending) {
            try {
                impl_->commitSyncPromise.set_exception(
                    std::make_exception_ptr(
                        std::runtime_error("GroupConsumer closed")));
            } catch (...) {}
            impl_->commitSyncPending = false;
        }
    }
}

// ---------------------------------------------------------------------------
// active()
// ---------------------------------------------------------------------------

bool GroupConsumer::active() const noexcept {
    return impl_ && impl_->active.load(std::memory_order_acquire);
}

// ---------------------------------------------------------------------------
// generation() — runner-thread read (see header threading note)
// ---------------------------------------------------------------------------

int32_t GroupConsumer::generation() const noexcept {
    return impl_ ? impl_->generationId : 0;
}

} // namespace rune::client
