// src/group/GroupConsumer.cpp
//
// CG-5 — GroupConsumer handle implementation.
//
// The handle is a thin wrapper around GroupConsumerImpl (shared_ptr).
// All hot-path work (state machine, records routing) runs in GroupConsumerHub
// on the runner thread.  F8a: every handle operation is a HubCommand for the
// duty thread — the handle touches no duty-owned state.

#include <rune/client/group/GroupConsumer.h>

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <stdexcept>

namespace rune::client {

// ---------------------------------------------------------------------------
// GroupConsumer — constructor / destructor / move
// ---------------------------------------------------------------------------

GroupConsumer::GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl,
                             detail::HubCommandSink             sink,
                             void*                              sinkCtx) noexcept
    : impl_(std::move(impl))
    , sink_(sink)
    , sinkCtx_(sinkCtx)
{}

GroupConsumer::~GroupConsumer() {
    if (!moved_ && impl_) {
        close();
    }
}

GroupConsumer::GroupConsumer(GroupConsumer&& o) noexcept
    : impl_(std::move(o.impl_))
    , sink_(o.sink_)
    , sinkCtx_(o.sinkCtx_)
    , moved_(o.moved_)
    , closed_(o.closed_)
{
    o.moved_ = true;
}

GroupConsumer& GroupConsumer::operator=(GroupConsumer&& o) noexcept {
    if (this != &o) {
        if (!moved_ && impl_) close();
        impl_    = std::move(o.impl_);
        sink_    = o.sink_;
        sinkCtx_ = o.sinkCtx_;
        moved_   = o.moved_;
        closed_  = o.closed_;
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
// submit_ — hand one command to the duty thread
// ---------------------------------------------------------------------------

detail::HubSubmit GroupConsumer::submit_(const detail::HubCommand& cmd) {
    // Finished on the duty side (closed / Client closed): the Client may be
    // gone — never touch the sink again.
    if (!impl_ || sink_ == nullptr || impl_->status.closed()) {
        return detail::HubSubmit::Closed;
    }
    return sink_(sinkCtx_, cmd);
}

// ---------------------------------------------------------------------------
// commit() — a command for the duty thread
// ---------------------------------------------------------------------------

void GroupConsumer::commit() {
    assert(impl_);
    if (closed_) return;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupCommit;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    (void)submit_(cmd);  // Closed: nothing to commit any more
}

// ---------------------------------------------------------------------------
// commitSync() — the duty thread owns + resolves the promise
// ---------------------------------------------------------------------------

std::future<void> GroupConsumer::commitSync() {
    assert(impl_);

    auto failed = [](const char* why) {
        std::promise<void> p;
        p.set_exception(std::make_exception_ptr(std::runtime_error(why)));
        return p.get_future();
    };
    if (closed_) return failed("GroupConsumer closed");

    auto prom = std::make_unique<std::promise<void>>();
    auto fut  = prom->get_future();

    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupCommitSync;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    cmd.a      = static_cast<int64_t>(reinterpret_cast<uintptr_t>(prom.get()));

    switch (submit_(cmd)) {  // may throw (ring full past the bounded wait): prom frees itself
    case detail::HubSubmit::Accepted:
        (void)prom.release();  // the duty thread (or close()'s drain) owns it now
        return fut;
    case detail::HubSubmit::Closed:
        return failed(impl_->status.closed() ? "GroupConsumer closed" : "Client closed");
    case detail::HubSubmit::ClosedAfterSubmit:
        // Racing Client::close(): the promise may already be adopted (and then
        // failed by closeAll) or be stranded in the ring — ownership is
        // indeterminate, so leak it and hand back a future that is failed now.
        (void)prom.release();
        return failed("Client closed");
    }
    return failed("Client closed");  // unreachable
}

// ---------------------------------------------------------------------------
// close()
// ---------------------------------------------------------------------------

void GroupConsumer::close() {
    if (!impl_ || closed_) return;
    closed_ = true;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupClose;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    try {
        (void)submit_(cmd);
    } catch (const std::exception& e) {
        std::fprintf(stderr,
            "rune-client: GroupConsumer::close: command not delivered: %s "
            "(Client::close() deactivates the consumer)\n", e.what());
    } catch (...) {
        std::fprintf(stderr,
            "rune-client: GroupConsumer::close: command not delivered "
            "(Client::close() deactivates the consumer)\n");
    }
}

// ---------------------------------------------------------------------------
// active()
// ---------------------------------------------------------------------------

bool GroupConsumer::active() const noexcept {
    return impl_ && !moved_ && !closed_ && !impl_->status.closed();
}

// ---------------------------------------------------------------------------
// generation() — runner-thread read (see header threading note)
// ---------------------------------------------------------------------------

int32_t GroupConsumer::generation() const noexcept {
    return impl_ ? impl_->generationId : 0;
}

} // namespace rune::client
