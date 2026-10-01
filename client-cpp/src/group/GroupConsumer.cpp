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
#include <memory>
#include <stdexcept>
#include <string>

namespace rune::client {

// ---------------------------------------------------------------------------
// GroupConsumer — constructor / destructor / move
// ---------------------------------------------------------------------------

GroupConsumer::GroupConsumer(std::shared_ptr<GroupConsumerImpl> impl,
                             detail::HubCommandSink             sink,
                             void*                              sinkCtx,
                             std::thread::id                    dutyThread,
                             detail::TargetedOpSink             opSink,
                             detail::AndWaitSink                waitSink) noexcept
    : impl_(std::move(impl))
    , sink_(sink)
    , sinkCtx_(sinkCtx)
    , opSink_(opSink)
    , waitSink_(waitSink)
    , dutyThread_(dutyThread)
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
    , opSink_(o.opSink_)
    , waitSink_(o.waitSink_)
    , dutyThread_(o.dutyThread_)
    , moved_(o.moved_)
    , closed_(o.closed_)
    , cache_(o.cache_)
{
    o.moved_ = true;
}

GroupConsumer& GroupConsumer::operator=(GroupConsumer&& o) noexcept {
    if (this != &o) {
        if (!moved_ && impl_) close();
        impl_       = std::move(o.impl_);
        sink_       = o.sink_;
        sinkCtx_    = o.sinkCtx_;
        opSink_     = o.opSink_;
        waitSink_   = o.waitSink_;
        dutyThread_ = o.dutyThread_;
        moved_      = o.moved_;
        closed_     = o.closed_;
        cache_      = o.cache_;
        o.moved_    = true;
    }
    return *this;
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
// Callback registration — a GroupSetCallback command (fix1 I1): the duty
// thread installs the callback; no app-thread write to duty-owned state.
// ---------------------------------------------------------------------------

template <class Fn>
void GroupConsumer::setCallback_(int which, Fn cb) {
    assert(impl_);
    auto holder = std::make_unique<Fn>(std::move(cb));
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupSetCallback;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    cmd.a      = which;
    cmd.b      = static_cast<int64_t>(reinterpret_cast<uintptr_t>(holder.get()));
    switch (submit_(cmd)) {  // throws: holder frees itself
    case detail::HubSubmit::Accepted:
        (void)holder.release();
        break;
    case detail::HubSubmit::Closed:
        break;  // never submitted: holder frees itself
    }
}

void GroupConsumer::onAssigned(AssignmentCallback cb) { setCallback_(0, std::move(cb)); }
void GroupConsumer::onRevoked(AssignmentCallback cb)  { setCallback_(1, std::move(cb)); }
void GroupConsumer::onRecords(RecordsCallback cb)     { setCallback_(2, std::move(cb)); }

// ---------------------------------------------------------------------------
// commit() — a command for the duty thread
// ---------------------------------------------------------------------------

void GroupConsumer::commit() {
    assert(impl_);
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupCommit;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    if (onDutyThread_()) {  // inside a callback: applied directly by the sink
        (void)submit_(cmd);
        return;
    }
    if (closed_) return;
    (void)submit_(cmd);  // Closed: nothing to commit any more
}

// ---------------------------------------------------------------------------
// commitSync(target, token) — F8c C4: a token op, completed exactly once by the
// duty thread (or close()).  No promise, no future.
// ---------------------------------------------------------------------------

OpResult GroupConsumer::commitSync(CompletionTarget target, Token token) {
    // Finished on the duty side (closed / Client closed): the Client may be
    // gone — never touch the sink again.
    if (!impl_ || moved_ || opSink_ == nullptr || impl_->status.closed()) return OpResult::Closed;
    if (!onDutyThread_() && closed_) return OpResult::Closed;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupCommitSync;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    return opSink_(sinkCtx_, cmd, target, token, OpKind::CommitSync);
}

OpResult GroupConsumer::issueCommitSync_(void* self, const CompletionTarget& target, Token token) {
    return static_cast<GroupConsumer*>(self)->commitSync(target, token);
}

void GroupConsumer::commitSyncAndWait(std::chrono::nanoseconds timeout) {
    if (!impl_ || moved_ || waitSink_ == nullptr || impl_->status.closed() || (!onDutyThread_() && closed_)) {
        throw std::runtime_error("commitSync(): GroupConsumer closed");
    }
    std::string text;
    const Completion c = waitSink_(sinkCtx_, "commitSync()", timeout, &GroupConsumer::issueCommitSync_, this, &text);
    switch (c.status) {
    case OpStatus::Ok:
        return;
    case OpStatus::SessionLost:
        throw SessionLostError("commitSync() aborted: the session to the broker was lost");
    case OpStatus::BrokerError:
        throw std::runtime_error("commitSync() failed: OffsetCommit errorCode " + std::to_string(c.errorCode));
    case OpStatus::Superseded:
        throw std::runtime_error("commitSync() superseded by a newer call");
    case OpStatus::Rejoined:
        throw std::runtime_error("commitSync() aborted: the group is rebalancing (rejoin)");
    case OpStatus::Timeout:
        throw std::runtime_error("commitSync() timed out");
    case OpStatus::NotConnected:
        throw std::runtime_error("commitSync() failed: the OffsetCommit frame can never be sent");
    case OpStatus::Closed:
        throw std::runtime_error("commitSync(): GroupConsumer closed");
    default:
        throw std::runtime_error("commitSync() failed (status " + std::to_string(static_cast<int>(c.status)) + ")");
    }
}

// ---------------------------------------------------------------------------
// close() — lifecycle: guaranteed delivery, never throws
// ---------------------------------------------------------------------------

void GroupConsumer::close() {
    if (!impl_ || moved_) return;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupClose;
    cmd.handle = reinterpret_cast<uint64_t>(impl_.get());
    if (!onDutyThread_()) {
        if (closed_) return;
        closed_ = true;
    }
    try {
        (void)submit_(cmd);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "rune-client: GroupConsumer::close: %s\n", e.what());
    } catch (...) {
        std::fprintf(stderr, "rune-client: GroupConsumer::close: unknown error\n");
    }
}

// ---------------------------------------------------------------------------
// active() / finished() / generation()
// ---------------------------------------------------------------------------

bool GroupConsumer::active() const noexcept {
    if (!impl_ || moved_) return false;
    if (onDutyThread_()) return impl_->active;
    return !closed_ && !impl_->status.closed();
}

bool GroupConsumer::finished() const noexcept {
    return !impl_ || impl_->status.closed();
}

int32_t GroupConsumer::generation() const noexcept {
    if (!impl_) return 0;
    if (onDutyThread_()) return impl_->generationId;
    (void)impl_->status.drainLatest(cache_);
    return cache_.generationId;
}

} // namespace rune::client
