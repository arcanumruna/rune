// src/group/GroupConsumer.cpp
//
// CG-5 — GroupConsumer handle implementation.
//
// The handle holds its share of GroupConsumerImpl (F8c C7: raw, §3.3 release).
// All hot-path work (state machine, records routing) runs in GroupConsumerHub
// on the runner thread.  F8a: every handle operation is a HubCommand for the
// duty thread — the handle touches no duty-owned state.

#include <rune/client/group/GroupConsumer.h>

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <span>
#include <string>
#include <utility>

namespace rune::client {

// ---------------------------------------------------------------------------
// GroupConsumer — constructor / destructor / move
// ---------------------------------------------------------------------------

GroupConsumer::GroupConsumer(GroupConsumerImpl*     impl,
                             detail::HubCommandSink sink,
                             void*                  sinkCtx,
                             std::thread::id        dutyThread,
                             detail::TargetedOpSink opSink,
                             detail::AndWaitSink    waitSink) noexcept
    : impl_(impl)
    , sink_(sink)
    , sinkCtx_(sinkCtx)
    , opSink_(opSink)
    , waitSink_(waitSink)
    , dutyThread_(dutyThread)
{}

GroupConsumer::~GroupConsumer() { release_(); }

void GroupConsumer::release_() noexcept {
    GroupConsumerImpl* impl = std::exchange(impl_, nullptr);
    if (impl == nullptr) return;  // moved-from
    if (sink_ == nullptr) {
        // Standalone handle (no duty side was ever attached): ours alone.
        delete impl;
        return;
    }
    // The duty side already detached (Client close handed it back, or it was
    // never registered): never touch the Client — it may be gone.
    if (!impl->status.producerDetached()) {
        detail::HubCommand cmd;
        cmd.op     = detail::HubCommand::Op::HandleRelease;
        cmd.handle = reinterpret_cast<uint64_t>(impl);
        cmd.a      = static_cast<int64_t>(detail::HandleKind::GroupConsumer);
        try {
            // Accepted: the duty side closes it and later detaches its share.
            // Closed: a close() on another thread hands it back.
            // Orphaned: the sink handed the share over itself (F8c C7 F1).
            if (sink_(sinkCtx_, cmd) == detail::HubSubmit::Orphaned) return;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "rune-client: ~GroupConsumer: release not delivered: %s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "rune-client: ~GroupConsumer: release not delivered (unknown error)\n");
        }
    }
    // The handle's last access: the side that detaches last frees the impl.
    if (impl->status.detachConsumer()) delete impl;
}

GroupConsumer::GroupConsumer(GroupConsumer&& o) noexcept
    : impl_(std::exchange(o.impl_, nullptr))
    , sink_(o.sink_)
    , sinkCtx_(o.sinkCtx_)
    , opSink_(o.opSink_)
    , waitSink_(o.waitSink_)
    , dutyThread_(o.dutyThread_)
    , closed_(o.closed_)
    , cache_(o.cache_)
{}

GroupConsumer& GroupConsumer::operator=(GroupConsumer&& o) noexcept {
    if (this != &o) {
        release_();  // the current consumer: closed + released
        impl_       = std::exchange(o.impl_, nullptr);
        sink_       = o.sink_;
        sinkCtx_    = o.sinkCtx_;
        opSink_     = o.opSink_;
        waitSink_   = o.waitSink_;
        dutyThread_ = o.dutyThread_;
        closed_     = o.closed_;
        cache_      = o.cache_;
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
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
    cmd.a      = which;
    cmd.b      = static_cast<int64_t>(reinterpret_cast<uintptr_t>(holder.get()));
    switch (submit_(cmd)) {  // throws: holder frees itself
    case detail::HubSubmit::Accepted:
        (void)holder.release();
        break;
    case detail::HubSubmit::Closed:
    case detail::HubSubmit::Orphaned:  // HandleRelease only
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
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
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
    if (!impl_ || opSink_ == nullptr || impl_->status.closed()) return OpResult::Closed;
    if (!onDutyThread_() && closed_) return OpResult::Closed;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupCommitSync;
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
    return opSink_(sinkCtx_, cmd, target, token, OpKind::CommitSync);
}

OpResult GroupConsumer::issueCommitSync_(void* self, const CompletionTarget& target, Token token) {
    return static_cast<GroupConsumer*>(self)->commitSync(target, token);
}

void GroupConsumer::commitSyncAndWait(std::chrono::nanoseconds timeout) {
    if (!impl_ || waitSink_ == nullptr || impl_->status.closed() || (!onDutyThread_() && closed_)) {
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
// seekToCommitted / seek — Phase 2b F (Task 10): token ops, completed exactly
// once by the duty thread (FD8).  Issued exactly like commitSync.
// ---------------------------------------------------------------------------

OpResult GroupConsumer::seekToCommitted(CompletionTarget target, Token token) {
    if (!impl_ || opSink_ == nullptr || impl_->status.closed()) return OpResult::Closed;
    if (!onDutyThread_() && closed_) return OpResult::Closed;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupSeekToCommitted;
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
    return opSink_(sinkCtx_, cmd, target, token, OpKind::SeekToCommitted);
}

OpResult GroupConsumer::issueSeekToCommitted_(void* self, const CompletionTarget& target, Token token) {
    return static_cast<GroupConsumer*>(self)->seekToCommitted(target, token);
}

namespace {
/// Rejects every wait outcome but Ok with the commitSyncAndWait-style exception.
[[noreturn]] void throwRewind(const char* what, const Completion& c, const std::string& text) {
    switch (c.status) {
    case OpStatus::InvalidArgument:
        throw std::invalid_argument(std::string(what) + " failed: " + text);
    case OpStatus::Superseded:
        throw std::runtime_error(std::string(what) + " superseded by a newer rewind");
    case OpStatus::Rejoined:
        throw std::runtime_error(std::string(what) + " aborted: the group is rebalancing (rejoin)");
    case OpStatus::SessionLost:
        throw SessionLostError(std::string(what) + " aborted: the session to the broker was lost");
    case OpStatus::Timeout:
        throw std::runtime_error(std::string(what) + " timed out");
    case OpStatus::Closed:
        throw std::runtime_error(std::string(what) + ": GroupConsumer closed");
    default:
        throw std::runtime_error(std::string(what) + " failed (status " +
                                 std::to_string(static_cast<int>(c.status)) + ")");
    }
}
} // namespace

void GroupConsumer::seekToCommittedAndWait(std::chrono::nanoseconds timeout) {
    if (!impl_ || waitSink_ == nullptr || impl_->status.closed() || (!onDutyThread_() && closed_)) {
        throw std::runtime_error("seekToCommitted(): GroupConsumer closed");
    }
    std::string text;
    const Completion c = waitSink_(sinkCtx_, "seekToCommitted()", timeout, &GroupConsumer::issueSeekToCommitted_,
                                   this, &text);
    if (c.status != OpStatus::Ok) throwRewind("seekToCommitted()", c, text);
}

struct GroupConsumer::SeekIssue {
    GroupConsumer*  self;
    TopicPartition  tp;
    int64_t         offset;
};

OpResult GroupConsumer::seek(TopicPartition tp, int64_t offset, CompletionTarget target, Token token) {
    if (!impl_ || opSink_ == nullptr || impl_->status.closed()) return OpResult::Closed;
    if (!onDutyThread_() && closed_) return OpResult::Closed;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupSeek;
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
    cmd.d      = offset;
    cmd.e      = static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(tp.topicId)) << 32) |
                                      static_cast<uint32_t>(tp.partition));
    return opSink_(sinkCtx_, cmd, target, token, OpKind::Seek);
}

OpResult GroupConsumer::issueSeek_(void* ctx, const CompletionTarget& target, Token token) {
    auto* si = static_cast<SeekIssue*>(ctx);
    return si->self->seek(si->tp, si->offset, target, token);
}

void GroupConsumer::seek(TopicPartition tp, int64_t offset) {
    // G6: fire-and-forget — the internal handler drops the COMPLETION; a refusal at submit throws.
    if (offset < 0) throw std::invalid_argument("seek(): offset must be >= 0");
    static constexpr CompletionHandler kDrop{
        [](void*, const Completion&, std::span<const uint8_t>) noexcept {}, nullptr};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (;;) {
        switch (seek(tp, offset, CompletionTarget(kDrop), Token{0})) {
        case OpResult::Accepted:
            return;
        case OpResult::BackPressured:
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("seek(): not delivered, the command ring stayed full (BackPressured)");
            }
            std::this_thread::yield();   // the duty thread drains the ring
            continue;
        case OpResult::Closed:
            throw std::runtime_error("seek(): GroupConsumer closed (Closed)");
        case OpResult::WrongThread:
            throw std::runtime_error("seek(): called off the owner thread (WrongThread)");
        case OpResult::InvalidArgument:
        case OpResult::QueueFull:
            throw std::runtime_error("seek(): refused at submit (InvalidArgument/QueueFull)");
        }
    }
}

void GroupConsumer::seekAndWait(TopicPartition tp, int64_t offset, std::chrono::nanoseconds timeout) {
    if (!impl_ || waitSink_ == nullptr || impl_->status.closed() || (!onDutyThread_() && closed_)) {
        throw std::runtime_error("seek(): GroupConsumer closed");
    }
    SeekIssue si{this, tp, offset};
    std::string text;
    const Completion c = waitSink_(sinkCtx_, "seek()", timeout, &GroupConsumer::issueSeek_, &si, &text);
    if (c.status != OpStatus::Ok) throwRewind("seek()", c, text);
}

// ---------------------------------------------------------------------------
// close() — lifecycle: guaranteed delivery, never throws
// ---------------------------------------------------------------------------

void GroupConsumer::close() {
    if (!impl_) return;
    detail::HubCommand cmd;
    cmd.op     = detail::HubCommand::Op::GroupClose;
    cmd.handle = reinterpret_cast<uint64_t>(impl_);
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
    if (!impl_) return false;
    if (onDutyThread_()) return impl_->active;
    return !closed_ && !impl_->status.closed();
}

bool GroupConsumer::finished() const noexcept {
    return !impl_ || impl_->status.closed();
}

GroupFailReason GroupConsumer::failReason() const noexcept {
    if (impl_ == nullptr) return GroupFailReason::None;
    if (onDutyThread_()) return impl_->failReason;
    (void)impl_->status.drainLatest(cache_);
    return static_cast<GroupFailReason>(cache_.reserved);
}

int32_t GroupConsumer::generation() const noexcept {
    if (!impl_) return 0;
    if (onDutyThread_()) return impl_->generationId;
    (void)impl_->status.drainLatest(cache_);
    return cache_.generationId;
}

} // namespace rune::client
