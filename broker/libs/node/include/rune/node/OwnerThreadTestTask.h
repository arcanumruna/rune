#pragma once
// OwnerThreadTestTask.h — F1: TEST-ONLY plumbing that runs a callable on the
// thread that OWNS a (topicId, partition) PartitionReplica (or on the control
// thread) and hands the result back to the test thread.
//
// WHY
// ===
// Integration tests used to fetch a raw PartitionReplica* (replicaForTest) and
// call append()/logEndOffset()/highWatermark()/isr()/log()->read() FROM THE TEST
// THREAD while the replica + its Log are owned by a worker thread.  append()
// pushes inline — i.e. offers on the per-worker single-writer Aeron
// ExclusivePublication concurrently with the worker — which corrupts/loses
// frames; highWatermark() even writes a mutable floor.  Rune's law is
// shared-nothing: every piece of mutable state has ONE owning thread and all
// cross-thread traffic travels over ring buffers.
//
// HOW (shared-nothing in production code)
// =======================================
//   test thread ──[NodeRuntimeAgent test-task SPSC ring]──▶ control thread
//       control thread looks the replica up in ITS OWN ReplicaRegistry, then
//         - internal partition (topicId < 0) or no WorkerPool: it IS the owner
//           → runs the task inline;
//         - user partition with a WorkerPool: forwards a WorkerOp::TestTask over
//           the EXISTING control→worker ingress ring (single producer = control
//           thread) → the owning worker runs it inside its duty cycle.
//
// Production code only ever moves an opaque pointer (the ring-carried ref) and
// calls runOwnerThreadTestTask().  The task object is shared-owned
// (std::shared_ptr) by the test thread and the ring-carried ref so neither side
// can dangle.
//
// F9a (no atomics, no mutex, no std::promise/future — user ruling 2026-10-01):
//   - The run-or-skip DECISION is made by the OWNER alone, from immutable data:
//     the deadline (submit time + timeout) is fixed before the ref is enqueued
//     (the ring publishes it).  Owner reaches the task at/after the deadline →
//     it SKIPS (never invokes the callable); before → it runs it.
//   - Every outcome travels to the test thread as an EVENT on the task's own
//     ControlRing (owner = sole producer, test = sole consumer):
//       Started   → posted BEFORE the callable runs;
//       Completed → posted AFTER the result / exception was stored in the task
//                   (the ring commit publishes those plain fields);
//       Skipped   → the deadline had passed; the callable never runs.
//
// Timeout safety: the test thread waits for Completed or Skipped with NO bound
// of its own (the ctest timeout is the only bound).  It therefore never unwinds
// while the callable could still run against its frame — there is no window in
// which a timed-out test returns and the owner starts the callable later.
// Precondition (BrokerNode::onOwnerThreadForTest): the node's control thread is
// running for the whole call (the calling test thread is started after start()
// and finishes before stop()).

#include <rune/consensus/ControlRing.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

namespace rune::replication { class PartitionReplica; }

namespace rune::node {

class OwnerThreadTestTask {
public:
    /// Where the task must run.
    enum class Target : std::uint8_t {
        Replica,        ///< the thread that owns (topicId, partition)
        ControlThread,  ///< the control (NodeRuntimeAgent) thread
    };

    using ExecFn = void (*)(OwnerThreadTestTask&,
                            rune::replication::PartitionReplica*) noexcept;

    using Clock = std::chrono::steady_clock;

    /// Owner → test events (on events_).
    static constexpr std::int32_t kStartedEvent   = 1;
    static constexpr std::int32_t kSkippedEvent   = 2;
    static constexpr std::int32_t kCompletedEvent = 3;

    OwnerThreadTestTask(Target target, std::int32_t topicId, std::int32_t partition,
                        ExecFn exec, Clock::time_point deadline) noexcept
        : exec_(exec), deadline_(deadline), topicId_(topicId), partition_(partition),
          target_(target) {}

    ~OwnerThreadTestTask() = default;  // shared_ptr (make_shared<Impl>) deletes via the concrete type
    OwnerThreadTestTask(const OwnerThreadTestTask&)            = delete;
    OwnerThreadTestTask& operator=(const OwnerThreadTestTask&) = delete;

    [[nodiscard]] Target       target()    const noexcept { return target_; }
    [[nodiscard]] std::int32_t topicId()   const noexcept { return topicId_; }
    [[nodiscard]] std::int32_t partition() const noexcept { return partition_; }
    [[nodiscard]] Clock::time_point deadline() const noexcept { return deadline_; }

    /// OWNER side: run the task if its deadline has not passed, else skip it.
    void runOnOwner(rune::replication::PartitionReplica* replica) noexcept {
        if (Clock::now() >= deadline_) {
            (void)events_.offer(kSkippedEvent);      // <= 3 events per task: never full
            return;                                   // callable never invoked
        }
        (void)events_.offer(kStartedEvent);
        exec_(*this, replica);                        // stores result / exception
        (void)events_.offer(kCompletedEvent);         // publishes them (ring commit)
    }

    /// OWNER side, shutdown: report the task as skipped without running it.
    void skipOnOwner() noexcept { (void)events_.offer(kSkippedEvent); }

    /// TEST side (sole consumer): drain events; returns the last terminal event
    /// seen (Completed / Skipped) or 0 if none yet.
    [[nodiscard]] std::int32_t pollTerminal() noexcept {
        std::int32_t t = 0;
        events_.poll([&t](std::int32_t type, std::int64_t, std::int64_t) noexcept {
            if (type == kCompletedEvent || type == kSkippedEvent) t = type;
        });
        return t;
    }

private:
    ExecFn                            exec_;
    const Clock::time_point           deadline_;   // immutable; published by the ring
    std::int32_t                      topicId_;
    std::int32_t                      partition_;
    Target                            target_;
    rune::consensus::ControlRing<256> events_;     // owner → test
};

/// The ring carries a raw pointer to a HEAP-allocated shared_ptr holder (one
/// reference owned by "the ring"); the owner releases it after running.
using OwnerThreadTestTaskRef = std::shared_ptr<OwnerThreadTestTask>;

/// Owner-side view of a ring-carried ref (topicId / partition / target routing).
[[nodiscard]] inline const OwnerThreadTestTask&
ownerThreadTestTaskOf(void* ref) noexcept {
    return **static_cast<OwnerThreadTestTaskRef*>(ref);
}

/// Owner side, shutdown: skip the ring-carried task and release the ring's ref.
inline void skipOwnerThreadTestTask(void* ref) noexcept {
    auto* holder = static_cast<OwnerThreadTestTaskRef*>(ref);
    (*holder)->skipOnOwner();
    delete holder;
}

/// Owner side: run the ring-carried task on THIS thread, then release the
/// ring's reference.  `replica` may be null (task reports "no replica").
inline void runOwnerThreadTestTask(void* ref,
                                   rune::replication::PartitionReplica* replica) noexcept {
    auto* holder = static_cast<OwnerThreadTestTaskRef*>(ref);
    (*holder)->runOnOwner(replica);
    delete holder;
}

namespace detail {

// Result of invoking F for a target: ControlThread → f(); Replica → f(PartitionReplica&)
// when F accepts a reference (missing replica = error), else f(PartitionReplica*)
// (nullptr passed through — for tests that tolerate a missing replica).
template <class F, OwnerThreadTestTask::Target T>
struct OwnerTaskResult;

template <class F>
struct OwnerTaskResult<F, OwnerThreadTestTask::Target::ControlThread> {
    using type = std::invoke_result_t<F&>;
};

template <class F>
struct OwnerTaskResult<F, OwnerThreadTestTask::Target::Replica> {
    using type = typename std::conditional_t<
        std::is_invocable_v<F&, rune::replication::PartitionReplica&>,
        std::invoke_result<F&, rune::replication::PartitionReplica&>,
        std::invoke_result<F&, rune::replication::PartitionReplica*>>::type;
};

template <class F, OwnerThreadTestTask::Target T>
class OwnerThreadTestTaskImpl final : public OwnerThreadTestTask {
public:
    using R = typename OwnerTaskResult<F, T>::type;

    OwnerThreadTestTaskImpl(std::int32_t topicId, std::int32_t partition, F f,
                            OwnerThreadTestTask::Clock::time_point deadline)
        : OwnerThreadTestTask(T, topicId, partition, &exec, deadline), f_(std::move(f)) {}

    /// TEST side, only after kCompletedEvent was drained: the result (or rethrow).
    R take() {
        if (error_) std::rethrow_exception(error_);
        if constexpr (!std::is_void_v<R>) return std::move(*result_);
    }

private:
    R invoke_(rune::replication::PartitionReplica* replica) {
        if constexpr (T == Target::ControlThread) {
            (void)replica;
            return f_();
        } else if constexpr (std::is_invocable_v<F&, rune::replication::PartitionReplica&>) {
            if (!replica) {
                throw std::runtime_error(
                    "onOwnerThreadForTest: no PartitionReplica for (topicId=" +
                    std::to_string(topicId()) + ", partition=" +
                    std::to_string(partition()) + ")");
            }
            return f_(*replica);
        } else {
            return f_(replica);
        }
    }

    // OWNER side: store the outcome in plain fields; runOnOwner's Completed
    // event (ring commit) publishes them to the test thread.
    static void exec(OwnerThreadTestTask& self,
                     rune::replication::PartitionReplica* replica) noexcept {
        auto& me = static_cast<OwnerThreadTestTaskImpl&>(self);
        try {
            if constexpr (std::is_void_v<R>) {
                me.invoke_(replica);
            } else {
                me.result_.emplace(me.invoke_(replica));
            }
        } catch (...) {
            me.error_ = std::current_exception();
        }
    }

    using Stored = std::conditional_t<std::is_void_v<R>, bool, R>;

    F                     f_;
    std::optional<Stored> result_;   // owner writes before Completed; test reads after
    std::exception_ptr    error_;    // ditto
};

} // namespace detail

/// TEST side: wait (no bound of its own — see header) for the owner's terminal
/// event.  Completed → the result (or the callable's exception); Skipped → the
/// task reached the owner after its deadline and never ran → throw.
template <class Impl>
auto awaitOwnerThreadTestTask(Impl& task, std::chrono::milliseconds timeout) {
    std::int32_t t = task.pollTerminal();
    while (t == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds{50});
        t = task.pollTerminal();
    }
    if (t == OwnerThreadTestTask::kSkippedEvent) {
        throw std::runtime_error(
            "onOwnerThreadForTest: owner thread did not run the task within " +
            std::to_string(timeout.count()) + "ms (topicId=" +
            std::to_string(task.topicId()) + ", partition=" +
            std::to_string(task.partition()) + ")");
    }
    return task.take();
}

} // namespace rune::node
