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
// F9a (no atomics — the old Pending/Running/Cancelled CAS is gone):
//   - The run-or-skip DECISION is made by the OWNER alone, from immutable data:
//     the deadline (submit time + timeout) is fixed before the ref is enqueued
//     (the ring publishes it).  Owner reaches the task at/after the deadline →
//     it SKIPS (never invokes the callable); before → it runs it.
//   - The owner reports its decision to the test thread as an EVENT on the
//     task's own ControlRing (owner = sole producer, test = sole consumer):
//     Started or Skipped.  The result travels in the std::promise as before.
//
// Timeout safety: when the result is not ready by the deadline the test reads
// the owner's decision event.  Skipped → throw (the callable never ran and never
// will).  Started → wait for completion (the callable may reference the test
// frame).  No decision yet → the owner has not reached the task, so when it does
// it is past the deadline and will skip; the test waits a short grace for the
// event and then throws (the owner can only skip from then on).

#include <rune/consensus/ControlRing.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
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

    /// Owner → test decision events (on decisions_).
    static constexpr std::int32_t kStartedEvent = 1;
    static constexpr std::int32_t kSkippedEvent = 2;

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
    /// The decision is posted to the test thread (decisions_) BEFORE running.
    void runOnOwner(rune::replication::PartitionReplica* replica) noexcept {
        if (Clock::now() >= deadline_) {
            (void)decisions_.offer(kSkippedEvent);   // 1 event per task: never full
            skip_(*this);
            return;                                   // callable never invoked
        }
        (void)decisions_.offer(kStartedEvent);
        exec_(*this, replica);
    }

    /// TEST side (sole consumer): the owner's decision, or 0 if none yet.
    [[nodiscard]] std::int32_t pollDecision() noexcept {
        std::int32_t d = 0;
        decisions_.poll([&d](std::int32_t type, std::int64_t, std::int64_t) noexcept { d = type; });
        return d;
    }

protected:
    using SkipFn = void (*)(OwnerThreadTestTask&) noexcept;
    SkipFn skip_ = nullptr;   // set by the concrete task: fail the promise on skip

private:
    ExecFn                            exec_;
    const Clock::time_point           deadline_;   // immutable; published by the ring
    std::int32_t                      topicId_;
    std::int32_t                      partition_;
    Target                            target_;
    rune::consensus::ControlRing<256> decisions_;  // owner → test (one event)
};

/// The ring carries a raw pointer to a HEAP-allocated shared_ptr holder (one
/// reference owned by "the ring"); the owner releases it after running.
using OwnerThreadTestTaskRef = std::shared_ptr<OwnerThreadTestTask>;

/// Owner-side view of a ring-carried ref (topicId / partition / target routing).
[[nodiscard]] inline const OwnerThreadTestTask&
ownerThreadTestTaskOf(void* ref) noexcept {
    return **static_cast<OwnerThreadTestTaskRef*>(ref);
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
        : OwnerThreadTestTask(T, topicId, partition, &exec, deadline), f_(std::move(f)) {
        skip_ = &skip;
    }

    std::future<R> future() { return promise_.get_future(); }

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

    static void exec(OwnerThreadTestTask& self,
                     rune::replication::PartitionReplica* replica) noexcept {
        auto& me = static_cast<OwnerThreadTestTaskImpl&>(self);
        try {
            if constexpr (std::is_void_v<R>) {
                me.invoke_(replica);
                me.promise_.set_value();
            } else {
                me.promise_.set_value(me.invoke_(replica));
            }
        } catch (...) {
            try { me.promise_.set_exception(std::current_exception()); } catch (...) {}
        }
    }

    // Owner skipped the task (deadline passed): complete the promise so nothing
    // waits on a value that will never come.
    static void skip(OwnerThreadTestTask& self) noexcept {
        auto& me = static_cast<OwnerThreadTestTaskImpl&>(self);
        try {
            me.promise_.set_exception(std::make_exception_ptr(std::runtime_error(
                "onOwnerThreadForTest: task skipped by the owner (deadline passed)")));
        } catch (...) {}
    }

    F               f_;
    std::promise<R> promise_;
};

} // namespace detail

/// Grace the test waits, after the deadline, for the owner's decision event.
inline constexpr std::chrono::milliseconds kOwnerDecisionGrace{2000};

/// TEST side: wait for the owner to run the task.  On timeout, read the owner's
/// decision: Started → wait for completion (never unwind while the callable may
/// still reference the caller's frame); Skipped / no decision → throw (past the
/// deadline the owner can only skip).
template <class R>
R awaitOwnerThreadTestTask(OwnerThreadTestTask& task, std::future<R>& fut,
                           std::chrono::milliseconds timeout) {
    if (fut.wait_until(task.deadline()) != std::future_status::ready) {
        // Past the deadline: the owner's decision event says what happened.
        // (Started is posted BEFORE the callable runs, so a run is never missed.)
        std::int32_t decision = task.pollDecision();
        const auto graceEnd = OwnerThreadTestTask::Clock::now() + kOwnerDecisionGrace;
        while (decision == 0 && OwnerThreadTestTask::Clock::now() < graceEnd) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            decision = task.pollDecision();
        }
        if (decision != OwnerThreadTestTask::kStartedEvent) {
            // Skipped, or not reached yet (it can then only be skipped).
            throw std::runtime_error(
                "onOwnerThreadForTest: owner thread did not run the task within " +
                std::to_string(timeout.count()) + "ms (topicId=" +
                std::to_string(task.topicId()) + ", partition=" +
                std::to_string(task.partition()) + ")");
        }
        fut.wait();   // started: the callable may reference the caller's frame
    }
    return fut.get();
}

} // namespace rune::node
