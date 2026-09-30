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
// calls runOwnerThreadTestTask(); it never touches any synchronisation.  ALL the
// synchronisation (the cancel CAS + std::promise/future completion) lives in the
// TEST-OWNED task object below, which is shared-owned (std::shared_ptr) by the
// test thread and the ring-carried ref so neither side can dangle.
//
// Timeout safety: if the owner has not STARTED the task by the deadline, the test
// thread cancels it (Pending→Cancelled CAS) and throws — the owner will later see
// Cancelled and skip it, so the callable (which typically captures test locals by
// reference) is never invoked after the test frame unwinds.  If the owner already
// started it, the test waits for completion (the callable is short) before
// returning, for the same reason.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
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

    OwnerThreadTestTask(Target target, std::int32_t topicId, std::int32_t partition,
                        ExecFn exec) noexcept
        : exec_(exec), topicId_(topicId), partition_(partition), target_(target) {}

    ~OwnerThreadTestTask() = default;  // shared_ptr (make_shared<Impl>) deletes via the concrete type
    OwnerThreadTestTask(const OwnerThreadTestTask&)            = delete;
    OwnerThreadTestTask& operator=(const OwnerThreadTestTask&) = delete;

    [[nodiscard]] Target       target()    const noexcept { return target_; }
    [[nodiscard]] std::int32_t topicId()   const noexcept { return topicId_; }
    [[nodiscard]] std::int32_t partition() const noexcept { return partition_; }

    /// OWNER side: run the task unless the test already cancelled it.
    void runOnOwner(rune::replication::PartitionReplica* replica) noexcept {
        State expected = State::Pending;
        if (!state_.compare_exchange_strong(expected, State::Running,
                                            std::memory_order_acq_rel)) {
            return;  // cancelled by the test thread (timed out) — never invoke
        }
        exec_(*this, replica);
    }

    /// TEST side: cancel if the owner has not started it.  true = cancelled
    /// (owner will never invoke the callable); false = owner is running / ran it.
    [[nodiscard]] bool tryCancel() noexcept {
        State expected = State::Pending;
        return state_.compare_exchange_strong(expected, State::Cancelled,
                                              std::memory_order_acq_rel);
    }

private:
    enum class State : std::uint8_t { Pending, Running, Cancelled };

    ExecFn             exec_;
    std::int32_t       topicId_;
    std::int32_t       partition_;
    Target             target_;
    std::atomic<State> state_{State::Pending};
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

    OwnerThreadTestTaskImpl(std::int32_t topicId, std::int32_t partition, F f)
        : OwnerThreadTestTask(T, topicId, partition, &exec), f_(std::move(f)) {}

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

    F               f_;
    std::promise<R> promise_;
};

} // namespace detail

/// TEST side: wait for the owner to run the task.  On timeout, cancel it if the
/// owner has not started it (then throw); if the owner is mid-run, wait for it
/// (never unwind while the callable may still reference the caller's frame).
template <class R>
R awaitOwnerThreadTestTask(OwnerThreadTestTask& task, std::future<R>& fut,
                           std::chrono::milliseconds timeout) {
    if (fut.wait_for(timeout) != std::future_status::ready) {
        if (task.tryCancel()) {
            throw std::runtime_error(
                "onOwnerThreadForTest: owner thread did not run the task within " +
                std::to_string(timeout.count()) + "ms (topicId=" +
                std::to_string(task.topicId()) + ", partition=" +
                std::to_string(task.partition()) + ")");
        }
        fut.wait();
    }
    return fut.get();
}

} // namespace rune::node
