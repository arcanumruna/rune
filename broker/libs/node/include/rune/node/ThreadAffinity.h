#pragma once
// ThreadAffinity — pin broker threads by CPU set (D2).
//
// Linux: pthread_setaffinity_np / pthread_getaffinity_np on the CALLING thread.
// Elsewhere: no-op (ENOTSUP / empty) — macOS has no hard affinity.
//
// Threads inherit their creator's mask, so pinning the thread that spawns the
// broker's non-worker threads (BrokerNode::start) pins all of them: the node
// driver thread, the Aeron client conductor (thread placement), the runtime,
// admin and cleaner runners, the portable flushers.  Workers re-pin themselves
// to their own core on their first pass (Worker::pinCurrentThreadToCore_).
// Cold path only (startup).

#include <cerrno>
#include <cstdint>
#include <vector>

#include <pthread.h>
#if defined(__linux__)
#include <sched.h>
#endif

namespace rune::node {

/// Pin thread `t` to `cpus` (CPUs >= CPU_SETSIZE ignored).  0 or an errno;
/// EINVAL for an empty set (never pin to nothing), ENOTSUP off Linux.
[[nodiscard]] inline int pinThreadToCpus(pthread_t t, const std::vector<int32_t>& cpus) noexcept {
    if (cpus.empty()) return EINVAL;
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    bool any = false;
    for (const int32_t c : cpus)
        if (c >= 0 && c < CPU_SETSIZE) { CPU_SET(c, &set); any = true; }
    if (!any) return EINVAL;
    return ::pthread_setaffinity_np(t, sizeof(set), &set);
#else
    (void)t;
    return ENOTSUP;
#endif
}

/// Pin the calling thread to `cpus` (see pinThreadToCpus).
[[nodiscard]] inline int pinCurrentThreadToCpus(const std::vector<int32_t>& cpus) noexcept {
    return pinThreadToCpus(::pthread_self(), cpus);
}

/// The calling thread's current CPU set, sorted (empty off Linux or on error).
[[nodiscard]] inline std::vector<int32_t> currentThreadCpus() {
    std::vector<int32_t> out;
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::pthread_getaffinity_np(::pthread_self(), sizeof(set), &set) != 0) return out;
    for (int c = 0; c < CPU_SETSIZE; ++c)
        if (CPU_ISSET(c, &set)) out.push_back(c);
#endif
    return out;
}

/// RAII: pin the calling thread to `cpus` for the scope (threads created inside
/// inherit the set), restoring the previous mask on exit.  An empty `cpus` or a
/// refused pin leaves the mask unchanged.  `result()` = 0, or the errno / ENOTSUP.
class ScopedThreadAffinity {
public:
    explicit ScopedThreadAffinity(const std::vector<int32_t>& cpus) {
        if (cpus.empty()) return;
        saved_  = currentThreadCpus();
        result_ = pinCurrentThreadToCpus(cpus);
    }
    ~ScopedThreadAffinity() {
        if (result_ == 0 && !saved_.empty()) (void)pinCurrentThreadToCpus(saved_);
    }
    ScopedThreadAffinity(const ScopedThreadAffinity&)            = delete;
    ScopedThreadAffinity& operator=(const ScopedThreadAffinity&) = delete;
    [[nodiscard]] int result() const noexcept { return result_; }

private:
    std::vector<int32_t> saved_;
    int                  result_ = -1;   ///< -1 = nothing requested
};

} // namespace rune::node
