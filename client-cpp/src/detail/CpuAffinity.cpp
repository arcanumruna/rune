// src/detail/CpuAffinity.cpp
#include <rune/client/detail/CpuAffinity.h>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace rune::client::detail {

bool pinCurrentThreadToCore(int core) noexcept {
#if defined(__linux__)
    if (core < 0 || core >= CPU_SETSIZE) return false;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)core;
    return false;
#endif
}

int currentCore() noexcept {
#if defined(__linux__)
    return sched_getcpu();
#else
    return -1;
#endif
}

} // namespace rune::client::detail
