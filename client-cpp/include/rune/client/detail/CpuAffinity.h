// include/rune/client/detail/CpuAffinity.h — thread pinning (cold path: thread start).
#pragma once

namespace rune::client::detail {
/// Pins the calling thread to one CPU (Linux pthread_setaffinity_np).  Returns
/// false where unsupported (macOS) or on failure.
bool pinCurrentThreadToCore(int core) noexcept;
/// CPU the calling thread runs on (Linux sched_getcpu), -1 where unsupported.
int currentCore() noexcept;
} // namespace rune::client::detail
