// tests/support/AllocCounter.h — counts global operator new calls in a window.
#pragma once
namespace alloc_counter {
void start() noexcept;
long stop() noexcept;
/// Count only the calling thread's allocations (other threads — runners, a
/// fake broker — are not in the window).
void startThisThread() noexcept;
long stopThisThread() noexcept;
} // namespace alloc_counter
