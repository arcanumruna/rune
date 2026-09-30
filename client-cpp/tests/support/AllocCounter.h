// tests/support/AllocCounter.h — counts global operator new calls in a window.
#pragma once
namespace alloc_counter {
void start() noexcept;
long stop() noexcept;
} // namespace alloc_counter
