// include/rune/client/producer/FrameBytes.h — the byte buffer a Produce frame
// is built in (ProduceFrameBuilder) and retained in (InFlightBatch::wireFrame);
// the two swap storage per batch, so they share this one type.
//
// Growing it does NOT zero the new bytes (default-initialisation): every byte
// of a frame is written before it is sent (the builder writes record bytes as
// it appends, seal() writes the whole Produce + RecordBatch header, a claim's
// caller writes its value in place).  Task 10 (perf): std::vector<uint8_t>'s
// value-initialising resize() memset the claimed value before the caller
// overwrote it — ~5 ns per Invoker claim.
#pragma once

#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace rune::client::producer {

/// std::allocator whose no-argument construct() default-initialises (leaves a
/// trivially constructible element uninitialised) instead of value-initialising.
template <class T>
struct DefaultInitAllocator : std::allocator<T> {
    using value_type = T;
    DefaultInitAllocator() noexcept = default;
    template <class U>
    DefaultInitAllocator(const DefaultInitAllocator<U>&) noexcept {}  // NOLINT: rebind converting ctor
    template <class U>
    struct rebind {
        using other = DefaultInitAllocator<U>;
    };
    template <class U>
    void construct(U* p) noexcept(std::is_nothrow_default_constructible_v<U>) {
        ::new (static_cast<void*>(p)) U;
    }
    template <class U, class... A>
    void construct(U* p, A&&... a) {
        ::new (static_cast<void*>(p)) U(std::forward<A>(a)...);
    }
};
template <class T, class U>
constexpr bool operator==(const DefaultInitAllocator<T>&, const DefaultInitAllocator<U>&) noexcept {
    return true;
}

/// A Produce wire frame's bytes (see the file comment: growth is not zeroed).
using FrameBytes = std::vector<uint8_t, DefaultInitAllocator<uint8_t>>;

}  // namespace rune::client::producer
