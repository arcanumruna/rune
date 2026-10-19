#pragma once
// ZeroedBytes — a fixed-size, zero-filled byte buffer allocated once (at construction) and never resized.
//
// Why not std::vector<uint8_t>(n): in an unoptimized build (Debug, ASan, TSan — every test build) libc++
// constructs AND destroys a vector's elements one by one through allocator_traits, a function call per
// byte.  A component that preallocates a multi-MiB scratch / ring at construction paid ~10 ns x n twice —
// hundreds of ms per broker start / test fixture.  Here construction is one memset (every page written
// once, exactly as the vector's value fill did in an optimized build) and destruction one delete[].
// Behaviour in an optimized build is unchanged: zeroed, prefaulted, fixed capacity, owned.
//
// Cold-path only by construction: the one allocation happens in the owner's constructor (or an explicit
// reset(n) at setup); the hot path uses data() / size() / operator[] — zero allocations in steady state.
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

namespace rune::storage {

class ZeroedBytes {
public:
    ZeroedBytes() noexcept = default;
    explicit ZeroedBytes(std::size_t n) { reset(n); }

    ZeroedBytes(ZeroedBytes&& o) noexcept : bytes_(std::move(o.bytes_)), size_(o.size_) { o.size_ = 0; }
    ZeroedBytes& operator=(ZeroedBytes&& o) noexcept {
        bytes_ = std::move(o.bytes_);
        size_  = o.size_;
        o.size_ = 0;
        return *this;
    }
    ZeroedBytes(const ZeroedBytes&)            = delete;
    ZeroedBytes& operator=(const ZeroedBytes&) = delete;

    /// (Re)allocate `n` zeroed bytes — setup only, never on a hot path.
    void reset(std::size_t n) {
        bytes_.reset(n == 0 ? nullptr : new uint8_t[n]);
        if (n != 0) std::memset(bytes_.get(), 0, n);
        size_ = n;
    }

    [[nodiscard]] uint8_t*       data() noexcept { return bytes_.get(); }
    [[nodiscard]] const uint8_t* data() const noexcept { return bytes_.get(); }
    [[nodiscard]] std::size_t    size() const noexcept { return size_; }
    [[nodiscard]] bool           empty() const noexcept { return size_ == 0; }
    uint8_t&       operator[](std::size_t i) noexcept { return bytes_[i]; }
    const uint8_t& operator[](std::size_t i) const noexcept { return bytes_[i]; }
    [[nodiscard]] uint8_t*       begin() noexcept { return bytes_.get(); }
    [[nodiscard]] uint8_t*       end() noexcept { return bytes_.get() + size_; }
    [[nodiscard]] const uint8_t* begin() const noexcept { return bytes_.get(); }
    [[nodiscard]] const uint8_t* end() const noexcept { return bytes_.get() + size_; }

private:
    std::unique_ptr<uint8_t[]> bytes_;
    std::size_t                size_ = 0;
};

} // namespace rune::storage
