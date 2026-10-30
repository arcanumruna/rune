#pragma once
// WriteHookPolicy.h — the compile-time write-hook seam of handleProduceBatch<LogT, HookT>.
//
// An active hook (kActive == true) provides:
//   ErrorCode check(const uint8_t* batch, std::size_t len, int32_t recordCount)  — after dedupe, before the min-ISR gate
//   HookStaged stage(LogT& log, const uint8_t* batch, std::size_t len)            — right before the append
//   void onAppended(int64_t base, int64_t lastOffset)                             — after the PSM update
//   uint64_t assignedBase() const noexcept                                        — read right after onAppended

#include <rune/protocol/client/Protocol.h>

#include <cstddef>
#include <cstdint>

namespace rune::storage {

struct HookStaged {
    rune::protocol::client::ErrorCode error{};
    const uint8_t*                    batch = nullptr;
    std::size_t                       len   = 0;
};

struct NoWriteHook {
    static constexpr bool kActive = false;
};

} // namespace rune::storage
