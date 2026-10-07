#pragma once
// Tests only: a ProvisionChannel whose demands go to a plain callback (no
// provisioner thread).  Growth / dispose posts are not used (servesReadMaps()
// is false while intercepted).
#include <rune/storage/Log.h>
#include <rune/storage/ProvisionChannel.h>

#include <cstdint>

namespace rune::storage::testing {

class DemandChannelForTest : public ProvisionChannel {
public:
    /// Returns false to refuse the demand (a full ring).
    using DemandFn = bool (*)(void* ctx, Log* log, Log::ProvisionDemand kind) noexcept;
    DemandChannelForTest(DemandFn fn, void* ctx) noexcept : fn_(fn), ctx_(ctx) { interceptForTest(&tramp_, this); }

private:
    static bool tramp_(void* self, int32_t type, int64_t a, int64_t b) noexcept {
        auto* t = static_cast<DemandChannelForTest*>(self);
        if (type != kDemandMsgType) return true;
        return t->fn_(t->ctx_, reinterpret_cast<Log*>(static_cast<std::uintptr_t>(a)),
                      static_cast<Log::ProvisionDemand>(b));
    }
    DemandFn fn_;
    void*    ctx_;
};

} // namespace rune::storage::testing
