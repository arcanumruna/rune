#pragma once
// AeronPump.h — spec §3.6: "add, then pump THIS shard's own driver + conductor
// until found or deadline".  Startup/control path only (a function pointer is
// fine here; nothing on the per-record path calls it).
//   default-constructed (threaded driver + conductor threads): resolve() sleeps
//     1 ms between attempts — exactly the legacy behaviour;
//   invoker (driver + client conductor owned by the resolving thread): resolve()
//     pumps both between attempts and never sleeps.
#include <Aeron.h>

#include <chrono>
#include <cstdint>
#include <thread>

namespace rune::aeronutil {

class AeronPump {
public:
    using DriverPumpFn = int (*)(void*) noexcept;

    AeronPump() noexcept = default;
    AeronPump(DriverPumpFn fn, void* driverCtx, aeron::Aeron* client) noexcept
        : fn_(fn), ctx_(driverCtx), client_(client) {}

    [[nodiscard]] bool invoker() const noexcept { return fn_ != nullptr; }

    int pump() noexcept {
        if (!fn_) return 0;
        ++pumps_;
        int w = fn_(ctx_);
        if (client_) w += client_->conductorAgentInvoker().invoke();
        return w;
    }

    template <class Find>
    auto resolve(Find&& find, std::chrono::milliseconds budget) -> decltype(find()) {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + budget;
        for (;;) {
            auto r = find();
            if (r || clock::now() >= deadline) return r;
            if (invoker()) pump();
            else           std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }

    [[nodiscard]] uint64_t pumps() const noexcept { return pumps_; }

private:
    DriverPumpFn  fn_     = nullptr;
    void*         ctx_    = nullptr;
    aeron::Aeron* client_ = nullptr;
    uint64_t      pumps_  = 0;
};

} // namespace rune::aeronutil
