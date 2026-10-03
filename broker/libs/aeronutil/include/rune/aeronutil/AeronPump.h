#pragma once
// AeronPump.h — spec §3.6: "add, then pump THIS shard's own driver + conductor
// until found or deadline".  Startup/control path only (a function pointer is
// fine here; nothing on the per-record path calls it).
//   default-constructed (threaded driver + conductor threads): resolve() sleeps
//     1 ms between attempts — exactly the legacy behaviour;
//   invoker (driver + client conductor owned by the resolving thread): resolve()
//     pumps both between attempts and never sleeps.
// Fail-fast (never a silent spin):
//   - a driver pump fn that returns kRefused (< 0) — e.g. an invoker driver
//     pumped from a thread that is not its bound pumper — stops resolve()
//     immediately (the conductor is not invoked either); refused() latches;
//   - a caller-owned `failed` flag (set by the find lambda when the driver
//     reported a registration error) stops resolve() on the next check.
#include <Aeron.h>

#include <chrono>
#include <cstdint>
#include <thread>

namespace rune::aeronutil {

class AeronPump {
public:
    using DriverPumpFn = int (*)(void*) noexcept;
    /// Returned by a DriverPumpFn that refuses to pump (not this thread's driver).
    static constexpr int kRefused = -1;

    AeronPump() noexcept = default;
    AeronPump(DriverPumpFn fn, void* driverCtx, aeron::Aeron* client) noexcept
        : fn_(fn), ctx_(driverCtx), client_(client) {}

    [[nodiscard]] bool invoker() const noexcept { return fn_ != nullptr; }

    int pump() noexcept {
        if (!fn_) return 0;
        ++pumps_;
        int w = fn_(ctx_);
        if (w < 0) { refused_ = true; return kRefused; }   // not ours to pump: stop
        if (client_) w += client_->conductorAgentInvoker().invoke();
        return w;
    }

    /// "No deadline of our own": resolve until Aeron ANSWERS — find yields, a registration error is
    /// reported (find throws / sets `failed`), or the driver refuses the pump.  The bound is Aeron's:
    /// ClientConductor::find* throws DriverTimeoutException once a registration is older than the
    /// client's driverTimeoutMs (default 10 s, wall clock), in threaded AND invoker modes.  Production
    /// startup resolves use this instead of Rune's own tighter 5 s cap, which made a loaded node refuse
    /// to start (D8 2026-10-03).  Pass exactly kUntilAnswered (any other huge budget overflows).
    static constexpr std::chrono::milliseconds kUntilAnswered = std::chrono::milliseconds::max();

    /// Probe `find` until it yields a truthy value, `budget` elapses (never, for kUntilAnswered),
    /// `*failed` becomes true (find saw a hard registration error), or the driver refuses to be
    /// pumped by this thread.  Returns find()'s last result.
    template <class Find>
    auto resolve(Find&& find, std::chrono::milliseconds budget,
                 const bool* failed = nullptr) -> decltype(find()) {
        using clock = std::chrono::steady_clock;
        const bool bounded = budget != kUntilAnswered;
        const auto deadline = bounded ? clock::now() + budget : clock::time_point::max();
        for (;;) {
            auto r = find();
            if (r || (failed && *failed) || (bounded && clock::now() >= deadline)) return r;
            if (invoker()) {
                if (pump() < 0) return r;                      // refused: fail fast
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
        }
    }

    [[nodiscard]] uint64_t pumps() const noexcept { return pumps_; }
    /// True once the driver refused a pump from this thread (resolve failed fast).
    [[nodiscard]] bool refused() const noexcept { return refused_; }

private:
    DriverPumpFn  fn_     = nullptr;
    void*         ctx_    = nullptr;
    aeron::Aeron* client_ = nullptr;
    uint64_t      pumps_  = 0;
    bool          refused_ = false;
};

} // namespace rune::aeronutil
