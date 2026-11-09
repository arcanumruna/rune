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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

namespace rune::aeronutil {

/// Fused startup: the starter thread and the interim pump thread share ONE invoker-mode Aeron client, whose
/// add* / find* / invoke are not mutually thread-safe (an unlocked append to the conductor's registering list can
/// race its compaction and lose a registration, whose driver answer is then ignored forever).  The starter holds
/// the gate while it works, AeronPump::resolve drops it around its sleeps, and the pump thread invokes the client
/// only while it can take the gate.  Spin-based so a different thread may release a hold (stop paths).
class StartupClientGate {
public:
    /// Pump thread: true = the client is yours for one invoke; unlock() afterwards.
    [[nodiscard]] bool tryLock() noexcept { return !busy_.exchange(true, std::memory_order_acquire); }
    void unlock() noexcept { busy_.store(false, std::memory_order_release); }

    /// Starter thread: hold the gate until release() (resolve() lets go around its sleeps).
    void acquire() noexcept {
        while (!tryLock()) std::this_thread::yield();
        owner_.store(std::this_thread::get_id(), std::memory_order_relaxed);
        held() = this;
    }
    /// Idempotent; also valid from another thread than the holder.
    void release() noexcept {
        if (owner_.exchange(std::thread::id{}, std::memory_order_relaxed) != std::thread::id{}) unlock();
    }
    /// The gate this thread currently holds, else nullptr.
    [[nodiscard]] static StartupClientGate* heldHere() noexcept {
        StartupClientGate* g = held();
        return g && g->owner_.load(std::memory_order_relaxed) == std::this_thread::get_id() ? g : nullptr;
    }

    /// Lets the pump thread through for a long non-client stall of the holder, then takes the gate back.
    class Yield {
    public:
        Yield() noexcept : gate_(heldHere()) { if (gate_) gate_->release(); }
        ~Yield() { if (gate_) gate_->acquire(); }
        Yield(const Yield&) = delete;
        Yield& operator=(const Yield&) = delete;
    private:
        StartupClientGate* gate_;
    };

private:
    static StartupClientGate*& held() noexcept { thread_local StartupClientGate* g = nullptr; return g; }
    std::atomic<bool>           busy_{false};
    std::atomic<std::thread::id> owner_{};
};

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

    /// "No budget of our own": resolve until Aeron ANSWERS — find yields, a registration error is
    /// reported (find throws / sets `failed`), or the driver refuses the pump.  Aeron's own bound
    /// (DriverTimeoutException after driverTimeoutMs) is 1 h in-process, so kUntilAnswered is capped by
    /// startupDeadline() (default 60 s, well past a loaded node's D8 2026-10-03 start): a registration whose
    /// answer never arrives fails loudly (deadlineExpired()) instead of hanging.  Pass exactly kUntilAnswered
    /// (any other huge budget overflows).
    static constexpr std::chrono::milliseconds kUntilAnswered = std::chrono::milliseconds::max();
    static constexpr std::chrono::milliseconds kDefaultStartupDeadline{60'000};

    AeronPump& withStartupDeadline(std::chrono::milliseconds d) noexcept { startupDeadline_ = d; return *this; }
    [[nodiscard]] std::chrono::milliseconds startupDeadline() const noexcept { return startupDeadline_; }
    /// True once a kUntilAnswered resolve gave up at startupDeadline().
    [[nodiscard]] bool deadlineExpired() const noexcept { return deadlineExpired_; }

    /// Probe `find` until it yields a truthy value, `budget` elapses (never, for kUntilAnswered),
    /// `*failed` becomes true (find saw a hard registration error), or the driver refuses to be
    /// pumped by this thread.  Returns find()'s last result.
    template <class Find>
    auto resolve(Find&& find, std::chrono::milliseconds budget,
                 const bool* failed = nullptr) -> decltype(find()) {
        using clock = std::chrono::steady_clock;
        const bool capped = budget == kUntilAnswered;
        const auto deadline = clock::now() + (capped ? startupDeadline_ : budget);
        for (;;) {
            auto r = find();
            if (r || (failed && *failed)) return r;
            if (clock::now() >= deadline) { deadlineExpired_ = capped; return r; }
            if (invoker()) {
                if (pump() < 0) return r;                      // refused: fail fast
            } else if (StartupClientGate* g = StartupClientGate::heldHere()) {
                g->release();                                  // the pump thread services the client meanwhile
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
                g->acquire();
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
    bool          deadlineExpired_ = false;
    std::chrono::milliseconds startupDeadline_ = kDefaultStartupDeadline;
};

} // namespace rune::aeronutil
