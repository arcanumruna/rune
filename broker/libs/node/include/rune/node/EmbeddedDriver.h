#pragma once
// EmbeddedDriver.h — one embedded Aeron C media driver, driven by EXACTLY ONE
// thread (spec §3.6).
//   Threaded: today's node driver — a dedicated std::thread loops the main duty
//             cycle; no other thread may pump it (doWork() refuses and counts).
//   Invoker : AERON_THREADING_MODE_INVOKER — conductor+sender+receiver run ONLY
//             inside doWork() on the bound thread (shard worker / fused runtime).
#include <cstddef>   // aeronmd.h uses size_t without including <stddef.h>
#include <cstdint>
extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <rune/aeronutil/AeronPump.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/node/NodeConfig.h>

#include <atomic>
#include <optional>
#include <string>
#include <thread>

namespace rune::node {

struct EmbeddedDriverConfig {
    std::string dir;
    int32_t     termBufferBytes   = 4 * 1024 * 1024;
    int32_t     mtuBytes          = 1408;
    int32_t     rcvWindowBytes    = 128 * 1024;
    int32_t     socketBufferBytes = 2 * 1024 * 1024;
    NodeConfig::AeronThreadingMode threadingMode = NodeConfig::AeronThreadingMode::Shared;
    std::string idleStrategy      = "sleep-ns";
    bool        deleteDirOnShutdown = true;
    /// 0 = Aeron default (10 s).  Also the invoker client's inter-service timeout
    /// (read from CnC).  Must exceed the driver timer interval (1 s).  Test-only
    /// override (shard startup-gap regression); never set in production.
    int64_t     clientLivenessTimeoutNs = 0;

    static EmbeddedDriverConfig fromNode(const NodeConfig& c, std::string dir);
};

enum class DriverMode : uint8_t { Threaded, Invoker };

/// tmpfs budget: every driver needs at least one full log (3 terms + metadata).
[[nodiscard]] std::optional<std::string>
checkAeronSpace(uint64_t freeBytes, int32_t drivers, int32_t termBufferBytes);

class EmbeddedDriver {
public:
    EmbeddedDriver(EmbeddedDriverConfig cfg, DriverMode mode);
    /// Closes the driver.  An Invoker driver destroyed off its pump thread (no
    /// hand-off) cannot close: logged critical, and debug builds abort.
    ~EmbeddedDriver();
    EmbeddedDriver(const EmbeddedDriver&)            = delete;
    EmbeddedDriver& operator=(const EmbeddedDriver&) = delete;

    int  doWork() noexcept;
    /// The calling thread becomes the only pumper (Invoker).  Calling it also
    /// asserts that the previous pumper has stopped, e.g. a closer takes ownership
    /// after joining the worker thread and before close().
    void bindPumpThread() noexcept { pumpOwner_.store(std::this_thread::get_id(), std::memory_order_relaxed); }
    /// Idempotent.  Invoker: only the bound pumper may close.  A close from any other
    /// thread is REFUSED (logged, counted in foreignCloseAttempts(), driver left
    /// untouched) because draining would make it a second pumper.
    void close() noexcept;

    /// A pump for startup resolves against THIS driver.
    ///  - Threaded: the default pump (driver thread makes progress; resolve sleeps 1 ms).
    ///  - Invoker:  pumps this driver + `client`'s conductor.  Only the bound pump
    ///    thread may use it: a pump from any other thread is REFUSED loudly
    ///    (logged, foreignPumpAttempts()++) and the resolve fails fast — never a
    ///    silent spin for the whole budget.
    [[nodiscard]] rune::aeronutil::AeronPump pumpWith(aeron::Aeron* client) noexcept {
        if (mode_ == DriverMode::Threaded) return {};
        return rune::aeronutil::AeronPump(&EmbeddedDriver::pumpTrampoline_, this, client);
    }

    /// True until close() has actually released the driver (a refused close
    /// leaves it open).  Read on the closing / pump thread only.
    [[nodiscard]] bool               isOpen() const noexcept { return driver_ != nullptr; }
    [[nodiscard]] DriverMode         mode() const noexcept { return mode_; }
    [[nodiscard]] const std::string& dir() const noexcept { return cfg_.dir; }
    [[nodiscard]] uint64_t foreignPumpAttempts() const noexcept { return foreignPumpAttempts_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t foreignCloseAttempts() const noexcept { return foreignCloseAttempts_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t callerPumpsWhileThreadRunning() const noexcept { return callerPumpsWhileThreadRunning_.load(std::memory_order_relaxed); }

private:
    static int pumpTrampoline_(void* self) noexcept;   // AeronPump::kRefused off the pump thread
    int callerPump_() noexcept;   // any pump NOT made by the driver thread

    EmbeddedDriverConfig    cfg_;
    DriverMode              mode_;
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::atomic<bool>       running_{false};   // Threaded: stop flag for the driver thread
    std::thread             thread_;
    // Diagnostics only (never used for coordination): relaxed atomics because
    // foreign threads touch them by definition (spec §3.6 one-pumper contract).
    std::atomic<std::thread::id> pumpOwner_{};
    std::atomic<uint64_t>        foreignPumpAttempts_{0};
    std::atomic<uint64_t>        foreignCloseAttempts_{0};
    std::atomic<uint64_t>        callerPumpsWhileThreadRunning_{0};
};

/// Interim pumper for an Invoker driver + its invoker client, run by its own
/// AgentRunner (Aeron: a composite of the driver and client-conductor invokers).
/// Fused runtime (spec §3.2): the runtime's driver + client exist from step 3 of
/// BrokerNode::start(), long before worker 0's loop runs; this agent keeps them
/// serviced through every startup phase, however long.  Hand-off: the runner is
/// stopped and JOINED, then the next owner binds itself (bindPumpThread) at its
/// first pass — never two pumpers.  Startup resolves on that client use the
/// default (sleep-poll) AeronPump while this agent runs.
class DriverPumpAgent final : public rune::consensus::Agent {
public:
    DriverPumpAgent(EmbeddedDriver& driver, aeron::Aeron& client) noexcept
        : driver_(driver), client_(client) {}
    const char* name() const noexcept override { return "rune-rt-pump"; }
    int doWork(int64_t /*nowNs*/) noexcept override {
        if (!bound_) [[unlikely]] {
            driver_.bindPumpThread();          // hand-off: constructing thread → this runner
            bound_ = true;
        }
        return driver_.doWork() + client_.conductorAgentInvoker().invoke();
    }

private:
    EmbeddedDriver& driver_;
    aeron::Aeron&   client_;
    bool            bound_ = false;
};

} // namespace rune::node
