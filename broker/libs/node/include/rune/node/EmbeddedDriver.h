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

    static EmbeddedDriverConfig fromNode(const NodeConfig& c, std::string dir);
};

enum class DriverMode : uint8_t { Threaded, Invoker };

/// tmpfs budget: every driver needs at least one full log (3 terms + metadata).
[[nodiscard]] std::optional<std::string>
checkAeronSpace(uint64_t freeBytes, int32_t drivers, int32_t termBufferBytes);

class EmbeddedDriver {
public:
    EmbeddedDriver(EmbeddedDriverConfig cfg, DriverMode mode);
    ~EmbeddedDriver();
    EmbeddedDriver(const EmbeddedDriver&)            = delete;
    EmbeddedDriver& operator=(const EmbeddedDriver&) = delete;

    int  doWork() noexcept;
    void bindPumpThread() noexcept { pumpOwner_ = std::this_thread::get_id(); }
    void close() noexcept;

    [[nodiscard]] DriverMode         mode() const noexcept { return mode_; }
    [[nodiscard]] const std::string& dir() const noexcept { return cfg_.dir; }
    [[nodiscard]] uint64_t foreignPumpAttempts() const noexcept { return foreignPumpAttempts_; }
    [[nodiscard]] uint64_t callerPumpsWhileThreadRunning() const noexcept { return callerPumpsWhileThreadRunning_; }

private:
    int callerPump_() noexcept;   // any pump NOT made by the driver thread

    EmbeddedDriverConfig    cfg_;
    DriverMode              mode_;
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::atomic<bool>       running_{false};   // Threaded: stop flag for the driver thread
    std::thread             thread_;
    std::thread::id         pumpOwner_{};
    uint64_t                foreignPumpAttempts_           = 0;
    uint64_t                callerPumpsWhileThreadRunning_ = 0;
};

} // namespace rune::node
