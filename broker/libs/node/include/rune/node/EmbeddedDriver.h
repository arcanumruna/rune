#pragma once
// EmbeddedDriver.h — one embedded Aeron C media driver, driven by EXACTLY ONE
// thread (spec §3.6).
//   Threaded: today's node driver — a dedicated std::thread loops the main duty
//             cycle; no other thread may pump it (doWork() refuses and counts).
//   Invoker : AERON_THREADING_MODE_INVOKER — conductor+sender+receiver run ONLY
//             inside doWork() on the bound thread (shard worker / fused runtime).
//
// F9a (no atomics):
//   - Threaded stop: the controller (the thread that constructed / closes the
//     driver) posts Stop on stopRing_; the driver thread drains it every loop
//     iteration; join() orders everything after.  threadRunning_ is
//     controller-owned.
//   - pumpOwner_ is a plain thread id: written at construction (Invoker) or by
//     bindPumpThread() on the NEW pumper, which only happens after the previous
//     pumper stopped (ShardAgent's first pass runs after the startup thread
//     handed off; a closer binds after joining the worker) — so every legit
//     write/read pair is ordered by a thread start/join.
//   - Misuse detection (a second pumper / foreign close / caller pump while the
//     thread runs) compares this_thread to pumpOwner_ with NO shared counter:
//     the offending thread refuses, logs, and posts a misuse EVENT on
//     misuseRing_ (many-to-one).  The *Attempts() accessors drain that ring on
//     the ONE observer thread (tests) into observer-owned tallies.
#include <cstddef>   // aeronmd.h uses size_t without including <stddef.h>
#include <cstdint>
extern "C" {
#include <aeronmd/aeronmd.h>
#include "rune/protocol/net/rune_gso_bindings.h"
}
#include <rune/aeronutil/AeronPump.h>
#include <rune/node/NodeConfig.h>

#include <rune/consensus/ControlRing.h>

#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace rune::node {

struct EmbeddedDriverConfig {
    std::string dir;
    // Defaults = NodeConfig's (the findings §4 config).  mtu / window / socket
    // buffer = NodeConfig::kAeronAuto: derived from the host in the constructor
    // (HostDefaults.h deriveAeronTransport) — BrokerNode resolves them once at
    // start and passes concrete values; direct users (tests) get the same rules.
    int32_t     termBufferBytes   = 16 * 1024 * 1024;
    int32_t     mtuBytes          = NodeConfig::kAeronAuto;
    int32_t     rcvWindowBytes    = NodeConfig::kAeronAuto;
    int32_t     socketBufferBytes = NodeConfig::kAeronAuto;   ///< 0 = OS default
    /// false: allocate + zero-fill terms at creation.  nullopt = derived (non-sparse
    /// only when `dir` is on tmpfs — HostDefaults.h deriveTermSparse).
    std::optional<bool> termBufferSparse;
    /// Hosts this driver's endpoints use (loopback decides the derived MTU).
    /// Empty = loopback.
    std::vector<std::string> endpointHosts;
    int32_t     maxMessagesPerSend = 0;      ///< 0 = Aeron default (2), or 16 when udpGso is active
    bool        udpGso             = true;   ///< install rune_gso bindings (Linux UDP GSO/GRO; else stock)
    int32_t     senderIoVector     = 0;      ///< 0 = Aeron default
    int32_t     receiverIoVector   = 0;      ///< 0 = Aeron default
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
    void bindPumpThread() noexcept { pumpOwner_ = std::this_thread::get_id(); }
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
    /// The rune_gso UDP GSO/GRO bindings were installed (stays true after close()).
    [[nodiscard]] bool udpGsoActive() const noexcept { return gsoWasActive_; }
    /// GSO/GRO totals of every transport this driver closed; complete after close().
    [[nodiscard]] rune_gso_totals_t gsoTotals() const noexcept { return gsoTotals_; }
    /// network_publication_max_messages_per_send the driver was started with.
    [[nodiscard]] uint32_t maxMessagesPerSend() const noexcept { return maxMessagesPerSend_; }
    /// Misuse tallies (diagnostics).  OBSERVER thread only (one at a time —
    /// tests): each call drains misuseRing_ into the observer-owned tallies.
    [[nodiscard]] uint64_t foreignPumpAttempts() const noexcept { drainMisuse_(); return foreignPumps_; }
    [[nodiscard]] uint64_t foreignCloseAttempts() const noexcept { drainMisuse_(); return foreignCloses_; }
    [[nodiscard]] uint64_t callerPumpsWhileThreadRunning() const noexcept { drainMisuse_(); return callerPumpsWhileRunning_; }

private:
    static int pumpTrampoline_(void* self) noexcept;   // AeronPump::kRefused off the pump thread
    int callerPump_() noexcept;   // any pump NOT made by the driver thread

    enum Misuse : std::int32_t { kForeignPump = 1, kForeignClose = 2, kCallerPumpWhileRunning = 3 };
    static constexpr std::int32_t kStopCmd = 1;

    /// Any thread: report a misuse event (cold path; dropped + already logged
    /// by the caller if the ring is full — diagnostics, never coordination).
    void reportMisuse_(Misuse m) noexcept { (void)misuseRing_.offer(m); }
    void drainMisuse_() const noexcept;
    /// After aeron_driver_context_close: capture the GSO totals and free the bindings.
    void releaseGso_() noexcept;

    EmbeddedDriverConfig    cfg_;
    DriverMode              mode_;
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    rune_gso_bindings_t*    gso_    = nullptr;   // owned; outlives ctx_ (Aeron points at it)
    rune_gso_totals_t       gsoTotals_{};
    bool                    gsoWasActive_ = false;
    uint32_t                maxMessagesPerSend_ = 0;
    bool                    threadRunning_ = false;   // Threaded: CONTROLLER-owned
    rune::consensus::ControlRing<256> stopRing_;       // controller → driver thread
    std::thread             thread_;
    std::thread::id         pumpOwner_{};             // see F9a note above
    // any thread → observer: misuse events (many-to-one).
    mutable rune::consensus::ControlRing<1024> misuseRing_;
    // Observer-owned tallies (drained from misuseRing_).
    mutable uint64_t        foreignPumps_            = 0;
    mutable uint64_t        foreignCloses_           = 0;
    mutable uint64_t        callerPumpsWhileRunning_ = 0;
};

} // namespace rune::node
