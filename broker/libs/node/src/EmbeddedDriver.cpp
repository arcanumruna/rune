#include <rune/node/EmbeddedDriver.h>
#include <rune/node/HostDefaults.h>
#include "rune/protocol/net/rune_driver_wake.h"
#include "rune/protocol/net/rune_udp_gso.h"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

namespace rune::node {

EmbeddedDriverConfig EmbeddedDriverConfig::fromNode(const NodeConfig& c, std::string dir) {
    EmbeddedDriverConfig e;
    e.dir               = std::move(dir);
    e.termBufferBytes   = c.aeronTermBufferBytes;
    e.mtuBytes          = c.aeronMtuBytes;
    e.rcvWindowBytes    = c.aeronRcvWindowBytes;
    e.socketBufferBytes = c.aeronSocketBufferBytes;
    e.termBufferSparse  = c.aeronTermBufferSparse;
    e.maxMessagesPerSend = c.aeronMaxMessagesPerSend;
    e.udpGso             = c.aeronUdpGso;
    e.epollPoll          = c.aeronEpollPoll;
    e.senderIoVector     = c.aeronSenderIoVectorCapacity;
    e.receiverIoVector   = c.aeronReceiverIoVectorCapacity;
    e.threadingMode     = c.aeronThreadingMode;
    e.idleStrategy      = std::string(c.effectiveDriverIdleStrategy());
    e.idleStrategyArgs  = c.effectiveDriverIdleArgs();
    e.endpointHosts     = nodeEndpointHosts(c);
    return e;
}

// Aeron C client internals (aeron_agent.h / aeron_alloc.h; linked into the driver library) — used only to
// validate an idle strategy + args on a scratch state BEFORE handing them to the context.
extern "C" {
using rune_aeron_idle_func_t = void (*)(void*, int);
rune_aeron_idle_func_t aeron_idle_strategy_load(const char* idle_strategy_name, void** idle_strategy_state,
                                                const char* env_var, const char* init_args);
void aeron_free(void* ptr);
int aeron_parse_duration_ns(const char* str, uint64_t* result);
}

namespace {
/// D2 round 2: would Aeron accept this idle strategy + init args?  A failing context setter leaves freed
/// pointers behind (aeron_driver_context_set_*_idle_strategy frees the old name/state before loading), so
/// aeron_driver_context_close would double-free — validate on a scratch state first, then set.
bool aeronIdleStrategyLoads(const char* name, const char* args) {
    // Aeron's sleeping init allocates its state BEFORE parsing the args and does not free it on a parse failure
    // (aeron_agent.c: aeron_idle_strategy_sleeping_init_args / aeron_idle_strategy_load), so a malformed duration
    // would leak.  Parse it here first, on a stack word, and never reach Aeron's failing path.
    if (args != nullptr && name != nullptr &&
        (std::strcmp(name, "sleeping") == 0 || std::strcmp(name, "sleep-ns") == 0 ||
         std::strcmp(name, "aeron_idle_strategy_sleeping") == 0)) {
        uint64_t durationNs = 0;
        if (aeron_parse_duration_ns(args, &durationNs) < 0) return false;
    }
    void* state = nullptr;
    if (aeron_idle_strategy_load(name, &state, "RUNE_AERON_DRIVER_IDLE", args) == nullptr) return false;
    aeron_free(state);
    return true;
}

/// Resolve any kAeronAuto transport field (direct EmbeddedDriverConfig users;
/// BrokerNode already passes concrete values).  Clamps explicit values that
/// aeron_driver_init would reject (never EINVAL at init).
void resolveTransport(EmbeddedDriverConfig& c) {
    const bool autoAny = c.mtuBytes == NodeConfig::kAeronAuto || c.socketBufferBytes == NodeConfig::kAeronAuto ||
                         c.rcvWindowBytes == NodeConfig::kAeronAuto || !c.termBufferSparse;
    if (!autoAny) return;   // BrokerNode passes resolved values: no second probe (M2)
    const auto facts = probeHostNetFacts(c.endpointHosts, c.dir);
    AeronTransportRequest req;
    req.mtuBytes = c.mtuBytes; req.socketBufferBytes = c.socketBufferBytes;
    req.rcvWindowBytes = c.rcvWindowBytes; req.termBufferBytes = c.termBufferBytes;
    const auto d = deriveAeronTransport(facts, req);
    if (!d.warnings.empty()) spdlog::warn("EmbeddedDriver[{}]: {}", c.dir, d.warnings);
    c.mtuBytes = d.mtuBytes; c.socketBufferBytes = d.socketBufferBytes; c.rcvWindowBytes = d.rcvWindowBytes;
    c.termBufferSparse = deriveTermSparse(c.termBufferSparse, facts.aeronDirTmpfs);
}
} // namespace

std::optional<std::string> checkAeronSpace(uint64_t freeBytes, int32_t drivers, int32_t termBufferBytes) {
    const uint64_t perDriver = 3ULL * static_cast<uint64_t>(termBufferBytes) + 64ULL * 1024;
    const uint64_t need      = perDriver * static_cast<uint64_t>(drivers);
    if (freeBytes >= need) return std::nullopt;
    return "aeron dir has " + std::to_string(freeBytes) + " bytes free but " + std::to_string(drivers) +
           " driver(s) need at least " + std::to_string(need) +
           " (3 x RUNE_AERON_TERM_BUFFER_BYTES + 64 KiB each) — lower RUNE_AERON_TERM_BUFFER_BYTES, "
           "fewer RUNE_WORKERS, or a larger tmpfs";
}

// The ONE primitive for every pump not made by thread_ (Invoker doWork, close
// drain).  Kept deliberately: callerPumpsWhileThreadRunning() is only meaningful
// if all non-thread pumps go through here.  It guards against someone
// reintroducing a caller-side pump while thread_ runs, such as the old startup
// readiness loop (the two-thread race, spec §3.6).
// EmbeddedDriverRegression.ThreadedStartNeverPumpsFromCaller pins it at 0.
// threadRunning_ is controller-owned; in Threaded mode callerPump_ is only
// reachable from close() on the controller (doWork / the trampoline refuse
// Threaded mode), and in Invoker mode threadRunning_ is never written after
// construction — so this read never races a write (F9a).
int EmbeddedDriver::callerPump_() noexcept {
    if (threadRunning_) [[unlikely]] {
        spdlog::error("EmbeddedDriver: caller-side pump while the driver thread runs (dir={}) — "
                      "two pumpers (spec §3.6)", cfg_.dir);
        reportMisuse_(kCallerPumpWhileRunning);
    }
    return aeron_driver_main_do_work(driver_);
}

void EmbeddedDriver::drainMisuse_() const noexcept {
    misuseRing_.poll([this](std::int32_t type, std::int64_t, std::int64_t) noexcept {
        switch (type) {
            case kForeignPump:            ++foreignPumps_; break;
            case kForeignClose:           ++foreignCloses_; break;
            case kCallerPumpWhileRunning: ++callerPumpsWhileRunning_; break;
            default: break;
        }
    });
}

EmbeddedDriver::EmbeddedDriver(EmbeddedDriverConfig cfg, DriverMode mode)
    : cfg_(std::move(cfg)), mode_(mode)
{
    resolveTransport(cfg_);
    if (aeron_driver_context_init(&ctx_) < 0)
        throw std::runtime_error(std::string("EmbeddedDriver: aeron_driver_context_init failed: ") +
                                 aeron_errmsg());
    if (aeron_is_driver_active(cfg_.dir.c_str(), /*timeout_ms=*/1000, +[](const char*) {})) {
        aeron_driver_context_close(ctx_);
        ctx_ = nullptr;
        releaseGso_();
        throw std::runtime_error(
            "EmbeddedDriver: aeron dir '" + cfg_.dir + "' is already in use by an ACTIVE media "
            "driver (a prior broker instance may still be running, or was killed without clean "
            "shutdown). Use a distinct RUNE_AERON_DIR/RUNE_DATA_DIR, or ensure the previous "
            "instance exited.");
    }
    aeron_driver_context_set_dir(ctx_, cfg_.dir.c_str());
    const char* idle = cfg_.idleStrategy.c_str();
    // D2 round 2: an idle strategy's state is built from the init args present when its setter runs — set
    // the args FIRST.  Empty args = Aeron's own (AERON_*_IDLE_STRATEGY_INIT_ARGS env, else built-in).  A
    // failed setter leaves Aeron a NULL idle function (the driver thread aborts on its first idle pass):
    // refuse construction instead.
    // Validated on a scratch state first: a failing context setter would leave freed pointers for
    // aeron_driver_context_close to free again.
    const char* idleArgs = cfg_.idleStrategyArgs.empty() ? nullptr : cfg_.idleStrategyArgs.c_str();
    bool idleOk = true;
    std::string usedArgs = cfg_.idleStrategyArgs;   // the args Aeron builds the strategy from (for the refusal text)
    // D2 R2-L1: validate each role with the args ITS setter will use — explicit args, else the context's own
    // (initialised from AERON_<ROLE>_IDLE_STRATEGY_INIT_ARGS).
    const auto setIdle = [&](auto setArgs, auto getArgs, auto setStrategy) {
        if (!idleOk) return;
        const char* effective = idleArgs != nullptr ? idleArgs : getArgs(ctx_);
        if (idleArgs == nullptr && effective != nullptr) usedArgs = effective;
        idleOk = aeronIdleStrategyLoads(idle, effective);
        if (idleOk && idleArgs != nullptr) idleOk = setArgs(ctx_, idleArgs) == 0;
        idleOk = idleOk && setStrategy(ctx_, idle) == 0;
    };
    if (mode_ == DriverMode::Invoker) {
        // INVOKER: SHARED runner layout, manual main loop REQUIRED, no agent thread
        // (aeron_driver.c:939-956, 1046-1082).  DEDICATED / SHARED_NETWORK would still
        // spawn sender/receiver threads under a manual main loop.
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_INVOKER);
    } else if (cfg_.threadingMode == NodeConfig::AeronThreadingMode::Dedicated) {
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_DEDICATED);
        setIdle(aeron_driver_context_set_conductor_idle_strategy_init_args,
                aeron_driver_context_get_conductor_idle_strategy_init_args, aeron_driver_context_set_conductor_idle_strategy);
        setIdle(aeron_driver_context_set_sender_idle_strategy_init_args,
                aeron_driver_context_get_sender_idle_strategy_init_args, aeron_driver_context_set_sender_idle_strategy);
        setIdle(aeron_driver_context_set_receiver_idle_strategy_init_args,
                aeron_driver_context_get_receiver_idle_strategy_init_args, aeron_driver_context_set_receiver_idle_strategy);
    } else {
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        setIdle(aeron_driver_context_set_shared_idle_strategy_init_args,
                aeron_driver_context_get_shared_idle_strategy_init_args, aeron_driver_context_set_shared_idle_strategy);
    }
    if (!idleOk) {
        const std::string why = aeron_errmsg();
        aeron_driver_context_close(ctx_);
        ctx_ = nullptr;
        throw std::runtime_error("EmbeddedDriver: driver idle strategy '" + cfg_.idleStrategy + "' with args '" +
                                 usedArgs + "' refused by Aeron (RUNE_AERON_DRIVER_IDLE / "
                                 "RUNE_AERON_DRIVER_IDLE_ARGS): " + why);
    }
    if (cfg_.clientLivenessTimeoutNs > 0) {
        // Aeron requires timer_interval < client_liveness < publication_unblock (aeron_driver_context.c, validate):
        // derive both from the liveness, as the rune client does (AeronTransport.cpp).  Every publication on an
        // embedded driver is written by this process, so a 1.5x-liveness unblock only matters if the process dies.
        const auto livenessNs = static_cast<uint64_t>(cfg_.clientLivenessTimeoutNs);
        aeron_driver_context_set_client_liveness_timeout_ns(ctx_, livenessNs);
        if (aeron_driver_context_get_timer_interval_ns(ctx_) >= livenessNs)
            aeron_driver_context_set_timer_interval_ns(ctx_, livenessNs / 10);
        if (aeron_driver_context_get_publication_unblock_timeout_ns(ctx_) <= livenessNs)
            aeron_driver_context_set_publication_unblock_timeout_ns(ctx_, livenessNs + livenessNs / 2);
    }
    aeron_driver_context_set_dir_delete_on_start(ctx_, true);
    aeron_driver_context_set_dir_delete_on_shutdown(ctx_, cfg_.deleteDirOnShutdown);
    aeron_driver_context_set_term_buffer_sparse_file(ctx_, cfg_.termBufferSparse.value_or(true));
    if (cfg_.udpGso) {
        gso_ = rune_gso_bindings_create(1, 1);   // NULL when the kernel/platform has neither -> stock Aeron
        if (gso_ != nullptr && rune_gso_bindings_install(ctx_, gso_) < 0) {
            rune_gso_bindings_delete(gso_);
            gso_ = nullptr;
        }
        gsoActiveMask_ = rune_gso_bindings_active(gso_);
    }
    // A7: epoll-first polling — patches rune_gso's own bindings in place, or a private copy of Aeron's stock
    // ones (never the process-wide stock struct).  After the GSO install (it replaces the context's bindings).
    // Review L2: only over the Aeron release its internals were verified against (else stock polling).
    if (cfg_.epollPoll && !epollPollSupportsAeron(aeron_version_major(), aeron_version_minor(), aeron_version_patch())) {
        spdlog::error("EmbeddedDriver: epoll-first polling disabled — it is verified against Aeron {}.{}.{}, the "
                      "runtime is {} (re-verify rune_driver_wake.c); stock polling, no park-until-work wake fds",
                      kEpollPollAeronPin[0], kEpollPollAeronPin[1], kEpollPollAeronPin[2], aeron_version_full());
    } else if (cfg_.epollPoll) {
        epollPollActive_ = rune_driver_epoll_poll_install(ctx_, /*ctx_bindings_owned=*/gso_ != nullptr ? 1 : 0,
                                                          &epollBindings_) == 1;
    }
    if (cfg_.maxMessagesPerSend > 0)
        aeron_driver_context_set_network_publication_max_messages_per_send(ctx_, static_cast<uint32_t>(cfg_.maxMessagesPerSend));
    else if (const unsigned m = rune_gso_msgs_per_send(udpGsoActive(), static_cast<std::size_t>(cfg_.mtuBytes)); m > 0)
        aeron_driver_context_set_network_publication_max_messages_per_send(ctx_, m);   // GSO groups <= 16 datagrams
    maxMessagesPerSend_ = aeron_driver_context_get_network_publication_max_messages_per_send(ctx_);
    if (cfg_.senderIoVector > 0)
        aeron_driver_context_set_sender_io_vector_capacity(ctx_, static_cast<uint32_t>(cfg_.senderIoVector));
    if (cfg_.receiverIoVector > 0)
        aeron_driver_context_set_receiver_io_vector_capacity(ctx_, static_cast<uint32_t>(cfg_.receiverIoVector));
    aeron_driver_context_set_term_buffer_length(ctx_, static_cast<uint64_t>(cfg_.termBufferBytes));
    aeron_driver_context_set_mtu_length(ctx_, static_cast<size_t>(cfg_.mtuBytes));
    aeron_driver_context_set_rcv_initial_window_length(ctx_, static_cast<size_t>(cfg_.rcvWindowBytes));
    if (cfg_.socketBufferBytes > 0) {
        aeron_driver_context_set_socket_so_sndbuf(ctx_, static_cast<size_t>(cfg_.socketBufferBytes));
        aeron_driver_context_set_socket_so_rcvbuf(ctx_, static_cast<size_t>(cfg_.socketBufferBytes));
    }
    if (cfg_.peerResolution.valid()) {
        nameInbox_.attach(cfg_.peerResolution);
        nameLookup_ = rune_peer_name_lookup_t{&EmbeddedDriver::peerLookup_, &nameInbox_};
        if (rune_peer_name_resolver_install(ctx_, &nameLookup_) < 0) {
            const std::string err = aeron_errmsg();
            aeron_driver_context_close(ctx_);
            ctx_ = nullptr;
            releaseGso_();
            throw std::runtime_error("EmbeddedDriver: peer name resolver install failed: " + err);
        }
    }
    if (aeron_driver_init(&driver_, ctx_) < 0) {
        aeron_driver_context_close(ctx_);
        ctx_ = nullptr;
        releaseGso_();
        throw std::runtime_error(std::string("EmbeddedDriver: aeron_driver_init failed: ") + aeron_errmsg());
    }
    if (aeron_driver_start(driver_, /*manual_main_loop=*/true) < 0) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(ctx_);
        driver_ = nullptr;
        ctx_    = nullptr;
        releaseGso_();
        throw std::runtime_error(std::string("EmbeddedDriver: aeron_driver_start failed: ") + aeron_errmsg());
    }
    if (mode_ == DriverMode::Threaded) {
        try {
            // F9a: the thread exits on a Stop command (stopRing_), drained every
            // loop iteration — no shared flag.
            thread_ = std::thread([this] {
                bool stop = false;
                for (;;) {
                    const int work = aeron_driver_main_do_work(driver_);
                    // Stop is checked BEFORE idling (no extra park after Stop).
                    stopRing_.poll([&stop](std::int32_t type, std::int64_t, std::int64_t) noexcept {
                        if (type == kStopCmd) stop = true;
                    });
                    if (stop) break;
                    aeron_driver_main_idle_strategy(driver_, work);
                }
            });
            threadRunning_ = true;
        } catch (...) {
            // No thread was started (threadRunning_ stays false, so close() does
            // not join): drain and close the initialised driver/context (no leak).
            close();
            throw;
        }
        // No caller-side readiness pumping: aeron_driver_init already wrote cnc.dat
        // (aeron_driver.c:864) and stamped the heartbeat (:911); only thread_ drives.
    } else {
        pumpOwner_ = std::this_thread::get_id();
    }
    spdlog::info("EmbeddedDriver: started (dir={}, mode={})", cfg_.dir,
                 mode_ == DriverMode::Invoker ? "invoker" : "threaded");
    // WARN: visible in perf runs (RUNE_LOG_LEVEL=warn) — the INSTALLED UDP path of this driver.
    spdlog::warn("EmbeddedDriver[{}]: udp mtu={} gso={} gro={} msgsPerSend={} poll={}", cfg_.dir, cfg_.mtuBytes,
                 udpGsoActive() ? "on" : (cfg_.udpGso ? "unsupported" : "off"),
                 udpGroActive() ? "on" : (cfg_.udpGso ? "unsupported" : "off"), maxMessagesPerSend_,
                 epollPollActive_ ? "epoll-first" : "stock");
}

int EmbeddedDriver::peerLookup_(void* state, const char* host, uint32_t* ipv4) noexcept {
    auto* inbox = static_cast<rune::consensus::PeerResolutionInbox*>(state);
    (void)inbox->poll();
    if (host == nullptr) return 0;
    const int idx = inbox->table()->indexOf(host);
    if (idx < 0) return -1;
    return inbox->addressOf(idx, *ipv4) ? 1 : 0;
}

int EmbeddedDriver::wakeFds(int* out, int max) const noexcept {
    if (mode_ != DriverMode::Invoker || driver_ == nullptr) return 0;
    return rune_driver_wake_fds(driver_, out, max);
}

int EmbeddedDriver::epollSelectedForTest(bool reset) noexcept {
    return driver_ == nullptr ? 0 : rune_driver_epoll_selected_for_test(driver_, reset ? 1 : 0);
}

std::string EmbeddedDriver::idleStrategyArgsForTest() const {
    if (ctx_ == nullptr) return {};
    const char* a = cfg_.threadingMode == NodeConfig::AeronThreadingMode::Dedicated
                        ? aeron_driver_context_get_conductor_idle_strategy_init_args(ctx_)
                        : aeron_driver_context_get_shared_idle_strategy_init_args(ctx_);
    return a != nullptr ? std::string(a) : std::string();
}

int EmbeddedDriver::doWork() noexcept {
    if (mode_ == DriverMode::Threaded || std::this_thread::get_id() != pumpOwner_) [[unlikely]] {
        spdlog::error("EmbeddedDriver: doWork() refused — not this driver's pump thread "
                      "(dir={}, mode={})", cfg_.dir,
                      mode_ == DriverMode::Invoker ? "invoker" : "threaded");
        reportMisuse_(kForeignPump);   // exactly one pumper
        return 0;
    }
    return callerPump_();
}

int EmbeddedDriver::pumpTrampoline_(void* self) noexcept {
    auto* d = static_cast<EmbeddedDriver*>(self);
    if (d->mode_ == DriverMode::Threaded || std::this_thread::get_id() != d->pumpOwner_) [[unlikely]] {
        d->reportMisuse_(kForeignPump);
        spdlog::error("EmbeddedDriver: AeronPump refused — not this driver's pump thread "
                      "(dir={}, mode={}); resolve fails fast", d->cfg_.dir,
                      d->mode_ == DriverMode::Invoker ? "invoker" : "threaded");
        return rune::aeronutil::AeronPump::kRefused;
    }
    return d->callerPump_();
}

void EmbeddedDriver::close() noexcept {
    if (mode_ == DriverMode::Invoker && driver_ &&
        std::this_thread::get_id() != pumpOwner_) [[unlikely]] {
        // Draining here would make this thread a SECOND pumper while the bound
        // owner may still be running.  Refuse loudly and leave the driver intact.
        // The owner closes it, or a closer takes over via bindPumpThread() after
        // joining the owner.
        reportMisuse_(kForeignClose);
        spdlog::error("EmbeddedDriver: close() refused from non-pumper thread (dir={}); "
                      "the bound pump thread must close, or bindPumpThread() first", cfg_.dir);
        return;
    }
    if (mode_ == DriverMode::Threaded && threadRunning_) {
        // Controller-only (Threaded close is a controller operation): one Stop
        // command — the ring is otherwise empty, so the offer cannot fail.
        (void)stopRing_.offer(kStopCmd);
        if (thread_.joinable()) thread_.join();
        threadRunning_ = false;
    }
    // The driver thread (if any) has exited: the closing thread is now the ONLY
    // pumper.  Drain the client-close commands still queued in the CnC ring before
    // aeron_driver_close (prevents the conductor on_close use-after-free).
    if (driver_) {
        constexpr int kMaxDrainPumps = 10'000;
        int idle = 0;
        for (int i = 0; i < kMaxDrainPumps && idle < 2; ++i)
            idle = (callerPump_() == 0) ? idle + 1 : 0;
        if (aeron_driver_close(driver_) < 0)
            spdlog::warn("EmbeddedDriver: aeron_driver_close error (dir={}): {}", cfg_.dir, aeron_errmsg());
        driver_ = nullptr;
    }
    if (ctx_) {
        if (aeron_driver_context_close(ctx_) < 0)
            spdlog::warn("EmbeddedDriver: aeron_driver_context_close error (dir={}): {}", cfg_.dir, aeron_errmsg());
        ctx_ = nullptr;
    }
    releaseGso_();
}

void EmbeddedDriver::releaseGso_() noexcept {
    rune_driver_epoll_poll_free(epollBindings_);   // after aeron_driver_context_close (Aeron pointed at it)
    epollBindings_ = nullptr;
    if (gso_ == nullptr) return;
    rune_gso_bindings_totals(gso_, &gsoTotals_);
    if (gsoTotals_.gso_fallbacks > 0 || gsoTotals_.gro_disabled > 0 || gsoTotals_.gso_path_mtu_fallbacks > 0)
        spdlog::warn("EmbeddedDriver[{}]: udp runtime fallbacks: gso_fallbacks={} (transports, GSO off) "
                     "path MTU below the Aeron MTU: {} GSO groups sent plainly (per destination, re-probed) "
                     "gro_disabled={} gro_trunc_drops={}", cfg_.dir, gsoTotals_.gso_fallbacks,
                     gsoTotals_.gso_path_mtu_fallbacks, gsoTotals_.gro_disabled, gsoTotals_.gro_trunc_drops);
    rune_gso_bindings_delete(gso_);
    gso_ = nullptr;
}

EmbeddedDriver::~EmbeddedDriver() {
    close();
    if (driver_) {
        // close() was refused (Invoker destroyed off its pump thread): the driver,
        // its context and its dir are leaked.  Never silent: shard teardown must
        // hand the driver to the closing thread first (join the pumper, then
        // bindPumpThread(), then close()).  Debug builds abort on the spot.
        spdlog::critical("EmbeddedDriver: destroyed off its pump thread without a hand-off — "
                         "driver LEAKED (dir={}); join the pumper and bindPumpThread() before close()",
                         cfg_.dir);
#ifndef NDEBUG
        std::abort();
#endif
    }
}

} // namespace rune::node
