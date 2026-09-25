// src/transport/AeronTransport.cpp
//
// AeronTransport implementation.
//
// Template: /rune-broker/docker/ping-probe/ping_probe.cpp (driver boot + pub/sub)
//           /rune-broker/libs/node/src/BrokerNode.cpp     (teardown ORDER — BR-3b)
//
// Key design decisions:
//   - SHARED threading mode: one driver thread for conductor+sender+receiver.
//   - aeronDir = dataDir/aeron (must match the aeron::Context aeronDir — gotcha).
//   - Teardown order: reset pub/sub → reset aeron client → stop+join driver
//     thread → drain CnC → aeron_driver_close → aeron_driver_context_close.
//     (This order prevents the BR-3b use-after-free.)
//   - tryClaim / fallback to offer for zero-copy send.
//   - FragmentAssembler wraps all poll calls for multi-frame reassembly.

#include <rune/client/transport/AeronTransport.h>

#include <cassert>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::client::transport {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

AeronTransport::AeronTransport(TransportConfig cfg)
    : cfg_(std::move(cfg))
{}

AeronTransport::~AeronTransport() {
    stop();
}

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

void AeronTransport::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // already started
    }

    // 1. Create dataDir (and the aeron sub-dir will be created by the driver).
    std::error_code ec;
    fs::create_directories(cfg_.dataDir, ec);
    if (ec) {
        throw std::runtime_error("AeronTransport: cannot create dataDir '" +
                                 cfg_.dataDir + "': " + ec.message());
    }

    // 2. Start the embedded media driver.
    startEmbeddedDriver();

    // 3. Connect the Aeron C++ client — aeronDir MUST match the driver's dir.
    const std::string aeronDir = (fs::path{cfg_.dataDir} / "aeron").string();
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) {
            stopEmbeddedDriver();
            throw std::runtime_error("AeronTransport: Aeron::connect failed");
        }
    }

    // 4. Register ingress ExclusivePublication + egress Subscription.
    int64_t pubRegId = aeron_->addExclusivePublication(cfg_.ingressChannel,
                                                        cfg_.ingressStreamId);
    int64_t subRegId = aeron_->addSubscription(cfg_.egressChannel,
                                                cfg_.egressStreamId);

    // 5. Resolve both within kResolveMaxMs (bounded — no busy-spin).
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds{kResolveMaxMs};

    while (std::chrono::steady_clock::now() < deadline) {
        if (!ingressPub_) ingressPub_ = aeron_->findExclusivePublication(pubRegId);
        if (!egressSub_)  egressSub_  = aeron_->findSubscription(subRegId);
        if (ingressPub_ && egressSub_) break;
        std::this_thread::sleep_for(std::chrono::microseconds{kResolveSliceUs});
    }

    if (!ingressPub_ || !egressSub_) {
        // Release partially-resolved resources before tearing down.
        ingressPub_.reset();
        egressSub_.reset();
        aeron_.reset();
        stopEmbeddedDriver();
        throw std::runtime_error(
            "AeronTransport: timed out resolving ExclusivePublication/Subscription "
            "(ingressChannel=" + cfg_.ingressChannel + " egressChannel=" + cfg_.egressChannel + ")");
    }

    // 6. Build the FragmentAssembler (installed fresh each start; owns a lambda
    //    that captures nothing — the handler is passed in at poll() time).
    //    We use a wrapper that stores a pointer to the current poll Handler so we
    //    can change the handler each poll() call without rebuilding the assembler.
    //    The assembler holds per-session reassembly state so it must NOT be reset
    //    between poll() calls.
    assembler_ = std::make_unique<aeron::FragmentAssembler>(
        [this](aeron::concurrent::AtomicBuffer& buf,
               aeron::util::index_t            offset,
               aeron::util::index_t            len,
               aeron::Header&) {
            if (pollHandler_) {
                const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                (*pollHandler_)(p, static_cast<std::size_t>(len));
            }
        });
}

// ---------------------------------------------------------------------------
// stop (idempotent, teardown order = BR-3b)
// ---------------------------------------------------------------------------

void AeronTransport::stop() noexcept {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // already stopped (or never started)
    }

    // Step 1: Release the FragmentAssembler (no Aeron resources held directly).
    assembler_.reset();

    // Step 2: Release pub/sub BEFORE closing the aeron client.
    //   Resetting shared_ptrs triggers deregistration commands into the conductor.
    //   pubCache_ (per-worker ingress ExclusivePublications) MUST be released here
    //   too.  Each ExclusivePublication's destructor calls
    //   ClientConductor::releaseExclusivePublication(), which locks the conductor's
    //   admin mutex.  If left to member-teardown they outlive aeron_.reset() below
    //   and lock a destroyed recursive_mutex → EINVAL → std::terminate (observed on
    //   client shutdown whenever per-worker ingress had populated the cache).
    ingressPub_.reset();
    egressSub_.reset();
    pubCache_.clear();

    // Step 3: Close the Aeron C++ client (joins the conductor thread; the
    //   conductor's onClose() flushes remaining commands into the CnC ring).
    if (aeron_) {
        aeron_.reset();
    }

    // Step 4: Stop the embedded driver (join thread → drain CnC → close driver).
    stopEmbeddedDriver();
}

// ---------------------------------------------------------------------------
// offerToPub — shared implementation for default and per-worker offer paths
// ---------------------------------------------------------------------------

bool AeronTransport::offerToPub(aeron::ExclusivePublication& pub,
                                 const uint8_t* buf,
                                 std::size_t    len) noexcept
{
    const auto ilen = static_cast<aeron::util::index_t>(len);
    try {
        if (len > 0 && ilen <= pub.maxPayloadLength()) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            const int64_t claimPos = pub.tryClaim(ilen, claim);
            if (claimPos > 0) {
                std::memcpy(claim.buffer().buffer() + claim.offset(), buf, len);
                claim.commit();
                return true;
            }
        }
        static uint8_t kEmptyBuf[1] = {};
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(len > 0 ? buf : kEmptyBuf),
            ilen);
        const int64_t pos = pub.offer(ab, 0, ilen);
        return pos > 0;
    } catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// offer (default stream)
// ---------------------------------------------------------------------------

bool AeronTransport::offer(const uint8_t* buf, std::size_t len) noexcept {
    if (!ingressPub_) return false;

    // This method is noexcept but the Aeron publication calls below can throw
    // (e.g. IllegalStateException on a closed pub during a racing teardown).
    // The single-writer lifetime contract makes that unreachable in normal use,
    // but a throw escaping a noexcept fn would std::terminate — so we contain it
    // and report a failed send instead. Aeron never mutates the source buffer.
    return offerToPub(*ingressPub_, buf, len);
}

// ---------------------------------------------------------------------------
// resolveWorkerPub — resolve or retrieve a per-worker publication
// ---------------------------------------------------------------------------

std::shared_ptr<aeron::ExclusivePublication>
AeronTransport::resolveWorkerPub(int32_t ingressStreamId) noexcept
{
    if (!aeron_) return nullptr;

    // Check the cache first.
    auto it = pubCache_.find(ingressStreamId);
    if (it != pubCache_.end()) {
        return it->second;
    }

    // Resolve a new publication (bounded, same timeout as start()).
    try {
        const int64_t regId =
            aeron_->addExclusivePublication(cfg_.ingressChannel, ingressStreamId);

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds{kResolveMaxMs};

        std::shared_ptr<aeron::ExclusivePublication> pub;
        while (std::chrono::steady_clock::now() < deadline) {
            pub = aeron_->findExclusivePublication(regId);
            if (pub) break;
            std::this_thread::sleep_for(std::chrono::microseconds{kResolveSliceUs});
        }

        if (pub) {
            pubCache_[ingressStreamId] = pub;
        }
        return pub;
    } catch (...) {
        return nullptr;
    }
}

// ---------------------------------------------------------------------------
// offer (per-worker stream)
// ---------------------------------------------------------------------------

bool AeronTransport::offer(int32_t ingressStreamId,
                            const uint8_t* buf,
                            std::size_t    len) noexcept
{
    // 0 means "legacy / no per-worker stream" — fall back to default.
    if (ingressStreamId == 0 || ingressStreamId == cfg_.ingressStreamId) {
        return offer(buf, len);
    }

    auto pub = resolveWorkerPub(ingressStreamId);
    if (!pub) {
        // Resolution failed — fall back to the default publication.
        return offer(buf, len);
    }

    return offerToPub(*pub, buf, len);
}

// ---------------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------------

int AeronTransport::poll(const Handler& handler, int fragmentLimit) {
    if (!egressSub_ || !assembler_) return 0;

    // Store handler pointer for this poll cycle.  The lambda in the
    // FragmentAssembler reads pollHandler_ during the callback.
    pollHandler_ = &handler;
    const int frags = egressSub_->poll(assembler_->handler(), fragmentLimit);
    pollHandler_ = nullptr;
    return frags;
}

// ---------------------------------------------------------------------------
// Readiness
// ---------------------------------------------------------------------------

bool AeronTransport::ingressConnected() const noexcept {
    return ingressPub_ && ingressPub_->isConnected();
}

bool AeronTransport::egressConnected() const noexcept {
    return egressSub_ && egressSub_->isConnected();
}

// ---------------------------------------------------------------------------
// retarget — release current ingress pub, create new one on newIngressChannel
// ---------------------------------------------------------------------------

bool AeronTransport::retarget(const std::string& newIngressChannel) noexcept {
    // FIX F8: noexcept + non-throwing.  Any Aeron call below can throw
    // (addExclusivePublication / findExclusivePublication on a closed client);
    // contain it so nothing escapes into the agent's noexcept dispatch path
    // (→ std::terminate).  On ANY failure we leave the OLD ingressPub_ intact.
    try {
        if (!aeron_) {
            return false;  // not started — cannot retarget, old state untouched
        }

        // Resolve the NEW default publication BEFORE releasing the old one, so a
        // failed resolve leaves the old pub usable (offer() keeps working).
        const int64_t pubRegId =
            aeron_->addExclusivePublication(newIngressChannel, cfg_.ingressStreamId);

        // Bounded resolution (kResolveMaxMs hard cap — no busy-spin).
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds{kResolveMaxMs};

        std::shared_ptr<aeron::ExclusivePublication> newPub;
        while (std::chrono::steady_clock::now() < deadline) {
            newPub = aeron_->findExclusivePublication(pubRegId);
            if (newPub) break;
            std::this_thread::sleep_for(std::chrono::microseconds{kResolveSliceUs});
        }

        if (!newPub) {
            // Resolve timed out — keep the old pub; report failure.
            return false;
        }

        // Success: update channel, swap default pub, clear per-worker pub cache.
        // E5.1: REDIRECT changes channel — per-worker publications are on the new
        // channel.  Clear the cache so they are lazily re-resolved with the new
        // channel on next use (stream ids are PRESERVED — only the channel changes).
        cfg_.ingressChannel = newIngressChannel;
        ingressPub_ = std::move(newPub);
        pubCache_.clear();  // E5.1: lazy re-resolve with new channel on next offer
        return true;
    } catch (...) {
        // Any Aeron exception → treat as a failed retarget; old pub intact.
        return false;
    }
}

// ---------------------------------------------------------------------------
// startEmbeddedDriver — mirrors ping_probe.cpp + BrokerNode::startEmbeddedDriver
// ---------------------------------------------------------------------------

void AeronTransport::startEmbeddedDriver() {
    const std::string aeronDir = (fs::path{cfg_.dataDir} / "aeron").string();

    if (aeron_driver_context_init(&driverCtx_) < 0) {
        throw std::runtime_error(
            std::string("AeronTransport: aeron_driver_context_init failed: ") +
            aeron_errmsg());
    }

    // Silent-failure guard: if an ACTIVE media driver already holds this aeron
    // dir, dir_delete_on_start cannot wipe it and the start degrades into a
    // cryptic init failure or an opaque produce timeout (the stale-dir hang we
    // hit under SIGKILL'd prior clients).  Detect it and fail LOUDLY with a
    // clear, actionable message instead.
    if (aeron_is_driver_active(aeronDir.c_str(), /*timeout_ms=*/1000,
                               +[](const char*){})) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
        throw std::runtime_error(
            "AeronTransport: aeron dir '" + aeronDir + "' is already in use by an "
            "ACTIVE media driver (a prior client instance may still be running, or "
            "was killed without clean shutdown so its driver is still holding the "
            "dir). Use a distinct dataDir, or ensure the previous instance exited.");
    }

    aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());
    aeron_driver_context_set_threading_mode(driverCtx_, AERON_THREADING_MODE_SHARED);
    aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
    aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
    aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
    aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
    aeron_driver_context_set_term_buffer_length(
        driverCtx_, static_cast<uint64_t>(cfg_.termBufferLength));

    if (aeron_driver_init(&driver_, driverCtx_) < 0) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
        throw std::runtime_error(
            std::string("AeronTransport: aeron_driver_init failed: ") +
            aeron_errmsg());
    }

    if (aeron_driver_start(driver_, /*manual_main_loop=*/true) < 0) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(driverCtx_);
        driver_    = nullptr;
        driverCtx_ = nullptr;
        throw std::runtime_error(
            std::string("AeronTransport: aeron_driver_start failed: ") +
            aeron_errmsg());
    }

    driverRunning_.store(true, std::memory_order_release);
    driverThread_ = std::thread([this]() {
        while (driverRunning_.load(std::memory_order_acquire)) {
            aeron_driver_main_idle_strategy(
                driver_,
                aeron_driver_main_do_work(driver_));
        }
    });

    // Wait for cnc.dat (bounded) — mirrors BrokerNode::startEmbeddedDriver.
    const std::string cncPath = aeronDir + "/cnc.dat";
    const auto readyDeadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{200};
    while (std::chrono::steady_clock::now() < readyDeadline) {
        aeron_driver_main_do_work(driver_);
        if (fs::exists(cncPath)) {
            aeron_driver_main_do_work(driver_);
            aeron_driver_main_do_work(driver_);
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    if (!fs::exists(cncPath)) {
        throw std::runtime_error(
            "AeronTransport: Aeron CnC file not created within timeout: " + cncPath);
    }
}

// ---------------------------------------------------------------------------
// stopEmbeddedDriver — mirrors BrokerNode::stopEmbeddedDriver (BR-3b order)
// ---------------------------------------------------------------------------

void AeronTransport::stopEmbeddedDriver() noexcept {
    // Step A: signal the driver thread to stop and join it.
    if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
        if (driverThread_.joinable()) {
            driverThread_.join();
        }
    }

    // Step B: drain any CnC commands the conductor posted that the driver
    // thread did not yet process (we are the ONLY thread now — no data race).
    if (driver_) {
        static constexpr int kDrainMaxMs   = 50;
        static constexpr int kDrainSliceUs = 200;
        const auto drainDeadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds{kDrainMaxMs};
        while (std::chrono::steady_clock::now() < drainDeadline) {
            const int work = aeron_driver_main_do_work(driver_);
            if (work == 0) {
                aeron_driver_main_do_work(driver_);
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds{kDrainSliceUs});
        }
    }

    // Step C: close the driver.
    if (driver_) {
        aeron_driver_close(driver_);
        driver_ = nullptr;
    }

    // Step D: close the driver context.
    if (driverCtx_) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
    }
}

} // namespace rune::client::transport
