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

#include "rune/protocol/net/rune_driver_wake.h"
#include <rune/client/transport/AeronTransport.h>
#include "rune/protocol/net/rune_udp_gso.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rune::client::transport {

namespace {
struct PubRegistry {
    aeron::Aeron&      aeron;
    const std::string& channel;
    int32_t            streamId;
    int64_t add() { return aeron.addExclusivePublication(channel, streamId); }
    std::shared_ptr<aeron::ExclusivePublication> find(int64_t id) {
        return aeron.findExclusivePublication(id);
    }
};
struct SubRegistry {
    aeron::Aeron&      aeron;
    const std::string& channel;
    int32_t            streamId;
    int64_t add() { return aeron.addSubscription(channel, streamId); }
    std::shared_ptr<aeron::Subscription> find(int64_t id) { return aeron.findSubscription(id); }
};
} // namespace

int64_t AeronTransport::monoNs_() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

template <class Ready>
bool AeronTransport::pumpUntil_(Ready&& ready, int64_t timeoutNs) {
    const int64_t deadline = monoNs_() + timeoutNs;
    for (;;) {
        dutyCycle();
        if (ready()) return true;
        if (monoNs_() >= deadline) return false;
        if (cfg_.driverMode == DriverMode::Thread) {
            // Background driver/conductor threads do the work; back off briefly.
            std::this_thread::sleep_for(std::chrono::microseconds{50});
        }
    }
}

int AeronTransport::wakeFds(int* out, int max) const noexcept {
    if (cfg_.driverMode != DriverMode::Invoker || driver_ == nullptr) return 0;
    return rune_driver_wake_fds(driver_, out, max);
}

int AeronTransport::epollSelectedForTest(bool reset) noexcept {
    return driver_ == nullptr ? 0 : rune_driver_epoll_selected_for_test(driver_, reset ? 1 : 0);
}

int AeronTransport::dutyCycle() noexcept {
    if (cfg_.driverMode != DriverMode::Invoker || driver_ == nullptr) return 0;
    // spec §3.1 order: driver, then client conductor.  T15 R2: the driver
    // refreshes its heartbeat before the conductor checks it — a second line of
    // defence after mediaDriverTimeoutMsFor(); nothing relies on it alone.
    int work = pumpDriverFromCaller_();
    if (pumpObserver_ != nullptr) [[unlikely]] pumpObserver_(pumpObserverCtx_, PumpStep::Driver);
    if (aeron_) {
        try {
            work += aeron_->conductorAgentInvoker().invoke();
        } catch (...) {
            // AgentInvoker routes SourcedException to the error handler; anything
            // else is contained here — dutyCycle() is noexcept.
        }
        if (pumpObserver_ != nullptr) [[unlikely]] pumpObserver_(pumpObserverCtx_, PumpStep::Conductor);
    }
    return work;
}

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

AeronTransport::AeronTransport(TransportConfig cfg)
    : cfg_(std::move(cfg))
    , maxMessageLength_(computeMaxMessageLength(cfg_))
    , sealSupplier_([this](aeron::concurrent::AtomicBuffer& term, aeron::util::index_t termOffset,
                           aeron::util::index_t) -> std::int64_t { return sealInTerm_(term, termOffset); })
{}

std::size_t AeronTransport::computeMaxMessageLength(const TransportConfig& c) noexcept {
    std::size_t term = c.termBufferLength;
    const std::string key = "term-length=";
    const auto pos = c.ingressChannel.find(key);
    if (pos != std::string::npos) {
        std::size_t v = 0;
        for (std::size_t i = pos + key.size();
             i < c.ingressChannel.size() && c.ingressChannel[i] >= '0' && c.ingressChannel[i] <= '9'; ++i) {
            v = v * 10 + static_cast<std::size_t>(c.ingressChannel[i] - '0');
        }
        if (v != 0 && v < term) term = v;
    }
    constexpr std::size_t kAeronMaxMessage = 16u * 1024 * 1024;  // AERON_FRAME_MAX_MESSAGE_LENGTH cap
    const std::size_t m = term / 8;
    return m < kAeronMaxMessage ? m : kAeronMaxMessage;
}

AeronTransport::~AeronTransport() {
    stop();
}

// ---------------------------------------------------------------------------
// start
// ---------------------------------------------------------------------------

void AeronTransport::start() {
    if (started_) return;  // already started (owner state)
    started_ = true;
    bindPumpThread();      // the starting thread owns every pump until a handoff

    if (auto err = validate(cfg_)) {
        throw std::invalid_argument("AeronTransport: invalid TransportConfig: " + *err);
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
        ctx.useConductorAgentInvoker(cfg_.driverMode == DriverMode::Invoker);
        // T15 R2: the driver timeout follows the liveness when one thread pumps
        // both driver and conductor (unchanged 10 s with a driver thread).
        static_assert(kAeronDefaultMediaDriverTimeoutMs == aeron::DEFAULT_MEDIA_DRIVER_TIMEOUT_MS);
        ctx.mediaDriverTimeout(mediaDriverTimeoutMsFor(cfg_));
        // Aeron's default handler calls ::exit(-1) (Context.h:145-161).  A client
        // library must never exit its host: log, and let closed() report it.
        ctx.errorHandler([](const std::exception& e) {
            std::fprintf(stderr, "rune-client: aeron client error: %s\n", e.what());
        });
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) {
            stopEmbeddedDriver();
            throw std::runtime_error("AeronTransport: Aeron::connect failed");
        }
    }

    // 4-5. Register + resolve the ingress ExclusivePublication and egress
    //      Subscription, pumping THIS transport's driver + conductor (invoker) or
    //      letting the background agents run (thread).  Bounded by kResolveMaxMs.
    Resolution<aeron::ExclusivePublication> pubRes{kResolveTimeoutNs, kResolveRetryNs};
    Resolution<aeron::Subscription>         subRes{kResolveTimeoutNs, kResolveRetryNs};
    PubRegistry preg{*aeron_, cfg_.ingressChannel, cfg_.ingressStreamId};
    SubRegistry sreg{*aeron_, cfg_.egressChannel, cfg_.egressStreamId};
    pumpUntil_([&] {
        const int64_t now = resolveNow_();
        const bool pubOk = pubRes.poll(preg, now) == ResolveStatus::Ready;
        const bool subOk = subRes.poll(sreg, now) == ResolveStatus::Ready;
        return (pubOk && subOk) || pubRes.rejected() || subRes.rejected();
    }, kResolveTimeoutNs);
    if (pubRes.ready() == nullptr || subRes.ready() == nullptr) {
        const std::string why = pubRes.rejected() || subRes.rejected() ? "rejected by the driver"
                                                                        : "timed out";
        aeron_.reset();
        stopEmbeddedDriver();
        throw std::runtime_error("AeronTransport: resolving ExclusivePublication/Subscription " + why +
                                 " (ingressChannel=" + cfg_.ingressChannel +
                                 " egressChannel=" + cfg_.egressChannel + ")");
    }
    ingressPub_ = *pubRes.readyShared();
    egressSub_  = *subRes.readyShared();

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
            if (pollThunk_) {
                pollThunk_(pollCtx_, reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                           static_cast<std::size_t>(len));
            }
        });
}

// ---------------------------------------------------------------------------
// stop (idempotent, teardown order = BR-3b)
// ---------------------------------------------------------------------------

void AeronTransport::stop() noexcept {
    if (stopped_) return;  // already stopped (owner state)
    stopped_ = true;
    // Precondition (see the header): no other thread pumps any more — the
    // caller takes pump ownership for the final driver drain below.
    bindPumpThread();

    // Step 1: Release the FragmentAssembler (no Aeron resources held directly).
    assembler_.reset();
    // read-rework I4: release per-worker egress subscriptions + their assemblers
    // BEFORE closing the aeron client (same recursive_mutex teardown reason as
    // the publication slots below — Subscription dtors deregister via the conductor).
    subSlots_.clear();                  // assemblers + subscriptions (conductor dereg)

    // Step 2: Release pub/sub BEFORE closing the aeron client.
    //   Resetting shared_ptrs triggers deregistration commands into the conductor.
    //   pubSlots_ (per-stream / per-endpoint ExclusivePublications) MUST be released here
    //   too.  Each ExclusivePublication's destructor calls
    //   ClientConductor::releaseExclusivePublication(), which locks the conductor's
    //   admin mutex.  If left to member-teardown they outlive aeron_.reset() below
    //   and lock a destroyed recursive_mutex → EINVAL → std::terminate (observed on
    //   client shutdown whenever per-worker ingress had populated the cache).
    ingressPub_.reset();
    egressSub_.reset();
    pubSlots_.clear();                  // every per-stream / per-endpoint publication
    streamPub_.clear();                 // in place: stop() is noexcept, no allocation

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

AeronTransport::OfferResult AeronTransport::offerToPub(aeron::ExclusivePublication& pub,
                                                       const uint8_t* buf,
                                                       std::size_t    len,
                                                       FrameCrc       crc) noexcept
{
    // Aeron result codes: NOT_CONNECTED(-1), BACK_PRESSURED(-2),
    // ADMIN_ACTION(-3) are transient; PUBLICATION_CLOSED(-4) and
    // MAX_POSITION_EXCEEDED(-5) are permanent; a throw (e.g. a frame over the
    // publication's max message length) is permanent too.
    auto verdict = [this, &pub](int64_t pos) noexcept {
        if (pos > 0) return OfferResult::Accepted;
        // FR: refused (arg = -position: 1 NOT_CONNECTED, 2 BACK_PRESSURED,
        // 3 ADMIN_ACTION, 4 CLOSED, 5 MAX_POSITION) — throttled.
        RUNE_FR_THROTTLED_PRODUCE(frOfferRefused_, rune_fr::Site::ClientTransportRefused,
                                  static_cast<uint32_t>(pub.streamId()), static_cast<uint16_t>(-pos));
        if (pos == aeron::PUBLICATION_CLOSED || pos == aeron::MAX_POSITION_EXCEEDED) {
            return OfferResult::Fatal;
        }
        return OfferResult::Retry;
    };
    const auto ilen = static_cast<aeron::util::index_t>(len);
    // B1: a pending CRC is sealed into the producer's own (writable) frame.
    uint8_t* const sealable = crc.pending() ? const_cast<uint8_t*>(buf) : nullptr;
    try {
        if (len > 0 && ilen <= pub.maxPayloadLength()) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            const int64_t claimPos = pub.tryClaim(ilen, claim);
            if (claimPos > 0) {
                // Accepted: the space is ours — only now is the CRC computed.
                if (sealable != nullptr) (void)sealFrame(sealable, len, crc);
                std::memcpy(claim.buffer().buffer() + claim.offset(), buf, len);
                claim.commit();
                return OfferResult::Accepted;
            }
            return verdict(claimPos);
        }
        static uint8_t kEmptyBuf[1] = {};
        aeron::concurrent::AtomicBuffer ab(
            const_cast<uint8_t*>(len > 0 ? buf : kEmptyBuf),
            ilen);
        if (sealable == nullptr) return verdict(pub.offer(ab, 0, ilen));
        // Multi-fragment frame: Aeron calls the supplier per fragment after its
        // position < limit check and the fragment copy, before that fragment's
        // commit.  The CRC field sits in the Produce + RecordBatch headers, so
        // in the FIRST fragment for any legal MTU; the supplier computes the CRC
        // over the whole source frame (all of it is in `buf`) and patches both.
        if (static_cast<std::size_t>(crc.crcAt) + 4 > static_cast<std::size_t>(pub.maxPayloadLength())) [[unlikely]] {
            (void)sealFrame(sealable, len, crc);  // unreachable for a Produce frame (header ~110 B)
            return verdict(pub.offer(ab, 0, ilen));
        }
        seal_ = PendingSeal{sealable, len, crc, true};
        const int64_t pos = pub.offer(ab, 0, ilen, sealSupplier_);
        if (pos > 0 && seal_.armed) [[unlikely]] {
            // Accepted but the supplier never ran: the frame left with CRC 0 and every retransmit
            // would patch an invalid CRC.  Only an Aeron change of when the supplier runs can do
            // this — an invariant violation, never a silent corrupt send.
            std::fprintf(stderr, "rune-client FATAL: offer accepted without sealing the frame CRC "
                                 "(Aeron reserved-value supplier contract changed?)\n");
            std::abort();
        }
        seal_.armed = false;  // refused: never sealed (the next attempt re-arms)
        return verdict(pos);
    } catch (...) {
        seal_.armed = false;
        return OfferResult::Fatal;
    }
}

std::int64_t AeronTransport::sealInTerm_(aeron::concurrent::AtomicBuffer& term,
                                         aeron::util::index_t termOffset) noexcept {
    if (seal_.armed) {  // the BEGIN fragment: the offer was accepted
        seal_.armed = false;
        const uint32_t c = sealFrame(seal_.frame, seal_.len, seal_.crc);
        putCrc(term.buffer() + termOffset + aeron::concurrent::logbuffer::DataFrameHeader::LENGTH +
                   seal_.crc.crcAt,
               c);
    }
    return 0;  // the reserved value Aeron's DEFAULT_RESERVED_VALUE_SUPPLIER writes
}

// ---------------------------------------------------------------------------
// offer (default stream)
// ---------------------------------------------------------------------------

bool AeronTransport::offer(const uint8_t* buf, std::size_t len, FrameCrc crc) noexcept {
    if (!ingressPub_) return false;

    // This method is noexcept but the Aeron publication calls below can throw
    // (e.g. IllegalStateException on a closed pub during a racing teardown).
    // The single-writer lifetime contract makes that unreachable in normal use,
    // but a throw escaping a noexcept fn would std::terminate — so we contain it
    // and report a failed send instead. Aeron never mutates the source buffer.
    return offerToPub(*ingressPub_, buf, len, crc) == OfferResult::Accepted;
}

AeronTransport::OfferResult AeronTransport::offerResult(const uint8_t* buf, std::size_t len) noexcept {
    if (!ingressPub_) return OfferResult::Retry;  // not resolved yet
    return offerToPub(*ingressPub_, buf, len);
}

// ---------------------------------------------------------------------------
// Publication handles — resolved asynchronously, never sleep (spec §3.6)
// ---------------------------------------------------------------------------

AeronTransport::PubHandle AeronTransport::publicationHandle(const std::string& channel,
                                                            int32_t streamId) {
    for (std::size_t i = 0; i < pubSlots_.size(); ++i) {
        if (pubSlots_[i].streamId == streamId && pubSlots_[i].channel == channel) {
            return static_cast<PubHandle>(i);
        }
    }
    pubSlots_.push_back(PubSlot{channel, streamId,
                                Resolution<aeron::ExclusivePublication>{kResolveTimeoutNs,
                                                                        kResolveRetryNs}});
    return static_cast<PubHandle>(pubSlots_.size() - 1);
}

bool AeronTransport::offerOn(PubHandle h, const uint8_t* buf, std::size_t len, FrameCrc crc) noexcept {
    if (h < 0 || static_cast<std::size_t>(h) >= pubSlots_.size()) return false;
    PubSlot& s = pubSlots_[static_cast<std::size_t>(h)];
    if (aeron::ExclusivePublication* p = s.res.ready()) return offerToPub(*p, buf, len, crc) == OfferResult::Accepted;
    if (!aeron_) return false;
    PubRegistry reg{*aeron_, s.channel, s.streamId};
    if (s.res.poll(reg, resolveNow_()) != ResolveStatus::Ready) return false;
    return offerToPub(*s.res.ready(), buf, len, crc) == OfferResult::Accepted;
}

// ---------------------------------------------------------------------------
// client<->broker TLS/AEAD (Task 6) — reusable seal scratch
// ---------------------------------------------------------------------------

uint8_t* AeronTransport::sealScratch(std::size_t minCap) noexcept {
    // noexcept contract: std::vector::resize can throw (bad_alloc) — contained
    // here, same reasoning as offer()'s own noexcept-wraps-a-throwing-call note
    // above.  A failed grow returns nullptr; the caller (offerProduceToLeader_)
    // treats that as a seal failure and drops the frame rather than risk
    // writing past a too-small buffer.
    if (cryptoSealScratch_.size() < minCap) {
        try {
            cryptoSealScratch_.resize(minCap);
        } catch (...) {
            return nullptr;
        }
    }
    return cryptoSealScratch_.data();
}

// ---------------------------------------------------------------------------
// offer (per-worker stream)
// ---------------------------------------------------------------------------

bool AeronTransport::offer(int32_t ingressStreamId, const uint8_t* buf, std::size_t len,
                           FrameCrc crc) noexcept {
    // 0 means "legacy / no per-worker stream" — fall back to default.
    if (ingressStreamId == 0 || ingressStreamId == cfg_.ingressStreamId) return offer(buf, len, crc);
    uint32_t h = streamPub_.find(ingressStreamId);
    if (h == detail::FlatIndexMap::kNone) {
        try {
            h = static_cast<uint32_t>(publicationHandle(cfg_.ingressChannel, ingressStreamId));
            streamPub_.insert(ingressStreamId, h);
        } catch (...) {
            return false;  // back-pressure: the caller keeps the batch and retries
        }
    }
    // Resolving or back-pressured -> false.  Never fall back to the default
    // publication: one partition's batches must stay on ONE stream, or the broker
    // may process consecutive in-flight batches out of order.
    return offerOn(static_cast<PubHandle>(h), buf, len, crc);
}

// ---------------------------------------------------------------------------
// Task 1.3: offerTo — publish to an explicit (channel, ingressStreamId)
// ---------------------------------------------------------------------------

bool AeronTransport::offerTo(const std::string& channel, int32_t ingressStreamId,
                             const uint8_t* buf, std::size_t len) noexcept {
    // An empty channel means "no route yet" — report a failed send.
    if (channel.empty()) return false;
    // The default ingress channel keeps the single-node per-worker path.
    if (channel == cfg_.ingressChannel) return offer(ingressStreamId, buf, len);
    PubHandle h = kNoPub;
    try {
        h = publicationHandle(channel,
                              ingressStreamId != 0 ? ingressStreamId : cfg_.ingressStreamId);
    } catch (...) {
        return false;
    }
    return offerOn(h, buf, len);
}

// ---------------------------------------------------------------------------
// poll
// ---------------------------------------------------------------------------

aeron::Subscription* AeronTransport::resolveSubSlot_(SubSlot& s) noexcept {
    if (!aeron_) return nullptr;
    SubRegistry reg{*aeron_, s.channel, s.streamId};
    if (s.res.poll(reg, resolveNow_()) != ResolveStatus::Ready) return nullptr;
    try {
        s.assembler = std::make_unique<aeron::FragmentAssembler>(
            [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                   aeron::util::index_t len, aeron::Header&) {
                if (pollThunk_) {
                    pollThunk_(pollCtx_, reinterpret_cast<const uint8_t*>(buf.buffer() + offset),
                               static_cast<std::size_t>(len));
                }
            });
    } catch (...) {
        s.res.reset();
        return nullptr;
    }
    return s.res.ready();
}

// ---------------------------------------------------------------------------
// read-rework I4: open a per-worker dynamic-MDC egress subscription (idempotent).
// ---------------------------------------------------------------------------

void AeronTransport::addEgressSubscription(const std::string& controlChannel,
                                           int32_t streamId) noexcept {
    if (!aeron_ || streamId == 0 || controlChannel.empty()) return;
    for (const SubSlot& s : subSlots_) {
        if (s.streamId == streamId && s.channel == controlChannel) return;  // idempotent
    }
    try {
        subSlots_.push_back(SubSlot{controlChannel, streamId,
                                    Resolution<aeron::Subscription>{kResolveTimeoutNs,
                                                                    kResolveRetryNs},
                                    nullptr});
    } catch (...) {
    }
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
        if (!aeron_) return false;  // not started — cannot retarget, old state untouched
        // Resolve the NEW default publication BEFORE releasing the old one, so a
        // failed resolve leaves the old pub usable (offer() keeps working).
        Resolution<aeron::ExclusivePublication> res{kResolveTimeoutNs, kResolveRetryNs};
        PubRegistry reg{*aeron_, newIngressChannel, cfg_.ingressStreamId};
        pumpUntil_([&] {
            return res.poll(reg, resolveNow_()) == ResolveStatus::Ready || res.rejected();
        }, kResolveTimeoutNs);
        if (res.ready() == nullptr) return false;  // old publication stays intact
        // Per-stream publications on the OLD default channel are released and
        // lazily re-resolved on the new channel (stream ids preserved).
        // Only Ready slots are released: resetting a Resolving/TimedOut slot would
        // orphan its pending driver registration (it would later come alive with
        // no owner); such a slot keeps its registration and resolves on next use.
        for (PubSlot& s : pubSlots_) {
            if (s.channel == cfg_.ingressChannel && s.res.ready() != nullptr) s.res.reset();
        }
        streamPub_.clear();
        cfg_.ingressChannel = newIngressChannel;
        ingressPub_ = *res.readyShared();
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
    // INVOKER: one runner (conductor+sender+receiver), no threads, refuses a
    // non-manual start (aeron_driver.c:939-957, :1054-1087).
    aeron_driver_context_set_threading_mode(
        driverCtx_, cfg_.driverMode == DriverMode::Invoker ? AERON_THREADING_MODE_INVOKER
                                                           : AERON_THREADING_MODE_SHARED);
    aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
    aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
    aeron_driver_context_set_shared_idle_strategy(driverCtx_, cfg_.driverIdleStrategy.c_str());
    aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
    aeron_driver_context_set_term_buffer_length(
        driverCtx_, static_cast<uint64_t>(cfg_.termBufferLength));
    aeron_driver_context_set_mtu_length(driverCtx_, cfg_.mtuLength);
    if (cfg_.udpGso) {
        gso_ = rune_gso_bindings_create(1, 1);   // NULL off Linux / unsupported kernel -> stock Aeron
        if (gso_ != nullptr && gsoInjectCount_ > 0) rune_gso_bindings_test_inject(gso_, gsoInjectErr_, gsoInjectCount_);
        if (gso_ != nullptr && rune_gso_bindings_install(driverCtx_, gso_) < 0) {
            rune_gso_bindings_delete(gso_);
            gso_ = nullptr;
        }
        gsoActiveMask_ = rune_gso_bindings_active(gso_);
    }
    // A7: epoll-first polling (after the GSO install, which replaces the context's bindings).
    // Review L2: only over the Aeron release its internals were verified against (else stock polling).
    if (cfg_.epollPoll &&
        !epollPollSupportsAeron(aeron_version_major(), aeron_version_minor(), aeron_version_patch())) {
        std::fprintf(stderr,
                     "AeronTransport: epoll-first polling disabled — it is verified against Aeron %d.%d.%d, the "
                     "runtime is %s (re-verify rune_driver_wake.c); stock polling, no park-until-work wake fds\n",
                     kEpollPollAeronPin[0], kEpollPollAeronPin[1], kEpollPollAeronPin[2], aeron_version_full());
    } else if (cfg_.epollPoll) {
        epollPollActive_ = rune_driver_epoll_poll_install(driverCtx_, gso_ != nullptr ? 1 : 0, &epollBindings_) == 1;
    }
    if (cfg_.maxMessagesPerSend > 0)
        aeron_driver_context_set_network_publication_max_messages_per_send(driverCtx_, cfg_.maxMessagesPerSend);
    else if (const unsigned m = rune_gso_msgs_per_send(udpGsoActive(), cfg_.mtuLength); m > 0)
        aeron_driver_context_set_network_publication_max_messages_per_send(driverCtx_, m);   // GSO groups <= 16
    maxMessagesPerSend_ = aeron_driver_context_get_network_publication_max_messages_per_send(driverCtx_);
    // Aeron requires timer_interval < client_liveness < publication_unblock
    // (aeron_driver_context.c:1322-1344).  validate() bounds liveness to
    // [kMinClientLivenessTimeoutNs, kMaxClientLivenessTimeoutNs]; derive the two
    // driver timers from it so every accepted value starts the driver:
    //   timer   = liveness/10  -> in [100 ms, 6 min], strictly < liveness
    //   unblock = 1.5x liveness -> <= 5.4e12 ns, no int64 overflow in the conductor
    static_assert(kMinClientLivenessTimeoutNs / 10 > 0);
    static_assert(kMaxClientLivenessTimeoutNs <= INT64_MAX / 2);
    const auto livenessNs = static_cast<uint64_t>(cfg_.clientLivenessTimeoutNs);
    aeron_driver_context_set_client_liveness_timeout_ns(driverCtx_, livenessNs);
    if (aeron_driver_context_get_timer_interval_ns(driverCtx_) >= livenessNs) {
        aeron_driver_context_set_timer_interval_ns(driverCtx_, livenessNs / 10);
    }
    if (aeron_driver_context_get_publication_unblock_timeout_ns(driverCtx_) <= livenessNs) {
        aeron_driver_context_set_publication_unblock_timeout_ns(driverCtx_,
                                                                livenessNs + livenessNs / 2);
    }
    if (cfg_.socketSndBufBytes > 0) {
        aeron_driver_context_set_socket_so_sndbuf(driverCtx_, cfg_.socketSndBufBytes);
    }
    if (cfg_.socketRcvBufBytes > 0) {
        aeron_driver_context_set_socket_so_rcvbuf(driverCtx_, cfg_.socketRcvBufBytes);
    }
    if (cfg_.rcvInitialWindowLength > 0) {  // 0 = Aeron's default (validate() checks either)
        aeron_driver_context_set_rcv_initial_window_length(driverCtx_, cfg_.rcvInitialWindowLength);
    }

    if (aeron_driver_init(&driver_, driverCtx_) < 0) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
        releaseGso_();
        throw std::runtime_error(
            std::string("AeronTransport: aeron_driver_init failed: ") +
            aeron_errmsg());
    }

    if (aeron_driver_start(driver_, /*manual_main_loop=*/true) < 0) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(driverCtx_);
        driver_    = nullptr;
        driverCtx_ = nullptr;
        releaseGso_();
        throw std::runtime_error(
            std::string("AeronTransport: aeron_driver_start failed: ") +
            aeron_errmsg());
    }

    // aeron_driver_init already created AND signalled the CnC file
    // (aeron_driver.c:864 create_cnc_file, :912 signal_cnc_ready), so prime the
    // conductor from THIS thread before any driver thread exists.  From here on
    // exactly one thread drives the driver (spec §3.6).
    const std::string cncPath = aeronDir + "/cnc.dat";
    if (!fs::exists(cncPath)) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(driverCtx_);
        driver_    = nullptr;
        driverCtx_ = nullptr;
        releaseGso_();
        throw std::runtime_error(
            "AeronTransport: Aeron CnC file missing after aeron_driver_init: " + cncPath);
    }
    pumpDriverFromCaller_();
    pumpDriverFromCaller_();
    pumpDriverFromCaller_();

    if (cfg_.driverMode == DriverMode::Thread) {
        // From here on the driver thread is the driver's ONLY pumper; the owner
        // stops it through the control ring and joins it (stopEmbeddedDriver).
        const std::size_t ctlBytes =
            kDriverCtlCapacity + static_cast<std::size_t>(detail::OneToOneRingBuffer::trailerLength());
        driverCtlStorage_ = std::make_unique<uint8_t[]>(ctlBytes);  // zeroed
        driverCtl_ = std::make_unique<detail::OneToOneRingBuffer>(driverCtlStorage_.get(), ctlBytes);
        driverThreadLaunched_ = true;
        detail::OneToOneRingBuffer* ctl = driverCtl_.get();
        aeron_driver_t*             drv = driver_;
        driverThread_ = std::thread([ctl, drv]() {
            while (!ctl->producerClosed()) {
                aeron_driver_main_idle_strategy(drv, aeron_driver_main_do_work(drv));
            }
        });
    }
}

int AeronTransport::pumpDriverFromCaller_() noexcept {
#ifndef NDEBUG
    if (std::this_thread::get_id() != pumpOwner_) [[unlikely]] {
        std::fprintf(stderr,
                     "rune-client: FATAL: AeronTransport driver pumped by a second thread "
                     "(the pump owner is the thread that started it or last called "
                     "bindPumpThread()); aborting\n");
        std::fflush(stderr);
        std::abort();
    }
#endif
    if (driverThreadLaunched_) {
        ++callerPumpViolations_;
    }
    return aeron_driver_main_do_work(driver_);
}

// ---------------------------------------------------------------------------
// stopEmbeddedDriver — mirrors BrokerNode::stopEmbeddedDriver (BR-3b order)
// ---------------------------------------------------------------------------

void AeronTransport::stopEmbeddedDriver() noexcept {
    // Step A: signal the driver thread to stop (control ring close) and join it.
    if (driverThreadLaunched_) {
        driverCtl_->producerClose();
        if (driverThread_.joinable()) {
            driverThread_.join();
        }
        driverThreadLaunched_ = false;
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
            const int work = pumpDriverFromCaller_();
            if (work == 0) {
                pumpDriverFromCaller_();
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

    // Step D: close the driver context, then the GSO bindings it pointed at.
    if (driverCtx_) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
    }
    releaseGso_();
}

void AeronTransport::releaseGso_() noexcept {
    rune_driver_epoll_poll_free(epollBindings_);   // after aeron_driver_context_close
    epollBindings_ = nullptr;
    if (gso_ == nullptr) return;
    rune_gso_bindings_totals(gso_, &gsoTotals_);
    rune_gso_bindings_delete(gso_);
    gso_ = nullptr;
}

} // namespace rune::client::transport
