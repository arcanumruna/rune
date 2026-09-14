// ClientGateway.cpp — implementation of the client-facing session gateway (BR-4 / BR-5 / BR-7a / GR-4).

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/replication/PartitionReplica.h>

#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>

#include <spdlog/spdlog.h>

#include <cstring>

#include <algorithm>
#include <chrono>
#include <thread>

namespace rune::gateway {

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Redirect-path synchronous resolution timeout.
// Used for:
//   (a) The non-leader redirect path (resolvePublication / sendOnPub).
//   (b) The ingress subscription resolve in the constructor.
// NOT used for the normal leader-accept path (that is async via pendingConnects_).
// ---------------------------------------------------------------------------
static constexpr int kResolveMaxMs   = 5000; // 5 s
static constexpr int kResolveSliceMs = 1;

// ---------------------------------------------------------------------------
// ClientGateway constructor
// ---------------------------------------------------------------------------
ClientGateway::ClientGateway(std::shared_ptr<aeron::Aeron>   aeron,
                             std::string                     clientChannel,
                             int32_t                         clientStreamId,
                             int32_t                         thisNodeId,
                             int64_t                         sessionTimeoutNs,
                             std::function<LeaderView()>     leaderView,
                             int64_t                         connectResolveTimeoutNs)
    : aeron_(std::move(aeron))
    , clientChannel_(std::move(clientChannel))
    , clientStreamId_(clientStreamId)
    , thisNodeId_(thisNodeId)
    , sessionTimeoutNs_(sessionTimeoutNs)
    , connectResolveTimeoutNs_(connectResolveTimeoutNs)
    , leaderView_(std::move(leaderView))
    , assembler_([this](aeron::concurrent::AtomicBuffer& buffer,
                        aeron::util::index_t             offset,
                        aeron::util::index_t             length,
                        aeron::Header&                   header) {
          onFragment(buffer, offset, length, header);
      })
{
    // Pre-allocate the draining-release vector (bounded by kMaxPendingConnects).
    // pendingConnects_ is a deque — no reserve() needed; O(1) front push/pop.
    drainingRegistrations_.reserve(kMaxPendingConnects);

    // Register the ingress subscription.
    int64_t regId = aeron_->addSubscription(clientChannel_, clientStreamId_);

    // Bounded resolve — same pattern as AeronIngress.
    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};
    while (clock::now() < deadline) {
        sub_ = aeron_->findSubscription(regId);
        if (sub_) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
    }
    if (!sub_) {
        throw std::runtime_error(
            "ClientGateway: ingress subscription not resolved within timeout");
    }
}

// ---------------------------------------------------------------------------
// doWork — poll ingress; poll pending-connects; reap expired sessions.
// Returns fragment count + pending-connect resolves.
// ---------------------------------------------------------------------------
int ClientGateway::doWork(int64_t nowNs) {
    nowNs_ = nowNs;
    constexpr int kFragmentLimit = 10;
    int work = sub_->poll(assembler_.handler(), kFragmentLimit);
    pollPendingConnects(nowNs);
    reapExpired(nowNs);
    // GB-5p2: group-commit flush — time-based cadence (kGroupCommitIntervalNs).
    // Fsyncs all dirty partition Logs (appendNoSync'd since last flush) at most
    // once per interval.  Produces between flushes land in page cache at ~1-10µs.
    // Must run AFTER the ingress poll; placed before pushSubscriptions so push
    // reads fsync-durable data.  No busy-spin; no-op when interval not elapsed.
    flushDirtyLogs();
    // GB-3: push new Records to all active subscribers (egress-paced, single-writer).
    pushSubscriptions();
    return work;
}

// ---------------------------------------------------------------------------
// reapExpired — remove sessions idle beyond sessionTimeoutNs_.
//
// Uses injected nowNs ONLY (no wall-clock).  Bounded O(n) scan.
// Two-phase: collect expired IDs into reapScratch_ (pre-allocated vector),
// then erase them — avoids iterator invalidation.
// ---------------------------------------------------------------------------
void ClientGateway::reapExpired(int64_t nowNs) {
    reapScratch_.clear();

    sessions_.forEach([&](Session& s) {
        if (nowNs - s.lastSeenNs > sessionTimeoutNs_) {
            reapScratch_.push_back(s.sessionId);
        }
    });

    for (int64_t sid : reapScratch_) {
        sessions_.remove(sid);
        // Removing from sessions_ resets the shared_ptr → Aeron releases the
        // ExclusivePublication once no other holder exists.

        // GB-3: drop all subscriptions belonging to this session so we no longer
        // attempt to push Records to a dead egress.
        auto it = subscriptions_.begin();
        while (it != subscriptions_.end()) {
            if (it->second.sessionId == sid) {
                it = subscriptions_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// onFragment — called by FragmentAssembler for each reassembled frame.
// Drop-safe: any exception or decode failure is caught and silently dropped.
// ---------------------------------------------------------------------------
void ClientGateway::onFragment(aeron::concurrent::AtomicBuffer& buffer,
                               aeron::util::index_t             offset,
                               aeron::util::index_t             length,
                               aeron::Header&                   /*header*/)
{
    const auto* buf = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
    const auto  len = static_cast<std::size_t>(length);

    // Step 1: validate frame with checkFrame — reject anything non-conformant.
    const auto fc = pc::checkFrame(buf, len);
    if (fc.status != pc::FrameStatus::Ok) {
        // Drop silently — bad/unknown frame never crashes the gateway.
        return;
    }

    // Step 2: dispatch on templateId.
    const auto tid = static_cast<pc::TemplateId>(fc.templateId);
    switch (tid) {
        case pc::TemplateId::Connect:
            handleConnect(buf, len, nowNs_);
            break;
        case pc::TemplateId::Keepalive:
            handleKeepalive(buf, len, nowNs_);
            break;
        case pc::TemplateId::Ping:
            handlePing(buf, len, nowNs_);
            break;
        case pc::TemplateId::Disconnect:
            handleDisconnect(buf, len);
            break;
        case pc::TemplateId::Produce:
            handleProduce(buf, len);
            break;
        case pc::TemplateId::Subscribe:
            handleSubscribe(buf, len);
            break;
        case pc::TemplateId::Unsubscribe:
            handleUnsubscribe(buf, len);
            break;
        default:
            // ConnectResponse, ErrorResponse, Pong, ProduceResponse, SubscribeResponse,
            // Records — server→client or other direction; drop.
            break;
    }
}

// ---------------------------------------------------------------------------
// handleConnect (BR-7a async path)
//
// BR-5 leader-awareness:
//   - Query leaderView_ (if set).
//   - If NOT leader AND leaderClientEndpoint non-empty → send ErrorResponse
//     {code=Redirect, message=leaderClientEndpoint}.  The redirect path still
//     uses the synchronous resolvePublication — it's a one-off transient pub
//     to an already-running client endpoint, not on the hot consensus thread.
//   - If leader (or hook unset/leader-unknown) → ASYNC: register
//     addExclusivePublication and enqueue a PendingConnect.  The publication
//     is polled across doWork() cycles via pollPendingConnects().
// ---------------------------------------------------------------------------
void ClientGateway::handleConnect(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto reqOpt = pc::ConnectRequest::decode(buf, len);
    if (!reqOpt) return; // truncated — drop

    const auto& req = *reqOpt;

    // -----------------------------------------------------------------------
    // BR-5: check leadership before creating a session.
    // -----------------------------------------------------------------------
    bool capturedIsLeader = true;
    std::string capturedLeaderEndpoint;
    if (leaderView_) {
        const LeaderView lv = leaderView_();
        capturedIsLeader        = lv.isLeader;
        capturedLeaderEndpoint  = lv.leaderClientEndpoint;

        // NOTE: the REDIRECT case (!isLeader && !leaderClientEndpoint.empty()) is
        // handled by the SAME async pending-connect machinery as the leader-accept
        // path below (see DR-BUG-2 fix).  We do NOT resolve the egress publication
        // synchronously here.
        //
        // DR-BUG-2 (redirect-reconnect storm) ROOT CAUSE + FIX:
        //   Previously this path called the BLOCKING resolvePublication(), which
        //   sleeps up to kResolveMaxMs (5 s) in 1 ms slices ON THE GATEWAY
        //   DUTY-CYCLE THREAD.  When N clients hit a follower at once (the DR-3
        //   reconnect storm), each ConnectRequest was dispatched serially inside a
        //   single sub_->poll() and each blocked in resolvePublication before the
        //   next client's addExclusivePublication was even issued.  One slow egress
        //   warm-up (media-driver contention while N new publications warm up on
        //   BOTH follower and leader simultaneously) head-of-line-blocked the whole
        //   follower gateway — no ingress poll, no reaping, and the LAST client's
        //   redirect was delayed enough that its subsequent leader connect blew the
        //   app-side connect deadline (the intermittent 1-of-5 connect() timeout).
        //   The synchronous resolve ALSO serialised the follower's own doWork loop.
        //
        //   Fix: enqueue the redirect as an async PendingConnect (non-blocking
        //   addExclusivePublication) — pollPendingConnects() resolves ALL pending
        //   entries fairly per cycle (no head-of-line block), stashes the resolved
        //   pub, and retries the ErrorResponse{Redirect} until the egress link
        //   warms up.  Bounded by kMaxPendingConnects + connectResolveTimeoutNs_,
        //   single-writer on the gateway agent thread, no busy-spin.
        //
        // else (leader-unknown, empty endpoint) → degrade to local accept below,
        // ALSO via the same async machinery (isLeader captured true / redirect
        // endpoint empty ⇒ pollPendingConnects sends ConnectResponse{OK}).
    }

    // -----------------------------------------------------------------------
    // Async accept path (BR-7a): never block the duty-cycle thread.
    // -----------------------------------------------------------------------

    // Cap: evict the oldest pending entry if we are at capacity.
    // The evicted registration must be released: attempt a final
    // findExclusivePublication — if it resolves (shared_ptr non-null) let it
    // drop immediately (destructor calls
    // ClientConductor::releaseExclusivePublication, see ExclusivePublication.cpp
    // ~ExclusivePublication()); if it throws (ERRORED), conductor already cleaned
    // up; if still AWAITING (nullptr returned), enqueue in drainingRegistrations_
    // and keep polling until it resolves.
    if (pendingConnects_.size() >= kMaxPendingConnects) {
        spdlog::warn("ClientGateway: pending-connect cap reached; dropping oldest entry");
        const int64_t evictedRegId      = pendingConnects_.front().registrationId;
        // DR-BUG-2 FIX: if the oldest entry has a stashed resolvedPub, the
        // shared_ptr destructor (run by pop_front()) is sufficient to release the
        // registration.  Do NOT call findExclusivePublication again — that would
        // touch a conductor entry that is already being released, and if it
        // returns nullptr we would push a stale regId to drainingRegistrations_
        // that never drains.  Only push to drain when resolvedPub==nullptr (pub
        // not yet resolved — the conductor still holds it in AWAITING state).
        const bool evictedAlreadyResolved = (pendingConnects_.front().resolvedPub != nullptr);
        pendingConnects_.pop_front(); // O(1); drops resolvedPub shared_ptr if non-null
        if (!evictedAlreadyResolved) {
            try {
                // Attempt immediate release by resolving and dropping the shared_ptr.
                auto pub = aeron_->findExclusivePublication(evictedRegId);
                if (!pub) {
                    // Still AWAITING_MEDIA_DRIVER — keep draining until it resolves.
                    drainingRegistrations_.push_back(evictedRegId);
                }
                // If pub non-null: drops here → ~ExclusivePublication() releases registration.
            } catch (...) {
                // ERRORED_MEDIA_DRIVER: conductor erased the entry and threw — no leak.
            }
        }
        // If evictedAlreadyResolved: the drop of resolvedPub in pop_front() already
        // releases the registration — no findExclusivePublication needed, no drain entry.
    }

    // Pre-allocate sessionId now so it is stable across cycles.
    const int64_t sessionId = sessions_.nextSessionId();

    // Kick off the egress publication registration (returns immediately).
    int64_t regId = -1;
    try {
        regId = aeron_->addExclusivePublication(
            std::string(req.egressChannel), req.egressStreamId);
    } catch (...) {
        // Bad channel — drop (never crash).
        spdlog::warn("ClientGateway: addExclusivePublication threw for egress channel '{}'; dropping Connect",
                     std::string(req.egressChannel));
        return;
    }

    PendingConnect pc_entry;
    pc_entry.registrationId        = regId;
    pc_entry.sessionId             = sessionId;
    pc_entry.correlationId         = req.correlationId;
    pc_entry.requestedAtNs         = nowNs;
    pc_entry.isLeader              = capturedIsLeader;
    pc_entry.leaderClientEndpoint  = std::move(capturedLeaderEndpoint);
    pendingConnects_.push_back(std::move(pc_entry));
}

// ---------------------------------------------------------------------------
// pollPendingConnects — called from doWork() each cycle.
//
// For each pending-connect entry:
//   - Try findExclusivePublication(registrationId).
//   - If resolved → promote to SessionTable + send ConnectResponse (OK).
//   - If timeout exceeded → drop + log.
//   - Otherwise → leave in pending for next cycle.
// Never blocks.
// ---------------------------------------------------------------------------
void ClientGateway::pollPendingConnects(int64_t nowNs)
{
    // Drive the draining-release table first (bounded, non-blocking).
    drainPendingReleases();

    if (pendingConnects_.empty()) return;

    namespace pc = rune::protocol::client;

    // Hand-rolled forward-erase loop (not std::remove_if — we need per-element
    // control to handle three distinct outcomes: resolve, timeout, and error).
    auto it = pendingConnects_.begin();
    while (it != pendingConnects_.end()) {
        PendingConnect& pending = *it;

        // Check timeout first.
        if (nowNs - pending.requestedAtNs > connectResolveTimeoutNs_) {
            spdlog::warn("ClientGateway: pending-connect session={} timed out after {}ms; dropped",
                         pending.sessionId,
                         (nowNs - pending.requestedAtNs) / 1'000'000LL);

            // DR-BUG-2 FIX: if the publication was already resolved and stashed
            // in pending.resolvedPub, the shared_ptr release happens naturally
            // when the PendingConnect entry is erased (destructor drops the
            // shared_ptr → ~ExclusivePublication() releases the registration).
            // We must NOT call findExclusivePublication again in that case — the
            // conductor entry is already being released, and a redundant find
            // would return nullptr and push a stale regId into drainingRegistrations_
            // that can never drain (use-after-release).
            //
            // Only call findExclusivePublication / push to drainingRegistrations_
            // when the publication was NOT yet resolved (resolvedPub == nullptr):
            // in that case the conductor still holds the registration in AWAITING
            // state and we must poll it until it resolves so we can drop it.
            const int64_t timedOutRegId = pending.registrationId;
            const bool alreadyResolved  = (pending.resolvedPub != nullptr);
            it = pendingConnects_.erase(it); // drops resolvedPub shared_ptr if non-null
            if (!alreadyResolved) {
                // Publication not yet resolved: enqueue in drain table.
                // If resolved (non-null shared_ptr): drop immediately.
                // If throws (ERRORED): conductor already cleaned up.
                // If nullptr (still AWAITING): enqueue to keep polling until resolved.
                try {
                    auto pub = aeron_->findExclusivePublication(timedOutRegId);
                    if (!pub) {
                        drainingRegistrations_.push_back(timedOutRegId);
                    }
                    // pub non-null: drops here → destructor releases registration.
                } catch (...) {
                    // ERRORED_MEDIA_DRIVER: conductor erased the entry — no leak.
                }
            }
            continue;
        }

        // Try to find the publication (non-blocking).
        std::shared_ptr<aeron::ExclusivePublication> pub;
        try {
            pub = aeron_->findExclusivePublication(pending.registrationId);
        } catch (...) {
            // ERRORED_MEDIA_DRIVER: conductor signalled an error (bad channel, etc.).
            // The conductor erased its own entry when throwing — no registration leak.
            spdlog::warn("ClientGateway: findExclusivePublication threw for session={}; dropped",
                         pending.sessionId);
            it = pendingConnects_.erase(it);
            continue;
        }

        if (!pub) {
            // Not yet resolved — but we may already have a stashed resolved pub
            // from a previous cycle (DR-BUG-2 fix: stash-and-retry path).
            if (!pending.resolvedPub) {
                // Still waiting for the conductor to register the publication.
                ++it;
                continue;
            }
            // Use the already-resolved pub from a previous cycle.
            pub = pending.resolvedPub;
        }

        // Resolved (either this cycle or previously stashed).
        //
        // DR-BUG-2 FIX: stash the resolved pub in the PendingConnect entry so
        // that if the ConnectResponse send fails (publication not yet Aeron-
        // connected — the UDP image between the broker and the client egress
        // endpoint may not be established yet when 5 clients start
        // simultaneously), we keep retrying on subsequent doWork() cycles
        // instead of dropping the entry.  The resolve-timeout still applies:
        // if connectResolveTimeoutNs_ elapses before the send succeeds, the
        // entry is dropped and the client times out and re-Connects.
        //
        // This fixes the ~33% failure rate for one-of-five concurrent clients
        // under load: previously a not-yet-connected pub caused an immediate
        // drop and the client waited the full 15 s connectTimeoutMs.
        if (!pending.resolvedPub) {
            pending.resolvedPub = pub;
        }

        // DR-BUG-2 (redirect-reconnect storm): a follower entry (captured
        // !isLeader with a known leader endpoint) sends ErrorResponse{Redirect}
        // on its resolved egress and creates NO session — the client will
        // retarget to the leader and reconnect there.  This shares the fair,
        // non-blocking poll loop with the leader-accept path (no synchronous
        // resolvePublication head-of-line block on the duty-cycle thread).
        const bool isRedirect = (!pending.isLeader && !pending.leaderClientEndpoint.empty());

        std::size_t encoded = 0;
        if (isRedirect) {
            pc::ErrorResponse err;
            err.sessionId     = 0;
            err.correlationId = pending.correlationId;
            err.code          = static_cast<int16_t>(pc::ErrorCode::Redirect);
            err.message       = pending.leaderClientEndpoint;

            // Guard: message must fit in scratch (encode would overflow otherwise).
            const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                       pc::ErrorResponse::BODY_FIXED + 2 +
                                       pending.leaderClientEndpoint.size();
            if (needed > scratch_.size()) {
                // Pathological endpoint length — drop (cannot encode).  The
                // resolvedPub shared_ptr release on erase() frees the egress reg.
                spdlog::warn("ClientGateway: redirect endpoint too large ({}B); dropping",
                             pending.leaderClientEndpoint.size());
                it = pendingConnects_.erase(it);
                continue;
            }
            encoded = err.encode(scratch_.data());
        } else {
            // Invariant (FIX A): a session must NEVER be added to the table unless
            // the client received a ConnectResponse for it.  Only promote to a
            // session after a successful send.
            pc::ConnectResponse resp;
            resp.sessionId         = pending.sessionId;
            resp.correlationId     = pending.correlationId;
            resp.code              = static_cast<int16_t>(pc::ErrorCode::OK);
            resp.leaderMemberId    = thisNodeId_;
            resp.assignedSessionId = pending.sessionId;
            encoded = resp.encode(scratch_.data());
        }

        if (!sendOnPub(*pub, scratch_.data(), encoded)) {
            // Publication not yet Aeron-connected (UDP image not yet up) or
            // transient back-pressure.  Keep the PendingConnect alive
            // (resolvedPub is now stashed); retry next doWork() cycle.
            // The resolve-timeout still bounds this: if it expires before the
            // link warms up, we drop + the client re-Connects.
            spdlog::debug("ClientGateway: {} not-connected/back-pressured for "
                          "session={}; retrying next cycle (pub stashed)",
                          isRedirect ? "Redirect" : "ConnectResponse",
                          pending.sessionId);
            ++it;
            continue;
        }

        // Send succeeded.  Redirect entries create NO session (the resolvedPub
        // shared_ptr release on erase frees the transient egress registration).
        if (!isRedirect) {
            // Only now is it safe to add the session (leader-accept path).
            sessions_.add(pending.sessionId, std::move(pub), nowNs);
        }

        it = pendingConnects_.erase(it);
    }
}

// ---------------------------------------------------------------------------
// drainPendingReleases — poll registrations that were logically dropped before
// their ExclusivePublication resolved, to complete the release.
//
// Release mechanism (Aeron C++ 1.41.4, ExclusivePublication.cpp line 62-65):
//   ~ExclusivePublication() { m_conductor.releaseExclusivePublication(m_registrationId); }
// So: obtaining a shared_ptr via findExclusivePublication and letting it drop
// is the ONLY correct release path for a registration that is still in
// AWAITING_MEDIA_DRIVER state.  Once the conductor transitions it to
// REGISTERED_MEDIA_DRIVER, findExclusivePublication returns a non-null
// shared_ptr; dropping it triggers the destructor which releases the
// registration (and driver-side log buffers).
// ---------------------------------------------------------------------------
void ClientGateway::drainPendingReleases()
{
    if (drainingRegistrations_.empty()) return;

    // Drain-table growth warning (rate-limited: emit once per threshold-crossing).
    // The table is expected-bounded — an Aeron ExclusivePublication resolves within
    // a few driver cycles — so sustained growth indicates a stuck/never-resolving
    // driver entry that warrants operator attention.  We do NOT hard-cap (evicting
    // an unresolved entry re-introduces the registration leak this table prevents).
    {
        static constexpr std::size_t kDrainWarnThreshold = 4 * kMaxPendingConnects;
        static bool sWarned = false;
        const std::size_t drainSz = drainingRegistrations_.size();
        if (drainSz > kDrainWarnThreshold && !sWarned) {
            spdlog::warn("ClientGateway: drainingRegistrations_ size {} exceeds {}×kMaxPendingConnects={}; "
                         "possible stuck driver entry — investigate Aeron media driver state",
                         drainSz, 4u, kMaxPendingConnects);
            sWarned = true;
        } else if (drainSz <= kDrainWarnThreshold) {
            sWarned = false; // reset so the warning fires again if it grows again
        }
    }

    auto it = drainingRegistrations_.begin();
    while (it != drainingRegistrations_.end()) {
        const int64_t regId = *it;
        try {
            auto pub = aeron_->findExclusivePublication(regId);
            if (pub) {
                // Resolved: drop the shared_ptr immediately.
                // ~ExclusivePublication() calls
                // m_conductor.releaseExclusivePublication(registrationId).
                pub.reset();
                it = drainingRegistrations_.erase(it);
            } else {
                // Still AWAITING — try again next doWork cycle.
                ++it;
            }
        } catch (...) {
            // ERRORED_MEDIA_DRIVER: conductor erased its internal entry and threw.
            // The registration is already cleaned up — remove from drain list.
            it = drainingRegistrations_.erase(it);
        }
    }
}

// ---------------------------------------------------------------------------
// handleKeepalive
// ---------------------------------------------------------------------------
void ClientGateway::handleKeepalive(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto msgOpt = pc::Keepalive::decode(buf, len);
    if (!msgOpt) return;
    sessions_.touch(msgOpt->sessionId, nowNs);
}

// ---------------------------------------------------------------------------
// handlePing
// ---------------------------------------------------------------------------
void ClientGateway::handlePing(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto pingOpt = pc::Ping::decode(buf, len);
    if (!pingOpt) return;

    Session* session = sessions_.find(pingOpt->sessionId);
    if (!session) {
        // Unknown session — drop (no egress to reply on).
        return;
    }

    // Refresh session liveness: every inbound message (including Ping) keeps
    // the session alive.  Touch before sending the Pong so a Ping-only client
    // (no explicit Keepalive) is never wrongly reaped mid-activity.
    sessions_.touch(pingOpt->sessionId, nowNs);

    // Encode Pong with verbatim echo.
    pc::Pong pong;
    pong.sessionId     = pingOpt->sessionId;
    pong.correlationId = pingOpt->correlationId;
    pong.echo          = pingOpt->echo;

    // Guard: echo payload must fit in the scratch buffer.
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + 2 + pingOpt->echo.size();
    if (needed > scratch_.size()) return; // oversized echo — drop

    const std::size_t encoded = pong.encode(scratch_.data());
    sendTo(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// handleDisconnect
// ---------------------------------------------------------------------------
void ClientGateway::handleDisconnect(const uint8_t* buf, std::size_t len)
{
    auto msgOpt = pc::Disconnect::decode(buf, len);
    if (!msgOpt) return;
    const int64_t sid = msgOpt->sessionId;
    sessions_.remove(sid);

    // GB-3: drop all subscriptions belonging to this session.
    auto it = subscriptions_.begin();
    while (it != subscriptions_.end()) {
        if (it->second.sessionId == sid) {
            it = subscriptions_.erase(it);
        } else {
            ++it;
        }
    }
}

// ---------------------------------------------------------------------------
// sendTo — tryClaim first, fallback to offer (mirrors AeronMessageSink).
// ---------------------------------------------------------------------------
bool ClientGateway::sendTo(Session& session, const uint8_t* buf, std::size_t len)
{
    return sendOnPub(*session.egress, buf, len);
}

// ---------------------------------------------------------------------------
// sendOnPub — send on a raw ExclusivePublication (used by redirect path).
// ---------------------------------------------------------------------------
bool ClientGateway::sendOnPub(aeron::ExclusivePublication& pub,
                              const uint8_t*               buf,
                              std::size_t                  len)
{
    if (!pub.isConnected()) return false;

    // AtomicBuffer takes a non-const pointer, but Aeron only reads (copies out)
    // the data — it never writes back through the pointer.  The cast is safe.
    auto* mutableBuf = const_cast<uint8_t*>(buf);
    aeron::concurrent::AtomicBuffer atomicBuf(
        mutableBuf, static_cast<aeron::util::index_t>(len));

    if (len <= static_cast<std::size_t>(pub.maxPayloadLength())) {
        aeron::concurrent::logbuffer::BufferClaim claim;
        const int64_t pos = pub.tryClaim(
            static_cast<aeron::util::index_t>(len), claim);
        if (pos > 0) {
            claim.buffer().putBytes(claim.offset(), atomicBuf, 0,
                                    static_cast<aeron::util::index_t>(len));
            claim.commit();
            return true;
        }
        // BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED — fall through to offer.
    }

    const int64_t result = pub.offer(atomicBuf, 0,
                                     static_cast<aeron::util::index_t>(len));
    return result > 0;
}

// ---------------------------------------------------------------------------
// handleProduce (GB-2) — leader-local append with broker-assigned offsets.
//
// Flow:
//   1. Decode Produce (tid16) — drop on truncation.
//   2. Require known session — drop (UnknownSession) if not found.
//   GB-4: Check leadership BEFORE touching the engine.
//     - NOT leader + known leaderClientEndpoint → ErrorResponse{Redirect, endpoint},
//       engine untouched.
//     - NOT leader + unknown endpoint → ErrorResponse{NotLeader}, engine untouched.
//     - Leader → serve (existing path).
//   3. Resolve partition: explicit when partitionKnown==1, else partitionForKey().
//   4. Lookup Log* from registry; reply error on unknown topic / bad partition.
//   5. Assign offsets (CRC-safe restamp):
//        base = log->logEndOffset()
//        copy recordBatch bytes into restampScratch_
//        write base as LE64 at kBaseOffsetOffset (excluded from CRC region)
//        read recordCount as LE32 at kRecordCountOffset
//        guard recordCount >= 1 (0-record batch → drop / InvalidBatch response)
//        lastOffset = base + recordCount - 1
//        log->append(restampScratch_[0..batchLen], lastOffset)
//   6. Reply ProduceResponse{OK, partition, baseOffset=base}.
//
// Acks semantics (M-B leader-local):
//   Leader and None → ack after local append (only leader log exists here).
//   All (ISR-majority HW) → same as Leader for now — replication-ack wiring
//   is deferred.
//   // TODO(Milestone B GB-4/replication): true acks=All waits for ISR HW.
//   (Produce in M-B does not carry an `acks` field; always ack after append.)
//
// Drop-safe: never throws; all engine calls wrapped in try/catch.
// Single-writer: called only from onFragment → doWork duty-cycle thread.
// Zero heap allocation on hot path: restampScratch_ is a reusable member array.
// ---------------------------------------------------------------------------
void ClientGateway::handleProduce(const uint8_t* buf, std::size_t len)
{
    // 1. Decode Produce.
    auto msgOpt = pc::Produce::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: Produce decode failed (truncated?) — dropped");
        return;
    }
    const pc::Produce& msg = *msgOpt;

    // 2. Require known session.
    Session* session = sessions_.find(msg.sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: Produce for unknown sessionId={} — dropped", msg.sessionId);
        return;
    }

    // GB-4: Leadership check — data ops MUST reach the leader (correctness).
    // // TODO(future): optional internal proxy for single-endpoint transparency.
    if (leaderView_) {
        const LeaderView lv = leaderView_();
        if (!lv.isLeader) {
            pc::ErrorResponse errResp;
            errResp.sessionId     = msg.sessionId;
            errResp.correlationId = msg.correlationId;
            if (!lv.leaderClientEndpoint.empty()) {
                // Known leader endpoint → redirect.
                errResp.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
                errResp.message = lv.leaderClientEndpoint;
                spdlog::debug("ClientGateway: Produce redirect sessionId={} → {}",
                              msg.sessionId, lv.leaderClientEndpoint);
            } else {
                // Leader unknown → NotLeader (client backs off + retries).
                errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                spdlog::debug("ClientGateway: Produce NotLeader (endpoint unknown) sessionId={}",
                              msg.sessionId);
            }
            const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                       pc::ErrorResponse::BODY_FIXED + 2 +
                                       errResp.message.size();
            if (needed <= scratch_.size()) {
                const std::size_t encoded = errResp.encode(scratch_.data());
                sendTo(*session, scratch_.data(), encoded);
            }
            return;  // engine untouched
        }
    }

    // Helper: send ProduceResponse on the session egress.
    auto sendResp = [&](pc::ErrorCode ec, int32_t partition, int64_t baseOffset) {
        pc::ProduceResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(ec);
        resp.partition     = partition;
        resp.baseOffset    = baseOffset;
        static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                      "scratch too small for ProduceResponse");
        const std::size_t encoded = resp.encode(scratch_.data());
        sendTo(*session, scratch_.data(), encoded);
    };

    // 3. Resolve partition.
    int32_t partition = msg.partition;
    if (msg.partitionKnown == 0) {
        // Key-based: resolve via TopicRegistry (FNV-1a 64 % partitionCount).
        if (!topicRegistry_) {
            spdlog::warn("ClientGateway: Produce key-partition but no TopicRegistry — UnknownTopic");
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
        partition = topicRegistry_->partitionForKey(msg.topicId, msg.key);
        if (partition < 0) {
            spdlog::debug("ClientGateway: Produce partitionForKey → unknown topicId={}", msg.topicId);
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
    }

    // 4. Lookup Log.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway: Produce but no TopicRegistry — UnknownTopic");
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    // topic() returns nullptr for unknown topicId.
    const rune::node::TopicSpec* spec = topicRegistry_->topic(msg.topicId);
    if (!spec) {
        spdlog::debug("ClientGateway: Produce unknown topicId={}", msg.topicId);
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    rune::storage::Log* log = topicRegistry_->log(msg.topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }

    // GR-4: Per-partition NotLeader defence-in-depth.
    //
    // If the ReplicaRegistry is wired in, look up the PartitionReplica for this
    // (topicId, partition).  If the replica exists and is NOT in Leader role,
    // reject the produce BEFORE touching the Log.  This is belt-and-suspenders
    // beyond the cluster-level leaderView_ check above: the cluster check uses a
    // coarse atomic observer that may lag a partition-level role transition
    // (particularly during epoch changes or single-node bootstrap where
    // replicaRegistry_ is set but the bridge has not yet called becomeLeader).
    //
    // We accept the produce when:
    //   a) replicaRegistry_ is null (replication not wired in — passthrough, e.g. tests
    //      that exercise the gateway without replication).
    //   b) no replica found for this (topicId, partition) — replica map is sparse
    //      (same passthrough).
    //   c) replica.role() == Leader.
    //
    // We reject (NotLeader) when replica.role() == Follower.
    //
    // O(1) lookup (unordered_map): zero per-op alloc, lock-free (single-writer).
    rune::replication::PartitionReplica* replica = nullptr;
    if (replicaRegistry_) {
        replica = replicaRegistry_->lookup(msg.topicId, partition);
        if (replica && replica->role() != rune::replication::Role::Leader) {
            // Per-partition NotLeader: reject without appending.
            // Use the same errResp path as the cluster-level check above.
            pc::ErrorResponse errResp;
            errResp.sessionId     = msg.sessionId;
            errResp.correlationId = msg.correlationId;
            errResp.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            spdlog::debug("ClientGateway: Produce per-partition NotLeader "
                          "(topicId={} partition={}) sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                       pc::ErrorResponse::BODY_FIXED + 2;
            if (needed <= scratch_.size()) {
                const std::size_t encoded = errResp.encode(scratch_.data());
                sendTo(*session, scratch_.data(), encoded);
            }
            return;  // engine untouched — no append
        }
        // replica is either nullptr (no replica for this partition — passthrough)
        // or role == Leader (allow produce through).
    }

    // 5. Assign offsets (CRC-safe restamp).

    // Guard: recordBatch must be present (non-empty).
    const std::size_t batchLen = msg.recordBatch.size();
    if (batchLen == 0) {
        spdlog::debug("ClientGateway: Produce empty recordBatch — dropped");
        // Reply InvalidBatch (no valid errorCode for InvalidBatch in client::ErrorCode;
        // use Internal as the closest sentinel for a malformed produce).
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // Guard: batch must fit in the restamp scratch.
    if (batchLen > restampScratch_.size()) {
        spdlog::warn("ClientGateway: Produce batch too large ({} > {}); dropped",
                     batchLen, restampScratch_.size());
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // Guard: batch must be at least large enough to hold the fixed header fields
    // we will read (kRecordCountOffset + 4 bytes).
    // M1 fix: also need kLastOffsetDeltaOffset + 4 bytes (kLastOffsetDeltaOffset=23 < 57).
    constexpr std::size_t kMinBatchSize =
        static_cast<std::size_t>(rune::protocol::batch::kRecordCountOffset) + 4u;
    if (batchLen < kMinBatchSize) {
        spdlog::debug("ClientGateway: Produce batch too short ({} < {}); dropped",
                      batchLen, kMinBatchSize);
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // Copy the recordBatch bytes into the reusable restamp scratch.
    std::memcpy(restampScratch_.data(), msg.recordBatch.data(), batchLen);

    // Assign base offset and rewrite the LE64 at kBaseOffsetOffset.
    // This field sits at bytes [0..7], BEFORE the CRC region that starts at
    // kAttributesOffset (byte 21) — rewriting it is CRC-safe (identical to Kafka).
    const int64_t base = log->logEndOffset();
    {
        uint64_t u = static_cast<uint64_t>(base);
        for (int i = 0; i < 8; ++i) {
            restampScratch_[static_cast<std::size_t>(rune::protocol::batch::kBaseOffsetOffset) + i] =
                static_cast<uint8_t>(u >> (i * 8));
        }
    }

    // M1 fix: derive lastOffset from the CRC-covered lastOffsetDelta field
    // (kLastOffsetDeltaOffset=23, LE32) rather than trusting client recordCount.
    //
    // A buggy/malicious client can send recordCount=1 with lastOffsetDelta=1000;
    // LEO would advance by 1 while the stored batch self-describes as spanning
    // 1001 offsets — the next produce's baseOffset overlaps the previous batch
    // (exactly-once violation).  lastOffsetDelta is CRC-covered: the CRC region
    // starts at kAttributesOffset=21 which precedes kLastOffsetDeltaOffset=23.
    //
    // Canonical derivation:
    //   lastOffset = base + lastOffsetDelta
    //   count      = lastOffsetDelta + 1
    // Validate: recordCount (client-supplied) must match lastOffsetDelta+1.
    // A mismatch means the batch is internally inconsistent → reject (Internal).
    // Also validate via CRC (ensureValid) to reject any corrupt batch.

    // Read lastOffsetDelta (LE32 at kLastOffsetDeltaOffset=23).
    int32_t lastOffsetDelta = 0;
    {
        const uint8_t* p = restampScratch_.data() + rune::protocol::batch::kLastOffsetDeltaOffset;
        lastOffsetDelta = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) |
            (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) |
            (static_cast<uint32_t>(p[3]) << 24));
    }

    // Read recordCount (LE32 at kRecordCountOffset=57) for consistency validation only.
    int32_t recordCount = 0;
    {
        const uint8_t* p = restampScratch_.data() + rune::protocol::batch::kRecordCountOffset;
        recordCount = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) |
            (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) |
            (static_cast<uint32_t>(p[3]) << 24));
    }

    // lastOffsetDelta must be >= 0 (single-record batch: delta=0 → count=1).
    if (lastOffsetDelta < 0) {
        spdlog::debug("ClientGateway: Produce lastOffsetDelta={} < 0; rejected (Internal)",
                      lastOffsetDelta);
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // recordCount must equal lastOffsetDelta + 1.
    if (recordCount != lastOffsetDelta + 1) {
        spdlog::warn("ClientGateway: Produce recordCount={} != lastOffsetDelta+1={}; "
                     "batch internally inconsistent → rejected (Internal)",
                     recordCount, lastOffsetDelta + 1);
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // CRC validation: reject corrupt batches before they touch the log.
    {
        rune::protocol::RecordBatchReader reader(restampScratch_.data(), batchLen);
        if (!reader.ensureValid()) {
            spdlog::warn("ClientGateway: Produce CRC mismatch (corrupt batch) → rejected (Internal)");
            sendResp(pc::ErrorCode::Internal, partition, 0);
            return;
        }
    }

    // Derive lastOffset from the CRC-covered lastOffsetDelta (single source of truth).
    // This is the offset of the last record in the batch; LEO advances past it.
    const int64_t lastOffset = base + static_cast<int64_t>(lastOffsetDelta);

    // GB-5p2: use appendNoSync — page-cache write only, no fsync on the hot path.
    // flushDirtyLogs() is called once per doWork() cycle (after all ingress has
    // been processed) to fsync all touched partition Logs in one pass → group-commit.
    //
    // Durability contract — acks=Leader (acks=1):
    //   Records are written to the OS page cache and visible to Log::read()
    //   immediately.  They are NOT fsync-durable until flushDirtyLogs() runs
    //   at the end of this doWork() cycle.  A leader crash in the window between
    //   appendNoSync() and sync() may lose the most-recent un-flushed acked
    //   records.  True crash-durability requires acks=All + ISR replication
    //   (Milestone C).  This is the standard acks=1 / Kafka-default trade-off.
    //
    // TODO(Milestone C): when acks=All is supported, the ProduceResponse must be
    //   deferred until the ISR high-watermark advances past lastOffset on all
    //   in-sync replicas.  Today we ack immediately after page-cache write.
    try {
        log->appendNoSync(restampScratch_.data(), batchLen, lastOffset);
    } catch (const std::exception& ex) {
        spdlog::error("ClientGateway: Log::appendNoSync threw: {}; ProduceResponse Internal", ex.what());
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    } catch (...) {
        spdlog::error("ClientGateway: Log::appendNoSync threw unknown exception; ProduceResponse Internal");
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // Mark this Log dirty so flushDirtyLogs() will sync it at end of cycle.
    dirtyLogs_.insert(log);

    // GR-4: Reconcile the leader replica's tracked LEO/HW (Mode A seam).
    //
    // The gateway appended DIRECTLY to the Log (bypassing PartitionReplica::append)
    // so the leader replica's ReplicaLeaderState still holds the pre-append LEO.
    // onLeaderLogAppended() reads log_->logEndOffset(), refreshes per-follower LEO,
    // and recomputes HW — this is EXACTLY the Mode-A seam described in the spec.
    //
    // No double-append: the Log was written once by appendNoSync() above.
    // onLeaderLogAppended() only reads the Log's LEO (log_->logEndOffset()) and
    // updates in-memory replica state (ReplicaLeaderState + IsrManager).
    //
    // No per-op alloc: replica lookup is O(1) (pointer was captured before the
    // restamp section); onLeaderLogAppended() is noexcept with no allocation.
    //
    // acks=Leader / None: we ack immediately after the append + reconciliation
    // (same as before GR-4).  acks=All deferral is deferred to GR-5 — for now
    // acks=All behaves identically (immediate ack, no wait for ISR HW advance).
    if (replica) {
        replica->onLeaderLogAppended();
    }

    // 6. Reply ProduceResponse{OK, partition, baseOffset=base}.
    // acks=Leader: ack after page-cache write; fsync runs in flushDirtyLogs().
    sendResp(pc::ErrorCode::OK, partition, base);
}

// ---------------------------------------------------------------------------
// resolvePublication — bounded wait for ExclusivePublication, mirrors
// AeronMessageSink::resolvePublications().
//
// Exception-safe: if Aeron rejects the channel (malformed URI, unknown
// transport, etc.) the conductor stores an error and findExclusivePublication
// throws on the next poll.  We catch that and return nullptr so the caller can
// treat it identically to a timeout (log-and-drop, no session created).
// ---------------------------------------------------------------------------
std::shared_ptr<aeron::ExclusivePublication>
ClientGateway::resolvePublication(const std::string& channel, int32_t streamId)
{
    int64_t regId;
    try {
        regId = aeron_->addExclusivePublication(channel, streamId);
    } catch (...) {
        return nullptr; // channel rejected by the media driver
    }

    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};

    while (clock::now() < deadline) {
        try {
            auto pub = aeron_->findExclusivePublication(regId);
            if (pub) return pub;
        } catch (...) {
            return nullptr; // conductor signalled an error (bad channel, etc.)
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
    }
    return nullptr; // timed out
}

// ---------------------------------------------------------------------------
// handleSubscribe (GB-3, tid18) — register a per-session subscription.
//
// Flow:
//   1. Decode Subscribe — drop on truncation.
//   2. Require known session — drop on unknown session (no egress to reply on).
//   GB-4: Check leadership BEFORE touching the engine.
//     - NOT leader + known leaderClientEndpoint → ErrorResponse{Redirect, endpoint},
//       engine untouched, no subscription registered.
//     - NOT leader + unknown endpoint → ErrorResponse{NotLeader}, engine untouched.
//     - Leader → serve (existing path).
//   3. Resolve partition: explicit (partitionKnown=1) or partitionForKey().
//   4. Validate topic + partition → SubscribeResponse{error} on failure.
//   5. Clamp fromOffset:
//        fromOffset < 0  → treat as "from Latest" (LEO).
//        fromOffset > LEO → clamp to LEO.
//        0 ≤ fromOffset ≤ LEO → use as-is.
//   6. Allocate subscriptionId; register Subscription; reply SubscribeResponse{OK}.
//
// No heap allocation on the hot path (scratch_ is pre-allocated).
// ---------------------------------------------------------------------------
void ClientGateway::handleSubscribe(const uint8_t* buf, std::size_t len)
{
    // 1. Decode.
    auto msgOpt = pc::Subscribe::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: Subscribe decode failed (truncated?) — dropped");
        return;
    }
    const pc::Subscribe& msg = *msgOpt;

    // 2. Require known session.
    Session* session = sessions_.find(msg.sessionId);
    if (!session) {
        spdlog::debug("ClientGateway: Subscribe for unknown sessionId={} — dropped", msg.sessionId);
        return;
    }

    // GB-4: Leadership check — data ops MUST reach the leader (correctness).
    // // TODO(future): optional internal proxy for single-endpoint transparency.
    if (leaderView_) {
        const LeaderView lv = leaderView_();
        if (!lv.isLeader) {
            pc::ErrorResponse errResp;
            errResp.sessionId     = msg.sessionId;
            errResp.correlationId = msg.correlationId;
            if (!lv.leaderClientEndpoint.empty()) {
                // Known leader endpoint → redirect.
                errResp.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
                errResp.message = lv.leaderClientEndpoint;
                spdlog::debug("ClientGateway: Subscribe redirect sessionId={} → {}",
                              msg.sessionId, lv.leaderClientEndpoint);
            } else {
                // Leader unknown → NotLeader (client backs off + retries).
                errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                spdlog::debug("ClientGateway: Subscribe NotLeader (endpoint unknown) sessionId={}",
                              msg.sessionId);
            }
            const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                       pc::ErrorResponse::BODY_FIXED + 2 +
                                       errResp.message.size();
            if (needed <= scratch_.size()) {
                const std::size_t encoded = errResp.encode(scratch_.data());
                sendTo(*session, scratch_.data(), encoded);
            }
            return;  // engine untouched, no subscription registered
        }
    }

    // Helper: send SubscribeResponse on the session egress.
    auto sendResp = [&](pc::ErrorCode ec, int32_t partition, int64_t subId) {
        pc::SubscribeResponse resp;
        resp.sessionId      = msg.sessionId;
        resp.correlationId  = msg.correlationId;
        resp.errorCode      = static_cast<int16_t>(ec);
        resp.partition      = partition;
        resp.subscriptionId = subId;
        static_assert(pc::SubscribeResponse::ENCODED_SIZE <= kScratchSize,
                      "scratch too small for SubscribeResponse");
        const std::size_t encoded = resp.encode(scratch_.data());
        sendTo(*session, scratch_.data(), encoded);
    };

    // 3. Resolve partition.
    int32_t partition = msg.partition;
    if (msg.partitionKnown == 0) {
        if (!topicRegistry_) {
            spdlog::warn("ClientGateway: Subscribe key-partition but no TopicRegistry — UnknownTopic");
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
        partition = topicRegistry_->partitionForKey(msg.topicId, msg.key);
        if (partition < 0) {
            spdlog::debug("ClientGateway: Subscribe partitionForKey → unknown topicId={}", msg.topicId);
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
    }

    // 4. Validate topic + partition.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway: Subscribe but no TopicRegistry — UnknownTopic");
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    const rune::node::TopicSpec* spec = topicRegistry_->topic(msg.topicId);
    if (!spec) {
        spdlog::debug("ClientGateway: Subscribe unknown topicId={}", msg.topicId);
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    rune::storage::Log* log = topicRegistry_->log(msg.topicId, partition);
    if (!log) {
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }

    // 5. Clamp fromOffset to [0, LEO].
    //    fromOffset < 0 → "from Latest" (LEO); fromOffset > LEO → clamp to LEO.
    //    Convention: negative fromOffset is the "Latest" sentinel documented in the brief.
    //
    // GB-5 Fix #5 — Sub-batch over-delivery (Kafka-consistent whole-batch semantics):
    //    If fromOffset lands mid-batch (i.e., between the batch's baseOffset and its
    //    last record offset), pushSubscriptions() will deliver the ENTIRE containing
    //    batch starting at baseOffset — not just the records from fromOffset onward.
    //    This is intentional (Kafka-compatible whole-batch delivery): the broker never
    //    splits batches, so the client must filter sub-batch records client-side.
    //    Example: batch [baseOffset=0, recordCount=5], fromOffset=3 → the whole batch
    //    (offsets 0..4) is delivered; client drops records 0..2 and consumes 3..4.
    // CB-5: honour the client's From sentinels distinctly (see client
    // From::toWire()):  Earliest = -2 → offset 0 (deliver the whole log);
    // Latest = -1 → LEO (only new records).  Previously ALL negatives mapped
    // to LEO, which silently dropped Earliest subscriptions (0/N delivered).
    static constexpr int64_t kFromEarliest = -2;
    static constexpr int64_t kFromLatest   = -1;
    const int64_t leo = log->logEndOffset();
    int64_t nextOffset;
    if (msg.fromOffset == kFromEarliest) {
        // Earliest → from the beginning of the log.
        nextOffset = 0;
    } else if (msg.fromOffset < 0) {
        // Latest (-1) or any other negative sentinel → subscribe to new records.
        (void)kFromLatest;
        nextOffset = leo;
    } else if (msg.fromOffset > leo) {
        // Beyond LEO → clamp to LEO (no data there yet).
        nextOffset = leo;
    } else {
        nextOffset = msg.fromOffset;
    }

    // 6. Allocate subscriptionId and register subscription.
    const int64_t subId = ++nextSubscriptionId_;

    Subscription sub;
    sub.subscriptionId = subId;
    sub.sessionId      = msg.sessionId;
    sub.topicId        = msg.topicId;
    sub.partition      = partition;
    sub.nextOffset     = nextOffset;
    sub.isolation      = msg.isolation;

    subscriptions_.emplace(subId, std::move(sub));

    // Reply SubscribeResponse{OK, partition, subscriptionId}.
    sendResp(pc::ErrorCode::OK, partition, subId);

    spdlog::debug("ClientGateway: Subscribe sessionId={} subId={} topic={} partition={} nextOffset={} isolation={}",
                  msg.sessionId, subId, msg.topicId, partition, nextOffset, msg.isolation);
}

// ---------------------------------------------------------------------------
// handleUnsubscribe (GB-3, tid21) — remove a subscription.
//
// No response required (brief: "just remove").
// Drop-safe: unknown subscriptionId is silently ignored.
// ---------------------------------------------------------------------------
void ClientGateway::handleUnsubscribe(const uint8_t* buf, std::size_t len)
{
    auto msgOpt = pc::Unsubscribe::decode(buf, len);
    if (!msgOpt) {
        spdlog::debug("ClientGateway: Unsubscribe decode failed — dropped");
        return;
    }
    const std::size_t removed = subscriptions_.erase(msgOpt->subscriptionId);
    spdlog::debug("ClientGateway: Unsubscribe subId={} removed={}",
                  msgOpt->subscriptionId, removed);
}

// ---------------------------------------------------------------------------
// pushSubscriptions (GB-3) — push new Records from the Log to each subscriber.
//
// Called from doWork() AFTER the ingress poll (single-writer, duty-cycle thread).
//
// For each active subscription:
//   - Compute isolation bound: both read_uncommitted (0) and read_committed (1)
//     use logEndOffset() for M-B (no transactions yet → LSO == LEO).
//     // TODO(Milestone C): read_committed uses the real LSO once transactions land.
//   - If nextOffset >= bound → skip (nothing to push).
//   - Otherwise: Log::read(nextOffset, kMaxChunkBytes) → iterate whole batches:
//       For each batch (identified via kLengthOffset = 12 + LE32@8):
//         Encode Records{subscriptionId, topicId, partition, baseOffset, batchBytes}.
//         tryClaim on session egress.
//         SUCCESS → advance nextOffset = baseOffset + recordCount.
//         BACK_PRESSURED / not-connected → STOP for this subscription this cycle.
//           Do NOT advance nextOffset (retry next cycle from same offset).
//   - Per-cycle fairness cap: at most kMaxBatchesPerSubPerCycle batches per subscription.
//
// Zero-alloc hot path: scratch_ is the pre-allocated encode buffer; Log::read
// returns a vector (unavoidable OS read) but we do not allocate per-batch.
//
// Correctness guarantee: nextOffset is NEVER advanced past a batch that was not
// successfully pushed (tryClaim > 0).  This ensures exactly-once delivery in order,
// even under back-pressure.
// ---------------------------------------------------------------------------
void ClientGateway::pushSubscriptions()
{
    using namespace rune::protocol::batch;

    if (subscriptions_.empty()) return;

    for (auto& [subId, sub] : subscriptions_) {
        // Find the owning session.
        Session* session = sessions_.find(sub.sessionId);
        if (!session) {
            // Session gone — subscription will be reaped; skip this cycle.
            continue;
        }

        // Compute isolation bound.
        // TODO(Milestone C): read_committed uses the real LSO once transactions land.
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        if (!log) continue; // registry removed or not set — skip

        const int64_t bound = log->logEndOffset();  // both isolation modes use LEO for M-B

        if (sub.nextOffset >= bound) continue; // nothing new to push

        // Fetch a bounded chunk of bytes from the log starting at nextOffset.
        std::vector<uint8_t> chunk;
        try {
            chunk = log->read(sub.nextOffset, kMaxChunkBytes);
        } catch (const std::exception& ex) {
            spdlog::warn("ClientGateway: Log::read threw for sub={}: {}", subId, ex.what());
            continue;
        } catch (...) {
            spdlog::warn("ClientGateway: Log::read threw unknown exception for sub={}", subId);
            continue;
        }

        if (chunk.empty()) continue; // nothing returned (nextOffset not yet visible)

        // Iterate whole record batches in the chunk.
        // Batch layout: kBaseOffsetOffset=0 (LE64), kLengthOffset=8 (LE32 = bytes after this field),
        //               total batch size = 12 + LE32@8; kRecordCountOffset=57 (LE32).
        std::size_t chunkPos  = 0;
        const std::size_t chunkSize = chunk.size();
        int batchesPushed = 0;
        bool backPressured = false;

        while (chunkPos < chunkSize && batchesPushed < kMaxBatchesPerSubPerCycle && !backPressured) {
            // Need at least kRecordCountOffset + 4 bytes to read header fields.
            constexpr std::size_t kMinBatchHdr =
                static_cast<std::size_t>(kRecordCountOffset) + 4u;
            if (chunkPos + kMinBatchHdr > chunkSize) break; // partial header at end of chunk

            const uint8_t* batchPtr = chunk.data() + chunkPos;

            // Read baseOffset (LE64@0).
            int64_t baseOffset = 0;
            {
                for (int i = 0; i < 8; ++i)
                    baseOffset |= (static_cast<int64_t>(batchPtr[i]) << (i * 8));
            }

            // Read length field (LE32@8) → batchSize = 12 + length.
            uint32_t lengthField = 0;
            {
                const uint8_t* p = batchPtr + kLengthOffset;
                lengthField = static_cast<uint32_t>(p[0]) |
                              (static_cast<uint32_t>(p[1]) << 8) |
                              (static_cast<uint32_t>(p[2]) << 16) |
                              (static_cast<uint32_t>(p[3]) << 24);
            }
            const std::size_t batchSize = 12u + static_cast<std::size_t>(lengthField);

            // Guard: the batch must fit within the remaining chunk.
            if (chunkPos + batchSize > chunkSize) break; // incomplete batch at end

            // Read recordCount (LE32@57).
            int32_t recordCount = 0;
            {
                const uint8_t* p = batchPtr + kRecordCountOffset;
                recordCount = static_cast<int32_t>(
                    static_cast<uint32_t>(p[0]) |
                    (static_cast<uint32_t>(p[1]) << 8) |
                    (static_cast<uint32_t>(p[2]) << 16) |
                    (static_cast<uint32_t>(p[3]) << 24));
            }

            // Guard: recordCount must be positive.
            if (recordCount < 1) {
                // M2 fix: corrupt batch — skip it and advance BOTH chunkPos AND
                // sub.nextOffset so the subscription makes forward progress.
                //
                // Without advancing nextOffset, the next pushSubscriptions() cycle
                // re-reads from the same offset, re-encounters the same corrupt
                // batch, and stalls permanently (the subscription can never advance
                // past it).  This is the mid-chunk analogue of the GB-5p1 fix
                // applied to the too-large/u16-overflow branches below.
                //
                // A corrupt batch in a recovered log should be unreachable
                // (recover() drops torn tails), but if it occurs (e.g. a bug
                // writes recordCount=0 to the log), this ensures no permanent stall.
                //
                // Note: this is NOT the back-pressure path.  Back-pressure (pos≤0)
                // must NOT advance nextOffset — the back-pressured batch will be
                // retried next cycle from the same offset (see the else-branch below).
                spdlog::warn("ClientGateway: push sub={} corrupt batch at chunkPos={} "
                             "recordCount={} baseOffset={} — skipping, advancing nextOffset",
                             subId, chunkPos, recordCount, baseOffset);
                chunkPos += batchSize;
                // Use baseOffset+1 as the best-effort next offset when recordCount
                // is zero/negative (we cannot compute lastOffset = base + recordCount - 1).
                sub.nextOffset = baseOffset + 1;
                continue;
            }

            // Encode Records{subscriptionId, topicId, partition, baseOffset, batchBytes}.
            // The batch bytes are a zero-copy span into the chunk.
            const std::size_t needed =
                pc::Records::BODY_FIXED + pc::MessageHeader::ENCODED_SIZE + 2u + batchSize;

            // GB-5 Fix #1: on skip of a genuinely-unsendable batch, advance BOTH
            // chunkPos AND sub.nextOffset so the subscription makes forward progress.
            // Without this fix the subscription stalls permanently at the oversized
            // batch, losing all subsequent records.
            // This must NOT apply to normal (sendable) batches or back-pressured
            // batches — only to batches that can never be encoded/sent.
            if (needed > kScratchSize) {
                // Batch too large for scratch — skip (should not happen: u16 cap = 65535).
                spdlog::warn("ClientGateway: push sub={} batch too large ({} > {}); skipping, advancing nextOffset",
                             subId, needed, kScratchSize);
                chunkPos += batchSize;
                sub.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                continue;
            }

            // Also guard: Records uses a u16 length prefix for recordBatch.
            if (batchSize > 65535u) {
                spdlog::warn("ClientGateway: push sub={} batchSize {} exceeds u16 max; skipping, advancing nextOffset",
                             subId, batchSize);
                chunkPos += batchSize;
                sub.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                continue;
            }

            pc::Records rec;
            rec.sessionId      = sub.sessionId;
            rec.correlationId  = 0;  // push; no correlation
            rec.subscriptionId = sub.subscriptionId;
            rec.topicId        = sub.topicId;
            rec.partition      = sub.partition;
            rec.baseOffset     = baseOffset;
            rec.recordBatch    = std::span<const uint8_t>(batchPtr, batchSize);

            const std::size_t encoded = rec.encode(scratch_.data());

            // Push via tryClaim (no offer-spin — single writer, no blocking).
            aeron::ExclusivePublication& pub = *session->egress;
            int64_t pos = -1;
            if (pub.isConnected() && encoded <= static_cast<std::size_t>(pub.maxPayloadLength())) {
                aeron::concurrent::logbuffer::BufferClaim claim;
                pos = pub.tryClaim(static_cast<aeron::util::index_t>(encoded), claim);
                if (pos > 0) {
                    auto* mutableBuf = const_cast<uint8_t*>(scratch_.data());
                    aeron::concurrent::AtomicBuffer atomicBuf(
                        mutableBuf, static_cast<aeron::util::index_t>(encoded));
                    claim.buffer().putBytes(claim.offset(), atomicBuf, 0,
                                            static_cast<aeron::util::index_t>(encoded));
                    claim.commit();
                }
            }

            if (pos > 0) {
                // SUCCESS: advance nextOffset exactly past this batch.
                sub.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                chunkPos += batchSize;
                ++batchesPushed;
            } else {
                // BACK_PRESSURED or not-connected: STOP for this subscription this cycle.
                // Do NOT advance nextOffset — retry from the same offset next cycle.
                backPressured = true;
                spdlog::debug("ClientGateway: push sub={} back-pressured at offset={} (pos={})",
                              subId, baseOffset, pos);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// flushDirtyLogs (GB-5p2) — group-commit fsync for the produce hot path.
//
// Called from doWork() every cycle, but only performs the actual fsync when
// kGroupCommitIntervalNs nanoseconds have elapsed since the last flush.
// This is the time-based group-commit cadence:
//
//   - Produces BETWEEN flushes: appendNoSync() writes to page cache at ~1-10µs.
//     ProduceResponse is sent immediately (acks=Leader, page-cache-durable).
//     Data is visible to Log::read() before the fsync.
//   - At flush time: one fdatasync per dirty partition Log amortises the fsync
//     cost over every produce since the last flush.
//   - Under idle (no produces): dirtyLogs_ is empty; the timer still fires but
//     no syscalls are made → zero overhead.
//
// Durability contract — acks=Leader (acks=1):
//   Data written since the last flush is in the OS page cache and visible to
//   Log::read(), but NOT fsync-durable.  A leader crash before the next flush
//   may lose the most-recent un-flushed acked records.  The maximum durability
//   window is kGroupCommitIntervalNs (10 ms by default).  True fsync-durability
//   requires acks=All + ISR replication (Milestone C).
//
// TODO(Milestone C): when acks=All is wired, the ProduceResponse must be deferred
//   until ISR HWM advances past lastOffset on all in-sync replicas.  Today we ack
//   immediately after page-cache write.
//
// Single-writer: called only on the duty-cycle thread → no locking.
// Zero per-produce heap: dirtyLogs_ is a pre-allocated unordered_set<Log*>.
// ---------------------------------------------------------------------------
void ClientGateway::flushDirtyLogs()
{
    // Time-based cadence: skip if the flush interval has not elapsed yet.
    // This is the key that keeps fsync off the per-produce hot path.
    // Lazy init: lastFlushNs_ == 0 means "not yet set"; initialise to nowNs_
    // so the first flush fires after kGroupCommitIntervalNs, not immediately.
    if (lastFlushNs_ == 0) {
        lastFlushNs_ = nowNs_;
        return;
    }
    if (nowNs_ - lastFlushNs_ < kGroupCommitIntervalNs) return;

    if (dirtyLogs_.empty()) {
        // No produces since last flush — just advance the timer.
        lastFlushNs_ = nowNs_;
        return;
    }

    // Flush each dirty partition Log exactly once per interval.
    // MC6 fix: if sync() throws, keep the Log in dirtyLogs_ so the next cycle
    // retries (don't silently mark it clean on fsync failure).  An fsync failure
    // means the data is NOT durable; falsely clearing dirtyLogs_ would ack
    // durability that was never achieved.  We log the error but do NOT crash —
    // fsync failure is surfaced here and the dirty state is retained.
    // Note: on Linux, fsync failure may be unrecoverable (EIO/fsyncgate semantics);
    // at Milestone C this path should escalate to a leader step-down.
    std::unordered_set<rune::storage::Log*> stillDirty;
    for (rune::storage::Log* log : dirtyLogs_) {
        bool synced = false;
        try {
            log->sync();
            synced = true;
        } catch (const std::exception& ex) {
            spdlog::error("ClientGateway: flushDirtyLogs log->sync() threw: {}; "
                          "log stays dirty (durability NOT achieved)", ex.what());
        } catch (...) {
            spdlog::error("ClientGateway: flushDirtyLogs log->sync() threw unknown exception; "
                          "log stays dirty (durability NOT achieved)");
        }
        if (!synced) stillDirty.insert(log);
    }
    dirtyLogs_ = std::move(stillDirty);
    lastFlushNs_ = nowNs_;
}

} // namespace rune::gateway
