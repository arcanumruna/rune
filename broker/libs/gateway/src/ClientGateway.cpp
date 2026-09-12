// ClientGateway.cpp — implementation of the client-facing session gateway (BR-4 / BR-5 / BR-7a).

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>

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

        if (!lv.isLeader && !lv.leaderClientEndpoint.empty()) {
            // REDIRECT: resolve a temporary egress to the client synchronously
            // (this is a non-leader redirect — only happens on follower nodes,
            //  not on the critical leader path).
            // TODO(Milestone B): route redirect egress through the async pending machine —
            // this synchronous resolvePublication would block the consensus thread once
            // per-peer client endpoints exist; unreachable in Milestone A (single-node is
            // always leader; multi-node followers return leaderClientEndpoint="" per the
            // ruling → degrade-to-accept, never redirect).
            auto pub = resolvePublication(std::string(req.egressChannel), req.egressStreamId);
            if (!pub) return; // can't reach client — drop

            pc::ErrorResponse err;
            err.sessionId     = 0;
            err.correlationId = req.correlationId;
            err.code          = static_cast<int16_t>(pc::ErrorCode::Redirect);
            err.message       = lv.leaderClientEndpoint;

            // Guard: message must fit in scratch.
            const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                       pc::ErrorResponse::BODY_FIXED + 2 +
                                       lv.leaderClientEndpoint.size();
            if (needed <= scratch_.size()) {
                const std::size_t encoded = err.encode(scratch_.data());
                sendOnPub(*pub, scratch_.data(), encoded);
            }
            // pub goes out of scope → ExclusivePublication released (no session created).
            return;
        }
        // else: leader-unknown (empty endpoint) → degrade to local accept below.
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
        const int64_t evictedRegId = pendingConnects_.front().registrationId;
        pendingConnects_.pop_front(); // O(1) on deque
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
            // Release the registration: attempt final findExclusivePublication.
            // If resolved (non-null shared_ptr): drop it — destructor calls
            // ClientConductor::releaseExclusivePublication(registrationId)
            // (Aeron C++ 1.41.4: ~ExclusivePublication() → m_conductor.releaseExclusivePublication).
            // If throws (ERRORED): conductor already cleaned up — no leak.
            // If nullptr (still AWAITING): enqueue in drainingRegistrations_ and
            // keep polling per-cycle until the conductor resolves it.
            const int64_t timedOutRegId = pending.registrationId;
            it = pendingConnects_.erase(it);
            try {
                auto pub = aeron_->findExclusivePublication(timedOutRegId);
                if (!pub) {
                    drainingRegistrations_.push_back(timedOutRegId);
                }
                // pub non-null: drops here → destructor releases registration.
            } catch (...) {
                // ERRORED_MEDIA_DRIVER: conductor erased the entry — no leak.
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
            // Not yet resolved — try again next cycle.
            ++it;
            continue;
        }

        // Resolved! Attempt to send ConnectResponse BEFORE adding the session.
        //
        // Invariant (FIX A): a session must NEVER be added to the table unless
        // the client received a ConnectResponse for it.  If back-pressure causes
        // the offer to fail we keep the PendingConnect entry (with the already-
        // resolved shared_ptr stashed) and retry the send on the next doWork
        // cycle.  The resolve-timeout still applies: if the timeout elapses
        // before the send succeeds we drop the pending entry and release the
        // publication — no session is created, and the client times out and
        // re-Connects.
        pc::ConnectResponse resp;
        resp.sessionId         = pending.sessionId;
        resp.correlationId     = pending.correlationId;
        resp.code              = static_cast<int16_t>(pc::ErrorCode::OK);
        resp.leaderMemberId    = thisNodeId_;
        resp.assignedSessionId = pending.sessionId;

        const std::size_t encoded = resp.encode(scratch_.data());

        if (!sendOnPub(*pub, scratch_.data(), encoded)) {
            // Back-pressure: the publication resolved but the ConnectResponse
            // could not be sent (term buffer full / not yet connected). Send
            // BEFORE adding the session so we never create a session the client
            // was not told about: drop the pending entry and release the egress
            // pub (its resolved shared_ptr goes out of scope here →
            // ~ExclusivePublication releases the registration — no leak). The
            // client re-Connects. (A retry-in-place sub-state is deferred to
            // Milestone B; a transient re-Connect is acceptable for now.)
            spdlog::debug("ClientGateway: ConnectResponse back-pressured for session={}; "
                          "releasing pub, client will re-Connect",
                          pending.sessionId);
            it = pendingConnects_.erase(it);
            continue;
        }

        // Send succeeded — only now is it safe to add the session.
        sessions_.add(pending.sessionId, std::move(pub), nowNs);

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

    // Read recordCount (LE32 at kRecordCountOffset).
    int32_t recordCount = 0;
    {
        const uint8_t* p = restampScratch_.data() + rune::protocol::batch::kRecordCountOffset;
        recordCount = static_cast<int32_t>(
            static_cast<uint32_t>(p[0]) |
            (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) |
            (static_cast<uint32_t>(p[3]) << 24));
    }

    if (recordCount < 1) {
        spdlog::debug("ClientGateway: Produce recordCount={} < 1; dropped", recordCount);
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    const int64_t lastOffset = base + static_cast<int64_t>(recordCount) - 1;

    // Build a vector for the Log::append call (Log takes const std::vector<uint8_t>&).
    // Single-writer: this is the unavoidable copy for offset-restamp storage.
    // We reuse restampScratch_ as the source to avoid double-copy.
    const std::vector<uint8_t> batchVec(
        restampScratch_.data(),
        restampScratch_.data() + batchLen);

    try {
        log->append(batchVec, lastOffset);
    } catch (const std::exception& ex) {
        spdlog::error("ClientGateway: Log::append threw: {}; ProduceResponse Internal", ex.what());
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    } catch (...) {
        spdlog::error("ClientGateway: Log::append threw unknown exception; ProduceResponse Internal");
        sendResp(pc::ErrorCode::Internal, partition, 0);
        return;
    }

    // 6. Reply ProduceResponse{OK, partition, baseOffset=base}.
    // TODO(Milestone B GB-4/replication): true acks=All waits for ISR HW advance.
    // Produce in M-B does not carry an acks field; we always ack after local append.
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
    const int64_t leo = log->logEndOffset();
    int64_t nextOffset;
    if (msg.fromOffset < 0) {
        // Negative → from Latest (subscribe only to new records).
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
                // Corrupt batch — skip it to avoid getting stuck.
                spdlog::warn("ClientGateway: push sub={} corrupt batch at chunkPos={} recordCount={} — skipping",
                             subId, chunkPos, recordCount);
                chunkPos += batchSize;
                continue;
            }

            // Encode Records{subscriptionId, topicId, partition, baseOffset, batchBytes}.
            // The batch bytes are a zero-copy span into the chunk.
            const std::size_t needed =
                pc::Records::BODY_FIXED + pc::MessageHeader::ENCODED_SIZE + 2u + batchSize;
            if (needed > kScratchSize) {
                // Batch too large for scratch — skip (should not happen: u16 cap = 65535).
                spdlog::warn("ClientGateway: push sub={} batch too large ({} > {}); skipping",
                             subId, needed, kScratchSize);
                chunkPos += batchSize;
                continue;
            }

            // Also guard: Records uses a u16 length prefix for recordBatch.
            if (batchSize > 65535u) {
                spdlog::warn("ClientGateway: push sub={} batchSize {} exceeds u16 max; skipping",
                             subId, batchSize);
                chunkPos += batchSize;
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

} // namespace rune::gateway
