// ClientGateway.cpp — implementation of the client-facing session gateway (BR-4 / BR-5 / BR-7a / GR-4 / GR-5).

#include <rune/catalog/NsName.h>
#include <rune/node/NamespaceLimitCheck.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/LogSafe.h>  // issue #232: client-supplied names in log lines
#include <rune/view/PlanSources.h>
#include <rune/gateway/ConnectDowngradePolicy.h>
#include <rune/gateway/CryptoFrame.h>
#include <rune/gateway/RequestRejection.h>
#include <rune/gateway/BootstrapProof.h>
#include <rune/gateway/SecurityRequestFingerprint.h>
#include <rune/crypto/Kdf.h>
#include <rune/protocol/ProducerIdBinding.h>
#include <rune/crypto/Scram.h>
#include <cassert>
#include <flight_recorder/FlightRecorder.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/gateway/GatewayReplicaView.h>
#include <rune/node/AeronChannel.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <random>
#include <rune/transaction/ControlMarker.h>
// GroupCoordinationService.h is NOT included here (CG-4 fix: break rune_gateway→rune_node
// symbol cycle).  ClientGateway dispatches group messages through IGroupRequestHandler
// (defined in this lib); the concrete type lives in rune_node.
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/IsrManager.h>

// E2 inc2a: SPSC worker routing (included via RUNE_NODE_INCLUDE raw path to
// avoid a circular CMake target dep; rune_gateway does NOT link rune_node).
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/SessionAckSink.h>
// E5.1.2d: shared produce-batch helper now in rune_storage (leaf lib, no circular dep).
#include <rune/storage/ProducePath.h>

#include <rune/protocol/client/Dispatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/RecordBatch.h>

#include <spdlog/spdlog.h>

static_assert(rune::gateway::ClientGateway::kShardMaxSessions ==
                  rune::node::SessionAckSink::kMaxSessions - 1,
              "Task 13: the runtime's shard session cap is a worker ack table's capacity");
static_assert(rune::gateway::ClientGateway::kWorkerRoutedCap == rune::node::Worker::kRoutedAckBudget,
              "D8c fix round 1: the gateway's per-worker routed cap is the worker's routed ack share");
static_assert(rune::gateway::ClientGateway::kMaxSessionProduceCredits * 4 <=
                  rune::gateway::ClientGateway::kWorkerRoutedCap,
              "D8c: one session's produce credit budget stays a small share of a worker's routed ack share");
static_assert(rune::gateway::Session::kMaxWorkers == rune::gateway::ClientGateway::kMaxHeldMaskWorkers);
static_assert(rune::gateway::ClientGateway::kLightSessionInFlight * 64 <=
                  rune::gateway::ClientGateway::kWorkerRoutedCap / 4,
              "D8c: the congested band admits >= 64 light sessions");
static_assert(rune::gateway::ClientGateway::kDefaultSessionProduceCredits >= 1 &&
                  rune::gateway::ClientGateway::kDefaultSessionProduceCredits <=
                      rune::gateway::ClientGateway::kMaxSessionProduceCredits);

#include <cstring>

#include <algorithm>
#include <deque>
#include <chrono>


namespace rune::gateway {

namespace pc = rune::protocol::client;

// Namespaces Task 10: the fixed refusal texts (D7: they never echo a name, so they reveal no foreign object).
constexpr const char* kMalformedNameDetail = "malformed qualified name";

// ---------------------------------------------------------------------------
// Startup-only synchronous resolve budget: the ingress subscription in the
// constructor.  Every duty-cycle resolve (Connect / redirect egress) is async
// via pendingConnects_ — the blocking resolvePublication() was deleted (Task 11:
// a duty-cycle sleep-poll would stall worker 0's loop under fused placement).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ClientGateway constructor
// ---------------------------------------------------------------------------
ClientGateway::ClientGateway(std::shared_ptr<aeron::Aeron>   aeron,
                             std::string                     clientChannel,
                             int32_t                         clientStreamId,
                             int32_t                         thisNodeId,
                             int64_t                         sessionTimeoutNs,
                             std::function<LeaderView()>     leaderView,
                             int64_t                         connectResolveTimeoutNs,
                             rune::aeronutil::AeronPump      pump,
                             std::string                     tlsCertPem,
                             std::string                     tlsKeyPem,
                             bool                            requireTls)
    : aeron_(std::move(aeron))
    , clientChannel_(std::move(clientChannel))
    , clientStreamId_(clientStreamId)
    , thisNodeId_(thisNodeId)
    , sessionTimeoutNs_(sessionTimeoutNs)
    , connectResolveTimeoutNs_(connectResolveTimeoutNs)
    , leaderView_(std::move(leaderView))
    , tlsCertPem_(std::move(tlsCertPem))
    , tlsKeyPem_(std::move(tlsKeyPem))
    , requireTls_(requireTls)
{
    // Pre-allocate the draining-release vector (bounded by kMaxPendingConnects).
    // pendingConnects_ is a deque — no reserve() needed; O(1) front push/pop.
    drainingRegistrations_.reserve(kMaxPendingConnects);

    // Cluster-unique session / subscription ids (see setClusterIdNode).
    setClusterIdNode(thisNodeId_);

    // GR-5: Pre-allocate the pending-ack queue to avoid heap allocs on the hot path.
    // The vector is used as a shrinking queue (erase-by-swap-and-pop); capacity
    // stays at kMaxPendingAcks after the initial reserve.
    pendingAcks_.reserve(kMaxPendingAcks);

    // Register the ingress subscription.
    int64_t regId = aeron_->addSubscription(clientChannel_, clientStreamId_);

    // Bounded resolve — same pattern as AeronIngress (spec §3.6: an invoker
    // driver + conductor is pumped by this thread between probes).
    sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); },
                        rune::aeronutil::AeronPump::kUntilAnswered);   // D8: until Aeron answers (errors throw)
    if (!sub_) {
        throw std::runtime_error(
            "ClientGateway: ingress subscription not resolved (driver refused the pump)");
    }
}

// ---------------------------------------------------------------------------
// G0.1: Test-only constructor — skips Aeron setup entirely.
// ---------------------------------------------------------------------------
ClientGateway::ClientGateway(TestConstructTag,
                             TestEgressFn  egressFn,
                             int64_t       currentSessionId)
    : aeron_(nullptr)
    , clientChannel_()
    , clientStreamId_(0)
    , thisNodeId_(0)
    , sessionTimeoutNs_(kDefaultSessionTimeoutNs)
    , connectResolveTimeoutNs_(kDefaultConnectResolveTimeoutNs)
    , leaderView_(nullptr)
    , testEgressFn_(std::move(egressFn))
    , currentProduceSessionId_(currentSessionId)
{
    pendingAcks_.reserve(kMaxPendingAcks);
    drainingRegistrations_.reserve(kMaxPendingConnects);
}

// G1.1: Test-only constructor — also wires leaderView_.
ClientGateway::ClientGateway(TestConstructTag,
                             TestEgressFn                egressFn,
                             std::function<LeaderView()> leaderView,
                             int64_t                     currentSessionId)
    : aeron_(nullptr)
    , clientChannel_()
    , clientStreamId_(0)
    , thisNodeId_(0)
    , sessionTimeoutNs_(kDefaultSessionTimeoutNs)
    , connectResolveTimeoutNs_(kDefaultConnectResolveTimeoutNs)
    , leaderView_(std::move(leaderView))
    , testEgressFn_(std::move(egressFn))
    , currentProduceSessionId_(currentSessionId)
{
    pendingAcks_.reserve(kMaxPendingAcks);
    drainingRegistrations_.reserve(kMaxPendingConnects);
}

void ClientGateway::injectPendingHandshakeForTest(int64_t sessionId)
{
    static unsigned char placeholder alignas(64)[512];
    assert(testEgressFn_ && "injectPendingHandshakeForTest needs makeForTest's TestEgressFn");
    PendingConnect pc;
    pc.sessionId         = sessionId;
    pc.awaitingHandshake = true;
    pc.resolvedPub = std::shared_ptr<aeron::ExclusivePublication>(
        reinterpret_cast<aeron::ExclusivePublication*>(placeholder), [](aeron::ExclusivePublication*) {});
    pendingConnects_.push_back(std::move(pc));
}

int64_t ClientGateway::injectReliableTestSessionForTest(int64_t nowNs, int32_t clientVersion)
{
    // A non-null placeholder (never dereferenced while testEgressFn_ intercepts
    // every offer; the no-op deleter frees nothing).
    static unsigned char placeholder alignas(64)[512];
    assert(testEgressFn_ && "injectReliableTestSessionForTest needs makeForTest's TestEgressFn");
    std::shared_ptr<aeron::ExclusivePublication> fake(
        reinterpret_cast<aeron::ExclusivePublication*>(placeholder), [](aeron::ExclusivePublication*) {});
    const int64_t sid = sessions_.nextSessionId();
    sessions_.add(sid, std::move(fake), rune::gateway::AuthAnonymous{}, nowNs, {}, 0, clientVersion);
    return sid;
}

// ---------------------------------------------------------------------------
// G0.1: makeForTest — static factory for test-only (no Aeron) construction.
// ---------------------------------------------------------------------------
ClientGateway ClientGateway::makeForTest(TestEgressFn egressFn,
                                         int64_t      currentProduceSessionId)
{
    return ClientGateway(TestConstructTag{},
                         std::move(egressFn),
                         currentProduceSessionId);
}

// G1.1: makeForTestWithLeaderView — like makeForTest but also wires leaderView_.
// ---------------------------------------------------------------------------
ClientGateway ClientGateway::makeForTestWithLeaderView(TestEgressFn             egressFn,
                                                       std::function<LeaderView()> leaderView,
                                                       int64_t currentProduceSessionId)
{
    return ClientGateway(TestConstructTag{},
                         std::move(egressFn),
                         std::move(leaderView),
                         currentProduceSessionId);
}

// ---------------------------------------------------------------------------
// G0.1: offerBytes_ — try to deliver encoded bytes via the egress.
//
// Uses testEgressFn_ when set (non-null only in tests), otherwise calls
// sendTo on the provided session pointer.
// Returns > 0 on success, <= 0 on back-pressure / failure.
// ---------------------------------------------------------------------------
int64_t ClientGateway::offerBytes_(Session* session,
                                   const uint8_t* buf,
                                   std::size_t    len)
{
    if (testEgressFn_) {
        return testEgressFn_(buf, len);
    }
    if (!session) return -1;
    // sendTo returns bool: true = delivered (may have used tryClaim or offer),
    // false = back-pressure / not-connected.  Map to int64_t for a uniform API.
    return sendTo(*session, buf, len) ? 1 : -1;
}

// ---------------------------------------------------------------------------
// CG round 3 (MEDIUM-2): a Subscribe's answer (SubscribeResponse / its
// Redirect / NotLeader) is queued and retried under back-pressure like the
// produce answers (sendReliable_) — never a best-effort drop of the
// answer that binds the client's subscription.  Sessions without a real egress
// publication (test fixtures: injectTestSession / the null-egress capture
// hook) keep the direct sendTo path they are observed through.
// ---------------------------------------------------------------------------
void ClientGateway::sendAnswer_(Session& session, const uint8_t* buf, std::size_t len)
{
    if (!session.egress) {
        (void)sendTo(session, buf, len);
        return;
    }
    sendReliable_(session, buf, len);
}

// ---------------------------------------------------------------------------
// G0.1 / no-hang fix rounds 2-4 (I1 / N8): sendReliable_ — THE reliable-egress
// primitive.
//
// EVERY client answer (produce acks, redirects, error answers, DDL / group /
// txn / metadata / view-row answers, Pong) routes through this function.  Per
// the session's spill state (Session::spill, SessionTable.h):
//   Normal   — immediate delivery (offerBytes_); one compare on the hot path.
//              Refused → the answer is spilled and the session becomes Spilled.
//   Spilled  — earlier answers still wait: this one is appended behind them
//              (per-session FIFO), re-offered by drainPendingResponses.
//   Expiring — refused and counted (the session's SessionExpired is owed).
//   Closing  — issue #119: refused and counted; the session's LAST answer is
//              already queued, and nothing may overtake or follow it.
// Over the per-session quota, or a record larger than the staging buffer: the
// session itself is EXPIRED.  Pool full: the session holding the most pool
// bytes is expired (possibly this one) — never a best-effort send, never a
// silent drop.
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::sendReliable_(Session&       session,
                                  const uint8_t* buf,
                                  std::size_t    len)
{
    // Fix round 5: every gateway-encoded answer fits the spill's staging buffer
    // (a larger scratch must fail the build, not expire healthy clients).
    static_assert(kScratchSize <= kMaxSpillRecordBytes, "gateway scratch answers must be spillable");
    if (session.spill.state == SpillState::Normal) [[likely]] {
        if (offerBytes_(&session, buf, len) > 0) return;   // delivered
    } else if (session.spill.state == SpillState::Expiring) [[unlikely]] {
        ++spillExpiryDrops_;   // the session is being expired: SessionExpired tells the client
        return;
    } else if (session.spill.state == SpillState::Closing) [[unlikely]] {
        ++closingAnswerDrops_;   // issue #119: its last answer is queued - never appended to, never overtaken
        return;
    }
    // Fix round 3 (I1 fairness) / round 4 (N10): the quota counts POOL bytes; the
    // session over quota is the one expired — the slow / stalled consumer,
    // never an innocent pusher.
    if (len > kMaxSpillRecordBytes ||
        session.spill.bytes + SpillPool::bytesFor(len) > kSessionSpillQuota) {
        expireForSpill_(session);
        return;
    }
    while (!egressSpill_.append(session.spill.chain, buf, len)) {
        // Pool full: expire the session holding the MOST pool bytes (possibly
        // this one) — O(1) to free (its chain is spliced back) — and retry.
        Session* hog = largestSpiller_();
        if (hog == nullptr || hog == &session || hog->spill.bytes <= session.spill.bytes) {
            expireForSpill_(session);
            return;
        }
        expireForSpill_(*hog);
    }
    session.spill.bytes += static_cast<uint32_t>(SpillPool::bytesFor(len));
    if (session.spill.state == SpillState::Normal) {
        session.spill.state = SpillState::Spilled;
        linkSpillReady_(session);
    }
}

// Cold (pool full only): O(sessions on the ready list), never O(all sessions).
Session* ClientGateway::largestSpiller_()
{
    Session* best = nullptr;
    for (Session* s = spillReadyHead_; s != nullptr; s = s->spill.nextReady)
        if (best == nullptr || s->spill.bytes > best->spill.bytes) best = s;
    return best;
}

void ClientGateway::linkSpillReady_(Session& s) noexcept
{
    s.spill.prevReady = spillReadyTail_;
    s.spill.nextReady = nullptr;
    if (spillReadyTail_ != nullptr) spillReadyTail_->spill.nextReady = &s; else spillReadyHead_ = &s;
    spillReadyTail_ = &s;
    ++spillReadySessions_;
}

void ClientGateway::unlinkSpillReady_(Session& s) noexcept
{
    if (s.spill.prevReady != nullptr) s.spill.prevReady->spill.nextReady = s.spill.nextReady;
    else spillReadyHead_ = s.spill.nextReady;
    if (s.spill.nextReady != nullptr) s.spill.nextReady->spill.prevReady = s.spill.prevReady;
    else spillReadyTail_ = s.spill.prevReady;
    s.spill.prevReady = nullptr;
    s.spill.nextReady = nullptr;
    --spillReadySessions_;
}

bool ClientGateway::spillWritable_(const Session& s) const noexcept
{
    // Test egress (testEgressFn_) and egress-less sessions: the offer decides.
    if (testEgressFn_ || !s.egress) return true;
    // Aeron refuses an offer only when not connected or position >= limit.
    return s.egress->isConnected() && s.egress->availableWindow() > 0;
}

// The spill cannot hold this session's answers any more.  Fix round 3 (I1) /
// round 4 (N8, N9): the session enters the explicit EXPIRING state — its
// spilled answers are released (O(1): the chain is spliced back), every new
// answer is refused (counted), its ingress is dropped (admit path), and its
// SessionExpired is re-offered each drain (serviceSpillExpiring_) until it is
// delivered or the lease deadline passes; only then is the session closed.
// The publication stays alive until then (it is the only way to tell the client).
void ClientGateway::expireForSpill_(Session& session)
{
    if (spillExpiries_++ % 1024 == 0)
        spdlog::error("ClientGateway: egress spill: session={} holds {} B ({} answers; spill {} B of {} B) — "
                      "expiring it (SessionExpired) instead of dropping answers (expiries={})",
                      session.sessionId, session.spill.bytes, session.spill.chain.records,
                      egressSpill_.usedBytes(), egressSpill_.capacityBytes(), spillExpiries_);
    if (session.spill.state == SpillState::Spilled) {
        (void)egressSpill_.freeAll(session.spill.chain);
        unlinkSpillReady_(session);
    } else if (session.spill.state == SpillState::Closing) {
        // Issue #119: largestSpiller_ may pick a Closing session when the pool is full.  Its owed last answers are
        // released like a Spilled session's and it is expired: under pool exhaustion the client gets SessionExpired
        // instead of its 46 / 44 / 48 - still a definite end (it reconnects), never a silent vanish.
        (void)egressSpill_.freeAll(session.spill.chain);
        unlinkSpillReady_(session);
        --closingSessions_;
    }
    session.spill = SessionSpill{};
    session.spill.state = SpillState::Expiring;
    ++expiringSessions_;
    if (spillExpiringCount_ == spillExpiring_.size()) [[unlikely]] {
        // The preallocated EXPIRING table is full: the session stays marked
        // (every answer refused) and is adopted by serviceSpillExpiring_ as soon
        // as a slot frees — never closed here (a caller may still hold it).
        ++spillExpiringOverflow_;
        return;
    }
    spillExpiring_[spillExpiringCount_++] = SpillExpiring{session.sessionId, nowNs_ + sessionTimeoutNs_};
}

void ClientGateway::serviceSpillExpiring_(int64_t nowNs)
{
    pc::ErrorResponse e;
    e.correlationId = 0;
    e.code          = static_cast<int16_t>(pc::ErrorCode::SessionExpired);
    for (std::size_t i = 0; i < spillExpiringCount_;) {
        SpillExpiring& ex = spillExpiring_[i];
        Session* s = sessions_.find(ex.sessionId);
        bool done = false;
        if (s == nullptr) {
            // Fix round 4 (N15): closed meanwhile (reaped / disconnected) before
            // its SessionExpired could be delivered — counted as notify-lost.
            ++spillExpiryNotifyLost_;
            done = true;
        } else {
            // Fix round 4 (N9): once serviced, an Expiring session's
            // subscriptions end (gateway pushes AND the owning workers'
            // cursors) — no Records compete with its SessionExpired.
            if (!ex.subscriptionsDropped) {
                dropSessionSubscriptions_(ex.sessionId);
                ex.subscriptionsDropped = true;
            }
            e.sessionId = ex.sessionId;
            const std::size_t n = e.encode(scratch_.data());
            const bool sent = s->egress ? offerBytes_(s, scratch_.data(), n) > 0
                                        : sendTo(*s, scratch_.data(), n);   // egress-less test session (hook)
            if (sent) {
                ++spillExpiryNotified_;
                done = true;
            } else if (nowNs >= ex.deadlineNs) {
                ++spillExpiryNotifyLost_;   // lease over: the client's own keepalive timeout has fired too
                done = true;
            }
            if (done) (void)closeSession_(ex.sessionId, /*notifyExpired=*/false);
        }
        if (done) spillExpiring_[i] = spillExpiring_[--spillExpiringCount_];
        else ++i;
    }
    // Marked sessions the full table could not take yet: adopt them now.
    if (spillExpiringOverflow_ != 0 && spillExpiringCount_ < spillExpiring_.size()) [[unlikely]] {
        sessions_.forEach([&](Session& s) {
            if (s.spill.state != SpillState::Expiring || spillExpiringCount_ == spillExpiring_.size()) return;
            for (std::size_t k = 0; k < spillExpiringCount_; ++k)
                if (spillExpiring_[k].sessionId == s.sessionId) return;
            spillExpiring_[spillExpiringCount_++] = SpillExpiring{s.sessionId, nowNs + sessionTimeoutNs_};
        });
        spillExpiringOverflow_ = (spillExpiringCount_ == spillExpiring_.size()) ? spillExpiringOverflow_ : 0;
    }
}

// Fix round 4 (N9): one probe of the frame's header session (bytes 8..15, as
// produceFramePause_) — called only while some session is Expiring.
bool ClientGateway::dropExpiringFrame_(const uint8_t* buf, std::size_t len)
{
    if (len < pc::MessageHeader::ENCODED_SIZE) return false;
    uint64_t sid = 0;
    for (int i = 7; i >= 0; --i) sid = (sid << 8) | buf[8 + i];
    const Session* s = sessions_.find(static_cast<int64_t>(sid));
    if (s == nullptr) return false;
    if (s->spill.state == SpillState::Closing) [[unlikely]] {
        // Issue #119: the session was sent its last answer - none of its frames is executed.  Only a PLAINTEXT
        // Disconnect closes it now (nobody waits for the rest); on an encrypted session the frame is unauthenticated
        // here (see below) and is dropped like any other.
        const uint16_t ctid = static_cast<uint16_t>(buf[2] | (buf[3] << 8));
        if (ctid == static_cast<uint16_t>(pc::TemplateId::Disconnect) && !s->crypto.encrypted) {
            (void)closeSession_(static_cast<int64_t>(sid), /*notifyExpired=*/false);
            return true;
        }
        ++closingIngressDrops_;
        return true;
    }
    if (s->spill.state != SpillState::Expiring) return false;
    const uint16_t tid = static_cast<uint16_t>(buf[2] | (buf[3] << 8));
    // Only a PLAINTEXT session's Disconnect finalizes: this probe runs before
    // the AEAD check, so on an encrypted session the frame is unauthenticated
    // (session ids are guessable) and must not change its state — it keeps the
    // counted drop and its lease (final no-hang review B1).
    if (tid == static_cast<uint16_t>(pc::TemplateId::Disconnect) && !s->crypto.encrypted) {
        // Fix round 5: the client asked to leave — nobody waits for its
        // SessionExpired.  Finalize now (no notification), leave the EXPIRING
        // table, counted apart (never as notify-lost).
        for (std::size_t i = 0; i < spillExpiringCount_; ++i) {
            if (spillExpiring_[i].sessionId != s->sessionId) continue;
            spillExpiring_[i] = spillExpiring_[--spillExpiringCount_];
            break;
        }
        ++spillExpiryClientLeft_;
        (void)closeSession_(static_cast<int64_t>(sid), /*notifyExpired=*/false);
        return true;
    }
    ++expiringIngressDrops_;   // never executed: the client is owed SessionExpired (→ SessionLost)
    return true;
}

// ---------------------------------------------------------------------------
// F19 / Task 8b: pushRecords_ — send one Records push frame on the session's
// egress.  Real egress, PLAINTEXT session → rune::node::sendRecordsFrame's
// unchanged path (tryClaim fast path for one-MTU frames; vectored
// [fixed | batch] offer above it; TooLarge above maxMessageLength).
//
// Real egress, ENCRYPTED session (session.crypto.encrypted): the Records
// push is the highest-volume broker->client traffic and was bypassing
// sendTo's Task 8 sealing entirely — sent in the clear, dropped by the
// client's decrypt-everything-but-handshake dispatch. Build a
// rune::node::RecordsSealCtx from the session's OWN crypto fields (raw
// params — never rune::gateway::CryptoState itself; RecordsPush.h/
// RecordsSeal.h must not depend on libs/gateway, see RecordsSeal.h's file
// header comment) and pass it + this gateway's own recordsSealScratch_ to
// sendRecordsFrame, which assembles the contiguous plaintext, seals it, and
// publishes the single contiguous sealed buffer (see RecordsPush.h's
// ENCRYPTED-path comment). Gated on `encrypted` alone (not
// `session.crypto.send.has_value()`), exactly like sendTo — a null `send`
// (key not installed) is still passed through as a RecordsSealCtx with a
// null `send`, so sealRecordsFrame fails closed instead of ever falling
// back to plaintext.
//
// Null egress (injectTestSession) → the test capture hook (treated as
// sent); no hook → Retry.
// ---------------------------------------------------------------------------
rune::node::RecordsSendResult ClientGateway::pushRecords_(Session& session, const pc::Records& rec)
{
    // Fix round 4 (N9): an Expiring session receives no Records (its cursor is
    // not advanced; the subscription ends with the session).
    if (session.spill.state == SpillState::Expiring) [[unlikely]] return {rune::node::RecordsSendKind::Retry, -1};
    // Issue #119: nor does a Closing one (its subscriptions ended; no Records overtake its owed last answers).
    if (session.spill.state == SpillState::Closing) [[unlikely]] return {rune::node::RecordsSendKind::Retry, -1};
    if (session.egress) [[likely]] {
        if (session.crypto.encrypted) {
            rune::node::RecordsSealCtx seal;
            seal.send    = session.crypto.send.has_value() ? &*session.crypto.send : nullptr;
            seal.sendSeq = &session.crypto.sendSeq;
            seal.cipher  = static_cast<uint8_t>(session.crypto.cipher);
            seal.epoch   = session.crypto.epoch;
            return rune::node::sendRecordsFrame(*session.egress, rec, scratch_.data(), scratch_.size(),
                                                &seal, &recordsSealScratch_);
        }
        return rune::node::sendRecordsFrame(*session.egress, rec, scratch_.data(), scratch_.size());
    }
    if (!onEgressFrame_) return {rune::node::RecordsSendKind::Retry, -1};
    captureTestRecordsFrame_(session, rec);
    return {rune::node::RecordsSendKind::Sent, 1};
}

void ClientGateway::captureTestRecordsFrame_(Session& session, const pc::Records& rec)
{
    const std::size_t frameLen = rec.encodedSize();
    if (frameLen <= scratch_.size()) {
        onEgressFrame_(session.sessionId, scratch_.data(), rec.encode(scratch_.data()));
        return;
    }
    std::vector<uint8_t> frame(frameLen);   // test-only: frame larger than scratch_
    onEgressFrame_(session.sessionId, frame.data(), rec.encode(frame.data()));
}

// ---------------------------------------------------------------------------
// F19: failPushSubscriptionTooLarge_ — the next stored batch of `sub` can never
// be delivered on this session's egress publication (frame > maxMessageLength).
// Fail the subscription ONCE, explicitly, and park it AT the batch — nextOffset
// is NOT advanced, so nothing is lost silently; pushSubscriptions() skips it
// (no spin) until the client unsubscribes.  The error (F15 S3 m-f convention:
// correlationId 0, subscription named in the message) is owed until sent.
// ---------------------------------------------------------------------------
void ClientGateway::failPushSubscriptionTooLarge_(Subscription& sub, Session& session,
                                                  std::size_t frameLen)
{
    if (sub.failed) return;
    sub.failed         = true;
    sub.errorOwed      = true;
    sub.failedCause    = rune::node::PushTooLargeCause::EgressFrame;
    sub.failedFrameLen = frameLen;
    spdlog::error("ClientGateway: push sub={} (topicId={} partition={}) failed at offset={}: "
                  "Records frame {} B exceeds the egress maxMessageLength {} B — "
                  "MessageTooLarge, subscription parked (cursor not advanced)",
                  sub.subscriptionId, sub.topicId, sub.partition, sub.nextOffset, frameLen,
                  session.egress ? session.egress->maxMessageLength() : 0);
    trySendOwedPushError_(sub, session);
}

void ClientGateway::trySendOwedPushError_(Subscription& sub, Session& session)
{
    static_assert(rune::node::kPushTooLargeErrorMaxBytes <= kScratchSize);
    const std::size_t n = rune::node::encodePushTooLargeError(
        scratch_.data(), sub.sessionId, sub.subscriptionId, sub.topicId, sub.partition,
        sub.nextOffset, sub.failedFrameLen,
        session.egress ? session.egress->maxMessageLength() : 0, sub.failedCause);
    if (offerBytes_(&session, scratch_.data(), n) > 0) sub.errorOwed = false;
}

// ---------------------------------------------------------------------------
// G0.1: sendRespReliable_ — encode a ProduceResponse into scratch_ and route it
// through sendReliable_.  Returns false only on an encode error.
// ---------------------------------------------------------------------------
bool ClientGateway::sendRespReliable_(Session&          session,
                                      pc::ErrorCode     ec,
                                      int32_t           partition,
                                      int64_t           baseOffset,
                                      int64_t           correlationId,
                                      int64_t           logStartOffset)
{
    static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                  "ProduceResponse::ENCODED_SIZE exceeds kScratchSize");

    pc::ProduceResponse resp;
    resp.sessionId     = session.sessionId;
    resp.correlationId = correlationId;
    // D6: UnknownProducerId only to clients that understand it (else OOS).
    // D6 review M4: encodeFor applies the version gate (28 -> OOS) AND the
    // frame size (pre-D6 38 B below clientVersion 2) for this session.
    resp.errorCode      = static_cast<int16_t>(ec);
    resp.partition      = partition;
    resp.baseOffset     = baseOffset;
    resp.logStartOffset = logStartOffset;
    const std::size_t encoded = resp.encodeFor(scratch_.data(), session.clientVersion);
    if (encoded == 0) {
        spdlog::error("ClientGateway::sendRespReliable_: encode error ({} bytes)", encoded);
        return false;
    }

    sendReliable_(session, scratch_.data(), encoded);
    return true;
}

// ---------------------------------------------------------------------------
// G0.1: sendRespReliable — public 4-param test-seam (targets
// currentProduceSessionId_; set by handleProduce or makeForTest).
//
// Production path: look up the session and delegate to sendRespReliable_().
// Test path (testEgressFn_ set): the ring/offer logic is exercised by wrapping
// a synthetic Session whose id is currentProduceSessionId_ — offerBytes_ ignores
// the session pointer entirely when testEgressFn_ is set, so this is safe.
// ---------------------------------------------------------------------------
bool ClientGateway::sendRespReliable(rune::protocol::client::ErrorCode ec,
                                     int32_t    partition,
                                     int64_t    baseOffset,
                                     int64_t    correlationId)
{
    if (testEgressFn_ && !sessions_.find(currentProduceSessionId_)) {
        // No Session in the test harness yet — register an egress-less one under
        // currentProduceSessionId_ (offerBytes_ routes through testEgressFn_ and
        // never dereferences the publication in test mode), so the spill drain
        // finds it like any live session.
        sessions_.add(currentProduceSessionId_, /*egress=*/nullptr, rune::gateway::AuthAnonymous{}, nowNs_, {}, 0,
                      rune::protocol::client::kClientVersion);
    }

    Session* session = sessions_.find(currentProduceSessionId_);
    if (!session) {
        spdlog::debug("ClientGateway::sendRespReliable: session {} not found",
                      currentProduceSessionId_);
        return false;
    }
    return sendRespReliable_(*session, ec, partition, baseOffset, correlationId);
}

// ---------------------------------------------------------------------------
// G0.1 / fix rounds 3-4 (I1, N8): drainPendingResponses — re-offer spilled
// answers.  Walks the READY list (sessions holding spilled answers), never the
// records: per session, an O(1) window check (a session whose publication has
// no window costs one compare and is skipped), then its oldest answers are
// offered until one is refused.  Per-session order holds by construction (one
// chain per session); a stalled session never blocks another.
// Returns the number of answers delivered.
// ---------------------------------------------------------------------------
int ClientGateway::drainPendingResponses(int64_t nowNs)
{
    // Fix round 3 (I1): sessions in the EXPIRING state get their SessionExpired
    // re-offered (closed once delivered, or at the lease deadline).
    if ((spillExpiringCount_ | spillExpiringOverflow_) != 0) [[unlikely]]
        serviceSpillExpiring_(nowNs != 0 ? nowNs : nowNs_);
    if (spillReadyHead_ == nullptr) [[likely]] return 0;
    const int64_t now = nowNs != 0 ? nowNs : nowNs_;
    int delivered = 0;
    for (Session* s = spillReadyHead_; s != nullptr;) {
        Session* const next = s->spill.nextReady;
        SessionSpill& sp = s->spill;
        if (spillWritable_(*s)) {
            while (!sp.chain.empty()) {
                const std::size_t len = egressSpill_.headLen(sp.chain);
                const uint8_t* data = egressSpill_.headData(sp.chain, spillStage_.data());
                if (offerBytes_(s, data, len) <= 0) break;   // refused: the rest keeps its order
                egressSpill_.popHead(sp.chain);
                sp.bytes -= static_cast<uint32_t>(SpillPool::bytesFor(len));
                ++delivered;
            }
            if (sp.chain.empty()) {
                unlinkSpillReady_(*s);
                const bool closing = sp.state == SpillState::Closing;
                if (closing) --closingSessions_;
                sp = SessionSpill{};   // back to Normal
                // Issue #119: a Closing session's last answers are delivered - it ends now (closeSession_ removes
                // only this session; `next` stays valid).
                if (closing) (void)closeSession_(s->sessionId);
                s = next;
                continue;
            }
        }
        // Issue #119: a Closing session that cannot drain is closed at its deadline (the session lease - a resource
        // bound, not a convergence timer); closeSession_ frees the rest of its chain.
        if (sp.state == SpillState::Closing && now >= sp.closeDeadlineNs) [[unlikely]] {
            ++closingDeadlineCloses_;
            (void)closeSession_(s->sessionId);
        }
        s = next;
    }
    return delivered;
}

// ---------------------------------------------------------------------------
// doWork — poll ingress; poll pending-connects; reap expired sessions.
// Returns fragment count + pending-connect resolves.
// ---------------------------------------------------------------------------
int ClientGateway::doWork(int64_t nowNs) {
    nowNs_ = nowNs;

    // G1.1: Snapshot minIsr_ once per duty-cycle cycle so handleProduce() and
    // drainPendingAcks() use a plain int load instead of a per-record atomic load.
    // Safe: cachedMinIsr_ is only accessed on this single-writer thread.
    cachedMinIsr_ = minIsr_.load(std::memory_order_relaxed);

    // G0.1: drain any back-pressured egress frames FIRST — before ingress poll,
    // before worker returns, before everything else.  This ensures a buffered
    // produce ack is sent as soon as the publication clears back-pressure,
    // preserving the "no silent drop on back-pressure" invariant.
    drainPendingResponses(nowNs);
    if (!pendingLineageNotes_.empty()) [[unlikely]] (void)flushPendingLineageNotes_();   // round 6 (F1)
    if (!pendingCatalogUnsubs_.empty()) [[unlikely]] (void)flushPendingCatalogUnsubs_();   // catalog Task 12
    if (!sessionsWithReadEnds_.empty()) [[unlikely]] (void)flushPendingReadEnds_();         // issue #117
    // No-hang fix round 3: the no-hang counters, logged while the broker runs
    // (on change, at most every kNoHangStatsLogNs) — one compare per duty cycle.
    if (nowNs >= nextNoHangStatsLogNs_) [[unlikely]] (void)logNoHangStatsIfChanged_(nowNs);

    // E2 inc2a: drain the workers' return rings FIRST — send responses from the
    // previous cycle's produce-appends and push-reads before processing any new
    // ingress.  This keeps produce responses strictly ordered before push-Records
    // (same session, different op types) and ensures deferred acks=All are
    // checked with the latest HW from the previous cycle.
    //
    // Guard: only active when a WorkerPool is wired in.  Falls back gracefully
    // to the pre-E2 inline-execution path when workerPool_ is null.
    if (workerPool_) {
        drainWorkerReturns_();
    }

    // TX-2b: Detect Follower→Leader transition and rebuild the PSM from the
    // partition Logs.  Mirrors TransactionCoordinationService::doWork().
    //
    // leaderView_ is set only when replication is configured; when null (single-
    // node passthrough) we are always "leader" — the gateway was never a follower,
    // so no rebuild is needed on the very first doWork() call (wasLeader_ starts
    // true in the no-leaderView path to suppress the unnecessary cold rebuild).
    //
    // With leaderView_: rebuild fires exactly once on the first duty cycle after
    // the node becomes leader (wasLeader_ flips false→true). Losing leadership
    // resets wasLeader_ so the next election fires another rebuild.
    if (localLeader_ >= 0 || leaderView_) {
        // Task 11 fix 1: the runtime pushes this cycle's leadership as a value
        // (no per-cycle std::function call); gateway-only tests use leaderView_.
        const bool leader = localLeader_ >= 0 ? localLeader_ == 1 : leaderView_().isLeader;
        if (leader && !wasLeader_) {
            // Follower→Leader: rebuild PSM from all partition Logs.
            rebuildProducerStateFromLogs();
            wasLeader_ = true;
        } else if (!leader && wasLeader_) {
            // Leader→Follower: reset so the next election triggers another rebuild.
            wasLeader_ = false;
        }
    }

    // G0.3: fragment limit is configurable (default 256, was hard-coded 10).
    // Sourced from NodeConfig::gatewayFragmentLimit via setFragmentLimit().
    // Task 10 round 4: retry worker-bound ops a full ring refused earlier
    // (control markers, subscription lifecycle) BEFORE new ingress, in order.
    int work = 0;
    if (workerPool_) work += pumpSubOps_();
    work += pumpEgressReleases_();   // retire-on-ack release ops a full ring refused (never dropped)
    work += pumpTxnMarkers_();
    work += pumpTxnVerify_();   // #112: verification answers / forwards / lost-ask sweep
    if (shardMode_) {
        // D8c: ... or a Produce frame of a session with no produce credit left
        // (sessionProduceCredits_ forwarded, not yet claimed by a worker): the
        // pause engages BEFORE a worker's ack queue can fill, independent of
        // how many frames one poll / one datagram carries (admitClientFrame_).
        // Fix round 1: and a produce whose owning worker's routed share is full
        // (per-worker cap, handleProduce returns false) is ABORTed unconsumed.
        // LOW-4 (by design, Kafka-like in-flight-per-connection): the budget is
        // per SESSION, so one slow partition (e.g. acks=all parked through an
        // ISR-shrink window, bounded by the ack timeout) can use up a session's
        // credits — its aborted Produce then holds back that session's produces
        // to healthy partitions, its Metadata and its Keepalives too, until
        // those acks are answered and claimed.
        // The same head-of-line holds for a frame deferred by a full worker's
        // routed share (re-review LOW-R4): it holds back the session's frames
        // to other workers too; each redelivery refreshes its lease (only a
        // live sender's image redelivers).
        // Task 13 fix round 2 (finding 1, G-A6 on the gateway-routed path): a
        // Produce frame of a session some worker holds acks for is ABORTed — it
        // stays on ITS image (one client publication) and the poll moves on to
        // the other images; the client's offers back-pressure until it reads
        // its acks (Released).  One branch when nothing is held.
        // ABORT stops the WHOLE image: every later frame of that client —
        // Keepalive, Metadata, Disconnect, … — waits behind the aborted Produce
        // until Released, a close (SessionFailed), or the lease reap.  No
        // deadlock (releasing a held ack never needs a gateway frame).  The
        // stuck frame never drains its image, so the image is no liveness
        // signal; the lease of a paused session comes from the workers' signals
        // (reapExpired, D8b): a HELD session is reaped after maxHeldIdleNs_
        // without a Progress signal (the worker claimed acks for it, i.e. the
        // client reads); a session LOST on any worker (its ack publication
        // NOT_CONNECTED — dead client, unreachable egress, or a client that
        // stopped pumping its Aeron runtime) is reaped sessionTimeoutNs_ after
        // the Lost edge unless a Found edge clears it first.
        work += sub_->controlledPoll(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l,
                   aeron::Header& h) {
                return controlledAssembler_.onFragment(
                    [this](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                           aeron::util::index_t length, aeron::Header& header) {
                        const auto* frame = reinterpret_cast<const uint8_t*>(buffer.buffer() + offset);
                        if (!admitClientFrame_(frame, static_cast<std::size_t>(length))) [[unlikely]]
                            return aeron::ControlledPollAction::ABORT;
                        currentImageSessionId_ = header.sessionId();   // D14 (one store per frame)
                        // D8c fix round 1: a produce deferred by the per-worker cap is not
                        // consumed — ABORT it (redelivered at the next poll).
                        if (!onClientFrame_(frame, static_cast<std::size_t>(length))) [[unlikely]]
                            return aeron::ControlledPollAction::ABORT;
                        return aeron::ControlledPollAction::CONTINUE;
                    },
                    b, o, l, h);
            },
            fragmentLimit_);
        controlledImageWatch_.afterPoll(*sub_, controlledAssembler_);
        noteControlledImages_(sub_->imageCount());   // review L1: a lost image's deferral memo
    } else {
        // Task 11 fix 2 (B3): templated assembler + handler — no std::function.
        work += sub_->poll(
            [this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l,
                   aeron::Header& h) {
                assembler_.onFragment(
                    [this](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                           aeron::util::index_t length, aeron::Header& header) {
                        onFragment(buffer, offset, length, header);
                    },
                    b, o, l, h);
            },
            fragmentLimit_);
        imageWatch_.afterPoll(*sub_, assembler_);   // free buffers of lost client images (m-c)
    }
    pollPendingConnects(nowNs);
    work += advanceInlinePsmScans_();   // I3: budgeted inline producer-state rebuilds
    // Task 13: SessionClose ops a full worker ring refused earlier — never dropped.
    if (shardMode_ && workerPool_) work += sessionAnnouncer_.retryCloses(*workerPool_);
    work += servicePermissionShipping_();   // authentication Task 13: unfinished WRITE bitmaps + held login answers
    // Task 19: the per-worker crypto-key SessionAnnounce (second wave) a
    // full worker ring refused earlier — retried until every worker has the
    // real key. Never a safety concern while this is in flight (every
    // worker already has a "parked" entry from the first, announce-before-
    // accept wave — see pollPendingConnects), only a liveness one.
    if (shardMode_ && workerPool_ && !pendingCryptoAnnounces_.empty())
        work += retryPendingCryptoAnnounces_();
    pollPendingRecordEgress_(nowNs);  // hybrid egress: resolve unicast pubs + drain deferred subscribes
    reapExpired(nowNs);
    // GB-5p2: group-commit flush — time-based cadence (kGroupCommitIntervalNs).
    // Fsyncs all dirty partition Logs (appendNoSync'd since last flush) at most
    // once per interval.  Produces between flushes land in page cache at ~1-10µs.
    // Must run AFTER the ingress poll; placed before pushSubscriptions so push
    // reads fsync-durable data.  No busy-spin; no-op when interval not elapsed.
    flushDirtyLogs();
    // GR-5: drain deferred acks=All pending-ack queue.
    // Called AFTER ingress poll (appends already done for this cycle) and AFTER
    // the replica's HW may have advanced (replication doWork is driven by the
    // BrokerNode agent loop BEFORE gateway.doWork in the same duty-cycle thread,
    // so any follower FetchResponse processed this cycle already updated HW).
    // Must run BEFORE pushSubscriptions so the client gets the ProduceResponse
    // before any Records push (strictly ordered on the same egress publication).
    drainPendingAcks();
    // GB-3: push new Records to all active subscribers (egress-paced, single-writer).
    pushSubscriptions();
    return work;
}

// ---------------------------------------------------------------------------
// rebuildProducerStateFromLogs — node-level Follower→Leader edge.
//
// Called once on the first doWork() cycle after the node becomes leader.
//
// Producer state (inline path): NOT rebuilt here any more.  The former rebuild
// scanned only to the committed HW (TX-2b, to keep a never-committed tail out
// of the dedup window) — but a new leader KEEPS its tail between HW and LEO,
// so a resend of the first tail batch (seq == lastSequence + 1) was accepted
// and appended twice (a silent duplicate), and later tail batches answered OOS
// ("NOT appended" — the client then re-sends them).  It also ran only on this
// node-level edge, missing every per-partition promotion / truncation.  Now the
// partition's proof is invalidated here and proveInlinePsm_ rebuilds it from
// logStart to LEO on the next idempotent batch — and on every change of the
// replica's log / leadership generation, which is what makes the LEO bound
// safe: a later KIP-101 truncation of the tail bumps the log generation and
// forces another rebuild before any answer.
//
// read_committed state (TX-7, no worker pool only): rebuilt from [0, HW) as
// before.
//
// No-op when topicRegistry_ is null (gateway not yet wired).
// Cold path: allocations acceptable.  Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::rebuildProducerStateFromLogs()
{
    if (!topicRegistry_) return;

    const int32_t topicCount = topicRegistry_->topicCount();
    for (int32_t topicId = 0; topicId < topicCount; ++topicId) {
        const rune::node::TopicSpec* spec = topicRegistry_->topic(topicId);
        if (!spec) continue;

        for (int32_t partition = 0; partition < spec->partitionCount; ++partition) {
            rune::storage::Log* log = topicRegistry_->log(topicId, partition);
            if (!log) continue;
            // CP1-I4: a worker-routed partition is rebuilt by its worker; its log (LEO included) is never read here.
            if (routesToWorker_(topicId)) continue;

            // The read_committed rebuild below is bounded by the committed HW
            // (LEO when no replication is wired: passthrough, HW == LEO).
            int64_t scanTo = log->logEndOffset(); // fallback: passthrough/test mode
            if (replicaRegistry_) {
                rune::replication::PartitionReplica* replica =
                    replicaRegistry_->lookup(topicId, partition);
                if (replica) {
                    scanTo = replica->gatewayHighWatermark();
                }
            }

            // F15 S5 (§2.5, Q3): the owning worker rebuilds the producer state of
            // every partition it owns (its RC build) — the gateway keeps state
            // only for partitions it appends to itself (no worker pool, or an
            // internal partition in shard mode), and never reads a worker-owned
            // log from this thread.
            if (routesToWorker_(topicId)) continue;
            // PSM <-> log consistency (review of b4594f8, finding 1): the
            // inline producer state is NOT rebuilt here to HW — a new leader
            // keeps its tail between HW and LEO, and a resend of the first tail
            // batch (seq == lastSequence + 1) was then appended twice.  The
            // state is invalidated; the next idempotent batch of the partition
            // rebuilds it from logStart to LEO (proveInlinePsm_), which also
            // covers every per-partition promotion / truncation this node-level
            // edge never sees.
            if (auto it = inlinePsm_.find(inlinePsmKey_(topicId, partition)); it != inlinePsm_.end())
                setInlinePsmState_(it->second, InlinePsmUnproven{});

            // TX-7: Rebuild per-partition read_committed state (LsoTracker +
            // AbortedTxnIndex) from the same committed log range [0, scanTo).
            //
            // On a Follower→Leader transition the partitionTxnStates_ map is
            // EMPTY: it is only populated incrementally via onProduceBatch /
            // onControlMarker during normal produce handling.  An empty tracker
            // returns lso(hw) == hw, which would allow a read_committed consumer
            // to see records belonging to OPEN transactions (violating KIP-98 §8:
            // LSO must pin at the first open-txn offset).  Similarly, an empty
            // abortedIndex would deliver ABORTED data batches that are already
            // below HW in the log (spec §8: aborted batches must be filtered).
            //
            // ReadCommittedView::rebuild scans [0, hw) from the log and
            // populates both LsoTracker and AbortedTxnIndex — exactly the same
            // structures that pushSubscriptions() reads via getTxnStateRO().
            //
            // We must CLEAR any stale state first (a partition that briefly had
            // incremental state before the node lost leadership could carry
            // stale entries; a clean rebuild from the committed log is the
            // canonical source of truth).
            //
            // scanTo == hw — bounded to committed records, never seeding from
            // the uncommitted ahead-tail.
            //
            // F15 S4 (Option B): with a worker pool (every production mode) the
            // OWNING worker builds and serves the read_committed state (§2.3) —
            // the gateway holds none for those partitions.  Only the no-pool
            // inline path (unit rigs; the control thread is the single reader
            // and writer there) keeps this gateway-side state.
            if (!workerPool_) {
                PartitionTxnState& ts = getTxnState(topicId, partition);
                ts.lsoTracker.clear();
                ts.abortedIndex.clear();
                rune::transaction::ReadCommittedView view(
                    &ts.lsoTracker, &ts.abortedIndex);
                view.rebuild(log, scanTo);
            }
        }
    }

    spdlog::info("ClientGateway: inline producer state invalidated + read_committed state rebuilt "
                 "from {} topic(s) on leader transition", topicCount);
}

// ---------------------------------------------------------------------------
// proveInlinePsm_ — PSM <-> log consistency gate of the inline path (see the
// InlinePsmState comment in ClientGateway.h).  Hot path (Proven, unchanged
// facts): one hash lookup + a compare.  Otherwise at most ONE budgeted rebuild
// step (kInlinePsmStepBytes) — never an O(log) scan on the duty thread (I3).
// ---------------------------------------------------------------------------
void ClientGateway::setInlinePsmState_(InlinePsmState& slot, InlinePsmState next) noexcept
{
    if (std::holds_alternative<InlinePsmScanning>(slot)) --inlinePsmScanning_;
    if (std::holds_alternative<InlinePsmScanning>(next)) ++inlinePsmScanning_;
    slot = std::move(next);
}

ClientGateway::InlinePsmGate
ClientGateway::proveInlinePsm_(int32_t topicId, int32_t partition, rune::storage::Log& log,
                               const rune::replication::PartitionReplica* replica)
{
    const uint64_t key = inlinePsmKey_(topicId, partition);
    InlinePsmFacts facts;
    if (replica != nullptr) {
        // Only a leader's log is stable below its LEO: a follower takes
        // replicated appends / truncations (the worker path's RC gate, I2).
        if (replica->role() != rune::replication::Role::Leader) [[unlikely]] {
            if (auto it = inlinePsm_.find(key); it != inlinePsm_.end())
                setInlinePsmState_(it->second, InlinePsmUnproven{});
            return InlinePsmGate::NotLeader;
        }
        facts.incarnation = replica->incarnation();
        facts.logGen      = replica->logGeneration();
        facts.leadGen     = replica->leadershipGeneration();
    } else {
        facts.logGen = log.contentGeneration();
    }
    auto it = inlinePsm_.find(key);
    if (it == inlinePsm_.end()) [[unlikely]] it = inlinePsm_.emplace(key, InlinePsmUnproven{}).first;   // cold
    InlinePsmState& slot = it->second;
    if (const auto* p = std::get_if<InlinePsmProven>(&slot); p != nullptr && p->facts == facts) [[likely]]
        return InlinePsmGate::Proven;

    int64_t backoffNs = kInlinePsmRetryMinNs;
    if (const auto* f = std::get_if<InlinePsmFailed>(&slot)) {
        // I2: a failed proof is LATCHED — no rescan per produce.  Retried only
        // when the facts change (the log / leadership moved) or the backoff ends.
        if (f->facts == facts && nowNs_ < f->retryAtNs) return InlinePsmGate::Unprovable;
        if (f->facts == facts) backoffNs = std::min(f->backoffNs * 2, kInlinePsmRetryMaxNs);
    }
    auto* sc = std::get_if<InlinePsmScanning>(&slot);
    if (sc == nullptr || sc->facts != facts) {
        // (Re)start the build: from logStart to the LEO of NOW.
        InlinePsmScanning fresh;
        fresh.facts     = facts;
        fresh.backoffNs = backoffNs;
        producerStateManager_.beginRebuild(fresh.cursor, topicId, log, log.logEndOffset(), partition);
        ++inlinePsmRebuilds_;
        setInlinePsmState_(slot, std::move(fresh));
        sc = std::get_if<InlinePsmScanning>(&slot);
    }
    using RS = rune::storage::ProducerStateManager::Rebuild::Status;
    const RS st = producerStateManager_.rebuildStep(sc->cursor, log, kInlinePsmStepBytes);
    if (st == RS::InProgress) return InlinePsmGate::NotLeader;
    if (st == RS::Done) {
        setInlinePsmState_(slot, InlinePsmProven{facts});
        return InlinePsmGate::Proven;
    }
    const int64_t b = sc->backoffNs;
    producerStateManager_.clearPartition(topicId, partition);
    setInlinePsmState_(slot, InlinePsmFailed{facts, nowNs_ + b, b});
    spdlog::error("ClientGateway: producer-state rebuild of topicId={} partition={} stopped short of the log "
                  "end (an unreadable batch) — idempotent produces answered Internal (outcome unknown); "
                  "retried on a log change or in {} ms", topicId, partition, b / 1'000'000);
    return InlinePsmGate::Unprovable;
}

int ClientGateway::advanceInlinePsmScans_()
{
    if (inlinePsmScanning_ == 0 || topicRegistry_ == nullptr) [[likely]] return 0;
    int work = 0;
    for (auto& [key, state] : inlinePsm_) {
        if (!std::holds_alternative<InlinePsmScanning>(state)) continue;
        const auto topicId   = static_cast<int32_t>(static_cast<uint32_t>(key >> 32));
        const auto partition = static_cast<int32_t>(static_cast<uint32_t>(key));
        rune::storage::Log* log = topicRegistry_->log(topicId, partition);
        if (log == nullptr) { setInlinePsmState_(state, InlinePsmUnproven{}); continue; }
        const rune::replication::PartitionReplica* rep =
            replicaRegistry_ ? replicaRegistry_->lookup(topicId, partition) : nullptr;
        (void)proveInlinePsm_(topicId, partition, *log, rep);   // one budgeted step (no rehash: key exists)
        ++work;
    }
    return work;
}

// M2: leadershipGeneration is deliberately NOT checked — the data under a
// waiter changes only through a truncation or a reconcile append, both of which
// bump logGeneration; a leadership flip with the log untouched leaves the
// waiter's data in place (the worker path's §3.2 rule is the same).
bool ClientGateway::inlineWaiterStillValid_(const rune::replication::PartitionReplica& rep,
                                            PendingAck& entry) noexcept
{
    if (rep.role() != rune::replication::Role::Leader) return false;
    const uint64_t g = rep.logGeneration();
    if (g == entry.logGen) [[likely]] return true;
    if (g != entry.logGen + 1 || entry.lastOffset >= rep.lastTruncationOffset()) return false;
    entry.logGen = g;   // exactly one cut, strictly above the waiter: its data is untouched
    return true;
}


// ---------------------------------------------------------------------------
// reapExpired — remove sessions idle beyond sessionTimeoutNs_.
//
// Uses injected nowNs ONLY (no wall-clock).  Bounded O(n) scan.
// Two-phase: collect expired IDs into reapScratch_ (pre-allocated vector),
// then erase them — avoids iterator invalidation.
// ---------------------------------------------------------------------------
void ClientGateway::reapExpired(int64_t nowNs) {
    reapAuthDeadlines_(nowNs);   // Authentication Task 11 (D13): sessions that never logged in
    reapScratch_.clear();

    sessions_.forEach([&](Session& s) {
        // D8b: lost on any worker (ack publication NOT_CONNECTED) → the normal lease from the Lost edge (its
        // owed acks are freed in ~sessionTimeout); else held (paused by back-pressure) → maxHeldIdleNs_ since the
        // last activity / Progress signal; else the normal lease since the last activity.
        // D8c: a session paused for want of produce credit is paused like a held one (its image waits at a
        // Produce frame, Keepalives behind it, while its forwarded produces are answered / claimed).
        const bool paused  = s.heldByWorkers != 0 || s.produceInFlight >= sessionProduceCredits_;
        const bool expired = s.lostByWorkers != 0 ? nowNs - s.lostSinceNs > sessionTimeoutNs_
                           : paused               ? nowNs - s.lastSeenNs > maxHeldIdleNs_
                                                  : nowNs - s.lastSeenNs > sessionTimeoutNs_;
        if (expired) reapScratch_.push_back(s.sessionId);
    });

    // Fix round 2 (finding 4): ONE close path (closeSession_) for reap,
    // Disconnect and SessionFailed — table, workers, record-egress resolves,
    // subscriptions, group-member tracking, held state.
    for (int64_t sid : reapScratch_) {
        const Session* s = sessions_.find(sid);
        // F-A: tell the client before its publication goes (the rune client resumes at once on SessionExpired,
        // instead of at its next stale-session timeout).  Best effort: a session lost on a worker has a dead
        // egress and the offer just fails.  An EXPIRING session's notice is already owed by
        // serviceSpillExpiring_ - never a second one.
        const bool notify = s != nullptr && s->spill.state != SpillState::Expiring;
        // Review M2: on a Spilled session the notice goes out directly, AHEAD of the spilled answers closeSession_ then
        // frees - they are lost with the session either way (the client resends with the same requestId / pid+seq).
        (void)closeSession_(sid, notify);
    }
}

// ---------------------------------------------------------------------------
// onFragment — called by FragmentAssembler for each reassembled frame.
// Drop-safe: any exception or decode failure is caught and silently dropped.
// ---------------------------------------------------------------------------
void ClientGateway::onFragment(aeron::concurrent::AtomicBuffer& buffer,
                               aeron::util::index_t             offset,
                               aeron::util::index_t             length,
                               aeron::Header&                   header)
{
    currentImageSessionId_ = header.sessionId();   // D14: the Connect's source host is read off this image
    (void)onClientFrame_(reinterpret_cast<const uint8_t*>(buffer.buffer() + offset), static_cast<std::size_t>(length));
}

bool ClientGateway::onClientFrame_(const uint8_t* buf, std::size_t len)
{
    // Fix round 4 (N9): an Expiring session's frames are never executed — no
    // Produce append, DDL, commit or txn op after the broker decided the
    // session is dead.  One compare while no session is Expiring.
    // Issue #119: a Closing session's frames likewise (it was sent its last answer).
    if ((expiringSessions_ | closingSessions_) != 0) [[unlikely]] {
        if (dropExpiringFrame_(buf, len)) return true;   // consumed, not executed
    }
    // client<->broker TLS/AEAD (Task 8): decrypt BEFORE checkFrame — mirrors
    // the client's dispatchEgressFrame (Task 6). templateId/sessionId are
    // always plaintext (they form the AEAD AAD), so peek them via
    // MessageHeader::decode without validating anything else. Connect /
    // HandshakeContinue legs are exempt: they precede or complete key
    // establishment itself and must stay plaintext even on a session that is
    // otherwise `encrypted` (e.g. a stray HandshakeContinue retry delivered
    // after done() must still be readable in the clear). Plaintext path
    // (crypto off, or session not found/not encrypted) is byte-identical to
    // before Task 8 — buf/len are reassigned only on a successful decrypt.
    //
    // Cross-node sessionId collision (task-8-interfaces.md): NOT reachable
    // here. `sessions_` (SessionTable.h) is a flat unordered_map<sessionId,
    // Session> that is this NODE's OWN table — find() returns at most one
    // Session by construction of the map (a plain hash-map key lookup), and
    // SessionTable::nextSessionId() ORs in this node's own idPrefix_, so ids
    // are cluster-unique by construction in production. This differs from
    // the client's own ambiguity (ClientAgent::dispatchEgressFrame): the
    // client holds one Session PER NODE in one local collection, so the same
    // numeric sessionId can legitimately be claimed by two different nodes'
    // Sessions in that single collection, and the client must therefore try
    // every encrypted candidate. The gateway never holds another node's
    // sessions at all — a single find() + single open() is correct here, no
    // multi-candidate AEAD-disambiguation loop is needed.
    // Lows batch C (D8c R3): a Produce's seq is committed to the replay window
    // only once the frame is CONSUMED — handleProduce may defer it (full worker
    // share: ABORT + redelivery), and a committed seq would make the redelivery
    // a "replay" that is silently dropped.  Every other frame is always consumed.
    int64_t  produceOpenSid = 0;
    uint64_t produceOpenSeq = 0;
    if (const auto hdrOpt = pc::MessageHeader::decode(buf, len)) {
        const auto peekedTid = static_cast<pc::TemplateId>(hdrOpt->templateId);
        if (peekedTid != pc::TemplateId::Connect &&
            peekedTid != pc::TemplateId::HandshakeContinue) {
            if (Session* s = sessions_.find(hdrOpt->sessionId); s != nullptr && s->crypto.encrypted) {
                // Plaintext is always kCryptoOverhead (28) bytes SHORTER than
                // the wire frame — `len` bytes of capacity is always enough.
                // noexcept-adjacent hot path: vector::resize can throw
                // (bad_alloc) — contained here so a failed grow drops the
                // frame instead of propagating out of onFragment's callback.
                if (decryptScratch_.size() < len) {
                    try {
                        decryptScratch_.resize(len);
                    } catch (...) {
                        return true;  // scratch exhausted — drop, never crash
                    }
                }
                // Issue #116: the gateway is receiver 0.  A receiver-key session's frame naming another receiver (a
                // frame sealed for a shard worker, replayed here) is dropped before any AEAD and commits nothing.
                if (s->crypto.receiverKeys && len >= pc::kCryptoHeaderBytes) {
                    const auto sub = pc::CryptoSubHeader::decode(buf + pc::MessageHeader::ENCODED_SIZE,
                                                                 len - pc::MessageHeader::ENCODED_SIZE);
                    if (sub && sub->senderId != 0) [[unlikely]] {
                        if (c2bWrongReceiverDrops_++ % 1024 == 0)
                            spdlog::warn("ClientGateway: encrypted frame for receiver {} (not the gateway) on "
                                         "session {}; dropped (total {})", sub->senderId, hdrOpt->sessionId,
                                         c2bWrongReceiverDrops_);
                        return true;
                    }
                }
                uint64_t seq = 0;
                const std::size_t plainLen = rune::gateway::codec::openIngressFrameUncommitted(
                    s->crypto, buf, len, decryptScratch_.data(), decryptScratch_.size(), seq);
                if (plainLen == 0) return true;  // tamper/replay/truncation — drop
                if (peekedTid == pc::TemplateId::Produce) { produceOpenSid = hdrOpt->sessionId; produceOpenSeq = seq; }
                else rune::gateway::codec::commitIngressSeq(s->crypto, seq);
                buf = decryptScratch_.data();
                len = plainLen;
            }
        }
    }

    // Step 1: validate frame with checkFrame — reject anything non-conformant.
    const auto fc = pc::checkFrame(buf, len);
    if (fc.status != pc::FrameStatus::Ok) [[unlikely]] {
        // Never crashes the gateway.  No-hang (R2): a frame whose header decodes
        // on a known session (wrong schema / newer version / unknown template —
        // a newer or foreign client) is answered ErrorResponse{ProtocolVersion}
        // on its correlationId instead of leaving the client waiting.
        const auto hdr = pc::MessageHeader::decode(buf, len);
        Session* s = hdr ? sessions_.find(hdr->sessionId) : nullptr;
        // Fix round 2 (I6): a known request template is answered with ITS OWN
        // response type (the client completes that op; an ErrorResponse only
        // completes control ops); ErrorResponse only when the template has none.
        if (s != nullptr && answerRejection_(*s, *hdr, static_cast<uint16_t>(pc::ErrorCode::ProtocolVersion),
                                             "frame failed checkFrame", buf, len)) {
            // answered
        } else if (s != nullptr) {
            pc::ErrorResponse er;
            er.sessionId     = hdr->sessionId;
            er.correlationId = hdr->correlationId;
            er.code          = static_cast<int16_t>(pc::ErrorCode::ProtocolVersion);
            const std::size_t n = er.encode(scratch_.data());
            ++rejectedRequests_;
            sendAnswer_(*s, scratch_.data(), n);
        } else {
            noteUnanswerable_(buf, len, "frame failed checkFrame");
        }
        return true;
    }

    // Step 2: dispatch on templateId.
    const auto tid = static_cast<pc::TemplateId>(fc.templateId);

    // F23: every client->broker request refreshes its session's liveness, not
    // only Keepalive/Ping (Kafka: a connection is idle only when it sends no
    // requests).  Produce refreshes inside handleProduce (it already looks the
    // session up); Connect has no session yet; Keepalive/Ping touch in their
    // handlers.  Cold request types pay one session lookup here.
    switch (tid) {
        case pc::TemplateId::MetadataRequest:
        case pc::TemplateId::Subscribe:
        case pc::TemplateId::Unsubscribe:
        case pc::TemplateId::JoinGroup:
        case pc::TemplateId::SyncGroup:
        case pc::TemplateId::Heartbeat:
        case pc::TemplateId::LeaveGroup:
        case pc::TemplateId::OffsetCommit:
        case pc::TemplateId::OffsetFetch:
        case pc::TemplateId::InitProducerId:
        case pc::TemplateId::AddPartitionsToTxn:
        case pc::TemplateId::AddOffsetsToTxn:
        case pc::TemplateId::TxnOffsetCommit:
        case pc::TemplateId::EndTxn:
        case pc::TemplateId::DefineTopic:
        case pc::TemplateId::GetTopicSchema:
        case pc::TemplateId::AlterTopic:
        case pc::TemplateId::CreateView:
        case pc::TemplateId::DropTopic:
        case pc::TemplateId::AlterTopicConfig:
        case pc::TemplateId::DropView:
        case pc::TemplateId::AlterView:
        case pc::TemplateId::SecurityDdl:
        case pc::TemplateId::SecurityBootstrap:
        case pc::TemplateId::NamespaceDdl:
        case pc::TemplateId::ListNamespaces:
        case pc::TemplateId::AuthClientFirst:
        case pc::TemplateId::AuthClientFinal:
        case pc::TemplateId::GetViewRow:
            if (const auto hdr = pc::MessageHeader::decode(buf, len)) {
                sessions_.touch(hdr->sessionId, nowNs_);
            }
            break;
        default:
            break;
    }

    // Authentication Task 10 (D13 + the bootstrap-window ruling): while the broker requires authentication an
    // unauthenticated session may only keep itself alive - and, in the bootstrap window, send SecurityBootstrap.
    if (authMode_ == rune::gateway::GatewayAuthMode::Required && authGateRefuses_(tid, buf, len)) return true;

    // Namespaces Phase 2 Task 3 (D21): a suspended namespace refuses what its mode says.  Produce is gated inside
    // handleProduce on the session it already looked up; every other templates' check is one constexpr array load.
    if (rune::gateway::suspendGateChecked(static_cast<uint16_t>(tid)) && tid != pc::TemplateId::Produce &&
        nsGateRefuses_(static_cast<uint16_t>(tid), buf, len))
        return true;

    switch (tid) {
        case pc::TemplateId::Connect:
            handleConnect(buf, len, nowNs_);
            break;
        // client<->broker TLS/AEAD (Task 7): a later leg of a multi-round TLS
        // handshake started on the Connect/ConnectResponse leg above.
        case pc::TemplateId::HandshakeContinue:
            handleHandshakeContinue_(buf, len, nowNs_);
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
        // MD1 Task 10: Metadata request — answerable by any node.
        case pc::TemplateId::MetadataRequest:
            handleMetadata(buf, len);
            break;
        case pc::TemplateId::Produce: {
            const bool consumed = handleProduce(buf, len);
            if (produceOpenSid != 0 && consumed) [[unlikely]]
                if (Session* s = sessions_.find(produceOpenSid)) rune::gateway::codec::commitIngressSeq(s->crypto, produceOpenSeq);
            return consumed;
        }
        case pc::TemplateId::Subscribe:
            handleSubscribe(buf, len);
            break;
        case pc::TemplateId::Unsubscribe:
            handleUnsubscribe(buf, len);
            break;
        // CG-3: consumer-group range (32–43); Assign (44) is broker→client, drop.
        case pc::TemplateId::JoinGroup:
        case pc::TemplateId::SyncGroup:
        case pc::TemplateId::Heartbeat:
        case pc::TemplateId::LeaveGroup:
        case pc::TemplateId::OffsetCommit:
        case pc::TemplateId::OffsetFetch:
            handleGroupMessage(buf, len, nowNs_, fc.templateId);
            break;
        // TX-3: txn request range (45,47,49,51,53); responses (46,48,50,52,54) are broker→client.
        case pc::TemplateId::InitProducerId:
        case pc::TemplateId::AddPartitionsToTxn:
        case pc::TemplateId::AddOffsetsToTxn:
        case pc::TemplateId::TxnOffsetCommit:
        case pc::TemplateId::EndTxn:
            handleTxnMessage(buf, len, nowNs_, fc.templateId);
            break;
        // Task 8a: schema DDL — route to the admin sink (ring hand-off).
        case pc::TemplateId::DefineTopic:
            handleDefineTopic(buf, len);
            break;
        case pc::TemplateId::GetTopicSchema:
            handleGetTopicSchema(buf, len);
            break;
        case pc::TemplateId::AlterTopic:
            handleAlterTopic(buf, len);
            break;
        case pc::TemplateId::CreateView:
            handleCreateView(buf, len);
            break;
        // Catalog history Task 9: the catalog DDL ops (74–77).
        case pc::TemplateId::DropTopic:
            handleDropTopic(buf, len);
            break;
        case pc::TemplateId::AlterTopicConfig:
            handleAlterTopicConfig(buf, len);
            break;
        case pc::TemplateId::DropView:
            handleDropView(buf, len);
            break;
        case pc::TemplateId::AlterView:
            handleAlterView(buf, len);
            break;
        // Authentication Task 11: the SCRAM login (12 / 14); 13 / 15 are broker->client.
        case pc::TemplateId::AuthClientFirst:
            handleAuthClientFirst_(buf, len);
            break;
        case pc::TemplateId::AuthClientFinal:
            handleAuthClientFinal_(buf, len);
            break;
        // Authentication Task 10: security DDL (79) and the one-time bootstrap (80).
        case pc::TemplateId::SecurityDdl:
            handleSecurityDdl(buf, len);
            break;
        case pc::TemplateId::SecurityBootstrap:
            handleSecurityBootstrap(buf, len);
            break;
        // Namespaces Task 7: namespace DDL (81, catalog-routed) and ListNamespaces (82, any broker).
        case pc::TemplateId::NamespaceDdl:
            handleNamespaceDdl(buf, len);
            break;
        case pc::TemplateId::ListNamespaces:
            handleListNamespaces(buf, len);
            break;
        // Task 20: point read of a materialized view row — routed to the owning
        // worker via the same op/return plumbing as PushRead.
        case pc::TemplateId::GetViewRow:
            handleGetViewRow(buf, len);
            break;
        default:
            // ConnectResponse, ErrorResponse, Pong, MetadataResponse (broker→client),
            // ProduceResponse, SubscribeResponse, Records, Assign,
            // InitProducerIdResponse, AddPartitionsToTxnResponse,
            // AddOffsetsToTxnResponse, TxnOffsetCommitResponse, EndTxnResponse
            // — server→client or other direction; drop.
            break;
    }
    return true;
}

// ---------------------------------------------------------------------------
// handleConnect (BR-7a async path)
//
// BR-5 leader-awareness:
//   - Query leaderView_ (if set).
//   - If NOT leader AND leaderClientEndpoint non-empty → send ErrorResponse
//     {code=Redirect, message=leaderClientEndpoint} — over the same async
//     pending-connect machinery (DR-BUG-2; no synchronous resolve exists).
//   - If leader (or hook unset/leader-unknown) → ASYNC: register
//     addExclusivePublication and enqueue a PendingConnect.  The publication
//     is polled across doWork() cycles via pollPendingConnects().
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// evictOldestPendingConnect_ — the kMaxPendingConnects cap.
// The evicted registration must be released: attempt a final
// findExclusivePublication — see handleConnect.
// ---------------------------------------------------------------------------
void ClientGateway::evictOldestPendingConnect_(std::string_view incomingHost)
{
    // PR #611 review (issue #236's lockout through the pending table): the victim is the OLDEST entry of the source
    // host holding the most entries, counting the incoming Connect for its host - so a host flooding Connects only
    // ever evicts its own, and another host's pending Connect is evicted only when every host holds as many.  Hosts
    // are known under GatewayAuthMode::Required only; otherwise every entry has the same (empty) host and this is the
    // plain oldest-first eviction.  O(kMaxPendingConnects^2) string compares, no allocation: cold (one Connect).
    auto victim = pendingConnects_.begin();
    std::size_t most = 0;
    for (auto it = pendingConnects_.begin(); it != pendingConnects_.end(); ++it) {
        std::size_t n = it->sourceHost == incomingHost ? 1 : 0;
        for (const PendingConnect& p : pendingConnects_) n += p.sourceHost == it->sourceHost ? 1 : 0;
        if (n > most) { most = n; victim = it; }   // strict: the oldest entry of the largest host wins a tie
    }
    spdlog::warn("ClientGateway: pending-connect cap reached; dropping the oldest entry of the busiest source host");
    PendingConnect& ev0 = *victim;
    const int64_t evictedRegId      = ev0.registrationId;
    // DR-BUG-2 FIX: if the evicted entry has a stashed resolvedPub, the
    // shared_ptr destructor (run by erase()) is sufficient to release the
    // registration.  Do NOT call findExclusivePublication again — that would
    // touch a conductor entry that is already being released, and if it
    // returns nullptr we would push a stale regId to drainingRegistrations_
    // that never drains.  Only push to drain when resolvedPub==nullptr (pub
    // not yet resolved — the conductor still holds it in AWAITING state).
    const bool evictedAlreadyResolved = (ev0.resolvedPub != nullptr) ||
                                        ev0.awaitingResolve;   // nothing registered
    // Fix round 3 (fix-on-sight): an evicted connect whose reply publication is
    // resolved is ANSWERED (ConnectResponse{Internal}, no session) — one offer:
    // the eviction exists to bound this table, so it is not retried (counted).
    // Fix round 5 (N17): answer the leg the client is WAITING for — a client in
    // Handshaking ignores a ConnectResponse; it gets HandshakeContinueResponse:
    // the owed HandshakeFailed for a failing handshake, Internal (retriable)
    // for one still in progress.
    if (const auto& ev = ev0; ev.resolvedPub) {
        std::size_t n = 0;
        if (ev.failingHandshake || ev.awaitingHandshake) {
            pc::HandshakeContinueResponse hr;
            hr.sessionId     = ev.sessionId;
            hr.correlationId = ev.failingHandshake ? ev.handshakeFailCorr : ev.correlationId;
            hr.code          = static_cast<int16_t>(ev.failingHandshake ? pc::ErrorCode::HandshakeFailed
                                                                        : pc::ErrorCode::Internal);
            n = hr.encode(scratch_.data());
        } else {
            pc::ConnectResponse resp;
            resp.sessionId     = 0;
            resp.correlationId = ev.correlationId;
            resp.code          = static_cast<int16_t>(pc::ErrorCode::Internal);
            n = resp.encode(scratch_.data());
        }
        if (!offerOnPub_(*ev.resolvedPub, scratch_.data(), n)) ++unanswerableFrames_;
    } else {
        ++unanswerableFrames_;   // no reply publication yet: nothing to answer on
    }
    // Fix round 1 (I2): an evicted entry whose announcement began holds
    // worker ack-table slots — close it on every worker (never leaked).
    closeDroppedPendingConnect_(ev0);
    pendingConnects_.erase(victim);   // drops resolvedPub shared_ptr if non-null
    if (!evictedAlreadyResolved && aeron_) abandonRegistration_(evictedRegId);
    // If evictedAlreadyResolved: the drop of resolvedPub in erase() already
    // releases the registration — no findExclusivePublication needed, no drain entry.
}

// ---------------------------------------------------------------------------
// Task 13 fix round 1 — shard-mode session lifecycle helpers.
// ---------------------------------------------------------------------------
std::size_t ClientGateway::shardSessionSlotsInUse_() const noexcept
{
    std::size_t begun = 0;
    for (const auto& p : pendingConnects_) begun += p.announceBegun ? 1 : 0;   // <= kMaxPendingConnects
    return sessions_.size() + sessionAnnouncer_.pendingCloses() + begun;
}

void ClientGateway::closeDroppedPendingConnect_(const PendingConnect& pending)
{
    if (shardMode_ && workerPool_ && pending.announceBegun)
        sessionAnnouncer_.close(*workerPool_, pending.sessionId);
}

// ---------------------------------------------------------------------------
// Task 19 (per-worker egress crypto addendum): retry the second-wave,
// crypto-key-bearing SessionAnnounce for sessions a full worker ring
// refused on an earlier attempt (handleHandshakeContinue_'s best-effort
// first try, or a previous call to this function). Re-reads session.crypto
// fresh each attempt (the Session, not the long-gone PendingConnect, is the
// only thing that outlives the original attempt). A session already closed
// (closeSession_/reapExpired) in the meantime is simply dropped from the
// list — SessionClose will have already reached every worker via the
// normal close path, so there is nothing left to announce.
// ---------------------------------------------------------------------------
int ClientGateway::retryPendingCryptoAnnounces_()
{
    if (!workerPool_) return 0;
    int work = 0;
    for (std::size_t i = 0; i < pendingCryptoAnnounces_.size(); ) {
        const int64_t sid = pendingCryptoAnnounces_[i];
        Session* s = sessions_.find(sid);
        if (!s || !s->crypto.encrypted) {
            // Closed/expired, or (defensively) somehow not encrypted anymore
            // — nothing to retry.
            pendingCryptoAnnounces_[i] = pendingCryptoAnnounces_.back();
            pendingCryptoAnnounces_.pop_back();
            continue;
        }
        if (sessionAnnouncer_.announce(*workerPool_, sid, s->egressChannel, s->egressStreamId,
                                        s->clientVersion, /*sessionEncrypted=*/true, /*keyReady=*/true,
                                        s->crypto.b2c, s->crypto.b2cPrefix, s->crypto.cipher,
                                        s->crypto.c2b, s->crypto.c2bPrefix)) {
            ++work;
            pendingCryptoAnnounces_[i] = pendingCryptoAnnounces_.back();
            pendingCryptoAnnounces_.pop_back();
            continue;
        }
        ++i;   // still full on at least one worker — retry next cycle
    }
    return work;
}

bool ClientGateway::closeSession_(int64_t sid, bool notifyExpired)
{
    Session* s = sessions_.find(sid);
    if (!s) return false;                     // m4: unknown → no worker traffic
    // Issue #132: a first leg answered (a salt revealed) and never followed by a final is a failed attempt - whatever
    // ends the session (the auth deadline, a disconnect, an eviction).  Charged once: a final leg sets finalLegSeen.
    if (auto* awaitingFinal = std::get_if<rune::gateway::AuthAwaitingFinal>(&s->auth);
        awaitingFinal != nullptr && !awaitingFinal->finalLegSeen) [[unlikely]] {
        awaitingFinal->finalLegSeen = true;
        ++loginsAbandoned_;
        loginThrottle_.onFailure(awaitingFinal->user, s->sourceHost, nowNs_);
    }
    // Fix round 2 (finding 2): tell the client before its publication goes —
    // the rune client reconnects only on SessionExpired / UnknownSession.
    if (notifyExpired) sendSessionExpired_(nullptr, s, sid);
    // Fix round 4 (N16): its spilled answers die with it (O(1)) and it leaves
    // the ready list; an Expiring one leaves the exact Expiring count (its
    // table row is dropped by serviceSpillExpiring_, counted notify-lost
    // unless this close follows its delivered SessionExpired).
    if (s->spill.state == SpillState::Spilled) {
        (void)egressSpill_.freeAll(s->spill.chain);
        unlinkSpillReady_(*s);
    } else if (s->spill.state == SpillState::Closing) {
        // Issue #119: closed at its deadline, on a Disconnect or by the reaper (a drained one is Normal again by
        // then): whatever is still spilled dies with it.
        (void)egressSpill_.freeAll(s->spill.chain);
        unlinkSpillReady_(*s);
        --closingSessions_;
    } else if (s->spill.state == SpillState::Expiring) {
        --expiringSessions_;
    }
    s->spill = SessionSpill{};
    // Namespaces Phase 2 Task 10 (D24, #236): the ONE place a session's connection counts are given back - every close
    // path (client Disconnect, idle timeout, revoke, suspend RejectAll, delete, shutdown) ends in closeSession_.
    releaseConnection_(*s);
    if (s->anonCounted) { s->anonCounted = false; if (anonSessions_ > 0) --anonSessions_; }
    if (s->heldByWorkers != 0) --heldSessions_;
    if (s->produceInFlight >= sessionProduceCredits_) --creditBlockedSessions_;   // D8c: its budget dies with it
    if (s->capDeferredOn >= 0) --capDeferredSessions_;                              // R3: its deferral memo too
    for (int w = 0; w < Session::kMaxWorkers; ++w) {                               // ... and its share of each worker's cap
        if (s->produceInFlightOn[static_cast<std::size_t>(w)] == 0) continue;
        routedInFlight_[static_cast<std::size_t>(w)] -= s->produceInFlightOn[static_cast<std::size_t>(w)];
        noteWorkerShareFreed_(w);
    }
    // Record-egress lifetime (retire-on-ack, 2026-10-05): a worker may still
    // hold (and push through) the raw pointer of each per-worker record-egress
    // pub — destroying it with the session was a cross-thread use-after-free
    // (garbage maxMessageLength 0 → OutOfBounds / SIGSEGV in the push loop).
    // Each pub is retired: kept alive until its worker acknowledges the release.
    for (auto& [w, pub] : s->recordEgressByWorker)
        if (pub) retireRecordEgress_(sid, w, std::move(pub));
    sessions_.remove(sid);                    // releases its (gateway-written) egress publication
    // Task 13: the workers forget it too (its queued acks drop, its
    // publications are released) — retried until every worker has it.
    if (shardMode_ && workerPool_) sessionAnnouncer_.close(*workerPool_, sid);
    for (std::size_t i = 0; i < heldLoginFinals_.size(); ++i)
        if (heldLoginFinals_[i].sessionId == sid) { heldLoginFinals_.erase(heldLoginFinals_.begin() + static_cast<std::ptrdiff_t>(i)); break; }
    // Hybrid egress: drop any in-flight record-egress resolves for this session
    // (all workers — its buffered subscribes die with the session).
    for (auto pit = pendingRecordEgress_.begin(); pit != pendingRecordEgress_.end(); ) {
        if (pit->first.first != sid) { ++pit; continue; }
        abandonRegistration_(pit->second.regId);   // a registration still in flight is released, not leaked
        pit = pendingRecordEgress_.erase(pit);
    }

    dropSessionSubscriptions_(sid);
    // Catalog history Task 12: its catalog subscriptions (admin-owned) end too.
    forwardCatalogUnsubscribe_(sid, -1);

    // CG-4 fix (#2): erase the group-member tracking entry so groupMemberSubs_
    // does not grow unboundedly under connect/disconnect churn.
    groupMemberSubs_.erase(sid);
    return true;
}

// GB-3: drop all subscriptions belonging to this session — and the owning
// workers' cursors too (2026-10-05: they used to outlive the session and push
// forever; queued, never dropped, ordered behind queued Subscribes).  Called on
// close, and (fix round 4, N9) once an Expiring session is first serviced —
// never from inside a subscriptions_ walk.
// ---------------------------------------------------------------------------
// Issue #119: closeAfterAnswers_ - end a session that has just been sent its
// LAST answer(s) (a revoke's 46s, a failed / unavailable login's 44 / 48, the
// auth gate's 44, the stale-security AuthUnavailable).  Under egress
// back-pressure those answers were spilled, and an immediate closeSession_
// freed them: the client saw the session vanish, never got its 46 / 44 and
// lost retryAfterMs.  A session with answers still spilled enters the explicit
// Closing state instead: its chain drains in order (drainPendingResponses),
// nothing new is accepted (sendReliable_ refuses, the ingress gate drops), and
// it is closed once the chain is empty or at closeDeadlineNs.
// ---------------------------------------------------------------------------
void ClientGateway::closeAfterAnswers_(int64_t sid)
{
    Session* s = sessions_.find(sid);
    if (s == nullptr) return;
    switch (s->spill.state) {
    case SpillState::Normal:     // everything was delivered
        (void)closeSession_(sid);
        return;
    case SpillState::Expiring:   // exactly as before: the expiry path owns the notice
        (void)closeSession_(sid);
        return;
    case SpillState::Closing:    // idempotent
        return;
    case SpillState::Spilled:
        s->spill.state           = SpillState::Closing;   // chain, bytes and ready-list links carry over
        s->spill.closeDeadlineNs = nowNs_ + sessionTimeoutNs_;   // the lease bound expireForSpill_ uses
        ++closingSessions_;
        dropSessionSubscriptions_(sid);   // no Records compete with the owed answers
        return;
    }
}

void ClientGateway::dropSessionSubscriptions_(int64_t sid)
{
    const bool workerSubs = perWorkerEgressActive_ && workerPool_;
    auto it = subscriptions_.begin();
    while (it != subscriptions_.end()) {
        if (it->second.sessionId == sid) {
            if (workerSubs && it->second.topicId >= 0)
                forwardUnsubscribeToWorker_(it->first, it->second.topicId, it->second.partition);
            it = subscriptions_.erase(it);
        } else {
            ++it;
        }
    }
}

ClientGateway::ProducePause ClientGateway::produceFramePause_(const uint8_t* buf, std::size_t len, int32_t* capWorker)
{
    if (len < pc::MessageHeader::ENCODED_SIZE) return ProducePause::None;
    const uint16_t templateId = static_cast<uint16_t>(buf[2] | (buf[3] << 8));
    if (templateId != pc::Produce::TEMPLATE_ID) return ProducePause::None;   // only produce intake is paused
    uint64_t sid = 0;
    for (int i = 7; i >= 0; --i) sid = (sid << 8) | buf[8 + i];
    Session* s = sessions_.find(static_cast<int64_t>(sid));
    if (!s) return ProducePause::None;
    if (s->heldByWorkers != 0) return ProducePause::Held;
    if (s->produceInFlight >= sessionProduceCredits_) return ProducePause::NoCredit;
    if (s->capDeferredOn >= 0) [[unlikely]] {
        // Lows batch C (D8c R3): this session's head Produce was deferred by a
        // full worker share (one image per session: the redelivered frame IS
        // it).  While that worker still refuses it, ABORT here — before the
        // decrypt, decode and lookups handleProduce would redo every cycle.
        const int32_t w = s->capDeferredOn;
        const WorkerAdmit a = workerAdmitsProduce_(w, *s);
        if (a != WorkerAdmit::Admit) {
            if (capWorker) *capWorker = w;
            return a == WorkerAdmit::Full ? ProducePause::WorkerCapFull : ProducePause::WorkerCapBand;
        }
        clearCapDeferred_(*s);                                    // admitted now: the full path runs
    }
    return ProducePause::None;
}

bool ClientGateway::logNoHangStatsIfChanged_(int64_t nowNs)
{
    nextNoHangStatsLogNs_ = nowNs + kNoHangStatsLogNs;
    const NoHangStats nh = noHangStats();
    // Fix round 4 (N11): counters compared one by one (no signature that can
    // cancel out).
    if (!noHangCountersDiffer(nh, lastNoHangStats_)) return false;
    lastNoHangStats_ = nh;
    spdlog::info("ClientGateway: no-hang stats — rejected (answered with an error)={} unanswerable={} "
                 "spillExpiries={} (SessionExpired delivered={} lost={} expiring now={}) spillExpiryDrops={} "
                 "expiringIngressDrops={} clientLeftWhileExpiring={} oldClientUnknownMember={} (session-wide={}) "
                 "spilledNow={} ({} B)", nh.rejectedRequests, nh.unanswerableFrames,
                 nh.spillExpiries, nh.spillExpiryNotified, nh.spillExpiryNotifyLost, nh.spillExpiring,
                 nh.spillExpiryDrops, nh.expiringIngressDrops, nh.spillExpiryClientLeft, nh.oldClientUnknownMember,
                 nh.oldClientSessionWideEvictions, nh.spilledAnswers, nh.spillUsedBytes);
    return true;
}

void ClientGateway::logProducePauseStats() const
{
    if (const NoHangStats nh = noHangStats(); noHangCountersDiffer(nh, NoHangStats{}) || nh.spilledAnswers != 0) {
        spdlog::warn("ClientGateway: no-hang stats — rejected (answered with an error)={} unanswerable={} "
                     "spillExpiries={} (SessionExpired delivered={} lost={}) spillExpiryDrops={} "
                     "expiringIngressDrops={} clientLeftWhileExpiring={} oldClientUnknownMember={} "
                     "(session-wide={}) spilledNow={} ({} B)",
                     nh.rejectedRequests, nh.unanswerableFrames, nh.spillExpiries, nh.spillExpiryNotified,
                     nh.spillExpiryNotifyLost, nh.spillExpiryDrops, nh.expiringIngressDrops,
                     nh.spillExpiryClientLeft, nh.oldClientUnknownMember, nh.oldClientSessionWideEvictions,
                     nh.spilledAnswers, nh.spillUsedBytes);
    }
    if (heldProduceAborts_ == 0 && produceCreditUnderflows_ == 0) return;
    spdlog::warn("ClientGateway: produce pauses — aborts={} (credit={} workerCap={}) creditUnderflows={}",
                 heldProduceAborts_, creditProduceAborts_, workerCapProduceAborts_, produceCreditUnderflows_);
}

bool ClientGateway::admitClientFrame_(const uint8_t* buf, std::size_t len)
{
    if ((heldSessions_ | creditBlockedSessions_ | capDeferredSessions_ | expiringSessions_ | closingSessions_) == 0) [[likely]]
        return true;
    // Fix round 4/5 (N9 / N20): a frame of an Expiring session is checked FIRST
    // and CONSUMED (admitted; onClientFrame_ drops it, counted) — never
    // ABORTed by a credit / cap pause it can no longer clear.
    // Issue #119: a Closing session's frame likewise (onClientFrame_ drops it).
    if ((expiringSessions_ | closingSessions_) != 0 && len >= pc::MessageHeader::ENCODED_SIZE) [[unlikely]] {
        uint64_t sid = 0;
        for (int i = 7; i >= 0; --i) sid = (sid << 8) | buf[8 + i];
        if (const Session* s = sessions_.find(static_cast<int64_t>(sid));
            s != nullptr && (s->spill.state == SpillState::Expiring || s->spill.state == SpillState::Closing))
            return true;
        if ((heldSessions_ | creditBlockedSessions_ | capDeferredSessions_) == 0) return true;
    }
    int32_t capWorker = -1;
    const ProducePause p = produceFramePause_(buf, len, &capWorker);
    if (p == ProducePause::None) return true;
    ++heldProduceAborts_;
    if (p == ProducePause::NoCredit) ++creditProduceAborts_;
    else if (p == ProducePause::WorkerCapFull || p == ProducePause::WorkerCapBand) {
        // Exactly what handleProduce's deferral counts (expiry pacing unchanged).
        ++workerCapProduceAborts_;
        ++capDeferMemoAborts_;
        if (p == ProducePause::WorkerCapFull) onWorkerCapFull_(capWorker);   // may expire this very session
    }
    return false;
}

void ClientGateway::clearCapDeferred_(Session& s) noexcept
{
    s.capDeferredOn = -1;
    --capDeferredSessions_;
}

void ClientGateway::noteControlledImages_(int images) noexcept
{
    // A memo is only a fast-path hint: a frame that does come back runs the
    // full check (handleProduce) and is re-memoized while still refused.
    if (images < lastControlledImages_ && capDeferredSessions_ != 0) [[unlikely]] {
        sessions_.forEach([this](Session& s) {
            if (s.capDeferredOn >= 0) clearCapDeferred_(s);
        });
    }
    lastControlledImages_ = images;
}

// D8c: credits are counted only in shard mode (worker-side acks — the only mode
// whose workers return them); the forward site guards on shardMode_.
void ClientGateway::takeProduceCredit_(Session& s, int32_t worker) noexcept
{
    const auto w = static_cast<std::size_t>(worker);
    ++s.produceInFlightOn[w];
    ++routedInFlight_[w];
    if (++s.produceInFlight == sessionProduceCredits_) ++creditBlockedSessions_;
}

ClientGateway::WorkerAdmit ClientGateway::workerAdmitsProduce_(int32_t worker, const Session& s) const noexcept
{
    const auto w = static_cast<std::size_t>(worker);
    const uint32_t used = routedInFlight_[w];
    if (used >= workerRoutedCap_) return WorkerAdmit::Full;          // hard: the worker's routed share is full
    // Congested band (the cap's last quarter): only light sessions — a stalled
    // session (at its whole budget) is the one that runs out first.
    if (used < workerRoutedCap_ - workerRoutedCap_ / 4 || s.produceInFlightOn[w] < kLightSessionInFlight)
        return WorkerAdmit::Admit;
    return WorkerAdmit::Band;
}

// D8c fix round 2 (re-review MEDIUM-R1).  Stalled sessions can still fill one
// worker's routed share (about 12 at the full budget + 64 "light" ones that
// stall at 63): every gateway-routed produce to it would then wait for their
// reap (maxHeldIdleNs_, 10 min).  So when the hard cap defers a frame, the
// gateway EXPIRES the session that is HELD by that worker (its acks are queued
// and its publication refuses them — it is not reading) with the largest
// share on that worker: closeSession_ + SessionExpired (the client
// reconnects; its unacked produces are resent — idempotent producers dedupe),
// which frees that share at once.
//   * Never a session that is not Held by that worker: a busy reader whose
//     acks drain is never evicted.
//   * Never a session that returned a Credit from that worker during the
//     current scan window — the last kCapEvictAfterDeferredPolls deferred
//     polls (it is reading, just slowly; a fresh window tag after each scan
//     that finds no victim, as D8d's direct episodes) — and only after
//     kCapEvictAfterDeferredPolls deferred polls in the episode, so such a
//     reader has had its chance to show it.
//   * No cascade / thrash: at most ONE expiry per episode.  An episode starts
//     when the share is full and a frame is deferred; it ends with its expiry,
//     or when the share drops below the congested band (noteWorkerShareFreed_
//     — credits inside the band keep it, so reader marks made in it persist).
//     So every further expiry needs the share refilled to the cap by new
//     forwards and 64 more deferred polls of a NEW episode: expiries are paced
//     by fresh demand, each frees a whole stalled share, and K stalled
//     sessions need at most K.
//   * Event-driven (deferred polls, credits, closes), no timer.
// The scan over sessions is cold: at most once per kCapEvictAfterDeferredPolls
// deferred polls of a full worker.
// Tags (uint16, global) are issued per episode AND per scan window (lows batch
// C), so they wrap after 65535 WINDOWS — about 4M deferred polls (~80 s at 50k
// polls/s), not 65536 episodes.  A wrap can alias a stale creditTagOn with a
// fresh window: at worst one window's delay to an eviction (harmless).
void ClientGateway::onWorkerCapFull_(int32_t worker)
{
    CapEpisode& ep = capEpisode_[static_cast<std::size_t>(worker)];
    if (ep.tag == 0) {                                            // a new episode
        if (++capEpisodeTags_ == 0) ++capEpisodeTags_;            // tags are never 0
        ep = CapEpisode{capEpisodeTags_, 0};
    }
    if (++ep.deferredPolls % kCapEvictAfterDeferredPolls != 0) return;
    const auto w = static_cast<std::size_t>(worker);
    const uint64_t bit = uint64_t{1} << worker;
    int64_t victim = 0;
    uint16_t share = 0;
    sessions_.forEach([&](const Session& s) {
        if ((s.heldByWorkers & bit) == 0) return;                 // not held by this worker: a reader
        if (s.creditTagOn[w] == ep.tag) return;                   // credited in this scan window: a (slow) reader
        if (s.produceInFlightOn[w] > share) { share = s.produceInFlightOn[w]; victim = s.sessionId; }
    });
    if (victim == 0) {                                            // no held session to blame: keep deferring
        // Lows batch C (aligned with D8d's directEpisode_): reader marks count
        // Credits of the LAST scan window only — a fresh tag per window.  A
        // session that credited once early in a long episode and then stalled
        // is judged by what it does now, not marked a reader forever.
        if (++capEpisodeTags_ == 0) ++capEpisodeTags_;
        ep.tag = capEpisodeTags_;
        return;
    }
    ep = CapEpisode{};                                            // the episode ends with its one expiry
    ++workerCapEvictions_;
    lastCapEvictedSid_ = victim;
    spdlog::warn("ClientGateway: worker {}'s gateway-routed share is full ({}); expiring held session {} "
                 "(largest share, {} unclaimed acks) — the client reconnects (evictions={})",
                 worker, routedInFlight_[w], victim, share, workerCapEvictions_);
    (void)closeSession_(victim, /*notifyExpired=*/true);         // frees its share (noteWorkerShareFreed_)
}

void ClientGateway::returnProduceCredits_(Session& s, int32_t worker, int64_t n) noexcept
{
    if (n <= 0) [[unlikely]] return;
    const auto w = static_cast<std::size_t>(worker);
    const bool wasBlocked = s.produceInFlight >= sessionProduceCredits_;
    uint32_t take = s.produceInFlightOn[w];
    if (static_cast<uint64_t>(n) > take) [[unlikely]] {
        // Unreachable: every Credit is a claimed ack of a produce this gateway
        // forwarded for this session to this worker (one credit taken per
        // forward).  Clamp — never a negative budget — and count it, loudly.
        ++produceCreditUnderflows_;
        if (produceCreditUnderflows_ == 1 || (produceCreditUnderflows_ & 1023u) == 0)
            spdlog::error("ClientGateway: session {} returned {} produce credits on worker {} with {} in flight "
                          "(total={})", s.sessionId, n, worker, take, produceCreditUnderflows_);
    } else {
        take = static_cast<uint32_t>(n);
    }
    s.produceInFlightOn[w] = static_cast<uint16_t>(s.produceInFlightOn[w] - take);
    routedInFlight_[w]     -= take;
    s.produceInFlight      -= take;
    if (const uint16_t tag = capEpisode_[w].tag; tag != 0) s.creditTagOn[w] = tag;   // a reader in this episode
    noteWorkerShareFreed_(worker);
    if (wasBlocked && s.produceInFlight < sessionProduceCredits_) {
        --creditBlockedSessions_;
        // The session's image was stalled at a Produce frame while it had no
        // credit (its Keepalives queued behind it): LEAVING that pause is client
        // progress — refresh the lease, as a Released edge does (D8b re-review
        // MEDIUM-1).  Other claims are not client liveness (fix round 1, LOW-1).
        s.lastSeenNs = nowNs_;
    }
}

void ClientGateway::sendSessionExpired_(aeron::ExclusivePublication* pub, Session* session, int64_t sid)
{
    pc::ErrorResponse e;
    e.sessionId     = sid;
    e.correlationId = 0;
    e.code          = static_cast<int16_t>(pc::ErrorCode::SessionExpired);
    const std::size_t encoded = e.encode(scratch_.data());
    // Best effort by nature: the session's publication goes away with it (the
    // client's next request then meets UnknownSession).  offerBytes_ = sendTo in
    // production; the test egress intercepts it.
    if (session) {
        if (session->egress) (void)offerBytes_(session, scratch_.data(), encoded);
        else                 (void)sendTo(*session, scratch_.data(), encoded);   // egress-less test session (hook)
    }
    else if (pub) (void)sendOnPub(*pub, scratch_.data(), encoded);
}

void ClientGateway::onSessionSignal_(const rune::node::WorkerReturn& ret)
{
    using K = rune::node::SessionSignalKind;
    const auto kind = static_cast<K>(ret.lastOffset);
    if (kind == K::PeerFrameDrops) [[unlikely]] {   // Task 16: not a session signal - a worker's plane drop count
        if (peerDropFn_ != nullptr) peerDropFn_(peerDropCtx_, ret.partition, static_cast<uint64_t>(ret.baseOffset));
        return;
    }
    if (kind == K::NsSlotMismatch) [[unlikely]] {   // namespaces Phase 2 Task 8: a worker's cumulative count
        if (ret.partition >= 0 && ret.partition < 4096) {
            if (workerNsSlotMismatches_.size() <= static_cast<std::size_t>(ret.partition))
                workerNsSlotMismatches_.resize(static_cast<std::size_t>(ret.partition) + 1, 0);
            workerNsSlotMismatches_[static_cast<std::size_t>(ret.partition)] = static_cast<uint64_t>(ret.baseOffset);
        }
        return;
    }
    if (kind == K::NsUsage) [[unlikely]] {   // namespaces Phase 2 Task 14 (D28): sessionId = (slot << 8) | metric
        if (nsAggregator_ != nullptr && ret.partition >= 0 && ret.sessionId >= 0) {
            const auto metric = static_cast<std::size_t>(ret.sessionId & 0xFF);
            if (metric < rune::node::kNsMetrics)
                (void)nsAggregator_->report(static_cast<uint32_t>(ret.partition), static_cast<uint16_t>(ret.sessionId >> 8),
                                            static_cast<uint32_t>(ret.correlationId), static_cast<rune::node::NsMetric>(metric),
                                            static_cast<uint64_t>(ret.baseOffset));
        }
        return;
    }
    if (kind == K::EgressReleased) {                                  // retire-on-ack: the worker let go
        onEgressReleased_(ret.partition, static_cast<uint64_t>(ret.baseOffset));
        return;
    }
    if (kind == K::FenceAck) [[unlikely]] {                           // issues #109 / #196: the D24 gate's proof
        onEnforceFenceAck_(ret.partition, static_cast<int>(ret.sessionId), static_cast<uint64_t>(ret.baseOffset));
        return;
    }
    if (kind != K::Failed && kind != K::Expire) {
        Session* s = sessions_.find(ret.sessionId);
        if (!s) return;                                               // closed meanwhile
        if (kind == K::Progress) { s->lastSeenNs = nowNs_; return; }  // D8b: the held client reads its acks
        // Unreachable: NodeConfig refuses > kMaxHeldMaskWorkers shard workers.
        assert(ret.partition >= 0 && ret.partition < kMaxHeldMaskWorkers);
        if (ret.partition < 0 || ret.partition >= kMaxHeldMaskWorkers) [[unlikely]] return;
        if (kind == K::Credit) { returnProduceCredits_(*s, ret.partition, ret.baseOffset); return; }   // D8c: acks claimed
        const uint64_t bit = uint64_t{1} << ret.partition;
        if (kind == K::Lost || kind == K::Found) {                    // D8b round 2
            if (kind == K::Lost) {
                if (s->lostByWorkers == 0) s->lostSinceNs = nowNs_;
                s->lostByWorkers |= bit;
            } else if (s->lostByWorkers & bit) {
                // D8b-L: a Found is a successful claim — proof the client is alive.  If another worker is still
                // Lost its suspicion restarts here (it cannot predate this proof of life), so the lease never
                // runs from an edge older than the newest evidence the client was reachable.
                s->lostByWorkers &= ~bit;
                if (s->lostByWorkers != 0) s->lostSinceNs = nowNs_;
            }
            return;
        }
        if (kind != K::Held && kind != K::Released) [[unlikely]] return;   // unknown kind: never a release
        const bool was = s->heldByWorkers != 0;
        if (kind == K::Held) s->heldByWorkers |= bit; else s->heldByWorkers &= ~bit;
        const bool now = s->heldByWorkers != 0;
        if (now && !was) ++heldSessions_;
        if (was && !now) {
            --heldSessions_;
            // D8b re-review MEDIUM-1: the hold ends only when the queue emptied through successful claims (or a
            // close/fail, which removes the session anyway) — that is progress; a backlog drained in one pass
            // supersedes its Progress signal, so refresh the lease here or the session is reaped as it recovers.
            s->lastSeenNs = nowNs_;
        }
        return;
    }
    // Failed (C2): close the session everywhere and tell the client, so it
    // reconnects; its later notifications find no session.  State-driven.
    // Expire (D8d): the worker's direct (worker-ingress) ack share is full and
    // this session — held there with the largest direct share — is expired the
    // same way (D8c's gateway-cap expiry, for the direct path): the client
    // reconnects and resends its unacked produces (idempotent producers dedupe).
    if (closeSession_(ret.sessionId, /*notifyExpired=*/true)) {
        if (kind == K::Expire) {
            ++directShareExpiries_;
            spdlog::warn("ClientGateway: session {} expired — worker {}'s direct ack share is full and it holds "
                         "the largest share ({} acks) without reading (the client was told SessionExpired, "
                         "expiries={})", ret.sessionId, ret.partition, ret.baseOffset, directShareExpiries_);
        } else {
            spdlog::warn("ClientGateway: session {} closed — its ack publication on worker {} can never "
                         "deliver (the client was told SessionExpired)", ret.sessionId, ret.partition);
        }
        return;
    }
    // Not accepted yet (ConnectResponse still retrying): drop the pending
    // connect — never accept a session no worker can ack.
    for (auto it = pendingConnects_.begin(); it != pendingConnects_.end(); ++it) {
        if (it->sessionId != ret.sessionId) continue;
        if (kind == K::Expire) {                                      // D8d review LOW-4: the right reason, counted
            ++directShareExpiries_;
            spdlog::warn("ClientGateway: pending session {} dropped — worker {}'s direct ack share is full and it "
                         "holds the largest share ({} acks) without reading (expiries={})", ret.sessionId,
                         ret.partition, ret.baseOffset, directShareExpiries_);
        } else {
            spdlog::warn("ClientGateway: pending session {} dropped — its ack publication on worker {} can "
                         "never deliver", ret.sessionId, ret.partition);
        }
        if (it->resolvedPub) sendSessionExpired_(it->resolvedPub.get(), nullptr, ret.sessionId);
        closeDroppedPendingConnect_(*it);
        pendingConnects_.erase(it);   // resolvedPub released with the entry
        break;
    }
}

void ClientGateway::handleConnect(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto reqOpt = pc::ConnectRequest::decode(buf, len);
    if (!reqOpt) {
        // No session and no decodable reply channel: nothing to answer on.  The
        // client's connect deadline bounds it.  Counted + warned (R2).
        noteUnanswerable_(buf, len, "Connect body undecodable (no reply channel)");
        return;
    }

    const auto& req = *reqOpt;

    // Issue #10 (M1): a Connect never resumes an authenticated session by its id.  The plaintext resume-by-id
    // (D11) let any UDP sender that names a session id and its reply endpoint take the session over; every client
    // session of an authenticating broker is TLS now, and an encrypted session is bound to its keys, so a reconnect
    // is a fresh session that logs in again.
    // Authentication Task 11 (D11 version gate): an authenticating broker refuses a pre-v9 client AT THE HANDSHAKE
    // (ConnectResponse{ProtocolVersion}, non-retriable for every client version) - it could never log in, and it
    // must not be able to reach a gated session at all.  No session is created.
    const bool refusedForVersion = authMode_ == rune::gateway::GatewayAuthMode::Required &&
                                   !pc::clientUnderstandsAuth(req.clientVersion);
    if (refusedForVersion && unauthLog_.due(5)) [[unlikely]]   // X103: any UDP sender can drive this
        spdlog::warn("auth: refused client version {} at connect (authentication requires {})", req.clientVersion,
                     pc::kClientVersionAuth);
    // Task 13 fix round 1 (I2): a worker's ack table stores the reply channel in
    // a fixed slot — refuse a longer one HERE (never accept a session no worker
    // can ack; the worker treats a refused announcement as an invariant halt).
    // Fix round 2: refused EXPLICITLY — the gateway can still resolve the reply
    // channel (only the workers' fixed slots cannot hold it), so the connect
    // goes through the pending-connect machinery and is answered
    // ConnectResponse{InvalidRequest} with no session (pollPendingConnects).
    const bool channelTooLongForShard =
        shardMode_ && req.egressChannel.size() > rune::node::SessionAckSink::kMaxChannelBytes;
    if (channelTooLongForShard) [[unlikely]] {
        spdlog::warn("ClientGateway: egress channel of {} B exceeds the shard limit {} B; Connect refused "
                     "(InvalidRequest)", req.egressChannel.size(), rune::node::SessionAckSink::kMaxChannelBytes);
    }

    // -----------------------------------------------------------------------
    // BR-5 / MD1 Fix 6: check leadership before creating a session.
    //
    // MD1 Fix 6 (per-partition routing session gate):
    //   When routingImage_ is set (per-partition routing is active — either from
    //   StaticBalancer during the bootstrap window, or from committedMap in steady
    //   state), this node can accept sessions regardless of Raft leadership.
    //   The produce / subscribe handlers route each op to the correct partition
    //   leader via Path A (routingImage_ lookup), so the session host does NOT
    //   need to be the Raft leader.
    //
    //   Rationale: without this gate, Raft-follower nodes that are partition
    //   leaders (e.g. rune-1 = node 0 holds events:0) could never accept client
    //   sessions, because their leaderView_.isLeader == false caused
    //   handleConnect to enqueue an ErrorResponse{Redirect} back to the Raft
    //   leader.  Combined with Fix 5 (StaticBalancer immediate routingImage_ on
    //   all nodes), this caused a redirect loop:
    //     rune-2 (Raft leader) → Path A → Redirect produce to rune-1
    //     rune-1 (Raft follower) → handleConnect → Redirect session to rune-2
    //     → kMaxRedirects exhausted.
    //
    //   When routingImage_ is null (not yet set), fall back to the original
    //   Raft-leader gate (Path B / pre-MD1 behaviour).
    // -----------------------------------------------------------------------
    //
    // Follower-connect hang fix (2026-10-14): which node hosts a session is the
    // gateway's explicit SessionAdmission, never inferred from whether a
    // routing image happens to exist.  A BrokerNode (metadata-driven routing)
    // admits sessions on EVERY broker (AnyNode): its data ops route per
    // partition and its DDL per catalog leader, so the Raft-leader gate below
    // only applies to a gateway left in ClusterLeaderOnly.  The old inference
    // ("no image yet ⇒ bootstrap ⇒ Raft-leader gate") was wrong for a cluster
    // with no user topic: it has no image FOREVER (NodeRuntimeAgent publishes
    // none without one), so every follower Redirected every Connect.
    bool capturedIsLeader = true;
    std::string capturedLeaderEndpoint;
    if (leaderView_ && sessionAdmission_ == SessionAdmission::ClusterLeaderOnly && !routingImage_) {
        // No per-partition routing image yet: use the Raft-leader gate (Path B /
        // pre-MD1 bootstrap).  Raft followers redirect sessions to the Raft leader.
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
        //   sleeps up to the then 5 s resolve cap in 1 ms slices ON THE GATEWAY
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
    // else: routingImage_ is set → capturedIsLeader remains true (accept session
    //       on any node; per-partition routing in the produce/subscribe handlers
    //       routes each op to the correct partition leader).  This is the MD1
    //       steady-state and bootstrap-window path (Fix 5 + Fix 6 together).

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
    // D14: the source host (the throttle key and, issue #236, the unauthenticated-session cap's and the pending
    // table's fairness key) - known under GatewayAuthMode::Required only (cold: one Connect).
    std::string connectHost;
    if (authMode_ == rune::gateway::GatewayAuthMode::Required) connectHost = sourceHostOf_(req.egressChannel);
    if (pendingConnects_.size() >= kMaxPendingConnects) evictOldestPendingConnect_(connectHost);

    // Pre-allocate sessionId now so it is stable across cycles.
    const int64_t sessionId = sessions_.nextSessionId();

    PendingConnect pc_entry;
    pc_entry.sessionId             = sessionId;
    pc_entry.correlationId         = req.correlationId;
    pc_entry.requestedAtNs         = nowNs;
    pc_entry.isLeader              = capturedIsLeader;
    pc_entry.leaderClientEndpoint  = std::move(capturedLeaderEndpoint);
    pc_entry.egressChannel         = std::string(req.egressChannel);
    pc_entry.egressStreamId        = req.egressStreamId;
    pc_entry.clientVersion         = req.clientVersion;   // D6
    pc_entry.sourceHost            = std::move(connectHost);   // every entry: the eviction's fairness key
    if (channelTooLongForShard) [[unlikely]]
        pc_entry.refuseCode = static_cast<uint16_t>(pc::ErrorCode::InvalidRequest);
    if (refusedForVersion) [[unlikely]]
        pc_entry.refuseCode = static_cast<uint16_t>(pc::ErrorCode::ProtocolVersion);   // wins over any other refusal
    if (authMode_ == rune::gateway::GatewayAuthMode::Required && pc_entry.refuseCode == 0) {
        // Issue #236: an unauthenticated session holds its slot until the auth deadline; one source host may hold at
        // most maxUnauthPerHost_ of them (so it can never fill kShardMaxSessions).  A session that logged in, was
        // reaped or closed no longer counts.  A redirect creates no session here and is never refused.
        const bool redirect = !pc_entry.isLeader && !pc_entry.leaderClientEndpoint.empty();
        if (!redirect && unauthSessionsOf_(pc_entry.sourceHost) >= maxUnauthPerHost_) [[unlikely]] {
            pc_entry.refuseCode = static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed);   // answered, no session
            ++connectsRefusedUnauthCap_;
            if (unauthLog_.due(15)) [[unlikely]]
                spdlog::warn("auth: connect refused source={} reason=unauthenticated-sessions-per-host cap={} "
                             "(refused so far: {})", LogSafeName{pc_entry.sourceHost}, maxUnauthPerHost_,
                             connectsRefusedUnauthCap_);
        }
    }

    // client<->broker TLS/AEAD (Task 7): drive the server-role handshake's
    // FIRST leg right here, synchronously, off the client's ClientHello
    // bytes carried on this very ConnectRequest.  Skipped entirely for a
    // redirect (this node will create no session either way).
    // NOTE: capturedLeaderEndpoint was already std::move()'d into
    // pc_entry.leaderClientEndpoint above — read the redirect decision back
    // off pc_entry, not the now-moved-from local.
    const bool isRedirectCapture = (!pc_entry.isLeader && !pc_entry.leaderClientEndpoint.empty()) ||
                                   pc_entry.refuseCode != 0;   // a refused connect engages no crypto either
    const bool clientOffersCrypto = !req.cipherList.empty() && !req.handshakeBytes.empty();
    const bool brokerHasTls       = !tlsCertPem_.empty() && !tlsKeyPem_.empty();
    // Task 19 (per-worker egress crypto addendum): shard mode used to refuse
    // every crypto offer here (ConnectDowngradePolicy.h's now-removed
    // shardModeRefusesCryptoOffer) because a shard-mode worker's Records
    // egress was plaintext-only. The shard worker now seals its own Records
    // egress with its own per-sender key (see
    // handleHandshakeContinue_/pollPendingConnects' deferred crypto-key
    // SessionAnnounce), so shard mode engages crypto exactly like legacy mode.
    if (!isRedirectCapture && shouldEngageCrypto(clientOffersCrypto, brokerHasTls)) {
        pc_entry.tls.emplace(rune::crypto::TlsSession::makeServer(tlsCertPem_.c_str(), tlsKeyPem_.c_str()));
        if (!pc_entry.tls->valid()) {
            // Local TLS init failure (e.g. a misconfigured cert/key on THIS
            // broker) — degrade to plaintext rather than hard-fail a connect
            // over a broker-side problem the client had no part in (mirrors
            // the client's own beginCryptoHandshake_ degrade-on-local-failure).
            // require_tls (below) still catches this and rejects rather than
            // serving it plaintext when the operator asked for mandatory TLS.
            pc_entry.tls.reset();
        } else {
            // The broker's chosen cipher: the client's first preference.
            // (A real negotiation policy — e.g. intersecting with a broker
            // allow-list — is future scope; here we mirror the client
            // reference test's own driveHandshake(), which takes
            // cipherList[0] verbatim.)
            pc_entry.chosenCipher = static_cast<rune::crypto::Cipher>(req.cipherList[0]);
            const auto hs = pc_entry.tls->readInbound(
                reinterpret_cast<const uint8_t*>(req.handshakeBytes.data()),
                req.handshakeBytes.size());
            if (hs == rune::crypto::HsState::Failed) {
                // Malformed/invalid ClientHello (bad token embedded in it,
                // cert/SNI mismatch, protocol error, ...) — fail the connect
                // non-retriably; pollPendingConnects answers
                // ConnectResponse{HandshakeFailed} and creates no session.
                pc_entry.cryptoFailed = true;
                pc_entry.tls.reset();
            } else {
                drainHandshakeOutbound_(pc_entry);
            }
        }
    }

    // require_tls (Task 9, downgrade safety — spec §11): once the above has
    // resolved whether this connect established crypto, hard-reject any
    // case that would otherwise be served plaintext.  Covers: a plaintext
    // client, this broker having no cert/key, a local TLS init failure
    // degrading to plaintext above, AND a shard-mode-refused crypto offer
    // (already cryptoFailed=true, so this is a no-op there — it stays
    // HandshakeFailed either way, never silently plaintext).
    if (!isRedirectCapture && !pc_entry.cryptoFailed &&
        mustRejectForRequireTls(requireTls_, pc_entry.tls.has_value())) {
        if (unauthLog_.due(0)) [[unlikely]]   // X103: any host can drive this
            spdlog::warn("ClientGateway: require_tls rejecting a plaintext Connect "
                         "(session={})", sessionId);
        pc_entry.cryptoFailed = true;
    }

    // DR-15 fix round 2 (LOW-4): a reply HOSTNAME is resolved off-thread by the
    // cold resolver (never by a driver conductor on a duty thread); the session
    // stays pending — bounded by the connect timeout — until the hub answers.
    const std::string_view replyHost = rune::consensus::PeerHostTable::endpointHost(pc_entry.egressChannel);
    if (hostResolver_.valid() && !replyHost.empty() &&
        !rune::consensus::PeerHostTable::isNumericHost(replyHost)) [[unlikely]] {
        pc_entry.awaitingResolve = true;
        // Fix round 3 (concern 2): a momentarily full resolve ring no longer
        // drops the Connect — the pending connect is kept (ResolveOfferPending)
        // and the offer retried every poll until accepted or the connect
        // timeout (pollPendingConnects).
        pc_entry.resolveOfferPending = !offerHostResolve_(pc_entry);
        if (pc_entry.resolveOfferPending)
            spdlog::warn("ClientGateway: host-resolve ring full; session={} Connect kept pending, offer retried",
                         sessionId);
        pendingConnects_.push_back(std::move(pc_entry));
        return;
    }

    // Kick off the egress publication registration (returns immediately).
    if (!registerConnectEgress_(pc_entry)) return;
    pendingConnects_.push_back(std::move(pc_entry));
}

// ---------------------------------------------------------------------------
// client<->broker TLS/AEAD (Task 7): drain every pending outbound handshake
// byte out of pending.tls into pending.hsBytes.  Mirrors the client's own
// SessionMachine::drainHandshakeOutbound_() exactly (cold path — once per
// handshake leg; the growable vector is explicitly out of the zero-alloc
// hot-path rule, same reasoning as the client side). No-op if pending.tls is
// empty (callers only invoke this with tls engaged).
// ---------------------------------------------------------------------------
void ClientGateway::drainHandshakeOutbound_(PendingConnect& pending)
{
    pending.hsBytes.clear();
    if (!pending.tls) return;
    uint8_t chunk[4096];
    for (;;) {
        const std::size_t n = pending.tls->writeOutbound(chunk, sizeof(chunk));
        if (n == 0) break;
        pending.hsBytes.insert(pending.hsBytes.end(), chunk, chunk + n);
    }
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
bool ClientGateway::registerConnectEgress_(PendingConnect& pending)
{
    if (testEgressFn_ && !aeron_) [[unlikely]] {
        // Test gateways never reach Aeron (issue #236 tests drive real Connects): a placeholder reply publication,
        // never dereferenced while testEgressFn_ intercepts every pre-session offer (offerOnPub_).
        static unsigned char placeholder alignas(64)[512];
        pending.resolvedPub = std::shared_ptr<aeron::ExclusivePublication>(
            reinterpret_cast<aeron::ExclusivePublication*>(placeholder), [](aeron::ExclusivePublication*) {});
        return true;
    }
    try {
        // A10: sparse terms — the gateway runs on a worker thread in fused placement, where a non-sparse
        // term's tmpfs fallocate fails with EINTR while io_uring completions are pending (withSparseTerms).
        pending.registrationId = aeron_->addExclusivePublication(
            rune::node::withSparseTerms(pending.egressChannel), pending.egressStreamId);
    } catch (...) {
        // Bad channel — drop (never crash).
        spdlog::warn("ClientGateway: addExclusivePublication threw for egress channel {}; dropping Connect",
                     LogSafeName{pending.egressChannel});
        return false;
    }
    return true;
}

// Offer the reply host of `pending` to the cold resolver (cold path: the host
// string is heap-owned by the request until the hub frees it).  False = the
// ring is full right now (the caller keeps the connect pending and retries).
bool ClientGateway::offerHostResolve_(PendingConnect& pending)
{
    // Fix round 4 (N12): allocated once per connect; a refused offer keeps it
    // (retried each poll with no new allocation); the ring takes it on success.
    if (!pending.resolveHost)
        pending.resolveHost = std::make_unique<std::string>(
            rune::consensus::PeerHostTable::endpointHost(pending.egressChannel));
    if (!hostResolver_.req->offer(rune::consensus::kHostResolveRequest, pending.sessionId,
                                  static_cast<int64_t>(reinterpret_cast<std::intptr_t>(pending.resolveHost.get()))))
        return false;
    (void)pending.resolveHost.release();   // owned by the resolver from here (it deletes it)
    return true;
}

int ClientGateway::pollHostResolutions_()
{
    if (!hostResolver_.valid()) [[likely]] return 0;
    return hostResolver_.resp->poll([this](std::int32_t type, std::int64_t id, std::int64_t ip) {
        for (auto it = pendingConnects_.begin(); it != pendingConnects_.end(); ++it) {
            if (it->sessionId != id || !it->awaitingResolve) continue;
            if (type != rune::consensus::kHostResolved) {
                spdlog::warn("ClientGateway: reply host of channel {} does not resolve; session={} dropped "
                             "(the client retries)", LogSafeName{it->egressChannel}, it->sessionId);
                closeDroppedPendingConnect_(*it);
                pendingConnects_.erase(it);
                return;
            }
            in_addr a{};
            a.s_addr = static_cast<uint32_t>(ip);
            char buf[INET_ADDRSTRLEN] = {};
            ::inet_ntop(AF_INET, &a, buf, sizeof buf);
            it->egressChannel   = rune::consensus::PeerHostTable::withEndpointHost(it->egressChannel, buf);
            it->awaitingResolve = false;
            if ((shardMode_ && it->egressChannel.size() > rune::node::SessionAckSink::kMaxChannelBytes) ||
                !registerConnectEgress_(*it)) {
                spdlog::warn("ClientGateway: resolved egress channel {} unusable; session={} dropped",
                             LogSafeName{it->egressChannel}, it->sessionId);
                closeDroppedPendingConnect_(*it);
                pendingConnects_.erase(it);
            }
            return;
        }
        // Not pending any more (timed out / evicted): nothing registered — nothing to release.
    });
}

void ClientGateway::pollPendingConnects(int64_t nowNs)
{
    // Drive the draining-release table first (bounded, non-blocking).
    drainPendingReleases();
    (void)pollHostResolutions_();   // fix round 2 (LOW-4): an empty ring is a few loads

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
            // Awaiting its reply host: no publication was ever registered.
            const bool alreadyResolved  = (pending.resolvedPub != nullptr) || pending.awaitingResolve;
            // Task 13: a session announced (fully or partly) but never accepted
            // is closed on every worker — the client never learned its id.
            closeDroppedPendingConnect_(pending);
            it = pendingConnects_.erase(it); // drops resolvedPub shared_ptr if non-null
            if (!alreadyResolved) abandonRegistration_(timedOutRegId);
            continue;
        }

        if (pending.failingHandshake) [[unlikely]] {      // fix round 3 (N4): HandshakeFailed not yet delivered
            if (offerHandshakeFailed_(pending)) {
                closeDroppedPendingConnect_(pending);
                it = pendingConnects_.erase(it);
            } else {
                ++it;
            }
            continue;
        }
        if (pending.resolveOfferPending) [[unlikely]] {   // fix round 3: retry a refused resolve offer
            pending.resolveOfferPending = !offerHostResolve_(pending);
            ++it;
            continue;
        }
        if (pending.awaitingResolve) { ++it; continue; }   // reply host not resolved yet (LOW-4)
        // client<->broker TLS/AEAD (Task 7): the first ConnectResponse (with
        // the chosen cipher + server's first flight) was already sent; this
        // entry now waits for the client's HandshakeContinue legs
        // (handleHandshakeContinue_) to drive tls->done() — never re-sent nor
        // erased here.  The generic timeout check above still bounds a
        // client that never continues the handshake.
        if (pending.awaitingHandshake) { ++it; continue; }

        // Try to find the publication (non-blocking).
        std::shared_ptr<aeron::ExclusivePublication> pub;
        try {
            if (aeron_) pub = aeron_->findExclusivePublication(pending.registrationId);   // test gateway: stashed pub
        } catch (...) {
            // ERRORED_MEDIA_DRIVER: conductor signalled an error (bad channel, etc.).
            // The conductor erased its own entry when throwing — no registration leak.
            spdlog::warn("ClientGateway: findExclusivePublication threw for session={}; dropped",
                         pending.sessionId);
            closeDroppedPendingConnect_(pending);   // fix round 1 (I2): every drop site
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
                // Fix round 2: a pathological endpoint length is never a silent
                // drop — the client gets the bare NotLeader (no endpoint).
                spdlog::warn("ClientGateway: redirect endpoint too large ({}B); answered NotLeader",
                             pending.leaderClientEndpoint.size());
                err.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                err.message = {};
            }
            encoded = err.encode(scratch_.data());
        } else if (pending.refuseCode != 0) [[unlikely]] {
            // Fix round 2: an explicitly refused connect (e.g. a reply channel the
            // shard workers cannot hold) — answered, NO session created.
            pc::ConnectResponse resp;
            resp.sessionId     = 0;
            resp.correlationId = pending.correlationId;
            resp.code          = static_cast<int16_t>(pending.refuseCode);
            encoded = resp.encode(scratch_.data());
        } else if (pending.cryptoFailed) {
            // client<->broker TLS/AEAD (Task 7): the server-role TlsSession
            // rejected the client's opening handshake bytes (handleConnect).
            // Fail the connect non-retriably — mirrors the redirect branch
            // above in creating NO session (Invariant FIX A extends to this
            // path too: never add a session the client wasn't told about).
            pc::ConnectResponse resp;
            resp.sessionId     = 0;
            resp.correlationId = pending.correlationId;
            resp.code          = static_cast<int16_t>(pc::ErrorCode::HandshakeFailed);
            encoded = resp.encode(scratch_.data());
        } else {
            // Task 13 (spec §3.4) announce-before-accept: every worker must know
            // this session's reply destination before the client can learn its
            // session id (its first Produce may reach a worker before any later
            // runtime message).  A full worker ring defers the accept to the next
            // cycle (bounded by the connect timeout, never a wait); workers
            // already reached are not re-sent.  The worker ack table's capacity
            // bounds concurrent shard sessions (pending closes still hold slots).
            if (shardMode_ && workerPool_ && !pending.announced) {
                // Fix round 1 (I2): count every slot a worker table may hold —
                // including announced-but-not-accepted connects (this one is
                // counted once its announcement begins).
                if (!pending.announceBegun &&
                    shardSessionSlotsInUse_() + 1 > kShardMaxSessions) [[unlikely]] {
                    spdlog::warn("ClientGateway: shard session cap {} reached; session={} held until the "
                                 "connect timeout", kShardMaxSessions, pending.sessionId);
                    ++it;
                    continue;
                }
                pending.announceBegun = true;
                // Task 19 ★: mark `sessionEncrypted` here too (keyReady=false
                // — the real b2c key doesn't exist yet; it is derived only
                // once pending.tls->done(), in handleHandshakeContinue_'s own
                // second announce wave). This FIRST wave is what the client's
                // own ConnectResponse (sent right below, once `announced`)
                // is gated on — i.e. every worker is guaranteed to already
                // have a "parked" (fail-closed) entry for this session
                // BEFORE the client can possibly learn its sessionId and
                // Subscribe, closing the race against the second wave's own
                // retry (see pending.tls.has_value() below: true only for a
                // connect still attempting crypto — see shouldEngageCrypto).
                if (!sessionAnnouncer_.announce(*workerPool_, pending.sessionId,
                                                pending.egressChannel, pending.egressStreamId,
                                                pending.clientVersion,
                                                /*sessionEncrypted=*/pending.tls.has_value(),
                                                /*keyReady=*/false)) {
                    ++it;
                    continue;   // a worker ring is full — retry next cycle
                }
                pending.announced = true;
            }
            // Invariant (FIX A): a session must NEVER be added to the table unless
            // the client received a ConnectResponse for it.  Only promote to a
            // session after a successful send.
            pc::ConnectResponse resp;
            resp.sessionId         = pending.sessionId;
            resp.correlationId     = pending.correlationId;
            resp.code              = static_cast<int16_t>(pc::ErrorCode::OK);
            resp.leaderMemberId    = thisNodeId_;
            resp.assignedSessionId = pending.sessionId;
            // Authentication Task 10 (D11): a v9 client of an authenticating broker is told how it authenticates and
            // where it stands.  A NEVER-bootstrapped cluster (no `__security` event yet: nobody exists who could log
            // in): Anonymous - it may send ONLY SecurityBootstrap.  Otherwise MustAuthenticate - also while the window
            // is REOPENED (the last admin disabled / dropped, D3 recovery): the other users still exist and log in
            // (issue #121), and SecurityBootstrap stays allowed in AwaitingFirst while the window is open (gateFor).
            // An in-process broker keeps the pre-v9 frame.
            pending.auth = newSessionAuth_(nowNs, pending.correlationId);
            if (authMode_ != rune::gateway::GatewayAuthMode::InProcess && pc::clientUnderstandsAuth(pending.clientVersion)) {
                resp.v9 = true;
                resp.authMode = authMode_ == rune::gateway::GatewayAuthMode::Required ? pc::AuthMode::Required
                                                                                       : pc::AuthMode::DevDisabled;
                resp.authState = connectAuthState_();
            }
            if (pending.tls) {
                // client<->broker TLS/AEAD (Task 7): offer the chosen cipher +
                // the server's first handshake flight (ServerHello/cert/
                // Finished) on this SAME leg — mirrors the client's Task-5
                // first-leg shape (ConnectResponse v4's additive fields).
                resp.chosenCipher   = static_cast<uint8_t>(pending.chosenCipher);
                resp.handshakeBytes = std::string_view(
                    reinterpret_cast<const char*>(pending.hsBytes.data()), pending.hsBytes.size());
            }
            encoded = resp.encode(scratch_.data());
        }

        if (!offerOnPub_(*pub, scratch_.data(), encoded)) {
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

            // Send succeeded.  Redirect / handshake-failure entries create NO
        // session (the resolvedPub shared_ptr release on erase frees the
        // transient egress registration).
        if (isRedirect || pending.cryptoFailed || pending.refuseCode != 0) {
            it = pendingConnects_.erase(it);
            continue;
        }
        if (pending.tls) {
            // client<->broker TLS/AEAD (Task 7): the client now has its
            // sessionId and the server's first flight; it drives the rest
            // over HandshakeContinue/HandshakeContinueResponse
            // (handleHandshakeContinue_).  Do NOT promote to SessionTable yet
            // (Invariant FIX A: no session without a completed, keyed
            // handshake) and do NOT erase — this entry keeps the in-flight
            // TlsSession (and the now-resolved, Aeron-connected pub) alive,
            // keyed by sessionId.
            pending.awaitingHandshake = true;
            ++it;
            continue;
        }
        // Only now is it safe to add the session (leader-accept, plaintext
        // path).  Carry the client's egress endpoint so a per-consumer
        // unicast Records publication can be lazily created on the first
        // subscribe.
        sessions_.add(pending.sessionId, std::move(pub), std::move(pending.auth), nowNs,
                      pending.egressChannel, pending.egressStreamId,
                      pending.clientVersion);
        {
            Session* added = sessions_.find(pending.sessionId);
            added->sourceHost = std::move(pending.sourceHost);
        }

        it = pendingConnects_.erase(it);
    }
}

// ---------------------------------------------------------------------------
// client<->broker TLS/AEAD (Task 7): handleHandshakeContinue_ — a later leg
// of a multi-round TLS handshake, after handleConnect/pollPendingConnects
// started it on the ConnectResponse leg.  The session this concerns has NOT
// been promoted to sessions_ yet (it's still a PendingConnect with
// awaitingHandshake==true) — this is the ONLY other place besides
// pollPendingConnects that may promote one.  Linear scan over pendingConnects_
// (a deque bounded by kMaxPendingConnects==64) keyed by sessionId.  Unknown
// sessionId / decode failure / no matching awaiting-handshake entry → drop,
// never crash (same contract as every other dispatch handler).
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// failPendingHandshake_ — THE failure path of a handshake in progress: send
// HandshakeFailed (non-retriable) on its reply publication, close it
// everywhere (Task 13: an announced-but-never-accepted session), forget it.
// ---------------------------------------------------------------------------
void ClientGateway::failPendingHandshake_(std::deque<PendingConnect>::iterator it, int64_t correlationId)
{
    PendingConnect& pending = *it;
    // Fix round 3 (N4): delivered reliably — a refused offer leaves the entry in
    // the explicit FailingHandshake state (awaitingHandshake cleared), re-offered
    // every poll until delivered or the connect timeout.
    pending.awaitingHandshake  = false;
    pending.failingHandshake   = true;
    pending.handshakeFailCorr  = correlationId;
    if (offerHandshakeFailed_(pending)) {
        closeDroppedPendingConnect_(pending);
        pendingConnects_.erase(it);
    }
}

bool ClientGateway::offerHandshakeFailed_(PendingConnect& pending)
{
    pc::HandshakeContinueResponse fail;
    fail.sessionId     = pending.sessionId;
    fail.correlationId = pending.handshakeFailCorr;
    fail.code          = static_cast<int16_t>(pc::ErrorCode::HandshakeFailed);
    static_assert(pc::MessageHeader::ENCODED_SIZE + 64 <= kScratchSize);
    return offerOnPub_(*pending.resolvedPub, scratch_.data(), fail.encode(scratch_.data()));
}

// Pre-session sends (a pending connect's reply publication): the real
// publication, or the TestEgressFn (test gateways never reach Aeron).
bool ClientGateway::offerOnPub_(aeron::ExclusivePublication& pub, const uint8_t* buf, std::size_t len)
{
    if (testEgressFn_) [[unlikely]] return testEgressFn_(buf, len) > 0;
    return sendOnPub(pub, buf, len);
}

void ClientGateway::handleHandshakeContinue_(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto hcOpt = pc::HandshakeContinue::decode(buf, len);
    if (!hcOpt) {
        // Fix round 2 (I5): the header's sessionId names the handshake in
        // progress (an awaitingHandshake pending connect with a resolved reply
        // publication) — THAT handshake fails explicitly: HandshakeFailed on the
        // header's correlationId, the pending connect closed.  Only a frame with
        // no decodable header or no such handshake is unanswerable.
        const auto hdr = pc::MessageHeader::decode(buf, len);
        auto pit = pendingConnects_.begin();
        for (; hdr && pit != pendingConnects_.end(); ++pit)
            if (pit->sessionId == hdr->sessionId && pit->awaitingHandshake) break;
        if (!hdr || pit == pendingConnects_.end() || !pit->resolvedPub) {
            noteUnanswerable_(buf, len, "HandshakeContinue undecodable, no handshake in progress");
            return;
        }
        ++rejectedRequests_;
        failPendingHandshake_(pit, hdr->correlationId);
        return;
    }
    const auto& hc = *hcOpt;

    auto it = pendingConnects_.begin();
    for (; it != pendingConnects_.end(); ++it) {
        if (it->sessionId == hc.sessionId && it->awaitingHandshake) break;
    }
    if (it == pendingConnects_.end()) return;  // stale/unknown — drop (client retries are safe: no-op)

    PendingConnect& pending = *it;
    // Defensive: awaitingHandshake is only ever set together with tls
    // engaged (pollPendingConnects); a missing tls here would be a logic bug
    // elsewhere, not a peer-triggerable condition — drop rather than crash.
    if (!pending.tls || !pending.resolvedPub) return;

    const auto hs = pending.tls->readInbound(
        reinterpret_cast<const uint8_t*>(hc.handshakeBytes.data()), hc.handshakeBytes.size());
    if (hs == rune::crypto::HsState::Failed) {
        failPendingHandshake_(it, hc.correlationId);
        return;
    }

    drainHandshakeOutbound_(pending);
    if (!pending.hsBytes.empty()) {
        pc::HandshakeContinueResponse cont;
        cont.sessionId     = pending.sessionId;
        cont.correlationId = hc.correlationId;
        cont.code          = static_cast<int16_t>(pc::ErrorCode::OK);
        cont.handshakeBytes = std::string_view(
            reinterpret_cast<const char*>(pending.hsBytes.data()), pending.hsBytes.size());
        const std::size_t need = cont.encodedSize();
        if (need <= scratch_.size()) {
            // Best-effort (matches the rest of the connect-leg sends in this
            // file, which stash-and-retry via pollPendingConnects; a dropped
            // HandshakeContinueResponse here just means the client's own
            // connect-timeout retries the leg — bounded, never silent data
            // corruption, since no keys are installed until done() below).
            (void)sendOnPub(*pending.resolvedPub, scratch_.data(), cont.encode(scratch_.data()));
        } else {
            spdlog::warn("ClientGateway: HandshakeContinueResponse too large ({}B) for session={}; dropping leg",
                         need, pending.sessionId);
        }
    }

    if (!pending.tls->done()) return;  // more legs to come

    // client<->broker TLS/AEAD (Task 7): exportKeys's documented fail-closed
    // sentinel — Cipher::None means "do not trust these keys", never install
    // an all-zero AeadCipher.
    const rune::crypto::SessionKeys keys = pending.tls->exportKeys(pending.chosenCipher, pending.sessionId);
    if (keys.cipher == rune::crypto::Cipher::None) {
        pc::HandshakeContinueResponse fail;
        fail.sessionId     = pending.sessionId;
        fail.correlationId = hc.correlationId;
        fail.code          = static_cast<int16_t>(pc::ErrorCode::HandshakeFailed);
        const std::size_t need = fail.encodedSize();
        if (need <= scratch_.size()) {
            (void)sendOnPub(*pending.resolvedPub, scratch_.data(), fail.encode(scratch_.data()));
        }
        closeDroppedPendingConnect_(pending);
        pendingConnects_.erase(it);
        return;
    }

    // ★ DIRECTION MAPPING (task-7-interfaces.md) — the broker is the mirror
    // of the client: recv OPENS client->broker ingress (c2b/c2bPrefix), send
    // SEALS broker->client egress (b2c/b2cPrefix).  Getting this backwards
    // makes the handshake "succeed" with matching keys while every data-plane
    // AEAD op fails.
    Session& session = sessions_.add(pending.sessionId, std::move(pending.resolvedPub), std::move(pending.auth), nowNs,
                                      pending.egressChannel, pending.egressStreamId,
                                      pending.clientVersion);
    session.sourceHost = std::move(pending.sourceHost);
    // The TLS handshake took time: the login deadline runs from the moment the session exists.
    if (auto* first = std::get_if<rune::gateway::AuthAwaitingFirst>(&session.auth)) first->deadlineNs = nowNs + authTimeoutNs_;
    session.crypto.cipher = keys.cipher;
    session.crypto.recv.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);  // OPENs client->broker
    session.crypto.send.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);  // SEALs broker->client
    session.crypto.encrypted = true;
    // Task 19 (per-worker egress crypto addendum): retain the RAW b2c key +
    // prefix (session.crypto.send above already holds the AeadCipher built
    // from these same bytes) so the gateway can derive each shard worker's
    // own per-sender key (shard mode only; unread in legacy mode).
    session.crypto.b2c = keys.b2c;
    std::memcpy(session.crypto.b2cPrefix, keys.b2cPrefix, 4);
    // Task 22 (shard worker decrypts produce / per-worker ingress): retain
    // the RAW c2b key + prefix the same way (session.crypto.recv above
    // already holds the AeadCipher built from these same bytes) so the
    // gateway can ship this session's SINGLE ingress key to every shard
    // worker (shard mode only; unread in legacy mode).
    session.crypto.c2b = keys.c2b;
    std::memcpy(session.crypto.c2bPrefix, keys.c2bPrefix, 4);
    // Issue #116: a client >= 12 binds each c2b frame to one receiver; the gateway (receiver 0) keeps the raw key.
    session.crypto.receiverKeys = pc::sessionUsesReceiverKeys(pending.clientVersion, pc::kBrokerVersion);

    // Task 19 ★: ship the real per-worker key now. Every worker ALREADY has
    // a "parked" entry for this session from the FIRST (announce-before-
    // accept, pollPendingConnects) wave — sent while sessionEncrypted was
    // known but no real key existed yet — so even if THIS second wave can't
    // fully land on every worker in one shot (a ring momentarily full), no
    // worker can ever mistake this session for plaintext: the first wave's
    // entry already makes it fail-closed (park) until this one lands.
    // Retried from doWork() (retryPendingCryptoAnnounces_) until complete —
    // a liveness concern only, never a safety one, by the above invariant.
    if (shardMode_ && workerPool_ &&
        !sessionAnnouncer_.announce(*workerPool_, pending.sessionId, pending.egressChannel,
                                     pending.egressStreamId, pending.clientVersion,
                                     /*sessionEncrypted=*/true, /*keyReady=*/true,
                                     session.crypto.b2c, session.crypto.b2cPrefix, session.crypto.cipher,
                                     session.crypto.c2b, session.crypto.c2bPrefix)) {
        pendingCryptoAnnounces_.push_back(pending.sessionId);
    }

    pendingConnects_.erase(it);
}

// abandonRegistration_ — release an addExclusivePublication registration whose owner is dropped while it may still be
// in flight.  Resolve it and drop the shared_ptr (the destructor releases it), or queue it on drainingRegistrations_
// until it resolves — see the drainPendingReleases doc below.  A throw means ERRORED_MEDIA_DRIVER: the conductor
// already cleaned up, nothing leaks.  regId < 0 = nothing was registered.
void ClientGateway::abandonRegistration_(int64_t regId) noexcept
{
    if (regId < 0 || !aeron_) return;
    try {
        auto pub = aeron_->findExclusivePublication(regId);
        if (!pub) drainingRegistrations_.push_back(regId);   // still AWAITING_MEDIA_DRIVER: keep draining
    } catch (...) {}
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
    if (!msgOpt) {
        noteUnanswerable_(buf, len, "Keepalive body undecodable (no response type)");
        return;
    }
    sessions_.touch(msgOpt->sessionId, nowNs);
}

// ---------------------------------------------------------------------------
// handlePing
// ---------------------------------------------------------------------------
void ClientGateway::handlePing(const uint8_t* buf, std::size_t len, int64_t nowNs)
{
    auto pingOpt = pc::Ping::decode(buf, len);
    if (!pingOpt) {
        // No-hang (R2): answered ErrorResponse{InvalidRequest} (the client
        // completes its pending ping with that error).
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "Ping body undecodable");
        return;
    }

    Session* session = sessions_.find(pingOpt->sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "Ping for unknown session");
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
    if (needed > scratch_.size()) {
        // No-hang (R2): an echo the Pong cannot carry is answered, not dropped.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::MessageTooLarge),
                             "Ping echo exceeds the Pong scratch");
        return;
    }

    const std::size_t encoded = pong.encode(scratch_.data());
    // No-hang (R2): queued under back-pressure, never a best-effort drop.
    sendAnswer_(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// handleDisconnect
// ---------------------------------------------------------------------------
void ClientGateway::handleDisconnect(const uint8_t* buf, std::size_t len)
{
    auto msgOpt = pc::Disconnect::decode(buf, len);
    if (!msgOpt) {
        noteUnanswerable_(buf, len, "Disconnect body undecodable (no response type)");
        return;
    }
    (void)closeSession_(msgOpt->sessionId);
}

// ---------------------------------------------------------------------------
// sendTo — tryClaim first, fallback to offer (mirrors AeronMessageSink).
// ---------------------------------------------------------------------------
bool ClientGateway::sendTo(Session& session, const uint8_t* buf, std::size_t len)
{
    // client<->broker TLS/AEAD (Task 8): encrypt AFTER the response body is
    // encoded (every caller already encoded into scratch_/its own buffer),
    // BEFORE handing bytes to the egress. sendTo is the single chokepoint
    // every Session-aware broker->client send path funnels through
    // (sendRespReliable_/sendReliable_/offerBytes_, sendControlFrame,
    // sendAnswer_, drainPendingResponses, trySendOwedPushError_,
    // handlePing, sendSessionExpired_) — so sealing here, once, covers all of
    // them with no per-caller duplication (Rule 3: one state, one code path).
    // `seq=++crypto.sendSeq`, encode CryptoSubHeader, seal into a reusable
    // grow-once scratch, emit [24B header][12B subheader][ciphertext][16B
    // tag]. Drop (never transmit) on seal failure (sealResponseFrame
    // returns 0). Plaintext path (crypto off) is byte-identical to before
    // Task 8 — buf/len are reassigned only on a successful seal.
    //
    // NOTE: the raw sendOnPub() call sites that do NOT go through sendTo
    // (handleConnect/handleHandshakeContinue_'s ConnectResponse/
    // HandshakeContinueResponse/HandshakeFailed legs, and the redirect/
    // sendSessionExpired_ fallback when no Session exists yet) are
    // intentionally left untouched: those are either the handshake legs
    // themselves (must stay plaintext — keys are not yet installed, or are
    // being installed by this very frame) or genuinely session-less (no
    // CryptoState to seal under).
    if (session.crypto.encrypted) {
        const std::size_t need = len + rune::protocol::client::kCryptoOverhead;
        if (sealScratch_.size() < need) {
            try {
                sealScratch_.resize(need);
            } catch (...) {
                return false;  // scratch exhausted — drop, never transmit
            }
        }
        const std::size_t sealedLen = rune::gateway::codec::sealResponseFrame(
            session.crypto, buf, len, sealScratch_.data(), sealScratch_.size());
        if (sealedLen == 0) return false;  // seal failure — never transmit
        buf = sealScratch_.data();
        len = sealedLen;
    }

    // Guard against null egress (test sessions injected via injectTestSession
    // have no real ExclusivePublication).
    if (!session.egress) {
        // CG-4 test hook: when a null-egress callback is installed, fire it and
        // return true so callers (pushSubscriptions) advance nextOffset — this lets
        // tests drive the real push path and capture Records without a live Aeron driver.
        if (onEgressFrame_) {
            onEgressFrame_(session.sessionId, buf, len);
            return true;
        }
        return false;
    }
    return sendOnPub(*session.egress, buf, len);
}

// ---------------------------------------------------------------------------
// sendOnPub — send on a raw ExclusivePublication (used by redirect path).
// ---------------------------------------------------------------------------
bool ClientGateway::sendOnPub(aeron::ExclusivePublication& pub,
                              const uint8_t*               buf,
                              std::size_t                  len)
{
    if (!pub.isConnected()) {
        // FR: refused, not connected (arg 1 = NOT_CONNECTED) — throttled.
        FLIGHT_RECORDER_THROTTLED_PRODUCE(frOfferRefused_, flight_recorder::Site::GatewayOfferRefused,
                                  static_cast<uint32_t>(pub.streamId()), 1);
        return false;
    }

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
    if (result <= 0)   // FR: refused (arg = -position) — throttled, retried by the caller
        FLIGHT_RECORDER_THROTTLED_PRODUCE(frOfferRefused_, flight_recorder::Site::GatewayOfferRefused,
                                  static_cast<uint32_t>(pub.streamId()), static_cast<uint16_t>(result < 0 ? -result : 0));
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
void ClientGateway::setWorkerPool(rune::node::WorkerPool* pool) noexcept {
    workerPool_ = pool;
    if (!pool) return;
    for (int32_t i = 0; i < pool->workerCount(); ++i)
        maxProduceBatchBytes_ = std::min(maxProduceBatchBytes_, pool->worker(i).maxIntakeBatchBytes());
}

void ClientGateway::sendErrorResponse_(Session& session, int64_t sessionId, int64_t correlationId,
                                       rune::protocol::client::ErrorCode code)
{
    namespace pc = rune::protocol::client;
    pc::ErrorResponse errResp;
    errResp.sessionId     = sessionId;
    errResp.correlationId = correlationId;
    errResp.code          = static_cast<int16_t>(code);
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2;
    if (needed <= scratch_.size()) {
        const std::size_t encoded = errResp.encode(scratch_.data());
        // G0.1: reliable delivery (retry on back-pressure).
        sendReliable_(session, scratch_.data(), encoded);
    }
}

// ---------------------------------------------------------------------------
// No-hang wave (R2): rejectRequest_ / noteUnanswerable_ — see the declaration.
// Both run only after a decode / admission failure (cold path).  No
// allocation: the answer is encoded into scratch_ (or by sendRespReliable_);
// the log is rate-limited (first, then every 1024th) and formats integers only.
// ---------------------------------------------------------------------------
// VLS B.3: a view sink is a catalog-only namespace -- never a data-plane target.  The refusal is permanent
// (InvalidRequest, non-retriable) and comes AFTER authorization (a hidden id still answers like a missing one).
bool ClientGateway::refuseViewSink_(const uint8_t* buf, std::size_t len, int32_t topicId, const char* what)
{
    if (topicRegistry_ == nullptr || !topicRegistry_->isViewSink(topicId)) [[likely]] return false;
    if (viewSinkRefusals_++ % 1024 == 0)
        spdlog::warn("ClientGateway: {} on view sink id={} refused InvalidRequest (a view sink owns no log; "
                     "refusals={})", what, topicId, viewSinkRefusals_);
    (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), what);
    return true;
}

bool ClientGateway::rejectRequest_(const uint8_t* buf, std::size_t len, uint16_t code,
                                   const char* why)
{
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) { noteUnanswerable_(buf, len, why); return false; }
    Session* session = sessions_.find(hdr->sessionId);
    if (!session) { noteUnanswerable_(buf, len, why); return false; }
    if (!answerRejection_(*session, *hdr, code, why, buf, len)) { noteUnanswerable_(buf, len, why); return false; }
    return true;
}

// The typed answer itself (own response type), or false when the request type
// has none.  Old clients (< kClientVersionInvalidRequest) re-join forever on an
// InvalidRequest / Internal JoinGroup / SyncGroup answer, so they get
// ProtocolVersion there — which they already treat as non-retriable.
bool ClientGateway::answerRejection_(Session& session, const pc::MessageHeader& hdr, uint16_t code,
                                     const char* why, const uint8_t* buf, std::size_t len)
{
    const uint16_t tid = hdr.templateId;
    if (!pc::clientUnderstandsInvalidRequest(session.clientVersion) &&
        (tid == static_cast<uint16_t>(pc::TemplateId::JoinGroup) ||
         tid == static_cast<uint16_t>(pc::TemplateId::SyncGroup))) {
        code = static_cast<uint16_t>(pc::ErrorCode::ProtocolVersion);
    }
    // Fix round 4 (item 8): an old client stays Stable on a Heartbeat /
    // OffsetFetch Internal / InvalidRequest (and re-fetches forever) — it gets
    // UnknownMember, made true by evicting its members: it re-joins.
    if (!pc::clientUnderstandsInvalidRequest(session.clientVersion) &&
        (tid == static_cast<uint16_t>(pc::TemplateId::Heartbeat) ||
         tid == static_cast<uint16_t>(pc::TemplateId::OffsetFetch)) &&
        (code == static_cast<uint16_t>(pc::ErrorCode::Internal) ||
         code == static_cast<uint16_t>(pc::ErrorCode::InvalidRequest))) [[unlikely]] {
        code = static_cast<uint16_t>(pc::ErrorCode::UnknownMember);
        evictOldClientMember_(session, tid, buf, len, nowNs_);
        ++oldClientUnknownMemberAnswers_;
    }
    if (tid == static_cast<uint16_t>(pc::TemplateId::Produce)) {
        // Version-gated frame (pre-D6 38 B for old clients), queued under back-pressure.
        ++rejectedRequests_;
        (void)sendRespReliable_(session, static_cast<pc::ErrorCode>(code), /*partition=*/-1,
                                /*baseOffset=*/0, hdr.correlationId, /*logStartOffset=*/-1);
        return true;
    }
    const std::size_t n = encodeRejection(tid, hdr.sessionId, hdr.correlationId, code,
                                          scratch_.data(), scratch_.size());
    if (n == 0) return false;
    ++rejectedRequests_;
    if (rejectedRequests_ % 1024 == 1) {
        spdlog::warn("ClientGateway: request tid={} session={} corr={} rejected ({}): answered code {} "
                     "(rejections={})", tid, hdr.sessionId, hdr.correlationId, why, code, rejectedRequests_);
    }
    sendAnswer_(session, scratch_.data(), n);
    return true;
}

void ClientGateway::evictOldClientMember_(Session& session, uint16_t tid, const uint8_t* buf, std::size_t len,
                                          int64_t nowNs)
{
    if (!groupService_) return;
    const auto sv = [](std::span<const uint8_t> b) {
        return std::string_view(reinterpret_cast<const char*>(b.data()), b.size());
    };
    // Phase 2 Task 5 (D19): the service keys groups by their scoped id.  Cold path (an old client's error answer).
    const rune::coordination::NsRequestContext nsCtx{sessionNsId_(session), permsOf_(session).isRoot(), &namespaces_.registry()};
    if (tid == static_cast<uint16_t>(pc::TemplateId::Heartbeat)) {
        if (const auto m = pc::Heartbeat::decode(buf, len)) {
            groupService_->evictMember(rune::gateway::scopeClientId(nsCtx, sv(m->groupId)), sv(m->memberId), session.sessionId, nowNs);
            return;
        }
    } else if (tid == static_cast<uint16_t>(pc::TemplateId::SyncGroup)) {
        if (const auto m = pc::SyncGroup::decode(buf, len)) {
            groupService_->evictMember(rune::gateway::scopeClientId(nsCtx, sv(m->groupId)), sv(m->memberId), session.sessionId, nowNs);
            return;
        }
    } else if (tid == static_cast<uint16_t>(pc::TemplateId::OffsetFetch)) {
        if (const auto m = pc::OffsetFetch::decode(buf, len)) {
            groupService_->evictMember(rune::gateway::scopeClientId(nsCtx, sv(m->groupId)), std::string_view{}, session.sessionId, nowNs);
            return;
        }
    }
    // Undecodable body: the group is unknown — the session's members (counted apart).
    groupService_->evictSessionMembers(session.sessionId, nowNs);
    ++oldClientSessionWideEvictions_;
}

void ClientGateway::noteUnanswerable_(const uint8_t* buf, std::size_t len, const char* why)
{
    if (unanswerableFrames_++ % 1024 != 0) return;
    const auto hdr = pc::MessageHeader::decode(buf, len);
    spdlog::warn("ClientGateway: unanswerable frame dropped ({}): len={} tid={} session={} "
                 "(header {}; dropped so far={})", why, len, hdr ? hdr->templateId : 0,
                 hdr ? hdr->sessionId : 0, hdr ? "decoded, no session / no response type" : "undecodable",
                 unanswerableFrames_);
}

// ---------------------------------------------------------------------------
// Phase 2b E2: servedPartition_ — see ServedPartition.  The disk fast path is the
// same single servableLog() probe every site made before; the C1 / topic /
// replica probes run only when there is no servable disk Log.  Control thread.
// ---------------------------------------------------------------------------
ClientGateway::ServedPartition ClientGateway::servedPartition_(int32_t topicId, int32_t partition,
                                                               const RoutedEntry* routed) const noexcept
{
    using Kind = ServedPartition::Kind;
    ServedPartition s;
    if (topicRegistry_ == nullptr) return s;
    if (rune::storage::Log* log = topicRegistry_->servableLog(topicId, partition)) [[likely]] {
        // Task 8 (C1 generalized): a catalog-applied topic serves only once its
        // placement is committed too.  After Task 9 every user topic is a catalog
        // topic: this runs on every produce / subscribe — two byte reads on the
        // entry servableLog loaded, plus the routing entry step 3b already looked
        // up (`routed`: never a second lookup on the data path).
        if (topicRegistry_->placementGated(topicId) && !placementCommitted_(topicId, partition, routed)) {
            s.kind = Kind::NotPlaced;
            return s;
        }
        s.kind = Kind::Disk;
        s.log  = log;
        return s;
    }
    const rune::node::TopicSpec* spec = topicRegistry_->topic(topicId);
    if (spec == nullptr || spec->persistence != rune::node::PersistenceMode::Transient) return s;
    if (partition < 0 || partition >= spec->partitionCount) return s;
    if (topicRegistry_->placementGated(topicId) && !placementCommitted_(topicId, partition, routed)) {
        s.kind = Kind::NotPlaced;   // Task 8 (C1 generalized), transient too
        return s;
    }
    if (replicaRegistry_ != nullptr && replicaRegistry_->lookup(topicId, partition) != nullptr)
        s.kind = Kind::Transient;
    return s;
}

bool ClientGateway::placementCommitted_(int32_t topicId, int32_t partition, const RoutedEntry* routed) const noexcept
{
    // Fix round 1 (I1): only an image built from the COMMITTED assignment map
    // names committed placements — never the static-balancer image (bootstrap,
    // or after the last user assignment was removed), whatever
    // committedMetadataReady_ latched.
    if (!routingImage_ || !routingImage_->committed()) return false;
    const RoutedEntry& e = routed != nullptr ? *routed : routingImage_->lookup(rune::metadata::gpidOf(topicId, partition));
    return e.leaderNodeId != rune::metadata::RoutingImage::NO_LEADER_NODE_ID;
}

bool ClientGateway::topicAssignable(int32_t topicId) const noexcept
{
    if (topicRegistry_ == nullptr) return true;
    const rune::node::TopicSpec* spec = topicRegistry_->topic(topicId);
    if (spec == nullptr) return false;
    if (!topicRegistry_->placementGated(topicId)) return true;   // a static topic serves from boot
    for (int32_t p = 0; p < spec->partitionCount; ++p)
        if (!placementCommitted_(topicId, p)) return false;
    return true;
}

pc::ErrorCode ClientGateway::unappliedTopicCode_(int32_t topicId, int32_t partition) const noexcept
{
    using St = rune::gateway::TopicRegistry::TopicState;
    const St st = topicRegistry_ != nullptr ? topicRegistry_->state(topicId) : St::Unknown;
    if (st == St::Dropped) return pc::ErrorCode::UnknownTopic;
    if (st == St::Pending) return pc::ErrorCode::NotLeader;   // applied, waits behind a drop (fix round 1, C1)
    return placementCommitted_(topicId, partition < 0 ? 0 : partition) ? pc::ErrorCode::NotLeader
                                                                       : pc::ErrorCode::UnknownTopic;
}

std::size_t ClientGateway::endTopicSubscriptions(int32_t topicId)
{
    std::size_t ended = 0;
    const bool workerSubs = perWorkerEgressActive_ && workerPool_;
    for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
        const int64_t subId = it->first;
        Subscription& sub   = it->second;
        if (sub.topicId != topicId) { ++it; continue; }
        if (sub.isGroup) {
            // Fix round 1 (I3): a group-created subscription has no Subscribe of
            // its own (correlation id 0) — its member learns the drop from its
            // GROUP: the coordinator rebalances the dropped partitions away
            // (GroupCoordinationService::onTopicDropped, revoke + new Assign).
            // Here only the subscription and its member tracking go.
            if (auto ms = groupMemberSubs_.find(sub.sessionId); ms != groupMemberSubs_.end()) {
                std::erase(ms->second, subId);
                if (ms->second.empty()) groupMemberSubs_.erase(ms);
            }
        } else if (Session* session = sessions_.find(sub.sessionId)) {
            pc::ErrorResponse err;
            err.sessionId     = sub.sessionId;
            err.correlationId = sub.subscribeCorrId;
            err.code          = static_cast<int16_t>(pc::ErrorCode::UnknownTopic);
            err.message       = "topic dropped";
            const std::size_t encoded = err.encode(scratch_.data());
            sendAnswer_(*session, scratch_.data(), encoded);   // queued + retried, never best-effort
            if (const auto ix = session->subIdByKey.find(Session::SubKey{sub.subscribeCorrId, sub.topicId,
                                                                         sub.partition});
                ix != session->subIdByKey.end() && ix->second == subId)
                session->subIdByKey.erase(ix);
        }
        if (workerSubs) forwardUnsubscribeToWorker_(subId, sub.topicId, sub.partition);
        pendingWorkerReads_.erase(subId);
        it = subscriptions_.erase(it);
        ++ended;
    }
    if (ended > 0)
        spdlog::info("ClientGateway: topic id={} dropped — {} subscription(s) ended with UnknownTopic",
                     topicId, ended);
    return ended;
}

void ClientGateway::forgetDroppedTopic(int32_t topicId, int32_t partitions,
                                       std::span<rune::storage::Log* const> logs)
{
    std::erase(viewSinksAwaitingPlan_, topicId);   // Task 12 (N5): a dropped sink waits for nothing
    forgetViewSource(topicId);                     // Task 22 fix round 1 (C1): no source to route a read to
    for (int32_t p = 0; p < partitions; ++p) {
        partitionTxnStates_.erase(partitionKey(topicId, p));
        if (const auto it = inlinePsm_.find(inlinePsmKey_(topicId, p)); it != inlinePsm_.end()) {
            if (std::holds_alternative<InlinePsmScanning>(it->second)) --inlinePsmScanning_;
            inlinePsm_.erase(it);
        }
    }
    for (rune::storage::Log* l : logs) {
        if (l == nullptr) continue;
        workerOwnedLogs_.erase(l);
        dirtyLogs_.erase(l);
    }
}

bool ClientGateway::readCommittedServableHere_(int32_t topicId, int32_t partition) const noexcept
{
    if (replicaRegistry_ == nullptr || topicId < 0) return true;   // unreplicated / internal: unchanged
    const GatewayReplicaView v{replicaRegistry_->lookupHandle(topicId, partition)};
    return !v || v.publishedLeader();
}

void ClientGateway::scanMovedAwaySlice_()
{
    const std::size_t nb = subscriptions_.bucket_count();
    if (nb == 0) return;
    // E2 final fix wave (N1): the budget counts actual movedAway_ PROBES (one
    // replica-registry lookup each) and the slice stops the moment it is
    // spent — mid-bucket if need be (movedAwayBucketPos_ = the survivors of
    // the current bucket already visited), so a colliding bucket can never
    // exceed it.  A retired (erased) element does not advance the position;
    // a rehash between slices only moves where the walk resumes — every
    // subscription is still reached on a later lap (and the routed sweep
    // covers them all).
    int budget = kMovedAwayLookupsPerSlice;
    for (std::size_t visited = 0; visited < nb && visited < kMovedAwayBucketsPerSlice && budget > 0; ++visited) {
        if (movedAwayBucket_ >= nb) { movedAwayBucket_ = 0; movedAwayBucketPos_ = 0; }
        const std::size_t b = movedAwayBucket_;
        auto it = subscriptions_.begin(b);
        std::size_t pos = 0;
        for (; it != subscriptions_.end(b) && pos < movedAwayBucketPos_; ++it) ++pos;   // resume mid-bucket
        bool spent = false;
        while (it != subscriptions_.end(b)) {
            if (budget == 0) { spent = true; break; }
            const int64_t subId = it->first;
            Subscription& sub   = it->second;
            ++it;   // before a possible erase (erase invalidates only that element)
            if (!movedAwayEligible_(sub)) { ++pos; continue; }
            --budget;
            if (movedAway_(sub) && retireReadCommittedMovedAway_(subId, sub)) {
                subscriptions_.erase(subId);   // the position stays: the next survivor takes its place
                continue;
            }
            ++pos;
        }
        if (spent) { movedAwayBucketPos_ = pos; break; }
        ++movedAwayBucket_;
        movedAwayBucketPos_ = 0;
    }
    const int probes = kMovedAwayLookupsPerSlice - budget;
    if (probes > movedAwayMaxProbesPerSlice_) movedAwayMaxProbesPerSlice_ = probes;
}

void ClientGateway::logMovedAwaySummary_()
{
    spdlog::info("ClientGateway: {} subscription(s) this node can no longer serve (demoted / partition moved "
                 "away) answered NotLeader (v6: Redirect to a known new leader) and dropped since the last "
                 "summary ({} in total); the clients "
                 "re-subscribe from their positions on the leaders",
                 movedAwaySinceSummary_, readCommittedMovedAway_);
    ++movedAwaySummaries_;
    lastMovedAwaySummaryCount_ = movedAwaySinceSummary_;
    movedAwaySinceSummary_     = 0;
    nextMovedAwaySummaryNs_    = nowNs_ + kMovedAwaySummaryEveryNs;
}

bool ClientGateway::movedAway_(Subscription& sub) const noexcept
{
    const GatewayReplicaView v{replicaRegistry_->lookupHandle(sub.topicId, sub.partition)};
    if (!v) return sub.hadReplica;   // removed from this node since it (or a sweep) saw one
    // E2 final fix wave (M5): a subscription accepted BEFORE this node got a
    // replica of the partition (a reassignment adding the node later) learns it
    // here, the first time a sweep sees the registered replica — so that
    // replica's later removal retires it instead of leaving it silent.
    sub.hadReplica = true;
    return sub.isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted) &&
           !v.publishedLeader();
}

bool ClientGateway::retireReadCommittedMovedAway_(int64_t subId, Subscription& sub)
{
    // Precondition (both callers gate on movedAwayEligible_): never a
    // coordinator-created group subscription — those are revoked by the group
    // coordinator, not retired here (E2 final fix wave: was a dead `!isGroup`
    // term in the Redirect condition below).
    assert(!sub.isGroup && "retireReadCommittedMovedAway_: a coordinator-created group subscription");
    Session* session = sessions_.find(sub.sessionId);
    if (session == nullptr) return false;   // dies with its session (dropSessionSubscriptions_)
    // Fix round 2 (M3): first the POSITION NOTE (Protocol.h, client version 6)
    // — an empty-batch Records frame of this subscription carrying this
    // broker's cursor (in shard mode the bind-time resolved start; legacy, the
    // live cursor), so a subscription that delivered nothing resumes there on
    // the new leader instead of re-resolving Latest (records produced around
    // the move would be skipped).  Ordered before the NotLeader on the same
    // reliable session egress.  Older clients ignore an empty batch.
    if (pc::clientUnderstandsMovedPositionNote(session->clientVersion)) {
        pc::Records note;
        note.sessionId      = sub.sessionId;
        note.correlationId  = sub.subscribeCorrId;
        note.subscriptionId = subId;
        note.topicId        = sub.topicId;
        note.partition      = sub.partition;
        note.baseOffset     = sub.nextOffset;
        note.recordBatch    = {};
        const std::size_t n = note.encode(scratch_.data());
        sendAnswer_(*session, scratch_.data(), n);
    }
    pc::ErrorResponse err;
    err.sessionId     = sub.sessionId;
    err.correlationId = sub.subscribeCorrId;
    err.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
    static_assert(pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2 <= kScratchSize,
                  "scratch too small for a bare ErrorResponse");
    // E2 Task 14 (3-node e2e): a plain subscription rides the client's SEED
    // session, which followed the subscribe Redirect to this node.  A bare
    // NotLeader makes the client re-arm it on that same session and wait for
    // metadata over it — stranded when this node is stopping (controlled
    // shutdown).  When the routing image already names ANOTHER leader with a
    // known client endpoint, a v6 session is answered Redirect{endpoint}: the
    // client follows it per partition (that leader's own session; the seed is
    // never retargeted — E2 final fix wave I2) and re-subscribes from its
    // position.  Group-member partition subscriptions (bit-61 correlation ids)
    // ride per-leader sessions and keep the NotLeader their re-route handles.
    constexpr int64_t kGroupFetchCorrTag = int64_t{1} << 61;   // rune-client GroupConsumerHub
    if (pc::clientUnderstandsMovedPositionNote(session->clientVersion) &&
        (sub.subscribeCorrId & kGroupFetchCorrTag) == 0 && routingImage_) {
        const auto& entry = routingImage_->lookup(rune::metadata::gpidOf(sub.topicId, sub.partition));
        if (entry.leaderNodeId != rune::metadata::RoutingImage::NO_LEADER_NODE_ID &&
            entry.leaderNodeId != thisNodeId_) {
            if (const auto ep = nodeEndpointMap_.find(entry.leaderNodeId);
                ep != nodeEndpointMap_.end() && !ep->second.empty() &&
                pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2 + ep->second.size() <=
                    scratch_.size()) {
                err.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
                err.message = ep->second;
            }
        }
    }
    const std::size_t encoded = err.encode(scratch_.data());
    sendAnswer_(*session, scratch_.data(), encoded);   // queued + retried, never best-effort
    if (perWorkerEgressActive_ && workerPool_)
        forwardUnsubscribeToWorker_(subId, sub.topicId, sub.partition);   // the worker drops its cursor
    pendingWorkerReads_.erase(subId);   // a PushRead in flight answers a sub that is gone (ignored)
    if (const auto ix = session->subIdByKey.find(Session::SubKey{sub.subscribeCorrId, sub.topicId, sub.partition});
        ix != session->subIdByKey.end() && ix->second == subId)
        session->subIdByKey.erase(ix);
    ++readCommittedMovedAway_;
    ++movedAwaySinceSummary_;   // one rate-limited info summary (logMovedAwaySummary_)
    spdlog::debug("ClientGateway: subscription {} (sessionId={} topicId={} partition={} isolation={} "
                 "nextOffset={}) — this node no longer serves it (demoted / partition moved away): {}, "
                 "dropped (the client re-subscribes from its position on the leader)",
                 subId, sub.sessionId, sub.topicId, sub.partition, sub.isolation, sub.nextOffset,
                 err.code == static_cast<int16_t>(pc::ErrorCode::Redirect) ? "Redirect" : "NotLeader");
    return true;
}

bool ClientGateway::handleProduce(const uint8_t* buf, std::size_t len)
{
    // 1. Decode Produce.
    auto msgOpt = pc::Produce::decode(buf, len);
    if (!msgOpt) [[unlikely]] {
        // No-hang (R2): answered InvalidRequest on the header's correlationId.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                             "Produce body undecodable");
        return true;
    }
    const pc::Produce& msg = *msgOpt;
    FLIGHT_RECORDER_SPAN_PRODUCE(flight_recorder::Site::GatewayHandle, flight_recorder::foldCorr(msg.correlationId));

    // 2. Require known session.
    Session* session = sessions_.find(msg.sessionId);
    if (!session) [[unlikely]] {
        noteUnanswerable_(buf, len, "Produce for unknown session");
        return true;
    }
    // F23: any request is liveness (Kafka: idle == no requests).  Reuses the
    // lookup above — no extra cost on the produce hot path.
    session->lastSeenNs = nowNs_;
    // Review C I3: the auth state is checked HERE, on the session already looked up (Task 12's PermissionTable::allows
    // joins this spot).  AwaitingFirst / AwaitingFinal / PasswordChangeRequired (variant index < 3) never produce.
    static_assert(std::is_same_v<std::variant_alternative_t<3, rune::gateway::SessionAuth>, rune::gateway::AuthAuthenticated>);
    if (session->auth.index() < 3) [[unlikely]] {
        // Namespaces Task 9: a first request that is a Produce makes the anonymous reader too (then 45 below, the
        // session stays) - the same outcome as a Produce after a metadata request.
        if (becomeAnonymousIfAllowed_(*session, pc::TemplateId::Produce)) {   // Task 10 (#236): the anonymous-connection cap
            if (!answerRejection_(*session, *pc::MessageHeader::decode(buf, len), nsLimitCode_(*session),
                                  "namespace limit exceeded: anonymous connections", buf, len))
                noteUnanswerable_(buf, len, "anonymous connection cap");
            return true;
        }
        if (std::holds_alternative<rune::gateway::AuthAnonymousRead>(session->auth) &&
            anonOpsRefused_(*session, *pc::MessageHeader::decode(buf, len), buf, len)) return true;
        if (session->auth.index() < 3) {
            const auto hdr = pc::MessageHeader::decode(buf, len);
            answerGate_(*session, *hdr, rune::gateway::gateFor(session->auth, pc::TemplateId::Produce, false, false), buf, len);
            return true;
        }
    }
    // Issue #603: the view is behind committed security events this broker holds - its grants may already be revoked.
    // One predictable branch on a control-thread bool.
    if (securityBehind_) [[unlikely]] {
        answerSecurityBehind_(*session, *pc::MessageHeader::decode(buf, len), buf, len);
        return true;
    }
    // Namespaces Phase 2 Task 3 (D21): a suspended namespace takes no produce (one byte compare; the refusal is cold).
    if (session->nsGate != rune::catalog::NsStateTag::Active) [[unlikely]]
        if (!gateExempt_(*session)) {
            answerSuspended_(*session, *pc::MessageHeader::decode(buf, len), buf, len);
            return true;
        }
    // Namespaces Phase 2 Task 8 (D22): the session's (nsId, nsSlot) must still be what the slot table holds - one array
    // index and a compare.  A stale tag is refused and counted, never applied to another namespace.
    if (!nsTagMatches_(*session)) [[unlikely]] {
        if (!answerRejection_(*session, *pc::MessageHeader::decode(buf, len),
                              static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "namespace slot mismatch", buf, len))
            noteUnanswerable_(buf, len, "namespace slot mismatch");
        return true;
    }
    // Namespaces Phase 2 Task 9 (D23): the acks floor of the session's namespace, per produce (a client picks acks per
    // request): one slot-array load and a compare.  wire acks 0 = Leader, 1 = All; lowestAcks >= 2 requires All.
    if (!rune::node::nsAcksAllowed(nsSlots_.limits(session->nsSlot), msg.acks)) [[unlikely]] {
        if (!answerRejection_(*session, *pc::MessageHeader::decode(buf, len), nsLimitCode_(*session),
                              "namespace limit exceeded: acks below the namespace's lowest acks", buf, len))
            noteUnanswerable_(buf, len, "namespace acks floor");
        return true;
    }
    // Authentication Task 12 (D30, spec §2.1): WRITE on the topic - ONE index test, ONE bounds check, ONE byte load
    // (PermissionTable::allows).  permsOf_ is allow-all only on a dev / in-process broker; an AuthAnonymous session on
    // an authenticating broker gets the deny-all table (X013), so nothing is produced for it.  The denial path is cold.
    {
        const rune::gateway::PermissionTable& perms = permsOf_(*session);
        if (!perms.allows(msg.topicId, rune::security::opBit(rune::security::Operation::Write))) [[unlikely]]
            if (deny_(*session, authorizeTopicIdHere_(perms, msg.topicId, msg.partitionKnown != 0 ? msg.partition : -1,
                                                      rune::security::Operation::Write), buf, len))
                return true;
    }
    // Issue #122: a producer id is honoured only for the principal it was issued to (the owner tag in the id, see
    // rune/protocol/ProducerIdBinding.h) - one mask and one compare, no lookup.  Another principal's id is refused
    // whatever its state (nothing to probe), before any producer state is touched.
    if (msg.producerId >= 0 && authMode_ == rune::gateway::GatewayAuthMode::Required) {
        const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&session->auth);
        if (!rune::protocol::producerIdUsableBy(msg.producerId, a != nullptr ? a->userId : 0)) [[unlikely]]
            if (deny_(*session, refusedProducerIdDenial_(msg.producerId), buf, len)) return true;
    }
    if (refuseViewSink_(buf, len, msg.topicId, "Produce to a view sink")) [[unlikely]] return true;   // VLS B.3
    // G0.1: capture session id so the public sendRespReliable() test-seam knows
    // which session to target for the duration of this handleProduce() call.
    currentProduceSessionId_ = msg.sessionId;

    // GB-4 / MD1-9: Leadership check — data ops MUST reach the partition leader.
    //
    // Two-path routing decision:
    //   (A) Per-partition routing (MD1 Task 9): when routingImage_ is set, we
    //       resolve the partition first, then look up the routing image to determine
    //       whether this node is the partition leader.  If not → REDIRECT to that
    //       partition's leader endpoint.  This replaces the cluster-level boolean
    //       check for the per-partition case.
    //
    //   (B) Cluster-level fallback: when routingImage_ is null (no metadata service
    //       wired in yet, or single-node bypass), fall back to the original
    //       leaderView_-based check for backward compatibility.
    //
    // Helper: send a redirect/notleader error response (used by both paths).
    // G0.1: also routes through the retry queue on back-pressure.
    auto sendRedirect = [&](const std::string& endpoint, int64_t corrId) {
        pc::ErrorResponse errResp;
        errResp.sessionId     = msg.sessionId;
        errResp.correlationId = corrId;
        if (!endpoint.empty()) {
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
            errResp.message = endpoint;
            spdlog::debug("ClientGateway: Produce per-partition redirect sessionId={} → {}",
                          msg.sessionId, endpoint);
        } else {
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            spdlog::debug("ClientGateway: Produce per-partition NotLeader (endpoint unknown) sessionId={}",
                          msg.sessionId);
        }
        const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                   pc::ErrorResponse::BODY_FIXED + 2 +
                                   errResp.message.size();
        if (needed > scratch_.size()) [[unlikely]] {
            // Fix round 2: an endpoint too long for the frame is never dropped —
            // the client gets the bare NotLeader (it refreshes metadata, retries).
            errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
            errResp.message = {};
        }
        const std::size_t encoded = errResp.encode(scratch_.data());
        // G0.1: the single reliable-egress primitive (spilled under back-pressure).
        sendReliable_(*session, scratch_.data(), encoded);
    };

    // Cluster-level fallback (path B): only when routingImage_ is null.
    if (!routingImage_ && leaderView_) {
        const LeaderView lv = leaderView_();
        if (!lv.isLeader) {
            sendRedirect(lv.leaderClientEndpoint, msg.correlationId);
            return true;  // engine untouched
        }
    }

    // G0.1: Helper: send ProduceResponse reliably (retry on back-pressure).
    // Delegates to sendRespReliable_ which encodes once and enqueues on
    // BACK_PRESSURED / ADMIN_ACTION instead of silently dropping the ack.
    auto sendResp = [&](pc::ErrorCode ec, int32_t partition, int64_t baseOffset,
                        int64_t logStartOffset = -1) {
        sendRespReliable_(*session, ec, partition, baseOffset, msg.correlationId, logStartOffset);
    };

    // 3. Resolve partition.
    int32_t partition = msg.partition;
    if (msg.partitionKnown == 0) {
        // Key-based: resolve via TopicRegistry (FNV-1a 64 % partitionCount).
        if (!topicRegistry_) {
            spdlog::warn("ClientGateway: Produce key-partition but no TopicRegistry — UnknownTopic");
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return true;
        }
        partition = topicRegistry_->partitionForKey(msg.topicId, msg.key);
        if (partition < 0) {
            spdlog::debug("ClientGateway: Produce partitionForKey → unknown topicId={}", msg.topicId);
            sendResp(unappliedTopicCode_(msg.topicId, 0), 0, 0);   // Task 8: C1 generalized
            return true;
        }
    }

    // 3b. MD1 Task 9: Per-partition routing image check (path A).
    //
    // When routingImage_ is set, look up the global partition id for this
    // (topicId, partition) to determine the authoritative leader for this
    // partition.  The GPID scheme: gpid = gpidOf(topicId, partition).
    // This must match the scheme used by BrokerNode/MetadataService when building
    // the RoutingImage.
    //
    // Capture-once discipline (MD1-8 ruling):
    //   routingImage_ is a shared_ptr captured ONCE at the point we check it here.
    //   All lookup() calls within this handler use `img` (the raw pointer from the
    //   captured shared_ptr), never calling routingImage_ again.
    //   This avoids per-produce atomic_load (Apple libc++ global lock / Linux CAS).
    //
    // BOOTSTRAP FIX — defense-in-depth (MD1-13):
    //   Even after the primary fix (NodeRuntimeAgent gates setRoutingImageSnapshot
    //   on a non-empty committedMap), a convergence window exists where routingImage_
    //   is non-null but contains NO_LEADER for a partition that is legitimately not
    //   yet assigned (e.g. rolling assignment commit, or a topic added mid-flight).
    //   When that happens, fall back to Path B (cluster-leader check) rather than
    //   returning NotLeader — this avoids redirect-exhaustion for genuinely
    //   bootstrap-window produces.  A steady-state unassigned partition (where the
    //   Raft leader cannot serve it either) will still reject via Path B.
    const RoutedEntry* routed = nullptr;   // Task 8 (M1): reused by servedPartition_ (no 2nd lookup)
    if (routingImage_) {
        // Capture the snapshot once (the shared_ptr was already stored in routingImage_
        // on the duty-cycle thread — no cross-thread atomic_load needed here).
        const rune::metadata::RoutingImage* img = routingImage_.get();
        const int32_t gpid = rune::metadata::gpidOf(msg.topicId, partition);
        const auto& entry = img->lookup(gpid);
        routed = &entry;

        if (entry.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID) {
            // Partition not yet assigned in the routing image — bootstrap window or
            // genuinely unassigned.  Fall back to Path B (cluster-leader fallback)
            // instead of returning NotLeader directly, so the Raft leader can serve
            // the produce if it is the cluster leader and has the log locally.
            // Path B is applied immediately below (outside this if-block) when
            // leaderView_ is set; if leaderView_ is null, we fall through to the
            // log lookup (single-node passthrough, always-leader).
            spdlog::debug("ClientGateway: Produce partition not in routing image "
                          "(topicId={} part={}) — falling back to cluster-leader path, sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            if (leaderView_) {
                const LeaderView lv = leaderView_();
                if (!lv.isLeader) {
                    sendRedirect(lv.leaderClientEndpoint, msg.correlationId);
                    return true;  // engine untouched
                }
            }
            // leaderView_ says we ARE the leader (or is null) — serve locally (fall through).
        } else if (entry.leaderNodeId != thisNodeId_) {
            // Catalog history Task 8: a topic this broker DROPPED is UnknownTopic at
            // once — never a redirect on a placement whose removal has not committed.
            if (topicRegistry_ != nullptr &&
                topicRegistry_->state(msg.topicId) == TopicRegistry::TopicState::Dropped) [[unlikely]] {
                sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
                return true;
            }
            // This partition is led by a different node — REDIRECT.
            const std::string& redirectEndpoint = [&]() -> const std::string& {
                auto it = nodeEndpointMap_.find(entry.leaderNodeId);
                if (it != nodeEndpointMap_.end()) return it->second;
                static const std::string kEmpty;
                return kEmpty;
            }();
            spdlog::debug("ClientGateway: Produce per-partition redirect "
                          "(topicId={} part={} leader={} epoch={}) sessionId={} → {}",
                          msg.topicId, partition, entry.leaderNodeId, entry.leaderEpoch,
                          msg.sessionId, redirectEndpoint);
            sendRedirect(redirectEndpoint, msg.correlationId);
            return true;  // engine untouched
        }
        // entry.leaderNodeId == thisNodeId_ → serve locally (fall through).
    }

    // 4. Lookup Log.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway: Produce but no TopicRegistry — UnknownTopic");
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return true;
    }

    // topic() returns nullptr for unknown topicId.
    const rune::node::TopicSpec* spec = topicRegistry_->topic(msg.topicId);
    if (!spec) {
        // Task 8 (C1 generalized): not applied HERE — NotLeader (retriable) when the
        // MSM already holds its committed placement, UnknownTopic otherwise / dropped.
        const pc::ErrorCode code = unappliedTopicCode_(msg.topicId, partition);
        spdlog::debug("ClientGateway: Produce unknown topicId={} — {}", msg.topicId,
                      code == pc::ErrorCode::NotLeader ? "placed, not applied here: NotLeader" : "UnknownTopic");
        sendResp(code, partition, 0);
        return true;
    }

    // C1 (DDL conformance final review): servableLog() is log() for a topic whose
    // catalog record committed; a runtime topic still awaiting its commit is not
    // writable — answered NotLeader (retriable: the client refreshes metadata, which
    // lists the topic once it commits, and retries).  Nothing is appended, so a
    // topic boot may later reclaim as an orphan never holds an acknowledged record.
    // Phase 2b E2: a TRANSIENT partition has no disk Log — routed to its owning
    // worker below (op.logPtr = nullptr: the worker appends to the replica's ring).
    const ServedPartition served = servedPartition_(msg.topicId, partition, routed);
    if (served.kind == ServedPartition::Kind::NotPlaced) [[unlikely]] {
        spdlog::debug("ClientGateway: Produce to topicId={} awaiting its committed placement — "
                      "NotLeader (retriable)", msg.topicId);
        sendResp(pc::ErrorCode::NotLeader, partition, 0);
        return true;
    }
    if (served.kind == ServedPartition::Kind::Unknown) [[unlikely]] {
        spdlog::debug("ClientGateway: Produce bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }
    rune::storage::Log* log = served.log;   // nullptr ⟺ Kind::Transient

    // GR-4 / F15 §3.1 (I1): per-partition NotLeader.
    //
    // A worker-owned replica's role is the OWNING worker's state: reading
    // role() here (control thread) was a data race and a check-then-act race —
    // the role can change between this check and the worker's append.  The
    // owning worker is the authority (role gate in executeProduceWire_ /
    // handleWorkerIngressFrame_).  The gateway holds only a GatewayReplicaView
    // (no role()) and consults the control-owned intended-role HINT the bridge
    // records when it routes a role op:
    //   - Follower → fast NotLeader (nothing routed);
    //   - Leader or absent → route; W decides.
    // The hint may lead or lag W; both are safe (W is authoritative, both
    // answers are retriable).  The bootstrap window (committedMetadataReady_
    // false, replicas still Follower) is answered NotLeader by W (Q4).
    // The INLINE path below (no worker pool / gateway-owned internal partition)
    // is the replica's owner thread and keeps its own role check there.
    //
    // O(1) lookup (unordered_map): zero per-op alloc, single-writer.
    GatewayReplicaView replica;
    if (replicaRegistry_) {
        replica = GatewayReplicaView{replicaRegistry_->lookupHandle(msg.topicId, partition)};
        if (replica.intendedRole() == GatewayReplicaView::IntendedRole::Follower) [[unlikely]] {
            spdlog::debug("ClientGateway: Produce per-partition NotLeader (intended-role hint) "
                          "(topicId={} partition={}) sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            sendErrorResponse_(*session, msg.sessionId, msg.correlationId, pc::ErrorCode::NotLeader);
            return true;  // engine untouched — no append, nothing routed
        }
    }

    // D8c fix round 1 (MAJOR-1): the owning worker's routed share.  Checked
    // BEFORE any validation work or answer (nothing done yet but liveness), so a
    // deferred frame is simply not consumed: the controlled poll ABORTs it and
    // redelivers it next poll.  Shard mode only (worker-side acks return credits).
    int32_t routedWorker = -1;
    if (shardMode_ && workerPool_ && routesToWorker_(msg.topicId)) {
        routedWorker = workerPool_->ownerWorkerId(msg.topicId, partition);   // router arithmetic
        assert(routedWorker >= 0 && routedWorker < kMaxHeldMaskWorkers);
        if (const WorkerAdmit a = workerAdmitsProduce_(routedWorker, *session); a != WorkerAdmit::Admit) [[unlikely]] {
            ++heldProduceAborts_;
            ++workerCapProduceAborts_;
            // Lows batch C (R3): remember the deferral — the redeliveries are
            // ABORTed in admitClientFrame_ before any decrypt / decode.
            if (session->capDeferredOn < 0) { session->capDeferredOn = static_cast<int8_t>(routedWorker); ++capDeferredSessions_; }
            // May expire a held session — possibly this one: `session` is not used after this.
            if (a == WorkerAdmit::Full) onWorkerCapFull_(routedWorker);
            return false;
        }
    }

    // 5. Assign offsets (CRC-safe restamp).

    // Guard: recordBatch must be present (non-empty).
    const std::size_t batchLen = msg.recordBatch.size();
    if (batchLen == 0) {
        // Fix round 2: a malformed request is InvalidRequest (39, non-retriable),
        // not Internal.
        sendResp(pc::ErrorCode::InvalidRequest, partition, 0);
        return true;
    }

    // Guard: reject an oversized batch LOUDLY (explicit MessageTooLarge), never
    // silently drop.  The zero-copy append path has no fixed scratch cap anymore;
    // this bound only protects the worker ingress ring / log segment from an
    // absurd or malformed length.  The client caps its batch wire-size below this.
    // F15 S3 (review m4 / I-1): the gateway and the worker share these bounds.
    static_assert(kMaxProduceBatchBytes == rune::node::Worker::kMaxProduceBatchBytes,
                  "gateway and shard-ingress max produce batch must match");
    static_assert(kMaxChunkBytes == rune::node::Worker::kMaxPushReadChunkBytes,
                  "gateway and worker PushRead chunk budget must match");
    if (batchLen > maxProduceBatchBytes_) [[unlikely]] {
        // F3 round 6, item 2: rate-limited (first, then every 1024th) —
        // control-thread-owned counter (single-writer).
        if (oversizeProduceRejects_++ % 1024 == 0) {
            ++oversizeProduceRejectLogs_;
            spdlog::warn("ClientGateway: Produce batch too large ({} > {}); rejected "
                         "(MessageTooLarge, rejects={})",
                         batchLen, maxProduceBatchBytes_, oversizeProduceRejects_);
        }
        sendResp(pc::ErrorCode::MessageTooLarge, partition, 0);
        return true;
    }

    // Guard: batch must be at least large enough to hold the fixed header fields
    // we will read (kRecordCountOffset + 4 bytes).
    // M1 fix: also need kLastOffsetDeltaOffset + 4 bytes (kLastOffsetDeltaOffset=23 < 57).
    constexpr std::size_t kMinBatchSize =
        static_cast<std::size_t>(rune::protocol::batch::kRecordCountOffset) + 4u;
    if (batchLen < kMinBatchSize) {
        spdlog::debug("ClientGateway: Produce batch too short ({} < {}); answered InvalidRequest",
                      batchLen, kMinBatchSize);
        sendResp(pc::ErrorCode::InvalidRequest, partition, 0);
        return true;
    }

    // X036: a client never writes a control batch (COMMIT/ABORT markers are the broker's own); any batch of the frame
    // carrying kAttrControl refuses the whole frame, consumed and never appended.
    if (rune::node::Worker::refusesClientBatches(msg.recordBatch.data(), batchLen)) [[unlikely]] {
        if (controlBatchRefusals_++ % 1024 == 0)
            spdlog::warn("ClientGateway: Produce frame is not exactly one non-control batch; refused InvalidRequest "
                         "(session={}, topic={}); total {}", msg.sessionId, msg.topicId, controlBatchRefusals_);
        sendResp(pc::ErrorCode::InvalidRequest, partition, 0);
        return true;
    }

    // Zero-copy: the recordBatch is validated + appended directly from the
    // read-only Aeron fragment (msg.recordBatch) — no restamp scratch copy.  The
    // base offset is patched into the page cache by Log::appendNoSyncRestamp
    // (inline path) or by the worker's appendNoSyncRestamp (worker path).
    const uint8_t* rbData = msg.recordBatch.data();

    // ---------------------------------------------------------------------------
    // acks=All gateway-policy checks (bootstrap window + ISR gate).
    //
    // These checks live in the gateway (not the helper) because they use gateway
    // state (committedMetadataReady_, replica->isr(), cachedMinIsr_) and may
    // return non-ProduceResponse error types (ErrorResponse{NotLeader}).
    //
    // BOOTSTRAP WINDOW — acks=All DURABILITY CONTRACT (MD1-13 Fix Round 2):
    //   When committedMetadataReady_ is false (committedMap still empty — no
    //   majority-committed assignment yet), the replica is still a Follower
    //   (bridge Step B has not fired).  In this state:
    //     - The replica's ISR is empty (Follower role) → min-ISR gate would always
    //       reject with MinIsrViolation.
    //     - onLeaderLogAppended() is a no-op on Followers → HW never advances →
    //       the deferred-ack queue would stall until timeout.
    //   SILENTLY downgrading acks=All to acks=Leader (immediate ack after local
    //   append) violates the diamond-durability contract: the caller requested
    //   majority-committed durability and the broker would ack without it.
    //
    //   Correct behaviour: return NotLeader (a retriable error code) so the client
    //   retries the produce after bootstrap completes.  NotLeader is retriable by
    //   the client SDK; once committedMetadataReady_ becomes true the retry lands
    //   on the steady-state path where acks=All is honored with real majority-
    //   commit semantics (deferred-ack queue + ISR HW gate).
    //
    //   acks=Leader and acks=0 in the bootstrap window are fine to serve locally
    //   — their contract explicitly does NOT guarantee majority durability.
    //
    //   The log is untouched (no append, no LEO advance) for the retriable-error
    //   path, preserving the "NEVER falsely ack" invariant.
    const bool acksAll = (msg.acks == 1);
    if (acksAll && !committedMetadataReady_) {
        // Bootstrap window: acks=All cannot be served with majority durability yet.
        // Return NotLeader (retriable) so the client retries after bootstrap.
        spdlog::debug("ClientGateway: acks=All produce rejected (bootstrap: committedMetadataReady_=false) "
                      "sessionId={} topicId={} partition={} — returning NotLeader (retriable)",
                      msg.sessionId, msg.topicId, partition);
        pc::ErrorResponse errResp;
        errResp.sessionId     = msg.sessionId;
        errResp.correlationId = msg.correlationId;
        errResp.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                                   pc::ErrorResponse::BODY_FIXED + 2;
        if (needed <= scratch_.size()) {
            const std::size_t encoded = errResp.encode(scratch_.data());
            // G0.1: reliable delivery (retry on back-pressure).
            sendReliable_(*session, scratch_.data(), encoded);
        }
        return true;  // log untouched — no append, no LEO advance
    }

    // E2 inc2a: When a WorkerPool is wired, route the append to the owning worker
    // via the SPSC ingress ring.  The worker executes appendNoSync +
    // onLeaderLogAppended; the response (or pending-ack) is completed in
    // drainWorkerReturns_() at the top of the NEXT doWork() cycle.
    //
    // When no WorkerPool is wired (null), fall through to the inline path below.
    // Route the produce to the OWNING WORKER via the ingress ring whenever a
    // WorkerPool exists AND either:
    //   - per-worker ingress is off (legacy: workers own all partitions), OR
    //   - per-worker ingress is on AND this is a USER partition (topicId >= 0),
    //     which is worker-owned.
    //
    // #13 (HW-visibility race / read tail-stall) FIX: when per-worker ingress is
    // active, a USER-partition log is worker-owned — the worker is its SOLE
    // appender AND the sole writer of its replica high-watermark (via
    // onLeaderLogAppended), AND the sole reader of that HW (pushOwnedSubscriptions_).
    // The gateway must therefore NEVER inline-append to a worker-owned log: doing so
    // wrote the HW on the gateway thread while the worker read it on its own thread
    // with no synchronization — on a weak memory model the worker could read a stale
    // HW that never included the final produce, so the last records were never pushed
    // (consumer stalls on the tail).  Routing to the worker keeps append + HW write +
    // HW read all on the one worker thread — shared-nothing, race-free.
    //
    // No double-processing: a single produce reaches EITHER the worker's own Aeron
    // ingress stream (client routed per-partition) OR the gateway's stream (client
    // fell back) — never both — so routing gateway-received produce to the worker
    // appends it exactly once.  Internal partitions (topicId < 0) are gateway-owned
    // and stay on the inline path below.
    const bool routeToWorker = routesToWorker_(msg.topicId);   // F15 K3: shared with markers
    if (routeToWorker) {
        // ---------------------------------------------------------------------------
        // WORKER PATH: Parse + validate the batch inline (same logic as before) so
        // that the WorkerOp carries the pre-validated fields (lastOffsetDelta,
        // recordCount) and ISR gate is enforced on the gateway thread.
        //
        // M1 fix: derive lastOffset from the CRC-covered lastOffsetDelta field
        // (kLastOffsetDeltaOffset=23, LE32) rather than trusting client recordCount.
        //
        // The base offset is NOT assigned here — the worker's appendNoSyncRestamp
        // assigns it authoritatively when it appends (worker is the single writer).
        // We validate the batch on the READ-ONLY fragment and forward it by view.

        // Read lastOffsetDelta (LE32 at kLastOffsetDeltaOffset=23).
        int32_t lastOffsetDelta = 0;
        {
            const uint8_t* p = rbData + rune::protocol::batch::kLastOffsetDeltaOffset;
            lastOffsetDelta = static_cast<int32_t>(
                static_cast<uint32_t>(p[0]) |
                (static_cast<uint32_t>(p[1]) << 8) |
                (static_cast<uint32_t>(p[2]) << 16) |
                (static_cast<uint32_t>(p[3]) << 24));
        }

        // Read recordCount (LE32 at kRecordCountOffset=57) for consistency validation only.
        int32_t recordCount = 0;
        {
            const uint8_t* p = rbData + rune::protocol::batch::kRecordCountOffset;
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
            return true;
        }

        // recordCount must equal lastOffsetDelta + 1.
        if (recordCount != lastOffsetDelta + 1) {
            spdlog::warn("ClientGateway: Produce recordCount={} != lastOffsetDelta+1={}; "
                         "batch internally inconsistent → rejected (Internal)",
                         recordCount, lastOffsetDelta + 1);
            sendResp(pc::ErrorCode::Internal, partition, 0);
            return true;
        }

        // CRC validation: reject corrupt batches before they touch the log.
        {
            rune::protocol::RecordBatchReader reader(rbData, batchLen);
            if (!reader.ensureValid()) {
                spdlog::warn("ClientGateway: Produce CRC mismatch (corrupt batch) → rejected (Internal)");
                sendResp(pc::ErrorCode::Internal, partition, 0);
                return true;
            }
        }

        // F15 S5 (§2.5, I6 / Q3): NO idempotent-producer gate here, in any mode.
        // The OWNING worker's psm_ is the single producer-state authority for
        // every partition it owns (validate → append → update in its produce
        // core, in program order with every marker's fence) — a gateway mirror
        // would be a second authority for one pid (a check-then-act race, and a
        // one-pass window for seq k+1 before seq k's result drained).

        // GR-5 (Task 10 round 5): NO min-ISR gate here.  The replica's ISR is
        // worker-owned state; reading replica->isr() on this control thread was a
        // data race (and could segfault while becomeLeader was assigning the ISR
        // manager).  The OWNING worker applies the gate (executeProduceWire_ /
        // the shard ingress path) and answers MinIsrViolation in the ProduceResult.

        // P1 (Task 10 round 4): a control marker for this partition is still
        // waiting for worker-ring space — no later batch may overtake it.
        if (!markerSendQueue_.empty() && markerQueuedFor_(msg.topicId, partition)) [[unlikely]] {
            sendResp(pc::ErrorCode::NotLeader, partition, 0);
            return true;
        }

        // Build the op envelope.
        rune::node::WorkerOp op;
        op.type         = rune::node::WorkerOp::Type::Produce;
        op.topicId      = msg.topicId;
        op.partition    = partition;
        replica.stampRouting(op);
        op.logPtr       = log;
        op.sessionId    = msg.sessionId;
        op.correlationId = msg.correlationId;
        op.acks          = msg.acks;
        op.producerId    = msg.producerId;
        op.producerEpoch = msg.producerEpoch;
        op.baseSequence  = msg.baseSequence;
        op.lastOffsetDelta = lastOffsetDelta;
        op.recordCount   = recordCount;
        op.deferAck      = acksAll && static_cast<bool>(replica) && committedMetadataReady_;
        op.minIsr        = cachedMinIsr_;   // the admission policy; the worker gates on its own ISR
        op.enqueuedAtNs  = nowNs_;
        // E3 Task 5: plumb the topic's async-durability tier so the worker
        // knows whether to defer the ack until fsync completion (Strict=2).
        // Majority (1) is the default — keeps existing behavior unchanged.
        op.durabilityTier = replica.durabilityTier();

        // Capture batch attributes for LSO tracking (forwarded in return).
        constexpr std::size_t kAttrOffset =
            static_cast<std::size_t>(rune::protocol::batch::kAttributesOffset);
        if (batchLen >= kAttrOffset + 2u) {
            op.batchAttrs = static_cast<uint16_t>(
                static_cast<uint16_t>(rbData[kAttrOffset]) |
                (static_cast<uint16_t>(rbData[kAttrOffset + 1u]) << 8));
        }

        // Zero-copy: forward the batch by READ-ONLY view (aliases the Aeron
        // fragment, valid for the synchronous tryEnqueueOp call below).  putTail
        // copies it ONCE into the ring slot — no restampScratch_, no owning heap
        // vector.  The worker's appendNoSyncRestamp assigns + patches the base
        // offset (CRC-safe: base-offset field [0..7] precedes the CRC region).
        op.recordBatchView = msg.recordBatch;
        // F15 S5 fix round 1 (M-3): these exact bytes (rbData == msg.recordBatch,
        // unmodified since the ensureValid() above, copied once into the ring by
        // enqueueOp below) passed the CRC32C check — the worker may skip its
        // repeat pass.  INVARIANT: set ONLY here, only after ensureValid().
        op.crcValidated    = true;

        // Enqueue to the worker.  tryEnqueueOp returns false on back-pressure
        // (ring full, or the worker's SetOwnership still undelivered).  M5 (Task
        // 10 round 4): answer the RETRIABLE NotLeader — the client re-sends the
        // same batch (same producer sequence) — never Internal: nothing was
        // appended, so a retry is always safe.  (The client frame aliases the
        // Aeron fragment, so the gateway cannot hold it for its own retry.)
        const rune::node::EnqueueResult er =
            workerPool_->enqueueOp(msg.topicId, partition, std::move(op));
        if (er == rune::node::EnqueueResult::TooLarge) [[unlikely]] {
            // F18 (m-1): the batch can NEVER fit one worker-ingress frame — not
            // back-pressure.  NotLeader would make the client retry it forever;
            // answer the non-retriable MessageTooLarge instead (loud).  Unreachable
            // while maxProduceBatchBytes_ <= the intake limit.
            spdlog::error("ClientGateway: produce batch {} B for topicId={} partition={} exceeds "
                          "the worker ingress frame; MessageTooLarge",
                          batchLen, msg.topicId, partition);
            sendResp(pc::ErrorCode::MessageTooLarge, partition, 0);
        } else if (er != rune::node::EnqueueResult::Enqueued) {
            ++produceBackPressured_;
            if (produceBackPressured_ == 1 || (produceBackPressured_ & 1023u) == 0)
                spdlog::warn("ClientGateway: worker back-pressure for topicId={} partition={}; "
                             "ProduceResponse NotLeader (retriable; total={})",
                             msg.topicId, partition, produceBackPressured_);
            sendResp(pc::ErrorCode::NotLeader, partition, 0);
        } else if (routedWorker >= 0) {   // shard mode (set above)
            // D8c: forwarded — one produce credit is held until the owning
            // worker CLAIMS its answer on the session's ack publication
            // (SessionSignal Credit).  Every forwarded produce is answered
            // exactly once through the worker's SessionAckSink (append, refusal,
            // HW fire / timeout, fsync, purge — acks=0 included); a dropped
            // answer means the session was closed, which drops the budget too.
            takeProduceCredit_(*session, routedWorker);
        }
        // Response (or pending-ack) will be processed in drainWorkerReturns_().
        return true;
    }

    // Phase 2b E2: a TRANSIENT partition is appended only by its owning worker —
    // never inline (the gateway never touches a worker-owned ring).  Reached
    // only without a worker pool (unit rigs; ruling 6).
    if (log == nullptr) [[unlikely]] {
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return true;
    }

    // ---------------------------------------------------------------------------
    // INLINE path (pre-E2 / no WorkerPool):
    //
    // E5.1.2b: Delegate to the shared handleProduceBatch helper.  The helper
    // performs: base-offset restamp in restampScratch_, lastOffsetDelta/recordCount
    // parse + consistency check, CRC validation, idempotent-dedup (PSM validate +
    // update), min-ISR gate (acks=All + isrSize vs minIsr), appendNoSync, and
    // Strict-tier sync().  Returns a ProduceOutcome.
    //
    // The gateway retains ownership of (this NO-WORKER-POOL inline path only):
    // LSO tracking (getTxnState — F15 S4: never used with a worker pool, where
    // the owning worker holds the read_committed state), dirty-log set,
    // replica->onLeaderLogAppended(), and deferred-ack queue management.
    //
    // Durability contract — acks=Leader (acks=1):
    //   Records are written to the OS page cache and visible to Log::read()
    //   immediately.  They are NOT fsync-durable until flushDirtyLogs() runs
    //   at the end of this doWork() cycle.  A leader crash in the window between
    //   appendNoSync() and sync() may lose the most-recent un-flushed acked
    //   records.  True crash-durability requires acks=All + ISR replication
    //   (GR-5/Milestone C).
    // F15 §3.1: the inline path runs on the replica's OWNER thread (no worker
    // pool, or a gateway-owned internal partition) — its live state is ours.
    rune::replication::PartitionReplica* const ownedReplica = replica.controlOwned();
    if (ownedReplica && committedMetadataReady_ &&
        ownedReplica->role() != rune::replication::Role::Leader) {
        spdlog::debug("ClientGateway: Produce per-partition NotLeader (inline owner) "
                      "(topicId={} partition={}) sessionId={}",
                      msg.topicId, partition, msg.sessionId);
        sendErrorResponse_(*session, msg.sessionId, msg.correlationId, pc::ErrorCode::NotLeader);
        return true;  // engine untouched — no append
    }
    // DUR-1 fix round 1 (B1): an INCOMPLETE leader (DR-BUG-6 completeness gate
    // holding — it has not proven it holds the committed frontier) refuses EVERY
    // batch, idempotent or not, with a retriable NotLeader produce answer: a
    // pid -1 batch appended here would land over records a survivor still holds.
    // Same predicate as the worker gate (PartitionReplica::acceptsProduce).
    if (ownedReplica && committedMetadataReady_ && !ownedReplica->acceptsProduce()) [[unlikely]] {
        sendResp(pc::ErrorCode::NotLeader, partition, 0);
        return true;  // engine untouched — no append
    }
    const uint8_t effectiveTier = replica.durabilityTier();
    // ISR size for min-ISR gate: only applies in steady state (committedMetadataReady_
    // true) and when a replica is wired.  Zero causes the helper to skip the gate
    // (passthrough — single-node / bootstrap).
    // Note: bootstrap-window acks=All is already rejected above before we reach here.
    const int isrSizeForHelper = (acksAll && ownedReplica && committedMetadataReady_)
                                 ? static_cast<int>(ownedReplica->isr().size()) : 0;

    // PSM <-> log consistency (findings 1 + 2): an idempotent batch is validated
    // only against a producer state PROVEN consistent with this log — rebuilt
    // here (cold) when the log / leadership it was built for changed.  An
    // unprovable state never answers OOS (the client's "NOT appended" proof)
    // nor accepts a possibly-stored batch: retriable NotLeader while this is not
    // a complete leader, Internal (outcome unknown) if the rebuild failed.
    if (msg.producerId >= 0) {
        const InlinePsmGate gate =
            proveInlinePsm_(msg.topicId, partition, *log, ownedReplica);
        if (gate != InlinePsmGate::Proven) [[unlikely]] {
            sendResp(gate == InlinePsmGate::NotLeader ? pc::ErrorCode::NotLeader : pc::ErrorCode::Internal,
                     partition, 0);
            return true;   // log untouched
        }
    }

    // F15-FU1 (#112): the transactional registration check, synchronously against this
    // node's own coordinator (the inline path runs on the coordinator's thread).
    if (msg.producerId >= 0) [[unlikely]] {
        if (const int16_t c = inlineTxnAdmission_(msg.topicId, partition, rbData, batchLen); c != 0) {
            sendResp(static_cast<pc::ErrorCode>(c), partition, 0);
            return true;   // log untouched
        }
    }

    const auto outcome = rune::node::handleProduceBatch(
        rbData, batchLen,
        msg.topicId, partition,
        msg.producerId, msg.producerEpoch, msg.baseSequence,
        *log, producerStateManager_,
        acksAll,
        isrSizeForHelper,
        cachedMinIsr_,
        effectiveTier);

    // Handle non-OK / duplicate outcomes (no append occurred).
    if (!outcome.isOK()) {
        if (outcome.identityMismatch) [[unlikely]] {
            // EOS-DUP defense: frame identity != CRC-covered batch header.
            if (identityMismatchRejects_++ % 1024 == 0)
                spdlog::warn("ClientGateway: produce rejected — frame producer identity (pid={} epoch={} seq={}) "
                             "!= RecordBatch header; topicId={} partition={} (SchemaValidationFailed, rejects={})",
                             msg.producerId, msg.producerEpoch, msg.baseSequence,
                             msg.topicId, partition, identityMismatchRejects_);
        }
        sendResp(outcome.error, partition, 0, outcome.logStartOffset);   // D6: log start with 28
        return true;
    }
    // Dedup path (log untouched).  F15 S5 parity on the inline path (Kafka
    // DelayedProduce): a duplicate is answered only under its original's ack
    // condition — acks=all parks below on the HW past outcome.lastOffset (the
    // original's range); otherwise it is answered at once.
    // The producer state is proven consistent with this log (the gate above),
    // so a KNOWN-OLD duplicate (baseOffset -1: entirely <= lastSequence, out of
    // the window) is stored: OK with an unknown offset (Kafka
    // DUPLICATE_SEQUENCE_NUMBER parity, as on the worker path) under its ack
    // condition (acks=all parks on outcome.lastOffset below).
    if (outcome.isDuplicate && !(acksAll && ownedReplica && committedMetadataReady_)) {
        sendResp(pc::ErrorCode::OK, partition, outcome.baseOffset);
        return true;
    }

    if (!outcome.isDuplicate) {
    // Successful append — apply post-append gateway-owned side-effects.

    // TX-4 Task 9: Feed LsoTracker for transactional data batches.
    //
    // A transactional batch has producerId >= 0 AND the TRANSACTIONAL bit set in
    // the batch attributes field (bit 4, kAttributesOffset=21).  The helper
    // returns batchAttrs so we can check this bit without re-reading the scratch.
    if (msg.producerId >= 0) {
        constexpr uint16_t kTransactionalBit = 0x10u;
        if (outcome.batchAttrs & kTransactionalBit) {
            getTxnState(msg.topicId, partition)
                .lsoTracker.onProduceBatch(msg.producerId, outcome.baseOffset);
        }
    }

    // E3 Task 9: Strict-tier sync() was already done by the helper (outcome.syncDone).
    // For any other tier, mark the Log dirty so flushDirtyLogs() group-commits it.
    //
    // This path is only reached when workerPool_ is null — test fixtures that bypass
    // BrokerNode/WorkerPool construction or (theoretically) workerCount==0.
    // Internal partitions (topicId < 0) always use durabilityTier=1 (Majority).
    if (outcome.syncDone) {
        // Strict-tier: log was sync()'d by the helper; remove from dirtyLogs_ so
        // flushDirtyLogs() doesn't double-sync it at end of cycle.
        dirtyLogs_.erase(log);
    } else {
        // Non-strict: mark dirty for group-commit at end of cycle.
        dirtyLogs_.insert(log);
    }

    // GR-4: Reconcile the leader replica's tracked LEO/HW (Mode A seam).
    //
    // The gateway appended DIRECTLY to the Log (bypassing PartitionReplica::append)
    // so the leader replica's ReplicaLeaderState still holds the pre-append LEO.
    // onLeaderLogAppended() reads log_->logEndOffset(), refreshes per-follower LEO,
    // and recomputes HW — this is EXACTLY the Mode-A seam described in the spec.
    //
    // No double-append: the Log was written once by the helper's appendNoSync().
    // onLeaderLogAppended() only reads the Log's LEO and updates in-memory state.
    if (ownedReplica) {
        ownedReplica->onLeaderLogAppended();
    }
    } // !outcome.isDuplicate

    // 6. Reply ProduceResponse or enqueue deferred ack.
    //
    // acks=Leader / None: ack immediately after page-cache write.
    // acks=All:           enqueue a PendingAck; drainPendingAcks() will send the
    //                     response once replica->highWatermark() >= lastOffset.
    //
    // acks=All during the bootstrap window (!committedMetadataReady_) is rejected
    // ABOVE before reaching here; by this point either acks != All or
    // committedMetadataReady_ is true.
    const bool deferAck = acksAll && ownedReplica && committedMetadataReady_;
    if (!deferAck) {
        // Immediate ack: acks=Leader or no replica wired (passthrough).
        sendResp(pc::ErrorCode::OK, partition, outcome.baseOffset);
    } else {
        // acks=All deferred ack path (steady state: committedMetadataReady_ true).
        // Cap check: if the queue is full, reject rather than silently overflowing.
        if (pendingAcks_.size() >= kMaxPendingAcks) {
            spdlog::warn("ClientGateway: pendingAcks_ full ({} entries); acks=All produce rejected "
                         "with MinIsrViolation sessionId={} topicId={} partition={}",
                         kMaxPendingAcks, msg.sessionId, msg.topicId, partition);
            sendResp(pc::ErrorCode::MinIsrViolation, partition, 0);
            return true;
        }

        PendingAck entry;
        entry.sessionId     = msg.sessionId;
        entry.correlationId = msg.correlationId;
        entry.partition     = partition;
        entry.baseOffset    = outcome.baseOffset;
        entry.lastOffset    = outcome.lastOffset;
        entry.enqueuedAtNs  = nowNs_;
        entry.topicId       = msg.topicId;
        entry.incarnation   = replica.incarnation();
        entry.logGen        = ownedReplica->logGeneration();   // deferAck ⇒ ownedReplica

        pendingAcks_.push_back(entry);  // no heap alloc (capacity pre-reserved)

        spdlog::debug("ClientGateway: acks=All produce enqueued pending-ack "
                      "sessionId={} partition={} baseOffset={} lastOffset={}",
                      msg.sessionId, partition, outcome.baseOffset, outcome.lastOffset);
    }
    return true;
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
// ---------------------------------------------------------------------------
// read-rework T3/T4 (I3b): route subscription lifecycle to the owning worker.
// ---------------------------------------------------------------------------
// enqueueSubscribeOp_ — build a Subscribe WorkerOp with the resolved per-consumer
// egress pub pointer and route it to the owning worker.  egressPub may be null
// (no session / test fixture / resolve timeout) → the worker falls back to MDC.
// CG round 4 (A-MAJOR-1): the ONE builder of a worker Subscribe op — the
// direct path (enqueueSubscribeOp_) and the queued retry (trySubOp_) both use
// it, so neither can drop a field (the retry used to omit the Subscribe's
// correlationId — every push then carried 0 — and the replica).
rune::node::WorkerOp ClientGateway::buildSubscribeOp_(const BufferedSubscribe& s, void* egressPub,
                                                      std::span<const uint8_t> filter) const
{
    rune::storage::Log* log = topicRegistry_ ? topicRegistry_->log(s.topicId, s.partition) : nullptr;
    const rune::replication::PartitionReplica* rep =
        replicaRegistry_ ? replicaRegistry_->lookup(s.topicId, s.partition) : nullptr;

    rune::node::WorkerOp op;
    op.type                 = rune::node::WorkerOp::Type::Subscribe;
    op.subscriptionId       = s.subId;
    op.sessionId            = s.sessionId;
    op.correlationId        = s.subscribeCorrId;  // stamped on the worker's Records pushes
    op.topicId              = s.topicId;
    op.partition            = s.partition;
    op.nextOffset           = s.nextOffset;
    op.isolation            = s.isolation;
    op.isGroup              = s.isGroup;
    op.groupCarrierReceived = s.groupCarrierReceived;
    op.logPtr               = log;
    op.replica              = const_cast<rune::replication::PartitionReplica*>(rep);
    op.egressPub            = egressPub;
    // Phase 2c: the filter rides the op's tail (a view valid for the synchronous enqueue - the caller's bytes outlive it).
    op.filterBytes          = filter;
    return op;
}

void ClientGateway::parkAwaitingEgress_(const BufferedSubscribe& s)
{
    const int32_t wid = workerPool_ ? workerPool_->ownerWorkerId(s.topicId, s.partition) : 0;
    pendingRecordEgress_[{s.sessionId, wid}].waiters.push_back(s);
    if (const auto it = subscriptions_.find(s.subId); it != subscriptions_.end()) it->second.cursor = Cursor::AwaitingEgress;
}

void ClientGateway::releaseAwaiting_(const BufferedSubscribe& s, void* egressPub)
{
    // The subscription may have ended while its Subscribe waited (a state AwaitingEgress is cancelled by the unsubscribe,
    // so this is the defensive half): nothing goes to the worker for a subscription the gateway no longer holds.
    const auto it = subscriptions_.find(s.subId);
    if (it == subscriptions_.end() || it->second.cursor != Cursor::AwaitingEgress) return;
    enqueueSubscribeOp_(s, egressPub);
}

void ClientGateway::enqueueSubscribeOp_(const BufferedSubscribe& s, void* egressPub)
{
    // Issue #118 (X020, M10): the ONE place a Subscribe without its own record egress would reach a worker - which then
    // pushes on its shared dynamic-MDC publication, received by every party joined to the worker's advertised control
    // port.  Under authentication that is a disclosure to principals without READ: the subscription is retired
    // retriably instead (never a fallback).
    if (egressPub == nullptr && authMode_ == rune::gateway::GatewayAuthMode::Required) [[unlikely]] {
        retireWithoutRecordEgress_(s.subId);
        return;
    }
    if (const auto it = subscriptions_.find(s.subId); it != subscriptions_.end()) {
        it->second.cursor = Cursor::AtWorker;
        if (it->second.okOwed) sendOwedSubscribeOk_(s.subId);   // F1: the egress exists: the held OK goes first
    }
    // The live record's filter (this enqueue is synchronous); a queued retry carries its own copy, taken below.
    std::span<const uint8_t> filter;
    if (const auto it = subscriptions_.find(s.subId); it != subscriptions_.end()) filter = std::span<const uint8_t>(it->second.filter);
    rune::node::WorkerOp op = buildSubscribeOp_(s, egressPub, filter);

    // M5: never dropped — a refused Subscribe (or one behind a queued lifecycle
    // op for the same worker) is queued and retried every doWork, in order.
    const auto w = static_cast<std::size_t>(workerPool_->ownerWorkerId(s.topicId, s.partition));
    const bool behind = w < pendingSubOps_.size() && !pendingSubOps_[w].empty();
    if (behind || !workerPool_->tryEnqueueOp(s.topicId, s.partition, std::move(op))) {
        queueSubOp_(w, SubOpSubscribe{s, egressPub == nullptr ? EgressRoute::Mdc : EgressRoute::Unicast,
                                      std::vector<uint8_t>(filter.begin(), filter.end())});
    }
}

void ClientGateway::queueSubOp_(std::size_t w, PendingSubOp p)
{
    if (pendingSubOps_.size() <= w) pendingSubOps_.resize(static_cast<std::size_t>(workerPool_->workerCount()));
    pendingSubOps_[w].push_back(std::move(p));
    const std::size_t n = pendingSubOpsForTest();
    if (n == 1 || (n & (n - 1)) == 0)   // rate limit: first + each power of two
        spdlog::warn("ClientGateway: {} subscription op(s) waiting for worker ring space — retried every cycle", n);
}

bool ClientGateway::trySubOp_(std::size_t w, const PendingSubOp& p)
{
    struct Visitor {
        ClientGateway& g;
        std::size_t    w;
        bool operator()(const SubOpUnsubscribe& u) const
        {
            rune::node::WorkerOp op;
            op.type           = rune::node::WorkerOp::Type::Unsubscribe;
            op.subscriptionId = u.subId;
            op.topicId        = u.topicId;
            op.partition      = u.partition;
            op.endCode        = u.endCode;      // issue #109: the worker ends it after its last Records
            op.endDetail      = u.endDetail;    // a view: valid for this (synchronous) enqueue
            return g.workerPool_->tryEnqueueOp(u.topicId, u.partition, std::move(op));
        }
        bool operator()(const SubOpSubscribe& sp) const
        {
            const auto& s = sp.sub;
            // Re-resolve the egress publication NOW (never a pointer held across cycles).
            void* egressPub = nullptr;
            if (sp.route == EgressRoute::Mdc && g.authMode_ == rune::gateway::GatewayAuthMode::Required) [[unlikely]] {
                // Issue #118: never queued under authentication (enqueueSubscribeOp_ retires it); defensive - nothing is sent.
                g.retireWithoutRecordEgress_(s.subId);
                return true;
            }
            if (sp.route == EgressRoute::Unicast) {
                rune::gateway::Session* sess = g.sessions_.find(s.sessionId);
                if (!sess) return true;   // session gone: the Subscribe is moot (dropped)
                const int32_t wid = g.workerPool_->ownerWorkerId(s.topicId, s.partition);
                auto it = sess->recordEgressByWorker.find(wid);
                if (it == sess->recordEgressByWorker.end() || !it->second) return true;   // moot
                egressPub = it->second.get();
            }
            rune::node::WorkerOp op = g.buildSubscribeOp_(s, egressPub, sp.filter);  // A-MAJOR-1: correlationId + replica included; the filter is the queued copy
            return g.workerPool_->tryEnqueueOp(s.topicId, s.partition, std::move(op));
        }
        bool operator()(const SubOpFence& f) const
        {
            // issues #109 / #196: behind every op queued before it for this worker
            rune::node::WorkerOp op;
            op.type          = rune::node::WorkerOp::Type::EnforceFence;
            op.correlationId = static_cast<int64_t>(f.seq);
            return g.workerPool_->worker(static_cast<int32_t>(w)).tryEnqueueOp(std::move(op));
        }
        bool operator()(const SubOpFilterSchema& f) const
        {
            rune::node::WorkerOp op;
            op.type            = rune::node::WorkerOp::Type::FilterSchema;
            op.topicId         = f.topicId;
            op.clientVersion   = f.version;
            op.schemaUpdateBlob = f.blob;
            // Direct to worker w (the schema is never ownership-gated); TooLarge is not retried (it was counted when queued).
            return g.workerPool_->worker(static_cast<int32_t>(w)).enqueueOp(std::move(op)) != rune::node::EnqueueResult::Full;
        }
    };
    return std::visit(Visitor{*this, w}, p);
}

int ClientGateway::pumpSubOps_()
{
    int done = 0;
    for (std::size_t w = 0; w < pendingSubOps_.size(); ++w) {
        auto& q = pendingSubOps_[w];
        while (!q.empty()) {
            if (!trySubOp_(w, q.front())) break;   // this worker's ring still full; others proceed
            q.pop_front();
            ++done;
        }
    }
    return done;
}

void ClientGateway::forwardSubscribeToWorker_(int64_t subId, int64_t sessionId,
                                              int32_t topicId, int32_t partition,
                                              int64_t nextOffset, uint8_t isolation,
                                              bool isGroup, bool groupCarrierReceived,
                                              int64_t subscribeCorrId)
{
    BufferedSubscribe s{subId, subscribeCorrId, sessionId, topicId, partition,
                        nextOffset, isolation, isGroup, groupCarrierReceived};

    rune::gateway::Session* sess = sessions_.find(sessionId);

    // Hybrid egress: forward with this consumer's per-(session,worker) UNICAST
    // Records pub (independent per-consumer offsets, not MDC broadcast).  ONE pub
    // per owning worker — an ExclusivePublication is single-writer, so partitions
    // owned by different workers MUST use different pubs.
    //   - No session / no egress endpoint (test fixtures) → forward with MDC (null).
    //   - Pub for this worker already resolved on the Session → forward immediately.
    //   - Otherwise → kick off a NON-BLOCKING resolve and DEFER the Subscribe until
    //     the pub is ready (drained by pollPendingRecordEgress_).  Never blocks the
    //     control thread.
    if (!sess || !workerPool_) {
        enqueueSubscribeOp_(s, nullptr);   // under authentication: retired (issue #118)
        return;
    }
    const bool authRequired = authMode_ == rune::gateway::GatewayAuthMode::Required;
    const int32_t wid = workerPool_->ownerWorkerId(topicId, partition);
    if (auto it = sess->recordEgressByWorker.find(wid);
        it != sess->recordEgressByWorker.end() && it->second) {
        enqueueSubscribeOp_(s, it->second.get());
        return;
    }
    if (sess->egressChannel.empty() || !aeron_) {
        // Issue #118: under authentication the Subscribe waits as a FAILED resolve (regId -1), so the retirement is
        // answered on the next pass, after this Subscribe's OK (the client then re-subscribes from its position).
        if (authRequired) [[unlikely]] { parkAwaitingEgress_(s); return; }
        enqueueSubscribeOp_(s, nullptr);
        return;
    }

    // Ensure a pending resolve is in flight for (session, worker), then buffer the
    // subscribe until it resolves.  Each worker gets its OWN ExclusivePublication
    // to the client's egress endpoint (distinct Aeron session → distinct image;
    // single-writer preserved).
    auto& pend = pendingRecordEgress_[{sessionId, wid}];
    if (pend.regId < 0) {
        try {
            // A10 review M1: a consumer DATA stream keeps the pre-faulted (non-sparse) default; an add error
            // here falls back to the MDC path (degraded, never fatal).
            pend.regId         = aeron_->addExclusivePublication(sess->egressChannel,
                                                                 sess->egressStreamId);
            pend.requestedAtNs = nowNs_;
        } catch (...) {
            spdlog::warn("ClientGateway: record-egress pub create threw for session {} "
                         "worker {} (channel {}); Subscribe {}",
                         sessionId, wid, LogSafeName{sess->egressChannel},
                         authRequired ? "retired (no shared-egress fallback under authentication)" : "uses MDC fallback");
            if (authRequired) [[unlikely]] {   // issue #118: answered on the next pass, after the OK
                pend.regId = -1;
                pend.requestedAtNs = nowNs_;
                pend.waiters.push_back(s);
                if (const auto it = subscriptions_.find(subId); it != subscriptions_.end()) it->second.cursor = Cursor::AwaitingEgress;
                return;
            }
            pendingRecordEgress_.erase({sessionId, wid});
            enqueueSubscribeOp_(s, nullptr);
            return;
        }
    }
    pend.waiters.push_back(s);
    if (const auto it = subscriptions_.find(subId); it != subscriptions_.end()) it->second.cursor = Cursor::AwaitingEgress;
}

// pollPendingRecordEgress_ — resolve in-flight per-session record-egress pubs
// (non-blocking) and drain each session's buffered Subscribes once its pub is
// ready.  Called from doWork().  A resolve that exceeds the connect timeout drops
// to the MDC fallback so a subscribe is never wedged forever.
void ClientGateway::pollPendingRecordEgress_(int64_t nowNs)
{
    if (pendingRecordEgress_.empty() || holdRecordEgressResolve_) return;
    for (auto it = pendingRecordEgress_.begin(); it != pendingRecordEgress_.end(); ) {
        const int64_t sessionId = it->first.first;
        const int32_t workerId  = it->first.second;
        PendingRecordEgress& pend = it->second;

        std::shared_ptr<aeron::ExclusivePublication> pub;
        try {
            if (pend.regId >= 0) pub = aeron_->findExclusivePublication(pend.regId);
        } catch (...) {
            pub = nullptr;  // ERRORED — treat as failed resolve below
            pend.regId = -1;
        }

        const bool timedOut =
            (nowNs - pend.requestedAtNs) > connectResolveTimeoutNs_;

        if (pub) {
            // Resolved: cache on the Session under its worker id and drain waiters.
            // Record-egress lifetime (2026-10-05): a worker gets ONLY the pointer
            // of the pub the Session owns — never one this scope destroys (the
            // old session-gone branch handed the local pub's raw pointer out and
            // then freed it), and a cached pub is never REPLACED (a worker may
            // hold it; it lives until its retire-on-ack release at close).
            onRecordEgressResolved_(sessionId, workerId, pend, std::move(pub));
            it = pendingRecordEgress_.erase(it);
            continue;
        }
        if (timedOut || pend.regId < 0) {
            spdlog::warn("ClientGateway: record-egress pub for session {} worker {} did not "
                         "resolve ({} waiters) — {}",
                         sessionId, workerId, pend.waiters.size(),
                         authMode_ == rune::gateway::GatewayAuthMode::Required
                             ? "retired NotLeader (no shared-egress fallback under authentication)"
                             : "falling back to MDC");
            for (const auto& w : pend.waiters) releaseAwaiting_(w, nullptr);
            abandonRegistration_(pend.regId);   // timed out while still in flight: release it (regId < 0 = nothing)
            it = pendingRecordEgress_.erase(it);
            continue;
        }
        ++it;  // still resolving — retry next cycle
    }
}

void ClientGateway::onRecordEgressResolved_(int64_t sessionId, int32_t workerId, PendingRecordEgress& pend,
                                            std::shared_ptr<aeron::ExclusivePublication> pub)
{
    if (rune::gateway::Session* sess = sessions_.find(sessionId)) {
        auto& slot = sess->recordEgressByWorker[workerId];
        if (!slot) slot = std::move(pub);   // else: the extra resolve dies here, never handed out
        for (const auto& w : pend.waiters) releaseAwaiting_(w, slot.get());
    }
    // else: the session closed meanwhile — its buffered subscribes are moot and
    // the pub (never handed to a worker) is released with `pub`.
}

void ClientGateway::sendOwedSubscribeOk_(int64_t subId)
{
    const auto it = subscriptions_.find(subId);
    if (it == subscriptions_.end() || !it->second.okOwed) return;
    Subscription& sub = it->second;
    sub.okOwed = false;
    Session* session = sessions_.find(sub.sessionId);
    if (session == nullptr) return;
    pc::SubscribeResponse resp;
    resp.sessionId      = sub.sessionId;
    resp.correlationId  = sub.subscribeCorrId;
    resp.errorCode      = 0;
    resp.partition      = sub.partition;
    resp.subscriptionId = subId;
    sendAnswer_(*session, scratch_.data(), resp.encode(scratch_.data()));
    if (std::exchange(sub.okOwedLineage, false)) noteLineage_(subId);
}

void ClientGateway::retireWithoutRecordEgress_(int64_t subId)
{
    // Issue #118: a subscription this broker cannot serve on its own per-session record egress under authentication.
    // Explicit, retriable, never silent: a plain subscription gets the position note + NotLeader on its correlation id
    // (the client's moved-subscription path re-subscribes from its position, bounded by its retry budget); a group
    // member's partition subscription evicts the session's members (the consumer rejoins).  Nothing reaches a worker.
    const auto it = subscriptions_.find(subId);
    if (it == subscriptions_.end()) return;   // ended meanwhile: nothing to answer
    const Subscription sub = std::move(it->second);   // moved: a filter ticket is move-only (released with `sub`)
    subscriptions_.erase(it);
    ++subscribesWithoutRecordEgress_;
    if ((subscribesWithoutRecordEgress_ & (subscribesWithoutRecordEgress_ - 1)) == 0)   // first + each power of two
        spdlog::warn("ClientGateway: subscription {:#x} (session {}, topic {} partition {}) has no record egress of its "
                     "own - retired NotLeader, never pushed on the shared egress under authentication (so far: {})",
                     subId, sub.sessionId, sub.topicId, sub.partition, subscribesWithoutRecordEgress_);
    Session* session = sessions_.find(sub.sessionId);
    if (session == nullptr) return;           // closed: it ended with its session
    if (sub.okOwed) {
        // PR #441 F1/F4: the Subscribe was never answered OK - it is answered now, as the pending request it still
        // is, with its own cause (RecordEgressUnavailable, never a routing NotLeader): the client retries it a few
        // times (plain and group-member fetches alike), then fails it explicitly; routing refusals stay unbounded.
        session->subIdByKey.erase(Session::SubKey{sub.subscribeCorrId, sub.topicId, sub.partition});
        pc::SubscribeResponse resp;
        resp.sessionId      = sub.sessionId;
        resp.correlationId  = sub.subscribeCorrId;
        resp.errorCode      = static_cast<int16_t>(pc::ErrorCode::RecordEgressUnavailable);
        resp.partition      = sub.partition;
        resp.subscriptionId = 0;
        sendAnswer_(*session, scratch_.data(), resp.encode(scratch_.data()));
        return;
    }
    if (sub.isGroup) {
        if (const auto g = groupMemberSubs_.find(sub.sessionId); g != groupMemberSubs_.end())
            std::erase(g->second, subId);
        if (groupService_ != nullptr) groupService_->evictSessionMembers(sub.sessionId, nowNs_);
        return;
    }
    session->subIdByKey.erase(Session::SubKey{sub.subscribeCorrId, sub.topicId, sub.partition});
    if (pc::clientUnderstandsMovedPositionNote(session->clientVersion)) {
        pc::Records note;
        note.sessionId      = sub.sessionId;
        note.correlationId  = sub.subscribeCorrId;
        note.subscriptionId = subId;
        note.topicId        = sub.topicId;
        note.partition      = sub.partition;
        note.baseOffset     = sub.nextOffset;
        note.recordBatch    = {};
        sendAnswer_(*session, scratch_.data(), note.encode(scratch_.data()));
    }
    pc::ErrorResponse err;
    err.sessionId     = sub.sessionId;
    err.correlationId = sub.subscribeCorrId;
    err.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
    sendAnswer_(*session, scratch_.data(), err.encode(scratch_.data()));
}

bool ClientGateway::forwardUnsubscribeToWorker_(int64_t subId, int32_t topicId, int32_t partition, int16_t endCode,
                                                std::string_view endDetail)
{
    // Task 18 (seeded stress): a subscription in state AwaitingEgress has not reached the worker (its Subscribe waits in
    // pendingRecordEgress_ for the session's pub), so there is nothing there to unsubscribe - and left waiting it would be
    // enqueued later, a zombie cursor the gateway no longer knows (it kept pushing after a revoke ended the subscription,
    // and resumed on a re-grant).  Cancel the waiter the state names.
    if (const auto sit = subscriptions_.find(subId); sit != subscriptions_.end() && sit->second.cursor == Cursor::AwaitingEgress) {
        const int32_t wid = workerPool_->ownerWorkerId(sit->second.topicId, sit->second.partition);
        if (const auto pit = pendingRecordEgress_.find({sit->second.sessionId, wid}); pit != pendingRecordEgress_.end()) {
            auto& ws = pit->second.waiters;
            ws.erase(std::remove_if(ws.begin(), ws.end(), [subId](const BufferedSubscribe& w) { return w.subId == subId; }), ws.end());
        }
        sit->second.cursor = Cursor::GatewayPushed;
        return false;   // no worker ever had it: the caller answers the end
    }
    // M5: never dropped on a full ring — queued (behind any queued Subscribe of
    // the same worker, so the pair keeps its order) and retried every doWork.
    // Issue #109: only a cursor the worker HOLDS (its Subscribe was forwarded) is ended by the worker; anything else
    // is answered by the caller (the plain Unsubscribe still goes, harmless if the worker never had it).
    const auto sw = subscriptions_.find(subId);
    const bool atWorker = sw != subscriptions_.end() && sw->second.cursor == Cursor::AtWorker;
    SubOpUnsubscribe u{subId, topicId, partition};
    if (atWorker && endCode != 0) {
        u.endCode = endCode;
        u.endDetail.assign(endDetail.substr(0, 192));   // cold: a revoked READ
    }
    PendingSubOp p = std::move(u);
    const auto w = static_cast<std::size_t>(workerPool_->ownerWorkerId(topicId, partition));
    if (applyTracking_ && w < 64) [[unlikely]] applyOpWorkers_ |= uint64_t{1} << w;   // #109: the event's fence covers it
    const bool behind = w < pendingSubOps_.size() && !pendingSubOps_[w].empty();
    if (behind || !trySubOp_(w, p)) queueSubOp_(w, std::move(p));
    return atWorker;
}

// ---------------------------------------------------------------------------
// Record-egress lifetime — retire-on-ack (see ClientGateway.h RetiringEgress).
// ---------------------------------------------------------------------------
void ClientGateway::retireRecordEgress_(int64_t sid, int32_t worker,
                                        std::shared_ptr<aeron::ExclusivePublication> pub)
{
    // Pubs are only ever handed to a worker of the pool (forwardSubscribeToWorker_
    // requires it, and the router is fixed for the pool's lifetime); without
    // one, no worker can hold it — it is released here.
    if (!workerPool_ || worker < 0 || worker >= workerPool_->workerCount()) return;
    RetiringEgress r;
    r.token     = ++nextRetireToken_;
    r.sessionId = sid;
    r.worker    = worker;
    r.pub       = std::move(pub);
    // PR #459 F3: FIFO behind this worker's queued subscription ops (an ending Unsubscribe among them must run while
    // its egress still exists) - a release never overtakes them.
    const auto wq = static_cast<std::size_t>(worker);
    const bool behind = wq < pendingSubOps_.size() && !pendingSubOps_[wq].empty();
    r.enqueued  = !behind && tryEnqueueEgressRelease_(r);
    if (!r.enqueued) ++retiringUnsent_;
    retiringEgress_.push_back(std::move(r));
}

bool ClientGateway::tryEnqueueEgressRelease_(const RetiringEgress& r)
{
    // The release op: an Unsubscribe carrying the pointer (egressPub != 0 is the
    // release form; subscriptionId 0 names no subscription).  Sent to the worker
    // the pointer was handed to, on its op ring — FIFO behind every Subscribe op
    // that carried it.  (Subscribes still queued in pendingSubOps_ re-resolve
    // the pub from the Session at send time, so none can carry it after this.)
    rune::node::WorkerOp op;
    op.type           = rune::node::WorkerOp::Type::Unsubscribe;
    op.subscriptionId = 0;
    op.sessionId      = r.sessionId;
    op.correlationId  = static_cast<int64_t>(r.token);
    op.egressPub      = r.pub.get();
    return workerPool_->worker(r.worker).tryEnqueueOp(std::move(op));
}

int ClientGateway::pumpEgressReleases_()
{
    if (retiringUnsent_ == 0) [[likely]] return 0;
    int sent = 0;
    uint64_t refused = 0;   // review MEDIUM-1: a worker whose ring refused is not retried this pass
    static_assert(Session::kMaxWorkers <= 64, "refused-worker mask is 64 bits");
    for (auto& r : retiringEgress_) {
        if (r.enqueued) continue;
        const uint64_t bit = uint64_t{1} << static_cast<unsigned>(r.worker);
        if ((refused & bit) != 0) continue;
        const auto wq = static_cast<std::size_t>(r.worker);
        if (wq < pendingSubOps_.size() && !pendingSubOps_[wq].empty()) { refused |= bit; continue; }   // F3: FIFO
        if (!tryEnqueueEgressRelease_(r)) {
            refused |= bit;
            continue;
        }
        r.enqueued = true;
        --retiringUnsent_;
        ++sent;
    }
    return sent;
}

void ClientGateway::onEgressReleased_(int32_t worker, uint64_t token)
{
    for (std::size_t i = 0; i < retiringEgress_.size(); ++i) {
        RetiringEgress& r = retiringEgress_[i];
        if (r.token != token || r.worker != worker) continue;
        if (!r.enqueued) [[unlikely]] {         // unreachable: an ack can never precede its op
            spdlog::error("ClientGateway: EgressReleased from worker {} for retire token {} before its release "
                          "op was sent — ignored", worker, token);
            return;
        }
        if (i + 1 != retiringEgress_.size()) std::swap(retiringEgress_[i], retiringEgress_.back());
        retiringEgress_.pop_back();            // the worker let go — the pub is destroyed now
        return;
    }
    spdlog::error("ClientGateway: EgressReleased from worker {} for unknown retire token {} — ignored",
                  worker, token);
}

void ClientGateway::handleSubscribe(const uint8_t* buf, std::size_t len)
{
    // 1. Decode.  P13 / E13: the filter trailer is parsed only for a session that negotiated client version >= 11,
    // so the session is looked up first.  An unknown session decodes as version 0 (no trailer parsed), which keeps
    // the existing unknown-session path below exactly as it was.
    int32_t decodeVersion = 0;
    if (const auto hdr = pc::MessageHeader::decode(buf, len)) {
        if (const Session* s0 = sessions_.find(hdr->sessionId)) decodeVersion = s0->clientVersion;
    }
    auto msgOpt = pc::Subscribe::decode(buf, len, decodeVersion);
    if (!msgOpt) [[unlikely]] {
        // No-hang (R2): answered SubscribeResponse{InvalidRequest}.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                             "Subscribe body undecodable");
        return;
    }
    const pc::Subscribe& msg = *msgOpt;

    // 2. Require known session.
    Session* session = sessions_.find(msg.sessionId);
    if (!session) [[unlikely]] {
        noteUnanswerable_(buf, len, "Subscribe for unknown session");
        return;
    }

    // Authentication Task 12 (spec §2.1, D21 / D22): -6 (catalog reads) needs DESCRIBE ON CLUSTER, -7 (security reads)
    // ADMIN ON CLUSTER, anything else READ on the topic id (one byte load).  Before any routing or redirect.
    if (const PermissionTable& perms = permsOf_(*session); !perms.isAllowAll()) {
        namespace rsec = rune::security;
        std::optional<rune::gateway::Denial> d;
        if (msg.topicId == pc::kCatalogTopicId)       d = rune::gateway::authorizeCluster(perms, rsec::Operation::Describe);
        else if (msg.topicId == pc::kSecurityTopicId) d = rune::gateway::authorizeCluster(perms, rsec::Operation::Admin);
        else d = authorizeTopicIdHere_(perms, msg.topicId, msg.partitionKnown != 0 ? msg.partition : -1, rsec::Operation::Read);
        if (deny_(*session, d, buf, len)) return;
    }
    if (refuseViewSink_(buf, len, msg.topicId, "Subscribe to a view sink")) [[unlikely]] return;   // VLS B.3

    // Helper: send SubscribeResponse on the session egress.  Phase 2c: a refusal's `detail` (<= 256 bytes) is written
    // only for a session >= 11 (encodeFor); an older session gets the byte-identical 38-byte frame.
    auto sendResp = [&](pc::ErrorCode ec, int32_t partition, int64_t subId, std::string_view detail = {}) {
        pc::SubscribeResponse resp;
        resp.sessionId      = msg.sessionId;
        resp.correlationId  = msg.correlationId;
        resp.errorCode      = static_cast<int16_t>(ec);
        resp.partition      = partition;
        resp.subscriptionId = subId;
        resp.detail         = detail.substr(0, 256);
        static_assert(pc::SubscribeResponse::ENCODED_SIZE + 2 + 256 <= kScratchSize,
                      "scratch too small for SubscribeResponse with a detail");
        const std::size_t encoded = resp.encodeFor(scratch_.data(), session->clientVersion, kScratchSize);
        // CG round 3 (MEDIUM-2): queued + retried under back-pressure like the
        // produce answers - never a best-effort drop of the binding answer.
        sendAnswer_(*session, scratch_.data(), encoded);
    };

    // Phase 2c (spec 9, E1): a filter is read only from a session >= 11; `__catalog` / `__security` are untyped.
    if (pc::clientUnderstandsFilters(session->clientVersion) && !msg.filter.empty() &&
        (msg.topicId == pc::kCatalogTopicId || msg.topicId == pc::kSecurityTopicId)) [[unlikely]] {
        sendResp(pc::ErrorCode::InvalidRequest, msg.partition, 0,
                 rune::codec::describe({rune::codec::FilterErrorKind::UntypedTopic}));
        return;
    }

    // Catalog history Task 12 (spec §7, F19): `__catalog` is readable from ANY
    // broker — never redirected, never routed by the data-plane image.  The
    // admin thread (the catalog replica's owner) answers it.
    if (msg.topicId == pc::kCatalogTopicId) [[unlikely]] {
        handleCatalogSubscribe_(*session, msg);
        return;
    }
    // Authentication Task 10 (D39): `__security` is read the same way, from the SECOND read server of any broker (the
    // reader's ADMIN check is Task 12's).
    if (msg.topicId == pc::kSecurityTopicId) [[unlikely]] {
        handleSecuritySubscribe_(*session, msg);
        return;
    }

    // GB-4 / MD1-9: Leadership check — data ops MUST reach the partition leader.
    //
    // Helper: send redirect/notleader error response for subscribe.
    auto sendSubRedirect = [&](const std::string& endpoint, int64_t corrId) {
        sendRouteRefusal_(*session, msg.sessionId, corrId, endpoint);
    };

    // Cluster-level fallback: only when routingImage_ is null.
    if (!routingImage_ && leaderView_) {
        const LeaderView lv = leaderView_();
        if (!lv.isLeader) {
            sendSubRedirect(lv.leaderClientEndpoint, msg.correlationId);
            return;  // engine untouched, no subscription registered
        }
    }

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
            if (unappliedTopicCode_(msg.topicId, 0) == pc::ErrorCode::NotLeader) {   // Task 8
                sendSubRedirect(std::string{}, msg.correlationId);
                return;
            }
            sendResp(pc::ErrorCode::UnknownTopic, 0, 0);
            return;
        }
    }

    // 3b. MD1 Task 9: Per-partition routing image check.
    // Mirrors the produce path: capture-once discipline (routingImage_.get()).
    //
    // BOOTSTRAP FIX — defense-in-depth (MD1-13): same as handleProduce.
    // When the image is set but this partition is NO_LEADER (not yet assigned),
    // fall back to Path B (cluster-leader) rather than returning NotLeader.
    const RoutedEntry* routed = nullptr;   // Task 8 (M1): reused by servedPartition_
    if (routingImage_) {
        const rune::metadata::RoutingImage* img = routingImage_.get();
        const int32_t gpid = rune::metadata::gpidOf(msg.topicId, partition);
        const auto& entry = img->lookup(gpid);
        routed = &entry;

        if (entry.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID) {
            // Partition not yet assigned — fall back to cluster-leader path.
            spdlog::debug("ClientGateway: Subscribe partition not in routing image "
                          "(topicId={} part={}) — falling back to cluster-leader path, sessionId={}",
                          msg.topicId, partition, msg.sessionId);
            if (leaderView_) {
                const LeaderView lv = leaderView_();
                if (!lv.isLeader) {
                    sendSubRedirect(lv.leaderClientEndpoint, msg.correlationId);
                    return;  // engine untouched, no subscription registered
                }
            }
            // leaderView_ says we ARE the leader (or is null) — serve locally (fall through).
        } else if (entry.leaderNodeId != thisNodeId_) {
            // Catalog history Task 8: a dropped topic is UnknownTopic at once (see produce).
            if (topicRegistry_ != nullptr &&
                topicRegistry_->state(msg.topicId) == TopicRegistry::TopicState::Dropped) [[unlikely]] {
                sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
                return;
            }
            const std::string& redirectEndpoint = [&]() -> const std::string& {
                auto it = nodeEndpointMap_.find(entry.leaderNodeId);
                if (it != nodeEndpointMap_.end()) return it->second;
                static const std::string kEmpty;
                return kEmpty;
            }();
            spdlog::debug("ClientGateway: Subscribe per-partition redirect "
                          "(topicId={} part={} leader={} epoch={}) sessionId={} → {}",
                          msg.topicId, partition, entry.leaderNodeId, entry.leaderEpoch,
                          msg.sessionId, redirectEndpoint);
            sendSubRedirect(redirectEndpoint, msg.correlationId);
            return;  // engine untouched, no subscription registered
        }
        // entry.leaderNodeId == thisNodeId_ → serve locally (fall through).
    }

    // 4. Validate topic + partition.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway: Subscribe but no TopicRegistry — UnknownTopic");
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    const rune::node::TopicSpec* spec = topicRegistry_->topic(msg.topicId);
    if (!spec) {
        // Task 8 (C1 generalized): placed but not applied here → the retriable NotLeader.
        if (unappliedTopicCode_(msg.topicId, partition) == pc::ErrorCode::NotLeader) {
            spdlog::debug("ClientGateway: Subscribe to topicId={} placed but not applied here — NotLeader",
                          msg.topicId);
            sendSubRedirect(std::string{}, msg.correlationId);
            return;
        }
        spdlog::debug("ClientGateway: Subscribe unknown topicId={}", msg.topicId);
        sendResp(pc::ErrorCode::UnknownTopic, partition, 0);
        return;
    }

    // C1: a runtime topic awaiting its DDL commit is not readable — the bare
    // ErrorResponse{NotLeader} every unservable-here subscribe gets (retriable:
    // refresh metadata, retry); no subscription is registered.
    const ServedPartition served = servedPartition_(msg.topicId, partition, routed);
    if (served.kind == ServedPartition::Kind::NotPlaced) {
        spdlog::debug("ClientGateway: Subscribe to topicId={} awaiting its committed placement — "
                      "NotLeader (retriable)", msg.topicId);
        sendSubRedirect(std::string{}, msg.correlationId);
        return;
    }
    // Phase 2b E2: a TRANSIENT partition (no disk Log) is pushed by its owning
    // worker only — refused without a worker pool (no inline push path).
    if (served.kind == ServedPartition::Kind::Unknown ||
        (served.kind == ServedPartition::Kind::Transient && !workerPool_)) {
        spdlog::debug("ClientGateway: Subscribe bad partition={} for topicId={}", partition, msg.topicId);
        sendResp(pc::ErrorCode::UnknownPartition, partition, 0);
        return;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ Kind::Transient

    // E2 Task 11 ruling (no silent stall): read_committed is served from a
    // LEADER's RC state only — a follower answered OK and then delivered nothing.
    // Refused with the bare ErrorResponse{NotLeader} (retriable: the client
    // refreshes metadata and re-routes to the leader), disk and transient alike,
    // BEFORE any subscription is registered — the one registration point both
    // worker modes go through (shard mode forwards the registered sub to the
    // owning worker below).  Also covers a group carrier switching to it (5a).
    if (msg.isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted) &&
        !readCommittedServableHere_(msg.topicId, partition)) [[unlikely]] {
        spdlog::debug("ClientGateway: read_committed Subscribe to topicId={} partition={} on a follower — "
                      "NotLeader (retriable)", msg.topicId, partition);
        sendSubRedirect(std::string{}, msg.correlationId);
        return;
    }

    // 4b. E2 residual round 6 (F1): a RESUMED plain subscription of a v7 client
    //     claims, in `key`, the data lineage its position belongs to.  A claim
    //     against a TRANSIENT partition whose ring restarted (a new lineage) is
    //     answered PartitionDataReset — never served from a clamped position
    //     (the client's resume floor would drop the refilled records: silent
    //     loss); a claim while the lineage is unknown: the retriable NotLeader.
    // FU1: a v7 PLAIN Subscribe (any — new or resumed) on a transient partition
    //     whose lineage is not confirmed yet is answered the retriable NotLeader
    //     (as OffsetFetch is): it never binds without a lineage note, so every
    //     later resume of it can claim one.  Group-member fetches resume through
    //     their group (bit-61 correlation ids) and are not gated here.
    if (lineageFn_ != nullptr && served.kind == ServedPartition::Kind::Transient &&
        (msg.correlationId & (int64_t{1} << 61)) == 0 &&
        pc::clientUnderstandsLineageNote(session->clientVersion) &&
        lineageFn_(lineageCtx_, msg.topicId, partition) == kLineageUnknown) [[unlikely]] {
        ++subscribesAwaitingLineage_;
        sendSubRedirect(std::string{}, msg.correlationId);   // retriable: bound once the lineage is known
        return;
    }
    if (lineageFn_ != nullptr && served.kind == ServedPartition::Kind::Transient && msg.partitionKnown == 1 &&
        msg.key.size() == 8 && pc::clientUnderstandsLineageNote(session->clientVersion)) [[unlikely]] {
        uint64_t claim = 0;
        for (int i = 0; i < 8; ++i) claim |= static_cast<uint64_t>(msg.key[static_cast<std::size_t>(i)]) << (8 * i);
        const uint64_t now = lineageFn_(lineageCtx_, msg.topicId, partition);
        if (claim != 0 && now == kLineageUnknown) {
            sendSubRedirect(std::string{}, msg.correlationId);   // retriable: decided once it is known
            return;
        }
        if (claim != 0 && now != 0 && now != claim) {
            ++partitionDataResets_;
            spdlog::info("ClientGateway: resumed Subscribe sessionId={} topicId={} partition={} claims data lineage "
                         "{:#x}, now {:#x} — PartitionDataReset (the client resumes from the log start)",
                         msg.sessionId, msg.topicId, partition, claim, now);
            pc::ErrorResponse er;
            er.sessionId     = msg.sessionId;
            er.correlationId = msg.correlationId;
            er.code          = static_cast<int16_t>(pc::ErrorCode::PartitionDataReset);
            const std::size_t n = er.encode(scratch_.data());
            sendAnswer_(*session, scratch_.data(), n);
            return;
        }
    }

    // 5. Clamp fromOffset to [0, HW] (GR-6) — or [0, LEO] for non-replicated topics.
    //    fromOffset < 0 → "from Latest"; fromOffset > clamp-limit → clamped.
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
    // Latest = -1 → HW (only new committed records).  Previously ALL negatives mapped
    // to LEO, which silently dropped Earliest subscriptions (0/N delivered).
    //
    // GR-6: "Latest" and "beyond-clamp" both use HW (not LEO) when a replica is
    // wired.  A subscriber starting at "Latest" should NOT sit above HW — that
    // would mean it would only receive new records AFTER HW advances past its
    // starting point, which is correct behavior since it should only see committed
    // data.  Clamping to HW also prevents the caller from providing a fromOffset
    // above HW (e.g. from a stale client cache reflecting LEO of a previous leader
    // that lost uncommitted records on failover — clamping to HW avoids a permanent
    // "nothing to deliver" stall on a reconnecting subscriber).
    static constexpr int64_t kFromEarliest = -2;
    static constexpr int64_t kFromLatest   = -1;
    // E2: a transient partition's replica is registered (servedPartition_), so
    // its clamp is the published HW below — the ring itself is never read here.
    // Sanitizer triage T2 (2026-10-14): a WORKER-owned Log (shard mode) is the
    // owning worker's state — its LEO / segment map are never read from this
    // thread (TSan: logEndOffset_ written by the worker's append, read here).
    // The clamp is the replica's published HW; the log start resolves on the
    // worker (Worker::raiseFreshDiskCursor_).  Only a gateway-owned Log
    // (inline / single-node pass-through) is read here.
    const bool gatewayLog = gatewayOwnsLog_(msg.topicId, log);
    const rune::replication::PartitionReplica* const clampReplica =
        replicaRegistry_ ? replicaRegistry_->lookup(msg.topicId, partition) : nullptr;
    if (log != nullptr && !gatewayLog && clampReplica == nullptr) [[unlikely]] {
        sendSubRedirect(std::string{}, msg.correlationId);   // retriable: no published HW for it yet
        return;
    }
    const int64_t leo = gatewayLog ? log->logEndOffset() : 0;
    // GR-6: use HW as the clamp limit when a replica is available.
    // For non-replicated / single-node topics, HW == LEO — no behavioral change.
    const int64_t clampLimit = clampReplica ? clampReplica->gatewayHighWatermark() : leo;
    // CP-5.2 FIX (Earliest-below-logStart starvation): the oldest offset that is
    // still on disk.  Retention/compaction can delete the earliest segment(s),
    // advancing the first segment base past 0.  Log::read(offset) for any offset
    // BELOW the first segment base returns {} forever (upper_bound lands on
    // begin()), so an Earliest subscriber pinned to 0 gets 0 records for the life
    // of the subscription.  Resolve Earliest — and clamp any explicit fromOffset
    // that points into a deleted region — UP to the live log start instead.
    // E2: a transient ring's start is worker-owned state (never read cross-
    // thread): Earliest resolves to 0 and the owning worker moves a cursor below
    // its ring start up to it (TRANSIENT_EVICTED, Worker::skipEvictedCursor_).
    // T2: a worker-owned log's start is resolved by its worker (fresh cursor).
    const int64_t logStart = gatewayLog ? log->logStartOffset() : 0;
    int64_t nextOffset;
    if (msg.fromOffset == kFromEarliest) {
        // Earliest → the oldest offset still readable (NOT hardcoded 0).
        nextOffset = logStart;
    } else if (msg.fromOffset < 0) {
        // Latest (-1) or any other negative sentinel → subscribe from HW (GR-6).
        (void)kFromLatest;
        nextOffset = clampLimit;
    } else if (msg.fromOffset > clampLimit) {
        // Beyond HW → clamp to HW (GR-6: don't skip ahead of committed data).
        nextOffset = clampLimit;
    } else if (msg.fromOffset < logStart) {
        // Explicit offset points into a retention/compaction-deleted region →
        // clamp up to the oldest surviving offset (avoids the same 0-delivered
        // starvation for a stale client cursor after aggressive retention).
        nextOffset = logStart;
    } else {
        nextOffset = msg.fromOffset;
    }

    // 5a. Group-consumer isolation carrier (TX-5/TX-6):
    //     A group member conveys its isolation level via a Subscribe frame AFTER
    //     the SyncGroup assignment has already created the internal group
    //     subscription (isGroup=true) through addGroupSubscription().  Treat this
    //     Subscribe as an UPDATE of that existing group subscription's isolation
    //     rather than a second, independent push subscription.
    //
    //     Without this, the same (session, topicId, partition) would carry TWO
    //     subscriptions — the group one (created read_uncommitted by
    //     GroupCoordinationService::handleSyncGroup) and this per-session one —
    //     both driven by pushSubscriptions().  That produced (a) 2× delivery of
    //     every record and (b) an isolation leak: the group subscription stayed
    //     read_uncommitted and delivered aborted/open transactional records even
    //     when the client asked for read_committed.  We must NOT reset the group
    //     subscription's nextOffset (seeded from the committed group offset) — we
    //     only carry the isolation across.
    {
        const auto memIt = groupMemberSubs_.find(msg.sessionId);
        if (memIt != groupMemberSubs_.end()) {
            for (const int64_t existingSubId : memIt->second) {
                auto subIt = subscriptions_.find(existingSubId);
                if (subIt == subscriptions_.end()) continue;
                Subscription& gsub = subIt->second;
                if (gsub.isGroup && gsub.topicId == msg.topicId &&
                    gsub.partition == partition) {
                    // Phase 2c (spec 13.3, M2): carriers never carry filters - a group's filter is set at JoinGroup.
                    // An older session's trailing bytes were ignored at decode (msg.filter is empty for it).
                    if (pc::clientUnderstandsFilters(session->clientVersion) && !msg.filter.empty()) [[unlikely]] {
                        sendResp(pc::ErrorCode::InvalidRequest, partition, 0,
                                 "group filters are set at JoinGroup, not on a group subscription");
                        return;
                    }
                    // TX-6b: Mark carrier received and update isolation.
                    // The push loop stalls pre-carrier group subs at any transactional
                    // batch that would be abort-filtered (so nextOffset has NOT advanced
                    // past aborted records yet).  Marking groupCarrierReceived=true here
                    // unblocks the push loop on the next duty cycle with the correct
                    // isolation level: isolation=0 will deliver aborted records;
                    // isolation=1 will skip them — both correctly, without duplicates or
                    // permanent record loss.
                    gsub.isolation            = msg.isolation;
                    gsub.groupCarrierReceived = true;
                    gsub.parkedAtHw           = -1;   // F15 R-3: a new isolation → read again
                    spdlog::debug("ClientGateway: Subscribe carries isolation={} to group "
                                  "sub sessionId={} subId={} topic={} partition={} "
                                  "(no duplicate subscription)",
                                  msg.isolation, msg.sessionId, existingSubId,
                                  msg.topicId, partition);
                    sendResp(pc::ErrorCode::OK, partition, existingSubId);
                    return;
                }
            }
        }
    }

    // 5b. Subscribe dedupe (2026-10-05) — AFTER the group-carrier update (review M-1:
    //     a carrier must always reach its group subscription, never a stale plain entry): (session, correlationId[, topic, partition]) is ONE
    //     subscription.  A re-sent Subscribe (the client resends after redirect /
    //     reconnect paths; it binds only one instance per correlationId) used to
    //     register a SECOND subscription — a leaked duplicate stream for the whole
    //     session (an Earliest subscription replayed the log twice).  Re-answer the
    //     existing subscription and create nothing: this one registration point
    //     feeds every push path (gateway inline, the owning worker's Subscribe op,
    //     its queued pendingSubOps_ retry and deferred record-egress waiters), so
    //     none of them can see a duplicate.  Placed after the leadership +
    //     validation checks so a duplicate that now belongs elsewhere is still
    //     redirected.
    //     Keyed (correlationId, topicId, partition): clients that send untagged
    //     Subscribes (group carriers: correlationId 0 per partition) keep one
    //     subscription per partition.
    const Session::SubKey subKey{msg.correlationId, msg.topicId, partition};
    if (const auto dup = session->subIdByKey.find(subKey); dup != session->subIdByKey.end()) {
        const auto existing = subscriptions_.find(dup->second);
        if (existing != subscriptions_.end() && existing->second.sessionId == msg.sessionId) {
            spdlog::debug("ClientGateway: duplicate Subscribe sessionId={} corrId={} topic={} partition={} "
                          "→ existing subId={} (no second subscription)",
                          msg.sessionId, msg.correlationId, msg.topicId, partition, dup->second);
            if (!existing->second.okOwed) sendResp(pc::ErrorCode::OK, partition, dup->second);   // F1: else it is owed
            return;
        }
        session->subIdByKey.erase(dup);   // stale index entry (sub already gone)
    }

    // Phase 2c (spec 6 step 1, 9, FD1, FD3): a filter is read only from a session >= 11 (older sessions' trailing
    // bytes were ignored at decode).  Every refusal is answered with its detail; nothing is registered.  Placed after
    // the group-carrier and dedupe branches: a duplicate Subscribe re-answers its existing subscription (one ticket).
    std::span<const uint8_t> filterBytes;
    FilterAdmission::Ticket  ticket;
    if (pc::clientUnderstandsFilters(session->clientVersion) && !msg.filter.empty()) [[unlikely]] {
        if (!perWorkerEgressActive_ || !workerPool_) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0, "filter needs RUNE_WORKER_MODE=shard");
            return;
        }
        const rune::codec::FilterSchema* fs = msg.topicId >= 0 ? filterSchemas_.find(msg.topicId) : nullptr;
        if (fs == nullptr) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0,
                     rune::codec::describe({rune::codec::FilterErrorKind::UntypedTopic}));
            return;
        }
        if (filterSchemaTooLargeTopics_.contains(msg.topicId)) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0,
                     "filter schema too large for the worker ring (RUNE_WORKER_INGRESS_RING_BYTES)");
            return;
        }
        if (!workerPool_->fitsIngressTail(msg.filter.size())) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0,
                     "filter too large for the worker ring (RUNE_WORKER_INGRESS_RING_BYTES)");
            return;
        }
        auto c = rune::codec::CompiledFilter::compile(msg.filter, *fs);
        if (!c) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0, rune::codec::describe(c.error()));
            return;
        }
        const auto& it = c->term(c->indexTerm());
        auto t = filterAdmission_.admit(msg.topicId, partition, it.tag, c->values(c->indexTerm()));
        if (!t) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0, t.error());
            return;
        }
        filterBytes = msg.filter;
        ticket      = std::move(*t);
    }

    // 6. Allocate subscriptionId and register subscription.
    const int64_t subId = allocSubscriptionId_();

    Subscription sub;
    sub.subscriptionId = subId;
    sub.sessionId      = msg.sessionId;
    sub.topicId        = msg.topicId;
    sub.partition      = partition;
    sub.nextOffset     = nextOffset;
    sub.isolation      = msg.isolation;
    sub.subscribeCorrId = msg.correlationId;
    sub.hadReplica      = replicaRegistry_ != nullptr &&
                          static_cast<bool>(replicaRegistry_->lookupHandle(msg.topicId, partition));
    sub.filter.assign(filterBytes.begin(), filterBytes.end());
    sub.filterTicket = std::move(ticket);

    // PR #441 F1 (issue #118): under authentication a worker-pushed partition answers OK only once the session's own
    // record egress for the owning worker exists (now, or when it resolves); a failed resolve then refuses the
    // still-pending Subscribe NotLeader - the client's budget for THAT request counts, and runs out explicitly.
    const bool workerPushed = perWorkerEgressActive_ && workerPool_ && msg.topicId >= 0;
    if (workerPushed && authMode_ == rune::gateway::GatewayAuthMode::Required) {
        const int32_t wid = workerPool_->ownerWorkerId(msg.topicId, partition);
        const auto eg = session->recordEgressByWorker.find(wid);
        sub.okOwed        = eg == session->recordEgressByWorker.end() || !eg->second;
        sub.okOwedLineage = sub.okOwed && served.kind == ServedPartition::Kind::Transient;
    }
    const bool okOwed = sub.okOwed;
    subscriptions_.emplace(subId, std::move(sub));
    session->subIdByKey[subKey] = subId;   // 5a: the dedupe index

    // read-rework T3/T4 (I3b): when per-worker egress is active, route the push to
    // the OWNING worker — it owns the cursor and pushes on its own dynamic-MDC
    // egress.  User partitions only (internal topicId < 0 stay gateway-pushed).
    // The gateway keeps its subscriptions_ record for setup/teardown; its
    // pushSubscriptions() skips these subs (see the per-worker-egress guard there).
    if (workerPushed) {
        forwardSubscribeToWorker_(subId, msg.sessionId, msg.topicId, partition,
                                  nextOffset, msg.isolation,
                                  /*isGroup=*/false, /*groupCarrierReceived=*/false,
                                  /*subscribeCorrId=*/msg.correlationId);
    }
    if (okOwed) return;   // F1: the OK (and the lineage note) follow the egress, or the refusal replaces them

    // Reply SubscribeResponse{OK, partition, subscriptionId}.
    sendResp(pc::ErrorCode::OK, partition, subId);
    if (served.kind == ServedPartition::Kind::Transient) noteLineage_(subId);   // round 6 (F1)

    spdlog::debug("ClientGateway: Subscribe sessionId={} subId={} topic={} partition={} nextOffset={} isolation={}",
                  msg.sessionId, subId, msg.topicId, partition, nextOffset, msg.isolation);
}

// ---------------------------------------------------------------------------
// Catalog history Task 12: a Subscribe on `__catalog` (topic id −6).  Every one
// is answered: a pre-v8 session as before (UnknownTopic — it cannot parse the
// caught-up marker), partition ≠ 0 / a key-resolved partition InvalidRequest, no
// admin thread Internal, a full admin ring the retriable NotLeader; otherwise
// the admin thread sends the SubscribeResponse, the backlog up to this broker's
// HW, the caught-up marker and the live events.
// ---------------------------------------------------------------------------
void ClientGateway::noteReadCorr_(std::vector<int64_t>& corrs, int64_t corr)
{
    for (const int64_t c : corrs) if (c == corr) return;
    if (corrs.size() >= Session::kMaxReadCorrs) corrs.erase(corrs.begin());
    corrs.push_back(corr);
}

void ClientGateway::handleCatalogSubscribe_(Session& session, const pc::Subscribe& msg)
{
    auto answer = [&](pc::ErrorCode ec) {
        pc::SubscribeResponse resp;
        resp.sessionId      = msg.sessionId;
        resp.correlationId  = msg.correlationId;
        resp.errorCode      = static_cast<int16_t>(ec);
        resp.partition      = msg.partition;
        resp.subscriptionId = 0;
        const std::size_t n = resp.encode(scratch_.data());
        sendAnswer_(session, scratch_.data(), n);
    };
    if (!pc::clientUnderstandsCatalogDdl(session.clientVersion)) {
        answer(pc::ErrorCode::UnknownTopic);
        return;
    }
    if (msg.partitionKnown != 1 || msg.partition != 0) {
        spdlog::debug("ClientGateway: catalog Subscribe sessionId={} partition={} known={} — InvalidRequest "
                      "(__catalog has partition 0 only)", msg.sessionId, msg.partition, msg.partitionKnown);
        answer(pc::ErrorCode::InvalidRequest);
        return;
    }
    if (adminSink_ == nullptr) {
        answer(pc::ErrorCode::Internal);
        return;
    }
    // Issue #117: an end of this kind still owed to the admin goes first (FIFO) - else it would end the new read.
    const bool endOwed = (session.pendingReadEnds & Session::kPendingEndCatalogReads) != 0 && !submitReadEnds_(session);
    if (endOwed || !adminSink_->submitCatalogSubscribe(msg.sessionId, msg.correlationId, msg.fromOffset)) {
        // Back-pressure on the admin ring: the retriable NotLeader (the client retries).
        pc::ErrorResponse er;
        er.sessionId     = msg.sessionId;
        er.correlationId = msg.correlationId;
        er.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        const std::size_t n = er.encode(scratch_.data());
        sendAnswer_(session, scratch_.data(), n);
        return;
    }
    noteReadCorr_(session.catalogReadCorrs, msg.correlationId);
    ++catalogSubscribesForwarded_;
}

// ---------------------------------------------------------------------------
// Authentication Task 10: a Subscribe on `__security` (topic id -7) - handleCatalogSubscribe_'s contract for the
// second read server.  A pre-v9 session as before (UnknownTopic), partition != 0 / key-resolved InvalidRequest, no
// admin thread Internal, a full admin ring the retriable NotLeader.
// ---------------------------------------------------------------------------
void ClientGateway::handleSecuritySubscribe_(Session& session, const pc::Subscribe& msg)
{
    auto answer = [&](pc::ErrorCode ec) {
        pc::SubscribeResponse resp;
        resp.sessionId      = msg.sessionId;
        resp.correlationId  = msg.correlationId;
        resp.errorCode      = static_cast<int16_t>(ec);
        resp.partition      = msg.partition;
        resp.subscriptionId = 0;
        const std::size_t n = resp.encode(scratch_.data());
        sendAnswer_(session, scratch_.data(), n);
    };
    if (!pc::clientUnderstandsAuth(session.clientVersion)) {
        answer(pc::ErrorCode::UnknownTopic);
        return;
    }
    if (msg.partitionKnown != 1 || msg.partition != 0) {
        answer(pc::ErrorCode::InvalidRequest);
        return;
    }
    if (adminSink_ == nullptr) {
        answer(pc::ErrorCode::Internal);
        return;
    }
    if (securityUnavailable_()) [[unlikely]] {   // issues #15 / #111: a stale replica serves no `__security` read
        answer(pc::ErrorCode::AuthUnavailable);
        return;
    }
    // Issue #117: an end of this kind still owed to the admin goes first (FIFO) - else it would end the new read.
    const bool endOwed = (session.pendingReadEnds & Session::kPendingEndSecurityReads) != 0 && !submitReadEnds_(session);
    if (endOwed || !adminSink_->submitSecuritySubscribe(msg.sessionId, msg.correlationId, msg.fromOffset)) {
        pc::ErrorResponse er;
        er.sessionId     = msg.sessionId;
        er.correlationId = msg.correlationId;
        er.code          = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        const std::size_t n = er.encode(scratch_.data());
        sendAnswer_(session, scratch_.data(), n);
        return;
    }
    noteReadCorr_(session.securityReadCorrs, msg.correlationId);
    ++securitySubscribesForwarded_;
}

// The admin request an Unsubscribe id names: a `__security` subscription id (or kEndSecurityReadsOnly) the second read
// server, anything else (-1 = a session close, which the admin applies to both; kEndCatalogReadsOnly) the catalog route.
bool ClientGateway::submitCatalogOrSecurityUnsub_(int64_t sid, int64_t subscriptionId)
{
    if (subscriptionId == rune::gateway::kEndSecurityReadsOnly) return adminSink_->submitSecurityUnsubscribe(sid, -1);
    if (rune::gateway::isSecuritySubscriptionId(subscriptionId)) return adminSink_->submitSecurityUnsubscribe(sid, subscriptionId);
    return adminSink_->submitCatalogUnsubscribe(sid, subscriptionId);
}

void ClientGateway::forwardCatalogUnsubscribe_(int64_t sid, int64_t subscriptionId)
{
    if (adminSink_ == nullptr) return;
    // Issue #117: an authorization end of a LIVE session is never subject to the cap below (it was dropped there, and
    // a session that lost ADMIN kept streaming `__security`): the session holds it until the admin ring takes it.
    if (subscriptionId == rune::gateway::kEndCatalogReadsOnly || subscriptionId == rune::gateway::kEndSecurityReadsOnly) {
        if (Session* s = sessions_.find(sid))
            endAdminLogReads_(*s, subscriptionId == rune::gateway::kEndSecurityReadsOnly ? Session::kPendingEndSecurityReads
                                                                                     : Session::kPendingEndCatalogReads);
        return;   // no session: its close (-1) ends both servers' reads
    }
    // FIFO behind any already waiting (a close after a single unsubscribe stays after it).
    // A `__security` subscription id names the second read server (its own request-ring message).
    if (pendingCatalogUnsubs_.empty() && submitCatalogOrSecurityUnsub_(sid, subscriptionId)) return;
    // M3: a close supersedes the session's waiting single unsubscribes; duplicates coalesce.
    if (subscriptionId == -1)
        std::erase_if(pendingCatalogUnsubs_, [sid](const PendingCatalogUnsubscribe& p) { return p.sessionId == sid; });
    else
        for (const auto& p : pendingCatalogUnsubs_)
            if (p.sessionId == sid && (p.subscriptionId == subscriptionId || p.subscriptionId == -1)) return;
    if (pendingCatalogUnsubs_.size() >= kPendingCatalogUnsubsCap) [[unlikely]] {
        if (subscriptionId != -1) {
            // Explicit, bounded: the subscription ends with its session's close at the latest.
            ++catalogUnsubsDropped_;
            spdlog::error("ClientGateway: catalog Unsubscribe {:#x} (session {}) not queued — {} waiting for the "
                          "admin ring (the cap); it ends with the session (dropped so far: {})", subscriptionId, sid,
                          pendingCatalogUnsubs_.size(), catalogUnsubsDropped_);
            return;
        }
        // A close is never dropped: it replaces the oldest single unsubscribe.
        for (auto it = pendingCatalogUnsubs_.begin(); it != pendingCatalogUnsubs_.end(); ++it)
            if (it->subscriptionId != -1) { pendingCatalogUnsubs_.erase(it); ++catalogUnsubsDropped_; break; }
        if (pendingCatalogUnsubs_.size() >= kPendingCatalogUnsubsCap) {
            // Fix round 3 (F3b): never lost — the admin's session-liveness probe
            // (every catalog subscription, each second) finds the session gone and
            // reclaims its subscriptions.  Counted; no queue growth.
            if (catalogClosesToSweep_++ % 1024 == 0)
                spdlog::warn("ClientGateway: {} closed sessions wait for the admin ring (the cap) — session {}'s "
                             "catalog subscriptions are reclaimed by the liveness sweep (so far: {})",
                             pendingCatalogUnsubs_.size(), sid, catalogClosesToSweep_);
            return;
        }
    }
    pendingCatalogUnsubs_.push_back(PendingCatalogUnsubscribe{sid, subscriptionId});   // reserved: never allocates
}

void ClientGateway::endAdminLogReads_(Session& s, uint8_t bit)
{
    const bool listed = s.pendingReadEnds != 0;
    s.pendingReadEnds |= bit;   // coalesced: one end per kind ends every read of that kind
    if (submitReadEnds_(s) || listed) return;
    sessionsWithReadEnds_.push_back(s.sessionId);   // one entry per session: bounded by the sessions
}

bool ClientGateway::submitReadEnds_(Session& s)
{
    if (adminSink_ == nullptr) { s.pendingReadEnds = 0; return true; }
    if ((s.pendingReadEnds & Session::kPendingEndCatalogReads) != 0 &&
        adminSink_->submitCatalogUnsubscribe(s.sessionId, rune::gateway::kEndCatalogReadsOnly))
        s.pendingReadEnds &= static_cast<uint8_t>(~Session::kPendingEndCatalogReads);
    if ((s.pendingReadEnds & Session::kPendingEndSecurityReads) != 0 &&
        adminSink_->submitSecurityUnsubscribe(s.sessionId, -1))
        s.pendingReadEnds &= static_cast<uint8_t>(~Session::kPendingEndSecurityReads);
    return s.pendingReadEnds == 0;
}

int ClientGateway::flushPendingReadEnds_()
{
    int sent = 0;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < sessionsWithReadEnds_.size(); ++i) {
        Session* s = sessions_.find(sessionsWithReadEnds_[i]);
        if (s == nullptr) continue;                      // closed: its close (-1) supersedes the ends
        if (submitReadEnds_(*s)) { ++sent; continue; }
        sessionsWithReadEnds_[keep++] = sessionsWithReadEnds_[i];
    }
    sessionsWithReadEnds_.resize(keep);
    return sent;
}

ClientGateway::AdminLogAdmit ClientGateway::admitAdminLogFrame(int64_t sessionId, int64_t subId, const uint8_t* frame,
                                                              std::size_t len)
{
    using rune::security::Operation;
    if (authMode_ != rune::gateway::GatewayAuthMode::Required) return AdminLogAdmit::Deliver;
    const Session* s = sessions_.find(sessionId);
    if (s == nullptr) return AdminLogAdmit::Deliver;    // the offer answers NoSession
    if (securityBehind_) [[unlikely]] return AdminLogAdmit::Hold;   // #603: the grant below may already be revoked
    const bool security = rune::gateway::isSecuritySubscriptionId(subId);
    const PermissionTable& perms = permsOf_(*s);
    if (!perms.allowsCluster(rune::security::opBit(security ? Operation::Admin : Operation::Describe))) {
        ++adminLogFramesWithheld_;
        return AdminLogAdmit::Withhold;
    }
    if (!security) return AdminLogAdmit::Deliver;
    // A `__security` Records frame: [header 24][body 24][u32 batch bytes][batches].  Its newest EVENT (control batches
    // are consumed by the applier, never mirrored) must be applied by this thread's SecurityView first - otherwise a
    // revoke the admin applied and streamed ahead of this thread would let the events after it (verifiers) through.
    constexpr std::size_t kBatchesAt = 24 + 24 + 4;
    if (len <= kBatchesAt) return AdminLogAdmit::Deliver;   // the epoch note / marker carry no event
    uint32_t batchBytes = 0;
    std::memcpy(&batchBytes, frame + kBatchesAt - 4, 4);
    std::size_t rem = std::min<std::size_t>(batchBytes, len - kBatchesAt);
    const uint8_t* p = frame + kBatchesAt;
    int64_t newestEvent = -1;
    while (rem >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader reader(p, rem);
        if (!reader.ensureValid()) break;
        if (!reader.isControl())
            newestEvent = std::max(newestEvent, reader.baseOffset() + static_cast<int64_t>(reader.lastOffsetDelta()));
        const auto tl = static_cast<std::size_t>(reader.totalLength());
        if (tl == 0 || tl > rem) break;
        p += tl;
        rem -= tl;
    }
    if (newestEvent < 0) return AdminLogAdmit::Deliver;
    if (securityView_ == nullptr || securityView_->halted()) {   // fail closed: the halt revokes the session
        ++adminLogFramesWithheld_;
        return AdminLogAdmit::Withhold;
    }
    return newestEvent <= securityView_->appliedOffset() ? AdminLogAdmit::Deliver : AdminLogAdmit::Hold;
}

int ClientGateway::flushPendingCatalogUnsubs_()
{
    std::size_t sent = 0;
    while (sent < pendingCatalogUnsubs_.size() &&
           submitCatalogOrSecurityUnsub_(pendingCatalogUnsubs_[sent].sessionId, pendingCatalogUnsubs_[sent].subscriptionId))
        ++sent;
    if (sent != 0)
        pendingCatalogUnsubs_.erase(pendingCatalogUnsubs_.begin(),
                                    pendingCatalogUnsubs_.begin() + static_cast<std::ptrdiff_t>(sent));
    return static_cast<int>(sent);
}

ClientGateway::CatalogOffer ClientGateway::tryOfferCatalogFrame(int64_t sessionId, const uint8_t* buf,
                                                                std::size_t len)
{
    Session* s = sessions_.find(sessionId);
    if (s == nullptr) return CatalogOffer::NoSession;
    if (catalogBackPressureForTest_ == sessionId) [[unlikely]] return CatalogOffer::BackPressured;
    // Answers queued ahead of it keep their order: the Records waits (never spills).
    if (s->spill.state != SpillState::Normal) return CatalogOffer::BackPressured;
    if (s->egress) s->catalogWindowSeen = std::max(s->catalogWindowSeen, s->egress->availableWindow());   // C2
    if (!sendTo(*s, buf, len)) return CatalogOffer::BackPressured;
    if (onControlFrame_) onControlFrame_(sessionId, buf, len);   // M4: a capture sees only what was sent
    return CatalogOffer::Sent;
}

ClientGateway::CatalogRoom ClientGateway::catalogEgressRoom(int64_t sessionId)
{
    Session* s = sessions_.find(sessionId);
    if (s == nullptr) return CatalogRoom::Gone;
    // #603 review: a subscription paused because this broker's `__security` view is behind (admitAdminLogFrame Hold)
    // resumes only once it is not - otherwise every pass would resume it, the admin re-read it and the gate hold it
    // again, a busy loop on the admin thread for as long as the hold lasts.
    if (securityBehind_) [[unlikely]] return CatalogRoom::NotYet;
    if (catalogBackPressureForTest_ == sessionId) [[unlikely]] return CatalogRoom::NotYet;
    if (s->spill.state != SpillState::Normal) return CatalogRoom::NotYet;
    if (!s->egress) return CatalogRoom::Ready;   // egress-less test session (capture hook)
    if (!s->egress->isConnected()) return CatalogRoom::NotYet;
    const int64_t w = s->egress->availableWindow();
    s->catalogWindowSeen = std::max(s->catalogWindowSeen, w);
    const int64_t need = catalogResumeThreshold(s->egress->termBufferLength(), s->catalogWindowSeen);
    return w >= need ? CatalogRoom::Ready : CatalogRoom::NotYet;
}

// Task 22 fix round 1 (C1): the shared Redirect / NotLeader refusal (the body of
// handleSubscribe's former sendSubRedirect lambda, DRY with GetViewRow).
void ClientGateway::sendRouteRefusal_(Session& s, int64_t sessionId, int64_t corr, std::string_view endpoint)
{
    pc::ErrorResponse errResp;
    errResp.sessionId     = sessionId;
    errResp.correlationId = corr;
    if (!endpoint.empty()) {
        errResp.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
        errResp.message = endpoint;
        spdlog::debug("ClientGateway: per-partition redirect sessionId={} → {}", sessionId, endpoint);
    } else {
        errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        spdlog::debug("ClientGateway: per-partition NotLeader (endpoint unknown) sessionId={}", sessionId);
    }
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE +
                               pc::ErrorResponse::BODY_FIXED + 2 +
                               errResp.message.size();
    if (needed > scratch_.size()) [[unlikely]] {
        // Fix round 2: an endpoint too long for the frame is never dropped —
        // the client gets the bare NotLeader (it refreshes metadata, retries).
        errResp.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        errResp.message = {};
    }
    const std::size_t encoded = errResp.encode(scratch_.data());
    sendAnswer_(s, scratch_.data(), encoded);  // CG round 3: queued, not best-effort
}

// Task 22 fix round 1 (C1) — view -> source topic (control thread, cold writes).
void ClientGateway::noteViewSource(int32_t viewTopicId, int32_t sourceTopicId)
{
    const auto it = std::lower_bound(viewSources_.begin(), viewSources_.end(), viewTopicId,
                                     [](const auto& e, int32_t id) { return e.first < id; });
    if (it != viewSources_.end() && it->first == viewTopicId) it->second = sourceTopicId;
    else viewSources_.insert(it, {viewTopicId, sourceTopicId});
}

void ClientGateway::forgetViewSource(int32_t viewTopicId)
{
    const auto it = std::lower_bound(viewSources_.begin(), viewSources_.end(), viewTopicId,
                                     [](const auto& e, int32_t id) { return e.first < id; });
    if (it != viewSources_.end() && it->first == viewTopicId) viewSources_.erase(it);
}

int32_t ClientGateway::viewSourceOf_(int32_t viewTopicId) const noexcept
{
    const auto it = std::lower_bound(viewSources_.begin(), viewSources_.end(), viewTopicId,
                                     [](const auto& e, int32_t id) { return e.first < id; });
    return (it != viewSources_.end() && it->first == viewTopicId) ? it->second : -1;
}

int32_t ClientGateway::nthViewSinkOfSource_(int32_t sourceTopicId, std::size_t n) const noexcept
{
    for (const auto& e : viewSources_)
        if (e.second == sourceTopicId && n-- == 0) return e.first;
    return -1;
}

bool ClientGateway::viewSourceLedElsewhere_(int32_t viewTopicId, int32_t partition,
                                            std::string_view& endpoint) const noexcept
{
    endpoint = {};
    if (!routingImage_) return false;
    const int32_t src = viewSourceOf_(viewTopicId);
    if (src < 0) return false;
    const auto& e = routingImage_.get()->lookup(rune::metadata::gpidOf(src, partition));
    if (e.leaderNodeId == thisNodeId_) return false;
    if (e.leaderNodeId != rune::metadata::RoutingImage::NO_LEADER_NODE_ID)
        if (const auto it = nodeEndpointMap_.find(e.leaderNodeId); it != nodeEndpointMap_.end())
            endpoint = it->second;
    return true;
}

void ClientGateway::noteViewSinkAwaitingPlan(int32_t viewTopicId)
{
    if (!viewSinkAwaitingPlan(viewTopicId)) viewSinksAwaitingPlan_.push_back(viewTopicId);   // cold
}

void ClientGateway::noteViewPlanDelivered(int32_t viewTopicId)
{
    std::erase(viewSinksAwaitingPlan_, viewTopicId);
}

// ---------------------------------------------------------------------------
// E2 residual round 6 (F1): the transient LINEAGE NOTE — a Records frame of the
// subscription whose recordBatch is exactly the 8-byte LE lineage token (see
// rune-protocol kClientVersionLineageNote), on the session's reliable egress
// after its SubscribeResponse.  Plain subscriptions of v7 sessions only; a
// lineage still unknown at bind is noted once it resolves (doWork, cold).
// ---------------------------------------------------------------------------
void ClientGateway::sendLineageNote_(Session& session, int64_t subId, const Subscription& sub, uint64_t lineage) {
    uint8_t token[8];
    for (int i = 0; i < 8; ++i) token[i] = static_cast<uint8_t>(lineage >> (8 * i));
    pc::Records note;
    note.sessionId      = sub.sessionId;
    note.correlationId  = sub.subscribeCorrId;
    note.subscriptionId = subId;
    note.topicId        = sub.topicId;
    note.partition      = sub.partition;
    note.baseOffset     = -1;
    note.recordBatch    = std::span<const uint8_t>(token, 8);
    const std::size_t n = note.encode(scratch_.data());
    sendAnswer_(session, scratch_.data(), n);
    ++lineageNotesSent_;
}

void ClientGateway::noteLineage_(int64_t subId) {
    if (lineageFn_ == nullptr) return;
    const auto it = subscriptions_.find(subId);
    if (it == subscriptions_.end() || it->second.isGroup) return;
    constexpr int64_t kGroupFetchCorrTag = int64_t{1} << 61;   // group-member fetches resume via their group
    if ((it->second.subscribeCorrId & kGroupFetchCorrTag) != 0) return;
    Session* session = sessions_.find(it->second.sessionId);
    if (session == nullptr || !pc::clientUnderstandsLineageNote(session->clientVersion)) return;
    const uint64_t now = lineageFn_(lineageCtx_, it->second.topicId, it->second.partition);
    if (now == 0) return;
    if (now == kLineageUnknown) {
        try { pendingLineageNotes_.push_back(subId); } catch (...) {}   // cold; a lost note = no claim (old behaviour)
        return;
    }
    sendLineageNote_(*session, subId, it->second, now);
}

int ClientGateway::flushPendingLineageNotes_() {
    int work = 0;
    std::size_t kept = 0;
    for (std::size_t i = 0; i < pendingLineageNotes_.size(); ++i) {
        const int64_t subId = pendingLineageNotes_[i];
        const auto it = subscriptions_.find(subId);
        Session* session = it != subscriptions_.end() ? sessions_.find(it->second.sessionId) : nullptr;
        if (session == nullptr) { ++work; continue; }   // gone
        const uint64_t now = lineageFn_(lineageCtx_, it->second.topicId, it->second.partition);
        if (now == kLineageUnknown) { pendingLineageNotes_[kept++] = subId; continue; }
        if (now != 0) sendLineageNote_(*session, subId, it->second, now);
        ++work;
    }
    pendingLineageNotes_.resize(kept);
    return work;
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
        // No-hang (R2): Unsubscribe has no response type — nothing waits on it
        // (the client removes its routing entry locally; the broker-side
        // subscription dies with the session at the latest).  Counted + warned.
        noteUnanswerable_(buf, len, "Unsubscribe body undecodable (no response type)");
        return;
    }
    // Catalog history Task 12: a catalog subscription lives on the admin thread
    // (its owner check is there too).
    if (rune::gateway::isCatalogSubscriptionId(msgOpt->subscriptionId)) [[unlikely]] {
        forwardCatalogUnsubscribe_(msgOpt->sessionId, msgOpt->subscriptionId);
        return;
    }
    // Only the owning session may remove a subscription (an Unsubscribe from
    // another session — a stale client episode, another client — is ignored).
    {
        const auto own = subscriptions_.find(msgOpt->subscriptionId);
        if (own != subscriptions_.end() && own->second.sessionId != msgOpt->sessionId) {
            spdlog::debug("ClientGateway: Unsubscribe subId={} from sessionId={} — owned by sessionId={}, ignored",
                          msgOpt->subscriptionId, msgOpt->sessionId, own->second.sessionId);
            return;
        }
    }
    // read-rework T3/T4 (I3b): tell the owning worker to drop the cursor too.
    if (perWorkerEgressActive_ && workerPool_) {
        auto it = subscriptions_.find(msgOpt->subscriptionId);
        if (it != subscriptions_.end() && it->second.topicId >= 0) {
            forwardUnsubscribeToWorker_(msgOpt->subscriptionId,
                                        it->second.topicId, it->second.partition);
        }
    }

    // 5a dedupe index: a later Subscribe with the same correlationId is a NEW
    // subscription once this one is gone.
    if (const auto own = subscriptions_.find(msgOpt->subscriptionId); own != subscriptions_.end()) {
        if (Session* s = sessions_.find(own->second.sessionId)) {
            const auto ix = s->subIdByKey.find(Session::SubKey{own->second.subscribeCorrId, own->second.topicId,
                                                               own->second.partition});
            if (ix != s->subIdByKey.end() && ix->second == msgOpt->subscriptionId) s->subIdByKey.erase(ix);
        }
    }
    const std::size_t removed = subscriptions_.erase(msgOpt->subscriptionId);
    spdlog::debug("ClientGateway: Unsubscribe subId={} removed={}",
                  msgOpt->subscriptionId, removed);
}

// ---------------------------------------------------------------------------
// pushSubscriptions (GB-3 / GR-6) — push new Records from the Log to each subscriber.
//
// Called from doWork() AFTER the ingress poll (single-writer, duty-cycle thread).
//
// For each active subscription:
//   - Compute isolation bound (GR-6):
//       When replicaRegistry_ is wired, look up the PartitionReplica.  If found,
//       use replica->highWatermark() as the delivery bound — this is the
//       majority-committed offset; no record above HW can ever be delivered to a
//       subscriber regardless of isolation setting.  A record above HW has been
//       written to the leader's log (LEO advanced) but has NOT been acknowledged
//       by a majority of replicas.  If the leader fails before majority replication,
//       the incoming leader truncates those records; a subscriber that already
//       received them would hold data that is no longer on any replica — a
//       correctness violation.  HW-bounding prevents this.
//
//       Isolation semantics post-GR-6:
//         isolation=0 (read_uncommitted): still bounded by HW.  The "uncommitted"
//           in the Kafka sense refers to open TRANSACTIONS (no-op in Milestone B/C
//           scope here) — it does NOT mean "allow delivery of truncatable records".
//           We default ALL subscribers to HW-bounded to be safe.
//         isolation=1 (read_committed): additionally bounded by the LSO and
//           aborted batches are filtered.  F15 S4 (Option B, I-B): with a worker
//           pool (every production mode) the gateway HW snapshot only bounds the
//           PushRead REQUEST — the owning worker decides read_committed from its
//           own RC state (executePushReadWire_).  Only the no-pool inline path
//           below keeps a gateway-side LSO (unit rigs; deletion scheduled T16).
//
//       If no replica is found (topic not replicated, or replicaRegistry_ null),
//       fall back to logEndOffset() — single-node / non-replicated path where HW
//       equals LEO by definition (no followers to lag).
//
//   - If nextOffset >= bound → skip (nothing committed to push yet).
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
// Replica lookup is O(1) (unordered_map pointer; no alloc).
//
// Correctness guarantee: nextOffset is NEVER advanced past a batch that was not
// successfully pushed (tryClaim > 0).  This ensures exactly-once delivery in order,
// even under back-pressure.
// ---------------------------------------------------------------------------
void ClientGateway::pushSubscriptions()
{
    ++pushCycle_;   // F15 S4 fix round 1: the backoff clock (cycles, never wall time)
    pushSubscriptionsPass_();
    // E2 final fix wave (N2): the moved-away summary is logged at the END of
    // the pass that retired subscriptions — prompt (the same pass), and
    // accurate (a routed sweep's whole count, never a partial one) — then at
    // most once per kMovedAwaySummaryEveryNs of wall time (retirements in the
    // window are carried into the next summary, logged as soon as it elapses).
    if (movedAwaySinceSummary_ != 0 && nowNs_ >= nextMovedAwaySummaryNs_) [[unlikely]]
        logMovedAwaySummary_();
}

void ClientGateway::pushSubscriptionsPass_()
{
    using namespace rune::protocol::batch;

    if (subscriptions_.empty()) return;

    // Task 11 carry (E2 Task 13): a plain subscription this node can no longer
    // serve (movedAway_: read_committed after a demotion; any isolation after
    // its replica was removed) is answered NotLeader and dropped — both worker
    // modes (checked BEFORE the per-worker-egress skip: the owning worker
    // cannot tell the client, it only stops pushing).
    //   - Routed trigger (immediate): every routed role change / replica
    //     removal bumps the registry's roleChanges(); the routed role is
    //     recorded BEFORE the owner flips, and movedAway_ reads it
    //     (publishedLeader() is false once intended == Follower).
    //   - Fallback (fix round 2: spread): a demotion no route reports (fenced by
    //     a higher epoch) — every kMovedAwayScanEveryCycles passes, a slice of
    //     at most ~kMovedAwayLookupsPerSlice subscriptions behind a bucket
    //     cursor (scanMovedAwaySlice_); never an O(subs) spike.
    //   - E2 final fix wave (M4): routed changes are COALESCED — the first one
    //     after a quiet period sweeps at once; while changes keep arriving (a
    //     controlled shutdown moves many partitions, one routed pass each) at
    //     most one sweep per kRoutedSweepMinGapCycles passes covers them all.
    //     Never one O(subscriptions) probe pass per role change.
    if (replicaRegistry_ != nullptr && replicaRegistry_->roleChanges() != seenRcSweepRoleChanges_) [[unlikely]] {
        seenRcSweepRoleChanges_ = replicaRegistry_->roleChanges();
        routedSweepPending_     = true;
    }
    const bool routedSweep = routedSweepPending_ && pushCycle_ >= nextRoutedSweepCycle_;
    if (routedSweep) [[unlikely]] {
        routedSweepPending_   = false;
        nextRoutedSweepCycle_ = pushCycle_ + kRoutedSweepMinGapCycles;
        ++routedMovedAwaySweeps_;
    } else if (replicaRegistry_ != nullptr && (pushCycle_ & (kMovedAwayScanEveryCycles - 1)) == 0) [[unlikely]] {
        scanMovedAwaySlice_();
        if (subscriptions_.empty()) return;
    }
    for (auto subIt = subscriptions_.begin(); subIt != subscriptions_.end(); ) {
        const int64_t subId = subIt->first;
        Subscription& sub   = subIt->second;
        if (routedSweep && movedAwayEligible_(sub) && movedAway_(sub) &&
            retireReadCommittedMovedAway_(subId, sub)) [[unlikely]] {
            subIt = subscriptions_.erase(subIt);
            continue;
        }
        ++subIt;
        // read-rework T3/T4 (I3b): when per-worker egress is active the OWNING worker
        // reads+encodes+pushes user partitions on its own dynamic-MDC egress — the
        // gateway must NOT also push them (that would double-deliver).  Internal
        // partitions (topicId < 0) are not worker-owned, so keep gateway-pushing them.
        if (perWorkerEgressActive_ && sub.topicId >= 0) continue;
        // Issue #603: nothing is pushed while the view is behind committed security events (they may revoke READ).
        if (securityBehind_) [[unlikely]] continue;
        // Find the owning session.
        Session* session = sessions_.find(sub.sessionId);
        if (sub.failed) [[unlikely]] {   // F19: parked at an undeliverable batch
            if (sub.errorOwed && session) trySendOwedPushError_(sub, *session);
            continue;
        }
        if (!session) {
            // Session gone — subscription will be reaped; skip this cycle.
            continue;
        }

        // Compute isolation bound (GR-6: HW-bounded delivery).
        // See block comment above for the full rationale.
        rune::storage::Log* log = topicRegistry_ ?
            topicRegistry_->log(sub.topicId, sub.partition) : nullptr;
        // Phase 2b E2: a TRANSIENT partition (no disk Log) is read by its owning
        // worker only (the PushRead below) — never by the inline path.
        // Final fix wave (M6): its classification + replica are cached on the
        // subscription, refreshed only when a registry generation moved (a
        // replica added / removed, a topic created / committed) — two integer
        // compares per cycle instead of four hash probes.
        if (!log) {
            if (!workerPool_ || replicaRegistry_ == nullptr || topicRegistry_ == nullptr) continue;
            const uint64_t regGen = replicaRegistry_->generation();
            const uint64_t catGen = topicRegistry_->catalogGeneration();
            if (sub.transientClassRegGen != regGen || sub.transientClassCatGen != catGen) [[unlikely]] {
                sub.transientClassRegGen = regGen;
                sub.transientClassCatGen = catGen;
                ++transientClassifications_;
                sub.transientReplica =
                    servedPartition_(sub.topicId, sub.partition).kind == ServedPartition::Kind::Transient
                        ? replicaRegistry_->lookup(sub.topicId, sub.partition)
                        : nullptr;
            }
            if (sub.transientReplica == nullptr) continue;   // registry removed or not set — skip
        }

        // GR-6: Look up the PartitionReplica for this (topicId, partition).
        // If found → bound = replica->highWatermark() (majority-committed).
        // If not found (non-replicated / registry not wired) → bound = LEO (pass-through).
        // O(1), no alloc, lock-free (single-writer duty-cycle thread).
        int64_t hw;
        if (!log) {
            hw = sub.transientReplica->gatewayHighWatermark();   // M6: the cached replica (generations checked above)
        } else if (replicaRegistry_) {
            const rune::replication::PartitionReplica* replica =
                replicaRegistry_->lookup(sub.topicId, sub.partition);
            // CP1-I4: only a gateway-owned log has its LEO read here; a worker-owned one without a replica has no
            // published HW yet (nothing to push).
            hw = replica ? replica->gatewayHighWatermark()
                         : (gatewayOwnsLog_(sub.topicId, log) ? log->logEndOffset() : 0);
        } else {
            // No replication wired — single-node pass-through: HW == LEO (gateway-owned logs only).
            hw = gatewayOwnsLog_(sub.topicId, log) ? log->logEndOffset() : 0;
        }

        // TX-4 Task 9: Apply isolation-level bound.
        //
        // read_uncommitted (isolation=0): bound = HW (deliver everything committed).
        // read_committed  (isolation=1): bound = LSO = min(firstOffset of open txns)
        //                                       or HW if no open transactions.
        //
        // Control batches (COMMIT/ABORT markers) are NEVER delivered to clients
        // in EITHER isolation mode (Kafka KIP-98 invariant 5; they carry
        // internal coordinator data, not application records).
        //
        // No heap alloc: getTxnStateRO is O(unordered_map lookup) = O(1) amortised.
        //
        // F15 S4 (I-B): with a worker pool the gateway makes NO read_committed
        // decision.  Its HW snapshot only bounds (loosens) the PushRead request;
        // the owning worker clamps the answer to its own LSO and strips aborted
        // and control batches (executePushReadWire_).  Only the no-pool inline
        // path below keeps a gateway-side LSO.
        int64_t bound = hw;
        if (!workerPool_ &&
            sub.isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted)) {
            if (const PartitionTxnState* ts = getTxnStateRO(sub.topicId, sub.partition)) {
                bound = ts->lsoTracker.lso(hw);
            }
            // If no txn state exists yet, no transactional batches written → bound = hw.
        }

        if (sub.nextOffset >= bound) continue; // nothing committed to push yet
        // Fix round 4/5 (N9, N24): no Records to an Expiring session (its
        // subscriptions end when its Expiring state is serviced) — checked only
        // when there is something to push (the idle pass reads nothing more).
        if (session->spill.state == SpillState::Expiring) [[unlikely]] continue;
        if (session->spill.state == SpillState::Closing) [[unlikely]] continue;   // issue #119: its subscriptions ended

        // E2 inc2a: When a WorkerPool is wired, route the log read through the
        // owning worker's SPSC ingress ring.  The chunk is returned via the
        // return ring and processed in drainWorkerReturns_() next cycle.
        //
        // Skip this subscription if it already has an in-flight PushRead (the
        // previous cycle's read has not yet returned).  This preserves the
        // strictly-ordered delivery guarantee: we never issue a second read
        // before the first one's response has been sent.
        if (workerPool_) {
            if (pendingWorkerReads_.count(subId) > 0) {
                // In-flight — skip until the return is processed.
                continue;
            }
            // F15 addendum R-3: the owning worker answered "nothing below its
            // bound" (an open txn pins the LSO, a pre-carrier cut, the HW) for
            // this cursor at HW snapshot parkedAtHw — re-issue only once the HW
            // snapshot moved past it (any LSO advance needs a marker append,
            // which moves the HW).  No empty-PushRead spin.
            if (sub.parkedAtHw >= 0 && hw <= sub.parkedAtHw) continue;
            // Hold-capacity fallback (the worker could not hold a not-Ready
            // read_committed read): back off in push cycles, never a spin.
            if (sub.retryAtCycle > pushCycle_) continue;

            // Look up the owning replica (for the log accessor in the worker).
            // M6: a transient partition's is the cached one (no second probe).
            const rune::replication::PartitionReplica* repForRead = log ? nullptr : sub.transientReplica;
            if (log && replicaRegistry_) {
                repForRead = replicaRegistry_->lookup(sub.topicId, sub.partition);
            }

            rune::node::WorkerOp op;
            op.type                = rune::node::WorkerOp::Type::PushRead;
            op.topicId             = sub.topicId;
            op.partition           = sub.partition;
            op.replica             = const_cast<rune::replication::PartitionReplica*>(repForRead);
            op.logPtr              = log;
            op.subscriptionId      = subId;
            op.sessionId           = sub.sessionId;
            op.nextOffset          = sub.nextOffset;
            op.hwBound             = bound;
            op.isolation           = sub.isolation;
            op.isGroup             = sub.isGroup;
            op.freshCursor         = sub.cursorFresh;   // M2
            op.groupCarrierReceived = sub.groupCarrierReceived;

            if (!workerPool_->tryEnqueueOp(sub.topicId, sub.partition, std::move(op))) {
                spdlog::warn("ClientGateway: worker ring full for push sub={}; retrying next cycle",
                             subId);
                // Ring full — skip this cycle; pendingWorkerReads_ not set so
                // we retry next cycle.
            } else {
                // Successfully enqueued — mark as in-flight.
                pendingWorkerReads_.insert(subId);
                sub.issuedNextOffset = sub.nextOffset;   // R-1: the cursor this read answers
                sub.issuedHw         = bound;
                sub.parkedAtHw       = -1;
            }
            continue; // Gateway will process the return in the next cycle.
        }

        // INLINE path (pre-E2 / no WorkerPool):
        // Zero-copy: map a bounded span of the log starting at nextOffset — no heap
        // allocation, no pread copy.  The span aliases the segment's read-only mmap
        // (shared page cache with the writer), valid until the next readView() on
        // this log.  We fully consume (encode + push) it within this loop iteration.
        rune::storage::LogReadView view;
        try {
            view = log->readView(sub.nextOffset, kMaxChunkBytes);
        } catch (const std::exception& ex) {
            spdlog::warn("ClientGateway: Log::readView threw for sub={}: {}", subId, ex.what());
            continue;
        } catch (...) {
            spdlog::warn("ClientGateway: Log::readView threw unknown exception for sub={}", subId);
            continue;
        }

        if (view.empty()) continue; // nothing returned (nextOffset not yet visible)

        // Iterate whole record batches in the chunk (span into the read mmap).
        // Batch layout: kBaseOffsetOffset=0 (LE64), kLengthOffset=8 (LE32 = bytes after this field),
        //               total batch size = 12 + LE32@8; kRecordCountOffset=57 (LE32).
        const uint8_t* const chunk = view.data;
        std::size_t chunkPos  = 0;
        const std::size_t chunkSize = view.len;
        int batchesPushed = 0;
        bool backPressured = false;

        while (chunkPos < chunkSize && batchesPushed < maxBatchesPerSubPerCycle_ && !backPressured) {
            // Need at least kRecordCountOffset + 4 bytes to read header fields.
            constexpr std::size_t kMinBatchHdr =
                static_cast<std::size_t>(kRecordCountOffset) + 4u;
            if (chunkPos + kMinBatchHdr > chunkSize) break; // partial header at end of chunk

            const uint8_t* batchPtr = chunk + chunkPos;

            // Read baseOffset (LE64@0).
            int64_t baseOffset = 0;
            {
                for (int i = 0; i < 8; ++i)
                    baseOffset |= (static_cast<int64_t>(batchPtr[i]) << (i * 8));
            }

            // TX-4 Task 9: Per-batch isolation bound check.
            //
            // Stop delivering if this batch's baseOffset has reached the bound.
            //   read_uncommitted (isolation=0): bound = HW.
            //   read_committed  (isolation=1): bound = LSO.
            //
            // We STOP (break) rather than skip-and-continue, because the log is
            // ordered by baseOffset: all subsequent batches in the chunk also
            // have baseOffset >= bound (log is sequential).  Breaking also avoids
            // advancing nextOffset past `bound`, which would violate exactly-once
            // delivery once the LSO advances (those batches should be re-delivered
            // next cycle after the LSO moves forward).
            if (baseOffset >= bound) break;

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

            // Read attributes (LE16@kAttributesOffset=21).
            // Bits: bit4=TRANSACTIONAL, bit5=CONTROL.
            // Used for:
            //   (a) Control-batch skip (both isolation modes).
            //   (b) Aborted-range filter (read_committed only).
            uint16_t batchAttrs = 0;
            {
                constexpr std::size_t kAttrOff =
                    static_cast<std::size_t>(kAttributesOffset);
                if (chunkPos + kAttrOff + 2u <= chunkSize) {
                    batchAttrs = static_cast<uint16_t>(
                        static_cast<uint16_t>(batchPtr[kAttrOff]) |
                        (static_cast<uint16_t>(batchPtr[kAttrOff + 1u]) << 8));
                }
            }
            constexpr uint16_t kControlBit      = 0x20u;  // bit 5
            constexpr uint16_t kTransactionalBit = 0x10u;  // bit 4

            // TX-4: Skip control batches (COMMIT/ABORT markers) unconditionally.
            // Control batches must NEVER be delivered to applications in either
            // isolation mode (Kafka KIP-98; they are coordinator metadata only).
            if (batchAttrs & kControlBit) {
                // Advance past this control batch without delivering it.
                // We still need to read recordCount to compute the nextOffset advance.
                // Control batches have recordCount=1 (one ControlRecord), so we can
                // safely advance by 1.  But if recordCount field is available use it
                // to be correct for any future multi-record control batch format.
                const uint8_t* prc = batchPtr + kRecordCountOffset;
                int32_t ctlRecordCount = static_cast<int32_t>(
                    static_cast<uint32_t>(prc[0]) |
                    (static_cast<uint32_t>(prc[1]) << 8) |
                    (static_cast<uint32_t>(prc[2]) << 16) |
                    (static_cast<uint32_t>(prc[3]) << 24));
                if (ctlRecordCount < 1) ctlRecordCount = 1; // safety fallback
                chunkPos += batchSize;
                sub.nextOffset = baseOffset + static_cast<int64_t>(ctlRecordCount);
                continue;
            }

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
                // past it).  (F19: an oversize batch is NOT skipped like this —
                // it is undeliverable, not corrupt: the subscription is failed
                // explicitly and parked at it.)
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

            // TX-4 Task 9: Aborted-txn filter for read_committed.
            //
            // For read_committed (isolation=1): if this is a transactional data
            // batch (TRANSACTIONAL bit set) and its producerId falls within a
            // recorded aborted range, skip it (advance past it) without delivering.
            //
            // For read_uncommitted (isolation=0): no abort filtering — deliver all
            // non-control batches that are below HW (already bounded by `bound`).
            //
            // No alloc: AbortedTxnIndex::isAborted is O(ranges) = O(tiny) with a
            // vector scan; in practice at most a few concurrent aborted txns.
            //
            // TX-6b pre-carrier guard: group subscriptions are created with the
            // conservative default isolation=1 (read_committed) before the client's
            // Subscribe carrier frame arrives carrying the TRUE isolation level.
            // If we advance nextOffset past an aborted batch under isolation=1 and
            // the client then sends a carrier with isolation=0 (read_uncommitted),
            // those aborted records are permanently lost (nextOffset is already past
            // them).  To avoid this, if the subscription is a group sub whose carrier
            // hasn't arrived yet (!groupCarrierReceived), we STOP pushing at the first
            // transactional batch that would be isolation-filtered: break out of this
            // subscription's push loop without advancing nextOffset.  On the next
            // duty cycle — either after the carrier arrived (groupCarrierReceived=true,
            // correct isolation applied) or while still waiting (same stall, no harm)
            // — the batch is re-evaluated under the correct semantics.
            //
            // This stall is bounded: the carrier arrives within one client duty cycle
            // after SyncGroup and is processed on the SAME broker duty-cycle thread
            // (no cross-thread race).  The pre-carrier stall never affects non-group
            // subscriptions (isGroup=false never sets groupCarrierReceived=false in any
            // ambiguous way) and does not affect already-confirmed group subs.
            if (sub.isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted)
                && (batchAttrs & kTransactionalBit)) {
                // Read producerId (LE64 at kProducerIdOffset=43).
                constexpr std::size_t kPidOff =
                    static_cast<std::size_t>(kProducerIdOffset);
                if (chunkPos + kPidOff + 8u <= chunkSize) {
                    int64_t batchPid = 0;
                    for (int bi = 0; bi < 8; ++bi)
                        batchPid |= (static_cast<int64_t>(batchPtr[kPidOff + bi]) << (bi * 8));

                    if (const PartitionTxnState* ts = getTxnStateRO(sub.topicId, sub.partition)) {
                        if (ts->abortedIndex.isAborted(batchPid, baseOffset)) {
                            // TX-6b: pre-carrier group sub: do NOT advance past this
                            // aborted batch.  Break so we re-evaluate after the carrier
                            // arrives with the true isolation level.
                            if (sub.isGroup && !sub.groupCarrierReceived) {
                                spdlog::debug("ClientGateway: push sub={} pre-carrier group "
                                              "sub stalling at aborted batch offset={} "
                                              "(waiting for isolation carrier)",
                                              subId, baseOffset);
                                break;
                            }
                            // Skip aborted batch — advance past it.
                            chunkPos += batchSize;
                            sub.nextOffset = baseOffset + static_cast<int64_t>(recordCount);
                            continue;
                        }
                    }
                }
            }

            // Records{subscriptionId, topicId, partition, baseOffset, batchBytes}:
            // the batch bytes are a zero-copy span into the chunk.
            //
            // F19: no size guard here — the Records var field has a u32 length and
            // pushRecords_ delivers any frame up to the egress maxMessageLength
            // (tryClaim fast path / vectored offer).  A larger frame is answered
            // with an explicit error and the subscription parks AT the batch —
            // never skipped silently.
            pc::Records rec;
            rec.sessionId      = sub.sessionId;
            rec.correlationId  = sub.subscribeCorrId;  // the client's Subscribe: exact instance identity
            rec.subscriptionId = sub.subscriptionId;
            rec.topicId        = sub.topicId;
            rec.partition      = sub.partition;
            rec.baseOffset     = baseOffset;
            rec.recordBatch    = std::span<const uint8_t>(batchPtr, batchSize);

            const rune::node::RecordsSendResult sent = pushRecords_(*session, rec);
            if (sent.kind == rune::node::RecordsSendKind::TooLarge) [[unlikely]] {
                failPushSubscriptionTooLarge_(sub, *session, rec.encodedSize());
                break;
            }
            const int64_t pos = sent.position;

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
// drainWorkerReturns_ (E2 inc2a) — collect results from per-worker SPSC rings
// and complete the corresponding in-flight gateway operations.
//
// Called at the top of doWork() (before ingress poll) when workerPool_ is set.
// This ensures that results from the PREVIOUS cycle's produce-appends and
// push-reads are delivered before processing new ingress — preserving the
// session-level ordering guarantee.
//
// ProduceResult handling:
//   1. If errorCode != 0: find session, send ProduceResponse{Internal, partition, 0}.
//   2. If success: send ProduceResponse{OK, partition, baseOffset} — every
//      worker result is FINAL (F15 I-A).  No mirror is fed (F15 S4/S5: the
//      owning worker holds the read_committed and producer state and syncs
//      its own logs).  A deferAck result is a shard-mode post-ack liveness
//      notification: the session is touched, nothing is sent.
//
// PushReadResult handling:
//   1. Remove subscriptionId from pendingWorkerReads_.
//   2. If chunk is empty → done (nothing to push this cycle).
//   3. Iterate whole record batches in the chunk (same loop as inline path).
//   4. Encode + tryClaim Records frames; advance sub.nextOffset on success only.
//      Back-pressure: stop for this subscription (same invariant as inline path).
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
bool ClientGateway::trySendMarker_(int64_t token)
{
    auto it = markersInFlight_.find(token);
    if (it == markersInFlight_.end()) return true;   // already resolved
    if (it->second.remoteNode >= 0) return sendRemoteMarker_(token, it->second);   // D9
    if (!workerPool_) return false;
    const MarkerInFlight& m = it->second;
    rune::node::WorkerOp op;
    op.type          = rune::node::WorkerOp::Type::AppendControlRecord;
    op.topicId       = m.topicId;
    op.partition     = m.partition;
    op.correlationId = token;
    op.producerId    = m.producerId;
    op.logPtr        = topicRegistry_ ? topicRegistry_->log(m.topicId, m.partition) : nullptr;
    op.recordBatchView = std::span<const uint8_t>(m.bytes);   // copied once into the ring slot
    // Re-review MEDIUM-3: a remote-origin marker carries the target leader epoch
    // it was routed under, encoded +1 in roleEpoch (0 = not checked: local markers).
    op.roleEpoch     = m.targetLeaderEpoch >= 0 ? m.targetLeaderEpoch + 1 : 0;
    // Replica identity is stamped by WorkerPool::tryEnqueueOp from the registry.
    return workerPool_->tryEnqueueOp(m.topicId, m.partition, std::move(op));
}

int ClientGateway::pumpMarkerQueue_()
{
    int sent = 0;
    while (!markerSendQueue_.empty()) {
        if (!trySendMarker_(markerSendQueue_.front())) break;   // still back-pressured
        markerSendQueue_.pop_front();
        ++sent;
    }
    return sent;
}

bool ClientGateway::markerQueuedFor_(int32_t topicId, int32_t partition) const noexcept
{
    for (const int64_t t : markerSendQueue_) {
        auto it = markersInFlight_.find(t);
        if (it != markersInFlight_.end() && it->second.remoteNode < 0 &&   // D9: a remote one is not ordered here
            it->second.topicId == topicId && it->second.partition == partition)
            return true;
    }
    return false;
}

void ClientGateway::onControlRecordAppended_(const rune::node::WorkerReturn& ret)
{
    auto it = markersInFlight_.find(ret.correlationId);
    if (it == markersInFlight_.end()) {
        spdlog::warn("ClientGateway: ControlRecordAppended for unknown marker token={} topic={} partition={}",
                     ret.correlationId, ret.topicId, ret.partition);
        return;
    }
    const MarkerInFlight m = std::move(it->second);
    markersInFlight_.erase(it);
    if (m.originNode >= 0) {
        // D9: appended on behalf of a remote coordinator — answer IT, never the
        // local coordinator (the owning worker already applied its RC state).
        answerRemoteOrigin_(m, ret.errorCode, ret.errorCode == 0 ? ret.baseOffset : -1);
        return;
    }
    if (ret.errorCode == rune::transaction::kMarkerCoordinatorFencedError) {
        // Re-review LOW-4: a deposed coordinator's marker — expected, not a failure.
        spdlog::warn("ClientGateway: control marker for topic={} partition={} producerId={} FENCED "
                     "(coordinator epoch {} deposed) — surfaced to the txn coordinator",
                     m.topicId, m.partition, m.producerId, m.coordinatorEpoch);
        if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, ret.errorCode,
                                                             m.coordinatorEpoch);
        return;
    }
    if (ret.errorCode != 0) {
        spdlog::error("ClientGateway: control marker for topic={} partition={} producerId={} FAILED "
                      "to append (error={}) — surfaced to the txn coordinator",
                      m.topicId, m.partition, m.producerId, ret.errorCode);
        if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, ret.errorCode,
                                                             m.coordinatorEpoch);
        return;
    }
    // F15 S4 (Option B): the owning worker applied the marker to its own RC
    // state in the same invocation as the append — the gateway holds none.
    // Round 5: only now may the coordinator count this partition's marker as
    // written (the transaction completes when the last one lands).
    if (txnService_) txnService_->onControlMarkerLanded(m.topicId, m.partition, m.producerId, m.coordinatorEpoch);
}

void ClientGateway::onControlRecordRefused_(const rune::node::WorkerReturn& ret)
{
    ++controlRecordsRefused_;
    const bool registered = replicaRegistry_ && replicaRegistry_->lookup(ret.topicId, ret.partition);
    auto it = markersInFlight_.find(ret.correlationId);
    if (it == markersInFlight_.end()) {
        // Not a marker this gateway issued.  Still on this node → an invariant
        // violation (halt); gone → surface NotLeader to the coordinator.
        if (registered) {
            const std::string what = "control marker refused (unknown token " + std::to_string(ret.correlationId) +
                                     ") while topic=" + std::to_string(ret.topicId) + " partition=" +
                                     std::to_string(ret.partition) + " is registered on this node";
            spdlog::critical("ClientGateway: INVARIANT VIOLATION — {}", what);
            if (invariantViolationHandler_) invariantViolationHandler_(what);
            return;
        }
        if (txnService_) txnService_->onControlMarkerRefused(ret.topicId, ret.partition, ret.producerId, ret.errorCode,
                                                             ITxnRequestHandler::kAnyCoordinatorEpoch);
        return;
    }
    MarkerInFlight& m = it->second;
    if (registered) {
        // Task 10 round 4 (P1): re-routing is SAFE now — no offset was ever
        // pre-computed or reported; the offset comes from the append that
        // finally executes.  WorkerPool re-stamps the CURRENT registry identity.
        // Bounded: after (a)-(c) of round 3 a registered partition's marker can
        // only be refused transiently, so repeated refusals = a broken invariant.
        if (++m.reroutes > kMaxMarkerReroutes) {
            const std::string what = "control marker for topic=" + std::to_string(m.topicId) + " partition=" +
                                     std::to_string(m.partition) + " refused " + std::to_string(m.reroutes) +
                                     " times while registered on this node";
            spdlog::critical("ClientGateway: INVARIANT VIOLATION — {}", what);
            if (invariantViolationHandler_) invariantViolationHandler_(what);
            return;
        }
        spdlog::warn("ClientGateway: control marker for topic={} partition={} refused by the worker "
                     "(stale routing) — re-routed (attempt {})", m.topicId, m.partition, m.reroutes);
        if (!markerSendQueue_.empty() || !trySendMarker_(ret.correlationId))
            markerSendQueue_.push_front(ret.correlationId);   // ahead of later markers
        return;
    }
    if (m.originNode >= 0) {
        // D9: a remote coordinator's marker for a partition no longer here.
        answerRemoteOrigin_(m, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader), -1);
        markersInFlight_.erase(it);
        return;
    }
    spdlog::error("ClientGateway: control marker for topic={} partition={} producerId={} REFUSED "
                  "(partition not on this node) — surfaced to the txn coordinator as NotLeader",
                  m.topicId, m.partition, m.producerId);
    if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, ret.errorCode,
                                                         m.coordinatorEpoch);
    markersInFlight_.erase(it);
}

// ---------------------------------------------------------------------------
// D9 — cross-node transaction markers (design 2026-10-04-d9).  Runtime thread,
// cold path (allocation is fine; the produce path is untouched).
// ---------------------------------------------------------------------------
int64_t ClientGateway::initialMarkerToken_()
{
    // Review MEDIUM-1: a per-boot incarnation in bits 32..62 (positive tokens);
    // the low 32 bits count.  Cold (once per gateway).
    std::random_device rd;
    const uint64_t inc = ((static_cast<uint64_t>(rd()) << 16) ^ static_cast<uint64_t>(rd())) & 0x7FFF'FFFFull;
    return static_cast<int64_t>(((inc == 0 ? 1ull : inc) << 32) | 1ull);
}

int32_t ClientGateway::remoteMarkerLeader_(int32_t topicId, int32_t partition, int32_t& leaderEpoch) const noexcept
{
    if (txnMarkerChannel_.sendMarker == nullptr || !routingImage_ || topicId < 0) return -1;
    const auto& e = routingImage_->lookup(rune::metadata::gpidOf(topicId, partition));
    if (e.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID || e.leaderNodeId == thisNodeId_)
        return -1;   // unassigned (local path answers NotLeader → retried) or led here
    leaderEpoch = e.leaderEpoch;
    return e.leaderNodeId;
}

bool ClientGateway::sendRemoteMarker_(int64_t token, MarkerInFlight& m)
{
    rune::replication::WriteTxnMarkerMsg w{};
    w.coordinatorNodeId = thisNodeId_;
    w.coordinatorEpoch  = m.coordinatorEpoch;
    w.token             = token;
    w.topicId           = m.topicId;
    w.partition         = m.partition;
    w.producerId        = m.producerId;
    w.commit            = m.commit ? 1 : 0;
    w.leaderEpoch       = m.remoteLeaderEpoch;   // re-review MEDIUM-3
    w.marker            = std::span<const uint8_t>(m.bytes);
    if (!txnMarkerChannel_.sendMarker(txnMarkerChannel_.ctx, m.remoteNode, w)) return false;
    m.sent = true;
    return true;
}

void ClientGateway::refuseMarker_(int64_t token, int16_t errorCode)
{
    auto it = markersInFlight_.find(token);
    if (it == markersInFlight_.end()) return;
    const MarkerInFlight m = std::move(it->second);
    markersInFlight_.erase(it);
    for (auto q = markerSendQueue_.begin(); q != markerSendQueue_.end(); ++q) {
        if (*q == token) { markerSendQueue_.erase(q); break; }
    }
    if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, errorCode,
                                                         m.coordinatorEpoch);
}

void ClientGateway::failRemoteMarkersOnLeaderChange_()
{
    std::vector<int64_t> moved;   // cold: only with markers in flight on an image change
    for (const auto& [token, m] : markersInFlight_) {
        if (m.remoteNode < 0) continue;
        int32_t epoch = -1;
        int32_t leader = -1;
        if (routingImage_) {
            const auto& e = routingImage_->lookup(rune::metadata::gpidOf(m.topicId, m.partition));
            leader = e.leaderNodeId;
            epoch  = e.leaderEpoch;
        }
        if (leader != m.remoteNode || epoch != m.remoteLeaderEpoch) moved.push_back(token);
    }
    for (const int64_t token : moved) {
        if (auto it = markersInFlight_.find(token); it != markersInFlight_.end())
            spdlog::info("ClientGateway: leadership of topic={} partition={} moved off node {} — in-flight "
                         "remote marker (producerId={}) completes NotLeader (re-routed by the coordinator)",
                         it->second.topicId, it->second.partition, it->second.remoteNode, it->second.producerId);
        refuseMarker_(token, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
    }
}

int ClientGateway::resendRemoteMarkers_()
{
    // Review MAJOR-1: a frame (or its answer) may have been dropped while the
    // plane saw the peer down — re-send every SENT remote marker to the
    // reconnected peer(s) with the SAME token.  Idempotent end to end: the
    // leader skips a copy still in flight there and re-answers otherwise; the
    // answer to any copy completes the marker once; copies stay FIFO per peer.
    const bool all = resendAllRemoteMarkers_;
    resendAllRemoteMarkers_ = false;
    std::vector<int32_t> peers;
    peers.swap(reconnectedPeers_);
    int resent = 0;
    for (auto& [token, m] : markersInFlight_) {
        if (m.remoteNode < 0 || !m.sent) continue;   // local, or still queued (will be sent anyway)
        if (!all && std::find(peers.begin(), peers.end(), m.remoteNode) == peers.end()) continue;
        if (!sendRemoteMarker_(token, m)) {
            m.sent = false;
            markerSendQueue_.push_back(token);       // ring full: sent by the queue pump (FIFO)
        }
        ++resent;
    }
    if (resent > 0)
        spdlog::info("ClientGateway: cluster-plane reconnect — re-sent {} in-flight remote marker(s)", resent);
    return resent;
}

void ClientGateway::onRemoteControlRecordResponse(const rune::replication::WriteTxnMarkerResponseMsg& r)
{
    auto it = markersInFlight_.find(r.token);
    if (it == markersInFlight_.end() || it->second.remoteNode < 0 || it->second.topicId != r.topicId ||
        it->second.partition != r.partition || it->second.producerId != r.producerId ||
        it->second.coordinatorEpoch != r.coordinatorEpoch) {
        // Already completed (an answer to another copy / a leader change), or
        // not this marker (review MEDIUM-1: matched on token AND coordinator
        // epoch; tokens carry a boot incarnation).  A duplicate landing is harmless.
        spdlog::debug("ClientGateway: WriteTxnMarkerResponse for unknown marker token={} topic={} partition={}",
                      r.token, r.topicId, r.partition);
        return;
    }
    const MarkerInFlight m = std::move(it->second);
    markersInFlight_.erase(it);
    if (r.errorCode != 0) {
        spdlog::warn("ClientGateway: remote marker topic={} partition={} producerId={} refused by node {} "
                     "(error={}) — surfaced to the txn coordinator", m.topicId, m.partition, m.producerId,
                     m.remoteNode, r.errorCode);
        if (txnService_) txnService_->onControlMarkerRefused(m.topicId, m.partition, m.producerId, r.errorCode,
                                                             m.coordinatorEpoch);
        return;
    }
    ++remoteMarkersLanded_;
    if (txnService_) txnService_->onControlMarkerLanded(m.topicId, m.partition, m.producerId, m.coordinatorEpoch);
}

void ClientGateway::appendRemoteControlRecord(const rune::replication::WriteTxnMarkerMsg& w)
{
    namespace pcl = rune::protocol::client;
    MarkerInFlight m;
    m.topicId          = w.topicId;
    m.partition        = w.partition;
    m.producerId       = w.producerId;
    m.commit           = w.commit != 0;
    m.coordinatorEpoch = w.coordinatorEpoch;
    m.originNode       = w.coordinatorNodeId;
    m.originToken      = w.token;
    if (w.marker.size() >= 43u + 8u) {   // the control batch's producerId (LE @43) is authoritative
        int64_t pid = 0;
        for (int i = 0; i < 8; ++i) pid |= static_cast<int64_t>(w.marker[43u + i]) << (i * 8);
        m.producerId = pid;
    }
    // No such partition here, or nothing to append → NotLeader at once
    // (retriable: the coordinator re-resolves).  Never UnknownTopic /
    // UnknownPartition: those make the coordinator DROP the marker, and a
    // lagging topic registry here is no proof the partition is gone.
    // Review MEDIUM-5: this node's routing image is NOT consulted — it can lag
    // the replica role; the worker's role gate is the authority (a follower
    // replica answers NotLeader through it).
    // C1 + E2: a committed transient partition takes markers too (its owning
    // worker appends the control batch to the ring); uncommitted / unknown →
    // NotLeader (the coordinator re-resolves — never Unknown*, see above).
    const ServedPartition served = servedPartition_(w.topicId, w.partition);
    rune::storage::Log* const log = served.log;
    const bool servable = served.kind == ServedPartition::Kind::Disk ||
                          served.kind == ServedPartition::Kind::Transient;
    if (!servable || w.topicId < 0 || w.marker.empty()) {
        answerRemoteOrigin_(m, static_cast<int16_t>(pcl::ErrorCode::NotLeader), -1);
        return;
    }
    // Re-review NEW MAJOR-1: idempotence for re-sent copies.  Per (topic,
    // partition, producer) this node remembers the last remote marker it took
    // (origin, coordinator epoch, token) and its outcome.  From the same
    // coordinator load (same origin, epoch and token incarnation):
    //   same token, still pending  → its answer is owed: skip the copy;
    //   same token, answered       → RE-ANSWER the original outcome, never append
    //                                 again (a copy trailing the answer would close
    //                                 the producer's NEXT transaction);
    //   older token                → a stale copy of a marker this coordinator
    //                                 already replaced: never appended (answered
    //                                 NotLeader; the coordinator forgot that token).
    // Not persisted: after a leader change the marker was applied under the old
    // leader and a stale copy is refused by its target leader epoch (MEDIUM-3).
    const RemoteMarkerKey key{w.topicId, w.partition, m.producerId};
    if (auto it = lastRemoteMarker_.find(key); it != lastRemoteMarker_.end()) {
        LastRemoteMarker& e = it->second;
        const bool sameLoad = e.origin == w.coordinatorNodeId && e.coordinatorEpoch == w.coordinatorEpoch &&
                              (e.token >> 32) == (w.token >> 32);
        if (sameLoad && w.token == e.token) {
            if (e.answered) answerRemoteOrigin_(m, e.errorCode, e.offset);
            return;
        }
        if (w.coordinatorEpoch < e.coordinatorEpoch) {
            // Re-review 2 MEDIUM-1: a NEWER coordinator load's marker was already
            // taken here (even if refused transiently and never applied, so the
            // PSM fence did not rise) — this copy comes from a deposed load.
            // Coordinator epochs per producer are monotone: fenced, never appended.
            spdlog::warn("ClientGateway: remote marker from node {} at coordinator epoch {} for topic={} "
                         "partition={} producerId={} FENCED (epoch {} already taken here)", w.coordinatorNodeId,
                         w.coordinatorEpoch, w.topicId, w.partition, m.producerId, e.coordinatorEpoch);
            answerRemoteOrigin_(m, rune::transaction::kMarkerCoordinatorFencedError, -1);
            return;
        }
        if (sameLoad && w.token < e.token) {
            spdlog::info("ClientGateway: stale copy of remote marker token={} from node {} (topic={} partition={} "
                         "producerId={}; newer token {} applied) — not appended", w.token, w.coordinatorNodeId,
                         w.topicId, w.partition, m.producerId, e.token);
            answerRemoteOrigin_(m, static_cast<int16_t>(pcl::ErrorCode::NotLeader), -1);
            return;
        }
    }
    lastRemoteMarker_[key] = LastRemoteMarker{w.coordinatorNodeId, w.coordinatorEpoch, w.token, false, 0, -1};
    m.targetLeaderEpoch = w.leaderEpoch;   // MEDIUM-3: checked by the worker against the replica's epoch
    if (routesToWorker_(w.topicId)) {
        // The SAME owning-worker op-ring path as a local marker: lands after
        // every produce routed before it (L4); a full ring queues it (FIFO).
        const int64_t token = nextMarkerToken_++;
        m.bytes.assign(w.marker.begin(), w.marker.end());
        markersInFlight_.emplace(token, std::move(m));
        if (!markerSendQueue_.empty() || !trySendMarker_(token)) markerSendQueue_.push_back(token);
        return;
    }
    // No worker pool (unit rigs / single-thread): the inline append.
    // E2: a transient partition has no inline append (worker-owned ring) — NotLeader, the coordinator retries.
    const int64_t base = log != nullptr ? appendControlRecordInline_(w.topicId, w.partition, w.marker, log) : -1;
    if (base >= 0) notifyControlMarker(w.topicId, w.partition, w.producerId, base, w.commit != 0);
    answerRemoteOrigin_(m, base >= 0 ? int16_t{0} : static_cast<int16_t>(pcl::ErrorCode::NotLeader), base);
}

void ClientGateway::onProducerExpired(int64_t producerId)
{
    std::erase_if(lastRemoteMarker_, [producerId](const auto& kv) { return kv.first.producerId == producerId; });
}

void ClientGateway::pruneRemoteMarkerIndex_()
{
    // Re-review 2 MEDIUM-2: a partition demoted (the bridge routed BecomeFollower,
    // queued on the worker op ring AHEAD of any later copy, so that copy meets the
    // follower role gate) or removed on this node needs no idempotence entries —
    // a copy arriving after a re-promotion carries an older target leader epoch.
    std::erase_if(lastRemoteMarker_, [this](const auto& kv) {
        const auto h = replicaRegistry_->lookupHandle(kv.first.topicId, kv.first.partition);
        return !h || h.intendedRole == rune::node::ReplicaRegistry::IntendedRole::Follower;
    });
}

void ClientGateway::answerRemoteOrigin_(const MarkerInFlight& m, int16_t errorCode, int64_t offset)
{
    // NEW MAJOR-1: remember the outcome for a later copy of the same token.
    if (auto it = lastRemoteMarker_.find(RemoteMarkerKey{m.topicId, m.partition, m.producerId});
        it != lastRemoteMarker_.end() && it->second.origin == m.originNode && it->second.token == m.originToken &&
        it->second.coordinatorEpoch == m.coordinatorEpoch) {
        it->second.answered  = true;
        it->second.errorCode = errorCode;
        it->second.offset    = offset;
    }
    rune::replication::WriteTxnMarkerResponseMsg r{};
    r.token            = m.originToken;
    r.topicId          = m.topicId;
    r.partition        = m.partition;
    r.producerId       = m.producerId;
    r.coordinatorEpoch = m.coordinatorEpoch;
    r.errorCode        = errorCode;
    r.offset           = offset;
    sendMarkerResponse_(m.originNode, r);
}

void ClientGateway::sendMarkerResponse_(int32_t toNode, const rune::replication::WriteTxnMarkerResponseMsg& r)
{
    if (txnMarkerChannel_.sendResponse == nullptr) {
        spdlog::warn("ClientGateway: no cluster-plane channel — WriteTxnMarkerResponse to node {} dropped "
                     "(the coordinator re-sends)", toNode);
        return;
    }
    // FIFO behind earlier back-pressured answers; never dropped.
    if (!pendingMarkerResponses_.empty() || !txnMarkerChannel_.sendResponse(txnMarkerChannel_.ctx, toNode, r))
        pendingMarkerResponses_.emplace_back(toNode, r);
}

int ClientGateway::pumpTxnMarkers_()
{
    int work = 0;
    // Re-review LOW-3: the leader-change check FIRST — a queued remote marker
    // whose leader just moved is failed, never sent once to the old leader.
    if (routingChangedForMarkers_) [[unlikely]] {
        routingChangedForMarkers_ = false;
        if (!markersInFlight_.empty()) failRemoteMarkersOnLeaderChange_();
    }
    if (!markerSendQueue_.empty()) work += pumpMarkerQueue_();   // worker-bound and D9 remote markers
    if (!pendingMarkerResponses_.empty()) [[unlikely]] work += pumpMarkerResponses_();
    if (replicaRegistry_ != nullptr && !lastRemoteMarker_.empty() &&
        replicaRegistry_->roleChanges() != seenRegistryRoleChanges_) [[unlikely]] {
        seenRegistryRoleChanges_ = replicaRegistry_->roleChanges();
        pruneRemoteMarkerIndex_();
    }
    if (resendAllRemoteMarkers_ || !reconnectedPeers_.empty()) [[unlikely]] work += resendRemoteMarkers_();
    if (!markerSendQueue_.empty()) work += pumpMarkerQueue_();   // re-sends the ring refused
    return work;
}

// ---------------------------------------------------------------------------
// F15-FU1 (#112): transaction-registration verification.  Runtime thread, cold
// (once per transaction and partition); allocation is fine here.
// ---------------------------------------------------------------------------
int32_t ClientGateway::verifyCoordinatorNode_() const noexcept
{
    if (txnService_ == nullptr || txnMarkerChannel_.sendVerify == nullptr) return -1;
    const int32_t n = txnService_->coordinatorNode();
    return (n < 0 || n == thisNodeId_) ? -1 : n;
}

void ClientGateway::onTxnVerifyRequest_(const rune::node::WorkerReturn& ret)
{
    namespace pcl = rune::protocol::client;
    VerifyAnswer a;
    a.topicId       = ret.topicId;
    a.partition     = ret.partition;
    a.producerId    = ret.producerId;
    a.producerEpoch = ret.producerEpoch;
    a.reqId         = ret.correlationId;
    if (txnService_ != nullptr && txnService_->coordinatesLocally()) {
        a.code = txnService_->verifyTxnPartition(a.producerId, a.producerEpoch, a.topicId, a.partition, nowNs_);
        answerVerify_(a);
        return;
    }
    const int32_t coordinator = verifyCoordinatorNode_();
    if (coordinator < 0) {
        a.code = static_cast<int16_t>(pcl::ErrorCode::NotCoordinator);   // nobody to ask now: the client retries
        answerVerify_(a);
        return;
    }
    if (!txnService_->clusterVerifiesTxnPartitions()) {
        // A member predates the verification: its coordinator would drop the ask.
        // Admitted exactly as before #112 until every member is upgraded.
        a.code = 0;
        answerVerify_(a);
        return;
    }
    const int64_t token = nextVerifyToken_++;
    // The sweep's reference node: set only when nothing is in flight, so asks still
    // out to a previous coordinator are swept by the next pump (coordinator changed).
    if (verifiesInFlight_.empty()) lastVerifyCoordinator_ = coordinator;
    VerifyInFlight& v = verifiesInFlight_[token];
    v.req         = a;
    v.coordinator = coordinator;
    if (!verifySendQueue_.empty() || !sendRemoteVerify_(token, v)) verifySendQueue_.push_back(token);
}

bool ClientGateway::sendRemoteVerify_(int64_t token, VerifyInFlight& v)
{
    rune::replication::VerifyTxnPartitionMsg m{};
    m.requesterNodeId = thisNodeId_;
    m.topicId         = v.req.topicId;
    m.partition       = v.req.partition;
    m.token           = token;
    m.producerId      = v.req.producerId;
    m.producerEpoch   = v.req.producerEpoch;
    if (!txnMarkerChannel_.sendVerify(txnMarkerChannel_.ctx, v.coordinator, m)) return false;
    v.sent = true;
    ++txnVerifiesForwarded_;
    return true;
}

void ClientGateway::failRemoteVerify_(int64_t token, int16_t code)
{
    auto it = verifiesInFlight_.find(token);
    if (it == verifiesInFlight_.end()) return;
    VerifyAnswer a = it->second.req;
    verifiesInFlight_.erase(it);
    a.code = code;
    answerVerify_(a);
}

void ClientGateway::onRemoteVerifyTxnPartitionResponse(const rune::replication::VerifyTxnPartitionResponseMsg& r)
{
    auto it = verifiesInFlight_.find(r.token);
    if (it == verifiesInFlight_.end() || it->second.req.topicId != r.topicId ||
        it->second.req.partition != r.partition || it->second.req.producerId != r.producerId ||
        it->second.req.producerEpoch != r.producerEpoch) {
        // Already answered (failed by a reconnect / coordinator change), or not ours.
        spdlog::debug("ClientGateway: VerifyTxnPartitionResponse for unknown token={} topic={} partition={}",
                      r.token, r.topicId, r.partition);
        return;
    }
    VerifyAnswer a = it->second.req;
    verifiesInFlight_.erase(it);
    a.code = r.errorCode;
    answerVerify_(a);
}

void ClientGateway::onRemoteVerifyTxnPartition(const rune::replication::VerifyTxnPartitionMsg& m)
{
    namespace pcl = rune::protocol::client;
    rune::replication::VerifyTxnPartitionResponseMsg r{};
    static_cast<rune::replication::VerifyTxnPartitionMsg&>(r) = m;
    const bool local = txnService_ != nullptr && txnService_->coordinatesLocally();
    r.errorCode = local ? txnService_->verifyTxnPartition(m.producerId, m.producerEpoch, m.topicId, m.partition, nowNs_)
                        : static_cast<int16_t>(pcl::ErrorCode::NotCoordinator);   // the asker retries
    if (local) ++txnVerifiesServedForPeers_;
    if (txnMarkerChannel_.sendVerifyResponse == nullptr) return;   // unreachable: the ask came over the plane
    // FIFO behind earlier back-pressured answers; never dropped.
    if (!pendingVerifyResponses_.empty() ||
        !txnMarkerChannel_.sendVerifyResponse(txnMarkerChannel_.ctx, m.requesterNodeId, r))
        pendingVerifyResponses_.emplace_back(m.requesterNodeId, r);
}

bool ClientGateway::tryEnqueueVerifyAnswer_(const VerifyAnswer& a)
{
    rune::node::WorkerOp op;
    op.type                        = rune::node::WorkerOp::Type::TxnVerifyResult;
    op.topicId                     = a.topicId;
    op.partition                   = a.partition;
    op.producerId                  = a.producerId;
    op.producerEpoch               = a.producerEpoch;
    op.correlationId               = a.reqId;
    op.replicatedPayload.errorCode = a.code;
    // TooLarge cannot happen (no tail); Full = back-pressure, retried in order.
    return workerPool_->enqueueOp(a.topicId, a.partition, std::move(op)) != rune::node::EnqueueResult::Full;
}

void ClientGateway::answerVerify_(const VerifyAnswer& a)
{
    if (workerPool_ == nullptr) return;   // no worker asked (the inline path verifies synchronously)
    if (!verifyAnswerQueue_.empty() || !tryEnqueueVerifyAnswer_(a)) verifyAnswerQueue_.push_back(a);
}

int16_t ClientGateway::inlineTxnAdmission_(int32_t topicId, int32_t partition, const uint8_t* batch, std::size_t len)
{
    namespace pcl = rune::protocol::client;
    if (batch == nullptr || len < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) return 0;
    const rune::protocol::RecordBatchReader r(batch, len);
    if ((static_cast<uint16_t>(r.attributes()) & static_cast<uint16_t>(rune::protocol::batch::kAttrTransactional)) == 0)
        return 0;
    const int64_t pid = r.producerId();
    if (pid < 0 || txnService_ == nullptr) return 0;   // no coordinator wired (a rig without transactions)
    if (getTxnState(topicId, partition).lsoTracker.firstOffsetOf(pid) >= 0) return 0;   // open here: verified
    if (!txnService_->coordinatesLocally()) {
        // The inline path (a gateway with no worker pool: rigs only — BrokerNode always
        // builds one) cannot ask a remote coordinator synchronously.  Before every member
        // supports the verification it admits as before #112; otherwise retriable.
        if (!txnService_->clusterVerifiesTxnPartitions()) return 0;
        return static_cast<int16_t>(pcl::ErrorCode::ConcurrentTransactions);
    }
    const int16_t c = txnService_->verifyTxnPartition(pid, r.producerEpoch(), topicId, partition, nowNs_);
    if (c == static_cast<int16_t>(pcl::ErrorCode::NotCoordinator)) return static_cast<int16_t>(pcl::ErrorCode::ConcurrentTransactions);
    return c;
}

int ClientGateway::pumpTxnVerify_()
{
    namespace pcl = rune::protocol::client;
    int work = 0;
    // A forwarded ask (or its answer) may be lost: on a plane reconnect, or when the
    // coordinator moved.  Answered NotCoordinator — the client's retry asks again.
    // Swept only on an event (a reconnect, or the coordinator node changed) — never per pass.
    const int32_t coordinator = verifiesInFlight_.empty() ? -1 : verifyCoordinatorNode_();
    if (!verifiesInFlight_.empty() &&
        (failAllRemoteVerifies_ || !verifyReconnectedPeers_.empty() || coordinator != lastVerifyCoordinator_))
        [[unlikely]] {
        const bool all = failAllRemoteVerifies_;
        std::vector<int64_t> lost;   // cold
        for (const auto& [token, v] : verifiesInFlight_) {
            const bool reconnected = all || std::find(verifyReconnectedPeers_.begin(), verifyReconnectedPeers_.end(),
                                                      v.coordinator) != verifyReconnectedPeers_.end();
            if (v.coordinator != coordinator || (reconnected && v.sent)) lost.push_back(token);
        }
        for (const int64_t token : lost) failRemoteVerify_(token, static_cast<int16_t>(pcl::ErrorCode::NotCoordinator));
        work += static_cast<int>(lost.size());
    }
    if (!verifiesInFlight_.empty()) lastVerifyCoordinator_ = coordinator;
    failAllRemoteVerifies_ = false;
    verifyReconnectedPeers_.clear();
    while (!verifySendQueue_.empty()) {
        const int64_t token = verifySendQueue_.front();
        auto it = verifiesInFlight_.find(token);
        if (it != verifiesInFlight_.end() && !it->second.sent && !sendRemoteVerify_(token, it->second)) break;
        verifySendQueue_.pop_front();
        ++work;
    }
    while (!pendingVerifyResponses_.empty()) {
        const auto& [to, r] = pendingVerifyResponses_.front();
        if (!txnMarkerChannel_.sendVerifyResponse(txnMarkerChannel_.ctx, to, r)) break;
        pendingVerifyResponses_.pop_front();
        ++work;
    }
    while (!verifyAnswerQueue_.empty()) {
        if (!tryEnqueueVerifyAnswer_(verifyAnswerQueue_.front())) break;
        verifyAnswerQueue_.pop_front();
        ++work;
    }
    return work;
}

int ClientGateway::pumpMarkerResponses_()
{
    int sent = 0;
    while (!pendingMarkerResponses_.empty()) {
        const auto& [to, r] = pendingMarkerResponses_.front();
        if (!txnMarkerChannel_.sendResponse(txnMarkerChannel_.ctx, to, r)) break;
        pendingMarkerResponses_.pop_front();
        ++sent;
    }
    return sent;
}

void ClientGateway::raiseStorageFailure_(const std::string& logDir, int err)
{
    spdlog::critical("ClientGateway: STORAGE FAILURE on {} (errno={}) — fsync failed; "
                     "halting the broker (fail-stop)", logDir, err);
    if (storageFailureRaised_) return;
    storageFailureRaised_ = true;
    if (storageFailureHandler_) storageFailureHandler_(logDir, err);
}

// F15 S4 addendum R-3 + review I-1 / I-2: what a PushReadResult tells the
// scheduler.  Park the subscription (re-read only once the gateway HW snapshot
// moves past the one the read was issued at) ONLY on an explicit answer: a
// bound >= 0, ready, and the cursor at / past it.  An unanswered result (a
// refusal, no log: bound -1) parks nothing — the next push cycle re-issues.  A
// notReady result (the worker's hold capacity was exhausted) backs off
// 1, 2, 4 … 1024 push cycles, reset by the next ready answer.
void ClientGateway::noteWorkerReadAnswer_(Subscription& sub, int64_t effectiveBound, bool notReady) noexcept
{
    if (notReady) {
        sub.notReadyBackoff = sub.notReadyBackoff == 0 ? 1 : std::min<int64_t>(sub.notReadyBackoff * 2, 1024);
        sub.retryAtCycle    = pushCycle_ + sub.notReadyBackoff;
        return;
    }
    sub.notReadyBackoff = 0;
    sub.retryAtCycle    = 0;
    if (effectiveBound >= 0 && sub.nextOffset >= effectiveBound) sub.parkedAtHw = sub.issuedHw;
}

void ClientGateway::drainWorkerReturns_()
{
    using namespace rune::protocol::batch;

    // F5: zero-copy — a PushReadResult chunk is a view of the worker's return
    // ring slot (valid inside this callback; handleWorkerReturn_ finishes with
    // it before returning — never stored).
    workerPool_->drainAllReturnsView([this](rune::node::WorkerReturn ret) {
        handleWorkerReturn_(std::move(ret));
    });
}

void ClientGateway::handleWorkerReturnForTest(const rune::node::WorkerReturn& ret)
{
    handleWorkerReturn_(ret);
}

void ClientGateway::handleWorkerReturn_(rune::node::WorkerReturn ret)
{
    using namespace rune::protocol::batch;

        // ===================================================================
        // StorageFailure (fsyncgate fail-stop) — halt the broker.
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::StorageFailure) [[unlikely]] {
            raiseStorageFailure_(std::string(reinterpret_cast<const char*>(ret.chunkBytes().data()), ret.chunkBytes().size()),
                                ret.errorCode);
            return;
        }

        // Task 13 fix round 1 (C2): a worker's ack publication for this session
        // can never deliver — close the session everywhere (table, workers,
        // subscriptions) so the client reconnects; its later notifications find
        // no session (no liveness refresh).  State-driven, never a timer.
        if (ret.type == rune::node::WorkerReturn::Type::SessionSignal) [[unlikely]] {
            onSessionSignal_(ret);
            return;
        }

        // Task 10 fix 3: a worker applied SetOwnership(gen) — lets the runtime
        // free replicas removed at or below the minimum acked generation.
        if (ret.type == rune::node::WorkerReturn::Type::OwnershipAck) {
            workerPool_->onOwnershipAck(ret.partition, static_cast<std::uint64_t>(ret.lastOffset));
            return;
        }

        // Task 10 fix 3 / round 4: a control marker refused by the worker
        // ownership guard.  Re-routed with the CURRENT routing if the partition
        // is still on this node (safe: its offset is never pre-computed);
        // otherwise NotLeader to the txn coordinator — never a silent drop.
        if (ret.type == rune::node::WorkerReturn::Type::ControlRecordRefused) [[unlikely]] {
            onControlRecordRefused_(ret);
            return;
        }
        // Task 10 round 4 (P1): the marker's AUTHORITATIVE offset.
        if (ret.type == rune::node::WorkerReturn::Type::ControlRecordAppended) {
            onControlRecordAppended_(ret);
            return;
        }
        // F15-FU1 (#112): a partition's owning worker asks the transaction coordinator.
        if (ret.type == rune::node::WorkerReturn::Type::TxnVerifyRequest) [[unlikely]] {
            onTxnVerifyRequest_(ret);
            return;
        }

        // ===================================================================
        // Task 20: ViewRowResult — a worker's answer to a GetViewRow point read.
        // Encode a ViewRowResponse (status + row bytes) and send it to the
        // session on the gateway egress.  The row bytes travel in the return
        // chunk (empty unless status == OK); recordCount carries rowLen.
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::ViewRowResult) {
            Session* session = sessions_.find(ret.sessionId);
            if (!session) return;   // session gone — drop (point read is idempotent)
            // C1: the worker does not lead the view's source partition (the image
            // lagged, or this is an ex-leader): redirect to the image's leader —
            // never encode the internal sentinel as a ViewRowResponse status.
            if (ret.errorCode == rune::node::kViewRowNotLeaderInternal) [[unlikely]] {
                std::string_view ep;
                (void)viewSourceLedElsewhere_(ret.readTopicId, ret.readPartition, ep);   // this node: bare NotLeader
                sendRouteRefusal_(*session, ret.sessionId, ret.correlationId, ep);
                return;
            }
            const std::span<const uint8_t> rowBytes = ret.chunkBytes();
            pc::ViewRowResponse resp;
            resp.sessionId     = ret.sessionId;
            resp.correlationId = ret.correlationId;
            resp.status        = static_cast<uint16_t>(ret.errorCode);
            resp.rowLen        = static_cast<uint32_t>(ret.recordCount);
            resp.rowBytes      = rowBytes.data();
            resp.rowBytesLen   = static_cast<uint32_t>(rowBytes.size());
            // Namespaces Phase 2 Task 12 (D26): a view paused over its namespace's memory limit.  Client >= 14: status
            // ViewPaused (54) with the paused fold offset (the worker's lastOffset) as 8 LE bytes; older: InvalidRequest.
            uint8_t pausedOffsetLe[8];
            // Task 14 (D28): Throttled (53) is the same shape - the retry-after ns rides lastOffset.
            if (ret.errorCode == static_cast<int16_t>(pc::ViewRowResponse::kViewPaused) ||
                ret.errorCode == static_cast<int16_t>(pc::ViewRowResponse::kThrottled)) [[unlikely]] {
                if (session->clientVersion >= pc::kClientVersionNamespaces) {
                    for (int b = 0; b < 8; ++b)
                        pausedOffsetLe[b] = static_cast<uint8_t>(static_cast<uint64_t>(ret.lastOffset) >> (8 * b));
                    resp.rowBytes    = pausedOffsetLe;
                    resp.rowBytesLen = 8;
                } else {
                    resp.status = pc::ViewRowResponse::kInvalidRequest;
                }
            }
            const std::size_t need = resp.encodedSize();
            if (need > kScratchSize) [[unlikely]] {
                // Fix round 2 (I4): a row larger than the egress frame is answered
                // ViewRowResponse{RowTooLarge (7)} with no row bytes — the read
                // completes with that status (it used to be dropped).
                spdlog::error("ClientGateway: ViewRowResponse {} B exceeds scratch ({} B) — answered "
                              "RowTooLarge (viewTopicId={})", need, kScratchSize, ret.readTopicId);
                resp.status      = pc::ViewRowResponse::kRowTooLarge;
                resp.rowLen      = 0;
                resp.rowBytes    = nullptr;
                resp.rowBytesLen = 0;
            }
            const std::size_t encoded = resp.encode(scratch_.data());
            sendReliable_(*session, scratch_.data(), encoded);
            return;
        }

        // ===================================================================
        // ProduceResult
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::ProduceResult) {
            // Mark the instant the gateway first sees the worker's produce result.
            // The gap between WorkerAppend (worker thread) and this mark is the
            // worker→gateway return-ring + duty-cycle latency (same correlationId).
            FLIGHT_RECORDER_MARK_PRODUCE(flight_recorder::Site::WorkerProcessOp, flight_recorder::foldCorr(ret.correlationId));
            // Task 13 (spec §3.4): with worker-side acks (shard mode) a
            // deferAck ProduceResult is the post-ack notification of an ack the
            // worker already sent — and the only produce traffic the runtime
            // sees for the session (F23 liveness, ack time >= request time:
            // never early).  One lookup; nothing is sent.  F15 S5: it carries no
            // facts — the owning worker's psm_ is the sole producer-state
            // authority (the facts-only return and the gateway mirror are gone).
            assert(!(ret.deferAck && ret.errorCode != 0) &&
                   "worker invariant: an error ProduceResult never carries deferAck");
            if (ret.deferAck) {
                sessions_.touch(ret.sessionId, nowNs_);
                return;
            }
            Session* session = sessions_.find(ret.sessionId);
            if (!session) {
                // Session gone — drop result silently.
                return;
            }
            // F23: per-worker ingress (forced on in shard mode) delivers Produce
            // straight to the worker, bypassing onFragment, and every such
            // produce (acks=0 included) returns here — refresh liveness on the
            // lookup we already do (ack time >= request time: never early).
            session->lastSeenNs = nowNs_;

            // Error path: worker failed to append (e.g. Log threw).
            if (ret.errorCode != 0) {
                // D6: an UnknownProducerId result carries the log start in
                // baseOffset (the ProduceResult carrier does not grow); the
                // answer is version-gated for this session.
                const auto ec = static_cast<pc::ErrorCode>(ret.errorCode);
                pc::ProduceResponse resp;
                resp.sessionId      = ret.sessionId;
                resp.correlationId  = ret.correlationId;
                resp.errorCode      = ret.errorCode;   // gated by encodeFor (28 -> OOS for old sessions)
                resp.partition      = ret.partition;
                resp.baseOffset     = 0;
                resp.logStartOffset = ec == pc::ErrorCode::UnknownProducerId ? ret.baseOffset : -1;
                // Task 14 (D28): Throttled carries the retry-after in ns (the worker's lastOffset) in logStartOffset; a
                // client below v14 is answered InvalidRequest by encodeFor and gets no retry-after.
                if (ec == pc::ErrorCode::Throttled && session->clientVersion >= pc::kClientVersionNamespaces)
                    resp.logStartOffset = ret.lastOffset;
                static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                              "scratch too small for ProduceResponse");
                // D6 review M4: the pre-D6 38 B frame below clientVersion 2.
                const std::size_t encoded = resp.encodeFor(scratch_.data(), session->clientVersion);
                // G0.1: reliable delivery (retry on back-pressure).
                sendReliable_(*session, scratch_.data(), encoded);
                return;
            }

            // F15 I-A: every ProduceResult from a worker is FINAL.  The owning
            // worker answers acks=all itself — parked on W until the HW passes
            // with |ISR| >= minIsr and the §3.2 role / ownership / log-generation
            // re-validation holds — so the gateway never parks a worker-owned
            // ack (its pendingAcks_ serves only the inline, control-owned path).
            {
                FLIGHT_RECORDER_SPAN_PRODUCE(flight_recorder::Site::ProduceAckSend, flight_recorder::foldCorr(ret.correlationId));
                pc::ProduceResponse okResp;
                okResp.sessionId     = ret.sessionId;
                okResp.correlationId = ret.correlationId;
                okResp.errorCode     = static_cast<int16_t>(pc::ErrorCode::OK);
                okResp.partition     = ret.partition;
                okResp.baseOffset    = ret.baseOffset;
                const std::size_t encoded = okResp.encodeFor(scratch_.data(), session->clientVersion);
                // G0.1: reliable delivery (retry on back-pressure).
                sendReliable_(*session, scratch_.data(), encoded);
            }
            return;
        }

        // ===================================================================
        // PushReadResult
        // ===================================================================
        if (ret.type == rune::node::WorkerReturn::Type::PushReadResult) {
            // Always clear the in-flight marker regardless of outcome.
            pendingWorkerReads_.erase(ret.subscriptionId);

            // F15 S3 re-review 1 I-4: the next stored batch is larger than the
            // worker's return ring can carry — fail this subscription once,
            // non-retriably, and park it (never re-issue the read: no spin).
            // Merged with F19 (integration wave 2): the SAME failure state and the
            // same owed error (encodePushTooLargeError: MessageTooLarge,
            // correlationId 0, subscription named — re-review 2 m-f), re-offered
            // each push cycle until sent (errorOwed) — never best-effort.
            if (ret.errorCode != 0) [[unlikely]] {
                auto it = subscriptions_.find(ret.subscriptionId);
                if (it == subscriptions_.end() || it->second.failed) return;
                Subscription& sub = it->second;
                sub.failed         = true;
                sub.errorOwed      = true;
                sub.failedCause    = rune::node::PushTooLargeCause::ReturnRing;
                sub.failedFrameLen = 0;
                spdlog::error("ClientGateway: subscription {} (topicId={} partition={} offset={}) failed: "
                              "worker answered error {} (a stored batch exceeds the worker return ring) — "
                              "MessageTooLarge, subscription parked (cursor not advanced)",
                              ret.subscriptionId, ret.readTopicId, ret.readPartition, ret.newNextOffset,
                              ret.errorCode);
                if (Session* session = sessions_.find(sub.sessionId))
                    trySendOwedPushError_(sub, *session);
                return;
            }

            // Find the subscription (it may have been removed if the client
            // unsubscribed while the read was in-flight).
            auto subIt = subscriptions_.find(ret.subscriptionId);
            if (subIt == subscriptions_.end()) return;
            Subscription& sub = subIt->second;
            if (sub.failed) [[unlikely]] return;   // F19 ⇄ F15 S3: parked — never push past the failed batch
            // A result for a cursor the subscription no longer holds (rewound /
            // re-seeded meanwhile) carries nothing to adopt.
            const bool current = sub.nextOffset == sub.issuedNextOffset;
            if (current) sub.cursorFresh = false;   // M2: the worker's answer is the cursor from now on

            if (ret.chunkBytes().empty()) {
                // F15 addendum R-1: the worker may have consumed batches without
                // returning any (a trailing marker, an aborted tail stripped on
                // the worker) — adopt its cursor, else the partition is re-read
                // every cycle.  R-3: a cursor at the worker's effective bound
                // parks until the gateway's HW snapshot moves.
                if (current) {
                    if (ret.newNextOffset > sub.nextOffset) sub.nextOffset = ret.newNextOffset;
                    noteWorkerReadAnswer_(sub, ret.pushEffectiveBound(), ret.pushRcNotReady());
                }
                return;
            }

            // Find the owning session.
            Session* session = sessions_.find(ret.sessionId);
            if (!session) return;
            if (!current) return;   // stale cursor: the next push cycle re-reads

            // Iterate whole record batches in the chunk — mirrors the inline
            // pushSubscriptions() loop exactly.
            const std::span<const uint8_t> chunk = ret.chunkBytes();
            std::size_t chunkPos  = 0;
            const std::size_t chunkSize = chunk.size();
            int batchesPushed = 0;
            bool backPressured = false;

            while (chunkPos < chunkSize
                   && batchesPushed < maxBatchesPerSubPerCycle_
                   && !backPressured)
            {
                constexpr std::size_t kMinBatchHdr =
                    static_cast<std::size_t>(kRecordCountOffset) + 4u;
                if (chunkPos + kMinBatchHdr > chunkSize) break;

                const uint8_t* batchPtr = chunk.data() + chunkPos;

                // Read baseOffset (LE64@0).
                int64_t baseOffset = 0;
                for (int i = 0; i < 8; ++i)
                    baseOffset |= (static_cast<int64_t>(batchPtr[i]) << (i * 8));

                // Isolation bound check (bound was captured at enqueue time).
                if (baseOffset >= ret.hwBound) break;

                // Read length field (LE32@8) → batchSize = 12 + length.
                uint32_t lengthField = 0;
                {
                    const uint8_t* p = batchPtr + kLengthOffset;
                    lengthField = static_cast<uint32_t>(p[0]) |
                                  (static_cast<uint32_t>(p[1]) << 8) |
                                  (static_cast<uint32_t>(p[2]) << 16) |
                                  (static_cast<uint32_t>(p[3]) << 24);
                }
                const std::size_t batchSize =
                    12u + static_cast<std::size_t>(lengthField);

                if (chunkPos + batchSize > chunkSize) break;

                // Read attributes (LE16@kAttributesOffset=21).
                uint16_t batchAttrs = 0;
                {
                    constexpr std::size_t kAttrOff =
                        static_cast<std::size_t>(kAttributesOffset);
                    if (chunkPos + kAttrOff + 2u <= chunkSize) {
                        batchAttrs = static_cast<uint16_t>(
                            static_cast<uint16_t>(batchPtr[kAttrOff]) |
                            (static_cast<uint16_t>(batchPtr[kAttrOff + 1u]) << 8));
                    }
                }
                constexpr uint16_t kControlBit      = 0x20u;
                constexpr uint16_t kTransactionalBit = 0x10u;

                // Skip control batches unconditionally.
                if (batchAttrs & kControlBit) {
                    const uint8_t* prc = batchPtr + kRecordCountOffset;
                    int32_t ctlRc = static_cast<int32_t>(
                        static_cast<uint32_t>(prc[0]) |
                        (static_cast<uint32_t>(prc[1]) << 8) |
                        (static_cast<uint32_t>(prc[2]) << 16) |
                        (static_cast<uint32_t>(prc[3]) << 24));
                    if (ctlRc < 1) ctlRc = 1;
                    chunkPos += batchSize;
                    sub.nextOffset = baseOffset + static_cast<int64_t>(ctlRc);
                    continue;
                }

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

                if (recordCount < 1) {
                    spdlog::warn(
                        "ClientGateway: push (worker) sub={} corrupt batch at "
                        "chunkPos={} recordCount={} baseOffset={} — skipping",
                        ret.subscriptionId, chunkPos, recordCount, baseOffset);
                    chunkPos += batchSize;
                    sub.nextOffset = baseOffset + 1;
                    continue;
                }

                // F15 S4 (I-B): aborted transactional batches were stripped by
                // the owning worker (and, before a group's isolation carrier,
                // the chunk was CUT at the first one — addendum R-2): the
                // gateway makes no read_committed decision.

                // Records frame (zero-copy batch span).  F19: any size up to the
                // egress maxMessageLength is delivered; above it the subscription
                // is failed explicitly and parked AT the batch (no silent skip).
                pc::Records rec;
                rec.sessionId      = ret.sessionId;
                rec.correlationId  = sub.subscribeCorrId;  // the client's Subscribe: exact instance identity
                rec.subscriptionId = ret.subscriptionId;
                rec.topicId        = ret.readTopicId;
                rec.partition      = ret.readPartition;
                rec.baseOffset     = baseOffset;
                rec.recordBatch    =
                    std::span<const uint8_t>(batchPtr, batchSize);

                const rune::node::RecordsSendResult sent = pushRecords_(*session, rec);
                if (sent.kind == rune::node::RecordsSendKind::TooLarge) [[unlikely]] {
                    failPushSubscriptionTooLarge_(sub, *session, rec.encodedSize());
                    break;
                }
                const int64_t pos = sent.position;

                if (pos > 0) {
                    sub.nextOffset =
                        baseOffset + static_cast<int64_t>(recordCount);
                    chunkPos += batchSize;
                    ++batchesPushed;
                } else {
                    backPressured = true;
                    spdlog::debug(
                        "ClientGateway: push (worker) sub={} back-pressured at "
                        "offset={} (pos={})",
                        ret.subscriptionId, baseOffset, pos);
                }
            }
            // F15 addendum R-1 / R-3: every batch of the chunk was delivered →
            // adopt the worker's cursor (it is past the batches it stripped
            // after the last one returned) and park at its effective bound.
            if (!backPressured && chunkPos >= chunkSize) {
                if (ret.newNextOffset > sub.nextOffset) sub.nextOffset = ret.newNextOffset;
                noteWorkerReadAnswer_(sub, ret.pushEffectiveBound(), ret.pushRcNotReady());
            }
            return;
        }

}

// ---------------------------------------------------------------------------
// drainPendingAcks (GR-5) — drain the deferred acks=All pending-ack queue.
//
// Called from doWork() each cycle, after the replica's HW has been advanced
// by any FetchResponse processing (replication doWork is ordered BEFORE gateway
// doWork on the duty-cycle thread in BrokerNode::doWork()).
//
// For each pending entry:
//   - If replica->highWatermark() > lastOffset AND replica->isr().size() >=
//     minIsr_ → HW covers the WHOLE batch AND the ISR is still healthy at
//     DRAIN time → send ProduceResponse{OK, partition, baseOffset} and remove.
//     (HW is EXCLUSIVE — count of committed records — so the last record at
//     offset `lastOffset` is majority-durable only once HW > lastOffset.)
//     (DR-BUG-3: the ISR>=minIsr check at drain closes the enqueue→drain
//     shrink-to-minority TOCTOU; a minority leader never falsely acks OK.)
//   - If nowNs_ - enqueuedAtNs >= kPendingAckTimeoutNs → stuck → send
//     ProduceResponse{Internal} (followers not advancing) and remove the entry.
//   - Otherwise → leave in queue for next cycle.
//
// Correctness:
//   - An acks=All ack ONLY fires when HW > lastOffset (majority-durable).
//     HW is EXCLUSIVE (committed-record count); the last record at lastOffset is
//     durable IFF HW > lastOffset (== HW >= lastOffset + 1).
//   - An acks=All ack is NEVER sent with errorCode=OK if HW <= lastOffset.
//   - Timeout path: sends Internal (error) — NEVER an erroneous OK.
//   - Produce-order is NOT relied on: swap-and-pop (erase-by-swap-back-pop) can
//     reorder the vector on removal, so positional order is not guaranteed after
//     the first eviction.  This is correct because each entry carries its own
//     lastOffset and the HW check is purely per-entry (HW > lastOffset).
//     Every entry resolves independently; a stuck entry does NOT block later
//     entries from acking.  This is safe because HW is monotonically
//     non-decreasing: once HW > lastOffset[n], the same HW automatically
//     satisfies all earlier batches in the same partition sequence.
//     Future maintainers: do NOT rely on positional vector order for
//     per-session or per-partition sequencing — the swap-and-pop is intentional.
//
// Zero-alloc: no heap allocation on any code path.  Erase-by-swap-back-pop
// ensures amortised O(1) removal at arbitrary positions.
//
// Single-writer: called only on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::drainPendingAcks()
{
    if (pendingAcks_.empty()) return;

    // Iterate with index (not iterator) because we swap-and-pop, which invalidates
    // iterators.  Traversal is front-to-back; positional order is not maintained
    // across removals (swap-and-pop reorders entries).  Each entry resolves
    // independently via its own HW check — no positional ordering is required.
    for (std::size_t i = 0; i < pendingAcks_.size(); ) {
        PendingAck& entry = pendingAcks_[i];

        bool remove = false;

        // HW check: has a majority replicated the WHOLE batch?
        //
        // INVARIANT (GR-9): highWatermark() is EXCLUSIVE — it is the count of
        // majority-committed records (== the next offset to be committed), i.e.
        // HW == leos[majority-1] where each follower/leader LEO is the NEXT slot
        // to write.  A record at offset X is majority-durable IFF HW > X
        // (equivalently HW >= X + 1).  entry.lastOffset is the INCLUSIVE offset
        // of the LAST record in the batch (base + lastOffsetDelta).  Therefore
        // the batch is fully majority-durable IFF  HW > lastOffset.
        //
        // Do NOT use >= here: with an exclusive HW that acks a single-record
        // batch (lastOffset=0) at HW=0 — BEFORE any follower has replicated it —
        // silently defeating acks=All durability (records lost on leader kill).
        // Do NOT over-correct to > lastOffset+1 either: once a majority's LEO
        // reaches N (all N records fetched) HW=N and HW > lastOffset=N-1 is true,
        // so the last record acks promptly (no never-ack stall).
        // DR-BUG-3: drain-time min-ISR COMMIT gate (Kafka min.insync.replicas).
        //
        // The enqueue-time gate (handleProduce) only proves the ISR was healthy
        // at ADMISSION.  The ISR can shrink below minIsr in the window between
        // enqueue and this drain (leader stranded in a minority after followers
        // die).  An acks=All entry may be acked OK ONLY when BOTH:
        //   (a) HW covers the whole batch (HW > lastOffset), AND
        //   (b) the ISR is still >= minIsr at DRAIN time.
        //
        // If the ISR is below minIsr we do NOT ack (hold the entry); it stays in
        // the queue and either (i) acks later if the ISR recovers and HW covers
        // it, or (ii) hits the existing timeout path below → error (never OK).
        // This closes the shrink-to-minority TOCTOU: a minority leader NEVER
        // returns a false majority-durable ack.
        //
        // Defense in depth: PartitionReplica::highWatermark() also clamps the
        // exposed HW to the last majority-committed offset while |ISR| < minIsr
        // (so HW > lastOffset would already be false for the stranded tail); this
        // explicit gate makes the commit contract unambiguous at the ack site and
        // is robust to any future HW-exposure change.
        // G1.1: use per-cycle snapshot (plain int) instead of per-iteration atomic load.
        const int drainMinIsr = cachedMinIsr_;
        // Task 10 round 4 (Important 3): resolve the replica through the
        // registry on EVERY check — never a pointer held across cycles.  Gone
        // or re-created (different incarnation) → stale → fail RETRIABLY.
        const rune::replication::PartitionReplica* rep = nullptr;
        if (replicaRegistry_) {
            rep = entry.incarnation != 0
                ? replicaRegistry_->resolve(entry.topicId, entry.partition, entry.incarnation)
                : replicaRegistry_->lookup(entry.topicId, entry.partition);
        }
        if (rep == nullptr) [[unlikely]] {
            if (Session* session = sessions_.find(entry.sessionId)) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                resp.partition     = entry.partition;
                resp.baseOffset    = 0;
                const std::size_t encoded = resp.encodeFor(scratch_.data(), session->clientVersion);   // D6 M4
                sendReliable_(*session, scratch_.data(), encoded);
            }
            spdlog::debug("ClientGateway: acks=All pending ack for topic={} partition={} is stale "
                          "(partition removed / re-created) → NotLeader", entry.topicId, entry.partition);
            if (i + 1 < pendingAcks_.size()) pendingAcks_[i] = std::move(pendingAcks_.back());
            pendingAcks_.pop_back();
            continue;
        }
        // Truncation (KIP-101) / demotion: the worker path's §3.2 rule — a
        // waiter whose data a truncation or reconcile may have replaced never
        // fires OK (its offsets may now hold another leader's data); it fails
        // RETRIABLY (the resend is deduplicated or appended by the proven
        // producer state).  The replica is control-owned on this inline path.
        if (!inlineWaiterStillValid_(*rep, entry)) [[unlikely]] {
            if (Session* session = sessions_.find(entry.sessionId)) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::NotLeader);
                resp.partition     = entry.partition;
                resp.baseOffset    = 0;
                const std::size_t encoded = resp.encodeFor(scratch_.data(), session->clientVersion);   // D6 M4
                sendReliable_(*session, scratch_.data(), encoded);
            }
            if (i + 1 < pendingAcks_.size()) pendingAcks_[i] = std::move(pendingAcks_.back());
            pendingAcks_.pop_back();
            continue;
        }
        // Task #13: use gatewayIsrSize() (published-snapshot acquire-load)
        // instead of the live isr().size() — closes the ISR vector race.
        // (Round 4: pre-existing atomic snapshot — not extended; F9 removes it.)
        const bool isrHealthy = rep->gatewayIsrSize() >= drainMinIsr;

        // Task #13: use gatewayHighWatermark() (published-snapshot acquire-load)
        // instead of the live highWatermark() — closes TSan R5/R6b.
        const int64_t entryHw = rep->gatewayHighWatermark();

        if (isrHealthy && entryHw > entry.lastOffset) {
            // HW advanced past this batch AND ISR is healthy — ack OK.
            Session* session = sessions_.find(entry.sessionId);
            if (session) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::OK);
                resp.partition     = entry.partition;
                resp.baseOffset    = entry.baseOffset;
                static_assert(pc::ProduceResponse::ENCODED_SIZE <= kScratchSize,
                              "scratch too small for ProduceResponse");
                const std::size_t encoded = resp.encodeFor(scratch_.data(), session->clientVersion);   // D6 M4
                // G0.1: reliable delivery (retry on back-pressure) — this is the
                // highest-durability acks=All OK ack; a silent drop here is the
                // exact defect #3 the retry queue exists to prevent.
                sendReliable_(*session, scratch_.data(), encoded);
                spdlog::debug("ClientGateway: acks=All deferred ack sent OK "
                              "sessionId={} partition={} baseOffset={} lastOffset={} hw={}",
                              entry.sessionId, entry.partition, entry.baseOffset,
                              entry.lastOffset, entryHw);
            }
            // Session gone (reaped): entry is silently discarded (no way to ack).
            remove = true;
        } else if (nowNs_ - entry.enqueuedAtNs >= kPendingAckTimeoutNs) {
            // Timeout: HW never advanced to cover this batch within the timeout
            // (follower stuck/crashed and ISR not updated yet).  Send an error
            // response so the client can retry rather than waiting forever.
            // NEVER send OK here — that would falsely claim majority durability.
            Session* session = sessions_.find(entry.sessionId);
            if (session) {
                pc::ProduceResponse resp;
                resp.sessionId     = entry.sessionId;
                resp.correlationId = entry.correlationId;
                resp.errorCode     = static_cast<int16_t>(pc::ErrorCode::Internal);
                resp.partition     = entry.partition;
                resp.baseOffset    = entry.baseOffset;
                const std::size_t encoded = resp.encodeFor(scratch_.data(), session->clientVersion);   // D6 M4
                // G0.1: reliable delivery (retry on back-pressure).
                sendReliable_(*session, scratch_.data(), encoded);
                spdlog::warn("ClientGateway: acks=All deferred ack timed out "
                             "sessionId={} partition={} baseOffset={} lastOffset={} "
                             "hw={} elapsed_ms={}",
                             entry.sessionId, entry.partition, entry.baseOffset,
                             entry.lastOffset, entryHw,
                             (nowNs_ - entry.enqueuedAtNs) / 1'000'000LL);
            }
            remove = true;
        }

        if (remove) {
            // Swap-and-pop: move last element to position i, shrink by 1.
            // If this IS the last element, swap is a no-op and pop reduces size by 1.
            if (i + 1 < pendingAcks_.size()) {
                pendingAcks_[i] = std::move(pendingAcks_.back());
            }
            pendingAcks_.pop_back();
            // Do NOT increment i — the swapped-in entry at position i needs checking.
        } else {
            ++i;
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
    //
    // E5.1.3 (d) R7 fix: skip any log in workerOwnedLogs_ — those are exclusively
    // owned by a worker thread (which submits its own fsyncs to its io backend:
    // syncOwnedLogsAsync_ + follower flush / seals, F30).  Calling Log::sync from the gateway thread while the worker thread
    // appends is a TSan-confirmed data race (R7).  Internal partitions (topicId<0)
    // are never added to workerOwnedLogs_ and always take this gateway sync path.
    std::unordered_set<rune::storage::Log*> stillDirty;
    for (rune::storage::Log* log : dirtyLogs_) {
        // R7 gate: skip worker-owned logs (they sync themselves on the worker thread).
        if (!workerOwnedLogs_.empty() && workerOwnedLogs_.count(log)) {
            // Worker-owned: remove from dirtyLogs_ (the worker handles sync).
            continue;
        }
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
        // fsyncgate: an fsync failure is never retried — the log is permanently
        // failed (LogSegment::sync) and the broker halts.
        if (!synced && log->storageFailed()) raiseStorageFailure_(log->dir(), log->storageErrno());
        if (!synced) stillDirty.insert(log);
    }
    dirtyLogs_ = std::move(stillDirty);
    lastFlushNs_ = nowNs_;
}

// ---------------------------------------------------------------------------
// handleGroupMessage (CG-3) — route consumer-group frames to the
// GroupCoordinationService.
//
// Flow:
//   1. Require a known session (drop on unknown session — no egress).
//   2. Delegate decode + act + encode to groupService_ (null-safe: drop if
//      service not wired in).
//   3. Send the encoded response on the session egress (same send path as all
//      other handlers — sendTo / scratch_).
//
// Reused response buffer: responses are encoded into scratch_ which is already
// 68 KiB — sufficient for any group response (max SyncGroupResponse with 64
// partitions ≈ 24 + 2 + 2 + 64*8 = 540 B; OffsetFetchResponse with 1024
// entries ≈ 24 + 2 + 1024*18 = 18458 B).  Well within kScratchSize.
//
// Single-writer: called only on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::handleGroupMessage(const uint8_t* buf, std::size_t len,
                                       int64_t nowNs, uint16_t tid)
{
    if (!groupService_) {
        // No-hang (R2): no group service on this node — answered Internal.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal),
                             "group service not wired");
        return;
    }

    // Decode just the session/correlationId from the MessageHeader to find the
    // session egress.  Every group request carries sessionId in the MessageHeader
    // (positions [8..15] LE64).  We don't need the full decode here — just the
    // session lookup.
    auto hdrOpt = pc::MessageHeader::decode(buf, len);
    if (!hdrOpt) {
        noteUnanswerable_(buf, len, "group message header undecodable");
        return;
    }

    Session* session = sessions_.find(hdrOpt->sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "group message for unknown session");
        return;
    }
    if (redirectToCoordinator_(*session, hdrOpt->correlationId)) return;   // D4
    if (denyGroupRequest_(*session, tid, buf, len)) return;                 // authentication Task 12 (D18)

    // Fix round 2 (old-client compatibility): a client below
    // kClientVersionInvalidRequest re-joins forever on an InvalidRequest
    // JoinGroup / SyncGroup answer — the gateway checks the body itself for
    // those clients so answerRejection_ can answer ProtocolVersion instead.
    // Fix round 4 (item 8): ... and Heartbeat / OffsetFetch, whose rejection an
    // old client gets as UnknownMember (answerRejection_).
    if (!pc::clientUnderstandsInvalidRequest(session->clientVersion)) [[unlikely]] {
        const bool undecodable =
            (tid == static_cast<uint16_t>(pc::TemplateId::JoinGroup) && !pc::JoinGroup::decode(buf, len, session->clientVersion)) ||
            (tid == static_cast<uint16_t>(pc::TemplateId::SyncGroup) && !pc::SyncGroup::decode(buf, len)) ||
            (tid == static_cast<uint16_t>(pc::TemplateId::Heartbeat) && !pc::Heartbeat::decode(buf, len)) ||
            (tid == static_cast<uint16_t>(pc::TemplateId::OffsetFetch) && !pc::OffsetFetch::decode(buf, len));
        if (undecodable) {
            (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                                 "group request undecodable (old client)");
            return;
        }
    }

    // Dispatch to the service.  The service encodes the response into scratch_.
    // Capacity is passed so handlers bound-check BEFORE writing each element;
    // the post-write guard below is a final safety net only.
    const rune::coordination::NsRequestContext nsCtx{sessionNsId_(*session), permsOf_(*session).isRoot(), &namespaces_.registry()};
    std::size_t encoded = 0;
    const std::size_t cap = scratch_.size();
    const auto templateId = static_cast<pc::TemplateId>(tid);
    switch (templateId) {
        case pc::TemplateId::JoinGroup:
            encoded = groupService_->handleJoinGroup(buf, len, session->clientVersion, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::SyncGroup:
            encoded = groupService_->handleSyncGroup(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::Heartbeat:
            encoded = groupService_->handleHeartbeat(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::LeaveGroup:
            encoded = groupService_->handleLeaveGroup(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::OffsetCommit:
            encoded = groupService_->handleOffsetCommit(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::OffsetFetch:
            encoded = groupService_->handleOffsetFetch(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        default:
            // Unreachable: onClientFrame_ routes only the six group request ids here.
            (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                                 "not a group request");
            return;
    }

    // #433: an OffsetCommit answer waits for its __consumer_offsets records to commit - the service parked it and
    // sends it itself (sendControlFrame) once they do, or NotCoordinator when the offsets partition changed first.
    if (encoded == 0 && templateId == pc::TemplateId::OffsetCommit) return;   // deferred
    // No-hang (R2): the service answers its own decode failures
    // (InvalidRequest, RequestRejection.h) — no other group handler defers its
    // answer, so 0 here means it could not answer at all: Internal, never a drop.
    if (encoded == 0 || encoded > scratch_.size()) [[unlikely]] {
        spdlog::error("ClientGateway: group response tid={} encoded size {} (scratch {}) — answered Internal",
                      tid, encoded, scratch_.size());
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal),
                             "group service produced no answer");
        return;
    }

    // #126: a fetch-all (no entries) lists only the topics the caller may READ - a listed entry already needs READ
    // (denyGroupRequest_).  Sentinels (topicId < 0) stay.
    if (templateId == pc::TemplateId::OffsetFetch) [[unlikely]] encoded = filterFetchAllByRead_(*session, buf, len, encoded);

    // Fix round 4 (item 8, pre-v5 clients): a v4 member stays Stable on a
    // Heartbeat / SyncGroup error outside the rebalance set — Internal (an
    // unknown coordinator result) is answered UnknownMember instead, and the
    // member is evicted so the answer is true: the v4 member re-joins.
    if (!pc::clientUnderstandsInvalidRequest(session->clientVersion) &&
        (templateId == pc::TemplateId::Heartbeat || templateId == pc::TemplateId::SyncGroup) &&
        encoded >= pc::MessageHeader::ENCODED_SIZE + 2) [[unlikely]] {
        uint8_t* const ec = scratch_.data() + pc::MessageHeader::ENCODED_SIZE;   // errorCode: body offset 0
        const auto code = static_cast<pc::ErrorCode>(static_cast<uint16_t>(ec[0] | (ec[1] << 8)));
        if (code == pc::ErrorCode::Internal || code == pc::ErrorCode::InvalidRequest) {
            const auto um = static_cast<uint16_t>(pc::ErrorCode::UnknownMember);
            ec[0] = static_cast<uint8_t>(um & 0xFF);
            ec[1] = static_cast<uint8_t>(um >> 8);
            evictOldClientMember_(*session, tid, buf, len, nowNs);
            ++oldClientUnknownMemberAnswers_;
        }
    }

    // Queued under back-pressure (never a best-effort drop of a coordinator answer).
    sendAnswer_(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// D4 (backlog "group subscribe redirect"): the group / transaction coordinator
// is the consensus leader (GroupCoordinationService / TransactionCoordination-
// Service coordinatorFor: v1 seam).  A follower used to answer every group /
// txn request NotCoordinator — but Rune has no FindCoordinator, and the client
// re-sends coordinator requests on its seed session, so a member bootstrapped
// against a follower re-joined that follower forever (dockerized repro:
// test_cg_members_on_a_follower_find_the_coordinator consumed 0 of 80).  Like
// a data op on a follower, the answer names the coordinator:
// ErrorResponse{NotCoordinator, leader endpoint}.  NotCoordinator (not Redirect)
// so the client tells it from a data-op redirect — while a group is active the
// client never moves its seed on a data-op Redirect, but a coordinator answer
// moves the seed (the coordinator's session by design) and the member re-joins
// there (rune-client-cpp Client::onCoordinatorRedirect_).  Unknown leader
// endpoint (election in progress): fall through — the service answers the typed
// NotCoordinator (retriable).
// ---------------------------------------------------------------------------
bool ClientGateway::redirectToCoordinator_(Session& session, int64_t correlationId)
{
    if (!leaderView_) return false;
    const LeaderView lv = leaderView_();
    if (lv.isLeader || lv.leaderClientEndpoint.empty()) return false;
    pc::ErrorResponse errResp;
    errResp.sessionId     = session.sessionId;
    errResp.correlationId = correlationId;
    errResp.code          = static_cast<int16_t>(pc::ErrorCode::NotCoordinator);
    errResp.message       = lv.leaderClientEndpoint;
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2 +
                               errResp.message.size();
    if (needed > scratch_.size()) return false;
    const std::size_t encoded = errResp.encode(scratch_.data());
    ++coordinatorRedirects_;
    spdlog::debug("ClientGateway: coordinator request on a follower sessionId={} corr={} → NotCoordinator {}",
                  session.sessionId, correlationId, lv.leaderClientEndpoint);
    // No-hang (R2): queued under back-pressure, never a best-effort drop.
    sendAnswer_(session, scratch_.data(), encoded);
    return true;
}

// ---------------------------------------------------------------------------
// handleTxnMessage (TX-3) — route transaction frames to the
// TransactionCoordinationService.
//
// Mirrors handleGroupMessage exactly: decode session from MessageHeader,
// dispatch to txnService_, send the encoded response on the session egress.
//
// Single-writer: called only on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::handleTxnMessage(const uint8_t* buf, std::size_t len,
                                     int64_t nowNs, uint16_t tid)
{
    if (!txnService_) {
        // No-hang (R2): no txn service on this node — answered Internal.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal),
                             "txn service not wired");
        return;
    }

    auto hdrOpt = pc::MessageHeader::decode(buf, len);
    if (!hdrOpt) {
        noteUnanswerable_(buf, len, "txn message header undecodable");
        return;
    }

    Session* session = sessions_.find(hdrOpt->sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "txn message for unknown session");
        return;
    }
    if (redirectToCoordinator_(*session, hdrOpt->correlationId)) return;   // D4
    if (denyTxnRequest_(*session, tid, buf, len)) return;                   // authentication Task 12 (D18)
    // Namespaces Phase 2 Task 9 (D23): the Txns feature is checked at the first transactional call (InitProducerId).
    if (tid == static_cast<uint16_t>(pc::TemplateId::InitProducerId) &&
        !rune::node::nsFeatureAllowed(nsSlots_.limits(session->nsSlot), rune::protocol::kNsFeatureTxns)) [[unlikely]] {
        if (!answerRejection_(*session, *hdrOpt, nsLimitCode_(*session), "namespace limit exceeded: transactions are not enabled for this namespace", buf, len))
            noteUnanswerable_(buf, len, "namespace txns feature");
        return;
    }

    std::size_t encoded = 0;
    // Fix round 3 (I3): the scratch always holds any fixed-size txn answer and
    // any rejection — a handler's 0 can only mean "deferred" (ITxnRequestHandler).
    static_assert(kScratchSize >= kTxnMinOutCap, "txn handlers need kTxnMinOutCap");
    const rune::coordination::NsRequestContext nsCtx{sessionNsId_(*session), permsOf_(*session).isRoot(), &namespaces_.registry()};
    const std::size_t cap = scratch_.size();
    const auto templateId = static_cast<pc::TemplateId>(tid);
    switch (templateId) {
        case pc::TemplateId::InitProducerId:
            encoded = txnService_->handleInitProducerId(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::AddPartitionsToTxn:
            encoded = txnService_->handleAddPartitions(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::AddOffsetsToTxn:
            encoded = txnService_->handleAddOffsets(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::TxnOffsetCommit:
            encoded = txnService_->handleTxnOffsetCommit(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        case pc::TemplateId::EndTxn:
            encoded = txnService_->handleEndTxn(buf, len, nowNs, scratch_.data(), cap, nsCtx);
            break;
        default:
            // Unreachable: onClientFrame_ routes only the five txn request ids here.
            (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                                 "not a txn request");
            return;
    }

    // 0 = the answer is DEFERRED (ITxnRequestHandler contract): (#358) any answer
    // that depends on a __transaction_state record, EndTxn's Prepare* record
    // included (#38: not its markers), waits for that record to commit - the service sends it later (or
    // answers NotCoordinator when it unloads first).  The service answers decode
    // failures (InvalidRequest) and responses that do not fit (MessageTooLarge)
    // itself.
    if (encoded == 0) return;   // deferred
    if (encoded > scratch_.size()) [[unlikely]] {
        spdlog::error("ClientGateway: txn response tid={} encoded size {} > scratch {} — BUG; answered Internal",
                      tid, encoded, scratch_.size());
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal),
                             "txn response overflow");
        return;
    }

    // Queued under back-pressure (never a best-effort drop of a coordinator answer).
    sendAnswer_(*session, scratch_.data(), encoded);
}

// ---------------------------------------------------------------------------
// Catalog history Task 9: the DDL extension → the admin sink's DdlMetaView.  The
// session id and the source address (the session's egress channel) are the
// gateway's own (ruling 13); a pre-v8 frame (no extension) carries none of the
// client fields and a {0, 0} request id.  Views into the ingress buffer / session.
// ---------------------------------------------------------------------------
rune::gateway::DdlMetaView ClientGateway::ddlMetaOf_(const pc::DdlExtension* ext, int64_t sessionId,
                                                     const Session& session, uint32_t targetNsId) noexcept
{
    rune::gateway::DdlMetaView m;
    m.targetNsId    = targetNsId;   // namespaces Task 10: the object's resolved namespace (kHiddenNsId = answered missing)
    m.sessionId     = sessionId;
    m.sourceAddress = session.egressChannel;
    // The authenticated user (Task 11; Task 12 makes the catalog principal carry it too): a view into the session's
    // auth state, copied by the admin ring submit before this frame's handler returns.
    if (const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&session.auth)) m.authenticated = a->user;
    else if (const auto* p = std::get_if<rune::gateway::AuthPasswordChangeRequired>(&session.auth)) m.authenticated = p->user;
    m.callerNsId = sessionNsId_(session);   // namespaces Task 9: the login's namespace (0 = `default` / anonymous)
    if (ext != nullptr) {
        m.statement         = ext->statement;
        m.clientName        = ext->clientName;
        m.requestId         = ext->requestId;
        if (ext->hasReplayedFrom == 1)   // the wire's flag (decode refuses anything but 0 / 1)
            m.replayedFrom = rune::gateway::ReplayedFromView{ext->replayedClusterId, ext->replayedOffset,
                                                             ext->replayedPrincipal};
    }
    return m;
}

// The admin sink's verdict → the answer when it did not take the request.
// RingFull: Internal (retriable back-pressure, as before).  TooLarge: the
// request can never fit one admin ring message — InvalidRequest, explicitly.
void ClientGateway::answerAdminSubmit_(const uint8_t* buf, std::size_t len, rune::gateway::AdminSubmit r,
                                       const char* ringFull, const char* tooLarge)
{
    switch (r) {
    case rune::gateway::AdminSubmit::Accepted:
        return;   // the AdminWorker answers asynchronously on the session egress
    case rune::gateway::AdminSubmit::RingFull:
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), ringFull);
        return;
    case rune::gateway::AdminSubmit::TooLarge:
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), tooLarge);
        return;
    }
}

// ---------------------------------------------------------------------------
// Catalog history Task 11: the DDL route gate (spec §5.1 Redirect, §5.2 / §5.5
// NotLeader, §9 Internal when halted).  The route is a control-thread value
// (setCatalogRoute); the gateway reads no catalog state.  A pre-v8 session
// (F37) cannot follow a DDL Redirect and waits an ErrorResponse out, so it is
// answered in its request's OWN response type: InvalidRequest on a broker that
// knows another leader (logged at warn — those frames carry no message),
// Internal when halted, NotLeader when no leader is known.
// ---------------------------------------------------------------------------
bool ClientGateway::ddlRouted_(const uint8_t* buf, std::size_t len, const rune::gateway::CatalogRoute& route,
                               const char* log)
{
    if (std::holds_alternative<rune::gateway::RouteLocalLeader>(route)) [[likely]] return false;
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) return false;                        // the handler answers / notes it
    Session* session = sessions_.find(hdr->sessionId);
    if (!session) return false;                    // the handler notes it (no session to answer)
    ++ddlRoutedAway_;
    const auto* halted = std::get_if<rune::gateway::RouteHalted>(&route);
    const auto* remote = std::get_if<rune::gateway::RouteRemote>(&route);

    if (!pc::clientUnderstandsCatalogDdl(session->clientVersion)) {
        uint16_t code = static_cast<uint16_t>(pc::ErrorCode::NotLeader);
        const char* why = "pre-v8 DDL: no catalog leader known";
        if (halted != nullptr) {
            code = static_cast<uint16_t>(pc::ErrorCode::Internal);
            why  = "pre-v8 DDL: security/catalog apply halted on this broker";
        } else if (remote != nullptr) {
            code = static_cast<uint16_t>(pc::ErrorCode::InvalidRequest);
            why  = "client too old for cluster DDL routing; upgrade";
            ++preV8DdlRefused_;
            spdlog::warn("ClientGateway: a pre-v8 client's DDL (session {}, template {}, client version {}) reached "
                         "a broker that is not the catalog leader — answered InvalidRequest: client too old for "
                         "cluster DDL routing; upgrade (refused so far: {})", hdr->sessionId, hdr->templateId,
                         session->clientVersion, preV8DdlRefused_);
        }
        if (!answerRejection_(*session, *hdr, code, why, buf, len)) noteUnanswerable_(buf, len, why);
        return true;
    }

    pc::ErrorResponse err;
    err.sessionId     = hdr->sessionId;
    err.correlationId = hdr->correlationId;
    char haltedText[64];
    if (halted != nullptr) {
        // Spec §9: never a redirect — the leader may be healthy, THIS broker is not.
        const int n = std::snprintf(haltedText, sizeof haltedText, "%s apply halted at offset %lld",
                                    log, static_cast<long long>(halted->offset));
        err.code    = static_cast<int16_t>(pc::ErrorCode::Internal);
        err.message = std::string_view(haltedText, n > 0 ? static_cast<std::size_t>(n) : 0u);
    } else if (remote != nullptr && !remote->leaderClientEndpoint.empty()) {
        err.code    = static_cast<int16_t>(pc::ErrorCode::Redirect);
        err.message = remote->leaderClientEndpoint;
    } else {
        err.code = static_cast<int16_t>(pc::ErrorCode::NotLeader);   // retriable: no leader known yet
    }
    const std::size_t needed = pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2 + err.message.size();
    if (needed > scratch_.size()) [[unlikely]] {
        // An endpoint too long for the frame is never dropped: the bare NotLeader.
        err.code    = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        err.message = {};
    }
    const std::size_t n = err.encode(scratch_.data());
    spdlog::debug("ClientGateway: DDL tid={} session={} corr={} routed ({}): code {}", hdr->templateId,
                  hdr->sessionId, hdr->correlationId, rune::gateway::catalogRouteName(route), err.code);
    sendAnswer_(*session, scratch_.data(), n);
    return true;
}

// ---------------------------------------------------------------------------
// Task 8a: handleDefineTopic (RULING-2 F) — route DefineTopic (64) to the admin
// sink via a ring hand-off.  Validate + require known session + decode, then
// copy the fields into the sink and RETURN immediately.  NO registry access, NO
// blocking on the gateway hot path — a stalled AdminWorker never delays produce.
// Catalog history Task 9: the v8 trailer (rf, config, DDL extension) rides along.
// ---------------------------------------------------------------------------
void ClientGateway::handleDefineTopic(const uint8_t* buf, std::size_t len)
{
    if (ddlRouted_(buf, len)) return;   // Task 11: not the catalog leader / halted — answered here
    if (!adminSink_) {
        // No-hang (R2): no admin thread wired — answered Internal.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "DefineTopic: admin sink not wired");
        return;
    }
    auto msgOpt = pc::DefineTopic::decode(buf, len);
    if (!msgOpt) {
        // Final review M3 / no-hang (R2): a malformed body (e.g. an unknown
        // persistence byte) is answered InvalidSchema (23) on the header's
        // correlationId — the client never waits out its timeout.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidSchema), "DefineTopic body undecodable");
        return;
    }
    const pc::DefineTopic& m = *msgOpt;

    // Require a known session (mirror handleProduce).
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "DefineTopic for unknown session");
        return;
    }
    // Namespaces Task 10 (D7, D9): the name in the session's view.  A hidden namespace is answered as a missing one
    // by the admin thread itself (kHiddenNsId); every check below sees the LOCAL name only.
    const rune::gateway::Resolution res = resolve_(*session, m.topicName);
    if (res.r == rune::gateway::Resolved::Malformed) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidTopicName), kMalformedNameDetail);
        return;
    }
    // Authentication Task 12 (spec §2.1): CREATE on the topic name (a CLUSTER grant is part of the table); CreateSchema
    // is the same request.
    if (deny_(*session, rune::gateway::authorizeName(permsOf_(*session), rune::security::ResourceType::Topic, res.local,
                                                     rune::security::Operation::Create), buf, len))
        return;
    // Issue #129: if the name is an existing UNTYPED topic, a descriptor makes this a new schema version of it
    // (CreateSchemaOps) - ALTER on the topic, exactly as AlterTopic (ALTER implies DESCRIBE, D15).  Whether the name
    // exists is the admin thread's call (its catalog may be ahead of this thread's index), so the verdict rides along.
    std::optional<rune::gateway::Denial> adopt;
    if (m.descriptorLen != 0)
        adopt = rune::gateway::authorizeName(permsOf_(*session), rune::security::ResourceType::Topic, res.local,
                                             rune::security::Operation::Alter);

    // Copy-and-return: the sink OWNS the copied bytes (every view points into
    // the ingress buffer and is copied by the sink before return).
    answerAdminSubmit_(buf, len,
                       adminSink_->submitDefineTopic(m.sessionId, m.correlationId, res.local, m.partitionCount,
                                                     m.keyField, m.validate, m.descriptorBlob, m.descriptorLen,
                                                     m.persistence, m.replicationFactor, m.config,
                                                     ddlMetaOf_(m.hasExt ? &m.ext : nullptr, m.sessionId, *session, res.nsId),
                                                     adopt ? std::string_view(adopt->detail) : std::string_view{}),
                       "DefineTopic: admin ring full", "DefineTopic: request larger than the admin ring message");
}

// ---------------------------------------------------------------------------
// Task 8a: handleGetTopicSchema (RULING-2 F) — route GetTopicSchema (66) to the
// admin sink via a ring hand-off (same discipline as handleDefineTopic).
// ---------------------------------------------------------------------------
void ClientGateway::handleGetTopicSchema(const uint8_t* buf, std::size_t len)
{
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "GetTopicSchema: admin sink not wired");
        return;
    }
    auto msgOpt = pc::GetTopicSchema::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered TopicSchemaResponse{InvalidSchema}.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidSchema), "GetTopicSchema body undecodable");
        return;
    }
    const pc::GetTopicSchema& m = *msgOpt;

    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "GetTopicSchema for unknown session");
        return;
    }
    const rune::gateway::Resolution res = resolve_(*session, m.topicName);   // namespaces Task 10 (D7, D9)
    if (res.r == rune::gateway::Resolved::Malformed) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidTopicName), kMalformedNameDetail);
        return;
    }
    // Authentication Task 12 (D18): DESCRIBE on the name, whether it is a topic or a view (the text always says TOPIC).
    if (deny_(*session, rune::gateway::authorizeTopicOrViewName(permsOf_(*session), res.local,
                                                                 rune::security::Operation::Describe, objectIndex_,
                                                                 res.nsId), buf, len))
        return;
    if (!adminSink_->submitGetSchema(m.sessionId, m.correlationId, res.local,
                                     m.requestedFingerprint, res.nsId)) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "GetTopicSchema: admin ring full");
    }
}

// ---------------------------------------------------------------------------
// Task 14: handleAlterTopic (RULING-2 F) — route AlterTopic (68) to the admin
// sink via a ring hand-off (same discipline as handleDefineTopic/
// handleGetTopicSchema).  Validate + require known session + decode, then
// copy the fields into the sink and RETURN immediately.  NO registry access,
// NO blocking on the gateway hot path — a stalled AdminWorker never delays
// produce.  The AlterTopicResponse (69) is produced asynchronously on the
// session egress via the admin→control egress ring (respondAlter, Task 11).
// ---------------------------------------------------------------------------
void ClientGateway::handleAlterTopic(const uint8_t* buf, std::size_t len)
{
    if (ddlRouted_(buf, len)) return;   // Task 11: not the catalog leader / halted — answered here
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "AlterTopic: admin sink not wired");
        return;
    }
    auto msgOpt = pc::AlterTopic::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered AlterTopicResponse{InvalidSchema}.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidSchema), "AlterTopic body undecodable");
        return;
    }
    const pc::AlterTopic& m = *msgOpt;

    // Require a known session (mirror handleDefineTopic).
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "AlterTopic for unknown session");
        return;
    }
    const rune::gateway::Resolution res = resolve_(*session, m.topicName);   // namespaces Task 10 (D7, D9)
    if (res.r == rune::gateway::Resolved::Malformed) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidTopicName), kMalformedNameDetail);
        return;
    }
    if (deny_(*session, rune::gateway::authorizeName(permsOf_(*session), rune::security::ResourceType::Topic, res.local,
                                                     rune::security::Operation::Alter), buf, len))
        return;

    // Copy-and-return: the sink OWNS the copied bytes (topicName/opsBlob views
    // point into the ingress buffer and are copied by the sink before return).
    answerAdminSubmit_(buf, len,
                       adminSink_->submitAlterTopic(m.sessionId, m.correlationId, res.local, m.expectedVersion,
                                                    m.opsBlob, m.opsLen,
                                                    ddlMetaOf_(m.hasExt ? &m.ext : nullptr, m.sessionId, *session, res.nsId)),
                       "AlterTopic: admin ring full", "AlterTopic: request larger than the admin ring message");
}

// ---------------------------------------------------------------------------
// Task 19: handleCreateView (RULING-2 F) — route CreateView (70) to the admin
// sink via a ring hand-off (same discipline as handleDefineTopic).  Validate +
// require known session + decode, then copy the fields into the sink and RETURN
// immediately.  NO registry access, NO blocking on the gateway hot path — a
// stalled AdminWorker never delays produce.  The CreateViewResponse (71) is
// produced asynchronously on the session egress via the admin→control egress
// ring (respondCreateView).
// ---------------------------------------------------------------------------
void ClientGateway::handleCreateView(const uint8_t* buf, std::size_t len)
{
    if (ddlRouted_(buf, len)) return;   // Task 11: not the catalog leader / halted — answered here
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "CreateView: admin sink not wired");
        return;
    }
    auto msgOpt = pc::CreateView::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered CreateViewResponse{InvalidSchema} (e.g. an
        // unknown persistence byte — CreateView::decode rejects > 1).
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidSchema), "CreateView body undecodable");
        return;
    }
    const pc::CreateView& m = *msgOpt;

    // Require a known session (mirror handleDefineTopic).
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "CreateView for unknown session");
        return;
    }
    const rune::gateway::Resolution res = resolve_(*session, m.viewName);   // namespaces Task 10 (D7, D9)
    if (res.r == rune::gateway::Resolved::Malformed) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidTopicName), kMalformedNameDetail);
        return;
    }
    // Authentication Task 12 (spec §2.1, D19): CREATE on the view name AND READ on every source topic of the plan.
    {
        std::optional<rune::gateway::Denial> d = rune::gateway::authorizeName(
            permsOf_(*session), rune::security::ResourceType::View, res.local, rune::security::Operation::Create);
        if (!d) d = authorizeViewSources_(*session, m.planBlob, m.planLen);
        if (deny_(*session, d, buf, len)) return;
    }

    // Copy-and-return: the sink OWNS the copied bytes (viewName/keyField/planBlob
    // views point into the ingress buffer and are copied by the sink before return).
    answerAdminSubmit_(buf, len,
                       adminSink_->submitCreateView(m.sessionId, m.correlationId, res.local, m.keyField,
                                                    m.persistence, m.planBlob, m.planLen,
                                                    ddlMetaOf_(m.hasExt ? &m.ext : nullptr, m.sessionId, *session, res.nsId)),
                       "CreateView: admin ring full", "CreateView: request larger than the admin ring message");
}

// ---------------------------------------------------------------------------
// Catalog history Task 9: DropTopic (74), AlterTopicConfig (75), DropView (76),
// AlterView (77) — the same copy-and-return discipline as handleDefineTopic;
// answered by a DdlResponse (78) from the admin thread.  Each carries a
// mandatory DDL extension: an undecodable body is answered InvalidRequest.
// ---------------------------------------------------------------------------
template <class Msg, class Submit>
void ClientGateway::handleCatalogDdl_(const uint8_t* buf, std::size_t len, const char* what, Submit&& submit)
{
    if (ddlRouted_(buf, len)) return;   // Task 11: not the catalog leader / halted — answered here
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), what);
        return;
    }
    auto msgOpt = Msg::decode(buf, len);
    if (!msgOpt) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), what);
        return;
    }
    const Msg& m = *msgOpt;
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, what);
        return;
    }
    std::string_view name;
    if constexpr (std::is_same_v<Msg, pc::DropView> || std::is_same_v<Msg, pc::AlterView>) name = m.viewName;
    else name = m.topicName;
    const rune::gateway::Resolution res = resolve_(*session, name);   // namespaces Task 10 (D7, D9)
    if (res.r == rune::gateway::Resolved::Malformed) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidTopicName), kMalformedNameDetail);
        return;
    }
    if (deny_(*session, authorizeCatalogDdl_(*session, m, res.local), buf, len)) return;   // authentication Task 12
    answerAdminSubmit_(buf, len, submit(m, res.local, ddlMetaOf_(&m.ext, m.sessionId, *session, res.nsId)),
                       "catalog DDL: admin ring full", "catalog DDL: request larger than the admin ring message");
}

void ClientGateway::handleDropTopic(const uint8_t* buf, std::size_t len)
{
    handleCatalogDdl_<pc::DropTopic>(buf, len, "DropTopic not routable",
        [this](const pc::DropTopic& m, std::string_view local, const rune::gateway::DdlMetaView& meta) {
            return adminSink_->submitDropTopic(m.sessionId, m.correlationId, local, meta);
        });
}

void ClientGateway::handleAlterTopicConfig(const uint8_t* buf, std::size_t len)
{
    handleCatalogDdl_<pc::AlterTopicConfig>(buf, len, "AlterTopicConfig not routable",
        [this](const pc::AlterTopicConfig& m, std::string_view local, const rune::gateway::DdlMetaView& meta) {
            return adminSink_->submitAlterTopicConfig(m.sessionId, m.correlationId, local,
                                                      m.expectedVersion, m.changes, meta);
        });
}

void ClientGateway::handleDropView(const uint8_t* buf, std::size_t len)
{
    handleCatalogDdl_<pc::DropView>(buf, len, "DropView not routable",
        [this](const pc::DropView& m, std::string_view local, const rune::gateway::DdlMetaView& meta) {
            return adminSink_->submitDropView(m.sessionId, m.correlationId, local, meta);
        });
}

void ClientGateway::handleAlterView(const uint8_t* buf, std::size_t len)
{
    handleCatalogDdl_<pc::AlterView>(buf, len, "AlterView not routable",
        [this](const pc::AlterView& m, std::string_view local, const rune::gateway::DdlMetaView& meta) {
            return adminSink_->submitAlterView(m.sessionId, m.correlationId, local, m.keyField,
                                               m.persistence, m.expectedVersion, m.planBlob, m.planLen, meta);
        });
}

// ---------------------------------------------------------------------------
// Authentication Task 11 (D11 / D13 / D14): the SCRAM server in the session handshake.
//
// Everything below runs on the CONTROL thread, which owns every Session's `auth` state, the LoginThrottle and the
// SecurityView it reads: no lock, no atomic, no copy of the view.  Login work is hash / HMAC only (the server never
// runs PBKDF2), so a login never stalls the duty cycle.
// ---------------------------------------------------------------------------
rune::gateway::SessionAuth ClientGateway::newSessionAuth_(int64_t nowNs, int64_t connectCorrelationId) const
{
    using namespace rune::gateway;
    if (authMode_ != GatewayAuthMode::Required) return AuthAnonymous{};
    return AuthAwaitingFirst{nowNs + authTimeoutNs_, connectCorrelationId};
}

// Issue #236: the sessions `host` holds unauthenticated - AuthAwaitingFirst / AuthAwaitingFinal sessions that are not
// Closing, plus pending Connects that will become one.  O(sessions + pending) per Connect: Connect is control-plane
// and cold (never the produce / consume / query path).
std::size_t ClientGateway::unauthSessionsOf_(std::string_view host)
{
    using namespace rune::gateway;
    std::size_t n = 0;
    sessions_.forEach([&](Session& s) {
        if (!std::holds_alternative<AuthAwaitingFirst>(s.auth) && !std::holds_alternative<AuthAwaitingFinal>(s.auth)) return;
        if (s.spill.state == SpillState::Closing) return;
        if (s.sourceHost == host) ++n;
    });
    for (const PendingConnect& p : pendingConnects_)
        if (p.refuseCode == 0 && !p.cryptoFailed && !(!p.isLeader && !p.leaderClientEndpoint.empty()) && p.sourceHost == host) ++n;
    return n;
}

std::string ClientGateway::sourceHostOf_(std::string_view egressChannel) const
{
    // The REAL UDP source of the Connect image ("1.2.3.4:5678" / "[::1]:5678"); IPC images have no host, and a test
    // gateway has no subscription: fall back to the host of the client's reply endpoint.
    if (sub_ != nullptr && currentImageSessionId_ != 0) {
        if (const auto image = sub_->imageBySessionId(currentImageSessionId_)) {
            // Image::sourceIdentity() returns a std::string BY VALUE: never bind a string_view to it (it dangled - the
            // throttle keys and the login log lines carried freed memory); hostOfSourceIdentity copies out.
            std::string host = rune::gateway::hostOfSourceIdentity(image->sourceIdentity());
            if (!host.empty()) return host;
        }
    }
    return std::string(rune::consensus::PeerHostTable::endpointHost(egressChannel));
}

bool ClientGateway::gateExempt_(const Session& s) const
{
    return !rune::gateway::authorizeCluster(permsOf_(s), rune::security::Operation::Admin).has_value();
}

uint16_t ClientGateway::suspendedCode_(const Session& s) const noexcept
{
    return static_cast<uint16_t>(s.clientVersion >= pc::kClientVersionNamespaces ? pc::ErrorCode::NamespaceSuspended
                                                                                  : pc::ErrorCode::InvalidRequest);
}

uint16_t ClientGateway::nsLimitCode_(const Session& s) const noexcept
{
    return static_cast<uint16_t>(s.clientVersion >= pc::kClientVersionNamespaces ? pc::ErrorCode::NamespaceLimitExceeded
                                                                                  : pc::ErrorCode::InvalidRequest);
}

void ClientGateway::answerSuspended_(Session& s, const pc::MessageHeader& hdr, const uint8_t* buf, std::size_t len)
{
    if (!answerRejection_(s, hdr, suspendedCode_(s), "namespace suspended", buf, len))
        noteUnanswerable_(buf, len, "namespace suspended");
}

bool ClientGateway::nsGateRefuses_(uint16_t tid, const uint8_t* buf, std::size_t len)
{
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) return false;                                // the handler answers / notes it
    Session* s = sessions_.find(hdr->sessionId);
    if (s == nullptr || s->nsGate == rune::catalog::NsStateTag::Active) return false;
    if (rune::gateway::suspendAction(tid, s->nsGate) != rune::gateway::SuspendAction::Refuse) return false;
    if (gateExempt_(*s)) return false;
    answerSuspended_(*s, *hdr, buf, len);
    return true;
}

void ClientGateway::onNsSlotAssign(uint16_t slot, uint32_t nsId, const rune::protocol::NamespaceLimits& l)
{
    if (nsSlots_.nsIdAt(slot) != nsId && !nsSlots_.place(slot, nsId)) { ++nsSlotMismatches_; return; }
    nsSlots_.setLimits(slot, l);
    if (nsConns_.size() < nsSlots_.capacity()) nsConns_.resize(nsSlots_.capacity());
    if (slot < nsConns_.size() && nsConns_[slot].perUser.empty()) nsConns_[slot].perUser.reserve(16);   // D24: preallocated at assign
}

void ClientGateway::onNsSlotRelease(uint16_t slot, uint32_t nsId)
{
    if (!nsSlots_.release(slot, nsId)) { ++nsSlotMismatches_; return; }
    if (slot < nsConns_.size()) nsConns_[slot] = NsConnSlot{};   // a reused slot starts from zero usage
    sessions_.forEach([&](Session& s) { if (s.nsSlot == slot && s.nsId == nsId) s.nsSlot = rune::node::kNoNsSlot; });
}

void ClientGateway::onNamespaceUpserted(const rune::catalog::NamespaceRecord& r)
{
    using rune::catalog::NsStateTag;
    if (nsSlots_.slotOf(r.nsId) == rune::node::kNoNsSlot) (void)nsSlots_.assign(r.nsId);   // no BrokerNode in front (unit rigs)
    if (!namespaces_.upsert(r)) return;   // an older record: nothing changed
    nsSlots_.setLimits(nsSlots_.slotOf(r.nsId), r.limits);
    if (txnService_ != nullptr) txnService_->setNamespaceTxnLimits(r.nsId, r.limits.maxOpenTxns, r.limits.maxTxnTimeMs);   // Task 10
    const NsStateTag tag = rune::catalog::tagOf(r.state);
    // Namespaces Phase 2 Task 4: on every node, at the apply of the state change.  Only the node whose transaction
    // coordinator is loaded holds transactions (others return 0), so this covers the transactions whose sessions live
    // on other gateways or are already gone.  Idempotent.
    if (tag != NsStateTag::Active && txnService_ != nullptr) (void)txnService_->abortAllForNamespace(r.nsId, nowNs_);
    nsGateScratch_.clear();
    sessions_.forEach([&](Session& s) {
        // Only sessions past the login's first leg carry a namespace; the login itself sets the gate of the rest.
        if (s.auth.index() < 2 || sessionNsId_(s) != r.nsId || s.nsGate == tag) return;
        nsGateScratch_.push_back(s.sessionId);
        s.nsGate = tag;
    });
    // Outside the forEach: revoke / bind erase or touch sessions.
    for (const int64_t sid : nsGateScratch_) {
        Session* s = sessions_.find(sid);
        if (s == nullptr || gateExempt_(*s)) continue;
        switch (tag) {
        case NsStateTag::SuspendedRejectAll:
            revokeSession_(sid, "namespace suspended", suspendedCode_(*s));
            break;
        case NsStateTag::Deleting:
            revokeSession_(sid, "namespace deleted");
            break;
        case NsStateTag::SuspendedReadOnly:
            bindDenyTable_(*s);
            break;
        case NsStateTag::Active:
            (void)shipPermissions_(*s);   // resumed (it was ReadOnly: closed sessions are gone): its real permissions again
            break;
        }
    }
}

bool ClientGateway::authGateRefuses_(pc::TemplateId tid, const uint8_t* buf, std::size_t len)
{
    using namespace rune::gateway;
    switch (tid) {   // the session level: always allowed (gateFor says so too; this skips the session lookup)
        case pc::TemplateId::Connect:
        case pc::TemplateId::HandshakeContinue:
        case pc::TemplateId::Keepalive:
        case pc::TemplateId::Ping:
        case pc::TemplateId::Disconnect:
            return false;
        // Review C I3: Produce is the hot path - it checks the auth state inside handleProduce, on the session it
        // already looked up (no second header decode / hash lookup per frame).
        case pc::TemplateId::Produce:
            return false;
        default:
            break;
    }
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) return false;                               // the handler answers / notes it
    Session* session = sessions_.find(hdr->sessionId);
    if (!session) return false;                           // the handler notes it (no session to answer)

    // Namespaces Task 9 (spec §11): with RUNE_ANONYMOUS_READ on, a session that sends a request instead of logging in
    // becomes the anonymous reader of `default` (Describe only) rather than ending.  The login legs and the bootstrap
    // keep their D13 meaning.  Off (the default): nothing changes.
    if (anonymousRead_) [[unlikely]] {
        if (becomeAnonymousIfAllowed_(*session, tid)) {   // Task 10 (#236): the anonymous-connection cap
            if (!answerRejection_(*session, *hdr, nsLimitCode_(*session), "namespace limit exceeded: anonymous connections", buf, len))
                noteUnanswerable_(buf, len, "anonymous connection cap");
            return true;
        }
        if (std::holds_alternative<AuthAnonymousRead>(session->auth) && anonOpsRefused_(*session, *hdr, buf, len)) return true;
    }

    // A one-time-password session may send exactly `ALTER USER <self> SET PASSWORD`.
    bool selfSetPassword = false;
    if (tid == pc::TemplateId::SecurityDdl)
        if (const auto* pcr = std::get_if<AuthPasswordChangeRequired>(&session->auth))
            if (const auto m = pc::SecurityDdl::decode(buf, len))
                selfSetPassword = m->kind == pc::SecurityOpKind::AlterUser &&
                                  m->change == pc::UserChangeKind::SetPassword && ownUserName_(*session, m->name, pcr->user);

    // bootstrapOpen_() walks the account list: evaluated only for the one request that can use it (never per frame).
    const AuthGate gate = gateFor(session->auth, tid, tid == pc::TemplateId::SecurityBootstrap && bootstrapOpen_(), selfSetPassword);
    if (gate == AuthGate::Allow) {
        // Issue #603: every grant-checked request waits for the view to apply the committed events this broker holds.
        // The login legs answer it themselves (securityUnavailable_: AuthServerFinal 48 + the session's end).
        if (securityBehind_ && tid != pc::TemplateId::AuthClientFirst && tid != pc::TemplateId::AuthClientFinal) [[unlikely]] {
            answerSecurityBehind_(*session, *hdr, buf, len);
            return true;
        }
        return false;
    }

    answerGate_(*session, *hdr, gate, buf, len);
    return true;
}

void ClientGateway::answerGate_(Session& sessionRef, const pc::MessageHeader& hdrRef, rune::gateway::AuthGate gate,
                                const uint8_t* buf, std::size_t len)
{
    using namespace rune::gateway;
    Session* session = &sessionRef;
    const pc::MessageHeader* hdr = &hdrRef;
    // Issue #119: a Closing session was already sent its last answer (its frames never get here; defensive).
    if (session->spill.state == SpillState::Closing) [[unlikely]] return;
    const bool end = gate == AuthGate::AnswerAuthFailedAndEnd;
    const bool anon = gate == AuthGate::AnswerNotAuthorized;   // namespaces Task 9: the anonymous reader (stays)
    const auto code = end ? pc::ErrorCode::AuthenticationFailed
                    : anon ? pc::ErrorCode::NotAuthorized : pc::ErrorCode::PasswordChangeRequired;
    ++authGateRefusals_;
    if (authGateRefusals_ % 1024 == 1)
        spdlog::warn("ClientGateway: session {} sent template {} {} - answered {}{} (D13; refusals so far: {})", hdr->sessionId,
                     hdr->templateId, end ? "before authenticating" : anon ? "anonymously" : "while it must change its password",
                     end ? "AuthenticationFailed and ended" : anon ? "NotAuthorized" : "PasswordChangeRequired", "",
                     authGateRefusals_);
    const char* why = end ? "authentication required" : anon ? "not authorized (anonymous)" : "password change required";
    if (!rejectRequest_(buf, len, static_cast<uint16_t>(code), why)) {
        // The request has no response type of its own: the session-level ErrorResponse, so the client is never left
        // waiting.
        pc::ErrorResponse er;
        er.sessionId     = hdr->sessionId;
        er.correlationId = hdr->correlationId;
        er.code          = static_cast<int16_t>(code);
        const std::size_t n = er.encode(scratch_.data());
        sendAnswer_(*session, scratch_.data(), n);
    }
    if (end) closeAfterAnswers_(hdr->sessionId);   // issue #119: its 44 is delivered first, even when spilled
}


// Issue #603 - see setSecurityBehind.  Cold: one answer per refused request.
void ClientGateway::answerSecurityBehind_(Session& session, const pc::MessageHeader& hdr, const uint8_t* buf, std::size_t len)
{
    if (session.spill.state == rune::gateway::SpillState::Closing) [[unlikely]] return;
    ++securityBehindRefusals_;
    if (securityBehindRefusals_ % 1024 == 1)
        spdlog::warn("ClientGateway: session {} sent template {} while this broker's __security view is behind committed "
                     "events it holds - answered AuthUnavailable (refusals so far: {})", hdr.sessionId, hdr.templateId,
                     securityBehindRefusals_);
    constexpr auto code = static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable);
    if (answerRejection_(session, hdr, code, "security state behind", buf, len)) return;
    pc::ErrorResponse er;   // no response type of its own: never left waiting
    er.sessionId     = hdr.sessionId;
    er.correlationId = hdr.correlationId;
    er.code          = static_cast<int16_t>(code);
    sendAnswer_(session, scratch_.data(), er.encode(scratch_.data()));
}

void ClientGateway::setSecurityBehind(bool behind)
{
    behind = behind && authMode_ == rune::gateway::GatewayAuthMode::Required;
    if (behind == securityBehind_) return;
    securityBehind_ = behind;
    spdlog::warn("ClientGateway: __security view {}", behind
        ? "BEHIND committed events this broker holds - grant-checked requests answer AuthUnavailable, nothing is pushed"
        : "applied every committed event this broker holds - grant-checked requests served again");
    (void)syncWorkersBehind_();
}

int ClientGateway::syncWorkersBehind_()
{
    if (workersBehind_ == securityBehind_) [[likely]] return 0;
    if (workerPool_ == nullptr) { workersBehind_ = securityBehind_; return 0; }
    // The clear follows the view's effects: every applied event's ops (revoked bitmaps / binds on the lifecycle ring,
    // ending Unsubscribes on the op ring) were EXECUTED by every worker (its enforcement fences acknowledged) - until
    // then a worker could still push to, or accept a produce of, a session the applied events cut off.
    if (!securityBehind_ && !pendingEnforce_.empty()) return 0;
    workersBehind_ = securityBehind_;
    (void)permissionShipper_.securityBehind(*workerPool_, workersBehind_);   // FIFO behind every queued item
    return 1;
}

// ---------------------------------------------------------------------------
// Authentication Task 12: permission tables and the enforcement helpers.  Control thread only.
// ---------------------------------------------------------------------------
const rune::gateway::PermissionTable& ClientGateway::permsOf_(const Session& s) const noexcept
{
    if (const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&s.auth)) return a->perms.table();
    if (const auto* a = std::get_if<rune::gateway::AuthAnonymousRead>(&s.auth)) return a->perms.table();   // ns Task 9
    // Dev / in-process brokers authenticate nobody: everything is allowed whatever a session's (test-set) state says.
    // On an authenticating broker only AuthAuthenticated holds grants: AuthAnonymous (the state of nobody who logged
    // in) is denied everything (X013).
    if (authMode_ != rune::gateway::GatewayAuthMode::Required) return allowAllPerms_;
    return denyAllPerms_;
}

uint32_t ClientGateway::sessionNsId_(const Session& s) noexcept
{
    if (const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&s.auth)) return a->nsId;
    if (const auto* p = std::get_if<rune::gateway::AuthPasswordChangeRequired>(&s.auth)) return p->nsId;
    return 0;   // anonymous (dev / in-process / anonymous read) and not-yet-logged-in sessions: `default` (D12)
}

bool ClientGateway::becomeAnonymousIfAllowed_(Session& session, pc::TemplateId tid)
{
    // Namespaces Phase 2 Task 9 (D23): anonymous read is also a feature of `default` (bit 0 of featureBits = unrestricted).
    const auto* d = namespaces_.registry().byId(rune::catalog::kDefaultNsId);
    if (anonymousRead_ && authMode_ == GatewayAuthMode::Required && tid != pc::TemplateId::AuthClientFirst &&
        tid != pc::TemplateId::AuthClientFinal && tid != pc::TemplateId::SecurityBootstrap &&
        std::holds_alternative<rune::gateway::AuthAwaitingFirst>(session.auth) &&
        (d == nullptr || rune::node::nsFeatureAllowed(d->limits, rune::protocol::kNsFeatureAnonymousRead))) {
        // Namespaces Phase 2 Task 10 (#236, D24): the anonymous cap applies before any namespace limit.
        if (anonSessions_ >= anonMaxConnections_) [[unlikely]] return true;
        session.auth = rune::gateway::AuthAnonymousRead{anonPerms_()};
        session.anonCounted = true;
        ++anonSessions_;
        if (d != nullptr)
            session.nsGate = rune::catalog::tagOf(d->state);   // namespaces Phase 2 Task 3: the anonymous reader is `default`'s
    }
    return false;
}

bool ClientGateway::anonOpsRefused_(Session& session, const pc::MessageHeader& hdr, const uint8_t* buf, std::size_t len)
{
    // GCRA over the injected clock: interval T = 1 s / rate, burst = one second of operations.
    const int64_t interval = std::max<int64_t>(1, 1'000'000'000LL / static_cast<int64_t>(anonOpsPerSec_));
    const int64_t tau      = static_cast<int64_t>(anonOpsPerSec_ - 1) * interval;
    const int64_t t        = std::max(anonTatNs_, nowNs_);
    if (t - nowNs_ <= tau) [[likely]] {
        anonTatNs_ = t + interval;
        return false;
    }
    const int64_t retryAfter = t - nowNs_ - tau;
    if (session.clientVersion >= pc::kClientVersionNamespaces) {
        pc::ErrorResponse er;
        er.sessionId     = hdr.sessionId;
        er.correlationId = hdr.correlationId;
        er.code          = static_cast<int16_t>(pc::ErrorCode::Throttled);
        er.retryAfterNs  = static_cast<uint64_t>(retryAfter);
        sendAnswer_(session, scratch_.data(), er.encode(scratch_.data()));
    } else if (!answerRejection_(session, hdr, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "anonymous rate limit", buf, len)) {
        noteUnanswerable_(buf, len, "anonymous rate limit");
    }
    return true;
}

ClientGateway::Admit ClientGateway::admitConnection_(Session& s, uint64_t userId, const rune::catalog::NamespaceRecord& rec, bool exempt)
{
    const uint16_t slot = nsSlots_.slotOf(rec.nsId);
    if (slot == rune::node::kNoNsSlot) return Admit::Ok;   // unreachable: the login's namespace has a slot
    if (nsConns_.size() < nsSlots_.capacity()) nsConns_.resize(nsSlots_.capacity());   // cold, once (unit rigs)
    NsConnSlot& c = nsConns_[slot];
    const uint32_t mine = [&] { const auto it = c.perUser.find(userId); return it == c.perUser.end() ? 0u : it->second; }();
    const auto& L = rec.limits;
    if (!exempt) {   // count >= limit refuses ("at -> over"); 0 = unlimited; a cluster admin must always be able to log in
        if (L.maxConnections != 0 && c.total >= L.maxConnections) return Admit::Connections;
        if (L.maxConnectionsPerUser != 0 && mine >= L.maxConnectionsPerUser) return Admit::PerUser;
        if (L.maxSessionsPerUser != 0 && mine >= L.maxSessionsPerUser) return Admit::SessionsPerUser;
    }
    ++c.total;
    ++c.perUser[userId];
    s.connCounted = true;
    s.connSlot    = slot;
    s.connNsId    = rec.nsId;
    s.connUserId  = userId;
    return Admit::Ok;
}

void ClientGateway::releaseConnection_(Session& s) noexcept
{
    if (!s.connCounted) return;
    s.connCounted = false;
    if (!nsSlots_.matches(s.connSlot, s.connNsId) || s.connSlot >= nsConns_.size()) return;   // the slot was reset / reassigned
    NsConnSlot& c = nsConns_[s.connSlot];
    if (c.total > 0) --c.total;
    if (const auto it = c.perUser.find(s.connUserId); it != c.perUser.end() && --it->second == 0) c.perUser.erase(it);
}

rune::gateway::PermissionRef ClientGateway::anonPerms_()
{
    using namespace rune::security;
    // Sorted (GrantSpec's <=>: Describe on Topic before Describe on View), as PermissionPool::acquire requires.  No
    // Cluster grant: no __catalog subscribe, no other namespace's catalog; internal objects never match (I6).
    return permPool_.acquire({GrantSpec{Operation::Describe, ResourcePattern{ResourceType::Topic, PatternKind::Prefix, ""}},
                              GrantSpec{Operation::Describe, ResourcePattern{ResourceType::View, PatternKind::Prefix, ""}}},
                             objectIndex_);
}

rune::gateway::PermissionRef ClientGateway::compilePerms_(uint64_t userId, uint32_t nsId)
{
    if (securityView_ == nullptr) return rune::gateway::PermissionRef{};   // deny-all
    // Issue #259: the effective set is computed per principal (O(its grants)); the table is compiled only when no
    // session holds that exact set already.  Namespaces Task 10: the session's namespace is part of the key - a
    // foreign object gets no bits, except for root (ADMIN ON CLUSTER keeps every op in every namespace, #626).
    // Plan D11: the session's namespace name decides whether ADMIN ON NAMESPACE makes it that namespace's admin.
    const rune::catalog::NamespaceRecord* ns = namespaces_.registry().byId(nsId);
    return permPool_.acquire(rune::security::effectiveGrants(securityView_->state(), userId), nsId, objectIndex_,
                             ns != nullptr ? std::string_view(ns->name) : std::string_view{});
}

void ClientGateway::syncPermissionTables_()
{
    if (!permPool_.hasEvents()) return;
    const bool shipping = authMode_ == rune::gateway::GatewayAuthMode::Required && shardMode_ && workerPool_;
    permPool_.drainEvents([&](rune::gateway::PermissionPool::Event ev, uint32_t id, const PermissionTable& t) {
        if (!shipping) return;
        if (ev == rune::gateway::PermissionPool::Event::Created)
            (void)permissionShipper_.shipTable(*workerPool_, id, t.writeBits(), t.isAllowAll());
        else
            (void)permissionShipper_.dropTable(*workerPool_, id);
    });
}

void ClientGateway::onCatalogObjectApplied(int32_t id, const std::string& name, bool isView)
{
    onCatalogObjectApplied(rune::catalog::kDefaultNsId, id, name, isView);
}

void ClientGateway::onCatalogObjectApplied(uint32_t nsId, int32_t id, const std::string& name, bool isView)
{
    if (isView) objectIndex_.onView(nsId, id, name); else objectIndex_.onTopic(nsId, id, name);
    const rune::gateway::ObjectEntry* e = objectIndex_.at(id);
    if (e == nullptr) return;
    // Issue #259: every LIVE TABLE once (not every session); a table whose WRITE bit of the new object changed (a
    // prefix grant now covering it, spec §6) ships that one word - never a full bitmap.
    syncPermissionTables_();   // the workers know every live table before any patch of one
    const bool shipping = authMode_ == rune::gateway::GatewayAuthMode::Required && shardMode_ && workerPool_;
    permPool_.onObjectCreated(id, *e, [&](uint32_t tid, const PermissionTable& t, bool before, bool after) {
        if (!shipping || before == after) return;
        const auto w = static_cast<std::size_t>(id) / 64;
        (void)permissionShipper_.patchTable(*workerPool_, tid, w, t.writeBits().subspan(w, 1), t.writeBits().size());
    });
    // Issue #197: the object's DDL OK must not leave before every worker ring accepted what was queued above (the
    // worker's grace hold covers an item on its ring, not one still in this queue).  queuedSeq() covers the table
    // ships and this call's patches; conservative is fine.  Idempotent: an existing entry for `id` is replaced.
    if (shipping && permissionShipper_.queuedSeq() > permissionShipper_.shippedSeq()) {
        const uint64_t need = permissionShipper_.queuedSeq();
        for (auto& en : objectShipSeqs_)
            if (en.first == id) { en.second = need; return; }
        objectShipSeqs_.emplace_back(id, need);
    }
}

void ClientGateway::onFilterSchema(int32_t topicId, uint16_t version, std::span<const uint8_t> evolvingBlob)
{
    filterSchemas_.install(topicId, version, evolvingBlob);
    if (!workerPool_) return;   // no workers: nothing serves filters (a filtered Subscribe is refused E12)
    const std::size_t nw = static_cast<std::size_t>(workerPool_->workerCount());
    if (pendingSubOps_.size() < nw) pendingSubOps_.resize(nw);
    if (!workerPool_->fitsIngressTail(evolvingBlob.size())) [[unlikely]] {
        ++filterSchemaTooLarge_;
        filterSchemaTooLargeTopics_.insert(topicId);
        spdlog::error("ClientGateway: filter schema of topicId={} ({} B) can never fit the worker ingress ring "
                      "(max tail {} B) - filtered Subscribes to it are refused; raise RUNE_WORKER_INGRESS_RING_BYTES",
                      topicId, evolvingBlob.size(), workerPool_->maxIngressTail());
        return;
    }
    filterSchemaTooLargeTopics_.erase(topicId);
    for (std::size_t w = 0; w < nw; ++w) {
        // FIFO with the Subscribe / Unsubscribe ops: behind anything already queued for this worker, so it never
        // overtakes an op queued before it, and a filtered Subscribe arriving later finds the schema first.
        if (!pendingSubOps_[w].empty()) {
            queueSubOp_(w, SubOpFilterSchema{topicId, version, std::vector<uint8_t>(evolvingBlob.begin(), evolvingBlob.end())});
            continue;
        }
        rune::node::WorkerOp op;
        op.type             = rune::node::WorkerOp::Type::FilterSchema;
        op.topicId          = topicId;
        op.clientVersion    = version;
        op.schemaUpdateBlob.assign(evolvingBlob.begin(), evolvingBlob.end());
        const auto r = workerPool_->worker(static_cast<int32_t>(w)).enqueueOp(std::move(op));
        if (r == rune::node::EnqueueResult::Full)
            queueSubOp_(w, SubOpFilterSchema{topicId, version, std::vector<uint8_t>(evolvingBlob.begin(), evolvingBlob.end())});
    }
}

void ClientGateway::onCatalogObjectDropped(int32_t id)
{
    filterSchemas_.erase(id);
    filterSchemaTooLargeTopics_.erase(id);
    objectIndex_.onDropped(id);
    std::erase_if(objectShipSeqs_, [id](const auto& e) { return e.first == id; });   // issue #197: it never serves
    syncPermissionTables_();
    const bool shipping = authMode_ == rune::gateway::GatewayAuthMode::Required && shardMode_ && workerPool_;
    permPool_.onObjectDropped(id, [&](uint32_t tid, const PermissionTable& t, bool before, bool) {
        if (!shipping || !before) return;
        const auto w = static_cast<std::size_t>(id) / 64;
        if (w < t.writeBits().size())
            (void)permissionShipper_.patchTable(*workerPool_, tid, w, t.writeBits().subspan(w, 1), t.writeBits().size());
    });
}

rune::gateway::Denial ClientGateway::refusedProducerIdDenial_(int64_t producerId)
{
    return rune::gateway::Denial{static_cast<uint16_t>(pc::ErrorCode::NotAuthorized),
                                 "not authorized: WRITE on PRODUCER_ID " + std::to_string(producerId)};
}

void ClientGateway::noteDenied_(const Session& session, pc::TemplateId tid, const rune::gateway::Denial& denial)
{
    ++notAuthorizedAnswers_;
    if (notAuthorizedAnswers_ % 1024 == 1) {
        const char* user = "<anonymous>";
        if (const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&session.auth)) user = a->user.c_str();
        spdlog::warn("auth: request refused user={} session={} template={}: {} (refusals so far: {})",
                     LogSafeName{user}, session.sessionId,
                     static_cast<uint16_t>(tid), denial.detail, notAuthorizedAnswers_);
    }
}

std::optional<rune::gateway::Denial> ClientGateway::authorizeTopicIdHere_(const rune::gateway::PermissionTable& perms,
                                                                       int32_t topicId, int32_t partition,
                                                                       rune::security::Operation op,
                                                                       rune::security::ResourceType shownType) const
{
    auto d = rune::gateway::authorizeTopicId(perms, topicId, op, objectIndex_, shownType);
    if (!d || objectIndex_.at(topicId) != nullptr) return d;
    if (partition >= rune::metadata::kMaxGpidPartitions) return d;   // never placed: would alias the next id's gpid
    // #511: a view sink is placed but never indexed as a topic - it is known here, not waiting to be applied.
    if (topicRegistry_ != nullptr && topicRegistry_->isViewSink(topicId)) return d;
    if (unappliedTopicCode_(topicId, partition < 0 ? 0 : partition) == pc::ErrorCode::NotLeader)
        d->code = static_cast<uint16_t>(pc::ErrorCode::NotLeader);
    return d;
}

bool ClientGateway::deny_(Session& session, const std::optional<rune::gateway::Denial>& denial, const uint8_t* buf, std::size_t len)
{
    if (!denial) return false;
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (denial->code != static_cast<uint16_t>(pc::ErrorCode::NotAuthorized)) {   // #127: a retriable verdict, not a refusal
        if (hdr && answerRejection_(session, *hdr, denial->code, "topic not applied here yet", buf, len)) return true;
        noteUnanswerable_(buf, len, "topic not applied here yet");
        return true;
    }
    noteDenied_(session, static_cast<pc::TemplateId>(hdr ? hdr->templateId : 0), *denial);
    if (!hdr) return true;   // the caller decoded the body, so this cannot happen; nothing to answer on
    // The DDL family carries the D23 detail text; every other response type has no free-text field.
    switch (static_cast<pc::TemplateId>(hdr->templateId)) {
        case pc::TemplateId::DropTopic:
        case pc::TemplateId::AlterTopicConfig:
        case pc::TemplateId::DropView:
        case pc::TemplateId::AlterView:
        case pc::TemplateId::SecurityDdl:
        case pc::TemplateId::SecurityBootstrap:
        case pc::TemplateId::NamespaceDdl: {
            pc::DdlResponse r;
            r.sessionId = hdr->sessionId; r.correlationId = hdr->correlationId;
            r.errorCode = denial->code; r.detail = denial->detail;
            if (r.fits() && r.encodedSize() <= scratch_.size()) {
                sendAnswer_(session, scratch_.data(), r.encode(scratch_.data()));
                return true;
            }
            break;
        }
        default: break;
    }
    if (!answerRejection_(session, *hdr, denial->code, "not authorized", buf, len)) {
        pc::ErrorResponse er;
        er.sessionId = hdr->sessionId; er.correlationId = hdr->correlationId;
        er.code = static_cast<int16_t>(denial->code); er.message = denial->detail;
        if (pc::MessageHeader::ENCODED_SIZE + pc::ErrorResponse::BODY_FIXED + 2 + er.message.size() <= scratch_.size())
            sendAnswer_(session, scratch_.data(), er.encode(scratch_.data()));
    }
    return true;
}

namespace {
std::string_view sv(std::span<const uint8_t> b) noexcept { return {reinterpret_cast<const char*>(b.data()), b.size()}; }
}

/// #126 - see handleGroupMessage.  Rewrites the OffsetFetchResponse in scratch_ without the results of topics the
/// session cannot READ; returns the new encoded size (unchanged when nothing applies).
std::size_t ClientGateway::filterFetchAllByRead_(const Session& session, const uint8_t* reqBuf, std::size_t reqLen,
                                                 std::size_t encoded)
{
    const PermissionTable& perms = permsOf_(session);
    if (perms.isAllowAll()) return encoded;
    const auto req = pc::OffsetFetch::decode(reqBuf, reqLen);
    if (!req || !req->entries.empty()) return encoded;   // only a fetch-all; a listed entry was authorized up front
    const auto resp = pc::OffsetFetchResponse::decode(scratch_.data(), encoded);
    if (!resp) return encoded;
    if (fetchFilterScratch_.capacity() == 0) fetchFilterScratch_.reserve(kScratchSize / 18 + 1);   // once
    fetchFilterScratch_.clear();
    bool dropped = false;
    for (const auto res : resp->results) {
        if (res.topicId >= 0 &&
            rune::gateway::authorizeTopicId(perms, res.topicId, rune::security::Operation::Read, objectIndex_)) {
            dropped = true;
            continue;
        }
        fetchFilterScratch_.push_back(pc::OffsetFetchResult{res.topicId, res.partition, res.offset, res.errorCode});
    }
    if (!dropped) return encoded;
    pc::OffsetFetchResponse out;
    out.sessionId     = resp->sessionId;
    out.correlationId = resp->correlationId;
    return out.encode(scratch_.data(), std::span<const pc::OffsetFetchResult>(fetchFilterScratch_));
}

bool ClientGateway::denyGroupRequest_(Session& session, uint16_t tid, const uint8_t* buf, std::size_t len)
{
    namespace rsec = rune::security;
    using rune::gateway::authorizeName;
    using rune::gateway::authorizeTopicId;
    const PermissionTable& perms = permsOf_(session);
    if (perms.isAllowAll()) return false;
    const auto group = [&](std::span<const uint8_t> id) {
        return authorizeName(perms, rsec::ResourceType::Group, sv(id), rsec::Operation::Read);
    };
    const auto topic = [&](int32_t id, int32_t partition) {
        return authorizeTopicIdHere_(perms, id, partition, rsec::Operation::Read);
    };
    std::optional<rune::gateway::Denial> d;
    switch (static_cast<pc::TemplateId>(tid)) {
        case pc::TemplateId::JoinGroup:
            if (const auto m = pc::JoinGroup::decode(buf, len, session.clientVersion)) {
                d = group(m->groupId);
                for (const int32_t t : m->subscribedTopicIds) { if (d) break; d = topic(t, 0); }
            }
            break;
        case pc::TemplateId::SyncGroup:
            if (const auto m = pc::SyncGroup::decode(buf, len)) d = group(m->groupId);
            break;
        case pc::TemplateId::Heartbeat:
            if (const auto m = pc::Heartbeat::decode(buf, len)) d = group(m->groupId);
            break;
        case pc::TemplateId::OffsetCommit:
            if (const auto m = pc::OffsetCommit::decode(buf, len)) {
                d = group(m->groupId);
                for (const auto& e : m->entries) { if (d) break; d = topic(e.topicId, e.partition); }
            }
            break;
        case pc::TemplateId::OffsetFetch:
            if (const auto m = pc::OffsetFetch::decode(buf, len)) {
                d = group(m->groupId);
                for (const auto& e : m->entries) { if (d) break; d = topic(e.topicId, e.partition); }
            }
            break;
        case pc::TemplateId::LeaveGroup:   // issue #123: READ on the group, like every other group request
            if (const auto m = pc::LeaveGroup::decode(buf, len)) d = group(m->groupId);
            break;
        default: break;   // an undecodable body is the service's InvalidRequest
    }
    return deny_(session, d, buf, len);
}

bool ClientGateway::readmitParkedTxnRequest(int64_t sessionId, uint16_t tid, const uint8_t* buf, std::size_t len)
{
    Session* s = sessions_.find(sessionId);
    if (s == nullptr) return false;                                                 // closed while parked
    if (authGateRefuses_(static_cast<pc::TemplateId>(tid), buf, len)) return false;   // answered by the gate (#603 too)
    s = sessions_.find(sessionId);                                                  // the gate may have ended it
    return s != nullptr && !denyTxnRequest_(*s, tid, buf, len);                     // 45 when a grant went meanwhile
}

bool ClientGateway::denyTxnRequest_(Session& session, uint16_t tid, const uint8_t* buf, std::size_t len)
{
    namespace rsec = rune::security;
    using rune::gateway::authorizeName;
    using rune::gateway::authorizeTopicId;
    const PermissionTable& perms = permsOf_(session);
    if (perms.isAllowAll()) return false;
    const auto txn = [&](std::span<const uint8_t> id) {
        return authorizeName(perms, rsec::ResourceType::TransactionalId, sv(id), rsec::Operation::Write);
    };
    const auto group = [&](std::span<const uint8_t> id) {
        return authorizeName(perms, rsec::ResourceType::Group, sv(id), rsec::Operation::Read);
    };
    std::optional<rune::gateway::Denial> d;
    switch (static_cast<pc::TemplateId>(tid)) {
        case pc::TemplateId::InitProducerId:
            if (const auto m = pc::InitProducerId::decode(buf, len)) {
                if (!m->transactionalId.empty()) d = txn(m->transactionalId);
                // #211 (Kafka's rule): without one, WRITE on at least one topic - a zero-grant principal mints no ids.
                else d = rune::gateway::authorizeAnyGrant(perms, rsec::ResourceType::Topic, rsec::Operation::Write);
            }
            break;
        case pc::TemplateId::AddPartitionsToTxn:
            if (const auto m = pc::AddPartitionsToTxn::decode(buf, len)) {
                d = txn(m->transactionalId);
                for (const auto& p : m->partitions) {
                    if (d) break;
                    d = authorizeTopicIdHere_(perms, p.topicId, p.partition, rsec::Operation::Write);
                }
            }
            break;
        case pc::TemplateId::AddOffsetsToTxn:
            if (const auto m = pc::AddOffsetsToTxn::decode(buf, len)) { d = txn(m->transactionalId); if (!d) d = group(m->groupId); }
            break;
        case pc::TemplateId::TxnOffsetCommit:
            // Issue #125: READ on every listed topic too, exactly as OffsetCommit (an offset is a read position).
            if (const auto m = pc::TxnOffsetCommit::decode(buf, len)) {
                d = txn(m->transactionalId);
                if (!d) d = group(m->groupId);
                for (const auto& e : m->entries) {
                    if (d) break;
                    d = authorizeTopicIdHere_(perms, e.topicId, e.partition, rsec::Operation::Read);
                }
            }
            break;
        case pc::TemplateId::EndTxn:
            if (const auto m = pc::EndTxn::decode(buf, len)) d = txn(m->transactionalId);
            break;
        default: break;
    }
    return deny_(session, d, buf, len);
}

std::optional<rune::gateway::Denial> ClientGateway::authorizeViewSources_(const Session& session, const uint8_t* blob, std::size_t len) const
{
    const PermissionTable& perms = permsOf_(session);
    if (perms.isAllowAll()) return std::nullopt;
    // An undecodable blob is the admin thread's InvalidSchema (nothing is created); only a decodable plan names sources.
    const auto ids = rune::view::planSourceTopicIds(std::span<const uint8_t>(blob, len));
    if (!ids) return std::nullopt;
    for (const int32_t id : *ids)
        if (auto d = rune::gateway::authorizeViewSource(perms, id)) return d;   // no D22 pass-through for sources
    return std::nullopt;
}

template <class Msg>
std::optional<rune::gateway::Denial> ClientGateway::authorizeCatalogDdl_(const Session& session, const Msg& m,
                                                                         std::string_view local) const
{
    namespace rsec = rune::security;
    using rune::gateway::authorizeName;
    const PermissionTable& perms = permsOf_(session);
    if constexpr (std::is_same_v<Msg, pc::DropTopic>)
        return authorizeName(perms, rsec::ResourceType::Topic, local, rsec::Operation::Drop);
    else if constexpr (std::is_same_v<Msg, pc::AlterTopicConfig>)
        return authorizeName(perms, rsec::ResourceType::Topic, local, rsec::Operation::Alter);
    else if constexpr (std::is_same_v<Msg, pc::DropView>)
        return authorizeName(perms, rsec::ResourceType::View, local, rsec::Operation::Drop);
    else {
        static_assert(std::is_same_v<Msg, pc::AlterView>);
        if (auto d = authorizeName(perms, rsec::ResourceType::View, local, rsec::Operation::Alter)) return d;
        return authorizeViewSources_(session, m.planBlob, m.planLen);
    }
}

void ClientGateway::failLogin_(Session& session, int64_t correlationId, uint32_t retryAfterMs)
{
    if (session.spill.state == SpillState::Closing) [[unlikely]] return;   // issue #119: its last answer is queued
    pc::AuthServerFinal f;
    f.sessionId     = session.sessionId;
    f.correlationId = correlationId;
    f.errorCode     = static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed);
    f.retryAfterMs  = retryAfterMs;
    f.detail        = "authentication failed";   // the ONLY failure text (Review Focus 1)
    const std::size_t n = f.encode(scratch_.data());
    sendAnswer_(session, scratch_.data(), n);
    closeAfterAnswers_(session.sessionId);   // issue #119: the 44 (and its retryAfterMs) is delivered first
}

void ClientGateway::unavailableLogin_(Session& session, int64_t correlationId, std::string_view user)
{
    if (session.spill.state == SpillState::Closing) [[unlikely]] return;   // issue #119: its last answer is queued
    ++loginsUnavailable_;
    if (unauthLog_.due(7)) [[unlikely]]
        spdlog::warn("auth: login refused user={} source={} session={} reason=security-not-current (AuthUnavailable; "
                     "refused so far: {})", LogSafeName{user}, LogSafeName{session.sourceHost}, session.sessionId,
                     loginsUnavailable_);
    pc::AuthServerFinal f;
    f.sessionId     = session.sessionId;
    f.correlationId = correlationId;
    f.errorCode     = static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable);
    f.detail        = "this broker's security state is not current - retry on another broker";
    const std::size_t n = f.encode(scratch_.data());
    sendAnswer_(session, scratch_.data(), n);
    closeAfterAnswers_(session.sessionId);   // issue #119: the 48 is delivered first
}

void ClientGateway::endSessionsSecurityUnavailable()
{
    using namespace rune::gateway;
    liveScratch_.clear();
    sessions_.forEach([&](Session& s) {
        if (std::holds_alternative<AuthAuthenticated>(s.auth) || std::holds_alternative<AuthPasswordChangeRequired>(s.auth))
            liveScratch_.push_back(s.sessionId);
    });
    for (const int64_t sid : liveScratch_) {
        Session* s = sessions_.find(sid);
        if (s == nullptr) continue;
        if (s->spill.state == SpillState::Closing) continue;   // issue #119: already sent its last answer
        endSessionUnavailable_(*s, "this broker's security state is stale - reconnect to another broker");
    }
    if (!liveScratch_.empty())
        spdlog::warn("auth: {} authenticated session(s) ended: this broker's security state stayed stale", liveScratch_.size());
}

void ClientGateway::endSessionUnavailable_(Session& s, std::string_view reason)
{
    ++sessionsEndedUnavailable_;
    // Session level (corr 0): every in-flight request of the session ends with it; the client reconnects elsewhere.
    sendErrorOn_(s, 0, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable), reason);
    closeAfterAnswers_(s.sessionId);   // issue #119: the AuthUnavailable is delivered first
}

void ClientGateway::handleAuthClientFirst_(const uint8_t* buf, std::size_t len)
{
    using namespace rune::gateway;
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) { noteUnanswerable_(buf, len, "AuthClientFirst: no decodable header"); return; }
    Session* session = sessions_.find(hdr->sessionId);
    if (!session) { noteUnanswerable_(buf, len, "AuthClientFirst for unknown session"); return; }
    const auto* awaiting = std::get_if<AuthAwaitingFirst>(&session->auth);
    if (awaiting == nullptr) {
        // Authenticated / anonymous (the gate let it pass): a login makes no sense here.  Answered, the session stays.
        pc::AuthServerFinal f;
        f.sessionId = hdr->sessionId; f.correlationId = hdr->correlationId;
        f.errorCode = static_cast<uint16_t>(pc::ErrorCode::InvalidRequest);
        f.detail    = "session does not accept a login";
        sendAnswer_(*session, scratch_.data(), f.encode(scratch_.data()));
        return;
    }
    const int64_t deadline = awaiting->deadlineNs;
    const auto m = decodeClientFirst(buf, len);
    if (!m || m->mechanism != pc::kMechanismScramSha512) {
        ++loginsFailed_;
        if (unauthLog_.due(6)) [[unlikely]]
            spdlog::warn("auth: login failed user=<undecodable> source={} session={} reason=malformed",
                         LogSafeName{session->sourceHost}, session->sessionId);
        failLogin_(*session, hdr->correlationId, 0);
        return;
    }
    const std::string user(m->username);
    const rune::gateway::LoginThrottle::Verdict v = loginThrottle_.check(user, session->sourceHost, nowNs_);
    if (!v.allowed) {
        ++loginsRateLimited_;
        if (unauthLog_.due(1)) [[unlikely]]
            spdlog::warn("auth: rate limited user={} source={} retry_after_ms={}",
                         LogSafeName{user}, LogSafeName{session->sourceHost}, v.retryAfterMs);
        failLogin_(*session, m->correlationId, v.retryAfterMs);
        return;
    }
    if (securityView_ == nullptr || securityView_->halted()) {   // #206: not a verdict - retriable (fails closed all the same)
        if (unauthLog_.due(2)) [[unlikely]]
            spdlog::warn("auth: login failed user={} source={} session={} reason={}",
                         LogSafeName{user}, LogSafeName{session->sourceHost}, session->sessionId,
                         loginFailureName(LoginFailure::SecurityHalted));
        unavailableLogin_(*session, m->correlationId, user);
        return;
    }
    if (securityUnavailable_()) [[unlikely]] {   // issues #15 / #111: never verified against a stale view
        unavailableLogin_(*session, m->correlationId, user);
        return;
    }
    ServerFirstPlan plan = scramServerFirst(securityView_->state(), *m, deadline, securityView_->fakeIterations(),
                                            &namespaces_.registry());   // namespaces Task 9: `ns/user` logins
    if (plan.frame.empty()) {   // no randomness for the nonce: fail closed
        ++loginsFailed_;
        spdlog::error("auth: login failed user={} source={} session={} reason=no-randomness",
                      LogSafeName{user}, LogSafeName{session->sourceHost}, session->sessionId);
        failLogin_(*session, m->correlationId, 0);
        return;
    }
    plan.next.viewAppliedAtFirst = securityView_->appliedOffset();   // #205: what the verifiers were copied from
    plan.next.viewBehindAtFirst  = securityViewBehind_();
    session->auth = std::move(plan.next);
    sendAnswer_(*session, plan.frame.data(), plan.frame.size());
}

void ClientGateway::handleAuthClientFinal_(const uint8_t* buf, std::size_t len)
{
    using namespace rune::gateway;
    const auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) { noteUnanswerable_(buf, len, "AuthClientFinal: no decodable header"); return; }
    Session* session = sessions_.find(hdr->sessionId);
    if (!session) { noteUnanswerable_(buf, len, "AuthClientFinal for unknown session"); return; }
    auto* const st = std::get_if<AuthAwaitingFinal>(&session->auth);
    if (st == nullptr) {
        pc::AuthServerFinal f;
        f.sessionId = hdr->sessionId; f.correlationId = hdr->correlationId;
        f.errorCode = static_cast<uint16_t>(pc::ErrorCode::InvalidRequest);
        f.detail    = "session does not accept a login";
        sendAnswer_(*session, scratch_.data(), f.encode(scratch_.data()));
        return;
    }
    const std::string user = st->user;
    // Issue #130: the throttle gates every proof VERIFICATION, not only the first leg - otherwise N sessions that all
    // sent AuthClientFirst before any final get N verified guesses per delay.  The control thread handles the finals
    // one at a time, so each one sees the failure the previous one recorded.  Refused here: the proof is never looked
    // at and the final is no verdict - so finalLegSeen stays false and the close below charges the episode's (allowed,
    // salt-revealing) first leg as abandoned, once (PR #522 review: otherwise N parked first legs cost 3 failures).
    if (const LoginThrottle::Verdict v = loginThrottle_.check(user, session->sourceHost, nowNs_); !v.allowed) {
        ++loginsRateLimited_;
        if (unauthLog_.due(1)) [[unlikely]]
            spdlog::warn("auth: rate limited user={} source={} retry_after_ms={}",
                         LogSafeName{user}, LogSafeName{session->sourceHost}, v.retryAfterMs);
        failLogin_(*session, hdr->correlationId, v.retryAfterMs);
        return;
    }
    // #132: from here the episode is settled at this final - a verdict charges it, a stale view (retriable, not the
    // client's doing) does not - never again as an abandoned first leg at the close.
    st->finalLegSeen = true;
    if (securityUnavailable_()) [[unlikely]] {   // issues #15 / #111: the view went stale between the two legs
        unavailableLogin_(*session, hdr->correlationId, user);
        return;
    }
    if (securityView_ == nullptr || securityView_->halted()) [[unlikely]] {   // #206: not a verdict - retriable
        if (unauthLog_.due(8 + static_cast<std::size_t>(LoginFailure::SecurityHalted))) [[unlikely]]
            spdlog::warn("auth: login failed user={} source={} session={} reason={}",
                         LogSafeName{user}, LogSafeName{session->sourceHost},
                         session->sessionId, loginFailureName(LoginFailure::SecurityHalted));
        unavailableLogin_(*session, hdr->correlationId, user);   // #132: the throttle is not charged
        return;
    }
    const auto fin = pc::AuthClientFinal::decode(buf, len);
    std::variant<LoginOk, LoginFailure> verdict = LoginFailure::BadProof;
    if (nowNs_ > st->deadlineNs) verdict = LoginFailure::Timeout;
    else if (fin)                verdict = scramServerFinal(securityView_->state(), *st, *fin);

    if (const LoginFailure* failure = std::get_if<LoginFailure>(&verdict)) {
        ++loginsFailed_;
        loginThrottle_.onFailure(user, session->sourceHost, nowNs_);
        if (unauthLog_.due(8 + static_cast<std::size_t>(*failure))) [[unlikely]]   // per reason
            spdlog::warn("auth: login failed user={} source={} session={} reason={}",
                         LogSafeName{user}, LogSafeName{session->sourceHost}, session->sessionId,
                         loginFailureName(*failure));
        // #205: a proof that fails against a view behind events this broker already holds (a just-created user, a
        // just-changed password, a just-enabled user) is not a verdict yet - retriable, never the terminal 44.  Still
        // counted and throttled.  The verifiers were copied at the FIRST leg (PR #516 review): a first leg on a lagging
        // view, or a view that applied an event between the legs, made the proof fail against a superseded state too.
        // (These three reasons are only reached with a live, unhalted view.)
        if ((*failure == LoginFailure::BadProof || *failure == LoginFailure::UnknownUser || *failure == LoginFailure::Disabled) &&
            (securityViewBehind_() || st->viewBehindAtFirst || st->viewAppliedAtFirst != securityView_->appliedOffset()))
            [[unlikely]] {
            unavailableLogin_(*session, hdr->correlationId, user);
            return;
        }
        failLogin_(*session, hdr->correlationId, 0);
        return;
    }
    const LoginOk& ok = std::get<LoginOk>(verdict);
    // Namespaces Phase 2 Task 3 (D21): the verdict is computed first (a wrong proof already got its 44 above), so a
    // suspended or deleted namespace leaks nothing to someone who does not hold the password.  A namespace that is
    // gone or being deleted is indistinguishable from a bad password; a RejectAll one answers 51 (44 below client
    // version 14) to every user but a cluster admin.  Neither charges the throttle.
    const rune::catalog::NamespaceRecord* const nsRec = namespaces_.registry().byId(ok.nsId);
    const rune::catalog::NsStateTag nsTag = nsRec != nullptr ? rune::catalog::tagOf(nsRec->state) : rune::catalog::NsStateTag::Deleting;
    if (nsTag == rune::catalog::NsStateTag::Deleting) [[unlikely]] {
        failLogin_(*session, hdr->correlationId, 0);
        return;
    }
    if (nsTag == rune::catalog::NsStateTag::SuspendedRejectAll) [[unlikely]] {
        const PermissionRef loginPerms = compilePerms_(ok.userId, ok.nsId);
        if (authorizeCluster(loginPerms.table(), rune::security::Operation::Admin).has_value()) {   // not a cluster admin
            if (session->spill.state == SpillState::Closing) return;
            pc::AuthServerFinal f;
            f.sessionId     = session->sessionId;
            f.correlationId = hdr->correlationId;
            f.errorCode     = suspendedCode_(*session) == static_cast<uint16_t>(pc::ErrorCode::NamespaceSuspended)
                                  ? static_cast<uint16_t>(pc::ErrorCode::NamespaceSuspended)
                                  : static_cast<uint16_t>(pc::ErrorCode::AuthenticationFailed);
            f.detail        = "namespace suspended";
            sendAnswer_(*session, scratch_.data(), f.encode(scratch_.data()));
            closeAfterAnswers_(session->sessionId);
            return;
        }
    }
    // Namespaces Phase 2 Task 9/10 (D23, D24): the AuthScram feature and the connection limits gate a login AFTER the
    // proof verified (nothing leaks to a guesser); a cluster admin is exempt (it must be able to log in to fix a limit).
    {
        const auto refuseLogin = [&](const char* detail) {
            if (session->spill.state == SpillState::Closing) return;
            pc::AuthServerFinal f;
            f.sessionId     = session->sessionId;
            f.correlationId = hdr->correlationId;
            f.errorCode     = static_cast<uint16_t>(session->clientVersion >= pc::kClientVersionNamespaces
                                                        ? pc::ErrorCode::NamespaceLimitExceeded
                                                        : pc::ErrorCode::AuthenticationFailed);
            f.detail        = detail;
            sendAnswer_(*session, scratch_.data(), f.encode(scratch_.data()));
            closeAfterAnswers_(session->sessionId);
        };
        const bool clusterAdmin = !authorizeCluster(compilePerms_(ok.userId, ok.nsId).table(), rune::security::Operation::Admin).has_value();
        if (nsRec != nullptr && !clusterAdmin && !rune::node::nsFeatureAllowed(nsRec->limits, rune::protocol::kNsFeatureAuthScram)) [[unlikely]] {
            refuseLogin("namespace limit exceeded: password login is not enabled for this namespace");
            return;
        }
        if (nsRec != nullptr && !session->connCounted) {
            switch (admitConnection_(*session, ok.userId, *nsRec, clusterAdmin)) {
            case Admit::Ok: break;
            case Admit::Connections:     refuseLogin("namespace limit exceeded: connections"); return;
            case Admit::PerUser:         refuseLogin("namespace limit exceeded: connections per user"); return;
            case Admit::SessionsPerUser: refuseLogin("namespace limit exceeded: sessions per user"); return;
            }
        }
    }
    std::string loginNonce = st->combinedNonce;   // issue #114: before session->auth (which owns *st) is replaced
    ++loginsOk_;
    loginThrottle_.onSuccess(user, session->sourceHost, nowNs_);
    spdlog::info("auth: login ok user={} source={} session={}",
                 LogSafeName{ok.user}, LogSafeName{session->sourceHost}, session->sessionId);
    if (!session->crypto.encrypted)   // only under ClientTransport::PlaintextForTest (M1, issue #10) - said once per session
        spdlog::warn("auth: session {} authenticated over plaintext (no TLS): frames are not integrity-protected",
                     session->sessionId);
    if (ok.mustChangePassword) session->auth = AuthPasswordChangeRequired{ok.userId, ok.user, std::move(loginNonce), ok.nsId};
    else session->auth = AuthAuthenticated{ok.userId, ok.user, compilePerms_(ok.userId, ok.nsId), std::move(loginNonce), ok.nsId};
    session->nsGate = nsTag;
    session->nsId   = ok.nsId;
    session->nsSlot = nsSlots_.slotOf(ok.nsId);   // namespaces Phase 2 Task 8: cached for the hot path
    // Authentication Task 13: announce-before-accept for permissions - the workers learn this session's WRITE bitmap
    // BEFORE the client learns it is logged in, so it can never produce ahead of its grants.
    const uint64_t shipEpoch = shipPermissions_(*session);

    pc::AuthServerFinal f;
    f.sessionId       = hdr->sessionId;
    f.correlationId   = hdr->correlationId;
    f.errorCode       = 0;
    f.flags           = ok.mustChangePassword ? pc::kAuthFlagMustChangePassword : uint8_t{0};
    f.serverSignature = std::string_view(reinterpret_cast<const char*>(ok.serverSignature.data()), ok.serverSignature.size());
    const std::size_t finalLen = f.encode(scratch_.data());
    if (shipEpoch != 0 && permissionShipper_.shippedSeq() < shipEpoch) {
        // A worker's lifecycle ring is full: hold the OK (cold, login rate) until every worker accepted the bitmap.
        heldLoginFinals_.push_back({session->sessionId, shipEpoch,
                                    std::vector<uint8_t>(scratch_.data(), scratch_.data() + finalLen)});
        return;
    }
    sendAnswer_(*session, scratch_.data(), finalLen);
}

uint64_t ClientGateway::shipPermissions_(const Session& session)
{
    if (authMode_ != rune::gateway::GatewayAuthMode::Required || !shardMode_ || !workerPool_) return 0;
    const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&session.auth);
    if (a == nullptr) return 0;   // must-change-password / anonymous: the worker slot stays empty (deny)
    syncPermissionTables_();   // a new table's content goes first: a bind never names a table the workers lack
    const uint64_t epoch = ++permEpoch_;
    const uint32_t id = a->perms.id();
    return permissionShipper_.bind(*workerPool_, session.sessionId,
                                   id == rune::gateway::PermissionRef::kNoId ? rune::node::PermissionShipper::kDenyTable
                                                                             : static_cast<int64_t>(id),
                                   epoch, a->userId);   // #122: producer ids are checked against the principal
}

int ClientGateway::servicePermissionShippingWork_()
{
    int work = 0;
    syncPermissionTables_();   // issue #259: a freed table's drop (after every rebind away from it)
    if (!workerPool_) return 0;
    work += syncWorkersBehind_();   // #603: in every worker mode (worker-pushed subscriptions pause too)
    if (permissionShipper_.pendingUpdates() != 0) work += permissionShipper_.retry(*workerPool_);
    if (!shardMode_) return work;
    if (!objectShipSeqs_.empty()) {   // issue #197: bounded even when an OK is never offered
        const uint64_t shipped = permissionShipper_.shippedSeq();
        std::erase_if(objectShipSeqs_, [shipped](const auto& e) { return e.second <= shipped; });
    }
    for (std::size_t i = 0; i < heldLoginFinals_.size(); ) {
        HeldLoginFinal& h = heldLoginFinals_[i];
        if (permissionShipper_.shippedSeq() < h.epoch) { ++i; continue; }
        if (Session* s = sessions_.find(h.sessionId)) sendAnswer_(*s, h.frame.data(), h.frame.size());
        heldLoginFinals_.erase(heldLoginFinals_.begin() + static_cast<std::ptrdiff_t>(i));
        ++work;
    }
    return work;
}

int ClientGateway::servicePermissionShipping_()
{
    // Issues #109 / #196: the D24 gate is released by the fence acks (onEnforceFenceAck_), never by the shipper's
    // queue draining - a chunk on a worker's ring is not installed yet.
    return servicePermissionShippingWork_();
}


void ClientGateway::reapAuthDeadlines_(int64_t nowNs)
{
    using namespace rune::gateway;
    if (authMode_ != GatewayAuthMode::Required) return;
    authReapScratch_.clear();
    sessions_.forEach([&](Session& s) {
        // PR #525 review: a Closing session already failed its login (its 44 is owed): never failed - and counted - again.
        if (s.spill.state == SpillState::Closing) [[unlikely]] return;
        int64_t deadline = INT64_MAX;
        if (const auto* a = std::get_if<AuthAwaitingFirst>(&s.auth)) deadline = a->deadlineNs;
        else if (const auto* b = std::get_if<AuthAwaitingFinal>(&s.auth)) deadline = b->deadlineNs;
        if (nowNs > deadline) authReapScratch_.push_back(s.sessionId);
    });
    for (const int64_t sid : authReapScratch_) {
        Session* s = sessions_.find(sid);
        if (s == nullptr) continue;
        int64_t corr = 0;
        std::string user = "<none>";
        if (const auto* a = std::get_if<AuthAwaitingFirst>(&s->auth)) corr = a->correlationId;
        else if (const auto* b = std::get_if<AuthAwaitingFinal>(&s->auth)) { corr = b->correlationId; user = b->user; }
        ++loginsFailed_;
        if (unauthLog_.due(8 + static_cast<std::size_t>(LoginFailure::Timeout))) [[unlikely]]
            spdlog::warn("auth: login failed user={} source={} session={} reason={}",
                         LogSafeName{user}, LogSafeName{s->sourceHost}, sid,
                         loginFailureName(LoginFailure::Timeout));
        failLogin_(*s, corr, 0);
    }
}

// ---------------------------------------------------------------------------
// Authentication Task 14 (spec §6, D24-D26, D28): live security changes.  Control thread; cold (one security event).
// ---------------------------------------------------------------------------
// Issues #109 / #196: which worker rings a security event's enforcement must be fenced on.  The lifecycle ring
// carries the WRITE bitmaps / binds (shipped only in shard mode under authentication); the op ring the ending
// Unsubscribes of worker-pushed subscriptions (per-worker egress).
bool ClientGateway::fencesLifecycleRing_() const noexcept
{
    return workerPool_ != nullptr && shardMode_ && authMode_ == rune::gateway::GatewayAuthMode::Required;
}

bool ClientGateway::fencesOpRing_() const noexcept
{
    return workerPool_ != nullptr && perWorkerEgressActive_;
}

void ClientGateway::noteEnforced_(int64_t offset, uint64_t opWorkers, bool lifecycle)
{
    // Enforced = every table refreshed here (synchronous, done by the caller) AND every worker executed what this
    // event queued for it: a fence goes behind the event's ops on each ring that got some - the lifecycle ring of every
    // worker when it shipped permission items, the op ring of each worker an Unsubscribe went to - and the event is
    // enforced once those workers acknowledged it (onEnforceFenceAck_).  An event that queued nothing is enforced now
    // (behind any still-fenced earlier one: the enforced offset only moves forward, in event order).
    const bool life = lifecycle && fencesLifecycleRing_();
    const uint64_t ops = fencesOpRing_() ? opWorkers : 0;
    if (!life && ops == 0) {
        if (pendingEnforce_.empty()) securityEnforcedOffset_ = std::max(securityEnforcedOffset_, offset);
        else pendingEnforce_.push_back({offset, 0, 0, false});
        return;
    }
    const uint64_t seq = ++enforceFenceSeq_;
    if (life) (void)permissionShipper_.fence(*workerPool_, seq);   // FIFO behind every bitmap / bind queued so far
    const int32_t n = std::min<int32_t>(workerPool_->workerCount(), kMaxFenceWorkers);
    for (int32_t w = 0; w < n; ++w) {
        if ((ops & (uint64_t{1} << w)) == 0) continue;
        PendingSubOp p = SubOpFence{seq};
        const auto wi = static_cast<std::size_t>(w);
        const bool behind = wi < pendingSubOps_.size() && !pendingSubOps_[wi].empty();   // FIFO behind the ends
        if (behind || !trySubOp_(wi, p)) queueSubOp_(wi, std::move(p));
    }
    pendingEnforce_.push_back({offset, seq, ops, life});
}

void ClientGateway::onEnforceFenceAck_(int32_t worker, int ring, uint64_t seq) noexcept
{
    if (worker < 0 || worker >= kMaxFenceWorkers) [[unlikely]] return;
    auto& acked = (ring == 0 ? opFenceAcked_ : lifeFenceAcked_)[static_cast<std::size_t>(worker)];
    if (seq > acked) acked = seq;   // a ring is FIFO: an ack covers every earlier fence on it
    promoteEnforced_();
}

void ClientGateway::promoteEnforced_() noexcept
{
    const int32_t n = workerPool_ != nullptr ? std::min<int32_t>(workerPool_->workerCount(), kMaxFenceWorkers) : 0;
    while (!pendingEnforce_.empty()) {
        const PendingEnforce& e = pendingEnforce_.front();
        bool done = true;
        for (int32_t w = 0; w < n && done; ++w) {
            const auto wi = static_cast<std::size_t>(w);
            if (e.lifecycle && lifeFenceAcked_[wi] < e.seq) done = false;
            if ((e.opWorkers & (uint64_t{1} << w)) != 0 && opFenceAcked_[wi] < e.seq) done = false;
        }
        if (!done) return;
        securityEnforcedOffset_ = std::max(securityEnforcedOffset_, e.offset);
        pendingEnforce_.pop_front();
    }
}

void ClientGateway::sendErrorOn_(Session& s, int64_t corr, uint16_t code, std::string_view message)
{
    pc::ErrorResponse er;
    er.sessionId     = s.sessionId;
    er.correlationId = corr;
    er.code          = static_cast<int16_t>(code);
    er.message       = message.substr(0, 240);
    sendAnswer_(s, scratch_.data(), er.encode(scratch_.data()));
}

void ClientGateway::onSecurityApplied(int64_t offset, const rune::security::Applied& applied)
{
    using namespace rune::gateway;
    if (securityView_ == nullptr || securityView_->halted()) return;
    // Issues #109 / #196: what this event queues for the workers decides which of their rings its fence must cover.
    applyTracking_  = true;
    applyOpWorkers_ = 0;
    const uint64_t shippedBefore = permissionShipper_.queuedSeq();
    if (applied.effect != rune::security::SessionEffect::None && !applied.affectedUsers.empty()) {
        liveScratch_.clear();
        sessions_.forEach([&](Session& s) {
            uint64_t uid = 0;
            if (const auto* a = std::get_if<AuthAuthenticated>(&s.auth)) uid = a->userId;
            else if (const auto* p = std::get_if<AuthPasswordChangeRequired>(&s.auth)) uid = p->userId;
            else return;
            if (std::binary_search(applied.affectedUsers.begin(), applied.affectedUsers.end(), uid)) liveScratch_.push_back(s.sessionId);
        });
        if (applied.effect == rune::security::SessionEffect::Refresh) {
            for (const int64_t sid : liveScratch_) refreshSession_(sid);
        } else {
            const char* reason = "password changed (TERMINATE SESSIONS)";
            if (applied.kind == rune::security::EventKind::DropUser) reason = "user dropped";
            else if (applied.kind == rune::security::EventKind::NamespacePurged) reason = "namespace deleted";
            else if (const auto* u = securityView_->state().userById(applied.objectId); u != nullptr && !u->enabled) reason = "user disabled";
            for (const int64_t sid : liveScratch_) revokeSession_(sid, reason);
        }
    }
    // Namespaces Phase 2 Task 6a: a NamespacePurged also strips the purged namespace's grants from SURVIVING users
    // (`refreshedUsers`, sorted): their sessions recompile their permissions, whatever the main effect was (and even
    // when no user was erased, i.e. `affectedUsers` is empty or already the refresh set).
    if (!applied.refreshedUsers.empty()) {
        liveScratch_.clear();
        sessions_.forEach([&](Session& s) {
            uint64_t uid = 0;
            if (const auto* a = std::get_if<AuthAuthenticated>(&s.auth)) uid = a->userId;
            else if (const auto* p = std::get_if<AuthPasswordChangeRequired>(&s.auth)) uid = p->userId;
            else return;
            if (std::binary_search(applied.refreshedUsers.begin(), applied.refreshedUsers.end(), uid)) liveScratch_.push_back(s.sessionId);
        });
        for (const int64_t sid : liveScratch_) refreshSession_(sid);
    }
    applyTracking_ = false;
    noteEnforced_(offset, applyOpWorkers_, permissionShipper_.queuedSeq() != shippedBefore);
}

void ClientGateway::refreshSession_(int64_t sid)
{
    using namespace rune::gateway;
    Session* s = sessions_.find(sid);
    if (s == nullptr) return;
    if (const auto* pcr = std::get_if<AuthPasswordChangeRequired>(&s->auth)) {
        // The Task 11 transition, through this one path: a SET PASSWORD cleared the user's must-change flag.
        const rune::security::UserObject* user = securityView_->state().userById(pcr->userId);
        if (user == nullptr || user->mustChangePassword) return;
        s->auth = AuthAuthenticated{pcr->userId, pcr->user, compilePerms_(pcr->userId, pcr->nsId), pcr->loginNonce, pcr->nsId};
        (void)shipPermissions_(*s);
        return;
    }
    auto* a = std::get_if<AuthAuthenticated>(&s->auth);
    if (a == nullptr) return;
    // Issue #259: a pooled table - compiled only if no session holds the new set yet; the same set = the same table.
    PermissionRef fresh = compilePerms_(a->userId, a->nsId);
    if (fresh.id() == a->perms.id() && fresh.id() != PermissionRef::kNoId) return;   // nothing changed for it
    const LiveActions actions = refreshActions(a->perms.table(), fresh.table(), *s, objectIndex_);
    // The old table is held until the rebind is queued: its drop (if this was its last holder) must FOLLOW the bind,
    // or the worker would deny this session between the two.
    const PermissionRef old = std::exchange(a->perms, std::move(fresh));
    (void)shipPermissions_(*s);   // a revoked WRITE fails the next produce on both paths
    if (!actions.empty()) executeLiveActions_(*s, actions);
}

void ClientGateway::executeLiveActions_(Session& s, const rune::gateway::LiveActions& act)
{
    using namespace rune::gateway;
    const PermissionTable& perms = permsOf_(s);
    for (const EndSubscription& e : act.ends) {
        const auto d = authorizeTopicId(perms, e.topicId, rune::security::Operation::Read, objectIndex_);
        const std::string_view detail = d ? std::string_view(d->detail) : std::string_view("not authorized");
        // Issue #109: a WORKER-pushed subscription (per-worker egress) is ended by its push owner - the worker writes
        // its Records on its own publication, so only it can put the terminal NotAuthorized AFTER its last Records.
        // The ending Unsubscribe is queued behind the subscription's Subscribe (never dropped) and the event's
        // enforcement fence behind it (noteEnforced_): the DDL's OK waits until the worker dropped the cursor.  A
        // gateway-pushed subscription is ended here: forgotten first, so the gateway drops a racing push of its id.
        const bool workerEnds = perWorkerEgressActive_ && workerPool_ && e.topicId >= 0 &&
                                forwardUnsubscribeToWorker_(e.subscriptionId, e.topicId, e.partition,
                                                            static_cast<int16_t>(e.code), detail);
        s.subIdByKey.erase(Session::SubKey{e.correlationId, e.topicId, e.partition});
        subscriptions_.erase(e.subscriptionId);
        if (!workerEnds) sendErrorOn_(s, e.correlationId, e.code, detail);
        ++notAuthorizedAnswers_;
    }
    if (act.endCatalogReads) {
        forwardCatalogUnsubscribe_(s.sessionId, rune::gateway::kEndCatalogReadsOnly);
        const auto d = authorizeCluster(perms, rune::security::Operation::Describe);
        for (const int64_t corr : s.catalogReadCorrs)
            sendErrorOn_(s, corr, static_cast<uint16_t>(pc::ErrorCode::NotAuthorized), d ? std::string_view(d->detail) : std::string_view("not authorized"));
        s.catalogReadCorrs.clear();
    }
    if (act.endSecurityReads) {
        forwardCatalogUnsubscribe_(s.sessionId, rune::gateway::kEndSecurityReadsOnly);
        const auto d = authorizeCluster(perms, rune::security::Operation::Admin);
        for (const int64_t corr : s.securityReadCorrs)
            sendErrorOn_(s, corr, static_cast<uint16_t>(pc::ErrorCode::NotAuthorized), d ? std::string_view(d->detail) : std::string_view("not authorized"));
        s.securityReadCorrs.clear();
    }
    // Task 4: a lost WRITE grant aborts the session's open transactions (they could no longer be ended by it).
    if (act.endTxns && txnService_ != nullptr) (void)txnService_->abortAllForSession(s.sessionId, nowNs_);
    // Group members: a lost group READ removes the member (its next Heartbeat / Commit / Fetch answers 45); issue #212:
    // a lost READ on a topic a member subscribed to removes that member too (its group rebalances now; the re-join is
    // refused by the join check) - a consumer must not keep an assignment of a topic it may no longer read.
    if (groupService_ != nullptr && (act.endGroups || act.endGroupTopics)) {
        const PermissionTable* table = &perms;
        groupService_->removeUnauthorizedMembers(
            s.sessionId, nowNs_,
            IGroupRequestHandler::GroupReadCheck{
                [](void* ctx, std::string_view group) noexcept {
                    return static_cast<const PermissionTable*>(ctx)->allowsName(
                        rune::security::ResourceType::Group, group, rune::security::opBit(rune::security::Operation::Read));
                },
                const_cast<PermissionTable*>(table),
                [](void* ctx, std::string_view topic) noexcept {
                    return static_cast<const PermissionTable*>(ctx)->allowsName(
                        rune::security::ResourceType::Topic, topic, rune::security::opBit(rune::security::Operation::Read));
                }});
    }
}

void ClientGateway::revokeSession_(int64_t sid, std::string_view reason, uint16_t code)
{
    using namespace rune::gateway;
    Session* s = sessions_.find(sid);
    if (s == nullptr) return;
    // Issue #119: already revoked (or otherwise ended) and draining its last answers - the first revoke counted it,
    // bound it to the deny table and queued its 46s; nothing more is sent.
    if (s->spill.state == SpillState::Closing) return;
    std::string user = "<unknown>";
    if (const auto* a = std::get_if<AuthAuthenticated>(&s->auth)) user = a->user;
    else if (const auto* p = std::get_if<AuthPasswordChangeRequired>(&s->auth)) user = p->user;
    ++sessionsRevoked_;
    spdlog::warn("auth: session {} of user {} revoked: {}", sid, LogSafeName{user}, reason);
    // Issue #196: on the shard path a worker accepts this session's direct produce until its SessionClose is
    // processed, and the close may queue behind a full lifecycle ring on its own (announcer) FIFO.  Bind the session
    // to nothing FIRST, on the permission shipper's FIFO: the event's lifecycle fence follows it, so the DDL's OK is
    // released only once every worker refuses this session's produce, whatever the close's progress.
    bindDenyTable_(*s);
    // Each live subscription (and admin-log read) completes SessionRevoked on its correlation id.  PR #459 F3 (the #109
    // class): a subscription a WORKER pushes is ended by that worker - its 46 follows its last Records on the same
    // egress, and it is forgotten here first so the close below sends no plain Unsubscribe that would cancel the end.
    for (const auto& [key, subId] : s->subIdByKey) {
        const bool workerEnds = perWorkerEgressActive_ && workerPool_ && key.topicId >= 0 &&
                                forwardUnsubscribeToWorker_(subId, key.topicId, key.partition, static_cast<int16_t>(code), reason);
        if (workerEnds) subscriptions_.erase(subId);
        else sendErrorOn_(*s, key.correlationId, code, reason);
    }
    for (const int64_t corr : s->catalogReadCorrs) sendErrorOn_(*s, corr, code, reason);
    for (const int64_t corr : s->securityReadCorrs) sendErrorOn_(*s, corr, code, reason);
    // ... then the session-level frame (corr 0): every other in-flight request of the session completes with it.
    sendErrorOn_(*s, 0, code, reason);
    // Issue #212: a revoked session is terminal (never resumed): its group members leave now, so their groups
    // rebalance at once instead of at the members' heartbeat timeout.
    if (groupService_ != nullptr) groupService_->removeSessionMembers(sid, nowNs_);
    // Namespaces Phase 2 Task 4: for EVERY reason (password change, user drop / disable, namespace suspend / delete /
    // purge) the session's open transactions abort now (PrepareAbort + ABORT markers), so a read_committed reader's
    // LSO never waits for the transaction timeout of a producer that can no longer end it.
    if (txnService_ != nullptr) (void)txnService_->abortAllForSession(sid, nowNs_);
    closeAfterAnswers_(sid);   // issue #119: every 46 is delivered first, even when spilled
}

void ClientGateway::bindDenyTable_(Session& s)
{
    using namespace rune::gateway;
    if (!fencesLifecycleRing_()) return;
    uint64_t owner = 0;
    if (const auto* a = std::get_if<AuthAuthenticated>(&s.auth)) owner = a->userId;
    else if (const auto* p = std::get_if<AuthPasswordChangeRequired>(&s.auth)) owner = p->userId;
    (void)permissionShipper_.bind(*workerPool_, s.sessionId, rune::node::PermissionShipper::kDenyTable, ++permEpoch_, owner);
}

void ClientGateway::onSecurityHalted(int64_t offset)
{
    using namespace rune::gateway;
    liveScratch_.clear();
    sessions_.forEach([&](Session& s) {
        if (std::holds_alternative<AuthAuthenticated>(s.auth) || std::holds_alternative<AuthPasswordChangeRequired>(s.auth))
            liveScratch_.push_back(s.sessionId);
    });
    // #206: this broker can no longer vouch for its security state - the meaning of the retriable AuthUnavailable, not
    // a verdict on the user (SessionRevoked would be terminal and the client would never fail over).  Still fail-closed:
    // each session is bound to nothing on the workers first (#196), then ended.  A Closing session (already ended) is
    // skipped, so a second halt sends and counts nothing.
    for (const int64_t sid : liveScratch_) {
        Session* s = sessions_.find(sid);
        if (s == nullptr || s->spill.state == SpillState::Closing) continue;
        bindDenyTable_(*s);
        endSessionUnavailable_(*s, "security log halted on this broker - reconnect to another broker");
    }
    if (!liveScratch_.empty())
        spdlog::warn("auth: {} authenticated session(s) ended (AuthUnavailable): the security log halted on this broker at "
                     "offset {}", liveScratch_.size(), offset);
}

void ClientGateway::revokeAllSessionsForTest(std::string_view reason)
{
    using namespace rune::gateway;
    liveScratch_.clear();
    sessions_.forEach([&](Session& s) {
        if (std::holds_alternative<AuthAuthenticated>(s.auth) || std::holds_alternative<AuthPasswordChangeRequired>(s.auth))
            liveScratch_.push_back(s.sessionId);
    });
    for (const int64_t sid : liveScratch_) revokeSession_(sid, reason);
}

// ---------------------------------------------------------------------------
// Authentication Task 10 (D36): SecurityDdl (79) and SecurityBootstrap (80) - the same copy-and-return discipline as
// the catalog DDL; answered by a DdlResponse (78) from the admin thread (an OK only once THIS broker enforces the
// event - the SecurityAnswerGate).  Routed by the `__security` route exactly like catalog DDL: a follower answers
// Redirect to the leader's client endpoint.  A pre-v9 session can never reach this (Task 11's version gate); until
// then it is refused here.
// ---------------------------------------------------------------------------
void ClientGateway::handleSecurityDdl(const uint8_t* buf, std::size_t len)
{
    if (ddlRouted_(buf, len, securityRoute_, "security")) return;
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "SecurityDdl: admin sink not wired");
        return;
    }
    auto msgOpt = pc::SecurityDdl::decode(buf, len);
    if (!msgOpt) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "SecurityDdl body undecodable");
        return;
    }
    const pc::SecurityDdl& m = *msgOpt;
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "SecurityDdl for unknown session");
        return;
    }
    if (!pc::clientUnderstandsAuth(session->clientVersion)) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                             "SecurityDdl needs client version 9");
        return;
    }
    if (securityUnavailable_()) [[unlikely]] {   // issues #15 / #111: never built on (or judged against) a stale view
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable),
                             "this broker's security state is not current - retry on another broker");
        return;
    }
    // Authentication Task 12 (D20): ADMIN ON CLUSTER - except a user's own ALTER USER <self> ADD / DROP / SET PASSWORD,
    // which (issue #114) needs a fresh proof of a CURRENT password instead: the session alone (a hijacked plaintext
    // session, a leaked one) must not turn into a permanent second password.
    //
    // PR #410 review F1: the verdict may depend on state the request's own FIRST attempt changed (a SET PASSWORD
    // replaced the password its retry is proven with; a REVOKE removed the ADMIN its retry needs), and only the leader's
    // dedupe table knows authoritatively whether this is such a retry.  So a request with a request id is forwarded
    // with the verdict, never refused here; the leader answers a retry its original result and refuses the rest 45.  A
    // request without one (pre-v8) cannot be a deduped retry: refused here.
    std::optional<rune::gateway::Denial> denial;
    bool proofChecked = false;
    {
        const std::string* self = nullptr;
        const std::string* loginNonce = nullptr;
        if (const auto* a = std::get_if<rune::gateway::AuthAuthenticated>(&session->auth)) { self = &a->user; loginNonce = &a->loginNonce; }
        else if (const auto* p = std::get_if<rune::gateway::AuthPasswordChangeRequired>(&session->auth)) { self = &p->user; loginNonce = &p->loginNonce; }
        std::optional<rune::gateway::Denial> admin =
            rune::gateway::authorizeCluster(permsOf_(*session), rune::security::Operation::Admin);
        // PR #642 review F1: "own" is the RESOLVED (namespace, local) pair, so 'alpha/nsadm' from an alpha/nsadm
        // session is its own change too and needs the proof (never ADMIN-denied and forwarded to the ns-admin rule).
        const bool ownPassword = self != nullptr && pc::isCredentialChange(m) && ownUserName_(*session, m.name, *self);
        // A TRUE retry: a request this broker's mirror already holds with the same request id AND the same statement
        // fingerprint (PR #410 review F2: the id alone would let a different change skip the throttle).  The mirror
        // may lag the leader by an apply, so a retry can also look new: then it is still answered right, only charged
        // once on the throttle.
        bool knownRetry = false;
        if (!m.ext.requestId.none() && securityView_ != nullptr)
            if (const auto rec = securityView_->state().request(m.ext.requestId))
                knownRetry = rec->requestFingerprint == rune::gateway::securityDdlFingerprint(m);
        if (!ownPassword) denial = std::move(admin);
        else if (admin) {
            proofChecked = true;
            denial = verifyOwnCredentialChange_(*session, m, *self, *loginNonce, /*chargeThrottle=*/!knownRetry);
        }
        if (denial && m.ext.requestId.none()) {
            (void)deny_(*session, denial, buf, len);
            return;
        }
        if (denial && !knownRetry) noteDenied_(*session, pc::TemplateId::SecurityDdl, *denial);
        if (!denial && ownPassword && m.change == pc::UserChangeKind::AddPassword)   // a second password: worth a line
            spdlog::warn("security: user {} adds a password to its own account (session {}, source {})",
                         LogSafeName{*self},
                         session->sessionId, LogSafeName{session->sourceHost});
    }
    answerAdminSubmit_(buf, len,
                       adminSink_->submitSecurityDdl(m, ddlMetaOf_(&m.ext, m.sessionId, *session, 0),
                                                     denial ? std::string_view(denial->detail) : std::string_view{},
                                                     proofChecked),
                       "SecurityDdl: admin ring full", "SecurityDdl: request larger than the admin ring message");
}

// Namespaces Task 7: NamespaceDdl (81) - the catalog DDL discipline (the `__catalog` route, decode, session, copy and
// return).  Cluster ADMIN is required (spec: only a cluster admin manages namespaces); like SecurityDdl a denied
// request that carries a request id is forwarded with the verdict so the leader can answer a retry of an applied
// request its original result.  Cold path (DDL rate).
void ClientGateway::handleNamespaceDdl(const uint8_t* buf, std::size_t len)
{
    if (ddlRouted_(buf, len)) return;
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "NamespaceDdl: admin sink not wired");
        return;
    }
    auto msgOpt = pc::NamespaceDdlRequest::decode(buf, len);
    if (!msgOpt) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "NamespaceDdl body undecodable");
        return;
    }
    const pc::NamespaceDdlRequest& m = *msgOpt;
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "NamespaceDdl for unknown session");
        return;
    }
    if (session->clientVersion < pc::kClientVersionNamespaces) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                             "NamespaceDdl needs client version 14");
        return;
    }
    std::optional<rune::gateway::Denial> denial =
        rune::gateway::authorizeCluster(permsOf_(*session), rune::security::Operation::Admin);
    if (denial && m.ext.requestId.none()) {
        (void)deny_(*session, denial, buf, len);
        return;
    }
    if (denial) noteDenied_(*session, pc::TemplateId::NamespaceDdl, *denial);
    answerAdminSubmit_(buf, len,
                       adminSink_->submitNamespaceDdl(m, ddlMetaOf_(&m.ext, m.sessionId, *session, 0),
                                                      denial ? std::string_view(denial->detail) : std::string_view{},
                                                      static_cast<uint16_t>(session->clientVersion)),
                       "NamespaceDdl: admin ring full", "NamespaceDdl: request larger than the admin ring message");
}

// ListNamespaces (82): a read of the applied catalog, served by ANY broker's admin thread (no route, no append).  A
// cluster admin sees every namespace; anyone else only its own - the session's login namespace (namespaces Task 9).
void ClientGateway::handleListNamespaces(const uint8_t* buf, std::size_t len)
{
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "ListNamespaces: admin sink not wired");
        return;
    }
    auto msgOpt = pc::ListNamespaces::decode(buf, len);
    if (!msgOpt) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "ListNamespaces body undecodable");
        return;
    }
    const pc::ListNamespaces& m = *msgOpt;
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "ListNamespaces for unknown session");
        return;
    }
    if (session->clientVersion < pc::kClientVersionNamespaces) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                             "ListNamespaces needs client version 14");
        return;
    }
    const bool clusterAdmin =
        !rune::gateway::authorizeCluster(permsOf_(*session), rune::security::Operation::Admin).has_value();
    answerAdminSubmit_(buf, len,
                       adminSink_->submitListNamespaces(m.sessionId, m.correlationId, m.afterNsId, m.max,
                                                        sessionNsId_(*session), clusterAdmin),
                       "ListNamespaces: admin ring full", "ListNamespaces: request larger than the admin ring message");
}

std::optional<rune::gateway::Denial> ClientGateway::verifyOwnCredentialChange_(Session& session, const pc::SecurityDdl& m,
                                                                               const std::string& self,
                                                                               const std::string& loginNonce,
                                                                               bool chargeThrottle)
{
    // Issue #114.  Every failure is the same text (it never says which part failed) and counts as a failed login on
    // the throttle: a proof is a password guess.  Cold path (security DDL rate).
    rune::gateway::Denial refused;
    refused.detail = std::string(rune::gateway::kOwnCredentialProofRefusal);
    // A retry of a request the leader already applied (chargeThrottle false) is neither throttled nor charged: the
    // leader answers it from its dedupe table whatever this verdict says.
    if (chargeThrottle) {
        const rune::gateway::LoginThrottle::Verdict v = loginThrottle_.check(self, session.sourceHost, nowNs_);
        if (!v.allowed) {
            ++loginsRateLimited_;
            return refused;
        }
    }
    bool ok = false;
    const rune::security::UserObject* user =
        securityView_ != nullptr && !securityView_->halted() ? securityView_->state().userByName(sessionNsId_(session), self) : nullptr;
    if (user != nullptr && !loginNonce.empty() && (m.flags & pc::kSecurityDdlFlagCredentialProof) != 0 &&
        m.credentialProof.size() == pc::kCredentialProofBytes && !m.ext.requestId.none()) {
        const std::string message = pc::credentialChangeMessage(loginNonce, m);
        const auto proof = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(m.credentialProof.data()),
                                                    pc::kCredentialProofBytes);
        for (const auto& cur : user->verifiers) {   // any CURRENT password; no early exit
            rune::crypto::DigestBytes stored;
            std::memcpy(stored.bytes.data(), cur.storedKey.data(), cur.storedKey.size());
            stored.size = static_cast<uint8_t>(cur.storedKey.size());
            ok |= rune::crypto::scram::verifyClientProof(rune::crypto::Digest::Sha512, stored, message, proof);
        }
    }
    if (!ok) {
        if (!chargeThrottle) return refused;
        loginThrottle_.onFailure(self, session.sourceHost, nowNs_);
        if (unauthLog_.due(4)) [[unlikely]]
            spdlog::warn("security: user {} sent a change of its own password without a valid proof (session {}, source {})",
                         LogSafeName{self}, session.sessionId, LogSafeName{session.sourceHost});
        return refused;
    }
    if (chargeThrottle) loginThrottle_.onSuccess(self, session.sourceHost, nowNs_);
    return std::nullopt;
}

void ClientGateway::handleSecurityBootstrap(const uint8_t* buf, std::size_t len)
{
    if (ddlRouted_(buf, len, securityRoute_, "security")) return;
    if (!adminSink_) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::Internal), "SecurityBootstrap: admin sink not wired");
        return;
    }
    auto msgOpt = pc::SecurityBootstrap::decode(buf, len);
    if (!msgOpt) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "SecurityBootstrap body undecodable");
        return;
    }
    const pc::SecurityBootstrap& m = *msgOpt;
    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "SecurityBootstrap for unknown session");
        return;
    }
    if (!pc::clientUnderstandsAuth(session->clientVersion)) {
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest),
                             "SecurityBootstrap needs client version 9");
        return;
    }
    if (securityUnavailable_()) [[unlikely]] {   // issues #15 / #111
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::AuthUnavailable),
                             "this broker's security state is not current - retry on another broker");
        return;
    }
    // D4: a fresh cluster cannot be hijacked by whoever reaches it first - the request must prove possession of the
    // cluster key (current or next).  Verified HERE with this thread's own copy of the keys, before the admin thread
    // is involved; the refusal is explicit, and logged with where it came from.
    const BootstrapProof proof = verifyBootstrapProof(m, clusterKeys_ ? &*clusterKeys_ : nullptr, bootstrapMacScratch_);
    if (proof != BootstrapProof::Ok) {
        ++bootstrapsRefused_;
        if (unauthLog_.due(3)) [[unlikely]]   // X103: an unauthenticated session drives this
            spdlog::warn("security: bootstrap refused from {}: bad cluster-key proof{}", LogSafeName{session->egressChannel},
                         proof == BootstrapProof::NoKey ? " (this broker holds no cluster key: authentication is not enabled)" : "");
        pc::DdlResponse r;
        r.sessionId     = m.sessionId;
        r.correlationId = m.correlationId;
        r.errorCode     = static_cast<uint16_t>(pc::ErrorCode::InvalidRequest);
        r.detail        = proof == BootstrapProof::NoKey ? "this broker has no cluster key: authentication is not enabled"
                                                         : "bootstrap proof does not match this cluster's key";
        const std::size_t n = r.encode(scratch_.data());
        sendAnswer_(*session, scratch_.data(), n);
        return;
    }
    answerAdminSubmit_(buf, len, adminSink_->submitSecurityBootstrap(m, ddlMetaOf_(nullptr, m.sessionId, *session, 0)),
                       "SecurityBootstrap: admin ring full", "SecurityBootstrap: request larger than the admin ring message");
}

// ---------------------------------------------------------------------------
// Task 20: handleGetViewRow — a point read of a materialized view row.
//
// Routed to the worker that OWNS (viewTopicId, partition) through the SAME
// op/return plumbing as PushRead: build a GetViewRow WorkerOp (reusing the
// scalar wire fields — topicId = viewTopicId, nextOffset = keyU64, hwBound =
// requestedFingerprint; see WorkerOp.h) and tryEnqueueOp it.  The worker probes
// its WorkerViewSet and answers a ViewRowResult that drainWorkerReturns_ encodes
// as a ViewRowResponse on the session egress.  No registry round-trip, no
// blocking on the gateway hot path.
//
// The view key space is the wire record-key (Phase 1 integer key): keyU64 is
// hashed to a partition via the view's output topic, encoding it as the
// canonical fixed-width (8-byte little-endian) key bytes — the same form the
// source rows were produced with.
// ---------------------------------------------------------------------------
uint16_t ClientGateway::viewRowGate_(int32_t viewTopicId, int32_t partition) const noexcept
{
    // Catalog history Task 8 fix round 1 (I5): the C1 gate on view reads (the
    // view's sink topic, spec §5.4).  Dropped → UnknownView (permanent); applied
    // but waiting behind a drop, placed but not applied here yet, or applied but
    // not placed → NotReady (retriable: a client that learned the view from
    // another broker retries).  Never applied here and never placed: no gate —
    // the worker answers (UnknownView: no plan).
    if (topicRegistry_ == nullptr) return kViewRowNoGate;
    using St = TopicRegistry::TopicState;
    const St st = topicRegistry_->state(viewTopicId);
    if (st == St::Dropped) return pc::ViewRowResponse::kUnknownView;
    // Catalog history Task 12 (Task 8 review N5): a sink applied here whose plan
    // has not reached every worker yet — retriable, never the worker's UnknownView.
    if (!viewSinksAwaitingPlan_.empty() && viewSinkAwaitingPlan(viewTopicId)) [[unlikely]]
        return pc::ViewRowResponse::kNotReady;
    // VLS B.2: a sink owns no placement -- the view partition IS the source partition, so its gate is the source's.
    if (st == St::ViewSink) {
        const int32_t src = viewSourceOf_(viewTopicId);
        // I3 (fail closed): the sink mirrors its source's partitioning; a catalog that says otherwise can never be
        // routed (the view partition IS the source partition).  Permanent UnknownView, never a wrong-table read.
        const rune::node::TopicSpec* sinkSpec = topicRegistry_->viewSinkSpec(viewTopicId);
        const rune::node::TopicSpec* srcSpec  = src >= 0 ? topicRegistry_->topic(src) : nullptr;
        if (sinkSpec != nullptr && srcSpec != nullptr && sinkSpec->partitionCount != srcSpec->partitionCount) [[unlikely]] {
            if (viewSinkMismatchLogs_++ % 1024 == 0)
                spdlog::critical("ClientGateway: view sink id={} has {} partition(s) but its source id={} has {} -- GetViewRow "
                                 "answers UnknownView (a sink mirrors its source's partitioning; fix the catalog)",
                                 viewTopicId, sinkSpec->partitionCount, src, srcSpec->partitionCount);
            return pc::ViewRowResponse::kUnknownView;
        }
        return (src >= 0 && !placementCommitted_(src, partition)) ? pc::ViewRowResponse::kNotReady : kViewRowNoGate;
    }
    if (st == St::Pending || (st == St::Unknown && placementCommitted_(viewTopicId, 0)) ||
        (st == St::Serving && topicRegistry_->placementGated(viewTopicId) &&
         !placementCommitted_(viewTopicId, partition)))
        return pc::ViewRowResponse::kNotReady;
    return kViewRowNoGate;
}

void ClientGateway::handleGetViewRow(const uint8_t* buf, std::size_t len)
{
    if (!workerPool_) {
        // Task 8 fix round 1 (I5): a gated view answers its C1 code first.
        if (const auto mo = pc::GetViewRow::decode(buf, len)) {
            const uint16_t gate = viewRowGate_(mo->viewTopicId, 0);
            if (gate != kViewRowNoGate) {
                (void)rejectRequest_(buf, len, gate, "GetViewRow: view not served here (C1)");
                return;
            }
        }
        // No-hang (R2): no workers on this gateway — no view is servable here, a
        // permanent condition: UnknownView (4), not the retriable NotReady.
        (void)rejectRequest_(buf, len, pc::ViewRowResponse::kUnknownView, "GetViewRow: no worker pool");
        return;
    }
    auto msgOpt = pc::GetViewRow::decode(buf, len);
    if (!msgOpt) {
        // No-hang (R2): answered ViewRowResponse{InvalidRequest (5)}.
        (void)rejectRequest_(buf, len, pc::ViewRowResponse::kInvalidRequest, "GetViewRow body undecodable");
        return;
    }
    const pc::GetViewRow& m = *msgOpt;

    Session* session = sessions_.find(m.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "GetViewRow for unknown session");
        return;
    }
    // Authentication Task 12 (spec §2.1, D22): READ on the view's topic id - one byte load; a hidden and a missing id
    // answer alike.
    if (const PermissionTable& perms = permsOf_(*session); !perms.isAllowAll())
        if (deny_(*session, authorizeTopicIdHere_(perms, m.viewTopicId, 0, rune::security::Operation::Read,
                                                             rune::security::ResourceType::View), buf, len))
            return;

    // Resolve the owning partition from the key (canonical 8-byte LE key bytes).
    // partitionForKey needs the view's output topic in the registry; an unknown
    // view id yields partition 0 and the worker answers UnknownView.
    uint8_t keyLe[8];
    for (int i = 0; i < 8; ++i) keyLe[i] = static_cast<uint8_t>(m.keyU64 >> (8 * i));
    int32_t partition = 0;
    if (topicRegistry_) {
        const int32_t p = topicRegistry_->partitionForKey(
            m.viewTopicId, std::span<const uint8_t>(keyLe, sizeof keyLe));
        if (p >= 0) partition = p;
    }
    if (const uint16_t gate = viewRowGate_(m.viewTopicId, partition); gate != kViewRowNoGate) {
        (void)rejectRequest_(buf, len, gate, "GetViewRow: view not served here (C1)");
        return;
    }
    // Task 22 fix round 1 (C1): a view row is served only by the broker that leads
    // its source partition (the view partition IS the source partition).  Anyone
    // else redirects to that leader (bare NotLeader when leaderless).  The gate
    // above stays first (Dropped -> UnknownView).  The worker re-checks its own
    // role + catch-up (the image may lag a leadership change).
    if (std::string_view ep; viewSourceLedElsewhere_(m.viewTopicId, partition, ep)) {
        sendRouteRefusal_(*session, m.sessionId, m.correlationId, ep);
        return;
    }

    rune::node::WorkerOp op;
    op.type          = rune::node::WorkerOp::Type::GetViewRow;
    op.topicId       = m.viewTopicId;                        // routing + viewTopicId
    op.partition     = partition;
    op.sessionId     = m.sessionId;
    op.correlationId = m.correlationId;
    op.nextOffset    = static_cast<int64_t>(m.keyU64);               // keyU64 (reused field)
    op.hwBound       = static_cast<int64_t>(m.requestedFingerprint); // requestedFingerprint (reused field)

    // VLS B.2: route by the SOURCE topic (the sink has no router entry); op.topicId stays the view id.
    const int32_t viewSrc    = viewSourceOf_(m.viewTopicId);
    const int32_t routeTopic = viewSrc >= 0 ? viewSrc : m.viewTopicId;
    if (!workerPool_->tryEnqueueOp(routeTopic, partition, std::move(op))) {
        // Back-pressure: the worker ingress ring is full this cycle.  No-hang
        // (R2): answered ViewRowResponse{Busy (6)} — the read was not performed;
        // the client re-issues it (a point read is idempotent).  Nothing parked.
        (void)rejectRequest_(buf, len, pc::ViewRowResponse::kBusy, "GetViewRow: worker ring full");
    }
}

// ---------------------------------------------------------------------------
// CG-4: addGroupSubscription — create an internal push subscription for a
// group member.  Seeds nextOffset from fromOffset (the committed offset from
// the ReplicatedOffsetStore, or log-start/HW for auto.offset.reset).
//
// Reuses the SAME Subscription struct and pushSubscriptions() machinery;
// marks isGroup=true so revokeGroupSubscriptions() can identify it.
// Tracks the subscriptionId in groupMemberSubs_[memberSessionId].
//
// Returns subscriptionId > 0 on success, -1 if session is unknown or topic
// is unreachable (topicRegistry_ null / bad topicId / bad partition).
//
// Single-writer: must be called on the duty-cycle thread.
// Rebalance-time cold path: not on the per-cycle push hot path.
// ---------------------------------------------------------------------------
int64_t ClientGateway::addGroupSubscription(int64_t memberSessionId,
                                            int32_t topicId,
                                            int32_t partition,
                                            int64_t fromOffset,
                                            uint8_t isolation,
                                            bool    pendingCarrier)
{
    // Require a known session.
    Session* session = sessions_.find(memberSessionId);
    if (!session) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown session={}", memberSessionId);
        return -1;
    }

    // Validate the topic and partition via the registry.
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway::addGroupSubscription: topicRegistry_ not set");
        return -1;
    }

    // C1 + E2: uncommitted / unknown → refused; a TRANSIENT partition (no disk
    // Log) is pushed by its owning worker only — refused without a worker pool.
    const ServedPartition served = servedPartition_(topicId, partition);
    if (served.kind == ServedPartition::Kind::NotPlaced ||
        served.kind == ServedPartition::Kind::Unknown ||
        (served.kind == ServedPartition::Kind::Transient && !workerPool_)) {
        spdlog::warn("ClientGateway::addGroupSubscription: unknown (or uncommitted) topicId={} partition={}",
                     topicId, partition);
        return -1;
    }
    // E2 Task 11 ruling: read_committed only where it is served (a leader).
    if (isolation == static_cast<uint8_t>(rune::transaction::IsolationLevel::ReadCommitted) &&
        !readCommittedServableHere_(topicId, partition)) [[unlikely]] {
        spdlog::debug("ClientGateway::addGroupSubscription: read_committed on a follower topicId={} "
                      "partition={} — refused (NotLeader)", topicId, partition);
        return -1;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ Kind::Transient

    // Clamp fromOffset to [0, bound] where bound is HW when a replica is
    // wired (GR-6), else LEO.  Negative fromOffset is clamped to 0 (log start;
    // E2: a transient cursor below the ring start is moved up by the worker).
    // Sanitizer triage T2: never read a worker-owned Log from this thread.
    const bool gatewayLog = gatewayOwnsLog_(topicId, log);
    const rune::replication::PartitionReplica* const boundReplica =
        replicaRegistry_ ? replicaRegistry_->lookup(topicId, partition) : nullptr;
    if (log != nullptr && !gatewayLog && boundReplica == nullptr) [[unlikely]] {
        spdlog::warn("ClientGateway::addGroupSubscription: worker-owned topicId={} partition={} has no "
                     "replica (no published HW) — refused", topicId, partition);
        return -1;
    }
    const int64_t leo = gatewayLog ? log->logEndOffset() : 0;
    const int64_t bound = boundReplica ? boundReplica->gatewayHighWatermark() : leo;
    const int64_t nextOffset = (fromOffset < 0)  ? int64_t{0}
                             : (fromOffset > bound) ? bound
                             : fromOffset;

    // Allocate subscriptionId and register the group subscription.
    const int64_t subId = allocSubscriptionId_();

    Subscription sub;
    sub.subscriptionId       = subId;
    sub.sessionId            = memberSessionId;
    sub.topicId              = topicId;
    sub.partition            = partition;
    sub.nextOffset           = nextOffset;
    sub.isolation            = isolation;
    sub.isGroup              = true;
    // TX-6b: stall pre-carrier ONLY when the caller signals that a Subscribe
    // carrier frame is still in flight (pendingCarrier=true).  This is set
    // exclusively by GroupCoordinationService, which seeds the sub with the
    // conservative default isolation=1 and expects the client to send a carrier
    // with the true isolation level.  All other callers (explicit isolation,
    // tests) pass pendingCarrier=false and behave normally from the start.
    sub.groupCarrierReceived = !pendingCarrier;

    subscriptions_.emplace(subId, std::move(sub));

    // Track the subscriptionId under this member session for eager revoke.
    groupMemberSubs_[memberSessionId].push_back(subId);

    // read-rework I5: route group subscriptions to the owning worker too (the
    // worker pushes on its own dynamic-MDC egress).  Without this, pushSubscriptions
    // skips user partitions under per-worker egress and group reads get nothing.
    if (perWorkerEgressActive_ && workerPool_ && topicId >= 0) {
        forwardSubscribeToWorker_(subId, memberSessionId, topicId, partition,
                                  nextOffset, isolation,
                                  /*isGroup=*/true, /*groupCarrierReceived=*/!pendingCarrier);
    }

    spdlog::debug("ClientGateway::addGroupSubscription: sessionId={} subId={} "
                  "topic={} partition={} nextOffset={}",
                  memberSessionId, subId, topicId, partition, nextOffset);
    return subId;
}

// ---------------------------------------------------------------------------
// CG-4: revokeGroupSubscriptions — eagerly remove all push subscriptions
// that were created via addGroupSubscription() for the given member session.
//
// Normal client-initiated subscriptions (isGroup==false) are NOT touched.
// O(n_group_subs) where n_group_subs is the number of partitions assigned
// to this member — typically very small.
//
// Single-writer: must be called on the duty-cycle thread.
// ---------------------------------------------------------------------------
void ClientGateway::revokeGroupSubscriptions(int64_t memberSessionId)
{
    auto it = groupMemberSubs_.find(memberSessionId);
    if (it == groupMemberSubs_.end()) return;  // no group subs for this session

    for (int64_t subId : it->second) {
        // read-rework I5: tell the owning worker to drop the cursor too (mirror of
        // addGroupSubscription's forward), before erasing the gateway record.
        if (perWorkerEgressActive_ && workerPool_) {
            auto sIt = subscriptions_.find(subId);
            if (sIt != subscriptions_.end() && sIt->second.topicId >= 0) {
                forwardUnsubscribeToWorker_(subId, sIt->second.topicId, sIt->second.partition);
            }
        }
        const std::size_t removed = subscriptions_.erase(subId);
        spdlog::debug("ClientGateway::revokeGroupSubscriptions: sessionId={} subId={} removed={}",
                      memberSessionId, subId, removed);
    }
    groupMemberSubs_.erase(it);
}

// ---------------------------------------------------------------------------
// CG-4: sendControlFrame — send an already-encoded frame (e.g. Assign) on
// a session's egress.  Reuses the same sendTo path as all other responses.
//
// Returns true if the frame was accepted by the egress publication.
// Does NOT modify scratch_ (the caller encodes the frame into its own buffer).
//
// Single-writer: must be called on the duty-cycle thread.
// ---------------------------------------------------------------------------
bool ClientGateway::sendControlFrame(int64_t sessionId,
                                     const uint8_t* buf,
                                     std::size_t len)
{
    // Invoke the optional test capture callback BEFORE the egress send.
    // This allows test code to capture Assign frames without a live Aeron publication.
    if (onControlFrame_) {
        onControlFrame_(sessionId, buf, len);
    }

    Session* session = sessions_.find(sessionId);
    if (!session) {
        spdlog::debug("ClientGateway::sendControlFrame: unknown sessionId={} — dropped", sessionId);
        return false;
    }
    // Namespaces Phase 2 Task 9: a DdlResponse the admin thread answered NamespaceLimitExceeded (52) reaches a client
    // below v14 as InvalidRequest (39); the admin thread does not know the session's version.  Cold: only that code.
    if (session->clientVersion < pc::kClientVersionNamespaces && len > pc::MessageHeader::ENCODED_SIZE + 2) [[unlikely]] {
        if (const auto h = pc::MessageHeader::decode(buf, len);
            h && h->templateId == static_cast<uint16_t>(pc::TemplateId::DdlResponse) &&
            (buf[pc::MessageHeader::ENCODED_SIZE] | (buf[pc::MessageHeader::ENCODED_SIZE + 1] << 8)) ==
                static_cast<int>(pc::ErrorCode::NamespaceLimitExceeded)) {
            ddlDowngradeScratch_.assign(buf, buf + len);
            ddlDowngradeScratch_[pc::MessageHeader::ENCODED_SIZE]     = static_cast<uint8_t>(pc::ErrorCode::InvalidRequest);
            ddlDowngradeScratch_[pc::MessageHeader::ENCODED_SIZE + 1] = 0;
            buf = ddlDowngradeScratch_.data();
        }
    }
    // No-hang (R2): every answer delivered through here (DDL responses from the
    // admin thread, deferred EndTxn answers, Assign) is queued and retried under
    // back-pressure — a best-effort sendTo lost it and left the client waiting.
    if (!session->egress) return sendTo(*session, buf, len);   // egress-less test sessions (capture hook)
    sendReliable_(*session, buf, len);
    return true;
}

// ---------------------------------------------------------------------------
// TX-4 Task 8: appendControlRecord
//
// Appends an already-framed control-marker batch (COMMIT or ABORT) to the
// log of the given (topicId, partition).  This is the "gateway writes marker"
// path used by TransactionCoordinationService::doWork() when it drives marker
// writes for PrepareCommit/PrepareAbort transactions.
//
// Key contract (KIP-98):
//   - Control batches are appended with acks=All (majority-durable) via
//     PartitionReplica::append(marker, acksAll=true).
//   - If the replica is not available (no replicaRegistry, or partition
//     unknown), falls back to a direct Log::appendNoSync (for single-node /
//     test environments where acks=All is trivially satisfied).
//   - After appending, calls replica->onLeaderLogAppended() to reconcile
//     leader LEO/HW.
//
// Returns the base offset of the appended batch, or -1 on failure.
//
// Single-writer: must be called on the duty-cycle thread.
// ---------------------------------------------------------------------------
std::size_t ClientGateway::purgeQueuedMarkers(int32_t olderThan)
{
    std::size_t purged = 0;
    for (auto q = markerSendQueue_.begin(); q != markerSendQueue_.end();) {
        auto it = markersInFlight_.find(*q);
        // D9: a marker appended on behalf of ANOTHER coordinator (originNode)
        // is not this coordinator's to purge (different epoch space).
        if (it != markersInFlight_.end() && it->second.originNode < 0 &&
            it->second.coordinatorEpoch < olderThan) {
            markersInFlight_.erase(it);
            q = markerSendQueue_.erase(q);
            ++purged;
        } else {
            ++q;
        }
    }
    if (purged > 0)
        spdlog::info("ClientGateway: purged {} queued control marker(s) of coordinator epochs < {}", purged, olderThan);
    return purged;
}

int64_t ClientGateway::appendControlRecord(int32_t topicId, int32_t partition,
                                            std::span<const uint8_t> marker,
                                            int64_t producerId, bool commit,
                                            int32_t coordinatorEpoch)
{
    if (!topicRegistry_) {
        spdlog::warn("ClientGateway::appendControlRecord: no TopicRegistry — dropped");
        return -1;
    }

    // C1 + E2: a committed TRANSIENT partition's markers ride the same worker /
    // remote paths below (the worker appends the control batch to the ring).
    const ServedPartition served = servedPartition_(topicId, partition);
    if (served.kind != ServedPartition::Kind::Disk && served.kind != ServedPartition::Kind::Transient) {
        spdlog::debug("ClientGateway::appendControlRecord: unknown (or uncommitted) topicId={} partition={} "
                      "— dropped", topicId, partition);
        return -1;
    }
    rune::storage::Log* const log = served.log;   // nullptr ⟺ Kind::Transient

    if (producerId < 0 && marker.size() >= 43u + 8u) {   // the control batch's producerId (LE @43)
        producerId = 0;
        for (int i = 0; i < 8; ++i) producerId |= static_cast<int64_t>(marker[43u + i]) << (i * 8);
    }

    static_assert(rune::transaction::ControlMarker::ENCODED_SIZE <=
                  rune::replication::WriteTxnMarkerMsg::kMaxMarkerBytes, "a marker must fit WriteTxnMarker");
    // D9: a partition led by ANOTHER node — forward the marker to its leader
    // over the cluster plane (Kafka WriteTxnMarkers); the answer completes it.
    if (int32_t leaderEpoch = -1; marker.size() <= rune::replication::WriteTxnMarkerMsg::kMaxMarkerBytes) {
        if (const int32_t leader = remoteMarkerLeader_(topicId, partition, leaderEpoch); leader >= 0) {
            if (marker.empty()) return -1;
            const int64_t token = nextMarkerToken_++;
            MarkerInFlight m;
            m.topicId           = topicId;
            m.partition         = partition;
            m.producerId        = producerId;
            m.commit            = commit;
            m.coordinatorEpoch  = coordinatorEpoch;
            m.remoteNode        = leader;
            m.remoteLeaderEpoch = leaderEpoch;
            m.bytes.assign(marker.begin(), marker.end());
            markersInFlight_.emplace(token, std::move(m));
            if (!markerSendQueue_.empty() || !trySendMarker_(token)) {
                markerSendQueue_.push_back(token);   // FIFO; outbound ring full → sent by a later doWork
                spdlog::warn("ClientGateway::appendControlRecord: cluster-plane back-pressure for topicId={} "
                             "partition={} (leader node {}) — marker queued ({} waiting)",
                             topicId, partition, leader, markerSendQueue_.size());
            }
            return kControlRecordAsync;
        }
    }


    // Task #13 / F15 K3: the marker is appended by the OWNING worker under
    // EXACTLY the produce-routing predicate (routesToWorker_), in every mode —
    // including the default (Legacy) config, where produces already go to the
    // worker: appending the marker inline here was a write-write race with the
    // worker, and the marker overtook produces still queued on its op ring.
    // Queued on the same op ring, the marker lands after every produce routed
    // before it (L4).
    //
    // Task 10 round 4 (P1): the marker's offset is NOT pre-computed here; the
    // worker returns where it ACTUALLY landed (ControlRecordAppended) and
    // the owning worker applied it to its own RC state in the same invocation
    // (F15 S4; no gateway-side LSO with a worker pool).  The caller
    // gets kControlRecordAsync.  A full ring never drops the marker: it waits in
    // markerSendQueue_ and is retried every doWork, in order.  An unregistered
    // partition is stamped stale by WorkerPool and answered through the normal
    // refusal path.
    //
    // The inline branch below remains only without a worker pool (unit rigs) and
    // for gateway-owned internal partitions in shard mode (never markers).
    if (routesToWorker_(topicId)) {
        if (marker.empty()) return -1;
        const int64_t token = nextMarkerToken_++;
        MarkerInFlight m;
        m.topicId    = topicId;
        m.partition  = partition;
        m.producerId = producerId;
        m.commit     = commit;
        m.coordinatorEpoch = coordinatorEpoch;
        m.bytes.assign(marker.begin(), marker.end());
        markersInFlight_.emplace(token, std::move(m));
        if (!markerSendQueue_.empty() || !trySendMarker_(token)) {
            markerSendQueue_.push_back(token);   // FIFO behind earlier markers
            spdlog::warn("ClientGateway::appendControlRecord: worker back-pressure for topicId={} "
                         "partition={} — marker queued ({} waiting), retried every cycle",
                         topicId, partition, markerSendQueue_.size());
        }
        return kControlRecordAsync;
    }
    if (log == nullptr) [[unlikely]] {   // E2: transient without a worker pool — no inline append (ruling 6)
        spdlog::debug("ClientGateway::appendControlRecord: transient topicId={} partition={} without a "
                      "worker pool — dropped", topicId, partition);
        return -1;
    }
    return appendControlRecordInline_(topicId, partition, marker, log);
}

int64_t ClientGateway::appendControlRecordInline_(int32_t topicId, int32_t partition,
                                                  std::span<const uint8_t> marker, rune::storage::Log* log)
{
    int64_t base = -1;
    rune::replication::PartitionReplica* replica =
        replicaRegistry_ ? replicaRegistry_->lookup(topicId, partition) : nullptr;

    if (replica) {
        // Durable path (F15 N2): the marker lands AS a control batch, restamped
        // in place, role- and min-ISR-gated (acks=All); negative = refused.
        base = replica->appendControlBatch(marker, /*acksAll=*/true);
        if (base < 0) {
            spdlog::warn("ClientGateway::appendControlRecord: appendControlBatch returned {} "
                         "topicId={} partition={}", base, topicId, partition);
            return -1;
        }
    } else {
        // No-replica path: single-node or test — direct log append.
        // In production this branch is only taken when replicaRegistry_ is null
        // (no replication wired); minIsr=1 is trivially satisfied locally.
        const int64_t batchLen = static_cast<int64_t>(marker.size());
        if (batchLen == 0) return -1;

        // Zero-copy restamp: read lastOffsetDelta (LE32@23) from the read-only
        // marker span (we must not mutate the caller's span), then let
        // appendNoSyncRestamp assign + patch the base offset via pwritev — no copy.
        int32_t lod = 0;
        {
            constexpr std::size_t kLodOff =
                static_cast<std::size_t>(rune::protocol::batch::kLastOffsetDeltaOffset);
            if (marker.size() >= kLodOff + 4u) {
                const uint8_t* p = marker.data() + kLodOff;
                lod = static_cast<int32_t>(
                    static_cast<uint32_t>(p[0]) |
                    (static_cast<uint32_t>(p[1]) << 8) |
                    (static_cast<uint32_t>(p[2]) << 16) |
                    (static_cast<uint32_t>(p[3]) << 24));
            }
        }

        try {
            log->appendNoSyncRestamp(marker.data(), marker.size(),
                                     lod >= 0 ? lod : 0, base);
        } catch (const std::exception& ex) {
            spdlog::error("ClientGateway::appendControlRecord: appendNoSyncRestamp threw: {}", ex.what());
            return -1;
        } catch (...) {
            spdlog::error("ClientGateway::appendControlRecord: appendNoSyncRestamp threw unknown exception");
            return -1;
        }

        dirtyLogs_.insert(log);
    }

    // Reconcile leader HW after the append.
    if (replica) {
        replica->onLeaderLogAppended();
    }

    // Markers / epoch fencing (PSM <-> log consistency): the rebuild applies
    // every control batch as an epoch raise (ProducerStateManager::rebuildFromLog);
    // the live inline state must too, or it differs from its own log (a zombie
    // of the fenced epoch would be appended).  Raise-only; cold (per marker).
    if (marker.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        const rune::protocol::RecordBatchReader mr(marker.data(), marker.size());
        producerStateManager_.fenceEpoch(topicId, mr.producerId(), partition, mr.producerEpoch());
    }

    // Synchronous (no-pool inline) path: the caller invokes notifyControlMarker()
    // to update the inline path's LsoTracker + AbortedTxnIndex.
    return base;
}

// ---------------------------------------------------------------------------
// TX-4 Task 9: notifyTransactionalBatch — test/integration hook.
//
// Feeds the LsoTracker for a transactional data batch that was appended via
// a path other than handleProduce (e.g. directly by a test fixture).
// In production this is called from handleProduce when the TRANSACTIONAL
// bit is set; this hook allows test fixtures to set up LSO state without
// going through the full produce pipeline.
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::notifyTransactionalBatch(int32_t topicId, int32_t partition,
                                              int64_t producerId, int64_t baseOffset) noexcept
{
    if (producerId < 0) return;  // non-transactional; ignore
    getTxnState(topicId, partition).lsoTracker.onProduceBatch(producerId, baseOffset);
}

// ---------------------------------------------------------------------------
// TX-4 Task 9: notifyControlMarker — feeds LsoTracker + AbortedTxnIndex.
//
// Called when a COMMIT or ABORT control marker is appended for a producer
// on (topicId, partition).  Updates the LsoTracker (removes the producer from
// open transactions → LSO can advance) and, for ABORT markers, records the
// aborted range in AbortedTxnIndex so pushSubscriptions() can filter it.
//
// In production this is called from appendControlRecord (or the doWork path
// that drives marker appends).  In tests the fixture calls it directly after
// appending test marker batches to set up the expected LSO/aborted state.
//
// Single-writer: duty-cycle thread only.
// ---------------------------------------------------------------------------
void ClientGateway::notifyControlMarker(int32_t topicId, int32_t partition,
                                         int64_t producerId, int64_t markerOffset,
                                         bool commit) noexcept
{
    if (producerId < 0) return;  // non-transactional marker; ignore
    PartitionTxnState& ts = getTxnState(topicId, partition);
    const int64_t firstOffset = ts.lsoTracker.onControlMarker(producerId, markerOffset, commit);
    if (!commit && firstOffset >= 0) {
        // ABORT: record the aborted range so pushSubscriptions() can filter it.
        ts.abortedIndex.onAbortMarker(producerId, firstOffset, markerOffset);
    }
}

// ---------------------------------------------------------------------------
// handleMetadata (MD1 Task 10) — answer a MetadataRequest from the routing image.
//
// ANSWERABLE BY ANY NODE — not just the leader.  Cluster metadata is
// cluster-wide information; any node can build a MetadataResponse from the
// RoutingImage (which is published by MetadataService to all nodes via the
// RoutingImageHolder and then installed into the gateway by
// setRoutingImageSnapshot()).
//
// Response contents:
//   - brokers[]: one entry per nodeId in nodeEndpointMap_ (nodeId + "host:port"
//     parsed from the Aeron channel string).  The port is extracted from the
//     endpoint part of "aeron:udp?endpoint=host:port"; nodeId 0 is the local node
//     (thisNodeId_) with its own channel.  Empty map → empty brokers list.
//   - partitions[]: one entry per slot in the routing image that is not the
//     NO_LEADER sentinel.  The (topicId, partition) pair is recovered from the
//     GPID via the inverse of the gpid = topicId * kMaxGpidPartitions + partition
//     scheme.  If topicRegistry_ is wired, only partitions belonging to known topics
//     are included; otherwise all non-sentinel slots are included.
//
// When routingImage_ is null (no metadata published yet) the response carries
// empty partitions but still includes the broker list (from nodeEndpointMap_).
//
// The response is sent back on the session's egress — it must be decoded by the
// client just like any other broker→client frame.
//
// Single-writer: duty-cycle thread only (same as all other handle* methods).
// ---------------------------------------------------------------------------
void ClientGateway::handleMetadata(const uint8_t* buf, std::size_t len)
{
    auto reqOpt = pc::MetadataRequest::decode(buf, len);
    if (!reqOpt) {
        // No-hang (R2): answered ErrorResponse{InvalidRequest}.
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), "MetadataRequest body undecodable");
        return;
    }

    const pc::MetadataRequest& req = *reqOpt;

    Session* session = sessions_.find(req.sessionId);
    if (!session) {
        noteUnanswerable_(buf, len, "MetadataRequest for unknown session");
        return;
    }

    // -----------------------------------------------------------------------
    // Build BrokerEntry list from nodeEndpointMap_.
    //
    // Each value in nodeEndpointMap_ is an Aeron channel string of the form
    //   "aeron:udp?endpoint=host:port"
    // We parse out "host" and "port" for the wire encoding.
    // A missing or unparseable entry is included as port=0 + empty host
    // (best-effort; the client can still route via the endpoint string).
    // -----------------------------------------------------------------------
    constexpr std::size_t kMaxBrokers = 32;
    static_assert(kMaxBrokers <= 256);
    std::vector<pc::BrokerEntry> brokers;
    brokers.reserve(nodeEndpointMap_.size());

    // We keep the parsed host strings alive for the encode pass.
    std::vector<std::string> hostStore;
    hostStore.reserve(nodeEndpointMap_.size());

    for (const auto& [nodeId, channel] : nodeEndpointMap_) {
        pc::BrokerEntry b;
        b.nodeId = nodeId;

        // Parse "aeron:udp?endpoint=host:port" → host, port.
        // We look for "endpoint=" and then split on the last ':'.
        int32_t port = 0;
        std::string host;
        const auto epPos = channel.find("endpoint=");
        if (epPos != std::string::npos) {
            const std::string_view ep(channel.c_str() + epPos + 9,
                                       channel.size() - epPos - 9);
            const auto colonPos = ep.rfind(':');
            if (colonPos != std::string_view::npos) {
                host = std::string(ep.substr(0, colonPos));
                try {
                    port = std::stoi(std::string(ep.substr(colonPos + 1)));
                } catch (...) {
                    port = 0;
                }
            } else {
                host = std::string(ep);
            }
        } else {
            host = channel;  // store whole channel string as fallback
        }
        b.port = port;
        hostStore.push_back(std::move(host));
        b.host = hostStore.back();  // string_view into hostStore
        brokers.push_back(b);
    }

    // -----------------------------------------------------------------------
    // Build PartitionEntry list from routingImage_.
    //
    // The GPID scheme used by BrokerNode/MetadataService:
    //   gpid = gpidOf(topicId, partition)  [= topicId * kMaxGpidPartitions + partition]
    // We iterate all capacity slots and invert:
    //   topicId   = gpid / kMaxGpidPartitions
    //   partition = gpid % kMaxGpidPartitions
    //
    // If topicRegistry_ is set, we only include partitions whose topicId is in
    // range [0, topicRegistry_->topicCount()) and whose partition is below that
    // topic's registered partitionCount.  This prevents ghost partitions from a
    // stale routing image that refers to topics/partitions no longer registered.
    //
    // If the request carries specific topicIds (topicIds non-empty), filter
    // to only those topics.
    // -----------------------------------------------------------------------
    using rune::metadata::kMaxGpidPartitions;
    // Stable storage for the per-partition ingressEndpoint views (deque: element
    // addresses never move).  Control path (Metadata), not the per-record path.
    std::deque<std::string> endpointStore;
    std::vector<pc::PartitionEntry> parts;

    const PermissionTable& perms = permsOf_(*session);
    const bool filterPerms = !perms.isAllowAll();
    if (routingImage_) {
        const rune::metadata::RoutingImage* img = routingImage_.get();
        const int32_t cap = img->capacity();
        parts.reserve(std::min(cap, 4096));

        // Build a quick lookup set of requested topicIds (empty = all).
        // After the MISALIGNMENT FIX in MetadataMessages.h (decode() now copies
        // each int32 via getLE32 into an owned, naturally-aligned vector),
        // req.topicIds elements are already correct native int32 values — no
        // reinterpret_cast workaround needed here.
        const bool filterTopics = !req.topicIds.empty();
        std::unordered_set<int32_t> requestedTopics;
        if (filterTopics) {
            for (const int32_t topicId : req.topicIds) {
                requestedTopics.insert(topicId);
            }
        }

        for (int32_t gpid = 0; gpid < cap; ++gpid) {
            const auto& entry = img->lookup(gpid);
            if (entry.leaderNodeId == rune::metadata::RoutingImage::NO_LEADER_NODE_ID) {
                continue;  // unassigned — skip
            }
            const int32_t srcTopicId = gpid / kMaxGpidPartitions;
            const int32_t partition  = gpid % kMaxGpidPartitions;
            // VLS B.2: alias 0 is the topic itself; alias n > 0 is its (n-1)-th view sink, whose per-partition leader
            // is SYNTHESISED from the source's entry (the sink owns no assignment) -- GetViewRow routing is unchanged.
            for (std::size_t alias = 0;; ++alias) {
            int32_t topicId = srcTopicId;
            if (alias > 0) {
                topicId = nthViewSinkOfSource_(srcTopicId, alias - 1);
                if (topicId < 0) break;
            }

            // Filter: known topics only (when topicRegistry_ is wired) — and only
            // the partitions the registry has: the replicated image can still
            // assign a partition the registry no longer holds (DDL conformance
            // D1: a ZERO-partition __orphan.<id> placeholder recovered at boot
            // keeps its pre-restart assignment in __cluster_metadata).
            if (topicRegistry_) {
                // M1: a source that fails a registry filter lists neither itself nor its sinks (alias 0 ends the loop).
                if (topicId >= topicRegistry_->topicCount()) { if (alias == 0) break; continue; }
                const rune::node::TopicSpec* spec = alias > 0 ? topicRegistry_->viewSinkSpec(topicId)
                                                              : topicRegistry_->topic(topicId);
                if (spec == nullptr || partition >= spec->partitionCount) { if (alias == 0) break; continue; }
                // Task 8 (C1 generalized): a catalog topic is listed once its
                // placement is committed (the bootstrap image is not committed).
                if (topicRegistry_->placementGated(topicId) && !img->committed()) { if (alias == 0) break; continue; }
            }

            // Filter: requested topics only (when non-empty topic list in request).
            if (filterTopics && requestedTopics.find(topicId) == requestedTopics.end()) {
                continue;
            }
            // Authentication Task 12 (visibility): only partitions of topics the session may DESCRIBE are listed
            // (an explicit id list is filtered the same way - a hidden id is simply absent).
            if (filterPerms && !perms.allows(topicId, rune::security::opBit(rune::security::Operation::Describe))) continue;

            pc::PartitionEntry p;
            p.topicId     = topicId;
            p.partition   = partition;
            p.leaderId    = entry.leaderNodeId;
            p.leaderEpoch = entry.leaderEpoch;
            // E5.1: per-worker ingress routing — ingressStreamId = base + workerForPartition.
            // CRITICAL: only advertise a per-worker stream when per-worker ingress is
            // ACTUALLY ACTIVE (perWorkerIngressActive_). Otherwise the broker has NO
            // subscriber on the per-worker stream (only the gateway on clientStreamId),
            // so a client routing there hits NOT_CONNECTED and can't send consistently.
            //
            // When per-worker ingress is INACTIVE (gateway-fanout mode), advertise the
            // gateway's OWN client ingress stream (clientStreamId_) — NOT 0.  Every
            // node's gateway subscribes on clientStreamId_, so a client that opens a
            // per-partition session to the true leader endpoint on this stream lands
            // directly on that leader's gateway.  Advertising 0 here disabled client
            // per-partition routing entirely (the client's route builder skips
            // ingressStreamId==0), which — once partition leadership legitimately
            // SPREADS across nodes after failover — forced the client onto a single
            // shared transport that ping-ponged between two partition leaders on
            // consecutive records and exhausted its redirect budget (dr3 rolling
            // leader-kill: produced 44/50).  clientStreamId_ has a live subscriber on
            // every node, so it is always a safe, consistent routing target.
            //
            // F25 — shard mode (spec §2/§3.3): advertise clientStreamId_.  The
            // node endpoint's gateway subscribes it (→ worker op ring →
            // executeProduceWire_) AND every shard's WorkerIngress subscribes it
            // on its own port, so BOTH routes a client can derive are live: an
            // ingressEndpoint-aware client sends (endpoint, stream) straight to
            // the owning worker; one that ignores the trailer sends (node
            // endpoint, stream) to the gateway.  base + owner was subscribed only
            // on the worker port → the latter hit NOT_CONNECTED forever.
            p.ingressStreamId = shardMode_ ? clientStreamId_
                              : perWorkerIngressActive_
                              ? (workerIngressBaseStreamId_
                                 + (workerPool_
                                    ? workerPool_->ownerWorkerId(srcTopicId, partition)
                                    : 0))
                              : clientStreamId_;
            // read-rework T3/T4 (I4): advertise the per-worker dynamic-MDC egress
            // endpoint so the client subscribes to the OWNING worker's control
            // address + stream to receive this partition's pushed Records.  Control
            // host = the leader BrokerEntry.host (client-side).  0 when inactive →
            // client falls back to the legacy per-session egress.
            if (perWorkerEgressActive_ && workerPool_) {
                const int32_t wid = workerPool_->ownerWorkerId(srcTopicId, partition);
                p.egressControlPort = workerEgressControlBasePort_ + wid;
                p.egressStreamId    = workerEgressBaseStreamId_ + wid;
            }
            // Shard mode (spec §3.3): advertise the LEADER's owning-worker ingress
            // endpoint from explicit configuration (worker index is symmetric across
            // nodes); no entry / unadvertised base → empty → node endpoint.
            if (!shardIngressEndpoints_.empty() && workerPool_) {
                for (const auto& ep : shardIngressEndpoints_) {
                    if (ep.nodeId != entry.leaderNodeId) continue;
                    // The client dials the egress control on the LEADER's host:
                    // advertise the LEADER's base, not this (answering) node's
                    // (per-node bases differ — review B1, 2026-10-02).
                    if (perWorkerEgressActive_ && ep.egressControlBasePort > 0)
                        p.egressControlPort = ep.egressControlBasePort + workerPool_->ownerWorkerId(srcTopicId, partition);
                    if (ep.basePort > 0 && !ep.host.empty()) {
                        endpointStore.push_back(ep.host + ":" +
                            std::to_string(ep.basePort + workerPool_->ownerWorkerId(srcTopicId, partition)));
                        p.ingressEndpoint = endpointStore.back();
                    }
                    break;
                }
            }
            // Issue #116: the receiver a client seals a produce for when it uses the advertised worker route - the
            // owning worker w (w+1) when that route reaches the worker itself (its own ingress endpoint in shard
            // mode, or its per-worker stream base + w on this node); 0 = the gateway's own stream.  Sent only to
            // sessions >= 12 (MetadataResponse::clientVersion below).
            if (workerPool_ && (!p.ingressEndpoint.empty() || (!shardMode_ && perWorkerIngressActive_)))
                p.ingressReceiverId = static_cast<uint8_t>(workerPool_->ownerWorkerId(srcTopicId, partition) + 1);
            parts.push_back(p);
            }   // alias
        }
    }

    // -----------------------------------------------------------------------
    // Encode MetadataResponse into scratch buffer.
    // -----------------------------------------------------------------------
    pc::MetadataResponse resp;
    resp.sessionId     = req.sessionId;
    resp.correlationId = req.correlationId;
    resp.brokers       = std::span<const pc::BrokerEntry>(brokers.data(), brokers.size());
    resp.partitions    = std::span<const pc::PartitionEntry>(parts.data(), parts.size());
    resp.clientVersion = session->clientVersion;   // issue #116: the receiver trailer only for sessions >= 12
    if (preReceiverKeyMetadataForTest_) [[unlikely]]   // test seam: an old broker's answer (no receiver trailer)
        resp.clientVersion = std::min(resp.clientVersion, pc::kClientVersionReceiverKeys - 1);

    const std::size_t needed = resp.encodedSize();
    if (needed <= scratch_.size()) {
        const std::size_t encoded = resp.encode(scratch_.data());
        // No-hang (R2): queued under back-pressure (best-effort only above a ring slot).
        sendAnswer_(*session, scratch_.data(), encoded);
    } else {
        spdlog::warn("ClientGateway: MetadataResponse too large ({} bytes); scratch={} — answered "
                     "MessageTooLarge", needed, scratch_.size());
        (void)rejectRequest_(buf, len, static_cast<uint16_t>(pc::ErrorCode::MessageTooLarge),
                             "MetadataResponse exceeds scratch");
    }
}

} // namespace rune::gateway
