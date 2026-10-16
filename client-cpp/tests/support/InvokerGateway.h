// tests/support/InvokerGateway.h
//
// Single-threaded fake broker for invoker-mode tests.  Its Aeron client runs in
// invoker mode and attaches to the CLIENT's embedded driver (same aeron dir), so
// the test thread drives everything by alternating client/agent doWork() and
// gw.pump().  No sleeps, no threads.  Endpoint 0 is the node (control) endpoint;
// addEndpoint() adds worker endpoints, each with its OWN ack publication on the
// client's egress channel (shard-mode brokers ack from a different publication).
#pragma once

#include "AeronTestErrors.h"
#include <rune/client/codec/Codec.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/TxnMessages.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <Subscription.h>

#include <cstdint>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace test_support {

struct InvokerGateway {
    struct PartitionAd {
        int32_t     topicId         = 0;
        int32_t     partition       = 0;
        int32_t     leaderId        = 0;
        int32_t     leaderEpoch     = 1;
        int32_t     ingressStreamId = 0;
        std::string ingressEndpoint;  // "host:port" of the owning worker; "" = node (Task 16)
    };
    struct Produced {
        int32_t topicId;
        int32_t partition;
        int64_t sessionId;
        int64_t corrId;
        int32_t records;
        int32_t baseSequence;
        int     endpoint;  // index of the endpoint that received it (0 = node)
        int16_t producerEpoch = -1;  // the frame's producer epoch (Task 16 fix round 1)
    };
    struct Endpoint {
        std::string channel;
        int32_t     stream = 0;
        int64_t     subReg = -1;
        int64_t     pubReg = -1;
        std::shared_ptr<aeron::Subscription>         sub;
        std::shared_ptr<aeron::ExclusivePublication> pub;  // acks/replies to the client egress
        std::unique_ptr<aeron::FragmentAssembler>    assembler;
    };

    std::string                            egressChannel;
    int32_t                                egressStreamId = 0;
    std::shared_ptr<aeron::Aeron>          aeron;
    std::vector<std::unique_ptr<Endpoint>> endpoints;
    std::vector<std::pair<int, std::vector<uint8_t>>> outbox;  // (endpoint, frame) awaiting a connected pub
    std::vector<PartitionAd> partitions;
    std::string brokerHost;           // "" = advertise no broker entry (routes fall back to the seed)
    int32_t     brokerPort = 0;
    // Re-review 1 (NEW-1): more advertised brokers (nodeId, host, port) besides node 0;
    // holdMetadata: MetadataRequests are counted (corrIds kept), never answered —
    // the test sends each MetadataResponse itself (sendMetadata), in any order.
    struct BrokerAd { int32_t nodeId = 1; std::string host; int32_t port = 0; };
    std::vector<BrokerAd> moreBrokers;
    bool                  holdMetadata = false;
    std::vector<int64_t>  metadataCorrIds;  // correlationIds of the MetadataRequests received
    int64_t     assignedSessionId = 42;
    int64_t     nextOffset = 100;
    bool        replyAcks = true;
    std::set<int32_t>     holdPartitions;  // partitions whose acks are held
    // Task 16 fix round 1: a worker that no longer owns its partitions — every
    // Produce arriving on these endpoints is answered ProduceResponse{NotLeader}
    // (the broker's refuseNotOwned_ / moved-replica answer).
    std::set<int>     notLeaderEndpoints;
    int               notLeaderAnswers = 0;
    // The next N Produce frames (any endpoint) are answered with a bare
    // ErrorResponse{NotLeader, ""} / an ErrorResponse{Redirect, redirectTo}.
    int               errorNotLeaderNext = 0;
    int               redirectNext = 0;
    std::string       redirectTo;
    // The next N Subscribes answer ErrorResponse{Redirect, subscribeRedirectTo}
    // (any endpoint — e.g. a stopped node's — the client must survive it).
    int               subscribeRedirectNext = 0;
    std::string       subscribeRedirectTo;
    std::vector<Produced> produced;
    std::vector<Produced> held;
    // Acks sent immediately BEFORE the next ConnectResponse, from the same
    // publication, so the client receives [ack..., ConnectResponse] in one poll.
    std::vector<Produced> ackBeforeNextConnect;
    // F8a fix4: Records pushed immediately AFTER the next ConnectResponse, from
    // the same publication, so the client receives [ConnectResponse, Records...]
    // in one poll.  Element: (subscription, baseOffset).
    int connects = 0;
    int metadataRequests = 0;
    std::vector<int> metadataEndpoints;   // the endpoint each MetadataRequest arrived on
    // DR fix round 1: per-endpoint routing image (I4: a lagging node keeps naming the
    // dead leader) and per-endpoint Connect refusal (I2: ConnectResponse{code}).
    std::map<int, std::vector<PartitionAd>> partitionsAt;
    std::map<int, int16_t>                  refuseConnectsWith;
    int connectsArrived = 0;   // every ConnectRequest that arrived anywhere, dead endpoints included
    // F8a: direct subscriptions (Subscribe -> SubscribeResponse; Records pushes).
    struct Subscribed {
        int64_t subscriptionId;
        int32_t topicId;
        int32_t partition;
        int64_t sessionId;
        int     endpoint   = 0;   // index of the endpoint that received the Subscribe
        int64_t fromOffset = 0;   // the Subscribe's wire fromOffset
        int64_t corrId     = 0;
    };
    std::vector<Subscribed> subscribed;
    // E2 round 6 (F1): a TRANSIENT partition's current data lineage (partition ->
    // token).  A Subscribe carrying a lineage CLAIM (partitionKnown, 8-byte key)
    // for a partition listed here with another token is answered
    // ErrorResponse{PartitionDataReset} (43), as the broker does.
    std::map<int32_t, uint64_t> partitionLineage;
    std::vector<uint64_t>       lineageClaims;   // every claim received (0 = none)
    int                         dataResetAnswers = 0;
    std::vector<int64_t>    unsubscribed;  // subscriptionIds
    std::vector<int>        unsubscribedAt;  // the endpoint each Unsubscribe arrived on
    // Partition leadership for Subscribe (the broker's per-partition routing
    // image): partition -> the endpoint index that leads it.  A Subscribe for a
    // listed partition arriving on any OTHER endpoint registers nothing and is
    // answered ErrorResponse{Redirect, "host:port" of the leader endpoint} (or a
    // bare ErrorResponse{NotLeader} when subscribeNotLeader).  Unlisted
    // partitions are served anywhere (the historical gateway).
    std::map<int32_t, int> subscribeLeader;
    // A real broker answers a session on that session's own node: Connect /
    // Subscribe / Ping answers (and pushRecords) leave from the endpoint the
    // request arrived on (default: endpoint 0, the historical gateway).
    bool replyFromArrival = false;
    // Subscription ids are allocated per endpoint (each node's own counter
    // starting at nextSubscriptionId) — they collide across nodes like a real
    // cluster's (default: one counter).
    bool perEndpointSubIds = false;
    std::map<int, int64_t> endpointSubIds;
    // Endpoints that are dead: every frame arriving there is ignored.
    std::set<int> deadEndpoints;
    // The next N OffsetFetch answers carry an error code on every entry.
    int offsetFetchErrorsNext = 0;
    // Records pushes carry the creating Subscribe's correlationId (the broker
    // since client/broker version 3); false = an older broker (0).
    bool stampSubscribeCorr = true;
    // The next N SubscribeResponses are lost (the subscription IS registered).
    int dropSubscribeResponsesNext = 0;
    // JoinGroup answers carry this error code (0 = the normal answer).
    int16_t joinErrorCode = 0;
    // No-hang fix round 2: SyncGroup / Heartbeat answers carry these codes
    // (0 = normal); OffsetFetch: the code of offsetFetchErrorsNext answers, and
    // the next N OffsetFetch answers are the broker's whole-request rejection
    // {topicId -1, partition -1, offsetFetchRejectCode}.
    int16_t syncErrorCode        = 0;
    int16_t heartbeatErrorCode   = 0;
    int16_t offsetFetchErrorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator);
    int     offsetFetchRejectNext = 0;
    int16_t offsetFetchRejectCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::InvalidRequest);
    // ConnectResponse.brokerVersion advertised (-1 = an older broker's 38-byte frame).
    int32_t advertisedBrokerVersion = rune::protocol::client::kBrokerVersion;
    std::map<int, int32_t> advertisedBrokerVersionAt;  // per endpoint (a rolling upgrade)
    // OffsetFetch answers are held until releaseOffsetFetches().
    bool holdOffsetFetch = false;
    std::vector<std::vector<uint8_t>> heldOffsetFetches;
    void releaseOffsetFetches() {
        std::vector<std::vector<uint8_t>> h;
        h.swap(heldOffsetFetches);
        for (const auto& f : h) send(0, f.data(), f.size());
    }
    // ConnectRequests received per endpoint (answered or not).
    std::map<int, int> connectsAt;
    std::vector<int64_t> connectCorrIds;  // D4: the correlationId of every ConnectRequest
    bool                   subscribeNotLeader = false;
    int                    subscribeRefusals  = 0;
    // Consumer-group coordinator model (off by default = the historical gateway,
    // which never answers group frames).  JoinGroup -> JoinGroupResponse{OK,
    // "member-1", groupGeneration} (held in pendingJoins while holdJoins);
    // SyncGroup -> SyncGroupResponse{OK, groupAssignment}; Heartbeat -> OK;
    // OffsetFetch -> OffsetFetchResponse with committedOffsets (-1 = none).
    bool groupProtocol = false;
    bool holdJoins     = false;
    std::vector<std::pair<int32_t, int32_t>>          groupAssignment;   // (topicId, partition)
    std::map<std::pair<int32_t, int32_t>, int64_t>    committedOffsets;  // (topicId, partition) -> offset
    int32_t groupGeneration = 1;
    int     joinGroups = 0, syncGroups = 0, heartbeats = 0, offsetFetches = 0;
    std::vector<int64_t> groupSessionIds;  // sessionId of every group frame received
    std::vector<int>     groupEndpoints;   // the endpoint every group frame arrived on
    // The node that is the group coordinator (-1 = any node answers).  A
    // JoinGroup / SyncGroup / Heartbeat arriving on another endpoint is answered
    // NotCoordinator (the broker's isLocalCoordinator() fence).
    int  coordinatorEndpoint = -1;
    int  notCoordinatorAnswers = 0;
    // D4 (broker 2026-10-08): a follower answers a coordinator request with
    // ErrorResponse{NotCoordinator, coordinator endpoint} ("host:port") when it
    // knows the coordinator.  Empty = the typed NotCoordinator answer.
    std::string coordinatorRedirectTo;
    // D4: the endpoint every txn COORDINATOR request (InitProducerId /
    // AddPartitionsToTxn / EndTxn) arrived on; with coordinatorRedirectTo set, one
    // arriving on a node that is not coordinatorEndpoint is answered
    // ErrorResponse{NotCoordinator, coordinatorRedirectTo, its corrId}.
    std::vector<int> coordinatorEndpoints;
    /// Catalog history (Task 5, F7): per-template frame handlers, consulted first
    /// in onFrame (after the dead-endpoint check).  `endpoint` is the index of
    /// the endpoint the frame arrived on; the handler answers through send() /
    /// sendError() / sendDdlResponse().
    std::map<uint16_t, std::function<void(int endpoint, const uint8_t*, std::size_t)>> templateHandlers;
    /// A DdlResponse (78) from endpoint `from`.
    void sendDdlResponse(int from, const rune::protocol::client::DdlResponse& r) {
        std::vector<uint8_t> out(r.encodedSize());
        const std::size_t n = r.encode(out.data());
        send(from, out.data(), n);
    }
    /// An ErrorResponse from endpoint `from` (a late / unsolicited answer).
    void sendError(int from, int16_t code, const std::string& message, int64_t sessionId, int64_t corrId) {
        namespace rcc = rune::client::codec;
        std::vector<uint8_t> e(256 + message.size());
        const std::size_t en = rcc::encodeErrorResponse(e.data(), e.size(), code, message, sessionId, corrId);
        send(from, e.data(), en);
    }
    // A rebalance in progress: Heartbeats are answered RebalanceInProgress
    // until the next JoinGroup (which completes it at groupGeneration).
    bool rebalancing = false;
    /// The coordinator starts generation `gen`: an empty Assign{gen} (the eager
    /// revoke) to `sessionId`, then every Heartbeat answers RebalanceInProgress
    /// until the member re-joins.
    void startRebalance(int32_t gen, int64_t sessionId) {
        namespace rp = rune::protocol::client;
        groupGeneration = gen;
        rebalancing     = true;
        rp::Assign a;
        a.sessionId     = sessionId;
        a.correlationId = 0;
        a.generationId  = gen;
        std::vector<uint8_t> out(rp::Assign::encodedSizeFor(0));
        const std::size_t n = a.encode(out.data(), std::span<const rp::TopicPartition32>{});
        send(0, out.data(), n);
    }
    std::vector<int64_t> pendingJoins;     // sessionIds of held JoinGroups
    /// Answer every held JoinGroup now.
    void releaseJoins() {
        std::vector<int64_t> p;
        p.swap(pendingJoins);
        for (const int64_t sid : p) answerJoin_(sid);
    }
    /// The latest subscription for (topicId, partition), or nullptr.
    [[nodiscard]] const Subscribed* latestSubscription(int32_t topicId, int32_t partition) const {
        for (auto it = subscribed.rbegin(); it != subscribed.rend(); ++it) {
            if (it->topicId == topicId && it->partition == partition) return &*it;
        }
        return nullptr;
    }
    std::vector<std::pair<Subscribed, int64_t>> recordsAfterNextConnect;
    int64_t nextSubscriptionId = 500;
    // F8a fix2: stop consuming client ingress (the client's publication then
    // back-pressures once its term window is full).
    bool pauseIngress = false;
    int  pings        = 0;
    std::vector<std::vector<uint8_t>> offsetCommits;  // raw OffsetCommit frames received
    bool replyOffsetCommits = true;
    int16_t offsetCommitErrorCode = 0;   // != 0: the broker's rejection {-1, -1, code}
    // F8c C3: control-op replies (all off by default = the historical gateway).
    bool    replyConnects   = true;   // false: ConnectRequests are counted, never answered
    bool    replyPongs      = false;  // Ping -> Pong{echo}
    int16_t pingErrorCode   = 0;      // != 0: Ping -> ErrorResponse{code, the ping's corrId}
    std::vector<int64_t> pingCorrIds; // correlationIds of the Pings received
    bool    replySchemas    = false;  // DefineTopic / GetTopicSchema -> their responses
    int32_t schemaTopicId   = 7;
    uint16_t schemaVersion  = 1;
    uint16_t schemaErrorCode = 0;
    std::vector<uint8_t> schemaBlob;  // the GetTopicSchema descriptor blob
    int defines = 0;
    int schemaGets = 0;
    std::vector<int64_t> subscribeCorrIds;  // correlationIds of the Subscribes received
    // F8c C5 / F14: transactional requests.  Counted always; InitProducerId is
    // answered when replyInitPid: the next code of initPidCodes (popped), or
    // OK{initPid, epoch 0} once it is empty.  Others are never answered.
    bool                initPidReply = false;
    std::deque<int16_t> initPidCodes;
    int64_t             initPid = 4242;
    int initPids = 0, addPartitions = 0, addOffsets = 0, txnOffsetCommits = 0, endTxns = 0;
    // C10: the sessionId stamped on every txn COORDINATOR request received
    // (InitProducerId / AddPartitionsToTxn / EndTxn) — which client session
    // carried it (the seed session = the coordinator's, vs a partition leader's).
    std::vector<int64_t> coordinatorSessionIds;
    // F8c C7: answer AddPartitionsToTxn and EndTxn OK (txn close tests); the
    // commit flag of every EndTxn received (1 commit / 0 abort).
    bool                 replyTxn = false;
    std::vector<uint8_t> endTxnCommits;
    // T15b fix round 1 (shard lifecycle / gates):
    std::set<int> silentEndpoints;   // ConnectRequests arriving on these endpoints are never answered
    int16_t       connectErrorCode = 0;  // != 0: every ConnectRequest -> ConnectResponse{code}
    int           framesReceived   = 0;  // every frame from the client, any template
    bool          uniqueSessionIds = false;  // each fresh connect (sessionId 0) gets its own id
    // A slow broker: ConnectResponses go out this long after the request (real
    // time — the stimulus of a test about the caller's own wait budget).
    std::chrono::milliseconds connectReplyDelay{0};
    // Replies (ConnectResponse, ProduceResponse, ...) of a session go to the
    // egress channel/stream ITS ConnectRequest announced (a real broker's rule),
    // not the fixed constructor egress.  acksPerEgress counts ProduceResponses
    // per announced egress channel.
    bool                       replyOnAnnouncedEgress = false;
    std::map<std::string, int> acksPerEgress;
    // Record values per partition, in arrival order (keepValues).
    bool                                                   keepValues = false;
    std::map<int32_t, std::vector<std::vector<uint8_t>>> values;

    // ---- EOS-OOS: broker idempotence model + fault injection (off by default) ----
    // idempotence: Produce frames with producerId >= 0 are validated like the
    // broker's ProducerStateManager (keyed on the FRAME identity) plus the
    // EOS-OOS rulings: trust-on-first-use at seq 0; a lower epoch -> fenced; a
    // higher epoch only at seq 0; an exact retry inside the last
    // `idempotenceWindow` batches -> OK with its original offset; a batch
    // entirely <= lastSequence but outside the window -> OK with baseOffset -1
    // (known-old duplicate); otherwise a gap -> OutOfOrderSequence.  Only
    // accepted batches reach `log` (what a reader would see).  Held / paused
    // acks (replyAcks / holdPartitions) are not modelled on this path.
    bool idempotence       = false;
    int  idempotenceWindow = rune::protocol::client::kIdempotentMaxInFlightBatches;  // the broker's window
    // Fault injection by the 1-based ordinal of the Produce frames received:
    // dropProduce — the frame is lost on the wire (never seen);
    // dropAnswer  — processed, but its ProduceResponse is lost;
    // errorFor    — NOT processed; answered with this error code instead;
    // holdAnswer  — processed; its answer is held until releaseAnswer(ordinal);
    // answerDelay — processed; its answer goes out this many pump() calls later.
    // dropIf      — every frame it returns true for is lost (all attempts).
    std::set<int>           dropProduce;
    std::set<int>           dropAnswer;
    std::map<int, int16_t>  errorFor;
    std::set<int>           holdAnswer;
    std::map<int, int>      answerDelay;
    std::function<bool(int32_t partition, int16_t epoch, int32_t baseSequence)> dropIf;
    // errorIf — every frame it returns != 0 for is NOT processed and answered
    // with that error (all attempts: e.g. a batch the broker keeps rejecting).
    std::function<int16_t(int32_t partition, int16_t epoch, int32_t baseSequence)> errorIf;
    // EOS-OOS M-3 (acks=All emulation): an OK answer (incl. duplicates) to a
    // frame with acks=All goes out okDelayPumps pump() calls later — like the
    // broker's deferred acks=all answers — while error answers go out at once,
    // so later errors can overtake earlier OKs.
    int           okDelayPumps  = 0;
    int           produceFrames = 0;
    struct LogBatch {
        int32_t topicId = 0;
        int32_t partition = 0;
        int64_t producerId = -1;
        int16_t producerEpoch = -1;
        int32_t baseSequence = -1;
        int64_t baseOffset = 0;
        std::vector<std::vector<uint8_t>> values;
    };
    std::vector<LogBatch> log;
    // ---- D6: UnknownProducerId (Kafka UNKNOWN_PRODUCER_ID) ----
    // The model answers a Produce with no producer entry and baseSequence != 0
    // UnknownProducerId + logStartOffset — to a session whose ConnectRequest
    // announced clientVersion >= 2 (kClientVersionUnknownProducerId); older
    // sessions get OutOfOrderSequence (the real broker's version gate).
    int64_t logStartOffset = 0;                  // the model log's start (retention moves it)
    std::map<int64_t, int32_t> clientVersions;   // sessionId -> ConnectRequest.clientVersion
    int32_t lastConnectClientVersion = -1;
    int unknownPidAnswers = 0;
    /// Retention + failover: drop every log batch wholly below `newLogStart`,
    /// then REBUILD the producer state from what is left (a new leader / a
    /// restart scans [logStart, LEO)) — producers whose batches were all
    /// removed are forgotten.
    void retainAndRebuild(int64_t newLogStart) {
        std::vector<LogBatch> kept;
        for (auto& b : log) {
            const int64_t last = b.baseOffset + static_cast<int64_t>(b.values.size()) - 1;
            if (last >= newLogStart) kept.push_back(std::move(b));
        }
        log            = std::move(kept);
        logStartOffset = std::max(logStartOffset, newLogStart);
        seqStates.clear();
        for (const auto& b : log) {
            if (b.producerId < 0) continue;
            SeqState& st = seqStates[std::make_tuple(b.topicId, b.producerId, b.partition)];
            const int32_t lastSeq = b.baseSequence + static_cast<int32_t>(b.values.size()) - 1;
            if (b.producerEpoch > st.epoch) { st.window.clear(); }
            st.epoch   = std::max(st.epoch, b.producerEpoch);
            st.lastSeq = lastSeq;
            st.window.push_back({b.baseSequence, lastSeq, b.baseOffset});
            while (static_cast<int>(st.window.size()) > idempotenceWindow) st.window.pop_front();
        }
    }
    /// Failover WITHOUT retention that lost the producer state anyway (e.g. an
    /// unclean leader whose log lacks the producer's acked batches).
    void forgetProducers() { seqStates.clear(); }
    [[nodiscard]] std::size_t knownProducers() const { return seqStates.size(); }
    int oosAnswers = 0, windowDupAnswers = 0, oldDupAnswers = 0, fencedAnswers = 0;
    // Re-review 1 (NEW-2): the broker's ProducePath step 4 (RecordBatch CRC32C ->
    // Internal) and step 4b (the Produce FRAME's producerId / epoch / baseSequence
    // must equal the CRC-covered batch HEADER's -> SchemaValidationFailed).  Every
    // Produce frame of the idempotence model is checked, before anything else
    // (fault injection, NotLeader, dedup): a failed frame is never appended.  Both
    // counters stay 0 for a correct client — re-stamped frames included.
    int crcFailures = 0, identityMismatches = 0;
    /// Feeds one raw client frame to the gateway as if it arrived on `endpoint`
    /// (tests of the broker model itself).
    void injectFrame(int endpoint, const uint8_t* buf, std::size_t len) { onFrame(endpoint, buf, len); }
    /// The broker already holds (producerId, epoch) on (topicId, partition) up to
    /// `lastSeq` (an empty dedup window) — a test reaches high sequences without
    /// producing 2^31 records.
    void seedSequence(int32_t topicId, int64_t producerId, int32_t partition, int16_t epoch, int32_t lastSeq) {
        SeqState& st = seqStates[std::make_tuple(topicId, producerId, partition)];
        st.epoch   = epoch;
        st.lastSeq = lastSeq;
        st.window.clear();
    }
    /// Task 16 fix round 1 (the broker's moved-replica answer): every pending
    /// (deferred acks=all / held) answer to a frame received on `endpoint` —
    /// the frame IS appended — goes out NOW as `ec` (e.g. NotLeader).
    void failPendingAt(int endpoint, int16_t ec) {
        for (auto& pa : pendingAnswers_) {
            if (pa.pr.endpoint != endpoint) continue;
            pa.ec    = ec;
            pa.off   = -1;
            pa.held  = false;
            pa.dueAt = 0;
        }
        flushAnswers_();
    }
    /// The answers awaiting their pump (delayed / held), by Produce ordinal.
    [[nodiscard]] std::size_t pendingAnswers() const { return pendingAnswers_.size(); }
    /// Sends a held answer now (no-op when not held).
    void releaseAnswer(int ordinal) {
        for (auto& pa : pendingAnswers_) {
            if (pa.ordinal == ordinal && pa.held) {
                pa.held  = false;
                pa.dueAt = 0;
            }
        }
        flushAnswers_();
    }

    InvokerGateway(const std::string& aeronDir, const std::string& controlChannel,
                   int32_t controlStream, std::string egressCh, int32_t egressStream,
                   std::string endpoint0PubChannel = {})
        : egressChannel(std::move(egressCh)), egressStreamId(egressStream) {
        nextPubChannel = std::move(endpoint0PubChannel);
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        ctx.useConductorAgentInvoker(true);
        ctx.errorHandler(test_support::countAeronError);
        aeron = aeron::Aeron::connect(ctx);
        addEndpoint(controlChannel, controlStream);
    }

    ~InvokerGateway() {
        endpoints.clear();
        sessionPubs.clear();  // publications before their Aeron client
        aeron.reset();
    }

    // The publication channel of the NEXT addEndpoint's replies (empty = the
    // egress channel): e.g. the egress channel with its own `interface=` port,
    // so each fake node is a distinct SOURCE (Image::sourceIdentity) like real
    // broker nodes on distinct hosts.
    std::string nextPubChannel;
    int addEndpoint(const std::string& channel, int32_t stream) {
        auto ep = std::make_unique<Endpoint>();
        ep->channel = channel;
        ep->stream  = stream;
        ep->subReg  = aeron->addSubscription(channel, stream);
        const std::string pubCh = nextPubChannel.empty() ? egressChannel : nextPubChannel;
        nextPubChannel.clear();
        ep->pubReg  = aeron->addExclusivePublication(pubCh, egressStreamId);
        const int index = static_cast<int>(endpoints.size());
        ep->assembler = std::make_unique<aeron::FragmentAssembler>(
            [this, index](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                          aeron::util::index_t len, aeron::Header&) {
                onFrame(index, reinterpret_cast<const uint8_t*>(b.buffer() + off),
                        static_cast<std::size_t>(len));
            });
        endpoints.push_back(std::move(ep));
        return index;
    }

    [[nodiscard]] bool ready() const {
        for (const auto& ep : endpoints) {
            if (!ep->sub || !ep->pub) return false;
        }
        return true;
    }

    /// ErrorResponse{SessionExpired} to `sessionId` (the broker expired it).
    void expireSession(int64_t sessionId) {
        namespace rcc = rune::client::codec;
        std::vector<uint8_t> out(128);
        const std::size_t    n = rcc::encodeErrorResponse(
            out.data(), out.size(),
            static_cast<int16_t>(rune::protocol::client::ErrorCode::SessionExpired), "expired", sessionId, 0);
        sendToSession(sessionId, out.data(), n);
    }

    /// One gateway duty cycle: conductor, registrations, outbox, polls.
    int pump() {
        ++pumps_;
        flushAnswers_();
        int work = aeron->conductorAgentInvoker().invoke();
        for (auto& ep : endpoints) {
            if (!ep->sub) ep->sub = aeron->findSubscription(ep->subReg);
            if (!ep->pub) ep->pub = aeron->findExclusivePublication(ep->pubReg);
        }
        for (auto& [sid, sp] : sessionPubs) {
            if (!sp.pub) sp.pub = aeron->findExclusivePublication(sp.reg);
        }
        for (std::size_t i = 0; i < delayedConnects.size();) {
            if (std::chrono::steady_clock::now() >= delayedConnects[i].first) {
                const std::vector<uint8_t> f = std::move(delayedConnects[i].second);
                delayedConnects.erase(delayedConnects.begin() + static_cast<std::ptrdiff_t>(i));
                onFrame(0, f.data(), f.size(), /*replay=*/true);
                ++work;
            } else {
                ++i;
            }
        }
        for (std::size_t i = 0; i < sessionOutbox.size();) {
            if (offerSession(sessionOutbox[i].first, sessionOutbox[i].second)) {
                sessionOutbox.erase(sessionOutbox.begin() + static_cast<std::ptrdiff_t>(i));
                ++work;
            } else {
                ++i;
            }
        }
        for (std::size_t i = 0; i < outbox.size();) {
            if (offerNow(outbox[i].first, outbox[i].second)) {
                outbox.erase(outbox.begin() + static_cast<std::ptrdiff_t>(i));
                ++work;
            } else {
                ++i;
            }
        }
        if (!pauseIngress) {
            for (auto& ep : endpoints) {
                if (ep->sub) work += ep->sub->poll(ep->assembler->handler(), 16);
            }
        }
        return work;
    }

    void send(int endpoint, const uint8_t* b, std::size_t n) {
        std::vector<uint8_t> frame(b, b + n);
        if (!offerNow(endpoint, frame)) outbox.emplace_back(endpoint, std::move(frame));
    }

    /// An Assign{gen, parts} frame (the coordinator's assignment change) to a session.
    void sendAssign(int64_t sessionId, int32_t gen, const std::vector<std::pair<int32_t, int32_t>>& parts) {
        namespace rp = rune::protocol::client;
        std::vector<rp::TopicPartition32> tps;
        for (const auto& [t, p] : parts) {
            rp::TopicPartition32 tp;
            tp.topicId   = t;
            tp.partition = p;
            tps.push_back(tp);
        }
        rp::Assign a;
        a.sessionId     = sessionId;
        a.correlationId = 0;
        a.generationId  = gen;
        std::vector<uint8_t> out(rp::Assign::encodedSizeFor(tps.size()));
        const std::size_t n = a.encode(out.data(), tps);
        send(0, out.data(), n);
    }

    /// Unsolicited or solicited MetadataResponse from the node endpoint.
    void sendMetadata(int64_t sessionId, int64_t correlationId, int endpoint = -1) {
        namespace rp = rune::protocol::client;
        std::vector<rp::BrokerEntry> brokers;
        if (!brokerHost.empty()) {
            rp::BrokerEntry b;
            b.nodeId = 0;
            b.host   = brokerHost;  // view into a member: valid through encode()
            b.port   = brokerPort;
            brokers.push_back(b);
        }
        for (const BrokerAd& ad : moreBrokers) {
            rp::BrokerEntry b;
            b.nodeId = ad.nodeId;
            b.host   = ad.host;
            b.port   = ad.port;
            brokers.push_back(b);
        }
        std::vector<rp::PartitionEntry> parts;
        const auto over = partitionsAt.find(endpoint);   // DR fix round 1 (I4): a per-node routing image
        for (const PartitionAd& ad : over != partitionsAt.end() ? over->second : partitions) {
            rp::PartitionEntry p;
            p.topicId         = ad.topicId;
            p.partition       = ad.partition;
            p.leaderId        = ad.leaderId;
            p.leaderEpoch     = ad.leaderEpoch;
            p.ingressStreamId = ad.ingressStreamId;
            p.ingressEndpoint = ad.ingressEndpoint;  // view/copy of a member: valid through encode()
            parts.push_back(p);
        }
        rp::MetadataResponse msg;
        msg.sessionId     = sessionId;
        msg.correlationId = correlationId;
        msg.brokers       = std::span<const rp::BrokerEntry>(brokers);
        msg.partitions    = std::span<const rp::PartitionEntry>(parts);
        std::vector<uint8_t> buf(msg.encodedSize());
        const std::size_t n = msg.encode(buf.data());
        sendToSession(sessionId, buf.data(), n);
    }

    /// ProduceResponse with an explicit baseOffset (idempotence model).
    void ackAt(const Produced& pr, int16_t errorCode, int64_t baseOffset, int64_t logStart = -1) {
        namespace rcc = rune::client::codec;
        uint8_t buf[64];
        const std::size_t n = rcc::encodeProduceResponse(buf, sizeof buf, errorCode, pr.partition,
                                                         baseOffset, pr.sessionId, pr.corrId, logStart);
        if (replyOnAnnouncedEgress) {
            sendToSession(pr.sessionId, buf, n);
            return;
        }
        send(pr.endpoint, buf, n);
    }

    /// ProduceResponse from the endpoint that received the batch.
    void ack(const Produced& pr, int16_t errorCode = 0) {
        namespace rcc = rune::client::codec;
        uint8_t buf[64];
        const int64_t base = nextOffset;
        nextOffset += pr.records;
        const std::size_t n = rcc::encodeProduceResponse(buf, sizeof buf, errorCode, pr.partition,
                                                         base, pr.sessionId, pr.corrId);
        if (replyOnAnnouncedEgress) {
            const auto it = sessionPubs.find(pr.sessionId);
            if (it != sessionPubs.end()) ++acksPerEgress[it->second.channel];
            sendToSession(pr.sessionId, buf, n);
            return;
        }
        send(pr.endpoint, buf, n);
    }

    /// A reply to one session: its announced egress (replyOnAnnouncedEgress),
    /// else the node endpoint's publication.
    void sendToSession(int64_t sessionId, const uint8_t* b, std::size_t n) {
        if (!replyOnAnnouncedEgress || sessionPubs.count(sessionId) == 0) {
            send(0, b, n);
            return;
        }
        std::vector<uint8_t> frame(b, b + n);
        if (!offerSession(sessionId, frame)) sessionOutbox.emplace_back(sessionId, std::move(frame));
    }

    /// Push one Records frame of `count` records at baseOffset to a subscription.
    void pushRecords(const Subscribed& sub, int64_t baseOffset, int32_t count) {
        pushRecordsFrom(sub, baseOffset, count, replyFromArrival ? sub.endpoint : 0);
    }
    /// ... from endpoint `from`'s publication (e.g. a worker's own socket — the
    /// broker's MDC fallback — that no session's ConnectResponse came from).
    void pushRecordsFrom(const Subscribed& sub, int64_t baseOffset, int32_t count, int from) {
        namespace rcc = rune::client::codec;
        rune::protocol::RecordBatchBuilder b(baseOffset, 1'000'000);
        for (int32_t i = 0; i < count; ++i) b.append("k", "v", 1'000'000 + i);
        const std::vector<uint8_t> batch = b.build();
        std::vector<uint8_t> out(batch.size() + 256);
        const std::size_t n = rcc::encodeRecords(out.data(), out.size(), sub.subscriptionId,
                                                 sub.topicId, sub.partition, baseOffset,
                                                 std::span<const uint8_t>(batch), sub.sessionId,
                                                 stampSubscribeCorr ? sub.corrId : 0);
        send(from, out.data(), n);
    }

    /// E2 round 6: the broker's transient LINEAGE NOTE for `sub` (an 8-byte batch).
    void sendLineageNote(const Subscribed& sub, uint64_t token) {
        namespace rcc = rune::client::codec;
        uint8_t t[8];
        for (int i = 0; i < 8; ++i) t[i] = static_cast<uint8_t>(token >> (8 * i));
        std::vector<uint8_t> out(256);
        const std::size_t n = rcc::encodeRecords(out.data(), out.size(), sub.subscriptionId, sub.topicId,
                                                 sub.partition, -1, std::span<const uint8_t>(t, 8), sub.sessionId,
                                                 stampSubscribeCorr ? sub.corrId : 0);
        send(replyFromArrival ? sub.endpoint : 0, out.data(), n);
    }

    void releaseHeld() {
        std::vector<Produced> h;
        h.swap(held);
        for (const Produced& pr : h) ack(pr);
    }

private:
    // NotCoordinator fence: true (and answered) when `endpoint` is not the coordinator.
    template <class Resp>
    bool notCoordinator_(int endpoint, int64_t sessionId, Resp r, int64_t corrId = 0) {
        if (coordinatorEndpoint < 0 || endpoint == coordinatorEndpoint) return false;
        ++notCoordinatorAnswers;
        if (!coordinatorRedirectTo.empty()) {
            // The real broker (review LOW-5): the request's own correlationId,
            // answered on the session's own node.
            sendError(replyFromArrival ? endpoint : 0,
                      static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator),
                      coordinatorRedirectTo, sessionId, corrId);
            return true;
        }
        r.sessionId     = sessionId;
        r.correlationId = 0;
        r.errorCode     = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator);
        std::vector<uint8_t> out(512);
        std::size_t n = 0;
        if constexpr (requires { r.encode(out.data()); }) {
            n = r.encode(out.data());
        } else {
            n = r.encode(out.data(), {});
        }
        send(0, out.data(), n);
        return true;
    }

    // D4: the txn coordinator fence (only with coordinatorRedirectTo set).
    bool txnNotCoordinator_(int endpoint, int64_t sessionId, int64_t corrId) {
        coordinatorEndpoints.push_back(endpoint);
        if (coordinatorRedirectTo.empty() || coordinatorEndpoint < 0 || endpoint == coordinatorEndpoint) return false;
        ++notCoordinatorAnswers;
        sendError(replyFromArrival ? endpoint : 0,
                  static_cast<int16_t>(rune::protocol::client::ErrorCode::NotCoordinator),
                  coordinatorRedirectTo, sessionId, corrId);
        return true;
    }

    void answerJoin_(int64_t sessionId) {
        namespace rp = rune::protocol::client;
        static const std::string kMember = "member-1";
        rp::JoinGroupResponse r;
        r.sessionId     = sessionId;
        r.correlationId = 0;
        r.generationId  = groupGeneration;
        r.isLeader      = 0;
        r.errorCode     = joinErrorCode;
        r.memberId      = joinErrorCode != 0 ? std::span<const uint8_t>()
                                             : std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(kMember.data()),
                                                                        kMember.size());
        std::vector<uint8_t> out(r.encodedSize());
        const std::size_t n = r.encode(out.data());
        send(0, out.data(), n);
    }

    struct SessionPub {
        std::string                                  channel;
        int64_t                                      reg = -1;
        std::shared_ptr<aeron::ExclusivePublication> pub;
    };
    std::map<int64_t, SessionPub>                     sessionPubs;
    std::vector<std::pair<std::chrono::steady_clock::time_point, std::vector<uint8_t>>> delayedConnects;
    std::vector<std::pair<int64_t, std::vector<uint8_t>>> sessionOutbox;
    int64_t                                           nextUniqueSessionId = 1000;

    bool offerSession(int64_t sessionId, const std::vector<uint8_t>& f) {
        auto& sp = sessionPubs[sessionId];
        if (!sp.pub) return false;
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(f.data()),
                                           static_cast<aeron::util::index_t>(f.size()));
        return sp.pub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }

    bool offerNow(int endpoint, const std::vector<uint8_t>& f) {
        auto& ep = endpoints[static_cast<std::size_t>(endpoint)];
        if (!ep->pub) return false;
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(f.data()),
                                           static_cast<aeron::util::index_t>(f.size()));
        return ep->pub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }

    struct SeqDesc {
        int32_t first = 0, last = 0;
        int64_t offset = 0;
    };
    struct SeqState {
        int16_t             epoch   = 0;
        int32_t             lastSeq = -1;
        std::deque<SeqDesc> window;
    };
    std::map<std::tuple<int32_t, int64_t, int32_t>, SeqState> seqStates;

    struct PendingAnswer {
        int      ordinal = 0;
        int64_t  dueAt   = 0;     // pump count
        bool     held    = false;
        Produced pr{};
        int16_t  ec  = 0;
        int64_t  off = 0;
        int64_t  lso = -1;   // D6: ProduceResponse.logStartOffset
    };
    std::vector<PendingAnswer> pendingAnswers_;
    int64_t                    pumps_ = 0;

    void flushAnswers_() {
        for (std::size_t i = 0; i < pendingAnswers_.size();) {
            const PendingAnswer& pa = pendingAnswers_[i];
            if (!pa.held && pa.dueAt <= pumps_) {
                const PendingAnswer a = pa;
                pendingAnswers_.erase(pendingAnswers_.begin() + static_cast<std::ptrdiff_t>(i));
                ackAt(a.pr, a.ec, a.off, a.lso);
            } else {
                ++i;
            }
        }
    }

    // Model answer: now, delayed or held (see the fault-injection knobs).
    void answerModel_(int ordinal, const Produced& pr, bool acksAll, int16_t ec, int64_t off, int64_t lso = -1) {
        int64_t delay = 0;
        if (ec == 0 && acksAll) delay = okDelayPumps;
        if (const auto it = answerDelay.find(ordinal); it != answerDelay.end()) delay = it->second;
        const bool held = holdAnswer.count(ordinal) != 0;
        if (delay == 0 && !held) {
            ackAt(pr, ec, off, lso);
            return;
        }
        pendingAnswers_.push_back({ordinal, pumps_ + delay, held, pr, ec, off, lso});
    }

    template <class View>
    void onIdempotentProduce(const View& pv, int endpoint) {
        namespace rp = rune::protocol::client;
        const int ordinal = ++produceFrames;
        if (dropProduce.count(ordinal) != 0) return;  // lost on the wire
        if (dropIf && dropIf(pv.partition, pv.producerEpoch, pv.baseSequence)) return;
        rune::protocol::RecordBatchReader r(pv.recordBatch.data(), pv.recordBatch.size());
        const Produced pr{pv.topicId, pv.partition, pv.sessionId, pv.correlationId,
                          r.recordCount(), r.baseSequence(), endpoint, pv.producerEpoch};
        produced.push_back(pr);
        const bool answer = pv.correlationId != 0 && dropAnswer.count(ordinal) == 0;
        const bool acksAll = pv.acks == 1;
        auto ackAt = [&](const Produced& p, int16_t ec, int64_t off, int64_t lso = -1) {
            answerModel_(ordinal, p, acksAll, ec, off, lso);
        };
        // NEW-2: step 4 (CRC) then step 4b (identity: producerId always; epoch +
        // baseSequence when producerId >= 0) — the real broker's order and codes.
        if (!r.ensureValid()) {
            ++crcFailures;
            if (answer) ackAt(pr, static_cast<int16_t>(rp::ErrorCode::Internal), 0);
            return;
        }
        bool mismatch = r.producerId() != pv.producerId;
        if (!mismatch && pv.producerId >= 0) {
            mismatch = r.producerEpoch() != pv.producerEpoch || r.baseSequence() != pv.baseSequence;
        }
        if (mismatch) {
            ++identityMismatches;
            if (answer) ackAt(pr, static_cast<int16_t>(rp::ErrorCode::SchemaValidationFailed), 0);
            return;
        }
        int16_t injected = 0;
        if (const auto it = errorFor.find(ordinal); it != errorFor.end()) injected = it->second;
        if (injected == 0 && errorIf) injected = errorIf(pv.partition, pv.producerEpoch, pv.baseSequence);
        if (injected != 0) {  // rejected, not processed
            if (injected == static_cast<int16_t>(rp::ErrorCode::OutOfOrderSequence)) ++oosAnswers;
            if (answer) ackAt(pr, injected, 0);
            return;
        }
        if (notLeaderEndpoints.count(endpoint) != 0) {  // refuseNotOwned_: not processed
            ++notLeaderAnswers;
            if (answer) ackAt(pr, static_cast<int16_t>(rp::ErrorCode::NotLeader), -1);
            return;
        }
        auto append = [&]() -> int64_t {
            LogBatch lb;
            lb.topicId       = pv.topicId;
            lb.partition     = pv.partition;
            lb.producerId    = pv.producerId;
            lb.producerEpoch = pv.producerEpoch;
            lb.baseSequence  = pv.baseSequence;
            lb.baseOffset    = nextOffset;
            r.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
                lb.values.emplace_back(v.begin(), v.end());
            });
            nextOffset += pr.records;
            log.push_back(std::move(lb));
            return log.back().baseOffset;
        };
        if (pv.producerId < 0) {
            const int64_t off = append();
            if (answer) ackAt(pr, 0, off);
            return;
        }
        const int32_t first = pv.baseSequence;
        const int32_t last  = first + pr.records - 1;
        const auto key = std::make_tuple(pv.topicId, pv.producerId, pv.partition);
        auto it = seqStates.find(key);
        auto accept = [&](SeqState& st) {
            const int64_t off = append();
            st.epoch   = pv.producerEpoch;
            st.lastSeq = last;
            st.window.push_back({first, last, off});
            while (static_cast<int>(st.window.size()) > idempotenceWindow) st.window.pop_front();
            if (answer) ackAt(pr, 0, off);
        };
        auto oos = [&] {
            ++oosAnswers;
            if (answer) ackAt(pr, static_cast<int16_t>(rp::ErrorCode::OutOfOrderSequence), 0);
        };
        if (it == seqStates.end()) {
            if (first != 0) {
                // D6: no entry + seq != 0 -> UnknownProducerId with the log start
                // (version-gated like the broker: OOS to an old client).
                const auto cv = clientVersions.find(pv.sessionId);
                if (cv == clientVersions.end() || !rp::clientUnderstandsUnknownProducerId(cv->second)) return oos();
                ++unknownPidAnswers;
                if (answer) ackAt(pr, static_cast<int16_t>(rp::ErrorCode::UnknownProducerId), 0, logStartOffset);
                return;
            }
            accept(seqStates[key]);
            return;
        }
        SeqState& st = it->second;
        if (pv.producerEpoch < st.epoch) {
            ++fencedAnswers;
            if (answer) ackAt(pr, static_cast<int16_t>(rp::ErrorCode::InvalidProducerEpoch), 0);
            return;
        }
        if (pv.producerEpoch > st.epoch) {
            if (first != 0) return oos();
            st.window.clear();
            accept(st);
            return;
        }
        for (const SeqDesc& d : st.window) {
            if (d.first == first && d.last == last) {
                ++windowDupAnswers;
                if (answer) ackAt(pr, 0, d.offset);
                return;
            }
        }
        if (first == st.lastSeq + 1) {
            accept(st);
            return;
        }
        if (last <= st.lastSeq) {  // known-old duplicate (outside the window)
            ++oldDupAnswers;
            if (answer) ackAt(pr, 0, -1);
            return;
        }
        oos();
    }

    void onFrame(int endpoint, const uint8_t* buf, std::size_t len, bool replay = false) {
        namespace rcc = rune::client::codec;
        namespace rp  = rune::protocol::client;
        const auto tid = rcc::peekTemplate(buf, len);
        if (!tid) return;
        if (static_cast<rcc::TemplateId>(*tid) == rcc::TemplateId::Connect) ++connectsArrived;   // dead or not
        if (deadEndpoints.count(endpoint) != 0) return;  // a dead node hears nothing
        if (!replay) ++framesReceived;
        // Catalog history (Task 5, F7): a test-installed handler for this template
        // answers the frame itself (e.g. Redirect / NotLeader / DdlResponse) —
        // consulted before any built-in behaviour.
        if (const auto h = templateHandlers.find(*tid); h != templateHandlers.end()) {
            h->second(endpoint, buf, len);
            return;
        }
        switch (static_cast<rcc::TemplateId>(*tid)) {
        case rcc::TemplateId::Connect: {
            const auto c = rcc::decodeConnectRequest(buf, len);
            if (!c) return;
            if (!replay && connectReplyDelay.count() > 0) {
                delayedConnects.emplace_back(std::chrono::steady_clock::now() + connectReplyDelay,
                                             std::vector<uint8_t>(buf, buf + len));
                return;
            }
            ++connects;
            ++connectsAt[endpoint];
            connectCorrIds.push_back(c->correlationId);
            if (!replyConnects || silentEndpoints.count(endpoint) != 0) return;
            if (const auto rf = refuseConnectsWith.find(endpoint); rf != refuseConnectsWith.end()) {
                // DR fix round 1 (I2): this node refuses every Connect with a code.
                uint8_t ro[64];
                const std::size_t rn = rcc::encodeConnectResponse(ro, sizeof ro, rf->second, 0, 0, c->sessionId,
                                                                  c->correlationId, advertisedBrokerVersion);
                send(endpoint, ro, rn);
                return;
            }
            for (const Produced& pr : ackBeforeNextConnect) ack(pr);
            ackBeforeNextConnect.clear();
            int64_t sid = assignedSessionId;
            if (uniqueSessionIds) sid = c->sessionId != 0 ? c->sessionId : nextUniqueSessionId++;
            clientVersions[sid]      = c->clientVersion;   // D6: the version gate's input
            lastConnectClientVersion = c->clientVersion;
            if (replyOnAnnouncedEgress && sessionPubs.count(sid) == 0) {
                SessionPub sp;
                sp.channel = std::string(c->egressChannel);
                sp.reg     = aeron->addExclusivePublication(sp.channel, c->egressStreamId);
                sessionPubs.emplace(sid, std::move(sp));
            }
            uint8_t out[64];
            const std::size_t n = rcc::encodeConnectResponse(
                out, sizeof out, connectErrorCode != 0 ? connectErrorCode : static_cast<int16_t>(rcc::ErrorCode::OK),
                0, sid, c->sessionId, c->correlationId,
                advertisedBrokerVersionAt.count(endpoint) != 0 ? advertisedBrokerVersionAt[endpoint]
                                                               : advertisedBrokerVersion);
            if (replyFromArrival && !replyOnAnnouncedEgress) {
                send(endpoint, out, n);
            } else {
                sendToSession(sid, out, n);
            }
            for (const auto& [sub, base] : recordsAfterNextConnect) pushRecords(sub, base, 1);
            recordsAfterNextConnect.clear();
            return;
        }
        case rcc::TemplateId::MetadataRequest: {
            const auto m = rp::MetadataRequest::decode(buf, len);
            if (!m) return;
            ++metadataRequests;
            metadataCorrIds.push_back(m->correlationId);
            metadataEndpoints.push_back(endpoint);   // DR client bug 1: which node was asked
            if (holdMetadata) return;
            sendMetadata(m->sessionId, m->correlationId, endpoint);
            return;
        }
        case rcc::TemplateId::Produce: {
            const auto pv = rcc::decodeProduce(buf, len);
            if (!pv) return;
            if (idempotence) {
                onIdempotentProduce(*pv, endpoint);
                return;
            }
            rune::protocol::RecordBatchReader r(pv->recordBatch.data(), pv->recordBatch.size());
            Produced pr{pv->topicId, pv->partition, pv->sessionId, pv->correlationId,
                        r.recordCount(), r.baseSequence(), endpoint};
            pr.producerEpoch = pv->producerEpoch;
            produced.push_back(pr);
            if (keepValues) {
                auto& vs = values[pv->partition];
                r.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
                    vs.emplace_back(v.begin(), v.end());
                });
            }
            if (pv->correlationId == 0) return;  // acks=None
            if (notLeaderEndpoints.count(endpoint) != 0) {
                ++notLeaderAnswers;
                ackAt(pr, static_cast<int16_t>(rp::ErrorCode::NotLeader), -1);
                return;
            }
            if (errorNotLeaderNext > 0 || redirectNext > 0) {
                const bool redirect = errorNotLeaderNext == 0;
                (redirect ? redirectNext : errorNotLeaderNext) -= 1;
                std::vector<uint8_t> out(256 + redirectTo.size());
                const std::size_t n = rcc::encodeErrorResponse(
                    out.data(), out.size(),
                    static_cast<int16_t>(redirect ? rp::ErrorCode::Redirect : rp::ErrorCode::NotLeader),
                    redirect ? std::string_view(redirectTo) : std::string_view(), pv->sessionId,
                    pv->correlationId);
                send(endpoint, out.data(), n);
                return;
            }
            if (!replyAcks || holdPartitions.count(pv->partition) != 0) {
                held.push_back(pr);
                return;
            }
            ack(pr);
            return;
        }
        case rcc::TemplateId::Subscribe: {
            const auto sv = rcc::decodeSubscribe(buf, len);
            if (!sv) return;
            subscribeCorrIds.push_back(sv->correlationId);
            if (subscribeRedirectNext > 0) {
                --subscribeRedirectNext;
                ++subscribeRefusals;
                std::vector<uint8_t> out(256 + subscribeRedirectTo.size());
                const std::size_t n = rcc::encodeErrorResponse(
                    out.data(), out.size(), static_cast<int16_t>(rp::ErrorCode::Redirect),
                    std::string_view(subscribeRedirectTo), sv->sessionId, sv->correlationId);
                send(replyFromArrival ? endpoint : 0, out.data(), n);
                return;
            }
            if (const auto lead = subscribeLeader.find(sv->partition);
                lead != subscribeLeader.end() && lead->second != endpoint) {
                ++subscribeRefusals;
                std::string to = endpoints[static_cast<std::size_t>(lead->second)]->channel;
                if (const auto at = to.find("endpoint="); at != std::string::npos) to = to.substr(at + 9);
                if (const auto bar = to.find('|'); bar != std::string::npos) to = to.substr(0, bar);
                std::vector<uint8_t> out(256 + to.size());
                const std::size_t n = rcc::encodeErrorResponse(
                    out.data(), out.size(),
                    static_cast<int16_t>(subscribeNotLeader ? rp::ErrorCode::NotLeader : rp::ErrorCode::Redirect),
                    subscribeNotLeader ? std::string_view() : std::string_view(to), sv->sessionId,
                    sv->correlationId);
                send(replyFromArrival ? endpoint : 0, out.data(), n);
                return;
            }
            {
                uint64_t claim = 0;
                if (sv->partitionKnown == 1 && sv->key.size() == 8)
                    for (int i = 0; i < 8; ++i) claim |= static_cast<uint64_t>(sv->key[static_cast<std::size_t>(i)]) << (8 * i);
                lineageClaims.push_back(claim);
                if (const auto pl = partitionLineage.find(sv->partition);
                    claim != 0 && pl != partitionLineage.end() && pl->second != claim) {
                    ++dataResetAnswers;
                    std::vector<uint8_t> out(256);
                    const std::size_t n = rcc::encodeErrorResponse(
                        out.data(), out.size(), static_cast<int16_t>(rp::ErrorCode::PartitionDataReset), "",
                        sv->sessionId, sv->correlationId);
                    send(replyFromArrival ? endpoint : 0, out.data(), n);
                    return;
                }
            }
            int64_t subId = nextSubscriptionId++;
            if (perEndpointSubIds) {
                // Like the broker: each node's own counter, cluster-unique
                // through the node prefix ((node + 1) << 40).
                auto [it, fresh] = endpointSubIds.try_emplace(endpoint, int64_t{1});
                subId = (static_cast<int64_t>(endpoint + 1) << 40) | (fresh ? it->second : ++it->second);
            }
            const Subscribed sub{subId, sv->topicId, sv->partition, sv->sessionId,
                                 endpoint, sv->fromOffset, sv->correlationId};
            subscribed.push_back(sub);
            uint8_t out[64];
            const std::size_t n = rcc::encodeSubscribeResponse(
                out, sizeof out, 0, sv->partition, sub.subscriptionId, sv->sessionId,
                sv->correlationId);
            if (dropSubscribeResponsesNext > 0) {
                --dropSubscribeResponsesNext;
                return;
            }
            send(replyFromArrival ? endpoint : 0, out, n);
            return;
        }
        case rcc::TemplateId::Ping: {
            ++pings;
            const auto pv = rcc::decodePing(buf, len);
            if (!pv) return;
            pingCorrIds.push_back(pv->correlationId);
            if (pingErrorCode != 0) {
                std::vector<uint8_t> out(256);
                const std::size_t n = rcc::encodeErrorResponse(out.data(), out.size(), pingErrorCode,
                                                               "ping refused", pv->sessionId,
                                                               pv->correlationId);
                send(0, out.data(), n);
            } else if (replyPongs) {
                std::vector<uint8_t> out(64 + pv->echo.size());
                const std::size_t n =
                    rcc::encodePong(out.data(), out.size(), pv->echo, pv->sessionId, pv->correlationId);
                send(replyFromArrival ? endpoint : 0, out.data(), n);
            }
            return;
        }
        case rcc::TemplateId::DefineTopic: {
            ++defines;
            const auto d = rp::DefineTopic::decode(buf, len);
            if (!d || !replySchemas) return;
            rp::DefineTopicResponse r;
            r.sessionId     = d->sessionId;
            r.correlationId = d->correlationId;
            r.topicId       = schemaTopicId;
            r.version       = schemaVersion;
            r.errorCode     = schemaErrorCode;
            std::vector<uint8_t> out(r.encodedSize());
            const std::size_t n = r.encode(out.data());
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::GetTopicSchema: {
            ++schemaGets;
            const auto g = rp::GetTopicSchema::decode(buf, len);
            if (!g || !replySchemas) return;
            rp::TopicSchemaResponse r;
            r.sessionId      = g->sessionId;
            r.correlationId  = g->correlationId;
            r.topicId        = schemaTopicId;
            r.version        = schemaVersion;
            r.errorCode      = schemaErrorCode;
            r.descriptorBlob = schemaBlob.data();
            r.descriptorLen  = static_cast<uint32_t>(schemaBlob.size());
            // Task 19: a nonzero requestedFingerprint selects by fingerprint
            // (the real broker's Task 13/18 behavior).  This stub only knows
            // one materialized version (schemaBlob/schemaVersion), so a
            // request for any OTHER fingerprint cannot be resolved here —
            // answer not-found, same as a real broker would for a fingerprint
            // it never saw.
            if (g->requestedFingerprint != 0) {
                bool matches = false;
                try {
                    matches = rune::codec::fingerprint(rune::codec::deserialize(
                        std::span<const uint8_t>(schemaBlob))) == g->requestedFingerprint;
                } catch (...) {}
                if (!matches) {
                    r.errorCode      = schemaErrorCode != 0 ? schemaErrorCode : 1;
                    r.descriptorBlob = nullptr;
                    r.descriptorLen  = 0;
                }
            }
            std::vector<uint8_t> out(r.encodedSize());
            const std::size_t n = r.encode(out.data());
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::OffsetCommit: {
            offsetCommits.emplace_back(buf, buf + len);
            const auto oc = rp::OffsetCommit::decode(buf, len);
            if (!oc || !replyOffsetCommits) return;
            rp::OffsetCommitResponse rsp;
            rsp.sessionId     = oc->sessionId;
            rsp.correlationId = oc->correlationId;
            std::vector<rp::OffsetCommitResult> res;
            if (offsetCommitErrorCode != 0) res.push_back(rp::OffsetCommitResult{-1, -1, offsetCommitErrorCode});
            std::vector<uint8_t> out(rp::OffsetCommitResponse::encodedSizeFor(res.size()));
            const std::size_t n = rsp.encode(out.data(), std::span<const rp::OffsetCommitResult>(res));
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::InitProducerId: {
            ++initPids;
            const auto ip = rp::InitProducerId::decode(buf, len);
            if (ip) coordinatorSessionIds.push_back(ip->sessionId);
            if (ip && txnNotCoordinator_(endpoint, ip->sessionId, ip->correlationId)) return;
            if (!ip || !initPidReply) return;
            rp::InitProducerIdResponse rsp;
            rsp.sessionId     = ip->sessionId;
            rsp.correlationId = ip->correlationId;
            if (!initPidCodes.empty()) {
                rsp.errorCode = initPidCodes.front();
                initPidCodes.pop_front();
            }
            rsp.producerId    = rsp.errorCode == 0 ? initPid : -1;
            rsp.producerEpoch = 0;
            std::vector<uint8_t> out(rp::InitProducerIdResponse::ENCODED_SIZE);
            const std::size_t n = rsp.encode(out.data());
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::AddPartitionsToTxn: {
            ++addPartitions;
            const auto ap = rp::AddPartitionsToTxn::decode(buf, len);
            if (ap) coordinatorSessionIds.push_back(ap->sessionId);
            if (ap && txnNotCoordinator_(endpoint, ap->sessionId, ap->correlationId)) return;
            if (!ap || !replyTxn) return;
            rp::AddPartitionsToTxnResponse rsp;
            rsp.sessionId     = ap->sessionId;
            rsp.correlationId = ap->correlationId;
            std::vector<uint8_t> out(rp::AddPartitionsToTxnResponse::encodedSizeFor(0));
            const std::size_t n = rsp.encode(out.data(), {});
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::AddOffsetsToTxn:    ++addOffsets; return;
        case rcc::TemplateId::TxnOffsetCommit:    ++txnOffsetCommits; return;
        case rcc::TemplateId::EndTxn: {
            ++endTxns;
            const auto et = rp::EndTxn::decode(buf, len);
            if (!et) return;
            coordinatorSessionIds.push_back(et->sessionId);
            if (txnNotCoordinator_(endpoint, et->sessionId, et->correlationId)) return;
            endTxnCommits.push_back(et->commit);
            if (!replyTxn) return;
            rp::EndTxnResponse rsp;
            rsp.sessionId     = et->sessionId;
            rsp.correlationId = et->correlationId;
            std::vector<uint8_t> out(rp::EndTxnResponse::ENCODED_SIZE);
            const std::size_t n = rsp.encode(out.data());
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::Unsubscribe: {
            const auto uv = rcc::decodeUnsubscribe(buf, len);
            if (uv) {
                unsubscribed.push_back(uv->subscriptionId);
                unsubscribedAt.push_back(endpoint);
            }
            return;
        }
        case rcc::TemplateId::JoinGroup: {
            const auto j = rp::JoinGroup::decode(buf, len);
            if (!j) return;
            ++joinGroups;
            groupSessionIds.push_back(j->sessionId);
            groupEndpoints.push_back(endpoint);
            if (!groupProtocol) return;
            if (notCoordinator_(endpoint, j->sessionId, rp::JoinGroupResponse{}, j->correlationId)) return;
            rebalancing = false;
            if (holdJoins) {
                pendingJoins.push_back(j->sessionId);
                return;
            }
            answerJoin_(j->sessionId);
            return;
        }
        case rcc::TemplateId::SyncGroup: {
            const auto sg = rp::SyncGroup::decode(buf, len);
            if (!sg) return;
            ++syncGroups;
            groupSessionIds.push_back(sg->sessionId);
            groupEndpoints.push_back(endpoint);
            if (!groupProtocol) return;
            if (notCoordinator_(endpoint, sg->sessionId, rp::SyncGroupResponse{}, sg->correlationId)) return;
            std::vector<rp::TopicPartition32> asgn;
            for (const auto& [t, p] : groupAssignment) {
                rp::TopicPartition32 tp;
                tp.topicId   = t;
                tp.partition = p;
                asgn.push_back(tp);
            }
            rp::SyncGroupResponse r;
            r.sessionId     = sg->sessionId;
            r.correlationId = 0;
            r.errorCode     = syncErrorCode;
            if (syncErrorCode != 0) asgn.clear();
            std::vector<uint8_t> out(rp::SyncGroupResponse::encodedSizeFor(asgn.size()));
            const std::size_t n = r.encode(out.data(), asgn);
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::Heartbeat: {
            const auto hb = rp::Heartbeat::decode(buf, len);
            if (!hb) return;
            ++heartbeats;
            groupSessionIds.push_back(hb->sessionId);
            groupEndpoints.push_back(endpoint);
            if (!groupProtocol) return;
            if (notCoordinator_(endpoint, hb->sessionId, rp::HeartbeatResponse{}, hb->correlationId)) return;
            rp::HeartbeatResponse r;
            r.sessionId     = hb->sessionId;
            r.correlationId = 0;
            r.errorCode     = heartbeatErrorCode != 0 ? heartbeatErrorCode
                            : rebalancing ? static_cast<int16_t>(rp::ErrorCode::RebalanceInProgress) : int16_t{0};
            std::vector<uint8_t> out(rp::HeartbeatResponse::ENCODED_SIZE);
            const std::size_t n = r.encode(out.data());
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::OffsetFetch: {
            const auto of = rp::OffsetFetch::decode(buf, len);
            if (!of) return;
            ++offsetFetches;
            groupSessionIds.push_back(of->sessionId);
            groupEndpoints.push_back(endpoint);
            if (!groupProtocol) return;
            std::vector<rp::OffsetFetchResult> res;
            const bool failAll = offsetFetchErrorsNext > 0;
            if (failAll) --offsetFetchErrorsNext;
            if (offsetFetchRejectNext > 0) {
                --offsetFetchRejectNext;
                rp::OffsetFetchResult r;
                r.topicId = -1; r.partition = -1; r.offset = -1; r.errorCode = offsetFetchRejectCode;
                res.push_back(r);
            } else for (const auto& e : of->entries) {
                rp::OffsetFetchResult r;
                r.topicId   = e.topicId;
                r.partition = e.partition;
                r.errorCode = failAll ? offsetFetchErrorCode : int16_t{0};
                const auto it = committedOffsets.find({e.topicId, e.partition});
                r.offset = it == committedOffsets.end() ? -1 : it->second;
                res.push_back(r);
            }
            rp::OffsetFetchResponse rsp;
            rsp.sessionId     = of->sessionId;
            rsp.correlationId = of->correlationId;
            std::vector<uint8_t> out(rp::OffsetFetchResponse::encodedSizeFor(res.size()));
            const std::size_t n = rsp.encode(out.data(), std::span<const rp::OffsetFetchResult>(res));
            if (holdOffsetFetch) {
                out.resize(n);
                heldOffsetFetches.push_back(std::move(out));
                return;
            }
            send(0, out.data(), n);
            return;
        }
        default:
            return;
        }
    }
};

} // namespace test_support
