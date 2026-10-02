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
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
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
    int64_t     assignedSessionId = 42;
    int64_t     nextOffset = 100;
    bool        replyAcks = true;
    std::set<int32_t>     holdPartitions;  // partitions whose acks are held
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
    // F8a: direct subscriptions (Subscribe -> SubscribeResponse; Records pushes).
    struct Subscribed { int64_t subscriptionId; int32_t topicId; int32_t partition; int64_t sessionId; };
    std::vector<Subscribed> subscribed;
    std::vector<int64_t>    unsubscribed;  // subscriptionIds
    std::vector<std::pair<Subscribed, int64_t>> recordsAfterNextConnect;
    int64_t nextSubscriptionId = 500;
    // F8a fix2: stop consuming client ingress (the client's publication then
    // back-pressures once its term window is full).
    bool pauseIngress = false;
    int  pings        = 0;
    std::vector<std::vector<uint8_t>> offsetCommits;  // raw OffsetCommit frames received
    bool replyOffsetCommits = true;
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
        int32_t partition = 0;
        int64_t producerId = -1;
        int16_t producerEpoch = -1;
        int32_t baseSequence = -1;
        int64_t baseOffset = 0;
        std::vector<std::vector<uint8_t>> values;
    };
    std::vector<LogBatch> log;
    int oosAnswers = 0, windowDupAnswers = 0, oldDupAnswers = 0, fencedAnswers = 0;
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
                   int32_t controlStream, std::string egressCh, int32_t egressStream)
        : egressChannel(std::move(egressCh)), egressStreamId(egressStream) {
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

    int addEndpoint(const std::string& channel, int32_t stream) {
        auto ep = std::make_unique<Endpoint>();
        ep->channel = channel;
        ep->stream  = stream;
        ep->subReg  = aeron->addSubscription(channel, stream);
        ep->pubReg  = aeron->addExclusivePublication(egressChannel, egressStreamId);
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

    /// Unsolicited or solicited MetadataResponse from the node endpoint.
    void sendMetadata(int64_t sessionId, int64_t correlationId) {
        namespace rp = rune::protocol::client;
        std::vector<rp::BrokerEntry> brokers;
        if (!brokerHost.empty()) {
            rp::BrokerEntry b;
            b.nodeId = 0;
            b.host   = brokerHost;  // view into a member: valid through encode()
            b.port   = brokerPort;
            brokers.push_back(b);
        }
        std::vector<rp::PartitionEntry> parts;
        for (const PartitionAd& ad : partitions) {
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
    void ackAt(const Produced& pr, int16_t errorCode, int64_t baseOffset) {
        namespace rcc = rune::client::codec;
        uint8_t buf[64];
        const std::size_t n = rcc::encodeProduceResponse(buf, sizeof buf, errorCode, pr.partition,
                                                         baseOffset, pr.sessionId, pr.corrId);
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
        namespace rcc = rune::client::codec;
        rune::protocol::RecordBatchBuilder b(baseOffset, 1'000'000);
        for (int32_t i = 0; i < count; ++i) b.append("k", "v", 1'000'000 + i);
        const std::vector<uint8_t> batch = b.build();
        std::vector<uint8_t> out(batch.size() + 256);
        const std::size_t n = rcc::encodeRecords(out.data(), out.size(), sub.subscriptionId,
                                                 sub.topicId, sub.partition, baseOffset,
                                                 std::span<const uint8_t>(batch), sub.sessionId, 0);
        send(0, out.data(), n);
    }

    void releaseHeld() {
        std::vector<Produced> h;
        h.swap(held);
        for (const Produced& pr : h) ack(pr);
    }

private:
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
    };
    std::vector<PendingAnswer> pendingAnswers_;
    int64_t                    pumps_ = 0;

    void flushAnswers_() {
        for (std::size_t i = 0; i < pendingAnswers_.size();) {
            const PendingAnswer& pa = pendingAnswers_[i];
            if (!pa.held && pa.dueAt <= pumps_) {
                const PendingAnswer a = pa;
                pendingAnswers_.erase(pendingAnswers_.begin() + static_cast<std::ptrdiff_t>(i));
                ackAt(a.pr, a.ec, a.off);
            } else {
                ++i;
            }
        }
    }

    // Model answer: now, delayed or held (see the fault-injection knobs).
    void answerModel_(int ordinal, const Produced& pr, bool acksAll, int16_t ec, int64_t off) {
        int64_t delay = 0;
        if (ec == 0 && acksAll) delay = okDelayPumps;
        if (const auto it = answerDelay.find(ordinal); it != answerDelay.end()) delay = it->second;
        const bool held = holdAnswer.count(ordinal) != 0;
        if (delay == 0 && !held) {
            ackAt(pr, ec, off);
            return;
        }
        pendingAnswers_.push_back({ordinal, pumps_ + delay, held, pr, ec, off});
    }

    template <class View>
    void onIdempotentProduce(const View& pv, int endpoint) {
        namespace rp = rune::protocol::client;
        const int ordinal = ++produceFrames;
        if (dropProduce.count(ordinal) != 0) return;  // lost on the wire
        if (dropIf && dropIf(pv.partition, pv.producerEpoch, pv.baseSequence)) return;
        rune::protocol::RecordBatchReader r(pv.recordBatch.data(), pv.recordBatch.size());
        const Produced pr{pv.topicId, pv.partition, pv.sessionId, pv.correlationId,
                          r.recordCount(), r.baseSequence(), endpoint};
        produced.push_back(pr);
        const bool answer = pv.correlationId != 0 && dropAnswer.count(ordinal) == 0;
        const bool acksAll = pv.acks == 1;
        auto ackAt = [&](const Produced& p, int16_t ec, int64_t off) { answerModel_(ordinal, p, acksAll, ec, off); };
        int16_t injected = 0;
        if (const auto it = errorFor.find(ordinal); it != errorFor.end()) injected = it->second;
        if (injected == 0 && errorIf) injected = errorIf(pv.partition, pv.producerEpoch, pv.baseSequence);
        if (injected != 0) {  // rejected, not processed
            if (injected == static_cast<int16_t>(rp::ErrorCode::OutOfOrderSequence)) ++oosAnswers;
            if (answer) ackAt(pr, injected, 0);
            return;
        }
        auto append = [&]() -> int64_t {
            LogBatch lb;
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
            if (first != 0) return oos();
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
        if (!replay) ++framesReceived;
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
            if (!replyConnects || silentEndpoints.count(endpoint) != 0) return;
            for (const Produced& pr : ackBeforeNextConnect) ack(pr);
            ackBeforeNextConnect.clear();
            int64_t sid = assignedSessionId;
            if (uniqueSessionIds) sid = c->sessionId != 0 ? c->sessionId : nextUniqueSessionId++;
            if (replyOnAnnouncedEgress && sessionPubs.count(sid) == 0) {
                SessionPub sp;
                sp.channel = std::string(c->egressChannel);
                sp.reg     = aeron->addExclusivePublication(sp.channel, c->egressStreamId);
                sessionPubs.emplace(sid, std::move(sp));
            }
            uint8_t out[64];
            const std::size_t n = rcc::encodeConnectResponse(
                out, sizeof out, connectErrorCode != 0 ? connectErrorCode : static_cast<int16_t>(rcc::ErrorCode::OK),
                0, sid, c->sessionId, c->correlationId);
            sendToSession(sid, out, n);
            for (const auto& [sub, base] : recordsAfterNextConnect) pushRecords(sub, base, 1);
            recordsAfterNextConnect.clear();
            return;
        }
        case rcc::TemplateId::MetadataRequest: {
            const auto m = rp::MetadataRequest::decode(buf, len);
            if (!m) return;
            ++metadataRequests;
            sendMetadata(m->sessionId, m->correlationId);
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
            const Produced pr{pv->topicId, pv->partition, pv->sessionId, pv->correlationId,
                              r.recordCount(), r.baseSequence(), endpoint};
            produced.push_back(pr);
            if (keepValues) {
                auto& vs = values[pv->partition];
                r.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
                    vs.emplace_back(v.begin(), v.end());
                });
            }
            if (pv->correlationId == 0) return;  // acks=None
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
            const Subscribed sub{nextSubscriptionId++, sv->topicId, sv->partition, sv->sessionId};
            subscribed.push_back(sub);
            uint8_t out[64];
            const std::size_t n = rcc::encodeSubscribeResponse(
                out, sizeof out, 0, sv->partition, sub.subscriptionId, sv->sessionId,
                sv->correlationId);
            send(0, out, n);
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
                send(0, out.data(), n);
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
            std::vector<uint8_t> out(rp::OffsetCommitResponse::encodedSizeFor(0));
            const std::size_t n = rsp.encode(out.data(), {});
            send(0, out.data(), n);
            return;
        }
        case rcc::TemplateId::InitProducerId: {
            ++initPids;
            const auto ip = rp::InitProducerId::decode(buf, len);
            if (ip) coordinatorSessionIds.push_back(ip->sessionId);
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
            if (uv) unsubscribed.push_back(uv->subscriptionId);
            return;
        }
        default:
            return;
        }
    }
};

} // namespace test_support
