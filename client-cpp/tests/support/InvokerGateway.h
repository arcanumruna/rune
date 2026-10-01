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
#include <rune/protocol/client/MetadataMessages.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <Subscription.h>

#include <cstdint>
#include <memory>
#include <set>
#include <span>
#include <string>
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
    int connects = 0;
    int metadataRequests = 0;
    // F8a: direct subscriptions (Subscribe -> SubscribeResponse; Records pushes).
    struct Subscribed { int64_t subscriptionId; int32_t topicId; int32_t partition; int64_t sessionId; };
    std::vector<Subscribed> subscribed;
    std::vector<int64_t>    unsubscribed;  // subscriptionIds
    int64_t nextSubscriptionId = 500;

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

    /// One gateway duty cycle: conductor, registrations, outbox, polls.
    int pump() {
        int work = aeron->conductorAgentInvoker().invoke();
        for (auto& ep : endpoints) {
            if (!ep->sub) ep->sub = aeron->findSubscription(ep->subReg);
            if (!ep->pub) ep->pub = aeron->findExclusivePublication(ep->pubReg);
        }
        for (std::size_t i = 0; i < outbox.size();) {
            if (offerNow(outbox[i].first, outbox[i].second)) {
                outbox.erase(outbox.begin() + static_cast<std::ptrdiff_t>(i));
                ++work;
            } else {
                ++i;
            }
        }
        for (auto& ep : endpoints) {
            if (ep->sub) work += ep->sub->poll(ep->assembler->handler(), 16);
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
            parts.push_back(p);
        }
        rp::MetadataResponse msg;
        msg.sessionId     = sessionId;
        msg.correlationId = correlationId;
        msg.brokers       = std::span<const rp::BrokerEntry>(brokers);
        msg.partitions    = std::span<const rp::PartitionEntry>(parts);
        std::vector<uint8_t> buf(msg.encodedSize());
        const std::size_t n = msg.encode(buf.data());
        send(0, buf.data(), n);
    }

    /// ProduceResponse from the endpoint that received the batch.
    void ack(const Produced& pr, int16_t errorCode = 0) {
        namespace rcc = rune::client::codec;
        uint8_t buf[64];
        const int64_t base = nextOffset;
        nextOffset += pr.records;
        const std::size_t n = rcc::encodeProduceResponse(buf, sizeof buf, errorCode, pr.partition,
                                                         base, pr.sessionId, pr.corrId);
        send(pr.endpoint, buf, n);
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
    bool offerNow(int endpoint, const std::vector<uint8_t>& f) {
        auto& ep = endpoints[static_cast<std::size_t>(endpoint)];
        if (!ep->pub) return false;
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(f.data()),
                                           static_cast<aeron::util::index_t>(f.size()));
        return ep->pub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) > 0;
    }

    void onFrame(int endpoint, const uint8_t* buf, std::size_t len) {
        namespace rcc = rune::client::codec;
        namespace rp  = rune::protocol::client;
        const auto tid = rcc::peekTemplate(buf, len);
        if (!tid) return;
        switch (static_cast<rcc::TemplateId>(*tid)) {
        case rcc::TemplateId::Connect: {
            const auto c = rcc::decodeConnectRequest(buf, len);
            if (!c) return;
            ++connects;
            for (const Produced& pr : ackBeforeNextConnect) ack(pr);
            ackBeforeNextConnect.clear();
            uint8_t out[64];
            const std::size_t n = rcc::encodeConnectResponse(
                out, sizeof out, static_cast<int16_t>(rcc::ErrorCode::OK), 0, assignedSessionId,
                c->sessionId, c->correlationId);
            send(0, out, n);
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
            rune::protocol::RecordBatchReader r(pv->recordBatch.data(), pv->recordBatch.size());
            const Produced pr{pv->topicId, pv->partition, pv->sessionId, pv->correlationId,
                              r.recordCount(), r.baseSequence(), endpoint};
            produced.push_back(pr);
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
            const Subscribed sub{nextSubscriptionId++, sv->topicId, sv->partition, sv->sessionId};
            subscribed.push_back(sub);
            uint8_t out[64];
            const std::size_t n = rcc::encodeSubscribeResponse(
                out, sizeof out, 0, sv->partition, sub.subscriptionId, sv->sessionId,
                sv->correlationId);
            send(0, out, n);
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
