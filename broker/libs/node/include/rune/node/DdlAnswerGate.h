#pragma once
// DdlAnswerGate.h — DDL conformance final review C1 / M7: a DefineTopic /
// CreateView OK reaches the client only once its topic SERVES.
//
// The admin thread posts kA2cTopicCommitted (failure ring) and then the client's
// OK (egress ring): two rings, no order between them.  The control thread owns
// the topic's servability (TopicRegistry::commitTopic, driven by that very
// kA2cTopicCommitted) and its replicas / assignment, so it is the one place that
// can tell "the topic serves" — no cross-thread read, no ring-order assumption.
//
// Every admin egress frame passes through offer():
//   * a DefineTopicResponse / CreateViewResponse with errorCode 0 and a topic id
//     is GATED on that topic: it leaves only when ready(topicId) holds (the
//     caller's predicate: committed + every partition's role routed);
//   * any other frame is gated on nothing;
//   * FIFO: once one frame is held, every later frame queues behind it (a
//     session's answers never overtake each other).
// A gated frame at the head past maxHoldNs leaves anyway (logged by the caller):
// the gate delays an answer, it never swallows one (no-hang).  The caller drains
// its ring only while the gate is empty, so one drain's frames always fit.
//
// Control-thread-owned, single-threaded.  The hold buffer is preallocated
// (SpillFifo); offer / release never allocate.  Cold path (DDL answers only).
#include <rune/gateway/SpillFifo.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <cstdint>
#include <cstring>

namespace rune::node {

class DdlAnswerGate {
public:
    enum class Offer : uint8_t { Pass, Held, Full };

    explicit DdlAnswerGate(std::size_t capacityBytes, int64_t maxHoldNs) noexcept
        : held_(capacityBytes), maxHoldNs_(maxHoldNs) {}

    /// The topic a DDL answer frame must wait for: the topic id of a successful
    /// DefineTopicResponse / CreateViewResponse, else -1 (not gated).
    [[nodiscard]] static int32_t gatedTopicOf(const uint8_t* frame, std::size_t n) noexcept {
        namespace pc = rune::protocol::client;
        const auto h = pc::MessageHeader::decode(frame, n);
        if (!h) return -1;
        if (h->templateId == pc::DefineTopicResponse::TEMPLATE_ID) {
            const auto r = pc::DefineTopicResponse::decode(frame, n);
            return (r && r->errorCode == 0) ? r->topicId : -1;
        }
        if (h->templateId == pc::CreateViewResponse::TEMPLATE_ID) {
            const auto r = pc::CreateViewResponse::decode(frame, n);
            return (r && r->errorCode == 0) ? r->viewTopicId : -1;
        }
        return -1;
    }

    [[nodiscard]] bool empty() const noexcept { return held_.empty(); }
    [[nodiscard]] std::size_t heldCount() const noexcept { return held_.count(); }

    /// Pass: send it now.  Held: queued (FIFO; it, or a frame ahead of it, waits
    /// for its topic).  Full: not taken — impossible when the caller offers only
    /// what one drain of an empty gate yields (capacity >= that drain's bytes).
    template <class Ready>
    Offer offer(int64_t session, const uint8_t* frame, std::size_t n, int64_t nowNs, Ready&& ready) noexcept {
        if (held_.empty()) {
            const int32_t t = gatedTopicOf(frame, n);
            if (t < 0 || ready(t)) return Offer::Pass;
            headSinceNs_ = nowNs;
        }
        return held_.push(session, frame, n) ? Offer::Held : Offer::Full;
    }

    /// Send the held frames, oldest first, while the head is ready — or has waited
    /// maxHoldNs (then `overdue(session, topicId)` is told before it is sent).
    template <class Ready, class Send, class Overdue>
    int release(int64_t nowNs, Ready&& ready, Send&& send, Overdue&& overdue) {
        int sent = 0;
        while (!held_.empty()) {
            const auto r = held_.front();
            const int32_t t = gatedTopicOf(r.data, r.len);
            if (t >= 0 && !ready(t)) {
                if (nowNs - headSinceNs_ < maxHoldNs_) break;
                overdue(r.key, t);
            }
            send(r.key, r.data, r.len);
            held_.pop();
            headSinceNs_ = nowNs;   // the next frame becomes the head now
            ++sent;
        }
        return sent;
    }

private:
    rune::gateway::SpillFifo held_;
    int64_t                  maxHoldNs_;
    int64_t                  headSinceNs_ = 0;   // when the current head became the head
};

} // namespace rune::node
