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
// A gated frame at the head past maxHoldNs whose topic STILL does not serve is
// answered Internal (6, "in doubt") instead of the OK — the record committed but
// the topic is not usable, so an OK would be a silent fallback (concern-4 ruling).
// The caller is told (log + count).  The gate delays an answer, it never swallows
// one (no-hang).  The caller drains its ring only while the gate is empty, so one
// drain's frames always fit.
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

    /// Encode, into `out`, the Internal (6, in doubt) answer to the request a held
    /// DefineTopic / CreateView OK `frame` answers: same session + correlation id,
    /// topic id -1.  Returns its length (0 when `frame` is neither).  No allocation.
    static constexpr std::size_t kInDoubtMaxBytes = 64;
    [[nodiscard]] static std::size_t encodeInDoubt(const uint8_t* frame, std::size_t n,
                                                   uint8_t (&out)[kInDoubtMaxBytes]) noexcept {
        namespace pc = rune::protocol::client;
        const auto h = pc::MessageHeader::decode(frame, n);
        if (!h) return 0;
        const auto internal = static_cast<uint16_t>(pc::ErrorCode::Internal);
        if (h->templateId == pc::DefineTopicResponse::TEMPLATE_ID) {
            auto r = pc::DefineTopicResponse::decode(frame, n);
            if (!r) return 0;
            r->topicId = -1; r->version = 0; r->errorCode = internal;
            static_assert(pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED <= kInDoubtMaxBytes);
            return r->encode(out);
        }
        if (h->templateId == pc::CreateViewResponse::TEMPLATE_ID) {
            auto r = pc::CreateViewResponse::decode(frame, n);
            if (!r) return 0;
            r->viewTopicId = -1; r->outputFingerprint = 0; r->errorCode = internal;
            static_assert(pc::MessageHeader::ENCODED_SIZE + pc::CreateViewResponse::BODY_FIXED <= kInDoubtMaxBytes);
            return r->encode(out);
        }
        return 0;
    }

    /// Send the held frames, oldest first, while the head is ready.  A head whose
    /// topic still does not serve after maxHoldNs is answered Internal (in doubt —
    /// encodeInDoubt) and `overdue(session, topicId)` is told; never the OK.
    template <class Ready, class Send, class Overdue>
    int release(int64_t nowNs, Ready&& ready, Send&& send, Overdue&& overdue) {
        int sent = 0;
        while (!held_.empty()) {
            const auto r = held_.front();
            const int32_t t = gatedTopicOf(r.data, r.len);
            if (t >= 0 && !ready(t)) {
                if (nowNs - headSinceNs_ < maxHoldNs_) break;
                overdue(r.key, t);
                uint8_t inDoubt[kInDoubtMaxBytes];
                const std::size_t m = encodeInDoubt(r.data, r.len, inDoubt);
                // gatedTopicOf() >= 0 implies a decodable Define/CreateView frame, so m > 0.
                send(r.key, m > 0 ? inDoubt : r.data, m > 0 ? m : r.len);
                held_.pop();
                headSinceNs_ = nowNs;
                ++sent;
                continue;
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
