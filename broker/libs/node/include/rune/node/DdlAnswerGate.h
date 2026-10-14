#pragma once
// DdlAnswerGate.h — DDL conformance final review C1 / M7: a DefineTopic /
// CreateView OK (and, catalog history Task 9, a DdlResponse OK naming a
// gateTopicId — AlterView's fresh sink) reaches the client only once its topic SERVES.
//
// The admin thread posts the applied topic (kA2cCatalogTopicApplied, failure
// ring) and then the client's OK (egress ring): two rings, no order between them.
// The control thread builds the topic (TopicRegistry::createTopicAt) and its
// replicas / assignment, so it is the one place that can tell "the topic serves"
// — no cross-thread read, no ring-order assumption.
//
// Every admin egress frame passes through offer():
//   * a DefineTopicResponse / CreateViewResponse with errorCode 0 and a topic id,
//     or a DdlResponse with errorCode 0 and gateTopicId >= 0, is GATED on that
//     topic: it leaves only when ready(topicId) holds (the caller's predicate:
//     applied + every partition assigned and its role routed);
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
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <cstdint>
#include <cstring>
#include <string_view>

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
        // Catalog history Task 9: a DdlResponse names the topic its answer is
        // gated on (AlterView's fresh sink; -1 = none).
        if (h->templateId == pc::DdlResponse::TEMPLATE_ID) {
            const auto r = pc::DdlResponse::decode(frame, n);
            return (r && r->errorCode == 0 && r->gateTopicId >= 0) ? r->gateTopicId : -1;
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
    ///
    /// Task 11 fix round 1 (I2): `detail` (truncated to fit) says WHY — e.g. the
    /// committed topic is unplaceable and when it is retried.  A DdlResponse
    /// carries it in its own detail; a DefineTopic / CreateView answer carries it
    /// only as an ErrorResponse{Internal, detail}, which only a v8 session
    /// understands (`asErrorResponse`) — a pre-v8 session gets its own type.
    static constexpr std::size_t kInDoubtMaxBytes = 512;
    [[nodiscard]] static std::size_t encodeInDoubt(const uint8_t* frame, std::size_t n,
                                                   uint8_t (&out)[kInDoubtMaxBytes],
                                                   std::string_view detail = {},
                                                   bool asErrorResponse = false) noexcept {
        namespace pc = rune::protocol::client;
        const auto h = pc::MessageHeader::decode(frame, n);
        if (!h) return 0;
        const auto internal = static_cast<uint16_t>(pc::ErrorCode::Internal);
        constexpr std::size_t kDetailRoom = kInDoubtMaxBytes - 64;
        if (detail.size() > kDetailRoom) detail = detail.substr(0, kDetailRoom);
        const bool definesOrView = h->templateId == pc::DefineTopicResponse::TEMPLATE_ID ||
                                   h->templateId == pc::CreateViewResponse::TEMPLATE_ID;
        if (definesOrView && asErrorResponse && !detail.empty()) {
            pc::ErrorResponse e;
            e.sessionId = h->sessionId; e.correlationId = h->correlationId;
            e.code = static_cast<int16_t>(internal); e.message = detail;
            return e.encode(out);
        }
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
        if (h->templateId == pc::DdlResponse::TEMPLATE_ID) {
            auto r = pc::DdlResponse::decode(frame, n);
            if (!r) return 0;
            r->errorCode = internal; r->gateTopicId = -1; r->detail = detail;
            static_assert(pc::MessageHeader::ENCODED_SIZE + pc::DdlResponse::BODY_FIXED + 2 <= kInDoubtMaxBytes);
            return r->encode(out);
        }
        return 0;
    }

    /// Send the held frames, oldest first, while the head is ready.  A head whose
    /// topic still does not serve after maxHoldNs is answered Internal (in doubt —
    /// encodeInDoubt) and `overdue(session, topicId)` is told; never the OK.
    template <class Ready, class Send, class Overdue>
    int release(int64_t nowNs, Ready&& ready, Send&& send, Overdue&& overdue) {
        return release(nowNs, ready, send, overdue, [](int64_t, int32_t) { return Explanation{}; });
    }
    /// Why an overdue topic does not serve (Task 11 fix round 1): `detail` views
    /// storage the caller keeps alive across the call; `asErrorResponse` = the
    /// session understands an ErrorResponse detail (v8).
    struct Explanation { std::string_view detail; bool asErrorResponse = false; };
    template <class Ready, class Send, class Overdue, class Explain>
    int release(int64_t nowNs, Ready&& ready, Send&& send, Overdue&& overdue, Explain&& explain) {
        int sent = 0;
        while (!held_.empty()) {
            const auto r = held_.front();
            const int32_t t = gatedTopicOf(r.data, r.len);
            if (t >= 0 && !ready(t)) {
                if (nowNs - headSinceNs_ < maxHoldNs_) break;
                overdue(r.key, t);
                const Explanation why = explain(r.key, t);
                uint8_t inDoubt[kInDoubtMaxBytes];
                const std::size_t m = encodeInDoubt(r.data, r.len, inDoubt, why.detail, why.asErrorResponse);
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
