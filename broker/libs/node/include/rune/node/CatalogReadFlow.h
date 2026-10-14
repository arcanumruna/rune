#pragma once
// CatalogReadFlow.h — catalog history Task 12 fix round 0: the CONTROL-thread
// half of catalog-read flow control.  The control thread drains the admin →
// control catalog-read ring and delivers each frame to its session:
//
//   * a reliable answer (head.subscriptionId 0: SubscribeResponse / ErrorResponse)
//     → sendReliable (queued under back-pressure, as every answer is);
//   * a Records frame → OFFERED, never spilled.  When the session cannot take it
//     (its publication's window is full, or answers wait in its spill) the frame
//     is dropped and the subscription PAUSED: the admin is told the frame's
//     baseOffset (it rewinds to it — the log is the subscription's buffer), every
//     later frame of that subscription is dropped until the admin's pause ACK
//     (an empty frame; FIFO: every frame sent before the pause precedes it), and
//     once the session's window has room again the admin is told to resume.
//
// A slow reader therefore never grows the session's spill and is never expired
// for being slow (only the session timeout applies); the ring is always drained
// (a paused reader never blocks another reader, and the DDL answers have their
// own ring).  One fixed-capacity entry per PAUSED subscription — preallocated,
// zero steady-state allocation; an idle flow costs one empty() check.
//
// Each entry is one explicit state (hard rule 3):
//   PauseOwed{baseOffset} — the pause report did not fit the control → admin ring yet;
//   AwaitingAck           — reported; dropping its in-flight frames until the ack;
//   AwaitingRoom          — acked; waiting for the session's window;
//   ResumeOwed            — room; the resume did not fit the ring yet.
// The entry is erased once the resume is posted (nothing of the subscription is
// in flight then: it sent nothing since its ack), or when the session is gone.
//
// Sink (the control thread's adapters; tests use a fake):
//   Offer   tryOffer(int64_t session, const uint8_t* frame, std::size_t n);
//   void    sendReliable(int64_t session, const uint8_t* frame, std::size_t n);
//   Room    room(int64_t session);
//   bool    postPaused(int64_t session, int64_t subscriptionId, int64_t baseOffset, bool droppedMarker);
//   bool    postResume(int64_t session, int64_t subscriptionId);

#include <rune/node/CatalogReadServer.h>

#include <spdlog/spdlog.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <variant>
#include <vector>

namespace rune::node {

class CatalogReadFlow {
public:
    enum class Offer : uint8_t { Sent, BackPressured, NoSession };
    enum class Room  : uint8_t { Ready, NotYet, Gone };

    struct PauseOwed    { int64_t baseOffset = -1; bool droppedMarker = false; };
    struct AwaitingAck  {};
    struct AwaitingRoom {};
    struct ResumeOwed   {};
    using State = std::variant<PauseOwed, AwaitingAck, AwaitingRoom, ResumeOwed>;
    struct Entry {
        int64_t session        = 0;
        int64_t subscriptionId = 0;
        State   state;
    };

    explicit CatalogReadFlow(std::size_t capacity) : capacity_(capacity) { entries_.reserve(capacity); }

    /// One catalog-read ring frame: [CatalogReadFrameHead (24 B)][client frame].
    template <class Sink>
    void onFrame(const uint8_t* p, std::size_t n, Sink& sink) {
        if (n < kCatalogReadHeadBytes) [[unlikely]] { ++malformed_; return; }
        CatalogReadFrameHead h;
        std::memcpy(&h.session, p, 8);
        std::memcpy(&h.subscriptionId, p + 8, 8);
        std::memcpy(&h.baseOffset, p + 16, 8);
        const uint8_t*    frame = p + kCatalogReadHeadBytes;
        const std::size_t len   = n - kCatalogReadHeadBytes;
        if (h.subscriptionId == 0) { sink.sendReliable(h.session, frame, len); return; }
        if (Entry* e = find_(h.session, h.subscriptionId)) {
            if (len == 0) {   // the pause ack: every in-flight frame of it is behind us
                if (std::holds_alternative<AwaitingAck>(e->state) || std::holds_alternative<PauseOwed>(e->state))
                    e->state = AwaitingRoom{};
                return;
            }
            ++dropped_;       // in flight when it paused: the admin rewound to before it
            return;
        }
        if (len == 0) return;   // an ack for a subscription not paused here (already cleared)
        switch (sink.tryOffer(h.session, frame, len)) {
        case Offer::Sent:
            ++delivered_;
            return;
        case Offer::NoSession:
            return;   // closed: the session close ends its catalog subscriptions on the admin
        case Offer::BackPressured:
            break;
        }
        ++pauses_;
        ++dropped_;
        if (entries_.size() >= capacity_) [[unlikely]] {
            // Cannot happen (one entry per paused subscription; capacity >= the
            // admin's subscription cap).  Never a silent drop: queue it reliably.
            spdlog::error("CatalogReadFlow: {} paused catalog subscriptions (the capacity) — frame of {:#x} "
                          "queued reliably instead", entries_.size(), h.subscriptionId);
            --dropped_;
            sink.sendReliable(h.session, frame, len);
            return;
        }
        // I1: the caught-up marker is the one Records with an empty batch.
        const bool marker = len == kCatalogRecordsOverhead;
        Entry e{h.session, h.subscriptionId, PauseOwed{h.baseOffset, marker}};
        if (sink.postPaused(h.session, h.subscriptionId, h.baseOffset, marker)) e.state = AwaitingAck{};
        entries_.push_back(e);   // reserved: never allocates
    }

    /// Owed reports, and the paused sessions' room.  Call every control pass
    /// while !idle().  Returns the reports posted.
    template <class Sink>
    int service(Sink& sink) {
        int work = 0;
        for (std::size_t i = 0; i < entries_.size();) {
            Entry& e = entries_[i];
            if (const auto* po = std::get_if<PauseOwed>(&e.state)) {
                if (sink.postPaused(e.session, e.subscriptionId, po->baseOffset, po->droppedMarker)) {
                    e.state = AwaitingAck{};
                    ++work;
                }
                ++i;
                continue;
            }
            if (std::holds_alternative<AwaitingAck>(e.state)) {
                // M1: a session gone before its ack (the admin drops what it owed
                // a closed session) clears the entry — never a leaked slot.
                if (sink.room(e.session) == Room::Gone) { erase_(i); continue; }
                ++i;
                continue;
            }
            if (std::holds_alternative<AwaitingRoom>(e.state)) {
                const Room r = sink.room(e.session);
                if (r == Room::Gone) { erase_(i); continue; }
                if (r == Room::NotYet) { ++i; continue; }
                e.state = ResumeOwed{};
            }
            // ResumeOwed
            if (sink.postResume(e.session, e.subscriptionId)) {
                ++resumes_;
                ++work;
                erase_(i);
                continue;
            }
            ++i;
        }
        return work;
    }

    [[nodiscard]] bool        idle() const noexcept { return entries_.empty(); }
    [[nodiscard]] std::size_t paused() const noexcept { return entries_.size(); }
    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }
    [[nodiscard]] uint64_t    pauses() const noexcept { return pauses_; }
    [[nodiscard]] uint64_t    resumes() const noexcept { return resumes_; }
    [[nodiscard]] uint64_t    dropped() const noexcept { return dropped_; }
    [[nodiscard]] uint64_t    delivered() const noexcept { return delivered_; }
    [[nodiscard]] uint64_t    malformed() const noexcept { return malformed_; }

private:
    Entry* find_(int64_t session, int64_t subId) noexcept {
        for (Entry& e : entries_)
            if (e.session == session && e.subscriptionId == subId) return &e;
        return nullptr;
    }
    void erase_(std::size_t i) noexcept {
        entries_[i] = entries_.back();
        entries_.pop_back();
    }

    std::size_t        capacity_;
    std::vector<Entry> entries_;
    uint64_t pauses_ = 0, resumes_ = 0, dropped_ = 0, delivered_ = 0, malformed_ = 0;
};

} // namespace rune::node
