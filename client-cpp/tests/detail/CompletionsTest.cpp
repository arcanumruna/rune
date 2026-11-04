// tests/detail/CompletionsTest.cpp — F8c C1(b): detail::CompletionRing +
// Completions, standalone (no Client): poll, credit, stash, waitFor with an
// injected pump.  No sleeps: every wait is on another thread's progress or on
// an injected clock.

#include <rune/client/Completions.h>
#include <rune/client/detail/OneToOneRingBuffer.h>
#include <rune/client/producer/ProducerImpl.h>

#include "../support/AllocCounter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using rune::client::Completion;
using rune::client::Completions;
using rune::client::CompletionsConfig;
using rune::client::NoPump;
using rune::client::OpKind;
using rune::client::OpResult;
using rune::client::OpStatus;
using rune::client::Token;
using rune::client::detail::CompletionRing;
using rune::client::detail::OneToOneRingBuffer;
using namespace std::chrono_literals;

namespace {

int64_t g_frozenNow = 0;
int64_t frozenClock() noexcept { return g_frozenNow; }

CompletionsConfig smallCfg(uint32_t cap = 4096, uint32_t maxResult = 256, uint32_t maxErr = 64) {
    CompletionsConfig c;
    c.capacityBytes  = cap;
    c.maxResultBytes = maxResult;
    c.maxErrorText   = maxErr;
    c.nanoClock      = &frozenClock;
    return c;
}

uint8_t patternByte(Token t, std::size_t k) { return static_cast<uint8_t>((t * 31u + k) & 0xFFu); }

/// Duty-side delivery of `t` within reservation `r` with a patterned payload.
bool deliver(Completions& cq, Token t, uint32_t r, uint32_t payloadLen,
             CompletionRing::Overflow ov = CompletionRing::Overflow::Truncate) {
    std::vector<uint8_t> p(payloadLen);
    for (uint32_t k = 0; k < payloadLen; ++k) p[k] = patternByte(t, k);
    Completion c;
    c.token = t;
    c.op = OpKind::Ping;
    c.reservedBytes = r;
    return cq.ring().deliver(c, p, ov);
}

/// Idle-strategy test double: records every idle(work) call; never blocks.
struct CountingIdle {
    int parks = 0;      // idle(0): the real strategy would back off / park
    int busy  = 0;      // idle(>0): the real strategy resets (no park)
    void idle(int work) { (work == 0 ? parks : busy) += 1; }
};

} // namespace

// ---------------------------------------------------------------------------
// Two threads: 1M completions with variable payloads across many wraps.  The
// app thread reserves credit and hands each op to the "duty" thread through an
// op ring; the duty thread delivers the completion; the app thread polls.  Every
// token completes exactly once, in order, with its own payload; no completion is
// ever lost to a full ring.
// ---------------------------------------------------------------------------
TEST(Completions, CompletionRingTwoThreadExactlyOnceRegression) {
    constexpr Token kN = 1'000'000;
    Completions cq(smallCfg(64 * 1024, 1024, 64));
    struct Op { Token token; uint32_t reserved; uint32_t payloadLen; };
    constexpr std::size_t kOpCap = 64 * 1024;
    std::unique_ptr<uint8_t[]> opMem(new uint8_t[kOpCap + OneToOneRingBuffer::trailerLength()]());
    OneToOneRingBuffer ops(opMem.get(), kOpCap + OneToOneRingBuffer::trailerLength());

    std::thread duty([&] {
        Token delivered = 0;
        std::vector<uint8_t> payload(1024);
        while (delivered < kN) {
            ops.read([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                         aeron::util::index_t) {
                Op op;
                std::memcpy(&op, b.buffer() + off, sizeof op);
                for (uint32_t k = 0; k < op.payloadLen; ++k) payload[k] = patternByte(op.token, k);
                Completion c;
                c.token = op.token;
                c.op = OpKind::Ping;
                c.value = static_cast<int64_t>(op.token) * 3;
                c.reservedBytes = op.reserved;
                cq.ring().deliver(c, std::span<const uint8_t>(payload.data(), op.payloadLen));
                ++delivered;
            });
        }
        cq.ring().testProducerClose();
    });

    Token next = 0;
    bool ok = true;
    auto onDone = [&](const Completion& c, std::span<const uint8_t> p) {
        const uint32_t want = static_cast<uint32_t>((c.token * 7919u) % 600u);
        if (c.token != next || c.value != static_cast<int64_t>(c.token) * 3 || p.size() != want) ok = false;
        for (std::size_t k = 0; ok && k < p.size(); ++k) ok = p[k] == patternByte(c.token, k);
        ++next;
    };
    for (Token t = 0; t < kN; ++t) {
        const uint32_t len = static_cast<uint32_t>((t * 7919u) % 600u);
        const uint32_t r = CompletionRing::completionRecordBytes(len);
        while (cq.reserve(r) == OpResult::QueueFull) cq.poll(onDone);  // credit comes back as we poll
        const Op op{t, r, len};
        while (!ops.writeWith(1, static_cast<aeron::util::index_t>(sizeof op), [&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t at) {
            std::memcpy(b.buffer() + at, &op, sizeof op);
        })) {
            cq.poll(onDone);
        }
    }
    while (!cq.finished()) cq.poll(onDone);
    cq.poll(onDone);
    duty.join();
    EXPECT_TRUE(ok) << "a completion arrived out of order, twice, or with a wrong payload";
    EXPECT_EQ(next, kN) << "every token must complete exactly once";
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.ring().lostCompletions(), 0u);
}

// ---------------------------------------------------------------------------
// Fuzzed op mix driven to the credit limit (QueueFull) over and over, with
// payloads up to 2x the reservation (truncated / ResultTooLarge), partial polls
// and waitFor()s that stash: the duty side's write never fails.
// ---------------------------------------------------------------------------
TEST(Completions, CreditNeverOverflowsRingRegression) {
    Completions cq(smallCfg(4096, 256, 64));
    struct Pending { Token token; uint32_t reserved; OpKind kind; };
    std::deque<Pending> issued;       // accepted, not yet delivered by the "duty" side
    std::vector<int> completions;     // per token: times delivered to the app
    uint64_t rng = 0x9E3779B97F4A7C15ull;
    auto next = [&] { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; };
    const OpKind kinds[] = {OpKind::Send, OpKind::Ping, OpKind::GetTopicSchema, OpKind::Connect};
    Token token = 0;
    int queueFulls = 0;
    auto onDone = [&](const Completion& c, std::span<const uint8_t>) { ++completions[c.token]; };

    for (int step = 0; step < 200'000; ++step) {
        const uint64_t a = next() % 10;
        if (a < 5) {  // issue until the credit limit
            const OpKind k = kinds[next() % 4];
            const uint32_t echo = static_cast<uint32_t>(next() % 200);
            const uint32_t r = cq.reservationFor(k, echo);
            const OpResult res = cq.reserve(r);
            if (res == OpResult::QueueFull) { ++queueFulls; continue; }
            ASSERT_EQ(res, OpResult::Accepted);
            completions.push_back(0);
            issued.push_back({token++, r, k});
        } else if (a < 8) {  // the duty side delivers everything issued so far
            while (!issued.empty()) {
                const Pending p = issued.front();
                issued.pop_front();
                // Half the time a payload that fills its whole reservation (the
                // worst case for the ring), otherwise anything up to 2x it.
                const uint32_t len = (next() & 1) ? p.reserved
                                                  : static_cast<uint32_t>(next() % (2 * p.reserved));
                ASSERT_TRUE(deliver(cq, p.token, p.reserved, len,
                                    p.kind == OpKind::GetTopicSchema ? CompletionRing::Overflow::TooLarge
                                                                     : CompletionRing::Overflow::Truncate))
                    << "step " << step << ": the ring refused a reserved completion";
            }
        } else if (a < 9) {
            cq.poll(onDone, static_cast<int>(next() % 4));
        } else if (token > 0) {  // waitFor a random token: stashes what it reads past
            const Token t = next() % token;
            (void)cq.waitFor(t, 0ns, onDone);  // frozen clock: one pass, never idles
        }
    }
    while (!issued.empty()) {
        const Pending p = issued.front();
        issued.pop_front();
        ASSERT_TRUE(deliver(cq, p.token, p.reserved, 0));
    }
    while (cq.poll(onDone) > 0) {}
    EXPECT_GT(queueFulls, 1000) << "the fuzz must actually hit the credit limit";
    EXPECT_EQ(cq.ring().lostCompletions(), 0u);
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    for (Token t = 0; t < token; ++t) ASSERT_EQ(completions[t], 1) << "token " << t;
}

// ---------------------------------------------------------------------------
// Why the credit keeps one maximal record of slack: the ring can need an
// end-of-buffer padding record on top of the records themselves.  Deterministic
// worst case on a 1 KiB ring: head at 48, 864 bytes in use (tail index 912), and
// a 136-byte record that does not fit the 112 bytes to the end and cannot wrap
// in front of the head.  Without the slack that reservation would be granted
// (1000 <= 1024) and the duty side's write would fail.
// ---------------------------------------------------------------------------
TEST(Completions, CreditReservesWrapPaddingSlackRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    const uint32_t small = cq.reservationFor(OpKind::Send);            // 48
    const uint32_t big   = cq.reservationFor(OpKind::GetTopicSchema);  // 136 (the maximal record)
    ASSERT_EQ(small, 48u);
    ASSERT_EQ(big, 136u);
    ASSERT_EQ(cq.reserve(small), OpResult::Accepted);
    ASSERT_TRUE(deliver(cq, 0, small, 0));
    ASSERT_EQ(cq.poll([](const Completion&, std::span<const uint8_t>) {}), 1);  // head = 48
    Token t = 1;
    for (int i = 0; i < 6; ++i, ++t) {
        ASSERT_EQ(cq.reserve(big), OpResult::Accepted);
        ASSERT_TRUE(deliver(cq, t, big, 88));
    }
    ASSERT_EQ(cq.reserve(small), OpResult::Accepted);
    ASSERT_TRUE(deliver(cq, t++, small, 0));                             // 864 in use, tail index 912
    const OpResult r = cq.reserve(big);
    EXPECT_EQ(r, OpResult::QueueFull) << "1000 bytes reserved + one maximal record of slack > 1024";
    if (r == OpResult::Accepted) EXPECT_TRUE(deliver(cq, t, big, 88)) << "the write the slack prevents";
    EXPECT_EQ(cq.ring().lostCompletions(), 0u);
    ASSERT_EQ(cq.poll([](const Completion&, std::span<const uint8_t>) {}, 1), 1);
    ASSERT_EQ(cq.reserve(big), OpResult::Accepted) << "credit returns as completions are polled";
    EXPECT_TRUE(deliver(cq, 100, big, 88));
    EXPECT_EQ(cq.ring().lostCompletions(), 0u);
}

// ---------------------------------------------------------------------------
// waitFor() delivers its own token and stashes the others it reads past;
// poll() then returns the stashed ones FIRST, in their original order; a later
// waitFor() for a stashed token is served from the stash.
// ---------------------------------------------------------------------------
TEST(Completions, WaitForStashesOtherTokensInOrderRegression) {
    Completions cq(smallCfg());
    const uint32_t r = cq.reservationFor(OpKind::Ping, 8);
    for (Token t = 1; t <= 6; ++t) ASSERT_EQ(cq.reserve(r), OpResult::Accepted);
    for (Token t = 1; t <= 4; ++t) ASSERT_TRUE(deliver(cq, t, r, static_cast<uint32_t>(t)));

    std::vector<Token> got;
    auto record = [&](const Completion& c, std::span<const uint8_t> p) {
        got.push_back(c.token);
        EXPECT_EQ(p.size(), c.token);
        for (std::size_t k = 0; k < p.size(); ++k) EXPECT_EQ(p[k], patternByte(c.token, k));
    };
    EXPECT_EQ(cq.waitFor(3, 1s, record), Completions::Wait::Done);
    EXPECT_EQ(got, (std::vector<Token>{3}));
    EXPECT_EQ(cq.stashed(), 3u) << "1,2 (read before 3) and 4 (same read) are stashed";
    for (Token t = 5; t <= 6; ++t) ASSERT_TRUE(deliver(cq, t, r, static_cast<uint32_t>(t)));
    EXPECT_EQ(cq.waitFor(2, 1s, record), Completions::Wait::Done) << "served from the stash";
    EXPECT_EQ(cq.stashed(), 2u);
    EXPECT_EQ(cq.poll(record), 4);
    EXPECT_EQ(got, (std::vector<Token>{3, 2, 1, 4, 5, 6}))
        << "stashed completions are older than anything still in the ring: they come first";
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
}

// ---------------------------------------------------------------------------
// Invoker form: the waiting thread IS the duty thread, so waitFor pumps the
// injected duty cycle itself.  While the pump reports work it never parks (the
// idle strategy only ever sees work > 0); the completion the pump produces is
// found; other tokens produced meanwhile are stashed.  Once idle it may back off.
// ---------------------------------------------------------------------------
TEST(Completions, WaitForInvokerPumpsAndNeverParksWhileWorkRegression) {
    Completions cq(smallCfg());
    const uint32_t r = cq.reservationFor(OpKind::Connect);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(cq.reserve(r), OpResult::Accepted);
    int pumps = 0;
    auto dutyCycle = [&] {  // the Invoker duty cycle: always busy, completes token 7 on cycle 50
        ++pumps;
        if (pumps == 10) EXPECT_TRUE(deliver(cq, 100, r, 0));
        if (pumps == 50) EXPECT_TRUE(deliver(cq, 7, r, 0));
        return 1;
    };
    CountingIdle idle;
    int done = 0;
    EXPECT_EQ(cq.waitFor(7, 1h, [&](const Completion& c, std::span<const uint8_t>) {
                  EXPECT_EQ(c.token, 7u);
                  ++done;
              }, dutyCycle, idle),
              Completions::Wait::Done);
    EXPECT_EQ(done, 1);
    EXPECT_EQ(pumps, 50) << "it pumps exactly until its token arrives";
    EXPECT_EQ(idle.parks, 0) << "never parks while the duty cycle reports work";
    EXPECT_EQ(idle.busy, 49);
    EXPECT_EQ(cq.stashed(), 1u) << "token 100 seen while waiting is kept for poll()";

    // An idle duty cycle: the wait backs off (idle(0)) and times out on the
    // injected clock without the op being lost.
    CountingIdle idle2;
    int idlePumps = 0;
    auto quiet = [&] { if (++idlePumps == 3) g_frozenNow += 2'000'000'000; return 0; };
    EXPECT_EQ(cq.waitFor(8, 1s, [](const Completion&, std::span<const uint8_t>) {}, quiet, idle2),
              Completions::Wait::TimedOut);
    EXPECT_EQ(idle2.parks, 2);
    EXPECT_EQ(cq.outstanding(), 2u) << "a timed-out wait does not consume anything";
}

// ---------------------------------------------------------------------------
// Final drain: the duty thread delivers, then producer-closes.  An owner that
// observes producerClosed() and THEN drains sees every completion (the close is
// a release, the observation an acquire); afterwards the queue is finished(),
// waitFor() for anything else returns NotPending without pumping, and new
// reservations are refused (Closed).
// ---------------------------------------------------------------------------
TEST(Completions, FinalDrainAfterProducerCloseRegression) {
    constexpr int kK = 500;
    Completions cq(smallCfg(64 * 1024, 256, 64));
    const uint32_t r = cq.reservationFor(OpKind::Ping, 16);
    for (int i = 0; i < kK; ++i) ASSERT_EQ(cq.reserve(r), OpResult::Accepted);
    std::thread duty([&] {
        for (int i = 0; i < kK; ++i) deliver(cq, static_cast<Token>(i), r, 16);
        cq.ring().testProducerClose();
    });
    int got = 0;
    auto count = [&](const Completion&, std::span<const uint8_t>) { ++got; };
    while (!cq.ring().producerClosed()) {}  // wait for the duty thread's close
    cq.poll(count);                          // ONE drain after observing it
    duty.join();
    EXPECT_EQ(got, kK) << "a drain after observing the close must see every completion";
    EXPECT_TRUE(cq.finished());
    int pumped = 0;
    EXPECT_EQ(cq.waitFor(kK + 1, 1h, count, [&] { return ++pumped; }), Completions::Wait::NotPending);
    EXPECT_EQ(pumped, 0) << "never pumps once the producer side closed";
    EXPECT_EQ(cq.reserve(r), OpResult::Closed);
    EXPECT_EQ(cq.ring().lostCompletions(), 0u);
}

// ---------------------------------------------------------------------------
// The duty side stays within each reservation: error text is truncated, an
// oversize result becomes ResultTooLarge carrying the real size.
// ---------------------------------------------------------------------------
TEST(Completions, DeliverStaysWithinReservationRegression) {
    Completions cq(smallCfg(4096, 256, 64));
    const uint32_t err = cq.reservationFor(OpKind::DefineTopic);
    const uint32_t res = cq.reservationFor(OpKind::GetTopicSchema);
    ASSERT_EQ(cq.reserve(err), OpResult::Accepted);
    ASSERT_EQ(cq.reserve(res), OpResult::Accepted);
    ASSERT_TRUE(deliver(cq, 1, err, 500, CompletionRing::Overflow::Truncate));
    ASSERT_TRUE(deliver(cq, 2, res, 300, CompletionRing::Overflow::TooLarge));
    std::vector<Completion> got;
    std::vector<std::size_t> lens;
    cq.poll([&](const Completion& c, std::span<const uint8_t> p) { got.push_back(c); lens.push_back(p.size()); });
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(lens[0], 64u) << "error text truncated to maxErrorText";
    EXPECT_EQ(got[1].status, OpStatus::ResultTooLarge);
    EXPECT_EQ(got[1].value, 300);
    EXPECT_EQ(lens[1], 0u);
    EXPECT_EQ(cq.reserve(cq.maxReservation() + 8), OpResult::InvalidArgument);
    EXPECT_EQ(cq.reserve(err + 3), OpResult::InvalidArgument) << "an unaligned reservation is refused";
}

// Review fix round (blocker): a View batch completion above the slack record M reserves 2r - 48 (record + its own wrap
// padding) and the ring caps what it writes at (R + 48) / 2, so large completions never lose one.  Seeded randomized model on
// a standalone queue: reserve a batch completion, deliver a record of the FULL size r for a random outstanding reservation,
// poll; nothing is ever lost, every payload comes back byte-equal, the credit balances to zero.
TEST(CompletionsLargeRecordsRegression, CompletionRingLargeRecordsNeverLoseACompletionRegression) {
    struct Case { uint32_t cap; uint32_t maxResult; };
    // 64 KiB: maxMsg 8 KiB, M = 2096 (a default 16 KiB maxResultBytes does not fit a 64 KiB ring: Completions::validate).
    // 1 MiB: the default config (M = 16432), maxMsg 128 KiB; View batches stop at 4096 records.
    for (const Case cs : {Case{64u << 10, 2048u}, Case{1u << 20, 16u << 10}}) {
        for (const uint64_t seed : {1ull, 0xC0FFEEull, 12345ull}) {
            CompletionsConfig cfg;
            cfg.capacityBytes  = cs.cap;
            cfg.maxResultBytes = cs.maxResult;
            Completions cq(cfg);
            const uint32_t maxRec = CompletionRing::kRecordHeaderBytes + cq.maxMsgLength();
            uint32_t nMax = (maxRec - CompletionRing::completionRecordBytes(0)) / 8;
            if (nMax > 4096) nMax = 4096;
            ASSERT_GT(CompletionRing::completionRecordBytes(8 * nMax), cq.maxRecordBytes()) << "the model must reach large records";
            ASSERT_GE(cq.maxReservation(), rune::client::producer::batchCompletionBytes(true, nMax));

            uint64_t rng = seed * 0x9E3779B97F4A7C15ull + 1;
            auto next = [&]() { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; };
            struct Held { Token t; uint32_t n; uint32_t resv; };
            std::vector<Held> held;                  // reserved, not yet delivered
            std::deque<Held> inRing;                 // delivered, not yet polled (FIFO)
            Token nextTok = 1;
            uint64_t reserved = 0, polled = 0;
            auto pollSome = [&](uint32_t limit) {
                (void)cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
                    ASSERT_FALSE(inRing.empty());
                    const Held h = inRing.front();
                    inRing.pop_front();
                    EXPECT_EQ(c.token, h.t);
                    ASSERT_EQ(p.size(), h.n > 1 ? 8u * h.n : 0u);
                    for (std::size_t k = 0; k < p.size(); ++k) {
                        if (p[k] != patternByte(h.t, k)) { ADD_FAILURE() << "payload byte " << k << " of token " << h.t; break; }
                    }
                    ++polled;
                }, static_cast<int>(limit));
            };
            for (int step = 0; step < 100'000; ++step) {
                const uint32_t op = static_cast<uint32_t>(next() % 100);
                if (op < 40) {
                    const uint32_t n = (next() & 1) ? 1 + static_cast<uint32_t>(next() % nMax) : 1 + static_cast<uint32_t>(next() % 64);
                    const uint32_t resv = rune::client::producer::batchCompletionBytes(true, n);
                    const OpResult r = cq.reserve(resv);
                    ASSERT_TRUE(r == OpResult::Accepted || r == OpResult::QueueFull) << "n=" << n;
                    if (r == OpResult::Accepted) { held.push_back({nextTok++, n, resv}); ++reserved; }
                } else if (op < 75) {
                    if (held.empty()) continue;
                    const std::size_t i = next() % held.size();
                    const Held h = held[i];
                    held[i] = held.back();
                    held.pop_back();
                    ASSERT_TRUE(deliver(cq, h.t, h.resv, h.n > 1 ? 8u * h.n : 0u)) << "step " << step << " n=" << h.n;
                    inRing.push_back(h);
                } else {
                    pollSome(1 + static_cast<uint32_t>(next() % 8));
                }
                ASSERT_EQ(cq.ring().lostCompletions(), 0u) << "step " << step;
            }
            while (!held.empty()) {
                const Held h = held.back();
                held.pop_back();
                ASSERT_TRUE(deliver(cq, h.t, h.resv, h.n > 1 ? 8u * h.n : 0u));
                inRing.push_back(h);
            }
            while (!inRing.empty()) pollSome(64);
            EXPECT_EQ(polled, reserved) << "cap " << cs.cap << " seed " << seed;
            EXPECT_EQ(cq.reservedBytes(), 0u) << "the credit balances";
            EXPECT_EQ(cq.ring().lostCompletions(), 0u);
        }
    }
}

// ===========================================================================
// Batch 1 adversarial pass: the completion-ring credit contract.
// ===========================================================================

namespace {

/// Fills `cq`'s ring with consumed records so that both head and tail sit at byte `pos` (8-aligned, 0 or >= 48): the next
/// record is written there.  Every record it uses is reserved, delivered and polled, so the credit returns to zero.
void parkRingAt(Completions& cq, uint32_t pos) {
    ASSERT_EQ(pos % 8, 0u);
    ASSERT_TRUE(pos == 0 || pos >= 48);
    uint32_t at = 0;
    Token t = 1u << 30;
    while (at < pos) {
        int batch = 0;
        for (;;) {
            if (at == pos) break;
            const uint32_t rec = pos - at > 96 ? 48u : pos - at;   // 48-byte steps; the last closes the gap exactly (48..96)
            if (cq.reserve(rec) != OpResult::Accepted) break;
            if (rec == 48) {
                Completion c;
                c.token = t++;
                c.reservedBytes = 48;
                ASSERT_TRUE(cq.ring().deliver(c, {}));
            } else {
                ASSERT_TRUE(deliver(cq, t++, rec, rec - 48));
            }
            at += rec;
            ++batch;
        }
        int polled = 0;
        while (polled < batch) {
            const int got = cq.poll([](const Completion&, std::span<const uint8_t>) {});
            ASSERT_GT(got, 0);
            polled += got;
        }
    }
    ASSERT_EQ(cq.reservedBytes(), 0u);
    ASSERT_EQ(cq.outstanding(), 0u);
}

}  // namespace

// Input bounds: n = 0 / 1 / 2 / max / max + 1 of a View batch completion, and the reservation at maxReservation and above it.
// A reservation of exactly maxReservation() is accepted and its largest record (header + maxMsgLength) is delivered whole.
TEST(CompletionsAdversarialRegression, BatchCompletionBoundsAtZeroOneMaxAndMaxPlusOneAdversarialRegression) {
    using rune::client::producer::batchCompletionBytes;
    EXPECT_EQ(batchCompletionBytes(true, 0), 48u) << "n = 0 is refused before it reserves; it never asks for more than one record";
    EXPECT_EQ(batchCompletionBytes(true, 1), 48u) << "a 1-record batch keeps single-record semantics";
    EXPECT_EQ(batchCompletionBytes(false, 4096), 48u) << "no View: the plain record";
    EXPECT_EQ(batchCompletionBytes(true, 2), 2u * CompletionRing::completionRecordBytes(16) - 48u);
    EXPECT_EQ(batchCompletionBytes(true, 4096), 2u * CompletionRing::completionRecordBytes(8u * 4096u) - 48u);

    Completions cq;   // the default queue: 1 MiB, M = 16432
    ASSERT_EQ(cq.maxRecordBytes(), 16432u);
    ASSERT_EQ(cq.maxMsgLength(), (1u << 20) / 8);
    const uint32_t maxRec = CompletionRing::kRecordHeaderBytes + cq.maxMsgLength();
    EXPECT_EQ(cq.maxReservation(), 2u * maxRec - 48u);
    EXPECT_GE(cq.maxReservation(), batchCompletionBytes(true, 4096)) << "4096 records fit a default queue";
    EXPECT_EQ(cq.reserve(cq.maxReservation() + 8), OpResult::InvalidArgument) << "max + 1 (aligned)";
    EXPECT_EQ(cq.reserve(cq.maxReservation() + 1), OpResult::InvalidArgument) << "max + 1 (unaligned)";
    EXPECT_EQ(cq.reserve(47), OpResult::InvalidArgument) << "below one bare completion";
    EXPECT_EQ(cq.reserve(0), OpResult::InvalidArgument);

    // Park the tail where the largest record cannot fit before the end: it must wrap and leave the largest padding.
    parkRingAt(cq, (1u << 20) - maxRec + 8);
    ASSERT_EQ(cq.reserve(cq.maxReservation()), OpResult::Accepted);
    // Fill the rest of the credit with small records; the big one goes first (it wraps with the largest padding), then
    // every small one meets the fullest ring.
    std::vector<Token> small;
    for (Token t = 100; cq.reserve(48) == OpResult::Accepted; ++t) small.push_back(t);
    ASSERT_TRUE(deliver(cq, 7, cq.maxReservation(), cq.maxMsgLength() - CompletionRing::kCompletionBytes))
        << "the largest record of the largest reservation, wrapping with the largest padding";
    for (const Token t : small) ASSERT_TRUE(deliver(cq, t, 48, 0));
    EXPECT_EQ(cq.ring().lostCompletions(), 0u);
    std::size_t bigLen = 0;
    int got = 0;
    while (cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
               ++got;
               if (c.token == 7) {
                   bigLen = p.size();
                   for (std::size_t k = 0; k < p.size(); ++k)
                       if (p[k] != patternByte(7, k)) { ADD_FAILURE() << "byte " << k; break; }
               }
           }) > 0) {}
    EXPECT_EQ(got, static_cast<int>(small.size()) + 1);
    EXPECT_EQ(bigLen, cq.maxMsgLength() - CompletionRing::kCompletionBytes) << "nothing truncated: the cap is the largest record";
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.outstanding(), 0u);
}

// Wrap at every tail offset: on a 64 KiB queue (M = 2096, maxMsg 8 KiB) park the ring at every 8-aligned byte, hold one large
// reservation (the largest a View batch can make here) plus small ones up to the credit limit, and deliver the large one first,
// last, or in the middle.  The large record lands at every position relative to the buffer end; nothing is ever lost.
TEST(CompletionsAdversarialRegression, LargeAmongSmallAtEveryTailOffsetNeverLosesACompletionAdversarialRegression) {
    using rune::client::producer::batchCompletionBytes;
    CompletionsConfig cfg;
    cfg.capacityBytes  = 64u << 10;
    cfg.maxResultBytes = 2048;
    const uint32_t cap = cfg.capacityBytes;
    uint32_t nMax = 0;
    {
        Completions probe(cfg);
        nMax = (CompletionRing::kRecordHeaderBytes + probe.maxMsgLength() - CompletionRing::completionRecordBytes(0)) / 8;
        ASSERT_GT(CompletionRing::completionRecordBytes(8 * nMax), probe.maxRecordBytes());
    }
    uint64_t positions = 0;
    for (uint32_t pos = 48; pos < cap; pos += 8) {
        for (int order = 0; order < 3; ++order) {
            Completions cq(cfg);
            parkRingAt(cq, pos);
            if (::testing::Test::HasFatalFailure()) return;
            const uint32_t n = nMax - (pos / 8) % 64;   // vary the large record's size too
            const uint32_t big = batchCompletionBytes(true, n);
            ASSERT_EQ(cq.reserve(big), OpResult::Accepted) << "pos " << pos;
            std::vector<Token> small;
            for (Token t = 100; cq.reserve(48) == OpResult::Accepted; ++t) small.push_back(t);
            const std::size_t mid = small.size() / 2;
            auto deliverBig = [&] {
                ASSERT_TRUE(deliver(cq, 7, big, 8 * n)) << "pos " << pos << " order " << order;
            };
            if (order == 0) deliverBig();
            for (std::size_t i = 0; i < small.size(); ++i) {
                if (order == 2 && i == mid) deliverBig();
                ASSERT_TRUE(deliver(cq, small[i], 48, 0)) << "pos " << pos << " order " << order << " small " << i;
            }
            if (order == 1) deliverBig();
            ASSERT_EQ(cq.ring().lostCompletions(), 0u) << "pos " << pos << " order " << order;
            int got = 0;
            bool bigOk = false;
            while (cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
                       ++got;
                       if (c.token == 7) bigOk = p.size() == 8u * n && p[0] == patternByte(7, 0) && p.back() == patternByte(7, p.size() - 1);
                   }) > 0) {}
            ASSERT_EQ(got, static_cast<int>(small.size()) + 1) << "pos " << pos;
            ASSERT_TRUE(bigOk) << "pos " << pos;
            ASSERT_EQ(cq.reservedBytes(), 0u);
            ++positions;
        }
    }
    EXPECT_EQ(positions, 3u * ((cap - 48) / 8));
}

// A broker that answers with MORE verdict entries than the batch has (or a reservation smaller than the payload): deliver()
// caps the record at what the reservation stands for — Truncate keeps the first bytes, TooLarge reports ResultTooLarge with
// the real size — and a ring kept at its credit limit never loses one, over a seeded mix of small and large reservations.
TEST(CompletionsAdversarialRegression, OverDeliveryBeyondTheReservationIsCappedNeverLostAdversarialRegression) {
    using rune::client::producer::batchCompletionBytes;
    CompletionsConfig cfg;
    cfg.capacityBytes  = 64u << 10;
    cfg.maxResultBytes = 2048;
    for (const uint64_t seed : {7ull, 0xBADC0DEull, 99991ull}) {
        Completions cq(cfg);
        const uint32_t maxPayload = cq.maxMsgLength() - CompletionRing::kCompletionBytes;
        const uint32_t nMax = (maxPayload) / 8;
        uint64_t rng = seed;
        auto next = [&] { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; };
        struct Held { Token t; uint32_t resv; uint32_t standFor; };
        std::vector<Held> held;
        std::deque<std::pair<Token, std::size_t>> expect;   // token, expected payload length (TooLarge: SIZE_MAX)
        Token tok = 1;
        uint64_t issued = 0, polled = 0;
        for (int step = 0; step < 60'000; ++step) {
            const uint64_t a = next() % 10;
            if (a < 4) {
                const bool large = next() & 1;
                const uint32_t n = large ? 2 + static_cast<uint32_t>(next() % (nMax - 1)) : 1 + static_cast<uint32_t>(next() % 8);
                const uint32_t resv = large ? batchCompletionBytes(true, n) : CompletionRing::completionRecordBytes(8 * n);
                if (cq.reserve(resv) != OpResult::Accepted) continue;
                uint32_t standFor = resv;
                if (resv > cq.maxRecordBytes()) {
                    standFor = ((resv + 48) / 2) & ~7u;
                    if (standFor < cq.maxRecordBytes()) standFor = cq.maxRecordBytes();
                }
                if (standFor > CompletionRing::kRecordHeaderBytes + cq.maxMsgLength())
                    standFor = CompletionRing::kRecordHeaderBytes + cq.maxMsgLength();
                held.push_back({tok++, resv, standFor});
                ++issued;
            } else if (a < 7) {
                if (held.empty()) continue;
                const std::size_t i = next() % held.size();
                const Held h = held[i];
                held[i] = held.back();
                held.pop_back();
                const uint32_t room = h.standFor - 48;
                const uint32_t len = static_cast<uint32_t>(room + 8 + next() % 4096);   // always more than fits
                const bool tooLarge = next() & 1;
                ASSERT_TRUE(deliver(cq, h.t, h.resv, len, tooLarge ? CompletionRing::Overflow::TooLarge : CompletionRing::Overflow::Truncate))
                    << "seed " << seed << " step " << step;
                expect.emplace_back(h.t, tooLarge ? SIZE_MAX : room);
            } else {
                (void)cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
                    ASSERT_FALSE(expect.empty());
                    const auto [t, len] = expect.front();
                    expect.pop_front();
                    ASSERT_EQ(c.token, t);
                    if (len == SIZE_MAX) {
                        EXPECT_EQ(c.status, OpStatus::ResultTooLarge);
                        EXPECT_EQ(p.size(), 0u);
                        EXPECT_GT(c.value, 0);
                    } else {
                        EXPECT_EQ(p.size(), len) << "truncated to what the reservation stands for";
                    }
                    ++polled;
                }, 1 + static_cast<int>(next() % 6));
            }
            ASSERT_EQ(cq.ring().lostCompletions(), 0u) << "seed " << seed << " step " << step;
        }
        for (const Held& h : held) { ASSERT_TRUE(deliver(cq, h.t, h.resv, 0)); expect.emplace_back(h.t, 0); }
        while (cq.poll([&](const Completion&, std::span<const uint8_t>) { expect.pop_front(); ++polled; }) > 0) {}
        EXPECT_EQ(polled, issued);
        EXPECT_EQ(cq.reservedBytes(), 0u);
        EXPECT_EQ(cq.ring().lostCompletions(), 0u);
    }
}

// waitFor() stashes large records it reads past; the stash is as large as the ring and holds only credited records, so
// large and small records stashed out of order, then taken out of order, never overflow it and come back byte-equal.
TEST(CompletionsAdversarialRegression, WaitForStashesLargeRecordsOutOfOrderAdversarialRegression) {
    using rune::client::producer::batchCompletionBytes;
    CompletionsConfig cfg;
    cfg.capacityBytes  = 64u << 10;
    cfg.maxResultBytes = 2048;
    cfg.nanoClock      = &frozenClock;
    Completions cq(cfg);
    const uint32_t nMax = (cq.maxMsgLength() - CompletionRing::kCompletionBytes) / 8;
    uint64_t rng = 0x5EED;
    auto next = [&] { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; };
    struct Op { uint32_t n; uint32_t resv; };
    std::vector<Op> ops(1);   // token 0 unused
    std::vector<int> done;
    done.push_back(0);
    std::vector<Token> undelivered, inFlight;
    auto check = [&](const Completion& c, std::span<const uint8_t> p) {
        ASSERT_LT(c.token, ops.size());
        const Op& o = ops[c.token];
        ASSERT_EQ(p.size(), o.n > 1 ? 8u * o.n : 0u) << "token " << c.token;
        if (!p.empty()) { EXPECT_EQ(p[0], patternByte(c.token, 0)); EXPECT_EQ(p.back(), patternByte(c.token, p.size() - 1)); }
        ++done[c.token];
    };
    for (int step = 0; step < 40'000; ++step) {
        const uint64_t a = next() % 10;
        if (a < 4) {
            const uint32_t n = (next() & 3) == 0 ? 2 + static_cast<uint32_t>(next() % (nMax - 1)) : 1;
            const uint32_t resv = batchCompletionBytes(true, n);
            if (cq.reserve(resv) != OpResult::Accepted) continue;
            ops.push_back({n, resv});
            done.push_back(0);
            undelivered.push_back(ops.size() - 1);
        } else if (a < 7) {
            if (undelivered.empty()) continue;
            const std::size_t i = next() % undelivered.size();
            const Token t = undelivered[i];
            undelivered[i] = undelivered.back();
            undelivered.pop_back();
            ASSERT_TRUE(deliver(cq, t, ops[t].resv, ops[t].n > 1 ? 8u * ops[t].n : 0u)) << "step " << step;
            inFlight.push_back(t);
        } else if (a < 9) {
            if (inFlight.empty()) continue;
            const std::size_t i = next() % inFlight.size();
            const Token t = inFlight[i];
            inFlight[i] = inFlight.back();
            inFlight.pop_back();
            ASSERT_EQ(cq.waitFor(t, 0ns, check), Completions::Wait::Done) << "step " << step;
        } else {
            (void)cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
                check(c, p);
                for (std::size_t k = 0; k < inFlight.size(); ++k)
                    if (inFlight[k] == c.token) { inFlight[k] = inFlight.back(); inFlight.pop_back(); break; }
            }, 1 + static_cast<int>(next() % 4));
        }
        ASSERT_EQ(cq.ring().lostCompletions(), 0u) << "step " << step;
    }
    for (const Token t : undelivered) ASSERT_TRUE(deliver(cq, t, ops[t].resv, ops[t].n > 1 ? 8u * ops[t].n : 0u));
    while (cq.poll(check) > 0) {}
    for (Token t = 1; t < ops.size(); ++t) ASSERT_EQ(done[t], 1) << "token " << t;
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.outstanding(), 0u);
}

// The single-owner contract lets a poll() handler issue the NEXT op (reserve, then hand it to the duty side) — the usual
// pipelined loop.  The credit of a completion returns the moment its handler runs, but the ring's head only moves when the
// whole read batch ends, so a duty thread that delivers the new op's completion inside that window meets a ring fuller than
// the credit says.  Here the "duty side" delivers inside the handler (what a fast duty thread does in Thread mode).
// URGENT break found by the batch 1 adversarial pass (lost completion -> its op never completes): poll and waitFor used
// to return a record's credit inside the ring read, before OneToOneRingBuffer::read publishes the head.  The credit is
// now applied after the read.
TEST(CompletionsAdversarialRegression, PollHandlerThatReissuesNeverLosesACompletionAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));   // M = 136: 18 outstanding 48-byte ops fill the credit
    Token next = 0;
    std::vector<int> times(64, 0);
    while (cq.reserve(48) == OpResult::Accepted) ASSERT_TRUE(deliver(cq, next++, 48, 0));
    const Token first = next;
    ASSERT_EQ(first, 18u);
    std::function<void(const Completion&, std::span<const uint8_t>)> onDone;
    onDone = [&](const Completion& c, std::span<const uint8_t>) {
        ++times[c.token];
        if (next < times.size() && cq.reserve(48) == OpResult::Accepted) {   // reissue: the credit says there is room
            (void)deliver(cq, next++, 48, 0);
        }
    };
    for (int pass = 0; pass < 100 && !(cq.outstanding() == 0 && next == times.size()); ++pass) cq.poll(onDone);
    EXPECT_EQ(cq.ring().lostCompletions(), 0u) << "a reserved completion was refused by the ring: it is lost, its op never completes";
    for (Token t = 0; t < next; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
    EXPECT_EQ(cq.outstanding(), 0u) << "a lost completion leaves its op outstanding forever";
}

// Same window through waitFor(): the matching record's onDone reissues while the ring is full of other completions.
TEST(CompletionsAdversarialRegression, WaitForHandlerThatReissuesNeverLosesACompletionAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    Token next = 0;
    std::vector<int> times(64, 0);
    while (cq.reserve(48) == OpResult::Accepted) ASSERT_TRUE(deliver(cq, next++, 48, 0));
    ASSERT_EQ(next, 18u);
    std::function<void(const Completion&, std::span<const uint8_t>)> onDone;
    uint32_t reservedBeforeWait = 0;
    onDone = [&](const Completion& c, std::span<const uint8_t>) {
        ++times[c.token];
        // The handled record is still in the ring (its head moves after the read): its credit is not back yet.
        // (Only token 0 comes from the ring; the rest are taken from the stash, whose credit is returned at once.)
        if (c.token == 0) EXPECT_EQ(cq.reservedBytes(), reservedBeforeWait);
        if (next < times.size() && cq.reserve(48) == OpResult::Accepted) {
            (void)deliver(cq, next++, 48, 0);
            reservedBeforeWait += 48;
        }
    };
    Token want = 0;
    for (int step = 0; step < 1000 && want < times.size(); ++step) {
        if (want >= next) break;
        reservedBeforeWait = cq.reservedBytes();
        ASSERT_EQ(cq.waitFor(want, 0ns, onDone), Completions::Wait::Done) << "token " << want;
        ++want;
    }
    while (cq.poll(onDone) > 0) {}
    EXPECT_EQ(cq.ring().lostCompletions(), 0u) << "a reserved completion was refused by the ring";
    for (Token t = 0; t < next; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
    EXPECT_EQ(next, times.size());
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
}

// A handler that throws still gets the credit of every record already handed over (the ring head advances on unwind).
TEST(CompletionsAdversarialRegression, PollHandlerThatThrowsStillReturnsTheCreditRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    Token next = 0;
    while (cq.reserve(48) == OpResult::Accepted) ASSERT_TRUE(deliver(cq, next++, 48, 0));
    const uint32_t total = next;
    ASSERT_EQ(total, 18u);
    const uint32_t resv0 = cq.reservedBytes();
    ASSERT_EQ(cq.outstanding(), total);
    int seen = 0;
    const int k = 5;
    EXPECT_THROW(cq.poll([&](const Completion&, std::span<const uint8_t>) {
        if (++seen == k) throw std::runtime_error("handler");
    }), std::runtime_error);
    EXPECT_EQ(seen, k);
    EXPECT_EQ(cq.outstanding(), total - k);
    EXPECT_EQ(cq.reservedBytes(), resv0 - k * 48u);
    while (cq.poll([](const Completion&, std::span<const uint8_t>) {}) > 0) {}
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
}

// ===========================================================================
// Batch 1 fix-round re-attack: the deferred credit (Completions::DeferredCredit).
// ===========================================================================
namespace {
bool deliverKind(Completions& cq, Token t, OpKind op, uint32_t r) {
    Completion c;
    c.token = t;
    c.op = op;
    c.reservedBytes = r;
    return cq.ring().deliver(c, {}, CompletionRing::Overflow::Truncate);
}
}  // namespace

// A BatchAck returns `units` reservations in one record: inside the handler none of them is back yet (the record is still
// in the ring); after poll() returns, all of them are, and the queue takes exactly that many new ops again.
TEST(CompletionsAdversarialRegression, BatchAckUnitsReturnAfterTheReadAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));   // M = 136: 18 outstanding 48-byte reservations fill the credit
    constexpr uint32_t kUnits = 7;
    for (uint32_t i = 0; i < kUnits; ++i) ASSERT_EQ(cq.reserve(48), OpResult::Accepted);
    Token next = 100;
    while (cq.reserve(48) == OpResult::Accepted) ASSERT_TRUE(deliver(cq, next++, 48, 0));
    const uint32_t singles = static_cast<uint32_t>(next - 100);
    ASSERT_EQ(singles + kUnits, 18u);
    ASSERT_TRUE(deliverKind(cq, 0, OpKind::BatchAck, kUnits * rune::client::kBatchAckUnitBytes));
    int acks = 0;
    uint32_t insideOutstanding = 0, insideReserved = 0;
    const int n = cq.poll([&](const Completion& c, std::span<const uint8_t>) {
        if (c.op != OpKind::BatchAck) return;
        ++acks;
        insideOutstanding = cq.outstanding();
        insideReserved    = cq.reservedBytes();
        EXPECT_EQ(cq.reserve(48), OpResult::QueueFull) << "no credit of the read is back while it runs";
    });
    EXPECT_EQ(n, static_cast<int>(singles) + 1);
    EXPECT_EQ(acks, 1);
    EXPECT_EQ(insideOutstanding, 18u);
    EXPECT_EQ(insideReserved, 18u * 48u);
    EXPECT_EQ(cq.outstanding(), 0u) << "the BatchAck returned its " << kUnits << " units, the singles one each";
    EXPECT_EQ(cq.reservedBytes(), 0u);
    uint32_t again = 0;
    while (cq.reserve(48) == OpResult::Accepted) ++again;
    EXPECT_EQ(again, 18u) << "the whole credit is back, no more and no less";
}

// Stash plus ring in one poll(): the stash entries' credit returns at once (their bytes left the ring when waitFor
// stashed them), the ring records' only after the read.  A handler that reissues on every completion never loses one,
// and the totals end at zero.  Seeded mix of waitFor (stashing) and poll.
TEST(CompletionsAdversarialRegression, StashAndRingMixWithReissuingHandlerAdversarialRegression) {
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        Completions cq(smallCfg(1024, 88, 64));
        uint32_t rng = seed * 2654435761u;
        auto rnd = [&]() { rng = rng * 1664525u + 1013904223u; return rng >> 8; };
        constexpr Token kTotal = 400;
        Token next = 0;
        std::vector<int> times(kTotal, 0);
        std::deque<Token> inRing;
        auto issue = [&]() {
            if (next >= kTotal) return;
            if (cq.reserve(48) != OpResult::Accepted) return;
            ASSERT_TRUE(deliver(cq, next, 48, 0)) << "token " << next << " refused by the ring";
            inRing.push_back(next++);
        };
        auto handler = [&](const Completion& c, std::span<const uint8_t>) {
            ++times[c.token];
            if (rnd() % 2) issue();   // reissue from inside the handler (ring or stash delivery)
        };
        for (int step = 0; step < 100000 && (next < kTotal || cq.outstanding() != 0); ++step) {
            const uint32_t k = rnd() % 4;
            if (k == 0) {
                for (int i = 0; i < 5; ++i) issue();
            } else if (k == 1 && !inRing.empty()) {
                // waitFor a token deep in the ring: everything before it is stashed.
                const std::size_t at = rnd() % inRing.size();
                const Token want = inRing[at];
                inRing.erase(inRing.begin() + static_cast<std::ptrdiff_t>(at));
                if (times[want] == 0) {
                    const auto w = cq.waitFor(want, 0ns, handler);
                    ASSERT_EQ(w, Completions::Wait::Done) << "token " << want;
                }
            } else {
                (void)cq.poll(handler, static_cast<int>(1 + rnd() % 8));
                inRing.clear();   // tokens may now be in the stash or delivered; waitFor only targets fresh ones
            }
        }
        while (cq.poll(handler) > 0) {}
        EXPECT_EQ(cq.ring().lostCompletions(), 0u);
        EXPECT_EQ(next, kTotal);
        for (Token t = 0; t < next; ++t) ASSERT_EQ(times[t], 1) << "token " << t;
        EXPECT_EQ(cq.outstanding(), 0u);
        EXPECT_EQ(cq.reservedBytes(), 0u);
        EXPECT_EQ(cq.stashed(), 0u);
    }
}

// A handler that throws on the LAST record of a read, and one that throws while the read spans a wrap (two Aeron reads in
// one CompletionRing::read): the credit returned equals exactly the records handed over, and every later token arrives.
TEST(CompletionsAdversarialRegression, ThrowAcrossAWrapReturnsExactlyTheHandedCreditAdversarialRegression) {
    for (int throwAt = 1; throwAt <= 18; ++throwAt) {
        SCOPED_TRACE("throw at " + std::to_string(throwAt));
        Completions cq(smallCfg(1024, 88, 64));
        Token next = 0;
        // Advance the ring so the next batch wraps: 11 records in, 11 out.
        for (int i = 0; i < 11; ++i) { ASSERT_EQ(cq.reserve(48), OpResult::Accepted); ASSERT_TRUE(deliver(cq, next++, 48, 0)); }
        while (cq.poll([](const Completion&, std::span<const uint8_t>) {}) > 0) {}
        ASSERT_EQ(cq.outstanding(), 0u);
        const Token first = next;
        while (cq.reserve(48) == OpResult::Accepted) ASSERT_TRUE(deliver(cq, next++, 48, 0));
        ASSERT_EQ(next - first, 18u);
        int seen = 0;
        std::vector<int> times(64, 0);
        EXPECT_THROW(cq.poll([&](const Completion& c, std::span<const uint8_t>) {
            ++times[c.token];
            if (++seen == throwAt) throw std::runtime_error("handler");
        }), std::runtime_error);
        EXPECT_EQ(seen, throwAt);
        EXPECT_EQ(cq.outstanding(), 18u - static_cast<uint32_t>(throwAt));
        EXPECT_EQ(cq.reservedBytes(), (18u - static_cast<uint32_t>(throwAt)) * 48u);
        while (cq.poll([&](const Completion& c, std::span<const uint8_t>) { ++times[c.token]; }) > 0) {}
        for (Token t = first; t < next; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
        EXPECT_EQ(cq.outstanding(), 0u);
        EXPECT_EQ(cq.reservedBytes(), 0u);
    }
}

// A handler that sees reserve() answer QueueFull used to call poll() (or waitFor()) from inside poll(): the nested ring read
// starts from the same, not yet moved, head.  Contract: nesting is a programming error, refused loudly with std::logic_error
// (never a silent 0 an app could spin on); the handler returns on QueueFull and the app polls again after the outer call.
// Every completion is delivered exactly once, the credit ends at zero and the ring still works.
// URGENT break found by the batch 1 fix-round re-attack (hang): OneToOneRingBuffer::read is not re-entrant - the nested read
// re-delivers from the unmoved head, then the outer read's exit moves the head back over memory the nested one zeroed, and
// the ring is wedged (later completions never arrive, their ops stay outstanding).  Fixed by the owner-thread re-entrancy
// guard in Completions::poll / waitFor.
TEST(CompletionsAdversarialRegression, NestedPollInsideAPollHandlerDeliversEachCompletionOnceAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    Token next = 0;
    std::vector<int> times(64, 0);
    while (cq.reserve(48) == OpResult::Accepted) ASSERT_TRUE(deliver(cq, next++, 48, 0));
    ASSERT_EQ(next, 18u);
    int refused = 0;
    bool attempting = false;   // one nested attempt at a time: without the guard the test fails instead of recursing forever
    std::function<void(const Completion&, std::span<const uint8_t>)> h = [&](const Completion& c, std::span<const uint8_t>) {
        ++times[c.token];
        if (!attempting && cq.reserve(48) == OpResult::QueueFull) {   // the nested call is refused; the handler just returns
            attempting = true;
            EXPECT_THROW((void)cq.poll(h, 2), std::logic_error);
            attempting = false;
            ++refused;
        }
    };
    for (int i = 0; i < 100 && cq.poll(h) > 0; ++i) {}
    EXPECT_GT(refused, 0) << "no handler reached the nested call: the test is vacuous";
    for (Token t = 0; t < next; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_TRUE(cq.ring().empty());
    // The ring still works afterwards: a fresh op round-trips.
    ASSERT_EQ(cq.reserve(48), OpResult::Accepted);
    ASSERT_TRUE(deliver(cq, 40, 48, 0));
    int got = 0;
    for (int i = 0; i < 4 && got == 0; ++i) got += cq.poll([&](const Completion& c, std::span<const uint8_t>) { EXPECT_EQ(c.token, 40u); });
    EXPECT_EQ(got, 1) << "the ring is wedged after a nested read";
}

// waitFor() inside a poll() handler: issue the next op and wait for it synchronously (a natural request chain).
// URGENT (same root cause as the nested poll above): the nested waitFor() throws std::logic_error; the request chain is
// continued by the app after the outer poll (the issued op is delivered by a later poll).
TEST(CompletionsAdversarialRegression, WaitForInsideAPollHandlerDeliversEachCompletionOnceAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    std::vector<int> times(64, 0);
    for (Token t = 0; t < 4; ++t) { ASSERT_EQ(cq.reserve(48), OpResult::Accepted); ASSERT_TRUE(deliver(cq, t, 48, 0)); }
    Token next = 4;
    int refused = 0;
    std::function<void(const Completion&, std::span<const uint8_t>)> h = [&](const Completion& c, std::span<const uint8_t>) {
        ++times[c.token];
        if (c.token < 4 && next < 8 && cq.reserve(48) == OpResult::Accepted) {
            const Token t = next++;
            ASSERT_TRUE(deliver(cq, t, 48, 0));
            EXPECT_THROW((void)cq.waitFor(t, 0ns, [&](const Completion& d, std::span<const uint8_t>) { ++times[d.token]; }),
                         std::logic_error);
            ++refused;
        }
    };
    for (int i = 0; i < 100 && cq.poll(h) > 0; ++i) {}
    EXPECT_GT(refused, 0) << "no handler reached the nested call: the test is vacuous";
    for (Token t = 0; t < next; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.stashed(), 0u);
}

// Hot path: poll() and waitFor() with the deferred credit allocate nothing per record or per call (the guard is two
// counters on the stack), including a handler that reissues, a BatchAck, and waitFor stashing other tokens.
TEST(CompletionsAdversarialRegression, DeferredCreditPollAndWaitForAllocateNothingAdversarialRegression) {
    Completions cq(smallCfg(4096, 88, 64));
    Token next = 0;
    auto h = [&](const Completion& c, std::span<const uint8_t>) {
        if (c.op != OpKind::BatchAck && cq.reserve(48) == OpResult::Accepted) (void)deliver(cq, next++, 48, 0);
    };
    auto cycle = [&]() {
        for (int i = 0; i < 8; ++i) if (cq.reserve(48) == OpResult::Accepted) (void)deliver(cq, next++, 48, 0);
        for (int i = 0; i < 3; ++i) ASSERT_EQ(cq.reserve(48), OpResult::Accepted);
        ASSERT_TRUE(deliverKind(cq, 0, OpKind::BatchAck, 3 * rune::client::kBatchAckUnitBytes));
        const Token want = next;
        ASSERT_EQ(cq.reserve(48), OpResult::Accepted);
        ASSERT_TRUE(deliver(cq, next++, 48, 0));
        ASSERT_EQ(cq.waitFor(want, 0ns, h), Completions::Wait::Done);   // stashes the earlier records
        for (int i = 0; i < 4 && cq.poll(h) > 0; ++i) {}
    };
    for (int w = 0; w < 50; ++w) cycle();   // warm-up
    alloc_counter::startThisThread();
    for (int r = 0; r < 2000; ++r) cycle();
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "poll/waitFor allocated on the hot path";
    while (cq.poll([](const Completion&, std::span<const uint8_t>) {}) > 0) {}
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
}

// ---------------------------------------------------------------------------
// PR #851 review re-attack (7bd588a9): the re-entrancy guard covers every handler entry point, not only poll()'s ring
// read.  A waitFor() onDone run from the ring and one run from the stash, and a poll() handler run from the stash, each
// refuse a nested poll()/waitFor() with std::logic_error; every completion is still delivered exactly once.
// ---------------------------------------------------------------------------
TEST(CompletionsAdversarialRegression, NestedCallsInsideWaitForAndStashHandlersAreRefusedAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    std::vector<int> times(16, 0);
    for (Token t = 0; t < 6; ++t) { ASSERT_EQ(cq.reserve(48), OpResult::Accepted); ASSERT_TRUE(deliver(cq, t, 48, 0)); }
    auto count = [&](const Completion& d, std::span<const uint8_t>) { ++times[d.token]; };
    int refused = 0;
    auto nestBoth = [&]() {
        EXPECT_THROW((void)cq.poll(count), std::logic_error);
        EXPECT_THROW((void)cq.waitFor(0, 0ns, count), std::logic_error);
        ++refused;
    };
    // 1. waitFor's onDone delivered from the ring (tokens 0..4 are stashed on the way to 5).
    ASSERT_EQ(cq.waitFor(5, 0ns, [&](const Completion& c, std::span<const uint8_t> p) { count(c, p); nestBoth(); }),
              Completions::Wait::Done);
    EXPECT_EQ(cq.stashed(), 5u);
    // 2. waitFor's onDone delivered from the stash.
    ASSERT_EQ(cq.waitFor(4, 0ns, [&](const Completion& c, std::span<const uint8_t> p) { count(c, p); nestBoth(); }),
              Completions::Wait::Done);
    // 3. poll()'s handler delivered from the stash (0..3).
    int polled = cq.poll([&](const Completion& c, std::span<const uint8_t> p) { count(c, p); nestBoth(); });
    EXPECT_EQ(polled, 4);
    EXPECT_EQ(refused, 6);
    for (Token t = 0; t < 6; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.stashed(), 0u);
    EXPECT_TRUE(cq.ring().empty());
}

// A nested call the handler does not catch unwinds out of the outer poll()/waitFor().  The guard must be released on that
// unwind (else every later poll() throws and the queue is dead), the record whose handler threw counts as delivered, and
// the rest are delivered exactly once by the next poll.
TEST(CompletionsAdversarialRegression, AnEscapingNestedCallLeavesTheQueueUsableAdversarialRegression) {
    Completions cq(smallCfg(1024, 88, 64));
    std::vector<int> times(16, 0);
    for (Token t = 0; t < 6; ++t) { ASSERT_EQ(cq.reserve(48), OpResult::Accepted); ASSERT_TRUE(deliver(cq, t, 48, 0)); }
    auto count = [&](const Completion& d, std::span<const uint8_t>) { ++times[d.token]; };
    // Ring path of poll(): the handler of token 0 nests and lets the logic_error escape.
    EXPECT_THROW((void)cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
                     count(c, p);
                     if (c.token == 0) (void)cq.poll(count);
                 }),
                 std::logic_error);
    // waitFor() ring path: onDone of token 3 nests and escapes; 1 and 2 are stashed on the way.
    EXPECT_THROW((void)cq.waitFor(3, 0ns, [&](const Completion& c, std::span<const uint8_t> p) {
                     count(c, p);
                     (void)cq.waitFor(4, 0ns, count);
                 }),
                 std::logic_error);
    // Stash path of poll(): the handler of token 1 nests and escapes.
    EXPECT_THROW((void)cq.poll([&](const Completion& c, std::span<const uint8_t> p) {
                     count(c, p);
                     if (c.token == 1) (void)cq.poll(count);
                 }),
                 std::logic_error);
    // The queue is not left "inside a read": a plain poll drains the rest.
    for (int i = 0; i < 8 && cq.poll(count) > 0; ++i) {}
    for (Token t = 0; t < 6; ++t) EXPECT_EQ(times[t], 1) << "token " << t;
    EXPECT_EQ(cq.outstanding(), 0u);
    EXPECT_EQ(cq.reservedBytes(), 0u);
    EXPECT_EQ(cq.stashed(), 0u);
    ASSERT_EQ(cq.reserve(48), OpResult::Accepted);
    ASSERT_TRUE(deliver(cq, 9, 48, 0));
    EXPECT_EQ(cq.waitFor(9, 0ns, count), Completions::Wait::Done) << "the guard stuck after an unwind";
    EXPECT_EQ(times[9], 1);
}

// The guard is per queue: a handler of one queue may poll() and waitFor() ANOTHER queue (an app routing completions
// between its own queues); that is never refused.
TEST(CompletionsAdversarialRegression, AHandlerMayPollAnotherQueueAdversarialRegression) {
    Completions a(smallCfg(1024, 88, 64));
    Completions b(smallCfg(1024, 88, 64));
    for (Token t = 0; t < 3; ++t) {
        ASSERT_EQ(a.reserve(48), OpResult::Accepted); ASSERT_TRUE(deliver(a, t, 48, 0));
        ASSERT_EQ(b.reserve(48), OpResult::Accepted); ASSERT_TRUE(deliver(b, t, 48, 0));
    }
    int fromB = 0;
    auto countB = [&](const Completion&, std::span<const uint8_t>) { ++fromB; };
    int fromA = 0;
    EXPECT_NO_THROW((void)a.poll([&](const Completion& c, std::span<const uint8_t>) {
        ++fromA;
        if (c.token == 0) { EXPECT_EQ(b.waitFor(2, 0ns, countB), Completions::Wait::Done); }
        else if (c.token == 1) { EXPECT_EQ(b.poll(countB), 2); }
    }));
    EXPECT_EQ(fromA, 3);
    EXPECT_EQ(fromB, 3);
    EXPECT_EQ(a.outstanding(), 0u);
    EXPECT_EQ(b.outstanding(), 0u);
}
