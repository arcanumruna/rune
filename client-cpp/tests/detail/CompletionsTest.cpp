// tests/detail/CompletionsTest.cpp — F8c C1(b): detail::CompletionRing +
// Completions, standalone (no Client): poll, credit, stash, waitFor with an
// injected pump.  No sleeps: every wait is on another thread's progress or on
// an injected clock.

#include <rune/client/Completions.h>
#include <rune/client/detail/OneToOneRingBuffer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <span>
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
        cq.ring().producerClose();
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
        cq.ring().producerClose();
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
    EXPECT_EQ(cq.reserve(cq.maxRecordBytes() + 8), OpResult::InvalidArgument);
    EXPECT_EQ(cq.reserve(err + 3), OpResult::InvalidArgument) << "an unaligned reservation is refused";
}
