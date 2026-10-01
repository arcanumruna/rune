// tests/support/AckLedger.h — F8c C6: exactly-once ledger for producer ack
// tokens on a bare ProducerImpl (no Client).  The producer's onAck handler
// (a plain function pointer) records every completion per token; tests assert
// each accepted token completed exactly once, with which status / value.
//
// Threading: the handler runs on the duty thread (the test thread in the
// single-threaded rigs; the test's duty thread in the TSan rigs — read the
// ledger only after joining it).
#pragma once

#include <rune/client/Completion.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <cstdint>
#include <functional>
#include <map>
#include <utility>
#include <vector>

namespace test_support {

struct AckLedger {
    struct Rec {
        rune::client::Token    token  = 0;
        int64_t                value  = 0;
        rune::client::OpStatus status = rune::client::OpStatus::Ok;
        uint16_t               ec     = 0;
    };
    std::vector<Rec>                       recs;     // completion order
    std::map<rune::client::Token, int>     count;
    std::map<rune::client::Token, Rec>     last;
    std::vector<rune::client::BatchAck>    batchAcks;

    static void onAck(void* ctx, rune::client::Token t, int64_t v, rune::client::OpStatus st,
                      uint16_t ec) noexcept {
        auto* self = static_cast<AckLedger*>(ctx);
        const Rec r{t, v, st, ec};
        self->recs.push_back(r);
        ++self->count[t];
        self->last[t] = r;
    }
    static void onBatch(void* ctx, const rune::client::BatchAck& a) noexcept {
        static_cast<AckLedger*>(ctx)->batchAcks.push_back(a);
    }

    void attach(rune::client::producer::ProducerImpl& impl) { impl.setOnAck(&onAck, this); }
    void attach(rune::client::Producer& p) { p.onAck(&onAck, this); }
    void attachBatches(rune::client::producer::ProducerImpl& impl) { impl.setOnBatchAck(&onBatch, this); }

    /// The tagged handle of an AckToken send of `t` (the onAck target).
    static uint64_t h(rune::client::Token t) noexcept {
        return rune::client::producer::ackhandle::forHandler(t);
    }
    static int64_t hi(rune::client::Token t) noexcept { return static_cast<int64_t>(h(t)); }

    [[nodiscard]] int times(rune::client::Token t) const {
        const auto it = count.find(t);
        return it == count.end() ? 0 : it->second;
    }
    [[nodiscard]] int64_t value(rune::client::Token t) const { return last.at(t).value; }
    [[nodiscard]] rune::client::OpStatus status(rune::client::Token t) const { return last.at(t).status; }
    /// Every token in [from, to) completed exactly once.
    [[nodiscard]] bool exactlyOnce(rune::client::Token from, rune::client::Token to) const {
        for (rune::client::Token t = from; t < to; ++t) {
            if (times(t) != 1) return false;
        }
        return true;
    }
    [[nodiscard]] bool noneDuplicated() const {
        for (const auto& [t, n] : count) {
            if (n != 1) return false;
        }
        return true;
    }
    /// Records reported by BatchAcks (every accepted record exactly once).
    [[nodiscard]] int64_t batchAckedRecords() const {
        int64_t n = 0;
        for (const auto& a : batchAcks) n += a.recordCount;
        return n;
    }
};

/// Per-token scripted reactions (test-only convenience; std::function is fine
/// here): cb(f) registers `f` under a fresh token and returns the tagged
/// handle to stamp on a send; the producer's onAck handler runs `f(offset)`
/// exactly once — offset -1 for any non-Ok status (the old callback contract).
/// rejected(h) emulates the old app-side "fail the unsubmitted handle" for a
/// send that was NOT accepted (no completion ever arrives for it — F8c C6).
struct ScriptedAcks {
    std::map<rune::client::Token, std::function<void(int64_t)>> fns;
    std::map<rune::client::Token, int>                          fired;
    rune::client::Token                                         next = 1;

    static void onAck(void* ctx, rune::client::Token t, int64_t v, rune::client::OpStatus st,
                      uint16_t) noexcept {
        auto* self = static_cast<ScriptedAcks*>(ctx);
        ++self->fired[t];
        const auto it = self->fns.find(t);
        if (it == self->fns.end()) return;
        auto f = std::move(it->second);
        self->fns.erase(it);
        if (f) f(st == rune::client::OpStatus::Ok ? v : -1);
    }
    void attach(rune::client::producer::ProducerImpl& impl) { impl.setOnAck(&onAck, this); }
    void attach(rune::client::Producer& p) { p.onAck(&onAck, this); }
    /// cb() for the Producer API: the AckToken to send with.
    rune::client::AckToken tok(std::function<void(int64_t)> f) {
        const rune::client::Token t = next++;
        fns[t] = std::move(f);
        return rune::client::AckToken{t};
    }
    int64_t cb(std::function<void(int64_t)> f) {
        const rune::client::Token t = next++;
        fns[t] = std::move(f);
        return AckLedger::hi(t);
    }
    void rejected(int64_t h) {
        const rune::client::Token t = rune::client::producer::ackhandle::token(static_cast<uint64_t>(h));
        const auto it = fns.find(t);
        if (it == fns.end()) return;
        auto f = std::move(it->second);
        fns.erase(it);
        if (f) f(-1);
    }
    /// Every token completed at most once.
    [[nodiscard]] bool noneDuplicated() const {
        for (const auto& [t, n] : fired) {
            if (n != 1) return false;
        }
        return true;
    }
};

}  // namespace test_support
