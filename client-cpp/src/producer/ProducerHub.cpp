// src/producer/ProducerHub.cpp
//
// CB-2 — ProducerHub non-inline method implementations.

#include <rune/client/producer/ProducerHub.h>

namespace rune::client::producer {

int ProducerHub::drainAndFlush(int64_t nowNs,
                                int64_t sessionId,
                                std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    int work = 0;
    for (auto& impl : impls_) {
        // Drain + flush in a tight loop: drain one message at a time so that
        // flush boundaries (kMsgFlush or batchBytes triggers) are honoured
        // between individual records.  This ensures that a flush() call between
        // two sends produces two separate batches (the expected user-facing
        // semantic).  We cap the loop at 256 iterations to bound CPU.
        static constexpr int kMaxInnerIters = 256;
        for (int iter = 0; iter < kMaxInnerIters; ++iter) {
            const int drained = impl->drainRing(nowNs);
            const int flushed = impl->flushBatches(nowNs, sessionId, sendFn);
            work += drained + flushed;
            if (drained == 0) break; // no more messages in the ring
        }
    }
    return work;
}

void ProducerHub::onProduceResponse(int64_t correlationId, int64_t baseOffset) {
    for (auto& impl : impls_) {
        impl->onProduceResponse(correlationId, baseOffset);
    }
}

void ProducerHub::failAll(const std::string& reason) {
    for (auto& impl : impls_) {
        impl->failAllPending(reason);
    }
}

void ProducerHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    for (auto& impl : impls_) {
        impl->sweepTimedOut(now);
    }
}

} // namespace rune::client::producer
