// tests/support/FrameCapture.h — Phase 2b Part B test helper: a SendFn sink that
// decodes every Produce frame a bare ProducerImpl offers and records each record
// (partition, frame ordinal, offset within its batch, key, value) in order.
// `watch` (optional): a claimed buffer address; watchInFrame turns true when an
// offered frame contains it (the Invoker "the claim IS the frame" proof).
#pragma once
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace test_support {

struct CapturedRecord {
    int32_t     partition = 0;
    int         frame     = 0;
    int64_t     offset    = 0;  // offsetDelta within its batch (the client's baseOffset is 0)
    int64_t     corr      = 0;  // the frame's correlation id
    std::string key;
    std::string value;
};

struct FrameCapture {
    std::vector<CapturedRecord> recs;
    std::vector<int64_t>        corrs;  // one per frame
    int                         frames       = 0;
    const uint8_t*              watch        = nullptr;
    bool                        watchInFrame = false;

    rune::client::producer::ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t partition, int32_t, const uint8_t* b, std::size_t n) {
            if (watch != nullptr && watch >= b && watch < b + n) watchInFrame = true;
            const auto v = rune::client::codec::decodeProduce(b, n);
            if (!v) return true;
            const int f = frames++;
            corrs.push_back(v->correlationId);
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            r.forEachRecord([&](std::string_view k, std::string_view val, int64_t off, int64_t) {
                recs.push_back(CapturedRecord{partition, f, off, v->correlationId, std::string(k), std::string(val)});
            });
            return true;
        };
    }
    [[nodiscard]] std::vector<std::string> values() const {
        std::vector<std::string> out;
        for (const auto& r : recs) out.push_back(r.value);
        return out;
    }
};

}  // namespace test_support
