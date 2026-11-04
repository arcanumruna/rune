// Shared builders for the aborted-transaction fold tests (#537, #796): the REAL AbortedTxnIndex the worker hands
// the fold, and byte concatenation of whole batches.
#pragma once
#include <cstdint>
#include <initializer_list>
#include <vector>

#include <rune/transaction/AbortedTxnIndex.h>
#include <rune/view/FoldSkip.h>

namespace abortedfix {

// One aborted range [first, last] of one producer.
struct OneAbortedRange {
    rune::transaction::AbortedTxnIndex idx;
    OneAbortedRange(int64_t pid, int64_t first, int64_t last) { idx.onAbortMarker(pid, first, last); }
    [[nodiscard]] rune::view::AbortedTxnFilter filter() const { return rune::view::AbortedTxnFilter{&idx}; }
};

inline std::vector<uint8_t> concatBytes(std::initializer_list<std::vector<uint8_t>> parts) {
    std::vector<uint8_t> out;
    for (const auto& p : parts) out.insert(out.end(), p.begin(), p.end());
    return out;
}

} // namespace abortedfix
