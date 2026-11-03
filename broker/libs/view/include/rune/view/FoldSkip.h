// libs/view/include/rune/view/FoldSkip.h
//
// What a fold must not apply.  Header-only: rune_view links only rune_codec, plus the header-only
// rune_transaction/AbortedTxnIndex.h.
#pragma once
#include <cstdint>

#include <rune/protocol/RecordBatch.h>
#include <rune/storage/RejectedOffsetIndex.h>
#include <rune/transaction/AbortedTxnIndex.h>

namespace rune::view {

// Issue #537: the source partition's aborted-transaction predicate, handed to
// every fold.  A view folds only COMMITTED data: the caller's `bound` (the
// partition's LSO) keeps every open transaction out, and this predicate keeps
// out every batch of an ABORTED one — the same rule the read_committed fetch
// and the log cleaner apply (AbortedTxnIndex::isAborted(producerId, baseOffset)
// over the owning worker's RcState).  #591: it holds the CONCRETE index and
// calls its inline isAborted — no function pointer, no indirect call (rule 5).
// Called once per TRANSACTIONAL batch only — a non-transactional batch costs
// one attribute-bit test.  `index == nullptr` means the source holds no aborted
// transaction (kNoAbortedTxns: plain-data unit tests and benches; the worker
// always passes its partition's index).
struct AbortedTxnFilter {
    const rune::transaction::AbortedTxnIndex* index = nullptr;   // nullptr: the source holds no aborted transaction

    // True when `r` (attributes `attrs`, already read by the caller) is a data
    // batch of an aborted transaction.  Callers copy the filter by value into
    // the walk (one word): no reload through a reference the table writes may alias.
    [[nodiscard]] bool skips(const rune::protocol::RecordBatchReader& r, int16_t attrs) const noexcept {
        if ((attrs & rune::protocol::batch::kAttrTransactional) == 0 || index == nullptr) [[likely]] return false;
        const int64_t pid = r.producerId();
        return pid >= 0 && index->isAborted(pid, r.baseOffset());
    }
};
inline constexpr AbortedTxnFilter kNoAbortedTxns{};

// Everything the fold skips: `aborted` drops whole batches of aborted transactions.
struct FoldSkip {
    AbortedTxnFilter                          aborted{};
    const rune::storage::RejectedOffsetIndex* rejected = nullptr;   // offsets the judge rejected: never applied
};
inline constexpr FoldSkip kNoSkip{};

[[nodiscard]] inline bool isAbortedBatch(const FoldSkip& skip, const rune::protocol::RecordBatchReader& r, int16_t attrs) noexcept {
    return skip.aborted.skips(r, attrs);
}

// One branch on `size() != 0` keeps a partition without rejections at the cost of a pointer test.
[[nodiscard]] inline bool isRejectedRecord(const FoldSkip& skip, int64_t offset) noexcept {
    return skip.rejected != nullptr && skip.rejected->size() != 0 && skip.rejected->isRejected(offset);
}

} // namespace rune::view
