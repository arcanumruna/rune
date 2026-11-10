// include/rune/client/lease/LeaseElection.h - leader election over lease records (etcd concurrency.Election; spec 12.3).
//
// The same pattern as LeaseOwnership with a value: campaign(row) grants a new key whose row is `row` (the key column is patched in), and
// completes (OpKind::LeaseCampaign) when this candidate leads; proclaim(row) is a fenced update of the leader's row; resign() is the fenced
// revoke; leader() and observe() read the name's lowest live (version, key) from the shared LeaseWatch.  While the held lease is at risk the
// candidate is suspended (isLeader() == false, the atRisk callback ran); Cleared re-evaluates it.
#pragma once

#include <rune/client/lease/LeaseRecipe.h>

namespace rune::client {

class LeaseElection {
public:
    /// `maxRowBytes`: the largest row campaign() / proclaim() accept (the recipe keeps a copy for its re-grants).
    template <class HostT>
    LeaseElection(HostT& host, std::string_view name, uint32_t maxRowBytes, const LeaseRecipeCallbacks& cb = {})
        : core_(detail::recipeCoreOf(host), name, cb, maxRowBytes) {}

    [[nodiscard]] bool valid() const noexcept { return core_.valid(); }
    /// `row`: a whole row image of the topic's type (its key column is overwritten with this candidate's key).
    [[nodiscard]] OpResult campaign(std::span<const uint8_t> row, CompletionTarget target, Token token) {
        return core_.claim(detail::LeaseRecipeCore::Mode::Claim, row, target, token);
    }
    /// The typed form: `row` is a generated row of the topic (encodedSize() / encode()).
    template <class T>
    [[nodiscard]] OpResult campaign(const T& row, CompletionTarget target, Token token) {
        if (!core_.acceptsClaim()) return OpResult::InvalidArgument;   // before encoding: a refused call must not touch the stored row
        const std::size_t n = encode_(row);
        return n == 0 ? OpResult::InvalidArgument : core_.claimStored(detail::LeaseRecipeCore::Mode::Claim, n, target, token);
    }
    [[nodiscard]] OpResult proclaim(std::span<const uint8_t> row, CompletionTarget target, Token token) { return core_.proclaim(row, target, token); }
    template <class T>
    [[nodiscard]] OpResult proclaim(const T& row, CompletionTarget target, Token token) {
        if (!core_.acceptsProclaim()) return OpResult::InvalidArgument;
        const std::size_t n = encode_(row);
        return n == 0 ? OpResult::InvalidArgument : core_.proclaimStored(n, target, token);
    }
    [[nodiscard]] OpResult resign() { return core_.release(); }
    [[nodiscard]] bool    isLeader() const noexcept { return core_.isOwner(); }
    [[nodiscard]] int64_t token() const noexcept { return core_.token(); }
    /// The current leader as seen by the shared watch: (lease_version, key).
    [[nodiscard]] auto leader() const noexcept { return core_.leader(); }
    /// One observer.  fn(ctx, version, key, bytes) runs once per change of the name's lowest (key, version), first with the current leader when
    /// the watch is ready; `bytes` is the leader's row when the change came with it (a log row), else empty.
    using ObserveFn = detail::LeaseRecipeCore::ObserveFn;
    [[nodiscard]] bool observe(ObserveFn fn, void* ctx) { return core_.setObserver(fn, ctx); }
    [[nodiscard]] uint32_t campaignNo() const noexcept { return core_.campaignNo(); }
    [[nodiscard]] std::span<const uint8_t> key() const noexcept { return core_.key(); }
    [[nodiscard]] uint64_t evaluations() const noexcept { return core_.evaluations(); }

private:
    template <class T>
    [[nodiscard]] std::size_t encode_(const T& row) {
        auto buf = core_.rowBuffer();
        if (row.encodedSize() > buf.size()) return 0;
        return row.encode(buf.data());
    }

    detail::LeaseRecipeCore core_;
};

}  // namespace rune::client
