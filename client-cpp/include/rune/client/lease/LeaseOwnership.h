// include/rune/client/lease/LeaseOwnership.h - cluster-wide ownership of a name by lease records (etcd concurrency.Mutex, renamed; spec 12.3).
//
// Never a thread lock: no lock is taken in the client or the broker.  claim() grants a new key under `name/` and completes (OpKind::LeaseCampaign)
// when this recipe's lease_version is the lowest live version under the name; tryClaim() completes at once with ClientErrorCode::LeaseClaimed
// (and gives its key back) when the name is owned by someone else.  token() is the fencing token (the lease_version).  See LeaseRecipe.h for the
// wiring, the key shape, the completions and the unknown-outcome rule.
#pragma once

#include <rune/client/lease/LeaseRecipe.h>

namespace rune::client {

class LeaseOwnership {
public:
    /// `host`: a LeaseRecipeHost<..> (or its core).  `name`: 1..(N - 26) bytes.  It may contain '/' ("jobs/42"): a key belongs to a name when the rest after name + '/' has no further '/'.  Check valid().
    template <class HostT>
    LeaseOwnership(HostT& host, std::string_view name, const LeaseRecipeCallbacks& cb = {})
        : core_(detail::recipeCoreOf(host), name, cb, 0) {}

    [[nodiscard]] bool valid() const noexcept { return core_.valid(); }
    /// Grants a new key; `target` completes with Ok (value = token) when the name is owned.  See LeaseRecipeCore::claim for the other results.
    [[nodiscard]] OpResult claim(CompletionTarget target, Token token) {
        return core_.claim(detail::LeaseRecipeCore::Mode::Claim, {}, target, token);
    }
    /// Like claim(), but completes LeaseClaimed (nothing held) when the name is already owned.
    [[nodiscard]] OpResult tryClaim(CompletionTarget target, Token token) {
        return core_.claim(detail::LeaseRecipeCore::Mode::TryClaim, {}, target, token);
    }
    /// A fenced revoke of the held key (or the abandonment of a campaign in flight).
    [[nodiscard]] OpResult release() { return core_.release(); }
    [[nodiscard]] bool    isOwner() const noexcept { return core_.isOwner(); }
    [[nodiscard]] int64_t token() const noexcept { return core_.token(); }
    /// The current owner of the name as seen by the shared watch: (lease_version, key).
    [[nodiscard]] auto owner() const noexcept { return core_.leader(); }
    [[nodiscard]] uint32_t campaignNo() const noexcept { return core_.campaignNo(); }
    [[nodiscard]] std::span<const uint8_t> key() const noexcept { return core_.key(); }
    [[nodiscard]] uint64_t evaluations() const noexcept { return core_.evaluations(); }

private:
    detail::LeaseRecipeCore core_;
};

}  // namespace rune::client
