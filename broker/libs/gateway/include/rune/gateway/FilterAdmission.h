#pragma once
// FilterAdmission.h - Phase 2c (spec 13.2): the gateway's control-thread bookkeeping for filtered subscriptions.
//   FilterSchemaRegistry : the typed topics' FilterSchemas the gateway validates a filter against.
//   FilterAdmission      : per (topic, partition) caps on filtered subscriptions, distinct index values and distinct
//                          index fields, so the gateway never answers OK to a Subscribe its worker would refuse.
// Both are owned by the ClientGateway and touched only on the control thread (shared-nothing; no locks).
#include <rune/codec/FilterSchema.h>
#include <rune/codec/FilterWire.h>

#include <cstdint>
#include <expected>
#include <map>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::gateway {

class FilterSchemaRegistry {
public:
    /// Cold.  Installs or extends the topic's schema from its EVOLVING descriptor blob at `version`.  A bad blob
    /// or a conflicting extension is logged and the old schema is kept (a conflicting one is replaced by the
    /// worker, so the gateway follows it: the newer build wins).
    void install(int32_t topicId, uint16_t version, std::span<const uint8_t> evolvingBlob);
    void erase(int32_t topicId) noexcept { byTopic_.erase(topicId); }
    [[nodiscard]] const rune::codec::FilterSchema* find(int32_t topicId) const noexcept {
        const auto it = byTopic_.find(topicId);
        return it == byTopic_.end() ? nullptr : &it->second;
    }

private:
    std::unordered_map<int32_t, rune::codec::FilterSchema> byTopic_;
};

class FilterAdmission {
public:
    struct Limits {
        uint32_t maxSubs   = 4096;
        uint32_t maxValues = 4096;
    };

    /// Move-only; its destructor releases the registration.
    class Ticket {
    public:
        Ticket() = default;
        Ticket(Ticket&& o) noexcept
            : owner_(std::exchange(o.owner_, nullptr)), tp_(o.tp_), tag_(o.tag_), keys_(std::move(o.keys_)) {}
        Ticket& operator=(Ticket&& o) noexcept {
            if (this != &o) {
                reset_();
                owner_ = std::exchange(o.owner_, nullptr);
                tp_    = o.tp_;
                tag_   = o.tag_;
                keys_  = std::move(o.keys_);
            }
            return *this;
        }
        ~Ticket() { reset_(); }
        Ticket(const Ticket&)            = delete;
        Ticket& operator=(const Ticket&) = delete;
        [[nodiscard]] bool held() const noexcept { return owner_ != nullptr; }

    private:
        friend class FilterAdmission;
        void reset_() noexcept;
        FilterAdmission*                owner_ = nullptr;
        uint64_t                        tp_    = 0;
        uint16_t                        tag_   = 0;
        std::vector<rune::codec::FilterKey> keys_;
    };

    void setLimits(Limits l) noexcept { limits_ = l; }
    [[nodiscard]] Limits limits() const noexcept { return limits_; }

    /// Admit one filtered subscription: its index term's tag and values (CompiledFilter::indexTerm()).  The refusal
    /// text is the Subscribe detail ("filter capacity: ...").  Cold; allocates only the ticket's key copy.
    [[nodiscard]] std::expected<Ticket, std::string> admit(int32_t topicId, int32_t partition, uint16_t indexTag,
                                                           std::span<const rune::codec::FilterKey> keys);

    [[nodiscard]] uint32_t subsFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] uint32_t valuesFor(int32_t topicId, int32_t partition) const noexcept;
    [[nodiscard]] uint32_t indexFieldsFor(int32_t topicId, int32_t partition) const noexcept;

private:
    void release_(Ticket& t) noexcept;
    struct Counts {
        uint32_t subs = 0;
        std::map<std::pair<uint16_t, rune::codec::FilterKey>, uint32_t> values;
        std::map<uint16_t, uint32_t>                                    tags;   // distinct index fields (P19)
    };
    static uint64_t key_(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<uint64_t>(static_cast<uint32_t>(topicId)) << 32) | static_cast<uint32_t>(partition);
    }
    std::unordered_map<uint64_t, Counts> byTp_;
    Limits                               limits_{};
};

}  // namespace rune::gateway
