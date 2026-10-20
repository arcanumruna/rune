// include/rune/client/producer/ProducerRef.h - Phase 2b F (FD7): a borrowed, non-owning handle to a Producer.
//
// TypedTopic<T, ProducerT> owns its producer by value.  A typed topic that must write INSIDE another producer's
// transaction (or share one producer across several topics) uses TypedTopic<T, ProducerRef> instead: the same
// send / claim API, forwarded to the Producer it borrows.  Open one with
//   Client::openTopicAndWait<T>(name, producer, timeout)
//
// Lifetime: a ProducerRef must not outlive the Producer it borrows (a dangling one is undefined behaviour; a
// Producer that was moved from or closed answers like any such Producer).  Threading: exactly the borrowed
// Producer's (a transactional producer: ONE issuing thread, FD3).  Trivially copyable; no allocation.
#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>

#include <cstdint>
#include <span>
#include <type_traits>

namespace rune::client {

class ProducerRef {
public:
    explicit ProducerRef(Producer& p) noexcept : p_(&p) {}

    SendResult trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value) const noexcept {
        return p_->trySend(topic, key, value);
    }
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) const {
        p_->send(topic, key, value);
    }
    SendResult trySendTo(TopicHandle topic, int32_t partition, const Key& key,
                         std::span<const uint8_t> value) const noexcept {
        return p_->trySendTo(topic, partition, key, value);
    }
    void sendTo(TopicHandle topic, int32_t partition, const Key& key, std::span<const uint8_t> value) const {
        p_->sendTo(topic, partition, key, value);
    }
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen) const noexcept {
        return p_->tryClaim(topic, key, valueLen);
    }
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) const noexcept {
        return p_->claimBatch(topic, key, n, recLen);
    }

    /// The borrowed producer (lifecycle calls: begin / commit / abort ...).
    [[nodiscard]] Producer& producer() const noexcept { return *p_; }

private:
    Producer* p_;
};

static_assert(std::is_trivially_copyable_v<ProducerRef>);

}  // namespace rune::client
