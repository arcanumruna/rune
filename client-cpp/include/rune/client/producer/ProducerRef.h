// include/rune/client/producer/ProducerRef.h - Phase 2b F (FD7): a borrowed, non-owning handle to a Producer.
//
// TypedTopic<T, ProducerT> owns its producer by value.  A typed topic that must write INSIDE another producer's
// transaction (or share one producer across several topics) uses TypedTopic<T, ProducerRef> instead: the same
// send / claim API, forwarded to the Producer it borrows.  Open one with
//   Client::openTopicAndWait<T>(name, producer, timeout)
//
// Lifetime: a ProducerRef must not outlive the Producer it borrows (a dangling one is undefined behaviour; a
// Producer that was moved from or closed answers like any such Producer).  Debug builds (no NDEBUG) assert it on
// every operation while the duty side still holds the producer's impl (ProducerImpl::handleDetached(), the check
// BatchBuilder makes): a ref that outlived its Producer aborts instead of touching a dead handle.  Threading: exactly the borrowed
// Producer's (a transactional producer: ONE issuing thread, FD3).  Trivially copyable; no allocation.
#pragma once

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>

#include <cassert>
#include <cstdint>
#include <span>
#include <type_traits>

namespace rune::client {

class ProducerRef {
public:
    explicit ProducerRef(Producer& p) noexcept : p_(&p), impl_(p.implForRef_()) {}

    SendResult trySend(TopicHandle topic, const Key& key, std::span<const uint8_t> value) const noexcept {
        check_();
        return p_->trySend(topic, key, value);
    }
    void send(TopicHandle topic, const Key& key, std::span<const uint8_t> value) const {
        check_();
        p_->send(topic, key, value);
    }
    SendResult trySendTo(TopicHandle topic, int32_t partition, const Key& key,
                         std::span<const uint8_t> value) const noexcept {
        check_();
        return p_->trySendTo(topic, partition, key, value);
    }
    void sendTo(TopicHandle topic, int32_t partition, const Key& key, std::span<const uint8_t> value) const {
        check_();
        p_->sendTo(topic, partition, key, value);
    }
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen) const noexcept {
        check_();
        return p_->tryClaim(topic, key, valueLen);
    }
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) const noexcept {
        check_();
        return p_->claimBatch(topic, key, n, recLen);
    }
    // #692: the acked claimBatch forms Producer declares (one completion for the whole batch).
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen, Completions& cq,
                                        Token token) const noexcept {
        check_();
        return p_->claimBatch(topic, key, n, recLen, cq, token);
    }
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen,
                                        AckToken token) const noexcept {
        check_();
        return p_->claimBatch(topic, key, n, recLen, token);
    }

    /// The borrowed producer (lifecycle calls: begin / commit / abort ...).
    [[nodiscard]] Producer& producer() const noexcept {
        check_();
        return *p_;
    }

private:
    /// FD7 debug check: the borrowed Producer's handle is still attached (see the file comment).
    void check_() const noexcept {
#ifndef NDEBUG
        assert(!detail::producerImplDetached(impl_) && "ProducerRef: used after its Producer was destroyed");
#endif
    }

    Producer*                    p_;
    const producer::ProducerImpl* impl_;   ///< the borrowed producer's impl (identity for the debug check only)
};

static_assert(std::is_trivially_copyable_v<ProducerRef>);

}  // namespace rune::client
