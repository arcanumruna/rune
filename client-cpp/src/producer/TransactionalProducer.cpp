// src/producer/TransactionalProducer.cpp
//
// TX-5 — TransactionalProducer: public facade (PIMPL → TransactionalProducerImpl).

#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/producer/TransactionalProducerImpl.h>

#include <stdexcept>

namespace rune::client {

TransactionalProducer::TransactionalProducer(
    std::shared_ptr<producer::TransactionalProducerImpl> impl) noexcept
    : impl_(std::move(impl))
{}

std::future<void> TransactionalProducer::initTransactions()
{
    return impl_->submitInitTransactions();
}

void TransactionalProducer::beginTransaction()
{
    impl_->beginTransaction();
}

void TransactionalProducer::send(TopicHandle topic,
                                  const Key& key,
                                  std::span<const uint8_t> value)
{
    // fix1 I8: never silently drop an invalid topic (TransactionalProducer has
    // no callback to report through, unlike Producer::send's opt-in overloads,
    // so — consistent with the void Producer::send() policy — report via throw).
    if (!topic.valid()) {
        throw std::invalid_argument("TransactionalProducer::send: invalid topic handle");
    }
    const int32_t partition = key.partition(topic.partitionCount);
    impl_->submitSend(topic.topicId, partition, key, value);
}

std::future<void> TransactionalProducer::sendOffsetsToTransaction(
    const std::map<TopicPartitionKey, int64_t>& offsets,
    const std::string& groupId)
{
    return impl_->submitSendOffsets(offsets, groupId);
}

std::future<void> TransactionalProducer::commitTransaction()
{
    return impl_->submitCommit();
}

std::future<void> TransactionalProducer::abortTransaction()
{
    return impl_->submitAbort();
}

int64_t TransactionalProducer::producerId() const noexcept
{
    return impl_->producerId();
}

int16_t TransactionalProducer::producerEpoch() const noexcept
{
    return impl_->producerEpoch();
}

bool TransactionalProducer::transactionActive() const noexcept
{
    return impl_->transactionActive();
}

} // namespace rune::client
