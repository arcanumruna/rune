#pragma once
// TransientViewBrokerSupport.h - checked writes Part 10: helpers of the real-broker tests of persisted views over a transient
// topic: the two view plans, a producer of {id, hp} records, a bounded row wait and a bounded checkpoint-file wait.
#include "client/OneBroker.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/view/ViewPlan.h>

#include <chrono>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace rune::node::test::transient_view_broker {

using namespace std::chrono_literals;
namespace rc = rune::client;

/// {id, hp} Replace view keyed by id over `src`.
inline std::vector<uint8_t> rowsBlob(const std::string& name, int32_t src, uint8_t persistence = 0) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName    = name;
    p.key         = "id";
    p.persistence = persistence;
    p.sources.push_back(rv::SourceSpec{src, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

/// {hp, n, minId} per hp: the number of keys at that hp and the smallest of them.
inline std::vector<uint8_t> totalsBlob(const std::string& name, int32_t src) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = name;
    p.key      = "hp";
    p.sources.push_back(rv::SourceSpec{src, "hp"});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"n", rune::codec::PrimType::Int64, 0, "", rv::MergeRule::CountAll});
    p.output.push_back(rv::AuthoredField{"minId", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Min});
    return rv::serializeAuthored(p);
}

inline std::string le(uint64_t v, std::size_t n) {
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n; ++i) s[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
    return s;
}

/// One client + producer over the transient topic `name`; put() awaits the ack of the record {id, hp}.
class Prod {
public:
    Prod(rc::ClientConfig cfg, const std::string& name, int32_t topicId, rc::Acks acks) {
        cfg.topics.push_back(rc::TopicSpec{name, topicId, 1});
        client_ = std::make_unique<rc::Client>(cfg);
        rc::ProducerConfig pc;
        pc.acks     = acks;
        pc.lingerUs = 1;
        prod_.emplace(client_->producer(pc));
        client_->connectAndWait(30s);
        cq_ = std::make_unique<rc::Completions>(*client_);
        topic_.emplace(client_->topic(name));
    }
    ~Prod() {
        if (prod_) prod_->closeImmediately();
        if (client_) client_->close();
    }
    Prod(const Prod&)            = delete;
    Prod& operator=(const Prod&) = delete;

    [[nodiscard]] bool put(uint64_t id, uint64_t hp) {
        const std::string v = le(16, 2) + le(1, 2) + le(id, 8) + le(hp, 8);
        const uint64_t tok  = next_++;
        rc::SendResult r = rc::SendResult::BackPressured;
        while (r == rc::SendResult::BackPressured)
            r = prod_->trySend(*topic_, rc::Key(id), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()), *cq_, tok);
        if (r != rc::SendResult::Ok) return false;
        prod_->flush();
        std::optional<rc::Completion> out;
        (void)cq_->waitFor(tok, 30s, [&](const rc::Completion& d, std::span<const uint8_t>) { out = d; });
        return out && out->status == rc::OpStatus::Ok;
    }

private:
    std::unique_ptr<rc::Client>      client_;
    std::optional<rc::Producer>      prod_;
    std::unique_ptr<rc::Completions> cq_;
    std::optional<rc::TopicHandle>   topic_;
    uint64_t                         next_ = 1;
};

/// Polls GetViewRow of `key` until the row holds the Int64 columns `want` (in output order); 30 s bound.  NotReady, NotLeader
/// and AbsentKey are the states of a fold that has not got there yet.
inline bool awaitRow(rc::Client& c, int32_t viewTopicId, int64_t key, const std::vector<int64_t>& want) {
    const std::vector<rune::codec::PrimType> types(want.size(), rune::codec::PrimType::Int64);
    const auto layout = rc::view::buildRowLayout(types);
    for (const auto deadline = std::chrono::steady_clock::now() + 30s; std::chrono::steady_clock::now() < deadline;) {
        rc::ViewRowResult row;
        try { row = c.getViewRowAndWait(viewTopicId, key, 0, 10s); } catch (const std::exception&) {}
        if (row.status == rc::ViewRowResult::kOk) {
            bool same = true;
            for (std::size_t i = 0; i < want.size(); ++i) {
                const auto f = rc::view::readSlot(row.rowBytes, layout, static_cast<uint16_t>(i));
                same = same && f.present && f.i == want[i];
            }
            if (same) return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

/// The last record of the committed checkpoint file of (viewTopicId, partition 0) records a cursor >= `want`.
inline bool awaitCheckpointO(const std::string& dataDir, int32_t viewTopicId, int64_t want, std::chrono::milliseconds within) {
    const std::string path = rune::node::viewCheckpointFilePath(dataDir + "/__view_snapshots", viewTopicId, 0);
    for (const auto deadline = std::chrono::steady_clock::now() + within; std::chrono::steady_clock::now() < deadline;) {
        std::ifstream f(path, std::ios::binary);
        const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        const auto recs = rune::node::splitViewCheckpointRecords(bytes);
        if (!recs.empty() && recs.back().size() >= 12) {
            int64_t o = 0;
            for (int i = 0; i < 8; ++i) o |= static_cast<int64_t>(recs.back()[4 + i]) << (8 * i);
            if (o >= want) return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

}  // namespace rune::node::test::transient_view_broker
