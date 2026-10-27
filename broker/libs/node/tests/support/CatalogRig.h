#pragma once
// CatalogRig.h — catalog history Task 7: the CatalogApplier fixture, shared by
// CatalogApplierTest.cpp, CatalogAllocRegressionTest.cpp (own binary) and Task 8.
//
//   * a real disk Log under a temp dir (`<tmp>/__catalog`, as dataDir/__catalog);
//   * a LEADER PartitionReplica with ISR {1, 2}: an event is COMMITTED once the
//     scripted follower 2 fetches at the leader's LEO (appendEvents), and held
//     at the HW while 2 stays silent (appendUncommitted);
//   * a SchemaRegistry, a ViewRegistry and an applier already moved to Live with
//     the recorder's liveEffects();
//   * a Recorder whose bootEffects() / liveEffects() push every effect into
//     vectors and into `sequence` as "<effect>:<topicId>";
//   * restart(): the same dir — log recovered (and re-committed: follower 2 acks
//     its end), fresh registries, a fresh applier in AwaitingBoot, the recorder
//     cleared.
//
// Events come from rune::catalog::testing::goldenScriptV1() (23 events, 0..22).
// Single-threaded test fixture: plain members, no atomics.

#include "support/TestNodeConfig.h"
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/gateway/ObjectIndex.h>
#include <rune/node/CatalogApplier.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

namespace rune::node::test {

namespace rig_detail {
inline int gRigSeq = 0;   // single-threaded test binaries
}

/// Records every effect the applier posts.
struct Recorder {
    std::vector<int64_t>                    appliedOffsets;
    std::vector<uint64_t>                   appliedDigests;
    std::vector<rune::catalog::ApplyError>  halts;
    std::vector<std::string>                sequence;          // "<effect>:<topicId>"
    std::vector<int32_t>                    bootViewInstalls;  // viewTopicIds installed through the BOOT sink
    std::vector<int32_t>                    liveViewInstalls;  // ... through the LIVE sink
    std::vector<std::pair<int, std::string>> storageFailures;
    std::function<void()>                   onTopicApplied;    // test hook, runs inside topicApplied
    std::function<void(int32_t topicId)>    onTopicAppliedId;  // test hook (may throw: a simulated crash)
    // Namespaces Task 5: what the control thread's mirror learns from the effects (nsId-aware), not in `sequence`.
    rune::gateway::ObjectIndex                      objects;
    std::vector<rune::catalog::NamespaceRecord>     nsUpserts;

    void clear() {
        appliedOffsets.clear(); appliedDigests.clear(); halts.clear(); sequence.clear();
        bootViewInstalls.clear(); liveViewInstalls.clear(); storageFailures.clear();
        onTopicApplied = nullptr;
        onTopicAppliedId = nullptr;
        objects = rune::gateway::ObjectIndex{};
        nsUpserts.clear();
    }

    [[nodiscard]] CatalogEffects effects(std::vector<int32_t>& viewInstalls) {
        CatalogEffects e;
        e.topicApplied = [this](const rune::catalog::TopicObject& t) {
            if (onTopicApplied) onTopicApplied();
            if (onTopicAppliedId) onTopicAppliedId(t.topicId);
            objects.onTopic(t.nsId, t.topicId, t.name);
            sequence.push_back("topicApplied:" + std::to_string(t.topicId));
        };
        e.sinkApplied = [this](const SinkSpec& s) {
            objects.onView(s.nsId, s.topicId, s.name);
            sequence.push_back("sinkApplied:" + std::to_string(s.topicId));
        };
        e.namespaceUpserted = [this](const rune::catalog::NamespaceRecord& r) { nsUpserts.push_back(r); };
        e.topicDropped = [this](int32_t id, uint32_t, const std::string&) {
            objects.onDropped(id);
            sequence.push_back("topicDropped:" + std::to_string(id));
        };
        e.topicConfig = [this](int32_t id, std::span<const rune::catalog::ConfigChange>) {
            sequence.push_back("topicConfig:" + std::to_string(id));
        };
        e.schemaChanged = [this](const rune::catalog::TopicObject& t, uint16_t) {
            sequence.push_back("schemaChanged:" + std::to_string(t.topicId));
        };
        e.viewInstalled = [this, &viewInstalls](const rune::catalog::ViewObject& v) {
            viewInstalls.push_back(v.def.viewTopicId);
            sequence.push_back("viewInstalled:" + std::to_string(v.def.viewTopicId));
        };
        e.viewUninstalled = [this](int32_t id) { sequence.push_back("viewUninstalled:" + std::to_string(id)); };
        e.applied = [this](int64_t off, uint64_t digest) {
            appliedOffsets.push_back(off);
            appliedDigests.push_back(digest);
        };
        e.halted = [this](const rune::catalog::ApplyError& err) { halts.push_back(err); };
        e.storageFailed = [this](int err, const std::string& dir) { storageFailures.emplace_back(err, dir); };
        return e;
    }
    [[nodiscard]] BootEffects bootEffects() { return BootEffects{effects(bootViewInstalls)}; }
    [[nodiscard]] LiveEffects liveEffects() { return LiveEffects{effects(liveViewInstalls)}; }
};

class CatalogRig {
public:
    static constexpr int32_t kSelf     = 1;
    static constexpr int32_t kFollower = 2;

    explicit CatalogRig(const char* tag)
        : root_(std::filesystem::temp_directory_path() /
                (std::string("rune_catrig_") + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(rig_detail::gRigSeq++))),
          applier(catalogDirPath_().string(), schemas, views),
          golden_(rune::catalog::testing::goldenScriptV1()) {
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(catalogDirPath_());
        open_();
        applier.goLive(rec.liveEffects());
    }
    ~CatalogRig() {
        replica_.reset();
        log_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    CatalogRig(const CatalogRig&)            = delete;
    CatalogRig& operator=(const CatalogRig&) = delete;

    [[nodiscard]] rune::replication::PartitionReplica& replica() { return *replica_; }
    [[nodiscard]] rune::storage::Log& log() { return *log_; }
    [[nodiscard]] std::string catalogDir() const { return catalogDirPath_().string(); }
    [[nodiscard]] const std::vector<rune::catalog::CatalogEvent>& golden() const noexcept { return golden_; }

    /// Golden events [from, to), each committed (follower 2 acks at the LEO).
    void appendEvents(std::size_t from, std::size_t to) {
        for (std::size_t i = from; i < to; ++i) appendCommitted_(golden_.at(i));
    }
    /// Golden event i with its fingerprint flipped (^ 1), committed.
    void appendTampered(std::size_t i) {
        rune::catalog::CatalogEvent e = golden_.at(i);
        e.fingerprint ^= 1;
        appendCommitted_(e);
    }
    /// Golden event i appended, NOT committed: follower 2 stays silent (HW held).
    void appendUncommitted(std::size_t i) { (void)appendBytes_(encode_(golden_.at(i))); }
    /// Any event (not from the golden script), committed.
    void appendEventCommitted(const rune::catalog::CatalogEvent& e) { appendCommitted_(e); }
    /// Encode `e` exactly as appendEvents does.
    [[nodiscard]] static std::vector<uint8_t> encodeEvent(const rune::catalog::CatalogEvent& e) { return encode_(e); }
    /// Raw record bytes, committed.
    void appendRawCommitted(const std::vector<uint8_t>& bytes) { ack_(appendBytes_(bytes)); }

    /// Same dir: the log recovered, fresh registries, a fresh applier (AwaitingBoot), the recorder cleared.
    void restart() {
        replica_.reset();
        log_.reset();
        schemas = SchemaRegistry{};
        views   = ViewRegistry{};
        applier = CatalogApplier(catalogDir(), schemas, views);
        rec.clear();
        ++epoch_;
        open_();
    }

    SchemaRegistry schemas;
    ViewRegistry   views;
    Recorder       rec;

private:
    std::filesystem::path root_;

public:
    CatalogApplier applier;

private:
    [[nodiscard]] std::filesystem::path catalogDirPath_() const { return root_ / "__catalog"; }

    void open_() {
        log_ = std::make_unique<rune::storage::Log>(catalogDir(), kTestSegmentRollBytes);
        log_->recover();
        NodeConfig nc;
        boundTestSegments(nc);
        nc.nodeId = kSelf;
        rune::replication::PartitionConfig cfg = makeCatalogPartitionConfig(nc, 2);
        replica_ = std::make_unique<rune::replication::PartitionReplica>(
            cfg, rune::replication::Role::Follower, 0, std::vector<rune::replication::NodeId>{kSelf, kFollower},
            std::vector<rune::replication::NodeId>{kSelf, kFollower}, log_.get(), &transport_);
        replica_->becomeLeader(epoch_, {kSelf, kFollower}, /*partitionEpoch=*/0);
        (void)replica_->doWork(now_ += 1'000'000);
        // A restarted leader's HW is 0 until a follower fetches: the follower (caught
        // up before the restart) acks the recovered log end, so it is committed again.
        if (replica_->logEndOffset() > 0) ack_(0);
    }

    static std::vector<uint8_t> encode_(const rune::catalog::CatalogEvent& e) {
        auto bytes = rune::catalog::encode(e);
        if (!bytes) throw std::logic_error("CatalogRig: golden event does not encode");
        return std::move(*bytes);
    }

    rune::replication::Offset appendBytes_(const std::vector<uint8_t>& bytes) {
        const rune::replication::Offset base =
            replica_->append(std::span<const uint8_t>(bytes.data(), bytes.size()), /*acksAll=*/true);
        if (base < 0) throw std::logic_error("CatalogRig: append refused (" + std::to_string(base) + ")");
        return base;
    }

    void appendCommitted_(const rune::catalog::CatalogEvent& e) { ack_(appendBytes_(encode_(e))); }

    /// Follower 2 fetches at the leader's LEO → the HW covers everything appended.
    void ack_(rune::replication::Offset /*base*/) {
        rune::replication::FetchRequestMsg ack{};
        ack.leaderEpoch      = epoch_;
        ack.partition        = kCatalogPartition;
        ack.followerId       = kFollower;
        ack.fetchOffset      = replica_->logEndOffset();
        ack.maxBytes         = 1 << 20;
        ack.topicId          = kCatalogTopicId;
        ack.lastFetchedEpoch = -1;
        replica_->onFetchRequest(ack, now_ += 1'000'000);
        (void)replica_->doWork(now_ += 1'000'000);
        if (replica_->highWatermark() != replica_->logEndOffset())
            throw std::logic_error("CatalogRig: the HW did not reach the LEO after the follower's ack");
    }

    std::vector<rune::catalog::CatalogEvent>             golden_;
    std::unique_ptr<rune::storage::Log>                  log_;
    rune::replication::NullPartitionTransport            transport_;
    std::unique_ptr<rune::replication::PartitionReplica> replica_;
    int32_t                                              epoch_ = 1;
    int64_t                                              now_   = 1'000'000;
};

} // namespace rune::node::test
