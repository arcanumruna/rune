#pragma once
// SecurityRig.h — authentication Task 9: the SecurityApplier fixture (CatalogRig.h's counterpart), shared by
// SecurityApplierTest.cpp and SecurityAllocRegressionTest.cpp (own binary).
//
//   * a real disk Log under a temp dir (`<tmp>/__security`, as dataDir/__security);
//   * a LEADER PartitionReplica with ISR {1, 2}: an event is COMMITTED once the scripted follower 2 fetches at the
//     leader's LEO (appendEvents), and held at the HW while 2 stays silent (appendUncommitted);
//   * an applier already moved to Live with the recorder's liveEffects();
//   * a Recorder whose bootEffects() / liveEffects() push every effect into vectors;
//   * restart(): the same dir — log recovered (and re-committed: follower 2 acks its end), a fresh applier in
//     AwaitingBoot, the recorder cleared.
//
// Events come from rune::security::testing::goldenScriptV1() (16 events, 0..15).  Single-threaded: plain members.

#include "support/TestNodeConfig.h"
#include <rune/node/NodeConfig.h>
#include <rune/node/SecurityApplier.h>
#include <rune/node/SecurityPartition.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/security/SecurityEvent.h>
#include <rune/security/testing/SecurityGoldenScript.h>
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

namespace sec_rig_detail { inline int gSeq = 0; }

struct SecurityRecorder {
    struct Mirrored { int64_t offset; std::vector<uint8_t> bytes; };
    std::vector<Mirrored>                         mirrored;          // eventApplied
    std::vector<int64_t>                          appliedOffsets;
    std::vector<uint64_t>                         appliedDigests;
    std::vector<rune::security::ApplyError>       halts;
    std::vector<std::pair<int, std::string>>      storageFailures;
    std::vector<int64_t>                          bootMirrored;      // offsets seen through the BOOT sink (must stay empty)
    std::function<void(int64_t offset)>           onEventApplied;    // test hook (may throw: a simulated crash)

    void clear() {
        mirrored.clear(); appliedOffsets.clear(); appliedDigests.clear(); halts.clear(); storageFailures.clear();
        bootMirrored.clear(); onEventApplied = nullptr;
    }
    [[nodiscard]] SecurityEffects effects(bool boot) {
        SecurityEffects e;
        e.eventApplied = [this, boot](int64_t off, std::span<const uint8_t> bytes) {
            if (boot) bootMirrored.push_back(off);
            mirrored.push_back({off, std::vector<uint8_t>(bytes.begin(), bytes.end())});
            if (onEventApplied) onEventApplied(off);
        };
        e.applied = [this](int64_t off, uint64_t digest) {
            appliedOffsets.push_back(off);
            appliedDigests.push_back(digest);
        };
        e.halted = [this](const rune::security::ApplyError& err) { halts.push_back(err); };
        e.storageFailed = [this](int err, const std::string& dir) { storageFailures.emplace_back(err, dir); };
        return e;
    }
    [[nodiscard]] SecurityBootEffects bootEffects() { return SecurityBootEffects{effects(true)}; }
    [[nodiscard]] SecurityLiveEffects liveEffects() { return SecurityLiveEffects{effects(false)}; }
};

class SecurityRig {
public:
    static constexpr int32_t kSelf     = 1;
    static constexpr int32_t kFollower = 2;

    explicit SecurityRig(const char* tag, bool goLive = true)
        : root_(std::filesystem::temp_directory_path() /
                (std::string("rune_secrig_") + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(sec_rig_detail::gSeq++))),
          applier(securityDirPath_().string()),
          golden_(rune::security::testing::goldenScriptV1()) {
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(securityDirPath_());
        open_();
        if (goLive) applier.goLive(rec.liveEffects());
    }
    ~SecurityRig() {
        replica_.reset();
        log_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }
    SecurityRig(const SecurityRig&)            = delete;
    SecurityRig& operator=(const SecurityRig&) = delete;

    [[nodiscard]] rune::replication::PartitionReplica& replica() { return *replica_; }
    [[nodiscard]] rune::storage::Log& log() { return *log_; }
    [[nodiscard]] std::string securityDir() const { return securityDirPath_().string(); }
    [[nodiscard]] const std::vector<rune::security::SecurityEvent>& golden() const noexcept { return golden_; }

    /// Golden events [from, to), each committed (follower 2 acks at the LEO).
    void appendEvents(std::size_t from, std::size_t to) {
        for (std::size_t i = from; i < to; ++i) appendCommitted_(golden_.at(i));
    }
    /// Golden event i with its fingerprint flipped (^ 1), committed.
    void appendTampered(std::size_t i) {
        rune::security::SecurityEvent e = golden_.at(i);
        e.fingerprint ^= 1;
        appendCommitted_(e);
    }
    /// Golden event i appended, NOT committed: follower 2 stays silent (HW held).
    void appendUncommitted(std::size_t i) { (void)appendBytes_(encode_(golden_.at(i))); }
    /// Any event, committed.
    void appendEventCommitted(const rune::security::SecurityEvent& e) { appendCommitted_(e); }
    /// Raw record bytes, committed.
    void appendRawCommitted(const std::vector<uint8_t>& bytes) { ack_(appendBytes_(bytes)); }
    [[nodiscard]] static std::vector<uint8_t> encodeEvent(const rune::security::SecurityEvent& e) { return encode_(e); }

    /// Same dir: the log recovered, a fresh applier (AwaitingBoot), the recorder cleared.
    void restart() {
        replica_.reset();
        log_.reset();
        applier = SecurityApplier(securityDir());
        rec.clear();
        ++epoch_;
        open_();
    }

    SecurityRecorder rec;

private:
    std::filesystem::path root_;

public:
    SecurityApplier applier;

private:
    [[nodiscard]] std::filesystem::path securityDirPath_() const { return root_ / "__security"; }

    void open_() {
        log_ = std::make_unique<rune::storage::Log>(securityDir(), kTestSegmentRollBytes);
        log_->recover();
        NodeConfig nc;
        boundTestSegments(nc);
        nc.nodeId = kSelf;
        rune::replication::PartitionConfig cfg = makeSecurityPartitionConfig(nc, 2);
        replica_ = std::make_unique<rune::replication::PartitionReplica>(
            cfg, rune::replication::Role::Follower, 0, std::vector<rune::replication::NodeId>{kSelf, kFollower},
            std::vector<rune::replication::NodeId>{kSelf, kFollower}, log_.get(), &transport_);
        replica_->becomeLeader(epoch_, {kSelf, kFollower}, /*partitionEpoch=*/0);
        (void)replica_->doWork(now_ += 1'000'000);
        if (replica_->logEndOffset() > 0) ack_(0);
    }

    static std::vector<uint8_t> encode_(const rune::security::SecurityEvent& e) {
        auto bytes = rune::security::encode(e);
        if (!bytes) throw std::logic_error("SecurityRig: event does not encode");
        return std::move(*bytes);
    }

    rune::replication::Offset appendBytes_(const std::vector<uint8_t>& bytes) {
        const rune::replication::Offset base =
            replica_->append(std::span<const uint8_t>(bytes.data(), bytes.size()), /*acksAll=*/true);
        if (base < 0) throw std::logic_error("SecurityRig: append refused (" + std::to_string(base) + ")");
        return base;
    }

    void appendCommitted_(const rune::security::SecurityEvent& e) { ack_(appendBytes_(encode_(e))); }

    void ack_(rune::replication::Offset /*base*/) {
        rune::replication::FetchRequestMsg ack{};
        ack.leaderEpoch      = epoch_;
        ack.partition        = kSecurityPartition;
        ack.followerId       = kFollower;
        ack.fetchOffset      = replica_->logEndOffset();
        ack.maxBytes         = 1 << 20;
        ack.topicId          = kSecurityTopicId;
        ack.lastFetchedEpoch = -1;
        replica_->onFetchRequest(ack, now_ += 1'000'000);
        (void)replica_->doWork(now_ += 1'000'000);
        if (replica_->highWatermark() != replica_->logEndOffset())
            throw std::logic_error("SecurityRig: the HW did not reach the LEO after the follower's ack");
    }

    std::vector<rune::security::SecurityEvent>           golden_;
    std::unique_ptr<rune::storage::Log>                  log_;
    rune::replication::NullPartitionTransport            transport_;
    std::unique_ptr<rune::replication::PartitionReplica> replica_;
    int32_t                                              epoch_ = 1;
    int64_t                                              now_   = 1'000'000;
};

} // namespace rune::node::test
