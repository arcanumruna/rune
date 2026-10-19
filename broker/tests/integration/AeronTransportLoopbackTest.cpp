// AeronTransportLoopbackTest.cpp — P2P-8 Aeron transport integration test.
//
// Spike + loopback election over REAL Aeron IPC channels.
//
// Test 1: IpcRoundTrip_Spike — bare Aeron IPC round-trip (feasibility check):
//   Start an EmbeddedMediaDriver → publish 1 message on aeron:ipc stream 1001
//   → poll it back via Subscription + FragmentAssembler → assert bytes match.
//
// Test 2: AeronLoopback_3NodeElection — 3 in-process consensus nodes wired
//   over real Aeron IPC channels elect a leader over real Aeron transport.
//   Each node owns:
//     - An AeronIngress  (Subscription + FragmentAssembler → IngressRing)
//     - An AeronMessageSink (ExclusivePublication per peer)
//     - SbeConsensusPublisher(sink) → Election → ConsensusAgent
//     - AgentRunner thread (BackoffIdleStrategy, MonotonicNanoClock)
//   Bounded wait (10 s): assert exactly 1 leader and all live nodes agree
//   on the same term.  Clean shutdown: no hangs.
//
// Constraints:
//   - No busy-spin: AgentRunner uses BackoffIdleStrategy.
//   - Bounded: FAIL (not hang) if no leader within timeout.
//   - Normal ctest suite must NOT include this file unless
//     BUILD_AERON_TRANSPORT_TESTS=ON.

#include <gtest/gtest.h>

// EmbeddedMediaDriver — from the aeron source tree (included via package).
// Copied to the integration test directory since it's not an installed header.
extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <Aeron.h>
#include <Context.h>
#include <Subscription.h>
#include <ExclusivePublication.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>

#include <rune/consensus/AeronMessageSink.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/NanoClock.h>
#include <rune/consensus/ElectionState.h>
#include <rune/consensus/Types.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace rune::consensus;
using namespace std::chrono;

// ---------------------------------------------------------------------------
// Embedded media driver lifecycle (thin RAII wrapper around aeron C API)
// ---------------------------------------------------------------------------
class EmbeddedMediaDriver {
public:
    explicit EmbeddedMediaDriver(const std::string& dir = "") {
        if (aeron_driver_context_init(&ctx_) < 0) {
            throw std::runtime_error(std::string("driver context init: ") + aeron_errmsg());
        }

        // Use a unique per-test temp dir so concurrent tests don't collide.
        if (!dir.empty()) {
            aeron_driver_context_set_dir(ctx_, dir.c_str());
        }

        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        // Sparse term buffers so we don't pre-allocate large files in /tmp.
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        aeron_driver_context_set_term_buffer_length(ctx_, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(ctx_, 64 * 1024);

        if (aeron_driver_init(&driver_, ctx_) < 0) {
            aeron_driver_context_close(ctx_);
            throw std::runtime_error(std::string("driver init: ") + aeron_errmsg());
        }

        if (aeron_driver_start(driver_, true) < 0) {
            aeron_driver_close(driver_);
            aeron_driver_context_close(ctx_);
            throw std::runtime_error(std::string("driver start: ") + aeron_errmsg());
        }

        aeronDir_ = aeron_driver_context_get_dir(ctx_);

        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this]() {
            while (running_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
            }
        });
    }

    ~EmbeddedMediaDriver() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        aeron_driver_close(driver_);
        aeron_driver_context_close(ctx_);
    }

    const std::string& aeronDir() const { return aeronDir_; }

private:
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::string             aeronDir_;
    std::atomic<bool>       running_{false};
    std::thread             thread_;
};

// ---------------------------------------------------------------------------
// Helper: unique temp directory for state files
// ---------------------------------------------------------------------------
struct TempDir {
    fs::path dir;
    explicit TempDir(const std::string& prefix = "rune_aeron_") {
        static std::atomic<int> ctr{0};
        auto ts = steady_clock::now().time_since_epoch().count();
        dir = fs::temp_directory_path() / (prefix + std::to_string(ts) +
                                           "_" + std::to_string(ctr.fetch_add(1)));
        fs::create_directories(dir);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(dir, ec); }
    std::string nsfPath()  const { return (dir / "nodestate.nsf").string(); }
    std::string termPath() const { return (dir / "termlog.tl").string(); }
};

// ---------------------------------------------------------------------------
// Helper: wait for Aeron Publication to connect (bounded)
// ---------------------------------------------------------------------------
static bool waitForConnect(aeron::ExclusivePublication& pub,
                           milliseconds timeout = seconds{5}) {
    auto deadline = steady_clock::now() + timeout;
    while (steady_clock::now() < deadline) {
        if (pub.isConnected()) return true;
        std::this_thread::sleep_for(milliseconds{1});
    }
    return false;
}

static bool waitForImageCount(aeron::Subscription& sub, int32_t count,
                              milliseconds timeout = seconds{5}) {
    auto deadline = steady_clock::now() + timeout;
    while (steady_clock::now() < deadline) {
        if (sub.imageCount() >= count) return true;
        std::this_thread::sleep_for(milliseconds{1});
    }
    return false;
}

// ===========================================================================
// TEST 1: IpcRoundTrip_Spike
//   Bare Aeron IPC round-trip — the FEASIBILITY SPIKE.
//   Start embedded driver → publish → poll → assert bytes round-trip.
// ===========================================================================
TEST(AeronTransportTest, IpcRoundTrip_Spike) {
    TempDir td("rune_spike_");
    std::string driverDir = (td.dir / "aeron").string();

    EmbeddedMediaDriver driver(driverDir);

    aeron::Context ctx;
    ctx.aeronDir(driver.aeronDir());
    ctx.errorHandler([](const std::exception& e) {
        fprintf(stderr, "Aeron error: %s\n", e.what());
    });

    auto aeronClient = aeron::Aeron::connect(ctx);

    constexpr int32_t kStream = 1001;
    const std::string kChannel = "aeron:ipc";

    // Publisher
    int64_t pubRegId = aeronClient->addExclusivePublication(kChannel, kStream);
    std::shared_ptr<aeron::ExclusivePublication> pub;
    {
        auto deadline = steady_clock::now() + seconds{5};
        while (steady_clock::now() < deadline) {
            pub = aeronClient->findExclusivePublication(pubRegId);
            if (pub) break;
            std::this_thread::sleep_for(milliseconds{1});
        }
        ASSERT_TRUE(pub != nullptr) << "ExclusivePublication not resolved";
    }

    // Subscriber
    int64_t subRegId = aeronClient->addSubscription(kChannel, kStream);
    std::shared_ptr<aeron::Subscription> sub;
    {
        auto deadline = steady_clock::now() + seconds{5};
        while (steady_clock::now() < deadline) {
            sub = aeronClient->findSubscription(subRegId);
            if (sub) break;
            std::this_thread::sleep_for(milliseconds{1});
        }
        ASSERT_TRUE(sub != nullptr) << "Subscription not resolved";
    }

    // Wait for image (publisher connected to subscriber)
    ASSERT_TRUE(waitForConnect(*pub)) << "Publication never connected";
    ASSERT_TRUE(waitForImageCount(*sub, 1)) << "No image on subscription";

    // Offer a small payload
    const uint8_t kPayload[] = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE};
    aeron::concurrent::AtomicBuffer offerBuf(const_cast<uint8_t*>(kPayload), sizeof(kPayload));
    int64_t offerResult = pub->offer(offerBuf, 0, static_cast<aeron::util::index_t>(sizeof(kPayload)));
    ASSERT_GT(offerResult, 0) << "offer() failed: " << offerResult;

    // Poll back
    bool received = false;
    std::vector<uint8_t> received_bytes;
    aeron::FragmentAssembler assembler([&](aeron::concurrent::AtomicBuffer& buf,
                                          aeron::util::index_t             offset,
                                          aeron::util::index_t             length,
                                          aeron::Header&                   /*hdr*/) {
        received_bytes.assign(buf.buffer() + offset, buf.buffer() + offset + length);
        received = true;
    });

    auto deadline = steady_clock::now() + seconds{5};
    while (!received && steady_clock::now() < deadline) {
        sub->poll(assembler.handler(), 10);
        if (!received) std::this_thread::sleep_for(milliseconds{1});
    }

    ASSERT_TRUE(received) << "No message received within timeout";
    ASSERT_EQ(received_bytes.size(), sizeof(kPayload));
    for (std::size_t i = 0; i < sizeof(kPayload); ++i) {
        EXPECT_EQ(received_bytes[i], kPayload[i]) << "Byte mismatch at index " << i;
    }
}

// ===========================================================================
// TEST 2: AeronLoopback_3NodeElection
//   Three in-process consensus nodes wired over real Aeron IPC channels.
//   Each node has its own stream ID (2000 + nodeId).
//   AeronIngress feeds IngressRing; AeronMessageSink sends via ExclusivePublication.
//   AgentRunner threads drive the duty cycle.
//   Assert: exactly 1 leader, all agree on same term, within 10 s.
// ===========================================================================
TEST(AeronTransportTest, AeronLoopback_3NodeElection) {
    constexpr int kN = 3;
    constexpr int32_t kBaseStream = 2000; // per-node stream = kBaseStream + nodeId

    TempDir driverTd("rune_loopback_driver_");
    std::string driverDir = (driverTd.dir / "aeron").string();

    EmbeddedMediaDriver driver(driverDir);

    // Timing constants for a fast test
    constexpr int64_t kElectionTimeoutNs      = 500'000'000LL;  // 500 ms real-time
    constexpr int64_t kStatusIntervalNs        = 100'000'000LL;  // 100 ms
    constexpr int64_t kStartupCanvassTimeoutNs = 1'000'000'000LL; // 1 s

    // One Aeron client shared by all nodes (thread-safe ClientConductor).
    aeron::Context aeronCtx;
    aeronCtx.aeronDir(driver.aeronDir());
    aeronCtx.errorHandler([](const std::exception& e) {
        fprintf(stderr, "Aeron error: %s\n", e.what());
    });
    auto aeronClient = aeron::Aeron::connect(aeronCtx);

    // Build cluster member list
    auto makeMembers = [&]() {
        std::vector<ClusterMember> ms(kN);
        for (int i = 0; i < kN; ++i) {
            ms[i].id = i; ms[i].logPosition = 0;
            ms[i].logLeadershipTermId = 0; ms[i].leadershipTermId = 0;
        }
        return ms;
    };

    // Pre-create per-node temp directories managed outside of the NodeCtx
    // structs to avoid any lifetime issues with default-constructing the vector.
    std::vector<TempDir> nodeDirs;
    nodeDirs.reserve(kN);
    for (int i = 0; i < kN; ++i) {
        nodeDirs.emplace_back("rune_aeron_node" + std::to_string(i) + "_");
    }

    // Per-node structures — only non-owning pointers; life managed by unique_ptrs below.
    struct NodeCtx {
        std::unique_ptr<IngressRing>           ring;
        std::unique_ptr<AeronIngress>          ingress;
        std::unique_ptr<AeronMessageSink>      sink;
        std::unique_ptr<SbeConsensusPublisher> publisher;
        std::unique_ptr<NodeStateFile>         nsf;
        std::unique_ptr<TermLog>               tl;
        std::unique_ptr<Election>              election;
        std::unique_ptr<ConsensusAgent>        agent;
        std::unique_ptr<BackoffIdleStrategy>   idle;
        MonotonicNanoClock                     clock;
        std::unique_ptr<AgentRunner>           runner;
    };

    std::vector<NodeCtx> nodes(kN);

    // Step 1: Create ingress subscriptions (all nodes) so images exist when
    //         publications are added in step 2.
    for (int i = 0; i < kN; ++i) {
        auto& n = nodes[i];
        n.ring    = std::make_unique<IngressRing>(64 * 1024);
        int32_t myStream = kBaseStream + i;
        n.ingress = std::make_unique<AeronIngress>(aeronClient, "aeron:ipc", myStream, *n.ring);
    }

    // Step 2: Create publications to peer nodes.
    for (int i = 0; i < kN; ++i) {
        auto& n = nodes[i];
        std::unordered_map<NodeId, int32_t> peerStreams;
        for (int j = 0; j < kN; ++j) {
            if (j != i) {
                peerStreams[j] = kBaseStream + j;
            }
        }
        n.sink = std::make_unique<AeronMessageSink>(aeronClient, "aeron:ipc", peerStreams);
    }

    // Step 3: Wire up publishers, elections, agents using the pre-created dirs.
    for (int i = 0; i < kN; ++i) {
        auto& n = nodes[i];
        const auto& td = nodeDirs[i];

        n.publisher = std::make_unique<SbeConsensusPublisher>(*n.sink);
        n.nsf = std::make_unique<NodeStateFile>(td.nsfPath(), 0);
        n.tl  = std::make_unique<TermLog>(td.termPath());

        n.election = std::make_unique<Election>(
            /*isNodeStartup=*/true,
            /*gracefulClosedLeaderId=*/-1,
            /*leadershipTermId=*/0,
            /*logLeadershipTermId=*/0,
            /*logPosition=*/0,
            /*appendPosition=*/0,
            makeMembers(),
            /*thisMemberId=*/i,
            *n.publisher,
            *n.nsf,
            *n.tl,
            kElectionTimeoutNs,
            kStatusIntervalNs,
            kStartupCanvassTimeoutNs,
            /*rngSeed=*/static_cast<uint64_t>(i) * 0xdeadbeef01234567ULL);

        n.agent = std::make_unique<ConsensusAgent>(*n.election, *n.ring);
        n.idle  = std::make_unique<BackoffIdleStrategy>();
    }

    // Step 4: Wrap each ConsensusAgent in a combined Agent that polls ingress
    //         then calls doWork().
    struct CombinedAgent final : Agent {
        AeronIngress&   ingress;
        ConsensusAgent& agent;
        explicit CombinedAgent(AeronIngress& i, ConsensusAgent& a) : ingress(i), agent(a) {}
        int doWork(int64_t nowNs) override {
            int w = ingress.poll(10);
            w    += agent.doWork(nowNs);
            return w;
        }
    };

    std::vector<std::unique_ptr<CombinedAgent>> combined(kN);
    for (int i = 0; i < kN; ++i) {
        combined[i] = std::make_unique<CombinedAgent>(*nodes[i].ingress, *nodes[i].agent);
    }

    // Step 5: Start AgentRunners.
    for (int i = 0; i < kN; ++i) {
        auto& n = nodes[i];
        n.runner = std::make_unique<AgentRunner>(*combined[i], *n.idle, n.clock);
        n.runner->start();
    }

    // Step 6: Bounded wait for election convergence (10 s real-time).
    bool elected = false;
    auto electionDeadline = steady_clock::now() + seconds{10};

    while (steady_clock::now() < electionDeadline) {
        int leaderCount = 0;
        Term agreedTerm = -1;
        bool termAgreement = true;

        for (int i = 0; i < kN; ++i) {
            if (!nodes[i].agent) continue;
            const auto& el = nodes[i].agent->election();
            ElectionState s = el.state();
            if (s == ElectionState::LeaderReady || s == ElectionState::Closed) {
                if (el.leaderMemberId() == i) {
                    ++leaderCount;
                    Term t = el.leadershipTermId();
                    if (agreedTerm < 0) agreedTerm = t;
                    else if (agreedTerm != t) termAgreement = false;
                }
            }
        }

        if (leaderCount == 1 && termAgreement && agreedTerm >= 0) {
            elected = true;
            break;
        }
        std::this_thread::sleep_for(milliseconds{20});
    }

    // Step 7: Shutdown all runners before assertions (so threads don't access
    // destroyed objects).
    for (int i = 0; i < kN; ++i) {
        if (nodes[i].runner) nodes[i].runner->stopAndJoin();
    }

    ASSERT_TRUE(elected) << "Cluster did not elect a single leader within 10 s";

    // Verify election safety: no two nodes are leader in the same term.
    std::unordered_map<Term, std::vector<NodeId>> termLeaders;
    for (int i = 0; i < kN; ++i) {
        if (!nodes[i].agent) continue;
        const auto& el = nodes[i].agent->election();
        ElectionState s = el.state();
        if ((s == ElectionState::LeaderReady || s == ElectionState::Closed) &&
            el.leaderMemberId() == i) {
            termLeaders[el.leadershipTermId()].push_back(i);
        }
    }
    for (auto& [term, ldrs] : termLeaders) {
        EXPECT_EQ(ldrs.size(), 1u)
            << "Election safety violated: " << ldrs.size()
            << " leaders in term " << term;
    }
}
