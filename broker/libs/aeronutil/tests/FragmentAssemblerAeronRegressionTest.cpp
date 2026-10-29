// FragmentAssemblerAeronRegressionTest.cpp — real-Aeron regression: Aeron 1.51's C++ wrapper hands every fragment
// handler a buffer over the payload with offset 0, so reassembly must judge contiguity by header.termOffset().
// An embedded C media driver carries a 60 000 B message over a 4 KiB MTU (many fragments) and a 10 B message.
#include <rune/aeronutil/FragmentAssembler.h>
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>
#include <Context.h>
#include <Publication.h>
#include <Subscription.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
namespace fs = std::filesystem;

namespace {
constexpr int kStream = 1001;
constexpr const char* kChannel = "aeron:ipc?mtu=4096|term-length=1m";
constexpr std::size_t kBig = 60000, kSmall = 10;
constexpr int kMaxSpins = 20000;

std::string pattern(std::size_t n) {
    std::string s(n, '\0');
    for (std::size_t i = 0; i < n; ++i) s[i] = static_cast<char>((i * 31) & 0xff);
    return s;
}

class FragmentAssemblerAeronRegression : public ::testing::Test {
protected:
    void SetUp() override {
        root_ = fs::temp_directory_path() / ("rune-fragasm-" + std::to_string(::getpid()));
        fs::create_directories(root_);
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir              = (root_ / "aeron").string();
        cfg.termBufferBytes  = 1 << 20;
        cfg.termBufferSparse = true;
        cfg.udpGso           = false;
        cfg.epollPoll        = false;
        driver_ = std::make_unique<rune::node::EmbeddedDriver>(std::move(cfg), rune::node::DriverMode::Threaded);
        const std::string dir = driver_->dir();
        aeron::Context c;
        c.aeronDir(dir);
        aeron_ = aeron::Aeron::connect(c);
        ASSERT_NE(aeron_, nullptr);
        const auto pid = aeron_->addPublication(kChannel, kStream);
        const auto sid = aeron_->addSubscription(kChannel, kStream);
        for (int i = 0; i < kMaxSpins && !(pub_ && sub_); ++i) {
            if (!pub_) pub_ = aeron_->findPublication(pid);
            if (!sub_) sub_ = aeron_->findSubscription(sid);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_TRUE(pub_ && sub_) << "publication/subscription did not resolve";
        for (int i = 0; i < kMaxSpins && !pub_->isConnected(); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ASSERT_TRUE(pub_->isConnected());
    }

    void TearDown() override {
        pub_.reset(); sub_.reset(); aeron_.reset();
        driver_.reset();
        std::error_code ec;
        fs::remove_all(root_, ec);
    }

    void offerBoth(const std::string& big, const std::string& small) {
        for (const std::string* m : {&big, &small}) {
            AtomicBuffer b(reinterpret_cast<std::uint8_t*>(const_cast<char*>(m->data())), m->size());
            int i = 0;
            for (; i < kMaxSpins; ++i) {
                if (pub_->offer(b, 0, static_cast<aeron::util::index_t>(m->size())) > 0) break;
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            ASSERT_LT(i, kMaxSpins) << "offer never succeeded";
        }
    }

    fs::path root_;
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::shared_ptr<aeron::Publication> pub_;
    std::shared_ptr<aeron::Subscription> sub_;
};
} // namespace

TEST_F(FragmentAssemblerAeronRegression, RealAeron151MultiFragmentMessageIsReassembled) {
    const std::string big = pattern(kBig), small = pattern(kSmall);
    offerBoth(big, small);
    rune::aeronutil::FragmentAssembler assembler;
    std::vector<std::string> got;
    auto delegate = [&](AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, Header&) {
        got.emplace_back(reinterpret_cast<const char*>(b.buffer() + o), static_cast<std::size_t>(l));
    };
    auto handler = [&](AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, Header& h) {
        assembler.onFragment(delegate, b, o, l, h);
    };
    for (int i = 0; i < kMaxSpins && got.size() < 2; ++i) {
        if (sub_->poll(handler, 10) == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ASSERT_EQ(got.size(), 2u);
    EXPECT_TRUE(got[0] == big) << "60 000 B message must arrive byte-for-byte";
    EXPECT_TRUE(got[1] == small);
}

TEST_F(FragmentAssemblerAeronRegression, RealAeron151ControlledMultiFragmentMessageIsReassembled) {
    const std::string big = pattern(kBig), small = pattern(kSmall);
    offerBoth(big, small);
    rune::aeronutil::ControlledFragmentAssembler assembler;
    std::vector<std::string> got;
    auto delegate = [&](AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, Header&) {
        got.emplace_back(reinterpret_cast<const char*>(b.buffer() + o), static_cast<std::size_t>(l));
        return aeron::ControlledPollAction::CONTINUE;
    };
    auto handler = [&](AtomicBuffer& b, aeron::util::index_t o, aeron::util::index_t l, Header& h) {
        return assembler.onFragment(delegate, b, o, l, h);
    };
    for (int i = 0; i < kMaxSpins && got.size() < 2; ++i) {
        if (sub_->controlledPoll(handler, 10) == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ASSERT_EQ(got.size(), 2u);
    EXPECT_TRUE(got[0] == big) << "60 000 B message must arrive byte-for-byte";
    EXPECT_TRUE(got[1] == small);
}
