// FragmentAssemblerAeronRegressionTest.cpp — real-Aeron regression: Aeron 1.51's C++ wrapper hands every fragment
// handler a buffer over the payload with offset 0, so reassembly must judge contiguity by header.termOffset().
// An embedded C media driver carries a 60 000 B message over a 4 KiB MTU (many fragments) and a 10 B message.
#include <rune/aeronutil/FragmentAssembler.h>

#include <Aeron.h>
#include <Context.h>
#include <Publication.h>
#include <Subscription.h>
#include <aeronmd.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
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
        char tmpl[] = "/tmp/rune-fragasm-XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr);
        root_ = tmpl;
        const std::string dir = (fs::path(root_) / "aeron").string();
        ASSERT_EQ(aeron_driver_context_init(&ctx_), 0);
        aeron_driver_context_set_dir(ctx_, dir.c_str());
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        ASSERT_EQ(aeron_driver_init(&driver_, ctx_), 0) << aeron_errmsg();
        ASSERT_EQ(aeron_driver_start(driver_, true), 0) << aeron_errmsg();
        running_.store(true);
        thread_ = std::thread([this] {
            while (running_.load()) aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
        });
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
        running_.store(false);
        if (thread_.joinable()) thread_.join();
        if (driver_) { aeron_driver_close(driver_); driver_ = nullptr; }
        if (ctx_) { aeron_driver_context_close(ctx_); ctx_ = nullptr; }
        std::error_code ec;
        if (!root_.empty()) fs::remove_all(root_, ec);
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

    std::string root_;
    aeron_driver_context_t* ctx_ = nullptr;
    aeron_driver_t* driver_ = nullptr;
    std::atomic<bool> running_{false};
    std::thread thread_;
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
