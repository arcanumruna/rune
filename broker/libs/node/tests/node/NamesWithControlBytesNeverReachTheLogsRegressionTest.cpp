// #666: a client-supplied name with control bytes never reaches a broker log line raw (node level: the real admin
// worker and gateway behind one authenticating broker).  The gateway-only twin is AuthLogNamesGatewayTest.cpp.
#include "support/AuthBroker.h"

#include <gtest/gtest.h>

#include <spdlog/sinks/ringbuffer_sink.h>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;
using namespace std::chrono_literals;

namespace {

// A sink with room for every line of the run (the rig's LogCapture keeps 4096 at info level only).
struct TraceCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink =
        std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(32768);
    std::shared_ptr<spdlog::logger> previous = spdlog::default_logger();
    TraceCapture() {
        auto capture = std::make_shared<spdlog::logger>("names-log-capture", sink);
        capture->set_level(spdlog::level::trace);
        spdlog::set_default_logger(capture);
    }
    ~TraceCapture() { spdlog::set_default_logger(previous); }
};

const std::vector<std::string>& hostileNames() {
    static const std::vector<std::string> names = {
        "x\nauth: login ok user=admin", "a\rb", "a\x1b[31mb", std::string("a\0b", 3), "a\x7f", "a\x85" "b",
        "a\xE2\x80\xA8" "b"};
    return names;
}

void judge(const TraceCapture& cap, const std::string& what, int& linesJudged) {
    const std::string forged = "auth: login ok user=admin";
    for (std::string line : cap.sink->last_formatted()) {
        ++linesJudged;
        if (!line.empty() && line.back() == '\n') line.pop_back();   // the logger's own terminator
        for (std::size_t i = 0; i < line.size(); ++i) {
            const auto c = static_cast<unsigned char>(line[i]);
            EXPECT_FALSE(c < 0x20 || c == 0x7f) << what << ": raw control byte " << int(c) << " in: " << line;
        }
        EXPECT_NE(line.rfind(forged, 0), 0u) << what << ": a forged line: " << line;
        for (const char* raw : {"\x85", "\xE2\x80\xA8"})
            EXPECT_EQ(line.find(std::string("a") + raw + "b"), std::string::npos)
                << what << ": the raw name reached a log line: " << line;
    }
}

}  // namespace

// RED: log a client-supplied name with a plain `{}` at any site these frames reach.
TEST(AuthLogRegression, NamesWithControlBytesNeverReachABrokerLogLineRegression) {
    TraceCapture cap;
    int linesJudged = 0;
    {
        AuthBroker b("namelog");
        b.bootstrapAdmin();
        AuthWireClient& w = b.admin();
        const int32_t stream = AuthWireClient::kClientStream;
        for (const std::string& name : hostileNames()) {
            const std::string pw = "a password of some length";
            (void)w.createUser(b.channel(), b.freshAdminSid_(), name, pw);
            (void)w.grant(b.channel(), b.freshAdminSid_(), name, false, rune::protocol::SecurityOperation::Read,
                          rune::protocol::SecurityResourceType::Topic, rune::protocol::SecurityPatternKind::Exact, "t");
            (void)w.grant(b.channel(), b.freshAdminSid_(), "admin", false, rune::protocol::SecurityOperation::Read,
                          rune::protocol::SecurityResourceType::Topic, rune::protocol::SecurityPatternKind::Prefix,
                          name);
            (void)w.createRole(b.channel(), b.freshAdminSid_(), name);
            (void)w.grantRole(b.channel(), b.freshAdminSid_(), "admin", name);
            const auto deadline = [] { return std::chrono::steady_clock::now() + 3s; };
            auto answered = [&](int64_t corr) { (void)w.raw().awaitAnyDdlAnswer(corr, deadline()); };
            answered(w.raw().sendDefineTopic(b.channel(), stream, b.freshAdminSid_(), name, 1, {}));
            answered(w.raw().sendDropTopic(b.channel(), stream, b.freshAdminSid_(), name));
            answered(w.raw().sendAlterTopicConfig(b.channel(), stream, b.freshAdminSid_(), name, 1, {}));
        }
        judge(cap, "DDL frames", linesJudged);
    }
    // The bootstrap's admin name, one fresh (never bootstrapped) broker per name.
    for (const std::string& name : hostileNames()) {
        TraceCapture fresh;
        AuthBroker b("namelogboot");
        AuthWireClient& w = b.wire();
        const int64_t sid = w.connect(b.channel());
        ASSERT_GT(sid, 0);
        (void)w.bootstrap(b.channel(), sid, b.keyContents(), name, AuthBroker::kBootstrapPassword);
        judge(fresh, "SecurityBootstrap.adminName", linesJudged);
    }
    EXPECT_GT(linesJudged, 0) << "no log line was captured at all: the capture is not live";
}
