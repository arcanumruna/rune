// #186: no secret reaches the logs.  One real authenticating BrokerNode is driven through the whole credential
// lifecycle (key load, bootstrap, CREATE USER, ALTER USER SET / ADD PASSWORD, logins, throttling, a refused statement
// carrying a literal, a restart) while a trace-level sink sits on EVERY logger; the captured text must hold none of
// the secrets in any of the common encodings.
#include "support/AuthBroker.h"

#include <rune/crypto/Kdf.h>
#include <rune/crypto/Scram.h>

#include <gtest/gtest.h>

#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <fstream>
#include <cstring>
#include <mutex>
#include <set>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;

namespace {

// A sink that keeps every formatted line, with no capacity limit.
class CollectSink final : public spdlog::sinks::base_sink<std::mutex> {
public:
    [[nodiscard]] std::string text() {
        std::lock_guard<std::mutex> g(mutex_);
        return text_;
    }
protected:
    void sink_it_(const spdlog::details::log_msg& m) override {
        spdlog::memory_buf_t buf;
        formatter_->format(m, buf);
        text_.append(buf.data(), buf.size());
    }
    void flush_() override {}
private:
    std::string text_;
};

// RAII: the sink on the default logger and on every named logger, all at trace; the previous state is restored.
class AllLoggersCapture {
public:
    AllLoggersCapture() : sink_(std::make_shared<CollectSink>()), previous_(spdlog::default_logger()) {
        auto d = std::make_shared<spdlog::logger>("secrets-log-capture", sink_);
        d->set_level(spdlog::level::trace);
        spdlog::set_default_logger(d);
        attach();
    }
    ~AllLoggersCapture() {
        for (auto& [lg, level] : touched_) {
            auto& sinks = lg->sinks();
            sinks.erase(std::remove(sinks.begin(), sinks.end(), sink_), sinks.end());
            lg->set_level(level);
        }
        spdlog::set_default_logger(previous_);
    }
    /// Named loggers created since the last call get the sink too.
    void attach() {
        spdlog::apply_all([this](std::shared_ptr<spdlog::logger> lg) {
            if (lg->name() == "secrets-log-capture" || seen_.count(lg.get()) != 0) return;
            seen_.insert(lg.get());
            touched_.emplace_back(lg, lg->level());
            lg->sinks().push_back(sink_);
            lg->set_level(spdlog::level::trace);
        });
    }
    [[nodiscard]] std::string text() { return sink_->text(); }
private:
    std::shared_ptr<CollectSink> sink_;
    std::shared_ptr<spdlog::logger> previous_;
    std::set<spdlog::logger*> seen_;
    std::vector<std::pair<std::shared_ptr<spdlog::logger>, spdlog::level::level_enum>> touched_;
};

std::string hexOf(const std::string& s, bool upper) {
    static const char* lo = "0123456789abcdef";
    static const char* up = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : s) { out += (upper ? up : lo)[c >> 4]; out += (upper ? up : lo)[c & 15]; }
    return out;
}

std::string base64Of(const std::string& s, bool urlSafe, bool pad) {
    const char* tbl = urlSafe ? "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
                              : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    std::size_t i = 0;
    for (; i + 2 < s.size(); i += 3) {
        const uint32_t v = (uint8_t(s[i]) << 16) | (uint8_t(s[i + 1]) << 8) | uint8_t(s[i + 2]);
        out += tbl[v >> 18]; out += tbl[(v >> 12) & 63]; out += tbl[(v >> 6) & 63]; out += tbl[v & 63];
    }
    if (const std::size_t rem = s.size() - i; rem > 0) {
        uint32_t v = uint8_t(s[i]) << 16;
        if (rem == 2) v |= uint8_t(s[i + 1]) << 8;
        out += tbl[v >> 18]; out += tbl[(v >> 12) & 63];
        if (rem == 2) out += tbl[(v >> 6) & 63];
        if (pad) out.append(rem == 2 ? "=" : "==");
    }
    return out;
}

/// Every form a secret could take in a log line.
std::vector<std::pair<std::string, std::string>> forms(const std::string& secret) {
    return {{"raw", secret},
            {"hex", hexOf(secret, false)},
            {"HEX", hexOf(secret, true)},
            {"base64", base64Of(secret, false, true)},
            {"base64 unpadded", base64Of(secret, false, false)},
            {"base64url", base64Of(secret, true, true)},
            {"base64url unpadded", base64Of(secret, true, false)}};
}

std::string bytesOf(const std::array<uint8_t, 64>& a) { return std::string(a.begin(), a.end()); }
std::string bytesOf(const std::vector<uint8_t>& a) { return std::string(a.begin(), a.end()); }
std::string bytesOf(std::string_view a) { return std::string(a); }
std::vector<uint8_t> spanOf(std::string_view a) { return std::vector<uint8_t>(a.begin(), a.end()); }

constexpr const char* kPwCreate  = "Zq7!vN2$kRw9#Lp4xT-create";
constexpr const char* kPwSet     = "Hd3@mB8&yUe5^Jc1sF-setpw";
constexpr const char* kPwAdd     = "Wt6*gP4%aKx2(Qn9rV-addpw";
constexpr const char* kPwAdmin   = "Mv8)cE5+zYb3_Ru7hG-admin";
constexpr const char* kLiteral   = "Lk9~dS2=oXf6!Ty4jN-literal";
constexpr const char* kWrongPw   = "Pa5#wC1^eZi8$Vh3uB-wrong1";
constexpr const char* kKeyA      = "rT4vYq9LmZ2xKd7BnW5cHs8JfP3gAe6UoQ1iXz0VtN";
constexpr const char* kKeyB      = "Gs8dJk3WnP6yLc1RbT9mFx5ZaQ2vHe7UoD4iYh0NuC";

}  // namespace

// RED: add a log line that prints any one of the secrets below (e.g. spdlog::info("{}", kPwCreate) in the
// admin worker).
TEST(SecretsNeverReachTheLogsRegression, NoSecretByteStringAppearsOverTheAuthLifecycleRegression) {
    AllLoggersCapture capture;
    const std::filesystem::path nextKey = std::filesystem::temp_directory_path() /
                                          ("rune_secretslog_next_" + std::to_string(::getpid()) + ".key");
    { std::ofstream f(nextKey, std::ios::binary); f << kKeyB << "\n"; }
    std::filesystem::permissions(nextKey, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);

    std::vector<std::pair<std::string, std::string>> secrets;   // what, bytes
    auto add = [&](const std::string& what, const std::string& bytes) { secrets.emplace_back(what, bytes); };
    add("password create", kPwCreate); add("password set", kPwSet); add("password add", kPwAdd);
    add("password admin", kPwAdmin);   add("literal in a refused statement", kLiteral);
    add("wrong-guess password", kWrongPw);
    add("one-time password", AuthBroker::kBootstrapPassword);
    add("cluster key file text", kKeyA); add("next cluster key file text", kKeyB);
    for (const char* k : {kKeyA, kKeyB}) {
        const auto m = rune::crypto::deriveClusterKey(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k), std::strlen(k)));
        ASSERT_TRUE(m.has_value());
        add(std::string("derived master key of ") + k, bytesOf(m->master));
    }

    AuthBroker::Options o;
    o.keyContents = std::string(kKeyA) + "\n";
    o.configure = [&](NodeConfig& c) { c.clusterKeyFileNext = nextKey.string(); };
    {
        AuthBroker b("secretslog", o);
        capture.attach();                                   // loggers the broker created while booting
        b.bootstrapAdmin(kPwAdmin);                         // bootstrap, one-time login, SET PASSWORD
        const AuthWireClient::Verifier created = AuthWireClient::verifierOf(kPwCreate);
        auto a = b.admin().createUserWithVerifier(b.channel(), b.adminSession(), "u1", created);
        ASSERT_EQ(a.errorCode, 0) << a.detail;
        add("verifier salt (create)", bytesOf(created.salt));
        add("verifier StoredKey (create)", bytesOf(created.stored));
        add("verifier ServerKey (create)", bytesOf(created.server));
        b.addPassword("u1", kPwAdd);
        {
            auto r = b.admin().alterPassword(b.channel(), b.adminSession(), "u1", pc::UserChangeKind::SetPassword,
                                             kPwSet);
            ASSERT_EQ(r.errorCode, 0) << r.detail;
        }

        // Logins: success (with the proofs and signatures), wrong password, then enough failures to throttle.
        AuthWireClient& w = b.wire();
        const auto ok = w.connectAndLogin(b.channel(), "u1", kPwSet);
        ASSERT_TRUE(ok.ok());
        ASSERT_TRUE(ok.first.has_value());
        // every listed salt, with the verifier derived from each password
        for (uint8_t i = 0; i < ok.first->count; ++i) {
            add("salt on the wire", bytesOf(ok.first->salts[i].salt));
            for (const char* pw : {kPwCreate, kPwAdd, kPwSet}) {
                const std::vector<uint8_t> salt = spanOf(ok.first->salts[i].salt);
                const auto sp = rune::crypto::scram::saltPassword(rune::crypto::Digest::Sha512, pw, salt,
                                                                  ok.first->salts[i].iterations);
                ASSERT_TRUE(sp.has_value());
                const auto v = rune::crypto::scram::makeVerifier(rune::crypto::Digest::Sha512, *sp, salt,
                                                                 ok.first->salts[i].iterations);
                ASSERT_TRUE(v.has_value());
                add("derived StoredKey", std::string(v->storedKey.bytes.begin(), v->storedKey.bytes.end()));
                add("derived ServerKey", std::string(v->serverKey.bytes.begin(), v->serverKey.bytes.end()));
                add("salted password", std::string(sp->bytes.bytes.begin(), sp->bytes.bytes.end()));
            }
        }
        ASSERT_TRUE(ok.answer.has_value());
        for (uint8_t i = 0; i < ok.answer->final.count; ++i) {
            add("client proof", std::string(ok.answer->final.proofs[i].begin(), ok.answer->final.proofs[i].end()));
            add("expected server signature",
                std::string(ok.answer->expectedSignature[i].begin(), ok.answer->expectedSignature[i].end()));
        }
        add("server signature", std::string(ok.fin->serverSignature.begin(), ok.fin->serverSignature.end()));
        for (int i = 0; i < 6; ++i) {                        // wrong guesses: verified failures, then throttled
            const auto bad = w.connectAndLogin(b.channel(), "u1", kWrongPw);
            EXPECT_FALSE(bad.ok());
        }
        // A refused statement carrying a password literal (the guard refuses; the literal must never be logged
        // either way).
        pc::SecurityDdl m;
        m.sessionId = b.adminSession();
        m.kind = pc::SecurityOpKind::CreateUser;
        m.name = "u2";
        const AuthWireClient::Verifier v2 = AuthWireClient::verifierOf(kPwCreate);
        m.verifier = v2.wire();
        (void)b.admin().securityDdl(b.channel(), m, std::string("CREATE USER u2 PASSWORD '") + kLiteral + "';");
        b.restart(false);                                      // the boot replay of the security log
        capture.attach();
    }
    std::filesystem::remove(nextKey);

    const std::string log = capture.text();
    ASSERT_FALSE(log.empty()) << "the capture saw nothing: the sink is not live";
    EXPECT_NE(log.find("security"), std::string::npos) << "sanity: the lifecycle logged at least a security line";
    for (const auto& [what, bytes] : secrets) {
        ASSERT_FALSE(bytes.empty()) << what;
        for (const auto& [form, text] : forms(bytes))
            EXPECT_EQ(log.find(text), std::string::npos) << what << " appears in the log as " << form;
    }
}
