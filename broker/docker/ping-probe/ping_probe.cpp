// ping_probe.cpp — raw-Aeron ping probe for BR-7b
//
// Boots an embedded Aeron media driver + client, connects to a rune-broker
// node's client gateway over aeron:udp, and runs:
//   ConnectRequest → ConnectResponse{OK} → Ping → Pong
//
// Environment variables:
//   RUNE_PROBE_TARGET        — node client ingress channel (required)
//                              e.g. "aeron:udp?endpoint=rune-1:40200"
//   RUNE_PROBE_TARGET_STREAM — int, default 1
//   RUNE_PROBE_EGRESS        — this probe's own egress channel (required)
//                              e.g. "aeron:udp?endpoint=probe:40300"
//   RUNE_PROBE_EGRESS_STREAM — int, default 2
//   RUNE_PROBE_TIMEOUT_MS    — int, hard deadline for entire probe, default 8000
//   RUNE_DATA_DIR            — driver dir, default /tmp/rune_probe_<pid>
//   RUNE_TLS_CA_FILE         — PEM CA bundle: run the session over TLS (issue #10: an authenticating broker serves only
//                              TLS sessions), checking the certificate against RUNE_PROBE_TARGET's host.  Unset =
//                              plaintext (a dev-mode broker).
//
// Exit 0 = Pong received. Non-zero = failure (reason printed to stderr).
//
// Design: single-writer poll loop, NO busy-spin (sleep between polls),
// every wait hard-capped at RUNE_PROBE_TIMEOUT_MS.

#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/CryptoEnvelope.h>

#include <rune/crypto/Aead.h>
#include <rune/crypto/Cipher.h>
#include <rune/crypto/TlsSession.h>

#include <Aeron.h>
#include <Publication.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

static std::string getEnvStr(const char* name, std::string defaultVal = {}) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : std::move(defaultVal);
}

static int getEnvInt(const char* name, int defaultVal) {
    const char* v = std::getenv(name);
    if (!v || !*v) return defaultVal;
    return std::atoi(v);
}

/// The host of "aeron:udp?endpoint=host:port[|...]" ("" when none).
static std::string endpointHost(const std::string& ch) {
    const auto ep = ch.find("endpoint=");
    if (ep == std::string::npos) return {};
    std::string hp = ch.substr(ep + 9);
    if (const auto end = hp.find_first_of("|&"); end != std::string::npos) hp = hp.substr(0, end);
    const auto colon = hp.rfind(':');
    return colon == std::string::npos ? hp : hp.substr(0, colon);
}

/// Every byte the TLS session has pending for the peer.
static std::string drainTls(rune::crypto::TlsSession& t) {
    std::string out;
    uint8_t chunk[4096];
    for (;;) {
        const std::size_t n = t.writeOutbound(chunk, sizeof chunk);
        if (n == 0) break;
        out.append(reinterpret_cast<const char*>(chunk), n);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Driver state
// ---------------------------------------------------------------------------
static aeron_driver_context_t* g_driverCtx = nullptr;
static aeron_driver_t*         g_driver     = nullptr;
static std::atomic<bool>       g_driverRunning{false};
static std::thread             g_driverThread;

static bool startDriver(const std::string& aeronDir) {
    fs::create_directories(aeronDir);

    if (aeron_driver_context_init(&g_driverCtx) < 0) {
        std::cerr << "PROBE: aeron_driver_context_init failed: " << aeron_errmsg() << "\n";
        return false;
    }
    aeron_driver_context_set_dir(g_driverCtx, aeronDir.c_str());
    aeron_driver_context_set_threading_mode(g_driverCtx, AERON_THREADING_MODE_SHARED);
    aeron_driver_context_set_dir_delete_on_start(g_driverCtx, true);
    aeron_driver_context_set_dir_delete_on_shutdown(g_driverCtx, true);
    aeron_driver_context_set_shared_idle_strategy(g_driverCtx, "sleep-ns");
    aeron_driver_context_set_term_buffer_sparse_file(g_driverCtx, true);
    aeron_driver_context_set_term_buffer_length(g_driverCtx, 64 * 1024);

    if (aeron_driver_init(&g_driver, g_driverCtx) < 0) {
        std::cerr << "PROBE: aeron_driver_init failed: " << aeron_errmsg() << "\n";
        aeron_driver_context_close(g_driverCtx);
        g_driverCtx = nullptr;
        return false;
    }
    if (aeron_driver_start(g_driver, /*manual_main_loop=*/true) < 0) {
        std::cerr << "PROBE: aeron_driver_start failed: " << aeron_errmsg() << "\n";
        aeron_driver_close(g_driver);
        aeron_driver_context_close(g_driverCtx);
        g_driver = nullptr;
        g_driverCtx = nullptr;
        return false;
    }

    g_driverRunning.store(true, std::memory_order_release);
    g_driverThread = std::thread([]() {
        while (g_driverRunning.load(std::memory_order_acquire)) {
            aeron_driver_main_idle_strategy(g_driver, aeron_driver_main_do_work(g_driver));
        }
    });

    // No caller-side pumping: aeron_driver_init/start already wrote cnc.dat and
    // stamped the heartbeat synchronously before g_driverThread even starts;
    // pumping here too would be a second concurrent pumper on the
    // single-pumper driver (spec Sec 3.6).
    const std::string cncPath = aeronDir + "/cnc.dat";
    if (!fs::exists(cncPath)) {
        std::cerr << "PROBE: Aeron CnC file not created within timeout\n";
        return false;
    }
    return true;
}

static void stopDriver() noexcept {
    if (g_driverRunning.exchange(false, std::memory_order_acq_rel)) {
        if (g_driverThread.joinable()) g_driverThread.join();
    }
    if (g_driver) {
        // Drain residual work.
        for (int i = 0; i < 250; ++i) {
            if (aeron_driver_main_do_work(g_driver) == 0) break;
            std::this_thread::sleep_for(200us);
        }
        aeron_driver_close(g_driver);
        g_driver = nullptr;
    }
    if (g_driverCtx) {
        aeron_driver_context_close(g_driverCtx);
        g_driverCtx = nullptr;
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main() {
    // --- Config from env ---
    const std::string target       = getEnvStr("RUNE_PROBE_TARGET");
    const int         targetStream = getEnvInt("RUNE_PROBE_TARGET_STREAM", 1);
    const std::string egress       = getEnvStr("RUNE_PROBE_EGRESS");
    const int         egressStream = getEnvInt("RUNE_PROBE_EGRESS_STREAM", 2);
    const int64_t     timeoutMs    = getEnvInt("RUNE_PROBE_TIMEOUT_MS", 8000);

    std::string dataDir = getEnvStr("RUNE_DATA_DIR");
    if (dataDir.empty()) {
        dataDir = std::string("/tmp/rune_probe_") + std::to_string(static_cast<long>(::getpid()));
    }
    const std::string aeronDir = dataDir + "/aeron";

    if (target.empty()) {
        std::cerr << "PROBE: RUNE_PROBE_TARGET is required\n";
        return 1;
    }
    if (egress.empty()) {
        std::cerr << "PROBE: RUNE_PROBE_EGRESS is required\n";
        return 1;
    }

    std::cerr << "PROBE: target=" << target << " stream=" << targetStream
              << " egress=" << egress << " egressStream=" << egressStream
              << " timeoutMs=" << timeoutMs << "\n";

    // --- Start driver ---
    if (!startDriver(aeronDir)) {
        return 2;
    }

    int rc = 1; // default fail

    try {
        // Connect Aeron client.
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        auto aeron = aeron::Aeron::connect(ctx);
        if (!aeron) {
            std::cerr << "PROBE: Aeron::connect failed\n";
            stopDriver();
            return 3;
        }

        // Add ingress Publication (to target node).
        int64_t pubRegId = aeron->addPublication(target, targetStream);
        // Add egress Subscription (node publishes back to us).
        int64_t subRegId = aeron->addSubscription(egress, egressStream);

        std::shared_ptr<aeron::Publication>  pub;
        std::shared_ptr<aeron::Subscription> sub;

        // Resolve pub+sub (bounded).
        const int64_t deadline0 = nowMs() + timeoutMs;
        while (nowMs() < deadline0) {
            if (!pub) pub = aeron->findPublication(pubRegId);
            if (!sub) sub = aeron->findSubscription(subRegId);
            if (pub && sub) break;
            std::this_thread::sleep_for(5ms);
        }
        if (!pub || !sub) {
            std::cerr << "PROBE: timed out resolving Publication/Subscription\n";
            aeron.reset();
            stopDriver();
            return 4;
        }

        // Wait for publication to connect to the node (bounded).
        while (nowMs() < deadline0 && !pub->isConnected()) {
            std::this_thread::sleep_for(5ms);
        }
        if (!pub->isConnected()) {
            std::cerr << "PROBE: timed out waiting for publication to connect to " << target << "\n";
            aeron.reset();
            stopDriver();
            return 5;
        }
        std::cerr << "PROBE: publication connected to " << target << "\n";

        // --- TLS (issue #10): the ClientHello rides the ConnectRequest ---
        const std::string caFile = getEnvStr("RUNE_TLS_CA_FILE");
        std::optional<rune::crypto::TlsSession> tls;
        std::string clientHello;
        uint8_t offeredCipher = static_cast<uint8_t>(rune::crypto::bestLocalCipher());
        if (!caFile.empty()) {
            std::ifstream caIn(caFile, std::ios::binary);
            const std::string caPem((std::istreambuf_iterator<char>(caIn)), std::istreambuf_iterator<char>());
            const std::string host = endpointHost(target);
            tls.emplace(rune::crypto::TlsSession::makeClient(caPem.c_str(), host.c_str()));
            if (caPem.empty() || !tls->valid()) {
                std::cerr << "PROBE: cannot build a TLS session (CA '" << caFile << "', host '" << host << "')\n";
                aeron.reset();
                stopDriver();
                return 13;
            }
            (void)tls->readInbound(nullptr, 0);
            clientHello = drainTls(*tls);
            std::cerr << "PROBE: TLS offered (host " << host << ")\n";
        }

        // --- Build ConnectRequest ---
        constexpr std::size_t kScratch = 4096;
        uint8_t scratch[kScratch] = {};

        pc::ConnectRequest req;
        req.sessionId      = 0;
        req.correlationId  = 1001;
        req.egressStreamId = egressStream;
        // Authentication (Task 20): a cluster with authentication on refuses a client older than version 9 at Connect.
        // The probe never logs in: Ping is a session-level frame the broker answers before authentication.
        req.clientVersion  = pc::kClientVersionAuth;
        req.egressChannel  = egress;
        if (tls) {
            req.cipherList     = std::span<const uint8_t>(&offeredCipher, 1);
            req.handshakeBytes = clientHello;
        }
        const std::size_t reqLen = req.encode(scratch);

        // Offer ConnectRequest (bounded).
        bool sent = false;
        while (nowMs() < deadline0 && !sent) {
            aeron::concurrent::AtomicBuffer ab(scratch, static_cast<aeron::util::index_t>(reqLen));
            int64_t pos = pub->offer(ab, 0, static_cast<aeron::util::index_t>(reqLen));
            if (pos > 0) {
                sent = true;
            } else {
                std::this_thread::sleep_for(2ms);
            }
        }
        if (!sent) {
            std::cerr << "PROBE: timed out offering ConnectRequest\n";
            aeron.reset();
            stopDriver();
            return 6;
        }
        std::cerr << "PROBE: ConnectRequest sent\n";

        // --- Await ConnectResponse ---
        std::optional<pc::ConnectResponse> connResp;
        std::string serverFlight;          // stable storage for the server's TLS flight

        aeron::FragmentAssembler connAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                            aeron::util::index_t offset,
                                            aeron::util::index_t len,
                                            aeron::Header&) {
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
            auto dec = pc::ConnectResponse::decode(p, static_cast<std::size_t>(len));
            if (dec) {
                serverFlight.assign(dec->handshakeBytes);   // the view dies with the fragment
                connResp = *dec;
                connResp->handshakeBytes = {};
            }
        });

        while (nowMs() < deadline0 && !connResp.has_value()) {
            sub->poll(connAsm.handler(), 10);
            std::this_thread::sleep_for(2ms);
        }
        if (!connResp.has_value()) {
            std::cerr << "PROBE: timed out waiting for ConnectResponse\n";
            aeron.reset();
            stopDriver();
            return 7;
        }

        const int16_t okCode = static_cast<int16_t>(pc::ErrorCode::OK);
        if (connResp->code != okCode) {
            std::cerr << "PROBE: ConnectResponse code=" << connResp->code << " (expected OK=" << okCode << ")\n";
            aeron.reset();
            stopDriver();
            return 8;
        }
        const int64_t sessionId = connResp->assignedSessionId;
        std::cerr << "PROBE: ConnectResponse OK sessionId=" << sessionId
                  << " leaderMemberId=" << connResp->leaderMemberId << "\n";

        // --- TLS: finish the handshake over HandshakeContinue legs, then derive the session keys ---
        std::optional<rune::crypto::AeadCipher> sendKey, recvKey;
        auto offerFrame = [&](const uint8_t* b, std::size_t n) {
            while (nowMs() < deadline0) {
                aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(b), static_cast<aeron::util::index_t>(n));
                if (pub->offer(ab, 0, static_cast<aeron::util::index_t>(n)) > 0) return true;
                std::this_thread::sleep_for(2ms);
            }
            return false;
        };
        if (tls) {
            if (connResp->chosenCipher == 0) {
                std::cerr << "PROBE: the broker answered the TLS offer in plaintext (no cipher)\n";
                aeron.reset();
                stopDriver();
                return 14;
            }
            const auto chosen = static_cast<rune::crypto::Cipher>(connResp->chosenCipher);
            std::string inbound = std::move(serverFlight);
            std::optional<pc::HandshakeContinueResponse> hcr;
            std::string hcrBytes;
            aeron::FragmentAssembler hcAsm([&](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t offset,
                                               aeron::util::index_t len, aeron::Header&) {
                const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
                if (auto dec = pc::HandshakeContinueResponse::decode(p, static_cast<std::size_t>(len))) {
                    hcrBytes.assign(dec->handshakeBytes);
                    hcr = *dec;
                    hcr->handshakeBytes = {};
                }
            });
            for (int leg = 0; leg < 8 && !tls->done(); ++leg) {
                if (tls->readInbound(reinterpret_cast<const uint8_t*>(inbound.data()), inbound.size()) ==
                    rune::crypto::HsState::Failed) {
                    std::cerr << "PROBE: TLS handshake failed (certificate not trusted or not for this host?)\n";
                    aeron.reset();
                    stopDriver();
                    return 15;
                }
                const std::string outBytes = drainTls(*tls);
                if (!outBytes.empty()) {
                    pc::HandshakeContinue hc;
                    hc.sessionId = sessionId;
                    hc.correlationId = req.correlationId;
                    hc.handshakeBytes = outBytes;
                    std::vector<uint8_t> f(hc.encodedSize());
                    if (!offerFrame(f.data(), hc.encode(f.data()))) break;
                }
                if (tls->done()) break;
                hcr.reset();
                while (nowMs() < deadline0 && !hcr) {
                    sub->poll(hcAsm.handler(), 10);
                    std::this_thread::sleep_for(2ms);
                }
                if (!hcr || hcr->code != 0) break;
                inbound = hcrBytes;
            }
            if (!tls->done()) {
                std::cerr << "PROBE: TLS handshake did not complete\n";
                aeron.reset();
                stopDriver();
                return 16;
            }
            const auto keys = tls->exportKeys(chosen, sessionId);
            if (keys.cipher == rune::crypto::Cipher::None) {
                std::cerr << "PROBE: TLS key export failed\n";
                aeron.reset();
                stopDriver();
                return 17;
            }
            sendKey.emplace(keys.cipher, keys.c2b, keys.c2bPrefix);
            recvKey.emplace(keys.cipher, keys.b2c, keys.b2cPrefix);
            offeredCipher = static_cast<uint8_t>(keys.cipher);
            std::cerr << "PROBE: TLS established\n";
        }

        // --- Send Ping ---
        const std::string echoStr = "rune-probe-echo";
        pc::Ping ping;
        ping.sessionId      = sessionId;
        ping.correlationId  = 2002;
        ping.echo           = echoStr;
        const std::size_t pingLen = ping.encode(scratch);
        std::vector<uint8_t> wire(scratch, scratch + pingLen);
        if (sendKey) {   // [24 B header][12 B CryptoSubHeader][sealed body][16 B tag], header + subheader the AAD
            wire.assign(pingLen + pc::kCryptoOverhead, 0);
            std::memcpy(wire.data(), scratch, pc::MessageHeader::ENCODED_SIZE);
            pc::CryptoSubHeader subHdr;
            subHdr.cipher = offeredCipher;
            subHdr.seq    = 1;
            subHdr.encode(wire.data() + pc::MessageHeader::ENCODED_SIZE);
            const std::size_t n = sendKey->seal(1, wire.data(), pc::kCryptoHeaderBytes,
                                                scratch + pc::MessageHeader::ENCODED_SIZE,
                                                pingLen - pc::MessageHeader::ENCODED_SIZE, wire.data() + pc::kCryptoHeaderBytes);
            if (n == 0) {
                std::cerr << "PROBE: sealing the Ping failed\n";
                aeron.reset();
                stopDriver();
                return 18;
            }
            wire.resize(pc::kCryptoHeaderBytes + n);
        }
        sent = offerFrame(wire.data(), wire.size());
        if (!sent) {
            std::cerr << "PROBE: timed out offering Ping\n";
            aeron.reset();
            stopDriver();
            return 9;
        }
        std::cerr << "PROBE: Ping sent\n";

        // --- Await Pong ---
        std::optional<pc::Pong> pong;
        std::string pongEcho; // stable storage

        aeron::FragmentAssembler pongAsm([&](aeron::concurrent::AtomicBuffer& buf,
                                            aeron::util::index_t offset,
                                            aeron::util::index_t len,
                                            aeron::Header&) {
            const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.buffer() + offset);
            std::size_t n = static_cast<std::size_t>(len);
            std::vector<uint8_t> plain;
            if (recvKey) {   // an encrypted session's frames open under its b2c key (the gateway is sender 0)
                const auto subHdr = pc::CryptoSubHeader::decode(p + pc::MessageHeader::ENCODED_SIZE,
                                                                n > pc::MessageHeader::ENCODED_SIZE ? n - pc::MessageHeader::ENCODED_SIZE : 0);
                if (!subHdr || n < pc::kCryptoHeaderBytes + 16) return;
                plain.resize(n);
                const long pl = recvKey->open(subHdr->seq, p, pc::kCryptoHeaderBytes, p + pc::kCryptoHeaderBytes,
                                              n - pc::kCryptoHeaderBytes, plain.data() + pc::MessageHeader::ENCODED_SIZE);
                if (pl < 0) return;
                std::memcpy(plain.data(), p, pc::MessageHeader::ENCODED_SIZE);
                p = plain.data();
                n = pc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(pl);
            }
            auto dec = pc::Pong::decode(p, n);
            if (dec) {
                pongEcho = std::string(dec->echo);
                dec->echo = pongEcho;
                pong = *dec;
            }
        });

        while (nowMs() < deadline0 && !pong.has_value()) {
            sub->poll(pongAsm.handler(), 10);
            std::this_thread::sleep_for(2ms);
        }

        if (!pong.has_value()) {
            std::cerr << "PROBE: timed out waiting for Pong\n";
            aeron.reset();
            stopDriver();
            return 10;
        }

        if (pongEcho != echoStr) {
            std::cerr << "PROBE: Pong echo mismatch: got='" << pongEcho
                      << "' want='" << echoStr << "'\n";
            aeron.reset();
            stopDriver();
            return 11;
        }

        // Assert correlationId matches what we sent (mirrors GatewayLoopbackTest.cpp).
        constexpr int64_t kExpectedCorrelationId = 2002;
        if (pong->correlationId != kExpectedCorrelationId) {
            std::cerr << "PROBE: Pong correlationId mismatch: got=" << pong->correlationId
                      << " want=" << kExpectedCorrelationId << "\n";
            aeron.reset();
            stopDriver();
            return 12;
        }

        std::cout << "PROBE: OK node=" << target << " session=" << sessionId << "\n";
        std::cerr << "PROBE: Pong received correlationId=" << pong->correlationId
                  << " echo='" << pongEcho << "'\n";
        rc = 0;

        aeron.reset();
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: exception: " << ex.what() << "\n";
        rc = 20;
    }

    stopDriver();
    return rc;
}
