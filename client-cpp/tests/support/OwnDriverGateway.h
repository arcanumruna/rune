// tests/support/OwnDriverGateway.h — a fake broker on its OWN media driver.
//
// InvokerGateway normally attaches to the CLIENT's driver dir; a transport rebuild deletes and recreates that driver
// and would take the broker with it.  BrokerHost runs the gateway on a separate driver (a Thread-mode transport on
// aeron:ipc: its driver thread needs no pumping, so the test thread only invokes the gateway's conductor) and the client
// reaches it over UDP, as in ShardSigstopDrGateTest.
#pragma once

#include "InvokerGateway.h"

#include <rune/client/transport/AeronTransport.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <unistd.h>

namespace test_support {

struct BrokerHost {
    struct DirGuard {  // first member: removed after the driver closed
        std::string path;
        ~DirGuard() {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    } dir;
    rune::client::transport::AeronTransport host;
    InvokerGateway                          gw;  // destroyed first: its Aeron client goes before the driver

    static std::string uniqueDir() {
        static std::atomic<int> n{0};
        return std::filesystem::temp_directory_path().string() + "/own_broker_" + std::to_string(::getpid()) + "_" +
               std::to_string(n.fetch_add(1));
    }
    static rune::client::transport::TransportConfig hostConfig(const std::string& dir) {
        rune::client::transport::TransportConfig c;
        c.ingressChannel  = "aeron:ipc";
        c.ingressStreamId = 1;
        c.egressChannel   = "aeron:ipc";
        c.egressStreamId  = 2;
        c.dataDir         = dir;
        c.driverMode      = rune::client::transport::DriverMode::Thread;
        return c;
    }
    static std::string startedAeronDir(rune::client::transport::AeronTransport& t, const std::string& dir) {
        t.start();
        return dir + "/aeron";
    }

    BrokerHost(const std::string& dir, const std::string& gatewayChannel, int32_t gatewayStream,
               const std::string& egressChannel, int32_t egressStream)
        : dir{dir}, host(hostConfig(dir)),
          gw(startedAeronDir(host, dir), gatewayChannel, gatewayStream, egressChannel, egressStream) {
        gw.replyOnAnnouncedEgress = true;
        gw.uniqueSessionIds       = true;
    }
};

} // namespace test_support
