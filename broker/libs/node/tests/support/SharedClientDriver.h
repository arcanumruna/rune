#pragma once
// One media driver for all the clients of a test (and its CLI runs), apart from the brokers' own: a client otherwise starts an
// embedded driver of its own (100-180 ms, three 128 MiB command queues zeroed).  Never a broker's node driver: crash() would kill it.
#include <rune/client/Client.h>
#include <rune/node/EmbeddedDriver.h>
#include <unistd.h>
#include <atomic>
#include <filesystem>
#include <string>

namespace rune::node::test {

class SharedClientDriver {
public:
    SharedClientDriver()
        : scratch_(std::filesystem::temp_directory_path() / ("rune_clientdrv_" + std::to_string(::getpid()) + "_" + std::to_string(seq_()++))),
          driver_(config_(scratch_.dir / "aeron"), DriverMode::Threaded) {}
    SharedClientDriver(const SharedClientDriver&)            = delete;
    SharedClientDriver& operator=(const SharedClientDriver&) = delete;

    /// The aeron dir to hand `--cli-aeron-dir`, so a `rune::cli::run` attaches to this driver as well.
    [[nodiscard]] const std::string& dir() const { return driver_.dir(); }

    /// `cfg` attached to this driver: the caller's clients must be destroyed before it.
    [[nodiscard]] rune::client::ClientConfig attach(rune::client::ClientConfig cfg) const {
        cfg.aeronDir            = driver_.dir();
        cfg.driverCountersBytes = 0;   // sizes a driver of the client's own, which this one has none of
        return cfg;
    }

private:
    static EmbeddedDriverConfig config_(const std::filesystem::path& dir) {
        EmbeddedDriverConfig c;
        c.dir                 = dir.string();
        c.termBufferBytes     = 4 * 1024 * 1024;
        c.countersValuesBytes = kTestClientCountersBytes;   // a client's few counters: a small CnC
        c.statusMessageTimeoutNs = 20'000'000;              // a client's egress subscription connects within 20 ms, not Aeron's 200
        return c;
    }
    static std::atomic<uint64_t>& seq_() { static std::atomic<uint64_t> s{0}; return s; }

    struct Scratch {   // removed after the driver, which deletes its own dir at close
        std::filesystem::path dir;
        explicit Scratch(std::filesystem::path d) : dir(std::move(d)) { std::filesystem::create_directories(dir); }
        ~Scratch() { std::error_code ec; std::filesystem::remove_all(dir, ec); }
    };
    Scratch        scratch_;
    EmbeddedDriver driver_;
};

}  // namespace rune::node::test
