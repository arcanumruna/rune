// AeadBench.cpp — AEAD seal/open microbenchmark + CI/on-box latency gate.
//
// Measures the per-frame added-latency proxy for the client<->broker TLS/AEAD
// path: one seal() (sender) + one open() (receiver) round trip, for each
// {cipher} x {frame size}. No gtest, no external bench lib, no allocation in
// the measured loop — all buffers are preallocated once.
//
// Gate: RUNE_CRYPTO_BENCH_BUDGET_NS (ns) bounds the seal+open ns/op of
// AES-256-GCM at 4096 bytes, the representative frame size for this feature.
// Unset -> default ceiling of 5000ns (5us). Exceeding it (or any cipher op
// failing) makes main() return 1, so this binary doubles as a CI gate.
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include "rune/crypto/Aead.h"

using namespace rune::crypto;
using Clock = std::chrono::steady_clock;

namespace {

constexpr size_t kFrameSizes[] = {64, 256, 1024, 4096, 16384};
constexpr Cipher kCiphers[] = {Cipher::Aes256Gcm, Cipher::ChaCha20Poly1305};

const char* cipherName(Cipher c) {
    switch (c) {
        case Cipher::Aes256Gcm: return "aes256gcm";
        case Cipher::ChaCha20Poly1305: return "chacha20poly1305";
        default: return "none";
    }
}

AeadKey fixedKey() {
    AeadKey k{};
    for (int i = 0; i < 32; ++i) k.bytes[i] = uint8_t(0xA5 ^ i);
    return k;
}

// Client<->broker envelope clear header: 24B MessageHeader + 12B CryptoSubHeader.
constexpr size_t kAadLen = 36;

constexpr size_t kMaxFrame = 16384;
constexpr size_t kMaxCt = kMaxFrame + kTagSize;

constexpr uint64_t kWarmupIters = 10000;
constexpr uint64_t kCalibIters = 20000;
constexpr uint64_t kMinIters = 200000;
constexpr double kMinSeconds = 0.2;

struct Result {
    Cipher cipher;
    size_t frameSize;
    double nsPerOp;   // seal+open, one round trip
    double mbPerSec;
    uint64_t checksum;
};

// Preallocated once, reused across every (cipher,size) run. Sized for the
// largest frame so no run needs to grow anything.
uint8_t g_plain[kMaxFrame];
uint8_t g_aad[kAadLen];
uint8_t g_ct[kMaxCt];
uint8_t g_opened[kMaxFrame];

[[noreturn]] void fatal(const char* what, Cipher cipher, size_t frameSize, uint64_t iter) {
    std::fprintf(stderr,
                 "FATAL: %s failed (cipher=%s frame=%zuB iter=%llu) — a failed cipher op "
                 "must never be counted as a fast success\n",
                 what, cipherName(cipher), frameSize, (unsigned long long)iter);
    std::exit(1);
}

Result benchOne(Cipher cipher, size_t frameSize) {
    const AeadKey key = fixedKey();
    const uint8_t prefix[kNoncePrefix] = {0x11, 0x22, 0x33, 0x44};
    AeadCipher send(cipher, key, prefix);
    AeadCipher recv(cipher, key, prefix);

    for (size_t i = 0; i < kMaxFrame; ++i) g_plain[i] = uint8_t(i * 2654435761u);
    for (size_t i = 0; i < kAadLen; ++i) g_aad[i] = uint8_t(0x5A + i);

    uint64_t seq = 1;
    uint64_t checksum = 0;

    // The hot loop: zero allocation, every buffer preallocated above. seal()
    // and open() are compiled in a separate TU (the rune_crypto static lib),
    // so the optimizer cannot see through the call and elide it; branching on
    // the real return value both verifies correctness and pins the work.
    auto roundTrip = [&](uint64_t iterForError) {
        size_t n = send.seal(seq, g_aad, kAadLen, g_plain, frameSize, g_ct);
        if (n != frameSize + kTagSize) fatal("seal", cipher, frameSize, iterForError);
        long m = recv.open(seq, g_aad, kAadLen, g_ct, n, g_opened);
        if (m != (long)frameSize) fatal("open", cipher, frameSize, iterForError);
        checksum += g_ct[n - 1];
        checksum ^= uint64_t(g_opened[frameSize - 1]) << (iterForError & 7);
        ++seq;
    };

    for (uint64_t i = 0; i < kWarmupIters; ++i) roundTrip(i);

    // Calibrate: measure a small batch to estimate ns/op, then pick a fixed
    // iteration count for the timed loop (>= kMinIters and >= kMinSeconds).
    // The timed loop itself does no clock reads or size arithmetic — only
    // roundTrip() — so calibration overhead never pollutes the measurement.
    auto tc0 = Clock::now();
    for (uint64_t i = 0; i < kCalibIters; ++i) roundTrip(kWarmupIters + i);
    auto tc1 = Clock::now();
    double calibNs = std::chrono::duration<double, std::nano>(tc1 - tc0).count();
    double estNsPerOp = calibNs / double(kCalibIters);

    uint64_t iters = kMinIters;
    double neededForTime = (kMinSeconds * 1e9) / estNsPerOp;
    if (neededForTime > double(iters)) iters = uint64_t(neededForTime) + 1;

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < iters; ++i) roundTrip(kWarmupIters + kCalibIters + i);
    auto t1 = Clock::now();

    double totalNs = std::chrono::duration<double, std::nano>(t1 - t0).count();
    double nsPerOp = totalNs / double(iters);
    double bytesMoved = double(frameSize) * 2.0 * double(iters); // seal + open each touch frameSize bytes
    double mbPerSec = (bytesMoved / (totalNs / 1e9)) / (1024.0 * 1024.0);

    return Result{cipher, frameSize, nsPerOp, mbPerSec, checksum};
}

}  // namespace

int main() {
    std::printf("%-18s %9s %16s %18s\n", "cipher", "frame(B)", "seal+open(ns)", "throughput(MB/s)");
    std::printf("%-18s %9s %16s %18s\n", "------", "--------", "-------------", "-----------------");

    Result aes4096{};
    bool haveAes4096 = false;
    uint64_t grandChecksum = 0;

    for (Cipher cipher : kCiphers) {
        for (size_t frameSize : kFrameSizes) {
            Result r = benchOne(cipher, frameSize);
            std::printf("%-18s %9zu %16.1f %18.1f\n", cipherName(cipher), r.frameSize, r.nsPerOp,
                        r.mbPerSec);
            grandChecksum ^= r.checksum;
            if (cipher == Cipher::Aes256Gcm && frameSize == 4096) {
                aes4096 = r;
                haveAes4096 = true;
            }
        }
    }

    // Printed (not just computed) so the compiler can't prove the measured
    // loops' outputs are unobserved and fold them away.
    std::printf("checksum=0x%016llx\n", (unsigned long long)grandChecksum);

    if (!haveAes4096) {
        std::fprintf(stderr, "FATAL: AES-256-GCM 4096B measurement missing\n");
        return 1;
    }

    long budgetNs = 5000;  // default ceiling: 5us, active-by-default gate
    if (const char* env = std::getenv("RUNE_CRYPTO_BENCH_BUDGET_NS")) {
        char* end = nullptr;
        long v = std::strtol(env, &end, 10);
        if (end != env && v > 0) budgetNs = v;
    }

    if (aes4096.nsPerOp > double(budgetNs)) {
        std::printf("GATE FAIL: aes256gcm 4096B seal+open %.1fns > budget %ldns\n", aes4096.nsPerOp,
                    budgetNs);
        return 1;
    }
    std::printf("GATE PASS: aes256gcm 4096B seal+open %.1fns <= budget %ldns\n", aes4096.nsPerOp,
                budgetNs);
    return 0;
}
