// SCRAM benchmarks: the login cost the D7 iteration default is sized by.
#include "rune/crypto/Kdf.h"
#include "rune/crypto/Scram.h"
#include <benchmark/benchmark.h>
#include <string>
#include <vector>

namespace rc = rune::crypto;
namespace rs = rune::crypto::scram;

static const std::vector<uint8_t> kSalt(16, 0x5a);
static const char* kPw = "correct horse battery";

static void BM_ScramClientSaltPassword(benchmark::State& st) {
    for (auto _ : st) {
        auto sp = rs::saltPassword(rc::Digest::Sha512, kPw, kSalt, rs::kDefaultIterations);
        benchmark::DoNotOptimize(sp);
    }
}
BENCHMARK(BM_ScramClientSaltPassword)->Unit(benchmark::kMicrosecond);

static void BM_ScramServerVerify(benchmark::State& st) {
    auto sp = rs::saltPassword(rc::Digest::Sha512, kPw, kSalt, rs::kDefaultIterations);
    auto v = rs::makeVerifier(rc::Digest::Sha512, *sp, kSalt, rs::kDefaultIterations);
    const std::string am = "n=u,r=c,r=cs,s=x,i=4096,c=biws,r=cs";
    auto proof = rs::clientProof(rc::Digest::Sha512, *sp, am);
    for (auto _ : st) {
        bool ok = rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, am, proof->view());
        auto sig = rs::serverSignature(rc::Digest::Sha512, v->serverKey, am);
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(sig);
    }
}
BENCHMARK(BM_ScramServerVerify)->Unit(benchmark::kMicrosecond);

// D5 rotation worst case: two active verifiers -> client salts twice, proves twice, server verifies once.
static void BM_ScramFullExchangeTwoVerifiers(benchmark::State& st) {
    std::vector<uint8_t> salt2(16, 0x3c);
    auto spv = rs::saltPassword(rc::Digest::Sha512, kPw, salt2, rs::kDefaultIterations);
    auto v = rs::makeVerifier(rc::Digest::Sha512, *spv, salt2, rs::kDefaultIterations);
    const std::string am = "n=u,r=c,r=cs,s=x,i=4096,c=biws,r=cs";
    for (auto _ : st) {
        auto sp1 = rs::saltPassword(rc::Digest::Sha512, kPw, kSalt, rs::kDefaultIterations);
        auto sp2 = rs::saltPassword(rc::Digest::Sha512, kPw, salt2, rs::kDefaultIterations);
        auto p1 = rs::clientProof(rc::Digest::Sha512, *sp1, am);
        auto p2 = rs::clientProof(rc::Digest::Sha512, *sp2, am);
        bool ok = rs::verifyClientProof(rc::Digest::Sha512, v->storedKey, am, p2->view());
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(p1);
    }
}
BENCHMARK(BM_ScramFullExchangeTwoVerifiers)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
