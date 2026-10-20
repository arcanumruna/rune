# Task 9 report: four-way benchmark gate + docs

## Implemented
- client-cpp/bench/ProducerClaimBench.cpp (verbatim from brief) + CMake target rune_producer_claim_bench.
- Docs: Producer.h (Usage lines; claimBatch/BatchClaim wording corrected: unfenced claimBatch can join an open RecordBatch,
  all n records contiguous, acked by the BatchAck(s) covering that RecordBatch; flush() (+doWork() Invoker) fences),
  client-cpp/CLAUDE.md section 2.1 (the claim bullet was absent; added with corrected wording), docs/api/client-cpp.md
  (new "Zero-copy claims" section; removed tryClaim/claimBatch from the "Not yet in this tree" list).

## Build
build/bench-rel, Release, Ninja. Conan cannot produce an armv8 aeron (recipe refuses on macOS/armv8, which would drop
aeron_FOUND and all benches), and the existing toolchain build/conan-rel is x86_64 (the project's Mac builds are all
x86_64 under Rosetta, CMAKE_OSX_ARCHITECTURES=x86_64), so I reused it. Same toolchain/flags (-O3 -DNDEBUG -m64 -stdlib=libc++)
for base and HEAD. Timer granularity under Rosetta is ~41.7 ns, so p50/p99 are multiples of it.

## 8 RESULT lines (gate: allocs=0 on both claim forms, both ingress modes: PASS, no GATE FAILED)
```
RESULT mode=send ingress=thread records=1000000 p50_ns=42 p99_ns=166 allocs=0 frames=60903
RESULT mode=claim ingress=thread records=1000000 p50_ns=125 p99_ns=208 allocs=0 frames=573147
RESULT mode=typed_send ingress=thread records=1000000 p50_ns=125 p99_ns=209 allocs=0 frames=658923
RESULT mode=typed_claim ingress=thread records=1000000 p50_ns=125 p99_ns=250 allocs=0 frames=472229
RESULT mode=send ingress=invoker records=1000000 p50_ns=83 p99_ns=125 allocs=0 frames=125000
RESULT mode=claim ingress=invoker records=1000000 p50_ns=83 p99_ns=125 allocs=0 frames=125000
RESULT mode=typed_send ingress=invoker records=1000000 p50_ns=83 p99_ns=125 allocs=0 frames=125000
RESULT mode=typed_claim ingress=invoker records=1000000 p50_ns=84 p99_ns=125 allocs=0 frames=125000
```
Expectation (not a gate) claim <= send / typed_claim < typed_send is NOT met in Thread mode: send p50 42 ns vs claim 125 ns,
typed_send 125 vs typed_claim 125 ns (equal). Invoker mode: all four equal (83-84 ns, timer-quantized). Single run each;
not investigated.

## Hot-path regression check: plain send path, base e770604f vs HEAD
bench/ProducerAccumBench (ACCUM_RECORDS=5M, 256 B, 8 partitions, acks=none), ABAB n=3, same env, x86_64 Release.
| run | base rec/s | HEAD rec/s |
|---|---|---|
| 1 | 14,353,023 | 13,551,437 |
| 2 | 14,339,785 | 13,584,840 |
| 3 | 14,318,508 | 13,567,490 |
| median | 14,339,785 (69.7 ns/rec) | 13,567,490 (73.7 ns/rec) |
| noise band | 14.319-14.353M (0.24%) | 13.551-13.585M (0.25%) |
Delta: -5.4% throughput (+4.0 ns/record), ~20x the noise band, clearly outside noise. Frames/records-per-batch identical
(20840 / 239.9). NOT fixed (per ruling); the suspects are Task 3-5 changes (writePrefix_/admitRecord_/appendBatch_/noteAppended_
split and the CAS in M2ORingBuffer::commit). Base tree and its build deleted.

## Tests
- ctest build/sub-client-cpp -R "NoNewSyncOrSleeps|Claim": 73/73 passed (incl. ProducerClaimAllocRegression, TypedClaim*).
- check_no_atomics_or_mutexes.py --tests: OK.
- Full/Regression/sanitizer suites: deferred to controller. No TDD RED step (bench + docs task).

## Concerns
- Plain send path -5.4% vs base (above). Claim not faster than send in Thread mode on this box.
- x86_64 under Rosetta, not native armv8 (aeron unavailable on armv8).
