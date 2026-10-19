# Rune Archive Feasibility Spike — 2026-09-11

**Task:** Determine whether `aeron-archive` can run embedded in this
environment the way the media driver did in P2-8.

**Decision: DEFERRED — file-backed store is the P3-4 deliverable.**

---

## What is available in the conan package

The `aeron/1.41.4` conan package ships:

| Component | File |
|---|---|
| Archive C++ client headers | `include/aeron-archive/...` (AeronArchive.h, ArchiveProxy.h, etc.) |
| Archive C++ client library | `lib/libaeron_archive_client.a` |
| Archive C++ wrapper | `lib/libaeron_archive_client_wrapper.a` |
| aeron-all JAR | `<build-dir>/aeron-all-1.41.4.jar` (in the build cache) |

The `aeron::aeron` CMake target already links `libaeron_archive_client.a`,
so no additional CMake plumbing is needed to use the C++ client headers.

## Why embedded archive is NOT feasible here

The Aeron `ArchivingMediaDriver` is a **Java process**, not a C++ library.
The C++ archive client (`AeronArchive`) is a **client** that connects to a
separately-running `io.aeron.archive.ArchivingMediaDriver` JVM process.

Evidence:

- `TestArchive.h` (in the aeron source tree at
  `aeron-archive/src/test/cpp/TestArchive.h`) spawns the archive via
  `posix_spawn("java", ..., "io.aeron.archive.ArchivingMediaDriver", ...)`.
  There is no C++ ArchivingMediaDriver.
- `java -version` on this machine returns:
  _"The operation couldn't be completed. Unable to locate a Java Runtime."_
- Even if Java were present, starting an ArchivingMediaDriver requires a
  JAR (`aeron-all-1.41.4.jar`) and a live Aeron media driver alongside it.

This is fundamentally different from P2-8's embedded C++ media driver
(`EmbeddedMediaDriver`), which IS a C++ class with no Java dependency.

## Decision

The `ArchiveRecordingStore` / live wiring is **deferred**.

**P3-4 deliverable = file-backed `FileRecordingStore`** which provides full
recording + replay semantics without Java, meeting the plan's explicit
acceptable-outcome clause:

> "If NOT feasible: document the deferral … keeping the file-backed store as
> the Phase-3 deliverable (this is an ACCEPTABLE outcome per the plan —
> do not block)."

A gated `BUILD_AERON_ARCHIVE_TESTS` CMake option mirrors `BUILD_AERON_TRANSPORT_TESTS`
from P2-8 and is ready to be completed when a JVM is available.

## Interface design (future ArchiveRecordingStore)

The `RecordingStore` interface (in
`libs/replication/include/rune/replication/RecordingStore.h`) is designed
so that an `ArchiveRecordingStore` can be drop-in substituted for
`FileRecordingStore` without changing `PartitionRecorder` or `PartitionReplay`.

When Java becomes available:

1. Implement `ArchiveRecordingStore : RecordingStore` that:
   - Wraps `AeronArchive` to call `startRecording` on a channel/stream.
   - Implements `append()` by publishing to the recorded publication.
   - Implements `scan()` by issuing `startReplay()` and draining the replay image.
   - Tracks `recordingPosition()` via `AeronArchive::getRecordingPosition()`.
2. Enable `BUILD_AERON_ARCHIVE_TESTS` and add `AeronArchiveRecordReplayTest.cpp`
   (scaffold at `libs/replication/tests/archive/`).
