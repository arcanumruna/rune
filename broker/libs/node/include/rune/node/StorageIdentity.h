#pragma once
// StorageIdentity.h — the data dir's storage identity, <dataDir>/storage.meta (ISR design v3 §3.2, Step 5).
//
//   version=1
//   storageId=<16 hex, non-zero>
//   origin=undecided|founder|joiner|legacy
//   mode=new|auto                 (only when origin=undecided)
//   manifest=<comma list of: consensus,knowledge,metadataLog>   (may be empty)
//   crc=<8 hex CRC-32C over the preceding bytes>
//
// Written with tmp + fsync + rename + dir fsync.  Cold path only: boot (BrokerNode::start, before anything
// else writes into the data dir), the `rune_broker storage format` CLI, and the rare origin / manifest
// transitions.  Every function is static and keeps no state: the caller owns its Loaded.

#include <rune/node/StorageFormat.h>

#include <cstdint>
#include <stdexcept>
#include <string>

namespace rune::node {

/// A refusal to start (the R-1 … R-4b texts).  main() prints it after
/// "rune-broker: refusing to start — " and exits 78.
struct StorageRefusal : std::runtime_error { using std::runtime_error::runtime_error; };

/// An I/O failure writing storage.meta: "<op> <path>: <strerror>".  The CLI exits 74 on it.
struct StorageIoError : std::runtime_error { using std::runtime_error::runtime_error; };

class StorageIdentity {
public:
    enum class Origin : uint8_t { Undecided, Founder, Joiner, Legacy };
    enum class Mode : uint8_t { None, New, Auto };
    enum class Class : uint8_t { Consensus = 1, Knowledge = 2, MetadataLog = 4 };

    struct Loaded {
        int64_t id       = 0;                  ///< the storage id (a non-zero u64, bit-cast)
        Origin  origin   = Origin::Undecided;
        Mode    mode     = Mode::None;         ///< New / Auto only while origin == Undecided
        uint8_t manifest = 0;                  ///< OR of Class bits
    };

    /// One CLI outcome: the exit code and the stdout / stderr text (each ends with '\n' when non-empty).
    struct FormatResult {
        int         code = 0;
        std::string out;
        std::string err;
    };

    /// Boot: load, adopt (legacy) or format <dir> per the §3.2 table.  Throws StorageRefusal with the exact
    /// R-1, R-2, R-3, R-3b, R-4 or R-4b text; StorageIoError when a write fails.  `nodeId` names this node in
    /// the single-node texts (R-4 / R-4b).
    static Loaded resolve(const std::string& dir, StorageFormat fmt, bool singleNode, int32_t nodeId = 0);

    /// A random non-zero storage id.
    static int64_t freshId();

    /// tmp + fsync + rename + dir fsync.  Throws StorageIoError.
    static void write_(const std::string& dir, const Loaded& l);

    /// The CLI's --new (Undecided, New) and --join (Joiner, None) rows (M12).  An Undecided dir formatted
    /// Joiner goes through reformatJoin.  Never throws.
    static FormatResult format(const std::string& dir, Origin o, Mode m, bool ignoreFormatted);
    /// The undecided → joiner row: keeps the storage id and the manifest.
    static FormatResult reformatJoin(const std::string& dir);
    /// --reassign-join (R13, v2-review I8): a formatted dir, or a pre-storage-id dir that holds the Legacy
    /// rule, gets a fresh id, origin joiner, its manifest minus knowledge, and loses internal-isr.state.
    static FormatResult reassignJoin(const std::string& dir);

    /// Undecided → Founder / Joiner only (anything else is a std::logic_error).  On success `l` is updated;
    /// false (and `l` unchanged) when the write failed.
    static bool persistOrigin(const std::string& dir, Loaded& l, Origin to);
    /// Sets one manifest class.  Idempotent (no write when already set); never clears.
    static bool markPresent(const std::string& dir, Loaded& l, Class c);
    /// Test only (ShardModeTest): clears one manifest class of the dir's storage.meta.
    static bool clearClassForTest(const std::string& dir, Class c);

    /// KIP-903 process incarnation (R8): `max(prev + 1, realtimeNowNs)`, where `prev` is the counter in
    /// <dir>/process-incarnation, bumped durably (tmp + fsync + rename).  The clock makes it monotonic across a
    /// wiped data dir.  Falls back to `realtimeNowNs` when the dir is empty/unwritable.  Cold (once per process).
    static int64_t bumpProcessIncarnation(const std::string& dataDir, int64_t realtimeNowNs);

    static const char* originName(Origin o) noexcept;
    /// 16 lower-case hex digits.
    static std::string idHex(int64_t id);
};

/// `rune_broker storage format (--new|--join|--reassign-join) [--data-dir D] [--ignore-formatted]`:
/// argv are the arguments AFTER "storage format".  Prints the stdout / stderr texts and returns the exit
/// code (0, 1, 64 usage, 74 I/O).  Creates no thread.
int runStorageFormatCli(int argc, char** argv);

} // namespace rune::node
