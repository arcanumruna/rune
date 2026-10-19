// StorageIdentityTest.cpp — storage.meta: the §3.2 format table, the refusals, the CLI rows and the origin /
// manifest transitions (ISR design v3, Step 5).  Pure filesystem: no threads, no BrokerNode.
#include <rune/node/StorageIdentity.h>

#include <rune/transaction/detail/Crc32c.h>

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

namespace fs = std::filesystem;
using rune::node::StorageFormat;
using rune::node::StorageIdentity;
using rune::node::StorageRefusal;
using Origin = StorageIdentity::Origin;
using Mode = StorageIdentity::Mode;
using Class = StorageIdentity::Class;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const std::string& tag) {
        static int seq = 0;
        path = fs::temp_directory_path() / ("rune_storage_id_" + std::to_string(::getpid()) + "_" + tag + "_" +
                                            std::to_string(seq++));
        fs::remove_all(path);
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
    [[nodiscard]] std::string str() const { return path.string(); }
};

void touch(const fs::path& p, const std::string& content = "x") {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f << content;
}

std::string slurp(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

std::size_t entryCount(const fs::path& d) {
    std::size_t n = 0;
    for ([[maybe_unused]] const auto& e : fs::directory_iterator(d)) ++n;
    return n;
}

std::string firstLine(const std::string& s) { return s.substr(0, s.find('\n')); }

/// The refusal's first line, or "" when resolve did not throw StorageRefusal.  Any other exception fails.
std::string refusalOf(const std::string& dir, StorageFormat f, bool single) {
    try {
        (void)StorageIdentity::resolve(dir, f, single);
    } catch (const StorageRefusal& e) {
        return firstLine(e.what());
    } catch (const std::exception& e) {
        ADD_FAILURE() << "not a StorageRefusal: " << e.what();
        return "<other exception>";
    }
    return "";
}

/// A storage.meta with a VALID crc over `body` (lines ending '\n').
void writeRawMeta(const fs::path& dir, const std::string& body) {
    const uint32_t crc = rune::transaction::detail::crc32c(reinterpret_cast<const uint8_t*>(body.data()), body.size());
    char b[16];
    std::snprintf(b, sizeof b, "%08x", crc);
    touch(dir / "storage.meta", body + "crc=" + b + "\n");
}

void makeLegacy(const fs::path& dir) {
    touch(dir / "node-state.dat");
    touch(dir / "term.log");
}

// ── R-text first lines (verbatim, §3.2) ──────────────────────────────────────────────────────────────────────
std::string r1(const std::string& d) {
    return "data dir " + d + " is empty and not formatted (no storage.meta); nothing was written.";
}
std::string r4(const std::string& d) {
    return d + " is formatted to JOIN a cluster (origin=joiner), but RUNE_PEERS lists only node 0: a single-node";
}
std::string r4b(const std::string& d) {
    return d + " was re-identified with --reassign-join (origin=joiner), but RUNE_PEERS lists only node 0: a single-node";
}

// ── The format table (M12) ───────────────────────────────────────────────────────────────────────────────────
enum class DirState { Empty, Legacy, Founder, JoinerEmptyManifest };
enum class Expect { R1, R4, Load };

struct Row {
    const char*   name;
    StorageFormat fmt;
    bool          single;
    DirState      state;
    Expect        expect;
    Origin        origin = Origin::Undecided;
    Mode          mode = Mode::None;
};

void PrintTo(const Row& r, std::ostream* os) { *os << r.name; }

class StorageIdentityRegression : public ::testing::TestWithParam<Row> {};

TEST_P(StorageIdentityRegression, FormatTableRegression) {
    const Row& r = GetParam();
    TmpDir t(r.name);
    switch (r.state) {
        case DirState::Empty: break;
        case DirState::Legacy: makeLegacy(t.path); break;
        case DirState::Founder: StorageIdentity::write_(t.str(), {StorageIdentity::freshId(), Origin::Founder, Mode::None, 0}); break;
        case DirState::JoinerEmptyManifest: StorageIdentity::write_(t.str(), {StorageIdentity::freshId(), Origin::Joiner, Mode::None, 0}); break;
    }
    const std::size_t before = entryCount(t.path);
    const std::string before_meta = fs::exists(t.path / "storage.meta") ? slurp(t.path / "storage.meta") : "";
    switch (r.expect) {
        case Expect::R1:
            EXPECT_EQ(refusalOf(t.str(), r.fmt, r.single), r1(t.str()));
            EXPECT_EQ(entryCount(t.path), before) << "an empty-row refusal writes nothing";
            break;
        case Expect::R4:
            EXPECT_EQ(refusalOf(t.str(), r.fmt, r.single), r4(t.str()));
            EXPECT_EQ(entryCount(t.path), before) << "a refusal writes nothing";
            if (!before_meta.empty()) EXPECT_EQ(slurp(t.path / "storage.meta"), before_meta);
            break;
        case Expect::Load: {
            const auto l = StorageIdentity::resolve(t.str(), r.fmt, r.single);
            EXPECT_EQ(l.origin, r.origin);
            EXPECT_EQ(l.mode, r.mode);
            EXPECT_NE(l.id, 0);
            if (r.state == DirState::Legacy) EXPECT_EQ(l.manifest, static_cast<uint8_t>(Class::Consensus));
            // What was persisted is what resolve returned (a restart loads the same identity).
            const auto again = StorageIdentity::resolve(t.str(), StorageFormat::Required, r.single);
            EXPECT_EQ(again.id, l.id);
            EXPECT_EQ(again.origin, l.origin);
            EXPECT_EQ(again.mode, l.mode);
            break;
        }
    }
}

constexpr auto U = StorageFormat::Required;
constexpr auto N = StorageFormat::New;
constexpr auto J = StorageFormat::Join;
constexpr auto A = StorageFormat::Auto;
constexpr auto T = StorageFormat::TestFounder;

INSTANTIATE_TEST_SUITE_P(Table, StorageIdentityRegression, ::testing::Values(
    // empty
    Row{"unset_single_empty", U, true,  DirState::Empty, Expect::R1},
    Row{"unset_multi_empty",  U, false, DirState::Empty, Expect::R1},
    Row{"new_single_empty",   N, true,  DirState::Empty, Expect::Load, Origin::Founder},
    Row{"new_multi_empty",    N, false, DirState::Empty, Expect::Load, Origin::Undecided, Mode::New},
    Row{"join_single_empty",  J, true,  DirState::Empty, Expect::R4},
    Row{"join_multi_empty",   J, false, DirState::Empty, Expect::Load, Origin::Joiner},
    Row{"auto_single_empty",  A, true,  DirState::Empty, Expect::Load, Origin::Founder},
    Row{"auto_multi_empty",   A, false, DirState::Empty, Expect::Load, Origin::Undecided, Mode::Auto},
    Row{"tf_single_empty",    T, true,  DirState::Empty, Expect::Load, Origin::Founder},
    Row{"tf_multi_empty",     T, false, DirState::Empty, Expect::Load, Origin::Founder},
    // legacy (node-state.dat + term.log, no storage.meta)
    Row{"unset_single_legacy", U, true,  DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"unset_multi_legacy",  U, false, DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"new_single_legacy",   N, true,  DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"new_multi_legacy",    N, false, DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"join_single_legacy",  J, true,  DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"join_multi_legacy",   J, false, DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"auto_single_legacy",  A, true,  DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"auto_multi_legacy",   A, false, DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"tf_single_legacy",    T, true,  DirState::Legacy, Expect::Load, Origin::Legacy},
    Row{"tf_multi_legacy",     T, false, DirState::Legacy, Expect::Load, Origin::Legacy},
    // formatted as origin=founder: loaded whatever the env says
    Row{"unset_single_founder", U, true,  DirState::Founder, Expect::Load, Origin::Founder},
    Row{"unset_multi_founder",  U, false, DirState::Founder, Expect::Load, Origin::Founder},
    Row{"new_single_founder",   N, true,  DirState::Founder, Expect::Load, Origin::Founder},
    Row{"new_multi_founder",    N, false, DirState::Founder, Expect::Load, Origin::Founder},
    Row{"join_single_founder",  J, true,  DirState::Founder, Expect::Load, Origin::Founder},
    Row{"join_multi_founder",   J, false, DirState::Founder, Expect::Load, Origin::Founder},
    Row{"auto_single_founder",  A, true,  DirState::Founder, Expect::Load, Origin::Founder},
    Row{"auto_multi_founder",   A, false, DirState::Founder, Expect::Load, Origin::Founder},
    Row{"tf_single_founder",    T, true,  DirState::Founder, Expect::Load, Origin::Founder},
    Row{"tf_multi_founder",     T, false, DirState::Founder, Expect::Load, Origin::Founder},
    // single node, origin=joiner with an empty manifest
    Row{"unset_single_joiner_empty_manifest", U, true, DirState::JoinerEmptyManifest, Expect::R4}),
    [](const ::testing::TestParamInfo<Row>& i) { return std::string(i.param.name); });

// ── The named rows ───────────────────────────────────────────────────────────────────────────────────────────

TEST(StorageIdentityRegression, EmptyIgnoresForeignEntriesRegression) {
    TmpDir t("foreign");
    fs::create_directories(t.path / "lost+found");
    fs::create_directories(t.path / "aeron");
    touch(t.path / ".hidden");
    const auto l = StorageIdentity::resolve(t.str(), StorageFormat::New, false);
    EXPECT_EQ(l.origin, Origin::Undecided);
    EXPECT_EQ(l.mode, Mode::New);
    EXPECT_TRUE(fs::exists(t.path / "storage.meta"));
    EXPECT_TRUE(fs::exists(t.path / "lost+found")) << "foreign entries are left alone";
}

TEST(StorageIdentityRegression, TmpLeftoverIsDiscardedRegression) {
    TmpDir t("tmp");
    touch(t.path / "storage.meta.tmp", "garbage\x01\x02 not a storage.meta");
    const auto l = StorageIdentity::resolve(t.str(), StorageFormat::Join, false);
    EXPECT_EQ(l.origin, Origin::Joiner);
    EXPECT_FALSE(fs::exists(t.path / "storage.meta.tmp"));
    EXPECT_EQ(StorageIdentity::resolve(t.str(), StorageFormat::Required, false).id, l.id);
}

TEST(StorageIdentityRegression, UndecidedRequiresModeRegression) {
    TmpDir t("nomode");
    writeRawMeta(t.path, "version=1\nstorageId=00000000000000ab\norigin=undecided\nmanifest=\n");
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Required, false),
              t.str() + "/storage.meta is unreadable (undecided without mode), so this dir's storage identity is lost.");
}

TEST(StorageIdentityRegression, CorruptStorageMetaRefusesWithItsReasonRegression) {
    TmpDir t("corrupt");
    StorageIdentity::write_(t.str(), {StorageIdentity::freshId(), Origin::Founder, Mode::None, 0});
    std::string text = slurp(t.path / "storage.meta");
    text[text.find("founder")] = 'F';   // one flipped byte
    touch(t.path / "storage.meta", text);
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Auto, false),
              t.str() + "/storage.meta is unreadable (CRC mismatch), so this dir's storage identity is lost.");
    writeRawMeta(t.path, "version=1\nstorageId=0000000000000000\norigin=founder\nmanifest=\n");
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Auto, false),
              t.str() + "/storage.meta is unreadable (storageId is 0), so this dir's storage identity is lost.");
    writeRawMeta(t.path, "version=1\nstorageId=00000000000000ab\norigin=martian\nmanifest=\n");
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Auto, false),
              t.str() + "/storage.meta is unreadable (unknown origin 'martian'), so this dir's storage identity is lost.");
    writeRawMeta(t.path, "version=1\nstorageId=00000000000000ab\norigin=founder\nmanifest=\nclusterId=7\n");
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Auto, false),
              t.str() + "/storage.meta is unreadable (unknown key 'clusterId'), so this dir's storage identity is lost.");
    touch(t.path / "storage.meta", "version=1\nstorageId=00000000000000ab\n");
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Auto, false),
              t.str() + "/storage.meta is unreadable (truncated), so this dir's storage identity is lost.");
}

TEST(StorageIdentityRegression, SingleNodeRejectsAJoinFormattedDirRegression) {
    TmpDir t("single_join");
    ASSERT_EQ(StorageIdentity::format(t.str(), Origin::Joiner, Mode::None, false).code, 0);
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Required, true), r4(t.str()));
}

TEST(StorageIdentityRegression, SingleNodeReassignedRestoreNamesTheSingleNodeProcedureRegression) {
    TmpDir t("single_reassign");
    makeLegacy(t.path);
    const auto res = StorageIdentity::reassignJoin(t.str());
    ASSERT_EQ(res.code, 0) << res.err;
    EXPECT_EQ(refusalOf(t.str(), StorageFormat::Required, true), r4b(t.str()));
}

TEST(StorageIdentityRegression, LegacyNeedsConsensusStateRegression) {
    {
        TmpDir t("legacy_inc");
        touch(t.path / "process-incarnation", "3");
        EXPECT_EQ(refusalOf(t.str(), StorageFormat::Auto, false),
                  t.str() + " holds broker data but not node-state.dat and term.log (missing: node-state.dat, term.log): a partly deleted data dir");
        EXPECT_FALSE(fs::exists(t.path / "storage.meta"));
    }
    {
        TmpDir t("legacy_half");
        touch(t.path / "node-state.dat");
        fs::create_directories(t.path / "topics");
        EXPECT_EQ(refusalOf(t.str(), StorageFormat::Required, true),
                  t.str() + " holds broker data but not node-state.dat and term.log (missing: term.log): a partly deleted data dir");
    }
}

TEST(StorageIdentityRegression, ManifestClassMissingRefusesRegression) {
    TmpDir t("manifest");
    const std::string d = t.str();
    ASSERT_EQ(StorageIdentity::format(d, Origin::Undecided, Mode::New, false).code, 0);
    auto l = StorageIdentity::resolve(d, StorageFormat::Required, false);
    ASSERT_EQ(l.origin, Origin::Undecided);
    ASSERT_TRUE(StorageIdentity::persistOrigin(d, l, Origin::Founder));
    ASSERT_TRUE(StorageIdentity::markPresent(d, l, Class::Consensus));
    ASSERT_TRUE(StorageIdentity::markPresent(d, l, Class::Knowledge));
    ASSERT_TRUE(StorageIdentity::markPresent(d, l, Class::Knowledge)) << "idempotent";
    makeLegacy(t.path);
    touch(t.path / "internal-isr.state");
    const auto ok = StorageIdentity::resolve(d, StorageFormat::Required, false);   // precondition: intact → loads
    EXPECT_EQ(ok.manifest, static_cast<uint8_t>(static_cast<uint8_t>(Class::Consensus) | static_cast<uint8_t>(Class::Knowledge)));
    fs::remove(t.path / "internal-isr.state");
    EXPECT_EQ(refusalOf(d, StorageFormat::Required, false),
              d + " is damaged: storage.meta says knowledge existed, but internal-isr.state are gone. Wipe the whole dir (including");
    touch(t.path / "internal-isr.state");
    fs::remove(t.path / "term.log");
    EXPECT_EQ(refusalOf(d, StorageFormat::Required, false),
              d + " is damaged: storage.meta says consensus existed, but term.log are gone. Wipe the whole dir (including");
    makeLegacy(t.path);
    ASSERT_TRUE(StorageIdentity::markPresent(d, l, Class::MetadataLog));
    EXPECT_EQ(refusalOf(d, StorageFormat::Required, false),
              d + " is damaged: storage.meta says metadataLog existed, but __cluster_metadata are gone. Wipe the whole dir (including");
    fs::create_directories(t.path / "__cluster_metadata");
    touch(t.path / "__cluster_metadata" / "00000000000000000000.log", "");
    EXPECT_NE(refusalOf(d, StorageFormat::Required, false), "") << "only empty files: the metadata log is gone";
    touch(t.path / "__cluster_metadata" / "00000000000000000000.log", "rec");
    EXPECT_EQ(refusalOf(d, StorageFormat::Required, false), "");
    ASSERT_TRUE(StorageIdentity::clearClassForTest(d, Class::MetadataLog));
    fs::remove_all(t.path / "__cluster_metadata");
    EXPECT_EQ(refusalOf(d, StorageFormat::Required, false), "") << "a cleared class is not checked";
}

TEST(StorageIdentityRegression, ReassignJoinRotatesIdAndDropsKnowledgeRegression) {
    TmpDir t("rotate");
    const std::string d = t.str();
    ASSERT_EQ(StorageIdentity::format(d, Origin::Undecided, Mode::New, false).code, 0);
    auto l = StorageIdentity::resolve(d, StorageFormat::Required, false);
    ASSERT_TRUE(StorageIdentity::persistOrigin(d, l, Origin::Founder));
    ASSERT_TRUE(StorageIdentity::markPresent(d, l, Class::Knowledge));
    touch(t.path / "internal-isr.state");
    const int64_t old = l.id;
    const auto res = StorageIdentity::reassignJoin(d);
    ASSERT_EQ(res.code, 0) << res.err;
    EXPECT_EQ(res.out.rfind("re-identified " + d + ": storageId=", 0), 0u) << res.out;
    EXPECT_FALSE(fs::exists(t.path / "internal-isr.state")) << "the restored knowledge must not vote";
    const auto n = StorageIdentity::resolve(d, StorageFormat::Required, false);
    EXPECT_NE(n.id, old);
    EXPECT_EQ(n.origin, Origin::Joiner);
    EXPECT_EQ(n.manifest & static_cast<uint8_t>(Class::Knowledge), 0);
}

TEST(StorageIdentityRegression, ReassignJoinAdoptsAPreUpgradeBackupRegression) {
    TmpDir t("preupgrade");
    const std::string d = t.str();
    makeLegacy(t.path);
    touch(t.path / "internal-isr.state");
    const auto res = StorageIdentity::reassignJoin(d);
    ASSERT_EQ(res.code, 0) << res.err;
    EXPECT_EQ(res.out.rfind("re-identified " + d + " (pre-storage-id data kept): storageId=", 0), 0u) << res.out;
    EXPECT_FALSE(fs::exists(t.path / "internal-isr.state"));
    const auto n = StorageIdentity::resolve(d, StorageFormat::Required, false);
    EXPECT_EQ(n.origin, Origin::Joiner);
    EXPECT_EQ(n.manifest, static_cast<uint8_t>(Class::Consensus));
    // Without the Legacy rule it is the R-3 refusal (nothing written).
    TmpDir u("preupgrade_half");
    touch(u.path / "internal-isr.state");
    const auto bad = StorageIdentity::reassignJoin(u.str());
    EXPECT_EQ(bad.code, 1);
    EXPECT_EQ(firstLine(bad.err), u.str() + " holds broker data but not node-state.dat and term.log (missing: node-state.dat, term.log): a partly deleted data dir");
    EXPECT_FALSE(fs::exists(u.path / "storage.meta"));
}

TEST(StorageIdentityRegression, OriginTransitionsAreGuardedRegression) {
    TmpDir t("guard");
    const std::string d = t.str();
    StorageIdentity::Loaded f{StorageIdentity::freshId(), Origin::Founder, Mode::None, 0};
    StorageIdentity::write_(d, f);
    EXPECT_THROW(StorageIdentity::persistOrigin(d, f, Origin::Joiner), std::logic_error);
    EXPECT_EQ(f.origin, Origin::Founder);
    EXPECT_EQ(StorageIdentity::resolve(d, StorageFormat::Required, false).origin, Origin::Founder) << "nothing written";
    StorageIdentity::Loaded u{StorageIdentity::freshId(), Origin::Undecided, Mode::Auto, 0};
    StorageIdentity::write_(d, u);
    EXPECT_THROW(StorageIdentity::persistOrigin(d, u, Origin::Legacy), std::logic_error);
    EXPECT_THROW(StorageIdentity::persistOrigin(d, u, Origin::Undecided), std::logic_error);
    ASSERT_TRUE(StorageIdentity::persistOrigin(d, u, Origin::Joiner));
    EXPECT_EQ(u.origin, Origin::Joiner);
    EXPECT_EQ(u.mode, Mode::None);
    EXPECT_THROW(StorageIdentity::persistOrigin(d, u, Origin::Founder), std::logic_error);
}

TEST(StorageIdentityRegression, CliFormatRowsRegression) {
    TmpDir t("cli");
    const std::string d = t.str();
    auto n = StorageIdentity::format(d, Origin::Undecided, Mode::New, false);
    ASSERT_EQ(n.code, 0) << n.err;
    EXPECT_NE(n.out.find("origin=undecided (new cluster; founds once a majority of the configured members has started)"), std::string::npos) << n.out;
    // --new again: already formatted.
    auto again = StorageIdentity::format(d, Origin::Undecided, Mode::New, false);
    EXPECT_EQ(again.code, 1);
    EXPECT_NE(again.err.find("is already formatted"), std::string::npos) << again.err;
    // --join on undecided: re-formatted as joiner, same id.
    const int64_t id = StorageIdentity::resolve(d, StorageFormat::Required, false).id;
    auto j = StorageIdentity::format(d, Origin::Joiner, Mode::None, false);
    ASSERT_EQ(j.code, 0) << j.err;
    EXPECT_EQ(j.out, "re-formatted " + d + " as joiner (it never voted, stood or led)\n");
    const auto l = StorageIdentity::resolve(d, StorageFormat::Required, false);
    EXPECT_EQ(l.id, id);
    EXPECT_EQ(l.origin, Origin::Joiner);
    // A joiner: refused; --ignore-formatted → the same text on stdout, exit 0.
    auto k = StorageIdentity::format(d, Origin::Joiner, Mode::None, false);
    EXPECT_EQ(k.code, 1);
    EXPECT_EQ(k.err, "refusing: " + d + " is already formatted (storageId=" + StorageIdentity::idHex(id) +
                         ", origin=joiner); nothing changed. To replace this disk's identity, wipe the whole dir first.\n");
    auto ki = StorageIdentity::format(d, Origin::Joiner, Mode::None, true);
    EXPECT_EQ(ki.code, 0);
    EXPECT_EQ(ki.out, k.err);
    // A pre-storage-id dir: never formatted.
    TmpDir lg("cli_legacy");
    makeLegacy(lg.path);
    auto lj = StorageIdentity::format(lg.str(), Origin::Joiner, Mode::None, false);
    EXPECT_EQ(lj.code, 1);
    EXPECT_EQ(lj.err.rfind("refusing: " + lg.str() + " holds data written before storage ids existed;", 0), 0u) << lj.err;
    EXPECT_FALSE(fs::exists(lg.path / "storage.meta"));
    // --reassign-join on an empty dir.
    TmpDir e("cli_empty");
    auto r = StorageIdentity::reassignJoin(e.str());
    EXPECT_EQ(r.code, 1);
    EXPECT_EQ(r.err, "refusing: " + e.str() + " is not formatted; --reassign-join re-identifies a restored data dir. For an empty dir use --join.\n");
    EXPECT_EQ(entryCount(e.path), 0u);
}

} // namespace

// CP1 review M3: --reassign-join on a PRE-UPGRADE dir writes the new Joiner storage.meta BEFORE it unlinks the restored
// knowledge file.  The old order left a window with no storage.meta, in which the next boot adopted the dir as Legacy
// with a fresh id (and the stale knowledge).  The unlink is made to fail (the "file" is a non-empty directory), so the
// state after the failure shows the order.  RED: unlink first (no storage.meta is left behind).
TEST(StorageIdentityRegression, ReassignJoinOnAPreUpgradeDirWritesTheJoinerBeforeUnlinkingKnowledgeRegression) {
    TmpDir t("preupgrade_order");
    const std::string d = t.str();
    makeLegacy(t.path);
    fs::create_directories(t.path / "internal-isr.state");
    touch(t.path / "internal-isr.state" / "x");
    const auto res = StorageIdentity::reassignJoin(d);
    EXPECT_EQ(res.code, 74) << "the unlink must have failed: " << res.out << res.err;
    ASSERT_TRUE(fs::exists(t.path / "storage.meta")) << "no storage.meta: the crash window adopts the dir as Legacy";
    std::ifstream mf(t.path / "storage.meta");
    std::stringstream ms; ms << mf.rdbuf();
    EXPECT_NE(ms.str().find("origin=joiner"), std::string::npos) << "a dir interrupted mid-command must already be a Joiner";
}

// CP1 review M4: an UNREADABLE storage.meta (EACCES/EIO) is an I/O fault, not a corrupt identity - R-2 would advise
// wiping a healthy storage id.  RED: the old `if (!f) return Corrupt{"truncated"}` (a StorageRefusal is thrown).
TEST(StorageIdentityRegression, UnreadableStorageMetaIsAnIoErrorNotACorruptIdentityRegression) {
    if (::geteuid() == 0) GTEST_SKIP() << "root reads any file";
    TmpDir t("unreadable");
    const std::string d = t.str();
    ASSERT_EQ(StorageIdentity::format(d, Origin::Joiner, Mode::None, false).code, 0);
    fs::permissions(t.path / "storage.meta", fs::perms::none);
    EXPECT_THROW((void)StorageIdentity::resolve(d, StorageFormat::Required, false), rune::node::StorageIoError);
    fs::permissions(t.path / "storage.meta", fs::perms::owner_read | fs::perms::owner_write);
}

// CP1 N1: the crash between --reassign-join's storage.meta write and the unlink of the restored knowledge file leaves a
// Joiner (manifest without `knowledge`) next to an internal-isr.state.  The next boot must delete it before it can be
// loaded or voted.  RED: drop the boot-time removal (the file survives resolve).
TEST(StorageIdentityRegression, JoinerBootDeletesAStaleInternalIsrStateRegression) {
    TmpDir t("joiner_stale_knowledge");
    const std::string d = t.str();
    makeLegacy(t.path);
    fs::create_directories(t.path / "internal-isr.state");   // makes the command's unlink fail = the crash point
    touch(t.path / "internal-isr.state" / "x");
    ASSERT_EQ(StorageIdentity::reassignJoin(d).code, 74);
    ASSERT_TRUE(fs::exists(t.path / "storage.meta"));
    fs::remove_all(t.path / "internal-isr.state");           // the restored file as the next boot would see it
    touch(t.path / "internal-isr.state");
    const auto n = StorageIdentity::resolve(d, StorageFormat::Required, false);
    EXPECT_EQ(n.origin, Origin::Joiner);
    EXPECT_FALSE(fs::exists(t.path / "internal-isr.state")) << "a Joiner booted with a restored knowledge file";
}

// B-M1 (ISR v3 Step 6): the process incarnation is max(prev + 1, realtime ns), so a WIPED data dir (counter back to
// 0) still yields a value above every incarnation the dead process ever sent.
// RED: `prev + 1` only (gives 1000 and then 1).
TEST(StorageIdentityRegression, IncarnationIsMonotonicAcrossAWipedDataDirRegression) {
    TmpDir t("incarnation");
    const std::string d = t.str();
    touch(t.path / "process-incarnation", "999\n");
    EXPECT_EQ(StorageIdentity::bumpProcessIncarnation(d, 5000), 5000);
    fs::remove(t.path / "process-incarnation");
    EXPECT_EQ(StorageIdentity::bumpProcessIncarnation(d, 6000), 6000);
    touch(t.path / "process-incarnation", "10000\n");
    EXPECT_EQ(StorageIdentity::bumpProcessIncarnation(d, 5000), 10001);
    // The counter is persisted: the next boot continues from it.
    EXPECT_EQ(StorageIdentity::bumpProcessIncarnation(d, 5000), 10002);
    // Unwritable / unnamed dir: the clock.
    EXPECT_EQ(StorageIdentity::bumpProcessIncarnation("", 7777), 7777);
}
