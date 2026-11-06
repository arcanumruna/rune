// Part 07 Task 10: a keyed view checkpoints as v3 records and recovers rows, owner map and prefix index from them.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include <unistd.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <rune/node/AdminChannels.h>
#include <rune/node/ViewCheckpointScan.h>
#include <rune/node/ViewSnapshotChannel.h>
#include <rune/node/WorkerViewSet.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/view/ViewCheckpoint.h>

#include "CheckedWritesKeyedAggFixture.h"
#include "CheckedWritesKeyedViewFixture.h"

namespace {

using namespace cwkeyed;
using rune::node::WorkerViewSet;
using Status = rune::protocol::client::ViewRowResponse;

std::string freshDir(const std::string& name) {
    const auto d = std::filesystem::temp_directory_path() / ("rune-cw-keyed-recovery-" + name + "-" + std::to_string(::getpid()));
    std::filesystem::remove_all(d);
    std::filesystem::create_directories(d);
    return d.string();
}

struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
    std::shared_ptr<spdlog::logger> prev = spdlog::default_logger();
    LogCapture() {
        auto lg = std::make_shared<spdlog::logger>("capture", sink);
        lg->set_level(spdlog::level::trace);
        lg->set_pattern("%v");
        spdlog::set_default_logger(lg);
    }
    ~LogCapture() { spdlog::set_default_logger(prev); }
    std::vector<std::string> grep(const std::string& needle) const {
        std::vector<std::string> out;
        for (auto& l : sink->last_formatted()) if (l.find(needle) != std::string::npos) out.push_back(l);
        return out;
    }
};

// One keyed view on one persisted worker set; every pushed delta is appended to the instance file as the IO agent would.
struct Writer {
    rune::codec::Descriptor src = characterSource();
    rune::view::CompileResult comp;
    std::string dir;
    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs;
    Log log{src};
    std::vector<uint8_t> file;

    Writer(rune::view::CompileResult c, const std::string& name, uint64_t seed = 0) : comp(std::move(c)), dir(freshDir(name)), vs(ch, dir, seed) {
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted=*/true);
    }
    void foldAll() {
        // a pass is capped, so keep driving until the cursor reaches the end
        for (int guard = 0; guard < 10000 && vs.foldCursorForTest(kViewTopic, 0) < log.end(); ++guard) {
            vs.driveFolds(kSrcTopic, 0, [&](int64_t f, int64_t t) { return log.read(f, t); }, log.end(), rune::view::kNoSkip, 0);
            ch.deltas.drain([&](int32_t type, aeron::concurrent::AtomicBuffer& b, rune::node::OneToOneByteRing::index_t off,
                                rune::node::OneToOneByteRing::index_t len) {
                if (type != rune::node::ViewSnapshotChannel::kDeltaMsgType) return;
                const uint8_t* p = b.buffer() + off + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes;
                file.insert(file.end(), p, p + len - rune::node::ViewSnapshotChannel::kDeltaHeaderBytes);
            });
        }
    }
    void writeFile() const {
        std::ofstream f(rune::node::viewCheckpointFilePath(dir, kViewTopic, 0), std::ios::binary);
        f.write(reinterpret_cast<const char*>(file.data()), static_cast<std::streamsize>(file.size()));
    }
};

// A fresh set that recovers the view from the writer's file (the production path: a recovery install, then materialize).
struct Recovered {
    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs;
    explicit Recovered(const Writer& w, uint64_t seed = 0) : vs(ch, w.dir, seed) {
        rune::node::ViewPlanUpdateMsg m;
        m.viewTopicId = kViewTopic;
        m.persistence = 0;
        m.recovery    = true;
        m.planBlob    = rune::view::serialize(w.comp.plan);
        m.outDescBlob = rune::codec::serialize(w.comp.outputDescriptor);
        m.srcDescBlob = rune::codec::serialize(w.src);
        EXPECT_TRUE(rune::node::encodeViewPlanUpdate(ch.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
        EXPECT_EQ(vs.drainInstalls(), 1);
        vs.materializeRecoveryInstancesFor(kSrcTopic, 0, w.log.end());
    }
};

std::vector<uint8_t> rowAt(const WorkerViewSet& vs, const std::vector<uint8_t>& key) {
    const rune::view::RowLayout* l = nullptr;
    const uint8_t* row = vs.getRowK(kViewTopic, 0, key, l);
    return row == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(row, row + l->rowLen);
}

std::vector<uint8_t> ownerAt(const WorkerViewSet& vs, uint64_t characterId) {
    const auto* inst = vs.instanceForTest(kViewTopic, 0);
    const std::string wire = wireKey(characterId);
    const uint8_t* o = inst->owners->get(rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()}));
    return o == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(o, o + inst->keyed->keyWidth);
}

// Every row under `prefix`, paged with a small limit, sorted (the scan order depends on insertion history).
std::vector<std::vector<uint8_t>> scanAll(WorkerViewSet& vs, const std::vector<uint8_t>& prefix, uint32_t rowLen) {
    std::vector<std::vector<uint8_t>> rows;
    std::vector<uint8_t> out(rune::node::kMaxScanPageBytes);
    std::vector<uint8_t> token;
    for (int guard = 0; guard < 1000; ++guard) {
        const auto p = vs.scanPage(kViewTopic, 0, prefix, token, 7, out);
        EXPECT_EQ(p.status, Status::kOk);
        if (p.status != Status::kOk) break;
        size_t pos = 0;
        for (uint16_t i = 0; i < p.rows; ++i) {
            uint32_t len;
            std::memcpy(&len, out.data() + pos, 4);
            EXPECT_EQ(len, rowLen);
            rows.emplace_back(out.begin() + static_cast<long>(pos) + 4, out.begin() + static_cast<long>(pos) + 4 + len);
            pos += 4 + len;
        }
        if (p.nextTokenLen == 0) break;
        token.assign(p.nextToken.begin(), p.nextToken.begin() + p.nextTokenLen);
    }
    std::sort(rows.begin(), rows.end());
    return rows;
}

}  // namespace

TEST(CheckedWritesKeyedRecovery, V3CheckpointRestoresRowsOwnersAndPrefixes) {
    // each worker draws its own seed: the recovered tables hash differently from the written ones
    Writer w(accountCharacters(characterSource()), "restore", 12345);
    std::mt19937 rng(2026);
    std::map<uint64_t, std::pair<uint64_t, uint16_t>> model;   // characterId -> (userId, gender)
    int ops = 0;
    while (ops < 1000) {
        std::vector<Rec> batch;
        const int n = 1 + static_cast<int>(rng() % 20);
        for (int i = 0; i < n && ops < 1000; ++i, ++ops) {
            Rec r;
            r.characterId = 1 + rng() % 30;
            if (rng() % 7 == 0) {
                r.tombstone = true;
                model.erase(r.characterId);
            } else {
                r.userId = 1 + rng() % 4;
                r.gender = static_cast<uint16_t>(rng() % 1000);
                model[r.characterId] = {r.userId, r.gender};
            }
            batch.push_back(r);
        }
        w.log.append(batch);
        w.foldAll();
    }
    w.writeFile();
    ASSERT_FALSE(w.file.empty());
    EXPECT_GT(w.vs.foldCounters(kViewTopic, 0)->ownerMoves, 50u) << "the run must exercise renames";
    EXPECT_GT(rune::node::splitViewCheckpointRecords(w.file).size(), 10u);

    LogCapture cap;
    Recovered r(w, 99);
    const auto lines = cap.grep("VIEW-RECOVERED view=" + std::to_string(kViewTopic) + " partition=0 O=" + std::to_string(w.log.end()));
    ASSERT_EQ(lines.size(), 1u);
    EXPECT_NE(lines[0].find("format=v3"), std::string::npos) << lines[0];
    EXPECT_EQ(r.vs.readiness(kViewTopic, 0), rune::node::ViewReadiness::Ready);

    const uint32_t rowLen = w.comp.rowLayout.rowLen;
    for (uint64_t c = 1; c <= 30; ++c) {
        const auto it = model.find(c);
        for (uint64_t u = 1; u <= 4; ++u) {
            const auto key = beKey({u, c});
            const auto want = rowAt(w.vs, key);
            EXPECT_EQ(rowAt(r.vs, key), want) << "user " << u << " character " << c;
            EXPECT_EQ(!want.empty(), it != model.end() && it->second.first == u);
        }
        EXPECT_EQ(ownerAt(r.vs, c), ownerAt(w.vs, c)) << "character " << c;
        EXPECT_EQ(ownerAt(r.vs, c).empty(), it == model.end());
    }
    for (uint64_t u = 1; u <= 5; ++u) EXPECT_EQ(scanAll(r.vs, beKey({u}), rowLen), scanAll(w.vs, beKey({u}), rowLen)) << "user " << u;
    size_t rows = 0;
    for (uint64_t u = 1; u <= 4; ++u) rows += scanAll(r.vs, beKey({u}), rowLen).size();
    EXPECT_EQ(rows, model.size());
}

TEST(CheckedWritesKeyedRecoveryRegression, RenameJustBeforeRestartRecoversOneRowRegression) {
    Writer w(namedCharacters(characterSource()), "rename");
    w.log.append({{7, 1, "Foo", 3}});
    w.foldAll();
    w.log.append({{7, 1, "Bar", 3}});   // the last pass renames c7
    w.foldAll();
    w.writeFile();

    Recovered r(w);
    Writer fresh(namedCharacters(characterSource()), "rename-fresh");
    fresh.log.append({{7, 1, "Foo", 3}});
    fresh.log.append({{7, 1, "Bar", 3}});
    fresh.foldAll();

    const uint32_t rowLen = w.comp.rowLayout.rowLen;
    for (const WorkerViewSet* vs : {&r.vs, &fresh.vs}) {
        EXPECT_TRUE(rowAt(*vs, nameKey("foo", 7)).empty());
        EXPECT_FALSE(rowAt(*vs, nameKey("bar", 7)).empty());
        EXPECT_EQ(ownerAt(*vs, 7), nameKey("bar", 7));
    }
    EXPECT_EQ(rowAt(r.vs, nameKey("bar", 7)), rowAt(fresh.vs, nameKey("bar", 7)));
    std::vector<uint8_t> bar(24, 0);
    std::copy_n("bar", 3, bar.begin());
    std::vector<uint8_t> foo(24, 0);
    std::copy_n("foo", 3, foo.begin());
    EXPECT_EQ(scanAll(r.vs, bar, rowLen).size(), 1u);
    EXPECT_EQ(scanAll(r.vs, foo, rowLen).size(), 0u);
}

namespace {

// The device_history aggregate on one persisted worker set, mirroring Writer.
struct AggWriter {
    rune::codec::Descriptor src = cwagg::deviceSource();
    rune::view::CompileResult comp = cwagg::deviceHistory(src);
    std::string dir;
    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet vs;
    cwagg::Log log{src};
    std::vector<uint8_t> file;

    AggWriter(const std::string& name, uint64_t seed) : dir(freshDir(name)), vs(ch, dir, seed) {
        vs.installView(comp.plan, comp.rowLayout, src, /*persisted=*/true);
    }
    // a pass is capped and the delta ring must be drained for the next one, so keep driving until the cursor reaches the end
    void foldAll(const cwagg::Log& l) {
        for (int guard = 0; guard < 10000 && vs.foldCursorForTest(kViewTopic, 0) < l.end(); ++guard) {
            vs.driveFolds(kSrcTopic, 0, [&](int64_t f, int64_t t) { return l.read(f, t); }, l.end(), rune::view::kNoSkip, 0);
            drainDeltas();
        }
    }
    void drainDeltas() {
        ch.deltas.drain([&](int32_t type, aeron::concurrent::AtomicBuffer& b, rune::node::OneToOneByteRing::index_t off,
                            rune::node::OneToOneByteRing::index_t len) {
            if (type != rune::node::ViewSnapshotChannel::kDeltaMsgType) return;
            const uint8_t* p = b.buffer() + off + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes;
            file.insert(file.end(), p, p + len - rune::node::ViewSnapshotChannel::kDeltaHeaderBytes);
        });
    }
    void writeFile() const {
        std::ofstream f(rune::node::viewCheckpointFilePath(dir, kViewTopic, 0), std::ios::binary);
        f.write(reinterpret_cast<const char*>(file.data()), static_cast<std::streamsize>(file.size()));
    }
};

const char* const kFps[] = {"fpA", "fpB", "fpC", "fpD", "fpE", "fpF"};

// Random inserts, moves between groups, and tombstones over a pool of 200 event ids.
void appendRandomEvents(AggWriter& w, std::mt19937& rng, int count) {
    for (int done = 0; done < count;) {
        std::vector<cwagg::Ev> batch;
        const int n = std::min(count - done, 1 + static_cast<int>(rng() % 20));
        for (int i = 0; i < n; ++i) {
            cwagg::Ev e;
            e.id = 1 + rng() % 200;
            if (rng() % 6 == 0) {
                e.tombstone = true;
            } else {
                e.user     = 1 + rng() % 4;
                e.fp       = kFps[rng() % 6];
                e.platform = static_cast<uint32_t>(rng() % 3);
                e.lastSeen = static_cast<int64_t>(rng() % 1000);
                e.bytes    = rng() % 100;
            }
            batch.push_back(e);
        }
        w.log.append(batch);
        w.foldAll(w.log);
        done += n;
    }
}

const uint8_t* bytesOf(const rune::view::ViewTable& t, uint64_t k) { return t.get(k); }

void expectSameRow(const uint8_t* a, const uint8_t* b, uint32_t len, const std::string& what) {
    ASSERT_EQ(a == nullptr, b == nullptr) << what;
    if (a != nullptr) EXPECT_EQ(std::vector<uint8_t>(a, a + len), std::vector<uint8_t>(b, b + len)) << what;
}

// The refold folds in another order inside blocks, so only the view rows are comparable.
void expectSameView(const rune::node::ViewInstance& a, const rune::node::ViewInstance& b) {
    EXPECT_EQ(a.table.size(), b.table.size());
    for (uint64_t u = 1; u <= 4; ++u) {
        for (const char* fp : kFps) {
            const auto vk = rune::view::ViewKey::fromBytes(cwagg::groupKey(u, fp));
            expectSameRow(a.table.get(vk), b.table.get(vk), a.layout.rowLen, "view user " + std::to_string(u) + " " + fp);
        }
    }
}

// Every table of two aggregate instances holds the same rows (the tables cannot be iterated, so probe every key that can exist).
void expectSameTables(const rune::node::ViewInstance& a, const rune::node::ViewInstance& b) {
    ASSERT_TRUE(a.agg && b.agg);
    const auto& x = *a.agg;
    const auto& y = *b.agg;
    EXPECT_EQ(x.nextGroupId, y.nextGroupId);
    EXPECT_EQ(a.table.size(), b.table.size());
    EXPECT_EQ(x.contributors.size(), y.contributors.size());
    EXPECT_EQ(x.groups.size(), y.groups.size());
    EXPECT_EQ(x.groupIds->size(), y.groupIds->size());
    ASSERT_EQ(x.blocks.has_value(), y.blocks.has_value());
    if (x.blocks) EXPECT_EQ(x.blocks->size(), y.blocks->size());

    for (uint64_t u = 1; u <= 4; ++u) {
        for (const char* fp : kFps) {
            const auto key = cwagg::groupKey(u, fp);
            const auto vk  = rune::view::ViewKey::fromBytes(key);
            const std::string what = "user " + std::to_string(u) + " " + fp;
            expectSameRow(a.table.get(vk), b.table.get(vk), a.layout.rowLen, "view " + what);
            expectSameRow(x.groupIds->get(vk), y.groupIds->get(vk), 8, "groupIds " + what);
        }
    }
    for (uint64_t id = 0; id <= 201; ++id)
        expectSameRow(bytesOf(x.contributors, id), bytesOf(y.contributors, id), x.layout.contributorRow.rowLen, "contributor " + std::to_string(id));
    for (uint64_t g = 0; g <= x.nextGroupId + 1; ++g) {
        expectSameRow(bytesOf(x.groups, g), bytesOf(y.groups, g), x.layout.groupRow.rowLen, "group " + std::to_string(g));
        if (!x.blocks) continue;
        for (uint32_t id = rune::view::blockIdCandidate(g), n = 0; n < 8; ++n, ++id)
            for (uint32_t chunk = 0; chunk < 4; ++chunk)
                expectSameRow(bytesOf(*x.blocks, rune::view::blockChunkKey(id, chunk)), bytesOf(*y.blocks, rune::view::blockChunkKey(id, chunk)),
                              x.layout.blockRow.rowLen, "block of group " + std::to_string(g));
    }
}

}  // namespace

TEST(CheckedWritesKeyedRecoveryRegression, KeyedAggregateRestartEqualsRefoldRegression) {
    AggWriter w("agg-restart", 12345);
    std::mt19937 rng{77};
    appendRandomEvents(w, rng, 2000);
    w.writeFile();
    ASSERT_FALSE(w.file.empty());
    const auto* orig = w.vs.instanceForTest(kViewTopic, 0);
    ASSERT_NE(orig, nullptr);
    ASSERT_TRUE(orig->agg && orig->agg->groupIds && orig->agg->blocks);
    const auto* counters = w.vs.foldCounters(kViewTopic, 0);
    EXPECT_GT(counters->aggRescans, 0u) << "the run must retract Min/Max extremes";
    EXPECT_GT(orig->agg->nextGroupId, 0u);

    // recovery draws its own hash seed: the tables hash differently from the written ones
    rune::node::ViewSnapshotChannel ch;
    WorkerViewSet rec(ch, w.dir, 99);
    rune::node::ViewPlanUpdateMsg m;
    m.viewTopicId = kViewTopic;
    m.persistence = 0;
    m.recovery    = true;
    m.planBlob    = rune::view::serialize(w.comp.plan);
    m.outDescBlob = rune::codec::serialize(w.comp.outputDescriptor);
    m.srcDescBlob = rune::codec::serialize(w.src);
    ASSERT_TRUE(rune::node::encodeViewPlanUpdate(ch.install, rune::node::ViewSnapshotChannel::kInstallMsgType, m));
    ASSERT_EQ(rec.drainInstalls(), 1);
    rec.materializeRecoveryInstancesFor(kSrcTopic, 0, w.log.end());
    ASSERT_EQ(rec.readiness(kViewTopic, 0), rune::node::ViewReadiness::Ready);
    { SCOPED_TRACE("recovered"); expectSameTables(*orig, *rec.instanceForTest(kViewTopic, 0)); }

    // 500 more events after the restart fold like a fresh refold of all 2,500: no recovered gid is handed out twice
    appendRandomEvents(w, rng, 500);
    for (int guard = 0; guard < 10000 && rec.foldCursorForTest(kViewTopic, 0) < w.log.end(); ++guard) {
        rec.driveFolds(kSrcTopic, 0, [&](int64_t f, int64_t t) { return w.log.read(f, t); }, w.log.end(), rune::view::kNoSkip, 0);
        ch.deltas.drain([](int32_t, aeron::concurrent::AtomicBuffer&, rune::node::OneToOneByteRing::index_t, rune::node::OneToOneByteRing::index_t) {});
    }
    AggWriter fresh("agg-restart-fresh", 5);
    fresh.foldAll(w.log);
    expectSameView(*fresh.vs.instanceForTest(kViewTopic, 0), *rec.instanceForTest(kViewTopic, 0));
}

namespace {

std::vector<uint8_t> bytesAt(const rune::view::ViewTable& t, const rune::view::ViewKey& k, size_t len) {
    const uint8_t* p = t.get(k);
    return p == nullptr ? std::vector<uint8_t>{} : std::vector<uint8_t>(p, p + len);
}

constexpr int kUniqueNames = 30;
constexpr uint64_t kUniqueIds = 200;

std::string uniqueName(int i, bool upper) { return (upper ? "N" : "n") + std::to_string(i); }

// Tables 0, 1, 6 and 7 of two unique instances hold the same bytes (probed over every name and id that can exist).
void expectSameClaims(const rune::node::ViewInstance& a, const rune::node::ViewInstance& b) {
    ASSERT_TRUE(a.claims && b.claims);
    const size_t K = a.keyed->keyWidth;
    const size_t W = a.keyed->wireKeyWidth;
    EXPECT_EQ(a.table.size(), b.table.size());
    EXPECT_EQ(a.owners->size(), b.owners->size());
    EXPECT_EQ(a.claims->indexTable().size(), b.claims->indexTable().size());
    EXPECT_EQ(a.claims->extrasTable().size(), b.claims->extrasTable().size());
    EXPECT_EQ(a.claims->extras(), b.claims->extras());
    for (int i = 0; i < kUniqueNames; ++i) {
        std::vector<uint8_t> n(K, 0);
        const std::string name = uniqueName(i, false);
        std::copy(name.begin(), name.end(), n.begin());
        const auto nk = rune::view::ViewKey::fromBytes(n);
        EXPECT_EQ(bytesAt(a.table, nk, a.layout.rowLen), bytesAt(b.table, nk, b.layout.rowLen)) << name;
        EXPECT_EQ(bytesAt(a.claims->indexTable(), nk, 2 * W + 5), bytesAt(b.claims->indexTable(), nk, 2 * W + 5)) << name;
    }
    for (uint64_t id = 1; id <= kUniqueIds; ++id) {
        const std::string wire = wireKey(id);
        const auto wk = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(wire.data()), wire.size()});
        EXPECT_EQ(bytesAt(*a.owners, wk, K), bytesAt(*b.owners, wk, K)) << id;
        EXPECT_EQ(bytesAt(a.claims->extrasTable(), wk, K + 2 * W + 1 + a.layout.rowLen),
                  bytesAt(b.claims->extrasTable(), wk, K + 2 * W + 1 + b.layout.rowLen)) << id;
    }
}

rune::view::CompileResult uniqueNames() {
    const auto d = characterSource();
    rune::view::ViewPlan v;
    v.viewName       = "unique_names";
    v.sources        = {{kSrcTopic, "characterId"}};
    v.unique         = true;
    v.unicodeVersion = rune::codec::kUnicodeVersion;
    v.keyComponents  = {{"name", 1, "nameKey"}};
    v.output.push_back({"gender", d.find("gender")->type, 0, "gender", rune::view::MergeRule::Replace});
    const rune::codec::Descriptor* srcs[] = {&d};
    auto r = rune::view::compileView(v, srcs, kViewTopic);
    if (!r) throw std::runtime_error(r.error().message);
    return std::move(*r);
}

}  // namespace

TEST(CheckedWritesKeyedRecoveryRegression, UniqueClaimsSurviveRestartRegression) {
    Writer w(uniqueNames(), "unique", 12345);
    std::mt19937 rng(7);
    std::vector<std::vector<Rec>> batches;
    for (int batch = 0; batch < 10; ++batch) {
        std::vector<Rec> recs;
        for (int i = 0; i < 100; ++i) {
            Rec r;
            r.characterId = 1 + rng() % kUniqueIds;
            r.userId      = 1 + rng() % 5;
            r.name        = uniqueName(static_cast<int>(rng() % kUniqueNames), rng() % 2 == 0);
            r.gender      = static_cast<uint16_t>(rng() % 4);
            r.tombstone   = rng() % 100 < 15;
            recs.push_back(r);
        }
        w.log.append(recs);
        w.foldAll();
        batches.push_back(std::move(recs));
    }
    w.writeFile();
    const auto* wi = w.vs.instanceForTest(kViewTopic, 0);
    ASSERT_NE(wi, nullptr);
    ASSERT_TRUE(wi->claims.has_value());
    ASSERT_GT(wi->counters.uniqueConflicts, 0u);

    Recovered r(w, 99);
    const auto* ri = r.vs.instanceForTest(kViewTopic, 0);
    ASSERT_NE(ri, nullptr);
    expectSameClaims(*wi, *ri);
    EXPECT_EQ(ri->counters.uniqueConflicts, wi->counters.uniqueConflicts);

    // The owner of a name that has extras leaves after the restart: the same extra takes over as in a fresh refold of the whole log.
    uint64_t leaver = 0;
    for (int i = 0; i < kUniqueNames && leaver == 0; ++i) {
        std::vector<uint8_t> n(wi->keyed->keyWidth, 0);
        const std::string name = uniqueName(i, false);
        std::copy(name.begin(), name.end(), n.begin());
        const auto nk = rune::view::ViewKey::fromBytes(n);
        if (!wi->claims->hasExtras(nk)) continue;
        const uint8_t* o = wi->claims->ownerOf(nk);
        std::memcpy(&leaver, o, 8);
    }
    ASSERT_NE(leaver, 0u);
    batches.push_back({Rec{leaver, 0, "", 0, true}});
    w.log.append(batches.back());
    auto& inst = const_cast<rune::node::ViewInstance&>(*ri);
    auto fold = rune::node::projectionFoldOf(inst);
    int64_t cursor = inst.foldCursor;
    const auto st = rune::view::foldProjectionBatch(fold, inst.prospect, w.log.read(cursor, w.log.end()), w.log.end(), cursor, rune::view::kNoSkip);
    EXPECT_EQ(st.uniquePromotions, 1u);
    inst.counters.uniqueConflicts = inst.claims->extras();

    Writer fresh(uniqueNames(), "unique-fresh", 5);
    for (const auto& b : batches) fresh.log.append(b);
    fresh.foldAll();
    expectSameClaims(*fresh.vs.instanceForTest(kViewTopic, 0), inst);
}

// One record dirties at most 5 extras rows (removeExtra: itself and both neighbours; addExtra: the new row and the old head), and the
// pass bound must cover them: every single-record delta of a heavily colliding unique view stays within the bound it was sized for.
TEST(CheckedWritesKeyedRecoveryRegression, UniqueClaimsDeltaFitsTheBoundRegression) {
    Writer w(uniqueNames(), "unique-bound");
    const int64_t K = w.comp.plan.keyWidth, W = 8, rowLen = w.comp.rowLayout.rowLen;
    const int64_t perRecord = 2 * (5 + K) + rowLen + (5 + W + K) + 2 * (5 + K + 2 * W + 5) + 5 * (5 + W + K + 2 * W + 1 + rowLen);
    const int64_t fixed = rune::node::ViewSnapshotChannel::kDeltaHeaderBytes + rune::view::kCheckpointCrcBytes + rune::view::kCheckpointV3HeaderBytes +
                          4 * rune::view::kCheckpointV3TableHeaderBytes;
    w.vs.driveFolds(kSrcTopic, 0, [&](int64_t, int64_t) { return rune::storage::LogReadView{}; }, 0, rune::view::kNoSkip, 0);
    EXPECT_EQ(w.vs.maxRecordsPerPassForTest(kViewTopic, 0), (w.ch.deltas.maxMsgLength() - fixed) / perRecord);

    // "a" owned by c1 with extras c10, c11, c12 (list head c12); "b" owned by c30 with extras c40, c41. The middle extra c11
    // renames to "b" and takes it over from c30: it leaves a's list (c11, c12, c10), c30 joins b's list (c30, c41).
    const std::pair<uint64_t, const char*> setup[] = {{1, "a"}, {10, "a"}, {11, "a"}, {12, "a"}, {30, "b"}, {40, "b"}, {41, "b"}};
    for (const auto& [id, name] : setup) w.log.append({Rec{id, 1, name, 0}});
    w.foldAll();
    const std::size_t before = w.file.size();
    w.log.append({Rec{11, 1, "b", 0}});
    w.foldAll();
    const int64_t frame = static_cast<int64_t>(w.file.size() - before) + rune::node::ViewSnapshotChannel::kDeltaHeaderBytes;
    const int64_t oldBound = 2 * (5 + K) + rowLen + (5 + W + K) + 2 * (5 + K + 2 * W + 5) + 2 * (5 + W + K + 2 * W + 1 + rowLen);
    EXPECT_GT(frame - fixed, oldBound);
    EXPECT_LE(frame - fixed, perRecord);
    EXPECT_LE(fixed + w.vs.maxRecordsPerPassForTest(kViewTopic, 0) * (frame - fixed), w.ch.deltas.maxMsgLength()) << "a full pass of such records overflows the ring";
}
