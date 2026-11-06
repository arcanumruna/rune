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
