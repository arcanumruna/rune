// tests/support/LeaseScanModel.h - a stub broker's model of ONE lease topic partition (lease topics Task 16): the log (offset = index), the
// folded lease view, the ScanLeases (100) server that pages it exactly like the broker (hash order, maxRows, whole tie groups, done, prefix,
// asOf exclusive), and the subscription feed that pushes log records to the client's latest subscription from its fromOffset.
//
// Row layouts used here (the descriptor of LeaseWatchTest.cpp: lease_version, lease_ttl_ms, lease_holder, lease_remaining_ms, id):
//   log value  : 4-byte header, version(8) @4, ttl(8) @12, holder(8) @20, remaining(8) @28, id(8) @36   (44 bytes)
//   view row   : null bitmap(1) @0, the five fields @1..40, lease_granted_offset(8) @41, lease_updated_offset(8) @49   (57 bytes)
#pragma once

#include "InvokerGateway.h"

#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/LeaseScanHash.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace test_support {

using Bytes = std::vector<uint8_t>;

inline Bytes bytesOf(std::string_view s) { return Bytes(s.begin(), s.end()); }

struct LeaseModelRec {
    Bytes   key;
    uint8_t attrs = 0;
    Bytes   value;
};

class LeaseModel {
public:
    struct Held { int64_t version = 0, granted = 0, updated = 0; uint64_t holder = 0; };
    static constexpr std::size_t kValueLen = 44, kRowLen = 57;   // the default (8-byte key column) layouts
    /// Task 20: the key column's width.  The log value is 36 + keyWidth bytes, the view row 49 + keyWidth + 8 (kRowLen at width 8).
    std::size_t keyWidth = 8;
    [[nodiscard]] std::size_t valueLen() const { return 36 + keyWidth; }
    [[nodiscard]] std::size_t rowLen() const { return 49 + keyWidth; }

    std::vector<LeaseModelRec> log;                 ///< offset = index
    std::map<Bytes, Held>      table;               ///< the folded lease view
    std::optional<int64_t>     asOfOverride;        ///< a (deliberately inconsistent) asOf the scan reports

    [[nodiscard]] int64_t asOf() const { return asOfOverride ? *asOfOverride : static_cast<int64_t>(log.size()); }
    [[nodiscard]] bool held(const Bytes& key) const { return table.count(key) != 0; }

    static Bytes value(int64_t version, const Bytes& key, uint64_t holder = 0, std::size_t width = 8) {
        Bytes v(36 + width, 0);
        v[0] = 40;
        v[2] = 1;
        std::memcpy(v.data() + 4, &version, 8);
        const uint64_t ttl = 3000;
        std::memcpy(v.data() + 12, &ttl, 8);
        std::memcpy(v.data() + 20, &holder, 8);
        std::memcpy(v.data() + 36, key.data(), std::min<std::size_t>(width, key.size()));
        return v;
    }
    static Bytes row(const Bytes& key, const Held& h, std::size_t width = 8) {
        Bytes r(49 + width, 0);   // bitmap 0: every slot present
        std::memcpy(r.data() + 1, &h.version, 8);
        const uint64_t ttl = 3000;
        std::memcpy(r.data() + 9, &ttl, 8);
        std::memcpy(r.data() + 17, &h.holder, 8);
        std::memcpy(r.data() + 33, key.data(), std::min<std::size_t>(width, key.size()));
        std::memcpy(r.data() + 33 + width, &h.granted, 8);
        std::memcpy(r.data() + 41 + width, &h.updated, 8);
        return r;
    }

    // ---- writes (each appends one log record and folds it) ----
    int64_t grant(const Bytes& key, uint64_t holder = 0) {
        const int64_t o = static_cast<int64_t>(log.size());
        log.push_back({key, static_cast<uint8_t>(1), value(o, key, holder, keyWidth)});   // Put|insert; version == offset reads as Granted (intent 0 would be a Carry)
        table[key] = Held{o, o, o, holder};
        return o;
    }
    int64_t update(const Bytes& key) {
        const int64_t o = static_cast<int64_t>(log.size());
        Held& h = table.at(key);
        log.push_back({key, static_cast<uint8_t>(2), value(h.version, key, h.holder, keyWidth)});   // Put|update; version != offset reads as Updated
        h.updated = o;
        return o;
    }
    int64_t end(const Bytes& key, rune::protocol::record::LeaseOp op) {
        const int64_t o = static_cast<int64_t>(log.size());
        const Held h = table.at(key);
        log.push_back({key, rune::protocol::record::withLeaseOp(0, op), value(h.version, key, h.holder, keyWidth)});
        table.erase(key);
        return o;
    }
    int64_t revoke(const Bytes& key) { return end(key, rune::protocol::record::LeaseOp::Revoke); }
    int64_t expire(const Bytes& key) { return end(key, rune::protocol::record::LeaseOp::Expire); }
    /// A record that is NOT folded (the scan view is set by hand): used to put a hand-made row into the view.
    void setRow(const Bytes& key, int64_t version, int64_t granted) { table[key] = Held{version, granted, granted}; }
    /// A revoke record of an arbitrary (key, version) that does not touch the view.
    int64_t rawEnd(const Bytes& key, int64_t version, rune::protocol::record::LeaseOp op) {
        const int64_t o = static_cast<int64_t>(log.size());
        log.push_back({key, rune::protocol::record::withLeaseOp(0, op), value(version, key)});
        return o;
    }
    int64_t rawPut(const Bytes& key, int64_t version) {
        const int64_t o = static_cast<int64_t>(log.size());
        log.push_back({key, static_cast<uint8_t>(version == o ? 1 : 2), value(version, key)});   // never intent 0 (a Carry)
        return o;
    }
    /// A broker Carry (Put, intent 0) of an arbitrary (key, version) that does not touch the view (spec A.3).
    int64_t rawCarry(const Bytes& key, int64_t version) {
        const int64_t o = static_cast<int64_t>(log.size());
        log.push_back({key, static_cast<uint8_t>(0), value(version, key)});
        return o;
    }
};

/// The ScanLeases (100) server of a set of partition models, plus the record feed.
class LeaseStub {
public:
    struct ScanSeen { int32_t partition = 0; Bytes prefix; uint64_t resumeHash = 0; bool first = true; uint16_t maxRows = 0; };

    LeaseStub(InvokerGateway& gw, int32_t topicId, std::size_t partitions) : parts(partitions), gw_(gw), topicId_(topicId) {}

    std::vector<LeaseModel> parts;
    std::vector<ScanSeen>   seen;                         ///< every ScanLeases request received, in order
    bool                    hold = false;                 ///< hold the answers (they are computed on arrival, sent on release())
    std::function<void(const ScanSeen&)> onRequest;       ///< runs on arrival, BEFORE the page is computed
    int16_t                 errorCode = 0;                ///< non-zero: answer this many requests with this code (done = 1, no rows)
    int                     errorNext = 0;
    std::set<int32_t>       holdPartitions;               ///< hold the answers of these partitions only

    void install() {
        gw_.templateHandlers[static_cast<uint16_t>(rune::protocol::client::TemplateId::ScanLeases)] =
            [this](int endpoint, const uint8_t* b, std::size_t n) { onScan_(endpoint, b, n); };
    }
    void reset() {
        for (auto& m : parts) { const std::size_t w = m.keyWidth; m = LeaseModel{}; m.keyWidth = w; }
        seen.clear();
        held_.clear();
        hold = false;
        holdPartitions.clear();
        onRequest = nullptr;
        errorNext = 0;
    }
    void release() {
        hold = false;
        holdPartitions.clear();
        std::vector<std::pair<int, Bytes>> h;
        h.swap(held_);
        for (auto& [ep, frame] : h) gw_.send(ep, frame.data(), frame.size());
    }
    [[nodiscard]] std::size_t heldCount() const { return held_.size(); }

    // ---- the subscription feed ----
    /// Pushes the log records of `partition` the latest subscription has not been sent yet, at most `maxRecords` in batches of at most 32.
    /// Returns the number pushed.  A subscription this has not seen before starts at its fromOffset.
    std::size_t feed(int32_t partition, std::size_t maxRecords = SIZE_MAX) {
        const auto* sub = gw_.latestSubscription(topicId_, partition);
        if (sub == nullptr) return 0;
        Feed& f = feeds_[partition];
        if (f.subId != sub->subscriptionId || f.sess != sub->sessionId) {
            f.subId = sub->subscriptionId;
            f.sess  = sub->sessionId;
            f.next  = std::max<int64_t>(sub->fromOffset, 0);
        }
        const LeaseModel& m = parts[static_cast<std::size_t>(partition)];
        std::size_t pushed = 0;
        while (pushed < maxRecords && f.next < static_cast<int64_t>(m.log.size())) {
            const std::size_t n = std::min<std::size_t>({32, maxRecords - pushed, m.log.size() - static_cast<std::size_t>(f.next)});
            rune::protocol::RecordBatchBuilder b(f.next, 1'000'000);
            for (std::size_t i = 0; i < n; ++i) {
                const LeaseModelRec& r = m.log[static_cast<std::size_t>(f.next) + i];
                b.append(std::string_view(reinterpret_cast<const char*>(r.key.data()), r.key.size()),
                         std::string_view(reinterpret_cast<const char*>(r.value.data()), r.value.size()), 1'000'000 + static_cast<int64_t>(i), r.attrs);
            }
            gw_.pushBatch(*sub, f.next, b.build());
            f.next += static_cast<int64_t>(n);
            pushed += n;
        }
        return pushed;
    }
    [[nodiscard]] int64_t fed(int32_t partition) const {
        const auto it = feeds_.find(partition);
        return it == feeds_.end() ? -1 : it->second.next;
    }
    /// Every record of `partition` has reached the latest subscription's feed.
    [[nodiscard]] bool caughtUp(int32_t partition) const {
        const auto* sub = gw_.latestSubscription(topicId_, partition);
        if (sub == nullptr) return true;
        const auto it = feeds_.find(partition);
        return it != feeds_.end() && it->second.subId == sub->subscriptionId && it->second.next >= static_cast<int64_t>(parts[static_cast<std::size_t>(partition)].log.size());
    }

private:
    struct Feed { int64_t subId = -1, sess = -1, next = 0; };

    void onScan_(int endpoint, const uint8_t* buf, std::size_t len) {
        namespace rp = rune::protocol::client;
        const auto m = rp::ScanLeases::decode(buf, len);
        if (!m || m->partition < 0 || static_cast<std::size_t>(m->partition) >= parts.size()) return;
        ScanSeen s{m->partition, Bytes(m->prefix.begin(), m->prefix.end()), m->resumeHash, m->first == 1, m->maxRows};
        seen.push_back(s);
        if (onRequest) onRequest(s);
        rp::ScanLeasesResponse r;
        r.sessionId     = m->sessionId;
        r.correlationId = m->correlationId;
        Bytes rows;
        if (errorNext > 0) {
            --errorNext;
            r.code = static_cast<uint16_t>(errorCode);
            r.done = 1;
        } else {
            const LeaseModel& lm = parts[static_cast<std::size_t>(m->partition)];
            std::vector<std::pair<uint64_t, const std::pair<const Bytes, LeaseModel::Held>*>> cands;
            for (const auto& e : lm.table) {
                if (e.first.size() < s.prefix.size() || !std::equal(s.prefix.begin(), s.prefix.end(), e.first.begin())) continue;
                const uint64_t h = rune::protocol::scanHash(e.first, topicId_);
                if (s.first || h > s.resumeHash) cands.emplace_back(h, &e);
            }
            std::sort(cands.begin(), cands.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
            std::size_t take = std::min<std::size_t>(cands.size(), s.maxRows);
            while (take < cands.size() && cands[take].first == cands[take - 1].first) ++take;   // whole tie groups
            uint64_t last = s.first ? 0 : s.resumeHash;
            for (std::size_t i = 0; i < take; ++i) {
                const Bytes rw = LeaseModel::row(cands[i].second->first, cands[i].second->second, lm.keyWidth);
                rows.push_back(static_cast<uint8_t>(cands[i].second->first.size()));
                rows.insert(rows.end(), cands[i].second->first.begin(), cands[i].second->first.end());
                const auto rl = static_cast<uint32_t>(rw.size());
                for (int k = 0; k < 4; ++k) rows.push_back(static_cast<uint8_t>(rl >> (8 * k)));
                rows.insert(rows.end(), rw.begin(), rw.end());
                last = cands[i].first;
            }
            r.code       = 0;
            r.asOfOffset = lm.asOf();
            r.lastHash   = last;
            r.done       = take == cands.size() ? 1 : 0;
            r.rowCount   = static_cast<uint16_t>(take);
            r.rows       = std::span<const uint8_t>(rows.data(), rows.size());
        }
        Bytes out(r.encodedSize());
        const std::size_t n = r.encode(out.data());
        out.resize(n);
        if (hold || holdPartitions.count(m->partition) != 0) held_.emplace_back(endpoint, std::move(out));
        else gw_.send(endpoint, out.data(), out.size());
    }

    InvokerGateway&                     gw_;
    int32_t                             topicId_;
    std::vector<std::pair<int, Bytes>>  held_;
    std::map<int32_t, Feed>             feeds_;
};

}  // namespace test_support
