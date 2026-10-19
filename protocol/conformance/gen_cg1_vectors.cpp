// conformance/gen_cg1_vectors.cpp — CG-1 golden vector generator.
//
// Generates 4 committed binary golden vectors for the consumer-group codecs:
//   cg1_join_group.bin            JoinGroup with 2 subscribed topic IDs
//   cg1_sync_group_response.bin   SyncGroupResponse with 2 partition assignments
//   cg1_offset_commit.bin         OffsetCommit with 1 entry carrying metadata
//   cg1_assign_revoke.bin         Assign with empty assignment (= revoke all)
//
// Compile & run (from rune-protocol root):
//   c++ -std=c++23 -I include conformance/gen_cg1_vectors.cpp -o /tmp/rune_gen_cg1
//   /tmp/rune_gen_cg1 conformance/vectors

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>
#include <vector>

#include "rune/protocol/client/GroupMessages.h"
#include "rune/protocol/client/OffsetMessages.h"

namespace rpc = rune::protocol::client;

static void write_vec(const char* dir, const char* name,
                      const uint8_t* buf, std::size_t len) {
    std::string path = std::string(dir) + "/" + name;
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) { std::perror(path.c_str()); std::exit(1); }
    if (std::fwrite(buf, 1, len, f) != len) {
        std::perror(path.c_str()); std::fclose(f); std::exit(1);
    }
    std::fclose(f);
    std::printf("wrote %s (%zu bytes)\n", path.c_str(), len);
}

int main(int argc, char** argv) {
    const char* dir = (argc > 1) ? argv[1] : "conformance/vectors";

    static constexpr int64_t kSessionId     = static_cast<int64_t>(0x1122334455667788ULL);
    static constexpr int64_t kCorrelationId = static_cast<int64_t>(0x0A0B0C0D0E0F1011ULL);

    static const uint8_t kGroupId[]  = {'g', 'r', 'p', '2'};
    static const uint8_t kMemberId[] = {'m', 'b', 'r', 'X'};
    static const uint8_t kMeta[]     = {0xDE, 0xAD, 0xBE, 0xEF};

    // -----------------------------------------------------------------------
    // 1. cg1_join_group.bin
    //
    // JoinGroup{ sessionTimeoutMs=30000, groupId="grp2", memberId="mbrX",
    //            subscribedTopicIds=[10, 20] }
    //
    // Frame layout:
    //   [0..23]  header: blockLen=4, tid=32, schema=120, ver=1, session, corr
    //   [24..27] sessionTimeoutMs = 30000
    //   [28..29] groupId len = 4
    //   [30..33] "grp2"
    //   [34..35] memberId len = 4
    //   [36..39] "mbrX"
    //   [40..41] topicIds count = 2
    //   [42..45] topicId[0] = 10
    //   [46..49] topicId[1] = 20
    //   Total = 50 bytes
    // -----------------------------------------------------------------------
    {
        const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
        const std::span<const uint8_t> mbr(kMemberId, sizeof(kMemberId));
        const int32_t topicIds[] = {10, 20};
        const std::span<const int32_t> topicSpan(topicIds, 2);

        rpc::JoinGroup msg;
        msg.sessionId        = kSessionId;
        msg.correlationId    = kCorrelationId;
        msg.sessionTimeoutMs = 30000;
        msg.groupId          = grp;
        msg.memberId         = mbr;

        const std::size_t total = rpc::JoinGroup::encodedSizeFor(grp, mbr, 2u);
        std::vector<uint8_t> buf(total);
        const std::size_t n = msg.encode(buf.data(), topicSpan);
        write_vec(dir, "cg1_join_group.bin", buf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 2. cg1_sync_group_response.bin
    //
    // SyncGroupResponse{ errorCode=0, assignment=[(topicId=10,partition=0),
    //                                              (topicId=20,partition=3)] }
    //
    // Frame layout:
    //   [0..23]  header: blockLen=2, tid=35, schema=120, ver=1, session, corr
    //   [24..25] errorCode = 0
    //   [26..27] assignment count = 2
    //   [28..31] topicId[0] = 10
    //   [32..35] partition[0] = 0
    //   [36..39] topicId[1] = 20
    //   [40..43] partition[1] = 3
    //   Total = 44 bytes
    // -----------------------------------------------------------------------
    {
        const rpc::TopicPartition32 asgn[] = { {10, 0}, {20, 3} };
        const std::span<const rpc::TopicPartition32> asgnSpan(asgn, 2);

        rpc::SyncGroupResponse msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.errorCode     = 0;

        const std::size_t total = rpc::SyncGroupResponse::encodedSizeFor(2u);
        std::vector<uint8_t> buf(total);
        const std::size_t n = msg.encode(buf.data(), asgnSpan);
        write_vec(dir, "cg1_sync_group_response.bin", buf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 3. cg1_offset_commit.bin
    //
    // OffsetCommit{ generationId=5, groupId="grp2", memberId="mbrX",
    //               entries=[(topicId=10,partition=0,offset=1000,meta={DE AD BE EF})] }
    //
    // Frame layout:
    //   [0..23]  header: blockLen=4, tid=40, schema=120, ver=1, session, corr
    //   [24..27] generationId = 5
    //   [28..29] groupId len = 4
    //   [30..33] "grp2"
    //   [34..35] memberId len = 4
    //   [36..39] "mbrX"
    //   [40..41] entries count = 1
    //   [42..45] topicId = 10
    //   [46..49] partition = 0
    //   [50..57] offset = 1000
    //   [58..59] meta len = 4
    //   [60..63] DE AD BE EF
    //   Total = 64 bytes
    // -----------------------------------------------------------------------
    {
        const std::span<const uint8_t> grp(kGroupId, sizeof(kGroupId));
        const std::span<const uint8_t> mbr(kMemberId, sizeof(kMemberId));

        rpc::OffsetCommitEntry e;
        e.topicId   = 10;
        e.partition = 0;
        e.offset    = 1000LL;
        e.metadata  = std::span<const uint8_t>(kMeta, sizeof(kMeta));

        const rpc::OffsetCommitEntry entries[] = {e};
        const std::span<const rpc::OffsetCommitEntry> entrySpan(entries, 1);

        rpc::OffsetCommit msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.generationId  = 5;
        msg.groupId       = grp;
        msg.memberId      = mbr;

        const std::size_t total = rpc::OffsetCommit::encodedSizeFor(grp, mbr, entrySpan);
        std::vector<uint8_t> buf(total);
        const std::size_t n = msg.encode(buf.data(), entrySpan);
        write_vec(dir, "cg1_offset_commit.bin", buf.data(), n);
    }

    // -----------------------------------------------------------------------
    // 4. cg1_assign_revoke.bin
    //
    // Assign{ generationId=7, assignment=[] (empty = revoke all) }
    //
    // Frame layout:
    //   [0..23]  header: blockLen=4, tid=44, schema=120, ver=1, session, corr
    //   [24..27] generationId = 7
    //   [28..29] assignment count = 0
    //   Total = 30 bytes
    // -----------------------------------------------------------------------
    {
        const std::span<const rpc::TopicPartition32> empty{};

        rpc::Assign msg;
        msg.sessionId     = kSessionId;
        msg.correlationId = kCorrelationId;
        msg.generationId  = 7;

        const std::size_t total = rpc::Assign::encodedSizeFor(0u);
        std::vector<uint8_t> buf(total);
        const std::size_t n = msg.encode(buf.data(), empty);
        write_vec(dir, "cg1_assign_revoke.bin", buf.data(), n);
    }

    std::printf("Done. 4 CG-1 vectors written to %s/\n", dir);
    return 0;
}
