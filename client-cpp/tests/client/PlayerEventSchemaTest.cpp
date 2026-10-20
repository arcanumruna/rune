// Phase 2c Task 4: the shared player_event schema (client-cpp/examples/filter/PlayerEventSchema.h) and the
// claim-gen header built from it.  The fingerprint is pinned as a LITERAL (the same one the codec's
// FilterGeneratedRegression.PlayerEventFingerprintIsPinnedRegression pins), never compared descriptor-to-descriptor.
#include "../../examples/filter/PlayerEventSchema.h"
#include "player_event.gen.h"

#include <rune/codec/Fingerprint.h>
#include <rune/codec/FilterBuilder.h>

#include <gtest/gtest.h>

namespace {
constexpr uint64_t kPlayerEventV1Fingerprint = 18066230342614569726ull;
}

TEST(PlayerEventSchemaRegression, SharedDescriptorFingerprintIsPinnedRegression) {
    EXPECT_EQ(rune::codec::fingerprint(rune::examples::playerEventDescriptor()), kPlayerEventV1Fingerprint);
}
TEST(PlayerEventSchemaRegression, GeneratedHeaderFingerprintIsPinnedRegression) {
    EXPECT_EQ(rune::gen::PlayerEvent::kFingerprint, kPlayerEventV1Fingerprint);
}
TEST(PlayerEventSchemaRegression, GeneratedFilterAndBuilderAgreeOnTheSharedSchemaRegression) {
    const auto d = rune::examples::playerEventDescriptor();
    const auto a = rune::gen::PlayerEvent::Filter{ .worldId = 42u, .zone = "eu" }.encode();
    const auto b = rune::codec::FilterBuilder::on(d).field("zone").eq(std::string_view("eu")).field("worldId").eq(int64_t{42}).build();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a, *b);
}
