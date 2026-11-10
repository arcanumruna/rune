// The one lease-column helper (prepend / strip) shared by the broker, the catalog replay, the CLI and the QL.
#include <rune/codec/LeaseColumns.h>
#include <rune/codec/Serialize.h>

#include <gtest/gtest.h>

#include <string>

namespace cd = rune::codec;

namespace {

cd::Descriptor userDescriptor() {
    cd::DescriptorBuilder b("inst");
    b.addTagged("a", 0, cd::PrimType::Int32, 1, 0xFFFF, false, {});
    b.addTagged("b", 2, cd::PrimType::UInt64, 1, 0xFFFF, false, {});
    b.addFixedStrTagged("c", 5, 16, 1, 0xFFFF, true);
    return b.build();
}

std::string refusalFor(const std::string& name) {
    return "lease topic '" + name + "' does not start with the lease system columns";
}

}  // namespace

TEST(LeaseColumnsRegression, PrependThenStripRoundTripsByteIdentically) {
    const cd::Descriptor user = userDescriptor();
    const std::vector<uint8_t> before = cd::serialize(user);

    cd::Descriptor d = user;
    ASSERT_TRUE(cd::prependLeaseColumns(d));
    ASSERT_EQ(d.fields.size(), 7u);
    for (std::size_t i = 0; i < cd::kLeaseColumns.size(); ++i) {
        EXPECT_EQ(d.fields[i].id, i);
        EXPECT_EQ(d.fields[i].name, cd::kLeaseColumns[i].name);
        EXPECT_EQ(d.fields[i].type, cd::kLeaseColumns[i].type);
    }
    EXPECT_EQ(d.fields[4].id, 4);
    EXPECT_EQ(d.fields[5].id, 6);
    EXPECT_EQ(d.fields[6].id, 9);

    auto stripped = cd::stripLeaseColumns(cd::deserialize(cd::serialize(d)), "inst");
    ASSERT_TRUE(stripped.has_value()) << stripped.error();
    EXPECT_EQ(cd::serialize(*stripped), before);
}

TEST(LeaseColumnsRegression, PrependRefusesAnIdThatNoLongerFits) {
    cd::DescriptorBuilder b("inst");
    b.addTagged("a", 0xFFFC, cd::PrimType::Int32, 1, 0xFFFF, false, {});
    cd::Descriptor d = b.build();
    EXPECT_FALSE(cd::prependLeaseColumns(d));
}

TEST(LeaseColumnsRegression, StripRefusalsKeepTheirText) {
    const auto withColumns = [] {
        cd::Descriptor d = userDescriptor();
        EXPECT_TRUE(cd::prependLeaseColumns(d));
        return d;
    };
    // fewer than four fields
    {
        auto r = cd::stripLeaseColumns(userDescriptor(), "t1");
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), refusalFor("t1"));
    }
    // a column missing (lease_holder removed)
    {
        cd::Descriptor d = withColumns();
        d.fields.erase(d.fields.begin() + 2);
        auto r = cd::stripLeaseColumns(std::move(d), "t2");
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), refusalFor("t2"));
    }
    // wrong type
    {
        cd::Descriptor d = withColumns();
        d.fields[1].type = cd::PrimType::Int64;
        auto r = cd::stripLeaseColumns(std::move(d), "t3");
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), refusalFor("t3"));
    }
    // wrong order
    {
        cd::Descriptor d = withColumns();
        std::swap(d.fields[2], d.fields[3]);
        auto r = cd::stripLeaseColumns(std::move(d), "t4");
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), refusalFor("t4"));
    }
    // wrong id
    {
        cd::Descriptor d = withColumns();
        d.fields[0].id = 9;
        auto r = cd::stripLeaseColumns(std::move(d), "t5");
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), refusalFor("t5"));
    }
}
