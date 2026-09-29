#include <gtest/gtest.h>
#include "rune/node/AdminWorker.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
using namespace rune::node; using namespace rune::codec;

namespace {
std::vector<uint8_t> charsBlob() {
    return serialize(DescriptorBuilder("characters",1)
        .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
        .add("name",PrimType::String).build());
}
struct Recorder {
    int creates=0, appends=0, pushes=0;
    int32_t lastTopicId=-1; uint16_t lastErr=0xffff;
    // RULING-1: createTopic no longer receives a pre-allocated id — it returns
    // the authoritative sequential id from the registry (simulated here as a
    // monotone counter starting at 0).
    int32_t nextTopicId = 0;
    AdminDeps make(bool commit=true) {
        AdminDeps d;
        d.createTopic=[this](int32_t,const std::string&){ ++creates; return nextTopicId++; };
        d.appendDurable=[this,commit](const TopicSchema&){ ++appends; return commit; };
        d.pushValidators=[this](const RegistryEntry&,int32_t){ ++pushes; };
        d.respondDefine=[this](int64_t,int64_t,int32_t tid,uint16_t,uint16_t e){ lastTopicId=tid; lastErr=e; };
        d.respondGet=[this](int64_t,int64_t,int32_t,uint16_t,const std::vector<uint8_t>&,uint16_t e){ lastErr=e; };
        return d;
    }
};
}

TEST(AdminWorker, DefineTopicHappyPath) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"characters","userId",16,true,charsBlob()});
    EXPECT_EQ(r.creates,1);
    EXPECT_EQ(r.appends,1);
    EXPECT_EQ(r.pushes,1);                       // validate=true → validators pushed
    EXPECT_EQ(r.lastErr,0);                      // OK
    // RULING-1: id is the sequential value returned by createTopic (0 for first topic).
    EXPECT_EQ(r.lastTopicId, 0);
    ASSERT_NE(reg.byName("characters"), nullptr); // registry updated on commit
}

TEST(AdminWorker, RejectsMalformedDescriptor) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"bad","",1,false,{0,1,2}});   // not a valid blob
    EXPECT_EQ(r.creates,0);
    EXPECT_EQ(r.appends,0);
    EXPECT_EQ(r.lastErr,23);                            // InvalidSchema
}

TEST(AdminWorker, DuplicateNameReturnsTopicExists) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"characters","userId",16,false,charsBlob()});
    r.lastErr=0xffff;
    w.onDefineTopic({1,3,"characters","userId",16,false,charsBlob()});
    EXPECT_EQ(r.lastErr,25);                            // TopicExists
    EXPECT_EQ(r.creates,1);                             // no second create
}

TEST(AdminWorker, NoValidatorPushWhenValidateOff) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"characters","userId",16,false,charsBlob()});
    EXPECT_EQ(r.pushes,0);
    EXPECT_EQ(r.lastErr,0);
}

TEST(AdminWorker, GetSchemaUnknownTopic) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onGetTopicSchema({1,2,"nope"});
    EXPECT_EQ(r.lastErr,7);                             // UnknownTopic
}

TEST(AdminWorker, GetSchemaKnownTopic) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    auto blob = charsBlob();
    w.onDefineTopic({1,2,"characters","userId",16,false,blob});
    ASSERT_EQ(r.lastErr,0);
    r.lastErr = 0xffff;
    w.onGetTopicSchema({1,2,"characters"});
    EXPECT_EQ(r.lastErr,0);  // OK
}

TEST(AdminWorker, AppendDurableFailureRespondsInternal) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make(/*commit=*/false));
    w.onDefineTopic({1,2,"characters","userId",16,false,charsBlob()});
    EXPECT_EQ(r.appends,1);
    EXPECT_EQ(r.lastErr,6);                // Internal
    // Registry must NOT have been updated (applyRecord only after commit)
    EXPECT_EQ(reg.byName("characters"), nullptr);
}
