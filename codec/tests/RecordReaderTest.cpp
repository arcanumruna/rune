#include <gtest/gtest.h>
#include <rune/codec/Record.h>
using namespace rune::codec;
// hand-frame a v1 record for {id:int64, userId:int64, strength:int32, name:string}
static std::vector<uint8_t> frame(int64_t id,int64_t uid,int32_t str,std::string_view name){
  std::vector<uint8_t> b; auto le=[&](auto v){ for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i))); };
  uint16_t block=20, ver=1; le(block); le(ver); le(id); le(uid); le(str);
  uint32_t nlen=uint32_t(name.size()); le(nlen); b.insert(b.end(),name.begin(),name.end()); return b;
}
TEST(RecordReader, ReadsFields) {
  auto d = DescriptorBuilder("c").add("id",PrimType::Int64).add("userId",PrimType::Int64)
             .add("strength",PrimType::Int32).add("name",PrimType::String).build();
  auto rec = frame(1001, 42, 7, "Bob");
  EXPECT_EQ(readField(d, rec, d.find("id")->id).i, 1001);
  EXPECT_EQ(readField(d, rec, d.find("userId")->id).i, 42);
  EXPECT_EQ(readField(d, rec, d.find("strength")->id).i, 7);
  EXPECT_EQ(readField(d, rec, d.find("name")->id).bytes, "Bob");
}
TEST(RecordReader, FieldNewerThanRecordVersionAbsent) {
  auto d = DescriptorBuilder("c").add("id",PrimType::Int64)
             .add("luck",PrimType::Int32,/*sinceVersion=*/2).build();
  // v1 record (block=8, ver=1, just id)
  std::vector<uint8_t> b; auto le=[&](auto v){for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i)));};
  uint16_t block=8,ver=1; le(block); le(ver); le(int64_t(5));
  EXPECT_TRUE(readField(d, b, d.find("id")->id).present);
  EXPECT_FALSE(readField(d, b, d.find("luck")->id).present);   // sinceVersion 2 > record v1
}
