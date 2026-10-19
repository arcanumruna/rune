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

// M-4 (HARD RULE 5): adversarial, hostile-but-in-span records fed to readField.
// Every access in readField is bounds-checked against record.size(); these
// prove a malicious record yields an absent field and NEVER reads out of bounds.
static Descriptor advD() {
  return DescriptorBuilder("c").add("id",PrimType::Int64).add("userId",PrimType::Int64)
           .add("strength",PrimType::Int32).add("name",PrimType::String).build();
}

TEST(RecordReaderAdversarial, BlockLengthExceedsBuffer) {
  auto d = advD();
  // Declared blockLength = 0xFFFF, but buffer is tiny. The var section base is
  // 4 + 0xFFFF, far past the end. Fixed reads whose declared offset exceeds the
  // buffer must be absent; var read must be absent (pos > size).
  std::vector<uint8_t> b; auto le=[&](auto v){for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i)));};
  uint16_t block=0xFFFF, ver=1; le(block); le(ver); le(int64_t(1));  // only 8 payload bytes
  EXPECT_TRUE(readField(d, b, d.find("id")->id).present);            // id at offset 0 fits
  EXPECT_FALSE(readField(d, b, d.find("userId")->id).present);       // offset 8, sz 8 -> past end
  EXPECT_FALSE(readField(d, b, d.find("strength")->id).present);     // offset 16 -> past end
  EXPECT_FALSE(readField(d, b, d.find("name")->id).present);         // var base past end
}

TEST(RecordReaderAdversarial, VarLengthPrefixAllOnes) {
  auto d = advD();
  // Well-formed fixed block; the var length prefix is 0xFFFFFFFF. pos+len must
  // overflow the size check and the field must be reported absent (no OOB read).
  std::vector<uint8_t> b; auto le=[&](auto v){for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i)));};
  uint16_t block=20, ver=1; le(block); le(ver);
  le(int64_t(1)); le(int64_t(2)); le(int32_t(3));   // id,userId,strength
  le(uint32_t(0xFFFFFFFFu));                          // hostile var length prefix
  b.push_back('x');                                   // 1 byte of "data"
  EXPECT_TRUE(readField(d, b, d.find("strength")->id).present);
  EXPECT_FALSE(readField(d, b, d.find("name")->id).present);
}

TEST(RecordReaderAdversarial, TruncatedVarSegment) {
  auto d = advD();
  // Var length prefix claims 10 bytes but only 2 are present.
  std::vector<uint8_t> b; auto le=[&](auto v){for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i)));};
  uint16_t block=20, ver=1; le(block); le(ver);
  le(int64_t(1)); le(int64_t(2)); le(int32_t(3));
  le(uint32_t(10));                                   // claims 10 bytes
  b.push_back('h'); b.push_back('i');                 // only 2 present
  EXPECT_FALSE(readField(d, b, d.find("name")->id).present);
}
