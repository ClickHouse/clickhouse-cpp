#include <clickhouse/base/coded.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

TEST(CodedStreamCase, Varint64) {
    Buffer buf;

    {
        BufferOutput output(&buf);
        CodedOutputStream coded(&output);
        coded.WriteVarint64(18446744071965638648ULL);
    }


    {
        ArrayInput input(buf.data(), buf.size());
        CodedInputStream coded(&input);
        uint64_t value;
        ASSERT_TRUE(coded.ReadVarint64(&value));
        ASSERT_EQ(value, 18446744071965638648ULL);
    }
}
