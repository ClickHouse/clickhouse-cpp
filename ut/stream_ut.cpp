#include <clickhouse/base/wire_format.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/input.h>

#include <contrib/gtest/gtest.h>

using namespace clickhouse;

TEST(CodedStreamCase, Varint64) {
    Buffer buf;

    {
        BufferOutput output(&buf);
        WireFormat::WriteVarint64(&output, 18446744071965638648ULL);
    }

    {
        ArrayInput input(buf.data(), buf.size());
        uint64_t value;
        ASSERT_TRUE(WireFormat::ReadVarint64(&input, &value));
        ASSERT_EQ(value, 18446744071965638648ULL);
    }
}
