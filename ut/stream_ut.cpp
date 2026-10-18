#include <clickhouse/base/compressed.h>
#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/base/wire_format.h>
#include <gtest/gtest.h>

#include "utils.h"

using namespace clickhouse;

namespace {

Buffer MakeCompressedBlockHeader(uint32_t compressed_size) {
    Buffer buffer;
    BufferOutput output(&buffer);

    uint8_t hash[16] = {};
    const uint8_t method = 0x82;
    const uint32_t original_size = 0;

    WireFormat::WriteFixed(output, hash);
    WireFormat::WriteFixed(output, method);
    WireFormat::WriteFixed(output, compressed_size);
    WireFormat::WriteFixed(output, original_size);
    output.Flush();

    return buffer;
}

}

TEST(CodedStreamCase, Varint64) {
    Buffer buf;

    {
        BufferOutput output(&buf);
        WireFormat::WriteVarint64(output, 18446744071965638648ULL);
        output.Flush();
    }

    {
        ArrayInput input(buf.data(), buf.size());
        uint64_t value = 0;
        ASSERT_TRUE(WireFormat::ReadVarint64(input, &value));
        ASSERT_EQ(value, 18446744071965638648ULL);
    }
}

TEST(CompressedInputCase, RejectsBlocksShorterThanHeader) {
    for (const uint32_t compressed_size : {0U, 1U, 8U}) {
        Buffer buffer = MakeCompressedBlockHeader(compressed_size);
        ArrayInput input(buffer.data(), buffer.size());
        CompressedInput compressed_input(&input);
        const void* data = nullptr;

        EXPECT_THROW(compressed_input.Next(&data, 1), CompressionError)
            << "compressed size: " << compressed_size;
    }
}

TEST(CodedStreamCase, ReadStringDoesNotAllocateDeclaredLength) {
    Buffer buffer;
    {
        BufferOutput output(&buffer);
        WireFormat::WriteUInt64(output, 0x00FFFFFFULL);
        WireFormat::WriteBytes(output, "abc", 3);
        output.Flush();
    }
    ArrayInput input(buffer.data(), buffer.size());
    std::string value;

    SKIP_UNLESS_LIBRARY_ALLOCATIONS_OBSERVED();
    LargestAllocationScope allocations;
    EXPECT_FALSE(WireFormat::ReadString(input, &value));
    EXPECT_LT(allocations.Get(), 4u * 1024 * 1024);
}

TEST(CodedStreamCase, ReadStringRoundTrip) {
    const std::string large(3 * 1024 * 1024 + 17, 'q');
    Buffer buffer;
    {
        BufferOutput output(&buffer);
        WireFormat::WriteString(output, "");
        WireFormat::WriteString(output, "name");
        WireFormat::WriteString(output, large);
        output.Flush();
    }
    ArrayInput input(buffer.data(), buffer.size());

    std::string value = "stale";
    ASSERT_TRUE(WireFormat::ReadString(input, &value));
    EXPECT_EQ(value, "");
    ASSERT_TRUE(WireFormat::ReadString(input, &value));
    EXPECT_EQ(value, "name");
    ASSERT_TRUE(WireFormat::ReadString(input, &value));
    EXPECT_EQ(value, large);
    EXPECT_TRUE(input.Exhausted());
}

TEST(CodedStreamCase, NextReadCapacityDoesNotOverflow) {
    constexpr uint32_t first = 1 << 20;
    EXPECT_EQ(WireFormat::NextReadCapacity<uint32_t>(0, 3, first), 3u);
    EXPECT_EQ(WireFormat::NextReadCapacity<uint32_t>(0, 1u << 30, first), first);
    EXPECT_EQ(WireFormat::NextReadCapacity<uint32_t>(first, 1u << 30, first), 8 * first);
    // loaded * 8 wraps in 32 bits from 2^29 on; the result must still cover the count.
    constexpr uint32_t count = (1u << 29) + 4096;
    EXPECT_EQ(WireFormat::NextReadCapacity<uint32_t>(1u << 29, count, first), count);
    EXPECT_EQ(WireFormat::NextReadCapacity<uint32_t>(count / 8 + 1, count, first), count);
    EXPECT_EQ(WireFormat::NextReadCapacity<uint64_t>(uint64_t(1) << 61, UINT64_MAX, 1), UINT64_MAX);
}
