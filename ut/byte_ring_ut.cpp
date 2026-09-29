#include <clickhouse/base/byte_ring.h>

#include <gtest/gtest.h>

using clickhouse::internal::ByteRing;

TEST(ByteRingCase, WraparoundReadWrite) {
    ByteRing ring(8);

    const std::uint8_t a[] = {1, 2, 3, 4, 5};
    ASSERT_EQ(ring.write(a, sizeof(a)), sizeof(a));
    ASSERT_EQ(ring.size(), sizeof(a));

    std::uint8_t out1[3] = {};
    ASSERT_EQ(ring.read(out1, sizeof(out1)), sizeof(out1));
    EXPECT_EQ(out1[0], 1);
    EXPECT_EQ(out1[1], 2);
    EXPECT_EQ(out1[2], 3);

    const std::uint8_t b[] = {6, 7, 8, 9, 10, 11};
    ASSERT_EQ(ring.write(b, sizeof(b)), sizeof(b));
    ASSERT_EQ(ring.size(), 2u + sizeof(b));  // remaining 4,5 plus new 6..11

    std::uint8_t out2[8] = {};
    ASSERT_EQ(ring.read(out2, sizeof(out2)), sizeof(out2));
    EXPECT_EQ(out2[0], 4);
    EXPECT_EQ(out2[1], 5);
    EXPECT_EQ(out2[2], 6);
    EXPECT_EQ(out2[3], 7);
    EXPECT_EQ(out2[4], 8);
    EXPECT_EQ(out2[5], 9);
    EXPECT_EQ(out2[6], 10);
    EXPECT_EQ(out2[7], 11);
    EXPECT_EQ(ring.size(), 0u);
}

TEST(ByteRingCase, CommitWriteClampedToSpan) {
    ByteRing ring(8);

    const std::uint8_t initial[] = {1, 2, 3, 4, 5, 6};
    ASSERT_EQ(ring.write(initial, sizeof(initial)), sizeof(initial));

    ring.consume_read(5);
    ASSERT_EQ(ring.size(), 1u);

    const auto span = ring.write_span();
    ASSERT_GT(span.size, 0u);
    ASSERT_LT(span.size, ring.available());

    span.data[0] = 0xAA;
    if (span.size > 1) {
        span.data[1] = 0xBB;
    }

    ring.commit_write(span.size + 1);
    EXPECT_EQ(ring.size(), 1u + span.size);
}
