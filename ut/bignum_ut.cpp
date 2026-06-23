#include <clickhouse/types/bignum.h>
#include <gtest/gtest.h>

#include <limits>

using clickhouse::Bignum;
using clickhouse::Int128;
using clickhouse::UInt128;

// These tests exercise the comparison operators for the wide integer types. Both are provided
// natively by Abseil and by the fallback implementation in bignum.h, so the tests are expected
// to pass in either mode.

// hi * 2^64 + lo
static Int128 I(int64_t hi, uint64_t lo) { return Bignum::MakeInt128(hi, lo); }
static UInt128 U(uint64_t hi, uint64_t lo) { return Bignum::MakeUInt128(hi, lo); }

TEST(BignumCompare, UInt128Equality) {
    EXPECT_TRUE(U(0, 0) == U(0, 0));
    EXPECT_TRUE(U(1, 2) == U(1, 2));
    EXPECT_FALSE(U(1, 2) == U(1, 3));
    EXPECT_FALSE(U(1, 2) == U(2, 2));

    EXPECT_TRUE(U(1, 2) != U(1, 3));
    EXPECT_FALSE(U(1, 2) != U(1, 2));
}

TEST(BignumCompare, UInt128OrderingLowLimb) {
    // Same high limb, ordering decided by the low limb.
    EXPECT_TRUE(U(5, 1) < U(5, 2));
    EXPECT_FALSE(U(5, 2) < U(5, 1));
    EXPECT_FALSE(U(5, 2) < U(5, 2));
}

TEST(BignumCompare, UInt128OrderingHighLimb) {
    // High limb dominates the comparison, even when the low limb would suggest otherwise.
    EXPECT_TRUE(U(1, 0xFFFFFFFFFFFFFFFFULL) < U(2, 0));
    EXPECT_FALSE(U(2, 0) < U(1, 0xFFFFFFFFFFFFFFFFULL));
}

TEST(BignumCompare, UInt128IsUnsigned) {
    // The high limb must be treated as unsigned: a value with the top bit set is
    // the largest, not the smallest.
    const UInt128 big = U(0x8000000000000000ULL, 0);
    EXPECT_TRUE(U(1, 0) < big);
    EXPECT_TRUE(U(0, 0) < big);
}

TEST(BignumCompare, UInt128DerivedOperators) {
    const UInt128 a = U(1, 1);
    const UInt128 b = U(1, 2);

    EXPECT_TRUE(b > a);
    EXPECT_FALSE(a > b);

    EXPECT_TRUE(a <= b);
    EXPECT_TRUE(a <= a);
    EXPECT_FALSE(b <= a);

    EXPECT_TRUE(b >= a);
    EXPECT_TRUE(b >= b);
    EXPECT_FALSE(a >= b);
}

TEST(BignumCompare, UInt128Limits) {
    const UInt128 min = std::numeric_limits<UInt128>::min();
    const UInt128 max = std::numeric_limits<UInt128>::max();

    EXPECT_EQ(min, U(0, 0));
    EXPECT_EQ(max, U(0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL));
    EXPECT_TRUE(min < max);
    EXPECT_TRUE(min <= U(1, 0));
    EXPECT_TRUE(U(1, 0) < max);
}

TEST(BignumCompare, Int128Equality) {
    EXPECT_TRUE(I(0, 0) == I(0, 0));
    EXPECT_TRUE(I(-1, 7) == I(-1, 7));
    EXPECT_FALSE(I(-1, 7) == I(-1, 8));
    EXPECT_TRUE(I(-1, 7) != I(0, 7));
}

TEST(BignumCompare, Int128OrderingLowLimb) {
    // Same (positive) high limb, ordering decided by the unsigned low limb.
    EXPECT_TRUE(I(5, 1) < I(5, 2));
    EXPECT_FALSE(I(5, 2) < I(5, 1));
}

TEST(BignumCompare, Int128IsSigned) {
    // Negative values must compare less than positive ones. A naive unsigned
    // comparison of the high limb would get this wrong.
    const Int128 neg = I(-1, 0);   // -2^64
    const Int128 pos = I(0, 1);    // 1
    EXPECT_TRUE(neg < pos);
    EXPECT_FALSE(pos < neg);

    EXPECT_TRUE(Int128(-1) < Int128(0));
    EXPECT_TRUE(Int128(-1) < Int128(1));
    EXPECT_TRUE(Int128(-2) < Int128(-1));
}

TEST(BignumCompare, Int128NegativeLowLimb) {
    // Among two negative values the unsigned low limb still breaks the tie.
    EXPECT_TRUE(Int128(-2) < Int128(-1));
    EXPECT_TRUE(I(-1, 1) < I(-1, 2));
    EXPECT_FALSE(I(-1, 2) < I(-1, 1));
}

TEST(BignumCompare, Int128DerivedOperators) {
    const Int128 a = I(-1, 0);
    const Int128 b = I(1, 0);

    EXPECT_TRUE(b > a);
    EXPECT_FALSE(a > b);

    EXPECT_TRUE(a <= b);
    EXPECT_TRUE(a <= a);
    EXPECT_FALSE(b <= a);

    EXPECT_TRUE(b >= a);
    EXPECT_TRUE(b >= b);
    EXPECT_FALSE(a >= b);
}

TEST(BignumCompare, Int128Limits) {
    const Int128 min = std::numeric_limits<Int128>::min();
    const Int128 max = std::numeric_limits<Int128>::max();

    EXPECT_EQ(min, I(static_cast<int64_t>(0x8000000000000000ULL), 0));
    EXPECT_EQ(max, I(0x7FFFFFFFFFFFFFFFLL, 0xFFFFFFFFFFFFFFFFULL));

    EXPECT_TRUE(min < max);
    EXPECT_TRUE(min < Int128(0));
    EXPECT_TRUE(Int128(0) < max);
    EXPECT_TRUE(min <= min);
    EXPECT_TRUE(max >= max);
}
