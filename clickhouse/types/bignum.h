#pragma once

#include <cstdint>
#include <string>
#include <string_view>


 #if defined(__SIZEOF_INT128__)                                                                       
 #define CH_CPP_HAS_INT128 1                                                                  
 #else                                                                                                
 #define CH_CPP_HAS_INT128 0                                                                  
 #endif                                                                                               

/**
 * This file contains declarations and definitions of the types and API used for wide
 * (128-, 256- bit) integers. It first declares the Int128 and UInt128, implementation of which
 * depends on the mode in which the library is build, i.e. whether CH_USE_ABSEIL_FOR_BIGNUM is
 * enabled or disabled. Then the file provides common API that works for both types. This API
 * resides in the static Bignum class.
 *
 * Additionally for non-Abseil implementation of wide integers, the class declares `numeric_limits`
 * specialization and the `ostream << value` operator. Using this API the library can switch
 * between implementations of wide integers without changing anything in the library code itself.
 * However the API is very limited and does not provide any arithmetic operations.
 * Users must choose their own library to wrap the provided types (unless they use Abseil, which
 * will be deprecated in the near future).
 */

#if CH_USE_ABSEIL_FOR_BIGNUM

#include "absl/numeric/int128.h"

namespace clickhouse {

using Int128 = absl::int128;
using UInt128 = absl::uint128;

} // namespace clickhouse

#else

#include <cstddef>
#include <limits>
#include <ostream>
#include <type_traits>

namespace clickhouse {

struct UInt128 {

    UInt128() : limbs{0, 0} {}

    UInt128(uint64_t x) : limbs{x, 0} { }

    // Produces hi * 2^64 + lo
    UInt128(uint64_t hi, uint64_t lo) : limbs{lo, hi} {}

    bool operator==(const UInt128& other) const
    {
        return limbs[0] == other.limbs[0] && limbs[1] == other.limbs[1];
    }

    bool operator!=(const UInt128& other) const {
        return !(*this == other);
    }

    // Unsigned comparison: compare the high limb first, then the low limb.
    bool operator<(const UInt128& other) const {
        return limbs[1] != other.limbs[1] ? limbs[1] < other.limbs[1] : limbs[0] < other.limbs[0];
    }

    bool operator>(const UInt128& other) const { return other < *this; }
    bool operator<=(const UInt128& other) const { return !(other < *this); }
    bool operator>=(const UInt128& other) const { return !(*this < other); }

#if CH_CPP_HAS_INT128
    explicit operator unsigned __int128() const {
        return ((unsigned __int128)limbs[1] << 64 | limbs[0]);
    }
#endif

    // The value as two 64-bit limbs in little-endian order:
    // `limbs[0]` is the low 64 bits and `limbs[1]` is the high 64 bits
    uint64_t limbs[2];
};

struct Int128 {
    Int128() : limbs{0, 0} {}

    Int128(int64_t x) : limbs{static_cast<uint64_t>(x), 0} {
        if (x < 0) {
            limbs[1] = 0xFFFFFFFFFFFFFFFFUL;
        }
    }

    // Produces hi * 2^64 + lo
    Int128(int64_t hi, uint64_t lo) : limbs{lo, static_cast<uint64_t>(hi)} {}

    bool operator==(const Int128& other) const
    {
        return limbs[0] == other.limbs[0] && limbs[1] == other.limbs[1];
    }

    bool operator!=(const Int128& other) const {
        return !(*this == other);
    }

    // Signed comparison: the high limb is compared as signed (so the sign bit is
    // honored), the low limb as unsigned.
    bool operator<(const Int128& other) const {
        const int64_t hi = static_cast<int64_t>(limbs[1]);
        const int64_t other_hi = static_cast<int64_t>(other.limbs[1]);
        return hi != other_hi ? hi < other_hi : limbs[0] < other.limbs[0];
    }

    bool operator>(const Int128& other) const { return other < *this; }
    bool operator<=(const Int128& other) const { return !(other < *this); }
    bool operator>=(const Int128& other) const { return !(*this < other); }

#if CH_CPP_HAS_INT128
    explicit operator __int128() const {
        return (__int128)((unsigned __int128)limbs[1] << 64 | limbs[0]);
    }
#endif

    // The value as two 64-bit limbs in little-endian order:
    // `limbs[0]` is the low 64 bits and `limbs[1]` is the high 64 bits
    uint64_t limbs[2];
};

// The columns serialize/deserialize these types by copying their raw bytes
// straight to/from the wire (see ColumnVector<T>::SaveBody/LoadBody). That is
// only correct if the in-memory representation is exactly 16 contiguous bytes,
// trivially copyable, and laid out as little-endian (low limb first). These
// asserts guard that invariant so any accidental change to the layout fails to
// compile rather than silently corrupting the wire format.
static_assert(sizeof(UInt128) == 16 && alignof(UInt128) == 8,
              "UInt128 must be exactly 16 bytes with no padding for raw wire serialization");
static_assert(sizeof(Int128) == 16 && alignof(Int128) == 8,
              "Int128 must be exactly 16 bytes with no padding for raw wire serialization");
static_assert(std::is_trivially_copyable_v<UInt128>,
              "UInt128 must be trivially copyable for raw wire serialization");
static_assert(std::is_trivially_copyable_v<Int128>,
              "Int128 must be trivially copyable for raw wire serialization");

#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__)
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "The fallback Int128/UInt128 raw serialization only supports little-endian hosts");
#endif

} // namespace clickhouse

#endif

namespace clickhouse {

class Bignum {
public:

    static Int128 StringToInt128(std::string_view str);
    static UInt128 StringToUInt128(std::string_view str);

    static std::string Int128ToString(const Int128 x);
    static std::string UInt128ToString(const UInt128 x);


#if CH_USE_ABSEIL_FOR_BIGNUM

    static inline Int128 MakeInt128(int64_t hi, uint64_t lo) {
        return absl::MakeInt128(hi, lo);
    }

    static inline UInt128 MakeUInt128(uint64_t hi, uint64_t lo) {
        return absl::MakeUint128(hi, lo);
    }

    static inline uint64_t Int128Low64(Int128 v) {
        return absl::Int128Low64(v);
    }

    static inline int64_t Int128High64(Int128 v) {
        return absl::Int128High64(v);
    }

    static inline uint64_t UInt128Low64(UInt128 v) {
        return absl::Uint128Low64(v);
    }

    static inline uint64_t UInt128High64(UInt128 v) {
        return absl::Uint128High64(v);
    }

#else

    static inline Int128 MakeInt128(int64_t hi, uint64_t lo) {
        return Int128(hi, lo);
    }

    static inline UInt128 MakeUInt128(uint64_t hi, uint64_t lo) {
        return UInt128(hi, lo);
    }

    static inline uint64_t Int128Low64(Int128 v) {
        return v.limbs[0];
    }

    static inline int64_t Int128High64(Int128 v) {
        // unsigned-to-signed is implementation defined (as opposed to signed-to-unsigned, which is
        // well defined). However all major compilers (gcc, clang, msvc) guarantee that
        // unsigned-to-signed and signed-to-unsigned are reverse operations.
        return static_cast<int64_t>(v.limbs[1]);
    }

    static inline uint64_t UInt128Low64(UInt128 v) {
        return v.limbs[0];
    }

    static inline uint64_t UInt128High64(UInt128 v) {
        return v.limbs[1];
    }

#endif

};


} // namespace clickhouse

#if !CH_USE_ABSEIL_FOR_BIGNUM

namespace clickhouse {

// Stream the decimal representation of a signed 128-bit value.
std::ostream& operator<<(std::ostream& os, Int128 v);
// Stream the decimal representation of an unsigned 128-bit value.
std::ostream& operator<<(std::ostream& os, UInt128 v);

} // namespace clickhouse

namespace std {

// numeric_limits specialization for the unsigned 128-bit type.
template <>
class numeric_limits<clickhouse::UInt128> {
public:
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = false;
    static constexpr bool is_integer = true;
    static constexpr bool is_exact = true;
    static constexpr bool is_bounded = true;
    static constexpr bool is_modulo = true;
    static constexpr int digits = 128;
    static constexpr int digits10 = 38;
    static constexpr int radix = 2;

    // 2^128 - 1: every bit set.
    static clickhouse::UInt128 max() noexcept {
        return clickhouse::Bignum::MakeUInt128(0xFFFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL);
    }
    // 0.
    static clickhouse::UInt128 min() noexcept { return clickhouse::UInt128{}; }
    static clickhouse::UInt128 lowest() noexcept { return min(); }
};

// numeric_limits specialization for the signed 128-bit type.
template <>
class numeric_limits<clickhouse::Int128> {
public:
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = true;
    static constexpr bool is_integer = true;
    static constexpr bool is_exact = true;
    static constexpr bool is_bounded = true;
    static constexpr bool is_modulo = false;
    static constexpr int digits = 127;
    static constexpr int digits10 = 38;
    static constexpr int radix = 2;

    // 2^127 - 1: sign bit clear, all other bits set.
    static clickhouse::Int128 max() noexcept {
        return clickhouse::Bignum::MakeInt128(static_cast<int64_t>(0x7FFFFFFFFFFFFFFFLL), 0xFFFFFFFFFFFFFFFFULL);
    }
    // -2^127: only the sign bit set.
    static clickhouse::Int128 min() noexcept {
        return clickhouse::Bignum::MakeInt128(static_cast<int64_t>(0x8000000000000000ULL), 0x0ULL);
    }
    static clickhouse::Int128 lowest() noexcept { return min(); }
};

} // namespace std

#endif
