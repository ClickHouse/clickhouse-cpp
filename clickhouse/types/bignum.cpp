#include "bignum.h"

#include "clickhouse/base/bignum_string.h"
#include "clickhouse/exceptions.h"

#include <array>

using clickhouse::internal::bignum::string::limb_arr_to_str;
using clickhouse::internal::bignum::string::limb_arr_str_len;
using clickhouse::internal::bignum::string::str_to_limb_arr;
using clickhouse::internal::bignum::string::SignMode;
using clickhouse::internal::bignum::string::StatusCode;
using clickhouse::ValidationError;

namespace {

std::array<uint32_t, 4> Int64LimbsToInt32(std::array<uint64_t, 2> limbs)
{
    std::array<uint32_t, 4> ret{0};
    ret[0] = static_cast<uint32_t>(limbs[0] & 0xFFFFFFFFUL);
    ret[1] = static_cast<uint32_t>(limbs[0] >> 32);
    ret[2] = static_cast<uint32_t>(limbs[1] & 0xFFFFFFFFUL);
    ret[3] = static_cast<uint32_t>(limbs[1] >> 32);
    return ret;
}

std::array<uint64_t, 2> Int32LimbsToInt64(std::array<uint32_t, 4> limbs)
{
    std::array<uint64_t, 2> ret{0};
    ret[0] = ((uint64_t)limbs[0] | (uint64_t)limbs[1] << 32);
    ret[1] = ((uint64_t)limbs[2] | (uint64_t)limbs[3] << 32);
    return ret;
}

template <size_t W>
std::string Int64LimbsToStr(std::array<uint64_t, W> limbs, SignMode mode) {
    auto limbs_32 = Int64LimbsToInt32(limbs);
    constexpr size_t buffer_size = limb_arr_str_len(limbs_32.size());
    static_assert(buffer_size > 0);
    std::array<char, buffer_size> buffer{0};
    char * str = limb_arr_to_str(limbs_32.data(), limbs_32.size(), mode, buffer.data(), buffer.size());
    return str;
}

template <size_t W>
std::array<uint64_t, W> StringToInt64Limbs(std::string_view str, SignMode mode) {
    std::array<uint32_t, W * 2> limbs{0};
    auto res = str_to_limb_arr(str.data(), str.size(), mode, limbs.data(), limbs.size());
    switch (res) {
        case StatusCode::OK:
            break;
        case StatusCode::TOO_LARGE:
            throw ValidationError("string \"" + std::string(str) + "\" is too big for 128-bit integer");
        case StatusCode::BAD_ARGUMENT:
        case StatusCode::BAD_STRING:
            throw ValidationError("cannot convert string \"" + std::string(str) + "\" to 128-bit integer");
            break;
    }
    return Int32LimbsToInt64(limbs);
}

} // anonymous namespace

namespace clickhouse {

std::string Bignum::Int128ToString(const Int128 x)
{
    std::array<uint64_t, 2> limbs{Bignum::Int128Low64(x), (uint64_t)(Bignum::Int128High64(x))};
    return Int64LimbsToStr(limbs, SignMode::SIGNED);
}

std::string Bignum::UInt128ToString(const UInt128 x)
{
    std::array<uint64_t, 2> limbs{Bignum::UInt128Low64(x), Bignum::UInt128High64(x)};
    return Int64LimbsToStr(limbs, SignMode::UNSIGNED);
}

Int128 Bignum::StringToInt128(std::string_view str) {
    auto ret = StringToInt64Limbs<2>(str, SignMode::SIGNED);
    return MakeInt128((int64_t)ret[1], ret[0]);
}

UInt128 Bignum::StringToUInt128(std::string_view str) {
    auto ret = StringToInt64Limbs<2>(str, SignMode::UNSIGNED);
    return MakeUInt128(ret[1], ret[0]);
}

} // namespace clickhouse

#if !CH_USE_ABSEIL_FOR_BIGNUM

namespace clickhouse {

std::ostream& operator<<(std::ostream& os, Int128 v)
{
    os << Bignum::Int128ToString(v);
    return os;
}

std::ostream& operator<<(std::ostream& os, UInt128 v)
{
    os << Bignum::UInt128ToString(v);
    return os;
}

}

#endif
