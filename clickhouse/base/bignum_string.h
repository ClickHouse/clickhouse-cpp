#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

/**
 * This is low level api for converting large (128-, 256-, 512- bit) integers to strings.
 * It must not be imported in any of the header in the public API.
 */
namespace clickhouse::internal::bignum::string
{

static constexpr size_t bignum_max_limbs = 16;

// max pow 10 integer that fits in 32-bit unsigned integer
static constexpr uint32_t uint32_max_pow10 = 1000000000u;

static constexpr uint32_t pow_10[] = {
    1u,
    10u,
    100u,
    1000u,
    10000u,
    100000u,
    1000000u,
    10000000u,
    100000000u,
    1000000000u,
};

enum class StatusCode
{
    OK = 0,
    TOO_LARGE,
    BAD_ARGUMENT,
    BAD_STRING,
};

enum class SignMode
{
    UNSIGNED = 0,
    SIGNED = 1,
};

// returns the required string buffer size for a given limb count, or 0 if unsupported
constexpr inline uint8_t limb_arr_str_len(size_t size)
{
    switch (size) {
        case 4:
            return 41;
        case 8:
            return 79;
        case 16:
            return 156;
    }
    return 0;
}

// returns true if every limb is zero
inline bool limb_arr_is_zero(const uint32_t * limbs, size_t size)
{
    uint32_t ret = 0;
    for (size_t i = 0; i < size; ++i) {
        ret |= limbs[i];
    }
    return ret == 0;
}

// negates the limb array in place via two's-complement (invert and add one)
inline void limb_arr_negate_inplace(uint32_t * limbs, size_t size)
{
    uint32_t carry = 1;
    for (size_t i = 0; i < size; ++i) {
        uint32_t inverted = ~limbs[i];
        limbs[i] = inverted + carry;
        carry = (limbs[i] < carry) ? 1 : 0;
    }
}

// divides the limb array by divisor in place and returns the remainder
inline uint32_t limb_arr_divmod_inplace(uint32_t * limbs, size_t size, uint32_t divisor)
{
    uint32_t remainder = 0;
    for (size_t i = size; i-- > 0;) {
        uint64_t acc = ((uint64_t)remainder << 32) | limbs[i];
        limbs[i] = (uint32_t)(acc / divisor);
        remainder = (uint32_t)(acc % divisor);
    }
    return remainder;
}

// computes (limbs * mul + add) in place and returns the carry out of the top limb
inline uint32_t limb_arr_muladd_inplace(uint32_t * limbs, size_t size, uint32_t mul, uint32_t add)
{
    uint64_t carry = add;
    for (size_t i = 0; i < size; i++) {
        uint64_t acc = (uint64_t)limbs[i] * mul + carry;
        limbs[i] = (uint32_t)acc;
        carry = acc >> 32;
    }
    return (uint32_t)carry;
}

// returns true if the limbs equal the signed minimum (-2^(w-1)), i.e. only the top bit set
inline bool limb_arr_is_signed_min(const uint32_t * limbs, size_t size)
{
    if (size == 0) {
        return false;
    }

    if (limbs[size - 1] != 1UL << 31) {
        return false;
    }

    uint32_t acc = 0;
    for (size_t i = 0; i < size - 1; ++i) {
        acc |= limbs[i];
    }

    return acc == 0;
}

/**
 * @brief Parse a decimal string into a fixed-width little-endian limb array.
 *
 * Accepts an optional leading `-` (only when @p signed_type is true), followed
 * by one or more decimal digits (`0`-`9`). Leading zeros are accepted; `-0`
 * normalizes to canonical zero. No whitespace, internal/trailing signs, `+`,
 * non-ASCII bytes, or out-of-range digits are permitted.
 *
 * On success the parsed value is written to @p limbs as a two's-complement
 * little-endian array of 32-bit limbs. On any failure @p limbs may have been
 * partially modified and its contents are unspecified.
 *
 * @param str         Decimal string to parse. Not NUL-terminated; exactly
 *                    @p str_len bytes are read. Must not be NULL.
 * @param str_len     Number of bytes in @p str.
 * @param signed_type If true, the value is interpreted as signed two's
 *                    complement with range [-2^(w-1), 2^(w-1)-1] and a leading
 *                    `-` is allowed. If false, the value is unsigned with range
 *                    [0, 2^w-1] and a leading `-` is rejected.
 * @param limbs       Output array of @p limbs_len 32-bit limbs (little-endian).
 *                    Must not be NULL.
 * @param limbs_len   Number of 32-bit limbs. Only 4, 8, and 16 (128/256/512-bit)
 *                    are supported; any other value is rejected with BAD_ARGUMENT.
 *
 * @return BignumStatusCode::OK on success; BAD_ARGUMENT if @p str or @p limbs is
 *         NULL or @p limbs_len is unsupported; BAD_STRING if the input is malformed
 *         or empty; TOO_LARGE if the value overflows the target width.
 */
inline StatusCode str_to_limb_arr(
    const char * str, size_t str_len, SignMode mode, uint32_t * limbs, size_t limbs_len)
{
    if (str == NULL || limbs == NULL) {
        return StatusCode::BAD_ARGUMENT;
    }

    if (limb_arr_str_len(limbs_len) == 0) {
        return StatusCode::BAD_ARGUMENT;
    }

    bool negative = false;
    if (mode == SignMode::SIGNED && str_len > 0 && str[0] == '-') {
        ++str;
        --str_len;
        negative = true;
    }

    if (str_len == 0) {
        return StatusCode::BAD_STRING;
    }

    bool valid_chars = true;
    for (size_t i = 0; i < str_len; ++i) {
        valid_chars &= ('0' <= str[i] && str[i] <= '9');
    }
    if (!valid_chars) {
        return StatusCode::BAD_STRING;
    }

    for (size_t i = 0; i < limbs_len; ++i) {
        limbs[i] = 0;
    }

    size_t i = 0, first = str_len % 9; // leading short chunk, so the rest are full 9s
    if (first) {
        uint32_t chunk = 0;
        for (size_t k = 0; k < first; k++) {
            chunk = chunk * 10u + (uint32_t)(str[i++] - '0');
        }
        if (limb_arr_muladd_inplace(limbs, limbs_len, pow_10[first], chunk)) {
            return StatusCode::TOO_LARGE;
        }
    }
    while (i < str_len) {
        uint32_t chunk = 0;
        for (int k = 0; k < 9; k++) {
            chunk = chunk * 10u + (uint32_t)(str[i++] - '0');
        }
        if (limb_arr_muladd_inplace(limbs, limbs_len, uint32_max_pow10, chunk)) {
            return StatusCode::TOO_LARGE;
        }
    }

    if (mode == SignMode::SIGNED && !negative && (limbs[limbs_len - 1] & 1UL << 31)) {
        return StatusCode::TOO_LARGE;
    }

    if (negative && (limbs[limbs_len - 1] & 1UL << 31)
        && !limb_arr_is_signed_min(limbs, limbs_len)) {
        return StatusCode::TOO_LARGE;
    }

    if (negative) {
        limb_arr_negate_inplace(limbs, limbs_len);
    }

    return StatusCode::OK;
}

/**
 * @brief Format a fixed-width little-endian limb array as a decimal string.
 *
 * Writes the decimal representation into @p buf right-aligned against the end
 * of the buffer and returns a pointer to the first character of the (NUL-
 * terminated) result, which is generally *not* the start of @p buf. A negative
 * signed value is prefixed with `-`; zero formats as canonical `"0"`.
 *
 * Non-destructive: the function operates on an internal copy, so @p limbs is
 * left unchanged.
 *
 * @param limbs       Input array of @p limbs_len 32-bit limbs (little-endian,
 *                    two's complement). Must not be NULL.
 * @param limbs_len   Number of 32-bit limbs. Only 4, 8, and 16 (128/256/512-bit)
 *                    are supported; any other value causes failure.
 * @param signed_type If true, the value is interpreted as signed two's
 *                    complement (top bit denotes sign); if false, as unsigned.
 * @param buf         Output buffer the result is written into. Must not be NULL.
 * @param buf_len     Size of @p buf in bytes. Must be at least
 *                    `limb_arr_str_len(limbs_len)` (the NUL-inclusive required
 *                    size: 41/79/156 for 128/256/512-bit).
 *
 * @return Pointer into @p buf to the start of the formatted NUL-terminated
 *         string, or NULL if @p limbs or @p buf is NULL, @p limbs_len is
 *         unsupported, or @p buf_len is too small.
 */
inline char * limb_arr_to_str(
    const uint32_t * limbs, size_t limbs_len, SignMode mode, char * buf, size_t buf_len)
{
    if (buf == NULL || limbs == NULL) {
        return NULL;
    }

    if (limb_arr_str_len(limbs_len) == 0 || buf_len < limb_arr_str_len(limbs_len)) {
        return NULL; // buffer is too small or limbs_len is not supported
    }

    bool negative = false;
    char * it = buf + buf_len;
    *--it = '\0';

    if (limb_arr_is_zero(limbs, limbs_len)) {
        *--it = '0';
        return it;
    }

    uint32_t copy[bignum_max_limbs]; // assuming limbs_length is already checked above
    memcpy(copy, limbs, sizeof(*limbs) * limbs_len);

    if (mode == SignMode::SIGNED && (copy[limbs_len - 1] & 1UL << 31)) {
        negative = true;
        limb_arr_negate_inplace(copy, limbs_len);
    }

    uint32_t chunk = limb_arr_divmod_inplace(copy, limbs_len, uint32_max_pow10);
    while (!limb_arr_is_zero(copy, limbs_len)) {
        for (size_t i = 0; i < 9; ++i) {
            *--it = (char)('0' + (chunk % 10));
            chunk /= 10;
        }
        chunk = limb_arr_divmod_inplace(copy, limbs_len, uint32_max_pow10);
    }

    while (chunk != 0) {
        *--it = (char)('0' + (chunk % 10));
        chunk /= 10;
    }

    if (negative) {
        *--it = '-';
    }
    return it;
}
}
