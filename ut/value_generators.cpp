#include "value_generators.h"

#include <algorithm>
#include <math.h>

namespace {
using namespace clickhouse;
}

std::vector<uint32_t> MakeNumbers() {
    return std::vector<uint32_t> {1, 2, 3, 7, 11, 13, 17, 19, 23, 29, 31};
}

std::vector<uint8_t> MakeBools() {
    return std::vector<uint8_t> {1, 0, 0, 0, 1, 1, 0, 1, 1, 1, 0};
}

std::vector<std::string> MakeFixedStrings(size_t string_size) {
    std::vector<std::string> result {"aaa", "bbb", "ccc", "ddd"};

    std::for_each(result.begin(), result.end(), [string_size](auto& value) {
        value.resize(string_size, '\0');
    });

    return result;
}

std::vector<std::string> MakeStrings() {
    return {"a", "ab", "abc", "abcd"};
}

std::vector<UUID> MakeUUIDs() {
    return {
        UUID(0llu, 0llu),
        UUID(0xbb6a8c699ab2414cllu, 0x86697b7fd27f0825llu),
        UUID(0x84b9f24bc26b49c6llu, 0xa03b4ab723341951llu),
        UUID(0x3507213c178649f9llu, 0x9faf035d662f60aellu)
    };
}

std::vector<Int64> MakeDateTime64s(size_t scale, size_t values_size) {
    const auto seconds_multiplier = static_cast<size_t>(std::pow(10, scale));
    const auto year = 86400ull * 365 * seconds_multiplier; // ~approx, but this doesn't matter here.

    // Approximatelly +/- 200 years around epoch (and value of epoch itself)
    // with non zero seconds and sub-seconds.
    // Please note there are values outside of DateTime (32-bit) range that might
    // not have correct string representation in CH yet,
    // but still are supported as Int64 values.
    return GenerateVector(values_size,
        [seconds_multiplier, year] (size_t i )-> Int64 {
            return (i - 100) * year * 2 + (i * 10) * seconds_multiplier + i;
        });
}

std::vector<clickhouse::Int64> MakeDates() {
    // in CH Date internally a UInt16 and stores a day number
    // ColumnDate expects values to be seconds, which is then
    // converted to day number internally, hence the `* 86400`.
    std::vector<clickhouse::Int64> result {0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 - 1};
    std::for_each(result.begin(), result.end(), [](auto& value) {
        value *= 86400;
    });

    return result;
}

std::vector<clickhouse::Int64> MakeDates32() {
    // in CH Date32 internally a UInt32 and stores a day number
    // ColumnDate expects values to be seconds, which is then
    // converted to day number internally, hence the `* 86400`.
    // 114634 * 86400 is 2282-11-10, last integer that fits into DateTime32 range
    // (max is 2283-11-11)
    std::vector<clickhouse::Int64> result  = MakeDates();

    // add corresponding negative values, since pre-epoch date are supported too.
    const auto size = result.size();
    for (size_t i = 0; i < size; ++i) {
        result.push_back(result[i] * -1);
    }

    return result;
}

std::vector<clickhouse::Int64> MakeDateTimes() {
    // in CH DateTime internally a UInt32
    return {
        0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
        131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864,
        134217728, 268435456, 536870912, 1073741824, 2147483648, 4294967296 - 1
    };
}

std::vector<clickhouse::Int128> MakeInt128s() {
    return {
        absl::MakeInt128(0xffffffffffffffffll, 0xffffffffffffffffll), // -1
        absl::MakeInt128(0, 0xffffffffffffffffll),  // 2^64
        absl::MakeInt128(0xffffffffffffffffll, 0),
        absl::MakeInt128(0x8000000000000000ll, 0),
        Int128(0)
    };
}

std::vector<clickhouse::Int128> MakeDecimals(size_t /*precision*/, size_t scale) {
    const auto scale_multiplier = static_cast<size_t>(std::pow(10, scale));
    const auto rhs_value = 12345678910;

    const std::vector<long long int> vals {0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 - 1};

    std::vector<clickhouse::Int128> result;
    result.reserve(vals.size());

    std::transform(vals.begin(), vals.end(), std::back_inserter(result), [scale_multiplier, rhs_value](const auto& value) {
        return value * scale_multiplier + rhs_value % scale_multiplier;
    });

    return result;
}

std::string FooBarGenerator(size_t i) {
    std::string result;
    if (i % 3 == 0)
        result += "Foo";
    if (i % 5 == 0)
        result += "Bar";
    if (result.empty())
        result = std::to_string(i);

    return result;
}

std::vector<in_addr> MakeIPv4s() {
    return {
        MakeIPv4(0x12345678), // 255.255.255.255
        MakeIPv4(0x0100007f), // 127.0.0.1
        MakeIPv4(3585395774),
        MakeIPv4(0),
        MakeIPv4(0x12345678),
    };
}

std::vector<in6_addr> MakeIPv6s() {
    return {
        MakeIPv6(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15), // 1:203:405:607:809:a0b:c0d:e0f
        MakeIPv6(0, 0, 0, 0, 0, 1),                                     // ::1
        MakeIPv6(0, 0, 0, 0, 0, 0),                                     // ::
        MakeIPv6(0xff, 0xff, 204, 152, 189, 116),                       // ::ffff:204.152.189.116
    };
}
