#pragma once

#include <clickhouse/base/socket.h> // for ipv4-ipv6 platform-specific stuff
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/uuid.h>

#include "utils.h"

#include <vector>
#include <random>

inline in_addr MakeIPv4(uint32_t ip) {
    static_assert(sizeof(in_addr) == sizeof(ip));
    in_addr result;
    memcpy(&result, &ip, sizeof(ip));

    return result;
}

inline in6_addr MakeIPv6(uint8_t v0,  uint8_t v1,  uint8_t v2,  uint8_t v3,
                   uint8_t v4,  uint8_t v5,  uint8_t v6,  uint8_t v7,
                   uint8_t v8,  uint8_t v9,  uint8_t v10, uint8_t v11,
                   uint8_t v12, uint8_t v13, uint8_t v14, uint8_t v15) {
    return in6_addr{{{v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15}}};
}

inline in6_addr MakeIPv6(uint8_t v10, uint8_t v11, uint8_t v12, uint8_t v13, uint8_t v14, uint8_t v15) {
    return in6_addr{{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, v10, v11, v12, v13, v14, v15}}};
}

std::vector<uint32_t> MakeNumbers();
std::vector<uint8_t> MakeBools();
std::vector<std::string> MakeFixedStrings(size_t string_size);
std::vector<std::string> MakeStrings();
std::vector<clickhouse::Int64> MakeDateTime64s(size_t scale, size_t values_size = 200);
std::vector<int32_t> MakeDates32();
std::vector<clickhouse::Int64> MakeDateTimes();
std::vector<in_addr> MakeIPv4s();
std::vector<in6_addr> MakeIPv6s();
std::vector<clickhouse::UUID> MakeUUIDs();
std::vector<clickhouse::Int128> MakeInt128s();
std::vector<clickhouse::UInt128> MakeUInt128s();
std::vector<clickhouse::Int128> MakeDecimals(size_t precision, size_t scale);

template <typename T, std::enable_if_t<std::is_integral<T>::value, bool> = true>
inline std::vector<T> MakeNumbers() {

    std::vector<T> result;
    result.reserve(32);

    // to reach from in to max in 32 steps, it also has to be lower than 7 to work for int8 values.
    const T step = static_cast<T>(1) << (sizeof(T)*8 - 5);

    // `- step` to avoid accidential overflow
    for (T i = std::numeric_limits<T>::min(); i <= std::numeric_limits<T>::max() - step; i += step)
    {
        result.push_back(i);
    }
    result.push_back(std::numeric_limits<T>::max());

    return result;
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value, bool> = true>
inline std::vector<T> MakeNumbers() {

    std::vector<T> result {
        std::numeric_limits<T>::min(),
        std::numeric_limits<T>::max(),
        std::numeric_limits<T>::quiet_NaN(),
        std::numeric_limits<T>::infinity(),
        -std::numeric_limits<T>::infinity(),
        static_cast<T>(0),
        static_cast<T>(0) + std::numeric_limits<T>::epsilon(),
        static_cast<T>(0) - std::numeric_limits<T>::epsilon()
    };

    const auto total_steps = 100;
    const auto step = std::pow(10, (std::numeric_limits<T>::max_exponent - std::numeric_limits<T>::min_exponent) / total_steps);
    const auto min_value = std::pow(10, std::numeric_limits<T>::min_exponent10);

    // cover most of the precision ranges
    for (T i = std::numeric_limits<T>::max(); i >= min_value * step; i /= step)
    {
        result.push_back(i);
        result.push_back(-1 * i);
    }
    result.push_back(min_value);
    result.push_back(-min_value);

    return result;
}

template <size_t size>
inline std::vector<std::string> MakeFixedStrings() {
    return MakeFixedStrings(size);
}

template <size_t scale>
inline std::vector<clickhouse::Int64> MakeDateTime64s() {
    return MakeDateTime64s(scale);
}

template <size_t precision, size_t scale>
inline std::vector<clickhouse::Int128> MakeDecimals() {
    return MakeDecimals(precision, scale);
}


template <typename ResultType>
inline auto MakeDates() {
    std::vector<ResultType> result {0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 - 1};

    if constexpr (std::is_same_v<time_t, ResultType>) {
        // in CH Date internally a UInt16 and stores a day number
        // ColumnDate expects values to be seconds, which is then
        // converted to day number internally, hence the `* 86400`.
        std::for_each(result.begin(), result.end(), [](auto& value) {
            value *= 86400;
        });
    }

    return result;
}

template <typename ValueType, std::vector<ValueType> (*Generator)()>
inline auto MakeArrays() {
    const auto nested_values = Generator();
    std::vector<std::vector<ValueType>> result;
    result.reserve(nested_values.size());

    for (size_t i = 0; i < nested_values.size(); ++i)
    {
        result.emplace_back(nested_values.begin(), nested_values.begin() + i);
    }

    return result;
}


std::string FooBarGenerator(size_t i);

template <typename ValueType = void, typename Generator>
auto GenerateVector(size_t items, Generator && gen) {
    using ActualValueType = std::conditional_t<std::is_same_v<void, ValueType>, my_result_of_t<Generator, size_t>, ValueType>;
    std::vector<ActualValueType> result;
    result.reserve(items);
    for (size_t i = 0; i < items; ++i) {
        result.push_back(std::move(gen(i)));
    }

    return result;
}

template <typename T, typename U = T>
auto SameValueGenerator(const U & value) {
    return [&value](size_t) -> T {
        return value;
    };
}

template <typename ResultType, typename Generator1, typename Generator2>
auto AlternateGenerators(Generator1 && gen1, Generator2 && gen2) {
    return [&gen1, &gen2](size_t i) -> ResultType {
        if (i % 2 == 0)
            return gen1(i/2);
        else
            return gen2(i/2);
    };
}

template <typename T>
struct RandomGenerator {
    using uniform_distribution =
    typename std::conditional_t<std::is_floating_point_v<T>, std::uniform_real_distribution<T>,
            typename std::conditional_t<std::is_integral_v<T>, std::uniform_int_distribution<T>, void>>;

    explicit RandomGenerator(T seed = 0, T value_min = std::numeric_limits<T>::min(), T value_max = std::numeric_limits<T>::max())
        : random_engine(seed)
        , distribution(value_min, value_max)
    {
    }

    template <typename U>
    T operator()(U) {
        return distribution(random_engine);
    }

private:
    std::default_random_engine random_engine;
    uniform_distribution distribution;
};

template <typename T>
std::vector<T> ConcatSequences(std::vector<T> && vec1, std::vector<T> && vec2) {
    std::vector<T> result(vec1);

    result.reserve(vec1.size() + vec2.size());
    result.insert(result.end(), vec2.begin(), vec2.end());

    return result;
}

template <typename T>
struct FromVectorGenerator {
    const std::vector<T> data;
    RandomGenerator<size_t> random_generator;

    FromVectorGenerator(std::vector<T> data_)
        : data(std::move(data_)),
          random_generator(0, 0, data.size() - 1)
    {
        if (data.size() == 0)
            throw std::runtime_error("can't generate values from empty vector");
    }

    auto operator()(size_t pos) {
        return data[random_generator(pos)];
    }
};
