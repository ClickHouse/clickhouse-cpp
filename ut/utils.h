#pragma once

#include <clickhouse/base/platform.h>

#include <chrono>
#include <cstring>
#include <ostream>
#include <ratio>
#include <system_error>
#include <vector>
#include <type_traits>

#include <time.h>

namespace clickhouse {
    class Block;
}

template <typename ChronoDurationType>
struct Timer {
    using DurationType = ChronoDurationType;

    Timer()
        : started_at(Now())
    {}

    void Restart() {
        started_at = Now();
    }

    void Start() {
        Restart();
    }

    auto Elapsed() const {
        return std::chrono::duration_cast<ChronoDurationType>(Now() - started_at);
    }

private:
    static auto Now() {
        return std::chrono::high_resolution_clock::now().time_since_epoch();
    }

private:
    std::chrono::nanoseconds started_at;
};

template <typename R>
inline const char * getPrefix() {
    const char * prefix = "?";
    if constexpr (std::ratio_equal_v<R, std::nano>) {
        prefix = "n";
    } else if constexpr (std::ratio_equal_v<R, std::micro>) {
        prefix = "u";
    } else if constexpr (std::ratio_equal_v<R, std::milli>) {
        prefix = "m";
    } else if constexpr (std::ratio_equal_v<R, std::centi>) {
        prefix = "c";
    } else if constexpr (std::ratio_equal_v<R, std::deci>) {
        prefix = "d";
    } else if constexpr (std::ratio_equal_v<R, std::ratio<1, 1>>) {
        prefix = "";
    } else {
        static_assert("Unsupported ratio");
    }

    return prefix;
}

namespace std {
template <typename R, typename P>
inline ostream & operator<<(ostream & ostr, const chrono::duration<R, P> & d) {
    return ostr << d.count() << ::getPrefix<P>() << "s";
}
}

// Since result_of is deprecated in C++17, and invoke_result_of is unavailable until C++20...
template <class F, class... ArgTypes>
using my_result_of_t =
#if __cplusplus >= 201703L
    std::invoke_result_t<F, ArgTypes...>;
#else
    std::result_of_t<F(ArgTypes...)>;
#endif

template <typename MeasureFunc>
class MeasuresCollector {
public:
    using Result = my_result_of_t<MeasureFunc>;

    explicit MeasuresCollector(MeasureFunc && measurment_func, const size_t preallocate_results = 10)
        : measurment_func_(std::move(measurment_func))
    {
        results_.reserve(preallocate_results);
    }

    template <typename NameType>
    void Add(NameType && name) {
        results_.emplace_back(name, measurment_func_());
    }

    const auto & GetResults() const {
        return results_;
    }

private:
    MeasureFunc measurment_func_;
    std::vector<std::pair<std::string, Result>> results_;
};

template <typename MeasureFunc>
MeasuresCollector<MeasureFunc> collect(MeasureFunc && f) {
    return MeasuresCollector<MeasureFunc>(std::forward<MeasureFunc>(f));
}

struct in_addr;
struct in6_addr;
// Helper for pretty-printing of the Block
struct PrettyPrintBlock {
    const clickhouse::Block & block;
};

std::ostream& operator<<(std::ostream & ostr, const clickhouse::Block & block);
std::ostream& operator<<(std::ostream & ostr, const PrettyPrintBlock & block);
std::ostream& operator<<(std::ostream& ostr, const in_addr& addr);
std::ostream& operator<<(std::ostream& ostr, const in6_addr& addr);


template <typename ResultType = std::string>
auto getEnvOrDefault(const std::string& env, const char * default_val) {
    const char* v = std::getenv(env.c_str());
    if (!v && !default_val)
        throw std::runtime_error("Environment var '" + env + "' is not set.");

    const std::string value = v ? v : default_val;

    if constexpr (std::is_same_v<ResultType, std::string>) {
        return value;
    } else if constexpr (std::is_integral_v<ResultType>) {
        // since std::from_chars is not available on GCC-7  on linux
        if constexpr (std::is_signed_v<ResultType>) {
            if constexpr (sizeof(ResultType) <= sizeof(int))
                return std::stoi(value);
            else if constexpr (sizeof(ResultType) <= sizeof(long))
                return std::stol(value);
            else if constexpr (sizeof(ResultType) <= sizeof(long long))
                return std::stoll(value);
        } else if constexpr (std::is_unsigned_v<ResultType>) {
            if constexpr (sizeof(ResultType) <= sizeof(unsigned long))
                return std::stoul(value);
            else if constexpr (sizeof(ResultType) <= sizeof(unsigned long long))
                return std::stoull(value);
        }
    }
}
