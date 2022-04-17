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

#include <gtest/gtest.h>

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


// based on https://stackoverflow.com/a/31207079
template <typename T, typename _ = void>
struct is_container : std::false_type {};

namespace details {
template <typename... Ts>
struct is_container_helper {};
}

// A very loose definition of container, nerfed to fit both C-array and ColumnArrayT<X>::ArrayWrapper
template <typename T>
struct is_container<
        T,
        std::conditional_t<
            false,
            details::is_container_helper<
                decltype(std::declval<T>().size()),
                decltype(std::begin(std::declval<T>())),
                decltype(std::end(std::declval<T>()))
                >,
            void
            >
        > : public std::true_type {};

template <typename T>
inline constexpr bool is_container_v = is_container<T>::value;

template <typename Container>
struct PrintContainer {
    const Container & container_;

    explicit PrintContainer(const Container& container)
        : container_(container)
    {}
};

template <typename T>
std::ostream& operator<<(std::ostream & ostr, const PrintContainer<T>& print_container) {
    ostr << "[";

    const auto & container = print_container.container_;
    for (auto i = std::begin(container); i != std::end(container); /*intentionally no ++i*/) {
        const auto & elem = *i;

        if constexpr (is_container_v<std::decay_t<decltype(elem)>>) {
            ostr << PrintContainer{elem};
        } else {
            ostr << elem;
        }

        if (++i != std::end(container)) {
            ostr << ", ";
        }
    }

    return ostr << "]";
}

// Compare values to each other, if values are container-ish, then recursively deep compare those containers.
template <typename Left, typename Right>
::testing::AssertionResult CompareRecursive(const Left & left, const Right & right);

// Compare containers element-wise, if elements are containers themselves - compare recursevely
template <typename LeftContainer, typename RightContainer>
::testing::AssertionResult CompareCotainersRecursive(const LeftContainer& left, const RightContainer& right) {
    if (left.size() != right.size())
        return ::testing::AssertionFailure() << "\nContainers size mismatch, expected: " << left.size() << " actual: " << right.size();

    auto l_i = std::begin(left);
    auto r_i = std::begin(right);

    for (size_t i = 0; i < left.size(); ++i, ++l_i, ++r_i) {
        auto result = CompareRecursive(*l_i, *r_i);
        if (!result)
            return result << "\n\nMismatching items at pos: " << i + 1;
    }

    return ::testing::AssertionSuccess();
}

template <typename Left, typename Right>
::testing::AssertionResult CompareRecursive(const Left & left, const Right & right) {
    if constexpr (is_container_v<Left> && is_container_v<Right>) {
        if (auto result = CompareCotainersRecursive(left, right))
            return result;
        else {
            return result << "\nExpected container: " << PrintContainer{left}
                          << "\nActual container  : " << PrintContainer{right};
        }
    } else {
        if (left != right)
            return ::testing::AssertionFailure()
                    << "\nexpected: " << left
                    << "\nactual  : " << right;

        return ::testing::AssertionSuccess();
    }
}

