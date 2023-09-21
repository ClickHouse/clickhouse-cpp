#pragma once

#include "utils_meta.h"

#include <clickhouse/base/socket.h> // for ipv4-ipv6 platform-specific stuff

#include <gtest/gtest.h>

#include <string_view>
#include <cstring>
#include <cmath>
#include <type_traits>

namespace clickhouse {
    class Block;
    class Column;
}

inline bool operator==(const in6_addr& left, const in6_addr& right) {
    return memcmp(&left, &right, sizeof(left)) == 0;
}

inline bool operator==(const in_addr& left, const in_addr& right) {
    return memcmp(&left, &right, sizeof(left)) == 0;
}

inline bool operator==(const in_addr & left, const uint32_t& right) {
    return memcmp(&left, &right, sizeof(left)) == 0;
}

inline bool operator==(const uint32_t& left, const in_addr& right) {
    return memcmp(&left, &right, sizeof(left)) == 0;
}

inline bool operator==(const in6_addr & left, const std::string_view & right) {
    return right.size() == sizeof(left) && memcmp(&left, right.data(), sizeof(left)) == 0;
}

inline bool operator==(const std::string_view & left, const in6_addr & right) {
    return left.size() == sizeof(right) && memcmp(left.data(), &right, sizeof(right)) == 0;
}

inline bool operator!=(const in6_addr& left, const in6_addr& right) {
    return !(left == right);
}

inline bool operator!=(const in_addr& left, const in_addr& right) {
    return !(left == right);
}

inline bool operator!=(const in_addr & left, const uint32_t& right) {
    return !(left == right);
}

inline bool operator!=(const uint32_t& left, const in_addr& right) {
    return !(left == right);
}

inline bool operator!=(const in6_addr & left, const std::string_view & right) {
    return !(left == right);
}

inline bool operator!=(const std::string_view & left, const in6_addr & right) {
    return !(left == right);
}

namespace details {
// Make a column a RO stl-like container
template <typename NestedColumnType>
struct ColumnAsContainerWrapper {
    const NestedColumnType& nested_col;

    struct Iterator {
        const NestedColumnType& nested_col;
        size_t i = 0;

        auto& operator++() {
            ++i;
            return *this;
        }

        auto operator*() const {
            return nested_col.At(i);
        }

        bool operator==(const Iterator & other) const {
            return &other.nested_col == &this->nested_col && other.i == this->i;
        }

        bool operator!=(const Iterator & other) const {
            return !(other == *this);
        }
    };

    size_t size() const {
        return nested_col.Size();
    }

    auto begin() const {
        return Iterator{nested_col, 0};
    }

    auto end() const {
        return Iterator{nested_col, nested_col.Size()};
    }
};
}

template <typename T>
auto maybeWrapColumnAsContainer(const T & t) {
    if constexpr (std::is_base_of_v<clickhouse::Column, T>) {
        return ::details::ColumnAsContainerWrapper<T>{t};
    } else {
        return t;
    }
}


// Compare values to each other, if values are container-ish, then recursively deep compare those containers.
template <typename Left, typename Right>
::testing::AssertionResult CompareRecursive(const Left & left, const Right & right);

// Compare containers element-wise, if elements are containers themselves - compare recursevely
template <typename LeftContainer, typename RightContainer>
::testing::AssertionResult CompareCotainersRecursive(const LeftContainer& left, const RightContainer& right) {
    if (left.size() != right.size())
        return ::testing::AssertionFailure() << "\nMismatching containers size, expected: " << left.size() << " actual: " << right.size();

    auto l_i = std::begin(left);
    auto r_i = std::begin(right);

    for (size_t i = 0; i < left.size(); ++i, ++l_i, ++r_i) {
        auto result = CompareRecursive(*l_i, *r_i);
        if (!result)
            return result << "\n\nMismatch at pos: " << i + 1;
    }

    return ::testing::AssertionSuccess();
}

template <typename Container>
struct PrintContainer;

template <typename Left, typename Right>
::testing::AssertionResult CompareRecursive(const Left & left, const Right & right) {
    if constexpr (!is_string_v<Left> && !is_string_v<Right>
            && (is_container_v<Left> || std::is_base_of_v<clickhouse::Column, std::decay_t<Left>>)
            && (is_container_v<Right> || std::is_base_of_v<clickhouse::Column, std::decay_t<Right>>) ) {

        const auto & l = maybeWrapColumnAsContainer(left);
        const auto & r = maybeWrapColumnAsContainer(right);

        if (auto result = CompareCotainersRecursive(l, r))
            return result;
        else
            return result << "\nExpected container: " << PrintContainer{l}
                          << "\nActual container  : " << PrintContainer{r};
    } else {
        if (left != right) {

            // Handle std::optional<float>(nan)
            // I'm too lazy to code comparison against std::nullopt, but this shpudn't be a problem in real life.
            // RN comparing against std::nullopt, you'll receive an compilation error.
            if constexpr (is_instantiation_of<std::optional, Left>::value && is_instantiation_of<std::optional, Right>::value)
            {
                if (left.has_value() && right.has_value())
                    return CompareRecursive(*left, *right);
            }
            else if constexpr (is_instantiation_of<std::optional, Left>::value) {
                if (left)
                    return CompareRecursive(*left, right);
            } else if constexpr (is_instantiation_of<std::optional, Right>::value) {
                if (right)
                    return CompareRecursive(left, *right);
            } else if constexpr (std::is_floating_point_v<Left> && std::is_floating_point_v<Right>) {
                if (std::isnan(left) && std::isnan(right))
                    return ::testing::AssertionSuccess();
            }

            return ::testing::AssertionFailure()
                    << "\nExpected value: " << left
                    << "\nActual value  : " << right;
        }

        return ::testing::AssertionSuccess();
    }
}
