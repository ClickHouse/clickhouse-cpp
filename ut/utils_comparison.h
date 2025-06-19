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

    std::ostream& operator<<(std::ostream& ostr, const ItemView& item_view);
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

// Helper to allow comparing values of two instances of clickhouse::Column, when concrete type is unknown.
// Comparison is done by comparing result of Column::GetItem().
template <>
struct ColumnAsContainerWrapper<clickhouse::Column> {
    const clickhouse::Column& nested_col;

    struct Iterator {
        const clickhouse::Column& nested_col;
        size_t i = 0;

        auto& operator++() {
            ++i;
            return *this;
        }

        struct ItemWrapper {
            const clickhouse::ItemView item_view;

            bool operator==(const ItemWrapper & other) const {
                // type-erased comparison, byte-by-byte
                return item_view.type == other.item_view.type
                       && item_view.data == other.item_view.data;
            }

            template <typename U>
            bool operator==(const U & other) const {
                return item_view.get<U>() == other;
            }

            template <typename U>
            bool operator!=(const U & other) const {
                return !(*this == other);
            }

            friend std::ostream& operator<<(std::ostream& ostr, const ItemWrapper& val) {
                return ostr << val.item_view;
            }
        };

        auto operator*() const {
            return ItemWrapper{nested_col.GetItem(i)};
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
    using L = std::decay_t<Left>;
    using R = std::decay_t<Right>;

    if constexpr (!is_string_v<L> && !is_string_v<R>
            && (is_container_v<L> || std::is_base_of_v<clickhouse::Column, L>)
            && (is_container_v<R> || std::is_base_of_v<clickhouse::Column, R>) ) {

        const auto & l = maybeWrapColumnAsContainer(left);
        const auto & r = maybeWrapColumnAsContainer(right);

        if (auto result = CompareCotainersRecursive(l, r))
            return result;
        else
            return result << "\nExpected container: " << PrintContainer{l}
                          << "\nActual container  : " << PrintContainer{r};
    }
    else if constexpr (std::is_same_v<char const *, L> || std::is_same_v<char *, L>) {
        return CompareRecursive(std::string_view(left), right);
    }
    else if constexpr (std::is_same_v<char const *, R> || std::is_same_v<char *, R>) {
        return CompareRecursive(left, std::string_view(right));
    } else {
        if (left != right) {
            // Handle std::optional<float>(nan)
            // I'm too lazy to code comparison against std::nullopt, but this shouldn't be a problem in real life.
            // RN comparing against std::nullopt, you'll get a compilation error.
            if constexpr (is_instantiation_of<std::optional, L>::value && is_instantiation_of<std::optional, R>::value)
            {
                if (left.has_value() && right.has_value())
                    return CompareRecursive(*left, *right);
            }
            else if constexpr (is_instantiation_of<std::optional, L>::value) {
                if (left)
                    return CompareRecursive(*left, right);
            } else if constexpr (is_instantiation_of<std::optional, R>::value) {
                if (right)
                    return CompareRecursive(left, *right);
            } else if constexpr (std::is_floating_point_v<L> && std::is_floating_point_v<R>) {
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
