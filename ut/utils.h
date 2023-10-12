#pragma once

#include <clickhouse/base/platform.h>
#include <clickhouse/base/uuid.h>

#include "clickhouse/query.h"
#include "utils_meta.h"
#include "utils_comparison.h"

#include <iterator>
#include <optional>
#include <ostream>
#include <ratio>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>
#include <cmath>

#include <time.h>

#include <gtest/gtest.h>

namespace clickhouse {
    class Client;
    class Block;
    class Type;
    struct ServerInfo;
    struct Profile;
    struct QuerySettingsField;
    struct Progress;
}

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
                // For cases when ResultType is unsigned int.
                return static_cast<ResultType>(std::stoul(value));
            else if constexpr (sizeof(ResultType) <= sizeof(unsigned long long))
                return std::stoull(value);
        }
    }
}


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

template <typename T, size_t index = std::tuple_size_v<T> >
inline std::ostream & printTuple(std::ostream & ostr, [[maybe_unused]] const T & t) {
    static_assert(index <= std::tuple_size_v<T>);
    if constexpr (index == 0) {
        return ostr << "( ";
    } else {
        printTuple<T, index - 1>(ostr, t);
        using ElementType = std::tuple_element_t<index - 1, T>;
        if constexpr (is_container_v<ElementType>) {
            ostr << PrintContainer{std::get<index - 1>(t)};
        } else {
            ostr << std::get<0>(t);
        }
        if constexpr (index == std::tuple_size_v<T>) {
            return ostr << " )";
        } else {
            return ostr << ", ";
        }
    }
}

namespace std {
template <typename R, typename P>
inline ostream & operator<<(ostream & ostr, const chrono::duration<R, P> & d) {
    return ostr << d.count() << ::getPrefix<P>() << "s";
}

template <typename F, typename S>
inline ostream & operator<<(ostream & ostr, const pair<F, S> & t) {
    return ostr << "{ " << t.first << ", " << t.second << " }";
}

template <typename ... T>
inline ostream & operator<<(ostream & ostr, const tuple<T...> & t) {
    return printTuple(ostr, t);
}

template <typename T>
inline ostream & operator<<(ostream & ostr, const optional<T> & t) {
    if (t.has_value()) {
        return ostr << *t;
    } else {
        return ostr << "NULL";
    }
}
}


struct in_addr;
struct in6_addr;
// Helper for pretty-printing of the Block
struct PrettyPrintBlock {
    const clickhouse::Block & block;
};

namespace clickhouse {
std::ostream& operator<<(std::ostream & ostr, const Block & block);
std::ostream& operator<<(std::ostream & ostr, const Type & type);
std::ostream & operator<<(std::ostream & ostr, const ServerInfo & server_info);
std::ostream & operator<<(std::ostream & ostr, const Profile & profile);
std::ostream & operator<<(std::ostream & ostr, const Progress & progress);
}

std::ostream& operator<<(std::ostream & ostr, const PrettyPrintBlock & block);
std::ostream& operator<<(std::ostream& ostr, const in_addr& addr);
std::ostream& operator<<(std::ostream& ostr, const in6_addr& addr);


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

        if constexpr (is_string_v<decltype(elem)>) {
            ostr << '"' << elem << '"';
        }
        else if constexpr (is_container_v<std::decay_t<decltype(elem)>>) {
            ostr << PrintContainer{elem};
        } else {
            ostr << elem;
        }

        if (++i != std::end(container)) {
            ostr << ", ";
        }
    }

    return ostr << "] (" << std::size(container) << " items)";
}

inline uint64_t versionNumber(
        uint64_t version_major,
        uint64_t version_minor,
        uint64_t version_patch = 0,
        uint64_t revision = 0) {

    // in this case version_major can be up to 1000
    static auto revision_decimal_places = 8;
    static auto patch_decimal_places = 4;
    static auto minor_decimal_places = 4;

    auto const result = version_major * static_cast<uint64_t>(std::pow(10, minor_decimal_places + patch_decimal_places + revision_decimal_places))
            + version_minor * static_cast<uint64_t>(std::pow(10, patch_decimal_places + revision_decimal_places))
            + version_patch * static_cast<uint64_t>(std::pow(10, revision_decimal_places))
            + revision;

    return result;
}

uint64_t versionNumber(const clickhouse::ServerInfo & server_info);

std::string ToString(const clickhouse::UUID& v);
