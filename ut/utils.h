#pragma once

#include <clickhouse/base/platform.h>

#include "utils_meta.h"
#include "utils_comparison.h"

#include <ostream>
#include <ratio>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

#include <time.h>

#include <gtest/gtest.h>

namespace clickhouse {
    class Client;
    class Block;
    class Type;
    struct ServerInfo;
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
                return std::stoul(value);
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

namespace std {
template <typename R, typename P>
inline ostream & operator<<(ostream & ostr, const chrono::duration<R, P> & d) {
    return ostr << d.count() << ::getPrefix<P>() << "s";
}

template <typename F, typename S>
inline ostream & operator<<(ostream & ostr, const pair<F, S> & t) {
    return ostr << "{ " << t.first << ", " << t.second << " }";
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
