#pragma once

#include <type_traits>
#include <array> // for std::begin

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
            ::details::is_container_helper<
                decltype(std::declval<T>().size()),
                decltype(std::begin(std::declval<T>())),
                decltype(std::end(std::declval<T>()))
                >,
            void
            >
        > : public std::true_type {};

template <typename T>
inline constexpr bool is_container_v = is_container<T>::value;

// Since result_of is deprecated in C++17, and invoke_result_of is unavailable until C++20...
template <class F, class... ArgTypes>
using my_result_of_t =
#if __cplusplus >= 201703L
    std::invoke_result_t<F, ArgTypes...>;
#else
    std::result_of_t<F(ArgTypes...)>;
#endif

