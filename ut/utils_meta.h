#pragma once

#include <type_traits>
#include <array> // for std::begin
#include <string>
#include <string_view>

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

// https://stackoverflow.com/a/11251408
template < template <typename...> class Template, typename T >
struct is_instantiation_of : std::false_type {};

template < template <typename...> class Template, typename... Args >
struct is_instantiation_of< Template, Template<Args...> > : std::true_type {};


template <typename T>
inline constexpr bool is_string_v = std::is_same_v<std::string, std::decay_t<T>> || std::is_same_v<std::string_view, std::decay_t<T>>;

// https://stackoverflow.com/a/34111095
template <typename...>
struct is_one_of {
    static constexpr bool value = false;
};

template <typename F, typename S, typename... T>
struct is_one_of<F, S, T...> {
    static constexpr bool value =
        std::is_same<F, S>::value || is_one_of<F, T...>::value;
};

template <typename F, typename S, typename... T>
inline constexpr bool is_one_of_v = is_one_of<F, S, T...>::value;


#define HAS_METHOD(FUN)															            \
template <typename T, class U = void>														\
struct has_method_##FUN : std::false_type {};										        \
template <typename T>																		\
struct has_method_##FUN<T, std::enable_if_t<std::is_member_function_pointer_v<decltype(&T::FUN)>>> \
: std::true_type {};																		\
template <class T>																			\
constexpr bool has_method_##FUN##_v = has_method_##FUN<T>::value;

HAS_METHOD(Reserve);
HAS_METHOD(Capacity);
HAS_METHOD(GetWritableData);
