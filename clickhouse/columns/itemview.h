#pragma once

#include "../types/types.h"

#include <string_view>
#include <variant>

namespace clickhouse {

/** ItemView is a view into a value stored in Column, provides stable interface for reading values from Column in safe manner.
 *
 * Data is not owned (hance the name View) and will be invalidated on column update, load or destruction (basically on calling any non-const method of Column).
 * `type` reflects what is stored in `data`: one of Type::Float64, Type::UInt64, Type::Int64, or Type::String.
 *
 */
struct ItemView {
    using DataType = std::variant<double, uint64_t, int64_t, std::string_view>;

    const Type::Code type;
    const DataType data;

private:
    template <typename X>
    static std::string_view BinaryDataFromValue(const X& t) {
        return std::string_view{reinterpret_cast<const char*>(&t), sizeof(X)};
    }

public:
    explicit ItemView()
        : type(Type::Void),
          data{static_cast<std::uint64_t>(0u)}
    {}

    template <typename T>
    explicit ItemView(const T & value)
        : ItemView(DataType(ConvertToStorageValue(value)))
    {}

    explicit ItemView(DataType data_type)
        : type(DeduceType(data_type)),
          data{std::move(data_type)}
    {}

    template <typename T>
    T get() const {
        return std::get<T>(data);
    }

    inline std::string_view AsBinaryData() const {
        return std::visit([](const auto & v) -> std::string_view {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, std::string_view>) {
                return v;
            }
            else {
                return BinaryDataFromValue(v);
            }
        }, data);
    }

private:
    inline static Type::Code DeduceType(const DataType & data) {
        static_assert(std::is_same_v<double, std::variant_alternative_t<0, DataType>>);
        static_assert(std::is_same_v<uint64_t, std::variant_alternative_t<1, DataType>>);
        static_assert(std::is_same_v<int64_t, std::variant_alternative_t<2, DataType>>);
        static_assert(std::is_same_v<std::string_view, std::variant_alternative_t<3, DataType>>);

        switch (data.index()) {
            case 0:
                return Type::Float64;
            case 1:
                return Type::UInt64;
            case 2:
                return Type::Int64;
            case 3:
                return Type::String;
        }
        return Type::Void;
    }

    template <typename T>
    inline auto ConvertToStorageValue(const T& t) {
        if constexpr (std::is_same_v<std::string_view, T> || std::is_same_v<std::string, T>) {
            return std::string_view{t};
        }
        else if constexpr (std::is_fundamental_v<T>) {
            if constexpr (std::is_integral_v<T>) {
                if constexpr (std::is_unsigned_v<T>)
                    return static_cast<uint64_t>(t);
                else
                    return static_cast<int64_t>(t);
            }
            else if constexpr (std::is_floating_point_v<T>) {
                return static_cast<double>(t);
            }
        }
    }
};

// A bit misleading but safe sugar.
template <>
inline std::string ItemView::get<std::string>() const {
    return std::string{std::get<std::string_view>(data)};
}
}
