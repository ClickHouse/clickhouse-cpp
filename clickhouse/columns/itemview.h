#pragma once

#include "../types/types.h"

#include <string_view>
#include <stdexcept>

namespace clickhouse {

/** ItemView is a view on a data stored in Column, safe-ish interface for reading values from Column.
 *
 * Data is not owned (hence the name View) and will be invalidated on column update, load
 * or destruction (basically on calling any non-const method of Column).
 * `type` reflects what is stored in `data` and can be almost any value-type
 * (except Nullable, Array, Tuple, LowCardinality).
 *
 */
struct ItemView {
    using DataType = std::string_view;

    const Type::Code type;
    const DataType data;

private:
    template <typename T>
    inline auto ConvertToStorageValue(const T& t) {
        if constexpr (std::is_same_v<std::string_view, T> || std::is_same_v<std::string, T>) {
            return std::string_view{t};
        } else if constexpr (std::is_fundamental_v<T>) {
            return std::string_view{reinterpret_cast<const char*>(&t), sizeof(T)};
        } else {
            // will caue error at compile-time
            return;
        }
    }

public:
    ItemView(Type::Code type, DataType data)
        : type(type),
          data(data)
    {
        ValidateData(type, data);
    }

    explicit ItemView()
        : ItemView(Type::Void, {nullptr, 0})
    {}

    template <typename T>
    explicit ItemView(Type::Code type, const T & value)
        : ItemView(type, ConvertToStorageValue(value))
    {}

    template <typename T>
    T get() const {
        if constexpr (std::is_same_v<std::string_view, T> || std::is_same_v<std::string, T>) {
            return data;
        } else if constexpr (std::is_fundamental_v<T>) {
            if (sizeof(T) == data.size()) {
                return *reinterpret_cast<const T*>(data.data());
            } else {
                throw std::runtime_error("Incompatitable value type and size.");
            }
        }
    }

    inline std::string_view AsBinaryData() const {
        return data;
    }

    // Validate that value matches type, will throw an exception if validation fails.
    static void ValidateData(Type::Code type, DataType data);
};

}
