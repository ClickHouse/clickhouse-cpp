#pragma once

#include "../types/types.h"

#include <sstream>
#include <stdexcept>
#include <type_traits>
#include "../base/string_view.h"

namespace clickhouse {

template <class T>
static typename std::enable_if<std::is_same<string_view, T>::value || std::is_same<std::string, T>::value,
    string_view>::type ConvertToStorageValue(const T& t) { return {t}; }

template <class T>
static typename std::enable_if<std::is_fundamental<T>::value, string_view>::type
    ConvertToStorageValue(const T& t) { return {reinterpret_cast<const char*>(&t), sizeof(T)}; }

/** ItemView is a view on a data stored in Column, safe-ish interface for reading values from Column.
 *
 * Data is not owned (hence the name View) and will be invalidated on column update, load
 * or destruction (basically on calling any non-const method of Column).
 * `type` reflects what is stored in `data` and can be almost any value-type
 * (except Nullable, Array, Tuple, LowCardinality).
 *
 */
struct ItemView {
    using DataType = string_view;

    const Type::Code type;
    const DataType data;

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

    template <class T>
    typename std::enable_if<
        std::is_same<string_view, T>::value ||
        std::is_same<std::string, T>::value, T>::type
    get() const { return data; }

    template <class T>
    typename std::enable_if<std::is_fundamental<T>::value>::type
    get() const {
        if (sizeof(T) == data.size()) {
            return *reinterpret_cast<const T*>(data.data());
        } else {
            throw std::runtime_error("Incompatitable value type and size.");
        }
    }

    inline string_view AsBinaryData() const {
        return data;
    }

    // Validate that value matches type, will throw an exception if validation fails.
    static void ValidateData(Type::Code type, DataType data);
};

}
