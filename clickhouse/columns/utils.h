#pragma once

#include <algorithm>
#include <vector>
#include <memory>
#include "column.h"

namespace clickhouse {

template <typename T>
std::vector<T> SliceVector(const std::vector<T>& vec, size_t begin, size_t len) {
    std::vector<T> result;

    if (begin < vec.size()) {
        len = std::min(len, vec.size() - begin);
        result.assign(vec.begin() + begin, vec.begin() + (begin + len));
    }

    return result;
}

template <typename T>
struct HasWrapMethod {
private:
    static int detect(...);
    template <typename U>
    static decltype(U::Wrap(std::move(std::declval<ColumnRef>()))) detect(const U&);

public:
    static constexpr bool value = !std::is_same<int, decltype(detect(std::declval<T>()))>::value;
};

// Non-throwing: returns nullptr and (if `error` is non-null) fills `*error` when `column`
// can't be wrapped as T.
template <typename T>
inline std::shared_ptr<T> WrapColumn(const ColumnRef& column, ValidationError* error) {
    if constexpr (HasWrapMethod<T>::value) {
        return T::Wrap(column, error);
    } else {
        auto result = column->template As<T>();
        if (!result && error) {
            *error = ValidationError("Can't wrap column of type " + column->GetType().GetName());
        }
        return result;
    }
}

// Throwing convenience wrapper.
template <typename T>
inline std::shared_ptr<T> WrapColumn(const ColumnRef& column) {
    ValidationError error;
    auto result = WrapColumn<T>(column, &error);
    if (!result) {
        throw error;
    }
    return result;
}

}
