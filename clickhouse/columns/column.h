#pragma once

#include "../types/types.h"
#include "../columns/itemview.h"
#include "../exceptions.h"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <vector>

namespace clickhouse {

class InputStream;
class OutputStream;

using ColumnRef = std::shared_ptr<class Column>;

/**
 * An abstract base of all columns classes.
 */
class Column : public std::enable_shared_from_this<Column> {
public:
    explicit inline Column(TypeRef type) : type_(type) {}

    virtual ~Column() {}

    /// Downcast pointer to the specific column's subtype.
    ///
    /// If T is a "wrappable" typed column (one exposing a static Wrap method, e.g.
    /// ColumnArrayT/ColumnTupleT/ColumnMapT/ColumnNullableT/ColumnLowCardinalityT) and the
    /// column is not already exactly T, this attempts to Wrap it as T (a storage-sharing
    /// typed view). Returns nullptr when neither an exact cast nor a wrap is possible.
    /// (Definitions are out-of-line below, after WrapColumn is declared.)
    template <typename T>
    inline std::shared_ptr<T> As();

    /// Const overload. Unlike the non-const As(), this does NOT wrap: it only performs an
    /// exact downcast and returns nullptr on mismatch (even for a wrappable T). Wrapping is
    /// intentionally disabled here because it would synthesize a mutable, storage-sharing
    /// view from a const column (requiring a const_cast), which is not const-correct.
    template <typename T>
    inline std::shared_ptr<const T> As() const;

    /// Like As(), but throws ValidationError instead of returning nullptr on failure.
    template <typename T>
    inline std::shared_ptr<T> AsStrict();

    /// Get type object of the column.
    inline TypeRef Type() const { return type_; }
    inline const class Type& GetType() const { return *type_; }

    /// Appends content of given column to the end of current one.
    virtual void Append(ColumnRef column) = 0;

    /// Increase the capacity of the column for large block insertion.
    virtual void Reserve(size_t new_cap) = 0;

    /// Template method to load column data from input stream. It'll call LoadPrefix and LoadBody.
    /// Should be called only once from the client. Derived classes should not call it.
    bool Load(InputStream* input, size_t rows);

    /// Loads column prefix from input stream.
    virtual bool LoadPrefix(InputStream* input, size_t rows);

    /// Loads column data from input stream.
    virtual bool LoadBody(InputStream* input, size_t rows) = 0;

    /// Saves column prefix to output stream. Column types with prefixes must implement it.
    virtual void SavePrefix(OutputStream* output);

    /// Saves column body to output stream.
    virtual void SaveBody(OutputStream* output) = 0;

    /// Template method to save to output stream. It'll call SavePrefix and SaveBody respectively
    /// Should be called only once from the client. Derived classes should not call it.
    /// Save is split in Prefix and Body because some data types require prefixes and specific serialization order.
    /// For instance, Array(LowCardinality(X)) requires LowCardinality.key_version bytes to come before Array.offsets
    void Save(OutputStream* output);

    /// Clear column data .
    virtual void Clear() = 0;

    /// Returns count of rows in the column.
    virtual size_t Size() const = 0;

    /// Makes slice of the current column.
    virtual ColumnRef Slice(size_t begin, size_t len) const = 0;

    virtual ColumnRef CloneEmpty() const = 0;

    virtual void Swap(Column&) = 0;

    /// Get a view on raw item data if it is supported by column, will throw an exception if index is out of range.
    /// Please note that view is invalidated once column items are added or deleted, column is loaded from strean or destroyed.
    virtual ItemView GetItem(size_t) const {
        throw UnimplementedError("GetItem() is not supported for column of " + type_->GetName());
    }

    friend void swap(Column& left, Column& right) {
        left.Swap(right);
    }

protected:
    TypeRef type_;
};

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

template <typename T>
inline std::shared_ptr<T> Column::As() {
    if constexpr (HasWrapMethod<T>::value) {
        if (auto exact = std::dynamic_pointer_cast<T>(shared_from_this())) {
            return exact;
        }
        return WrapColumn<T>(shared_from_this(), nullptr);
    } else {
        return std::dynamic_pointer_cast<T>(shared_from_this());
    }
}

template <typename T>
inline std::shared_ptr<const T> Column::As() const {
    // No wrapping for the const overload (see declaration): exact downcast only.
    return std::dynamic_pointer_cast<const T>(shared_from_this());
}

template <typename T>
inline std::shared_ptr<T> Column::AsStrict() {
    if constexpr (HasWrapMethod<T>::value) {
        if (auto exact = std::dynamic_pointer_cast<T>(shared_from_this())) {
            return exact;
        }
        return WrapColumn<T>(shared_from_this());
    } else {
        auto result = std::dynamic_pointer_cast<T>(shared_from_this());
        if (!result) {
            throw ValidationError("Can't cast from " + type_->GetName());
        }
        return result;
    }
}

}  // namespace clickhouse
