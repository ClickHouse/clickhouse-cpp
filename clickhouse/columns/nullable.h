#pragma once

#include "column.h"
#include "numeric.h"

#include <optional>

namespace clickhouse {

/**
 * Represents column of Nullable(T).
 */
class ColumnNullable : public Column {
public:
    ColumnNullable(ColumnRef nested, ColumnRef nulls);

    /// Appends one null flag to the end of the column
    void Append(bool isnull);

    /// Returns null flag at given row number.
    bool IsNull(size_t n) const;

    /// Returns nested column.
    ColumnRef Nested() const;

    /// Returns nulls column.
    ColumnRef Nulls() const;

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream.
    void SavePrefix(OutputStream* output) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column&) override;

    ItemView GetItem(size_t) const override;

private:
    ColumnRef nested_;
    std::shared_ptr<ColumnUInt8> nulls_;
};

template <typename ColumnType>
class ColumnNullableT : public ColumnNullable, public WrappableColumn<ColumnNullableT<ColumnType>, ColumnNullable> {
public:
    using NestedColumnType = ColumnType;
    using ValueType = std::optional<std::decay_t<decltype(std::declval<NestedColumnType>().At(0))>>;

    ColumnNullableT(std::shared_ptr<NestedColumnType> data, std::shared_ptr<ColumnUInt8> nulls)
        : ColumnNullable(data, nulls)
        , typed_nested_data_(data)
    {}

    explicit ColumnNullableT(std::shared_ptr<NestedColumnType> data)
        : ColumnNullableT(data, FillNulls(data->Size()))
    {}

    template <typename ...Args>
    explicit ColumnNullableT(Args &&... args)
        : ColumnNullableT(std::make_shared<NestedColumnType>(std::forward<Args>(args)...))
    {}

    inline ValueType At(size_t index) const {
        return IsNull(index) ? ValueType{} : ValueType{typed_nested_data_->At(index)};
    }

    inline ValueType operator[](size_t index) const { return At(index); }

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override {
        ColumnNullable::Append(std::move(column));
    }

    inline void Append(ValueType value) {
        ColumnNullable::Append(!value.has_value());
        try {
            if (value.has_value()) {
                typed_nested_data_->Append(std::move(*value));
            } else {
                typed_nested_data_->Append(typename ValueType::value_type{});
            }
        } catch (...) {
            Nulls()->template As<ColumnUInt8>()->Erase(Size() - 1);
            throw;
        }
    }

    /** Create a ColumnNullableT that SHARES the internals of `col` (nested data and
     *  null map) via shared_ptr, WITHOUT stealing or copying them.
     *
     *  The original `col` remains fully valid and usable. Both the original and the
     *  returned wrapper reference the same underlying columns, so mutations through
     *  one are visible through the other.
     *
     *  The two-argument overloads are non-throwing: on a type mismatch they return
     *  nullptr and, if `error` is non-null, assign a description to `*error`. The
     *  single-argument overloads throw ValidationError on a type mismatch instead.
     */
    static std::shared_ptr<ColumnNullableT<NestedColumnType>> Wrap(const ColumnNullable& col, ValidationError* error) {
        auto nested = WrapColumn<NestedColumnType>(col.Nested(), error);
        if (!nested) {
            return nullptr;
        }
        auto nulls = col.Nulls()->As<ColumnUInt8>();
        if (!nulls) {
            if (error) {
                *error = ValidationError("Can't wrap Nullable column: unexpected null-map type");
            }
            return nullptr;
        }
        return std::make_shared<ColumnNullableT<NestedColumnType>>(nested, nulls);
    }

    static std::shared_ptr<ColumnNullableT<NestedColumnType>> Wrap(const Column& col, ValidationError* error) {
        if (auto* c = dynamic_cast<const ColumnNullable*>(&col)) {
            return Wrap(*c, error);
        }
        if (error) {
            *error = ValidationError("Can't wrap column of type " + col.GetType().GetName() + " as Nullable");
        }
        return nullptr;
    }

    // Helper to simplify integration with other APIs
    static std::shared_ptr<ColumnNullableT<NestedColumnType>> Wrap(const ColumnRef& col, ValidationError* error) {
        return Wrap(*col, error);
    }

    // Throwing single-argument overloads (concrete type / Column& / ColumnRef&).
    using WrappableColumn<ColumnNullableT<ColumnType>, ColumnNullable>::Wrap;

    ColumnRef Slice(size_t begin, size_t size) const override {
        return Wrap(ColumnNullable::Slice(begin, size));
    }

    ColumnRef CloneEmpty() const override { return Wrap(ColumnNullable::CloneEmpty()); }

    void Swap(Column& other) override {
        auto& col = dynamic_cast<ColumnNullableT<NestedColumnType>&>(other);
        // Base swaps sub-column contents in place, preserving object identity, so the cached
        // typed_nested_data_ still points at the correct object and must NOT be repointed.
        ColumnNullable::Swap(col);
    }

private:
    static inline auto FillNulls(size_t n){
        auto result = std::make_shared<ColumnUInt8>();
        for (size_t i = 0; i < n; ++i) {
            result->Append(0);
        }
        return result;
    }

    std::shared_ptr<NestedColumnType> typed_nested_data_;
};

template <typename T>
constexpr bool IsNullable = std::is_base_of_v<ColumnNullable, T>;

}
