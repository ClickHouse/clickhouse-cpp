#pragma once

#include "column.h"

#include <tuple>
#include <vector>

namespace clickhouse {

/**
 * Represents column of Tuple([T]).
 */
class ColumnTuple : public Column {
public:
    ColumnTuple(const std::vector<ColumnRef>& columns);
    ColumnTuple(const std::vector<ColumnRef>& columns,
                std::vector<std::string> names);

    /// Returns count of columns in the tuple.
    size_t TupleSize() const;

    inline ColumnRef operator [] (size_t n) const {
        return columns_[n];
    }

    inline ColumnRef At(size_t n) const {
        return columns_[n];
    }

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
    ColumnRef Slice(size_t, size_t) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

private:
    std::vector<ColumnRef> columns_;
};

template <typename... Columns>
class ColumnTupleT : public ColumnTuple {
public:
    using TupleOfColumns = std::tuple<std::shared_ptr<Columns>...>;

    using ValueType = std::tuple<std::decay_t<decltype(std::declval<Columns>().At(0))>...>;

    ColumnTupleT(std::tuple<std::shared_ptr<Columns>...> columns)
        : ColumnTuple(TupleToVector(columns)), typed_columns_(std::move(columns)) {}

    ColumnTupleT(std::tuple<std::shared_ptr<Columns>...> columns, std::vector<std::string> names)
        : ColumnTuple(TupleToVector(columns), std::move(names)), typed_columns_(std::move(columns)) {}

    ColumnTupleT(std::vector<ColumnRef> columns)
        : ColumnTuple(columns), typed_columns_(VectorToTuple(std::move(columns))) {}

    ColumnTupleT(std::vector<ColumnRef> columns, std::vector<std::string> names)
        : ColumnTuple(columns, std::move(names)), typed_columns_(VectorToTuple(std::move(columns))) {}

    ColumnTupleT(const std::initializer_list<ColumnRef> columns)
        : ColumnTuple(columns), typed_columns_(VectorToTuple(std::move(columns))) {}

    ColumnTupleT(std::initializer_list<ColumnRef> columns, std::vector<std::string> names)
        : ColumnTuple(std::vector<ColumnRef>(columns), std::move(names))
        , typed_columns_(VectorToTuple(std::vector<ColumnRef>(columns))) {}

    inline ValueType At(size_t index) const { return GetTupleOfValues(index); }

    inline ValueType operator[](size_t index) const { return GetTupleOfValues(index); }

    using ColumnTuple::Append;

    template <typename... T>
    inline void Append(std::tuple<T...> value) {
        AppendTuple(std::move(value));
    }

    /** Create a ColumnTupleT that SHARES the internals of `col` (its element columns)
     *  via shared_ptr, WITHOUT stealing or copying them.
     *
     *  The original `col` remains fully valid and usable. Both the original and the
     *  returned wrapper reference the same underlying element columns, so mutations
     *  through one are visible through the other.
     *
     *  The two-argument overloads are non-throwing: on a type mismatch they return
     *  nullptr and, if `error` is non-null, assign a description to `*error`. The
     *  single-argument overloads throw ValidationError on a type mismatch instead.
     */
    static std::shared_ptr<ColumnTupleT<Columns...>> Wrap(const ColumnTuple& col, ValidationError* error) {
        if (col.TupleSize() != std::tuple_size_v<TupleOfColumns>) {
            if (error) {
                *error = ValidationError("Can't wrap from " + col.GetType().GetName());
            }
            return nullptr;
        }
        auto columns = TupleFromColumn(col, error);
        const bool all_wrapped = std::apply(
            [](const auto&... column) { return (... && static_cast<bool>(column)); }, columns);
        if (!all_wrapped) {
            return nullptr;
        }
        auto names = col.Type()->As<TupleType>()->GetItemNames();
        return std::make_shared<ColumnTupleT<Columns...>>(std::move(columns), std::move(names));
    }

    static std::shared_ptr<ColumnTupleT<Columns...>> Wrap(const Column& col, ValidationError* error) {
        if (auto* c = dynamic_cast<const ColumnTuple*>(&col)) {
            return Wrap(*c, error);
        }
        if (error) {
            *error = ValidationError("Can't wrap column of type " + col.GetType().GetName() + " as Tuple");
        }
        return nullptr;
    }

    // Helper to simplify integration with other APIs
    static std::shared_ptr<ColumnTupleT<Columns...>> Wrap(const ColumnRef& col, ValidationError* error) {
        return Wrap(*col, error);
    }

    static auto Wrap(const ColumnTuple& col) {
        ValidationError error;
        auto result = Wrap(col, &error);
        if (!result) {
            throw error;
        }
        return result;
    }

    static auto Wrap(const Column& col) {
        ValidationError error;
        auto result = Wrap(col, &error);
        if (!result) {
            throw error;
        }
        return result;
    }

    // Helper to simplify integration with other APIs
    static auto Wrap(const ColumnRef& col) {
        ValidationError error;
        auto result = Wrap(col, &error);
        if (!result) {
            throw error;
        }
        return result;
    }

    ColumnRef Slice(size_t begin, size_t size) const override {
        return Wrap(ColumnTuple::Slice(begin, size));
    }

    ColumnRef CloneEmpty() const override { return Wrap(ColumnTuple::CloneEmpty()); }

    void Swap(Column& other) override {
        auto& col = dynamic_cast<ColumnTupleT<Columns...>&>(other);
        typed_columns_.swap(col.typed_columns_);
        ColumnTuple::Swap(other);
    }

private:
    template <typename T, size_t index = std::tuple_size_v<T>>
    inline void AppendTuple([[maybe_unused]] T value) {
        static_assert(index <= std::tuple_size_v<T>);
        static_assert(std::tuple_size_v<TupleOfColumns> == std::tuple_size_v<T>);
        if constexpr (index == 0) {
            return;
        } else {
            std::get<index - 1>(typed_columns_)->Append(std::move(std::get<index - 1>(value)));
            AppendTuple<T, index - 1>(std::move(value));
        }
    }

    template <typename T, size_t index = std::tuple_size_v<T>>
    inline static std::vector<ColumnRef> TupleToVector([[maybe_unused]] const T& value) {
        static_assert(index <= std::tuple_size_v<T>);
        if constexpr (index == 0) {
            std::vector<ColumnRef> result;
            result.reserve(std::tuple_size_v<T>);
            return result;
        } else {
            auto result = TupleToVector<T, index - 1>(value);
            result.push_back(std::get<index - 1>(value));
            return result;
        }
    }

    // Builds a tuple of the element columns wrapped as their typed counterparts. Any element
    // that can't be wrapped is left as a null shared_ptr (and `*error` is set if provided).
    template <size_t column_index = std::tuple_size_v<TupleOfColumns>>
    inline static auto TupleFromColumn([[maybe_unused]] const ColumnTuple& col,
                                       [[maybe_unused]] ValidationError* error) {
        static_assert(column_index <= std::tuple_size_v<TupleOfColumns>);
        if constexpr (column_index == 0) {
            return std::make_tuple();
        } else {
            using ColumnType =
                typename std::tuple_element<column_index - 1, TupleOfColumns>::type::element_type;
            auto column = WrapColumn<ColumnType>(col[column_index - 1], error);
            return std::tuple_cat(TupleFromColumn<column_index - 1>(col, error),
                                  std::make_tuple(std::move(column)));
        }
    }

    template <typename T, size_t column_index = std::tuple_size_v<TupleOfColumns>>
    inline static auto VectorToTuple([[maybe_unused]] T columns) {
        static_assert(column_index <= std::tuple_size_v<TupleOfColumns>);
        if constexpr (column_index == 0) {
            return std::make_tuple();
        } else {
            using ColumnType =
                typename std::tuple_element<column_index - 1, TupleOfColumns>::type::element_type;
            auto column = WrapColumn<ColumnType>(columns[column_index - 1]);
            return std::tuple_cat(std::move(VectorToTuple<T, column_index - 1>(std::move(columns))),
                                  std::make_tuple(std::move(column)));
        }
    }

    template <size_t column_index = std::tuple_size_v<TupleOfColumns>>
    inline auto GetTupleOfValues([[maybe_unused]]size_t index) const {
        static_assert(column_index <= std::tuple_size_v<TupleOfColumns>);
        if constexpr (column_index == 0) {
            return std::make_tuple();
        } else {
            return std::tuple_cat(
                std::move(GetTupleOfValues<column_index - 1>(index)),
                std::move(std::make_tuple(std::get<column_index - 1>(typed_columns_)->At(index))));
        }
    }

    TupleOfColumns typed_columns_;
};

}  // namespace clickhouse
