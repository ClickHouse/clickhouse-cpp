#pragma once

#include "column.h"
#include "numeric.h"

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>

namespace clickhouse
{

template <typename NestedColumnType>
class ColumnLowCardinalityWrapper;

namespace details
{

/** LowCardinalityHashKey used as key in unique items hashmap to abstract away key value
 * (type of which depends on dictionary column) and to reduce likelehood of collisions.
 *
 * In order to dramatically reduce collision rate, we use 2 different hashes from 2 different hash functions.
 * First hash is used in hashtable (to calculate item position).
 * Second one is used as part of key value and accessed via `operator==()` upon collision resolution/detection.
 */
using LowCardinalityHashKey = std::pair<std::uint64_t, std::uint64_t>;

struct LowCardinalityHashKeyHash {
    inline std::size_t operator()(const LowCardinalityHashKey &hash_key) const noexcept {
        return hash_key.first;
    }
};

}

class ColumnLowCardinality : public Column
{
public:
    using IndexColumn = std::variant<ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64>;
    using UniqueItems = std::unordered_map<details::LowCardinalityHashKey, size_t /*dictionary index*/, details::LowCardinalityHashKeyHash>;

    template <typename T>
    friend class ColumnLowCardinalityWrapper;

private:
    ColumnRef dictionary;
    IndexColumn index;
    UniqueItems unique_items_map;

//private:
//    struct ConstructWithValuesTag {};
//    ColumnLowCardinality(ColumnRef dictionary_column, ConstructWithValuesTag);

public:
    explicit ColumnLowCardinality(ColumnRef dictionary_column);
    ~ColumnLowCardinality();

    /// Appends another LowCardinality column to the end of this one, updating dictionary.
    void Append(ColumnRef /*column*/) override;

    /// Loads column data from input stream.
    bool Load(CodedInputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(CodedOutputStream* output) override;

    /// Clear column data.
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of current column, with compacted dictionary
    ColumnRef Slice(size_t begin, size_t len) override;

    void Swap(Column& other) override;
    ItemView GetItem(size_t index) const override;
    void AppendFrom(const Column& other, size_t index) override;

    size_t GetDictionarySize() const;
    TypeRef GetNestedType() const;

private:
    std::uint64_t getDictionaryIndex(std::uint64_t item_index) const;
    void appendIndex(std::uint64_t item_index);
    void removeLastIndex();

    ColumnRef GetDictionary();
    void AppendUnsafe(const ItemView &);

public:
    static details::LowCardinalityHashKey computeHashKey(const ItemView &);
};

/** Wrapper that provides convenience interface for accessing/appending individual items in LowCardinalityColumn.
 *
 * Can create an instance of ColumnLowCardinality with dictionary NestedColumnType dictionary column.
 * OR
 * Can wrap existing ColumnLowCardinality with proper dictionary type.
 *
 * Intentionaly not a subclass of Column since:
 * * it is expected to be instantiated on a stack, rather than wrapped in shared_ptr.
 * * to avoid confusion (and complexity) when casting to/from ColumnLowCardinality.
 * * to discourage putting ColumnLowCardinalityWrapper to a block.
 */
template <typename DictionaryColumnType>
class ColumnLowCardinalityWrapper
{
    std::shared_ptr<ColumnLowCardinality> lc_column;

public:
    using WrappedColumnType = DictionaryColumnType;
    // Type this column takes as argument of Append and returns with At() and operator[]
    using ValueType = typename DictionaryColumnType::ValueType;

    template <typename ...Args>
    explicit ColumnLowCardinalityWrapper(Args &&... args)
        : lc_column(std::make_shared<ColumnLowCardinality>(std::make_shared<DictionaryColumnType>(std::forward<Args>(args)...)))
    {}

    // Wrap existing ColumnLowCardinality, throw exception if dictionary type is not NestedColumnType.
    explicit ColumnLowCardinalityWrapper(ColumnRef low_cardinality_colum)
        : ColumnLowCardinalityWrapper(low_cardinality_colum->As<ColumnLowCardinality>())
    {}

    // Wrap existing ColumnLowCardinality, throw exception if dictionary type is not NestedColumnType.
    explicit ColumnLowCardinalityWrapper(std::shared_ptr<ColumnLowCardinality> low_cardinality_colum)
        : lc_column(low_cardinality_colum)
    {
        if (!low_cardinality_colum)
            throw std::runtime_error("Can't wrap nullptr ColumnLowCardinality");
        // casting via reference to throw exception on wrong type.
        (dynamic_cast<DictionaryColumnType&>(*lc_column->GetDictionary()));
    }

    /// Extended interface to simplify reading/adding individual items.

    /// Returns element at given row number.
    inline ValueType At(size_t n) const {
        return GetTypedDictionary()->At(lc_column->getDictionaryIndex(n));
    }

    /// Returns element at given row number.
    inline ValueType operator [] (size_t n) const {
        return (*GetTypedDictionary())[lc_column->getDictionaryIndex(n)];
    }

    inline void Append(const ValueType & value) {
        lc_column->AppendUnsafe(ItemView{value});
    }

    template <typename T>
    inline void AppendMany(const T& container) {
        for (const auto & item : container) {
            Append(item);
        }
    }

    inline size_t GetDictionarySize() const {
        return lc_column->GetDictionarySize();
    }

    inline ColumnRef GetLowCardinalityColumn() const {
        return lc_column;
    }

    // Column-like functions to simplify testing.
    inline TypeRef Type() const {
        return lc_column->Type();
    }

    inline void Clear() {
        lc_column->Clear();
    }

    inline size_t Size() const {
        return lc_column->Size();
    }

    inline bool Load(CodedInputStream* input, size_t rows) {
        return lc_column->Load(input, rows);
    }

    /// Saves column data to output stream.
    inline void Save(CodedOutputStream* output) {
        lc_column->Save(output);
    }

    inline ColumnRef Slice(size_t begin, size_t len) {
        return lc_column->Slice(begin, len);
    }

private:
    auto GetTypedDictionary()
    {
        return static_cast<DictionaryColumnType*>(lc_column->GetDictionary().get());
    }

    auto GetTypedDictionary() const
    {
        return static_cast<const DictionaryColumnType*>(lc_column->GetDictionary().get());
    }
};

}
