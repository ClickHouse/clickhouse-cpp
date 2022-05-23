#include "lowcardinality.h"

#include "string.h"
#include "nullable.h"
#include "../base/wire_format.h"

#include <cityhash/city.h>

#include <functional>
#include <string_view>
#include <type_traits>

#include <cassert>

namespace {
using namespace clickhouse;

enum KeySerializationVersion {
    SharedDictionariesWithAdditionalKeys = 1,
};

enum IndexType {
    UInt8 = 0,
    UInt16,
    UInt32,
    UInt64,
};

constexpr uint64_t IndexTypeMask = 0b11111111;

enum IndexFlag {
    /// Need to read dictionary if it wasn't.
    NeedGlobalDictionaryBit = 1u << 8u,
    /// Need to read additional keys. Additional keys are stored before indexes as value N and N keys after them.
    HasAdditionalKeysBit = 1u << 9u,
    /// Need to update dictionary. It means that previous granule has different dictionary.
    NeedUpdateDictionary = 1u << 10u
};

ColumnRef createIndexColumn(IndexType type) {
    switch (type) {
        case IndexType::UInt8:
            return std::make_shared<ColumnUInt8>();
        case IndexType::UInt16:
            return std::make_shared<ColumnUInt16>();
        case IndexType::UInt32:
            return std::make_shared<ColumnUInt32>();
        case IndexType::UInt64:
            return std::make_shared<ColumnUInt64>();
    }

    throw ValidationError("Invalid LowCardinality index type value: " + std::to_string(static_cast<uint64_t>(type)));
}

IndexType indexTypeFromIndexColumn(const Column & index_column) {
    switch (index_column.Type()->GetCode()) {
        case Type::UInt8:
            return IndexType::UInt8;
        case Type::UInt16:
            return IndexType::UInt16;
        case Type::UInt32:
            return IndexType::UInt32;
        case Type::UInt64:
            return IndexType::UInt64;
        default:
            throw ValidationError("Invalid index column type for LowCardinality column:" + index_column.Type()->GetName());
    }
}

template <typename ResultColumnType, typename ColumnType>
inline const ResultColumnType & column_down_cast(const ColumnType & c) {
    return dynamic_cast<const ResultColumnType &>(c);
}

template <typename ResultColumnType, typename ColumnType>
inline ResultColumnType & column_down_cast(ColumnType & c) {
    return dynamic_cast<ResultColumnType &>(c);
}

// std::visit-ish function to avoid including <variant> header, which is not present in older version of XCode.
template <typename Vizitor, typename ColumnType>
inline auto VisitIndexColumn(Vizitor && vizitor, ColumnType && col) {
    switch (col.Type()->GetCode()) {
        case Type::UInt8:
            return vizitor(column_down_cast<ColumnUInt8>(col));
        case Type::UInt16:
            return vizitor(column_down_cast<ColumnUInt16>(col));
        case Type::UInt32:
            return vizitor(column_down_cast<ColumnUInt32>(col));
        case Type::UInt64:
            return vizitor(column_down_cast<ColumnUInt64>(col));
        default:
            throw ValidationError("Invalid index column type " + col.GetType().GetName());
    }
}

// A special NULL-item, which is expected at pos(0) in dictionary,
// note that we distinguish empty string from NULL-value.
inline auto GetNullItemForDictionary(const ColumnRef dictionary) {
    if (auto n = dictionary->As<ColumnNullable>()) {
        return ItemView {};
    } else {
        return ItemView{dictionary->Type()->GetCode(), std::string_view{}};
    }
}

// A special default item, which is expected at pos(0) in dictionary,
// note that we distinguish empty string from NULL-value.
inline ItemView GetDefaultItemForDictionary(const ColumnRef dictionary) {
    if (auto n = dictionary->As<ColumnNullable>()) {
        return GetDefaultItemForDictionary(n->Nested());
    } else {
        return ItemView{dictionary->Type()->GetCode(), std::string_view{}};
    }
}

void AppendToDictionary(Column& dictionary, const ItemView & item);

inline void AppendNullableToDictionary(ColumnNullable& nullable, const ItemView & item) {
    auto nested = nullable.Nested();

    const bool isNullValue = item.type == Type::Void;

    if (isNullValue) {
        AppendToDictionary(*nested, GetNullItemForDictionary(nested));
    } else {
        const auto nestedType = nested->GetType().GetCode();
        if (nestedType != item.type) {
            throw ValidationError("Invalid value. Type expected: " + nested->GetType().GetName());
        }

        AppendToDictionary(*nested, item);
    }

    nullable.Append(isNullValue);
}

inline void AppendToDictionary(Column& dictionary, const ItemView & item) {
    switch (dictionary.GetType().GetCode()) {
        case Type::FixedString:
            column_down_cast<ColumnFixedString>(dictionary).Append(item.get<std::string_view>());
            return;
        case Type::String:
            column_down_cast<ColumnString>(dictionary).Append(item.get<std::string_view>());
            return;
        case Type::Nullable:
            AppendNullableToDictionary(column_down_cast<ColumnNullable>(dictionary), item);
            return;
        default:
            throw ValidationError("Unexpected dictionary column type: " + dictionary.GetType().GetName());
    }
}

}

namespace clickhouse {
ColumnLowCardinality::ColumnLowCardinality(ColumnRef dictionary_column)
    : Column(Type::CreateLowCardinality(dictionary_column->Type())),
      dictionary_column_(dictionary_column->CloneEmpty()), // safe way to get an column of the same type.
      index_column_(std::make_shared<ColumnUInt32>())
{
    Setup(dictionary_column);
}

ColumnLowCardinality::ColumnLowCardinality(std::shared_ptr<ColumnNullable> dictionary_column)
    : Column(Type::CreateLowCardinality(dictionary_column->Type())),
      dictionary_column_(dictionary_column->CloneEmpty()), // safe way to get an column of the same type.
      index_column_(std::make_shared<ColumnUInt32>())
{
    AppendNullItem();
    Setup(dictionary_column);
}

ColumnLowCardinality::~ColumnLowCardinality()
{}

void ColumnLowCardinality::Setup(ColumnRef dictionary_column) {
    AppendDefaultItem();

    if (dictionary_column->Size() != 0) {
        // Add values, updating index_column_ and unique_items_map_.

        // TODO: it would be possible to eliminate copying
        // by adding InsertUnsafe(pos, ItemView) method to a Column
        // (to insert null-item at pos 0),
        // but that is too much work for now.
        for (size_t i = 0; i < dictionary_column->Size(); ++i) {
            AppendUnsafe(dictionary_column->GetItem(i));
        }
    }
}

std::uint64_t ColumnLowCardinality::getDictionaryIndex(std::uint64_t item_index) const {
    return VisitIndexColumn([item_index](const auto & arg) -> std::uint64_t {
        return arg[item_index];
    }, *index_column_);
}

void ColumnLowCardinality::appendIndex(std::uint64_t item_index) {
    // TODO (nemkov): handle case when index should go from UInt8 to UInt16, etc.
    VisitIndexColumn([item_index](auto & arg) {
        arg.Append(item_index);
    }, *index_column_);
}

void ColumnLowCardinality::removeLastIndex() {
    VisitIndexColumn([](auto & arg) {
        arg.Erase(arg.Size() - 1);
    }, *index_column_);
}

details::LowCardinalityHashKey ColumnLowCardinality::computeHashKey(const ItemView & item) {
    static const auto hasher = std::hash<ItemView::DataType>{};
    if (item.type == Type::Void) {
        // to distinguish NULL of ColumnNullable and empty string.
        return {0u, 0u};
    }

    const auto hash1 = hasher(item.data);
    const auto hash2 = CityHash64(item.data.data(), item.data.size());

    return details::LowCardinalityHashKey{hash1, hash2};
}

ColumnRef ColumnLowCardinality::GetDictionary() {
    return dictionary_column_;
}

void ColumnLowCardinality::Append(ColumnRef col) {
    auto c = col->As<ColumnLowCardinality>();
    if (!c || !dictionary_column_->Type()->IsEqual(c->dictionary_column_->Type()))
        return;

    for (size_t i = 0; i < c->Size(); ++i) {
        AppendUnsafe(c->GetItem(i));
    }
}

namespace {

auto Load(ColumnRef new_dictionary_column, InputStream& input, size_t rows) {
    // This code tries to follow original implementation of ClickHouse's LowCardinality serialization with
    // NativeBlockOutputStream::writeData() for DataTypeLowCardinality
    // (see corresponding serializeBinaryBulkStateSuffix, serializeBinaryBulkStatePrefix, and serializeBinaryBulkWithMultipleStreams),
    // but with certain simplifications: no shared dictionaries, no on-the-fly dictionary updates.
    //
    // As for now those features are not used in client-server protocol and minimal implementation suffices,
    // however some day they may.

    uint64_t index_serialization_type;
    if (!WireFormat::ReadFixed(input, &index_serialization_type))
        throw ProtocolError("Failed to read index serializaton type.");

    auto new_index_column = createIndexColumn(static_cast<IndexType>(index_serialization_type & IndexTypeMask));
    if (index_serialization_type & IndexFlag::NeedGlobalDictionaryBit)
        throw UnimplementedError("Global dictionary is not supported.");

    if ((index_serialization_type & IndexFlag::HasAdditionalKeysBit) == 0)
        throw ValidationError("HasAdditionalKeysBit is missing.");

    uint64_t number_of_keys;
    if (!WireFormat::ReadFixed(input, &number_of_keys))
        throw ProtocolError("Failed to read number of rows in dictionary column.");

    auto dataColumn = new_dictionary_column;
    if (auto nullable = new_dictionary_column->As<ColumnNullable>()) {
        dataColumn = nullable->Nested();
    }

    if (!dataColumn->LoadBody(&input, number_of_keys))
        throw ProtocolError("Failed to read values of dictionary column.");

    uint64_t number_of_rows;
    if (!WireFormat::ReadFixed(input, &number_of_rows))
        throw ProtocolError("Failed to read number of rows in index column.");

    if (number_of_rows != rows)
        throw AssertionError("LowCardinality column must be read in full.");

    new_index_column->LoadBody(&input, number_of_rows);

    if (auto nullable = new_dictionary_column->As<ColumnNullable>()) {
        nullable->Append(true);
        for(std::size_t i = 1; i < new_index_column->Size(); i++) {
            nullable->Append(false);
        }
    }

    ColumnLowCardinality::UniqueItems new_unique_items_map;
    for (size_t i = 0; i < dataColumn->Size(); ++i) {
        const auto key = ColumnLowCardinality::computeHashKey(new_dictionary_column->GetItem(i));
        new_unique_items_map.emplace(key, i);
    }

    // suffix
    // NOP

    return std::make_tuple(new_dictionary_column, new_index_column, new_unique_items_map);
}

}

bool ColumnLowCardinality::LoadPrefix(InputStream* input, size_t) {
    uint64_t key_version;

    if (!WireFormat::ReadFixed(*input, &key_version))
        throw ProtocolError("Failed to read key serialization version.");

    if (key_version != KeySerializationVersion::SharedDictionariesWithAdditionalKeys)
        throw ProtocolError("Invalid key serialization version value.");

    return true;
}

bool ColumnLowCardinality::LoadBody(InputStream* input, size_t rows) {
    try {
        auto [new_dictionary, new_index, new_unique_items_map] = ::Load(dictionary_column_->CloneEmpty(), *input, rows);

        dictionary_column_->Swap(*new_dictionary);
        index_column_.swap(new_index);
        unique_items_map_.swap(new_unique_items_map);

        return true;
    } catch (...) {
        return false;
    }
}

void ColumnLowCardinality::SavePrefix(OutputStream* output) {
    const auto version = static_cast<uint64_t>(KeySerializationVersion::SharedDictionariesWithAdditionalKeys);
    WireFormat::WriteFixed(*output, version);
}

void ColumnLowCardinality::SaveBody(OutputStream* output) {
    const uint64_t index_serialization_type = indexTypeFromIndexColumn(*index_column_) | IndexFlag::HasAdditionalKeysBit;
    WireFormat::WriteFixed(*output, index_serialization_type);

    const uint64_t number_of_keys = dictionary_column_->Size();
    WireFormat::WriteFixed(*output, number_of_keys);

    if (auto columnNullable = dictionary_column_->As<ColumnNullable>()) {
        columnNullable->Nested()->SaveBody(output);
    } else {
        dictionary_column_->SaveBody(output);
    }

    const uint64_t number_of_rows = index_column_->Size();
    WireFormat::WriteFixed(*output, number_of_rows);

    index_column_->SaveBody(output);
}

void ColumnLowCardinality::Clear() {
    index_column_->Clear();
    dictionary_column_->Clear();
    unique_items_map_.clear();

    if (auto columnNullable = dictionary_column_->As<ColumnNullable>()) {
        AppendNullItem();
    }
    AppendDefaultItem();
}

size_t ColumnLowCardinality::Size() const {
    return index_column_->Size();
}

ColumnRef ColumnLowCardinality::Slice(size_t begin, size_t len) const {
    begin = std::min(begin, Size());
    len = std::min(len, Size() - begin);

    auto result = std::make_shared<ColumnLowCardinality>(dictionary_column_->CloneEmpty());

    for (size_t i = begin; i < begin + len; ++i)
        result->AppendUnsafe(this->GetItem(i));

    return result;
}

ColumnRef ColumnLowCardinality::CloneEmpty() const {
    return std::make_shared<ColumnLowCardinality>(dictionary_column_->CloneEmpty());
}

void ColumnLowCardinality::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnLowCardinality &>(other);
    if (!dictionary_column_->Type()->IsEqual(col.dictionary_column_->Type()))
        throw ValidationError("Can't swap() LowCardinality columns of different types.");

    // It is important here not to swap pointers to dictionary object,
    // but swap contents of dictionaries, so the object inside shared_ptr stays the same
    // (needed for ColumnLowCardinalityT)
    dictionary_column_->Swap(*col.dictionary_column_);

    index_column_.swap(col.index_column_);
    unique_items_map_.swap(col.unique_items_map_);
}

ItemView ColumnLowCardinality::GetItem(size_t index) const {
    const auto dictionaryIndex = getDictionaryIndex(index);

    if (auto nullable = dictionary_column_->As<ColumnNullable>()) {
        const auto isNull = dictionaryIndex == 0u;

        if (isNull) {
            return GetNullItemForDictionary(nullable);
        }
    }

    return dictionary_column_->GetItem(dictionaryIndex);
}

// No checks regarding value type or validity of value is made.
void ColumnLowCardinality::AppendUnsafe(const ItemView & value) {
    const auto key = computeHashKey(value);
    const auto initial_index_size = index_column_->Size();
    // If the value is unique, then we are going to append it to a dictionary, hence new index is Size().
    auto [iterator, is_new_item] = unique_items_map_.try_emplace(key, dictionary_column_->Size());
    try {
        // Order is important, adding to dictionary last, since it is much (MUCH!!!!) harder
        // to remove item from dictionary column than from index column
        // (also, there is currently no API to do that).
        // Hence in catch-block we assume that dictionary wasn't modified on exception
        // and there is nothing to rollback.

        appendIndex(iterator->second);
        if (is_new_item) {
            AppendToDictionary(*dictionary_column_, value);
        }
    }
    catch (...) {
        if (index_column_->Size() != initial_index_size)
            removeLastIndex();
        if (is_new_item)
            unique_items_map_.erase(iterator);

        throw;
    }
}

void ColumnLowCardinality::AppendNullItem()
{
    const auto null_item = GetNullItemForDictionary(dictionary_column_);
    AppendToDictionary(*dictionary_column_, null_item);
    unique_items_map_.emplace(computeHashKey(null_item), 0);
}

void ColumnLowCardinality::AppendDefaultItem()
{
    const auto defaultItem = GetDefaultItemForDictionary(dictionary_column_);
    unique_items_map_.emplace(computeHashKey(defaultItem), dictionary_column_->Size());
    AppendToDictionary(*dictionary_column_, defaultItem);
}

size_t ColumnLowCardinality::GetDictionarySize() const {
    return dictionary_column_->Size();
}

TypeRef ColumnLowCardinality::GetNestedType() const {
    return dictionary_column_->Type();
}

}
