#pragma once

#include "../base/coded.h"
#include "../base/input.h"
#include "../types/types.h"

#include <variant>

namespace clickhouse {

using ColumnRef = std::shared_ptr<class Column>;

struct ItemView {
    using DataType = std::variant<double, uint64_t, int64_t, std::string_view>;

    const Type::Code type;
    const DataType data;

public:
    explicit ItemView()
        : type(Type::Void),
          data{static_cast<std::uint64_t>(0u)}
    {}

//    template <typename T>
//    ItemView(Type::Code type, T value)
//        : type(type),
//          data{value}
//    {}

//    struct DeduceTypeTag {};

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
    inline static std::string_view BinaryDataFromValue(const T& t) {
        return std::string_view{reinterpret_cast<const char*>(&t), sizeof(T)};
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

/**
 * An abstract base of all columns classes.
 */
class Column : public std::enable_shared_from_this<Column> {
public:
    explicit inline Column(TypeRef type) : type_(type) {}

    virtual ~Column() {}

    /// Downcast pointer to the specific column's subtype.
    template <typename T>
    inline std::shared_ptr<T> As() {
        return std::dynamic_pointer_cast<T>(shared_from_this());
    }

    /// Downcast pointer to the specific column's subtype.
    template <typename T>
    inline std::shared_ptr<const T> As() const {
        return std::dynamic_pointer_cast<const T>(shared_from_this());
    }

    /// Get type object of the column.
    inline TypeRef Type() const { return type_; }

    /// Appends content of given column to the end of current one.
    virtual void Append(ColumnRef column) = 0;

    /// Loads column data from input stream.
    virtual bool Load(CodedInputStream* input, size_t rows) = 0;

    /// Saves column data to output stream.
    virtual void Save(CodedOutputStream* output) = 0;

    /// Clear column data .
    virtual void Clear() = 0;

    /// Returns count of rows in the column.
    virtual size_t Size() const = 0;

    /// Makes slice of the current column.
    virtual ColumnRef Slice(size_t begin, size_t len) = 0;

    virtual void Swap(Column&) = 0;

    /// Get a view on raw item data if it is supported by column, will throw an exception if index is out of range.
    /// Please note that view is invalidated once column is items are added or deleted, column is loaded from strean or destroyed.
    virtual ItemView GetItem(size_t) const {
        throw std::runtime_error("GetItem() is not supported for column of " + type_->GetName());
    }

    virtual void AppendFrom(const Column &, size_t /*index*/) {
        throw std::runtime_error("AppendFrom() is not supported for column of " + type_->GetName());
    }

    friend void swap(Column& left, Column& right)
    {
        left.Swap(right);
    }

//    virtual ColumnRef Clone() const = 0;

protected:
    TypeRef type_;
};

}  // namespace clickhouse
