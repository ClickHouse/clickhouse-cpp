#pragma once

#include "../base/coded.h"
#include "../base/input.h"
#include "../types/types.h"
#include "../columns/itemview.h"

#include <memory>

namespace clickhouse {

using ColumnRef = std::shared_ptr<class Column>;

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
    inline const class Type& GetType() const { return *type_; }

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
    /// Please note that view is invalidated once column items are added or deleted, column is loaded from strean or destroyed.
    virtual ItemView GetItem(size_t) const {
        throw std::runtime_error("GetItem() is not supported for column of " + type_->GetName());
    }

    friend void swap(Column& left, Column& right) {
        left.Swap(right);
    }

protected:
    TypeRef type_;
};

}  // namespace clickhouse
