#pragma once

#include "numeric.h"

struct in_addr;

namespace clickhouse {

class ColumnIPv4 : public Column {
public:
    using DataType = in_addr;
    using ValueType = in_addr;

    ColumnIPv4();
    /** Takes ownership of the data, expects ColumnUInt32.
     * Modifying memory pointed by `data` from outside is UB.
     *
     * TODO: deprecate and remove as it is too dangerous and error-prone.
     */
    explicit ColumnIPv4(ColumnRef data);

    /// Appends one element to the column.
    void Append(const std::string& ip);

    /// @params ip numeric value with host byte order.
    void Append(uint32_t ip);

    ///
    void Append(in_addr ip);

    /// Returns element at given row number.
    in_addr At(size_t n) const;

    /// Returns element at given row number.
    in_addr operator [] (size_t n) const;

    std::string AsString(size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool Load(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void Save(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;

    void Swap(Column& other) override;

    ItemView GetItem(size_t index) const override;

private:
    std::shared_ptr<ColumnUInt32> data_;
};

}
