
#pragma once

#include "column.h"
#include "../base/input.h"

#include <stdexcept>
#include <utility>

namespace clickhouse {

/**
 * Represents dummy column of NULLs.
 */
class ColumnNothing : public Column {
public:
    ColumnNothing()
        : Column(Type::CreateNothing(), Serialization::MakeDefault(this))
        , size_(0)
    {
    }

    explicit ColumnNothing(size_t n)
        : Column(Type::CreateNothing(), Serialization::MakeDefault(this))
        , size_(n)
    {
    }

    /// Appends one element to the column.
    void Append(std::unique_ptr<void*>) { ++size_; }

    /// Returns element at given row number.
    std::nullptr_t At(size_t) const { return nullptr; };

    /// Returns element at given row number.
    std::nullptr_t operator [] (size_t) const { return nullptr; };

    /// Makes slice of the current column.
    ColumnRef Slice(size_t, size_t len) const override {
        return std::make_shared<ColumnNothing>(len);
    }

    ColumnRef CloneEmpty() const override {
        return std::make_shared<ColumnNothing>();
    }

    ItemView GetItem(size_t /*index*/) const override { return ItemView{}; }

    void SetSerializationKind(Serialization::Kind kind)  override {
        switch (kind)
        {
        case Serialization::Kind::DEFAULT:
            serialization_ = Serialization::MakeDefault(this);
            break;
        default:
            throw UnimplementedError("Serialization kind:" + std::to_string(static_cast<int>(kind))
                + " is not supported for column of " + type_->GetName());
        }
    }

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override {
        if (auto col = column->As<ColumnNothing>()) {
            size_ += col->Size();
        }
    }

    /// Clear column data .
    void Clear() override { size_ = 0; }

    /// Returns count of rows in the column.
    size_t Size() const override { return size_; }

    void Swap(Column& other) override {
        auto & col = dynamic_cast<ColumnNothing &>(other);
        std::swap(size_, col.size_);
    }

private:
    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) {
        input->Skip(rows);
        size_ += rows;
        return true;
    }

    /// Saves column data to output stream.
    void SaveBody(OutputStream*) {
        throw UnimplementedError("method SaveBody is not supported for Nothing column");
    }

    friend SerializationDefault<ColumnNothing>;

    size_t size_;
};

}
