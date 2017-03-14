#pragma once

#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/traits.h"

namespace clickhouse {

/** */
class ColumnArray : public Column {
public:
    ColumnArray(ColumnRef data);

    size_t Size() const override;

    bool Load(CodedInputStream* input, size_t rows) override;

    void Save(CodedOutputStream* output) override;

    template <typename T>
    std::vector<T> GetArray(size_t n) const {
        size_t offset = GetOffset(n);
        size_t size = GetSize(n);
        std::vector<T> result;

        for (size_t i = offset; i < offset + size; ++i) {
            result.push_back((*data_->As<typename TypeToColumn<T>::Column>())[i]);
        }

        return result;
    }

private:
    inline size_t GetOffset(size_t n) const {
        return (n == 0) ? 0 : (*offsets_)[n - 1];
    }

    inline size_t GetSize(size_t n) const {
        return (n == 0) ? (*offsets_)[n] : ((*offsets_)[n] - (*offsets_)[n - 1]);
    }

private:
    ColumnRef data_;
    std::shared_ptr<ColumnUInt64> offsets_;
};

}
