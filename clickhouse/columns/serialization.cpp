#include "serialization.h"

#include "../base/wire_format.h"
#include "column.h"

namespace {
/// 2^62, because VarInt supports only values < 2^63.
constexpr auto END_OF_GRANULE_FLAG = 1ULL << 62;
}  // namespace

namespace clickhouse {

SerializationSparse::SerializationSparse(SerializationRef nested) : nested_(std::move(nested)) {
    assert(nested_);
}

Serialization::Kind SerializationSparse::GetKind() const {
    return Kind::SPARSE;
}

bool SerializationSparse::LoadPrefix(Column* column, InputStream* input, size_t rows) const {
    return nested_->LoadPrefix(column, input, rows);
}

bool SerializationSparse::LoadBody(Column* column, InputStream* input, size_t rows) const {
    assert(column);
    std::vector<size_t> offsets;
    while (true) {
        uint64_t group_size;
        if (!WireFormat::ReadUInt64(*input, &group_size)) {
            return false;
        }

        if (group_size & END_OF_GRANULE_FLAG) {
            break;
        }

        size_t start_of_group = 0;
        if (!offsets.empty()) start_of_group = offsets.back() + 1;

        offsets.push_back(start_of_group + group_size);
    }
    auto nested_column = column->CloneEmpty();
    if (offsets.size() > 0) {
        if (!nested_->LoadBody(nested_column.get(), input, offsets.size())) {
            return false;
        }
    }
    column->Clear();
    AppendSparseColumn(column, rows, offsets, nested_column.get());
    return true;
}

void SerializationSparse::SavePrefix(Column* column, OutputStream* output) const {
    nested_->SavePrefix(column, output);
}

void SerializationSparse::SaveBody(Column* column, OutputStream* output) const {
    assert(column);
    std::vector<size_t> offsets = GetIndicesOfNonDefaultRows(column);
    size_t start                = 0;
    size_t size                 = offsets.size();
    for (size_t i = 0; i < size; ++i) {
        size_t group_size = offsets[i] - start;
        WireFormat::WriteUInt64(*output, group_size);
        start += group_size + 1;
    }

    size_t group_size = column->Size() - start;
    group_size |= END_OF_GRANULE_FLAG;
    WireFormat::WriteUInt64(*output, group_size);
    if (offsets.size() == 0) {
        return;
    }
    auto nested_column = GetByIndices(column, offsets);
    nested_->SaveBody(nested_column.get(), output);
}

}  // namespace clickhouse
