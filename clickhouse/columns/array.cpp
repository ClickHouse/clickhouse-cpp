#include "array.h"

#include <stdexcept>

namespace clickhouse {

ColumnArray::ColumnArray(ColumnRef data)
    : Column(Type::CreateArray(data->Type()))
    , data_(data)
    , offsets_(std::make_shared<ColumnUInt64>())
{
}

void ColumnArray::AppendAsColumn(ColumnRef array) {
    if (!data_->Type()->IsEqual(array->Type())) {
        throw std::runtime_error(
            "can't append column of type " + array->Type()->GetName() + " "
            "to column type " + data_->Type()->GetName());
    }

    if (offsets_->Size() == 0) {
        offsets_->Append(array->Size());
    } else {
        offsets_->Append((*offsets_)[offsets_->Size() - 1] + array->Size());
    }

    data_->Append(array);
}

ColumnRef ColumnArray::GetAsColumn(size_t n) const {
    return data_->Slice(GetOffset(n), GetSize(n));
}

void ColumnArray::Append(ColumnRef column) {
    if (auto col = column->As<ColumnArray>()) {
        if (!col->data_->Type()->IsEqual(data_->Type())) {
            return;
        }

        for (size_t i = 0; i < col->Size(); ++i) {
            AppendAsColumn(col->GetAsColumn(i));
        }
    }
}

bool ColumnArray::Load(CodedInputStream* input, size_t rows) {
    if (!offsets_->Load(input, rows)) {
        return false;
    }
    if (!data_->Load(input, (*offsets_)[rows - 1])) {
        return false;
    }
    return true;
}

void ColumnArray::Save(CodedOutputStream* output) {
    offsets_->Save(output);
    data_->Save(output);
}

void ColumnArray::Clear() {
    offsets_->Clear();
    data_->Clear();
}

size_t ColumnArray::Size() const {
    return offsets_->Size();
}

size_t ColumnArray::GetOffset(size_t n) const {
    return (n == 0) ? 0 : (*offsets_)[n - 1];
}

size_t ColumnArray::GetSize(size_t n) const {
    return (n == 0) ? (*offsets_)[n] : ((*offsets_)[n] - (*offsets_)[n - 1]);
}

}
