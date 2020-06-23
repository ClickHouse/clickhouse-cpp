#include "tuple.h"
#include "ostream"

namespace clickhouse {

static std::vector<TypeRef> CollectTypes(const std::vector<ColumnRef>& columns) {
    std::vector<TypeRef> types;
    for (const auto& col : columns) {
        types.push_back(col->Type());
    }
    return types;
}

ColumnTuple::ColumnTuple(const std::vector<ColumnRef>& columns) : Column(Type::CreateTuple(CollectTypes(columns))), columns_(columns) {
}

size_t ColumnTuple::TupleSize() const {
    return columns_.size();
}

size_t ColumnTuple::Size() const {
    return columns_.empty() ? 0 : columns_[0]->Size();
}

bool ColumnTuple::Load(CodedInputStream* input, size_t rows) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        if (!(*ci)->Load(input, rows)) {
            return false;
        }
    }

    return true;
}

void ColumnTuple::Save(CodedOutputStream* output) {
    for (auto ci = columns_.begin(); ci != columns_.end(); ++ci) {
        (*ci)->Save(output);
    }
}

void ColumnTuple::Clear() {
    columns_.clear();
}

void ColumnTuple::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnTuple&>(other);
    columns_.swap(col.columns_);
}

void appendQuote(std::ostream& o, Type::Code code) {
    switch (code) {
        case Type::Code::String:
        case Type::Code::FixedString:
        case Type::Code::DateTime:
        case Type::Code::Date:
        case Type::Code::Enum8:
        case Type::Code::Enum16:
        case Type::Code::UUID:
        case Type::Code::IPv4:
        case Type::Code::IPv6:
            o << '\'';
            break;
        default:
            // do nothing
            break;
    };
}

std::ostream& ColumnTuple::Dump(std::ostream& o, size_t index) const {
    o << "(";
    size_t size = TupleSize();
    for (size_t i = 0; i < size; i++) {
        auto code = columns_[i]->GetType().GetCode();
        appendQuote(o, code);
        columns_[i]->Dump(o, index);
        appendQuote(o, code);
        if (i != size - 1) {
            o << ",";
        }
    }
    return o << ")";
}
}  // namespace clickhouse
