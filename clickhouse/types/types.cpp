#include "types.h"

namespace clickhouse {

Type::Type(const Code code)
    : code_(code)
{
    if (code_ == Array) {
        array_ = new ArrayImpl;
    } else if (code_ == Tuple) {
        tuple_ = new TupleImpl;
    } else if (code_ == Nullable) {
        nullable_ = new NullableImpl;
    }
}

Type::~Type() {
    if (code_ == Array) {
        delete array_;
    } else if (code_ == Tuple) {
        delete tuple_;
    } else if (code_ == Nullable) {
        delete nullable_;
    }
}

Type::Code Type::GetCode() const {
    return code_;
}

TypeRef Type::GetItemType() const {
    if (code_ == Array) {
        return array_->item_type;
    }
    return TypeRef();
}

std::string Type::GetName() const {
    switch (code_) {
        case Void:
            return "Void";
        case Int8:
            return "Int8";
        case Int16:
            return "Int16";
        case Int32:
            return "Int32";
        case Int64:
            return "Int64";
        case UInt8:
            return "UInt8";
        case UInt16:
            return "UInt16";
        case UInt32:
            return "UInt32";
        case UInt64:
            return "UInt64";
        case Float32:
            return "Float32";
        case Float64:
            return "Float64";
        case String:
            return "String";
        case FixedString:
            return "FixedString(" + std::to_string(string_size_) + ")";
        case DateTime:
            return "DateTime";
        case Date:
            return "Date";
        case Array:
            return std::string("Array(") + array_->item_type->GetName() +")";
        case Nullable:
            return std::string("Nullable(") + nullable_->nested_type->GetName() + ")";
        case Tuple:
            return "Tuple()";
    }

    return std::string();
}

bool Type::IsEqual(const TypeRef& other) const {
    return this->GetName() == other->GetName();
}

TypeRef Type::CreateArray(TypeRef item_type) {
    TypeRef type(new Type(Type::Array));
    type->array_->item_type = item_type;
    return type;
}

TypeRef Type::CreateDate() {
    return TypeRef(new Type(Type::Date));
}

TypeRef Type::CreateDateTime() {
    return TypeRef(new Type(Type::DateTime));
}

TypeRef Type::CreateNullable(TypeRef nested_type) {
    TypeRef type(new Type(Type::Nullable));
    type->nullable_->nested_type = nested_type;
    return type;
}

TypeRef Type::CreateString() {
    return TypeRef(new Type(Type::String));
}

TypeRef Type::CreateString(size_t n) {
    TypeRef type(new Type(Type::FixedString));
    type->string_size_ = n;
    return type;
}

TypeRef Type::CreateTuple(const std::vector<TypeRef>& item_types) {
    TypeRef type(new Type(Type::Tuple));
    type->tuple_->item_types.assign(item_types.begin(), item_types.end());
    return type;
}

}
