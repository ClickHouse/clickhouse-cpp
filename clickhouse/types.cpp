#include "types.h"

namespace clickhouse {

Type::Type(const Code code)
    : code_(code)
{
    if (code_ == Array) {
        array_ = new ArrayImpl;
    } else if (code_ == Tuple) {
        tuple_ = new TupleImpl;
    }
}

Type::~Type() {
    if (code_ == Array) {
        delete array_;
    } else if (code_ == Tuple) {
        delete tuple_;
    }
}

TypeRef Type::CreateDate() {
    return TypeRef(new Type(Type::Date));
}

TypeRef Type::CreateDateTime() {
    return TypeRef(new Type(Type::DateTime));
}

TypeRef Type::CreateString() {
    return TypeRef(new Type(Type::String));
}

TypeRef Type::CreateString(size_t n) {
    TypeRef type(new Type(Type::FixedString));
    type->string_size_ = n;
    return type;
}

Type::Code Type::GetCode() const {
    return code_;
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
            return "FixedString()";
        case DateTime:
            return "DateTime";
        case Date:
            return "Date";
        case Array:
            return "Array()";
        case Tuple:
            return "Tuple()";
    }

    return std::string();
}

}
