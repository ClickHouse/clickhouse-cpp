#include "types.h"

#include <assert.h>

namespace clickhouse {

Type::Type(const Code code) : code_(code) {
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
        case UUID:
            return "UUID";
        case Float32:
            return "Float32";
        case Float64:
            return "Float64";
        case String:
            return "String";
        case FixedString:
            return As<FixedStringType>()->GetName();
        case DateTime:
            return "DateTime";
        case Date:
            return "Date";
        case Array:
            return As<ArrayType>()->GetName();
        case Nullable:
            return As<NullableType>()->GetName();
        case Tuple:
            return As<TupleType>()->GetName();
        case Enum8:
        case Enum16:
            return As<EnumType>()->GetName();
    }

    // XXX: NOT REACHED!
    return std::string();
}

TypeRef Type::CreateArray(TypeRef item_type) {
    return TypeRef(new ArrayType(item_type));
}

TypeRef Type::CreateDate() {
    return TypeRef(new Type(Type::Date));
}

TypeRef Type::CreateDateTime() {
    return TypeRef(new Type(Type::DateTime));
}

TypeRef Type::CreateNullable(TypeRef nested_type) {
    return TypeRef(new NullableType(nested_type));
}

TypeRef Type::CreateString() {
    return TypeRef(new Type(Type::String));
}

TypeRef Type::CreateString(size_t n) {
    return TypeRef(new FixedStringType(n));
}

TypeRef Type::CreateTuple(const std::vector<TypeRef>& item_types) {
    return TypeRef(new TupleType(item_types));
}

TypeRef Type::CreateEnum8(const std::vector<EnumItem>& enum_items) {
    return TypeRef(new EnumType(Type::Enum8, enum_items));
}

TypeRef Type::CreateEnum16(const std::vector<EnumItem>& enum_items) {
    return TypeRef(new EnumType(Type::Enum16, enum_items));
}

TypeRef Type::CreateUUID() {
    return TypeRef(new Type(Type::UUID));
}

ArrayType::ArrayType(TypeRef item_type) : Type(Array), item_type_(item_type) {
}

EnumType::EnumType(Type::Code type, const std::vector<EnumItem>& items) : Type(type) {
    for (const auto& item : items) {
        value_to_name_[item.second] = item.first;
        name_to_value_[item.first]  = item.second;
    }
}

std::string EnumType::GetName() const {
    std::string result;

    if (GetCode() == Enum8) {
        result = "Enum8(";
    } else {
        result = "Enum16(";
    }

    for (auto ei = value_to_name_.begin();;) {
        result += "'";
        result += ei->second;
        result += "' = ";
        result += std::to_string(ei->first);

        if (++ei != value_to_name_.end()) {
            result += ", ";
        } else {
            break;
        }
    }

    result += ")";

    return result;
}

FixedStringType::FixedStringType(size_t n) : Type(FixedString), size_(n) {
}

NullableType::NullableType(TypeRef nested_type) : Type(Nullable), nested_type_(nested_type) {
}

TupleType::TupleType(const std::vector<TypeRef>& item_types) : Type(Tuple), item_types_(item_types) {
}

std::string TupleType::GetName() const {
    std::string result("Tuple(");

    if (!item_types_.empty()) {
        result += item_types_[0]->GetName();
    }

    for (size_t i = 1; i < item_types_.size(); ++i) {
        result += ", " + item_types_[i]->GetName();
    }

    result += ")";

    return result;
}

const std::string& EnumType::GetEnumName(int16_t value) const {
    return value_to_name_.at(value);
}

int16_t EnumType::GetEnumValue(const std::string& name) const {
    return name_to_value_.at(name);
}

bool EnumType::HasEnumName(const std::string& name) const {
    return name_to_value_.find(name) != name_to_value_.end();
}

bool EnumType::HasEnumValue(int16_t value) const {
    return value_to_name_.find(value) != value_to_name_.end();
}

EnumType::ValueToNameIterator EnumType::BeginValueToName() const {
    return value_to_name_.begin();
}

EnumType::ValueToNameIterator EnumType::EndValueToName() const {
    return value_to_name_.end();
}

}  // namespace clickhouse
