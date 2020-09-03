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
        case Int128:
            return "Int128";
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
        case IPv4:
            return "IPv4";
        case IPv6:
            return "IPv6";
        case DateTime:
            return "DateTime";
        case DateTime64:
            return As<DateTime64Type>()->GetName();
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
        case Decimal:
        case Decimal32:
        case Decimal64:
        case Decimal128:
            return As<DecimalType>()->GetName();
        case LowCardinality:
            return As<LowCardinalityType>()->GetName();
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

TypeRef Type::CreateDateTime64(size_t precision) {
    return TypeRef(new DateTime64Type(precision));
}

TypeRef Type::CreateDecimal(size_t precision, size_t scale) {
    return TypeRef(new DecimalType(precision, scale));
}

TypeRef Type::CreateIPv4() {
    return TypeRef(new Type(Type::IPv4));
}

TypeRef Type::CreateIPv6() {
    return TypeRef(new Type(Type::IPv6));
}

TypeRef Type::CreateNothing() {
    return TypeRef(new Type(Type::Void));
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

TypeRef Type::CreateLowCardinality(TypeRef item_type) {
    return std::make_shared<LowCardinalityType>(item_type);
}

/// class ArrayType

ArrayType::ArrayType(TypeRef item_type) : Type(Array), item_type_(item_type) {
}

/// class DecimalType

DecimalType::DecimalType(size_t precision, size_t scale)
    : Type(Decimal),
      precision_(precision),
      scale_(scale) {
    // TODO: assert(precision <= 38 && precision > 0);
}

std::string DecimalType::GetName() const {
    switch (GetCode()) {
        case Decimal:
            return "Decimal(" + std::to_string(precision_) + "," + std::to_string(scale_) + ")";
        case Decimal32:
            return "Decimal32(" + std::to_string(scale_) + ")";
        case Decimal64:
            return "Decimal64(" + std::to_string(scale_) + ")";
        case Decimal128:
            return "Decimal128(" + std::to_string(scale_) + ")";
        default:
            /// XXX: NOT REACHED!
            return "";
    }
}

/// class EnumType

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

/// class DateTime64Type

DateTime64Type::DateTime64Type(size_t precision)
    : Type(DateTime64), precision_(precision) {

    if (precision_ > 18) {
        throw std::runtime_error("DateTime64 precision is > 18");
    }
}

std::string DateTime64Type::GetName() const {
    std::string datetime64_representation;
    datetime64_representation.reserve(14);
    datetime64_representation += "DateTime64(";
    datetime64_representation += std::to_string(precision_);
    datetime64_representation += ")";
    return datetime64_representation;
}

/// class FixedStringType

FixedStringType::FixedStringType(size_t n) : Type(FixedString), size_(n) {
}

/// class NullableType

NullableType::NullableType(TypeRef nested_type) : Type(Nullable), nested_type_(nested_type) {
}

/// class TupleType

TupleType::TupleType(const std::vector<TypeRef>& item_types) : Type(Tuple), item_types_(item_types) {
}

/// class LowCardinalityType
LowCardinalityType::LowCardinalityType(TypeRef nested_type) : Type(LowCardinality), nested_type_(nested_type) {
}
LowCardinalityType::~LowCardinalityType()
{}

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

}  // namespace clickhouse
