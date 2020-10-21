#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

namespace clickhouse {

using TypeRef = std::shared_ptr<class Type>;

class Type {
public:
    enum Code {
        Void = 0,
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float32,
        Float64,
        String,
        FixedString,
        DateTime,
        Date,
        Array,
        Nullable,
        Tuple,
        Enum8,
        Enum16,
        UUID,
        IPv4,
        IPv6,
        Int128,
        Decimal,
        Decimal32,
        Decimal64,
        Decimal128,
        LowCardinality,
        DateTime64,
    };

    using EnumItem = std::pair<std::string /* name */, int16_t /* value */>;

protected:
    Type(const Code code);

public:
    template <typename Derived>
    auto* As() {
        return static_cast<Derived*>(this);
    }

    template <typename Derived>
    const auto* As() const {
        return static_cast<const Derived*>(this);
    }

    /// Type's code.
    Code GetCode() const { return code_; }

    /// String representation of the type.
    std::string GetName() const;

    /// Is given type same as current one.
    bool IsEqual(const Type& other) const { return this->GetName() == other.GetName(); }
    bool IsEqual(const TypeRef& other) const { return IsEqual(*other); }

public:
    static TypeRef CreateArray(TypeRef item_type);

    static TypeRef CreateDate();

    static TypeRef CreateDateTime();

    static TypeRef CreateDateTime64(size_t precision);

    static TypeRef CreateDecimal(size_t precision, size_t scale);

    static TypeRef CreateIPv4();

    static TypeRef CreateIPv6();

    static TypeRef CreateNothing();

    static TypeRef CreateNullable(TypeRef nested_type);

    template <typename T>
    static TypeRef CreateSimple();

    static TypeRef CreateString();

    static TypeRef CreateString(size_t n);

    static TypeRef CreateTuple(const std::vector<TypeRef>& item_types);

    static TypeRef CreateEnum8(const std::vector<EnumItem>& enum_items);

    static TypeRef CreateEnum16(const std::vector<EnumItem>& enum_items);

    static TypeRef CreateUUID();

    static TypeRef CreateLowCardinality(TypeRef item_type);

private:
    const Code code_;
};

inline bool operator==(const Type & left, const Type & right) {
    if (&left == &right)
        return true;
    if (typeid(left) == typeid(right))
        return left.IsEqual(right);
    return false;
}

inline bool operator==(const TypeRef & left, const TypeRef & right) {
    return *left == *right;
}

class ArrayType : public Type {
public:
    explicit ArrayType(TypeRef item_type);

    std::string GetName() const { return std::string("Array(") + item_type_->GetName() + ")"; }

    /// Type of array's elements.
    inline TypeRef GetItemType() const { return item_type_; }

private:
    TypeRef item_type_;
};

class DecimalType : public Type {
public:
    DecimalType(size_t precision, size_t scale);

    std::string GetName() const;

    inline size_t GetScale() const { return scale_; }
    inline size_t GetPrecision() const { return precision_; }

private:
    const size_t precision_, scale_;
};

class DateTime64Type: public Type {
public:
    explicit DateTime64Type(size_t precision);

    std::string GetName() const;

    inline size_t GetPrecision() const { return precision_; }

private:
    size_t precision_;
};

class EnumType : public Type {
public:
    EnumType(Type::Code type, const std::vector<EnumItem>& items);

    std::string GetName() const;

    /// Methods to work with enum types.
    const std::string& GetEnumName(int16_t value) const;
    int16_t GetEnumValue(const std::string& name) const;
    bool HasEnumName(const std::string& name) const;
    bool HasEnumValue(int16_t value) const;

private:
    using ValueToNameType     = std::map<int16_t, std::string>;
    using NameToValueType     = std::map<std::string, int16_t>;
    using ValueToNameIterator = ValueToNameType::const_iterator;

    ValueToNameType value_to_name_;
    NameToValueType name_to_value_;

public:
    ValueToNameIterator BeginValueToName() const;
    ValueToNameIterator EndValueToName() const;
};

class FixedStringType : public Type {
public:
    explicit FixedStringType(size_t n);

    std::string GetName() const { return std::string("FixedString(") + std::to_string(size_) + ")"; }

private:
    size_t size_;
};

class NullableType : public Type {
public:
    explicit NullableType(TypeRef nested_type);

    std::string GetName() const { return std::string("Nullable(") + nested_type_->GetName() + ")"; }

    /// Type of nested nullable element.
    TypeRef GetNestedType() const { return nested_type_; }

private:
    TypeRef nested_type_;
};

class TupleType : public Type {
public:
    explicit TupleType(const std::vector<TypeRef>& item_types);

    std::string GetName() const;

    /// Type of nested Tuple element type.
    std::vector<TypeRef> GetTupleType() const { return item_types_; }

private:
    std::vector<TypeRef> item_types_;
};

class LowCardinalityType : public Type {
public:
    explicit LowCardinalityType(TypeRef nested_type);
    ~LowCardinalityType();

    std::string GetName() const { return std::string("LowCardinality(") + nested_type_->GetName() + ")"; }

    /// Type of nested nullable element.
    TypeRef GetNestedType() const { return nested_type_; }

private:
    TypeRef nested_type_;
};

template <>
inline TypeRef Type::CreateSimple<int8_t>() {
    return TypeRef(new Type(Int8));
}

template <>
inline TypeRef Type::CreateSimple<int16_t>() {
    return TypeRef(new Type(Int16));
}

template <>
inline TypeRef Type::CreateSimple<int32_t>() {
    return TypeRef(new Type(Int32));
}

template <>
inline TypeRef Type::CreateSimple<int64_t>() {
    return TypeRef(new Type(Int64));
}

template <>
inline TypeRef Type::CreateSimple<__int128>() {
    return TypeRef(new Type(Int128));
}

template <>
inline TypeRef Type::CreateSimple<uint8_t>() {
    return TypeRef(new Type(UInt8));
}

template <>
inline TypeRef Type::CreateSimple<uint16_t>() {
    return TypeRef(new Type(UInt16));
}

template <>
inline TypeRef Type::CreateSimple<uint32_t>() {
    return TypeRef(new Type(UInt32));
}

template <>
inline TypeRef Type::CreateSimple<uint64_t>() {
    return TypeRef(new Type(UInt64));
}

template <>
inline TypeRef Type::CreateSimple<float>() {
    return TypeRef(new Type(Float32));
}

template <>
inline TypeRef Type::CreateSimple<double>() {
    return TypeRef(new Type(Float64));
}

}  // namespace clickhouse
