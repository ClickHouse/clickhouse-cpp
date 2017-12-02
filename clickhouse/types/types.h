#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

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
    };

    struct EnumItem {
        std::string name;
        int16_t value;
    };

    /// Destructor
    ~Type();

    /// Type's code.
    Code GetCode() const;

    /// Type of array's elements.
    TypeRef GetItemType() const;

    /// Type of nested nullable element.
    TypeRef GetNestedType() const;

    /// String representation of the type.
    std::string GetName() const;

    /// Is given type same as current one.
    bool IsEqual(const TypeRef& other) const;

public:
    static TypeRef CreateArray(TypeRef item_type);

    static TypeRef CreateDate();

    static TypeRef CreateDateTime();

    static TypeRef CreateNullable(TypeRef nested_type);

    template <typename T>
    static TypeRef CreateSimple();

    static TypeRef CreateString();

    static TypeRef CreateString(size_t n);

    static TypeRef CreateTuple(const std::vector<TypeRef>& item_types);

    static TypeRef CreateEnum8(const std::vector<EnumItem>& enum_items);

    static TypeRef CreateEnum16(const std::vector<EnumItem>& enum_items);

    static TypeRef CreateUUID();

private:
    Type(const Code code);

    struct ArrayImpl {
        TypeRef item_type;
    };

    struct NullableImpl {
        TypeRef nested_type;
    };

    struct TupleImpl {
        std::vector<TypeRef> item_types;
    };

    struct EnumImpl {
        using ValueToNameType = std::map<int16_t, std::string>;
        using NameToValueType = std::map<std::string, int16_t>;
        ValueToNameType value_to_name;
        NameToValueType name_to_value;
    };

    friend class EnumType;


    const Code code_;
    union {
        ArrayImpl* array_;
        NullableImpl* nullable_;
        TupleImpl* tuple_;
        EnumImpl* enum_;
        int string_size_;
    };
};

class EnumType {
public:
    explicit EnumType(const TypeRef& type);

    std::string GetName() const {
        return type_->GetName();
    }
    /// Methods to work with enum types.
    const std::string& GetEnumName(int16_t value) const;
    int16_t GetEnumValue(const std::string& name) const;
    bool HasEnumName(const std::string& name) const;
    bool HasEnumValue(int16_t value) const;

    /// Iterator for enum elements.
    using ValueToNameIterator = Type::EnumImpl::ValueToNameType::const_iterator;
    ValueToNameIterator BeginValueToName() const;
    ValueToNameIterator EndValueToName() const;

private:
    TypeRef type_;
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

}
