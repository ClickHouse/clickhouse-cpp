#pragma once

#include "types.h"

#include <list>
#include <stack>
#include <string>
#include <string_view>

namespace clickhouse {

struct TypeAst {
    enum Meta {
        Array,
        Null,
        Nullable,
        Number,
        Terminal,
        Tuple,
        Enum
    };

    /// Type's category.
    Meta meta;
    Type::Code code;
    /// Type's name.
    /// Need to cache TypeAst, so can't use std::string_view for name.
    std::string name;
    /// Value associated with the node,
    /// used for fixed-width types and enum values.
    int64_t value = 0;
    /// Subelements of the type.
    /// Used to store enum's names and values as well.
    std::list<TypeAst> elements;
};


class TypeParser {

    struct Token {
        enum Type {
            Invalid = 0,
            Name,
            Number,
            LPar,
            RPar,
            Comma,
            EOS,
        };

        Type type;
        std::string_view value;
    };

public:
    explicit TypeParser(const std::string_view name);
    ~TypeParser();

    bool Parse(TypeAst* type);

private:
    Token NextToken();

private:
    const char* cur_;
    const char* end_;

    TypeAst* type_;
    std::stack<TypeAst*> open_elements_;
};


const TypeAst* ParseTypeName(const std::string& type_name);

}
