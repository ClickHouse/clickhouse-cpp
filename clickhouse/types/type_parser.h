#pragma once

#include "../base/string_view.h"
#include "types.h"

#include <list>
#include <stack>
#include <string>

namespace clickhouse {

struct TypeAst {
    enum Meta {
        Array,
        Null,
        Nullable,
        Number,
        Terminal,
        Tuple,
        Enum,
        LowCardinality,
        SimpleAggregateFunction
    };

    /// Type's category.
    Meta meta;
    Type::Code code;
    /// Type's name.
    /// Need to cache TypeAst, so can't use StringView for name.
    std::string name;
    /// Value associated with the node,
    /// used for fixed-width types and enum values.
    int64_t value = 0;
    /// Subelements of the type.
    /// Used to store enum's names and values as well.
    std::vector<TypeAst> elements;

    bool operator==(const TypeAst & other) const;
    inline bool operator!=(const TypeAst & other) const {
        return !(*this == other);
    }
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
            QuotedString, // string with quotation marks included
            EOS,
        };

        Type type;
        StringView value;
    };

public:
    explicit TypeParser(const StringView& name);
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
