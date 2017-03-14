#pragma once

#include "clickhouse/base/string_view.h"

#include <list>
#include <stack>
#include <string>

namespace clickhouse {

struct TypeAst {
    enum Meta {
        Array,
        Null,
        Number,
        Terminal,
        Tuple,
    };

    /// Type's category.
    Meta meta;
    /// Type's name.
    StringView name;
    /// Size of type's instance.  For fixed-width types only.
    size_t size = 0;
    /// Subelements of the type.
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

}
