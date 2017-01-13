#include "columns.h"
#include "type_parser.h"

#include <iostream>

namespace clickhouse {

static ColumnRef CreateColumnFromAst(const TypeAst& ast);

static ColumnRef CreateTupleColumn(const TypeAst& ast) {
    std::vector<ColumnRef> columns;

    for (const auto& elem : ast.elements) {
        if (auto col = CreateColumnFromAst(elem)) {
            columns.push_back(col);
        } else {
            return nullptr;
        }
    }

    return std::make_shared<ColumnTuple>(columns);
}

static ColumnRef CreateTerminalColumn(const TypeAst& ast) {
    if (ast.name == "UInt8")
        return std::make_shared<ColumnUInt8>();
    if (ast.name == "UInt16")
        return std::make_shared<ColumnUInt16>();
    if (ast.name == "UInt32")
        return std::make_shared<ColumnUInt32>();
    if (ast.name == "UInt64")
        return std::make_shared<ColumnUInt64>();

    if (ast.name == "Int8")
        return std::make_shared<ColumnInt8>();
    if (ast.name == "Int16")
        return std::make_shared<ColumnInt16>();
    if (ast.name == "Int32")
        return std::make_shared<ColumnInt32>();
    if (ast.name == "Int64")
        return std::make_shared<ColumnInt64>();

    if (ast.name == "Float32")
        return std::make_shared<ColumnFloat32>();
    if (ast.name == "Float64")
        return std::make_shared<ColumnFloat64>();

    if (ast.name == "String")
        return std::make_shared<ColumnString>();

    if (ast.name == "FixedString")
        return std::make_shared<ColumnFixedString>(ast.elements.front().size);

    return nullptr;
}

static ColumnRef CreateColumnFromAst(const TypeAst& ast) {
    if (ast.meta == TypeAst::Terminal) {
        return CreateTerminalColumn(ast);
    }
    if (ast.meta == TypeAst::Tuple) {
        return CreateTupleColumn(ast);
    }
    return nullptr;
}

ColumnRef CreateColumnByType(const std::string& type_name) {
    TypeParser parser(type_name);
    TypeAst ast;

    if (parser.Parse(&ast)) {
        if (ast.meta == TypeAst::Terminal) {
            return CreateTerminalColumn(ast);
        }
        if (ast.meta == TypeAst::Tuple) {
            return CreateTupleColumn(ast);
        }
        // TODO
    }

    return nullptr;
}

}
