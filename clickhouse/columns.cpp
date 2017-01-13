#include "columns.h"
#include "type_parser.h"

#include <iostream>

namespace clickhouse {

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

ColumnRef CreateColumnByType(const std::string& type_name) {
    TypeParser parser(type_name);
    TypeAst ast;

    if (parser.Parse(&ast)) {
        if (ast.meta == TypeAst::Terminal) {
            return CreateTerminalColumn(ast);
        }
        // TODO
    }

    return nullptr;
}

}
