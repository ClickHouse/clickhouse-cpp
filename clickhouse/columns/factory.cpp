#include "factory.h"

#include "array.h"
#include "date.h"
#include "enum.h"
#include "nullable.h"
#include "numeric.h"
#include "string.h"
#include "tuple.h"
#include "uuid.h"

#include "../types/type_parser.h"

namespace clickhouse {
namespace {

static ColumnRef CreateTerminalColumn(const TypeAst& ast) {
    switch (ast.code) {
    case Type::UInt8:
        return std::make_shared<ColumnUInt8>();
    case Type::UInt16:
        return std::make_shared<ColumnUInt16>();
    case Type::UInt32:
        return std::make_shared<ColumnUInt32>();
    case Type::UInt64:
        return std::make_shared<ColumnUInt64>();

    case Type::Int8:
        return std::make_shared<ColumnInt8>();
    case Type::Int16:
        return std::make_shared<ColumnInt16>();
    case Type::Int32:
        return std::make_shared<ColumnInt32>();
    case Type::Int64:
        return std::make_shared<ColumnInt64>();

    case Type::UUID:
        return std::make_shared<ColumnUUID>();

    case Type::Float32:
        return std::make_shared<ColumnFloat32>();
    case Type::Float64:
        return std::make_shared<ColumnFloat64>();

    case Type::String:
        return std::make_shared<ColumnString>();
    case Type::FixedString:
        return std::make_shared<ColumnFixedString>(ast.elements.front().value);

    case Type::DateTime:
        return std::make_shared<ColumnDateTime>();
    case Type::Date:
        return std::make_shared<ColumnDate>();

    default:
        return nullptr;
    }
}

static ColumnRef CreateColumnFromAst(const TypeAst& ast) {
    switch (ast.meta) {
        case TypeAst::Array: {
            return std::make_shared<ColumnArray>(
                CreateColumnFromAst(ast.elements.front())
            );
        }

        case TypeAst::Nullable: {
            return std::make_shared<ColumnNullable>(
                CreateColumnFromAst(ast.elements.front()),
                std::make_shared<ColumnUInt8>()
            );
        }

        case TypeAst::Terminal: {
            return CreateTerminalColumn(ast);
        }

        case TypeAst::Tuple: {
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

        case TypeAst::Enum: {
            std::vector<Type::EnumItem> enum_items;

            for (const auto& elem : ast.elements) {
                enum_items.push_back(
                    Type::EnumItem{elem.name, (int16_t)elem.value});
            }

            if (ast.code == Type::Enum8) {
                return std::make_shared<ColumnEnum8>(
                    Type::CreateEnum8(enum_items)
                );
            } else if (ast.code == Type::Enum16) {
                return std::make_shared<ColumnEnum16>(
                    Type::CreateEnum16(enum_items)
                );
            }
            break;
        }

        case TypeAst::Null:
        case TypeAst::Number:
            break;
    }

    return nullptr;
}

} // namespace


ColumnRef CreateColumnByType(const std::string& type_name) {
    auto ast = ParseTypeName(type_name);
    if (ast != nullptr) {
        return CreateColumnFromAst(*ast);
    }

    return nullptr;
}

}
