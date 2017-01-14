#include <clickhouse/type_parser.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

TEST(TypeParserCase, ParseTerminals) {
    TypeAst ast;
    TypeParser("UInt8").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "UInt8");
}

TEST(TypeParserCase, ParseFixedString) {
    TypeAst ast;
    TypeParser("FixedString(24)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "FixedString");
    ASSERT_EQ(ast.elements.front().size, 24U);
}
