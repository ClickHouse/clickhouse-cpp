#include <clickhouse/types/type_parser.h>
#include <contrib/gtest/gtest.h>

using namespace clickhouse;

TEST(TypeParserCase, ParseTerminals) {
    TypeAst ast;
    TypeParser("UInt8").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "UInt8");
    ASSERT_EQ(ast.code, Type::UInt8);
}

TEST(TypeParserCase, ParseFixedString) {
    TypeAst ast;
    TypeParser("FixedString(24)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "FixedString");
    ASSERT_EQ(ast.code, Type::FixedString);
    ASSERT_EQ(ast.elements.front().value, 24U);
}

TEST(TypeParserCase, ParseArray) {
    TypeAst ast;
    TypeParser("Array(Int32)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Array);
    ASSERT_EQ(ast.name, "Array");
    ASSERT_EQ(ast.code, Type::Array);
    ASSERT_EQ(ast.elements.front().meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements.front().name, "Int32");
}

TEST(TypeParserCase, ParseNullable) {
    TypeAst ast;
    TypeParser("Nullable(Date)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Nullable);
    ASSERT_EQ(ast.name, "Nullable");
    ASSERT_EQ(ast.code, Type::Nullable);
    ASSERT_EQ(ast.elements.front().meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements.front().name, "Date");
}

TEST(TypeParserCase, ParseEnum) {
    TypeAst ast;
    TypeParser(
        "Enum8('COLOR_red_10_T' = -12, 'COLOR_green_20_T'=-25, 'COLOR_blue_30_T'= 53, 'COLOR_black_30_T' = 107")
        .Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Enum);
    ASSERT_EQ(ast.name, "Enum8");
    ASSERT_EQ(ast.code, Type::Enum8);
    ASSERT_EQ(ast.elements.size(), 4u);

    std::vector<std::string> names = {"COLOR_red_10_T", "COLOR_green_20_T", "COLOR_blue_30_T", "COLOR_black_30_T"};
    std::vector<int16_t> values = {-12, -25, 53, 107};

    auto element = ast.elements.begin();
    for (size_t i = 0; i < 4; ++i) {
        ASSERT_EQ(element->name, names[i]);
        ASSERT_EQ(element->code, Type::Void);
        ASSERT_EQ(element->value, values[i]);
        ++element;
    }
}
