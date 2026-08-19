#include <clickhouse/types/type_parser.h>
#include <gtest/gtest.h>

using namespace clickhouse;

// TODO: add tests for Decimal column types.

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

TEST(TypeParserCase, ParseBool) {
    TypeAst ast;
    TypeParser("Bool").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Bool");
#if !CH_MAP_BOOL_TO_UINT8
    ASSERT_EQ(ast.code, Type::Bool);
#else
    ASSERT_EQ(ast.code, Type::UInt8);
#endif
}

TEST(TypeParserCase, ParseJSON) {
    TypeAst ast;
    TypeParser("JSON").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "JSON");
    ASSERT_EQ(ast.code, Type::JSON);
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
    ASSERT_EQ(ast.elements.size(), 8u);

    std::vector<std::string> names = {"COLOR_red_10_T", "COLOR_green_20_T", "COLOR_blue_30_T", "COLOR_black_30_T"};
    std::vector<int16_t> values = {-12, -25, 53, 107};

    auto element = ast.elements.begin();
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(element->code, Type::String);
        EXPECT_EQ(element->meta, TypeAst::Terminal);
        EXPECT_EQ(element->value_string, names[i]);

        ++element;
        EXPECT_EQ(element->code, Type::Void);
        EXPECT_EQ(element->meta, TypeAst::Number);
        EXPECT_EQ(element->value, values[i]);

        ++element;
    }
}

TEST(TypeParserCase, ParseEnum_AllEscapedBytes) {
    // {fragment as seen by the parser, expected decoded raw byte}
    const std::vector<std::pair<std::string, char>> cases = {
        {"\\0", '\0'},   // NUL
        {"\\b", '\b'},   // Backspace
        {"\\t", '\t'},   // Tab
        {"\\n", '\n'},   // Line feed
        {"\\f", '\f'},   // Form feed
        {"\\r", '\r'},   // Carriage return
        {"\\\\", '\\'},  // Backslash
        {"\\'", '\''},   // Single quote
    };

    // Each escaped byte, embedded in the middle of a label.
    for (const auto& [fragment, raw] : cases) {
        TypeAst ast;
        ASSERT_TRUE(TypeParser("Enum8('x" + fragment + "y' = 1)").Parse(&ast))
            << "failed to parse fragment: " << fragment;
        ASSERT_EQ(ast.meta, TypeAst::Enum);
        ASSERT_EQ(ast.elements.size(), 2u);
        EXPECT_EQ(ast.elements[0].value_string, std::string("x") + raw + "y")
            << "wrong decoding for fragment: " << fragment;
    }

    // Escaped byte as the only/first/last character of the label.
    for (const auto& [fragment, raw] : cases) {
        TypeAst only;
        ASSERT_TRUE(TypeParser("Enum8('" + fragment + "' = 1)").Parse(&only));
        EXPECT_EQ(only.elements[0].value_string, std::string(1, raw));

        TypeAst first;
        ASSERT_TRUE(TypeParser("Enum8('" + fragment + "z' = 1)").Parse(&first));
        EXPECT_EQ(first.elements[0].value_string, std::string(1, raw) + "z");

        TypeAst last;
        ASSERT_TRUE(TypeParser("Enum8('z" + fragment + "' = 1)").Parse(&last));
        EXPECT_EQ(last.elements[0].value_string, std::string("z") + raw);
    }

    // All escaped bytes combined in a single label.
    {
        std::string escaped;
        std::string expected;
        for (const auto& [fragment, raw] : cases) {
            escaped += fragment;
            expected += raw;
        }
        TypeAst ast;
        ASSERT_TRUE(TypeParser("Enum8('" + escaped + "' = 1)").Parse(&ast));
        EXPECT_EQ(ast.elements[0].value_string, expected);
    }

    // Bytes that are NOT escaped must pass through unchanged.
    {
        TypeAst ast;
        ASSERT_TRUE(TypeParser(std::string("Enum8('x\x07y' = 1)")).Parse(&ast));  // BEL
        EXPECT_EQ(ast.elements[0].value_string, std::string("x\x07y"));

        TypeAst emoji;
        ASSERT_TRUE(TypeParser("Enum8('😀' = 1)").Parse(&emoji));
        EXPECT_EQ(emoji.elements[0].value_string, "😀");
    }
}

TEST(TypeParserCase, ParseTuple) {
    TypeAst ast;
    TypeParser(
        "Tuple(UInt8, String)")
        .Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Tuple);
    ASSERT_EQ(ast.name, "Tuple");
    ASSERT_EQ(ast.code, Type::Tuple);
    ASSERT_EQ(ast.elements.size(), 2u);

    std::vector<std::string> names = {"UInt8", "String"};

    auto element = ast.elements.begin();
    for (size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(element->name, names[i]);
        ASSERT_TRUE(element->element_name.empty());
        ++element;
    }
}

TEST(TypeParserCase, ParseNamedTuple) {
    TypeAst ast;
    TypeParser("Tuple(a UInt8, b String)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Tuple);
    ASSERT_EQ(ast.name, "Tuple");
    ASSERT_EQ(ast.code, Type::Tuple);
    ASSERT_EQ(ast.elements.size(), 2u);

    ASSERT_EQ(ast.elements[0].element_name, "a");
    ASSERT_EQ(ast.elements[0].name, "UInt8");
    ASSERT_EQ(ast.elements[0].code, Type::UInt8);

    ASSERT_EQ(ast.elements[1].element_name, "b");
    ASSERT_EQ(ast.elements[1].name, "String");
    ASSERT_EQ(ast.elements[1].code, Type::String);
}

TEST(TypeParserCase, ParseNamedTuple_BacktickQuotedFieldNames) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(`a.b` Int8, `c.d` String)").Parse(&ast));
    ASSERT_EQ(ast.meta, TypeAst::Tuple);
    ASSERT_EQ(ast.elements.size(), 2u);

    ASSERT_EQ(ast.elements[0].element_name, "a.b");
    ASSERT_EQ(ast.elements[0].name, "Int8");
    ASSERT_EQ(ast.elements[0].code, Type::Int8);

    ASSERT_EQ(ast.elements[1].element_name, "c.d");
    ASSERT_EQ(ast.elements[1].name, "String");
    ASSERT_EQ(ast.elements[1].code, Type::String);
}

TEST(TypeParserCase, ParseNamedTuple_DoubleQuotedFieldNames) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(\"a.b\" Int8, \"c.d\" String)").Parse(&ast));
    ASSERT_EQ(ast.meta, TypeAst::Tuple);
    ASSERT_EQ(ast.elements.size(), 2u);

    ASSERT_EQ(ast.elements[0].element_name, "a.b");
    ASSERT_EQ(ast.elements[0].name, "Int8");
    ASSERT_EQ(ast.elements[0].code, Type::Int8);

    ASSERT_EQ(ast.elements[1].element_name, "c.d");
    ASSERT_EQ(ast.elements[1].name, "String");
    ASSERT_EQ(ast.elements[1].code, Type::String);
}

TEST(TypeParserCase, ParseNamedTuple_UnterminatedQuote) {
    TypeAst ast;
    EXPECT_FALSE(TypeParser("Tuple(`a.b Int8)").Parse(&ast));
    EXPECT_FALSE(TypeParser("Tuple(a.b` Int8)").Parse(&ast));
}

TEST(TypeParserCase, ParseNamedTuple_DoubledBacktickEscape) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(`a``b` UInt8)").Parse(&ast));
    ASSERT_EQ(ast.elements[0].element_name, "a`b");
    ASSERT_EQ(ast.elements[0].code, Type::UInt8);
}

TEST(TypeParserCase, ParseNamedTuple_BackslashBacktickEscape) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(`a\\`b` UInt8)").Parse(&ast));
    ASSERT_EQ(ast.elements[0].element_name, "a`b");
    ASSERT_EQ(ast.elements[0].code, Type::UInt8);
}

TEST(TypeParserCase, ParseNamedTuple_DoubleQuoteNotEscape) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(`a\"\"b` UInt8)").Parse(&ast));
    ASSERT_EQ(ast.elements[0].element_name, "a\"\"b");
    ASSERT_EQ(ast.elements[0].code, Type::UInt8);
}

TEST(TypeParserCase, ParseNamedTuple_DoubledDoubleQuoteEscape) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(\"a\"\"b\" UInt8)").Parse(&ast));
    ASSERT_EQ(ast.elements[0].element_name, "a\"b");
    ASSERT_EQ(ast.elements[0].code, Type::UInt8);
}

TEST(TypeParserCase, ParseNamedTuple_BacktickNotEscape) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("Tuple(\"a``b\" UInt8)").Parse(&ast));
    ASSERT_EQ(ast.elements[0].element_name, "a``b");
    ASSERT_EQ(ast.elements[0].code, Type::UInt8);
}

TEST(TypeParserCase, ParseDecimal) {
    TypeAst ast;
    TypeParser("Decimal(12, 5)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Decimal");
    ASSERT_EQ(ast.code, Type::Decimal);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].value, 12);
    ASSERT_EQ(ast.elements[1].value, 5);
}

TEST(TypeParserCase, ParseDecimal32) {
    TypeAst ast;
    TypeParser("Decimal32(7)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Decimal32");
    ASSERT_EQ(ast.code, Type::Decimal32);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].value, 7);
}

TEST(TypeParserCase, ParseDecimal64) {
    TypeAst ast;
    TypeParser("Decimal64(1)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Decimal64");
    ASSERT_EQ(ast.code, Type::Decimal64);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].value, 1);
}

TEST(TypeParserCase, ParseDecimal128) {
    TypeAst ast;
    TypeParser("Decimal128(3)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Decimal128");
    ASSERT_EQ(ast.code, Type::Decimal128);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].value, 3);
}

TEST(TypeParserCase, ParseDateTime_NO_TIMEZONE) {
    TypeAst ast;
    TypeParser("DateTime").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "DateTime");
    ASSERT_EQ(ast.code, Type::DateTime);
    ASSERT_EQ(ast.elements.size(), 0u);
}

TEST(TypeParserCase, ParseDateTime_UTC_TIMEZONE) {
    TypeAst ast;
    TypeParser("DateTime('UTC')").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "DateTime");
    ASSERT_EQ(ast.code, Type::DateTime);
    ASSERT_EQ(ast.elements.size(), 1u);

    ASSERT_EQ(ast.elements[0].code, Type::String);
    ASSERT_EQ(ast.elements[0].value_string, "UTC");
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
}

TEST(TypeParserCase, ParseDateTime_MINSK_TIMEZONE) {
    TypeAst ast;
    TypeParser("DateTime('Europe/Minsk')").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "DateTime");
    ASSERT_EQ(ast.code, Type::DateTime);
    ASSERT_EQ(ast.elements[0].code, Type::String);
    ASSERT_EQ(ast.elements[0].value_string, "Europe/Minsk");
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
}

TEST(TypeParserCase, ParseTimezoneEscapedStrings) {
    const std::vector<std::string> escaped_timezones = {"can\\'t", "a\\tb", "a\\\\b", "a\\nb", "a\\rb",  "😀", "a,b=(c)"};
    const std::vector<std::string> timezones = {"can't", "a\tb", "a\\b", "a\nb", "a\rb",  "😀", "a,b=(c)"};

    for (size_t i = 0; i < timezones.size(); ++i) {
        TypeAst date_time;
        ASSERT_TRUE(TypeParser("DateTime('" + escaped_timezones[i] + "')").Parse(&date_time));
        ASSERT_EQ(date_time.code, Type::DateTime);
        ASSERT_EQ(date_time.elements.size(), 1u);
        EXPECT_EQ(date_time.elements[0].value_string, timezones[i]);

        TypeAst date_time64;
        ASSERT_TRUE(TypeParser("DateTime64(3, '" + escaped_timezones[i] + "')").Parse(&date_time64));
        ASSERT_EQ(date_time64.code, Type::DateTime64);
        ASSERT_EQ(date_time64.elements.size(), 2u);
        EXPECT_EQ(date_time64.elements[1].value_string, timezones[i]);
    }
}

TEST(TypeParserCase, ParseTimezone_AllEscapedBytes) {
    // {fragment as seen by the parser, expected decoded raw byte}
    const std::vector<std::pair<std::string, char>> cases = {
        {"\\0", '\0'},   // NUL
        {"\\b", '\b'},   // Backspace
        {"\\t", '\t'},   // Tab
        {"\\n", '\n'},   // Line feed
        {"\\f", '\f'},   // Form feed
        {"\\r", '\r'},   // Carriage return
        {"\\\\", '\\'},  // Backslash
        {"\\'", '\''},   // Single quote
    };

    for (const auto& [fragment, raw] : cases) {
        const std::string expected = std::string("A") + raw + "B";

        TypeAst date_time;
        ASSERT_TRUE(TypeParser("DateTime('A" + fragment + "B')").Parse(&date_time))
            << "failed to parse fragment: " << fragment;
        ASSERT_EQ(date_time.code, Type::DateTime);
        ASSERT_EQ(date_time.elements.size(), 1u);
        EXPECT_EQ(date_time.elements[0].value_string, expected)
            << "wrong decoding for fragment: " << fragment;

        TypeAst date_time64;
        ASSERT_TRUE(TypeParser("DateTime64(3, 'A" + fragment + "B')").Parse(&date_time64))
            << "failed to parse fragment: " << fragment;
        ASSERT_EQ(date_time64.code, Type::DateTime64);
        ASSERT_EQ(date_time64.elements.size(), 2u);
        EXPECT_EQ(date_time64.elements[1].value_string, expected)
            << "wrong decoding for fragment: " << fragment;
    }

    // All escaped bytes combined in a single timezone string.
    {
        std::string escaped;
        std::string expected;
        for (const auto& [fragment, raw] : cases) {
            escaped += fragment;
            expected += raw;
        }

        TypeAst date_time;
        ASSERT_TRUE(TypeParser("DateTime('" + escaped + "')").Parse(&date_time));
        ASSERT_EQ(date_time.elements.size(), 1u);
        EXPECT_EQ(date_time.elements[0].value_string, expected);

        TypeAst date_time64;
        ASSERT_TRUE(TypeParser("DateTime64(3, '" + escaped + "')").Parse(&date_time64));
        ASSERT_EQ(date_time64.elements.size(), 2u);
        EXPECT_EQ(date_time64.elements[1].value_string, expected);
    }
}

TEST(TypeParserCase, EqualityIncludesValueString) {
    TypeAst utc;
    TypeAst minsk;
    ASSERT_TRUE(TypeParser("DateTime('UTC')").Parse(&utc));
    ASSERT_TRUE(TypeParser("DateTime('Europe/Minsk')").Parse(&minsk));
    ASSERT_NE(utc, minsk);

    TypeAst enum_one;
    TypeAst enum_two;
    ASSERT_TRUE(TypeParser("Enum8('ONE' = 1)").Parse(&enum_one));
    ASSERT_TRUE(TypeParser("Enum8('TWO' = 1)").Parse(&enum_two));
    ASSERT_NE(enum_one, enum_two);
}

TEST(TypeParserCase, LowCardinality_String) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("LowCardinality(String)").Parse(&ast));
    ASSERT_EQ(ast.meta, TypeAst::LowCardinality);
    ASSERT_EQ(ast.name, "LowCardinality");
    ASSERT_EQ(ast.code, Type::LowCardinality);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].code, Type::String);
    ASSERT_EQ(ast.elements[0].name, "String");
    ASSERT_EQ(ast.elements[0].value, 0);
    ASSERT_EQ(ast.elements[0].elements.size(), 0u);
}

TEST(TypeParserCase, LowCardinality_FixedString) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("LowCardinality(FixedString(10))").Parse(&ast));
    ASSERT_EQ(ast.meta, TypeAst::LowCardinality);
    ASSERT_EQ(ast.name, "LowCardinality");
    ASSERT_EQ(ast.code, Type::LowCardinality);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].code, Type::FixedString);
    ASSERT_EQ(ast.elements[0].name, "FixedString");
    ASSERT_EQ(ast.elements[0].value, 0);
    ASSERT_EQ(ast.elements[0].elements.size(), 1u);
    auto param = TypeAst{TypeAst::Number, Type::Void, "", "", 10, {}, {}};
    ASSERT_EQ(ast.elements[0].elements[0], param);
}

TEST(TypeParserCase, SimpleAggregateFunction_UInt64) {
    TypeAst ast;
    TypeParser("SimpleAggregateFunction(func, UInt64)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::SimpleAggregateFunction);
    ASSERT_EQ(ast.name, "SimpleAggregateFunction");
    ASSERT_EQ(ast.code, Type::Void);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].name, "func");
    ASSERT_EQ(ast.elements[0].code, Type::Void);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].value, 0);
    ASSERT_EQ(ast.elements[1].name, "UInt64");
    ASSERT_EQ(ast.elements[1].code, Type::UInt64);
    ASSERT_EQ(ast.elements[1].meta, TypeAst::Terminal);
}

TEST(TypeParserCase, ParseTime) {
    TypeAst ast;
    TypeParser("Time").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Time");
    ASSERT_EQ(ast.code, Type::Time);
    ASSERT_EQ(ast.elements.size(), 0u);
}

TEST(TypeParserCase, ParseTime64) {
    TypeAst ast;
    TypeParser("Time64(3)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "Time64");
    ASSERT_EQ(ast.code, Type::Time64);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].name, "");
    ASSERT_EQ(ast.elements[0].value, 3);
}

TEST(TypeParserCase, ParseDateTime64) {
    TypeAst ast;
    TypeParser("DateTime64(3, 'UTC')").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "DateTime64");
    ASSERT_EQ(ast.code, Type::DateTime64);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].name, "");
    ASSERT_EQ(ast.elements[0].value, 3);
    ASSERT_EQ(ast.elements[1].value_string, "UTC");
    ASSERT_EQ(ast.elements[1].value, 0);
}

TEST(TypeParserCase, ParseMap) {
    TypeAst ast;
    TypeParser("Map(Int32, String)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Map);
    ASSERT_EQ(ast.name, "Map");
    ASSERT_EQ(ast.code, Type::Map);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].name, "Int32");
    ASSERT_EQ(ast.elements[1].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[1].name, "String");
}

TEST(TypeParser, EmptyName) {
    {
        TypeAst ast;
        EXPECT_EQ(false, TypeParser("").Parse(&ast));
    }

    {
        TypeAst ast;
        EXPECT_EQ(false, TypeParser(" ").Parse(&ast));
    }
}

TEST(ParseTypeName, EmptyName) {
    // Empty and invalid names shouldn't produce any AST and shoudn't crash
    EXPECT_EQ(nullptr, ParseTypeName(""));
    EXPECT_EQ(nullptr, ParseTypeName(" "));
    EXPECT_EQ(nullptr, ParseTypeName(std::string(5, '\0')));
}

TEST(TypeParser, AggregateFunction) {
    {
        TypeAst ast;
        EXPECT_FALSE(TypeParser("AggregateFunction(argMax, Int32, DateTime(3))").Parse(&ast));
    }

    {
        TypeAst ast;
        EXPECT_FALSE(TypeParser("AggregateFunction(argMax, LowCardinality(Nullable(FixedString(4))), DateTime(3, 'UTC'))").Parse(&ast));
    }
}
