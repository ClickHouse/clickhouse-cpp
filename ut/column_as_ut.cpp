#include <clickhouse/columns/array.h>
#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace {
using namespace clickhouse;

struct ArrayAsTestCase {
    using TypedColumn = ColumnArrayT<ColumnUInt64>;
    using Value = std::vector<uint64_t>;

    static const char* TypeName() { return "Array(UInt64)"; }

    static std::array<Value, 3> Values() {
        return {{{1, 2}, {7}, {9, 10, 11}}};
    }

    static void Append(TypedColumn& column, const Value& value) {
        column.Append(value);
    }

    static void ExpectValue(const TypedColumn& column, size_t index, const Value& expected) {
        const auto actual = column.At(index);
        ASSERT_EQ(expected.size(), actual.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(expected[i], actual[i]);
        }
    }
};

struct NullableAsTestCase {
    using TypedColumn = ColumnNullableT<ColumnUInt64>;
    using Value = std::optional<uint64_t>;

    static const char* TypeName() { return "Nullable(UInt64)"; }

    static std::array<Value, 3> Values() {
        return {{uint64_t{1}, std::nullopt, uint64_t{42}}};
    }

    static void Append(TypedColumn& column, const Value& value) {
        column.Append(value);
    }

    static void ExpectValue(const TypedColumn& column, size_t index, const Value& expected) {
        EXPECT_EQ(expected, column.At(index));
    }
};

struct TupleAsTestCase {
    using TypedColumn = ColumnTupleT<ColumnUInt64, ColumnString>;
    using Value = std::tuple<uint64_t, std::string>;

    static const char* TypeName() { return "Tuple(UInt64, String)"; }

    static std::array<Value, 3> Values() {
        return {{{1, "one"}, {2, "two"}, {3, "three"}}};
    }

    static void Append(TypedColumn& column, const Value& value) {
        column.Append(value);
    }

    static void ExpectValue(const TypedColumn& column, size_t index, const Value& expected) {
        const auto actual = column.At(index);
        EXPECT_EQ(std::get<0>(expected), std::get<0>(actual));
        EXPECT_EQ(std::string_view(std::get<1>(expected)), std::get<1>(actual));
    }
};

struct MapAsTestCase {
    using TypedColumn = ColumnMapT<ColumnString, ColumnUInt64>;
    using Value = std::map<std::string, uint64_t>;

    static const char* TypeName() { return "Map(String, UInt64)"; }

    static std::array<Value, 3> Values() {
        return {{{{"one", 1}}, {{"two", 2}, {"second", 20}}, {{"three", 3}}}};
    }

    static void Append(TypedColumn& column, const Value& value) {
        column.Append(value);
    }

    static void ExpectValue(const TypedColumn& column, size_t index, const Value& expected) {
        const auto actual = column.At(index);
        ASSERT_EQ(expected.size(), actual.size());
        for (const auto& [key, value] : expected) {
            EXPECT_EQ(value, actual.At(std::string_view(key)));
        }
    }
};

struct LowCardinalityAsTestCase {
    using TypedColumn = ColumnLowCardinalityT<ColumnString>;
    using Value = std::string;

    static const char* TypeName() { return "LowCardinality(String)"; }

    static std::array<Value, 3> Values() {
        return {{"alpha", "beta", "alpha"}};
    }

    static void Append(TypedColumn& column, const Value& value) {
        column.Append(std::string_view(value));
    }

    static void ExpectValue(const TypedColumn& column, size_t index, const Value& expected) {
        EXPECT_EQ(std::string_view(expected), column.At(index));
    }
};

template <typename T>
class ColumnAsTest : public testing::Test {
protected:
    using TypedColumn = typename T::TypedColumn;
    using Value = typename T::Value;

    static ColumnRef MakeBaseColumn() {
        return CreateColumnByType(T::TypeName());
    }

    static void AppendToBase(const ColumnRef& column, const Value& value) {
        auto donor = MakeBaseColumn();
        T::Append(*donor->template AsStrict<TypedColumn>(), value);
        column->Append(std::move(donor));
    }

    static void ExpectValues(
        const std::shared_ptr<TypedColumn>& column,
        const std::vector<Value>& expected) {
        ASSERT_EQ(expected.size(), column->Size());
        for (size_t i = 0; i < expected.size(); ++i) {
            T::ExpectValue(*column, i, expected[i]);
        }
    }
};

using ColumnAsTestCases = testing::Types<
    ArrayAsTestCase,
    NullableAsTestCase,
    TupleAsTestCase,
    MapAsTestCase,
    LowCardinalityAsTestCase>;

TYPED_TEST_SUITE(ColumnAsTest, ColumnAsTestCases);

TYPED_TEST(ColumnAsTest, SharesMutationsInBothDirections) {
    using TypedColumn = typename TestFixture::TypedColumn;
    const auto values = TypeParam::Values();
    auto base = TestFixture::MakeBaseColumn();

    ASSERT_NE(nullptr, base);
    EXPECT_EQ(nullptr, std::dynamic_pointer_cast<TypedColumn>(base));

    auto first_view = base->template As<TypedColumn>();
    auto second_view = base->template AsStrict<TypedColumn>();
    ASSERT_NE(nullptr, first_view);
    ASSERT_NE(nullptr, second_view);
    EXPECT_NE(first_view.get(), second_view.get());
    EXPECT_NE(base.get(), static_cast<Column*>(first_view.get()));

    TestFixture::AppendToBase(base, values[0]);
    EXPECT_EQ(1u, base->Size());
    TestFixture::ExpectValues(first_view, {values[0]});
    TestFixture::ExpectValues(second_view, {values[0]});

    TypeParam::Append(*first_view, values[1]);
    EXPECT_EQ(2u, base->Size());
    TestFixture::ExpectValues(first_view, {values[0], values[1]});
    TestFixture::ExpectValues(second_view, {values[0], values[1]});

    TypeParam::Append(*second_view, values[2]);
    EXPECT_EQ(3u, base->Size());
    TestFixture::ExpectValues(first_view, {values[0], values[1], values[2]});
    TestFixture::ExpectValues(second_view, {values[0], values[1], values[2]});
}

TYPED_TEST(ColumnAsTest, AsOnAliasReturnsSameInstance) {
    using TypedColumn = typename TestFixture::TypedColumn;
    const auto values = TypeParam::Values();
    auto base = TestFixture::MakeBaseColumn();
    ASSERT_NE(nullptr, base);

    auto typed_alias = base->template AsStrict<TypedColumn>();
    auto same_alias = typed_alias->template As<TypedColumn>();
    ASSERT_NE(nullptr, same_alias);
    EXPECT_EQ(typed_alias.get(), same_alias.get());

    TypeParam::Append(*same_alias, values[0]);
    EXPECT_EQ(1u, base->Size());
    TestFixture::ExpectValues(typed_alias, {values[0]});

    TestFixture::AppendToBase(base, values[1]);
    EXPECT_EQ(2u, same_alias->Size());
    TestFixture::ExpectValues(same_alias, {values[0], values[1]});
}

TYPED_TEST(ColumnAsTest, ClearPreservesAllViewsAndAllowsReuse) {
    const auto values = TypeParam::Values();
    auto base = TestFixture::MakeBaseColumn();
    ASSERT_NE(nullptr, base);

    auto first_view = base->template AsStrict<typename TestFixture::TypedColumn>();
    auto second_view = base->template AsStrict<typename TestFixture::TypedColumn>();
    TestFixture::AppendToBase(base, values[0]);
    TestFixture::AppendToBase(base, values[1]);

    base->Clear();
    EXPECT_EQ(0u, base->Size());
    TestFixture::ExpectValues(first_view, {});
    TestFixture::ExpectValues(second_view, {});

    TypeParam::Append(*first_view, values[2]);
    EXPECT_EQ(1u, base->Size());
    TestFixture::ExpectValues(second_view, {values[2]});

    second_view->Clear();
    EXPECT_EQ(0u, base->Size());
    TestFixture::ExpectValues(first_view, {});
    TestFixture::ExpectValues(second_view, {});

    TestFixture::AppendToBase(base, values[1]);
    EXPECT_EQ(1u, base->Size());
    TestFixture::ExpectValues(first_view, {values[1]});
    TestFixture::ExpectValues(second_view, {values[1]});
}

TYPED_TEST(ColumnAsTest, SwapPreservesAllViewsAndAllowsReuse) {
    const auto values = TypeParam::Values();
    auto first_base = TestFixture::MakeBaseColumn();
    auto second_base = TestFixture::MakeBaseColumn();
    ASSERT_NE(nullptr, first_base);
    ASSERT_NE(nullptr, second_base);

    TestFixture::AppendToBase(first_base, values[0]);
    TestFixture::AppendToBase(second_base, values[1]);
    TestFixture::AppendToBase(second_base, values[2]);

    auto first_view = first_base->template AsStrict<typename TestFixture::TypedColumn>();
    auto first_second_view = first_base->template AsStrict<typename TestFixture::TypedColumn>();
    auto second_view = second_base->template AsStrict<typename TestFixture::TypedColumn>();
    auto second_second_view = second_base->template AsStrict<typename TestFixture::TypedColumn>();

    first_base->Swap(*second_base);
    TestFixture::ExpectValues(first_view, {values[1], values[2]});
    TestFixture::ExpectValues(first_second_view, {values[1], values[2]});
    TestFixture::ExpectValues(second_view, {values[0]});
    TestFixture::ExpectValues(second_second_view, {values[0]});

    TypeParam::Append(*first_view, values[0]);
    EXPECT_EQ(3u, first_base->Size());
    TestFixture::ExpectValues(first_second_view, {values[1], values[2], values[0]});

    first_view->Swap(*second_view);
    EXPECT_EQ(1u, first_base->Size());
    EXPECT_EQ(3u, second_base->Size());
    TestFixture::ExpectValues(first_view, {values[0]});
    TestFixture::ExpectValues(first_second_view, {values[0]});
    TestFixture::ExpectValues(second_view, {values[1], values[2], values[0]});
    TestFixture::ExpectValues(second_second_view, {values[1], values[2], values[0]});

    TypeParam::Append(*second_second_view, values[1]);
    EXPECT_EQ(4u, second_base->Size());
    TestFixture::ExpectValues(second_view, {values[1], values[2], values[0], values[1]});
}

}  // namespace
