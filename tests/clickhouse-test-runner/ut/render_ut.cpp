#include <clickhouse/client.h>
#include <clickhouse/columns/factory.h>
#include <gtest/gtest.h>
#include <netinet/in.h>

#include <cmath>
#include <limits>
#include <optional>

#include "render.h"

using namespace clickhouse;
using namespace clickhouse_test_runner;

namespace {

std::string Render(const ColumnRef& col, size_t row = 0, TextForm form = TextForm::TopLevel) {
    RenderContext ctx;
    ctx.default_time_zone = "UTC";
    std::string out;
    MakeValueWriter(col, ctx)->Write(row, form, out);
    return out;
}

template <typename T, typename... Args>
std::shared_ptr<T> Make(Args&&... args) {
    return std::make_shared<T>(std::forward<Args>(args)...);
}

std::shared_ptr<ColumnString> Strings(std::initializer_list<const char*> values) {
    auto c = Make<ColumnString>();
    for (const auto* v : values) {
        c->Append(v);
    }
    return c;
}

}  // namespace

TEST(FormatFloat, Layout) {
    // Variables, so that the compiler does not fold 0.1 + 0.2 exactly.
    volatile double a = 0.1, b = 0.2;
    // The expected text is what ClickHouse 26.10 writes for FORMAT TSV.
    EXPECT_EQ("1000000000000000", FormatFloat64(1e15));
    EXPECT_EQ("100000000000000000000", FormatFloat64(1e20));
    EXPECT_EQ("1e21", FormatFloat64(1e21));
    EXPECT_EQ("1.2345e21", FormatFloat64(1.2345e21));
    EXPECT_EQ("123450000000000000000", FormatFloat64(1.2345e20));
    EXPECT_EQ("0.0001", FormatFloat64(0.0001));
    EXPECT_EQ("0.000001", FormatFloat64(1e-6));
    EXPECT_EQ("0.0000015", FormatFloat64(1.5e-6));
    EXPECT_EQ("1e-7", FormatFloat64(1e-7));
    EXPECT_EQ("1.5e-7", FormatFloat64(1.5e-7));
    EXPECT_EQ("1.5e300", FormatFloat64(1.5e300));
    EXPECT_EQ("0.30000000000000004", FormatFloat64(a + b));
    EXPECT_EQ("5e-324", FormatFloat64(5e-324));
    EXPECT_EQ("-12345678.9", FormatFloat64(-12345678.9));
    EXPECT_EQ("100", FormatFloat64(100));
    EXPECT_EQ("nan", FormatFloat64(std::nan("")));
    EXPECT_EQ("inf", FormatFloat64(std::numeric_limits<double>::infinity()));
    EXPECT_EQ("-inf", FormatFloat64(-std::numeric_limits<double>::infinity()));
    EXPECT_EQ("-0", FormatFloat64(-0.0));
    EXPECT_EQ("0", FormatFloat64(0.0));
    EXPECT_EQ("0.1", FormatFloat32(0.1f));
    EXPECT_EQ("3.4028235e38", FormatFloat32(3.4028235e38f));
    EXPECT_EQ("16777216", FormatFloat32(16777216.0f));
}

TEST(FormatIPv6, Rules) {
    const std::vector<std::pair<const char*, const char*>> cases = {
        {"::ffff:1.2.3.4", "::ffff:1.2.3.4"},
        {"::1.2.3.4", "::1.2.3.4"},
        {"::", "::"},
        {"::1", "::1"},
        {"2001:db8::1", "2001:db8::1"},
        {"1::", "1::"},
        {"2001:db8:0:1::1", "2001:db8:0:1::1"},
        {"1:0:0:2:0:0:0:3", "1:0:0:2::3"},
        {"1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8"},
    };
    for (const auto& [in, want] : cases) {
        auto col = Make<ColumnIPv6>();
        col->Append(std::string_view(in));
        EXPECT_EQ(want, Render(col)) << in;
    }
}

TEST(FormatTime, Values) {
    EXPECT_EQ("-01:02:03", FormatTime(-(3600 + 2 * 60 + 3), 0));
    EXPECT_EQ("100:00:00.500", FormatTime((100 * 3600) * 1000LL + 500, 3));
    // The server writes a value above 999:59:59 as 999:59:59.
    EXPECT_EQ("999:59:59.00", FormatTime((2160LL * 3600 + 12) * 100, 2));
    EXPECT_EQ("-999:59:59", FormatTime(-2000LL * 3600, 0));
    EXPECT_EQ("00:00:01.1234567", FormatTime(11234567, 7));
}

TEST(FormatDateTime, Values) {
    EXPECT_EQ("1970-01-01", FormatDate(0));
    EXPECT_EQ("1900-01-01", FormatDate(-25567));
    EXPECT_EQ("2299-12-31", FormatDate(120529));
    EXPECT_EQ("2020-01-01 09:00:00", FormatDateTime(1577836800, "Asia/Tokyo"));
    EXPECT_EQ("2020-01-01 00:00:00.120", FormatDateTime64(1577836800120LL, 3, "UTC"));
    EXPECT_EQ("1969-12-31 23:59:59.9", FormatDateTime64(-1, 1, "UTC"));
    EXPECT_EQ("1900-01-01 00:00:00.000000000", FormatDateTime64(-2208988800LL * 1000000000LL, 9, "UTC"));
    EXPECT_EQ("2299-12-31 23:59:59", FormatDateTime64(10413791999LL, 0, "UTC"));
    EXPECT_THROW(FormatDateTime(0, "No/Such_Zone"), UnsupportedType);
}

TEST(FormatDateTime, YearBounds) {
    // After the year 9999 and before the year 0 (local time), the server
    // writes the bound; the fractional digits stay.
    EXPECT_EQ("9999-12-31 23:59:59", FormatDateTime64(1234567891011LL, 0, "UTC"));
    EXPECT_EQ("9999-12-31 23:59:59.807", FormatDateTime64(std::numeric_limits<int64_t>::max(), 3, "UTC"));
    EXPECT_EQ("0000-01-01 00:00:00", FormatDateTime64(std::numeric_limits<int64_t>::min(), 0, "UTC"));
    EXPECT_EQ("0000-01-01 00:00:00", FormatDateTime(-62167219201LL, "UTC"));
    EXPECT_EQ("0001-01-01 00:00:00", FormatDateTime(-62135596800LL, "UTC"));
    EXPECT_EQ("9999-12-31 23:59:58", FormatDateTime(253402300799LL - 9 * 3600 - 1, "Asia/Tokyo"));
    EXPECT_EQ("9999-12-31 23:59:59", FormatDateTime(253402300799LL, "Asia/Tokyo"));
    EXPECT_EQ("9999-12-31", FormatDate(3000000));
    EXPECT_EQ("0000-01-01", FormatDate(-719528));
    EXPECT_EQ("0000-01-01", FormatDate(-719529));
    EXPECT_EQ("0000-12-31", FormatDate(-719528 + 365));
}

TEST(FormatDateTime, FixedOffsetZones) {
    EXPECT_EQ("1970-01-01 00:00:00", FormatDateTime(0, "Fixed/UTC+00:00:00"));
    EXPECT_EQ("1970-01-01 05:30:00", FormatDateTime(0, "Fixed/UTC+05:30:00"));
    EXPECT_EQ("1969-12-31 16:00:00", FormatDateTime(0, "Fixed/UTC-08:00:00"));
    EXPECT_EQ("1970-01-01 14:00:00.000", FormatDateTime64(0, 3, "Fixed/UTC+14:00:00"));
    EXPECT_EQ("1969-12-31 10:00:00.000", FormatDateTime64(0, 3, "Fixed/UTC-14:00:00"));
    EXPECT_EQ("1970-01-01 01:15:01", FormatDateTime(1, "Fixed/UTC+01:15:00"));
    // A fixed zone between two named zones does not change the named ones.
    EXPECT_EQ("2020-01-01 09:00:00", FormatDateTime(1577836800, "Asia/Tokyo"));
    EXPECT_EQ("2020-01-01 05:30:00", FormatDateTime(1577836800, "Fixed/UTC+05:30:00"));
    EXPECT_EQ("2020-01-01 09:00:00", FormatDateTime(1577836800, "Asia/Tokyo"));
    // Only the server's one spelling is a fixed zone.
    EXPECT_THROW(FormatDateTime(0, "Fixed/UTC+5:30:00"), UnsupportedType);
    EXPECT_THROW(FormatDateTime(0, "Fixed/UTC 05:30:00"), UnsupportedType);
    EXPECT_THROW(FormatDateTime(0, "Fixed/UTC+05:30"), UnsupportedType);
}

TEST(TrimDecimalZeros, Values) {
    EXPECT_EQ("1.5", TrimDecimalZeros("1.50"));
    EXPECT_EQ("0", TrimDecimalZeros("0.00"));
    EXPECT_EQ("100", TrimDecimalZeros("100.000"));
    EXPECT_EQ("100", TrimDecimalZeros("100"));
    EXPECT_EQ("-0.001", TrimDecimalZeros("-0.00100"));
}

TEST(RenderValue, Strings) {
    auto s = Make<ColumnString>();
    s->Append(std::string("a\tb\\c\nd'e\0\b\f\x01", 13));
    EXPECT_EQ("a\\tb\\\\c\\nd\\'e\\0\\b\\f\x01", Render(s));
    EXPECT_EQ("'a\\tb\\\\c\\nd\\'e\\0\\b\\f\x01'", Render(s, 0, TextForm::Nested));
    EXPECT_EQ(std::string("a\tb\\c\nd'e\0\b\f\x01", 13), Render(s, 0, TextForm::TopLevelRaw));

    auto fs = Make<ColumnFixedString>(4);
    fs->Append("ab");
    EXPECT_EQ("ab\\0\\0", Render(fs));
}

TEST(RenderValue, Nullable) {
    auto n = Make<ColumnNullableT<ColumnString>>();
    n->Append(std::nullopt);
    n->Append(std::optional<std::string_view>("x"));
    EXPECT_EQ("\\N", Render(n, 0));
    EXPECT_EQ("NULL", Render(n, 0, TextForm::Nested));
    EXPECT_EQ("\\N", Render(n, 0, TextForm::TopLevelRaw));
    EXPECT_EQ("x", Render(n, 1));
}

TEST(RenderValue, Array) {
    auto items = Make<ColumnNullableT<ColumnString>>();
    items->Append(std::optional<std::string_view>("a'b"));
    items->Append(std::nullopt);
    auto arr = Make<ColumnArray>(Make<ColumnNullableT<ColumnString>>());
    arr->AppendAsColumn(items);
    arr->AppendAsColumn(items->Slice(0, 0));
    EXPECT_EQ("['a\\'b',NULL]", Render(arr, 0));
    EXPECT_EQ("[]", Render(arr, 1));
}

TEST(RenderValue, Tuple) {
    auto a = Make<ColumnUInt8>();
    a->Append(1);
    auto n = Make<ColumnNullableT<ColumnUInt8>>();
    n->Append(std::nullopt);
    auto t = Make<ColumnTuple>(std::vector<ColumnRef>{a, Strings({"x"}), n});
    EXPECT_EQ("(1,'x',NULL)", Render(t));
}

TEST(RenderValue, MapKeepsServerOrder) {
    auto keys   = Strings({"k", "a"});
    auto values = Make<ColumnUInt8>();
    values->Append(1);
    values->Append(2);
    auto entries = Make<ColumnArray>(Make<ColumnTuple>(std::vector<ColumnRef>{Make<ColumnString>(), Make<ColumnUInt8>()}));
    entries->AppendAsColumn(Make<ColumnTuple>(std::vector<ColumnRef>{keys, values}));
    auto map = Make<ColumnMap>(entries);
    EXPECT_EQ("{'k':1,'a':2}", Render(map));
}

TEST(RenderValue, Numbers) {
    auto i128 = Make<ColumnInt128>();
    i128->Append(Bignum::MakeInt128(-1, ~uint64_t{0} - 4));  // -5
    EXPECT_EQ("-5", Render(i128));

    auto u64 = Make<ColumnUInt64>();
    u64->Append(std::numeric_limits<uint64_t>::max());
    EXPECT_EQ("18446744073709551615", Render(u64));

    auto i8 = Make<ColumnInt8>();
    i8->Append(-1);
    EXPECT_EQ("-1", Render(i8));

    auto d = Make<ColumnDecimal>(9, 2);
    d->Append(std::string("1.50"));
    d->Append(std::string("0"));
    EXPECT_EQ("1.5", Render(d, 0));
    EXPECT_EQ("0", Render(d, 1));

    auto f = Make<ColumnFloat64>();
    f->Append(std::nan(""));
    f->Append(std::numeric_limits<double>::infinity());
    auto arr = Make<ColumnArray>(Make<ColumnFloat64>());
    arr->AppendAsColumn(f);
    EXPECT_EQ("[nan,inf]", Render(arr));

    auto b = Make<ColumnBool>();
    b->Append(true);
    EXPECT_EQ("true", Render(b));
}

TEST(RenderValue, DatesAndTimes) {
    auto dt64 = Make<ColumnDateTime64>(3);
    dt64->Append(1577836800120LL);
    EXPECT_EQ("2020-01-01 00:00:00.120", Render(dt64));
    EXPECT_EQ("'2020-01-01 00:00:00.120'", Render(dt64, 0, TextForm::Nested));

    auto tokyo = Make<ColumnDateTime>(std::string("Asia/Tokyo"));
    tokyo->Append(1577836800);
    EXPECT_EQ("2020-01-01 09:00:00", Render(tokyo));

    auto date = Make<ColumnDate>();
    date->Append(std::time_t{1577923200});
    auto arr = Make<ColumnArray>(Make<ColumnDate>());
    arr->AppendAsColumn(date);
    EXPECT_EQ("['2020-01-02']", Render(arr));

    auto time = Make<ColumnTime>();
    time->Append(-(3600 + 2 * 60 + 3));
    EXPECT_EQ("-01:02:03", Render(time));

    auto time64 = Make<ColumnTime64>(3);
    time64->Append(100LL * 3600 * 1000 + 500);
    EXPECT_EQ("100:00:00.500", Render(time64));
}

TEST(RenderValue, UUIDAndAddresses) {
    auto u = Make<ColumnUUID>();
    u->Append(UUID{0x61f0c4045cb311e7ULL, 0x907ba6006ad3dba0ULL});
    EXPECT_EQ("61f0c404-5cb3-11e7-907b-a6006ad3dba0", Render(u));

    auto v4 = Make<ColumnIPv4>();
    v4->Append(std::string("1.2.3.4"));
    EXPECT_EQ("1.2.3.4", Render(v4));

    auto v6 = Make<ColumnIPv6>();
    v6->Append(std::string_view("::ffff:1.2.3.4"));
    auto arr = Make<ColumnArray>(Make<ColumnIPv6>());
    arr->AppendAsColumn(v6);
    EXPECT_EQ("['::ffff:1.2.3.4']", Render(arr));
}

TEST(RenderValue, EnumAndLowCardinality) {
    auto e = Make<ColumnEnum8>(Type::CreateEnum8({{"a", 1}, {"b", 2}}));
    e->Append(std::string("b"));
    EXPECT_EQ("b", Render(e));
    EXPECT_EQ("'b'", Render(e, 0, TextForm::Nested));

    auto lc = Make<ColumnLowCardinalityT<ColumnString>>();
    lc->Append("a");
    EXPECT_EQ("a", Render(lc));

    auto lcn = Make<ColumnLowCardinalityT<ColumnNullableT<ColumnString>>>();
    lcn->Append(std::nullopt);
    lcn->Append(std::optional<std::string_view>("y"));
    EXPECT_EQ("\\N", Render(lcn, 0));
    EXPECT_EQ("y", Render(lcn, 1));
}

TEST(RenderValue, Geo) {
    auto p = Make<ColumnPoint>();
    p->Append(std::make_tuple(1.5, 2.5));
    EXPECT_EQ("(1.5,2.5)", Render(p));
}

TEST(RenderValue, Nothing) {
    auto n = CreateColumnByType("Nullable(Nothing)");
    ASSERT_NE(nullptr, n);
    n->As<ColumnNullable>()->Append(true);
    EXPECT_EQ("\\N", Render(n));
}
