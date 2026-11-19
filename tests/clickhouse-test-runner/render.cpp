#include "render.h"

#include <clickhouse/client.h>
#include <netinet/in.h>
#include <sys/stat.h>

#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

namespace clickhouse_test_runner {

using namespace clickhouse;

namespace {

bool Quoted(TextForm form) {
    return form == TextForm::Nested;
}

void WriteNull(TextForm form, std::string& out) {
    out += form == TextForm::Nested ? "NULL" : "\\N";
}

/// Writes text that has no characters to escape (dates, UUIDs, addresses),
/// quoted when nested.
void WriteQuotedIf(std::string& out, std::string_view s, TextForm form) {
    if (Quoted(form)) {
        out += '\'';
        out += s;
        out += '\'';
    } else {
        out += s;
    }
}

void WriteStringValue(std::string& out, std::string_view s, TextForm form) {
    if (form == TextForm::TopLevelRaw) {
        out += s;
    } else {
        WriteEscapedString(out, s, Quoted(form));
    }
}

template <typename T>
void WriteInteger(std::string& out, T v) {
    char buf[32];
    const auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v);
    (void)ec;
    out.append(buf, ptr);
}

int64_t FloorDiv(int64_t a, int64_t b) {
    int64_t q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0))) {
        --q;
    }
    return q;
}

int64_t Pow10(size_t n) {
    int64_t p = 1;
    for (size_t i = 0; i < n; ++i) {
        p *= 10;
    }
    return p;
}

std::string TimeZoneDir() {
    if (const char* dir = std::getenv("TZDIR"); dir && *dir) {
        return dir;
    }
    return "/usr/share/zoneinfo";
}

bool TwoDigits(const char* p, int64_t& value) {
    if (p[0] < '0' || p[0] > '9' || p[1] < '0' || p[1] > '9') {
        return false;
    }
    value = (p[0] - '0') * 10 + (p[1] - '0');
    return true;
}

/// The offset east of UTC, in seconds, of a fixed-offset zone name of the
/// server (`Fixed/UTC+HH:MM:SS`, `Fixed/UTC-HH:MM:SS`: the server's cctz makes
/// these zones, the tz database has no files for them). The server accepts
/// only this one spelling, so the shim reads only it.
std::optional<int64_t> FixedOffset(const std::string& time_zone) {
    static constexpr std::string_view kPrefix = "Fixed/UTC";
    const std::string_view name               = time_zone;
    if (name.size() != kPrefix.size() + 9 || name.substr(0, kPrefix.size()) != kPrefix) {
        return std::nullopt;
    }
    const char sign = name[kPrefix.size()];
    const char* hms = name.data() + kPrefix.size() + 1;  // HH:MM:SS
    int64_t h       = 0;
    int64_t m       = 0;
    int64_t s       = 0;
    if ((sign != '+' && sign != '-') || !TwoDigits(hms, h) || hms[2] != ':' || !TwoDigits(hms + 3, m) || hms[5] != ':' ||
        !TwoDigits(hms + 6, s)) {
        return std::nullopt;
    }
    const int64_t offset = h * 3600 + m * 60 + s;
    return sign == '-' ? -offset : offset;
}

/// localtime_r in a named time zone. The process switches TZ only when the
/// zone changes, so a column of one zone loads the zone file once. A
/// fixed-offset zone of the server is UTC shifted by its offset.
struct tm LocalTime(int64_t seconds, const std::string& time_zone) {
    if (const auto offset = FixedOffset(time_zone)) {
        const time_t t = static_cast<time_t>(seconds + *offset);
        struct tm result;
        if (!gmtime_r(&t, &result)) {
            throw UnsupportedType("time " + std::to_string(seconds) + " is out of the range of gmtime_r");
        }
        return result;
    }
    static std::string current;
    static bool initialized = false;
    if (!initialized || time_zone != current) {
        struct stat st;
        const std::string path = TimeZoneDir() + "/" + time_zone;
        if (time_zone.empty() || time_zone.find("..") != std::string::npos || stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            throw UnsupportedType("time zone '" + time_zone + "' is not in the tz database (" + TimeZoneDir() + ")");
        }
        setenv("TZ", time_zone.c_str(), 1);
        tzset();
        current     = time_zone;
        initialized = true;
    }
    const time_t t = static_cast<time_t>(seconds);
    struct tm result;
    if (!localtime_r(&t, &result)) {
        throw UnsupportedType("time " + std::to_string(seconds) + " is out of the range of localtime_r");
    }
    return result;
}

/// The server writes a date after the year 9999 as 9999-12-31 23:59:59, and
/// a date before the year 0 as 0000-01-01 00:00:00 (the local time of the
/// column zone). The fractional digits of a DateTime64 stay.
constexpr int64_t kMaxTextYear          = 9999;
constexpr std::string_view kMaxDateText = "9999-12-31";
constexpr std::string_view kMaxTimeText = " 23:59:59";
constexpr std::string_view kMinDateText = "0000-01-01";
constexpr std::string_view kMinTimeText = " 00:00:00";

std::string BoundText(bool max, bool with_time) {
    std::string out(max ? kMaxDateText : kMinDateText);
    if (with_time) {
        out += max ? kMaxTimeText : kMinTimeText;
    }
    return out;
}

std::string FormatTm(const struct tm& t, bool with_time) {
    const int64_t year = static_cast<int64_t>(t.tm_year) + 1900;
    if (year > kMaxTextYear || year < 0) {
        return BoundText(year > kMaxTextYear, with_time);
    }
    char buf[64];
    if (with_time) {
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min,
                      t.tm_sec);
    } else {
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    }
    return buf;
}

std::string TimeZoneOf(const std::string& type_time_zone, const RenderContext& ctx) {
    return type_time_zone.empty() ? ctx.default_time_zone : type_time_zone;
}

// ---------------------------------------------------------------------------
// The value formatters: one per value type that the typed columns return.
// The column writers and the LowCardinality writers share them.

struct StringFmt {
    void operator()(std::string& out, std::string_view v, TextForm form) const { WriteStringValue(out, v, form); }
};

template <typename T>
struct IntFmt {
    void operator()(std::string& out, T v, TextForm) const { WriteInteger(out, v); }
};

struct Int128Fmt {
    void operator()(std::string& out, const Int128& v, TextForm) const { out += Bignum::Int128ToString(v); }
};

struct UInt128Fmt {
    void operator()(std::string& out, const UInt128& v, TextForm) const { out += Bignum::UInt128ToString(v); }
};

struct Float32Fmt {
    void operator()(std::string& out, float v, TextForm) const { out += FormatFloat32(v); }
};

struct Float64Fmt {
    void operator()(std::string& out, double v, TextForm) const { out += FormatFloat64(v); }
};

struct BoolFmt {
    void operator()(std::string& out, bool v, TextForm) const { out += v ? "true" : "false"; }
};

/// ColumnDate and ColumnDate32 give the day as a time_t (days * 86400).
struct DateFmt {
    void operator()(std::string& out, std::time_t v, TextForm form) const {
        WriteQuotedIf(out, FormatDate(FloorDiv(static_cast<int64_t>(v), 86400)), form);
    }
};

struct DateTimeFmt {
    std::string time_zone;
    void operator()(std::string& out, std::time_t v, TextForm form) const {
        WriteQuotedIf(out, FormatDateTime(static_cast<int64_t>(v), time_zone), form);
    }
};

struct UUIDFmt {
    void operator()(std::string& out, const UUID& v, TextForm form) const { WriteQuotedIf(out, FormatUUID(v.first, v.second), form); }
};

struct IPv4Fmt {
    void operator()(std::string& out, const in_addr& v, TextForm form) const {
        unsigned char bytes[4];
        std::memcpy(bytes, &v.s_addr, 4);
        WriteQuotedIf(out, FormatIPv4(bytes), form);
    }
};

struct IPv6Fmt {
    void operator()(std::string& out, const in6_addr& v, TextForm form) const {
        unsigned char bytes[16];
        std::memcpy(bytes, &v, 16);
        WriteQuotedIf(out, FormatIPv6(bytes), form);
    }
};

// ---------------------------------------------------------------------------
// Writers.

/// A column whose typed class has `At(row)`; `Fmt` writes the value.
template <typename ColumnType, typename Fmt>
class TypedWriter : public ValueWriter {
public:
    TypedWriter(std::shared_ptr<const ColumnType> col, Fmt fmt) : col_(std::move(col)), fmt_(std::move(fmt)) {}

    void Write(size_t row, TextForm form, std::string& out) const override { fmt_(out, col_->At(row), form); }

private:
    std::shared_ptr<const ColumnType> col_;
    Fmt fmt_;
};

template <typename ColumnType, typename Fmt>
std::unique_ptr<ValueWriter> Typed(const ColumnRef& column, Fmt fmt = Fmt{}) {
    auto col = column->As<ColumnType>();
    if (!col) {
        throw UnsupportedType("column of type " + column->Type()->GetName() + " is not a " + typeid(ColumnType).name());
    }
    return std::make_unique<TypedWriter<ColumnType, Fmt>>(std::move(col), std::move(fmt));
}

/// `Nothing` has no values: the server sends it only as `Nullable(Nothing)`
/// with every value NULL, or in empty arrays. A value that is not NULL shows
/// that the client read the data with a wrong type (clickhouse-cpp makes a
/// `Void` column for a nested type name that it does not know).
class NothingWriter : public ValueWriter {
public:
    void Write(size_t, TextForm, std::string&) const override {
        throw UnsupportedType("the client gives a value of type Nothing (Void), which has no values: it read the data with a wrong type");
    }
};

class FixedStringWriter : public ValueWriter {
public:
    explicit FixedStringWriter(std::shared_ptr<const ColumnFixedString> col) : col_(std::move(col)) {}
    void Write(size_t row, TextForm form, std::string& out) const override { WriteStringValue(out, col_->At(row), form); }

private:
    std::shared_ptr<const ColumnFixedString> col_;
};

class EnumWriter8 : public ValueWriter {
public:
    explicit EnumWriter8(std::shared_ptr<const ColumnEnum8> col) : col_(std::move(col)) {}
    void Write(size_t row, TextForm form, std::string& out) const override { WriteStringValue(out, col_->NameAt(row), form); }

private:
    std::shared_ptr<const ColumnEnum8> col_;
};

class EnumWriter16 : public ValueWriter {
public:
    explicit EnumWriter16(std::shared_ptr<const ColumnEnum16> col) : col_(std::move(col)) {}
    void Write(size_t row, TextForm form, std::string& out) const override { WriteStringValue(out, col_->NameAt(row), form); }

private:
    std::shared_ptr<const ColumnEnum16> col_;
};

class DecimalWriter : public ValueWriter {
public:
    explicit DecimalWriter(std::shared_ptr<const ColumnDecimal> col) : col_(std::move(col)) {}
    void Write(size_t row, TextForm, std::string& out) const override { out += TrimDecimalZeros(col_->StringAt(row)); }

private:
    std::shared_ptr<const ColumnDecimal> col_;
};

class DateTime64Writer : public ValueWriter {
public:
    DateTime64Writer(std::shared_ptr<const ColumnDateTime64> col, std::string time_zone)
        : col_(std::move(col)), time_zone_(std::move(time_zone)) {}
    void Write(size_t row, TextForm form, std::string& out) const override {
        WriteQuotedIf(out, FormatDateTime64(col_->At(row), col_->GetPrecision(), time_zone_), form);
    }

private:
    std::shared_ptr<const ColumnDateTime64> col_;
    std::string time_zone_;
};

class TimeWriter : public ValueWriter {
public:
    explicit TimeWriter(std::shared_ptr<const ColumnTime> col) : col_(std::move(col)) {}
    void Write(size_t row, TextForm form, std::string& out) const override { WriteQuotedIf(out, FormatTime(col_->At(row), 0), form); }

private:
    std::shared_ptr<const ColumnTime> col_;
};

class Time64Writer : public ValueWriter {
public:
    explicit Time64Writer(std::shared_ptr<const ColumnTime64> col) : col_(std::move(col)) {}
    void Write(size_t row, TextForm form, std::string& out) const override {
        WriteQuotedIf(out, FormatTime(col_->At(row), col_->GetPrecision()), form);
    }

private:
    std::shared_ptr<const ColumnTime64> col_;
};

class NullableWriter : public ValueWriter {
public:
    NullableWriter(std::shared_ptr<const ColumnNullable> col, std::unique_ptr<ValueWriter> nested)
        : col_(std::move(col)), nested_(std::move(nested)) {}
    void Write(size_t row, TextForm form, std::string& out) const override {
        if (col_->IsNull(row)) {
            WriteNull(form, out);
        } else {
            nested_->Write(row, form, out);
        }
    }

private:
    std::shared_ptr<const ColumnNullable> col_;
    std::unique_ptr<ValueWriter> nested_;
};

class ArrayWriter : public ValueWriter {
public:
    ArrayWriter(std::shared_ptr<const ColumnArray> col, std::unique_ptr<ValueWriter> items)
        : col_(std::move(col)), items_(std::move(items)) {}
    void Write(size_t row, TextForm, std::string& out) const override {
        const size_t offset = col_->GetOffset(row);
        const size_t size   = col_->GetSize(row);
        out += '[';
        for (size_t i = 0; i < size; ++i) {
            if (i > 0) {
                out += ',';
            }
            items_->Write(offset + i, TextForm::Nested, out);
        }
        out += ']';
    }

private:
    std::shared_ptr<const ColumnArray> col_;
    std::unique_ptr<ValueWriter> items_;
};

class TupleWriter : public ValueWriter {
public:
    explicit TupleWriter(std::vector<std::unique_ptr<ValueWriter>> elements) : elements_(std::move(elements)) {}
    void Write(size_t row, TextForm, std::string& out) const override {
        out += '(';
        for (size_t i = 0; i < elements_.size(); ++i) {
            if (i > 0) {
                out += ',';
            }
            elements_[i]->Write(row, TextForm::Nested, out);
        }
        out += ')';
    }

private:
    std::vector<std::unique_ptr<ValueWriter>> elements_;
};

/// A Map value: ColumnMap::GetAsColumn gives the entries of one row, as the
/// server sent them, as a column of (key, value) tuples.
class MapWriter : public ValueWriter {
public:
    MapWriter(std::shared_ptr<const ColumnMap> col, RenderContext ctx) : col_(std::move(col)), ctx_(std::move(ctx)) {}
    void Write(size_t row, TextForm, std::string& out) const override {
        const auto entries = col_->GetAsColumn(row);
        const auto tuple   = entries->As<ColumnTuple>();
        if (!tuple || tuple->TupleSize() != 2) {
            throw UnsupportedType("ColumnMap::GetAsColumn did not give a column of (key, value) tuples, but " + entries->Type()->GetName());
        }
        const auto keys   = MakeValueWriter(tuple->At(0), ctx_);
        const auto values = MakeValueWriter(tuple->At(1), ctx_);
        out += '{';
        for (size_t i = 0; i < tuple->Size(); ++i) {
            if (i > 0) {
                out += ',';
            }
            keys->Write(i, TextForm::Nested, out);
            out += ':';
            values->Write(i, TextForm::Nested, out);
        }
        out += '}';
    }

private:
    std::shared_ptr<const ColumnMap> col_;
    RenderContext ctx_;
};

/// A LowCardinality value, read with ColumnLowCardinalityT<Dictionary>::At.
template <typename Dictionary, typename Fmt>
class LowCardinalityWriter : public ValueWriter {
public:
    LowCardinalityWriter(std::shared_ptr<ColumnLowCardinalityT<Dictionary>> col, Fmt fmt) : col_(std::move(col)), fmt_(std::move(fmt)) {}
    void Write(size_t row, TextForm form, std::string& out) const override {
        if constexpr (IsNullable<Dictionary>) {
            const auto v = col_->At(row);
            if (!v) {
                WriteNull(form, out);
                return;
            }
            fmt_(out, *v, form);
        } else {
            fmt_(out, col_->At(row), form);
        }
    }

private:
    std::shared_ptr<ColumnLowCardinalityT<Dictionary>> col_;
    Fmt fmt_;
};

template <typename Dictionary, typename Fmt>
std::unique_ptr<ValueWriter> LowCardinalityOf(const ColumnRef& column, bool nullable, Fmt fmt = Fmt{}) {
    ValidationError error;
    if (nullable) {
        auto col = ColumnLowCardinalityT<ColumnNullableT<Dictionary>>::Wrap(column, &error);
        if (!col) {
            throw UnsupportedType("cannot read " + column->Type()->GetName() + " as ColumnLowCardinalityT: " + error.what());
        }
        return std::make_unique<LowCardinalityWriter<ColumnNullableT<Dictionary>, Fmt>>(std::move(col), std::move(fmt));
    }
    auto col = ColumnLowCardinalityT<Dictionary>::Wrap(column, &error);
    if (!col) {
        throw UnsupportedType("cannot read " + column->Type()->GetName() + " as ColumnLowCardinalityT: " + error.what());
    }
    return std::make_unique<LowCardinalityWriter<Dictionary, Fmt>>(std::move(col), std::move(fmt));
}

std::unique_ptr<ValueWriter> MakeLowCardinalityWriter(const ColumnRef& column, const RenderContext& ctx) {
    TypeRef dict  = column->Type()->As<LowCardinalityType>()->GetNestedType();
    bool nullable = false;
    if (dict->GetCode() == Type::Nullable) {
        nullable = true;
        dict     = dict->As<NullableType>()->GetNestedType();
    }
    switch (dict->GetCode()) {
        case Type::String:
            return LowCardinalityOf<ColumnString, StringFmt>(column, nullable);
        case Type::FixedString:
            return LowCardinalityOf<ColumnFixedString, StringFmt>(column, nullable);
        case Type::Int8:
            return LowCardinalityOf<ColumnInt8, IntFmt<int8_t>>(column, nullable);
        case Type::Int16:
            return LowCardinalityOf<ColumnInt16, IntFmt<int16_t>>(column, nullable);
        case Type::Int32:
            return LowCardinalityOf<ColumnInt32, IntFmt<int32_t>>(column, nullable);
        case Type::Int64:
            return LowCardinalityOf<ColumnInt64, IntFmt<int64_t>>(column, nullable);
        case Type::UInt8:
            return LowCardinalityOf<ColumnUInt8, IntFmt<uint8_t>>(column, nullable);
        case Type::UInt16:
            return LowCardinalityOf<ColumnUInt16, IntFmt<uint16_t>>(column, nullable);
        case Type::UInt32:
            return LowCardinalityOf<ColumnUInt32, IntFmt<uint32_t>>(column, nullable);
        case Type::UInt64:
            return LowCardinalityOf<ColumnUInt64, IntFmt<uint64_t>>(column, nullable);
        case Type::Int128:
            return LowCardinalityOf<ColumnInt128, Int128Fmt>(column, nullable);
        case Type::UInt128:
            return LowCardinalityOf<ColumnUInt128, UInt128Fmt>(column, nullable);
        case Type::Float32:
            return LowCardinalityOf<ColumnFloat32, Float32Fmt>(column, nullable);
        case Type::Float64:
            return LowCardinalityOf<ColumnFloat64, Float64Fmt>(column, nullable);
        case Type::Date:
            return LowCardinalityOf<ColumnDate, DateFmt>(column, nullable);
        case Type::Date32:
            return LowCardinalityOf<ColumnDate32, DateFmt>(column, nullable);
        case Type::DateTime:
            return LowCardinalityOf<ColumnDateTime, DateTimeFmt>(column, nullable,
                                                                 DateTimeFmt{TimeZoneOf(dict->As<DateTimeType>()->Timezone(), ctx)});
        case Type::UUID:
            return LowCardinalityOf<ColumnUUID, UUIDFmt>(column, nullable);
        case Type::IPv4:
            return LowCardinalityOf<ColumnIPv4, IPv4Fmt>(column, nullable);
        case Type::IPv6:
            return LowCardinalityOf<ColumnIPv6, IPv6Fmt>(column, nullable);
        default:
            throw UnsupportedType("no TabSeparated renderer for type " + column->Type()->GetName());
    }
}

/// Point is a (Float64, Float64) tuple; Ring, Polygon and MultiPolygon are
/// arrays of the type before. Their text is the text of those types.
template <typename V>
void WriteGeoValue(std::string& out, const V& v) {
    if constexpr (std::is_same_v<std::decay_t<V>, std::tuple<double, double>>) {
        out += '(';
        out += FormatFloat64(std::get<0>(v));
        out += ',';
        out += FormatFloat64(std::get<1>(v));
        out += ')';
    } else {
        out += '[';
        bool first = true;
        for (const auto& item : v) {
            if (!first) {
                out += ',';
            }
            first = false;
            WriteGeoValue(out, item);
        }
        out += ']';
    }
}

struct GeoFmt {
    template <typename V>
    void operator()(std::string& out, const V& v, TextForm) const {
        WriteGeoValue(out, v);
    }
};

template <typename ColumnType>
std::shared_ptr<const ColumnType> Cast(const ColumnRef& column) {
    auto col = column->As<ColumnType>();
    if (!col) {
        throw UnsupportedType("column of type " + column->Type()->GetName() + " has an unexpected column class");
    }
    return col;
}

}  // namespace

std::unique_ptr<ValueWriter> MakeValueWriter(const ColumnRef& column, const RenderContext& ctx) {
    const auto& type = column->Type();
    switch (type->GetCode()) {
        case Type::Void:
            return std::make_unique<NothingWriter>();
        case Type::Int8:
            return Typed<ColumnInt8, IntFmt<int8_t>>(column);
        case Type::Int16:
            return Typed<ColumnInt16, IntFmt<int16_t>>(column);
        case Type::Int32:
            return Typed<ColumnInt32, IntFmt<int32_t>>(column);
        case Type::Int64:
            return Typed<ColumnInt64, IntFmt<int64_t>>(column);
        case Type::UInt8:
            return Typed<ColumnUInt8, IntFmt<uint8_t>>(column);
        case Type::UInt16:
            return Typed<ColumnUInt16, IntFmt<uint16_t>>(column);
        case Type::UInt32:
            return Typed<ColumnUInt32, IntFmt<uint32_t>>(column);
        case Type::UInt64:
            return Typed<ColumnUInt64, IntFmt<uint64_t>>(column);
        case Type::Int128:
            return Typed<ColumnInt128, Int128Fmt>(column);
        case Type::UInt128:
            return Typed<ColumnUInt128, UInt128Fmt>(column);
        case Type::Float32:
            return Typed<ColumnFloat32, Float32Fmt>(column);
        case Type::Float64:
            return Typed<ColumnFloat64, Float64Fmt>(column);
#if !CH_MAP_BOOL_TO_UINT8
        case Type::Bool:
            return Typed<ColumnBool, BoolFmt>(column);
#endif
        case Type::String:
            return Typed<ColumnString, StringFmt>(column);
        case Type::FixedString:
            return std::make_unique<FixedStringWriter>(Cast<ColumnFixedString>(column));
        case Type::Enum8:
            return std::make_unique<EnumWriter8>(Cast<ColumnEnum8>(column));
        case Type::Enum16:
            return std::make_unique<EnumWriter16>(Cast<ColumnEnum16>(column));
        case Type::Decimal:
        case Type::Decimal32:
        case Type::Decimal64:
        case Type::Decimal128:
            return std::make_unique<DecimalWriter>(Cast<ColumnDecimal>(column));
        case Type::Date:
            return Typed<ColumnDate, DateFmt>(column);
        case Type::Date32:
            return Typed<ColumnDate32, DateFmt>(column);
        case Type::DateTime:
            return Typed<ColumnDateTime, DateTimeFmt>(column, DateTimeFmt{TimeZoneOf(type->As<DateTimeType>()->Timezone(), ctx)});
        case Type::DateTime64:
            return std::make_unique<DateTime64Writer>(Cast<ColumnDateTime64>(column),
                                                      TimeZoneOf(type->As<DateTime64Type>()->Timezone(), ctx));
        case Type::Time:
            return std::make_unique<TimeWriter>(Cast<ColumnTime>(column));
        case Type::Time64:
            return std::make_unique<Time64Writer>(Cast<ColumnTime64>(column));
        case Type::UUID:
            return Typed<ColumnUUID, UUIDFmt>(column);
        case Type::IPv4:
            return Typed<ColumnIPv4, IPv4Fmt>(column);
        case Type::IPv6:
            return Typed<ColumnIPv6, IPv6Fmt>(column);
        case Type::Nullable: {
            auto col    = Cast<ColumnNullable>(column);
            auto nested = MakeValueWriter(col->Nested(), ctx);
            return std::make_unique<NullableWriter>(std::move(col), std::move(nested));
        }
        case Type::Array: {
            auto col   = Cast<ColumnArray>(column);
            auto data  = col->GetData();
            auto items = MakeValueWriter(std::const_pointer_cast<Column>(data), ctx);
            return std::make_unique<ArrayWriter>(std::move(col), std::move(items));
        }
        case Type::Tuple: {
            auto col = Cast<ColumnTuple>(column);
            std::vector<std::unique_ptr<ValueWriter>> elements;
            for (size_t i = 0; i < col->TupleSize(); ++i) {
                elements.push_back(MakeValueWriter(col->At(i), ctx));
            }
            return std::make_unique<TupleWriter>(std::move(elements));
        }
        case Type::Map:
            return std::make_unique<MapWriter>(Cast<ColumnMap>(column), ctx);
        case Type::LowCardinality:
            return MakeLowCardinalityWriter(column, ctx);
        case Type::Point:
            return Typed<ColumnPoint, GeoFmt>(column);
        case Type::Ring:
            return Typed<ColumnRing, GeoFmt>(column);
        case Type::Polygon:
            return Typed<ColumnPolygon, GeoFmt>(column);
        case Type::MultiPolygon:
            return Typed<ColumnMultiPolygon, GeoFmt>(column);
        case Type::JSON:
            return Typed<ColumnJSON, StringFmt>(column);
        default:
            break;
    }
    throw UnsupportedType("no TabSeparated renderer for type " + type->GetName());
}

void WriteEscapedString(std::string& out, std::string_view s, bool quoted) {
    if (quoted) {
        out += '\'';
    }
    for (const char c : s) {
        switch (c) {
            case '\\':
                out += "\\\\";
                break;
            case '\'':
                out += "\\'";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            case '\0':
                out += "\\0";
                break;
            default:
                out += c;
        }
    }
    if (quoted) {
        out += '\'';
    }
}

namespace {

/// Lays out the shortest digits of a finite, non-zero value. `sci` is the
/// output of std::to_chars in scientific form, such as "-1.5e-07".
std::string LayoutFloat(std::string_view sci) {
    bool neg = false;
    if (!sci.empty() && sci[0] == '-') {
        neg = true;
        sci.remove_prefix(1);
    }
    const size_t e_pos              = sci.find('e');
    const std::string_view mantissa = sci.substr(0, e_pos);
    int exp                         = 0;
    if (e_pos != std::string_view::npos) {
        std::string_view exp_text = sci.substr(e_pos + 1);
        if (!exp_text.empty() && exp_text[0] == '+') {
            exp_text.remove_prefix(1);
        }
        std::from_chars(exp_text.data(), exp_text.data() + exp_text.size(), exp);
    }
    std::string digits;
    for (const char c : mantissa) {
        if (c != '.') {
            digits += c;
        }
    }
    // The value is 0.digits * 10^n.
    const int n = exp + 1;
    const int k = static_cast<int>(digits.size());
    std::string out;
    if (k <= n && n <= 21) {
        out = digits + std::string(static_cast<size_t>(n - k), '0');
    } else if (0 < n && n <= 21) {
        out = digits.substr(0, static_cast<size_t>(n)) + "." + digits.substr(static_cast<size_t>(n));
    } else if (-6 < n && n <= 0) {
        out = "0." + std::string(static_cast<size_t>(-n), '0') + digits;
    } else {
        out = digits.substr(0, 1);
        if (k > 1) {
            out += "." + digits.substr(1);
        }
        out += "e" + std::to_string(n - 1);
    }
    return neg ? "-" + out : out;
}

template <typename F>
std::string FormatFloat(F v) {
    if (v != v) {
        return "nan";
    }
    if (v == std::numeric_limits<F>::infinity()) {
        return "inf";
    }
    if (v == -std::numeric_limits<F>::infinity()) {
        return "-inf";
    }
    if (v == 0) {
        return std::signbit(v) ? "-0" : "0";
    }
    char buf[64];
    const auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v, std::chars_format::scientific);
    (void)ec;
    return LayoutFloat(std::string_view(buf, static_cast<size_t>(ptr - buf)));
}

}  // namespace

std::string FormatFloat64(double v) {
    return FormatFloat(v);
}

std::string FormatFloat32(float v) {
    return FormatFloat(v);
}

std::string FormatDate(int64_t days) {
    // civil_from_days (Howard Hinnant's algorithm).
    const int64_t z   = days + 719468;
    const int64_t era = FloorDiv(z, 146097);
    const int64_t doe = z - era * 146097;
    const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int64_t mp  = (5 * doy + 2) / 153;
    const int64_t d   = doy - (153 * mp + 2) / 5 + 1;
    const int64_t m   = mp < 10 ? mp + 3 : mp - 9;
    const int64_t y   = yoe + era * 400 + (m <= 2 ? 1 : 0);
    if (y > kMaxTextYear || y < 0) {
        return BoundText(y > kMaxTextYear, false);
    }
    char buf[96];
    std::snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld", static_cast<long long>(y), static_cast<long long>(m),
                  static_cast<long long>(d));
    return buf;
}

std::string FormatDateTime(int64_t seconds, const std::string& time_zone) {
    // Far outside the years 0..9999 in every zone (offsets are below a day):
    // the bound text, without localtime_r (its year can overflow an int).
    constexpr int64_t kMaxUtcSeconds = 253402300799;  // 9999-12-31 23:59:59 UTC
    constexpr int64_t kMinUtcSeconds = -62167219200;  // 0000-01-01 00:00:00 UTC
    constexpr int64_t kMargin        = 2 * 86400;
    if (seconds > kMaxUtcSeconds + kMargin || seconds < kMinUtcSeconds - kMargin) {
        return BoundText(seconds > 0, true);
    }
    return FormatTm(LocalTime(seconds, time_zone), true);
}

std::string FormatDateTime64(int64_t ticks, size_t precision, const std::string& time_zone) {
    if (precision > 9) {
        throw UnsupportedType("DateTime64 precision " + std::to_string(precision) + " is out of range");
    }
    const int64_t scale   = Pow10(precision);
    const int64_t seconds = FloorDiv(ticks, scale);
    const int64_t frac    = ticks - seconds * scale;
    std::string out       = FormatDateTime(seconds, time_zone);
    if (precision > 0) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%0*lld", static_cast<int>(precision), static_cast<long long>(frac));
        out += '.';
        out += buf;
    }
    return out;
}

std::string FormatTime(int64_t ticks, size_t precision) {
    if (precision > 9) {
        throw UnsupportedType("Time64 precision " + std::to_string(precision) + " is out of range");
    }
    constexpr int64_t kMaxTimeSeconds = 999 * 3600 + 59 * 60 + 59;
    std::string out;
    // Negative values: the sign, then the absolute value.
    uint64_t abs_ticks = static_cast<uint64_t>(ticks);
    if (ticks < 0) {
        out += '-';
        abs_ticks = 0 - abs_ticks;
    }
    const uint64_t scale = static_cast<uint64_t>(Pow10(precision));
    uint64_t total       = abs_ticks / scale;
    uint64_t frac        = abs_ticks % scale;
    // The server writes a value above 999:59:59 as 999:59:59, with a zero
    // fraction; the stored value is not changed.
    if (total > static_cast<uint64_t>(kMaxTimeSeconds)) {
        total = kMaxTimeSeconds;
        frac  = 0;
    }
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%02llu:%02llu:%02llu", static_cast<unsigned long long>(total / 3600),
                  static_cast<unsigned long long>(total / 60 % 60), static_cast<unsigned long long>(total % 60));
    out += buf;
    if (precision > 0) {
        std::snprintf(buf, sizeof(buf), ".%0*llu", static_cast<int>(precision), static_cast<unsigned long long>(frac));
        out += buf;
    }
    return out;
}

std::string FormatIPv4(const unsigned char (&b)[4]) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%u.%u.%u.%u", b[0], b[1], b[2], b[3]);
    return buf;
}

std::string FormatIPv6(const unsigned char (&ip)[16]) {
    uint16_t words[8];
    for (int i = 0; i < 8; ++i) {
        words[i] = static_cast<uint16_t>((ip[2 * i] << 8) | ip[2 * i + 1]);
    }
    int best_base = -1;
    int best_len  = 0;
    for (int i = 0; i < 8;) {
        if (words[i] != 0) {
            ++i;
            continue;
        }
        int j = i;
        while (j < 8 && words[j] == 0) {
            ++j;
        }
        if (j - i > best_len) {
            best_base = i;
            best_len  = j - i;
        }
        i = j;
    }
    if (best_len < 2) {
        best_base = -1;
    }
    const bool ipv4_compatible = best_base == 0 && best_len == 6;
    const bool ipv4_mapped     = best_base == 0 && best_len == 5 && words[5] == 0xffff;

    std::string out;
    char buf[16];
    for (int i = 0; i < 8; ++i) {
        if (best_base != -1 && i >= best_base && i < best_base + best_len) {
            if (i == best_base) {
                out += ':';
            }
            continue;
        }
        if (i != 0) {
            out += ':';
        }
        if (i == 6 && (ipv4_compatible || ipv4_mapped)) {
            const unsigned char v4[4] = {ip[12], ip[13], ip[14], ip[15]};
            out += FormatIPv4(v4);
            return out;
        }
        std::snprintf(buf, sizeof(buf), "%x", words[i]);
        out += buf;
    }
    if (best_base != -1 && best_base + best_len == 8) {
        out += ':';
    }
    return out;
}

std::string FormatUUID(uint64_t high, uint64_t low) {
    char hex[33];
    std::snprintf(hex, sizeof(hex), "%016llx%016llx", static_cast<unsigned long long>(high), static_cast<unsigned long long>(low));
    std::string s(hex, 32);
    return s.substr(0, 8) + "-" + s.substr(8, 4) + "-" + s.substr(12, 4) + "-" + s.substr(16, 4) + "-" + s.substr(20, 12);
}

std::string TrimDecimalZeros(std::string text) {
    if (text.find('.') == std::string::npos) {
        return text;
    }
    while (!text.empty() && text.back() == '0') {
        text.pop_back();
    }
    if (!text.empty() && text.back() == '.') {
        text.pop_back();
    }
    if (text == "-0") {
        return "0";
    }
    return text;
}

}  // namespace clickhouse_test_runner
