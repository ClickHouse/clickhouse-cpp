#pragma once

#include <clickhouse/columns/column.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

/// The renderer writes the values that clickhouse-cpp decoded as the text that
/// the server writes for the `TabSeparated` format. The upstream `.reference`
/// files hold the server's text, so a test passes only when the client decoded
/// every value correctly.
///
/// The rendering is type-directed: it follows the type of each column of the
/// block (`Column::Type()`), and reads each value with the typed column class
/// of that type (`ColumnInt32::At`, `ColumnNullable::IsNull`,
/// `ColumnLowCardinalityT<ColumnString>::At`, ...).
///
/// ClickHouse uses two text forms:
///   - top level (`serializeTextEscaped`): strings are escaped, not quoted,
///     and NULL is `\N`;
///   - nested in `Array`, `Tuple` and `Map` (`serializeTextQuoted`): strings
///     are quoted, and NULL is `NULL`.
namespace clickhouse_test_runner {

/// A type (or a value) that the renderer cannot write. The statement fails,
/// so a test cannot pass by accident.
class UnsupportedType : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

struct RenderContext {
    /// The time zone of `DateTime` and `DateTime64` values whose type has no
    /// time zone: the server time zone from the handshake
    /// (`ServerInfo::timezone`).
    std::string default_time_zone = "UTC";
};

enum class TextForm {
    TopLevel,     ///< Escaped strings, `\N`.
    TopLevelRaw,  ///< The `TSVRaw` formats: strings as they are, `\N`.
    Nested,       ///< Quoted strings, `NULL`.
};

/// Writes the values of one column.
class ValueWriter {
public:
    virtual ~ValueWriter()                                                = default;
    virtual void Write(size_t row, TextForm form, std::string& out) const = 0;
};

/// Makes the writer for a column of a block. Throws UnsupportedType for a
/// column type that the renderer does not support.
std::unique_ptr<ValueWriter> MakeValueWriter(const clickhouse::ColumnRef& column, const RenderContext& ctx);

/// Writes `s` with the escapes of ClickHouse's `writeAnyEscapedString` (with
/// the single quote as the quote character), quoted when `quoted`.
void WriteEscapedString(std::string& out, std::string_view s, bool quoted);

/// The shortest text that reads back as the same value, in positional
/// notation when the decimal exponent is in [-6, 21), else in scientific
/// notation without a `+` sign or exponent padding (`1e21`, `1.5e-7`): the
/// layout of ClickHouse (and of JavaScript's Number.prototype.toString).
std::string FormatFloat64(double v);
std::string FormatFloat32(float v);

/// `YYYY-MM-DD` of a day number (days since 1970-01-01, can be negative);
/// after the year 9999 `9999-12-31`, before the year 0 `0000-01-01`.
std::string FormatDate(int64_t days);

/// `YYYY-MM-DD hh:mm:ss` of a Unix time in a time zone of the tz database, or
/// in a fixed-offset zone of the server (`Fixed/UTC+HH:MM:SS`). After the year
/// 9999 and before the year 0, the server's bounds `9999-12-31 23:59:59` and
/// `0000-01-01 00:00:00`. Throws UnsupportedType when the time zone is not in
/// the tz database.
std::string FormatDateTime(int64_t seconds, const std::string& time_zone);

/// `YYYY-MM-DD hh:mm:ss[.f...]` with exactly `precision` fractional digits.
std::string FormatDateTime64(int64_t ticks, size_t precision, const std::string& time_zone);

/// `[-]hh:mm:ss[.f...]` of a `Time` / `Time64` value. The hours can be more
/// than 24. The server writes a value above 999:59:59 as 999:59:59.
std::string FormatTime(int64_t ticks, size_t precision);

/// ClickHouse's `formatIPv6`: the longest run of two or more zero words
/// becomes `::`, and an IPv4-mapped (`::ffff:a.b.c.d`) or IPv4-compatible
/// (`::a.b.c.d`) address ends in dotted form.
std::string FormatIPv6(const unsigned char (&bytes)[16]);

/// `a.b.c.d` of the four address bytes in network order.
std::string FormatIPv4(const unsigned char (&bytes)[4]);

/// The UUID text of the two halves that clickhouse-cpp gives.
std::string FormatUUID(uint64_t high, uint64_t low);

/// Removes the trailing zeros (and a trailing point) of a decimal text, as
/// the server does with `output_format_decimal_trailing_zeros = 0`.
std::string TrimDecimalZeros(std::string text);

}  // namespace clickhouse_test_runner
