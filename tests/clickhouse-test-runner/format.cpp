#include "format.h"

#include "sql_text.h"

namespace clickhouse_test_runner {

namespace {

/// Reads the format name after the FORMAT keyword that ends at `j`. On
/// true, `end` is the end of the name.
bool FormatName(std::string_view sql, size_t j, size_t& end, std::string& name) {
    const size_t k = SkipWhitespaceAndComments(sql, j);
    if (k == j || k >= sql.size()) {
        return false;
    }
    if (sql[k] == '`' || sql[k] == '"') {
        // A quoted format name: FORMAT `Null`.
        const size_t m = SkipQuoted(sql, k);
        end            = m;
        return UnquoteIdentifier(sql.substr(k, m - k), name);
    }
    size_t m = k;
    while (m < sql.size() && IsWordByte(sql[m])) {
        ++m;
    }
    if (m == k) {
        return false;
    }
    end  = m;
    name = std::string(sql.substr(k, m - k));
    return true;
}

/// Tells if the text from `i` on can follow the FORMAT clause of a query:
/// nothing, a semicolon, or a SETTINGS clause. Otherwise FORMAT is an
/// identifier, as in `SELECT format FROM t`.
bool EndsQuery(std::string_view sql, size_t i) {
    const size_t k = SkipWhitespaceAndComments(sql, i);
    if (k >= sql.size() || sql[k] == ';') {
        return true;
    }
    size_t m = k;
    while (m < sql.size() && IsWordByte(sql[m])) {
        ++m;
    }
    return EqualsIgnoreCase(sql.substr(k, m - k), "SETTINGS");
}

OutputFormat MakeFormat(const std::string& name) {
    OutputFormat f;
    f.name      = name;
    f.supported = false;
    // Format names are case-insensitive.
    const auto n = ToLower(name);
    if (n == "tabseparated" || n == "tsv") {
        f.supported = true;
    } else if (n == "tabseparatedwithnames" || n == "tsvwithnames") {
        f.supported = f.names = true;
    } else if (n == "tabseparatedwithnamesandtypes" || n == "tsvwithnamesandtypes") {
        f.supported = f.names = f.types = true;
    } else if (n == "tabseparatedraw" || n == "tsvraw" || n == "raw") {
        f.supported = f.raw = true;
    } else if (n == "tabseparatedrawwithnames" || n == "tsvrawwithnames" || n == "rawwithnames") {
        f.supported = f.raw = f.names = true;
    } else if (n == "tabseparatedrawwithnamesandtypes" || n == "tsvrawwithnamesandtypes" || n == "rawwithnamesandtypes") {
        f.supported = f.raw = f.names = f.types = true;
    } else if (n == "lineasstring") {
        // The server writes LineAsString as TabSeparatedRaw.
        f.supported = f.raw = true;
    } else if (n == "values") {
        f.supported = f.values = true;
    } else if (n == "null") {
        f.supported = f.discard = true;
    }
    return f;
}

}  // namespace

OutputFormat FindOutputFormat(std::string_view sql, std::string* without_clause) {
    int depth = 0;
    for (size_t i = 0; i < sql.size();) {
        const char c = sql[i];
        if (c == '\'' || c == '"' || c == '`') {
            i = SkipQuoted(sql, i);
        } else if (LineCommentAt(sql, i)) {
            i = SkipLineComment(sql, i);
        } else if (sql.substr(i, 2) == "/*") {
            i = SkipBlockComment(sql, i);
        } else if (c == '$') {
            const size_t e = HeredocEnd(sql, i);
            i              = e > 0 ? e : i + 1;
        } else if (c == '(' || c == '[') {
            ++depth;
            ++i;
        } else if (c == ')' || c == ']') {
            --depth;
            ++i;
        } else if (IsWordByte(c)) {
            size_t j = i;
            while (j < sql.size() && IsWordByte(sql[j])) {
                ++j;
            }
            if (depth == 0 && EqualsIgnoreCase(sql.substr(i, j - i), "FORMAT") && (i == 0 || sql[i - 1] != '.')) {
                // `ORDER BY format DESC` ends the query too: a clause keyword
                // after FORMAT shows an identifier named `format`.
                size_t end = 0;
                std::string name;
                if (FormatName(sql, j, end, name) && !IsClauseKeyword(name) && EndsQuery(sql, end)) {
                    if (without_clause) {
                        *without_clause = std::string(sql.substr(0, i)) + std::string(sql.substr(end));
                    }
                    return MakeFormat(name);
                }
            }
            i = j;
        } else {
            ++i;
        }
    }
    if (without_clause) {
        *without_clause = std::string(sql);
    }
    return OutputFormat{};
}

}  // namespace clickhouse_test_runner
