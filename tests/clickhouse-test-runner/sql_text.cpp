#include "sql_text.h"

#include <cctype>
#include <set>

namespace clickhouse_test_runner {

namespace {

bool StartsWithAt(std::string_view text, size_t i, std::string_view prefix) {
    return i <= text.size() && text.substr(i, prefix.size()) == prefix;
}

}  // namespace

bool IsWordByte(char c) {
    return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

bool IsSpace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

size_t SkipQuoted(std::string_view text, size_t i) {
    const char q = text[i];
    for (size_t j = i + 1; j < text.size(); ++j) {
        if (text[j] == '\\') {
            ++j;
        } else if (text[j] == q) {
            if (j + 1 < text.size() && text[j + 1] == q) {
                ++j;
                continue;
            }
            return j + 1;
        }
    }
    return text.size();
}

size_t SkipLineComment(std::string_view text, size_t i) {
    const size_t nl = text.find('\n', i);
    return nl == std::string_view::npos ? text.size() : nl;
}

size_t SkipBlockComment(std::string_view text, size_t i) {
    int depth = 0;
    for (size_t j = i; j + 1 < text.size();) {
        if (text[j] == '/' && text[j + 1] == '*') {
            ++depth;
            j += 2;
        } else if (text[j] == '*' && text[j + 1] == '/') {
            --depth;
            j += 2;
            if (depth == 0) {
                return j;
            }
        } else {
            ++j;
        }
    }
    return text.size();
}

size_t HeredocEnd(std::string_view text, size_t i) {
    const size_t tag_end = text.find('$', i + 1);
    if (tag_end == std::string_view::npos) {
        return 0;
    }
    for (size_t k = i + 1; k < tag_end; ++k) {
        if (!IsWordByte(text[k])) {
            return 0;
        }
    }
    const std::string_view delim = text.substr(i, tag_end + 1 - i);
    const size_t end             = text.find(delim, tag_end + 1);
    if (end == std::string_view::npos) {
        return 0;
    }
    return end + delim.size();
}

bool LineCommentAt(std::string_view text, size_t i) {
    return StartsWithAt(text, i, "--") || StartsWithAt(text, i, "#!") || StartsWithAt(text, i, "# ");
}

size_t SkipWhitespace(std::string_view text, size_t i) {
    while (i < text.size() && IsSpace(text[i])) {
        ++i;
    }
    return i;
}

size_t SkipWhitespaceAndComments(std::string_view text, size_t i) {
    while (i < text.size()) {
        if (IsSpace(text[i])) {
            ++i;
        } else if (LineCommentAt(text, i)) {
            i = SkipLineComment(text, i);
        } else if (StartsWithAt(text, i, "/*")) {
            i = SkipBlockComment(text, i);
        } else {
            return i;
        }
    }
    return i;
}

bool HasSignificantToken(std::string_view text) {
    return SkipWhitespaceAndComments(text, 0) < text.size();
}

std::string LeadingKeyword(std::string_view sql) {
    size_t i = SkipWhitespaceAndComments(sql, 0);
    while (i < sql.size() && (sql[i] == '(' || IsSpace(sql[i]))) {
        i = SkipWhitespaceAndComments(sql, i + 1);
    }
    size_t j = i;
    while (j < sql.size() && IsWordByte(sql[j])) {
        ++j;
    }
    return ToUpper(sql.substr(i, j - i));
}

std::vector<std::string_view> TopLevelWords(std::string_view sql) {
    std::vector<std::string_view> words;
    int depth = 0;
    for (size_t i = 0; i < sql.size();) {
        const char c = sql[i];
        if (c == '\'' || c == '"' || c == '`') {
            i = SkipQuoted(sql, i);
        } else if (LineCommentAt(sql, i)) {
            i = SkipLineComment(sql, i);
        } else if (StartsWithAt(sql, i, "/*")) {
            i = SkipBlockComment(sql, i);
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
            if (depth == 0) {
                words.push_back(sql.substr(i, j - i));
            }
            i = j;
        } else {
            ++i;
        }
    }
    return words;
}

bool IsClauseKeyword(std::string_view word) {
    static const std::set<std::string, std::less<>> keywords = {
        "FROM",    "WHERE", "PREWHERE", "AS",      "AND",    "OR",     "NOT",     "IN",     "IS",    "LIKE",     "ILIKE",
        "BETWEEN", "GROUP", "ORDER",    "LIMIT",   "OFFSET", "HAVING", "QUALIFY", "WINDOW", "UNION", "EXCEPT",   "INTERSECT",
        "JOIN",    "INNER", "LEFT",     "RIGHT",   "FULL",   "CROSS",  "ARRAY",   "ON",     "USING", "SETTINGS", "INTO",
        "ASC",     "DESC",  "NULLS",    "COLLATE", "WITH",   "THEN",   "ELSE",    "END",    "WHEN",  "OVER",     "GLOBAL",
        "DIV",     "MOD",   "REGEXP",   "FINAL",   "SAMPLE", "FILL",   "TIES",    "BY",
    };
    return keywords.count(ToUpper(word)) > 0;
}

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(a[i])) != std::toupper(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

std::string ToUpper(std::string_view s) {
    std::string out(s);
    for (auto& c : out) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return out;
}

std::string ToLower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
}

std::string_view TrimSpace(std::string_view s) {
    size_t b = 0;
    while (b < s.size() && IsSpace(s[b])) {
        ++b;
    }
    return TrimRightSpace(s.substr(b));
}

std::string_view TrimRightSpace(std::string_view s) {
    size_t e = s.size();
    while (e > 0 && IsSpace(s[e - 1])) {
        --e;
    }
    return s.substr(0, e);
}

bool UnquoteIdentifier(std::string_view s, std::string& out) {
    out.clear();
    if (s.size() < 2 || (s[0] != '`' && s[0] != '"') || s.back() != s[0]) {
        return false;
    }
    const char q = s[0];
    for (size_t i = 1; i + 1 < s.size(); ++i) {
        const char c = s[i];
        if (c == '\\' && i + 2 < s.size()) {
            ++i;
            out += s[i];
        } else if (c == q && i + 2 < s.size() && s[i + 1] == q) {
            ++i;
            out += q;
        } else {
            out += c;
        }
    }
    return true;
}

}  // namespace clickhouse_test_runner
