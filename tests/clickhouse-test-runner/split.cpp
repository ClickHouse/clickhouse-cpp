#include "split.h"

#include "sql_text.h"

namespace clickhouse_test_runner {

namespace {

/// Tells if a FORMAT that follows `head` can be the FORMAT clause of an
/// INSERT: `head` has no top-level SELECT, or a top-level FROM follows its
/// last top-level SELECT.
bool FormatAfterSelectFrom(std::string_view head) {
    bool after_select = false;
    bool saw_from     = false;
    for (const auto w : TopLevelWords(head)) {
        if (EqualsIgnoreCase(w, "SELECT")) {
            after_select = true;
            saw_from     = false;
        } else if (EqualsIgnoreCase(w, "FROM")) {
            saw_from = true;
        }
    }
    return !after_select || saw_from;
}

/// Tells if the `FORMAT` keyword that ends at `format_end` starts the inline
/// data of an INSERT statement that begins at `start`. On true, `data_start`
/// is the offset after the format name. The `Values` format is excluded: its
/// data ends at `;`, like a query.
bool InsertDataStart(std::string_view text, size_t start, size_t format_end, size_t& data_start) {
    if (LeadingKeyword(text.substr(start, format_end - start)) != "INSERT") {
        return false;
    }
    size_t i      = format_end;
    auto is_blank = [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; };
    if (i >= text.size() || !is_blank(text[i])) {
        return false;
    }
    while (i < text.size() && is_blank(text[i])) {
        ++i;
    }
    size_t j = i;
    while (j < text.size() && IsWordByte(text[j])) {
        ++j;
    }
    const auto name = text.substr(i, j - i);
    if (j == i || EqualsIgnoreCase(name, "Values") || IsClauseKeyword(name)) {
        return false;
    }
    // After a SELECT, the FORMAT clause comes at the end of the SELECT, so
    // after its top-level FROM: `INSERT INTO t SELECT * FROM input('x UInt8')
    // FORMAT TSV`. Before that FROM, `format` is in the select list
    // (`SELECT format x FROM s`, `SELECT format DIV 2 FROM s`).
    if (!FormatAfterSelectFrom(text.substr(start, format_end - 6 - start))) {
        return false;
    }
    data_start = j;
    return true;
}

/// Returns the offset of the `;` that ends the statement at `start`, or the
/// length of the text.
size_t ScanStatementEnd(std::string_view text, size_t start) {
    int parens = 0;
    for (size_t i = start; i < text.size();) {
        const char c = text[i];
        if (c == '\'' || c == '"' || c == '`') {
            i = SkipQuoted(text, i);
        } else if (c == '-' && text.substr(i, 2) == "--") {
            i = SkipLineComment(text, i);
        } else if (c == '#' && (text.substr(i, 2) == "#!" || text.substr(i, 2) == "# ")) {
            i = SkipLineComment(text, i);
        } else if (c == '/' && text.substr(i, 2) == "/*") {
            i = SkipBlockComment(text, i);
        } else if (c == '$') {
            const size_t end = HeredocEnd(text, i);
            i                = end > 0 ? end : i + 1;
        } else if (c == '(' || c == '[') {
            ++parens;
            ++i;
        } else if (c == ')' || c == ']') {
            --parens;
            ++i;
        } else if (c == ';' && parens <= 0) {
            return i;
        } else if (IsWordByte(c)) {
            size_t j = i;
            while (j < text.size() && IsWordByte(text[j])) {
                ++j;
            }
            size_t data_start = 0;
            if (parens <= 0 && EqualsIgnoreCase(text.substr(i, j - i), "FORMAT") && InsertDataStart(text, start, j, data_start)) {
                // The inline data ends at an empty line.
                const size_t k = text.find("\n\n", data_start);
                return k == std::string_view::npos ? text.size() : k;
            }
            i = j;
        } else {
            ++i;
        }
    }
    return text.size();
}

}  // namespace

size_t TestTagsLength(std::string_view text) {
    size_t pos = 0;
    bool first = true;
    while (true) {
        const size_t line_start = pos;
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t')) {
            ++pos;
        }
        if (text.substr(pos, 2) != "--") {
            return line_start;
        }
        pos += 2;
        if (first) {
            while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t')) {
                ++pos;
            }
            if (text.substr(pos, 5) != "Tags:") {
                return 0;
            }
            pos += 5;
            first = false;
        }
        const size_t eol = text.find_first_of("\r\n", pos);
        if (eol == std::string_view::npos) {
            return text.size();
        }
        size_t eol_len = 1;
        if (eol + 1 < text.size() && (text[eol + 1] == '\r' || text[eol + 1] == '\n') && text[eol + 1] != text[eol]) {
            eol_len = 2;
        }
        pos = eol + eol_len;
    }
}

std::vector<Statement> SplitStatements(std::string_view text) {
    std::vector<Statement> out;
    size_t pos = TestTagsLength(text);
    bool echo  = false;
    while (true) {
        pos = SkipWhitespace(text, pos);
        if (pos >= text.size() || !HasSignificantToken(text.substr(pos))) {
            return out;
        }
        const size_t begin   = pos;
        const size_t end     = ScanStatementEnd(text, pos);
        const size_t sql_end = end;
        // Consume the semicolons after the statement.
        pos = end;
        while (pos < text.size() && text[pos] == ';') {
            ++pos;
            const size_t next = SkipWhitespaceAndComments(text, pos);
            if (next < text.size() && text[next] == ';') {
                pos = next;
            }
        }
        // Add the same-line trailing comment when the next statement starts
        // after the end of this line.
        size_t newline = text.find('\n', pos);
        if (newline == std::string_view::npos) {
            newline = text.size();
        }
        if (newline <= SkipWhitespaceAndComments(text, pos)) {
            pos = newline;
        }
        Statement st;
        // clickhouse-client sends the statement with its semicolon. Tests that
        // read `system.query_log` see it.
        if (sql_end < text.size() && text[sql_end] == ';') {
            st.sql = std::string(text.substr(begin, sql_end + 1 - begin));
        } else {
            st.sql = std::string(TrimRightSpace(text.substr(begin, sql_end - begin)));
        }
        if (sql_end == text.size()) {
            // The last statement has no semicolon: clickhouse-client echoes all
            // the rest of the text, with its trailing newlines.
            st.full = std::string(text.substr(begin));
        } else {
            st.full = std::string(TrimRightSpace(text.substr(begin, pos - begin)));
        }
        st.hint = ParseTestHint(st.full);
        if (st.hint.echo) {
            echo = *st.hint.echo;
        }
        st.echo = echo;
        out.push_back(std::move(st));
    }
}

}  // namespace clickhouse_test_runner
