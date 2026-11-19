#pragma once

#include <string>
#include <string_view>
#include <vector>

/// Lexical helpers for SQL text: quotes, comments, heredocs and words, as the
/// ClickHouse lexer sees them. The statement splitter, the test-hint parser
/// and the FORMAT-clause finder use them.
namespace clickhouse_test_runner {

bool IsWordByte(char c);
bool IsSpace(char c);

/// Returns the offset after the quoted text that starts at `i` (a `'`, `"` or
/// backtick). A backslash escapes the next byte; a doubled quote is an
/// escaped quote.
size_t SkipQuoted(std::string_view text, size_t i);

/// Returns the offset of the newline that ends the line comment at `i`, or
/// the length of the text.
size_t SkipLineComment(std::string_view text, size_t i);

/// Returns the offset after the block comment at `i`. Block comments nest.
size_t SkipBlockComment(std::string_view text, size_t i);

/// Returns the end offset of the `$tag$ ... $tag$` heredoc that starts at
/// `i`, or 0 when the `$` at `i` does not start one.
size_t HeredocEnd(std::string_view text, size_t i);

/// Tells if a line comment (`--`, `#!` or `# `) starts at `i`.
bool LineCommentAt(std::string_view text, size_t i);

size_t SkipWhitespace(std::string_view text, size_t i);
size_t SkipWhitespaceAndComments(std::string_view text, size_t i);
bool HasSignificantToken(std::string_view text);

/// The first word of a statement in upper case, after comments, whitespace
/// and opening parentheses.
std::string LeadingKeyword(std::string_view sql);

/// The words of `sql` outside quotes, heredocs, comments and parentheses.
std::vector<std::string_view> TopLevelWords(std::string_view sql);

/// SQL words that can follow a column or an alias named `format`. No input
/// or output format has such a name.
bool IsClauseKeyword(std::string_view word);

bool EqualsIgnoreCase(std::string_view a, std::string_view b);
std::string ToUpper(std::string_view s);
std::string ToLower(std::string_view s);
std::string_view TrimSpace(std::string_view s);
std::string_view TrimRightSpace(std::string_view s);

/// Removes the quotes of a backtick- or double-quoted identifier. Returns
/// false when `s` is not one quoted identifier.
bool UnquoteIdentifier(std::string_view s, std::string& out);

}  // namespace clickhouse_test_runner
