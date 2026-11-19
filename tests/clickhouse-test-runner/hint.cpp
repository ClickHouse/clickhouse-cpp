#include "hint.h"

#include <charconv>

#include "error_codes.h"
#include "sql_text.h"

namespace clickhouse_test_runner {

namespace {

std::vector<std::string_view> TokenizeHint(std::string_view body) {
    std::vector<std::string_view> out;
    for (size_t i = 0; i < body.size();) {
        const char c = body[i];
        if (IsSpace(c)) {
            ++i;
        } else if (c == ',') {
            out.push_back(",");
            ++i;
        } else {
            size_t j = i;
            while (j < body.size() && !IsSpace(body[j]) && body[j] != ',') {
                ++j;
            }
            out.push_back(body.substr(i, j - i));
            i = j;
        }
    }
    return out;
}

bool ParseInt(std::string_view s, int& out) {
    const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && ptr == s.data() + s.size();
}

void ParseHintBody(TestHint& h, std::string_view body, bool leading) {
    const auto tokens = TokenizeHint(body);
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto tok = tokens[i];
        if (tok == "echo" || tok == "echoOn") {
            h.echo = true;
        } else if (tok == "echoOff") {
            h.echo = false;
        } else if (tok == "retry") {
            // `retry N until|while serverError X` is used by a few tests that
            // wait for a background process. It is not supported.
            h.parse_error = "the \"retry\" test hint is not supported";
            return;
        } else if (tok == "serverError" || tok == "clientError" || tok == "error") {
            if (leading) {
                continue;
            }
            std::vector<int> codes;
            for (size_t k = i + 1; k < tokens.size(); ++k) {
                const auto t = tokens[k];
                if (t == ",") {
                    continue;
                }
                int n = 0;
                if (ParseInt(t, n)) {
                    codes.push_back(n);
                    continue;
                }
                const auto code = ErrorCodeByName(t);
                if (!code) {
                    h.parse_error = "unknown error name \"" + std::string(t) + "\" in test hint";
                    return;
                }
                codes.push_back(*code);
            }
            if (tok == "serverError") {
                h.server_errors = codes;
            } else if (tok == "clientError") {
                h.client_errors = codes;
            } else {
                h.server_errors = codes;
                h.client_errors = codes;
            }
            return;
        }
    }
}

std::string CodeList(const std::vector<int>& codes) {
    std::string out;
    for (size_t i = 0; i < codes.size(); ++i) {
        if (i > 0) {
            out += ", ";
        }
        const auto name = ErrorNameByCode(codes[i]);
        out += name.empty() ? std::to_string(codes[i]) : name;
    }
    return out;
}

}  // namespace

bool TestHint::Matches(int code) const {
    for (int c : server_errors) {
        if (c == code) {
            return true;
        }
    }
    for (int c : client_errors) {
        if (c == code) {
            return true;
        }
    }
    return false;
}

std::string TestHint::Label() const {
    if (!server_errors.empty() && !client_errors.empty()) {
        return "error " + CodeList(server_errors);
    }
    if (!server_errors.empty()) {
        return "serverError " + CodeList(server_errors);
    }
    return "clientError " + CodeList(client_errors);
}

TestHint ParseTestHint(std::string_view full) {
    TestHint h;
    bool leading = true;
    for (size_t i = 0; i < full.size();) {
        size_t end   = 0;
        const char c = full[i];
        if (IsSpace(c)) {
            ++i;
            continue;
        } else if (LineCommentAt(full, i)) {
            end = SkipLineComment(full, i);
        } else if (full.substr(i, 2) == "/*") {
            end = SkipBlockComment(full, i);
        } else if (c == '\'' || c == '"' || c == '`') {
            leading = false;
            i       = SkipQuoted(full, i);
            continue;
        } else if (c == '$') {
            leading        = false;
            const size_t e = HeredocEnd(full, i);
            i              = e > 0 ? e : i + 1;
            continue;
        } else {
            leading = false;
            ++i;
            continue;
        }
        const auto comment = full.substr(i, end - i);
        if (const size_t open = comment.find('{'); open != std::string_view::npos) {
            if (const size_t closing = comment.find('}', open); closing != std::string_view::npos) {
                ParseHintBody(h, comment.substr(open + 1, closing - open - 1), leading);
            }
        }
        i = end;
    }
    return h;
}

}  // namespace clickhouse_test_runner
