#include "runner.h"

#include <chrono>
#include <cstdlib>

#include "error_codes.h"
#include "sql_text.h"

namespace clickhouse_test_runner {

namespace {

constexpr int kSyntaxErrorCode = 62;

/// Over the native protocol the server asks for the data of an INSERT that
/// has no inline data (`INSERT INTO t VALUES` alone, or `INSERT ... SELECT
/// ... FROM input(...)` without data): it sends the header block and waits.
/// Client::Execute never sends data, so without this check the client waits
/// for ever.
class InsertNeedsData : public ShimError {
public:
    InsertNeedsData()
        : ShimError(
              "the server asks for the data of the INSERT (it sent the header block), but Client::Execute does not send "
              "data and waits for ever; the shim stops the statement here") {}
};

/// `Code: 60. DB::Exception: Unknown table ... (UNKNOWN_TABLE)`, as
/// clickhouse-client writes it, so that a test that looks for the error name
/// in stderr can find it. The display text of the server starts with the
/// exception name (`DB::Exception: ...`).
std::string ServerExceptionText(const clickhouse::Exception& e) {
    std::string msg = e.display_text;
    if (msg.compare(0, e.name.size() + 1, e.name + ":") != 0) {
        msg = e.name + ": " + msg;
    }
    const auto name = ErrorNameByCode(e.code);
    if (!name.empty() && msg.find("(" + name + ")") == std::string::npos) {
        msg += " (" + name + ")";
    }
    return "Code: " + std::to_string(e.code) + ". " + msg;
}

}  // namespace

std::string FormatServerError(const clickhouse::ServerException& e) {
    return ServerExceptionText(e.GetException());
}

bool EchoesAfterError(int code, const std::string& message) {
    if (code != kSyntaxErrorCode) {
        return true;
    }
    return message.find(": In scope ") != std::string::npos || message.find(": While processing ") != std::string::npos;
}

int Runner::ReceiveTimeoutSeconds() {
    if (const char* v = std::getenv("CLICKHOUSE_TEST_RUNNER_RECEIVE_TIMEOUT"); v && *v) {
        try {
            return std::max(0, std::stoi(v));
        } catch (const std::exception&) {
        }
    }
    return 60;
}

Runner::Runner(const Options& opts, std::ostream& out, InvocationLog& log) : opts_(opts), out_(out), log_(log), settings_(opts.settings) {
}

Runner::~Runner() = default;

void Runner::Connect() {
    clickhouse::ClientOptions co;
    co.SetHost(opts_.host)
        .SetPort(static_cast<uint16_t>(opts_.port != 0 ? opts_.port : (opts_.secure ? 9440 : 9000)))
        .SetUser(opts_.user)
        .SetPassword(opts_.password)
        .SetDefaultDatabase(opts_.database)
        .SetConnectionRecvTimeout(std::chrono::seconds(ReceiveTimeoutSeconds()));
    if (opts_.secure) {
        co.SetSSLOptions(clickhouse::ClientOptions::SSLOptions{});
    }
    log_.Add("address", opts_.host + ":" + std::to_string(co.port));
    client_          = std::make_unique<clickhouse::Client>(co);
    const auto& info = client_->GetServerInfo();
    if (!info.timezone.empty()) {
        render_ctx_.default_time_zone = info.timezone;
    }
    log_.Add("server", info.display_name + " " + std::to_string(info.version_major) + "." + std::to_string(info.version_minor) + "." +
                           std::to_string(info.version_patch) + ", time zone " + info.timezone);
}

void Runner::Run(const std::vector<Statement>& statements) {
    Connect();
    for (const auto& st : statements) {
        RunOne(st);
    }
}

void Runner::RunOne(const Statement& st) {
    auto echo = [&] {
        if (st.echo) {
            out_ << st.full << '\n';
        }
    };
    if (!st.hint.parse_error.empty()) {
        echo();
        throw ShimError(st.hint.parse_error);
    }

    log_.Add("query", st.sql);
    result_.clear();
    std::optional<clickhouse::ServerException> exc;
    try {
        Execute(st.sql);
    } catch (const clickhouse::ServerException& e) {
        exc = e;
    } catch (...) {
        // Not a server error: the client can be out of sync with the server,
        // and the statement failed. Write what came, then stop.
        echo();
        out_ << result_;
        out_.flush();
        throw;
    }

    // clickhouse-client parses a statement before it echoes it, so it does
    // not echo a statement with a syntax error (EchoesAfterError).
    if (!exc || EchoesAfterError(exc->GetCode(), exc->GetException().display_text)) {
        echo();
    }
    out_ << result_;
    out_.flush();
    if (!out_) {
        throw ShimError("cannot write stdout");
    }

    if (st.hint.ExpectsError() && !exc) {
        throw HintError("expected " + st.hint.Label() + ", but the query succeeded (query: " + st.full + ")", 0);
    }
    if (st.hint.ExpectsError() && exc) {
        if (!st.hint.Matches(exc->GetCode())) {
            throw HintError("expected " + st.hint.Label() + ", but got " + FormatServerError(*exc) + " (query: " + st.full + ")",
                            exc->GetCode());
        }
        log_.Add("expected_error_matched", st.hint.Label());
        return;
    }
    if (exc) {
        throw *exc;
    }
    AfterSuccess(st.sql);
}

bool Runner::OtherDialect(const std::string& sql) const {
    return !dialect_.empty() && !EqualsIgnoreCase(dialect_, "clickhouse") && LeadingKeyword(sql) != "SET";
}

void Runner::Execute(const std::string& sql) {
    OutputFormat format;
    if (!OtherDialect(sql)) {
        // The FORMAT clause is SQL; other dialects can use the word
        // differently (the `format` pipe of LogsQL).
        format = FindOutputFormat(sql);
    }
    if (!format.supported) {
        throw ShimError("output format " + format.name +
                        " is not supported: clickhouse-cpp reads results as Native blocks, and the shim writes them only as "
                        "TabSeparated, LineAsString, Values or Null");
    }
    if (format.discard && LeadingKeyword(sql) == "EXPLAIN") {
        // With ignore_format_null_for_explain (on by default), FORMAT Null
        // does not apply to EXPLAIN: the plan is written.
        format = OutputFormat{};
    }

    // The statement goes as it is, also its FORMAT clause: over the native
    // protocol the server ignores the clause and sends Native blocks; tests
    // that read system.query_log see the text.
    clickhouse::Query query(sql);
    for (const auto& [name, value] : settings_) {
        query.SetSetting(name, clickhouse::QuerySettingsField{value, clickhouse::QuerySettingsField::IMPORTANT});
    }
    for (const auto& [name, value] : opts_.params) {
        query.SetParam(name, value);
    }
    const bool is_insert = LeadingKeyword(sql) == "INSERT";
    WriteState state;
    query.OnData([&](const clickhouse::Block& block) {
        if (block.GetColumnCount() == 0) {
            return;
        }
        if (is_insert) {
            throw InsertNeedsData();
        }
        WriteBlock(block, format, state);
    });
    client_->Execute(query);
}

void Runner::WriteBlock(const clickhouse::Block& block, const OutputFormat& format, WriteState& state) {
    const size_t columns = block.GetColumnCount();
    if (!state.header_written) {
        state.header_written = true;
        auto header          = [&](auto get) {
            for (size_t i = 0; i < columns; ++i) {
                if (i > 0) {
                    result_ += '\t';
                }
                WriteEscapedString(result_, get(i), false);
            }
            result_ += '\n';
        };
        if (format.names) {
            header([&](size_t i) { return block.GetColumnName(i); });
        }
        if (format.types) {
            header([&](size_t i) { return block[i]->Type()->GetName(); });
        }
    }
    std::vector<std::unique_ptr<ValueWriter>> writers;
    writers.reserve(columns);
    for (size_t i = 0; i < columns; ++i) {
        try {
            writers.push_back(MakeValueWriter(block[i], render_ctx_));
        } catch (const UnsupportedType& e) {
            throw UnsupportedType("column '" + block.GetColumnName(i) + "': " + e.what());
        }
    }
    if (format.discard) {
        // `FORMAT Null`: the values are read, but not written.
        return;
    }
    // `Values`: each row as a tuple, its values as inside an array; the rows
    // of all blocks separated by commas, and no newline.
    const TextForm form  = format.values ? TextForm::Nested : format.raw ? TextForm::TopLevelRaw : TextForm::TopLevel;
    const char separator = format.values ? ',' : '\t';
    const size_t rows    = block.GetRowCount();
    for (size_t row = 0; row < rows; ++row) {
        if (format.values) {
            result_ += state.row_written ? ",(" : "(";
        }
        for (size_t i = 0; i < columns; ++i) {
            if (i > 0) {
                result_ += separator;
            }
            try {
                writers[i]->Write(row, form, result_);
            } catch (const UnsupportedType& e) {
                throw UnsupportedType("column '" + block.GetColumnName(i) + "': " + e.what());
            }
        }
        result_ += format.values ? ')' : '\n';
        state.row_written = true;
    }
}

void Runner::AfterSuccess(const std::string& sql) {
    if (LeadingKeyword(sql) != "SET") {
        return;
    }
    for (const auto& [name, value] : ParseSetAssignments(sql)) {
        for (auto it = settings_.begin(); it != settings_.end();) {
            it = it->first == name ? settings_.erase(it) : it + 1;
        }
        if (name == "dialect") {
            dialect_ = value;
        }
    }
}

std::vector<std::pair<std::string, std::string>> ParseSetAssignments(const std::string& sql) {
    std::vector<std::pair<std::string, std::string>> out;
    const size_t i = SkipWhitespaceAndComments(sql, 0) + 3;
    if (i > sql.size()) {
        return out;
    }
    std::string_view body = TrimSpace(std::string_view(sql).substr(i));
    if (!body.empty() && body.back() == ';') {
        body.remove_suffix(1);
    }
    auto add = [&](std::string_view part) {
        const size_t eq = part.find('=');
        if (eq == std::string_view::npos) {
            return;
        }
        std::string_view value = TrimSpace(part.substr(eq + 1));
        if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
            value = value.substr(1, value.size() - 2);
        }
        out.emplace_back(std::string(TrimSpace(part.substr(0, eq))), std::string(value));
    };
    int depth    = 0;
    size_t start = 0;
    for (size_t j = 0; j < body.size(); ++j) {
        const char c = body[j];
        if (c == '\'' || c == '"' || c == '`') {
            j = SkipQuoted(body, j) - 1;
        } else if (c == '(' || c == '[') {
            ++depth;
        } else if (c == ')' || c == ']') {
            --depth;
        } else if (c == ',' && depth == 0) {
            add(body.substr(start, j - start));
            start = j + 1;
        }
    }
    add(body.substr(start));
    return out;
}

}  // namespace clickhouse_test_runner
