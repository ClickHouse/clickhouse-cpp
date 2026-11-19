#include "shim.h"

#include <clickhouse/exceptions.h>

#include <iterator>
#include <sstream>
#include <system_error>

#include "args.h"
#include "log.h"
#include "runner.h"
#include "split.h"
#include "sql_text.h"

namespace clickhouse_test_runner {

namespace {

/// Answers `clickhouse extract-from-config --key <key>`, which
/// `tests/clickhouse-test` runs to read the server config. Only `listen_host`
/// has an answer; for other keys the output is empty, as for a key that is
/// not in the config.
int ExtractFromConfig(const std::vector<std::string>& argv, std::ostream& out) {
    std::string key;
    for (size_t i = 0; i < argv.size(); ++i) {
        if (argv[i] == "--key" && i + 1 < argv.size()) {
            key = argv[++i];
        } else if (argv[i].rfind("--key=", 0) == 0) {
            key = argv[i].substr(6);
        }
    }
    if (key == "listen_host") {
        out << "127.0.0.1\n";
    }
    return out ? 0 : 1;
}

/// As in `clickhouse-client`, the exit code is the error code modulo 256 when
/// it is a server error.
int ExitCode(int code) {
    return code % 256 != 0 ? code % 256 : 1;
}

}  // namespace

int RunMain(const std::vector<std::string>& argv, std::istream& in, std::ostream& out, std::ostream& err) {
    if (!argv.empty() && argv[0] == "extract-from-config") {
        return ExtractFromConfig(std::vector<std::string>(argv.begin() + 1, argv.end()), out);
    }

    InvocationLog log;
    {
        std::string joined;
        for (const auto& a : argv) {
            joined += (joined.empty() ? "" : " ") + a;
        }
        log.Add("argv", joined);
    }

    Options opts;
    try {
        opts = ParseArgs(argv);
    } catch (const std::invalid_argument& e) {
        err << "Error: " << e.what() << "\n\n" << kUsageText;
        return 1;
    }
    if (opts.help) {
        out << kUsageText;
        return out ? 0 : 1;
    }

    std::string text;
    if (opts.query) {
        text = *opts.query;
    } else {
        text.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    if (TrimSpace(text).empty()) {
        err << "Error: no query. Use --query or write the queries to stdin.\n";
        return 1;
    }

    const auto statements = SplitStatements(text);
    log.Add("statements", std::to_string(statements.size()));
    if (statements.empty()) {
        return 0;
    }

    Runner runner(opts, out, log);
    try {
        runner.Run(statements);
        out.flush();
        return 0;
    } catch (const HintError& e) {
        log.Add("error", e.what());
        err << "Error: " << e.what() << '\n';
        return ExitCode(e.Code());
    } catch (const clickhouse::ServerException& e) {
        const auto text_of_error = FormatServerError(e);
        log.Add("error", text_of_error);
        err << "Received exception from server:\n" << text_of_error << '\n';
        return ExitCode(e.GetCode());
    } catch (const std::system_error& e) {
        // Socket errors of clickhouse-cpp: connection refused, a receive
        // timeout (CLICKHOUSE_TEST_RUNNER_RECEIVE_TIMEOUT), a closed connection.
        log.Add("error", e.what());
        err << "Error: " << e.what() << " (" << e.code().category().name() << " error " << e.code().value() << ")\n";
        return ExitCode(210);  // NETWORK_ERROR, as clickhouse-client.
    } catch (const std::exception& e) {
        log.Add("error", e.what());
        err << "Error: " << e.what() << '\n';
        return 1;
    }
}

}  // namespace clickhouse_test_runner
