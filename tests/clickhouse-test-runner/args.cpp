#include "args.h"

#include <cctype>
#include <regex>
#include <stdexcept>

namespace clickhouse_test_runner {

namespace {

struct OptionSpec {
    const char* long_name;
    const char* short_name;
    bool has_arg;
};

const OptionSpec kKnownOptions[] = {
    {"host", "h", true},
    {"port", "", true},
    {"user", "u", true},
    {"password", "", true},
    {"database", "d", true},
    {"query", "q", true},
    {"secure", "s", false},
    {"no-secure", "", false},
    // As in clickhouse-client: `-n` is `--multiquery`, `-m` is
    // `--multiline`. The shim always runs every statement of the input.
    {"multiquery", "n", false},
    {"multi-query", "", false},
    {"multiline", "m", false},
    {"help", "", false},
    // Accepted and ignored.
    {"config-file", "C", true},
    {"client_logs_file", "", true},
    {"allow_repeated_settings", "", false},
    {"fake-drop", "", false},
};

const OptionSpec* FindOption(const std::string& long_name, const std::string& short_name) {
    for (const auto& o : kKnownOptions) {
        if ((!long_name.empty() && long_name == o.long_name) || (!short_name.empty() && short_name == o.short_name)) {
            return &o;
        }
    }
    return nullptr;
}

bool IsNegativeNumber(const std::string& s) {
    static const std::regex re(R"(^-(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$)");
    return std::regex_match(s, re);
}

void Set(Options& opts, const std::string& name, const std::string& value) {
    if (name == "host") {
        opts.host = value;
    } else if (name == "port") {
        size_t used = 0;
        int p       = 0;
        try {
            p = std::stoi(value, &used);
        } catch (const std::exception&) {
            used = 0;
        }
        if (used == 0 || used != value.size() || p < 0 || p > 65535) {
            throw std::invalid_argument("bad --port value \"" + value + "\"");
        }
        opts.port = p;
    } else if (name == "user") {
        opts.user = value;
    } else if (name == "password") {
        opts.password = value;
    } else if (name == "database") {
        opts.database = value;
    } else if (name == "query") {
        opts.query = value;
    } else if (name == "secure") {
        opts.secure = true;
    } else if (name == "no-secure") {
        opts.secure = false;
    } else if (name == "help") {
        opts.help = true;
    }
}

void AddSetting(Options& opts, const std::string& name, const std::string& value) {
    for (auto& [n, v] : opts.settings) {
        if (n == name) {
            v = value;
            return;
        }
    }
    opts.settings.emplace_back(name, value);
}

}  // namespace

bool IsServerSetting(const std::string& name) {
    return name == "log_comment" || name == "max_insert_threads" || name == "max_threads" || name == "send_logs_level" ||
           name == "async_insert" || name == "enable_parallel_replicas";
}

Options ParseArgs(const std::vector<std::string>& argv) {
    Options opts;
    for (size_t i = 0; i < argv.size(); ++i) {
        const std::string& arg = argv[i];
        if (arg == "--") {
            return opts;
        }
        if (arg.rfind("--", 0) == 0) {
            std::string name = arg.substr(2);
            std::string value;
            bool has_value = false;
            if (const auto eq = name.find('='); eq != std::string::npos) {
                value     = name.substr(eq + 1);
                name      = name.substr(0, eq);
                has_value = true;
            }
            if (name.empty()) {
                continue;
            }
            if (const auto* o = FindOption(name, "")) {
                if (!o->has_arg) {
                    Set(opts, o->long_name, "");
                    continue;
                }
                if (!has_value) {
                    if (i + 1 >= argv.size()) {
                        throw std::invalid_argument("option --" + name + " needs a value");
                    }
                    value = argv[++i];
                }
                Set(opts, o->long_name, value);
                continue;
            }
            // Any other long option is a setting (`--max_threads=1`) or a
            // query parameter (`--param_x=1`). The value is optional.
            if (!has_value && i + 1 < argv.size()) {
                const std::string& next = argv[i + 1];
                if (next.rfind("-", 0) != 0 || IsNegativeNumber(next)) {
                    value     = next;
                    has_value = true;
                    ++i;
                }
            }
            if (!has_value) {
                value = "1";
            }
            if (name.rfind("param_", 0) == 0) {
                opts.params[name.substr(6)] = value;
                continue;
            }
            std::string setting = name;
            for (auto& c : setting) {
                if (c == '-') {
                    c = '_';
                }
            }
            if (IsServerSetting(setting)) {
                AddSetting(opts, setting, value);
            }
        } else if (arg.size() > 1 && arg[0] == '-') {
            const auto* o = FindOption("", arg.substr(1));
            if (!o) {
                // Unknown short options are ignored, as the other language
                // ports do.
                continue;
            }
            if (!o->has_arg) {
                Set(opts, o->long_name, "");
                continue;
            }
            if (i + 1 >= argv.size()) {
                throw std::invalid_argument("option " + arg + " needs a value");
            }
            Set(opts, o->long_name, argv[++i]);
        }
        // Positional arguments are ignored.
    }
    return opts;
}

std::vector<std::string> RedactPassword(const std::vector<std::string>& argv) {
    // A setting (`--max_threads 1`) takes the next argument as its value only
    // when it does not start with `-` or is a negative number. Such a value is
    // never an option, so the walk can read it as an argument of its own.
    std::vector<std::string> result;
    result.reserve(argv.size());
    for (size_t i = 0; i < argv.size(); ++i) {
        const std::string& arg = argv[i];
        const OptionSpec* o    = nullptr;
        if (arg.rfind("--", 0) == 0) {
            const auto eq = arg.find('=');
            if (eq != std::string::npos) {
                result.push_back(arg.compare(2, eq - 2, "password") == 0 ? "--password=***" : arg);
                continue;
            }
            o = FindOption(arg.substr(2), "");
        } else if (arg.size() > 1 && arg[0] == '-') {
            o = FindOption("", arg.substr(1));
        }
        result.push_back(arg);
        if (o && o->has_arg && i + 1 < argv.size()) {
            ++i;
            result.push_back(std::string(o->long_name) == "password" ? "***" : argv[i]);
        }
    }
    return result;
}

const char kUsageText[] = R"(usage: clickhouse-client [options]

A clickhouse-client imitation that runs queries through clickhouse-cpp, so
that the ClickHouse tests/clickhouse-test runner can drive it. If --query is
not given, the queries are read from stdin. Every ";"-separated statement runs.

Options:
  -h, --host HOST          Server host (default: localhost)
      --port PORT          Native protocol port (default: 9000)
  -u, --user USER          User name (default: default)
      --password PASSWORD  Password (default: empty)
  -d, --database DB        Database (default: default)
  -q, --query SQL          Queries to run
  -s, --secure             Use TLS (needs a build with WITH_OPENSSL=ON)
      --param_NAME=VALUE   Query parameter
      --SETTING=VALUE      log_comment, max_insert_threads, max_threads,
                           send_logs_level, async_insert and
                           enable_parallel_replicas go to the server; other
                           settings are ignored
  -n, --multiquery         Ignored
  -m, --multiline          Ignored
      --help               Show this help

Environment variables:
  CLICKHOUSE_CLIENT_CLI_LOG               Log file for troubleshooting
  CLICKHOUSE_TEST_RUNNER_RECEIVE_TIMEOUT  Seconds to wait for a server packet
                                          (default: 60; 0: no limit)
)";

}  // namespace clickhouse_test_runner
