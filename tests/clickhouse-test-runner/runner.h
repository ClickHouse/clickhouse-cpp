#pragma once

#include <clickhouse/client.h>

#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "args.h"
#include "format.h"
#include "log.h"
#include "render.h"
#include "split.h"

namespace clickhouse_test_runner {

/// A result that does not match the test hint of the statement.
class HintError : public std::runtime_error {
public:
    HintError(const std::string& msg, int code) : std::runtime_error(msg), code_(code) {}
    int Code() const { return code_; }

private:
    int code_;
};

/// The shim cannot do what `clickhouse-client` does with a statement, or the
/// client gives something that the shim cannot use. The statement fails.
class ShimError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// Runs the statements of one shim invocation on one session.
///
/// The session is the one clickhouse::Client of the invocation, that is one
/// TCP connection of the native protocol: `SET`, `USE` and temporary tables
/// last for the whole invocation, as in `clickhouse-client`. After a server
/// exception the connection stays usable. Any other exception stops the
/// program, because the client then can be out of sync with the server.
///
/// Every statement runs with Client::Execute and a Query, as a user runs it:
/// the command-line settings go with Query::SetSetting, the query parameters
/// with Query::SetParam, and the result blocks come to Query::OnData. The
/// shim sends nothing that clickhouse-client does not send, because some
/// tests read their own queries from `system.query_log`.
class Runner {
public:
    Runner(const Options& opts, std::ostream& out, InvocationLog& log);
    ~Runner();

    /// Runs the statements in order. It stops at the first statement whose
    /// result does not match its test hint, like `clickhouse-client`, and
    /// throws the error.
    void Run(const std::vector<Statement>& statements);

    /// The seconds to wait for a packet of the server
    /// (CLICKHOUSE_TEST_RUNNER_RECEIVE_TIMEOUT, default 60; 0 = no limit).
    static int ReceiveTimeoutSeconds();

private:
    void Connect();
    void RunOne(const Statement& st);
    void Execute(const std::string& sql);
    void AfterSuccess(const std::string& sql);
    bool OtherDialect(const std::string& sql) const;
    /// What the statement has written so far: the header lines, a row.
    struct WriteState {
        bool header_written = false;
        bool row_written    = false;
    };
    void WriteBlock(const clickhouse::Block& block, const OutputFormat& format, WriteState& state);

    const Options& opts_;
    std::ostream& out_;
    InvocationLog& log_;
    std::unique_ptr<clickhouse::Client> client_;
    RenderContext render_ctx_;

    /// The command-line settings that go with each query, as
    /// clickhouse-client sends them. A `SET` of the same setting in the test
    /// removes it from here, because the session then holds the value.
    std::vector<std::pair<std::string, std::string>> settings_;
    /// The value of the last `SET dialect`.
    std::string dialect_;
    /// The output of the current statement, until the runner knows whether it
    /// echoes the statement first.
    std::string result_;
};

/// `SET a = 1, b = 'x'`: the settings and their values (string values
/// without quotes).
std::vector<std::pair<std::string, std::string>> ParseSetAssignments(const std::string& sql);

/// Writes a server exception the way `clickhouse-client` does.
std::string FormatServerError(const clickhouse::ServerException& e);

/// True when `clickhouse-client` echoes a statement that failed with this
/// server error. The client parses a statement before it echoes it, so it
/// does not echo a statement with a syntax error, but a syntax error that a
/// function finds in its argument (`formatQuery('...')`) comes after the
/// parse: then the message has `: In scope ` (analyzer) or
/// `: While processing ` after the error text.
bool EchoesAfterError(int code, const std::string& message);

}  // namespace clickhouse_test_runner
