#include "log.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>

namespace clickhouse_test_runner {

namespace {

std::string NowUtc() {
    const auto now = std::chrono::system_clock::now();
    const auto t   = std::chrono::system_clock::to_time_t(now);
    const auto us  = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() % 1000000;
    struct tm tm_utc;
    gmtime_r(&t, &tm_utc);
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06lldZ", tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
                  tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec, static_cast<long long>(us));
    return buf;
}

}  // namespace

InvocationLog::InvocationLog() {
    if (const char* p = std::getenv("CLICKHOUSE_CLIENT_CLI_LOG")) {
        path_ = p;
    }
    Add("=== clickhouse-client invocation", NowUtc());
}

InvocationLog::~InvocationLog() {
    Flush();
}

void InvocationLog::Add(std::string_view key, std::string_view value) {
    if (path_.empty()) {
        return;
    }
    lines_ += key;
    lines_ += '=';
    for (const char c : value) {
        if (c == '\n') {
            lines_ += "\\n";
        } else {
            lines_ += c;
        }
    }
    lines_ += '\n';
}

void InvocationLog::Flush() {
    if (path_.empty() || lines_.empty()) {
        return;
    }
    if (FILE* f = std::fopen(path_.c_str(), "a")) {
        std::fwrite(lines_.data(), 1, lines_.size(), f);
        std::fclose(f);
    }
    lines_.clear();
}

}  // namespace clickhouse_test_runner
