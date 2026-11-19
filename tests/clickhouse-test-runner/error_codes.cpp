#include "error_codes.h"

#include <map>
#include <utility>

namespace clickhouse_test_runner {

namespace {

struct ErrorCode {
    const char* name;
    int code;
};

// scripts/gen-error-codes.sh generates the list from src/Common/ErrorCodes.cpp
// of the upstream checkout (scripts/fetch-upstream.sh runs it).
constexpr ErrorCode kErrorCodes[] = {
#include "error_codes_generated.inc"
};

const std::map<std::string, int, std::less<>>& CodesByName() {
    static const auto* codes = [] {
        auto* m = new std::map<std::string, int, std::less<>>();
        for (const auto& e : kErrorCodes) {
            m->emplace(e.name, e.code);
        }
        return m;
    }();
    return *codes;
}

const std::map<int, std::string>& NamesByCode() {
    static const auto* names = [] {
        auto* m = new std::map<int, std::string>();
        for (const auto& e : kErrorCodes) {
            m->emplace(e.code, e.name);
        }
        return m;
    }();
    return *names;
}

}  // namespace

std::optional<int> ErrorCodeByName(std::string_view name) {
    const auto& codes = CodesByName();
    if (auto it = codes.find(name); it != codes.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::string ErrorNameByCode(int code) {
    const auto& names = NamesByCode();
    if (auto it = names.find(code); it != names.end()) {
        return it->second;
    }
    return {};
}

}  // namespace clickhouse_test_runner
