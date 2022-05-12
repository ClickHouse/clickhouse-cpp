#pragma once

#include "utils_meta.h"

#include <vector>
#include <chrono>
#include <string>
#include <utility>

template <typename ChronoDurationType>
struct Timer {
    using DurationType = ChronoDurationType;

    Timer()
        : started_at(Now())
    {}

    void Restart() {
        started_at = Now();
    }

    void Start() {
        Restart();
    }

    auto Elapsed() const {
        return std::chrono::duration_cast<ChronoDurationType>(Now() - started_at);
    }

private:
    static auto Now() {
        return std::chrono::high_resolution_clock::now().time_since_epoch();
    }

private:
    std::chrono::nanoseconds started_at;
};

template <typename MeasureFunc>
class MeasuresCollector {
public:
    using Result = my_result_of_t<MeasureFunc>;

    explicit MeasuresCollector(MeasureFunc && measurment_func, const size_t preallocate_results = 10)
        : measurment_func_(std::move(measurment_func))
    {
        results_.reserve(preallocate_results);
    }

    template <typename NameType>
    void Add(NameType && name) {
        results_.emplace_back(name, measurment_func_());
    }

    const auto & GetResults() const {
        return results_;
    }

private:
    MeasureFunc measurment_func_;
    std::vector<std::pair<std::string, Result>> results_;
};

template <typename MeasureFunc>
MeasuresCollector<MeasureFunc> collect(MeasureFunc && f) {
    return MeasuresCollector<MeasureFunc>(std::forward<MeasureFunc>(f));
}
