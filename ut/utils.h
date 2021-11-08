#pragma once

#include <chrono>
#include <ratio>
#include <ostream>
#include <vector>

#include <time.h>

namespace clickhouse {
    class Block;
}

template <typename ChronoDurationType>
struct Timer
{
    using DurationType = ChronoDurationType;

    Timer()
        : started_at(Now())
    {}

    void Restart()
    {
        started_at = Now();
    }

    void Start()
    {
        Restart();
    }

    auto Elapsed() const
    {
        return std::chrono::duration_cast<ChronoDurationType>(Now() - started_at);
    }

private:
    static auto Now()
    {
        struct timespec ts;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        return std::chrono::nanoseconds(ts.tv_sec * 1000000000LL + ts.tv_nsec);
    }

private:
    std::chrono::nanoseconds started_at;
};

template <typename R>
inline const char * getPrefix() {
    const char * prefix = "?";
    if constexpr (std::ratio_equal_v<R, std::nano>) {
        prefix = "n";
    } else if constexpr (std::ratio_equal_v<R, std::micro>) {
        prefix = "u";
    } else if constexpr (std::ratio_equal_v<R, std::milli>) {
        prefix = "m";
    } else if constexpr (std::ratio_equal_v<R, std::centi>) {
        prefix = "c";
    } else if constexpr (std::ratio_equal_v<R, std::deci>) {
        prefix = "d";
    } else {
        static_assert("Unsupported ratio");
    }

    return prefix;
}

namespace std {
template <typename R, typename P>
inline ostream & operator<<(ostream & ostr, const chrono::duration<R, P> & d) {
    return ostr << d.count() << ::getPrefix<P>() << "s";
}
}

template <typename MeasureFunc>
class MeasuresCollector {
public:
    using Result = std::result_of_t<MeasureFunc()>;
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
MeasuresCollector<MeasureFunc> collect(MeasureFunc && f)
{
    return MeasuresCollector<MeasureFunc>(std::forward<MeasureFunc>(f));
}

std::ostream& operator<<(std::ostream & ostr, const clickhouse::Block & block);
