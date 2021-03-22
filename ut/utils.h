#pragma once

#include <chrono>
#include <ratio>
#include <ostream>
#include <type_traits>

#include <time.h>

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

template <class R> typename std::enable_if<std::ratio_equal<R, std::nano>::value, const char*>::type getPrefix() { return "n"; }
template <class R> typename std::enable_if<std::ratio_equal<R, std::micro>::value, const char*>::type getPrefix() { return "u"; }
template <class R> typename std::enable_if<std::ratio_equal<R, std::milli>::value, const char*>::type getPrefix() { return "m"; }
template <class R> typename std::enable_if<std::ratio_equal<R, std::centi>::value, const char*>::type getPrefix() { return "c"; }
template <class R> typename std::enable_if<std::ratio_equal<R, std::deci>::value, const char*>::type getPrefix() { return "d"; }

namespace std {
template <typename R, typename P>
ostream & operator<<(ostream & ostr, const chrono::duration<R, P> & d) {
    return ostr << d.count() << ::getPrefix<P>() << "s";
}
}
