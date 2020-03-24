#pragma once

#include <chrono>
#include <time.h>

template <typename ChronoDurationType>
struct Timer
{
    using DurationType = ChronoDurationType;

    Timer() {}

    void Restart()
    {
        started_at = Current();
    }

    void Start()
    {
        Restart();
    }

    auto Elapsed() const
    {
        return std::chrono::duration_cast<ChronoDurationType>(Current() - started_at);
    }

    auto Current() const
    {
        struct timespec ts;
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        return std::chrono::nanoseconds(ts.tv_sec * 1000000000LL + ts.tv_nsec);
    }

private:
    std::chrono::nanoseconds started_at;
};

template <typename ChronoDurationType>
class PausableTimer
{
public:
    PausableTimer()
    {}

    void Start()
    {
        timer.Restart();
        paused = false;
    }

    void Pause()
    {
        total += timer.Elapsed();
        paused = true;
    }

    auto GetTotalElapsed() const
    {
        if (paused)
        {
            return total;
        }
        else
        {
            return total + timer.Elapsed();
        }
    }

    void Reset()
    {
        Pause();
        total = ChronoDurationType{0};
    }

    void Restart()
    {
        total = ChronoDurationType{0};
        Start();
    }

private:
    Timer<ChronoDurationType> timer;
    ChronoDurationType total = ChronoDurationType{0};
    bool paused = true;
};

