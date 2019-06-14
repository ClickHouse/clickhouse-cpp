#pragma once

#if defined(__linux__)
#   define _linux_
#elif defined(_WIN64)
#   define _win64_
#   define _win32_
#elif defined(__WIN32__) || defined(_WIN32)
#   define _win32_
#elif defined(__APPLE__)
#   define _darwin_
#endif

#if defined(_win32_) || defined(_win64_)
#   define _win_
#endif

#if defined(_linux_) || defined (_darwin_)
#   define _unix_
#endif

#if defined(_MSC_VER)
#   undef NOMINMAX
#   define NOMINMAX
#   include <basetsd.h>
#   define ssize_t SSIZE_T
#   define HAVE_SSIZE_T 1
#endif
