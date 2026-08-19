/* city_config.h.  Renamed from the autoconf-generated config.h; every macro
   it defines carries a CITY_ prefix so a consumer that vendors these sources
   next to its own autoconf config.h sees no name collisions. */
/* Originally generated from config.h.in by configure / autoheader. */

/* Define if building universal (internal helper macro) */
/* #undef CITY_AC_APPLE_UNIVERSAL_BUILD */

/* Define to 1 if the compiler supports __builtin_expect. */
#if WIN32 || WIN64
#	define CITY_HAVE_BUILTIN_EXPECT 0
#else
#	define CITY_HAVE_BUILTIN_EXPECT 1
#endif

/* Define to 1 if you have the <dlfcn.h> header file. */
#define CITY_HAVE_DLFCN_H 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define CITY_HAVE_INTTYPES_H 1

/* Define to 1 if you have the <memory.h> header file. */
#define CITY_HAVE_MEMORY_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define CITY_HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define CITY_HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define CITY_HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define CITY_HAVE_STRING_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define CITY_HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define CITY_HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define CITY_HAVE_UNISTD_H 1

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
#define CITY_LT_OBJDIR ".libs/"

/* Define to the address where bug reports for this package should be sent. */
#define CITY_PACKAGE_BUGREPORT "cityhash-discuss@googlegroups.com"

/* Define to the full name of this package. */
#define CITY_PACKAGE_NAME "CityHash"

/* Define to the full name and version of this package. */
#define CITY_PACKAGE_STRING "CityHash 1.0.2"

/* Define to the one symbol short name of this package. */
#define CITY_PACKAGE_TARNAME "cityhash"

/* Define to the home page for this package. */
#define CITY_PACKAGE_URL ""

/* Define to the version of this package. */
#define CITY_PACKAGE_VERSION "1.0.2"

/* Define to 1 if you have the ANSI C header files. */
#define CITY_STDC_HEADERS 1

/* Define CITY_WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined CITY_AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define CITY_WORDS_BIGENDIAN 1
# endif
#else
# ifndef CITY_WORDS_BIGENDIAN
/* #  undef CITY_WORDS_BIGENDIAN */
# endif
#endif

/* The placeholders below would define *standard* symbols (size_t, uint*_t, ...)
   on deficient platforms, not CityHash macros, so they are left unprefixed.
   All are inert (commented) in this checked-in copy. */

/* Define for Solaris 2.5.1 so the uint32_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
/* #undef _UINT32_T */

/* Define for Solaris 2.5.1 so the uint64_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
/* #undef _UINT64_T */

/* Define for Solaris 2.5.1 so the uint8_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
/* #undef _UINT8_T */

/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif

/* Define to `unsigned int' if <sys/types.h> does not define. */
/* #undef size_t */

/* Define to `int' if <sys/types.h> does not define. */
/* #undef ssize_t */

/* Define to the type of an unsigned integer type of width exactly 32 bits if
   such a type exists and the standard includes do not define it. */
/* #undef uint32_t */

/* Define to the type of an unsigned integer type of width exactly 64 bits if
   such a type exists and the standard includes do not define it. */
/* #undef uint64_t */

/* Define to the type of an unsigned integer type of width exactly 8 bits if
   such a type exists and the standard includes do not define it. */
/* #undef uint8_t */
