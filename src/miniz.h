#pragma once

#define MINIZ_NO_STDIO
#define MINIZ_NO_TIME
#define MINIZ_NO_ARCHIVE_APIS
#define MINIZ_NO_ARCHIVE_WRITING_APIS
#define MINIZ_NO_ZLIB_APIS

#if defined(__STRICT_ANSI__)
#define MZ_FORCEINLINE
#elif defined(_MSC_VER)
#define MZ_FORCEINLINE __forceinline
#elif defined(__GNUC__)
#define MZ_FORCEINLINE __inline__ __attribute__((__always_inline__))
#else
#define MZ_FORCEINLINE inline
#endif

#if !defined(MINIZ_LITTLE_ENDIAN)
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && \
    (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define MINIZ_LITTLE_ENDIAN 1
#else
#define MINIZ_LITTLE_ENDIAN 0
#endif
#endif

#if !defined(MINIZ_USE_UNALIGNED_LOADS_AND_STORES)
#define MINIZ_USE_UNALIGNED_LOADS_AND_STORES 0
#endif

#if !defined(MINIZ_HAS_64BIT_REGISTERS)
#if defined(_M_X64) || defined(_WIN64) || defined(__MINGW64__) || \
    defined(_LP64) || defined(__LP64__) || defined(__ia64__) || \
    defined(__x86_64__)
#define MINIZ_HAS_64BIT_REGISTERS 1
#else
#define MINIZ_HAS_64BIT_REGISTERS 0
#endif
#endif

#include "miniz_common.h"
#include "miniz_tdef.h"
#include "miniz_tinfl.h"

enum {
    MZ_DEFAULT_STRATEGY = 0,
    MZ_FILTERED = 1,
    MZ_HUFFMAN_ONLY = 2,
    MZ_RLE = 3,
    MZ_FIXED = 4
};

enum {
    MZ_NO_COMPRESSION = 0,
    MZ_BEST_SPEED = 1,
    MZ_BEST_COMPRESSION = 9,
    MZ_UBER_COMPRESSION = 10,
    MZ_DEFAULT_LEVEL = 6,
    MZ_DEFAULT_COMPRESSION = -1
};
