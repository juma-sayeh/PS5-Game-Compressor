#pragma once

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
#include "miniz_tinfl.h"
