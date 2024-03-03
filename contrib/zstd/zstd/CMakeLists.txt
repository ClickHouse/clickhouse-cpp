enable_language(ASM)

SET(Sources
        common/debug.c
        common/entropy_common.c
        common/error_private.c
        common/fse_decompress.c
        common/pool.c
        common/threading.c
        common/xxhash.c
        common/zstd_common.c
        compress/fse_compress.c
        compress/hist.c
        compress/huf_compress.c
        compress/zstd_compress.c
        compress/zstd_compress_literals.c
        compress/zstd_compress_sequences.c
        compress/zstd_compress_superblock.c
        compress/zstd_double_fast.c
        compress/zstd_fast.c
        compress/zstd_lazy.c
        compress/zstd_ldm.c
        compress/zstdmt_compress.c
        compress/zstd_opt.c
        decompress/huf_decompress_amd64.S
        decompress/huf_decompress.c
        decompress/zstd_ddict.c
        decompress/zstd_decompress_block.c
        decompress/zstd_decompress.c
        dictBuilder/cover.c
        dictBuilder/divsufsort.c
        dictBuilder/fastcover.c
        dictBuilder/zdict.c
)

SET(Headers
        common/bits.h
        common/bitstream.h
        common/compiler.h
        common/cpu.h
        common/debug.h
        common/error_private.h
        common/fse.h
        common/huf.h
        common/mem.h
        common/pool.h
        common/portability_macros.h
        common/threading.h
        common/xxhash.h
        common/zstd_deps.h
        common/zstd_internal.h
        common/zstd_trace.h
        compress/clevels.h
        compress/hist.h
        compress/zstd_compress_internal.h
        compress/zstd_compress_literals.h
        compress/zstd_compress_sequences.h
        compress/zstd_compress_superblock.h
        compress/zstd_cwksp.h
        compress/zstd_double_fast.h
        compress/zstd_fast.h
        compress/zstd_lazy.h
        compress/zstd_ldm_geartab.h
        compress/zstd_ldm.h
        compress/zstdmt_compress.h
        compress/zstd_opt.h
        decompress/zstd_ddict.h
        decompress/zstd_decompress_block.h
        decompress/zstd_decompress_internal.h
        dictBuilder/cover.h
        dictBuilder/divsufsort.h
        zdict.h
        zstd_errors.h
        zstd.h
)

SET(ZSTD_LEGACY_SUPPORT true)

IF (ZSTD_LEGACY_SUPPORT)
    SET(LIBRARY_LEGACY_DIR "${LIBRARY_DIR}/legacy")
    INCLUDE_DIRECTORIES(BEFORE ${LIBRARY_LEGACY_DIR})
    ADD_DEFINITIONS(-D ZSTD_LEGACY_SUPPORT=1)

    SET(Sources ${Sources}
            legacy/zstd_v01.c
            legacy/zstd_v02.c
            legacy/zstd_v03.c
            legacy/zstd_v04.c
            legacy/zstd_v05.c
            legacy/zstd_v06.c
            legacy/zstd_v07.c
    )

    SET(Headers ${Headers}
            legacy/zstd_legacy.h
            legacy/zstd_v01.h
            legacy/zstd_v02.h
            legacy/zstd_v03.h
            legacy/zstd_v04.h
            legacy/zstd_v05.h
            legacy/zstd_v06.h
            legacy/zstd_v07.h
    )

ENDIF (ZSTD_LEGACY_SUPPORT)

ADD_LIBRARY (zstdstatic STATIC ${Sources} ${Headers})

SET_PROPERTY(TARGET zstdstatic PROPERTY POSITION_INDEPENDENT_CODE ON)

TARGET_INCLUDE_DIRECTORIES (zstdstatic
    PUBLIC ${PROJECT_SOURCE_DIR}/contrib/zstd
)

ADD_LIBRARY(zstd::zstd ALIAS zstdstatic)
