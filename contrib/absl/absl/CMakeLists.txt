ADD_LIBRARY (absl_int128 STATIC
    numeric/int128.cc
)

TARGET_INCLUDE_DIRECTORIES (absl_int128
    PUBLIC ${PROJECT_SOURCE_DIR}/contrib/absl
)

ADD_LIBRARY (absl::int128 ALIAS absl_int128)

INSTALL(FILES
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/attributes.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/config.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/macros.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/optimization.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/options.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/policy_checks.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/port.h
    DESTINATION include/absl/base
)

INSTALL(FILES
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/base/internal/bits.h
    DESTINATION include/absl/base/internal
)

INSTALL(FILES
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/numeric/int128.h
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/numeric/int128_have_intrinsic.inc
    ${PROJECT_SOURCE_DIR}/contrib/absl/absl/numeric/int128_no_intrinsic.inc
  DESTINATION include/absl/numeric
)
