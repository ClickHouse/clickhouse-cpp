ADD_LIBRARY (absl_int128 STATIC
    numeric/int128.cc
)

TARGET_INCLUDE_DIRECTORIES (absl_int128
    PUBLIC ${PROJECT_SOURCE_DIR}/contrib/absl
)

ADD_LIBRARY (absl::int128 ALIAS absl_int128)
