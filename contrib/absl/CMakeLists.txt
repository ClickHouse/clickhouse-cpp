ADD_LIBRARY (absl-lib STATIC
    numeric/int128.cc
)
TARGET_INCLUDE_DIRECTORIES (absl-lib
    PUBLIC ${PROJECT_SOURCE_DIR}/contrib
)
