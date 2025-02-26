find_path(absl_INCLUDE_DIR
  NAMES absl/base/config.h
  DOC "absl include directory"
  REQUIRED
)
mark_as_advanced(absl_INCLUDE_DIR)
find_library(absl_LIBRARY
  NAMES absl_int128 libabsl_int128
  DOC "absl library"
  REQUIRED
)
mark_as_advanced(absl_LIBRARY)

# Unlike lz4, absl's version information does not seem to be available.

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(absl
  REQUIRED_VARS absl_LIBRARY absl_INCLUDE_DIR)

if (absl_FOUND)
  set(absl_INCLUDE_DIRS "${absl_INCLUDE_DIR}")
  set(absl_LIBRARIES "${absl_LIBRARY}")

  if (NOT TARGET absl::int128)
    add_library(absl::int128 UNKNOWN IMPORTED)
    set_target_properties(absl::int128 PROPERTIES
      IMPORTED_LOCATION "${absl_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${absl_INCLUDE_DIR}")
  endif ()
endif ()
