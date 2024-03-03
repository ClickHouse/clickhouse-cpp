find_path(zstd_INCLUDE_DIR
  NAMES zstd.h
  DOC "zstd include directory")
mark_as_advanced(zstd_INCLUDE_DIR)
find_library(zstd_LIBRARY
  NAMES zstd libzstd
  DOC "zstd library")
mark_as_advanced(zstd_LIBRARY)

if (zstd_INCLUDE_DIR)
  file(STRINGS "${zstd_INCLUDE_DIR}/zstd.h" _zstd_version_lines
    REGEX "#define[ \t]+ZSTD_VERSION_(MAJOR|MINOR|RELEASE)")
  string(REGEX REPLACE ".*ZSTD_VERSION_MAJOR *\([0-9]*\).*" "\\1" _zstd_version_major "${_zstd_version_lines}")
  string(REGEX REPLACE ".*ZSTD_VERSION_MINOR *\([0-9]*\).*" "\\1" _zstd_version_minor "${_zstd_version_lines}")
  string(REGEX REPLACE ".*ZSTD_VERSION_RELEASE *\([0-9]*\).*" "\\1" _zstd_version_release "${_zstd_version_lines}")
  set(zstd_VERSION "${_zstd_version_major}.${_zstd_version_minor}.${_zstd_version_release}")
  unset(_zstd_version_major)
  unset(_zstd_version_minor)
  unset(_zstd_version_release)
  unset(_zstd_version_lines)
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zstd
  REQUIRED_VARS zstd_LIBRARY zstd_INCLUDE_DIR
  VERSION_VAR zstd_VERSION)

if (zstd_FOUND)
  set(zstd_INCLUDE_DIRS "${zstd_INCLUDE_DIR}")
  set(zstd_LIBRARIES "${zstd_LIBRARY}")

  if (NOT TARGET zstd::zstd)
    add_library(zstd::zstd UNKNOWN IMPORTED)
    set_target_properties(zstd::zstd PROPERTIES
      INCLUDE_DIRECTORIES ${zstd_INCLUDE_DIRS}
      IMPORTED_LOCATION "${zstd_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${zstd_INCLUDE_DIR}")
  endif ()
endif ()
