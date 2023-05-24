find_path(lz4_INCLUDE_DIR
  NAMES lz4.h
  DOC "lz4 include directory")
mark_as_advanced(lz4_INCLUDE_DIR)
find_library(lz4_LIBRARY
  NAMES lz4 liblz4
  DOC "lz4 library")
mark_as_advanced(lz4_LIBRARY)

if (lz4_INCLUDE_DIR)
  file(STRINGS "${lz4_INCLUDE_DIR}/lz4.h" _lz4_version_lines
    REGEX "#define[ \t]+LZ4_VERSION_(MAJOR|MINOR|RELEASE)")
  string(REGEX REPLACE ".*LZ4_VERSION_MAJOR *\([0-9]*\).*" "\\1" _lz4_version_major "${_lz4_version_lines}")
  string(REGEX REPLACE ".*LZ4_VERSION_MINOR *\([0-9]*\).*" "\\1" _lz4_version_minor "${_lz4_version_lines}")
  string(REGEX REPLACE ".*LZ4_VERSION_RELEASE *\([0-9]*\).*" "\\1" _lz4_version_release "${_lz4_version_lines}")
  set(lz4_VERSION "${_lz4_version_major}.${_lz4_version_minor}.${_lz4_version_release}")
  unset(_lz4_version_major)
  unset(_lz4_version_minor)
  unset(_lz4_version_release)
  unset(_lz4_version_lines)
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4
  REQUIRED_VARS lz4_LIBRARY lz4_INCLUDE_DIR
  VERSION_VAR lz4_VERSION)

if (lz4_FOUND)
  set(lz4_INCLUDE_DIRS "${lz4_INCLUDE_DIR}")
  set(lz4_LIBRARIES "${lz4_LIBRARY}")

  if (NOT TARGET lz4::lz4)
    add_library(lz4::lz4 UNKNOWN IMPORTED)
    set_target_properties(lz4::lz4 PROPERTIES
      INCLUDE_DIRECTORIES ${lz4_INCLUDE_DIRS}
      IMPORTED_LOCATION "${lz4_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${lz4_INCLUDE_DIR}")
  endif ()
endif ()
