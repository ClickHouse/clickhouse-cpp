find_path(cityhash_INCLUDE_DIR
  NAMES city.h
  DOC "cityhash include directory")
mark_as_advanced(cityhash_INCLUDE_DIR)
find_library(cityhash_LIBRARY
  NAMES cityhash libcityhash
  DOC "cityhash library")
mark_as_advanced(cityhash_LIBRARY)

# Unlike lz4, cityhash's version information does not seem to be available.

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(cityhash
  REQUIRED_VARS cityhash_LIBRARY cityhash_INCLUDE_DIR)

if (cityhash_FOUND)
  set(cityhash_INCLUDE_DIRS "${cityhash_INCLUDE_DIR}")
  set(cityhash_LIBRARIES "${cityhash_LIBRARY}")

  if (NOT TARGET cityhash::cityhash)
    add_library(cityhash::cityhash UNKNOWN IMPORTED)
    set_target_properties(cityhash::cityhash PROPERTIES
      IMPORTED_LOCATION "${cityhash_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${cityhash_INCLUDE_DIR}")
  endif ()
endif ()
