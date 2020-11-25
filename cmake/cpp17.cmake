MACRO (USE_CXX17)
    IF (CMAKE_VERSION VERSION_LESS "3.1")
      SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    ELSE ()
      SET (CMAKE_CXX_STANDARD 17)
      SET (CMAKE_CXX_STANDARD_REQUIRED ON)
      # require gnu++17 over c++17 for std::is_fundamental_v<__int128>==1
      SET (CMAKE_CXX_EXTENSIONS ON)
    ENDIF ()
ENDMACRO (USE_CXX17)
