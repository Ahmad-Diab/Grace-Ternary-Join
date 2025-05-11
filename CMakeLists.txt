cmake_minimum_required(VERSION 3.5)
project(external_join)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wall -Wextra -Wvla)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -w")

add_definitions(-DBASE_DIR="${CMAKE_SOURCE_DIR}")
add_definitions(-DTEST_DIR="${CMAKE_SOURCE_DIR}/test/")
add_definitions(-DBENCHMARK_RESOURCES_DIR="${CMAKE_SOURCE_DIR}/benchmark/resources/")

# 16 KB
add_definitions(-DPAGE_SIZE=16384)

# 1GB (1024 MB)
add_definitions(-DMAX_MEMORY_SIZE=108760928)

# Enable Logger for debugging
#add_definitions(-DLOGGER_ON)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/vendor/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/googletest.cmake")


# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------
include_directories(
        ${CMAKE_SOURCE_DIR}/include
)

include_directories(SYSTEM
        ${GTEST_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
        ${BENCHMARK_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------
include("${CMAKE_SOURCE_DIR}/benchmark/local.cmake")
include("${CMAKE_SOURCE_DIR}/src/local.cmake")
include("${CMAKE_SOURCE_DIR}/test/local.cmake")

message("${CMAKE_SOURCE_DIR}")
find_package(Boost REQUIRED)

find_package(Boost REQUIRED COMPONENTS iostreams)


message(STATUS "[MAIN] settings")
message(STATUS "    LLVM_INCLUDE_DIRS           = ${LLVM_INCLUDE_DIRS}")
message(STATUS "    LLVM_INSTALL_PREFIX         = ${LLVM_INSTALL_PREFIX}")
message(STATUS "    GFLAGS_INCLUDE_DIR          = ${GFLAGS_INCLUDE_DIR}")
message(STATUS "    GFLAGS_LIBRARY_PATH         = ${GFLAGS_LIBRARY_PATH}")
message(STATUS "[TEST] settings")
message(STATUS "    GTEST_INCLUDE_DIR           = ${GTEST_INCLUDE_DIR}")
message(STATUS "    GTEST_LIBRARY_PATH          = ${GTEST_LIBRARY_PATH}")
