set(LIB_PROJECT_DIR ${CMAKE_SOURCE_DIR}/src)

set(INCLUDE_H
        ${LIB_PROJECT_DIR}/benchmark/Benchmark.h
        ${LIB_PROJECT_DIR}/benchmark/BenchmarkCounter.h
        ${LIB_PROJECT_DIR}/operator/Operator.h
        ${LIB_PROJECT_DIR}/operator/ScanOperator.h
        ${LIB_PROJECT_DIR}/operator/BinaryJoinOperator.h
        ${LIB_PROJECT_DIR}/operator/BucketOperator.h
        ${LIB_PROJECT_DIR}/operator/CounterOperator.h
        ${LIB_PROJECT_DIR}/operator/TernaryJoinOperator.h
        ${LIB_PROJECT_DIR}/operator/PrintOperator.h
        ${LIB_PROJECT_DIR}/joinstrategy/binary/JoinStrategy.h
        ${LIB_PROJECT_DIR}/joinstrategy/binary/GraceHashJoin.h
        ${LIB_PROJECT_DIR}/joinstrategy/ternary/JoinStrategy.h
        ${LIB_PROJECT_DIR}/joinstrategy/ternary/GraceHashJoin.h
        ${LIB_PROJECT_DIR}/joinstrategy/Bucket.h
        ${LIB_PROJECT_DIR}/utils/Page.h
        ${LIB_PROJECT_DIR}/utils/Logger.h
        ${LIB_PROJECT_DIR}/utils/TemporaryFile.h
        ${LIB_PROJECT_DIR}/utils/TemporaryDirectory.h
        ${LIB_PROJECT_DIR}/utils/Utils.h
        ${LIB_PROJECT_DIR}/Database.h
        ${LIB_PROJECT_DIR}/Table.h
        ${LIB_PROJECT_DIR}/typedefs.h
        ${LIB_PROJECT_DIR}/constants.h
)

set(SRC_CC
        ${LIB_PROJECT_DIR}/operator/ScanOperator.cpp
        ${LIB_PROJECT_DIR}/operator/BinaryJoinOperator.cpp
        ${LIB_PROJECT_DIR}/operator/BucketOperator.cpp
        ${LIB_PROJECT_DIR}/operator/CounterOperator.cpp
        ${LIB_PROJECT_DIR}/operator/TernaryJoinOperator.cpp
        ${LIB_PROJECT_DIR}/operator/PrintOperator.cpp
        ${LIB_PROJECT_DIR}/joinstrategy/binary/GraceHashJoin.cpp
        ${LIB_PROJECT_DIR}/joinstrategy/ternary/GraceHashJoin.cpp
        ${LIB_PROJECT_DIR}/joinstrategy/Bucket.cpp
        ${LIB_PROJECT_DIR}/utils/Page.cpp
        ${LIB_PROJECT_DIR}/utils/TemporaryDirectory.cpp
        ${LIB_PROJECT_DIR}/utils/TemporaryFile.cpp
        ${LIB_PROJECT_DIR}/utils/Utils.cpp
        ${LIB_PROJECT_DIR}/Database.cpp
        ${LIB_PROJECT_DIR}/Table.cpp
)

add_library(external_join_lib STATIC ${SRC_CC} ${INCLUDE_H})

add_subdirectory(third-party/robin-hood-hashing)
add_subdirectory(third-party/mio)

target_include_directories(external_join_lib
        PUBLIC
        ${LIB_PROJECT_DIR}
        ${LIB_PROJECT_DIR}/joinstrategy
        ${LIB_PROJECT_DIR}/operator
        ${LIB_PROJECT_DIR}/utils
        ${CMAKE_CURRENT_SOURCE_DIR}/third-party/robin-hood-hashing/src/include
        ${CMAKE_CURRENT_SOURCE_DIR}/third-party/mio/single_include
        ${Boost_LIBRARIES}
)

set(BOOST_ROOT   "$ENV{HOME}/local")
set(BOOST_INCLUDEDIR "${BOOST_ROOT}/include")
set(BOOST_LIBRARYDIR "${BOOST_ROOT}/lib")

find_package(Boost REQUIRED COMPONENTS filesystem system)

if(NOT Boost_FOUND)
    message(FATAL_ERROR "Could not find Boost")
endif()

target_link_libraries(external_join_lib PRIVATE ${Boost_LIBRARIES})


# ---------------------------------------------------------------------------
# Add Executable file for testing
# ---------------------------------------------------------------------------

add_executable(test_external_join_lib ${LIB_PROJECT_DIR}/main.cpp)
target_link_libraries(test_external_join_lib PRIVATE external_join_lib)
target_include_directories(test_external_join_lib PUBLIC ${LIB_PROJECT_DIR}/include)

