cmake_minimum_required(VERSION 3.25...4.0)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(PaCHash)
set(PROJECT_VENDOR "Hans-Peter Lehmann")
set(PROJECT_URL "https://github.com/ByteHamster/PaCHash")
set(PROJECT_DESCRIPTION "Succinct variable size static object store")

if(TARGET PaCHash)
    return()
endif()

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif ()

if(CMAKE_BUILD_TYPE STREQUAL "Release" AND PROJECT_IS_TOP_LEVEL)
    add_compile_options(-march=native)
endif()

add_library(PaCHash INTERFACE)
target_include_directories(PaCHash INTERFACE include)
target_compile_features(PaCHash INTERFACE cxx_std_20)

set(TLX_INSTALL_INCLUDE_DIR tlx CACHE PATH "Workaround for TLX breaking the first cmake call")
add_subdirectory(extern/tlx SYSTEM EXCLUDE_FROM_ALL)

set(IPS2RA_DISABLE_PARALLEL ON CACHE PATH "FindTBB greps a file that does not exist in recent TBB versions")
add_subdirectory(extern/ips2ra SYSTEM EXCLUDE_FROM_ALL)
target_include_directories(PaCHash SYSTEM INTERFACE extern/ips2ra/include)
target_include_directories(PaCHash SYSTEM INTERFACE extern/ips2ra/extern/tlx/tlx)
target_link_libraries(PaCHash INTERFACE ips2ra tlx)

add_subdirectory(extern/util EXCLUDE_FROM_ALL)
target_link_libraries(PaCHash INTERFACE ByteHamsterUtil)

find_package(LibAIO)
if(LibAIO_FOUND)
    target_link_libraries(PaCHash INTERFACE ${LIBAIO_LIBRARIES})
    target_compile_definitions(PaCHash INTERFACE HAS_LIBAIO)
endif()

find_package(LibUring)
if(LibUring_FOUND)
    target_link_libraries(PaCHash INTERFACE uring)
    target_compile_definitions(PaCHash INTERFACE HAS_LIBURING)
else()
    message(WARNING "Warning: LibUring not found. Continuing without it. Note that this reduces the performance significantly.")
endif()

add_library(PaCHash::pachash ALIAS PaCHash)

if(PROJECT_IS_TOP_LEVEL)
    target_compile_options(PaCHash INTERFACE -Wall -Wextra -Wpedantic -Werror)

    add_executable(Benchmark benchmark/benchmark.cpp)
    target_link_libraries(Benchmark PRIVATE PaCHash)

    add_executable(BenchmarkBaseline benchmark/baseline.cpp)
    target_link_libraries(BenchmarkBaseline PRIVATE PaCHash)

    add_executable(BenchmarkMerge benchmark/merge.cpp)
    target_link_libraries(BenchmarkMerge PRIVATE PaCHash)

    add_executable(Example example/example.cpp)
    target_link_libraries(Example PRIVATE PaCHash)

    add_executable(Query example/query.cpp)
    target_link_libraries(Query PRIVATE PaCHash)

    add_executable(Twitter example/twitter.cpp)
    target_link_libraries(Twitter PRIVATE PaCHash)

    add_executable(Uniprot example/uniprot.cpp)
    target_link_libraries(Uniprot PRIVATE PaCHash)

    add_custom_command(OUTPUT liblz4.so COMMAND make -j4 WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/extern/lz4)
    add_custom_target(LibLz4 DEPENDS liblz4.so)

    add_executable(Wikipedia example/wikipedia.cpp)
    add_dependencies(Wikipedia LibLz4)
    target_include_directories(Wikipedia PUBLIC extern/lz4/lib)
    target_link_libraries(Wikipedia PRIVATE PaCHash ${CMAKE_CURRENT_SOURCE_DIR}/extern/lz4/lib/liblz4.so)
endif()
