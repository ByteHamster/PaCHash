cmake_minimum_required(VERSION 3.10)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(PaCHash)
set(PROJECT_VENDOR "Hans-Peter Lehmann")
set(PROJECT_URL "https://github.com/ByteHamster/PaCHash")
set(PROJECT_DESCRIPTION "Succinct variable size static object store")

if(CMAKE_BUILD_TYPE STREQUAL "Release" AND CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_compile_options(-march=native)
endif()

add_library(PaCHash INTERFACE)
target_include_directories(PaCHash INTERFACE include)
target_compile_options(PaCHash INTERFACE -Wall -Wextra -Wpedantic)
target_compile_features(PaCHash INTERFACE cxx_std_20)

add_subdirectory(extern/sdsl-lite/external EXCLUDE_FROM_ALL)
add_subdirectory(extern/sdsl-lite/include EXCLUDE_FROM_ALL)
add_subdirectory(extern/sdsl-lite/lib EXCLUDE_FROM_ALL)
target_include_directories(PaCHash SYSTEM INTERFACE extern/sdsl-lite/include)
target_link_libraries(PaCHash INTERFACE sdsl)

set(TLX_INSTALL_INCLUDE_DIR tlx CACHE PATH "Workaround for TLX breaking the first cmake call")
set(IPS2RA_DISABLE_PARALLEL ON CACHE PATH "FindTBB greps a file that does not exist in recent TBB versions")
add_subdirectory(extern/ips2ra EXCLUDE_FROM_ALL)
target_include_directories(PaCHash SYSTEM INTERFACE extern/ips2ra/include)
target_include_directories(PaCHash SYSTEM INTERFACE extern/ips2ra/extern/tlx/tlx)
target_link_libraries(PaCHash INTERFACE ips2ra tlx)

add_subdirectory(extern/pasta-bit-vector EXCLUDE_FROM_ALL)
target_link_libraries(PaCHash INTERFACE pasta_bit_vector)
target_include_directories(PaCHash SYSTEM INTERFACE extern/pasta-bit-vector)

add_library(la_vector INTERFACE)
target_link_libraries(la_vector INTERFACE sdsl)
target_include_directories(la_vector SYSTEM INTERFACE extern/la_vector/include)
target_link_libraries(PaCHash INTERFACE la_vector)

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

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_executable(Benchmark benchmark/benchmark.cpp)
    target_link_libraries(Benchmark PRIVATE PaCHash)

    add_executable(BenchmarkBaseline benchmark/baseline.cpp)
    target_link_libraries(BenchmarkBaseline PRIVATE PaCHash)

    add_executable(BenchmarkMerge benchmark/merge.cpp)
    target_link_libraries(BenchmarkMerge PRIVATE PaCHash)

    add_executable(QueryInternal benchmark/queryInternal.cpp)
    target_link_libraries(QueryInternal PRIVATE PaCHash)

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
