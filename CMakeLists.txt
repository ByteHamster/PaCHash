cmake_minimum_required(VERSION 3.10)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(PactHash)
set(PROJECT_VENDOR "Hans-Peter Lehmann")
set(PROJECT_URL "https://github.com/ByteHamster/PactHash")
set(PROJECT_DESCRIPTION "Succinct variable size static object store")

if(CMAKE_BUILD_TYPE STREQUAL "Release" AND CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_compile_options(-march=native)
endif()

add_library(PactHash INTERFACE)
target_include_directories(PactHash INTERFACE include)
target_compile_options(PactHash INTERFACE -Wall -Wextra -Wpedantic -Wcast-align=strict)

add_subdirectory(extern/sdsl-lite/external EXCLUDE_FROM_ALL)
add_subdirectory(extern/sdsl-lite/include EXCLUDE_FROM_ALL)
add_subdirectory(extern/sdsl-lite/lib EXCLUDE_FROM_ALL)
target_include_directories(PactHash SYSTEM INTERFACE extern/sdsl-lite/include)
target_link_libraries(PactHash INTERFACE sdsl)

set(TLX_INSTALL_INCLUDE_DIR tlx)
add_subdirectory(extern/ips2ra EXCLUDE_FROM_ALL)
target_include_directories(PactHash SYSTEM INTERFACE extern/ips2ra/include)
target_include_directories(PactHash SYSTEM INTERFACE extern/ips2ra/extern/tlx/tlx)
target_link_libraries(PactHash INTERFACE ips2ra tlx)

add_subdirectory(extern/pasta-bit-vector EXCLUDE_FROM_ALL)
target_link_libraries(PactHash INTERFACE pasta_bit_vector)

find_package(LibAIO)
if(LibAIO_FOUND)
    target_link_libraries(PactHash INTERFACE ${LIBAIO_LIBRARIES})
    target_compile_definitions(PactHash INTERFACE HAS_LIBAIO)
else()
    message(WARNING "Warning: LibAIO not found. Continuing without it.")
endif()

find_package(LibUring)
if(LibUring_FOUND)
    target_link_libraries(PactHash INTERFACE uring)
    target_compile_definitions(PactHash INTERFACE HAS_LIBURING)
else()
    message(WARNING "Warning: LibUring not found. Continuing without it.")
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_executable(Benchmark benchmark/benchmark.cpp)
    target_compile_features(Benchmark PRIVATE cxx_std_20)
    target_link_libraries(Benchmark PRIVATE PactHash)

    add_executable(BenchmarkBaseline benchmark/baseline.cpp)
    target_compile_features(BenchmarkBaseline PRIVATE cxx_std_20)
    target_link_libraries(BenchmarkBaseline PRIVATE PactHash)

    add_executable(BenchmarkMerge benchmark/merge.cpp)
    target_compile_features(BenchmarkMerge PRIVATE cxx_std_20)
    target_link_libraries(BenchmarkMerge PRIVATE PactHash)

    add_executable(Example example/example.cpp)
    target_compile_features(Example PRIVATE cxx_std_20)
    target_link_libraries(Example PRIVATE PactHash)

    add_executable(Query example/query.cpp)
    target_compile_features(Query PRIVATE cxx_std_20)
    target_link_libraries(Query PRIVATE PactHash)

    add_executable(Twitter example/twitter.cpp)
    target_compile_features(Twitter PRIVATE cxx_std_20)
    target_link_libraries(Twitter PRIVATE PactHash)

    add_executable(Uniprot example/uniprot.cpp)
    target_compile_features(Uniprot PRIVATE cxx_std_20)
    target_link_libraries(Uniprot PRIVATE PactHash)

    add_custom_command(OUTPUT liblz4.so COMMAND make -j4 WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/extern/lz4)
    add_custom_target(LibLz4 DEPENDS liblz4.so)

    add_executable(Wikipedia example/wikipedia.cpp)
    add_dependencies(Wikipedia LibLz4)
    target_compile_features(Wikipedia PRIVATE cxx_std_20)
    target_include_directories(Wikipedia PUBLIC extern/lz4/lib)
    target_link_libraries(Wikipedia PRIVATE PactHash ${CMAKE_CURRENT_SOURCE_DIR}/extern/lz4/lib/liblz4.so)
endif()
