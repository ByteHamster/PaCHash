cmake_minimum_required(VERSION 3.10)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(VariableSizeObjectStore)
set(PROJECT_VENDOR "Hans-Peter Lehmann")
set(PROJECT_URL "https://github.com/ByteHamster/VariableSizeObjectStore")
set(PROJECT_DESCRIPTION "Succinct variable-size static object store")

if(CMAKE_BUILD_TYPE STREQUAL "Release" AND CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_compile_options(-O3 -march=native)
endif()

add_library(VariableSizeObjectStore INTERFACE)
target_include_directories(VariableSizeObjectStore INTERFACE include)

add_subdirectory(extern/sdsl-lite/external)
add_subdirectory(extern/sdsl-lite/include)
add_subdirectory(extern/sdsl-lite/lib)
target_include_directories(VariableSizeObjectStore INTERFACE extern/sdsl-lite/include)
target_link_libraries(VariableSizeObjectStore INTERFACE sdsl)

set(TLX_INSTALL_INCLUDE_DIR tlx)
add_subdirectory(extern/ips2ra)
target_include_directories(VariableSizeObjectStore INTERFACE extern/ips2ra/include)
target_include_directories(VariableSizeObjectStore INTERFACE extern/ips2ra/extern/tlx/tlx)
target_link_libraries(VariableSizeObjectStore INTERFACE ips2ra tlx)

add_library(pasta STATIC
        extern/pasta/pasta/container/bit_vector.cpp
        extern/pasta/pasta/container/support/bit_vector_rank.cpp
        extern/pasta/pasta/container/support/bit_vector_rank_select.cpp)
target_link_libraries(pasta PUBLIC tlx)
target_include_directories(pasta PUBLIC extern/pasta/pasta)
target_compile_features(pasta PRIVATE cxx_std_20)
target_link_libraries(VariableSizeObjectStore INTERFACE pasta)

find_package(LibAIO)
if(LibAIO_FOUND)
    target_link_libraries(VariableSizeObjectStore INTERFACE ${LIBAIO_LIBRARIES})
    target_compile_definitions(VariableSizeObjectStore INTERFACE HAS_LIBAIO)
else()
    message(WARNING "Warning: LibAIO not found. Continuing without it.")
endif()

find_package(LibUring)
if(LibUring_FOUND)
    target_link_libraries(VariableSizeObjectStore INTERFACE uring)
    target_compile_definitions(VariableSizeObjectStore INTERFACE HAS_LIBURING)
else()
    message(WARNING "Warning: LibUring not found. Continuing without it.")
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    add_executable(Benchmark benchmark/benchmark.cpp)
    target_compile_features(Benchmark PRIVATE cxx_std_20)
    target_link_libraries(Benchmark PRIVATE VariableSizeObjectStore)

    add_executable(BenchmarkBaseline benchmark/baseline.cpp)
    target_compile_features(BenchmarkBaseline PRIVATE cxx_std_20)
    target_link_libraries(BenchmarkBaseline PRIVATE VariableSizeObjectStore)

    add_executable(BenchmarkMerge benchmark/merge.cpp)
    target_compile_features(BenchmarkMerge PRIVATE cxx_std_20)
    target_link_libraries(BenchmarkMerge PRIVATE VariableSizeObjectStore)

    add_executable(Example example/example.cpp)
    target_compile_features(Example PRIVATE cxx_std_20)
    target_link_libraries(Example PRIVATE VariableSizeObjectStore)

    find_package(LibXml2)
    add_executable(Wikipedia example/wikipedia.cpp)
    target_compile_features(Wikipedia PRIVATE cxx_std_20)
    target_include_directories(Wikipedia PUBLIC ${LIBXML2_INCLUDE_DIRS})
    target_include_directories(Wikipedia PUBLIC extern/gzip-hpp/include)
    target_link_libraries(Wikipedia PRIVATE VariableSizeObjectStore ${LIBXML2_LIBRARIES} z)
endif()
