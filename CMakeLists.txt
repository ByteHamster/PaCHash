cmake_minimum_required(VERSION 3.10)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(VariableSizeObjectStore)
set(PROJECT_VENDOR "Hans-Peter Lehmann")
set(PROJECT_URL "https://github.com/ByteHamster/VariableSizeObjectStore")
set(PROJECT_DESCRIPTION "Succinct variable-size static object store")

add_library(VariableSizeObjectStore INTERFACE)
target_include_directories(VariableSizeObjectStore INTERFACE include)

add_subdirectory(extern/sdsl-lite)
target_link_libraries(VariableSizeObjectStore INTERFACE sdsl)

set(TLX_INSTALL_INCLUDE_DIR tlx)
add_subdirectory(extern/ips2ra)
target_include_directories(VariableSizeObjectStore INTERFACE extern/ips2ra/include)
target_include_directories(VariableSizeObjectStore INTERFACE extern/ips2ra/extern/tlx/tlx)
target_link_libraries(VariableSizeObjectStore INTERFACE ips2ra tlx)

find_package(LibAIO)
if(LibAIO_FOUND)
    target_link_libraries(VariableSizeObjectStore INTERFACE ${LIBAIO_LIBRARIES})
    target_compile_definitions(VariableSizeObjectStore INTERFACE HAS_LIBAIO)
else()
    message(WARNING "Warning: LibAIO not found. Continuing without it.")
endif()

find_package(LibUring)
if(LibUring_FOUND)
    target_link_libraries(VariableSizeObjectStore INTERFACE ${LIBURING_LIBRARIES})
    target_compile_definitions(VariableSizeObjectStore INTERFACE HAS_LIBURING)
else()
    message(WARNING "Warning: LibUring not found. Continuing without it.")
endif()

add_executable(Benchmark benchmark/main.cpp)
target_compile_features(Benchmark PRIVATE cxx_std_17)
target_link_libraries(Benchmark PRIVATE VariableSizeObjectStore)
