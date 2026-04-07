cmake_minimum_required(VERSION 3.16)

project(AFF4 
    VERSION 1.0.0
    LANGUAGES C CXX
    DESCRIPTION "AFF4 (Advanced Forensic Format 4) library for digital forensics"
)

# ---- C++ Standard ----
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---- Build Type ----
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ---- Build Options ----
option(AFF4_DISABLE_LOGGING "Disable internal logging" ON)
option(BUILD_EXECUTABLES "Build command-line tools (aff4imager)" ON)

# ---- Find Dependencies ----
find_package(PkgConfig REQUIRED)

# Required dependencies
pkg_check_modules(RAPTOR2 REQUIRED raptor2)
pkg_check_modules(ZLIB REQUIRED zlib)
pkg_check_modules(TCLAP REQUIRED tclap)
pkg_check_modules(SPDLOG REQUIRED spdlog)
pkg_check_modules(URIPARSER REQUIRED liburiparser)
pkg_check_modules(UUID REQUIRED uuid)

find_library(SNAPPY_LIB snappy REQUIRED)
find_library(LZ4_LIB lz4 REQUIRED)
find_library(ROCKSDB_LIB rocksdb REQUIRED)
include(FetchContent)
FetchContent_Declare(
  blake3
  GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3.git
  GIT_TAG        1.3.1
)
FetchContent_Populate(blake3)
add_library(blake3 STATIC
    ${blake3_SOURCE_DIR}/c/blake3.c
    ${blake3_SOURCE_DIR}/c/blake3_dispatch.c
    ${blake3_SOURCE_DIR}/c/blake3_portable.c
)
target_compile_definitions(blake3 PRIVATE 
    BLAKE3_NO_SSE2 BLAKE3_NO_SSE41 BLAKE3_NO_AVX2 BLAKE3_NO_AVX512
)
set(BLAKE3_LIB blake3)
find_library(FMT_LIB fmt)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

# ---- Generate config.h ----
# Create aff4 subdirectory in build for generated header
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/aff4")
set(CONFIG_H "${CMAKE_BINARY_DIR}/aff4/config.h")
file(WRITE ${CONFIG_H} 
"#pragma once

/* AFF4 Configuration Header */

/* Package version */
#define PACKAGE_VERSION \"${PROJECT_VERSION}\"

/* Library version */
#define AFF4_LIB_VERSION_MAJOR ${PROJECT_VERSION_MAJOR}
#define AFF4_LIB_VERSION_MINOR ${PROJECT_VERSION_MINOR}
#define AFF4_LIB_VERSION_PATCH ${PROJECT_VERSION_PATCH}

/* Compression support */
#define HAVE_ZLIB 1
#define HAVE_LZ4 1
#define HAVE_SNAPPY 1

/* Other dependencies */
#define HAVE_TCLAP 1
#define HAVE_RAPTOR2 1
#define HAVE_LIBUUID 1
#define HAVE_LIBURIPARSER 1

/* Optional features */
#define HAVE_LIBPTHREAD 1

/* Cryptographic hashing support */
#define HAVE_OPENSSL 1
"
)

# ---- Core Library Sources ----
set(AFF4_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/compatibility.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/data_store.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/lexicon.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/rdf.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/zip.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_directory.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_file.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_map.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_symstream.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_image.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_init.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/volume_group.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/libaff4.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/tclap_parsers.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_hash.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/aff4_archive.cc
)

# ---- Create Library Target ----
add_library(aff4 STATIC ${AFF4_SOURCES})

# ---- Include Directories ----
target_include_directories(aff4 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${RAPTOR2_INCLUDE_DIRS}
        ${ZLIB_INCLUDE_DIRS}
        ${TCLAP_INCLUDE_DIRS}
        ${SPDLOG_INCLUDE_DIRS}
        ${URIPARSER_INCLUDE_DIRS}
        ${UUID_INCLUDE_DIRS}
        ${OPENSSL_INCLUDE_DIR}
        ${blake3_SOURCE_DIR}/c
)

# ---- Link Libraries ----
target_link_libraries(aff4 
    PUBLIC
        ${RAPTOR2_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${SNAPPY_LIB}
        ${LZ4_LIB}
        ${TCLAP_LIBRARIES}
        ${SPDLOG_LIBRARIES}
        ${URIPARSER_LIBRARIES}
        ${UUID_LIBRARIES}
        ${FMT_LIB}
        ${ROCKSDB_LIB}
        ${BLAKE3_LIB}
        Threads::Threads
        OpenSSL::SSL
        OpenSSL::Crypto
)

# ---- Compiler Flags ----
target_compile_options(aff4 PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>:/W4>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra -Wpedantic>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wno-unused-parameter -Wno-sign-compare>
)

# ---- Compile Definitions ----
if(AFF4_DISABLE_LOGGING)
    target_compile_definitions(aff4 PRIVATE AFF4_NO_LOGGING)
endif()

# ---- Build Executables ----
if(BUILD_EXECUTABLES)
    add_executable(aff4imager ${CMAKE_CURRENT_SOURCE_DIR}/aff4imager.cc)
    target_link_libraries(aff4imager PRIVATE aff4)

    # Installation
    install(TARGETS aff4imager DESTINATION bin)
endif()

# ---- Installation ----
# Install library
install(TARGETS aff4
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

# Install public headers
file(GLOB AFF4_HEADERS "*.h")
install(FILES ${AFF4_HEADERS} DESTINATION include/aff4)
install(FILES ${CONFIG_H} DESTINATION include/aff4)

# ---- Status Messages ----
message(STATUS "========================================")
message(STATUS "AFF4 Library Configuration")
message(STATUS "========================================")
message(STATUS "Version:              ${PROJECT_VERSION}")
message(STATUS "Build type:           ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard:         ${CMAKE_CXX_STANDARD}")
message(STATUS "Shared library:       ${BUILD_SHARED_LIBS}")
message(STATUS "Build executables:    ${BUILD_EXECUTABLES}")
message(STATUS "YAML support:         ${WITH_YAML}")
message(STATUS "Logging disabled:     ${AFF4_DISABLE_LOGGING}")
message(STATUS "========================================")
message(STATUS "Dependencies found:")
message(STATUS "  - RAPTOR2:          ${RAPTOR2_FOUND}")
message(STATUS "  - ZLIB:             ${ZLIB_FOUND}")
message(STATUS "  - SNAPPY:           ${SNAPPY_LIB}")
message(STATUS "  - LZ4:              ${LZ4_LIB}")
message(STATUS "  - ROCKSDB:          ${ROCKSDB_LIB}")
message(STATUS "  - BLAKE3:           ${BLAKE3_LIB}")
message(STATUS "  - TCLAP:            ${TCLAP_FOUND}")
message(STATUS "  - Threads:          ${CMAKE_HAVE_THREADS_LIBRARY}")
message(STATUS "  - OpenSSL:          ${OPENSSL_FOUND}")
message(STATUS "========================================")

