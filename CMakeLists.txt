# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2020, Xinyu Li

cmake_minimum_required(VERSION 3.3)
project(nrhi)

set(CXX_STANDARD_REQUIRED ON)
set(CXX_STANDARD 11 CACHE STRING "C++ language standard")
set(CMAKE_CXX_STANDARD ${CXX_STANDARD})

include(FindThreads)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(CheckCXXSourceCompiles)
include(CheckCXXCompilerFlag)
include(GNUInstallDirs)
include(functions.cmake)
include(ctest_helpers.cmake)

if(MSVC_VERSION)
	add_flag(-W4)
	add_flag(-bigobj) # fix C1128 raised for some test binaries
else()
	add_flag(-Wall)
endif()

add_flag(-g)
add_flag(-Wpointer-arith)
add_flag(-Wunused-macros)
add_flag(-Wsign-conversion)
add_flag(-Wsign-compare)
add_flag(-Wunreachable-code-return)
add_flag(-Wmissing-variable-declarations)
add_flag(-fno-common)

# find libpmemobj
find_path(LIBPMEMOBJ_INCLUDE_DIR libpmemobj.h)
find_path(LIBPMEMOBJ++_INCLUDE_DIR "libpmemobj++")
find_library(LIBPMEMOBJ_LIBRARY NAMES pmemobj libpmemobj)
find_library(LIBPMEM_LIBRARY NAMES pmem libpmem)
set(LIBPMEMOBJ_LIBRARIES ${LIBPMEMOBJ_LIBRARY} ${LIBPMEM_LIBRARY})
set(INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory(tests)
