cmake_minimum_required(VERSION 3.20)
project(ZeroKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ---------- 依赖 ----------
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED IMPORTED_TARGET liblz4)

# ---------- 编译选项 ----------
add_compile_options(-Wall -Wextra)

set(ZEROKV_SANITIZE "" CACHE STRING "Comma-separated sanitizers: address,undefined,thread")
if(ZEROKV_SANITIZE)
  message(STATUS "ZeroKV: enabling sanitizers: ${ZEROKV_SANITIZE}")
  add_compile_options(-fsanitize=${ZEROKV_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${ZEROKV_SANITIZE})
endif()


set(ZEROKV_BENCH_SOURCE "bench.cpp" CACHE STRING "Benchmark source file name")

add_executable(zerokv_bench ${ZEROKV_BENCH_SOURCE})
target_include_directories(zerokv_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(zerokv_bench PRIVATE
  Threads::Threads
  ZLIB::ZLIB
  PkgConfig::LZ4)

# 组件级测试（component_tests.cpp 在仓库根目录）
add_executable(zerokv_component_tests component_tests.cpp)
target_include_directories(zerokv_component_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(zerokv_component_tests PRIVATE
  Threads::Threads
  ZLIB::ZLIB
  PkgConfig::LZ4)

# ---------- 单元测试 ----------
option(ZEROKV_BUILD_TESTS "Build unit tests" ON)
if(ZEROKV_BUILD_TESTS)
  enable_testing()
  add_executable(zerokv_tests unit_tests.cpp)
  target_include_directories(zerokv_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(zerokv_tests PRIVATE
    Threads::Threads
    ZLIB::ZLIB
    PkgConfig::LZ4)
  add_test(NAME zerokv_tests COMMAND zerokv_tests)
  add_test(NAME zerokv_component_tests COMMAND zerokv_component_tests)
endif()