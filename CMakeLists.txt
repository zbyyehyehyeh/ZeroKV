cmake_minimum_required(VERSION 3.20)
project(ZeroKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ---------- 寻找依赖 ----------
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LZ4 REQUIRED IMPORTED_TARGET liblz4)

# ---------- 全局编译选项 ----------
add_compile_options(-Wall -Wextra)

# Sanitizer 支持
set(ZEROKV_SANITIZE "" CACHE STRING "Comma-separated sanitizers: address,undefined,thread")
if(ZEROKV_SANITIZE)
  message(STATUS "ZeroKV: enabling sanitizers: ${ZEROKV_SANITIZE}")
  add_compile_options(-fsanitize=${ZEROKV_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${ZEROKV_SANITIZE})
endif()

# ---------- 核心库 (仅定义一次) ----------
file(GLOB_RECURSE ZEROKV_SOURCES 
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp"
)

add_library(zerokv STATIC ${ZEROKV_SOURCES})
set_target_properties(zerokv PROPERTIES LINKER_LANGUAGE CXX)

# 【稳健的递归头文件路径收集】
file(GLOB_RECURSE ALL_SUBDIRS LIST_DIRECTORIES true 
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

set(ZEROKV_INCLUDE_DIRS "")
foreach(entry ${ALL_SUBDIRS})
  if(IS_DIRECTORY ${entry})
    list(APPEND ZEROKV_INCLUDE_DIRS ${entry})
  endif()
endforeach()

target_include_directories(zerokv
  PUBLIC
    ${ZEROKV_INCLUDE_DIRS}
)

target_link_libraries(zerokv
  PRIVATE
    Threads::Threads
    ZLIB::ZLIB
    PkgConfig::LZ4
)

# ---------- 启用 CTest ----------
include(CTest)
enable_testing()

# ---------- 可执行文件与测试 ----------

# 1. Benchmark (不作为 CTest 测试，仅手动运行)
add_executable(zerokv_bench tests/bench.cpp)
target_link_libraries(zerokv_bench PRIVATE zerokv)

# 1b. 长尾延迟基准 (不作为 CTest 测试，仅手动运行)
add_executable(zerokv_latency tests/latency_tests.cpp)
target_link_libraries(zerokv_latency PRIVATE zerokv)

# 1c. 大小 value 详细对比 (不作为 CTest 测试，仅手动运行)
add_executable(zerokv_value_compare tests/value_size_compare.cpp)
target_link_libraries(zerokv_value_compare PRIVATE zerokv)

# 1d. 干净的写/读采样负载 (供火焰图，不作为 CTest 测试)
add_executable(zerokv_clean_perf tests/clean_perf.cpp)
target_link_libraries(zerokv_clean_perf PRIVATE zerokv)

# 2. 单元测试
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/unit_tests.cpp")
  add_executable(zerokv_tests tests/unit_tests.cpp)
  target_link_libraries(zerokv_tests PRIVATE zerokv)
  add_test(NAME zerokv_unit_tests COMMAND zerokv_tests)
endif()

# 3. 组件测试 
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/component_tests.cpp")
  add_executable(zerokv_component_tests tests/component_tests.cpp)
  target_link_libraries(zerokv_component_tests PRIVATE zerokv)
  add_test(NAME zerokv_component_tests COMMAND zerokv_component_tests)
endif()

# 4. 可靠性测试 (C++ 部分) + 5. Bash 脚本部分（合并到同一条件块）
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/robustness_tests.cpp")
  add_executable(zerokv_robustness_tests tests/robustness_tests.cpp)
  target_link_libraries(zerokv_robustness_tests PRIVATE zerokv)
  
  add_test(NAME zerokv_robustness_cpp COMMAND zerokv_robustness_tests)

  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/scripts/run_robustness_crash.sh")
    add_test(NAME zerokv_crash_script 
             COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/tests/scripts/run_robustness_crash.sh 10)
    
    set_tests_properties(zerokv_crash_script PROPERTIES
      ENVIRONMENT "ZEROKV_ROBUSTNESS_BIN=$<TARGET_FILE:zerokv_robustness_tests>"
      TIMEOUT 600
      LABELS "robustness;crash"
    )
  endif()
endif()