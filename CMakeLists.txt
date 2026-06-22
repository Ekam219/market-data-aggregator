cmake_minimum_required(VERSION 3.17)
project(MarketDataAggregator VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compiler flags ────────────────────────────────────────────────────────────
add_compile_options(
    -Wall -Wextra -Wpedantic
    -O3 -march=native
    -fno-omit-frame-pointer
)

# ── Core library ──────────────────────────────────────────────────────────────
file(GLOB_RECURSE CORE_SOURCES src/*.cpp)
list(REMOVE_ITEM CORE_SOURCES ${CMAKE_SOURCE_DIR}/src/main.cpp)

add_library(mda_core STATIC ${CORE_SOURCES})
target_include_directories(mda_core PUBLIC include)
target_link_libraries(mda_core PUBLIC pthread)

# ── Main executable ───────────────────────────────────────────────────────────
add_executable(market_data_aggregator src/main.cpp)
target_link_libraries(market_data_aggregator PRIVATE mda_core)

# ── Google Test ───────────────────────────────────────────────────────────────
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

file(GLOB TEST_SOURCES tests/*.cpp)
add_executable(mda_tests ${TEST_SOURCES})
target_include_directories(mda_tests PRIVATE include)
target_link_libraries(mda_tests PRIVATE mda_core GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(mda_tests)
