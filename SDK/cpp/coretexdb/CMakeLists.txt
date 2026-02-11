cmake_minimum_required(VERSION 3.14)
project(coretexdb-cpp VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(CURL REQUIRED)
find_package(nlohmann_json 3.10.5 REQUIRED)

add_library(coretexdb
    src/client.cpp
    src/models.cpp
)

target_include_directories(coretexdb
    PUBLIC
    include
)

target_link_libraries(coretexdb
    PRIVATE
    CURL::CURL
    nlohmann_json::nlohmann_json
)

add_executable(basic_example examples/basic.cpp)
target_link_libraries(basic_example PRIVATE coretexdb)

add_executable(async_example examples/async.cpp)
target_link_libraries(async_example PRIVATE coretexdb)

enable_testing()
add_subdirectory(tests)
