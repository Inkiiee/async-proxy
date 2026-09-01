cmake_minimum_required(VERSION 3.20)

project(async_proxy VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

set(ASIO_INCLUDE_DIR "" CACHE PATH "Directory containing asio.hpp")
if(NOT ASIO_INCLUDE_DIR)
    find_path(ASIO_DETECTED_INCLUDE_DIR asio.hpp
        HINTS
            ${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/include
            ${CMAKE_CURRENT_SOURCE_DIR}/../otac-gw/third_party/asio_1.38.0/include
    )
    if(ASIO_DETECTED_INCLUDE_DIR)
        set(
            ASIO_INCLUDE_DIR
            ${ASIO_DETECTED_INCLUDE_DIR}
            CACHE PATH
            "Directory containing asio.hpp"
            FORCE
        )
    endif()
endif()

if(NOT ASIO_INCLUDE_DIR)
    message(FATAL_ERROR "Standalone Asio was not found. Set ASIO_INCLUDE_DIR to the directory containing asio.hpp")
endif()

add_library(async_proxy_lib
    src/http.cpp
    src/proxy_system.cpp
)

target_include_directories(async_proxy_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_include_directories(async_proxy_lib
    SYSTEM PUBLIC
        ${ASIO_INCLUDE_DIR}
)

target_compile_definitions(async_proxy_lib PUBLIC ASIO_STANDALONE)
target_compile_options(async_proxy_lib PRIVATE
    -Wall
    -Wextra
    -Wshadow
    $<$<CONFIG:Release>:-O2>
)
target_link_libraries(async_proxy_lib
    PUBLIC
        Threads::Threads
        OpenSSL::SSL
        OpenSSL::Crypto
)

add_executable(async_proxy src/main.cpp)
target_link_libraries(async_proxy PRIVATE async_proxy_lib)

include(CTest)
if(BUILD_TESTING)
    add_executable(async_proxy_http_tests tests/http_test.cpp)
    target_link_libraries(async_proxy_http_tests PRIVATE async_proxy_lib)
    add_test(NAME async_proxy_http_tests COMMAND async_proxy_http_tests)
endif()
