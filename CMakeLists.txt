cmake_minimum_required(VERSION 3.20)
project(quant_cpp VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
# -march=native tunes for *this* machine: the binary may fault with an illegal
# instruction on an older CPU. Drop it before shipping a build elsewhere.
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native")

# Sanitizers are a build-wide choice, not a property of the Debug config:
# ASan and TSan are mutually exclusive, and the producer/consumer handoff in
# src/queue wants TSan specifically. A mutex does not exempt it -- TSan is what
# catches a tick read after it was handed over, or shutdown racing a blocked pop.
set(QUANT_SANITIZER "none" CACHE STRING "Sanitizer to build with: none|address|thread")
set_property(CACHE QUANT_SANITIZER PROPERTY STRINGS none address thread)

if(QUANT_SANITIZER STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(QUANT_SANITIZER STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(NOT QUANT_SANITIZER STREQUAL "none")
  message(FATAL_ERROR "QUANT_SANITIZER must be one of: none, address, thread (got '${QUANT_SANITIZER}')")
endif()

# Beast and Asio are header-only; only JSON needs a compiled component.
find_package(Boost 1.81 REQUIRED COMPONENTS json)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(PostgreSQL REQUIRED)

FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.14.1
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(spdlog)

# libpqxx is not packaged on every dev box (and installing it needs root), so it
# is pinned here the same way googletest is. It wraps libpq, which find_package
# above has already located.
set(SKIP_BUILD_TEST ON CACHE BOOL "" FORCE)
FetchContent_Declare(libpqxx
    GIT_REPOSITORY https://github.com/jtv/libpqxx.git
    GIT_TAG        8.0.2
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(libpqxx)

add_library(quant_marketdata STATIC
    src/core/Config.cpp
    src/core/Log.cpp
    src/marketdata/model.cpp
    src/marketdata/TickerCodec.cpp
    src/marketdata/InstrumentRepository.cpp
    src/marketdata/InstrumentRegistry.cpp
    src/marketdata/JsonRpcSession.cpp
    src/marketdata/deribit.cpp
)
target_include_directories(quant_marketdata PUBLIC src include)
target_link_libraries(quant_marketdata PUBLIC
    Boost::json
    Boost::headers
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    pqxx
    spdlog::spdlog
)

add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE quant_marketdata)

# Tests
option(BUILD_TESTS "Build tests" ON)

if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
