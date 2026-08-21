FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.15.2
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(googletest)

add_executable(tests
    test_main.cpp
    test_model.cpp
    test_ticker_codec.cpp
    test_instrument_registry.cpp
    test_blocking_queue.cpp
    test_deribit_session.cpp
)

target_include_directories(tests PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_definitions(tests PRIVATE
    QUANT_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
)
target_link_libraries(tests PRIVATE quant_marketdata GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(tests)
