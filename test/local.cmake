set(TEST_CC
        test/bucket_test.cpp
        test/hash_ternary_join_test.cpp
)

add_executable(gtest_external_join_lib test/tester.cpp ${TEST_CC})
target_link_libraries(gtest_external_join_lib PRIVATE  gtest gmock Threads::Threads external_join_lib)
#target_include_directories(gtest_external_join_lib PUBLIC ${LIB_PROJECT_DIR}/include)

enable_testing()
add_test(external_join_lib gtest_external_join_lib)

#add_clang_tidy_target(lint_test "${TEST_CC}")
#add_dependencies(lint_test gtest)
#list(APPEND lint_targets lint_test)

