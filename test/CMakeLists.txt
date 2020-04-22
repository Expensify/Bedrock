# Ensure required dependencies are available
# Note: This action is performed again to allow for module property availability
find_package(DL REQUIRED)
find_package(PCRECPP REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

# Gather all the files for the test executable
file(GLOB TEST_TESTS_JOBSH tests/jobs/*.h)
file(GLOB TESTCPP main.cpp)
file(GLOB TEST_TESTSCPP tests/*.cpp)
file(GLOB TEST_TESTS_JOBSCPP tests/jobs/*.cpp)

# Gather the headers for the test framework library
file(GLOB TEST_TPUNITH lib/*.h)

# Group the header and source for the test executable (IDE)
source_group("Header Files\\tests\\jobs" FILES ${TEST_TESTS_JOBSH})
source_group("Source Files" FILES ${TESTCPP})
source_group("Source Files\\tests" FILES ${TEST_TESTSCPP})
source_group("Source Files\\tests\\jobs" FILES ${TEST_TESTS_JOBSCPP})

# Group the headers used by the test executable (IDE)
source_group("Header Files\\tpunit" FILES ${TEST_TPUNITH})
source_group("Header Files\\bedrock" FILES ${BEDROCKH})
source_group("Header Files\\bedrock\\plugins" FILES ${BEDROCK_PLUGINSH})
source_group("Header Files\\bedrock\\sqlitecluster" FILES ${BEDROCK_SQLITECLUSTERH})
source_group("Header Files\\bedrock\\stuff" FILES ${BEDROCK_LIBSTUFFH})

# Add test executable to the build
# Note: Header files are included for IDEs
add_executable(test ${TESTCPP}
                    ${TEST_TESTSCPP}
                    ${TEST_TESTS_JOBSCPP}
                    ${TEST_TESTS_JOBSH}
                    ${TEST_TPUNITH}
                    ${BEDROCKH}
                    ${BEDROCK_PLUGINSH}
                    ${BEDROCK_SQLITECLUSTERH}
                    ${BEDROCK_LIBSTUFFH})
add_dependencies(test bedrock-bin
                      tpunit
                      testplugin)
target_link_libraries(test tpunit
                           ${BEDROCK_LIBRARIES})
set_target_properties(test PROPERTIES FOLDER "Tests")

# Add a post action to the test executable build
# Note: This copies the sample_data directory for proper execution of the
#       test executable
add_custom_target(copy-sample_data ALL
                                   COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/sample_data ${CMAKE_CURRENT_BINARY_DIR}/sample_data
                                   DEPENDS test)
set_target_properties(copy-sample_data PROPERTIES FOLDER "Tests/Post Build Actions")

# Add clustertest, test plugin and test framework to the build
add_subdirectory(clustertest)
add_subdirectory(lib)