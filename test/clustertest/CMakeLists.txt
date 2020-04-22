# Ensure required dependencies are available
# Note: This action is performed again to allow for module property availability
find_package(DL REQUIRED)
find_package(PCRECPP REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

# Gather all the files for the clustertest executable
file(GLOB CLUSTERTESTH BedrockClusterTester.h)
file(GLOB CLUSTERTESTCPP main.cpp)
file(GLOB CLUSTERTEST_TESTSCPP tests/*.cpp)

# Group the header and source for the clustertest executable (IDE)
source_group("Header Files" FILES ${CLUSTERTESTH})
source_group("Source Files" FILES ${CLUSTERTESTCPP})
source_group("Source Files\\tests" FILES ${CLUSTERTEST_TESTSCPP})

# Group the headers used by the clustertest executable (IDE)
source_group("Header Files\\tpunit" FILES ${TEST_TPUNITH})
source_group("Header Files\\bedrock" FILES ${BEDROCKH})
source_group("Header Files\\bedrock\\plugins" FILES ${BEDROCK_PLUGINSH})
source_group("Header Files\\bedrock\\sqlitecluster" FILES ${BEDROCK_SQLITECLUSTERH})
source_group("Header Files\\bedrock\\stuff" FILES ${BEDROCK_LIBSTUFFH})

# Add clustertest executable to the build
# Note: Header files are included for IDEs
add_executable(clustertest ${CLUSTERTESTCPP}
                           ${CLUSTERTEST_TESTPLUGINCPP}
                           ${CLUSTERTEST_TESTSCPP}
                           ${CLUSTERTESTH}
                           ${CLUSTERTEST_TESTPLUGINH}
                           ${TEST_TPUNITH}
                           ${BEDROCKH}
                           ${BEDROCK_PLUGINSH}
                           ${BEDROCK_SQLITECLUSTERH}
                           ${BEDROCK_LIBSTUFFH})
add_dependencies(clustertest bedrock-bin
                             tpunit
                             testplugin)
target_link_libraries(clustertest tpunit
                                  ${BEDROCK_LIBRARIES})
set_target_properties(clustertest PROPERTIES FOLDER "Tests")

# Add test plugin to the build
add_subdirectory(testplugin)