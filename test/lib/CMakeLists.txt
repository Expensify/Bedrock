# Gather all the files for the test framework library
file(GLOB TEST_LIBTPUNITH *.h)
file(GLOB TEST_LIBTPUNITCPP *.cpp)

# Group the header and source for the test framework library (IDE)
source_group("Header Files" FILES ${TEST_LIBTPUNITH})
source_group("Source Files" FILES ${TEST_LIBTPUNITCPP})

# Group the headers used by the test framework library (IDE)
source_group("Header Files\\bedrock" FILES ${BEDROCKH})
source_group("Header Files\\bedrock\\plugins" FILES ${BEDROCK_PLUGINSH})
source_group("Header Files\\bedrock\\sqlitecluster" FILES ${BEDROCK_SQLITECLUSTERH})
source_group("Header Files\\bedrock\\stuff" FILES ${LIBSTUFFH})

# Add test framework library to the build
# Note: Header files are included for IDEs
add_library(tpunit ${TEST_LIBTPUNITCPP}
                   ${TEST_LIBTPUNITH}
                   ${BEDROCKH}
                   ${BEDROCK_PLUGINSH}
                   ${BEDROCK_SQLITECLUSTERH}
                   ${LIBSTUFFH})
add_dependencies(tpunit bedrock
                        stuff)
set_target_properties(tpunit PROPERTIES FOLDER "Tests/Framework")