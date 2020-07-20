# Gather all the files for the test plugin library
file(GLOB CLUSTERTEST_TESTPLUGINH *.h)
file(GLOB CLUSTERTEST_TESTPLUGINCPP *.cpp)

# Group the header and source for the test plugin library (IDE)
source_group("Header Files" FILES ${CLUSTERTEST_TESTPLUGINH})
source_group("Source Files" FILES ${CLUSTERTEST_TESTPLUGINCPP})

# Group the headers used by the test plugin library (IDE)
source_group("Header Files\\bedrock" FILES ${BEDROCKH})
source_group("Header Files\\bedrock\\stuff" FILES ${BEDROCK_LIBSTUFFH})

# Define CXX flags specific for the test plugin library
set(CMAKE_CXX_EXTENSIONS On) # Ensure -std=gnu++14 is enabled
set(TESTPLUGIN_CXX_FLAGS "-DSVERSION=\"${GIT_REVISION}\" \
 -Wno-unused-result")
if(ENABLE_PRODUCTION)
  set(TESTPLUGIN_CXX_FLAGS "${TESTPLUGIN_CXX_FLAGS} \
 -fstack-protector \
 --param=ssp-buffer-size=4 \
 -Wformat")
endif()
set(CMAKE_CXX_FLAGS "${TESTPLUGIN_CXX_FLAGS}")

# Add testplugin library to the build
# Note: Header files are included for IDEs
add_library(testplugin SHARED
           ${CLUSTERTEST_TESTPLUGINCPP}
           ${CLUSTERTEST_TESTPLUGINH}
           ${BEDROCKH}
           ${BEDROCK_LIBSTUFFH})
add_dependencies(testplugin bedrock
                            libmbedtls
                            stuff)
if(APPLE)
  # OSX needs to re-link the libraries for some reason
  target_link_libraries(testplugin ${BEDROCK_LIBRARIES})
endif()
set_target_properties(testplugin PROPERTIES PREFIX ""  # Remove lib prefix
                                            FOLDER "Tests/Plugins")