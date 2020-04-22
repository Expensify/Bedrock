cmake_minimum_required(VERSION 3.16) # Minimum required for precompiled headers
project(Bedrock)

if(WIN32)
  message(FATAL_ERROR "Windows platform is not currently supported")
elseif(DEFINED ENV{WSL_DISTRO_NAME})
  message(WARNING "Windows subsystem for linux is not fully supported")
endif()

# Handle submodule initialization if necessary
find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
  execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                  OUTPUT_VARIABLE GIT_REVISION
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
  execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
                  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                  OUTPUT_VARIABLE GIT_SHORT_REVISION
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "Bedrock revision: ${GIT_SHORT_REVISION}")
  option(GIT_SUBMODULE "Check submodules during build" ON)
  if(GIT_SUBMODULE AND NOT EXISTS "${PROJECT_SOURCE_DIR}/mbedtls/CMakeLists.txt")
    execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                    RESULT_VARIABLE GIT_SUBMODULE_RESULT
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT GIT_SUBMODULE_RESULT EQUAL "0")
      message(FATAL_ERROR "git submodule update --init failed: ${GIT_SUBMODULE_RESULT}")
    endif()
  endif()
endif()

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/mbedtls/CMakeLists.txt")
  message(FATAL_ERROR "Third party library mbedtls is missing")
endif()

# Ensure additional CMake modules are made available
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/modules)
include(ExternalProject)

# Default to release builds
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(ENABLE_TESTING "Build Bedrock tests" Off)
option(ENABLE_MBEDTLS_PROGRAMS "Build libmedtls programs" Off)
option(ENABLE_PRODUCTION "Build Bedrock in production mode" Off)

# Ensure required dependencies are available
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(DL REQUIRED)
find_package(PCRECPP REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

# Define common CXX flags for libraries and executables
set(CMAKE_CXX_FLAGS "$ENV{BEDROCK_OPTIM_COMPILE_FLAG} \
 -Wall \
 -Werror \
 -Wformat-security \
 -DGIT_REVISION=${GIT_SHORT_REVISION} \
 -MMD")
set(CMAKE_POSITION_INDEPENDENT_CODE ON) # -fPIC
set(CMAKE_CXX_STANDARD 14) # -std=c++14
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off) # Ensure -std=gnu++14 is disabled
set(BEDROCK_LINKER_FLAGS "-rdynamic")
set(CMAKE_SHARED_LINKER_FLAGS "${BEDROCK_LINKER_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${BEDROCK_LINKER_FLAGS}")

# Place all external project build files in their own directory within the build
set_directory_properties(PROPERTIES EP_BASE "${PROJECT_BINARY_DIR}/external")

# Gather all the files for the Bedrock library
file(GLOB BEDROCKH *.h)
file(GLOB BEDROCK_PLUGINSH plugins/*.h)
file(GLOB BEDROCK_SQLITECLUSTERH sqlitecluster/*.h)
file(GLOB BEDROCKCPP *.cpp)
list(REMOVE_ITEM BEDROCKCPP "${PROJECT_SOURCE_DIR}/main.cpp")
file(GLOB BEDROCK_PLUGINSCPP plugins/*.cpp)
file(GLOB BEDROCK_SQLITECLUSTERCPP sqlitecluster/*.cpp)

# Gather the header files used by the Bedrock library
file(GLOB BEDROCK_LIBSTUFFH libstuff/*.h)

# Allow for folders to be used (IDEs)
set_property(GLOBAL PROPERTY USE_FOLDERS On)

# Group the header and source files for the Bedrock library (IDE)
source_group("Header Files" FILES ${BEDROCKH})
source_group("Header Files\\plugins" FILES ${BEDROCK_PLUGINSH})
source_group("Header Files\\sqlitecluster" FILES ${BEDROCK_SQLITECLUSTERH})
source_group("Source Files" FILES ${BEDROCKCPP})
source_group("Source Files\\plugins" FILES ${BEDROCK_PLUGINSCPP})
source_group("Source Files\\sqlitecluster" FILES ${BEDROCK_SQLITECLUSTERCPP})

# Group the libstuff headers used by the Bedrock library (IDE)
source_group("Header Files\\stuff" FILES ${BEDROCK_LIBSTUFFH})

# Ensure the proper includes are made available for all libraries and executables
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/mbedtls/include)

# Establish an external project for in source dependency build of libmedtls
externalproject_add(libmbedtls
                    SOURCE_DIR ${PROJECT_SOURCE_DIR}/mbedtls
                    INSTALL_DIR ${PROJECT_BINARY_DIR}/libmedtls
                    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}/libmedtls
                               -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE}
                               -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                               -DCMAKE_POSITION_INDEPENDENT_CODE=On
                               -DENABLE_PROGRAMS=${ENABLE_MBEDTLS_PROGRAMS}
                               -DENABLE_TESTING=Off)
ExternalProject_Get_Property(libmbedtls INSTALL_DIR)
link_directories(${INSTALL_DIR}/lib)

# Add libstuff to the build
add_subdirectory(libstuff)

# Add libbedrock to the build
# Note: Header files are included for IDEs
add_library(bedrock STATIC ${BEDROCKCPP}
                           ${BEDROCK_PLUGINSCPP}
                           ${BEDROCK_SQLITECLUSTERCPP}
                           ${BEDROCKH}
                           ${BEDROCK_PLUGINSH}
                           ${BEDROCK_SQLITECLUSTERH}
                           ${BEDROCK_LIBSTUFFH})
target_precompile_headers(bedrock PRIVATE libstuff/libstuff.h)
set_target_properties(bedrock PROPERTIES FOLDER "Libraries")

# Create a common list of libraries for Bedrock
list(APPEND BEDROCK_LIBRARIES bedrock stuff
                              mbedtls mbedx509 mbedcrypto
                              DL::DL
                              PCRECPP::PCRECPP
                              Threads::Threads
                              ZLIB::ZLIB)

# Add bedrock executable to the build
# Note: Header files are included for IDEs
add_executable(bedrock-bin main.cpp
                           ${BEDROCKH}
                           ${BEDROCK_PLUGINSH}
                           ${BEDROCK_SQLITECLUSTERH}
                           ${BEDROCK_LIBSTUFFH})
add_dependencies(bedrock-bin bedrock
                             libmbedtls
                             stuff)
target_link_libraries(bedrock-bin ${BEDROCK_LIBRARIES})
set_target_properties(bedrock-bin PROPERTIES OUTPUT_NAME bedrock
                                             FOLDER "Applications")

if(ENABLE_TESTING)
  # Add tests to the build
  add_subdirectory(test)
endif()
