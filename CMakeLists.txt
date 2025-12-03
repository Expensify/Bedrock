cmake_minimum_required(VERSION 3.10)
project(Bedrock)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

link_libraries("-fuse-ld=mold")

# Find the clang-tidy executable first
find_program(CLANG_TIDY_EXECUTABLE NAMES clang-tidy-18)
if(CLANG_TIDY_EXECUTABLE)
    # clang-tidy will automatically find and use the .clang-tidy config file
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXECUTABLE}")
endif()

# Amalgamation flags
set(AMALGAMATION_FLAGS " -Wno-unused-but-set-variable -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_ENABLE_NOOP_UPDATE -DSQLITE_MUTEX_ALERT_MILLISECONDS=20 -DHAVE_USLEEP=1 -DSQLITE_MAX_MMAP_SIZE=17592186044416ull -DSQLITE_SHARED_MAPPING -DSQLITE_ENABLE_NORMALIZE -DSQLITE_MAX_PAGE_COUNT=4294967294 -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS -DSQLITE_DEFAULT_CACHE_SIZE=-51200 -DSQLITE_MAX_FUNCTION_ARG=32767 -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=0 -DSQLITE_ENABLE_WAL_BIGHASH -DSQLITE_ENABLE_WAL2NOCKSUM")

# Set our standard C++ compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -DSQLITE_ENABLE_NORMALIZE -Wall -Werror -Wformat-security -Wno-unqualified-std-cast-call -Wno-sign-conversion -Wno-error=deprecated-declarations -Wunused-variable -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c23 -fPIC ${AMALGAMATION_FLAGS}")

if(DEFINED ENV{BEDROCK_OPTIM_COMPILE_FLAG})
    message(STATUS "Applying BEDROCK_OPTIM_COMPILE_FLAG compile flag: $ENV{BEDROCK_OPTIM_COMPILE_FLAG}")
    # Adjust the optimization level without repeating it to avoid confusions
    string(REGEX REPLACE "([\\/\\-]O3)" "$ENV{BEDROCK_OPTIM_COMPILE_FLAG}" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
    string(REGEX REPLACE "([\\/\\-]O3)" "$ENV{BEDROCK_OPTIM_COMPILE_FLAG}" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
    string(REGEX REPLACE "([\\/\\-]O2)" "$ENV{BEDROCK_OPTIM_COMPILE_FLAG}" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
    string(REGEX REPLACE "([\\/\\-]O2)" "$ENV{BEDROCK_OPTIM_COMPILE_FLAG}" CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO}")
else()
    # Modify compile flags to change optimization level from O3 to O2
    string(REGEX REPLACE "([\\/\\-]O)3" "\\12" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
    string(REGEX REPLACE "([\\/\\-]O)3" "\\12" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
endif()

# Check if Git is available. This is good practice.
find_package(Git QUIET)
if(Git_FOUND)
    # Execute the git command and store the output in GIT_REVISION
    execute_process(
        COMMAND git rev-parse HEAD
        OUTPUT_VARIABLE GIT_REVISION
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )

    # Shorten the hash to the first 10 characters, similar to grep -o '^.\{10\}'
    string(SUBSTRING "${GIT_REVISION}" 0 10 GIT_REVISION)
    message(STATUS "Git Revision: ${GIT_REVISION}")
else()
    message(WARNING "Git not found. Unable to get revision hash.")
    set(GIT_REVISION 1)
endif()

option(ENABLE_BUILD_PROFILING "Enable build profiling" OFF)
if (ENABLE_BUILD_PROFILING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftime-trace")
endif()

# There files need GIT_REVISION
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/BedrockServer.cpp PROPERTIES COMPILE_OPTIONS "-DGIT_REVISION=${GIT_REVISION}")
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/main.cpp PROPERTIES COMPILE_OPTIONS "-DGIT_REVISION=${GIT_REVISION}")
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/plugins/MySQL.cpp PROPERTIES COMPILE_OPTIONS "-DGIT_REVISION=${GIT_REVISION}")

# Set our include paths. We need this for the pre-processor to use to generate dependencies.
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/mbedtls/include)

# We use the same library paths and required libraries for all binaries.
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/mbedtls/library)

# We use our project directory as a search path so we don't need "../../../.." all over the place.
set(CMAKE_INCLUDE_CURRENT_DIR "ON")

# # Disable mbed tests and programs
# set(ENABLE_TESTING OFF CACHE BOOL "Build Mbed TLS tests")
# set(ENABLE_PROGRAMS OFF CACHE BOOL "Build Mbed TLS programs")

# # Add the mbedtls directory as a subdirectory.
# # This executes its CMakeLists.txt and creates the necessary targets (like mbedcrypto, mbedtls, mbedx509).
# add_subdirectory(mbedtls)

file(GLOB_RECURSE LIBSTUFF_SRC "${CMAKE_CURRENT_SOURCE_DIR}/libstuff/*.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/libstuff/qrf.c" "${CMAKE_CURRENT_SOURCE_DIR}/libstuff/sqlite3.c")
add_library(libstuff STATIC ${LIBSTUFF_SRC})
set_target_properties(libstuff PROPERTIES PREFIX "")

# Make an explicif list of files since the use of globs can be considered a bad practice
set(BEDROCK_SRC
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockBlockingCommandQueue.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockCommand.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockCommandQueue.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockConflictManager.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockCore.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockPlugin.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockServer.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/BedrockTimeoutCommandQueue.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/PageLockGuard.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/VMTouch.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/SDeburrBench.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/main.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/plugins/Cache.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/plugins/DB.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/plugins/Jobs.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/plugins/MySQL.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLite.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLiteClusterMessenger.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLiteCommand.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLiteCore.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLiteNode.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLitePeer.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLitePool.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/sqlitecluster/SQLiteUtils.cpp"
)

add_library(libbedrock STATIC ${BEDROCK_SRC})
set_target_properties(libbedrock PROPERTIES PREFIX "")# OUTPUT_NAME auth LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/auth")
target_link_libraries(libbedrock PUBLIC libstuff)
target_link_libraries(libbedrock INTERFACE dl pcre2-8 pthread mbedtls mbedx509 mbedcrypto z m)

# Build bedrock executable
add_executable(bedrock "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_link_libraries(bedrock PRIVATE libbedrock)

# Common files for tests and cluster tests
file(GLOB_RECURSE LIBTEST_SRC "${CMAKE_CURRENT_SOURCE_DIR}/test/lib/*.cpp")

# Test only include main, tests and lib (should not include clustertest)
file(GLOB_RECURSE TESTS_SRC "${CMAKE_CURRENT_SOURCE_DIR}/test/tests/*.cpp")
add_executable(test "${LIBTEST_SRC}" "${TESTS_SRC}" "${CMAKE_CURRENT_SOURCE_DIR}/test/main.cpp")
target_link_libraries(test PRIVATE libbedrock)

# And the same for cluster tests minus the ordinary tests
file(GLOB_RECURSE CLUSTERTEST_SRC "${CMAKE_CURRENT_SOURCE_DIR}/test/clustertest/*.cpp")
add_executable(clustertest "${LIBTEST_SRC}" "${CLUSTERTEST_SRC}" "${CMAKE_CURRENT_SOURCE_DIR}/test/tests/jobs/JobTestHelper.cpp")
target_link_libraries(clustertest PRIVATE libbedrock)

# Benchmarks binary (separate from unit tests) under top-level benchmarks/
file(GLOB_RECURSE BENCH_SRC "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/*.cpp")
add_executable(bench "${BENCH_SRC}" "${CMAKE_CURRENT_SOURCE_DIR}/test/lib/tpunit++.cpp")
target_link_libraries(bench PRIVATE libbedrock)

# The rule to build TestPlugin
add_library(testplugin SHARED "test/clustertest/testplugin/TestPlugin.cpp" "test/clustertest/testplugin/ExternPointer.cpp")
set_target_properties(testplugin PROPERTIES PREFIX "" OUTPUT_NAME testplugin LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_options(testplugin PRIVATE -Wall -Werror -Wformat-security -Wno-unqualified-std-cast-call -Wno-sign-conversion -fPIC)
target_link_libraries(testplugin INTERFACE libbedrock)

