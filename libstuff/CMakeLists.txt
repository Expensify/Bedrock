# Gather all the files for the stuff library
file(GLOB LIBSTUFFH *.h)
file(GLOB LIBSTUFFCPP *.cpp)
file(GLOB LIBSTUFFC *.c)

# Group the header and source files for the stuff library (IDE)
source_group("Header Files" FILES ${LIBSTUFFH})
source_group("Source Files" FILES ${LIBSTUFFCPP} ${LIBSTUFFC})

# Gather and group the headers for the mbedtls library (IDE)
file(GLOB LIBMBEDTLSH ${PROJECT_SOURCE_DIR}/mbedtls/include/mbedtls/*.h)
source_group("Header Files\\mbedtls" FILES ${LIBMBEDTLSH})

# Define the C flags for the c files within the stuff library
# Note: This is basically just for sqlite, so we don't bother with dependencies
# for it. SQLITE_MAX_MMAP_SIZE is set to 16TB
set(LIBSTUFF_C_FLAGS "$ENV{BEDROCK_OPTIM_COMPILE_FLAG} \
 -Wno-unused-but-set-variable \
 -DSQLITE_ENABLE_STAT4 \
 -DSQLITE_ENABLE_JSON1 \
 -DSQLITE_ENABLE_SESSION \
 -DSQLITE_ENABLE_PREUPDATE_HOOK \
 -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
 -DSQLITE_ENABLE_NOOP_UPDATE \
 -DSQLITE_MUTEX_ALERT_MILLISECONDS=20 \
 -DSQLITE_MAX_MMAP_SIZE=17592186044416ull \
 -DSQLITE_SHARED_MAPPING \
 -DSQLITE_ENABLE_NORMALIZE")
check_function_exists("usleep" HAVE_USLEEP)
if(HAVE_USLEEP)
  set(LIBSTUFF_C_FLAGS "${LIBSTUFF_C_FLAGS} -DHAVE_USLEEP=1")
else()
  message(FATAL_ERROR "usleep is required")
endif()
set(CMAKE_C_FLAGS "${LIBSTUFF_C_FLAGS}")

# Add libstuff to the build
# Note: Header files are included for IDEs
ADD_LIBRARY(stuff STATIC
            ${LIBSTUFFCPP}
            ${LIBSTUFFC}
            ${LIBSTUFFH}
            ${LIBMBEDTLSH})
set_target_properties(stuff PROPERTIES FOLDER "Libraries")