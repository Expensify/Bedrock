# PCRECPP_FOUND - Perl Compatible Regular Expressions available
# PCRECPP_INCLUDE_DIR - Where pcrecpp.h is located.
# PCRECPP_LIBRARY - Libraries needed to use PCRE
include(CheckFunctionExists)

find_path(PCRECPP_INCLUDE_DIR NAMES pcrecpp.h)
find_library(PCRECPP_LIBRARY NAMES pcrecpp)
if(PCRECPP_LIBRARY)
  set(PCRECPP_FOUND TRUE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCRECPP FOUND_VAR PCRECPP_FOUND
                                          REQUIRED_VARS PCRECPP_INCLUDE_DIR)
mark_as_advanced(PCRECPP_INCLUDE_DIR DL_LIBRARY)

if(PCRECPP_FOUND AND NOT TARGET PCRECPP::PCRECPP)
  if(PCRECPP_LIBRARY)
    add_library(PCRECPP::PCRECPP UNKNOWN IMPORTED)
    set_target_properties(PCRECPP::PCRECPP PROPERTIES
                                           IMPORTED_LOCATION "${PCRECPP_LIBRARY}")
  endif()
  set_target_properties(PCRECPP::PCRECPP PROPERTIES
                                         INTERFACE_INCLUDE_DIRECTORIES "${DL_INCLUDE_DIR}")
endif()