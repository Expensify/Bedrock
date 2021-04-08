# Set the compiler, if it's not set by the environment.
# TODO: Remove when the standard dev environment uses the new variable names.
ifndef CXX
	echo "Auto-setting C++ compiler to g++ 9"
	CXX = g++-9
endif

ifndef CC
	echo "Auto-setting C compiler to gcc 9"
	CC = gcc-9
endif

# Set the optimization level from the environment, or default to -O2.
ifndef BEDROCK_OPTIM_COMPILE_FLAG
	BEDROCK_OPTIM_COMPILE_FLAG = -O2
endif

# Pull some variables from the git repo itself. Note that this means this build does not work if Bedrock isn't
# contained in a git repo.
GIT_REVISION = "-DGIT_REVISION=$(shell git rev-parse --short HEAD)"
PROJECT = $(shell git rev-parse --show-toplevel)

# Set our include paths. We need this for the pre-processor to use to generate dependencies.
INCLUDE = -I$(PROJECT) -I$(PROJECT)/mbedtls/include

# Set our standard C++ compiler flags
CXXFLAGS = -g -std=c++17 -fpic $(BEDROCK_OPTIM_COMPILE_FLAG) -Wall -Werror -Wformat-security $(GIT_REVISION) $(INCLUDE)

# All our intermediate, dependency, object, etc files get hidden in here.
INTERMEDIATEDIR = .build

# We use the same library paths and required libraries for all binaries.
LIBPATHS =-Lmbedtls/library -L$(PROJECT)
LIBRARIES =-lbedrock -lstuff -lbedrock -ldl -lpcrecpp -lpthread -lmbedtls -lmbedx509 -lmbedcrypto -lz

# These targets aren't actual files.
.PHONY: all test clustertest clean testplugin

# This sets our default by being the first target, and also sets `all` in case someone types `make all`.
all: bedrock test clustertest
test: test/test
clustertest: test/clustertest/clustertest testplugin

# TODO: Collapse the separate Makefile into this one.
testplugin:
	cd test/clustertest/testplugin && $(MAKE)

clean:
	rm -rf $(INTERMEDIATEDIR)
	rm -rf libstuff.a
	rm -rf libbedrock.a
	rm -rf bedrock
	rm -rf test/test
	rm -rf test/clustertest/clustertest
	# The following two lines are unused but will remove old files that are no longer needed.
	rm -rf libstuff/libstuff.d 
	rm -rf libstuff/libstuff.h.gch
	# If we've never run `make`, `mbedtls/Makefile` does not exist. Add a `test
	# -f` check and `|| true` so it doesn't cause `make clean` to exit nonzero
	(test -f mbedtls/Makefile && cd mbedtls && $(MAKE) clean) || true
	cd test/clustertest/testplugin && $(MAKE) clean

# Rule to build mbedtls.
mbedtls/library/libmbedcrypto.a mbedtls/library/libmbedtls.a mbedtls/library/libmbedx509.a:
	git submodule init
	git submodule update
	cd mbedtls && git checkout -q 04a049bda1ceca48060b57bc4bcf5203ce591421
	cd mbedtls && $(MAKE) no_test

# We select all of the cpp files (and manually add sqlite3.c) that will be in libstuff.
# We then transform those file names into a list of object file name and dependency file names.
STUFFCPP = $(shell find libstuff -name '*.cpp')
STUFFOBJ = $(STUFFCPP:%.cpp=$(INTERMEDIATEDIR)/%.o) $(INTERMEDIATEDIR)/libstuff/sqlite3.o
STUFFDEP = $(STUFFCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# The same for libbedrock.
LIBBEDROCKCPP = $(shell find * -name '*.cpp' -not -name main.cpp -not -path 'test*' -not -path 'libstuff*')
LIBBEDROCKOBJ = $(LIBBEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
LIBBEDROCKDEP = $(LIBBEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# And the same for the main binary (which is just adding main.cpp)
BEDROCKCPP = main.cpp
BEDROCKOBJ = $(BEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
BEDROCKDEP = $(BEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# And the same for our tests.
TESTCPP = $(shell find test -name '*.cpp' -not -path 'test/clustertest*')
TESTOBJ = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
TESTDEP = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# And the same for the cluster tests (manually adding one file from `test`)
CLUSTERTESTCPP = $(shell find test -name '*.cpp' -not -path 'test/tests*' -not -path "test/main.cpp")
CLUSTERTESTCPP += test/tests/jobs/JobTestHelper.cpp
CLUSTERTESTOBJ = $(CLUSTERTESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
CLUSTERTESTDEP = $(CLUSTERTESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# Rules to build our two static libraries.
libstuff.a: $(STUFFOBJ)
	ar crv $@ $(STUFFOBJ)
libbedrock.a: $(LIBBEDROCKOBJ)
	ar crv $@ $(LIBBEDROCKOBJ)

# The prerequisites for all binaries are the same. We only include one of the mbedtls libs to avoid building three
# times in parallel if it's out of date.
BINPREREQS = libbedrock.a libstuff.a mbedtls/library/libmbedcrypto.a

# All of our binaries build in the same way.
bedrock: $(BEDROCKOBJ) $(BINPREREQS)
	$(CXX) -o $@ $(BEDROCKOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)
test/test: $(TESTOBJ) $(BINPREREQS)
	$(CXX) -o $@ $(TESTOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)
test/clustertest/clustertest: $(CLUSTERTESTOBJ) $(BINPREREQS)
	$(CXX) -o $@ $(CLUSTERTESTOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)

# Make dependency files from cpp files, putting them in $INTERMEDIATEDIR.
$(INTERMEDIATEDIR)/%.d: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(INCLUDE) -MM -MF $@ -MT $(@:.d=.o) $<

# The object files depend on both the cpp source files and the dependency files. They also depend on the precompiled
# header file, because that will force the precompiled header to be built before the object files that use it.
$(INTERMEDIATEDIR)/%.o: %.cpp $(INTERMEDIATEDIR)/%.d
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ -c $<

# Build c files. This is just for sqlite, so we don't bother with dependencies for it.
# SQLITE_MAX_MMAP_SIZE is set to 16TB.
$(INTERMEDIATEDIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) -O2 $(BEDROCK_OPTIM_COMPILE_FLAG) -Wno-unused-but-set-variable -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_ENABLE_NOOP_UPDATE -DSQLITE_MUTEX_ALERT_MILLISECONDS=20 -DHAVE_USLEEP=1 -DSQLITE_MAX_MMAP_SIZE=17592186044416ull -DSQLITE_SHARED_MAPPING -DSQLITE_ENABLE_NORMALIZE -o $@ -c $<

# Bring in the dependency files. This will cause them to be created if necessary. This is skipped if we're cleaning, as
# they'll just get deleted anyway.
ifneq ($(MAKECMDGOALS),clean)
-include $(LIBBEDROCKDEP)
-include $(STUFFDEP)
-include $(TESTDEP)
-include $(CLUSTERTESTDEP)
-include $(BEDROCKDEP)
endif
