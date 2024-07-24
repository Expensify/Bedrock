# If $CC and $CXX are defined as environment variables, those will be used here. However, if they aren't then GNU make
# automatically defines them as `cc` and `g++`. Ultimately, we'd like those names to work, or the environment variables
# to be set, but for the time being we need to override the defaults so that our existing dev environment works. This
# can be removed when that is resolved.
ifeq ($(CC),cc)
CC = gcc-13
endif
ifeq ($(CXX),g++)
CXX = g++-13
endif

# Set the optimization level from the environment, or default to -O2.
ifndef BEDROCK_OPTIM_COMPILE_FLAG
	BEDROCK_OPTIM_COMPILE_FLAG = -O2
endif

# Pull some variables from the git repo itself. Note that this means this build does not work if Bedrock isn't
# contained in a git repo.
GIT_REVISION = -DGIT_REVISION=$(shell git rev-parse HEAD | grep -o '^.\{10\}')
PROJECT = $(shell git rev-parse --show-toplevel)

# Set our include paths. We need this for the pre-processor to use to generate dependencies.
INCLUDE = -I$(PROJECT) -I$(PROJECT)/mbedtls/include

# Set our standard C++ compiler flags
CXXFLAGS = -g -std=c++20 -fPIC -DSQLITE_ENABLE_NORMALIZE $(BEDROCK_OPTIM_COMPILE_FLAG) -Wall -Werror -Wformat-security  -Wno-error=deprecated-declarations $(INCLUDE)

# Amalgamation flags
AMALGAMATION_FLAGS = -Wno-unused-but-set-variable -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_ENABLE_NOOP_UPDATE -DSQLITE_MUTEX_ALERT_MILLISECONDS=20 -DHAVE_USLEEP=1 -DSQLITE_MAX_MMAP_SIZE=17592186044416ull -DSQLITE_SHARED_MAPPING -DSQLITE_ENABLE_NORMALIZE -DSQLITE_MAX_PAGE_COUNT=4294967294 -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS

# All our intermediate, dependency, object, etc files get hidden in here.
INTERMEDIATEDIR = .build

# We use the same library paths and required libraries for all binaries.
LIBPATHS =-L$(PROJECT) -Lmbedtls/library
LIBRARIES =-Wl,--start-group -lbedrock -lstuff -Wl,--end-group -ldl -lpcrecpp -lpthread -lmbedtls -lmbedx509 -lmbedcrypto -lz -lm

# These targets aren't actual files.
.PHONY: all test clustertest clean testplugin deploy

# This sets our default by being the first target, and also sets `all` in case someone types `make all`.
all: bedrock test clustertest
test: test/test
clustertest: test/clustertest/clustertest testplugin
testplugin: test/clustertest/testplugin/testplugin.so

deploy:
	scp -J cole@bastion1.sjc bedrock ubuntu@54.184.12.51:~/bedrock

clean:
	rm -rf $(INTERMEDIATEDIR)
	rm -rf libstuff.a
	rm -rf libbedrock.a
	rm -rf bedrock
	rm -rf test/test
	rm -rf test/clustertest/clustertest
	rm -rf test/clustertest/testplugin/testplugin.so
	# The following two lines are unused but will remove old files that are no longer needed.
	rm -rf libstuff/libstuff.d
	rm -rf libstuff/libstuff.h.gch
	# If we've never run `make`, `mbedtls/Makefile` does not exist. Add a `test
	# -f` check and `|| true` so it doesn't cause `make clean` to exit nonzero
	(test -f mbedtls/Makefile && cd mbedtls && $(MAKE) clean) || true

# Rule to build mbedtls.
mbedtls/library/libmbedcrypto.a mbedtls/library/libmbedtls.a mbedtls/library/libmbedx509.a:
	git submodule init
	git submodule update
	cd mbedtls && git checkout -q v2.26.0
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

# And the same for the test plugin.
TESTPLUGINCPP = test/clustertest/testplugin/TestPlugin.cpp
TESTPLUGINOBJ = $(TESTPLUGINCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
TESTPLUGINTDEP = $(TESTPLUGINCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

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

# The rule to build TestPlugin
test/clustertest/testplugin/testplugin.so : $(TESTPLUGINOBJ) $(TESTPLUGINCPP) $(TESTPLUGINTDEP) $(BINPREREQS)
	$(CXX) $(CXXFLAGS) $(INCLUDE) $(TESTPLUGINOBJ) $(LIBPATHS) -shared -o $@

# All of the following files (BedrockServer, main, MySQL) require GIT_REVISION so we will pass that variable into the compiler for these.
$(INTERMEDIATEDIR)/BedrockServer.d $(INTERMEDIATEDIR)/BedrockServer.o: BedrockServer.cpp
	$(CXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/BedrockServer.d -MT $(INTERMEDIATEDIR)/BedrockServer.o $(GIT_REVISION) -o $(INTERMEDIATEDIR)/BedrockServer.o -c BedrockServer.cpp

$(INTERMEDIATEDIR)/main.d $(INTERMEDIATEDIR)/main.o: main.cpp
	$(CXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/main.d -MT $(INTERMEDIATEDIR)/main.o $(GIT_REVISION) -o $(INTERMEDIATEDIR)/main.o -c main.cpp

$(INTERMEDIATEDIR)/plugins/MySQL.d $(INTERMEDIATEDIR)/plugins/MySQL.o: plugins/MySQL.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/plugins/MySQL.d -MT $(INTERMEDIATEDIR)/plugins/MySQL.o $(GIT_REVISION) -o $(INTERMEDIATEDIR)/plugins/MySQL.o -c plugins/MySQL.cpp

# This builds both the dependencies and the object file from the cpp.
# We include one of the mbedtls files as a dependency because building it will cause our header files to get created,
# which many of our cpp files will reference.
$(INTERMEDIATEDIR)/%.d $(INTERMEDIATEDIR)/%.o: %.cpp mbedtls/library/libmbedcrypto.a
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/$*.d -MT $(INTERMEDIATEDIR)/$*.o -o $(INTERMEDIATEDIR)/$*.o -c $<

# Build c files. This is just for sqlite, so we don't bother with dependencies for it.
# SQLITE_MAX_MMAP_SIZE is set to 16TB.
$(INTERMEDIATEDIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(BEDROCK_OPTIM_COMPILE_FLAG) -fPIC $(AMALGAMATION_FLAGS) -o $@ -c $<

# Bring in the dependency files. This will cause them to be created if necessary. This is skipped if we're cleaning, as
# they'll just get deleted anyway.
ifneq ($(MAKECMDGOALS),clean)
-include $(LIBBEDROCKDEP)
-include $(STUFFDEP)
-include $(TESTDEP)
-include $(CLUSTERTESTDEP)
-include $(BEDROCKDEP)
-include $(TESTPLUGINTDEP)
endif
