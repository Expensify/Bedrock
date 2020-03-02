# Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++-9
endif

ifndef CC
	CC = gcc-9
endif

GIT_REVISION = $(shell git rev-parse --short HEAD)
PROJECT = $(shell git rev-parse --show-toplevel)
INCLUDE = -I$(PROJECT) -I$(PROJECT)/mbedtls/include
CXXFLAGS = -g -std=c++14 -fpic -O2 $(BEDROCK_OPTIM_COMPILE_FLAG) -Wall -Werror -Wformat-security -DGIT_REVISION=$(GIT_REVISION) $(INCLUDE)
LDFLAGS +=-Wl,-Bsymbolic-functions -Wl,-z,relro

# We'll stick object and dependency files in here so we don't need to look at them.
INTERMEDIATEDIR = .build

# These targets aren't actual files.
.PHONY: all test clustertest clean testplugin

# This sets our default by being the first target, and also sets `all` in case someone types `make all`.
all: bedrock test clustertest
test: test/test
clustertest: test/clustertest/clustertest testplugin

testplugin:
	cd test/clustertest/testplugin && $(MAKE)

# Set up our precompiled header. This makes building *way* faster (roughly twice as fast).
# Including it here causes it to be generated.
# Depends on one of our mbedtls files, to make sure the submodule gets pulled and built.
PRECOMPILE_D =libstuff/libstuff.d
PRECOMPILE_INCLUDE =-include libstuff/libstuff.h
libstuff/libstuff.h.gch libstuff/libstuff.d: libstuff/libstuff.h mbedtls/library/libmbedcrypto.a
	$(GXX) $(CXXFLAGS) -MMD -MF libstuff/libstuff.d -MT libstuff/libstuff.h.gch -c libstuff/libstuff.h
ifneq ($(MAKECMDGOALS),clean)
-include  libstuff/libstuff.d
endif

clean:
	rm -rf $(INTERMEDIATEDIR)
	rm -rf libstuff.a
	rm -rf libbedrock.a
	rm -rf bedrock
	rm -rf test/test
	rm -rf test/clustertest/clustertest
	rm -rf libstuff/libstuff.d
	rm -rf libstuff/libstuff.h.gch
	cd mbedtls && $(MAKE) clean
	cd test/clustertest/testplugin && $(MAKE) clean

# The mbedtls libraries are all built the same way.
mbedtls/library/libmbedcrypto.a mbedtls/library/libmbedtls.a mbedtls/library/libmbedx509.a:
	git submodule init
	git submodule update
	cd mbedtls && git checkout -q 04a049bda1ceca48060b57bc4bcf5203ce591421
	cd mbedtls && $(MAKE) no_test && touch library/libmbedcrypto.a && touch library/libmbedtls.a && touch library/libmbedx509.a

# Ok, that's the end of our magic PCH code. The only other mention of it is in the build line where we include it.

# We're going to build a shared library from every CPP file in this directory or it's children.
STUFFCPP = $(shell find libstuff -name '*.cpp')
STUFFC = $(shell find libstuff -name '*.c')
STUFFOBJ = $(STUFFCPP:%.cpp=$(INTERMEDIATEDIR)/%.o) $(STUFFC:%.c=$(INTERMEDIATEDIR)/%.o)
STUFFDEP = $(STUFFCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

LIBBEDROCKCPP = $(shell find * -name '*.cpp' -not -name main.cpp -not -path 'test*' -not -path 'libstuff*')
LIBBEDROCKOBJ = $(LIBBEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
LIBBEDROCKDEP = $(LIBBEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

BEDROCKCPP = main.cpp
BEDROCKOBJ = $(BEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
BEDROCKDEP = $(BEDROCKCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

TESTCPP = $(shell find test -name '*.cpp' -not -path 'test/clustertest*')
TESTOBJ = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
TESTDEP = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

CLUSTERTESTCPP = $(shell find test -name '*.cpp' -not -path 'test/tests*' -not -path "test/main.cpp")
CLUSTERTESTOBJ = $(CLUSTERTESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
CLUSTERTESTDEP = $(CLUSTERTESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# Bring in the dependency files. This will cause them to be created if necessary. This is skipped if we're cleaning, as
# they'll just get deleted anyway.
ifneq ($(MAKECMDGOALS),clean)
-include $(STUFFDEP)
-include $(LIBBEDROCKDEP)
-include $(BEDROCKDEP)
#-include $(TESTDEP)
#-include $(CLUSTERTESTDEP)
endif

# Our static libraries just depend on their object files.
libstuff.a: $(STUFFOBJ)
	ar crv $@ $(STUFFOBJ)
libbedrock.a: $(LIBBEDROCKOBJ)
	ar crv $@ $(LIBBEDROCKOBJ)

# We use the same library paths and required libraries for both binaries.
LIBPATHS =-Lmbedtls/library -L$(PROJECT)
LIBRARIES =-lbedrock -lstuff -ldl -lpcrecpp -lpthread -lmbedtls -lmbedx509 -lmbedcrypto -lz

# The prerequisites for both binaries are the same. We only include one of the mbedtls libs to avoid building three
# times in parallel.
BINPREREQS = libbedrock.a libstuff.a mbedtls/library/libmbedcrypto.a

# All of our binaries build in the same way.
bedrock: $(BEDROCKOBJ) $(BINPREREQS)
	echo $(BEDROCKOBJ)
	$(GXX) -o $@ $(BEDROCKOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)
test/test: $(TESTOBJ) $(BINPREREQS)
	$(GXX) -o $@ $(TESTOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)
test/clustertest/clustertest: $(CLUSTERTESTOBJ) $(BINPREREQS)
	$(GXX) -o $@ $(CLUSTERTESTOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)

# Make dependency files from cpp files, putting them in $INTERMEDIATEDIR.
# This is the same as making the object files, both dependencies and object files are built together. The only
# difference is that here, the fie passed as `-MF` is the target, and the output file is a modified version of that,
# where for the object file rule, the reverse is true.
$(INTERMEDIATEDIR)/%.d: %.cpp $(PRECOMPILE_D)
	@mkdir -p $(dir $@)
	$(GXX) $(CXXFLAGS) -MMD -MF $@ $(PRECOMPILE_INCLUDE) -o $(INTERMEDIATEDIR)/$*.o -c $<

# .o files depend on .d files to prevent simultaneous jobs from trying to create both.
$(INTERMEDIATEDIR)/%.o: %.cpp $(INTERMEDIATEDIR)/%.d
	@mkdir -p $(dir $@)
	$(GXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/$*.d $(PRECOMPILE_INCLUDE) -o $@ -c $<

# Build c files. This is basically just for sqlite, so we don't bother with dependencies for it.
# SQLITE_MAX_MMAP_SIZE is set to 16TB.
$(INTERMEDIATEDIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) -O2 $(BEDROCK_OPTIM_COMPILE_FLAG) -Wno-unused-but-set-variable -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_ENABLE_NOOP_UPDATE -DSQLITE_MUTEX_ALERT_MILLISECONDS=20 -DHAVE_USLEEP=1 -DSQLITE_MAX_MMAP_SIZE=17592186044416ull -DSQLITE_SHARED_MAPPING -DSQLITE_ENABLE_NORMALIZE -o $@ -c $<
