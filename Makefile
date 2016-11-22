# # Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++
endif

ifndef CC
	CC = gcc
endif

# We use our project directory as a search path so we don't need "../../../.." all over the place.
PROJECT = $(shell pwd)

# Extract our version information from git.
VERSION = $(shell git log -1 | head -n 1 | cut -d ' ' -f 2)

# Turn on C++11.
CXXFLAGS =-g -std=gnu++11 -DSVERSION="\"$(VERSION)\"" -Wall
CXXFLAGS +=-I$(PROJECT) -I$(PROJECT)/mbedtls/include

# This works because 'PRODUCTION' is passed as a command-line param, and so is ignored here when set that way.
PRODUCTION=false
ifeq ($(PRODUCTION),true)
# Extra build stuff
LDFLAGS +=-Wl,-Bsymbolic-functions -Wl,-z,relro
CFLAGS +=-O2 -fstack-protector --param=ssp-buffer-size=4 -Wformat -Wformat-security
else
CFLAGS +=-O0
endif

# We'll stick object and dependency files in here so we don't need to look at them.
INTERMEDIATEDIR = .build

# These targets aren't actual files.
.PHONY: all test clean

# This sets our default by being the first target, and also sets `all` in case someone types `make all`.
all: bedrock test
test: test/test

# Set up our precompiled header. This makes building *way* faster (roughly twice as fast).
# Including it here causes it to be generated.
libstuff/libstuff.h.gch libstuff/libstuff.d: libstuff/libstuff.h
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
	rm -rf libstuff/libstuff.d
	rm -rf libstuff/libstuff.h.gch
	cd mbedtls && make clean

# The mbedtls libraries are all built the same way.
mbedtls/library/libmbedcrypto.a:
	cd mbedtls && make no_test && touch library/libmbedcrypto.a
mbedtls/library/libmbedtls.a:
	cd mbedtls && make no_test && touch library/libmbedtls.a
mbedtls/library/libmbedx509.a:
	cd mbedtls && make no_test && touch library/libmbedx509.a

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

TESTCPP = $(shell find test -name '*.cpp')
TESTOBJ = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
TESTDEP = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# Bring in the dependency files. This will cause them to be created if necessary. This is skipped if we're cleaning, as
# they'll just get deleted anyway.
ifneq ($(MAKECMDGOALS),clean)
-include $(STUFFDEP)
-include $(LIBBEDROCKDEP)
-include $(BEDROCKDEP)
-include $(TESTDEP)
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

# Both of our binaries build in the same way.
bedrock: $(BEDROCKOBJ) $(BINPREREQS)
	$(GXX) -o $@ $(BEDROCKOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)
test/test: $(TESTOBJ) $(BINPREREQS)
	$(GXX) -o $@ $(TESTOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)

# Make dependency files from cpp files, putting them in $INTERMEDIATEDIR.
# This is the same as making the object files, both dependencies and object files are built together. The only
# difference is that here, the fie passed as `-MF` is the target, and the output file is a modified version of that,
# where for the object file rule, the reverse is true.
$(INTERMEDIATEDIR)/%.d: %.cpp libstuff/libstuff.d
	@mkdir -p $(dir $@)
	$(GXX) $(CFLAGS) $(CXXFLAGS) -MMD -MF $@ -include libstuff/libstuff.h -o $(INTERMEDIATEDIR)/$*.o -c $<

# .o files depend on .d files to prevent simultaneous jobs from trying to create both.
$(INTERMEDIATEDIR)/%.o: %.cpp $(INTERMEDIATEDIR)/%.d libstuff/libstuff.d
	@mkdir -p $(dir $@)
	$(GXX) $(CFLAGS) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/$*.d -include libstuff/libstuff.h -o $@ -c $<

# Build c files. This is basically just for sqlite, so we don't bother with dependencies for it.
$(INTERMEDIATEDIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -o $@ -c $<
