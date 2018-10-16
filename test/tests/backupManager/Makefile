# # Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++-6
endif

ifndef CC
	CC = gcc-6
endif

# We use our project directory as a search path so we don't need "../../../.." all over the place.
PROJECT = $(shell pwd)/../..

# Extract our version information from git.
VERSION = $(shell git log -1 | head -n 1 | cut -d ' ' -f 2)

# Turn on C++14.
CXXFLAGS =-g -std=gnu++14 -fpic -DSVERSION="\"$(VERSION)\"" -Wall -Werror -Wno-unused-result
CXXFLAGS +=-I$(PROJECT) -I../../../Bedrock -Ilibstuff -I../../../Bedrock/mbedtls/include

# This works because 'PRODUCTION' is passed as a command-line param, and so is ignored here when set that way.
PRODUCTION=false
ifeq ($(PRODUCTION),true)
# Extra build stuff
CXXFLAGS +=-O2 -fstack-protector --param=ssp-buffer-size=4 -Wformat -Wformat-security
else
CXXFLAGS +=-O0
endif

# We'll stick object and dependency files in here so we don't need to look at them.
INTERMEDIATEDIR = .build

# These targets aren't actual files.
.PHONY: all clean

# This sets our default by being the first target, and also sets `all` in case someone types `make all`.
all: expensifyBackupManagerTest

clean:
	rm -rf $(INTERMEDIATEDIR)
	rm -rf expensifyBackupManagerTest

# We're going to build a shared library from every CPP file in this directory or it's children.
TESTCPP = $(shell find * -name "*.cpp")
TESTCPP += ../../../Bedrock/test/lib/tpunit++.cpp
TESTCPP += ../../../Bedrock/test/lib/BedrockTester.cpp
TESTOBJ = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
TESTDEP = $(TESTCPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# Bring in the dependency files. This will cause them to be created if necessary. This is skipped if we're cleaning, as
# they'll just get deleted anyway.
ifneq ($(MAKECMDGOALS),clean)
-include $(TESTDEP)
endif

# The main library depends on all the .o files.
LIBPATHS =-L../../expensifyBackupManager -L../../../Bedrock -L../../../Bedrock/mbedtls/library -L$(PROJECT)
LIBRARIES =-lbedrock -lstuff -ldl -lpcrecpp -lpthread -lmbedtls -lmbedx509 -lmbedcrypto -lz
expensifyBackupManagerTest: $(TESTOBJ)
	$(GXX) -o $@ $(TESTOBJ) $(LIBPATHS) -rdynamic $(LIBRARIES)

# Precompiled headers are currently turned off for OS X.
UNAME_S := $(shell uname -s)
ifneq ($(UNAME_S),Darwin)
PRECOMPILE_INCLUDE =-include ../precompile.h
endif

# Make dependency files from cpp files, putting them in $INTERMEDIATEDIR.
# This is the same as making the object files, both dependencies and object files are built together. The only
# difference is that here, the fie passed as `-MF` is the target, and the output file is a modified version of that,
# where for the object file rule, the reverse is true.
$(INTERMEDIATEDIR)/%.d: %.cpp
	@mkdir -p $(dir $@)
	$(GXX) $(CXXFLAGS) -MMD -MF $@ $(PRECOMPILE_INCLUDE) -o $(INTERMEDIATEDIR)/$*.o -c $<

# Make object files from cpp files, putting them in $INTERMEDIATEDIR.
# This is the same as making the dependency files, both dependencies and object files are built together.
$(INTERMEDIATEDIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(GXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/$*.d $(PRECOMPILE_INCLUDE) -o $@ -c $<
