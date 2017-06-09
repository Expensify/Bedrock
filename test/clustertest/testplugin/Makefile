# # Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++-6
endif

ifndef CC
	CC = gcc-6
endif

# We use our project directory as a search path so we don't need "../../../.." all over the place.
PROJECT = $(shell pwd)/../../..

# Extract our version information from git.
VERSION = $(shell git log -1 | head -n 1 | cut -d ' ' -f 2)

# Turn on C++14.
CXXFLAGS =-g -std=gnu++14 -fpic -DSVERSION="\"$(VERSION)\"" -Wall -Werror -Wno-unused-result
CXXFLAGS +=-I$(PROJECT) -I../../../mbedtls/include

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
all: testplugin.so

clean:
	rm -rf $(INTERMEDIATEDIR)
	rm -rf testplugin.so

# Ok, that's the end of our magic PCH code. The only other mention of it is in the build line where we include it.
# We're going to build a shared library from every CPP file in this directory or it's children.
CPP = TestPlugin.cpp
OBJ = $(CPP:%.cpp=$(INTERMEDIATEDIR)/%.o)
DEP = $(CPP:%.cpp=$(INTERMEDIATEDIR)/%.d)

# Bring in the dependency files. This will cause them to be created if necessary. This is skipped if we're cleaning, as
# they'll just get deleted anyway.
ifneq ($(MAKECMDGOALS),clean)
-include $(DEP)
endif

# The main library depends on all the .o files.
testplugin.so: $(OBJ)
	$(GXX) $(CXXFLAGS) -shared -o $@ $(OBJ)

# Make dependency files from cpp files, putting them in $INTERMEDIATEDIR.
# This is the same as making the object files, both dependencies and object files are built together. The only
# difference is that here, the fie passed as `-MF` is the target, and the output file is a modified version of that,
# where for the object file rule, the reverse is true.
$(INTERMEDIATEDIR)/%.d: %.cpp
	@mkdir -p $(dir $@)
	$(GXX) $(CXXFLAGS) -MMD -MF $@ -o $(INTERMEDIATEDIR)/$*.o -c $<

# Make object files from cpp files, putting them in $INTERMEDIATEDIR.
# This is the same as making the dependency files, both dependencies and object files are built together.
$(INTERMEDIATEDIR)/%.o: %.cpp $(INTERMEDIATEDIR)/%.d
	@mkdir -p $(dir $@)
	$(GXX) $(CXXFLAGS) -MMD -MF $(INTERMEDIATEDIR)/$*.d -o $@ -c $<

