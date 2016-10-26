# Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++
endif

ifndef CC
	CC = gcc
endif

# We use our project directory as a search path so we don't need "../../../.." all over the place.
PROJECT = $(shell pwd)

# Put .d and .o files in platform specific dirs.
UNAME = $(shell uname)
DDIR = .d-$(UNAME)/d
ODIR = .o-$(UNAME)/o
BINDIR = bin-$(UNAME)

VERSION = $(shell git log -1 | head -n 1 | cut -d ' ' -f 2)
CFLAGS += -DSVERSION="\"$(VERSION)\""

# Adds libstuff and mbedtls to the include path
# **FIXME: How do we get line numbers in backtrace()?  -g doesn't do it.
CFLAGS += -I$(PROJECT) -I$(PROJECT)/../ -I$(PROJECT)/../mbedtls/include -g

# Allows inclusion of extra paths from the environment
ifdef EXTRAINCLUDES
CFLAGS += -I$(EXTRAINCLUDES)
endif

# Enables all warnings so we have supaclean code
CFLAGS += -Wall

# Except on OS X, where we turn off one warning, specifically avoiding the
# 'warning: base class '...' is uninitialized when used here to access'
# message that recent llvm throws.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	CFLAGS += -Wno-uninitialized
endif

# Turn on C++11.
CXXFLAGS = -std=gnu++11

# Include our own libstuff.
LDFLAGS += -L../libstuff -lstuff
LDFLAGS += -L../$(BINDIR) -lmbedtls -lmbedx509 -lmbedcrypto

# Leaves the names in the ELF binaries for backtrace()
LDFLAGS	+= -rdynamic

# Needed for sqlite
LDFLAGS += -ldl

# Needed for pcre
LDFLAGS += -lpcrecpp

# Needed for ZLib
LDFLAGS += -lz

# Needed for pthreads.
LDFLAGS += -lpthread

# Allows inclusion of extra paths from the environment
ifdef EXTRALIBS
LDFLAGS += -L$(EXTRALIBS)
endif

# This works because make wont re-set settings passed on the command line, i.e.: `make PRODUCTION=true`
PRODUCTION=false
ifeq ($(PRODUCTION),true)
# Extra build stuff
LDFLAGS +=-Wl,-Bsymbolic-functions -Wl,-z,relro
CFLAGS +=-O2 -fstack-protector --param=ssp-buffer-size=4 -Wformat -Wformat-security
else
CFLAGS +=-O0
endif

# None of our named targets are real, we just use them as shortcuts to build real files in different directories.
.PHONY: test
.PHONY: clean cleantest

# Define what counts as 'all' and list this at the top so that it gets run if no target is specified.
all: test

# Our rule to clean up calls the clean recipe for each target, and then, in addition, it deletes our entire `.d` and
# `.o` directories.
clean: cleantest
	rm -rf $(shell dirname $(DDIR))
	rm -rf $(shell dirname $(ODIR))

TESTC = $(shell find . -name "*.c")
TESTCPP = $(shell find . -name "*.cpp")
TESTCPP += $(shell find ../sqlitecluster -iname "*.cpp" ! -name main.cpp)
TESTOBJ = $(TESTC:%.c=$(ODIR)/%.o) $(TESTCPP:%.cpp=$(ODIR)/%.o)
TESTDEP = $(TESTC:%.c=$(DDIR)/%.d) $(TESTCPP:%.cpp=$(DDIR)/%.d)
test: $(TESTOBJ)
	$(GXX) -o $@ $(TESTOBJ) $(LDFLAGS)
cleantest:
	rm -rf $(ODIR)/test
	rm -rf $(DDIR)/test
	rm -rf test

########################################################################################################################

# This recipe creates `.d` files from `.c` or `.cpp` files. These files all get created inside the `.d` directory at our
# source root. This keeps all of these files from polluting our entire workspace.
# TODO: this is inefficient, we can build .o and .d files in one pass by the compiler (composing a list of dependencies)
# is a necessary step in compiling a whole .c(pp)? file).
$(DDIR)/%.d: %.cpp
	mkdir -p $(dir $@)
	$(GXX) $(CFLAGS) $(CXXFLAGS) -MT $(patsubst $(DDIR)/%.d, $(ODIR)/%.o, $@) -MM $< > $@
$(DDIR)/%.d: %.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MT $(patsubst $(DDIR)/%.d, $(ODIR)/%.o, $@) -MM $< > $@

# Now that we've computed our .d files, we want to include them here, but only if we're not inside of *clean* or
# similar, because in that case we'd genreate them all and then just delete them.
# Reference on automatic prerequisities:
# https://www.gnu.org/software/make/manual/html_node/Automatic-Prerequisites.html
# NOTE: This always generates all the dependencies for everything (i.e., doing `build bedrock`) will still include the
# dependencies for `auth`, so you'll end up generating all those if you haven't already.

# TODO make this check for any of the `clean` targets, not just `clean`.
ifneq ($(MAKECMDGOALS),clean)
-include $(AUTHTESTDEP)
-include $(RECEIPTSDEP)
endif

########################################################################################################################

# This recipe creates `.o` files from `.c` or `.cpp` files. These files all get created inside the `.o` directory at our
# source root. This keeps all of these files from polluting our entire workspace.

$(ODIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(GXX) $(CFLAGS) $(CXXFLAGS) -o $@ -c $<

$(ODIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -o $@ -c $<
