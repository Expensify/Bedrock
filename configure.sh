#/bin/bash

# Set up mbedtls.
git submodule init
git submodule update
cd mbedtls
git checkout c49b808ae490f03d665df5faae457f613aa31aaf
cd ..

# Create the binary dir if it doesn't exist already.
BINDIR=bin-`uname`
[ -d $BINDIR ] || mkdir $BINDIR

# Read plugins from the commandline.
PLUGINS=()
INCLUDES=()
for i in "$@"
do
case $i in
    -p=*|--plugin=*)
    PLUGINS+=("${i#*=}")
    shift # past argument=value
    ;;
    -I=*|--include=*)
    INCLUDES+=("${i#*=}")
    shift # past argument=value
    ;;
    *)
            # unknown option
    ;;
esac
done

for PLUGIN in "${PLUGINS[@]}"
do
  echo "Using plugin: ${PLUGIN}"
done

for INCLUDE in "${INCLUDES[@]}"
do
  echo "Using include: ${INCLUDE}"
done

# Build the library.
cd mbedtls
make -j 4 && cp library/*.a ../$BINDIR
cd ..

# Create the Makefile.
cat > Makefile << "EOF"
# Set the compiler, if it's not set by the environment.
ifndef GXX
	GXX = g++
endif

ifndef CC
	CC = gcc
endif

# We use our project directory as a search path so we don't need "../../../.." all over the place.
PROJECT = $(shell pwd)

# Put .d, and .o, and binary files in platform specific dirs.
UNAME = $(shell uname)
DDIR = .d-$(UNAME)
ODIR = .o-$(UNAME)
BINDIR = bin-$(UNAME)

VERSION = $(shell git log -1 | head -n 1 | cut -d ' ' -f 2)
CFLAGS += -DSVERSION="\"$(VERSION)\""

# Adds libstuff to the include path
# **FIXME: How do we get line numbers in backtrace()?  -g doesn't do it.
CFLAGS += -I$(PROJECT) -Ilibstuff -g

CFLAGS += -Imbedtls/include
EOF

for INCLUDE in "${INCLUDES[@]}"
do
cat >> Makefile << EOF
CFLAGS += -I${INCLUDE}
EOF
done

cat >> Makefile << "EOF"
# Enables all warnings
CFLAGS += -Wall

# TODO: Only really need this for one file.
# Enable https://www.sqlite.org/compile.html#enable_stat4
CFLAGS += -DSQLITE_ENABLE_STAT4

# Enable https://www.sqlite.org/json1.html
CFLAGS += -DSQLITE_ENABLE_JSON1

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
LDFLAGS += -Llibstuff -lstuff
LDFLAGS += -L$(BINDIR) -lmbedtls -lmbedx509 -lmbedcrypto

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

# This works because 'PRODUCTION' is passed as a command-line param, and so is ignored here
# when set that way.
PRODUCTION=false
ifeq ($(PRODUCTION),true)
# Extra build stuff
LDFLAGS +=-Wl,-Bsymbolic-functions -Wl,-z,relro
CFLAGS +=-O2 -fstack-protector --param=ssp-buffer-size=4 -Wformat -Wformat-security
else
CFLAGS +=-O0
endif

# None of our named targets are real, we just use them as shortcuts to build real files in different directories.
.PHONY: libstuff sqlitecluster bedrock
.PHONY: clean cleanlibstuff cleansqlitecluster cleanbedrock cleanbinaries

# Define what counts as 'all' and list this at the top so that it gets run if no target is specified.
all: libstuff sqlitecluster bedrock

# Our rule to clean up calls the clean recipe for each target, and then, in addition, it deletes our entire `.d` and
# `.o` directories.
clean: cleanlibstuff cleansqlitecluster cleanbedrock
	rm -rf $(DDIR)
	rm -rf $(ODIR)
	rm -rf $(BINDIR)
	cd mbedtls && make clean
	rm Makefile

# There are several similar targets defined in a row here. For more documentation on what they're doing, scroll down to
# libstuff, as it does basically the same thing as the other targets, but with more commenting.
BEDROCKC = $(shell find . -name "*.c" -not -path "*mbedtls*")
BEDROCKCPP = $(shell find . -name "*.cpp" -not -path "*mbedtls*" -not -path "*sqlitecluster/main.cpp" -not -path "*test/*")
EOF

for PLUGIN in "${PLUGINS[@]}"
do
cat >> Makefile << EOF
BEDROCKC += \$(shell find ${PLUGIN} -name "*.c")
BEDROCKCPP += \$(shell find ${PLUGIN} -name "*.cpp")
EOF
done

cat >> Makefile << "EOF"
BEDROCKOBJ = $(BEDROCKC:%.c=$(ODIR)/current/%.o) $(BEDROCKCPP:%.cpp=$(ODIR)/current/%.o)
BEDROCKDEP = $(BEDROCKC:%.c=$(DDIR)/current/%.d) $(BEDROCKCPP:%.cpp=$(DDIR)/current/%.d)
bedrock: libstuff/libstuff.a $(BEDROCKOBJ)
	$(GXX) -o $@ $(BEDROCKOBJ) $(LDFLAGS)
cleanbedrock:
	rm -rf $(ODIR)/current/bedrock
	rm -rf $(DDIR)/current/bedrock
	rm -rf bedrock

SQLITECLUSTERC = $(shell find sqlitecluster -name "*.c")
SQLITECLUSTERCPP = $(shell find sqlitecluster -name "*.cpp")
SQLITECLUSTEROBJ = $(SQLITECLUSTERC:%.c=$(ODIR)/current/%.o) $(SQLITECLUSTERCPP:%.cpp=$(ODIR)/current/%.o)
SQLITECLUSTERDEP = $(SQLITECLUSTERC:%.c=$(DDIR)/current/%.d) $(SQLITECLUSTERCPP:%.cpp=$(DDIR)/current/%.d)
sqlitecluster: sqlitecluster/sqlitecluster
sqlitecluster/sqlitecluster: libstuff/libstuff.a $(SQLITECLUSTEROBJ)
	$(GXX) -o $@ $(SQLITECLUSTEROBJ) $(LDFLAGS)
cleansqlitecluster:
	rm -rf $(ODIR)/current/sqlitecluster
	rm -rf $(DDIR)/current/sqlitecluster
	rm -rf sqlitecluster/sqlitecluster

################## libstuff ############################################################################################

# This is where we actually build libstuff. It depends on the .o files for all of the c and cpp files in it's directory.
# It *doesn't* just depend on all the .o files, as we need to make sure those are up to date with everything else.

# Find all of our .c files.
STUFFC = $(shell find libstuff -name "*.c")

# And all of our .cpp files.
STUFFCPP = $(shell find libstuff -name "*.cpp")

# and then create a list of all the .o files and .d files that we'll need based on these .c(pp)? files.
# Reference for 'substitution references', i.e., getting '.o' files from a list of .c files.
# http://www.gnu.org/software/make/manual/make.html#Substitution-Refs
STUFFOBJ = $(STUFFC:%.c=$(ODIR)/current/%.o) $(STUFFCPP:%.cpp=$(ODIR)/current/%.o)
STUFFDEP = $(STUFFC:%.c=$(DDIR)/current/%.d) $(STUFFCPP:%.cpp=$(DDIR)/current/%.d)

# Then create the PHONY target that actually depends on our real file.
libstuff: libstuff/libstuff.a

# And the real target for our library.
libstuff/libstuff.a: $(STUFFOBJ)
	rm -f $@
	ar crv $@ $(STUFFOBJ)

# And create our clean recipe. We delete all our dependency files, all our object files, and our library itself.
cleanlibstuff:
	rm -rf $(ODIR)/current/libstuff
	rm -rf $(DDIR)/current/libstuff
	rm -rf libstuff/libstuff.a

# Just delete the binaries, not all the intermediates.
cleanbinaries:
	rm -rf libstuff/libstuff.a
	rm -rf bedrock
	rm -rf sqlitecluster/sqlitecluster

########################################################################################################################

# This recipe creates `.d` files from `.c` or `.cpp` files. These files all get created inside the `.d` directory at our
# source root. This keeps all of these files from polluting our entire workspace.
# TODO: this is inefficient, we can build .o and .d files in one pass by the compiler (composing a list of dependencies)
# is a necessary step in compiling a whole .c(pp)? file).
$(DDIR)/current/%.d: %.cpp
	mkdir -p $(dir $@)
	$(GXX) $(CFLAGS) $(CXXFLAGS) -MT $(patsubst $(DDIR)/current/%.d, $(ODIR)/%.o, $@) -MM $< > $@
$(DDIR)/current/%.d: %.c
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MT $(patsubst $(DDIR)/current/%.d, $(ODIR)/%.o, $@) -MM $< > $@

# Now that we've computed our .d files, we want to include them here, but only if we're not inside of *clean* or
# similar, because in that case we'd genreate them all and then just delete them.
# Reference on automatic prerequisities:
# https://www.gnu.org/software/make/manual/html_node/Automatic-Prerequisites.html
# NOTE: This always generates all the dependencies for everything (i.e., doing `build bedrock`) will still include the
# dependencies for `auth`, so you'll end up generating all those if you haven't already.

# TODO make this check for any of the `clean` targets, not just `clean`.
ifneq ($(MAKECMDGOALS),clean)
-include $(STUFFDEP)
-include $(BEDROCKDEP)
-include $(SQLITECLUSTERDEP)
endif

########################################################################################################################

# This recipe creates `.o` files from `.c` or `.cpp` files. These files all get created inside the `.o` directory at our
# source root. This keeps all of these files from polluting our entire workspace.

$(ODIR)/current/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(GXX) $(CFLAGS) $(CXXFLAGS) -o $@ -c $<

$(ODIR)/current/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -o $@ -c $<
EOF
