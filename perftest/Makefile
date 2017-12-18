
OPTS=-g -O2
#OPTS=-g

all: perftest

sqlite3.o: ../libstuff/sqlite3.c
	gcc-7 $(OPTS) -c -Wno-unused-but-set-variable -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_THREADSAFE=2 -DSQLITE_MAX_MMAP_SIZE=1099511627776 -DSQLITE_SHARED_MAPPING  ../libstuff/sqlite3.c;

perftest.o: perftest.cpp
	g++-7 -std=c++11 $(OPTS) -c perftest.cpp;

perftest: perftest.o sqlite3.o
	g++-7 -std=c++11 $(OPTS) -o perftest perftest.o sqlite3.o -ldl -lpthread -lnuma;

clean:
	rm -f sqlite3.o
	rm -f perftest.o
	rm -f perftest
