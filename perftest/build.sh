gcc -std=c++11 -g -c -O2 -Wno-unused-but-set-variable -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT ../libstuff/sqlite3.c;
g++ -std=c++11 -g -c -O2 perftest.cpp;
g++ -std=c++11 -g -O2 -o perftest perftest.o sqlite3.o -ldl -lpthread;
