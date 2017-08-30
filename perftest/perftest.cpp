#include "../libstuff/sqlite3.h"
#include <string>
#include <list>
#include <vector>
#include <thread>
#include <iostream>
#include <atomic>
#include <random>
#include <sys/time.h>
#include <unistd.h>
using namespace std;

// Number of queries to run per test
static int global_nQuery = 10000;
static int global_bMmap = 0;
static int global_separate = 0;
static const char *global_vfs = 0;
static sqlite3_mutex *global_mutex = 0;

#if 0
Compile with:
gcc-6  -g -c -O2 -Wno-unused-but-set-variable -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT ../libstuff/sqlite3.c;
g++-6 -g -c -O2 sqlite_standalone_parallel_test.cpp;
g++-6 -g -O2 -o sqlitetest sqlite_standalone_parallel_test.o sqlite3.o -ldl -lpthread;
#endif

// If this is set to true, we'll fake select queries.
bool allNOOP = false;        /* All threads are no-op */
bool allButOneNOOP = false;  /* All threads but one are no-op */

// This will store known-good primary keys in the DB.
vector<uint64_t> dbKeys;

mt19937_64 _generator = mt19937_64(random_device()());
uniform_int_distribution<uint64_t> _distribution64 = uniform_int_distribution<uint64_t>();
uint64_t rand64() {
    return _distribution64(_generator);
}

uint64_t STimeNow() {
    timeval time;
    gettimeofday(&time, 0);
    return ((uint64_t)time.tv_sec * 1000000 + (uint64_t)time.tv_usec);
}

int queryCallback(void* data, int columns, char** columnText, char** columnName) {
    list<list<string>>* results = (list<list<string>>*)data;
    list<string> row;
    for (int i = 0; i < columns; i++) {
        row.push_back(columnText[i]);
    }
    results->push_back(move(row));
    return SQLITE_OK;
}

void preselect() {
    sqlite3* _db;
    sqlite3_initialize();
    sqlite3_open_v2("testdb.db", &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, global_vfs);
    sqlite3_exec(_db, "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA synchronous = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA count_changes = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA cache_size = -1000000;", 0, 0, 0);
    sqlite3_wal_autocheckpoint(_db, 10000);
    if( global_bMmap ){
      sqlite3_exec(_db, "PRAGMA mmap_size = 2000000000;", 0, 0, 0);
    }

    // Start timing.
    auto start = STimeNow();

    string query = "SELECT COUNT(*) FROM perfTest;";
    list<list<string>> results;
    int error = sqlite3_exec(_db, query.c_str(), queryCallback, &results, 0);
    if (error != SQLITE_OK) {
        cout << "Error running query: " << sqlite3_errmsg(_db) << ", query: " << query << endl;
    }

    uint64_t rows = stoll(results.front().front());
    cout << "DB has " << rows << " rows." << endl;

    // Select a million keys.
    query = "SELECT indexedColumn FROM perfTest WHERE "
        "(indexedColumn % " + to_string(rows) + " / " + to_string(global_nQuery) + ") >= 0 "
        "AND "
        "(indexedColumn % " + to_string(rows) + " / " + to_string(global_nQuery) + ") < 10;";

    results.clear();
    error = sqlite3_exec(_db, query.c_str(), queryCallback, &results, 0);
    if (error != SQLITE_OK) {
        cout << "Error running query: " << sqlite3_errmsg(_db) << ", query: " << query << endl;
    }

    dbKeys.resize(results.size());
    int index = 0;
    for (auto& row : results) {
        dbKeys[index] = stoll(row.front());
        index++;
    }
    results.clear();

    // End Timing.
    auto end = STimeNow();
    cout << "Selected " << dbKeys.size() << " keys in " << ((end - start) / 1000000) << " seconds." << endl;
    sqlite3_close(_db);
}

void generate_db() {
    cout << "Creating 1gb db 'testdb.db'." << endl;
    sqlite3* _db;
    sqlite3_initialize();
    sqlite3_open_v2("testdb.db", &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, global_vfs);
    sqlite3_exec(_db, "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "CREATE TABLE perfTest(indexedColumn INT PRIMARY KEY, nonIndexedColumn INT);", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA synchronous = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA count_changes = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA cache_size = -1000000;", 0, 0, 0);
    sqlite3_wal_autocheckpoint(_db, 10000);

    const int NUM_KEYS = 2400;
    cout << "Allocating " << NUM_KEYS << " random keys." << endl;
    uint64_t random[NUM_KEYS];
    for (int i = 0; i < NUM_KEYS; i++) {
        random[i] = rand64() >> 2;
    }
    cout << "Done." << endl;

    // For each key, generate a query with 10000 rows.
    for (int i = 0; i < NUM_KEYS; i++) {
        string query = "INSERT INTO perfTest values";
        int rowsInQuery = 0;
        uint64_t value = random[i];
        while (rowsInQuery < 10000) {
            // Add a semi-random value each time to create the rest of the values.
            string valString = to_string(value + (rowsInQuery * 76423));
            query += "(" + valString + "," + valString + "), ";
            rowsInQuery++;
        }
        query = query.substr(0, query.size() - 2);
        query += ";";

        if (i % 50 == 0) {
            cout << "Executing query " << i << " of " << NUM_KEYS << "." << endl;
        }
        int error = sqlite3_exec(_db, query.c_str(), 0, 0, 0);
        if (error != SQLITE_OK) {
            cout << "Error running insert query: " << sqlite3_errmsg(_db) << ", query: " << query << endl;
        }
    }
    sqlite3_close(_db);
};

void generate_queries(atomic<int>& keyIndex, int count, vector<string>& vec) {
    const int ROWS_PER_SELECT = 1000;
    vec.clear();
    for (int i = 0; i < count; i++) {
        string in = "IN (";
        for (int j = 0; j < ROWS_PER_SELECT - 1; j++) {
            int k = keyIndex.fetch_add(1) % dbKeys.size();
            in += to_string(dbKeys[k]) + ", ";
        }
        int k = keyIndex.fetch_add(1) % dbKeys.size();
        in += to_string(dbKeys[k]) + ")";
        string query = "SELECT * FROM perfTest WHERE indexedColumn " + in + ";";
        vec.push_back(query);
    }
};

void generate_query_wrapper(int threadCount, vector<vector<string>>& queries) {
    // Start timing.
    auto start = STimeNow();

    list <thread> threads;
    queries.resize(threadCount, {});
    for (int i = 0; i < threadCount; i++) {
        // Split up the global_nQuery queries for threadCount threads.
        atomic<int> keyIndex(0);
        threads.emplace_back(generate_queries, ref(keyIndex), (global_nQuery / threadCount), ref(queries[i]));
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    int totalQueries = 0;
    for (auto& row : queries) {
        totalQueries += row.size();
    }

    // End Timing.
    auto end = STimeNow();
    cout << "Generated " << totalQueries << " queries in " << ((end - start) / 1000000) << " seconds." << endl;
}

/*
** Display a single line of status using 64-bit values.
*/
static void displayStatLine(
  const char *zLabel,             /* Label for this one line */
  const char *zFormat,            /* Format for the result */
  int iStatusCtrl                 /* Which status to display */
){
  sqlite3_int64 iCur = -1;
  sqlite3_int64 iHiwtr = -1;
  int i, nPercent;
  char zLine[200];
  sqlite3_status64(iStatusCtrl, &iCur, &iHiwtr, 0);
  for(i=0, nPercent=0; zFormat[i]; i++){
    if( zFormat[i]=='%' ) nPercent++;
  }
  if( nPercent>1 ){
    sqlite3_snprintf(sizeof(zLine), zLine, zFormat, iCur, iHiwtr);
  }else{
    sqlite3_snprintf(sizeof(zLine), zLine, zFormat, iHiwtr);
  }
  printf("%-36s %s\n", zLabel, zLine);
}

/*
** Display memory stats.
*/
static int display_stats(sqlite3 *db){
  int iCur;
  int iHiwtr;

  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_USED,
                    &iCur, &iHiwtr, 0);
  printf("Lookaside Slots Used:                %d (max %d)\n",
          iCur, iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_HIT,
                    &iCur, &iHiwtr, 0);
  printf("Successful lookaside attempts:       %d\n",
          iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE,
                    &iCur, &iHiwtr, 0);
  printf("Lookaside failures due to size:      %d\n",
          iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL,
                    &iCur, &iHiwtr, 0);
  printf("Lookaside failures due to OOM:       %d\n",
          iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &iCur, &iHiwtr, 0);
  printf("Pager Heap Usage:                    %d bytes\n",
          iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_HIT, &iCur, &iHiwtr, 1);
  printf("Page cache hits:                     %d\n", iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_MISS, &iCur, &iHiwtr, 1);
  printf("Page cache misses:                   %d\n", iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_WRITE, &iCur, &iHiwtr, 1);
  printf("Page cache writes:                   %d\n", iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, &iCur, &iHiwtr, 0);
  printf("Schema Heap Usage:                   %d bytes\n",
          iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_STMT_USED, &iCur, &iHiwtr, 0);
  printf("Statement Heap/Lookaside Usage:      %d bytes\n",
          iCur);
  fflush(stdout);
}

void run_queries(sqlite3* db, vector<string>& queries, bool logThread) {
    int percent = 0;
    int count = 0;
    bool doNothing = allNOOP || (allButOneNOOP && !logThread);

    for (auto& query : queries) {
        if (!doNothing) {
            int error = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);
            if (error != SQLITE_OK) {
                 cout << "Error running BEGIN: " << sqlite3_errmsg(db) << endl;
            }
            error = sqlite3_exec(db, query.c_str(), 0, 0, 0);
            if (error != SQLITE_OK) {
                cout << "Error running insert query: " << sqlite3_errmsg(db) << ", query: " << query << endl;
            }
            error = sqlite3_exec(db, "COMMIT", 0, 0, 0);
            if (error != SQLITE_OK) {
                cout << "Error running BEGIN: " << sqlite3_errmsg(db) << endl;
            }
        } else {
            string x = "";
            for(int ii=0; ii<350; ii++){
                x += query;
            }
        }

        if (logThread) {
            count++;
            int currentPercent = (int)(((double)count / (double)queries.size()) * 100.0);
            if (currentPercent != percent) {
                percent = currentPercent;
                cout << percent << "% " << flush;
                if (percent % 20 == 0) {
                    cout << endl;
                }
            }
        }
    }

}

void test (int threadCount, int showStats, int cacheSize) {
    cout << endl << "Testing with " << threadCount << " threads." << endl;
    vector<vector<string>> queries;
    generate_query_wrapper(threadCount, queries);

    // We've got our queries, set up our DBs.
    cout << "Initializing DBs." << endl;
    vector<sqlite3*> dbs(threadCount);
    for (int i = 0; i < threadCount; i++) {
        char *zDb = 0;
        if( global_separate ){
          zDb = sqlite3_mprintf("testdb.db%d", i);
        }
        sqlite3_open_v2((zDb ? zDb : "testdb.db"), &dbs[i], SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, global_vfs);
        sqlite3_exec(dbs[i], "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA journal_mode = WAL;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA synchronous = OFF;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA count_changes = OFF;", 0, 0, 0);
        char *zSql = sqlite3_mprintf("PRAGMA cache_size(%d)", cacheSize);
        sqlite3_exec(dbs[i], zSql, 0, 0, 0);
        sqlite3_free(zSql);
        sqlite3_wal_autocheckpoint(dbs[i], 10000);
        if( global_bMmap ){
          sqlite3_exec(dbs[i], "PRAGMA mmap_size = 2000000000;", 0, 0, 0);
        }
        sqlite3_free(zDb);
    }
    string fake = allNOOP ? " (all-fake)" : allButOneNOOP ? "(one-real)" : "";
    cout << "Done, running" << fake << " queries in " << threadCount << " threads, " << queries.front().size() << " queries each." << endl;

    // Start timing.
    auto start = STimeNow();

    list <thread> threads;
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back(run_queries, dbs[i], ref(queries[i]), (i == (threadCount - 1)));
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();

    // End timing.
    auto end = STimeNow();
    cout << "Finished in " << ((end - start) / 1000000.0) << " seconds." << endl;

    for (int i = 0; i < threadCount; i++) {
        if (showStats) {
            printf("*************** Thread %d *****************\n", i);
            display_stats(dbs[i]);
        }
        sqlite3_close(dbs[i]);
    }
    if (showStats) {
        printf("*************** Global ********************\n");
        displayStatLine("Memory Used:",
           "%lld (max %lld) bytes", SQLITE_STATUS_MEMORY_USED);
        displayStatLine("Number of Outstanding Allocations:",
           "%lld (max %lld)", SQLITE_STATUS_MALLOC_COUNT);
        displayStatLine("Number of Pcache Overflow Bytes:",
           "%lld (max %lld) bytes", SQLITE_STATUS_PAGECACHE_OVERFLOW);
        displayStatLine("Largest Allocation:",
           "%lld bytes", SQLITE_STATUS_MALLOC_SIZE);
        displayStatLine("Largest Pcache Allocation:",
           "%lld bytes", SQLITE_STATUS_PAGECACHE_SIZE);
    }
    cout << "DBs closed." << endl;
}


int main(int argc, char *argv[]) {
  sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);
  int nThread = -1;
  int mxThread = 16;
  int showStats = 0;
  int cacheSize = -1000000;
    for (int i = 1; i < argc; i++) {
        char *z = argv[i];
        if( z[0]=='-' && z[1]=='-' ) z++;
        if (z == string("-createDB")) {
            generate_db();
            exit(0);
        }else 
        if (z == string("-noop")) {
            allNOOP = true;
        }else
        if (z == string("-stats")) {
            showStats = 1;
            sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 1);
        }else
        if (z == string("-all-but-one-noop")) {
            allButOneNOOP = true;
        }else
        if (z == string("-enable-memstatus")) {
            sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 1);
        }else
        if (z == string("-nquery")) {
            global_nQuery = atoi(argv[++i]);
        }else
        if (z == string("-cache-size")) {
            cacheSize = atoi(argv[++i]);
        }else
        if (z == string("-nthread")) {
            nThread = atoi(argv[++i]);
        }else
        if (z == string("-max-thread")) {
            mxThread = atoi(argv[++i]);
        }else
        if (z == string("-initsz")) {
            sqlite3_config(SQLITE_CONFIG_PAGECACHE,0,0,atoi(argv[++i]));
        }else
        if (z == string("-vfs")) {
            global_vfs = argv[++i];
        }else
        if (z == string("-mmap")) {
          global_bMmap = 1;
        }else
        if (z == string("-separate")) {
          global_separate = 1;
        }else
        {
            cerr << "unknown option: " << argv[i] << "\n";
            exit(1);
        }
    }
    preselect();
    if( nThread<0 ){
      int threads = 1;
      while (threads <= mxThread) {
        test(threads, showStats, cacheSize);
        threads *= 2;
      }
    }else{
      test(nThread, showStats, cacheSize);
    }
}
