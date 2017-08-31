#include "../libstuff/sqlite3.h"
#include <string>
#include <list>
#include <vector>
#include <thread>
#include <iostream>
#include <fstream>
#include <atomic>
#include <random>
#include <sys/time.h>
#include <unistd.h>
using namespace std;

// Overall test settings
static int global_numQueries = 10000;
static int global_bMmap = 0;
static const char* global_dbFilename = "test.db";
static int global_cacheSize = -1000000;
static int global_querySize = 10;

// Data about the database
static uint64_t global_dbRows = 0;

// Returns the current time down to the microsecond
uint64_t STimeNow() {
    timeval time;
    gettimeofday(&time, 0);
    return ((uint64_t)time.tv_sec * 1000000 + (uint64_t)time.tv_usec);
}

void process_mem_usage(double& vm_usage, double& resident_set)
{
    vm_usage     = 0.0;
    resident_set = 0.0;

    // the two fields we want
    unsigned long vsize;
    long rss;
    {
        std::string ignore;
        std::ifstream ifs("/proc/self/stat", std::ios_base::in);
        ifs >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
                >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
                >> ignore >> ignore >> vsize >> rss;
    }

    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}

// This is called by sqlite for each row of the result
int queryCallback(void* data, int columns, char** columnText, char** columnName) {
    list<vector<string>>* results = (list<vector<string>>*)data;
    vector<string> row;
    row.reserve(columns);
    for (int i = 0; i < columns; i++) {
        row.push_back(columnText[i] ? columnText[i] : "NULL");
    }
    results->push_back(move(row));
    return SQLITE_OK;
}

// This runs a test query some number of times, optionally showing progress
void runTestQueries(sqlite3* db, int numQueries, const string& testQuery, bool showProgress) {
    // Run however many queries are requested
    for (int q=0; q<numQueries; q++) {
        // Run the query
        list<vector<string>> results;
        int error = sqlite3_exec(db, testQuery.c_str(), queryCallback, &results, 0);
        if (error != SQLITE_OK) {
            cout << "Error running test query: " << sqlite3_errmsg(db) << ", query: " << testQuery << endl;
        }

        // Optionally show progress
        if (showProgress && (q % (numQueries/10))==0 ) {
            int percent = (int)(((double)q / (double)numQueries) * 100.0);
            cout << percent << "% " << flush;
        }
    }
}

sqlite3* openDatabase() {
    // Open it per the global settings
    sqlite3* db = 0;
    sqlite3_open_v2(global_dbFilename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, 0);
    sqlite3_exec(db, "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA synchronous = OFF;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA count_changes = OFF;", 0, 0, 0);
    char *zSql = sqlite3_mprintf("PRAGMA cache_size(%d)", global_cacheSize);
    sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    sqlite3_wal_autocheckpoint(db, 10000);
    if( global_bMmap ){
      sqlite3_exec(db, "PRAGMA mmap_size = 1024*1024*1024*1024;", 0, 0, 0); // 1TB
    }
    return db;
}

void test(int threadCount, const string& testQuery) {
    // Open a separate database handle for each thread
    cout << "Testing with " << threadCount << " threads: ";
    vector<sqlite3*> dbs(threadCount);
    for (int i = 0; i < threadCount; i++) {
        dbs[i] = openDatabase();
    }

    // We want to do the same number of total queries each test, but split between however
    // many threads we have
    int numQueries = global_numQueries / threadCount; 

    // Run the actual test
    auto start = STimeNow();
    list <thread> threads;
    for (int i = 0; i < threadCount; i++) {
        bool showProgress = (i == (threadCount - 1)); // Only show progress on the last thread
        threads.emplace_back(runTestQueries, dbs[i], numQueries, testQuery, showProgress);
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    auto end = STimeNow();

    // Output the results
    double vm, rss;
    process_mem_usage(vm, rss);
    cout << "Done! (" << ((end - start) / 1000000.0) << " seconds, vm=" << vm << ", rss=" << rss << ")" << endl;

    // Close all the database handles
    for (int i = 0; i < threadCount; i++) {
        sqlite3_close(dbs[i]);
    }
}


int main(int argc, char *argv[]) {
    // Disable memory status tracking as this has a known concurrency problem
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);

    // Process the command line
    int numThreads = -1;
    int maxNumThreads = 16;
    int showStats = 0;
    const char* customQuery = 0;
    for (int i = 1; i < argc; i++) {
        char *z = argv[i];
        if( z[0]=='-' && z[1]=='-' ) z++;
        if (z == string("-numQueries")) {
            global_numQueries = atoi(argv[++i]);
        }else
        if (z == string("-cacheSize")) {
            global_cacheSize = atoi(argv[++i]);
        }else
        if (z == string("-numThreads")) {
            numThreads = atoi(argv[++i]);
        }else
        if (z == string("-querySize")) {
            global_querySize = atoi(argv[++i]);
        }else
        if (z == string("-maxNumThreads")) {
            maxNumThreads = atoi(argv[++i]);
        }else
        if (z == string("-mmap")) {
          global_bMmap = 1;
        }else
        if (z == string("-dbFilename")) {
          global_dbFilename = argv[++i];
        } else
        if (z == string("-customQuery")) {
          customQuery = argv[++i];
        } else
        {
            cerr << "unknown option: " << argv[i] << "\n";
            exit(1);
        }
    }

    // Inspect the existing database with a full table scan, pulling it all into memory
    cout << "Precaching '" << global_dbFilename << "'...";
    sqlite3* db = openDatabase();
    string query = "SELECT COUNT(*), MIN(nonIndexedColumn) FROM perfTest;";
    list<vector<string>> results;
    auto start = STimeNow();
    int error = sqlite3_exec(db, query.c_str(), queryCallback, &results, 0);
    auto end = STimeNow();
    if (error != SQLITE_OK) {
        cout << "Error running query: " << sqlite3_errmsg(db) << ", query: " << query << endl;
    }
    global_dbRows = stoll(results.front()[0]);
    cout << "Done (" << ((end - start) / 1000000) << " seconds, " << global_dbRows << " rows)" << endl;
    sqlite3_close(db);

    // Figure out what query to use
    string testQuery;
    if (customQuery) {
        // Use the query supplied on the command line
        testQuery = customQuery;
    } else {
        // The test dataset is simply two columns filled with RANDOM(), one
        // indexed, one not.  Let's pick a random location from inside the
        // database, and then pick the next 10 rows.
        testQuery = "SELECT COUNT(*), AVG(nonIndexedColumn) FROM "
            "(SELECT nonIndexedColumn FROM perfTest WHERE indexedColumn > RANDOM() LIMIT " + to_string(global_querySize) + ");";
    }
    cout << "Testing: " << testQuery << endl;

    // Run the test for however many configurations were requested
    if( numThreads<0 ){
      // Ramp up to the test desired test size
      int threads = 1;
      while (threads <= maxNumThreads) {
        test(threads, testQuery);
        threads *= 2;
      }

      // Now ramp back down to the original to make sure it matches the first timings
      while (threads >= 2) {
        threads /= 2;
        test(threads, testQuery);
      }
    }else{
      test(numThreads, testQuery);
    }
}
