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
#include <numa.h>
using namespace std;

// Overall test settings
static int global_bMmap = 0;
static const char* global_dbFilename = "test.db";
static int global_cacheSize = -1000000;
static int global_querySize = 10;
static int global_numa = 0;
static int64_t global_noopResult = 0;
static int global_testSeconds = 10;
static int global_secondsRemaining = 0;
static int global_linear = 0;
static int global_csv = 0;

// Data about the database
static uint64_t global_dbRows = 0;

// Returns the current time down to the microsecond
uint64_t STimeNow() {
    timeval time;
    gettimeofday(&time, 0);
    return ((uint64_t)time.tv_sec * 1000000 + (uint64_t)time.tv_usec);
}

// Gets the current virtual memory usage and resident set
// From: https://stackoverflow.com/a/19770392
void getMemUsage(double& vm_usage, double& resident_set)
{
    // the two fields we want
    vm_usage     = 0.0;
    resident_set = 0.0;
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

// This just increments a value every second to signal to threads that they should
// record another QPS measure.  This is to avoid having every thread poll the system
// clock at high frequency, as that is an expensive kernel call that triggers a context
// switch and thus will distort the results if done at high frequency.
void countDownTimer()
{
    // Keep incrementing once per second until done
    do {
        sleep(1);
    }
    while (--global_secondsRemaining);
}

// This runs a test query some number of times, optionally showing progress
void runTestQueries(sqlite3* db, int threadNum, vector<uint64_t>* queriesPerSecond, const string& testQuery, bool showProgress) {
    // If we're numa aware, spread the memory across all nodes
    if (global_numa) {
        numa_run_on_node(threadNum%numa_num_task_nodes());
        numa_set_preferred(threadNum%numa_num_task_nodes());
    }

    // Initialize our query counters
    uint64_t numQueriesLastSecond = 0;
    int lastSampleSecond = global_secondsRemaining;

    // Run however many queries are requested
    uint64_t noopResult = 0;
    while (global_secondsRemaining) {
        // See if it's a special query
        if (testQuery=="SLEEP") {
            // Waits 10ms
            usleep(10*1000);
        } else if (testQuery=="NOOP") {
            // Does a simple calculation
            int c=1000000;
            while(c--) { noopResult+=c; }
        } else {
            // Run the query
            list<vector<string>> results;
            int error = sqlite3_exec(db, testQuery.c_str(), queryCallback, &results, 0);
            if (error != SQLITE_OK) {
                cout << "Error running test query: " << sqlite3_errmsg(db) << ", query: " << testQuery << endl;
            }
        }

        // Update timers
        numQueriesLastSecond++;
        if (lastSampleSecond != global_secondsRemaining) {
            // Record QPS
            lastSampleSecond = global_secondsRemaining;
            queriesPerSecond->push_back(numQueriesLastSecond);
            numQueriesLastSecond = 0;

            // Optionally show progress
            if (showProgress && !global_csv) {
                cout << "." << flush;
            }
        }
    }

    // Do this at the end to avoid any kind of contention while the thread is running
    global_noopResult += noopResult;
}

sqlite3* openDatabase(int threadNum) {
    // If we're numa aware, spread the memory across all nodes
    if (global_numa) {
        numa_set_preferred(threadNum%numa_num_task_nodes());
    }

    // Open it per the global settings
    sqlite3* db = 0;
    if (SQLITE_OK != sqlite3_open_v2(global_dbFilename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, "unix-none")) {
        cout << "Error: Can't open '" << global_dbFilename << "'." << endl;
        exit(1);
    }
    sqlite3_exec(db, "PRAGMA locking_mode=EXCLUSIVE;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA synchronous = OFF;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA count_changes = OFF;", 0, 0, 0);
    char *zSql = sqlite3_mprintf("PRAGMA cache_size(%d)", global_cacheSize);
    sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    sqlite3_wal_autocheckpoint(db, 10000);
    if( global_bMmap ){
      sqlite3_exec(db, "PRAGMA mmap_size = 1099511627776;", 0, 0, 0); // 1TB
    } else {
      sqlite3_exec(db, "PRAGMA mmap_size = 0;", 0, 0, 0); // Disable
    }
    return db;
}

void test(int threadCount, const string& testQuery) {
    // Open a separate database handle for each thread
    if (global_csv) {
        cout << threadCount;
    } else {
        cout << "Testing with " << threadCount << " threads: ";
    }
    vector<sqlite3*> dbs(threadCount);
    for (int i = 0; i < threadCount; i++) {
        dbs[i] = openDatabase(i);
    }

    // Start a thread that just harvests QPS every second
    global_secondsRemaining = global_testSeconds;
    thread timerCounter(countDownTimer);
    vector<vector<uint64_t>> queriesPerSecondPerThread;
    queriesPerSecondPerThread.resize(threadCount);

    // Run the actual test
    list <thread> threads;
    auto start = STimeNow();
    for (int i = 0; i < threadCount; i++) {
        bool showProgress = (i == (threadCount - 1)); // Only show progress on the last thread
        threads.emplace_back(runTestQueries, dbs[i], i, &queriesPerSecondPerThread[i], testQuery, showProgress);
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    auto end = STimeNow();

    // Stop the timer and calculate max QPS
    timerCounter.join();
    uint64_t maxQPS = 0;
    for(int sec=1; sec<queriesPerSecondPerThread[0].size(); sec++) { // skip the first second
       uint64_t qps = 0;
       for(int t=0; t<threadCount; t++) {
           qps += queriesPerSecondPerThread[t][sec];
       }
       if (qps>maxQPS) {
           maxQPS = qps;
       }
    }

    // Output the results
    double vm, rss;
    getMemUsage(vm, rss);
    if (global_csv) {
        cout << ", " << maxQPS << ", " << (double)maxQPS/(double)threadCount << endl;
    } else {
        cout << "Done! (" << ((end - start) / 1000000.0) << " seconds, vm=" << vm << ", rss=" << rss << ", maxQPS=" << maxQPS << ", maxQPS/t=" << (double)maxQPS/(double)threadCount << ")" << endl;
    }

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
        if (z == string("-testSeconds")) {
            global_testSeconds = atoi(argv[++i]);
        }else
        if (z == string("-mmap")) {
          global_bMmap = 1;
        }else
        if (z == string("-linear")) {
          global_linear = 1;
        }else
        if (z == string("-csv")) {
          global_csv = 1;
        }else
        if (z == string("-dbFilename")) {
          global_dbFilename = argv[++i];
        } else
        if (z == string("-customQuery")) {
          customQuery = argv[++i];
        } else
        if (z == string("-numa")) {
            // Output numa information about this system
            global_numa = 1;
        } else
        if (z == string("-numastats")) {
            cout << "Enabling NUMA awareness:" << endl;
            cout << "numa_available=" << numa_available() << endl;
            cout << "numa_max_node=" << numa_max_node() << endl;
            cout << "numa_pagesize=" << numa_pagesize() << endl;
            cout << "numa_num_configured_cpus=" << numa_num_configured_cpus() << endl;
            cout << "numa_num_task_cpus=" << numa_num_task_cpus() << endl;
            cout << "numa_num_task_nodes=" << numa_num_task_nodes() << endl;
        } else
        {
            cerr << "unknown option: " << argv[i] << "\n";
            exit(1);
        }
    }

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
    cout << "global_bMmap: " << global_bMmap << endl;
    cout << "global_dbFilename: " << global_dbFilename << endl;
    cout << "global_cacheSize: " << global_cacheSize << endl;
    cout << "global_querySize: " << global_querySize << endl;
    cout << "global_numa: " << global_numa << endl;
    cout << "global_testSeconds: " << global_testSeconds << endl;
    cout << "global_linear: " << global_linear << endl;
    cout << "global_csv: " << global_csv << endl;
    cout << "testQuery: " << testQuery << endl;

    // Run the test for however many configurations were requested
    if (global_csv) {
        cout << "numThreads, maxQPS, maxQPSpT" << endl;
    }
    if( numThreads<0 ){
      // Ramp up to the test desired test size
      int threads = 1;
      while (threads <= maxNumThreads) {
        test(threads, testQuery);
        if (global_linear) {
            threads++;
        } else {
            threads *= 2;
        }
      }

      // Now ramp back down to the original to make sure it matches the first timings
      while (threads >= 2) {
        threads /= 2;
        test(threads, testQuery);
      }
    }else{
      test(numThreads, testQuery);
    }

    // Output the NOOP number, if availble, so compiler doesn't throw it out
    if (global_noopResult) {
        cout << "NOOP=" << global_noopResult << endl;
    }
}
