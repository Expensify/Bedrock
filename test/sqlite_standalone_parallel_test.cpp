#include "../libstuff/sqlite3.h"
//#include "../libstuff/sqlite3ext.h"
#include <string>
#include <list>
#include <vector>
#include <thread>
#include <iostream>
#include <atomic>
#include <mutex>
#include <random>
#include <sys/time.h>
using namespace std;

#if 0
Compile with:
gcc-6  -g -c -O2 -Wno-unused-but-set-variable -DSQLITE_ENABLE_STAT4 -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT ../libstuff/sqlite3.c;
g++-6 -g -c -O2 sqlite_standalone_parallel_test.cpp;
g++-6 -g -O2 -o sqlitetest sqlite_standalone_parallel_test.o sqlite3.o -ldl -lpthread;
#endif

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
    sqlite3_open_v2("testdb.db", &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
    sqlite3_exec(_db, "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA synchronous = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA count_changes = OFF;", 0, 0, 0);
    sqlite3_exec(_db, "PRAGMA cache_size = -1000000;", 0, 0, 0);
    sqlite3_wal_autocheckpoint(_db, 10000);

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
        "(indexedColumn % " + to_string(rows) + " / 100000) >= 0 "
        "AND "
        "(indexedColumn % " + to_string(rows) + " / 100000) < 10;";

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
    sqlite3_open_v2("testdb.db", &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
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
        // Split up 100,000 queries for threadCount threads.
        atomic<int> keyIndex(0);
        threads.emplace_back(generate_queries, ref(keyIndex), (100000 / threadCount), ref(queries[i]));
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

void run_queries(sqlite3* db, vector<string>& queries, bool logThread) {
    int percent = 0;
    int count = 0;
    for (auto& query : queries) {
        int error = sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);
        error = sqlite3_exec(db, query.c_str(), 0, 0, 0);
        if (error != SQLITE_OK) {
            cout << "Error running insert query: " << sqlite3_errmsg(db) << ", query: " << query << endl;
        }
        error = sqlite3_exec(db, "COMMIT", 0, 0, 0);

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

void test (int threadCount) {
    cout << endl << "Testing with " << threadCount << " threads." << endl;
    vector<vector<string>> queries;
    generate_query_wrapper(threadCount, queries);

    // We've got our queries, set up our DBs.
    cout << "Initializing DBs." << endl;
    vector<sqlite3*> dbs(threadCount);
    for (int i = 0; i < threadCount; i++) {
        sqlite3_open_v2("testdb.db", &dbs[i], SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
        sqlite3_exec(dbs[i], "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA journal_mode = WAL;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA synchronous = OFF;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA count_changes = OFF;", 0, 0, 0);
        sqlite3_exec(dbs[i], "PRAGMA cache_size = -1000000;", 0, 0, 0);
        sqlite3_wal_autocheckpoint(dbs[i], 10000);
    }
    cout << "Done, running queries in " << threadCount << " threads, " << queries.front().size() << " queries each." << endl;

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
    cout << "Finished in " << ((end - start) / 1000000) << " seconds." << endl;

    for (int i = 0; i < threadCount; i++) {
        sqlite3_close(dbs[i]);
    }
    cout << "DBs closed." << endl;
}

int main(int argc, char *argv[]) {
    for (int i = 0; i < argc; i++) {
        if (argv[i] == string("-createDB")) {
            generate_db();
            exit(0);
        }
    }
    preselect();
    int threads = 1;
    while (threads <= 16) {
        test(threads);
        threads *= 2;
    }
}
