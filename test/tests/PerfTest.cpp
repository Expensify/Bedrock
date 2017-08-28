#include <test/lib/BedrockTester.h>

struct PerfTest : tpunit::TestFixture {
    PerfTest()
        : tpunit::TestFixture("Perf",
                              BEFORE_CLASS(PerfTest::setup),
                              TEST(PerfTest::prepare),
                              TEST(PerfTest::manySelects),
                              //TEST(PerfTest::insertSerialBatches),
                              //TEST(PerfTest::clearTable),
                              //TEST(PerfTest::insertRandomParallel),
                              //TEST(PerfTest::indexPerf),
                              //TEST(PerfTest::nonIndexPerf),
                              AFTER_CLASS(PerfTest::tearDown)) { }

    BedrockTester* tester;

    // How many rows to insert.
    // A million rows is about 33mb.
    int64_t NUM_ROWS = 1000000ll * 24ll; // Approximately 1gb.

    string dbFile = "";

    mutex insertMutex;
    list<string> outstandingQueries;

    int64_t rowCount;
    list<int64_t> keys;

    void setup() {
        int threads = 8;

        // If the user specified a number of threads, use that.
        if (BedrockTester::globalArgs && BedrockTester::globalArgs->isSet("-brthreads")) {
            threads = SToInt64((*BedrockTester::globalArgs)["-brthreads"]);
        }

        // If the user specified a DB file, use that.
        if (BedrockTester::globalArgs && BedrockTester::globalArgs->isSet("-dbfile")) {
            dbFile = (*BedrockTester::globalArgs)["-dbfile"];
        }

        if (BedrockTester::globalArgs && BedrockTester::globalArgs->isSet("-createDB")) {

            // Create the database table.
            tester = new BedrockTester(dbFile, "", {
                "PRAGMA legacy_file_format = OFF",
                "CREATE TABLE perfTest(indexedColumn INT PRIMARY KEY, nonIndexedColumn INT);"
            }, {{"-readThreads", to_string(threads)}});

            tester->deleteOnClose = false;
            delete tester;

            // Insert shittons of data.
            // DB size, in GB.
            int64_t DBSize = SToInt64((*BedrockTester::globalArgs)["-createDB"]);
            DBSize = max(DBSize, (int64_t)1);

            NUM_ROWS *= DBSize;

            sqlite3* _db;
            sqlite3_initialize();
            
            sqlite3_open_v2(dbFile.c_str(), &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
            sqlite3_exec(_db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
            sqlite3_exec(_db, "PRAGMA synchronous = OFF;", 0, 0, 0);
            sqlite3_exec(_db, "PRAGMA count_changes = OFF;", 0, 0, 0);
            sqlite3_exec(_db, "PRAGMA cache_size = -1000000;", 0, 0, 0);
            sqlite3_wal_autocheckpoint(_db, 10000);

            int64_t currentRows = 0;
            int lastPercent = 0;

            while (currentRows < NUM_ROWS) {

                // If we've run out of queries, generate some new ones.
                if (outstandingQueries.empty()) {
                    list<thread> threads;
                    for (int i = 0; i < 16; i++) {
                        threads.emplace_back([this](){
                            for (int j = 0; j < 100; j++) {
                                string query = "INSERT INTO perfTest values";
                                int rowsInQuery = 0;
                                uint64_t value;
                                while (rowsInQuery < 10000) {
                                    {
                                        SAUTOLOCK(insertMutex);
                                        value = SRandom::rand64() >> 2;
                                    }
                                    string valString = to_string(value);
                                    query += "(" + valString + "," + valString + "), ";
                                    rowsInQuery++;
                                }
                                query = query.substr(0, query.size() - 2);
                                query += ";";

                                SAUTOLOCK(insertMutex);
                                outstandingQueries.push_back(query);
                            }
                        });
                    }
                    for (auto& thread : threads) {
                        thread.join();
                    }
                }

                string& query = outstandingQueries.front();
                int error = sqlite3_exec(_db, query.c_str(), 0, 0, 0);
                if (error != SQLITE_OK) {
                    cout << "Error running insert query: " << sqlite3_errmsg(_db) << ", query: " << query << endl;
                }
                currentRows += 10000;
                outstandingQueries.pop_front();
                // Output progress.
                int percent = (int)(((double)currentRows/(double)NUM_ROWS) * 100.0);
                if (percent > lastPercent) {
                    lastPercent = percent;
                    cout << "Inserted " << lastPercent << "% of " << NUM_ROWS << " rows." << endl;
                }
            }
            SASSERT(!sqlite3_close(_db));
        }

        // Re-create the tester with the existing DB file.
        tester = new BedrockTester(dbFile, "", {}, {{"-readThreads", to_string(threads)}});
        tester->deleteOnClose = false;
        delete tester;
    }

    void prepare() {
        // Re-create the tester with the existing DB file.
        tester = new BedrockTester(dbFile);
        tester->deleteOnClose = false;

        // Start timing.
        auto start = STimeNow();

        SData query("Query");
        query["query"] = "SELECT COUNT(*) FROM perfTest;";
        query["nowhere"] = "true";
        auto result = tester->executeWait(query);

        list<string> rows;
        SParseList(result, rows, '\n');
        rows.pop_front();
        rowCount = SToInt64(rows.front());
        cout << "Total DB rows: " << rowCount << endl;

        // And get a list of possible values.
        query["query"] = "SELECT indexedColumn FROM perfTest WHERE "
            "(indexedColumn % " + SQ(rowCount) + " / 100000) >= 0 "
            "AND "
            "(indexedColumn % " + SQ(rowCount) + " / 100000) < 10;";
        query["nowhere"] = "true";
        result = tester->executeWait(query);
        rows.clear();
        SParseList(result, rows, '\n');
        rows.pop_front();
        cout << "Selected rows: " << rows.size() << endl;

        auto it = rows.begin();
        while (it != rows.end()) {
            keys.emplace_back(SToInt64(*it));
            it++;
        }

        // End Timing.
        auto end = STimeNow();
        cout << "Elapsed " << ((end - start) / 1000000) << " seconds." << endl;
        cout << "Have " << keys.size() << " keys to pick from." << endl;

        delete tester;
    }

    void manySelects() {

        if (BedrockTester::globalArgs && BedrockTester::globalArgs->isSet("-createDB")) {
            return;
        }

        // We're going to perform SELECT_COUNT select statements, that each looks up ROWS_PER_SELECT rows.
        const int SELECT_COUNT = 100000;
        const int ROWS_PER_SELECT = 1000;

        int i = 1;
        int MAX = 16; // or 512.
        while (i <= MAX) {
            cout << "Testing " << SELECT_COUNT << " SELECTS (" << ROWS_PER_SELECT << " rows per SELECT) with "
                 << i << " bedrock threads." << endl;

            // We have a list of batches of queries. Each batch is 5000 queries.
            list<vector<SData>> queryList;
            auto it = keys.begin();
            int selectStatmentCount = 0;
            while (selectStatmentCount < SELECT_COUNT) {
                vector<SData> queries;
                mutex selectMutex;
                list<thread> threads;
                // 25 threads.
                for (int i = 0; i < 25; i++) {
                    threads.emplace_back([i, &it, &selectMutex, &queries, this](){
                        // 200 queries on each thread.
                        for (int j = 0; j < 200; j++) {
                            uint64_t key[ROWS_PER_SELECT] = {0};
                            {
                                SAUTOLOCK(selectMutex);
                                for (int k = 0; k < ROWS_PER_SELECT; k++) {
                                    if (it != keys.end()) {
                                        it = keys.begin();
                                    }
                                    key[k] = *it;
                                    it++;
                                }
                            }

                            // Build the list of "in" values to select from.
                            string in = "IN (";
                            for (int k = 0; k < ROWS_PER_SELECT - 1; k++) {
                                in += SQ(key[k]) + ", ";
                            }
                            in += SQ(key[ROWS_PER_SELECT - 1]) + ")";
                            string query = "SELECT * FROM perfTest WHERE indexedColumn " + in + ";";
                            {
                                SData q("Query");
                                q["query"] = query;
                                SAUTOLOCK(selectMutex);
                                queries.push_back(q);
                            }
                        }
                    });
                }
                for (auto& thread : threads) {
                    thread.join();
                }
                queryList.push_back(queries);

                // We've added another queries.
                selectStatmentCount += 5000;
            }

            cout << selectStatmentCount << " queries prepared." << endl;

            // Re-create the tester with the existing DB file.
            cout << "Starting server." << endl;
            tester = new BedrockTester(dbFile, "", {}, {{"-readThreads", to_string(i)}});
            tester->deleteOnClose = false;
            cout << "Bedrock running." << endl;

            // Start timing.
            auto start = STimeNow();

            list<vector<pair<string,SData>>> results;
            auto it2 = queryList.begin();
            while (it2 != queryList.end()) {
                auto& qlist = *it2;
                results.emplace_back(tester->executeWaitMultipleData(qlist, i * 2));
                it2++;
            }

            // End Timing.
            auto end = STimeNow();
            cout << "Elapsed " << ((end - start) / 1000000) << " seconds." << endl;

            // Parse out the results.
            uint64_t queryTime = 0;
            uint64_t queryCount = 0;
            uint64_t processTime = 0;
            uint64_t processCount = 0;
            for (auto& result: results) {
                for (auto& pair: result) {
                    auto& data = pair.second;
                    if (data.isSet("readTimeUS")) {
                        queryTime += SToUInt64(data["readTimeUS"]);
                        queryCount++;
                    }
                    if (data.isSet("processTimeUS")) {
                        processTime += SToUInt64(data["processTimeUS"]);
                        processCount++;
                    }
                }
            }
            cout << "Total time spent in read queries: " << (queryTime / 1000000) << " seconds (count: " << queryCount << ")." << endl; 
            cout << "Total time spent in process queries: " << (processTime / 1000000) << " seconds (count: " << processCount << ")." << endl; 

            cout << "Shutting down." << endl;
            delete tester;
            tester = nullptr;
            i*= 2;
        }
    }

    void tearDown() {
        if (tester) {
            delete tester;
        }
    }
} __PerfTest;

