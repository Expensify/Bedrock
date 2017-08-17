#include <test/lib/BedrockTester.h>

struct PerfTest : tpunit::TestFixture {
    PerfTest()
        : tpunit::TestFixture("Perf",
                              BEFORE_CLASS(PerfTest::setup),
                              TEST(PerfTest::insertSerialBatches),
                              //TEST(PerfTest::clearTable),
                              //TEST(PerfTest::insertRandomParallel),
                              TEST(PerfTest::indexPerf),
                              TEST(PerfTest::nonIndexPerf),
                              AFTER_CLASS(PerfTest::tearDown)) { }

    BedrockTester* tester;

    // How many rows to insert.
    // A million rows is about 33mb.
    int64_t NUM_ROWS = 1000000ll * 30ll; // Approximately 1 gb.

    set<int64_t> randomValues1;
    set<int64_t> randomValues2;

    void setup() {
        int threads = 8;
        if (BedrockTester::globalArgs->isSet("-brthreads")) {
            threads = SToInt64((*BedrockTester::globalArgs)["-brthreads"]);
        }
        // Create the database table.
        tester = new BedrockTester("", "", {
            "CREATE TABLE perfTest(indexedColumn INT PRIMARY KEY, nonIndexedColumn INT);"
        }, {{"-readThreads", to_string(threads)}});
    }

    void tearDown() { delete tester; }

    void insertSerialBatches() {
        // Insert rows in batches of 10000.
        int64_t currentRows = 0;
        int lastPercent = 0;
        auto start = STimeNow();
        while (currentRows < NUM_ROWS) {
            int64_t startRows = currentRows;
            string query = "INSERT INTO perfTest values";
            while (currentRows < NUM_ROWS && currentRows < startRows + 10000) {
                query += "(" + to_string(currentRows) + "," + to_string(currentRows) + "), ";
                currentRows++;
            }
            query = query.substr(0, query.size() - 2);
            query += ";";

            // Now we have 10000 value pairs to insert.
            SData command("Query");

            // Turn off multi-write for this query, so that this runs on the sync thread where checkpointing is
            // enabled. We don't want to run the whole test on the wal file.
            command["processOnSyncThread"] = "true";
            command["query"] = query;
            tester->executeWait(command);

            int percent = (int)(((double)currentRows/(double)NUM_ROWS) * 100.0);
            if (percent >= lastPercent + 5) {
                lastPercent = percent;
                cout << "Inserted " << lastPercent << "% of " << NUM_ROWS << " rows." << endl;
            }
        }
        auto end = STimeNow();
        cout << "Inserted (batch) " << NUM_ROWS << " rows in " << ((end - start) / 1000000) << " seconds." << endl;
    }

    void clearTable() {
        SData command("Query");
        command["processOnSyncThread"] = "true";
        command["query"] = "DELETE FROM perfTest;";
        command["nowhere"] = "true";
        tester->executeWait(command);
    }

    void insertRandomParallel() {
        // Create batches of 10,000 commands that will be sent and input in parallel.
        vector<SData> commands;
        int64_t currentRows = 0;
        int lastPercent = 0;
        auto start = STimeNow();
        while (currentRows < NUM_ROWS) {

            while (currentRows < NUM_ROWS && commands.size() < 10000) {
                // Generate some random values to use that we can look up later.
                int64_t candidate1 = 0;
                int64_t candidate2 = (int64_t)(SRandom::rand64() >> 2);
                while (1) {
                    candidate1 = (int64_t)(SRandom::rand64() >> 2);
                    if (randomValues1.find(candidate1) == randomValues1.end()) {
                        break;
                    } else {
                        cout << "Duplicate random generated, retrying." << endl;
                    }
                }
                randomValues1.insert(candidate1);
                randomValues2.insert(candidate2);

                // Generate a command to insert this.
                SData command("Query");
                command["query"] = "INSERT INTO perfTest values(" + SQ(candidate1) + ", " + SQ(candidate2) + ");";
                commands.push_back(command);
                currentRows++;
            }

            // Send 10,000 commands on 500 connections.
            tester->executeWaitMultiple(commands, 500);

            int percent = (int)(((double)currentRows/(double)NUM_ROWS) * 100.0);
            //if (percent >= lastPercent + 5) {
                lastPercent = percent;
                cout << "Inserted " << lastPercent << "% of " << NUM_ROWS << " rows." << endl;
            //}
        }

        auto end = STimeNow();
        cout << "Inserted (parallel) " << NUM_ROWS << " rows in " << ((end - start) / 1000000) << " seconds." << endl;
    }

    void indexPerf() {
        try {
        int64_t TOTAL_QUERIES = 1000000;
        int64_t BATCH_SIZE = 25000;
        int64_t THREADS = 500;

        int64_t totalReadTime = 0;

        cout << "Running " << TOTAL_QUERIES << " total queries in batches of size " << BATCH_SIZE << " using "
             << THREADS << " parallel connections." << endl;

        auto start = STimeNow();
        for (int64_t i = 0; i < TOTAL_QUERIES; i += BATCH_SIZE) {
            cout << "Query set starting at " << i << "." << endl;
            // Make a whole bunch or request objects.
            vector <SData> requests(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; i++) {
                SData q("Query");
                q["query"] = "SELECT indexedColumn, nonIndexedColumn FROM perfTest WHERE indexedColumn = "
                             + SQ(SRandom::rand64() % NUM_ROWS) + ";";
                requests[i] = q;
            }

            // Send them on 250 threads.
            auto result = tester->executeWaitMultipleData(requests, THREADS);

            // Parse the results.
            for (auto i : result) {
                totalReadTime += SToInt64(i.second["readTimeUS"]);
            }
        }
        auto end = STimeNow();
        cout << "Ran " << TOTAL_QUERIES << " rows in " << ((end - start) / 1000000) << " seconds." << endl;
        cout << "Total read time was: " << (totalReadTime / 1000000) << " seconds. Average "
             << (totalReadTime / TOTAL_QUERIES) << "us per query." << endl;
        }
        catch (...) {
            cout << "WTF" << endl;
        }
    }

    void nonIndexPerf() {
        try {
        int64_t TOTAL_QUERIES = 100;
        int64_t BATCH_SIZE = 25;
        int64_t THREADS = 500;

        int64_t totalReadTime = 0;

        cout << "Running " << TOTAL_QUERIES << " total queries in batches of size " << BATCH_SIZE << " using "
             << THREADS << " parallel connections." << endl;

        auto start = STimeNow();
        for (int64_t i = 0; i < TOTAL_QUERIES; i += BATCH_SIZE) {
            cout << "Query set starting at " << i << "." << endl;
            // Make a whole bunch or request objects.
            vector <SData> requests(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; i++) {
                SData q("Query");
                q["query"] = "SELECT indexedColumn, nonIndexedColumn FROM perfTest WHERE nonIndexedColumn = "
                             + SQ(SRandom::rand64() % NUM_ROWS) + ";";
                requests[i] = q;
            }

            // Send them on 250 threads.
            auto result = tester->executeWaitMultipleData(requests, THREADS);

            // Parse the results.
            for (auto i : result) {
                totalReadTime += SToInt64(i.second["readTimeUS"]);
            }
        }
        auto end = STimeNow();
        cout << "Ran " << TOTAL_QUERIES << " rows in " << ((end - start) / 1000000) << " seconds." << endl;
        cout << "Total read time was: " << (totalReadTime / 1000000) << " seconds. Average "
             << (totalReadTime / TOTAL_QUERIES) << "us per query." << endl;
        }
        catch (...) {
            cout << "WTF" << endl;
        }
    }

} __PerfTest;

