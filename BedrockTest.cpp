#include <libstuff/libstuff.h>
#include "BedrockPlugin.h"
#include "BedrockTest.h"

using Plugin = BedrockPlugin;

BedrockTester::BedrockTester() {
    // Initialize
    server = 0;
}

void BedrockTester::onResponse(const string& host, const SData& request, const SData& response) {
    // Does the request provide a correct response?  If not, just assume 200 OK
    if (request.isSet("BedrockTester.correctResponse")) {
        // Deserialize and compare against that
        SData correctResponse("200 OK"); // Default
        correctResponse.deserialize(request["BedrockTester.correctResponse"]);
        SASSERTEQUALS(SToInt(response.methodLine), SToInt(correctResponse.methodLine));
        for_each(
            correctResponse.nameValueMap.begin(), correctResponse.nameValueMap.end(),
            [&correctResponse](pair<string, string> item) { SASSERTEQUALS(item.second, correctResponse[item.first]); });
        SASSERTEQUALS(response.content, correctResponse.content);
    } else {
        // No correct response given -- just test for 200
        SASSERTEQUALS(SToInt(response.methodLine), 200);
    }
}

void BedrockTester::loop(uint64_t& nextActivity) {
    // Do one iteration of the select loop
    fd_map fdm;
    SMax(preSelect(fdm), server->preSelect(fdm));
    uint64_t now = STimeNow();
    S_poll(fdm, SMax(nextActivity, now) - now);
    nextActivity = STimeNow() + STIME_US_PER_S; // 1s max period
    postSelect(fdm);
    server->postSelect(fdm, nextActivity);
}

void BedrockTester::startServer(SData args) {
// Create the server with these settings for testing
#define SETDEFAULT(_NAME_, _VAL_)                                                                                      \
    do {                                                                                                               \
        if (!args.isSet(_NAME_))                                                                                       \
            args[_NAME_] = _VAL_;                                                                                      \
    } while (false)
    SETDEFAULT("-db", "test.db");
    SETDEFAULT("-serverHost", "localhost:8888");
    SETDEFAULT("-nodeName", "bedrock");
    SETDEFAULT("-nodeHost", "localhost:9999");
    SETDEFAULT("-plugins", "DB");
    SETDEFAULT("-priority", "100");
    SETDEFAULT("-readThreads", "1");
    SETDEFAULT("-cacheSize", "1000000");
    SETDEFAULT("-maxJournalSize", "1000000");

    // Process some flags that main.cpp would normally handle
    if (args.isSet("-clean")) {
        // Remove the database
        SDEBUG("Resetting database");
        string db = args["-db"];
        unlink(db.c_str());
    }

    // Start the server
    SASSERT(!server);
    SClearSignals();
    server = new BedrockServer(args);

    // Start the server and wait up to 3 seconds for the threads to
    // settle.
    uint64_t nextActivity = STimeNow();
    SStopwatch startup(STIME_US_PER_S * 3);
    startup.start();
    while (!startup.ringing() || server->getState() != SQLC_MASTERING)
        loop(nextActivity);
}

void BedrockTester::stopServer() {
    // Stop the server, wait for it to shut down, and then clean.
    SASSERT(server);
    uint64_t nextActivity = STimeNow();
    SSendSignal(SIGTERM);
    while (!server->shutdownComplete())
        loop(nextActivity);
    SDELETE(server);
}

void BedrockTester::waitForResponses() {
    // Keep going so long as there are active connections
    uint64_t nextActivity = STimeNow();
    while (!activeConnectionList.empty())
        loop(nextActivity);
}

void BedrockTester::sendQuery(const string& query, int numToSend, int numActiveConnections) {
    uint64_t nextActivity = STimeNow();
    while (numToSend) {
        if ((int)activeConnectionList.size() < numActiveConnections) {
            // Send it
            SData queryRequest("Query");
            queryRequest["query"] = query;
            sendRequest("localhost:8888", queryRequest);
            numToSend--;
        } else {
            // We want to send more but can't; wait
            loop(nextActivity);
        }
    }

    // All done, wait for everything to be processed
    waitForResponses();
}

// Sends a request, waits for the response, and confirms the response is correct
void BedrockTester::testRequest(const SData& request, const SData& correctResponse) {
    // Record the correct response into the request, and it will be verified
    // inside the onMessage
    SData requestCopy = request;
    requestCopy["BedrockTester.correctResponse"] = correctResponse.serialize();
    sendRequest("localhost:8888", requestCopy);
    waitForResponses();
}

// Run the tests
void BedrockTest(SData& trueArgs) {
    // Create the shared test harness
    SINFO("Starting BedrockTest");
    BedrockTester* tester = new BedrockTester();

    // If no args are set (other than "-test"), set "-all"
    if (trueArgs.nameValueMap.size() == 1) {
        // Do a full test
        trueArgs["-all"] = "true";
    }

    // Start with libstuff itself
    if (trueArgs.isSet("-all")) {
        // Libstuff
        STestLibStuff();
    }

    // What do we test?
    if (trueArgs.isSet("-all")) {
        // Create and run the main tester
        {
            STestTimer test("Testing basic start/stop of 1 read thread node");
            SData args;
            args["-clean"] = "1"; // First time, let's blow away the db
            tester->startServer(args);
            tester->stopServer();
        }

        {
            STestTimer test("Testing 1 read command");
            tester->startServer();
            tester->sendQuery("SELECT 1;");
            tester->stopServer();
        }

        {
            STestTimer test("Testing 1 write command");
            tester->startServer();
            tester->sendQuery("CREATE TABLE foo ( bar INTEGER );");
            tester->stopServer();
        }

        for (int numReadThreads = 1; numReadThreads <= 4; ++numReadThreads) {
            const int NUM_COMMANDS = 10000;
            STestTimer test("Benchmarking " + SToStr(NUM_COMMANDS) + " small read commands on " +
                            SToStr(numReadThreads) + " read threads");
            tester->startServer();
            SStopwatch benchmark;
            tester->sendQuery("SELECT 1;", NUM_COMMANDS);
            uint64_t commandsPerSec = NUM_COMMANDS * 1000 / (benchmark.elapsed() / STIME_US_PER_MS);
            STESTLOG(commandsPerSec << " commands/s");
            tester->stopServer();
        }

        {
            const int NUM_COMMANDS = 10000;
            const int NUM_THREADS = 1;
            STestTimer test("Benchmarking " + SToStr(NUM_COMMANDS) + " small write commands on " + SToStr(NUM_THREADS) +
                            " read threads");
            tester->startServer();
            SStopwatch benchmark;
            tester->sendQuery("INSERT INTO foo VALUES ( RANDOM() );", NUM_COMMANDS);
            uint64_t commandsPerSec = NUM_COMMANDS * 1000 / (benchmark.elapsed() / STIME_US_PER_MS);
            STESTLOG(commandsPerSec << " commands/s");
            tester->stopServer();
        }

        for (int numReadThreads = 1; numReadThreads <= 4; numReadThreads++) {
            const int NUM_COMMANDS = 500;
            STestTimer test("Benchmarking " + SToStr(NUM_COMMANDS) + " large read commands on " +
                            SToStr(numReadThreads) + " read threads");
            SData args;
            args["-readThreads"] = SToStr(numReadThreads);
            tester->startServer(args);
            tester->sendQuery("DELETE FROM foo;");
            tester->sendQuery("INSERT INTO foo VALUES ( 1 );");
            tester->sendQuery("INSERT INTO foo SELECT bar FROM foo;", 18); // 2^18 rows
            SStopwatch benchmark;
            tester->sendQuery("SELECT SUM(bar) FROM foo;", NUM_COMMANDS);
            float commandsPerSec = NUM_COMMANDS * 1000.0 / (float)(benchmark.elapsed() / STIME_US_PER_MS);
            STESTLOG(commandsPerSec << " commands/s");
            tester->stopServer();
        }
    }

    // Test each of the plugins
    for_each(Plugin::g_registeredPluginList->begin(), Plugin::g_registeredPluginList->end(), [&](Plugin* plugin) {
        // Do we need to clean up?
        if (tester->server) {
            // Stop the server to provide a clean test environment for the next plugin
            tester->stopServer();
        }

        // Test the plugin
        list<string> pluginList = SParseList(SToLower(trueArgs["-plugins"]));
        if (trueArgs.isSet("-all") || SContains(pluginList, SToLower(plugin->getName()))) {
            // Yep, test this one
            SINFO("Testing plugin '" << plugin->getName() << "'...");
            plugin->test(tester);
        }
    });

    // All done!
    SDELETE(tester);
    SINFO("Finished BedrockTest");
}
