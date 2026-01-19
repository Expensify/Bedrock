#include "TestPlugin.h"

#include <dlfcn.h>
#include <iostream>
#include <sys/file.h>
#include <string.h>

#include <libstuff/SQResult.h>
#include <libstuff/SThread.h>

extern int* __pointerToFakeIntArray;

mutex BedrockPlugin_TestPlugin::dataLock;
map<string, string> BedrockPlugin_TestPlugin::arbitraryData;

const string BedrockPlugin_TestPlugin::name("TestPlugin");
const string& BedrockPlugin_TestPlugin::getName() const
{
    return name;
}

extern "C" BedrockPlugin* BEDROCK_PLUGIN_REGISTER_TESTPLUGIN(BedrockServer& s)
{
    return new BedrockPlugin_TestPlugin(s);
}

BedrockPlugin_TestPlugin::BedrockPlugin_TestPlugin(BedrockServer& s) :
    BedrockPlugin(s), httpsManager(new TestHTTPSManager(*this)), _maxID(-1)
{
}

BedrockPlugin_TestPlugin::~BedrockPlugin_TestPlugin()
{
}

bool fileAppend(const string& path, const string& buffer)
{
    // Try to open the file for appending.
    FILE* fp = fopen(path.c_str(), "a");
    if (!fp) {
        return false;
    }

    // Lock, nobody else can write.
    flock(fileno(fp), LOCK_EX);

    // Write.
    size_t numWritten = fwrite(buffer.c_str(), 1, buffer.size(), fp);

    // Done.
    flock(fileno(fp), LOCK_UN);
    fclose(fp);

    // Return whether we wrote the whole thing.
    return numWritten == buffer.size();
}

string fileLockAndLoad(const string& path)
{
    string buffer;
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) {
        return buffer;
    }

    // Lock so nobody writes while we're reading.
    flock(fileno(fp), LOCK_EX);

    // Read as much as we can
    char readBuffer[32 * 1024];
    size_t numRead = 0;
    while ((numRead = fread(readBuffer, 1, sizeof(readBuffer), fp))) {
        // Append to the buffer
        size_t oldSize = buffer.size();
        buffer.resize(oldSize + numRead);
        memcpy(&buffer[oldSize], readBuffer, numRead);
    }

    flock(fileno(fp), LOCK_UN);
    fclose(fp);
    return buffer;
}

unique_ptr<BedrockCommand> BedrockPlugin_TestPlugin::getCommand(SQLiteCommand&& baseCommand)
{
    static set<string> supportedCommands = {
        "testcommand",
        "testescalate",
        "broadcastwithtimeouts",
        "storeboradcasttimeouts",
        "getbroadcasttimeouts",
        "sendrequest",
        "slowquery",
        "httpstimeout",
        "exceptioninpeek",
        "generatesegfaultpeek",
        "generateassertpeek",
        "preventattach",
        "chainedrequest",
        "ineffectiveUpdate",
        "exceptioninprocess",
        "generatesegfaultprocess",
        "idcollision",
        "slowprocessquery",
        "bigquery",
        "prepeekcommand",
        "postprocesscommand",
        "prepeekpostprocesscommand",
        "preparehandler",
        "testquery",
        "testPostProcessTimeout",
        "EscalateSerializedData",
        "ThreadException",
        "httpswait"
    };
    for (auto& cmdName : supportedCommands) {
        if (SStartsWith(baseCommand.request.methodLine, cmdName)) {
            return make_unique<TestPluginCommand>(move(baseCommand), this);
        }
    }
    return unique_ptr<BedrockCommand>(nullptr);
}

TestPluginCommand::TestPluginCommand(SQLiteCommand&& baseCommand, BedrockPlugin_TestPlugin* plugin) :
    BedrockCommand(move(baseCommand), plugin),
    pendingResult(false),
    urls(request["urls"])
{
}

string TestPluginCommand::serializeData() const
{
    if (SStartsWith(request.methodLine, "EscalateSerializedData")) {
        return serializedDataString;
    }
    return "";
}

void TestPluginCommand::deserializeData(const string& data)
{
    if (SStartsWith(request.methodLine, "EscalateSerializedData")) {
        serializedDataString = data;
    }
    if (request.isSet("throw")) {
        STHROW("500 BAD DESERIALIZE");
    }
}

TestPluginCommand::~TestPluginCommand()
{
    if (request.methodLine == "testescalate") {
        string serverState = SQLiteNode::stateName(plugin().server.getState());
        string statString = "Destroying testescalate (" + serverState + ")\n";
        fileAppend(request["tempFile"], statString);

        // The intention here is to verify that the destructor on our follower is the last thing that runs, however we
        // check simply that we're not leading, because this should also fail if we end up in some weird state (we
        // don't want the test to pass if our follower is actually `WAITING` or something strange).
        if (serverState != SQLiteNode::stateName(SQLiteNodeState::LEADING)) {
            SASSERT(escalated);
            string fileContents = fileLockAndLoad(request["tempFile"]);
            SFileDelete(request["tempFile"]);

            // Verify this all happened in the right order. We're running this on the follower, but it's feasible the
            // destructor on the leader hasn't happened yet. We verify everything up to the first destruction.
            if (!SStartsWith(fileContents, "Peeking testescalate (FOLLOWING)\n"
                                           "Peeking testescalate (LEADING)\n"
                                           "Processing testescalate (LEADING)\n"
                                           "Destroying testescalate")) {
                cout << "Crashing the server on purpose, execution order is wrong: " << endl;
                cout << fileContents;
                SASSERT(false);
            }
        }
    }
}

void TestPluginCommand::reset(BedrockCommand::STAGE stage)
{
    if (stage == STAGE::PEEK) {
        // We don't reset `pendingResult`, `urls`, or `chainedHTTPResponseContent` because they preserve state across
        // multiple `repeek` calls.
    }
    BedrockCommand::reset(stage);
};

bool TestPluginCommand::shouldPrePeek()
{
    return request.methodLine == "prepeekcommand" || request.methodLine == "prepeekpostprocesscommand";
}

bool TestPluginCommand::shouldPostProcess()
{
    return set<string>{
        "postprocesscommand",
        "prepeekpostprocesscommand",
        "testPostProcessTimeout",
        "preparehandler",
    }.count(request.methodLine);
}

bool BedrockPlugin_TestPlugin::preventAttach()
{
    return shouldPreventAttach;
}

void TestPluginCommand::prePeek(SQLite& db)
{
    if (request.methodLine == "prepeekcommand" || request.methodLine == "prepeekpostprocesscommand") {
        if (request["shouldThrow"] == "true") {
            STHROW("501 ERROR");
        }
        jsonContent["prePeekInfo"] = "this was returned in prePeekInfo";
    } else {
        STHROW("500 no prePeek defined, shouldPrePeek should be false");
    }
}

bool TestPluginCommand::peek(SQLite& db)
{
    // Always blacklist on userID.
    crashIdentifyingValues.insert("userID");

    // Sleep if requested.
    if (request.calc("PeekSleep")) {
        usleep(request.calc("PeekSleep") * 1000);
    }

    if (SStartsWith(request.methodLine, "testcommand")) {
        if (!request["response"].empty()) {
            response.methodLine = request["response"];
        } else {
            response.methodLine = "200 OK";
        }
        response.content = "this is a test response";
        return true;
    } else if (SStartsWith(request.methodLine, "ThreadException")) {

        // Retuns the thread and the future associated with its completion.
        auto threadpair = SThread([](){
            STHROW("500 THREAD THREW");
        });

        // Wait for the thread to finish.
        threadpair.first.join();

        // See if the thread threw, and if so, rethrow.
        if (request.isSet("rethrow")) {
            threadpair.second.get();
        }

        return true;
    } else if (SStartsWith(request.methodLine, "EscalateSerializedData")) {
        // Only set this if it's blank. The intention is that it will be blank on a follower, but already set by
        // serialize/deserialize on the leader.
        if (serializedDataString.empty()) {
            serializedDataString = _plugin->server.args["-nodeName"] + ":" + _plugin->server.args["-testName"];
        }
        // On followers, this immediately escalates to leader.
        // On leader, it immediately skips to `process`.
        return false;
    } else if (SStartsWith(request.methodLine, "broadcastwithtimeouts")) {
        // First, send a `broadcastwithtimeouts` which will generate a new command and broadcast that to peers.
        SData subCommand("storeboradcasttimeouts");
        subCommand["processTimeout"] = to_string(5001);
        subCommand["timeout"] = to_string(6000);
        subCommand["not_special"] = "whatever";
        plugin().server.broadcastCommand(subCommand);
        return true;
    } else if (SStartsWith(request.methodLine, "storeboradcasttimeouts")) {
        // This is the command that will be broadcast to peers, it will store some data.
        lock_guard<mutex> lock(plugin().dataLock);
        plugin().arbitraryData["timeout"] = request["timeout"];
        plugin().arbitraryData["processTimeout"] = request["processTimeout"];
        plugin().arbitraryData["peekedAt"] = to_string(STimeNow());
        plugin().arbitraryData["not_special"] = request["not_special"];
        return true;
    } else if (SStartsWith(request.methodLine, "getbroadcasttimeouts")) {
        // Finally, the caller can send this command to the peers to make sure they received the correct timeout data.
        lock_guard<mutex> lock(plugin().dataLock);
        response["stored_timeout"] = plugin().arbitraryData["timeout"];
        response["stored_processTimeout"] = plugin().arbitraryData["processTimeout"];
        response["stored_peekedAt"] = plugin().arbitraryData["peekedAt"];
        response["stored_not_special"] = plugin().arbitraryData["not_special"];
        return true;
    } else if (SStartsWith(request.methodLine, "sendrequest")) {
        int requestCount = 1;
        if (request.isSet("httpsRequestCount")) {
            requestCount = max(request.calc("httpsRequestCount"), 1);
        }
        for (int i = 0; i < requestCount; i++) {
            SData newRequest("GET / HTTP/1.1");
            string host = request["Host"];
            if (host.empty()) {
                host = "www.google.com";
            }
            newRequest["Host"] = host;
            httpsRequests.push_back(plugin().httpsManager->send("https://" + host + "/", newRequest));
        }
        return false; // Not complete.
    } else if (SStartsWith(request.methodLine, "slowquery")) {
        int size = 100000000;
        int count = 1;
        if (request.isSet("size")) {
            size = SToInt(request["size"]);
        }
        if (request.isSet("count")) {
            count = SToInt(request["count"]);
        }
        for (int i = 0; i < count; i++) {
            string query = "WITH RECURSIVE cnt(x) AS ( SELECT random() UNION ALL SELECT x+1 FROM cnt LIMIT " + SQ(size) + ") SELECT MAX(x) FROM cnt;";
            SQResult result;
            db.read(query, result);
        }
        return true;
    } else if (SStartsWith(request.methodLine, "idcollision")) {
        usleep(1001); // for TimingTest to not get 0 values.
    } else if (SStartsWith(request.methodLine, "httpstimeout")) {
        // This command doesn't actually make the connection for 35 seconds, allowing us to use it to test what happens
        // when there's a blocking command and leader needs to stand down, to verify the timeout for that works.
        // It *does* eventually connect and return, so that we can also verify that the leftover command gets cleaned
        // up correctly on the former leader.
        SData newRequest("GET / HTTP/1.1");
        newRequest["Host"] = "www.google.com";
        auto transaction = plugin().httpsManager->httpsDontSend("https://www.google.com/", newRequest);
        httpsRequests.push_back(transaction);
        if (request["neversend"].empty()) {
            int waitFor = request.isSet("waitFor") ? request.calc("waitFor") : 35;
            thread([waitFor, transaction, newRequest](){
                SINFO("Sleeping " << waitFor << " seconds for httpstimeout");
                sleep(waitFor);
                SINFO("Done Sleeping " << waitFor << " seconds for httpstimeout");
                transaction->s->send(newRequest.serialize());
            }).detach();
        }
    } else if (SStartsWith(request.methodLine, "exceptioninpeek")) {
        throw 1;
    } else if (SStartsWith(request.methodLine, "generatesegfaultpeek")) {
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            total += __pointerToFakeIntArray[i];
        }
        response["invalid"] = to_string(total);
    } else if (SStartsWith(request.methodLine, "generateassertpeek")) {
        SASSERT(0);
        response["invalid"] = "nope";
    } else if (SStartsWith(request.methodLine, "preventattach")) {
        // We do all of this work in a thread because plugins don't poll in detached
        // mode, so the tester will send this command to the plugin, then detach BedrockServer,
        // then the tester will try to attach, sleep, then try again.
        // The command will be gone by the time the sleep finishes, so pass a pointer to the plugin.
        BedrockPlugin_TestPlugin* pluginPtr = &plugin();
        thread([pluginPtr](){
            // Have this plugin block attaching
            pluginPtr->shouldPreventAttach = true;

            // Wait for the tester to try to attach
            sleep(5);

            // Reset so the tester can attach this time.
            pluginPtr->shouldPreventAttach = false;
        }).detach();
        return true;
    } else if (SStartsWith(request.methodLine, "httpswait")) {
        SData newRequest("GET / HTTP/1.1");
        newRequest["Host"] = "example.com";
        httpsRequests.push_back(plugin().httpsManager->send("https://example.com/", newRequest));
        int responseNow = httpsRequests.back()->response;
        if (responseNow) {
            STHROW("500 Shouldn't have a response yet");
        }
        waitForHTTPSRequests(db);
        responseNow = httpsRequests.back()->response;
        if (responseNow != 200) {
            STHROW("500 expected 200 response");
        }
        response.content = httpsRequests.back()->fullResponse.content;
        return true;
    } else if (SStartsWith(request.methodLine, "chainedrequest")) {
        // Let's see what the user wanted to request.
        if (pendingResult) {
            if (httpsRequests.empty()) {
                STHROW("Pending Result flag set but no requests!");
            }
            // There was a previous request, let's record it's result.
            chainedHTTPResponseContent += httpsRequests.back()->fullRequest["Host"] + ":" + to_string(httpsRequests.back()->response) + "\n";
        }
        list<string> remainingURLs = SParseList(urls);
        if (remainingURLs.size()) {
            SData newRequest("GET / HTTP/1.1");
            string host = remainingURLs.front();
            newRequest["Host"] = host;
            httpsRequests.push_back(plugin().httpsManager->send("https://" + host + "/", newRequest));

            // Indicate there will be a result waiting next time `peek` is called, and that we need to peek again.
            pendingResult = true;
            repeek = true;

            // re-write the URL list for the next iteration.
            remainingURLs.pop_front();
            urls = SComposeList(remainingURLs);
        } else {
            // There are no URLs left.
            repeek = false;

            // But we still want to call `process`. We make this explicit for clarity, even though its the fall-through
            // case
            return false;
        }
    } else if (request.methodLine == "prepeekcommand" || request.methodLine == "postprocesscommand" || request.methodLine == "prepeekpostprocesscommand") {
        // Do something in here that reads from the database and sets a jsonResponse
        jsonContent["peekInfo"] = "this was returned in peekInfo";
        SQResult result;
        db.read("SELECT COUNT(*) FROM test WHERE id = 999999999", result);
        jsonContent["peekCount"] = result[0][0];
        if (request.methodLine == "prepeekcommand") {
            return true;
        } else {
            return false;
        }
    } else if (request.methodLine == "testescalate") {
        string serverState = SQLiteNode::stateName(plugin().server.getState());
        string statString = "Peeking testescalate (" + serverState + ")\n";
        fileAppend(request["tempFile"], statString);
        return false;
    } else if (request.methodLine == "testquery") {
        response["nodeRequestWasExecuted"] = plugin().server.args["-nodeName"];
        if (SStartsWith(request["Query"], "SELECT")) {
            db.read(request["Query"]);
            return true;
        }
        return false;
    }

    return false;
}

void TestPluginCommand::process(SQLite& db)
{
    // If `stateChanged` hasn't finished, we need to wait.
    // This simulates what we want in our internal plugins, because we don't want
    // any command processed until the leader server knows the maxID value.
    // This is really only here in case of a race condition between the sync thread finishing
    // upgradeDatabase and the thread running in `stateChanged`, it is possible the sync thread
    // tries to accept a command on a loop before `stateChanged` queries and gets a value for _maxID.
    // Also note this really doesn't matter in production, because we don't use `upgradeDatabase` this
    // truly only exists for dev and testing to function properly.
    while (plugin()._maxID < 0) {
        SINFO("Waiting for _maxID " << plugin()._maxID);
        usleep(50'000);
    }
    if (request.calc("ProcessSleep")) {
        usleep(request.calc("ProcessSleep") * 1000);
    }
    if (SStartsWith(request.methodLine, "bigquery")) {
        SQResult result;
        db.read("SELECT MAX(id) FROM test", result);
        SASSERT(result.size());
        int nextID = SToInt(result[0][0]) + 1;
        const string value = "THIS IS A TEST STRING WITH EXACTLY 48 CHARACTERS";
        db.write("INSERT INTO TEST VALUES(" + SQ(nextID) + ", " + SQ(value) + ");");
        size_t querySize = 0;
        for (size_t i = 0; i < 2'000'000; i++) {
            string nq = "UPDATE TEST SET VALUE = " + SQ(value) + " WHERE id = " + SQ(nextID) + ";";
            db.write(nq);
            querySize += nq.size();
        }
        response["QuerySize"] = to_string(querySize);
    } else if (SStartsWith(request.methodLine, "EscalateSerializedData")) {
        // We want to return the data that was serialized and escalated, and also our own nodename, to verify it does not match
        // the node that serialized the data.
        response.content = _plugin->server.args["-nodeName"] + ":" + serializedDataString;
    } else if (SStartsWith(request.methodLine, "sendrequest")) {
        // This flag makes us pass through the response we got from the server, rather than returning 200 if every
        // response we got from the server was < 400. I.e., if the server returns 202, or 304, or anything less than
        // 400, we return 200 except when this flag is set.
        if (request.test("passthrough")) {
            response.methodLine = httpsRequests.front()->fullResponse.methodLine;
            if (httpsRequests.front()->response >= 500 && httpsRequests.front()->response <= 503) {
                // Error transaction, couldn't send.
                response.methodLine = "NO_RESPONSE";
            }
            response["Host"] = httpsRequests.front()->fullRequest["Host"];
        } else {
            // Assert if we got here with no requests.
            if (httpsRequests.empty()) {
                SINFO("Calling process with no https request: " << request.methodLine);
                SASSERT(false);
            }
            // If any of our responses were bad, we want to know that.
            bool allGoodResponses = true;
            for (auto& request : httpsRequests) {
                // If we're calling `process` on a command with a https request, it had better be finished.
                SASSERT(request->response);

                // Concatenate all of our responses into the body.
                response.content += to_string(request->response) + "\n";

                // If our response is an error, store that.
                if (request->response >= 400) {
                    allGoodResponses = false;
                }
            }

            // Update the response method line.
            if (!request["response"].empty() && allGoodResponses) {
                response.methodLine = request["response"];
            } else {
                response.methodLine = "200 OK";
            }

            // Update the DB so we can test conflicts.
            SQResult result;
            db.read("SELECT MAX(id) FROM test", result);
            SASSERT(result.size());
            int nextID = SToInt(result[0][0]) + 1;
            SASSERT(db.write("INSERT INTO TEST VALUES(" + SQ(nextID) + ", " + SQ(request["value"]) + ");"));
        }

        // Done.
        return;
    } else if (SStartsWith(request.methodLine, "idcollision")) {
        usleep(1001); // for TimingTest to not get 0 values.
        SQResult result;
        db.read("SELECT MAX(id) FROM test", result);
        SASSERT(result.size());
        int nextID = SToInt(result[0][0]) + 1;
        SASSERT(db.write("INSERT INTO TEST VALUES(" + SQ(nextID) + ", " + SQ(request["value"]) + ");"));

        if (!request["response"].empty()) {
            response.methodLine = request["response"];
        } else {
            response.methodLine = "200 OK";
        }

        return;
    } else if (SStartsWith(request.methodLine, "slowprocessquery")) {
        SQResult result;
        db.read("SELECT MAX(id) FROM test", result);
        SASSERT(result.size());
        int nextID = SToInt(result[0][0]) + 1;

        int size = 1;
        int count = 1;
        if (request.isSet("size")) {
            size = SToInt(request["size"]);
        }
        if (request.isSet("count")) {
            count = SToInt(request["count"]);
        }

        for (int i = 0; i < count; i++) {
            string query = "INSERT INTO test (id, value) VALUES ";
            for (int j = 0; j < size; j++) {
                if (j) {
                    query += ", ";
                }
                query += "(" + to_string(nextID) + ", " + to_string(nextID) + ")";
                nextID++;
            }
            query += ";";
            db.write(query);
        }
    } else if (SStartsWith(request.methodLine, "exceptioninprocess")) {
        throw 2;
    } else if (SStartsWith(request.methodLine, "generatesegfaultprocess")) {
        int total = 0;
        for (int i = 0; i < 1000000; i++) {
            total += __pointerToFakeIntArray[i];
        }
        response["invalid"] = to_string(total);
    } else if (SStartsWith(request.methodLine, "ineffectiveUpdate")) {
        // This command does nothing on purpose so that we can run it in 10x mode and verify it replicates OK.
        return;
    } else if (SStartsWith(request.methodLine, "chainedrequest")) {
        // Note that we eventually got to process, though we write nothing to the DB.
        response.content = chainedHTTPResponseContent + "PROCESSED\n";
    } else if (request.methodLine == "testescalate") {
        string serverState = SQLiteNode::stateName(plugin().server.getState());
        string statString = "Processing testescalate (" + serverState + ")\n";
        fileAppend(request["tempFile"], statString);
        return;
    } else if (request.methodLine == "postprocesscommand") {
        jsonContent["processInfo"] = "this was returned in processInfo";
        db.write("INSERT INTO test (id, value) VALUES (999999999, 'this is a test');");
        return;
    } else if (request.methodLine == "prepeekpostprocesscommand") {
        jsonContent["processInfo"] = "this was returned in processInfo";
        db.write("DELETE FROM test WHERE id = 999999999;");
        return;
    } else if (request.methodLine == "preparehandler") {
        jsonContent["cole"] = "hello";
        db.write("INSERT INTO test (id, value) VALUES (999999888, 'this is a test');");
        return;
    } else if (request.methodLine == "testquery") {
        db.write(request["Query"]);
    } else if (request.methodLine == "httpstimeout") {
        response["processingNode"] = plugin().server.args["-nodeName"];
    }
}

void TestPluginCommand::postProcess(SQLite& db)
{
    if (request.methodLine == "postprocesscommand" || request.methodLine == "prepeekpostprocesscommand") {
        jsonContent["postProcessInfo"] = "this was returned in postProcessInfo";
        SQResult result;
        db.read("SELECT COUNT(*) FROM test WHERE id = 999999999", result);
        jsonContent["postProcessCount"] = result[0][0];
    } else if (request.methodLine == "preparehandler") {
        SQResult result;
        db.read("SELECT id FROM test WHERE value = 'this is written in onPrepareHandler'", result);
        jsonContent["tableID"] = result[0][0];
    } else if (request.methodLine == "testPostProcessTimeout") {
        // It'll timeout eventually.
        while (true) {
            SQResult result;
            db.read("SELECT COUNT(*) FROM test WHERE id = 999999999", result);
        }
    } else {
        STHROW("500 no prePeek defined, shouldPrePeek should be false");
    }
}

bool TestPluginCommand::shouldEnableOnPrepareNotification(const SQLite& db, void(**handler)(SQLite & _db, int64_t tableID))
{
    *handler = BedrockPlugin_TestPlugin::onPrepareHandler;
    return request.methodLine == "preparehandler";
}

void BedrockPlugin_TestPlugin::upgradeDatabase(SQLite& db)
{
    bool ignore;
    SASSERT(db.verifyTable("dbupgrade", "CREATE TABLE dbupgrade ( "
                                        "id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                        "value )", ignore));
    SASSERT(db.verifyTable("test", "CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)", ignore));
}

void BedrockPlugin_TestPlugin::onPrepareHandler(SQLite& db, int64_t tableID)
{
    int64_t tid = 999999999 + tableID;
    db.write("INSERT INTO test (id, value) VALUES (" + to_string(tid) + ", 'this is written in onPrepareHandler');");
}

void BedrockPlugin_TestPlugin::stateChanged(SQLite& db, SQLiteNodeState newState)
{
    // We spin this up in another thread because `stateChanged` is called from the `sync` thread in bedrock
    // so this function cannot do any sort of waiting or it will block the sync thread. By offloading this,
    // we can let the code wait for a condition to be met before it actually runs. In our case, we want to
    // wait until upgradeDatabase has completed so that we can run a query on a table.
    SINFO("Running stateChanged new state " << SQLiteNode::stateName(newState));
    thread([&, newState]() {
        if (newState != SQLiteNodeState::LEADING) {
            SINFO("Returning early stateChanged new state " << SQLiteNode::stateName(newState));
            return;
        }
        SINFO("Sleeping until the server is done upgrading.");
        while (!server.isUpgradeComplete()) {
            usleep(50'000);
        }

        SQResult result;
        db.read("SELECT count(*) FROM dbupgrade", result);
        _maxID = SToInt64(result[0][0]);
        SINFO("Server completed upgrade, _maxID " << _maxID);
    }).detach();
}

bool TestHTTPSManager::_onRecv(Transaction* transaction)
{
    string methodLine = transaction->fullResponse.methodLine;
    transaction->response = 0;
    size_t offset = methodLine.find_first_of(' ', 0);
    offset = methodLine.find_first_not_of(' ', offset);
    if (offset != string::npos) {
        int status = SToInt(methodLine.substr(offset));
        if (status) {
            transaction->response = status;
        }
    }
    if (!transaction->response) {
        transaction->response = 400;
        SWARN("Failed to parse method line from request: " << methodLine);
    }

    return false;
}

TestHTTPSManager::~TestHTTPSManager()
{
}

TestHTTPSManager::Transaction* TestHTTPSManager::send(const string& url, const SData& request)
{
    return _httpsSend(url, request);
}

SHTTPSManager::Transaction* TestHTTPSManager::httpsDontSend(const string& url, const SData& request)
{
    // Open a connection, optionally using SSL (if the URL is HTTPS). If that doesn't work, then just return a
    // completed transaction with an error response.
    string host, path;
    if (!SParseURI(url, host, path)) {
        return _createErrorTransaction();
    }
    if (!SContains(host, ":")) {
        host += ":443";
    }

    // If this is going to be an https transaction, create a certificate and give it to the socket.
    Socket* s = nullptr;
    try {
        s = new Socket(host, SStartsWith(url, "https://"));
    } catch (const SException& e) {
        return _createErrorTransaction();
    }

    // Wrap in a transaction
    Transaction* transaction = new Transaction(*this);
    transaction->s = s;
    transaction->fullRequest = request;

    // Ship it.
    // DOESN'T actually send
    //transaction->s->send(request.serialize());

    // Keep track of the transaction.
    return transaction;
}
