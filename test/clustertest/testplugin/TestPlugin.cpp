#include "TestPlugin.h"

mutex BedrockPlugin_TestPlugin::dataLock;
map<string, string> BedrockPlugin_TestPlugin::arbitraryData;

const string BedrockPlugin_TestPlugin::name("TestPlugin");
const string& BedrockPlugin_TestPlugin::getName() const {
    return name;
}

extern "C" BedrockPlugin* BEDROCK_PLUGIN_REGISTER_TESTPLUGIN(BedrockServer& s) {
    return new BedrockPlugin_TestPlugin(s);
}

BedrockPlugin_TestPlugin::BedrockPlugin_TestPlugin(BedrockServer& s) :
BedrockPlugin(s), httpsManager(new TestHTTPSManager(*this))
{
}

BedrockPlugin_TestPlugin::~BedrockPlugin_TestPlugin()
{
}

unique_ptr<BedrockCommand> BedrockPlugin_TestPlugin::getCommand(SQLiteCommand&& baseCommand) {
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

TestPluginCommand::~TestPluginCommand()
{
    if (request.methodLine == "testescalate") {
        string serverState = SQLiteNode::stateName(plugin().server.getState());
        string statString = "Destroying testescalate (" + serverState + ")\n";
        SFileSave(request["tempFile"], SFileLoad(request["tempFile"]) + statString);

        // The intention here is to verify that the destructor on our follower is the last thing that runs, however we
        // check simply that we're not leading, because this should also fail if we end up in some weird state (we
        // don't want the test to pass if our follower is actually `WAITING` or something strange).
        if (serverState != SQLiteNode::stateName(SQLiteNode::LEADING)) {
            string fileContents = SFileLoad(request["tempFile"]);
            SFileDelete(request["tempFile"]);

            // Verifiy this all happened in the right order.
            if (fileContents != "Peeking testescalate (FOLLOWING)\n"
                                "Peeking testescalate (LEADING)\n"
                                "Processing testescalate (LEADING)\n"
                                "Destroying testescalate (LEADING)\n"
                                "Destroying testescalate (FOLLOWING)\n") {
                cout << "Crashing the server on purpose, execution order is wrong: " << endl;
                cout << fileContents;
                SASSERT(false);
            }
        }
    }
}

void TestPluginCommand::reset(BedrockCommand::STAGE stage) {
    if (stage == STAGE::PEEK) {
        // We don't reset `pendingResult`, `urls`, or `chainedHTTPResponseContent` because they preserve state across
        // multiple `repeek` calls.
    }
    BedrockCommand::reset(stage);
};

bool BedrockPlugin_TestPlugin::preventAttach() {
    return shouldPreventAttach;
}

bool TestPluginCommand::peek(SQLite& db) {
    // Always blacklist on userID.
    crashIdentifyingValues.insert("userID");

    // Sleep if requested.
    if (request.calc("PeekSleep")) {
        usleep(request.calc("PeekSleep") * 1000);
    }

    if (SStartsWith(request.methodLine,"testcommand")) {
        if (!request["response"].empty()) {
            response.methodLine = request["response"];
        } else {
            response.methodLine = "200 OK";
        }
        response.content = "this is a test response";
        return true;
    } else if (SStartsWith(request.methodLine, "broadcastwithtimeouts")) {
        // First, send a `broadcastwithtimeouts` which will generate a new command and broadcast that to peers.
        SData subCommand("storeboradcasttimeouts");
        subCommand["processTimeout"] = to_string(5001);
        subCommand["timeout"] = to_string(5002);
        subCommand["not_special"] = "whatever";
        plugin().server.broadcastCommand(subCommand);
        return true;
    } else if (SStartsWith(request.methodLine, "storeboradcasttimeouts")) {
        // This is the command that will be broadcast to peers, it will store some data.
        lock_guard<mutex> lock(plugin().dataLock);
        plugin().arbitraryData["timeout"] = request["timeout"];
        plugin().arbitraryData["processTimeout"] = request["processTimeout"];
        plugin().arbitraryData["commandExecuteTime"] = request["commandExecuteTime"];
        plugin().arbitraryData["not_special"] = request["not_special"];
        return true;
    } else if (SStartsWith(request.methodLine, "getbroadcasttimeouts")) {
        // Finally, the caller can send this command to the peers to make sure they received the correct timeout data.
        lock_guard<mutex> lock(plugin().dataLock);
        response["stored_timeout"] = plugin().arbitraryData["timeout"];
        response["stored_processTimeout"] = plugin().arbitraryData["processTimeout"];
        response["stored_commandExecuteTime"] = plugin().arbitraryData["commandExecuteTime"];
        response["stored_not_special"] = plugin().arbitraryData["not_special"];
        return true;
    } else if (SStartsWith(request.methodLine, "sendrequest")) {
        if (plugin().server.getState() != SQLiteNode::LEADING && plugin().server.getState() != SQLiteNode::STANDINGDOWN) {
            // Only start HTTPS requests on leader, otherwise, we'll escalate.
            return false;
        }
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
            thread([transaction, newRequest](){
                SINFO("Sleeping 35 seconds for httpstimeout");
                sleep(35);
                SINFO("Done Sleeping 35 seconds for httpstimeout");
                transaction->s->send(newRequest.serialize());
            }).detach();
        }
    } else if (SStartsWith(request.methodLine, "exceptioninpeek")) {
        throw 1;
    } else if (SStartsWith(request.methodLine, "generatesegfaultpeek")) {
        int* i = 0;
        int x = *i;
        response["invalid"] = to_string(x);
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
    } else if (request.methodLine == "testescalate") {
        string serverState = SQLiteNode::stateName(plugin().server.getState());
        string statString = "Peeking testescalate (" + serverState + ")\n";
        SFileSave(request["tempFile"], SFileLoad(request["tempFile"]) + statString);
        return false;
    }

    return false;
}

void TestPluginCommand::process(SQLite& db) {
    if (request.calc("ProcessSleep")) {
        usleep(request.calc("ProcessSleep") * 1000);
    }
    if (SStartsWith(request.methodLine, "sendrequest")) {
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
                SINFO ("Calling process with no https request: " << request.methodLine);
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
    }
    else if (SStartsWith(request.methodLine, "idcollision")) {
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
        int* i = 0;
        int x = *i;
        response["invalid"] = to_string(x);
    } else if (SStartsWith(request.methodLine, "ineffectiveUpdate")) {
        // This command does nothing on purpose so that we can run it in 10x mode and verify it replicates OK.
        return;
    } else if (SStartsWith(request.methodLine, "chainedrequest")) {
        // Note that we eventually got to process, though we write nothing to the DB.
        response.content = chainedHTTPResponseContent + "PROCESSED\n";
    } else if (request.methodLine == "testescalate") {
        string serverState = SQLiteNode::stateName(plugin().server.getState());
        string statString = "Processing testescalate (" + serverState + ")\n";
        SFileSave(request["tempFile"], SFileLoad(request["tempFile"]) + statString);
        return;
    }
}

void BedrockPlugin_TestPlugin::upgradeDatabase(SQLite& db) {
    bool ignore;
    SASSERT(db.verifyTable("dbupgrade", "CREATE TABLE dbupgrade ( "
                                        "id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                        "value )", ignore));
    SASSERT(db.verifyTable("test", "CREATE TABLE test (id INTEGER NOT NULL PRIMARY KEY, value TEXT NOT NULL)", ignore));
}


bool TestHTTPSManager::_onRecv(Transaction* transaction) {
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

TestHTTPSManager::~TestHTTPSManager() {
}

TestHTTPSManager::Transaction* TestHTTPSManager::send(const string& url, const SData& request) {
    return _httpsSend(url, request);
}

SHTTPSManager::Transaction* TestHTTPSManager::httpsDontSend(const string& url, const SData& request) {
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
    SX509* x509 = SStartsWith(url, "https://") ? SX509Open(_pem, _srvCrt, _caCrt) : nullptr;
    Socket* s = openSocket(host, x509);
    if (!s) {
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
    SAUTOLOCK(_listMutex);
    _activeTransactionList.push_front(transaction);
    return transaction;
}
