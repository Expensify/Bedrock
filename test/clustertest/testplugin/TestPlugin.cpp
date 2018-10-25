#include "TestPlugin.h"

extern "C" void BEDROCK_PLUGIN_REGISTER_TESTPLUGIN() {
    // Register the global instance
    new BedrockPlugin_TestPlugin();
}

BedrockPlugin_TestPlugin::BedrockPlugin_TestPlugin() :
  _server(nullptr)
{ }

void BedrockPlugin_TestPlugin::initialize(const SData& args, BedrockServer& server) {
    if (httpsManagers.empty()) {
        httpsManagers.push_back(&httpsManager);
    }
    _server = &server;
}

bool BedrockPlugin_TestPlugin::preventAttach() {
    return shouldPreventAttach;
}

bool BedrockPlugin_TestPlugin::peekCommand(SQLite& db, BedrockCommand& command) {
    if (command.request.calc("PeekSleep")) {
        usleep(command.request.calc("PeekSleep") * 1000);
    }
    // Always blacklist on userID.
    command.crashIdentifyingValues.insert("userID");
    // This should never exist when calling peek.
    SASSERT(!command.httpsRequests.size());
    if (SStartsWith(command.request.methodLine,"testcommand")) {
        if (!command.request["response"].empty()) {
            command.response.methodLine = command.request["response"];
        } else {
            command.response.methodLine = "200 OK";
        }
        command.response.content = "this is a test response";
        return true;
    } else if (SStartsWith(command.request.methodLine, "sendrequest")) {
        if (_server->getState() != SQLiteNode::MASTERING && _server->getState() != SQLiteNode::STANDINGDOWN) {
            // Only start HTTPS requests on master, otherwise, we'll escalate.
            return false;
        }
        int requestCount = 1;
        if (command.request.isSet("httpsRequestCount")) {
            requestCount = max(command.request.calc("httpsRequestCount"), 1);
        }
        for (int i = 0; i < requestCount; i++) {
            SData request("GET / HTTP/1.1");
            request["Host"] = "www.google.com";
            command.httpsRequests.push_back(httpsManager.send("https://www.google.com/", request));
        }
        return false; // Not complete.
    } else if (SStartsWith(command.request.methodLine, "slowquery")) {
        int size = 100000000;
        int count = 1;
        if (command.request.isSet("size")) {
            size = SToInt(command.request["size"]);
        }
        if (command.request.isSet("count")) {
            count = SToInt(command.request["count"]);
        }
        for (int i = 0; i < count; i++) {
            string query = "WITH RECURSIVE cnt(x) AS ( SELECT 1 UNION ALL SELECT x+1 FROM cnt LIMIT " + SQ(size) + ") SELECT MAX(x) FROM cnt;";
            SQResult result;
            db.read(query, result);
        }
        return true;
    } else if (SStartsWith(command.request.methodLine, "httpstimeout")) {
        // This command doesn't actually make the connection for 35 seconds, allowing us to use it to test what happens
        // when there's a blocking command and master needs to stand down, to verify the timeout for that works.
        // It *does* eventually connect and return, so that we can also verify that the leftover command gets cleaned
        // up correctly on the former master.
        SData request("GET / HTTP/1.1");
        request["Host"] = "www.google.com";
        auto transaction = httpsManager.httpsDontSend("https://www.google.com/", request);
        command.httpsRequests.push_back(transaction);
        thread([transaction, request](){
            SINFO("Sleeping 35 seconds for httpstimeout");
            sleep(35);
            SINFO("Done Sleeping 35 seconds for httpstimeout");
            transaction->s->send(request.serialize());
        }).detach();
    } else if (SStartsWith(command.request.methodLine, "dieinpeek")) {
        throw 1;
    } else if (SStartsWith(command.request.methodLine, "generatesegfaultpeek")) {
        int* i = 0;
        int x = *i;
        command.response["invalid"] = to_string(x);
    } else if (SStartsWith(command.request.methodLine, "generateassertpeek")) {
        SASSERT(0);
        command.response["invalid"] = "nope";
    } else if (SStartsWith(command.request.methodLine, "preventattach")) {
        // We do all of this work in a thread because plugins don't poll in detached
        // mode, so the tester will send this command to the plugin, then detach BedrockServer,
        // then the tester will try to attach, sleep, then try again.
        thread([this](){
            // Have this plugin block attaching
            shouldPreventAttach = true;

            // Wait for the tester to try to attach
            sleep(5);

            // Reset so the tester can attach this time.
            shouldPreventAttach = false;
        }).detach();
        return true;
    }

    return false;
}

bool BedrockPlugin_TestPlugin::processCommand(SQLite& db, BedrockCommand& command) {
    if (command.request.calc("ProcessSleep")) {
        usleep(command.request.calc("ProcessSleep") * 1000);
    }
    if (SStartsWith(command.request.methodLine, "sendrequest")) {
        // Assert if we got here with no requests.
        if (command.httpsRequests.empty()) {
            SINFO ("Calling process with no https request: " << command.request.methodLine);
            SASSERT(false);
        }
        // If any of our responses were bad, we want to know that.
        bool allGoodResponses = true;
        for (auto& request : command.httpsRequests) {
            // If we're calling `process` on a command with a https request, it had better be finished.
            SASSERT(request->response);

            // Concatenate all of our responses into the body.
            command.response.content += to_string(request->response) + "\n";

            // If our response is an error, store that.
            if (request->response >= 400) {
                allGoodResponses = false;
            }
        }
        
        // Update the response method line.
        if (!command.request["response"].empty() && allGoodResponses) {
            command.response.methodLine = command.request["response"];
        } else {
            command.response.methodLine = "200 OK";
        }

        // Update the DB so we can test conflicts.
        SQResult result;
        db.read("SELECT MAX(id) FROM test", result);
        SASSERT(result.size());
        int nextID = SToInt(result[0][0]) + 1;
        SASSERT(db.write("INSERT INTO TEST VALUES(" + SQ(nextID) + ", " + SQ(command.request["value"]) + ");"));

        // Done.
        return true;
    }
    else if (SStartsWith(command.request.methodLine, "idcollision")) {
        SQResult result;
        db.read("SELECT MAX(id) FROM test", result);
        SASSERT(result.size());
        int nextID = SToInt(result[0][0]) + 1;
        SASSERT(db.write("INSERT INTO TEST VALUES(" + SQ(nextID) + ", " + SQ(command.request["value"]) + ");"));

        if (!command.request["response"].empty()) {
            command.response.methodLine = command.request["response"];
        } else {
            command.response.methodLine = "200 OK";
        }

        return true;
    } else if (SStartsWith(command.request.methodLine, "slowprocessquery")) {
        SQResult result;
        db.read("SELECT MAX(id) FROM test", result);
        SASSERT(result.size());
        int nextID = SToInt(result[0][0]) + 1;

        int size = 1;
        int count = 1;
        if (command.request.isSet("size")) {
            size = SToInt(command.request["size"]);
        }
        if (command.request.isSet("count")) {
            count = SToInt(command.request["count"]);
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
            db.read(query, result);
        }
    } else if (SStartsWith(command.request.methodLine, "dieinprocess")) {
        throw 2;
    } else if (SStartsWith(command.request.methodLine, "generatesegfaultprocess")) {
        int* i = 0;
        int x = *i;
        command.response["invalid"] = to_string(x);
    } else if (SStartsWith(command.request.methodLine, "ineffectiveUpdate")) {
        // This command does nothing on purpose so that we can run it in 10x mode and verify it replicates OK.
        return true;
    }
    return false;
}

void BedrockPlugin_TestPlugin::upgradeDatabase(SQLite& db) {
    bool ignore;
    SASSERT(db.verifyTable("dbupgrade", "CREATE TABLE dbupgrade ( "
                                        "id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                        "value )", ignore));
}


bool TestHTTPSMananager::_onRecv(Transaction* transaction) {
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

TestHTTPSMananager::~TestHTTPSMananager() {
}

TestHTTPSMananager::Transaction* TestHTTPSMananager::send(const string& url, const SData& request) {
    return _httpsSend(url, request);
}

SHTTPSManager::Transaction* TestHTTPSMananager::httpsDontSend(const string& url, const SData& request) {
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
