#include "BedrockTester.h"
#include <sys/wait.h>

// Define static vars.
string BedrockTester::defaultDBFile; // Unused, exists for backwards compatibility.
string BedrockTester::defaultServerAddr; // Unused, exists for backwards compatibility.
SData BedrockTester::globalArgs;
mutex BedrockTester::_testersMutex;
PortMap BedrockTester::ports;
set<BedrockTester*> BedrockTester::_testers;
list<string> BedrockTester::locations = {
    "../bedrock",
    "../../bedrock"
};

// Set to 2 or more for duplicated requests.
int BedrockTester::mockRequestMode = 0;

string BedrockTester::getTempFileName(string prefix) {
    string templateStr = "/tmp/" + prefix + "bedrocktest_XXXXXX.db";
    char buffer[templateStr.size() + 1];
    strcpy(buffer, templateStr.c_str());
    int filedes = mkstemps(buffer, 3);
    close(filedes);
    return buffer;
}

string BedrockTester::getServerName() {
    for (auto location : locations) {
        if (SFileExists(location)) {
            return location;
        }
    }
    cout << "Couldn't find bedrock server" << endl;
    exit(1);

    // Won't get hit.
    return "";
}

void BedrockTester::stopAll() {
    lock_guard<decltype(_testersMutex)> lock(_testersMutex);
    for (auto p : _testers) {
        p->stopServer();
    }
}

BedrockTester::BedrockTester(const map<string, string>& args,
                             const list<string>& queries,
                             bool startImmediately,
                             bool keepFilesWhenFinished,
                             uint16_t serverPort,
                             uint16_t nodePort,
                             uint16_t controlPort,
                             bool ownPorts) :
    BedrockTester(0, args, queries, startImmediately, keepFilesWhenFinished, serverPort, nodePort, controlPort, ownPorts)
{ }

BedrockTester::BedrockTester(int threadID, const map<string, string>& args,
                             const list<string>& queries,
                             bool startImmediately,
                             bool keepFilesWhenFinished,
                             uint16_t serverPort,
                             uint16_t nodePort,
                             uint16_t controlPort,
                             bool ownPorts)
  : _keepFilesWhenFinished(keepFilesWhenFinished),
    _serverPort(ownPorts ? ports.getPort() : serverPort),
    _nodePort(ownPorts ? ports.getPort() : nodePort),
    _controlPort(ownPorts ? ports.getPort() : controlPort),
    _ownPorts(ownPorts)
{
    {
        lock_guard<decltype(_testersMutex)> lock(_testersMutex);
        _testers.insert(this);
    }

    // Set these values from the arguments if provided, or the defaults if not.
    try {
        _dbName = args.at("-db");
    } catch (...) {
        _dbName = getTempFileName();
    }
    try {
        _serverAddr = args.at("-serverHost");
    } catch (...) {
        _serverAddr = "127.0.0.1:" + to_string(_serverPort);
    }

    map <string, string> defaultArgs = {
        {"-db",               _dbName},
        {"-serverHost",       _serverAddr},
        {"-nodeName",         "bedrock_test"},
        {"-nodeHost",         "localhost:" + to_string(_nodePort)},
        {"-controlPort",      "localhost:" + to_string(_controlPort)},
        {"-priority",         "200"},
        {"-plugins",          "db"},
        {"-workerThreads",    "8"},
        {"-mmapSizeGB",       "1"},
        {"-maxJournalSize",   "25000"},
        {"-v",                ""},
        {"-quorumCheckpoint", "50"},
        {"-enableMultiWrite", "true"},
        {"-cacheSize",        "1000"},
    };

    // Set defaults.
    for (auto& row : defaultArgs) {
        _args[row.first] = row.second;
    }

    // And replace with anything specified.
    for (auto& row : args) {
        _args[row.first] = row.second;
    }
    
    _controlAddr = _args["-controlPort"];

    // If the DB file doesn't exist, create it.
    if (!SFileExists(_dbName)) {
        SFileSave(_dbName, "");
    }

    // Run any supplied queries on the DB.
    // We don't use SQLite here, because we specifically want to avoid dealing with journal tables.
    if (queries.size()) {
        sqlite3* _db;
        sqlite3_initialize();
        sqlite3_open_v2(_dbName.c_str(), &_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
        for (string query : queries) {
            int error = sqlite3_exec(_db, query.c_str(), 0, 0, 0);
            if (error) {
                cout << "Init Query: " << query << ", FAILED. Error: " << error << endl;
            }
        }
        SASSERT(!sqlite3_close(_db));
    }
    if (startImmediately) {
        startServer();
    }
}

BedrockTester::~BedrockTester() {
    if (_db) {
        delete _db;
    }
    if (_serverPID) {
        stopServer();
    }
    if (!_keepFilesWhenFinished) {
        SFileExists(_dbName.c_str()) && unlink(_dbName.c_str());
        SFileExists((_dbName + "-shm").c_str()) && unlink((_dbName + "-shm").c_str());
        SFileExists((_dbName + "-wal").c_str()) && unlink((_dbName + "-wal").c_str());
    }
    lock_guard<decltype(_testersMutex)> lock(_testersMutex);
    _testers.erase(this);

    // Release programmatically allocated ports
    if (!_ownPorts) {
        ports.returnPort(_serverPort);
        ports.returnPort(_nodePort);
        ports.returnPort(_controlPort);
    }
}

string BedrockTester::startServer(bool dontWait) {
    string serverName = getServerName();
    int childPID = fork();
    if (childPID == -1) {
        cout << "Fork failed, acting like server died." << endl;
        exit(1);
    }
    if (!childPID) {
        // We are the child!
        list<string> args;
        // First arg is path to file.
        args.push_front(getServerName());
        for (auto& row : _args) {
            args.push_back(row.first);
            if (!row.second.empty()) {
                args.push_back(row.second);
            }
        }

        // Convert our c++ strings to old-school C strings for exec.
        char* cargs[args.size() + 1];
        int count = 0;
        for(string arg : args) {
            char* newstr = (char*)malloc(arg.size() + 1);
            strcpy(newstr, arg.c_str());
            cargs[count] = newstr;
            count++;
        }
        cargs[count] = 0;

        // Make sure the ports we need are free.
        int portsFree = 0;
        portsFree |= ports.waitForPort(getServerPort());
        portsFree |= ports.waitForPort(getNodePort());
        portsFree |= ports.waitForPort(getControlPort());

        if (portsFree) {
            cout << "At least one port wasn't free (of: " << getServerPort() << ", " << getNodePort() << ", "
                 << getControlPort() << ") to start server, things will probably fail." << endl;
        }

        // And then start the new server!
        execvp(serverName.c_str(), cargs);

        // The above line should only ever return if it failed, so let's check for that.
        cout << "Starting bedrock failed: " << serverName;
        for (auto& arg : args) {
            cout << " " << arg;
        }
        cout << endl;
        exit(1);
    } else {
        // We'll kill this later.
        _serverPID = childPID;

        // Wait for the server to start up.
        // TODO: Make this not take so long, particularly in Travis. This probably really requires making the server
        // come up faster, not a change in how we wait for it, though it would be nice if we could do something
        // besides this 100ms polling.
        bool needSocket = true;
        uint64_t startTime = STimeNow();

        while (1) {
            // Give up after a minute. This will fail the remainder of the test, but won't hang indefinitely.
            if (startTime + 60'000'000 < STimeNow()) {
                cout << "startServer(): ran out of time waiting for server to start" << endl;
                break;
            }
            if (needSocket) {
                int socket = 0;
                socket = S_socket(dontWait ? _controlAddr : _serverAddr, true, false, true);
                if (socket == -1) {
                    usleep(100000); // 0.1 seconds.
                    continue;
                }
                ::shutdown(socket, SHUT_RDWR);
                ::close(socket);
                needSocket = false;
            }

            // We've successfully opened a socket, so let's try and send a command.
            SData status("Status");
            auto result = executeWaitMultipleData({status}, 1, dontWait);
            if (result[0].methodLine == "200 OK") {
                return result[0].content;
            }
            // This will happen if the server's not up yet. We'll just try again.
            usleep(100000); // 0.1 seconds.
            continue;
        }
    }
    return "";
}

void BedrockTester::stopServer(int signal) {
    if (_serverPID) {
        kill(_serverPID, signal);
        int status;
        waitpid(_serverPID, &status, 0);
        _serverPID = 0;
    }
}

string BedrockTester::executeWaitVerifyContent(SData request, const string& expectedResult, bool control) {
    auto results = executeWaitMultipleData({request}, 1, control);
    if (results.size() == 0) {
        STHROW("No result.");
    }
    if (!SStartsWith(results[0].methodLine, expectedResult)) {
        STable temp;
        temp["originalMethod"] = results[0].methodLine;
        STHROW("Expected " + expectedResult + ", but got '" + results[0].methodLine + "'.", temp);
    }
    return results[0].content;
}

STable BedrockTester::executeWaitVerifyContentTable(SData request, const string& expectedResult) {
    string result = executeWaitVerifyContent(request, expectedResult);
    return SParseJSONObject(result);
}

vector<SData> BedrockTester::executeWaitMultipleData(vector<SData> requests, int connections, bool control, bool returnOnDisconnect, int* errorCode) {
    // Synchronize dequeuing requests, and saving results.
    recursive_mutex listLock;

    // Our results go here.
    vector<SData> results;
    results.resize(requests.size());

    // This is the next index of `requests` that needs processing.
    int currentIndex = 0;

    // This is the list of threads that we'll use for each connection.
    list <thread> threads;

    //reset the error code.
    if (errorCode) {
        *errorCode = 0;
    }

    // Spawn a thread for each connection.
    for (int i = 0; i < connections; i++) {
        threads.emplace_back([&, i](){
            int socket = 0;

            // This continues until there are no more requests to process.
            bool timedOut = false;
            int timeoutAutoRetries = 0;
            size_t myIndex = 0;
            SData myRequest;
            while (true) {

                // This tries to create a socket to Bedrock on the correct port.
                uint64_t sendStart = STimeNow();
                while (true) {
                    // If there's no socket, create a socket.
                    if (socket <= 0) {
                        socket = S_socket((control ? _controlAddr : _serverAddr), true, false, true);
                    }

                    // If that failed, we'll continue our main loop and try again.
                    if (socket == -1) {
                        // Return if we've specified to return on failure, or if it's been 20 seconds.
                        bool timeout = sendStart + 20'000'000 < STimeNow();
                        if (returnOnDisconnect || timeout) {
                            if (returnOnDisconnect && errorCode) {
                                *errorCode = 1;
                            } else if (errorCode) {
                                *errorCode = 2;
                            }
                            if (timeout) {
                                cout << "executeWaitMultiple(): ran out of time waiting for socket" << endl;
                            }
                            return;
                        }

                        // Otherwise, try again, but wait 1/10th second to avoid spamming too badly.
                        usleep(100'000);
                        continue;
                    }
                    
                    // Socket is successfully created. We can exit this loop.
                    break;
                }

                // If we timed out, reuse the last request.
                if (timedOut && timeoutAutoRetries--) {
                    // reuse last request.
                    cout << "Timed out a request, auto-retrying. Might work." << endl;
                    cout << myRequest.serialize() << endl;
                } else {
                    // Get a request to work on.
                    SAUTOLOCK(listLock);
                    myIndex = currentIndex;
                    currentIndex++;
                    if (myIndex >= requests.size()) {
                        // No more requests to process.
                        break;
                    } else {
                        myRequest = requests[myIndex];
                    }

                    // Reset this for the next request that might need it.
                    timeoutAutoRetries = 0;
                }
                timedOut = false;

                // See if we need to send mock requests.
                size_t count = 1;
                if (mockRequestMode > 1) {
                    count = mockRequestMode;
                }
                
                // Send all of the requests, including mocked ones.
                for (size_t mockCount = 0; mockCount < count; mockCount++) {
                    // For every request but the first one, mark it mocked.
                    if (mockCount) {
                        myRequest["mockRequest"] = "true";
                    }

                    // Send until there's nothing left in the buffer.
                    string sendBuffer = myRequest.serialize();
                    while (sendBuffer.size()) {
                        bool result = S_sendconsume(socket, sendBuffer);
                        if (!result) {
                            cout << "Failed to send! Probably disconnected. Should we reconnect?" << endl;
                            ::shutdown(socket, SHUT_RDWR);
                            ::close(socket);
                            socket = -1;
                            if (returnOnDisconnect) {
                                if (errorCode) {
                                    *errorCode = 3;
                                }
                                return;
                            }

                            break;
                        }
                    }

                    // Now we wait for the response.
                    string recvBuffer = "";
                    string methodLine, content;
                    STable headers;
                    int count = 0;
                    uint64_t recvStart = STimeNow();
                    while (!SParseHTTP(recvBuffer.c_str(), recvBuffer.size(), methodLine, headers, content)) {
                        // Poll the socket, so we get a timeout.
                        pollfd readSock;
                        readSock.fd = socket;
                        readSock.events = POLLIN | POLLHUP;
                        readSock.revents = 0;

                        // wait for a second...
                        poll(&readSock, 1, 1000);
                        count++;
                        if (readSock.revents & POLLIN) {
                            bool result = S_recvappend(socket, recvBuffer);
                            if (!result) {
                                ::shutdown(socket, SHUT_RDWR);
                                ::close(socket);
                                socket = -1;
                                if (errorCode) {
                                    *errorCode = 4;
                                }
                                if (returnOnDisconnect) {
                                    return;
                                }
                                break;
                            }
                        } else if (readSock.revents & POLLHUP) {
                            cout << "Failure in readSock.revents & POLLHUP" << endl;
                            ::shutdown(socket, SHUT_RDWR);
                            ::close(socket);
                            socket = -1;
                            if (errorCode) {
                                *errorCode = 5;
                            }
                            if (returnOnDisconnect) {
                                return;
                            }
                            break;
                        } else {
                            // If it's been over 60s, give up.
                            if (recvStart + 60'000'000 < STimeNow()) {
                                timedOut = true;
                                break;
                            }
                        }
                    }

                    // Lock to avoid log lines writing over each other.
                    {
                        SAUTOLOCK(listLock);
                        if (timedOut && !timeoutAutoRetries) {
                            SData responseData = myRequest;
                            responseData.nameValueMap = headers;
                            responseData.methodLine = "000 Timeout";
                            responseData.content = content;
                            if (!mockCount) {
                                results[myIndex] = move(responseData);
                            }
                            ::shutdown(socket, SHUT_RDWR);
                            ::close(socket);
                            socket = 0;
                            break;
                        } else if (!timedOut) {
                            // Ok, done, let's lock again and insert this in the results.
                            SData responseData;
                            responseData.nameValueMap = headers;
                            responseData.methodLine = methodLine;
                            responseData.content = content;

                            if (!mockCount) {
                                results[myIndex] = move(responseData);
                            }

                            if (headers["Connection"] == "close") {
                                ::shutdown(socket, SHUT_RDWR);
                                ::close(socket);
                                socket = 0;
                                break;
                            }
                        }
                    }
                }
            }

            // Close our socket if it's not already an error code.
            if (socket != -1) {
                ::shutdown(socket, SHUT_RDWR);
                ::close(socket);
            }
        });
    }

    // Wait for our threads to finish.
    for (thread& t : threads) {
        t.join();
    }

    // All done!
    return results;
}

SQLite& BedrockTester::getSQLiteDB()
{
    if (!_db) {
        _db = new SQLite(_dbName, 1000000, 0, 3000000, -1, 0);
    }
    return *_db;
}

string BedrockTester::readDB(const string& query)
{
    return getSQLiteDB().read(query);
}

bool BedrockTester::readDB(const string& query, SQResult& result)
{
    return getSQLiteDB().read(query, result);
}

int BedrockTester::getServerPort() {
    return _serverPort;
}

int BedrockTester::getNodePort() {
    return _nodePort;
}

int BedrockTester::getControlPort() {
    return _controlPort;
}

bool BedrockTester::waitForState(string state, uint64_t timeoutUS, bool control) {
    return waitForStates({state}, timeoutUS, control);
}

bool BedrockTester::waitForStates(set<string> states, uint64_t timeoutUS, bool control)
{
    STable json;
    uint64_t start = STimeNow();
    while (STimeNow() < start + timeoutUS) {
        try {
            json = SParseJSONObject(executeWaitVerifyContent(SData("Status"), "200", control));
            auto it = states.find(json["state"]);
            if (it != states.end()) {
                return true;
            }
            // It's still not there, let it try again.
        } catch (...) {
            // Doesn't do anything, we'll fall through to the sleep and try again.
        }
        usleep(100'000);
    }

    // we timed out, let's get some debugging info
    cout << "waitForStates() timed out at " << STimeNow() << " waiting for states " << SComposeList(states) << ". Started waiting at " << start << " Most recent status: " << SComposeJSONObject(json) << endl;

    return false;
}

STable BedrockTester::getStatus(bool control) {
    // FYI: sometimes the status command doesn't return with every key/value expected
    return SParseJSONObject(executeWaitVerifyContent(SData("Status"), "200", control));
}

string BedrockTester::getStatusTerm(string term, bool control) {
    // FYI: sometimes the status command doesn't return with every key/value expected
    // Use with caution
    return getStatus(control)[term];
}

bool BedrockTester::waitForStatusTerm(string term, string testValue, uint64_t timeoutUS, bool control){
    uint64_t start = STimeNow();
    while (STimeNow() < start + timeoutUS) {
        try {
            string result = getStatusTerm(term, control);

            // if the value matches, return, otherwise wait
            if (result == testValue) {
                return true;
            }
        } catch (...) {
            // Doesn't do anything, we'll fall through to the sleep and try again.
        }
        usleep(100'000);
    }
    return false;
}

bool BedrockTester::waitForCommit(int minCommitCount, int retries, bool control){
    int commitCount = SToInt64(getStatusTerm("commitCount", control));
    int i = 0;

    // check commitCount up to "retries" times with a 1s sleep between calls
    while (commitCount < minCommitCount && i < retries) {
        sleep(1);
        commitCount = SToInt64(getStatusTerm("commitCount", control));
        i++;
    }
    return commitCount >= minCommitCount;
}

