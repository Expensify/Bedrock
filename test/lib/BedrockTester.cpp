#include "BedrockTester.h"

#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/wait.h>
#include <unistd.h>

#include <libstuff/SData.h>
#include <libstuff/SFastBuffer.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/BedrockTester.h>
#include <test/lib/tpunit++.hpp>

PortMap BedrockTester::ports;
mutex BedrockTester::_testersMutex;
set<BedrockTester*> BedrockTester::_testers;

string BedrockTester::getTempFileName(string prefix) {
    string templateStr = "/tmp/" + prefix + "bedrocktest_XXXXXX.db";
    char buffer[templateStr.size() + 1];
    strcpy(buffer, templateStr.c_str());
    int filedes = mkstemps(buffer, 3);
    close(filedes);
    return buffer;
}

void BedrockTester::stopAll() {
    lock_guard<decltype(_testersMutex)> lock(_testersMutex);
    for (auto p : _testers) {
        p->stopServer(SIGKILL);
    }
}

BedrockTester::BedrockTester(const map<string, string>& args,
                             const list<string>& queries,
                             uint16_t serverPort,
                             uint16_t nodePort,
                             uint16_t controlPort,
                             bool startImmediately,
                             const string& bedrockBinary) :
    _serverPort(serverPort ?: ports.getPort()),
    _nodePort(nodePort ?: ports.getPort()),
    _controlPort(controlPort ?: ports.getPort()),
    _commandPortPrivate(ports.getPort())
{
    {
        lock_guard<decltype(_testersMutex)> lock(_testersMutex);
        _testers.insert(this);
    }

    string currentTestName;
    {
        lock_guard<mutex> lock(tpunit::currentTestNameMutex);
        currentTestName = tpunit::currentTestName;
    }

    if (bedrockBinary.empty()) {
        serverName = "bedrock";
    } else {
        serverName = bedrockBinary;
    }

    map <string, string> defaultArgs = {
        {"-db", getTempFileName()},
        {"-serverHost", "127.0.0.1:" + to_string(_serverPort)},
        {"-nodeName", "bedrock_test"},
        {"-nodeHost", "localhost:" + to_string(_nodePort)},
        {"-controlPort", "localhost:" + to_string(_controlPort)},
        {"-commandPortPrivate", "0.0.0.0:" + to_string(_commandPortPrivate)},
        {"-priority", "200"},
        {"-plugins", "db"},
        {"-workerThreads", "8"},
        {"-mmapSizeGB", "1"},
        {"-maxJournalSize", "25000"},
        {"-v", ""},
        {"-extraExceptionLogging", ""},
        {"-enableMultiWrite", "true"},
        {"-escalateOverHTTP", "true"},
        {"-cacheSize", "1000"},
        {"-parallelReplication", "true"},
        // Currently breaks only in Travis and needs debugging, which has been removed, maybe?
        //{"-logDirectlyToSyslogSocket", ""},
        {"-testName", currentTestName},
    };

    // Set defaults.
    for (auto& row : defaultArgs) {
        _args[row.first] = row.second;
    }

    // And replace with anything specified.
    for (auto& row : args) {
        _args[row.first] = row.second;
    }
    
    // If the DB file doesn't exist, create it.
    if (!SFileExists(_args["-db"])) {
        SFileSave(_args["-db"], "");
    }

    // Run any supplied queries on the DB.
    // We don't use SQLite here, because we specifically want to avoid dealing with journal tables.
    if (queries.size()) {
        sqlite3* db;
        sqlite3_initialize();
        sqlite3_open_v2(_args["-db"].c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
        for (string query : queries) {
            int error = sqlite3_exec(db, query.c_str(), 0, 0, 0);
            if (error) {
                cout << "Init Query: " << query << ", FAILED. Error: " << error << endl;
            }
        }
        SASSERT(!sqlite3_close(db));
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

    SFileExists(_args["-db"].c_str()) && unlink(_args["-db"].c_str());
    SFileExists((_args["-db"] + "-shm").c_str()) && unlink((_args["-db"] + "-shm").c_str());
    SFileExists((_args["-db"] + "-wal").c_str()) && unlink((_args["-db"] + "-wal").c_str());
    SFileExists((_args["-db"] + "-wal2").c_str()) && unlink((_args["-db"] + "-wal2").c_str());

    ports.returnPort(_serverPort);
    ports.returnPort(_nodePort);
    ports.returnPort(_controlPort);

    lock_guard<decltype(_testersMutex)> lock(_testersMutex);
    _testers.erase(this);
}

void BedrockTester::updateArgs(const map<string, string> args) {
    for (auto& row : args) {
        _args[row.first] = row.second;
    }
}

string BedrockTester::getArg(const string& arg) const {
    if (_args.find(arg) != _args.end()) {
        return _args.at(arg);
    }
    return "";
}

string BedrockTester::startServer(bool wait) {
    int childPID = fork();
    if (childPID == -1) {
        cout << "Fork failed, acting like server died." << endl;
        exit(1);
    }
    if (!childPID) {
        // We are the child!
        list<string> args;
        // First arg is path to file.
        args.push_front(serverName);
        for (auto& row : _args) {
            args.push_back(row.first);
            if (!row.second.empty()) {
                args.push_back(row.second);
            }
        }



        // Make sure the ports we need are free.
        int portsFree = 0;
        portsFree |= ports.waitForPort(_serverPort);
        portsFree |= ports.waitForPort(_nodePort);
        portsFree |= ports.waitForPort(_controlPort);

        if (portsFree) {
            cout << "At least one port wasn't free (of: " << _serverPort << ", " << _nodePort << ", "
                 << _controlPort << ") to start server, things will probably fail." << endl;
        }

#ifdef VALGRIND
    #define xstr(a) str(a)
    #define str(a) #a

    list<string> valgrind = SParseList(xstr(VALGRIND), ' ');
    if (valgrind.size()) {
        serverName = valgrind.front();
        cout << "Starting bedrock server in '" << serverName << "' with args: " << endl;
        auto it = valgrind.rbegin();
        while (it != valgrind.rend()) {
            args.push_front(*it);
            it++;
        }
        cout << SComposeList(args, " ") << endl;
        cout << "==========================" << endl;
    }
#endif

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

        // And then start the new server!
        execvp(serverName.c_str(), cargs);

        // The above line should only ever return if it failed, so let's check for that.
        cout << "Starting bedrock failed. Errno: " << errno << ", msg: " << strerror(errno) << ", serverName: " << serverName;
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
                socket = S_socket(wait ? _args["-serverHost"] : _args["-controlPort"], true, false, true);
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
            auto result = executeWaitMultipleData({status}, 1, !wait);
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

string BedrockTester::executeWaitVerifyContent(SData request, const string& expectedResult, bool control, uint64_t retryTimeoutUS) {
    uint64_t start = STimeNow();
    vector<SData> results;
    do {
        results = executeWaitMultipleData({request}, 1, control);

        if (results.size() > 0 && SStartsWith(results[0].methodLine, expectedResult)) {
            // good, got the result we wanted
            break;
        }
        usleep(100'000);

    } while(STimeNow() < start + retryTimeoutUS);

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
                        socket = S_socket((control ? _args["-controlPort"] : _args["-serverHost"]), true, false, true);
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

                // Send until there's nothing left in the buffer.
                SFastBuffer sendBuffer(myRequest.serialize());
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
                SFastBuffer recvBuffer("");
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
                        results[myIndex] = move(responseData);
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
                        results[myIndex] = move(responseData);
                        if (headers["Connection"] == "close") {
                            ::shutdown(socket, SHUT_RDWR);
                            ::close(socket);
                            socket = 0;
                            break;
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
        // Assumes wal2 mode.
        _db = new SQLite(_args["-db"], 1000000, 3000000, -1, "", 0);
    }
    return *_db;
}

string BedrockTester::readDB(const string& query)
{
    SQLite& db = getSQLiteDB();
    db.beginTransaction();
    string result = db.read(query);
    db.rollback();
    return result;
}

bool BedrockTester::readDB(const string& query, SQResult& result)
{
    SQLite& db = getSQLiteDB();
    db.beginTransaction();
    bool success = db.read(query, result);
    db.rollback();
    return success;
}

bool BedrockTester::waitForStatusTerm(const string& term, const string& testValue, uint64_t timeoutUS) {
    uint64_t start = STimeNow();
    while (STimeNow() < start + timeoutUS) {
        try {
            string result = SParseJSONObject(executeWaitVerifyContent(SData("Status"), "200", true))[term];

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

bool BedrockTester::waitForState(const string& state, uint64_t timeoutUS)
{
    return waitForStatusTerm("state", state, timeoutUS);
}

int BedrockTester::getPID() const
{
    return _serverPID;
}
