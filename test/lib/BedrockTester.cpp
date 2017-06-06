#include "BedrockTester.h"
#include <sys/wait.h>
string BedrockTester::DB_FILE = "";
string BedrockTester::SERVER_ADDR = "";
bool BedrockTester::startServers = true;
set<int> BedrockTester::serverPIDs;

// Make llvm and gcc get along.
#ifdef _NOEXCEPT
#define __NOEXCEPT _NOEXCEPT
#else
#define __NOEXCEPT _GLIBCXX_USE_NOEXCEPT
#endif

#define DB_WRITE_OPEN_FLAGS SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX

class BedrockTestException : public std::exception {
  private:
    const string message;

  public:
    BedrockTestException(string message_) : message(message_) {}

    virtual const char* what() const __NOEXCEPT { return message.c_str(); }
};

// Create temporary file. Returns its name or the empty string on failure.
string BedrockTester::getTempFileName(string prefix) {
    string templateStr = "/tmp/" + prefix + "bedrocktest_XXXXXX.db";
    char buffer[templateStr.size() + 1];
    strcpy(buffer, templateStr.c_str());
    int filedes = mkstemps(buffer, 3);
    close(filedes);
    return buffer;
}

BedrockTester::BedrockTester(const string& filename, const string& serverAddress, const list<string>& queries, const map<string, string>& args, bool wait) {
    nextActivity = 0;
    if (filename.empty()) {
        _dbFile = BedrockTester::DB_FILE;
    } else {
        _dbFile = filename;
    }

    if (serverAddress.empty()) {
        _serverAddr = BedrockTester::SERVER_ADDR;
    } else {
        _serverAddr = serverAddress;
    }

    createFile(_dbFile);

    // We don't use SQLite here, because we specifically want to avoid dealing with journal tables.
    if (queries.size()) {
        sqlite3* _db;
        sqlite3_initialize();
        sqlite3_open_v2(_dbFile.c_str(), &_db, DB_WRITE_OPEN_FLAGS, NULL);

        for (string query : queries) {
            int error = sqlite3_exec(_db, query.c_str(), 0, 0, 0);
            if (error) {
                cout << "Init Query: " << query << ", FAILED. Error: " << error << endl;
            }
        }

        SASSERT(!sqlite3_close(_db));
    }

    if (startServers) {
        startServer(args, wait);
    }
}

BedrockTester::~BedrockTester() {
    if (db) {
        delete db;
        db = 0;
    }
    if (writableDB) {
        delete writableDB;
        writableDB = 0;
    }
    if (serverPID) {
        stopServer();
        deleteFile(_dbFile);
    }
}

SQLite& BedrockTester::getSQLiteDB() {
    if (!db) {
        db = new SQLite(_dbFile, 1000000, 100, 3000000, -1, -1);
    }
    return *db;
}

SQLite& BedrockTester::getWritableSQLiteDB() {
    if (!writableDB) {
        writableDB = new SQLite(_dbFile, 1000000, 100, 3000000, -1, -1);
    }
    return *writableDB;
}

// This is sort of convoluted because of the way it was originally built. We can probably just have this always
// use the member variable db and never open it's own handle. This was added early ob for debugging and should be
// obsolete.
string BedrockTester::readDB(const string& query) { return getSQLiteDB().read(query); }

bool BedrockTester::readDB(const string& query, SQResult& result) { return getSQLiteDB().read(query, result); }

bool BedrockTester::deleteFile(string name) {
return true;
    string shm = name + "-shm";
    string wal = name + "-wal";
    bool retval = true;
    if (SFileExists(name.c_str()) && unlink(name.c_str())) {
        retval = false;
    }
    if (SFileExists(shm.c_str()) && unlink(shm.c_str())) {
        retval = false;
    }
    if (SFileExists(wal.c_str()) && unlink(wal.c_str())) {
        retval = false;
    }
    return retval;
}

bool BedrockTester::createFile(string name) {
    if (!SFileExists(name)) {
        return SFileSave(name, "");
    }
    return true;
}

string BedrockTester::getServerName() {
    list<string> locations = {
        "../bedrock",
        "../../bedrock",
    };
    for (auto location : locations) {
        if (SFileExists(location)) {
            return location;
        }
    }
    return "";
}

list<string> BedrockTester::getServerArgs(map <string, string> args) {

    map <string, string> defaults = {
        {"-db",               _dbFile.empty() ? DB_FILE : _dbFile},
        {"-serverHost",       _serverAddr.empty() ? SERVER_ADDR : _serverAddr},
        {"-nodeName",        "bedrock_test"},
        {"-nodeHost",         "localhost:9889"},
        {"-priority",         "200"},
        {"-plugins",          "db,cache"},
        {"-readThreads",      "8"},
        {"-maxJournalSize",   "100"},
        {"-v",                ""},
        {"-quorumCheckpoint", "50"},
        {"-parallelCommands", "Query,idcollision"},
        {"-cache",            "10001"},
    };

    for (auto row : defaults) {
        if (args.find(row.first) == args.end()) {
            args[row.first] = row.second;
        }
    }

    list<string> arglist;
    for (auto arg : args) {
        arglist.push_back(arg.first);
        arglist.push_back(arg.second);
    }

    return arglist;
}

string BedrockTester::getCommandLine() {
    string cmd = getServerName();
    list<string> args = getServerArgs();
    for (string arg: args) {
        cmd += " " + arg;
    }
    return cmd;
}

void BedrockTester::startServer(map<string, string> args_, bool wait) {
    string serverName = getServerName();
    int childPID = fork();
    if (!childPID) {
        // We are the child!
        list<string> args = getServerArgs(args_);
        // First arg is path to file.
        args.push_front(getServerName());
        if (_spoofInternalCommands) {
            args.push_back("-allowInternalCommands");
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

        // And then start the new server!
        execvp(serverName.c_str(), cargs);
    } else {
        // We'll kill this later.
        serverPID = childPID;
        serverPIDs.insert(serverPID);

        // Wait for the server to start up.
        // TODO: Make this not take so long, particularly in Travis. This probably really requires making the server
        // come up faster, not a change in how we wait for it, though it would be nice if we could do something
        // besides this 100ms polling.
        int count = 0;
        while (wait) {
            count++;
            // Give up after a minute. This will fail the remainder of the test, but won't hang indefinitely.
            if (count > 60 * 10) {
                break;
            }
            try {
                SData status("Status");
                executeWait(status, "200");
                break;
            } catch (...) {
                // This will happen if the server's not up yet. We'll just try again.
            }
            usleep(100000); // 0.1 seconds.
        }
    }
}

void BedrockTester::stopServer(int pid) {
    kill(pid, SIGINT);
    int status;
    waitpid(pid, &status, 0);
    serverPIDs.erase(pid);
}

void BedrockTester::stopServer() {
    stopServer(serverPID);
}

vector<pair<string,string>> BedrockTester::executeWaitMultiple(vector<SData> requests, int connections) {

    // Synchronize dequeuing requessts, and saving results.
    recursive_mutex listLock;

    // Our results go here.
    vector<pair<string,string>> results;
    results.resize(requests.size());

    // This is the next index of `requests` that needs processing.
    int currentIndex = 0;

    // This is the list of threads that we'll use for each connection.
    list <thread> threads;

    // Spawn a thread for each connection.
    for (int i = 0; i < connections; i++) {

        threads.emplace_back([&, i](){

            // Create a socket.
            int socket = S_socket(_serverAddr.empty() ? SERVER_ADDR : _serverAddr, true, false, true);

            while (true) {

                size_t myIndex = 0;
                SData myRequest;
                {
                    SAUTOLOCK(listLock);

                    myIndex = currentIndex;
                    currentIndex++;

                    if (myIndex >= requests.size()) {
                        // No more requests to process.
                        break;
                    } else {
                        myRequest = requests[myIndex];
                    }
                }

                // We've released our lock so other threads can dequeue stuff now.

                // Send some stuff on our socket.
                string sendBuffer = myRequest.serialize();
                // Send our data.
                while (sendBuffer.size()) {
                    bool result = S_sendconsume(socket, sendBuffer);
                    if (!result) {
                        break;
                    }
                }

                // Receive some stuff on our socket.
                string recvBuffer = "";

                string methodLine, content;
                STable headers;
                int timeouts = 0;
                while (!SParseHTTP(recvBuffer.c_str(), recvBuffer.size(), methodLine, headers, content)) {

                    // Poll the socket, so we get a timeout.

                    pollfd readSock;
                    readSock.fd = socket;
                    readSock.events = POLLIN;
                    readSock.revents = 0;

                    // wait for a second...
                    poll(&readSock, 1, 1000);
                    if (readSock.revents & POLLIN) {
                        bool result = S_recvappend(socket, recvBuffer);
                        if (!result) {
                            break;
                        }
                    } else {
                        timeouts++;
                        if (timeouts > 5) {
                            //SAUTOLOCK(listLock);
                            //cout << "Timeout (" << timeouts << ") waiting on socket, will try again." << endl;
                        }
                        if (timeouts == 60) {
                            SAUTOLOCK(listLock);
                            cout << "Thread " << i << ". Too many timeouts! Giving up on: " << myRequest["Query"] << endl;
                            break;
                        }
                    }

                }

                // Lock to avoid log lines writing over each other.
                {
                    SAUTOLOCK(listLock);
                    if (timeouts == 60) {
                        // cout << "this failed: " << myRequest.serialize() << endl;
                        results[myIndex] = make_pair("000 Timeout", myRequest.serialize());
                    } else {
                        // Ok, done, let's lock again and insert this in the results.
                        results[myIndex] = make_pair(methodLine, content);
                    }
                }
            }

            close(socket);
        });

    }

    // Wait for our threads to finish.
    for (thread& t : threads) {
        t.join();
    }

    // All done!
    return results;
}

string BedrockTester::getServerAddr() {
    return _serverAddr.empty() ? SERVER_ADDR : _serverAddr;
}

string BedrockTester::executeWait(const SData& request, const string& correctResponse) {
    SData response = executeWaitData(request, correctResponse);
    return response.content;
}

SData BedrockTester::executeWaitData(const SData& request, const string& correctResponse) {
    // We create a socket, send the message, wait for the response, close the socket, and parse the message.
    int socket = S_socket(getServerAddr(), true, false, true);

    string sendBuffer = request.serialize();
    // Send our data.
    while (sendBuffer.size()) {
        bool result = S_sendconsume(socket, sendBuffer);
        if (!result) {
            break;
        }
    }

    // Receive the response.
    string recvBuffer = "";

    string methodLine, content;
    STable headers;
    while (!SParseHTTP(recvBuffer.c_str(), recvBuffer.size(), methodLine, headers, content)) {
        bool result = S_recvappend(socket, recvBuffer);
        if (!result) {
            break;
        }
    }

    if (!SStartsWith(methodLine, correctResponse)) {
        throw BedrockTestException(string("Expected '" + correctResponse + "', got: " + methodLine));
    }

    close(socket);

    SData response(methodLine);
    response.nameValueMap = headers;
    response.content = content;
    return response;
}
