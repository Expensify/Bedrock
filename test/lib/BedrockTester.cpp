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

class BedrockTestException : public std::exception {
private:
    const string message;

public:
    BedrockTestException(string message_) : message(message_) {}

    virtual const char* what() const __NOEXCEPT {
        return message.c_str();
    }
};

BedrockTester::BedrockTester(string filename) : passing(true) {
    nextActivity = 0;

    if (filename.empty()) {
        dbFile = BedrockTester::DB_FILE;
    } else {
        dbFile = filename;
    }

    createFile(dbFile);
    if (startServers) {
        startServer();
    }
}

BedrockTester::~BedrockTester()
{
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
        deleteFile(dbFile);
    }
}

SQLite& BedrockTester::getSQLiteDB()
{
    if (!db) {
        db = new TestSQLite(dbFile, 1000000, false, true, 3000000);
    }
    return *db;
}

SQLite& BedrockTester::getWritableSQLiteDB()
{
    if (!writableDB) {
        writableDB = new TestSQLite(dbFile, 1000000, false, false, 3000000);
    }
    return *writableDB;
}

// This is sort of convoluted because of the way it was originally built. We can probably just have this always
// use the member variable db and never open it's own handle. This was added early ob for debugging and should be
// obsolete.
string BedrockTester::readDB(const string& query)
{
    return getSQLiteDB().read(query);
}

bool BedrockTester::readDB(const string& query, SQResult& result)
{
    return getSQLiteDB().read(query, result);
}

bool BedrockTester::deleteFile(string name)
{
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

bool BedrockTester::createFile(string name)
{
    if(!SFileExists(name)) {
        return SFileSave(name, "");
    }
    return true;
}

string BedrockTester::getServerName() {
    return "../bedrock";
}

list<string> BedrockTester::getServerArgs() {
    list<string> args = {
        "-db",
        BedrockTester::DB_FILE,
        "-serverHost",
        SERVER_ADDR,
        "-nodeName",
        "bedrock_test",
        "-nodeHost",
        "localhost:9889",
        "-priority",
        "200",
        "-plugins",
        "status,db,cache",
        "-v",
        "-cache",
        "10001",
    };

    return args;
}

string BedrockTester::getCommandLine() {
    string cmd = getServerName();
    list<string> args = getServerArgs();
    for_each(args.begin(), args.end(), [&](string arg) {
        cmd += " " + arg;
    });
    return cmd;
}

void BedrockTester::startServer()
{
    string serverName = getServerName();
    int childPID = fork();
    if (!childPID) {
        // We are the child!
        list<string> args = getServerArgs();
        // First arg is path to file.
        args.push_front(getServerName());
        if (_spoofInternalCommands) {
            args.push_back("-allowInternalCommands");
        }

        // Convert our c++ strings to old-school C strings for exec.
        char* cargs[args.size() + 1];
        int count = 0;
        for_each(args.begin(), args.end(), [&](string arg){
            char* newstr = (char*)malloc(arg.size() + 1);
            strcpy(newstr, arg.c_str());
            cargs[count] = newstr;
            count++;
        });
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
        while (1) {
            count++;
            // Give up after a minute. This will fail the remainder of the test, but won't hang indefinitely.
            if (count > 60 * 10) {
                break;
            }
            try {
                SData status("Status");
                executeWait(status, "200");
                break;
            }
            catch (...) {
                // This will happen if the server's not up yet. We'll just try again.
            }
            usleep(100000); // 0.1 seconds.
        }
    }
}

void BedrockTester::stopServer(int pid)
{
    kill(pid, SIGKILL);
    int status;
    waitpid(pid, &status, 0);
    serverPIDs.erase(pid);
}

void BedrockTester::stopServer()
{
    stopServer(serverPID);
}

string BedrockTester::executeWait(const SData& request, const std::string& correctResponse)
{
    // We create a socket, send the message, wait for the response, close the socket, and parse the message.
    int socket = S_socket(SERVER_ADDR, true, false, true);

    string sendBuffer = request.serialize();
    // Send our data.
    while(sendBuffer.size()) {
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
    
    return content;
}
