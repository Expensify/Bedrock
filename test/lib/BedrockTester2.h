#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/TestHTTPS.h>
#include <test/lib/tpunit++.hpp>
#include <test/lib/PortMap.h>

class BedrockTester2 {
  public:
    // This is an allocator for TCP ports, new servers can get new port numbers from this object and return them when
    // they're done.
    static PortMap ports;

    // Constructor.
    // args: A set of command line arguments to pass to the bedrock server when it starts.
    // queries: A list of queries to run on the newly created DB for the server *before it starts*.
    // serverPort: the port on which to listen for commands
    // nodePort: the port on which to communicate with the rest of the cluster.
    // controlPort: the port on which to send control messages to the server.
    //
    // NOTE ON PORTS:
    // IF these are 0, they will be allocated automatically from `ports` above. If the are non-zero, they must have
    // already been allocated from `ports` above by the caller, and they will be returned to `ports` on destruction.
    //
    // startImmediately: Should the server start running before the constructor returns?
    BedrockTester2(const map<string, string>& args = {},
                  const list<string>& queries = {},
                  uint16_t serverPort = 0,
                  uint16_t nodePort = 0,
                  uint16_t controlPort = 0,
                  bool startImmediately = true);

    // Destructor.
    ~BedrockTester2();

    // Start and stop the bedrock server. If `dontWait` is specified, return as soon as the control port, rather that
    // the command port, is ready.
    string startServer(bool dontWait = false);
    void stopServer(int signal = SIGINT);

    // Shuts down all bedrock servers associated with any existing testers.
    static void stopAll();

    // Generate a temporary filename with an optional prefix. Used particularly to create new DB files for each server,
    // but can generally be used for any temporary file required.
    static string getTempFileName(string prefix = "");

    // Change the args on a stopped server.
    void updateArgs(const map<string, string> args);

    // Takes a list of requests, and returns a corresponding list of responses.
    // Uses `connections` parallel connections to the server to send the requests.
    // If `control` is set, sends the message to the control port.
    vector<SData> executeWaitMultipleData(vector<SData> requests, int connections = 10, bool control = false, bool returnOnDisconnect = false, int* errorCode = nullptr);

    // Sends a single request, returning the response content.
    // If the response method line doesn't begin with the expected result, throws.
    string executeWaitVerifyContent(SData request, const string& expectedResult = "200", bool control = false);

    // Sends a single request, returning the response content as a STable.
    // If the response method line doesn't begin with the expected result, throws.
    STable executeWaitVerifyContentTable(SData request, const string& expectedResult = "200");

    // Read from the DB file. Interface is the same as SQLiteNode's 'read' for backwards compatibility.
    string readDB(const string& query);
    bool readDB(const string& query, SQResult& result);
    SQLite& getSQLiteDB();
    bool autoRollbackEveryDBCall = true;

    // This allows callers to run an entire transaction easily.
    class ScopedTransaction {
      public:
        ScopedTransaction(BedrockTester2* tester) : _tester(tester) {
            _tester->autoRollbackEveryDBCall = false;
            _tester->getSQLiteDB().beginTransaction();
        }
        ~ScopedTransaction() {
            _tester->getSQLiteDB().rollback();
            _tester->autoRollbackEveryDBCall = true;
        }
      private:
        BedrockTester2* _tester;
    };

    int getServerPID() { return _serverPID; }

    // Expose the ports that the server is listening on.
    int getServerPort();
    int getNodePort();
    int getControlPort();

    // Waits up to timeoutUS for the node to be in state `state`, returning true as soon as that state is reached, or
    // false if the timeout is hit.
    bool waitForState(string state, uint64_t timeoutUS = 60'000'000, bool control = false);

    // Like `waitForState` but wait for any of a set of states.
    bool waitForStates(set<string> states, uint64_t timeoutUS = 60'000'000, bool control = false);

    // get the output of a "Status" command from the command port
    STable getStatus(bool control = false);

    // get the value of a particular term from the output of a "Status" command
    string getStatusTerm(string term, bool control = false);

    // wait for the value of a particular term to match the testValue
    bool waitForStatusTerm(string term, string testValue, uint64_t timeoutUS = 30'000'000, bool control = false);

    // wait for the specified commit, up to "retries" times
    bool waitForCommit(int minCommitCount, int retries = 30, bool control = false);

  protected:
    // Returns the name of the server binary, by finding the first path that exists in `locations`.
    static string getServerName();

    // Args passed on creation, which will be used to start the server if the `start` flag is set, or if `startServer`
    // is called later on with an empty args list.
    map<string, string> _args;

    // If these are set, they'll be used instead of the global defaults.
    string _serverAddr;
    string _dbName;

    string _controlAddr;

    // The PID of the bedrock server we started.
    int _serverPID = 0;

    // A set of all bedrock testers.
    static set<BedrockTester2*> _testers;

    // A version of the DB that can be queries without going through bedrock.
    SQLite* _db = 0;

    // For locking around changes to the _testers list.
    static mutex _testersMutex;

    // The ports the server will listen on.
    uint16_t _serverPort;
    uint16_t _nodePort;
    uint16_t _controlPort;
};
