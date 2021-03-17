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

    // Start the server. If `wait` is specified, wait until the server is fully up with the command port open and
    // accepting requests. Otherwise, returns as soon as the control port is open and can return `Status`.
    string startServer(bool wait = true);

    // Stop a server by sending it a signal.
    void stopServer(int signal = SIGINT);

    // Shuts down all bedrock servers associated with any existing testers.
    static void stopAll();

    // Generate a temporary filename with an optional prefix. Used particularly to create new DB files for each server,
    // but can generally be used for any temporary file required.
    static string getTempFileName(string prefix = "");

    // Change the arguments for a server. Only takes effect when the server next starts. This can change or add args,
    // but not remove args. Any args specified here are added or replaced into the existing set.
    void updateArgs(const map<string, string> args);

    // Takes a list of requests, and returns a corresponding list of responses.
    // Uses `connections` parallel connections to the server to send the requests.
    // If `control` is set, sends the message to the control port.
    vector<SData> executeWaitMultipleData(vector<SData> requests, int connections = 10, bool control = false, bool returnOnDisconnect = false, int* errorCode = nullptr);

    // Sends a single request, returning the response content.
    // If the response method line doesn't begin with the expected result, throws.
    // Convenience wrapper around executeWaitMultipleData.
    string executeWaitVerifyContent(SData request, const string& expectedResult = "200", bool control = false);

    // Sends a single request, returning the response content as a STable.
    // If the response method line doesn't begin with the expected result, throws.
    // Convenience wrapper around executeWaitMultipleData.
    STable executeWaitVerifyContentTable(SData request, const string& expectedResult = "200");

    // Read from the DB file, without going through the bedrock server. Two interfaces are provided to maintain
    // compatibility with the `SQLite` class.
    string readDB(const string& query);
    bool readDB(const string& query, SQResult& result);

    // Wait for a particular key in a `Status` message to equal a particular value, for up to `timeoutUS` us. Returns
    // true if a match was found, or times out otherwose.
    bool waitForStatusTerm(const string& term, const string& testValue, uint64_t timeoutUS = 60'000'000);

    // This is just a convenience wrapper around `waitForStatusTerm` looking for the state of the node.
    bool waitForState(const string& state, uint64_t timeoutUS = 60'000'000);

  protected:
    // Returns the name of the server binary, by finding the first path that exists in `locations`.
    static string getServerName();

    // Returns an SQLite object attached to the same DB file as the bedrock server. Writing to this is dangerous and
    // should not be done!
    SQLite& getSQLiteDB();

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
