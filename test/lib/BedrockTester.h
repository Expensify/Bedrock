#pragma once
#include <signal.h>

#include <test/lib/PortMap.h>
#include <test/lib/tpunit++.hpp>

class SQLite;

class BedrockTester {
  public:
    static const bool ENABLE_HCTREE;

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
    BedrockTester(const map<string, string>& args = {},
                  const list<string>& queries = {},
                  uint16_t serverPort = 0,
                  uint16_t nodePort = 0,
                  uint16_t controlPort = 0,
                  bool startImmediately = true,
                  const string& bedrockBinary = "",
                  atomic<uint64_t>* alternateCounter = nullptr);

    // Destructor.
    virtual ~BedrockTester();

    // Start the server. If `wait` is specified, wait until the server is fully up with the command port open and
    // accepting requests. Otherwise, returns as soon as the control port is open and can return `Status`.
    string startServer(bool wait = true);

    // Stop a server by sending it a signal.
    virtual void stopServer(int signal = SIGTERM);

    // Shuts down all bedrock servers associated with any existing testers.
    static void stopAll();

    // If enabled, commands will send the most recent `commitCount` received back from this BedrockTester with each new request,
    // enforcing that prior commands finish before later commands.
    void setEnforceCommandOrder(bool enforce);

    // Generate a temporary filename with an optional prefix. Used particularly to create new DB files for each server,
    // but can generally be used for any temporary file required.
    static string getTempFileName(string prefix = "");

    // Change the arguments for a server. Only takes effect when the server next starts. This can change or add args,
    // but not remove args. Any args specified here are added or replaced into the existing set.
    void updateArgs(const map<string, string> args);

    string getArg(const string& arg) const;

    // Takes a list of requests, and returns a corresponding list of responses.
    // Uses `connections` parallel connections to the server to send the requests.
    // If `control` is set, sends the message to the control port.
    virtual vector<SData> executeWaitMultipleData(vector<SData> requests, int connections = 10, bool control = false, bool returnOnDisconnect = false, int* errorCode = nullptr);

    // Sends a single request, returning the response content.
    // If the response method line doesn't begin with the expected result, throws.
    // Convenience wrapper around executeWaitMultipleData.
    virtual string executeWaitVerifyContent(SData request, const string& expectedResult = "200 OK", bool control = false, uint64_t retryTimeoutUS = 0);

    // Sends a single request, returning the response content as a STable.
    // If the response method line doesn't begin with the expected result, throws.
    // Convenience wrapper around executeWaitMultipleData.
    virtual STable executeWaitVerifyContentTable(SData request, const string& expectedResult = "200 OK");

    // Read from the DB file, without going through the bedrock server. Two interfaces are provided to maintain
    // compatibility with the `SQLite` class.
    // Note that timeoutMS only applies in HC-Tree mode. It is ignored in WAL2 mode.
    string readDB(const string& query, bool online = true, int64_t timeoutMS = 0);
    bool readDB(const string& query, SQResult& result, bool online = true, int64_t timeoutMS = 0);

    // Closes and releases any existing DB file.
    void freeDB();

    // Wait for a particular key in a `Status` message to equal a particular value, for up to `timeoutUS` us. Returns
    // true if a match was found, or times out otherwose.
    bool waitForStatusTerm(const string& term, const string& testValue, uint64_t timeoutUS = 60'000'000);

    // Waits for the status to be either LEADING or FOLLOWING
    bool waitForLeadingFollowing(uint64_t timeoutUS = 60'000'000);

    // This is just a convenience wrapper around `waitForStatusTerm` looking for the state of the node.
    bool waitForState(const string& state, uint64_t timeoutUS = 60'000'000);

    int getPID() const;

    string serverName;

  protected:
    // Returns an SQLite object attached to the same DB file as the bedrock server. Writing to this is dangerous and
    // should not be done!
    SQLite& getSQLiteDB();

    // These are the arguments for the bedrock process we'll start for this tester. This is a combination of defaults,
    // automatically assigned arguments (like a randomly generated DB file name) and any args passed into the
    // constructor. These are stored and used any time the server is started, and can be modified with `updateArgs`.
    map<string, string> _args;

    // Stores the process ID of the running bedrock server while it's online, so that we can signal it to shut down.
    int _serverPID = 0;

    // Each new tester registers itself in this set on creation, and removes itself on destruction. This exists to
    // faciliate `stopAll` tearing down all existing servers in case we need to shutdown, for instance in the case
    // where we send the test `ctrl+c`.
    static set<BedrockTester*> _testers;

    // Locks around changes to the _testers list as each tester can run in a separate thread.
    static mutex _testersMutex;

    // This is the underlying storage for `getSQLiteDB` and will only be initialized once per tester.
    SQLite* _db = nullptr;

    // The ports the server will listen on.
    uint16_t _serverPort;
    uint16_t _nodePort;
    uint16_t _controlPort;
    uint16_t _commandPortPrivate;

    bool _enforceCommandOrder = false;
    atomic<uint64_t> _commitCountBase = 0;
    atomic<uint64_t>& _commitCount;
};

