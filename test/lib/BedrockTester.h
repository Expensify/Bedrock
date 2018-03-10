#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/TestHTTPS.h>
#include <test/lib/tpunit++.hpp>

class BedrockTester {
  public:

    static int mockRequestMode;
    // Generate a temporary filename for a test DB, with an optional prefix.
    static string getTempFileName(string prefix = "");

    // Returns the name of the server binary, by finding the first path that exists in `locations`.
    static string getServerName();

    // Search paths for `getServerName()`. Allowed to be modified before startup by implementer.
    static list<string> locations;

    // Default values for the location of the DB file and the server to talk to.
    // These can be over-ridden when instantiating a tester.
    // Typically, these values will be set in main().
    static string defaultDBFile;
    static string defaultServerAddr;

    // This is expected to be set by main, built from argv, to expose command-line options to tests.
    static SData globalArgs;

    // Shuts down all bedrock servers associated with any testers.
    static void stopAll();

    // Returns the address of this server.
    string getServerAddr() { return _serverAddr; };

    // Constructor/destructor
    BedrockTester(const map<string, string>& args = {},
                  const list<string>& queries = {}, 
                  bool startImmediately = true,
                  bool keepFilesWhenFinished = false);
    ~BedrockTester();

    // Start and stop the bedrock server. If `dontWait` is specified, return as soon as the control port, rather that
    // the cmmand port, is ready.
    string startServer(bool dontWait = false);
    void stopServer();

    // Takes a list of requests, and returns a corresponding list of responses.
    // Uses `connections` parallel connections to the server to send the requests.
    // If `control` is set, sends the message to the control port.
    vector<SData> executeWaitMultipleData(vector<SData> requests, int connections = 10, bool control = false);

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

  protected:
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
    static set<BedrockTester*> _testers;

    // Flag indicating whether the DB should be kept when the tester is destroyed.
    bool _keepFilesWhenFinished;

    // A version of the DB that can be queries without going through bedrock.
    SQLite* _db = 0;

    // For locking around changes to the _testers list.
    static mutex _testersMutex;
};
