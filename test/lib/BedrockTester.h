#pragma once
#include <libstuff/libstuff.h>
#include <sqlitecluster/SQLite.h>
#include <test/lib/TestHTTPS.h>
#include <test/lib/tpunit++.hpp>

class BedrockTester {
  public:
    // The location of the database. This is static so we can re-use it for the life of the test app.
    static string DB_FILE;
    static string SERVER;
    static string SERVER_ADDR;
    static set<int> serverPIDs;
    static void stopServer(int pid);
    static bool deleteFile(string name);
    static bool startServers;

    uint64_t nextActivity;
    int serverPID = 0;
    SQLite* db = nullptr;
    SQLite* writableDB = nullptr;

    string getServerAddr();

    // Constructor
    BedrockTester(const string& filename = "", const string& serverAddress = "", const list<string>& queries = {}, const map<string, string>& args = {}, bool wait = true);
    ~BedrockTester();

    // Executes a command and waits for the response
    string executeWait(const SData& request, const std::string& correctResponse = "200");

    // Same as above, but returns entire SData for the response
    SData executeWaitData(const SData& request, const std::string& correctResponse = "200");

    // like executeWait, except it will execute multiple requests in parallel over several simultaneous connections.
    // returns a pair of strings for each request, with the response code and the response text, in that order.
    vector<pair<string,string>> executeWaitMultiple(vector<SData> requests, int connections = 10);

    string readDB(const string& query);
    bool readDB(const string& query, SQResult& result);
    SQLite& getSQLiteDB();
    SQLite& getWritableSQLiteDB();
    string getCommandLine();

    static string getTempFileName(string prefix = "");

    void stopServer();
    void startServer(map <string, string> args_ = {},  bool wait = true);

  private:
    // these exist to allow us to create and delete our database file.
    bool createFile(string name);

    string getServerName();
    list<string> getServerArgs(map <string, string> args = {});

    bool _spoofInternalCommands;

    // If these are set, they'll be used instead of the global defaults.
    string _serverAddr;
    string _dbFile;
};
