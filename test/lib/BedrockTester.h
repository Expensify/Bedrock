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

    string dbFile;
    uint64_t nextActivity;
    bool passing;
    int serverPID = 0;
    SQLite* db = nullptr;
    SQLite* writableDB = nullptr;

    // Constructor
    BedrockTester(string filename = "");
    ~BedrockTester();

    // Executes a command and waits for the response
    string executeWait(const SData& request, const std::string& correctResponse = "200");

    string readDB(const string& query);
    bool readDB(const string& query, SQResult& result);
    SQLite& getSQLiteDB();
    SQLite& getWritableSQLiteDB();
    static string getCommandLine();

  private:
    // these exist to allow us to create and delete our database file.
    bool createFile(string name);

    void startServer();
    void stopServer();

    static string getServerName();
    static list<string> getServerArgs();

    bool _spoofInternalCommands;
};
