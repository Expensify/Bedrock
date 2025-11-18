#pragma once
#include <libstuff/libstuff.h>
#include "../BedrockPlugin.h"

class BedrockPlugin_DB : public BedrockPlugin {
public:
    BedrockPlugin_DB(BedrockServer& s);
    virtual const string& getName() const;
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);

    static const string name;

    class Sqlite3QRFSpecWrapper {
public:
        sqlite3_qrf_spec spec{0};
        string* zColumnSep{nullptr};
        string* zNull{nullptr};

        Sqlite3QRFSpecWrapper();
        ~Sqlite3QRFSpecWrapper();

        Sqlite3QRFSpecWrapper(const Sqlite3QRFSpecWrapper&) = delete;
        Sqlite3QRFSpecWrapper& operator=(const Sqlite3QRFSpecWrapper&) = delete;

        Sqlite3QRFSpecWrapper(Sqlite3QRFSpecWrapper&& other) noexcept;
        Sqlite3QRFSpecWrapper& operator=(Sqlite3QRFSpecWrapper&& other) noexcept;
    };

    static Sqlite3QRFSpecWrapper parseSQLite3Args(const string& argsToParse);
};

class BedrockDBCommand : public BedrockCommand {
public:
    BedrockDBCommand(SQLiteCommand&& baseCommand, BedrockPlugin_DB* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);

private:
    string query;

    // Callback for SQLite output formatter.
    static int SQLiteFormatAppend(void* destString, const char* appendString, sqlite3_int64 length);
};
