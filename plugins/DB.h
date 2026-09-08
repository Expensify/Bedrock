/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    DB.h
 * Path:    plugins/DB.h
 * Pair:    DB.cpp
 *
 * INTENT
 *   Declares the "DB" plugin, which exposes raw SQL as a Bedrock command:
 *   clients (and the sqlite3 CLI-compatible tooling and the MySQL plugin's
 *   fallback path) send a "Query"/"Query: ...sql..." request and get back
 *   the result formatted per sqlite3-CLI-style flags or as JSON.
 *
 * OBJECTS
 *   BedrockPlugin_DB                      - the plugin; recognizes "Query"
 *                                            requests and hands them to
 *                                            BedrockDBCommand.
 *   BedrockPlugin_DB::Sqlite3QRFSpecWrapper - move-only RAII wrapper around a
 *                                            sqlite3_qrf_spec C struct, whose
 *                                            zColumnSep/zNull char pointers
 *                                            point into two heap-owned
 *                                            strings this wrapper manages.
 *   BedrockPlugin_DB::parseSQLite3Args (static) - parses a sqlite3-CLI-style
 *                                            argument string (-json, -csv,
 *                                            -separator, etc.) into a
 *                                            Sqlite3QRFSpecWrapper.
 *   BedrockPlugin_DB::generateErrorContextMessage (static) - builds a
 *                                            two-line, caret-pointing error
 *                                            snippet mimicking the sqlite3
 *                                            CLI's error display.
 *   BedrockDBCommand                      - BedrockCommand implementing
 *                                            peek()/process() for arbitrary
 *                                            read/write SQL, with named
 *                                            bound parameters taken from
 *                                            "sql-param-<name>" headers.
 *   BedrockDBCommand::SQLiteFormatAppend (static, private) - write callback
 *                                            registered with the QRF
 *                                            formatter to accumulate output.
 *
 * OUT OF PLACE
 *   Sqlite3QRFSpecWrapper and parseSQLite3Args/generateErrorContextMessage
 *   [CANDIDATE] - this is a generic sqlite3-CLI argument-parsing and
 *   error-formatting layer wrapped around libstuff/qrf.h's C struct; none of
 *   it is specific to being a request-command plugin, and it could live
 *   alongside qrf.h in libstuff instead of inside the plugin that merely
 *   uses it.
 *
 * NAME/LOCATION FIT
 *   "DB" is a very generic name for what is specifically "run arbitrary SQL
 *   as a command" — every plugin here works with the DB. It fits by
 *   convention (this is the original, catch-all query plugin) but the name
 *   alone doesn't convey that scope.
 *
 * NAMING QUALITY
 *   Sqlite3QRFSpecWrapper mixes cases oddly ("Sqlite3" matching the C
 *   library's sqlite3_ prefix, "QRF" all-caps for the acronym) but is
 *   internally consistent with the wrapped sqlite3_qrf_spec type it names.
 *   Note the header depends on SQliteParameter (libstuff), whose own
 *   capitalization ("SQlite") does not match this codebase's usual "SQLite"
 *   capitalization (e.g. SQLite, SQLiteCommand) — a pre-existing
 *   inconsistency in that type's name, not introduced here.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/SQliteParameter.h>
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

    // This was implemented based on the sqlite3 cli code found here:
    // https://sqlite.org/src/info/55424c650715b3?ln=2545-2586
    static string generateErrorContextMessage(const string& query, const string& errorMessage, int errorOffset);
};

class BedrockDBCommand : public BedrockCommand {
public:
    BedrockDBCommand(SQLiteCommand&& baseCommand, BedrockPlugin_DB* plugin);
    virtual bool peek(SQLite& db);
    virtual void process(SQLite& db);

private:
    string query;

    // Named bound parameters extracted from `sql-param-<name>` request headers.
    map<string, SQliteParameter> params;

    // Callback for SQLite output formatter.
    static int SQLiteFormatAppend(void* destString, const char* appendString, sqlite3_int64 length);
};
