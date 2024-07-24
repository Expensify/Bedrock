#include "DB.h"

#include <string.h>

#include <BedrockServer.h>
#include <libstuff/SQResult.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

const string BedrockPlugin_DB::name("DB");
const string& BedrockPlugin_DB::getName() const {
    return name;
}

BedrockPlugin_DB::BedrockPlugin_DB(BedrockServer& s) : BedrockPlugin(s)
{
}

BedrockDBCommand::BedrockDBCommand(SQLiteCommand&& baseCommand, BedrockPlugin_DB* plugin) :
  BedrockCommand(move(baseCommand), plugin),
    // The "full" syntax of a query request is:
    //
    //      Query
    //      Query: ...sql...
    //
    // However, that's awfully redundant.  As shorthand, we'll accept the query
    // in the method line as follows:
    //
    //      Query: ...sql...
  query(STrim(SStartsWith(SToLower(request.methodLine), "query:") ? request.methodLine.substr(strlen("query:")) : request["query"]))
{
}

unique_ptr<BedrockCommand> BedrockPlugin_DB::getCommand(SQLiteCommand&& baseCommand) {
    if (SStartsWith(SToLower(baseCommand.request.methodLine), "query:") || SIEquals(baseCommand.request.getVerb(), "Query")) {
        return make_unique<BedrockDBCommand>(move(baseCommand), this);
    }
    return nullptr;
}

bool BedrockDBCommand::peek(SQLite& db) {
    if (query.size() < 1 || query.size() > BedrockPlugin::MAX_SIZE_QUERY) {
        STHROW("402 Missing query");
    }

    if (!SEndsWith(query, ";")) {
        SALERT("Query aborted, query must end in ';'");
        STHROW("502 Query Missing Semicolon");
    }

    // Get a list of prepared statements from the database.
    list<sqlite3_stmt*> statements;
    int prepareResult = db.getPreparedStatements(query, statements);

    // Check each one to see if it's a write, and then release it.
    bool write = false;
    for (sqlite3_stmt* statement : statements) {
        if (!sqlite3_stmt_readonly(statement)) {
            write = true;
        }
        sqlite3_finalize(statement);
    }

    // If we got any errors while preparing, we're calling this a bad command.
    if (prepareResult != SQLITE_OK) {
        response["error"] = db.getLastError();
        STHROW("402 Bad query");
    }

    // If anything was a write, escalate to `process`.
    if (write) {
        return false;
    }

    // Attempt the read-only query
    SQResult result;
    if (!db.read(query, result)) {
        response["error"] = db.getLastError();
        STHROW("402 Bad query");
    }

    // Worked! Set the output and return.
    response.content = result.serialize(request["Format"]);

    return true;
}

void BedrockDBCommand::process(SQLite& db) {
    if (db.getUpdateNoopMode()) {
        SINFO("Query run in mocked request, just ignoring.");
        return;
    }
    BedrockPlugin::verifyAttributeBool(request, "nowhere",  false);

    const string upperQuery = SToUpper(query);
    if (!request.test("nowhere") &&
        (SStartsWith(upperQuery, "UPDATE") || SStartsWith(upperQuery, "DELETE")) &&
        !SContains(upperQuery, " WHERE ")) {
        SALERT("Query aborted, it has no 'where' clause: '" << query << "'");
        STHROW("502 Query aborted");
    }

    // Attempt the query
    if (!db.write(query)) {
        // Query failed
        SALERT("Query failed: '" << query << "'");
        response["error"] = db.getLastError();
        STHROW("502 Query failed");
    }

    // Worked! Let's save the last inserted row id
    if (SStartsWith(upperQuery, "INSERT ")) {
        response["lastInsertRowID"] = SToStr(db.getLastInsertRowID());
    }

    // Successfully processed
    return;
}
