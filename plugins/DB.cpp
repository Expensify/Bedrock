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
  query(SStartsWith(SToLower(request.methodLine), "query:") ? request.methodLine.substr(strlen("query:")) : request["query"])
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
    BedrockPlugin::verifyAttributeBool(request, "nowhere",  false);

    // See if it's read-only (and thus safely peekable) or read-write
    // (and thus requires processing).
    //
    // **NOTE: This isn't intended to be foolproof, and attempts to err on the
    //         side of caution (eg, assuming read-write unless clearly read-
    //         only).  A not-so-clever client could easily bypass this.  But
    //         that same person could also easily wreck havoc in a bunch of
    //         other ways, too.  That said, the worst-case scenario is that a
    //         read-write command is mis-classified as read-only and executed in
    //         the peek, but even then we'll detect it after the fact and shut
    //         the node down.
    const string& upperQuery = SToUpper(STrim(query));
    bool shouldRequireWhere = !request.test("nowhere");

    if (!SEndsWith(upperQuery, ";")) {
        SALERT("Query aborted, query must end in ';'");
        STHROW("502 Query aborted");
    }

    // Attempt the read-only query
    SQResult result;
    int preChangeCount = db.getChangeCount();
    if (!db.read(query, result)) {
        if (shouldRequireWhere &&
            (SStartsWith(upperQuery, "UPDATE") || SStartsWith(upperQuery, "DELETE")) &&
            !SContains(upperQuery, " WHERE ")) {
            SALERT("Query aborted, it has no 'where' clause: '" << query << "'");
            STHROW("502 Query aborted");
        }

        // Assume it's write or bad query, escalating it to process
        // If it's a bad query, it will fail in the process too
        SINFO("Query appears to be read/write, queuing for processing.");
        return false;
    }

    // Verify it didn't change anything -- assert because if we did, we did so
    // outside of a replicated transaction and that's REALLY bad.
    if (preChangeCount != db.getChangeCount()) {
        // This database is fucked -- we made a change outside of a transaction
        // so we can't roll back, and outside of a *distributed* transaction so
        // now it's out of sync with the rest of the cluster.  This database
        // needs to be destroyed and recovered from a peer.
        SERROR("Read query actually managed to write; database is corrupt "
               << "and must be recovered from backup or peer.  Offending query: '" << query << "'");
    }

    // Worked!  What format do we want the output?
    response.content = result.serialize(request["Format"]);
    return true; // Successfully peeked
}

void BedrockDBCommand::process(SQLite& db) {
    if (db.getUpdateNoopMode()) {
        SINFO("Query run in mocked request, just ignoring.");
        return;
    }

    // Attempt the query
    if (!db.write(query)) {
        // Query failed
        SALERT("Query failed: '" << query << "'");
        response["error"] = db.getLastError();
        STHROW("502 Query failed");
    }

    // Worked!  Let's save the last inserted row id
    const string& upperQuery = SToUpper(STrim(query));
    if (SStartsWith(upperQuery, "INSERT ")) {
        response["lastInsertRowID"] = SToStr(db.getLastInsertRowID());
    }

    // Successfully processed
    return;
}
