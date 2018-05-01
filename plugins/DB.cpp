#include "DB.h"

#undef SLOGPREFIX
#define SLOGPREFIX "{" << _args["-nodeName"] << ":" << getName() << "} "

bool BedrockPlugin_DB::peekCommand(SQLite& db, BedrockCommand& command) {
    // Pull out some helpful variables
    SData& request = command.request;
    SData& response = command.response;

    // ----------------------------------------------------------------------
    // The "full" syntax of a query request is:
    //
    //      Query
    //      Query: ...sql...
    //
    // However, that's awfully redundant.  As shorthand, we'll accept the query
    // in the method line as follows:
    //
    //      Query: ...sql...
    //
    // We do this by rewriting the request when it matches that pattern
    if (SStartsWith(SToLower(request.methodLine), "query:")) {
        //  Just take everything after that and put into the query param
        SINFO("Rewriting command: " << request.methodLine);
        request["query"] = request.methodLine.substr(strlen("query:"));
        request.methodLine = "Query";
    }

    // ----------------------------------------------------------------------
    if (SIEquals(request.getVerb(), "Query")) {
        // - Query( query )
        //
        //     Executes a simple query
        //
        verifyAttributeSize(request, "query", 1, MAX_SIZE_QUERY);
        verifyAttributeBool(request, "nowhere",  false);

        // See if it's read-only (and thus safely peekable) or read-write
        // (and thus requires processing).
        //
        // **NOTE: This isn't intended to be foolproof, and attempts to err on the
        //         side of caution (eg, assuming read-write unless clearly read-
        //         only).  A not-so-clever client could easily bypass this.  But
        //         that same person could also easily wreck havoc in a bunch of
        //         other ways, too.  That said, the worst-case scenario is that a
        //         read-write command is mis-classified as read-only an executed in
        //         the peek, but even then we'll detect it after the fact and shut
        //         the node down.
        const string& query = request["query"];
        const string& upperQuery = SToUpper(STrim(query));
        bool shouldRequireWhere = !request.test("nowhere");

        if (!SEndsWith(upperQuery, ";")) {
            SALERT("Query aborted, query must end in ';'");
            STHROW("502 Query aborted");
        }

        if (SStartsWith(upperQuery, "SELECT ")) {
            // Seems to be read-only
            SINFO("Query appears to be read-only, peeking.");
        } else {
            if (shouldRequireWhere &&
               (SStartsWith(upperQuery, "UPDATE") || SStartsWith(upperQuery, "DELETE")) &&
               !SContains(upperQuery, " WHERE ")) {
                SALERT("Query aborted, it has no 'where' clause: '" << query << "'");
                STHROW("502 Query aborted");
            }

            // Assume it's read/write
            SINFO("Query appears to be read/write, queuing for processing.");
            return false;
        }

        // Attempt the read-only query
        SQResult result;
        int preChangeCount = db.getChangeCount();
        if (!db.read(query, result)) {
            // Query failed
            SALERT("Query failed: '" << query << "'");
            response["error"] = db.getLastError();
            STHROW("502 Query failed");
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

    // Didn't recognize this command
    return false;
}

bool BedrockPlugin_DB::processCommand(SQLite& db, BedrockCommand& command) {
    // Pull out some helpful variables
    SData& request = command.request;
    SData& response = command.response;

    // ----------------------------------------------------------------------
    if (SIEquals(request.getVerb(), "Query")) {
        if (db.getUpdateNoopMode()) {
            SINFO("Query run in mocked request, just ignoring.");
            return true;
        }
        // - Query( query )
        //
        //     Executes a simple read/write query
        //
        verifyAttributeSize(request, "query", 1, MAX_SIZE_QUERY);

        // Attempt the query
        const string& query = request["query"] + ";";
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
        return true;
    }

    // Didn't recognize this command
    return false;
}
