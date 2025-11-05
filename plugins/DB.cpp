#include "DB.h"
#include "libstuff/libstuff.h"

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

ssize_t SQLiteFormatAppend(void* destString, const unsigned char* appendString, size_t length) {

    string* output = static_cast<string*>(destString);
    output->append(reinterpret_cast<const char*>(appendString), length);
    return 0;
}

bool BedrockDBCommand::peek(SQLite& db) {
    if (query.size() < 1 || query.size() > BedrockPlugin::MAX_SIZE_QUERY) {
        STHROW("402 Missing query");
    }

    // Read the flags that the readdb tool supports.
    list<string> readDBFlags;
    if (request.isSet("ReadDBFlags")) {
        readDBFlags = SParseList(request["ReadDBFlags"], ' ');
    }

    string output;
    sqlite3_qrf_spec spec;
    spec.iVersion = 1;     /* Version number of this structure */
    spec.eFormat = QRF_MODE_List;      /* Output format */
    spec.bShowCNames = 1;  /* True to show column names */
    spec.eEscape = 0;      /* How to deal with control characters */
    spec.eQuote = 0;       /* Quoting style for text */
    spec.eBlob = 0;        /* Quoting style for BLOBs */
    spec.bWordWrap = 1;    /* Try to wrap on word boundaries */
    spec.mxWidth = 0;      /* Maximum width of any column */
    spec.nWidth = 0;       /* Number of column width parameters */
    spec.aWidth = 0;       /* Column widths */
    spec.zColumnSep = 0;   /* Alternative column separator */
    spec.zRowSep = 0;      /* Alternative row separator */
    spec.zTableName = 0;   /* Output table name */
    spec.zNull = 0;        /* Rendering of NULL */
    spec.xRender = nullptr;    /* Render a value */
    spec.xWrite = &SQLiteFormatAppend; /* Write callback */
    spec.pRenderArg = nullptr;   /* First argument to the xRender callback */
    spec.pWriteArg = &output;    /* First argument to the xWrite callback */
    spec.pzOutput = nullptr;     /* Storage location for output string */

    // Set the format. Allow the legacy behavior for `format: json` if supplied.
    if (SIEquals(request["Format"], "json")) {
        spec.eFormat = QRF_MODE_Json;
    }
    for (auto flag : readDBFlags) {
        if (flag == "-column") {
            spec.eFormat = QRF_MODE_Column;
        }
        if (flag == "-csv") {
            spec.eQuote = QRF_TXT_Csv;
            spec.eFormat = QRF_MODE_Csv;
        }
        if (flag == "-tsv") {
            // TODO: Not yet implemented.
            //format = SQResultFormatter::FORMAT::TABS;
        }
        if (flag == "-json") {
            spec.eQuote = QRF_TXT_Json;
            spec.eFormat = QRF_MODE_Json;
        }
        if (flag == "-quote") {
            spec.eFormat = QRF_MODE_Quote;
        }
        if (flag == "-header") {
            spec.bShowCNames = 1;
        }
        if (flag == "-noheader") {
            spec.bShowCNames = 0;
        }
    }

    // The `.schema` command (and other dot commands) isn't part of sqlite itself, but a convenience function built into the sqlite3 CLI.
    // This re-writes this into the internal query that does the same thing, and sets the format options to match the CLI.
    vector<string> matches;
    bool isSchema = SREMatch("\\s*\\.schema\\s+(.*?)\\s*", query, false, false, &matches);
    if (isSchema) {
        SINFO("Re-writing schema query for " + matches[1]);
        query = "SELECT sql FROM sqlite_schema WHERE tbl_name LIKE " + SQ(matches[1]) + ";";
        spec.bShowCNames = 0;
        spec.eFormat = QRF_MODE_Column;
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

    // We rollback here because if we are in a transaction and the querytakes long (which the queries in this command can)
    // it prevents sqlite from checkpointing and if we accumulate a lot of things to checkpoint, things become slow
    ((SQLite&) db).rollback();

    // Attempt the read-only query
    SQResult result;
    if (!db.read(query, &spec)) {
        response["error"] = db.getLastError();
        STHROW("402 Bad query");
    }

    // Worked! Set the output and return.
    response.content = output;

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
