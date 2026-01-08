#include "DB.h"
#include "libstuff/libstuff.h"
#include "libstuff/sqlite3.h"

#include <cctype>
#include <string.h>
#include "libstuff/SQResultFormatter.h"
#include <BedrockServer.h>
#include <libstuff/SQResult.h>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

const string BedrockPlugin_DB::name("DB");
const string& BedrockPlugin_DB::getName() const
{
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

unique_ptr<BedrockCommand> BedrockPlugin_DB::getCommand(SQLiteCommand&& baseCommand)
{
    if (SStartsWith(SToLower(baseCommand.request.methodLine), "query:") || SIEquals(baseCommand.request.getVerb(), "Query")) {
        return make_unique<BedrockDBCommand>(move(baseCommand), this);
    }
    return nullptr;
}

int BedrockDBCommand::SQLiteFormatAppend(void* destString, const char* appendString, sqlite3_int64 length)
{
    string* output = static_cast<string*>(destString);
    output->append(reinterpret_cast<const char*>(appendString), length);
    return 0;
}

bool BedrockDBCommand::peek(SQLite& db)
{
    if (query.size() < 1 || query.size() > BedrockPlugin::MAX_SIZE_QUERY) {
        STHROW("402 Missing query");
    }

    // We build a set of render options from either `readDBFlags` or `SQLiteArgs`.
    // The first name is a legacy Expensify-internal name, and combined sqlite arguments with expensify tooling arguments.
    // The second is a more sqlite general name
    BedrockPlugin_DB::Sqlite3QRFSpecWrapper wrapper = BedrockPlugin_DB::parseSQLite3Args("");
    if (request.isSet("ReadDBFlags")) {
        wrapper = BedrockPlugin_DB::parseSQLite3Args(request["ReadDBFlags"]);
    }
    if (request.isSet("SQLiteArgs")) {
        wrapper = BedrockPlugin_DB::parseSQLite3Args(request["SQLiteArgs"]);
    }

    string output;
    wrapper.spec.xWrite = &SQLiteFormatAppend;
    wrapper.spec.pWriteArg = &output;

    // We support two extra flags.
    if (request.isSet("MYSQLFlags")) {
        wrapper.spec.bTitles = QRF_SW_Off;
        wrapper.zColumnSep->assign(" ");
        wrapper.zColumnSep->data()[0] = 0x1E;
        wrapper.zNull->assign("\n");
        wrapper.spec.zColumnSep = const_cast<char*>(wrapper.zColumnSep->c_str());
        wrapper.spec.zNull = const_cast<char*>(wrapper.zNull->c_str());
    }

    if (request.isSet("SuppressResult")) {
        wrapper.spec.eStyle = QRF_STYLE_Off;
    }

    // The `.schema` command (and other dot commands) isn't part of sqlite itself, but a convenience function built into the sqlite3 CLI.
    // This re-writes this into the internal query that does the same thing, and sets the format options to match the CLI.
    vector<string> matches;
    bool isSchema = SREMatch("\\s*\\.schema\\s+(.*?)\\s*", query, false, false, &matches);
    if (isSchema) {
        SINFO("Re-writing schema query for " + matches[1]);
        query = "SELECT sql FROM sqlite_schema WHERE tbl_name LIKE " + SQ(matches[1]) + ";";
        wrapper.spec.bTitles = 0;
        wrapper.spec.eStyle = QRF_STYLE_Column;
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
        string errorMessage = db.getLastError();
        int errorOffset = sqlite3_error_offset(db.getDBHandle());
        response["error"] = errorMessage;
        response.content = BedrockPlugin_DB::generateErrorContextMessage(query, errorMessage, errorOffset);
        STHROW("402 Bad query");
    }

    // If anything was a write, escalate to `process`.
    if (write) {
        return false;
    }

    // We rollback here because if we are in a transaction and the query takes long (which the queries in this command can)
    // it prevents sqlite from checkpointing and if we accumulate a lot of things to checkpoint, things become slow
    ((SQLite&) db).rollback();

    // Attempt the read-only query
    if (SIEquals(request["Format"], "json")) {
        SQResult result;
        if (!db.read(query, result)) {
            response["error"] = db.getLastError();
            STHROW("402 Bad query");
        }

        response.content = SQResultFormatter::format(result, SQResultFormatter::FORMAT::JSON);
    } else {
        int queryResult = db.read(query, &wrapper.spec);
        if (queryResult) {
            response["error"] = to_string(queryResult) + " " + sqlite3_errstr(queryResult);
            STHROW("402 Bad query");
        }

        // Worked! Set the output and return.
        response.content = output;
    }

    return true;
}

void BedrockDBCommand::process(SQLite& db)
{
    if (db.getUpdateNoopMode()) {
        SINFO("Query run in mocked request, just ignoring.");
        return;
    }
    BedrockPlugin::verifyAttributeBool(request, "nowhere", false);

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

BedrockPlugin_DB::Sqlite3QRFSpecWrapper BedrockPlugin_DB::parseSQLite3Args(const string& argsToParse)
{
    // Parse this into a map corresponding to the allowed options specified here:
    // https://sqlite.org/cli.html#command_line_options
    // Note that just because we parse all of these doesn't mean we support them.
    char quoteChar = 0;
    bool escaped = false;
    string currentArg;
    list<string> splitArgs;
    for (size_t currentOffset = 0; currentOffset < argsToParse.size(); currentOffset++) {
        if (escaped) {
            // Note that this is checked *before* `quoteChar`, meaning that escaped quotes inside
            // of quotes don't end the quoted string.
            currentArg += argsToParse[currentOffset];
            escaped = false;
        } else if (quoteChar) {
            // If we found the match to our quote char, we're no longer in quotes.
            if (argsToParse[currentOffset] == quoteChar) {
                quoteChar = 0;
            } else {
                // for any other character, we simply append it to the current string.
                currentArg += argsToParse[currentOffset];
            }
        } else {
            // If we're not in quotes, then a space signals the end of the current string.
            if (isspace(argsToParse[currentOffset])) {
                // Finish this string and move to the next, unless it's empty.
                if (currentArg.length()) {
                    splitArgs.push_back(currentArg);
                    currentArg = "";
                }
            } else {
                // Ok, so if it's not a space, then we care if it's a quote or escape character.
                if (argsToParse[currentOffset] == '\'' || argsToParse[currentOffset] == '"') {
                    // Start a quoted string.
                    quoteChar = argsToParse[currentOffset];
                } else if (argsToParse[currentOffset] == '\\') {
                    // Start an escape sequence.
                    escaped = true;
                } else {
                    // Any other character is part of the actual argument.
                    currentArg += argsToParse[currentOffset];
                }
            }
        }
    }

    // If we got to the end with data accumulated, we use that as the last argument. We don't bother validating for matched
    // quotes or unfinished escape sequences.
    if (!currentArg.empty()) {
        splitArgs.push_back(currentArg);
    }

    // Set all the defaults for our render spec.
    Sqlite3QRFSpecWrapper spec;
    spec.spec.iVersion = 1;
    spec.spec.eStyle = QRF_STYLE_List;
    spec.spec.eEsc = QRF_ESC_Auto;
    spec.spec.eText = QRF_TEXT_Auto;
    spec.spec.eTitle = QRF_TEXT_Auto;
    spec.spec.eBlob = QRF_TEXT_Auto;
    spec.spec.bTitles = QRF_SW_On;
    spec.spec.bWordWrap = QRF_SW_On;
    spec.spec.bTextJsonb = QRF_SW_On;
    spec.spec.eDfltAlign = 0;
    spec.spec.eTitleAlign = 0;
    spec.spec.nWrap = 10000;
    spec.spec.nScreenWidth = 10000;
    spec.spec.nLineLimit = 1;
    spec.spec.nCharLimit = 10000;
    spec.spec.nWidth = 0;
    spec.spec.nAlign = 0;
    spec.spec.aWidth = 0;
    spec.spec.aAlign = 0;
    spec.spec.zColumnSep = 0;
    spec.spec.zRowSep = 0;
    spec.spec.zTableName = 0;
    spec.spec.zNull = 0;
    spec.spec.xRender = 0;
    spec.spec.xWrite = 0;
    spec.spec.pRenderArg = 0;
    spec.spec.pWriteArg = 0;
    spec.spec.pzOutput = 0;

    // Now we should have a list of strings we can parse into actual usable data.
    string previous = "";
    for (auto it = splitArgs.begin(); it != splitArgs.end(); it++) {
        if (previous != "") {
            // handle argument values.
            if (previous == "-nullvalue" || previous == "--nullvalue") {
                spec.zNull->assign(*it);
                spec.spec.zNull = const_cast<char*>(spec.zNull->c_str());
            } else if (previous == "-separator" || previous == "--separator") {
                spec.zColumnSep->assign(*it);
                spec.spec.zColumnSep = const_cast<char*>(spec.zColumnSep->c_str());
            } else {
                // Ignore.
            }
            previous = "";
        } else {
            // Handle argument names.
            if (*it == "-ascii" || *it == "--ascii") {
                // Nothing to set.
            } else if (*it == "-box" || *it == "--box") {
                spec.spec.eStyle = QRF_STYLE_Box;
            } else if (*it == "-column" || *it == "--column") {
                spec.spec.eStyle = QRF_STYLE_Column;
            } else if (*it == "-csv" || *it == "--csv") {
                spec.spec.eStyle = QRF_STYLE_Csv;
            } else if (*it == "-header" || *it == "--header") {
                spec.spec.bTitles = QRF_SW_On;
            } else if (*it == "-noheader" || *it == "--noheader") {
                spec.spec.bTitles = QRF_SW_Off;
            } else if (*it == "-html" || *it == "--html") {
                spec.spec.eStyle = QRF_STYLE_Html;
            } else if (*it == "-json" || *it == "--json") {
                spec.spec.eStyle = QRF_STYLE_Json;
            } else if (*it == "-line" || *it == "--line") {
                spec.spec.eStyle = QRF_STYLE_Line;
            } else if (*it == "-list" || *it == "--list") {
                spec.spec.eStyle = QRF_STYLE_List;
            } else if (*it == "-markdown" || *it == "--markdown") {
                spec.spec.eStyle = QRF_STYLE_Markdown;
            } else if (*it == "-nullvalue" || *it == "--nullvalue") {
                previous = *it;
            } else if (*it == "-quote" || *it == "--quote") {
                spec.spec.eStyle = QRF_STYLE_Quote;
            } else if (*it == "-separator" || *it == "--separator") {
                previous = *it;
            } else if (*it == "-table" || *it == "--table") {
                spec.spec.eStyle = QRF_STYLE_Table;
            } else if (*it == "-tabs" || *it == "--tabs") {
                // Nothing to set.
            }
        }
    }

    // At this point, we've filled out the spec struct as much as we can.
    return spec;
}

string BedrockPlugin_DB::generateErrorContextMessage(const string& query, const string& errorMessage, int errorOffset) {
    if (errorOffset < 0) {
        return errorMessage;
    }

    // Move the snippet window forward until the error is within ~50 bytes of the start, taking care not to split UTF-8 continuation bytes.
    size_t sqlOffset = 0;
    while (errorOffset > 50) {
        errorOffset--;
        sqlOffset++;
        while ((query[sqlOffset] & 0xc0) == 0x80) {
            sqlOffset++;
            errorOffset--;
        }
    }

    // Determine how much of sqlText to show (cap at 78 bytes), again not splitting UTF-8 continuation bytes at the end.
    size_t snippetByteLength = query.length() - sqlOffset;
    if (snippetByteLength > 78) {
        snippetByteLength = 78;
        while (snippetByteLength > 0 && (query[sqlOffset + snippetByteLength - 1] & 0xc0) == 0x80) {
            snippetByteLength--;
        }
    }
    string sqlSnippet = query.substr(sqlOffset, snippetByteLength);

    // Replace any whitespace in the displayed snippet with spaces so the caret alignment is stable.
    for (char& c : sqlSnippet) {
        if (isspace(c)) {
            c = ' ';
        }
    }

    // Build the final two-line context message with a caret marker.
    string contextMessage = sqlSnippet + "\n";
    string spaces = "";
    if (errorOffset < 25) {
        for (int i = 0; i < errorOffset; i++) {
            spaces += " ";
        }
        contextMessage += spaces + "^--- error here";
    } else {
        for (int i = 0; i < errorOffset - 14; i++) {
            spaces += " ";
        }
        contextMessage += spaces + "error here ---^";
    }

    return errorMessage + "\n" + contextMessage + "\n";
}



BedrockPlugin_DB::Sqlite3QRFSpecWrapper::Sqlite3QRFSpecWrapper()
    : zColumnSep(new string),
    zNull(new string)
{
}

BedrockPlugin_DB::Sqlite3QRFSpecWrapper::~Sqlite3QRFSpecWrapper()
{
    delete zColumnSep;
    delete zNull;
}

BedrockPlugin_DB::Sqlite3QRFSpecWrapper::Sqlite3QRFSpecWrapper(Sqlite3QRFSpecWrapper&& other) noexcept
    : spec(other.spec),
    zColumnSep(other.zColumnSep),
    zNull(other.zNull)
{
    other.spec = sqlite3_qrf_spec{};
    other.zColumnSep = nullptr;
    other.zNull = nullptr;
}

BedrockPlugin_DB::Sqlite3QRFSpecWrapper& BedrockPlugin_DB::Sqlite3QRFSpecWrapper::operator=(BedrockPlugin_DB::Sqlite3QRFSpecWrapper&& other) noexcept
{
    if (this != &other) {
        delete zColumnSep;
        delete zNull;
        spec = other.spec;
        zColumnSep = other.zColumnSep;
        zNull = other.zNull;
        other.spec = sqlite3_qrf_spec{};
        other.zColumnSep = nullptr;
        other.zNull = nullptr;
    }
    return *this;
}
