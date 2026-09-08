/* SUMMARY ─────────────────────────────────────────────────────────────
 * File:    MySQL.h
 * Path:    plugins/MySQL.h
 * Pair:    MySQL.cpp
 *
 * INTENT
 *   Declares a Bedrock plugin that impersonates a MySQL server on the wire so
 *   unmodified MySQL clients (and tools such as Alteryx) can talk to a
 *   Bedrock/SQLite node. It owns its own TCP port and protocol framing rather
 *   than routing through the normal BedrockCommand pipeline.
 *
 * OBJECTS
 *   MySQLUtils (namespace)          - free functions that recognize and pick
 *                                      apart specific client query shapes
 *                                      (VERSION(), CONNECTION_ID(),
 *                                      information_schema.*, SHOW KEYS FROM,
 *                                      foreign-key introspection) so they can
 *                                      be rewritten into SQLite equivalents.
 *   MySQLPacket        - encodes/decodes the MySQL wire-protocol packet
 *                         header+payload framing; builds handshake, OK, ERR,
 *                         and tabular query-response packets.
 *   BedrockPlugin_MySQL - the plugin itself: opens the MySQL-protocol port,
 *                         accepts raw client requests, translates recognized
 *                         queries into internal "Query" commands (or answers
 *                         them directly), and converts the DB response back
 *                         into MySQL wire format. getCommand() always returns
 *                         null: this plugin has no BedrockCommand of its own.
 *   MYSQL_NUM_VARIABLES (macro)     - element count of g_MySQLVariables.
 *   g_MySQLVariables (extern)       - table of fake MySQL server variables
 *                                      returned for SHOW VARIABLES / @@var.
 *
 * OUT OF PLACE
 *   g_MySQLVariables [CANDIDATE] - a ~300-row hardcoded table of fake AWS-RDS
 *   MySQL server variables/values. It is pure static data, not protocol
 *   logic, and its bulk dominates this header/cpp; it would read better as a
 *   generated or external data file than as a C++ array declared alongside
 *   the plugin's actual behavior.
 *
 * NAME/LOCATION FIT
 *   Fits: it is the MySQL-protocol plugin, filed under plugins/ alongside
 *   the other BedrockPlugin_* implementations.
 *
 * NAMING QUALITY
 *   Consistent with repo convention: BedrockPlugin_<Name> for the plugin
 *   class, g_ prefix for the extern global. mysqlVersion (lowerCamel
 *   constexpr member) sits next to MYSQL_NUM_VARIABLES (a macro) and
 *   g_MySQLVariables (a global) — three different naming registers for
 *   related "MySQL protocol constant" concepts, but each follows its own
 *   category's convention correctly.
 * ─────────────────────────────────────────────────────────────────────*/
#pragma once
#include <libstuff/libstuff.h>
#include <BedrockPlugin.h>

// Forward declarations
class SQResult;
class BedrockServer;
class BedrockCommand;
class SQLiteCommand;
struct STCPManager;
struct SData;

/**
 * MySQL utility functions for parsing queries and extracting information.
 * These functions are used by the MySQL plugin and are exposed for unit testing.
 */
namespace MySQLUtils {
/**
 * Parses VERSION() queries and extracts alias information
 * @param query The SQL query to parse
 * @param matches [out] Vector to store regex matches (matches[1] will contain alias if present)
 * @return true if the query matches VERSION() pattern, false otherwise
 */
bool parseVersionQuery(const string& query, vector<string>& matches);

/**
 * Parses CONNECTION_ID() queries and extracts alias information
 * @param query The SQL query to parse
 * @param matches [out] Vector to store regex matches (matches[1] will contain alias if present)
 * @return true if the query matches CONNECTION_ID() pattern, false otherwise
 */
bool parseConnectionIdQuery(const string& query, vector<string>& matches);

/**
 * Extracts table name from information_schema.columns queries
 * @param query The SQL query to parse
 * @return The extracted table name, or empty string if not found
 */
string extractTableNameFromColumnsQuery(const string& query);

/**
 * Checks if a query is targeting information_schema.tables
 * @param query The SQL query to check
 * @return true if the query targets information_schema.tables
 */
bool isInformationSchemaTablesQuery(const string& query);

/**
 * Checks if a query is targeting information_schema.views
 * @param query The SQL query to check
 * @return true if the query targets information_schema.views
 */
bool isInformationSchemaViewsQuery(const string& query);

/**
 * Checks if a query is targeting information_schema.columns
 * @param query The SQL query to check
 * @return true if the query targets information_schema.columns
 */
bool isInformationSchemaColumnsQuery(const string& query);

/**
 * Checks if a query is a SHOW KEYS FROM query
 * @param query The SQL query to check
 * @return true if the query is a SHOW KEYS FROM query
 */
bool isShowKeysQuery(const string& query);

/**
 * Extracts table name from SHOW KEYS FROM queries
 * @param query The SQL query to parse
 * @return The extracted table name, or empty string if not found
 */
string extractTableNameFromShowKeysQuery(const string& query);

/**
 * Checks if a query is a foreign key constraint query (uses both KEY_COLUMN_USAGE and REFERENTIAL_CONSTRAINTS)
 * @param query The SQL query to check
 * @return true if the query is a foreign key constraint query
 */
bool isForeignKeyConstraintQuery(const string& query);

/**
 * Extracts table name from foreign key constraint queries
 * @param query The SQL query to parse
 * @return The extracted table name, or empty string if not found
 */
string extractTableNameFromForeignKeyQuery(const string& query);
}

/**
 * MySQL protocol packet handler
 */
class MySQLPacket {
public:
    MySQLPacket();

    // Attributes
    uint8_t sequenceID;
    string payload;

    // Methods
    string serialize();
    int deserialize(const char* packet, const size_t size);

    // Static helper methods
    static string lenEncInt(uint64_t val);
    static string lenEncStr(const string& str);
    static string serializeHandshake();
    static string serializeQueryResponse(int sequenceID, const SQResult& result);
    static string serializeOK(int sequenceID);
    static string serializeERR(int sequenceID, uint16_t code, const string& message);
};

/**
 * MySQL plugin for Bedrock
 */
class BedrockPlugin_MySQL : public BedrockPlugin {
public:
    BedrockPlugin_MySQL(BedrockServer& s);
    static const string name;
    static constexpr const char* mysqlVersion = "5.1.73-log";
    virtual const string& getName() const;
    virtual string getPort();
    virtual unique_ptr<BedrockCommand> getCommand(SQLiteCommand&& baseCommand);
    virtual void onPortAccept(STCPManager::Socket* s);
    virtual void onPortRecv(STCPManager::Socket* s, SData& request);
    virtual void onPortRequestComplete(const BedrockCommand& command, STCPManager::Socket* s);
};

// MySQL variables
#define MYSQL_NUM_VARIABLES 292
extern const char* g_MySQLVariables[MYSQL_NUM_VARIABLES][2];
