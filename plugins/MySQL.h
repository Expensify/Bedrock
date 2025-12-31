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
