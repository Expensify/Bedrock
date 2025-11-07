#include "MySQL.h"

#include <bedrockVersion.h>
#include <libstuff/SQResult.h>
#include <BedrockServer.h>

#include <cstring>

#undef SLOGPREFIX
#define SLOGPREFIX "{" << getName() << "} "

// MySQL utility functions for query parsing and extraction
// These functions are used by the MySQL plugin and are exposed for unit testing
namespace MySQLUtils {

    bool parseVersionQuery(const string& query, vector<string>& matches) {
        string upperQuery = SToUpper(query);
        return SREMatch("^SELECT\\s+VERSION\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, &matches);
    }

    bool parseConnectionIdQuery(const string& query, vector<string>& matches) {
        string upperQuery = SToUpper(query);
        return SREMatch("^SELECT\\s+CONNECTION_ID\\(\\s*\\)(?:\\s+AS\\s+(\\w+))?\\s*;?$", upperQuery, false, false, &matches);
    }

    string extractTableNameFromColumnsQuery(const string& query) {
        string tableName;
        string upperQuery = SToUpper(query);

        // Find "TABLE_NAME = '" pattern
        size_t pos = upperQuery.find("TABLE_NAME");
        if (pos != string::npos) {
            // Find the equals sign
            size_t equalsPos = upperQuery.find("=", pos);
            if (equalsPos != string::npos) {
                // Find the opening quote after the equals
                size_t quoteStart = query.find_first_of("'\"", equalsPos);
                if (quoteStart != string::npos) {
                    char quoteChar = query[quoteStart];
                    // Find the closing quote
                    size_t quoteEnd = query.find(quoteChar, quoteStart + 1);
                    if (quoteEnd != string::npos) {
                        tableName = query.substr(quoteStart + 1, quoteEnd - quoteStart - 1);
                    }
                }
            }
        }
        return tableName;
    }

    bool isInformationSchemaTablesQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.TABLES");
    }

    bool isInformationSchemaViewsQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.VIEWS");
    }

    bool isInformationSchemaColumnsQuery(const string& query) {
        return SContains(SToUpper(query), "INFORMATION_SCHEMA.COLUMNS");
    }

    bool isShowKeysQuery(const string& query) {
        return SContains(SToUpper(query), "SHOW KEYS FROM");
    }

    string extractTableNameFromShowKeysQuery(const string& query) {
        string upperQuery = SToUpper(query);
        string tableName;

        // Look for patterns like "FROM `tablename`" or "FROM tablename"
        size_t fromPos = upperQuery.find("FROM");
        if (fromPos != string::npos) {
            size_t tableStart = query.find_first_not_of(" \t", fromPos + 4);
            if (tableStart != string::npos) {
                size_t tableEnd;
                if (query[tableStart] == '`') {
                    // Handle backtick-quoted table names like `bankAccounts`
                    tableStart++; // Skip opening backtick
                    tableEnd = query.find('`', tableStart);
                } else {
                    // Handle unquoted table names
                    tableEnd = query.find_first_of(" \t;", tableStart);
                    if (tableEnd == string::npos) tableEnd = query.length();
                }

                if (tableEnd != string::npos && tableEnd > tableStart) {
                    tableName = query.substr(tableStart, tableEnd - tableStart);
                }
            }
        }
        return tableName;
    }

    bool isForeignKeyConstraintQuery(const string& query) {
        string upperQuery = SToUpper(query);
        return SContains(upperQuery, "INFORMATION_SCHEMA.KEY_COLUMN_USAGE") &&
               SContains(upperQuery, "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS");
    }

    string extractTableNameFromForeignKeyQuery(const string& query) {
        vector<string> matches;
        // Look for pattern "table_name = 'value'" or "cu.table_name = 'value'" (with optional table alias)
        if (SREMatch("(?:\\w+\\.)?table_name\\s*=\\s*['\"]([^'\"]+)['\"]", query, false, true, &matches) && matches.size() > 1) {
            return matches[1]; // matches[1] is the captured group, matches[0] is the entire match
        }
        return "";
    }

} // namespace MySQLUtils

const string BedrockPlugin_MySQL::name("MySQL");
const string& BedrockPlugin_MySQL::getName() const {
    return name;
}

MySQLPacket::MySQLPacket() {
    // Initialize
    sequenceID = 0;
}

string MySQLPacket::serialize() {
    // Wrap in a 3-byte header
    uint32_t payloadLength = payload.size();
    string header;
    header.resize(4);
    memcpy(&header[0], &payloadLength, 3);
    header[3] = sequenceID;
    return header + payload;
}

int MySQLPacket::deserialize(const char* packet, const size_t size) {
    // Does it have a header?
    if (size < 4) {
        return 0;
    }

    // Has a header, parse it out
    uint32_t payloadLength = (*(uint32_t*)&packet[0]) & 0x00FFFFFF; // 3 bytes
    sequenceID = (uint8_t)packet[3];

    // Do we have enough data for the full payload?
    if (size < (4 + payloadLength)) {
        return 0;
    }

    // Have the full payload, parse it out
    payload = string(packet + 4, payloadLength);

    // Indicate that we've consumed this full packet
    return 4 + payloadLength;
}

string MySQLPacket::lenEncInt(uint64_t val) {
    // Encode based on the length.
    // **NOTE: The below assume this is running on a "little-endian"
    //         machine, which means the least significant byte comes first
    string out;
    void* valPtr = &val;
    if (val < 251) {
        // Take the last byte
        SAppend(out, valPtr, 1);
    } else if (val < 1 << 16) {
        // Take the last 2 bytes
        out += "\xFC";
        SAppend(out, valPtr, 2);
    } else if (val < 1 << 24) {
        // Take the last 3 bytes
        out += "\xFD";
        SAppend(out, valPtr, 3);
    } else {
        // Take all bytes
        out += "\xFE";
        SAppend(out, valPtr, sizeof(val));
    }
    return out;
}

string MySQLPacket::lenEncStr(const string& str) {
    // Add the length, and then the string
    return lenEncInt(str.size()) + str;
}

string MySQLPacket::serializeHandshake() {
    // Protocol described here:
    // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake

    // Just hard code the values for now
    MySQLPacket handshake;
    handshake.payload += lenEncInt(10);      // protocol version
    handshake.payload += BedrockPlugin_MySQL::mysqlVersion; // server version
    handshake.payload += lenEncInt(0);       // NULL
    uint32_t connectionID = 1;
    SAppend(handshake.payload, &connectionID, 4); // connection_id
    handshake.payload += "xxxxxxxx"s;     // auth_plugin_data_part_1
    handshake.payload += lenEncInt(0);            // filler

    uint32_t CLIENT_LONG_PASSWORD = 0x00000001;
    uint32_t CLIENT_PROTOCOL_41   = 0x00000200;
    uint32_t CLIENT_PLUGIN_AUTH   = 0x00080000;
    uint32_t capability_flags = CLIENT_LONG_PASSWORD | CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH;

    uint16_t capability_flags_1 = (const unsigned short)(capability_flags);
    uint16_t capability_flags_2 = (const unsigned short)(capability_flags >> 16);
    SAppend(handshake.payload, &capability_flags_1, 2); // capability_flags_1 (low 2 bytes)

    uint8_t latin1_swedish_ci = 0x08;
    SAppend(handshake.payload, &latin1_swedish_ci, 1); // character_set

    uint16_t SERVER_STATUS_AUTOCOMMIT = 0x0002;
    SAppend(handshake.payload, &SERVER_STATUS_AUTOCOMMIT, 2); // status_flags

    SAppend(handshake.payload, &capability_flags_2, 2); // capability_flags_2 (high 2 bytes)

    // The first byte is the length of the auth_plugin_name string. Followed by 10 NULL
    // characters for the "reserved" field. Since we don't support CLIENT_SECURE_CONNECTION
    // in our capabilities we can skip auth-plugin-data-part-2
    // https://dev.mysql.com/doc/internals/en/client-wants-native-server-wants-old.html
    // (Initial Handshake Packet)
    uint8_t auth_plugin_data[] = {
        0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00 };

    SAppend(handshake.payload, auth_plugin_data, sizeof(auth_plugin_data));

    handshake.payload += "mysql_native_password"s; // auth_plugin_name

    return handshake.serialize();
}

string MySQLPacket::serializeQueryResponse(int sequenceID, const SQResult& result) {
    // Add the response
    string sendBuffer;

    // First the column count
    MySQLPacket columnCount;
    columnCount.sequenceID = ++sequenceID;
    columnCount.payload = lenEncInt(result.headers.size());
    sendBuffer += columnCount.serialize();

    // Add all the columns
    for (const auto& header : result.headers) {
        // Now a column description
        MySQLPacket column;
        column.sequenceID = ++sequenceID;
        column.payload += lenEncStr("def");     // catalog (lenenc_str) -- catalog (always "def")
        column.payload += lenEncStr("unknown"); // schema (lenenc_str) -- schema-name
        column.payload += lenEncStr("unknown"); // table (lenenc_str) -- virtual table-name
        column.payload += lenEncStr("unknown"); // org_table (lenenc_str) -- physical table-name
        column.payload += lenEncStr(header);    // name (lenenc_str) -- virtual column name
        column.payload += lenEncStr(header);    // org_name (lenenc_str) -- physical column name

        uint8_t next_length = 0x0c;
        SAppend(column.payload, &next_length, 1); // next_length (lenenc_int) -- length of the following fields (always 0x0c)

        uint16_t latin1_swedish_ci = 0x08;
        SAppend(column.payload, &latin1_swedish_ci, 2); // character_set (2) -- is the column character set and is defined in Protocol::CharacterSet.

        uint32_t colLength = 1024;
        SAppend(column.payload, &colLength, 4); // column_length (4) -- maximum length of the field

        //uint8_t colType = 0; // Decimal;
        uint8_t colType = 254; // string.
        SAppend(column.payload, &colType, 1); // column_type (1) -- type of the column as defined in Column Type

        uint16_t flags = 0;
        SAppend(column.payload, &flags, 2); // flags (2) -- flags

        uint8_t decimals = 0;
        SAppend(column.payload, &decimals, 1); // decimals (1) -- max shown decimal digits, 0x00 for integers and static strings

        uint16_t filler = 0;
        SAppend(column.payload, &filler, 2); // filler (to pad to 0x0c)

        sendBuffer += column.serialize();
    }

    // EOF packet to signal no more columns
    MySQLPacket eofPacket;
    eofPacket.sequenceID = ++sequenceID;
    SAppend(eofPacket.payload, "\xFE", 1); // EOF
    uint32_t zero = 0;
    SAppend(eofPacket.payload, &zero, 4); // EOF
    sendBuffer += eofPacket.serialize();

    // Add all the rows
    for (const auto& row : result) {
        // Now the row
        MySQLPacket rowPacket;
        rowPacket.sequenceID = ++sequenceID;
        for (const auto& cell : row) {
            rowPacket.payload += lenEncStr(cell);
        }
        SAppend(rowPacket.payload, "\xFE", 1); // EOF
        sendBuffer += rowPacket.serialize();
    }

    // Finish with another EOF packet
    eofPacket.sequenceID = ++sequenceID;
    sendBuffer += eofPacket.serialize();

    // Done!
    return sendBuffer;
}

string MySQLPacket::serializeOK(int sequenceID) {
    // Just fill out the packet
    MySQLPacket ok;
    ok.sequenceID = sequenceID + 1;
    ok.payload += lenEncInt(0); // OK
    ok.payload += lenEncInt(0); // Affected rows
    ok.payload += lenEncInt(0); // Last insert ID

    uint16_t SERVER_STATUS_AUTOCOMMIT = 0x0002;
    SAppend(ok.payload, &SERVER_STATUS_AUTOCOMMIT, 2); // status_flags
    uint16_t WARNING_COUNT = 0x0;
    SAppend(ok.payload, &WARNING_COUNT, 2); // required for protocol 4.1

    ok.payload += "OK";         // Message
    return ok.serialize();
}

string MySQLPacket::serializeERR(int sequenceID, uint16_t code, const string& message) {
    // Fill it with our custom error message
    MySQLPacket err;
    err.sequenceID = sequenceID + 1;
    err.payload += "\xFF";                     // Header of the ERR packet
    SAppend(err.payload, &code, sizeof(code)); // Error code
    err.payload += message;                    // Error message
    return err.serialize();
}

BedrockPlugin_MySQL::BedrockPlugin_MySQL(BedrockServer& s) : BedrockPlugin(s)
{
}

string BedrockPlugin_MySQL::getPort() {
    return server.args.isSet("-mysql.host") ? server.args["-mysql.host"] : "localhost:3306";
}

// This plugin supports no commands.
unique_ptr<BedrockCommand> BedrockPlugin_MySQL::getCommand(SQLiteCommand&& baseCommand) {
    return nullptr;
}

void BedrockPlugin_MySQL::onPortAccept(STCPManager::Socket* s) {
    // Send Protocol::HandshakeV10
    SINFO("Accepted MySQL request from '" << s->addr << "'");
    s->send(MySQLPacket::serializeHandshake());
}

void BedrockPlugin_MySQL::onPortRecv(STCPManager::Socket* s, SData& request) {
    // Get any new MySQL requests
    int packetSize = 0;
    MySQLPacket packet;
    while ((packetSize = packet.deserialize(s->recvBuffer.c_str(), s->recvBuffer.size()))) {
        // Got a packet, process it
        SDEBUG("Received command #" << packet.payload[0] << ", sequenceID #" << (int)packet.sequenceID << " : '" << SToHex(packet.serialize()) << "'");
        s->recvBuffer.consumeFront(packetSize);
        SDEBUG("Packet payload " + packet.payload);
        switch (packet.payload[0]) {
        case 3: { // COM_QUERY
            // Decode the query
            string query = STrim(packet.payload.substr(1, packet.payload.size() - 1));
            if (!SEndsWith(query, ";")) {
                // We translate our query to one we can pass to `DB`, for which this is mandatory.
                query += ";";
            }
            // JDBC Does this.
            if (SStartsWith(query, "/*")) {
                auto index = query.find("*/");
                if (index != query.npos) {
                    query = query.substr(index + 2);
                }
            }
            SINFO("Processing query '" << query << "'");

            // See if it's asking for a global variable
            string regExp = "^(?:(?:SELECT\\s+)?@@(?:\\w+\\.)?|SHOW VARIABLES LIKE ')(\\w+).*$";
            vector<string> matches;
            if (SREMatch(regExp, query, false, false, &matches)) {
                string varName = matches[0];
                // Loop across and look for it
                vector<SQResultRow> rows;
                for (int c = 0; c < MYSQL_NUM_VARIABLES; ++c) {
                    if (SIEquals(g_MySQLVariables[c][0], varName)) {
                        // Found it!
                        SINFO("Returning variable '" << varName << "'='" << g_MySQLVariables[c][1] << "'");
                        SQResultRow row;
                        row.push_back(g_MySQLVariables[c][1]);
                        rows.push_back(row);
                        break;
                    }
                }
                if (rows.empty()) {
                    SHMMM("Couldn't find variable '" << varName << "', returning empty.");
                }
                vector<string> headers = {varName};
                SQResult result(move(rows), move(headers));
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SIEquals(query, "SHOW VARIABLES;")) {
                // Return the variable list
                SINFO("Responding with fake variable list");
                vector<SQResultRow> rows;
                for (int c = 0; c < MYSQL_NUM_VARIABLES; ++c) {
                    SQResultRow row;
                    row.push_back(g_MySQLVariables[c][0]);
                    row.push_back(g_MySQLVariables[c][1]);
                    rows.push_back(row);
                }
                vector<string> headers = {"Variable Name", "Value"};
                SQResult result(move(rows), move(headers));
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SIEquals(query, "SHOW DATABASES;") ||
                       SIEquals(SToUpper(query), "SELECT DATABASE();") ||
                       SIEquals(SToUpper(query), "SELECT * FROM (SELECT DATABASE() AS DATABASE_NAME) A WHERE A.DATABASE_NAME IS NOT NULL;")) {
                // Return a fake "main" database
                SINFO("Responding with fake database list");
                SQResultRow row;
                row.push_back("main");
                vector<SQResultRow> rows = {row};
                vector<string> headers = {"Database"};
                SQResult result(move(rows), move(headers));
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SIEquals(SToUpper(query), "SHOW /*!50002 FULL*/ TABLES;") ||
                       SIEquals(SToUpper(query), "SHOW FULL TABLES;")) {
                SINFO("Getting table list");

                // Transform this into an internal request
                request.methodLine = "Query";
                request["ReadDBFlags"] = "-json";
                request["sequenceID"] = SToStr(packet.sequenceID);
                request["query"] =
                    "SELECT "
                        "name as Tables_in_main, "
                        "CASE type "
                            "WHEN 'table' THEN 'BASE TABLE' "
                            "WHEN 'view' THEN 'VIEW' "
                        "END as Table_type "
                    "FROM sqlite_master "
                    "WHERE type IN ('table', 'view');";
            } else if (SIEquals(SToUpper(query),
                                "SELECT TABLE_NAME,TABLE_COMMENT,IF(TABLE_TYPE='BASE TABLE', 'TABLE', "
                                "TABLE_TYPE),TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=DATABASE() "
                                "AND ( TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW' ) AND TABLE_NAME LIKE '%' ORDER BY "
                                "TABLE_SCHEMA, TABLE_NAME;")) {
                // This is the query Alteryx uses to get Table information to display in the GUI, so let's support it.
                // This likely isn't restricted to just Alteryx, becuase it uses a generic ODBC connector from MySQL,
                // but that is the only client we have tested that has sent this query.
                SINFO("Getting table list");

                // Transform this into an internal request
                request.methodLine = "Query";
                request["ReadDBFlags"] = "-json";
                request["sequenceID"] = SToStr(packet.sequenceID);
                request["query"] =
                    "SELECT "
                        "name as TABLE_NAME, "
                        "'' as TABLE_COMMENT, "
                        "UPPER(type) as TABLE_TYPE, "
                        "'main' as TABLE_SCHEMA "
                    "FROM sqlite_master "
                    "WHERE type IN ('table', 'view') "
                    "ORDER BY TABLE_SCHEMA, TABLE_NAME;";
            } else if (MySQLUtils::isInformationSchemaTablesQuery(query)) {
                // Handle information_schema.tables queries for table listing
                SINFO("Processing information_schema.tables query for table listing");

                // Transform this into an internal request to get table names from sqlite_master
                request.methodLine = "Query";
                request["ReadDBFlags"] = "-json";
                request["sequenceID"] = SToStr(packet.sequenceID);
                request["query"] =
                    "SELECT name as name "
                    "FROM sqlite_master "
                    "WHERE type = 'table' "
                    "ORDER BY name;";
            } else if (MySQLUtils::isInformationSchemaViewsQuery(query)) {
                // Handle information_schema.views queries for view listing
                SINFO("Processing information_schema.views query for view listing");

                // Transform this into an internal request to get view names from sqlite_master
                request.methodLine = "Query";
                request["ReadDBFlags"] = "-json";
                request["sequenceID"] = SToStr(packet.sequenceID);
                request["query"] =
                    "SELECT name as name "
                    "FROM sqlite_master "
                    "WHERE type = 'view' "
                    "ORDER BY name;";
            } else if (MySQLUtils::isInformationSchemaColumnsQuery(query)) {
                // Handle information_schema.columns queries for column listing
                SINFO("Processing information_schema.columns query for column listing");

                // Extract table name from the query
                string tableName = MySQLUtils::extractTableNameFromColumnsQuery(query);

                if (!tableName.empty()) {
                    SDEBUG("Extracted table name: '" << tableName << "'");

                    // Transform this into an internal request to get column info using PRAGMA table_info
                    request.methodLine = "Query";
                    request["ReadDBFlags"] = "-json";
                    request["sequenceID"] = SToStr(packet.sequenceID);
                    request["query"] =
                        "SELECT "
                            + SQ(tableName) + " as table_name, "
                            "name as column_name, "
                            "type as column_type, "
                            "type as data_type, "
                            "CASE WHEN \"notnull\" = 0 THEN 'YES' ELSE 'NO' END as is_nullable, "
                            "dflt_value as column_default, "
                            "(cid + 1) as ordinal_position, "
                            "NULL as column_comment, "
                            "CASE WHEN pk = 1 THEN 'auto_increment' ELSE NULL END as extra "
                        "FROM pragma_table_info(" + SQ(tableName) + ") "
                        "ORDER BY cid;";
                } else {
                    // If we can't extract table name, return empty result
                    SWARN("Could not extract table name from columns query, returning empty result", {{"query", query}});
                    SQResult result;
                    s->send(MySQLPacket::serializeERR(packet.sequenceID, 1046, "Could not extract table name from columns query"));
                }
            } else if (SContains(query, "information_schema")) {
                // Return an empty set for other information_schema queries
                SINFO("Responding with empty result for information_schema query");
                SQResult result;
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (SStartsWith(SToUpper(query), "SET ") || SStartsWith(SToUpper(query), "USE ") ||
                       SIEquals(query, "ROLLBACK;")) {
                // Ignore
                SINFO("Responding OK to SET/USE/ROLLBACK query.");
                s->send(MySQLPacket::serializeOK(packet.sequenceID));
            } else if (SIEquals(SToUpper(query), "SELECT $$;")) {
                // Some new clients send this through and expect an OK, non-empty string
                // response or else the client will hang.
                SINFO("Responding OK to $$ query.");
                s->send(MySQLPacket::serializeOK(packet.sequenceID));
            } else if (MySQLUtils::parseVersionQuery(query, matches)) {
                // Return our fake version - handles SELECT VERSION(); and SELECT VERSION() AS alias;
                SINFO("Responding fake version string");
                SQResultRow row;
                row.push_back(BedrockPlugin_MySQL::mysqlVersion);
                vector<SQResultRow> rows = {row};

                // Extract the alias if present, otherwise use default column name
                string columnName = "version()";
                if (matches.size() > 1) {
                    columnName = SToLower(matches[1]); // Use the alias if provided
                }

                vector<string> headers = {columnName};
                SQResult result(move(rows), move(headers));
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (MySQLUtils::parseConnectionIdQuery(query, matches)) {
                // Return connection ID - handles SELECT connection_id(); and SELECT connection_id() AS alias;
                SINFO("Responding with connection ID");
                SQResultRow row;
                row.push_back("1"); // Return a simple connection ID
                vector<SQResultRow> rows = {row};

                // Extract the alias if present, otherwise use default column name
                string columnName = "connection_id()";
                if (matches.size() > 1) {
                    columnName = SToLower(matches[1]); // Use the alias if provided
                }

                vector<string> headers = {columnName};
                SQResult result(move(rows), move(headers));
                s->send(MySQLPacket::serializeQueryResponse(packet.sequenceID, result));
            } else if (MySQLUtils::isShowKeysQuery(query)) {
                // Handle SHOW KEYS FROM table queries
                SINFO("Processing SHOW KEYS query for table indexes");

                // Extract table name from SHOW KEYS FROM query
                string tableName = MySQLUtils::extractTableNameFromShowKeysQuery(query);

                if (!tableName.empty()) {
                    SINFO("Extracted table name for SHOW KEYS: '" << tableName << "'");

                    // Transform this into an internal request to get primary key info
                    request.methodLine = "Query";
                    request["ReadDBFlags"] = "-json";
                    request["sequenceID"] = SToStr(packet.sequenceID);
                    request["query"] =
                        "SELECT "
                            + SQ(tableName) + " as Table_name, "
                            "0 as Non_unique, "
                            "'PRIMARY' as Key_name, "
                            "(cid + 1) as Seq_in_index, "
                            "name as Column_name, "
                            "'A' as Collation, "
                            "NULL as Cardinality, "
                            "NULL as Sub_part, "
                            "NULL as Packed, "
                            "CASE WHEN \"notnull\" = 1 THEN '' ELSE 'YES' END as Null_col, "
                            "'BTREE' as Index_type, "
                            "'' as Comment, "
                            "'' as Index_comment "
                        "FROM pragma_table_info(" + SQ(tableName) + ") "
                        "WHERE pk = 1 "
                        "ORDER BY cid;";
                } else {
                    // If we can't extract table name, return empty result
                    SWARN("Could not extract table name from SHOW KEYS query, returning empty result", {{"query", query}});
                    SQResult result;
                    s->send(MySQLPacket::serializeERR(packet.sequenceID, 1046, "Could not extract table name from SHOW KEYS query"));
                }
            } else if (MySQLUtils::isForeignKeyConstraintQuery(query)) {
                // Handle foreign key constraint queries
                SINFO("Processing information_schema foreign key constraints query");

                // Extract table name from the query
                string tableName = MySQLUtils::extractTableNameFromForeignKeyQuery(query);

                if (!tableName.empty()) {
                    SINFO("Extracted table name for foreign key query: '" << tableName << "'");

                    // Transform this into an internal request to get foreign key info using PRAGMA foreign_key_list
                    request.methodLine = "Query";
                    request["ReadDBFlags"] = "-json";
                    request["sequenceID"] = SToStr(packet.sequenceID);
                    request["query"] =
                        "SELECT "
                            "'fk_' || \"table\" || '_' || \"from\" as constraint_name, "
                            "\"from\" as column_name, "
                            "\"table\" as referenced_table_name, "
                            "'FOREIGN' as key_type, "
                            "\"table\" as referenced_table, "
                            "\"to\" as referenced_column, "
                            "CASE WHEN on_update = 'NO ACTION' THEN 'RESTRICT' ELSE on_update END as on_update, "
                            "CASE WHEN on_delete = 'NO ACTION' THEN 'RESTRICT' ELSE on_delete END as on_delete, "
                            "'fk_' || \"table\" || '_' || \"from\" as rc_constraint_name, "
                            "(seq + 1) as ordinal_position "
                        "FROM pragma_foreign_key_list(" + SQ(tableName) + ") "
                        "ORDER BY \"table\", seq;";
                } else {
                    // If we can't extract table name, return empty result
                    SWARN("Could not extract table name from foreign key query, returning empty result", {{"query", query}});
                    SQResult result;
                    s->send(MySQLPacket::serializeERR(packet.sequenceID, 1046, "Could not extract table name from foreign key query"));
                }
            } else {
                // Transform this into an internal request
                request.methodLine = "Query";
                request["ReadDBFlags"] = "-json";
                request["sequenceID"] = SToStr(packet.sequenceID);
                request["query"] = query;
            }
            break;
        }

        default: { // Say OK to everything else
            // Send OK
            SINFO("Sending OK");
            s->send(MySQLPacket::serializeOK(packet.sequenceID));
            break;
        }
        }
    }
}

void BedrockPlugin_MySQL::onPortRequestComplete(const BedrockCommand& command, STCPManager::Socket* s) {
    // Only one request supported: Query.
    SASSERT(SIEquals(command.request.methodLine, "Query"));
    SASSERT(command.request.isSet("sequenceID"));
    if (SToInt(command.response.methodLine) == 200) {
        // Success!  Were there any results?
        if (command.response.content.empty()) {
            // Just send OK
            s->send(MySQLPacket::serializeOK(command.request.calc("sequenceID")));
        } else {
            // Convert the JSON response from Bedrock::DB into MySQL protocol
            SQResult result;
            SASSERT(command.response.content.empty() || result.deserialize(command.response.content));
            s->send(MySQLPacket::serializeQueryResponse(command.request.calc("sequenceID"), result));
        }
    } else {
        // Failure -- pass along the message
        int64_t errorCode = SToInt64(SBefore(command.response.methodLine, " "));
        string errorMessage = SAfter(command.response.methodLine, " ");
        string sqliteError = command.response["error"];
        s->send(MySQLPacket::serializeERR(command.request.calc("sequenceID"), errorCode, errorMessage + " " + sqliteError));
    }
}

// Define the global variable list to pretend to be MySQL
const char* g_MySQLVariables[MYSQL_NUM_VARIABLES][2] = {
    {"auto_increment_increment", "1"},
    {"auto_increment_offset", "1"},
    {"autocommit", "ON"},
    {"automatic_sp_privileges", "ON"},
    {"back_log", "50"},
    {"basedir", "/rdsdbbin/mysql-5.1.73a.R1/"},
    {"big_tables", "OFF"},
    {"binlog_cache_size", "32768"},
    {"binlog_direct_non_transactional_updates", "OFF"},
    {"binlog_format", "MIXED"},
    {"bulk_insert_buffer_size", "8388608"},
    {"character_set_client", "latin1"},
    {"character_set_connection", "latin1"},
    {"character_set_database", "utf8"},
    {"character_set_filesystem", "binary"},
    {"character_set_results", "latin1"},
    {"character_set_server", "latin1"},
    {"character_set_system", "utf8"},
    {"character_sets_dir", "/rdsdbbin/mysql-5.1.73a.R1/share/mysql/charsets/"},
    {"collation_connection", "latin1_swedish_ci"},
    {"collation_database", "utf8_unicode_ci"},
    {"collation_server", "latin1_swedish_ci"},
    {"completion_type", "0"},
    {"concurrent_insert", "1"},
    {"connect_timeout", "15"},
    {"datadir", "/rdsdbdata/db/"},
    {"date_format", "%Y-%m-%d"},
    {"datetime_format", "%Y-%m-%d %H:%i:%s"},
    {"default_week_format", "0"},
    {"delay_key_write", "ON"},
    {"delayed_insert_limit", "100"},
    {"delayed_insert_timeout", "300"},
    {"delayed_queue_size", "1000"},
    {"div_precision_increment", "4"},
    {"engine_condition_pushdown", "ON"},
    {"error_count", "0"},
    {"event_scheduler", "OFF"},
    {"expire_logs_days", "0"},
    {"flush", "OFF"},
    {"flush_time", "0"},
    {"foreign_key_checks", "ON"},
    {"ft_boolean_syntax", "+ -><()~*:"
                          "&|"},
    {"ft_max_word_len", "84"},
    {"ft_min_word_len", "4"},
    {"ft_query_expansion_limit", "20"},
    {"ft_stopword_file", "(built-in)"},
    {"general_log", "OFF"},
    {"general_log_file", "/rdsdbdata/log/general/mysql-general.log"},
    {"group_concat_max_len", "1024"},
    {"have_community_features", "YES"},
    {"have_compress", "YES"},
    {"have_crypt", "YES"},
    {"have_csv", "YES"},
    {"have_dynamic_loading", "YES"},
    {"have_geometry", "YES"},
    {"have_innodb", "YES"},
    {"have_ndbcluster", "NO"},
    {"have_openssl", "YES"},
    {"have_partitioning", "YES"},
    {"have_query_cache", "YES"},
    {"have_rtree_keys", "YES"},
    {"have_ssl", "YES"},
    {"have_symlink", "YES"},
    {"hostname", "ip-10-178-20-210"},
    {"identity", "0"},
    {"ignore_builtin_innodb", "ON"},
    {"init_connect", ""},
    {"init_file", ""},
    {"init_slave", ""},
    {"innodb_adaptive_flushing", "ON"},
    {"innodb_adaptive_hash_index", "ON"},
    {"innodb_additional_mem_pool_size", "2097152"},
    {"innodb_autoextend_increment", "8"},
    {"innodb_autoinc_lock_mode", "1"},
    {"innodb_buffer_pool_size", "11674845184"},
    {"innodb_change_buffering", "inserts"},
    {"innodb_checksums", "ON"},
    {"innodb_commit_concurrency", "0"},
    {"innodb_concurrency_tickets", "500"},
    {"innodb_data_file_path", "ibdata1:10M:autoextend"},
    {"innodb_data_home_dir", "/rdsdbdata/db/innodb"},
    {"innodb_doublewrite", "ON"},
    {"innodb_fast_shutdown", "1"},
    {"innodb_file_format", "Antelope"},
    {"innodb_file_format_check", "Barracuda"},
    {"innodb_file_per_table", "ON"},
    {"innodb_flush_log_at_trx_commit", "0"},
    {"innodb_flush_method", "O_DIRECT"},
    {"innodb_force_recovery", "0"},
    {"innodb_io_capacity", "200"},
    {"innodb_lock_wait_timeout", "50"},
    {"innodb_locks_unsafe_for_binlog", "OFF"},
    {"innodb_log_buffer_size", "8388608"},
    {"innodb_log_file_size", "134217728"},
    {"innodb_log_files_in_group", "2"},
    {"innodb_log_group_home_dir", "/rdsdbdata/log/innodb"},
    {"innodb_max_dirty_pages_pct", "75"},
    {"innodb_max_purge_lag", "0"},
    {"innodb_mirrored_log_groups", "1"},
    {"innodb_old_blocks_pct", "37"},
    {"innodb_old_blocks_time", "0"},
    {"innodb_open_files", "300"},
    {"innodb_random_read_ahead", "OFF"},
    {"innodb_read_ahead_threshold", "56"},
    {"innodb_read_io_threads", "4"},
    {"innodb_replication_delay", "0"},
    {"innodb_rollback_on_timeout", "OFF"},
    {"innodb_spin_wait_delay", "6"},
    {"innodb_stats_method", "nulls_equal"},
    {"innodb_stats_on_metadata", "ON"},
    {"innodb_stats_sample_pages", "8"},
    {"innodb_strict_mode", "OFF"},
    {"innodb_support_xa", "ON"},
    {"innodb_sync_spin_loops", "30"},
    {"innodb_table_locks", "ON"},
    {"innodb_thread_concurrency", "0"},
    {"innodb_thread_sleep_delay", "10000"},
    {"innodb_use_sys_malloc", "ON"},
    {"innodb_version", "5.1.73"},
    {"innodb_write_io_threads", "4"},
    {"insert_id", "0"},
    {"interactive_timeout", "28800"},
    {"join_buffer_size", "131072"},
    {"keep_files_on_create", "OFF"},
    {"key_buffer_size", "16777216"},
    {"key_cache_age_threshold", "300"},
    {"key_cache_block_size", "1024"},
    {"key_cache_division_limit", "100"},
    {"language", "/rdsdbbin/mysql/share/mysql/english/"},
    {"large_files_support", "ON"},
    {"large_page_size", "0"},
    {"large_pages", "OFF"},
    {"last_insert_id", "0"},
    {"lc_time_names", "en_US"},
    {"license", "GPL"},
    {"local_infile", "ON"},
    {"locked_in_memory", "OFF"},
    {"log", "OFF"},
    {"log_bin", "ON"},
    {"log_bin_trust_function_creators", "ON"},
    {"log_bin_trust_routine_creators", "ON"},
    {"log_error", "/rdsdbdata/log/error/mysql-error.log"},
    {"log_output", "TABLE"},
    {"log_queries_not_using_indexes", "OFF"},
    {"log_slave_updates", "OFF"},
    {"log_slow_queries", "ON"},
    {"log_warnings", "1"},
    {"long_query_time", "10.000000"},
    {"low_priority_updates", "OFF"},
    {"lower_case_file_system", "OFF"},
    {"lower_case_table_names", "0"},
    {"max_allowed_packet", "5242880"},
    {"max_binlog_cache_size", "18446744073709547520"},
    {"max_binlog_size", "134217728"},
    {"max_connect_errors", "1000000000"},
    {"max_connections", "1500"},
    {"max_delayed_threads", "20"},
    {"max_error_count", "64"},
    {"max_heap_table_size", "16777216"},
    {"max_insert_delayed_threads", "20"},
    {"max_join_size", "18446744073709551615"},
    {"max_length_for_sort_data", "1024"},
    {"max_long_data_size", "5242880"},
    {"max_prepared_stmt_count", "16382"},
    {"max_relay_log_size", "0"},
    {"max_seeks_for_key", "18446744073709551615"},
    {"max_sort_length", "1024"},
    {"max_sp_recursion_depth", "0"},
    {"max_tmp_tables", "32"},
    {"max_user_connections", "600"},
    {"max_write_lock_count", "18446744073709551615"},
    {"min_examined_row_limit", "0"},
    {"multi_range_count", "256"},
    {"myisam_data_pointer_size", "6"},
    {"myisam_max_sort_file_size", "9223372036853727232"},
    {"myisam_mmap_size", "18446744073709551615"},
    {"myisam_recover_options", "OFF"},
    {"myisam_repair_threads", "1"},
    {"myisam_sort_buffer_size", "8388608"},
    {"myisam_stats_method", "nulls_unequal"},
    {"myisam_use_mmap", "OFF"},
    {"net_buffer_length", "16384"},
    {"net_read_timeout", "30"},
    {"net_retry_count", "10"},
    {"net_write_timeout", "60"},
    {"new", "OFF"},
    {"old", "OFF"},
    {"old_alter_table", "OFF"},
    {"old_passwords", "OFF"},
    {"open_files_limit", "65535"},
    {"optimizer_prune_level", "1"},
    {"optimizer_search_depth", "62"},
    {"optimizer_switch", "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on"},
    {"pid_file", "/rdsdbdata/log/mysql-3306.pid"},
    {"plugin_dir", "/rdsdbbin/mysql/lib/mysql/plugin"},
    {"port", "3306"},
    {"preload_buffer_size", "32768"},
    {"profiling", "OFF"},
    {"profiling_history_size", "15"},
    {"protocol_version", "10"},
    {"pseudo_thread_id", "26810995"},
    {"query_alloc_block_size", "8192"},
    {"query_cache_limit", "1048576"},
    {"query_cache_min_res_unit", "4096"},
    {"query_cache_size", "0"},
    {"query_cache_type", "ON"},
    {"query_cache_wlock_invalidate", "OFF"},
    {"query_prealloc_size", "8192"},
    {"rand_seed1", ""},
    {"rand_seed2", ""},
    {"range_alloc_block_size", "4096"},
    {"read_buffer_size", "262144"},
    {"read_only", "OFF"},
    {"read_rnd_buffer_size", "524288"},
    {"relay_log", "/rdsdbdata/log/relaylog/relaylog"},
    {"relay_log_index", ""},
    {"relay_log_info_file", "relay-log.info"},
    {"relay_log_purge", "ON"},
    {"relay_log_space_limit", "0"},
    {"report_host", ""},
    {"report_password", ""},
    {"report_port", "3306"},
    {"report_user", ""},
    {"rpl_recovery_rank", "0"},
    {"secure_auth", "OFF"},
    {"secure_file_priv", "/tmp/"},
    {"server_id", "973870556"},
    {"skip_external_locking", "ON"},
    {"skip_name_resolve", "OFF"},
    {"skip_networking", "OFF"},
    {"skip_show_database", "OFF"},
    {"slave_compressed_protocol", "OFF"},
    {"slave_exec_mode", "STRICT"},
    {"slave_load_tmpdir", "/rdsdbdata/tmp"},
    {"slave_max_allowed_packet", "1073741824"},
    {"slave_net_timeout", "3600"},
    {"slave_skip_errors", "OFF"},
    {"slave_transaction_retries", "10"},
    {"slow_launch_time", "2"},
    {"slow_query_log", "ON"},
    {"slow_query_log_file", "/rdsdbdata/log/slowquery/mysql-slowquery.log"},
    {"socket", "/tmp/mysql.sock"},
    {"sort_buffer_size", "2097144"},
    {"sql_auto_is_null", "ON"},
    {"sql_big_selects", "ON"},
    {"sql_big_tables", "OFF"},
    {"sql_buffer_result", "OFF"},
    {"sql_log_bin", "ON"},
    {"sql_log_off", "OFF"},
    {"sql_log_update", "ON"},
    {"sql_low_priority_updates", "OFF"},
    {"sql_max_join_size", "18446744073709551615"},
    {"sql_mode", ""},
    {"sql_notes", "ON"},
    {"sql_quote_show_create", "ON"},
    {"sql_safe_updates", "OFF"},
    {"sql_select_limit", "18446744073709551615"},
    {"sql_slave_skip_counter", ""},
    {"sql_warnings", "OFF"},
    {"ssl_ca", "/rdsdbdata/rds-metadata/ca-cert.pem"},
    {"ssl_capath", ""},
    {"ssl_cert", "/rdsdbdata/rds-metadata/server-cert.pem"},
    {"ssl_cipher", "EXP1024-RC4-SHA:EXP1024-DES-CBC-SHA:AES256-SHA:AES128-SHA:DES-CBC3-SHA:DES-CBC-SHA:EXP-DES-CBC-SHA:"
                   "EXP-RC2-CBC-MD5:RC4-SHA:RC4-MD5:EXP-RC4-MD5:NULL-SHA:NULL-MD5:DES-CBC3-MD5:DES-CBC-MD5:EXP-RC2-CBC-"
                   "MD5:RC2-CBC-MD5:EXP-RC4-MD5:RC4-MD5:KRB5-DES-CBC3-MD5:KRB5-DES-CBC3-SHA:ADH-DES-CBC3-SHA:EDH-RSA-"
                   "DES-CBC3-SHA:EDH-DSS-DES-CBC3-SHA:ADH-AES256-SHA:DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA:ADH-AES128-"
                   "SHA:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA:EXP-KRB5-RC4-MD5:EXP-KRB5-RC2-CBC-MD5:EXP-KRB5-DES-CBC-"
                   "MD5:KRB5-RC4-MD5:KRB5-DES-CBC-MD5:ADH-RC4-MD5:EXP-ADH-RC4-MD5:DHE-DSS-RC4-SHA:EXP1024-DHE-DSS-RC4-"
                   "SHA:EXP1024-DHE-DSS-DES-CBC-SHA:EXP-KRB5-RC4-SHA:EXP-KRB5-RC2-CBC-SHA:EXP-KRB5-DES-CBC-SHA:KRB5-"
                   "RC4-SHA:KRB5-DES-CBC-SHA:ADH-DES-CBC-SHA:EXP-ADH-DES-CBC-SHA:EDH-RSA-DES-CBC-SHA:EXP-EDH-RSA-DES-"
                   "CBC-SHA:EDH-DSS-DES-CBC-SHA:EXP-EDH-DSS-DES-CBC-SHA"},
    {"ssl_key", "/rdsdbdata/rds-metadata/server-key.pem"},
    {"storage_engine", "InnoDB"},
    {"sync_binlog", "0"},
    {"sync_frm", "ON"},
    {"system_time_zone", "UTC"},
    {"table_definition_cache", "256"},
    {"table_lock_wait_timeout", "50"},
    {"table_open_cache", "96"},
    {"table_type", "InnoDB"},
    {"thread_cache_size", "0"},
    {"thread_handling", "one-thread-per-connection"},
    {"thread_stack", "196608"},
    {"time_format", "%H:%i:%s"},
    {"time_zone", "UTC"},
    {"timed_mutexes", "OFF"},
    {"timestamp", "1454813864"},
    {"tmp_table_size", "16777216"},
    {"tmpdir", "/rdsdbdata/tmp"},
    {"transaction_alloc_block_size", "8192"},
    {"transaction_prealloc_size", "4096"},
    {"tx_isolation", "REPEATABLE-READ"},
    {"unique_checks", "ON"},
    {"updatable_views_with_limit", "YES"},
    {"version", BedrockPlugin_MySQL::mysqlVersion},
    {"version_comment", VERSION},
    {"version_compile_machine", "x86_64"},
    {"version_compile_os", "unknown-linux-gnu"},
    {"wait_timeout", "28800"},
    {"warning_count", "0"},
};
