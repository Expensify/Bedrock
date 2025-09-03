#pragma once
#include <libstuff/libstuff.h>

/**
 * Utility functions for parsing MySQL queries and extracting information.
 * These functions are shared between the MySQL plugin implementation and tests.
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
